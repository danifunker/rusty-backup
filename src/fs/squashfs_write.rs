//! SquashFS v4.0 writer — emit a complete image from an in-memory tree.
//!
//! Phase 1 of `docs/squashfs_edit.md`. SquashFS has no in-place write story:
//! nothing on disk has slack, so *every* edit is a whole-image rebuild, exactly
//! as `mksquashfs` does it. This module is that rebuild.
//!
//! # Output layout
//!
//! ```text
//! 0                       superblock (96 bytes)
//! 96                      file data blocks, back to back
//! inode_table_start       inode table      (metadata blocks)
//! directory_table_start   directory table  (metadata blocks)
//! fragment_table_start    fragment table   (metadata blocks + index)
//! id_table_start          ID table         (metadata blocks + index)
//! xattr_id_table_start    xattr tables     (kv blocks, id blocks, header+index)
//!                         pad to 4 KiB
//! ```
//!
//! # Scope
//!
//! **Fragments are packed** (phase 1b), as `mksquashfs` does by default: a file
//! tail shorter than a block shares a block with other files' tails. The win is
//! compression ratio rather than padding — SquashFS already stores each data
//! block at its compressed length, so a 10-byte file never wasted a whole
//! block; what it wasted was *context*, since compressing tens of thousands of
//! tiny files as separate streams gives the codec nothing to work across.
//! `BuildOptions::use_fragments = false` reproduces `mksquashfs -no-fragments`.
//!
//! **Not yet: verbatim block reuse.** Rebuilding from a source image still
//! recompresses everything. Copying an unchanged file's already-compressed
//! blocks straight across is what makes a rebuild cost scale with the edit
//! rather than the image, and it needs the source-to-tree bridge from phase 2.
//!
//! **Compressors: gzip, XZ and Zstandard.** These are the three the read side
//! supports that also have an encoder available in-tree. LZ4 needs the
//! compressor-options header we don't emit, legacy LZMA is deprecated by
//! upstream, and LZO has no mature pure-Rust encoder (decision D5). Those three
//! are refused by name rather than silently substituted, so a rebuild never
//! changes the compressor out from under an image.

use std::collections::HashMap;
use std::io::{Seek, SeekFrom, Write};

// `off.is_multiple_of(4)` in the test-only offset scanner is an inherent method
// only since Rust 1.87; the vintage 1.73 build gets it from this trait. Scoped
// to `test` so it isn't an unused import on the vintage *library* build (which
// doesn't compile the test module).
#[cfg(all(test, feature = "rust173-polyfill"))]
use crate::rust173_compat::IntIsMultipleOf as _;

use super::filesystem::FilesystemError;
use super::squashfs::{
    Compressor, Xattr, INODE_DIR, INODE_EXT_DIR, INODE_EXT_FILE, INODE_EXT_SYMLINK, INODE_FILE,
    INODE_SYMLINK, METADATA_BLOCK_SIZE, SQUASHFS_INVALID_XATTR, SQUASHFS_MAGIC, SUPERBLOCK_SIZE,
};

/// Basic inode type codes as they appear in a *directory entry*. Directory
/// entries always name the basic form even when the inode itself is extended.
const DIR_TYPE_DIR: u16 = 1;
const DIR_TYPE_FILE: u16 = 2;
const DIR_TYPE_SYMLINK: u16 = 3;
const DIR_TYPE_BLKDEV: u16 = 4;
const DIR_TYPE_CHRDEV: u16 = 5;
const DIR_TYPE_FIFO: u16 = 6;
const DIR_TYPE_SOCKET: u16 = 7;

/// Superblock flag bits.
///
/// The three "uncompressed X" flags are never set: we attempt compression on
/// every block and fall back to storing that *individual* block verbatim (the
/// per-block header bit), which is strictly better than declaring a whole
/// category incompressible. They are named here because the flag word is only
/// legible alongside the bits it doesn't set.
const FLAG_NO_FRAGMENTS: u16 = 0x0010;
const FLAG_DUPLICATES: u16 = 0x0040;
const FLAG_NO_XATTRS: u16 = 0x0200;

/// Images are padded out to this boundary, matching `mksquashfs`.
const PAD_TO: u64 = 4096;

/// Maximum entries sharing one directory header.
const DIR_HEADER_MAX_ENTRIES: usize = 256;

/// Where a file's bytes come from at write time.
///
/// A whole-image rebuild must not hold every file's content in RAM at once — a
/// 558 MB rootfs would need 558 MB of `Vec<u8>`. So content is a *source* the
/// writer pulls one block at a time, never materializing more than one block
/// beyond what a `Bytes` variant already holds.
///
/// This is also the seam where verbatim block reuse will land (phase 2b): an
/// `FileContent::Source` variant that copies an unchanged file's
/// already-compressed blocks straight across, instead of decompress →
/// recompress. Not here yet; the enum is shaped to grow that variant without
/// touching callers.
pub enum FileContent {
    /// Content already in memory — a new or edited file, usually small.
    Bytes(Vec<u8>),
    /// Streamed from a host file, so a large added file never fully loads.
    HostFile { path: std::path::PathBuf, len: u64 },
}

impl FileContent {
    /// In-memory content.
    pub fn bytes(data: Vec<u8>) -> Self {
        Self::Bytes(data)
    }

    /// A host file, streamed at write time. Stats it now so the length is known
    /// without holding the bytes.
    pub fn host_file(path: std::path::PathBuf) -> Result<Self, FilesystemError> {
        let len = std::fs::metadata(&path).map_err(FilesystemError::Io)?.len();
        Ok(Self::HostFile { path, len })
    }

    /// The content length in bytes — known without reading the content.
    pub fn len(&self) -> u64 {
        match self {
            Self::Bytes(b) => b.len() as u64,
            Self::HostFile { len, .. } => *len,
        }
    }

    /// True when the file is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Open a fresh reader over the content.
    fn open(&self) -> Result<Box<dyn std::io::Read>, FilesystemError> {
        match self {
            Self::Bytes(b) => Ok(Box::new(std::io::Cursor::new(b.clone()))),
            Self::HostFile { path, .. } => {
                let f = std::fs::File::open(path).map_err(FilesystemError::Io)?;
                Ok(Box::new(std::io::BufReader::new(f)))
            }
        }
    }
}

/// What a node in the tree to be written actually is.
pub enum BuildKind {
    Dir(Vec<BuildNode>),
    File(FileContent),
    Symlink(String),
    BlockDev { major: u32, minor: u32 },
    CharDev { major: u32, minor: u32 },
    Fifo,
    Socket,
}

/// One node of the tree handed to [`write_squashfs`].
pub struct BuildNode {
    pub name: String,
    /// Permission bits only (`0o7777`); the type bits come from `kind`.
    pub mode: u16,
    pub uid: u32,
    pub gid: u32,
    /// Seconds since the Unix epoch.
    pub mtime: u32,
    pub xattrs: Vec<Xattr>,
    pub kind: BuildKind,
}

impl BuildNode {
    /// A directory node with sensible defaults, for tests and callers building
    /// a tree by hand.
    pub fn dir(name: &str, mode: u16, children: Vec<BuildNode>) -> Self {
        Self {
            name: name.to_string(),
            mode,
            uid: 0,
            gid: 0,
            mtime: 0,
            xattrs: Vec::new(),
            kind: BuildKind::Dir(children),
        }
    }

    /// A regular file node with in-memory content and sensible defaults.
    pub fn file(name: &str, mode: u16, data: Vec<u8>) -> Self {
        Self {
            name: name.to_string(),
            mode,
            uid: 0,
            gid: 0,
            mtime: 0,
            xattrs: Vec::new(),
            kind: BuildKind::File(FileContent::Bytes(data)),
        }
    }

    /// A symlink node with sensible defaults.
    pub fn symlink(name: &str, target: &str) -> Self {
        Self {
            name: name.to_string(),
            mode: 0o777,
            uid: 0,
            gid: 0,
            mtime: 0,
            xattrs: Vec::new(),
            kind: BuildKind::Symlink(target.to_string()),
        }
    }

    /// Build a tree from a host directory, as `mksquashfs DIR IMAGE` does.
    ///
    /// Permissions and mtimes come from the host; **ownership does not**. Every
    /// node is written as uid/gid 0, because the building user's ids (501:20 on
    /// a Mac) are meaningless inside the image — the same reasoning as
    /// [`crate::fs::attrs`]. Pass explicit ids afterwards if the target needs
    /// something else.
    ///
    /// Symlinks are recorded as symlinks (never followed). Sockets, FIFOs and
    /// device nodes in the source directory are skipped: representing them
    /// faithfully needs metadata the portable `std::fs` API doesn't expose, and
    /// silently turning a device node into an empty file would be worse than
    /// leaving it out.
    pub fn from_host_dir(root: &std::path::Path) -> Result<Self, FilesystemError> {
        let mut node = Self::dir("/", 0o755, collect_dir(root)?);
        node.mode = host_mode(root).unwrap_or(0o755) as u16;
        node.mtime = host_mtime(root);
        Ok(node)
    }
}

/// Fill `buf` exactly, turning a short read into a clear error rather than a
/// silent truncation — a content source that reports a length longer than it
/// can produce would otherwise corrupt the image.
fn read_exact_from(r: &mut dyn std::io::Read, buf: &mut [u8]) -> Result<(), FilesystemError> {
    r.read_exact(buf).map_err(|e| {
        FilesystemError::Io(std::io::Error::new(
            e.kind(),
            format!("squashfs: file content ended early: {e}"),
        ))
    })
}

/// Host permission bits, on platforms that have them.
fn host_mode(path: &std::path::Path) -> Option<u32> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::symlink_metadata(path)
            .ok()
            .map(|m| m.permissions().mode() & 0o7777)
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        None
    }
}

/// Host mtime as Unix seconds, or 0 when unavailable.
fn host_mtime(path: &std::path::Path) -> u32 {
    std::fs::symlink_metadata(path)
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs().min(u32::MAX as u64) as u32)
        .unwrap_or(0)
}

/// Read one directory into [`BuildNode`]s, recursing into subdirectories.
fn collect_dir(dir: &std::path::Path) -> Result<Vec<BuildNode>, FilesystemError> {
    let mut out = Vec::new();
    let entries = std::fs::read_dir(dir).map_err(FilesystemError::Io)?;
    for entry in entries {
        let entry = entry.map_err(FilesystemError::Io)?;
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        let meta = std::fs::symlink_metadata(&path).map_err(FilesystemError::Io)?;
        let mode = host_mode(&path).unwrap_or(if meta.is_dir() { 0o755 } else { 0o644 }) as u16;
        let mtime = host_mtime(&path);

        let kind = if meta.is_symlink() {
            let target = std::fs::read_link(&path).map_err(FilesystemError::Io)?;
            BuildKind::Symlink(target.to_string_lossy().into_owned())
        } else if meta.is_dir() {
            BuildKind::Dir(collect_dir(&path)?)
        } else if meta.is_file() {
            // Stream from disk at write time rather than reading every file into
            // RAM up front — a distro rootfs is far too big for that.
            BuildKind::File(FileContent::host_file(path.clone())?)
        } else {
            // Socket / FIFO / device node -- see `from_host_dir`.
            continue;
        };

        out.push(BuildNode {
            name,
            mode,
            uid: 0,
            gid: 0,
            mtime,
            xattrs: Vec::new(),
            kind,
        });
    }
    Ok(out)
}

/// Knobs for the emitted image. Defaults match `mksquashfs`: gzip, 128 KiB
/// blocks, tails packed into fragments — but a rebuild should pass the *source*
/// image's settings so an edit doesn't silently re-tune compression (D3).
pub struct BuildOptions {
    pub compressor: Compressor,
    pub block_size: u32,
    /// Stamped into the superblock's `mod_time`.
    pub mod_time: u32,
    /// Pack file tails shorter than a block into shared **fragment** blocks.
    ///
    /// On by default, as in `mksquashfs`. The win is compression ratio, not
    /// padding: SquashFS data blocks are already stored at their compressed
    /// length, so a 10-byte file never wasted a whole block. What it *did* waste
    /// is context — compressing fifty thousand tiny files as fifty thousand
    /// separate streams gives the codec nothing to work with, whereas packing
    /// their tails into shared 128 KiB blocks lets it compress across them.
    ///
    /// Setting this false reproduces `mksquashfs -no-fragments`.
    pub use_fragments: bool,
}

impl Default for BuildOptions {
    fn default() -> Self {
        Self {
            compressor: Compressor::Gzip,
            block_size: 131_072,
            mod_time: 0,
            use_fragments: true,
        }
    }
}

/// Accumulates file tails into shared fragment blocks.
///
/// A tail is handed over with `Builder::push_tail`, which returns the
/// `(index, offset)` pair the file's inode records. The block itself is not
/// written until it fills or the build ends, so the index is assigned eagerly
/// (it is simply how many blocks have already been flushed) while the entry's
/// on-disk position is filled in at flush time — which is why `entries` holds
/// positions rather than the blocks themselves.
struct FragmentWriter {
    /// The block being accumulated.
    buf: Vec<u8>,
    /// Flushed blocks: `(start, size_field)`, in index order.
    entries: Vec<(u64, u32)>,
    block_size: usize,
}

impl FragmentWriter {
    fn new(block_size: u32) -> Self {
        Self {
            buf: Vec::new(),
            entries: Vec::new(),
            block_size: block_size as usize,
        }
    }

    /// Room left in the block currently being filled.
    fn room(&self) -> usize {
        self.block_size - self.buf.len()
    }
}

/// Accumulates bytes into 8 KiB metadata blocks, compressing each as it fills.
///
/// SquashFS addresses inodes and directory entries by a 48-bit *reference*
/// packing the containing block's start (relative to the table's own start)
/// with a byte offset inside that block's **decompressed** payload — so a
/// reference has to be captured before the item is written, and an item is free
/// to straddle a block boundary.
struct MetadataWriter {
    /// Finished, compressed blocks with their 2-byte headers.
    out: Vec<u8>,
    /// The current block's uncompressed payload, at most 8 KiB.
    buf: Vec<u8>,
    /// Offset within `out` where the current block will land.
    block_start: u64,
    compressor: Compressor,
}

impl MetadataWriter {
    fn new(compressor: Compressor) -> Self {
        Self {
            out: Vec::new(),
            buf: Vec::new(),
            block_start: 0,
            compressor,
        }
    }

    /// The reference for whatever is written next.
    fn current_ref(&self) -> u64 {
        (self.block_start << 16) | self.buf.len() as u64
    }

    /// Byte position within the finished stream, as directory inodes record it.
    fn current_block_start(&self) -> u64 {
        self.block_start
    }

    fn current_offset(&self) -> u16 {
        self.buf.len() as u16
    }

    fn write(&mut self, data: &[u8]) -> Result<(), FilesystemError> {
        let mut rest = data;
        while !rest.is_empty() {
            let room = METADATA_BLOCK_SIZE - self.buf.len();
            let take = room.min(rest.len());
            self.buf.extend_from_slice(&rest[..take]);
            rest = &rest[take..];
            if self.buf.len() == METADATA_BLOCK_SIZE {
                self.flush_block()?;
            }
        }
        Ok(())
    }

    /// Compress and emit the pending block. A block that does not shrink is
    /// stored verbatim with the header's top bit set, exactly as the reader
    /// expects.
    fn flush_block(&mut self) -> Result<(), FilesystemError> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let packed = compress(self.compressor, &self.buf)?;
        let (payload, stored): (&[u8], bool) = if packed.len() < self.buf.len() {
            (&packed, false)
        } else {
            (&self.buf, true)
        };
        let header = payload.len() as u16 | if stored { 0x8000 } else { 0 };
        self.out.extend_from_slice(&header.to_le_bytes());
        self.out.extend_from_slice(payload);
        self.buf.clear();
        self.block_start = self.out.len() as u64;
        Ok(())
    }

    fn finish(mut self) -> Result<Vec<u8>, FilesystemError> {
        self.flush_block()?;
        Ok(self.out)
    }
}

/// Compress one block with the image's codec.
fn compress(compressor: Compressor, data: &[u8]) -> Result<Vec<u8>, FilesystemError> {
    match compressor {
        Compressor::Gzip => {
            // SquashFS uses raw zlib streams, not the gzip wrapper — mirror
            // `ZlibDecoder` on the read side.
            use flate2::write::ZlibEncoder;
            use flate2::Compression;
            let mut enc = ZlibEncoder::new(Vec::new(), Compression::new(9));
            enc.write_all(data)
                .map_err(|e| FilesystemError::Io(crate::compat::io_other(e.to_string())))?;
            enc.finish()
                .map_err(|e| FilesystemError::Io(crate::compat::io_other(e.to_string())))
        }
        Compressor::Xz => {
            let mut out = Vec::new();
            lzma_rs::xz_compress(&mut std::io::Cursor::new(data), &mut out).map_err(|e| {
                FilesystemError::Io(crate::compat::io_other(format!("xz compress: {e:?}")))
            })?;
            Ok(out)
        }
        Compressor::Zstd => compress_zstd(data),
        other => Err(FilesystemError::Unsupported(format!(
            "squashfs: writing {}-compressed images is not supported \
             (readable, but no encoder — see docs/squashfs_edit.md)",
            other.name()
        ))),
    }
}

fn compress_zstd(data: &[u8]) -> Result<Vec<u8>, FilesystemError> {
    crate::rbformats::zstd_compat::encode_all(data, 15)
        .map_err(|e| FilesystemError::Io(crate::compat::io_other(format!("zstd compress: {e}"))))
}

/// Interns uid/gid values into the image's ID pool. Inodes reference IDs by a
/// 16-bit index, so the pool is capped at 65536 distinct values.
#[derive(Default)]
struct IdTable {
    ids: Vec<u32>,
    index: HashMap<u32, u16>,
}

impl IdTable {
    fn intern(&mut self, id: u32) -> Result<u16, FilesystemError> {
        if let Some(&i) = self.index.get(&id) {
            return Ok(i);
        }
        if self.ids.len() >= u16::MAX as usize {
            return Err(FilesystemError::Unsupported(
                "squashfs: more than 65535 distinct uid/gid values".into(),
            ));
        }
        let i = self.ids.len() as u16;
        self.ids.push(id);
        self.index.insert(id, i);
        Ok(i)
    }
}

/// Builds the two xattr streams. Identical attribute *sets* share one ID, which
/// is how `mksquashfs` keeps `security.capability` from being written once per
/// binary.
struct XattrBuilder {
    kv: MetadataWriter,
    ids: Vec<(u64, u32, u32)>,
    /// Serialized-set -> already-assigned ID.
    seen: HashMap<Vec<u8>, u32>,
}

impl XattrBuilder {
    fn new(compressor: Compressor) -> Self {
        Self {
            kv: MetadataWriter::new(compressor),
            ids: Vec::new(),
            seen: HashMap::new(),
        }
    }

    /// Split a fully-qualified name into its prefix id and remainder. An
    /// unrecognized prefix cannot be represented, so it is refused rather than
    /// written under the wrong namespace.
    fn split_prefix(name: &str) -> Result<(u16, &str), FilesystemError> {
        for (id, prefix) in [(0u16, "user."), (1, "trusted."), (2, "security.")] {
            if let Some(rest) = name.strip_prefix(prefix) {
                return Ok((id, rest));
            }
        }
        Err(FilesystemError::Unsupported(format!(
            "squashfs: xattr {name:?} has no representable namespace prefix"
        )))
    }

    fn intern(&mut self, attrs: &[Xattr]) -> Result<u32, FilesystemError> {
        if attrs.is_empty() {
            return Ok(SQUASHFS_INVALID_XATTR);
        }
        // Canonical serialization: sorted by name, so two sets differing only in
        // order collapse to one ID.
        let mut sorted: Vec<&Xattr> = attrs.iter().collect();
        sorted.sort_by(|a, b| a.name.cmp(&b.name));

        let mut blob = Vec::new();
        for a in &sorted {
            let (prefix, rest) = Self::split_prefix(&a.name)?;
            blob.extend_from_slice(&prefix.to_le_bytes());
            blob.extend_from_slice(&(rest.len() as u16).to_le_bytes());
            blob.extend_from_slice(rest.as_bytes());
            blob.extend_from_slice(&(a.value.len() as u32).to_le_bytes());
            blob.extend_from_slice(&a.value);
        }
        if let Some(&id) = self.seen.get(&blob) {
            return Ok(id);
        }

        let reference = self.kv.current_ref();
        self.kv.write(&blob)?;
        let id = self.ids.len() as u32;
        self.ids
            .push((reference, sorted.len() as u32, blob.len() as u32));
        self.seen.insert(blob, id);
        Ok(id)
    }

    fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }
}

/// What writing one node produced, for its parent's directory entry.
struct Written {
    inode_ref: u64,
    inode_number: u32,
    dir_type: u16,
}

/// Running state for one image build.
struct Builder<'a, W: Write + Seek> {
    out: &'a mut W,
    opts: &'a BuildOptions,
    inodes: MetadataWriter,
    dirs: MetadataWriter,
    ids: IdTable,
    xattrs: XattrBuilder,
    frags: FragmentWriter,
    next_inode: u32,
    /// Current write position for data blocks.
    data_pos: u64,
}

impl<'a, W: Write + Seek> Builder<'a, W> {
    /// Stream a file's data, returning where its blocks start, each block's
    /// size field, and the fragment its tail landed in (if any).
    ///
    /// Content is pulled one block at a time from the source, so a multi-hundred
    /// -MB file never fully materializes. With fragments on, only whole blocks
    /// reach the data area; a short tail is packed with other files' tails. A
    /// file that is an exact multiple of the block size has no tail and so no
    /// fragment.
    fn write_file_data(
        &mut self,
        content: &FileContent,
    ) -> Result<(u64, Vec<u32>, u32, u32), FilesystemError> {
        let start = self.data_pos;
        let mut sizes = Vec::new();
        let total = content.len();
        if total == 0 {
            return Ok((start, sizes, SQUASHFS_INVALID_XATTR, 0));
        }

        let bs = self.opts.block_size as u64;
        // The tail is packed into a fragment only when fragments are on; with
        // them off it is written as its own (short) trailing data block, so
        // treat the whole file as blocks.
        let tail_len = if self.opts.use_fragments {
            (total % bs) as usize
        } else {
            0
        };
        let whole_bytes = total - tail_len as u64;

        let mut reader = content.open()?;
        let mut buf = vec![0u8; bs as usize];
        let mut done = 0u64;
        while done < whole_bytes {
            let n = (whole_bytes - done).min(bs) as usize;
            read_exact_from(&mut reader, &mut buf[..n])?;
            let field = self.emit_data_block(&buf[..n])?;
            sizes.push(field);
            done += n as u64;
        }

        if tail_len == 0 {
            Ok((start, sizes, SQUASHFS_INVALID_XATTR, 0))
        } else {
            read_exact_from(&mut reader, &mut buf[..tail_len])?;
            let (index, offset) = self.push_tail(&buf[..tail_len])?;
            Ok((start, sizes, index, offset))
        }
    }

    /// Compress one block and append it to the data area, returning its size
    /// field (bit 24 set = stored uncompressed, because compressing it did not
    /// help).
    fn emit_data_block(&mut self, chunk: &[u8]) -> Result<u32, FilesystemError> {
        let packed = compress(self.opts.compressor, chunk)?;
        let (payload, stored): (&[u8], bool) = if packed.len() < chunk.len() {
            (&packed, false)
        } else {
            (chunk, true)
        };
        self.out.write_all(payload).map_err(FilesystemError::Io)?;
        self.data_pos += payload.len() as u64;
        let mut field = payload.len() as u32;
        if stored {
            field |= 0x0100_0000;
        }
        Ok(field)
    }

    /// Flush the fragment block currently being accumulated, recording where it
    /// landed. Called when a block fills, and once more at the end of the build.
    fn flush_fragment(&mut self) -> Result<(), FilesystemError> {
        if self.frags.buf.is_empty() {
            return Ok(());
        }
        let buf = std::mem::take(&mut self.frags.buf);
        let start = self.data_pos;
        let field = self.emit_data_block(&buf)?;
        self.frags.entries.push((start, field));
        Ok(())
    }

    /// Hand a file's tail to the fragment writer, returning the
    /// `(index, offset)` pair its inode records.
    fn push_tail(&mut self, tail: &[u8]) -> Result<(u32, u32), FilesystemError> {
        if tail.len() > self.frags.room() {
            self.flush_fragment()?;
        }
        let index = self.frags.entries.len() as u32;
        let offset = self.frags.buf.len() as u32;
        self.frags.buf.extend_from_slice(tail);
        Ok((index, offset))
    }

    /// Emit one node (post-order: a directory's children are written before the
    /// directory itself, because its entries carry their inode references).
    fn write_node(&mut self, node: &BuildNode) -> Result<Written, FilesystemError> {
        let uid_idx = self.ids.intern(node.uid)?;
        let gid_idx = self.ids.intern(node.gid)?;
        let xattr_idx = self.xattrs.intern(&node.xattrs)?;
        let has_xattr = xattr_idx != SQUASHFS_INVALID_XATTR;

        match &node.kind {
            BuildKind::Dir(children) => {
                // Children first.
                let mut sorted: Vec<&BuildNode> = children.iter().collect();
                sorted.sort_by(|a, b| a.name.as_bytes().cmp(b.name.as_bytes()));
                let mut written = Vec::with_capacity(sorted.len());
                for child in &sorted {
                    written.push((child.name.clone(), self.write_node(child)?));
                }

                let inode_number = self.take_inode_number();
                let (dir_start, dir_offset, dir_size) = self.write_dir_entries(&written)?;

                let inode_ref = self.inodes.current_ref();
                let kind = if has_xattr || dir_size > u16::MAX as u32 {
                    INODE_EXT_DIR
                } else {
                    INODE_DIR
                };
                let mut b = Vec::new();
                self.push_common(&mut b, kind, node, uid_idx, gid_idx, inode_number);
                if kind == INODE_DIR {
                    b.extend_from_slice(&dir_start.to_le_bytes());
                    b.extend_from_slice(&(2u32 + count_subdirs(children)).to_le_bytes());
                    b.extend_from_slice(&(dir_size as u16).to_le_bytes());
                    b.extend_from_slice(&dir_offset.to_le_bytes());
                    b.extend_from_slice(&(self.next_inode).to_le_bytes()); // parent
                } else {
                    b.extend_from_slice(&(2u32 + count_subdirs(children)).to_le_bytes());
                    b.extend_from_slice(&dir_size.to_le_bytes());
                    b.extend_from_slice(&dir_start.to_le_bytes());
                    b.extend_from_slice(&(self.next_inode).to_le_bytes()); // parent
                    b.extend_from_slice(&0u16.to_le_bytes()); // index count
                    b.extend_from_slice(&dir_offset.to_le_bytes());
                    b.extend_from_slice(&xattr_idx.to_le_bytes());
                }
                self.inodes.write(&b)?;
                Ok(Written {
                    inode_ref,
                    inode_number,
                    dir_type: DIR_TYPE_DIR,
                })
            }
            BuildKind::File(content) => {
                let file_len = content.len();
                let (start, sizes, frag_index, frag_offset) = self.write_file_data(content)?;
                let inode_number = self.take_inode_number();
                let inode_ref = self.inodes.current_ref();
                let needs_ext = has_xattr || file_len > u32::MAX as u64 || start > u32::MAX as u64;
                let kind = if needs_ext {
                    INODE_EXT_FILE
                } else {
                    INODE_FILE
                };
                let mut b = Vec::new();
                self.push_common(&mut b, kind, node, uid_idx, gid_idx, inode_number);
                if kind == INODE_FILE {
                    b.extend_from_slice(&(start as u32).to_le_bytes());
                    b.extend_from_slice(&frag_index.to_le_bytes());
                    b.extend_from_slice(&frag_offset.to_le_bytes());
                    b.extend_from_slice(&(file_len as u32).to_le_bytes());
                } else {
                    b.extend_from_slice(&start.to_le_bytes());
                    b.extend_from_slice(&file_len.to_le_bytes());
                    b.extend_from_slice(&0u64.to_le_bytes()); // sparse
                    b.extend_from_slice(&1u32.to_le_bytes()); // nlink
                    b.extend_from_slice(&frag_index.to_le_bytes());
                    b.extend_from_slice(&frag_offset.to_le_bytes());
                    b.extend_from_slice(&xattr_idx.to_le_bytes());
                }
                for s in &sizes {
                    b.extend_from_slice(&s.to_le_bytes());
                }
                self.inodes.write(&b)?;
                Ok(Written {
                    inode_ref,
                    inode_number,
                    dir_type: DIR_TYPE_FILE,
                })
            }
            BuildKind::Symlink(target) => {
                let inode_number = self.take_inode_number();
                let inode_ref = self.inodes.current_ref();
                let kind = if has_xattr {
                    INODE_EXT_SYMLINK
                } else {
                    INODE_SYMLINK
                };
                let mut b = Vec::new();
                self.push_common(&mut b, kind, node, uid_idx, gid_idx, inode_number);
                b.extend_from_slice(&1u32.to_le_bytes()); // nlink
                b.extend_from_slice(&(target.len() as u32).to_le_bytes());
                b.extend_from_slice(target.as_bytes());
                if kind == INODE_EXT_SYMLINK {
                    // The extended form parks its xattr index after the target.
                    b.extend_from_slice(&xattr_idx.to_le_bytes());
                }
                self.inodes.write(&b)?;
                Ok(Written {
                    inode_ref,
                    inode_number,
                    dir_type: DIR_TYPE_SYMLINK,
                })
            }
            BuildKind::BlockDev { major, minor } | BuildKind::CharDev { major, minor } => {
                let is_block = matches!(node.kind, BuildKind::BlockDev { .. });
                let inode_number = self.take_inode_number();
                let inode_ref = self.inodes.current_ref();
                let (basic, ext) = if is_block {
                    (4u16, 11u16)
                } else {
                    (5u16, 12u16)
                };
                let kind = if has_xattr { ext } else { basic };
                let mut b = Vec::new();
                self.push_common(&mut b, kind, node, uid_idx, gid_idx, inode_number);
                b.extend_from_slice(&1u32.to_le_bytes()); // nlink
                let dev = ((major & 0xFFF) << 8) | (minor & 0xFF) | ((minor & 0xFFF00) << 12);
                b.extend_from_slice(&dev.to_le_bytes());
                if kind == ext {
                    b.extend_from_slice(&xattr_idx.to_le_bytes());
                }
                self.inodes.write(&b)?;
                Ok(Written {
                    inode_ref,
                    inode_number,
                    dir_type: if is_block {
                        DIR_TYPE_BLKDEV
                    } else {
                        DIR_TYPE_CHRDEV
                    },
                })
            }
            BuildKind::Fifo | BuildKind::Socket => {
                let is_fifo = matches!(node.kind, BuildKind::Fifo);
                let inode_number = self.take_inode_number();
                let inode_ref = self.inodes.current_ref();
                let (basic, ext) = if is_fifo {
                    (6u16, 13u16)
                } else {
                    (7u16, 14u16)
                };
                let kind = if has_xattr { ext } else { basic };
                let mut b = Vec::new();
                self.push_common(&mut b, kind, node, uid_idx, gid_idx, inode_number);
                b.extend_from_slice(&1u32.to_le_bytes()); // nlink
                if kind == ext {
                    b.extend_from_slice(&xattr_idx.to_le_bytes());
                }
                self.inodes.write(&b)?;
                Ok(Written {
                    inode_ref,
                    inode_number,
                    dir_type: if is_fifo {
                        DIR_TYPE_FIFO
                    } else {
                        DIR_TYPE_SOCKET
                    },
                })
            }
        }
    }

    fn take_inode_number(&mut self) -> u32 {
        let n = self.next_inode;
        self.next_inode += 1;
        n
    }

    /// The 16-byte header every inode starts with.
    fn push_common(
        &self,
        b: &mut Vec<u8>,
        kind: u16,
        node: &BuildNode,
        uid_idx: u16,
        gid_idx: u16,
        inode_number: u32,
    ) {
        b.extend_from_slice(&kind.to_le_bytes());
        b.extend_from_slice(&(node.mode & 0o7777).to_le_bytes());
        b.extend_from_slice(&uid_idx.to_le_bytes());
        b.extend_from_slice(&gid_idx.to_le_bytes());
        b.extend_from_slice(&node.mtime.to_le_bytes());
        b.extend_from_slice(&inode_number.to_le_bytes());
    }

    /// Serialize a directory's entries into the directory table.
    ///
    /// Entries are grouped under headers; a group must share one inode-table
    /// block, stay within 256 entries, and keep every inode number within `i16`
    /// of the header's base. Returns the block start, in-block offset, and the
    /// size the inode records (which counts three bytes that are not there — an
    /// artefact of the format, and why an empty directory records 3).
    fn write_dir_entries(
        &mut self,
        entries: &[(String, Written)],
    ) -> Result<(u32, u16, u32), FilesystemError> {
        let start_block = self.dirs.current_block_start() as u32;
        let start_offset = self.dirs.current_offset();
        if entries.is_empty() {
            return Ok((start_block, start_offset, 3));
        }

        let mut body = Vec::new();
        let mut i = 0usize;
        while i < entries.len() {
            let base_block = (entries[i].1.inode_ref >> 16) as u32;
            let base_inode = entries[i].1.inode_number as i64;

            // How many following entries can share this header?
            let mut n = 0usize;
            while i + n < entries.len() && n < DIR_HEADER_MAX_ENTRIES {
                let w = &entries[i + n].1;
                if (w.inode_ref >> 16) as u32 != base_block {
                    break;
                }
                let delta = w.inode_number as i64 - base_inode;
                if delta < i16::MIN as i64 || delta > i16::MAX as i64 {
                    break;
                }
                n += 1;
            }

            body.extend_from_slice(&((n - 1) as u32).to_le_bytes());
            body.extend_from_slice(&base_block.to_le_bytes());
            body.extend_from_slice(&(base_inode as u32).to_le_bytes());
            for (name, w) in &entries[i..i + n] {
                if name.len() > 256 {
                    return Err(FilesystemError::Unsupported(format!(
                        "squashfs: name {name:?} exceeds 256 bytes"
                    )));
                }
                let delta = (w.inode_number as i64 - base_inode) as i16;
                body.extend_from_slice(&((w.inode_ref & 0xFFFF) as u16).to_le_bytes());
                body.extend_from_slice(&delta.to_le_bytes());
                body.extend_from_slice(&w.dir_type.to_le_bytes());
                body.extend_from_slice(&((name.len() - 1) as u16).to_le_bytes());
                body.extend_from_slice(name.as_bytes());
            }
            i += n;
        }

        self.dirs.write(&body)?;
        Ok((start_block, start_offset, body.len() as u32 + 3))
    }
}

/// Count immediate subdirectories — a directory's link count is
/// `2 + subdirectories` (`.`, `..`, and one `..` per child directory).
fn count_subdirs(children: &[BuildNode]) -> u32 {
    children
        .iter()
        .filter(|c| matches!(c.kind, BuildKind::Dir(_)))
        .count() as u32
}

/// Write a complete SquashFS image for `root` into `out`, returning the number
/// of bytes used (before the trailing pad).
pub fn write_squashfs<W: Write + Seek>(
    out: &mut W,
    root: &BuildNode,
    opts: &BuildOptions,
) -> Result<u64, FilesystemError> {
    if !matches!(root.kind, BuildKind::Dir(_)) {
        return Err(FilesystemError::InvalidData(
            "squashfs: the root node must be a directory".into(),
        ));
    }
    if opts.block_size < 4096 || !opts.block_size.is_power_of_two() || opts.block_size > 1 << 20 {
        return Err(FilesystemError::Unsupported(format!(
            "squashfs: block size {} must be a power of two in 4 KiB..=1 MiB",
            opts.block_size
        )));
    }
    // Fail before writing anything if the codec has no encoder.
    compress(opts.compressor, b"probe")?;

    out.seek(SeekFrom::Start(SUPERBLOCK_SIZE as u64))
        .map_err(FilesystemError::Io)?;

    let mut builder = Builder {
        out,
        opts,
        inodes: MetadataWriter::new(opts.compressor),
        dirs: MetadataWriter::new(opts.compressor),
        ids: IdTable::default(),
        xattrs: XattrBuilder::new(opts.compressor),
        frags: FragmentWriter::new(opts.block_size),
        next_inode: 1,
        data_pos: SUPERBLOCK_SIZE as u64,
    };

    let root_written = builder.write_node(root)?;
    // The last partly-filled fragment block still has to land in the data area,
    // before any of the tables are written after it.
    builder.flush_fragment()?;
    let inode_count = builder.next_inode - 1;

    let Builder {
        out,
        inodes,
        dirs,
        ids,
        xattrs,
        frags,
        data_pos,
        ..
    } = builder;

    let inode_blob = inodes.finish()?;
    let dir_blob = dirs.finish()?;
    let had_xattrs = !xattrs.is_empty();

    // ---- inode table ----
    let inode_table_start = data_pos;
    out.write_all(&inode_blob).map_err(FilesystemError::Io)?;

    // ---- directory table ----
    let directory_table_start = inode_table_start + inode_blob.len() as u64;
    out.write_all(&dir_blob).map_err(FilesystemError::Io)?;

    let mut pos = directory_table_start + dir_blob.len() as u64;

    // ---- fragment table: 16-byte entries in metadata blocks, then an index ----
    let fragment_count = frags.entries.len() as u32;
    let fragment_table_start = if fragment_count == 0 {
        // No fragments at all: the format spells an absent table as -1.
        u64::MAX
    } else {
        let mut fw = MetadataWriter::new(opts.compressor);
        let mut block_starts = Vec::new();
        for (i, (start, size)) in frags.entries.iter().enumerate() {
            // 8192 / 16 = 512 entries per metadata block.
            if i % 512 == 0 {
                block_starts.push(fw.current_block_start());
            }
            let mut e = Vec::with_capacity(16);
            e.extend_from_slice(&start.to_le_bytes());
            e.extend_from_slice(&size.to_le_bytes());
            e.extend_from_slice(&0u32.to_le_bytes()); // unused
            fw.write(&e)?;
        }
        let blob = fw.finish()?;
        let blocks_at = pos;
        out.write_all(&blob).map_err(FilesystemError::Io)?;
        pos += blob.len() as u64;

        let index_at = pos;
        for s in &block_starts {
            out.write_all(&(blocks_at + s).to_le_bytes())
                .map_err(FilesystemError::Io)?;
        }
        pos += (block_starts.len() * 8) as u64;
        index_at
    };

    // ---- ID table: metadata blocks, then an index of pointers to them ----
    let mut id_writer = MetadataWriter::new(opts.compressor);
    let mut id_block_starts = Vec::new();
    for (i, id) in ids.ids.iter().enumerate() {
        if i % 2048 == 0 {
            id_block_starts.push(id_writer.current_block_start());
        }
        id_writer.write(&id.to_le_bytes())?;
    }
    let id_blob = id_writer.finish()?;
    let id_blocks_at = pos;
    out.write_all(&id_blob).map_err(FilesystemError::Io)?;
    pos += id_blob.len() as u64;

    let id_table_start = pos;
    for s in &id_block_starts {
        out.write_all(&(id_blocks_at + s).to_le_bytes())
            .map_err(FilesystemError::Io)?;
    }
    pos += (id_block_starts.len() * 8) as u64;

    // ---- xattr tables: kv blocks, id blocks, then header + index ----
    let xattr_id_table_start = if had_xattrs {
        let XattrBuilder { kv, ids: xids, .. } = xattrs;
        let kv_blob = kv.finish()?;
        let xattr_table_start = pos;
        out.write_all(&kv_blob).map_err(FilesystemError::Io)?;
        pos += kv_blob.len() as u64;

        let mut idw = MetadataWriter::new(opts.compressor);
        let mut id_starts = Vec::new();
        for (i, (r, count, size)) in xids.iter().enumerate() {
            if i % 512 == 0 {
                id_starts.push(idw.current_block_start());
            }
            let mut e = Vec::with_capacity(16);
            e.extend_from_slice(&r.to_le_bytes());
            e.extend_from_slice(&count.to_le_bytes());
            e.extend_from_slice(&size.to_le_bytes());
            idw.write(&e)?;
        }
        let id_blob = idw.finish()?;
        let xid_blocks_at = pos;
        out.write_all(&id_blob).map_err(FilesystemError::Io)?;
        pos += id_blob.len() as u64;

        let header_at = pos;
        out.write_all(&xattr_table_start.to_le_bytes())
            .map_err(FilesystemError::Io)?;
        out.write_all(&(xids.len() as u32).to_le_bytes())
            .map_err(FilesystemError::Io)?;
        out.write_all(&0u32.to_le_bytes())
            .map_err(FilesystemError::Io)?;
        for s in &id_starts {
            out.write_all(&(xid_blocks_at + s).to_le_bytes())
                .map_err(FilesystemError::Io)?;
        }
        pos += 16 + (id_starts.len() * 8) as u64;
        header_at
    } else {
        u64::MAX
    };

    let bytes_used = pos;

    // ---- superblock ----
    let mut flags = FLAG_DUPLICATES;
    if fragment_count == 0 {
        flags |= FLAG_NO_FRAGMENTS;
    }
    if !had_xattrs {
        flags |= FLAG_NO_XATTRS;
    }
    let mut sb = vec![0u8; SUPERBLOCK_SIZE];
    sb[0..4].copy_from_slice(&SQUASHFS_MAGIC.to_le_bytes());
    sb[4..8].copy_from_slice(&inode_count.to_le_bytes());
    sb[8..12].copy_from_slice(&opts.mod_time.to_le_bytes());
    sb[12..16].copy_from_slice(&opts.block_size.to_le_bytes());
    sb[16..20].copy_from_slice(&fragment_count.to_le_bytes());
    sb[20..22].copy_from_slice(&compressor_id(opts.compressor).to_le_bytes());
    sb[22..24].copy_from_slice(&(opts.block_size.trailing_zeros() as u16).to_le_bytes());
    sb[24..26].copy_from_slice(&flags.to_le_bytes());
    sb[26..28].copy_from_slice(&(ids.ids.len() as u16).to_le_bytes());
    sb[28..30].copy_from_slice(&4u16.to_le_bytes()); // version major
    sb[30..32].copy_from_slice(&0u16.to_le_bytes()); // version minor
    sb[32..40].copy_from_slice(&root_written.inode_ref.to_le_bytes());
    sb[40..48].copy_from_slice(&bytes_used.to_le_bytes());
    sb[48..56].copy_from_slice(&id_table_start.to_le_bytes());
    sb[56..64].copy_from_slice(&xattr_id_table_start.to_le_bytes());
    sb[64..72].copy_from_slice(&inode_table_start.to_le_bytes());
    sb[72..80].copy_from_slice(&directory_table_start.to_le_bytes());
    sb[80..88].copy_from_slice(&fragment_table_start.to_le_bytes());
    // No NFS export table.
    sb[88..96].copy_from_slice(&u64::MAX.to_le_bytes());

    out.seek(SeekFrom::Start(0)).map_err(FilesystemError::Io)?;
    out.write_all(&sb).map_err(FilesystemError::Io)?;

    // ---- pad ----
    let padded = bytes_used.div_ceil(PAD_TO) * PAD_TO;
    if padded > bytes_used {
        out.seek(SeekFrom::Start(bytes_used))
            .map_err(FilesystemError::Io)?;
        out.write_all(&vec![0u8; (padded - bytes_used) as usize])
            .map_err(FilesystemError::Io)?;
    }
    out.flush().map_err(FilesystemError::Io)?;
    Ok(bytes_used)
}

fn compressor_id(c: Compressor) -> u16 {
    match c {
        Compressor::Gzip => 1,
        Compressor::Lzma => 2,
        Compressor::Lzo => 3,
        Compressor::Xz => 4,
        Compressor::Lz4 => 5,
        Compressor::Zstd => 6,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::filesystem::Filesystem;
    use crate::fs::squashfs::SquashfsFilesystem;
    use std::io::Cursor;

    /// True when `unsquashfs` is on PATH. Note it **exits 1** while still
    /// printing its banner, so the exit status cannot be the probe.
    fn unsquashfs_available() -> bool {
        std::process::Command::new("unsquashfs")
            .arg("-version")
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).contains("unsquashfs version"))
            .unwrap_or(false)
    }

    /// A tree covering the cases that actually break writers: nested
    /// directories, an empty directory, an empty file, a file that spans
    /// several blocks, a symlink, a device node, and xattrs.
    fn sample_tree() -> BuildNode {
        let big: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
        BuildNode {
            name: "/".into(),
            mode: 0o755,
            uid: 0,
            gid: 0,
            mtime: 1_700_000_000,
            xattrs: Vec::new(),
            kind: BuildKind::Dir(vec![
                BuildNode::file("hello.txt", 0o644, b"hello squashfs\n".to_vec()),
                BuildNode::file("empty.bin", 0o600, Vec::new()),
                BuildNode::file("big.bin", 0o644, big),
                BuildNode::symlink("link", "hello.txt"),
                BuildNode::dir("emptydir", 0o755, vec![]),
                BuildNode::dir(
                    "sub",
                    0o750,
                    vec![
                        BuildNode::file("nested.txt", 0o444, b"nested\n".to_vec()),
                        BuildNode {
                            name: "capped".into(),
                            mode: 0o755,
                            uid: 0,
                            gid: 0,
                            mtime: 1_700_000_000,
                            xattrs: vec![Xattr {
                                name: "security.capability".into(),
                                value: vec![1, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0],
                            }],
                            kind: BuildKind::File(FileContent::Bytes(b"has an xattr\n".to_vec())),
                        },
                    ],
                ),
                BuildNode {
                    name: "null".into(),
                    mode: 0o666,
                    uid: 0,
                    gid: 0,
                    mtime: 1_700_000_000,
                    xattrs: Vec::new(),
                    kind: BuildKind::CharDev { major: 1, minor: 3 },
                },
            ]),
        }
    }

    fn build(opts: BuildOptions) -> Vec<u8> {
        let mut cur = Cursor::new(Vec::new());
        write_squashfs(&mut cur, &sample_tree(), &opts).expect("write squashfs");
        cur.into_inner()
    }

    /// Round-trip through **our own** reader: every field we wrote comes back.
    #[test]
    fn round_trips_through_our_reader() {
        for compressor in [Compressor::Gzip, Compressor::Zstd, Compressor::Xz] {
            let img = build(BuildOptions {
                compressor,
                block_size: 131_072,
                mod_time: 1_700_000_000,
                use_fragments: true,
            });
            let mut fs = SquashfsFilesystem::open(Cursor::new(img), 0)
                .unwrap_or_else(|e| panic!("{}: open failed: {e}", compressor.name()));

            let root = fs.root().expect("root");
            assert_eq!(root.mode.map(|m| m & 0o7777), Some(0o755));
            let entries = fs.list_directory(&root).expect("list root");
            let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
            for want in [
                "hello.txt",
                "empty.bin",
                "big.bin",
                "link",
                "emptydir",
                "sub",
                "null",
            ] {
                assert!(
                    names.contains(&want),
                    "{}: root missing {want} (got {names:?})",
                    compressor.name()
                );
            }

            let hello = entries.iter().find(|e| e.name == "hello.txt").unwrap();
            assert_eq!(hello.mode.map(|m| m & 0o7777), Some(0o644));
            assert_eq!(
                fs.read_file(hello, 4096).expect("read hello"),
                b"hello squashfs\n"
            );

            // Multi-block file must come back byte-exact.
            let big = entries.iter().find(|e| e.name == "big.bin").unwrap();
            let got = fs.read_file(big, 10_000_000).expect("read big");
            let want: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
            assert_eq!(
                got.len(),
                want.len(),
                "{}: big.bin length",
                compressor.name()
            );
            assert_eq!(got, want, "{}: big.bin content", compressor.name());

            let empty = entries.iter().find(|e| e.name == "empty.bin").unwrap();
            assert_eq!(empty.size, 0);

            let link = entries.iter().find(|e| e.name == "link").unwrap();
            assert_eq!(link.symlink_target.as_deref(), Some("hello.txt"));

            let dev = entries.iter().find(|e| e.name == "null").unwrap();
            assert_eq!(
                dev.special_type.as_deref(),
                Some("char device (1, 3)"),
                "{}: device node",
                compressor.name()
            );

            let emptydir = entries.iter().find(|e| e.name == "emptydir").unwrap();
            assert!(fs
                .list_directory(emptydir)
                .expect("list emptydir")
                .is_empty());

            let sub = entries.iter().find(|e| e.name == "sub").unwrap();
            let subs = fs.list_directory(sub).expect("list sub");
            assert_eq!(subs.len(), 2, "{}: sub entry count", compressor.name());
            let nested = subs.iter().find(|e| e.name == "nested.txt").unwrap();
            assert_eq!(fs.read_file(nested, 4096).unwrap(), b"nested\n");
        }
    }

    /// **The phase-1 oracle test.** `unsquashfs` must accept our image and
    /// report exactly the tree we asked for. Our own reader agreeing with our
    /// own writer proves only that they share assumptions; this proves the
    /// bytes are real SquashFS.
    #[test]
    fn unsquashfs_accepts_our_image() {
        if !unsquashfs_available() {
            eprintln!("unsquashfs not on PATH — skipping writer oracle test");
            return;
        }
        for compressor in [Compressor::Gzip, Compressor::Zstd, Compressor::Xz] {
            let img = build(BuildOptions {
                compressor,
                block_size: 131_072,
                mod_time: 1_700_000_000,
                use_fragments: true,
            });
            let dir = tempfile::tempdir().expect("tempdir");
            let path = dir.path().join("out.squashfs");
            std::fs::write(&path, &img).expect("write image");

            // -s: superblock must parse.
            let stat = std::process::Command::new("unsquashfs")
                .arg("-s")
                .arg(&path)
                .output()
                .expect("run unsquashfs -s");
            let stat_txt = String::from_utf8_lossy(&stat.stdout).into_owned()
                + &String::from_utf8_lossy(&stat.stderr);
            assert!(
                stat_txt.contains("valid SQUASHFS 4:0 superblock"),
                "{}: unsquashfs rejected our superblock:\n{stat_txt}",
                compressor.name()
            );

            // -lls: the listing must match what we built.
            let out = std::process::Command::new("unsquashfs")
                .arg("-lls")
                .arg(&path)
                .output()
                .expect("run unsquashfs -lls");
            let txt = String::from_utf8_lossy(&out.stdout).into_owned()
                + &String::from_utf8_lossy(&out.stderr);
            assert!(
                out.status.success(),
                "{}: unsquashfs -lls failed:\n{txt}",
                compressor.name()
            );
            for want in [
                "squashfs-root/hello.txt",
                "squashfs-root/empty.bin",
                "squashfs-root/big.bin",
                "squashfs-root/emptydir",
                "squashfs-root/sub/nested.txt",
                "squashfs-root/sub/capped",
                "squashfs-root/link -> hello.txt",
                "squashfs-root/null",
            ] {
                assert!(
                    txt.contains(want),
                    "{}: unsquashfs listing missing {want}:\n{txt}",
                    compressor.name()
                );
            }
            assert!(
                txt.contains("-rw-r--r--") && txt.contains("crw-rw-rw-"),
                "{}: modes did not survive:\n{txt}",
                compressor.name()
            );

            // Extract and byte-compare. Creating a device node and applying a
            // `security.capability` xattr both need root, so extraction is run
            // with -no-xattrs and its exit status is not the assertion — the
            // per-file comparisons below are. (Those two features are verified
            // through the listing above and `decodes_our_xattrs` instead.)
            let dest = dir.path().join("x");
            let ex = std::process::Command::new("unsquashfs")
                .arg("-no-xattrs")
                .arg("-d")
                .arg(&dest)
                .arg(&path)
                .output()
                .expect("run unsquashfs extract");
            let ex_txt = String::from_utf8_lossy(&ex.stdout).into_owned()
                + &String::from_utf8_lossy(&ex.stderr);
            assert!(
                !ex_txt.contains("Data queue"),
                "{}: unsquashfs reported a decode problem:\n{ex_txt}",
                compressor.name()
            );
            assert_eq!(
                std::fs::read(dest.join("hello.txt")).unwrap(),
                b"hello squashfs\n"
            );
            assert_eq!(
                std::fs::read(dest.join("sub/nested.txt")).unwrap(),
                b"nested\n"
            );
            let want: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
            assert_eq!(
                std::fs::read(dest.join("big.bin")).unwrap(),
                want,
                "{}: multi-block file did not survive extraction",
                compressor.name()
            );
            assert_eq!(std::fs::read(dest.join("empty.bin")).unwrap().len(), 0);
            assert_eq!(
                std::fs::read_link(dest.join("link")).unwrap(),
                std::path::Path::new("hello.txt"),
                "{}: symlink target did not survive extraction",
                compressor.name()
            );
            assert!(
                dest.join("emptydir").is_dir(),
                "{}: empty directory did not survive extraction",
                compressor.name()
            );
            eprintln!("{}: unsquashfs round-trip OK", compressor.name());
        }
    }

    /// `unsquashfs` must find our `security.capability` and try to apply it.
    ///
    /// Applying it needs root, so the *attempt* is the oracle signal: the tool
    /// names the attribute and the file it belongs to, which it can only do by
    /// having walked our xattr ID table, followed the reference into the
    /// key/value blocks, and reassembled the `security.` prefix. That is the
    /// whole decision-D4 path, verified end to end without needing root.
    #[test]
    fn unsquashfs_decodes_our_xattrs() {
        if !unsquashfs_available() {
            eprintln!("unsquashfs not on PATH — skipping xattr oracle test");
            return;
        }
        let img = build(BuildOptions::default());
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("x.squashfs");
        std::fs::write(&path, &img).expect("write image");

        let out = std::process::Command::new("unsquashfs")
            .arg("-d")
            .arg(dir.path().join("out"))
            .arg(&path)
            .output()
            .expect("run unsquashfs");
        let txt = String::from_utf8_lossy(&out.stdout).into_owned()
            + &String::from_utf8_lossy(&out.stderr);

        // Either it applied the xattr (running as root) or it refused for lack
        // of privilege — both prove it decoded name + owner correctly.
        let named_attr = txt.contains("security.capability");
        assert!(
            named_attr,
            "unsquashfs never mentioned our xattr, so it did not decode the \
             xattr table:\n{txt}"
        );
        assert!(
            txt.contains("capped"),
            "unsquashfs did not attribute the xattr to the right file:\n{txt}"
        );
        eprintln!("unsquashfs decoded security.capability on /sub/capped");
    }

    /// A tree of many small files — the shape fragments exist for. A real
    /// rootfs is tens of thousands of these.
    fn many_small_files(count: usize) -> BuildNode {
        let children = (0..count)
            .map(|i| {
                BuildNode::file(
                    &format!("f{i:05}"),
                    0o644,
                    format!("small file number {i}, with some repetitive filler text\n")
                        .into_bytes(),
                )
            })
            .collect();
        BuildNode::dir("/", 0o755, children)
    }

    /// Packing tails into shared blocks must actually compress better than
    /// compressing each tiny file as its own stream — that is the entire reason
    /// fragments exist, so measure it rather than assume it.
    #[test]
    fn fragments_compress_small_files_better_than_separate_blocks() {
        let tree = many_small_files(400);
        let mut with = Cursor::new(Vec::new());
        let used_with = write_squashfs(
            &mut with,
            &tree,
            &BuildOptions {
                use_fragments: true,
                ..Default::default()
            },
        )
        .expect("build with fragments");
        let mut without = Cursor::new(Vec::new());
        let used_without = write_squashfs(
            &mut without,
            &tree,
            &BuildOptions {
                use_fragments: false,
                ..Default::default()
            },
        )
        .expect("build without fragments");

        eprintln!("400 small files: {used_with} bytes with fragments, {used_without} without");
        assert!(
            used_with < used_without,
            "fragments should shrink an image full of small files: \
             {used_with} vs {used_without}"
        );
    }

    /// Both fragment settings must produce images our reader and `unsquashfs`
    /// agree on, with file contents intact.
    #[test]
    fn fragmented_and_unfragmented_images_both_round_trip() {
        for use_fragments in [true, false] {
            let tree = many_small_files(40);
            let mut cur = Cursor::new(Vec::new());
            write_squashfs(
                &mut cur,
                &tree,
                &BuildOptions {
                    use_fragments,
                    ..Default::default()
                },
            )
            .expect("build");
            let img = cur.into_inner();

            let mut fs = SquashfsFilesystem::open(Cursor::new(img.clone()), 0)
                .unwrap_or_else(|e| panic!("fragments={use_fragments}: open: {e}"));
            let root = fs.root().expect("root");
            let entries = fs.list_directory(&root).expect("list");
            assert_eq!(entries.len(), 40, "fragments={use_fragments}: entry count");
            for (i, e) in entries.iter().enumerate() {
                let got = fs.read_file(e, 4096).expect("read");
                let want = format!("small file number {i}, with some repetitive filler text\n");
                assert_eq!(
                    String::from_utf8_lossy(&got),
                    want,
                    "fragments={use_fragments}: content of {}",
                    e.name
                );
            }

            if !unsquashfs_available() {
                continue;
            }
            let dir = tempfile::tempdir().expect("tempdir");
            let path = dir.path().join("f.squashfs");
            std::fs::write(&path, &img).expect("write");
            let out = std::process::Command::new("unsquashfs")
                .arg("-no-xattrs")
                .arg("-d")
                .arg(dir.path().join("x"))
                .arg(&path)
                .output()
                .expect("run unsquashfs");
            assert!(
                out.status.success(),
                "fragments={use_fragments}: unsquashfs failed:\n{}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
            let sample = std::fs::read(dir.path().join("x/f00007")).expect("extracted f00007");
            assert_eq!(
                String::from_utf8_lossy(&sample),
                "small file number 7, with some repetitive filler text\n",
                "fragments={use_fragments}: unsquashfs extracted the wrong bytes"
            );
        }
    }

    /// A file that is an exact multiple of the block size has no tail, so it
    /// must carry no fragment even when fragments are on — getting this wrong
    /// writes a zero-length fragment the reader would misread.
    #[test]
    fn exact_block_multiple_files_use_no_fragment() {
        let bs = 4096usize;
        let tree = BuildNode::dir(
            "/",
            0o755,
            vec![
                BuildNode::file("exact", 0o644, vec![0xAB; bs * 2]),
                BuildNode::file("ragged", 0o644, vec![0xCD; bs * 2 + 7]),
            ],
        );
        let mut cur = Cursor::new(Vec::new());
        write_squashfs(
            &mut cur,
            &tree,
            &BuildOptions {
                block_size: bs as u32,
                ..Default::default()
            },
        )
        .expect("build");

        let mut fs = SquashfsFilesystem::open(cur, 0).expect("open");
        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list");
        let exact = entries.iter().find(|e| e.name == "exact").unwrap();
        let ragged = entries.iter().find(|e| e.name == "ragged").unwrap();
        assert_eq!(fs.read_file(exact, 1 << 20).unwrap(), vec![0xAB; bs * 2]);
        assert_eq!(
            fs.read_file(ragged, 1 << 20).unwrap(),
            vec![0xCD; bs * 2 + 7]
        );
    }

    /// True when `mksquashfs` is on PATH. Like `unsquashfs`, it prints its
    /// banner and exits non-zero, so match text rather than status.
    fn mksquashfs_available() -> bool {
        std::process::Command::new("mksquashfs")
            .arg("-version")
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).contains("mksquashfs version"))
            .unwrap_or(false)
    }

    /// **Size parity with `mksquashfs`.** Build the same directory tree both
    /// ways and compare.
    ///
    /// This is the check that our images are not quietly much larger than the
    /// reference implementation's — the thing fragment packing exists for, and
    /// the thing a rebuild has to preserve so replacing one file in an appliance
    /// image doesn't balloon it.
    ///
    /// The tolerance is deliberately loose in the *upward* direction only. We
    /// don't implement mksquashfs's whole-file duplicate detection, and our
    /// gzip is `flate2` rather than the C zlib, so small differences either way
    /// are expected; what would matter is a structural regression that makes us
    /// multiples larger.
    #[test]
    fn image_size_is_comparable_to_mksquashfs() {
        if !mksquashfs_available() {
            eprintln!("mksquashfs not on PATH — skipping size-parity test");
            return;
        }
        // Build a corpus with the shape that actually stresses packing: lots of
        // small text files, a few larger ones, nested directories, symlinks.
        // `RB_SQUASHFS_SIZE_CORPUS` points this at a real tree instead — a
        // synthetic corpus is a weak proxy for a distro rootfs.
        let dir = tempfile::tempdir().expect("tempdir");
        let src = match std::env::var("RB_SQUASHFS_SIZE_CORPUS") {
            Ok(p) => std::path::PathBuf::from(p),
            Err(_) => build_synthetic_corpus(dir.path()),
        };

        let ours_path = dir.path().join("ours.squashfs");
        let tree = BuildNode::from_host_dir(&src).expect("read host tree");
        let mut out = std::fs::File::create(&ours_path).expect("create");
        let ours = write_squashfs(&mut out, &tree, &BuildOptions::default()).expect("build");
        drop(out);

        let ref_path = dir.path().join("ref.squashfs");
        let st = std::process::Command::new("mksquashfs")
            .arg(&src)
            .arg(&ref_path)
            .args(["-no-xattrs", "-noappend", "-quiet", "-no-progress"])
            .output()
            .expect("run mksquashfs");
        assert!(
            ref_path.exists(),
            "mksquashfs produced nothing:\n{}{}",
            String::from_utf8_lossy(&st.stdout),
            String::from_utf8_lossy(&st.stderr)
        );
        let theirs = std::fs::metadata(&ref_path).unwrap().len();

        let ratio = ours as f64 / theirs as f64;
        eprintln!(
            "corpus {}: ours {ours} bytes, mksquashfs {theirs} bytes (ratio {ratio:.3})",
            src.display()
        );
        assert!(
            ratio < 1.30,
            "our image is {ratio:.2}x the size of mksquashfs's \
             ({ours} vs {theirs}) — packing has regressed"
        );

        // A similarly-sized image that doesn't decode would pass the ratio check
        // and be worthless, so hold both to the same corpus: `unsquashfs` must
        // list exactly as many entries out of ours as out of the reference.
        if unsquashfs_available() {
            let count = |p: &std::path::Path| -> usize {
                let out = std::process::Command::new("unsquashfs")
                    .arg("-lls")
                    .arg(p)
                    .output()
                    .expect("run unsquashfs -lls");
                String::from_utf8_lossy(&out.stdout)
                    .lines()
                    .filter(|l| {
                        l.len() > 10 && "-dlbcps".contains(&l[0..1]) && l.contains("squashfs-root")
                    })
                    .count()
            };
            let ours_entries = count(&ours_path);
            let their_entries = count(&ref_path);
            assert!(
                ours_entries > 0,
                "unsquashfs listed nothing from our image — it is not readable"
            );
            assert_eq!(
                ours_entries, their_entries,
                "our image holds {ours_entries} entries but mksquashfs's holds \
                 {their_entries} for the same tree"
            );
            eprintln!("both images list {ours_entries} entries");
        }
    }

    /// Lots of small text files, a few multi-block ones, nested directories.
    fn build_synthetic_corpus(base: &std::path::Path) -> std::path::PathBuf {
        let src = base.join("src");
        std::fs::create_dir(&src).unwrap();
        for d in 0..8 {
            let sub = src.join(format!("dir{d}"));
            std::fs::create_dir(&sub).unwrap();
            for f in 0..60 {
                let body = format!(
                    "# config file {d}/{f}\nkey = value {f}\n\
                     # a line of filler that repeats across files so the codec \
                     has something to find\n"
                );
                std::fs::write(sub.join(format!("conf{f}.cfg")), body).unwrap();
            }
        }
        // A couple of multi-block files.
        for n in 0..3 {
            let big: Vec<u8> = (0..400_000u32)
                .map(|i| (i.wrapping_mul(n + 1) % 251) as u8)
                .collect();
            std::fs::write(src.join(format!("big{n}.bin")), big).unwrap();
        }
        src
    }

    /// A file whose content is a host path must stream in and come back
    /// byte-exact — the multi-block, large-file path that must not hold the
    /// whole file in RAM.
    #[test]
    fn host_file_content_streams_and_round_trips() {
        let dir = tempfile::tempdir().expect("tempdir");
        // Several blocks plus a ragged tail, with enough entropy that it won't
        // all collapse to one stored block.
        let big: Vec<u8> = (0..500_000u32)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
            .collect();
        let host = dir.path().join("payload.bin");
        std::fs::write(&host, &big).unwrap();

        let tree = BuildNode {
            name: "/".into(),
            mode: 0o755,
            uid: 0,
            gid: 0,
            mtime: 0,
            xattrs: Vec::new(),
            kind: BuildKind::Dir(vec![BuildNode {
                name: "payload.bin".into(),
                mode: 0o644,
                uid: 0,
                gid: 0,
                mtime: 0,
                xattrs: Vec::new(),
                kind: BuildKind::File(
                    FileContent::host_file(host.clone()).expect("stat host file"),
                ),
            }]),
        };

        let mut cur = Cursor::new(Vec::new());
        write_squashfs(&mut cur, &tree, &BuildOptions::default()).expect("build");
        let mut fs = SquashfsFilesystem::open(cur, 0).expect("open");
        let root = fs.root().expect("root");
        let entry = fs.list_directory(&root).expect("list")[0].clone();
        assert_eq!(entry.size, big.len() as u64);
        let got = fs.read_file(&entry, big.len() + 1).expect("read");
        assert_eq!(got, big, "streamed host file did not round-trip byte-exact");
    }

    /// A content source that reports a longer length than it can produce must
    /// fail loudly, not silently truncate the file in the image.
    #[test]
    fn short_content_source_is_an_error_not_a_truncation() {
        // A host file we delete after the node is built, so its stat'd length
        // outlives its bytes.
        let dir = tempfile::tempdir().expect("tempdir");
        let host = dir.path().join("vanishes.bin");
        std::fs::write(&host, vec![0u8; 200_000]).unwrap();
        let content = FileContent::host_file(host.clone()).expect("stat");
        std::fs::remove_file(&host).unwrap();

        let tree = BuildNode {
            name: "/".into(),
            mode: 0o755,
            uid: 0,
            gid: 0,
            mtime: 0,
            xattrs: Vec::new(),
            kind: BuildKind::Dir(vec![BuildNode {
                name: "vanishes.bin".into(),
                mode: 0o644,
                uid: 0,
                gid: 0,
                mtime: 0,
                xattrs: Vec::new(),
                kind: BuildKind::File(content),
            }]),
        };
        let mut cur = Cursor::new(Vec::new());
        let err = write_squashfs(&mut cur, &tree, &BuildOptions::default())
            .expect_err("a vanished source must fail the build");
        // Either the open fails (file gone) or the read ends early — both are
        // errors, never a silently short image.
        let _ = err;
    }

    /// **The phase-2 bridge round-trip.** Read a real image into a tree, rebuild
    /// it, read the rebuild back, and prove the two trees are identical.
    ///
    /// This drives the whole reader -> `read_build_tree` -> writer ->
    /// `read_build_tree` path over a real mksquashfs image (the Ubuntu 12.04
    /// live CD's `casper/filesystem.squashfs`). It compares the in-memory source
    /// tree against the tree read back from our rebuild, field for field: name,
    /// mode, uid, gid, symlink target, device major/minor, xattrs, and every
    /// regular file's bytes.
    ///
    /// The reader is independently trustworthy here — it is oracle-validated
    /// against `unsquashfs` at 123k entries elsewhere — so "the rebuild reads
    /// back identical" is a real fidelity check on the *writer*, not two halves
    /// agreeing on a shared mistake: a writer that dropped a file, corrupted
    /// content or rewrote a mode produces a tree that differs from the source.
    #[test]
    fn rebuilds_a_real_image_faithfully() {
        use crate::fs::squashfs::SquashfsFilesystem;

        let path = std::env::var("RB_SQUASHFS_ISO").unwrap_or_else(|_| {
            "/Users/dani/Downloads/ubuntu-12.04-desktop-powerpc.iso".to_string()
        });
        let Ok(mut file) = std::fs::File::open(&path) else {
            eprintln!("{path} absent — skipping bridge round-trip");
            return;
        };
        let Some(offset) = find_squashfs_offset_for_test(&mut file) else {
            eprintln!("no squashfs in {path} — skipping");
            return;
        };

        // Rebuild a representative subtree rather than the whole 558 MB image:
        // /etc is ~1500 files with modes, symlinks and nested directories.
        // `read_build_subtree` decompresses only /etc, not the whole rootfs.
        let mut fs = SquashfsFilesystem::open(file, offset).expect("open source");
        let etc = fs.read_build_subtree("etc").expect("image has no /etc");

        let mut cur = Cursor::new(Vec::new());
        write_squashfs(&mut cur, &etc, &BuildOptions::default()).expect("rebuild");

        let mut rebuilt = SquashfsFilesystem::open(cur, 0).expect("open rebuild");
        let round = rebuilt.read_build_tree().expect("read rebuild tree");

        // The rebuild's root name is empty; `etc`'s is "/". Compare their
        // children.
        let mut checked = 0usize;
        compare_trees(&etc, &round, "/", &mut checked);
        eprintln!("bridge round-trip: {checked} nodes identical after rebuild");
        assert!(
            checked > 200,
            "only compared {checked} nodes — the walk did not cover /etc"
        );
    }

    /// Recursively assert two build trees are identical. Children are matched by
    /// name (directory order is an implementation detail), so a missing or extra
    /// node fails as a count mismatch at its parent.
    fn compare_trees(a: &BuildNode, b: &BuildNode, path: &str, checked: &mut usize) {
        assert_eq!(a.mode, b.mode, "mode differs at {path}");
        assert_eq!(a.uid, b.uid, "uid differs at {path}");
        assert_eq!(a.gid, b.gid, "gid differs at {path}");
        assert_eq!(a.mtime, b.mtime, "mtime differs at {path}");
        assert_eq!(a.xattrs, b.xattrs, "xattrs differ at {path}");
        *checked += 1;

        match (&a.kind, &b.kind) {
            (BuildKind::Dir(ac), BuildKind::Dir(bc)) => {
                assert_eq!(
                    ac.len(),
                    bc.len(),
                    "child count differs at {path}: {} vs {}",
                    ac.len(),
                    bc.len()
                );
                let by_name = |v: &[BuildNode]| {
                    let mut m: std::collections::HashMap<String, usize> =
                        std::collections::HashMap::new();
                    for (i, c) in v.iter().enumerate() {
                        m.insert(c.name.clone(), i);
                    }
                    m
                };
                let bmap = by_name(bc);
                for child in ac {
                    let j = *bmap
                        .get(&child.name)
                        .unwrap_or_else(|| panic!("{path}/{} missing after rebuild", child.name));
                    compare_trees(child, &bc[j], &format!("{path}/{}", child.name), checked);
                }
            }
            (BuildKind::File(fa), BuildKind::File(fb)) => {
                let ba = read_content(fa);
                let bb = read_content(fb);
                assert_eq!(ba.len(), bb.len(), "file length differs at {path}");
                assert_eq!(ba, bb, "file content differs at {path}");
            }
            (BuildKind::Symlink(ta), BuildKind::Symlink(tb)) => {
                assert_eq!(ta, tb, "symlink target differs at {path}");
            }
            (
                BuildKind::BlockDev {
                    major: ma,
                    minor: na,
                },
                BuildKind::BlockDev {
                    major: mb,
                    minor: nb,
                },
            )
            | (
                BuildKind::CharDev {
                    major: ma,
                    minor: na,
                },
                BuildKind::CharDev {
                    major: mb,
                    minor: nb,
                },
            ) => {
                assert_eq!((ma, na), (mb, nb), "device numbers differ at {path}");
            }
            (BuildKind::Fifo, BuildKind::Fifo) | (BuildKind::Socket, BuildKind::Socket) => {}
            (x, y) => panic!(
                "node kind differs at {path}: {} vs {}",
                kind_name(x),
                kind_name(y)
            ),
        }
    }

    fn read_content(c: &FileContent) -> Vec<u8> {
        match c {
            FileContent::Bytes(b) => b.clone(),
            FileContent::HostFile { path, .. } => std::fs::read(path).expect("read host file"),
        }
    }

    fn kind_name(k: &BuildKind) -> &'static str {
        match k {
            BuildKind::Dir(_) => "dir",
            BuildKind::File(_) => "file",
            BuildKind::Symlink(_) => "symlink",
            BuildKind::BlockDev { .. } => "blockdev",
            BuildKind::CharDev { .. } => "chardev",
            BuildKind::Fifo => "fifo",
            BuildKind::Socket => "socket",
        }
    }

    /// Scan a file for the `hsqs` magic at a 4-byte boundary.
    fn find_squashfs_offset_for_test(file: &mut std::fs::File) -> Option<u64> {
        use std::io::{Read, Seek, SeekFrom};
        file.seek(SeekFrom::Start(0)).ok()?;
        let mut buf = vec![0u8; 1 << 20];
        let mut base = 0u64;
        let mut carry = Vec::new();
        loop {
            let n = file.read(&mut buf).ok()?;
            if n == 0 {
                return None;
            }
            let mut hay = carry.clone();
            hay.extend_from_slice(&buf[..n]);
            let mut i = 0;
            while i + 4 <= hay.len() {
                if &hay[i..i + 4] == b"hsqs" {
                    let off = base + i as u64 - carry.len() as u64;
                    if off.is_multiple_of(4) {
                        return Some(off);
                    }
                }
                i += 1;
            }
            carry = hay[hay.len().saturating_sub(3)..].to_vec();
            base += n as u64;
        }
    }

    /// A codec we can read but not encode must be refused up front, before any
    /// bytes are written — never silently substituted with a different one.
    #[test]
    fn refuses_codecs_it_cannot_encode() {
        for c in [Compressor::Lz4, Compressor::Lzo, Compressor::Lzma] {
            let mut cur = Cursor::new(Vec::new());
            let err = write_squashfs(
                &mut cur,
                &sample_tree(),
                &BuildOptions {
                    compressor: c,
                    ..Default::default()
                },
            )
            .expect_err("should refuse");
            assert!(
                err.to_string().contains(c.name()),
                "error should name the codec, got: {err}"
            );
            assert!(
                cur.into_inner().is_empty(),
                "{}: refused build still wrote bytes",
                c.name()
            );
        }
    }
}
