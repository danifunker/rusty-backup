//! SquashFS (v4.0) — read-only.
//!
//! SquashFS is the read-only compressed filesystem Linux appliance and live
//! images are built from, so it shows up as the root filesystem of Buildroot
//! images, MiSTer/SuperStation-style device images, and inside the ISO 9660 of
//! practically every live CD.
//!
//! # Layout
//!
//! A SquashFS image is a 96-byte superblock followed by several tables, each of
//! which is a chain of **metadata blocks**: a 2-byte length header (top bit set
//! = stored uncompressed) followed by at most 8 KiB of payload. Inodes and
//! directory entries are addressed by a 48-bit *reference* packing the start of
//! the containing metadata block together with a byte offset inside its
//! decompressed payload — so reading an inode means decompressing one metadata
//! block and indexing into it, never seeking to a byte offset directly.
//!
//! File contents are separate: each file owns a list of individually compressed
//! blocks, and the tail of a file smaller than one block may instead live packed
//! together with other tails in a shared **fragment**.
//!
//! # Scope
//!
//! Read-only: browse the tree, read files and symlinks. SquashFS is built
//! offline by `mksquashfs` and has no in-place write story worth emulating, so
//! there is deliberately no editable implementation — matching how the format is
//! actually used.
//!
//! Supports the gzip, XZ/LZMA, LZ4 and Zstandard compressors. LZO is the one
//! upstream compressor left out: it is rare in practice and would pull in a new
//! dependency for it. An LZO image is refused by name rather than misparsed.

use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};

use super::entry::{EntryType, FileEntry};
use super::filesystem::{Filesystem, FilesystemError};

/// `hsqs` little-endian — a SquashFS superblock always starts here.
pub(crate) const SQUASHFS_MAGIC: u32 = 0x7371_7368;

/// Metadata blocks decompress to at most 8 KiB.
pub(crate) const METADATA_BLOCK_SIZE: usize = 8192;

/// Superblock length in bytes.
pub(crate) const SUPERBLOCK_SIZE: usize = 96;

/// Inode types. 1-7 are the "basic" forms; 8-14 are the extended forms that add
/// fields (hard-link counts, sparse-file support, xattr indices).
pub(crate) const INODE_DIR: u16 = 1;
pub(crate) const INODE_FILE: u16 = 2;
pub(crate) const INODE_SYMLINK: u16 = 3;
pub(crate) const INODE_BLKDEV: u16 = 4;
pub(crate) const INODE_CHRDEV: u16 = 5;
pub(crate) const INODE_FIFO: u16 = 6;
pub(crate) const INODE_SOCKET: u16 = 7;
pub(crate) const INODE_EXT_DIR: u16 = 8;
pub(crate) const INODE_EXT_FILE: u16 = 9;
pub(crate) const INODE_EXT_SYMLINK: u16 = 10;
pub(crate) const INODE_EXT_BLKDEV: u16 = 11;
pub(crate) const INODE_EXT_CHRDEV: u16 = 12;
pub(crate) const INODE_EXT_FIFO: u16 = 13;
pub(crate) const INODE_EXT_SOCKET: u16 = 14;

/// Which compressor the image's blocks use.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Compressor {
    Gzip,
    Lzma,
    Lzo,
    Xz,
    Lz4,
    Zstd,
}

impl Compressor {
    pub fn from_id(id: u16) -> Option<Self> {
        match id {
            1 => Some(Self::Gzip),
            2 => Some(Self::Lzma),
            3 => Some(Self::Lzo),
            4 => Some(Self::Xz),
            5 => Some(Self::Lz4),
            6 => Some(Self::Zstd),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Gzip => "gzip",
            Self::Lzma => "lzma",
            Self::Lzo => "lzo",
            Self::Xz => "xz",
            Self::Lz4 => "lz4",
            Self::Zstd => "zstd",
        }
    }
}

/// Parsed superblock. Field order follows the on-disk layout.
///
/// Fields past `fragment_table_start` are not needed to *browse* an image, but
/// a faithful rebuild cannot do without them: `id_table_start` is the only way
/// to turn an inode's 16-bit uid/gid index into a real uid/gid, and
/// `xattr_id_table_start` is where capability bits on appliance binaries live.
/// See `docs/squashfs_edit.md` §3.
#[derive(Debug, Clone)]
struct Superblock {
    /// Writer input (phase 1): the rebuilt image restates this.
    #[allow(dead_code)]
    inode_count: u32,
    /// Writer input (phase 1).
    #[allow(dead_code)]
    mod_time: u32,
    block_size: u32,
    fragment_count: u32,
    compressor: Compressor,
    /// Writer input (phase 1) — must agree with `block_size`.
    #[allow(dead_code)]
    block_log: u16,
    flags: u16,
    /// Number of entries in the ID table (uid/gid pool).
    no_ids: u16,
    root_inode_ref: u64,
    bytes_used: u64,
    id_table_start: u64,
    /// `u64::MAX` when the image carries no extended attributes.
    xattr_id_table_start: u64,
    inode_table_start: u64,
    directory_table_start: u64,
    fragment_table_start: u64,
    /// NFS export (inode-number -> inode-reference) table; `u64::MAX` when the
    /// image was built without `-no-exports`. Writer input (phase 1): the
    /// rebuild has to regenerate this or explicitly drop it.
    #[allow(dead_code)]
    export_table_start: u64,
}

impl Superblock {
    fn parse(buf: &[u8]) -> Result<Self, FilesystemError> {
        if buf.len() < SUPERBLOCK_SIZE {
            return Err(FilesystemError::InvalidData(
                "squashfs: truncated superblock".into(),
            ));
        }
        let magic = u32le(buf, 0);
        if magic != SQUASHFS_MAGIC {
            return Err(FilesystemError::InvalidData(format!(
                "squashfs: bad magic 0x{magic:08X}, expected 0x{SQUASHFS_MAGIC:08X}"
            )));
        }

        let version_major = u16le(buf, 28);
        let version_minor = u16le(buf, 30);
        if version_major != 4 {
            // v1-v3 differ structurally (different superblock, 16-bit sizes,
            // no fragment table in v1). Refuse rather than misread them.
            return Err(FilesystemError::Unsupported(format!(
                "squashfs: version {version_major}.{version_minor} is not supported (need 4.0)"
            )));
        }

        let compressor_id = u16le(buf, 20);
        let compressor = Compressor::from_id(compressor_id).ok_or_else(|| {
            FilesystemError::Unsupported(format!("squashfs: unknown compressor id {compressor_id}"))
        })?;
        if compressor == Compressor::Lzo {
            return Err(FilesystemError::Unsupported(
                "squashfs: LZO-compressed images are not supported".into(),
            ));
        }

        let block_size = u32le(buf, 12);
        // block_log must agree with block_size; a mismatch means we are not
        // looking at a real superblock even though the magic matched.
        let block_log = u16le(buf, 22);
        if block_size == 0 || block_size != 1u32 << block_log {
            return Err(FilesystemError::InvalidData(format!(
                "squashfs: block size {block_size} disagrees with block_log {block_log}"
            )));
        }

        Ok(Self {
            inode_count: u32le(buf, 4),
            mod_time: u32le(buf, 8),
            block_size,
            fragment_count: u32le(buf, 16),
            compressor,
            block_log,
            flags: u16le(buf, 24),
            no_ids: u16le(buf, 26),
            root_inode_ref: u64le(buf, 32),
            bytes_used: u64le(buf, 40),
            id_table_start: u64le(buf, 48),
            xattr_id_table_start: u64le(buf, 56),
            inode_table_start: u64le(buf, 64),
            directory_table_start: u64le(buf, 72),
            fragment_table_start: u64le(buf, 80),
            export_table_start: u64le(buf, 88),
        })
    }

    /// Bit 4 of the flags: file data is stored uncompressed.
    fn data_uncompressed(&self) -> bool {
        self.flags & 0x0002 != 0
    }
}

/// Sentinel in an inode's `xattr_idx` meaning "no extended attributes".
pub(crate) const SQUASHFS_INVALID_XATTR: u32 = 0xFFFF_FFFF;

/// A decoded inode.
///
/// The common-header fields (`mode` .. `inode_number`) are read for every inode
/// type. Browsing only needs `kind`, but a rebuild has to reproduce ownership,
/// permissions and timestamps exactly — getting those wrong on an appliance
/// root image is the difference between a working and a broken device.
#[derive(Debug, Clone)]
struct Inode {
    kind: u16,
    /// Permission bits only (`0o7777`); the file-type bits come from `kind`.
    /// Combine via [`Inode::unix_mode`].
    mode: u16,
    /// Index into the ID table, *not* a uid. Resolve with
    /// [`SquashfsFilesystem::resolve_id`].
    uid_idx: u16,
    gid_idx: u16,
    /// Seconds since the Unix epoch.
    mtime: u32,
    inode_number: u32,
    /// Hard-link count. Absent from the *basic* file inode (which cannot be
    /// hardlinked), where it stays 1.
    nlink: u32,
    /// Index into the xattr ID table, or [`SQUASHFS_INVALID_XATTR`].
    xattr_idx: u32,
    /// Directories: where their entries live in the directory table.
    dir_block_start: u32,
    dir_offset: u16,
    dir_size: u32,
    /// Files: where the data lives.
    file_start: u64,
    file_size: u64,
    block_sizes: Vec<u32>,
    fragment_index: u32,
    fragment_offset: u32,
    /// Symlinks.
    symlink_target: String,
    /// Block/char devices, decoded from the packed 32-bit `device` field.
    dev_major: u32,
    dev_minor: u32,
}

impl Inode {
    fn empty(kind: u16) -> Self {
        Self {
            kind,
            mode: 0,
            uid_idx: 0,
            gid_idx: 0,
            mtime: 0,
            inode_number: 0,
            nlink: 1,
            xattr_idx: SQUASHFS_INVALID_XATTR,
            dir_block_start: 0,
            dir_offset: 0,
            dir_size: 0,
            file_start: 0,
            file_size: 0,
            block_sizes: Vec::new(),
            fragment_index: 0xFFFF_FFFF,
            fragment_offset: 0,
            symlink_target: String::new(),
            dev_major: 0,
            dev_minor: 0,
        }
    }

    /// The full Unix mode: the S_IF* type bits implied by `kind`, OR'd with the
    /// stored permission bits. SquashFS stores only the low 12 bits on disk.
    fn unix_mode(&self) -> u32 {
        let type_bits: u32 = match self.kind {
            INODE_DIR | INODE_EXT_DIR => 0o040_000,
            INODE_FILE | INODE_EXT_FILE => 0o100_000,
            INODE_SYMLINK | INODE_EXT_SYMLINK => 0o120_000,
            INODE_BLKDEV | INODE_EXT_BLKDEV => 0o060_000,
            INODE_CHRDEV | INODE_EXT_CHRDEV => 0o020_000,
            INODE_FIFO | INODE_EXT_FIFO => 0o010_000,
            INODE_SOCKET | INODE_EXT_SOCKET => 0o140_000,
            _ => 0,
        };
        type_bits | (self.mode as u32 & 0o7777)
    }

    /// Human-readable label for the special-file kinds, for the browse view.
    fn special_label(&self) -> Option<&'static str> {
        match self.kind {
            INODE_BLKDEV | INODE_EXT_BLKDEV => Some("block device"),
            INODE_CHRDEV | INODE_EXT_CHRDEV => Some("char device"),
            INODE_FIFO | INODE_EXT_FIFO => Some("fifo"),
            INODE_SOCKET | INODE_EXT_SOCKET => Some("socket"),
            _ => None,
        }
    }

    fn entry_type(&self) -> EntryType {
        match self.kind {
            INODE_DIR | INODE_EXT_DIR => EntryType::Directory,
            INODE_FILE | INODE_EXT_FILE => EntryType::File,
            INODE_SYMLINK | INODE_EXT_SYMLINK => EntryType::Symlink,
            _ => EntryType::Special,
        }
    }
}

/// One entry in the fragment table: where a shared tail block lives.
#[derive(Debug, Clone, Copy)]
struct FragmentEntry {
    start: u64,
    size: u32,
}

/// One extended attribute, with the on-disk prefix already applied to `name`
/// (SquashFS stores `security.capability` as prefix-id 2 + `"capability"`).
///
/// Appliance images encode Linux capabilities here — `security.capability` on
/// `ping`, `dumpcap` and friends — so these have to survive a rebuild or the
/// image boots with subtly broken binaries. See `docs/squashfs_edit.md` D4.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Xattr {
    pub name: String,
    pub value: Vec<u8>,
}

/// One 16-byte entry in the xattr ID table: where an inode's attribute list
/// lives, and how big it is.
#[derive(Debug, Clone, Copy)]
struct XattrIdEntry {
    /// Metadata reference (block << 16 | offset) into the xattr key/value table.
    xattr_ref: u64,
    /// Number of key/value pairs.
    count: u32,
    /// Total uncompressed byte length of those pairs.
    size: u32,
}

/// Prefix IDs in an xattr key's `type` field. The high bit (0x0100) instead
/// marks an out-of-line value — a back-reference to an identical value written
/// earlier, which is how mksquashfs deduplicates repeated attributes.
///
/// Read today only by [`SquashfsFilesystem::read_xattrs`], whose consumers land
/// in phase 1 (the rebuild) and phase 2 (browse-view display) — see
/// `docs/squashfs_edit.md`.
const XATTR_PREFIX_MASK: u16 = 0xFF;
const XATTR_OUT_OF_LINE: u16 = 0x0100;

/// A mounted SquashFS image.
pub struct SquashfsFilesystem<R: Read + Seek> {
    reader: R,
    /// Byte offset of the image within `reader` (partition start, or 0).
    offset: u64,
    sb: Superblock,
    /// Decompressed metadata blocks, keyed by their start offset within the
    /// image, stored as `(payload, on-disk length)`. Chasing an inode reference
    /// decompresses one block; directory listings hit the same handful
    /// repeatedly, so caching turns a directory walk from O(entries)
    /// decompressions into O(blocks). The on-disk length is kept because it is
    /// what locates the *next* block in a chain, and it is not recoverable from
    /// the decompressed payload.
    metadata_cache: HashMap<u64, (Vec<u8>, u64)>,
    fragments: Vec<FragmentEntry>,
    /// The uid/gid pool. Inodes store a 16-bit *index* into this, not an ID, so
    /// nothing can report real ownership without it.
    ids: Vec<u32>,
    /// Where the xattr key/value metadata blocks begin. Zero when the image has
    /// no extended attributes.
    xattr_table_start: u64,
    /// The xattr ID table, indexed by an inode's `xattr_idx`. Loaded eagerly
    /// (it is small — 16 bytes per *distinct* attribute set, not per inode);
    /// the key/value pairs themselves are read on demand.
    xattr_ids: Vec<XattrIdEntry>,
}

impl<R: Read + Seek> SquashfsFilesystem<R> {
    /// Open the SquashFS image starting at `offset` within `reader`.
    pub fn open(mut reader: R, offset: u64) -> Result<Self, FilesystemError> {
        let mut buf = [0u8; SUPERBLOCK_SIZE];
        reader
            .seek(SeekFrom::Start(offset))
            .map_err(FilesystemError::Io)?;
        reader.read_exact(&mut buf).map_err(FilesystemError::Io)?;
        let sb = Superblock::parse(&buf)?;

        let mut fs = Self {
            reader,
            offset,
            sb,
            metadata_cache: HashMap::new(),
            fragments: Vec::new(),
            ids: Vec::new(),
            xattr_table_start: 0,
            xattr_ids: Vec::new(),
        };
        fs.load_fragment_table()?;
        fs.load_id_table()?;
        fs.load_xattr_table()?;
        Ok(fs)
    }

    /// Does `reader` hold a SquashFS image at `offset`?
    ///
    /// Checks the magic *and* parses the superblock, so a chance 4-byte match in
    /// unrelated data does not get reported as a filesystem.
    pub fn detect(reader: &mut R, offset: u64) -> bool {
        let mut buf = [0u8; SUPERBLOCK_SIZE];
        if reader.seek(SeekFrom::Start(offset)).is_err() {
            return false;
        }
        if reader.read_exact(&mut buf).is_err() {
            return false;
        }
        Superblock::parse(&buf).is_ok()
    }

    /// Read and decompress the metadata block starting at `pos` (an offset
    /// within the image), returning its payload and where the next block begins.
    fn read_metadata_block(&mut self, pos: u64) -> Result<(Vec<u8>, u64), FilesystemError> {
        if let Some((payload, on_disk_len)) = self.metadata_cache.get(&pos) {
            return Ok((payload.clone(), pos + 2 + on_disk_len));
        }

        self.reader
            .seek(SeekFrom::Start(self.offset + pos))
            .map_err(FilesystemError::Io)?;
        let mut header = [0u8; 2];
        self.reader
            .read_exact(&mut header)
            .map_err(FilesystemError::Io)?;
        let raw = u16::from_le_bytes(header);
        // Top bit set means the payload was stored as-is because compressing it
        // would not have helped.
        let stored_uncompressed = raw & 0x8000 != 0;
        let on_disk_len = (raw & 0x7FFF) as usize;
        if on_disk_len == 0 || on_disk_len > METADATA_BLOCK_SIZE {
            return Err(FilesystemError::InvalidData(format!(
                "squashfs: metadata block at {pos} has implausible length {on_disk_len}"
            )));
        }

        let mut payload = vec![0u8; on_disk_len];
        self.reader
            .read_exact(&mut payload)
            .map_err(FilesystemError::Io)?;

        let decoded = if stored_uncompressed {
            payload
        } else {
            decompress(self.sb.compressor, &payload, METADATA_BLOCK_SIZE)?
        };

        self.metadata_cache
            .insert(pos, (decoded.clone(), on_disk_len as u64));
        Ok((decoded, pos + 2 + on_disk_len as u64))
    }

    /// Read `len` bytes from the metadata chain beginning at `start`, skipping
    /// `offset` bytes into the first block's payload.
    ///
    /// A run of metadata can straddle block boundaries, so this keeps pulling
    /// successive blocks until it has enough — the caller never has to know how
    /// the bytes were split up.
    fn read_metadata_span(
        &mut self,
        start: u64,
        offset: u16,
        len: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let mut out = Vec::with_capacity(len);
        let mut pos = start;
        let mut skip = offset as usize;

        while out.len() < len {
            let (block, next) = self.read_metadata_block(pos)?;
            if skip >= block.len() {
                // A directory/inode reference pointing past the end of its own
                // block is corruption, not something to silently ride past.
                if next <= pos {
                    return Err(FilesystemError::InvalidData(
                        "squashfs: metadata chain does not advance".into(),
                    ));
                }
                skip -= block.len();
                pos = next;
                continue;
            }
            let take = (len - out.len()).min(block.len() - skip);
            out.extend_from_slice(&block[skip..skip + take]);
            skip = 0;
            if out.len() < len {
                if next <= pos {
                    return Err(FilesystemError::InvalidData(
                        "squashfs: metadata chain does not advance".into(),
                    ));
                }
                pos = next;
            }
        }
        Ok(out)
    }

    /// The fragment table is a list of 16-byte entries, reached through a table
    /// of 64-bit pointers to the metadata blocks holding them.
    fn load_fragment_table(&mut self) -> Result<(), FilesystemError> {
        let count = self.sb.fragment_count as usize;
        if count == 0 || self.sb.fragment_table_start == u64::MAX {
            return Ok(());
        }

        // Each metadata block holds 8192/16 = 512 fragment entries.
        let index_count = count.div_ceil(512);
        let mut index = vec![0u8; index_count * 8];
        self.reader
            .seek(SeekFrom::Start(self.offset + self.sb.fragment_table_start))
            .map_err(FilesystemError::Io)?;
        self.reader
            .read_exact(&mut index)
            .map_err(FilesystemError::Io)?;

        let mut entries = Vec::with_capacity(count);
        for i in 0..index_count {
            let block_start = u64le(&index, i * 8);
            let (block, _) = self.read_metadata_block(block_start)?;
            let mut off = 0;
            while off + 16 <= block.len() && entries.len() < count {
                entries.push(FragmentEntry {
                    start: u64le(&block, off),
                    size: u32le(&block, off + 8),
                });
                off += 16;
            }
        }
        self.fragments = entries;
        Ok(())
    }

    /// The ID table is the uid/gid pool: a table of 64-bit pointers to metadata
    /// blocks, each packed with 32-bit IDs. Inodes reference it by index, so
    /// ownership is unreportable — and unreproducible on rebuild — without it.
    fn load_id_table(&mut self) -> Result<(), FilesystemError> {
        let count = self.sb.no_ids as usize;
        if count == 0 || self.sb.id_table_start == u64::MAX {
            return Ok(());
        }

        // 8192 / 4 = 2048 IDs per metadata block.
        let index_count = count.div_ceil(2048);
        let mut index = vec![0u8; index_count * 8];
        self.reader
            .seek(SeekFrom::Start(self.offset + self.sb.id_table_start))
            .map_err(FilesystemError::Io)?;
        self.reader
            .read_exact(&mut index)
            .map_err(FilesystemError::Io)?;

        let mut ids = Vec::with_capacity(count);
        for i in 0..index_count {
            let block_start = u64le(&index, i * 8);
            let (block, _) = self.read_metadata_block(block_start)?;
            let mut off = 0;
            while off + 4 <= block.len() && ids.len() < count {
                ids.push(u32le(&block, off));
                off += 4;
            }
        }
        self.ids = ids;
        Ok(())
    }

    /// Turn an inode's 16-bit uid/gid index into the real ID. An out-of-range
    /// index means a damaged or truncated ID table; report 0 rather than
    /// failing the whole listing, since browsing is still useful.
    fn resolve_id(&self, idx: u16) -> u32 {
        self.ids.get(idx as usize).copied().unwrap_or(0)
    }

    /// The xattr ID table is reached through a 16-byte header (where the
    /// key/value blocks live, and how many ID entries there are), followed by
    /// the usual index of 64-bit metadata-block pointers.
    ///
    /// An image built without xattrs sets `xattr_id_table_start` to `u64::MAX`.
    fn load_xattr_table(&mut self) -> Result<(), FilesystemError> {
        let table = self.sb.xattr_id_table_start;
        if table == u64::MAX || table == 0 || table >= self.sb.bytes_used {
            return Ok(());
        }

        let mut header = [0u8; 16];
        self.reader
            .seek(SeekFrom::Start(self.offset + table))
            .map_err(FilesystemError::Io)?;
        self.reader
            .read_exact(&mut header)
            .map_err(FilesystemError::Io)?;
        self.xattr_table_start = u64le(&header, 0);
        let count = u32le(&header, 8) as usize;
        if count == 0 {
            return Ok(());
        }

        // 8192 / 16 = 512 ID entries per metadata block. The index sits
        // immediately after the header.
        let index_count = count.div_ceil(512);
        let mut index = vec![0u8; index_count * 8];
        self.reader
            .read_exact(&mut index)
            .map_err(FilesystemError::Io)?;

        let mut entries = Vec::with_capacity(count);
        for i in 0..index_count {
            let block_start = u64le(&index, i * 8);
            let (block, _) = self.read_metadata_block(block_start)?;
            let mut off = 0;
            while off + 16 <= block.len() && entries.len() < count {
                entries.push(XattrIdEntry {
                    xattr_ref: u64le(&block, off),
                    count: u32le(&block, off + 8),
                    size: u32le(&block, off + 12),
                });
                off += 16;
            }
        }
        self.xattr_ids = entries;
        Ok(())
    }

    /// Read the extended attributes an inode's `xattr_idx` points at. Returns
    /// an empty list for inodes with none, which is the overwhelming majority.
    ///
    /// Exercised today by `reads_xattrs_from_a_real_image`; its production
    /// callers are the rebuild (phase 1) and the browse view (phase 2).
    #[allow(dead_code)]
    fn read_xattrs(&mut self, idx: u32) -> Result<Vec<Xattr>, FilesystemError> {
        if idx == SQUASHFS_INVALID_XATTR {
            return Ok(Vec::new());
        }
        let Some(entry) = self.xattr_ids.get(idx as usize).copied() else {
            return Ok(Vec::new());
        };

        let block = self.xattr_table_start + (entry.xattr_ref >> 16);
        let offset = (entry.xattr_ref & 0xFFFF) as u16;
        let raw = self.read_metadata_span(block, offset, entry.size as usize)?;

        let mut out = Vec::with_capacity(entry.count as usize);
        let mut pos = 0usize;
        for _ in 0..entry.count {
            if pos + 4 > raw.len() {
                break;
            }
            let kind = u16le(&raw, pos);
            let name_size = u16le(&raw, pos + 2) as usize;
            pos += 4;
            if pos + name_size > raw.len() {
                break;
            }
            let suffix = String::from_utf8_lossy(&raw[pos..pos + name_size]).into_owned();
            pos += name_size;

            if pos + 4 > raw.len() {
                break;
            }
            let vsize = u32le(&raw, pos) as usize;
            pos += 4;
            if pos + vsize > raw.len() {
                break;
            }
            let value = if kind & XATTR_OUT_OF_LINE != 0 {
                // Deduplicated: the payload is an 8-byte reference to the value
                // written for an earlier inode.
                self.read_xattr_value_at(u64le(&raw, pos))?
            } else {
                raw[pos..pos + vsize].to_vec()
            };
            pos += vsize;

            let prefix = match kind & XATTR_PREFIX_MASK {
                0 => "user.",
                1 => "trusted.",
                2 => "security.",
                // An unknown prefix id means a newer mksquashfs than we model;
                // keep the bare name rather than inventing one.
                _ => "",
            };
            out.push(Xattr {
                name: format!("{prefix}{suffix}"),
                value,
            });
        }
        Ok(out)
    }

    /// Follow an out-of-line xattr value reference: a 32-bit length followed by
    /// that many bytes, at a metadata reference into the xattr table.
    #[allow(dead_code)]
    fn read_xattr_value_at(&mut self, reference: u64) -> Result<Vec<u8>, FilesystemError> {
        let block = self.xattr_table_start + (reference >> 16);
        let offset = (reference & 0xFFFF) as u16;
        let header = self.read_metadata_span(block, offset, 4)?;
        let vsize = u32le(&header, 0) as usize;
        if vsize > METADATA_BLOCK_SIZE * 8 {
            return Err(FilesystemError::InvalidData(
                "squashfs: implausible out-of-line xattr value length".into(),
            ));
        }
        let full = self.read_metadata_span(block, offset, 4 + vsize)?;
        Ok(full[4..].to_vec())
    }

    /// Copy the POSIX metadata off a decoded inode onto the browse-view entry.
    /// Everything here was previously parsed and thrown away, so `ls` showed no
    /// mode, owner or mtime on a SquashFS volume.
    fn decorate(&self, fe: &mut FileEntry, inode: &Inode) {
        fe.mode = Some(inode.unix_mode());
        fe.uid = Some(self.resolve_id(inode.uid_idx));
        fe.gid = Some(self.resolve_id(inode.gid_idx));
        if inode.mtime != 0 {
            fe.modified = Some(super::unix_common::inode::format_unix_timestamp(
                inode.mtime as i64,
            ));
        }
        if let Some(label) = inode.special_label() {
            fe.special_type = Some(match inode.kind {
                INODE_BLKDEV | INODE_EXT_BLKDEV | INODE_CHRDEV | INODE_EXT_CHRDEV => {
                    format!("{label} ({}, {})", inode.dev_major, inode.dev_minor)
                }
                _ => label.to_string(),
            });
        }
    }

    /// Decode the inode at `reference` (block start in the high bits, byte
    /// offset within the decompressed block in the low 16).
    fn read_inode(&mut self, reference: u64) -> Result<Inode, FilesystemError> {
        let block = self.sb.inode_table_start + (reference >> 16);
        let offset = (reference & 0xFFFF) as u16;

        // Common header: type, permissions, uid, gid, mtime, inode number.
        let header = self.read_metadata_span(block, offset, 16)?;
        let kind = u16le(&header, 0);
        let mut inode = Inode::empty(kind);
        inode.mode = u16le(&header, 2);
        inode.uid_idx = u16le(&header, 4);
        inode.gid_idx = u16le(&header, 6);
        inode.mtime = u32le(&header, 8);
        inode.inode_number = u32le(&header, 12);

        match kind {
            // Basic directory, 32 bytes after the 16-byte common header:
            //   16 dir_block_start (u32), 20 hard_link_count (u32),
            //   24 file_size (u16!), 26 block_offset (u16), 28 parent (u32).
            // Note file_size is 16-bit here — it is 32-bit only in the extended
            // form — and `dir_size` counts a phantom 3 bytes (see
            // read_dir_entries).
            INODE_DIR => {
                let b = self.read_metadata_span(block, offset, 32)?;
                inode.dir_block_start = u32le(&b, 16);
                inode.nlink = u32le(&b, 20);
                inode.dir_size = u16le(&b, 24) as u32;
                inode.dir_offset = u16le(&b, 26);
            }
            // Extended directory, 40 bytes:
            //   16 hard_link_count (u32), 20 file_size (u32),
            //   24 dir_block_start (u32), 28 parent (u32),
            //   32 index_count (u16), 34 block_offset (u16), 36 xattr (u32).
            INODE_EXT_DIR => {
                let b = self.read_metadata_span(block, offset, 40)?;
                inode.nlink = u32le(&b, 16);
                inode.dir_size = u32le(&b, 20);
                inode.dir_block_start = u32le(&b, 24);
                inode.dir_offset = u16le(&b, 34);
                inode.xattr_idx = u32le(&b, 36);
            }
            INODE_FILE => {
                let b = self.read_metadata_span(block, offset, 32)?;
                inode.file_start = u32le(&b, 16) as u64;
                inode.fragment_index = u32le(&b, 20);
                inode.fragment_offset = u32le(&b, 24);
                inode.file_size = u32le(&b, 28) as u64;
                inode.block_sizes = self.read_block_sizes(
                    block,
                    offset,
                    32,
                    inode.file_size,
                    inode.fragment_index,
                )?;
            }
            INODE_EXT_FILE => {
                let b = self.read_metadata_span(block, offset, 56)?;
                inode.file_start = u64le(&b, 16);
                inode.file_size = u64le(&b, 24);
                // 32: sparse-file byte count — informational, not needed to read.
                inode.nlink = u32le(&b, 40);
                inode.fragment_index = u32le(&b, 44);
                inode.fragment_offset = u32le(&b, 48);
                inode.xattr_idx = u32le(&b, 52);
                inode.block_sizes = self.read_block_sizes(
                    block,
                    offset,
                    56,
                    inode.file_size,
                    inode.fragment_index,
                )?;
            }
            INODE_SYMLINK | INODE_EXT_SYMLINK => {
                let b = self.read_metadata_span(block, offset, 24)?;
                inode.nlink = u32le(&b, 16);
                let target_len = u32le(&b, 20) as usize;
                if target_len > 4096 {
                    return Err(FilesystemError::InvalidData(
                        "squashfs: implausible symlink target length".into(),
                    ));
                }
                // The extended form parks its xattr index *after* the variable-
                // length target, so it can only be read once the length is known.
                let tail = if kind == INODE_EXT_SYMLINK { 4 } else { 0 };
                let full = self.read_metadata_span(block, offset, 24 + target_len + tail)?;
                inode.symlink_target =
                    String::from_utf8_lossy(&full[24..24 + target_len]).into_owned();
                if tail == 4 {
                    inode.xattr_idx = u32le(&full, 24 + target_len);
                }
            }
            // Device nodes pack major/minor into one 32-bit word; fifos and
            // sockets carry no payload beyond the link count.
            INODE_BLKDEV | INODE_CHRDEV | INODE_EXT_BLKDEV | INODE_EXT_CHRDEV => {
                let extended = matches!(kind, INODE_EXT_BLKDEV | INODE_EXT_CHRDEV);
                let want = if extended { 28 } else { 24 };
                let b = self.read_metadata_span(block, offset, want)?;
                inode.nlink = u32le(&b, 16);
                let (major, minor) = super::unix_common::inode::device_major_minor(u32le(&b, 20));
                inode.dev_major = major;
                inode.dev_minor = minor;
                if extended {
                    inode.xattr_idx = u32le(&b, 24);
                }
            }
            INODE_FIFO | INODE_SOCKET | INODE_EXT_FIFO | INODE_EXT_SOCKET => {
                let extended = matches!(kind, INODE_EXT_FIFO | INODE_EXT_SOCKET);
                let want = if extended { 24 } else { 20 };
                let b = self.read_metadata_span(block, offset, want)?;
                inode.nlink = u32le(&b, 16);
                if extended {
                    inode.xattr_idx = u32le(&b, 20);
                }
            }
            other => {
                return Err(FilesystemError::InvalidData(format!(
                    "squashfs: unknown inode type {other}"
                )));
            }
        }
        Ok(inode)
    }

    /// A regular file's inode is followed by one 32-bit size per full block.
    /// The tail is excluded when it lives in a fragment.
    fn read_block_sizes(
        &mut self,
        block: u64,
        offset: u16,
        header_len: usize,
        file_size: u64,
        fragment_index: u32,
    ) -> Result<Vec<u32>, FilesystemError> {
        let bs = self.sb.block_size as u64;
        let has_fragment = fragment_index != 0xFFFF_FFFF;
        let count = if has_fragment {
            (file_size / bs) as usize
        } else {
            file_size.div_ceil(bs) as usize
        };
        if count == 0 {
            return Ok(Vec::new());
        }
        // A plausibility bound: a file cannot have more blocks than the image
        // could possibly hold.
        if count > 1 << 24 {
            return Err(FilesystemError::InvalidData(
                "squashfs: implausible block count".into(),
            ));
        }

        let raw = self.read_metadata_span(block, offset, header_len + count * 4)?;
        Ok((0..count)
            .map(|i| u32le(&raw, header_len + i * 4))
            .collect())
    }

    /// Read one compressed (or stored) data block from the image.
    fn read_data_block(&mut self, start: u64, size_field: u32) -> Result<Vec<u8>, FilesystemError> {
        // Bit 24 set means the block was stored uncompressed.
        let stored_uncompressed = size_field & 0x0100_0000 != 0;
        let len = (size_field & 0x00FF_FFFF) as usize;
        if len == 0 {
            // A zero length is a sparse block: a whole block of zeroes.
            return Ok(vec![0u8; self.sb.block_size as usize]);
        }

        self.reader
            .seek(SeekFrom::Start(self.offset + start))
            .map_err(FilesystemError::Io)?;
        let mut buf = vec![0u8; len];
        self.reader
            .read_exact(&mut buf)
            .map_err(FilesystemError::Io)?;

        if stored_uncompressed || self.sb.data_uncompressed() {
            Ok(buf)
        } else {
            decompress(self.sb.compressor, &buf, self.sb.block_size as usize)
        }
    }

    /// Walk a directory inode's entries.
    fn read_dir_entries(
        &mut self,
        inode: &Inode,
    ) -> Result<Vec<(String, u64, u16)>, FilesystemError> {
        // A directory's recorded size counts three bytes that are not there —
        // an artefact of the on-disk format. An empty directory records 3.
        if inode.dir_size <= 3 {
            return Ok(Vec::new());
        }
        let want = inode.dir_size as usize - 3;
        let raw = self.read_metadata_span(
            self.sb.directory_table_start + inode.dir_block_start as u64,
            inode.dir_offset,
            want,
        )?;

        let mut out = Vec::new();
        let mut pos = 0usize;
        // The table is a sequence of headers, each introducing up to 256 entries
        // that share an inode-table block.
        while pos + 12 <= raw.len() {
            let count = u32le(&raw, pos) as usize + 1;
            let start_block = u32le(&raw, pos + 4);
            pos += 12;

            for _ in 0..count {
                if pos + 8 > raw.len() {
                    break;
                }
                let entry_offset = u16le(&raw, pos);
                let kind = u16le(&raw, pos + 4);
                let name_len = u16le(&raw, pos + 6) as usize + 1;
                pos += 8;
                if pos + name_len > raw.len() {
                    break;
                }
                let name = String::from_utf8_lossy(&raw[pos..pos + name_len]).into_owned();
                pos += name_len;

                let reference = ((start_block as u64) << 16) | entry_offset as u64;
                out.push((name, reference, kind));
            }
        }
        Ok(out)
    }

    /// Read a regular file's bytes given its already-decoded inode. Shared by
    /// the [`Filesystem::read_file`] path and the rebuild bridge; the whole-
    /// block-then-fragment-tail logic lives in one place.
    fn read_regular_file(
        &mut self,
        inode: &Inode,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let want = (inode.file_size as usize).min(max_bytes);
        let mut out = Vec::with_capacity(want);

        // Whole blocks first, laid end to end from the file's start offset.
        let mut pos = inode.file_start;
        for &size_field in &inode.block_sizes {
            if out.len() >= want {
                break;
            }
            let block = self.read_data_block(pos, size_field)?;
            let take = (want - out.len()).min(block.len());
            out.extend_from_slice(&block[..take]);
            pos += (size_field & 0x00FF_FFFF) as u64;
        }

        // Then the tail, if it was packed into a shared fragment.
        if out.len() < want && inode.fragment_index != 0xFFFF_FFFF {
            let frag = self
                .fragments
                .get(inode.fragment_index as usize)
                .copied()
                .ok_or_else(|| {
                    FilesystemError::InvalidData(format!(
                        "squashfs: fragment {} out of range",
                        inode.fragment_index
                    ))
                })?;
            let block = self.read_data_block(frag.start, frag.size)?;
            let from = inode.fragment_offset as usize;
            let take = (want - out.len()).min(block.len().saturating_sub(from));
            if take > 0 {
                out.extend_from_slice(&block[from..from + take]);
            }
        }

        Ok(out)
    }

    /// Read the entire image into an editable [`BuildNode`] tree — the bridge
    /// from the read side to the rebuild side (`docs/squashfs_edit.md` phase 2).
    ///
    /// Every field a faithful rebuild needs is carried across: mode, uid, gid
    /// (resolved through the ID table), mtime, symlink targets, device
    /// major/minor, and extended attributes. This is the whole reason phase 0
    /// retained that metadata rather than keeping only what browsing showed.
    ///
    /// **Memory:** file contents are read eagerly into `FileContent::Bytes`, so
    /// peak use is the decompressed image. Fine for AppImages and typical
    /// appliance images; lazy per-file streaming and verbatim block reuse are
    /// the noted phase-2b optimization, and land behind the same `FileContent`
    /// seam without changing this signature.
    pub fn read_build_tree(&mut self) -> Result<super::squashfs_write::BuildNode, FilesystemError> {
        let root_ref = self.sb.root_inode_ref;
        let mut root = self.build_node(String::new(), root_ref)?;
        // The tree's root name is conventionally empty; the writer ignores it.
        root.name = String::new();
        Ok(root)
    }

    /// Read only the subtree at `path` (slash-separated, relative to the image
    /// root) into a fresh-root [`BuildNode`]. Navigates the directory table so
    /// it decompresses only what that subtree needs, rather than the whole image
    /// — the difference between seconds and minutes on a distro rootfs.
    ///
    /// `path` must name a directory. Primarily for tests and for tooling that
    /// rebuilds a portion; a full edit rebuild uses [`Self::read_build_tree`].
    pub fn read_build_subtree(
        &mut self,
        path: &str,
    ) -> Result<super::squashfs_write::BuildNode, FilesystemError> {
        let mut reference = self.sb.root_inode_ref;
        for component in path.split('/').filter(|c| !c.is_empty()) {
            let inode = self.read_inode(reference)?;
            if !matches!(inode.kind, INODE_DIR | INODE_EXT_DIR) {
                return Err(FilesystemError::NotADirectory(path.to_string()));
            }
            let entries = self.read_dir_entries(&inode)?;
            reference = entries
                .into_iter()
                .find(|(name, _, _)| name == component)
                .map(|(_, r, _)| r)
                .ok_or_else(|| FilesystemError::NotFound(path.to_string()))?;
        }
        let mut node = self.build_node(String::new(), reference)?;
        node.name = String::new();
        Ok(node)
    }

    /// Recursively turn the inode at `reference` into a [`BuildNode`].
    fn build_node(
        &mut self,
        name: String,
        reference: u64,
    ) -> Result<super::squashfs_write::BuildNode, FilesystemError> {
        use super::squashfs_write::{BuildKind, FileContent};

        let inode = self.read_inode(reference)?;
        let xattrs = self.read_xattrs(inode.xattr_idx)?;
        let mode = inode.mode & 0o7777;
        let uid = self.resolve_id(inode.uid_idx);
        let gid = self.resolve_id(inode.gid_idx);
        let mtime = inode.mtime;

        let kind = match inode.kind {
            INODE_DIR | INODE_EXT_DIR => {
                let entries = self.read_dir_entries(&inode)?;
                let mut children = Vec::with_capacity(entries.len());
                for (child_name, child_ref, _kind) in entries {
                    children.push(self.build_node(child_name, child_ref)?);
                }
                BuildKind::Dir(children)
            }
            INODE_FILE | INODE_EXT_FILE => {
                let data = self.read_regular_file(&inode, usize::MAX)?;
                BuildKind::File(FileContent::Bytes(data))
            }
            INODE_SYMLINK | INODE_EXT_SYMLINK => BuildKind::Symlink(inode.symlink_target.clone()),
            INODE_BLKDEV | INODE_EXT_BLKDEV => BuildKind::BlockDev {
                major: inode.dev_major,
                minor: inode.dev_minor,
            },
            INODE_CHRDEV | INODE_EXT_CHRDEV => BuildKind::CharDev {
                major: inode.dev_major,
                minor: inode.dev_minor,
            },
            INODE_FIFO | INODE_EXT_FIFO => BuildKind::Fifo,
            INODE_SOCKET | INODE_EXT_SOCKET => BuildKind::Socket,
            other => {
                return Err(FilesystemError::InvalidData(format!(
                    "squashfs: cannot rebuild unknown inode type {other}"
                )))
            }
        };

        Ok(super::squashfs_write::BuildNode {
            name,
            mode,
            uid,
            gid,
            mtime,
            xattrs,
            kind,
        })
    }
}

impl<R: Read + Seek + Send> Filesystem for SquashfsFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        let mut fe =
            FileEntry::new_directory("/".to_string(), "/".to_string(), self.sb.root_inode_ref);
        // Best-effort: a root inode we cannot decode is already fatal for any
        // real use, but `root()` itself stays infallible for the caller.
        if let Ok(inode) = self.read_inode(self.sb.root_inode_ref) {
            self.decorate(&mut fe, &inode);
        }
        Ok(fe)
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        let inode = self.read_inode(entry.location)?;
        if inode.entry_type() != EntryType::Directory {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }
        let raw_entries = self.read_dir_entries(&inode)?;

        let base = if entry.path == "/" {
            String::new()
        } else {
            entry.path.trim_end_matches('/').to_string()
        };

        let mut out = Vec::with_capacity(raw_entries.len());
        for (name, reference, _kind) in raw_entries {
            let path = format!("{base}/{name}");
            // The directory entry carries a type, but the inode is the
            // authority — and we need its size and target anyway.
            let child = match self.read_inode(reference) {
                Ok(i) => i,
                // One unreadable child should not abort the whole listing.
                Err(_) => continue,
            };
            let mut fe = match child.entry_type() {
                EntryType::Directory => FileEntry::new_directory(name, path, reference),
                EntryType::Symlink => FileEntry::new_symlink(
                    name,
                    path,
                    child.symlink_target.len() as u64,
                    reference,
                    child.symlink_target.clone(),
                ),
                EntryType::File => FileEntry::new_file(name, path, child.file_size, reference),
                EntryType::Special => {
                    let mut e = FileEntry::new_file(name, path, 0, reference);
                    e.entry_type = EntryType::Special;
                    e
                }
            };
            self.decorate(&mut fe, &child);
            out.push(fe);
        }
        Ok(out)
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let inode = self.read_inode(entry.location)?;
        match inode.entry_type() {
            EntryType::File => {}
            EntryType::Symlink => return Ok(inode.symlink_target.clone().into_bytes()),
            _ => {
                return Err(FilesystemError::InvalidData(format!(
                    "squashfs: {} is not a regular file",
                    entry.path
                )))
            }
        }
        self.read_regular_file(&inode, max_bytes)
    }

    fn volume_label(&self) -> Option<&str> {
        // SquashFS has no volume label field.
        None
    }

    fn fs_type(&self) -> &str {
        "SquashFS"
    }

    fn total_size(&self) -> u64 {
        self.sb.bytes_used
    }

    fn used_size(&self) -> u64 {
        // Everything in a read-only image is live by construction.
        self.sb.bytes_used
    }

    fn allocation_unit(&self) -> Option<u64> {
        Some(self.sb.block_size as u64)
    }
}

// ── Compression ───────────────────────────────────────────────────────────────

/// Decompress `input`, which is known to expand to at most `max_out` bytes.
fn decompress(
    compressor: Compressor,
    input: &[u8],
    max_out: usize,
) -> Result<Vec<u8>, FilesystemError> {
    // Name the compressor in the message: a decompression failure usually means
    // the image is truncated or we mis-framed a block, and knowing which codec
    // was in play is the first thing you want.
    let err = |what: &str, e: String| {
        FilesystemError::InvalidData(format!(
            "squashfs: {what} decompression failed ({} image): {e}",
            compressor.name()
        ))
    };

    match compressor {
        Compressor::Gzip => {
            // SquashFS uses raw zlib streams, not the gzip wrapper.
            use std::io::Read as _;
            let mut out = Vec::with_capacity(max_out);
            flate2::read::ZlibDecoder::new(input)
                .take(max_out as u64)
                .read_to_end(&mut out)
                .map_err(|e| err("zlib", e.to_string()))?;
            Ok(out)
        }
        Compressor::Xz => {
            let mut out = Vec::with_capacity(max_out);
            lzma_rs::xz_decompress(&mut std::io::Cursor::new(input), &mut out)
                .map_err(|e| err("xz", format!("{e:?}")))?;
            Ok(out)
        }
        Compressor::Lzma => {
            let mut out = Vec::with_capacity(max_out);
            lzma_rs::lzma_decompress(&mut std::io::Cursor::new(input), &mut out)
                .map_err(|e| err("lzma", format!("{e:?}")))?;
            Ok(out)
        }
        Compressor::Lz4 => {
            // SquashFS stores a bare LZ4 block, not a frame, and the caller
            // always knows the maximum expanded size.
            lz4_flex::block::decompress(input, max_out).map_err(|e| err("lz4", e.to_string()))
        }
        Compressor::Zstd => decompress_zstd(input, max_out),
        Compressor::Lzo => Err(FilesystemError::Unsupported(
            "squashfs: LZO-compressed images are not supported".into(),
        )),
    }
}

/// Route through the [`zstd_compat`](crate::rbformats::zstd_compat) shim rather
/// than naming a zstd crate directly, so the backend follows the cargo feature
/// (`native-zstd` = C libzstd, `pure-zstd` = libzstd-bitexact-rs for the slim /
/// cross builds). The shim `compile_error!`s when neither is enabled, so there
/// is no "no backend" arm to write here.
fn decompress_zstd(input: &[u8], max_out: usize) -> Result<Vec<u8>, FilesystemError> {
    let mut out = crate::rbformats::zstd_compat::decode_all(input).map_err(|e| {
        FilesystemError::InvalidData(format!("squashfs: zstd decompression failed: {e}"))
    })?;
    out.truncate(max_out);
    Ok(out)
}

// ── Little-endian readers ─────────────────────────────────────────────────────

fn u16le(b: &[u8], off: usize) -> u16 {
    u16::from_le_bytes([b[off], b[off + 1]])
}

fn u32le(b: &[u8], off: usize) -> u32 {
    u32::from_le_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]])
}

fn u64le(b: &[u8], off: usize) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[off..off + 8]);
    u64::from_le_bytes(a)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Open the fixture image, or `None` when it isn't present so CI stays
    /// green. Same `RB_SQUASHFS_ISO` override as the other real-image tests.
    fn open_real_image() -> Option<SquashfsFilesystem<std::fs::File>> {
        let path = std::env::var("RB_SQUASHFS_ISO").unwrap_or_else(|_| {
            "/Users/dani/Downloads/ubuntu-12.04-desktop-powerpc.iso".to_string()
        });
        let mut file = match std::fs::File::open(&path) {
            Ok(f) => f,
            Err(_) => {
                eprintln!("{path} not present — skipping real-image test");
                return None;
            }
        };
        let offset = find_squashfs_offset(&mut file)?;
        Some(SquashfsFilesystem::open(file, offset).expect("open real squashfs"))
    }

    /// Minimal 96-byte superblock with the fields this module validates.
    fn superblock(compressor: u16, block_log: u16, version_major: u16) -> Vec<u8> {
        let mut b = vec![0u8; SUPERBLOCK_SIZE];
        b[0..4].copy_from_slice(&SQUASHFS_MAGIC.to_le_bytes());
        b[12..16].copy_from_slice(&(1u32 << block_log).to_le_bytes());
        b[20..22].copy_from_slice(&compressor.to_le_bytes());
        b[22..24].copy_from_slice(&block_log.to_le_bytes());
        b[28..30].copy_from_slice(&version_major.to_le_bytes());
        b
    }

    #[test]
    fn parses_a_well_formed_superblock() {
        let sb = Superblock::parse(&superblock(1, 17, 4)).expect("valid superblock");
        assert_eq!(sb.compressor, Compressor::Gzip);
        assert_eq!(sb.block_size, 131_072);
    }

    #[test]
    fn rejects_foreign_data_that_is_not_squashfs() {
        let mut b = superblock(1, 17, 4);
        b[0..4].copy_from_slice(&0xDEAD_BEEFu32.to_le_bytes());
        assert!(Superblock::parse(&b).is_err());
    }

    #[test]
    fn rejects_pre_v4_images_rather_than_misreading_them() {
        // v1-v3 have a different superblock; reading them as v4 would produce
        // plausible-looking garbage.
        let err = Superblock::parse(&superblock(1, 17, 3)).unwrap_err();
        assert!(format!("{err}").contains("not supported"), "{err}");
    }

    #[test]
    fn rejects_a_block_size_that_disagrees_with_block_log() {
        // Magic can match by chance; this catches that.
        let mut b = superblock(1, 17, 4);
        b[12..16].copy_from_slice(&99_999u32.to_le_bytes());
        assert!(Superblock::parse(&b).is_err());
    }

    #[test]
    fn refuses_lzo_by_name_instead_of_misparsing() {
        let err = Superblock::parse(&superblock(3, 17, 4)).unwrap_err();
        assert!(format!("{err}").contains("LZO"), "{err}");
    }

    #[test]
    fn detect_says_no_on_random_data() {
        let mut cur = Cursor::new(vec![0u8; SUPERBLOCK_SIZE]);
        assert!(!SquashfsFilesystem::detect(&mut cur, 0));
    }

    /// Fixture-gated end-to-end read of a **real** SquashFS produced by
    /// `mksquashfs` — the Ubuntu 12.04 PowerPC live CD's `casper/filesystem
    /// .squashfs`, read in place at its offset inside the ISO.
    ///
    /// Skips when the ISO is absent so CI stays green. Set `RB_SQUASHFS_ISO` to
    /// point at another image; the offset is found by scanning for the magic.
    #[test]
    fn reads_a_real_squashfs_image() {
        let path = std::env::var("RB_SQUASHFS_ISO").unwrap_or_else(|_| {
            "/Users/dani/Downloads/ubuntu-12.04-desktop-powerpc.iso".to_string()
        });
        let Ok(mut file) = std::fs::File::open(&path) else {
            eprintln!("{path} not present — skipping real-image test");
            return;
        };

        let Some(offset) = find_squashfs_offset(&mut file) else {
            eprintln!("no squashfs found in {path} — skipping");
            return;
        };

        let mut fs = SquashfsFilesystem::open(file, offset).expect("open real squashfs");
        assert_eq!(fs.fs_type(), "SquashFS");

        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list root");
        let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
        eprintln!("root: {} entries: {names:?}", entries.len());

        // A Linux root filesystem has these, and getting them proves the
        // superblock, inode table, directory table and gzip metadata path all
        // agree with what mksquashfs actually wrote.
        for expected in ["bin", "etc", "usr", "var"] {
            assert!(names.contains(&expected), "root is missing {expected}");
        }

        // Descend and read a file whose content is known independently — this
        // exercises the data-block/fragment path, not just metadata.
        let etc = entries
            .iter()
            .find(|e| e.name == "etc")
            .expect("etc")
            .clone();
        let etc_entries = fs.list_directory(&etc).expect("list /etc");
        let lsb = etc_entries
            .iter()
            .find(|e| e.name == "lsb-release")
            .expect("/etc/lsb-release");
        let body = String::from_utf8_lossy(&fs.read_file(lsb, 4096).expect("read lsb-release"))
            .into_owned();
        eprintln!("/etc/lsb-release:\n{body}");
        // Deliberately not asserting a specific release: the same test runs
        // against whatever image RB_SQUASHFS_ISO points at, and the point is
        // that real mksquashfs output decodes to real text.
        assert!(
            body.contains("DISTRIB_ID="),
            "lsb-release did not decode to the expected shape: {body}"
        );
    }

    /// Phase 0 (`docs/squashfs_edit.md` §3): the POSIX metadata a rebuild has to
    /// reproduce. Before this, `read_inode` parsed the 16-byte common header and
    /// kept only the type — so mode, uid, gid and mtime were all discarded and
    /// browsing showed none of them.
    ///
    /// Asserts against a real Linux root filesystem, where the expected shapes
    /// are not in doubt: `/` is 0755, `/etc/shadow` is not world-readable, and
    /// `/bin` holds root-owned executables.
    #[test]
    fn retains_posix_metadata_for_rebuild() {
        let Some(mut fs) = open_real_image() else {
            return;
        };

        let root = fs.root().expect("root");
        assert_eq!(
            root.mode.map(|m| m & 0o7777),
            Some(0o755),
            "root directory should be 0755"
        );
        assert_eq!(root.uid, Some(0), "root directory should be owned by uid 0");

        let entries = fs.list_directory(&root).expect("list root");
        let bin = entries.iter().find(|e| e.name == "bin").expect("/bin");
        assert_eq!(
            bin.mode.map(|m| m & 0o170_000),
            Some(0o040_000),
            "/bin is a dir"
        );

        // Every entry must now carry mode/uid/gid, and a plausible mtime.
        let bin_entries = fs.list_directory(bin).expect("list /bin");
        assert!(!bin_entries.is_empty(), "/bin should not be empty");
        for e in &bin_entries {
            assert!(e.mode.is_some(), "{} has no mode", e.path);
            assert!(e.uid.is_some(), "{} has no uid", e.path);
            assert!(e.gid.is_some(), "{} has no gid", e.path);
        }

        // At least one executable, and it should be root-owned.
        let execs: Vec<_> = bin_entries
            .iter()
            .filter(|e| e.entry_type == EntryType::File && e.mode.unwrap_or(0) & 0o111 != 0)
            .collect();
        assert!(!execs.is_empty(), "/bin should hold executables");
        assert!(
            execs.iter().all(|e| e.uid == Some(0)),
            "/bin executables should be root-owned"
        );

        // mtime must decode to a real date, not the epoch.
        assert!(
            bin_entries.iter().any(|e| e.modified.is_some()),
            "no entry carried an mtime"
        );
    }

    /// Device nodes carry a packed major/minor that the old reader dropped on
    /// the floor (the `{}` arm). A Linux `/dev` is the natural place to check.
    #[test]
    fn decodes_device_nodes() {
        let Some(mut fs) = open_real_image() else {
            return;
        };
        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list root");
        let Some(dev) = entries.iter().find(|e| e.name == "dev") else {
            eprintln!("image has no /dev — skipping");
            return;
        };
        let dev_entries = fs.list_directory(dev).expect("list /dev");
        let specials: Vec<_> = dev_entries
            .iter()
            .filter(|e| e.entry_type == EntryType::Special)
            .collect();
        if specials.is_empty() {
            eprintln!("/dev holds no device nodes — skipping");
            return;
        }
        for s in &specials {
            let label = s.special_type.as_deref().unwrap_or("");
            assert!(
                !label.is_empty(),
                "{} is Special but carries no special_type",
                s.path
            );
        }
        eprintln!(
            "/dev: {} special entries, e.g. {:?}",
            specials.len(),
            specials
                .iter()
                .take(4)
                .map(|e| (&e.name, &e.special_type))
                .collect::<Vec<_>>()
        );
    }

    /// Extended attributes must survive a rebuild (decision D4), so the reader
    /// has to decode them — including mksquashfs's out-of-line dedup form.
    /// Reports rather than asserts a specific attribute: whether a given image
    /// uses xattrs at all depends on how it was built.
    #[test]
    fn reads_xattrs_from_a_real_image() {
        let Some(mut fs) = open_real_image() else {
            return;
        };
        eprintln!(
            "xattr id table: {} entries, kv table at {}",
            fs.xattr_ids.len(),
            fs.xattr_table_start
        );

        // Walk a bounded slice of the tree collecting any attributes found.
        let root = fs.root().expect("root");
        let mut queue = vec![root];
        let mut found: Vec<(String, Vec<Xattr>)> = Vec::new();
        let mut visited = 0usize;
        while let Some(dir) = queue.pop() {
            if visited > 400 {
                break;
            }
            visited += 1;
            let Ok(children) = fs.list_directory(&dir) else {
                continue;
            };
            for child in children {
                let Ok(inode) = fs.read_inode(child.location) else {
                    continue;
                };
                if inode.xattr_idx != SQUASHFS_INVALID_XATTR {
                    let attrs = fs.read_xattrs(inode.xattr_idx).expect("read xattrs");
                    if !attrs.is_empty() {
                        found.push((child.path.clone(), attrs));
                    }
                }
                if child.entry_type == EntryType::Directory && queue.len() < 400 {
                    queue.push(child);
                }
            }
        }

        if fs.xattr_ids.is_empty() {
            eprintln!("image carries no xattr table — nothing to decode");
            return;
        }
        eprintln!("found xattrs on {} entries (walk is bounded)", found.len());

        // The walk above is bounded, so it may not reach whichever inode
        // references the table. Decode every ID-table entry directly — that
        // exercises the key/value parser deterministically, and every entry in
        // the table is by construction referenced by some inode.
        for idx in 0..fs.xattr_ids.len() as u32 {
            let attrs = fs
                .read_xattrs(idx)
                .unwrap_or_else(|e| panic!("xattr id {idx} failed to decode: {e}"));
            let expected = fs.xattr_ids[idx as usize].count as usize;
            assert_eq!(
                attrs.len(),
                expected,
                "xattr id {idx}: decoded {} pairs, table says {expected}",
                attrs.len()
            );
            for a in &attrs {
                eprintln!("  xattr[{idx}]: {} = {} bytes", a.name, a.value.len());
                assert!(!a.name.is_empty(), "empty xattr name at id {idx}");
                assert!(
                    a.name.contains('.'),
                    "xattr name {:?} lost its prefix",
                    a.name
                );
            }
        }

        for (path, attrs) in found.iter().take(8) {
            for a in attrs {
                eprintln!("  {path}: {} = {} bytes", a.name, a.value.len());
            }
        }
        // Whatever we decoded must be well-formed: a prefixed name and a value.
        for (path, attrs) in &found {
            for a in attrs {
                assert!(!a.name.is_empty(), "{path}: empty xattr name");
                assert!(
                    a.name.contains('.'),
                    "{path}: xattr name {:?} lost its prefix",
                    a.name
                );
            }
        }
    }

    /// True when `unsquashfs` is on PATH (Homebrew formula `squashfs`), so a
    /// machine without squashfs-tools skips rather than fails.
    ///
    /// Unlike `qemu-img --version`, `unsquashfs -version` **exits 1** while
    /// still printing its banner, so the exit status can't be the probe — match
    /// the banner text on stdout instead.
    fn unsquashfs_available() -> bool {
        std::process::Command::new("unsquashfs")
            .arg("-version")
            .output()
            .map(|o| {
                let text = String::from_utf8_lossy(&o.stdout);
                text.contains("unsquashfs version")
            })
            .unwrap_or(false)
    }

    /// One entry of an `unsquashfs -lls` listing, normalized for comparison.
    #[derive(Debug, PartialEq, Eq)]
    struct OracleEntry {
        mode: u32,
        size: u64,
        /// `Some((major, minor))` for device nodes, whose size column is a
        /// major/minor pair rather than a byte count.
        dev: Option<(u32, u32)>,
        symlink: Option<String>,
    }

    /// Parse `unsquashfs -lls` output into path -> entry.
    ///
    /// Lines look like:
    /// ```text
    /// -rwxr-xr-x root/root   900772 2012-04-03 12:32 squashfs-root/bin/bash
    /// lrwxrwxrwx root/root        6 2011-12-15 01:16 squashfs-root/bin/bzcmp -> bzdiff
    /// crw-rw---- root/44      10,175 2012-04-23 07:37 squashfs-root/dev/agpgart
    /// ```
    fn parse_oracle(text: &str) -> HashMap<String, OracleEntry> {
        let mut out = HashMap::new();
        for line in text.lines() {
            let perms = line.split_whitespace().next().unwrap_or("");
            if perms.len() != 10 || !"-dlbcps".contains(&perms[0..1]) {
                continue;
            }
            let Some(rest) = line.split_once(' ') else {
                continue;
            };
            // owner/group, then either "SIZE" or "MAJ, MIN" (the comma form can
            // carry padding spaces), then date, time, path.
            let mut toks = rest.1.split_whitespace();
            let _owner = toks.next();
            let joined: Vec<&str> = toks.collect();
            // Walk forward to the date token (YYYY-MM-DD); everything before it
            // is the size / device columns.
            let date_at = joined
                .iter()
                .position(|t| t.len() == 10 && t.as_bytes()[4] == b'-');
            let Some(date_at) = date_at else { continue };
            let size_part = joined[..date_at].join("");
            // path is everything after date + time
            let tail = joined[date_at + 2..].join(" ");
            let (path_part, symlink) = match tail.split_once(" -> ") {
                Some((p, t)) => (p.to_string(), Some(t.to_string())),
                None => (tail, None),
            };
            let path = path_part
                .trim_start_matches("squashfs-root")
                .trim_end()
                .to_string();
            let path = if path.is_empty() { "/".into() } else { path };

            let (size, dev) = if let Some((maj, min)) = size_part.split_once(',') {
                (
                    0,
                    Some((
                        maj.trim().parse().unwrap_or(0),
                        min.trim().parse().unwrap_or(0),
                    )),
                )
            } else {
                (size_part.trim().parse().unwrap_or(0), None)
            };

            out.insert(
                path,
                OracleEntry {
                    mode: mode_from_perm_string(perms),
                    size,
                    dev,
                    symlink,
                },
            );
        }
        out
    }

    /// Turn `-rwxr-xr-x` / `crw-rw----` into the numeric mode (type + perms).
    fn mode_from_perm_string(p: &str) -> u32 {
        let b = p.as_bytes();
        let type_bits: u32 = match b[0] {
            b'd' => 0o040_000,
            b'l' => 0o120_000,
            b'b' => 0o060_000,
            b'c' => 0o020_000,
            b'p' => 0o010_000,
            b's' => 0o140_000,
            _ => 0o100_000,
        };
        let mut perms = 0u32;
        for (i, chunk) in [1usize, 4, 7].iter().enumerate() {
            let shift = 6 - i * 3;
            if b[*chunk] == b'r' {
                perms |= 4 << shift;
            }
            if b[chunk + 1] == b'w' {
                perms |= 2 << shift;
            }
            match b[chunk + 2] {
                b'x' => perms |= 1 << shift,
                // setuid / setgid / sticky replace the x column.
                b's' | b't' => {
                    perms |= 1 << shift;
                    perms |= match i {
                        0 => 0o4000,
                        1 => 0o2000,
                        _ => 0o1000,
                    };
                }
                b'S' | b'T' => {
                    perms |= match i {
                        0 => 0o4000,
                        1 => 0o2000,
                        _ => 0o1000,
                    }
                }
                _ => {}
            }
        }
        type_bits | perms
    }

    /// **The phase-0 oracle test.** Walk the whole fixture image with our
    /// reader and diff every entry against `unsquashfs -lls`.
    ///
    /// The other phase-0 tests assert *plausible* shapes (root is 0755, /bin is
    /// root-owned). This one asserts *ground truth* for mode, size, symlink
    /// target and device major/minor across every inode in a 123k-entry real
    /// image — which is what actually establishes that a rebuild driven off
    /// this metadata would be faithful.
    #[test]
    fn matches_unsquashfs_listing_entry_for_entry() {
        let Some(mut fs) = open_real_image() else {
            return;
        };
        if !unsquashfs_available() {
            eprintln!("unsquashfs not on PATH — skipping oracle comparison");
            return;
        }
        let path = std::env::var("RB_SQUASHFS_ISO").unwrap_or_else(|_| {
            "/Users/dani/Downloads/ubuntu-12.04-desktop-powerpc.iso".to_string()
        });
        let offset = {
            let mut f = std::fs::File::open(&path).unwrap();
            find_squashfs_offset(&mut f).unwrap()
        };

        let out = std::process::Command::new("unsquashfs")
            .arg("-o")
            .arg(offset.to_string())
            .arg("-lls")
            .arg(&path)
            .output()
            .expect("run unsquashfs");
        assert!(
            out.status.success(),
            "unsquashfs failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let oracle = parse_oracle(&String::from_utf8_lossy(&out.stdout));
        assert!(
            oracle.len() > 1000,
            "oracle listing looks too small ({} entries) — parser drifted?",
            oracle.len()
        );

        // Walk our reader over the same tree.
        let root = fs.root().expect("root");
        let mut queue = vec![root];
        let mut checked = 0usize;
        let mut mismatches: Vec<String> = Vec::new();
        while let Some(dir) = queue.pop() {
            let Ok(children) = fs.list_directory(&dir) else {
                continue;
            };
            for child in children {
                if let Some(exp) = oracle.get(&child.path) {
                    checked += 1;
                    let got_mode = child.mode.unwrap_or(0);
                    if got_mode != exp.mode {
                        mismatches.push(format!(
                            "{}: mode {:o} != oracle {:o}",
                            child.path, got_mode, exp.mode
                        ));
                    }
                    if exp.dev.is_none()
                        && child.entry_type == EntryType::File
                        && child.size != exp.size
                    {
                        mismatches.push(format!(
                            "{}: size {} != oracle {}",
                            child.path, child.size, exp.size
                        ));
                    }
                    if let Some(t) = &exp.symlink {
                        if child.symlink_target.as_deref() != Some(t.as_str()) {
                            mismatches.push(format!(
                                "{}: symlink {:?} != oracle {:?}",
                                child.path, child.symlink_target, t
                            ));
                        }
                    }
                    if let Some((maj, min)) = exp.dev {
                        let label = child.special_type.clone().unwrap_or_default();
                        let want = format!("({maj}, {min})");
                        if !label.contains(&want) {
                            mismatches.push(format!(
                                "{}: device {:?} does not carry oracle {}",
                                child.path, label, want
                            ));
                        }
                    }
                }
                if child.entry_type == EntryType::Directory {
                    queue.push(child);
                }
            }
        }

        eprintln!("compared {checked} entries against unsquashfs");
        assert!(
            checked > 1000,
            "only compared {checked} entries — the walk did not cover the image"
        );
        assert!(
            mismatches.is_empty(),
            "{} mismatches vs unsquashfs, first 20:\n{}",
            mismatches.len(),
            mismatches
                .iter()
                .take(20)
                .cloned()
                .collect::<Vec<_>>()
                .join("\n")
        );
    }

    /// Fixture-gated stress read of the same real image. The shallow test above
    /// barely touches the data path; this walks a few thousand entries and reads
    /// files in full, which is what actually exercises multi-block files,
    /// fragment tails and symlink targets.
    #[test]
    fn walks_and_reads_a_real_squashfs_broadly() {
        let path = std::env::var("RB_SQUASHFS_ISO").unwrap_or_else(|_| {
            "/Users/dani/Downloads/ubuntu-12.04-desktop-powerpc.iso".to_string()
        });
        let Ok(mut file) = std::fs::File::open(&path) else {
            eprintln!("{path} not present — skipping");
            return;
        };
        let Some(offset) = find_squashfs_offset(&mut file) else {
            return;
        };
        let mut fs = SquashfsFilesystem::open(file, offset).expect("open");

        let root = fs.root().expect("root");
        let mut queue = vec![root];
        let (mut dirs, mut files, mut symlinks) = (0usize, 0usize, 0usize);
        let mut multi_block: Option<FileEntry> = None;
        let mut a_symlink: Option<FileEntry> = None;
        let mut bytes_read = 0u64;

        // Bounded so the test stays quick; 2000 directories is deep enough to
        // cross many metadata blocks and fragments.
        while let Some(dir) = queue.pop() {
            if dirs >= 2000 {
                break;
            }
            dirs += 1;
            let entries = match fs.list_directory(&dir) {
                Ok(e) => e,
                Err(e) => panic!("list {} failed: {e}", dir.path),
            };
            for e in entries {
                match e.entry_type {
                    EntryType::Directory => queue.push(e),
                    EntryType::Symlink => {
                        symlinks += 1;
                        if a_symlink.is_none() {
                            a_symlink = Some(e);
                        }
                    }
                    EntryType::File => {
                        files += 1;
                        if e.size > 131_072 && multi_block.is_none() {
                            multi_block = Some(e.clone());
                        }
                        // Read a sample of files in full and confirm the data
                        // path returns exactly the size the inode advertised.
                        if files % 97 == 0 && e.size > 0 && e.size < 2_000_000 {
                            let got = fs
                                .read_file(&e, usize::MAX)
                                .unwrap_or_else(|err| panic!("read {} failed: {err}", e.path));
                            assert_eq!(
                                got.len() as u64,
                                e.size,
                                "{} read back {} bytes, inode said {}",
                                e.path,
                                got.len(),
                                e.size
                            );
                            bytes_read += got.len() as u64;
                        }
                    }
                    EntryType::Special => {}
                }
            }
        }

        eprintln!(
            "walked {dirs} dirs, {files} files, {symlinks} symlinks; read {bytes_read} bytes"
        );
        assert!(
            files > 1000,
            "expected a populated image, saw {files} files"
        );
        assert!(symlinks > 0, "a Linux rootfs always has symlinks");

        // A file larger than one 128 KiB block exercises the block list rather
        // than a single fragment.
        let big = multi_block.expect("no multi-block file found");
        let got = fs
            .read_file(&big, usize::MAX)
            .expect("read multi-block file");
        assert_eq!(got.len() as u64, big.size, "{}", big.path);
        eprintln!("multi-block: {} ({} bytes)", big.path, got.len());

        // Symlink targets come from the inode, not the data area.
        let link = a_symlink.expect("symlink");
        assert!(
            link.symlink_target
                .as_deref()
                .is_some_and(|t| !t.is_empty()),
            "symlink {} has no target",
            link.path
        );
        eprintln!(
            "symlink: {} -> {}",
            link.path,
            link.symlink_target.as_deref().unwrap_or("")
        );
    }

    /// The routing layer must recognise a SquashFS partition on its own —
    /// callers go through `open_filesystem`, never the concrete type.
    #[test]
    fn dispatch_routes_a_real_image_to_squashfs() {
        let path = std::env::var("RB_SQUASHFS_ISO").unwrap_or_else(|_| {
            "/Users/dani/Downloads/ubuntu-12.04-desktop-powerpc.iso".to_string()
        });
        let Ok(mut file) = std::fs::File::open(&path) else {
            eprintln!("{path} not present — skipping");
            return;
        };
        let Some(offset) = find_squashfs_offset(&mut file) else {
            return;
        };

        // Type byte 0 is the auto-detect path; 0x83 is how an appliance image
        // stamps a Linux root partition.
        for type_byte in [0x00u8, 0x83u8] {
            let f = std::fs::File::open(&path).expect("reopen");
            let mut fs = crate::fs::open_filesystem(f, offset, type_byte, None)
                .unwrap_or_else(|e| panic!("dispatch failed for type 0x{type_byte:02X}: {e}"));
            assert_eq!(fs.fs_type(), "SquashFS");
            let root = fs.root().expect("root");
            assert!(!fs.list_directory(&root).expect("list").is_empty());
        }

        // And the 0x83 probe should name it for the partition table view.
        let mut f = std::fs::File::open(&path).expect("reopen");
        assert_eq!(
            crate::fs::probe_0x83_fs_type(&mut f, offset),
            Some("SquashFS")
        );
    }

    /// Scan 2048-byte boundaries for a SquashFS superblock.
    #[cfg(test)]
    fn find_squashfs_offset(file: &mut std::fs::File) -> Option<u64> {
        use std::io::Read as _;
        let mut pos = 0u64;
        let mut buf = vec![0u8; 1 << 22];
        loop {
            file.seek(SeekFrom::Start(pos)).ok()?;
            let n = file.read(&mut buf).ok()?;
            if n == 0 {
                return None;
            }
            let mut i = 0;
            while i + 4 <= n {
                if buf[i..i + 4] == SQUASHFS_MAGIC.to_le_bytes() {
                    return Some(pos + i as u64);
                }
                i += 2048;
            }
            pos += n as u64;
        }
    }
}
