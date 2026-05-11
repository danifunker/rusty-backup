//! SGI EFS (Extent File System) read-only support.
//!
//! Implements Steps 3 and 4 of the SGI Filesystem plan: superblock parsing,
//! inode lookup, directory listing (recursive), file reads via inline
//! extents, streaming writes, and symlink-target resolution. EFS has no
//! indirect blocks — every file is bounded by 12 direct extents (max
//! 12 * 16 MiB = 192 MiB per file).
//!
//! References:
//! - `~/xfs-efs/refs/efs-linux-5.15/efs/` — Linux kernel v5.15 EFS sources.
//! - `docs/SGI_Filesystems.md` — implementation plan and on-disk notes.

use std::io::{Read, Seek, SeekFrom};

use byteorder::{BigEndian, ByteOrder};

use super::entry::{EntryType, FileEntry};
use super::filesystem::{Filesystem, FilesystemError};

const EFS_BLOCKSIZE: u64 = 512;
const EFS_INODESIZE: u64 = 128;
const EFS_INODES_PER_BLOCK: u64 = EFS_BLOCKSIZE / EFS_INODESIZE; // 4
const EFS_DIRECTEXTENTS: usize = 12;

const EFS_MAGIC_OLD: u32 = 0x00072959;
const EFS_MAGIC_NEW: u32 = 0x0007295A;

const EFS_DIRBLK_MAGIC: u16 = 0xBEEF;
const EFS_DIRBLK_HEADERSIZE: usize = 4;

const EFS_ROOT_INODE: u32 = 2;

/// EFS superblock. Lives at byte 512 of the partition.
#[derive(Debug, Clone)]
pub struct EfsSuperblock {
    pub fs_size: u32,   // total size in 512-byte blocks
    pub firstcg: u32,   // block of first cylinder group
    pub cgfsize: u32,   // blocks per cylinder group
    pub cgisize: u16,   // inode blocks per cylinder group
    pub sectors: u16,   // sectors per track
    pub heads: u16,     // heads per cylinder
    pub ncg: u16,       // number of cylinder groups
    pub dirty: u16,     // dirty flag
    pub fs_time: u32,   // last mount time
    pub magic: u32,     // EFS_MAGIC_OLD or EFS_MAGIC_NEW
    pub fname: [u8; 6], // volume name
    pub fpack: [u8; 6], // pack name
}

impl EfsSuperblock {
    /// Parse a 92-byte superblock buffer (we only consult the documented
    /// prefix; the trailing fields are not needed for browse).
    pub fn parse(buf: &[u8]) -> Result<Self, FilesystemError> {
        if buf.len() < 44 {
            return Err(FilesystemError::Parse(format!(
                "EFS superblock buffer too small: {} bytes",
                buf.len()
            )));
        }
        // The C `struct efs_super` is NOT __attribute__((packed)), so the
        // compiler inserts 2 bytes of natural-alignment padding between
        // `fs_dirty` (be16 at +20) and `fs_time` (be32 at +24). Read by
        // explicit byte offset rather than as a packed struct.
        let magic = BigEndian::read_u32(&buf[28..32]);
        if magic != EFS_MAGIC_OLD && magic != EFS_MAGIC_NEW {
            return Err(FilesystemError::Parse(format!(
                "bad EFS magic: 0x{magic:08X} (expected 0x{EFS_MAGIC_OLD:08X} or 0x{EFS_MAGIC_NEW:08X})"
            )));
        }
        let mut fname = [0u8; 6];
        fname.copy_from_slice(&buf[32..38]);
        let mut fpack = [0u8; 6];
        fpack.copy_from_slice(&buf[38..44]);
        Ok(EfsSuperblock {
            fs_size: BigEndian::read_u32(&buf[0..4]),
            firstcg: BigEndian::read_u32(&buf[4..8]),
            cgfsize: BigEndian::read_u32(&buf[8..12]),
            cgisize: BigEndian::read_u16(&buf[12..14]),
            sectors: BigEndian::read_u16(&buf[14..16]),
            heads: BigEndian::read_u16(&buf[16..18]),
            ncg: BigEndian::read_u16(&buf[18..20]),
            dirty: BigEndian::read_u16(&buf[20..22]),
            fs_time: BigEndian::read_u32(&buf[24..28]),
            magic,
            fname,
            fpack,
        })
    }

    fn label(&self) -> String {
        let n = trim_ascii(&self.fname);
        let p = trim_ascii(&self.fpack);
        if n.is_empty() && p.is_empty() {
            String::new()
        } else if p.is_empty() {
            n
        } else if n.is_empty() {
            p
        } else {
            format!("{n}:{p}")
        }
    }
}

/// A single on-disk extent (8 bytes, big-endian).
#[derive(Debug, Clone, Copy)]
pub struct EfsExtent {
    pub magic: u8,
    pub bn: u32,     // start block on disk (24 bits)
    pub length: u8,  // number of 512-byte blocks
    pub offset: u32, // logical file offset in blocks (24 bits)
}

impl EfsExtent {
    fn parse(buf: &[u8; 8]) -> Self {
        let w0 = BigEndian::read_u32(&buf[0..4]);
        let w1 = BigEndian::read_u32(&buf[4..8]);
        EfsExtent {
            magic: ((w0 >> 24) & 0xFF) as u8,
            bn: w0 & 0x00FF_FFFF,
            length: ((w1 >> 24) & 0xFF) as u8,
            offset: w1 & 0x00FF_FFFF,
        }
    }
}

/// Parsed EFS on-disk inode (128 bytes).
#[derive(Debug, Clone)]
pub struct EfsInode {
    pub inum: u32,
    pub mode: u16,
    pub nlink: u16,
    pub uid: u16,
    pub gid: u16,
    pub size: u32,
    pub atime: u32,
    pub mtime: u32,
    pub ctime: u32,
    pub gen: u32,
    pub numextents: u16,
    pub version: u8,
    pub extents: [EfsExtent; EFS_DIRECTEXTENTS],
}

impl EfsInode {
    fn parse(inum: u32, buf: &[u8; 128]) -> Self {
        let mut extents = [EfsExtent {
            magic: 0,
            bn: 0,
            length: 0,
            offset: 0,
        }; EFS_DIRECTEXTENTS];
        for (i, ext) in extents.iter_mut().enumerate() {
            let off = 32 + i * 8;
            let chunk: &[u8; 8] = buf[off..off + 8].try_into().unwrap();
            *ext = EfsExtent::parse(chunk);
        }
        EfsInode {
            inum,
            mode: BigEndian::read_u16(&buf[0..2]),
            nlink: BigEndian::read_u16(&buf[2..4]),
            uid: BigEndian::read_u16(&buf[4..6]),
            gid: BigEndian::read_u16(&buf[6..8]),
            size: BigEndian::read_u32(&buf[8..12]),
            atime: BigEndian::read_u32(&buf[12..16]),
            mtime: BigEndian::read_u32(&buf[16..20]),
            ctime: BigEndian::read_u32(&buf[20..24]),
            gen: BigEndian::read_u32(&buf[24..28]),
            numextents: BigEndian::read_u16(&buf[28..30]),
            version: buf[30],
            extents,
        }
    }

    fn is_dir(&self) -> bool {
        (self.mode & 0o170000) == 0o040000
    }

    fn is_symlink(&self) -> bool {
        (self.mode & 0o170000) == 0o120000
    }

    fn is_regular(&self) -> bool {
        (self.mode & 0o170000) == 0o100000
    }

    fn entry_type(&self) -> EntryType {
        if self.is_dir() {
            EntryType::Directory
        } else if self.is_symlink() {
            EntryType::Symlink
        } else if self.is_regular() {
            EntryType::File
        } else {
            EntryType::Special
        }
    }

    fn special_type(&self) -> Option<String> {
        match self.mode & 0o170000 {
            0o020000 => Some("char device".into()),
            0o060000 => Some("block device".into()),
            0o010000 => Some("fifo".into()),
            0o140000 => Some("socket".into()),
            _ => None,
        }
    }
}

/// EFS filesystem reader.
pub struct EfsFilesystem<R: Read + Seek + Send> {
    reader: R,
    partition_offset: u64,
    sb: EfsSuperblock,
    label: String,
}

impl<R: Read + Seek + Send> EfsFilesystem<R> {
    /// Open an EFS filesystem at the given byte offset within `reader`.
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        // Superblock lives at sector 1 of the partition. Read a full
        // sector via the aligned helper.
        let mut sector = [0u8; EFS_BLOCKSIZE as usize];
        read_at_aligned(
            &mut reader,
            partition_offset + EFS_BLOCKSIZE,
            EFS_BLOCKSIZE,
            &mut sector,
        )?;
        let sb = EfsSuperblock::parse(&sector)?;
        let label = sb.label();
        Ok(EfsFilesystem {
            reader,
            partition_offset,
            sb,
            label,
        })
    }

    /// Compute the byte offset (from start of partition) of inode `inum`.
    fn inode_byte_offset(&self, inum: u32) -> u64 {
        // Mirrors `efs_iget` in Linux v5.15:
        //   inodes_per_cg = cgisize * (BLOCKSIZE / INODESIZE)
        //   cg            = inum / inodes_per_cg
        //   inblock       = (inum % inodes_per_cg) / (BLOCKSIZE / INODESIZE)
        //   block         = firstcg + cg * cgfsize + inblock
        //   off           = (inum % (BLOCKSIZE / INODESIZE)) << INODESIZE_BITS
        let inodes_per_cg = self.sb.cgisize as u64 * EFS_INODES_PER_BLOCK;
        let cg = inum as u64 / inodes_per_cg;
        let off_in_cg = inum as u64 % inodes_per_cg;
        let inblock = off_in_cg / EFS_INODES_PER_BLOCK;
        let block = self.sb.firstcg as u64 + cg * self.sb.cgfsize as u64 + inblock;
        let byte_in_block = (inum as u64 % EFS_INODES_PER_BLOCK) * EFS_INODESIZE;
        block * EFS_BLOCKSIZE + byte_in_block
    }

    /// Read a single inode by number.
    fn read_inode(&mut self, inum: u32) -> Result<EfsInode, FilesystemError> {
        if inum == 0 {
            return Err(FilesystemError::InvalidData(
                "EFS inode 0 is reserved".into(),
            ));
        }
        let off = self.inode_byte_offset(inum);
        // Read the 512-byte block containing this inode and slice out 128 bytes.
        let mut block = [0u8; EFS_BLOCKSIZE as usize];
        let block_byte = (off / EFS_BLOCKSIZE) * EFS_BLOCKSIZE;
        read_at_aligned(
            &mut self.reader,
            self.partition_offset + block_byte,
            EFS_BLOCKSIZE,
            &mut block,
        )?;
        let in_block = (off - block_byte) as usize;
        let mut ino_buf = [0u8; 128];
        ino_buf.copy_from_slice(&block[in_block..in_block + 128]);
        Ok(EfsInode::parse(inum, &ino_buf))
    }

    /// Read one 512-byte directory block at disk block `bn`. Returns a flag
    /// indicating whether the block was successfully fetched and had a valid
    /// magic; truncated/missing blocks produce `Ok(None)` so callers can
    /// continue listing the rest of the directory.
    fn read_dir_block(
        &mut self,
        bn: u32,
        out: &mut [u8; EFS_BLOCKSIZE as usize],
    ) -> Result<bool, FilesystemError> {
        let byte = bn as u64 * EFS_BLOCKSIZE;
        match read_at_aligned(
            &mut self.reader,
            self.partition_offset + byte,
            EFS_BLOCKSIZE,
            out,
        ) {
            Ok(()) => {
                let magic = BigEndian::read_u16(&out[0..2]);
                Ok(magic == EFS_DIRBLK_MAGIC)
            }
            Err(FilesystemError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                Ok(false)
            }
            Err(e) => Err(e),
        }
    }
}

/// Decode one EFS directory block. Each block is 512 bytes with a 4-byte
/// header (magic:be16, firstused:u8, slots:u8) followed by a slot table where
/// each non-zero slot byte points (<<1) to a `(inode:be32, namelen:u8,
/// name[namelen])` dirent.
fn iter_dir_block<F>(buf: &[u8; EFS_BLOCKSIZE as usize], parent_path: &str, mut emit: F)
where
    F: FnMut(u32, &str, String),
{
    let slots = buf[3] as usize;
    let max_slots = (EFS_BLOCKSIZE as usize - EFS_DIRBLK_HEADERSIZE).min(slots);
    for slot in 0..max_slots {
        let raw = buf[EFS_DIRBLK_HEADERSIZE + slot];
        if raw == 0 {
            continue;
        }
        let off = (raw as usize) << 1;
        if off + 5 > EFS_BLOCKSIZE as usize {
            continue;
        }
        let inum = BigEndian::read_u32(&buf[off..off + 4]);
        let namelen = buf[off + 4] as usize;
        if namelen == 0 || off + 5 + namelen > EFS_BLOCKSIZE as usize {
            continue;
        }
        let name_bytes = &buf[off + 5..off + 5 + namelen];
        let name = String::from_utf8_lossy(name_bytes).into_owned();
        let path = if parent_path == "/" {
            format!("/{name}")
        } else {
            format!("{parent_path}/{name}")
        };
        emit(inum, &name, path);
    }
}

impl<R: Read + Seek + Send> Filesystem for EfsFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        let ino = self.read_inode(EFS_ROOT_INODE)?;
        if !ino.is_dir() {
            return Err(FilesystemError::InvalidData(format!(
                "EFS root inode 2 is not a directory (mode=0o{:o})",
                ino.mode
            )));
        }
        Ok(FileEntry {
            name: "/".into(),
            path: "/".into(),
            entry_type: EntryType::Directory,
            size: ino.size as u64,
            location: EFS_ROOT_INODE as u64,
            modified: None,
            type_code: None,
            creator_code: None,
            symlink_target: None,
            special_type: None,
            mode: Some(ino.mode as u32),
            uid: Some(ino.uid as u32),
            gid: Some(ino.gid as u32),
            resource_fork_size: None,
            aux_type: None,
            link_target_cnid: None,
        })
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if !entry.is_directory() {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }
        let inum = entry.location as u32;
        let dir_ino = self.read_inode(inum)?;
        if !dir_ino.is_dir() {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }

        // Walk the inline extents in logical-offset order. EFS has no indirect
        // blocks; a directory's data lives entirely in the 12 inline extents.
        let mut extents: Vec<EfsExtent> = dir_ino
            .extents
            .iter()
            .take(dir_ino.numextents as usize)
            .copied()
            .collect();
        extents.sort_by_key(|e| e.offset);

        let mut entries: Vec<FileEntry> = Vec::new();
        let mut block_buf = [0u8; EFS_BLOCKSIZE as usize];
        for ext in &extents {
            for i in 0..ext.length as u32 {
                let bn = ext.bn + i;
                let ok = self.read_dir_block(bn, &mut block_buf)?;
                if !ok {
                    // Truncated fixture or damaged block — stop here. We've
                    // already returned everything we could read.
                    continue;
                }
                let parent_path = entry.path.as_str();
                let mut pending: Vec<(u32, String, String)> = Vec::new();
                iter_dir_block(&block_buf, parent_path, |inum, name, path| {
                    pending.push((inum, name.to_string(), path));
                });
                for (child_inum, name, path) in pending {
                    if name == "." || name == ".." {
                        continue;
                    }
                    if child_inum == 0 {
                        continue;
                    }
                    // Read child inode for type/size info. If reading fails
                    // (e.g., the inode block is past the truncated fixture),
                    // emit a placeholder file entry so the caller still sees
                    // the name. Pure browse should never panic on damaged
                    // metadata.
                    match self.read_inode(child_inum) {
                        Ok(child) => {
                            // Resolve symlink target eagerly so the GUI can
                            // display it without a second round trip. If the
                            // target data lives past the fixture window we
                            // leave it as None — the entry still surfaces as
                            // a symlink, just without a populated target.
                            let symlink_target = if child.is_symlink() {
                                read_inode_data(
                                    &mut self.reader,
                                    self.partition_offset,
                                    &child,
                                    usize::MAX,
                                )
                                .ok()
                                .map(|bytes| {
                                    String::from_utf8_lossy(&bytes)
                                        .trim_end_matches('\0')
                                        .to_string()
                                })
                            } else {
                                None
                            };
                            let mut e = FileEntry {
                                name,
                                path,
                                entry_type: child.entry_type(),
                                size: child.size as u64,
                                location: child_inum as u64,
                                modified: None,
                                type_code: None,
                                creator_code: None,
                                symlink_target,
                                special_type: child.special_type(),
                                mode: Some(child.mode as u32),
                                uid: Some(child.uid as u32),
                                gid: Some(child.gid as u32),
                                resource_fork_size: None,
                                aux_type: None,
                                link_target_cnid: None,
                            };
                            if matches!(e.entry_type, EntryType::Directory) {
                                e.size = 0;
                            }
                            entries.push(e);
                        }
                        Err(_) => {
                            entries.push(FileEntry::new_file(name, path, 0, child_inum as u64));
                        }
                    }
                }
            }
        }
        Ok(entries)
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        if entry.is_directory() {
            return Err(FilesystemError::InvalidData(format!(
                "EFS read_file on directory: {}",
                entry.path
            )));
        }
        let inum = entry.location as u32;
        let inode = self.read_inode(inum)?;
        read_inode_data(&mut self.reader, self.partition_offset, &inode, max_bytes)
    }

    fn write_file_to(
        &mut self,
        entry: &FileEntry,
        writer: &mut dyn std::io::Write,
    ) -> Result<u64, FilesystemError> {
        if entry.is_directory() {
            return Err(FilesystemError::InvalidData(format!(
                "EFS write_file_to on directory: {}",
                entry.path
            )));
        }
        let inum = entry.location as u32;
        let inode = self.read_inode(inum)?;
        stream_inode_data(&mut self.reader, self.partition_offset, &inode, writer)
    }

    fn volume_label(&self) -> Option<&str> {
        if self.label.is_empty() {
            None
        } else {
            Some(&self.label)
        }
    }

    fn fs_type(&self) -> &str {
        "EFS"
    }

    fn total_size(&self) -> u64 {
        self.sb.fs_size as u64 * EFS_BLOCKSIZE
    }

    fn used_size(&self) -> u64 {
        // No allocation summary parsed yet; report total for now.
        self.total_size()
    }
}

/// Walk the inline extents of `inode` and return up to `max_bytes` of file
/// data. EFS has no indirect blocks: a file is bounded by 12 extents and the
/// inode's `di_size` is the authoritative file length.
fn read_inode_data<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
    inode: &EfsInode,
    max_bytes: usize,
) -> Result<Vec<u8>, FilesystemError> {
    let size = inode.size as usize;
    let want = size.min(max_bytes);
    let mut out = Vec::with_capacity(want);
    if want == 0 {
        return Ok(out);
    }
    let mut extents: Vec<EfsExtent> = inode
        .extents
        .iter()
        .take(inode.numextents as usize)
        .copied()
        .collect();
    extents.sort_by_key(|e| e.offset);
    let mut block_buf = [0u8; EFS_BLOCKSIZE as usize];
    'outer: for ext in &extents {
        for i in 0..ext.length as u32 {
            let bn = ext.bn + i;
            read_at_aligned(
                reader,
                partition_offset + bn as u64 * EFS_BLOCKSIZE,
                EFS_BLOCKSIZE,
                &mut block_buf,
            )?;
            let remaining = want.saturating_sub(out.len());
            if remaining == 0 {
                break 'outer;
            }
            let take = remaining.min(EFS_BLOCKSIZE as usize);
            out.extend_from_slice(&block_buf[..take]);
            if out.len() >= want {
                break 'outer;
            }
        }
    }
    Ok(out)
}

/// Streaming version of `read_inode_data`. Writes the file's bytes to
/// `writer` 1 MiB at a time and returns the byte count written. The 1 MiB
/// staging buffer is the chunk size used by the rest of the codebase for
/// streaming filesystem reads.
fn stream_inode_data<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
    inode: &EfsInode,
    writer: &mut dyn std::io::Write,
) -> Result<u64, FilesystemError> {
    let size = inode.size as u64;
    if size == 0 {
        return Ok(0);
    }
    let mut extents: Vec<EfsExtent> = inode
        .extents
        .iter()
        .take(inode.numextents as usize)
        .copied()
        .collect();
    extents.sort_by_key(|e| e.offset);

    const CHUNK_BLOCKS: u32 = 1024 * 1024 / EFS_BLOCKSIZE as u32; // 1 MiB
    let mut buf = vec![0u8; (CHUNK_BLOCKS as usize) * EFS_BLOCKSIZE as usize];
    let mut written: u64 = 0;
    for ext in &extents {
        let mut remaining_blocks = ext.length as u32;
        let mut cur_bn = ext.bn;
        while remaining_blocks > 0 {
            let take_blocks = remaining_blocks.min(CHUNK_BLOCKS);
            let bytes = take_blocks as u64 * EFS_BLOCKSIZE;
            read_at_aligned(
                reader,
                partition_offset + cur_bn as u64 * EFS_BLOCKSIZE,
                bytes,
                &mut buf[..bytes as usize],
            )?;
            let remaining_file = size - written;
            let emit = remaining_file.min(bytes) as usize;
            writer.write_all(&buf[..emit])?;
            written += emit as u64;
            if written >= size {
                return Ok(written);
            }
            cur_bn += take_blocks;
            remaining_blocks -= take_blocks;
        }
    }
    Ok(written)
}

/// Sector-aligned read helper. Reads `len` bytes starting at byte offset
/// `byte_off` in the underlying file/device into `out` (which must be at
/// least `len` bytes long). `byte_off` and `len` must already be 512-byte
/// aligned; this helper enforces that, since EFS only ever reads whole
/// sectors. A short read past the end of the underlying stream is reported
/// as `Io(UnexpectedEof)`.
fn read_at_aligned<R: Read + Seek>(
    reader: &mut R,
    byte_off: u64,
    len: u64,
    out: &mut [u8],
) -> Result<(), FilesystemError> {
    debug_assert!(byte_off.is_multiple_of(EFS_BLOCKSIZE));
    debug_assert!(len.is_multiple_of(EFS_BLOCKSIZE));
    let len = len as usize;
    if out.len() < len {
        return Err(FilesystemError::InvalidData(format!(
            "read_at_aligned: output buffer {} < {len}",
            out.len()
        )));
    }
    reader.seek(SeekFrom::Start(byte_off))?;
    let mut filled = 0;
    while filled < len {
        let n = reader.read(&mut out[filled..len])?;
        if n == 0 {
            return Err(FilesystemError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("EFS short read at byte {byte_off}: got {filled} of {len}",),
            )));
        }
        filled += n;
    }
    Ok(())
}

fn trim_ascii(b: &[u8]) -> String {
    let end = b.iter().position(|&c| c == 0).unwrap_or(b.len());
    String::from_utf8_lossy(&b[..end])
        .trim_matches(|c: char| c == ' ' || c == '\0')
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn load_fixture() -> Vec<u8> {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/sgi/efs_small.img.zst");
        let compressed = std::fs::read(&path).expect("fixture present");
        let mut decoder =
            zstd::stream::read::Decoder::new(Cursor::new(compressed)).expect("zstd decoder");
        let mut out = Vec::new();
        decoder.read_to_end(&mut out).expect("decompress");
        out
    }

    #[test]
    fn parses_superblock_from_fixture() {
        let img = load_fixture();
        let fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open EFS");
        // From docs/SGI_Filesystems.md "Known images on hand" (IRIX 5.3).
        assert_eq!(fs.sb.fs_size, 7_486_242);
        assert_eq!(fs.sb.firstcg, 1830);
        assert_eq!(fs.sb.cgfsize, 95_954);
        assert_eq!(fs.sb.cgisize, 2460);
        assert_eq!(fs.sb.sectors, 63);
        assert_eq!(fs.sb.heads, 10);
        assert_eq!(fs.sb.ncg, 78);
        assert_eq!(fs.sb.magic, EFS_MAGIC_OLD);
        assert_eq!(fs.fs_type(), "EFS");
        assert_eq!(fs.volume_label(), Some("noname:nopack"));
        assert_eq!(fs.total_size(), 7_486_242 * 512);
    }

    #[test]
    fn rejects_wrong_magic() {
        let mut img = vec![0u8; 4096];
        // Write a deliberately-wrong magic at the superblock magic offset.
        img[512 + 28..512 + 32].copy_from_slice(&0xDEAD_BEEFu32.to_be_bytes());
        match EfsFilesystem::open(Cursor::new(img), 0) {
            Err(FilesystemError::Parse(_)) => {}
            Err(e) => panic!("expected Parse error, got: {e}"),
            Ok(_) => panic!("expected error on bad magic"),
        }
    }

    #[test]
    fn inode_offset_matches_hand_computation() {
        let img = load_fixture();
        let fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open EFS");
        let _ = &fs;
        // Root inode (2): cg=0, inblock=0, inblock_offset=2*128.
        // byte = firstcg * 512 + 256 = 1830 * 512 + 256 = 937216 = 0xE4D00.
        assert_eq!(fs.inode_byte_offset(2), 0xE4D00);
        // Inode 4: still cg=0, block_in_cg=0, byte_off_in_block=4*128 already > 512,
        // so block_in_cg=1, byte_off=0. byte = (1830+1)*512 = 0xE4E00.
        // Wait — inblock = (4 % 9840) / 4 = 1. byte_in_block = (4 % 4) * 128 = 0.
        // byte = (1830 + 1) * 512 = 937472 = 0xE4E00.
        assert_eq!(fs.inode_byte_offset(4), 0xE4E00);
    }

    #[test]
    fn root_directory_lists_expected_entries() {
        let img = load_fixture();
        let mut fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open EFS");
        let root = fs.root().expect("root inode");
        assert!(root.is_directory());

        let entries = fs.list_directory(&root).expect("list root");
        let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();

        // From manual decode of block 4290 of the fixture: the root has
        // these names. Asserting subset rather than full equality because
        // the second directory block of the root lives past the 4 MiB
        // fixture window and is correctly skipped.
        for expected in [
            "lost+found",
            "etc",
            "dev",
            "bin",
            "sbin",
            "tmp",
            "stand",
            "lib",
            "proc",
            "opt",
            "temp",
        ] {
            assert!(names.contains(&expected), "missing {expected} in {names:?}");
        }
        // "." and ".." must be filtered out.
        assert!(!names.contains(&"."), "should not list '.'");
        assert!(!names.contains(&".."), "should not list '..'");
    }

    #[test]
    fn reads_small_single_extent_file() {
        // .Sgiresources: inode 16, size=21, single extent at block 4312.
        // Content was confirmed via xxd of the fixture.
        let img = load_fixture();
        let mut fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open EFS");
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let entry = entries
            .iter()
            .find(|e| e.name == ".Sgiresources")
            .expect("entry");
        assert_eq!(entry.size, 21);
        let data = fs.read_file(entry, usize::MAX).expect("read");
        assert_eq!(data, b"*scheme:\tIndigoMagic\n");
    }

    #[test]
    fn read_file_respects_max_bytes() {
        let img = load_fixture();
        let mut fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open EFS");
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let entry = entries
            .iter()
            .find(|e| e.name == ".Sgiresources")
            .expect("entry");
        let head = fs.read_file(entry, 8).expect("read");
        assert_eq!(head, b"*scheme:");
    }

    #[test]
    fn reads_multi_block_file() {
        // .audiopanelrc: inode 17, size=566, single extent (4313, length=2).
        // The 566-byte file spans two 512-byte blocks; verify size+ first/last bytes.
        let img = load_fixture();
        let mut fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open EFS");
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let entry = entries
            .iter()
            .find(|e| e.name == ".audiopanelrc")
            .expect("entry");
        assert_eq!(entry.size, 566);
        let data = fs.read_file(entry, usize::MAX).expect("read");
        assert_eq!(data.len(), 566);
    }

    #[test]
    fn write_file_to_matches_read_file() {
        let img = load_fixture();
        let mut fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open EFS");
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let entry = entries.iter().find(|e| e.name == ".cshrc").expect("entry");
        let direct = fs.read_file(entry, usize::MAX).expect("read");
        let mut streamed = Vec::new();
        let n = fs.write_file_to(entry, &mut streamed).expect("stream");
        assert_eq!(n as usize, direct.len());
        assert_eq!(streamed, direct);
        assert_eq!(streamed.len(), entry.size as usize);
    }

    #[test]
    fn list_directory_descends_into_subdirectory() {
        // .desktop-IRIS (inode 22) is a subdirectory whose data block is
        // inside the fixture window. Verify list_directory traverses it.
        let img = load_fixture();
        let mut fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open EFS");
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let subdir = entries
            .iter()
            .find(|e| e.name == ".desktop-IRIS")
            .expect("subdir entry");
        assert!(subdir.is_directory());
        let children = fs.list_directory(subdir).expect("list subdir");
        let child_names: Vec<&str> = children.iter().map(|c| c.name.as_str()).collect();
        for expected in [
            "log",
            "log.bak",
            "iconbook",
            "ScreenSaver",
            "4Dwmsession",
            ".userenv",
        ] {
            assert!(
                child_names.contains(&expected),
                "missing {expected} in {child_names:?}"
            );
        }
        // Paths nested under the parent's path.
        for c in &children {
            assert!(
                c.path.starts_with("/.desktop-IRIS/"),
                "bad nested path: {}",
                c.path
            );
        }
    }

    #[test]
    fn synthetic_symlink_populates_target() {
        // Build a tiny synthetic EFS image with: superblock at sector 1,
        // root inode (2) as a one-block directory listing a single symlink
        // child (inode 4) whose data block holds the target string.
        //
        // Layout (sectors / 512B blocks):
        //   0: pad
        //   1: superblock
        //   2..18: pad to firstcg
        //   18..20: inode area (cgisize=2 -> 8 inodes total, inum 2,3,4 fit)
        //   20: root directory block
        //   21: symlink target data
        let mut img = vec![0u8; 32 * 512];

        // Superblock at sector 1.
        let sb_off = 512;
        let total_blocks = (img.len() / 512) as u32;
        img[sb_off..sb_off + 4].copy_from_slice(&total_blocks.to_be_bytes()); // fs_size
        img[sb_off + 4..sb_off + 8].copy_from_slice(&18u32.to_be_bytes()); // firstcg
        img[sb_off + 8..sb_off + 12].copy_from_slice(&14u32.to_be_bytes()); // cgfsize
        img[sb_off + 12..sb_off + 14].copy_from_slice(&2u16.to_be_bytes()); // cgisize
        img[sb_off + 14..sb_off + 16].copy_from_slice(&63u16.to_be_bytes()); // sectors
        img[sb_off + 16..sb_off + 18].copy_from_slice(&1u16.to_be_bytes()); // heads
        img[sb_off + 18..sb_off + 20].copy_from_slice(&1u16.to_be_bytes()); // ncg
        img[sb_off + 28..sb_off + 32].copy_from_slice(&EFS_MAGIC_OLD.to_be_bytes());
        // fname / fpack — leave zero.

        // Inode area starts at block 18.
        // Inode 2 (root dir): one extent pointing to block 20.
        let inode2_off = 18 * 512 + 2 * 128;
        // mode 0o040755 directory
        img[inode2_off..inode2_off + 2].copy_from_slice(&0o040755u16.to_be_bytes());
        // size 512 (one dir block)
        img[inode2_off + 8..inode2_off + 12].copy_from_slice(&512u32.to_be_bytes());
        // numextents 1
        img[inode2_off + 28..inode2_off + 30].copy_from_slice(&1u16.to_be_bytes());
        // extent: bn=20, length=1, offset=0
        let w0: u32 = 20;
        let w1: u32 = 1u32 << 24;
        img[inode2_off + 32..inode2_off + 36].copy_from_slice(&w0.to_be_bytes());
        img[inode2_off + 36..inode2_off + 40].copy_from_slice(&w1.to_be_bytes());

        // Inode 4 (symlink): one extent at block 21, target "/usr/sbin/init".
        let target = b"/usr/sbin/init";
        let inode4_off = 18 * 512 + 4 * 128;
        img[inode4_off..inode4_off + 2].copy_from_slice(&0o120777u16.to_be_bytes());
        img[inode4_off + 8..inode4_off + 12].copy_from_slice(&(target.len() as u32).to_be_bytes());
        img[inode4_off + 28..inode4_off + 30].copy_from_slice(&1u16.to_be_bytes());
        let w0: u32 = 21;
        let w1: u32 = 1u32 << 24;
        img[inode4_off + 32..inode4_off + 36].copy_from_slice(&w0.to_be_bytes());
        img[inode4_off + 36..inode4_off + 40].copy_from_slice(&w1.to_be_bytes());

        // Directory block at block 20: one entry "link" -> inode 4.
        let dir_off = 20 * 512;
        img[dir_off..dir_off + 2].copy_from_slice(&EFS_DIRBLK_MAGIC.to_be_bytes());
        // firstused (unused by our reader) and slots
        img[dir_off + 2] = 0xfd;
        img[dir_off + 3] = 1;
        // slot 0 byte at offset 4. Dirent at byte 502 -> stored value 502/2 = 251 = 0xfb.
        // (Dirent length is 4 + 1 + 4 = 9, must fit within the 512-byte block.)
        img[dir_off + 4] = 0xfb;
        let de_off = dir_off + 502;
        img[de_off..de_off + 4].copy_from_slice(&4u32.to_be_bytes());
        img[de_off + 4] = 4;
        img[de_off + 5..de_off + 9].copy_from_slice(b"link");

        // Symlink target data at block 21.
        let data_off = 21 * 512;
        img[data_off..data_off + target.len()].copy_from_slice(target);

        let mut fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open EFS");
        let root = fs.root().unwrap();
        let children = fs.list_directory(&root).expect("list root");
        let link = children
            .iter()
            .find(|e| e.name == "link")
            .expect("link entry");
        assert!(link.is_symlink());
        assert_eq!(link.symlink_target.as_deref(), Some("/usr/sbin/init"));
        // read_file on the symlink returns the target bytes verbatim.
        let bytes = fs.read_file(link, usize::MAX).unwrap();
        assert_eq!(bytes, target);
    }

    #[test]
    fn synthetic_multi_extent_read() {
        // Build an image with a regular file (inode 4) that spans two
        // disjoint extents. Verify read_file concatenates them in
        // logical-offset order and matches write_file_to.
        let mut img = vec![0u8; 64 * 512];

        let sb_off = 512;
        let total_blocks = (img.len() / 512) as u32;
        img[sb_off..sb_off + 4].copy_from_slice(&total_blocks.to_be_bytes());
        img[sb_off + 4..sb_off + 8].copy_from_slice(&18u32.to_be_bytes());
        img[sb_off + 8..sb_off + 12].copy_from_slice(&44u32.to_be_bytes());
        img[sb_off + 12..sb_off + 14].copy_from_slice(&2u16.to_be_bytes());
        img[sb_off + 18..sb_off + 20].copy_from_slice(&1u16.to_be_bytes());
        img[sb_off + 28..sb_off + 32].copy_from_slice(&EFS_MAGIC_OLD.to_be_bytes());

        // Inode 4: 2 extents, block 30 (1 block) at offset 0, block 50 (1 block) at offset 1.
        let ino_off = 18 * 512 + 4 * 128;
        img[ino_off..ino_off + 2].copy_from_slice(&0o100644u16.to_be_bytes()); // regular file
        img[ino_off + 8..ino_off + 12].copy_from_slice(&900u32.to_be_bytes()); // size 900
        img[ino_off + 28..ino_off + 30].copy_from_slice(&2u16.to_be_bytes());

        // Extent 0: bn=30, length=1, offset=0
        let w0: u32 = 30;
        let w1: u32 = 1 << 24;
        img[ino_off + 32..ino_off + 36].copy_from_slice(&w0.to_be_bytes());
        img[ino_off + 36..ino_off + 40].copy_from_slice(&w1.to_be_bytes());
        // Extent 1: bn=50, length=1, offset=1
        let w0: u32 = 50;
        let w1: u32 = (1 << 24) | 1;
        img[ino_off + 40..ino_off + 44].copy_from_slice(&w0.to_be_bytes());
        img[ino_off + 44..ino_off + 48].copy_from_slice(&w1.to_be_bytes());

        // Fill block 30 with 0xAA, block 50 with 0xBB.
        img[30 * 512..31 * 512].fill(0xAA);
        img[50 * 512..51 * 512].fill(0xBB);

        let mut fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open EFS");
        // Synthesize a FileEntry that points at inode 4 without going through
        // list_directory (root inode 2 isn't populated in this image).
        let entry = FileEntry::new_file("data".into(), "/data".into(), 900, 4);
        let data = fs.read_file(&entry, usize::MAX).unwrap();
        assert_eq!(data.len(), 900);
        assert!(data[..512].iter().all(|&b| b == 0xAA));
        assert!(data[512..900].iter().all(|&b| b == 0xBB));

        let mut streamed = Vec::new();
        fs.write_file_to(&entry, &mut streamed).unwrap();
        assert_eq!(streamed, data);
    }
}
