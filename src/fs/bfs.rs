//! BFS — the Be File System (BeOS DR9 / PR / R3-R5, and Haiku).
//!
//! A 64-bit journaled extent filesystem with B+tree directories and inline
//! extended attributes. Volumes come in both byte orders: BeOS/Intel writes
//! little-endian, BeOS/PPC (and BeBox) big-endian. The `fs_byte_order` field is
//! the literal constant `'BIGE'` written in the volume's own order, so the raw
//! bytes at `magic1` decide which way to read everything else.
//!
//! Layout, all offsets relative to the partition start:
//! - block 0 — boot block (512 bytes) with the superblock at byte **512**,
//! - blocks `1 .. 1 + num_ags * blocks_per_ag` — the allocation bitmap
//!   (**set bit = allocated**, the opposite of the Amiga filesystems),
//! - `log_blocks` — the journal,
//! - everything else — inodes and file data, interleaved.
//!
//! A **`block_run`** is `{ allocation_group i32, start u16, len u16 }` and
//! resolves to block `(allocation_group << ag_shift) | start`. That number is
//! also a file's identity: an inode "number" in BFS *is* the block its
//! `bfs_inode` lives at.
//!
//! Documented in Dominic Giampaolo's *Practical File System Design*; the
//! struct offsets here were re-derived from the BeOS R5 and BeOS/PPC fixtures
//! and are listed in `src/fs/README.md` under "BFS on-disk offsets".
//!
//! Writing lives in [`crate::fs::bfs_write`].

use std::io::{Read, Seek, SeekFrom, Write};

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};
use super::unix_common::inode::{unix_file_type, UnixFileType};

/// `'BFS1'` — `magic1`, at byte 32 of the superblock.
pub const BFS_MAGIC1: u32 = 0x4246_5331;
/// `magic2`, at byte 68.
pub const BFS_MAGIC2: u32 = 0xdd12_1031;
/// `magic3`, at byte 112.
pub const BFS_MAGIC3: u32 = 0x15b6_830e;
/// `'BIGE'` — the byte-order marker, written in the volume's own order.
pub const BFS_BYTE_ORDER: u32 = 0x4249_4745;
/// `magic1` of a `bfs_inode`.
pub const BFS_INODE_MAGIC: u32 = 0x3bbe_0ad9;
/// `magic` of a `bplustree_header`.
pub const BPLUSTREE_MAGIC: u32 = 0x69f6_c2e8;

/// `'CLEN'` — the volume was unmounted cleanly.
pub const BFS_CLEAN: u32 = 0x434c_454e;
/// `'DIRT'` — mounted, or unmounted with a live journal.
pub const BFS_DIRTY: u32 = 0x4449_5254;

/// Where the superblock sits on an x86 volume — byte 512, past the boot block.
pub const SUPERBLOCK_OFFSET: u64 = 512;
/// BeOS/PPC has no PC boot block, so its superblock starts at byte 0. Haiku's
/// `Volume::Identify` probes 512 first and falls back here; so do we.
pub const SUPERBLOCK_OFFSET_PPC: u64 = 0;
/// Bytes of the superblock that carry fields (the rest is padding).
const SUPERBLOCK_SIZE: usize = 164;

/// `BPLUSTREE_NULL` — an absent link.
const BPLUSTREE_NULL: i64 = -1;
/// `bplustree_node`'s fixed header: three 8-byte links + two u16 counts.
const BPLUSTREE_NODE_HEADER: usize = 28;
/// `bplustree_header`'s size.
const BPLUSTREE_HEADER_SIZE: usize = 40;

/// Direct `block_run` slots in a `data_stream`.
const NUM_DIRECT_BLOCKS: usize = 12;
/// A `block_run` on disk.
const BLOCK_RUN_SIZE: usize = 8;

/// `INODE_LONG_SYMLINK` — the target lives in the data stream, not inline.
const INODE_LONG_SYMLINK: u32 = 0x0000_0040;
/// `INODE_ATTR_INODE` — this inode backs a single out-of-line attribute.
const INODE_ATTR_INODE: u32 = 0x0000_0004;

/// Offset of the `short_symlink` union arm inside a `bfs_inode`.
const INODE_OFF_DATA: usize = 0x48;
/// `data_stream` is 144 bytes, which is also `SHORT_SYMLINK_NAME_LENGTH`.
const DATA_STREAM_SIZE: usize = 144;
/// `small_data` entries begin here, after the stream and 16 bytes of padding.
const INODE_OFF_SMALL_DATA: usize = 0xE8;

/// `'CSTR'` — the `small_data` type holding a file's own name.
const FILE_NAME_TYPE: u32 = 0x4353_5452;
/// The one-byte `small_data` name under which the file name is filed.
const FILE_NAME_NAME: u8 = 0x13;

/// BFS timestamps are `seconds << 16 | uniquifier`.
const INODE_TIME_SHIFT: u32 = 16;

/// Refuse to materialise a directory listing beyond this many entries; a
/// corrupt `all_key_count` would otherwise allocate without bound.
const MAX_DIR_ENTRIES: usize = 500_000;

/// Byte order of a BFS volume — BeOS/Intel is little, BeOS/PPC big.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BfsEndian {
    Little,
    Big,
}

impl BfsEndian {
    fn u16(self, b: &[u8], off: usize) -> u16 {
        let v = [b[off], b[off + 1]];
        match self {
            BfsEndian::Little => u16::from_le_bytes(v),
            BfsEndian::Big => u16::from_be_bytes(v),
        }
    }
    fn u32(self, b: &[u8], off: usize) -> u32 {
        let v = [b[off], b[off + 1], b[off + 2], b[off + 3]];
        match self {
            BfsEndian::Little => u32::from_le_bytes(v),
            BfsEndian::Big => u32::from_be_bytes(v),
        }
    }
    fn i64(self, b: &[u8], off: usize) -> i64 {
        let mut v = [0u8; 8];
        v.copy_from_slice(&b[off..off + 8]);
        match self {
            BfsEndian::Little => i64::from_le_bytes(v),
            BfsEndian::Big => i64::from_be_bytes(v),
        }
    }
    pub(crate) fn put_u16(self, b: &mut [u8], off: usize, v: u16) {
        let bytes = match self {
            BfsEndian::Little => v.to_le_bytes(),
            BfsEndian::Big => v.to_be_bytes(),
        };
        b[off..off + 2].copy_from_slice(&bytes);
    }
    pub(crate) fn put_u32(self, b: &mut [u8], off: usize, v: u32) {
        let bytes = match self {
            BfsEndian::Little => v.to_le_bytes(),
            BfsEndian::Big => v.to_be_bytes(),
        };
        b[off..off + 4].copy_from_slice(&bytes);
    }
    pub(crate) fn put_i64(self, b: &mut [u8], off: usize, v: i64) {
        let bytes = match self {
            BfsEndian::Little => v.to_le_bytes(),
            BfsEndian::Big => v.to_be_bytes(),
        };
        b[off..off + 8].copy_from_slice(&bytes);
    }
    pub(crate) fn read_u16(self, b: &[u8], off: usize) -> u16 {
        self.u16(b, off)
    }
    pub(crate) fn read_u32(self, b: &[u8], off: usize) -> u32 {
        self.u32(b, off)
    }
    pub(crate) fn read_i64(self, b: &[u8], off: usize) -> i64 {
        self.i64(b, off)
    }
}

/// `{ allocation_group, start, len }` — BFS's extent primitive.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BlockRun {
    pub allocation_group: u32,
    pub start: u16,
    pub len: u16,
}

impl BlockRun {
    pub fn is_zero(&self) -> bool {
        self.allocation_group == 0 && self.start == 0 && self.len == 0
    }
    /// Resolve to an absolute block number.
    pub fn to_block(&self, ag_shift: u32) -> u64 {
        ((self.allocation_group as u64) << ag_shift) | self.start as u64
    }
    pub(crate) fn read(e: BfsEndian, b: &[u8], off: usize) -> Self {
        BlockRun {
            allocation_group: e.u32(b, off),
            start: e.u16(b, off + 4),
            len: e.u16(b, off + 6),
        }
    }
    pub(crate) fn write(&self, e: BfsEndian, b: &mut [u8], off: usize) {
        e.put_u32(b, off, self.allocation_group);
        e.put_u16(b, off + 4, self.start);
        e.put_u16(b, off + 6, self.len);
    }
}

/// A `data_stream` — 12 direct runs plus indirect and double-indirect levels.
#[derive(Debug, Clone, Copy, Default)]
pub struct DataStream {
    pub direct: [BlockRun; NUM_DIRECT_BLOCKS],
    pub max_direct_range: i64,
    pub indirect: BlockRun,
    pub max_indirect_range: i64,
    pub double_indirect: BlockRun,
    pub max_double_indirect_range: i64,
    pub size: i64,
}

impl DataStream {
    fn read(e: BfsEndian, b: &[u8], off: usize) -> Self {
        let mut direct = [BlockRun::default(); NUM_DIRECT_BLOCKS];
        for (i, slot) in direct.iter_mut().enumerate() {
            *slot = BlockRun::read(e, b, off + i * BLOCK_RUN_SIZE);
        }
        let base = off + NUM_DIRECT_BLOCKS * BLOCK_RUN_SIZE;
        DataStream {
            direct,
            max_direct_range: e.i64(b, base),
            indirect: BlockRun::read(e, b, base + 8),
            max_indirect_range: e.i64(b, base + 16),
            double_indirect: BlockRun::read(e, b, base + 24),
            max_double_indirect_range: e.i64(b, base + 32),
            size: e.i64(b, base + 40),
        }
    }

    pub(crate) fn write(&self, e: BfsEndian, b: &mut [u8], off: usize) {
        for (i, run) in self.direct.iter().enumerate() {
            run.write(e, b, off + i * BLOCK_RUN_SIZE);
        }
        let base = off + NUM_DIRECT_BLOCKS * BLOCK_RUN_SIZE;
        e.put_i64(b, base, self.max_direct_range);
        self.indirect.write(e, b, base + 8);
        e.put_i64(b, base + 16, self.max_indirect_range);
        self.double_indirect.write(e, b, base + 24);
        e.put_i64(b, base + 32, self.max_double_indirect_range);
        e.put_i64(b, base + 40, self.size);
    }
}

/// One inline attribute (`small_data`) as stored in the tail of an inode.
#[derive(Debug, Clone)]
pub struct SmallData {
    /// A BeOS type code (`'CSTR'`, `'RAWT'`, `'LONG'`, …).
    pub type_code: u32,
    pub name: Vec<u8>,
    pub data: Vec<u8>,
}

/// A parsed `bfs_inode`.
#[derive(Debug, Clone)]
pub struct BfsInode {
    /// Block the inode lives at — also its identity to the rest of the system.
    pub block: u64,
    pub inode_num: BlockRun,
    pub uid: u32,
    pub gid: u32,
    pub mode: u32,
    pub flags: u32,
    pub create_time: i64,
    pub last_modified_time: i64,
    pub parent: BlockRun,
    pub attributes: BlockRun,
    pub type_code: u32,
    pub inode_size: u32,
    pub data: DataStream,
    /// Inline symlink target — populated only when the mode says link and the
    /// long-symlink flag is clear, because these bytes overlay `data`.
    pub short_symlink: Vec<u8>,
    pub small_data: Vec<SmallData>,
}

impl BfsInode {
    /// The `'CSTR'` `small_data` under name `0x13` holds the file's own name.
    pub fn name(&self) -> Option<String> {
        self.small_data
            .iter()
            .find(|sd| sd.type_code == FILE_NAME_TYPE && sd.name == [FILE_NAME_NAME])
            .map(|sd| {
                String::from_utf8_lossy(&sd.data)
                    .trim_end_matches('\0')
                    .to_string()
            })
    }
    pub fn is_directory(&self) -> bool {
        matches!(unix_file_type(self.mode), UnixFileType::Directory)
    }
    pub fn is_symlink(&self) -> bool {
        matches!(unix_file_type(self.mode), UnixFileType::Symlink)
    }
    fn is_long_symlink(&self) -> bool {
        self.flags & INODE_LONG_SYMLINK != 0
    }
    /// True when the target sits in the dinode's union rather than in a data
    /// stream — in which case those 144 bytes are text, not `block_run`s.
    pub fn is_inline_symlink(&self) -> bool {
        self.is_symlink() && !self.is_long_symlink()
    }
}

/// A parsed BFS superblock.
#[derive(Debug, Clone)]
pub struct BfsSuperBlock {
    pub name: String,
    pub endian: BfsEndian,
    pub block_size: u32,
    pub block_shift: u32,
    pub num_blocks: i64,
    pub used_blocks: i64,
    pub inode_size: u32,
    pub blocks_per_ag: u32,
    pub ag_shift: u32,
    pub num_ags: u32,
    pub flags: u32,
    pub log_blocks: BlockRun,
    pub log_start: i64,
    pub log_end: i64,
    pub root_dir: BlockRun,
    pub indices: BlockRun,
}

impl BfsSuperBlock {
    /// Parse the 164-byte superblock, deciding byte order from `magic1`.
    pub fn parse(buf: &[u8]) -> Result<Self, FilesystemError> {
        if buf.len() < SUPERBLOCK_SIZE {
            return Err(FilesystemError::Parse(
                "bfs: superblock buffer too short".into(),
            ));
        }
        let raw_magic = [buf[32], buf[33], buf[34], buf[35]];
        let endian = if u32::from_be_bytes(raw_magic) == BFS_MAGIC1 {
            BfsEndian::Big
        } else if u32::from_le_bytes(raw_magic) == BFS_MAGIC1 {
            BfsEndian::Little
        } else {
            return Err(FilesystemError::Parse(
                "bfs: no BFS1 magic at superblock offset 32".into(),
            ));
        };

        if endian.u32(buf, 68) != BFS_MAGIC2 || endian.u32(buf, 112) != BFS_MAGIC3 {
            return Err(FilesystemError::Parse(
                "bfs: magic2/magic3 mismatch (superblock is not BFS)".into(),
            ));
        }
        if endian.u32(buf, 36) != BFS_BYTE_ORDER {
            return Err(FilesystemError::Parse(
                "bfs: fs_byte_order is not 'BIGE' in the detected order".into(),
            ));
        }

        let block_size = endian.u32(buf, 40);
        let block_shift = endian.u32(buf, 44);
        if block_shift > 20 || 1u32 << block_shift != block_size {
            return Err(FilesystemError::Parse(format!(
                "bfs: block_size {block_size} does not match block_shift {block_shift}"
            )));
        }
        let inode_size = endian.u32(buf, 64);
        if inode_size < 256 || inode_size > block_size.max(8192) {
            return Err(FilesystemError::Parse(format!(
                "bfs: implausible inode size {inode_size}"
            )));
        }
        let ag_shift = endian.u32(buf, 76);
        if ag_shift == 0 || ag_shift > 40 {
            return Err(FilesystemError::Parse(format!(
                "bfs: implausible ag_shift {ag_shift}"
            )));
        }
        let num_blocks = endian.i64(buf, 48);
        if num_blocks <= 0 {
            return Err(FilesystemError::Parse(format!(
                "bfs: num_blocks {num_blocks} is not positive"
            )));
        }

        let raw_name = &buf[..32];
        let end = raw_name.iter().position(|&c| c == 0).unwrap_or(32);

        Ok(BfsSuperBlock {
            name: String::from_utf8_lossy(&raw_name[..end]).trim().to_string(),
            endian,
            block_size,
            block_shift,
            num_blocks,
            used_blocks: endian.i64(buf, 56),
            inode_size,
            blocks_per_ag: endian.u32(buf, 72),
            ag_shift,
            num_ags: endian.u32(buf, 80),
            flags: endian.u32(buf, 84),
            log_blocks: BlockRun::read(endian, buf, 88),
            log_start: endian.i64(buf, 96),
            log_end: endian.i64(buf, 104),
            root_dir: BlockRun::read(endian, buf, 116),
            indices: BlockRun::read(endian, buf, 124),
        })
    }

    /// First block after the allocation bitmap.
    pub fn first_data_block(&self) -> u64 {
        1 + self.num_ags as u64 * self.blocks_per_ag as u64
    }

    /// True when the journal was flushed — the precondition for writing, since
    /// we do not maintain the log and BeOS would replay stale entries over us.
    pub fn log_is_empty(&self) -> bool {
        self.log_start == self.log_end
    }
}

/// A mounted BFS volume.
pub struct BfsFilesystem<R> {
    pub(crate) reader: R,
    pub(crate) partition_offset: u64,
    pub(crate) sb: BfsSuperBlock,
    /// Byte offset the superblock was found at — 512 on x86, 0 on PPC.
    pub(crate) sb_offset: u64,
    /// Set once a write has dirtied the superblock's counters.
    pub(crate) sb_dirty: bool,
}

impl<R: Read + Seek + Send> BfsFilesystem<R> {
    /// Probe for a BFS superblock at byte 512, then at byte 0 (BeOS/PPC).
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        let mut last_err = None;
        for at in [SUPERBLOCK_OFFSET, SUPERBLOCK_OFFSET_PPC] {
            reader.seek(SeekFrom::Start(partition_offset + at))?;
            let mut buf = [0u8; SUPERBLOCK_SIZE];
            if reader.read_exact(&mut buf).is_err() {
                continue;
            }
            match BfsSuperBlock::parse(&buf) {
                Ok(sb) => {
                    return Ok(BfsFilesystem {
                        reader,
                        partition_offset,
                        sb,
                        sb_offset: at,
                        sb_dirty: false,
                    })
                }
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or_else(|| {
            FilesystemError::Parse("bfs: no superblock at byte 512 or byte 0".into())
        }))
    }

    /// Does a BFS superblock live at byte 512 or byte 0 of this partition?
    pub fn detect(reader: &mut (impl Read + Seek), partition_offset: u64) -> Option<u64> {
        for at in [SUPERBLOCK_OFFSET, SUPERBLOCK_OFFSET_PPC] {
            if reader.seek(SeekFrom::Start(partition_offset + at)).is_err() {
                continue;
            }
            let mut buf = [0u8; SUPERBLOCK_SIZE];
            if reader.read_exact(&mut buf).is_ok() && BfsSuperBlock::parse(&buf).is_ok() {
                return Some(at);
            }
        }
        None
    }

    pub fn superblock(&self) -> &BfsSuperBlock {
        &self.sb
    }

    pub(crate) fn block_byte(&self, block: u64) -> u64 {
        self.partition_offset + block * self.sb.block_size as u64
    }

    pub(crate) fn read_blocks(
        &mut self,
        block: u64,
        count: u64,
    ) -> Result<Vec<u8>, FilesystemError> {
        let bs = self.sb.block_size as u64;
        if block.saturating_add(count) > self.sb.num_blocks as u64 {
            return Err(FilesystemError::Parse(format!(
                "bfs: block range {block}..{} past end of volume ({} blocks)",
                block + count,
                self.sb.num_blocks
            )));
        }
        self.reader.seek(SeekFrom::Start(self.block_byte(block)))?;
        let mut buf = vec![0u8; (count * bs) as usize];
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Read the `bfs_inode` living at `block`.
    pub fn read_inode(&mut self, block: u64) -> Result<BfsInode, FilesystemError> {
        let bs = self.sb.block_size as u64;
        let inode_blocks = (self.sb.inode_size as u64).div_ceil(bs).max(1);
        let buf = self.read_blocks(block, inode_blocks)?;
        self.parse_inode(block, &buf)
    }

    fn parse_inode(&self, block: u64, buf: &[u8]) -> Result<BfsInode, FilesystemError> {
        let e = self.sb.endian;
        if buf.len() < INODE_OFF_SMALL_DATA {
            return Err(FilesystemError::Parse("bfs: inode buffer too short".into()));
        }
        if e.u32(buf, 0) != BFS_INODE_MAGIC {
            return Err(FilesystemError::Parse(format!(
                "bfs: block {block} does not hold a bfs_inode (bad magic)"
            )));
        }
        let flags = e.u32(buf, 0x18);
        let mode = e.u32(buf, 0x14);
        let data = DataStream::read(e, buf, INODE_OFF_DATA);
        // The union arm is only a symlink target when the mode says so and the
        // long-symlink flag is clear; otherwise these bytes ARE the data
        // stream, and keeping a copy here would let a rewrite resurrect them
        // over the real one.
        let inline_link = matches!(unix_file_type(mode), UnixFileType::Symlink)
            && flags & INODE_LONG_SYMLINK == 0;
        let short_symlink = if inline_link {
            buf[INODE_OFF_DATA..INODE_OFF_DATA + DATA_STREAM_SIZE].to_vec()
        } else {
            Vec::new()
        };
        let inode_size = e.u32(buf, 0x40);
        let limit = (inode_size as usize).min(buf.len());

        Ok(BfsInode {
            block,
            inode_num: BlockRun::read(e, buf, 0x04),
            uid: e.u32(buf, 0x0C),
            gid: e.u32(buf, 0x10),
            mode,
            flags,
            create_time: e.i64(buf, 0x1C),
            last_modified_time: e.i64(buf, 0x24),
            parent: BlockRun::read(e, buf, 0x2C),
            attributes: BlockRun::read(e, buf, 0x34),
            type_code: e.u32(buf, 0x3C),
            inode_size,
            data,
            short_symlink,
            small_data: parse_small_data(e, &buf[..limit]),
        })
    }

    /// Every `(block, count)` extent the stream owns, in file order.
    pub fn stream_extents(&mut self, ds: &DataStream) -> Result<Vec<(u64, u64)>, FilesystemError> {
        let shift = self.sb.ag_shift;
        let mut out: Vec<(u64, u64)> = Vec::new();
        for run in &ds.direct {
            if run.len == 0 {
                continue;
            }
            out.push((run.to_block(shift), run.len as u64));
        }
        if ds.indirect.len > 0 {
            let raw = self.read_blocks(ds.indirect.to_block(shift), ds.indirect.len as u64)?;
            push_runs(self.sb.endian, shift, &raw, &mut out);
        }
        if ds.double_indirect.len > 0 {
            let level1 = self.read_blocks(
                ds.double_indirect.to_block(shift),
                ds.double_indirect.len as u64,
            )?;
            let mut indirects: Vec<(u64, u64)> = Vec::new();
            push_runs(self.sb.endian, shift, &level1, &mut indirects);
            for (blk, count) in indirects {
                let raw = self.read_blocks(blk, count)?;
                push_runs(self.sb.endian, shift, &raw, &mut out);
            }
        }
        Ok(out)
    }

    /// Read `len` bytes of a stream starting at `offset`.
    pub fn read_stream(
        &mut self,
        ds: &DataStream,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let mut out = Vec::with_capacity(len.min(1 << 20));
        self.stream_to(ds, offset, len as u64, &mut out)?;
        Ok(out)
    }

    /// Stream `len` bytes of `ds` from `offset` into `sink`.
    pub fn stream_to(
        &mut self,
        ds: &DataStream,
        offset: u64,
        len: u64,
        sink: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        let size = ds.size.max(0) as u64;
        if offset >= size {
            return Ok(0);
        }
        let want = len.min(size - offset);
        let bs = self.sb.block_size as u64;
        let extents = self.stream_extents(ds)?;

        let mut cursor = 0u64; // byte position of the current extent's start
        let mut written = 0u64;
        for (block, count) in extents {
            let extent_bytes = count * bs;
            let extent_end = cursor + extent_bytes;
            if extent_end <= offset {
                cursor = extent_end;
                continue;
            }
            if written >= want {
                break;
            }
            let skip = offset.saturating_sub(cursor);
            let avail = extent_bytes - skip;
            let take = avail.min(want - written);
            self.reader
                .seek(SeekFrom::Start(self.block_byte(block) + skip))?;
            let mut remaining = take;
            let mut chunk = vec![0u8; (256 * 1024).min(remaining.max(1)) as usize];
            while remaining > 0 {
                let n = (chunk.len() as u64).min(remaining) as usize;
                self.reader.read_exact(&mut chunk[..n])?;
                sink.write_all(&chunk[..n])?;
                remaining -= n as u64;
            }
            written += take;
            cursor = extent_end;
        }
        Ok(written)
    }

    /// Walk a directory's B+tree and return `(name, inode block)` pairs.
    pub fn read_directory(
        &mut self,
        inode: &BfsInode,
    ) -> Result<Vec<(String, u64)>, FilesystemError> {
        if !inode.is_directory() {
            return Err(FilesystemError::NotADirectory(format!(
                "bfs: inode at block {} is not a directory",
                inode.block
            )));
        }
        let tree = self.read_stream(&inode.data, 0, inode.data.size.max(0) as usize)?;
        let e = self.sb.endian;
        if tree.len() < BPLUSTREE_HEADER_SIZE || e.u32(&tree, 0) != BPLUSTREE_MAGIC {
            return Err(FilesystemError::Parse(format!(
                "bfs: directory at block {} has no B+tree header",
                inode.block
            )));
        }
        let node_size = e.u32(&tree, 4) as usize;
        if node_size == 0 || node_size > tree.len() {
            return Err(FilesystemError::Parse(format!(
                "bfs: B+tree node size {node_size} does not fit the directory stream"
            )));
        }
        let root = e.i64(&tree, 16);

        // Descend to the leftmost leaf, then follow `right_link` across the
        // leaf chain — a full tree walk without recursion.
        let mut node_off = root;
        let mut guard = 0usize;
        loop {
            let node = read_node(e, &tree, node_off, node_size)?;
            if node.overflow_link == BPLUSTREE_NULL {
                break;
            }
            node_off = if node.values.is_empty() {
                node.overflow_link
            } else {
                node.values[0]
            };
            guard += 1;
            if guard > 64 {
                return Err(FilesystemError::Parse(
                    "bfs: B+tree deeper than 64 levels (cycle?)".into(),
                ));
            }
        }

        let mut out: Vec<(String, u64)> = Vec::new();
        let mut visited = 0usize;
        loop {
            let node = read_node(e, &tree, node_off, node_size)?;
            for (name, value) in node.entries() {
                if name == "." || name == ".." {
                    continue;
                }
                if value <= 0 {
                    continue;
                }
                out.push((name, value as u64));
                if out.len() > MAX_DIR_ENTRIES {
                    return Err(FilesystemError::Parse(
                        "bfs: directory claims more entries than we will materialise".into(),
                    ));
                }
            }
            if node.right_link == BPLUSTREE_NULL {
                break;
            }
            node_off = node.right_link;
            visited += 1;
            if visited > tree.len() / node_size + 2 {
                return Err(FilesystemError::Parse(
                    "bfs: B+tree leaf chain loops".into(),
                ));
            }
        }
        Ok(out)
    }

    /// Resolve a symlink's target, inline or streamed.
    pub fn symlink_target(&mut self, inode: &BfsInode) -> Result<String, FilesystemError> {
        let raw = if inode.is_long_symlink() {
            self.read_stream(&inode.data, 0, inode.data.size.max(0) as usize)?
        } else {
            inode.short_symlink.clone()
        };
        let end = raw.iter().position(|&c| c == 0).unwrap_or(raw.len());
        Ok(String::from_utf8_lossy(&raw[..end]).into_owned())
    }

    /// Build the browse entry for one child inode.
    fn build_entry(
        &mut self,
        name: &str,
        parent: &FileEntry,
        inode: &BfsInode,
    ) -> Result<FileEntry, FilesystemError> {
        let parent_path = if parent.path == "/" {
            String::new()
        } else {
            parent.path.clone()
        };
        let path = format!("{parent_path}/{name}");
        let loc = inode.block;
        let mut entry = match unix_file_type(inode.mode) {
            UnixFileType::Directory => FileEntry::new_directory(name.to_string(), path, loc),
            UnixFileType::Symlink => {
                let target = self.symlink_target(inode)?;
                let len = target.len() as u64;
                FileEntry::new_symlink(name.to_string(), path, len, loc, target)
            }
            UnixFileType::Regular | UnixFileType::Unknown => {
                FileEntry::new_file(name.to_string(), path, inode.data.size.max(0) as u64, loc)
            }
            other => FileEntry::new_special(name.to_string(), path, loc, special_name(other)),
        };
        entry.mode = Some(inode.mode & 0xFFFF);
        entry.uid = Some(inode.uid);
        entry.gid = Some(inode.gid);
        let secs = (inode.last_modified_time >> INODE_TIME_SHIFT).max(0);
        if secs > 0 {
            entry.modified_unix = Some(secs as u64);
            entry.modified = Some(super::unix_common::inode::format_unix_timestamp(secs));
        }
        Ok(entry)
    }
}

/// Plain-ASCII name for a non-regular inode type, matching what the other
/// Unix drivers put in `FileEntry::special_type`.
fn special_name(ft: UnixFileType) -> String {
    match ft {
        UnixFileType::BlockDevice => "block device",
        UnixFileType::CharDevice => "char device",
        UnixFileType::Fifo => "fifo",
        UnixFileType::Socket => "socket",
        _ => "special",
    }
    .to_string()
}

/// Decode a block of `block_run`s into `(block, count)` pairs.
fn push_runs(e: BfsEndian, ag_shift: u32, raw: &[u8], out: &mut Vec<(u64, u64)>) {
    for off in (0..raw.len()).step_by(BLOCK_RUN_SIZE) {
        if off + BLOCK_RUN_SIZE > raw.len() {
            break;
        }
        let run = BlockRun::read(e, raw, off);
        if run.len == 0 {
            continue;
        }
        out.push((run.to_block(ag_shift), run.len as u64));
    }
}

/// One decoded `bplustree_node`.
struct BpNode {
    right_link: i64,
    overflow_link: i64,
    keys: Vec<String>,
    values: Vec<i64>,
}

impl BpNode {
    fn entries(&self) -> Vec<(String, i64)> {
        self.keys
            .iter()
            .cloned()
            .zip(self.values.iter().copied())
            .collect()
    }
}

/// Byte offsets inside a node: keys sit right after the 28-byte header, the
/// key-length array starts at the next 8-byte boundary *from the node start*,
/// and the value array follows it unpadded.
pub(crate) fn key_lengths_offset(all_key_length: usize) -> usize {
    (BPLUSTREE_NODE_HEADER + all_key_length).next_multiple_of(8)
}

pub(crate) fn values_offset(all_key_length: usize, all_key_count: usize) -> usize {
    key_lengths_offset(all_key_length) + all_key_count * 2
}

fn read_node(
    e: BfsEndian,
    tree: &[u8],
    offset: i64,
    node_size: usize,
) -> Result<BpNode, FilesystemError> {
    if offset < 0 {
        return Err(FilesystemError::Parse(
            "bfs: B+tree node pointer is null".into(),
        ));
    }
    let start = offset as usize;
    let end = start
        .checked_add(node_size)
        .ok_or_else(|| FilesystemError::Parse("bfs: B+tree node offset overflow".into()))?;
    if end > tree.len() {
        return Err(FilesystemError::Parse(format!(
            "bfs: B+tree node at {start} runs past the {}-byte directory stream",
            tree.len()
        )));
    }
    let n = &tree[start..end];
    let all_key_count = e.u16(n, 24) as usize;
    let all_key_length = e.u16(n, 26) as usize;

    let values_off = values_offset(all_key_length, all_key_count);
    if all_key_count == 0 {
        return Ok(BpNode {
            right_link: e.i64(n, 8),
            overflow_link: e.i64(n, 16),
            keys: Vec::new(),
            values: Vec::new(),
        });
    }
    if BPLUSTREE_NODE_HEADER + all_key_length > node_size
        || values_off + all_key_count * 8 > node_size
    {
        return Err(FilesystemError::Parse(format!(
            "bfs: B+tree node claims {all_key_count} keys / {all_key_length} key bytes, \
             which does not fit a {node_size}-byte node"
        )));
    }

    let lengths_off = key_lengths_offset(all_key_length);
    let mut keys = Vec::with_capacity(all_key_count);
    let mut values = Vec::with_capacity(all_key_count);
    let mut prev = 0usize;
    for i in 0..all_key_count {
        let end_off = e.u16(n, lengths_off + i * 2) as usize;
        if end_off < prev || end_off > all_key_length {
            return Err(FilesystemError::Parse(format!(
                "bfs: B+tree key {i} ends at {end_off}, outside [{prev}, {all_key_length}]"
            )));
        }
        let key = &n[BPLUSTREE_NODE_HEADER + prev..BPLUSTREE_NODE_HEADER + end_off];
        keys.push(String::from_utf8_lossy(key).into_owned());
        values.push(e.i64(n, values_off + i * 8));
        prev = end_off;
    }
    Ok(BpNode {
        right_link: e.i64(n, 8),
        overflow_link: e.i64(n, 16),
        keys,
        values,
    })
}

/// Walk the `small_data` chain in an inode's tail.
fn parse_small_data(e: BfsEndian, buf: &[u8]) -> Vec<SmallData> {
    let mut out = Vec::new();
    let mut off = INODE_OFF_SMALL_DATA;
    while off + 8 <= buf.len() {
        let type_code = e.u32(buf, off);
        let name_size = e.u16(buf, off + 4) as usize;
        let data_size = e.u16(buf, off + 6) as usize;
        if name_size == 0 {
            break;
        }
        // Layout is `hdr(8) name[name_size] pad(3) data[data_size] NUL`.
        let name_at = off + 8;
        let data_at = name_at + name_size + 3;
        let next = data_at + data_size + 1;
        if next > buf.len() {
            break;
        }
        out.push(SmallData {
            type_code,
            name: buf[name_at..name_at + name_size].to_vec(),
            data: buf[data_at..data_at + data_size].to_vec(),
        });
        off = next;
    }
    out
}

impl<R: Read + Seek + Send> Filesystem for BfsFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        let block = self.sb.root_dir.to_block(self.sb.ag_shift);
        let mut entry = FileEntry::root();
        entry.location = block;
        Ok(entry)
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        let dir = self.read_inode(entry.location)?;
        let children = self.read_directory(&dir)?;
        let mut out = Vec::with_capacity(children.len());
        for (name, block) in children {
            // A dangling B+tree value is a corrupt directory, not a fatal
            // volume error — skip it so the rest of the listing survives.
            let child = match self.read_inode(block) {
                Ok(i) => i,
                Err(_) => continue,
            };
            if child.flags & INODE_ATTR_INODE != 0 {
                continue;
            }
            out.push(self.build_entry(&name, entry, &child)?);
        }
        Ok(out)
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let inode = self.read_inode(entry.location)?;
        if inode.is_symlink() {
            return Ok(self.symlink_target(&inode)?.into_bytes());
        }
        let want = (inode.data.size.max(0) as usize).min(max_bytes);
        self.read_stream(&inode.data, 0, want)
    }

    fn write_file_to(
        &mut self,
        entry: &FileEntry,
        writer: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        let inode = self.read_inode(entry.location)?;
        if inode.is_symlink() {
            let target = self.symlink_target(&inode)?;
            writer.write_all(target.as_bytes())?;
            return Ok(target.len() as u64);
        }
        let size = inode.data.size.max(0) as u64;
        self.stream_to(&inode.data, 0, size, writer)
    }

    fn volume_label(&self) -> Option<&str> {
        if self.sb.name.is_empty() {
            None
        } else {
            Some(&self.sb.name)
        }
    }

    fn fs_type(&self) -> &str {
        "BFS"
    }

    fn total_size(&self) -> u64 {
        self.sb.num_blocks as u64 * self.sb.block_size as u64
    }

    fn used_size(&self) -> u64 {
        self.sb.used_blocks.max(0) as u64 * self.sb.block_size as u64
    }

    fn allocation_unit(&self) -> Option<u64> {
        Some(self.sb.block_size as u64)
    }

    fn validate_name(&self, name: &str) -> Result<(), FilesystemError> {
        validate_bfs_name(name)
    }

    fn fsck(&mut self) -> Option<Result<super::fsck::FsckResult, FilesystemError>> {
        Some(self.fsck_bfs())
    }
}

/// BFS names are bytes with no `/`, capped by `B_FILE_NAME_LENGTH`.
pub(crate) fn validate_bfs_name(name: &str) -> Result<(), FilesystemError> {
    if name.is_empty() {
        return Err(FilesystemError::InvalidData("name cannot be empty".into()));
    }
    if name.len() > 255 {
        return Err(FilesystemError::InvalidData(format!(
            "bfs: name is {} bytes, over the 255-byte limit",
            name.len()
        )));
    }
    if name.contains('/') {
        return Err(FilesystemError::InvalidData(
            "bfs: '/' cannot appear in a name".into(),
        ));
    }
    if name == "." || name == ".." {
        return Err(FilesystemError::InvalidData(
            "bfs: '.' and '..' are reserved".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn le_sb() -> Vec<u8> {
        let mut b = vec![0u8; SUPERBLOCK_SIZE];
        b[..5].copy_from_slice(b"Be HD");
        let e = BfsEndian::Little;
        e.put_u32(&mut b, 32, BFS_MAGIC1);
        e.put_u32(&mut b, 36, BFS_BYTE_ORDER);
        e.put_u32(&mut b, 40, 1024);
        e.put_u32(&mut b, 44, 10);
        e.put_i64(&mut b, 48, 1_584_576);
        e.put_i64(&mut b, 56, 1_345_367);
        e.put_u32(&mut b, 64, 1024);
        e.put_u32(&mut b, 68, BFS_MAGIC2);
        e.put_u32(&mut b, 72, 1);
        e.put_u32(&mut b, 76, 13);
        e.put_u32(&mut b, 80, 194);
        e.put_u32(&mut b, 84, BFS_CLEAN);
        e.put_u32(&mut b, 112, BFS_MAGIC3);
        BlockRun {
            allocation_group: 8,
            start: 0,
            len: 1,
        }
        .write(e, &mut b, 116);
        b
    }

    #[test]
    fn parses_a_little_endian_superblock() {
        let sb = BfsSuperBlock::parse(&le_sb()).unwrap();
        assert_eq!(sb.endian, BfsEndian::Little);
        assert_eq!(sb.block_size, 1024);
        assert_eq!(sb.num_ags, 194);
        assert_eq!(sb.root_dir.to_block(sb.ag_shift), 65536);
        assert_eq!(sb.first_data_block(), 195);
        assert!(sb.log_is_empty());
    }

    #[test]
    fn parses_a_big_endian_superblock() {
        // Byte-swap every field of the LE fixture and confirm the same values
        // come back — this is the BeOS/PPC case.
        let mut b = vec![0u8; SUPERBLOCK_SIZE];
        b[..5].copy_from_slice(b"Be AA");
        let e = BfsEndian::Big;
        e.put_u32(&mut b, 32, BFS_MAGIC1);
        e.put_u32(&mut b, 36, BFS_BYTE_ORDER);
        e.put_u32(&mut b, 40, 1024);
        e.put_u32(&mut b, 44, 10);
        e.put_i64(&mut b, 48, 409_779);
        e.put_u32(&mut b, 64, 1024);
        e.put_u32(&mut b, 68, BFS_MAGIC2);
        e.put_u32(&mut b, 72, 1);
        e.put_u32(&mut b, 76, 13);
        e.put_u32(&mut b, 80, 50);
        e.put_u32(&mut b, 112, BFS_MAGIC3);
        let sb = BfsSuperBlock::parse(&b).unwrap();
        assert_eq!(sb.endian, BfsEndian::Big);
        assert_eq!(sb.block_size, 1024);
        assert_eq!(sb.num_ags, 50);
    }

    #[test]
    fn a_mismatched_block_shift_is_rejected() {
        let mut b = le_sb();
        BfsEndian::Little.put_u32(&mut b, 44, 11);
        assert!(BfsSuperBlock::parse(&b).is_err());
    }

    #[test]
    fn block_run_resolves_through_the_ag_shift() {
        let run = BlockRun {
            allocation_group: 8,
            start: 1,
            len: 2,
        };
        assert_eq!(run.to_block(13), 65537);
    }

    /// The layout the BeOS R5 fixture's root directory node actually has:
    /// 13 keys, 88 key bytes, lengths at 120, values at 146.
    #[test]
    fn node_array_offsets_match_the_r5_fixture() {
        assert_eq!(key_lengths_offset(88), 120);
        assert_eq!(values_offset(88, 13), 146);
    }

    #[test]
    fn small_data_chain_walks_name_then_data() {
        let e = BfsEndian::Little;
        let mut buf = vec![0u8; 512];
        let off = INODE_OFF_SMALL_DATA;
        e.put_u32(&mut buf, off, FILE_NAME_TYPE);
        e.put_u16(&mut buf, off + 4, 1);
        e.put_u16(&mut buf, off + 6, 5);
        buf[off + 8] = FILE_NAME_NAME;
        buf[off + 12..off + 17].copy_from_slice(b"hello");
        let sd = parse_small_data(e, &buf);
        assert_eq!(sd.len(), 1);
        assert_eq!(sd[0].data, b"hello");
    }
}
