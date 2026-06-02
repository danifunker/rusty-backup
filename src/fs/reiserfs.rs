//! ReiserFS v3.5 / v3.6 — Tier-A read support + on-disk tree structures.
//!
//! Implements the [`Filesystem`] trait far enough to detect a ReiserFS
//! partition, surface its on-disk magic, label, and total/used/free sizes in
//! the inspect tab, and back the partition up byte-for-byte through the
//! existing layout-preserving pipeline. R.3a adds the S+tree on-disk
//! parsers ([`BlockHead`], [`Key`], [`ItemHead`], [`DiskChild`]); the
//! actual tree walker + `list_directory` / `read_file` follow in R.3b–d.
//!
//! Scope (from R.1 of the plan):
//!  * **v3.5** (`ReIsErFs` magic) and **v3.6** (`ReIsEr2Fs` magic) only.
//!  * **reiser4** (`ReIsEr4` magic) is rejected with a clear error — wholly
//!    different on-disk format, effectively extinct.
//!  * Read-only. No edit, no fsck, no resize.
//!
//! Reference: `~/Downloads/partimage-0.6.9/src/client/fs/fs_reiser.{cpp,h}`
//! for Tier-A superblock + bitmap; Linux kernel `fs/reiserfs/reiserfs_fs.h`
//! for the authoritative on-disk struct layout below.
//!
//! # Superblock layout (V1 portion, offsets all little-endian)
//!
//! ```text
//!  0  s_block_count        u32   total blocks in the filesystem
//!  4  s_free_blocks        u32   free blocks
//!  8  s_root_block         u32   logical block number of the S+tree root
//! 12  s_journal_params     [u32; 8]   32 bytes of journal parameters
//! 44  s_blocksize          u16   filesystem block size (1024, 2048, 4096, 8192)
//! 46  s_oid_maxsize        u16
//! 48  s_oid_cursize        u16
//! 50  s_umount_state       u16   1 = clean, 2 = dirty
//! 52  s_magic[10]          char  one of: "ReIsErFs\0\0", "ReIsEr2Fs\0", "ReIsEr4\0\0\0"
//! 62  s_fs_state           u16
//! 64  s_hash_function_code u32
//! 68  s_tree_height        u16
//! 70  s_bmap_nr            u16   number of bitmap blocks (0 means "compute from block count")
//! 72  s_version            u16   on-disk version: 0 = v3.5, 2 = v3.6, others = unknown
//! 74  s_reserved           u16
//! ```
//!
//! V2 extension (only valid when `s_version == 2`, i.e. v3.6):
//!
//! ```text
//! 76  s_inode_generation   u32
//! 80  s_flags              u32
//! 84  s_uuid[16]
//! 100 s_label[16]                 null-padded volume label
//! ```

use std::io::{Read, Seek, SeekFrom};

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};
use super::unix_common::bitmap::BitmapReader;
use super::unix_common::compact::{CompactLayout, CompactSection, CompactStreamReader};
use crate::fs::CompactResult;

// ---- Constants ----

/// ReiserFS superblock lives 64 KiB into the partition.
const REISERFS_SUPERBLOCK_OFFSET: u64 = 65536;

/// Magic-string offset within the superblock.
const MAGIC_OFFSET: usize = 52;
const MAGIC_LEN: usize = 10;

/// v3.5 magic (8 bytes + 2 NULs).
const MAGIC_V3_5: &[u8] = b"ReIsErFs";
/// v3.6 magic (9 bytes + 1 NUL).
const MAGIC_V3_6: &[u8] = b"ReIsEr2Fs";
/// reiser4 magic — rejected, structurally unrelated.
const MAGIC_REISER4: &[u8] = b"ReIsEr4";

/// We read the V1 superblock (76 bytes) plus the V2 extension up to the
/// 16-byte label at offset 116. One sector-sized read covers both.
const SUPERBLOCK_READ_SIZE: usize = 512;

// ---- Version ----

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReiserFsVersion {
    V3_5,
    V3_6,
}

impl ReiserFsVersion {
    fn name(self) -> &'static str {
        match self {
            ReiserFsVersion::V3_5 => "ReiserFS 3.5",
            ReiserFsVersion::V3_6 => "ReiserFS 3.6",
        }
    }
}

// ---- Filesystem ----

pub struct ReiserFsFilesystem<R> {
    reader: R,
    partition_offset: u64,
    version: ReiserFsVersion,
    block_size: u64,
    total_blocks: u64,
    free_blocks: u64,
    bmap_nr: u32,
    label: Option<String>,
    /// Logical block number of the S+tree root, read from superblock offset 8.
    /// R.3 will use this to descend into the catalog.
    #[allow(dead_code)]
    root_block: u32,
}

impl<R: Read + Seek + Send> ReiserFsFilesystem<R> {
    /// Open a ReiserFS v3.5 or v3.6 filesystem at the given partition offset.
    /// Rejects reiser4 and unknown variants with a clear error.
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        reader.seek(SeekFrom::Start(
            partition_offset + REISERFS_SUPERBLOCK_OFFSET,
        ))?;
        let mut sb = [0u8; SUPERBLOCK_READ_SIZE];
        reader.read_exact(&mut sb)?;

        // Magic dispatch first — everything else is meaningless if this fails.
        let magic = &sb[MAGIC_OFFSET..MAGIC_OFFSET + MAGIC_LEN];
        let version = if magic.starts_with(MAGIC_V3_6) {
            ReiserFsVersion::V3_6
        } else if magic.starts_with(MAGIC_V3_5) {
            ReiserFsVersion::V3_5
        } else if magic.starts_with(MAGIC_REISER4) {
            return Err(FilesystemError::Unsupported(
                "reiser4 on-disk format is not supported (ReiserFS 3.5 / 3.6 only)".into(),
            ));
        } else {
            return Err(FilesystemError::Parse(
                "reiserfs: invalid superblock magic at offset 65536+52".into(),
            ));
        };

        let block_count = le32(&sb, 0x00) as u64;
        let free_blocks = le32(&sb, 0x04) as u64;
        let root_block = le32(&sb, 0x08);
        let block_size = u16::from_le_bytes([sb[0x2C], sb[0x2D]]) as u64;

        // Block size sanity. ReiserFS supports 1024, 2048, 4096, 8192; reject
        // anything else as a corrupt/non-ReiserFS image rather than crashing
        // on multiplication overflow downstream.
        if !matches!(block_size, 1024 | 2048 | 4096 | 8192) {
            return Err(FilesystemError::Parse(format!(
                "reiserfs: invalid block size {block_size} (expected 1024/2048/4096/8192)"
            )));
        }
        if block_count == 0 {
            return Err(FilesystemError::Parse("reiserfs: block_count is 0".into()));
        }
        if free_blocks > block_count {
            return Err(FilesystemError::Parse(format!(
                "reiserfs: free_blocks ({free_blocks}) exceeds block_count ({block_count})"
            )));
        }

        // s_bmap_nr at offset 70: number of allocation-bitmap blocks. Stored
        // as u16 on disk so it tops out at 65535; modern ReiserFS uses 0 as a
        // sentinel meaning "compute from block_count". One bitmap block
        // covers `block_size * 8` data blocks.
        let bmap_nr_disk = u16::from_le_bytes([sb[0x46], sb[0x47]]) as u32;
        let bmap_nr = if bmap_nr_disk == 0 {
            let blocks_per_bitmap = block_size * 8;
            block_count.div_ceil(blocks_per_bitmap) as u32
        } else {
            bmap_nr_disk
        };

        // Volume label: only present on v3.6 (lives in the V2 extension at
        // offset 100). v3.5 has no label field; return None.
        let label = if version == ReiserFsVersion::V3_6 {
            let label_bytes = &sb[100..116];
            let end = label_bytes.iter().position(|&b| b == 0).unwrap_or(16);
            if end > 0 {
                let s = String::from_utf8_lossy(&label_bytes[..end])
                    .trim()
                    .to_string();
                if s.is_empty() {
                    None
                } else {
                    Some(s)
                }
            } else {
                None
            }
        } else {
            None
        };

        Ok(Self {
            reader,
            partition_offset,
            version,
            block_size,
            total_blocks: block_count,
            free_blocks,
            bmap_nr,
            label,
            root_block,
        })
    }

    /// Return the on-disk version (3.5 or 3.6).
    pub fn version(&self) -> ReiserFsVersion {
        self.version
    }

    /// Block where the superblock lives. Superblock byte offset is always
    /// 65536, so `sb_block = 65536 / block_size`.
    fn sb_block(&self) -> u64 {
        REISERFS_SUPERBLOCK_OFFSET / self.block_size
    }

    /// Read the bitmap block whose index in the bitmap chain is `i`.
    ///
    /// ReiserFS places bitmap 0 immediately after the superblock (so its
    /// physical block number is `sb_block + 1`); bitmap `i >= 1` lives at
    /// block `i * (block_size * 8)` (the first block of the i-th
    /// `blocks_per_bitmap` group). Each bitmap covers `block_size * 8`
    /// consecutive volume blocks starting at block `i * blocks_per_bitmap`.
    fn read_bitmap_block(&mut self, i: u32) -> Result<Vec<u8>, FilesystemError> {
        let blocks_per_bitmap = self.block_size * 8;
        let phys_block = if i == 0 {
            self.sb_block() + 1
        } else {
            i as u64 * blocks_per_bitmap
        };
        self.reader.seek(SeekFrom::Start(
            self.partition_offset + phys_block * self.block_size,
        ))?;
        let mut buf = vec![0u8; self.block_size as usize];
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Number of valid bits in bitmap `i`.
    ///
    /// Every bitmap except the last covers exactly `block_size * 8` blocks
    /// (a full bitmap). The last bitmap covers the trailing remainder.
    fn bitmap_valid_bits(&self, i: u32) -> u64 {
        let blocks_per_bitmap = self.block_size * 8;
        let logical_start = i as u64 * blocks_per_bitmap;
        if i == self.bmap_nr - 1 {
            self.total_blocks.saturating_sub(logical_start)
        } else {
            blocks_per_bitmap
        }
    }
}

impl<R: Read + Seek + Send> Filesystem for ReiserFsFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Err(FilesystemError::Unsupported(
            "ReiserFS browse not yet implemented (see OPEN-WORK.md §1.1 R.3)".into(),
        ))
    }

    fn list_directory(&mut self, _entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        Err(FilesystemError::Unsupported(
            "ReiserFS browse not yet implemented (see OPEN-WORK.md §1.1 R.3)".into(),
        ))
    }

    fn read_file(
        &mut self,
        _entry: &FileEntry,
        _max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        Err(FilesystemError::Unsupported(
            "ReiserFS read not yet implemented (see OPEN-WORK.md §1.1 R.3)".into(),
        ))
    }

    fn volume_label(&self) -> Option<&str> {
        self.label.as_deref()
    }

    fn fs_type(&self) -> &str {
        self.version.name()
    }

    fn total_size(&self) -> u64 {
        self.total_blocks * self.block_size
    }

    fn used_size(&self) -> u64 {
        self.total_blocks.saturating_sub(self.free_blocks) * self.block_size
    }

    /// Walk the bitmap chain from the end and return the byte offset just
    /// past the highest allocated block. ReiserFS uses **set bit =
    /// allocated**, the standard Linux convention (verified against
    /// `partimage-0.6.9/src/client/fs/fs_reiser.cpp`). Bitmap blocks
    /// themselves are always marked allocated by the on-disk image, so a
    /// non-zero answer is guaranteed for a valid volume.
    fn last_data_byte(&mut self) -> Result<u64, FilesystemError> {
        // Scan bitmaps from the last to the first to short-circuit on the
        // first hit. `bmap_nr` is bounded at open-time (computed from
        // block_count for the sentinel-zero case), so it's always sane here.
        for i in (0..self.bmap_nr).rev() {
            let bitmap = self.read_bitmap_block(i)?;
            let valid_bits = self.bitmap_valid_bits(i);
            let bm = BitmapReader::new(&bitmap, valid_bits);
            if let Some(highest) = bm.highest_set_bit() {
                let absolute = i as u64 * (self.block_size * 8) + highest;
                return Ok((absolute + 1) * self.block_size);
            }
        }
        // No allocated blocks anywhere — degenerate but possible on a
        // freshly-mkfs'd-then-emptied volume. Return total_size to match
        // the trait default behavior.
        Ok(self.total_blocks * self.block_size)
    }
}

// ---- Compact reader ----

/// A streaming `Read` that produces a compacted ReiserFS partition image.
///
/// **Layout-preserving** — allocated blocks are read from their original
/// positions, unallocated blocks are emitted as zeros. Block addresses
/// inside the volume's S+tree therefore remain valid against the stream,
/// which is what the layout-preserving backup pipeline needs.
///
/// The compaction win comes from the trailing zero-fill: free space in
/// the middle of the volume compresses near-perfectly under zstd / CHD,
/// and `last_data_byte()`-driven trimming drops the tail after the last
/// allocated block entirely.
pub struct CompactReiserFsReader<R: Read + Seek> {
    inner: CompactStreamReader<R>,
}

impl<R: Read + Seek + Send> CompactReiserFsReader<R> {
    /// Create a new compacted ReiserFS reader.
    ///
    /// Parses the superblock, walks every bitmap block, and builds a
    /// section list with one entry per allocated/free run.
    pub fn new(
        mut reader: R,
        partition_offset: u64,
    ) -> Result<(Self, CompactResult), FilesystemError> {
        // Re-open the volume via the regular parser so we share validation
        // (magic, block size, label, bmap_nr fallback) with the read path.
        let mut fs = ReiserFsFilesystem::open(reader, partition_offset)?;
        let block_size = fs.block_size;
        let total_blocks = fs.total_blocks;
        let blocks_per_bitmap = block_size * 8;

        let mut sections: Vec<CompactSection> = Vec::new();
        let mut total_allocated: u64 = 0;
        let mut next_logical_block: u64 = 0;

        for i in 0..fs.bmap_nr {
            let bitmap = fs.read_bitmap_block(i)?;
            let valid_bits = fs.bitmap_valid_bits(i);
            if valid_bits == 0 {
                continue;
            }
            let bm = BitmapReader::new(&bitmap, valid_bits);
            let group_start = i as u64 * blocks_per_bitmap;

            // Coalesce consecutive same-state runs into single sections.
            let mut bit: u64 = 0;
            while bit < valid_bits {
                let is_set = bm.is_bit_set(bit);
                let mut run_end = bit + 1;
                while run_end < valid_bits && bm.is_bit_set(run_end) == is_set {
                    run_end += 1;
                }
                let run_len = run_end - bit;

                if is_set {
                    let old_blocks: Vec<u64> = (bit..run_end).map(|b| group_start + b).collect();
                    total_allocated += run_len;
                    sections.push(CompactSection::MappedBlocks { old_blocks });
                } else {
                    sections.push(CompactSection::Zeros(run_len * block_size));
                }
                bit = run_end;
            }

            next_logical_block = group_start + valid_bits;
        }

        // Trailing gap if total_blocks > sum of bitmap coverage (the on-disk
        // sentinel-zero bmap_nr branch should already round up to cover the
        // full volume, but emit an explicit zero region defensively).
        if next_logical_block < total_blocks {
            sections.push(CompactSection::Zeros(
                (total_blocks - next_logical_block) * block_size,
            ));
        }

        reader = fs.reader;
        let original_size = total_blocks * block_size;
        let layout = CompactLayout {
            sections,
            block_size: block_size as usize,
            source_data_start: 0,
            source_partition_offset: partition_offset,
        };
        let inner = CompactStreamReader::new(reader, layout);
        Ok((
            Self { inner },
            CompactResult {
                original_size,
                compacted_size: original_size,
                data_size: total_allocated * block_size,
                clusters_used: total_allocated as u32,
            },
        ))
    }
}

impl<R: Read + Seek> Read for CompactReiserFsReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

// ---- On-disk S+tree structures (R.3a) ----
//
// The reader half of the S+tree. Every metadata block in the volume is one
// of these:
//
//   Internal node (level >= 1): block_head + N keys + (N+1) DiskChild ptrs.
//                               Used to navigate to the right leaf for a key.
//   Leaf node    (level == 0): block_head + N item_heads + item-data area.
//                               Item data grows backwards from the end of
//                               the block; each ItemHead carries its data's
//                               byte offset (`item_location`) and length.
//
// All numeric fields are little-endian. Sources:
//   * Linux kernel `fs/reiserfs/reiserfs_fs.h` (`block_head`, `reiserfs_key`,
//     `item_head`, `disk_child`).
//   * `~/Downloads/reiserfsprogs/reiserfscore` for cross-checks.

/// Bytes occupied by the [`BlockHead`] at offset 0 of every tree node.
pub const BLOCK_HEAD_SIZE: usize = 24;
/// Bytes occupied by a [`Key`] on disk.
pub const KEY_SIZE: usize = 16;
/// Bytes occupied by an [`ItemHead`] in a leaf node.
pub const ITEM_HEAD_SIZE: usize = 24;
/// Bytes occupied by a [`DiskChild`] in an internal node.
pub const DISK_CHILD_SIZE: usize = 8;

/// Level value that identifies a leaf node.
pub const LEAF_LEVEL: u16 = 0;

/// Header at offset 0 of every tree node.
///
/// `level == 0` means a leaf; non-zero means an internal node whose children
/// live `level - 1` steps closer to the leaves. `nr_item` is the number of
/// keys (internal) or item heads (leaf) immediately following this struct.
/// `right_delim_key` is the boundary key between this subtree and its
/// right neighbor in the parent — present in every node but only meaningful
/// during inserts; readers can ignore it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockHead {
    pub level: u16,
    pub nr_item: u16,
    pub free_space: u16,
    pub right_delim_key: Key,
}

impl BlockHead {
    /// Parse a block head from the first 24 bytes of a tree node.
    pub fn parse(block: &[u8]) -> Result<Self, FilesystemError> {
        if block.len() < BLOCK_HEAD_SIZE {
            return Err(FilesystemError::Parse(format!(
                "reiserfs block head: need {BLOCK_HEAD_SIZE} bytes, got {}",
                block.len()
            )));
        }
        let level = u16::from_le_bytes([block[0], block[1]]);
        let nr_item = u16::from_le_bytes([block[2], block[3]]);
        let free_space = u16::from_le_bytes([block[4], block[5]]);
        // block[6..8] is the reserved padding field; skip it.
        let right_delim_key = Key::parse_raw(&block[8..24]);
        Ok(Self {
            level,
            nr_item,
            free_space,
            right_delim_key,
        })
    }

    /// Returns true when this node is a leaf (holds item heads + item data).
    pub fn is_leaf(&self) -> bool {
        self.level == LEAF_LEVEL
    }
}

/// A 16-byte ReiserFS key.
///
/// Stored verbatim — interpretation depends on the key format (v3.5
/// "short" / v3.6 "long"), which is recorded per-leaf-item in
/// [`ItemHead::version`]. The first two u32 fields are common (`dir_id`,
/// `objectid`); the last 8 bytes are either two u32s (offset + type) or one
/// packed u64 (60 bits offset + 4 bits type).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Key {
    pub raw: [u8; KEY_SIZE],
}

impl Key {
    /// Parse the 16-byte key from a byte slice. Trailing bytes are ignored.
    pub fn parse_raw(buf: &[u8]) -> Self {
        let mut raw = [0u8; KEY_SIZE];
        let n = buf.len().min(KEY_SIZE);
        raw[..n].copy_from_slice(&buf[..n]);
        Self { raw }
    }

    /// Directory ID (parent objectid). Common between v1 and v2 keys.
    pub fn dir_id(&self) -> u32 {
        u32::from_le_bytes([self.raw[0], self.raw[1], self.raw[2], self.raw[3]])
    }

    /// Object ID. Common between v1 and v2 keys.
    pub fn objectid(&self) -> u32 {
        u32::from_le_bytes([self.raw[4], self.raw[5], self.raw[6], self.raw[7]])
    }

    /// Offset within the object, interpreted for the given key format.
    ///
    /// v1 keys store a 32-bit offset at bytes 8..12.
    /// v2 keys pack `offset << 4 | type` into the u64 at bytes 8..16, so
    /// the offset occupies the low 60 bits.
    pub fn offset(&self, format: KeyFormat) -> u64 {
        match format {
            KeyFormat::V1 => {
                u32::from_le_bytes([self.raw[8], self.raw[9], self.raw[10], self.raw[11]]) as u64
            }
            KeyFormat::V2 => {
                let packed = u64::from_le_bytes([
                    self.raw[8],
                    self.raw[9],
                    self.raw[10],
                    self.raw[11],
                    self.raw[12],
                    self.raw[13],
                    self.raw[14],
                    self.raw[15],
                ]);
                packed & 0x0FFF_FFFF_FFFF_FFFF
            }
        }
    }

    /// Item type, decoded for the given key format. Unrecognized values
    /// surface as [`ItemType::Unknown`] so callers can route gracefully.
    pub fn item_type(&self, format: KeyFormat) -> ItemType {
        match format {
            KeyFormat::V1 => {
                let t =
                    u32::from_le_bytes([self.raw[12], self.raw[13], self.raw[14], self.raw[15]]);
                ItemType::from_v1(t)
            }
            KeyFormat::V2 => {
                let high = u64::from_le_bytes([
                    self.raw[8],
                    self.raw[9],
                    self.raw[10],
                    self.raw[11],
                    self.raw[12],
                    self.raw[13],
                    self.raw[14],
                    self.raw[15],
                ]);
                let t = ((high >> 60) & 0xF) as u8;
                ItemType::from_v2(t)
            }
        }
    }
}

/// On-disk key format. Recorded per leaf item via [`ItemHead::version`].
/// Internal node keys inherit the volume's "natural" version from the
/// superblock (V2 for 3.6, V1 for 3.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyFormat {
    V1,
    V2,
}

impl KeyFormat {
    /// Translate an [`ItemHead::version`] byte (0 = V1, 2 = V2) into the
    /// matching key format. Other values are rejected so callers don't
    /// silently mis-parse a newer item format.
    pub fn from_version(version: u16) -> Result<Self, FilesystemError> {
        match version {
            0 => Ok(KeyFormat::V1),
            2 => Ok(KeyFormat::V2),
            other => Err(FilesystemError::Parse(format!(
                "reiserfs: unknown item version {other} (expected 0 or 2)"
            ))),
        }
    }
}

/// Logical type of an item in the S+tree, derived from the key's
/// type field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ItemType {
    /// Stat-data: file/dir metadata (mode, size, uid, gid, times).
    StatData,
    /// Indirect item: file body stored as an array of u32 block pointers.
    Indirect,
    /// Direct item: file body stored inline (used for small files + tails).
    Direct,
    /// Directory item: a chain of `(child_key, name_offset)` entries
    /// followed by the name area.
    DirEntry,
    /// Anything else — surfaced verbatim so the caller can log and skip.
    Unknown(u8),
}

impl ItemType {
    /// V1 keys use sentinel u32s: 0 = stat-data, 0xfffffffe = indirect,
    /// 0xffffffff = direct, 500..1000 = directory hash. We collapse those
    /// into our enum.
    pub fn from_v1(t: u32) -> Self {
        match t {
            0 => ItemType::StatData,
            0xFFFF_FFFE => ItemType::Indirect,
            0xFFFF_FFFF => ItemType::Direct,
            // V1 dir entries use a hashed offset in the `offset` field; the
            // type is always 500. Some sources use a different value but
            // 500 is what mainstream tools (reiserfsprogs) emit.
            500 => ItemType::DirEntry,
            other => ItemType::Unknown((other & 0xFF) as u8),
        }
    }

    /// V2 keys pack the type code into 4 bits. The standard mapping is:
    /// 0 = stat-data, 1 = indirect, 2 = direct, 3 = directory entry,
    /// 15 = "any" (used internally as a sentinel during inserts).
    pub fn from_v2(t: u8) -> Self {
        match t {
            0 => ItemType::StatData,
            1 => ItemType::Indirect,
            2 => ItemType::Direct,
            3 => ItemType::DirEntry,
            other => ItemType::Unknown(other),
        }
    }
}

/// Entry in an internal node's child-pointer array. There are
/// `nr_item + 1` of these immediately after the key array.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskChild {
    pub block_number: u32,
    pub size: u16,
}

impl DiskChild {
    /// Parse a single DiskChild from an 8-byte slice.
    pub fn parse(buf: &[u8]) -> Result<Self, FilesystemError> {
        if buf.len() < DISK_CHILD_SIZE {
            return Err(FilesystemError::Parse(format!(
                "reiserfs DiskChild: need {DISK_CHILD_SIZE} bytes, got {}",
                buf.len()
            )));
        }
        Ok(Self {
            block_number: u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]),
            size: u16::from_le_bytes([buf[4], buf[5]]),
            // buf[6..8] is dc_reserved; skipped.
        })
    }
}

/// Item header in a leaf node. The actual item body lives at byte offset
/// [`Self::item_location`] within the same leaf block, for
/// [`Self::item_len`] bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ItemHead {
    pub key: Key,
    /// For directory items this is the entry count; for other item types
    /// it's a free-space counter the reader can ignore.
    pub free_space_or_entry_count: u16,
    pub item_len: u16,
    pub item_location: u16,
    pub version: u16,
}

impl ItemHead {
    /// Parse an item head from a 24-byte slice.
    pub fn parse(buf: &[u8]) -> Result<Self, FilesystemError> {
        if buf.len() < ITEM_HEAD_SIZE {
            return Err(FilesystemError::Parse(format!(
                "reiserfs ItemHead: need {ITEM_HEAD_SIZE} bytes, got {}",
                buf.len()
            )));
        }
        Ok(Self {
            key: Key::parse_raw(&buf[0..16]),
            free_space_or_entry_count: u16::from_le_bytes([buf[16], buf[17]]),
            item_len: u16::from_le_bytes([buf[18], buf[19]]),
            item_location: u16::from_le_bytes([buf[20], buf[21]]),
            version: u16::from_le_bytes([buf[22], buf[23]]),
        })
    }

    /// Returns the key format implied by [`Self::version`].
    pub fn key_format(&self) -> Result<KeyFormat, FilesystemError> {
        KeyFormat::from_version(self.version)
    }
}

/// Parse every ItemHead in a leaf node.
///
/// The slice must be the whole leaf block (so the parser can refuse to
/// read past `block_size` if the on-disk `nr_item` is corrupt).
pub fn parse_leaf_item_heads(block: &[u8]) -> Result<Vec<ItemHead>, FilesystemError> {
    let head = BlockHead::parse(block)?;
    if !head.is_leaf() {
        return Err(FilesystemError::Parse(format!(
            "reiserfs: expected leaf node (level 0), got level {}",
            head.level
        )));
    }
    let nr = head.nr_item as usize;
    let needed = BLOCK_HEAD_SIZE + nr * ITEM_HEAD_SIZE;
    if block.len() < needed {
        return Err(FilesystemError::Parse(format!(
            "reiserfs leaf: nr_item={nr} requires {needed} bytes, have {}",
            block.len()
        )));
    }
    let mut items = Vec::with_capacity(nr);
    for i in 0..nr {
        let off = BLOCK_HEAD_SIZE + i * ITEM_HEAD_SIZE;
        items.push(ItemHead::parse(&block[off..off + ITEM_HEAD_SIZE])?);
    }
    Ok(items)
}

/// Parse every key + child pointer in an internal node.
///
/// Returns `(keys, children)` where `keys.len() == nr_item` and
/// `children.len() == nr_item + 1` (B-tree internal-node invariant).
pub fn parse_internal_keys_and_children(
    block: &[u8],
) -> Result<(Vec<Key>, Vec<DiskChild>), FilesystemError> {
    let head = BlockHead::parse(block)?;
    if head.is_leaf() {
        return Err(FilesystemError::Parse(
            "reiserfs: expected internal node, got leaf".into(),
        ));
    }
    let nr = head.nr_item as usize;
    let keys_end = BLOCK_HEAD_SIZE + nr * KEY_SIZE;
    let children_end = keys_end + (nr + 1) * DISK_CHILD_SIZE;
    if block.len() < children_end {
        return Err(FilesystemError::Parse(format!(
            "reiserfs internal: nr_item={nr} requires {children_end} bytes, have {}",
            block.len()
        )));
    }

    let mut keys = Vec::with_capacity(nr);
    for i in 0..nr {
        let off = BLOCK_HEAD_SIZE + i * KEY_SIZE;
        keys.push(Key::parse_raw(&block[off..off + KEY_SIZE]));
    }
    let mut children = Vec::with_capacity(nr + 1);
    for i in 0..=nr {
        let off = keys_end + i * DISK_CHILD_SIZE;
        children.push(DiskChild::parse(&block[off..off + DISK_CHILD_SIZE])?);
    }
    Ok((keys, children))
}

// ---- Helpers ----

#[inline]
fn le32(buf: &[u8], off: usize) -> u32 {
    u32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]])
}

// ---- Tests ----

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Build a synthetic ReiserFS superblock at offset 65536 of a fresh
    /// byte vector. `magic` is the 10-byte (or shorter, zero-padded) magic
    /// string. `block_size_field` is what to write at offset 44 inside the
    /// superblock. The returned vector is sized to `block_count *
    /// real_block_size` so it could plausibly host the volume.
    fn synth_volume(
        magic: &[u8],
        block_count: u32,
        free_blocks: u32,
        block_size_field: u16,
        label: Option<&[u8]>,
        s_version: u16,
    ) -> Vec<u8> {
        // Volume is sized to 4 MiB minimum — plenty of room past the
        // superblock for any future bitmap reads.
        let real_block_size = block_size_field as u64;
        let total_bytes = (block_count as u64 * real_block_size).max(4 * 1024 * 1024);
        let mut buf = vec![0u8; total_bytes as usize];

        let sb_off = REISERFS_SUPERBLOCK_OFFSET as usize;
        buf[sb_off..sb_off + 4].copy_from_slice(&block_count.to_le_bytes());
        buf[sb_off + 4..sb_off + 8].copy_from_slice(&free_blocks.to_le_bytes());
        // s_root_block + s_journal_params (32 bytes) are left zeroed.
        buf[sb_off + 0x2C..sb_off + 0x2E].copy_from_slice(&block_size_field.to_le_bytes());
        // s_umount_state = 1 (clean) at offset 50.
        buf[sb_off + 0x32..sb_off + 0x34].copy_from_slice(&1u16.to_le_bytes());

        let mut magic_field = [0u8; MAGIC_LEN];
        let n = magic.len().min(MAGIC_LEN);
        magic_field[..n].copy_from_slice(&magic[..n]);
        buf[sb_off + MAGIC_OFFSET..sb_off + MAGIC_OFFSET + MAGIC_LEN].copy_from_slice(&magic_field);

        // s_bmap_nr at offset 70 = 0 (compute-from-block-count sentinel).
        buf[sb_off + 0x46..sb_off + 0x48].copy_from_slice(&0u16.to_le_bytes());
        // s_version at offset 72.
        buf[sb_off + 0x48..sb_off + 0x4A].copy_from_slice(&s_version.to_le_bytes());

        if let Some(lbl) = label {
            let n = lbl.len().min(16);
            buf[sb_off + 100..sb_off + 100 + n].copy_from_slice(&lbl[..n]);
        }
        buf
    }

    #[test]
    fn opens_v3_6_with_label() {
        let buf = synth_volume(MAGIC_V3_6, 100, 50, 4096, Some(b"test_reiser"), 2);
        let fs = ReiserFsFilesystem::open(Cursor::new(buf), 0).expect("open v3.6");

        assert_eq!(fs.version(), ReiserFsVersion::V3_6);
        assert_eq!(fs.fs_type(), "ReiserFS 3.6");
        assert_eq!(fs.volume_label(), Some("test_reiser"));
        assert_eq!(fs.total_size(), 100 * 4096);
        assert_eq!(fs.used_size(), 50 * 4096);
        // bmap_nr should be computed: ceil(100 / (4096 * 8)) = 1.
        assert_eq!(fs.bmap_nr, 1);
    }

    #[test]
    fn opens_v3_5_no_label() {
        // v3.5 has no label field. Even if we wrote bytes at offset 100, the
        // parser shouldn't surface them.
        let buf = synth_volume(MAGIC_V3_5, 200, 100, 4096, Some(b"ignored"), 0);
        let fs = ReiserFsFilesystem::open(Cursor::new(buf), 0).expect("open v3.5");

        assert_eq!(fs.version(), ReiserFsVersion::V3_5);
        assert_eq!(fs.fs_type(), "ReiserFS 3.5");
        assert_eq!(fs.volume_label(), None);
        assert_eq!(fs.total_size(), 200 * 4096);
        assert_eq!(fs.used_size(), 100 * 4096);
    }

    #[test]
    fn rejects_reiser4() {
        let buf = synth_volume(MAGIC_REISER4, 100, 50, 4096, None, 0);
        match ReiserFsFilesystem::open(Cursor::new(buf), 0) {
            Err(FilesystemError::Unsupported(msg)) => assert!(msg.contains("reiser4")),
            Ok(_) => panic!("expected error rejecting reiser4"),
            Err(other) => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[test]
    fn rejects_unknown_magic() {
        let buf = synth_volume(b"NotReiser", 100, 50, 4096, None, 0);
        match ReiserFsFilesystem::open(Cursor::new(buf), 0) {
            Err(FilesystemError::Parse(msg)) => assert!(msg.contains("invalid superblock magic")),
            Ok(_) => panic!("expected error rejecting unknown magic"),
            Err(other) => panic!("expected Parse, got {other:?}"),
        }
    }

    #[test]
    fn rejects_invalid_block_size() {
        let buf = synth_volume(MAGIC_V3_6, 100, 50, 3000, None, 2);
        match ReiserFsFilesystem::open(Cursor::new(buf), 0) {
            Err(FilesystemError::Parse(msg)) => assert!(msg.contains("invalid block size")),
            Ok(_) => panic!("expected error on odd block size"),
            Err(other) => panic!("expected Parse, got {other:?}"),
        }
    }

    #[test]
    fn rejects_free_blocks_exceeding_total() {
        let buf = synth_volume(MAGIC_V3_6, 100, 200, 4096, None, 2);
        match ReiserFsFilesystem::open(Cursor::new(buf), 0) {
            Err(FilesystemError::Parse(msg)) => assert!(msg.contains("free_blocks")),
            Ok(_) => panic!("expected error on inconsistent free count"),
            Err(other) => panic!("expected Parse, got {other:?}"),
        }
    }

    #[test]
    fn opens_at_nonzero_partition_offset() {
        // Place the volume at offset 1 MiB to confirm partition_offset is
        // threaded through. The buffer is sized to fit the offset plus the
        // synthesised volume.
        let inner = synth_volume(MAGIC_V3_6, 100, 25, 4096, Some(b"offset_test"), 2);
        let part_off: u64 = 1024 * 1024;
        let mut buf = vec![0u8; part_off as usize + inner.len()];
        buf[part_off as usize..].copy_from_slice(&inner);

        let fs = ReiserFsFilesystem::open(Cursor::new(buf), part_off).expect("open at offset");
        assert_eq!(fs.volume_label(), Some("offset_test"));
        assert_eq!(fs.total_size(), 100 * 4096);
        assert_eq!(fs.used_size(), 75 * 4096);
    }

    #[test]
    fn probe_0x83_routes_reiserfs() {
        // Confirm the magic-based dispatch in src/fs/mod.rs surfaces
        // ReiserFS through probe_0x83_fs_type — the public entry point the
        // partition-table label column uses to swap "Linux" for the real
        // filesystem family.
        let buf = synth_volume(MAGIC_V3_6, 100, 50, 4096, Some(b"probe"), 2);
        let mut cursor = Cursor::new(buf);
        assert_eq!(
            crate::fs::probe_0x83_fs_type(&mut cursor, 0),
            Some("ReiserFS")
        );
    }

    /// Set bit `bit` (LSB-first within each byte) in `data`. Tests construct
    /// real bitmap layouts with this so the bitmap-walk path is exercised.
    fn set_bit(data: &mut [u8], bit: u64) {
        let byte = (bit / 8) as usize;
        let off = (bit % 8) as u32;
        data[byte] |= 1u8 << off;
    }

    /// Build a synthetic ReiserFS volume + write a populated bitmap.
    /// Returns the raw image plus `(block_size, total_blocks)` for assertions.
    ///
    /// Reserved blocks (boot area, superblock, and every bitmap block) are
    /// always marked allocated — that matches what a real `mkfs.reiserfs`
    /// produces. `extra_allocated` lists additional user-data blocks the
    /// caller wants set for a specific test.
    fn synth_volume_with_bitmap(
        magic: &[u8],
        block_count: u32,
        block_size_field: u16,
        s_version: u16,
        extra_allocated: &[u64],
    ) -> (Vec<u8>, u64, u64) {
        let mut buf = synth_volume(magic, block_count, 0, block_size_field, None, s_version);
        let block_size = block_size_field as u64;
        let sb_block = REISERFS_SUPERBLOCK_OFFSET / block_size;
        let blocks_per_bitmap = block_size * 8;
        let bmap_nr = (block_count as u64).div_ceil(blocks_per_bitmap) as u32;

        // Collect everything that needs to be set, then write all bits.
        let mut allocated: Vec<u64> = Vec::new();
        // Reserved area: blocks 0..=sb_block. The actual boot area is usually
        // sparse but real ReiserFS marks the leading run allocated.
        for b in 0..=sb_block {
            allocated.push(b);
        }
        // Bitmap blocks themselves.
        for i in 0..bmap_nr {
            let phys = if i == 0 {
                sb_block + 1
            } else {
                i as u64 * blocks_per_bitmap
            };
            if phys < block_count as u64 {
                allocated.push(phys);
            }
        }
        allocated.extend_from_slice(extra_allocated);

        for absolute_block in allocated {
            if absolute_block >= block_count as u64 {
                continue;
            }
            let bitmap_idx = (absolute_block / blocks_per_bitmap) as u32;
            assert!(bitmap_idx < bmap_nr, "test sets out-of-range block");
            let bit_in_bitmap = absolute_block % blocks_per_bitmap;
            let bitmap_phys_block = if bitmap_idx == 0 {
                sb_block + 1
            } else {
                bitmap_idx as u64 * blocks_per_bitmap
            };
            let bitmap_byte_off = (bitmap_phys_block * block_size) as usize;
            // The synth_volume helper pads to 4 MiB minimum but extra bitmap
            // blocks past block_count may sit past EOF — extend buf if needed.
            let bitmap_block_end = bitmap_byte_off + block_size as usize;
            if buf.len() < bitmap_block_end {
                buf.resize(bitmap_block_end, 0);
            }
            set_bit(
                &mut buf[bitmap_byte_off..bitmap_byte_off + block_size as usize],
                bit_in_bitmap,
            );
        }

        (buf, block_size, block_count as u64)
    }

    #[test]
    fn last_data_byte_returns_highest_allocated_block_plus_one() {
        // 64-block 4096-byte volume; reserved blocks fill 0..=17, extras
        // add 20, 30, 50. Highest is 50, last_data_byte = 51 * 4096.
        let (buf, bs, _) = synth_volume_with_bitmap(MAGIC_V3_6, 64, 4096, 2, &[20, 30, 50]);
        let mut fs = ReiserFsFilesystem::open(Cursor::new(buf), 0).expect("open");
        assert_eq!(fs.last_data_byte().unwrap(), 51 * bs);
    }

    #[test]
    fn last_data_byte_with_only_reserved_blocks() {
        // Fresh-from-mkfs case: only reserved metadata (boot + superblock +
        // bitmap) is allocated. For block_size = 4096, sb_block = 16 and
        // the first bitmap at block 17 is the highest set bit, so
        // last_data_byte = (17 + 1) * 4096.
        let (buf, bs, _) = synth_volume_with_bitmap(MAGIC_V3_6, 64, 4096, 2, &[]);
        let mut fs = ReiserFsFilesystem::open(Cursor::new(buf), 0).expect("open");
        assert_eq!(fs.last_data_byte().unwrap(), 18 * bs);
    }

    #[test]
    fn last_data_byte_falls_back_to_total_when_bitmap_empty() {
        // Construct a volume whose bitmap is genuinely all-zero (no reserved
        // blocks marked). The fallback path returns total_size verbatim.
        let buf = synth_volume(MAGIC_V3_6, 64, 0, 4096, None, 2);
        let mut fs = ReiserFsFilesystem::open(Cursor::new(buf), 0).expect("open");
        assert_eq!(fs.last_data_byte().unwrap(), 64 * 4096);
    }

    #[test]
    fn last_data_byte_spans_multiple_bitmap_blocks() {
        // block_size = 1024 → blocks_per_bitmap = 8192, so a 24576-block
        // volume has 3 bitmaps (at physical blocks 65, 8192, 16384). With
        // an extra allocation at block 20000 (inside bitmap 2's range) the
        // highest set bit lives in bitmap 2 and last_data_byte is
        // (20000 + 1) * 1024.
        let (buf, bs, _) =
            synth_volume_with_bitmap(MAGIC_V3_6, 24576, 1024, 2, &[100, 9000, 20000]);
        let mut fs = ReiserFsFilesystem::open(Cursor::new(buf), 0).expect("open");
        assert_eq!(fs.last_data_byte().unwrap(), 20001 * bs);
    }

    #[test]
    fn compact_reader_emits_layout_preserving_stream() {
        // 64-block 4096-byte volume with extras 20, 30, 50 allocated past the
        // 18-block reserved area. Compact stream is 64 * 4096 bytes; each
        // allocated user block's bytes sit at their original byte offset.
        let (mut buf, bs, total) = synth_volume_with_bitmap(MAGIC_V3_6, 64, 4096, 2, &[20, 30, 50]);
        for (block, fill) in [(20u64, 0xAAu8), (30, 0xBB), (50, 0xCC)] {
            let off = (block * bs) as usize;
            buf[off..off + bs as usize].fill(fill);
        }

        let (mut compact, info) =
            CompactReiserFsReader::new(Cursor::new(buf), 0).expect("build compact");
        assert_eq!(info.original_size, total * bs);
        assert_eq!(info.compacted_size, total * bs); // layout-preserving
                                                     // Reserved (boot 0..=16, superblock at 16 dedups, first bitmap at 17)
                                                     // = 18 blocks, plus 3 extras = 21 allocated total.
        assert_eq!(info.clusters_used, 21);

        let mut out = Vec::new();
        compact.read_to_end(&mut out).expect("read");
        assert_eq!(out.len(), (total * bs) as usize);

        for (block, fill) in [(20u64, 0xAAu8), (30, 0xBB), (50, 0xCC)] {
            let off = (block * bs) as usize;
            assert!(
                out[off..off + bs as usize].iter().all(|&b| b == fill),
                "block {block} did not contain its fill byte 0x{fill:02X}"
            );
        }
        // Spot-check a free block (25, between extras and well past reserved)
        // stays zeroed in the compact stream.
        let free_off = (25 * bs) as usize;
        assert!(out[free_off..free_off + bs as usize]
            .iter()
            .all(|&b| b == 0));
    }

    #[test]
    fn compact_reader_round_trips_through_parser() {
        // Compact stream of a populated volume should re-detect as the same
        // ReiserFS volume and report the same sizes / label.
        let (buf, _bs, _total) =
            synth_volume_with_bitmap(MAGIC_V3_6, 128, 4096, 2, &[5, 10, 18, 100]);
        let original_len = buf.len();
        // Inject a label to confirm the V2 extension survives the round-trip.
        let sb_off = REISERFS_SUPERBLOCK_OFFSET as usize;
        let mut buf = buf;
        buf[sb_off + 100..sb_off + 100 + 9].copy_from_slice(b"roundtrip");

        let (mut compact, _) =
            CompactReiserFsReader::new(Cursor::new(buf), 0).expect("build compact");
        let mut out = Vec::new();
        compact.read_to_end(&mut out).expect("read");
        // The compact stream covers the volume's declared block range; the
        // original `synth_volume` buffer may have been larger (4 MiB pad).
        // Padding tail beyond the volume is dropped by the reader.
        assert!(out.len() <= original_len);

        let fs = ReiserFsFilesystem::open(Cursor::new(out), 0).expect("re-open compact");
        assert_eq!(fs.version(), ReiserFsVersion::V3_6);
        assert_eq!(fs.volume_label(), Some("roundtrip"));
        assert_eq!(fs.total_size(), 128 * 4096);
    }

    #[test]
    fn computes_bmap_nr_from_explicit_field() {
        // Override s_bmap_nr to a non-zero value and confirm we use it
        // verbatim instead of computing.
        let mut buf = synth_volume(MAGIC_V3_6, 1000, 0, 4096, None, 2);
        let sb_off = REISERFS_SUPERBLOCK_OFFSET as usize;
        buf[sb_off + 0x46..sb_off + 0x48].copy_from_slice(&7u16.to_le_bytes());

        let fs = ReiserFsFilesystem::open(Cursor::new(buf), 0).expect("open");
        assert_eq!(fs.bmap_nr, 7);
    }

    // ---- R.3a: S+tree on-disk structure tests ----

    /// Build a 16-byte V2 key with the given fields. `kind` is the 4-bit
    /// type code; `offset` occupies the low 60 bits of the packed u64.
    fn build_v2_key(dir_id: u32, objectid: u32, offset: u64, kind: u8) -> [u8; 16] {
        let mut k = [0u8; 16];
        k[0..4].copy_from_slice(&dir_id.to_le_bytes());
        k[4..8].copy_from_slice(&objectid.to_le_bytes());
        let packed = ((kind as u64) << 60) | (offset & 0x0FFF_FFFF_FFFF_FFFF);
        k[8..16].copy_from_slice(&packed.to_le_bytes());
        k
    }

    /// Build a 16-byte V1 key with the given fields.
    fn build_v1_key(dir_id: u32, objectid: u32, offset: u32, kind: u32) -> [u8; 16] {
        let mut k = [0u8; 16];
        k[0..4].copy_from_slice(&dir_id.to_le_bytes());
        k[4..8].copy_from_slice(&objectid.to_le_bytes());
        k[8..12].copy_from_slice(&offset.to_le_bytes());
        k[12..16].copy_from_slice(&kind.to_le_bytes());
        k
    }

    #[test]
    fn parses_v2_key_fields() {
        let raw = build_v2_key(7, 42, 0xABCD, 3 /* DIR_ENTRY */);
        let k = Key::parse_raw(&raw);
        assert_eq!(k.dir_id(), 7);
        assert_eq!(k.objectid(), 42);
        assert_eq!(k.offset(KeyFormat::V2), 0xABCD);
        assert_eq!(k.item_type(KeyFormat::V2), ItemType::DirEntry);
    }

    #[test]
    fn parses_v1_key_fields_and_type_sentinels() {
        let raw = build_v1_key(1, 2, 100, 0xFFFF_FFFE /* INDIRECT */);
        let k = Key::parse_raw(&raw);
        assert_eq!(k.dir_id(), 1);
        assert_eq!(k.objectid(), 2);
        assert_eq!(k.offset(KeyFormat::V1), 100);
        assert_eq!(k.item_type(KeyFormat::V1), ItemType::Indirect);
    }

    #[test]
    fn item_type_v2_covers_all_known_kinds() {
        assert_eq!(ItemType::from_v2(0), ItemType::StatData);
        assert_eq!(ItemType::from_v2(1), ItemType::Indirect);
        assert_eq!(ItemType::from_v2(2), ItemType::Direct);
        assert_eq!(ItemType::from_v2(3), ItemType::DirEntry);
        assert_eq!(ItemType::from_v2(15), ItemType::Unknown(15));
    }

    #[test]
    fn item_type_v1_decodes_sentinels() {
        assert_eq!(ItemType::from_v1(0), ItemType::StatData);
        assert_eq!(ItemType::from_v1(0xFFFF_FFFE), ItemType::Indirect);
        assert_eq!(ItemType::from_v1(0xFFFF_FFFF), ItemType::Direct);
        assert_eq!(ItemType::from_v1(500), ItemType::DirEntry);
        assert_eq!(ItemType::from_v1(7), ItemType::Unknown(7));
    }

    #[test]
    fn key_format_from_version_byte() {
        assert_eq!(KeyFormat::from_version(0).unwrap(), KeyFormat::V1);
        assert_eq!(KeyFormat::from_version(2).unwrap(), KeyFormat::V2);
        match KeyFormat::from_version(99) {
            Err(FilesystemError::Parse(msg)) => assert!(msg.contains("99")),
            other => panic!("expected Parse(99), got {other:?}"),
        }
    }

    #[test]
    fn parses_block_head_leaf_and_internal() {
        let mut block = [0u8; 4096];
        // Leaf: level 0, nr_item 3, free_space 1234.
        block[0..2].copy_from_slice(&0u16.to_le_bytes());
        block[2..4].copy_from_slice(&3u16.to_le_bytes());
        block[4..6].copy_from_slice(&1234u16.to_le_bytes());
        let head = BlockHead::parse(&block).expect("parse leaf");
        assert!(head.is_leaf());
        assert_eq!(head.nr_item, 3);
        assert_eq!(head.free_space, 1234);

        // Internal: level 2, nr_item 5.
        block[0..2].copy_from_slice(&2u16.to_le_bytes());
        block[2..4].copy_from_slice(&5u16.to_le_bytes());
        let head = BlockHead::parse(&block).expect("parse internal");
        assert!(!head.is_leaf());
        assert_eq!(head.level, 2);
        assert_eq!(head.nr_item, 5);
    }

    #[test]
    fn block_head_rejects_short_buffer() {
        let short = [0u8; 16];
        match BlockHead::parse(&short) {
            Err(FilesystemError::Parse(msg)) => assert!(msg.contains("block head")),
            other => panic!("expected Parse, got {other:?}"),
        }
    }

    #[test]
    fn parses_disk_child() {
        let bytes = [
            0x12, 0x34, 0x56, 0x78, // block_number = 0x78563412
            0x10, 0x20, // size = 0x2010
            0x00, 0x00, // reserved
        ];
        let dc = DiskChild::parse(&bytes).expect("parse child");
        assert_eq!(dc.block_number, 0x7856_3412);
        assert_eq!(dc.size, 0x2010);
    }

    #[test]
    fn parses_item_head_fields() {
        let mut ih_bytes = [0u8; ITEM_HEAD_SIZE];
        let key = build_v2_key(1, 2, 0, 0 /* STAT_DATA */);
        ih_bytes[0..16].copy_from_slice(&key);
        ih_bytes[16..18].copy_from_slice(&0u16.to_le_bytes()); // free_space
        ih_bytes[18..20].copy_from_slice(&44u16.to_le_bytes()); // item_len
        ih_bytes[20..22].copy_from_slice(&3500u16.to_le_bytes()); // item_location
        ih_bytes[22..24].copy_from_slice(&2u16.to_le_bytes()); // version = V2

        let ih = ItemHead::parse(&ih_bytes).expect("parse item head");
        assert_eq!(ih.item_len, 44);
        assert_eq!(ih.item_location, 3500);
        assert_eq!(ih.key_format().unwrap(), KeyFormat::V2);
        assert_eq!(ih.key.item_type(KeyFormat::V2), ItemType::StatData);
    }

    #[test]
    fn parse_leaf_returns_all_item_heads() {
        // Build a 4096-byte leaf with 3 item heads (24 bytes each).
        let mut block = vec![0u8; 4096];
        // BlockHead: level 0, nr_item 3.
        block[0..2].copy_from_slice(&0u16.to_le_bytes());
        block[2..4].copy_from_slice(&3u16.to_le_bytes());

        // Three V2 stat-data item heads at increasing positions.
        for i in 0..3 {
            let off = BLOCK_HEAD_SIZE + i * ITEM_HEAD_SIZE;
            let key = build_v2_key(i as u32 + 1, 10, 0, 0 /* STAT_DATA */);
            block[off..off + 16].copy_from_slice(&key);
            // item_len = 44, item_location = 4096 - (i+1) * 44
            block[off + 18..off + 20].copy_from_slice(&44u16.to_le_bytes());
            let loc = 4096u16 - ((i + 1) as u16) * 44;
            block[off + 20..off + 22].copy_from_slice(&loc.to_le_bytes());
            block[off + 22..off + 24].copy_from_slice(&2u16.to_le_bytes());
        }

        let heads = parse_leaf_item_heads(&block).expect("parse");
        assert_eq!(heads.len(), 3);
        for (i, h) in heads.iter().enumerate() {
            assert_eq!(h.key.dir_id(), i as u32 + 1);
            assert_eq!(h.item_len, 44);
            assert_eq!(h.key_format().unwrap(), KeyFormat::V2);
        }
    }

    #[test]
    fn parse_leaf_rejects_internal_node() {
        let mut block = vec![0u8; 4096];
        // level = 1 (not a leaf)
        block[0..2].copy_from_slice(&1u16.to_le_bytes());
        block[2..4].copy_from_slice(&3u16.to_le_bytes());
        match parse_leaf_item_heads(&block) {
            Err(FilesystemError::Parse(msg)) => assert!(msg.contains("expected leaf")),
            other => panic!("expected Parse, got {other:?}"),
        }
    }

    #[test]
    fn parse_internal_returns_keys_and_n_plus_one_children() {
        // 4-KiB internal node with 2 keys (so 3 children).
        let mut block = vec![0u8; 4096];
        block[0..2].copy_from_slice(&1u16.to_le_bytes()); // level 1
        block[2..4].copy_from_slice(&2u16.to_le_bytes()); // nr_item 2

        let keys_off = BLOCK_HEAD_SIZE;
        block[keys_off..keys_off + 16].copy_from_slice(&build_v2_key(1, 100, 0, 0));
        block[keys_off + 16..keys_off + 32].copy_from_slice(&build_v2_key(1, 200, 0, 0));

        let children_off = keys_off + 2 * KEY_SIZE;
        // Three DiskChild entries (8 bytes each): block_number = i+10, size = 4096.
        for i in 0..3 {
            let off = children_off + i * DISK_CHILD_SIZE;
            block[off..off + 4].copy_from_slice(&(i as u32 + 10).to_le_bytes());
            block[off + 4..off + 6].copy_from_slice(&4096u16.to_le_bytes());
        }

        let (keys, children) = parse_internal_keys_and_children(&block).expect("parse internal");
        assert_eq!(keys.len(), 2);
        assert_eq!(children.len(), 3);
        assert_eq!(keys[0].objectid(), 100);
        assert_eq!(keys[1].objectid(), 200);
        assert_eq!(children[0].block_number, 10);
        assert_eq!(children[1].block_number, 11);
        assert_eq!(children[2].block_number, 12);
    }

    #[test]
    fn parse_internal_rejects_leaf_node() {
        let mut block = vec![0u8; 4096];
        block[0..2].copy_from_slice(&0u16.to_le_bytes()); // level 0 (leaf)
        block[2..4].copy_from_slice(&1u16.to_le_bytes());
        match parse_internal_keys_and_children(&block) {
            Err(FilesystemError::Parse(msg)) => assert!(msg.contains("expected internal")),
            other => panic!("expected Parse, got {other:?}"),
        }
    }
}
