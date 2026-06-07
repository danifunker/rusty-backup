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
use super::unix_common::inode::{format_unix_timestamp, unix_file_type, UnixFileType};
use crate::fs::CompactResult;

// ---- Constants ----

/// ReiserFS superblock lives 64 KiB into the partition.
const REISERFS_SUPERBLOCK_OFFSET: u64 = 65536;

// ---- Reserved object-id sentinels (kernel `reiserfs_fs.h`) ----

/// Sentinel "parent objectid" recorded against the root directory. Real
/// objectids start at 2; this 1 is just a tag meaning "no real parent."
pub const REISERFS_ROOT_PARENT_OBJECTID: u32 = 1;

/// Objectid every freshly-created ReiserFS volume assigns to its root
/// directory. (`mkfs.reiserfs` writes the root's StatData with key
/// `(1, 2, 0, StatData)`.)
pub const REISERFS_ROOT_OBJECTID: u32 = 2;

/// Hidden directory name used by reiserfs to store extended attributes
/// and ACLs. We filter it from `list_directory` the same way the kernel
/// hides it from userland.
const REISERFS_PRIV_NAME: &str = ".reiserfs_priv";

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

    /// Read an arbitrary tree (or data) block by its absolute block number.
    /// Refuses to read past the volume's declared block count so a corrupt
    /// child pointer can't drag the cursor into adjacent partitions.
    pub(crate) fn read_tree_block(&mut self, block_num: u32) -> Result<Vec<u8>, FilesystemError> {
        if block_num == 0 {
            return Err(FilesystemError::Parse(
                "reiserfs: block 0 is the boot area, not a tree block".into(),
            ));
        }
        if (block_num as u64) >= self.total_blocks {
            return Err(FilesystemError::Parse(format!(
                "reiserfs: block number {block_num} exceeds total_blocks {}",
                self.total_blocks
            )));
        }
        let off = self.partition_offset + (block_num as u64) * self.block_size;
        self.reader.seek(SeekFrom::Start(off))?;
        let mut buf = vec![0u8; self.block_size as usize];
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Walk the S+tree from the root and return every leaf block number in
    /// left-to-right (key-sorted) order.
    ///
    /// The naïve "visit every leaf" strategy is intentional for a first
    /// pass. Real volumes can have many leaves, but each `list_directory`
    /// or `read_file` call only ever consults a tiny subset; a follow-up
    /// can replace this with a key-bounded descent once a workload makes
    /// the difference measurable. The on-disk fixture
    /// (`tests/fixtures/test_reiserfs_v3_6.img.zst`) is a single-leaf
    /// tree, so this gets exercised end-to-end without B-tree depth.
    pub(crate) fn collect_leaf_block_numbers(&mut self) -> Result<Vec<u32>, FilesystemError> {
        // Refuse the degenerate "root_block = 0" case: real volumes always
        // have the tree past the journal / bitmap reservation.
        if self.root_block == 0 {
            return Err(FilesystemError::Parse(
                "reiserfs: root_block is 0 (uninitialized superblock?)".into(),
            ));
        }
        let mut leaves: Vec<u32> = Vec::new();
        // DFS via an explicit stack; children pushed in reverse so the
        // returned leaf order matches the on-disk key order.
        let mut stack: Vec<u32> = vec![self.root_block];
        // Bound the worst-case traversal so a corrupt tree can't run away.
        // 1M tree nodes covers any realistic volume (a 1 TB FS with 4 KiB
        // blocks tops out at ~262M data blocks but the tree's internal +
        // leaf count is a tiny fraction of that).
        const MAX_VISITED: u32 = 1 << 20;
        let mut visited: u32 = 0;
        while let Some(block_num) = stack.pop() {
            visited = visited.saturating_add(1);
            if visited > MAX_VISITED {
                return Err(FilesystemError::Parse(
                    "reiserfs: tree walk exceeded 1M nodes (likely corrupt)".into(),
                ));
            }
            let block = self.read_tree_block(block_num)?;
            let head = BlockHead::parse(&block)?;
            if head.is_leaf() {
                leaves.push(block_num);
                continue;
            }
            // Internal node. Refuse impossibly tall trees up-front.
            if head.level > MAX_TREE_LEVEL {
                return Err(FilesystemError::Parse(format!(
                    "reiserfs: tree level {} exceeds MAX_TREE_LEVEL {}",
                    head.level, MAX_TREE_LEVEL
                )));
            }
            let (_keys, children) = parse_internal_keys_and_children(&block)?;
            // Reverse so popping gives left-to-right traversal.
            for child in children.iter().rev() {
                // `read_tree_block` validates the range, but a child
                // pointer of 0 specifically means "no subtree" in a
                // partially-written internal node — refuse here rather
                // than chase the boot block.
                if child.block_number == 0 {
                    return Err(FilesystemError::Parse(
                        "reiserfs: internal node child pointer is 0".into(),
                    ));
                }
                stack.push(child.block_number);
            }
        }
        Ok(leaves)
    }

    /// Collect every item in the tree whose key matches
    /// `(dir_id, objectid)`. Each match is returned as
    /// `(item_head, item_body_bytes)` where the bytes are a fresh copy
    /// of the in-leaf body (so the caller can hand them to a decoder
    /// without juggling block buffers).
    ///
    /// Items are returned in tree (key-sorted) order, which is also the
    /// natural order for stitching together SD + IND + DRCT pieces of a
    /// single file or DIR_ENTRY items of a single directory.
    pub(crate) fn collect_items_for_object(
        &mut self,
        dir_id: u32,
        objectid: u32,
    ) -> Result<Vec<(ItemHead, Vec<u8>)>, FilesystemError> {
        let leaf_numbers = self.collect_leaf_block_numbers()?;
        let mut out: Vec<(ItemHead, Vec<u8>)> = Vec::new();
        for leaf_num in leaf_numbers {
            let block = self.read_tree_block(leaf_num)?;
            let items = parse_leaf_item_heads(&block)?;
            for ih in items {
                if ih.key.dir_id() != dir_id || ih.key.objectid() != objectid {
                    continue;
                }
                let loc = ih.item_location as usize;
                let end = loc.saturating_add(ih.item_len as usize);
                if end > block.len() {
                    return Err(FilesystemError::Parse(format!(
                        "reiserfs: item bytes {loc}..{end} exceed block size {}",
                        block.len()
                    )));
                }
                out.push((ih, block[loc..end].to_vec()));
            }
        }
        Ok(out)
    }

    /// Read an object's StatData. Errors if the object has no SD item
    /// (corrupt) or the item's body doesn't decode.
    fn read_statdata(&mut self, dir_id: u32, objectid: u32) -> Result<StatData, FilesystemError> {
        let items = self.collect_items_for_object(dir_id, objectid)?;
        for (ih, body) in items {
            let fmt = ih.key_format()?;
            if matches!(ih.key.item_type(fmt), ItemType::StatData) {
                return StatData::parse(&body);
            }
        }
        Err(FilesystemError::Parse(format!(
            "reiserfs: object ({dir_id}, {objectid}) has no StatData item"
        )))
    }

    /// Read a symlink's target text. Symlink bodies live in a Direct
    /// item (always one block or smaller in practice). We truncate to
    /// the StatData-recorded size to drop the slot's NUL padding.
    fn read_symlink_target(
        &mut self,
        dir_id: u32,
        objectid: u32,
        sd_size: u64,
    ) -> Result<String, FilesystemError> {
        let items = self.collect_items_for_object(dir_id, objectid)?;
        for (ih, body) in items {
            let fmt = ih.key_format()?;
            if matches!(ih.key.item_type(fmt), ItemType::Direct) {
                let take = (sd_size as usize).min(body.len());
                return Ok(String::from_utf8_lossy(&body[..take]).into_owned());
            }
        }
        Err(FilesystemError::Parse(format!(
            "reiserfs: symlink ({dir_id}, {objectid}) has no Direct item"
        )))
    }
}

// ---- Location packing for FileEntry ----
//
// `FileEntry::location` is a single u64, but ReiserFS needs both the
// parent's objectid (the `dir_id` field of an item's key) and the
// object's own objectid to look up its SD / DIR_ENTRY items. We pack
// them as `(dir_id << 32) | objectid` since both are u32 on disk.

#[inline]
fn pack_loc(dir_id: u32, objectid: u32) -> u64 {
    ((dir_id as u64) << 32) | (objectid as u64)
}

#[inline]
fn unpack_loc(loc: u64) -> (u32, u32) {
    ((loc >> 32) as u32, (loc & 0xFFFF_FFFF) as u32)
}

// ---- StatData (item type 0) ----

/// Decoded fields of a ReiserFS StatData item.
///
/// On-disk versions:
///   * v3.6 / "new" SD (44 bytes): the layout below.
///   * v3.5 / "old" SD (32 bytes): different field shapes; not produced
///     by `mkfs.reiserfs --format 3.6` and not exercised by our fixture.
///     We accept either size; the old-SD path follows the v1 layout.
#[derive(Debug, Clone, Copy)]
pub(crate) struct StatData {
    pub mode: u32,
    pub size: u64,
    pub uid: u32,
    pub gid: u32,
    pub mtime: u32,
}

impl StatData {
    /// Parse a StatData body. Dispatches on body length (44 = new, 32 = old).
    fn parse(body: &[u8]) -> Result<Self, FilesystemError> {
        match body.len() {
            44 => Self::parse_new(body),
            32 => Self::parse_old(body),
            other => Err(FilesystemError::Parse(format!(
                "reiserfs StatData: expected 32 or 44 bytes, got {other}"
            ))),
        }
    }

    /// v3.6 / "new" StatData. Field offsets per kernel `struct stat_data`
    /// in `reiserfs_fs.h`.
    fn parse_new(b: &[u8]) -> Result<Self, FilesystemError> {
        let mode = u16::from_le_bytes([b[0], b[1]]) as u32;
        // bytes 2..4 = sd_attrs (skipped)
        // bytes 4..8  = sd_nlink (parsed but not surfaced)
        let size = u64::from_le_bytes([b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]]);
        let uid = u32::from_le_bytes([b[16], b[17], b[18], b[19]]);
        let gid = u32::from_le_bytes([b[20], b[21], b[22], b[23]]);
        // bytes 24..28 = sd_atime (skipped)
        let mtime = u32::from_le_bytes([b[28], b[29], b[30], b[31]]);
        Ok(Self {
            mode,
            size,
            uid,
            gid,
            mtime,
        })
    }

    /// v3.5 / "old" StatData. Field offsets per kernel
    /// `struct stat_data_v1`.
    fn parse_old(b: &[u8]) -> Result<Self, FilesystemError> {
        let mode = u16::from_le_bytes([b[0], b[1]]) as u32;
        // bytes 2..4  = sd_nlink (u16, not surfaced)
        let uid = u16::from_le_bytes([b[4], b[5]]) as u32;
        let gid = u16::from_le_bytes([b[6], b[7]]) as u32;
        let size = u32::from_le_bytes([b[8], b[9], b[10], b[11]]) as u64;
        // bytes 12..16 = sd_atime (skipped)
        let mtime = u32::from_le_bytes([b[16], b[17], b[18], b[19]]);
        Ok(Self {
            mode,
            size,
            uid,
            gid,
            mtime,
        })
    }
}

// ---- Directory entry decoding (item type 3) ----

/// On-disk DIR_ENTRY header (`struct reiserfs_de_head`). Sixteen bytes,
/// always little-endian.
const DEH_SIZE: usize = 16;

/// One decoded directory entry (name + target object key).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DirEntry {
    pub name: String,
    /// Target object's `dir_id` (= the directory's objectid, since
    /// the entry lives inside that directory).
    pub deh_dir_id: u32,
    /// Target object's own objectid.
    pub deh_objectid: u32,
}

/// Decode every entry in a DIR_ENTRY item body.
///
/// `body` is the in-leaf bytes (`item_location..item_location+item_len`),
/// `entry_count` is the `free_space_or_entry_count` field of the
/// `ItemHead`.
///
/// On-disk layout: `entry_count` × 16-byte `reiserfs_de_head` (sorted by
/// hash), then names packed at the trailing end of the item, in reverse
/// order. The slot allocated to entry `i` runs from `deh[i].location`
/// up to `deh[i-1].location` (or `item_len` when `i == 0`). Within the
/// slot, the name extends from `deh.location` up to the first NUL byte
/// (slots are NUL-padded out to the next 8-byte alignment).
pub(crate) fn parse_dirent_item(
    body: &[u8],
    entry_count: u16,
) -> Result<Vec<DirEntry>, FilesystemError> {
    let n = entry_count as usize;
    if n == 0 {
        return Ok(Vec::new());
    }
    let headers_end = n.checked_mul(DEH_SIZE).ok_or_else(|| {
        FilesystemError::Parse(format!("reiserfs dirent: entry_count {n} overflows"))
    })?;
    if headers_end > body.len() {
        return Err(FilesystemError::Parse(format!(
            "reiserfs dirent: {n} headers need {headers_end} bytes, item is {}",
            body.len()
        )));
    }
    // First pass: parse all headers + validate ordering of name locations.
    let mut headers: Vec<(u32, u32, u16)> = Vec::with_capacity(n); // (dir_id, objectid, location)
    for i in 0..n {
        let off = i * DEH_SIZE;
        let h = &body[off..off + DEH_SIZE];
        // h[0..4]: deh_offset (hash | gen num) — not needed for read.
        let dir_id = u32::from_le_bytes([h[4], h[5], h[6], h[7]]);
        let objectid = u32::from_le_bytes([h[8], h[9], h[10], h[11]]);
        let location = u16::from_le_bytes([h[12], h[13]]);
        // h[14..16]: deh_state — bit 2 = visible. Not enforced.
        if (location as usize) < headers_end {
            return Err(FilesystemError::Parse(format!(
                "reiserfs dirent: name location {location} overlaps {n}-entry \
                 header area (ends at {headers_end})"
            )));
        }
        if (location as usize) > body.len() {
            return Err(FilesystemError::Parse(format!(
                "reiserfs dirent: name location {location} past item body ({})",
                body.len()
            )));
        }
        headers.push((dir_id, objectid, location));
    }
    // Decode names. Slot for entry i runs from headers[i].loc to
    // (i==0 ? item_len : headers[i-1].loc).
    let mut entries = Vec::with_capacity(n);
    for (i, &(dir_id, objectid, loc)) in headers.iter().enumerate() {
        let slot_end = if i == 0 {
            body.len()
        } else {
            headers[i - 1].2 as usize
        };
        let slot_start = loc as usize;
        if slot_end < slot_start {
            return Err(FilesystemError::Parse(format!(
                "reiserfs dirent: entry {i} slot is reversed ({slot_end} < {slot_start})"
            )));
        }
        let slot = &body[slot_start..slot_end];
        // Name ends at the first NUL byte; slot is NUL-padded.
        let name_end = slot.iter().position(|&b| b == 0).unwrap_or(slot.len());
        let name = String::from_utf8_lossy(&slot[..name_end]).into_owned();
        entries.push(DirEntry {
            name,
            deh_dir_id: dir_id,
            deh_objectid: objectid,
        });
    }
    Ok(entries)
}

impl<R: Read + Seek + Send> Filesystem for ReiserFsFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        let mut entry = FileEntry::root();
        entry.location = pack_loc(REISERFS_ROOT_PARENT_OBJECTID, REISERFS_ROOT_OBJECTID);
        // The root dir's StatData carries permissions / uid / gid the
        // browse view shows. Failing to read it here would block the
        // browse tab entirely, so surface a Parse error.
        let sd = self.read_statdata(REISERFS_ROOT_PARENT_OBJECTID, REISERFS_ROOT_OBJECTID)?;
        entry.mode = Some(sd.mode);
        entry.uid = Some(sd.uid);
        entry.gid = Some(sd.gid);
        Ok(entry)
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        let (dir_id, objectid) = unpack_loc(entry.location);
        let items = self.collect_items_for_object(dir_id, objectid)?;
        let mut children: Vec<FileEntry> = Vec::new();
        for (ih, body) in items {
            let fmt = ih.key_format()?;
            if !matches!(ih.key.item_type(fmt), ItemType::DirEntry) {
                continue;
            }
            let parsed = parse_dirent_item(&body, ih.free_space_or_entry_count)?;
            for ent in parsed {
                if ent.name == "." || ent.name == ".." || ent.name == REISERFS_PRIV_NAME {
                    continue;
                }
                let child_sd = self.read_statdata(ent.deh_dir_id, ent.deh_objectid)?;
                let parent_path = if entry.path == "/" {
                    String::new()
                } else {
                    entry.path.clone()
                };
                let child_path = format!("{}/{}", parent_path, ent.name);
                let child_loc = pack_loc(ent.deh_dir_id, ent.deh_objectid);
                let mut child = match unix_file_type(child_sd.mode) {
                    UnixFileType::Directory => {
                        FileEntry::new_directory(ent.name.clone(), child_path, child_loc)
                    }
                    UnixFileType::Symlink => {
                        let target = self.read_symlink_target(
                            ent.deh_dir_id,
                            ent.deh_objectid,
                            child_sd.size,
                        )?;
                        FileEntry::new_symlink(
                            ent.name.clone(),
                            child_path,
                            child_sd.size,
                            child_loc,
                            target,
                        )
                    }
                    UnixFileType::BlockDevice
                    | UnixFileType::CharDevice
                    | UnixFileType::Fifo
                    | UnixFileType::Socket => FileEntry::new_special(
                        ent.name.clone(),
                        child_path,
                        child_loc,
                        match unix_file_type(child_sd.mode) {
                            UnixFileType::BlockDevice => "block device".into(),
                            UnixFileType::CharDevice => "char device".into(),
                            UnixFileType::Fifo => "fifo".into(),
                            UnixFileType::Socket => "socket".into(),
                            _ => unreachable!(),
                        },
                    ),
                    UnixFileType::Regular | UnixFileType::Unknown => {
                        FileEntry::new_file(ent.name.clone(), child_path, child_sd.size, child_loc)
                    }
                };
                child.mode = Some(child_sd.mode);
                child.uid = Some(child_sd.uid);
                child.gid = Some(child_sd.gid);
                child.modified = Some(format_unix_timestamp(child_sd.mtime as i64));
                children.push(child);
            }
        }
        Ok(children)
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let (dir_id, objectid) = unpack_loc(entry.location);
        let items = self.collect_items_for_object(dir_id, objectid)?;

        // StatData carries the logical file size we truncate to. Items
        // come back in tree (key.offset) order, so SD (offset 0) is
        // always first, followed by IND / DRCT in increasing-offset
        // order. Concatenating their bodies gives the file in order.
        let mut sd_size: Option<u64> = None;
        let mut data: Vec<u8> = Vec::new();

        for (ih, body) in items {
            if data.len() >= max_bytes {
                break;
            }
            let fmt = ih.key_format()?;
            match ih.key.item_type(fmt) {
                ItemType::StatData => {
                    let sd = StatData::parse(&body)?;
                    sd_size = Some(sd.size);
                    let capacity = (sd.size as usize).min(max_bytes);
                    data.reserve(capacity.saturating_sub(data.capacity()));
                }
                ItemType::Indirect => {
                    // Body is a u32[] of block pointers; each pointer
                    // names an unformatted data block holding one
                    // block_size slice of the file.
                    if !body.len().is_multiple_of(4) {
                        return Err(FilesystemError::Parse(format!(
                            "reiserfs: IND item body length {} is not a multiple of 4",
                            body.len()
                        )));
                    }
                    for chunk in body.chunks_exact(4) {
                        if data.len() >= max_bytes {
                            break;
                        }
                        let block_num =
                            u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
                        let remaining = max_bytes - data.len();
                        let take = (self.block_size as usize).min(remaining);
                        if block_num == 0 {
                            // Sparse block — emit `take` zeros.
                            data.extend(std::iter::repeat_n(0u8, take));
                        } else {
                            let block = self.read_tree_block(block_num)?;
                            data.extend_from_slice(&block[..take]);
                        }
                    }
                }
                ItemType::Direct => {
                    // The body holds raw bytes — for tail-packed files
                    // this is the last (< block_size) chunk; for fully
                    // small files it's the whole file (possibly NUL-
                    // padded out to the slot size, trimmed below).
                    let take = body.len().min(max_bytes - data.len());
                    data.extend_from_slice(&body[..take]);
                }
                ItemType::DirEntry | ItemType::Unknown(_) => {
                    // Not a file item. Skip silently; a directory
                    // surfaced as `read_file` will simply return an
                    // empty body.
                }
            }
        }

        let size = sd_size.ok_or_else(|| {
            FilesystemError::Parse(format!(
                "reiserfs: read_file({dir_id}, {objectid}) found no StatData item"
            ))
        })?;
        // Truncate to the file's logical size (drops the NUL padding
        // that tail-packed Direct items carry inside their 8-byte
        // slots) and respect the caller's max_bytes ceiling.
        let target = (size as usize).min(max_bytes);
        if data.len() > target {
            data.truncate(target);
        }
        Ok(data)
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
///
/// ReiserFS reserves level 0 for "free / unused" blocks; the smallest level
/// a real tree node carries is 1 (leaves). Internal nodes have level >= 2.
/// `s_tree_height` in the superblock counts up to the root's level.
///
/// Verified against the kernel header (`DISK_LEAF_NODE_LEVEL = 1`) and
/// `debugreiserfs -d` on a freshly-formatted single-leaf v3.6 volume.
pub const LEAF_LEVEL: u16 = 1;

/// Highest tree level we'll honour during a descent. Matches
/// `MAX_HEIGHT = 7` from the reiserfs kernel; anything taller is a sign
/// of a corrupt internal node we should refuse rather than chase.
pub const MAX_TREE_LEVEL: u16 = 7;

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
///
/// Important: directory items keep V1 keys (`version == 0`) even on a
/// v3.6 volume — only StatData / Indirect / Direct items get V2 keys
/// (`version == 1`) when the volume is v3.6. This is enforced by
/// `mkfs.reiserfs --format 3.6` and confirmed by `debugreiserfs` on our
/// fixture (DIR items show `format old`, SD/IND/DRCT show `format new`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyFormat {
    V1,
    V2,
}

impl KeyFormat {
    /// Translate an [`ItemHead::version`] byte into the matching key
    /// format. Kernel convention (`reiserfs_fs.h`):
    ///   * `ITEM_VERSION_1 = 0`  → V1 keys (3.5 layout: u32 offset + u32 type)
    ///   * `ITEM_VERSION_2 = 1`  → V2 keys (3.6 layout: packed u64 with
    ///     60-bit offset + 4-bit type)
    ///
    /// Any other value indicates a newer item format we don't yet handle;
    /// we surface a clear error rather than silently mis-parse.
    pub fn from_version(version: u16) -> Result<Self, FilesystemError> {
        match version {
            0 => Ok(KeyFormat::V1),
            1 => Ok(KeyFormat::V2),
            other => Err(FilesystemError::Parse(format!(
                "reiserfs: unknown item version {other} (expected 0 or 1)"
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
        // Kernel convention: ITEM_VERSION_1 = 0, ITEM_VERSION_2 = 1.
        assert_eq!(KeyFormat::from_version(0).unwrap(), KeyFormat::V1);
        assert_eq!(KeyFormat::from_version(1).unwrap(), KeyFormat::V2);
        match KeyFormat::from_version(99) {
            Err(FilesystemError::Parse(msg)) => assert!(msg.contains("99")),
            other => panic!("expected Parse(99), got {other:?}"),
        }
    }

    #[test]
    fn parses_block_head_leaf_and_internal() {
        let mut block = [0u8; 4096];
        // Leaf: level 1, nr_item 3, free_space 1234.
        block[0..2].copy_from_slice(&1u16.to_le_bytes());
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
    fn block_at_level_zero_is_not_a_leaf() {
        // Level 0 is reserved for free blocks; treating one as a leaf would
        // make a corrupt internal-node child pointer silently dump zero data.
        let mut block = [0u8; 4096];
        block[0..2].copy_from_slice(&0u16.to_le_bytes());
        let head = BlockHead::parse(&block).expect("parse free");
        assert!(!head.is_leaf());
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
        ih_bytes[22..24].copy_from_slice(&1u16.to_le_bytes()); // version = V2 (kernel ITEM_VERSION_2 = 1)

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
        // BlockHead: level 1 (leaf), nr_item 3.
        block[0..2].copy_from_slice(&1u16.to_le_bytes());
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
            block[off + 22..off + 24].copy_from_slice(&1u16.to_le_bytes()); // V2 = ITEM_VERSION_2 = 1
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
        // level = 2 (internal, not a leaf)
        block[0..2].copy_from_slice(&2u16.to_le_bytes());
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
        block[0..2].copy_from_slice(&2u16.to_le_bytes()); // level 2 (internal)
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
        block[0..2].copy_from_slice(&1u16.to_le_bytes()); // level 1 (leaf)
        block[2..4].copy_from_slice(&1u16.to_le_bytes());
        match parse_internal_keys_and_children(&block) {
            Err(FilesystemError::Parse(msg)) => assert!(msg.contains("expected internal")),
            other => panic!("expected Parse, got {other:?}"),
        }
    }

    // ---- R.3b: tree walker (collect_leaf_block_numbers /
    //                         collect_items_for_object) ----

    /// Build a synth volume large enough to host the volume + a handful of
    /// extra blocks for tree nodes. Writes `block_count` to the superblock
    /// and points `s_root_block` (offset 8) at `root_block`. The volume
    /// buffer is sized to `total_blocks * block_size` so block reads up to
    /// `total_blocks - 1` succeed.
    fn synth_volume_with_root(block_count: u32, block_size_field: u16, root_block: u32) -> Vec<u8> {
        let mut buf = synth_volume(MAGIC_V3_6, block_count, 0, block_size_field, None, 2);
        let bs = block_size_field as u64;
        let needed = (block_count as u64) * bs;
        if (buf.len() as u64) < needed {
            buf.resize(needed as usize, 0);
        }
        let sb = REISERFS_SUPERBLOCK_OFFSET as usize;
        buf[sb + 8..sb + 12].copy_from_slice(&root_block.to_le_bytes());
        buf
    }

    /// Write a single 24-byte item head + its body into a leaf block.
    /// `body_loc` is the byte offset of the item body inside the block.
    fn place_item(block: &mut [u8], idx: usize, head: &ItemHead, body: &[u8]) {
        let off = BLOCK_HEAD_SIZE + idx * ITEM_HEAD_SIZE;
        block[off..off + 16].copy_from_slice(&head.key.raw);
        block[off + 16..off + 18].copy_from_slice(&head.free_space_or_entry_count.to_le_bytes());
        block[off + 18..off + 20].copy_from_slice(&head.item_len.to_le_bytes());
        block[off + 20..off + 22].copy_from_slice(&head.item_location.to_le_bytes());
        block[off + 22..off + 24].copy_from_slice(&head.version.to_le_bytes());
        let loc = head.item_location as usize;
        block[loc..loc + body.len()].copy_from_slice(body);
    }

    fn write_leaf_at(
        buf: &mut [u8],
        block_num: u32,
        block_size: u64,
        items: Vec<(ItemHead, Vec<u8>)>,
    ) {
        let block_off = (block_num as u64 * block_size) as usize;
        let block = &mut buf[block_off..block_off + block_size as usize];
        block[0..2].copy_from_slice(&LEAF_LEVEL.to_le_bytes());
        block[2..4].copy_from_slice(&(items.len() as u16).to_le_bytes());
        for (i, (head, body)) in items.iter().enumerate() {
            place_item(block, i, head, body);
        }
    }

    fn write_internal_at(
        buf: &mut [u8],
        block_num: u32,
        block_size: u64,
        level: u16,
        keys: &[[u8; 16]],
        child_blocks: &[u32],
    ) {
        assert_eq!(keys.len() + 1, child_blocks.len(), "B+tree N+1 invariant");
        let block_off = (block_num as u64 * block_size) as usize;
        let block = &mut buf[block_off..block_off + block_size as usize];
        block[0..2].copy_from_slice(&level.to_le_bytes());
        block[2..4].copy_from_slice(&(keys.len() as u16).to_le_bytes());
        let mut off = BLOCK_HEAD_SIZE;
        for k in keys {
            block[off..off + 16].copy_from_slice(k);
            off += 16;
        }
        for cb in child_blocks {
            block[off..off + 4].copy_from_slice(&cb.to_le_bytes());
            block[off + 4..off + 6].copy_from_slice(&(block_size as u16).to_le_bytes());
            off += 8;
        }
    }

    fn ih(
        dir_id: u32,
        objectid: u32,
        off: u64,
        kind_v2: u8,
        item_len: u16,
        item_loc: u16,
    ) -> ItemHead {
        ItemHead {
            key: Key {
                raw: build_v2_key(dir_id, objectid, off, kind_v2),
            },
            free_space_or_entry_count: 0,
            item_len,
            item_location: item_loc,
            // V2 keys live in items with version = ITEM_VERSION_2 = 1
            // (kernel constant). Some 2026 ReiserFS docs incorrectly
            // say 2 — the on-disk byte is 1.
            version: 1,
        }
    }

    #[test]
    fn walks_single_leaf_tree() {
        // root_block points directly at a leaf — the same shape as our
        // real-image fixture (small volume, no internal nodes).
        const BS: u16 = 4096;
        let mut buf = synth_volume_with_root(64, BS, 25);
        // Put a leaf at block 25 with 2 items.
        let bs = BS as u64;
        let items = vec![
            (ih(1, 2, 0, 0, 44, 4000), vec![0xAB; 44]),
            (ih(2, 3, 0, 0, 44, 3950), vec![0xCD; 44]),
        ];
        write_leaf_at(&mut buf, 25, bs, items);

        let mut fs = ReiserFsFilesystem::open(Cursor::new(buf), 0).expect("open");
        let leaves = fs.collect_leaf_block_numbers().expect("walk");
        assert_eq!(leaves, vec![25]);

        let matches = fs.collect_items_for_object(1, 2).expect("collect");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].0.key.dir_id(), 1);
        assert_eq!(matches[0].0.key.objectid(), 2);
        assert_eq!(matches[0].1, vec![0xAB; 44]);

        let no_match = fs.collect_items_for_object(99, 99).expect("collect-empty");
        assert!(no_match.is_empty());
    }

    #[test]
    fn walks_root_with_two_leaf_children() {
        // root (internal, level 2) → leaf[block=30], leaf[block=40].
        const BS: u16 = 4096;
        let mut buf = synth_volume_with_root(64, BS, 20);
        let bs = BS as u64;

        // Two leaves, each with one item.
        write_leaf_at(
            &mut buf,
            30,
            bs,
            vec![(ih(1, 2, 0, 0, 44, 4000), vec![0xAA; 44])],
        );
        write_leaf_at(
            &mut buf,
            40,
            bs,
            vec![(ih(2, 5, 0, 0, 44, 4000), vec![0xBB; 44])],
        );

        // Internal root with one key separating the two children.
        let keys = [build_v2_key(2, 5, 0, 0)];
        write_internal_at(&mut buf, 20, bs, 2, &keys, &[30, 40]);

        let mut fs = ReiserFsFilesystem::open(Cursor::new(buf), 0).expect("open");
        let leaves = fs.collect_leaf_block_numbers().expect("walk");
        // Left-to-right traversal order.
        assert_eq!(leaves, vec![30, 40]);

        let matches = fs.collect_items_for_object(2, 5).expect("collect");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].1, vec![0xBB; 44]);
    }

    #[test]
    fn refuses_zero_child_pointer() {
        const BS: u16 = 4096;
        let mut buf = synth_volume_with_root(64, BS, 20);
        let bs = BS as u64;
        let keys = [build_v2_key(2, 5, 0, 0)];
        // Both children are 0 → corrupt; collect_leaf_block_numbers must
        // refuse rather than chase the boot block.
        write_internal_at(&mut buf, 20, bs, 2, &keys, &[0, 0]);
        let mut fs = ReiserFsFilesystem::open(Cursor::new(buf), 0).expect("open");
        match fs.collect_leaf_block_numbers() {
            Err(FilesystemError::Parse(msg)) => assert!(msg.contains("child pointer is 0")),
            other => panic!("expected Parse, got {other:?}"),
        }
    }

    // ---- R.3b: real-image checks against tests/fixtures/test_reiserfs_v3_6.img.zst ----

    /// Decompress a zstd fixture into a byte vector. Matches the
    /// `load_fixture` pattern used elsewhere (`ntfs.rs`, `xfs/mod.rs`).
    fn load_fixture(name: &str) -> Vec<u8> {
        let path = format!("tests/fixtures/{name}");
        let compressed =
            std::fs::read(&path).unwrap_or_else(|e| panic!("Failed to read fixture {path}: {e}"));
        let mut decoder = zstd::stream::read::Decoder::new(Cursor::new(compressed))
            .unwrap_or_else(|e| panic!("Failed to create zstd decoder for {path}: {e}"));
        let mut output = Vec::new();
        std::io::Read::read_to_end(&mut decoder, &mut output)
            .unwrap_or_else(|e| panic!("Failed to decompress {path}: {e}"));
        output
    }

    #[test]
    fn fixture_v3_6_opens_and_reports_metadata() {
        let img = load_fixture("test_reiserfs_v3_6.img.zst");
        let fs = ReiserFsFilesystem::open(Cursor::new(img), 0).expect("open fixture");
        assert_eq!(fs.fs_type(), "ReiserFS 3.6");
        assert_eq!(fs.volume_label(), Some("reiser36_test"));
        assert_eq!(fs.total_size(), 64 * 1024 * 1024);
        // debugreiserfs -d on this fixture reports `Free blocks: 8166`
        // (16384 - 8218 used). `used_size()` reports the same.
        assert_eq!(fs.used_size(), 8218 * 4096);
    }

    #[test]
    fn fixture_v3_6_walks_to_single_leaf() {
        // The fresh-mkfs + 5-file fixture lives entirely inside one leaf;
        // root_block IS that leaf. Confirms the leaf-detection path
        // (LEAF_LEVEL = 1) before R.3c starts decoding items.
        let img = load_fixture("test_reiserfs_v3_6.img.zst");
        let mut fs = ReiserFsFilesystem::open(Cursor::new(img), 0).expect("open");
        let leaves = fs.collect_leaf_block_numbers().expect("walk");
        assert_eq!(leaves.len(), 1, "expected single-leaf tree");
        assert_eq!(leaves[0], 8211, "root_block per debugreiserfs");
    }

    #[test]
    fn fixture_v3_6_collects_root_dir_items() {
        // debugreiserfs dump shows the root directory has two items:
        //   key (1, 2, 0x0) StatData
        //   key (1, 2, 0x1) DirEntry — the chain of 8 directory entries.
        // collect_items_for_object(1, 2) should surface both, in tree order.
        let img = load_fixture("test_reiserfs_v3_6.img.zst");
        let mut fs = ReiserFsFilesystem::open(Cursor::new(img), 0).expect("open");
        let items = fs
            .collect_items_for_object(1, 2)
            .expect("collect root items");
        assert_eq!(items.len(), 2, "expected SD + DIR_ENTRY for root");

        // SD comes first (offset 0).
        let (sd_head, sd_body) = &items[0];
        assert_eq!(sd_head.key.dir_id(), 1);
        assert_eq!(sd_head.key.objectid(), 2);
        // The SD body is 44 bytes (stat-data v2). Just spot-check size;
        // R.3c will parse the body.
        assert_eq!(sd_head.item_len as usize, 44);
        assert_eq!(sd_body.len(), 44);

        // DIR_ENTRY follows. It uses V1 keys (format old) on a v3.6
        // volume — recorded via ItemHead::version = 0.
        let (dir_head, dir_body) = &items[1];
        assert_eq!(dir_head.version, 0, "DIR items use V1 keys");
        assert_eq!(dir_head.free_space_or_entry_count, 8, "8 entries in root");
        assert_eq!(dir_head.item_len as usize, 216);
        assert_eq!(dir_body.len(), 216);
    }

    #[test]
    fn fixture_v3_6_collects_hello_txt_items() {
        // debugreiserfs: hello.txt lives at (dir_id=2, objectid=4), with
        //   SD len 44 + DRCT len 16 ("Hello, ReiserFS!" zero-padded).
        let img = load_fixture("test_reiserfs_v3_6.img.zst");
        let mut fs = ReiserFsFilesystem::open(Cursor::new(img), 0).expect("open");
        let items = fs.collect_items_for_object(2, 4).expect("collect hello");
        assert_eq!(items.len(), 2);
        let (sd, _) = &items[0];
        assert_eq!(sd.item_len as usize, 44);
        // V2 = ITEM_VERSION_2 = 1 on disk (per kernel `reiserfs_fs.h`).
        assert_eq!(sd.version, 1, "SD uses V2 keys");
        assert_eq!(sd.key_format().unwrap(), KeyFormat::V2);
        let (drct, drct_body) = &items[1];
        assert_eq!(drct.item_len as usize, 16);
        assert_eq!(drct.version, 1);
        assert_eq!(drct.key_format().unwrap(), KeyFormat::V2);
        // The DRCT body is the 16-byte file contents (16 chars + 0-pad).
        assert!(
            drct_body.starts_with(b"Hello, ReiserFS!"),
            "DRCT body should start with the file contents, got {:?}",
            &drct_body[..drct_body.len().min(20)]
        );
    }

    #[test]
    fn fixture_v3_6_collects_large_bin_indirect_item() {
        // large.bin is 24 KiB at (2, 9): SD + IND (one indirect item
        // pointing at 6 unformatted data blocks per the debugreiserfs
        // dump: `[ 8212(6) ]` means 6 consecutive blocks starting at
        // 8212). 24 KiB / 4 KiB = 6 ✓.
        let img = load_fixture("test_reiserfs_v3_6.img.zst");
        let mut fs = ReiserFsFilesystem::open(Cursor::new(img), 0).expect("open");
        let items = fs
            .collect_items_for_object(2, 9)
            .expect("collect large.bin");
        assert_eq!(items.len(), 2);
        let (ind, ind_body) = &items[1];
        // IND item: 6 u32 block pointers = 24 bytes.
        assert_eq!(ind.item_len as usize, 24);
        assert_eq!(ind_body.len(), 24);
        // First pointer should be 8212 per the debugreiserfs dump.
        let first_ptr = u32::from_le_bytes([ind_body[0], ind_body[1], ind_body[2], ind_body[3]]);
        assert_eq!(first_ptr, 8212);
    }

    // ---- R.3c: StatData, DIR_ENTRY parser, list_directory ----

    fn build_new_statdata(mode: u32, size: u64, uid: u32, gid: u32, mtime: u32) -> [u8; 44] {
        let mut b = [0u8; 44];
        b[0..2].copy_from_slice(&(mode as u16).to_le_bytes());
        // attrs(2..4) left zero
        b[4..8].copy_from_slice(&1u32.to_le_bytes()); // nlink
        b[8..16].copy_from_slice(&size.to_le_bytes());
        b[16..20].copy_from_slice(&uid.to_le_bytes());
        b[20..24].copy_from_slice(&gid.to_le_bytes());
        b[28..32].copy_from_slice(&mtime.to_le_bytes());
        b
    }

    #[test]
    fn parses_new_statdata() {
        let body = build_new_statdata(0o100644, 12345, 1000, 100, 1700000000);
        let sd = StatData::parse(&body).expect("parse");
        assert_eq!(sd.mode, 0o100644);
        assert_eq!(sd.size, 12345);
        assert_eq!(sd.uid, 1000);
        assert_eq!(sd.gid, 100);
        assert_eq!(sd.mtime, 1700000000);
    }

    #[test]
    fn parses_old_statdata() {
        let mut b = [0u8; 32];
        b[0..2].copy_from_slice(&(0o100644u32 as u16).to_le_bytes()); // mode
        b[2..4].copy_from_slice(&1u16.to_le_bytes()); // nlink
        b[4..6].copy_from_slice(&500u16.to_le_bytes()); // uid
        b[6..8].copy_from_slice(&100u16.to_le_bytes()); // gid
        b[8..12].copy_from_slice(&999u32.to_le_bytes()); // size
        b[16..20].copy_from_slice(&1700000000u32.to_le_bytes()); // mtime
        let sd = StatData::parse(&b).expect("parse old");
        assert_eq!(sd.mode, 0o100644);
        assert_eq!(sd.size, 999);
        assert_eq!(sd.uid, 500);
        assert_eq!(sd.gid, 100);
        assert_eq!(sd.mtime, 1700000000);
    }

    #[test]
    fn rejects_wrong_size_statdata() {
        match StatData::parse(&[0u8; 16]) {
            Err(FilesystemError::Parse(msg)) => {
                assert!(msg.contains("32 or 44"), "unexpected msg: {msg}")
            }
            other => panic!("expected Parse, got {other:?}"),
        }
    }

    /// Build a DIR_ENTRY item body containing `entries`. Names are packed
    /// 8-aligned at the end (matching mkfs.reiserfs convention).
    fn build_dirent_body(entries: &[(&str, u32, u32)]) -> (Vec<u8>, u16) {
        let n = entries.len();
        let headers_size = n * DEH_SIZE;
        // Compute name slots (8-aligned).
        let mut slot_sizes: Vec<usize> = entries
            .iter()
            .map(|(name, _, _)| name.len().div_ceil(8) * 8)
            .collect();
        // Ensure at least one byte slot (catch zero-length names).
        for s in slot_sizes.iter_mut() {
            if *s == 0 {
                *s = 8;
            }
        }
        let total: usize = headers_size + slot_sizes.iter().sum::<usize>();
        let mut body = vec![0u8; total];

        // Names live at the END of the body, with entry[0] having the
        // HIGHEST location (closest to the end).
        let mut name_offset = total;
        let mut deh_locations = Vec::with_capacity(n);
        for (i, (name, _, _)) in entries.iter().enumerate() {
            name_offset -= slot_sizes[i];
            deh_locations.push(name_offset);
            body[name_offset..name_offset + name.len()].copy_from_slice(name.as_bytes());
        }
        // Now write the headers in array order.
        for (i, (_name, dir_id, objectid)) in entries.iter().enumerate() {
            let off = i * DEH_SIZE;
            // deh_offset: just write i as the hash (not used by parser).
            body[off..off + 4].copy_from_slice(&(i as u32).to_le_bytes());
            body[off + 4..off + 8].copy_from_slice(&dir_id.to_le_bytes());
            body[off + 8..off + 12].copy_from_slice(&objectid.to_le_bytes());
            body[off + 12..off + 14].copy_from_slice(&(deh_locations[i] as u16).to_le_bytes());
            // state bit 2 = visible
            body[off + 14..off + 16].copy_from_slice(&4u16.to_le_bytes());
        }
        (body, n as u16)
    }

    #[test]
    fn parse_dirent_empty() {
        let entries = parse_dirent_item(&[], 0).expect("parse empty");
        assert!(entries.is_empty());
    }

    #[test]
    fn parse_dirent_single() {
        let (body, n) = build_dirent_body(&[("hello", 2, 4)]);
        let entries = parse_dirent_item(&body, n).expect("parse");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "hello");
        assert_eq!(entries[0].deh_dir_id, 2);
        assert_eq!(entries[0].deh_objectid, 4);
    }

    #[test]
    fn parse_dirent_multiple_with_padding() {
        // Three entries — array order is the on-disk order (by hash).
        // First entry has HIGHEST location (latest byte slot near end).
        let (body, n) = build_dirent_body(&[(".", 1, 2), ("..", 0, 1), ("subdir", 2, 5)]);
        let entries = parse_dirent_item(&body, n).expect("parse");
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].name, ".");
        assert_eq!(entries[1].name, "..");
        assert_eq!(entries[2].name, "subdir");
        assert_eq!(entries[2].deh_objectid, 5);
    }

    #[test]
    fn parse_dirent_rejects_overlap_with_header_area() {
        // entry_count=2 means headers occupy bytes 0..32. A name
        // location of 16 would land inside the header area.
        let mut body = vec![0u8; 64];
        // header 0
        body[12..14].copy_from_slice(&16u16.to_le_bytes());
        // header 1
        body[28..30].copy_from_slice(&48u16.to_le_bytes());
        match parse_dirent_item(&body, 2) {
            Err(FilesystemError::Parse(msg)) => {
                assert!(msg.contains("overlaps"), "unexpected msg: {msg}")
            }
            other => panic!("expected Parse, got {other:?}"),
        }
    }

    #[test]
    fn parse_dirent_rejects_location_past_body() {
        let mut body = vec![0u8; 32]; // 1 entry header + 16 bytes name area
        body[12..14].copy_from_slice(&999u16.to_le_bytes());
        match parse_dirent_item(&body, 1) {
            Err(FilesystemError::Parse(msg)) => {
                assert!(msg.contains("past item body"), "unexpected msg: {msg}")
            }
            other => panic!("expected Parse, got {other:?}"),
        }
    }

    #[test]
    fn pack_unpack_loc_roundtrip() {
        for (d, o) in [(1, 2), (5, 999), (u32::MAX, 0), (0, u32::MAX)] {
            assert_eq!(unpack_loc(pack_loc(d, o)), (d, o));
        }
    }

    // ---- R.3c: real-image checks against the v3.6 fixture ----

    #[test]
    fn fixture_v3_6_root_returns_directory() {
        let img = load_fixture("test_reiserfs_v3_6.img.zst");
        let mut fs = ReiserFsFilesystem::open(Cursor::new(img), 0).expect("open");
        let root = fs.root().expect("root");
        assert!(root.is_directory());
        assert_eq!(root.path, "/");
        // (dir_id, objectid) for root = (1, 2).
        assert_eq!(unpack_loc(root.location), (1, 2));
        // Root SD: drwxr-xr-x per debugreiserfs.
        assert!(root.mode.is_some(), "root has SD mode bits");
        let mode = root.mode.unwrap() & 0o7777;
        assert_eq!(mode, 0o755);
    }

    #[test]
    fn fixture_v3_6_list_root_filters_hidden() {
        let img = load_fixture("test_reiserfs_v3_6.img.zst");
        let mut fs = ReiserFsFilesystem::open(Cursor::new(img), 0).expect("open");
        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list root");
        let names: Vec<_> = entries.iter().map(|e| e.name.as_str()).collect();
        // `.`, `..`, `.reiserfs_priv` filtered. Remaining: 5 user entries.
        assert_eq!(entries.len(), 5, "got entries: {names:?}");
        for visible in &["hello.txt", "subdir", "link.txt", "tiny.txt", "large.bin"] {
            assert!(names.contains(visible), "missing {visible} in {names:?}");
        }
        for hidden in &[".", "..", ".reiserfs_priv"] {
            assert!(!names.contains(hidden), "{hidden} should be hidden");
        }
    }

    #[test]
    fn fixture_v3_6_file_entries_carry_metadata() {
        let img = load_fixture("test_reiserfs_v3_6.img.zst");
        let mut fs = ReiserFsFilesystem::open(Cursor::new(img), 0).expect("open");
        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list root");

        let hello = entries.iter().find(|e| e.name == "hello.txt").unwrap();
        assert!(hello.is_file());
        assert_eq!(hello.size, 16); // "Hello, ReiserFS!" = 16 bytes
        assert_eq!(hello.mode.map(|m| m & 0o777), Some(0o644));
        assert!(hello.uid.is_some());
        assert!(hello.gid.is_some());
        assert!(hello.modified.is_some());

        let large = entries.iter().find(|e| e.name == "large.bin").unwrap();
        assert!(large.is_file());
        assert_eq!(large.size, 24576);

        let tiny = entries.iter().find(|e| e.name == "tiny.txt").unwrap();
        assert!(tiny.is_file());
        assert_eq!(tiny.size, 10);
    }

    #[test]
    fn fixture_v3_6_symlink_target() {
        let img = load_fixture("test_reiserfs_v3_6.img.zst");
        let mut fs = ReiserFsFilesystem::open(Cursor::new(img), 0).expect("open");
        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list root");
        let link = entries.iter().find(|e| e.name == "link.txt").unwrap();
        assert!(link.is_symlink());
        assert_eq!(link.size, 9); // "hello.txt"
        assert_eq!(link.symlink_target.as_deref(), Some("hello.txt"));
        assert_eq!(link.mode.map(|m| m & 0o7777), Some(0o777));
    }

    #[test]
    fn fixture_v3_6_subdir_listing() {
        let img = load_fixture("test_reiserfs_v3_6.img.zst");
        let mut fs = ReiserFsFilesystem::open(Cursor::new(img), 0).expect("open");
        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list root");
        let subdir = entries.iter().find(|e| e.name == "subdir").unwrap();
        assert!(subdir.is_directory());
        assert_eq!(subdir.path, "/subdir");
        // Recurse one level.
        let sub_entries = fs.list_directory(subdir).expect("list subdir");
        assert_eq!(sub_entries.len(), 1, "subdir has only nested.txt");
        let nested = &sub_entries[0];
        assert_eq!(nested.name, "nested.txt");
        assert_eq!(nested.path, "/subdir/nested.txt");
        assert_eq!(nested.size, 11); // "nested file"
    }

    // ---- R.3d: read_file ----

    #[test]
    fn fixture_v3_6_read_hello_txt() {
        let img = load_fixture("test_reiserfs_v3_6.img.zst");
        let mut fs = ReiserFsFilesystem::open(Cursor::new(img), 0).expect("open");
        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list");
        let hello = entries.iter().find(|e| e.name == "hello.txt").unwrap();
        let data = fs.read_file(hello, 4096).expect("read hello.txt");
        assert_eq!(data, b"Hello, ReiserFS!");
    }

    #[test]
    fn fixture_v3_6_read_tiny_txt_tail_packed() {
        // tiny.txt is 10 bytes ("tiny bytes") stored fully in a DRCT
        // item whose body is 16 bytes (NUL-padded to 8-byte slot).
        // read_file must truncate to SD.size = 10 so the trailing NULs
        // are dropped.
        let img = load_fixture("test_reiserfs_v3_6.img.zst");
        let mut fs = ReiserFsFilesystem::open(Cursor::new(img), 0).expect("open");
        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list");
        let tiny = entries.iter().find(|e| e.name == "tiny.txt").unwrap();
        let data = fs.read_file(tiny, 1024).expect("read tiny.txt");
        assert_eq!(data, b"tiny bytes");
        assert_eq!(data.len(), 10);
    }

    #[test]
    fn fixture_v3_6_read_large_bin_indirect() {
        // large.bin is 24576 bytes (= 6 * 4096) generated by the python
        // formula `(i * 37 + 11) & 0xFF`. Read it back and check the
        // size + a few sentinel bytes.
        let img = load_fixture("test_reiserfs_v3_6.img.zst");
        let mut fs = ReiserFsFilesystem::open(Cursor::new(img), 0).expect("open");
        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list");
        let large = entries.iter().find(|e| e.name == "large.bin").unwrap();
        let data = fs.read_file(large, 64 * 1024).expect("read large.bin");
        assert_eq!(data.len(), 24576);
        // Byte i = (i * 37 + 11) & 0xFF
        for i in [0usize, 1, 100, 4095, 4096, 12345, 24575] {
            let want = ((i * 37 + 11) & 0xFF) as u8;
            assert_eq!(data[i], want, "mismatch at i={i}");
        }
    }

    #[test]
    fn fixture_v3_6_read_nested_txt_under_subdir() {
        // /subdir/nested.txt is 11 bytes via DRCT, exercising the
        // location-pack round-trip + read on a non-root child.
        let img = load_fixture("test_reiserfs_v3_6.img.zst");
        let mut fs = ReiserFsFilesystem::open(Cursor::new(img), 0).expect("open");
        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list");
        let subdir = entries.iter().find(|e| e.name == "subdir").unwrap();
        let sub_entries = fs.list_directory(subdir).expect("list subdir");
        let nested = sub_entries.iter().find(|e| e.name == "nested.txt").unwrap();
        let data = fs.read_file(nested, 1024).expect("read nested.txt");
        assert_eq!(data, b"nested file");
    }

    #[test]
    fn fixture_v3_6_read_with_max_bytes_caps_output() {
        // max_bytes < SD.size should clip the output. Use large.bin and
        // cap at 100; data must equal the first 100 bytes of the file.
        let img = load_fixture("test_reiserfs_v3_6.img.zst");
        let mut fs = ReiserFsFilesystem::open(Cursor::new(img), 0).expect("open");
        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list");
        let large = entries.iter().find(|e| e.name == "large.bin").unwrap();
        let clipped = fs.read_file(large, 100).expect("read clipped");
        assert_eq!(clipped.len(), 100);
        for (i, &byte) in clipped.iter().enumerate() {
            let want = ((i * 37 + 11) & 0xFF) as u8;
            assert_eq!(byte, want, "clipped mismatch at i={i}");
        }
    }

    #[test]
    fn refuses_out_of_range_child_pointer() {
        const BS: u16 = 4096;
        // 64-block volume, child points at block 999 → past EOF.
        let mut buf = synth_volume_with_root(64, BS, 20);
        let bs = BS as u64;
        let keys = [build_v2_key(2, 5, 0, 0)];
        write_internal_at(&mut buf, 20, bs, 2, &keys, &[30, 999]);
        // Leaf at 30 is valid so the walker descends past root first.
        write_leaf_at(
            &mut buf,
            30,
            bs,
            vec![(ih(1, 2, 0, 0, 44, 4000), vec![0xAA; 44])],
        );
        let mut fs = ReiserFsFilesystem::open(Cursor::new(buf), 0).expect("open");
        match fs.collect_leaf_block_numbers() {
            Err(FilesystemError::Parse(msg)) => assert!(
                msg.contains("exceeds total_blocks"),
                "unexpected error msg: {msg}"
            ),
            other => panic!("expected Parse(out-of-range), got {other:?}"),
        }
    }
}
