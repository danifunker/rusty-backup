//! ReiserFS v3.5 / v3.6 — Tier-A read support (detect + size).
//!
//! Implements the [`Filesystem`] trait far enough to detect a ReiserFS
//! partition, surface its on-disk magic, label, and total/used/free sizes in
//! the inspect tab, and back the partition up byte-for-byte through the
//! existing layout-preserving pipeline. Browse / read_file are **not**
//! implemented yet — see `docs/OPEN-WORK.md` §1.1 R.3 for the S+tree walker.
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
}
