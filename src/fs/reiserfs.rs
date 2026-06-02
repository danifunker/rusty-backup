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
    #[allow(dead_code)] // R.2 will use this for bitmap reads.
    reader: R,
    #[allow(dead_code)] // R.2 will use this for bitmap reads.
    partition_offset: u64,
    version: ReiserFsVersion,
    block_size: u64,
    total_blocks: u64,
    free_blocks: u64,
    #[allow(dead_code)] // R.2 will use this to size the bitmap walk.
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
