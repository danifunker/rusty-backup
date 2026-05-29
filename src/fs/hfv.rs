//! BasiliskII **HFV** support — flat, partition-less classic-HFS volume images.
//!
//! An `.hfv` (a.k.a. `.HFV`) file is a raw image of a single classic HFS
//! volume with **no** partition table: HFS boot blocks at sector 0, the Master
//! Directory Block at byte 1024, and the HFS volume occupying the whole file.
//! BasiliskII / SheepShaver mount it as a flat image (see
//! `../macemu/BasiliskII/src/disk.cpp` `find_hfs_partition`, which fails to find
//! an APM map and falls back to `num_blocks = filesize / 512`).
//!
//! Reading an HFV needs no new code — the existing superfloppy detection
//! (`partition::detect_superfloppy`) finds the MDB at offset 1024 and the
//! auto-detect factory opens it at offset 0. This module is the **write** side:
//! the constraints every HFV-producing path must honour, plus the primitives
//! that build a flat HFS volume image.
//!
//! ## The hard limits (see docs/basilisk_hfv.md §2)
//!
//! * **Classic HFS only — never HFS+.** A flat HFV is mounted by the 68k ROM's
//!   classic HFS driver. HFS+ in a flat image needs Mac OS 8.1+ plus a driver
//!   shim and is out of scope.
//! * **Total size ≤ 2047 MB.** Classic HFS uses a 16-bit allocation-block count
//!   (`drNmAlBlks`, max 65,535) and classic Mac OS does signed 32-bit byte
//!   arithmetic on volume sizes; a volume ≥ 2 GiB (2^31 bytes) wraps negative
//!   and won't mount / corrupts. The safe ceiling is [`HFV_MAX_BYTES`].
//! * The allocation-block-size floor follows from the size: [`suggest_block_size`]
//!   picks the smallest standard block size keeping `drNmAlBlks ≤ 65,535`.

use std::io::{Cursor, Read, Seek};

use crate::fs::filesystem::{Filesystem, FilesystemError};
use crate::fs::hfs::{create_blank_hfs_sized, HfsFilesystem};
use crate::fs::hfs_clone::{clone_hfs_volume, CloneReport};

/// Maximum byte size of an HFV (classic-HFS) volume: **2047 MB**.
///
/// Just under the 2 GiB (2^31-byte) boundary at which classic Mac OS's signed
/// 32-bit volume arithmetic wraps. Anything at or above this is refused rather
/// than handed to an emulator that would corrupt it.
pub const HFV_MAX_BYTES: u64 = 2047 * 1024 * 1024;

/// Allocation block sizes an HFV may use. Each maps to a 2 GB-class ceiling
/// defined by HFS's 65,535-block `u16`. Canonical list — the HFS expand
/// runner re-exports these so there is a single source of truth.
pub const BLOCK_SIZE_CHOICES: &[u32] = &[4096, 8192, 16384, 32768, 65536];

/// Maximum HFS volume size addressable with a given allocation block size
/// (`65,535 × block_size`).
pub fn max_volume_for_block_size(bs: u32) -> u64 {
    65535u64 * bs as u64
}

/// Smallest [`BLOCK_SIZE_CHOICES`] entry whose ceiling can hold `target_bytes`.
/// Falls back to the largest available size if nothing fits.
pub fn suggest_block_size(target_bytes: u64) -> u32 {
    for &bs in BLOCK_SIZE_CHOICES {
        if max_volume_for_block_size(bs) >= target_bytes {
            return bs;
        }
    }
    *BLOCK_SIZE_CHOICES.last().unwrap()
}

/// Which classic-Mac volume format a candidate source carries. Only
/// [`HfvVolumeKind::Hfs`] is a valid HFV payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HfvVolumeKind {
    /// Classic HFS — the only format a flat HFV may contain.
    Hfs,
    /// HFS+ / HFSX (including HFS-wrapped HFS+). Rejected for HFV output.
    HfsPlus,
}

impl HfvVolumeKind {
    /// Classify from a [`Filesystem::fs_type`] string (`"HFS"`, `"HFS+"`, …).
    pub fn from_fs_type(fs_type: &str) -> Self {
        if fs_type.eq_ignore_ascii_case("HFS") {
            HfvVolumeKind::Hfs
        } else {
            HfvVolumeKind::HfsPlus
        }
    }
}

/// Reject any HFV target that classic Mac OS / BasiliskII couldn't safely
/// mount: a non-HFS payload, or a volume at/above [`HFV_MAX_BYTES`].
///
/// The lower bound (room for boot blocks + MDB + B-trees) is left to
/// [`create_blank_hfs_sized`], which errors precisely for a given block size.
pub fn validate_hfv_target(size_bytes: u64, kind: HfvVolumeKind) -> Result<(), FilesystemError> {
    if kind != HfvVolumeKind::Hfs {
        return Err(FilesystemError::InvalidData(
            "HFV volumes must be classic HFS, not HFS+/HFSX (a flat HFS+ image \
             needs Mac OS 8.1+ and is out of scope)"
                .to_string(),
        ));
    }
    if size_bytes > HFV_MAX_BYTES {
        return Err(FilesystemError::InvalidData(format!(
            "HFV volume size {} bytes exceeds the classic-HFS limit of {} bytes (2047 MB)",
            size_bytes, HFV_MAX_BYTES
        )));
    }
    Ok(())
}

/// Build a blank HFV: a flat classic-HFS volume image of `size_bytes` with the
/// given allocation `block_size` and `volume_name`, and **no** partition table.
/// The result is exactly what BasiliskII expects in a `.hfv` file.
///
/// Validates against [`validate_hfv_target`] first. `block_size` should come
/// from [`suggest_block_size`] unless the caller has a reason to pick another
/// [`BLOCK_SIZE_CHOICES`] value.
pub fn build_blank_hfv(
    size_bytes: u64,
    block_size: u32,
    volume_name: &str,
) -> Result<Vec<u8>, FilesystemError> {
    validate_hfv_target(size_bytes, HfvVolumeKind::Hfs)?;
    // Default B-tree sizing (0,0 -> the 4-block-each floor inside
    // create_blank_hfs_sized) is fine for a blank volume.
    create_blank_hfs_sized(size_bytes, block_size, volume_name, 0, 0)
}

/// Clone an open classic-HFS `source` into a freshly-built, defragmented flat
/// HFV image of `target_size` bytes, returning the image bytes.
///
/// This is the engine primitive behind "export to HFV" and "expand HFV": it
/// builds a blank target sized to comfortably hold the source's catalog and
/// extents B-trees, opens it in memory, and replays the source volume onto it
/// via [`clone_hfs_volume`] (which copies boot blocks, so a bootable source
/// stays bootable — no APM/DDR wrapper required). The `emit_apm_disk_with_hfs`
/// step the `.hda` expand path uses is deliberately skipped: an HFV is the bare
/// volume.
///
/// `target_size` must satisfy [`validate_hfv_target`]; the source must be
/// classic HFS (HFS+ sources are refused).
pub fn clone_into_hfv<RS>(
    source: &mut HfsFilesystem<RS>,
    target_size: u64,
    block_size: u32,
    volume_name: &str,
) -> Result<(Vec<u8>, CloneReport), FilesystemError>
where
    RS: Read + Seek + Send,
{
    if source.fs_type() != "HFS" {
        return Err(FilesystemError::InvalidData(format!(
            "source is {}, not classic HFS; HFV output requires an HFS source",
            source.fs_type()
        )));
    }
    validate_hfv_target(target_size, HfvVolumeKind::Hfs)?;

    let summary = source.volume_summary();
    // Size the target B-trees with the blank-format default as a floor (not
    // just 1.5x the source), so a densely-packed source catalog can't undersize
    // the target and exhaust it mid-clone. See clone_target_btree_sizes.
    let (catalog_min, extents_min) = crate::fs::hfs::clone_target_btree_sizes(
        target_size,
        block_size,
        summary.catalog_file_size,
        summary.extents_file_size,
    );

    let mut target_buf = create_blank_hfs_sized(
        target_size,
        block_size,
        volume_name,
        extents_min,
        catalog_min,
    )?;

    let report = {
        let target_cursor = Cursor::new(&mut target_buf);
        let mut target_fs = HfsFilesystem::open(target_cursor, 0)?;
        clone_hfs_volume(source, &mut target_fs)?
    };

    Ok((target_buf, report))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::hfs::create_blank_hfs;
    use crate::partition::PartitionTable;

    const MIB: u64 = 1024 * 1024;

    #[test]
    fn validate_rejects_hfs_plus() {
        let err = validate_hfv_target(10 * MIB, HfvVolumeKind::HfsPlus).unwrap_err();
        assert!(err.to_string().contains("classic HFS"));
    }

    #[test]
    fn validate_rejects_oversize() {
        // Exactly the cap is allowed; one byte over is not.
        assert!(validate_hfv_target(HFV_MAX_BYTES, HfvVolumeKind::Hfs).is_ok());
        let err = validate_hfv_target(HFV_MAX_BYTES + 1, HfvVolumeKind::Hfs).unwrap_err();
        assert!(err.to_string().contains("2047 MB"));
    }

    #[test]
    fn suggest_block_size_covers_full_range() {
        assert_eq!(suggest_block_size(10 * MIB), 4096);
        // 2047 MB must be coverable by a single choice keeping blocks <= 65535.
        let bs = suggest_block_size(HFV_MAX_BYTES);
        assert!(max_volume_for_block_size(bs) >= HFV_MAX_BYTES);
        assert_eq!(bs, 32768);
    }

    #[test]
    fn build_blank_hfv_detects_and_fscks() {
        let bs = suggest_block_size(16 * MIB);
        let img = build_blank_hfv(16 * MIB, bs, "BlankHFV").unwrap();

        // Re-detects as a partition-less HFS superfloppy.
        let mut cur = Cursor::new(img.clone());
        match PartitionTable::detect(&mut cur).unwrap() {
            PartitionTable::None { fs_hint, .. } => assert_eq!(fs_hint, "HFS"),
            other => panic!("expected HFS superfloppy, got {other:?}"),
        }

        // Opens, names correctly, and 16-bit drNmAlBlks holds the volume.
        let mut fs = HfsFilesystem::open(Cursor::new(img), 0).unwrap();
        assert_eq!(fs.fs_type(), "HFS");
        let summary = fs.volume_summary();
        assert_eq!(summary.volume_name, "BlankHFV");
        assert!(summary.total_blocks > 0); // u16 by construction => <= 65535
        let result = fs.fsck().unwrap();
        assert!(result.errors.is_empty());
    }

    #[test]
    fn build_blank_hfv_refuses_hfs_plus_and_oversize() {
        // Oversize is refused before create_blank touches it.
        assert!(build_blank_hfv(HFV_MAX_BYTES + MIB, 32768, "X").is_err());
    }

    #[test]
    fn clone_into_hfv_round_trips_a_file() {
        use crate::fs::filesystem::{CreateFileOptions, EditableFilesystem};

        // Build a small source HFV and drop a file into it.
        let mut src_img = create_blank_hfs(8 * MIB, 4096, "Source").unwrap();
        {
            let mut efs = HfsFilesystem::open(Cursor::new(&mut src_img), 0).unwrap();
            let root = efs.root().unwrap();
            let payload = b"clone me into an HFV";
            let mut data = Cursor::new(payload.as_slice());
            efs.create_file(
                &root,
                "doc.txt",
                &mut data,
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();
            efs.sync_metadata().unwrap();
        }

        // Clone it into a larger flat HFV.
        let mut source_fs = HfsFilesystem::open(Cursor::new(src_img), 0).unwrap();
        let (hfv, report) = clone_into_hfv(&mut source_fs, 16 * MIB, 4096, "Cloned").unwrap();
        assert_eq!(report.files_copied, 1);

        // The clone is a valid HFV with the file present.
        let mut out = HfsFilesystem::open(Cursor::new(hfv), 0).unwrap();
        assert_eq!(out.volume_summary().volume_name, "Cloned");
        let root = out.root().unwrap();
        let kids = out.list_directory(&root).unwrap();
        assert!(kids.iter().any(|e| e.name == "doc.txt"));
        assert!(out.fsck().unwrap().errors.is_empty());
    }
}
