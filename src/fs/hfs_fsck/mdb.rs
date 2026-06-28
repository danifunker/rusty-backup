//! HFS fsck — Phase 1: MDB sanity checks.
//!
//! Validates the Master Directory Block: signature, allocation block size,
//! bitmap/data-area placement, clump sizes, the alternate MDB at the end of
//! the volume, and any embedded HFS+ wrapper region.
//!
//! Split out of `hfs_fsck/mod.rs` per §8 of `docs/codecleanup.md`.

use byteorder::{BigEndian, ByteOrder};

use super::super::fsck::FsckIssue;
use super::super::hfs::HfsMasterDirectoryBlock;
use super::{hfs_issue, HfsFsckCode};

/// Validate an HFS catalog name (Mac Roman bytes) for *structural* validity.
///
/// Returns `Some(problem)` only for genuinely invalid names: empty, longer than
/// the 31-byte HFS limit, or containing a colon (0x3A, the path separator that
/// classic Mac OS forbids). Null and other control bytes are deliberately NOT
/// rejected — classic HFS permits any byte except the colon, and real disks
/// carry such names (a leading null forces a file to the top of the sort, an
/// old Finder trick). Those surface separately via [`unusual_hfs_name`].
pub(super) fn validate_hfs_name(name: &[u8]) -> Option<String> {
    if name.is_empty() {
        return Some("name is empty".into());
    }
    if name.len() > 31 {
        return Some(format!(
            "name length {} exceeds HFS maximum of 31",
            name.len()
        ));
    }
    if name.contains(&0x3A) {
        return Some("name contains colon (HFS path separator)".into());
    }
    None
}

/// A structurally-valid catalog name that is unusual enough to surface as a
/// *warning* (not an error). Returns `Some(note)` when the name embeds a null
/// byte — valid on classic HFS, but worth flagging since most tooling assumes
/// C-string names.
pub(super) fn unusual_hfs_name(name: &[u8]) -> Option<String> {
    if name.contains(&0x00) {
        return Some("name contains a null byte (valid on classic HFS, but unusual)".into());
    }
    None
}

/// Validate the alternate MDB against the primary MDB.
pub(super) fn check_alternate_mdb(
    mdb: &HfsMasterDirectoryBlock,
    alt_sector: &[u8; 512],
    errors: &mut Vec<FsckIssue>,
) {
    let alt_sig = BigEndian::read_u16(&alt_sector[0..2]);
    if alt_sig != 0x4244 {
        // The alternate MDB is a backup copy whose location we calculate as
        // right after the allocation area.  When the partition is larger than
        // the filesystem's allocation blocks the real alternate MDB sits at
        // the partition's penultimate sector — a location we can't determine
        // without the partition size.  Since we can't tell "missing" from
        // "looked in the wrong place", silently skip the cross-check.
        return;
    }

    // Cross-check critical fields. The triple is (label, accessor, expected);
    // hoisting to a named struct would be more code than it's worth here.
    #[allow(clippy::type_complexity)]
    let checks: &[(&str, fn(&[u8; 512]) -> u32, u32)] = &[
        (
            "block size",
            |s| BigEndian::read_u32(&s[20..24]),
            mdb.block_size,
        ),
        (
            "total blocks",
            |s| BigEndian::read_u16(&s[18..20]) as u32,
            mdb.total_blocks as u32,
        ),
        (
            "volume bitmap start",
            |s| BigEndian::read_u16(&s[14..16]) as u32,
            mdb.volume_bitmap_block as u32,
        ),
        (
            "first alloc block start",
            |s| BigEndian::read_u16(&s[28..30]) as u32,
            mdb.first_alloc_block as u32,
        ),
    ];
    for (field, read_fn, primary_val) in checks {
        let alt_val = read_fn(alt_sector);
        if alt_val != *primary_val {
            errors.push(hfs_issue(
                HfsFsckCode::AlternateMdbMismatch,
                format!(
                    "alternate MDB {} ({}) differs from primary ({})",
                    field, alt_val, primary_val
                ),
            ));
        }
    }
}

/// Check for embedded HFS+ volume inside an HFS wrapper.
pub(super) fn check_embedded_hfs_plus(
    mdb: &HfsMasterDirectoryBlock,
    errors: &mut Vec<FsckIssue>,
    warnings: &mut Vec<FsckIssue>,
) {
    if mdb.embedded_signature == 0 {
        return; // No embedded volume
    }
    if mdb.embedded_signature == 0x482B {
        // Valid HFS+ embedded signature — check extents are within bounds
        let start = mdb.embedded_start_block as u32;
        let count = mdb.embedded_block_count as u32;
        if start + count > mdb.total_blocks as u32 {
            errors.push(hfs_issue(
                HfsFsckCode::EmbeddedHfsPlusInvalid,
                format!(
                    "embedded HFS+ region [{}, +{}) exceeds total blocks {}",
                    start, count, mdb.total_blocks
                ),
            ));
        } else {
            warnings.push(hfs_issue(
                HfsFsckCode::EmbeddedHfsPlusInvalid,
                format!(
                    "HFS wrapper contains embedded HFS+ volume (blocks {}..{})",
                    start,
                    start + count
                ),
            ));
        }
    } else {
        errors.push(hfs_issue(
            HfsFsckCode::EmbeddedHfsPlusInvalid,
            format!(
                "unknown embedded volume signature 0x{:04X}",
                mdb.embedded_signature
            ),
        ));
    }
}

pub(super) fn check_mdb(mdb: &HfsMasterDirectoryBlock, errors: &mut Vec<FsckIssue>) {
    if mdb.signature != 0x4244 {
        errors.push(hfs_issue(
            HfsFsckCode::BadSignature,
            format!(
                "bad MDB signature: 0x{:04X} (expected 0x4244)",
                mdb.signature
            ),
        ));
    }

    // HFS block size must be a positive multiple of 512
    #[allow(clippy::manual_is_multiple_of)]
    if mdb.block_size == 0 || mdb.block_size % 512 != 0 {
        errors.push(hfs_issue(
            HfsFsckCode::BadBlockSize,
            format!(
                "bad allocation block size: {} (must be a positive multiple of 512)",
                mdb.block_size
            ),
        ));
    }

    // Gap 8: Block size upper bound (Apple Max_ABSiz = 0x7FFFFE00, ~2GB)
    if mdb.block_size > 0x7FFF_FE00 {
        errors.push(hfs_issue(
            HfsFsckCode::BlockSizeUpperBound,
            format!(
                "allocation block size 0x{:08X} exceeds HFS maximum (0x7FFFFE00)",
                mdb.block_size
            ),
        ));
    }

    // Gap 5: Total allocation blocks sanity
    if mdb.total_blocks == 0 && mdb.block_size > 0 {
        errors.push(hfs_issue(
            HfsFsckCode::MdbTotalBlocksInvalid,
            "total allocation blocks is 0".to_string(),
        ));
    }

    // Gap 7: Volume bitmap must start at sector 3 or later
    // (sectors 0-1 are boot blocks, sector 2 is the MDB)
    if mdb.volume_bitmap_block < 3 {
        errors.push(hfs_issue(
            HfsFsckCode::MdbVBMStTooLow,
            format!(
                "volume bitmap start sector {} is too low (must be >= 3)",
                mdb.volume_bitmap_block
            ),
        ));
    }

    // Volume bitmap must not collide with the allocation data area.
    // VBM starts at sector drVBMSt and occupies ceil(total_blocks / (512*8)) sectors.
    // The data area starts at sector drAlBlSt.
    if mdb.block_size > 0 && mdb.total_blocks > 0 {
        let bits_per_sector: u32 = 512 * 8; // 4096 bits per 512-byte sector
        let vbm_sectors = (mdb.total_blocks as u32).div_ceil(bits_per_sector);
        let vbm_end = mdb.volume_bitmap_block as u32 + vbm_sectors;
        if vbm_end > mdb.first_alloc_block as u32 {
            errors.push(hfs_issue(
                HfsFsckCode::VbmDataAreaCollision,
                format!(
                    "volume bitmap (sectors {}..{}) overlaps data area (starts at sector {})",
                    mdb.volume_bitmap_block, vbm_end, mdb.first_alloc_block
                ),
            ));
        }

        // Gap 6: First alloc block start must be past the VBM
        if (mdb.first_alloc_block as u32) < vbm_end {
            // Only report if not already covered by VbmDataAreaCollision
            // (VbmDataAreaCollision checks overlap; this is the same condition)
            // Already reported above — skip to avoid duplicate.
        } else if mdb.first_alloc_block < 3 {
            errors.push(hfs_issue(
                HfsFsckCode::MdbAlBlStTooLow,
                format!(
                    "first allocation block start sector {} is too low (must be >= 3)",
                    mdb.first_alloc_block
                ),
            ));
        }
    } else if mdb.first_alloc_block < 3 {
        // Gap 6: Even without valid block_size/total_blocks, catch obviously bad values
        errors.push(hfs_issue(
            HfsFsckCode::MdbAlBlStTooLow,
            format!(
                "first allocation block start sector {} is too low (must be >= 3)",
                mdb.first_alloc_block
            ),
        ));
    }

    // Gap 9: Clump size validation
    // Read from raw_sector since these aren't in the parsed struct fields.
    if mdb.block_size > 0 && mdb.total_blocks > 0 {
        let volume_size = mdb.total_blocks as u64 * mdb.block_size as u64;
        let clump_checks: [(&str, usize); 3] = [
            ("default clump size (drClpSiz)", 24),
            ("extents file clump size (drXTClpSiz)", 74),
            ("catalog file clump size (drCTClpSiz)", 78),
        ];
        for (label, offset) in &clump_checks {
            let clump = BigEndian::read_u32(&mdb.raw_sector[*offset..*offset + 4]);
            if clump == 0 {
                continue; // zero is acceptable (means use default)
            }
            #[allow(clippy::manual_is_multiple_of)]
            if clump % mdb.block_size != 0 {
                errors.push(hfs_issue(
                    HfsFsckCode::InvalidClumpSize,
                    format!(
                        "{}: {} is not a multiple of block size {}",
                        label, clump, mdb.block_size
                    ),
                ));
            }
            if clump as u64 > volume_size {
                errors.push(hfs_issue(
                    HfsFsckCode::InvalidClumpSize,
                    format!("{}: {} exceeds volume size {}", label, clump, volume_size),
                ));
            }
        }
    }
}
