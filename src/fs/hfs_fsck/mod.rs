//! HFS filesystem checker (fsck) — read-only integrity verification.
//!
//! Validates the HFS volume in four phases:
//! 1. MDB sanity checks
//! 2. B-tree structure verification
//! 3. Catalog consistency (threads ↔ records, counts)
//! 4. Extent/allocation bitmap cross-check (including extents overflow)

use byteorder::{BigEndian, ByteOrder};

use super::fsck::{FsckIssue, FsckResult, FsckStats, RepairReport};
use super::hfs::HfsMasterDirectoryBlock;
use super::hfs_common::BTreeHeader;

pub(super) const CATALOG_DIR_THREAD: i8 = 3;
pub(super) const CATALOG_FILE_THREAD: i8 = 4;

mod bitmap;
mod btree;
mod catalog;
mod extents;
mod mdb;

use bitmap::{check_extents_and_bitmap, repair_extents, repair_mdb_counts_and_bitmap};
use btree::{
    check_btree_structure, fix_leaf_record_count, repair_btree_structure_leaves_only,
    repair_node_bitmap,
};
use catalog::{check_catalog_consistency, repair_catalog_consistency};
use mdb::{check_alternate_mdb, check_embedded_hfs_plus, check_mdb};

// Re-export for external callers (hfs.rs uses `super::hfs_fsck::rebuild_index_nodes`).
pub(crate) use btree::rebuild_index_nodes;

// ---------------------------------------------------------------------------
// HFS-specific issue codes and repair classification
// ---------------------------------------------------------------------------

/// HFS-specific issue codes used internally for repair classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HfsFsckCode {
    // MDB issues
    BadSignature,
    AlternateMdbMismatch,
    EmbeddedHfsPlusInvalid,
    BadBlockSize,
    BlockSizeUpperBound,
    VbmDataAreaCollision,
    MdbVBMStTooLow,
    MdbAlBlStTooLow,
    MdbTotalBlocksInvalid,
    InvalidClumpSize,
    NextCatalogIdTooLow,
    // B-tree structure
    HeaderNodeBadKind,
    RootNodeBadKind,
    MapNodeBadStructure,
    NodeBitmapMissing,
    KeysOutOfOrder,
    OffsetTableNotMonotonic,
    OffsetTableOutOfBounds,
    LeafChainBroken,
    LeafBacklinkMismatch,
    LeafRecordCountMismatch,
    IndexSiblingLinkBroken,
    InvalidRecordLength,
    // Catalog consistency
    MissingDirThread,
    ThreadParentNameMismatch,
    FileCountMismatch,
    FolderCountMismatch,
    OrphanedThread,
    MissingParent,
    ValenceMismatch,
    // Directory structure
    DirectoryLoop,
    NestingDepthWarning,
    // Catalog record field validation
    LeoFExceedsPeoF,
    InvalidCnidRange,
    ReservedFieldNonZero,
    InvalidCatalogName,
    ThreadNameNotEmpty,
    // Extents overflow B-tree structure
    ExtentsBtreeStructure,
    // Extent/allocation
    BadBlockExtentNotInBitmap,
    ExtentOutOfRange,
    OverlappingExtents,
    BitmapMismatch,
    FreeBlockCountMismatch,
}

/// Whether an HFS issue code is automatically repairable.
fn is_repairable(code: HfsFsckCode) -> bool {
    !matches!(
        code,
        HfsFsckCode::BadBlockSize
            | HfsFsckCode::BlockSizeUpperBound
            | HfsFsckCode::VbmDataAreaCollision
            | HfsFsckCode::MdbVBMStTooLow
            | HfsFsckCode::MdbAlBlStTooLow
            | HfsFsckCode::MdbTotalBlocksInvalid
            | HfsFsckCode::InvalidClumpSize
            | HfsFsckCode::EmbeddedHfsPlusInvalid
            | HfsFsckCode::DirectoryLoop
            | HfsFsckCode::NestingDepthWarning
            | HfsFsckCode::LeoFExceedsPeoF
            | HfsFsckCode::InvalidCnidRange
            | HfsFsckCode::InvalidCatalogName
            | HfsFsckCode::OffsetTableNotMonotonic
            | HfsFsckCode::OffsetTableOutOfBounds
            | HfsFsckCode::MissingParent
    )
}

/// Build a shared `FsckIssue` from an HFS-specific code and message.
pub(super) fn hfs_issue(code: HfsFsckCode, message: impl Into<String>) -> FsckIssue {
    FsckIssue {
        code: format!("{:?}", code),
        message: message.into(),
        repairable: is_repairable(code),
        debug: false,
    }
}

/// Run a full integrity check on a classic HFS volume.
///
/// `mdb` — parsed Master Directory Block.
/// `catalog_data` — full catalog B-tree file content.
/// `bitmap` — volume allocation bitmap bytes.
/// `extents_data` — optional extents overflow B-tree file content (for files with >3 extents).
/// `alt_mdb_sector` — optional raw 512-byte alternate MDB sector (last sector of volume).
pub(crate) fn check_hfs_integrity(
    mdb: &HfsMasterDirectoryBlock,
    catalog_data: &[u8],
    bitmap: &[u8],
    extents_data: Option<&[u8]>,
    alt_mdb_sector: Option<&[u8; 512]>,
) -> FsckResult {
    let mut errors = Vec::new();
    let mut warnings = Vec::new();
    let mut leaf_nodes_visited = 0u32;
    let mut orphaned_threads = 0u32;
    let mut files_checked = 0u32;
    let mut directories_checked = 0u32;

    // Phase 1: MDB sanity
    check_mdb(mdb, &mut errors);

    // If signature is bad, the rest of the check is meaningless
    if errors.iter().any(|e| e.code == "BadSignature") {
        let repairable = errors.iter().any(|e| e.repairable);
        return FsckResult {
            errors,
            warnings,
            stats: build_stats(
                files_checked,
                directories_checked,
                leaf_nodes_visited,
                orphaned_threads,
            ),
            repairable,
            orphaned_entries: Vec::new(),
        };
    }

    // Phase 1b: Alternate MDB cross-check
    if let Some(alt_sector) = alt_mdb_sector {
        check_alternate_mdb(mdb, alt_sector, &mut errors);
    }

    // Phase 1c: Embedded HFS+ wrapper check
    check_embedded_hfs_plus(mdb, &mut errors, &mut warnings);

    // Phase 2: B-tree structure
    //
    // A blank HFS volume (no files) may have an empty or minimal catalog B-tree.
    // If the volume reports zero files and zero folders and the catalog data is
    // too small to contain a valid B-tree header, treat this as a valid empty
    // volume rather than an error.
    let is_empty_volume = mdb.file_count == 0 && mdb.folder_count == 0;

    if catalog_data.len() < 44 {
        // Need at least 44 bytes to read the B-tree header (14-byte node descriptor
        // + 30 bytes of header record fields).
        if !is_empty_volume {
            errors.push(hfs_issue(
                HfsFsckCode::HeaderNodeBadKind,
                "catalog B-tree data too small to contain a header",
            ));
        }
        let repairable = errors.iter().any(|e| e.repairable);
        return FsckResult {
            errors,
            warnings,
            stats: build_stats(
                files_checked,
                directories_checked,
                leaf_nodes_visited,
                orphaned_threads,
            ),
            repairable,
            orphaned_entries: Vec::new(),
        };
    }

    let header = BTreeHeader::read(catalog_data);
    let node_size = header.node_size as usize;
    if node_size == 0 || catalog_data.len() < node_size {
        if !is_empty_volume {
            errors.push(hfs_issue(
                HfsFsckCode::HeaderNodeBadKind,
                "catalog B-tree node size is zero or catalog data too small",
            ));
        }
        let repairable = errors.iter().any(|e| e.repairable);
        return FsckResult {
            errors,
            warnings,
            stats: build_stats(
                files_checked,
                directories_checked,
                leaf_nodes_visited,
                orphaned_threads,
            ),
            repairable,
            orphaned_entries: Vec::new(),
        };
    }
    check_btree_structure(catalog_data, &header, &mut errors, &mut leaf_nodes_visited);

    // Phase 3: Catalog consistency
    let mut orphaned_entries = Vec::new();
    check_catalog_consistency(
        mdb,
        catalog_data,
        &header,
        &mut errors,
        &mut warnings,
        &mut files_checked,
        &mut directories_checked,
        &mut orphaned_threads,
        &mut orphaned_entries,
    );

    // Phase 3.5: Extents overflow B-tree structure
    if let Some(ext_data) = extents_data {
        extents::check_extents_btree_structure(ext_data, &mut errors);
    }

    // Phase 4: Extent/allocation bitmap
    check_extents_and_bitmap(
        mdb,
        catalog_data,
        &header,
        bitmap,
        extents_data,
        &mut errors,
        &mut warnings,
    );

    let repairable = errors.iter().any(|e| e.repairable);
    FsckResult {
        errors,
        warnings,
        stats: build_stats(
            files_checked,
            directories_checked,
            leaf_nodes_visited,
            orphaned_threads,
        ),
        repairable,
        orphaned_entries,
    }
}

fn build_stats(files: u32, dirs: u32, leaf_nodes: u32, orphaned_threads: u32) -> FsckStats {
    let mut extra = vec![("Leaf nodes visited".into(), leaf_nodes.to_string())];
    if orphaned_threads > 0 {
        extra.push(("Orphaned threads".into(), orphaned_threads.to_string()));
    }
    FsckStats {
        files_checked: files,
        directories_checked: dirs,
        extra,
    }
}

// ---------------------------------------------------------------------------
// Repair
// ---------------------------------------------------------------------------

/// Repair repairable HFS volume issues in-place on the in-memory structures.
///
/// After this returns, the caller must write the modified `mdb`, `catalog_data`,
/// and `bitmap` back to disk.
pub(crate) fn repair_hfs(
    mdb: &mut HfsMasterDirectoryBlock,
    catalog_data: &mut [u8],
    bitmap: &mut Vec<u8>,
    mut extents_data: Option<&mut Vec<u8>>,
) -> RepairReport {
    // Count unrepairable issues by running a quick check first
    let check_result = check_hfs_integrity(
        mdb,
        catalog_data,
        bitmap,
        extents_data.as_deref().map(|v| v.as_slice()),
        None, // alt MDB not available during repair
    );
    let unrepairable_count = check_result.errors.iter().filter(|e| !e.repairable).count();

    let mut report = RepairReport {
        fixes_applied: Vec::new(),
        fixes_failed: Vec::new(),
        unrepairable_count,
    };

    // --- Phase 0: MDB signature ---
    if mdb.signature != 0x4244 {
        mdb.signature = 0x4244;
        // Also patch raw_sector so serialize_to_sector is correct
        BigEndian::write_u16(&mut mdb.raw_sector[0..2], 0x4244);
        report
            .fixes_applied
            .push("Fixed MDB signature to 0x4244".into());
    }

    // Need a valid node_size to proceed.  On a blank volume (no files, no
    // folders) the catalog may be empty/zeroed — nothing to repair.
    let is_empty_volume = mdb.file_count == 0 && mdb.folder_count == 0;
    if catalog_data.len() < 44 {
        if !is_empty_volume {
            report
                .fixes_failed
                .push("Cannot repair: catalog B-tree data too small to contain a header".into());
        }
        return report;
    }
    let header = BTreeHeader::read(catalog_data);
    let node_size = header.node_size as usize;
    if node_size == 0 || catalog_data.len() < node_size {
        if !is_empty_volume {
            report
                .fixes_failed
                .push("Cannot repair: B-tree node size is zero or catalog data too small".into());
        }
        return report;
    }

    // --- Phase 1: Initial B-tree structure fixes (leaf chain only) ---
    // Fix header/root node kinds and rebuild the leaf chain so catalog
    // consistency repairs can find all records. Index nodes are NOT rebuilt
    // yet — that must happen AFTER catalog modifications.
    repair_btree_structure_leaves_only(catalog_data, node_size, &mut report);

    // --- Phase 2: Catalog consistency fixes ---
    // This may insert/remove leaf records (threads, orphans, valence).
    repair_catalog_consistency(mdb, catalog_data, node_size, &mut report);

    // --- Phase 3: Rebuild B-tree index nodes and fix header counts ---
    // Now that all catalog modifications are done, rebuild the full B-tree
    // structure: index nodes, sibling links, and header record counts.
    rebuild_index_nodes(catalog_data, node_size, &mut report);
    fix_leaf_record_count(catalog_data, &mut report);

    // Fix node bitmap after all structural changes are finalized
    repair_node_bitmap(catalog_data, node_size, &mut report);

    // --- Phase 3.5: Extents overflow B-tree structure repair ---
    if let Some(ext) = extents_data.as_mut() {
        extents::repair_extents_btree_structure(ext, &mut report);
    }

    // --- Phase 4: Extent fixes ---
    repair_extents(
        mdb,
        catalog_data,
        node_size,
        extents_data.as_deref().map(|v| v.as_slice()),
        &mut report,
    );

    // --- Phase 5: MDB counts + bitmap rebuild ---
    repair_mdb_counts_and_bitmap(
        mdb,
        catalog_data,
        bitmap,
        extents_data.as_deref().map(|v| v.as_slice()),
        &mut report,
    );

    report
}

#[cfg(test)]
mod tests {
    use super::super::hfs_common::{
        bitmap_set_bit_be, BTREE_HEADER_NODE, BTREE_INDEX_NODE, BTREE_LEAF_NODE, BTREE_MAP_NODE,
    };
    use super::btree::{
        check_key_ordering, check_map_nodes, check_node_bitmap_consistency, repair_key_ordering,
        repair_map_nodes, repair_node_bitmap,
    };
    use std::collections::HashMap;

    use super::super::hfs::{HfsExtDescriptor, CATALOG_DIR, CATALOG_FILE};
    use super::super::hfs_common::btree_record_range;
    use super::catalog::check_directory_structure;
    use super::mdb::validate_hfs_name;
    use super::*;

    #[test]
    fn test_fsck_code_to_string() {
        let issue = hfs_issue(HfsFsckCode::BadSignature, "test");
        assert_eq!(issue.code, "BadSignature");
        let issue = hfs_issue(HfsFsckCode::LeafChainBroken, "test");
        assert_eq!(issue.code, "LeafChainBroken");
    }

    #[test]
    fn test_fsck_result_is_clean() {
        let result = FsckResult {
            errors: vec![],
            warnings: vec![hfs_issue(HfsFsckCode::BitmapMismatch, "minor")],
            stats: build_stats(0, 0, 0, 0),
            repairable: false,
            orphaned_entries: vec![],
        };
        assert!(result.is_clean()); // warnings don't count

        let result_err = FsckResult {
            errors: vec![hfs_issue(HfsFsckCode::BadSignature, "bad")],
            warnings: vec![],
            stats: build_stats(0, 0, 0, 0),
            repairable: true,
            orphaned_entries: vec![],
        };
        assert!(!result_err.is_clean());
    }

    #[test]
    fn test_block_size_multiple_of_512() {
        // 8704 = 17 × 512 — valid HFS block size
        let mut mdb = make_test_mdb();
        mdb.block_size = 8704;
        let mut errors = Vec::new();
        check_mdb(&mdb, &mut errors);
        assert!(
            errors.is_empty(),
            "8704 should be valid: {:?}",
            errors
                .iter()
                .map(|e| e.message.as_str())
                .collect::<Vec<_>>()
        );

        // Not a multiple of 512
        mdb.block_size = 1000;
        errors.clear();
        check_mdb(&mdb, &mut errors);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].code, "BadBlockSize");

        // Zero is invalid
        mdb.block_size = 0;
        errors.clear();
        check_mdb(&mdb, &mut errors);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].code, "BadBlockSize");
    }

    fn make_test_mdb() -> HfsMasterDirectoryBlock {
        HfsMasterDirectoryBlock {
            signature: 0x4244,
            create_date: 0,
            modify_date: 0,
            total_blocks: 100,
            block_size: 4096,
            free_blocks: 50,
            volume_name: "Test".into(),
            volume_name_raw: b"Test".to_vec(),
            volume_bitmap_block: 3,
            first_alloc_block: 5,
            next_catalog_id: 16,
            file_count: 0,
            folder_count: 0, // HFS drDirCnt excludes root directory
            finder_info: [0; 8],
            catalog_file_size: 0,
            catalog_file_extents: [HfsExtDescriptor {
                start_block: 0,
                block_count: 0,
            }; 3],
            extents_file_size: 0,
            extents_file_extents: [HfsExtDescriptor {
                start_block: 0,
                block_count: 0,
            }; 3],
            embedded_signature: 0,
            embedded_start_block: 0,
            embedded_block_count: 0,
            raw_sector: [0; 512],
        }
    }

    #[test]
    fn test_vbm_no_collision() {
        // Default test MDB: bitmap at sector 3, total_blocks=100.
        // VBM needs ceil(100 / 4096) = 1 sector. VBM occupies sector 3..4.
        // Data area starts at sector 5. No collision.
        let mdb = make_test_mdb();
        let mut errors = Vec::new();
        check_mdb(&mdb, &mut errors);
        assert!(
            errors.is_empty(),
            "no VBM collision expected: {:?}",
            errors
                .iter()
                .map(|e| e.message.as_str())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_vbm_collision_detected() {
        let mut mdb = make_test_mdb();
        // Bitmap at sector 3, total_blocks=50000 → needs ceil(50000/4096) = 13 sectors
        // VBM occupies sectors 3..16, but data area starts at sector 5 → collision
        mdb.total_blocks = 50000;
        let mut errors = Vec::new();
        check_mdb(&mdb, &mut errors);
        assert!(
            errors.iter().any(|e| e.code == "VbmDataAreaCollision"),
            "should detect VBM collision: {:?}",
            errors
                .iter()
                .map(|e| e.message.as_str())
                .collect::<Vec<_>>()
        );
        // VBM collision is non-repairable
        assert!(
            !errors
                .iter()
                .find(|e| e.code == "VbmDataAreaCollision")
                .unwrap()
                .repairable
        );
    }

    // -----------------------------------------------------------------------
    // Stage 1: Extended MDB Validation tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_mdb_vbm_start_too_low() {
        let mut mdb = make_test_mdb();
        mdb.volume_bitmap_block = 1; // must be >= 3
        let mut errors = Vec::new();
        check_mdb(&mdb, &mut errors);
        assert!(
            errors.iter().any(|e| e.code == "MdbVBMStTooLow"),
            "should detect VBM start too low: {:?}",
            errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_mdb_vbm_start_at_3_ok() {
        let mdb = make_test_mdb(); // volume_bitmap_block = 3
        let mut errors = Vec::new();
        check_mdb(&mdb, &mut errors);
        assert!(
            !errors.iter().any(|e| e.code == "MdbVBMStTooLow"),
            "VBM start at 3 should be ok"
        );
    }

    #[test]
    fn test_mdb_alblst_too_low() {
        let mut mdb = make_test_mdb();
        mdb.first_alloc_block = 2; // must be >= 3 at minimum
        mdb.volume_bitmap_block = 3;
        mdb.total_blocks = 1; // tiny volume so VBM is 1 sector (3..4)
        let mut errors = Vec::new();
        check_mdb(&mdb, &mut errors);
        // first_alloc_block=2 < vbm_end=4, triggers VbmDataAreaCollision
        // and first_alloc_block=2 < 3 triggers MdbAlBlStTooLow
        // Due to the overlap check being reported first, AlBlSt<3 is also caught
        // in the else-if fallback when total_blocks is 0. But with total_blocks=1,
        // the VBM collision covers it. Let's test with total_blocks=0 path.
        let mut mdb2 = make_test_mdb();
        mdb2.first_alloc_block = 2;
        mdb2.total_blocks = 0;
        mdb2.block_size = 0; // skip VBM collision path
        let mut errors2 = Vec::new();
        check_mdb(&mdb2, &mut errors2);
        assert!(
            errors2.iter().any(|e| e.code == "MdbAlBlStTooLow"),
            "should detect first alloc block too low: {:?}",
            errors2.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_mdb_block_size_upper_bound() {
        let mut mdb = make_test_mdb();
        mdb.block_size = 0x8000_0000;
        let mut errors = Vec::new();
        check_mdb(&mdb, &mut errors);
        assert!(
            errors.iter().any(|e| e.code == "BlockSizeUpperBound"),
            "should detect block size exceeding max: {:?}",
            errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_mdb_block_size_at_max_valid() {
        let mut mdb = make_test_mdb();
        mdb.block_size = 0x7FFF_FE00;
        let mut errors = Vec::new();
        check_mdb(&mdb, &mut errors);
        assert!(
            !errors.iter().any(|e| e.code == "BlockSizeUpperBound"),
            "block size at max should not trigger upper bound error"
        );
    }

    #[test]
    fn test_mdb_total_blocks_zero() {
        let mut mdb = make_test_mdb();
        mdb.total_blocks = 0;
        let mut errors = Vec::new();
        check_mdb(&mdb, &mut errors);
        assert!(
            errors.iter().any(|e| e.code == "MdbTotalBlocksInvalid"),
            "should detect total_blocks=0: {:?}",
            errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_mdb_clump_size_not_multiple() {
        let mut mdb = make_test_mdb();
        // block_size=4096, set drClpSiz to 5000 (not a multiple of 4096)
        BigEndian::write_u32(&mut mdb.raw_sector[24..28], 5000);
        let mut errors = Vec::new();
        check_mdb(&mdb, &mut errors);
        assert!(
            errors.iter().any(|e| e.code == "InvalidClumpSize"),
            "should detect clump size not a multiple of block size: {:?}",
            errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_mdb_clump_size_exceeds_volume() {
        let mut mdb = make_test_mdb();
        // total_blocks=100, block_size=4096 → volume_size=409600
        // Set drXTClpSiz to something larger
        BigEndian::write_u32(&mut mdb.raw_sector[74..78], 4096 * 200);
        let mut errors = Vec::new();
        check_mdb(&mdb, &mut errors);
        assert!(
            errors
                .iter()
                .any(|e| e.code == "InvalidClumpSize" && e.message.contains("exceeds volume size")),
            "should detect clump size exceeding volume: {:?}",
            errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_mdb_clump_size_valid() {
        let mut mdb = make_test_mdb();
        // Set all three clump sizes to valid multiples of block_size (4096)
        BigEndian::write_u32(&mut mdb.raw_sector[24..28], 4096 * 2); // drClpSiz
        BigEndian::write_u32(&mut mdb.raw_sector[74..78], 4096); // drXTClpSiz
        BigEndian::write_u32(&mut mdb.raw_sector[78..82], 4096 * 4); // drCTClpSiz
        let mut errors = Vec::new();
        check_mdb(&mdb, &mut errors);
        assert!(
            !errors.iter().any(|e| e.code == "InvalidClumpSize"),
            "valid clump sizes should not trigger errors"
        );
    }

    #[test]
    fn test_mdb_clump_size_zero_ok() {
        let mdb = make_test_mdb(); // raw_sector all zeros → clump sizes are 0
        let mut errors = Vec::new();
        check_mdb(&mdb, &mut errors);
        assert!(
            !errors.iter().any(|e| e.code == "InvalidClumpSize"),
            "zero clump sizes should be accepted"
        );
    }

    // -----------------------------------------------------------------------
    // Stage 2: Catalog Record Field Validation tests
    // -----------------------------------------------------------------------

    /// Build a minimal catalog B-tree containing a root dir (CNID 2) with
    /// its thread, plus one file record with configurable fields.
    /// Returns (catalog_data, file_cnid).
    fn make_btree_with_file(
        file_cnid: u32,
        data_logical: u32,
        data_physical: u32,
        rsrc_logical: u32,
        rsrc_physical: u32,
        reserved_byte: u8,
        data_start_block: u16,
        rsrc_start_block: u16,
    ) -> Vec<u8> {
        let node_size = 512usize;
        let num_nodes = 4usize;
        let mut data = vec![0u8; node_size * num_nodes];

        // Node 0: Header node
        data[8] = BTREE_HEADER_NODE as u8;
        BigEndian::write_u16(&mut data[10..12], 3); // 3 records

        let hr = 14;
        BigEndian::write_u16(&mut data[hr..hr + 2], 1); // depth = 1
        BigEndian::write_u32(&mut data[hr + 2..hr + 6], 1); // root_node = 1
        BigEndian::write_u32(&mut data[hr + 6..hr + 10], 4); // leaf_records = 4
        BigEndian::write_u32(&mut data[hr + 10..hr + 14], 1); // first_leaf = 1
        BigEndian::write_u32(&mut data[hr + 14..hr + 18], 1); // last_leaf = 1
        BigEndian::write_u16(&mut data[hr + 18..hr + 20], node_size as u16);
        BigEndian::write_u16(&mut data[hr + 20..hr + 22], 37); // max_key_len
        BigEndian::write_u32(&mut data[hr + 22..hr + 26], num_nodes as u32);
        BigEndian::write_u32(&mut data[hr + 26..hr + 30], (num_nodes - 2) as u32);

        // Offset table for header node
        let ot = node_size;
        BigEndian::write_u16(&mut data[ot - 2..ot], 14);
        BigEndian::write_u16(&mut data[ot - 4..ot - 2], 0x78);
        BigEndian::write_u16(&mut data[ot - 6..ot - 4], 0xf8);
        BigEndian::write_u16(&mut data[ot - 8..ot - 6], node_size as u16 - 8);

        // Node bitmap: mark nodes 0 and 1 as allocated
        data[0xf8] = 0b11000000;

        // Node 1: Leaf node with 4 records:
        //   rec 0: root dir thread (CNID 2 → parent 1, name "Test")
        //   rec 1: root dir (parent_id=1, name="", CNID=2, valence=1)
        //   rec 2: file record (parent_id=2, name="File", CNID=file_cnid)
        //   rec 3: file thread (CNID=file_cnid → parent 2, name "File")
        // Keys must be sorted: (1,"") < (2,"") < (2,"File") < (file_cnid,"")
        let n1 = node_size;
        data[n1 + 8] = BTREE_LEAF_NODE as u8;
        data[n1 + 9] = 1; // height

        // Key order: (1,"Test") < (2,"") < (2,"File") < (file_cnid,"")
        // So: rec0=root dir, rec1=root dir thread, rec2=file, rec3=file thread
        let mut write_pos = n1 + 14;
        let mut num_recs = 0u16;

        // Record 0: Root directory — key (parent_id=1, name="Test")
        {
            let kl_off = write_pos;
            data[kl_off] = 10; // key_len = 10 (1+4+1+4)
            data[kl_off + 2..kl_off + 6].copy_from_slice(&1u32.to_be_bytes()); // parent_id=1
            data[kl_off + 6] = 4; // name_len=4
            data[kl_off + 7..kl_off + 11].copy_from_slice(b"Test");
            let rec_off = kl_off + 12; // 1+10=11 → pad to 12
            data[rec_off] = CATALOG_DIR as u8; // type = 1
            BigEndian::write_u16(&mut data[rec_off + 4..rec_off + 6], 1); // valence=1
            BigEndian::write_u32(&mut data[rec_off + 6..rec_off + 10], 2); // dirID=2
            write_pos = rec_off + 70;
            num_recs += 1;
        }

        // Record 1: Root dir thread — key (parent_id=2, name="")
        {
            let kl_off = write_pos;
            data[kl_off] = 6; // key_len=6
            data[kl_off + 2..kl_off + 6].copy_from_slice(&2u32.to_be_bytes());
            data[kl_off + 6] = 0; // name_len=0
            let rec_off = kl_off + 8; // 1+6=7 → pad to 8
            data[rec_off] = CATALOG_DIR_THREAD as u8;
            BigEndian::write_u32(&mut data[rec_off + 10..rec_off + 14], 1); // parent=1
            data[rec_off + 14] = 4;
            data[rec_off + 15..rec_off + 19].copy_from_slice(b"Test");
            write_pos = rec_off + 46;
            num_recs += 1;
        }

        // Record 2: File record — key (parent_id=2, name="File")
        {
            let kl_off = write_pos;
            data[kl_off] = 10; // key_len=10 (1+4+1+4)
            data[kl_off + 2..kl_off + 6].copy_from_slice(&2u32.to_be_bytes());
            data[kl_off + 6] = 4; // name_len=4
            data[kl_off + 7..kl_off + 11].copy_from_slice(b"File");
            let rec_off = kl_off + 12; // 1+10=11 → pad to 12
            data[rec_off] = CATALOG_FILE as u8; // type = 2
            data[rec_off + 1] = reserved_byte; // cdrResrv
            BigEndian::write_u32(&mut data[rec_off + 20..rec_off + 24], file_cnid); // filFlNum
            BigEndian::write_u16(&mut data[rec_off + 24..rec_off + 26], data_start_block);
            BigEndian::write_u32(&mut data[rec_off + 26..rec_off + 30], data_logical);
            BigEndian::write_u32(&mut data[rec_off + 30..rec_off + 34], data_physical);
            BigEndian::write_u16(&mut data[rec_off + 34..rec_off + 36], rsrc_start_block);
            BigEndian::write_u32(&mut data[rec_off + 36..rec_off + 40], rsrc_logical);
            BigEndian::write_u32(&mut data[rec_off + 40..rec_off + 44], rsrc_physical);
            write_pos = rec_off + 102;
            num_recs += 1;
        }

        // Record 3: File thread — key (parent_id=file_cnid, name="")
        {
            let kl_off = write_pos;
            data[kl_off] = 6;
            data[kl_off + 2..kl_off + 6].copy_from_slice(&file_cnid.to_be_bytes());
            data[kl_off + 6] = 0;
            let rec_off = kl_off + 8;
            data[rec_off] = CATALOG_FILE_THREAD as u8;
            BigEndian::write_u32(&mut data[rec_off + 10..rec_off + 14], 2); // parent=2
            data[rec_off + 14] = 4;
            data[rec_off + 15..rec_off + 19].copy_from_slice(b"File");
            num_recs += 1;
        }

        // Set record count
        BigEndian::write_u16(&mut data[n1 + 10..n1 + 12], num_recs);

        // Write offset table for leaf node
        let mut rec_offsets = Vec::new();
        let mut cur = 14usize;
        // rec 0: root dir = key(12) + data(70) = 82
        rec_offsets.push(cur);
        cur += 82;
        // rec 1: dir thread = key(8) + data(46) = 54
        rec_offsets.push(cur);
        cur += 54;
        // rec 2: file = key(12) + data(102) = 114
        rec_offsets.push(cur);
        cur += 114;
        // rec 3: file thread = key(8) + data(46) = 54
        rec_offsets.push(cur);
        cur += 54;
        // free space
        let lot = n1 + node_size;
        for (i, &off) in rec_offsets.iter().enumerate() {
            BigEndian::write_u16(&mut data[lot - 2 * (i + 1)..lot - 2 * i], off as u16);
        }
        BigEndian::write_u16(
            &mut data[lot - 2 * (rec_offsets.len() + 1)..lot - 2 * rec_offsets.len()],
            cur as u16,
        );

        data
    }

    fn run_catalog_check_on_btree(
        mdb: &HfsMasterDirectoryBlock,
        catalog_data: &[u8],
    ) -> (Vec<FsckIssue>, Vec<FsckIssue>) {
        let header = BTreeHeader::read(catalog_data);
        let mut errors = Vec::new();
        let mut warnings = Vec::new();
        let mut files_checked = 0;
        let mut dirs_checked = 0;
        let mut orphaned_threads = 0;
        let mut orphaned_entries = Vec::new();
        check_catalog_consistency(
            mdb,
            catalog_data,
            &header,
            &mut errors,
            &mut warnings,
            &mut files_checked,
            &mut dirs_checked,
            &mut orphaned_threads,
            &mut orphaned_entries,
        );
        (errors, warnings)
    }

    #[test]
    fn test_leof_exceeds_peof_data_fork() {
        let catalog = make_btree_with_file(16, 5000, 4096, 0, 0, 0, 0, 0);
        let mut mdb = make_test_mdb();
        mdb.file_count = 1;
        mdb.next_catalog_id = 17;
        let (errors, _) = run_catalog_check_on_btree(&mdb, &catalog);
        assert!(
            errors
                .iter()
                .any(|e| e.code == "LeoFExceedsPeoF" && e.message.contains("data fork")),
            "should detect data LEOF > PEOF: {:?}",
            errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_leof_exceeds_peof_rsrc_fork() {
        let catalog = make_btree_with_file(16, 0, 0, 3000, 2048, 0, 0, 0);
        let mut mdb = make_test_mdb();
        mdb.file_count = 1;
        mdb.next_catalog_id = 17;
        let (errors, _) = run_catalog_check_on_btree(&mdb, &catalog);
        assert!(
            errors
                .iter()
                .any(|e| e.code == "LeoFExceedsPeoF" && e.message.contains("resource fork")),
            "should detect rsrc LEOF > PEOF: {:?}",
            errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_leof_within_peof_ok() {
        let catalog = make_btree_with_file(16, 4000, 4096, 1000, 2048, 0, 0, 0);
        let mut mdb = make_test_mdb();
        mdb.file_count = 1;
        mdb.next_catalog_id = 17;
        let (errors, _) = run_catalog_check_on_btree(&mdb, &catalog);
        assert!(
            !errors.iter().any(|e| e.code == "LeoFExceedsPeoF"),
            "LEOF <= PEOF should not error: {:?}",
            errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_cnid_below_16_file() {
        let catalog = make_btree_with_file(5, 0, 0, 0, 0, 0, 0, 0); // CNID 5 = reserved
        let mut mdb = make_test_mdb();
        mdb.file_count = 1;
        mdb.next_catalog_id = 17;
        let (errors, _) = run_catalog_check_on_btree(&mdb, &catalog);
        assert!(
            errors
                .iter()
                .any(|e| e.code == "InvalidCnidRange" && e.message.contains("file CNID 5")),
            "should detect reserved file CNID: {:?}",
            errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_cnid_16_file_ok() {
        let catalog = make_btree_with_file(16, 0, 0, 0, 0, 0, 0, 0);
        let mut mdb = make_test_mdb();
        mdb.file_count = 1;
        mdb.next_catalog_id = 17;
        let (errors, _) = run_catalog_check_on_btree(&mdb, &catalog);
        assert!(
            !errors
                .iter()
                .any(|e| e.code == "InvalidCnidRange" && e.message.contains("file")),
            "file CNID 16 should be ok: {:?}",
            errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_cnid_2_root_ok() {
        // Root dir has CNID 2 — should NOT trigger InvalidCnidRange
        let catalog = make_btree_with_file(16, 0, 0, 0, 0, 0, 0, 0);
        let mut mdb = make_test_mdb();
        mdb.file_count = 1;
        mdb.next_catalog_id = 17;
        let (errors, _) = run_catalog_check_on_btree(&mdb, &catalog);
        assert!(
            !errors
                .iter()
                .any(|e| e.code == "InvalidCnidRange" && e.message.contains("directory CNID 2")),
            "root dir CNID 2 should not trigger error: {:?}",
            errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_reserved_fields_nonzero() {
        // reserved_byte=1, data_start_block=1, rsrc_start_block=0
        let catalog = make_btree_with_file(16, 0, 0, 0, 0, 1, 1, 0);
        let mut mdb = make_test_mdb();
        mdb.file_count = 1;
        mdb.next_catalog_id = 17;
        let (_, warnings) = run_catalog_check_on_btree(&mdb, &catalog);
        assert!(
            warnings
                .iter()
                .any(|e| e.code == "ReservedFieldNonZero" && e.message.contains("reserved byte")),
            "should warn about non-zero reserved byte: {:?}",
            warnings.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
        // filStBlk (dataStartBlock) and filRStBlk (rsrcStartBlock) are real
        // fields — non-zero values are normal and should NOT produce warnings.
        assert!(
            !warnings
                .iter()
                .any(|e| e.message.contains("dataStartBlock")
                    || e.message.contains("rsrcStartBlock")),
            "start block fields should not warn: {:?}",
            warnings.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_reserved_fields_zero_ok() {
        let catalog = make_btree_with_file(16, 0, 0, 0, 0, 0, 0, 0);
        let mut mdb = make_test_mdb();
        mdb.file_count = 1;
        mdb.next_catalog_id = 17;
        let (_, warnings) = run_catalog_check_on_btree(&mdb, &catalog);
        assert!(
            !warnings.iter().any(|e| e.code == "ReservedFieldNonZero"),
            "all-zero reserved fields should not warn: {:?}",
            warnings.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
    }

    // -----------------------------------------------------------------------
    // Stage 3: Catalog Name & Thread Key Validation tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_catalog_name_valid() {
        // "File" is a valid 4-byte name — should pass
        let catalog = make_btree_with_file(16, 0, 0, 0, 0, 0, 0, 0);
        let mut mdb = make_test_mdb();
        mdb.file_count = 1;
        mdb.next_catalog_id = 17;
        let (errors, _) = run_catalog_check_on_btree(&mdb, &catalog);
        assert!(
            !errors.iter().any(|e| e.code == "InvalidCatalogName"),
            "valid name should not error: {:?}",
            errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_catalog_name_contains_colon() {
        let mut catalog = make_btree_with_file(16, 0, 0, 0, 0, 0, 0, 0);
        // Patch the file name in the catalog — the file record key is at
        // node 1, record 2. The name starts at key offset+7.
        // rec 2 starts at: 14 + 82 (rec0) + 54 (rec1) = 150 within node 1
        // node 1 offset = 512
        // key name at: 512 + 150 + 7 = 669
        // Name is "File" (4 bytes). Replace 'F' with ':'
        catalog[512 + 150 + 7] = 0x3A; // ':' = colon
        let mut mdb = make_test_mdb();
        mdb.file_count = 1;
        mdb.next_catalog_id = 17;
        let (errors, _) = run_catalog_check_on_btree(&mdb, &catalog);
        assert!(
            errors
                .iter()
                .any(|e| e.code == "InvalidCatalogName" && e.message.contains("colon")),
            "should detect colon in name: {:?}",
            errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_catalog_name_empty() {
        let mut catalog = make_btree_with_file(16, 0, 0, 0, 0, 0, 0, 0);
        // Patch the file name length to 0 but keep key_len unchanged so
        // the record data offset stays correct. The name bytes become padding.
        // rec 2 key: at 512 + 150. name_len at offset +6
        catalog[512 + 150 + 6] = 0; // name_len = 0
        let mut mdb = make_test_mdb();
        mdb.file_count = 1;
        mdb.next_catalog_id = 17;
        let (errors, _) = run_catalog_check_on_btree(&mdb, &catalog);
        assert!(
            errors
                .iter()
                .any(|e| e.code == "InvalidCatalogName" && e.message.contains("empty")),
            "should detect empty name: {:?}",
            errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_thread_key_name_zero_ok() {
        // Default btree has thread records with name_len=0 in keys — should be fine
        let catalog = make_btree_with_file(16, 0, 0, 0, 0, 0, 0, 0);
        let mut mdb = make_test_mdb();
        mdb.file_count = 1;
        mdb.next_catalog_id = 17;
        let (errors, _) = run_catalog_check_on_btree(&mdb, &catalog);
        assert!(
            !errors.iter().any(|e| e.code == "ThreadNameNotEmpty"),
            "zero-length thread key name should not error: {:?}",
            errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_thread_key_name_nonzero() {
        let mut catalog = make_btree_with_file(16, 0, 0, 0, 0, 0, 0, 0);
        // Patch the file thread key (record 3) to have name_len=1
        // rec 3 starts at: 14 + 82 + 54 + 114 = 264 within node 1
        // node 1 offset = 512
        // key_len at 512+264, name_len at 512+264+6
        let r3 = 512 + 264;
        catalog[r3] = 7; // key_len = 7 (was 6)
        catalog[r3 + 6] = 1; // name_len = 1
        catalog[r3 + 7] = b'X'; // name byte
        let mut mdb = make_test_mdb();
        mdb.file_count = 1;
        mdb.next_catalog_id = 17;
        let (errors, _) = run_catalog_check_on_btree(&mdb, &catalog);
        assert!(
            errors.iter().any(|e| e.code == "ThreadNameNotEmpty"),
            "non-zero thread key name should error: {:?}",
            errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_validate_hfs_name_too_long() {
        let name = vec![b'A'; 32];
        assert!(validate_hfs_name(&name).is_some());
        assert!(validate_hfs_name(&name).unwrap().contains("exceeds"));
    }

    // -----------------------------------------------------------------------
    // Stage 4: Directory Structure tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_directory_reaches_root() {
        // CNID 2 (root) → parent 1 (virtual root)
        // CNID 16 → parent 2
        let mut dirs: HashMap<u32, (u32, Vec<u8>, u16)> = HashMap::new();
        dirs.insert(2, (1, b"Root".to_vec(), 1));
        dirs.insert(16, (2, b"Sub".to_vec(), 0));
        let mut errors = Vec::new();
        let mut warnings = Vec::new();
        check_directory_structure(&dirs, &mut errors, &mut warnings);
        assert!(
            !errors.iter().any(|e| e.code == "DirectoryLoop"),
            "normal hierarchy should not have loops"
        );
        assert!(
            !warnings.iter().any(|e| e.code == "NestingDepthWarning"),
            "shallow hierarchy should not warn"
        );
    }

    #[test]
    fn test_directory_loop_detected() {
        // CNID 2 (root) → parent 1
        // CNID 16 → parent 17
        // CNID 17 → parent 16  (loop!)
        let mut dirs: HashMap<u32, (u32, Vec<u8>, u16)> = HashMap::new();
        dirs.insert(2, (1, b"Root".to_vec(), 0));
        dirs.insert(16, (17, b"A".to_vec(), 0));
        dirs.insert(17, (16, b"B".to_vec(), 0));
        let mut errors = Vec::new();
        let mut warnings = Vec::new();
        check_directory_structure(&dirs, &mut errors, &mut warnings);
        assert!(
            errors.iter().any(|e| e.code == "DirectoryLoop"),
            "should detect directory loop: {:?}",
            errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_deep_nesting_warning() {
        // Build a 101-level chain: 2 → 16 → 17 → ... → 116
        let mut dirs: HashMap<u32, (u32, Vec<u8>, u16)> = HashMap::new();
        dirs.insert(2, (1, b"Root".to_vec(), 1));
        for i in 0..101u32 {
            let cnid = 16 + i;
            let parent = if i == 0 { 2 } else { 16 + i - 1 };
            dirs.insert(cnid, (parent, b"D".to_vec(), if i < 100 { 1 } else { 0 }));
        }
        let mut errors = Vec::new();
        let mut warnings = Vec::new();
        check_directory_structure(&dirs, &mut errors, &mut warnings);
        assert!(
            warnings.iter().any(|e| e.code == "NestingDepthWarning"),
            "should warn about deep nesting: {:?}",
            warnings.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_normal_nesting_ok() {
        // 5-level chain: should not warn
        let mut dirs: HashMap<u32, (u32, Vec<u8>, u16)> = HashMap::new();
        dirs.insert(2, (1, b"Root".to_vec(), 1));
        for i in 0..5u32 {
            let cnid = 16 + i;
            let parent = if i == 0 { 2 } else { 16 + i - 1 };
            dirs.insert(cnid, (parent, b"D".to_vec(), if i < 4 { 1 } else { 0 }));
        }
        let mut errors = Vec::new();
        let mut warnings = Vec::new();
        check_directory_structure(&dirs, &mut errors, &mut warnings);
        assert!(
            !warnings.iter().any(|e| e.code == "NestingDepthWarning"),
            "shallow nesting should not warn"
        );
        assert!(!errors.iter().any(|e| e.code == "DirectoryLoop"), "no loop");
    }

    // -----------------------------------------------------------------------
    // Stage 5: Alternate MDB & Embedded HFS+ Wrapper tests
    // -----------------------------------------------------------------------

    fn make_alt_mdb_sector(mdb: &HfsMasterDirectoryBlock) -> [u8; 512] {
        // Build an alt MDB sector matching the primary
        let mut alt = [0u8; 512];
        BigEndian::write_u16(&mut alt[0..2], 0x4244); // signature
        BigEndian::write_u16(&mut alt[14..16], mdb.volume_bitmap_block);
        BigEndian::write_u16(&mut alt[18..20], mdb.total_blocks);
        BigEndian::write_u32(&mut alt[20..24], mdb.block_size);
        BigEndian::write_u16(&mut alt[28..30], mdb.first_alloc_block);
        alt
    }

    #[test]
    fn test_alternate_mdb_matches() {
        let mdb = make_test_mdb();
        let alt = make_alt_mdb_sector(&mdb);
        let mut errors = Vec::new();
        check_alternate_mdb(&mdb, &alt, &mut errors);
        assert!(
            errors.is_empty(),
            "matching alt MDB should not error: {:?}",
            errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_alternate_mdb_block_size_mismatch() {
        let mdb = make_test_mdb();
        let mut alt = make_alt_mdb_sector(&mdb);
        BigEndian::write_u32(&mut alt[20..24], 8192); // different block size
        let mut errors = Vec::new();
        check_alternate_mdb(&mdb, &alt, &mut errors);
        assert!(
            errors
                .iter()
                .any(|e| e.code == "AlternateMdbMismatch" && e.message.contains("block size")),
            "should detect block size mismatch: {:?}",
            errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_alternate_mdb_missing_signature_silently_skipped() {
        let mdb = make_test_mdb();
        let alt = [0u8; 512]; // all zeros — no valid MDB signature
        let mut errors = Vec::new();
        check_alternate_mdb(&mdb, &alt, &mut errors);
        // Can't distinguish "missing" from "looked in the wrong place"
        // (partition larger than allocation area), so produce nothing.
        assert!(
            errors.is_empty(),
            "unexpected errors: {:?}",
            errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_embedded_hfs_plus_valid_wrapper() {
        let mut mdb = make_test_mdb();
        mdb.embedded_signature = 0x482B;
        mdb.embedded_start_block = 10;
        mdb.embedded_block_count = 50;
        let mut errors = Vec::new();
        let mut warnings = Vec::new();
        check_embedded_hfs_plus(&mdb, &mut errors, &mut warnings);
        assert!(
            !errors.iter().any(|e| e.code == "EmbeddedHfsPlusInvalid"),
            "valid embedded region should not error: {:?}",
            errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
        // Should have an informational warning
        assert!(
            warnings
                .iter()
                .any(|e| e.code == "EmbeddedHfsPlusInvalid" && e.message.contains("wrapper")),
            "should warn about HFS wrapper: {:?}",
            warnings.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_embedded_hfs_plus_out_of_range() {
        let mut mdb = make_test_mdb();
        mdb.embedded_signature = 0x482B;
        mdb.embedded_start_block = 80;
        mdb.embedded_block_count = 30; // 80+30=110 > total_blocks=100
        let mut errors = Vec::new();
        let mut warnings = Vec::new();
        check_embedded_hfs_plus(&mdb, &mut errors, &mut warnings);
        assert!(
            errors
                .iter()
                .any(|e| e.code == "EmbeddedHfsPlusInvalid" && e.message.contains("exceeds")),
            "should detect out-of-range embedded region: {:?}",
            errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_embedded_unknown_signature() {
        let mut mdb = make_test_mdb();
        mdb.embedded_signature = 0x1234;
        let mut errors = Vec::new();
        let mut warnings = Vec::new();
        check_embedded_hfs_plus(&mdb, &mut errors, &mut warnings);
        assert!(
            errors
                .iter()
                .any(|e| e.code == "EmbeddedHfsPlusInvalid" && e.message.contains("unknown")),
            "should detect unknown embedded signature: {:?}",
            errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
    }

    // -----------------------------------------------------------------------
    // Stage 6: Bad Block File Bitmap Cross-Check tests
    // -----------------------------------------------------------------------

    /// Build a minimal extents overflow B-tree with one extent record for the given file_id.
    fn make_extents_btree_with_bad_blocks(
        file_id: u32,
        start_block: u16,
        block_count: u16,
    ) -> Vec<u8> {
        let node_size = 512usize;
        let num_nodes = 4usize;
        let mut data = vec![0u8; node_size * num_nodes];

        // Node 0: Header node
        data[8] = BTREE_HEADER_NODE as u8;
        BigEndian::write_u16(&mut data[10..12], 3); // 3 records

        let hr = 14;
        BigEndian::write_u16(&mut data[hr..hr + 2], 1); // depth = 1
        BigEndian::write_u32(&mut data[hr + 2..hr + 6], 1); // root_node = 1
        BigEndian::write_u32(&mut data[hr + 6..hr + 10], 1); // leaf_records = 1
        BigEndian::write_u32(&mut data[hr + 10..hr + 14], 1); // first_leaf = 1
        BigEndian::write_u32(&mut data[hr + 14..hr + 18], 1); // last_leaf = 1
        BigEndian::write_u16(&mut data[hr + 18..hr + 20], node_size as u16);
        BigEndian::write_u16(&mut data[hr + 20..hr + 22], 7); // max_key_len
        BigEndian::write_u32(&mut data[hr + 22..hr + 26], num_nodes as u32);
        BigEndian::write_u32(&mut data[hr + 26..hr + 30], (num_nodes - 2) as u32);

        let ot = node_size;
        BigEndian::write_u16(&mut data[ot - 2..ot], 14);
        BigEndian::write_u16(&mut data[ot - 4..ot - 2], 0x78);
        BigEndian::write_u16(&mut data[ot - 6..ot - 4], 0xf8);
        BigEndian::write_u16(&mut data[ot - 8..ot - 6], node_size as u16 - 8);

        // Node bitmap: mark nodes 0 and 1
        data[0xf8] = 0b11000000;

        // Node 1: Leaf node with 1 extent record
        let n1 = node_size;
        data[n1 + 8] = BTREE_LEAF_NODE as u8;
        data[n1 + 9] = 1;
        BigEndian::write_u16(&mut data[n1 + 10..n1 + 12], 1);

        // Record 0: key(8 bytes) + 3 extent descriptors(12 bytes) = 20 bytes
        let r0 = n1 + 14;
        data[r0] = 7; // key_len = 7
        data[r0 + 1] = 0x00; // fork_type = data
        BigEndian::write_u32(&mut data[r0 + 2..r0 + 6], file_id);
        BigEndian::write_u16(&mut data[r0 + 6..r0 + 8], 0); // startBlock

        // Record data at even offset: r0 + 1 + 7 = r0+8 (even)
        let rd = r0 + 8;
        // First extent descriptor
        BigEndian::write_u16(&mut data[rd..rd + 2], start_block);
        BigEndian::write_u16(&mut data[rd + 2..rd + 4], block_count);
        // Remaining 2 extents are zero

        // Offset table for leaf
        let lot = n1 + node_size;
        BigEndian::write_u16(&mut data[lot - 2..lot], 14);
        BigEndian::write_u16(&mut data[lot - 4..lot - 2], (14 + 20) as u16);

        data
    }

    #[test]
    fn test_no_bad_block_file_ok() {
        // No extents overflow → no bad block issues
        let mdb = make_test_mdb();
        let catalog = make_minimal_btree(512);
        let bitmap = vec![0u8; 13]; // 100 bits
        let mut errors = Vec::new();
        let mut warnings = Vec::new();
        let header = BTreeHeader::read(&catalog);
        check_extents_and_bitmap(
            &mdb,
            &catalog,
            &header,
            &bitmap,
            None,
            &mut errors,
            &mut warnings,
        );
        assert!(
            !warnings
                .iter()
                .any(|e| e.code == "BadBlockExtentNotInBitmap"),
            "no bad block file should produce no warnings"
        );
    }

    #[test]
    fn test_bad_block_extents_in_bitmap_ok() {
        let mdb = make_test_mdb();
        let catalog = make_minimal_btree(512);
        // Bad block file (CNID 5) claims blocks 50-52
        let ext_data = make_extents_btree_with_bad_blocks(5, 50, 3);
        // Bitmap: mark blocks 50-52 as allocated
        let mut bitmap = vec![0u8; 13]; // 100 bits
        for b in 50..53u32 {
            bitmap_set_bit_be(&mut bitmap, b);
        }
        let mut errors = Vec::new();
        let mut warnings = Vec::new();
        let header = BTreeHeader::read(&catalog);
        check_extents_and_bitmap(
            &mdb,
            &catalog,
            &header,
            &bitmap,
            Some(&ext_data),
            &mut errors,
            &mut warnings,
        );
        assert!(
            !warnings
                .iter()
                .any(|e| e.code == "BadBlockExtentNotInBitmap"),
            "allocated bad blocks should not warn: {:?}",
            warnings.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_bad_block_extent_not_in_bitmap() {
        let mdb = make_test_mdb();
        let catalog = make_minimal_btree(512);
        // Bad block file (CNID 5) claims blocks 50-52
        let ext_data = make_extents_btree_with_bad_blocks(5, 50, 3);
        // Bitmap: block 50 is free (not allocated)
        let bitmap = vec![0u8; 13]; // all free
        let mut errors = Vec::new();
        let mut warnings = Vec::new();
        let header = BTreeHeader::read(&catalog);
        check_extents_and_bitmap(
            &mdb,
            &catalog,
            &header,
            &bitmap,
            Some(&ext_data),
            &mut errors,
            &mut warnings,
        );
        assert!(
            warnings
                .iter()
                .any(|e| e.code == "BadBlockExtentNotInBitmap"),
            "free bad block should warn: {:?}",
            warnings.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_map_node_valid_no_continuation() {
        // Most B-trees have no continuation map nodes (node 0 fLink = 0).
        // This is the common case — should produce no errors.
        let node_size = 512;
        let mut data = vec![0u8; node_size * 4];
        // Node 0: header node, fLink = 0 (no continuation)
        data[8] = BTREE_HEADER_NODE as u8;
        BigEndian::write_u16(&mut data[10..12], 3);
        // Minimal offset table for header node
        let ot = node_size;
        BigEndian::write_u16(&mut data[ot - 2..ot], 14);
        BigEndian::write_u16(&mut data[ot - 4..ot - 2], 0x78);
        BigEndian::write_u16(&mut data[ot - 6..ot - 4], 0xf8);
        BigEndian::write_u16(&mut data[ot - 8..ot - 6], 0x1f8);

        let mut errors = Vec::new();
        check_map_nodes(&data, node_size, &mut errors);
        assert!(errors.is_empty(), "no map node errors expected");
    }

    #[test]
    fn test_map_node_bad_kind_detected() {
        let node_size = 512;
        let mut data = vec![0u8; node_size * 4];
        // Node 0: header node, fLink = 2 (points to map node at index 2)
        BigEndian::write_u32(&mut data[0..4], 2);
        data[8] = BTREE_HEADER_NODE as u8;
        BigEndian::write_u16(&mut data[10..12], 3);

        // Node 2: should be map node but has wrong kind (leaf)
        let n2 = 2 * node_size;
        data[n2 + 8] = BTREE_LEAF_NODE as u8; // wrong!
        BigEndian::write_u16(&mut data[n2 + 10..n2 + 12], 1);
        // Correct offset table
        BigEndian::write_u16(&mut data[n2 + node_size - 2..n2 + node_size], 14);
        BigEndian::write_u16(
            &mut data[n2 + node_size - 4..n2 + node_size - 2],
            (node_size - 4) as u16,
        );

        let mut errors = Vec::new();
        check_map_nodes(&data, node_size, &mut errors);
        assert_eq!(errors.len(), 1, "should detect bad map node kind");
        assert_eq!(errors[0].code, "MapNodeBadStructure");
        assert!(errors[0].repairable);
    }

    #[test]
    fn test_map_node_repair() {
        let node_size = 512;
        let mut data = vec![0u8; node_size * 4];
        // Node 0: header, fLink → node 2
        BigEndian::write_u32(&mut data[0..4], 2);
        data[8] = BTREE_HEADER_NODE as u8;
        BigEndian::write_u16(&mut data[10..12], 3);

        // Node 2: broken map node (wrong kind, wrong record count, wrong offsets)
        let n2 = 2 * node_size;
        data[n2 + 8] = BTREE_INDEX_NODE as u8; // wrong kind
        BigEndian::write_u16(&mut data[n2 + 10..n2 + 12], 5); // wrong record count
        BigEndian::write_u16(&mut data[n2 + node_size - 2..n2 + node_size], 99); // wrong offset

        let mut report = RepairReport {
            fixes_applied: Vec::new(),
            fixes_failed: Vec::new(),
            unrepairable_count: 0,
        };
        repair_map_nodes(&mut data, node_size, &mut report);

        // Verify repairs
        assert_eq!(data[n2 + 8] as i8, BTREE_MAP_NODE);
        assert_eq!(BigEndian::read_u16(&data[n2 + 10..n2 + 12]), 1);
        assert_eq!(
            BigEndian::read_u16(&data[n2 + node_size - 2..n2 + node_size]),
            14
        );
        assert_eq!(
            BigEndian::read_u16(&data[n2 + node_size - 4..n2 + node_size - 2]),
            (node_size - 4) as u16
        );
        assert_eq!(report.fixes_applied.len(), 1);

        // Check passes after repair
        let mut errors = Vec::new();
        check_map_nodes(&data, node_size, &mut errors);
        assert!(
            errors.is_empty(),
            "should be clean after repair: {:?}",
            errors
                .iter()
                .map(|e| e.message.as_str())
                .collect::<Vec<_>>()
        );
    }

    /// Helper: build a minimal B-tree with a header node and one leaf node.
    /// Returns (data, node_size). Node 0 = header, node 1 = leaf (root).
    /// The leaf has one dummy catalog record.
    fn make_minimal_btree(node_size: usize) -> Vec<u8> {
        let num_nodes = 4usize;
        let mut data = vec![0u8; node_size * num_nodes];

        // Node 0: Header node
        data[8] = BTREE_HEADER_NODE as u8; // kind
        BigEndian::write_u16(&mut data[10..12], 3); // 3 records

        // B-tree header record at offset 14
        let hr = 14;
        BigEndian::write_u16(&mut data[hr..hr + 2], 1); // depth = 1
        BigEndian::write_u32(&mut data[hr + 2..hr + 6], 1); // root_node = 1
        BigEndian::write_u32(&mut data[hr + 6..hr + 10], 1); // leaf_records = 1
        BigEndian::write_u32(&mut data[hr + 10..hr + 14], 1); // first_leaf = 1
        BigEndian::write_u32(&mut data[hr + 14..hr + 18], 1); // last_leaf = 1
        BigEndian::write_u16(&mut data[hr + 18..hr + 20], node_size as u16); // node_size
        BigEndian::write_u16(&mut data[hr + 20..hr + 22], 37); // max_key_len
        BigEndian::write_u32(&mut data[hr + 22..hr + 26], num_nodes as u32); // total_nodes
        BigEndian::write_u32(&mut data[hr + 26..hr + 30], (num_nodes - 2) as u32); // free_nodes

        // Offset table for header node
        let ot = node_size;
        BigEndian::write_u16(&mut data[ot - 2..ot], 14); // record 0
        BigEndian::write_u16(&mut data[ot - 4..ot - 2], 0x78); // record 1
        BigEndian::write_u16(&mut data[ot - 6..ot - 4], 0xf8); // record 2
        BigEndian::write_u16(&mut data[ot - 8..ot - 6], node_size as u16 - 8); // free space

        // Node bitmap (record 2 at offset 0xf8): mark nodes 0 and 1 as allocated
        data[0xf8] = 0b11000000;

        // Node 1: Leaf node with 1 dummy record
        let n1 = node_size;
        data[n1 + 8] = BTREE_LEAF_NODE as u8; // kind = leaf
        data[n1 + 9] = 1; // height = 1
        BigEndian::write_u16(&mut data[n1 + 10..n1 + 12], 1); // 1 record

        // Dummy catalog dir record: key(8 bytes) + dir data(70 bytes) = 78 bytes
        let r0 = n1 + 14;
        data[r0] = 6; // key_len = 6
        BigEndian::write_u32(&mut data[r0 + 2..r0 + 6], 1); // parent_id = 1
        data[r0 + 8] = CATALOG_DIR as u8; // type = directory
        BigEndian::write_u32(&mut data[r0 + 14..r0 + 18], 2); // dirID = 2

        // Offset table for leaf
        let lot = n1 + node_size;
        BigEndian::write_u16(&mut data[lot - 2..lot], 14); // record 0
        BigEndian::write_u16(&mut data[lot - 4..lot - 2], (14 + 78) as u16); // free space

        data
    }

    #[test]
    fn test_node_bitmap_consistent() {
        let node_size = 512;
        let data = make_minimal_btree(node_size);
        let header = BTreeHeader::read(&data);
        let mut errors = Vec::new();
        check_node_bitmap_consistency(&data, &header, &mut errors);
        assert!(
            errors.is_empty(),
            "consistent bitmap should have no errors: {:?}",
            errors
                .iter()
                .map(|e| e.message.as_str())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_node_bitmap_missing_detected() {
        let node_size = 512;
        let mut data = make_minimal_btree(node_size);

        // Clear the bitmap bit for node 1 (leaf/root) — it's still referenced
        // The bitmap is at offset 0xf8 in node 0. Bit 0 = node 0, bit 1 = node 1.
        data[0xf8] = 0b10000000; // only node 0 allocated, node 1 missing

        let header = BTreeHeader::read(&data);
        let mut errors = Vec::new();
        check_node_bitmap_consistency(&data, &header, &mut errors);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].code, "NodeBitmapMissing");
        assert!(errors[0].repairable);
        assert!(errors[0].message.contains("node 1"));
    }

    #[test]
    fn test_node_bitmap_missing_repaired() {
        let node_size = 512;
        let mut data = make_minimal_btree(node_size);

        // Clear bitmap bit for node 1
        data[0xf8] = 0b10000000;

        let mut report = RepairReport {
            fixes_applied: Vec::new(),
            fixes_failed: Vec::new(),
            unrepairable_count: 0,
        };
        repair_node_bitmap(&mut data, node_size, &mut report);

        // Verify node 1 is now allocated
        assert_eq!(
            data[0xf8] & 0b01000000,
            0b01000000,
            "node 1 bit should be set"
        );
        assert_eq!(report.fixes_applied.len(), 1);

        // Verify header free_nodes was decremented
        let header = BTreeHeader::read(&data);
        // Original: 2 free nodes (nodes 2,3). After fixing node 1: still 2 free
        // because node 1 was already counted as used. Actually free_nodes was set
        // to num_nodes - 2 = 2. After repair, it saturating_sub(1) = 1.
        assert_eq!(header.free_nodes, 1);

        // Check should now pass
        let mut errors = Vec::new();
        check_node_bitmap_consistency(&data, &header, &mut errors);
        assert!(errors.is_empty());
    }

    /// Build a leaf node with two catalog dir records at given parent_ids.
    /// Returns the modified btree data. Uses make_minimal_btree as base
    /// but replaces the leaf with two records.
    fn make_btree_with_two_leaf_records(
        node_size: usize,
        parent_id_0: u32,
        name_0: &[u8],
        parent_id_1: u32,
        name_1: &[u8],
    ) -> Vec<u8> {
        let mut data = make_minimal_btree(node_size);

        // Build two dir records in the leaf node (node 1)
        let n1 = node_size;
        BigEndian::write_u16(&mut data[n1 + 10..n1 + 12], 2); // 2 records

        // Record 0: key + dir data
        let r0 = n1 + 14;
        let key_len_0 = 1 + 4 + 1 + name_0.len(); // reserved + parent_id + name_len + name
        data[r0] = key_len_0 as u8;
        data[r0 + 1] = 0; // reserved
        BigEndian::write_u32(&mut data[r0 + 2..r0 + 6], parent_id_0);
        data[r0 + 6] = name_0.len() as u8;
        data[r0 + 7..r0 + 7 + name_0.len()].copy_from_slice(name_0);
        let mut key_total_0 = 1 + key_len_0;
        if !key_total_0.is_multiple_of(2) {
            key_total_0 += 1; // even alignment
        }
        let data_off_0 = r0 + key_total_0;
        data[data_off_0] = CATALOG_DIR as u8;
        BigEndian::write_u32(&mut data[data_off_0 + 6..data_off_0 + 10], 2); // dirID
        let rec0_len = key_total_0 + 70;

        // Record 1: key + dir data
        let r1 = r0 + rec0_len;
        let key_len_1 = 1 + 4 + 1 + name_1.len();
        data[r1] = key_len_1 as u8;
        data[r1 + 1] = 0;
        BigEndian::write_u32(&mut data[r1 + 2..r1 + 6], parent_id_1);
        data[r1 + 6] = name_1.len() as u8;
        data[r1 + 7..r1 + 7 + name_1.len()].copy_from_slice(name_1);
        let mut key_total_1 = 1 + key_len_1;
        if !key_total_1.is_multiple_of(2) {
            key_total_1 += 1;
        }
        let data_off_1 = r1 + key_total_1;
        data[data_off_1] = CATALOG_DIR as u8;
        BigEndian::write_u32(&mut data[data_off_1 + 6..data_off_1 + 10], 3);
        let rec1_len = key_total_1 + 70;

        // Offset table
        let lot = n1 + node_size;
        BigEndian::write_u16(&mut data[lot - 2..lot], 14); // record 0
        BigEndian::write_u16(&mut data[lot - 4..lot - 2], (14 + rec0_len) as u16); // record 1
        BigEndian::write_u16(
            &mut data[lot - 6..lot - 4],
            (14 + rec0_len + rec1_len) as u16,
        ); // free space

        // Update header leaf_records
        let hr = 14;
        BigEndian::write_u32(&mut data[hr + 6..hr + 10], 2); // leaf_records = 2

        data
    }

    #[test]
    fn test_keys_in_order() {
        let node_size = 512;
        // parent_id 1 < parent_id 5 → already sorted
        let data = make_btree_with_two_leaf_records(node_size, 1, b"", 5, b"");
        let header = BTreeHeader::read(&data);
        let mut errors = Vec::new();
        check_key_ordering(&data, &header, &mut errors);
        assert!(
            errors.is_empty(),
            "sorted keys should have no errors: {:?}",
            errors
                .iter()
                .map(|e| e.message.as_str())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_keys_out_of_order_detected() {
        let node_size = 512;
        // parent_id 10 > parent_id 1 → out of order (record 0 has higher key)
        let data = make_btree_with_two_leaf_records(node_size, 10, b"", 1, b"");
        let header = BTreeHeader::read(&data);
        let mut errors = Vec::new();
        check_key_ordering(&data, &header, &mut errors);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].code, "KeysOutOfOrder");
        assert!(errors[0].repairable);
    }

    #[test]
    fn test_keys_out_of_order_by_name() {
        let node_size = 512;
        // Same parent_id but name "Zebra" > "Apple" → out of order
        let data = make_btree_with_two_leaf_records(node_size, 2, b"Zebra", 2, b"Apple");
        let header = BTreeHeader::read(&data);
        let mut errors = Vec::new();
        check_key_ordering(&data, &header, &mut errors);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].code, "KeysOutOfOrder");
    }

    #[test]
    fn test_keys_out_of_order_repaired() {
        let node_size = 512;
        let mut data = make_btree_with_two_leaf_records(node_size, 10, b"", 1, b"");

        let mut report = RepairReport {
            fixes_applied: Vec::new(),
            fixes_failed: Vec::new(),
            unrepairable_count: 0,
        };
        repair_key_ordering(&mut data, node_size, &mut report);
        assert_eq!(report.fixes_applied.len(), 1);

        // Verify check passes after repair
        let header = BTreeHeader::read(&data);
        let mut errors = Vec::new();
        check_key_ordering(&data, &header, &mut errors);
        assert!(
            errors.is_empty(),
            "should be clean after repair: {:?}",
            errors
                .iter()
                .map(|e| e.message.as_str())
                .collect::<Vec<_>>()
        );

        // Verify the records are now in correct order: parent_id 1 first, then 10
        let n1 = node_size;
        let node = &data[n1..n1 + node_size];
        let (s0, _) = btree_record_range(node, node_size, 0);
        let (s1, _) = btree_record_range(node, node_size, 1);
        let pid0 = BigEndian::read_u32(&node[s0 + 2..s0 + 6]);
        let pid1 = BigEndian::read_u32(&node[s1 + 2..s1 + 6]);
        assert_eq!(pid0, 1, "first record should have parent_id 1");
        assert_eq!(pid1, 10, "second record should have parent_id 10");
    }

    #[test]
    fn test_fsck_blank_volume_empty_catalog() {
        // A blank HFS volume with no files may have an empty catalog B-tree.
        // This should NOT be reported as an error.
        let mdb = make_test_mdb(); // file_count=0, folder_count=0
        let bitmap = vec![0u8; 16];
        let catalog_data: &[u8] = &[];
        let result = check_hfs_integrity(&mdb, catalog_data, &bitmap, None, None);
        assert!(
            result.is_clean(),
            "blank volume with empty catalog should be clean, got errors: {:?}",
            result.errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_fsck_blank_volume_zeroed_catalog() {
        // A blank HFS volume whose catalog is allocated but zeroed out.
        // node_size reads as 0 — should not be flagged on an empty volume.
        let mdb = make_test_mdb();
        let bitmap = vec![0u8; 16];
        let catalog_data = vec![0u8; 512];
        let result = check_hfs_integrity(&mdb, &catalog_data, &bitmap, None, None);
        assert!(
            result.is_clean(),
            "blank volume with zeroed catalog should be clean, got errors: {:?}",
            result.errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_fsck_nonempty_volume_zeroed_catalog_is_error() {
        // A volume that claims to have files but has a zeroed catalog IS an error.
        let mut mdb = make_test_mdb();
        mdb.file_count = 5;
        let bitmap = vec![0u8; 16];
        let catalog_data = vec![0u8; 512];
        let result = check_hfs_integrity(&mdb, &catalog_data, &bitmap, None, None);
        assert!(
            !result.is_clean(),
            "non-empty volume with zeroed catalog should have errors"
        );
    }
}
