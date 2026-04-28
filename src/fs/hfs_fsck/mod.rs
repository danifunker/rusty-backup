//! HFS filesystem checker (fsck) — read-only integrity verification.
//!
//! Validates the HFS volume in four phases:
//! 1. MDB sanity checks
//! 2. B-tree structure verification
//! 3. Catalog consistency (threads ↔ records, counts)
//! 4. Extent/allocation bitmap cross-check (including extents overflow)

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use byteorder::{BigEndian, ByteOrder};

use super::hfs::{
    mac_roman_to_utf8, HfsExtDescriptor, HfsMasterDirectoryBlock, CATALOG_DIR, CATALOG_FILE,
};
use super::hfs_common::{
    bitmap_set_bit_be, bitmap_test_bit_be, btree_bitmap_set, btree_bitmap_test,
    btree_insert_record, btree_node_bitmap_range, btree_record_range, normalize_catalog_index_key,
    BTreeHeader, BTREE_HEADER_NODE, BTREE_INDEX_NODE, BTREE_LEAF_NODE, BTREE_MAP_NODE,
};

const CATALOG_DIR_THREAD: i8 = 3;
const CATALOG_FILE_THREAD: i8 = 4;

/// Maximum number of bitmap mismatch issues to report before capping.
const MAX_BITMAP_MISMATCHES: usize = 20;

use super::fsck::{FsckIssue, FsckResult, FsckStats, OrphanedEntry, RepairReport};

mod extents;
mod mdb;

use mdb::{check_alternate_mdb, check_embedded_hfs_plus, check_mdb, validate_hfs_name};

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

fn hfs_debug_issue(code: HfsFsckCode, message: impl Into<String>) -> FsckIssue {
    FsckIssue {
        code: format!("{:?}", code),
        message: message.into(),
        repairable: false,
        debug: true,
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

/// Phase 1 (partial): Fix header/root node kinds and rebuild the leaf chain.
/// Index nodes are NOT rebuilt here — that happens after catalog modifications.
fn repair_btree_structure_leaves_only(
    catalog_data: &mut [u8],
    node_size: usize,
    report: &mut RepairReport,
) {
    // Fix header node kind
    if catalog_data.len() >= 9 {
        let kind = catalog_data[8] as i8;
        if kind != BTREE_HEADER_NODE {
            catalog_data[8] = BTREE_HEADER_NODE as u8;
            report.fixes_applied.push(format!(
                "Fixed header node kind: {} -> {}",
                kind, BTREE_HEADER_NODE
            ));
        }
    }

    // Fix root node kind
    let header = BTreeHeader::read(catalog_data);
    if header.root_node != 0 && header.depth > 0 {
        let root_offset = header.root_node as usize * node_size;
        if root_offset + node_size <= catalog_data.len() {
            let root_kind = catalog_data[root_offset + 8] as i8;
            let expected_kind = if header.depth == 1 {
                BTREE_LEAF_NODE
            } else {
                BTREE_INDEX_NODE
            };
            if root_kind != expected_kind {
                catalog_data[root_offset + 8] = expected_kind as u8;
                report.fixes_applied.push(format!(
                    "Fixed root node {} kind: {} -> {}",
                    header.root_node, root_kind, expected_kind
                ));
            }
        }
    }

    // Fix map (continuation) nodes if present
    repair_map_nodes(catalog_data, node_size, report);

    // Sort records within nodes before rebuilding the leaf chain.
    // This fixes intra-node key order; rebuild_leaf_chain fixes inter-node order.
    repair_key_ordering(catalog_data, node_size, report);

    // Rebuild leaf chain: union fLink-reachable + bitmap-allocated leaf nodes,
    // sort by first key, relink, and fix node bitmap.
    rebuild_leaf_chain(catalog_data, node_size, report);
}

/// Fix map (continuation) nodes: ensure correct kind, record count, and offsets.
fn repair_map_nodes(catalog_data: &mut [u8], node_size: usize, report: &mut RepairReport) {
    if catalog_data.len() < node_size {
        return;
    }
    let mut node_idx = BigEndian::read_u32(&catalog_data[0..4]);
    let max_nodes = catalog_data.len() / node_size;
    let mut visited = HashSet::new();

    while node_idx != 0 {
        if !visited.insert(node_idx) {
            break;
        }
        if node_idx as usize >= max_nodes {
            break;
        }
        let off = node_idx as usize * node_size;
        let mut fixed = false;

        let kind = catalog_data[off + 8] as i8;
        if kind != BTREE_MAP_NODE {
            catalog_data[off + 8] = BTREE_MAP_NODE as u8;
            fixed = true;
        }

        let num_records = BigEndian::read_u16(&catalog_data[off + 10..off + 12]) as usize;
        if num_records != 1 {
            BigEndian::write_u16(&mut catalog_data[off + 10..off + 12], 1);
            fixed = true;
        }

        // Set correct offset table: record 0 at 14, free space at node_size - 2
        // The map record spans from offset 14 to (node_size - 4): the last 4 bytes
        // are the offset table itself (2 entries × 2 bytes).
        let rec0_pos = off + node_size - 2;
        let rec0_off = BigEndian::read_u16(&catalog_data[rec0_pos..rec0_pos + 2]) as usize;
        if rec0_off != 14 {
            BigEndian::write_u16(&mut catalog_data[rec0_pos..rec0_pos + 2], 14);
            fixed = true;
        }
        // Free space offset: end of map data = node_size - 4
        let free_pos = off + node_size - 4;
        let free_off = BigEndian::read_u16(&catalog_data[free_pos..free_pos + 2]) as usize;
        let expected_free = node_size - 4;
        if free_off != expected_free {
            BigEndian::write_u16(
                &mut catalog_data[free_pos..free_pos + 2],
                expected_free as u16,
            );
            fixed = true;
        }

        if fixed {
            report.fixes_applied.push(format!(
                "Fixed map node {} structure (kind/records/offsets)",
                node_idx
            ));
        }

        node_idx = BigEndian::read_u32(&catalog_data[off..off + 4]);
    }
}

/// Fix node bitmap: ensure every referenced node is marked as allocated.
fn repair_node_bitmap(catalog_data: &mut [u8], node_size: usize, report: &mut RepairReport) {
    let header = BTreeHeader::read(catalog_data);
    let (_bmp_off, bmp_size) = btree_node_bitmap_range(catalog_data, node_size);
    if bmp_size == 0 {
        return;
    }

    let referenced = collect_referenced_nodes(catalog_data, &header);
    let mut fixed_count = 0u32;

    for &node_idx in &referenced {
        if !btree_bitmap_test(catalog_data, node_size, node_idx) {
            btree_bitmap_set(catalog_data, node_size, node_idx);
            fixed_count += 1;
        }
    }

    if fixed_count > 0 {
        // Update free_nodes in header
        let mut h = BTreeHeader::read(catalog_data);
        h.free_nodes = h.free_nodes.saturating_sub(fixed_count);
        h.write(catalog_data);
        report.fixes_applied.push(format!(
            "Marked {} referenced node(s) as allocated in node bitmap",
            fixed_count
        ));
    }
}

/// Sort records within each leaf/index node by catalog key order.
/// This fixes intra-node key ordering; inter-node ordering is handled by
/// `rebuild_leaf_chain` (which sorts nodes by first key).
fn repair_key_ordering(catalog_data: &mut [u8], node_size: usize, report: &mut RepairReport) {
    let max_nodes = catalog_data.len() / node_size;
    let mut fixed_count = 0u32;

    for node_idx in 0..max_nodes as u32 {
        if !btree_bitmap_test(catalog_data, node_size, node_idx) {
            continue;
        }
        let off = node_idx as usize * node_size;
        let kind = catalog_data[off + 8] as i8;
        if kind != BTREE_LEAF_NODE && kind != BTREE_INDEX_NODE {
            continue;
        }
        let num_records = BigEndian::read_u16(&catalog_data[off + 10..off + 12]) as usize;
        if num_records < 2 {
            continue;
        }

        // Check if this node needs sorting
        let node = &catalog_data[off..off + node_size];
        let mut needs_sort = false;
        for i in 0..num_records - 1 {
            let (s1, e1) = btree_record_range(node, node_size, i);
            let (s2, e2) = btree_record_range(node, node_size, i + 1);
            let key_a = record_key(node, s1, e1);
            let key_b = record_key(node, s2, e2);
            if key_a.is_empty() || key_b.is_empty() {
                continue;
            }
            if compare_catalog_keys(key_a, key_b) != Ordering::Less {
                needs_sort = true;
                break;
            }
        }
        if !needs_sort {
            continue;
        }

        // Collect all records from this node
        let mut records: Vec<Vec<u8>> = Vec::with_capacity(num_records);
        for i in 0..num_records {
            let (rec_start, rec_end) = btree_record_range(node, node_size, i);
            if rec_start < rec_end && rec_end <= node_size {
                records.push(node[rec_start..rec_end].to_vec());
            }
        }

        // Sort by key
        records.sort_by(|a, b| compare_catalog_keys(a, b));

        // Rewrite node data sequentially from offset 14
        let node_mut = &mut catalog_data[off..off + node_size];
        let mut write_pos = 14usize;
        for (i, rec) in records.iter().enumerate() {
            node_mut[write_pos..write_pos + rec.len()].copy_from_slice(rec);
            let opos = node_size - 2 * (i + 1);
            BigEndian::write_u16(&mut node_mut[opos..opos + 2], write_pos as u16);
            write_pos += rec.len();
        }
        // Free-space offset
        let fpos = node_size - 2 * (records.len() + 1);
        BigEndian::write_u16(&mut node_mut[fpos..fpos + 2], write_pos as u16);
        // Clear leftover
        let ot_start = node_size - 2 * (records.len() + 1);
        if write_pos < ot_start {
            node_mut[write_pos..ot_start].fill(0);
        }

        fixed_count += 1;
    }

    if fixed_count > 0 {
        report.fixes_applied.push(format!(
            "Sorted records in {} node(s) to restore key ordering",
            fixed_count
        ));
    }
}

/// Fix the header leaf record count to match actual records in the leaf chain.
fn fix_leaf_record_count(catalog_data: &mut [u8], report: &mut RepairReport) {
    let header = BTreeHeader::read(catalog_data);
    let actual_leaf_count = count_leaf_records(catalog_data, &header);
    if actual_leaf_count != header.leaf_records {
        let mut h = BTreeHeader::read(catalog_data);
        let old = h.leaf_records;
        h.leaf_records = actual_leaf_count;
        h.write(catalog_data);
        report.fixes_applied.push(format!(
            "Fixed leaf record count: {} -> {}",
            old, actual_leaf_count
        ));
    }
}

/// Validate that a node looks like a real leaf node with parseable catalog keys.
fn is_valid_leaf_node(catalog_data: &[u8], node_size: usize, node_idx: u32) -> bool {
    let off = node_idx as usize * node_size;
    if off + node_size > catalog_data.len() {
        return false;
    }
    let kind = catalog_data[off + 8] as i8;
    if kind != BTREE_LEAF_NODE {
        return false;
    }
    let num_records = BigEndian::read_u16(&catalog_data[off + 10..off + 12]) as usize;
    if num_records == 0 {
        return false;
    }
    // Verify first record has a parseable HFS catalog key (min 6 bytes: 1 reserved + 4 parentID + 1 nameLen)
    let (rec_start, rec_end) =
        btree_record_range(&catalog_data[off..off + node_size], node_size, 0);
    if rec_start >= rec_end || rec_end > node_size {
        return false;
    }
    let key_len = catalog_data[off + rec_start] as usize;
    if key_len < 6 {
        return false;
    }
    let key_end = rec_start + 1 + key_len;
    key_end <= rec_end
}

/// Extract the first key from a validated leaf node.
fn extract_first_key(catalog_data: &[u8], node_size: usize, node_idx: u32) -> Vec<u8> {
    let off = node_idx as usize * node_size;
    let (rec_start, _rec_end) =
        btree_record_range(&catalog_data[off..off + node_size], node_size, 0);
    let key_len = catalog_data[off + rec_start] as usize;
    let key_end = rec_start + 1 + key_len;
    catalog_data[off + rec_start..off + key_end].to_vec()
}

/// Discover all valid leaf nodes by unioning the fLink chain and the node bitmap,
/// sort by first key, relink the chain, and fix the node bitmap.
fn rebuild_leaf_chain(catalog_data: &mut [u8], node_size: usize, report: &mut RepairReport) {
    let header = BTreeHeader::read(catalog_data);
    let max_nodes = catalog_data.len() / node_size;

    // Step 1: Walk existing fLink chain to find reachable leaf nodes
    let mut chain_nodes: HashSet<u32> = HashSet::new();
    {
        let mut node_idx = header.first_leaf_node;
        let mut visited = HashSet::new();
        while node_idx != 0 {
            if !visited.insert(node_idx) {
                break;
            }
            if is_valid_leaf_node(catalog_data, node_size, node_idx) {
                chain_nodes.insert(node_idx);
            }
            let off = node_idx as usize * node_size;
            if off + node_size > catalog_data.len() {
                break;
            }
            node_idx = BigEndian::read_u32(&catalog_data[off..off + 4]); // fLink
        }
    }

    // Step 2: Scan bitmap for allocated leaf nodes
    let mut bitmap_nodes: HashSet<u32> = HashSet::new();
    for node_idx in 1..max_nodes as u32 {
        if !btree_bitmap_test(catalog_data, node_size, node_idx) {
            continue;
        }
        if is_valid_leaf_node(catalog_data, node_size, node_idx) {
            bitmap_nodes.insert(node_idx);
        }
    }

    // Step 3: Union both sets
    let all_candidates: HashSet<u32> = chain_nodes.union(&bitmap_nodes).copied().collect();

    // Step 4: Collect with first keys
    let mut leaf_nodes: Vec<(u32, Vec<u8>)> = Vec::new();
    for &node_idx in &all_candidates {
        let first_key = extract_first_key(catalog_data, node_size, node_idx);
        leaf_nodes.push((node_idx, first_key));
    }

    if leaf_nodes.is_empty() {
        return;
    }

    // Sort by first key using HFS catalog key comparison
    leaf_nodes.sort_by(|a, b| {
        super::hfs::HfsFilesystem::<std::io::Cursor<Vec<u8>>>::catalog_compare(&a.1, &b.1)
    });

    // Check if chain is already correct (same nodes in same order)
    let mut chain_ok = header.first_leaf_node == leaf_nodes[0].0
        && header.last_leaf_node == leaf_nodes.last().unwrap().0;
    if chain_ok {
        let mut node_idx = header.first_leaf_node;
        let mut visited = HashSet::new();
        let mut idx = 0;
        while node_idx != 0 && idx < leaf_nodes.len() {
            if !visited.insert(node_idx) || node_idx != leaf_nodes[idx].0 {
                chain_ok = false;
                break;
            }
            let off = node_idx as usize * node_size;
            if off + node_size > catalog_data.len() {
                chain_ok = false;
                break;
            }
            node_idx = BigEndian::read_u32(&catalog_data[off..off + 4]);
            idx += 1;
        }
        if idx != leaf_nodes.len() || node_idx != 0 {
            chain_ok = false;
        }
    }

    if chain_ok {
        return; // Chain is already correct
    }

    // Rewrite fLink/bLink for all leaf nodes, set height=1
    for (i, (node_idx, _)) in leaf_nodes.iter().enumerate() {
        let off = *node_idx as usize * node_size;
        let prev = if i > 0 { leaf_nodes[i - 1].0 } else { 0 };
        let next = if i + 1 < leaf_nodes.len() {
            leaf_nodes[i + 1].0
        } else {
            0
        };
        BigEndian::write_u32(&mut catalog_data[off..off + 4], next); // fLink
        BigEndian::write_u32(&mut catalog_data[off + 4..off + 8], prev); // bLink
        catalog_data[off + 9] = 1; // height = 1 for leaf nodes
    }

    // Update header
    let mut h = BTreeHeader::read(catalog_data);
    h.first_leaf_node = leaf_nodes[0].0;
    h.last_leaf_node = leaf_nodes.last().unwrap().0;
    h.write(catalog_data);

    // Step 5: Fix node bitmap — mark all included leaf nodes as allocated
    let recovered_from_chain = chain_nodes.difference(&bitmap_nodes).count() as u32;
    for &(node_idx, _) in &leaf_nodes {
        if !btree_bitmap_test(catalog_data, node_size, node_idx) {
            btree_bitmap_set(catalog_data, node_size, node_idx);
        }
    }
    if recovered_from_chain > 0 {
        let mut h = BTreeHeader::read(catalog_data);
        h.free_nodes = h.free_nodes.saturating_sub(recovered_from_chain);
        h.write(catalog_data);
        report.fixes_applied.push(format!(
            "Recovered {} leaf nodes reachable via fLink chain but missing from B-tree bitmap",
            recovered_from_chain
        ));
    }

    report.fixes_applied.push(format!(
        "Rebuilt leaf chain: {} leaf nodes relinked",
        leaf_nodes.len()
    ));
}

/// Rebuild index nodes bottom-up from the sorted leaf chain.
///
/// After `rebuild_leaf_chain()` the leaves are correctly linked and sorted.
/// This function frees all old index nodes and builds new ones so that
/// root → index → leaf traversal is consistent with the leaf chain.
pub(crate) fn rebuild_index_nodes(
    catalog_data: &mut [u8],
    node_size: usize,
    report: &mut RepairReport,
) {
    use super::hfs_common::{btree_alloc_node, btree_free_node, init_node};

    let header = BTreeHeader::read(catalog_data);
    let (_bmp_off, bmp_size) = btree_node_bitmap_range(catalog_data, node_size);
    let max_nodes = catalog_data.len() / node_size;

    // Free all existing index nodes
    let mut freed = 0u32;
    let bmp_max_bit = (bmp_size as u32) * 8;
    for node_idx in 1..(max_nodes as u32).min(bmp_max_bit) {
        if !btree_bitmap_test(catalog_data, node_size, node_idx) {
            continue;
        }
        let off = node_idx as usize * node_size;
        if off + node_size > catalog_data.len() {
            break;
        }
        if catalog_data[off + 8] as i8 == BTREE_INDEX_NODE {
            btree_free_node(catalog_data, node_size, node_idx);
            freed += 1;
        }
    }

    // Collect the sorted leaf chain as the initial level
    // Each entry: (node_idx, first_key_bytes)
    let mut current_level: Vec<(u32, Vec<u8>)> = Vec::new();
    {
        let mut node_idx = header.first_leaf_node;
        let mut visited = HashSet::new();
        while node_idx != 0 {
            if !visited.insert(node_idx) {
                break;
            }
            let off = node_idx as usize * node_size;
            if off + node_size > catalog_data.len() {
                break;
            }
            let first_key = extract_first_key(catalog_data, node_size, node_idx);
            current_level.push((node_idx, first_key));
            node_idx = BigEndian::read_u32(&catalog_data[off..off + 4]); // fLink
        }
    }

    if current_level.is_empty() {
        return;
    }

    // If only one leaf node, it is the root
    if current_level.len() == 1 {
        let mut h = BTreeHeader::read(catalog_data);
        h.root_node = current_level[0].0;
        h.depth = 1;
        h.free_nodes += freed;
        h.write(catalog_data);
        if freed > 0 {
            report
                .fixes_applied
                .push(format!("Rebuilt index: freed {} old index nodes", freed));
        }
        return;
    }

    let mut h = BTreeHeader::read(catalog_data);
    h.free_nodes += freed;
    h.write(catalog_data);

    let compare = super::hfs::HfsFilesystem::<std::io::Cursor<Vec<u8>>>::catalog_compare;

    // Build index levels bottom-up
    let mut height: u8 = 1; // leaves are height 1
    let mut index_nodes_created = 0u32;

    while current_level.len() > 1 {
        height += 1;
        let mut next_level: Vec<(u32, Vec<u8>)> = Vec::new();

        // Allocate first index node for this level
        let mut h = BTreeHeader::read(catalog_data);
        let idx_node = match btree_alloc_node(catalog_data, node_size, h.total_nodes) {
            Ok(n) => n,
            Err(_) => {
                report
                    .fixes_failed
                    .push("Index rebuild: out of free nodes".into());
                return;
            }
        };
        h.free_nodes = h.free_nodes.saturating_sub(1);
        h.write(catalog_data);

        init_node(catalog_data, node_size, idx_node, BTREE_INDEX_NODE, height);
        index_nodes_created += 1;

        let first_key_of_node = current_level[0].1.clone();
        next_level.push((idx_node, first_key_of_node));

        for (child_idx, child_key) in &current_level {
            // Build index record: normalized_key + child_node_ptr (4 bytes BE)
            // Mac OS forces all catalog index keys to length 0x25 (37),
            // zero-padded. With key_len=0x25, the record is always 42 bytes:
            // 1 (key_len) + 37 (key data) + 4 (child pointer) = 42.
            let mut index_rec = normalize_catalog_index_key(child_key);
            let mut ptr_bytes = [0u8; 4];
            BigEndian::write_u32(&mut ptr_bytes, *child_idx);
            index_rec.extend_from_slice(&ptr_bytes);

            let off = next_level.last().unwrap().0 as usize * node_size;
            let node = &mut catalog_data[off..off + node_size];

            if btree_insert_record(node, node_size, &index_rec, &compare).is_err() {
                // Current node is full — allocate a new one
                let mut h = BTreeHeader::read(catalog_data);
                let new_idx = match btree_alloc_node(catalog_data, node_size, h.total_nodes) {
                    Ok(n) => n,
                    Err(_) => {
                        report
                            .fixes_failed
                            .push("Index rebuild: out of free nodes".into());
                        return;
                    }
                };
                h.free_nodes = h.free_nodes.saturating_sub(1);
                h.write(catalog_data);

                init_node(catalog_data, node_size, new_idx, BTREE_INDEX_NODE, height);
                index_nodes_created += 1;

                next_level.push((new_idx, child_key.clone()));

                let off2 = new_idx as usize * node_size;
                let node2 = &mut catalog_data[off2..off2 + node_size];
                if btree_insert_record(node2, node_size, &index_rec, &compare).is_err() {
                    report
                        .fixes_failed
                        .push("Index rebuild: record too large for empty node".into());
                    return;
                }
            }
        }

        // Link index nodes at this level with fLink/bLink
        for (i, (idx_node, _)) in next_level.iter().enumerate() {
            let off = *idx_node as usize * node_size;
            let next = if i + 1 < next_level.len() {
                next_level[i + 1].0
            } else {
                0
            };
            let prev = if i > 0 { next_level[i - 1].0 } else { 0 };
            BigEndian::write_u32(&mut catalog_data[off..off + 4], next); // fLink
            BigEndian::write_u32(&mut catalog_data[off + 4..off + 8], prev); // bLink
        }

        current_level = next_level;
    }

    // The single remaining node is the root
    let mut h = BTreeHeader::read(catalog_data);
    h.root_node = current_level[0].0;
    h.depth = height as u16;
    h.write(catalog_data);

    report.fixes_applied.push(format!(
        "Rebuilt index: {} index nodes created, depth = {}",
        index_nodes_created, height
    ));
}

/// Count total leaf records by walking the leaf chain.
fn count_leaf_records(catalog_data: &[u8], header: &BTreeHeader) -> u32 {
    let node_size = header.node_size as usize;
    let mut total = 0u32;
    let mut node_idx = header.first_leaf_node;
    let mut visited = HashSet::new();

    while node_idx != 0 {
        if !visited.insert(node_idx) {
            break;
        }
        let off = node_idx as usize * node_size;
        if off + node_size > catalog_data.len() {
            break;
        }
        total += BigEndian::read_u16(&catalog_data[off + 10..off + 12]) as u32;
        node_idx = BigEndian::read_u32(&catalog_data[off..off + 4]);
    }
    total
}

/// Phase 2: Fix catalog consistency issues.
fn repair_catalog_consistency(
    mdb: &mut HfsMasterDirectoryBlock,
    catalog_data: &mut [u8],
    node_size: usize,
    report: &mut RepairReport,
) {
    // Re-scan catalog to gather state
    let header = BTreeHeader::read(catalog_data);
    let entries = collect_catalog_entries(catalog_data, &header);

    let mut dir_records: HashMap<u32, (u32, Vec<u8>, u16)> = HashMap::new();
    let mut file_records: HashMap<u32, (u32, Vec<u8>)> = HashMap::new();
    let mut dir_threads: HashMap<u32, (u32, Vec<u8>)> = HashMap::new();
    let mut file_threads: HashMap<u32, (u32, Vec<u8>)> = HashMap::new();
    let mut children_count: HashMap<u32, u32> = HashMap::new();

    for entry in &entries {
        match entry {
            CatalogEntry::Directory {
                key,
                dir_id,
                valence,
            } => {
                dir_records.insert(*dir_id, (key.parent_id, key.name.clone(), *valence));
                *children_count.entry(key.parent_id).or_insert(0) += 1;
            }
            CatalogEntry::File { key, file_id, .. } => {
                file_records.insert(*file_id, (key.parent_id, key.name.clone()));
                *children_count.entry(key.parent_id).or_insert(0) += 1;
            }
            CatalogEntry::DirThread {
                key_cnid,
                parent_id,
                name,
                ..
            } => {
                dir_threads.insert(*key_cnid, (*parent_id, name.clone()));
            }
            CatalogEntry::FileThread {
                key_cnid,
                parent_id,
                name,
                ..
            } => {
                file_threads.insert(*key_cnid, (*parent_id, name.clone()));
            }
        }
    }

    // Fix missing directory threads
    for (&dir_id, &(parent_id, ref name, _)) in &dir_records {
        match dir_threads.get(&dir_id) {
            None => {
                // Try to insert the missing directory thread
                if insert_thread_record(
                    catalog_data,
                    node_size,
                    dir_id,
                    CATALOG_DIR_THREAD,
                    parent_id,
                    name,
                ) {
                    report.fixes_applied.push(format!(
                        "Inserted missing directory thread for CNID {}",
                        dir_id
                    ));
                } else {
                    // Thread insertion failed (node full). Remove the orphaned
                    // directory record instead — without a thread, Mac OS
                    // cannot resolve this CNID and some operations will fail.
                    let decoded = mac_roman_to_utf8(name);
                    if remove_record_by_key(catalog_data, node_size, parent_id, name) {
                        report.fixes_applied.push(format!(
                            "Removed directory CNID {} '{}' (could not create missing thread)",
                            dir_id, decoded
                        ));
                    } else {
                        report.fixes_failed.push(format!(
                            "Failed to insert or remove directory thread for CNID {} '{}'",
                            dir_id, decoded
                        ));
                    }
                }
            }
            Some((thr_parent, thr_name)) => {
                // Fix mismatched thread
                if *thr_parent != parent_id || *thr_name != *name {
                    if fix_thread_record(
                        catalog_data,
                        node_size,
                        dir_id,
                        CATALOG_DIR_THREAD,
                        parent_id,
                        name,
                    ) {
                        report.fixes_applied.push(format!(
                            "Fixed directory thread parent/name mismatch for CNID {}",
                            dir_id
                        ));
                    } else {
                        report.fixes_failed.push(format!(
                            "Failed to fix directory thread for CNID {}",
                            dir_id
                        ));
                    }
                }
            }
        }
    }

    // Fix mismatched file threads
    for (&file_id, &(parent_id, ref name)) in &file_records {
        if let Some((thr_parent, thr_name)) = file_threads.get(&file_id) {
            if *thr_parent != parent_id || *thr_name != *name {
                if fix_thread_record(
                    catalog_data,
                    node_size,
                    file_id,
                    CATALOG_FILE_THREAD,
                    parent_id,
                    name,
                ) {
                    report.fixes_applied.push(format!(
                        "Fixed file thread parent/name mismatch for CNID {}",
                        file_id
                    ));
                } else {
                    report
                        .fixes_failed
                        .push(format!("Failed to fix file thread for CNID {}", file_id));
                }
            }
        }
    }

    // Fix thread records with incorrect data lengths (should be exactly 46 bytes)
    repair_thread_record_lengths(catalog_data, node_size, report);

    // Remove orphaned threads (threads with no matching record)
    for &cnid in dir_threads.keys() {
        if !dir_records.contains_key(&cnid) && remove_thread_record(catalog_data, node_size, cnid) {
            report.fixes_applied.push(format!(
                "Removed orphaned directory thread for CNID {}",
                cnid
            ));
        }
    }
    for &cnid in file_threads.keys() {
        if !file_records.contains_key(&cnid) && remove_thread_record(catalog_data, node_size, cnid)
        {
            report
                .fixes_applied
                .push(format!("Removed orphaned file thread for CNID {}", cnid));
        }
    }

    // Fix valence mismatches
    fix_valence_mismatches(
        catalog_data,
        node_size,
        &dir_records,
        &children_count,
        report,
    );

    // Fix next_catalog_id
    let mut max_cnid = 0u32;
    for &dir_id in dir_records.keys() {
        max_cnid = max_cnid.max(dir_id);
    }
    for &file_id in file_records.keys() {
        max_cnid = max_cnid.max(file_id);
    }
    if mdb.next_catalog_id <= max_cnid {
        let old = mdb.next_catalog_id;
        mdb.next_catalog_id = max_cnid + 1;
        report.fixes_applied.push(format!(
            "Fixed next_catalog_id: {} -> {}",
            old,
            max_cnid + 1
        ));
    }
}

/// Insert a thread record into the catalog B-tree.
fn insert_thread_record(
    catalog_data: &mut [u8],
    node_size: usize,
    cnid: u32,
    thread_type: i8,
    parent_id: u32,
    name: &[u8],
) -> bool {
    use super::hfs::HfsFilesystem;

    // Build thread key: (cnid, "")
    let key = HfsFilesystem::<std::io::Cursor<Vec<u8>>>::build_catalog_key(cnid, &[]);

    // Build thread record data
    let (_, rec_data) = HfsFilesystem::<std::io::Cursor<Vec<u8>>>::build_thread_record(
        thread_type,
        parent_id,
        name,
    );

    let mut key_record = key;
    key_record.extend_from_slice(&rec_data);

    // Find the correct leaf node and try to insert
    let header = BTreeHeader::read(catalog_data);
    let compare = HfsFilesystem::<std::io::Cursor<Vec<u8>>>::catalog_compare;
    let (leaf_idx, _) =
        super::hfs_common::btree_find_insert_leaf(catalog_data, &header, &key_record, &compare);

    let offset = leaf_idx as usize * node_size;
    if offset + node_size > catalog_data.len() {
        return false;
    }

    let node = &mut catalog_data[offset..offset + node_size];
    if super::hfs_common::btree_insert_record(node, node_size, &key_record, &compare).is_ok() {
        let mut h = BTreeHeader::read(catalog_data);
        h.leaf_records += 1;
        h.write(catalog_data);
        true
    } else {
        false // Node full — simplified repair doesn't handle splits
    }
}

/// Fix a thread record by removing the old one and inserting a corrected one.
fn fix_thread_record(
    catalog_data: &mut [u8],
    node_size: usize,
    cnid: u32,
    thread_type: i8,
    correct_parent_id: u32,
    correct_name: &[u8],
) -> bool {
    // First remove the existing thread
    if !remove_thread_record(catalog_data, node_size, cnid) {
        return false;
    }
    // Then insert the correct one
    insert_thread_record(
        catalog_data,
        node_size,
        cnid,
        thread_type,
        correct_parent_id,
        correct_name,
    )
}

/// Scan all leaf nodes for thread records with incorrect data lengths and
/// rebuild them with the proper 46-byte format (fixed 32-byte Str31 field).
fn repair_thread_record_lengths(
    catalog_data: &mut [u8],
    node_size: usize,
    report: &mut RepairReport,
) {
    // Collect thread records that need fixing: (cnid, thread_type, parent_id, name)
    let mut bad_threads: Vec<(u32, i8, u32, Vec<u8>)> = Vec::new();

    let header = BTreeHeader::read(catalog_data);
    let mut node_idx = header.first_leaf_node;
    let mut visited = HashSet::new();

    while node_idx != 0 {
        if !visited.insert(node_idx) {
            break;
        }
        let off = node_idx as usize * node_size;
        if off + node_size > catalog_data.len() {
            break;
        }
        let node = &catalog_data[off..off + node_size];
        let kind = node[8] as i8;
        if kind != -1 {
            // Skip non-leaf nodes
            let flink = BigEndian::read_u32(&node[0..4]);
            node_idx = flink;
            continue;
        }
        let num_records = BigEndian::read_u16(&node[10..12]) as usize;
        for i in 0..num_records {
            let (rec_start, rec_end) = btree_record_range(node, node_size, i);
            if rec_start >= rec_end || rec_end > node_size {
                continue;
            }
            let key_len = node[rec_start] as usize;
            let key_total = 1 + key_len;
            let rec_len = rec_end - rec_start;
            if key_total >= rec_len {
                continue;
            }
            let mut data_start = key_total;
            if data_start % 2 != 0 {
                data_start += 1;
            }
            if data_start >= rec_len {
                continue;
            }
            let rec_type = node[rec_start + data_start] as i8;
            let data_len = rec_len - data_start;

            // Only fix thread records (type 3 = dir thread, type 4 = file thread)
            if (rec_type == 3 || rec_type == 4) && data_len != 46 {
                // Extract CNID from the key (parent_id field = CNID for thread keys)
                if key_len >= 5 {
                    let cnid = BigEndian::read_u32(&node[rec_start + 2..rec_start + 6]);
                    // Extract parent_id and name from the existing thread data
                    let data_off = rec_start + data_start;
                    if data_len >= 15 {
                        let parent_id = BigEndian::read_u32(&node[data_off + 10..data_off + 14]);
                        let name_len = (node[data_off + 14] as usize).min(31);
                        let name_end = (data_off + 15 + name_len).min(rec_end);
                        let name = node[data_off + 15..name_end].to_vec();
                        bad_threads.push((cnid, rec_type, parent_id, name));
                    }
                }
            }
        }
        node_idx = BigEndian::read_u32(&catalog_data[off..off + 4]);
    }

    // Now fix each bad thread by removing and re-inserting with correct format
    for (cnid, thread_type, parent_id, name) in bad_threads {
        if fix_thread_record(catalog_data, node_size, cnid, thread_type, parent_id, &name) {
            report.fixes_applied.push(format!(
                "Fixed thread record length for CNID {} (type {})",
                cnid, thread_type
            ));
        } else {
            report.fixes_failed.push(format!(
                "Failed to fix thread record length for CNID {} (type {})",
                cnid, thread_type
            ));
        }
    }
}

/// Remove a thread record (key = (cnid, "")) from the catalog B-tree.
fn remove_thread_record(catalog_data: &mut [u8], node_size: usize, cnid: u32) -> bool {
    let compare = super::hfs::HfsFilesystem::<std::io::Cursor<Vec<u8>>>::catalog_compare;
    let search_key =
        super::hfs::HfsFilesystem::<std::io::Cursor<Vec<u8>>>::build_catalog_key(cnid, &[]);

    let header = BTreeHeader::read(catalog_data);
    let mut node_idx = header.first_leaf_node;
    let mut visited = HashSet::new();

    while node_idx != 0 {
        if !visited.insert(node_idx) {
            break;
        }
        let off = node_idx as usize * node_size;
        if off + node_size > catalog_data.len() {
            break;
        }
        let num_records = BigEndian::read_u16(&catalog_data[off + 10..off + 12]) as usize;
        for i in 0..num_records {
            let (rec_start, rec_end) =
                btree_record_range(&catalog_data[off..off + node_size], node_size, i);
            if rec_start >= rec_end || rec_end > node_size {
                continue;
            }
            let key_len = catalog_data[off + rec_start] as usize;
            let key_end = rec_start + 1 + key_len;
            if key_end > rec_end {
                continue;
            }
            let key_portion = &catalog_data[off + rec_start..off + key_end];
            if compare(key_portion, &search_key) == std::cmp::Ordering::Equal {
                let node = &mut catalog_data[off..off + node_size];
                super::hfs_common::btree_remove_record(node, node_size, i);
                let mut h = BTreeHeader::read(catalog_data);
                h.leaf_records = h.leaf_records.saturating_sub(1);
                h.write(catalog_data);
                return true;
            }
        }
        node_idx = BigEndian::read_u32(&catalog_data[off..off + 4]);
    }
    false
}

/// Remove a catalog record by (parent_id, name) key from the catalog B-tree.
fn remove_record_by_key(
    catalog_data: &mut [u8],
    node_size: usize,
    parent_id: u32,
    name: &[u8],
) -> bool {
    let compare = super::hfs::HfsFilesystem::<std::io::Cursor<Vec<u8>>>::catalog_compare;
    let search_key =
        super::hfs::HfsFilesystem::<std::io::Cursor<Vec<u8>>>::build_catalog_key(parent_id, name);

    let header = BTreeHeader::read(catalog_data);
    let mut node_idx = header.first_leaf_node;
    let mut visited = HashSet::new();

    while node_idx != 0 {
        if !visited.insert(node_idx) {
            break;
        }
        let off = node_idx as usize * node_size;
        if off + node_size > catalog_data.len() {
            break;
        }
        let num_records = BigEndian::read_u16(&catalog_data[off + 10..off + 12]) as usize;
        for i in 0..num_records {
            let (rec_start, rec_end) =
                btree_record_range(&catalog_data[off..off + node_size], node_size, i);
            if rec_start >= rec_end || rec_end > node_size {
                continue;
            }
            let key_len = catalog_data[off + rec_start] as usize;
            let key_end = rec_start + 1 + key_len;
            if key_end > rec_end {
                continue;
            }
            let key_portion = &catalog_data[off + rec_start..off + key_end];
            if compare(key_portion, &search_key) == std::cmp::Ordering::Equal {
                let node = &mut catalog_data[off..off + node_size];
                super::hfs_common::btree_remove_record(node, node_size, i);
                let mut h = BTreeHeader::read(catalog_data);
                h.leaf_records = h.leaf_records.saturating_sub(1);
                h.write(catalog_data);
                return true;
            }
        }
        node_idx = BigEndian::read_u32(&catalog_data[off..off + 4]);
    }
    false
}

/// Fix valence mismatches by patching directory records in-place.
fn fix_valence_mismatches(
    catalog_data: &mut [u8],
    node_size: usize,
    dir_records: &HashMap<u32, (u32, Vec<u8>, u16)>,
    children_count: &HashMap<u32, u32>,
    report: &mut RepairReport,
) {
    let header = BTreeHeader::read(catalog_data);
    let mut node_idx = header.first_leaf_node;
    let mut visited = HashSet::new();

    while node_idx != 0 {
        if !visited.insert(node_idx) {
            break;
        }
        let off = node_idx as usize * node_size;
        if off + node_size > catalog_data.len() {
            break;
        }
        let num_records = BigEndian::read_u16(&catalog_data[off + 10..off + 12]) as usize;
        for i in 0..num_records {
            let (rec_start, _rec_end) =
                btree_record_range(&catalog_data[off..off + node_size], node_size, i);
            let abs_start = off + rec_start;
            if abs_start + 7 > catalog_data.len() {
                continue;
            }
            let key_len = catalog_data[abs_start] as usize;
            let mut rec_data_offset = abs_start + 1 + key_len;
            #[allow(clippy::manual_is_multiple_of)]
            if rec_data_offset % 2 != 0 {
                rec_data_offset += 1;
            }
            if rec_data_offset + 10 > catalog_data.len() {
                continue;
            }
            let record_type = catalog_data[rec_data_offset] as i8;
            if record_type != CATALOG_DIR {
                continue;
            }
            let dir_id =
                BigEndian::read_u32(&catalog_data[rec_data_offset + 6..rec_data_offset + 10]);
            if !dir_records.contains_key(&dir_id) {
                continue;
            }
            let current_valence =
                BigEndian::read_u16(&catalog_data[rec_data_offset + 4..rec_data_offset + 6]);
            let actual = children_count.get(&dir_id).copied().unwrap_or(0) as u16;
            if current_valence != actual {
                BigEndian::write_u16(
                    &mut catalog_data[rec_data_offset + 4..rec_data_offset + 6],
                    actual,
                );
                report.fixes_applied.push(format!(
                    "Fixed valence for directory CNID {}: {} -> {}",
                    dir_id, current_valence, actual
                ));
            }
        }
        node_idx = BigEndian::read_u32(&catalog_data[off..off + 4]);
    }
}

/// Phase 3: Fix extent issues (out-of-range and overlapping extents).
fn repair_extents(
    mdb: &HfsMasterDirectoryBlock,
    catalog_data: &mut [u8],
    node_size: usize,
    _extents_data: Option<&[u8]>,
    report: &mut RepairReport,
) {
    let total_blocks = mdb.total_blocks as u32;
    let header = BTreeHeader::read(catalog_data);
    let mut node_idx = header.first_leaf_node;
    let mut visited = HashSet::new();
    let mut used_blocks: HashSet<u32> = HashSet::new();

    // First mark system file extents
    for ext in &mdb.catalog_file_extents {
        if ext.block_count > 0 && (ext.start_block as u32 + ext.block_count as u32) <= total_blocks
        {
            for b in ext.start_block as u32..ext.start_block as u32 + ext.block_count as u32 {
                used_blocks.insert(b);
            }
        }
    }
    for ext in &mdb.extents_file_extents {
        if ext.block_count > 0 && (ext.start_block as u32 + ext.block_count as u32) <= total_blocks
        {
            for b in ext.start_block as u32..ext.start_block as u32 + ext.block_count as u32 {
                used_blocks.insert(b);
            }
        }
    }

    while node_idx != 0 {
        if !visited.insert(node_idx) {
            break;
        }
        let off = node_idx as usize * node_size;
        if off + node_size > catalog_data.len() {
            break;
        }
        let num_records = BigEndian::read_u16(&catalog_data[off + 10..off + 12]) as usize;
        for i in 0..num_records {
            let (rec_start, _rec_end) =
                btree_record_range(&catalog_data[off..off + node_size], node_size, i);
            let abs_start = off + rec_start;
            if abs_start + 7 > catalog_data.len() {
                continue;
            }
            let key_len = catalog_data[abs_start] as usize;
            let mut rec_data_offset = abs_start + 1 + key_len;
            #[allow(clippy::manual_is_multiple_of)]
            if rec_data_offset % 2 != 0 {
                rec_data_offset += 1;
            }
            if rec_data_offset + 102 > catalog_data.len() {
                continue;
            }
            let record_type = catalog_data[rec_data_offset] as i8;
            if record_type != CATALOG_FILE {
                continue;
            }

            let file_id =
                BigEndian::read_u32(&catalog_data[rec_data_offset + 20..rec_data_offset + 24]);

            // Check data fork extents at rec_data_offset + 74
            for j in 0..3 {
                let ext_off = rec_data_offset + 74 + j * 4;
                let start = BigEndian::read_u16(&catalog_data[ext_off..ext_off + 2]) as u32;
                let count = BigEndian::read_u16(&catalog_data[ext_off + 2..ext_off + 4]) as u32;
                if count == 0 {
                    continue;
                }

                if start + count > total_blocks {
                    // Truncate out-of-range extent
                    BigEndian::write_u16(&mut catalog_data[ext_off..ext_off + 2], 0);
                    BigEndian::write_u16(&mut catalog_data[ext_off + 2..ext_off + 4], 0);
                    report.fixes_applied.push(format!(
                        "Truncated out-of-range data extent for file CNID {} — data loss possible",
                        file_id
                    ));
                } else {
                    // Check for overlaps
                    let mut overlaps = false;
                    for b in start..start + count {
                        if !used_blocks.insert(b) {
                            overlaps = true;
                            break;
                        }
                    }
                    if overlaps {
                        // Remove all blocks we just added, then zero the extent
                        for b in start..start + count {
                            used_blocks.remove(&b);
                        }
                        BigEndian::write_u16(&mut catalog_data[ext_off..ext_off + 2], 0);
                        BigEndian::write_u16(&mut catalog_data[ext_off + 2..ext_off + 4], 0);
                        report.fixes_applied.push(format!(
                            "Zeroed overlapping data extent for file CNID {} — data loss possible",
                            file_id
                        ));
                    }
                }
            }

            // Check resource fork extents at rec_data_offset + 86
            for j in 0..3 {
                let ext_off = rec_data_offset + 86 + j * 4;
                let start = BigEndian::read_u16(&catalog_data[ext_off..ext_off + 2]) as u32;
                let count = BigEndian::read_u16(&catalog_data[ext_off + 2..ext_off + 4]) as u32;
                if count == 0 {
                    continue;
                }

                if start + count > total_blocks {
                    BigEndian::write_u16(&mut catalog_data[ext_off..ext_off + 2], 0);
                    BigEndian::write_u16(&mut catalog_data[ext_off + 2..ext_off + 4], 0);
                    report.fixes_applied.push(format!(
                        "Truncated out-of-range resource extent for file CNID {} — data loss possible",
                        file_id
                    ));
                } else {
                    let mut overlaps = false;
                    for b in start..start + count {
                        if !used_blocks.insert(b) {
                            overlaps = true;
                            break;
                        }
                    }
                    if overlaps {
                        for b in start..start + count {
                            used_blocks.remove(&b);
                        }
                        BigEndian::write_u16(&mut catalog_data[ext_off..ext_off + 2], 0);
                        BigEndian::write_u16(&mut catalog_data[ext_off + 2..ext_off + 4], 0);
                        report.fixes_applied.push(format!(
                            "Zeroed overlapping resource extent for file CNID {} — data loss possible",
                            file_id
                        ));
                    }
                }
            }
        }
        node_idx = BigEndian::read_u32(&catalog_data[off..off + 4]);
    }
}

/// Phase 4: Fix MDB file/folder counts and rebuild volume bitmap.
fn repair_mdb_counts_and_bitmap(
    mdb: &mut HfsMasterDirectoryBlock,
    catalog_data: &[u8],
    bitmap: &mut Vec<u8>,
    extents_data: Option<&[u8]>,
    report: &mut RepairReport,
) {
    let header = BTreeHeader::read(catalog_data);
    let entries = collect_catalog_entries(catalog_data, &header);
    let total_blocks = mdb.total_blocks as u32;

    let mut file_count = 0u32;
    let mut folder_count = 0u32;

    for entry in &entries {
        match entry {
            CatalogEntry::Directory { .. } => folder_count += 1,
            CatalogEntry::File { .. } => file_count += 1,
            _ => {}
        }
    }

    if mdb.file_count != file_count {
        let old = mdb.file_count;
        mdb.file_count = file_count;
        report
            .fixes_applied
            .push(format!("Fixed MDB file_count: {} -> {}", old, file_count));
    }
    // HFS drDirCnt does NOT include the root directory (CNID 2)
    let non_root_folder_count = folder_count.saturating_sub(1);
    if mdb.folder_count != non_root_folder_count {
        let old = mdb.folder_count;
        mdb.folder_count = non_root_folder_count;
        report.fixes_applied.push(format!(
            "Fixed MDB folder_count: {} -> {}",
            old, non_root_folder_count
        ));
    }

    // Rebuild bitmap from scratch
    let mut computed_bitmap = vec![0u8; (total_blocks as usize).div_ceil(8)];

    // Mark catalog file extents
    for ext in &mdb.catalog_file_extents {
        if ext.block_count > 0 {
            let start = ext.start_block as u32;
            let count = ext.block_count as u32;
            if start + count <= total_blocks {
                for b in start..start + count {
                    bitmap_set_bit_be(&mut computed_bitmap, b);
                }
            }
        }
    }

    // Mark extents overflow file extents
    for ext in &mdb.extents_file_extents {
        if ext.block_count > 0 {
            let start = ext.start_block as u32;
            let count = ext.block_count as u32;
            if start + count <= total_blocks {
                for b in start..start + count {
                    bitmap_set_bit_be(&mut computed_bitmap, b);
                }
            }
        }
    }

    // Mark all file data/resource fork extents (inline)
    for entry in &entries {
        if let CatalogEntry::File {
            data_extents,
            data_size,
            rsrc_extents,
            rsrc_size,
            ..
        } = entry
        {
            if *data_size > 0 {
                for ext in data_extents {
                    if ext.block_count > 0 {
                        let start = ext.start_block as u32;
                        let count = ext.block_count as u32;
                        if start + count <= total_blocks {
                            for b in start..start + count {
                                bitmap_set_bit_be(&mut computed_bitmap, b);
                            }
                        }
                    }
                }
            }
            if *rsrc_size > 0 {
                for ext in rsrc_extents {
                    if ext.block_count > 0 {
                        let start = ext.start_block as u32;
                        let count = ext.block_count as u32;
                        if start + count <= total_blocks {
                            for b in start..start + count {
                                bitmap_set_bit_be(&mut computed_bitmap, b);
                            }
                        }
                    }
                }
            }
        }
    }

    // Mark overflow extents
    if let Some(ext_data) = extents_data {
        let overflow = collect_overflow_extents(ext_data);
        for (start, count, _) in &overflow {
            if start + count <= total_blocks {
                for b in *start..*start + *count {
                    bitmap_set_bit_be(&mut computed_bitmap, b);
                }
            }
        }
    }

    // Check if bitmap differs
    let bitmap_changed = *bitmap != computed_bitmap;
    if bitmap_changed {
        *bitmap = computed_bitmap;
        report
            .fixes_applied
            .push("Rebuilt volume allocation bitmap from extents".into());
    }

    // Recount free blocks
    let mut free_blocks = 0u32;
    for block in 0..total_blocks {
        if !bitmap_test_bit_be(bitmap, block) {
            free_blocks += 1;
        }
    }
    if mdb.free_blocks as u32 != free_blocks {
        let old = mdb.free_blocks;
        mdb.free_blocks = free_blocks as u16;
        report
            .fixes_applied
            .push(format!("Fixed MDB free_blocks: {} -> {}", old, free_blocks));
    }
}

// ---------------------------------------------------------------------------
// Phase 2: B-tree Structure
// ---------------------------------------------------------------------------

/// Maximum records per node (based on minimum record size of ~10 bytes in 512-byte node).
pub(super) const HFS_MAX_NRECS: usize = 35;

fn check_btree_structure(
    catalog_data: &[u8],
    header: &BTreeHeader,
    errors: &mut Vec<FsckIssue>,
    leaf_nodes_visited: &mut u32,
) {
    let node_size = header.node_size as usize;

    // Check node size is a power of 2 multiple of 512
    if node_size < 512 || node_size % 512 != 0 {
        errors.push(hfs_issue(
            HfsFsckCode::HeaderNodeBadKind,
            format!(
                "B-tree node size {} (must be a positive multiple of 512)",
                node_size
            ),
        ));
    }

    // Check header node kind
    if catalog_data.len() >= 9 {
        let kind = catalog_data[8] as i8;
        if kind != BTREE_HEADER_NODE {
            errors.push(hfs_issue(
                HfsFsckCode::HeaderNodeBadKind,
                format!(
                    "node 0 kind = {} (expected {} = header)",
                    kind, BTREE_HEADER_NODE
                ),
            ));
        }
    }

    // Check header node structure: must have exactly 3 records
    // Record 0 = B-tree header (0x6a bytes), Record 1 = reserved (0x80 bytes),
    // Record 2 = node bitmap (0x100 bytes for 512-byte nodes, larger for bigger nodes)
    if catalog_data.len() >= node_size {
        let hdr_num_recs = BigEndian::read_u16(&catalog_data[10..12]) as usize;
        if hdr_num_recs != 3 {
            errors.push(hfs_issue(
                HfsFsckCode::HeaderNodeBadKind,
                format!("header node has {} records (expected 3)", hdr_num_recs),
            ));
        } else {
            // Validate first record starts at offset 0x00e (14 = after node descriptor)
            let pos = node_size - 2;
            let rec0_off = BigEndian::read_u16(&catalog_data[pos..pos + 2]);
            if rec0_off != 0x00e {
                errors.push(hfs_issue(
                    HfsFsckCode::HeaderNodeBadKind,
                    format!(
                        "header node offset[0] = 0x{:03x} (expected 0x00e)",
                        rec0_off
                    ),
                ));
            }
        }
    }

    // Check root node kind
    if header.root_node != 0 && header.depth > 0 {
        let root_offset = header.root_node as usize * node_size;
        if root_offset + node_size <= catalog_data.len() {
            let root_kind = catalog_data[root_offset + 8] as i8;
            let expected_kind = if header.depth == 1 {
                BTREE_LEAF_NODE
            } else {
                BTREE_INDEX_NODE
            };
            if root_kind != expected_kind {
                errors.push(hfs_issue(
                    HfsFsckCode::RootNodeBadKind,
                    format!(
                        "root node {} kind = {} (expected {} for depth {})",
                        header.root_node, root_kind, expected_kind, header.depth
                    ),
                ));
            }
        }
    }

    // Check all allocated nodes
    let max_nodes = catalog_data.len() / node_size;
    for node_idx in 0..max_nodes as u32 {
        if !btree_bitmap_test(catalog_data, node_size, node_idx) {
            continue; // free node — skip
        }
        let off = node_idx as usize * node_size;
        if off + node_size > catalog_data.len() {
            break;
        }
        let node = &catalog_data[off..off + node_size];
        let num_records = BigEndian::read_u16(&node[10..12]) as usize;

        // Check record count doesn't exceed maximum for this node size.
        // hfsutils defines HFS_MAX_NRECS=35 for 512-byte nodes. Scale proportionally.
        let max_nrecs = HFS_MAX_NRECS * (node_size / 512);
        if num_records > max_nrecs {
            errors.push(hfs_issue(
                HfsFsckCode::InvalidRecordLength,
                format!(
                    "node {}: {} records exceeds maximum {}",
                    node_idx, num_records, max_nrecs
                ),
            ));
            continue;
        }

        if num_records == 0 {
            continue;
        }

        check_offset_table(node, node_size, node_idx, num_records, errors);

        let kind = node[8] as i8;
        if kind == BTREE_INDEX_NODE {
            check_index_record_lengths(node, node_size, node_idx, num_records, errors);
        } else if kind == BTREE_LEAF_NODE {
            check_leaf_record_lengths(node, node_size, node_idx, num_records, errors);
        }
    }

    // Check map node structure (continuation nodes for large B-trees)
    check_map_nodes(catalog_data, node_size, errors);

    // Walk leaf chain and verify forward/backward links
    let leaf_count = walk_leaf_chain(catalog_data, header, errors, leaf_nodes_visited);

    // Walk index nodes and verify sibling links at each level
    check_index_sibling_links(catalog_data, header, errors);

    // Verify leaf record count
    if leaf_count != header.leaf_records {
        errors.push(hfs_issue(
            HfsFsckCode::LeafRecordCountMismatch,
            format!(
                "leaf record count: walked {} but header says {}",
                leaf_count, header.leaf_records
            ),
        ));
    }

    // Verify node bitmap consistency: every referenced node must be allocated,
    // and every allocated non-special node should be referenced.
    check_node_bitmap_consistency(catalog_data, header, errors);

    // Verify keys within each node are in ascending order
    check_key_ordering(catalog_data, header, errors);
}

/// Validate map (continuation) nodes in the B-tree.
///
/// Node 0 is the header node. Its fLink may point to a continuation map node
/// that holds additional node-allocation bitmap data. Each continuation map
/// node must have kind = BTREE_MAP_NODE (2), exactly 1 record, and the first
/// record must start at offset 14 (immediately after the node descriptor).
fn check_map_nodes(catalog_data: &[u8], node_size: usize, errors: &mut Vec<FsckIssue>) {
    if catalog_data.len() < node_size {
        return;
    }
    // Follow fLink chain from the header node (node 0)
    let mut node_idx = BigEndian::read_u32(&catalog_data[0..4]);
    let max_nodes = catalog_data.len() / node_size;
    let mut visited = HashSet::new();

    while node_idx != 0 {
        if !visited.insert(node_idx) {
            break; // loop detected — leaf chain check will catch this
        }
        if node_idx as usize >= max_nodes {
            break;
        }
        let off = node_idx as usize * node_size;
        let node = &catalog_data[off..off + node_size];

        let kind = node[8] as i8;
        if kind != BTREE_MAP_NODE {
            errors.push(hfs_issue(
                HfsFsckCode::MapNodeBadStructure,
                format!(
                    "map node {} has kind {} (expected {} = map)",
                    node_idx, kind, BTREE_MAP_NODE
                ),
            ));
        }

        let num_records = BigEndian::read_u16(&node[10..12]) as usize;
        if num_records != 1 {
            errors.push(hfs_issue(
                HfsFsckCode::MapNodeBadStructure,
                format!(
                    "map node {} has {} records (expected 1)",
                    node_idx, num_records
                ),
            ));
        }

        // First record should start at offset 14 (right after node descriptor)
        if num_records >= 1 {
            let rec0_pos = node_size - 2;
            let rec0_off = BigEndian::read_u16(&node[rec0_pos..rec0_pos + 2]) as usize;
            if rec0_off != 14 {
                errors.push(hfs_issue(
                    HfsFsckCode::MapNodeBadStructure,
                    format!(
                        "map node {} record 0 offset = {} (expected 14)",
                        node_idx, rec0_off
                    ),
                ));
            }
        }

        node_idx = BigEndian::read_u32(&node[0..4]);
    }
}

/// Collect all node indices reachable from the B-tree root via child pointers
/// and leaf chain. Returns a set of node indices that should be bitmap-allocated.
fn collect_referenced_nodes(catalog_data: &[u8], header: &BTreeHeader) -> HashSet<u32> {
    let node_size = header.node_size as usize;
    let max_nodes = catalog_data.len() / node_size;
    let mut referenced = HashSet::new();

    // Node 0 (header) is always referenced
    referenced.insert(0u32);

    // Map nodes (fLink chain from node 0)
    let mut map_idx = BigEndian::read_u32(&catalog_data[0..4]);
    let mut visited = HashSet::new();
    while map_idx != 0 && visited.insert(map_idx) && (map_idx as usize) < max_nodes {
        referenced.insert(map_idx);
        let off = map_idx as usize * node_size;
        map_idx = BigEndian::read_u32(&catalog_data[off..off + 4]);
    }

    if header.root_node == 0 || header.depth == 0 {
        return referenced;
    }

    // Walk the index tree top-down, collecting all referenced nodes
    let mut level_nodes: Vec<u32> = vec![header.root_node];
    referenced.insert(header.root_node);

    for _level in (2..=header.depth).rev() {
        let mut next_level = Vec::new();
        for &node_idx in &level_nodes {
            let off = node_idx as usize * node_size;
            if off + node_size > catalog_data.len() {
                continue;
            }
            let node = &catalog_data[off..off + node_size];
            if node[8] as i8 != BTREE_INDEX_NODE {
                continue;
            }
            for child in extract_child_pointers(node, node_size) {
                if (child as usize) < max_nodes {
                    referenced.insert(child);
                }
                next_level.push(child);
            }
        }
        level_nodes = next_level;
    }

    // Walk the leaf chain
    let mut leaf_idx = header.first_leaf_node;
    let mut leaf_visited = HashSet::new();
    while leaf_idx != 0 && leaf_visited.insert(leaf_idx) && (leaf_idx as usize) < max_nodes {
        referenced.insert(leaf_idx);
        let off = leaf_idx as usize * node_size;
        leaf_idx = BigEndian::read_u32(&catalog_data[off..off + 4]);
    }

    referenced
}

/// Verify that every node referenced by the B-tree structure is marked as
/// allocated in the node bitmap. Missing bitmap bits mean the node could be
/// overwritten by a future allocation — a serious structural issue.
fn check_node_bitmap_consistency(
    catalog_data: &[u8],
    header: &BTreeHeader,
    errors: &mut Vec<FsckIssue>,
) {
    let node_size = header.node_size as usize;
    let (_bmp_off, bmp_size) = btree_node_bitmap_range(catalog_data, node_size);
    if bmp_size == 0 {
        return;
    }

    let referenced = collect_referenced_nodes(catalog_data, header);

    for &node_idx in &referenced {
        if !btree_bitmap_test(catalog_data, node_size, node_idx) {
            errors.push(hfs_issue(
                HfsFsckCode::NodeBitmapMissing,
                format!(
                    "node {} is referenced but not marked allocated in node bitmap",
                    node_idx
                ),
            ));
        }
    }
}

/// Compare two catalog key byte slices for ordering.
/// Each slice starts with key_len(1) + reserved(1) + parent_id(4) + name_len(1) + name.
fn compare_catalog_keys(a: &[u8], b: &[u8]) -> Ordering {
    super::hfs::HfsFilesystem::<std::io::Cursor<Vec<u8>>>::catalog_compare(a, b)
}

/// Extract the key portion of a record (key_len byte + key_len bytes of data).
pub(super) fn record_key(node: &[u8], rec_start: usize, rec_end: usize) -> &[u8] {
    if rec_start >= rec_end || rec_start >= node.len() {
        return &[];
    }
    let key_len = node[rec_start] as usize;
    let key_end = (rec_start + 1 + key_len).min(rec_end).min(node.len());
    &node[rec_start..key_end]
}

/// Verify that records within each allocated node are sorted in ascending key order.
fn check_key_ordering(catalog_data: &[u8], header: &BTreeHeader, errors: &mut Vec<FsckIssue>) {
    let node_size = header.node_size as usize;
    let max_nodes = catalog_data.len() / node_size;

    for node_idx in 0..max_nodes as u32 {
        if !btree_bitmap_test(catalog_data, node_size, node_idx) {
            continue;
        }
        let off = node_idx as usize * node_size;
        let node = &catalog_data[off..off + node_size];
        let kind = node[8] as i8;
        if kind != BTREE_LEAF_NODE && kind != BTREE_INDEX_NODE {
            continue;
        }
        let num_records = BigEndian::read_u16(&node[10..12]) as usize;
        if num_records < 2 {
            continue;
        }

        for i in 0..num_records - 1 {
            let (s1, e1) = btree_record_range(node, node_size, i);
            let (s2, e2) = btree_record_range(node, node_size, i + 1);
            let key_a = record_key(node, s1, e1);
            let key_b = record_key(node, s2, e2);
            if key_a.is_empty() || key_b.is_empty() {
                continue;
            }
            if compare_catalog_keys(key_a, key_b) != Ordering::Less {
                errors.push(hfs_issue(
                    HfsFsckCode::KeysOutOfOrder,
                    format!(
                        "node {} records {} and {}: keys not in ascending order",
                        node_idx,
                        i,
                        i + 1
                    ),
                ));
                break; // one error per node is enough
            }
        }
    }
}

fn check_offset_table(
    node: &[u8],
    node_size: usize,
    node_idx: u32,
    num_records: usize,
    errors: &mut Vec<FsckIssue>,
) {
    let mut prev_offset = 0u16;
    for i in 0..=num_records {
        let pos = node_size - 2 * (i + 1);
        if pos + 2 > node.len() {
            break;
        }
        let offset = BigEndian::read_u16(&node[pos..pos + 2]);

        if offset as usize >= node_size {
            errors.push(hfs_issue(
                HfsFsckCode::OffsetTableOutOfBounds,
                format!(
                    "node {}: offset[{}] = {} >= node_size {}",
                    node_idx, i, offset, node_size
                ),
            ));
            return; // no point checking further
        }

        if i > 0 && offset < prev_offset {
            errors.push(hfs_issue(
                HfsFsckCode::OffsetTableNotMonotonic,
                format!(
                    "node {}: offset[{}] = {} < offset[{}] = {} (not monotonic)",
                    node_idx,
                    i,
                    offset,
                    i - 1,
                    prev_offset
                ),
            ));
            return;
        }
        prev_offset = offset;
    }
}

/// Validate catalog index record lengths.
/// Mac OS forces all catalog index keys to length 0x25 (37), making every
/// index record exactly 42 bytes: 1 (key_len) + 37 (key data) + 4 (child ptr).
fn check_index_record_lengths(
    node: &[u8],
    node_size: usize,
    node_idx: u32,
    num_records: usize,
    errors: &mut Vec<FsckIssue>,
) {
    use super::hfs_common::HFS_CAT_MAX_KEY_LEN;

    // Expected: key_len(1) + key_data(0x25=37) + child_ptr(4) = 42
    let expected_len: usize = 1 + HFS_CAT_MAX_KEY_LEN as usize + 4; // 42

    for i in 0..num_records {
        let (rec_start, rec_end) = btree_record_range(node, node_size, i);
        if rec_start >= rec_end || rec_end > node_size {
            continue;
        }
        let rec_len = rec_end - rec_start;
        if rec_len < 5 {
            errors.push(hfs_issue(
                HfsFsckCode::InvalidRecordLength,
                format!(
                    "node {} record {}: index record too short ({})",
                    node_idx, i, rec_len
                ),
            ));
            continue;
        }
        let key_len = node[rec_start] as usize;
        if rec_len != expected_len || key_len != HFS_CAT_MAX_KEY_LEN as usize {
            errors.push(hfs_issue(
                HfsFsckCode::InvalidRecordLength,
                format!(
                    "node {} record {}: index record length {} key_len {} (expected {} key_len {})",
                    node_idx, i, rec_len, key_len, expected_len, HFS_CAT_MAX_KEY_LEN
                ),
            ));
        }
    }
}

/// Validate leaf catalog record lengths against expected sizes for each record type.
fn check_leaf_record_lengths(
    node: &[u8],
    node_size: usize,
    node_idx: u32,
    num_records: usize,
    errors: &mut Vec<FsckIssue>,
) {
    for i in 0..num_records {
        let (rec_start, rec_end) = btree_record_range(node, node_size, i);
        if rec_start >= rec_end || rec_end > node_size {
            continue;
        }
        let rec_len = rec_end - rec_start;
        if rec_len < 7 {
            errors.push(hfs_issue(
                HfsFsckCode::InvalidRecordLength,
                format!(
                    "node {} record {}: leaf record too short ({})",
                    node_idx, i, rec_len
                ),
            ));
            continue;
        }
        let key_len = node[rec_start] as usize;
        let key_total = 1 + key_len;
        if key_total >= rec_len {
            continue;
        }
        let mut data_start = key_total;
        if data_start % 2 != 0 {
            data_start += 1;
        }
        if data_start >= rec_len {
            continue;
        }
        let rec_type = node[rec_start + data_start] as i8;
        let data_len = rec_len - data_start;
        // Minimum data sizes: dir record = 70, file record = 102,
        // thread records = at least 46 (type + reserved + parentID + name)
        // Expected data sizes per hfsutils:
        // dir record = 70, file record = 102,
        // thread records = exactly 46 (type(1)+rsv(1)+rsv(8)+parentID(4)+Str31(32))
        let (expected, exact) = match rec_type {
            1 => (70, true),     // CATALOG_DIR - fixed size
            2 => (102, true),    // CATALOG_FILE - fixed size
            3 | 4 => (46, true), // dir/file thread - fixed Str31 field
            _ => continue,       // unknown type, skip
        };
        if (exact && data_len != expected) || (!exact && data_len < expected) {
            errors.push(hfs_issue(
                HfsFsckCode::InvalidRecordLength,
                format!(
                    "node {} record {}: type {} data length {} (expected {})",
                    node_idx, i, rec_type, data_len, expected
                ),
            ));
        }
    }
}

/// Walk the leaf node chain from first_leaf to last_leaf.
/// Returns the total number of leaf records encountered.
fn walk_leaf_chain(
    catalog_data: &[u8],
    header: &BTreeHeader,
    errors: &mut Vec<FsckIssue>,
    leaf_nodes_visited: &mut u32,
) -> u32 {
    let node_size = header.node_size as usize;
    let mut total_records = 0u32;
    let mut node_idx = header.first_leaf_node;
    let mut prev_idx: u32 = 0;
    let mut visited = HashSet::new();

    while node_idx != 0 {
        if !visited.insert(node_idx) {
            errors.push(hfs_issue(
                HfsFsckCode::LeafChainBroken,
                format!("leaf chain loop detected at node {}", node_idx),
            ));
            break;
        }

        let off = node_idx as usize * node_size;
        if off + node_size > catalog_data.len() {
            errors.push(hfs_issue(
                HfsFsckCode::LeafChainBroken,
                format!("leaf node {} offset out of range", node_idx),
            ));
            break;
        }

        let node = &catalog_data[off..off + node_size];
        let flink = BigEndian::read_u32(&node[0..4]);
        let blink = BigEndian::read_u32(&node[4..8]);
        let kind = node[8] as i8;
        let num_records = BigEndian::read_u16(&node[10..12]) as u32;

        if kind != BTREE_LEAF_NODE {
            errors.push(hfs_issue(
                HfsFsckCode::LeafChainBroken,
                format!(
                    "node {} in leaf chain has kind {} (expected leaf = {})",
                    node_idx, kind, BTREE_LEAF_NODE
                ),
            ));
            break;
        }

        // Verify backlink
        if *leaf_nodes_visited > 0 && blink != prev_idx {
            errors.push(hfs_issue(
                HfsFsckCode::LeafBacklinkMismatch,
                format!(
                    "node {}: bLink = {} but previous node was {}",
                    node_idx, blink, prev_idx
                ),
            ));
        }

        total_records += num_records;
        *leaf_nodes_visited += 1;
        prev_idx = node_idx;
        node_idx = flink;
    }

    total_records
}

/// Extract all child node pointers from an index node.
pub(super) fn extract_child_pointers(node: &[u8], node_size: usize) -> Vec<u32> {
    let num_records = BigEndian::read_u16(&node[10..12]) as usize;
    let mut children = Vec::new();
    for i in 0..num_records {
        let (rec_start, rec_end) = btree_record_range(node, node_size, i);
        if rec_start >= rec_end || rec_end > node_size {
            continue;
        }
        let rec = &node[rec_start..rec_end];
        if rec.len() < 5 {
            continue;
        }
        let key_len = rec[0] as usize;
        let mut ptr_off = 1 + key_len;
        if ptr_off % 2 != 0 {
            ptr_off += 1;
        }
        if ptr_off + 4 <= rec.len() {
            children.push(BigEndian::read_u32(&rec[ptr_off..ptr_off + 4]));
        }
    }
    children
}

/// Walk index nodes from the root down, verifying fLink/bLink consistency at each level.
///
/// Instead of following the (potentially broken) sibling chain, we discover
/// the expected set of nodes at each level from the parent's child pointers,
/// then verify those nodes are properly chained via fLink/bLink.
fn check_index_sibling_links(
    catalog_data: &[u8],
    header: &BTreeHeader,
    errors: &mut Vec<FsckIssue>,
) {
    let node_size = header.node_size as usize;
    if header.depth <= 1 || header.root_node == 0 {
        return; // no index nodes
    }

    // Start with the root as the sole node at the top level
    let mut level_nodes: Vec<u32> = vec![header.root_node];

    // Walk down from the root level (depth) to level 2 (one above leaves)
    for _level in (2..=header.depth).rev() {
        if level_nodes.is_empty() {
            break;
        }

        // Verify sibling links among nodes at this level
        for (i, &node_idx) in level_nodes.iter().enumerate() {
            let off = node_idx as usize * node_size;
            if off + node_size > catalog_data.len() {
                errors.push(hfs_issue(
                    HfsFsckCode::IndexSiblingLinkBroken,
                    format!("index node {} offset out of range", node_idx),
                ));
                continue;
            }

            let node = &catalog_data[off..off + node_size];
            let flink = BigEndian::read_u32(&node[0..4]);
            let blink = BigEndian::read_u32(&node[4..8]);

            let expected_flink = if i + 1 < level_nodes.len() {
                level_nodes[i + 1]
            } else {
                0
            };
            let expected_blink = if i > 0 { level_nodes[i - 1] } else { 0 };

            if flink != expected_flink {
                errors.push(hfs_issue(
                    HfsFsckCode::IndexSiblingLinkBroken,
                    format!(
                        "index node {}: fLink = {} but expected {}",
                        node_idx, flink, expected_flink
                    ),
                ));
            }
            if blink != expected_blink {
                errors.push(hfs_issue(
                    HfsFsckCode::IndexSiblingLinkBroken,
                    format!(
                        "index node {}: bLink = {} but expected {}",
                        node_idx, blink, expected_blink
                    ),
                ));
            }
        }

        // Collect all child pointers from this level to get the next level's nodes
        let mut next_level = Vec::new();
        for &node_idx in &level_nodes {
            let off = node_idx as usize * node_size;
            if off + node_size > catalog_data.len() {
                continue;
            }
            let node = &catalog_data[off..off + node_size];
            if node[8] as i8 != BTREE_INDEX_NODE {
                continue;
            }
            next_level.extend(extract_child_pointers(node, node_size));
        }

        level_nodes = next_level;
    }
}

// ---------------------------------------------------------------------------
// Phase 3: Catalog Consistency
// ---------------------------------------------------------------------------

/// Parsed catalog key for consistency checking.
struct CatalogKey {
    parent_id: u32,
    name: Vec<u8>, // raw Mac Roman bytes
}

/// Parsed catalog record for consistency checking.
enum CatalogEntry {
    Directory {
        key: CatalogKey,
        dir_id: u32,
        valence: u16,
    },
    File {
        key: CatalogKey,
        file_id: u32,
        data_extents: [HfsExtDescriptor; 3],
        data_size: u32,
        data_physical_size: u32,
        rsrc_extents: [HfsExtDescriptor; 3],
        rsrc_size: u32,
        rsrc_physical_size: u32,
        reserved_byte: u8,
    },
    DirThread {
        key_cnid: u32,
        parent_id: u32,
        name: Vec<u8>,
        key_name_len: usize,
    },
    FileThread {
        key_cnid: u32,
        parent_id: u32,
        name: Vec<u8>,
        key_name_len: usize,
    },
}

/// Walk parent chains to detect directory loops and excessive nesting depth.
/// `dir_records` maps dir_id → (parent_id, name, valence).
fn check_directory_structure(
    dir_records: &HashMap<u32, (u32, Vec<u8>, u16)>,
    errors: &mut Vec<FsckIssue>,
    warnings: &mut Vec<FsckIssue>,
) {
    const MAX_NESTING: usize = 100;
    const MAX_HOPS: usize = 200;

    // Cache: dir_id → known depth (None if involved in a loop)
    let mut depth_cache: HashMap<u32, Option<usize>> = HashMap::new();
    // Root dir (CNID 2) has depth 0 if it exists; parent_id=1 is the virtual root parent.
    depth_cache.insert(2, Some(0));

    for &dir_id in dir_records.keys() {
        if depth_cache.contains_key(&dir_id) {
            continue;
        }

        // Walk toward root, collecting the path
        let mut path: Vec<u32> = Vec::new();
        let mut current = dir_id;
        let mut depth = None;

        for _ in 0..MAX_HOPS {
            if let Some(cached) = depth_cache.get(&current) {
                // We've reached a node with a known depth
                depth = cached.map(|d| d + path.len());
                break;
            }
            if current == 2 || current == 1 {
                // Reached root
                depth = Some(path.len());
                break;
            }
            if path.contains(&current) {
                // Loop detected
                errors.push(hfs_issue(
                    HfsFsckCode::DirectoryLoop,
                    format!("directory loop detected involving CNID {}", current),
                ));
                // Mark all nodes in the loop
                for &node in &path {
                    depth_cache.insert(node, None);
                }
                depth_cache.insert(current, None);
                break;
            }
            path.push(current);
            if let Some(&(parent_id, _, _)) = dir_records.get(&current) {
                current = parent_id;
            } else {
                // Parent not found — stop (MissingParent handles this separately)
                depth = Some(path.len());
                break;
            }
        }

        // Cache depths for all nodes in the path
        if let Some(base_depth) = depth {
            for (i, &node) in path.iter().enumerate() {
                let node_depth = base_depth - i;
                depth_cache.insert(node, Some(node_depth));
            }
        }
    }

    // Check for excessive nesting
    for (&dir_id, &cached_depth) in &depth_cache {
        if let Some(d) = cached_depth {
            if d > MAX_NESTING {
                let name = dir_records
                    .get(&dir_id)
                    .map(|(_, n, _)| mac_roman_to_utf8(n))
                    .unwrap_or_default();
                warnings.push(hfs_issue(
                    HfsFsckCode::NestingDepthWarning,
                    format!(
                        "directory \"{}\" (CNID {}) nesting depth {} exceeds recommended maximum of {}",
                        name, dir_id, d, MAX_NESTING
                    ),
                ));
            }
        }
    }
}

fn check_catalog_consistency(
    mdb: &HfsMasterDirectoryBlock,
    catalog_data: &[u8],
    header: &BTreeHeader,
    errors: &mut Vec<FsckIssue>,
    warnings: &mut Vec<FsckIssue>,
    files_checked: &mut u32,
    directories_checked: &mut u32,
    orphaned_threads: &mut u32,
    orphaned_entries: &mut Vec<OrphanedEntry>,
) {
    // Scan all leaf records
    let entries = collect_catalog_entries(catalog_data, header);

    // Build lookup maps
    // dir_records: dir_id → (parent_id, name_bytes, valence)
    let mut dir_records: HashMap<u32, (u32, Vec<u8>, u16)> = HashMap::new();
    // file_records: file_id → (parent_id, name_bytes)
    let mut file_records: HashMap<u32, (u32, Vec<u8>)> = HashMap::new();
    // dir_threads: cnid → (parent_id, name_bytes)
    let mut dir_threads: HashMap<u32, (u32, Vec<u8>)> = HashMap::new();
    // file_threads: cnid → (parent_id, name_bytes)
    let mut file_threads: HashMap<u32, (u32, Vec<u8>)> = HashMap::new();
    // children_count: parent_id → number of child records (files + dirs)
    let mut children_count: HashMap<u32, u32> = HashMap::new();

    let mut max_cnid: u32 = 0;

    for entry in &entries {
        match entry {
            CatalogEntry::Directory {
                key,
                dir_id,
                valence,
            } => {
                dir_records.insert(*dir_id, (key.parent_id, key.name.clone(), *valence));
                *children_count.entry(key.parent_id).or_insert(0) += 1;
                if *dir_id > max_cnid {
                    max_cnid = *dir_id;
                }
                // Gap 2: CNID range — must be >= 16 or == 2 (root)
                if *dir_id != 2 && *dir_id < 16 {
                    errors.push(hfs_issue(
                        HfsFsckCode::InvalidCnidRange,
                        format!(
                            "directory CNID {} is in the reserved range (must be >= 16 or == 2)",
                            dir_id
                        ),
                    ));
                }
                // Gap 10: Catalog name validation
                // Skip for root directory (CNID 2) — its key name is the volume name
                // which may legitimately be empty in some implementations.
                if *dir_id != 2 {
                    if let Some(problem) = validate_hfs_name(&key.name) {
                        errors.push(hfs_issue(
                            HfsFsckCode::InvalidCatalogName,
                            format!("directory CNID {}: {}", dir_id, problem),
                        ));
                    }
                }
                *directories_checked += 1;
            }
            CatalogEntry::File {
                key,
                file_id,
                data_size,
                data_physical_size,
                rsrc_size,
                rsrc_physical_size,
                reserved_byte,
                ..
            } => {
                file_records.insert(*file_id, (key.parent_id, key.name.clone()));
                *children_count.entry(key.parent_id).or_insert(0) += 1;
                if *file_id > max_cnid {
                    max_cnid = *file_id;
                }
                // Gap 2: CNID range — must be >= 16
                if *file_id < 16 {
                    errors.push(hfs_issue(
                        HfsFsckCode::InvalidCnidRange,
                        format!(
                            "file CNID {} is in the reserved range (must be >= 16)",
                            file_id
                        ),
                    ));
                }
                // Gap 10: Catalog name validation
                if let Some(problem) = validate_hfs_name(&key.name) {
                    errors.push(hfs_issue(
                        HfsFsckCode::InvalidCatalogName,
                        format!("file CNID {}: {}", file_id, problem),
                    ));
                }
                // Gap 1: LEOF > PEOF — logical size must not exceed physical size
                if *data_size > *data_physical_size {
                    errors.push(hfs_issue(
                        HfsFsckCode::LeoFExceedsPeoF,
                        format!(
                            "file CNID {}: data fork logical size {} > physical size {}",
                            file_id, data_size, data_physical_size
                        ),
                    ));
                }
                if *rsrc_size > *rsrc_physical_size {
                    errors.push(hfs_issue(
                        HfsFsckCode::LeoFExceedsPeoF,
                        format!(
                            "file CNID {}: resource fork logical size {} > physical size {}",
                            file_id, rsrc_size, rsrc_physical_size
                        ),
                    ));
                }
                // Gap 3: Reserved fields must be zero
                if *reserved_byte != 0 {
                    warnings.push(hfs_issue(
                        HfsFsckCode::ReservedFieldNonZero,
                        format!(
                            "file CNID {}: reserved byte is 0x{:02X} (expected 0)",
                            file_id, reserved_byte
                        ),
                    ));
                }
                // filStBlk / filRStBlk are NOT reserved — they hold the first
                // allocation block of the data and resource forks respectively.
                // Non-zero values are normal for any file that has data.
                *files_checked += 1;
            }
            CatalogEntry::DirThread {
                key_cnid,
                parent_id,
                name,
                key_name_len,
            } => {
                dir_threads.insert(*key_cnid, (*parent_id, name.clone()));
                // Gap 11: Thread key name must be zero-length
                if *key_name_len != 0 {
                    errors.push(hfs_issue(
                        HfsFsckCode::ThreadNameNotEmpty,
                        format!(
                            "directory thread CNID {}: key name length is {} (must be 0)",
                            key_cnid, key_name_len
                        ),
                    ));
                }
            }
            CatalogEntry::FileThread {
                key_cnid,
                parent_id,
                name,
                key_name_len,
            } => {
                file_threads.insert(*key_cnid, (*parent_id, name.clone()));
                // Gap 11: Thread key name must be zero-length
                if *key_name_len != 0 {
                    errors.push(hfs_issue(
                        HfsFsckCode::ThreadNameNotEmpty,
                        format!(
                            "file thread CNID {}: key name length is {} (must be 0)",
                            key_cnid, key_name_len
                        ),
                    ));
                }
            }
        }
    }

    // Check next_catalog_id > max CNID
    if mdb.next_catalog_id <= max_cnid {
        errors.push(hfs_issue(
            HfsFsckCode::NextCatalogIdTooLow,
            format!(
                "next_catalog_id {} <= max CNID {} found in catalog",
                mdb.next_catalog_id, max_cnid
            ),
        ));
    }

    // Verify thread ↔ record consistency for directories
    for (&dir_id, &(parent_id, ref name, _valence)) in &dir_records {
        if dir_id == 2 {
            // Root folder — thread should exist but parent/name rules are special
            if !dir_threads.contains_key(&dir_id) {
                errors.push(hfs_issue(
                    HfsFsckCode::MissingDirThread,
                    format!("directory CNID {} has no thread record", dir_id),
                ));
            }
            continue;
        }
        match dir_threads.get(&dir_id) {
            None => {
                errors.push(hfs_issue(
                    HfsFsckCode::MissingDirThread,
                    format!("directory CNID {} has no thread record", dir_id),
                ));
            }
            Some((thr_parent, thr_name)) => {
                if *thr_parent != parent_id || *thr_name != *name {
                    errors.push(hfs_issue(HfsFsckCode::ThreadParentNameMismatch, format!(
                            "directory CNID {} thread points to parent {} but record key has parent {}",
                            dir_id, thr_parent, parent_id
                        )));
                }
            }
        }
        // Verify parent exists
        if parent_id != 1 && !dir_records.contains_key(&parent_id) {
            let decoded_name = mac_roman_to_utf8(name);
            errors.push(hfs_issue(
                HfsFsckCode::MissingParent,
                format!(
                    "directory \"{}\" (CNID {}) references parent {} which doesn't exist",
                    decoded_name, dir_id, parent_id
                ),
            ));
            orphaned_entries.push(OrphanedEntry {
                id: dir_id as u64,
                name: decoded_name,
                is_directory: true,
                missing_parent_id: parent_id as u64,
            });
        }
    }

    // Verify thread ↔ record consistency for files
    // Note: in classic HFS, file thread records are OPTIONAL. Many real volumes
    // don't have them. Missing file threads are a warning, not an error.
    for (&file_id, &(parent_id, ref name)) in &file_records {
        if let Some((thr_parent, thr_name)) = file_threads.get(&file_id) {
            // Thread exists — verify it matches the record
            if *thr_parent != parent_id || *thr_name != *name {
                errors.push(hfs_issue(
                    HfsFsckCode::ThreadParentNameMismatch,
                    format!(
                        "file CNID {} thread points to parent {} but record key has parent {}",
                        file_id, thr_parent, parent_id
                    ),
                ));
            }
        }
        // Not having a file thread is normal in classic HFS — no warning needed.

        // Verify parent exists
        if parent_id != 1 && !dir_records.contains_key(&parent_id) {
            let decoded_name = mac_roman_to_utf8(name);
            errors.push(hfs_issue(
                HfsFsckCode::MissingParent,
                format!(
                    "file \"{}\" (CNID {}) references parent {} which doesn't exist",
                    decoded_name, file_id, parent_id
                ),
            ));
            orphaned_entries.push(OrphanedEntry {
                id: file_id as u64,
                name: decoded_name,
                is_directory: false,
                missing_parent_id: parent_id as u64,
            });
        }
    }

    // Check for orphaned threads (threads with no matching record)
    for &cnid in dir_threads.keys() {
        if !dir_records.contains_key(&cnid) {
            *orphaned_threads += 1;
            warnings.push(hfs_issue(
                HfsFsckCode::OrphanedThread,
                format!("orphaned directory thread for CNID {}", cnid),
            ));
        }
    }
    for &cnid in file_threads.keys() {
        if !file_records.contains_key(&cnid) {
            *orphaned_threads += 1;
            warnings.push(hfs_issue(
                HfsFsckCode::OrphanedThread,
                format!("orphaned file thread for CNID {}", cnid),
            ));
        }
    }

    // Gap 12/13: Directory loop detection and nesting depth
    check_directory_structure(&dir_records, errors, warnings);

    // Verify file_count and folder_count match MDB
    // HFS drDirCnt does NOT include the root directory (CNID 2),
    // so subtract 1 from catalog count (which includes root).
    let actual_file_count = *files_checked;
    let actual_folder_count = (*directories_checked).saturating_sub(1);
    if actual_file_count != mdb.file_count {
        errors.push(hfs_issue(
            HfsFsckCode::FileCountMismatch,
            format!(
                "MDB file_count = {} but catalog has {} file records",
                mdb.file_count, actual_file_count
            ),
        ));
    }
    if actual_folder_count != mdb.folder_count {
        errors.push(hfs_issue(
            HfsFsckCode::FolderCountMismatch,
            format!(
                "MDB folder_count = {} but catalog has {} directory records (excluding root)",
                mdb.folder_count, actual_folder_count
            ),
        ));
    }

    // Verify valence matches actual children count
    for (&dir_id, &(_parent_id, ref _name, valence)) in &dir_records {
        let actual = children_count.get(&dir_id).copied().unwrap_or(0);
        if actual != valence as u32 {
            errors.push(hfs_issue(
                HfsFsckCode::ValenceMismatch,
                format!(
                    "directory CNID {} valence = {} but has {} children",
                    dir_id, valence, actual
                ),
            ));
        }
    }
}

/// Scan all leaf nodes and collect catalog entries.
fn collect_catalog_entries(catalog_data: &[u8], header: &BTreeHeader) -> Vec<CatalogEntry> {
    let node_size = header.node_size as usize;
    let mut entries = Vec::new();
    let mut node_idx = header.first_leaf_node;
    let mut visited = HashSet::new();

    while node_idx != 0 {
        if !visited.insert(node_idx) {
            break;
        }
        let off = node_idx as usize * node_size;
        if off + node_size > catalog_data.len() {
            break;
        }
        let node = &catalog_data[off..off + node_size];
        let flink = BigEndian::read_u32(&node[0..4]);
        let num_records = BigEndian::read_u16(&node[10..12]) as usize;

        for i in 0..num_records {
            if let Some(entry) = parse_catalog_record(node, node_size, i) {
                entries.push(entry);
            }
        }

        node_idx = flink;
    }

    entries
}

/// Parse a single catalog record from a leaf node.
fn parse_catalog_record(node: &[u8], node_size: usize, rec_idx: usize) -> Option<CatalogEntry> {
    let (rec_start, _rec_end) = btree_record_range(node, node_size, rec_idx);
    if rec_start + 7 > node_size {
        return None;
    }

    let key_len = node[rec_start] as usize;
    if key_len < 6 || rec_start + 1 + key_len > node_size {
        return None;
    }

    let key_data = &node[rec_start + 1..rec_start + 1 + key_len];
    // key_data: reserved(1) + parent_id(4) + name_len(1) + name(N)
    let parent_id = BigEndian::read_u32(&key_data[1..5]);
    let name_len = key_data[5] as usize;
    let name_bytes = if name_len > 0 && 6 + name_len <= key_data.len() {
        key_data[6..6 + name_len].to_vec()
    } else {
        Vec::new()
    };

    // Record data follows key (aligned to even boundary)
    let mut rec_data_offset = rec_start + 1 + key_len;
    #[allow(clippy::manual_is_multiple_of)]
    if rec_data_offset % 2 != 0 {
        rec_data_offset += 1;
    }
    if rec_data_offset + 1 > node_size {
        return None;
    }

    let record_type = node[rec_data_offset] as i8;

    match record_type {
        CATALOG_DIR => {
            if rec_data_offset + 70 > node_size {
                return None;
            }
            let rec = &node[rec_data_offset..];
            let valence = BigEndian::read_u16(&rec[4..6]);
            let dir_id = BigEndian::read_u32(&rec[6..10]);
            Some(CatalogEntry::Directory {
                key: CatalogKey {
                    parent_id,
                    name: name_bytes,
                },
                dir_id,
                valence,
            })
        }
        CATALOG_FILE => {
            if rec_data_offset + 102 > node_size {
                return None;
            }
            let rec = &node[rec_data_offset..];
            let reserved_byte = rec[1];
            let file_id = BigEndian::read_u32(&rec[20..24]);
            let data_size = BigEndian::read_u32(&rec[26..30]);
            let data_physical_size = BigEndian::read_u32(&rec[30..34]);
            let rsrc_size = BigEndian::read_u32(&rec[36..40]);
            let rsrc_physical_size = BigEndian::read_u32(&rec[40..44]);
            let mut data_extents = [HfsExtDescriptor {
                start_block: 0,
                block_count: 0,
            }; 3];
            for j in 0..3 {
                data_extents[j] = HfsExtDescriptor::parse(&rec[74 + j * 4..78 + j * 4]);
            }
            let mut rsrc_extents = [HfsExtDescriptor {
                start_block: 0,
                block_count: 0,
            }; 3];
            for j in 0..3 {
                rsrc_extents[j] = HfsExtDescriptor::parse(&rec[86 + j * 4..90 + j * 4]);
            }
            Some(CatalogEntry::File {
                key: CatalogKey {
                    parent_id,
                    name: name_bytes,
                },
                file_id,
                data_extents,
                data_size,
                data_physical_size,
                rsrc_extents,
                rsrc_size,
                rsrc_physical_size,
                reserved_byte,
            })
        }
        CATALOG_DIR_THREAD => {
            if rec_data_offset + 14 > node_size {
                return None;
            }
            let rec = &node[rec_data_offset..];
            let thr_parent_id = BigEndian::read_u32(&rec[10..14]);
            let thr_name_len = if rec_data_offset + 15 <= node_size {
                rec[14] as usize
            } else {
                0
            };
            let thr_name = if thr_name_len > 0 && rec_data_offset + 15 + thr_name_len <= node_size {
                rec[15..15 + thr_name_len].to_vec()
            } else {
                Vec::new()
            };
            Some(CatalogEntry::DirThread {
                key_cnid: parent_id, // thread key uses CNID as "parent_id" field
                parent_id: thr_parent_id,
                name: thr_name,
                key_name_len: name_len,
            })
        }
        CATALOG_FILE_THREAD => {
            if rec_data_offset + 14 > node_size {
                return None;
            }
            let rec = &node[rec_data_offset..];
            let thr_parent_id = BigEndian::read_u32(&rec[10..14]);
            let thr_name_len = if rec_data_offset + 15 <= node_size {
                rec[14] as usize
            } else {
                0
            };
            let thr_name = if thr_name_len > 0 && rec_data_offset + 15 + thr_name_len <= node_size {
                rec[15..15 + thr_name_len].to_vec()
            } else {
                Vec::new()
            };
            Some(CatalogEntry::FileThread {
                key_cnid: parent_id,
                parent_id: thr_parent_id,
                name: thr_name,
                key_name_len: name_len,
            })
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Phase 4: Extent/Allocation Bitmap
// ---------------------------------------------------------------------------

/// Collect all extent records from the extents overflow B-tree.
///
/// The extents overflow B-tree stores additional extents for files/forks that
/// need more than 3 extents. Each leaf record key is:
///   keyLength(1) + forkType(1) + fileID(4) + startBlock(2) = 8 bytes
/// Each record contains 3 extent descriptors (12 bytes).
fn collect_overflow_extents(extents_data: &[u8]) -> Vec<(u32, u32, String)> {
    let mut result = Vec::new();

    if extents_data.len() < 512 {
        return result;
    }

    // Read B-tree header
    let header = BTreeHeader::read(extents_data);
    let node_size = header.node_size as usize;
    if node_size == 0 || extents_data.len() < node_size {
        return result;
    }

    // Walk leaf chain
    let mut node_idx = header.first_leaf_node;
    let mut visited = HashSet::new();

    while node_idx != 0 {
        if !visited.insert(node_idx) {
            break;
        }
        let off = node_idx as usize * node_size;
        if off + node_size > extents_data.len() {
            break;
        }
        let node = &extents_data[off..off + node_size];
        let flink = BigEndian::read_u32(&node[0..4]);
        let kind = node[8] as i8;
        if kind != BTREE_LEAF_NODE {
            break;
        }
        let num_records = BigEndian::read_u16(&node[10..12]) as usize;

        for i in 0..num_records {
            let (rec_start, _rec_end) = btree_record_range(node, node_size, i);
            if rec_start + 8 > node_size {
                continue;
            }
            // Key: keyLength(1) + forkType(1) + fileID(4) + startBlock(2)
            let key_len = node[rec_start] as usize;
            if key_len < 7 || rec_start + 1 + key_len > node_size {
                continue;
            }
            let fork_type = node[rec_start + 1]; // 0x00 = data, 0xFF = resource
            let file_id = BigEndian::read_u32(&node[rec_start + 2..rec_start + 6]);

            // Record data follows key (aligned to even)
            let mut data_off = rec_start + 1 + key_len;
            #[allow(clippy::manual_is_multiple_of)]
            if data_off % 2 != 0 {
                data_off += 1;
            }
            // 3 extent descriptors × 4 bytes = 12 bytes
            if data_off + 12 > node_size {
                continue;
            }

            let fork_label = if fork_type == 0xFF {
                "resource fork"
            } else {
                "data fork"
            };

            for j in 0..3 {
                let ext = HfsExtDescriptor::parse(&node[data_off + j * 4..data_off + j * 4 + 4]);
                if ext.block_count > 0 {
                    result.push((
                        ext.start_block as u32,
                        ext.block_count as u32,
                        format!("file {} {} (overflow)", file_id, fork_label),
                    ));
                }
            }
        }

        node_idx = flink;
    }

    result
}

/// Collect extents belonging to the bad block file (CNID 5) from the extents overflow B-tree.
fn collect_bad_block_extents(extents_data: &[u8]) -> Vec<(u32, u32)> {
    const BAD_BLOCK_FILE_ID: u32 = 5;
    let mut result = Vec::new();

    if extents_data.len() < 512 {
        return result;
    }

    let header = BTreeHeader::read(extents_data);
    let node_size = header.node_size as usize;
    if node_size == 0 || extents_data.len() < node_size {
        return result;
    }

    let mut node_idx = header.first_leaf_node;
    let mut visited = HashSet::new();

    while node_idx != 0 {
        if !visited.insert(node_idx) {
            break;
        }
        let off = node_idx as usize * node_size;
        if off + node_size > extents_data.len() {
            break;
        }
        let node = &extents_data[off..off + node_size];
        let flink = BigEndian::read_u32(&node[0..4]);
        let kind = node[8] as i8;
        if kind != BTREE_LEAF_NODE {
            break;
        }
        let num_records = BigEndian::read_u16(&node[10..12]) as usize;

        for i in 0..num_records {
            let (rec_start, _rec_end) = btree_record_range(node, node_size, i);
            if rec_start + 8 > node_size {
                continue;
            }
            let key_len = node[rec_start] as usize;
            if key_len < 7 || rec_start + 1 + key_len > node_size {
                continue;
            }
            let file_id = BigEndian::read_u32(&node[rec_start + 2..rec_start + 6]);
            if file_id != BAD_BLOCK_FILE_ID {
                continue;
            }

            let mut data_off = rec_start + 1 + key_len;
            #[allow(clippy::manual_is_multiple_of)]
            if data_off % 2 != 0 {
                data_off += 1;
            }
            if data_off + 12 > node_size {
                continue;
            }

            for j in 0..3 {
                let ext = HfsExtDescriptor::parse(&node[data_off + j * 4..data_off + j * 4 + 4]);
                if ext.block_count > 0 {
                    result.push((ext.start_block as u32, ext.block_count as u32));
                }
            }
        }

        node_idx = flink;
    }

    result
}

fn check_extents_and_bitmap(
    mdb: &HfsMasterDirectoryBlock,
    catalog_data: &[u8],
    header: &BTreeHeader,
    bitmap: &[u8],
    extents_data: Option<&[u8]>,
    errors: &mut Vec<FsckIssue>,
    warnings: &mut Vec<FsckIssue>,
) {
    let total_blocks = mdb.total_blocks as u32;

    // Compute the set of blocks that should be allocated based on all known extents
    let mut computed_bitmap = vec![0u8; (total_blocks as usize).div_ceil(8)];

    // Mark catalog file blocks
    mark_extents(
        &mdb.catalog_file_extents,
        total_blocks,
        &mut computed_bitmap,
        errors,
        "catalog file",
    );

    // Mark extents overflow file blocks
    mark_extents(
        &mdb.extents_file_extents,
        total_blocks,
        &mut computed_bitmap,
        errors,
        "extents overflow file",
    );

    // Collect all file extents from catalog (inline 3 extents per fork)
    let entries = collect_catalog_entries(catalog_data, header);
    let mut extent_blocks: Vec<(u32, u32, String)> = Vec::new();

    for entry in &entries {
        if let CatalogEntry::File {
            file_id,
            data_extents,
            data_size,
            rsrc_extents,
            rsrc_size,
            ..
        } = entry
        {
            if *data_size > 0 {
                for ext in data_extents {
                    if ext.block_count > 0 {
                        extent_blocks.push((
                            ext.start_block as u32,
                            ext.block_count as u32,
                            format!("file {} data fork", file_id),
                        ));
                    }
                }
            }
            if *rsrc_size > 0 {
                for ext in rsrc_extents {
                    if ext.block_count > 0 {
                        extent_blocks.push((
                            ext.start_block as u32,
                            ext.block_count as u32,
                            format!("file {} resource fork", file_id),
                        ));
                    }
                }
            }
        }
    }

    // Collect overflow extents (files with >3 extents per fork)
    if let Some(ext_data) = extents_data {
        let overflow = collect_overflow_extents(ext_data);
        extent_blocks.extend(overflow);
    }

    // Check extents are in range and mark them
    for (start, count, ref label) in &extent_blocks {
        if *start + *count > total_blocks {
            errors.push(hfs_issue(
                HfsFsckCode::ExtentOutOfRange,
                format!(
                    "{}: extent [{}, +{}) exceeds total_blocks {}",
                    label, start, count, total_blocks
                ),
            ));
            continue;
        }
        for b in *start..*start + *count {
            if bitmap_test_bit_be(&computed_bitmap, b) {
                errors.push(hfs_issue(
                    HfsFsckCode::OverlappingExtents,
                    format!("{}: block {} already allocated by another extent", label, b),
                ));
                break; // one report per extent is enough
            }
            super::hfs_common::bitmap_set_bit_be(&mut computed_bitmap, b);
        }
    }

    // Compare computed bitmap vs actual volume bitmap.
    //
    // "allocated in catalog but free in bitmap" is a real warning — a file's
    // data block isn't protected, so it could be overwritten.
    //
    // "free in catalog but allocated in bitmap" is harmless — just a small
    // amount of wasted space.  DiskWarrior also leaves these behind after
    // directory rebuilds.  We report them as debug-level only.
    let mut real_mismatch_count = 0usize;
    let mut wasted_block_count = 0usize;
    for block in 0..total_blocks {
        let computed = bitmap_test_bit_be(&computed_bitmap, block);
        let actual = bitmap_test_bit_be(bitmap, block);
        if computed != actual {
            if computed {
                // File references this block but bitmap says free — real problem
                real_mismatch_count += 1;
                if real_mismatch_count <= MAX_BITMAP_MISMATCHES {
                    warnings.push(hfs_issue(
                        HfsFsckCode::BitmapMismatch,
                        format!("block {}: allocated in catalog but free in bitmap", block),
                    ));
                }
            } else {
                // Bitmap says allocated but no file uses it — just wasted space
                wasted_block_count += 1;
            }
        }
    }
    if real_mismatch_count > MAX_BITMAP_MISMATCHES {
        warnings.push(hfs_issue(
            HfsFsckCode::BitmapMismatch,
            format!(
                "... and {} more bitmap mismatches (total {})",
                real_mismatch_count - MAX_BITMAP_MISMATCHES,
                real_mismatch_count
            ),
        ));
    }
    if wasted_block_count > 0 {
        warnings.push(hfs_debug_issue(
            HfsFsckCode::BitmapMismatch,
            format!(
                "{} block(s) marked allocated in bitmap but not referenced by any file \
                 (probably just small wasted space)",
                wasted_block_count
            ),
        ));
    }

    // Gap 15: Bad block file (CNID 5) extents must be marked allocated in the on-disk bitmap
    if let Some(ext_data) = extents_data {
        let bad_block_extents = collect_bad_block_extents(ext_data);
        for (start, count) in &bad_block_extents {
            for b in *start..(*start + *count).min(total_blocks) {
                if !bitmap_test_bit_be(bitmap, b) {
                    warnings.push(hfs_issue(
                        HfsFsckCode::BadBlockExtentNotInBitmap,
                        format!(
                            "bad block file: block {} is marked free in bitmap (should be allocated)",
                            b
                        ),
                    ));
                    break; // one warning per extent range is enough
                }
            }
        }
    }

    // Verify free block count
    let mut actual_free = 0u32;
    for block in 0..total_blocks {
        if !bitmap_test_bit_be(bitmap, block) {
            actual_free += 1;
        }
    }
    if actual_free != mdb.free_blocks as u32 {
        errors.push(hfs_issue(
            HfsFsckCode::FreeBlockCountMismatch,
            format!(
                "MDB free_blocks = {} but bitmap has {} free blocks",
                mdb.free_blocks, actual_free
            ),
        ));
    }
}

fn mark_extents(
    extents: &[HfsExtDescriptor; 3],
    total_blocks: u32,
    computed_bitmap: &mut [u8],
    errors: &mut Vec<FsckIssue>,
    label: &str,
) {
    for ext in extents {
        if ext.block_count == 0 {
            continue;
        }
        let start = ext.start_block as u32;
        let count = ext.block_count as u32;
        if start + count > total_blocks {
            errors.push(hfs_issue(
                HfsFsckCode::ExtentOutOfRange,
                format!(
                    "{}: extent [{}, +{}) exceeds total_blocks {}",
                    label, start, count, total_blocks
                ),
            ));
            continue;
        }
        for b in start..start + count {
            super::hfs_common::bitmap_set_bit_be(computed_bitmap, b);
        }
    }
}

// ---------------------------------------------------------------------------
// Extents Overflow B-tree Structure
// ---------------------------------------------------------------------------

/// Compare two extents overflow B-tree keys.
/// Key format: key_len(1) + fork_type(1) + file_id(4) + start_block(2)
/// Ordering: file_id first, then fork_type, then start_block.

#[cfg(test)]
mod tests {
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

        let mut write_pos = n1 + 14;
        let mut num_recs = 0u16;

        // Record 0: Dir thread for root dir — key: parent_id=2, name=""
        let r0_start = write_pos;
        data[write_pos] = 6; // key_len = 6 (1 reserved + 4 parentID + 1 nameLen)
                             // reserved byte at +1 already 0
        BigEndian::write_u32(&mut data[write_pos + 2..write_pos + 6], 2); // key parent_id = CNID 2
        data[write_pos + 6] = 0; // name_len = 0
                                 // Even-align record data
        let rec_off = write_pos + 8; // 1+6 = 7 → pad to 8
        data[rec_off] = CATALOG_DIR_THREAD as u8; // type = 3
                                                  // reserved 8 bytes (already 0)
        BigEndian::write_u32(&mut data[rec_off + 10..rec_off + 14], 1); // parent_id = 1
        data[rec_off + 14] = 4; // name_len = 4
        data[rec_off + 15..rec_off + 19].copy_from_slice(b"Test");
        write_pos = rec_off + 46; // dir thread = 46 bytes
        num_recs += 1;

        // Record 1: Root dir — key: parent_id=1, name="" (but actually the root dir
        // record uses parent_id=1 and typically the volume name... for simplicity use
        // a zero-length name which sorts before "File")
        // Actually we need keys in order. Thread key (2,"") should come AFTER dir key
        // (1,"name"). Let me reorder: put root dir first.
        // Rewrite: clear and redo in proper key order.
        // Key order: (1,"Test") < (2,"") < (2,"File") < (file_cnid,"")
        // So: rec0=root dir, rec1=root dir thread, rec2=file, rec3=file thread

        // Start over at offset 14
        write_pos = n1 + 14;
        num_recs = 0;

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
            write_pos = rec_off + 46;
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
        if key_total_0 % 2 != 0 {
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
        if key_total_1 % 2 != 0 {
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
