//! HFS filesystem checker (fsck) — read-only integrity verification.
//!
//! Validates the HFS volume in four phases:
//! 1. MDB sanity checks
//! 2. B-tree structure verification
//! 3. Catalog consistency (threads ↔ records, counts)
//! 4. Extent/allocation bitmap cross-check (including extents overflow)

use std::collections::{HashMap, HashSet};

use byteorder::{BigEndian, ByteOrder};

use super::hfs::{
    mac_roman_to_utf8, HfsExtDescriptor, HfsMasterDirectoryBlock, CATALOG_DIR, CATALOG_FILE,
};
use super::hfs_common::{
    bitmap_set_bit_be, bitmap_test_bit_be, btree_insert_record, btree_node_bitmap_range,
    btree_record_range, normalize_catalog_index_key, BTreeHeader, BTREE_HEADER_NODE,
    BTREE_INDEX_NODE, BTREE_LEAF_NODE,
};

const CATALOG_DIR_THREAD: i8 = 3;
const CATALOG_FILE_THREAD: i8 = 4;

/// Maximum number of bitmap mismatch issues to report before capping.
const MAX_BITMAP_MISMATCHES: usize = 20;

use super::fsck::{FsckIssue, FsckResult, FsckStats, OrphanedEntry, RepairReport};

// ---------------------------------------------------------------------------
// HFS-specific issue codes and repair classification
// ---------------------------------------------------------------------------

/// HFS-specific issue codes used internally for repair classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HfsFsckCode {
    // MDB issues
    BadSignature,
    BadBlockSize,
    NextCatalogIdTooLow,
    // B-tree structure
    HeaderNodeBadKind,
    RootNodeBadKind,
    OffsetTableNotMonotonic,
    OffsetTableOutOfBounds,
    LeafChainBroken,
    LeafBacklinkMismatch,
    LeafRecordCountMismatch,
    IndexSiblingLinkBroken,
    InvalidRecordLength,
    // Catalog consistency
    MissingFileThread,
    MissingDirThread,
    MissingFileRecord,
    MissingDirRecord,
    ThreadParentNameMismatch,
    FileCountMismatch,
    FolderCountMismatch,
    OrphanedThread,
    MissingParent,
    ValenceMismatch,
    // Extent/allocation
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
            | HfsFsckCode::OffsetTableNotMonotonic
            | HfsFsckCode::OffsetTableOutOfBounds
            | HfsFsckCode::MissingFileThread
            | HfsFsckCode::MissingParent
    )
}

/// Build a shared `FsckIssue` from an HFS-specific code and message.
fn hfs_issue(code: HfsFsckCode, message: impl Into<String>) -> FsckIssue {
    FsckIssue {
        code: format!("{:?}", code),
        message: message.into(),
        repairable: is_repairable(code),
    }
}

/// Run a full integrity check on a classic HFS volume.
///
/// `mdb` — parsed Master Directory Block.
/// `catalog_data` — full catalog B-tree file content.
/// `bitmap` — volume allocation bitmap bytes.
/// `extents_data` — optional extents overflow B-tree file content (for files with >3 extents).
pub(crate) fn check_hfs_integrity(
    mdb: &HfsMasterDirectoryBlock,
    catalog_data: &[u8],
    bitmap: &[u8],
    extents_data: Option<&[u8]>,
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

    // Phase 2: B-tree structure
    let header = BTreeHeader::read(catalog_data);
    let node_size = header.node_size as usize;
    if node_size == 0 || catalog_data.len() < node_size {
        errors.push(hfs_issue(
            HfsFsckCode::HeaderNodeBadKind,
            "catalog B-tree node size is zero or catalog data too small",
        ));
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
    extents_data: Option<&[u8]>,
) -> RepairReport {
    // Count unrepairable issues by running a quick check first
    let check_result = check_hfs_integrity(mdb, catalog_data, bitmap, extents_data);
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

    // Need a valid node_size to proceed
    let header = BTreeHeader::read(catalog_data);
    let node_size = header.node_size as usize;
    if node_size == 0 || catalog_data.len() < node_size {
        report
            .fixes_failed
            .push("Cannot repair: B-tree node size is zero or catalog data too small".into());
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

    // --- Phase 4: Extent fixes ---
    repair_extents(mdb, catalog_data, node_size, extents_data, &mut report);

    // --- Phase 5: MDB counts + bitmap rebuild ---
    repair_mdb_counts_and_bitmap(mdb, catalog_data, bitmap, extents_data, &mut report);

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

    // Rebuild leaf chain: union fLink-reachable + bitmap-allocated leaf nodes,
    // sort by first key, relink, and fix node bitmap.
    rebuild_leaf_chain(catalog_data, node_size, report);
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
    let (bmp_off, bmp_size) = btree_node_bitmap_range(catalog_data, node_size);
    let mut bitmap_nodes: HashSet<u32> = HashSet::new();
    for node_idx in 1..max_nodes as u32 {
        if !bitmap_test_bit_be(&catalog_data[bmp_off..bmp_off + bmp_size], node_idx) {
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
    let (bmp_off, bmp_size) = btree_node_bitmap_range(catalog_data, node_size);
    let bmp_max_bit = (bmp_size as u32) * 8;
    for &(node_idx, _) in &leaf_nodes {
        if node_idx < bmp_max_bit
            && !bitmap_test_bit_be(&catalog_data[bmp_off..bmp_off + bmp_size], node_idx)
        {
            bitmap_set_bit_be(&mut catalog_data[bmp_off..bmp_off + bmp_size], node_idx);
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
fn rebuild_index_nodes(catalog_data: &mut [u8], node_size: usize, report: &mut RepairReport) {
    use super::hfs_common::{btree_alloc_node, btree_free_node, init_node};

    let header = BTreeHeader::read(catalog_data);
    let (bmp_off, bmp_size) = btree_node_bitmap_range(catalog_data, node_size);
    let max_nodes = catalog_data.len() / node_size;

    // Free all existing index nodes
    let mut freed = 0u32;
    let bmp_max_bit = (bmp_size as u32) * 8;
    for node_idx in 1..(max_nodes as u32).min(bmp_max_bit) {
        if !bitmap_test_bit_be(&catalog_data[bmp_off..bmp_off + bmp_size], node_idx) {
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
            } => {
                dir_threads.insert(*key_cnid, (*parent_id, name.clone()));
            }
            CatalogEntry::FileThread {
                key_cnid,
                parent_id,
                name,
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
    if mdb.folder_count != folder_count {
        let old = mdb.folder_count;
        mdb.folder_count = folder_count;
        report.fixes_applied.push(format!(
            "Fixed MDB folder_count: {} -> {}",
            old, folder_count
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
// Phase 1: MDB Sanity
// ---------------------------------------------------------------------------

fn check_mdb(mdb: &HfsMasterDirectoryBlock, errors: &mut Vec<FsckIssue>) {
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
}

// ---------------------------------------------------------------------------
// Phase 2: B-tree Structure
// ---------------------------------------------------------------------------

/// Maximum records per node (based on minimum record size of ~10 bytes in 512-byte node).
const HFS_MAX_NRECS: usize = 35;

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
    let (bmp_off, bmp_size) = btree_node_bitmap_range(catalog_data, node_size);
    let max_nodes = catalog_data.len() / node_size;
    for node_idx in 0..max_nodes as u32 {
        if !bitmap_test_bit_be(&catalog_data[bmp_off..bmp_off + bmp_size], node_idx) {
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
        let min_data = match rec_type {
            1 => 70,       // CATALOG_DIR
            2 => 102,      // CATALOG_FILE
            3 | 4 => 15,   // dir/file thread: type(1)+rsv(1)+rsv(8)+parentID(4)+nameLen(1)
            _ => continue, // unknown type, skip
        };
        if data_len < min_data {
            errors.push(hfs_issue(
                HfsFsckCode::InvalidRecordLength,
                format!(
                    "node {} record {}: type {} data length {} < minimum {}",
                    node_idx, i, rec_type, data_len, min_data
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
fn extract_child_pointers(node: &[u8], node_size: usize) -> Vec<u32> {
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
        rsrc_extents: [HfsExtDescriptor; 3],
        rsrc_size: u32,
    },
    DirThread {
        key_cnid: u32,
        parent_id: u32,
        name: Vec<u8>,
    },
    FileThread {
        key_cnid: u32,
        parent_id: u32,
        name: Vec<u8>,
    },
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
                *directories_checked += 1;
            }
            CatalogEntry::File { key, file_id, .. } => {
                file_records.insert(*file_id, (key.parent_id, key.name.clone()));
                *children_count.entry(key.parent_id).or_insert(0) += 1;
                if *file_id > max_cnid {
                    max_cnid = *file_id;
                }
                *files_checked += 1;
            }
            CatalogEntry::DirThread {
                key_cnid,
                parent_id,
                name,
            } => {
                dir_threads.insert(*key_cnid, (*parent_id, name.clone()));
            }
            CatalogEntry::FileThread {
                key_cnid,
                parent_id,
                name,
            } => {
                file_threads.insert(*key_cnid, (*parent_id, name.clone()));
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

    // Verify file_count and folder_count match MDB
    // HFS folder_count includes the root directory (CNID 2)
    let actual_file_count = *files_checked;
    let actual_folder_count = *directories_checked;
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
                "MDB folder_count = {} but catalog has {} directory records",
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
            let file_id = BigEndian::read_u32(&rec[20..24]);
            let data_size = BigEndian::read_u32(&rec[26..30]);
            let mut data_extents = [HfsExtDescriptor {
                start_block: 0,
                block_count: 0,
            }; 3];
            for j in 0..3 {
                data_extents[j] = HfsExtDescriptor::parse(&rec[74 + j * 4..78 + j * 4]);
            }
            let rsrc_size = BigEndian::read_u32(&rec[36..40]);
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
                rsrc_extents,
                rsrc_size,
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

    // Compare computed bitmap vs actual volume bitmap
    let mut mismatch_count = 0;
    for block in 0..total_blocks {
        let computed = bitmap_test_bit_be(&computed_bitmap, block);
        let actual = bitmap_test_bit_be(bitmap, block);
        if computed != actual {
            mismatch_count += 1;
            if mismatch_count <= MAX_BITMAP_MISMATCHES {
                let direction = if computed {
                    "allocated in catalog but free in bitmap"
                } else {
                    "free in catalog but allocated in bitmap"
                };
                warnings.push(hfs_issue(
                    HfsFsckCode::BitmapMismatch,
                    format!("block {}: {}", block, direction),
                ));
            }
        }
    }
    if mismatch_count > MAX_BITMAP_MISMATCHES {
        warnings.push(hfs_issue(
            HfsFsckCode::BitmapMismatch,
            format!(
                "... and {} more bitmap mismatches (total {})",
                mismatch_count - MAX_BITMAP_MISMATCHES,
                mismatch_count
            ),
        ));
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
            errors.iter().map(|e| &e.message).collect::<Vec<_>>()
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
            folder_count: 1,
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
}
