//! HFS filesystem checker (fsck) — read-only integrity verification.
//!
//! Validates the HFS volume in four phases:
//! 1. MDB sanity checks
//! 2. B-tree structure verification
//! 3. Catalog consistency (threads ↔ records, counts)
//! 4. Extent/allocation bitmap cross-check (including extents overflow)

use std::collections::{HashMap, HashSet};

use byteorder::{BigEndian, ByteOrder};

use super::hfs::{HfsExtDescriptor, HfsMasterDirectoryBlock, CATALOG_DIR, CATALOG_FILE};
use super::hfs_common::{
    bitmap_set_bit_be, bitmap_test_bit_be, btree_node_bitmap_range, btree_record_range,
    BTreeHeader, BTREE_HEADER_NODE, BTREE_INDEX_NODE, BTREE_LEAF_NODE,
};

const CATALOG_DIR_THREAD: i8 = 3;
const CATALOG_FILE_THREAD: i8 = 4;

/// Maximum number of bitmap mismatch issues to report before capping.
const MAX_BITMAP_MISMATCHES: usize = 20;

/// Result of a filesystem check.
pub struct FsckResult {
    pub errors: Vec<FsckIssue>,
    pub warnings: Vec<FsckIssue>,
    pub stats: FsckStats,
    /// True when at least one error has a repair action that isn't `NotRepairable`.
    pub repairable: bool,
}

/// Report from a repair operation.
pub struct RepairReport {
    pub fixes_applied: Vec<String>,
    pub fixes_failed: Vec<String>,
}

/// Classification of what repair action an error maps to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairAction {
    FixMdbSignature,
    FixMdbCounts,
    FixNodeKind,
    RebuildLeafChain,
    FixLeafLink,
    FixBtreeHeader,
    FixThreadRecord,
    InsertDirThread,
    RemoveOrphanedThread,
    FixValence,
    RebuildBitmap,
    TruncateExtent,
    ResolveOverlap,
    NotRepairable,
}

impl FsckResult {
    /// Returns true if no errors were found (warnings are tolerated).
    pub fn is_clean(&self) -> bool {
        self.errors.is_empty()
    }
}

/// A single issue found during the check.
pub struct FsckIssue {
    pub code: FsckCode,
    pub message: String,
    /// B-tree node index where the issue was found, if applicable.
    pub node: Option<u32>,
}

/// Aggregate statistics from the check.
pub struct FsckStats {
    pub files_checked: u32,
    pub directories_checked: u32,
    pub leaf_nodes_visited: u32,
    pub orphaned_threads: u32,
}

/// Classification of check issues.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsckCode {
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
    // Catalog consistency
    MissingFileThread,
    MissingDirThread,
    MissingFileRecord,
    MissingDirRecord,
    ThreadParentNameMismatch,
    FileCountMismatch,
    FolderCountMismatch,
    OrphanedRecord,
    ValenceMismatch,
    // Extent/allocation
    ExtentOutOfRange,
    OverlappingExtents,
    BitmapMismatch,
    FreeBlockCountMismatch,
}

impl std::fmt::Display for FsckCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Map each FsckCode to its corresponding repair action.
pub fn classify_repair(code: &FsckCode) -> RepairAction {
    match code {
        FsckCode::BadSignature => RepairAction::FixMdbSignature,
        FsckCode::BadBlockSize => RepairAction::NotRepairable,
        FsckCode::NextCatalogIdTooLow => RepairAction::FixMdbCounts,
        FsckCode::HeaderNodeBadKind => RepairAction::FixNodeKind,
        FsckCode::RootNodeBadKind => RepairAction::FixNodeKind,
        FsckCode::OffsetTableNotMonotonic => RepairAction::NotRepairable,
        FsckCode::OffsetTableOutOfBounds => RepairAction::NotRepairable,
        FsckCode::LeafChainBroken => RepairAction::RebuildLeafChain,
        FsckCode::LeafBacklinkMismatch => RepairAction::FixLeafLink,
        FsckCode::LeafRecordCountMismatch => RepairAction::FixBtreeHeader,
        FsckCode::MissingFileThread => RepairAction::NotRepairable, // optional in classic HFS
        FsckCode::MissingDirThread => RepairAction::InsertDirThread,
        FsckCode::MissingFileRecord => RepairAction::RemoveOrphanedThread,
        FsckCode::MissingDirRecord => RepairAction::RemoveOrphanedThread,
        FsckCode::ThreadParentNameMismatch => RepairAction::FixThreadRecord,
        FsckCode::FileCountMismatch => RepairAction::FixMdbCounts,
        FsckCode::FolderCountMismatch => RepairAction::FixMdbCounts,
        FsckCode::OrphanedRecord => RepairAction::RemoveOrphanedThread,
        FsckCode::ValenceMismatch => RepairAction::FixValence,
        FsckCode::ExtentOutOfRange => RepairAction::TruncateExtent,
        FsckCode::OverlappingExtents => RepairAction::ResolveOverlap,
        FsckCode::BitmapMismatch => RepairAction::RebuildBitmap,
        FsckCode::FreeBlockCountMismatch => RepairAction::FixMdbCounts,
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
    let mut stats = FsckStats {
        files_checked: 0,
        directories_checked: 0,
        leaf_nodes_visited: 0,
        orphaned_threads: 0,
    };

    // Phase 1: MDB sanity
    check_mdb(mdb, &mut errors);

    // If signature is bad, the rest of the check is meaningless
    if errors.iter().any(|e| e.code == FsckCode::BadSignature) {
        let repairable = errors
            .iter()
            .any(|e| classify_repair(&e.code) != RepairAction::NotRepairable);
        return FsckResult {
            errors,
            warnings,
            stats,
            repairable,
        };
    }

    // Phase 2: B-tree structure
    let header = BTreeHeader::read(catalog_data);
    let node_size = header.node_size as usize;
    if node_size == 0 || catalog_data.len() < node_size {
        errors.push(FsckIssue {
            code: FsckCode::HeaderNodeBadKind,
            message: "catalog B-tree node size is zero or catalog data too small".into(),
            node: None,
        });
        let repairable = errors
            .iter()
            .any(|e| classify_repair(&e.code) != RepairAction::NotRepairable);
        return FsckResult {
            errors,
            warnings,
            stats,
            repairable,
        };
    }
    check_btree_structure(catalog_data, &header, &mut errors, &mut stats);

    // Phase 3: Catalog consistency
    check_catalog_consistency(
        mdb,
        catalog_data,
        &header,
        &mut errors,
        &mut warnings,
        &mut stats,
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

    let repairable = errors
        .iter()
        .any(|e| classify_repair(&e.code) != RepairAction::NotRepairable);
    FsckResult {
        errors,
        warnings,
        stats,
        repairable,
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
    let mut report = RepairReport {
        fixes_applied: Vec::new(),
        fixes_failed: Vec::new(),
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

    // --- Phase 1: B-tree structure fixes ---
    repair_btree_structure(catalog_data, node_size, &mut report);

    // --- Phase 2: Catalog consistency fixes ---
    repair_catalog_consistency(mdb, catalog_data, node_size, &mut report);

    // --- Phase 3: Extent fixes ---
    repair_extents(mdb, catalog_data, node_size, extents_data, &mut report);

    // --- Phase 4: MDB counts + bitmap rebuild ---
    repair_mdb_counts_and_bitmap(mdb, catalog_data, bitmap, extents_data, &mut report);

    report
}

/// Phase 1: Fix B-tree structural issues.
fn repair_btree_structure(catalog_data: &mut [u8], node_size: usize, report: &mut RepairReport) {
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

    // Rebuild leaf chain: scan all allocated nodes, collect leaf nodes, sort by first key, relink
    rebuild_leaf_chain(catalog_data, node_size, report);

    // Fix leaf record count
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

/// Scan all allocated leaf nodes via B-tree node bitmap, sort by first key, relink chain.
fn rebuild_leaf_chain(catalog_data: &mut [u8], node_size: usize, report: &mut RepairReport) {
    let (bmp_off, bmp_size) = btree_node_bitmap_range(catalog_data, node_size);
    let max_nodes = catalog_data.len() / node_size;

    // Collect all leaf nodes with their first key for sorting
    let mut leaf_nodes: Vec<(u32, Vec<u8>)> = Vec::new(); // (node_idx, first_key)

    for node_idx in 1..max_nodes as u32 {
        if !bitmap_test_bit_be(&catalog_data[bmp_off..bmp_off + bmp_size], node_idx) {
            continue;
        }
        let off = node_idx as usize * node_size;
        if off + node_size > catalog_data.len() {
            break;
        }
        let kind = catalog_data[off + 8] as i8;
        if kind != BTREE_LEAF_NODE {
            continue;
        }
        let num_records = BigEndian::read_u16(&catalog_data[off + 10..off + 12]) as usize;
        if num_records == 0 {
            continue;
        }

        // Extract first key for sorting
        let (rec_start, rec_end) =
            btree_record_range(&catalog_data[off..off + node_size], node_size, 0);
        if rec_start < rec_end && rec_end <= node_size {
            let key_len = catalog_data[off + rec_start] as usize;
            let key_end = rec_start + 1 + key_len;
            if key_end <= rec_end {
                let first_key = catalog_data[off + rec_start..off + key_end].to_vec();
                leaf_nodes.push((node_idx, first_key));
            }
        }
    }

    if leaf_nodes.is_empty() {
        return;
    }

    // Sort by first key using HFS catalog key comparison
    leaf_nodes.sort_by(|a, b| {
        super::hfs::HfsFilesystem::<std::io::Cursor<Vec<u8>>>::catalog_compare(&a.1, &b.1)
    });

    // Check if chain is already correct
    let header = BTreeHeader::read(catalog_data);
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

    // Rewrite fLink/bLink for all leaf nodes
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
    }

    // Update header
    let mut h = BTreeHeader::read(catalog_data);
    h.first_leaf_node = leaf_nodes[0].0;
    h.last_leaf_node = leaf_nodes.last().unwrap().0;
    h.write(catalog_data);

    report.fixes_applied.push(format!(
        "Rebuilt leaf chain: {} leaf nodes relinked",
        leaf_nodes.len()
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
                // Insert missing directory thread
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
                    report.fixes_failed.push(format!(
                        "Failed to insert directory thread for CNID {}",
                        dir_id
                    ));
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
        errors.push(FsckIssue {
            code: FsckCode::BadSignature,
            message: format!(
                "bad MDB signature: 0x{:04X} (expected 0x4244)",
                mdb.signature
            ),
            node: None,
        });
    }

    // HFS block size must be a positive multiple of 512
    #[allow(clippy::manual_is_multiple_of)]
    if mdb.block_size == 0 || mdb.block_size % 512 != 0 {
        errors.push(FsckIssue {
            code: FsckCode::BadBlockSize,
            message: format!(
                "bad allocation block size: {} (must be a positive multiple of 512)",
                mdb.block_size
            ),
            node: None,
        });
    }
}

// ---------------------------------------------------------------------------
// Phase 2: B-tree Structure
// ---------------------------------------------------------------------------

fn check_btree_structure(
    catalog_data: &[u8],
    header: &BTreeHeader,
    errors: &mut Vec<FsckIssue>,
    stats: &mut FsckStats,
) {
    let node_size = header.node_size as usize;

    // Check header node kind
    if catalog_data.len() >= 9 {
        let kind = catalog_data[8] as i8;
        if kind != BTREE_HEADER_NODE {
            errors.push(FsckIssue {
                code: FsckCode::HeaderNodeBadKind,
                message: format!(
                    "node 0 kind = {} (expected {} = header)",
                    kind, BTREE_HEADER_NODE
                ),
                node: Some(0),
            });
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
                errors.push(FsckIssue {
                    code: FsckCode::RootNodeBadKind,
                    message: format!(
                        "root node {} kind = {} (expected {} for depth {})",
                        header.root_node, root_kind, expected_kind, header.depth
                    ),
                    node: Some(header.root_node),
                });
            }
        }
    }

    // Check offset tables on all allocated nodes
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
        if num_records == 0 {
            continue;
        }

        check_offset_table(node, node_size, node_idx, num_records, errors);
    }

    // Walk leaf chain and verify forward/backward links
    let leaf_count = walk_leaf_chain(catalog_data, header, errors, stats);

    // Verify leaf record count
    if leaf_count != header.leaf_records {
        errors.push(FsckIssue {
            code: FsckCode::LeafRecordCountMismatch,
            message: format!(
                "leaf record count: walked {} but header says {}",
                leaf_count, header.leaf_records
            ),
            node: None,
        });
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
            errors.push(FsckIssue {
                code: FsckCode::OffsetTableOutOfBounds,
                message: format!(
                    "node {}: offset[{}] = {} >= node_size {}",
                    node_idx, i, offset, node_size
                ),
                node: Some(node_idx),
            });
            return; // no point checking further
        }

        if i > 0 && offset < prev_offset {
            errors.push(FsckIssue {
                code: FsckCode::OffsetTableNotMonotonic,
                message: format!(
                    "node {}: offset[{}] = {} < offset[{}] = {} (not monotonic)",
                    node_idx,
                    i,
                    offset,
                    i - 1,
                    prev_offset
                ),
                node: Some(node_idx),
            });
            return;
        }
        prev_offset = offset;
    }
}

/// Walk the leaf node chain from first_leaf to last_leaf.
/// Returns the total number of leaf records encountered.
fn walk_leaf_chain(
    catalog_data: &[u8],
    header: &BTreeHeader,
    errors: &mut Vec<FsckIssue>,
    stats: &mut FsckStats,
) -> u32 {
    let node_size = header.node_size as usize;
    let mut total_records = 0u32;
    let mut node_idx = header.first_leaf_node;
    let mut prev_idx: u32 = 0;
    let mut visited = HashSet::new();

    while node_idx != 0 {
        if !visited.insert(node_idx) {
            errors.push(FsckIssue {
                code: FsckCode::LeafChainBroken,
                message: format!("leaf chain loop detected at node {}", node_idx),
                node: Some(node_idx),
            });
            break;
        }

        let off = node_idx as usize * node_size;
        if off + node_size > catalog_data.len() {
            errors.push(FsckIssue {
                code: FsckCode::LeafChainBroken,
                message: format!("leaf node {} offset out of range", node_idx),
                node: Some(node_idx),
            });
            break;
        }

        let node = &catalog_data[off..off + node_size];
        let flink = BigEndian::read_u32(&node[0..4]);
        let blink = BigEndian::read_u32(&node[4..8]);
        let kind = node[8] as i8;
        let num_records = BigEndian::read_u16(&node[10..12]) as u32;

        if kind != BTREE_LEAF_NODE {
            errors.push(FsckIssue {
                code: FsckCode::LeafChainBroken,
                message: format!(
                    "node {} in leaf chain has kind {} (expected leaf = {})",
                    node_idx, kind, BTREE_LEAF_NODE
                ),
                node: Some(node_idx),
            });
            break;
        }

        // Verify backlink
        if stats.leaf_nodes_visited > 0 && blink != prev_idx {
            errors.push(FsckIssue {
                code: FsckCode::LeafBacklinkMismatch,
                message: format!(
                    "node {}: bLink = {} but previous node was {}",
                    node_idx, blink, prev_idx
                ),
                node: Some(node_idx),
            });
        }

        total_records += num_records;
        stats.leaf_nodes_visited += 1;
        prev_idx = node_idx;
        node_idx = flink;
    }

    total_records
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
    stats: &mut FsckStats,
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
                stats.directories_checked += 1;
            }
            CatalogEntry::File { key, file_id, .. } => {
                file_records.insert(*file_id, (key.parent_id, key.name.clone()));
                *children_count.entry(key.parent_id).or_insert(0) += 1;
                if *file_id > max_cnid {
                    max_cnid = *file_id;
                }
                stats.files_checked += 1;
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
        errors.push(FsckIssue {
            code: FsckCode::NextCatalogIdTooLow,
            message: format!(
                "next_catalog_id {} <= max CNID {} found in catalog",
                mdb.next_catalog_id, max_cnid
            ),
            node: None,
        });
    }

    // Verify thread ↔ record consistency for directories
    for (&dir_id, &(parent_id, ref name, _valence)) in &dir_records {
        if dir_id == 2 {
            // Root folder — thread should exist but parent/name rules are special
            if !dir_threads.contains_key(&dir_id) {
                errors.push(FsckIssue {
                    code: FsckCode::MissingDirThread,
                    message: format!("directory CNID {} has no thread record", dir_id),
                    node: None,
                });
            }
            continue;
        }
        match dir_threads.get(&dir_id) {
            None => {
                errors.push(FsckIssue {
                    code: FsckCode::MissingDirThread,
                    message: format!("directory CNID {} has no thread record", dir_id),
                    node: None,
                });
            }
            Some((thr_parent, thr_name)) => {
                if *thr_parent != parent_id || *thr_name != *name {
                    errors.push(FsckIssue {
                        code: FsckCode::ThreadParentNameMismatch,
                        message: format!(
                            "directory CNID {} thread points to parent {} but record key has parent {}",
                            dir_id, thr_parent, parent_id
                        ),
                        node: None,
                    });
                }
            }
        }
        // Verify parent exists
        if parent_id != 1 && !dir_records.contains_key(&parent_id) {
            errors.push(FsckIssue {
                code: FsckCode::OrphanedRecord,
                message: format!(
                    "directory CNID {} references parent {} which doesn't exist",
                    dir_id, parent_id
                ),
                node: None,
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
                errors.push(FsckIssue {
                    code: FsckCode::ThreadParentNameMismatch,
                    message: format!(
                        "file CNID {} thread points to parent {} but record key has parent {}",
                        file_id, thr_parent, parent_id
                    ),
                    node: None,
                });
            }
        }
        // Not having a file thread is normal in classic HFS — no warning needed.

        // Verify parent exists
        if parent_id != 1 && !dir_records.contains_key(&parent_id) {
            errors.push(FsckIssue {
                code: FsckCode::OrphanedRecord,
                message: format!(
                    "file CNID {} references parent {} which doesn't exist",
                    file_id, parent_id
                ),
                node: None,
            });
        }
    }

    // Check for orphaned threads (threads with no matching record)
    for &cnid in dir_threads.keys() {
        if !dir_records.contains_key(&cnid) {
            stats.orphaned_threads += 1;
            warnings.push(FsckIssue {
                code: FsckCode::OrphanedRecord,
                message: format!("orphaned directory thread for CNID {}", cnid),
                node: None,
            });
        }
    }
    for &cnid in file_threads.keys() {
        if !file_records.contains_key(&cnid) {
            stats.orphaned_threads += 1;
            warnings.push(FsckIssue {
                code: FsckCode::OrphanedRecord,
                message: format!("orphaned file thread for CNID {}", cnid),
                node: None,
            });
        }
    }

    // Verify file_count and folder_count match MDB
    // HFS folder_count includes the root directory (CNID 2)
    let actual_file_count = stats.files_checked;
    let actual_folder_count = stats.directories_checked;
    if actual_file_count != mdb.file_count {
        errors.push(FsckIssue {
            code: FsckCode::FileCountMismatch,
            message: format!(
                "MDB file_count = {} but catalog has {} file records",
                mdb.file_count, actual_file_count
            ),
            node: None,
        });
    }
    if actual_folder_count != mdb.folder_count {
        errors.push(FsckIssue {
            code: FsckCode::FolderCountMismatch,
            message: format!(
                "MDB folder_count = {} but catalog has {} directory records",
                mdb.folder_count, actual_folder_count
            ),
            node: None,
        });
    }

    // Verify valence matches actual children count
    for (&dir_id, &(_parent_id, ref _name, valence)) in &dir_records {
        let actual = children_count.get(&dir_id).copied().unwrap_or(0);
        if actual != valence as u32 {
            errors.push(FsckIssue {
                code: FsckCode::ValenceMismatch,
                message: format!(
                    "directory CNID {} valence = {} but has {} children",
                    dir_id, valence, actual
                ),
                node: None,
            });
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
            errors.push(FsckIssue {
                code: FsckCode::ExtentOutOfRange,
                message: format!(
                    "{}: extent [{}, +{}) exceeds total_blocks {}",
                    label, start, count, total_blocks
                ),
                node: None,
            });
            continue;
        }
        for b in *start..*start + *count {
            if bitmap_test_bit_be(&computed_bitmap, b) {
                errors.push(FsckIssue {
                    code: FsckCode::OverlappingExtents,
                    message: format!("{}: block {} already allocated by another extent", label, b),
                    node: None,
                });
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
                warnings.push(FsckIssue {
                    code: FsckCode::BitmapMismatch,
                    message: format!("block {}: {}", block, direction),
                    node: None,
                });
            }
        }
    }
    if mismatch_count > MAX_BITMAP_MISMATCHES {
        warnings.push(FsckIssue {
            code: FsckCode::BitmapMismatch,
            message: format!(
                "... and {} more bitmap mismatches (total {})",
                mismatch_count - MAX_BITMAP_MISMATCHES,
                mismatch_count
            ),
            node: None,
        });
    }

    // Verify free block count
    let mut actual_free = 0u32;
    for block in 0..total_blocks {
        if !bitmap_test_bit_be(bitmap, block) {
            actual_free += 1;
        }
    }
    if actual_free != mdb.free_blocks as u32 {
        errors.push(FsckIssue {
            code: FsckCode::FreeBlockCountMismatch,
            message: format!(
                "MDB free_blocks = {} but bitmap has {} free blocks",
                mdb.free_blocks, actual_free
            ),
            node: None,
        });
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
            errors.push(FsckIssue {
                code: FsckCode::ExtentOutOfRange,
                message: format!(
                    "{}: extent [{}, +{}) exceeds total_blocks {}",
                    label, start, count, total_blocks
                ),
                node: None,
            });
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
    fn test_fsck_code_display() {
        assert_eq!(format!("{}", FsckCode::BadSignature), "BadSignature");
        assert_eq!(format!("{}", FsckCode::LeafChainBroken), "LeafChainBroken");
    }

    #[test]
    fn test_fsck_result_is_clean() {
        let result = FsckResult {
            errors: vec![],
            warnings: vec![FsckIssue {
                code: FsckCode::BitmapMismatch,
                message: "minor".into(),
                node: None,
            }],
            stats: FsckStats {
                files_checked: 0,
                directories_checked: 0,
                leaf_nodes_visited: 0,
                orphaned_threads: 0,
            },
            repairable: false,
        };
        assert!(result.is_clean()); // warnings don't count

        let result_err = FsckResult {
            errors: vec![FsckIssue {
                code: FsckCode::BadSignature,
                message: "bad".into(),
                node: None,
            }],
            warnings: vec![],
            stats: FsckStats {
                files_checked: 0,
                directories_checked: 0,
                leaf_nodes_visited: 0,
                orphaned_threads: 0,
            },
            repairable: true,
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
        assert_eq!(errors[0].code, FsckCode::BadBlockSize);

        // Zero is invalid
        mdb.block_size = 0;
        errors.clear();
        check_mdb(&mdb, &mut errors);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].code, FsckCode::BadBlockSize);
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
