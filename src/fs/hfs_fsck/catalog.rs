//! HFS fsck — Phase 3: catalog consistency checks + repair.
//!
//! Walks the catalog leaf chain, parses every record into a typed
//! `CatalogEntry`, then validates the relationships between directories,
//! files, and their thread records. Owns the matching repairs
//! (`repair_catalog_consistency` and its thread-record helpers).
//!
//! Split out of `hfs_fsck/mod.rs` per §8 of `docs/codecleanup.md`.

use std::collections::{HashMap, HashSet};

use byteorder::{BigEndian, ByteOrder};

use super::super::fsck::{FsckIssue, OrphanedEntry, RepairReport};
use super::super::hfs::{
    mac_roman_to_utf8, HfsExtDescriptor, HfsMasterDirectoryBlock, CATALOG_DIR, CATALOG_FILE,
};
use super::super::hfs_common::{btree_record_range, BTreeHeader};
use super::mdb::validate_hfs_name;
use super::{hfs_issue, HfsFsckCode, CATALOG_DIR_THREAD, CATALOG_FILE_THREAD};

/// Parsed catalog key for consistency checking.
pub(super) struct CatalogKey {
    parent_id: u32,
    name: Vec<u8>, // raw Mac Roman bytes
}

/// Parsed catalog record for consistency checking.
pub(super) enum CatalogEntry {
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
pub(super) fn check_directory_structure(
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

#[allow(clippy::too_many_arguments)] // catalog walk needs MDB, btree data, extents, bitmap, error sinks
pub(super) fn check_catalog_consistency(
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
pub(super) fn collect_catalog_entries(
    catalog_data: &[u8],
    header: &BTreeHeader,
) -> Vec<CatalogEntry> {
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
/// Phase 2: Fix catalog consistency issues.
pub(super) fn repair_catalog_consistency(
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
    use super::super::hfs::HfsFilesystem;

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
    let (leaf_idx, _) = super::super::hfs_common::btree_find_insert_leaf(
        catalog_data,
        &header,
        &key_record,
        &compare,
    );

    let offset = leaf_idx as usize * node_size;
    if offset + node_size > catalog_data.len() {
        return false;
    }

    let node = &mut catalog_data[offset..offset + node_size];
    if super::super::hfs_common::btree_insert_record(node, node_size, &key_record, &compare).is_ok()
    {
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
            if !data_start.is_multiple_of(2) {
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
    let compare = super::super::hfs::HfsFilesystem::<std::io::Cursor<Vec<u8>>>::catalog_compare;
    let search_key =
        super::super::hfs::HfsFilesystem::<std::io::Cursor<Vec<u8>>>::build_catalog_key(cnid, &[]);

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
                super::super::hfs_common::btree_remove_record(node, node_size, i);
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
    let compare = super::super::hfs::HfsFilesystem::<std::io::Cursor<Vec<u8>>>::catalog_compare;
    let search_key =
        super::super::hfs::HfsFilesystem::<std::io::Cursor<Vec<u8>>>::build_catalog_key(
            parent_id, name,
        );

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
                super::super::hfs_common::btree_remove_record(node, node_size, i);
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
