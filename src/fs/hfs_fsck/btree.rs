//! HFS fsck — Phase 2: catalog B-tree structure checks + repair.
//!
//! Validates the on-disk catalog B-tree: header sanity, node bitmap, map-node
//! chain, key ordering, record offsets, leaf-chain integrity, index sibling
//! links. Owns the matching repairs (`repair_btree_structure_leaves_only`,
//! `repair_map_nodes`, `repair_node_bitmap`, `repair_key_ordering`,
//! `fix_leaf_record_count`, `rebuild_leaf_chain`, `rebuild_index_nodes`).
//!
//! Split out of `hfs_fsck/mod.rs` per §8 of `docs/codecleanup.md`.

use std::cmp::Ordering;
use std::collections::HashSet;

use byteorder::{BigEndian, ByteOrder};

use super::super::fsck::{FsckIssue, RepairReport};
use super::super::hfs_common::{
    btree_bitmap_set, btree_bitmap_test, btree_insert_record, btree_node_bitmap_range,
    btree_record_range, normalize_catalog_index_key, BTreeHeader, BTREE_HEADER_NODE,
    BTREE_INDEX_NODE, BTREE_LEAF_NODE, BTREE_MAP_NODE,
};
use super::{hfs_issue, HfsFsckCode};

pub(super) const HFS_MAX_NRECS: usize = 35;

pub(super) fn check_btree_structure(
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
pub(super) fn check_map_nodes(catalog_data: &[u8], node_size: usize, errors: &mut Vec<FsckIssue>) {
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
pub(super) fn check_node_bitmap_consistency(
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
    super::super::hfs::HfsFilesystem::<std::io::Cursor<Vec<u8>>>::catalog_compare(a, b)
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
pub(super) fn check_key_ordering(
    catalog_data: &[u8],
    header: &BTreeHeader,
    errors: &mut Vec<FsckIssue>,
) {
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
    use super::super::hfs_common::HFS_CAT_MAX_KEY_LEN;

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

pub(super) fn repair_btree_structure_leaves_only(
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
pub(super) fn repair_map_nodes(
    catalog_data: &mut [u8],
    node_size: usize,
    report: &mut RepairReport,
) {
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
pub(super) fn repair_node_bitmap(
    catalog_data: &mut [u8],
    node_size: usize,
    report: &mut RepairReport,
) {
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
pub(super) fn repair_key_ordering(
    catalog_data: &mut [u8],
    node_size: usize,
    report: &mut RepairReport,
) {
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
pub(super) fn fix_leaf_record_count(catalog_data: &mut [u8], report: &mut RepairReport) {
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
        super::super::hfs::HfsFilesystem::<std::io::Cursor<Vec<u8>>>::catalog_compare(&a.1, &b.1)
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
    use super::super::hfs_common::{btree_alloc_node, btree_free_node, init_node};

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

    let compare = super::super::hfs::HfsFilesystem::<std::io::Cursor<Vec<u8>>>::catalog_compare;

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
