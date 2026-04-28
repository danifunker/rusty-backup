//! HFS fsck — extents overflow B-tree checks and repairs.
//!
//! This module covers the extents overflow B-tree (used for files with more
//! than 3 extents per fork). The functions are essentially per-tree-shape
//! analogues of the catalog B-tree checks in `mod.rs`, specialized for the
//! extents B-tree's narrower 10-byte key (forkType + fileID + startBlock).
//!
//! Split out of `hfs_fsck.rs` per §8 of `docs/codecleanup.md`.

use std::cmp::Ordering;
use std::collections::HashSet;

use byteorder::{BigEndian, ByteOrder};

use super::{extract_child_pointers, hfs_issue, record_key, HfsFsckCode, HFS_MAX_NRECS};
use crate::fs::fsck::{FsckIssue, RepairReport};
use crate::fs::hfs_common::{
    bitmap_test_bit_be, btree_alloc_node, btree_bitmap_clear, btree_bitmap_set, btree_bitmap_test,
    btree_node_bitmap_range, btree_record_range, init_node, BTreeHeader, BTREE_HEADER_NODE,
    BTREE_INDEX_NODE, BTREE_LEAF_NODE, BTREE_MAP_NODE,
};

fn compare_extents_keys(a: &[u8], b: &[u8]) -> Ordering {
    if a.len() < 8 || b.len() < 8 {
        return a.len().cmp(&b.len());
    }
    let file_a = BigEndian::read_u32(&a[2..6]);
    let file_b = BigEndian::read_u32(&b[2..6]);
    match file_a.cmp(&file_b) {
        Ordering::Equal => {}
        other => return other,
    }
    match a[1].cmp(&b[1]) {
        Ordering::Equal => {}
        other => return other,
    }
    let start_a = BigEndian::read_u16(&a[6..8]);
    let start_b = BigEndian::read_u16(&b[6..8]);
    start_a.cmp(&start_b)
}

/// Check extents overflow B-tree structure (same structural checks as catalog).
pub(super) fn check_extents_btree_structure(extents_data: &[u8], errors: &mut Vec<FsckIssue>) {
    if extents_data.len() < 512 {
        return;
    }

    let header = BTreeHeader::read(extents_data);
    let node_size = header.node_size as usize;

    // Node size validation
    if node_size < 512 || node_size % 512 != 0 || extents_data.len() < node_size {
        errors.push(hfs_issue(
            HfsFsckCode::ExtentsBtreeStructure,
            format!(
                "extents B-tree node size {} invalid or data too small",
                node_size
            ),
        ));
        return;
    }

    // Header node kind
    let kind = extents_data[8] as i8;
    if kind != BTREE_HEADER_NODE {
        errors.push(hfs_issue(
            HfsFsckCode::ExtentsBtreeStructure,
            format!(
                "extents B-tree node 0 kind = {} (expected {} = header)",
                kind, BTREE_HEADER_NODE
            ),
        ));
    }

    // Header node must have 3 records
    let hdr_num_recs = BigEndian::read_u16(&extents_data[10..12]) as usize;
    if hdr_num_recs != 3 {
        errors.push(hfs_issue(
            HfsFsckCode::ExtentsBtreeStructure,
            format!(
                "extents B-tree header node has {} records (expected 3)",
                hdr_num_recs
            ),
        ));
    } else {
        let rec0_off = BigEndian::read_u16(&extents_data[node_size - 2..node_size]);
        if rec0_off != 0x00e {
            errors.push(hfs_issue(
                HfsFsckCode::ExtentsBtreeStructure,
                format!(
                    "extents B-tree header offset[0] = 0x{:03x} (expected 0x00e)",
                    rec0_off
                ),
            ));
        }
    }

    // Root node kind
    if header.root_node != 0 && header.depth > 0 {
        let root_off = header.root_node as usize * node_size;
        if root_off + node_size <= extents_data.len() {
            let root_kind = extents_data[root_off + 8] as i8;
            let expected = if header.depth == 1 {
                BTREE_LEAF_NODE
            } else {
                BTREE_INDEX_NODE
            };
            if root_kind != expected {
                errors.push(hfs_issue(
                    HfsFsckCode::ExtentsBtreeStructure,
                    format!(
                        "extents B-tree root node {} kind = {} (expected {})",
                        header.root_node, root_kind, expected
                    ),
                ));
            }
        }
    }

    // Map nodes
    check_extents_map_nodes(extents_data, node_size, errors);

    // If tree is empty, nothing more to check
    if header.root_node == 0 || header.depth == 0 {
        return;
    }

    // Check allocated nodes: offset tables, record counts, record lengths
    let max_nodes = extents_data.len() / node_size;
    let max_nrecs = HFS_MAX_NRECS * (node_size / 512);

    for node_idx in 1..max_nodes as u32 {
        if !btree_bitmap_test(extents_data, node_size, node_idx) {
            continue;
        }
        let off = node_idx as usize * node_size;
        if off + node_size > extents_data.len() {
            break;
        }
        let node = &extents_data[off..off + node_size];
        let num_records = BigEndian::read_u16(&node[10..12]) as usize;
        let nk = node[8] as i8;

        if num_records > max_nrecs {
            errors.push(hfs_issue(
                HfsFsckCode::ExtentsBtreeStructure,
                format!(
                    "extents node {}: {} records exceeds maximum {}",
                    node_idx, num_records, max_nrecs
                ),
            ));
            continue;
        }

        if num_records == 0 {
            continue;
        }

        // Offset table monotonicity
        check_extents_offset_table(node, node_size, node_idx, num_records, errors);

        // Record length checks
        if nk == BTREE_INDEX_NODE {
            check_extents_index_record_lengths(node, node_size, node_idx, num_records, errors);
        } else if nk == BTREE_LEAF_NODE {
            check_extents_leaf_record_lengths(node, node_size, node_idx, num_records, errors);
        }
    }

    // Leaf chain verification
    check_extents_leaf_chain(extents_data, &header, errors);

    // Index sibling links
    if header.depth > 1 {
        check_extents_index_sibling_links(extents_data, &header, errors);
    }

    // Leaf record count
    let leaf_count = count_extents_leaf_records(extents_data, &header);
    if leaf_count != header.leaf_records {
        errors.push(hfs_issue(
            HfsFsckCode::ExtentsBtreeStructure,
            format!(
                "extents B-tree leaf record count: walked {} but header says {}",
                leaf_count, header.leaf_records
            ),
        ));
    }

    // Node bitmap consistency
    check_extents_node_bitmap(extents_data, &header, errors);

    // Key ordering
    check_extents_key_ordering(extents_data, &header, errors);
}

fn check_extents_map_nodes(extents_data: &[u8], node_size: usize, errors: &mut Vec<FsckIssue>) {
    let mut node_idx = BigEndian::read_u32(&extents_data[0..4]);
    let max_nodes = extents_data.len() / node_size;
    let mut visited = HashSet::new();

    while node_idx != 0 {
        if !visited.insert(node_idx) || node_idx as usize >= max_nodes {
            break;
        }
        let off = node_idx as usize * node_size;
        let node = &extents_data[off..off + node_size];
        if node[8] as i8 != BTREE_MAP_NODE {
            errors.push(hfs_issue(
                HfsFsckCode::ExtentsBtreeStructure,
                format!(
                    "extents map node {} has wrong kind {}",
                    node_idx, node[8] as i8
                ),
            ));
        }
        if BigEndian::read_u16(&node[10..12]) != 1 {
            errors.push(hfs_issue(
                HfsFsckCode::ExtentsBtreeStructure,
                format!(
                    "extents map node {} has {} records (expected 1)",
                    node_idx,
                    BigEndian::read_u16(&node[10..12])
                ),
            ));
        }
        node_idx = BigEndian::read_u32(&node[0..4]);
    }
}

fn check_extents_offset_table(
    node: &[u8],
    node_size: usize,
    node_idx: u32,
    num_records: usize,
    errors: &mut Vec<FsckIssue>,
) {
    let mut prev = 0u16;
    for i in 0..=num_records {
        let pos = node_size - 2 * (i + 1);
        let off = BigEndian::read_u16(&node[pos..pos + 2]);
        if off as usize >= node_size {
            errors.push(hfs_issue(
                HfsFsckCode::ExtentsBtreeStructure,
                format!(
                    "extents node {} offset[{}] = {} out of bounds",
                    node_idx, i, off
                ),
            ));
            return;
        }
        if i > 0 && off <= prev {
            errors.push(hfs_issue(
                HfsFsckCode::ExtentsBtreeStructure,
                format!(
                    "extents node {} offset table not monotonic at index {}",
                    node_idx, i
                ),
            ));
            return;
        }
        prev = off;
    }
}

fn check_extents_index_record_lengths(
    node: &[u8],
    node_size: usize,
    node_idx: u32,
    num_records: usize,
    errors: &mut Vec<FsckIssue>,
) {
    // Extents index records: key_len=7, key(8 bytes even-aligned) + child_ptr(4) = 12 bytes
    for i in 0..num_records {
        let (rec_start, rec_end) = btree_record_range(node, node_size, i);
        if rec_start >= rec_end {
            continue;
        }
        let rec_len = rec_end - rec_start;
        if rec_len < 5 {
            continue;
        }
        let key_len = node[rec_start] as usize;
        if key_len != 7 {
            errors.push(hfs_issue(
                HfsFsckCode::ExtentsBtreeStructure,
                format!(
                    "extents index node {} record {}: key_len {} (expected 7)",
                    node_idx, i, key_len
                ),
            ));
        }
        // Total: 1 (key_len) + 7 (key) + 4 (child ptr) = 12
        if rec_len != 12 {
            errors.push(hfs_issue(
                HfsFsckCode::ExtentsBtreeStructure,
                format!(
                    "extents index node {} record {}: length {} (expected 12)",
                    node_idx, i, rec_len
                ),
            ));
        }
    }
}

fn check_extents_leaf_record_lengths(
    node: &[u8],
    node_size: usize,
    node_idx: u32,
    num_records: usize,
    errors: &mut Vec<FsckIssue>,
) {
    // Extents leaf records: key(8 bytes) + 3 extent descriptors(12 bytes) = 20 bytes
    for i in 0..num_records {
        let (rec_start, rec_end) = btree_record_range(node, node_size, i);
        if rec_start >= rec_end {
            continue;
        }
        let rec_len = rec_end - rec_start;
        let key_len = node[rec_start] as usize;
        if key_len != 7 {
            errors.push(hfs_issue(
                HfsFsckCode::ExtentsBtreeStructure,
                format!(
                    "extents leaf node {} record {}: key_len {} (expected 7)",
                    node_idx, i, key_len
                ),
            ));
            continue;
        }
        // Total: 1 (key_len) + 7 (key data) + 12 (3 × 4-byte extent descriptors) = 20
        if rec_len != 20 {
            errors.push(hfs_issue(
                HfsFsckCode::ExtentsBtreeStructure,
                format!(
                    "extents leaf node {} record {}: length {} (expected 20)",
                    node_idx, i, rec_len
                ),
            ));
        }
    }
}

fn check_extents_leaf_chain(
    extents_data: &[u8],
    header: &BTreeHeader,
    errors: &mut Vec<FsckIssue>,
) {
    let node_size = header.node_size as usize;
    let mut node_idx = header.first_leaf_node;
    let mut prev_idx: u32 = 0;
    let mut visited = HashSet::new();
    let mut count = 0u32;

    while node_idx != 0 {
        if !visited.insert(node_idx) {
            errors.push(hfs_issue(
                HfsFsckCode::ExtentsBtreeStructure,
                format!("extents leaf chain loop at node {}", node_idx),
            ));
            break;
        }
        let off = node_idx as usize * node_size;
        if off + node_size > extents_data.len() {
            errors.push(hfs_issue(
                HfsFsckCode::ExtentsBtreeStructure,
                format!("extents leaf node {} out of range", node_idx),
            ));
            break;
        }
        let node = &extents_data[off..off + node_size];
        let kind = node[8] as i8;
        if kind != BTREE_LEAF_NODE {
            errors.push(hfs_issue(
                HfsFsckCode::ExtentsBtreeStructure,
                format!(
                    "extents node {} in leaf chain has kind {} (expected leaf)",
                    node_idx, kind
                ),
            ));
            break;
        }
        let blink = BigEndian::read_u32(&node[4..8]);
        if count > 0 && blink != prev_idx {
            errors.push(hfs_issue(
                HfsFsckCode::ExtentsBtreeStructure,
                format!(
                    "extents node {}: bLink {} != previous {}",
                    node_idx, blink, prev_idx
                ),
            ));
        }
        prev_idx = node_idx;
        count += 1;
        node_idx = BigEndian::read_u32(&node[0..4]);
    }
}

fn check_extents_index_sibling_links(
    extents_data: &[u8],
    header: &BTreeHeader,
    errors: &mut Vec<FsckIssue>,
) {
    let node_size = header.node_size as usize;
    let mut level_nodes: Vec<u32> = vec![header.root_node];

    for _level in (2..=header.depth).rev() {
        if level_nodes.is_empty() {
            break;
        }
        for (i, &nidx) in level_nodes.iter().enumerate() {
            let off = nidx as usize * node_size;
            if off + node_size > extents_data.len() {
                continue;
            }
            let node = &extents_data[off..off + node_size];
            let flink = BigEndian::read_u32(&node[0..4]);
            let blink = BigEndian::read_u32(&node[4..8]);
            let exp_f = if i + 1 < level_nodes.len() {
                level_nodes[i + 1]
            } else {
                0
            };
            let exp_b = if i > 0 { level_nodes[i - 1] } else { 0 };
            if flink != exp_f {
                errors.push(hfs_issue(
                    HfsFsckCode::ExtentsBtreeStructure,
                    format!(
                        "extents index node {}: fLink {} expected {}",
                        nidx, flink, exp_f
                    ),
                ));
            }
            if blink != exp_b {
                errors.push(hfs_issue(
                    HfsFsckCode::ExtentsBtreeStructure,
                    format!(
                        "extents index node {}: bLink {} expected {}",
                        nidx, blink, exp_b
                    ),
                ));
            }
        }
        let mut next = Vec::new();
        for &nidx in &level_nodes {
            let off = nidx as usize * node_size;
            if off + node_size > extents_data.len() {
                continue;
            }
            let node = &extents_data[off..off + node_size];
            if node[8] as i8 != BTREE_INDEX_NODE {
                continue;
            }
            next.extend(extract_child_pointers(node, node_size));
        }
        level_nodes = next;
    }
}

fn count_extents_leaf_records(extents_data: &[u8], header: &BTreeHeader) -> u32 {
    let node_size = header.node_size as usize;
    let mut node_idx = header.first_leaf_node;
    let mut visited = HashSet::new();
    let mut total = 0u32;
    while node_idx != 0 {
        if !visited.insert(node_idx) {
            break;
        }
        let off = node_idx as usize * node_size;
        if off + node_size > extents_data.len() {
            break;
        }
        let node = &extents_data[off..off + node_size];
        if node[8] as i8 != BTREE_LEAF_NODE {
            break;
        }
        total += BigEndian::read_u16(&node[10..12]) as u32;
        node_idx = BigEndian::read_u32(&node[0..4]);
    }
    total
}

fn check_extents_node_bitmap(
    extents_data: &[u8],
    header: &BTreeHeader,
    errors: &mut Vec<FsckIssue>,
) {
    let node_size = header.node_size as usize;
    let (bmp_off, bmp_size) = btree_node_bitmap_range(extents_data, node_size);
    if bmp_size == 0 {
        return;
    }
    let bitmap = &extents_data[bmp_off..bmp_off + bmp_size];

    // Collect referenced nodes (same pattern as catalog)
    let max_nodes = extents_data.len() / node_size;
    let mut referenced = HashSet::new();
    referenced.insert(0u32);

    // Map nodes
    let mut map_idx = BigEndian::read_u32(&extents_data[0..4]);
    let mut visited = HashSet::new();
    while map_idx != 0 && visited.insert(map_idx) && (map_idx as usize) < max_nodes {
        referenced.insert(map_idx);
        let off = map_idx as usize * node_size;
        map_idx = BigEndian::read_u32(&extents_data[off..off + 4]);
    }

    if header.root_node != 0 && header.depth > 0 {
        referenced.insert(header.root_node);
        let mut level: Vec<u32> = vec![header.root_node];
        for _l in (2..=header.depth).rev() {
            let mut next = Vec::new();
            for &nidx in &level {
                let off = nidx as usize * node_size;
                if off + node_size > extents_data.len() {
                    continue;
                }
                let node = &extents_data[off..off + node_size];
                if node[8] as i8 != BTREE_INDEX_NODE {
                    continue;
                }
                for child in extract_child_pointers(node, node_size) {
                    if (child as usize) < max_nodes {
                        referenced.insert(child);
                    }
                    next.push(child);
                }
            }
            level = next;
        }

        let mut leaf_idx = header.first_leaf_node;
        let mut lv = HashSet::new();
        while leaf_idx != 0 && lv.insert(leaf_idx) && (leaf_idx as usize) < max_nodes {
            referenced.insert(leaf_idx);
            let off = leaf_idx as usize * node_size;
            leaf_idx = BigEndian::read_u32(&extents_data[off..off + 4]);
        }
    }

    for &nidx in &referenced {
        if !bitmap_test_bit_be(bitmap, nidx) {
            errors.push(hfs_issue(
                HfsFsckCode::ExtentsBtreeStructure,
                format!(
                    "extents node {} referenced but not allocated in node bitmap",
                    nidx
                ),
            ));
        }
    }
}

fn check_extents_key_ordering(
    extents_data: &[u8],
    header: &BTreeHeader,
    errors: &mut Vec<FsckIssue>,
) {
    let node_size = header.node_size as usize;
    let max_nodes = extents_data.len() / node_size;

    for node_idx in 0..max_nodes as u32 {
        if !btree_bitmap_test(extents_data, node_size, node_idx) {
            continue;
        }
        let off = node_idx as usize * node_size;
        let node = &extents_data[off..off + node_size];
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
            if compare_extents_keys(key_a, key_b) != Ordering::Less {
                errors.push(hfs_issue(
                    HfsFsckCode::ExtentsBtreeStructure,
                    format!(
                        "extents node {} records {} and {}: keys not in ascending order",
                        node_idx,
                        i,
                        i + 1
                    ),
                ));
                break;
            }
        }
    }
}

/// Repair extents overflow B-tree structure.
/// Applies the same structural fixes as the catalog B-tree repair:
/// header/root node kinds, map nodes, key ordering, leaf chain, index rebuild,
/// node bitmap.
pub(super) fn repair_extents_btree_structure(
    extents_data: &mut Vec<u8>,
    report: &mut RepairReport,
) {
    if extents_data.len() < 512 {
        return;
    }

    let header = BTreeHeader::read(extents_data);
    let node_size = header.node_size as usize;
    if node_size < 512 || node_size % 512 != 0 || extents_data.len() < node_size {
        report
            .fixes_failed
            .push("Cannot repair extents B-tree: invalid node size".into());
        return;
    }

    // Fix header node kind
    if extents_data[8] as i8 != BTREE_HEADER_NODE {
        extents_data[8] = BTREE_HEADER_NODE as u8;
        report
            .fixes_applied
            .push("Fixed extents B-tree header node kind".into());
    }

    // Fix header node record count and offset table
    let hdr_recs = BigEndian::read_u16(&extents_data[10..12]);
    if hdr_recs != 3 {
        BigEndian::write_u16(&mut extents_data[10..12], 3);
        report.fixes_applied.push(format!(
            "Fixed extents header record count: {} -> 3",
            hdr_recs
        ));
    }

    // Fix root node kind
    let header = BTreeHeader::read(extents_data);
    if header.root_node != 0 && header.depth > 0 {
        let root_off = header.root_node as usize * node_size;
        if root_off + node_size <= extents_data.len() {
            let root_kind = extents_data[root_off + 8] as i8;
            let expected = if header.depth == 1 {
                BTREE_LEAF_NODE
            } else {
                BTREE_INDEX_NODE
            };
            if root_kind != expected {
                extents_data[root_off + 8] = expected as u8;
                report.fixes_applied.push(format!(
                    "Fixed extents root node {} kind: {} -> {}",
                    header.root_node, root_kind, expected
                ));
            }
        }
    }

    // Fix map nodes
    repair_extents_map_nodes(extents_data, node_size, report);

    // Sort records within nodes
    repair_extents_key_ordering(extents_data, node_size, report);

    // Rebuild leaf chain
    rebuild_extents_leaf_chain(extents_data, node_size, report);

    // Rebuild index nodes
    if header.depth > 1 || (header.root_node != 0 && header.depth > 0) {
        rebuild_extents_index_nodes(extents_data, node_size, report);
    }

    // Fix leaf record count
    fix_extents_leaf_record_count(extents_data, report);

    // Fix node bitmap
    repair_extents_node_bitmap(extents_data, node_size, report);
}

fn repair_extents_map_nodes(extents_data: &mut [u8], node_size: usize, report: &mut RepairReport) {
    let mut node_idx = BigEndian::read_u32(&extents_data[0..4]);
    let max_nodes = extents_data.len() / node_size;
    let mut visited = HashSet::new();

    while node_idx != 0 {
        if !visited.insert(node_idx) || node_idx as usize >= max_nodes {
            break;
        }
        let off = node_idx as usize * node_size;
        let mut fixed = false;

        if extents_data[off + 8] as i8 != BTREE_MAP_NODE {
            extents_data[off + 8] = BTREE_MAP_NODE as u8;
            fixed = true;
        }
        if BigEndian::read_u16(&extents_data[off + 10..off + 12]) != 1 {
            BigEndian::write_u16(&mut extents_data[off + 10..off + 12], 1);
            fixed = true;
        }
        let rec0_pos = off + node_size - 2;
        if BigEndian::read_u16(&extents_data[rec0_pos..rec0_pos + 2]) != 14 {
            BigEndian::write_u16(&mut extents_data[rec0_pos..rec0_pos + 2], 14);
            fixed = true;
        }
        let free_pos = off + node_size - 4;
        let expected_free = (node_size - 4) as u16;
        if BigEndian::read_u16(&extents_data[free_pos..free_pos + 2]) != expected_free {
            BigEndian::write_u16(&mut extents_data[free_pos..free_pos + 2], expected_free);
            fixed = true;
        }

        if fixed {
            report
                .fixes_applied
                .push(format!("Fixed extents map node {} structure", node_idx));
        }
        node_idx = BigEndian::read_u32(&extents_data[off..off + 4]);
    }
}

fn repair_extents_key_ordering(
    extents_data: &mut [u8],
    node_size: usize,
    report: &mut RepairReport,
) {
    let max_nodes = extents_data.len() / node_size;
    let mut fixed_count = 0u32;

    for node_idx in 0..max_nodes as u32 {
        if !btree_bitmap_test(extents_data, node_size, node_idx) {
            continue;
        }
        let off = node_idx as usize * node_size;
        let kind = extents_data[off + 8] as i8;
        if kind != BTREE_LEAF_NODE && kind != BTREE_INDEX_NODE {
            continue;
        }
        let num_records = BigEndian::read_u16(&extents_data[off + 10..off + 12]) as usize;
        if num_records < 2 {
            continue;
        }

        let node = &extents_data[off..off + node_size];
        let mut needs_sort = false;
        for i in 0..num_records - 1 {
            let (s1, e1) = btree_record_range(node, node_size, i);
            let (s2, e2) = btree_record_range(node, node_size, i + 1);
            let ka = record_key(node, s1, e1);
            let kb = record_key(node, s2, e2);
            if !ka.is_empty() && !kb.is_empty() && compare_extents_keys(ka, kb) != Ordering::Less {
                needs_sort = true;
                break;
            }
        }
        if !needs_sort {
            continue;
        }

        let mut records: Vec<Vec<u8>> = Vec::with_capacity(num_records);
        for i in 0..num_records {
            let (s, e) = btree_record_range(node, node_size, i);
            if s < e && e <= node_size {
                records.push(node[s..e].to_vec());
            }
        }
        records.sort_by(|a, b| compare_extents_keys(a, b));

        let node_mut = &mut extents_data[off..off + node_size];
        let mut wp = 14usize;
        for (i, rec) in records.iter().enumerate() {
            node_mut[wp..wp + rec.len()].copy_from_slice(rec);
            let opos = node_size - 2 * (i + 1);
            BigEndian::write_u16(&mut node_mut[opos..opos + 2], wp as u16);
            wp += rec.len();
        }
        let fpos = node_size - 2 * (records.len() + 1);
        BigEndian::write_u16(&mut node_mut[fpos..fpos + 2], wp as u16);
        if wp < fpos {
            node_mut[wp..fpos].fill(0);
        }
        fixed_count += 1;
    }

    if fixed_count > 0 {
        report
            .fixes_applied
            .push(format!("Sorted records in {} extents node(s)", fixed_count));
    }
}

fn rebuild_extents_leaf_chain(
    extents_data: &mut [u8],
    node_size: usize,
    report: &mut RepairReport,
) {
    let max_nodes = extents_data.len() / node_size;

    // Collect all leaf nodes (bitmap-allocated with kind=leaf)
    let mut leaf_nodes: Vec<u32> = Vec::new();
    for idx in 1..max_nodes as u32 {
        if !btree_bitmap_test(extents_data, node_size, idx) {
            continue;
        }
        let off = idx as usize * node_size;
        if off + node_size > extents_data.len() {
            break;
        }
        if extents_data[off + 8] as i8 == BTREE_LEAF_NODE {
            leaf_nodes.push(idx);
        }
    }

    if leaf_nodes.is_empty() {
        return;
    }

    // Sort by first key
    leaf_nodes.sort_by(|&a, &b| {
        let off_a = a as usize * node_size;
        let off_b = b as usize * node_size;
        let na = &extents_data[off_a..off_a + node_size];
        let nb = &extents_data[off_b..off_b + node_size];
        let (sa, ea) = btree_record_range(na, node_size, 0);
        let (sb, eb) = btree_record_range(nb, node_size, 0);
        let ka = record_key(na, sa, ea);
        let kb = record_key(nb, sb, eb);
        compare_extents_keys(ka, kb)
    });

    // Relink
    let mut changed = false;
    for (i, &nidx) in leaf_nodes.iter().enumerate() {
        let off = nidx as usize * node_size;
        let flink = if i + 1 < leaf_nodes.len() {
            leaf_nodes[i + 1]
        } else {
            0
        };
        let blink = if i > 0 { leaf_nodes[i - 1] } else { 0 };

        let cur_f = BigEndian::read_u32(&extents_data[off..off + 4]);
        let cur_b = BigEndian::read_u32(&extents_data[off + 4..off + 8]);
        if cur_f != flink || cur_b != blink {
            BigEndian::write_u32(&mut extents_data[off..off + 4], flink);
            BigEndian::write_u32(&mut extents_data[off + 4..off + 8], blink);
            changed = true;
        }
        // Ensure height = 1
        extents_data[off + 9] = 1;
    }

    // Update header
    let mut h = BTreeHeader::read(extents_data);
    let first = leaf_nodes[0];
    let last = *leaf_nodes.last().unwrap();
    if h.first_leaf_node != first || h.last_leaf_node != last {
        h.first_leaf_node = first;
        h.last_leaf_node = last;
        h.write(extents_data);
        changed = true;
    }

    if changed {
        report.fixes_applied.push(format!(
            "Rebuilt extents leaf chain ({} nodes)",
            leaf_nodes.len()
        ));
    }
}

fn rebuild_extents_index_nodes(
    extents_data: &mut [u8],
    node_size: usize,
    report: &mut RepairReport,
) {
    // Free all existing index nodes
    let max_nodes = extents_data.len() / node_size;
    for idx in 1..max_nodes as u32 {
        if btree_bitmap_test(extents_data, node_size, idx) {
            let off = idx as usize * node_size;
            if off + node_size <= extents_data.len()
                && extents_data[off + 8] as i8 == BTREE_INDEX_NODE
            {
                btree_bitmap_clear(extents_data, node_size, idx);
            }
        }
    }

    // Collect leaf nodes from chain
    let header = BTreeHeader::read(extents_data);
    let mut leaves: Vec<u32> = Vec::new();
    let mut node_idx = header.first_leaf_node;
    let mut visited = HashSet::new();
    while node_idx != 0 && visited.insert(node_idx) {
        let off = node_idx as usize * node_size;
        if off + node_size > extents_data.len() {
            break;
        }
        if extents_data[off + 8] as i8 != BTREE_LEAF_NODE {
            break;
        }
        leaves.push(node_idx);
        node_idx = BigEndian::read_u32(&extents_data[off..off + 4]);
    }

    if leaves.len() <= 1 {
        // Single leaf = root, depth = 1
        if !leaves.is_empty() {
            let mut h = BTreeHeader::read(extents_data);
            h.root_node = leaves[0];
            h.depth = 1;
            h.write(extents_data);
        }
        return;
    }

    // Build index levels bottom-up
    let mut children = leaves;
    let mut height = 2u16;

    loop {
        let total_nodes_in_btree = extents_data.len() / node_size;
        let mut idx_nodes: Vec<u32> = Vec::new();
        let mut cur_idx_node =
            match btree_alloc_node(extents_data, node_size, total_nodes_in_btree as u32) {
                Ok(n) => n,
                Err(_) => {
                    report
                        .fixes_failed
                        .push("Cannot allocate extents index node".into());
                    return;
                }
            };
        init_node(
            extents_data,
            node_size,
            cur_idx_node,
            BTREE_INDEX_NODE,
            height as u8,
        );
        idx_nodes.push(cur_idx_node);

        let mut write_pos = 14usize;
        let mut rec_count = 0usize;

        for &child in &children {
            let child_off = child as usize * node_size;
            if child_off + node_size > extents_data.len() {
                continue;
            }
            let child_node = &extents_data[child_off..child_off + node_size];
            let (s, e) = btree_record_range(child_node, node_size, 0);
            if s >= e {
                continue;
            }
            let key = record_key(child_node, s, e).to_vec();
            if key.is_empty() || key.len() < 8 {
                continue;
            }

            // Extents index record: key(8 bytes) + child_ptr(4) = 12 bytes
            let rec_size = 8 + 4;
            let off = cur_idx_node as usize * node_size;
            let ot_size = 2 * (rec_count + 2); // current records + new + free space
            if write_pos + rec_size + ot_size > node_size {
                // Finalize current index node
                let node_mut = &mut extents_data[off..off + node_size];
                BigEndian::write_u16(&mut node_mut[10..12], rec_count as u16);
                let fpos = node_size - 2 * (rec_count + 1);
                BigEndian::write_u16(&mut node_mut[fpos..fpos + 2], write_pos as u16);

                // Allocate a new index node
                cur_idx_node =
                    match btree_alloc_node(extents_data, node_size, total_nodes_in_btree as u32) {
                        Ok(n) => n,
                        Err(_) => {
                            report
                                .fixes_failed
                                .push("Cannot allocate extents index node".into());
                            return;
                        }
                    };
                init_node(
                    extents_data,
                    node_size,
                    cur_idx_node,
                    BTREE_INDEX_NODE,
                    height as u8,
                );
                idx_nodes.push(cur_idx_node);
                write_pos = 14;
                rec_count = 0;
            }

            // Write index record
            let off = cur_idx_node as usize * node_size;
            // Key: key_len(1)=7 + fork_type(1) + file_id(4) + start_block(2) = 8 bytes
            extents_data[off + write_pos..off + write_pos + 8]
                .copy_from_slice(&key[..8.min(key.len())]);
            // Ensure key_len = 7
            extents_data[off + write_pos] = 7;
            BigEndian::write_u32(
                &mut extents_data[off + write_pos + 8..off + write_pos + 12],
                child,
            );
            let opos = off + node_size - 2 * (rec_count + 1);
            BigEndian::write_u16(&mut extents_data[opos..opos + 2], write_pos as u16);
            write_pos += rec_size;
            rec_count += 1;
        }

        // Finalize last index node
        let off = cur_idx_node as usize * node_size;
        BigEndian::write_u16(&mut extents_data[off + 10..off + 12], rec_count as u16);
        let fpos = off + node_size - 2 * (rec_count + 1);
        BigEndian::write_u16(&mut extents_data[fpos..fpos + 2], write_pos as u16);

        // Link index nodes
        for (i, &nidx) in idx_nodes.iter().enumerate() {
            let noff = nidx as usize * node_size;
            let flink = if i + 1 < idx_nodes.len() {
                idx_nodes[i + 1]
            } else {
                0
            };
            let blink = if i > 0 { idx_nodes[i - 1] } else { 0 };
            BigEndian::write_u32(&mut extents_data[noff..noff + 4], flink);
            BigEndian::write_u32(&mut extents_data[noff + 4..noff + 8], blink);
        }

        if idx_nodes.len() == 1 {
            // This is the root
            let mut h = BTreeHeader::read(extents_data);
            h.root_node = idx_nodes[0];
            h.depth = height;
            h.write(extents_data);
            report
                .fixes_applied
                .push(format!("Rebuilt extents index tree (depth {})", height));
            break;
        }

        children = idx_nodes;
        height += 1;
    }
}

fn fix_extents_leaf_record_count(extents_data: &mut [u8], report: &mut RepairReport) {
    let header = BTreeHeader::read(extents_data);
    let actual = count_extents_leaf_records(extents_data, &header);
    if actual != header.leaf_records {
        let old = header.leaf_records;
        let mut h = header;
        h.leaf_records = actual;
        h.write(extents_data);
        report.fixes_applied.push(format!(
            "Fixed extents leaf record count: {} -> {}",
            old, actual
        ));
    }
}

fn repair_extents_node_bitmap(
    extents_data: &mut [u8],
    node_size: usize,
    report: &mut RepairReport,
) {
    let header = BTreeHeader::read(extents_data);
    let (_bmp_off, bmp_size) = btree_node_bitmap_range(extents_data, node_size);
    if bmp_size == 0 {
        return;
    }

    let max_nodes = extents_data.len() / node_size;
    let mut referenced = HashSet::new();
    referenced.insert(0u32);

    // Map nodes
    let mut map_idx = BigEndian::read_u32(&extents_data[0..4]);
    let mut visited = HashSet::new();
    while map_idx != 0 && visited.insert(map_idx) && (map_idx as usize) < max_nodes {
        referenced.insert(map_idx);
        let off = map_idx as usize * node_size;
        map_idx = BigEndian::read_u32(&extents_data[off..off + 4]);
    }

    if header.root_node != 0 && header.depth > 0 {
        referenced.insert(header.root_node);
        let mut level: Vec<u32> = vec![header.root_node];
        for _l in (2..=header.depth).rev() {
            let mut next = Vec::new();
            for &nidx in &level {
                let off = nidx as usize * node_size;
                if off + node_size > extents_data.len() {
                    continue;
                }
                if extents_data[off + 8] as i8 != BTREE_INDEX_NODE {
                    continue;
                }
                let node = &extents_data[off..off + node_size];
                for child in extract_child_pointers(node, node_size) {
                    if (child as usize) < max_nodes {
                        referenced.insert(child);
                    }
                    next.push(child);
                }
            }
            level = next;
        }
        let mut leaf_idx = header.first_leaf_node;
        let mut lv = HashSet::new();
        while leaf_idx != 0 && lv.insert(leaf_idx) && (leaf_idx as usize) < max_nodes {
            referenced.insert(leaf_idx);
            let off = leaf_idx as usize * node_size;
            leaf_idx = BigEndian::read_u32(&extents_data[off..off + 4]);
        }
    }

    let mut fixed = 0u32;
    for &nidx in &referenced {
        if !btree_bitmap_test(extents_data, node_size, nidx) {
            btree_bitmap_set(extents_data, node_size, nidx);
            fixed += 1;
        }
    }

    if fixed > 0 {
        let mut h = BTreeHeader::read(extents_data);
        h.free_nodes = h.free_nodes.saturating_sub(fixed);
        h.write(extents_data);
        report.fixes_applied.push(format!(
            "Marked {} extents node(s) as allocated in node bitmap",
            fixed
        ));
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    // ---- Extents overflow B-tree tests ----

    /// Build a minimal extents overflow B-tree with a header node and one leaf.
    /// The leaf has one extent record for file_id=10, data fork, start_block=0.
    fn make_minimal_extents_btree(node_size: usize) -> Vec<u8> {
        let num_nodes = 4usize;
        let mut data = vec![0u8; node_size * num_nodes];

        // Node 0: Header
        data[8] = BTREE_HEADER_NODE as u8;
        BigEndian::write_u16(&mut data[10..12], 3);

        let hr = 14;
        BigEndian::write_u16(&mut data[hr..hr + 2], 1); // depth = 1
        BigEndian::write_u32(&mut data[hr + 2..hr + 6], 1); // root = 1
        BigEndian::write_u32(&mut data[hr + 6..hr + 10], 1); // leaf_records = 1
        BigEndian::write_u32(&mut data[hr + 10..hr + 14], 1); // first_leaf = 1
        BigEndian::write_u32(&mut data[hr + 14..hr + 18], 1); // last_leaf = 1
        BigEndian::write_u16(&mut data[hr + 18..hr + 20], node_size as u16);
        BigEndian::write_u16(&mut data[hr + 20..hr + 22], 7); // max_key_len = 7
        BigEndian::write_u32(&mut data[hr + 22..hr + 26], num_nodes as u32);
        BigEndian::write_u32(&mut data[hr + 26..hr + 30], (num_nodes - 2) as u32); // free

        // Offset table for header
        let ot = node_size;
        BigEndian::write_u16(&mut data[ot - 2..ot], 14);
        BigEndian::write_u16(&mut data[ot - 4..ot - 2], 0x78);
        BigEndian::write_u16(&mut data[ot - 6..ot - 4], 0xf8);
        BigEndian::write_u16(&mut data[ot - 8..ot - 6], node_size as u16 - 8);

        // Node bitmap: nodes 0 and 1 allocated
        data[0xf8] = 0b11000000;

        // Node 1: Leaf with 1 extent record
        let n1 = node_size;
        data[n1 + 8] = BTREE_LEAF_NODE as u8;
        data[n1 + 9] = 1; // height

        BigEndian::write_u16(&mut data[n1 + 10..n1 + 12], 1); // 1 record

        // Extent record: key(8) + data(12) = 20 bytes
        let r0 = n1 + 14;
        data[r0] = 7; // key_len = 7
        data[r0 + 1] = 0x00; // fork_type = data
        BigEndian::write_u32(&mut data[r0 + 2..r0 + 6], 10); // file_id = 10
        BigEndian::write_u16(&mut data[r0 + 6..r0 + 8], 0); // start_block = 0
                                                            // 3 extent descriptors (each 4 bytes): start_block(2) + block_count(2)
        BigEndian::write_u16(&mut data[r0 + 8..r0 + 10], 100); // ext[0].start = 100
        BigEndian::write_u16(&mut data[r0 + 10..r0 + 12], 5); // ext[0].count = 5
                                                              // ext[1] and ext[2] = 0 (already zeroed)

        // Offset table for leaf
        let lot = n1 + node_size;
        BigEndian::write_u16(&mut data[lot - 2..lot], 14);
        BigEndian::write_u16(&mut data[lot - 4..lot - 2], (14 + 20) as u16); // free

        data
    }

    #[test]
    fn test_extents_btree_valid() {
        let node_size = 512;
        let data = make_minimal_extents_btree(node_size);
        let mut errors = Vec::new();
        check_extents_btree_structure(&data, &mut errors);
        assert!(
            errors.is_empty(),
            "valid extents B-tree: {:?}",
            errors
                .iter()
                .map(|e| e.message.as_str())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_extents_btree_empty() {
        // No extents data at all — should produce no errors
        let mut errors = Vec::new();
        check_extents_btree_structure(&[], &mut errors);
        assert!(errors.is_empty());
    }

    #[test]
    fn test_extents_btree_bad_header() {
        let node_size = 512;
        let mut data = make_minimal_extents_btree(node_size);
        // Corrupt header node kind
        data[8] = BTREE_LEAF_NODE as u8;
        let mut errors = Vec::new();
        check_extents_btree_structure(&data, &mut errors);
        assert!(
            errors
                .iter()
                .any(|e| e.code == "ExtentsBtreeStructure" && e.message.contains("kind")),
            "should detect bad header kind: {:?}",
            errors
                .iter()
                .map(|e| e.message.as_str())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_extents_btree_bad_leaf_record_length() {
        let node_size = 512;
        let mut data = make_minimal_extents_btree(node_size);
        // Change free space offset to make record appear longer than 20
        let lot = node_size + node_size;
        BigEndian::write_u16(&mut data[lot - 4..lot - 2], (14 + 30) as u16);
        let mut errors = Vec::new();
        check_extents_btree_structure(&data, &mut errors);
        assert!(
            errors
                .iter()
                .any(|e| e.code == "ExtentsBtreeStructure" && e.message.contains("length")),
            "should detect bad record length: {:?}",
            errors
                .iter()
                .map(|e| e.message.as_str())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_extents_btree_key_ordering() {
        let node_size = 512;
        let mut data = make_minimal_extents_btree(node_size);
        // Add a second record with a LOWER file_id (out of order)
        let n1 = node_size;
        BigEndian::write_u16(&mut data[n1 + 10..n1 + 12], 2); // 2 records

        // Second record at offset 14 + 20 = 34
        let r1 = n1 + 34;
        data[r1] = 7;
        data[r1 + 1] = 0x00; // data fork
        BigEndian::write_u32(&mut data[r1 + 2..r1 + 6], 5); // file_id = 5 (< 10, out of order!)
        BigEndian::write_u16(&mut data[r1 + 6..r1 + 8], 0);

        let lot = n1 + node_size;
        BigEndian::write_u16(&mut data[lot - 4..lot - 2], 34); // record 1
        BigEndian::write_u16(&mut data[lot - 6..lot - 4], (34 + 20) as u16); // free

        // Update header leaf_records
        BigEndian::write_u32(&mut data[14 + 6..14 + 10], 2);

        let mut errors = Vec::new();
        check_extents_btree_structure(&data, &mut errors);
        assert!(
            errors
                .iter()
                .any(|e| e.code == "ExtentsBtreeStructure" && e.message.contains("ascending")),
            "should detect out-of-order keys: {:?}",
            errors
                .iter()
                .map(|e| e.message.as_str())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_extents_btree_repair() {
        let node_size = 512;
        let mut data = make_minimal_extents_btree(node_size);
        // Corrupt header node kind
        data[8] = BTREE_INDEX_NODE as u8;

        let mut report = RepairReport {
            fixes_applied: Vec::new(),
            fixes_failed: Vec::new(),
            unrepairable_count: 0,
        };
        repair_extents_btree_structure(&mut data, &mut report);
        assert!(
            !report.fixes_applied.is_empty(),
            "should have applied fixes"
        );

        // Verify clean after repair
        let mut errors = Vec::new();
        check_extents_btree_structure(&data, &mut errors);
        assert!(
            errors.is_empty(),
            "should be clean after repair: {:?}",
            errors
                .iter()
                .map(|e| e.message.as_str())
                .collect::<Vec<_>>()
        );
    }
}
