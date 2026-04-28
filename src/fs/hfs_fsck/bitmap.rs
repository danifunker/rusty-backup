//! HFS fsck — Phase 4: extent / allocation-bitmap cross-check + repair.
//!
//! Walks the catalog (inline extents) and the extents-overflow B-tree,
//! recomputes which allocation blocks should be marked used, and compares
//! against the on-disk volume bitmap. Also owns the bitmap/free-block
//! repair (`repair_extents`, `repair_mdb_counts_and_bitmap`).
//!
//! Split out of `hfs_fsck/mod.rs` per §8 of `docs/codecleanup.md`.

use std::collections::HashSet;

use byteorder::{BigEndian, ByteOrder};

use super::super::fsck::{FsckIssue, RepairReport};
use super::super::hfs::{HfsExtDescriptor, HfsMasterDirectoryBlock, CATALOG_FILE};
use super::super::hfs_common::{
    bitmap_set_bit_be, bitmap_test_bit_be, btree_record_range, BTreeHeader, BTREE_LEAF_NODE,
};
use super::{collect_catalog_entries, hfs_issue, CatalogEntry, HfsFsckCode};

/// Maximum number of bitmap mismatch issues to report before capping.
const MAX_BITMAP_MISMATCHES: usize = 20;

fn hfs_debug_issue(code: HfsFsckCode, message: impl Into<String>) -> FsckIssue {
    let mut issue = hfs_issue(code, message);
    issue.debug = true;
    issue
}

/// Collect all extent records from the extents overflow B-tree.
///
/// The extents overflow B-tree stores additional extents for files/forks that
/// need more than 3 extents. Each leaf record key is:
///   keyLength(1) + forkType(1) + fileID(4) + startBlock(2) = 8 bytes
/// Each record contains 3 extent descriptors (12 bytes).
pub(super) fn collect_overflow_extents(extents_data: &[u8]) -> Vec<(u32, u32, String)> {
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

pub(super) fn check_extents_and_bitmap(
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
            bitmap_set_bit_be(&mut computed_bitmap, b);
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
            bitmap_set_bit_be(computed_bitmap, b);
        }
    }
}

/// Phase 3: Fix extent issues (out-of-range and overlapping extents).
pub(super) fn repair_extents(
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
pub(super) fn repair_mdb_counts_and_bitmap(
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
