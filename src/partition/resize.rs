//! In-place partition resizing: physically move partition data within an image
//! file or device, update the partition table, and patch filesystem metadata.

use std::io::{Read, Seek, SeekFrom, Write};

use anyhow::{bail, Context, Result};

use super::editor::{self, PartitionTableEdit};
use super::{PartitionInfo, PartitionTable};

/// Chunk size for data copy operations (256 KiB).
const CHUNK_SIZE: usize = 256 * 1024;

/// Describes one partition's resize/move plan.
#[derive(Debug, Clone)]
pub struct PartitionResizePlan {
    pub index: usize,
    pub old_start_lba: u64,
    pub old_size_bytes: u64,
    pub new_start_lba: u64,
    pub new_size_bytes: u64,
    /// True if partition data must be relocated on disk.
    pub needs_data_move: bool,
    /// Byte offset change: positive = moved forward, negative = moved backward.
    pub move_delta_bytes: i64,
}

/// Compute a resize plan for the given partitions.
///
/// `desired_sizes` maps partition index → new size in bytes.
/// Only the listed partitions are resized; others keep their original size.
/// Subsequent partitions are shifted so nothing overlaps.
///
/// Returns one `PartitionResizePlan` entry per primary/extended partition
/// (logical partitions are skipped with an error if someone tries to resize them).
pub fn compute_resize_plan(
    partitions: &[PartitionInfo],
    desired_sizes: &[(usize, u64)],
    alignment_sectors: u64,
    disk_size_bytes: u64,
) -> Result<Vec<PartitionResizePlan>> {
    // Reject logical partitions
    for &(idx, _) in desired_sizes {
        if let Some(p) = partitions.iter().find(|p| p.index == idx) {
            if p.is_logical {
                bail!(
                    "Cannot resize logical partition {} (EBR resize not supported)",
                    idx
                );
            }
        }
    }

    // Build a size lookup from desired_sizes
    let size_map: std::collections::HashMap<usize, u64> = desired_sizes.iter().cloned().collect();

    // Work with primary/non-logical partitions sorted by start LBA
    let mut primaries: Vec<&PartitionInfo> = partitions.iter().filter(|p| !p.is_logical).collect();
    primaries.sort_by_key(|p| p.start_lba);

    let mut plans = Vec::new();
    // Track cumulative shift in bytes from all prior partitions' size changes
    let mut cumulative_shift: i64 = 0;

    for p in &primaries {
        let new_size = size_map.get(&p.index).copied().unwrap_or(p.size_bytes);
        let size_delta = new_size as i64 - p.size_bytes as i64;

        // New start = old start + cumulative shift from prior partitions
        let old_start_bytes = p.start_lba * 512;
        let new_start_bytes_raw = (old_start_bytes as i64 + cumulative_shift) as u64;

        // Align the new start if alignment is set and this isn't the first partition
        let new_start_bytes = if alignment_sectors > 0 {
            let align_bytes = alignment_sectors * 512;
            // Round up to alignment boundary, but keep sector 0 partitions at 0
            if new_start_bytes_raw == 0 {
                0
            } else {
                new_start_bytes_raw.div_ceil(align_bytes) * align_bytes
            }
        } else {
            new_start_bytes_raw
        };

        let new_start_lba = new_start_bytes / 512;
        let move_delta = new_start_bytes as i64 - old_start_bytes as i64;
        let needs_move = move_delta != 0;

        // Check it fits on disk
        if new_start_bytes + new_size > disk_size_bytes {
            bail!(
                "Partition {} would end at {} which exceeds disk size {}",
                p.index,
                crate::partition::format_size(new_start_bytes + new_size),
                crate::partition::format_size(disk_size_bytes),
            );
        }

        plans.push(PartitionResizePlan {
            index: p.index,
            old_start_lba: p.start_lba,
            old_size_bytes: p.size_bytes,
            new_start_lba,
            new_size_bytes: new_size,
            needs_data_move: needs_move,
            move_delta_bytes: move_delta,
        });

        // The shift that this partition's size change imposes on all subsequent partitions
        cumulative_shift += size_delta;
        // Also account for any alignment padding that was added
        let alignment_padding = new_start_bytes as i64 - new_start_bytes_raw as i64;
        if alignment_padding != 0 {
            cumulative_shift += alignment_padding;
        }
    }

    // Validate no overlaps in the new layout
    for i in 1..plans.len() {
        let prev_end = plans[i - 1].new_start_lba * 512 + plans[i - 1].new_size_bytes;
        let curr_start = plans[i].new_start_lba * 512;
        if prev_end > curr_start {
            bail!(
                "Partitions {} and {} would overlap after resize (end {} > start {})",
                plans[i - 1].index,
                plans[i].index,
                crate::partition::format_size(prev_end),
                crate::partition::format_size(curr_start),
            );
        }
    }

    Ok(plans)
}

/// Check if a file is a VHD by looking for the "conectix" cookie at end - 512.
pub fn detect_vhd(file: &mut (impl Read + Seek), file_size: u64) -> bool {
    if file_size < 512 {
        return false;
    }
    let mut buf = [0u8; 8];
    if file.seek(SeekFrom::Start(file_size - 512)).is_err() {
        return false;
    }
    if file.read_exact(&mut buf).is_err() {
        return false;
    }
    &buf == b"conectix"
}

/// Shift a region of data within a file by `delta_bytes`.
///
/// - Positive delta: shift data forward (copy backward to avoid clobbering)
/// - Negative delta: shift data backward (copy forward to avoid clobbering)
/// - Zero-fills the vacated gap after the copy
///
/// `progress_cb` receives (bytes_copied, total_bytes).
pub fn shift_region(
    file: &mut (impl Read + Write + Seek),
    src_start: u64,
    src_end: u64,
    delta_bytes: i64,
    progress_cb: &mut impl FnMut(u64, u64),
) -> Result<()> {
    let data_len = src_end.saturating_sub(src_start);
    if data_len == 0 || delta_bytes == 0 {
        return Ok(());
    }

    let mut buf = vec![0u8; CHUNK_SIZE];
    let abs_delta = delta_bytes.unsigned_abs();

    if delta_bytes > 0 {
        // Shift forward: copy backward (from end to start) to avoid clobbering
        let mut remaining = data_len;
        let mut bytes_done: u64 = 0;
        while remaining > 0 {
            let chunk = remaining.min(CHUNK_SIZE as u64);
            let read_pos = src_start + remaining - chunk;

            file.seek(SeekFrom::Start(read_pos))?;
            file.read_exact(&mut buf[..chunk as usize])?;

            file.seek(SeekFrom::Start(read_pos + abs_delta))?;
            file.write_all(&buf[..chunk as usize])?;

            remaining -= chunk;
            bytes_done += chunk;
            progress_cb(bytes_done, data_len);
        }
    } else {
        // Shift backward: copy forward (from start to end) to avoid clobbering
        let mut offset: u64 = 0;
        while offset < data_len {
            let chunk = (data_len - offset).min(CHUNK_SIZE as u64);
            let read_pos = src_start + offset;

            file.seek(SeekFrom::Start(read_pos))?;
            file.read_exact(&mut buf[..chunk as usize])?;

            let write_pos = (read_pos as i64 + delta_bytes) as u64;
            file.seek(SeekFrom::Start(write_pos))?;
            file.write_all(&buf[..chunk as usize])?;

            offset += chunk;
            progress_cb(offset, data_len);
        }
    }

    // Zero-fill the vacated gap
    let zeros = vec![0u8; CHUNK_SIZE];
    let (gap_start, gap_len) = if delta_bytes > 0 {
        // Forward shift: gap is at original start
        (src_start, abs_delta.min(data_len))
    } else {
        // Backward shift: gap is at the end of where data used to be
        let gap_start = (src_end as i64 + delta_bytes) as u64;
        (gap_start, abs_delta.min(data_len))
    };

    file.seek(SeekFrom::Start(gap_start))?;
    let mut remaining = gap_len;
    while remaining > 0 {
        let n = (remaining as usize).min(CHUNK_SIZE);
        file.write_all(&zeros[..n])?;
        remaining -= n as u64;
    }

    Ok(())
}

/// Apply a complete resize operation: move data, update partition table, patch filesystems.
///
/// Steps:
/// 1. Expand file if needed (image files only)
/// 2. Move data — backward moves (front-to-back)
/// 3. Move data — forward moves (back-to-front)
/// 4. Update partition table
/// 5. Patch filesystem metadata for resized/moved partitions
/// 6. Truncate file if needed (image files only)
/// 7. Regenerate VHD footer if applicable
#[allow(clippy::too_many_arguments)]
pub fn apply_resize(
    file: &mut (impl Read + Write + Seek),
    plans: &[PartitionResizePlan],
    table: &PartitionTable,
    is_device: bool,
    is_vhd: bool,
    old_disk_size: u64,
    progress_cb: &mut impl FnMut(u64, u64),
    log_cb: &mut impl FnMut(&str),
) -> Result<()> {
    // Calculate new disk size: find the furthest end of any partition
    let new_data_end = plans
        .iter()
        .map(|p| p.new_start_lba * 512 + p.new_size_bytes)
        .max()
        .unwrap_or(old_disk_size);

    // For VHD, the "disk size" is the data portion (excluding 512-byte footer)
    let old_data_size = if is_vhd {
        old_disk_size.saturating_sub(512)
    } else {
        old_disk_size
    };

    let new_data_size = new_data_end.max(old_data_size);
    let new_file_size = if is_vhd {
        new_data_size + 512
    } else {
        new_data_size
    };

    // Calculate total bytes to move for progress reporting
    let total_move_bytes: u64 = plans
        .iter()
        .filter(|p| p.needs_data_move)
        .map(|p| p.old_size_bytes)
        .sum();
    let mut moved_so_far: u64 = 0;

    // Step 1: Expand file if new size > old size (image files only)
    if !is_device && new_file_size > old_disk_size {
        log_cb(&format!(
            "Expanding file from {} to {}",
            crate::partition::format_size(old_disk_size),
            crate::partition::format_size(new_file_size),
        ));
        // We need to use set_len but we have a generic impl. We'll write zeros at the end.
        file.seek(SeekFrom::Start(new_file_size - 1))?;
        file.write_all(&[0])?;
        file.flush()?;
    }

    // Step 2: Backward moves (negative delta) — process front-to-back
    let mut backward_plans: Vec<&PartitionResizePlan> = plans
        .iter()
        .filter(|p| p.needs_data_move && p.move_delta_bytes < 0)
        .collect();
    backward_plans.sort_by_key(|p| p.old_start_lba);

    for plan in &backward_plans {
        let src_start = plan.old_start_lba * 512;
        let src_end = src_start + plan.old_size_bytes;
        log_cb(&format!(
            "Moving partition {} backward by {}",
            plan.index,
            crate::partition::format_size(plan.move_delta_bytes.unsigned_abs()),
        ));

        let base = moved_so_far;
        shift_region(
            file,
            src_start,
            src_end,
            plan.move_delta_bytes,
            &mut |done, _total| {
                progress_cb(base + done, total_move_bytes);
            },
        )?;
        moved_so_far += plan.old_size_bytes;
    }

    // Step 3: Forward moves (positive delta) — process back-to-front
    let mut forward_plans: Vec<&PartitionResizePlan> = plans
        .iter()
        .filter(|p| p.needs_data_move && p.move_delta_bytes > 0)
        .collect();
    forward_plans.sort_by_key(|p| std::cmp::Reverse(p.old_start_lba));

    for plan in &forward_plans {
        let src_start = plan.old_start_lba * 512;
        let src_end = src_start + plan.old_size_bytes;
        log_cb(&format!(
            "Moving partition {} forward by {}",
            plan.index,
            crate::partition::format_size(plan.move_delta_bytes.unsigned_abs()),
        ));

        let base = moved_so_far;
        shift_region(
            file,
            src_start,
            src_end,
            plan.move_delta_bytes,
            &mut |done, _total| {
                progress_cb(base + done, total_move_bytes);
            },
        )?;
        moved_so_far += plan.old_size_bytes;
    }

    // Step 4: Update partition table entries
    log_cb("Updating partition table...");
    let mut edits = Vec::new();
    for plan in plans {
        if plan.new_size_bytes != plan.old_size_bytes {
            edits.push(PartitionTableEdit::ResizeEntry {
                index: plan.index,
                new_size_bytes: plan.new_size_bytes,
            });
        }
        if plan.new_start_lba != plan.old_start_lba {
            edits.push(PartitionTableEdit::MoveEntry {
                index: plan.index,
                new_start_lba: plan.new_start_lba,
            });
        }
    }

    if !edits.is_empty() {
        editor::apply_edits(file, table, &edits, new_data_size, log_cb)
            .context("Failed to update partition table")?;
    }

    // Step 5: Patch filesystem metadata for resized/moved partitions
    log_cb("Patching filesystem metadata...");
    for plan in plans {
        let was_resized = plan.new_size_bytes != plan.old_size_bytes;
        let was_moved = plan.new_start_lba != plan.old_start_lba;

        if !was_resized && !was_moved {
            continue;
        }

        let partition_offset = plan.new_start_lba * 512;
        let fs_type = crate::restore::detect_partition_fs_type(file, partition_offset);

        if was_resized {
            patch_filesystem_size(
                file,
                &fs_type,
                partition_offset,
                plan.new_size_bytes,
                log_cb,
            )?;
        }

        if was_moved {
            patch_filesystem_hidden_sectors(
                file,
                &fs_type,
                partition_offset,
                plan.new_start_lba,
                log_cb,
            )?;
        }
    }

    // Step 6: Truncate file if new size < old size (image files only)
    if !is_device && new_data_end < old_data_size {
        let target_size = if is_vhd {
            new_data_end + 512
        } else {
            new_data_end
        };
        log_cb(&format!(
            "Truncating file to {}",
            crate::partition::format_size(target_size),
        ));
        // We can't call set_len on a generic. We'll leave the file as-is
        // and document that truncation requires a std::fs::File.
        // The GUI layer will handle truncation after this returns.
    }

    // Step 7: Regenerate VHD footer if applicable
    if is_vhd {
        log_cb("Regenerating VHD footer...");
        let footer = crate::rbformats::vhd::build_vhd_footer(new_data_end);
        file.seek(SeekFrom::Start(new_data_end))?;
        file.write_all(&footer)?;
    }

    file.flush()?;
    log_cb("Resize complete.");
    Ok(())
}

/// Patch filesystem size metadata after a partition resize.
fn patch_filesystem_size(
    file: &mut (impl Read + Write + Seek),
    fs_type: &crate::restore::PartitionFsType,
    partition_offset: u64,
    new_size_bytes: u64,
    log_cb: &mut impl FnMut(&str),
) -> Result<()> {
    use crate::restore::PartitionFsType;

    match fs_type {
        PartitionFsType::Fat => {
            let new_sectors = (new_size_bytes / 512) as u32;
            crate::fs::fat::resize_fat_in_place(file, partition_offset, new_sectors, log_cb)?;
        }
        PartitionFsType::Ntfs => {
            let new_sectors = new_size_bytes / 512;
            crate::fs::ntfs::resize_ntfs_in_place(file, partition_offset, new_sectors, log_cb)?;
        }
        PartitionFsType::Exfat => {
            let new_sectors = new_size_bytes / 512;
            crate::fs::exfat::resize_exfat_in_place(file, partition_offset, new_sectors, log_cb)?;
        }
        PartitionFsType::Ext => {
            crate::fs::ext::resize_ext_in_place(file, partition_offset, new_size_bytes, log_cb)?;
        }
        PartitionFsType::Btrfs => {
            crate::fs::btrfs::resize_btrfs_in_place(
                file,
                partition_offset,
                new_size_bytes,
                log_cb,
            )?;
        }
        PartitionFsType::Hfs => {
            crate::fs::hfs::resize_hfs_in_place(file, partition_offset, new_size_bytes, log_cb)?;
        }
        PartitionFsType::HfsPlus => {
            crate::fs::hfsplus::resize_hfsplus_in_place(
                file,
                partition_offset,
                new_size_bytes,
                log_cb,
            )?;
        }
        PartitionFsType::ProDos => {
            crate::fs::prodos::resize_prodos_in_place(
                file,
                partition_offset,
                new_size_bytes,
                log_cb,
            )?;
        }
        PartitionFsType::Unknown => {
            log_cb("Unknown filesystem type — skipping resize metadata patch");
        }
    }
    Ok(())
}

/// Patch filesystem hidden-sectors field after a partition move.
fn patch_filesystem_hidden_sectors(
    file: &mut (impl Read + Write + Seek),
    fs_type: &crate::restore::PartitionFsType,
    partition_offset: u64,
    new_start_lba: u64,
    log_cb: &mut impl FnMut(&str),
) -> Result<()> {
    use crate::restore::PartitionFsType;

    match fs_type {
        PartitionFsType::Fat => {
            crate::fs::fat::patch_bpb_hidden_sectors(
                file,
                partition_offset,
                new_start_lba,
                log_cb,
            )?;
        }
        PartitionFsType::Ntfs => {
            crate::fs::ntfs::patch_ntfs_hidden_sectors(
                file,
                partition_offset,
                new_start_lba,
                log_cb,
            )?;
        }
        PartitionFsType::Exfat => {
            crate::fs::exfat::patch_exfat_hidden_sectors(
                file,
                partition_offset,
                new_start_lba,
                log_cb,
            )?;
        }
        PartitionFsType::Hfs => {
            crate::fs::hfs::patch_hfs_hidden_sectors(
                file,
                partition_offset,
                new_start_lba,
                log_cb,
            )?;
        }
        PartitionFsType::HfsPlus => {
            crate::fs::hfsplus::patch_hfsplus_hidden_sectors(
                file,
                partition_offset,
                new_start_lba,
                log_cb,
            )?;
        }
        // ext, btrfs, prodos have no hidden sectors field
        PartitionFsType::Ext
        | PartitionFsType::Btrfs
        | PartitionFsType::ProDos
        | PartitionFsType::Unknown => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn make_partitions(specs: &[(usize, u64, u64, bool)]) -> Vec<PartitionInfo> {
        specs
            .iter()
            .map(
                |&(index, start_lba, size_bytes, is_logical)| PartitionInfo {
                    index,
                    type_name: "FAT32".to_string(),
                    partition_type_byte: 0x0C,
                    start_lba,
                    size_bytes,
                    bootable: false,
                    is_logical,
                    is_extended_container: false,
                    partition_type_string: None,
                    hfs_block_size: None,
                },
            )
            .collect()
    }

    #[test]
    fn test_shift_region_forward() {
        // [AAAA____] → [____AAAA]
        let mut data = vec![0u8; 1024];
        data[0..4].copy_from_slice(b"AAAA");

        let mut cursor = Cursor::new(data);
        shift_region(&mut cursor, 0, 4, 4, &mut |_, _| {}).unwrap();

        let buf = cursor.into_inner();
        assert_eq!(&buf[0..4], &[0, 0, 0, 0]); // gap zeroed
        assert_eq!(&buf[4..8], b"AAAA"); // data moved
    }

    #[test]
    fn test_shift_region_backward() {
        // [____AAAA] → [AAAA____]
        let mut data = vec![0u8; 1024];
        data[4..8].copy_from_slice(b"AAAA");

        let mut cursor = Cursor::new(data);
        shift_region(&mut cursor, 4, 8, -4, &mut |_, _| {}).unwrap();

        let buf = cursor.into_inner();
        assert_eq!(&buf[0..4], b"AAAA"); // data moved
        assert_eq!(&buf[4..8], &[0, 0, 0, 0]); // gap zeroed
    }

    #[test]
    fn test_shift_region_zero_delta() {
        let mut data = vec![0xAA; 64];
        let mut cursor = Cursor::new(data.clone());
        shift_region(&mut cursor, 0, 64, 0, &mut |_, _| {}).unwrap();
        // No change
        assert_eq!(cursor.into_inner(), data);
    }

    #[test]
    fn test_shift_region_large_forward() {
        // Test with data larger than CHUNK_SIZE
        let size = CHUNK_SIZE * 3;
        let mut data = vec![0u8; size + CHUNK_SIZE];
        for i in 0..size {
            data[i] = (i % 256) as u8;
        }
        let original = data[..size].to_vec();

        let delta = 512i64;
        let mut cursor = Cursor::new(data);
        shift_region(&mut cursor, 0, size as u64, delta, &mut |_, _| {}).unwrap();

        let buf = cursor.into_inner();
        // First 512 bytes should be zeroed
        assert!(buf[..512].iter().all(|&b| b == 0));
        // Shifted data should match
        assert_eq!(&buf[512..size + 512], &original[..]);
    }

    #[test]
    fn test_compute_plan_grow_first_partition() {
        // Two partitions: p0 at LBA 63 (10 MiB), p1 at LBA 63+20480 (10 MiB)
        let p0_start = 63u64;
        let p0_size = 10 * 1024 * 1024u64; // 10 MiB
        let p1_start = p0_start + p0_size / 512;
        let p1_size = 10 * 1024 * 1024u64;
        let disk_size = (p1_start + p1_size / 512) * 512 + 50 * 1024 * 1024;

        let parts =
            make_partitions(&[(0, p0_start, p0_size, false), (1, p1_start, p1_size, false)]);

        // Grow p0 by 5 MiB
        let new_p0_size = 15 * 1024 * 1024u64;
        let plans = compute_resize_plan(&parts, &[(0, new_p0_size)], 0, disk_size).unwrap();

        assert_eq!(plans.len(), 2);
        // p0: same start, bigger size
        assert_eq!(plans[0].new_start_lba, p0_start);
        assert_eq!(plans[0].new_size_bytes, new_p0_size);
        assert!(!plans[0].needs_data_move);

        // p1: shifted forward by 5 MiB
        let expected_p1_start = p1_start + (5 * 1024 * 1024 / 512);
        assert_eq!(plans[1].new_start_lba, expected_p1_start);
        assert_eq!(plans[1].new_size_bytes, p1_size); // unchanged
        assert!(plans[1].needs_data_move);
        assert_eq!(plans[1].move_delta_bytes, 5 * 1024 * 1024);
    }

    #[test]
    fn test_compute_plan_shrink_first_partition() {
        let p0_start = 63u64;
        let p0_size = 10 * 1024 * 1024u64;
        let p1_start = p0_start + p0_size / 512;
        let p1_size = 10 * 1024 * 1024u64;
        let disk_size = (p1_start + p1_size / 512) * 512;

        let parts =
            make_partitions(&[(0, p0_start, p0_size, false), (1, p1_start, p1_size, false)]);

        // Shrink p0 by 5 MiB
        let new_p0_size = 5 * 1024 * 1024u64;
        let plans = compute_resize_plan(&parts, &[(0, new_p0_size)], 0, disk_size).unwrap();

        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].new_size_bytes, new_p0_size);
        assert!(!plans[0].needs_data_move);

        // p1: shifted backward by 5 MiB
        assert!(plans[1].needs_data_move);
        assert_eq!(plans[1].move_delta_bytes, -(5 * 1024 * 1024i64));
    }

    #[test]
    fn test_compute_plan_resize_last_only() {
        let p0_start = 63u64;
        let p0_size = 10 * 1024 * 1024u64;
        let disk_size = (p0_start + p0_size / 512) * 512 + 50 * 1024 * 1024;

        let parts = make_partitions(&[(0, p0_start, p0_size, false)]);

        // Grow the only partition
        let new_size = 20 * 1024 * 1024u64;
        let plans = compute_resize_plan(&parts, &[(0, new_size)], 0, disk_size).unwrap();

        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].new_size_bytes, new_size);
        assert!(!plans[0].needs_data_move); // no move needed, just grow
    }

    #[test]
    fn test_compute_plan_exceeds_disk() {
        let p0_start = 63u64;
        let p0_size = 10 * 1024 * 1024u64;
        let disk_size = (p0_start + p0_size / 512) * 512; // exactly fits

        let parts = make_partitions(&[(0, p0_start, p0_size, false)]);

        // Try to grow beyond disk
        let result = compute_resize_plan(&parts, &[(0, 20 * 1024 * 1024)], 0, disk_size);
        assert!(result.is_err());
    }

    #[test]
    fn test_compute_plan_rejects_logical() {
        let parts = make_partitions(&[(4, 100, 1024 * 1024, true)]);
        let result = compute_resize_plan(&parts, &[(4, 2 * 1024 * 1024)], 0, 100 * 1024 * 1024);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("logical"));
    }

    #[test]
    fn test_detect_vhd_true() {
        let mut data = vec![0u8; 1024];
        data[512..520].copy_from_slice(b"conectix");
        let mut cursor = Cursor::new(data);
        assert!(detect_vhd(&mut cursor, 1024));
    }

    #[test]
    fn test_detect_vhd_false() {
        let mut data = vec![0u8; 1024];
        let mut cursor = Cursor::new(data);
        assert!(!detect_vhd(&mut cursor, 1024));
    }
}
