use std::io::{Read, Seek, SeekFrom, Write};

use anyhow::{bail, Result};

use super::apm::Apm;
use super::gpt::Gpt;
use super::mbr::{lba_to_chs, Mbr};
use super::PartitionTable;

/// A single edit operation on a partition table.
#[derive(Debug, Clone)]
pub enum PartitionTableEdit {
    /// Resize a partition entry (changes total_sectors / end_lba, NOT the data).
    ResizeEntry { index: usize, new_size_bytes: u64 },
    /// Move a partition entry to a new start LBA (does NOT move data).
    MoveEntry { index: usize, new_start_lba: u64 },
    /// Change the partition type byte/GUID.
    ChangeType {
        index: usize,
        /// MBR: new partition type byte. GPT/APM: ignored (use type_string/guid).
        new_type_byte: u8,
        /// GPT: new type GUID string. APM: new type string.
        new_type_string: Option<String>,
    },
    /// Delete a partition entry (zero it out).
    DeleteEntry { index: usize },
    /// Add a new partition entry.
    AddEntry {
        start_lba: u64,
        size_bytes: u64,
        /// MBR type byte.
        partition_type: u8,
        /// GPT/APM type string (GUID string for GPT, type name for APM).
        type_string: Option<String>,
        bootable: bool,
    },
}

/// Validate a set of edits against the current partition table.
///
/// Returns a list of warning messages. Returns Err if any edit is invalid
/// (e.g. overlapping partitions, exceeds disk size).
pub fn validate_edits(
    current_table: &PartitionTable,
    edits: &[PartitionTableEdit],
    disk_size_bytes: u64,
) -> Result<Vec<String>> {
    let mut warnings = Vec::new();
    let mut partitions = current_table.partitions();

    // Apply edits to a simulated partition list
    for edit in edits {
        match edit {
            PartitionTableEdit::ResizeEntry {
                index,
                new_size_bytes,
            } => {
                if let Some(p) = partitions.iter_mut().find(|p| p.index == *index) {
                    p.size_bytes = *new_size_bytes;
                } else {
                    bail!("partition index {} not found", index);
                }
            }
            PartitionTableEdit::MoveEntry {
                index,
                new_start_lba,
            } => {
                if let Some(p) = partitions.iter_mut().find(|p| p.index == *index) {
                    p.start_lba = *new_start_lba;
                } else {
                    bail!("partition index {} not found", index);
                }
            }
            PartitionTableEdit::ChangeType { index, .. } => {
                if !partitions.iter().any(|p| p.index == *index) {
                    bail!("partition index {} not found", index);
                }
            }
            PartitionTableEdit::DeleteEntry { index } => {
                partitions.retain(|p| p.index != *index);
            }
            PartitionTableEdit::AddEntry {
                start_lba,
                size_bytes,
                ..
            } => {
                // Find next available index
                let next_idx = partitions.iter().map(|p| p.index).max().unwrap_or(0) + 1;
                partitions.push(super::PartitionInfo {
                    index: next_idx,
                    type_name: String::new(),
                    partition_type_byte: 0,
                    start_lba: *start_lba,
                    size_bytes: *size_bytes,
                    bootable: false,
                    is_logical: false,
                    is_extended_container: false,
                    partition_type_string: None,
                    hfs_block_size: None,
                });
            }
        }
    }

    // Check for overlaps
    let mut sorted: Vec<_> = partitions
        .iter()
        .filter(|p| !p.is_extended_container)
        .collect();
    sorted.sort_by_key(|p| p.start_lba);

    for i in 0..sorted.len() {
        let end_lba = sorted[i].start_lba + sorted[i].size_bytes / 512;

        // Check disk bounds
        if end_lba * 512 > disk_size_bytes {
            bail!(
                "partition {} extends beyond disk (ends at LBA {}, disk has {} sectors)",
                sorted[i].index,
                end_lba,
                disk_size_bytes / 512,
            );
        }

        // Check overlap with next
        if i + 1 < sorted.len() && end_lba > sorted[i + 1].start_lba {
            bail!(
                "partitions {} and {} overlap (partition {} ends at LBA {}, partition {} starts at LBA {})",
                sorted[i].index,
                sorted[i + 1].index,
                sorted[i].index,
                end_lba,
                sorted[i + 1].index,
                sorted[i + 1].start_lba,
            );
        }
    }

    // MBR: check max 4 primary entries
    if matches!(current_table, PartitionTable::Mbr(_)) {
        let primary_count = partitions.iter().filter(|p| !p.is_logical).count();
        if primary_count > 4 {
            bail!(
                "MBR supports at most 4 primary partitions (have {})",
                primary_count
            );
        }
    }

    // Check for very small partitions
    for p in &partitions {
        if p.size_bytes > 0 && p.size_bytes < 512 {
            warnings.push(format!(
                "partition {} is smaller than one sector ({} bytes)",
                p.index, p.size_bytes,
            ));
        }
    }

    Ok(warnings)
}

/// Apply validated edits to a device or image file.
///
/// This modifies only the partition table structures (MBR, GPT header/entries, APM map).
/// Partition data is NOT touched.
pub fn apply_edits(
    file: &mut (impl Read + Write + Seek),
    current_table: &PartitionTable,
    edits: &[PartitionTableEdit],
    disk_size_bytes: u64,
    log_cb: &mut impl FnMut(&str),
) -> Result<()> {
    match current_table {
        PartitionTable::Mbr(mbr) => apply_mbr_edits(file, mbr, edits, log_cb),
        PartitionTable::Gpt { gpt, .. } => {
            apply_gpt_edits(file, gpt, edits, disk_size_bytes, log_cb)
        }
        PartitionTable::Apm(apm) => apply_apm_edits(file, apm, edits, disk_size_bytes, log_cb),
        PartitionTable::None { .. } => bail!("cannot edit partition table on a superfloppy"),
    }
}

fn apply_mbr_edits(
    file: &mut (impl Read + Write + Seek),
    mbr: &Mbr,
    edits: &[PartitionTableEdit],
    log_cb: &mut impl FnMut(&str),
) -> Result<()> {
    // Read current MBR
    file.seek(SeekFrom::Start(0))?;
    let mut mbr_bytes = [0u8; 512];
    file.read_exact(&mut mbr_bytes)?;

    // Extract CHS geometry from existing entries
    let (heads, spt) = {
        let mut max_head: u32 = 0;
        let mut max_sector: u32 = 0;
        for e in &mbr.entries {
            if !e.is_empty() {
                max_head = max_head
                    .max(e.chs_start.head as u32)
                    .max(e.chs_end.head as u32);
                max_sector = max_sector
                    .max(e.chs_start.sector as u32)
                    .max(e.chs_end.sector as u32);
            }
        }
        let h = if max_head > 0 { max_head + 1 } else { 255 };
        let s = if max_sector > 0 { max_sector } else { 63 };
        (h, s)
    };

    for edit in edits {
        match edit {
            PartitionTableEdit::ResizeEntry {
                index,
                new_size_bytes,
            } => {
                if *index >= 4 {
                    log_cb(&format!(
                        "Skipping resize of logical partition {} (EBR editing not supported)",
                        index
                    ));
                    continue;
                }
                let offset = 446 + index * 16;
                let start_lba = u32::from_le_bytes(mbr_bytes[offset + 8..offset + 12].try_into()?);
                let new_sectors = (*new_size_bytes / 512) as u32;

                // Update total sectors
                mbr_bytes[offset + 12..offset + 16].copy_from_slice(&new_sectors.to_le_bytes());

                // Update CHS end
                let end_lba = start_lba.saturating_add(new_sectors).saturating_sub(1);
                let (cyl, head, sec) = lba_to_chs(end_lba, heads, spt);
                mbr_bytes[offset + 5] = head as u8;
                mbr_bytes[offset + 6] = (sec as u8 & 0x3F) | ((cyl >> 2) as u8 & 0xC0);
                mbr_bytes[offset + 7] = cyl as u8;

                log_cb(&format!(
                    "Resized partition {} to {} sectors",
                    index, new_sectors
                ));
            }
            PartitionTableEdit::MoveEntry {
                index,
                new_start_lba,
            } => {
                if *index >= 4 {
                    log_cb(&format!(
                        "Skipping move of logical partition {} (EBR editing not supported)",
                        index
                    ));
                    continue;
                }
                let offset = 446 + index * 16;
                let total_sectors =
                    u32::from_le_bytes(mbr_bytes[offset + 12..offset + 16].try_into()?);

                // Update start LBA
                mbr_bytes[offset + 8..offset + 12]
                    .copy_from_slice(&(*new_start_lba as u32).to_le_bytes());

                // Update CHS start
                let (cyl, head, sec) = lba_to_chs(*new_start_lba as u32, heads, spt);
                mbr_bytes[offset + 1] = head as u8;
                mbr_bytes[offset + 2] = (sec as u8 & 0x3F) | ((cyl >> 2) as u8 & 0xC0);
                mbr_bytes[offset + 3] = cyl as u8;

                // Update CHS end
                let end_lba = (*new_start_lba as u32)
                    .saturating_add(total_sectors)
                    .saturating_sub(1);
                let (cyl, head, sec) = lba_to_chs(end_lba, heads, spt);
                mbr_bytes[offset + 5] = head as u8;
                mbr_bytes[offset + 6] = (sec as u8 & 0x3F) | ((cyl >> 2) as u8 & 0xC0);
                mbr_bytes[offset + 7] = cyl as u8;

                log_cb(&format!(
                    "Moved partition {} to LBA {}",
                    index, new_start_lba
                ));
            }
            PartitionTableEdit::ChangeType {
                index,
                new_type_byte,
                ..
            } => {
                if *index >= 4 {
                    continue;
                }
                let offset = 446 + index * 16;
                mbr_bytes[offset + 4] = *new_type_byte;
                log_cb(&format!(
                    "Changed partition {} type to 0x{:02X}",
                    index, new_type_byte
                ));
            }
            PartitionTableEdit::DeleteEntry { index } => {
                if *index >= 4 {
                    continue;
                }
                let offset = 446 + index * 16;
                mbr_bytes[offset..offset + 16].fill(0);
                log_cb(&format!("Deleted partition {}", index));
            }
            PartitionTableEdit::AddEntry {
                start_lba,
                size_bytes,
                partition_type,
                bootable,
                ..
            } => {
                // Find first empty slot
                let slot = (0..4).find(|i| {
                    let offset = 446 + i * 16;
                    mbr_bytes[offset + 4] == 0 // type byte = 0 means empty
                });
                let slot = match slot {
                    Some(s) => s,
                    None => {
                        log_cb("No empty MBR slot for new partition");
                        continue;
                    }
                };
                let offset = 446 + slot * 16;
                let start = *start_lba as u32;
                let sectors = (*size_bytes / 512) as u32;

                mbr_bytes[offset] = if *bootable { 0x80 } else { 0x00 };
                let (cyl, head, sec) = lba_to_chs(start, heads, spt);
                mbr_bytes[offset + 1] = head as u8;
                mbr_bytes[offset + 2] = (sec as u8 & 0x3F) | ((cyl >> 2) as u8 & 0xC0);
                mbr_bytes[offset + 3] = cyl as u8;
                mbr_bytes[offset + 4] = *partition_type;
                let end = start.saturating_add(sectors).saturating_sub(1);
                let (cyl, head, sec) = lba_to_chs(end, heads, spt);
                mbr_bytes[offset + 5] = head as u8;
                mbr_bytes[offset + 6] = (sec as u8 & 0x3F) | ((cyl >> 2) as u8 & 0xC0);
                mbr_bytes[offset + 7] = cyl as u8;
                mbr_bytes[offset + 8..offset + 12].copy_from_slice(&start.to_le_bytes());
                mbr_bytes[offset + 12..offset + 16].copy_from_slice(&sectors.to_le_bytes());

                log_cb(&format!(
                    "Added partition at slot {} (LBA {}, {} sectors, type 0x{:02X})",
                    slot, start_lba, sectors, partition_type
                ));
            }
        }
    }

    // Write back
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&mbr_bytes)?;
    file.flush()?;

    Ok(())
}

fn apply_gpt_edits(
    file: &mut (impl Read + Write + Seek),
    gpt: &Gpt,
    edits: &[PartitionTableEdit],
    disk_size_bytes: u64,
    log_cb: &mut impl FnMut(&str),
) -> Result<()> {
    let disk_sectors = disk_size_bytes / 512;
    let mut patched = gpt.clone();

    for edit in edits {
        match edit {
            PartitionTableEdit::ResizeEntry {
                index,
                new_size_bytes,
            } => {
                if let Some(e) = patched.entries.get_mut(*index) {
                    let new_sectors = new_size_bytes / 512;
                    e.last_lba = e.first_lba + new_sectors - 1;
                    log_cb(&format!(
                        "Resized GPT partition {} to {} sectors",
                        index, new_sectors
                    ));
                }
            }
            PartitionTableEdit::MoveEntry {
                index,
                new_start_lba,
            } => {
                if let Some(e) = patched.entries.get_mut(*index) {
                    let size = e.last_lba - e.first_lba + 1;
                    e.first_lba = *new_start_lba;
                    e.last_lba = new_start_lba + size - 1;
                    log_cb(&format!(
                        "Moved GPT partition {} to LBA {}",
                        index, new_start_lba
                    ));
                }
            }
            PartitionTableEdit::ChangeType {
                index,
                new_type_string,
                ..
            } => {
                if let Some(e) = patched.entries.get_mut(*index) {
                    if let Some(guid_str) = new_type_string {
                        match super::gpt::Guid::from_string(guid_str) {
                            Ok(guid) => {
                                e.type_guid = guid;
                                log_cb(&format!(
                                    "Changed GPT partition {} type to {}",
                                    index, guid_str
                                ));
                            }
                            Err(err) => {
                                log_cb(&format!("Invalid GUID {}: {}", guid_str, err));
                            }
                        }
                    }
                }
            }
            PartitionTableEdit::DeleteEntry { index } => {
                if *index < patched.entries.len() {
                    patched.entries.remove(*index);
                    log_cb(&format!("Deleted GPT partition {}", index));
                }
            }
            PartitionTableEdit::AddEntry {
                start_lba,
                size_bytes,
                type_string,
                ..
            } => {
                let type_guid = type_string
                    .as_ref()
                    .and_then(|s| super::gpt::Guid::from_string(s).ok())
                    .unwrap_or_else(|| {
                        super::gpt::Guid::from_string("EBD0A0A2-B9E5-4433-87C0-68B6B72699C7")
                            .unwrap()
                    });
                let end_lba = start_lba + size_bytes / 512 - 1;

                // Generate a simple unique GUID
                let mut unique = [0u8; 16];
                unique[0..8].copy_from_slice(&start_lba.to_le_bytes());
                unique[8..16].copy_from_slice(&end_lba.to_le_bytes());

                patched.entries.push(super::gpt::GptPartitionEntry {
                    type_guid,
                    unique_guid: super::gpt::Guid::from_bytes(unique),
                    first_lba: *start_lba,
                    last_lba: end_lba,
                    attributes: 0,
                    name: format!("Partition {}", patched.entries.len() + 1),
                });
                log_cb(&format!(
                    "Added GPT partition at LBA {}..{}",
                    start_lba, end_lba
                ));
            }
        }
    }

    // Write protective MBR
    let pmbr = Gpt::build_protective_mbr(disk_sectors);
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&pmbr)?;

    // Write primary GPT
    let primary = patched.build_primary_gpt(disk_sectors);
    file.seek(SeekFrom::Start(512))?;
    file.write_all(&primary)?;

    // Write backup GPT
    let backup = patched.build_backup_gpt(disk_sectors);
    let backup_offset = (disk_sectors - 33) * 512;
    file.seek(SeekFrom::Start(backup_offset))?;
    file.write_all(&backup)?;

    file.flush()?;
    log_cb("GPT partition table updated (primary + backup)");

    Ok(())
}

fn apply_apm_edits(
    file: &mut (impl Read + Write + Seek),
    apm: &Apm,
    edits: &[PartitionTableEdit],
    disk_size_bytes: u64,
    log_cb: &mut impl FnMut(&str),
) -> Result<()> {
    let block_size = apm.ddr.block_size as u64;
    let total_blocks = (disk_size_bytes / block_size) as u32;
    let mut patched = apm.clone();

    for edit in edits {
        match edit {
            PartitionTableEdit::ResizeEntry {
                index,
                new_size_bytes,
            } => {
                if let Some(e) = patched.entries.get_mut(*index) {
                    let new_blocks = (*new_size_bytes / block_size) as u32;
                    e.block_count = new_blocks;
                    e.data_count = new_blocks;
                    log_cb(&format!(
                        "Resized APM partition {} to {} blocks",
                        index, new_blocks
                    ));
                }
            }
            PartitionTableEdit::MoveEntry {
                index,
                new_start_lba,
            } => {
                if let Some(e) = patched.entries.get_mut(*index) {
                    // Convert LBA to APM block number
                    let new_block = (*new_start_lba * 512 / block_size) as u32;
                    e.start_block = new_block;
                    log_cb(&format!(
                        "Moved APM partition {} to block {}",
                        index, new_block
                    ));
                }
            }
            PartitionTableEdit::ChangeType {
                index,
                new_type_string,
                ..
            } => {
                if let Some(e) = patched.entries.get_mut(*index) {
                    if let Some(ts) = new_type_string {
                        e.partition_type = ts.clone();
                        log_cb(&format!("Changed APM partition {} type to {}", index, ts));
                    }
                }
            }
            PartitionTableEdit::DeleteEntry { index } => {
                if *index < patched.entries.len() {
                    patched.entries.remove(*index);
                    // Update map_entries count on remaining entries
                    let count = patched.entries.len() as u32;
                    for e in &mut patched.entries {
                        e.map_entries = count;
                    }
                    patched.map_entry_count = count;
                    log_cb(&format!("Deleted APM partition {}", index));
                }
            }
            PartitionTableEdit::AddEntry {
                start_lba,
                size_bytes,
                type_string,
                ..
            } => {
                let ts = type_string.as_deref().unwrap_or("Apple_HFS").to_string();
                let start_block = (*start_lba * 512 / block_size) as u32;
                let block_count = (*size_bytes / block_size) as u32;

                patched.entries.push(super::apm::ApmPartitionEntry {
                    signature: 0x504D,
                    map_entries: 0, // will be updated below
                    start_block,
                    block_count,
                    name: format!("Partition {}", patched.entries.len()),
                    partition_type: ts.clone(),
                    data_start: 0,
                    data_count: block_count,
                    status: 0x33,
                    boot_start: 0,
                    boot_size: 0,
                    boot_load: 0,
                    boot_entry: 0,
                    boot_checksum: 0,
                    processor: String::new(),
                });

                // Update map_entries count
                let count = patched.entries.len() as u32;
                for e in &mut patched.entries {
                    e.map_entries = count;
                }
                patched.map_entry_count = count;

                log_cb(&format!(
                    "Added APM partition at block {} ({} blocks, type {})",
                    start_block, block_count, ts
                ));
            }
        }
    }

    // Serialize and write
    let bytes = patched.build_apm_blocks(Some(total_blocks));
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&bytes)?;
    file.flush()?;
    log_cb("APM partition table updated");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::mbr::build_minimal_mbr;
    use std::io::Cursor;

    #[test]
    fn test_validate_overlapping_partitions() {
        let mbr_bytes = build_minimal_mbr(0x12345678, &[(0x0C, 2048, 1000, true)], 255, 63);
        let mbr = Mbr::parse(&mbr_bytes).unwrap();
        let table = PartitionTable::Mbr(mbr);

        // Add a partition that overlaps with the first
        let edits = vec![PartitionTableEdit::AddEntry {
            start_lba: 2500, // overlaps 2048..3048
            size_bytes: 1000 * 512,
            partition_type: 0x83,
            type_string: None,
            bootable: false,
        }];

        let result = validate_edits(&table, &edits, 100_000 * 512);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("overlap"));
    }

    #[test]
    fn test_validate_exceeds_disk() {
        let mbr_bytes = build_minimal_mbr(0x12345678, &[(0x0C, 2048, 1000, true)], 255, 63);
        let mbr = Mbr::parse(&mbr_bytes).unwrap();
        let table = PartitionTable::Mbr(mbr);

        let edits = vec![PartitionTableEdit::ResizeEntry {
            index: 0,
            new_size_bytes: 100_000 * 512, // way bigger than disk
        }];

        let result = validate_edits(&table, &edits, 10_000 * 512);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("beyond disk"));
    }

    #[test]
    fn test_validate_valid_edits() {
        let mbr_bytes = build_minimal_mbr(0x12345678, &[(0x0C, 2048, 1000, true)], 255, 63);
        let mbr = Mbr::parse(&mbr_bytes).unwrap();
        let table = PartitionTable::Mbr(mbr);

        let edits = vec![PartitionTableEdit::ResizeEntry {
            index: 0,
            new_size_bytes: 2000 * 512,
        }];

        let result = validate_edits(&table, &edits, 100_000 * 512);
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_mbr_resize() {
        let mbr_bytes = build_minimal_mbr(0x12345678, &[(0x0C, 2048, 1000, true)], 255, 63);
        let mut disk = vec![0u8; 100_000 * 512];
        disk[..512].copy_from_slice(&mbr_bytes);

        let mbr = Mbr::parse(&mbr_bytes).unwrap();
        let table = PartitionTable::Mbr(mbr);

        let edits = vec![PartitionTableEdit::ResizeEntry {
            index: 0,
            new_size_bytes: 2000 * 512,
        }];

        let mut cursor = Cursor::new(&mut disk[..]);
        apply_edits(&mut cursor, &table, &edits, 100_000 * 512, &mut |_| {}).unwrap();

        // Re-parse and verify
        let updated = Mbr::parse(disk[..512].try_into().unwrap()).unwrap();
        let non_empty: Vec<_> = updated.entries.iter().filter(|e| !e.is_empty()).collect();
        assert_eq!(non_empty.len(), 1);
        assert_eq!(non_empty[0].total_sectors, 2000);
        assert_eq!(non_empty[0].start_lba, 2048);
    }

    #[test]
    fn test_apply_mbr_delete() {
        let mbr_bytes = build_minimal_mbr(
            0x12345678,
            &[(0x0C, 2048, 1000, true), (0x83, 4096, 2000, false)],
            255,
            63,
        );
        let mut disk = vec![0u8; 100_000 * 512];
        disk[..512].copy_from_slice(&mbr_bytes);

        let mbr = Mbr::parse(&mbr_bytes).unwrap();
        let table = PartitionTable::Mbr(mbr);

        let edits = vec![PartitionTableEdit::DeleteEntry { index: 0 }];

        let mut cursor = Cursor::new(&mut disk[..]);
        apply_edits(&mut cursor, &table, &edits, 100_000 * 512, &mut |_| {}).unwrap();

        let updated = Mbr::parse(disk[..512].try_into().unwrap()).unwrap();
        let non_empty: Vec<_> = updated.entries.iter().filter(|e| !e.is_empty()).collect();
        assert_eq!(non_empty.len(), 1);
        assert_eq!(non_empty[0].partition_type, 0x83);
    }
}
