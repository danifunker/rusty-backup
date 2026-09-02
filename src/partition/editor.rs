use std::io::{Read, Seek, SeekFrom, Write};

use anyhow::{bail, Result};

use super::apm::Apm;
use super::gpt::Gpt;
use super::mbr::{lba_to_chs, Mbr};
use super::sgi::{
    SgiPartitionEntry, SgiPartitionType, SgiVolumeHeader, SGI_TYPE_BYTE_EFS, SGI_TYPE_BYTE_XFS,
};
use super::PartitionTable;
#[cfg(feature = "rust173-polyfill")]
use crate::rust173_compat::{IntIsMultipleOf as _, OptionIsNoneOr as _};

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
    /// Toggle the bootable flag on an existing partition. Only RDB tables
    /// honor this edit today; other tables either fold the flag into
    /// `AddEntry` (MBR) or don't carry a per-partition bootable bit (GPT,
    /// APM). On RDB it flips bit 0 of the PART block's `flags` field and
    /// recomputes the block checksum.
    SetBootable { index: usize, bootable: bool },
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
            PartitionTableEdit::SetBootable { index, .. } => {
                if !partitions.iter().any(|p| p.index == *index) {
                    bail!("partition index {} not found", index);
                }
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
                    start_byte: None,
                    size_bytes: *size_bytes,
                    bootable: false,
                    is_logical: false,
                    is_extended_container: false,
                    partition_type_string: None,
                    hfs_block_size: None,
                    rdb_part_block: None,
                    drv_name: None,
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
        PartitionTable::Rdb(rdb) => apply_rdb_edits(file, rdb, edits, log_cb),
        PartitionTable::Sgi(vh) => apply_sgi_edits(file, vh, edits, log_cb),
        PartitionTable::Ahdi(_) => {
            bail!("AHDI partition-table editing is not yet implemented")
        }
        PartitionTable::X68k { .. } => {
            bail!("X68000 partition-table editing is not yet implemented")
        }
        PartitionTable::Sun(_) => {
            bail!("Sun disk-label editing is not yet implemented (read / browse / back up only)")
        }
        PartitionTable::Next(label) => {
            apply_next_edits(file, label, edits, disk_size_bytes, log_cb)
        }
        PartitionTable::SolarisX86 { mbr, label } => {
            apply_solaris_x86_edits(file, mbr, label, edits, log_cb)
        }
        PartitionTable::SgiDkLabel(label) => {
            apply_sgi_dklabel_edits(file, label, edits, disk_size_bytes, log_cb)
        }
        PartitionTable::None { .. } => bail!("cannot edit partition table on a superfloppy"),
        PartitionTable::Dsd { .. } => {
            bail!("cannot edit partition table on a double-sided DFS (.dsd) image")
        }
    }
}

/// RDB tables only support `SetBootable` edits for now — full RDB editing
/// (resize / add / delete) is out of scope until we have a clean story for
/// AmigaDOS DosEnv geometry and the PFS/SFS block-size constraints. Any
/// other edit variant produces an error so the caller can surface it.
fn apply_rdb_edits(
    file: &mut (impl Read + Write + Seek),
    rdb: &super::rdb::Rdb,
    edits: &[PartitionTableEdit],
    log_cb: &mut impl FnMut(&str),
) -> Result<()> {
    for edit in edits {
        match edit {
            PartitionTableEdit::SetBootable { index, bootable } => {
                let part = rdb
                    .partitions
                    .get(*index)
                    .ok_or_else(|| anyhow::anyhow!("RDB partition index {} not found", index))?;
                let now = super::rdb::set_partition_bootable(file, part.block_num, *bootable)?;
                log_cb(&format!(
                    "Partition {} ({}): bootable -> {}",
                    index,
                    part.drv_name,
                    if now { "Yes" } else { "No" }
                ));
            }
            _ => bail!(
                "RDB partition table only supports toggling the bootable flag in this release"
            ),
        }
    }
    Ok(())
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
            PartitionTableEdit::SetBootable { .. } => {
                // MBR carries the bootable bit in the entry itself; the
                // editor folds it into `AddEntry` for new rows, but flipping
                // it on an existing row is not yet wired through this
                // apply path.
                log_cb("SetBootable: not yet supported on MBR partitions");
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
            PartitionTableEdit::SetBootable { .. } => {
                // GPT has no per-partition bootable bit. Boot ordering is
                // handled by the firmware (UEFI BootOrder) outside the
                // partition table, so this edit is a no-op here.
                log_cb("SetBootable: ignored on GPT (use firmware boot order)");
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
                    pad: Vec::new(),
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
            PartitionTableEdit::SetBootable { .. } => {
                // APM bootability is encoded in `status` plus the
                // pmBoot* metadata, not a simple flag we expose. Treat the
                // edit as a no-op until that surface is built out.
                log_cb("SetBootable: not yet supported on APM partitions");
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

/// Edit the eight `{d_base, d_size}` slots. `index` is the raw slot, matching
/// `PartitionInfo::index` and what `validate_edits` matches on.
/// NeXT disk label. Every offset here is in the label's own `d_secsize`
/// sectors measured from the end of the front porch, and every copy the disk
/// carries is rewritten — see the module header of
/// [`crate::partition::next`].
fn apply_next_edits(
    file: &mut (impl Read + Write + Seek),
    label: &crate::partition::next::NextDiskLabel,
    edits: &[PartitionTableEdit],
    disk_size_bytes: u64,
    log_cb: &mut impl FnMut(&str),
) -> Result<()> {
    use crate::partition::next::{
        clear_partition, present_copies, set_partition_extent, set_partition_type, write_copies,
        write_partition, NextDiskLabel, NextPartition, NextPartitionSpec, LABEL_SPAN, N_PARTITIONS,
    };

    let copies = present_copies(file);
    if copies.is_empty() {
        anyhow::bail!("NeXT disk label: no valid label copy to rewrite");
    }
    file.seek(SeekFrom::Start(label.label_offset))?;
    let mut buf = vec![0u8; LABEL_SPAN];
    file.read_exact(&mut buf)?;

    let secsize = u64::from(label.sector_size);
    let front = u64::from(label.front_porch);
    // Each edit reads its slot from the buffer as edited so far, so a resize
    // then a move of one slot compose and two adds take two slots.
    let live = |buf: &[u8]| -> Result<NextDiskLabel> {
        NextDiskLabel::parse(buf, 0)
            .map_err(|e| anyhow::anyhow!("NeXT disk label: edited label no longer parses: {e}"))
    };
    // Only slots the label actually lists may be edited; an unused slot is
    // reached with `add`, which picks the first free one itself.
    let resolve = |raw: usize| -> Result<usize> {
        if label.browsable_partitions().any(|(i, _)| i == raw) {
            Ok(raw)
        } else {
            anyhow::bail!(
                "NeXT disk label: slot {raw} ({}) is not a listed partition",
                NextPartition::letter(raw),
            )
        }
    };
    let to_sectors = |bytes: u64, what: &str| -> Result<i32> {
        if !bytes.is_multiple_of(secsize) {
            anyhow::bail!(
                "NeXT disk label: {what} of {bytes} bytes is not a whole number of the label's \
                 {secsize}-byte sectors"
            );
        }
        Ok((bytes / secsize) as i32)
    };
    let to_base = |start_lba: u64| -> Result<i32> {
        let byte = start_lba.saturating_mul(512);
        let sector = to_sectors(byte, "start")?;
        (sector as i64 - front as i64)
            .try_into()
            .ok()
            .filter(|_| byte >= front * secsize)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "NeXT disk label: LBA {start_lba} is inside the {}-byte front porch the label \
                     copies live in",
                    front * secsize,
                )
            })
    };

    for edit in edits {
        match edit {
            PartitionTableEdit::ResizeEntry {
                index,
                new_size_bytes,
            } => {
                let slot = resolve(*index)?;
                let size = to_sectors(*new_size_bytes, "size")?;
                let base = live(&buf)?.partitions[slot].base;
                set_partition_extent(&mut buf, slot, base, size);
                log_cb(&format!(
                    "NeXT disk label: slot {slot} ({}) resized to {size} sectors of {secsize}",
                    NextPartition::letter(slot),
                ));
            }
            PartitionTableEdit::MoveEntry {
                index,
                new_start_lba,
            } => {
                let slot = resolve(*index)?;
                let base = to_base(*new_start_lba)?;
                let size = live(&buf)?.partitions[slot].size;
                set_partition_extent(&mut buf, slot, base, size);
                log_cb(&format!(
                    "NeXT disk label: slot {slot} ({}) moved to p_base {base}",
                    NextPartition::letter(slot),
                ));
            }
            PartitionTableEdit::DeleteEntry { index } => {
                let slot = resolve(*index)?;
                clear_partition(&mut buf, slot);
                log_cb(&format!(
                    "NeXT disk label: slot {slot} ({}) cleared",
                    NextPartition::letter(slot),
                ));
                if buf[0xBC] == NextPartition::letter(slot) as u8 {
                    log_cb(
                        "NeXT disk label: warning - d_rootpartition still names that slot; \
                         point it at a live one with `partmap set-bootable`",
                    );
                }
            }
            PartitionTableEdit::AddEntry {
                start_lba,
                size_bytes,
                type_string,
                ..
            } => {
                let now = live(&buf)?;
                let free = (0..N_PARTITIONS)
                    .find(|i| now.partitions.get(*i).is_none_or(|p| p.is_empty()))
                    .ok_or_else(|| anyhow::anyhow!("NeXT disk label: all 8 slots are in use"))?;
                write_partition(
                    &mut buf,
                    free,
                    &NextPartitionSpec {
                        base: to_base(*start_lba)?,
                        size: to_sectors(*size_bytes, "size")?,
                        fs_type: type_string
                            .clone()
                            .filter(|t| !t.trim().is_empty())
                            .unwrap_or_else(|| "4.3BSD".to_string()),
                        ..Default::default()
                    },
                );
                log_cb(&format!(
                    "NeXT disk label: slot {free} ({}) added",
                    NextPartition::letter(free),
                ));
            }
            PartitionTableEdit::ChangeType {
                index,
                new_type_string,
                ..
            } => {
                let slot = resolve(*index)?;
                let text = new_type_string.as_deref().map(str::trim).ok_or_else(|| {
                    anyhow::anyhow!(
                        "NeXT disk label: p_type is an 8-byte name, not a number -- pass \
                         --type-string (try `partmap types --table next`)"
                    )
                })?;
                if text.len() > 7 {
                    anyhow::bail!("NeXT disk label: p_type holds 7 characters, '{text}' is longer");
                }
                set_partition_type(&mut buf, slot, text);
                log_cb(&format!("NeXT disk label: slot {slot} typed '{text}'"));
            }
            PartitionTableEdit::SetBootable { index, bootable } => {
                if !*bootable {
                    anyhow::bail!(
                        "NeXT disk label: d_rootpartition always names one slot, so a root slot \
                         can be moved but not cleared"
                    );
                }
                let slot = resolve(*index)?;
                buf[0xBC] = NextPartition::letter(slot) as u8;
                log_cb(&format!(
                    "NeXT disk label: d_rootpartition set to '{}'",
                    NextPartition::letter(slot),
                ));
            }
        }
    }

    check_next_layout(&buf, secsize, front, disk_size_bytes)?;
    // v1/v2 labels keep their checksum after dl_bad; stamping v3's offset
    // there left the real sum stale and every copy unreadable.
    NextDiskLabel::stamp_checksum(&mut buf, label.version);
    write_copies(file, &buf, &copies)?;
    log_cb(&format!(
        "NeXT disk label: rewrote {} copy/copies at block(s) {}",
        copies.len(),
        copies
            .iter()
            .map(|b| b.to_string())
            .collect::<Vec<_>>()
            .join(", "),
    ));
    Ok(())
}

/// Reject overlap or a partition running past the medium, reading the edited
/// buffer back through the parser so the check sees what a reader will.
fn check_next_layout(buf: &[u8], secsize: u64, front: u64, disk_size: u64) -> Result<()> {
    let label = crate::partition::next::NextDiskLabel::parse(buf, 0)
        .map_err(|e| anyhow::anyhow!("NeXT disk label: edited label no longer parses: {e}"))?;
    let live: Vec<_> = label.browsable_partitions().collect();
    for (i, p) in &live {
        let end = p.start_byte.saturating_add(p.size_bytes);
        if disk_size > 0 && end > disk_size {
            anyhow::bail!(
                "NeXT disk label: partition {} ends at byte {end}, past the {disk_size}-byte disk",
                crate::partition::next::NextPartition::letter(*i),
            );
        }
        if p.start_byte < front * secsize {
            anyhow::bail!(
                "NeXT disk label: partition {} starts inside the front porch",
                crate::partition::next::NextPartition::letter(*i),
            );
        }
        for (j, q) in &live {
            if j <= i {
                continue;
            }
            let qend = q.start_byte.saturating_add(q.size_bytes);
            if p.start_byte < qend && q.start_byte < end {
                anyhow::bail!(
                    "NeXT disk label: partitions {} and {} overlap",
                    crate::partition::next::NextPartition::letter(*i),
                    crate::partition::next::NextPartition::letter(*j),
                );
            }
        }
    }
    Ok(())
}

/// Solaris x86 VTOC. Slice offsets are sectors *relative to the Solaris MBR
/// partition*, so every edit is translated from the absolute LBA the caller
/// speaks and checked against that partition's extent.
fn apply_solaris_x86_edits(
    file: &mut (impl Read + Write + Seek),
    mbr: &Mbr,
    label: &crate::partition::solaris_x86::SolarisX86Label,
    edits: &[PartitionTableEdit],
    log_cb: &mut impl FnMut(&str),
) -> Result<()> {
    use crate::partition::solaris_x86::{
        data_area_sectors, get_slice, set_slice, stamp_checksum, write_label, N_SLICES, VTOC_SECTOR,
    };
    use crate::partition::sun::{tag_from_text, tag_name};

    let start = label.partition_start_lba;
    file.seek(SeekFrom::Start((start + VTOC_SECTOR) * 512))?;
    let mut sector = [0u8; 512];
    file.read_exact(&mut sector)?;

    // The label's own `dkl_ncyl` stops short of the alternate cylinders, so it
    // is the tighter bound; fall back to what the MBR entry says.
    let bound = data_area_sectors(&sector)
        .filter(|n| *n <= label.partition_sectors)
        .unwrap_or(label.partition_sectors);
    let resolve = |raw: usize| -> Result<usize> {
        if label.browsable_slices().any(|(i, _)| i == raw) {
            Ok(raw)
        } else if raw >= crate::partition::SOLARIS_MBR_INDEX_BASE {
            // Indexes past the slices are the disk's other MBR entries.
            anyhow::bail!(
                "Solaris x86 VTOC: partition {raw} is an MBR entry outside the Solaris \
                 partition, not a slice; only the VTOC slices are editable here"
            )
        } else {
            anyhow::bail!("Solaris x86 VTOC: slice {raw} is not a listed slice")
        }
    };
    let to_relative = |lba: u64| -> Result<u32> {
        lba.checked_sub(start)
            .and_then(|r| u32::try_from(r).ok())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Solaris x86 VTOC: LBA {lba} is outside the Solaris partition, which starts \
                     at LBA {start}"
                )
            })
    };
    let to_sectors = |bytes: u64| -> Result<u32> {
        u32::try_from(bytes / 512).map_err(|_| {
            anyhow::anyhow!(
                "Solaris x86 VTOC: {bytes} bytes is more slice than a \
                                          32-bit sector count can hold"
            )
        })
    };
    let tag_of = |string: &Option<String>, byte: u8| -> Result<u16> {
        if let Some(text) = string.as_deref().map(str::trim).filter(|t| !t.is_empty()) {
            return tag_from_text(text).ok_or_else(|| {
                anyhow::anyhow!(
                    "Solaris x86 VTOC: '{text}' is not a slice tag (try \
                     `partmap types --table solaris-x86`)"
                )
            });
        }
        Ok(u16::from(byte))
    };

    for edit in edits {
        match edit {
            PartitionTableEdit::ResizeEntry {
                index,
                new_size_bytes,
            } => {
                let slot = resolve(*index)?;
                let (tag, flag, rel, _) = get_slice(&sector, slot).unwrap_or_default();
                set_slice(
                    &mut sector,
                    slot,
                    tag,
                    flag,
                    rel,
                    to_sectors(*new_size_bytes)?,
                );
                log_cb(&format!("Solaris x86 VTOC: slice {slot} resized"));
            }
            PartitionTableEdit::MoveEntry {
                index,
                new_start_lba,
            } => {
                let slot = resolve(*index)?;
                let (tag, flag, _, size) = get_slice(&sector, slot).unwrap_or_default();
                let rel = to_relative(*new_start_lba)?;
                set_slice(&mut sector, slot, tag, flag, rel, size);
                log_cb(&format!(
                    "Solaris x86 VTOC: slice {slot} moved to partition-relative sector {rel}"
                ));
            }
            PartitionTableEdit::DeleteEntry { index } => {
                let slot = resolve(*index)?;
                set_slice(&mut sector, slot, 0, 0, 0, 0);
                log_cb(&format!("Solaris x86 VTOC: slice {slot} cleared"));
            }
            PartitionTableEdit::AddEntry {
                start_lba,
                size_bytes,
                partition_type,
                type_string,
                ..
            } => {
                // Slices 2, 8 and 9 are the label's own backup, boot and
                // alternates, so a new slice never lands in one.
                let free = (0..N_SLICES)
                    .filter(|i| !matches!(i, 2 | 8 | 9))
                    .find(|i| get_slice(&sector, *i).is_none_or(|(_, _, _, size)| size == 0))
                    .ok_or_else(|| anyhow::anyhow!("Solaris x86 VTOC: every slice is in use"))?;
                let tag = tag_of(type_string, *partition_type)?;
                set_slice(
                    &mut sector,
                    free,
                    tag,
                    0,
                    to_relative(*start_lba)?,
                    to_sectors(*size_bytes)?,
                );
                log_cb(&format!(
                    "Solaris x86 VTOC: slice {free} added as {}",
                    tag_name(tag),
                ));
            }
            PartitionTableEdit::ChangeType {
                index,
                new_type_byte,
                new_type_string,
            } => {
                let slot = resolve(*index)?;
                let (_, flag, rel, size) = get_slice(&sector, slot).unwrap_or_default();
                let tag = tag_of(new_type_string, *new_type_byte)?;
                set_slice(&mut sector, slot, tag, flag, rel, size);
                log_cb(&format!(
                    "Solaris x86 VTOC: slice {slot} tagged {}",
                    tag_name(tag),
                ));
            }
            PartitionTableEdit::SetBootable { .. } => {
                anyhow::bail!(
                    "Solaris x86 VTOC: a slice has no boot flag -- the bootable bit lives on the \
                     MBR entry that hosts the label, not on the slices inside it"
                );
            }
        }
    }

    check_solaris_layout(&sector, bound)?;
    if mbr.entries.get(label.mbr_slot).is_none_or(|e| e.is_empty()) {
        anyhow::bail!("Solaris x86 VTOC: the MBR entry hosting the label has gone");
    }
    stamp_checksum(&mut sector);
    write_label(file, start, &sector)?;
    log_cb(&format!(
        "Solaris x86 VTOC: rewrote the label in sector {} of the Solaris partition",
        VTOC_SECTOR,
    ));
    Ok(())
}

/// Reject a slice past the label's data area, or partially overlapping
/// another. Containment is allowed: the backup alias wraps every real slice,
/// which is how the label spells "the whole partition".
fn check_solaris_layout(sector: &[u8; 512], bound: u64) -> Result<()> {
    use crate::partition::solaris_x86::{get_slice, N_SLICES};

    let live: Vec<(usize, u64, u64)> = (0..N_SLICES)
        .filter_map(|i| get_slice(sector, i).map(|s| (i, s)))
        .filter(|(_, (_, _, _, size))| *size > 0)
        .map(|(i, (_, _, start, size))| (i, u64::from(start), u64::from(start) + u64::from(size)))
        .collect();
    for (i, a0, a1) in &live {
        if *a1 > bound {
            anyhow::bail!(
                "Solaris x86 VTOC: slice {i} ends at partition-relative sector {a1}, past the \
                 {bound}-sector data area"
            );
        }
        for (j, b0, b1) in &live {
            if j <= i {
                continue;
            }
            let contains = (a0 <= b0 && a1 >= b1) || (b0 <= a0 && b1 >= a1);
            if !contains && a0 < b1 && b0 < a1 {
                anyhow::bail!(
                    "Solaris x86 VTOC: slices {i} [{a0}..{a1}) and {j} [{b0}..{b1}) overlap"
                );
            }
        }
    }
    Ok(())
}

fn apply_sgi_dklabel_edits(
    file: &mut (impl Read + Write + Seek),
    label: &crate::partition::sgi_dklabel::SgiDiskLabel,
    edits: &[PartitionTableEdit],
    disk_size_bytes: u64,
    log_cb: &mut impl FnMut(&str),
) -> Result<()> {
    use crate::partition::sgi_dklabel::{apply_byte_order, OFF_BOOTFS, OFF_MAP, SGI_DKLABEL_NFS};
    use byteorder::{BigEndian, ByteOrder};

    file.seek(SeekFrom::Start(0))?;
    let mut sector = [0u8; 512];
    file.read_exact(&mut sector)?;
    apply_byte_order(label.byte_order, &mut sector);

    let mut map = label.map.clone();
    map.resize(
        SGI_DKLABEL_NFS,
        crate::partition::sgi_dklabel::SgiDiskMap { base: 0, size: 0 },
    );
    let mut bootfs = sector[OFF_BOOTFS];
    let total_blocks = (disk_size_bytes / 512) as u32;
    // Only slots the table actually lists may be edited: an empty or
    // whole-disk wrapper slot is not a partition the caller can have meant.
    let resolve = |raw: usize| -> Result<usize> {
        if label.browsable_slots().any(|(i, _)| i == raw) {
            Ok(raw)
        } else {
            anyhow::bail!(
                "SGI disk label: slot {raw} is not a listed partition (listed: {})",
                label
                    .browsable_slots()
                    .map(|(i, _)| i.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
    };

    for edit in edits {
        match edit {
            PartitionTableEdit::ResizeEntry {
                index,
                new_size_bytes,
            } => {
                let raw = resolve(*index)?;
                let slot = slot_mut(&mut map, raw)?;
                slot.size = (*new_size_bytes / 512) as u32;
                log_cb(&format!(
                    "SGI disk label: slot {raw} resized to {} blocks",
                    slot.size
                ));
            }
            PartitionTableEdit::MoveEntry {
                index,
                new_start_lba,
            } => {
                let raw = resolve(*index)?;
                let slot = slot_mut(&mut map, raw)?;
                slot.base = *new_start_lba as u32;
                log_cb(&format!(
                    "SGI disk label: slot {raw} moved to block {}",
                    slot.base
                ));
            }
            PartitionTableEdit::DeleteEntry { index } => {
                let raw = resolve(*index)?;
                let slot = slot_mut(&mut map, raw)?;
                slot.base = 0;
                slot.size = 0;
                log_cb(&format!("SGI disk label: slot {raw} cleared"));
            }
            PartitionTableEdit::AddEntry {
                start_lba,
                size_bytes,
                ..
            } => {
                let free = map
                    .iter()
                    .position(|m| m.size == 0)
                    .ok_or_else(|| anyhow::anyhow!("SGI disk label: all 8 slots are in use"))?;
                map[free].base = *start_lba as u32;
                map[free].size = (*size_bytes / 512) as u32;
                log_cb(&format!(
                    "SGI disk label: slot {free} added at block {} for {} blocks",
                    map[free].base, map[free].size
                ));
            }
            PartitionTableEdit::SetBootable { index, bootable } => {
                if !*bootable {
                    anyhow::bail!(
                        "SGI disk label: d_bootfs always names one slot, so a boot slot can be \
                         moved but not cleared"
                    );
                }
                let raw = resolve(*index)?;
                bootfs = raw as u8;
                log_cb(&format!("SGI disk label: d_bootfs set to slot {raw}"));
            }
            PartitionTableEdit::ChangeType { .. } => {
                anyhow::bail!(
                    "SGI disk label: slots have no type field — a slot's role comes from \
                     d_bootfs / d_swapfs / d_rootfs, not from a per-slot type"
                );
            }
        }
    }

    check_dklabel_layout(&map, total_blocks)?;

    for (i, m) in map.iter().enumerate() {
        let o = OFF_MAP + i * 8;
        BigEndian::write_u32(&mut sector[o..o + 4], m.base);
        BigEndian::write_u32(&mut sector[o + 4..o + 8], m.size);
    }
    sector[OFF_BOOTFS] = bootfs;
    // `d_rootnotboot` / `d_rootfs` are left exactly as found, so a disk that
    // boots from its root keeps doing so.

    apply_byte_order(label.byte_order, &mut sector);
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&sector)?;
    file.flush()?;
    Ok(())
}

fn slot_mut(
    map: &mut [crate::partition::sgi_dklabel::SgiDiskMap],
    index: usize,
) -> Result<&mut crate::partition::sgi_dklabel::SgiDiskMap> {
    map.get_mut(index)
        .ok_or_else(|| anyhow::anyhow!("SGI disk label: slot {index} out of range (0..7)"))
}

/// Reject partial overlap or a slot past the disk. Containment is allowed: a
/// slot wrapping another is how the label spells "the whole drive".
fn check_dklabel_layout(
    map: &[crate::partition::sgi_dklabel::SgiDiskMap],
    total_blocks: u32,
) -> Result<()> {
    for (i, m) in map.iter().enumerate() {
        if m.size == 0 {
            continue;
        }
        let end = m.base as u64 + m.size as u64;
        if total_blocks > 0 && end > total_blocks as u64 {
            anyhow::bail!(
                "SGI disk label: slot {i} runs to block {end}, past the {total_blocks}-block disk"
            );
        }
        for (j, n) in map.iter().enumerate().skip(i + 1) {
            if n.size == 0 {
                continue;
            }
            let (a0, a1) = (m.base as u64, end);
            let (b0, b1) = (n.base as u64, n.base as u64 + n.size as u64);
            let contains = (a0 <= b0 && a1 >= b1) || (b0 <= a0 && b1 >= a1);
            if !contains && a0 < b1 && b0 < a1 {
                anyhow::bail!(
                    "SGI disk label: slots {i} [{a0}..{a1}) and {j} [{b0}..{b1}) overlap"
                );
            }
        }
    }
    Ok(())
}

/// Map an [`PartitionTableEdit::AddEntry`]/[`PartitionTableEdit::ChangeType`]
/// type byte/string into the SGI partition-type discriminant. The string form
/// is shared with `new hd sgi` via `provision::sgi_type_from_text`, which also
/// takes a raw decimal / hex discriminant; the byte form accepts the synthetic
/// 0xA0 / 0xA1 `PartitionTable::partitions` hands out.
fn parse_sgi_type(byte: u8, type_string: Option<&str>) -> SgiPartitionType {
    if let Some(raw) = type_string.and_then(crate::partition::provision::sgi_type_from_text) {
        return SgiPartitionType::from_raw(raw);
    }
    match byte {
        SGI_TYPE_BYTE_XFS => SgiPartitionType::Xfs,
        SGI_TYPE_BYTE_EFS => SgiPartitionType::Efs,
        _ => SgiPartitionType::Unknown(byte as u32),
    }
}

fn apply_sgi_edits(
    file: &mut (impl Read + Write + Seek),
    vh: &SgiVolumeHeader,
    edits: &[PartitionTableEdit],
    log_cb: &mut impl FnMut(&str),
) -> Result<()> {
    use crate::partition::sgi::SGI_NUM_PARTITIONS;

    // Work on a clone so we can roll the in-memory state forward, then
    // serialize the whole sector once at the end (checksum is recomputed by
    // `SgiVolumeHeader::to_bytes`).
    let mut patched = vh.clone();
    // Ensure the partitions vec has exactly SGI_NUM_PARTITIONS slots so
    // index-based edits don't go out of range on disks whose parser
    // truncated trailing empty entries.
    while patched.partitions.len() < SGI_NUM_PARTITIONS {
        patched.partitions.push(SgiPartitionEntry {
            blocks: 0,
            first: 0,
            partition_type_raw: 0,
        });
    }

    for edit in edits {
        match edit {
            PartitionTableEdit::ResizeEntry {
                index,
                new_size_bytes,
            } => {
                let entry = patched.partitions.get_mut(*index).ok_or_else(|| {
                    anyhow::anyhow!("SGI partition slot {} out of range (max 15)", index)
                })?;
                let new_blocks = (*new_size_bytes / 512) as u32;
                entry.blocks = new_blocks;
                log_cb(&format!(
                    "Resized SGI partition slot {} to {} sectors",
                    index, new_blocks
                ));
            }
            PartitionTableEdit::MoveEntry {
                index,
                new_start_lba,
            } => {
                let entry = patched.partitions.get_mut(*index).ok_or_else(|| {
                    anyhow::anyhow!("SGI partition slot {} out of range (max 15)", index)
                })?;
                entry.first = (*new_start_lba) as u32;
                log_cb(&format!(
                    "Moved SGI partition slot {} to LBA {}",
                    index, new_start_lba
                ));
            }
            PartitionTableEdit::ChangeType {
                index,
                new_type_byte,
                new_type_string,
            } => {
                let entry = patched.partitions.get_mut(*index).ok_or_else(|| {
                    anyhow::anyhow!("SGI partition slot {} out of range (max 15)", index)
                })?;
                let new_type = parse_sgi_type(*new_type_byte, new_type_string.as_deref()).as_u32();
                entry.partition_type_raw = new_type;
                log_cb(&format!(
                    "Changed SGI partition slot {} type to {}",
                    index,
                    SgiPartitionType::from_raw(new_type).display_name(),
                ));
            }
            PartitionTableEdit::DeleteEntry { index } => {
                // SGI uses fixed 16 slots — clear in place rather than
                // removing, so subsequent edits' indices stay stable.
                let entry = patched.partitions.get_mut(*index).ok_or_else(|| {
                    anyhow::anyhow!("SGI partition slot {} out of range (max 15)", index)
                })?;
                entry.blocks = 0;
                entry.first = 0;
                entry.partition_type_raw = 0;
                log_cb(&format!("Cleared SGI partition slot {}", index));
            }
            PartitionTableEdit::AddEntry {
                start_lba,
                size_bytes,
                partition_type,
                type_string,
                ..
            } => {
                // SGI has 16 fixed slots. Find the first empty one — but
                // refuse to touch slots 8 (VOLUME) and 10 (VOLHDR) since
                // those are reserved disk-wide wrappers. (They're filtered
                // out of the editor's view by .partitions(), so the user
                // won't see them, but defensive check anyway.)
                let new_type = parse_sgi_type(*partition_type, type_string.as_deref());
                let slot = patched
                    .partitions
                    .iter()
                    .enumerate()
                    .find(|(i, e)| e.is_empty() && *i != 8 && *i != 10)
                    .map(|(i, _)| i)
                    .ok_or_else(|| {
                        anyhow::anyhow!("no empty SGI partition slot available (all 16 used)")
                    })?;
                patched.partitions[slot] = SgiPartitionEntry {
                    blocks: (*size_bytes / 512) as u32,
                    first: (*start_lba) as u32,
                    partition_type_raw: new_type.as_u32(),
                };
                log_cb(&format!(
                    "Added SGI partition in slot {} ({}, {} sectors at LBA {})",
                    slot,
                    new_type.display_name(),
                    size_bytes / 512,
                    start_lba,
                ));
            }
            PartitionTableEdit::SetBootable { .. } => {
                // SGI has no per-entry bootable flag. The bootable selection
                // lives in the volume header's `root_part_num` field, which
                // we're not yet exposing through the editor.
                log_cb("SetBootable: ignored on SGI (root partition is set in volume header)");
            }
        }
    }

    // Serialize the patched header (recomputes the sector checksum) and
    // write it back to sector 0.
    let sector = patched.to_bytes();
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&sector)?;
    file.flush()?;
    log_cb("SGI volume header updated (sector 0)");

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

    // --- SGI disklabel writer tests (Phase 3 of disk_expansion.md) ---

    fn mk_sgi_table_with_one_xfs() -> (PartitionTable, Vec<u8>) {
        use crate::partition::sgi::{
            SgiPartitionEntry, SgiVolumeDirEntry, SgiVolumeHeader, SGI_NUM_PARTITIONS,
            SGI_NUM_VOL_DIR, SGI_VOLHDR_MAGIC,
        };
        // One Xfs partition at slot 0 starting at LBA 4096 with 1000 sectors.
        let mut vh = SgiVolumeHeader {
            magic: SGI_VOLHDR_MAGIC,
            root_part_num: 0,
            swap_part_num: 1,
            device_parameters: crate::partition::sgi::SgiDeviceParameters::for_geometry(
                403, 16, 63,
            ),
            bootfile: "/unix".to_string(),
            volume_directory: Vec::new(),
            partitions: Vec::new(),
            checksum: 0,
            checksum_valid: true,
        };
        while vh.volume_directory.len() < SGI_NUM_VOL_DIR {
            vh.volume_directory.push(SgiVolumeDirEntry {
                name: String::new(),
                block_num: 0,
                bytes: 0,
            });
        }
        vh.partitions.push(SgiPartitionEntry {
            blocks: 1000,
            first: 4096,
            partition_type_raw: SgiPartitionType::Xfs.as_u32(),
        });
        while vh.partitions.len() < SGI_NUM_PARTITIONS {
            vh.partitions.push(SgiPartitionEntry {
                blocks: 0,
                first: 0,
                partition_type_raw: 0,
            });
        }

        let mut disk = vec![0u8; 100_000 * 512];
        disk[..512].copy_from_slice(&vh.to_bytes());

        (PartitionTable::Sgi(vh), disk)
    }

    #[test]
    fn sgi_resize_grows_blocks_and_recomputes_checksum() {
        let (table, mut disk) = mk_sgi_table_with_one_xfs();
        let edits = vec![PartitionTableEdit::ResizeEntry {
            index: 0,
            new_size_bytes: 2000 * 512,
        }];
        let mut cursor = Cursor::new(&mut disk[..]);
        apply_edits(&mut cursor, &table, &edits, 100_000 * 512, &mut |_| {}).unwrap();

        let updated = SgiVolumeHeader::parse(&disk[..512]).unwrap();
        assert_eq!(updated.partitions[0].blocks, 2000);
        assert_eq!(updated.partitions[0].first, 4096);
        assert_eq!(
            updated.partitions[0].partition_type(),
            SgiPartitionType::Xfs
        );
        // Checksum must round-trip — parse() rejects bad headers, so reaching
        // here means the new sum-to-zero invariant holds.
        assert!(updated.checksum_valid);
    }

    #[test]
    fn sgi_add_entry_fills_first_free_slot() {
        let (table, mut disk) = mk_sgi_table_with_one_xfs();
        let edits = vec![PartitionTableEdit::AddEntry {
            start_lba: 10_000,
            size_bytes: 500 * 512,
            partition_type: SGI_TYPE_BYTE_EFS,
            type_string: None,
            bootable: false,
        }];
        let mut cursor = Cursor::new(&mut disk[..]);
        apply_edits(&mut cursor, &table, &edits, 100_000 * 512, &mut |_| {}).unwrap();

        let updated = SgiVolumeHeader::parse(&disk[..512]).unwrap();
        // Slot 0 still has the original XFS; the new EFS goes into slot 1
        // (slot 8/10 are reserved for volume wrappers but stay empty here
        // so the next empty slot is 1).
        assert_eq!(
            updated.partitions[0].partition_type(),
            SgiPartitionType::Xfs
        );
        assert_eq!(
            updated.partitions[1].partition_type(),
            SgiPartitionType::Efs
        );
        assert_eq!(updated.partitions[1].first, 10_000);
        assert_eq!(updated.partitions[1].blocks, 500);
    }

    #[test]
    fn sgi_delete_clears_slot_in_place() {
        let (table, mut disk) = mk_sgi_table_with_one_xfs();
        let edits = vec![PartitionTableEdit::DeleteEntry { index: 0 }];
        let mut cursor = Cursor::new(&mut disk[..]);
        apply_edits(&mut cursor, &table, &edits, 100_000 * 512, &mut |_| {}).unwrap();

        let updated = SgiVolumeHeader::parse(&disk[..512]).unwrap();
        assert!(updated.partitions[0].is_empty());
    }

    #[test]
    fn sgi_change_type_accepts_string_or_byte() {
        let (table, mut disk) = mk_sgi_table_with_one_xfs();
        let edits = vec![PartitionTableEdit::ChangeType {
            index: 0,
            new_type_byte: 0,
            new_type_string: Some("EFS".into()),
        }];
        let mut cursor = Cursor::new(&mut disk[..]);
        apply_edits(&mut cursor, &table, &edits, 100_000 * 512, &mut |_| {}).unwrap();

        let updated = SgiVolumeHeader::parse(&disk[..512]).unwrap();
        assert_eq!(
            updated.partitions[0].partition_type(),
            SgiPartitionType::Efs
        );
    }

    // ---- SGI disk label ---------------------------------------------------

    /// A minimal but valid label: geometry that parses, one slot at block 119.
    fn dklabel_disk(swabbed: bool) -> Vec<u8> {
        use crate::partition::sgi_dklabel::{swab16_in_place, SGI_DKLABEL_MAGIC};
        use byteorder::{BigEndian, ByteOrder};
        let mut img = vec![0u8; 4 * 1024 * 1024];
        BigEndian::write_u32(&mut img[0x00..0x04], SGI_DKLABEL_MAGIC);
        BigEndian::write_u16(&mut img[0x08..0x0A], 100); // cylinders
        BigEndian::write_u16(&mut img[0x0A..0x0C], 4); // heads
        BigEndian::write_u16(&mut img[0x0C..0x0E], 20); // sectors
                                                        // slot 0: 119 .. 119+1000
        BigEndian::write_u32(&mut img[0x16..0x1A], 119);
        BigEndian::write_u32(&mut img[0x1A..0x1E], 1000);
        if swabbed {
            let mut head = img[..512].to_vec();
            swab16_in_place(&mut head);
            img[..512].copy_from_slice(&head);
        }
        img
    }

    fn dklabel_of(img: &[u8]) -> crate::partition::sgi_dklabel::SgiDiskLabel {
        crate::partition::sgi_dklabel::SgiDiskLabel::parse(&img[..512]).unwrap()
    }

    fn edit_dklabel(img: &mut Vec<u8>, edits: &[PartitionTableEdit]) -> Result<()> {
        let label = dklabel_of(img);
        let total = img.len() as u64;
        let mut cur = Cursor::new(std::mem::take(img));
        let r = apply_sgi_dklabel_edits(&mut cur, &label, edits, total, &mut |_| {});
        *img = cur.into_inner();
        r
    }

    /// Build a disk with one of the two new tables on it, through the same
    /// writer `rb-cli new hd` uses, so the tests edit what a user would have.
    fn provisioned_disk(kind: crate::partition::type_catalog::TableKind, disk: u64) -> Vec<u8> {
        use crate::partition::provision::{place, Geometry, PartSpec};

        let geometry = Geometry {
            heads: 16,
            sectors_per_track: 63,
        };
        let align = crate::partition::provision::default_align(kind, geometry);
        let specs = vec![PartSpec {
            size: Some(disk / 4),
            type_text: None,
            name: None,
        }];
        let placed = place(&specs, kind, disk, align, geometry).unwrap();
        let mut cur = Cursor::new(vec![0u8; disk as usize]);
        crate::partition::provision::write_table(&mut cur, kind, &placed, disk, geometry).unwrap();
        cur.into_inner()
    }

    fn next_disk() -> Vec<u8> {
        provisioned_disk(
            crate::partition::type_catalog::TableKind::Next,
            64 * 1024 * 1024,
        )
    }

    fn next_label_of(img: &[u8]) -> crate::partition::next::NextDiskLabel {
        let mut cur = Cursor::new(img.to_vec());
        crate::partition::next::detect(&mut cur).expect("a NeXT label")
    }

    fn edit_next(img: &mut Vec<u8>, edits: &[PartitionTableEdit]) -> Result<()> {
        let label = next_label_of(img);
        let total = img.len() as u64;
        let mut cur = Cursor::new(std::mem::take(img));
        let r = apply_next_edits(&mut cur, &label, edits, total, &mut |_| {});
        *img = cur.into_inner();
        r
    }

    /// `p_base` is in 1024-byte sectors past the front porch. An editor that
    /// took the LBA at face value would put the partition at half its offset.
    #[test]
    fn next_move_converts_the_lba_into_porch_relative_sectors() {
        let mut img = next_disk();
        let lba = 8 * 1024 * 1024 / 512;
        edit_next(
            &mut img,
            &[PartitionTableEdit::MoveEntry {
                index: 0,
                new_start_lba: lba,
            }],
        )
        .unwrap();
        let label = next_label_of(&img);
        let (_, p) = label.browsable_partitions().next().unwrap();
        assert_eq!(p.base, (8 * 1024 * 1024 / 1024) - 160);
        assert_eq!(p.start_byte, 8 * 1024 * 1024);
    }

    #[test]
    fn next_add_fills_the_first_free_slot_and_types_it() {
        let mut img = next_disk();
        edit_next(
            &mut img,
            &[PartitionTableEdit::AddEntry {
                start_lba: 32 * 1024 * 1024 / 512,
                size_bytes: 4 * 1024 * 1024,
                partition_type: 0,
                type_string: Some("swap".to_string()),
                bootable: false,
            }],
        )
        .unwrap();
        let label = next_label_of(&img);
        let live: Vec<_> = label.browsable_partitions().collect();
        assert_eq!(live.len(), 2);
        assert_eq!(live[1].0, 1, "slot b, the first free one");
        assert_eq!(live[1].1.fs_type, "swap");
    }

    /// The resize engine emits Resize then Move for one slot; the move used to
    /// read the slot's size from the label parsed before the resize.
    #[test]
    fn next_resize_then_move_keeps_the_new_size() {
        let mut img = next_disk();
        let new_size = 6 * 1024 * 1024;
        edit_next(
            &mut img,
            &[
                PartitionTableEdit::ResizeEntry {
                    index: 0,
                    new_size_bytes: new_size,
                },
                PartitionTableEdit::MoveEntry {
                    index: 0,
                    new_start_lba: 8 * 1024 * 1024 / 512,
                },
            ],
        )
        .unwrap();
        let label = next_label_of(&img);
        let (_, p) = label.browsable_partitions().next().unwrap();
        assert_eq!(p.size as u64 * 1024, new_size);
        assert_eq!(p.start_byte, 8 * 1024 * 1024);
    }

    #[test]
    fn next_two_adds_take_two_slots() {
        let mut img = next_disk();
        let add = |mb: u64| PartitionTableEdit::AddEntry {
            start_lba: mb * 1024 * 1024 / 512,
            size_bytes: 4 * 1024 * 1024,
            partition_type: 0,
            type_string: Some("swap".to_string()),
            bootable: false,
        };
        edit_next(&mut img, &[add(32), add(40)]).unwrap();
        let label = next_label_of(&img);
        let live: Vec<_> = label.browsable_partitions().collect();
        assert_eq!(
            live.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
    }

    /// A v2 label keeps its checksum after `dl_bad`; stamping v3's offset left
    /// the real one stale, and every copy stopped validating.
    #[test]
    fn next_v2_label_is_still_a_label_after_an_edit() {
        use crate::partition::next::{NextDiskLabel, LABEL_SPAN, NEXT_LABEL_V2};
        let mut img = next_disk();
        for block in [0u64, 15, 30, 45] {
            let off = (block * 512) as usize;
            let copy = &mut img[off..off + LABEL_SPAN];
            copy[0..4].copy_from_slice(&NEXT_LABEL_V2.to_be_bytes());
            NextDiskLabel::stamp_checksum(copy, NEXT_LABEL_V2);
        }
        assert_eq!(next_label_of(&img).version, NEXT_LABEL_V2);
        edit_next(
            &mut img,
            &[PartitionTableEdit::ResizeEntry {
                index: 0,
                new_size_bytes: 6 * 1024 * 1024,
            }],
        )
        .unwrap();
        let label = next_label_of(&img);
        assert_eq!(label.version, NEXT_LABEL_V2);
        assert_eq!(
            label.browsable_partitions().next().unwrap().1.size as u64 * 1024,
            6 * 1024 * 1024
        );
    }

    /// Every copy the disk carries has to be rewritten, and only those: a
    /// NeXTSTEP/Intel disk has no copy at block 0.
    #[test]
    fn next_edits_rewrite_only_the_copies_that_exist() {
        use crate::partition::next::LABEL_SPAN;

        let mut img = next_disk();
        // Stamp a PC boot sector over the copy at block 0, as NeXTSTEP/Intel does.
        for b in img[..LABEL_SPAN].iter_mut() {
            *b = 0;
        }
        img[510] = 0x55;
        img[511] = 0xAA;
        edit_next(
            &mut img,
            &[PartitionTableEdit::ResizeEntry {
                index: 0,
                new_size_bytes: 8 * 1024 * 1024,
            }],
        )
        .unwrap();
        assert_eq!(&img[510..512], &[0x55, 0xAA], "block 0 was overwritten");
        for block in [15u64, 30, 45] {
            let at = (block * 512) as usize;
            let label =
                crate::partition::next::NextDiskLabel::parse(&img[at..at + LABEL_SPAN], 0).unwrap();
            let (_, p) = label.browsable_partitions().next().unwrap();
            assert_eq!(p.size_bytes, 8 * 1024 * 1024, "copy at block {block}");
        }
    }

    #[test]
    fn next_refuses_a_size_that_is_not_whole_label_sectors() {
        let mut img = next_disk();
        let err = edit_next(
            &mut img,
            &[PartitionTableEdit::ResizeEntry {
                index: 0,
                new_size_bytes: 1536,
            }],
        )
        .expect_err("1.5 KiB is not a whole number of 1024-byte sectors");
        assert!(format!("{err:#}").contains("1024-byte sectors"), "{err:#}");
    }

    #[test]
    fn next_set_type_needs_a_string_and_refuses_clearing_the_root() {
        let mut img = next_disk();
        let err = edit_next(
            &mut img,
            &[PartitionTableEdit::ChangeType {
                index: 0,
                new_type_byte: 0x83,
                new_type_string: None,
            }],
        )
        .expect_err("p_type is a name, not a byte");
        assert!(format!("{err:#}").contains("--type-string"), "{err:#}");

        let err = edit_next(
            &mut img,
            &[PartitionTableEdit::SetBootable {
                index: 0,
                bootable: false,
            }],
        )
        .expect_err("d_rootpartition always names a slot");
        assert!(format!("{err:#}").contains("d_rootpartition"), "{err:#}");
    }

    #[test]
    fn next_overlap_is_refused() {
        let mut img = next_disk();
        let err = edit_next(
            &mut img,
            &[PartitionTableEdit::AddEntry {
                start_lba: 2 * 1024 * 1024 / 512,
                size_bytes: 8 * 1024 * 1024,
                partition_type: 0,
                type_string: None,
                bootable: false,
            }],
        )
        .expect_err("lands on top of partition a");
        assert!(format!("{err:#}").contains("overlap"), "{err:#}");
    }

    fn solaris_disk() -> Vec<u8> {
        provisioned_disk(
            crate::partition::type_catalog::TableKind::SolarisX86,
            256 * 1024 * 1024,
        )
    }

    fn solaris_parts(img: &[u8]) -> crate::partition::PartitionTable {
        let mut cur = Cursor::new(img.to_vec());
        crate::partition::PartitionTable::detect(&mut cur).expect("a Solaris x86 disk")
    }

    fn edit_solaris(img: &mut Vec<u8>, edits: &[PartitionTableEdit]) -> Result<()> {
        let table = solaris_parts(img);
        let crate::partition::PartitionTable::SolarisX86 { mbr, label } = &table else {
            panic!("not a Solaris x86 disk");
        };
        let mut cur = Cursor::new(std::mem::take(img));
        let r = apply_solaris_x86_edits(&mut cur, mbr, label, edits, &mut |_| {});
        *img = cur.into_inner();
        r
    }

    /// The label sector is a whole `struct dk_label`, so an edit that does not
    /// re-stamp the checksum leaves something Solaris itself would refuse.
    #[test]
    fn solaris_resize_keeps_the_tag_and_restamps_the_checksum() {
        use byteorder::{ByteOrder, LittleEndian};

        let mut img = solaris_disk();
        let before = solaris_parts(&img);
        let crate::partition::PartitionTable::SolarisX86 { label, .. } = &before else {
            unreachable!()
        };
        let (start, tag) = (label.partition_start_lba, label.slices[0].tag);

        edit_solaris(
            &mut img,
            &[PartitionTableEdit::ResizeEntry {
                index: 0,
                new_size_bytes: 16 * 1024 * 1024,
            }],
        )
        .unwrap();

        let at = ((start + 1) * 512) as usize;
        let sector = &img[at..at + 512];
        let mut x = 0u16;
        for w in sector.chunks_exact(2) {
            x ^= LittleEndian::read_u16(w);
        }
        assert_eq!(x, 0, "dkl_cksum no longer closes the sector");
        let after = solaris_parts(&img);
        let crate::partition::PartitionTable::SolarisX86 { label, .. } = &after else {
            unreachable!()
        };
        assert_eq!(label.slices[0].tag, tag, "the tag survived the resize");
        assert_eq!(label.slices[0].size_bytes(), 16 * 1024 * 1024);
    }

    #[test]
    fn solaris_refuses_set_bootable_and_a_slice_past_the_data_area() {
        let mut img = solaris_disk();
        let err = edit_solaris(
            &mut img,
            &[PartitionTableEdit::SetBootable {
                index: 0,
                bootable: true,
            }],
        )
        .expect_err("slices carry no boot flag");
        assert!(format!("{err:#}").contains("MBR entry"), "{err:#}");

        let err = edit_solaris(
            &mut img,
            &[PartitionTableEdit::ResizeEntry {
                index: 0,
                new_size_bytes: 1024 * 1024 * 1024,
            }],
        )
        .expect_err("1 GiB does not fit a 256 MiB disk");
        assert!(format!("{err:#}").contains("data area"), "{err:#}");
    }

    /// A slice tag is a name or a number, and the two must resolve the same.
    #[test]
    fn solaris_set_type_accepts_a_name_or_a_number() {
        let mut img = solaris_disk();
        edit_solaris(
            &mut img,
            &[PartitionTableEdit::ChangeType {
                index: 0,
                new_type_byte: 0,
                new_type_string: Some("usr".to_string()),
            }],
        )
        .unwrap();
        let table = solaris_parts(&img);
        let crate::partition::PartitionTable::SolarisX86 { label, .. } = &table else {
            unreachable!()
        };
        assert_eq!(label.slices[0].tag_name(), "usr");

        edit_solaris(
            &mut img,
            &[PartitionTableEdit::ChangeType {
                index: 0,
                new_type_byte: 7,
                new_type_string: None,
            }],
        )
        .unwrap();
        let table = solaris_parts(&img);
        let crate::partition::PartitionTable::SolarisX86 { label, .. } = &table else {
            unreachable!()
        };
        assert_eq!(label.slices[0].tag_name(), "var");
    }

    #[test]
    fn dklabel_resize_updates_the_slot() {
        let mut img = dklabel_disk(false);
        edit_dklabel(
            &mut img,
            &[PartitionTableEdit::ResizeEntry {
                index: 0,
                new_size_bytes: 2000 * 512,
            }],
        )
        .unwrap();
        assert_eq!(dklabel_of(&img).map[0].size, 2000);
        assert_eq!(dklabel_of(&img).map[0].base, 119);
    }

    #[test]
    fn dklabel_edits_preserve_a_byte_swapped_image() {
        let mut img = dklabel_disk(true);
        assert_eq!(
            dklabel_of(&img).byte_order,
            crate::partition::sgi_dklabel::SgiLabelByteOrder::Swabbed
        );
        edit_dklabel(
            &mut img,
            &[PartitionTableEdit::ResizeEntry {
                index: 0,
                new_size_bytes: 1500 * 512,
            }],
        )
        .unwrap();
        let after = dklabel_of(&img);
        // Still swabbed, and the edit landed: writing must not silently
        // normalise the medium's word order.
        assert_eq!(
            after.byte_order,
            crate::partition::sgi_dklabel::SgiLabelByteOrder::Swabbed
        );
        assert_eq!(after.map[0].size, 1500);
    }

    #[test]
    fn dklabel_add_uses_the_first_free_slot() {
        let mut img = dklabel_disk(false);
        edit_dklabel(
            &mut img,
            &[PartitionTableEdit::AddEntry {
                start_lba: 2000,
                size_bytes: 500 * 512,
                partition_type: 0,
                type_string: None,
                bootable: false,
            }],
        )
        .unwrap();
        let m = dklabel_of(&img).map;
        assert_eq!((m[1].base, m[1].size), (2000, 500));
    }

    #[test]
    fn dklabel_overlap_is_refused() {
        let mut img = dklabel_disk(false);
        let before = img.clone();
        let err = edit_dklabel(
            &mut img,
            &[PartitionTableEdit::AddEntry {
                start_lba: 500,
                size_bytes: 1000 * 512,
                partition_type: 0,
                type_string: None,
                bootable: false,
            }],
        )
        .unwrap_err();
        assert!(format!("{err}").contains("overlap"), "{err}");
        assert_eq!(img, before, "a refused edit must not write");
    }

    #[test]
    fn dklabel_wrapper_slot_containment_is_allowed() {
        let mut img = dklabel_disk(false);
        // A slot spanning the whole drive is how the label spells "everything".
        edit_dklabel(
            &mut img,
            &[PartitionTableEdit::AddEntry {
                start_lba: 0,
                size_bytes: 8192 * 512,
                partition_type: 0,
                type_string: None,
                bootable: false,
            }],
        )
        .unwrap();
        assert_eq!(dklabel_of(&img).map[1].size, 8192);
    }

    #[test]
    fn dklabel_past_end_is_refused() {
        let mut img = dklabel_disk(false);
        let err = edit_dklabel(
            &mut img,
            &[PartitionTableEdit::ResizeEntry {
                index: 0,
                new_size_bytes: 64 * 1024 * 1024,
            }],
        )
        .unwrap_err();
        assert!(format!("{err}").contains("past the"), "{err}");
    }

    #[test]
    fn dklabel_change_type_is_refused_with_a_reason() {
        let mut img = dklabel_disk(false);
        let err = edit_dklabel(
            &mut img,
            &[PartitionTableEdit::ChangeType {
                index: 0,
                new_type_byte: 0x83,
                new_type_string: None,
            }],
        )
        .unwrap_err();
        assert!(format!("{err}").contains("no type field"), "{err}");
    }

    #[test]
    fn dklabel_set_bootable_moves_d_bootfs() {
        let mut img = dklabel_disk(false);
        // Add a second slot, then boot from the partition listed second.
        edit_dklabel(
            &mut img,
            &[PartitionTableEdit::AddEntry {
                start_lba: 2000,
                size_bytes: 500 * 512,
                partition_type: 0,
                type_string: None,
                bootable: false,
            }],
        )
        .unwrap();
        edit_dklabel(
            &mut img,
            &[PartitionTableEdit::SetBootable {
                index: 1,
                bootable: true,
            }],
        )
        .unwrap();
        assert_eq!(dklabel_of(&img).bootfs, 1);
    }

    /// An empty or wrapper slot is not a partition, so editing one must be
    /// refused rather than landing on a slot the caller never saw.
    #[test]
    fn dklabel_editing_an_unlisted_slot_is_refused() {
        let mut img = dklabel_disk(false);
        let err =
            edit_dklabel(&mut img, &[PartitionTableEdit::DeleteEntry { index: 4 }]).unwrap_err();
        assert!(format!("{err}").contains("not a listed partition"), "{err}");
    }
}
