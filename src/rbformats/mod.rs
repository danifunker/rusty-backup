// Image-format converters legitimately take many parameters
// (reader, output path, sizes, callbacks, format options).
#![allow(clippy::too_many_arguments)]

pub mod chd;
pub mod chd_edit;
pub mod chd_options;
pub mod compress;
pub mod dc42;
pub mod dmg;
pub mod export;
pub mod gho;
pub mod imz;
pub mod interleave;
pub mod qcow2;
#[cfg(test)]
pub(crate) mod qemu_img_test;
pub mod raw;
pub mod sparse;
pub mod twomg;
pub mod vhd;
pub mod vmdk;
pub mod vmdk_sparse;
pub mod woz;
pub mod woz_write;
pub mod zstd;

use std::fs::{self, File};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};

use crate::backup::metadata::BackupMetadata;
use crate::fs::patch_hidden_sectors_for;
use crate::partition::apm::Apm;
use crate::partition::gpt::Gpt;
use crate::partition::mbr::patch_mbr_entries;
use crate::partition::rdb::{Rdb, RDB_SCAN_BLOCKS, RDSK_SIGNATURE};
use crate::partition::PartitionSizeOverride;

// Re-exports keep the historical `crate::rbformats::write_zeros` /
// `super::CHUNK_SIZE` paths working for callers in `restore/` and the
// per-format submodules.
pub use compress::{
    compress_file_to_archive, compress_partition, decompress_partition_to_file,
    decompress_to_writer,
};
pub(crate) use compress::{
    compress_partition_hashed, file_name, is_all_zeros, output_path, write_zeros,
    write_zeros_with_progress, OutputHasherHandle, SplitWriter, CHUNK_SIZE,
};

/// Reconstruct a disk image from a backup folder, writing to any seekable writer.
///
/// Shared by: VHD export (file writer), restore (device or file writer).
///
/// Writes the MBR (patched with partition overrides), then each partition's
/// compressed data at its correct offset, with FAT resize and BPB fixups.
/// Gaps and unused space handling depends on `is_device` and `write_zeros`.
///
/// Returns the total number of bytes written.
pub fn reconstruct_disk_from_backup(
    backup_folder: &Path,
    metadata: &BackupMetadata,
    mbr_bytes: Option<&[u8; 512]>,
    partition_sizes: &[PartitionSizeOverride],
    target_size: u64,
    writer: &mut (impl Read + Write + Seek),
    is_device: bool,
    fill_unused_with_zeros: bool,
    gpt: Option<&Gpt>,
    apm: Option<&Apm>,
    progress_cb: &mut impl FnMut(u64),
    cancel_check: &impl Fn() -> bool,
    log_cb: &mut impl FnMut(&str),
) -> Result<u64> {
    // EBR chain is built inside the MBR branch (after reading mbr_buf) so we
    // can pass the actual MBR bytes as a fallback for old backup formats that
    // pre-date the `extended_container` metadata field.
    let mut ebr_result: Option<crate::restore::EbrChainResult> = None;
    let mut total_written: u64 = 0;

    // Helper to look up export size for a partition index
    let get_export_size = |index: usize, default: u64| -> u64 {
        partition_sizes
            .iter()
            .find(|ps| ps.index == index)
            .map(|ps| ps.export_size)
            .unwrap_or(default)
    };

    // Helper to look up effective start LBA for a partition
    let get_effective_start_lba = |index: usize, default: u64| -> u64 {
        partition_sizes
            .iter()
            .find(|ps| ps.index == index)
            .map(|ps| ps.effective_start_lba())
            .unwrap_or(default)
    };

    let target_sectors = target_size / 512;
    let is_superfloppy = metadata.partition_table_type == "None";
    let is_rdb = metadata.partition_table_type == "RDB";

    if is_rdb {
        // Backup-folder restore for RDB-based Amiga disks is not yet wired
        // up: backups save a parsed `rdb.json` sidecar but not the raw
        // 512-byte RDSK/PART blocks, and re-serializing the parsed form
        // without losing FSHD/LSEG driver chains and BADB lists needs a
        // full RDB encoder we haven't written yet. For resize-aware
        // export of an Amiga disk today, point the user at the
        // direct-from-source path (Inspect tab -> Export raw/VHD), which
        // routes through `reconstruct_raw_rdb_disk` and works end-to-end.
        bail!(
            "RDB-based Amiga disks cannot yet be restored from a backup folder. \
             To resize an Amiga disk, export directly from the source image \
             (Inspect tab -> Export to Raw or VHD with size overrides) — that \
             path is RDB-aware and patches the partition table for you."
        );
    } else if is_superfloppy {
        // Superfloppy: no partition table to write — data starts at offset 0
        log_cb("Superfloppy: no partition table to write");
    } else if let Some(gpt_data) = gpt {
        // GPT restore: write protective MBR + primary GPT (LBAs 0-33)
        let patched_gpt = gpt_data.patch_for_restore(partition_sizes, target_sectors);

        let protective_mbr = Gpt::build_protective_mbr(target_sectors);
        writer
            .write_all(&protective_mbr)
            .context("failed to write protective MBR")?;
        total_written += 512;
        log_cb("Wrote protective MBR for GPT disk");

        let primary_gpt = patched_gpt.build_primary_gpt(target_sectors);
        writer
            .write_all(&primary_gpt)
            .context("failed to write primary GPT")?;
        total_written += primary_gpt.len() as u64;
        log_cb(&format!(
            "Wrote primary GPT ({} bytes, {} entries)",
            primary_gpt.len(),
            patched_gpt.entries.len()
        ));
    } else if let Some(apm_data) = apm {
        // APM restore: write patched DDR + partition map entries
        let target_blocks = (target_size / apm_data.ddr.block_size as u64) as u32;
        let patched_apm = apm_data.patch_for_restore(partition_sizes, target_blocks);
        let apm_blocks = patched_apm.build_apm_blocks(Some(target_blocks));

        writer
            .write_all(&apm_blocks)
            .context("failed to write APM blocks")?;
        total_written += apm_blocks.len() as u64;
        log_cb(&format!(
            "Wrote APM: DDR + {} partition entries ({} bytes)",
            patched_apm.entries.len(),
            apm_blocks.len()
        ));
    } else {
        // MBR restore: write patched MBR
        let mut mbr_buf = if let Some(mbr) = mbr_bytes {
            *mbr
        } else {
            let mbr_path = backup_folder.join("mbr.bin");
            if mbr_path.exists() {
                let data = fs::read(&mbr_path).context("failed to read mbr.bin")?;
                let mut buf = [0u8; 512];
                let copy_len = data.len().min(512);
                buf[..copy_len].copy_from_slice(&data[..copy_len]);
                buf
            } else {
                bail!("no MBR data available for disk reconstruction");
            }
        };
        if !partition_sizes.is_empty() {
            patch_mbr_entries(&mut mbr_buf, partition_sizes);
            log_cb("Patched MBR partition table with export sizes");
        }
        log_cb("Writing MBR to disk...");
        writer.write_all(&mbr_buf).context("failed to write MBR")?;
        total_written += 512;

        // Build EBR chain here so we can pass the actual MBR bytes as a
        // fallback for old backups that lack the `extended_container` field.
        ebr_result =
            crate::restore::build_restore_ebr_chain(metadata, partition_sizes, Some(&mbr_buf));

        // Patch the MBR extended container entry's total_sectors to match the
        // repacked EBR chain layout, then rewrite the MBR.
        if let Some(ref result) = ebr_result {
            let extended_start = result.extended_start_lba as u64;
            // Find the last logical partition end to compute new container size
            let last_end_lba = result
                .logical_starts
                .iter()
                .filter_map(|(idx, start_lba)| {
                    let size = partition_sizes
                        .iter()
                        .find(|ps| ps.index == *idx)
                        .map(|ps| ps.export_size / 512)
                        .or_else(|| {
                            metadata
                                .partitions
                                .iter()
                                .find(|pm| pm.index == *idx)
                                .map(|pm| pm.original_size_bytes / 512)
                        })?;
                    Some(start_lba + size)
                })
                .max();

            if let Some(end_lba) = last_end_lba {
                let new_total_sectors = (end_lba - extended_start) as u32;
                // Find and patch the extended container entry in the MBR
                for i in 0..4 {
                    let off = 446 + i * 16;
                    let type_byte = mbr_buf[off + 4];
                    if type_byte == 0x05 || type_byte == 0x0F || type_byte == 0x85 {
                        let entry_start = u32::from_le_bytes([
                            mbr_buf[off + 8],
                            mbr_buf[off + 9],
                            mbr_buf[off + 10],
                            mbr_buf[off + 11],
                        ]);
                        if entry_start == extended_start as u32 {
                            mbr_buf[off + 12..off + 16]
                                .copy_from_slice(&new_total_sectors.to_le_bytes());
                            log_cb(&format!(
                                "Patched MBR extended container: total_sectors = {}",
                                new_total_sectors,
                            ));
                            // Rewrite the MBR with the updated extended container
                            writer.seek(SeekFrom::Start(0))?;
                            writer
                                .write_all(&mbr_buf)
                                .context("failed to rewrite MBR with extended container patch")?;
                            break;
                        }
                    }
                }
            }
        }
    }

    // Write EBR chain if present (for logical partitions in extended container)
    if let Some(ref result) = ebr_result {
        log_cb(&format!(
            "Writing {} EBR sector(s) for logical partitions...",
            result.ebr_sectors.len()
        ));
        for (ebr_offset, ebr_sector) in &result.ebr_sectors {
            writer.seek(SeekFrom::Start(*ebr_offset))?;
            writer
                .write_all(ebr_sector)
                .context("failed to write EBR sector")?;
        }
        // Seek back to after MBR so gap filling works correctly
        writer.seek(SeekFrom::Start(512))?;
    }

    // Helper: resolve effective start LBA for a partition, using EBR-recalculated
    // positions for logical partitions when the layout was compacted after a resize.
    let get_partition_lba = |pm: &crate::backup::metadata::PartitionMetadata| -> u64 {
        if pm.is_logical || pm.index >= 4 {
            ebr_result
                .as_ref()
                .and_then(|r| {
                    r.logical_starts
                        .iter()
                        .find(|(idx, _)| *idx == pm.index)
                        .map(|(_, lba)| *lba)
                })
                .unwrap_or_else(|| get_effective_start_lba(pm.index, pm.start_lba))
        } else {
            get_effective_start_lba(pm.index, pm.start_lba)
        }
    };

    // Sort partitions by effective start LBA so we always write forward.
    // APM and other schemes can store partitions in arbitrary index order
    // (e.g. index 1 at LBA 300M, index 2 at LBA 100M). Writing in index
    // order would skip the backward seek and corrupt the disk.
    let mut sorted_partitions: Vec<&crate::backup::metadata::PartitionMetadata> =
        metadata.partitions.iter().collect();
    sorted_partitions.sort_by_key(|pm| get_partition_lba(pm));

    // Write each partition at its correct offset, filling gaps with zeros
    for pm in &sorted_partitions {
        if cancel_check() {
            bail!("operation cancelled");
        }

        let effective_lba = get_partition_lba(pm);
        let part_offset = effective_lba * 512;
        let export_size = get_export_size(pm.index, pm.original_size_bytes);

        log_cb(&format!(
            "Partition {}: LBA {}, offset {}, size {} bytes",
            pm.index, effective_lba, part_offset, export_size
        ));

        // Fill or seek over gap between current position and partition start.
        // We always use absolute seeks (SeekFrom::Start) rather than relative
        // seeks because the patch_*_hidden_sectors calls at the end of each
        // iteration may have moved the file cursor away from total_written.
        if total_written < part_offset {
            let gap = part_offset - total_written;
            // On Windows physical drives, we must write zeros to gaps (seeking
            // alone doesn't advance the physical write position reliably).
            #[cfg(target_os = "windows")]
            let force_write_zeros = is_device;
            #[cfg(not(target_os = "windows"))]
            let force_write_zeros = false;

            if force_write_zeros || (is_device && fill_unused_with_zeros) {
                writer.seek(SeekFrom::Start(total_written))?;
                log_cb(&format!("Writing {} bytes of zeros to gap...", gap));
                write_zeros(writer, gap)?;
                writer.flush().context("failed to flush after zeros")?;
                total_written += gap;
            } else if is_device {
                // Unix device without zero-fill: seek directly to partition start
                match writer.seek(SeekFrom::Start(part_offset)) {
                    Ok(_) => {
                        total_written = part_offset;
                    }
                    Err(e) if e.kind() == io::ErrorKind::InvalidInput => {
                        writer.seek(SeekFrom::Start(total_written))?;
                        log_cb(
                            "Warning: device doesn't support absolute seek, writing zeros to gap",
                        );
                        write_zeros(writer, gap)?;
                        total_written += gap;
                    }
                    Err(e) => return Err(e.into()),
                }
            } else {
                // File: use sparse seeks
                writer.seek(SeekFrom::Start(part_offset))?;
                total_written = part_offset;
            }
        } else if total_written == part_offset {
            // No gap, but ensure file position is correct (patch calls may have moved it)
            writer.seek(SeekFrom::Start(part_offset))?;
        }

        // Write partition data
        if pm.compressed_files.is_empty() {
            log_cb(&format!("partition-{}: no data files, skipping", pm.index));
            continue;
        }

        let data_file = &pm.compressed_files[0];
        let data_path = backup_folder.join(data_file);

        if !data_path.exists() {
            log_cb(&format!(
                "partition-{}: data file not found: {}, filling with zeros",
                pm.index,
                data_path.display()
            ));
            write_zeros(writer, export_size)?;
            total_written += export_size;
            continue;
        }

        log_cb(&format!(
            "partition-{}: decompressing {} to writer (target size: {} bytes)...",
            pm.index,
            data_path.display(),
            export_size
        ));

        let base_offset = total_written;
        let bytes_written = decompress_to_writer(
            &data_path,
            &metadata.compression_type,
            writer,
            Some(export_size),
            &mut |bytes| progress_cb(base_offset + bytes),
            cancel_check,
            log_cb,
        )
        .with_context(|| format!("failed to decompress partition {}", pm.index))?;

        log_cb(&format!(
            "partition-{}: decompressed {} bytes",
            pm.index, bytes_written
        ));
        total_written += bytes_written;

        // Handle unused space at end of partition
        if bytes_written < export_size {
            let pad = export_size - bytes_written;
            // Compacted (layout-preserving) partitions: the backup stream was trimmed to
            // the last used block, leaving the tail unwritten.  We MUST zero-fill so that:
            //   (a) no stale data from a previous install remains in the free region,
            //   (b) the alternate volume header slot (HFS/HFS+) is zeroed before step 8
            //       of restore writes the correct header there.
            // Defragmented-clone partitions (Phase-8) carry a fully-packed HFS+ volume
            // sized to the defragmented minimum; the tail past the volume must also be
            // explicitly zeroed so no stale residue remains in the partition window.
            // Progress is reported chunk-by-chunk since the tail can be substantial.
            if pm.compacted || pm.defragmented_clone {
                log_cb(&format!(
                    "partition-{}: trimmed backup — zero-filling {} bytes to complete partition \
                     (filesystem headers finalized in post-processing)",
                    pm.index, pad
                ));
                write_zeros_with_progress(writer, pad, total_written, progress_cb)?;
                total_written += pad;
            } else if fill_unused_with_zeros {
                // User requested zero-fill (may be needed for some filesystems)
                write_zeros(writer, pad)?;
                total_written += pad;
            } else if is_device {
                // Device without zero-fill: try seek, fallback to zeros if needed
                match writer.seek(SeekFrom::Current(pad as i64)) {
                    Ok(_) => total_written += pad,
                    Err(e) if e.kind() == io::ErrorKind::InvalidInput => {
                        log_cb(
                            "Warning: device doesn't support seek in data region, writing zeros",
                        );
                        write_zeros(writer, pad)?;
                        total_written += pad;
                    }
                    Err(e) => return Err(e.into()),
                }
            } else {
                // File: sparse seek - filesystem resize will handle the space
                writer.seek(SeekFrom::Current(pad as i64))?;
                total_written += pad;
            }
        }

        // Update hidden sectors / partition offset to match the new start LBA
        {
            writer.flush()?;
            patch_hidden_sectors_for(writer, part_offset, effective_lba, log_cb)?;
        }

        log_cb(&format!(
            "partition-{}: wrote {} bytes (export size: {})",
            pm.index, bytes_written, export_size,
        ));
    }

    // Write backup GPT at end of disk for GPT restores
    if let Some(gpt_data) = gpt {
        let patched_gpt = gpt_data.patch_for_restore(partition_sizes, target_sectors);
        let backup_gpt = patched_gpt.build_backup_gpt(target_sectors);
        let backup_offset = (target_sectors - 33) * 512;
        writer.seek(SeekFrom::Start(backup_offset))?;
        writer
            .write_all(&backup_gpt)
            .context("failed to write backup GPT")?;
        log_cb(&format!(
            "Wrote backup GPT at LBA {} ({} bytes)",
            target_sectors - 33,
            backup_gpt.len()
        ));
    }

    writer.flush()?;
    Ok(total_written)
}

/// Detect whether a raw source starts with an Apple Partition Map.
/// Returns the parsed APM on success, or `None` if the first sector doesn't
/// have the DDR signature (`ER`) or the map fails to parse.
pub fn detect_raw_apm(reader: &mut (impl Read + Seek)) -> Option<Apm> {
    reader.seek(SeekFrom::Start(0)).ok()?;
    let mut ddr = [0u8; 4];
    let read_ok = reader.read_exact(&mut ddr).is_ok();
    // Always restore the cursor to 0 so callers that fall through to a
    // sequential read of the unwrapped stream don't lose the first bytes.
    reader.seek(SeekFrom::Start(0)).ok()?;
    if !read_ok || &ddr[0..2] != b"ER" {
        return None;
    }
    Apm::parse(reader).ok()
}

/// Reconstruct a raw APM disk with partition size overrides applied.
///
/// For a raw source image (e.g. `.hda`, `.img`) whose first sector is an
/// Apple Driver Descriptor Record, this parses the APM, patches partition
/// entries via [`Apm::patch_for_restore`], writes the patched DDR + entries,
/// then copies each partition's data from its original source position to
/// its new destination position, zero-filling any gaps.
///
/// HFS partitions that are resized get their MDB updated in-place on the
/// destination via `resize_hfs_in_place`. HFS+/HFSX resize would go through
/// `resize_hfsplus_in_place` the same way — added opportunistically.
///
/// Returns the total number of bytes written.
pub fn reconstruct_raw_apm_disk(
    reader: &mut (impl Read + Seek),
    source_data_size: u64,
    writer: &mut (impl Read + Write + Seek),
    partition_sizes: &[PartitionSizeOverride],
    progress_cb: &mut impl FnMut(u64),
    cancel_check: &impl Fn() -> bool,
    log_cb: &mut impl FnMut(&str),
) -> Result<u64> {
    let apm = Apm::parse(reader).context("failed to parse APM from source")?;
    let block_size = apm.ddr.block_size as u64;
    if block_size == 0 {
        bail!("APM DDR has zero block size");
    }

    // Determine target DDR block count: the farthest partition end across
    // patched entries, or the source's original block count if larger.
    let mut max_end: u64 = apm.ddr.block_count as u64;
    for entry in &apm.entries {
        let entry_lba = entry.start_block as u64 * block_size / 512;
        if let Some(ov) = partition_sizes.iter().find(|o| o.start_lba == entry_lba) {
            let new_start_lba = ov.effective_start_lba();
            let new_start_block = new_start_lba * 512 / block_size;
            let new_blocks = ov.export_size / block_size;
            max_end = max_end.max(new_start_block + new_blocks);
        } else {
            max_end = max_end.max(entry.start_block as u64 + entry.block_count as u64);
        }
    }
    let target_block_count: u32 = max_end
        .try_into()
        .context("APM target block count exceeds u32")?;

    let patched = apm.patch_for_restore(partition_sizes, target_block_count);
    let apm_blocks = patched.build_apm_blocks(Some(target_block_count));

    writer.seek(SeekFrom::Start(0))?;
    writer.write_all(&apm_blocks)?;
    let mut total_written: u64 = apm_blocks.len() as u64;
    progress_cb(total_written);
    log_cb(&format!(
        "Wrote patched APM: DDR + {} entries, target block count {} ({} MB)",
        patched.entries.len(),
        target_block_count,
        target_block_count as u64 * block_size / 1024 / 1024
    ));

    // Build a write plan: for each entry, (source_lba, dest_lba, copy_size, export_size, is_hfs).
    struct PartPlan {
        source_lba: u64,
        dest_lba: u64,
        copy_size: u64,
        export_size: u64,
        name: String,
        partition_type: String,
    }
    let mut plans: Vec<PartPlan> = Vec::new();
    for entry in &apm.entries {
        if entry.partition_type == "Apple_partition_map" {
            // Already written as part of the APM blocks above.
            continue;
        }
        let entry_lba = entry.start_block as u64 * block_size / 512;
        let original_size = entry.block_count as u64 * block_size;
        let (dest_lba, export_size) =
            if let Some(ov) = partition_sizes.iter().find(|o| o.start_lba == entry_lba) {
                (ov.effective_start_lba(), ov.export_size)
            } else {
                (entry_lba, original_size)
            };
        plans.push(PartPlan {
            source_lba: entry_lba,
            dest_lba,
            copy_size: original_size.min(export_size),
            export_size,
            name: entry.name.clone(),
            partition_type: entry.partition_type.clone(),
        });
    }
    plans.sort_by_key(|p| p.dest_lba);

    let mut buf = vec![0u8; CHUNK_SIZE];
    for plan in &plans {
        if cancel_check() {
            bail!("export cancelled");
        }

        let dest_offset = plan.dest_lba * 512;
        if total_written < dest_offset {
            let gap = dest_offset - total_written;
            write_zeros_with_progress(writer, gap, 0, &mut |_| {})?;
            total_written += gap;
            progress_cb(total_written);
        } else if total_written > dest_offset {
            bail!(
                "APM export: partition {} at dest {} overlaps earlier data (already wrote {})",
                plan.name,
                dest_offset,
                total_written
            );
        }

        // Copy original partition data from the source.
        let source_offset = plan.source_lba * 512;
        let copy_bound = plan
            .copy_size
            .min(source_data_size.saturating_sub(source_offset));
        reader.seek(SeekFrom::Start(source_offset))?;
        let mut remaining = copy_bound;
        while remaining > 0 {
            if cancel_check() {
                bail!("export cancelled");
            }
            let to_read = (remaining as usize).min(CHUNK_SIZE);
            let n = reader
                .read(&mut buf[..to_read])
                .context("failed to read partition data")?;
            if n == 0 {
                break;
            }
            writer
                .write_all(&buf[..n])
                .context("failed to write partition data")?;
            total_written += n as u64;
            remaining -= n as u64;
            progress_cb(total_written);
        }

        // Zero-fill growth region (if the partition grew).
        if plan.export_size > plan.copy_size {
            let extra = plan.export_size - plan.copy_size;
            write_zeros_with_progress(writer, extra, 0, &mut |_| {})?;
            total_written += extra;
            progress_cb(total_written);
        }

        // For HFS partitions that grew, update the MDB in-place on the dest.
        if plan.partition_type == "Apple_HFS" && plan.export_size != plan.copy_size {
            let part_offset = plan.dest_lba * 512;
            writer.seek(SeekFrom::Start(0))?; // ensure writer is flushed/synced
            match crate::fs::resize_hfs_in_place(writer, part_offset, plan.export_size, &mut |m| {
                log_cb(&format!("  [hfs] {m}"))
            }) {
                Ok(()) => log_cb(&format!(
                    "Resized HFS in partition '{}' to {} MB",
                    plan.name,
                    plan.export_size / 1024 / 1024
                )),
                Err(e) => {
                    log_cb(&format!(
                        "HFS resize failed for partition '{}': {e}. Note: HFS+ / wrapped-HFS+ \
                         volumes can share the Apple_HFS type and aren't resized here.",
                        plan.name
                    ));
                }
            }
            writer.seek(SeekFrom::Start(total_written))?;
        }
    }

    Ok(total_written)
}

/// Probe a raw source reader for an RDB partition table. Returns the
/// parsed Rdb when found, or None for non-RDB sources. Always restores
/// the reader cursor to 0 so a fall-through stream copy doesn't lose
/// the first bytes.
///
/// Twin of [`detect_raw_apm`] — wired into the Raw / VHD export paths so
/// Amiga RDB disks pick up the resize-aware reconstruction path
/// ([`reconstruct_raw_rdb_disk`]) instead of being copied verbatim.
pub fn detect_raw_rdb(reader: &mut (impl Read + Seek)) -> Option<Rdb> {
    reader.seek(SeekFrom::Start(0)).ok()?;
    // Scan the first RDB_SCAN_BLOCKS sectors for an RDSK signature so we
    // don't try to parse an MBR/APM/GPT disk as RDB.
    let mut found = false;
    let mut sec = [0u8; 4];
    for block in 0..RDB_SCAN_BLOCKS {
        if reader.seek(SeekFrom::Start(block * 512)).is_err() {
            break;
        }
        if reader.read_exact(&mut sec).is_err() {
            break;
        }
        if sec == *RDSK_SIGNATURE {
            found = true;
            break;
        }
    }
    let _ = reader.seek(SeekFrom::Start(0));
    if !found {
        return None;
    }
    Rdb::parse(reader).ok()
}

/// Reconstruct a raw RDB-based Amiga disk with partition-size overrides.
///
/// Counterpart to [`reconstruct_raw_apm_disk`]. For an Amiga source disk
/// (RDB at sector 0..16), this:
///
/// 1. Copies the entire RDB region (sectors `[0, RDB_SCAN_BLOCKS)`) from
///    source to destination verbatim so FSHD / LSEG driver chains and
///    bad-block lists survive untouched.
/// 2. Overlays the patched RDSK + PART blocks from
///    [`Rdb::patch_for_restore`] at their original block numbers.
/// 3. Copies each partition's data from its source LBA to its
///    (possibly different) destination LBA, zero-filling growth tail.
/// 4. For partitions whose size changed, invokes
///    [`crate::fs::resize_filesystem_for`] so the filesystem metadata
///    (currently SFS via `resize_sfs_in_place`) is updated on the dest.
///
/// The output disk is sized to `plan.new_disk_size_bytes`, which is
/// derived from the highest patched partition end — shrinking the last
/// partition therefore yields a smaller output file.
///
/// Returns the total bytes written, equal to `plan.new_disk_size_bytes`.
/// Reconstruct an RDB-on-RAW disk into `writer`, applying `partition_sizes`
/// overrides.
///
/// `pfs3_clone_source`: when `Some(path)`, PFS3 partitions whose
/// `export_size < source_size` (i.e. a real shrink) are rebuilt via
/// [`crate::fs::pfs3_clone::stream_defragmented_pfs3`] instead of
/// being copied verbatim + in-place trimmed. The path is opened with
/// a fresh `File` for each PFS3 partition so the clone has its own
/// reader independent of the byte-copy `reader` above. When `None`,
/// all partitions fall back to the legacy verbatim-copy + in-place
/// resize path (`resize_filesystem_for`), which can only trim
/// trailing free sectors and typically refuses real shrinks on PFS3.
pub fn reconstruct_raw_rdb_disk(
    reader: &mut (impl Read + Seek),
    source_data_size: u64,
    writer: &mut (impl Read + Write + Seek),
    partition_sizes: &[PartitionSizeOverride],
    pfs3_clone_source: Option<&std::path::Path>,
    progress_cb: &mut impl FnMut(u64),
    cancel_check: &impl Fn() -> bool,
    log_cb: &mut impl FnMut(&str),
) -> Result<u64> {
    let rdb = Rdb::parse(reader).context("failed to parse RDB from source")?;
    let plan = rdb
        .patch_for_restore(partition_sizes, reader)
        .context("failed to patch RDB for restore")?;

    let target_size = plan.new_disk_size_bytes;
    log_cb(&format!(
        "RDB export: {} partitions, target disk size {} bytes ({} MiB)",
        plan.partition_plans.len(),
        target_size,
        target_size / 1024 / 1024,
    ));

    // --- 1. Copy the entire RDB region verbatim. ---
    let rdb_bytes = (RDB_SCAN_BLOCKS * 512).min(source_data_size);
    reader.seek(SeekFrom::Start(0))?;
    writer.seek(SeekFrom::Start(0))?;
    let mut buf = vec![0u8; CHUNK_SIZE];
    let mut copied: u64 = 0;
    while copied < rdb_bytes {
        let to_read = ((rdb_bytes - copied) as usize).min(CHUNK_SIZE);
        let n = reader
            .read(&mut buf[..to_read])
            .context("read RDB region")?;
        if n == 0 {
            break;
        }
        writer.write_all(&buf[..n]).context("write RDB region")?;
        copied += n as u64;
    }
    // If the destination is smaller than RDB_SCAN_BLOCKS sectors (very
    // unlikely but defensive), bail.
    if copied < (rdb.header.block_num + 1) * 512 {
        bail!(
            "RDB export: copied only {} bytes of RDB region, can't overlay header at block {}",
            copied,
            rdb.header.block_num
        );
    }

    // --- 2. Overlay patched RDSK + PART blocks. ---
    let (rdsk_blk, rdsk_buf) = plan.rdsk_block;
    writer.seek(SeekFrom::Start(rdsk_blk * 512))?;
    writer
        .write_all(&rdsk_buf)
        .context("overlay patched RDSK")?;
    for (part_blk, part_buf) in &plan.part_blocks {
        // PART blocks may live outside the first 16 sectors on
        // unusual disks; trust the source's block_num.
        if *part_blk * 512 + 512 > target_size {
            bail!(
                "RDB export: PART block at {} lies past target disk size {}",
                part_blk,
                target_size
            );
        }
        writer.seek(SeekFrom::Start(part_blk * 512))?;
        writer
            .write_all(part_buf)
            .with_context(|| format!("overlay patched PART block {part_blk}"))?;
    }

    let mut total_written = copied;
    progress_cb(total_written);
    // The PART overlay seeks left the writer at some block_num inside the
    // RDB region — seek back to `total_written` so the partition loop
    // below appends contiguously.
    writer.seek(SeekFrom::Start(total_written))?;

    // --- 3. Copy each partition. ---
    let mut plans: Vec<_> = plan.partition_plans.clone();
    plans.sort_by_key(|p| p.dest_lba);
    // Track partitions whose data was emitted via the PFS3 clone path —
    // they're already complete and must skip the in-place fixup below
    // (which would refuse the shrink and emit a misleading error).
    let mut clone_emitted: std::collections::HashSet<u64> = std::collections::HashSet::new();
    for p in &plans {
        if cancel_check() {
            bail!("export cancelled");
        }
        let dest_offset = p.dest_lba * 512;
        if dest_offset < total_written {
            // Partition starts before where we've already written —
            // probably overlapping the RDB region. RDB partitions
            // conventionally start at or after the RDB scan range, so
            // this is unexpected. Bail rather than silently corrupting.
            bail!(
                "RDB export: partition '{}' dest LBA {} ({} bytes) starts before \
                 already-written {} bytes",
                p.drv_name,
                p.dest_lba,
                dest_offset,
                total_written
            );
        }
        if dest_offset > total_written {
            let gap = dest_offset - total_written;
            writer.seek(SeekFrom::Start(total_written))?;
            write_zeros_with_progress(writer, gap, 0, &mut |_| {})?;
            total_written += gap;
            progress_cb(total_written);
        }
        // Make sure subsequent partition writes land at dest_offset, not
        // wherever the writer happened to be left.
        writer.seek(SeekFrom::Start(dest_offset))?;

        // PFS3 shrink fast-path: clone the source PFS3 volume into the
        // destination at `export_size` bytes rather than copying source
        // bytes verbatim. The in-place trim (`resize_pfs3_in_place`) is
        // useless on most PFS3 volumes because the rovingPointer
        // allocator scatters blocks throughout the volume — any
        // allocated tail block forces `last_data_byte ≈ partition_size`
        // and the in-place resize refuses. The clone rebuilds the
        // layout packed, achieving the defragmented floor.
        //
        // Gated by `pfs3_clone_source` being set AND
        // `export_size < copy_size` (an actual shrink). Grow / no-op
        // paths still fall through to the verbatim copy below.
        let is_pfs3 = crate::fs::is_amiga_pfs3_type(&p.dos_type_string);
        // `copy_size = min(original, export)` so on any shrink it
        // equals `export_size` — use `original_size` instead to
        // detect a real shrink request.
        let is_shrink = p.export_size < p.original_size;
        if is_pfs3 && is_shrink {
            if let Some(src_path) = pfs3_clone_source {
                log_cb(&format!(
                    "  [{}] PFS3 shrink: rebuilding volume via defragmenting clone \
                     ({} -> {} bytes)",
                    p.dos_type_string, p.copy_size, p.export_size
                ));
                let src_file = std::fs::File::open(src_path)
                    .with_context(|| format!("open PFS3 clone source {}", src_path.display()))?;
                let src_br = std::io::BufReader::new(src_file);
                let mut src_fs = crate::fs::pfs3::Pfs3Filesystem::open(src_br, p.source_lba * 512)
                    .with_context(|| {
                        format!("open source PFS3 at LBA {} for clone-shrink", p.source_lba)
                    })?;
                let base_written = total_written;
                let export_size = p.export_size;
                let report = {
                    let log_cb_ref = &mut *log_cb;
                    let progress_cb_ref = &mut *progress_cb;
                    let dos_label = p.dos_type_string.clone();
                    let mut clone_log = |s: &str| {
                        log_cb_ref(&format!("  [{}] {}", dos_label, s));
                    };
                    let mut clone_progress = |emitted: u64| {
                        progress_cb_ref(base_written + emitted.min(export_size));
                    };
                    crate::fs::pfs3_clone::stream_defragmented_pfs3(
                        &mut src_fs,
                        export_size,
                        writer,
                        &mut clone_log,
                        &mut clone_progress,
                    )
                    .with_context(|| format!("clone-shrink PFS3 partition '{}'", p.drv_name))?
                };
                for w in &report.warnings {
                    log_cb(&format!(
                        "  [{}] PFS3 clone warning: {w}",
                        p.dos_type_string
                    ));
                }
                log_cb(&format!(
                    "  [{}] PFS3 clone: {} files / {} dirs / {} symlinks / {} hardlinks / \
                     {} data bytes",
                    p.dos_type_string,
                    report.files_copied,
                    report.dirs_copied,
                    report.symlinks_copied,
                    report.hardlinks_copied,
                    report.bytes_copied,
                ));
                total_written += p.export_size;
                progress_cb(total_written);
                clone_emitted.insert(p.dest_lba);
                continue;
            } else {
                log_cb(&format!(
                    "  [{}] PFS3 shrink requested but no clone source provided; \
                     falling back to in-place trim (likely to fail)",
                    p.dos_type_string
                ));
            }
        }

        // Copy the actual partition bytes from the source.
        let source_offset = p.source_lba * 512;
        let copy_bound = p
            .copy_size
            .min(source_data_size.saturating_sub(source_offset));
        reader.seek(SeekFrom::Start(source_offset))?;
        let mut remaining = copy_bound;
        while remaining > 0 {
            if cancel_check() {
                bail!("export cancelled");
            }
            let to_read = (remaining as usize).min(CHUNK_SIZE);
            let n = reader
                .read(&mut buf[..to_read])
                .with_context(|| format!("read partition '{}' data", p.drv_name))?;
            if n == 0 {
                break;
            }
            writer
                .write_all(&buf[..n])
                .with_context(|| format!("write partition '{}' data", p.drv_name))?;
            total_written += n as u64;
            remaining -= n as u64;
            progress_cb(total_written);
        }

        // Zero-fill growth tail when shrinking is NOT happening (i.e.
        // the partition grew). For shrink, copy_size < export_size is
        // impossible (we take the min) — copy_bound caps at copy_size,
        // and export_size <= copy_size means no tail to zero.
        if p.export_size > p.copy_size {
            let extra = p.export_size - p.copy_size;
            write_zeros_with_progress(writer, extra, 0, &mut |_| {})?;
            total_written += extra;
            progress_cb(total_written);
        }
    }

    // --- 4. Per-partition filesystem fixup for resized partitions. ---
    for p in &plans {
        // Partitions emitted via the PFS3 clone path are already
        // complete + internally consistent — running the in-place
        // resize over them would scribble on a fresh volume and
        // (because export_size < the source's last_data_byte) fail.
        if clone_emitted.contains(&p.dest_lba) {
            continue;
        }
        if p.export_size == (p.copy_size).max(p.export_size).min(p.export_size)
            && p.source_lba == p.dest_lba
            && {
                // size unchanged AND no offset change → nothing to fix
                let orig_size = plan
                    .partition_plans
                    .iter()
                    .find(|q| q.source_lba == p.source_lba)
                    .map(|q| q.copy_size)
                    .unwrap_or(p.copy_size);
                orig_size == p.export_size
            }
        {
            continue;
        }
        let part_offset = p.dest_lba * 512;
        let mut local_log = |m: &str| log_cb(&format!("  [{}] {m}", p.dos_type_string));
        if let Err(e) =
            crate::fs::resize_filesystem_for(writer, part_offset, p.export_size, &mut local_log)
        {
            log_cb(&format!(
                "RDB export: resize for partition '{}' ({}) failed: {e}",
                p.drv_name, p.dos_type_string
            ));
        }
    }

    // --- 5. Pad to the declared target size. ---
    if total_written < target_size {
        let pad = target_size - total_written;
        write_zeros_with_progress(writer, pad, 0, &mut |_| {})?;
        total_written += pad;
        progress_cb(total_written);
    }

    Ok(total_written)
}

// ---------------------------------------------------------------------------
// Unified image format detection
// ---------------------------------------------------------------------------

/// Detected disk image format.
pub enum ImageFormat {
    /// No wrapper — treat the file as a raw disk image.
    Raw,
    /// VHD Fixed — strip the 512-byte footer; `data_size` is the raw data length.
    Vhd { data_size: u64 },
    /// VHD Dynamic (sparse) — opened on-demand by path via `DynamicVhdReader`;
    /// `logical_size` is the virtual disk size.
    VhdDynamic { path: PathBuf, logical_size: u64 },
    /// 2MG (Apple II) — skip header, use data_offset/data_length.
    TwoMg(twomg::TwoMgHeader),
    /// UDIF DMG — virtual decompressed reader.
    Dmg(dmg::DmgReader),
    /// Apple II DOS sector order — 5.25" floppy (140 KB), needs interleave to ProDOS order.
    DosOrder,
    /// DiskCopy 4.2 — classic Mac/Apple IIgs floppy image with 84-byte header.
    DiskCopy42(dc42::Dc42Header),
    /// WOZ 1.0/2.0 — nibble/bitstream floppy image, decoded to logical sectors.
    Woz(woz::WozReader),
    /// QCOW2 (v2 or v3, uncompressed) — opened on-demand by path via
    /// [`qcow2::Qcow2Reader`]; `logical_size` is the virtual disk size.
    Qcow2 {
        path: PathBuf,
        logical_size: u64,
        version: u32,
    },
    /// VMDK flat (`monolithicFlat` / `twoGbMaxExtentFlat`) — ASCII descriptor
    /// referencing one or more `FLAT`/`ZERO` extents. Opened on-demand by path
    /// via [`vmdk::VmdkFlatReader`]; `logical_size` is the virtual disk size.
    VmdkFlat { path: PathBuf, logical_size: u64 },
    /// VMDK sparse (`monolithicSparse`) — binary `KDMV` header + grain
    /// directory + grain tables. Opened on-demand by path via
    /// [`vmdk_sparse::VmdkSparseReader`]; `logical_size` is the virtual disk
    /// size (= `capacity_sectors * 512`).
    VmdkSparse { path: PathBuf, logical_size: u64 },
    /// MAME CHD — opened on-demand by path; logical_size is the uncompressed length.
    Chd { path: PathBuf, logical_size: u64 },
    /// MAME CD CHD — single-track MODE1, browsed via the cooked 2048-byte adapter.
    /// `logical_size` is `frames * 2048`.
    ChdCdCooked { path: PathBuf, logical_size: u64 },
}

impl ImageFormat {
    /// Human-readable name of the detected format.
    pub fn description(&self) -> String {
        match self {
            ImageFormat::Raw => "Raw disk image".to_string(),
            ImageFormat::Vhd { data_size } => {
                format!("Fixed VHD (data: {} bytes)", data_size)
            }
            ImageFormat::VhdDynamic { logical_size, .. } => {
                format!("Dynamic VHD (disk: {} bytes)", logical_size)
            }
            ImageFormat::TwoMg(hdr) => {
                format!("2MG ({}, {} bytes)", hdr.format_name(), hdr.data_length)
            }
            ImageFormat::Dmg(reader) => {
                format!(
                    "Compressed DMG/UDIF ({}, {} bytes)",
                    dmg::describe_dmg_methods(reader),
                    reader.total_size()
                )
            }
            ImageFormat::DosOrder => {
                format!(
                    "DOS sector order ({} bytes, will convert to ProDOS order)",
                    interleave::FLOPPY_140K
                )
            }
            ImageFormat::DiskCopy42(ref hdr) => {
                format!(
                    "DiskCopy 4.2 \"{}\" ({}, {} bytes)",
                    hdr.disk_name,
                    hdr.format_name(),
                    hdr.data_size
                )
            }
            ImageFormat::Woz(ref reader) => {
                format!(
                    "WOZ {} ({} bytes decoded)",
                    reader.disk_type_name(),
                    reader.len()
                )
            }
            ImageFormat::Qcow2 {
                logical_size,
                version,
                ..
            } => {
                format!("QCOW2 v{} (disk: {} bytes)", version, logical_size)
            }
            ImageFormat::VmdkFlat { logical_size, .. } => {
                format!("VMDK flat (disk: {} bytes)", logical_size)
            }
            ImageFormat::VmdkSparse { logical_size, .. } => {
                format!("VMDK sparse (disk: {} bytes)", logical_size)
            }
            ImageFormat::Chd { logical_size, .. } => {
                format!("MAME CHD ({} bytes decoded)", logical_size)
            }
            ImageFormat::ChdCdCooked { logical_size, .. } => {
                format!("MAME CD CHD ({} bytes cooked MODE1)", logical_size)
            }
        }
    }
}

/// Detect the image format of an opened file.
///
/// Checks for 2MG header, VHD footer, DMG koly footer, DOS 3.3-order floppy,
/// and falls back to raw.
///
/// The optional `path` enables extension-based heuristics for formats that lack
/// a magic signature (e.g. `.do`/`.dsk` files).
pub fn detect_image_format(file: File) -> Result<ImageFormat> {
    detect_image_format_with_path(file, None)
}

/// Like [`detect_image_format`] but with an optional filesystem path for
/// extension-based detection of headerless formats.
pub fn detect_image_format_with_path(file: File, path: Option<&Path>) -> Result<ImageFormat> {
    let file_size = file.metadata()?.len();
    let mut file = file;

    // 0. Check first 8 bytes for CHD magic ("MComprHD"). CHD is opened on-demand
    //    via libchdman_rs by path, so we don't keep the File handle.
    if file_size >= 16 {
        file.seek(SeekFrom::Start(0))?;
        let mut magic = [0u8; 8];
        if file.read_exact(&mut magic).is_ok() && &magic == b"MComprHD" {
            if let Some(p) = path {
                // CD CHDs need the cooked-sector adapter to expose 2048 B/sector
                // user data. HD/DVD CHDs are flat byte streams and use ChdReader.
                let is_cd = chd::chd_is_cd(p).unwrap_or(false);
                if is_cd {
                    if let Ok(reader) = chd::CdCookedReader::open_path(p) {
                        let logical_size = reader.logical_size();
                        drop(reader);
                        return Ok(ImageFormat::ChdCdCooked {
                            path: p.to_path_buf(),
                            logical_size,
                        });
                    }
                }
                if let Ok(reader) = chd::ChdReader::open(p) {
                    let logical_size = reader.logical_size();
                    drop(reader);
                    return Ok(ImageFormat::Chd {
                        path: p.to_path_buf(),
                        logical_size,
                    });
                }
            }
        }
    }

    // 1. Check first 8 bytes for WOZ magic (WOZ1/WOZ2 + high-bit check).
    //    WOZ has a very strong 8-byte signature, so check it first.
    if file_size >= 12 {
        file.seek(SeekFrom::Start(0))?;
        let mut magic = [0u8; 8];
        if file.read_exact(&mut magic).is_ok() && woz::is_woz(&magic) {
            // Read entire file and decode — floppy images are small (< 1MB)
            file.seek(SeekFrom::Start(0))?;
            let mut raw = Vec::new();
            file.read_to_end(&mut raw)?;
            match woz::WozReader::from_bytes(raw) {
                Ok(reader) => return Ok(ImageFormat::Woz(reader)),
                Err(_) => {
                    // WOZ magic matched but decoding failed — fall through
                }
            }
        }
    }

    // 1b. QCOW2 magic at offset 0 (`QFI\xFB`). Strong fixed signature, placed
    //     ahead of the heuristic/size-based checks. The reader does all the
    //     feature gating (rejects qcow1, snapshots, encryption, etc.) so we
    //     only have to confirm the magic + harvest the virtual size here.
    if file_size >= 32 {
        file.seek(SeekFrom::Start(0))?;
        let mut head = [0u8; 32];
        if file.read_exact(&mut head).is_ok() && &head[0..4] == qcow2::QCOW2_MAGIC {
            if let Some(p) = path {
                // Re-open through the reader so a malformed header rejects
                // here, not later during browse.
                if let Ok(reader) = qcow2::Qcow2Reader::open(File::open(p)?) {
                    let logical_size = reader.len();
                    let version = reader.version();
                    drop(reader);
                    return Ok(ImageFormat::Qcow2 {
                        path: p.to_path_buf(),
                        logical_size,
                        version,
                    });
                }
            }
        }
    }

    // 1c'. VMDK sparse magic at offset 0 (`KDMV`). Strong fixed signature;
    //      placed before the flat-descriptor sniff because sparse `.vmdk`
    //      files share the same extension but lead with binary magic the
    //      flat reader rejects.
    if file_size >= 512 {
        file.seek(SeekFrom::Start(0))?;
        let mut head = [0u8; 4];
        if file.read_exact(&mut head).is_ok() && &head == vmdk_sparse::VMDK_SPARSE_MAGIC {
            if let Some(p) = path {
                if let Ok(reader) = vmdk_sparse::VmdkSparseReader::open(File::open(p)?) {
                    let logical_size = reader.len();
                    drop(reader);
                    return Ok(ImageFormat::VmdkSparse {
                        path: p.to_path_buf(),
                        logical_size,
                    });
                }
            }
        }
    }

    // 1c. VMDK flat descriptor at offset 0 (ASCII `# Disk DescriptorFile`).
    //     Strong fixed signature; placed alongside the other magic-at-0
    //     checks. Sparse VMDKs lead with `KDMV` (handled above).
    if file_size >= vmdk::VMDK_DESCRIPTOR_MAGIC.len() as u64 {
        file.seek(SeekFrom::Start(0))?;
        let mut head = vec![0u8; vmdk::VMDK_DESCRIPTOR_MAGIC.len()];
        if file.read_exact(&mut head).is_ok() && head == vmdk::VMDK_DESCRIPTOR_MAGIC {
            if let Some(p) = path {
                if let Ok(reader) = vmdk::VmdkFlatReader::open(p) {
                    let logical_size = reader.len();
                    drop(reader);
                    return Ok(ImageFormat::VmdkFlat {
                        path: p.to_path_buf(),
                        logical_size,
                    });
                }
            }
        }
    }

    // 2. Check first 4 bytes for 2MG magic
    if file_size >= 64 {
        file.seek(SeekFrom::Start(0))?;
        let mut magic = [0u8; 4];
        if file.read_exact(&mut magic).is_ok() && &magic == twomg::TWOMG_MAGIC {
            file.seek(SeekFrom::Start(0))?;
            if let Some(hdr) = twomg::parse_twomg_header(&mut file) {
                return Ok(ImageFormat::TwoMg(hdr));
            }
        }
    }

    // 3. Check last 512 bytes for VHD "conectix" cookie. The footer's disk-type
    //    field (offset 60) distinguishes fixed (2) from dynamic/sparse (3).
    if file_size >= 512 {
        file.seek(SeekFrom::End(-512))?;
        let mut footer = [0u8; 512];
        if file.read_exact(&mut footer).is_ok() && &footer[0..8] == vhd::VHD_COOKIE {
            let disk_type = u32::from_be_bytes(footer[60..64].try_into().unwrap());
            if disk_type == vhd::VHD_TYPE_DYNAMIC {
                // Logical size = footer "current size" (offset 48). The reader
                // is opened by path in wrap_image_reader.
                let logical_size = u64::from_be_bytes(footer[48..56].try_into().unwrap());
                if let Some(p) = path {
                    return Ok(ImageFormat::VhdDynamic {
                        path: p.to_path_buf(),
                        logical_size,
                    });
                }
            }
            // Fixed (or any non-dynamic) VHD: strip the 512-byte footer.
            return Ok(ImageFormat::Vhd {
                data_size: file_size - 512,
            });
        }
    }

    // 4. Check last 512 bytes for DMG koly footer
    if file_size >= 512 {
        match dmg::detect_dmg(file) {
            Ok(Some(reader)) => return Ok(ImageFormat::Dmg(reader)),
            Ok(None) => {
                // Not a UDIF DMG — fall through.
                // We consumed the file; re-check remaining heuristics below.
            }
            Err(_) => {
                // Parse error — fall through to remaining checks.
            }
        }
    }

    // 5. DiskCopy 4.2 detection: 84-byte header with private field = 0x0100,
    //    file size must equal 84 + data_size + tag_size.
    if file_size >= dc42::DC42_HEADER_SIZE {
        // Re-open since DMG detection may have consumed the file handle.
        if let Some(p) = path {
            if let Ok(mut f) = File::open(p) {
                if let Some(hdr) = dc42::detect_dc42(&mut f, file_size) {
                    return Ok(ImageFormat::DiskCopy42(hdr));
                }
            }
        }
    }

    // 6. DOS-order floppy detection for 140K images with .do/.dsk extension.
    //    A .dsk file could be either DOS or ProDOS order, so we probe the
    //    sector data to disambiguate.
    if file_size == interleave::FLOPPY_140K {
        if let Some(ext) = path.and_then(|p| p.extension()).and_then(|e| e.to_str()) {
            let ext_lower = ext.to_ascii_lowercase();
            match ext_lower.as_str() {
                "do" => return Ok(ImageFormat::DosOrder),
                "dsk" => {
                    // Ambiguous — probe content to decide ordering.
                    // Re-open since DMG detection may have consumed the file handle.
                    if let Some(p) = path {
                        if let Ok(mut f) = File::open(p) {
                            let mut raw = vec![0u8; interleave::FLOPPY_140K as usize];
                            if f.read_exact(&mut raw).is_ok() && interleave::is_dos_order(&raw) {
                                return Ok(ImageFormat::DosOrder);
                            }
                        }
                    }
                    // If probe failed or it's ProDOS order, fall through to Raw
                }
                _ => {}
            }
        }
    }

    // 7. Fallback — raw image
    Ok(ImageFormat::Raw)
}

/// A boxed reader that implements both `Read` and `Seek`.
pub type BoxReadSeek = Box<dyn ReadSeek>;

/// Trait alias for `Read + Seek + Send`.
pub trait ReadSeek: Read + Seek + Send {}
impl<T: Read + Seek + Send> ReadSeek for T {}

/// Wraps a file according to its detected image format, returning a reader
/// positioned at the start of the disk data and the data length.
///
/// The caller must pass the *same* file used for `detect_image_format` (or a
/// freshly opened copy).  For `Dmg`, the `DmgReader` is moved out of the
/// `ImageFormat` enum.
pub fn wrap_image_reader(file: File, format: ImageFormat) -> Result<(BoxReadSeek, u64)> {
    match format {
        ImageFormat::Raw => {
            let size = file.metadata()?.len();
            let mut reader = BufReader::new(file);
            reader.seek(SeekFrom::Start(0))?;
            Ok((Box::new(reader), size))
        }
        ImageFormat::Vhd { data_size } => {
            let mut reader = BufReader::new(file);
            reader.seek(SeekFrom::Start(0))?;
            // We return a Take-limited reader so the caller never sees the footer.
            // But Take doesn't implement Seek, so we use a SectionReader.
            Ok((
                Box::new(SectionReader::new(reader, 0, data_size)?),
                data_size,
            ))
        }
        ImageFormat::TwoMg(hdr) => {
            let mut reader = BufReader::new(file);
            reader.seek(SeekFrom::Start(hdr.data_offset))?;
            Ok((
                Box::new(SectionReader::new(
                    reader,
                    hdr.data_offset,
                    hdr.data_length,
                )?),
                hdr.data_length,
            ))
        }
        ImageFormat::Dmg(reader) => {
            let size = reader.total_size();
            Ok((Box::new(reader), size))
        }
        ImageFormat::DosOrder => {
            let mut raw = Vec::new();
            let mut reader = BufReader::new(file);
            reader.seek(SeekFrom::Start(0))?;
            reader.read_to_end(&mut raw)?;
            let dos_reader = interleave::DosOrderReader::new(raw)
                .map_err(|e| anyhow::anyhow!("failed to read DOS-order image: {e}"))?;
            let size = dos_reader.len();
            Ok((Box::new(dos_reader), size))
        }
        ImageFormat::DiskCopy42(hdr) => {
            let reader = BufReader::new(file);
            Ok((
                Box::new(SectionReader::new(
                    reader,
                    dc42::DC42_HEADER_SIZE,
                    hdr.data_size,
                )?),
                hdr.data_size,
            ))
        }
        ImageFormat::Woz(reader) => {
            // WOZ data was already decoded during format detection.
            // The file parameter is unused — the WozReader holds the decoded data.
            drop(file);
            let size = reader.len();
            Ok((Box::new(reader), size))
        }
        ImageFormat::VhdDynamic { path, logical_size } => {
            // Re-open by path so the reader owns its own File handle.
            drop(file);
            let reader = vhd::DynamicVhdReader::open(File::open(&path)?)?;
            Ok((Box::new(reader), logical_size))
        }
        ImageFormat::Qcow2 {
            path,
            logical_size,
            version: _,
        } => {
            // Re-open by path so the reader owns its own File handle (mirrors
            // VhdDynamic / Chd).
            drop(file);
            let reader = qcow2::Qcow2Reader::open(File::open(&path)?)?;
            Ok((Box::new(reader), logical_size))
        }
        ImageFormat::VmdkFlat { path, logical_size } => {
            drop(file);
            let reader = vmdk::VmdkFlatReader::open(&path)?;
            Ok((Box::new(reader), logical_size))
        }
        ImageFormat::VmdkSparse { path, logical_size } => {
            drop(file);
            let reader = vmdk_sparse::VmdkSparseReader::open(File::open(&path)?)?;
            Ok((Box::new(reader), logical_size))
        }
        ImageFormat::Chd { path, logical_size } => {
            // CHD is opened directly via libchdman_rs; the raw File is unused.
            drop(file);
            let reader = chd::ChdReader::open(&path)?;
            Ok((Box::new(reader), logical_size))
        }
        ImageFormat::ChdCdCooked { path, logical_size } => {
            drop(file);
            let reader = chd::CdCookedReader::open_path(&path)?;
            Ok((Box::new(reader), logical_size))
        }
    }
}

/// A reader that exposes a section (offset..offset+length) of an inner reader
/// as if it were a standalone file.
struct SectionReader<R: Read + Seek> {
    inner: R,
    base: u64,
    length: u64,
    position: u64,
}

impl<R: Read + Seek> SectionReader<R> {
    fn new(mut inner: R, base: u64, length: u64) -> Result<Self> {
        inner.seek(SeekFrom::Start(base))?;
        Ok(Self {
            inner,
            base,
            length,
            position: 0,
        })
    }
}

impl<R: Read + Seek> Read for SectionReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let remaining = self.length.saturating_sub(self.position);
        if remaining == 0 {
            return Ok(0);
        }
        let to_read = (buf.len() as u64).min(remaining) as usize;
        let n = self.inner.read(&mut buf[..to_read])?;
        self.position += n as u64;
        Ok(n)
    }
}

impl<R: Read + Seek> Seek for SectionReader<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(p) => p as i64,
            SeekFrom::Current(delta) => self.position as i64 + delta,
            SeekFrom::End(delta) => self.length as i64 + delta,
        };
        if new_pos < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek to negative position",
            ));
        }
        let new_pos = new_pos as u64;
        self.inner.seek(SeekFrom::Start(self.base + new_pos))?;
        self.position = new_pos;
        Ok(self.position)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Build a minimal APM + classic HFS raw disk image for round-trip testing:
    /// block 0 = DDR, blocks 1-2 = APM (self-map + HFS), blocks 3+ = HFS data.
    fn build_apm_hfs_test_disk() -> Vec<u8> {
        use crate::partition::apm::build_minimal_apm;
        use byteorder::{BigEndian, ByteOrder};

        // Classic HFS at block 3. Use a tiny volume: 128 alloc blocks × 4096
        // bytes = 512 KB of data area, plus 3 sectors of overhead
        // (boot blocks 0-1, MDB 2, bitmap 3, first_alloc_block = 5).
        let ddr_block_size = 512u32;
        let hfs_start_block = 3u32; // in DDR blocks (512)
        let hfs_total_alloc_blocks = 128u16;
        let hfs_alloc_block_size = 4096u32;
        let hfs_first_alloc_sec = 5u16;
        let hfs_size_bytes = hfs_first_alloc_sec as u64 * 512
            + hfs_total_alloc_blocks as u64 * hfs_alloc_block_size as u64;
        let hfs_size_blocks = (hfs_size_bytes / ddr_block_size as u64) as u32;

        let total_disk_blocks = hfs_start_block + hfs_size_blocks;
        let apm = build_minimal_apm(
            &[("Apple_HFS".to_string(), hfs_start_block, hfs_size_blocks)],
            ddr_block_size,
            total_disk_blocks,
        );

        let mut disk = vec![0u8; total_disk_blocks as usize * ddr_block_size as usize];
        let apm_bytes = apm.build_apm_blocks(Some(total_disk_blocks));
        disk[..apm_bytes.len()].copy_from_slice(&apm_bytes);

        // Write a minimal HFS MDB at hfs_start_block * 512 + 1024.
        let part_offset = hfs_start_block as usize * 512;
        let mdb_off = part_offset + 1024;
        BigEndian::write_u16(&mut disk[mdb_off..mdb_off + 2], 0x4244); // HFS signature
        BigEndian::write_u16(&mut disk[mdb_off + 14..mdb_off + 16], 3); // vbm_st
        BigEndian::write_u16(
            &mut disk[mdb_off + 18..mdb_off + 20],
            hfs_total_alloc_blocks,
        );
        BigEndian::write_u32(&mut disk[mdb_off + 20..mdb_off + 24], hfs_alloc_block_size);
        BigEndian::write_u16(&mut disk[mdb_off + 28..mdb_off + 30], hfs_first_alloc_sec);
        BigEndian::write_u16(
            &mut disk[mdb_off + 34..mdb_off + 36],
            hfs_total_alloc_blocks,
        ); // all free
        disk[mdb_off + 36] = 4;
        disk[mdb_off + 37..mdb_off + 41].copy_from_slice(b"Test");

        disk
    }

    #[test]
    fn test_reconstruct_raw_apm_disk_grows_hfs_partition() {
        use crate::partition::apm::Apm;
        use crate::partition::PartitionSizeOverride;

        let source = build_apm_hfs_test_disk();
        let source_size = source.len() as u64;

        // Parse source APM to discover the HFS partition's layout.
        let mut reader = Cursor::new(source.clone());
        let apm = Apm::parse(&mut reader).unwrap();
        let hfs_entry = apm
            .entries
            .iter()
            .find(|e| e.partition_type == "Apple_HFS")
            .expect("source must have Apple_HFS partition");
        let block_size = apm.ddr.block_size as u64;
        let hfs_start_lba = hfs_entry.start_block as u64 * block_size / 512;
        let original_size = hfs_entry.block_count as u64 * block_size;
        // Grow by one sector's worth of allocation blocks.
        let new_size = original_size + 16 * 4096;

        let override_ = PartitionSizeOverride::size_only(0, hfs_start_lba, original_size, new_size);

        // Use a big enough Cursor so in-place writes past source length succeed.
        let mut dest_buf = vec![0u8; source_size as usize + new_size as usize];
        let mut writer = Cursor::new(&mut dest_buf);

        let mut reader2 = Cursor::new(source);
        let mut progress: u64 = 0;
        let total = reconstruct_raw_apm_disk(
            &mut reader2,
            source_size,
            &mut writer,
            &[override_],
            &mut |b| progress = b,
            &|| false,
            &mut |_| {},
        )
        .unwrap();

        assert!(
            total >= hfs_start_lba * 512 + new_size,
            "total={} expected >= {}",
            total,
            hfs_start_lba * 512 + new_size
        );

        // Re-parse the dest APM and confirm the HFS entry grew.
        let mut dest_reader = Cursor::new(&dest_buf[..total as usize]);
        let dest_apm = Apm::parse(&mut dest_reader).unwrap();
        let dest_hfs = dest_apm
            .entries
            .iter()
            .find(|e| e.partition_type == "Apple_HFS")
            .expect("dest must have Apple_HFS");
        assert_eq!(
            dest_hfs.block_count as u64 * block_size,
            new_size,
            "patched APM entry should reflect new size"
        );

        // Confirm the HFS MDB's drNmAlBlks was updated in the dest.
        use byteorder::{BigEndian, ByteOrder};
        let part_offset = hfs_start_lba as usize * 512;
        let mdb_off = part_offset + 1024;
        let new_total_blocks = BigEndian::read_u16(&dest_buf[mdb_off + 18..mdb_off + 20]);
        let expected_blocks = ((new_size - 5 * 512) / 4096) as u16;
        assert_eq!(
            new_total_blocks, expected_blocks,
            "HFS MDB drNmAlBlks should be updated to reflect new size"
        );
    }
}
