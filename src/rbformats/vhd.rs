use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};

use super::{decompress_to_writer, reconstruct_disk_from_backup, write_zeros, CHUNK_SIZE};
use crate::backup::metadata::BackupMetadata;
use crate::fs::btrfs::resize_btrfs_in_place;
use crate::fs::exfat::{patch_exfat_hidden_sectors, resize_exfat_in_place};
use crate::fs::ext::resize_ext_in_place;
use crate::fs::fat::{patch_bpb_hidden_sectors, resize_fat_in_place};
use crate::fs::hfs::{patch_hfs_hidden_sectors, resize_hfs_in_place};
use crate::fs::hfsplus::{patch_hfsplus_hidden_sectors, resize_hfsplus_in_place};
use crate::fs::ntfs::{patch_ntfs_hidden_sectors, resize_ntfs_in_place};
use crate::partition::mbr::{
    build_ebr_chain, parse_ebr_chain, patch_mbr_entries, LogicalPartitionInfo, Mbr,
};
use crate::partition::PartitionSizeOverride;

/// VHD cookie identifying a valid VHD footer.
pub const VHD_COOKIE: &[u8; 8] = b"conectix";

/// Build a 512-byte VHD (Fixed) footer for the given data size.
///
/// The footer follows the Microsoft VHD specification (v1.0):
/// - Cookie: `"conectix"`
/// - Features: `0x00000002` (reserved, must be set)
/// - File Format Version: `0x00010000` (1.0)
/// - Data Offset: `0xFFFFFFFFFFFFFFFF` (fixed disk, no dynamic header)
/// - Timestamp: seconds since 2000-01-01 00:00:00 UTC
/// - Creator Application: `"rsbk"` (Rusty Backup)
/// - Creator Version: `0x00010000`
/// - Creator Host OS: platform-dependent (`"Wi2k"` / `"Mac "`)
/// - Original / Current Size: the raw data size
/// - Disk Geometry: CHS per VHD spec algorithm
/// - Disk Type: `2` (Fixed)
/// - Unique ID: random 16 bytes
/// - Checksum: one's complement of the sum of all footer bytes (excluding checksum field)
pub fn build_vhd_footer(data_size: u64) -> [u8; 512] {
    let mut footer = [0u8; 512];

    // Cookie (offset 0, 8 bytes)
    footer[0..8].copy_from_slice(VHD_COOKIE);

    // Features (offset 8, 4 bytes) — 0x00000002 = reserved bit
    footer[8..12].copy_from_slice(&0x0000_0002u32.to_be_bytes());

    // File Format Version (offset 12, 4 bytes) — 1.0
    footer[12..16].copy_from_slice(&0x0001_0000u32.to_be_bytes());

    // Data Offset (offset 16, 8 bytes) — 0xFFFFFFFFFFFFFFFF for fixed disks
    footer[16..24].copy_from_slice(&0xFFFF_FFFF_FFFF_FFFFu64.to_be_bytes());

    // Timestamp (offset 24, 4 bytes) — seconds since 2000-01-01 00:00:00 UTC
    // VHD epoch: 2000-01-01 00:00:00 UTC = Unix timestamp 946684800
    let vhd_epoch: u64 = 946_684_800;
    let now_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let timestamp = now_unix.saturating_sub(vhd_epoch) as u32;
    footer[24..28].copy_from_slice(&timestamp.to_be_bytes());

    // Creator Application (offset 28, 4 bytes)
    footer[28..32].copy_from_slice(b"rsbk");

    // Creator Version (offset 32, 4 bytes) — 1.0
    footer[32..36].copy_from_slice(&0x0001_0000u32.to_be_bytes());

    // Creator Host OS (offset 36, 4 bytes)
    #[cfg(target_os = "windows")]
    footer[36..40].copy_from_slice(b"Wi2k");
    #[cfg(target_os = "macos")]
    footer[36..40].copy_from_slice(b"Mac ");
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    footer[36..40].copy_from_slice(b"Wi2k"); // Linux — use Windows ID (most compatible)

    // Original Size (offset 40, 8 bytes)
    footer[40..48].copy_from_slice(&data_size.to_be_bytes());

    // Current Size (offset 48, 8 bytes)
    footer[48..56].copy_from_slice(&data_size.to_be_bytes());

    // Disk Geometry (offset 56, 4 bytes): CHS packed as C(16) H(8) S(8)
    let (cylinders, heads, sectors_per_track) = vhd_chs_geometry(data_size);
    footer[56..58].copy_from_slice(&(cylinders as u16).to_be_bytes());
    footer[58] = heads as u8;
    footer[59] = sectors_per_track as u8;

    // Disk Type (offset 60, 4 bytes) — 2 = Fixed
    footer[60..64].copy_from_slice(&2u32.to_be_bytes());

    // Checksum (offset 64, 4 bytes) — computed after UUID
    // UUID (offset 68, 16 bytes) — random
    let uuid = random_uuid();
    footer[68..84].copy_from_slice(&uuid);

    // Saved State (offset 84, 1 byte) — 0
    // Reserved (offset 85..512) — already zero

    // Compute checksum: one's complement of the sum of all bytes,
    // treating the checksum field (bytes 64..68) as zero.
    let mut sum: u32 = 0;
    for (i, &b) in footer.iter().enumerate() {
        if (64..68).contains(&i) {
            continue; // skip checksum field
        }
        sum = sum.wrapping_add(b as u32);
    }
    let checksum = !sum;
    footer[64..68].copy_from_slice(&checksum.to_be_bytes());

    footer
}

/// Compute VHD CHS geometry from total disk size (in bytes) per the VHD spec.
///
/// This follows the algorithm from the Microsoft VHD specification appendix.
pub(crate) fn vhd_chs_geometry(size_bytes: u64) -> (u32, u32, u32) {
    let total_sectors = (size_bytes / 512).min(65535 * 16 * 255) as u32;

    if total_sectors == 0 {
        return (0, 0, 0);
    }

    if total_sectors >= 65535 * 16 * 63 {
        // Maximum geometry
        let spt = 255u32;
        let heads = 16u32;
        let cylinders = total_sectors / (heads * spt);
        return (cylinders, heads, spt);
    }

    let mut spt = 17u32;
    let mut cyl_times_heads = total_sectors / spt;
    let mut heads = (cyl_times_heads + 1023) / 1024;

    if heads < 4 {
        heads = 4;
    }

    if cyl_times_heads >= heads * 1024 || heads > 16 {
        spt = 31;
        heads = 16;
        cyl_times_heads = total_sectors / spt;
    }

    if cyl_times_heads >= heads * 1024 {
        spt = 63;
        heads = 16;
        cyl_times_heads = total_sectors / spt;
    }

    let cylinders = cyl_times_heads / heads;
    (cylinders, heads, spt)
}

/// Generate 16 random bytes for the VHD UUID field.
fn random_uuid() -> [u8; 16] {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut bytes = [0u8; 16];
    // Mix multiple entropy sources
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();

    let mut hasher = DefaultHasher::new();
    now.as_nanos().hash(&mut hasher);
    std::process::id().hash(&mut hasher);
    let h1 = hasher.finish();

    let mut hasher2 = DefaultHasher::new();
    (now.as_nanos() ^ 0xDEAD_BEEF_CAFE_BABE).hash(&mut hasher2);
    std::thread::current().id().hash(&mut hasher2);
    let h2 = hasher2.finish();

    bytes[0..8].copy_from_slice(&h1.to_le_bytes());
    bytes[8..16].copy_from_slice(&h2.to_le_bytes());
    bytes
}

/// Write VHD (Fixed): stream raw data to a single file, then append a 512-byte footer.
pub(crate) fn write_vhd(
    reader: &mut impl Read,
    output_base: &Path,
    skip_zeros: bool,
    progress_cb: &mut impl FnMut(u64),
    cancel_check: &impl Fn() -> bool,
) -> Result<Vec<String>> {
    use super::raw::stream_with_split;

    // Write raw data (no splitting for VHD)
    let files = stream_with_split(
        reader,
        output_base,
        "vhd",
        None,
        skip_zeros,
        progress_cb,
        cancel_check,
    )?;

    // Re-open the file to determine data size and append the VHD footer
    let vhd_path = output_base
        .parent()
        .unwrap_or(Path::new("."))
        .join(&files[0]);

    let data_size = fs::metadata(&vhd_path)
        .with_context(|| format!("failed to stat VHD file: {}", vhd_path.display()))?
        .len();

    let footer = build_vhd_footer(data_size);

    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(&vhd_path)
        .with_context(|| format!("failed to reopen VHD file: {}", vhd_path.display()))?;
    file.write_all(&footer)
        .context("failed to write VHD footer")?;
    file.flush()?;

    Ok(files)
}

/// Result of rebuilding the VHD EBR chain with repacked logical partitions.
struct VhdEbrResult {
    /// (byte_offset, 512-byte EBR sector) pairs.
    ebr_sectors: Vec<(u64, [u8; 512])>,
    /// Map from original start_lba to new start_lba for repacked logicals.
    logical_remap: Vec<(u64, u64)>,
    /// New total_sectors for the MBR extended container entry.
    new_extended_total_sectors: u32,
    /// The extended container start LBA.
    extended_start_lba: u32,
}

/// Parse the source MBR and EBR chain, then build an updated EBR chain where
/// logical partition sizes are replaced by the new sizes from `partition_sizes`.
/// When any logical partition is resized, repacks them contiguously.
///
/// Returns `None` if the source has no extended partition or its EBR cannot be
/// parsed (non-fatal: the export continues without updated EBR in that case).
fn rebuild_vhd_ebr_chain(
    source_mbr: &[u8; 512],
    source: &mut (impl Read + Seek),
    partition_sizes: &[PartitionSizeOverride],
    log_cb: &mut impl FnMut(&str),
) -> Option<VhdEbrResult> {
    // Find the extended container entry in the source MBR.
    let mbr = Mbr::parse(source_mbr).ok()?;
    let extended_entry = mbr
        .entries
        .iter()
        .find(|e| e.is_extended() && !e.is_empty())?;
    let extended_start_lba = extended_entry.start_lba;

    // Parse the source EBR chain to get current logical partition info
    // (positions and partition types).
    let source_logicals = match parse_ebr_chain(source, extended_start_lba) {
        Ok(v) if !v.is_empty() => v,
        Ok(_) => return None, // empty chain — nothing to update
        Err(e) => {
            log_cb(&format!("Warning: could not parse source EBR chain: {e}"));
            return None;
        }
    };

    // Check if any logical partition is resized
    let any_resized = source_logicals.iter().any(|src| {
        partition_sizes
            .iter()
            .find(|ps| ps.start_lba == src.start_lba as u64)
            .map(|ps| ps.export_size != src.total_sectors as u64 * 512)
            .unwrap_or(false)
    });

    // Build updated LogicalPartitionInfo entries.  When any logical is resized,
    // repack them contiguously (first keeps position, rest packed with 1-sector
    // EBR gap) — same algorithm as build_restore_ebr_chain in restore/mod.rs.
    let mut logical_infos: Vec<LogicalPartitionInfo> = Vec::with_capacity(source_logicals.len());
    let mut logical_remap: Vec<(u64, u64)> = Vec::with_capacity(source_logicals.len());
    let mut next_lba: u32 = 0;

    for (i, src) in source_logicals.iter().enumerate() {
        let new_sectors = partition_sizes
            .iter()
            .find(|ps| ps.start_lba == src.start_lba as u64)
            .map(|ps| (ps.export_size / 512) as u32)
            .unwrap_or(src.total_sectors);

        let start_lba = if i == 0 {
            // First logical: keep original position
            src.start_lba
        } else if any_resized {
            // Pack right after previous partition, leaving 1 sector for EBR
            next_lba + 1
        } else {
            src.start_lba
        };

        next_lba = start_lba + new_sectors;

        if start_lba != src.start_lba {
            logical_remap.push((src.start_lba as u64, start_lba as u64));
        }

        logical_infos.push(LogicalPartitionInfo {
            start_lba,
            total_sectors: new_sectors,
            partition_type: src.partition_type,
        });
    }

    let last_end_lba = next_lba;
    let new_extended_total_sectors = last_end_lba - extended_start_lba;

    let ebr_sectors = build_ebr_chain(extended_start_lba, &logical_infos);
    log_cb(&format!(
        "Rebuilt {} EBR sector(s) for logical partitions (extended container at LBA {}, new total_sectors {})",
        ebr_sectors.len(),
        extended_start_lba,
        new_extended_total_sectors,
    ));
    Some(VhdEbrResult {
        ebr_sectors,
        logical_remap,
        new_extended_total_sectors,
        extended_start_lba,
    })
}

/// Export a whole disk image as a Fixed VHD file.
///
/// For raw image files or devices: reconstructs the disk with partition size overrides.
/// For backup folders: reconstructs the disk from MBR + partition data files.
///
/// `source` is either a raw image/device path or a backup folder.
/// When `backup_metadata` is `Some`, the source is treated as a backup folder.
/// `partition_sizes` provides per-partition size overrides.
pub fn export_whole_disk_vhd(
    source_path: &Path,
    backup_metadata: Option<&BackupMetadata>,
    mbr_bytes: Option<&[u8; 512]>,
    partition_sizes: &[PartitionSizeOverride],
    dest_path: &Path,
    mut progress_cb: impl FnMut(u64),
    cancel_check: impl Fn() -> bool,
    mut log_cb: impl FnMut(&str),
) -> Result<()> {
    let mut total_written: u64 = 0;

    if let Some(meta) = backup_metadata {
        // Backup folder reconstruction — use File directly (not BufWriter)
        // because reconstruct_disk_from_backup needs Read + Write + Seek.
        // Must open with read+write (not O_WRONLY) because patch_bpb_hidden_sectors
        // and similar functions read back from the writer to detect filesystem type.
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dest_path)
            .with_context(|| format!("failed to create {}", dest_path.display()))?;

        total_written = reconstruct_disk_from_backup(
            source_path,
            meta,
            mbr_bytes,
            partition_sizes,
            meta.source_size_bytes,
            &mut file,
            false, // VHD export is to a file
            false, // No need to write zeros for VHD files
            None,  // VHD export doesn't write GPT structures
            None,  // VHD export doesn't write APM structures
            &mut progress_cb,
            &cancel_check,
            &mut log_cb,
        )?;

        // Append VHD footer
        let footer = build_vhd_footer(total_written);
        file.write_all(&footer)
            .context("failed to write VHD footer")?;
        file.flush()?;

        log_cb(&format!(
            "VHD export complete: {} ({} data bytes + 512 byte footer)",
            dest_path.display(),
            total_written,
        ));

        return Ok(());
    }

    // Raw image/device path — use BufWriter for streaming
    let mut writer = BufWriter::new(
        File::create(dest_path)
            .with_context(|| format!("failed to create {}", dest_path.display()))?,
    );

    // Raw image/device: reconstruct with partition size overrides
    {
        let file = File::open(source_path)
            .with_context(|| format!("failed to open {}", source_path.display()))?;
        let file_size = file.metadata()?.len();
        let mut reader = BufReader::new(file);

        // Check if this is a VHD file — if so, limit to data portion
        let source_data_size = if file_size >= 512 {
            let mut f = File::open(source_path)?;
            f.seek(SeekFrom::End(-512))?;
            let mut cookie = [0u8; 8];
            f.read_exact(&mut cookie)?;
            if &cookie == VHD_COOKIE {
                file_size - 512
            } else {
                file_size
            }
        } else {
            file_size
        };

        if partition_sizes.is_empty() {
            // No size overrides — stream the whole source
            let mut buf = vec![0u8; CHUNK_SIZE];
            let mut limited = (&mut reader).take(source_data_size);
            loop {
                if cancel_check() {
                    bail!("export cancelled");
                }
                let n = limited.read(&mut buf).context("failed to read source")?;
                if n == 0 {
                    break;
                }
                writer
                    .write_all(&buf[..n])
                    .context("failed to write VHD data")?;
                total_written += n as u64;
                progress_cb(total_written);
            }
        } else {
            // Reconstruct disk with partition size overrides
            let mut buf = vec![0u8; CHUNK_SIZE];

            // Read and patch MBR (first 512 bytes)
            let mut mbr_buf = [0u8; 512];
            reader
                .read_exact(&mut mbr_buf)
                .context("failed to read MBR from source")?;
            patch_mbr_entries(&mut mbr_buf, partition_sizes);

            // Build repacked EBR chain before writing so we know new positions.
            let ebr_result =
                rebuild_vhd_ebr_chain(&mbr_buf, &mut reader, partition_sizes, &mut log_cb);

            // Build a remap from original start_lba to new start_lba for logicals.
            let logical_remap: std::collections::HashMap<u64, u64> = ebr_result
                .as_ref()
                .map(|r| r.logical_remap.iter().copied().collect())
                .unwrap_or_default();

            // Patch extended container entry in MBR if logicals were repacked.
            if let Some(ref ebr) = ebr_result {
                let ext_start = ebr.extended_start_lba;
                let new_total = ebr.new_extended_total_sectors;
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
                        if entry_start == ext_start {
                            mbr_buf[off + 12..off + 16].copy_from_slice(&new_total.to_le_bytes());
                            log_cb(&format!(
                                "Patched MBR extended container: total_sectors = {}",
                                new_total,
                            ));
                            break;
                        }
                    }
                }
            }

            writer
                .write_all(&mbr_buf)
                .context("failed to write patched MBR")?;
            total_written += 512;
            log_cb("Patched MBR partition table with export sizes");

            // Build sorted partition list with remapped positions for logicals.
            struct PartWrite {
                source_lba: u64,
                dest_lba: u64,
                export_size: u64,
                original_size: u64,
                index: usize,
            }
            let mut sorted_parts: Vec<PartWrite> = partition_sizes
                .iter()
                .map(|ps| {
                    let dest_lba = logical_remap
                        .get(&ps.start_lba)
                        .copied()
                        .unwrap_or(ps.start_lba);
                    PartWrite {
                        source_lba: ps.start_lba,
                        dest_lba,
                        export_size: ps.export_size,
                        original_size: ps.original_size,
                        index: ps.index,
                    }
                })
                .collect();
            sorted_parts.sort_by_key(|p| p.dest_lba);

            for pw in &sorted_parts {
                if cancel_check() {
                    bail!("export cancelled");
                }

                let dest_offset = pw.dest_lba * 512;
                let source_offset = pw.source_lba * 512;

                // Fill gap between current position and this partition's dest
                if total_written < dest_offset {
                    let gap = dest_offset - total_written;
                    if pw.source_lba == pw.dest_lba {
                        // Not remapped: copy source data for the gap
                        reader.seek(SeekFrom::Start(total_written))?;
                        let mut gap_reader = (&mut reader).take(gap);
                        let mut gap_remaining = gap;
                        while gap_remaining > 0 {
                            let to_read = (gap_remaining as usize).min(CHUNK_SIZE);
                            let n = gap_reader.read(&mut buf[..to_read])?;
                            if n == 0 {
                                write_zeros(&mut writer, gap_remaining)?;
                                total_written += gap_remaining;
                                break;
                            }
                            writer.write_all(&buf[..n])?;
                            total_written += n as u64;
                            gap_remaining -= n as u64;
                            progress_cb(total_written);
                        }
                    } else {
                        // Remapped: zero-fill the gap (no meaningful source data)
                        write_zeros(&mut writer, gap)?;
                        total_written += gap;
                    }
                }

                // Write partition data (read from source position, write at dest)
                reader.seek(SeekFrom::Start(source_offset))?;
                let copy_size = pw.export_size.min(pw.original_size);
                let mut part_reader = (&mut reader).take(copy_size);
                let mut part_remaining = copy_size;
                while part_remaining > 0 {
                    if cancel_check() {
                        bail!("export cancelled");
                    }
                    let to_read = (part_remaining as usize).min(CHUNK_SIZE);
                    let n = part_reader.read(&mut buf[..to_read])?;
                    if n == 0 {
                        break;
                    }
                    writer.write_all(&buf[..n])?;
                    total_written += n as u64;
                    part_remaining -= n as u64;
                    progress_cb(total_written);
                }

                // If export_size > data copied (e.g. original), pad
                if pw.export_size > copy_size {
                    let pad = pw.export_size - copy_size;
                    write_zeros(&mut writer, pad)?;
                    total_written += pad;
                }

                // Update hidden sectors for all filesystem types
                {
                    writer.flush()?;
                    let end_pos = total_written;
                    patch_bpb_hidden_sectors(
                        writer.get_mut(),
                        dest_offset,
                        pw.dest_lba,
                        &mut log_cb,
                    )?;
                    patch_ntfs_hidden_sectors(
                        writer.get_mut(),
                        dest_offset,
                        pw.dest_lba,
                        &mut log_cb,
                    )?;
                    patch_exfat_hidden_sectors(
                        writer.get_mut(),
                        dest_offset,
                        pw.dest_lba,
                        &mut log_cb,
                    )?;
                    patch_hfs_hidden_sectors(
                        writer.get_mut(),
                        dest_offset,
                        pw.dest_lba,
                        &mut log_cb,
                    )?;
                    patch_hfsplus_hidden_sectors(
                        writer.get_mut(),
                        dest_offset,
                        pw.dest_lba,
                        &mut log_cb,
                    )?;
                    writer.seek(SeekFrom::Start(end_pos))?;
                }

                // Resize filesystem if the partition size changed
                if pw.export_size != pw.original_size {
                    writer.flush()?;
                    let end_pos = total_written;
                    let new_sectors = (pw.export_size / 512) as u32;
                    let new_sectors_u64 = pw.export_size / 512;
                    resize_fat_in_place(writer.get_mut(), dest_offset, new_sectors, &mut log_cb)?;
                    resize_ntfs_in_place(
                        writer.get_mut(),
                        dest_offset,
                        new_sectors_u64,
                        &mut log_cb,
                    )?;
                    resize_exfat_in_place(
                        writer.get_mut(),
                        dest_offset,
                        new_sectors_u64,
                        &mut log_cb,
                    )?;
                    resize_hfs_in_place(
                        writer.get_mut(),
                        dest_offset,
                        pw.export_size,
                        &mut log_cb,
                    )?;
                    resize_hfsplus_in_place(
                        writer.get_mut(),
                        dest_offset,
                        pw.export_size,
                        &mut log_cb,
                    )?;
                    resize_ext_in_place(
                        writer.get_mut(),
                        dest_offset,
                        pw.export_size,
                        &mut log_cb,
                    )?;
                    resize_btrfs_in_place(
                        writer.get_mut(),
                        dest_offset,
                        pw.export_size,
                        &mut log_cb,
                    )?;
                    writer.seek(SeekFrom::Start(end_pos))?;
                }

                log_cb(&format!(
                    "partition-{}: exported {} bytes at LBA {}",
                    pw.index, pw.export_size, pw.dest_lba,
                ));
            }

            // Compute the actual data end: max of all partition ends.
            let new_data_end = sorted_parts
                .iter()
                .map(|pw| pw.dest_lba * 512 + pw.export_size)
                .max()
                .unwrap_or(total_written);
            let effective_source_end = source_data_size.min(new_data_end);

            // Copy any remaining data after the last partition up to the
            // effective end (truncated when logicals were repacked).
            if total_written < effective_source_end {
                reader.seek(SeekFrom::Start(total_written))?;
                let remaining = effective_source_end - total_written;
                let mut tail_reader = (&mut reader).take(remaining);
                loop {
                    if cancel_check() {
                        bail!("export cancelled");
                    }
                    let n = tail_reader.read(&mut buf)?;
                    if n == 0 {
                        break;
                    }
                    writer.write_all(&buf[..n])?;
                    total_written += n as u64;
                    progress_cb(total_written);
                }
            }

            // Write the rebuilt EBR chain sectors into the output.
            if let Some(ref ebr) = ebr_result {
                for (ebr_offset, ebr_sector) in &ebr.ebr_sectors {
                    writer.seek(SeekFrom::Start(*ebr_offset))?;
                    writer.write_all(ebr_sector)?;
                }
                // Restore write position to data end for the footer.
                writer.seek(SeekFrom::Start(total_written))?;
            }
        }
    }

    writer.flush()?;

    // Append VHD footer
    let footer = build_vhd_footer(total_written);
    writer
        .write_all(&footer)
        .context("failed to write VHD footer")?;
    writer.flush()?;

    log_cb(&format!(
        "VHD export complete: {} ({} data bytes + 512 byte footer)",
        dest_path.display(),
        total_written,
    ));

    Ok(())
}

/// Export a single partition as a Fixed VHD file.
///
/// Handles raw, zstd, and CHD compressed partition files.
/// If `max_bytes` is `Some(n)`, the output is limited to at most `n` bytes of data.
pub fn export_partition_vhd(
    source_path: &Path,
    compression_type: &str,
    dest_path: &Path,
    max_bytes: Option<u64>,
    mut progress_cb: impl FnMut(u64),
    cancel_check: impl Fn() -> bool,
    mut log_cb: impl FnMut(&str),
) -> Result<()> {
    let mut writer = BufWriter::new(
        File::create(dest_path)
            .with_context(|| format!("failed to create {}", dest_path.display()))?,
    );

    let bytes_written = decompress_to_writer(
        source_path,
        compression_type,
        &mut writer,
        max_bytes,
        &mut progress_cb,
        &cancel_check,
        &mut log_cb,
    )?;

    writer.flush()?;

    // Use the requested size for the VHD footer if specified (may be larger than
    // the decompressed data to create a properly-sized VHD image).
    let vhd_data_size = max_bytes.unwrap_or(bytes_written).max(bytes_written);

    // Pad with zeros if we wrote less than the requested size
    if bytes_written < vhd_data_size {
        let pad = vhd_data_size - bytes_written;
        write_zeros(&mut writer, pad)?;
    }

    // Append VHD footer
    let footer = build_vhd_footer(vhd_data_size);
    writer
        .write_all(&footer)
        .context("failed to write VHD footer")?;
    writer.flush()?;

    log_cb(&format!(
        "VHD partition export complete: {} ({} data bytes)",
        dest_path.display(),
        vhd_data_size,
    ));

    Ok(())
}

/// Export a whole disk from a Clonezilla image as a Fixed VHD file.
///
/// Reconstructs the disk by writing the MBR, hidden data gap, EBR for extended
/// partitions, and each partition's data from partclone, then appends a VHD footer.
pub fn export_clonezilla_disk_vhd(
    cz_image: &crate::clonezilla::metadata::ClonezillaImage,
    _backup_folder: &Path,
    output_path: &Path,
    partition_sizes: &[PartitionSizeOverride],
    mut progress_cb: impl FnMut(u64),
    cancel_check: impl Fn() -> bool,
    mut log_cb: impl FnMut(&str),
) -> Result<()> {
    use crate::clonezilla::partclone::open_partclone_reader;

    let mut file = File::create(output_path)
        .with_context(|| format!("failed to create {}", output_path.display()))?;

    let mut total_written: u64 = 0;

    // Helper to look up export size
    let get_export_size = |index: usize, default: u64| -> u64 {
        partition_sizes
            .iter()
            .find(|ps| ps.index == index)
            .map(|ps| ps.export_size)
            .unwrap_or(default)
    };

    // Write MBR
    let mut mbr_buf = cz_image.mbr_bytes;
    if !partition_sizes.is_empty() {
        patch_mbr_entries(&mut mbr_buf, partition_sizes);
        log_cb("Patched MBR partition table with export sizes");
    }
    file.write_all(&mbr_buf).context("failed to write MBR")?;
    total_written += 512;

    // Write hidden data after MBR
    if !cz_image.hidden_data_after_mbr.is_empty() {
        file.write_all(&cz_image.hidden_data_after_mbr)
            .context("failed to write hidden data after MBR")?;
        total_written += cz_image.hidden_data_after_mbr.len() as u64;
        log_cb(&format!(
            "Wrote {} bytes of hidden data after MBR",
            cz_image.hidden_data_after_mbr.len()
        ));
    }

    // Sort partitions by start LBA
    let mut sorted_parts: Vec<&crate::clonezilla::metadata::ClonezillaPartition> =
        cz_image.partitions.iter().collect();
    sorted_parts.sort_by_key(|p| p.start_lba);

    for cz_part in &sorted_parts {
        if cancel_check() {
            bail!("export cancelled");
        }

        let part_offset = cz_part.start_lba * 512;
        let export_size = get_export_size(cz_part.index, cz_part.size_bytes());

        // Fill gap to partition start
        if total_written < part_offset {
            let gap = part_offset - total_written;

            // Write EBR if this is an extended partition and we have EBR data
            if let Some(ebr_data) = cz_image.ebr_data.get(&cz_part.device_name) {
                // EBR sits at the partition's start LBA
                let ebr_gap = part_offset - total_written - ebr_data.len().min(512) as u64;
                if ebr_gap > 0 {
                    super::write_zeros(&mut file, ebr_gap)?;
                    total_written += ebr_gap;
                }
                let write_len = ebr_data.len().min(512);
                file.write_all(&ebr_data[..write_len])?;
                total_written += write_len as u64;
            } else {
                super::write_zeros(&mut file, gap)?;
                total_written += gap;
            }
        }

        // Handle extended container partitions (no data to write)
        if cz_part.is_extended {
            continue;
        }

        // Write partition data from partclone
        if cz_part.partclone_files.is_empty() {
            log_cb(&format!(
                "partition-{}: no data files, filling with zeros",
                cz_part.index
            ));
            super::write_zeros(&mut file, export_size)?;
            total_written += export_size;
            continue;
        }

        log_cb(&format!(
            "partition-{}: decompressing partclone data...",
            cz_part.index
        ));

        let (_header, mut reader) = open_partclone_reader(&cz_part.partclone_files)?;

        let mut buf = vec![0u8; super::CHUNK_SIZE];
        let mut part_written: u64 = 0;
        let copy_limit = export_size.min(cz_part.size_bytes());
        let mut limited = (&mut reader).take(copy_limit);

        loop {
            if cancel_check() {
                bail!("export cancelled");
            }
            let n = limited.read(&mut buf)?;
            if n == 0 {
                break;
            }
            file.write_all(&buf[..n])?;
            part_written += n as u64;
            total_written += n as u64;
            progress_cb(total_written);
        }

        // Pad if export_size > data written
        if part_written < export_size {
            let pad = export_size - part_written;
            super::write_zeros(&mut file, pad)?;
            total_written += pad;
        }

        // Patch hidden sectors and resize filesystem
        {
            let effective_lba = partition_sizes
                .iter()
                .find(|ps| ps.index == cz_part.index)
                .map(|ps| ps.effective_start_lba())
                .unwrap_or(cz_part.start_lba);

            patch_bpb_hidden_sectors(&mut file, part_offset, effective_lba, &mut log_cb)?;
            patch_ntfs_hidden_sectors(&mut file, part_offset, effective_lba, &mut log_cb)?;
            patch_exfat_hidden_sectors(&mut file, part_offset, effective_lba, &mut log_cb)?;
            patch_hfs_hidden_sectors(&mut file, part_offset, effective_lba, &mut log_cb)?;
            patch_hfsplus_hidden_sectors(&mut file, part_offset, effective_lba, &mut log_cb)?;

            if export_size != cz_part.size_bytes() {
                let new_sectors = (export_size / 512) as u32;
                let new_sectors_u64 = export_size / 512;
                resize_fat_in_place(&mut file, part_offset, new_sectors, &mut log_cb)?;
                resize_ntfs_in_place(&mut file, part_offset, new_sectors_u64, &mut log_cb)?;
                resize_exfat_in_place(&mut file, part_offset, new_sectors_u64, &mut log_cb)?;
                resize_hfs_in_place(&mut file, part_offset, export_size, &mut log_cb)?;
                resize_hfsplus_in_place(&mut file, part_offset, export_size, &mut log_cb)?;
                resize_ext_in_place(&mut file, part_offset, export_size, &mut log_cb)?;
                resize_btrfs_in_place(&mut file, part_offset, export_size, &mut log_cb)?;
            }

            // Seek back to end for next partition
            file.seek(SeekFrom::Start(total_written))?;
        }

        log_cb(&format!(
            "partition-{}: wrote {} bytes",
            cz_part.index, export_size,
        ));
    }

    // Append VHD footer
    let footer = build_vhd_footer(total_written);
    file.write_all(&footer)
        .context("failed to write VHD footer")?;
    file.flush()?;

    log_cb(&format!(
        "VHD export complete: {} ({} data bytes + 512 byte footer)",
        output_path.display(),
        total_written,
    ));

    Ok(())
}

/// Export a single partition from a Clonezilla image as a Fixed VHD file.
pub fn export_clonezilla_partition_vhd(
    partclone_files: &[std::path::PathBuf],
    dest_path: &Path,
    export_size: Option<u64>,
    mut progress_cb: impl FnMut(u64),
    cancel_check: impl Fn() -> bool,
    mut log_cb: impl FnMut(&str),
) -> Result<()> {
    use crate::clonezilla::partclone::open_partclone_reader;

    let (header, mut reader) = open_partclone_reader(partclone_files)?;
    let partition_size = header.partition_size();
    let max_bytes = export_size.unwrap_or(partition_size);

    let mut writer = BufWriter::new(
        File::create(dest_path)
            .with_context(|| format!("failed to create {}", dest_path.display()))?,
    );

    let mut buf = vec![0u8; super::CHUNK_SIZE];
    let mut total_written: u64 = 0;
    let mut limited = (&mut reader).take(max_bytes);

    loop {
        if cancel_check() {
            bail!("export cancelled");
        }
        let n = limited.read(&mut buf)?;
        if n == 0 {
            break;
        }
        writer.write_all(&buf[..n])?;
        total_written += n as u64;
        progress_cb(total_written);
    }

    // Pad if needed
    if total_written < max_bytes {
        let pad = max_bytes - total_written;
        super::write_zeros(&mut writer, pad)?;
        total_written = max_bytes;
    }

    writer.flush()?;

    // Append VHD footer
    let footer = build_vhd_footer(total_written);
    writer
        .write_all(&footer)
        .context("failed to write VHD footer")?;
    writer.flush()?;

    log_cb(&format!(
        "VHD partition export complete: {} ({} data bytes)",
        dest_path.display(),
        total_written,
    ));

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::CompressionType;
    use super::*;
    use std::io::Cursor;
    use tempfile::TempDir;

    #[test]
    fn test_write_vhd() {
        let tmp = TempDir::new().unwrap();
        let data = vec![0xABu8; 4096];
        let mut reader = Cursor::new(&data);
        let base = tmp.path().join("partition-0");

        let files = super::super::compress_partition(
            &mut reader,
            &base,
            CompressionType::Vhd,
            None,
            false,
            |_| {},
            || false,
            |_| {},
        )
        .unwrap();

        assert_eq!(files, vec!["partition-0.vhd"]);
        let written = fs::read(tmp.path().join("partition-0.vhd")).unwrap();
        // Raw data (4096) + VHD footer (512)
        assert_eq!(written.len(), 4096 + 512);

        // Verify footer starts with "conectix" cookie
        let footer = &written[4096..];
        assert_eq!(&footer[0..8], b"conectix");

        // Verify data size fields (Original Size at offset 40, Current Size at offset 48)
        let orig_size = u64::from_be_bytes(footer[40..48].try_into().unwrap());
        assert_eq!(orig_size, 4096);
        let curr_size = u64::from_be_bytes(footer[48..56].try_into().unwrap());
        assert_eq!(curr_size, 4096);

        // Verify disk type = 2 (Fixed)
        let disk_type = u32::from_be_bytes(footer[60..64].try_into().unwrap());
        assert_eq!(disk_type, 2);

        // Verify checksum
        let stored_checksum = u32::from_be_bytes(footer[64..68].try_into().unwrap());
        let mut sum: u32 = 0;
        for (i, &b) in footer.iter().enumerate() {
            if (64..68).contains(&i) {
                continue;
            }
            sum = sum.wrapping_add(b as u32);
        }
        assert_eq!(stored_checksum, !sum);
    }

    #[test]
    fn test_build_vhd_footer_cookie_and_checksum() {
        let footer = build_vhd_footer(1024 * 1024); // 1 MB
        assert_eq!(&footer[0..8], b"conectix");

        // Verify checksum is valid
        let stored = u32::from_be_bytes(footer[64..68].try_into().unwrap());
        let mut sum: u32 = 0;
        for (i, &b) in footer.iter().enumerate() {
            if (64..68).contains(&i) {
                continue;
            }
            sum = sum.wrapping_add(b as u32);
        }
        assert_eq!(stored, !sum);
    }

    #[test]
    fn test_vhd_chs_geometry() {
        // Small disk: 100 MB
        let (c, h, s) = vhd_chs_geometry(100 * 1024 * 1024);
        assert!(c > 0 && h > 0 && s > 0);

        // Large disk: 8 GB
        let (c, h, s) = vhd_chs_geometry(8 * 1024 * 1024 * 1024);
        assert!(c > 0 && h > 0 && s > 0);

        // Zero size
        let (c, h, s) = vhd_chs_geometry(0);
        assert_eq!((c, h, s), (0, 0, 0));
    }
}
