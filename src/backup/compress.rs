use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};

use super::CompressionType;

const CHUNK_SIZE: usize = 256 * 1024; // 256 KB I/O buffer

/// VHD cookie identifying a valid VHD footer.
pub const VHD_COOKIE: &[u8; 8] = b"conectix";

/// Compress partition data from `reader` to `output_base` using the given compression method.
///
/// If `split_size` is `Some(bytes)`, output files are split at that boundary.
/// Returns the list of output file names (relative, e.g. `partition-0.zst`).
///
/// `progress_cb` is called with the number of bytes read so far from the source.
/// `cancel_check` returns true if the operation should abort.
pub fn compress_partition(
    reader: &mut impl Read,
    output_base: &Path,
    compression: CompressionType,
    split_size: Option<u64>,
    skip_zeros: bool,
    mut progress_cb: impl FnMut(u64),
    cancel_check: impl Fn() -> bool,
    mut log_cb: impl FnMut(&str),
) -> Result<Vec<String>> {
    match compression {
        CompressionType::None => {
            stream_with_split(reader, output_base, "raw", split_size, skip_zeros, &mut progress_cb, &cancel_check)
        }
        CompressionType::Vhd => {
            // VHD = raw data + 512-byte footer; no splitting support
            write_vhd(reader, output_base, skip_zeros, &mut progress_cb, &cancel_check)
        }
        CompressionType::Zstd => {
            // zstd compresses zero blocks efficiently; no need to skip
            compress_zstd(reader, output_base, split_size, &mut progress_cb, &cancel_check)
        }
        CompressionType::Chd => {
            // CHD needs complete raw temp file; chdman handles zero compression
            compress_chd(reader, output_base, split_size, &mut progress_cb, &cancel_check, &mut log_cb)
        }
    }
}

/// Detect whether `chdman` is available on PATH.
pub fn detect_chdman() -> bool {
    Command::new("chdman")
        .arg("help")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok()
}

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
fn vhd_chs_geometry(size_bytes: u64) -> (u32, u32, u32) {
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
fn write_vhd(
    reader: &mut impl Read,
    output_base: &Path,
    skip_zeros: bool,
    progress_cb: &mut impl FnMut(u64),
    cancel_check: &impl Fn() -> bool,
) -> Result<Vec<String>> {
    // Write raw data (no splitting for VHD)
    let files = stream_with_split(
        reader, output_base, "vhd", None, skip_zeros, progress_cb, cancel_check,
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

/// Partition size override for VHD export.
pub struct PartitionSizeOverride {
    pub index: usize,
    pub start_lba: u64,
    pub original_size: u64,
    pub export_size: u64,
}

/// Patch MBR partition table entries with new total_sectors values.
///
/// The MBR has 4 partition entries starting at byte 446, each 16 bytes.
/// Bytes 12-15 of each entry are the total_sectors field (little-endian u32).
/// This function modifies those fields based on the provided partition size overrides.
fn patch_mbr_partition_sizes(mbr: &mut [u8; 512], overrides: &[PartitionSizeOverride]) {
    const PARTITION_TABLE_OFFSET: usize = 446;
    const ENTRY_SIZE: usize = 16;

    for ps in overrides {
        // Primary partitions are entries 0-3 in the MBR
        if ps.index > 3 {
            continue; // Logical partitions are in EBRs, not the MBR
        }

        let entry_offset = PARTITION_TABLE_OFFSET + ps.index * ENTRY_SIZE;
        if entry_offset + ENTRY_SIZE > 512 {
            continue;
        }

        // Read current start_lba to verify we're patching the right entry
        let current_start_lba = u32::from_le_bytes([
            mbr[entry_offset + 8],
            mbr[entry_offset + 9],
            mbr[entry_offset + 10],
            mbr[entry_offset + 11],
        ]);

        if current_start_lba as u64 != ps.start_lba {
            continue; // Safety check: don't patch if start LBA doesn't match
        }

        // Patch total_sectors
        let new_sectors = (ps.export_size / 512) as u32;
        let bytes = new_sectors.to_le_bytes();
        mbr[entry_offset + 12] = bytes[0];
        mbr[entry_offset + 13] = bytes[1];
        mbr[entry_offset + 14] = bytes[2];
        mbr[entry_offset + 15] = bytes[3];
    }
}

/// Resize a FAT12/16/32 filesystem in-place within an output file.
///
/// The partition data must already be written starting at `partition_offset`.
/// This function:
///
/// - For shrinking: updates BPB `total_sectors` only (the oversized FAT is harmless)
/// - For growing: extends FAT tables with free cluster entries, shifting the
///   data region forward if the FAT needs additional sectors, then updates BPB
/// - For FAT32: also updates the backup BPB at sector 6 and the FSInfo sector
///
/// Silently returns `Ok(false)` for non-FAT partitions.
/// Returns `Ok(true)` if the resize was performed.
pub fn resize_fat_in_place(
    file: &mut (impl Read + Write + Seek),
    partition_offset: u64,
    new_total_sectors: u32,
    log_cb: &mut impl FnMut(&str),
) -> Result<bool> {
    // --- 1. Read and validate BPB ---
    file.seek(SeekFrom::Start(partition_offset))?;
    let mut bpb = [0u8; 512];
    file.read_exact(&mut bpb)?;

    if bpb[0] != 0xEB && bpb[0] != 0xE9 {
        return Ok(false); // Not a FAT BPB
    }

    let bytes_per_sector = u16::from_le_bytes([bpb[11], bpb[12]]);
    if !matches!(bytes_per_sector, 512 | 1024 | 2048 | 4096) {
        return Ok(false);
    }
    let bps = bytes_per_sector as u64;

    let sectors_per_cluster = bpb[13];
    if sectors_per_cluster == 0 || !sectors_per_cluster.is_power_of_two() {
        return Ok(false);
    }
    let spc = sectors_per_cluster as u32;

    let reserved_sectors = u16::from_le_bytes([bpb[14], bpb[15]]) as u32;
    let num_fats = bpb[16] as u32;
    if num_fats == 0 || num_fats > 2 {
        return Ok(false);
    }

    let root_entry_count = u16::from_le_bytes([bpb[17], bpb[18]]);
    let ts16 = u16::from_le_bytes([bpb[19], bpb[20]]);
    let spf16 = u16::from_le_bytes([bpb[22], bpb[23]]);
    let ts32 = u32::from_le_bytes([bpb[32], bpb[33], bpb[34], bpb[35]]);
    let spf32 = u32::from_le_bytes([bpb[36], bpb[37], bpb[38], bpb[39]]);

    let is_fat32 = spf16 == 0 && root_entry_count == 0;
    let old_spf = if is_fat32 { spf32 } else { spf16 as u32 };
    let old_total = if ts16 != 0 { ts16 as u32 } else { ts32 };

    if old_total == new_total_sectors {
        return Ok(false); // Nothing to do
    }

    let root_dir_sectors = if is_fat32 {
        0u32
    } else {
        ((root_entry_count as u32 * 32) + (bytes_per_sector as u32 - 1))
            / bytes_per_sector as u32
    };

    // --- 2. Calculate old layout ---
    let old_data_start = reserved_sectors + num_fats * old_spf + root_dir_sectors;
    let old_data_sectors = old_total.saturating_sub(old_data_start);
    let old_clusters = old_data_sectors / spc;

    let fat_bits: u32 = if is_fat32 {
        32
    } else if old_clusters < 4085 {
        12
    } else {
        16
    };

    // --- 3. Calculate new layout ---
    let new_spf = compute_fat_sectors(
        new_total_sectors, reserved_sectors, num_fats,
        root_dir_sectors, spc, fat_bits, bytes_per_sector,
    );
    let new_data_start = reserved_sectors + num_fats * new_spf + root_dir_sectors;
    let new_data_sectors = new_total_sectors.saturating_sub(new_data_start);
    let new_clusters = new_data_sectors / spc;

    // Verify FAT type doesn't change
    let new_fat_bits = if is_fat32 {
        32
    } else if new_clusters < 4085 {
        12
    } else {
        16
    };
    if new_fat_bits != fat_bits {
        log_cb(&format!(
            "FAT resize: type would change from FAT{} to FAT{}, updating BPB only",
            fat_bits, new_fat_bits,
        ));
        patch_bpb_total_sectors(&mut bpb, new_total_sectors, ts16);
        write_bpb(file, partition_offset, &bpb, is_fat32, bytes_per_sector)?;
        return Ok(true);
    }

    let growing = new_total_sectors > old_total;
    let fat_needs_growth = new_spf > old_spf;

    log_cb(&format!(
        "FAT{}: clusters {} -> {}, spf {} -> {}",
        fat_bits, old_clusters, new_clusters, old_spf, new_spf,
    ));

    // --- 4. Growing with FAT growth: shift data + extend FAT ---
    if growing && fat_needs_growth {
        let shift_sectors = (new_spf - old_spf) * num_fats;
        let shift_bytes = shift_sectors as u64 * bps;

        // Read old FAT data (one copy — both copies are identical)
        let old_fat_start = partition_offset + reserved_sectors as u64 * bps;
        file.seek(SeekFrom::Start(old_fat_start))?;
        let old_fat_bytes = old_spf as usize * bps as usize;
        let mut fat_data = vec![0u8; old_fat_bytes];
        file.read_exact(&mut fat_data)?;

        // Shift rootdir (FAT12/16) + data region forward to make room for larger FATs
        let move_start_sector = reserved_sectors + num_fats * old_spf;
        let move_start = partition_offset + move_start_sector as u64 * bps;
        let move_end = partition_offset + old_total as u64 * bps;

        if move_end > move_start {
            shift_region_forward(file, move_start, move_end, shift_bytes)?;
            log_cb(&format!(
                "Shifted data region forward by {} sectors",
                shift_sectors,
            ));
        }

        // Extend FAT data with free entries (zero = free for all FAT types)
        let new_fat_bytes = new_spf as usize * bps as usize;
        fat_data.resize(new_fat_bytes, 0);

        // Write extended FAT to each copy
        for i in 0..num_fats as u64 {
            let fat_pos = partition_offset
                + reserved_sectors as u64 * bps
                + i * new_fat_bytes as u64;
            file.seek(SeekFrom::Start(fat_pos))?;
            file.write_all(&fat_data)?;
        }

        log_cb(&format!(
            "Extended FAT: {} -> {} sectors per copy",
            old_spf, new_spf,
        ));
    } else if growing {
        log_cb("FAT has spare capacity, no table extension needed");
    }

    // --- 5. Update BPB ---
    patch_bpb_total_sectors(&mut bpb, new_total_sectors, ts16);
    if new_spf != old_spf {
        if is_fat32 {
            bpb[36..40].copy_from_slice(&new_spf.to_le_bytes());
        } else {
            bpb[22..24].copy_from_slice(&(new_spf as u16).to_le_bytes());
        }
    }
    write_bpb(file, partition_offset, &bpb, is_fat32, bytes_per_sector)?;

    // --- 6. FAT32: update FSInfo ---
    if is_fat32 {
        let fsinfo_sector = u16::from_le_bytes([bpb[48], bpb[49]]);
        if fsinfo_sector > 0 && (fsinfo_sector as u32) < reserved_sectors {
            let fsinfo_offset = partition_offset + fsinfo_sector as u64 * bps;
            file.seek(SeekFrom::Start(fsinfo_offset))?;
            let mut fsinfo = [0u8; 512];
            file.read_exact(&mut fsinfo)?;

            let sig1 = u32::from_le_bytes(fsinfo[0..4].try_into().unwrap());
            let sig2 = u32::from_le_bytes(fsinfo[484..488].try_into().unwrap());
            if sig1 == 0x41615252 && sig2 == 0x61417272 {
                // Set free cluster count to unknown (OS will recompute on mount)
                fsinfo[488..492].copy_from_slice(&0xFFFF_FFFFu32.to_le_bytes());
                // Next free cluster hint
                if new_clusters > old_clusters {
                    fsinfo[492..496].copy_from_slice(&(old_clusters + 2).to_le_bytes());
                } else {
                    fsinfo[492..496].copy_from_slice(&0xFFFF_FFFFu32.to_le_bytes());
                }

                file.seek(SeekFrom::Start(fsinfo_offset))?;
                file.write_all(&fsinfo)?;
                log_cb("Updated FAT32 FSInfo sector");
            }
        }
    }

    file.flush()?;
    log_cb(&format!(
        "FAT{} resize complete: {} clusters, {} total sectors",
        fat_bits, new_clusters, new_total_sectors,
    ));

    Ok(true)
}

/// Patch BPB total_sectors fields (16-bit or 32-bit) in a BPB buffer.
fn patch_bpb_total_sectors(bpb: &mut [u8; 512], new_total: u32, old_ts16: u16) {
    if old_ts16 != 0 && new_total <= u16::MAX as u32 {
        bpb[19..21].copy_from_slice(&(new_total as u16).to_le_bytes());
        bpb[32..36].copy_from_slice(&0u32.to_le_bytes());
    } else {
        bpb[19..21].copy_from_slice(&0u16.to_le_bytes());
        bpb[32..36].copy_from_slice(&new_total.to_le_bytes());
    }
}

/// Write a BPB to the primary boot sector and (for FAT32) the backup at sector 6.
fn write_bpb(
    file: &mut (impl Write + Seek),
    partition_offset: u64,
    bpb: &[u8; 512],
    is_fat32: bool,
    bytes_per_sector: u16,
) -> Result<()> {
    file.seek(SeekFrom::Start(partition_offset))?;
    file.write_all(bpb)?;
    if is_fat32 {
        let backup = partition_offset + 6 * bytes_per_sector as u64;
        file.seek(SeekFrom::Start(backup))?;
        file.write_all(bpb)?;
    }
    Ok(())
}

/// Compute the number of sectors needed for one FAT copy given the partition
/// parameters and FAT type.
fn compute_fat_sectors(
    total_sectors: u32,
    reserved: u32,
    num_fats: u32,
    root_dir_sectors: u32,
    sectors_per_cluster: u32,
    fat_bits: u32,
    bytes_per_sector: u16,
) -> u32 {
    let avail = total_sectors.saturating_sub(reserved + root_dir_sectors) as u64;
    let bps = bytes_per_sector as u64;
    let spc = sectors_per_cluster as u64;
    let n = num_fats as u64;

    match fat_bits {
        12 => {
            // FAT12: 1.5 bytes per entry — use iterative approach
            let mut spf = 1u32;
            loop {
                let data_sectors = avail.saturating_sub(n * spf as u64);
                let clusters = data_sectors / spc;
                let fat_bytes = ((clusters + 2) * 3 + 1) / 2;
                let needed = ((fat_bytes + bps - 1) / bps) as u32;
                if needed <= spf {
                    return spf;
                }
                spf = needed;
            }
        }
        16 => {
            // FAT16: 2 bytes per entry
            // Closed-form: ceil(2 * (avail + 2*spc) / (bps*spc + 2*n))
            let num = 2 * (avail + 2 * spc);
            let den = bps * spc + 2 * n;
            ((num + den - 1) / den) as u32
        }
        32 => {
            // FAT32: 4 bytes per entry
            let num = 4 * (avail + 2 * spc);
            let den = bps * spc + 4 * n;
            ((num + den - 1) / den) as u32
        }
        _ => 1,
    }
}

/// Shift a region of a file forward by `shift` bytes.
/// Reads backward from the end to avoid overwriting unread data.
fn shift_region_forward(
    file: &mut (impl Read + Write + Seek),
    src_start: u64,
    src_end: u64,
    shift: u64,
) -> Result<()> {
    let data_len = src_end.saturating_sub(src_start);
    if data_len == 0 || shift == 0 {
        return Ok(());
    }

    let mut buf = vec![0u8; CHUNK_SIZE];
    let mut remaining = data_len;

    // Copy backward: read from the end, write to offset + shift
    while remaining > 0 {
        let chunk = remaining.min(CHUNK_SIZE as u64);
        let read_pos = src_start + remaining - chunk;

        file.seek(SeekFrom::Start(read_pos))?;
        file.read_exact(&mut buf[..chunk as usize])?;

        file.seek(SeekFrom::Start(read_pos + shift))?;
        file.write_all(&buf[..chunk as usize])?;

        remaining -= chunk;
    }

    // Zero-fill the gap left by the shift
    let zeros = vec![0u8; CHUNK_SIZE];
    let mut gap = shift;
    file.seek(SeekFrom::Start(src_start))?;
    while gap > 0 {
        let n = (gap as usize).min(CHUNK_SIZE);
        file.write_all(&zeros[..n])?;
        gap -= n as u64;
    }

    Ok(())
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
    backup_metadata: Option<&super::metadata::BackupMetadata>,
    mbr_bytes: Option<&[u8; 512]>,
    partition_sizes: &[PartitionSizeOverride],
    dest_path: &Path,
    mut progress_cb: impl FnMut(u64),
    cancel_check: impl Fn() -> bool,
    mut log_cb: impl FnMut(&str),
) -> Result<()> {
    let mut writer = BufWriter::new(
        File::create(dest_path)
            .with_context(|| format!("failed to create {}", dest_path.display()))?,
    );
    let mut total_written: u64 = 0;

    // Helper to look up export size for a partition index
    let get_export_size = |index: usize, default: u64| -> u64 {
        partition_sizes
            .iter()
            .find(|ps| ps.index == index)
            .map(|ps| ps.export_size)
            .unwrap_or(default)
    };

    if let Some(meta) = backup_metadata {
        // Backup folder reconstruction
        let folder = source_path;
        let disk_size = meta.source_size_bytes;

        // Write MBR (first 512 bytes), patching partition sizes if needed
        let mut mbr_buf = if let Some(mbr) = mbr_bytes {
            *mbr
        } else {
            let mbr_path = folder.join("mbr.bin");
            if mbr_path.exists() {
                let data = fs::read(&mbr_path).context("failed to read mbr.bin")?;
                let mut buf = [0u8; 512];
                let copy_len = data.len().min(512);
                buf[..copy_len].copy_from_slice(&data[..copy_len]);
                buf
            } else {
                bail!("no MBR data available for whole-disk export");
            }
        };
        if !partition_sizes.is_empty() {
            patch_mbr_partition_sizes(&mut mbr_buf, partition_sizes);
            log_cb("Patched MBR partition table with export sizes");
        }
        writer.write_all(&mbr_buf).context("failed to write MBR")?;
        total_written += 512;

        // Write each partition at its correct offset, filling gaps with zeros
        for pm in &meta.partitions {
            if cancel_check() {
                bail!("export cancelled");
            }

            let part_offset = pm.start_lba * 512;
            let export_size = get_export_size(pm.index, pm.original_size_bytes);

            // Fill gap between current position and partition start
            if total_written < part_offset {
                let gap = part_offset - total_written;
                write_zeros(&mut writer, gap)?;
                total_written += gap;
            }

            // Write partition data
            if pm.compressed_files.is_empty() {
                log_cb(&format!("partition-{}: no data files, skipping", pm.index));
                continue;
            }

            let data_file = &pm.compressed_files[0];
            let data_path = folder.join(data_file);

            if !data_path.exists() {
                log_cb(&format!(
                    "partition-{}: data file not found: {}, filling with zeros",
                    pm.index,
                    data_path.display()
                ));
                write_zeros(&mut writer, export_size)?;
                total_written += export_size;
                continue;
            }

            let bytes_written = decompress_to_writer(
                &data_path,
                &meta.compression_type,
                &mut writer,
                Some(export_size),
                &mut progress_cb,
                &cancel_check,
                &mut log_cb,
            )?;
            total_written += bytes_written;

            // Pad to export_size if we wrote less
            if bytes_written < export_size {
                let pad = export_size - bytes_written;
                write_zeros(&mut writer, pad)?;
                total_written += pad;
            }

            // Resize FAT filesystem if the partition size changed
            if export_size != pm.original_size_bytes {
                writer.flush()?;
                let new_sectors = (export_size / 512) as u32;
                resize_fat_in_place(writer.get_mut(), part_offset, new_sectors, &mut log_cb)?;
                writer.seek(SeekFrom::Start(total_written))?;
            }

            log_cb(&format!(
                "partition-{}: wrote {} bytes (export size: {})",
                pm.index,
                bytes_written,
                export_size,
            ));
        }

        // Fill remainder up to disk_size
        if total_written < disk_size {
            let remaining = disk_size - total_written;
            write_zeros(&mut writer, remaining)?;
            total_written += remaining;
        }
    } else {
        // Raw image/device: reconstruct with partition size overrides
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
                writer.write_all(&buf[..n]).context("failed to write VHD data")?;
                total_written += n as u64;
                progress_cb(total_written);
            }
        } else {
            // Reconstruct disk with partition size overrides
            let mut buf = vec![0u8; CHUNK_SIZE];

            // Read and patch MBR (first 512 bytes)
            let mut mbr_buf = [0u8; 512];
            reader.read_exact(&mut mbr_buf).context("failed to read MBR from source")?;
            patch_mbr_partition_sizes(&mut mbr_buf, partition_sizes);
            writer.write_all(&mbr_buf).context("failed to write patched MBR")?;
            total_written += 512;
            log_cb("Patched MBR partition table with export sizes");

            // Sort partitions by start offset
            let mut sorted_parts: Vec<&PartitionSizeOverride> = partition_sizes.iter().collect();
            sorted_parts.sort_by_key(|p| p.start_lba);

            for ps in &sorted_parts {
                if cancel_check() {
                    bail!("export cancelled");
                }

                let part_offset = ps.start_lba * 512;

                // Copy everything from current position to this partition's start
                if total_written < part_offset {
                    let gap = part_offset - total_written;
                    reader.seek(SeekFrom::Start(total_written))?;
                    let mut gap_reader = (&mut reader).take(gap);
                    let mut gap_remaining = gap;
                    while gap_remaining > 0 {
                        let to_read = (gap_remaining as usize).min(CHUNK_SIZE);
                        let n = gap_reader.read(&mut buf[..to_read])?;
                        if n == 0 {
                            // Source ended early, fill remainder with zeros
                            write_zeros(&mut writer, gap_remaining)?;
                            total_written += gap_remaining;
                            break;
                        }
                        writer.write_all(&buf[..n])?;
                        total_written += n as u64;
                        gap_remaining -= n as u64;
                        progress_cb(total_written);
                    }
                }

                // Write partition data (limited to export_size)
                reader.seek(SeekFrom::Start(part_offset))?;
                let copy_size = ps.export_size.min(ps.original_size);
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
                if ps.export_size > copy_size {
                    let pad = ps.export_size - copy_size;
                    write_zeros(&mut writer, pad)?;
                    total_written += pad;
                }

                // Resize FAT filesystem if the partition size changed
                if ps.export_size != ps.original_size {
                    writer.flush()?;
                    let end_pos = total_written;
                    let new_sectors = (ps.export_size / 512) as u32;
                    resize_fat_in_place(writer.get_mut(), part_offset, new_sectors, &mut log_cb)?;
                    writer.seek(SeekFrom::Start(end_pos))?;
                }

                log_cb(&format!(
                    "partition-{}: exported {} bytes",
                    ps.index, ps.export_size,
                ));
            }

            // Copy any remaining data after the last partition up to source end
            if total_written < source_data_size {
                reader.seek(SeekFrom::Start(total_written))?;
                let remaining = source_data_size - total_written;
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
        }
    }

    writer.flush()?;

    // Append VHD footer
    let footer = build_vhd_footer(total_written);
    writer.write_all(&footer).context("failed to write VHD footer")?;
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
    writer.write_all(&footer).context("failed to write VHD footer")?;
    writer.flush()?;

    log_cb(&format!(
        "VHD partition export complete: {} ({} data bytes)",
        dest_path.display(),
        vhd_data_size,
    ));

    Ok(())
}

/// Decompress a partition data file and write it to the given writer.
/// If `max_bytes` is `Some(n)`, writing stops after `n` bytes.
/// Returns the number of raw bytes written.
fn decompress_to_writer(
    data_path: &Path,
    compression_type: &str,
    writer: &mut impl Write,
    max_bytes: Option<u64>,
    progress_cb: &mut impl FnMut(u64),
    cancel_check: &impl Fn() -> bool,
    log_cb: &mut impl FnMut(&str),
) -> Result<u64> {
    let limit = max_bytes.unwrap_or(u64::MAX);
    let mut total_written: u64 = 0;
    let mut buf = vec![0u8; CHUNK_SIZE];

    match compression_type {
        "none" | "raw" | "vhd" => {
            // Raw data — if it's a VHD file, strip the footer
            let file = File::open(data_path)
                .with_context(|| format!("failed to open {}", data_path.display()))?;
            let file_size = file.metadata()?.len();

            // Check if this is a VHD file (has footer)
            let data_size = if file_size >= 512 && compression_type == "vhd" {
                // Read last 512 bytes to check for VHD footer
                let mut f = File::open(data_path)?;
                f.seek(SeekFrom::End(-512))?;
                let mut footer_buf = [0u8; 8];
                f.read_exact(&mut footer_buf)?;
                if &footer_buf == VHD_COOKIE {
                    file_size - 512
                } else {
                    file_size
                }
            } else {
                file_size
            };

            let effective_size = data_size.min(limit);
            let mut reader = BufReader::new(File::open(data_path)?).take(effective_size);
            loop {
                if cancel_check() {
                    bail!("export cancelled");
                }
                let n = reader.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                writer.write_all(&buf[..n])?;
                total_written += n as u64;
                progress_cb(total_written);
            }
        }
        "zstd" => {
            let file = File::open(data_path)
                .with_context(|| format!("failed to open {}", data_path.display()))?;
            let mut decoder = zstd::Decoder::new(BufReader::new(file))
                .context("failed to create zstd decoder")?;
            loop {
                if cancel_check() {
                    bail!("export cancelled");
                }
                let remaining = limit - total_written;
                if remaining == 0 {
                    break;
                }
                let to_read = (remaining as usize).min(CHUNK_SIZE);
                let n = decoder.read(&mut buf[..to_read])?;
                if n == 0 {
                    break;
                }
                writer.write_all(&buf[..n])?;
                total_written += n as u64;
                progress_cb(total_written);
            }
        }
        "chd" => {
            // Use chdman to extract raw data to a temp file, then stream it
            let parent = data_path.parent().unwrap_or(Path::new("."));
            let temp_path = parent.join(format!(
                ".vhd-export-{}.tmp",
                data_path.file_stem().unwrap_or_default().to_string_lossy()
            ));

            log_cb(&format!("Extracting CHD: {}", data_path.display()));
            let output = Command::new("chdman")
                .arg("extractraw")
                .arg("-i")
                .arg(data_path)
                .arg("-o")
                .arg(&temp_path)
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .output()
                .context("failed to run chdman extractraw")?;

            if !output.status.success() {
                let _ = fs::remove_file(&temp_path);
                bail!(
                    "chdman extractraw failed: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                );
            }

            let temp_size = fs::metadata(&temp_path)
                .map(|m| m.len())
                .unwrap_or(u64::MAX);
            let effective_size = temp_size.min(limit);
            let mut reader = BufReader::new(
                File::open(&temp_path)
                    .with_context(|| format!("failed to open temp file: {}", temp_path.display()))?,
            )
            .take(effective_size);
            loop {
                if cancel_check() {
                    let _ = fs::remove_file(&temp_path);
                    bail!("export cancelled");
                }
                let n = reader.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                writer.write_all(&buf[..n])?;
                total_written += n as u64;
                progress_cb(total_written);
            }
            let _ = fs::remove_file(&temp_path);
        }
        other => {
            bail!("unsupported compression type for VHD export: {}", other);
        }
    }

    Ok(total_written)
}

/// Write `count` zero bytes to a writer, in chunks.
fn write_zeros(writer: &mut impl Write, count: u64) -> Result<()> {
    let zeros = vec![0u8; CHUNK_SIZE];
    let mut remaining = count;
    while remaining > 0 {
        let n = (remaining as usize).min(CHUNK_SIZE);
        writer.write_all(&zeros[..n]).context("failed to write zeros")?;
        remaining -= n as u64;
    }
    Ok(())
}

/// Stream raw data with optional splitting and zero-skipping.
fn stream_with_split(
    reader: &mut impl Read,
    output_base: &Path,
    extension: &str,
    split_size: Option<u64>,
    skip_zeros: bool,
    progress_cb: &mut impl FnMut(u64),
    cancel_check: &impl Fn() -> bool,
) -> Result<Vec<String>> {
    let mut files = Vec::new();
    let mut total_read: u64 = 0;
    let mut part_index: u32 = 0;
    let mut current_file_bytes: u64 = 0;
    let split_bytes = split_size.unwrap_or(u64::MAX);
    let mut skipped_zeros = false;

    let first_path = output_path(output_base, extension, split_size.is_some(), part_index);
    let mut writer = BufWriter::new(
        File::create(&first_path)
            .with_context(|| format!("failed to create {}", first_path.display()))?,
    );
    files.push(file_name(&first_path));

    let mut buf = vec![0u8; CHUNK_SIZE];
    loop {
        if cancel_check() {
            bail!("backup cancelled");
        }

        let n = reader.read(&mut buf).context("failed to read source")?;
        if n == 0 {
            break;
        }

        // When skip_zeros is enabled and the entire chunk is zeros, seek forward
        // in the output instead of writing. This creates a sparse file on
        // supported filesystems and saves I/O time on large mostly-empty partitions.
        if skip_zeros && is_all_zeros(&buf[..n]) {
            // We still need to account for split boundaries
            let mut remaining = n;
            while remaining > 0 {
                let space_in_split = split_bytes.saturating_sub(current_file_bytes) as usize;
                let skip_amount = remaining.min(space_in_split);
                current_file_bytes += skip_amount as u64;
                remaining -= skip_amount;

                if current_file_bytes >= split_bytes && remaining > 0 {
                    // Ensure correct file length before moving to next split
                    writer.flush()?;
                    writer.get_mut().set_len(current_file_bytes)?;
                    drop(writer);
                    part_index += 1;
                    current_file_bytes = 0;
                    let next_path = output_path(output_base, extension, true, part_index);
                    writer = BufWriter::new(
                        File::create(&next_path)
                            .with_context(|| format!("failed to create {}", next_path.display()))?,
                    );
                    files.push(file_name(&next_path));
                }
            }
            skipped_zeros = true;
            total_read += n as u64;
            progress_cb(total_read);
            continue;
        }

        // If we previously skipped zeros, seek the writer to the correct position
        if skipped_zeros {
            writer.flush()?;
            writer.seek(io::SeekFrom::Start(current_file_bytes))?;
            skipped_zeros = false;
        }

        let mut written = 0;
        while written < n {
            let remaining_in_split = split_bytes.saturating_sub(current_file_bytes) as usize;
            let to_write = (n - written).min(remaining_in_split);
            writer
                .write_all(&buf[written..written + to_write])
                .context("failed to write output")?;
            current_file_bytes += to_write as u64;
            written += to_write;

            if current_file_bytes >= split_bytes && written < n {
                writer.flush()?;
                drop(writer);
                part_index += 1;
                current_file_bytes = 0;
                skipped_zeros = false;
                let next_path =
                    output_path(output_base, extension, true, part_index);
                writer = BufWriter::new(
                    File::create(&next_path)
                        .with_context(|| format!("failed to create {}", next_path.display()))?,
                );
                files.push(file_name(&next_path));
            }
        }

        total_read += n as u64;
        progress_cb(total_read);
    }

    // Ensure correct file length if the last chunk(s) were skipped zeros
    writer.flush()?;
    if skipped_zeros {
        writer.get_mut().set_len(current_file_bytes)?;
    }

    Ok(files)
}

/// Check if a byte slice is entirely zeros.
fn is_all_zeros(data: &[u8]) -> bool {
    data.iter().all(|&b| b == 0)
}

/// Compress with zstd, streaming through the encoder with optional splitting.
fn compress_zstd(
    reader: &mut impl Read,
    output_base: &Path,
    split_size: Option<u64>,
    progress_cb: &mut impl FnMut(u64),
    cancel_check: &impl Fn() -> bool,
) -> Result<Vec<String>> {
    let mut files = Vec::new();
    let mut total_read: u64 = 0;
    let mut part_index: u32 = 0;
    let split_bytes = split_size.unwrap_or(u64::MAX);

    let first_path = output_path(output_base, "zst", split_size.is_some(), part_index);
    let mut encoder = zstd::Encoder::new(
        SplitWriter::new(&first_path, split_bytes, &mut files, &mut part_index, output_base)?,
        3, // compression level
    )
    .context("failed to create zstd encoder")?;
    files.push(file_name(&first_path));

    let mut buf = vec![0u8; CHUNK_SIZE];
    loop {
        if cancel_check() {
            bail!("backup cancelled");
        }

        let n = reader.read(&mut buf).context("failed to read source")?;
        if n == 0 {
            break;
        }

        encoder
            .write_all(&buf[..n])
            .context("failed to write compressed data")?;
        total_read += n as u64;
        progress_cb(total_read);
    }

    encoder.finish().context("failed to finalize zstd stream")?;
    Ok(files)
}

/// Compress via chdman external tool.
///
/// Steps:
/// 1. Write raw data to a temp file next to the output
/// 2. Run `chdman createraw -i temp -o output.chd -hs 4096`
/// 3. Clean up temp file
/// 4. If splitting is needed, split the output CHD manually
fn compress_chd(
    reader: &mut impl Read,
    output_base: &Path,
    split_size: Option<u64>,
    progress_cb: &mut impl FnMut(u64),
    cancel_check: &impl Fn() -> bool,
    log_cb: &mut impl FnMut(&str),
) -> Result<Vec<String>> {
    let parent = output_base
        .parent()
        .context("output path has no parent directory")?;

    // Step 1: Write raw data to temp file
    let temp_path = parent.join(format!(
        ".{}.tmp",
        output_base
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
    ));
    {
        let mut temp_writer = BufWriter::new(
            File::create(&temp_path)
                .with_context(|| format!("failed to create temp file: {}", temp_path.display()))?,
        );
        let mut total_read: u64 = 0;
        let mut buf = vec![0u8; CHUNK_SIZE];
        loop {
            if cancel_check() {
                let _ = fs::remove_file(&temp_path);
                bail!("backup cancelled");
            }
            let n = reader.read(&mut buf).context("failed to read source")?;
            if n == 0 {
                break;
            }
            temp_writer
                .write_all(&buf[..n])
                .context("failed to write temp file")?;
            total_read += n as u64;
            progress_cb(total_read);
        }
        temp_writer.flush()?;
    }

    // Step 2: Determine raw data size for chdman (must be known)
    let raw_size = fs::metadata(&temp_path)
        .with_context(|| format!("failed to stat temp file: {}", temp_path.display()))?
        .len();

    // chdman createraw parameters:
    // -us (unit size) = sector size, always 512 bytes
    // -hs (hunk size) = must be a multiple of unit size, and total data
    //     must be a multiple of hunk size. Default to 4096 (8 sectors).
    let unit_size: u64 = 512;
    let hunk_size: u64 = 4096;

    // Pad the raw data to the nearest hunk_size boundary if needed
    let remainder = raw_size % hunk_size;
    if remainder != 0 {
        let pad_bytes = hunk_size - remainder;
        let pad_file = fs::OpenOptions::new()
            .append(true)
            .open(&temp_path)
            .context("failed to open temp file for padding")?;
        let mut pad_writer = BufWriter::new(pad_file);
        let zeros = vec![0u8; pad_bytes as usize];
        pad_writer
            .write_all(&zeros)
            .context("failed to pad temp file")?;
        pad_writer.flush()?;
    }

    let chd_path = output_path(output_base, "chd", false, 0);
    log_cb(&format!("Running chdman createraw → {}", chd_path.display()));
    let output = Command::new("chdman")
        .arg("createraw")
        .arg("-i")
        .arg(&temp_path)
        .arg("-o")
        .arg(&chd_path)
        .arg("-hs")
        .arg(hunk_size.to_string())
        .arg("-us")
        .arg(unit_size.to_string())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .context("failed to run chdman")?;

    // Forward chdman output to log
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            log_cb(trimmed);
        }
    }
    for line in String::from_utf8_lossy(&output.stderr).lines() {
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            log_cb(trimmed);
        }
    }

    let _ = fs::remove_file(&temp_path);

    if !output.status.success() {
        bail!(
            "chdman exited with status {}",
            output.status.code().unwrap_or(-1)
        );
    }

    // Step 3: Split the CHD file if requested
    if let Some(split_bytes) = split_size {
        let chd_size = fs::metadata(&chd_path)
            .with_context(|| format!("failed to stat CHD output: {}", chd_path.display()))?
            .len();

        if chd_size > split_bytes {
            return split_file(&chd_path, output_base, "chd", split_bytes);
        }
    }

    Ok(vec![file_name(&chd_path)])
}

/// Split an existing file into chunks, removing the original.
fn split_file(
    source: &Path,
    output_base: &Path,
    extension: &str,
    split_bytes: u64,
) -> Result<Vec<String>> {
    let mut reader = BufReader::new(
        File::open(source).with_context(|| format!("failed to open {}", source.display()))?,
    );
    let mut files = Vec::new();
    let mut part_index: u32 = 0;
    let mut buf = vec![0u8; CHUNK_SIZE];

    loop {
        let out_path = output_path(output_base, extension, true, part_index);
        let mut writer = BufWriter::new(
            File::create(&out_path)
                .with_context(|| format!("failed to create {}", out_path.display()))?,
        );
        let mut written: u64 = 0;
        let mut eof = false;

        while written < split_bytes {
            let to_read = ((split_bytes - written) as usize).min(CHUNK_SIZE);
            let n = reader.read(&mut buf[..to_read])?;
            if n == 0 {
                eof = true;
                break;
            }
            writer.write_all(&buf[..n])?;
            written += n as u64;
        }
        writer.flush()?;

        if written > 0 {
            files.push(file_name(&out_path));
        } else {
            // Empty chunk, remove it
            let _ = fs::remove_file(&out_path);
        }

        part_index += 1;
        if eof {
            break;
        }
    }

    // Remove the original unsplit file
    let _ = fs::remove_file(source);

    Ok(files)
}

/// Build the output file path with optional split numbering.
/// `partition-0.zst` (no split) or `partition-0.001.zst` (split).
fn output_path(base: &Path, extension: &str, splitting: bool, part_index: u32) -> PathBuf {
    let stem = base
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy();
    let parent = base.parent().unwrap_or(Path::new("."));
    if splitting && part_index > 0 {
        parent.join(format!("{stem}.{:03}.{extension}", part_index))
    } else {
        parent.join(format!("{stem}.{extension}"))
    }
}

/// Extract just the file name as a String.
fn file_name(path: &Path) -> String {
    path.file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .into_owned()
}

/// A writer wrapper used only during zstd compression that does NOT do splitting.
/// Zstd doesn't support splitting mid-stream well (the compressed frame must be
/// contiguous), so we write the entire compressed output to one file.
/// Splitting of zstd output happens post-hoc if needed.
struct SplitWriter {
    inner: BufWriter<File>,
}

impl SplitWriter {
    fn new(
        path: &Path,
        _split_bytes: u64,
        _files: &mut Vec<String>,
        _part_index: &mut u32,
        _output_base: &Path,
    ) -> Result<Self> {
        let file = File::create(path)
            .with_context(|| format!("failed to create {}", path.display()))?;
        Ok(Self {
            inner: BufWriter::new(file),
        })
    }
}

impl Write for SplitWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tempfile::TempDir;

    #[test]
    fn test_output_path_no_split() {
        let base = Path::new("/tmp/backup/partition-0");
        let path = output_path(base, "zst", false, 0);
        assert_eq!(path, PathBuf::from("/tmp/backup/partition-0.zst"));
    }

    #[test]
    fn test_output_path_split() {
        let base = Path::new("/tmp/backup/partition-0");
        assert_eq!(
            output_path(base, "zst", true, 0),
            PathBuf::from("/tmp/backup/partition-0.zst")
        );
        assert_eq!(
            output_path(base, "zst", true, 1),
            PathBuf::from("/tmp/backup/partition-0.001.zst")
        );
        assert_eq!(
            output_path(base, "zst", true, 12),
            PathBuf::from("/tmp/backup/partition-0.012.zst")
        );
    }

    #[test]
    fn test_compress_none_no_split() {
        let tmp = TempDir::new().unwrap();
        let data = vec![0xABu8; 4096];
        let mut reader = Cursor::new(&data);
        let base = tmp.path().join("partition-0");

        let files = compress_partition(
            &mut reader,
            &base,
            CompressionType::None,
            None,
            false,
            |_| {},
            || false,
            |_| {},
        )
        .unwrap();

        assert_eq!(files, vec!["partition-0.raw"]);
        let written = fs::read(tmp.path().join("partition-0.raw")).unwrap();
        assert_eq!(written.len(), 4096);
        assert!(written.iter().all(|&b| b == 0xAB));
    }

    #[test]
    fn test_compress_none_with_split() {
        let tmp = TempDir::new().unwrap();
        let data = vec![0xCDu8; 3000];
        let mut reader = Cursor::new(&data);
        let base = tmp.path().join("partition-0");

        let files = compress_partition(
            &mut reader,
            &base,
            CompressionType::None,
            Some(1024),
            false,
            |_| {},
            || false,
            |_| {},
        )
        .unwrap();

        assert_eq!(files.len(), 3);
        assert_eq!(files[0], "partition-0.raw");
        assert_eq!(files[1], "partition-0.001.raw");
        assert_eq!(files[2], "partition-0.002.raw");

        let f0 = fs::read(tmp.path().join("partition-0.raw")).unwrap();
        assert_eq!(f0.len(), 1024);
        let f1 = fs::read(tmp.path().join("partition-0.001.raw")).unwrap();
        assert_eq!(f1.len(), 1024);
        let f2 = fs::read(tmp.path().join("partition-0.002.raw")).unwrap();
        assert_eq!(f2.len(), 952);
    }

    #[test]
    fn test_compress_zstd() {
        let tmp = TempDir::new().unwrap();
        // Highly compressible data
        let data = vec![0u8; 65536];
        let mut reader = Cursor::new(&data);
        let base = tmp.path().join("partition-0");

        let files = compress_partition(
            &mut reader,
            &base,
            CompressionType::Zstd,
            None,
            false,
            |_| {},
            || false,
            |_| {},
        )
        .unwrap();

        assert_eq!(files[0], "partition-0.zst");
        let compressed = fs::read(tmp.path().join("partition-0.zst")).unwrap();
        // Zstd compressed output should be smaller than input
        assert!(compressed.len() < 65536);

        // Decompress and verify
        let decompressed = zstd::decode_all(&compressed[..]).unwrap();
        assert_eq!(decompressed.len(), 65536);
        assert!(decompressed.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_skip_zeros_raw() {
        let tmp = TempDir::new().unwrap();
        // 256KB of zeros followed by 256KB of data
        let mut data = vec![0u8; CHUNK_SIZE];
        data.extend(vec![0xAAu8; CHUNK_SIZE]);
        let mut reader = Cursor::new(&data);
        let base = tmp.path().join("partition-0");

        let files = compress_partition(
            &mut reader,
            &base,
            CompressionType::None,
            None,
            true, // skip zeros
            |_| {},
            || false,
            |_| {},
        )
        .unwrap();

        assert_eq!(files, vec!["partition-0.raw"]);
        let written = fs::read(tmp.path().join("partition-0.raw")).unwrap();
        // File should still be the full size (sparse on disk, full logically)
        assert_eq!(written.len(), CHUNK_SIZE * 2);
        // First chunk should be zeros, second should be 0xAA
        assert!(written[..CHUNK_SIZE].iter().all(|&b| b == 0));
        assert!(written[CHUNK_SIZE..].iter().all(|&b| b == 0xAA));
    }

    #[test]
    fn test_skip_zeros_all_zeros() {
        let tmp = TempDir::new().unwrap();
        let data = vec![0u8; CHUNK_SIZE * 4];
        let mut reader = Cursor::new(&data);
        let base = tmp.path().join("partition-0");

        let files = compress_partition(
            &mut reader,
            &base,
            CompressionType::None,
            None,
            true, // skip zeros
            |_| {},
            || false,
            |_| {},
        )
        .unwrap();

        assert_eq!(files, vec!["partition-0.raw"]);
        let written = fs::read(tmp.path().join("partition-0.raw")).unwrap();
        assert_eq!(written.len(), CHUNK_SIZE * 4);
        assert!(written.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_cancel_aborts() {
        let tmp = TempDir::new().unwrap();
        let data = vec![0u8; 65536];
        let mut reader = Cursor::new(&data);
        let base = tmp.path().join("partition-0");

        let result = compress_partition(
            &mut reader,
            &base,
            CompressionType::None,
            None,
            false,
            |_| {},
            || true, // always cancel
            |_| {},
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cancelled"));
    }

    #[test]
    fn test_detect_chdman() {
        // Just ensure it doesn't panic; result depends on system
        let _available = detect_chdman();
    }

    #[test]
    fn test_write_vhd() {
        let tmp = TempDir::new().unwrap();
        let data = vec![0xABu8; 4096];
        let mut reader = Cursor::new(&data);
        let base = tmp.path().join("partition-0");

        let files = compress_partition(
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
