//! Wrap a superfloppy filesystem image with a synthetic MBR or GPT partition table.
//!
//! A "superfloppy" is a disk image whose sector 0 is the filesystem VBR, not a
//! partition table. Many emulators and BIOS-era machines refuse to use such an
//! image as a hard disk. This module synthesizes a single-partition MBR (or
//! GPT) around the filesystem and streams the result to a target writer,
//! patching the VBR `hidden_sectors` field so FAT/NTFS/exFAT will still mount.
//!
//! The transform is one-way: there is no helper here to collapse a partitioned
//! image back into a superfloppy.

use std::io::{Read, Seek, SeekFrom, Write};

use anyhow::{bail, Context, Result};

use crate::fs::patch_hidden_sectors_for;
use crate::partition::gpt::{build_minimal_gpt, Gpt, Guid};
use crate::partition::mbr::build_minimal_mbr;

const SECTOR_SIZE: u64 = 512;
const STREAM_CHUNK: usize = 1024 * 1024;

/// Where the synthesized first partition begins on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WrapAlignment {
    /// LBA 63 — classic DOS/XP geometry (255 heads × 63 sectors/track).
    DosTraditional,
    /// LBA 2048 — Vista+ 1 MiB alignment.
    Modern1MB,
    /// Caller-specified start LBA.
    Custom(u64),
}

impl WrapAlignment {
    pub fn first_partition_lba(self) -> u64 {
        match self {
            WrapAlignment::DosTraditional => 63,
            WrapAlignment::Modern1MB => 2048,
            WrapAlignment::Custom(lba) => lba,
        }
    }
}

/// Partition-table flavor and the metadata that flavor needs.
#[derive(Debug, Clone)]
pub enum WrapTable {
    Mbr {
        type_byte: u8,
        bootable: bool,
        alignment: WrapAlignment,
    },
    Gpt {
        type_guid: Guid,
        name: String,
        alignment: WrapAlignment,
    },
}

impl WrapTable {
    pub fn first_partition_lba(&self) -> u64 {
        match self {
            WrapTable::Mbr { alignment, .. } => alignment.first_partition_lba(),
            WrapTable::Gpt { alignment, .. } => alignment.first_partition_lba().max(34),
        }
    }
}

/// All parameters needed to wrap a superfloppy and stream it to a target.
#[derive(Debug, Clone)]
pub struct WrapParams {
    pub table: WrapTable,
    /// Total bytes the synthesized partition occupies. Must be >= source size
    /// and must fit in the target.
    pub partition_size_bytes: u64,
    pub target_size_bytes: u64,
    /// Source filesystem hint string (e.g. "NTFS", "FAT32"). Used only for
    /// log lines.
    pub source_fs_hint: String,
}

/// Wrap `source` (a superfloppy filesystem image of length `source_size`) with
/// the partition table described by `params` and write the resulting disk
/// image to `target`. After the data is on disk, the VBR `hidden_sectors`
/// field is patched so the filesystem will still mount at the new offset.
///
/// `target` must support read+write+seek because `patch_hidden_sectors_for`
/// reads the VBR back to detect FAT/NTFS/exFAT before patching.
pub fn wrap_and_write<R, W>(
    source: &mut R,
    source_size: u64,
    target: &mut W,
    params: &WrapParams,
    progress_cb: &mut impl FnMut(u64),
    cancel_check: &impl Fn() -> bool,
    log_cb: &mut impl FnMut(&str),
) -> Result<()>
where
    R: Read,
    W: Read + Write + Seek,
{
    let first_lba = params.table.first_partition_lba();
    let first_byte = first_lba
        .checked_mul(SECTOR_SIZE)
        .context("first partition LBA overflows")?;

    if params.partition_size_bytes < source_size {
        bail!(
            "partition size {} bytes is smaller than source filesystem ({} bytes)",
            params.partition_size_bytes,
            source_size
        );
    }
    if !params.partition_size_bytes.is_multiple_of(SECTOR_SIZE) {
        bail!(
            "partition size {} bytes is not a multiple of the sector size",
            params.partition_size_bytes
        );
    }
    let end_byte = first_byte
        .checked_add(params.partition_size_bytes)
        .context("partition end overflows")?;
    if end_byte > params.target_size_bytes {
        bail!(
            "partition end ({} bytes) exceeds target size ({} bytes)",
            end_byte,
            params.target_size_bytes
        );
    }

    let total_sectors = params.target_size_bytes / SECTOR_SIZE;
    let partition_sectors = params.partition_size_bytes / SECTOR_SIZE;

    log_cb(&format!(
        "Wrapping {} superfloppy ({} bytes) into a {} byte disk; partition starts at LBA {}",
        params.source_fs_hint, source_size, params.target_size_bytes, first_lba,
    ));

    write_table(
        target,
        params,
        first_lba,
        partition_sectors,
        total_sectors,
        log_cb,
    )?;

    zero_fill_gap(target, first_byte, log_cb)?;

    stream_source(
        target,
        source,
        source_size,
        first_byte,
        progress_cb,
        cancel_check,
    )?;

    zero_fill_partition_tail(
        target,
        first_byte,
        source_size,
        params.partition_size_bytes,
        cancel_check,
    )?;

    if let WrapTable::Gpt { .. } = params.table {
        write_backup_gpt(target, params, partition_sectors, total_sectors, log_cb)?;
    }

    target.flush().context("flushing target after wrap")?;

    patch_hidden_sectors_for(target, first_byte, first_lba, log_cb)
        .context("patching filesystem hidden_sectors after wrap")?;

    log_cb("Wrap complete");
    Ok(())
}

fn write_table<W: Read + Write + Seek>(
    target: &mut W,
    params: &WrapParams,
    first_lba: u64,
    partition_sectors: u64,
    total_sectors: u64,
    log_cb: &mut impl FnMut(&str),
) -> Result<()> {
    target
        .seek(SeekFrom::Start(0))
        .context("seeking to LBA 0")?;
    match &params.table {
        WrapTable::Mbr {
            type_byte,
            bootable,
            ..
        } => {
            let start_lba_u32: u32 = first_lba
                .try_into()
                .context("MBR start LBA exceeds 32-bit limit; use GPT")?;
            let sectors_u32: u32 = partition_sectors
                .try_into()
                .context("MBR partition sector count exceeds 32-bit limit; use GPT")?;
            let disk_sig = synth_disk_signature(params);
            let mut mbr = build_minimal_mbr(
                disk_sig,
                &[(*type_byte, start_lba_u32, sectors_u32, *bootable)],
                255,
                63,
            );
            // A bootable partition needs MBR boot code to chainload it;
            // build_minimal_mbr only writes the partition table.
            if *bootable {
                crate::partition::mbr::install_mbr_boot_code(&mut mbr);
            }
            target.write_all(&mbr).context("writing synthetic MBR")?;
            log_cb(&format!(
                "Wrote synthetic MBR (type 0x{:02X}, start LBA {}, {} sectors{})",
                type_byte,
                first_lba,
                sectors_u32,
                if *bootable { ", bootable" } else { "" },
            ));
        }
        WrapTable::Gpt {
            type_guid, name, ..
        } => {
            let last_lba = first_lba + partition_sectors - 1;
            let prot = Gpt::build_protective_mbr(total_sectors);
            target
                .write_all(&prot)
                .context("writing GPT protective MBR")?;
            let gpt = build_minimal_gpt(
                &[(*type_guid, first_lba, last_lba, name.clone())],
                params.target_size_bytes,
            );
            let primary = gpt.build_primary_gpt(total_sectors);
            target.write_all(&primary).context("writing primary GPT")?;
            log_cb(&format!(
                "Wrote protective MBR + primary GPT (start LBA {}, {} sectors)",
                first_lba, partition_sectors,
            ));
        }
    }
    Ok(())
}

fn zero_fill_gap<W: Write + Seek>(
    target: &mut W,
    first_byte: u64,
    log_cb: &mut impl FnMut(&str),
) -> Result<()> {
    let cur = target.stream_position().context("stream_position")?;
    if cur >= first_byte {
        return Ok(());
    }
    let zeros = vec![0u8; STREAM_CHUNK];
    let mut remaining = first_byte - cur;
    while remaining > 0 {
        let n = remaining.min(STREAM_CHUNK as u64) as usize;
        target
            .write_all(&zeros[..n])
            .context("zero-filling pre-partition gap")?;
        remaining -= n as u64;
    }
    log_cb(&format!(
        "Zero-filled {} bytes between table and partition",
        first_byte - cur
    ));
    Ok(())
}

fn stream_source<R: Read, W: Write + Seek>(
    target: &mut W,
    source: &mut R,
    source_size: u64,
    first_byte: u64,
    progress_cb: &mut impl FnMut(u64),
    cancel_check: &impl Fn() -> bool,
) -> Result<()> {
    target
        .seek(SeekFrom::Start(first_byte))
        .context("seeking to partition start")?;
    let mut buf = vec![0u8; STREAM_CHUNK];
    let mut written: u64 = 0;
    while written < source_size {
        if cancel_check() {
            bail!("wrap cancelled");
        }
        let want = ((source_size - written).min(STREAM_CHUNK as u64)) as usize;
        let mut filled = 0;
        while filled < want {
            let n = source
                .read(&mut buf[filled..want])
                .context("reading from source")?;
            if n == 0 {
                bail!(
                    "source ended early: expected {} bytes, got {}",
                    source_size,
                    written + filled as u64
                );
            }
            filled += n;
        }
        target
            .write_all(&buf[..filled])
            .context("writing partition data")?;
        written += filled as u64;
        progress_cb(written);
    }
    Ok(())
}

fn zero_fill_partition_tail<W: Write + Seek>(
    target: &mut W,
    first_byte: u64,
    source_size: u64,
    partition_size: u64,
    cancel_check: &impl Fn() -> bool,
) -> Result<()> {
    if partition_size <= source_size {
        return Ok(());
    }
    target
        .seek(SeekFrom::Start(first_byte + source_size))
        .context("seeking to partition tail")?;
    let zeros = vec![0u8; STREAM_CHUNK];
    let mut remaining = partition_size - source_size;
    while remaining > 0 {
        if cancel_check() {
            bail!("wrap cancelled");
        }
        let n = remaining.min(STREAM_CHUNK as u64) as usize;
        target
            .write_all(&zeros[..n])
            .context("zero-padding partition tail")?;
        remaining -= n as u64;
    }
    Ok(())
}

fn write_backup_gpt<W: Write + Seek>(
    target: &mut W,
    params: &WrapParams,
    partition_sectors: u64,
    total_sectors: u64,
    log_cb: &mut impl FnMut(&str),
) -> Result<()> {
    let (type_guid, name, first_lba) = match &params.table {
        WrapTable::Gpt {
            type_guid,
            name,
            alignment,
        } => (
            *type_guid,
            name.clone(),
            alignment.first_partition_lba().max(34),
        ),
        _ => return Ok(()),
    };
    let last_lba = first_lba + partition_sectors - 1;
    let gpt = build_minimal_gpt(
        &[(type_guid, first_lba, last_lba, name)],
        params.target_size_bytes,
    );
    let backup = gpt.build_backup_gpt(total_sectors);
    // Backup GPT occupies sectors total_sectors-33 .. total_sectors-1.
    let backup_offset = (total_sectors - 33) * SECTOR_SIZE;
    target
        .seek(SeekFrom::Start(backup_offset))
        .context("seeking to backup GPT")?;
    target.write_all(&backup).context("writing backup GPT")?;
    log_cb("Wrote backup GPT at end of disk");
    Ok(())
}

/// Derive a stable but unique-ish MBR disk signature from the params, so two
/// wraps of different filesystems don't accidentally collide.
fn synth_disk_signature(params: &WrapParams) -> u32 {
    let mut h: u64 = 0x9E37_79B9_7F4A_7C15;
    h ^= params
        .partition_size_bytes
        .wrapping_mul(0x6C62_272E_07BB_0142);
    h ^= params.target_size_bytes.wrapping_mul(0x517C_C1B7_2722_0A95);
    for b in params.source_fs_hint.as_bytes() {
        h = h.wrapping_mul(0x0100_0000_01B3).wrapping_add(*b as u64);
    }
    (h as u32) | 1
}

// =============================================================================
// Heuristic
// =============================================================================

/// Well-known GPT type GUID strings.
const GPT_MICROSOFT_BASIC_DATA: &str = "EBD0A0A2-B9E5-4433-87C0-68B6B72699C7";
const GPT_LINUX_FILESYSTEM: &str = "0FC63DAF-8483-4772-8E79-3D69D8477DE4";
const GPT_APPLE_HFS: &str = "48465300-0000-11AA-AA11-00306543ECAC";

/// Two TiB in bytes — the addressable limit for an MBR with 512-byte sectors.
const MBR_MAX_BYTES: u64 = 2u64 * 1024 * 1024 * 1024 * 1024;

/// Suggest a default `WrapParams` from the source's filesystem hint, size, and
/// (if available) the `hidden_sectors` value read from the VBR. Callers are
/// expected to surface the result in the UI and let the user override.
pub fn suggest_wrap_params(
    fs_hint: &str,
    source_size: u64,
    target_size: u64,
    detected_hidden_sectors: Option<u32>,
) -> WrapParams {
    let force_gpt = target_size > MBR_MAX_BYTES;
    let alignment = match detected_hidden_sectors {
        Some(63) => WrapAlignment::DosTraditional,
        Some(2048) => WrapAlignment::Modern1MB,
        _ => WrapAlignment::Modern1MB,
    };

    let table = if force_gpt {
        WrapTable::Gpt {
            type_guid: gpt_guid_for_fs(fs_hint),
            name: gpt_name_for_fs(fs_hint),
            alignment,
        }
    } else {
        WrapTable::Mbr {
            type_byte: mbr_type_byte_for_fs(fs_hint),
            bootable: false,
            alignment,
        }
    };

    // Default partition size = source size rounded up to the next sector.
    let partition_size_bytes = source_size.div_ceil(SECTOR_SIZE) * SECTOR_SIZE;

    WrapParams {
        table,
        partition_size_bytes,
        target_size_bytes: target_size,
        source_fs_hint: fs_hint.to_string(),
    }
}

/// Map a filesystem hint string to a recommended MBR partition type byte.
/// Returns 0x07 (NTFS/exFAT/IFS) when the hint is not recognized; the user
/// can override in the UI.
pub fn mbr_type_byte_for_fs(fs_hint: &str) -> u8 {
    let lower = fs_hint.to_ascii_lowercase();
    if lower.contains("ntfs") || lower.contains("exfat") {
        0x07
    } else if lower.contains("fat32") {
        0x0C
    } else if lower.contains("fat16") {
        0x06
    } else if lower.contains("fat12") {
        0x01
    } else if lower.contains("fat") {
        0x0C
    } else if lower.contains("ext") || lower.contains("linux") || lower.contains("xfs") {
        0x83
    } else if lower.contains("hfs") {
        0xAF
    } else {
        0x07
    }
}

fn gpt_guid_for_fs(fs_hint: &str) -> Guid {
    let lower = fs_hint.to_ascii_lowercase();
    let s = if lower.contains("hfs") {
        GPT_APPLE_HFS
    } else if lower.contains("ext") || lower.contains("linux") || lower.contains("xfs") {
        GPT_LINUX_FILESYSTEM
    } else {
        GPT_MICROSOFT_BASIC_DATA
    };
    Guid::from_string(s).expect("hardcoded GUID string is valid")
}

fn gpt_name_for_fs(fs_hint: &str) -> String {
    if fs_hint.is_empty() {
        "Data".to_string()
    } else {
        fs_hint.to_string()
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    const ONE_MIB: u64 = 1024 * 1024;
    const DISK_8M: u64 = 8 * ONE_MIB;

    fn no_progress() -> impl FnMut(u64) {
        |_| {}
    }
    fn no_cancel() -> impl Fn() -> bool {
        || false
    }
    fn no_log() -> impl FnMut(&str) {
        |_| {}
    }

    /// Build a minimal NTFS VBR fixture: 0xEB jump, "NTFS    " OEM ID, a
    /// `hidden_sectors` field at offset 0x1C set to 0, and 0x55AA boot
    /// signature. Anything else is zeros — that's all `patch_ntfs_hidden_sectors`
    /// inspects.
    fn ntfs_vbr_sector(initial_hidden_sectors: u32) -> [u8; 512] {
        let mut s = [0u8; 512];
        s[0] = 0xEB;
        s[1] = 0x52;
        s[2] = 0x90;
        s[3..11].copy_from_slice(b"NTFS    ");
        s[11..13].copy_from_slice(&512u16.to_le_bytes()); // bytes_per_sector
        s[0x1C..0x20].copy_from_slice(&initial_hidden_sectors.to_le_bytes());
        s[510] = 0x55;
        s[511] = 0xAA;
        s
    }

    /// Build a minimal FAT32 VBR fixture for testing the hidden_sectors patch.
    fn fat32_vbr_sector(initial_hidden_sectors: u32) -> [u8; 512] {
        let mut s = [0u8; 512];
        s[0] = 0xEB;
        s[1] = 0x58;
        s[2] = 0x90;
        s[3..11].copy_from_slice(b"MSWIN4.1");
        s[11..13].copy_from_slice(&512u16.to_le_bytes());
        s[13] = 8; // sectors per cluster
        s[14..16].copy_from_slice(&32u16.to_le_bytes()); // reserved sectors
        s[16] = 2; // num fats
        s[0x1C..0x20].copy_from_slice(&initial_hidden_sectors.to_le_bytes());
        s[0x52..0x5A].copy_from_slice(b"FAT32   "); // FS type at FAT32 offset
        s[510] = 0x55;
        s[511] = 0xAA;
        s
    }

    fn make_source(vbr: [u8; 512], total_size: u64) -> Vec<u8> {
        let mut v = vec![0u8; total_size as usize];
        v[..512].copy_from_slice(&vbr);
        v
    }

    fn wrap_to_vec(source: Vec<u8>, params: &WrapParams) -> Vec<u8> {
        let source_size = source.len() as u64;
        let mut target = vec![0u8; params.target_size_bytes as usize];
        let mut src = Cursor::new(source);
        let mut tgt = Cursor::new(&mut target[..]);
        wrap_and_write(
            &mut src,
            source_size,
            &mut tgt,
            params,
            &mut no_progress(),
            &no_cancel(),
            &mut no_log(),
        )
        .expect("wrap_and_write");
        target
    }

    #[test]
    fn test_wrap_mbr_ntfs_lba63() {
        let fs_size = 4 * ONE_MIB;
        let params = WrapParams {
            table: WrapTable::Mbr {
                type_byte: 0x07,
                bootable: false,
                alignment: WrapAlignment::DosTraditional,
            },
            partition_size_bytes: fs_size,
            target_size_bytes: DISK_8M,
            source_fs_hint: "NTFS".into(),
        };
        let src = make_source(ntfs_vbr_sector(0), fs_size);
        let out = wrap_to_vec(src, &params);

        // MBR signature.
        assert_eq!(out[510], 0x55);
        assert_eq!(out[511], 0xAA);

        // First partition entry: type 0x07, start LBA 63, sector count fs_size/512.
        let entry = &out[446..462];
        assert_eq!(entry[4], 0x07);
        let start_lba = u32::from_le_bytes(entry[8..12].try_into().unwrap());
        let sectors = u32::from_le_bytes(entry[12..16].try_into().unwrap());
        assert_eq!(start_lba, 63);
        assert_eq!(sectors as u64, fs_size / 512);

        // Original VBR bytes copied to LBA 63.
        let off = 63 * SECTOR_SIZE as usize;
        assert_eq!(out[off], 0xEB);
        assert_eq!(&out[off + 3..off + 11], b"NTFS    ");

        // hidden_sectors patched to 63.
        let hidden = u32::from_le_bytes(out[off + 0x1C..off + 0x20].try_into().unwrap());
        assert_eq!(hidden, 63);
    }

    #[test]
    fn test_wrap_mbr_fat32_lba2048() {
        let fs_size = 4 * ONE_MIB;
        let params = WrapParams {
            table: WrapTable::Mbr {
                type_byte: 0x0C,
                bootable: false,
                alignment: WrapAlignment::Modern1MB,
            },
            partition_size_bytes: fs_size,
            target_size_bytes: DISK_8M,
            source_fs_hint: "FAT32".into(),
        };
        let src = make_source(fat32_vbr_sector(0), fs_size);
        let out = wrap_to_vec(src, &params);

        let entry = &out[446..462];
        assert_eq!(entry[4], 0x0C);
        let start_lba = u32::from_le_bytes(entry[8..12].try_into().unwrap());
        assert_eq!(start_lba, 2048);

        let off = 2048 * SECTOR_SIZE as usize;
        assert_eq!(&out[off + 3..off + 11], b"MSWIN4.1");
        let hidden = u32::from_le_bytes(out[off + 0x1C..off + 0x20].try_into().unwrap());
        assert_eq!(hidden, 2048);
    }

    #[test]
    fn test_wrap_gpt_ntfs() {
        let fs_size = 4 * ONE_MIB;
        let type_guid = Guid::from_string(GPT_MICROSOFT_BASIC_DATA).unwrap();
        let params = WrapParams {
            table: WrapTable::Gpt {
                type_guid,
                name: "NTFS".into(),
                alignment: WrapAlignment::Modern1MB,
            },
            partition_size_bytes: fs_size,
            target_size_bytes: DISK_8M,
            source_fs_hint: "NTFS".into(),
        };
        let src = make_source(ntfs_vbr_sector(0), fs_size);
        let out = wrap_to_vec(src, &params);

        // Protective MBR at LBA 0 with type 0xEE.
        assert_eq!(out[450], 0xEE);
        // GPT signature "EFI PART" at LBA 1.
        assert_eq!(&out[512..520], b"EFI PART");
        // Backup GPT at last sector: signature is in the last 512 bytes.
        let last = (DISK_8M - 512) as usize;
        assert_eq!(&out[last..last + 8], b"EFI PART");

        // VBR copied + patched.
        let off = 2048 * SECTOR_SIZE as usize;
        assert_eq!(&out[off + 3..off + 11], b"NTFS    ");
        let hidden = u32::from_le_bytes(out[off + 0x1C..off + 0x20].try_into().unwrap());
        assert_eq!(hidden, 2048);
    }

    #[test]
    fn test_wrap_rejects_partition_too_large_for_disk() {
        let fs_size = 4 * ONE_MIB;
        let params = WrapParams {
            table: WrapTable::Mbr {
                type_byte: 0x07,
                bootable: false,
                alignment: WrapAlignment::Modern1MB,
            },
            partition_size_bytes: DISK_8M, // entire disk — no room for the table prefix
            target_size_bytes: DISK_8M,
            source_fs_hint: "NTFS".into(),
        };
        let source = make_source(ntfs_vbr_sector(0), fs_size);
        let source_size = source.len() as u64;
        let mut target = vec![0u8; DISK_8M as usize];
        let mut src = Cursor::new(source);
        let mut tgt = Cursor::new(&mut target[..]);
        let result = wrap_and_write(
            &mut src,
            source_size,
            &mut tgt,
            &params,
            &mut no_progress(),
            &no_cancel(),
            &mut no_log(),
        );
        assert!(result.is_err(), "expected validation error");
    }

    #[test]
    fn test_wrap_rejects_partition_smaller_than_source() {
        let fs_size = 4 * ONE_MIB;
        let params = WrapParams {
            table: WrapTable::Mbr {
                type_byte: 0x07,
                bootable: false,
                alignment: WrapAlignment::Modern1MB,
            },
            partition_size_bytes: 2 * ONE_MIB, // smaller than fs
            target_size_bytes: DISK_8M,
            source_fs_hint: "NTFS".into(),
        };
        let source = make_source(ntfs_vbr_sector(0), fs_size);
        let source_size = source.len() as u64;
        let mut target = vec![0u8; DISK_8M as usize];
        let mut src = Cursor::new(source);
        let mut tgt = Cursor::new(&mut target[..]);
        let result = wrap_and_write(
            &mut src,
            source_size,
            &mut tgt,
            &params,
            &mut no_progress(),
            &no_cancel(),
            &mut no_log(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_wrap_partition_smaller_than_disk_leaves_tail_zero() {
        let fs_size = 2 * ONE_MIB;
        let partition_size = 3 * ONE_MIB;
        let params = WrapParams {
            table: WrapTable::Mbr {
                type_byte: 0x07,
                bootable: false,
                alignment: WrapAlignment::Modern1MB,
            },
            partition_size_bytes: partition_size,
            target_size_bytes: DISK_8M,
            source_fs_hint: "NTFS".into(),
        };
        let src = make_source(ntfs_vbr_sector(0), fs_size);
        let out = wrap_to_vec(src, &params);

        // Last byte of disk should still be zero (untouched after partition end).
        assert_eq!(out[DISK_8M as usize - 1], 0);
        // A byte well past the partition end should also be zero.
        let after_partition = 2048 * SECTOR_SIZE as usize + partition_size as usize + 1024;
        assert_eq!(out[after_partition], 0);

        // Partition end matches the declared size: MBR sector count.
        let sectors = u32::from_le_bytes(out[446 + 12..446 + 16].try_into().unwrap());
        assert_eq!(sectors as u64, partition_size / 512);
    }

    #[test]
    fn test_wrap_progress_callback_fires() {
        let fs_size = 4 * ONE_MIB;
        let params = WrapParams {
            table: WrapTable::Mbr {
                type_byte: 0x07,
                bootable: false,
                alignment: WrapAlignment::Modern1MB,
            },
            partition_size_bytes: fs_size,
            target_size_bytes: DISK_8M,
            source_fs_hint: "NTFS".into(),
        };
        let source = make_source(ntfs_vbr_sector(0), fs_size);
        let source_size = source.len() as u64;
        let mut target = vec![0u8; DISK_8M as usize];
        let mut src = Cursor::new(source);
        let mut tgt = Cursor::new(&mut target[..]);
        let mut max_seen: u64 = 0;
        let mut count = 0;
        wrap_and_write(
            &mut src,
            source_size,
            &mut tgt,
            &params,
            &mut |b| {
                count += 1;
                max_seen = max_seen.max(b);
            },
            &no_cancel(),
            &mut no_log(),
        )
        .unwrap();
        assert!(count >= 1);
        assert_eq!(max_seen, fs_size);
    }

    #[test]
    fn test_wrap_cancel_aborts() {
        let fs_size = 4 * ONE_MIB;
        let params = WrapParams {
            table: WrapTable::Mbr {
                type_byte: 0x07,
                bootable: false,
                alignment: WrapAlignment::Modern1MB,
            },
            partition_size_bytes: fs_size,
            target_size_bytes: DISK_8M,
            source_fs_hint: "NTFS".into(),
        };
        let source = make_source(ntfs_vbr_sector(0), fs_size);
        let source_size = source.len() as u64;
        let mut target = vec![0u8; DISK_8M as usize];
        let mut src = Cursor::new(source);
        let mut tgt = Cursor::new(&mut target[..]);
        let result = wrap_and_write(
            &mut src,
            source_size,
            &mut tgt,
            &params,
            &mut no_progress(),
            &|| true,
            &mut no_log(),
        );
        assert!(result.is_err(), "cancel should abort");
    }

    #[test]
    fn test_suggest_ntfs_lba63_picks_dos_traditional_mbr() {
        let p = suggest_wrap_params("NTFS", 6 * ONE_MIB, 16 * ONE_MIB, Some(63));
        match p.table {
            WrapTable::Mbr {
                type_byte,
                alignment,
                ..
            } => {
                assert_eq!(type_byte, 0x07);
                assert_eq!(alignment, WrapAlignment::DosTraditional);
            }
            _ => panic!("expected MBR"),
        }
    }

    #[test]
    fn test_suggest_fat32_lba2048_picks_modern_mbr() {
        let p = suggest_wrap_params("FAT32", 6 * ONE_MIB, 16 * ONE_MIB, Some(2048));
        match p.table {
            WrapTable::Mbr {
                type_byte,
                alignment,
                ..
            } => {
                assert_eq!(type_byte, 0x0C);
                assert_eq!(alignment, WrapAlignment::Modern1MB);
            }
            _ => panic!("expected MBR"),
        }
    }

    #[test]
    fn test_suggest_huge_target_forces_gpt() {
        let p = suggest_wrap_params("NTFS", 100 * ONE_MIB, 3 * MBR_MAX_BYTES, Some(2048));
        assert!(matches!(p.table, WrapTable::Gpt { .. }));
    }

    #[test]
    fn test_suggest_unknown_fs_defaults_to_07() {
        let p = suggest_wrap_params("Unknown", 6 * ONE_MIB, 16 * ONE_MIB, None);
        match p.table {
            WrapTable::Mbr { type_byte, .. } => assert_eq!(type_byte, 0x07),
            _ => panic!("expected MBR"),
        }
    }

    #[test]
    fn test_mbr_type_byte_mapping() {
        assert_eq!(mbr_type_byte_for_fs("NTFS"), 0x07);
        assert_eq!(mbr_type_byte_for_fs("exFAT"), 0x07);
        assert_eq!(mbr_type_byte_for_fs("FAT32"), 0x0C);
        assert_eq!(mbr_type_byte_for_fs("FAT16"), 0x06);
        assert_eq!(mbr_type_byte_for_fs("FAT12"), 0x01);
        assert_eq!(mbr_type_byte_for_fs("ext4"), 0x83);
        assert_eq!(mbr_type_byte_for_fs("Linux"), 0x83);
        assert_eq!(mbr_type_byte_for_fs("xfs"), 0x83);
        assert_eq!(mbr_type_byte_for_fs("HFS+"), 0xAF);
        assert_eq!(mbr_type_byte_for_fs("???"), 0x07);
    }
}
