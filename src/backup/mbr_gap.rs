//! The sectors between the MBR and the first partition, kept as `mbr-gap.bin`.
//!
//! Sector 0 alone is not the whole boot path on a PC disk: GRUB embeds
//! `core.img` from LBA 1, a Dynamic Drive Overlay lives in the first track,
//! and OS/2 Boot Manager and some virus-scanner boot code sit there too. A
//! backup that keeps only the MBR restores a disk whose MBR jumps into zeros.
//! The gap is bounded to one 1 MiB alignment unit and trailing zero sectors
//! are dropped, so an ordinary disk stores nothing or a few KiB.

use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use anyhow::{Context, Result};

use crate::partition::PartitionInfo;

/// Sidecar filename inside the backup folder.
pub const FILE_NAME: &str = "mbr-gap.bin";

/// Longest gap captured: the 1 MiB unit modern tools align the first partition to.
pub const MAX_GAP_SECTORS: u64 = 2048;

/// Sectors between the MBR and the first primary partition, capped at `MAX_GAP_SECTORS`.
pub fn gap_sectors_before_first_partition(partitions: &[PartitionInfo]) -> u64 {
    partitions
        .iter()
        .filter(|p| !p.is_logical && p.start_lba > 0)
        .map(|p| p.start_lba)
        .min()
        .map(|first| first.min(MAX_GAP_SECTORS + 1) - 1)
        .unwrap_or(0)
}

/// Read the gap from LBA 1; None when it holds nothing but zeros.
pub fn read_gap(reader: &mut (impl Read + Seek), gap_sectors: u64) -> Result<Option<Vec<u8>>> {
    if gap_sectors == 0 {
        return Ok(None);
    }
    reader
        .seek(SeekFrom::Start(512))
        .context("seek to the MBR gap")?;
    let mut buf = vec![0u8; (gap_sectors * 512) as usize];
    let mut filled = 0usize;
    while filled < buf.len() {
        match reader.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e).context("read the MBR gap"),
        }
    }
    buf.truncate(filled);
    Ok(trim_zero_sectors(buf))
}

/// Drop trailing all-zero sectors; None when nothing is left.
pub fn trim_zero_sectors(mut gap: Vec<u8>) -> Option<Vec<u8>> {
    let last_data = gap.iter().rposition(|&b| b != 0)?;
    gap.truncate((last_data / 512 + 1) * 512);
    Some(gap)
}

/// Write the sidecar.
pub fn export(folder: &Path, gap: &[u8]) -> Result<()> {
    let path = folder.join(FILE_NAME);
    std::fs::write(&path, gap).with_context(|| format!("failed to write {}", path.display()))
}

/// Read the sidecar when the backup has one.
pub fn load(folder: &Path) -> Result<Option<Vec<u8>>> {
    let path = folder.join(FILE_NAME);
    if !path.exists() {
        return Ok(None);
    }
    let data =
        std::fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
    Ok(if data.is_empty() { None } else { Some(data) })
}

/// The leading part of `gap` that still fits below `first_partition_lba`.
pub fn clamp_to_first_partition(gap: &[u8], first_partition_lba: u64) -> &[u8] {
    let room = first_partition_lba.saturating_sub(1).saturating_mul(512);
    let keep = (gap.len() as u64).min(room) as usize;
    &gap[..keep]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn part(index: usize, start_lba: u64, is_logical: bool) -> PartitionInfo {
        PartitionInfo {
            index,
            type_name: String::new(),
            partition_type_byte: 0x0C,
            start_lba,
            start_byte: None,
            size_bytes: 512,
            bootable: false,
            is_logical,
            is_extended_container: false,
            partition_type_string: None,
            hfs_block_size: None,
            rdb_part_block: None,
            drv_name: None,
        }
    }

    #[test]
    fn gap_is_bounded_by_the_first_primary_and_the_1mib_cap() {
        assert_eq!(gap_sectors_before_first_partition(&[]), 0);
        assert_eq!(
            gap_sectors_before_first_partition(&[part(0, 63, false)]),
            62
        );
        assert_eq!(
            gap_sectors_before_first_partition(&[part(0, 2048, false)]),
            2047
        );
        assert_eq!(
            gap_sectors_before_first_partition(&[part(0, 1_000_000, false)]),
            2048
        );
        // Logicals sit inside the extended container; only primaries bound the gap.
        let parts = [part(0, 2048, false), part(4, 100, true)];
        assert_eq!(gap_sectors_before_first_partition(&parts), 2047);
    }

    #[test]
    fn read_gap_keeps_boot_code_and_drops_the_zero_tail() {
        let mut disk = vec![0u8; 64 * 512];
        disk[512..520].copy_from_slice(b"CORE.IMG");
        disk[3 * 512 + 7] = 0xAA;
        let gap = read_gap(&mut Cursor::new(disk.clone()), 62)
            .unwrap()
            .unwrap();
        assert_eq!(gap.len(), 3 * 512, "ends at the last non-zero sector");
        assert_eq!(&gap[..8], b"CORE.IMG");
        assert_eq!(gap[2 * 512 + 7], 0xAA);

        let blank = vec![0u8; 64 * 512];
        assert!(read_gap(&mut Cursor::new(blank), 62).unwrap().is_none());
        assert!(read_gap(&mut Cursor::new(disk), 0).unwrap().is_none());
    }

    #[test]
    fn clamp_keeps_what_fits_below_the_new_first_partition() {
        let gap = vec![1u8; 4 * 512];
        assert_eq!(clamp_to_first_partition(&gap, 2048).len(), 4 * 512);
        assert_eq!(clamp_to_first_partition(&gap, 3).len(), 2 * 512);
        assert!(clamp_to_first_partition(&gap, 1).is_empty());
        assert!(clamp_to_first_partition(&gap, 0).is_empty());
    }

    #[test]
    fn sidecar_round_trips_through_the_folder() {
        let dir = tempfile::tempdir().unwrap();
        assert!(load(dir.path()).unwrap().is_none());
        export(dir.path(), b"boot").unwrap();
        assert_eq!(load(dir.path()).unwrap().as_deref(), Some(&b"boot"[..]));
    }
}
