//! What an APM disk keeps outside its data partitions, saved as `apm-extras.bin`.
//!
//! Mac drivers and patches live in partitions of their own that the backup's partition list leaves
//! out, and a Mac cannot start up from a disk without them. The sidecar holds those bodies back to
//! back, in map order; `apm.json` says where each one goes.

use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use anyhow::{Context, Result};
use part_tables::apm::Apm;
use part_tables::PartitionSizeOverride;

/// Sidecar filename inside the backup folder.
pub const FILE_NAME: &str = "apm-extras.bin";

/// Byte start and length of each driver or patch partition, in map order.
pub fn regions(apm: &Apm) -> Vec<(u64, u64)> {
    let block = apm.ddr.block_size as u64;
    apm.entries
        .iter()
        .filter(|e| {
            !e.is_data_partition()
                && !matches!(
                    e.partition_type.as_str(),
                    "Apple_partition_map" | "Apple_Free" | "Apple_Void"
                )
                && e.block_count > 0
        })
        .map(|e| (e.start_block as u64 * block, e.block_count as u64 * block))
        .collect()
}

/// Stream the regions into the sidecar; returns the bytes saved, 0 when the disk has none.
pub fn export(source: &mut (impl Read + Seek), apm: &Apm, folder: &Path) -> Result<u64> {
    let regions = regions(apm);
    if regions.is_empty() {
        return Ok(0);
    }
    let path = folder.join(FILE_NAME);
    let mut out = std::fs::File::create(&path)
        .with_context(|| format!("failed to create {}", path.display()))?;
    let mut total = 0;
    for (start, len) in regions {
        source.seek(SeekFrom::Start(start))?;
        let copied = io::copy(&mut (&mut *source).take(len), &mut out)?;
        anyhow::ensure!(copied == len, "the disk ends inside a driver partition");
        total += len;
    }
    Ok(total)
}

/// True when this region would land on a restored partition or past the target.
fn collides(start: u64, len: u64, overrides: &[PartitionSizeOverride], target_size: u64) -> bool {
    start + len > target_size
        || overrides.iter().any(|o| {
            let begin = o.effective_start_lba() * 512;
            start < begin + o.export_size && begin < start + len
        })
}

/// Write the sidecar's regions back; false when the backup has no sidecar.
pub fn restore(
    folder: &Path,
    apm: &Apm,
    overrides: &[PartitionSizeOverride],
    target_size: u64,
    writer: &mut (impl Write + Seek),
    log_cb: &mut impl FnMut(&str),
) -> Result<bool> {
    let path = folder.join(FILE_NAME);
    if !path.exists() {
        return Ok(false);
    }
    let mut file =
        std::fs::File::open(&path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut at = 0;
    for (start, len) in regions(apm) {
        file.seek(SeekFrom::Start(at))?;
        at += len;
        put(
            &mut file,
            start,
            len,
            overrides,
            target_size,
            writer,
            log_cb,
        )?;
    }
    Ok(true)
}

/// Copy the regions from a whole-disk source, whose partitions may have moved on the target.
pub fn copy_from(
    source: &mut (impl Read + Seek),
    apm: &Apm,
    overrides: &[PartitionSizeOverride],
    target_size: u64,
    writer: &mut (impl Write + Seek),
    log_cb: &mut impl FnMut(&str),
) -> Result<()> {
    for (start, len) in regions(apm) {
        source.seek(SeekFrom::Start(start))?;
        put(source, start, len, overrides, target_size, writer, log_cb)?;
    }
    Ok(())
}

fn put(
    from: &mut impl Read,
    start: u64,
    len: u64,
    overrides: &[PartitionSizeOverride],
    target_size: u64,
    writer: &mut (impl Write + Seek),
    log_cb: &mut impl FnMut(&str),
) -> Result<()> {
    if collides(start, len, overrides, target_size) {
        log_cb(&format!(
            "Warning: the driver partition at byte {start} no longer fits the new layout and was \
             left out"
        ));
        return Ok(());
    }
    writer.seek(SeekFrom::Start(start))?;
    let copied = io::copy(&mut from.take(len), writer)?;
    anyhow::ensure!(
        copied == len,
        "{FILE_NAME} is shorter than the partition map says"
    );
    log_cb(&format!("Restored {len} driver byte(s) at byte {start}"));
    Ok(())
}

/// True when the disk had a Mac driver and this per-partition backup did not keep it.
pub fn driver_missing(folder: &Path, apm: &Apm) -> bool {
    apm.ddr.driver_count > 0 && !folder.join(FILE_NAME).exists()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn disk() -> (Apm, Vec<u8>) {
        let apm = part_tables::apm::build_minimal_apm(
            &[
                ("Apple_Driver43".to_string(), 64, 8),
                ("Apple_HFS".to_string(), 96, 100),
                ("Apple_Patches".to_string(), 200, 4),
            ],
            512,
            400,
        );
        let mut raw = vec![0u8; 400 * 512];
        raw[64 * 512..72 * 512].fill(0xD1);
        raw[96 * 512..196 * 512].fill(0x55);
        raw[200 * 512..204 * 512].fill(0xD2);
        (apm, raw)
    }

    fn override_at(start_lba: u64, sectors: u64) -> PartitionSizeOverride {
        PartitionSizeOverride {
            index: 2,
            start_lba,
            original_size: sectors * 512,
            export_size: sectors * 512,
            new_start_lba: None,
            heads: 0,
            sectors_per_track: 0,
        }
    }

    #[test]
    fn only_drivers_and_patches_are_kept() {
        let (apm, _) = disk();
        assert_eq!(
            regions(&apm),
            vec![(64 * 512, 8 * 512), (200 * 512, 4 * 512)]
        );
    }

    #[test]
    fn the_sidecar_puts_every_region_back_where_it_was() {
        let (apm, raw) = disk();
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            export(&mut Cursor::new(&raw), &apm, dir.path()).unwrap(),
            12 * 512
        );
        let mut out = Cursor::new(vec![0u8; raw.len()]);
        let ovs = [override_at(96, 100)];
        assert!(restore(dir.path(), &apm, &ovs, 400 * 512, &mut out, &mut |_| {}).unwrap());
        let out = out.into_inner();
        assert_eq!(out[64 * 512..72 * 512], raw[64 * 512..72 * 512]);
        assert_eq!(out[200 * 512..204 * 512], raw[200 * 512..204 * 512]);
        assert!(out[96 * 512..196 * 512].iter().all(|&b| b == 0));
    }

    #[test]
    fn a_region_that_would_hit_a_partition_is_left_out_and_said_so() {
        let (apm, raw) = disk();
        let mut out = Cursor::new(vec![0u8; raw.len()]);
        let mut logged = Vec::new();
        let ovs = [override_at(96, 250)];
        copy_from(
            &mut Cursor::new(&raw),
            &apm,
            &ovs,
            400 * 512,
            &mut out,
            &mut |m| logged.push(m.to_string()),
        )
        .unwrap();
        let out = out.into_inner();
        assert_eq!(out[64 * 512], 0xD1);
        assert_eq!(out[200 * 512], 0, "the grown partition keeps its bytes");
        assert!(
            logged.iter().any(|m| m.starts_with("Warning:")),
            "{logged:?}"
        );
    }

    #[test]
    fn a_backup_without_the_sidecar_says_its_driver_is_missing() {
        let (mut apm, raw) = disk();
        let dir = tempfile::tempdir().unwrap();
        assert!(!driver_missing(dir.path(), &apm), "no driver in the DDR");
        apm.ddr.driver_count = 1;
        assert!(driver_missing(dir.path(), &apm));
        export(&mut Cursor::new(&raw), &apm, dir.path()).unwrap();
        assert!(!driver_missing(dir.path(), &apm));
    }
}
