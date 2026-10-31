//! Moved to the `fs-mac` crate; re-exported so `crate::fs::hfs_clone` paths keep working.

pub use fs_mac::hfs_clone::*;

#[cfg(feature = "rust173-polyfill")]
use crate::rust173_compat::IntIsMultipleOf as _;
use std::io::{Read, Seek, SeekFrom, Write};

use crate::fs::FilesystemError;
use crate::partition::apm::{Apm, ApmPartitionEntry, DriverDescriptorRecord};

/// Summary returned by [`emit_apm_disk_with_hfs`].
#[derive(Debug, Clone)]
pub struct EmitReport {
    /// Total bytes written to the output (DDR + map + drivers + HFS partition).
    pub total_bytes: u64,
    /// Number of `Apple_Driver*` partitions copied verbatim from the source.
    pub drivers_copied: u32,
    /// Block on which the new HFS partition starts (in 512-byte units).
    pub hfs_start_block: u32,
    /// Length of the new HFS partition in 512-byte blocks.
    pub hfs_block_count: u32,
}

/// APM partition types that should be copied verbatim from the source disk
/// when wrapping a cloned HFS partition.
fn is_driver_type(t: &str) -> bool {
    matches!(
        t,
        "Apple_Driver"
            | "Apple_Driver43"
            | "Apple_Driver43_CD"
            | "Apple_Driver_ATA"
            | "Apple_Driver_ATAPI"
            | "Apple_Patches"
            | "Apple_FWDriver"
    )
}

/// Wrap a freshly-cloned HFS partition in a new APM disk image.
///
/// Step 6 of `docs/hfs_expand_block_size.md`. The output is a complete
/// raw disk: DDR + partition map + driver partitions copied byte-for-byte
/// from the source + the new `Apple_HFS` partition holding `hfs_image`.
///
/// `source_disk` is read-only; only driver partitions and the original HFS
/// partition's start block are inspected. `hfs_image` is the
/// already-populated HFS volume (typically produced by [`clone_hfs_volume`]
/// onto a buffer from `create_blank_hfs`). It is written verbatim at the
/// new partition's offset and must be a multiple of 512 bytes.
///
/// The new HFS partition starts at the same block as the source's first
/// `Apple_HFS` partition when one exists; otherwise it starts immediately
/// after the last driver partition (or at block 64 if there are none),
/// matching the layout Apple's tools produce.
///
/// A non-APM source (a raw single-partition HFS image with no DDR at sector
/// 0) is accepted: drivers come up empty, no `Apple_HFS` self-entry is
/// mimicked, and the output is a fresh APM-wrapped disk with just one
/// `Apple_HFS` partition at block 64.
pub fn emit_apm_disk_with_hfs<R, W>(
    source_disk: &mut R,
    source_data_size: u64,
    hfs_image: &[u8],
    output: &mut W,
) -> Result<EmitReport, FilesystemError>
where
    R: Read + Seek,
    W: Read + Write + Seek,
{
    if !hfs_image.len().is_multiple_of(512) {
        return Err(FilesystemError::InvalidData(format!(
            "hfs_image length {} is not a multiple of 512",
            hfs_image.len()
        )));
    }
    let hfs_block_count_u64 = (hfs_image.len() / 512) as u64;
    if hfs_block_count_u64 == 0 {
        return Err(FilesystemError::InvalidData(
            "hfs_image is empty".to_string(),
        ));
    }

    // Source APM is optional: a raw single-partition HFS image (no DDR at
    // sector 0) is a valid input too. When parsing fails we treat the source
    // as having no APM — no drivers to carry over, no source HFS partition
    // to mimic, no self-entry to clone. The result is a fresh APM with one
    // Apple_HFS partition at block 64.
    let source_apm = Apm::parse(source_disk).ok();
    if let Some(apm) = &source_apm {
        let block_size = apm.ddr.block_size as u64;
        if block_size != 512 {
            return Err(FilesystemError::InvalidData(format!(
                "unsupported APM block size {block_size}; only 512 is handled"
            )));
        }
    }

    // Collect driver partitions from the source, in start-block order.
    let mut drivers: Vec<ApmPartitionEntry> = source_apm
        .as_ref()
        .map(|apm| {
            apm.entries
                .iter()
                .filter(|e| is_driver_type(&e.partition_type))
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    drivers.sort_by_key(|d| d.start_block);

    // HFS start block: prefer the source's first Apple_HFS start so emulators
    // that hard-code partition layouts still recognise it; otherwise place it
    // right after the last driver, with a minimum of block 64.
    let drivers_end: u32 = drivers
        .iter()
        .map(|d| d.start_block.saturating_add(d.block_count))
        .max()
        .unwrap_or(0);
    let source_hfs_start: Option<u32> = source_apm
        .as_ref()
        .and_then(|apm| apm.entries.iter().find(|e| e.partition_type == "Apple_HFS"))
        .map(|e| e.start_block);

    // The APM map itself is `1 + map_entries` blocks (DDR + entries) starting
    // at block 0; first usable block is `1 + map_entries`. Plan for the worst
    // case (1 self-entry + drivers + 1 hfs).
    let min_after_map = 1 + (1 + drivers.len() as u32 + 1);
    let hfs_start_block = match source_hfs_start {
        Some(s) if s >= drivers_end.max(min_after_map) => s,
        _ => drivers_end.max(min_after_map).max(64),
    };

    let hfs_block_count: u32 = hfs_block_count_u64.try_into().map_err(|_| {
        FilesystemError::InvalidData(format!(
            "hfs_image too large for u32 block count: {} bytes",
            hfs_image.len()
        ))
    })?;
    let target_block_count: u32 = hfs_start_block
        .checked_add(hfs_block_count)
        .ok_or_else(|| FilesystemError::InvalidData("APM total block count overflow".into()))?;

    // Build the new APM. One self-referencing partition_map entry, then each
    // driver verbatim, then the new HFS partition.
    let map_entries: u32 = 1 + drivers.len() as u32 + 1;
    let mut entries: Vec<ApmPartitionEntry> = Vec::with_capacity(map_entries as usize);
    // Preserve the source's Apple_partition_map self-entry verbatim: keep its
    // reserved block_count (typically 63 on classic Mac disks) and status
    // flags (0x37 on bootable Quadra disks). Some Mac ROMs reject the disk
    // if the self-entry's status is zero.
    let source_self = source_apm.as_ref().and_then(|apm| {
        apm.entries
            .iter()
            .find(|e| e.partition_type == "Apple_partition_map")
    });
    let map_block_count = source_self.map(|e| e.block_count).unwrap_or(map_entries);
    entries.push(ApmPartitionEntry {
        signature: 0x504D,
        map_entries,
        start_block: 1,
        block_count: map_block_count,
        name: source_self
            .map(|e| e.name.clone())
            .unwrap_or_else(|| "Apple".to_string()),
        partition_type: "Apple_partition_map".to_string(),
        data_start: 0,
        data_count: map_block_count,
        status: source_self.map(|e| e.status).unwrap_or(0x37),
        boot_start: source_self.map(|e| e.boot_start).unwrap_or(0),
        boot_size: source_self.map(|e| e.boot_size).unwrap_or(0),
        boot_load: source_self.map(|e| e.boot_load).unwrap_or(0),
        boot_entry: source_self.map(|e| e.boot_entry).unwrap_or(0),
        boot_checksum: source_self.map(|e| e.boot_checksum).unwrap_or(0),
        processor: source_self.map(|e| e.processor.clone()).unwrap_or_default(),
        pad: source_self.map(|e| e.pad.clone()).unwrap_or_default(),
    });
    for d in &drivers {
        let mut e = d.clone();
        e.map_entries = map_entries;
        entries.push(e);
    }
    // Preserve the source's boot-related fields (status + pmBoot* + processor
    // + name) on the new Apple_HFS entry. Quadra ROMs read these out of the
    // partition map at boot, so a bootable source disk's flags must carry
    // through to the expanded copy. Fall back to 0x33 / "MacOS" only when
    // the source has no Apple_HFS entry.
    let source_hfs = source_apm
        .as_ref()
        .and_then(|apm| apm.entries.iter().find(|e| e.partition_type == "Apple_HFS"));
    entries.push(ApmPartitionEntry {
        signature: 0x504D,
        map_entries,
        start_block: hfs_start_block,
        block_count: hfs_block_count,
        name: source_hfs
            .map(|e| e.name.clone())
            .unwrap_or_else(|| "MacOS".to_string()),
        partition_type: "Apple_HFS".to_string(),
        data_start: 0,
        data_count: hfs_block_count,
        status: source_hfs.map(|e| e.status).unwrap_or(0x33),
        boot_start: source_hfs.map(|e| e.boot_start).unwrap_or(0),
        boot_size: source_hfs.map(|e| e.boot_size).unwrap_or(0),
        boot_load: source_hfs.map(|e| e.boot_load).unwrap_or(0),
        boot_entry: source_hfs.map(|e| e.boot_entry).unwrap_or(0),
        boot_checksum: source_hfs.map(|e| e.boot_checksum).unwrap_or(0),
        processor: source_hfs.map(|e| e.processor.clone()).unwrap_or_default(),
        pad: source_hfs.map(|e| e.pad.clone()).unwrap_or_default(),
    });

    // Reuse the source's DDR when present (preserves SCSI driver descriptor
    // entries the Mac ROM needs to load drivers at boot). For a non-APM
    // source there's nothing to preserve — build a minimal DDR that matches
    // what `build_minimal_apm` produces for a fresh disk.
    let mut new_apm = source_apm.unwrap_or_else(|| Apm {
        ddr: DriverDescriptorRecord {
            signature: 0x4552,
            block_size: 512,
            block_count: target_block_count,
            dev_type: 0,
            dev_id: 0,
            sb_data: 0,
            driver_count: 0,
            driver_info: Vec::new(),
        },
        entries: Vec::new(),
        map_entry_count: 0,
        ddr_present: true,
    });
    new_apm.entries = entries;
    new_apm.map_entry_count = map_entries;
    new_apm.ddr.block_count = target_block_count;
    new_apm.ddr.block_size = 512;

    let apm_blocks = new_apm.build_apm_blocks(Some(target_block_count));

    // Layout writers linearly: DDR+map at offset 0, then each entry at its
    // start_block*512 with zero-fill between. Output supports Seek so we can
    // skip ahead, but we still write the trailing region to set the file
    // length correctly.
    output.seek(SeekFrom::Start(0))?;
    output.write_all(&apm_blocks)?;
    let mut cursor: u64 = apm_blocks.len() as u64;

    // Sort write-targets by start offset so we can zero-fill any gap simply.
    struct Region {
        start: u64,
        bytes: Vec<u8>,
    }
    let mut regions: Vec<Region> = Vec::new();
    let mut buf = vec![0u8; 64 * 1024];
    for d in &drivers {
        let src_off = d.start_block as u64 * 512;
        let len = d.block_count as u64 * 512;
        let copy_bound = len.min(source_data_size.saturating_sub(src_off));
        let mut data = vec![0u8; len as usize];
        if copy_bound > 0 {
            source_disk.seek(SeekFrom::Start(src_off))?;
            let mut filled: u64 = 0;
            while filled < copy_bound {
                let want = ((copy_bound - filled) as usize).min(buf.len());
                let n = source_disk.read(&mut buf[..want])?;
                if n == 0 {
                    break;
                }
                data[filled as usize..filled as usize + n].copy_from_slice(&buf[..n]);
                filled += n as u64;
            }
        }
        regions.push(Region {
            start: d.start_block as u64 * 512,
            bytes: data,
        });
    }
    regions.push(Region {
        start: hfs_start_block as u64 * 512,
        bytes: hfs_image.to_vec(),
    });
    regions.sort_by_key(|r| r.start);

    for region in regions {
        if region.start < cursor {
            return Err(FilesystemError::InvalidData(format!(
                "APM emit: region at {} overlaps already-written data (cursor at {})",
                region.start, cursor
            )));
        }
        if region.start > cursor {
            let gap = region.start - cursor;
            let zeros = vec![0u8; 64 * 1024];
            let mut remaining = gap;
            while remaining > 0 {
                let n = (remaining as usize).min(zeros.len());
                output.write_all(&zeros[..n])?;
                remaining -= n as u64;
            }
            cursor = region.start;
        }
        output.write_all(&region.bytes)?;
        cursor += region.bytes.len() as u64;
    }

    Ok(EmitReport {
        total_bytes: cursor,
        drivers_copied: drivers.len() as u32,
        hfs_start_block,
        hfs_block_count,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::hfs::HfsFilesystem;
    use std::io::Cursor;

    /// Build a synthetic raw APM source disk with a partition_map entry, an
    /// `Apple_Driver43` partition with recognisable bytes, and an `Apple_HFS`
    /// partition. Used to verify the emit path preserves drivers and lays out
    /// the HFS partition at the right block.
    fn build_synthetic_apm_disk() -> Vec<u8> {
        use crate::partition::apm::build_minimal_apm;
        let total_blocks: u32 = 512; // 256 KiB total
        let driver_start: u32 = 32;
        let driver_blocks: u32 = 8;
        let hfs_start: u32 = 64;
        let hfs_blocks: u32 = total_blocks - hfs_start;
        let mut apm = build_minimal_apm(
            &[
                ("Apple_Driver43".to_string(), driver_start, driver_blocks),
                ("Apple_HFS".to_string(), hfs_start, hfs_blocks),
            ],
            512,
            total_blocks,
        );
        apm.entries[1].name = "Macintosh".to_string();
        apm.entries[2].name = "MacOS".to_string();
        let blocks = apm.build_apm_blocks(Some(total_blocks));

        let mut disk = vec![0u8; total_blocks as usize * 512];
        disk[..blocks.len()].copy_from_slice(&blocks);
        // Stamp the driver partition with a recognisable byte pattern so the
        // emit test can verify byte-for-byte preservation.
        for i in 0..(driver_blocks as usize * 512) {
            disk[driver_start as usize * 512 + i] = (i as u8).wrapping_mul(13).wrapping_add(7);
        }
        disk
    }

    #[test]
    fn emit_apm_disk_round_trip() {
        let source_disk = build_synthetic_apm_disk();
        let source_len = source_disk.len() as u64;

        // Cloned HFS partition. Use create_blank_hfs to produce a syntactically
        // valid (and fsck-clean) HFS volume of a chosen size. The emit step
        // doesn't care that the volume is empty, only that the bytes are valid.
        let hfs_image =
            crate::fs::hfs::create_blank_hfs(2 * 1024 * 1024, 4096, "Expanded").unwrap();

        let mut src_cursor = Cursor::new(source_disk.clone());
        let mut output: Vec<u8> = Vec::new();
        let mut out_cursor = Cursor::new(&mut output);
        let report =
            emit_apm_disk_with_hfs(&mut src_cursor, source_len, &hfs_image, &mut out_cursor)
                .expect("emit succeeds");

        assert_eq!(report.drivers_copied, 1);
        // Driver was at block 32 + 8 blocks; min_after_map = 1 + (1+1+1) = 4;
        // source HFS started at block 64, which is >= driver_end(40) and >= 64.
        assert_eq!(report.hfs_start_block, 64);
        assert_eq!(report.hfs_block_count, hfs_image.len() as u32 / 512);
        assert_eq!(
            report.total_bytes,
            (report.hfs_start_block as u64 + report.hfs_block_count as u64) * 512
        );

        // Re-parse the emitted disk's APM and verify the layout.
        let mut cursor = Cursor::new(&output);
        let parsed = Apm::parse(&mut cursor).expect("emitted APM parses");
        assert_eq!(parsed.ddr.block_size, 512);
        assert_eq!(parsed.ddr.block_count, report.total_bytes as u32 / 512);
        assert_eq!(parsed.entries.len(), 3);
        assert_eq!(parsed.entries[0].partition_type, "Apple_partition_map");
        assert_eq!(parsed.entries[1].partition_type, "Apple_Driver43");
        assert_eq!(parsed.entries[1].start_block, 32);
        assert_eq!(parsed.entries[1].block_count, 8);
        assert_eq!(parsed.entries[2].partition_type, "Apple_HFS");
        assert_eq!(parsed.entries[2].start_block, 64);

        // Driver bytes preserved verbatim.
        let drv_off = 32 * 512;
        let drv_len = 8 * 512;
        assert_eq!(
            &output[drv_off..drv_off + drv_len],
            &source_disk[drv_off..drv_off + drv_len]
        );

        // HFS bytes written at the right offset.
        let hfs_off = report.hfs_start_block as usize * 512;
        assert_eq!(&output[hfs_off..hfs_off + hfs_image.len()], &hfs_image[..]);

        // The wrapped HFS partition opens and is fsck-clean.
        let mut fs =
            HfsFilesystem::open(Cursor::new(&output), report.hfs_start_block as u64 * 512).unwrap();
        let fsck = fs.fsck().expect("HFS supports fsck");
        assert!(
            fsck.is_clean(),
            "fsck errors on emitted HFS partition: {:?}",
            fsck.errors
                .iter()
                .map(|e| e.code.as_str())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn emit_rejects_non_512_aligned_image() {
        let source_disk = build_synthetic_apm_disk();
        let source_len = source_disk.len() as u64;
        let bad_image = vec![0u8; 511];
        let mut src = Cursor::new(source_disk);
        let mut out: Vec<u8> = Vec::new();
        let mut out_cursor = Cursor::new(&mut out);
        let err = emit_apm_disk_with_hfs(&mut src, source_len, &bad_image, &mut out_cursor)
            .expect_err("non-512-aligned image must be rejected");
        assert!(matches!(err, FilesystemError::InvalidData(_)));
    }

    /// A raw single-partition HFS image (a `partition-0.img` with no APM
    /// wrapper) is a valid source for `emit_apm_disk_with_hfs`: it produces
    /// a fresh APM-wrapped disk with one `Apple_HFS` partition at block 64
    /// and zero drivers carried over.
    #[test]
    fn emit_apm_from_non_apm_source() {
        // Source: bare HFS bytes, no DDR at sector 0.
        let source_disk = crate::fs::hfs::create_blank_hfs(1024 * 1024, 4096, "Source").unwrap();
        let source_len = source_disk.len() as u64;
        // Sanity check: Apm::parse must refuse this — otherwise the test
        // isn't exercising the non-APM fallback.
        assert!(Apm::parse(&mut Cursor::new(&source_disk)).is_err());

        // Cloned HFS payload (the "expanded" volume).
        let hfs_image =
            crate::fs::hfs::create_blank_hfs(2 * 1024 * 1024, 4096, "Expanded").unwrap();

        let mut src_cursor = Cursor::new(source_disk);
        let mut output: Vec<u8> = Vec::new();
        let mut out_cursor = Cursor::new(&mut output);
        let report =
            emit_apm_disk_with_hfs(&mut src_cursor, source_len, &hfs_image, &mut out_cursor)
                .expect("emit succeeds against a non-APM source");

        assert_eq!(report.drivers_copied, 0);
        // min_after_map = 1 + (1 + 0 + 1) = 3; drivers_end = 0; falls back
        // to the block-64 floor.
        assert_eq!(report.hfs_start_block, 64);
        assert_eq!(report.hfs_block_count, hfs_image.len() as u32 / 512);

        // Re-parse the emitted disk: DDR + self-entry + Apple_HFS only.
        let mut cursor = Cursor::new(&output);
        let parsed = Apm::parse(&mut cursor).expect("emitted APM parses");
        assert_eq!(parsed.ddr.block_size, 512);
        assert_eq!(parsed.entries.len(), 2);
        assert_eq!(parsed.entries[0].partition_type, "Apple_partition_map");
        assert_eq!(parsed.entries[1].partition_type, "Apple_HFS");
        assert_eq!(parsed.entries[1].start_block, 64);
        assert_eq!(parsed.entries[1].name, "MacOS");

        // HFS partition is fsck-clean inside the wrapper.
        let mut fs =
            HfsFilesystem::open(Cursor::new(&output), report.hfs_start_block as u64 * 512).unwrap();
        let fsck = fs.fsck().expect("HFS supports fsck");
        assert!(
            fsck.is_clean(),
            "fsck errors on emitted HFS partition: {:?}",
            fsck.errors
                .iter()
                .map(|e| e.code.as_str())
                .collect::<Vec<_>>()
        );
    }
}
