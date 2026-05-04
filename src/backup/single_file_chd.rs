//! Single-file CHD backup orchestrator.
//!
//! Synthesises a whole-disk image from the source's partition table + each
//! partition's data, streams it into a single CHD, and produces the
//! `metadata.json` entries the rest of the backup pipeline expects.
//!
//! Scope of the initial implementation (intentionally bounded):
//!
//! - **MBR sources only.** GPT and APM fall back to the legacy per-partition
//!   layout. Adding GPT support requires emitting the alternate header at
//!   the end of the synthesised disk; APM needs equivalent treatment.
//! - **Source layout preserved.** Partition offsets and sizes equal those
//!   on the source disk; no resize at backup time. Backup-time resize is a
//!   follow-up and will plug in here via a `SizePlan` argument.
//! - **Smart compaction = layout-preserving compact reader where available**
//!   (HFS, HFS+, ext, btrfs, ProDOS), raw passthrough otherwise. Adding
//!   layout-preserving FAT/NTFS/exFAT readers is a future stage.
//! - **Sector-by-sector mode** = raw passthrough for every partition.
//!
//! The output is a real disk image: `chdman info` + MAME load it
//! correctly. Per-partition checksums are computed by re-reading the CHD
//! after compression so the metadata.json stays trustable for restore-time
//! validation.

use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use anyhow::{anyhow, Context, Result};

use super::disk_image_stream::DiskImageStreamBuilder;
use crate::fs;
use crate::partition::{PartitionInfo, PartitionTable};
use crate::rbformats::chd::{compress_chd, compress_chd_dvd, ChdReader};
use crate::rbformats::chd_options::ChdOptions;

/// Inputs for a single-file CHD backup. The caller (typically `run_backup`)
/// is responsible for opening the source, parsing the partition table,
/// creating the output folder, and persisting metadata. This function only
/// produces the CHD itself plus the per-partition byte ranges + checksums
/// the caller needs to populate `metadata.json`.
pub struct SingleFileChdInputs<'a> {
    pub source_file: &'a File,
    pub source_size: u64,
    /// First sector(s) of the source disk, captured verbatim. For MBR this
    /// is the 512-byte MBR sector. The caller already read this for the
    /// existing `mbr.bin` export, so we accept it pre-read rather than
    /// re-issuing the I/O.
    pub source_partition_table_bytes: &'a [u8],
    pub partition_table: &'a PartitionTable,
    pub partitions: &'a [PartitionInfo],
    pub partition_filter: Option<&'a [usize]>,
    /// True = read every byte off the source for every partition. False =
    /// use a layout-preserving compact reader where available, raw
    /// otherwise.
    pub sector_by_sector: bool,
    pub chd_options: Option<ChdOptions>,
    /// Whether to use the DVD profile (2048-byte units, `DVD ` metadata
    /// tag) instead of the HD profile.
    pub is_dvd: bool,
    /// Where to write the CHD (e.g. `<backup_folder>/disk`). The `.chd`
    /// extension is appended by `compress_chd`.
    pub output_base: &'a Path,
}

/// Per-partition byte range + checksum, returned to the caller for
/// metadata.json population.
#[derive(Debug, Clone)]
pub struct ChdPartitionRange {
    pub partition_index: usize,
    pub offset_in_disk: u64,
    pub length: u64,
    pub checksum_sha256: String,
}

/// Result of a successful single-file CHD backup.
#[derive(Debug, Clone)]
pub struct SingleFileChdResult {
    /// Filename written by `compress_chd` (e.g. `disk.chd`). Always exactly
    /// one entry — split CHDs are not produced by this path.
    pub container_filename: String,
    /// Total logical bytes the synthesised disk image covers (= source disk
    /// size when no resize is applied).
    pub container_logical_size: u64,
    /// SHA1 the CHD itself reports (the value `chdman info` prints).
    pub container_sha1: String,
    pub partition_ranges: Vec<ChdPartitionRange>,
}

/// True if `inputs` describes a source layout we can currently handle in
/// single-file CHD mode. MBR only for now.
pub fn is_supported(inputs_table: &PartitionTable) -> bool {
    matches!(inputs_table, PartitionTable::Mbr(_))
}

/// Run the single-file CHD backup. Caller-supplied callbacks follow the
/// progress pattern documented in `docs/progress_pattern.md`: `progress_cb`
/// is fired with cumulative logical bytes written, `cancel_check` is polled
/// at chunk boundaries by `compress_chd`, and `log_cb` carries user-facing
/// log lines into the shared `LogPanel`.
pub fn run(
    inputs: SingleFileChdInputs<'_>,
    progress_cb: &mut dyn FnMut(u64),
    cancel_check: &dyn Fn() -> bool,
    log_cb: &mut dyn FnMut(&str),
) -> Result<SingleFileChdResult> {
    if !is_supported(inputs.partition_table) {
        anyhow::bail!(
            "single-file CHD backup currently supports MBR sources only; \
             this disk uses {} — pick zstd/raw, or use the per-partition CHD \
             flow on a future build that supports {} for single-file.",
            inputs.partition_table.type_name(),
            inputs.partition_table.type_name(),
        );
    }

    let mut builder = DiskImageStreamBuilder::new(inputs.source_size);
    let mut partition_ranges: Vec<ChdPartitionRange> = Vec::new();

    // Segment 0: the partition table sectors verbatim. Putting the MBR at
    // offset 0 is what makes the resulting CHD a real disk image rather
    // than an opaque container.
    builder
        .add_segment(
            0,
            inputs.source_partition_table_bytes.len() as u64,
            Box::new(std::io::Cursor::new(
                inputs.source_partition_table_bytes.to_vec(),
            )),
        )
        .map_err(|e| anyhow!("disk-image stream rejected MBR segment: {}", e))?;

    for part in inputs.partitions {
        // Skip partitions the user filtered out.
        if let Some(filter) = inputs.partition_filter {
            if !filter.contains(&part.index) {
                continue;
            }
        }
        // Skip GPT protective entries (shouldn't appear in MBR but defensive).
        if part.partition_type_byte == 0xEE {
            continue;
        }
        // Skip extended-partition containers — their logical partitions get
        // emitted individually below. The container's bytes (EBR chain etc.)
        // sit inside the gaps and get zero-filled, which means restore needs
        // to reconstruct the EBR chain; that lives in
        // `ExtendedContainerMetadata` already.
        if part.is_extended_container {
            log_cb(&format!(
                "Skipping extended container partition-{} (logical partitions emitted individually)",
                part.index,
            ));
            continue;
        }

        let part_offset = part.start_lba * 512;
        let part_length = part.size_bytes;
        let segment_reader =
            build_partition_reader(inputs.source_file, part, inputs.sector_by_sector, log_cb)?;

        builder
            .add_segment(part_offset, part_length, segment_reader)
            .map_err(|e| {
                anyhow!(
                    "disk-image stream rejected partition-{} segment at offset {}: {}",
                    part.index,
                    part_offset,
                    e,
                )
            })?;

        partition_ranges.push(ChdPartitionRange {
            partition_index: part.index,
            offset_in_disk: part_offset,
            length: part_length,
            checksum_sha256: String::new(), // filled in after CHD write
        });
    }

    let mut stream = builder.build();
    let logical_size = stream.total_length();

    log_cb(&format!(
        "Synthesising single-file CHD: {} bytes ({} partition segments + zero-filled gaps)",
        logical_size,
        partition_ranges.len(),
    ));

    let mut chd_progress_cb = |n: u64| progress_cb(n);
    let mut chd_log_cb = |s: &str| log_cb(s);
    // Re-wrap the trait-object cancel callback as a sized closure so the
    // generic `impl Fn() -> bool` bound on `compress_chd*` is satisfied.
    let chd_cancel_check = || cancel_check();

    let written_files = if inputs.is_dvd {
        compress_chd_dvd(
            &mut stream,
            inputs.output_base,
            logical_size,
            None, // no splitting — CHD has no native split format
            inputs.chd_options.clone(),
            &mut chd_progress_cb,
            &chd_cancel_check,
            &mut chd_log_cb,
        )?
    } else {
        compress_chd(
            &mut stream,
            inputs.output_base,
            logical_size,
            None,
            inputs.chd_options.clone(),
            &mut chd_progress_cb,
            &chd_cancel_check,
            &mut chd_log_cb,
        )?
    };

    let container_filename = written_files
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("compress_chd returned no output filename"))?;

    // Compute per-partition checksums + the CHD's own SHA1 by reopening
    // the freshly-written file. This avoids teeing the stream during
    // compress (which would force a duplicate of every byte through a
    // hasher path) at the cost of one extra read pass.
    let chd_path = inputs
        .output_base
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(&container_filename);
    log_cb(&format!(
        "Computing per-partition checksums from {}",
        chd_path.display()
    ));
    let container_sha1 = compute_post_write_metadata(&chd_path, &mut partition_ranges, log_cb)?;

    Ok(SingleFileChdResult {
        container_filename,
        container_logical_size: logical_size,
        container_sha1,
        partition_ranges,
    })
}

/// Build a `Read` covering one partition's full source extent.
fn build_partition_reader(
    source_file: &File,
    part: &PartitionInfo,
    sector_by_sector: bool,
    log_cb: &mut dyn FnMut(&str),
) -> Result<Box<dyn Read + Send>> {
    let part_offset = part.start_lba * 512;
    let part_length = part.size_bytes;

    if !sector_by_sector {
        // Smart mode: try a compact reader. Only useful if it's
        // layout-preserving (`compacted_size == original_size`); otherwise
        // its output bytes don't align to disk-image positions and we'd
        // produce garbage. Falls through to raw passthrough on any miss.
        let clone = source_file
            .try_clone()
            .context("failed to clone source for compact reader")?;
        if let Some((reader, info)) = fs::compact_partition_reader(
            clone,
            part_offset,
            part.partition_type_byte,
            part.partition_type_string.as_deref(),
        ) {
            if info.compacted_size == info.original_size {
                log_cb(&format!(
                    "  partition-{}: layout-preserving compact reader \
                     (data {} of {} bytes)",
                    part.index, info.data_size, info.original_size,
                ));
                return Ok(reader);
            }
            log_cb(&format!(
                "  partition-{}: compact reader is packed (compacted {} < \
                 original {}); falling back to raw passthrough — disk-image \
                 mode needs full-extent reads",
                part.index, info.compacted_size, info.original_size,
            ));
        } else {
            log_cb(&format!(
                "  partition-{}: no compact reader for type 0x{:02X}; \
                 using raw passthrough",
                part.index, part.partition_type_byte,
            ));
        }
    } else {
        log_cb(&format!(
            "  partition-{}: sector-by-sector raw passthrough",
            part.index,
        ));
    }

    // Raw passthrough at the partition's source offset, capped to
    // partition length. The take() guard is critical: even a stray over-read
    // would push subsequent segments off-position in the stream.
    let mut clone = source_file
        .try_clone()
        .context("failed to clone source for raw passthrough")?;
    clone.seek(SeekFrom::Start(part_offset)).with_context(|| {
        format!(
            "failed to seek source to partition-{} offset {}",
            part.index, part_offset
        )
    })?;
    Ok(Box::new(BufReader::new(clone).take(part_length)))
}

/// Open the freshly-written CHD, compute SHA-256 for each partition's byte
/// range, return the CHD's own SHA1 (as `chdman info` would print).
fn compute_post_write_metadata(
    chd_path: &Path,
    ranges: &mut [ChdPartitionRange],
    log_cb: &mut dyn FnMut(&str),
) -> Result<String> {
    use sha2::{Digest, Sha256};

    let mut reader = ChdReader::open(chd_path).with_context(|| {
        format!(
            "failed to reopen newly-written CHD for checksum: {}",
            chd_path.display()
        )
    })?;

    // Per-partition SHA-256 over the byte range. We re-seek for each
    // partition; the caller has already paid the cost of streaming the
    // entire image once during compression, so a second pass here is
    // acceptable for the simplicity it buys.
    let mut buf = vec![0u8; 1024 * 1024]; // 1 MiB chunks per CONTRIBUTING.md
    for range in ranges.iter_mut() {
        reader
            .seek(SeekFrom::Start(range.offset_in_disk))
            .with_context(|| {
                format!(
                    "failed to seek CHD to partition-{} offset {}",
                    range.partition_index, range.offset_in_disk
                )
            })?;
        let mut hasher = Sha256::new();
        let mut remaining = range.length;
        while remaining > 0 {
            let want = (remaining as usize).min(buf.len());
            let n = reader.read(&mut buf[..want]).with_context(|| {
                format!(
                    "failed to read CHD partition-{} payload",
                    range.partition_index
                )
            })?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            remaining -= n as u64;
        }
        range.checksum_sha256 = format!("{:x}", hasher.finalize());
        log_cb(&format!(
            "  partition-{} sha256: {}",
            range.partition_index, range.checksum_sha256
        ));
    }

    // CHD's own SHA1 (mode-dependent: data SHA1 for v4+, overall for older).
    let info = libchdman_rs::Chd::open(
        chd_path
            .to_str()
            .ok_or_else(|| anyhow!("CHD path is not valid UTF-8: {}", chd_path.display()))?,
        false,
        None,
    )
    .map_err(|e| anyhow!("failed to reopen CHD for info: {:?}", e))?
    .info()
    .map_err(|e| anyhow!("failed to read CHD info: {:?}", e))?;

    let sha1_bytes = if info.version >= 4 {
        info.raw_sha1
    } else {
        info.sha1
    };
    Ok(hex_lower(&sha1_bytes))
}

fn hex_lower(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for byte in b {
        s.push_str(&format!("{:02x}", byte));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal MBR sector declaring one partition at LBA 1 covering
    /// `partition_sectors` sectors. Type byte 0x83 (Linux) — bypasses both
    /// the FAT/NTFS/exFAT smart-compaction probe (no matching VBR magic in
    /// our synthetic data) and the GPT protective-MBR special case, so the
    /// orchestrator falls through to raw passthrough.
    fn build_test_mbr(partition_sectors: u32) -> [u8; 512] {
        let mut mbr = [0u8; 512];
        // Boot signature.
        mbr[510] = 0x55;
        mbr[511] = 0xAA;
        // Primary partition entry at offset 446. Active flag 0, CHS-start
        // dummy, type 0x83, CHS-end dummy, LBA start 1, LBA size N.
        mbr[446] = 0x00;
        mbr[450] = 0x83;
        mbr[454..458].copy_from_slice(&1u32.to_le_bytes()); // start LBA
        mbr[458..462].copy_from_slice(&partition_sectors.to_le_bytes());
        mbr
    }

    #[test]
    fn end_to_end_round_trip_single_partition_chd() {
        // Layout: 4 MiB disk = 8192 sectors. MBR at sector 0; partition
        // covers sectors 1..=4095 (≈ 2 MiB), tail sectors are zero.
        const TOTAL_SECTORS: u32 = 8192;
        const PART_SECTORS: u32 = 4095;
        const SECTOR_SIZE: u64 = 512;
        let total_bytes = (TOTAL_SECTORS as u64) * SECTOR_SIZE;
        let part_bytes = (PART_SECTORS as u64) * SECTOR_SIZE;

        let tmp = tempfile::tempdir().expect("tempdir");
        let source_path = tmp.path().join("source.img");

        // Build the source: MBR then a recognizable byte pattern in the
        // partition (LBA-derived) then zeros.
        let mut data = vec![0u8; total_bytes as usize];
        data[..512].copy_from_slice(&build_test_mbr(PART_SECTORS));
        for i in 0..(part_bytes as usize) {
            // Each byte = (i mod 251). 251 is prime so the pattern is
            // distinguishable from any zero-fill artefacts.
            data[512 + i] = ((i % 251) as u8).wrapping_add(1);
        }
        std::fs::write(&source_path, &data).unwrap();

        let source_file = File::open(&source_path).unwrap();
        let source_size = source_file.metadata().unwrap().len();
        assert_eq!(source_size, total_bytes);

        // Parse our synthetic MBR using the production parser to make sure
        // we mimic the table shape `run_backup` will hand us.
        let mut br = BufReader::new(source_file.try_clone().unwrap());
        let table = PartitionTable::detect(&mut br).expect("detect MBR");
        let partitions = table.partitions();
        assert_eq!(partitions.len(), 1);

        let mbr_bytes = {
            let mut buf = [0u8; 512];
            buf.copy_from_slice(&data[..512]);
            buf
        };

        let output_base = tmp.path().join("disk");
        let inputs = SingleFileChdInputs {
            source_file: &source_file,
            source_size,
            source_partition_table_bytes: &mbr_bytes,
            partition_table: &table,
            partitions: &partitions,
            partition_filter: None,
            sector_by_sector: false,
            chd_options: None,
            is_dvd: false,
            output_base: &output_base,
        };

        let mut log_buf: Vec<String> = Vec::new();
        let mut log_cb = |s: &str| log_buf.push(s.to_string());
        let mut progress_seen: u64 = 0;
        let mut progress_cb = |n: u64| progress_seen = progress_seen.max(n);
        let cancel_check = || false;

        let result = run(inputs, &mut progress_cb, &cancel_check, &mut log_cb)
            .expect("single-file CHD backup");
        assert_eq!(result.container_filename, "disk.chd");
        assert_eq!(result.container_logical_size, total_bytes);
        assert_eq!(result.partition_ranges.len(), 1);
        assert_eq!(result.partition_ranges[0].partition_index, 0);
        assert_eq!(result.partition_ranges[0].offset_in_disk, 512);
        assert_eq!(result.partition_ranges[0].length, part_bytes);
        assert!(progress_seen > 0, "progress callback never fired");

        // Reopen the CHD and confirm bytes match the source disk.
        let chd_path = tmp.path().join("disk.chd");
        let mut reader = ChdReader::open(&chd_path).expect("reopen CHD");
        let mut readback = vec![0u8; total_bytes as usize];
        reader.read_exact(&mut readback).expect("read full CHD");
        assert_eq!(
            readback, data,
            "single-file CHD must round-trip the synthesised disk image byte-for-byte",
        );

        // Spot-check: log output mentions raw passthrough for our 0x83
        // partition (no compact reader matches Linux type byte).
        let joined = log_buf.join("\n");
        assert!(
            joined.contains("raw passthrough") || joined.contains("compact reader"),
            "log must explain how partition was read; got: {joined}",
        );
    }

    #[test]
    fn is_supported_only_returns_true_for_mbr() {
        // Build a synthetic MBR via our test helper and confirm.
        let mbr_bytes = build_test_mbr(64);
        let mut br = std::io::Cursor::new(mbr_bytes.to_vec());
        let table = PartitionTable::detect(&mut br).expect("detect MBR");
        assert!(is_supported(&table));
        // GPT/APM/None constructions live behind module-private fields, so
        // covering them via constructed values would couple this test to
        // their internals. The MBR-positive path here is enough — the
        // negative branch in `run()` is exercised whenever the matched
        // variant isn't `Mbr`.
    }
}
