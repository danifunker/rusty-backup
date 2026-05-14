//! Single-file CHD backup orchestrator.
//!
//! Synthesises a whole-disk image from the source's partition table + each
//! partition's data, streams it into a single CHD, and produces the
//! `metadata.json` entries the rest of the backup pipeline expects.
//!
//! Scope of the initial implementation (intentionally bounded):
//!
//! - **MBR / GPT / APM sources supported.** GPT rebuilds the primary header
//!   at LBA 1 and the backup header at the last 33 LBAs from the parsed
//!   `Gpt` so any tweaks the parser normalises (e.g. CRCs) are reflected
//!   in the synthesised image. APM reads the disk's head region (DDR +
//!   partition map + drivers) verbatim from the source up to the first
//!   real partition.
//! - **Backup-time resize supported.** When the caller passes
//!   `resize_targets`, partitions get their new sizes via the same
//!   `PartitionResizePlan` + `PartitionSizeOverride` machinery the restore
//!   path uses: assemble the synthesised disk into a sparse temp file,
//!   write the patched partition table at sector 0, drop each partition
//!   body at its new offset, run `resize_*_in_place` + hidden-sector
//!   patching, then hand the temp file to `compress_chd`. With no
//!   resize requested (or a no-op plan) the cheaper `DiskImageStream`
//!   path is used.
//! - **Smart compaction = layout-preserving compact reader where available**
//!   (HFS, HFS+, ext, btrfs, ProDOS), raw passthrough otherwise. Adding
//!   layout-preserving FAT/NTFS/exFAT readers is a future stage.
//! - **Sector-by-sector mode** = raw passthrough for every partition.
//!
//! The output is a real disk image: `chdman info` + MAME load it
//! correctly. Per-partition checksums are computed by re-reading the CHD
//! after compression so the metadata.json stays trustable for restore-time
//! validation.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use anyhow::{anyhow, Context, Result};

use super::disk_image_stream::DiskImageStreamBuilder;
use super::metadata::{BackupLayout, BackupMetadata};
use crate::fs;
use crate::partition::mbr::patch_mbr_entries;
use crate::partition::resize::{compute_resize_plan, PartitionResizePlan};
use crate::partition::{PartitionInfo, PartitionSizeOverride, PartitionTable};
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
    /// Optional per-partition target sizes `(index, new_size_bytes)`.
    /// `None` (or an empty slice / all-equal-to-source list) keeps source
    /// sizes and uses the streaming `DiskImageStream` path. Anything else
    /// triggers the temp-file resize path documented in
    /// `docs/whole_disk_chd_backup.md` (Stage 4b, Approach A).
    pub resize_targets: Option<&'a [(usize, u64)]>,
    /// HFS+/HFSX partitions that should be reproduced as a fully repacked
    /// (defragmented) volume via [`fs::hfsplus_defrag::stream_defragmented_hfsplus`]
    /// rather than via raw read + in-place resize. Entries are
    /// `(partition_index, target_size_bytes)`. The new partition size in the
    /// resize plan is taken from `resize_targets`; this list just flags which
    /// entries should be served by the clone pipeline. Set by `run_backup`
    /// when `BackupConfig::shrink_to_minimum` is on. `None` (or empty) keeps
    /// the legacy raw-read + in-place resize behaviour for every partition.
    pub hfsplus_clone_targets: Option<&'a [(usize, u64, crate::fs::DefragCloneShape)]>,
    /// Sectors used to align partition starts when relocating after a
    /// resize. Must match the source disk's detected alignment so that
    /// vintage OSes still accept the layout. Ignored on the no-resize
    /// streaming path.
    pub alignment_sectors: u64,
    /// Checksum algorithm for per-partition byte-range hashes recorded
    /// in metadata.json. Defaults to SHA-256; the caller (`run_backup`)
    /// passes `config.checksum` so backups stay consistent regardless
    /// of layout.
    pub checksum_type: super::ChecksumType,
}

/// Per-partition byte range + checksum, returned to the caller for
/// metadata.json population.
#[derive(Debug, Clone)]
pub struct ChdPartitionRange {
    pub partition_index: usize,
    pub offset_in_disk: u64,
    pub length: u64,
    pub checksum: String,
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

/// Estimate of the new on-disk usage that a re-export will incur. All
/// values are upper bounds; sparse temp files mean actual scratch usage
/// is typically lower, and CHD compression of zero-grow regions makes the
/// output smaller in practice. Use [`required_total`](Self::required_total)
/// for the free-space check.
#[derive(Debug, Clone, Copy)]
pub struct ExportDiskEstimate {
    /// Bytes the scratch temp file will consume at peak. Only set when a
    /// resize plan is in play; zero otherwise (the no-resize path streams
    /// directly into the encoder with no scratch file).
    pub scratch_bytes: u64,
    /// Bytes the output `.chd` is expected to occupy. Estimated as the
    /// existing source `.chd` file size when re-exporting from a CHD
    /// (compressed grow regions add little), or as the source data size
    /// when re-exporting from a raw image.
    pub output_bytes: u64,
}

impl ExportDiskEstimate {
    /// Total *new* on-disk usage during the export — what the destination
    /// filesystem needs free.
    pub fn required_total(&self) -> u64 {
        self.scratch_bytes.saturating_add(self.output_bytes)
    }
}

/// Estimate the disk usage a re-export will incur.
///
/// - `source_logical_size` — total logical bytes the source disk image
///   covers (== source data size for raw images; == container_logical_size
///   for CHD sources).
/// - `source_chd_file_size` — `Some(bytes)` when re-exporting from a CHD
///   (used as the output upper bound). `None` for raw-image sources, in
///   which case `source_logical_size` is used as the output upper bound.
/// - `partitions` — the parsed source partitions (post-resize sizes are
///   *not* applied here; this estimator works at the disk-image level
///   for the scratch upper bound).
/// - `has_resize` — true if any partition is being resized or moved. The
///   no-resize path uses no scratch file at all.
///
/// The scratch upper bound is the sum of partition `size_bytes` (which
/// covers the actually-written regions; sparse grow regions cost ~0)
/// plus a small head/tail allowance.
pub fn estimate_export_disk_usage(
    source_logical_size: u64,
    source_chd_file_size: Option<u64>,
    partitions: &[PartitionInfo],
    has_resize: bool,
) -> ExportDiskEstimate {
    // 1 MiB head/tail allowance covers MBR (1 sector), GPT (33 sectors at
    // each end), or APM (driver region up to a few hundred KiB). Cheap to
    // be generous here.
    const HEAD_TAIL_ALLOWANCE: u64 = 1024 * 1024;
    let scratch_bytes = if has_resize {
        partitions
            .iter()
            .filter(|p| !p.is_extended_container && p.partition_type_byte != 0xEE)
            .map(|p| p.size_bytes)
            .fold(0u64, u64::saturating_add)
            .saturating_add(HEAD_TAIL_ALLOWANCE)
    } else {
        0
    };
    let output_bytes = source_chd_file_size.unwrap_or(source_logical_size);
    ExportDiskEstimate {
        scratch_bytes,
        output_bytes,
    }
}

/// True if `inputs` describes a source layout we can currently handle in
/// single-file CHD mode. MBR / GPT / APM are supported; superfloppy is
/// rejected at the GUI layer (CHD output requires a partition table to
/// embed at sector 0).
pub fn is_supported(inputs_table: &PartitionTable) -> bool {
    matches!(
        inputs_table,
        PartitionTable::Mbr(_) | PartitionTable::Gpt { .. } | PartitionTable::Apm(_)
    )
}

/// Run the single-file CHD backup. Caller-supplied callbacks follow the
/// progress pattern documented in `docs/progress_pattern.md`: `progress_cb`
/// is fired with cumulative logical bytes written, `cancel_check` is polled
/// at chunk boundaries by `compress_chd`, and `log_cb` carries user-facing
/// log lines into the shared `LogPanel`.
///
/// `checksum_phase_cb` is invoked once at the start of the post-write
/// checksum phase with `(0, total_bytes)` and again periodically with
/// `(current, total_bytes)` so the caller can swap the GUI's progress bar
/// over from compression to hashing without bookkeeping. The default
/// no-op closure (`|_, _| {}`) is fine for callers that don't surface
/// per-phase progress (e.g. the inspect-tab export popup).
pub fn run(
    inputs: SingleFileChdInputs<'_>,
    progress_cb: &mut dyn FnMut(u64),
    cancel_check: &dyn Fn() -> bool,
    log_cb: &mut dyn FnMut(&str),
    checksum_phase_cb: &mut dyn FnMut(u64, u64),
) -> Result<SingleFileChdResult> {
    if !is_supported(inputs.partition_table) {
        anyhow::bail!(
            "single-file CHD backup requires a partition table; \
             {} sources are not supported. Pick zstd, raw, or VHD instead.",
            inputs.partition_table.type_name(),
        );
    }

    // Sector-by-sector + backup-time resize is rejected: sector-by-sector's
    // whole point is preserving the source byte-for-byte (free space +
    // unrecognized filesystem regions included), and resizing during
    // backup would defeat that. Users who want different sizes should
    // re-export the resulting CHD via the Inspect tab.
    if inputs.sector_by_sector {
        if let Some(targets) = inputs.resize_targets {
            let any_change = targets.iter().any(|(idx, new_size)| {
                inputs
                    .partitions
                    .iter()
                    .find(|p| p.index == *idx)
                    .map(|p| p.size_bytes != *new_size)
                    .unwrap_or(false)
            });
            if any_change {
                anyhow::bail!(
                    "sector-by-sector backups cannot be resized at backup time — \
                     they capture the source byte-for-byte. Re-export the resulting \
                     CHD via Inspect if you need to change partition sizes."
                );
            }
        }
    }

    // If the caller passed a non-trivial resize plan, fork off to the
    // sparse-temp-file pipeline. The streaming path below handles the
    // common "Original sizes" case at zero scratch-disk cost.
    if let Some(plans) = build_resize_plans(&inputs)? {
        return run_with_resize(
            inputs,
            &plans,
            progress_cb,
            cancel_check,
            log_cb,
            checksum_phase_cb,
        );
    }

    let mut builder = DiskImageStreamBuilder::new(inputs.source_size);
    let mut partition_ranges: Vec<ChdPartitionRange> = Vec::new();

    // Head segment: bytes that live before the first partition. Differs by
    // table type — see `build_head_segments`. Putting these at offset 0 is
    // what makes the resulting CHD a real disk image rather than an opaque
    // container.
    let (head_segments, backup_gpt_tail) = build_head_segments(
        inputs.source_file,
        inputs.source_partition_table_bytes,
        inputs.partition_table,
        inputs.partitions,
        inputs.source_size,
        log_cb,
    )?;
    for (offset, length, reader) in head_segments {
        builder
            .add_segment(offset, length, reader)
            .map_err(|e| anyhow!("disk-image stream rejected head segment at {offset}: {e}"))?;
    }

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
            checksum: String::new(), // filled in after CHD write
        });
    }

    // GPT backup header lives at the last 33 LBAs of the synthesised disk.
    // Add it as the final segment so it's emitted after every partition.
    if let Some((offset, length, reader)) = backup_gpt_tail {
        builder
            .add_segment(offset, length, reader)
            .map_err(|e| anyhow!("disk-image stream rejected backup-GPT tail segment: {}", e))?;
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
    let checksum_total: u64 = partition_ranges.iter().map(|r| r.length).sum();
    checksum_phase_cb(0, checksum_total);
    let container_sha1 = compute_post_write_metadata(
        &chd_path,
        &mut partition_ranges,
        inputs.checksum_type,
        &mut |cur| checksum_phase_cb(cur, checksum_total),
        log_cb,
    )?;

    Ok(SingleFileChdResult {
        container_filename,
        container_logical_size: logical_size,
        container_sha1,
        partition_ranges,
    })
}

/// Inputs for re-exporting an existing disk image (raw or `.chd`) to a
/// `.chd` via the single-file pipeline. Differs from `SingleFileChdInputs`
/// in two ways:
///
/// - Source is a path (not a pre-opened `&File`): when the source is a
///   `.chd` and a resize plan is requested, [`run_export`] decompresses
///   it to a temp file first because the resize pipeline needs raw
///   `File` semantics. Raw-image sources are opened directly.
/// - The output path is the user's literal destination filename
///   (e.g. `~/Desktop/MyDisk.chd`), not a `<folder>/<stem>` base —
///   `run_export` derives the `compress_chd` base by stripping the
///   trailing extension.
///
/// Used by the inspect-tab "Export Disk Image" popup; not by `run_backup`.
pub struct SingleFileChdExportInputs<'a> {
    /// Source disk image path (raw, VHD, 2MG, or `.chd`).
    pub source_path: &'a Path,
    /// Already-parsed partition table for the source.
    pub partition_table: &'a PartitionTable,
    /// Already-parsed partitions for the source.
    pub partitions: &'a [PartitionInfo],
    /// MBR sector bytes (only consulted when the table is MBR).
    pub source_partition_table_bytes: &'a [u8; 512],
    /// Source-disk alignment (typically copied from the inspect tab).
    pub alignment_sectors: u64,
    /// Final destination `.chd` path. Must end in `.chd`; the encoder
    /// writes `<dest_path with .chd stripped>.chd`.
    pub dest_path: &'a Path,
    /// CHD encoder options (codec / hunk size). `None` = profile defaults.
    pub chd_options: Option<ChdOptions>,
    /// `true` to emit a DVD-profile CHD (2048-byte unit + DVD metadata).
    pub is_dvd: bool,
    /// Per-partition resize targets `(partition_index, new_size_bytes)`.
    /// `None` (or empty / all no-op) routes to the fast streaming path
    /// with no scratch file.
    pub resize_targets: Option<&'a [(usize, u64)]>,
}

/// Re-export an opened disk image to a `.chd` file, optionally applying
/// per-partition resize. Mirrors what the inspect-tab "Export Disk Image"
/// popup needs: caller provides parsed partition info (from the open
/// inspect view) and an optional resize plan; the encoder picks the fast
/// streaming path when no resize is requested.
///
/// CHD-source + resize: the source is decompressed into a temp `.img`
/// next to the destination first, since the resize pipeline needs raw
/// `File` semantics for seek/read. The temp file is removed on return
/// (success or error) — see the RAII guard at the top of the function.
pub fn run_export(
    inputs: SingleFileChdExportInputs<'_>,
    progress_cb: &mut dyn FnMut(u64),
    cancel_check: &dyn Fn() -> bool,
    log_cb: &mut dyn FnMut(&str),
) -> Result<()> {
    if !is_supported(inputs.partition_table) {
        anyhow::bail!(
            "single-file CHD export does not support {} sources yet",
            inputs.partition_table.type_name(),
        );
    }

    // compress_chd writes "<base>.chd" — strip the extension from the
    // user's destination so the final filename matches what they asked
    // for. (e.g. ~/Desktop/MyDisk.chd → output_base = ~/Desktop/MyDisk.)
    let output_base = inputs.dest_path.with_extension("");

    let has_resize = inputs
        .resize_targets
        .map(|t| {
            t.iter().any(|(idx, new)| {
                inputs
                    .partitions
                    .iter()
                    .find(|p| p.index == *idx)
                    .map(|p| p.size_bytes != *new)
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false);

    // No-resize path: stream source -> compress_chd directly. Works for
    // raw images (`wrap_image_reader` returns a BufReader) and for `.chd`
    // sources (`wrap_image_reader` returns a ChdReader-backed stream).
    // No scratch file required.
    if !has_resize {
        log_cb("Re-exporting whole disk to CHD (no resize)...");
        let probe = File::open(inputs.source_path)
            .with_context(|| format!("failed to open {}", inputs.source_path.display()))?;
        let probe_path = inputs.source_path.to_path_buf();
        let fmt = crate::rbformats::detect_image_format_with_path(probe, Some(&probe_path))?;
        let file2 = File::open(inputs.source_path)?;
        let (mut reader, source_data_size) = crate::rbformats::wrap_image_reader(file2, fmt)?;

        let mut p_cb = |n: u64| progress_cb(n);
        let c_cb = || cancel_check();
        let mut l_cb = |s: &str| log_cb(s);
        let _written = if inputs.is_dvd {
            compress_chd_dvd(
                &mut reader,
                &output_base,
                source_data_size,
                None,
                inputs.chd_options.clone(),
                &mut p_cb,
                &c_cb,
                &mut l_cb,
            )?
        } else {
            compress_chd(
                &mut reader,
                &output_base,
                source_data_size,
                None,
                inputs.chd_options.clone(),
                &mut p_cb,
                &c_cb,
                &mut l_cb,
            )?
        };
        return Ok(());
    }

    // Resize path: build a real `&File` over the source bytes (decompress
    // CHD sources to a temp .img next to the destination first), then
    // hand off to the existing `run_with_resize` pipeline through `run`.
    log_cb("Re-exporting whole disk to CHD with resize plan...");
    let dest_dir = inputs.dest_path.parent().unwrap_or(Path::new("."));
    let temp_decompressed = if is_chd_extension(inputs.source_path) {
        let path = dest_dir.join(format!(".rusty-backup-export-{}.img", std::process::id(),));
        log_cb(&format!(
            "Decompressing source CHD to {}...",
            path.display()
        ));
        decompress_chd_to_file(inputs.source_path, &path, cancel_check)
            .with_context(|| format!("failed to decompress source CHD to {}", path.display()))?;
        Some(path)
    } else {
        None
    };
    // RAII guard so the temp .img is always cleaned up.
    struct TempFileGuard(Option<std::path::PathBuf>);
    impl Drop for TempFileGuard {
        fn drop(&mut self) {
            if let Some(p) = self.0.take() {
                let _ = std::fs::remove_file(&p);
            }
        }
    }
    let _guard = TempFileGuard(temp_decompressed.clone());

    let working_path = temp_decompressed.as_deref().unwrap_or(inputs.source_path);
    let source_file = File::open(working_path)
        .with_context(|| format!("failed to open {}", working_path.display()))?;
    let source_size = source_file.metadata()?.len();

    let run_inputs = SingleFileChdInputs {
        source_file: &source_file,
        source_size,
        source_partition_table_bytes: inputs.source_partition_table_bytes,
        partition_table: inputs.partition_table,
        partitions: inputs.partitions,
        partition_filter: None,
        sector_by_sector: false,
        chd_options: inputs.chd_options.clone(),
        is_dvd: inputs.is_dvd,
        output_base: &output_base,
        resize_targets: inputs.resize_targets,
        // Re-export path: no shrink-to-minimum clone — partitions are resized
        // in place by `resize_filesystem_for` as before.
        hfsplus_clone_targets: None,
        alignment_sectors: inputs.alignment_sectors,
        // The export path doesn't write metadata.json, so the only effect
        // of `checksum_type` here is the per-partition checksum log line
        // emitted during the post-write pass. SHA-256 is the historical
        // default — keep it.
        checksum_type: super::ChecksumType::Sha256,
    };

    // `run` returns post-write metadata; we don't need it here — the
    // caller writes no metadata.json on the export path.
    let _ = run(
        run_inputs,
        progress_cb,
        cancel_check,
        log_cb,
        &mut |_, _| {},
    )?;
    Ok(())
}

fn is_chd_extension(p: &Path) -> bool {
    matches!(
        p.extension().and_then(|s| s.to_str()).map(|s| s.to_ascii_lowercase()),
        Some(ref s) if s == "chd"
    )
}

/// Stream the entire logical contents of a CHD into a fresh raw `.img`
/// at `dest`. Used by the resize re-export path because the resize
/// pipeline needs raw `File` semantics on the source.
fn decompress_chd_to_file(
    chd_path: &Path,
    dest: &Path,
    cancel_check: &dyn Fn() -> bool,
) -> Result<()> {
    let mut reader = ChdReader::open(chd_path)?;
    let mut out = File::create(dest)?;
    let mut buf = vec![0u8; 1024 * 1024];
    loop {
        if cancel_check() {
            anyhow::bail!("operation cancelled");
        }
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        out.write_all(&buf[..n])?;
    }
    out.flush()?;
    Ok(())
}

/// Build a per-partition resize plan from `inputs.resize_targets`. Returns
/// `Ok(None)` when no resize is requested, or when every requested target
/// matches the source size (i.e. a no-op the streaming path can serve
/// faster). Returns `Ok(Some(plans))` when at least one partition would
/// move or change size.
pub fn build_resize_plans(
    inputs: &SingleFileChdInputs<'_>,
) -> Result<Option<Vec<PartitionResizePlan>>> {
    // Whenever any partition is flagged for HFS+ defrag-clone we MUST take
    // the temp-file branch — the streaming path can't run the clone pipeline
    // — so force a plan even when `resize_targets` is None / a no-op set.
    let has_clone_targets = inputs
        .hfsplus_clone_targets
        .map(|t| !t.is_empty())
        .unwrap_or(false);
    let targets = match inputs.resize_targets {
        Some(t) if !t.is_empty() => t,
        _ if has_clone_targets => &[][..],
        _ => return Ok(None),
    };

    // Filter out no-op entries up front: any target equal to its source
    // size adds no work, and we don't want to drag every partition into
    // the temp-file branch when the user accepted "Original" for all.
    let mut effective: Vec<(usize, u64)> = Vec::new();
    for (idx, new_size) in targets {
        if let Some(part) = inputs.partitions.iter().find(|p| p.index == *idx) {
            if part.size_bytes != *new_size {
                effective.push((*idx, *new_size));
            }
        }
    }
    if effective.is_empty() && !has_clone_targets {
        return Ok(None);
    }

    // Excluded partitions (skipped via filter) keep their original size in
    // the plan: their on-disk region is zero-filled by the temp-file path
    // but the partition table entry must still cover the same LBAs so
    // restore can find them later.
    let plans = compute_resize_plan(
        inputs.partitions,
        &effective,
        inputs.alignment_sectors,
        inputs.source_size,
    )
    .context("failed to compute single-file CHD resize plan")?;

    // If alignment + cumulative shifts collapsed everything back to the
    // original layout, drop to the streaming path.
    let any_change = plans
        .iter()
        .any(|p| p.needs_data_move || p.new_size_bytes != p.old_size_bytes);
    if !any_change && !has_clone_targets {
        return Ok(None);
    }
    Ok(Some(plans))
}

/// Convert a resize plan into the `PartitionSizeOverride` shape expected
/// by `patch_mbr_entries` / `Gpt::patch_for_restore` /
/// `Apm::patch_for_restore`. Heads/sectors-per-track stay at 0 because
/// CHS recalculation is the caller's concern when it matters; the
/// single-file-CHD path doesn't currently emit CHS-sensitive layouts.
fn plans_to_overrides(plans: &[PartitionResizePlan]) -> Vec<PartitionSizeOverride> {
    plans
        .iter()
        .map(|p| PartitionSizeOverride {
            index: p.index,
            start_lba: p.old_start_lba,
            original_size: p.old_size_bytes,
            export_size: p.new_size_bytes,
            new_start_lba: if p.needs_data_move {
                Some(p.new_start_lba)
            } else {
                None
            },
            heads: 0,
            sectors_per_track: 0,
        })
        .collect()
}

/// Compute the shrunken disk envelope for a resize-active backup. The
/// envelope is the byte just past the last partition's new extent, rounded
/// up to the disk's alignment sector (and with a GPT trailer reservation
/// when applicable), capped at the source size. Pure function — shared
/// between `run_with_resize` (sizes the scratch file + CHD container) and
/// `run_backup` (sets the GUI progress total before any bytes flow).
pub fn compute_resized_envelope(
    plans: &[PartitionResizePlan],
    alignment_sectors: u64,
    source_size: u64,
    partition_table: &PartitionTable,
) -> u64 {
    let alignment_bytes = (alignment_sectors.max(1) * 512).max(512);
    let last_partition_end = plans
        .iter()
        .map(|p| p.new_start_lba * 512 + p.new_size_bytes)
        .max()
        .unwrap_or(source_size);
    let trailer_reserve: u64 = match partition_table {
        PartitionTable::Gpt { .. } => 33 * 512,
        _ => 0,
    };
    let raw_target = last_partition_end.saturating_add(trailer_reserve);
    let aligned_target = raw_target.div_ceil(alignment_bytes) * alignment_bytes;
    aligned_target.min(source_size)
}

/// Thin `Write` wrapper that ticks a progress callback per write call.
/// Used by `run_with_resize` to surface mid-partition progress during the
/// HFS+ defrag-clone stream (otherwise the GUI sits at 0 B for the entire
/// multi-minute write phase on slow NAS targets). The callback receives
/// the cumulative byte count across the whole scratch file.
struct ProgressingWriter<'a, W: Write> {
    inner: W,
    cumulative: &'a mut u64,
    on_tick: &'a mut dyn FnMut(u64),
}

impl<'a, W: Write> Write for ProgressingWriter<'a, W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        *self.cumulative = self.cumulative.saturating_add(n as u64);
        (self.on_tick)(*self.cumulative);
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Sparse-temp-file resize pipeline. See module-level doc and
/// `docs/whole_disk_chd_backup.md` (Stage 4b, Approach A) for the
/// design rationale.
fn run_with_resize(
    inputs: SingleFileChdInputs<'_>,
    plans: &[PartitionResizePlan],
    progress_cb: &mut dyn FnMut(u64),
    cancel_check: &dyn Fn() -> bool,
    log_cb: &mut dyn FnMut(&str),
    checksum_phase_cb: &mut dyn FnMut(u64, u64),
) -> Result<SingleFileChdResult> {
    log_cb(&format!(
        "Single-file CHD: backup-time resize plan covers {} partition(s)",
        plans
            .iter()
            .filter(|p| p.new_size_bytes != p.old_size_bytes || p.needs_data_move)
            .count(),
    ));

    let overrides = plans_to_overrides(plans);
    let target_size = compute_resized_envelope(
        plans,
        inputs.alignment_sectors,
        inputs.source_size,
        inputs.partition_table,
    );
    log_cb(&format!(
        "  disk envelope: {} bytes (source {} bytes)",
        target_size, inputs.source_size,
    ));

    // Scratch disk image lives next to the eventual CHD so it shares a
    // filesystem with the destination — sparse semantics carry over and
    // we avoid cross-volume copies. `tempfile::NamedTempFile` cleans up
    // even on panic / early return.
    let scratch_dir = inputs
        .output_base
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    let scratch = tempfile::Builder::new()
        .prefix(".rb-chd-scratch-")
        .suffix(".img")
        .tempfile_in(&scratch_dir)
        .with_context(|| {
            format!(
                "failed to create scratch disk image in {}",
                scratch_dir.display()
            )
        })?;
    let scratch_path = scratch.path().to_path_buf();

    {
        // Open with read+write; we'll seek heavily during partition writes
        // and again during resize_*_in_place fixups.
        let mut tf = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&scratch_path)
            .context("failed to open scratch disk image read+write")?;
        tf.set_len(target_size)
            .context("failed to size scratch disk image")?;

        // Track bytes written into the scratch file so the GUI progress bar
        // advances during the partition-write phase (which dominates wall
        // time on slow NAS targets). `progress_cb` total is already
        // `target_size` (the shrunken envelope) — we feed it cumulative
        // bytes written. Coarse: tick once per partition with the
        // partition's new_size. The CHD compression pass at the end will
        // restart the bar (it calls progress_cb from 0..target_size again
        // with its own counter).
        let mut bytes_written_total: u64 = 0;

        write_patched_table(
            &mut tf,
            inputs.source_file,
            inputs.source_partition_table_bytes,
            inputs.partition_table,
            inputs.partitions,
            &overrides,
            target_size,
            log_cb,
        )?;

        for part in inputs.partitions {
            if cancel_check() {
                anyhow::bail!("operation cancelled");
            }
            if let Some(filter) = inputs.partition_filter {
                if !filter.contains(&part.index) {
                    continue;
                }
            }
            if part.partition_type_byte == 0xEE {
                continue;
            }
            if part.is_extended_container {
                log_cb(&format!(
                    "Skipping extended container partition-{} (logical partitions emitted individually)",
                    part.index,
                ));
                continue;
            }

            let plan = plans.iter().find(|p| p.index == part.index);
            let new_offset = plan
                .map(|p| p.new_start_lba * 512)
                .unwrap_or(part.start_lba * 512);
            let new_size = plan.map(|p| p.new_size_bytes).unwrap_or(part.size_bytes);
            let new_start_lba = new_offset / 512;

            // HFS+ defrag-clone path: stream a freshly-repacked HFS+ volume
            // directly into the scratch file at `new_offset`. The clone
            // pipeline produces a fully-consistent volume sized to the entry
            // recorded in the resize plan, so `resize_filesystem_for` must NOT
            // run afterwards — it would either no-op (already at target) or
            // corrupt the freshly-built MDB / volume header.
            let clone_entry = inputs
                .hfsplus_clone_targets
                .and_then(|t| t.iter().find(|(idx, _, _)| *idx == part.index).copied());

            if let Some((_, _, shape)) = clone_entry {
                let shape_label = match shape {
                    fs::DefragCloneShape::Flat => "flat HFS+",
                    fs::DefragCloneShape::Wrapped => "wrapped HFS+ (HFS shell preserved)",
                };
                log_cb(&format!(
                    "  partition-{}: streaming defragmented {} at offset {} \
                     (source size {}, new size {})",
                    part.index, shape_label, new_offset, part.size_bytes, new_size,
                ));
                tf.seek(SeekFrom::Start(new_offset))
                    .with_context(|| format!("seek scratch to partition-{} offset", part.index))?;
                let producer_clone = inputs
                    .source_file
                    .try_clone()
                    .context("clone source for HFS+ defrag-clone producer")?;
                let part_offset = part.start_lba * 512;
                // Wrap `tf` so each write call inside the clone stream ticks
                // `progress_cb` with the cumulative scratch-file bytes.
                // Without this the GUI bar stays at 0 for the full
                // partition write (tens of GiB on a slow NAS).
                let mut tick = |n: u64| progress_cb(n);
                let mut tf_progress = ProgressingWriter {
                    inner: &mut tf,
                    cumulative: &mut bytes_written_total,
                    on_tick: &mut tick,
                };
                let report = match shape {
                    fs::DefragCloneShape::Flat => {
                        let mut hfs = fs::hfsplus::HfsPlusFilesystem::open(
                            std::io::BufReader::new(producer_clone),
                            part_offset,
                        )
                        .with_context(|| {
                            format!("open source HFS+ for partition-{}", part.index)
                        })?;
                        fs::hfsplus_defrag::stream_defragmented_hfsplus(
                            &mut hfs,
                            new_size,
                            &mut tf_progress,
                        )
                        .with_context(|| {
                            format!("stream_defragmented_hfsplus for partition-{}", part.index)
                        })?
                    }
                    fs::DefragCloneShape::Wrapped => {
                        let mut br = std::io::BufReader::new(producer_clone);
                        let info = fs::hfsplus_wrapper_clone::detect_wrapped_hfsplus(
                            &mut br,
                            part_offset,
                            part.size_bytes,
                        )
                        .ok_or_else(|| {
                            anyhow!("wrapped HFS+ detection failed for partition-{}", part.index)
                        })?;
                        let outer_overhead = (info.al_block_start_sector as u64) * 512
                            + (info.embed_start_block as u64) * (info.al_block_size as u64)
                            + 1024;
                        let inner_target = new_size.saturating_sub(outer_overhead);
                        let plan =
                            fs::hfsplus_wrapper_clone::plan_wrapped_clone(&info, inner_target)
                                .map_err(|e| anyhow!("{e}"))?;
                        if plan.new_partition_size != new_size {
                            anyhow::bail!(
                                "wrapped HFS+ partition-{}: planned size {} disagrees with \
                                 resize plan size {}",
                                part.index,
                                plan.new_partition_size,
                                new_size,
                            );
                        }
                        fs::hfsplus_wrapper_clone::stream_wrapped_defragmented_hfsplus(
                            &mut br,
                            &plan,
                            &mut tf_progress,
                        )
                        .map_err(|e| anyhow!("{e}"))?
                    }
                };
                drop(tf_progress);
                log_cb(&format!(
                    "  partition-{}: defrag emit: {} files / {} folders / {} bytes / \
                     {} hardlinks / {} xattrs",
                    part.index,
                    report.files_copied,
                    report.dirs_copied,
                    report.data_bytes_copied,
                    report.hardlinks_copied + report.dir_hardlinks_copied,
                    report.xattrs_copied,
                ));
                // No resize_filesystem_for / patch_hidden_sectors_for here:
                // the cloned volume already matches `new_size` byte-for-byte,
                // and HFS+ has no hidden_sectors field to patch. The
                // ProgressingWriter ticks already advanced
                // bytes_written_total; emit one final tick to land on
                // exactly the partition's end byte (clone output may emit
                // slightly less than new_size if metadata was padded, in
                // which case round up so the bar reaches the expected
                // boundary).
                let target_total = (new_offset).saturating_add(new_size);
                if bytes_written_total < target_total {
                    bytes_written_total = target_total;
                    progress_cb(bytes_written_total);
                }
                // Flush the partition's writes to the scratch file before
                // moving to the next one. On a slow target (NAS / USB
                // bridge) this fsync can take many seconds during which
                // the progress bar would otherwise look stuck; surface a
                // log line so the user can tell why.
                log_cb(&format!(
                    "  partition-{}: flushing scratch writes to disk...",
                    part.index,
                ));
                tf.flush().with_context(|| {
                    format!("flush scratch after partition-{} clone", part.index)
                })?;
                log_cb(&format!("  partition-{}: flushed.", part.index,));
                continue;
            }

            // Bytes to write: cap at min(source_extent, new_size). When
            // shrinking, the FS resize call below is responsible for
            // ensuring no live data lived past `new_size`; the picker's
            // Min+20% computation guarantees this when used.
            let to_write = part.size_bytes.min(new_size);
            log_cb(&format!(
                "  partition-{}: writing {} bytes at offset {} (source size {}, new size {})",
                part.index, to_write, new_offset, part.size_bytes, new_size,
            ));

            tf.seek(SeekFrom::Start(new_offset))
                .with_context(|| format!("seek scratch to partition-{} offset", part.index))?;
            let mut reader =
                build_partition_reader(inputs.source_file, part, inputs.sector_by_sector, log_cb)?;
            copy_capped(&mut reader, &mut tf, to_write, cancel_check)
                .with_context(|| format!("write partition-{} body to scratch", part.index))?;

            // The grow case: ensure the temp file's logical extent
            // reaches new_offset + new_size before resize_*_in_place reads
            // back any tail bytes (resize functions seek to EOF-relative
            // positions like the HFS alt MDB).
            if to_write < new_size {
                tf.seek(SeekFrom::Start(new_offset + new_size - 1))?;
                tf.write_all(&[0u8])?;
            }

            // The fs::* helpers want a sized `impl FnMut(&str)`; rewrap
            // the trait-object log callback into a local closure for
            // each call.
            if new_size != part.size_bytes {
                let mut local_log = |s: &str| log_cb(s);
                fs::resize_filesystem_for(&mut tf, new_offset, new_size, &mut local_log)
                    .with_context(|| format!("resize filesystem in partition-{}", part.index))?;
            }
            // Always patch hidden_sectors when the partition moved (or in
            // case the source's BPB drifted from the table); the per-FS
            // patcher is a no-op on magic mismatch.
            let mut local_log = |s: &str| log_cb(s);
            fs::patch_hidden_sectors_for(&mut tf, new_offset, new_start_lba, &mut local_log)
                .with_context(|| format!("patch hidden sectors for partition-{}", part.index))?;

            bytes_written_total = bytes_written_total.saturating_add(new_size);
            progress_cb(bytes_written_total);
        }

        tf.flush().context("flush scratch disk image")?;
    }

    // Reopen as a plain `Read` for compress_chd. Forward-only; CHD
    // compression streams sequentially.
    let mut input = File::open(&scratch_path).context("reopen scratch disk image for compress")?;

    let mut chd_progress_cb = |n: u64| progress_cb(n);
    let mut chd_log_cb = |s: &str| log_cb(s);
    let chd_cancel_check = || cancel_check();

    let written_files = if inputs.is_dvd {
        compress_chd_dvd(
            &mut input,
            inputs.output_base,
            target_size,
            None,
            inputs.chd_options.clone(),
            &mut chd_progress_cb,
            &chd_cancel_check,
            &mut chd_log_cb,
        )?
    } else {
        compress_chd(
            &mut input,
            inputs.output_base,
            target_size,
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

    // Build the post-write partition_ranges list at the NEW offsets so
    // metadata.json reflects the resized layout.
    let mut partition_ranges: Vec<ChdPartitionRange> = inputs
        .partitions
        .iter()
        .filter(|p| {
            if p.partition_type_byte == 0xEE || p.is_extended_container {
                return false;
            }
            if let Some(filter) = inputs.partition_filter {
                if !filter.contains(&p.index) {
                    return false;
                }
            }
            true
        })
        .map(|part| {
            let plan = plans.iter().find(|p| p.index == part.index);
            let off = plan
                .map(|p| p.new_start_lba * 512)
                .unwrap_or(part.start_lba * 512);
            let len = plan.map(|p| p.new_size_bytes).unwrap_or(part.size_bytes);
            ChdPartitionRange {
                partition_index: part.index,
                offset_in_disk: off,
                length: len,
                checksum: String::new(),
            }
        })
        .collect();

    let chd_path = inputs
        .output_base
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(&container_filename);
    log_cb(&format!(
        "Computing per-partition checksums from {}",
        chd_path.display()
    ));
    let checksum_total: u64 = partition_ranges.iter().map(|r| r.length).sum();
    checksum_phase_cb(0, checksum_total);
    let container_sha1 = compute_post_write_metadata(
        &chd_path,
        &mut partition_ranges,
        inputs.checksum_type,
        &mut |cur| checksum_phase_cb(cur, checksum_total),
        log_cb,
    )?;

    Ok(SingleFileChdResult {
        container_filename,
        container_logical_size: target_size,
        container_sha1,
        partition_ranges,
    })
}

/// Write the patched partition table at sector 0 (and the backup GPT
/// at the disk tail when relevant). Mirrors the head-segment shapes
/// `build_head_segments` produces for the streaming path, but writes
/// directly to the scratch file so the partition loop can land bytes
/// at any offset afterwards.
#[allow(clippy::too_many_arguments)] // mirrors run_with_resize's locals
fn write_patched_table(
    scratch: &mut File,
    source_file: &File,
    source_partition_table_bytes: &[u8],
    table: &PartitionTable,
    partitions: &[PartitionInfo],
    overrides: &[PartitionSizeOverride],
    target_size: u64,
    log_cb: &mut dyn FnMut(&str),
) -> Result<()> {
    match table {
        PartitionTable::Mbr(_) => {
            let mut mbr_buf = [0u8; 512];
            let copy_len = source_partition_table_bytes.len().min(512);
            mbr_buf[..copy_len].copy_from_slice(&source_partition_table_bytes[..copy_len]);
            patch_mbr_entries(&mut mbr_buf, overrides);
            scratch.seek(SeekFrom::Start(0))?;
            scratch.write_all(&mbr_buf)?;
            log_cb("  table: MBR (patched with new partition sizes/offsets)");
        }
        PartitionTable::Gpt { gpt, .. } => {
            // Protective MBR is rebuilt deterministically; some sources
            // carry a hand-written variant. Either way we want a clean,
            // valid pMBR for the new disk size.
            let total_sectors = target_size / 512;
            let pmbr = crate::partition::gpt::Gpt::build_protective_mbr(total_sectors);
            scratch.seek(SeekFrom::Start(0))?;
            scratch.write_all(&pmbr)?;

            let patched = gpt.patch_for_restore(overrides, total_sectors);
            let primary = patched.build_primary_gpt(total_sectors);
            scratch.write_all(&primary)?;

            let backup = patched.build_backup_gpt(total_sectors);
            let backup_offset = target_size
                .checked_sub(backup.len() as u64)
                .ok_or_else(|| anyhow!("target size smaller than backup GPT region"))?;
            scratch.seek(SeekFrom::Start(backup_offset))?;
            scratch.write_all(&backup)?;
            log_cb(&format!(
                "  table: GPT (patched primary at LBA 1, backup at offset {backup_offset})"
            ));
        }
        PartitionTable::Apm(apm) => {
            // Drivers (Apple_Driver*) live as APM partitions before the
            // first user partition. Their data is real driver code that
            // we copy verbatim from the source; only the APM entry blocks
            // (DDR + map) get rewritten with patched sizes/offsets.
            let head_end = partitions
                .iter()
                .filter(|p| {
                    !matches!(
                        p.partition_type_string.as_deref(),
                        Some("Apple_partition_map")
                    )
                })
                .map(|p| p.start_lba * 512)
                .min()
                .unwrap_or(64 * 512)
                .min(target_size);
            if head_end == 0 {
                anyhow::bail!("APM head region computed as 0 bytes — partition table looks empty");
            }
            let mut head_buf = vec![0u8; head_end as usize];
            let mut clone = source_file
                .try_clone()
                .context("clone source for APM head")?;
            clone.seek(SeekFrom::Start(0))?;
            clone
                .read_exact(&mut head_buf)
                .context("read APM head region")?;
            scratch.seek(SeekFrom::Start(0))?;
            scratch.write_all(&head_buf)?;

            // Overlay patched DDR + entry blocks on top of the verbatim
            // head. Driver partition bodies (which sit AFTER the entry
            // blocks but inside `head_end`) are preserved by the
            // verbatim copy above.
            let block_size = apm.ddr.block_size as u64;
            let target_blocks = (target_size / block_size) as u32;
            let patched = apm.patch_for_restore(overrides, target_blocks);
            let blocks = patched.build_apm_blocks(Some(target_blocks));
            // build_apm_blocks returns DDR + entries only, sized at
            // `(1 + entry_count) * 512`. Cap at head_end just in case.
            let to_write = (blocks.len() as u64).min(head_end);
            scratch.seek(SeekFrom::Start(0))?;
            scratch.write_all(&blocks[..to_write as usize])?;
            log_cb(&format!(
                "  table: APM ({} bytes head copied verbatim, {} bytes of patched DDR+entries overlaid)",
                head_end,
                to_write,
            ));
        }
        PartitionTable::Sgi(_) => {
            anyhow::bail!(
                "single-file CHD: SGI Volume Header sources are not yet supported (browse only)"
            );
        }
        PartitionTable::None { .. } => {
            anyhow::bail!("single-file CHD: superfloppy sources are not supported on this path");
        }
    }
    Ok(())
}

/// `io::copy`-style helper that caps total bytes copied at `cap` and
/// polls `cancel_check` between chunks.
fn copy_capped(
    reader: &mut dyn Read,
    writer: &mut File,
    cap: u64,
    cancel_check: &dyn Fn() -> bool,
) -> Result<u64> {
    const CHUNK: usize = 1024 * 1024;
    let mut buf = vec![0u8; CHUNK];
    let mut remaining = cap;
    let mut written = 0u64;
    while remaining > 0 {
        if cancel_check() {
            anyhow::bail!("operation cancelled");
        }
        let want = (remaining as usize).min(CHUNK);
        let n = reader.read(&mut buf[..want])?;
        if n == 0 {
            break;
        }
        writer.write_all(&buf[..n])?;
        remaining -= n as u64;
        written += n as u64;
    }
    Ok(written)
}

/// Type alias for a stream segment: `(offset_in_disk, length, reader)`.
type Segment = (u64, u64, Box<dyn Read + Send>);

/// Build the segments that sit before the first partition (the "head"
/// region) and, for GPT only, the backup-GPT tail segment.
///
/// - **MBR**: head = the 512-byte MBR captured by the caller.
/// - **GPT**: head = protective MBR (verbatim) + freshly-built primary GPT
///   header + entries (33 sectors at LBAs 1..=33). Tail = backup GPT at
///   the last 33 LBAs.
/// - **APM**: head = bytes 0..first_partition_offset read verbatim from
///   the source. This covers the Driver Descriptor Record, the partition
///   map itself, any Apple_Driver* partitions, and the unused tail before
///   the first user partition.
fn build_head_segments(
    source_file: &File,
    source_partition_table_bytes: &[u8],
    table: &PartitionTable,
    partitions: &[PartitionInfo],
    source_size: u64,
    log_cb: &mut dyn FnMut(&str),
) -> Result<(Vec<Segment>, Option<Segment>)> {
    match table {
        PartitionTable::Mbr(_) => {
            let head: Segment = (
                0,
                source_partition_table_bytes.len() as u64,
                Box::new(std::io::Cursor::new(source_partition_table_bytes.to_vec())),
            );
            log_cb("  table: MBR sector embedded at offset 0");
            Ok((vec![head], None))
        }
        PartitionTable::Gpt { gpt, .. } => {
            // Source partition table bytes hold the protective MBR (at
            // LBA 0). Emit it verbatim, then rebuild the primary + backup
            // GPT from the parsed `Gpt`. Rebuilding (rather than copying
            // raw bytes) ensures CRCs are correct and that any header
            // fields the parser has normalised land in the output.
            let mut head: Vec<Segment> = Vec::new();
            head.push((
                0,
                source_partition_table_bytes.len() as u64,
                Box::new(std::io::Cursor::new(source_partition_table_bytes.to_vec())),
            ));

            let total_sectors = source_size / 512;
            let primary = gpt.build_primary_gpt(total_sectors);
            let primary_len = primary.len() as u64;
            head.push((512, primary_len, Box::new(std::io::Cursor::new(primary))));

            let backup_bytes = gpt.build_backup_gpt(total_sectors);
            let backup_len = backup_bytes.len() as u64;
            let backup_offset = source_size.checked_sub(backup_len).ok_or_else(|| {
                anyhow!("source size {} smaller than backup GPT region", source_size)
            })?;
            let tail: Segment = (
                backup_offset,
                backup_len,
                Box::new(std::io::Cursor::new(backup_bytes)),
            );

            log_cb(&format!(
                "  table: GPT — protective MBR + primary GPT (33 sectors) at offset 0; \
                 backup GPT at offset {backup_offset}",
            ));
            Ok((head, Some(tail)))
        }
        PartitionTable::Apm(_) => {
            // Read sectors 0..first_partition_offset verbatim. APM keeps
            // its DDR + partition map + driver partitions inside that
            // region, and reproducing them by hand would require parsing +
            // re-emitting every Apple_Driver* extension; copying bytes is
            // both simpler and faithful.
            let head_end = partitions
                .iter()
                .filter(|p| {
                    !matches!(
                        p.partition_type_string.as_deref(),
                        Some("Apple_partition_map")
                    )
                })
                .map(|p| p.start_lba * 512)
                .min()
                .unwrap_or_else(|| {
                    // No non-map partitions? Cover at least the first 64
                    // sectors so the partition map is still embedded.
                    64 * 512
                });
            let head_end = head_end.min(source_size);
            if head_end == 0 {
                anyhow::bail!("APM head region computed as 0 bytes — partition table looks empty");
            }
            // Read eagerly into a Vec rather than handing the stream a
            // `File` clone: on Unix, `try_clone` uses dup(), and dup'd fds
            // share the file offset. The partition loop builds its own
            // clones below and seeks them — those seeks would race with
            // the head reader's lazy reads. Eager-read sidesteps the
            // problem and keeps APM heads (~tens of KiB) cheap.
            let mut clone = source_file
                .try_clone()
                .context("clone source for APM head")?;
            clone.seek(SeekFrom::Start(0)).context("seek to APM head")?;
            let mut head_buf = vec![0u8; head_end as usize];
            clone
                .read_exact(&mut head_buf)
                .context("read APM head region")?;
            log_cb(&format!(
                "  table: APM — head region {} bytes (DDR + partition map + drivers) read verbatim",
                head_end
            ));
            Ok((
                vec![(0, head_end, Box::new(std::io::Cursor::new(head_buf)))],
                None,
            ))
        }
        PartitionTable::Sgi(_) => {
            anyhow::bail!(
                "single-file CHD: SGI Volume Header sources are not yet supported (browse only)"
            )
        }
        PartitionTable::None { .. } => {
            anyhow::bail!("single-file CHD: superfloppy sources are not supported on this path")
        }
    }
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
        // Smart mode: pack the FS body at the start of the partition
        // extent and zero-fill the trailing slack. For FAT/NTFS/exFAT
        // this defragments the volume in place (BPB shrinks to the new
        // cluster count, FAT tables track the packed clusters); for
        // HFS/HFS+/ext/btrfs/ProDOS the dispatcher returns the existing
        // layout-preserving stream unchanged. Either way, the returned
        // stream length equals `partition.size_bytes`.
        let clone = source_file
            .try_clone()
            .context("failed to clone source for compact reader")?;
        if let Some((reader, info)) = fs::packed_partition_reader_padded(
            clone,
            part_offset,
            part.partition_type_byte,
            part.partition_type_string.as_deref(),
        ) {
            // After padding, the stream must be exactly the partition
            // extent's length; an earlier regression let a packed
            // (un-padded) stream leak through and silently truncated the
            // disk image. Crash early instead.
            debug_assert_eq!(
                info.compacted_size, info.original_size,
                "single-file CHD requires streams sized to the partition extent",
            );
            if info.compacted_size == info.original_size {
                let packed_bytes = info.original_size.saturating_sub(
                    // For FAT/NTFS/exFAT the packed footprint is data_size
                    // (logical FS data); for layout-preserving readers
                    // data_size < compacted_size and the rest is verbatim
                    // free-region bytes. Either case: report the live
                    // footprint, with the slack tail counted as zero-fill.
                    0,
                );
                log_cb(&format!(
                    "  partition-{}: packed-and-padded reader (data {} of {} bytes, tail zero-filled)",
                    part.index, info.data_size, packed_bytes,
                ));
                return Ok(reader);
            }
            log_cb(&format!(
                "  partition-{}: dispatcher returned mis-sized stream; \
                 falling back to raw passthrough",
                part.index,
            ));
        } else {
            log_cb(&format!(
                "  partition-{}: no layout-preserving reader for type 0x{:02X}; \
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
    // partition length.
    //
    // A naive `clone.seek(part_offset)` + `BufReader<File>` is unsafe
    // here when more than one raw-passthrough segment is in play: on
    // Unix `try_clone()` uses dup(), and dup'd file descriptors share
    // the seek offset. The next call to `build_partition_reader` would
    // re-seek the shared fd to its own partition's offset, retroactively
    // moving the cursor of every previously-built reader. The
    // `DiskImageStream` would then read partition bodies from the wrong
    // disk locations.
    //
    // `PositionedSegmentReader` sidesteps this by tracking its own
    // logical offset and seeking + reading on every `read()`. The seek
    // is cheap on regular files; on raw devices it's amortised by the
    // 256 KiB+ chunks the CHD encoder pulls.
    let clone = source_file
        .try_clone()
        .context("failed to clone source for raw passthrough")?;
    Ok(Box::new(PositionedSegmentReader {
        file: clone,
        pos: part_offset,
        end: part_offset + part_length,
    }))
}

/// Forward-only reader over a fixed `[start, end)` byte range of a
/// `File`. Re-seeks before each read so that other clones of the same
/// underlying file descriptor (which share the kernel-level offset on
/// Unix) can't shift this reader's position out from under it.
struct PositionedSegmentReader {
    file: File,
    pos: u64,
    end: u64,
}

impl Read for PositionedSegmentReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.pos >= self.end {
            return Ok(0);
        }
        let want = ((self.end - self.pos) as usize).min(buf.len());
        self.file.seek(SeekFrom::Start(self.pos))?;
        let n = self.file.read(&mut buf[..want])?;
        self.pos += n as u64;
        Ok(n)
    }
}

/// Open the freshly-written CHD, compute the per-partition checksum for
/// each byte range using `checksum_type`, and return the CHD's own SHA1
/// (as `chdman info` would print).
///
/// `progress_cb` receives the *cumulative* number of bytes hashed across
/// all ranges so far — drop-in compatible with the cumulative-bytes
/// callback the rest of the backup pipeline uses.
fn compute_post_write_metadata(
    chd_path: &Path,
    ranges: &mut [ChdPartitionRange],
    checksum_type: super::ChecksumType,
    progress_cb: &mut dyn FnMut(u64),
    log_cb: &mut dyn FnMut(&str),
) -> Result<String> {
    let mut cumulative: u64 = 0;
    let mut delta_cb = |n: u64| {
        cumulative += n;
        progress_cb(cumulative);
    };
    let mut reader = ChdReader::open(chd_path).with_context(|| {
        format!(
            "failed to reopen newly-written CHD for checksum: {}",
            chd_path.display()
        )
    })?;

    // Per-partition checksum over the byte range. We re-seek for each
    // partition; the caller has already paid the cost of streaming the
    // entire image once during compression, so a second pass here is
    // acceptable for the simplicity it buys.
    let algo = checksum_type.as_str();
    for range in ranges.iter_mut() {
        reader
            .seek(SeekFrom::Start(range.offset_in_disk))
            .with_context(|| {
                format!(
                    "failed to seek CHD to partition-{} offset {}",
                    range.partition_index, range.offset_in_disk
                )
            })?;
        range.checksum = super::verify::hash_reader_range(
            &mut reader,
            range.length,
            checksum_type,
            &mut delta_cb,
        )
        .with_context(|| {
            format!(
                "failed to read CHD partition-{} payload",
                range.partition_index
            )
        })?;
        log_cb(&format!(
            "  partition-{} {}: {}",
            range.partition_index, algo, range.checksum
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

/// Refresh `metadata.json` after an in-place edit of a single-file-CHD
/// backup's container. Recomputes every partition's SHA-256 over its
/// recorded `[start_lba * 512, +imaged_size_bytes)` byte range and the
/// container's own SHA-1, then writes the updated metadata back.
///
/// Conservative by design: the chd_edit diff carries no easy-to-inspect
/// hunk map at this layer, so every partition is re-hashed regardless of
/// which one(s) the user touched. For typical backups (a handful of
/// partitions, single-digit GiB each) the second pass is fast.
///
/// Errors out (with the metadata untouched) if the layout isn't
/// `SingleFileChd`, the container is missing, or any byte range overflows
/// the container.
pub fn refresh_metadata_after_edit(
    backup_folder: &Path,
    log_cb: &mut dyn FnMut(&str),
) -> Result<()> {
    let metadata_path = backup_folder.join("metadata.json");
    let data = std::fs::read_to_string(&metadata_path)
        .with_context(|| format!("failed to read {}", metadata_path.display()))?;
    let mut metadata: BackupMetadata = serde_json::from_str(&data)
        .with_context(|| format!("failed to parse {}", metadata_path.display()))?;

    if !matches!(metadata.layout, BackupLayout::SingleFileChd) {
        anyhow::bail!(
            "refresh_metadata_after_edit called on non single-file-chd backup at {}",
            backup_folder.display(),
        );
    }

    let container_name = metadata
        .container
        .as_deref()
        .ok_or_else(|| anyhow!("metadata.container missing for single-file-chd backup"))?;
    let chd_path = backup_folder.join(container_name);

    let mut ranges: Vec<ChdPartitionRange> = metadata
        .partitions
        .iter()
        .map(|p| ChdPartitionRange {
            partition_index: p.index,
            offset_in_disk: p.start_lba.saturating_mul(512),
            length: p.imaged_size_bytes,
            checksum: String::new(),
        })
        .collect();

    log_cb(&format!(
        "Recomputing checksums for {} partition(s) in {}",
        ranges.len(),
        chd_path.display(),
    ));
    // Honor the checksum_type recorded in the backup's metadata; a backup
    // taken with CRC32 stays on CRC32 even after edits.
    let checksum_type = match metadata.checksum_type.as_str() {
        "crc32" => super::ChecksumType::Crc32,
        _ => super::ChecksumType::Sha256,
    };
    let new_sha1 =
        compute_post_write_metadata(&chd_path, &mut ranges, checksum_type, &mut |_| {}, log_cb)?;

    for range in &ranges {
        if let Some(part) = metadata
            .partitions
            .iter_mut()
            .find(|p| p.index == range.partition_index)
        {
            part.checksum = range.checksum.clone();
        }
    }
    metadata.container_sha1 = Some(new_sha1);

    let json = serde_json::to_string_pretty(&metadata)?;
    std::fs::write(&metadata_path, json)
        .with_context(|| format!("failed to write {}", metadata_path.display()))?;
    log_cb("metadata.json updated.");
    Ok(())
}

fn hex_lower(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for byte in b {
        s.push_str(&format!("{:02x}", byte));
    }
    s
}

/// Inputs for [`assemble_from_staging`]. Mirrors the shape of
/// [`SingleFileChdInputs`] but reads partition bodies from
/// per-partition zstd files in `staging_dir` instead of from a live
/// source disk. The patched partition table is synthesised in-memory
/// from `partition_table` + `plans`, so the source disk does not need
/// to be re-read here.
pub struct AssembleFromStagingInputs<'a> {
    /// Directory containing one `partition-N.zst` file per partition
    /// listed in `plans`. Each file decompresses to exactly
    /// `plan.new_size_bytes` bytes.
    pub staging_dir: &'a Path,
    /// Per-partition resize plan, one entry per partition to embed in
    /// the output CHD. Drives both the patched-table offsets and the
    /// segment placement.
    pub plans: &'a [PartitionResizePlan],
    /// MBR sector (or GPT pMBR sector) captured by the caller before
    /// the staging phase. Used to seed the patched-table emit step;
    /// for GPT the primary header + entries are rebuilt from
    /// `partition_table` so only the 512-byte pMBR portion is consulted.
    pub source_partition_table_bytes: &'a [u8],
    pub partition_table: &'a PartitionTable,
    pub partitions: &'a [PartitionInfo],
    /// Source disk size in bytes. Used as the upper bound for
    /// [`compute_resized_envelope`].
    pub source_size: u64,
    /// `<folder>/<stem>` base. `compress_chd` appends `.chd`.
    pub output_base: &'a Path,
    pub chd_options: Option<ChdOptions>,
    pub is_dvd: bool,
    pub alignment_sectors: u64,
    pub checksum_type: super::ChecksumType,
    /// APM only: bytes `[0, first_partition_offset)` from the source disk,
    /// covering the DDR + partition map + any Apple_Driver* partition
    /// bodies. The assembly path overlays patched DDR + entries on top
    /// of this region before emitting the head segment. MBR and GPT
    /// sources pass an empty slice — they rebuild the table from
    /// `source_partition_table_bytes` + `partition_table`.
    pub source_head_region: &'a [u8],
}

/// Assemble a single-file CHD by streaming per-partition zstd-compressed
/// bodies from a staging directory into the disk-image stream builder,
/// then through `compress_chd` / `compress_chd_dvd`. No scratch disk
/// image required: the patched partition table is synthesised in-memory
/// and each partition's bytes flow through a zstd decoder directly into
/// the CHD encoder under the back-pressure of the encoder's hunk writes.
///
/// **Status:** MBR, GPT, and APM sources supported. APM callers must
/// pre-populate `source_head_region` with the source's pre-first-partition
/// bytes (DDR + partition map + Apple_Driver* bodies); MBR / GPT
/// callers pass an empty slice.
pub fn assemble_from_staging(
    inputs: AssembleFromStagingInputs<'_>,
    progress_cb: &mut dyn FnMut(u64),
    cancel_check: &dyn Fn() -> bool,
    log_cb: &mut dyn FnMut(&str),
    checksum_phase_cb: &mut dyn FnMut(u64, u64),
) -> Result<SingleFileChdResult> {
    if !is_supported(inputs.partition_table) {
        anyhow::bail!(
            "assemble_from_staging: {} sources not supported",
            inputs.partition_table.type_name(),
        );
    }
    if matches!(inputs.partition_table, PartitionTable::Apm(_))
        && inputs.source_head_region.is_empty()
    {
        anyhow::bail!(
            "assemble_from_staging: APM source requires source_head_region — \
             caller did not populate it"
        );
    }
    if inputs.plans.is_empty() {
        anyhow::bail!("assemble_from_staging: empty resize plan");
    }

    let overrides = plans_to_overrides(inputs.plans);
    let target_size = compute_resized_envelope(
        inputs.plans,
        inputs.alignment_sectors,
        inputs.source_size,
        inputs.partition_table,
    );
    log_cb(&format!(
        "Assembling single-file CHD from staging dir {} (logical {} bytes, {} partition(s))",
        inputs.staging_dir.display(),
        target_size,
        inputs.plans.len(),
    ));

    let (head_segments, backup_tail) = build_patched_head_segments(
        inputs.source_partition_table_bytes,
        inputs.source_head_region,
        inputs.partition_table,
        inputs.partitions,
        &overrides,
        target_size,
        log_cb,
    )?;

    let mut builder = DiskImageStreamBuilder::new(target_size);
    let mut partition_ranges: Vec<ChdPartitionRange> = Vec::with_capacity(inputs.plans.len());

    for (offset, length, reader) in head_segments {
        builder
            .add_segment(offset, length, reader)
            .map_err(|e| anyhow!("disk-image stream rejected head segment at {offset}: {e}"))?;
    }

    for plan in inputs.plans {
        if cancel_check() {
            anyhow::bail!("operation cancelled");
        }
        let part = inputs
            .partitions
            .iter()
            .find(|p| p.index == plan.index)
            .ok_or_else(|| {
                anyhow!(
                    "assemble_from_staging: plan references unknown partition index {}",
                    plan.index,
                )
            })?;
        if part.is_extended_container || part.partition_type_byte == 0xEE {
            // Containers and protective-MBR entries don't have a body
            // file in staging; they live inside the patched table.
            continue;
        }
        let part_offset = plan.new_start_lba * 512;
        let part_length = plan.new_size_bytes;
        let staged = inputs
            .staging_dir
            .join(format!("partition-{}.zst", plan.index));
        let file = File::open(&staged).with_context(|| {
            format!(
                "failed to open staging file {} for partition-{}",
                staged.display(),
                plan.index
            )
        })?;
        // zstd::Decoder requires BufRead; wrap with BufReader for
        // efficient chunked reads. Cap to `part_length` so a
        // miscompressed staging file can't overrun its assigned
        // segment.
        let decoder = zstd::Decoder::new(file)
            .with_context(|| format!("failed to open zstd decoder on {}", staged.display(),))?;
        let reader: Box<dyn Read + Send> = Box::new(decoder.take(part_length));
        builder
            .add_segment(part_offset, part_length, reader)
            .map_err(|e| {
                anyhow!(
                    "disk-image stream rejected partition-{} segment at {part_offset}: {e}",
                    plan.index,
                )
            })?;
        partition_ranges.push(ChdPartitionRange {
            partition_index: plan.index,
            offset_in_disk: part_offset,
            length: part_length,
            checksum: String::new(),
        });
    }

    if let Some((offset, length, reader)) = backup_tail {
        builder
            .add_segment(offset, length, reader)
            .map_err(|e| anyhow!("disk-image stream rejected backup-GPT tail segment: {e}"))?;
    }

    let mut stream = builder.build();
    let logical_size = stream.total_length();

    let mut chd_progress_cb = |n: u64| progress_cb(n);
    let mut chd_log_cb = |s: &str| log_cb(s);
    let chd_cancel_check = || cancel_check();

    let written_files = if inputs.is_dvd {
        compress_chd_dvd(
            &mut stream,
            inputs.output_base,
            logical_size,
            None,
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

    let chd_path = inputs
        .output_base
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(&container_filename);
    log_cb(&format!(
        "Computing per-partition checksums from {}",
        chd_path.display()
    ));
    let checksum_total: u64 = partition_ranges.iter().map(|r| r.length).sum();
    checksum_phase_cb(0, checksum_total);
    let container_sha1 = compute_post_write_metadata(
        &chd_path,
        &mut partition_ranges,
        inputs.checksum_type,
        &mut |cur| checksum_phase_cb(cur, checksum_total),
        log_cb,
    )?;

    Ok(SingleFileChdResult {
        container_filename,
        container_logical_size: logical_size,
        container_sha1,
        partition_ranges,
    })
}

/// Read the APM head region — bytes `[0, first_user_partition_offset)`
/// — from the source disk into a `Vec<u8>`. Eager-read (vs handing the
/// assembler a `File` clone) sidesteps the dup-shared-offset hazard
/// the partition loop's clones would otherwise create.
fn read_apm_head_region(
    source_file: &File,
    partitions: &[PartitionInfo],
    source_size: u64,
) -> Result<Vec<u8>> {
    let head_end = partitions
        .iter()
        .filter(|p| {
            !matches!(
                p.partition_type_string.as_deref(),
                Some("Apple_partition_map")
            )
        })
        .map(|p| p.start_lba * 512)
        .min()
        .unwrap_or(64 * 512)
        .min(source_size);
    if head_end == 0 {
        anyhow::bail!("APM head region computed as 0 bytes — partition table looks empty");
    }
    let mut clone = source_file
        .try_clone()
        .context("clone source for APM head region")?;
    clone
        .seek(SeekFrom::Start(0))
        .context("seek to APM head region")?;
    let mut buf = vec![0u8; head_end as usize];
    clone.read_exact(&mut buf).context("read APM head region")?;
    Ok(buf)
}

/// `Read` adapter that produces exactly `target_len` bytes from `inner`,
/// truncating an over-long source or zero-padding a short one. Used by
/// the staging path to make each per-partition zstd file decompress to
/// exactly the partition's plan'd `new_size_bytes`, regardless of
/// whether the underlying compact reader emitted a slightly shorter
/// stream.
struct TakeOrPad<R: Read> {
    inner: R,
    remaining: u64,
    inner_done: bool,
}

impl<R: Read> TakeOrPad<R> {
    fn new(inner: R, target_len: u64) -> Self {
        Self {
            inner,
            remaining: target_len,
            inner_done: false,
        }
    }
}

impl<R: Read> Read for TakeOrPad<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.remaining == 0 {
            return Ok(0);
        }
        let cap = (self.remaining as usize).min(buf.len());
        if !self.inner_done {
            let n = self.inner.read(&mut buf[..cap])?;
            if n > 0 {
                self.remaining -= n as u64;
                return Ok(n);
            }
            self.inner_done = true;
        }
        // Inner finished — zero-pad to target.
        for b in buf[..cap].iter_mut() {
            *b = 0;
        }
        self.remaining -= cap as u64;
        Ok(cap)
    }
}

/// Pre-flight check: returns `None` when [`run_via_staging`] can handle
/// the given inputs, otherwise `Some(reason)` describing the gate that
/// rejected them. The caller (typically the CHD router in
/// `run_backup`) uses this to decide whether to take the new staging
/// path or fall back to [`run`] / [`run_with_resize`] during the
/// multi-commit migration.
///
/// Kept in sync with the bail-outs at the top of [`run_via_staging`] —
/// new staging-path gates land here too.
pub fn staging_path_unsupported_reason(inputs: &SingleFileChdInputs<'_>) -> Option<&'static str> {
    if !is_supported(inputs.partition_table) {
        return Some("partition table type not supported");
    }
    if inputs.is_dvd {
        // DVD-profile CHDs (optical media) are produced by the optical
        // tab's own pipeline. The single-file-CHD staging path is for
        // hard-disk backups only.
        return Some("DVD profile handled by the optical tab");
    }
    // FS-level resize check: synthesise the plan the same way
    // run_via_staging does, then look for any size or offset change
    // that ISN'T already covered by an HFS+ defrag-clone target.
    // Defrag-cloned partitions are sized to the plan's new_size by the
    // clone pipeline itself, so they don't count as "FS-level resize"
    // for routing purposes.
    let plans = match build_resize_plans(inputs) {
        Ok(Some(p)) => p,
        Ok(None) => synthesize_noop_plans(inputs.partitions),
        Err(_) => return Some("resize plan construction failed"),
    };
    let clone_idxs: std::collections::HashSet<usize> = inputs
        .hfsplus_clone_targets
        .map(|t| t.iter().map(|(i, _, _)| *i).collect())
        .unwrap_or_default();
    if plans.iter().any(|p| {
        let resized = p.new_size_bytes != p.old_size_bytes || p.needs_data_move;
        resized && !clone_idxs.contains(&p.index)
    }) {
        return Some("backup-time FS resize not yet supported on staging path");
    }
    None
}

/// Two-phase CHD backup: stage each partition into its own zstd file
/// under a temp staging dir, then call [`assemble_from_staging`] to fold
/// the staging files into a single `.chd`. Drop-in alternative to
/// [`run`] / [`run_with_resize`] for the same `SingleFileChdInputs`
/// shape.
///
/// **Status (commit 1b):** Handles raw / compact-reader partitions where
/// every requested `new_size_bytes` equals the source `size_bytes`
/// (no FS-level shrink). Partitions flagged for HFS+ defrag-clone or
/// any partition whose plan changes size return `Err(Unsupported)` for
/// now — those branches land in commits 1c (FS resize via temp .raw)
/// and 1d (HFS+ defrag-clone streamed into the staging file). APM
/// sources are rejected via [`assemble_from_staging`].
///
/// The staging dir lives next to `inputs.output_base` so it shares a
/// filesystem with the destination; sparse semantics carry over for
/// zstd's zero-block runs. Cleaned up on success and on every error
/// via the RAII guard.
pub fn run_via_staging(
    inputs: SingleFileChdInputs<'_>,
    progress_cb: &mut dyn FnMut(u64),
    cancel_check: &dyn Fn() -> bool,
    log_cb: &mut dyn FnMut(&str),
    checksum_phase_cb: &mut dyn FnMut(u64, u64),
    phase_cb: &mut dyn FnMut(&str),
    defrag_log_sink: Option<std::sync::Arc<dyn Fn(&str) + Send + Sync>>,
) -> Result<SingleFileChdResult> {
    if !is_supported(inputs.partition_table) {
        anyhow::bail!(
            "run_via_staging: {} sources not supported",
            inputs.partition_table.type_name(),
        );
    }
    let plans = match build_resize_plans(&inputs)? {
        Some(p) => p,
        None => synthesize_noop_plans(inputs.partitions),
    };

    // FS-level resize on non-cloned partitions still isn't supported on
    // the staging path. Clone targets DO resize via the clone pipeline,
    // so they're exempt from this gate.
    let clone_idxs: std::collections::HashSet<usize> = inputs
        .hfsplus_clone_targets
        .map(|t| t.iter().map(|(i, _, _)| *i).collect())
        .unwrap_or_default();
    let unsupported_resize = plans.iter().any(|p| {
        let resized = p.new_size_bytes != p.old_size_bytes || p.needs_data_move;
        resized && !clone_idxs.contains(&p.index)
    });
    if unsupported_resize {
        anyhow::bail!(
            "run_via_staging: backup-time FS resize is not yet supported on the \
             staging path for non-cloned partitions"
        );
    }

    // Staging dir lives next to the output. `tempfile::TempDir` removes
    // the directory on drop, so a bail-out mid-staging cleans up.
    let staging_parent = inputs
        .output_base
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    let staging = tempfile::Builder::new()
        .prefix(".rb-chd-staging-")
        .tempdir_in(&staging_parent)
        .with_context(|| {
            format!(
                "failed to create staging dir in {}",
                staging_parent.display()
            )
        })?;
    log_cb(&format!(
        "Staging per-partition zstd bodies in {}",
        staging.path().display()
    ));

    // For APM sources, capture the source head region eagerly before
    // any partition reads touch the source file. The head covers the
    // DDR + partition map + any Apple_Driver* partition bodies and
    // must reach the assembler intact so patched DDR + entries can be
    // overlaid on top of it.
    let source_head_region = if matches!(inputs.partition_table, PartitionTable::Apm(_)) {
        Some(read_apm_head_region(
            inputs.source_file,
            inputs.partitions,
            inputs.source_size,
        )?)
    } else {
        None
    };

    // Phase 1: stage each partition's body as zstd.
    stage_partitions_to_zst(
        &inputs,
        &plans,
        staging.path(),
        progress_cb,
        cancel_check,
        log_cb,
        phase_cb,
        defrag_log_sink.as_ref(),
    )?;

    // Phase 2: assemble the staged files into the final CHD.
    let head_region_slice: &[u8] = source_head_region.as_deref().unwrap_or(&[]);
    let assemble_inputs = AssembleFromStagingInputs {
        staging_dir: staging.path(),
        plans: &plans,
        source_partition_table_bytes: inputs.source_partition_table_bytes,
        partition_table: inputs.partition_table,
        partitions: inputs.partitions,
        source_size: inputs.source_size,
        output_base: inputs.output_base,
        chd_options: inputs.chd_options.clone(),
        is_dvd: inputs.is_dvd,
        alignment_sectors: inputs.alignment_sectors,
        checksum_type: inputs.checksum_type,
        source_head_region: head_region_slice,
    };
    phase_cb("Assembling CHD container from staged partitions");
    let result = assemble_from_staging(
        assemble_inputs,
        progress_cb,
        cancel_check,
        log_cb,
        checksum_phase_cb,
    )?;

    // staging.drop() removes the staging dir + all partition-N.zst files
    drop(staging);
    Ok(result)
}

/// Build a "no resize" plan that mirrors each partition's current
/// offset and size. Used by [`run_via_staging`] when the caller passes
/// no `resize_targets`.
fn synthesize_noop_plans(partitions: &[PartitionInfo]) -> Vec<PartitionResizePlan> {
    partitions
        .iter()
        .filter(|p| !p.is_extended_container && p.partition_type_byte != 0xEE)
        .map(|p| PartitionResizePlan {
            index: p.index,
            old_start_lba: p.start_lba,
            old_size_bytes: p.size_bytes,
            new_start_lba: p.start_lba,
            new_size_bytes: p.size_bytes,
            needs_data_move: false,
            move_delta_bytes: 0,
        })
        .collect()
}

/// Phase 1 of the staging path: write one `partition-N.zst` per
/// partition into `staging_dir`. Each file decompresses to exactly
/// `plan.new_size_bytes` bytes (truncated or zero-padded as needed)
/// so [`assemble_from_staging`] can wire them up as fixed-length
/// segments.
fn stage_partitions_to_zst(
    inputs: &SingleFileChdInputs<'_>,
    plans: &[PartitionResizePlan],
    staging_dir: &Path,
    progress_cb: &mut dyn FnMut(u64),
    cancel_check: &dyn Fn() -> bool,
    log_cb: &mut dyn FnMut(&str),
    phase_cb: &mut dyn FnMut(&str),
    defrag_log_sink: Option<&std::sync::Arc<dyn Fn(&str) + Send + Sync>>,
) -> Result<()> {
    let total_target: u64 = plans.iter().map(|p| p.new_size_bytes).sum();
    let mut bytes_done: u64 = 0;
    let staged_count = plans
        .iter()
        .filter(|p| {
            inputs
                .partitions
                .iter()
                .find(|x| x.index == p.index)
                .map(|x| !x.is_extended_container && x.partition_type_byte != 0xEE)
                .unwrap_or(false)
                && inputs
                    .partition_filter
                    .map(|f| f.contains(&p.index))
                    .unwrap_or(true)
        })
        .count();
    let mut stage_idx: usize = 0;

    for plan in plans {
        if cancel_check() {
            anyhow::bail!("operation cancelled");
        }
        let part = inputs
            .partitions
            .iter()
            .find(|p| p.index == plan.index)
            .ok_or_else(|| {
                anyhow!(
                    "stage_partitions: plan references unknown partition index {}",
                    plan.index,
                )
            })?;
        if part.is_extended_container || part.partition_type_byte == 0xEE {
            continue;
        }
        if let Some(filter) = inputs.partition_filter {
            if !filter.contains(&part.index) {
                continue;
            }
        }

        let target_len = plan.new_size_bytes;
        let stage_base = staging_dir.join(format!("partition-{}", plan.index));

        // HFS+ defrag-clone branch: drive the source's defragmenter on
        // a producer thread, pipe its bytes into compress_partition
        // (zstd). The clone pipeline is sized to `target_len`, so the
        // resulting staging file decompresses to exactly that length
        // and TakeOrPad is unnecessary. Otherwise: raw / compact reader
        // via build_partition_reader + TakeOrPad to guarantee length.
        let clone_entry = inputs
            .hfsplus_clone_targets
            .and_then(|t| t.iter().find(|(idx, _, _)| *idx == plan.index).copied());

        let base_for_progress = bytes_done;
        stage_idx += 1;
        if let Some((_, clone_target, shape)) = clone_entry {
            if clone_target != target_len {
                anyhow::bail!(
                    "stage_partitions: HFS+ clone target {} != plan new_size {} \
                     for partition-{}",
                    clone_target,
                    target_len,
                    plan.index,
                );
            }
            let shape_label = match shape {
                fs::DefragCloneShape::Flat => "flat HFS+",
                fs::DefragCloneShape::Wrapped => "wrapped HFS+",
            };
            phase_cb(&format!(
                "Staging partition-{} ({} of {}): analyzing {} catalog (may take minutes)...",
                plan.index, stage_idx, staged_count, shape_label,
            ));
            log_cb(&format!(
                "  staging partition-{} (HFS+ defrag-clone, {}) -> {}.zst ({} bytes)",
                plan.index,
                shape_label,
                stage_base.display(),
                target_len,
            ));

            let part_offset = part.start_lba * 512;
            let part_size = part.size_bytes;
            let plan_index: usize = plan.index;
            let producer_clone = inputs
                .source_file
                .try_clone()
                .context("clone source for HFS+ defrag-clone producer")?;
            let (mut writer, mut reader) = super::channel_pipe();

            // Install the same [defrag-build] progress sink the main
            // backup thread has — without it the catalog-walk ticks
            // only reach stderr and the GUI log goes silent for
            // minutes during build_target_metadata.
            let producer_sink: Option<std::sync::Arc<dyn Fn(&str) + Send + Sync>> =
                defrag_log_sink.cloned();
            let producer =
                std::thread::spawn(move || -> Result<fs::hfsplus_defrag::DefragReport> {
                    // Defensive: clear the thread-local on the way out
                    // regardless of how this thread exits, so a future
                    // OS-thread reuse can't inherit our sink.
                    struct ClearSinkOnDrop;
                    impl Drop for ClearSinkOnDrop {
                        fn drop(&mut self) {
                            fs::hfsplus_defrag::set_progress_sink(None);
                        }
                    }
                    let _sink_guard = ClearSinkOnDrop;
                    if let Some(sink) = producer_sink {
                        let sink_for_install = sink.clone();
                        fs::hfsplus_defrag::set_progress_sink(Some(Box::new(move |msg: &str| {
                            sink_for_install(msg)
                        })));
                    }
                    match shape {
                        fs::DefragCloneShape::Flat => {
                            let br = std::io::BufReader::new(producer_clone);
                            let mut hfs = fs::hfsplus::HfsPlusFilesystem::open(br, part_offset)
                                .context("open source HFS+ for defrag-clone")?;
                            fs::hfsplus_defrag::stream_defragmented_hfsplus(
                                &mut hfs,
                                target_len,
                                &mut writer,
                            )
                            .context("stream_defragmented_hfsplus")
                        }
                        fs::DefragCloneShape::Wrapped => {
                            let mut br = std::io::BufReader::new(producer_clone);
                            let info = fs::hfsplus_wrapper_clone::detect_wrapped_hfsplus(
                                &mut br,
                                part_offset,
                                part_size,
                            )
                            .ok_or_else(|| {
                                anyhow!(
                                    "wrapped HFS+ detection failed for partition-{}",
                                    plan_index,
                                )
                            })?;
                            let outer_overhead = (info.al_block_start_sector as u64) * 512
                                + (info.embed_start_block as u64) * (info.al_block_size as u64)
                                + 1024;
                            let inner_target = target_len.saturating_sub(outer_overhead);
                            let cplan =
                                fs::hfsplus_wrapper_clone::plan_wrapped_clone(&info, inner_target)
                                    .map_err(|e| anyhow!("{e}"))?;
                            if cplan.new_partition_size != target_len {
                                anyhow::bail!(
                                    "wrapped HFS+ partition-{}: planned size {} \
                                     disagrees with target {}",
                                    plan_index,
                                    cplan.new_partition_size,
                                    target_len,
                                );
                            }
                            fs::hfsplus_wrapper_clone::stream_wrapped_defragmented_hfsplus(
                                &mut br,
                                &cplan,
                                &mut writer,
                            )
                            .map_err(|e| anyhow!("{e}"))
                        }
                    }
                });

            // Once the producer starts emitting bytes, swap the phase
            // label so the user knows the catalog walk finished and
            // bytes are flowing.
            let mut emit_label_set = false;
            let compress_result = crate::rbformats::compress_partition(
                &mut reader,
                &stage_base,
                super::CompressionType::Zstd,
                target_len,
                None,
                false,
                None,
                |n_read| {
                    if !emit_label_set && n_read > 0 {
                        phase_cb(&format!(
                            "Staging partition-{} ({} of {}): writing defragmented HFS+ body...",
                            plan.index, stage_idx, staged_count,
                        ));
                        emit_label_set = true;
                    }
                    progress_cb(base_for_progress.saturating_add(n_read).min(total_target))
                },
                || cancel_check(),
                |msg| log_cb(msg),
            );

            // Always join the producer so its errors surface even when
            // the consumer succeeded (and vice-versa). Mirrors the
            // pattern in `run_backup`'s zstd path.
            let producer_result = producer.join().map_err(|_| {
                anyhow!(
                    "HFS+ defrag-clone producer thread panicked for partition-{}",
                    plan.index
                )
            })?;
            compress_result.with_context(|| {
                format!(
                    "failed to zstd-stage defrag-cloned partition-{}",
                    plan.index
                )
            })?;
            let report = producer_result.with_context(|| {
                format!("HFS+ defrag-clone failed for partition-{}", plan.index)
            })?;
            log_cb(&format!(
                "  partition-{} defrag emit: {} files / {} folders / {} bytes / \
                 {} hardlinks / {} xattrs",
                plan.index,
                report.files_copied,
                report.dirs_copied,
                report.data_bytes_copied,
                report.hardlinks_copied + report.dir_hardlinks_copied,
                report.xattrs_copied,
            ));
        } else {
            phase_cb(&format!(
                "Staging partition-{} ({} of {}): zstd-compressing partition body...",
                plan.index, stage_idx, staged_count,
            ));
            let body_reader =
                build_partition_reader(inputs.source_file, part, inputs.sector_by_sector, log_cb)?;
            let mut padded = TakeOrPad::new(body_reader, target_len);
            log_cb(&format!(
                "  staging partition-{} -> {}.zst ({} bytes)",
                plan.index,
                stage_base.display(),
                target_len,
            ));
            let _files = crate::rbformats::compress_partition(
                &mut padded,
                &stage_base,
                super::CompressionType::Zstd,
                target_len,
                None,
                false,
                None,
                |n_read| progress_cb(base_for_progress.saturating_add(n_read).min(total_target)),
                || cancel_check(),
                |msg| log_cb(msg),
            )
            .with_context(|| format!("failed to zstd-stage partition-{}", plan.index))?;
        }

        bytes_done = bytes_done.saturating_add(target_len);
        progress_cb(bytes_done.min(total_target));
    }
    Ok(())
}

/// In-memory analogue of [`write_patched_table`]: returns the synthesised
/// patched-table bytes (and, for GPT, the backup-GPT tail) as
/// `DiskImageStreamBuilder` segments. Used by [`assemble_from_staging`]
/// to avoid materialising a scratch disk image just to host the table.
fn build_patched_head_segments(
    source_partition_table_bytes: &[u8],
    source_head_region: &[u8],
    table: &PartitionTable,
    partitions: &[PartitionInfo],
    overrides: &[PartitionSizeOverride],
    target_size: u64,
    log_cb: &mut dyn FnMut(&str),
) -> Result<(Vec<Segment>, Option<Segment>)> {
    let _ = partitions; // used only by APM today; kept for symmetry.
    match table {
        PartitionTable::Mbr(_) => {
            let mut mbr_buf = [0u8; 512];
            let copy_len = source_partition_table_bytes.len().min(512);
            mbr_buf[..copy_len].copy_from_slice(&source_partition_table_bytes[..copy_len]);
            patch_mbr_entries(&mut mbr_buf, overrides);
            log_cb("  table: MBR (patched with new partition sizes/offsets)");
            let head: Segment = (
                0,
                mbr_buf.len() as u64,
                Box::new(std::io::Cursor::new(mbr_buf.to_vec())),
            );
            Ok((vec![head], None))
        }
        PartitionTable::Gpt { gpt, .. } => {
            let total_sectors = target_size / 512;
            let mut head_bytes: Vec<u8> = Vec::with_capacity(34 * 512);
            head_bytes.extend_from_slice(&crate::partition::gpt::Gpt::build_protective_mbr(
                total_sectors,
            ));
            let patched = gpt.patch_for_restore(overrides, total_sectors);
            head_bytes.extend(patched.build_primary_gpt(total_sectors));

            let backup = patched.build_backup_gpt(total_sectors);
            let backup_offset = target_size
                .checked_sub(backup.len() as u64)
                .ok_or_else(|| anyhow!("target size smaller than backup GPT region"))?;
            log_cb(&format!(
                "  table: GPT (patched primary at LBA 1, backup at offset {backup_offset})"
            ));
            let backup_len = backup.len() as u64;
            let head: Segment = (
                0,
                head_bytes.len() as u64,
                Box::new(std::io::Cursor::new(head_bytes)),
            );
            let tail: Segment = (
                backup_offset,
                backup_len,
                Box::new(std::io::Cursor::new(backup)),
            );
            Ok((vec![head], Some(tail)))
        }
        PartitionTable::Apm(apm) => {
            // Mirrors `write_patched_table`'s APM branch but produces a
            // single in-memory segment instead of scratch-file writes.
            // Take the source head bytes verbatim (DDR + map + drivers +
            // any verbatim driver bodies) and overlay patched DDR +
            // entry blocks on top.
            if source_head_region.is_empty() {
                anyhow::bail!("build_patched_head_segments: APM requires source_head_region");
            }
            let head_end = source_head_region.len() as u64;
            let mut head_buf = source_head_region.to_vec();

            let block_size = apm.ddr.block_size as u64;
            if block_size == 0 {
                anyhow::bail!("APM DDR block_size is 0 — corrupt partition table");
            }
            let target_blocks = (target_size / block_size) as u32;
            let patched = apm.patch_for_restore(overrides, target_blocks);
            let blocks = patched.build_apm_blocks(Some(target_blocks));
            let to_overlay = (blocks.len() as u64).min(head_end);
            head_buf[..to_overlay as usize].copy_from_slice(&blocks[..to_overlay as usize]);
            log_cb(&format!(
                "  table: APM ({} bytes head verbatim, {} bytes of patched DDR+entries overlaid)",
                head_end, to_overlay,
            ));
            let head: Segment = (0, head_end, Box::new(std::io::Cursor::new(head_buf)));
            Ok((vec![head], None))
        }
        PartitionTable::Sgi(_) => {
            anyhow::bail!(
                "assemble_from_staging: SGI Volume Header sources are not supported (browse only)"
            );
        }
        PartitionTable::None { .. } => {
            anyhow::bail!("assemble_from_staging: superfloppy sources are not supported");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::BufReader;

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
            resize_targets: None,
            hfsplus_clone_targets: None,
            alignment_sectors: 0,
            checksum_type: crate::backup::ChecksumType::Sha256,
        };

        let mut log_buf: Vec<String> = Vec::new();
        let mut log_cb = |s: &str| log_buf.push(s.to_string());
        let mut progress_seen: u64 = 0;
        let mut progress_cb = |n: u64| progress_seen = progress_seen.max(n);
        let cancel_check = || false;

        let result = run(
            inputs,
            &mut progress_cb,
            &cancel_check,
            &mut log_cb,
            &mut |_, _| {},
        )
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
    fn is_supported_accepts_mbr() {
        let mbr_bytes = build_test_mbr(64);
        let mut br = std::io::Cursor::new(mbr_bytes.to_vec());
        let table = PartitionTable::detect(&mut br).expect("detect MBR");
        assert!(is_supported(&table));
    }

    /// Build a tiny GPT-formatted disk and round-trip it through
    /// `single_file_chd::run`, then assert that the resulting CHD has both
    /// the protective MBR + primary GPT at the start and a backup GPT at
    /// the last 33 LBAs.
    #[test]
    fn end_to_end_round_trip_gpt() {
        use crate::partition::gpt::{build_minimal_gpt, Gpt, Guid};

        const TOTAL_SECTORS: u64 = 8192; // 4 MiB
        const SECTOR_SIZE: u64 = 512;
        let total_bytes = TOTAL_SECTORS * SECTOR_SIZE;

        // One Linux-data partition spanning LBAs 64..4095 (~2 MiB).
        let linux_data_guid = Guid::from_string("0FC63DAF-8483-4772-8E79-3D69D8477DE4").unwrap();
        let gpt = build_minimal_gpt(
            &[(linux_data_guid, 64, 4095, "linux".to_string())],
            total_bytes,
        );

        // Synthesise the on-disk image: protective MBR at 0, primary GPT
        // at LBA 1.., partition at LBA 64, backup GPT at last 33 LBAs.
        let mut data = vec![0u8; total_bytes as usize];
        let prot_mbr = Gpt::build_protective_mbr(TOTAL_SECTORS);
        data[..512].copy_from_slice(&prot_mbr);
        let primary = gpt.build_primary_gpt(TOTAL_SECTORS);
        data[512..512 + primary.len()].copy_from_slice(&primary);
        // Fill the partition body with a recognizable pattern.
        let part_offset = 64 * 512;
        let part_end = (4095 + 1) * 512;
        for i in 0..(part_end - part_offset) as usize {
            data[part_offset as usize + i] = ((i % 251) as u8).wrapping_add(1);
        }
        let backup = gpt.build_backup_gpt(TOTAL_SECTORS);
        let backup_offset = (total_bytes - backup.len() as u64) as usize;
        data[backup_offset..backup_offset + backup.len()].copy_from_slice(&backup);

        let tmp = tempfile::tempdir().unwrap();
        let source_path = tmp.path().join("source.img");
        std::fs::write(&source_path, &data).unwrap();
        let source_file = File::open(&source_path).unwrap();

        let mut br = BufReader::new(source_file.try_clone().unwrap());
        let table = PartitionTable::detect(&mut br).expect("detect GPT");
        assert!(matches!(table, PartitionTable::Gpt { .. }));
        let partitions = table.partitions();

        let output_base = tmp.path().join("disk");
        let mbr_bytes: [u8; 512] = data[..512].try_into().unwrap();
        let mut log_buf: Vec<String> = Vec::new();
        let mut log_cb = |s: &str| log_buf.push(s.to_string());
        let mut progress_cb = |_: u64| {};
        let cancel = || false;
        let result = run(
            SingleFileChdInputs {
                source_file: &source_file,
                source_size: total_bytes,
                source_partition_table_bytes: &mbr_bytes,
                partition_table: &table,
                partitions: &partitions,
                partition_filter: None,
                sector_by_sector: false,
                chd_options: None,
                is_dvd: false,
                output_base: &output_base,
                resize_targets: None,
                hfsplus_clone_targets: None,
                alignment_sectors: 0,
                checksum_type: crate::backup::ChecksumType::Sha256,
            },
            &mut progress_cb,
            &cancel,
            &mut log_cb,
            &mut |_, _| {},
        )
        .expect("GPT single-file CHD backup");
        assert_eq!(result.container_logical_size, total_bytes);

        // Read back via ChdReader and assert the resulting image parses as
        // GPT — that's the strongest assertion the synthesised image is
        // tool-compatible (CRCs, headers, backup header all valid).
        let chd_path = tmp.path().join("disk.chd");
        let chd_reader = ChdReader::open(&chd_path).unwrap();
        let mut br = BufReader::new(chd_reader);
        let detected = PartitionTable::detect(&mut br).expect("detect after round-trip");
        assert!(
            matches!(detected, PartitionTable::Gpt { .. }),
            "round-tripped CHD should still parse as GPT, got: {}",
            detected.type_name(),
        );
    }

    /// Tiny APM source (DDR + 2 partition map entries + an HFS-shaped data
    /// partition) — confirms the head region is copied verbatim and the
    /// partition data lands at its declared offset in the CHD.
    #[test]
    fn end_to_end_round_trip_apm() {
        const TOTAL_BLOCKS: u64 = 8192;
        const SECTOR: u64 = 512;
        let total_bytes = TOTAL_BLOCKS * SECTOR;

        let mut data = vec![0u8; total_bytes as usize];
        // DDR at block 0.
        const DDR_SIG: u16 = 0x4552; // 'ER'
        data[0..2].copy_from_slice(&DDR_SIG.to_be_bytes());
        data[2..4].copy_from_slice(&512u16.to_be_bytes());
        data[4..8].copy_from_slice(&(TOTAL_BLOCKS as u32).to_be_bytes());

        // Two APM entries at blocks 1 and 2.
        const ENTRY_SIG: u16 = 0x504D; // 'PM'
        let entries = [
            ("Apple", "Apple_partition_map", 1u32, 2u32),
            ("MacOS", "Apple_HFS", 64u32, 4096u32),
        ];
        for (i, (name, ptype, start, count)) in entries.iter().enumerate() {
            let off = (1 + i) * 512;
            data[off..off + 2].copy_from_slice(&ENTRY_SIG.to_be_bytes());
            data[off + 4..off + 8].copy_from_slice(&(entries.len() as u32).to_be_bytes());
            data[off + 8..off + 12].copy_from_slice(&start.to_be_bytes());
            data[off + 12..off + 16].copy_from_slice(&count.to_be_bytes());
            // Name: 32-byte C string at offset 16
            let nb = name.as_bytes();
            data[off + 16..off + 16 + nb.len()].copy_from_slice(nb);
            // Type: 32-byte C string at offset 48
            let tb = ptype.as_bytes();
            data[off + 48..off + 48 + tb.len()].copy_from_slice(tb);
            data[off + 84..off + 88].copy_from_slice(&count.to_be_bytes());
            data[off + 88..off + 92].copy_from_slice(&0x33u32.to_be_bytes());
        }

        // Pattern bytes in the data partition (block 64 .. 64+4096).
        let part_offset = 64usize * 512;
        let part_len = 4096usize * 512;
        for i in 0..part_len {
            data[part_offset + i] = ((i % 251) as u8).wrapping_add(2);
        }

        let tmp = tempfile::tempdir().unwrap();
        let source_path = tmp.path().join("source.img");
        std::fs::write(&source_path, &data).unwrap();
        let source_file = File::open(&source_path).unwrap();

        let mut br = BufReader::new(source_file.try_clone().unwrap());
        let table = PartitionTable::detect(&mut br).expect("detect APM");
        assert!(matches!(table, PartitionTable::Apm(_)));
        let partitions = table.partitions();

        let output_base = tmp.path().join("disk");
        let mbr_bytes: [u8; 512] = data[..512].try_into().unwrap();
        let mut log_buf: Vec<String> = Vec::new();
        let mut log_cb = |s: &str| log_buf.push(s.to_string());
        let mut progress_cb = |_: u64| {};
        let cancel = || false;
        let result = run(
            SingleFileChdInputs {
                source_file: &source_file,
                source_size: total_bytes,
                source_partition_table_bytes: &mbr_bytes,
                partition_table: &table,
                partitions: &partitions,
                partition_filter: None,
                sector_by_sector: true, // exercises raw passthrough for HFS-typed partition
                chd_options: None,
                is_dvd: false,
                output_base: &output_base,
                resize_targets: None,
                hfsplus_clone_targets: None,
                alignment_sectors: 0,
                checksum_type: crate::backup::ChecksumType::Sha256,
            },
            &mut progress_cb,
            &cancel,
            &mut log_cb,
            &mut |_, _| {},
        )
        .expect("APM single-file CHD backup");
        assert_eq!(result.container_logical_size, total_bytes);

        // Reopen + verify the head region (DDR + APM entries) round-trips
        // and the data partition still has its pattern at the declared
        // offset.
        let chd_path = tmp.path().join("disk.chd");
        let mut reader = ChdReader::open(&chd_path).unwrap();
        let mut readback = vec![0u8; total_bytes as usize];
        reader.read_exact(&mut readback).unwrap();
        // log_buf retained for in-test diagnostics if this regresses.
        let _ = &log_buf;

        // First two blocks must equal source verbatim.
        assert_eq!(
            &readback[..1024],
            &data[..1024],
            "DDR + first APM entry mismatch"
        );
        // Partition data must equal source.
        assert_eq!(
            &readback[part_offset..part_offset + part_len],
            &data[part_offset..part_offset + part_len],
            "APM partition body mismatch",
        );

        // And the table re-parses as APM.
        let mut br2 = BufReader::new(std::io::Cursor::new(readback));
        let detected = PartitionTable::detect(&mut br2).expect("detect after round-trip");
        assert!(
            matches!(detected, PartitionTable::Apm(_)),
            "round-tripped CHD should still parse as APM, got {}",
            detected.type_name(),
        );
    }

    /// Backup-time resize round-trip: build a 4 MiB MBR disk with one
    /// 0x83 (Linux) partition spanning sectors 1..=4095, ask
    /// `single_file_chd::run` to shrink it to half size, and verify the
    /// resulting CHD's MBR carries the new size + the partition body
    /// lives at its declared offset. We use a 0x83 partition so no
    /// in-place FS resize fires (the FS-specific resizers are tested in
    /// their own modules) — this exercises the plumbing in isolation.
    #[test]
    fn resize_plan_round_trip_shrinks_mbr_partition() {
        const TOTAL_SECTORS: u32 = 8192;
        const PART_SECTORS: u32 = 4095;
        const NEW_PART_SECTORS: u32 = 2048;
        const SECTOR_SIZE: u64 = 512;
        let total_bytes = (TOTAL_SECTORS as u64) * SECTOR_SIZE;
        let new_part_bytes = (NEW_PART_SECTORS as u64) * SECTOR_SIZE;

        let tmp = tempfile::tempdir().unwrap();
        let source_path = tmp.path().join("source.img");

        let mut data = vec![0u8; total_bytes as usize];
        data[..512].copy_from_slice(&build_test_mbr(PART_SECTORS));
        // Distinct pattern in the partition body.
        for i in 0..(PART_SECTORS as u64 * SECTOR_SIZE) as usize {
            data[512 + i] = ((i % 251) as u8).wrapping_add(7);
        }
        std::fs::write(&source_path, &data).unwrap();
        let source_file = File::open(&source_path).unwrap();

        let mut br = BufReader::new(source_file.try_clone().unwrap());
        let table = PartitionTable::detect(&mut br).expect("detect MBR");
        let partitions = table.partitions();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].size_bytes, PART_SECTORS as u64 * SECTOR_SIZE);

        let mbr_bytes: [u8; 512] = data[..512].try_into().unwrap();
        let output_base = tmp.path().join("disk");
        let resize_targets = vec![(0usize, new_part_bytes)];

        let mut log_buf: Vec<String> = Vec::new();
        let mut log_cb = |s: &str| log_buf.push(s.to_string());
        let mut progress_cb = |_: u64| {};
        let cancel = || false;

        let result = run(
            SingleFileChdInputs {
                source_file: &source_file,
                source_size: total_bytes,
                source_partition_table_bytes: &mbr_bytes,
                partition_table: &table,
                partitions: &partitions,
                partition_filter: None,
                sector_by_sector: false,
                chd_options: None,
                is_dvd: false,
                output_base: &output_base,
                resize_targets: Some(&resize_targets),
                hfsplus_clone_targets: None,
                alignment_sectors: 0,
                checksum_type: crate::backup::ChecksumType::Sha256,
            },
            &mut progress_cb,
            &cancel,
            &mut log_cb,
            &mut |_, _| {},
        )
        .expect("resize-plan single-file CHD backup");

        // Disk envelope now shrinks to fit the resized partition extent
        // (was previously locked to `total_bytes`). Floor: end-of-last-
        // partition rounded up to alignment. For this MBR fixture
        // (alignment_sectors=0 → 512-byte alignment, no GPT trailer) it
        // equals new_part_start + new_part_bytes = 512 + new_part_bytes.
        let expected_envelope = 512 + new_part_bytes;
        assert_eq!(
            result.container_logical_size, expected_envelope,
            "disk envelope shrinks to fit resized partition"
        );
        assert!(
            result.container_logical_size < total_bytes,
            "shrunken envelope must be smaller than source"
        );
        assert_eq!(result.partition_ranges.len(), 1);
        assert_eq!(result.partition_ranges[0].offset_in_disk, 512);
        assert_eq!(
            result.partition_ranges[0].length, new_part_bytes,
            "metadata length must reflect new partition size"
        );

        // Read back: MBR entry should now be NEW_PART_SECTORS.
        let chd_path = tmp.path().join("disk.chd");
        let mut reader = ChdReader::open(&chd_path).expect("reopen CHD");
        let mut head = [0u8; 512];
        reader.read_exact(&mut head).unwrap();
        let entry_size_lba = u32::from_le_bytes([
            head[446 + 12],
            head[446 + 13],
            head[446 + 14],
            head[446 + 15],
        ]);
        assert_eq!(
            entry_size_lba, NEW_PART_SECTORS,
            "MBR partition size must be patched to the new value",
        );

        // Partition body bytes 0..new_part_bytes must equal source's
        // first new_part_bytes — we shrank, so we kept the head data.
        reader.seek(SeekFrom::Start(512)).unwrap();
        let mut body = vec![0u8; new_part_bytes as usize];
        reader.read_exact(&mut body).unwrap();
        assert_eq!(
            body,
            data[512..512 + new_part_bytes as usize],
            "partition body must equal source's leading new_part_bytes",
        );

        // Disk envelope now shrinks to (512 + new_part_bytes), so the
        // old "tail past new partition end" no longer exists in the CHD.
        // Verify by reading one byte at the old-partition tail offset and
        // confirming the read fails (past EOF).
        reader.seek(SeekFrom::Start(512 + new_part_bytes)).unwrap();
        let mut one = [0u8; 1];
        let beyond = reader.read_exact(&mut one);
        assert!(
            beyond.is_err(),
            "disk envelope must end exactly at 512 + new_part_bytes; \
             reading past that should EOF (got {beyond:?})"
        );
    }

    /// Build an in-memory FAT12 partition image with cluster 2 allocated
    /// (EOF chain) and clusters 3..N free. Free-cluster sectors are
    /// pre-filled with 0xAB so we can prove the layout-preserving reader
    /// emits zeros there even though the source had garbage.
    fn build_test_fat12_partition() -> Vec<u8> {
        const BYTES_PER_SECTOR: usize = 512;
        const TOTAL_SECTORS: usize = 16;
        let mut img = vec![0u8; TOTAL_SECTORS * BYTES_PER_SECTOR];

        // Boot sector (BPB).
        img[0] = 0xEB;
        img[1] = 0x3C;
        img[2] = 0x90;
        img[3..11].copy_from_slice(b"MSDOS5.0");
        img[11..13].copy_from_slice(&(BYTES_PER_SECTOR as u16).to_le_bytes());
        img[13] = 1; // sectors per cluster
        img[14..16].copy_from_slice(&1u16.to_le_bytes()); // reserved sectors
        img[16] = 1; // num FATs
        img[17..19].copy_from_slice(&16u16.to_le_bytes()); // root entries
        img[19..21].copy_from_slice(&(TOTAL_SECTORS as u16).to_le_bytes());
        img[21] = 0xF8; // media byte
        img[22..24].copy_from_slice(&1u16.to_le_bytes()); // sectors per FAT
        img[510] = 0x55;
        img[511] = 0xAA;

        // FAT (1 sector at offset 512). FAT12 entries 0+1 are reserved
        // (0xFF8 + 0xFFF), entry 2 = EOF (0xFFF), entries 3..N = 0 (free).
        let fat = 1 * BYTES_PER_SECTOR;
        // FAT12 packing: entries 0,1 share 3 bytes.
        img[fat] = 0xF8;
        img[fat + 1] = 0xFF;
        img[fat + 2] = 0xFF;
        // Entry 2 occupies bytes 3..4.5 — the low nibble of byte 4 is
        // entry 2's high 4 bits, the high nibble of byte 4 is entry 3's
        // low 4 bits. We want entry 2 = 0xFFF, entry 3 = 0x000.
        img[fat + 3] = 0xFF;
        img[fat + 4] = 0x0F;
        // All later FAT bytes stay 0 → clusters 3..N are free.

        // Root dir (1 sector, all zero = empty directory). Already zero.

        // Data area starts at sector 3. Cluster 2 = sector 3.
        let data_start = 3 * BYTES_PER_SECTOR;
        let cluster2 = &mut img[data_start..data_start + BYTES_PER_SECTOR];
        let payload = b"hello layout-preserving FAT world";
        cluster2[..payload.len()].copy_from_slice(payload);

        // Fill clusters 3..N with 0xAB garbage so we can detect the
        // layout-preserving reader actually zeros them.
        for cluster in 3..(TOTAL_SECTORS - 3) {
            let off = (3 + cluster - 2) * BYTES_PER_SECTOR;
            for b in &mut img[off..off + BYTES_PER_SECTOR] {
                *b = 0xAB;
            }
        }

        img
    }

    /// Compact-always round-trip: a synthetic FAT12 partition with
    /// cluster 2 allocated and clusters 3..N free-but-garbage-filled
    /// goes through the single-file CHD pipeline. We assert that the
    /// resulting CHD body is the *packed* FAT volume at the start of
    /// the partition extent (BPB shrunk, allocated cluster's payload
    /// preserved) followed by zero-fill — the source's 0xAB garbage
    /// in free clusters must not bleed through.
    #[test]
    fn packed_and_padded_fat_zeros_free_clusters() {
        const PART_SECTORS: u32 = 16;
        const TOTAL_SECTORS: u32 = PART_SECTORS + 1;
        const SECTOR_SIZE: u64 = 512;
        let total_bytes = (TOTAL_SECTORS as u64) * SECTOR_SIZE;

        let fat_image = build_test_fat12_partition();
        assert_eq!(fat_image.len(), (PART_SECTORS as usize) * 512);

        let tmp = tempfile::tempdir().unwrap();
        let source_path = tmp.path().join("source.img");
        let mut data = vec![0u8; total_bytes as usize];
        // MBR with one FAT12 partition (type 0x01) starting at LBA 1.
        let mut mbr = [0u8; 512];
        mbr[510] = 0x55;
        mbr[511] = 0xAA;
        mbr[450] = 0x01; // FAT12
        mbr[454..458].copy_from_slice(&1u32.to_le_bytes());
        mbr[458..462].copy_from_slice(&PART_SECTORS.to_le_bytes());
        data[..512].copy_from_slice(&mbr);
        data[512..512 + fat_image.len()].copy_from_slice(&fat_image);
        std::fs::write(&source_path, &data).unwrap();

        let source_file = File::open(&source_path).unwrap();
        let mut br = BufReader::new(source_file.try_clone().unwrap());
        let table = PartitionTable::detect(&mut br).expect("detect MBR");
        let partitions = table.partitions();
        let mbr_bytes: [u8; 512] = data[..512].try_into().unwrap();

        let output_base = tmp.path().join("disk");
        let mut log_buf: Vec<String> = Vec::new();
        let mut log_cb = |s: &str| log_buf.push(s.to_string());
        let mut progress_cb = |_: u64| {};
        let cancel = || false;
        let result = run(
            SingleFileChdInputs {
                source_file: &source_file,
                source_size: total_bytes,
                source_partition_table_bytes: &mbr_bytes,
                partition_table: &table,
                partitions: &partitions,
                partition_filter: None,
                sector_by_sector: false,
                chd_options: None,
                is_dvd: false,
                output_base: &output_base,
                resize_targets: None,
                hfsplus_clone_targets: None,
                alignment_sectors: 0,
                checksum_type: crate::backup::ChecksumType::Sha256,
            },
            &mut progress_cb,
            &cancel,
            &mut log_cb,
            &mut |_, _| {},
        )
        .expect("layout-preserving FAT backup");

        assert_eq!(result.container_logical_size, total_bytes);
        let joined = log_buf.join("\n");
        assert!(
            joined.contains("packed-and-padded reader"),
            "log must mention packed-and-padded reader; got:\n{joined}",
        );

        let chd_path = tmp.path().join("disk.chd");
        let mut reader = ChdReader::open(&chd_path).expect("reopen CHD");
        let mut readback = vec![0u8; total_bytes as usize];
        reader.read_exact(&mut readback).unwrap();

        // MBR sector survives unchanged at offset 0.
        assert_eq!(&readback[..512], &data[..512], "MBR must round-trip");

        // Inside the partition extent: BPB at offset 512 has
        // total_sectors patched to the new (smaller) packed footprint.
        let bpb = &readback[512..1024];
        assert_eq!(&bpb[..3], &[0xEB, 0x3C, 0x90], "BPB jump must be intact");
        let new_total_sectors_16 = u16::from_le_bytes([bpb[19], bpb[20]]);
        assert!(
            (new_total_sectors_16 as u32) < PART_SECTORS,
            "packed BPB.total_sectors ({new_total_sectors_16}) must be smaller than \
             original partition extent ({PART_SECTORS})",
        );

        // Cluster 2's payload survived through the pipeline. Its
        // position inside the packed image is reserved + FAT + root_dir
        // sectors from partition start, but rather than recompute that
        // here we just look for the marker bytes anywhere in the
        // partition body.
        let part = &readback[512..512 + PART_SECTORS as usize * 512];
        assert!(
            part.windows(b"hello layout-preserving FAT world".len())
                .any(|w| w == b"hello layout-preserving FAT world"),
            "cluster 2's payload must survive the pack-and-pad round trip",
        );

        // The trailing slack — bytes after the packed volume — must be
        // zero, even though the source had 0xAB garbage in its free
        // clusters. Use the new BPB total_sectors as the boundary.
        let packed_end = 512 + (new_total_sectors_16 as usize) * 512;
        let part_end = 512 + (PART_SECTORS as usize) * 512;
        if packed_end < part_end {
            let tail = &readback[packed_end..part_end];
            assert!(
                tail.iter().all(|b| *b == 0),
                "packed-tail slack must be zero; saw {} non-zero bytes",
                tail.iter().filter(|b| **b != 0).count(),
            );
        }

        // Sanity: the source had 0xAB garbage in its free clusters, so
        // the zero-tail assertion above is meaningful.
        let src_free = &data[512 + 4 * 512..512 + (PART_SECTORS as usize) * 512];
        assert!(
            src_free.iter().any(|b| *b == 0xAB),
            "test setup failed — source free region must contain garbage",
        );
    }

    /// Stage 7: feed `refresh_metadata_after_edit` a backup folder whose
    /// metadata.json has stale partition + container checksums. After the
    /// call, both should reflect the actual CHD's contents.
    #[test]
    fn refresh_metadata_after_edit_recomputes_checksums() {
        use crate::backup::metadata::{
            AlignmentMetadata, BackupLayout, BackupMetadata, PartitionMetadata,
        };

        // Build a real single-file CHD backup body via `run` so the
        // recomputed numbers match values the production path agrees with.
        const TOTAL_SECTORS: u32 = 8192;
        const PART_SECTORS: u32 = 4095;
        const SECTOR_SIZE: u64 = 512;
        let total_bytes = (TOTAL_SECTORS as u64) * SECTOR_SIZE;
        let part_bytes = (PART_SECTORS as u64) * SECTOR_SIZE;

        let tmp = tempfile::tempdir().expect("tempdir");
        let backup_folder = tmp.path().join("mybackup");
        std::fs::create_dir(&backup_folder).unwrap();
        let source_path = tmp.path().join("source.img");

        let mut data = vec![0u8; total_bytes as usize];
        data[..512].copy_from_slice(&build_test_mbr(PART_SECTORS));
        for i in 0..(part_bytes as usize) {
            data[512 + i] = ((i % 251) as u8).wrapping_add(1);
        }
        std::fs::write(&source_path, &data).unwrap();

        let source_file = File::open(&source_path).unwrap();
        let source_size = source_file.metadata().unwrap().len();
        let mut br = BufReader::new(source_file.try_clone().unwrap());
        let table = PartitionTable::detect(&mut br).expect("detect MBR");
        let partitions = table.partitions();
        let mbr_bytes = {
            let mut buf = [0u8; 512];
            buf.copy_from_slice(&data[..512]);
            buf
        };
        let output_base = backup_folder.join("mybackup");
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
            resize_targets: None,
            hfsplus_clone_targets: None,
            alignment_sectors: 0,
            checksum_type: crate::backup::ChecksumType::Sha256,
        };
        let mut log_cb = |_: &str| {};
        let mut progress_cb = |_: u64| {};
        let cancel_check = || false;
        let result = run(
            inputs,
            &mut progress_cb,
            &cancel_check,
            &mut log_cb,
            &mut |_, _| {},
        )
        .expect("single-file CHD backup");

        let real_part_checksum = result.partition_ranges[0].checksum.clone();
        let real_container_sha1 = result.container_sha1.clone();

        // Write a metadata.json with intentionally-stale checksums.
        let metadata = BackupMetadata {
            version: 1,
            created: "2026-05-05T00:00:00Z".to_string(),
            source_device: source_path.display().to_string(),
            source_size_bytes: source_size,
            partition_table_type: "MBR".to_string(),
            checksum_type: "sha256".to_string(),
            compression_type: "chd".to_string(),
            split_size_mib: None,
            sector_by_sector: false,
            layout: BackupLayout::SingleFileChd,
            container: Some(result.container_filename.clone()),
            container_logical_size: Some(result.container_logical_size),
            container_sha1: Some("stale-container-sha1".to_string()),
            size_policy: None,
            alignment: AlignmentMetadata {
                detected_type: "None".to_string(),
                first_partition_lba: 1,
                alignment_sectors: 0,
                heads: 0,
                sectors_per_track: 0,
            },
            partitions: vec![PartitionMetadata {
                index: 0,
                type_name: "Linux".to_string(),
                partition_type_byte: 0x83,
                start_lba: 1,
                original_size_bytes: part_bytes,
                imaged_size_bytes: part_bytes,
                compressed_files: vec![],
                checksum: "stale-partition-checksum".to_string(),
                resized: false,
                compacted: true,
                is_logical: false,
                partition_type_string: None,
                minimum_size_bytes: None,
                defragmented_min_size_bytes: None,
                hfsplus_signature: None,
                defragmented_clone: false,
            }],
            bad_sectors: vec![],
            extended_container: None,
        };
        let metadata_path = backup_folder.join("metadata.json");
        std::fs::write(
            &metadata_path,
            serde_json::to_string_pretty(&metadata).unwrap(),
        )
        .unwrap();

        // Run the helper and re-read.
        refresh_metadata_after_edit(&backup_folder, &mut |_| {}).expect("refresh metadata");

        let after: BackupMetadata =
            serde_json::from_str(&std::fs::read_to_string(&metadata_path).unwrap()).unwrap();
        assert_eq!(
            after.partitions[0].checksum, real_part_checksum,
            "partition checksum must be refreshed to the live CHD value",
        );
        assert_eq!(
            after.container_sha1.as_deref(),
            Some(real_container_sha1.as_str()),
            "container_sha1 must be refreshed to the live CHD value",
        );
    }

    /// Stage 8a: re-export a raw image to a CHD with no resize plan.
    /// Verifies the destination filename matches what the user asked for
    /// and the bytes round-trip.
    #[test]
    fn run_export_no_resize_round_trips_raw_to_chd() {
        const TOTAL_SECTORS: u32 = 8192;
        const PART_SECTORS: u32 = 4095;
        const SECTOR_SIZE: u64 = 512;
        let total_bytes = (TOTAL_SECTORS as u64) * SECTOR_SIZE;

        let tmp = tempfile::tempdir().unwrap();
        let source_path = tmp.path().join("source.img");

        let mut data = vec![0u8; total_bytes as usize];
        data[..512].copy_from_slice(&build_test_mbr(PART_SECTORS));
        for i in 0..(PART_SECTORS as u64 * SECTOR_SIZE) as usize {
            data[512 + i] = ((i % 251) as u8).wrapping_add(13);
        }
        std::fs::write(&source_path, &data).unwrap();

        let mut br = BufReader::new(File::open(&source_path).unwrap());
        let table = PartitionTable::detect(&mut br).unwrap();
        let partitions = table.partitions();
        let mbr_bytes: [u8; 512] = data[..512].try_into().unwrap();

        let dest_path = tmp.path().join("export.chd");
        let mut log_buf: Vec<String> = Vec::new();
        let mut log_cb = |s: &str| log_buf.push(s.to_string());
        let mut progress_cb = |_: u64| {};
        let cancel = || false;

        run_export(
            SingleFileChdExportInputs {
                source_path: &source_path,
                partition_table: &table,
                partitions: &partitions,
                source_partition_table_bytes: &mbr_bytes,
                alignment_sectors: 0,
                dest_path: &dest_path,
                chd_options: None,
                is_dvd: false,
                resize_targets: None,
            },
            &mut progress_cb,
            &cancel,
            &mut log_cb,
        )
        .expect("run_export no-resize");

        assert!(
            dest_path.exists(),
            "export must produce {}",
            dest_path.display()
        );
        let mut reader = ChdReader::open(&dest_path).unwrap();
        let mut readback = vec![0u8; total_bytes as usize];
        reader.read_exact(&mut readback).unwrap();
        assert_eq!(
            readback, data,
            "no-resize re-export must round-trip the source byte-for-byte",
        );
    }

    /// Stage 8a: re-export a raw image with a per-partition shrink plan.
    /// Verifies the output's MBR carries the new partition size and the
    /// (smaller) partition body bytes match the source's prefix.
    #[test]
    fn run_export_with_resize_shrinks_partition() {
        const TOTAL_SECTORS: u32 = 8192;
        const PART_SECTORS: u32 = 4095;
        const NEW_PART_SECTORS: u32 = 2048;
        const SECTOR_SIZE: u64 = 512;
        let total_bytes = (TOTAL_SECTORS as u64) * SECTOR_SIZE;
        let new_part_bytes = (NEW_PART_SECTORS as u64) * SECTOR_SIZE;

        let tmp = tempfile::tempdir().unwrap();
        let source_path = tmp.path().join("source.img");

        let mut data = vec![0u8; total_bytes as usize];
        data[..512].copy_from_slice(&build_test_mbr(PART_SECTORS));
        for i in 0..(PART_SECTORS as u64 * SECTOR_SIZE) as usize {
            data[512 + i] = ((i % 251) as u8).wrapping_add(19);
        }
        std::fs::write(&source_path, &data).unwrap();

        let mut br = BufReader::new(File::open(&source_path).unwrap());
        let table = PartitionTable::detect(&mut br).unwrap();
        let partitions = table.partitions();
        let mbr_bytes: [u8; 512] = data[..512].try_into().unwrap();

        let dest_path = tmp.path().join("export.chd");
        let resize_targets = vec![(0usize, new_part_bytes)];
        let mut log_buf: Vec<String> = Vec::new();
        let mut log_cb = |s: &str| log_buf.push(s.to_string());
        let mut progress_cb = |_: u64| {};
        let cancel = || false;

        run_export(
            SingleFileChdExportInputs {
                source_path: &source_path,
                partition_table: &table,
                partitions: &partitions,
                source_partition_table_bytes: &mbr_bytes,
                alignment_sectors: 0,
                dest_path: &dest_path,
                chd_options: None,
                is_dvd: false,
                resize_targets: Some(&resize_targets),
            },
            &mut progress_cb,
            &cancel,
            &mut log_cb,
        )
        .expect("run_export with resize");

        let mut reader = ChdReader::open(&dest_path).unwrap();
        // MBR entry should reflect the new size.
        let mut head = [0u8; 512];
        reader.read_exact(&mut head).unwrap();
        let entry_size_lba = u32::from_le_bytes([
            head[0x1BE + 12],
            head[0x1BE + 13],
            head[0x1BE + 14],
            head[0x1BE + 15],
        ]);
        assert_eq!(
            entry_size_lba as u64, NEW_PART_SECTORS as u64,
            "MBR entry size must be the new size after resize",
        );
        // Partition body prefix should match the source's first
        // `new_part_bytes` of partition data.
        reader.seek(SeekFrom::Start(SECTOR_SIZE)).unwrap();
        let mut body = vec![0u8; new_part_bytes as usize];
        reader.read_exact(&mut body).unwrap();
        assert_eq!(
            body,
            data[512..(512 + new_part_bytes as usize)],
            "shrunk partition body must match source prefix",
        );
    }

    /// Stage 8a: estimate sums partition sizes + adds a head/tail
    /// allowance for the resize path; output_bytes uses the supplied CHD
    /// file size when provided.
    #[test]
    fn estimate_export_disk_usage_matches_partition_layout() {
        const TOTAL_SECTORS: u32 = 8192;
        const PART_SECTORS: u32 = 4095;
        let mbr_bytes = build_test_mbr(PART_SECTORS);
        let mut br = std::io::Cursor::new(mbr_bytes.to_vec());
        let table = PartitionTable::detect(&mut br).unwrap();
        let partitions = table.partitions();

        let est_no_resize = estimate_export_disk_usage(
            (TOTAL_SECTORS as u64) * 512,
            Some(123_456_789),
            &partitions,
            false,
        );
        assert_eq!(est_no_resize.scratch_bytes, 0);
        assert_eq!(est_no_resize.output_bytes, 123_456_789);

        let est_resize = estimate_export_disk_usage(
            (TOTAL_SECTORS as u64) * 512,
            Some(123_456_789),
            &partitions,
            true,
        );
        // Sum of partition sizes + 1 MiB head/tail allowance.
        let sum: u64 = partitions.iter().map(|p| p.size_bytes).sum();
        assert_eq!(est_resize.scratch_bytes, sum + 1024 * 1024);
        assert_eq!(est_resize.output_bytes, 123_456_789);
        assert_eq!(est_resize.required_total(), sum + 1024 * 1024 + 123_456_789,);
    }

    /// Commit 1a: `assemble_from_staging` reads a zstd-staged partition
    /// body and emits a CHD whose decompressed image matches the source.
    /// Builds a synthetic 4 MiB MBR disk with one partition, writes the
    /// partition body to `<staging>/partition-0.zst`, runs the assembler,
    /// and verifies the round-trip.
    #[test]
    fn assemble_from_staging_round_trip_mbr_single_partition() {
        const TOTAL_SECTORS: u32 = 8192;
        const PART_SECTORS: u32 = 4095;
        const SECTOR_SIZE: u64 = 512;
        let total_bytes = (TOTAL_SECTORS as u64) * SECTOR_SIZE;
        let part_bytes = (PART_SECTORS as u64) * SECTOR_SIZE;

        let tmp = tempfile::tempdir().expect("tempdir");
        let staging = tmp.path().join("staging");
        std::fs::create_dir(&staging).unwrap();

        // Build the source bytes (MBR + body + zero tail) just so we can
        // compare against the decompressed CHD. Only the MBR + body are
        // fed to the assembler; the tail is zero-filled by the stream
        // builder.
        let mut data = vec![0u8; total_bytes as usize];
        let mbr = build_test_mbr(PART_SECTORS);
        data[..512].copy_from_slice(&mbr);
        for i in 0..(part_bytes as usize) {
            data[512 + i] = ((i % 251) as u8).wrapping_add(1);
        }

        // Stage the partition body as partition-0.zst.
        let body = &data[512..(512 + part_bytes as usize)];
        let staged_path = staging.join("partition-0.zst");
        {
            let f = File::create(&staged_path).unwrap();
            let mut enc = zstd::Encoder::new(f, 3).unwrap();
            enc.write_all(body).unwrap();
            enc.finish().unwrap();
        }

        // Parse the synthesised MBR the same way `run_backup` would.
        let mut br = std::io::Cursor::new(mbr.to_vec());
        let table = PartitionTable::detect(&mut br).unwrap();
        let partitions = table.partitions();
        assert_eq!(partitions.len(), 1);

        // No resize: plan mirrors the source layout.
        let part = &partitions[0];
        let plans = vec![PartitionResizePlan {
            index: part.index,
            old_start_lba: part.start_lba,
            old_size_bytes: part.size_bytes,
            new_start_lba: part.start_lba,
            new_size_bytes: part.size_bytes,
            needs_data_move: false,
            move_delta_bytes: 0,
        }];

        let output_base = tmp.path().join("disk");
        let inputs = AssembleFromStagingInputs {
            staging_dir: &staging,
            plans: &plans,
            source_partition_table_bytes: &mbr,
            partition_table: &table,
            partitions: &partitions,
            source_size: total_bytes,
            output_base: &output_base,
            chd_options: None,
            is_dvd: false,
            alignment_sectors: 0,
            source_head_region: &[],
            checksum_type: crate::backup::ChecksumType::Sha256,
        };

        let mut log_buf: Vec<String> = Vec::new();
        let mut log_cb = |s: &str| log_buf.push(s.to_string());
        let mut progress_seen: u64 = 0;
        let mut progress_cb = |n: u64| progress_seen = progress_seen.max(n);
        let cancel_check = || false;

        let result = assemble_from_staging(
            inputs,
            &mut progress_cb,
            &cancel_check,
            &mut log_cb,
            &mut |_, _| {},
        )
        .expect("assemble_from_staging");

        // The shrunken envelope is the byte just past the last partition's
        // new extent (no GPT trailer, no aligned-up source size). For
        // this single-MBR-partition layout the assembled disk covers
        // sectors 0..(1 + PART_SECTORS) bytes — the source disk's
        // trailing zero pad is not part of the CHD.
        let expected_extent = 512 + part_bytes;
        assert_eq!(result.container_filename, "disk.chd");
        assert_eq!(result.container_logical_size, expected_extent);
        assert_eq!(result.partition_ranges.len(), 1);
        assert_eq!(result.partition_ranges[0].offset_in_disk, 512);
        assert_eq!(result.partition_ranges[0].length, part_bytes);
        assert!(progress_seen > 0, "progress callback never fired");

        let chd_path = tmp.path().join("disk.chd");
        let mut reader = ChdReader::open(&chd_path).expect("reopen CHD");
        let mut readback = vec![0u8; expected_extent as usize];
        reader.read_exact(&mut readback).expect("read full CHD");
        assert_eq!(
            readback,
            data[..expected_extent as usize],
            "assembled CHD must round-trip the synthesised disk image byte-for-byte over the envelope",
        );
    }

    /// Commit 1b: `run_via_staging` (no resize, MBR, raw passthrough)
    /// produces a CHD that round-trips the source disk. Exercises the
    /// full stage-then-assemble cycle end-to-end and verifies the
    /// staging dir is cleaned up afterwards.
    #[test]
    fn run_via_staging_round_trip_mbr_no_resize() {
        const TOTAL_SECTORS: u32 = 8192;
        const PART_SECTORS: u32 = 4095;
        const SECTOR_SIZE: u64 = 512;
        let total_bytes = (TOTAL_SECTORS as u64) * SECTOR_SIZE;
        let part_bytes = (PART_SECTORS as u64) * SECTOR_SIZE;

        let tmp = tempfile::tempdir().expect("tempdir");
        let source_path = tmp.path().join("source.img");

        let mut data = vec![0u8; total_bytes as usize];
        data[..512].copy_from_slice(&build_test_mbr(PART_SECTORS));
        for i in 0..(part_bytes as usize) {
            data[512 + i] = ((i % 251) as u8).wrapping_add(1);
        }
        std::fs::write(&source_path, &data).unwrap();

        let source_file = File::open(&source_path).unwrap();
        let source_size = source_file.metadata().unwrap().len();
        let mut br = BufReader::new(source_file.try_clone().unwrap());
        let table = PartitionTable::detect(&mut br).unwrap();
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
            resize_targets: None,
            hfsplus_clone_targets: None,
            alignment_sectors: 0,
            checksum_type: crate::backup::ChecksumType::Sha256,
        };

        let mut log_buf: Vec<String> = Vec::new();
        let mut log_cb = |s: &str| log_buf.push(s.to_string());
        let mut progress_seen: u64 = 0;
        let mut progress_cb = |n: u64| progress_seen = progress_seen.max(n);
        let cancel_check = || false;

        let result = run_via_staging(
            inputs,
            &mut progress_cb,
            &cancel_check,
            &mut log_cb,
            &mut |_, _| {},
            &mut |_| {},
            None,
        )
        .expect("run_via_staging");

        let expected_extent = 512 + part_bytes;
        assert_eq!(result.container_filename, "disk.chd");
        assert_eq!(result.container_logical_size, expected_extent);
        assert_eq!(result.partition_ranges.len(), 1);
        assert_eq!(result.partition_ranges[0].offset_in_disk, 512);
        assert_eq!(result.partition_ranges[0].length, part_bytes);
        assert!(progress_seen > 0, "progress callback never fired");

        let chd_path = tmp.path().join("disk.chd");
        let mut reader = ChdReader::open(&chd_path).expect("reopen CHD");
        let mut readback = vec![0u8; expected_extent as usize];
        reader.read_exact(&mut readback).expect("read full CHD");
        assert_eq!(
            readback,
            data[..expected_extent as usize],
            "run_via_staging must round-trip the source disk byte-for-byte over the envelope",
        );

        // Staging dir must be gone — tempfile::TempDir's drop runs on
        // success. Glob for any leftover ".rb-chd-staging-*" sibling.
        let parent = output_base.parent().unwrap();
        for entry in std::fs::read_dir(parent).unwrap() {
            let name = entry.unwrap().file_name();
            let s = name.to_string_lossy();
            assert!(
                !s.starts_with(".rb-chd-staging-"),
                "staging dir leaked: {s}",
            );
        }
    }

    /// APM round-trip through the staging path. Mirrors
    /// `end_to_end_round_trip_apm` but exercises `run_via_staging`
    /// instead of `run` to confirm the head region is captured eagerly
    /// and overlaid with patched DDR + entries on the assembly side.
    #[test]
    fn run_via_staging_round_trip_apm() {
        const TOTAL_BLOCKS: u64 = 8192;
        const SECTOR: u64 = 512;
        let total_bytes = TOTAL_BLOCKS * SECTOR;

        let mut data = vec![0u8; total_bytes as usize];
        const DDR_SIG: u16 = 0x4552;
        data[0..2].copy_from_slice(&DDR_SIG.to_be_bytes());
        data[2..4].copy_from_slice(&512u16.to_be_bytes());
        data[4..8].copy_from_slice(&(TOTAL_BLOCKS as u32).to_be_bytes());

        const ENTRY_SIG: u16 = 0x504D;
        let entries = [
            ("Apple", "Apple_partition_map", 1u32, 2u32),
            ("MacOS", "Apple_HFS", 64u32, 4096u32),
        ];
        for (i, (name, ptype, start, count)) in entries.iter().enumerate() {
            let off = (1 + i) * 512;
            data[off..off + 2].copy_from_slice(&ENTRY_SIG.to_be_bytes());
            data[off + 4..off + 8].copy_from_slice(&(entries.len() as u32).to_be_bytes());
            data[off + 8..off + 12].copy_from_slice(&start.to_be_bytes());
            data[off + 12..off + 16].copy_from_slice(&count.to_be_bytes());
            let nb = name.as_bytes();
            data[off + 16..off + 16 + nb.len()].copy_from_slice(nb);
            let tb = ptype.as_bytes();
            data[off + 48..off + 48 + tb.len()].copy_from_slice(tb);
            data[off + 84..off + 88].copy_from_slice(&count.to_be_bytes());
            data[off + 88..off + 92].copy_from_slice(&0x33u32.to_be_bytes());
        }
        let part_offset = 64usize * 512;
        let part_len = 4096usize * 512;
        for i in 0..part_len {
            data[part_offset + i] = ((i % 251) as u8).wrapping_add(2);
        }

        let tmp = tempfile::tempdir().unwrap();
        let source_path = tmp.path().join("source.img");
        std::fs::write(&source_path, &data).unwrap();
        let source_file = File::open(&source_path).unwrap();

        let mut br = BufReader::new(source_file.try_clone().unwrap());
        let table = PartitionTable::detect(&mut br).expect("detect APM");
        assert!(matches!(table, PartitionTable::Apm(_)));
        let partitions = table.partitions();

        let output_base = tmp.path().join("disk");
        let mbr_bytes: [u8; 512] = data[..512].try_into().unwrap();
        let mut log_buf: Vec<String> = Vec::new();
        let mut log_cb = |s: &str| log_buf.push(s.to_string());
        let mut progress_cb = |_: u64| {};
        let cancel = || false;

        let result = run_via_staging(
            SingleFileChdInputs {
                source_file: &source_file,
                source_size: total_bytes,
                source_partition_table_bytes: &mbr_bytes,
                partition_table: &table,
                partitions: &partitions,
                partition_filter: None,
                sector_by_sector: true,
                chd_options: None,
                is_dvd: false,
                output_base: &output_base,
                resize_targets: None,
                hfsplus_clone_targets: None,
                alignment_sectors: 0,
                checksum_type: crate::backup::ChecksumType::Sha256,
            },
            &mut progress_cb,
            &cancel,
            &mut log_cb,
            &mut |_, _| {},
            &mut |_| {},
            None,
        )
        .expect("APM run_via_staging");
        assert_eq!(
            result.container_logical_size,
            (part_offset as u64) + (part_len as u64)
        );

        let chd_path = tmp.path().join("disk.chd");
        let mut reader = ChdReader::open(&chd_path).unwrap();
        let extent = result.container_logical_size as usize;
        let mut readback = vec![0u8; extent];
        reader.read_exact(&mut readback).unwrap();

        // DDR signature + first APM entry signature survive through the
        // head segment. (The DDR's block_count is rewritten to the
        // shrunken envelope and the entries' map_entries / sizes get
        // patched, so a byte-for-byte compare against source would
        // fail by design — that's verified later via `re-detect APM`.)
        assert_eq!(&readback[0..2], &data[0..2], "DDR signature mismatch");
        assert_eq!(
            &readback[512..514],
            &data[512..514],
            "first APM entry signature mismatch",
        );
        // Partition body survives at its declared offset.
        assert_eq!(
            &readback[part_offset..part_offset + part_len],
            &data[part_offset..part_offset + part_len],
            "APM partition body mismatch after run_via_staging",
        );

        let mut br2 = BufReader::new(std::io::Cursor::new(readback));
        let detected = PartitionTable::detect(&mut br2).expect("re-detect APM");
        assert!(
            matches!(detected, PartitionTable::Apm(_)),
            "round-tripped CHD should still parse as APM, got {}",
            detected.type_name(),
        );
    }

    /// HFS+ defrag-clone producer-thread error propagation: when the
    /// caller flags a partition as a clone target but the underlying
    /// bytes aren't valid HFS+, the producer thread's open error
    /// surfaces back to `run_via_staging`'s caller (rather than being
    /// swallowed or surfacing as a generic broken-pipe). Exercises the
    /// `producer.join()` path and the producer/consumer error
    /// reconciliation.
    #[test]
    fn run_via_staging_propagates_hfsplus_clone_producer_errors() {
        const PART_SECTORS: u32 = 4095;
        let mbr = build_test_mbr(PART_SECTORS);
        let mut br = std::io::Cursor::new(mbr.to_vec());
        let table = PartitionTable::detect(&mut br).unwrap();
        let partitions = table.partitions();
        let tmp = tempfile::tempdir().unwrap();
        let source_path = tmp.path().join("source.img");
        // All zeros — no valid HFS+ volume header, so the producer
        // thread's HfsPlusFilesystem::open call must fail.
        std::fs::write(&source_path, vec![0u8; ((PART_SECTORS as usize) + 1) * 512]).unwrap();
        let source_file = File::open(&source_path).unwrap();
        let source_size = source_file.metadata().unwrap().len();
        let output_base = tmp.path().join("disk");
        let clone_targets = vec![(
            partitions[0].index,
            partitions[0].size_bytes,
            fs::DefragCloneShape::Flat,
        )];
        let inputs = SingleFileChdInputs {
            source_file: &source_file,
            source_size,
            source_partition_table_bytes: &mbr,
            partition_table: &table,
            partitions: &partitions,
            partition_filter: None,
            sector_by_sector: false,
            chd_options: None,
            is_dvd: false,
            output_base: &output_base,
            resize_targets: None,
            hfsplus_clone_targets: Some(&clone_targets),
            alignment_sectors: 0,
            checksum_type: crate::backup::ChecksumType::Sha256,
        };
        let err = run_via_staging(
            inputs,
            &mut |_| {},
            &|| false,
            &mut |_| {},
            &mut |_, _| {},
            &mut |_| {},
            None,
        )
        .expect_err("invalid HFS+ source must surface a clone-producer error");
        let msg = format!("{err:#}");
        // Error chain should reference either the clone pipeline or the
        // HFS+ open. We don't pin to a specific layer since the exact
        // chain depends on which side races to fail first.
        assert!(
            msg.to_ascii_lowercase().contains("hfs+")
                || msg.to_ascii_lowercase().contains("defrag-clone"),
            "error message must reference HFS+ / defrag-clone path: {msg}",
        );
    }
}
