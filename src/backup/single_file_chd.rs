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
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;

use anyhow::{anyhow, Context, Result};

use super::disk_image_stream::DiskImageStreamBuilder;
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
    /// Sectors used to align partition starts when relocating after a
    /// resize. Must match the source disk's detected alignment so that
    /// vintage OSes still accept the layout. Ignored on the no-resize
    /// streaming path.
    pub alignment_sectors: u64,
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
/// single-file CHD mode. MBR / GPT / APM are supported; superfloppy still
/// falls back to the per-partition path because there is no partition table
/// to embed at sector 0.
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
pub fn run(
    inputs: SingleFileChdInputs<'_>,
    progress_cb: &mut dyn FnMut(u64),
    cancel_check: &dyn Fn() -> bool,
    log_cb: &mut dyn FnMut(&str),
) -> Result<SingleFileChdResult> {
    if !is_supported(inputs.partition_table) {
        anyhow::bail!(
            "single-file CHD backup does not support {} sources yet — \
             pick zstd/raw, or use the per-partition CHD flow.",
            inputs.partition_table.type_name(),
        );
    }

    // If the caller passed a non-trivial resize plan, fork off to the
    // sparse-temp-file pipeline. The streaming path below handles the
    // common "Original sizes" case at zero scratch-disk cost.
    if let Some(plans) = build_resize_plans(&inputs)? {
        return run_with_resize(inputs, &plans, progress_cb, cancel_check, log_cb);
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
            checksum_sha256: String::new(), // filled in after CHD write
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
    let container_sha1 = compute_post_write_metadata(&chd_path, &mut partition_ranges, log_cb)?;

    Ok(SingleFileChdResult {
        container_filename,
        container_logical_size: logical_size,
        container_sha1,
        partition_ranges,
    })
}

/// Build a per-partition resize plan from `inputs.resize_targets`. Returns
/// `Ok(None)` when no resize is requested, or when every requested target
/// matches the source size (i.e. a no-op the streaming path can serve
/// faster). Returns `Ok(Some(plans))` when at least one partition would
/// move or change size.
fn build_resize_plans(
    inputs: &SingleFileChdInputs<'_>,
) -> Result<Option<Vec<PartitionResizePlan>>> {
    let targets = match inputs.resize_targets {
        Some(t) if !t.is_empty() => t,
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
    if effective.is_empty() {
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
    if !any_change {
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

/// Sparse-temp-file resize pipeline. See module-level doc and
/// `docs/whole_disk_chd_backup.md` (Stage 4b, Approach A) for the
/// design rationale.
fn run_with_resize(
    inputs: SingleFileChdInputs<'_>,
    plans: &[PartitionResizePlan],
    progress_cb: &mut dyn FnMut(u64),
    cancel_check: &dyn Fn() -> bool,
    log_cb: &mut dyn FnMut(&str),
) -> Result<SingleFileChdResult> {
    log_cb(&format!(
        "Single-file CHD: backup-time resize plan covers {} partition(s)",
        plans
            .iter()
            .filter(|p| p.new_size_bytes != p.old_size_bytes || p.needs_data_move)
            .count(),
    ));

    let overrides = plans_to_overrides(plans);
    // Disk-level logical size stays at the source's size for now: the
    // resize plan is constrained against `source_size` already (see
    // `compute_resize_plan`), so partitions never overflow it. Shrinking
    // the disk envelope itself is a future enhancement.
    let target_size = inputs.source_size;

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
                checksum_sha256: String::new(),
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
    let container_sha1 = compute_post_write_metadata(&chd_path, &mut partition_ranges, log_cb)?;

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
            resize_targets: None,
            alignment_sectors: 0,
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
                alignment_sectors: 0,
            },
            &mut progress_cb,
            &cancel,
            &mut log_cb,
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
                alignment_sectors: 0,
            },
            &mut progress_cb,
            &cancel,
            &mut log_cb,
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
                alignment_sectors: 0,
            },
            &mut progress_cb,
            &cancel,
            &mut log_cb,
        )
        .expect("resize-plan single-file CHD backup");

        assert_eq!(
            result.container_logical_size, total_bytes,
            "disk envelope size unchanged in stage 4b"
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

        // Tail (between new partition end and old partition end) is
        // zero-fill territory — bytes the source had there were dropped
        // when we capped the copy at new_part_bytes.
        reader.seek(SeekFrom::Start(512 + new_part_bytes)).unwrap();
        let mut tail = vec![0u8; (PART_SECTORS - NEW_PART_SECTORS) as usize * 512];
        reader.read_exact(&mut tail).unwrap();
        assert!(
            tail.iter().all(|b| *b == 0),
            "shrunk-out tail must be zero-filled (was {} non-zero bytes)",
            tail.iter().filter(|b| **b != 0).count(),
        );
    }
}
