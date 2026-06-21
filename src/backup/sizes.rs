//! Per-partition smart-sizing analysis for the backup pipeline.
//!
//! Splits the bulk of `run_backup`'s pre-flight accounting into two steps:
//!   1. `analyze_partitions` — per-partition compaction probe + size selection.
//!   2. `compute_totals` — sum the per-partition figures into the totals used
//!      for the progress bar and the smart-sizing log line.
//!
//! For background on the sizes (effective vs stream vs minimum, packed vs
//! layout-preserving), see the comments in `run_backup` where this module is
//! called from.

use std::io::BufReader;
use std::sync::{Arc, Mutex};

use crate::fs;
use crate::partition::{self, PartitionInfo};

use super::{log, set_operation, BackupProgress, LogLevel};

/// Parallel per-partition vectors produced by `analyze_partitions`.
pub(super) struct PartitionSizing {
    /// Bytes representing the on-disk image we will reconstruct (logical data).
    pub effective_sizes: Vec<u64>,
    /// Bytes that will actually flow through the compressor.
    pub stream_sizes: Vec<u64>,
    /// `Some(compacted_size)` if a compact reader is available for partition i.
    pub compact_sizes: Vec<Option<u64>>,
    /// True filesystem minimum (last-data-byte) for the resize/restore UI.
    pub minimum_sizes: Vec<Option<u64>>,
    /// Defragmented minimum (post-clone-shrink). `Some` only when the
    /// filesystem reports a value smaller than `minimum_sizes[i]` — currently
    /// HFS+ on fragmented volumes; `None` otherwise.
    pub defragmented_min_sizes: Vec<Option<u64>>,
    /// True when the compact reader emits free blocks as zeros (HFS/HFS+/ext/btrfs).
    pub is_layout_preserving_flags: Vec<bool>,
}

/// Per-partition analysis: try compaction, fall back to trim-based sizing.
///
/// `source` mints fresh seekable readers (a cloned local `File` or a
/// `RemoteBlockReader`) per partition probe — the analysis never holds more
/// than one open at a time.
pub(super) fn analyze_partitions(
    source: &super::SourceFactory,
    partitions: &[PartitionInfo],
    sector_by_sector: bool,
    is_superfloppy: bool,
    precomputed_min: Option<&[(usize, u64)]>,
    progress: &Arc<Mutex<BackupProgress>>,
) -> PartitionSizing {
    let n = partitions.len();
    let mut sizing = PartitionSizing {
        effective_sizes: Vec::with_capacity(n),
        stream_sizes: Vec::with_capacity(n),
        compact_sizes: Vec::with_capacity(n),
        minimum_sizes: Vec::with_capacity(n),
        defragmented_min_sizes: Vec::with_capacity(n),
        is_layout_preserving_flags: Vec::with_capacity(n),
    };

    for part in partitions.iter() {
        log(
            progress,
            LogLevel::Info,
            format!(
                "Analyzing partition-{}: {} at LBA {}",
                part.index, part.type_name, part.start_lba
            ),
        );

        // Skip GPT protective partitions (0xEE) — not a real partition.
        if part.partition_type_byte == 0xEE {
            push_skip(&mut sizing, 0);
            continue;
        }
        if sector_by_sector || part.is_extended_container || is_superfloppy {
            push_skip(&mut sizing, part.size_bytes);
            continue;
        }

        log(
            progress,
            LogLevel::Info,
            format!("Attempting compaction for partition-{}...", part.index),
        );
        let part_offset = part.start_lba * 512;
        let clone_result = source.open();
        if let Err(ref e) = clone_result {
            log(
                progress,
                LogLevel::Warning,
                format!(
                    "Failed to clone file handle for partition-{}: {}",
                    part.index, e
                ),
            );
        }
        let compact_result = match clone_result {
            Err(_) => None,
            Ok(clone) => match fs::try_compact_partition_reader(
                BufReader::new(clone),
                part_offset,
                part.partition_type_byte,
                part.partition_type_string.as_deref(),
            ) {
                Ok((_, result)) => {
                    log(
                        progress,
                        LogLevel::Info,
                        format!(
                            "Compact analysis (partition-{}): {} allocated clusters, \
                             data={} stream={} original={} mode={}",
                            part.index,
                            result.clusters_used,
                            partition::format_size(result.data_size),
                            partition::format_size(result.compacted_size),
                            partition::format_size(result.original_size),
                            if result.compacted_size == result.original_size {
                                "layout-preserving (free blocks -> zeros)"
                            } else {
                                "packed"
                            },
                        ),
                    );
                    Some(result)
                }
                Err(ref msg) if msg.starts_with("unsupported:") => {
                    log(
                        progress,
                        LogLevel::Info,
                        format!(
                            "Compact reader not available for partition-{} ({}): {msg}",
                            part.index, part.type_name,
                        ),
                    );
                    None
                }
                Err(ref msg) => {
                    log(
                        progress,
                        LogLevel::Warning,
                        format!(
                            "Compact reader failed for partition-{} ({}): {msg}",
                            part.index, part.type_name,
                        ),
                    );
                    None
                }
            },
        };

        if let Some(ref result) = compact_result {
            let is_lp = result.compacted_size == result.original_size;
            sizing.compact_sizes.push(Some(result.compacted_size));
            sizing.is_layout_preserving_flags.push(is_lp);

            // Packed: data_size is the tightest possible image.
            // LP: ask the filesystem for last-used-block via effective_partition_size.
            let minimum = if !is_lp && result.data_size < part.size_bytes {
                Some(result.data_size)
            } else {
                source
                    .open()
                    .ok()
                    .and_then(|clone| {
                        fs::effective_partition_size(
                            BufReader::new(clone),
                            part_offset,
                            part.partition_type_byte,
                            part.partition_type_string.as_deref(),
                        )
                    })
                    .map(|min| min.min(part.size_bytes))
            };
            sizing.minimum_sizes.push(minimum);

            // Defragmented minimum: only meaningfully smaller than `minimum`
            // for HFS+ on fragmented volumes; for every other FS this falls
            // through to `last_data_byte` and matches `minimum`. The HFS+
            // walk reads the catalog + extents-overflow B-trees and can
            // take seconds to minutes on a large volume — surface a per-
            // partition op label and log line so the GUI doesn't appear
            // hung at "Exporting partition table…".
            let pre = precomputed_min.and_then(|tbl| {
                tbl.iter()
                    .find(|(idx, _)| *idx == part.index)
                    .map(|(_, v)| *v)
            });
            let needs_walk = fs::is_expensive_minimum(
                part.partition_type_byte,
                part.partition_type_string.as_deref(),
            ) && pre.is_none();
            if let Some(v) = pre {
                log(
                    progress,
                    LogLevel::Info,
                    format!(
                        "Partition-{}: reusing precomputed defragmented minimum {} (skipped volume walk)",
                        part.index,
                        partition::format_size(v),
                    ),
                );
            }
            if needs_walk {
                set_operation(
                    progress,
                    format!(
                        "Computing defragmented minimum for partition-{} (this can take a while)…",
                        part.index
                    ),
                );
                log(
                    progress,
                    LogLevel::Info,
                    format!(
                        "Walking partition-{} to compute defragmented minimum…",
                        part.index
                    ),
                );
            }
            let defrag = if let Some(v) = pre {
                // Precomputed value is partition-level (already includes
                // wrapper overhead for wrapped HFS+); compare against the
                // partition extent only — `minimum` is the inner-volume
                // trim point and isn't apples-to-apples with partition-
                // level. Filtering on `minimum` here would wrongly skip
                // wrapped HFS+ partitions whose partition-level shrink is
                // larger than the inner trim but still smaller than the
                // source extent.
                Some(v.min(part.size_bytes)).filter(|&d| d < part.size_bytes)
            } else {
                source
                    .open()
                    .ok()
                    .and_then(|clone| {
                        fs::defragmented_partition_size(
                            BufReader::new(clone),
                            part_offset,
                            part.partition_type_byte,
                            part.partition_type_string.as_deref(),
                        )
                    })
                    .map(|d| d.min(part.size_bytes))
                    .filter(|&d| match minimum {
                        Some(m) => d < m,
                        None => d < part.size_bytes,
                    })
            };
            if needs_walk {
                if let Some(d) = defrag {
                    log(
                        progress,
                        LogLevel::Info,
                        format!(
                            "Partition-{}: defragmented minimum {}",
                            part.index,
                            partition::format_size(d)
                        ),
                    );
                } else {
                    log(
                        progress,
                        LogLevel::Info,
                        format!(
                            "Partition-{}: defragmented minimum unavailable \
                             (filesystem refused to open or volume could not be walked)",
                            part.index
                        ),
                    );
                }
            }
            sizing.defragmented_min_sizes.push(defrag);

            // LP: trim stream to minimum so the free tail isn't compressed.
            // Packed: stream == compacted (already optimal).
            let stream = if is_lp {
                minimum.unwrap_or(result.compacted_size)
            } else {
                result.compacted_size
            };
            sizing.effective_sizes.push(stream);
            sizing.stream_sizes.push(stream);
        } else {
            // Trim-based fallback.
            let effective = source
                .open()
                .ok()
                .and_then(|clone| {
                    fs::effective_partition_size(
                        BufReader::new(clone),
                        part_offset,
                        part.partition_type_byte,
                        part.partition_type_string.as_deref(),
                    )
                })
                .map(|data_end| data_end.min(part.size_bytes));
            let sz = effective.unwrap_or(part.size_bytes);
            sizing.effective_sizes.push(sz);
            sizing.stream_sizes.push(sz);
            sizing.compact_sizes.push(None);
            sizing.minimum_sizes.push(effective);
            sizing.defragmented_min_sizes.push(None);
            sizing.is_layout_preserving_flags.push(false);
        }
    }

    sizing
}

fn push_skip(sizing: &mut PartitionSizing, sz: u64) {
    sizing.effective_sizes.push(sz);
    sizing.stream_sizes.push(sz);
    sizing.compact_sizes.push(None);
    sizing.minimum_sizes.push(None);
    sizing.defragmented_min_sizes.push(None);
    sizing.is_layout_preserving_flags.push(false);
}

/// Aggregated byte totals for progress reporting and smart-sizing logs.
pub(super) struct BackupTotals {
    /// Logical data bytes to image (excludes zero-fill in LP readers).
    pub total_display_bytes: u64,
    /// Bytes that flow through the compressor (includes LP zero-fill).
    pub total_stream_bytes: u64,
    /// Sum of full original partition sizes for included partitions.
    pub full_partition_bytes: u64,
}

/// True if a partition is included in this backup (filters out extended containers
/// and partitions excluded by an explicit `partition_filter`).
pub(super) fn should_include(p: &PartitionInfo, filter: Option<&[usize]>) -> bool {
    if p.is_extended_container {
        return false;
    }
    if let Some(filter) = filter {
        return filter.contains(&p.index);
    }
    true
}

pub(super) fn compute_totals(
    partitions: &[PartitionInfo],
    effective_sizes: &[u64],
    stream_sizes: &[u64],
    filter: Option<&[usize]>,
) -> BackupTotals {
    let total_display_bytes: u64 = partitions
        .iter()
        .zip(effective_sizes)
        .filter(|(p, _)| should_include(p, filter))
        .map(|(_, &sz)| sz)
        .sum();

    let total_stream_bytes: u64 = partitions
        .iter()
        .zip(stream_sizes)
        .filter(|(p, _)| should_include(p, filter))
        .map(|(_, &sz)| sz)
        .sum();

    let full_partition_bytes: u64 = partitions
        .iter()
        .filter(|p| should_include(p, filter))
        .map(|p| p.size_bytes)
        .sum();

    BackupTotals {
        total_display_bytes,
        total_stream_bytes,
        full_partition_bytes,
    }
}
