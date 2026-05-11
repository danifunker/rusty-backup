pub mod disk_image_stream;
pub mod format;
pub mod metadata;
pub mod single_file_chd;
mod sizes;
pub mod verify;

use std::collections::VecDeque;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{bail, Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::fs;
use crate::partition::{self, PartitionTable};
use crate::rbformats::chd_options::ChdOptions;
use metadata::{
    AlignmentMetadata, BackupLayout, BackupMetadata, ExtendedContainerMetadata, PartitionMetadata,
    SizePolicy,
};

/// Compression type for backup output.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum CompressionType {
    Chd,
    /// DVD-profile CHD (MAME 0.287+). On disk it's still a `.chd` file;
    /// the metadata tag distinguishes it. Hunk size + codecs default to
    /// chdman's DVD profile (4096-byte hunks, 2048-byte sectors).
    Dvd,
    Vhd,
    Zstd,
    None,
}

impl CompressionType {
    pub fn as_str(&self) -> &'static str {
        match self {
            CompressionType::Chd => "chd",
            CompressionType::Dvd => "chd-dvd",
            CompressionType::Vhd => "vhd",
            CompressionType::Zstd => "zstd",
            CompressionType::None => "none",
        }
    }

    pub fn file_extension(&self) -> &'static str {
        match self {
            CompressionType::Chd | CompressionType::Dvd => "chd",
            CompressionType::Vhd => "vhd",
            CompressionType::Zstd => "zst",
            CompressionType::None => "raw",
        }
    }
}

/// Checksum algorithm choice.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ChecksumType {
    Sha256,
    Crc32,
}

impl ChecksumType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ChecksumType::Sha256 => "sha256",
            ChecksumType::Crc32 => "crc32",
        }
    }
}

/// Configuration for a backup run.
#[derive(Debug, Clone)]
pub struct BackupConfig {
    pub source_path: PathBuf,
    pub destination_dir: PathBuf,
    pub backup_name: String,
    pub compression: CompressionType,
    pub checksum: ChecksumType,
    pub split_size_mib: Option<u32>,
    /// When true, copy every sector verbatim (including blank space).
    /// When false (default), skip all-zero blocks for smaller/faster output.
    pub sector_by_sector: bool,
    /// When set, only back up partitions whose index is in the list.
    /// The partition table is always exported in full regardless.
    pub partition_filter: Option<Vec<usize>>,
    /// CHD codec + hunk-size options (only consulted when `compression`
    /// is `Chd` or `Dvd`). `None` = use the profile's chdman defaults.
    pub chd_options: Option<ChdOptions>,
    /// Backup-time partition size policy (single-file CHD only). `None`
    /// keeps source sizes. The actual per-partition sizes the user picked
    /// live in `partition_target_sizes`; `size_policy` is the policy label
    /// recorded in metadata for traceability.
    pub size_policy: Option<SizePolicy>,
    /// Per-partition target sizes in bytes, keyed by partition index
    /// (single-file CHD only). Entries omitted from this map default to
    /// the partition's source size. Non-trivial entries trigger
    /// `single_file_chd::run`'s temp-file resize pipeline.
    pub partition_target_sizes: Option<Vec<(usize, u64)>>,
    /// When `true`, eligible HFS+/HFSX partitions are routed through the
    /// streamed defrag-clone path: the captured image is a fully repacked
    /// HFS+ volume sized at its `defragmented_min_size_bytes`. Other
    /// filesystems are unaffected. Ignored when [`BackupConfig::sector_by_sector`]
    /// is on. Pre-flight via [`fs::can_defrag_clone_hfsplus`] gates each
    /// partition; failure aborts the whole backup with the reason.
    pub shrink_to_minimum: bool,
}

/// Shared progress state between background backup thread and the GUI.
pub struct BackupProgress {
    pub current_bytes: u64,
    pub total_bytes: u64,
    /// Full untrimmed partition sizes — when larger than `total_bytes`,
    /// indicates smart trimming is saving space.
    pub full_size_bytes: u64,
    pub operation: String,
    pub finished: bool,
    pub error: Option<String>,
    pub cancel_requested: bool,
    pub log_messages: VecDeque<LogMessage>,
}

/// A log message from the backup thread.
pub struct LogMessage {
    pub level: LogLevel,
    pub message: String,
}

#[derive(Debug, Clone, Copy)]
pub enum LogLevel {
    Info,
    Warning,
    Error,
}

impl Default for BackupProgress {
    fn default() -> Self {
        Self::new()
    }
}

impl BackupProgress {
    pub fn new() -> Self {
        Self {
            current_bytes: 0,
            total_bytes: 0,
            full_size_bytes: 0,
            operation: String::new(),
            finished: false,
            error: None,
            cancel_requested: false,
            log_messages: VecDeque::new(),
        }
    }
}

fn log(progress: &Arc<Mutex<BackupProgress>>, level: LogLevel, message: impl Into<String>) {
    if let Ok(mut p) = progress.lock() {
        p.log_messages.push_back(LogMessage {
            level,
            message: message.into(),
        });
    }
}

fn set_operation(progress: &Arc<Mutex<BackupProgress>>, op: impl Into<String>) {
    if let Ok(mut p) = progress.lock() {
        p.operation = op.into();
    }
}

fn is_cancelled(progress: &Arc<Mutex<BackupProgress>>) -> bool {
    progress.lock().map(|p| p.cancel_requested).unwrap_or(false)
}

fn set_progress_bytes(progress: &Arc<Mutex<BackupProgress>>, current: u64, total: u64) {
    if let Ok(mut p) = progress.lock() {
        p.current_bytes = current;
        p.total_bytes = total;
    }
}

/// Main backup orchestrator. Runs on a background thread.
pub fn run_backup(config: BackupConfig, progress: Arc<Mutex<BackupProgress>>) -> Result<()> {
    log(
        &progress,
        LogLevel::Info,
        format!("Starting backup of {}", config.source_path.display()),
    );

    // Step 1: Open source and detect partition table
    set_operation(&progress, "Opening source device...");
    log(
        &progress,
        LogLevel::Info,
        "Requesting device access (you may be prompted for administrator credentials)...",
    );
    let elevated = crate::os::open_source_for_reading(&config.source_path)
        .with_context(|| format!("cannot open source: {}", config.source_path.display()))?;
    if let Some(tmp) = elevated.temp_path() {
        log(
            &progress,
            LogLevel::Info,
            format!("Created temporary device image: {}", tmp.display()),
        );
    }
    // Split into file + cleanup guard. The guard auto-deletes the temp file
    // (if any) when it goes out of scope at the end of this function.
    let (source_file, _temp_guard) = elevated.into_parts();

    set_operation(&progress, "Reading partition table...");
    let mut source = BufReader::new(source_file);

    // Read the first 512 bytes for MBR export
    let mut mbr_bytes = [0u8; 512];
    source
        .read_exact(&mut mbr_bytes)
        .context("cannot read first sector")?;
    source.seek(SeekFrom::Start(0))?;

    // Log first bytes for diagnostics
    log(
        &progress,
        LogLevel::Info,
        format!(
            "First 16 bytes: {:02X} {:02X} {:02X} {:02X} {:02X} {:02X} {:02X} {:02X} {:02X} {:02X} {:02X} {:02X} {:02X} {:02X} {:02X} {:02X}, bytes 510-511: {:02X} {:02X}",
            mbr_bytes[0], mbr_bytes[1], mbr_bytes[2], mbr_bytes[3],
            mbr_bytes[4], mbr_bytes[5], mbr_bytes[6], mbr_bytes[7],
            mbr_bytes[8], mbr_bytes[9], mbr_bytes[10], mbr_bytes[11],
            mbr_bytes[12], mbr_bytes[13], mbr_bytes[14], mbr_bytes[15],
            mbr_bytes[510], mbr_bytes[511],
        ),
    );

    let mut table =
        PartitionTable::detect(&mut source).context("failed to detect partition table")?;

    // Fix up superfloppy size: seek(End(0)) returns 0 for macOS device files,
    // so we need to use platform-specific ioctl to get the real device size.
    if let PartitionTable::None { size_bytes, .. } = &mut table {
        if *size_bytes == 0 {
            if let Ok(real_size) = crate::os::get_file_size(source.get_ref(), &config.source_path) {
                log(
                    &progress,
                    LogLevel::Info,
                    format!(
                        "Device size via seek was 0, using ioctl size: {} bytes",
                        real_size
                    ),
                );
                *size_bytes = real_size;
            }
        }
    }

    let alignment = partition::detect_alignment(&table);
    let mut partitions = table.partitions();
    let is_superfloppy = matches!(table, PartitionTable::None { .. });

    // For APM disks, probe "Apple_HFS" partitions to detect the actual HFS variant
    // (native HFS vs native HFS+ vs embedded HFS+) and update the type_name accordingly.
    if matches!(table, PartitionTable::Apm(_)) {
        for part in &mut partitions {
            if part.partition_type_string.as_deref() == Some("Apple_HFS") {
                let part_offset = part.start_lba * 512;
                if let Ok(clone) = source.get_ref().try_clone() {
                    let mut br = std::io::BufReader::new(clone);
                    let detected = fs::probe_apple_hfs_type(&mut br, part_offset);
                    if detected == "HFS+" || detected == "HFSX" {
                        part.type_name = part
                            .type_name
                            .replace("Apple_HFS", &format!("Apple_HFS ({detected})"));
                    }
                }
            }
        }
    }

    if is_superfloppy {
        log(
            &progress,
            LogLevel::Info,
            format!(
                "Detected superfloppy (no partition table) with {} partition(s)",
                partitions.len(),
            ),
        );
        for p in &partitions {
            log(
                &progress,
                LogLevel::Info,
                format!(
                    "  Partition {}: type_byte=0x{:02X} type_name={} start_lba={} size={}",
                    p.index, p.partition_type_byte, p.type_name, p.start_lba, p.size_bytes,
                ),
            );
        }
    } else {
        log(
            &progress,
            LogLevel::Info,
            format!(
                "Detected {} partition table with {} partition(s), alignment: {}",
                table.type_name(),
                partitions.len(),
                alignment.alignment_type
            ),
        );
        for p in &partitions {
            log(
                &progress,
                LogLevel::Info,
                format!(
                    "  Partition {}: type_byte=0x{:02X} type_name={} start_lba={} size={}",
                    p.index, p.partition_type_byte, p.type_name, p.start_lba, p.size_bytes,
                ),
            );
        }
    }

    if is_cancelled(&progress) {
        bail!("backup cancelled");
    }

    // Get source size
    log(&progress, LogLevel::Info, "Getting source device size...");
    let source_size = crate::os::get_file_size(source.get_ref(), &config.source_path)
        .context("failed to get source size")?;
    log(
        &progress,
        LogLevel::Info,
        format!("Source size: {} bytes", source_size),
    );

    // Step 2: Create backup folder
    set_operation(&progress, "Creating backup folder...");
    let backup_folder = format::create_backup_folder(&config.destination_dir, &config.backup_name)?;
    log(
        &progress,
        LogLevel::Info,
        format!("Backup folder: {}", backup_folder.display()),
    );

    // Step 3: Export partition table
    //
    // Single-file CHD backups already carry the raw partition-table sector
    // inside `disk.chd` at offset 0 — writing a separate `mbr.bin` would
    // duplicate those bytes. We still emit the parsed JSON sidecar
    // (`mbr.json`) for fast inspect loads.
    let single_file_chd_planned = matches!(
        config.compression,
        CompressionType::Chd | CompressionType::Dvd
    ) && !is_superfloppy
        && config.split_size_mib.is_none()
        && single_file_chd::is_supported(&table);

    set_operation(&progress, "Exporting partition table...");
    match &table {
        PartitionTable::Mbr(mbr) => {
            if single_file_chd_planned {
                format::export_mbr_json(mbr, &backup_folder)?;
                log(
                    &progress,
                    LogLevel::Info,
                    "Exported MBR (mbr.json only — raw bytes live in disk.chd)",
                );
            } else {
                format::export_mbr(mbr, &mbr_bytes, &backup_folder)?;
                log(
                    &progress,
                    LogLevel::Info,
                    "Exported MBR (mbr.bin + mbr.json)",
                );
            }
        }
        PartitionTable::Gpt { gpt, .. } => {
            if single_file_chd_planned {
                // Raw GPT sectors live inside disk.chd; only emit the JSON
                // sidecar for fast inspect.
                let json =
                    serde_json::to_string_pretty(gpt).context("failed to serialize GPT to JSON")?;
                std::fs::write(backup_folder.join("gpt.json"), json)
                    .context("failed to write gpt.json")?;
                log(
                    &progress,
                    LogLevel::Info,
                    "Exported GPT (gpt.json only — raw sectors live in disk.chd)",
                );
            } else {
                format::export_gpt(gpt, &mbr_bytes, &backup_folder)?;
                log(
                    &progress,
                    LogLevel::Info,
                    "Exported GPT (gpt.json + mbr.bin)",
                );
                if let Err(e) = format::export_gpt_bin(&mut source, &backup_folder) {
                    log(
                        &progress,
                        LogLevel::Warning,
                        format!("Failed to export gpt.bin: {e}"),
                    );
                } else {
                    log(
                        &progress,
                        LogLevel::Info,
                        "Exported raw GPT sectors (gpt.bin)",
                    );
                }
            }
        }
        PartitionTable::Apm(apm) => {
            if single_file_chd_planned {
                // apm.bin would duplicate the DDR + partition map already
                // embedded in disk.chd; emit only the JSON sidecar.
                let json =
                    serde_json::to_string_pretty(apm).context("failed to serialize APM to JSON")?;
                std::fs::write(backup_folder.join("apm.json"), json)
                    .context("failed to write apm.json")?;
                log(
                    &progress,
                    LogLevel::Info,
                    "Exported APM (apm.json only — raw blocks live in disk.chd)",
                );
            } else {
                format::export_apm(apm, &backup_folder)?;
                log(
                    &progress,
                    LogLevel::Info,
                    "Exported APM (apm.json + apm.bin)",
                );
            }
        }
        PartitionTable::None { fs_hint, .. } => {
            log(
                &progress,
                LogLevel::Info,
                format!("No partition table (superfloppy, filesystem: {fs_hint})"),
            );
        }
    }

    if is_cancelled(&progress) {
        bail!("backup cancelled");
    }

    // Step 4: Analyze partitions for smart sizing
    //
    // When not in sector-by-sector mode, try filesystem-aware compaction first
    // (FAT only — packs allocated clusters contiguously for a smaller, defragmented
    // image). If compaction isn't possible, fall back to trim-based sizing that
    // skips unused space beyond the last allocated cluster.
    //
    // This runs before the single-file-CHD branch so the CHD path can persist
    // `minimum_size_bytes` / `defragmented_min_size_bytes` in metadata.json,
    // which the restore-resize path uses to validate Custom shrinks and to
    // populate the "Minimum" picker.
    log(
        &progress,
        LogLevel::Info,
        format!(
            "Analyzing {} partitions for smart sizing...",
            partitions.len()
        ),
    );

    let sizes::PartitionSizing {
        mut effective_sizes,
        mut stream_sizes,
        compact_sizes,
        minimum_sizes,
        defragmented_min_sizes,
        mut is_layout_preserving_flags,
    } = sizes::analyze_partitions(
        source.get_ref(),
        &partitions,
        config.sector_by_sector,
        is_superfloppy,
        &progress,
    );

    // Phase-8 / Step-22f: shrink-to-minimum pre-flight + sizing override.
    //
    // For each clone-eligible HFS+/HFSX partition, run
    // [`fs::can_defrag_clone_hfsplus`] before any bytes flow. On failure abort
    // the whole backup with the human-readable reason — the user can either
    // disable shrink-to-minimum or remediate the source (clean unmount, flatten
    // the wrapper) and retry.
    //
    // Successful pre-flights cause the per-partition loop to stream the
    // partition through `stream_defragmented_hfsplus` instead of the compact
    // reader; size vectors are overridden so progress accounting and the smart-
    // sizing log line reflect the post-clone footprint.
    let mut clone_target_sizes: Vec<Option<u64>> = vec![None; partitions.len()];
    if config.shrink_to_minimum && !config.sector_by_sector && !single_file_chd_planned {
        for (i, part) in partitions.iter().enumerate() {
            if part.is_extended_container || part.partition_type_byte == 0xEE {
                continue;
            }
            if let Some(ref filter) = config.partition_filter {
                if !filter.contains(&part.index) {
                    continue;
                }
            }
            // Only HFS+/HFSX partitions participate. Skip silently otherwise —
            // the flag is global; non-HFS+ partitions just keep their default
            // compaction path.
            let probe = source
                .get_ref()
                .try_clone()
                .ok()
                .map(|c| {
                    let mut br = BufReader::new(c);
                    fs::probe_hfsplus_signature(&mut br, part.start_lba * 512)
                })
                .unwrap_or(None);
            if probe.is_none() {
                continue;
            }
            let target = match defragmented_min_sizes[i] {
                Some(t) if t > 0 && t < part.size_bytes => t,
                _ => {
                    log(
                        &progress,
                        LogLevel::Info,
                        format!(
                            "partition-{}: shrink-to-minimum skipped — no defragmented \
                             minimum size available",
                            part.index
                        ),
                    );
                    continue;
                }
            };
            // Pre-flight on a fresh reader.
            let pf = source
                .get_ref()
                .try_clone()
                .map_err(|e| anyhow::anyhow!("clone source for shrink-preflight: {e}"))
                .and_then(|c| {
                    let mut br = BufReader::new(c);
                    fs::can_defrag_clone_hfsplus(&mut br, part.start_lba * 512)
                        .map_err(|reason| anyhow::anyhow!("{reason}"))
                });
            match pf {
                Ok(()) => {
                    log(
                        &progress,
                        LogLevel::Info,
                        format!(
                            "partition-{}: streaming defragmented HFS+ — \
                             source {} -> {}",
                            part.index,
                            partition::format_size(part.size_bytes),
                            partition::format_size(target),
                        ),
                    );
                    clone_target_sizes[i] = Some(target);
                    // Override sizing: the streamed image flows through the
                    // compressor verbatim at `target` bytes. `compact_sizes`
                    // is left as whatever `analyze_partitions` produced — the
                    // per-partition loop dispatches on `clone_target_sizes`
                    // first, so neither the compacted nor the trim branch
                    // runs for cloned partitions.
                    effective_sizes[i] = target;
                    stream_sizes[i] = target;
                    is_layout_preserving_flags[i] = false;
                }
                Err(e) => {
                    bail!("Aborting backup: partition-{}: {e}", part.index);
                }
            }
        }
    } else if config.shrink_to_minimum && single_file_chd_planned {
        log(
            &progress,
            LogLevel::Warning,
            "shrink-to-minimum is not supported with single-file CHD output; \
             ignoring the flag.",
        );
    } else if config.shrink_to_minimum && config.sector_by_sector {
        log(
            &progress,
            LogLevel::Warning,
            "shrink-to-minimum is incompatible with sector-by-sector mode; \
             ignoring the flag.",
        );
    }

    // Single-file CHD branch: when the user asked for CHD output and the
    // source has a partition table single_file_chd can handle, synthesise a
    // whole-disk image into one CHD and skip the per-partition loop. Falls
    // through to the legacy per-partition path otherwise.
    if single_file_chd_planned {
        return run_single_file_chd_path(
            &config,
            &progress,
            &source,
            source_size,
            &mbr_bytes,
            &table,
            &partitions,
            &alignment,
            &backup_folder,
            &minimum_sizes,
            &defragmented_min_sizes,
        );
    }
    // CHD/DVD selected on a source single_file_chd can't handle (only
    // superfloppies fit this today — every other shape is_supported).
    // Superfloppies route through the per-partition loop with
    // `effective_compression` forced to `None` (raw .img), so the user
    // ends up with a `partition-0.img` rather than a CHD. We don't emit
    // per-partition CHDs anywhere; CHD output is single-file or nothing.
    if matches!(
        config.compression,
        CompressionType::Chd | CompressionType::Dvd
    ) && config.split_size_mib.is_some()
    {
        log(
            &progress,
            LogLevel::Warning,
            "Split-size set with CHD output: splitting is incompatible with \
             chdman/MAME single-file CHDs and is ignored.",
        );
    }

    let split_bytes = config.split_size_mib.map(|mib| mib as u64 * 1024 * 1024);
    let mut partition_metadata = Vec::new();
    let mut overall_bytes_done: u64 = 0;

    // Export mbr-min.bin for MBR tables: a copy of the MBR with each
    // partition's total_sectors reduced to the effective (imaged) size.
    if let PartitionTable::Mbr(_) = &table {
        let mut min_sectors: Vec<(usize, u32)> = Vec::new();
        for (i, part) in partitions.iter().enumerate() {
            if part.is_logical {
                continue; // logical partitions live in EBRs, not the MBR
            }
            if part.is_extended_container {
                // Sum effective sizes of all logical partitions inside this container
                let logical_sum: u64 = partitions
                    .iter()
                    .enumerate()
                    .filter(|(_, p)| p.is_logical)
                    .map(|(j, _)| effective_sizes[j])
                    .sum();
                let new_sectors = (logical_sum / 512) as u32;
                min_sectors.push((part.index, new_sectors));
            } else {
                let new_sectors = (effective_sizes[i] / 512) as u32;
                min_sectors.push((part.index, new_sectors));
            }
        }
        if let Err(e) = format::export_mbr_min(&mbr_bytes, &min_sectors, &backup_folder) {
            log(
                &progress,
                LogLevel::Warning,
                format!("Failed to export mbr-min.bin: {e}"),
            );
        } else {
            log(
                &progress,
                LogLevel::Info,
                "Exported minimum-size MBR (mbr-min.bin)",
            );
        }
    }

    let sizes::BackupTotals {
        total_display_bytes,
        total_stream_bytes,
        full_partition_bytes,
    } = sizes::compute_totals(
        &partitions,
        &effective_sizes,
        &stream_sizes,
        config.partition_filter.as_deref(),
    );

    if let Ok(mut p) = progress.lock() {
        p.total_bytes = total_stream_bytes;
        p.full_size_bytes = full_partition_bytes;
        p.current_bytes = 0;
    }

    if full_partition_bytes > total_display_bytes {
        log(
            &progress,
            LogLevel::Info,
            format!(
                "Smart sizing: imaging {} of {} total (saving {})",
                partition::format_size(total_display_bytes),
                partition::format_size(full_partition_bytes),
                partition::format_size(full_partition_bytes - total_display_bytes),
            ),
        );
    }

    for (part_idx, part) in partitions.iter().enumerate() {
        if is_cancelled(&progress) {
            bail!("backup cancelled");
        }

        // Skip GPT protective partitions (0xEE) — not a real partition.
        if part.partition_type_byte == 0xEE {
            continue;
        }

        // Skip partitions not in the filter (if a filter is set)
        if let Some(ref filter) = config.partition_filter {
            if !filter.contains(&part.index) {
                log(
                    &progress,
                    LogLevel::Info,
                    format!(
                        "Skipping partition-{} (not selected for backup)",
                        part.index
                    ),
                );
                continue;
            }
        }

        log(
            &progress,
            LogLevel::Info,
            format!(
                "Processing partition index {} (partition-{})",
                part_idx, part.index
            ),
        );

        if part.is_extended_container {
            log(
                &progress,
                LogLevel::Info,
                format!(
                    "Skipping extended container partition-{} (logical partitions backed up individually)",
                    part.index
                ),
            );
            continue;
        }

        // image_size: logical data bytes (allocated blocks × block_size) for this partition.
        //   Stored in imaged_size_bytes and used for non-compacted source.take() limit.
        let image_size = effective_sizes[part_idx];
        // stream_size: actual bytes that will flow through the compressor.
        //   For LP readers this is the minimum (trimmed); equals image_size.
        let stream_size = stream_sizes[part_idx];
        let is_compacted = compact_sizes[part_idx].is_some();
        // is_layout_preserving: the compact reader emits free blocks as zeros.
        //   Tracked separately since after trimming stream_size == image_size for all modes.
        let is_layout_preserving = is_layout_preserving_flags[part_idx];

        let part_label = format!("partition-{}", part.index);
        set_operation(
            &progress,
            format!(
                "Backing up {} ({})...",
                part_label,
                partition::format_size(image_size)
            ),
        );

        if is_compacted {
            if is_layout_preserving {
                log(
                    &progress,
                    LogLevel::Info,
                    format!(
                        "Compacting {}: {} at LBA {}, trimmed {} -> {} (free blocks -> zeros)",
                        part_label,
                        part.type_name,
                        part.start_lba,
                        partition::format_size(part.size_bytes),
                        partition::format_size(stream_size),
                    ),
                );
            } else {
                log(
                    &progress,
                    LogLevel::Info,
                    format!(
                        "Compacting {}: {} at LBA {}, {} -> {} (defragmented)",
                        part_label,
                        part.type_name,
                        part.start_lba,
                        partition::format_size(part.size_bytes),
                        partition::format_size(stream_size),
                    ),
                );
            }
        } else if image_size < part.size_bytes {
            log(
                &progress,
                LogLevel::Info,
                format!(
                    "Processing {}: {} at LBA {}, trimmed from {} to {}",
                    part_label,
                    part.type_name,
                    part.start_lba,
                    partition::format_size(part.size_bytes),
                    partition::format_size(image_size),
                ),
            );
        } else {
            log(
                &progress,
                LogLevel::Info,
                format!(
                    "Processing {}: {} at LBA {}, size {}",
                    part_label,
                    part.type_name,
                    part.start_lba,
                    partition::format_size(part.size_bytes)
                ),
            );
        }

        let output_base = backup_folder.join(&part_label);
        let progress_clone = Arc::clone(&progress);
        let base_bytes = overall_bytes_done;

        // Superfloppy: force raw compression to produce a universally compatible .img file
        let effective_compression = if is_superfloppy {
            CompressionType::None
        } else {
            config.compression
        };

        log(
            &progress,
            LogLevel::Info,
            format!(
                "Starting compression for {}, is_compacted={}",
                part_label, is_compacted
            ),
        );

        let clone_target = clone_target_sizes[part_idx];
        let compressed_files = if let Some(target_size) = clone_target {
            // Phase-8 / Step-22f: stream a defragmented HFS+ clone directly
            // into the compressor via a bounded channel pipe. The producer
            // thread owns its own `HfsPlusFilesystem` over a cloned source
            // file handle; the consumer (this thread) is the compressor.
            // Errors from either side propagate through the join.
            log(
                &progress,
                LogLevel::Info,
                format!(
                    "Defrag-cloning {} into {}",
                    part_label,
                    partition::format_size(target_size)
                ),
            );
            let part_offset = part.start_lba * 512;
            let producer_clone = source
                .get_ref()
                .try_clone()
                .context("failed to clone source for defrag-clone producer")?;
            let (mut writer, mut reader) = channel_pipe();
            let progress_log_prod = Arc::clone(&progress);
            let producer =
                std::thread::spawn(move || -> Result<fs::hfsplus_defrag::DefragReport> {
                    let br = BufReader::new(producer_clone);
                    let mut hfs = fs::hfsplus::HfsPlusFilesystem::open(br, part_offset)
                        .context("open source HFS+ for defrag-clone")?;
                    let report = fs::hfsplus_defrag::stream_defragmented_hfsplus(
                        &mut hfs,
                        target_size,
                        &mut writer,
                    )
                    .context("stream_defragmented_hfsplus")?;
                    log(
                        &progress_log_prod,
                        LogLevel::Info,
                        format!(
                            "Defrag emit: {} files / {} folders / {} data / {} hardlinks / \
                             {} xattrs",
                            report.files_copied,
                            report.dirs_copied,
                            partition::format_size(report.data_bytes_copied),
                            report.hardlinks_copied + report.dir_hardlinks_copied,
                            report.xattrs_copied,
                        ),
                    );
                    Ok(report)
                });
            let progress_log = Arc::clone(&progress);
            let compress_result = crate::rbformats::compress_partition(
                &mut reader,
                &output_base,
                effective_compression,
                target_size,
                split_bytes,
                false, // streamed image is already packed; don't skip zeros
                config.chd_options.clone(),
                |bytes_read| {
                    set_progress_bytes(
                        &progress_clone,
                        base_bytes + bytes_read,
                        total_stream_bytes,
                    );
                },
                || is_cancelled(&progress_clone),
                |msg| log(&progress_log, LogLevel::Info, msg),
            );
            // Always join the producer so its errors surface even if the
            // consumer succeeded (e.g. partial pipe).
            let producer_result = producer
                .join()
                .map_err(|_| anyhow::anyhow!("defrag-clone producer thread panicked"))?;
            let files = compress_result
                .with_context(|| format!("failed to compress defrag-cloned {part_label}"))?;
            producer_result.with_context(|| format!("defrag-clone failed for {part_label}"))?;
            files
        } else if is_compacted {
            // Use compacted reader — create a fresh compact reader for this filesystem
            log(
                &progress,
                LogLevel::Info,
                format!("Creating compact reader for {}", part_label),
            );
            let part_offset = part.start_lba * 512;
            let clone = source
                .get_ref()
                .try_clone()
                .context("failed to clone source for compaction")?;
            let (compact_reader, _) = fs::compact_partition_reader(
                BufReader::new(clone),
                part_offset,
                part.partition_type_byte,
                part.partition_type_string.as_deref(),
            )
            .ok_or_else(|| anyhow::anyhow!("compaction failed for {part_label}"))?;

            // Trim the stream to stream_size.  For layout-preserving readers this
            // drops the zero-filled free tail; for packed readers stream_size equals
            // the natural end of the stream so take() is a no-op.
            let mut limited = compact_reader.take(stream_size);

            log(
                &progress,
                LogLevel::Info,
                format!("Calling compress_partition for compacted {}", part_label),
            );
            let progress_log = Arc::clone(&progress);
            crate::rbformats::compress_partition(
                &mut limited,
                &output_base,
                effective_compression,
                stream_size,
                split_bytes,
                false, // compacted image has no wasted space, don't skip zeros
                config.chd_options.clone(),
                |bytes_read| {
                    set_progress_bytes(
                        &progress_clone,
                        base_bytes + bytes_read,
                        total_stream_bytes,
                    );
                },
                || is_cancelled(&progress_clone),
                |msg| log(&progress_log, LogLevel::Info, msg),
            )
            .with_context(|| format!("failed to compress {part_label}"))?
        } else {
            // Fall back to trim-based read
            log(
                &progress,
                LogLevel::Info,
                format!("Using trim-based read for {}", part_label),
            );
            let part_offset = part.start_lba * 512;
            log(
                &progress,
                LogLevel::Info,
                format!("Seeking to offset {} for {}", part_offset, part_label),
            );
            source.seek(SeekFrom::Start(part_offset))?;
            log(
                &progress,
                LogLevel::Info,
                format!("Creating limited reader, image_size={}", image_size),
            );
            let part_reader = (&mut source).take(image_size);
            let mut limited = LimitedReader::new(part_reader);

            log(
                &progress,
                LogLevel::Info,
                format!("Calling compress_partition for trim-based {}", part_label),
            );
            let progress_log = Arc::clone(&progress);
            crate::rbformats::compress_partition(
                &mut limited,
                &output_base,
                effective_compression,
                image_size,
                split_bytes,
                !config.sector_by_sector,
                config.chd_options.clone(),
                |bytes_read| {
                    set_progress_bytes(
                        &progress_clone,
                        base_bytes + bytes_read,
                        total_stream_bytes,
                    );
                },
                || is_cancelled(&progress_clone),
                |msg| log(&progress_log, LogLevel::Info, msg),
            )
            .with_context(|| format!("failed to compress {part_label}"))?
        };

        // Log output file sizes for diagnostics
        for file_name in &compressed_files {
            let file_path = backup_folder.join(file_name);
            match std::fs::metadata(&file_path) {
                Ok(meta) => log(
                    &progress,
                    LogLevel::Info,
                    format!("Output file {} size: {} bytes", file_name, meta.len()),
                ),
                Err(e) => log(
                    &progress,
                    LogLevel::Warning,
                    format!("Cannot stat output file {}: {}", file_name, e),
                ),
            }
        }

        overall_bytes_done += stream_size;
        set_progress_bytes(&progress, overall_bytes_done, total_stream_bytes);

        // Superfloppy: rename .raw to .img for universally compatible raw image
        let compressed_files = if is_superfloppy {
            let mut renamed = Vec::new();
            for file_name in &compressed_files {
                let new_name = file_name.replace(".raw", ".img");
                let old_path = backup_folder.join(file_name);
                let new_path = backup_folder.join(&new_name);
                if old_path != new_path {
                    std::fs::rename(&old_path, &new_path)
                        .with_context(|| format!("failed to rename {file_name} to {new_name}"))?;
                }
                renamed.push(new_name);
            }
            renamed
        } else {
            compressed_files
        };

        // Compute checksums for each output file
        let mut all_checksums = Vec::new();
        for file_name in &compressed_files {
            let file_path = backup_folder.join(file_name);
            let checksum = verify::compute_checksum(&file_path, config.checksum)
                .with_context(|| format!("failed to checksum {file_name}"))?;
            verify::write_checksum_file(&checksum, &file_path, config.checksum)
                .with_context(|| format!("failed to write checksum for {file_name}"))?;
            all_checksums.push(checksum);
        }

        let combined_checksum = if all_checksums.len() == 1 {
            all_checksums[0].clone()
        } else {
            all_checksums.join(",")
        };

        log(
            &progress,
            LogLevel::Info,
            format!(
                "{}: {} file(s), checksum: {}",
                part_label,
                compressed_files.len(),
                &combined_checksum[..combined_checksum.len().min(16)]
            ),
        );

        let minimum_size = minimum_sizes[part_idx].filter(|&s| s < part.size_bytes);
        let defragmented_min = defragmented_min_sizes[part_idx]
            .filter(|&s| s < part.size_bytes)
            .filter(|&d| match minimum_size {
                Some(m) => d < m,
                None => true,
            });
        // Probe the HFS+/HFSX signature for HFS+-shaped partitions so
        // restore can warn on case-sensitivity mismatches (Step 20).
        // `probe_hfsplus_signature` returns None for non-HFS+ volumes.
        let hfsplus_signature = source.get_ref().try_clone().ok().and_then(|c| {
            let mut br = BufReader::new(c);
            fs::probe_hfsplus_signature(&mut br, part.start_lba * 512)
        });
        partition_metadata.push(PartitionMetadata {
            index: part.index,
            type_name: part.type_name.clone(),
            partition_type_byte: part.partition_type_byte,
            start_lba: part.start_lba,
            original_size_bytes: part.size_bytes,
            imaged_size_bytes: image_size,
            compressed_files,
            checksum: combined_checksum,
            resized: false,
            compacted: is_compacted,
            is_logical: part.is_logical,
            partition_type_string: part.partition_type_string.clone(),
            minimum_size_bytes: minimum_size,
            defragmented_min_size_bytes: defragmented_min,
            hfsplus_signature,
            defragmented_clone: clone_target.is_some(),
        });
    }

    if is_cancelled(&progress) {
        bail!("backup cancelled");
    }

    // Capture extended container info for MBR tables
    let extended_container = partitions
        .iter()
        .find(|p| p.is_extended_container)
        .map(|p| ExtendedContainerMetadata {
            mbr_index: p.index,
            partition_type_byte: p.partition_type_byte,
            start_lba: p.start_lba,
            size_bytes: p.size_bytes,
        });

    // Step 5: Write metadata.json
    set_operation(&progress, "Writing metadata...");
    let metadata = BackupMetadata {
        version: 1,
        created: Utc::now().to_rfc3339(),
        source_device: config.source_path.display().to_string(),
        source_size_bytes: source_size,
        partition_table_type: table.type_name().to_string(),
        checksum_type: config.checksum.as_str().to_string(),
        compression_type: if is_superfloppy {
            CompressionType::None.as_str().to_string()
        } else {
            config.compression.as_str().to_string()
        },
        split_size_mib: config.split_size_mib,
        sector_by_sector: config.sector_by_sector,
        layout: BackupLayout::PerPartition,
        container: None,
        container_logical_size: None,
        container_sha1: None,
        size_policy: config.size_policy,
        alignment: AlignmentMetadata {
            detected_type: format!("{}", alignment.alignment_type),
            first_partition_lba: alignment.first_lba,
            alignment_sectors: alignment.alignment_sectors,
            heads: alignment.heads,
            sectors_per_track: alignment.sectors_per_track,
        },
        partitions: partition_metadata,
        bad_sectors: vec![],
        extended_container,
    };

    let metadata_path = backup_folder.join("metadata.json");
    let metadata_file = File::create(&metadata_path)
        .with_context(|| format!("failed to create {}", metadata_path.display()))?;
    serde_json::to_writer_pretty(metadata_file, &metadata)
        .context("failed to write metadata.json")?;

    log(
        &progress,
        LogLevel::Info,
        format!("Backup complete: {}", backup_folder.display()),
    );

    // Step 6: Mark finished
    if let Ok(mut p) = progress.lock() {
        p.finished = true;
        p.operation = "Backup complete".to_string();
    }

    Ok(())
}

/// Single-file CHD backup path. Synthesises a whole-disk image into one
/// CHD container (chdman/MAME compatible) and writes a `single-file-chd`
/// `metadata.json` describing the byte ranges inside it.
///
/// When the user picks a non-Original `size_policy`,
/// `single_file_chd::run` takes the temp-file resize pipeline (Stage 4b,
/// Approach A); otherwise the cheaper `DiskImageStream` path produces a
/// CHD whose layout matches the source disk byte-for-byte.
#[allow(clippy::too_many_arguments)] // intentional — pulls run_backup's locals
fn run_single_file_chd_path(
    config: &BackupConfig,
    progress: &Arc<Mutex<BackupProgress>>,
    source: &BufReader<File>,
    source_size: u64,
    mbr_bytes: &[u8; 512],
    table: &PartitionTable,
    partitions: &[partition::PartitionInfo],
    alignment: &partition::PartitionAlignment,
    backup_folder: &std::path::Path,
    minimum_sizes: &[Option<u64>],
    defragmented_min_sizes: &[Option<u64>],
) -> Result<()> {
    set_operation(progress, "Building disk-image stream...");
    log(
        progress,
        LogLevel::Info,
        format!(
            "Single-file CHD layout selected ({} disk, {} bytes)",
            table.type_name(),
            source_size,
        ),
    );

    let requested_policy = config.size_policy.unwrap_or(SizePolicy::Original);

    // Output base for compress_chd (the .chd extension is appended by the
    // compressor). Use the backup folder's name so the resulting file is
    // `<backup_name>/<backup_name>.chd`. Falls back to the literal `disk`
    // when the path has no final component (shouldn't happen in practice
    // — backup_folder is always created with a name).
    let output_stem = backup_folder
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("disk");
    let output_base = backup_folder.join(output_stem);

    let inputs = single_file_chd::SingleFileChdInputs {
        source_file: source.get_ref(),
        source_size,
        source_partition_table_bytes: mbr_bytes,
        partition_table: table,
        partitions,
        partition_filter: config.partition_filter.as_deref(),
        sector_by_sector: config.sector_by_sector,
        chd_options: config.chd_options.clone(),
        is_dvd: matches!(config.compression, CompressionType::Dvd),
        output_base: &output_base,
        resize_targets: config.partition_target_sizes.as_deref(),
        alignment_sectors: alignment.alignment_sectors,
        checksum_type: config.checksum,
    };

    {
        let mut p = progress.lock().expect("backup progress mutex poisoned");
        p.total_bytes = source_size;
        p.full_size_bytes = source_size;
        p.current_bytes = 0;
    }

    let progress_for_cb = progress.clone();
    let progress_for_cancel = progress.clone();
    let progress_for_log = progress.clone();
    let progress_for_checksum = progress.clone();
    let mut progress_cb = move |n: u64| {
        if let Ok(mut p) = progress_for_cb.lock() {
            p.current_bytes = n;
        }
    };
    let cancel_check = move || {
        progress_for_cancel
            .lock()
            .map(|p| p.cancel_requested)
            .unwrap_or(false)
    };
    let mut log_cb = move |s: &str| {
        if let Ok(mut p) = progress_for_log.lock() {
            p.log_messages.push_back(LogMessage {
                level: LogLevel::Info,
                message: s.to_string(),
            });
        }
    };
    // Post-write checksum phase: swap the progress bar over to the
    // hashing total so the GUI doesn't sit at "100% / 100%" while we
    // re-read the CHD. The first invocation (current=0, total=N) flips
    // the operation label too.
    let mut first_checksum_tick = true;
    let progress_for_checksum_outer = progress_for_checksum;
    let mut checksum_phase_cb = move |current: u64, total: u64| {
        if let Ok(mut p) = progress_for_checksum_outer.lock() {
            if first_checksum_tick {
                p.operation = "Computing per-partition checksums...".to_string();
                first_checksum_tick = false;
            }
            p.current_bytes = current;
            p.total_bytes = total;
        }
    };

    let result = single_file_chd::run(
        inputs,
        &mut progress_cb,
        &cancel_check,
        &mut log_cb,
        &mut checksum_phase_cb,
    )?;

    // Build per-partition metadata. The byte range inside the CHD lives in
    // `result.partition_ranges`; we cross-reference each partition's
    // PartitionInfo for the type-name + alignment data.
    set_operation(progress, "Writing metadata...");
    let partition_metadata: Vec<PartitionMetadata> = result
        .partition_ranges
        .iter()
        .filter_map(|range| {
            let (part_idx, part) = partitions
                .iter()
                .enumerate()
                .find(|(_, p)| p.index == range.partition_index)?;
            // `start_lba` records where the partition lives inside the CHD's
            // synthesised disk image — that's the new offset when the
            // backup-time resize plan moved it, otherwise the source LBA.
            let new_start_lba = range.offset_in_disk / 512;
            // Pull the per-partition minimum sizes computed by
            // `analyze_partitions` upstream. Restore-resize uses these to
            // populate the "Minimum" picker and to validate Custom shrinks.
            // After a backup-time resize the partition has already been
            // shrunk to ~minimum, so the stored minimum should not exceed
            // the new imaged size.
            let minimum_size = minimum_sizes
                .get(part_idx)
                .copied()
                .flatten()
                .map(|m| m.min(range.length));
            let defragmented_min = defragmented_min_sizes
                .get(part_idx)
                .copied()
                .flatten()
                .map(|m| m.min(range.length));
            let hfsplus_signature = source.get_ref().try_clone().ok().and_then(|c| {
                let mut br = BufReader::new(c);
                fs::probe_hfsplus_signature(&mut br, part.start_lba * 512)
            });
            Some(PartitionMetadata {
                index: range.partition_index,
                type_name: part.type_name.clone(),
                partition_type_byte: part.partition_type_byte,
                start_lba: new_start_lba,
                original_size_bytes: part.size_bytes,
                imaged_size_bytes: range.length,
                compressed_files: vec![], // data is inside the container
                checksum: range.checksum.clone(),
                resized: range.length != part.size_bytes || new_start_lba != part.start_lba,
                compacted: !config.sector_by_sector,
                is_logical: part.is_logical,
                partition_type_string: part.partition_type_string.clone(),
                minimum_size_bytes: minimum_size,
                defragmented_min_size_bytes: defragmented_min,
                hfsplus_signature,
                defragmented_clone: false,
            })
        })
        .collect();

    let metadata = BackupMetadata {
        version: 1,
        created: Utc::now().to_rfc3339(),
        source_device: config.source_path.display().to_string(),
        source_size_bytes: source_size,
        partition_table_type: table.type_name().to_string(),
        checksum_type: config.checksum.as_str().to_string(),
        compression_type: config.compression.as_str().to_string(),
        split_size_mib: None, // splitting is rejected on this path
        sector_by_sector: config.sector_by_sector,
        layout: BackupLayout::SingleFileChd,
        container: Some(result.container_filename.clone()),
        container_logical_size: Some(result.container_logical_size),
        container_sha1: Some(result.container_sha1.clone()),
        size_policy: Some(requested_policy),
        alignment: AlignmentMetadata {
            detected_type: format!("{}", alignment.alignment_type),
            first_partition_lba: alignment.first_lba,
            alignment_sectors: alignment.alignment_sectors,
            heads: alignment.heads,
            sectors_per_track: alignment.sectors_per_track,
        },
        partitions: partition_metadata,
        bad_sectors: vec![],
        extended_container: None,
    };

    let metadata_path = backup_folder.join("metadata.json");
    let metadata_file = File::create(&metadata_path)
        .with_context(|| format!("failed to create {}", metadata_path.display()))?;
    serde_json::to_writer_pretty(metadata_file, &metadata)
        .context("failed to write metadata.json")?;

    log(
        progress,
        LogLevel::Info,
        format!(
            "Single-file CHD backup complete: {} ({}, SHA1 {})",
            backup_folder.display(),
            result.container_filename,
            result.container_sha1,
        ),
    );

    if let Ok(mut p) = progress.lock() {
        p.finished = true;
        p.operation = "Backup complete".to_string();
    }

    Ok(())
}

/// A reader wrapper that limits reads to exactly `limit` bytes.
struct LimitedReader<R> {
    inner: R,
}

impl<R: Read> LimitedReader<R> {
    fn new(inner: R) -> Self {
        Self { inner }
    }
}

impl<R: Read> Read for LimitedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

/// Bounded in-memory pipe used to feed a `Write`-driven producer (e.g.
/// [`fs::stream_defragmented_hfsplus`]) into a `Read`-consuming sink (e.g.
/// `compress_partition`). Each `write` call enqueues one owned chunk; the
/// reader serves bytes from a single pending chunk before pulling the next.
/// `sync_channel(capacity=4)` bounds peak memory to a few hundred KiB at
/// the chunk sizes the defrag emitter produces, with the producer blocked
/// on backpressure when the consumer falls behind.
pub(crate) fn channel_pipe() -> (ChannelPipeWriter, ChannelPipeReader) {
    let (tx, rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(4);
    (
        ChannelPipeWriter { tx: Some(tx) },
        ChannelPipeReader {
            rx,
            chunk: Vec::new(),
            pos: 0,
        },
    )
}

pub(crate) struct ChannelPipeWriter {
    tx: Option<std::sync::mpsc::SyncSender<Vec<u8>>>,
}

impl std::io::Write for ChannelPipeWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let tx = self.tx.as_ref().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::BrokenPipe, "writer already closed")
        })?;
        tx.send(buf.to_vec()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "consumer dropped pipe reader",
            )
        })?;
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Drop for ChannelPipeWriter {
    fn drop(&mut self) {
        // Closing the sender wakes the reader, which then sees EOF.
        self.tx = None;
    }
}

pub(crate) struct ChannelPipeReader {
    rx: std::sync::mpsc::Receiver<Vec<u8>>,
    chunk: Vec<u8>,
    pos: usize,
}

impl Read for ChannelPipeReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.pos >= self.chunk.len() {
            match self.rx.recv() {
                Ok(next) => {
                    self.chunk = next;
                    self.pos = 0;
                }
                Err(_) => return Ok(0), // sender dropped — clean EOF
            }
        }
        let avail = self.chunk.len() - self.pos;
        let n = avail.min(buf.len());
        buf[..n].copy_from_slice(&self.chunk[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

#[cfg(test)]
mod pipe_tests {
    use super::{channel_pipe, ChannelPipeReader, ChannelPipeWriter};
    use std::io::{Read, Write};

    #[test]
    fn channel_pipe_round_trips_in_order() {
        let (mut w, mut r) = channel_pipe();
        let producer = std::thread::spawn(move || -> std::io::Result<()> {
            for i in 0u8..16 {
                let chunk = vec![i; 1024];
                w.write_all(&chunk)?;
            }
            Ok(())
        });
        let mut got = Vec::new();
        r.read_to_end(&mut got).unwrap();
        producer.join().unwrap().unwrap();
        assert_eq!(got.len(), 16 * 1024);
        for i in 0u8..16 {
            let off = i as usize * 1024;
            assert!(got[off..off + 1024].iter().all(|&b| b == i));
        }
    }

    #[test]
    fn channel_pipe_reader_eof_on_writer_drop() {
        let (w, mut r) = channel_pipe();
        drop(w);
        let mut buf = [0u8; 8];
        let n = r.read(&mut buf).unwrap();
        assert_eq!(n, 0, "expected immediate EOF when writer dropped");
    }

    fn _assert_send_for_pipe_ends() {
        fn assert_send<T: Send>() {}
        assert_send::<ChannelPipeWriter>();
        assert_send::<ChannelPipeReader>();
    }
}
