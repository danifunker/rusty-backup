pub mod format;
pub mod metadata;
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
use metadata::{AlignmentMetadata, BackupMetadata, PartitionMetadata};

/// Compression type for backup output.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum CompressionType {
    Chd,
    Vhd,
    Zstd,
    None,
}

impl CompressionType {
    pub fn as_str(&self) -> &'static str {
        match self {
            CompressionType::Chd => "chd",
            CompressionType::Vhd => "vhd",
            CompressionType::Zstd => "zstd",
            CompressionType::None => "none",
        }
    }

    pub fn file_extension(&self) -> &'static str {
        match self {
            CompressionType::Chd => "chd",
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
    progress
        .lock()
        .map(|p| p.cancel_requested)
        .unwrap_or(false)
}

fn set_progress_bytes(progress: &Arc<Mutex<BackupProgress>>, current: u64, total: u64) {
    if let Ok(mut p) = progress.lock() {
        p.current_bytes = current;
        p.total_bytes = total;
    }
}

/// Main backup orchestrator. Runs on a background thread.
pub fn run_backup(config: BackupConfig, progress: Arc<Mutex<BackupProgress>>) -> Result<()> {
    log(&progress, LogLevel::Info, format!("Starting backup of {}", config.source_path.display()));

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
    source.read_exact(&mut mbr_bytes).context("cannot read first sector")?;
    source.seek(SeekFrom::Start(0))?;

    let table = PartitionTable::detect(&mut source)
        .context("failed to detect partition table")?;
    let alignment = partition::detect_alignment(&table);
    let partitions = table.partitions();

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

    if is_cancelled(&progress) {
        bail!("backup cancelled");
    }

    // Get source size
    let source_size = source.seek(SeekFrom::End(0))?;
    source.seek(SeekFrom::Start(0))?;

    // Step 2: Create backup folder
    set_operation(&progress, "Creating backup folder...");
    let backup_folder = format::create_backup_folder(&config.destination_dir, &config.backup_name)?;
    log(
        &progress,
        LogLevel::Info,
        format!("Backup folder: {}", backup_folder.display()),
    );

    // Step 3: Export partition table
    set_operation(&progress, "Exporting partition table...");
    match &table {
        PartitionTable::Mbr(mbr) => {
            format::export_mbr(mbr, &mbr_bytes, &backup_folder)?;
            log(&progress, LogLevel::Info, "Exported MBR (mbr.bin + mbr.json)");
        }
        PartitionTable::Gpt { gpt, .. } => {
            format::export_gpt(gpt, &mbr_bytes, &backup_folder)?;
            log(&progress, LogLevel::Info, "Exported GPT (gpt.json + mbr.bin)");
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
    let split_bytes = config.split_size_mib.map(|mib| mib as u64 * 1024 * 1024);
    let mut partition_metadata = Vec::new();
    let mut overall_bytes_done: u64 = 0;

    // Pre-compute effective sizes for each partition.
    // compact_sizes[i] is Some(compacted_size) if compaction is available for
    // partition i, or None if we should fall back to trimming.
    let mut effective_sizes: Vec<u64> = Vec::with_capacity(partitions.len());
    let mut compact_sizes: Vec<Option<u64>> = Vec::with_capacity(partitions.len());

    for part in partitions.iter() {
        if config.sector_by_sector || part.is_extended_container {
            effective_sizes.push(part.size_bytes);
            compact_sizes.push(None);
            continue;
        }

        // Try compaction first
        let part_offset = part.start_lba * 512;
        let compact_result = source
            .get_ref()
            .try_clone()
            .ok()
            .and_then(|clone| {
                fs::compact_partition_reader(
                    BufReader::new(clone),
                    part_offset,
                    part.partition_type_byte,
                )
            })
            .map(|(_, result)| result);

        if let Some(ref result) = compact_result {
            effective_sizes.push(result.compacted_size);
            compact_sizes.push(Some(result.compacted_size));
        } else {
            // Fall back to trim-based sizing
            let effective = source
                .get_ref()
                .try_clone()
                .ok()
                .and_then(|clone| {
                    fs::effective_partition_size(
                        BufReader::new(clone),
                        part_offset,
                        part.partition_type_byte,
                    )
                })
                .map(|data_end| data_end.min(part.size_bytes));
            effective_sizes.push(effective.unwrap_or(part.size_bytes));
            compact_sizes.push(None);
        }
    }

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
            log(&progress, LogLevel::Warning, format!("Failed to export mbr-min.bin: {e}"));
        } else {
            log(&progress, LogLevel::Info, "Exported minimum-size MBR (mbr-min.bin)");
        }
    }

    let total_partition_bytes: u64 = partitions
        .iter()
        .zip(&effective_sizes)
        .filter(|(p, _)| !p.is_extended_container)
        .map(|(_, &sz)| sz)
        .sum();

    let full_partition_bytes: u64 = partitions
        .iter()
        .filter(|p| !p.is_extended_container)
        .map(|p| p.size_bytes)
        .sum();

    if let Ok(mut p) = progress.lock() {
        p.total_bytes = total_partition_bytes;
        p.full_size_bytes = full_partition_bytes;
        p.current_bytes = 0;
    }

    if full_partition_bytes > total_partition_bytes {
        log(
            &progress,
            LogLevel::Info,
            format!(
                "Smart sizing: imaging {} of {} total (saving {})",
                partition::format_size(total_partition_bytes),
                partition::format_size(full_partition_bytes),
                partition::format_size(full_partition_bytes - total_partition_bytes),
            ),
        );
    }

    for (part_idx, part) in partitions.iter().enumerate() {
        if is_cancelled(&progress) {
            bail!("backup cancelled");
        }

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

        let image_size = effective_sizes[part_idx];
        let is_compacted = compact_sizes[part_idx].is_some();

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
            log(
                &progress,
                LogLevel::Info,
                format!(
                    "Compacting {}: {} at LBA {}, {} → {} (defragmented)",
                    part_label,
                    part.type_name,
                    part.start_lba,
                    partition::format_size(part.size_bytes),
                    partition::format_size(image_size),
                ),
            );
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

        let compressed_files = if is_compacted {
            // Use compacted reader — create a fresh CompactFatReader
            let part_offset = part.start_lba * 512;
            let clone = source.get_ref().try_clone()
                .context("failed to clone source for compaction")?;
            let (mut compact_reader, _) = fs::CompactFatReader::new(
                BufReader::new(clone),
                part_offset,
            ).map_err(|e| anyhow::anyhow!("compaction failed: {e}"))?;

            let progress_log = Arc::clone(&progress);
            crate::rbformats::compress_partition(
                &mut compact_reader,
                &output_base,
                config.compression,
                split_bytes,
                false, // compacted image has no wasted space, don't skip zeros
                |bytes_read| {
                    set_progress_bytes(&progress_clone, base_bytes + bytes_read, total_partition_bytes);
                },
                || is_cancelled(&progress_clone),
                |msg| log(&progress_log, LogLevel::Info, msg),
            )
            .with_context(|| format!("failed to compress {part_label}"))?
        } else {
            // Fall back to trim-based read
            let part_offset = part.start_lba * 512;
            source.seek(SeekFrom::Start(part_offset))?;
            let part_reader = (&mut source).take(image_size);
            let mut limited = LimitedReader::new(part_reader);

            let progress_log = Arc::clone(&progress);
            crate::rbformats::compress_partition(
                &mut limited,
                &output_base,
                config.compression,
                split_bytes,
                !config.sector_by_sector,
                |bytes_read| {
                    set_progress_bytes(&progress_clone, base_bytes + bytes_read, total_partition_bytes);
                },
                || is_cancelled(&progress_clone),
                |msg| log(&progress_log, LogLevel::Info, msg),
            )
            .with_context(|| format!("failed to compress {part_label}"))?
        };

        overall_bytes_done += image_size;
        set_progress_bytes(&progress, overall_bytes_done, total_partition_bytes);

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
        });
    }

    if is_cancelled(&progress) {
        bail!("backup cancelled");
    }

    // Step 5: Write metadata.json
    set_operation(&progress, "Writing metadata...");
    let metadata = BackupMetadata {
        version: 1,
        created: Utc::now().to_rfc3339(),
        source_device: config.source_path.display().to_string(),
        source_size_bytes: source_size,
        partition_table_type: table.type_name().to_string(),
        checksum_type: config.checksum.as_str().to_string(),
        compression_type: config.compression.as_str().to_string(),
        split_size_mib: config.split_size_mib,
        sector_by_sector: config.sector_by_sector,
        alignment: AlignmentMetadata {
            detected_type: format!("{}", alignment.alignment_type),
            first_partition_lba: alignment.first_lba,
            alignment_sectors: alignment.alignment_sectors,
            heads: alignment.heads,
            sectors_per_track: alignment.sectors_per_track,
        },
        partitions: partition_metadata,
        bad_sectors: vec![],
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
