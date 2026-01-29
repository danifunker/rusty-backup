pub mod compress;
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

use crate::partition::{self, PartitionTable};
use metadata::{AlignmentMetadata, BackupMetadata, PartitionMetadata};

/// Compression type for backup output.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum CompressionType {
    Chd,
    Zstd,
    None,
}

impl CompressionType {
    pub fn as_str(&self) -> &'static str {
        match self {
            CompressionType::Chd => "chd",
            CompressionType::Zstd => "zstd",
            CompressionType::None => "none",
        }
    }

    pub fn file_extension(&self) -> &'static str {
        match self {
            CompressionType::Chd => "chd",
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
}

/// Shared progress state between background backup thread and the GUI.
pub struct BackupProgress {
    pub current_bytes: u64,
    pub total_bytes: u64,
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

    // Step 4: Process each partition
    let split_bytes = config.split_size_mib.map(|mib| mib as u64 * 1024 * 1024);
    let mut partition_metadata = Vec::new();
    let mut overall_bytes_done: u64 = 0;
    let total_partition_bytes: u64 = partitions.iter().map(|p| p.size_bytes).sum();

    if let Ok(mut p) = progress.lock() {
        p.total_bytes = total_partition_bytes;
        p.current_bytes = 0;
    }

    for part in &partitions {
        if is_cancelled(&progress) {
            bail!("backup cancelled");
        }

        let part_label = format!("partition-{}", part.index);
        set_operation(
            &progress,
            format!(
                "Backing up {} ({})...",
                part_label,
                partition::format_size(part.size_bytes)
            ),
        );
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

        // Seek to partition start
        let part_offset = part.start_lba * 512;
        source.seek(SeekFrom::Start(part_offset))?;

        // Create a limited reader for exactly the partition's size
        let part_reader = (&mut source).take(part.size_bytes);
        let mut limited = LimitedReader::new(part_reader);

        let output_base = backup_folder.join(&part_label);
        let progress_clone = Arc::clone(&progress);
        let base_bytes = overall_bytes_done;

        let compressed_files = compress::compress_partition(
            &mut limited,
            &output_base,
            config.compression,
            split_bytes,
            |bytes_read| {
                set_progress_bytes(&progress_clone, base_bytes + bytes_read, total_partition_bytes);
            },
            || is_cancelled(&progress_clone),
        )
        .with_context(|| format!("failed to compress {part_label}"))?;

        overall_bytes_done += part.size_bytes;
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
            // For multi-file, join with commas
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
            start_lba: part.start_lba,
            original_size_bytes: part.size_bytes,
            compressed_files,
            checksum: combined_checksum,
            resized: false,
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
