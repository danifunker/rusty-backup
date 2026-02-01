use std::collections::VecDeque;
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{bail, Context, Result};

use crate::os::SectorAlignedWriter;
use crate::partition::PartitionSizeOverride;
use crate::rbformats::reconstruct_disk_from_backup;
use crate::backup::metadata::BackupMetadata;
use crate::backup::LogLevel;

/// Restore configuration.
pub struct RestoreConfig {
    pub backup_folder: PathBuf,
    pub target_path: PathBuf,
    pub target_is_device: bool,
    pub target_size: u64,
    pub alignment: RestoreAlignment,
    pub partition_sizes: Vec<RestorePartitionSize>,
}

/// Alignment choice for the restore.
#[derive(Debug, Clone)]
pub enum RestoreAlignment {
    /// Use alignment from the backup metadata.
    Original,
    /// Modern 1 MB boundaries (LBA 2048).
    Modern1MB,
    /// Custom sector alignment.
    Custom(u64),
}

/// Per-partition size choice.
#[derive(Debug, Clone)]
pub struct RestorePartitionSize {
    pub index: usize,
    pub size_choice: RestoreSizeChoice,
}

/// How to size a partition during restore.
#[derive(Debug, Clone)]
pub enum RestoreSizeChoice {
    /// Use the original partition size from backup.
    Original,
    /// Use the minimum (imaged) size from backup.
    Minimum,
    /// Custom size in bytes.
    Custom(u64),
    /// Last partition fills remaining disk space.
    FillRemaining,
}

/// Shared progress state between restore thread and GUI.
pub struct RestoreProgress {
    pub current_bytes: u64,
    pub total_bytes: u64,
    pub operation: String,
    pub finished: bool,
    pub error: Option<String>,
    pub cancel_requested: bool,
    pub log_messages: VecDeque<LogMessage>,
}

/// A log message from the restore thread.
pub struct LogMessage {
    pub level: LogLevel,
    pub message: String,
}

impl RestoreProgress {
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

fn log(progress: &Arc<Mutex<RestoreProgress>>, level: LogLevel, message: impl Into<String>) {
    if let Ok(mut p) = progress.lock() {
        p.log_messages.push_back(LogMessage {
            level,
            message: message.into(),
        });
    }
}

fn set_operation(progress: &Arc<Mutex<RestoreProgress>>, op: impl Into<String>) {
    if let Ok(mut p) = progress.lock() {
        p.operation = op.into();
    }
}

fn is_cancelled(progress: &Arc<Mutex<RestoreProgress>>) -> bool {
    progress.lock().map(|p| p.cancel_requested).unwrap_or(false)
}

fn set_progress_bytes(progress: &Arc<Mutex<RestoreProgress>>, current: u64, total: u64) {
    if let Ok(mut p) = progress.lock() {
        p.current_bytes = current;
        p.total_bytes = total;
    }
}

/// Calculate the final partition layout given alignment, sizing choices, and
/// target disk size. Returns the computed `PartitionSizeOverride` list.
pub fn calculate_restore_layout(
    metadata: &BackupMetadata,
    alignment: &RestoreAlignment,
    partition_sizes: &[RestorePartitionSize],
    target_size: u64,
) -> Result<Vec<PartitionSizeOverride>> {
    // Determine alignment parameters
    let (first_partition_lba, alignment_sectors, heads, spt) = match alignment {
        RestoreAlignment::Original => (
            metadata.alignment.first_partition_lba,
            metadata.alignment.alignment_sectors,
            metadata.alignment.heads,
            metadata.alignment.sectors_per_track,
        ),
        RestoreAlignment::Modern1MB => (2048, 2048, 0, 0),
        RestoreAlignment::Custom(n) => (*n, *n, 0, 0),
    };

    let mut overrides = Vec::new();
    let mut current_lba = first_partition_lba;

    for pm in &metadata.partitions {
        // Find the user's size choice for this partition
        let size_choice = partition_sizes
            .iter()
            .find(|s| s.index == pm.index)
            .map(|s| &s.size_choice)
            .unwrap_or(&RestoreSizeChoice::Original);

        // Align current_lba to boundary
        if alignment_sectors > 0 {
            let rem = current_lba % alignment_sectors;
            if rem != 0 {
                current_lba += alignment_sectors - rem;
            }
        }

        // Calculate partition size
        let partition_size = match size_choice {
            RestoreSizeChoice::Original => pm.original_size_bytes,
            RestoreSizeChoice::Minimum => {
                // Use imaged size, but ensure it's at least as large as data
                let min = if pm.imaged_size_bytes > 0 {
                    pm.imaged_size_bytes
                } else {
                    pm.original_size_bytes
                };
                // Round up to sector boundary
                (min + 511) & !511
            }
            RestoreSizeChoice::Custom(bytes) => {
                // Round up to sector boundary
                (bytes + 511) & !511
            }
            RestoreSizeChoice::FillRemaining => {
                let used_bytes = current_lba * 512;
                if target_size > used_bytes {
                    target_size - used_bytes
                } else {
                    pm.original_size_bytes
                }
            }
        };

        let new_start_lba = if current_lba != pm.start_lba {
            Some(current_lba)
        } else {
            None
        };

        overrides.push(PartitionSizeOverride {
            index: pm.index,
            start_lba: pm.start_lba,
            original_size: pm.original_size_bytes,
            export_size: partition_size,
            new_start_lba,
            heads,
            sectors_per_track: spt,
        });

        // Advance to next partition
        current_lba += partition_size / 512;
    }

    // Validate: no partition extends past target
    let target_sectors = target_size / 512;
    for ov in &overrides {
        let end_lba = ov.effective_start_lba() + ov.export_size / 512;
        if end_lba > target_sectors {
            bail!(
                "Partition {} would end at LBA {} which exceeds target size ({} sectors)",
                ov.index,
                end_lba,
                target_sectors,
            );
        }
    }

    // Validate: all sizes >= minimum
    for ov in &overrides {
        let pm = metadata.partitions.iter().find(|p| p.index == ov.index);
        if let Some(pm) = pm {
            let min = if pm.imaged_size_bytes > 0 {
                pm.imaged_size_bytes
            } else {
                pm.original_size_bytes
            };
            if ov.export_size < min {
                bail!(
                    "Partition {} size ({}) is smaller than minimum ({})",
                    ov.index,
                    ov.export_size,
                    min,
                );
            }
        }
    }

    Ok(overrides)
}

/// Main restore orchestrator. Runs on a background thread.
pub fn run_restore(config: RestoreConfig, progress: Arc<Mutex<RestoreProgress>>) -> Result<()> {
    log(
        &progress,
        LogLevel::Info,
        format!("Starting restore from {}", config.backup_folder.display()),
    );

    // Step 1: Load and validate backup
    set_operation(&progress, "Loading backup metadata...");
    let metadata_path = config.backup_folder.join("metadata.json");
    if !metadata_path.exists() {
        bail!("metadata.json not found in {}", config.backup_folder.display());
    }

    let metadata_file = File::open(&metadata_path)
        .with_context(|| format!("failed to open {}", metadata_path.display()))?;
    let metadata: BackupMetadata = serde_json::from_reader(metadata_file)
        .context("failed to parse metadata.json")?;

    log(
        &progress,
        LogLevel::Info,
        format!(
            "Backup: {} partition(s), source: {}, compression: {}",
            metadata.partitions.len(),
            metadata.source_device,
            metadata.compression_type,
        ),
    );

    // Step 2: Verify compressed files exist
    for pm in &metadata.partitions {
        for cf in &pm.compressed_files {
            let path = config.backup_folder.join(cf);
            if !path.exists() {
                bail!("Missing data file: {}", path.display());
            }
        }
    }

    if is_cancelled(&progress) {
        bail!("restore cancelled");
    }

    // Step 3: Read MBR from backup
    set_operation(&progress, "Reading MBR...");
    let mbr_path = config.backup_folder.join("mbr.bin");
    let mbr_bytes = if mbr_path.exists() {
        let data = fs::read(&mbr_path).context("failed to read mbr.bin")?;
        let mut buf = [0u8; 512];
        let copy_len = data.len().min(512);
        buf[..copy_len].copy_from_slice(&data[..copy_len]);
        Some(buf)
    } else {
        None
    };

    // Step 4: Calculate partition layout
    set_operation(&progress, "Calculating partition layout...");
    let overrides = calculate_restore_layout(
        &metadata,
        &config.alignment,
        &config.partition_sizes,
        config.target_size,
    )?;

    // Log the layout
    for ov in &overrides {
        let start = ov.effective_start_lba();
        let size_mib = ov.export_size / (1024 * 1024);
        log(
            &progress,
            LogLevel::Info,
            format!(
                "Partition {}: LBA {} -> {}, size {} MiB",
                ov.index,
                ov.start_lba,
                start,
                size_mib,
            ),
        );
    }

    // Step 5: Pre-flight check
    let total_bytes: u64 = overrides.iter().map(|o| o.export_size).sum();
    if total_bytes > config.target_size {
        bail!(
            "Total partition size ({} bytes) exceeds target ({} bytes)",
            total_bytes,
            config.target_size,
        );
    }

    set_progress_bytes(&progress, 0, config.target_size);

    if is_cancelled(&progress) {
        bail!("restore cancelled");
    }

    // Step 6: Open target
    set_operation(&progress, "Opening target...");
    let target_file = if config.target_is_device {
        log(&progress, LogLevel::Info, format!(
            "Opening device {} for writing...", config.target_path.display()
        ));
        crate::os::open_target_for_writing(&config.target_path)
            .with_context(|| format!(
                "cannot open {} for writing", config.target_path.display()
            ))?
    } else {
        log(&progress, LogLevel::Info, format!(
            "Creating image file {}...", config.target_path.display()
        ));
        File::create(&config.target_path)
            .with_context(|| format!(
                "failed to create {}", config.target_path.display()
            ))?
    };

    // Wrap in SectorAlignedWriter so that raw device writes (/dev/rdiskN on
    // macOS) are always multiples of 512 bytes.  For regular image files the
    // buffering is harmless and adds negligible overhead.
    let mut target = SectorAlignedWriter::new(target_file);

    if is_cancelled(&progress) {
        bail!("restore cancelled");
    }

    // Step 7: Reconstruct disk
    set_operation(&progress, "Writing disk image...");
    let progress_clone = Arc::clone(&progress);
    let progress_cancel = Arc::clone(&progress);

    let total_written = reconstruct_disk_from_backup(
        &config.backup_folder,
        &metadata,
        mbr_bytes.as_ref(),
        &overrides,
        config.target_size,
        &mut target,
        &mut |bytes| {
            set_progress_bytes(&progress_clone, bytes, config.target_size);
        },
        &|| is_cancelled(&progress_cancel),
        &mut |msg| {
            log(&progress, LogLevel::Info, msg);
        },
    )?;

    target.flush()?;

    // Step 8: Post-restore summary
    log(
        &progress,
        LogLevel::Info,
        format!(
            "Restore complete: {} bytes written to {}",
            total_written,
            config.target_path.display(),
        ),
    );

    if let Ok(mut p) = progress.lock() {
        p.finished = true;
        p.operation = "Restore complete".to_string();
    }

    Ok(())
}
