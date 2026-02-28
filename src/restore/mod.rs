use std::collections::VecDeque;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{bail, Context, Result};

use crate::backup::metadata::BackupMetadata;
use crate::backup::LogLevel;
use crate::clonezilla;
use crate::clonezilla::metadata::ClonezillaImage;
use crate::clonezilla::partclone::open_partclone_reader;
use crate::fs::exfat::patch_exfat_hidden_sectors;
use crate::fs::fat::patch_bpb_hidden_sectors;
use crate::fs::hfs::patch_hfs_hidden_sectors;
use crate::fs::hfsplus::patch_hfsplus_hidden_sectors;
use crate::fs::ntfs::patch_ntfs_hidden_sectors;
use crate::fs::{
    resize_btrfs_in_place, resize_exfat_in_place, resize_ext_in_place, resize_fat_in_place,
    resize_hfs_in_place, resize_hfsplus_in_place, resize_ntfs_in_place, resize_prodos_in_place,
    set_fat_clean_flags, validate_btrfs_integrity, validate_exfat_integrity,
    validate_ext_integrity, validate_fat_integrity, validate_hfs_integrity,
    validate_hfsplus_integrity, validate_ntfs_integrity, validate_prodos_integrity,
};
use crate::os::SectorAlignedWriter;
use crate::partition::apm::Apm;
use crate::partition::gpt::Gpt;
use crate::partition::mbr::{build_ebr_chain, patch_mbr_entries, LogicalPartitionInfo, Mbr};
use crate::partition::PartitionSizeOverride;
use crate::rbformats::reconstruct_disk_from_backup;

/// Restore configuration.
pub struct RestoreConfig {
    pub backup_folder: PathBuf,
    pub target_path: PathBuf,
    pub target_is_device: bool,
    pub target_size: u64,
    pub alignment: RestoreAlignment,
    pub partition_sizes: Vec<RestorePartitionSize>,
    /// Write zeros to unused filesystem space. Generally not needed for FAT.
    /// Set to true only if you encounter issues with specific filesystem types.
    pub write_zeros_to_unused: bool,
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
///
/// When the backup contains logical partitions inside an extended container,
/// this function accounts for EBR overhead (1 sector before each logical
/// partition) and generates a synthetic override for the extended container
/// entry so that `patch_mbr_entries` can resize it in the MBR.
pub fn calculate_restore_layout(
    metadata: &BackupMetadata,
    alignment: &RestoreAlignment,
    partition_sizes: &[RestorePartitionSize],
    target_size: u64,
) -> Result<Vec<PartitionSizeOverride>> {
    // APM: use dedicated layout logic that preserves absolute positions
    if metadata.partition_table_type == "APM" {
        return calculate_apm_restore_layout(metadata, alignment, partition_sizes, target_size);
    }

    // Superfloppy: single partition at offset 0, no alignment
    if metadata.partition_table_type == "None" {
        if let Some(pm) = metadata.partitions.first() {
            return Ok(vec![PartitionSizeOverride {
                index: 0,
                start_lba: 0,
                original_size: pm.original_size_bytes,
                export_size: pm.original_size_bytes,
                new_start_lba: None,
                heads: 0,
                sectors_per_track: 0,
            }]);
        }
    }

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

    // Separate primary and logical partitions
    let has_logical = metadata.partitions.iter().any(|pm| pm.is_logical);

    let is_gpt = metadata.partition_table_type == "GPT";

    let mut overrides = Vec::new();
    let mut current_lba = first_partition_lba;

    // GPT: partitions must start at or after LBA 34 (GPT header + entries)
    if is_gpt && current_lba < 34 {
        current_lba = 34;
    }

    // For GPT, reserve 33 sectors at end for backup GPT
    let usable_target_size = if is_gpt {
        target_size.saturating_sub(33 * 512)
    } else {
        target_size
    };

    // First pass: lay out primary (non-logical) partitions, skip logical ones
    for pm in &metadata.partitions {
        if pm.is_logical {
            continue; // handled in second pass
        }

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

        let partition_size =
            compute_partition_size(size_choice, pm, current_lba, usable_target_size);

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

        current_lba += partition_size / 512;
    }

    // Second pass: lay out logical partitions inside the extended container
    if has_logical {
        // Determine extended container start LBA
        let ext_info = metadata.extended_container.as_ref();

        // Align the extended container start
        if alignment_sectors > 0 {
            let rem = current_lba % alignment_sectors;
            if rem != 0 {
                current_lba += alignment_sectors - rem;
            }
        }

        let extended_start_lba = current_lba;

        // Collect logical partitions sorted by original start_lba
        let mut logical_pms: Vec<&crate::backup::metadata::PartitionMetadata> = metadata
            .partitions
            .iter()
            .filter(|pm| pm.is_logical)
            .collect();
        logical_pms.sort_by_key(|pm| pm.start_lba);

        for pm in &logical_pms {
            let size_choice = partition_sizes
                .iter()
                .find(|s| s.index == pm.index)
                .map(|s| &s.size_choice)
                .unwrap_or(&RestoreSizeChoice::Original);

            // Reserve 1 sector for the EBR before each logical partition
            current_lba += 1;

            // Align the partition data (not the EBR) if needed
            if alignment_sectors > 0 {
                let rem = current_lba % alignment_sectors;
                if rem != 0 {
                    current_lba += alignment_sectors - rem;
                }
            }

            let partition_size =
                compute_partition_size(size_choice, pm, current_lba, usable_target_size);

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

            current_lba += partition_size / 512;
        }

        // Compute the extended container bounds: from extended_start_lba to
        // the end of the last logical partition.
        let extended_end_lba = current_lba;
        let extended_size = (extended_end_lba - extended_start_lba) * 512;

        // Add a synthetic override for the extended container entry in the MBR
        if let Some(ext) = ext_info {
            let new_start = if extended_start_lba != ext.start_lba {
                Some(extended_start_lba)
            } else {
                None
            };
            overrides.push(PartitionSizeOverride {
                index: ext.mbr_index,
                start_lba: ext.start_lba,
                original_size: ext.size_bytes,
                export_size: extended_size,
                new_start_lba: new_start,
                heads,
                sectors_per_track: spt,
            });
        }
    }

    // Validate: no partition extends past usable area
    let usable_sectors = usable_target_size / 512;
    for ov in &overrides {
        let end_lba = ov.effective_start_lba() + ov.export_size / 512;
        if end_lba > usable_sectors {
            bail!(
                "Partition {} would end at LBA {} which exceeds usable area ({} sectors)",
                ov.index,
                end_lba,
                usable_sectors,
            );
        }
    }

    // Validate: all sizes >= minimum (skip the extended container override)
    let ext_index = metadata.extended_container.as_ref().map(|e| e.mbr_index);
    for ov in &overrides {
        if Some(ov.index) == ext_index {
            continue; // extended container size is computed, not user-specified
        }
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

/// Calculate partition layout for APM (Apple Partition Map) restores.
///
/// APM partitions have fixed absolute positions (not sequential like MBR).
/// With Original alignment, each partition keeps its original `start_lba`.
/// With non-Original alignment, partitions are sorted by `start_lba` and
/// laid out sequentially from the minimum start LBA.
fn calculate_apm_restore_layout(
    metadata: &BackupMetadata,
    alignment: &RestoreAlignment,
    partition_sizes: &[RestorePartitionSize],
    target_size: u64,
) -> Result<Vec<PartitionSizeOverride>> {
    let usable_target_size = target_size;

    // Sort partitions by start_lba for consistent layout
    let mut sorted_parts: Vec<&crate::backup::metadata::PartitionMetadata> =
        metadata.partitions.iter().collect();
    sorted_parts.sort_by_key(|pm| pm.start_lba);

    let mut overrides = Vec::new();

    match alignment {
        RestoreAlignment::Original => {
            // Each partition keeps its original absolute position
            for pm in &sorted_parts {
                let size_choice = partition_sizes
                    .iter()
                    .find(|s| s.index == pm.index)
                    .map(|s| &s.size_choice)
                    .unwrap_or(&RestoreSizeChoice::Original);

                let partition_size =
                    compute_partition_size(size_choice, pm, pm.start_lba, usable_target_size);

                overrides.push(PartitionSizeOverride {
                    index: pm.index,
                    start_lba: pm.start_lba,
                    original_size: pm.original_size_bytes,
                    export_size: partition_size,
                    new_start_lba: None,
                    heads: 0,
                    sectors_per_track: 0,
                });
            }
        }
        RestoreAlignment::Modern1MB | RestoreAlignment::Custom(_) => {
            // Sequential layout from the earliest partition position
            let alignment_sectors = match alignment {
                RestoreAlignment::Modern1MB => 2048,
                RestoreAlignment::Custom(n) => *n,
                _ => unreachable!(),
            };

            let mut current_lba = sorted_parts.first().map(|pm| pm.start_lba).unwrap_or(0);

            // Align the starting LBA
            if alignment_sectors > 0 {
                let rem = current_lba % alignment_sectors;
                if rem != 0 {
                    current_lba += alignment_sectors - rem;
                }
            }

            for pm in &sorted_parts {
                let size_choice = partition_sizes
                    .iter()
                    .find(|s| s.index == pm.index)
                    .map(|s| &s.size_choice)
                    .unwrap_or(&RestoreSizeChoice::Original);

                // Align current_lba
                if alignment_sectors > 0 {
                    let rem = current_lba % alignment_sectors;
                    if rem != 0 {
                        current_lba += alignment_sectors - rem;
                    }
                }

                let partition_size =
                    compute_partition_size(size_choice, pm, current_lba, usable_target_size);

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
                    heads: 0,
                    sectors_per_track: 0,
                });

                current_lba += partition_size / 512;
            }
        }
    }

    // Validate: no partition extends past usable area
    let usable_sectors = usable_target_size / 512;
    for ov in &overrides {
        let end_lba = ov.effective_start_lba() + ov.export_size / 512;
        if end_lba > usable_sectors {
            bail!(
                "Partition {} would end at LBA {} which exceeds usable area ({} sectors)",
                ov.index,
                end_lba,
                usable_sectors,
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

/// Compute a partition's size in bytes given the user's size choice.
fn compute_partition_size(
    size_choice: &RestoreSizeChoice,
    pm: &crate::backup::metadata::PartitionMetadata,
    current_lba: u64,
    target_size: u64,
) -> u64 {
    match size_choice {
        RestoreSizeChoice::Original => pm.original_size_bytes,
        RestoreSizeChoice::Minimum => {
            let min = if pm.imaged_size_bytes > 0 {
                pm.imaged_size_bytes
            } else {
                pm.original_size_bytes
            };
            (min + 511) & !511
        }
        RestoreSizeChoice::Custom(bytes) => (bytes + 511) & !511,
        RestoreSizeChoice::FillRemaining => {
            let used_bytes = current_lba * 512;
            if target_size > used_bytes {
                target_size - used_bytes
            } else {
                pm.original_size_bytes
            }
        }
    }
}

/// Result of building an EBR chain for restore or VHD export.
pub struct EbrChainResult {
    pub extended_start_lba: u32,
    /// (byte_offset, 512-byte EBR sector) pairs to write to the disk image.
    pub ebr_sectors: Vec<(u64, [u8; 512])>,
    /// New start LBA (in sectors) for each logical partition, keyed by
    /// partition metadata index.  Used by the data-write loop to place each
    /// partition at the correct offset after layout recalculation.
    pub logical_starts: Vec<(usize, u64)>,
}

/// Build the EBR chain for a restore or VHD export with logical partitions.
///
/// Returns `None` if there are no logical partitions.
///
/// `mbr_bytes` is used as a fallback for old backups that pre-date the
/// `extended_container` metadata field: the MBR is parsed to locate the
/// extended container's original start LBA.
///
/// When any logical partition is resized, subsequent partitions are packed
/// contiguously (1-sector EBR gap) to avoid large holes in the disk image.
pub fn build_restore_ebr_chain(
    metadata: &BackupMetadata,
    overrides: &[PartitionSizeOverride],
    mbr_bytes: Option<&[u8; 512]>,
) -> Option<EbrChainResult> {
    // Determine the extended container start LBA.
    // Priority: metadata field → MBR parse → give up.
    let extended_start_lba: u32 = if let Some(ext) = &metadata.extended_container {
        overrides
            .iter()
            .find(|o| o.index == ext.mbr_index)
            .map(|o| o.effective_start_lba())
            .unwrap_or(ext.start_lba) as u32
    } else if let Some(mbr) = mbr_bytes {
        // Old backup format without extended_container: parse the MBR to find
        // the extended partition entry's start LBA.
        let mbr_parsed = Mbr::parse(mbr).ok()?;
        let ext_entry = mbr_parsed
            .entries
            .iter()
            .find(|e| e.is_extended() && !e.is_empty())?;
        ext_entry.start_lba
    } else {
        return None;
    };

    // Collect logical partitions sorted by original start_lba.
    let mut logical_pms: Vec<&crate::backup::metadata::PartitionMetadata> = metadata
        .partitions
        .iter()
        .filter(|pm| pm.is_logical)
        .collect();

    if logical_pms.is_empty() {
        return None;
    }

    logical_pms.sort_by_key(|pm| pm.start_lba);

    // Detect whether any logical partition will change size.  If so, pack
    // subsequent partitions contiguously rather than using stale original LBAs,
    // which would leave large gaps and inflate the output image.
    let any_resized = logical_pms.iter().any(|pm| {
        overrides
            .iter()
            .find(|o| o.index == pm.index)
            .map(|ov| ov.export_size != pm.original_size_bytes)
            .unwrap_or(false)
    });

    let mut logical_infos: Vec<LogicalPartitionInfo> = Vec::with_capacity(logical_pms.len());
    let mut logical_starts: Vec<(usize, u64)> = Vec::with_capacity(logical_pms.len());
    let mut next_lba: u32 = 0;

    for (i, pm) in logical_pms.iter().enumerate() {
        let ov = overrides.iter().find(|o| o.index == pm.index);
        let new_size_sectors = ov
            .map(|o| o.export_size / 512)
            .unwrap_or(pm.original_size_bytes / 512) as u32;

        let start_lba = if i == 0 {
            // First logical partition: always keep its original absolute LBA so
            // the extended container entry in the MBR still points correctly.
            ov.map(|o| o.effective_start_lba()).unwrap_or(pm.start_lba) as u32
        } else if any_resized {
            // Pack right after the previous partition, leaving 1 sector for the EBR.
            next_lba + 1
        } else {
            ov.map(|o| o.effective_start_lba()).unwrap_or(pm.start_lba) as u32
        };

        next_lba = start_lba + new_size_sectors;
        logical_starts.push((pm.index, start_lba as u64));
        logical_infos.push(LogicalPartitionInfo {
            start_lba,
            total_sectors: new_size_sectors,
            partition_type: pm.partition_type_byte,
        });
    }

    let ebr_sectors = build_ebr_chain(extended_start_lba, &logical_infos);
    Some(EbrChainResult {
        extended_start_lba,
        ebr_sectors,
        logical_starts,
    })
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
        // Check if this is a Clonezilla image
        if clonezilla::metadata::is_clonezilla_image(&config.backup_folder) {
            return run_clonezilla_restore(config, progress);
        }
        bail!(
            "metadata.json not found in {}",
            config.backup_folder.display()
        );
    }

    let metadata_file = File::open(&metadata_path)
        .with_context(|| format!("failed to open {}", metadata_path.display()))?;
    let metadata: BackupMetadata =
        serde_json::from_reader(metadata_file).context("failed to parse metadata.json")?;

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

    // Step 3: Read MBR and GPT from backup
    set_operation(&progress, "Reading partition table...");
    let is_gpt = metadata.partition_table_type == "GPT";
    let is_superfloppy = metadata.partition_table_type == "None";

    let mbr_bytes = if is_superfloppy {
        None
    } else {
        let mbr_path = config.backup_folder.join("mbr.bin");
        if mbr_path.exists() {
            let data = fs::read(&mbr_path).context("failed to read mbr.bin")?;
            let mut buf = [0u8; 512];
            let copy_len = data.len().min(512);
            buf[..copy_len].copy_from_slice(&data[..copy_len]);
            Some(buf)
        } else {
            None
        }
    };

    // Load GPT data if this is a GPT backup
    let gpt_data: Option<Gpt> = if is_gpt {
        let gpt_json_path = config.backup_folder.join("gpt.json");
        if gpt_json_path.exists() {
            let gpt_file = File::open(&gpt_json_path)
                .with_context(|| format!("failed to open {}", gpt_json_path.display()))?;
            let gpt: Gpt = serde_json::from_reader(gpt_file).context("failed to parse gpt.json")?;
            log(
                &progress,
                LogLevel::Info,
                format!("Loaded GPT: {} partition entries", gpt.entries.len()),
            );
            Some(gpt)
        } else {
            log(
                &progress,
                LogLevel::Warning,
                "GPT backup has no gpt.json — GPT structures will not be written",
            );
            None
        }
    } else {
        None
    };

    // Load APM data if this is an APM backup
    let is_apm = metadata.partition_table_type == "APM";
    let apm_data: Option<Apm> = if is_apm {
        let apm_json_path = config.backup_folder.join("apm.json");
        if apm_json_path.exists() {
            let apm_file = File::open(&apm_json_path)
                .with_context(|| format!("failed to open {}", apm_json_path.display()))?;
            let apm: Apm = serde_json::from_reader(apm_file).context("failed to parse apm.json")?;
            log(
                &progress,
                LogLevel::Info,
                format!("Loaded APM: {} partition entries", apm.entries.len()),
            );
            Some(apm)
        } else {
            log(
                &progress,
                LogLevel::Warning,
                "APM backup has no apm.json — APM structures will not be written",
            );
            None
        }
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
                ov.index, ov.start_lba, start, size_mib,
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

    // Progress total = end of last partition (includes gaps between partitions)
    let data_extent: u64 = overrides
        .iter()
        .map(|o| (o.effective_start_lba() + o.export_size / 512) * 512)
        .max()
        .unwrap_or(0);
    set_progress_bytes(&progress, 0, data_extent);

    if is_cancelled(&progress) {
        bail!("restore cancelled");
    }

    // Step 6: Open target
    set_operation(&progress, "Opening target...");
    let device_handle = if config.target_is_device {
        log(
            &progress,
            LogLevel::Info,
            format!(
                "Opening device {} for writing...",
                config.target_path.display()
            ),
        );
        crate::os::open_target_for_writing(&config.target_path)
            .with_context(|| format!("cannot open {} for writing", config.target_path.display()))?
    } else {
        log(
            &progress,
            LogLevel::Info,
            format!("Creating image file {}...", config.target_path.display()),
        );
        let file = File::create(&config.target_path)
            .with_context(|| format!("failed to create {}", config.target_path.display()))?;
        crate::os::DeviceWriteHandle::from_file(file)
    };

    // Extract the file for I/O. On Windows, device_handle's remaining fields
    // hold volume locks that keep volumes dismounted until this function returns.
    let target_file = device_handle.file;

    // Wrap in SectorAlignedWriter so that raw device writes (/dev/rdiskN on
    // macOS) are always multiples of 512 bytes.  For regular image files the
    // buffering is harmless and adds negligible overhead.
    let mut target = SectorAlignedWriter::new(target_file);
    log(
        &progress,
        LogLevel::Info,
        "SectorAlignedWriter created successfully",
    );

    if is_cancelled(&progress) {
        bail!("restore cancelled");
    }

    // Step 6b: Clear any residual GPT structures from the target disk.
    // Only needed for MBR restores — GPT restores and superfloppies write these areas directly.
    if !is_gpt && !is_superfloppy {
        clear_gpt_structures(&mut target, config.target_size, &mut |msg| {
            log(&progress, LogLevel::Info, msg);
        })?;
    }

    // Step 7: Reconstruct disk (write all data)
    set_operation(&progress, "Writing disk image...");
    let progress_clone = Arc::clone(&progress);
    let progress_cancel = Arc::clone(&progress);

    let write_result = reconstruct_disk_from_backup(
        &config.backup_folder,
        &metadata,
        mbr_bytes.as_ref(),
        &overrides,
        config.target_size,
        &mut target,
        config.target_is_device,
        config.write_zeros_to_unused,
        gpt_data.as_ref(),
        apm_data.as_ref(),
        &mut |bytes| {
            set_progress_bytes(&progress_clone, bytes, data_extent);
        },
        &|| is_cancelled(&progress_cancel),
        &mut |msg| {
            log(&progress, LogLevel::Info, msg);
        },
    );

    // A stalled write to a raw device (e.g. EIO from macOS I/O arbitration
    // while another process such as VMware probes the disk concurrently) can
    // block in the kernel for several minutes before returning an error.
    // Once the blocked syscall finally returns, treat any I/O error as a
    // clean cancellation if the user already requested it — otherwise surface
    // the original error with a hint.
    let total_written = match write_result {
        Ok(n) => n,
        Err(e) => {
            if is_cancelled(&progress) {
                bail!("restore cancelled");
            }
            // Check whether the error looks like a macOS I/O stall (EIO = os error 5).
            let is_io_stall = e.chain().any(|cause| {
                cause
                    .downcast_ref::<std::io::Error>()
                    .map_or(false, |io_err| {
                        io_err.raw_os_error() == Some(5) // EIO
                            || io_err.kind() == std::io::ErrorKind::BrokenPipe
                    })
            });
            if is_io_stall {
                return Err(e.context(
                    "disk write stalled — if another application (e.g. VMware) is \
                     accessing the target drive, suspend it and retry",
                ));
            }
            return Err(e);
        }
    };

    // Step 8: Filesystem resize operations (using inner File to avoid buffer flush on every seek)
    set_operation(&progress, "Finalizing filesystems...");

    for pm in &metadata.partitions {
        let ov = match overrides.iter().find(|o| o.index == pm.index) {
            Some(o) => o,
            None => continue,
        };
        let part_offset = ov.effective_start_lba() * 512;
        let export_size = ov.export_size;

        // Get direct access to the File for filesystem operations (avoids SectorAlignedWriter overhead)
        let inner_file = target
            .inner_mut()
            .context("failed to access device for filesystem operations")?;

        // Resize filesystem if the partition size changed.
        let needs_resize = export_size != pm.original_size_bytes;
        // For compacted HFS/HFS+ restored to original size: the backup stream was
        // trimmed to the last used block, so the partition tail was zero-filled above.
        // We must still call resize (with the same export_size) to write the correct
        // alternate volume header into that zero-filled tail.  For other filesystem
        // types, no end-of-partition structure needs fixing.
        let compacted_hfs_fixup = pm.compacted && !needs_resize;
        if needs_resize || compacted_hfs_fixup {
            let fs_type = detect_partition_fs_type(inner_file, part_offset);

            if compacted_hfs_fixup
                && matches!(fs_type, PartitionFsType::Hfs | PartitionFsType::HfsPlus)
            {
                log(
                    &progress,
                    LogLevel::Info,
                    format!(
                        "Partition {}: trimmed HFS/HFS+ backup restored to original size — \
                         writing alternate volume header into zero-filled tail",
                        pm.index
                    ),
                );
            }

            match fs_type {
                PartitionFsType::Ntfs if needs_resize => {
                    let new_sectors = export_size / 512;
                    resize_ntfs_in_place(inner_file, part_offset, new_sectors, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    })?;
                }
                PartitionFsType::Exfat if needs_resize => {
                    let new_sectors = export_size / 512;
                    resize_exfat_in_place(inner_file, part_offset, new_sectors, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    })?;
                }
                // HFS and HFS+ are matched without a needs_resize guard: resize handles
                // both shrink/grow and the same-size case, always writing the alternate VH.
                PartitionFsType::Hfs => {
                    resize_hfs_in_place(inner_file, part_offset, export_size, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    })?;
                }
                PartitionFsType::HfsPlus => {
                    resize_hfsplus_in_place(inner_file, part_offset, export_size, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    })?;
                }
                PartitionFsType::Ext if needs_resize => {
                    resize_ext_in_place(inner_file, part_offset, export_size, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    })?;
                }
                PartitionFsType::Btrfs if needs_resize => {
                    resize_btrfs_in_place(inner_file, part_offset, export_size, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    })?;
                }
                PartitionFsType::ProDos if needs_resize => {
                    resize_prodos_in_place(inner_file, part_offset, export_size, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    })?;
                }
                PartitionFsType::Fat | PartitionFsType::Unknown if needs_resize => {
                    let new_sectors = (export_size / 512) as u32;
                    resize_fat_in_place(inner_file, part_offset, new_sectors, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    })?;
                }
                _ => {}
            }
        }

        // Validate filesystem integrity after resize
        if export_size != pm.original_size_bytes || pm.compacted {
            let fs_type = detect_partition_fs_type(inner_file, part_offset);
            match fs_type {
                PartitionFsType::Ntfs => {
                    let _ = validate_ntfs_integrity(inner_file, part_offset, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    });
                }
                PartitionFsType::Exfat => {
                    let _ = validate_exfat_integrity(inner_file, part_offset, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    });
                }
                PartitionFsType::Hfs => {
                    let _ = validate_hfs_integrity(inner_file, part_offset, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    });
                }
                PartitionFsType::HfsPlus => {
                    let _ = validate_hfsplus_integrity(inner_file, part_offset, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    });
                }
                PartitionFsType::Ext => {
                    let _ = validate_ext_integrity(inner_file, part_offset, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    });
                }
                PartitionFsType::Btrfs => {
                    let _ = validate_btrfs_integrity(inner_file, part_offset, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    });
                }
                PartitionFsType::ProDos => {
                    let _ = validate_prodos_integrity(inner_file, part_offset, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    });
                }
                PartitionFsType::Fat | PartitionFsType::Unknown => {
                    let _ = validate_fat_integrity(inner_file, part_offset, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    });
                }
            }
        }
    }

    target.flush()?;

    // Close the file to ensure all writes are committed
    drop(target);

    // Step 9: Reopen device and set FAT clean flags (requires fresh file handle on macOS)
    // Only do this if at least one FAT partition was resized or compacted.
    let any_fat_needs_flags = config.target_is_device
        && metadata.partitions.iter().any(|pm| {
            if !pm.type_name.to_ascii_lowercase().contains("fat") {
                return false;
            }
            let Some(ov) = overrides.iter().find(|o| o.index == pm.index) else {
                return false;
            };
            ov.export_size != pm.original_size_bytes || pm.compacted
        });

    if any_fat_needs_flags {
        log(
            &progress,
            LogLevel::Info,
            "Setting FAT clean shutdown flags...",
        );

        // Reopen device for read-write
        let mut device_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&config.target_path)
            .with_context(|| {
                format!(
                    "failed to reopen {} for setting flags",
                    config.target_path.display()
                )
            })?;

        for pm in &metadata.partitions {
            if !pm.type_name.to_ascii_lowercase().contains("fat") {
                continue;
            }
            let ov = match overrides.iter().find(|o| o.index == pm.index) {
                Some(o) => o,
                None => continue,
            };
            let part_offset = ov.effective_start_lba() * 512;
            let export_size = ov.export_size;

            // Set clean flags if this FAT partition was resized or compacted
            if export_size != pm.original_size_bytes || pm.compacted {
                let _ = set_fat_clean_flags(&mut device_file, part_offset, &mut |msg| {
                    log(&progress, LogLevel::Info, msg)
                });
            }
        }

        device_file.flush()?;
    }

    // Step 10: Post-restore summary
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

/// Calculate the final partition layout for a Clonezilla image restore.
pub fn calculate_clonezilla_restore_layout(
    cz_image: &ClonezillaImage,
    alignment: &RestoreAlignment,
    partition_sizes: &[RestorePartitionSize],
    target_size: u64,
) -> Result<Vec<PartitionSizeOverride>> {
    // Determine alignment parameters
    let (first_partition_lba, alignment_sectors, heads, spt) = match alignment {
        RestoreAlignment::Original => {
            // Use the original alignment from the Clonezilla image
            let first_lba = cz_image
                .partitions
                .iter()
                .filter(|p| !p.is_extended)
                .map(|p| p.start_lba)
                .min()
                .unwrap_or(63);
            // Detect alignment from partition starts
            let alignment_sectors = detect_clonezilla_alignment(cz_image);
            (
                first_lba,
                alignment_sectors,
                cz_image.heads as u16,
                cz_image.sectors_per_track as u16,
            )
        }
        RestoreAlignment::Modern1MB => (2048, 2048, 0, 0),
        RestoreAlignment::Custom(n) => (*n, *n, 0, 0),
    };

    let mut overrides = Vec::new();
    let mut current_lba = first_partition_lba;

    // GPT: partitions must start at or after LBA 34
    if cz_image.is_gpt && current_lba < 34 {
        current_lba = 34;
    }

    // For GPT, reserve 33 sectors at end for backup GPT
    let usable_target_size = if cz_image.is_gpt {
        target_size.saturating_sub(33 * 512)
    } else {
        target_size
    };

    // Sort partitions by start LBA for layout calculation
    let mut sorted_parts: Vec<&clonezilla::metadata::ClonezillaPartition> = cz_image
        .partitions
        .iter()
        .filter(|p| !p.is_extended)
        .collect();
    sorted_parts.sort_by_key(|p| p.start_lba);

    for cz_part in &sorted_parts {
        let size_choice = partition_sizes
            .iter()
            .find(|s| s.index == cz_part.index)
            .map(|s| &s.size_choice)
            .unwrap_or(&RestoreSizeChoice::Original);

        // Align current_lba to boundary
        if alignment_sectors > 0 {
            let rem = current_lba % alignment_sectors;
            if rem != 0 {
                current_lba += alignment_sectors - rem;
            }
        }

        let partition_size = match size_choice {
            RestoreSizeChoice::Original => cz_part.size_bytes(),
            RestoreSizeChoice::Minimum => {
                let min = cz_part.size_bytes(); // partclone doesn't have a smaller "imaged" size
                (min + 511) & !511
            }
            RestoreSizeChoice::Custom(bytes) => (bytes + 511) & !511,
            RestoreSizeChoice::FillRemaining => {
                let used_bytes = current_lba * 512;
                if usable_target_size > used_bytes {
                    usable_target_size - used_bytes
                } else {
                    cz_part.size_bytes()
                }
            }
        };

        let new_start_lba = if current_lba != cz_part.start_lba {
            Some(current_lba)
        } else {
            None
        };

        overrides.push(PartitionSizeOverride {
            index: cz_part.index,
            start_lba: cz_part.start_lba,
            original_size: cz_part.size_bytes(),
            export_size: partition_size,
            new_start_lba,
            heads,
            sectors_per_track: spt,
        });

        current_lba += partition_size / 512;
    }

    // Validate: no partition extends past usable area
    let usable_sectors = usable_target_size / 512;
    for ov in &overrides {
        let end_lba = ov.effective_start_lba() + ov.export_size / 512;
        if end_lba > usable_sectors {
            bail!(
                "Partition {} would end at LBA {} which exceeds usable area ({} sectors)",
                ov.index,
                end_lba,
                usable_sectors,
            );
        }
    }

    Ok(overrides)
}

/// Detect the alignment pattern from a Clonezilla image's partition table.
fn detect_clonezilla_alignment(cz_image: &ClonezillaImage) -> u64 {
    let starts: Vec<u64> = cz_image
        .partitions
        .iter()
        .filter(|p| !p.is_extended && p.start_lba > 0)
        .map(|p| p.start_lba)
        .collect();

    if starts.is_empty() {
        return 1;
    }

    // Check if all partitions are aligned to a common boundary
    for alignment in [2048u64, 63] {
        if starts.iter().all(|&s| s % alignment == 0 || s == starts[0]) {
            return alignment;
        }
    }

    1 // No alignment detected
}

/// Restore a Clonezilla image to a target device or file.
fn run_clonezilla_restore(
    config: RestoreConfig,
    progress: Arc<Mutex<RestoreProgress>>,
) -> Result<()> {
    log(
        &progress,
        LogLevel::Info,
        format!(
            "Detected Clonezilla image in {}",
            config.backup_folder.display()
        ),
    );

    // Load Clonezilla image metadata
    set_operation(&progress, "Loading Clonezilla image...");
    let cz_image = clonezilla::metadata::load(&config.backup_folder)
        .context("failed to load Clonezilla image metadata")?;

    log(
        &progress,
        LogLevel::Info,
        format!(
            "Clonezilla image: {} partition(s), source disk: {}",
            cz_image.partitions.len(),
            cz_image.disk_name,
        ),
    );

    // Verify partclone files exist
    for cz_part in &cz_image.partitions {
        if cz_part.is_extended {
            continue;
        }
        for f in &cz_part.partclone_files {
            if !f.exists() {
                bail!("Missing partclone data file: {}", f.display());
            }
        }
    }

    if is_cancelled(&progress) {
        bail!("restore cancelled");
    }

    // Calculate partition layout
    set_operation(&progress, "Calculating partition layout...");
    let overrides = calculate_clonezilla_restore_layout(
        &cz_image,
        &config.alignment,
        &config.partition_sizes,
        config.target_size,
    )?;

    for ov in &overrides {
        let start = ov.effective_start_lba();
        let size_mib = ov.export_size / (1024 * 1024);
        log(
            &progress,
            LogLevel::Info,
            format!(
                "Partition {}: LBA {} -> {}, size {} MiB",
                ov.index, ov.start_lba, start, size_mib,
            ),
        );
    }

    // Pre-flight check
    let total_bytes: u64 = overrides.iter().map(|o| o.export_size).sum();
    if total_bytes > config.target_size {
        bail!(
            "Total partition size ({} bytes) exceeds target ({} bytes)",
            total_bytes,
            config.target_size,
        );
    }

    // Progress total = end of last partition (includes gaps between partitions)
    let data_extent: u64 = overrides
        .iter()
        .map(|o| (o.effective_start_lba() + o.export_size / 512) * 512)
        .max()
        .unwrap_or(0);
    set_progress_bytes(&progress, 0, data_extent);

    if is_cancelled(&progress) {
        bail!("restore cancelled");
    }

    // Open target
    set_operation(&progress, "Opening target...");
    let device_handle = if config.target_is_device {
        log(
            &progress,
            LogLevel::Info,
            format!(
                "Opening device {} for writing...",
                config.target_path.display()
            ),
        );
        crate::os::open_target_for_writing(&config.target_path)
            .with_context(|| format!("cannot open {} for writing", config.target_path.display()))?
    } else {
        log(
            &progress,
            LogLevel::Info,
            format!("Creating image file {}...", config.target_path.display()),
        );
        let file = File::create(&config.target_path)
            .with_context(|| format!("failed to create {}", config.target_path.display()))?;
        crate::os::DeviceWriteHandle::from_file(file)
    };

    let target_file = device_handle.file;
    let mut target = SectorAlignedWriter::new(target_file);

    if is_cancelled(&progress) {
        bail!("restore cancelled");
    }

    // Parse GPT from raw sectors if this is a GPT image
    let gpt_data: Option<Gpt> = if cz_image.is_gpt {
        if let Some(ref raw) = cz_image.gpt_primary_raw {
            // gpt_primary_raw contains LBAs 0-33 (or just 1-33); parse GPT from it
            let mut cursor = std::io::Cursor::new(raw);
            match Gpt::parse(&mut cursor) {
                Ok(gpt) => {
                    log(
                        &progress,
                        LogLevel::Info,
                        format!("Parsed GPT from raw sectors: {} entries", gpt.entries.len()),
                    );
                    Some(gpt)
                }
                Err(e) => {
                    log(
                        &progress,
                        LogLevel::Warning,
                        format!("Failed to parse GPT from raw sectors: {e}"),
                    );
                    None
                }
            }
        } else {
            log(
                &progress,
                LogLevel::Warning,
                "GPT image has no raw GPT sectors — GPT structures will not be written",
            );
            None
        }
    } else {
        None
    };

    // Clear any residual GPT structures from the target disk (MBR restores only)
    if !cz_image.is_gpt {
        clear_gpt_structures(&mut target, config.target_size, &mut |msg| {
            log(&progress, LogLevel::Info, msg);
        })?;
    }

    // Write disk image
    set_operation(&progress, "Writing disk image...");
    let total_written = write_clonezilla_disk(
        &cz_image,
        &overrides,
        &mut target,
        config.target_is_device,
        config.write_zeros_to_unused,
        gpt_data.as_ref(),
        config.target_size,
        &progress,
    )?;

    // Filesystem resize operations
    set_operation(&progress, "Finalizing filesystems...");

    // Sort overrides the same way as partitions
    let mut sorted_parts: Vec<&clonezilla::metadata::ClonezillaPartition> = cz_image
        .partitions
        .iter()
        .filter(|p| !p.is_extended)
        .collect();
    sorted_parts.sort_by_key(|p| p.start_lba);

    for cz_part in &sorted_parts {
        let ov = match overrides.iter().find(|o| o.index == cz_part.index) {
            Some(o) => o,
            None => continue,
        };

        let part_offset = ov.effective_start_lba() * 512;
        let export_size = ov.export_size;

        let inner_file = target
            .inner_mut()
            .context("failed to access device for filesystem operations")?;

        // Resize if partition size changed
        if export_size != cz_part.size_bytes() {
            let fs_type = detect_partition_fs_type(inner_file, part_offset);
            match fs_type {
                PartitionFsType::Ntfs => {
                    let new_sectors = export_size / 512;
                    resize_ntfs_in_place(inner_file, part_offset, new_sectors, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    })?;
                }
                PartitionFsType::Exfat => {
                    let new_sectors = export_size / 512;
                    resize_exfat_in_place(inner_file, part_offset, new_sectors, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    })?;
                }
                PartitionFsType::Hfs => {
                    resize_hfs_in_place(inner_file, part_offset, export_size, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    })?;
                }
                PartitionFsType::HfsPlus => {
                    resize_hfsplus_in_place(inner_file, part_offset, export_size, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    })?;
                }
                PartitionFsType::Ext => {
                    resize_ext_in_place(inner_file, part_offset, export_size, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    })?;
                }
                PartitionFsType::Btrfs => {
                    resize_btrfs_in_place(inner_file, part_offset, export_size, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    })?;
                }
                PartitionFsType::ProDos => {
                    resize_prodos_in_place(inner_file, part_offset, export_size, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    })?;
                }
                PartitionFsType::Fat | PartitionFsType::Unknown => {
                    let new_sectors = (export_size / 512) as u32;
                    resize_fat_in_place(inner_file, part_offset, new_sectors, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    })?;
                }
            }
        }

        // Validate filesystem integrity
        if export_size != cz_part.size_bytes() || !cz_part.partclone_files.is_empty() {
            let fs_type = detect_partition_fs_type(inner_file, part_offset);
            match fs_type {
                PartitionFsType::Ntfs => {
                    let _ = validate_ntfs_integrity(inner_file, part_offset, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    });
                }
                PartitionFsType::Exfat => {
                    let _ = validate_exfat_integrity(inner_file, part_offset, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    });
                }
                PartitionFsType::Hfs => {
                    let _ = validate_hfs_integrity(inner_file, part_offset, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    });
                }
                PartitionFsType::HfsPlus => {
                    let _ = validate_hfsplus_integrity(inner_file, part_offset, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    });
                }
                PartitionFsType::Ext => {
                    let _ = validate_ext_integrity(inner_file, part_offset, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    });
                }
                PartitionFsType::Btrfs => {
                    let _ = validate_btrfs_integrity(inner_file, part_offset, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    });
                }
                PartitionFsType::ProDos => {
                    let _ = validate_prodos_integrity(inner_file, part_offset, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    });
                }
                PartitionFsType::Fat | PartitionFsType::Unknown => {
                    let _ = validate_fat_integrity(inner_file, part_offset, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    });
                }
            }
        }
    }

    target.flush()?;
    drop(target);

    // Reopen device to set FAT clean flags (only if any FAT partition was resized/used partclone)
    let any_fat_needs_flags = config.target_is_device
        && sorted_parts.iter().any(|cz_part| {
            let fs = cz_part.filesystem_type.to_ascii_lowercase();
            if !fs.contains("fat") && !fs.contains("vfat") {
                return false;
            }
            let Some(ov) = overrides.iter().find(|o| o.index == cz_part.index) else {
                return false;
            };
            ov.export_size != cz_part.size_bytes() || !cz_part.partclone_files.is_empty()
        });

    if any_fat_needs_flags {
        log(
            &progress,
            LogLevel::Info,
            "Setting FAT clean shutdown flags...",
        );

        let mut device_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&config.target_path)
            .with_context(|| {
                format!(
                    "failed to reopen {} for setting flags",
                    config.target_path.display()
                )
            })?;

        for cz_part in &sorted_parts {
            let fs = cz_part.filesystem_type.to_ascii_lowercase();
            if !fs.contains("fat") && !fs.contains("vfat") {
                continue;
            }
            let ov = match overrides.iter().find(|o| o.index == cz_part.index) {
                Some(o) => o,
                None => continue,
            };

            let part_offset = ov.effective_start_lba() * 512;
            let export_size = ov.export_size;

            if export_size != cz_part.size_bytes() || !cz_part.partclone_files.is_empty() {
                let _ = set_fat_clean_flags(&mut device_file, part_offset, &mut |msg| {
                    log(&progress, LogLevel::Info, msg)
                });
            }
        }

        device_file.flush()?;
    }

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

/// Write a Clonezilla disk image: MBR/GPT, hidden data, EBR, and partition data.
fn write_clonezilla_disk(
    cz_image: &ClonezillaImage,
    overrides: &[PartitionSizeOverride],
    writer: &mut (impl Read + Write + Seek),
    is_device: bool,
    fill_unused_with_zeros: bool,
    gpt: Option<&Gpt>,
    disk_target_size: u64,
    progress: &Arc<Mutex<RestoreProgress>>,
) -> Result<u64> {
    use std::io;

    let mut total_written: u64 = 0;
    let target_size = progress.lock().map(|p| p.total_bytes).unwrap_or(0);
    let target_sectors = disk_target_size / 512;

    let get_export_size = |index: usize, default: u64| -> u64 {
        overrides
            .iter()
            .find(|ps| ps.index == index)
            .map(|ps| ps.export_size)
            .unwrap_or(default)
    };

    if let Some(gpt_data) = gpt {
        // GPT restore: write protective MBR + primary GPT (LBAs 0-33)
        let patched_gpt = gpt_data.patch_for_restore(overrides, target_sectors);

        let protective_mbr = Gpt::build_protective_mbr(target_sectors);
        writer
            .write_all(&protective_mbr)
            .context("failed to write protective MBR")?;
        total_written += 512;
        log(
            progress,
            LogLevel::Info,
            "Wrote protective MBR for GPT disk",
        );

        let primary_gpt = patched_gpt.build_primary_gpt(target_sectors);
        writer
            .write_all(&primary_gpt)
            .context("failed to write primary GPT")?;
        total_written += primary_gpt.len() as u64;
        log(
            progress,
            LogLevel::Info,
            format!(
                "Wrote primary GPT ({} bytes, {} entries)",
                primary_gpt.len(),
                patched_gpt.entries.len()
            ),
        );
    } else {
        // MBR restore
        let mut mbr_buf = cz_image.mbr_bytes;
        if !overrides.is_empty() {
            patch_mbr_entries(&mut mbr_buf, overrides);
            log(progress, LogLevel::Info, "Patched MBR partition table");
        }
        writer.write_all(&mbr_buf).context("failed to write MBR")?;
        total_written += 512;
    }

    // Write hidden data after MBR (MBR restores only — GPT primary occupies LBAs 1-33)
    if gpt.is_none() && !cz_image.hidden_data_after_mbr.is_empty() {
        writer
            .write_all(&cz_image.hidden_data_after_mbr)
            .context("failed to write hidden data after MBR")?;
        total_written += cz_image.hidden_data_after_mbr.len() as u64;
        log(
            progress,
            LogLevel::Info,
            format!(
                "Wrote {} bytes of hidden data after MBR",
                cz_image.hidden_data_after_mbr.len()
            ),
        );
    }

    // Sort partitions by start LBA
    let mut sorted_parts: Vec<&clonezilla::metadata::ClonezillaPartition> =
        cz_image.partitions.iter().collect();
    sorted_parts.sort_by_key(|p| p.start_lba);

    for cz_part in &sorted_parts {
        if is_cancelled(progress) {
            bail!("restore cancelled");
        }

        let effective_lba = overrides
            .iter()
            .find(|ps| ps.index == cz_part.index)
            .map(|ps| ps.effective_start_lba())
            .unwrap_or(cz_part.start_lba);

        let part_offset = effective_lba * 512;
        let export_size = get_export_size(cz_part.index, cz_part.size_bytes());

        // Fill gap to partition start
        if total_written < part_offset {
            let gap = part_offset - total_written;

            // Write EBR if applicable
            if let Some(ebr_data) = cz_image.ebr_data.get(&cz_part.device_name) {
                let ebr_gap = part_offset - total_written - ebr_data.len().min(512) as u64;
                if ebr_gap > 0 {
                    fill_gap(writer, ebr_gap, is_device, fill_unused_with_zeros)?;
                    total_written += ebr_gap;
                }
                let write_len = ebr_data.len().min(512);
                writer.write_all(&ebr_data[..write_len])?;
                total_written += write_len as u64;
            } else {
                fill_gap(writer, gap, is_device, fill_unused_with_zeros)?;
                total_written += gap;
            }
        }

        // Extended container partitions have no data
        if cz_part.is_extended {
            continue;
        }

        // Write partition data from partclone
        if cz_part.partclone_files.is_empty() {
            log(
                progress,
                LogLevel::Info,
                format!(
                    "partition-{}: no data files, filling with zeros",
                    cz_part.index
                ),
            );
            crate::rbformats::write_zeros(writer, export_size)?;
            total_written += export_size;
            continue;
        }

        log(
            progress,
            LogLevel::Info,
            format!(
                "partition-{}: decompressing partclone data ({})...",
                cz_part.index, cz_part.device_name
            ),
        );

        let (_header, mut reader) = open_partclone_reader(&cz_part.partclone_files)?;

        let mut buf = vec![0u8; crate::rbformats::CHUNK_SIZE];
        let mut part_written: u64 = 0;
        let copy_limit = export_size.min(cz_part.size_bytes());
        let mut limited = (&mut reader).take(copy_limit);

        loop {
            if is_cancelled(progress) {
                bail!("restore cancelled");
            }
            let n = limited.read(&mut buf)?;
            if n == 0 {
                break;
            }
            writer.write_all(&buf[..n])?;
            part_written += n as u64;
            total_written += n as u64;
            set_progress_bytes(progress, total_written, target_size);
        }

        // Pad if export_size > data written
        if part_written < export_size {
            let pad = export_size - part_written;
            if fill_unused_with_zeros || is_device {
                #[cfg(target_os = "windows")]
                {
                    crate::rbformats::write_zeros(writer, pad)?;
                    total_written += pad;
                }
                #[cfg(not(target_os = "windows"))]
                {
                    if fill_unused_with_zeros {
                        crate::rbformats::write_zeros(writer, pad)?;
                        total_written += pad;
                    } else {
                        match writer.seek(SeekFrom::Current(pad as i64)) {
                            Ok(_) => total_written += pad,
                            Err(e) if e.kind() == io::ErrorKind::InvalidInput => {
                                crate::rbformats::write_zeros(writer, pad)?;
                                total_written += pad;
                            }
                            Err(e) => return Err(e.into()),
                        }
                    }
                }
            } else {
                writer.seek(SeekFrom::Current(pad as i64))?;
                total_written += pad;
            }
        }

        // Patch hidden sectors
        {
            writer.flush()?;
            patch_bpb_hidden_sectors(writer, part_offset, effective_lba, &mut |msg| {
                log(progress, LogLevel::Info, msg)
            })?;
            patch_ntfs_hidden_sectors(writer, part_offset, effective_lba, &mut |msg| {
                log(progress, LogLevel::Info, msg)
            })?;
            patch_exfat_hidden_sectors(writer, part_offset, effective_lba, &mut |msg| {
                log(progress, LogLevel::Info, msg)
            })?;
            patch_hfs_hidden_sectors(writer, part_offset, effective_lba, &mut |msg| {
                log(progress, LogLevel::Info, msg)
            })?;
            patch_hfsplus_hidden_sectors(writer, part_offset, effective_lba, &mut |msg| {
                log(progress, LogLevel::Info, msg)
            })?;
        }

        log(
            progress,
            LogLevel::Info,
            format!(
                "partition-{}: wrote {} bytes (export size: {})",
                cz_part.index, part_written, export_size,
            ),
        );
    }

    // Write backup GPT at end of disk for GPT restores
    if let Some(gpt_data) = gpt {
        let patched_gpt = gpt_data.patch_for_restore(overrides, target_sectors);
        let backup_gpt = patched_gpt.build_backup_gpt(target_sectors);
        let backup_offset = (target_sectors - 33) * 512;
        writer.seek(SeekFrom::Start(backup_offset))?;
        writer
            .write_all(&backup_gpt)
            .context("failed to write backup GPT")?;
        log(
            progress,
            LogLevel::Info,
            format!(
                "Wrote backup GPT at LBA {} ({} bytes)",
                target_sectors - 33,
                backup_gpt.len()
            ),
        );
    }

    writer.flush()?;
    Ok(total_written)
}

/// Fill a gap between partitions: write zeros or seek depending on context.
fn fill_gap(
    writer: &mut (impl Write + Seek),
    gap: u64,
    is_device: bool,
    fill_unused_with_zeros: bool,
) -> Result<()> {
    use std::io;

    #[cfg(target_os = "windows")]
    let force_write_zeros = is_device;
    #[cfg(not(target_os = "windows"))]
    let force_write_zeros = false;

    if force_write_zeros || (is_device && fill_unused_with_zeros) {
        crate::rbformats::write_zeros(writer, gap)?;
    } else if is_device {
        match writer.seek(SeekFrom::Current(gap as i64)) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::InvalidInput => {
                crate::rbformats::write_zeros(writer, gap)?;
            }
            Err(e) => return Err(e.into()),
        }
    } else {
        writer.seek(SeekFrom::Current(gap as i64))?;
    }
    Ok(())
}

/// Clear any residual GPT structures from a disk so that an MBR restore is
/// not confused by leftover GPT headers.
///
/// GPT stores a primary header at LBA 1 and a backup header at the last LBA.
/// Both contain the signature "EFI PART" which firmware and OSes use to detect
/// GPT.  If a disk was previously partitioned with GPT and we are now writing
/// an MBR-based image, we must destroy both headers to prevent the old GPT
/// from taking precedence over the new MBR.
fn clear_gpt_structures(
    writer: &mut (impl Write + Seek),
    target_size: u64,
    log_cb: &mut impl FnMut(&str),
) -> Result<()> {
    // Unconditionally zero GPT areas. We are about to write an MBR-based image
    // so any pre-existing GPT must be destroyed. The cost is negligible: 34
    // sectors at the start plus 33 at the end.
    //
    // Note: we do NOT try to detect GPT first via read because macOS raw
    // devices (/dev/rdiskN) require sector-aligned I/O, and reading here would
    // add complexity for no real benefit.
    log_cb("Clearing any existing GPT structures on target...");

    let zeros = [0u8; 512];

    // Zero LBAs 1-33: primary GPT header (LBA 1) + partition entries (LBAs 2-33)
    for lba in 1..34u64 {
        writer.seek(SeekFrom::Start(lba * 512))?;
        writer
            .write_all(&zeros)
            .context("failed to clear primary GPT area")?;
    }

    // Zero backup GPT at the end of the disk: partition entries (32 sectors)
    // followed by the backup header (last sector)
    if target_size >= 34 * 512 {
        let last_lba = target_size / 512 - 1;
        // Backup partition entries occupy 32 sectors before the backup header
        let backup_entries_start_lba = last_lba - 32;
        for lba in backup_entries_start_lba..=last_lba {
            writer.seek(SeekFrom::Start(lba * 512))?;
            writer
                .write_all(&zeros)
                .context("failed to clear backup GPT area")?;
        }
    }

    writer.flush()?;
    log_cb("Cleared GPT structures");

    // Seek back to start for subsequent operations
    writer.seek(SeekFrom::Start(0))?;
    Ok(())
}

/// Detected filesystem type for a partition.
enum PartitionFsType {
    Fat,
    Ntfs,
    Exfat,
    Hfs,
    HfsPlus,
    Ext,
    Btrfs,
    ProDos,
    Unknown,
}

/// Detect the filesystem type of a partition by reading its boot sector magic bytes.
/// All reads are done in sector-aligned 512-byte blocks to work on raw devices
/// (e.g. macOS /dev/rdiskN) which require sector-aligned I/O.
fn detect_partition_fs_type(
    file: &mut (impl Read + Seek),
    partition_offset: u64,
) -> PartitionFsType {
    let mut sector = [0u8; 512];

    // Read first sector (boot sector): FAT/NTFS/exFAT signatures
    if file.seek(SeekFrom::Start(partition_offset)).is_err() {
        return PartitionFsType::Unknown;
    }
    if file.read_exact(&mut sector).is_err() {
        return PartitionFsType::Unknown;
    }
    if &sector[3..11] == b"NTFS    " {
        return PartitionFsType::Ntfs;
    }
    if &sector[3..11] == b"EXFAT   " {
        return PartitionFsType::Exfat;
    }
    if sector[0] == 0xEB || sector[0] == 0xE9 {
        return PartitionFsType::Fat;
    }

    // Read sector at partition_offset + 1024 (sector 2): HFS/HFS+ and ext2/3/4 signatures
    // HFS/HFS+ volume header is at partition_offset + 1024
    // ext superblock magic (0xEF53) is at partition_offset + 1024 + 0x38
    // Both fall within the same 512-byte sector
    if file.seek(SeekFrom::Start(partition_offset + 1024)).is_ok()
        && file.read_exact(&mut sector).is_ok()
    {
        let sig = u16::from_be_bytes([sector[0], sector[1]]);
        match sig {
            0x4244 => {
                // HFS — check for embedded HFS+ at offset 124-125 from MDB start
                let embed_sig = u16::from_be_bytes([sector[124], sector[125]]);
                if embed_sig == 0x482B {
                    return PartitionFsType::HfsPlus;
                }
                return PartitionFsType::Hfs;
            }
            0x482B | 0x4858 => return PartitionFsType::HfsPlus,
            _ => {}
        }

        // ext2/3/4 magic at offset 0x38 within this sector
        if sector[0x38] == 0x53 && sector[0x39] == 0xEF {
            return PartitionFsType::Ext;
        }
        // ProDOS volume directory key block
        if sector[0] == 0
            && sector[1] == 0
            && (sector[4] >> 4) == 0xF
            && (sector[4] & 0xF) >= 1
            && sector[27] == 39
            && sector[28] == 13
        {
            return PartitionFsType::ProDos;
        }
    }

    // Read sector at partition_offset + 0x10000 (sector 128): btrfs superblock
    // btrfs magic "_BHRfS_M" at offset 0x40 within the superblock
    if file
        .seek(SeekFrom::Start(partition_offset + 0x10000))
        .is_ok()
        && file.read_exact(&mut sector).is_ok()
    {
        if &sector[0x40..0x48] == b"_BHRfS_M" {
            return PartitionFsType::Btrfs;
        }
    }

    PartitionFsType::Unknown
}
