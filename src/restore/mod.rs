pub mod single;

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
use crate::fs::patch_hidden_sectors_for;
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

impl Default for RestoreProgress {
    fn default() -> Self {
        Self::new()
    }
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
    // Old metadata may lack is_logical; treat index >= 4 as logical (MBR convention).
    let mut logical_pms: Vec<&crate::backup::metadata::PartitionMetadata> = metadata
        .partitions
        .iter()
        .filter(|pm| pm.is_logical || pm.index >= 4)
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
        let ptype = if pm.partition_type_byte != 0 {
            pm.partition_type_byte
        } else {
            infer_partition_type_byte(&pm.type_name)
        };
        logical_infos.push(LogicalPartitionInfo {
            start_lba,
            total_sectors: new_size_sectors,
            partition_type: ptype,
        });
    }

    let ebr_sectors = build_ebr_chain(extended_start_lba, &logical_infos);
    Some(EbrChainResult {
        extended_start_lba,
        ebr_sectors,
        logical_starts,
    })
}

/// Infer MBR partition type byte from a human-readable type name.
/// Used as a fallback for old backup metadata that lacks `partition_type_byte`.
fn infer_partition_type_byte(name: &str) -> u8 {
    let lower = name.to_ascii_lowercase();
    if lower.contains("fat32") {
        0x0C
    } else if lower.contains("fat16") {
        0x06
    } else if lower.contains("fat12") {
        0x01
    } else if lower.contains("fat") {
        0x0C
    } else if lower.contains("ntfs") || lower.contains("exfat") {
        0x07
    } else if lower.contains("linux") || lower.contains("ext") {
        0x83
    } else {
        0x06 // safe default
    }
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

    // Single-file-CHD layout: the whole disk image lives inside disk.chd.
    // When the user kept every partition at "Original" we just `io::copy`
    // the CHD's logical bytes onto the target (as-is). Any non-Original
    // choice — including FillRemaining, Custom, or Minimum — drops to the
    // re-resize path: extract each partition from the CHD and re-emit it
    // at its new offset/size, fixing up the partition table + FS
    // metadata along the way.
    if matches!(
        metadata.layout,
        crate::backup::metadata::BackupLayout::SingleFileChd
    ) {
        let any_non_original = config
            .partition_sizes
            .iter()
            .any(|p| !matches!(p.size_choice, RestoreSizeChoice::Original));
        return if any_non_original {
            run_single_file_chd_restore_resize(&config, progress, &metadata)
        } else {
            run_single_file_chd_restore_as_is(&config, progress, &metadata)
        };
    }

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
                    .is_some_and(|io_err| {
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
            let _ = validate_filesystem_for(inner_file, part_offset, fs_type, &mut |msg| {
                log(&progress, LogLevel::Info, msg)
            });
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

/// As-is restore for `BackupLayout::SingleFileChd` backups.
///
/// The CHD already contains a real disk image (partition table at sector 0,
/// gaps zero-filled, partitions in place), so restore is `io::copy` of the
/// CHD's logical bytes onto the target. Selected when every per-partition
/// size choice is `Original`; non-Original choices route through
/// `run_single_file_chd_restore_resize`.
fn run_single_file_chd_restore_as_is(
    config: &RestoreConfig,
    progress: Arc<Mutex<RestoreProgress>>,
    metadata: &BackupMetadata,
) -> Result<()> {
    use crate::rbformats::chd::ChdReader;

    let container_name = metadata.container.as_deref().unwrap_or("disk.chd");
    let chd_path = config.backup_folder.join(container_name);
    if !chd_path.exists() {
        bail!(
            "single-file-CHD backup is missing its container: {}",
            chd_path.display()
        );
    }

    log(
        &progress,
        LogLevel::Info,
        format!(
            "Single-file-CHD restore (as-is): {} -> {}",
            chd_path.display(),
            config.target_path.display(),
        ),
    );

    set_operation(&progress, "Opening CHD container...");
    let mut reader = ChdReader::open(&chd_path)
        .with_context(|| format!("failed to open {}", chd_path.display()))?;
    let logical_size = metadata
        .container_logical_size
        .unwrap_or_else(|| reader.logical_size());

    if logical_size > config.target_size {
        bail!(
            "target ({} bytes) is smaller than the CHD's logical size ({} bytes); \
             restore would overflow",
            config.target_size,
            logical_size,
        );
    }

    set_progress_bytes(&progress, 0, logical_size);

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

    // Stream the CHD's logical bytes onto the target. 1 MiB chunks per the
    // I/O sizing guidance in CONTRIBUTING.md.
    set_operation(&progress, "Writing disk image...");
    let mut buf = vec![0u8; 1024 * 1024];
    let mut written: u64 = 0;
    let mut remaining = logical_size;
    while remaining > 0 {
        if is_cancelled(&progress) {
            bail!("restore cancelled");
        }
        let want = (remaining as usize).min(buf.len());
        reader
            .read_exact(&mut buf[..want])
            .with_context(|| format!("failed to read CHD logical bytes at offset {written}"))?;
        target
            .write_all(&buf[..want])
            .with_context(|| format!("failed to write to target at offset {written}"))?;
        written += want as u64;
        remaining -= want as u64;
        set_progress_bytes(&progress, written, logical_size);
    }
    target.flush().context("failed to flush target")?;

    log(
        &progress,
        LogLevel::Info,
        format!(
            "Restore complete: {} bytes written to {}",
            written,
            config.target_path.display(),
        ),
    );

    if let Ok(mut p) = progress.lock() {
        p.finished = true;
        p.operation = "Restore complete".to_string();
    }
    Ok(())
}

/// Re-resize restore for `BackupLayout::SingleFileChd` backups.
///
/// Extracts each partition body from `disk.chd`, lays it out at the
/// user-chosen new offset/size on the target, patches the partition
/// table accordingly, and runs the standard `resize_*_in_place` +
/// `patch_hidden_sectors_for` fixups so the resulting disk is a
/// self-consistent image.
///
/// "Original" partition sizing in this path means "as-stored in the CHD"
/// (i.e. `imaged_size_bytes`), not the source disk's pre-Stage-4b
/// original — once a backup is committed, its CHD layout is the source
/// of truth.
///
/// Logical / extended partitions are not yet supported on this path
/// (the EBR rebuild that the per-partition restore does isn't wired in
/// here yet). The function bails with a clear error if any are present.
fn run_single_file_chd_restore_resize(
    config: &RestoreConfig,
    progress: Arc<Mutex<RestoreProgress>>,
    metadata: &BackupMetadata,
) -> Result<()> {
    use crate::rbformats::chd::ChdReader;

    if metadata.partitions.iter().any(|pm| pm.is_logical) {
        bail!(
            "re-resize restore for single-file-CHD backups with logical \
             partitions is not yet supported — restore as-is and use a \
             separate tool to resize, or restore to an image and re-back-up."
        );
    }

    let container_name = metadata.container.as_deref().unwrap_or("disk.chd");
    let chd_path = config.backup_folder.join(container_name);
    if !chd_path.exists() {
        bail!(
            "single-file-CHD backup is missing its container: {}",
            chd_path.display()
        );
    }

    log(
        &progress,
        LogLevel::Info,
        format!(
            "Single-file-CHD restore (re-resize): {} -> {}",
            chd_path.display(),
            config.target_path.display(),
        ),
    );

    // Treat the CHD's current layout (imaged_size_bytes) as the baseline
    // for "Original". `calculate_restore_layout` keys off
    // `original_size_bytes`, so swap them in a clone before calling.
    let mut adjusted = metadata.clone();
    for pm in &mut adjusted.partitions {
        pm.original_size_bytes = pm.imaged_size_bytes;
    }

    set_operation(&progress, "Calculating new partition layout...");
    let overrides = calculate_restore_layout(
        &adjusted,
        &config.alignment,
        &config.partition_sizes,
        config.target_size,
    )?;
    for ov in &overrides {
        log(
            &progress,
            LogLevel::Info,
            format!(
                "Partition {}: LBA {} -> {}, size {} bytes",
                ov.index,
                ov.start_lba,
                ov.effective_start_lba(),
                ov.export_size,
            ),
        );
    }

    set_operation(&progress, "Opening CHD container...");
    let mut chd_reader = ChdReader::open(&chd_path)
        .with_context(|| format!("failed to open {}", chd_path.display()))?;

    set_operation(&progress, "Opening target...");
    let device_handle = if config.target_is_device {
        crate::os::open_target_for_writing(&config.target_path)
            .with_context(|| format!("cannot open {} for writing", config.target_path.display()))?
    } else {
        // Open read+write+truncate: the FS resize fixups in step 3
        // read back partition data to inspect VBR/MDB structures, so
        // a write-only `File::create` would fail with EBADF.
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&config.target_path)
            .with_context(|| format!("failed to create {}", config.target_path.display()))?;
        // Pre-size the image so seeks past EOF don't surprise the FS
        // resize routines that read back from the alt-VH region.
        file.set_len(config.target_size)
            .context("failed to size target image")?;
        crate::os::DeviceWriteHandle::from_file(file)
    };
    let target_file = device_handle.file;
    let mut target = SectorAlignedWriter::new(target_file);

    let target_sectors = config.target_size / 512;
    let is_gpt = metadata.partition_table_type == "GPT";
    let is_apm = metadata.partition_table_type == "APM";
    let is_superfloppy = metadata.partition_table_type == "None";

    // Step 1: write the patched partition table at sector 0 (and APM
    // head / GPT primary as appropriate). The backup GPT goes at the
    // end after partition data is written.
    set_operation(&progress, "Writing partition table...");
    let mut gpt_for_backup_header: Option<Gpt> = None;
    if is_superfloppy {
        // No table to write.
    } else if is_gpt {
        let gpt_path = config.backup_folder.join("gpt.json");
        let gpt: Gpt = serde_json::from_reader(
            File::open(&gpt_path)
                .with_context(|| format!("failed to open {}", gpt_path.display()))?,
        )
        .context("failed to parse gpt.json")?;
        let patched = gpt.patch_for_restore(&overrides, target_sectors);
        target
            .write_all(&Gpt::build_protective_mbr(target_sectors))
            .context("failed to write protective MBR")?;
        target
            .write_all(&patched.build_primary_gpt(target_sectors))
            .context("failed to write primary GPT")?;
        gpt_for_backup_header = Some(patched);
    } else if is_apm {
        let apm_path = config.backup_folder.join("apm.json");
        let apm: Apm = serde_json::from_reader(
            File::open(&apm_path)
                .with_context(|| format!("failed to open {}", apm_path.display()))?,
        )
        .context("failed to parse apm.json")?;
        let block_size = apm.ddr.block_size as u64;
        let target_blocks = (config.target_size / block_size) as u32;
        let patched = apm.patch_for_restore(&overrides, target_blocks);
        let blocks = patched.build_apm_blocks(Some(target_blocks));
        target
            .write_all(&blocks)
            .context("failed to write APM head")?;
        // Driver partitions sit AFTER the APM entry blocks but BEFORE
        // the first user partition; their bodies come through the
        // normal partition loop below since they appear in
        // `metadata.partitions`.
    } else {
        // MBR: pull sector 0 from the CHD (it already carries any Stage
        // 4b patches from backup time) and re-patch with the new
        // overrides for restore-time changes.
        let mut mbr = [0u8; 512];
        chd_reader
            .seek(SeekFrom::Start(0))
            .context("failed to seek CHD to sector 0")?;
        chd_reader
            .read_exact(&mut mbr)
            .context("failed to read MBR from CHD")?;
        patch_mbr_entries(&mut mbr, &overrides);
        target
            .write_all(&mbr)
            .context("failed to write MBR to target")?;
    }

    // Pre-flight: a 1 MiB chunk size for the partition copy loop, sized
    // per CONTRIBUTING.md's I/O guidance.
    const CHUNK: usize = 1024 * 1024;
    let total_logical_bytes: u64 = overrides
        .iter()
        .map(|o| o.effective_start_lba() * 512 + o.export_size)
        .max()
        .unwrap_or(config.target_size)
        .min(config.target_size);
    set_progress_bytes(&progress, 0, total_logical_bytes);

    // Step 2: write each partition body into its new home. Sort by new
    // start LBA so we always seek forward — APM driver+data ordering
    // can interleave indices.
    let mut sorted: Vec<&crate::backup::metadata::PartitionMetadata> =
        metadata.partitions.iter().collect();
    sorted.sort_by_key(|pm| {
        overrides
            .iter()
            .find(|o| o.index == pm.index)
            .map(|o| o.effective_start_lba())
            .unwrap_or(pm.start_lba)
    });

    let mut buf = vec![0u8; CHUNK];
    let mut bytes_progressed: u64 = 0;
    for pm in &sorted {
        if is_cancelled(&progress) {
            bail!("restore cancelled");
        }
        let ov = match overrides.iter().find(|o| o.index == pm.index) {
            Some(o) => o,
            None => continue,
        };
        let new_offset = ov.effective_start_lba() * 512;
        let export_size = ov.export_size;

        // Source range inside the CHD: `pm.start_lba` was set by Stage
        // 4b's metadata writer to the partition's in-CHD position.
        let chd_offset = pm.start_lba * 512;
        let chd_size = pm.imaged_size_bytes;
        let to_copy = chd_size.min(export_size);

        log(
            &progress,
            LogLevel::Info,
            format!(
                "Partition {}: copying {} bytes from CHD offset {} to target offset {}",
                pm.index, to_copy, chd_offset, new_offset
            ),
        );

        target
            .seek(SeekFrom::Start(new_offset))
            .with_context(|| format!("seek target to partition-{} offset", pm.index))?;
        chd_reader
            .seek(SeekFrom::Start(chd_offset))
            .with_context(|| format!("seek CHD to partition-{} source", pm.index))?;

        let mut remaining = to_copy;
        while remaining > 0 {
            if is_cancelled(&progress) {
                bail!("restore cancelled");
            }
            let want = (remaining as usize).min(buf.len());
            chd_reader
                .read_exact(&mut buf[..want])
                .with_context(|| format!("read partition-{} body from CHD", pm.index))?;
            target
                .write_all(&buf[..want])
                .with_context(|| format!("write partition-{} body to target", pm.index))?;
            remaining -= want as u64;
            bytes_progressed += want as u64;
            set_progress_bytes(&progress, bytes_progressed, total_logical_bytes);
        }

        // Grow case: zero-fill the gap between the copied source bytes
        // and the new partition end so resize_filesystem_for can land
        // its alt-VH / boot tail bytes in clean space. Sparse seek is
        // not enough here because resize_*_in_place reads back, which
        // returns whatever junk was on the device.
        if to_copy < export_size {
            let pad = export_size - to_copy;
            let zeros = vec![0u8; CHUNK];
            let mut left = pad;
            while left > 0 {
                let want = (left as usize).min(zeros.len());
                target
                    .write_all(&zeros[..want])
                    .with_context(|| format!("zero-pad partition-{} grow region", pm.index))?;
                left -= want as u64;
            }
        }
    }

    target
        .flush()
        .context("flush target after partition writes")?;

    // Step 3: filesystem resize + hidden-sector patches per partition.
    set_operation(&progress, "Finalizing filesystems...");
    {
        let inner_file = target
            .inner_mut()
            .context("failed to access target file for filesystem fixups")?;
        for pm in &metadata.partitions {
            let ov = match overrides.iter().find(|o| o.index == pm.index) {
                Some(o) => o,
                None => continue,
            };
            let new_offset = ov.effective_start_lba() * 512;
            let export_size = ov.export_size;
            let new_start_lba = ov.effective_start_lba();
            // Resize is needed if the partition's size changed compared
            // to what the CHD held.
            if export_size != pm.imaged_size_bytes {
                let mut local_log = |m: &str| log(&progress, LogLevel::Info, m);
                crate::fs::resize_filesystem_for(
                    inner_file,
                    new_offset,
                    export_size,
                    &mut local_log,
                )
                .with_context(|| format!("resize partition-{} filesystem", pm.index))?;
            }
            let mut local_log = |m: &str| log(&progress, LogLevel::Info, m);
            patch_hidden_sectors_for(inner_file, new_offset, new_start_lba, &mut local_log)
                .with_context(|| format!("patch hidden sectors for partition-{}", pm.index))?;
        }
    }

    // Step 4: GPT backup header at the end of the target.
    if let Some(patched) = gpt_for_backup_header {
        let backup = patched.build_backup_gpt(target_sectors);
        let backup_offset = (target_sectors - 33) * 512;
        target
            .seek(SeekFrom::Start(backup_offset))
            .context("seek to backup GPT offset")?;
        target.write_all(&backup).context("write backup GPT")?;
    }

    target.flush().context("final flush")?;

    log(
        &progress,
        LogLevel::Info,
        format!(
            "Re-resize restore complete: {}",
            config.target_path.display()
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
            let _ = validate_filesystem_for(inner_file, part_offset, fs_type, &mut |msg| {
                log(&progress, LogLevel::Info, msg)
            });
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
            patch_hidden_sectors_for(writer, part_offset, effective_lba, &mut |msg| {
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
#[derive(Debug)]
pub(crate) enum PartitionFsType {
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

/// Validate filesystem integrity for whichever FS lives at `partition_offset`.
/// Per-FS validators have heterogeneous return types — some yield a `Vec`
/// of warnings, some a `bool`, some `()`. Callers in `restore` only care
/// about the side-effect logging, so this helper harmonizes the return
/// type to `()` and discards detailed results. (For diagnostic use cases
/// that need the warnings list, call the per-FS function directly.)
fn validate_filesystem_for(
    file: &mut (impl Read + Write + Seek),
    partition_offset: u64,
    fs_type: PartitionFsType,
    log_cb: &mut impl FnMut(&str),
) -> anyhow::Result<()> {
    match fs_type {
        PartitionFsType::Ntfs => {
            validate_ntfs_integrity(file, partition_offset, log_cb)?;
        }
        PartitionFsType::Exfat => {
            validate_exfat_integrity(file, partition_offset, log_cb)?;
        }
        PartitionFsType::Hfs => {
            validate_hfs_integrity(file, partition_offset, log_cb)?;
        }
        PartitionFsType::HfsPlus => {
            validate_hfsplus_integrity(file, partition_offset, log_cb)?;
        }
        PartitionFsType::Ext => {
            validate_ext_integrity(file, partition_offset, log_cb)?;
        }
        PartitionFsType::Btrfs => {
            validate_btrfs_integrity(file, partition_offset, log_cb)?;
        }
        PartitionFsType::ProDos => {
            validate_prodos_integrity(file, partition_offset, log_cb)?;
        }
        PartitionFsType::Fat | PartitionFsType::Unknown => {
            validate_fat_integrity(file, partition_offset, log_cb)?;
        }
    }
    Ok(())
}

/// Detect the filesystem type of a partition by reading its boot sector magic bytes.
/// All reads are done in sector-aligned 512-byte blocks to work on raw devices
/// (e.g. macOS /dev/rdiskN) which require sector-aligned I/O.
pub(crate) fn detect_partition_fs_type(
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
        && &sector[0x40..0x48] == b"_BHRfS_M"
    {
        return PartitionFsType::Btrfs;
    }

    PartitionFsType::Unknown
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backup::metadata::{AlignmentMetadata, BackupLayout, PartitionMetadata};
    use crate::backup::single_file_chd::{self, SingleFileChdInputs};
    use crate::partition::PartitionTable;
    use std::io::BufReader;

    fn build_test_mbr(part_sectors: u32) -> [u8; 512] {
        let mut mbr = [0u8; 512];
        mbr[510] = 0x55;
        mbr[511] = 0xAA;
        mbr[450] = 0x83;
        mbr[454..458].copy_from_slice(&1u32.to_le_bytes());
        mbr[458..462].copy_from_slice(&part_sectors.to_le_bytes());
        mbr
    }

    #[test]
    fn single_file_chd_as_is_restore_round_trips() {
        // Build a synthetic 4 MiB MBR disk → CHD backup folder → restore to
        // a fresh image file, verify byte-for-byte equality.
        const TOTAL_SECTORS: u32 = 8192;
        const PART_SECTORS: u32 = 4095;
        const SECTOR_SIZE: u64 = 512;
        let total_bytes = (TOTAL_SECTORS as u64) * SECTOR_SIZE;

        let tmp = tempfile::tempdir().unwrap();
        let source_path = tmp.path().join("source.img");
        let mut data = vec![0u8; total_bytes as usize];
        data[..512].copy_from_slice(&build_test_mbr(PART_SECTORS));
        for i in 0..((PART_SECTORS as u64) * SECTOR_SIZE) as usize {
            data[512 + i] = ((i % 251) as u8).wrapping_add(1);
        }
        std::fs::write(&source_path, &data).unwrap();

        let backup_folder = tmp.path().join("backup");
        std::fs::create_dir_all(&backup_folder).unwrap();
        let output_base = backup_folder.join("disk");
        let source_file = File::open(&source_path).unwrap();
        let mut br = BufReader::new(source_file.try_clone().unwrap());
        let table = PartitionTable::detect(&mut br).expect("detect MBR");
        let partitions = table.partitions();
        let mbr_bytes: [u8; 512] = data[..512].try_into().unwrap();

        let mut log_buf: Vec<String> = Vec::new();
        let mut log_cb = |s: &str| log_buf.push(s.to_string());
        let mut progress_cb = |_: u64| {};
        let cancel_check = || false;
        let chd_result = single_file_chd::run(
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
            &cancel_check,
            &mut log_cb,
        )
        .expect("backup");

        // Write metadata.json to mark this as a SingleFileChd backup folder.
        let metadata = BackupMetadata {
            version: 1,
            created: "2026-05-04T00:00:00Z".to_string(),
            source_device: source_path.display().to_string(),
            source_size_bytes: total_bytes,
            partition_table_type: table.type_name().to_string(),
            checksum_type: "sha256".to_string(),
            compression_type: "chd".to_string(),
            split_size_mib: None,
            sector_by_sector: false,
            layout: BackupLayout::SingleFileChd,
            container: Some(chd_result.container_filename.clone()),
            container_logical_size: Some(chd_result.container_logical_size),
            container_sha1: Some(chd_result.container_sha1.clone()),
            size_policy: Some(crate::backup::metadata::SizePolicy::Original),
            alignment: AlignmentMetadata {
                detected_type: "None detected".to_string(),
                first_partition_lba: 1,
                alignment_sectors: 1,
                heads: 0,
                sectors_per_track: 0,
            },
            partitions: chd_result
                .partition_ranges
                .iter()
                .map(|r| PartitionMetadata {
                    index: r.partition_index,
                    type_name: "Linux".to_string(),
                    partition_type_byte: 0x83,
                    start_lba: r.offset_in_disk / 512,
                    original_size_bytes: r.length,
                    imaged_size_bytes: r.length,
                    compressed_files: vec![],
                    checksum: r.checksum_sha256.clone(),
                    resized: false,
                    compacted: true,
                    is_logical: false,
                    partition_type_string: None,
                    minimum_size_bytes: None,
                })
                .collect(),
            bad_sectors: vec![],
            extended_container: None,
        };
        let meta_path = backup_folder.join("metadata.json");
        std::fs::write(&meta_path, serde_json::to_string_pretty(&metadata).unwrap()).unwrap();

        // Run restore to a target image file.
        let target_path = tmp.path().join("restored.img");
        let cfg = RestoreConfig {
            backup_folder: backup_folder.clone(),
            target_path: target_path.clone(),
            target_is_device: false,
            target_size: total_bytes,
            alignment: RestoreAlignment::Original,
            partition_sizes: vec![],
            write_zeros_to_unused: false,
        };
        let progress = Arc::new(Mutex::new(RestoreProgress::new()));
        run_restore(cfg, progress.clone()).expect("restore");
        assert!(progress.lock().unwrap().finished);

        let restored = std::fs::read(&target_path).unwrap();
        assert_eq!(restored.len(), data.len(), "restored length mismatch");
        assert_eq!(restored, data, "restored bytes must match source");
    }

    /// Stage 5b round-trip: build a 4 MiB MBR-disk single-file CHD backup
    /// with one 0x83 (Linux) partition spanning sectors 1..=2047, then
    /// restore with `RestoreSizeChoice::Custom` that GROWS the partition
    /// to 3 MiB. The restore validator forbids shrinking past
    /// `imaged_size_bytes`, since we'd be dropping live partition data;
    /// growth is the in-bounds resize on this path. Verify the resulting
    /// target carries a patched MBR + the partition body's leading half
    /// (matching the CHD) + zero-padded grow region.
    #[test]
    fn single_file_chd_re_resize_restore_grows_partition() {
        const TOTAL_SECTORS: u32 = 8192;
        const PART_SECTORS: u32 = 2047;
        const NEW_PART_SECTORS: u32 = 6144;
        const SECTOR_SIZE: u64 = 512;
        let total_bytes = (TOTAL_SECTORS as u64) * SECTOR_SIZE;
        let part_bytes = (PART_SECTORS as u64) * SECTOR_SIZE;
        let new_part_bytes = (NEW_PART_SECTORS as u64) * SECTOR_SIZE;

        let tmp = tempfile::tempdir().unwrap();
        let source_path = tmp.path().join("source.img");
        let mut data = vec![0u8; total_bytes as usize];
        data[..512].copy_from_slice(&build_test_mbr(PART_SECTORS));
        for i in 0..(part_bytes as usize) {
            data[512 + i] = ((i % 251) as u8).wrapping_add(1);
        }
        std::fs::write(&source_path, &data).unwrap();

        let backup_folder = tmp.path().join("backup");
        std::fs::create_dir_all(&backup_folder).unwrap();
        let output_base = backup_folder.join("disk");
        let source_file = File::open(&source_path).unwrap();
        let mut br = BufReader::new(source_file.try_clone().unwrap());
        let table = PartitionTable::detect(&mut br).expect("detect MBR");
        let partitions = table.partitions();
        let mbr_bytes: [u8; 512] = data[..512].try_into().unwrap();

        let mut log_buf: Vec<String> = Vec::new();
        let mut log_cb = |s: &str| log_buf.push(s.to_string());
        let mut progress_cb = |_: u64| {};
        let cancel_check = || false;
        let chd_result = single_file_chd::run(
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
            &cancel_check,
            &mut log_cb,
        )
        .expect("backup");

        let metadata = BackupMetadata {
            version: 1,
            created: "2026-05-04T00:00:00Z".to_string(),
            source_device: source_path.display().to_string(),
            source_size_bytes: total_bytes,
            partition_table_type: table.type_name().to_string(),
            checksum_type: "sha256".to_string(),
            compression_type: "chd".to_string(),
            split_size_mib: None,
            sector_by_sector: false,
            layout: BackupLayout::SingleFileChd,
            container: Some(chd_result.container_filename.clone()),
            container_logical_size: Some(chd_result.container_logical_size),
            container_sha1: Some(chd_result.container_sha1.clone()),
            size_policy: Some(crate::backup::metadata::SizePolicy::Original),
            alignment: AlignmentMetadata {
                detected_type: "None detected".to_string(),
                first_partition_lba: 1,
                alignment_sectors: 1,
                heads: 0,
                sectors_per_track: 0,
            },
            partitions: chd_result
                .partition_ranges
                .iter()
                .map(|r| PartitionMetadata {
                    index: r.partition_index,
                    type_name: "Linux".to_string(),
                    partition_type_byte: 0x83,
                    start_lba: r.offset_in_disk / 512,
                    original_size_bytes: r.length,
                    imaged_size_bytes: r.length,
                    compressed_files: vec![],
                    checksum: r.checksum_sha256.clone(),
                    resized: false,
                    compacted: true,
                    is_logical: false,
                    partition_type_string: None,
                    minimum_size_bytes: None,
                })
                .collect(),
            bad_sectors: vec![],
            extended_container: None,
        };
        let meta_path = backup_folder.join("metadata.json");
        std::fs::write(&meta_path, serde_json::to_string_pretty(&metadata).unwrap()).unwrap();

        // Restore with Custom shrink for partition 0.
        let target_path = tmp.path().join("restored.img");
        let cfg = RestoreConfig {
            backup_folder: backup_folder.clone(),
            target_path: target_path.clone(),
            target_is_device: false,
            target_size: total_bytes,
            alignment: RestoreAlignment::Original,
            partition_sizes: vec![RestorePartitionSize {
                index: 0,
                size_choice: RestoreSizeChoice::Custom(new_part_bytes),
            }],
            write_zeros_to_unused: false,
        };
        let progress = Arc::new(Mutex::new(RestoreProgress::new()));
        run_restore(cfg, progress.clone()).expect("restore");
        assert!(progress.lock().unwrap().finished);

        let restored = std::fs::read(&target_path).unwrap();
        // Restored MBR's partition entry must report the new (grown) size.
        let entry_size_lba = u32::from_le_bytes([
            restored[446 + 12],
            restored[446 + 13],
            restored[446 + 14],
            restored[446 + 15],
        ]);
        assert_eq!(
            entry_size_lba, NEW_PART_SECTORS,
            "restored MBR must carry the grown partition size",
        );

        // The first `part_bytes` of the partition (= imaged from CHD)
        // must equal source bytes verbatim.
        assert_eq!(
            &restored[512..512 + part_bytes as usize],
            &data[512..512 + part_bytes as usize],
            "imaged region must match source",
        );

        // The grow region (between source end and new partition end)
        // must be zero-padded by run_single_file_chd_restore_resize.
        let grow = &restored[(512 + part_bytes) as usize..(512 + new_part_bytes) as usize];
        assert!(
            grow.iter().all(|b| *b == 0),
            "grow region must be zero-filled (saw {} non-zero)",
            grow.iter().filter(|b| **b != 0).count(),
        );
        assert_eq!(
            restored.len(),
            total_bytes as usize,
            "target sized correctly"
        );
    }
}
