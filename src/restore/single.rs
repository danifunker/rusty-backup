use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{bail, Context, Result};

use crate::backup::metadata::BackupMetadata;
use crate::backup::LogLevel;
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
use crate::partition::apm::build_minimal_apm;
use crate::partition::gpt::{build_minimal_gpt, Gpt};
use crate::partition::mbr::build_minimal_mbr;
use crate::rbformats;

use super::{detect_partition_fs_type, PartitionFsType, RestoreProgress};

// Re-use the existing log/progress helpers from the parent module.
fn log(progress: &Arc<Mutex<RestoreProgress>>, level: LogLevel, message: impl Into<String>) {
    if let Ok(mut p) = progress.lock() {
        p.log_messages.push_back(super::LogMessage {
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

/// Source for a single-partition restore.
#[derive(Debug, Clone)]
pub enum SinglePartitionSource {
    /// From a Rusty Backup folder — pick one partition by index.
    Backup {
        folder: PathBuf,
        partition_index: usize,
    },
    /// From a standalone image file (VHD, Raw, 2MG, etc.).
    ImageFile { path: PathBuf },
}

/// Table type for creating a new partition table on a blank disk.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum NewTableType {
    Mbr,
    Gpt,
    Apm,
}

/// Configuration for creating a new partition table when restoring to an empty disk.
#[derive(Debug, Clone)]
pub struct NewDiskConfig {
    pub table_type: NewTableType,
    /// Alignment for the first partition.
    pub alignment_sectors: u64,
    /// MBR partition type byte (e.g. 0x0C for FAT32 LBA).
    pub partition_type_byte: u8,
    /// GPT partition type GUID (e.g. Microsoft Basic Data).
    pub partition_type_guid: Option<crate::partition::gpt::Guid>,
    /// APM partition type string (e.g. "Apple_HFS").
    pub partition_type_string: Option<String>,
    /// Whether the partition should be marked bootable (MBR only).
    pub bootable: bool,
    /// Target disk size in bytes (needed for GPT backup header placement).
    pub disk_size_bytes: u64,
}

/// Configuration for a single-partition restore.
#[derive(Debug, Clone)]
pub struct SinglePartitionRestoreConfig {
    pub source: SinglePartitionSource,
    /// Path to the target device or image file.
    pub target_path: PathBuf,
    /// Whether the target is a raw device (vs. image file).
    pub target_is_device: bool,
    /// Byte offset on the target where the partition data should be written.
    pub target_offset_bytes: u64,
    /// Size of the target partition in bytes. None = use source size.
    pub target_size_bytes: Option<u64>,
    /// LBA of the target partition on the target disk (for hidden-sector patching).
    pub target_start_lba: u64,
    /// Original start LBA of the source partition (for hidden-sector delta).
    pub source_start_lba: u64,
    /// When set, creates a new partition table on the target before writing data.
    pub new_disk: Option<NewDiskConfig>,
}

/// Main single-partition restore orchestrator. Runs on a background thread.
pub fn run_single_partition_restore(
    config: SinglePartitionRestoreConfig,
    progress: Arc<Mutex<RestoreProgress>>,
) -> Result<()> {
    log(
        &progress,
        LogLevel::Info,
        format!(
            "Starting single-partition restore to {} at offset {}",
            config.target_path.display(),
            config.target_offset_bytes,
        ),
    );

    // Step 1: Resolve source — determine data size and prepare reader
    set_operation(&progress, "Resolving source...");
    let (source_data_size, compression_type, compressed_files, backup_folder) = match &config.source
    {
        SinglePartitionSource::Backup {
            folder,
            partition_index,
        } => {
            let metadata_path = folder.join("metadata.json");
            let metadata_file = File::open(&metadata_path)
                .with_context(|| format!("failed to open {}", metadata_path.display()))?;
            let metadata: BackupMetadata =
                serde_json::from_reader(metadata_file).context("failed to parse metadata.json")?;

            let pm = metadata
                .partitions
                .iter()
                .find(|p| p.index == *partition_index)
                .with_context(|| {
                    format!("partition index {} not found in backup", partition_index)
                })?;

            log(
                &progress,
                LogLevel::Info,
                format!(
                    "Source: backup partition {} ({}), imaged {} / original {}",
                    pm.index,
                    pm.type_name,
                    crate::partition::format_size(pm.imaged_size_bytes),
                    crate::partition::format_size(pm.original_size_bytes),
                ),
            );

            (
                pm.imaged_size_bytes,
                metadata.compression_type.clone(),
                pm.compressed_files.clone(),
                Some(folder.clone()),
            )
        }
        SinglePartitionSource::ImageFile { path } => {
            let file =
                File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
            let format = rbformats::detect_image_format_with_path(file, Some(path))
                .with_context(|| format!("failed to detect format of {}", path.display()))?;
            let desc = format.description();
            log(
                &progress,
                LogLevel::Info,
                format!("Source: image file {} ({})", path.display(), desc),
            );

            let file2 = File::open(path)?;
            let (_reader, data_size) = rbformats::wrap_image_reader(file2, format)?;

            log(
                &progress,
                LogLevel::Info,
                format!(
                    "Image data size: {}",
                    crate::partition::format_size(data_size),
                ),
            );

            // For standalone image files, we treat them as raw data
            (data_size, "none".to_string(), vec![], None)
        }
    };

    let write_size = config.target_size_bytes.unwrap_or(source_data_size);

    if write_size < source_data_size {
        bail!(
            "Target partition ({}) is smaller than source data ({})",
            crate::partition::format_size(write_size),
            crate::partition::format_size(source_data_size),
        );
    }

    set_progress_bytes(&progress, 0, write_size);

    if is_cancelled(&progress) {
        bail!("restore cancelled");
    }

    // Step 2: Open target
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
            format!(
                "Opening image file {} for writing...",
                config.target_path.display()
            ),
        );
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&config.target_path)
            .with_context(|| format!("failed to open {}", config.target_path.display()))?;
        crate::os::DeviceWriteHandle::from_file(file)
    };

    let target_file = device_handle.file;
    let mut target = SectorAlignedWriter::new(target_file);

    // Step 2b: Write new partition table if creating a new disk
    if let Some(ref new_disk) = config.new_disk {
        set_operation(&progress, "Writing partition table...");

        // Calculate partition start LBA and size in sectors
        let start_lba = config.target_start_lba;
        let part_size_sectors = write_size / 512;

        match new_disk.table_type {
            NewTableType::Mbr => {
                log(
                    &progress,
                    LogLevel::Info,
                    format!(
                        "Creating MBR with partition at LBA {}, {} sectors, type 0x{:02X}",
                        start_lba, part_size_sectors, new_disk.partition_type_byte,
                    ),
                );
                let mbr = build_minimal_mbr(
                    0x12345678, // arbitrary disk signature
                    &[(
                        new_disk.partition_type_byte,
                        start_lba as u32,
                        part_size_sectors as u32,
                        new_disk.bootable,
                    )],
                    255,
                    63,
                );
                target.seek(SeekFrom::Start(0))?;
                target.write_all(&mbr)?;
            }
            NewTableType::Gpt => {
                let type_guid = new_disk.partition_type_guid.unwrap_or_else(|| {
                    // Default: Microsoft Basic Data
                    crate::partition::gpt::Guid::from_string("EBD0A0A2-B9E5-4433-87C0-68B6B72699C7")
                        .unwrap()
                });
                let end_lba = start_lba + part_size_sectors - 1;
                let disk_sectors = new_disk.disk_size_bytes / 512;

                log(
                    &progress,
                    LogLevel::Info,
                    format!(
                        "Creating GPT with partition at LBA {}..{}, disk {} sectors",
                        start_lba, end_lba, disk_sectors,
                    ),
                );

                let gpt = build_minimal_gpt(
                    &[(type_guid, start_lba, end_lba, "Partition 1".to_string())],
                    new_disk.disk_size_bytes,
                );

                // Write protective MBR at LBA 0
                let pmbr = Gpt::build_protective_mbr(disk_sectors);
                target.seek(SeekFrom::Start(0))?;
                target.write_all(&pmbr)?;

                // Write primary GPT (header + entries at LBAs 1-33)
                let primary = gpt.build_primary_gpt(disk_sectors);
                target.seek(SeekFrom::Start(512))?;
                target.write_all(&primary)?;

                // Write backup GPT at end of disk
                let backup = gpt.build_backup_gpt(disk_sectors);
                let backup_offset = (disk_sectors - 33) * 512;
                target.seek(SeekFrom::Start(backup_offset))?;
                target.write_all(&backup)?;
            }
            NewTableType::Apm => {
                let type_string = new_disk
                    .partition_type_string
                    .as_deref()
                    .unwrap_or("Apple_HFS")
                    .to_string();
                let block_size = 512u32;
                let total_blocks = (new_disk.disk_size_bytes / block_size as u64) as u32;

                log(
                    &progress,
                    LogLevel::Info,
                    format!(
                        "Creating APM with partition type {}, start block {}, {} blocks",
                        type_string, start_lba, part_size_sectors,
                    ),
                );

                let apm = build_minimal_apm(
                    &[(type_string, start_lba as u32, part_size_sectors as u32)],
                    block_size,
                    total_blocks,
                );
                let apm_bytes = apm.build_apm_blocks(Some(total_blocks));
                target.seek(SeekFrom::Start(0))?;
                target.write_all(&apm_bytes)?;
            }
        }

        log(&progress, LogLevel::Info, "Partition table written");
    }

    // Seek to the target offset
    target
        .seek(SeekFrom::Start(config.target_offset_bytes))
        .context("failed to seek to target offset")?;

    if is_cancelled(&progress) {
        bail!("restore cancelled");
    }

    // Step 3: Write partition data
    set_operation(&progress, "Writing partition data...");
    let progress_clone = Arc::clone(&progress);
    let progress_cancel = Arc::clone(&progress);
    let progress_log = Arc::clone(&progress);

    let bytes_written = match &config.source {
        SinglePartitionSource::Backup { .. } => {
            let folder = backup_folder.as_ref().unwrap();
            let mut total_written: u64 = 0;

            for file_name in &compressed_files {
                let data_path = folder.join(file_name);
                let remaining = source_data_size.saturating_sub(total_written);

                let written = rbformats::decompress_to_writer(
                    &data_path,
                    &compression_type,
                    &mut target,
                    Some(remaining),
                    &mut |bytes| {
                        set_progress_bytes(&progress_clone, total_written + bytes, write_size);
                    },
                    &|| is_cancelled(&progress_cancel),
                    &mut |msg| log(&progress_log, LogLevel::Info, msg),
                )
                .with_context(|| format!("failed to decompress {}", file_name))?;

                total_written += written;
            }

            total_written
        }
        SinglePartitionSource::ImageFile { path } => {
            let file = File::open(path)?;
            let format = rbformats::detect_image_format_with_path(file, Some(path))?;
            let file2 = File::open(path)?;
            let (mut reader, _) = rbformats::wrap_image_reader(file2, format)?;

            let mut buf = vec![0u8; 256 * 1024]; // 256K chunks
            let mut total_written: u64 = 0;
            let limit = source_data_size;

            loop {
                if is_cancelled(&progress_cancel) {
                    bail!("restore cancelled");
                }

                let to_read = ((limit - total_written) as usize).min(buf.len());
                if to_read == 0 {
                    break;
                }

                let n = reader.read(&mut buf[..to_read]).context("read error")?;
                if n == 0 {
                    break;
                }

                target.write_all(&buf[..n]).context("write error")?;
                total_written += n as u64;
                set_progress_bytes(&progress_clone, total_written, write_size);
            }

            total_written
        }
    };

    log(
        &progress,
        LogLevel::Info,
        format!(
            "Wrote {} of partition data",
            crate::partition::format_size(bytes_written),
        ),
    );

    // Step 4: Zero-pad if target partition is larger than source data
    if write_size > bytes_written {
        let pad_size = write_size - bytes_written;
        set_operation(&progress, "Zero-padding remaining space...");
        log(
            &progress,
            LogLevel::Info,
            format!(
                "Zero-padding {} at end of partition",
                crate::partition::format_size(pad_size),
            ),
        );

        let zero_buf = vec![0u8; 256 * 1024];
        let mut remaining = pad_size;
        while remaining > 0 {
            if is_cancelled(&progress) {
                bail!("restore cancelled");
            }
            let chunk = (remaining as usize).min(zero_buf.len());
            target.write_all(&zero_buf[..chunk])?;
            remaining -= chunk as u64;
            set_progress_bytes(
                &progress,
                bytes_written + (pad_size - remaining),
                write_size,
            );
        }
    }

    target.flush()?;

    // Step 5: Patch hidden sectors if start LBA changed
    if config.target_start_lba != config.source_start_lba {
        set_operation(&progress, "Patching filesystem metadata...");
        let inner_file = target
            .inner_mut()
            .context("failed to access device for patching")?;

        let fs_type = detect_partition_fs_type(inner_file, config.target_offset_bytes);

        log(
            &progress,
            LogLevel::Info,
            format!(
                "Patching hidden sectors: LBA {} -> {} (filesystem: {:?})",
                config.source_start_lba, config.target_start_lba, fs_type,
            ),
        );

        match fs_type {
            PartitionFsType::Fat => {
                let _ = patch_bpb_hidden_sectors(
                    inner_file,
                    config.target_offset_bytes,
                    config.target_start_lba,
                    &mut |msg| log(&progress, LogLevel::Info, msg),
                );
            }
            PartitionFsType::Ntfs => {
                let _ = patch_ntfs_hidden_sectors(
                    inner_file,
                    config.target_offset_bytes,
                    config.target_start_lba,
                    &mut |msg| log(&progress, LogLevel::Info, msg),
                );
            }
            PartitionFsType::Exfat => {
                let _ = patch_exfat_hidden_sectors(
                    inner_file,
                    config.target_offset_bytes,
                    config.target_start_lba,
                    &mut |msg| log(&progress, LogLevel::Info, msg),
                );
            }
            PartitionFsType::Hfs => {
                let _ = patch_hfs_hidden_sectors(
                    inner_file,
                    config.target_offset_bytes,
                    config.target_start_lba,
                    &mut |msg| log(&progress, LogLevel::Info, msg),
                );
            }
            PartitionFsType::HfsPlus => {
                let _ = patch_hfsplus_hidden_sectors(
                    inner_file,
                    config.target_offset_bytes,
                    config.target_start_lba,
                    &mut |msg| log(&progress, LogLevel::Info, msg),
                );
            }
            _ => {}
        }
    }

    // Step 6: Resize filesystem if target is larger than source
    if write_size != source_data_size {
        set_operation(&progress, "Resizing filesystem...");
        let inner_file = target
            .inner_mut()
            .context("failed to access device for resize")?;

        let fs_type = detect_partition_fs_type(inner_file, config.target_offset_bytes);
        log(
            &progress,
            LogLevel::Info,
            format!(
                "Resizing {:?} filesystem from {} to {}",
                fs_type,
                crate::partition::format_size(source_data_size),
                crate::partition::format_size(write_size),
            ),
        );

        match fs_type {
            PartitionFsType::Fat => {
                let new_sectors = (write_size / 512) as u32;
                resize_fat_in_place(
                    inner_file,
                    config.target_offset_bytes,
                    new_sectors,
                    &mut |msg| log(&progress, LogLevel::Info, msg),
                )?;
            }
            PartitionFsType::Ntfs => {
                let new_sectors = write_size / 512;
                resize_ntfs_in_place(
                    inner_file,
                    config.target_offset_bytes,
                    new_sectors,
                    &mut |msg| log(&progress, LogLevel::Info, msg),
                )?;
            }
            PartitionFsType::Exfat => {
                let new_sectors = write_size / 512;
                resize_exfat_in_place(
                    inner_file,
                    config.target_offset_bytes,
                    new_sectors,
                    &mut |msg| log(&progress, LogLevel::Info, msg),
                )?;
            }
            PartitionFsType::Hfs => {
                resize_hfs_in_place(
                    inner_file,
                    config.target_offset_bytes,
                    write_size,
                    &mut |msg| log(&progress, LogLevel::Info, msg),
                )?;
            }
            PartitionFsType::HfsPlus => {
                resize_hfsplus_in_place(
                    inner_file,
                    config.target_offset_bytes,
                    write_size,
                    &mut |msg| log(&progress, LogLevel::Info, msg),
                )?;
            }
            PartitionFsType::Ext => {
                resize_ext_in_place(
                    inner_file,
                    config.target_offset_bytes,
                    write_size,
                    &mut |msg| log(&progress, LogLevel::Info, msg),
                )?;
            }
            PartitionFsType::Btrfs => {
                resize_btrfs_in_place(
                    inner_file,
                    config.target_offset_bytes,
                    write_size,
                    &mut |msg| log(&progress, LogLevel::Info, msg),
                )?;
            }
            PartitionFsType::ProDos => {
                resize_prodos_in_place(
                    inner_file,
                    config.target_offset_bytes,
                    write_size,
                    &mut |msg| log(&progress, LogLevel::Info, msg),
                )?;
            }
            PartitionFsType::Unknown => {}
        }
    }

    // Step 7: Validate filesystem integrity
    {
        set_operation(&progress, "Validating filesystem...");
        let inner_file = target
            .inner_mut()
            .context("failed to access device for validation")?;

        let fs_type = detect_partition_fs_type(inner_file, config.target_offset_bytes);
        match fs_type {
            PartitionFsType::Fat => {
                let _ =
                    validate_fat_integrity(inner_file, config.target_offset_bytes, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    });
            }
            PartitionFsType::Ntfs => {
                let _ =
                    validate_ntfs_integrity(inner_file, config.target_offset_bytes, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    });
            }
            PartitionFsType::Exfat => {
                let _ =
                    validate_exfat_integrity(inner_file, config.target_offset_bytes, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    });
            }
            PartitionFsType::Hfs => {
                let _ =
                    validate_hfs_integrity(inner_file, config.target_offset_bytes, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    });
            }
            PartitionFsType::HfsPlus => {
                let _ = validate_hfsplus_integrity(
                    inner_file,
                    config.target_offset_bytes,
                    &mut |msg| log(&progress, LogLevel::Info, msg),
                );
            }
            PartitionFsType::Ext => {
                let _ =
                    validate_ext_integrity(inner_file, config.target_offset_bytes, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    });
            }
            PartitionFsType::Btrfs => {
                let _ =
                    validate_btrfs_integrity(inner_file, config.target_offset_bytes, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    });
            }
            PartitionFsType::ProDos => {
                let _ =
                    validate_prodos_integrity(inner_file, config.target_offset_bytes, &mut |msg| {
                        log(&progress, LogLevel::Info, msg)
                    });
            }
            PartitionFsType::Unknown => {}
        }
    }

    // Step 8: Set FAT clean flags if needed (requires flush + reopen on macOS devices)
    target.flush()?;
    drop(target);

    if config.target_is_device {
        let fs_type_check = {
            let mut f = std::fs::OpenOptions::new()
                .read(true)
                .open(&config.target_path)?;
            detect_partition_fs_type(&mut f, config.target_offset_bytes)
        };
        if matches!(fs_type_check, PartitionFsType::Fat) {
            log(
                &progress,
                LogLevel::Info,
                "Setting FAT clean shutdown flags...",
            );
            let mut device_file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&config.target_path)?;
            let _ = set_fat_clean_flags(&mut device_file, config.target_offset_bytes, &mut |msg| {
                log(&progress, LogLevel::Info, msg)
            });
            device_file.flush()?;
        }
    }

    log(
        &progress,
        LogLevel::Info,
        format!(
            "Single-partition restore complete: {} written to {} at offset {}",
            crate::partition::format_size(bytes_written),
            config.target_path.display(),
            config.target_offset_bytes,
        ),
    );

    if let Ok(mut p) = progress.lock() {
        p.finished = true;
        p.operation = "Restore complete".to_string();
    }

    Ok(())
}
