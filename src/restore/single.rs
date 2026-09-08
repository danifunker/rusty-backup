use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{bail, Context, Result};

use crate::backup::metadata::BackupMetadata;
use crate::backup::LogLevel;
use crate::fs::patch_hidden_sectors_for;
use crate::fs::{
    resize_hfs_in_place, resize_hfsplus_in_place, validate_btrfs_integrity,
    validate_exfat_integrity, validate_ext_integrity, validate_fat_integrity,
    validate_hfs_integrity, validate_hfsplus_integrity, validate_ntfs_integrity,
    validate_prodos_integrity,
};
use crate::os::SectorAlignedWriter;
use crate::partition::apm::build_minimal_apm;
use crate::partition::gpt::{build_minimal_gpt, Gpt};
use crate::partition::mbr::build_minimal_mbr;
use crate::rbformats;

use super::{detect_partition_fs_type, write_fat_clean_flags, PartitionFsType, RestoreProgress};

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
    // `header_size` is what the filesystem inside the stream believes it is: a
    // compacted stream is trimmed but still describes the original volume.
    let (
        source_data_size,
        header_size,
        verbatim,
        compression_type,
        compressed_files,
        backup_folder,
    ) = match &config.source {
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

            // The recorded checksum is the only defence against a damaged
            // member, and the target has not been opened yet.
            set_operation(&progress, "Verifying backup checksum...");
            crate::backup::verify::verify_partition_member(
                folder,
                &metadata.checksum_type,
                pm,
                &mut |m| log(&progress, LogLevel::Info, m),
            )?;

            let header_size = if pm.compacted && !pm.defragmented_clone && pm.imaged_size_bytes > 0
            {
                pm.original_size_bytes
            } else {
                pm.imaged_size_bytes
            };
            (
                pm.imaged_size_bytes,
                header_size,
                pm.defragmented_clone,
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
            (
                data_size,
                data_size,
                false,
                "none".to_string(),
                vec![],
                None,
            )
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
            // One decode over every member: a split backup cuts a single byte
            // stream across them, so decoding each file separately only works
            // for raw and corrupts a split .chd / .zst.
            let members: Vec<std::path::PathBuf> =
                compressed_files.iter().map(|f| folder.join(f)).collect();
            rbformats::decompress_members_to_writer(
                &members,
                &compression_type,
                &mut target,
                Some(source_data_size),
                &mut |bytes| set_progress_bytes(&progress_clone, bytes, write_size),
                &|| is_cancelled(&progress_cancel),
                &mut |msg| log(&progress_log, LogLevel::Info, msg),
            )
            .with_context(|| format!("failed to decompress {}", compressed_files.join(", ")))?
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
    target.sync_all()?;

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
            PartitionFsType::Fat
            | PartitionFsType::Ntfs
            | PartitionFsType::Exfat
            | PartitionFsType::Hfs
            | PartitionFsType::HfsPlus => {
                let _ = patch_hidden_sectors_for(
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
    // A defragmented clone is a complete volume at its imaged size; resizing
    // it would corrupt it, so a larger window is left zero-padded (as restore does).
    let needs_resize = !verbatim && write_size != header_size;
    let alt_header_fixup = !verbatim && !needs_resize && write_size != source_data_size;
    if needs_resize || alt_header_fixup {
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
                crate::partition::format_size(header_size),
                crate::partition::format_size(write_size),
            ),
        );
        let mut log_cb = |msg: &str| log(&progress, LogLevel::Info, msg);
        if needs_resize {
            crate::fs::resize_filesystem_for(
                inner_file,
                config.target_offset_bytes,
                write_size,
                &mut log_cb,
            )?;
        } else {
            // Trimmed HFS at its original size: only the alternate header in
            // the zero-filled tail is missing.
            resize_hfs_in_place(
                inner_file,
                config.target_offset_bytes,
                write_size,
                &mut log_cb,
            )?;
            resize_hfsplus_in_place(
                inner_file,
                config.target_offset_bytes,
                write_size,
                &mut log_cb,
            )?;
        }
    } else if verbatim && write_size != source_data_size {
        log(
            &progress,
            LogLevel::Info,
            "Defragmented clone: volume bytes left verbatim, tail zero-padded",
        );
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

    // Step 8: Set FAT clean flags, through the handle we already hold — see
    // `write_fat_clean_flags` for why this must not reopen the target.
    if config.target_is_device {
        let inner_file = target
            .inner_mut()
            .context("failed to access device for FAT clean flags")?;
        let is_fat = matches!(
            detect_partition_fs_type(inner_file, config.target_offset_bytes),
            PartitionFsType::Fat
        );
        if is_fat {
            log(
                &progress,
                LogLevel::Info,
                "Setting FAT clean shutdown flags...",
            );
            write_fat_clean_flags(&mut target, &[config.target_offset_bytes], &mut |msg| {
                log(&progress, LogLevel::Info, msg)
            })?;
        }
    }

    target.flush()?;
    target.sync_all()?;
    drop(target);

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backup::metadata::{AlignmentMetadata, BackupLayout, PartitionMetadata};
    use crate::fs::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
    use crate::fs::hfsplus::{create_blank_hfsplus, CompactHfsPlusReader, HfsPlusFilesystem};
    use std::io::Cursor;

    const MIB: u64 = 1024 * 1024;
    const ORIGINAL: u64 = 8 * MIB;

    /// A per-partition backup folder holding one compacted (trimmed) HFS+
    /// partition, exactly as run_backup writes it. Returns the trimmed length.
    fn compacted_hfsplus_backup(folder: &std::path::Path) -> u64 {
        let mut img = create_blank_hfsplus(ORIGINAL, 4096, "Trim", false);
        {
            let mut hfs = HfsPlusFilesystem::open(Cursor::new(&mut img), 0).unwrap();
            hfs.prepare_for_edit().unwrap();
            let root = hfs.root().unwrap();
            let mut b = Cursor::new(b"beta\n".as_ref());
            hfs.create_file(&root, "beta.txt", &mut b, 5, &CreateFileOptions::default())
                .unwrap();
            hfs.sync_metadata().unwrap();
        }
        // The backup trims the layout-preserving stream at the last data byte.
        let trimmed =
            crate::fs::effective_partition_size(Cursor::new(img.clone()), 0, 0xAF, None).unwrap();
        assert!(trimmed < ORIGINAL, "the blank volume must trim: {trimmed}");
        let (reader, _) = CompactHfsPlusReader::new(Cursor::new(img), 0).unwrap();
        let mut stream = Vec::new();
        reader.take(trimmed).read_to_end(&mut stream).unwrap();
        assert_eq!(stream.len() as u64, trimmed);

        std::fs::create_dir_all(folder).unwrap();
        let part_path = folder.join("partition-0.raw");
        std::fs::write(&part_path, &stream).unwrap();
        let checksum = crate::backup::verify::compute_checksum(
            &part_path,
            crate::backup::ChecksumType::Sha256,
        )
        .unwrap();
        std::fs::write(
            folder.join("partition-0.raw.sha256"),
            format!("{checksum}  partition-0.raw\n"),
        )
        .unwrap();
        let metadata = BackupMetadata {
            version: 1,
            created: "2026-09-02T00:00:00Z".to_string(),
            source_device: "synthetic".to_string(),
            source_size_bytes: ORIGINAL + 512,
            partition_table_type: "MBR".to_string(),
            checksum_type: "sha256".to_string(),
            compression_type: "none".to_string(),
            split_size_mib: None,
            sector_by_sector: false,
            layout: BackupLayout::PerPartition,
            container: None,
            container_logical_size: None,
            container_sha1: None,
            size_policy: None,
            alignment: AlignmentMetadata {
                detected_type: "None detected".to_string(),
                first_partition_lba: 1,
                alignment_sectors: 1,
                heads: 0,
                sectors_per_track: 0,
            },
            partitions: vec![PartitionMetadata {
                index: 0,
                type_name: "HFS+".to_string(),
                partition_type_byte: 0xAF,
                start_lba: 1,
                start_byte: None,
                original_size_bytes: ORIGINAL,
                imaged_size_bytes: trimmed,
                compressed_files: vec!["partition-0.raw".to_string()],
                checksum,
                resized: false,
                compacted: true,
                is_logical: false,
                partition_type_string: None,
                minimum_size_bytes: Some(trimmed),
                defragmented_min_size_bytes: None,
                hfsplus_signature: None,
                defragmented_clone: false,
            }],
            bad_sectors: vec![],
            extended_container: None,
        };
        std::fs::write(
            folder.join("metadata.json"),
            serde_json::to_string_pretty(&metadata).unwrap(),
        )
        .unwrap();
        trimmed
    }

    fn restore_to(folder: &std::path::Path, target: &std::path::Path, size: Option<u64>) {
        let config = SinglePartitionRestoreConfig {
            source: SinglePartitionSource::Backup {
                folder: folder.to_path_buf(),
                partition_index: 0,
            },
            target_path: target.to_path_buf(),
            target_is_device: false,
            target_offset_bytes: 0,
            target_size_bytes: size,
            target_start_lba: 0,
            source_start_lba: 1,
            new_disk: None,
        };
        let progress = Arc::new(Mutex::new(RestoreProgress::new()));
        run_single_partition_restore(config, Arc::clone(&progress)).unwrap_or_else(|e| {
            let lines: Vec<String> = progress
                .lock()
                .unwrap()
                .log_messages
                .iter()
                .map(|m| m.message.clone())
                .collect();
            panic!("{e:#}\n{}", lines.join("\n"))
        });
    }

    /// BR12: with no target size the trimmed stream was written as-is, so the
    /// volume header still claimed the original 8 MiB inside a smaller file.
    #[test]
    fn a_compacted_hfsplus_partition_restored_at_its_trimmed_size_is_resized() {
        let tmp = tempfile::tempdir().unwrap();
        let folder = tmp.path().join("backup");
        let trimmed = compacted_hfsplus_backup(&folder);
        let target = tmp.path().join("target.img");
        std::fs::write(&target, vec![0u8; trimmed as usize]).unwrap();
        restore_to(&folder, &target, None);

        let out = std::fs::read(&target).unwrap();
        assert_eq!(out.len() as u64, trimmed);
        let mut hfs = HfsPlusFilesystem::open(Cursor::new(out), 0).unwrap();
        assert!(
            hfs.total_size() <= trimmed,
            "{} > {trimmed}",
            hfs.total_size()
        );
        assert!(
            hfs.total_size() > trimmed - 64 * 1024,
            "{}",
            hfs.total_size()
        );
        let root = hfs.root().unwrap();
        assert!(hfs
            .list_directory(&root)
            .unwrap()
            .iter()
            .any(|e| e.name == "beta.txt"));
    }

    /// Restored back at its original size, the trimmed stream is zero-padded
    /// and needs the alternate volume header rewritten into that tail.
    #[test]
    fn a_compacted_hfsplus_partition_restored_at_original_size_gets_its_alternate_header() {
        let tmp = tempfile::tempdir().unwrap();
        let folder = tmp.path().join("backup");
        compacted_hfsplus_backup(&folder);
        let target = tmp.path().join("target.img");
        std::fs::write(&target, vec![0u8; ORIGINAL as usize]).unwrap();
        restore_to(&folder, &target, Some(ORIGINAL));

        let out = std::fs::read(&target).unwrap();
        let alt = ORIGINAL as usize - 1024;
        assert_eq!(&out[alt..alt + 2], b"H+", "alternate volume header missing");
        let hfs = HfsPlusFilesystem::open(Cursor::new(out), 0).unwrap();
        assert_eq!(hfs.total_size(), ORIGINAL);
    }
}
