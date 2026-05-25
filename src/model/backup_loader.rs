//! Loading and parsing backup folders + Clonezilla images.
//!
//! Both entry points read disk, parse JSON / partition tables / partclone
//! headers, and return a typed outcome (state + warnings) for the caller to
//! drop into its UI fields. Logging stays out of the model; warnings are
//! collected as strings so the GUI can replay them through `LogPanel` (or
//! a future log abstraction).
//!
//! Extracted from `gui/inspect_tab.rs` per §5 of `docs/codecleanup.md`.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, Cursor};
use std::path::Path;

use anyhow::{Context, Result};

use crate::backup::metadata::BackupMetadata;
use crate::clonezilla::metadata::{is_clonezilla_image, load as load_cz_image, ClonezillaImage};
use crate::clonezilla::partclone::read_partclone_header;
use crate::partition::{self, detect_alignment, PartitionAlignment, PartitionInfo, PartitionTable};

/// What `load_backup` produced — either a native rusty-backup folder or a
/// detected Clonezilla image.
#[allow(clippy::large_enum_variant)] // load happens once per open; outcomes are short-lived
pub enum LoadOutcome {
    Backup(BackupLoadOutcome),
    Clonezilla(ClonezillaLoadOutcome),
}

/// Successful parse of a native rusty-backup folder (metadata.json + optional mbr.bin).
pub struct BackupLoadOutcome {
    pub metadata: BackupMetadata,
    pub partition_table: Option<PartitionTable>,
    pub alignment: Option<PartitionAlignment>,
    pub partitions: Vec<PartitionInfo>,
    /// Non-fatal messages produced during parsing (mbr.bin parse failure,
    /// EBR-chain fallback, etc.). The caller replays these through the UI.
    pub warnings: Vec<String>,
    /// Informational messages (loaded N partitions, alignment X, …).
    pub info: Vec<String>,
}

/// Successful parse of a Clonezilla image folder.
pub struct ClonezillaLoadOutcome {
    pub image: ClonezillaImage,
    pub partition_table: Option<PartitionTable>,
    pub alignment: Option<PartitionAlignment>,
    pub partitions: Vec<PartitionInfo>,
    pub partition_min_sizes: HashMap<usize, u64>,
    pub cached_metadata_count: usize,
    pub warnings: Vec<String>,
    pub info: Vec<String>,
}

/// Top-level entry: detect whether `folder` is a rusty-backup folder or a
/// Clonezilla image and parse accordingly.
pub fn load_backup(folder: &Path) -> Result<LoadOutcome> {
    let metadata_path = folder.join("metadata.json");
    if !metadata_path.exists() {
        if is_clonezilla_image(folder) {
            return load_clonezilla(folder).map(LoadOutcome::Clonezilla);
        }
        anyhow::bail!("no metadata.json found in {}", folder.display());
    }
    load_backup_metadata(folder).map(LoadOutcome::Backup)
}

/// Parse a rusty-backup folder: `metadata.json` (required) + `mbr.bin`
/// (optional). When `mbr.bin` is unparseable or missing, the partition list
/// is reconstructed from the JSON metadata.
pub fn load_backup_metadata(folder: &Path) -> Result<BackupLoadOutcome> {
    let metadata_path = folder.join("metadata.json");
    let json_str = std::fs::read_to_string(&metadata_path)
        .with_context(|| format!("cannot read {}", metadata_path.display()))?;
    let metadata: BackupMetadata = serde_json::from_str(&json_str)
        .with_context(|| format!("failed to parse {}", metadata_path.display()))?;

    let mut warnings: Vec<String> = Vec::new();
    let mut info: Vec<String> = Vec::new();
    info.push(format!(
        "Backup: {} ({} partition(s), {} compression)",
        metadata.source_device,
        metadata.partitions.len(),
        metadata.compression_type,
    ));

    let mut partition_table: Option<PartitionTable> = None;
    let mut alignment: Option<PartitionAlignment> = None;
    let mut partitions: Vec<PartitionInfo> = Vec::new();

    let mbr_bin_path = folder.join("mbr.bin");
    let parsed_from_mbr = if mbr_bin_path.exists() {
        match File::open(&mbr_bin_path) {
            Ok(file) => {
                let mut reader = BufReader::new(file);
                match PartitionTable::detect(&mut reader) {
                    Ok(table) => {
                        let detected = detect_alignment(&table);
                        partitions = table.partitions();
                        info.push(format!(
                            "Parsed {}: {} partition table, {} partition(s), alignment: {}",
                            mbr_bin_path
                                .file_name()
                                .unwrap_or_default()
                                .to_string_lossy(),
                            table.type_name(),
                            partitions.len(),
                            detected.alignment_type,
                        ));
                        alignment = Some(detected);
                        partition_table = Some(table);
                        true
                    }
                    Err(e) => {
                        warnings.push(format!("Could not parse mbr.bin: {e}"));
                        false
                    }
                }
            }
            Err(e) => {
                warnings.push(format!("Could not open mbr.bin: {e}"));
                false
            }
        }
    } else {
        false
    };

    if parsed_from_mbr {
        // mbr.bin is only 512 bytes; merge in any logical partitions that
        // live in the EBR chain by consulting metadata.
        merge_logical_partitions_from_metadata(&mut partitions, &metadata, &mut info);
    } else {
        partitions = partitions_from_metadata(&metadata);
    }

    Ok(BackupLoadOutcome {
        metadata,
        partition_table,
        alignment,
        partitions,
        warnings,
        info,
    })
}

/// Parse a Clonezilla image folder: sfdisk output, raw MBR, partclone
/// headers (for minimum-size accounting), and any sidecar metadata caches
/// from a previous browse session.
pub fn load_clonezilla(folder: &Path) -> Result<ClonezillaLoadOutcome> {
    let image = load_cz_image(folder)
        .with_context(|| format!("failed to parse Clonezilla image at {}", folder.display()))?;

    let mut warnings: Vec<String> = Vec::new();
    let mut info: Vec<String> = Vec::new();
    info.push(format!(
        "Clonezilla image: {} partition(s), disk: {}, source size: {}",
        image.partitions.len(),
        image.disk_name,
        partition::format_size(image.source_size_bytes),
    ));

    // Parse MBR from the loaded image.
    let mut partition_table: Option<PartitionTable> = None;
    let mut alignment: Option<PartitionAlignment> = None;
    {
        let mut mbr_reader = Cursor::new(&image.mbr_bytes[..]);
        match PartitionTable::detect(&mut mbr_reader) {
            Ok(table) => {
                let detected = detect_alignment(&table);
                info.push(format!(
                    "MBR: {} partition table, alignment: {}",
                    table.type_name(),
                    detected.alignment_type,
                ));
                alignment = Some(detected);
                partition_table = Some(table);
            }
            Err(e) => {
                warnings.push(format!("Could not parse Clonezilla MBR: {e}"));
            }
        }
    }

    // Convert Clonezilla partitions to the GUI's PartitionInfo shape.
    let partitions: Vec<PartitionInfo> = image
        .partitions
        .iter()
        .map(|p| PartitionInfo {
            index: p.index,
            type_name: p.type_name(),
            partition_type_byte: p.partition_type_byte,
            start_lba: p.start_lba,
            size_bytes: p.size_bytes(),
            bootable: p.bootable,
            is_logical: p.is_logical,
            is_extended_container: p.is_extended,
            partition_type_string: None,
            hfs_block_size: None,
            rdb_part_block: None,
            drv_name: None,
        })
        .collect();

    // Compute minimum sizes by reading partclone headers.
    let mut partition_min_sizes: HashMap<usize, u64> = HashMap::new();
    for cz_part in &image.partitions {
        if cz_part.is_extended || cz_part.partclone_files.is_empty() {
            continue;
        }
        match read_partclone_header(&cz_part.partclone_files) {
            Ok(header) => {
                let used = header.used_size();
                if used > 0 && used < cz_part.size_bytes() {
                    partition_min_sizes.insert(cz_part.index, used);
                    info.push(format!(
                        "Partition {} ({}): min {} (used {} of {} blocks, block size {})",
                        cz_part.index,
                        cz_part.device_name,
                        partition::format_size(used),
                        header.used_blocks,
                        header.total_blocks,
                        header.block_size,
                    ));
                }
            }
            Err(e) => {
                warnings.push(format!(
                    "Could not read partclone header for {}: {e}",
                    cz_part.device_name,
                ));
            }
        }
    }

    // Count existing metadata cache files from previous browse sessions.
    let mut cached_metadata_count = 0usize;
    for cz_part in &image.partitions {
        let cache_path = folder.join(format!("_{}.metadata.cache", cz_part.device_name));
        if cache_path.exists() {
            cached_metadata_count += 1;
        }
    }

    Ok(ClonezillaLoadOutcome {
        image,
        partition_table,
        alignment,
        partitions,
        partition_min_sizes,
        cached_metadata_count,
        warnings,
        info,
    })
}

/// Map `BackupMetadata` partition entries into `PartitionInfo`, used as a
/// fallback when `mbr.bin` is missing or unparseable.
fn partitions_from_metadata(meta: &BackupMetadata) -> Vec<PartitionInfo> {
    meta.partitions
        .iter()
        .map(|p| {
            let ptype = if p.partition_type_byte != 0 {
                p.partition_type_byte
            } else {
                infer_fat_type_byte(&p.type_name)
            };
            PartitionInfo {
                index: p.index,
                type_name: p.type_name.clone(),
                partition_type_byte: ptype,
                start_lba: p.start_lba,
                size_bytes: p.original_size_bytes,
                bootable: false,
                is_logical: p.is_logical,
                is_extended_container: false,
                partition_type_string: p.partition_type_string.clone(),
                hfs_block_size: None,
                rdb_part_block: None,
                drv_name: None,
            }
        })
        .collect()
}

/// After parsing `mbr.bin` (which is only 512 bytes and cannot contain the
/// EBR chain), supplement `partitions` with any logical partitions found in
/// `metadata`.
fn merge_logical_partitions_from_metadata(
    partitions: &mut Vec<PartitionInfo>,
    metadata: &BackupMetadata,
    info: &mut Vec<String>,
) {
    if partitions.iter().any(|p| p.is_logical) {
        return;
    }

    let existing_indices: HashSet<usize> = partitions.iter().map(|p| p.index).collect();
    let meta_has_extra = metadata
        .partitions
        .iter()
        .any(|pm| !existing_indices.contains(&pm.index));
    if !meta_has_extra {
        return;
    }

    let mut added = 0usize;
    for pm in &metadata.partitions {
        // Tagged as is_logical in newer backups; for older backups that
        // lack the field, detect by index >= 4 (MBR primary entries are 0-3).
        let is_logical = pm.is_logical || pm.index >= 4;
        if is_logical && !existing_indices.contains(&pm.index) {
            let ptype = if pm.partition_type_byte != 0 {
                pm.partition_type_byte
            } else {
                infer_fat_type_byte(&pm.type_name)
            };
            partitions.push(PartitionInfo {
                index: pm.index,
                type_name: pm.type_name.clone(),
                partition_type_byte: ptype,
                start_lba: pm.start_lba,
                size_bytes: pm.original_size_bytes,
                bootable: false,
                is_logical: true,
                is_extended_container: false,
                partition_type_string: pm.partition_type_string.clone(),
                hfs_block_size: None,
                rdb_part_block: None,
                drv_name: None,
            });
            added += 1;
        }
    }

    if added > 0 {
        info.push(format!(
            "Added {added} logical partition(s) from backup metadata"
        ));
    }
}

/// Best-effort partition type byte inference from a human-readable
/// type name (used for old backups that didn't store the byte).
pub fn infer_fat_type_byte(name: &str) -> u8 {
    let lower = name.to_ascii_lowercase();
    if lower.contains("fat32") {
        0x0C // FAT32 LBA
    } else if lower.contains("fat16") {
        0x06
    } else if lower.contains("fat12") {
        0x01
    } else if lower.contains("fat") {
        0x0C
    } else if lower.contains("ntfs") || lower.contains("exfat") {
        // Both NTFS and exFAT share MBR type 0x07.
        0x07
    } else if lower.contains("linux") || lower.contains("ext") {
        0x83
    } else if lower.contains("hfs") {
        0xAF
    } else {
        0
    }
}
