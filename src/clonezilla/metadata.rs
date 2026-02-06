use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};

/// Top-level Clonezilla image metadata.
#[derive(Debug, Clone)]
pub struct ClonezillaImage {
    /// Device prefix (e.g. "sda")
    pub disk_name: String,
    /// CHS geometry
    pub cylinders: u32,
    pub heads: u32,
    pub sectors_per_track: u32,
    /// Raw MBR bytes (512 bytes)
    pub mbr_bytes: [u8; 512],
    /// Hidden data between MBR and first partition
    pub hidden_data_after_mbr: Vec<u8>,
    /// EBR bytes per extended partition (keyed by device name, e.g. "sda2")
    pub ebr_data: HashMap<String, Vec<u8>>,
    /// Partitions in this image
    pub partitions: Vec<ClonezillaPartition>,
    /// Source disk size in bytes (computed from sfdisk data)
    pub source_size_bytes: u64,
    /// Clonezilla image info text (from clonezilla-img file)
    pub image_info: String,
}

/// A single partition within a Clonezilla image.
#[derive(Debug, Clone)]
pub struct ClonezillaPartition {
    /// Partition index (0-based for primary, 4+ for logical)
    pub index: usize,
    /// Device name (e.g. "sda1")
    pub device_name: String,
    /// Start LBA from sfdisk
    pub start_lba: u64,
    /// Size in sectors from sfdisk
    pub size_sectors: u64,
    /// MBR partition type byte
    pub partition_type_byte: u8,
    /// Filesystem type string from dev-fs.list (e.g. "vfat", "ntfs")
    pub filesystem_type: String,
    /// Whether this is an extended container partition
    pub is_extended: bool,
    /// Whether this is a logical partition (inside extended)
    pub is_logical: bool,
    /// Whether this partition is bootable
    pub bootable: bool,
    /// Sorted list of partclone image split files
    pub partclone_files: Vec<PathBuf>,
}

impl ClonezillaPartition {
    /// Size in bytes.
    pub fn size_bytes(&self) -> u64 {
        self.size_sectors * 512
    }

    /// Human-readable type name.
    pub fn type_name(&self) -> String {
        let fs = if self.filesystem_type.is_empty() {
            partition_type_name(self.partition_type_byte)
        } else {
            self.filesystem_type.to_uppercase()
        };
        if self.is_extended {
            format!("{fs} (extended)")
        } else {
            fs
        }
    }
}

/// Check if a folder is a Clonezilla image by looking for the marker file.
pub fn is_clonezilla_image(folder: &Path) -> bool {
    folder.join("clonezilla-img").exists()
}

/// Load and parse a complete Clonezilla image from a folder.
pub fn load(folder: &Path) -> Result<ClonezillaImage> {
    // Read clonezilla-img info
    let image_info = std::fs::read_to_string(folder.join("clonezilla-img"))
        .context("failed to read clonezilla-img")?;

    // Parse disk name
    let disk_name = parse_disk(folder)?;

    // Parse partition list
    let part_names = parse_parts(folder)?;

    // Parse sfdisk data
    let sfdisk_entries = parse_sfdisk(folder, &disk_name)?;

    // Parse dev-fs.list
    let fs_map = parse_dev_fs_list(folder)?;

    // Parse CHS geometry
    let (cylinders, heads, sectors_per_track) = parse_chs(folder, &disk_name)?;

    // Read MBR
    let mbr_path = folder.join(format!("{disk_name}-mbr"));
    let mbr_bytes = if mbr_path.exists() {
        let data = std::fs::read(&mbr_path).context("failed to read MBR file")?;
        let mut buf = [0u8; 512];
        let copy_len = data.len().min(512);
        buf[..copy_len].copy_from_slice(&data[..copy_len]);
        buf
    } else {
        bail!("MBR file not found: {}", mbr_path.display());
    };

    // Read hidden data after MBR
    let hidden_path = folder.join(format!("{disk_name}-hidden-data-after-mbr"));
    let hidden_data_after_mbr = if hidden_path.exists() {
        std::fs::read(&hidden_path).context("failed to read hidden-data-after-mbr")?
    } else {
        Vec::new()
    };

    // Read EBR files
    let mut ebr_data = HashMap::new();
    for entry in std::fs::read_dir(folder).context("failed to read backup folder")? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name.ends_with("-ebr") {
            let dev_name = name.trim_end_matches("-ebr").to_string();
            let data = std::fs::read(entry.path())
                .with_context(|| format!("failed to read EBR file: {name}"))?;
            ebr_data.insert(dev_name, data);
        }
    }

    // Build partitions
    let mut partitions = Vec::new();
    let mut source_size_bytes: u64 = 0;

    for sf_entry in &sfdisk_entries {
        let end_bytes = (sf_entry.start + sf_entry.size) * 512;
        if end_bytes > source_size_bytes {
            source_size_bytes = end_bytes;
        }

        let is_extended = matches!(sf_entry.ptype, 0x05 | 0x0F | 0x85);
        let is_logical = sf_entry.index >= 4;
        let has_data = part_names.contains(&sf_entry.device_name);

        // Map device name to filesystem type
        let fs_type = fs_map
            .get(&format!("/dev/{}", sf_entry.device_name))
            .cloned()
            .unwrap_or_default();

        // Find partclone files
        let partclone_files = if has_data && !is_extended {
            find_partclone_files(folder, &sf_entry.device_name)?
        } else {
            Vec::new()
        };

        partitions.push(ClonezillaPartition {
            index: sf_entry.index,
            device_name: sf_entry.device_name.clone(),
            start_lba: sf_entry.start,
            size_sectors: sf_entry.size,
            partition_type_byte: sf_entry.ptype,
            filesystem_type: fs_type,
            is_extended,
            is_logical,
            bootable: sf_entry.bootable,
            partclone_files,
        });
    }

    Ok(ClonezillaImage {
        disk_name,
        cylinders,
        heads,
        sectors_per_track,
        mbr_bytes,
        hidden_data_after_mbr,
        ebr_data,
        partitions,
        source_size_bytes,
        image_info,
    })
}

/// Read the `disk` file to get the device prefix (e.g. "sda").
fn parse_disk(folder: &Path) -> Result<String> {
    let content =
        std::fs::read_to_string(folder.join("disk")).context("failed to read disk file")?;
    let name = content.trim().to_string();
    if name.is_empty() {
        bail!("disk file is empty");
    }
    Ok(name)
}

/// Read the `parts` file to get the list of partition device names.
fn parse_parts(folder: &Path) -> Result<Vec<String>> {
    let content =
        std::fs::read_to_string(folder.join("parts")).context("failed to read parts file")?;
    Ok(content.split_whitespace().map(String::from).collect())
}

/// An entry parsed from the sfdisk output file.
struct SfdiskEntry {
    device_name: String,
    index: usize,
    start: u64,
    size: u64,
    ptype: u8,
    bootable: bool,
}

/// Parse the `<disk>-pt.sf` sfdisk file.
fn parse_sfdisk(folder: &Path, disk_name: &str) -> Result<Vec<SfdiskEntry>> {
    let sf_path = folder.join(format!("{disk_name}-pt.sf"));
    let content = std::fs::read_to_string(&sf_path)
        .with_context(|| format!("failed to read {}", sf_path.display()))?;

    let mut entries = Vec::new();

    for line in content.lines() {
        let line = line.trim();
        // Lines look like: /dev/sda1 : start=          63, size=     4096512, type=6, bootable
        if !line.starts_with("/dev/") || !line.contains("start=") {
            continue;
        }

        // Split on " : "
        let parts: Vec<&str> = line.splitn(2, " : ").collect();
        if parts.len() < 2 {
            continue;
        }

        let dev_path = parts[0].trim();
        let device_name = dev_path
            .strip_prefix("/dev/")
            .unwrap_or(dev_path)
            .to_string();

        // Parse partition index from device name (e.g. "sda1" -> 0, "sda5" -> 4)
        let index = extract_partition_index(&device_name, disk_name);

        let attrs = parts[1];
        let start = parse_sfdisk_value(attrs, "start")?;
        let size = parse_sfdisk_value(attrs, "size")?;
        let ptype = parse_sfdisk_hex_value(attrs, "type")?;
        let bootable = attrs.contains("bootable");

        entries.push(SfdiskEntry {
            device_name,
            index,
            start,
            size,
            ptype,
            bootable,
        });
    }

    if entries.is_empty() {
        bail!("no partition entries found in {}", sf_path.display());
    }

    Ok(entries)
}

/// Extract a numeric value from sfdisk output (e.g. "start=63" from "start=          63, size=...").
fn parse_sfdisk_value(attrs: &str, key: &str) -> Result<u64> {
    for part in attrs.split(',') {
        let part = part.trim();
        if let Some(val_str) = part.strip_prefix(&format!("{key}=")) {
            return val_str
                .trim()
                .parse::<u64>()
                .with_context(|| format!("invalid {key} value: {val_str}"));
        }
    }
    bail!("missing {key} in sfdisk line: {attrs}")
}

/// Extract a hex or decimal type value from sfdisk output.
fn parse_sfdisk_hex_value(attrs: &str, key: &str) -> Result<u8> {
    for part in attrs.split(',') {
        let part = part.trim();
        if let Some(val_str) = part.strip_prefix(&format!("{key}=")) {
            let val_str = val_str.trim();
            let value = if let Some(hex) = val_str.strip_prefix("0x") {
                u8::from_str_radix(hex, 16)
                    .with_context(|| format!("invalid hex {key} value: {val_str}"))?
            } else if let Ok(v) = val_str.parse::<u8>() {
                v
            } else {
                // sfdisk may output bare hex like "type=f" without 0x prefix
                u8::from_str_radix(val_str, 16)
                    .with_context(|| format!("invalid {key} value: {val_str}"))?
            };
            return Ok(value);
        }
    }
    bail!("missing {key} in sfdisk line: {attrs}")
}

/// Extract partition index from device name. Linux numbering:
/// sda1..sda4 are primary (index 0-3), sda5+ are logical (index 4+).
fn extract_partition_index(device_name: &str, disk_name: &str) -> usize {
    let num_str = device_name.strip_prefix(disk_name).unwrap_or("");
    let num: usize = num_str.parse().unwrap_or(0);
    if num == 0 {
        0
    } else {
        num - 1
    }
}

/// Parse `dev-fs.list` to map device paths to filesystem type strings.
fn parse_dev_fs_list(folder: &Path) -> Result<HashMap<String, String>> {
    let path = folder.join("dev-fs.list");
    if !path.exists() {
        return Ok(HashMap::new());
    }

    let content = std::fs::read_to_string(&path).context("failed to read dev-fs.list")?;
    let mut map = HashMap::new();

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        // Format: /dev/sda1 vfat 2G
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 2 {
            map.insert(parts[0].to_string(), parts[1].to_string());
        }
    }

    Ok(map)
}

/// Parse `<disk>-chs.sf` for CHS geometry.
fn parse_chs(folder: &Path, disk_name: &str) -> Result<(u32, u32, u32)> {
    let chs_path = folder.join(format!("{disk_name}-chs.sf"));
    if !chs_path.exists() {
        return Ok((0, 0, 0));
    }

    let content = std::fs::read_to_string(&chs_path).context("failed to read CHS file")?;
    let mut cylinders = 0u32;
    let mut heads = 0u32;
    let mut sectors = 0u32;

    for line in content.lines() {
        let line = line.trim();
        if let Some(val) = line.strip_prefix("cylinders=") {
            cylinders = val.parse().unwrap_or(0);
        } else if let Some(val) = line.strip_prefix("heads=") {
            heads = val.parse().unwrap_or(0);
        } else if let Some(val) = line.strip_prefix("sectors=") {
            sectors = val.parse().unwrap_or(0);
        }
    }

    Ok((cylinders, heads, sectors))
}

/// Find and sort partclone image files for a given partition.
/// Clonezilla names them like: sda1.vfat-ptcl-img.zst.aa, sda1.vfat-ptcl-img.zst.ab, ...
fn find_partclone_files(folder: &Path, part_name: &str) -> Result<Vec<PathBuf>> {
    let prefix = format!("{part_name}.");
    let suffix = "-ptcl-img.";

    let mut files: Vec<PathBuf> = Vec::new();
    for entry in std::fs::read_dir(folder).context("failed to read backup folder")? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with(&prefix) && name.contains(suffix) {
            files.push(entry.path());
        }
    }

    // Sort by filename to get .aa, .ab, .ac, ... order
    files.sort_by(|a, b| {
        a.file_name()
            .unwrap_or_default()
            .cmp(b.file_name().unwrap_or_default())
    });

    Ok(files)
}

/// Map a partition type byte to a human-readable name.
fn partition_type_name(ptype: u8) -> String {
    match ptype {
        0x01 => "FAT12".to_string(),
        0x04 | 0x06 | 0x0E => "FAT16".to_string(),
        0x05 | 0x0F | 0x85 => "Extended".to_string(),
        0x07 => "NTFS/exFAT".to_string(),
        0x0B | 0x0C => "FAT32".to_string(),
        0x82 => "Linux Swap".to_string(),
        0x83 => "Linux".to_string(),
        _ => format!("0x{ptype:02X}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_partition_index() {
        assert_eq!(extract_partition_index("sda1", "sda"), 0);
        assert_eq!(extract_partition_index("sda2", "sda"), 1);
        assert_eq!(extract_partition_index("sda5", "sda"), 4);
        assert_eq!(extract_partition_index("sda7", "sda"), 6);
    }

    #[test]
    fn test_parse_sfdisk_value() {
        let line = "start=          63, size=     4096512, type=6, bootable";
        assert_eq!(parse_sfdisk_value(line, "start").unwrap(), 63);
        assert_eq!(parse_sfdisk_value(line, "size").unwrap(), 4096512);
    }

    #[test]
    fn test_parse_sfdisk_hex_value() {
        let line = "start=          63, size=     4096512, type=6, bootable";
        assert_eq!(parse_sfdisk_hex_value(line, "type").unwrap(), 6);

        let line2 = "start=63, size=100, type=0x0f";
        assert_eq!(parse_sfdisk_hex_value(line2, "type").unwrap(), 0x0f);

        // Bare hex without 0x prefix (e.g. "type=f")
        let line3 = "start=63, size=100, type=f";
        assert_eq!(parse_sfdisk_hex_value(line3, "type").unwrap(), 0x0f);

        let line4 = "start=63, size=100, type=83";
        assert_eq!(parse_sfdisk_hex_value(line4, "type").unwrap(), 83);
    }

    #[test]
    fn test_partition_type_name() {
        assert_eq!(partition_type_name(0x06), "FAT16");
        assert_eq!(partition_type_name(0x0C), "FAT32");
        assert_eq!(partition_type_name(0x07), "NTFS/exFAT");
        assert_eq!(partition_type_name(0x0F), "Extended");
    }

    #[test]
    fn test_is_clonezilla_image() {
        // Non-existent folder
        assert!(!is_clonezilla_image(Path::new("/nonexistent")));
    }
}
