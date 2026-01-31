use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{bail, Context, Result};
use plist::Value;

use crate::device::{DiskDevice, MountedPartition};
use super::ElevatedSource;

/// Enumerate devices using `diskutil list -plist` and `diskutil info -plist`.
pub fn enumerate_devices() -> Vec<DiskDevice> {
    let whole_disks = match get_whole_disks() {
        Some(disks) => disks,
        None => return Vec::new(),
    };

    let partitions_map = get_disk_partitions_map().unwrap_or_default();

    let mut devices = Vec::new();
    for disk_name in &whole_disks {
        if let Some(device) = build_device(disk_name, &partitions_map) {
            devices.push(device);
        }
    }

    devices.sort_by(|a, b| a.name.cmp(&b.name));
    devices
}

/// Run `diskutil list -plist` and extract the WholeDisks array.
fn get_whole_disks() -> Option<Vec<String>> {
    let output = Command::new("diskutil")
        .args(["list", "-plist"])
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let plist = Value::from_reader(std::io::Cursor::new(&output.stdout)).ok()?;
    let dict = plist.as_dictionary()?;

    let whole_disks = dict.get("WholeDisks")?.as_array()?;
    Some(
        whole_disks
            .iter()
            .filter_map(|v| v.as_string().map(String::from))
            .collect(),
    )
}

/// Parse `diskutil list -plist` to build a map of whole disk -> partition/volume info.
fn get_disk_partitions_map() -> Option<HashMap<String, Vec<DiskPartInfo>>> {
    let output = Command::new("diskutil")
        .args(["list", "-plist"])
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let plist = Value::from_reader(std::io::Cursor::new(&output.stdout)).ok()?;
    let dict = plist.as_dictionary()?;
    let all_disks = dict.get("AllDisksAndPartitions")?.as_array()?;

    let mut map: HashMap<String, Vec<DiskPartInfo>> = HashMap::new();

    for disk_entry in all_disks {
        let disk_dict = disk_entry.as_dictionary()?;
        let dev_id = disk_dict
            .get("DeviceIdentifier")?
            .as_string()?
            .to_string();

        let mut parts = Vec::new();

        // Collect from Partitions array (GPT/MBR disks)
        if let Some(partitions) = disk_dict.get("Partitions").and_then(|v| v.as_array()) {
            for p in partitions {
                if let Some(info) = extract_part_info(p) {
                    parts.push(info);
                }
            }
        }

        // Collect from APFSVolumes array (APFS containers)
        if let Some(volumes) = disk_dict.get("APFSVolumes").and_then(|v| v.as_array()) {
            for v in volumes {
                if let Some(info) = extract_part_info(v) {
                    parts.push(info);
                }
            }
        }

        // The disk itself may have a mount point (e.g., a plain ISO)
        if let Some(mount_point) = disk_dict.get("MountPoint").and_then(|v| v.as_string()) {
            if !mount_point.is_empty() {
                parts.push(DiskPartInfo {
                    device_identifier: dev_id.clone(),
                    mount_point: Some(mount_point.to_string()),
                    volume_name: disk_dict
                        .get("VolumeName")
                        .and_then(|v| v.as_string())
                        .unwrap_or("")
                        .to_string(),
                    size: disk_dict
                        .get("Size")
                        .and_then(|v| v.as_unsigned_integer())
                        .unwrap_or(0),
                    content: disk_dict
                        .get("Content")
                        .and_then(|v| v.as_string())
                        .unwrap_or("")
                        .to_string(),
                });
            }
        }

        map.insert(dev_id, parts);
    }

    Some(map)
}

struct DiskPartInfo {
    device_identifier: String,
    mount_point: Option<String>,
    #[allow(dead_code)]
    volume_name: String,
    size: u64,
    content: String,
}

fn extract_part_info(value: &Value) -> Option<DiskPartInfo> {
    let dict = value.as_dictionary()?;
    Some(DiskPartInfo {
        device_identifier: dict
            .get("DeviceIdentifier")?
            .as_string()?
            .to_string(),
        mount_point: dict
            .get("MountPoint")
            .and_then(|v| v.as_string())
            .map(String::from),
        volume_name: dict
            .get("VolumeName")
            .and_then(|v| v.as_string())
            .unwrap_or("")
            .to_string(),
        size: dict
            .get("Size")
            .and_then(|v| v.as_unsigned_integer())
            .unwrap_or(0),
        content: dict
            .get("Content")
            .and_then(|v| v.as_string())
            .unwrap_or("")
            .to_string(),
    })
}

/// Build a DiskDevice by querying `diskutil info -plist <disk>` for the whole disk,
/// and using the partitions map for volume info.
fn build_device(
    disk_name: &str,
    partitions_map: &HashMap<String, Vec<DiskPartInfo>>,
) -> Option<DiskDevice> {
    let output = Command::new("diskutil")
        .args(["info", "-plist", disk_name])
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let plist = Value::from_reader(std::io::Cursor::new(&output.stdout)).ok()?;
    let dict = plist.as_dictionary()?;

    let device_node = dict.get("DeviceNode")?.as_string()?.to_string();
    let size = dict
        .get("Size")
        .and_then(|v| v.as_unsigned_integer())
        .unwrap_or(0);
    let removable = dict
        .get("RemovableMediaOrExternalDevice")
        .and_then(|v| v.as_boolean())
        .unwrap_or(false);
    let internal = dict
        .get("Internal")
        .and_then(|v| v.as_boolean())
        .unwrap_or(false);
    let writable = dict
        .get("WritableMedia")
        .and_then(|v| v.as_boolean())
        .unwrap_or(true);
    let bus_protocol = dict
        .get("BusProtocol")
        .and_then(|v| v.as_string())
        .unwrap_or("")
        .to_string();
    let media_name = dict
        .get("MediaName")
        .and_then(|v| v.as_string())
        .unwrap_or("")
        .to_string();
    let virtual_or_physical = dict
        .get("VirtualOrPhysical")
        .and_then(|v| v.as_string())
        .unwrap_or("Physical")
        .to_string();

    // Skip virtual/disk image devices
    if virtual_or_physical == "Virtual" || bus_protocol == "Disk Image" {
        return None;
    }

    // Collect mounted partitions from the partitions map
    let mut mounted = Vec::new();
    if let Some(parts) = partitions_map.get(disk_name) {
        for part in parts {
            if let Some(mp) = &part.mount_point {
                if !mp.is_empty() {
                    mounted.push(MountedPartition {
                        name: part.device_identifier.clone(),
                        mount_point: PathBuf::from(mp),
                        filesystem: part.content.clone(),
                        total_space: part.size,
                        available_space: 0,
                    });
                }
            }
        }
    }

    let is_system = internal && !removable;

    Some(DiskDevice {
        name: disk_name.to_string(),
        path: PathBuf::from(&device_node),
        size_bytes: size,
        is_removable: removable,
        is_read_only: !writable,
        is_system,
        bus_protocol,
        media_name,
        partitions: mounted,
    })
}

/// Open a target device for writing, unmounting it first.
///
/// If normal open fails with permission denied, uses `osascript` to run
/// with admin privileges via the native macOS authentication dialog.
pub fn open_target_for_writing(path: &Path) -> Result<File> {
    let path_str = path.to_string_lossy();

    // Unmount all partitions on the disk first
    // Convert /dev/rdiskN to diskN for diskutil
    let disk_name = if path_str.starts_with("/dev/r") {
        &path_str[6..]
    } else if path_str.starts_with("/dev/") {
        &path_str[5..]
    } else {
        &path_str
    };

    // Unmount the entire disk (force to handle busy volumes)
    let unmount_result = Command::new("diskutil")
        .args(["unmountDisk", "force", disk_name])
        .output();

    if let Ok(output) = unmount_result {
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // Not fatal — the disk might not be mounted
            eprintln!("diskutil unmountDisk warning: {}", stderr.trim());
        }
    }

    // Use the raw device (/dev/rdiskN) for faster unbuffered writes
    let raw_device = if path_str.starts_with("/dev/disk") {
        format!("/dev/r{}", &path_str[5..])
    } else {
        path_str.to_string()
    };

    // Try normal open first
    match std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&raw_device)
    {
        Ok(file) => return Ok(file),
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
            // Fall through to elevation attempt
        }
        Err(e) => {
            return Err(anyhow::anyhow!(e)
                .context(format!("cannot open {} for writing", raw_device)));
        }
    }

    // Use osascript to open with elevated privileges.
    // We open via a helper dd that writes nothing but holds the fd,
    // but actually the simplest approach is just to re-try the unmount
    // with admin privileges and then open.
    let script = format!(
        "do shell script \"diskutil unmountDisk force {disk_name}\" with administrator privileges"
    );

    let output = Command::new("osascript")
        .arg("-e")
        .arg(&script)
        .output()
        .context("failed to launch osascript for elevated unmount")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        if stderr.contains("User canceled") || stderr.contains("-128") {
            bail!("User cancelled the administrator authentication request");
        }
    }

    // Try opening again after elevated unmount
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&raw_device)
        .with_context(|| format!(
            "cannot open {} for writing (even after elevated unmount)", raw_device
        ))
}

/// Open a source device or image file for reading, requesting elevated
/// privileges via the native macOS authentication dialog if needed.
///
/// When the path is a `/dev/disk*` device and normal open fails with
/// permission denied, this function:
/// 1. Presents the macOS admin credentials dialog
/// 2. Runs `dd` with elevated privileges to create a temporary raw image
///    (using `/dev/rdiskN` for faster unbuffered reads)
/// 3. Returns a handle to the temp image that auto-deletes on drop
pub fn open_source_for_reading(path: &Path) -> Result<ElevatedSource> {
    // Try normal open first
    match File::open(path) {
        Ok(file) => {
            return Ok(ElevatedSource {
                file,
                temp_path: None,
            });
        }
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
            // Fall through to elevation attempt
        }
        Err(e) => {
            return Err(anyhow::anyhow!(e)
                .context(format!("cannot open {}", path.display())));
        }
    }

    // Only attempt elevation for device paths
    let path_str = path.to_string_lossy();
    if !path_str.starts_with("/dev/") {
        bail!(
            "Permission denied: {}. Run the application with elevated privileges to access this file.",
            path.display()
        );
    }

    // Use /dev/rdiskN (raw character device) for faster unbuffered reads
    let source_device = if path_str.starts_with("/dev/disk") {
        format!("/dev/r{}", &path_str[5..])
    } else {
        path_str.to_string()
    };

    // Generate temp file path
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let device_stem = path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy();
    let temp_path = PathBuf::from(format!("/tmp/rusty-backup-{device_stem}-{timestamp}.raw"));
    let temp_str = temp_path.to_string_lossy();

    // Use osascript to run dd with administrator privileges.
    // This presents the native macOS authentication dialog.
    let script = format!(
        "do shell script \"dd if={source_device} of={temp_str} bs=1m\" with administrator privileges"
    );

    let output = Command::new("osascript")
        .arg("-e")
        .arg(&script)
        .output()
        .context("failed to launch osascript for elevated device access")?;

    if !output.status.success() {
        // Clean up partial temp file if it exists
        let _ = std::fs::remove_file(&temp_path);

        let stderr = String::from_utf8_lossy(&output.stderr);
        if stderr.contains("User canceled")
            || stderr.contains("user canceled")
            || stderr.contains("-128")
        {
            bail!("User cancelled the administrator authentication request");
        }
        bail!(
            "Failed to read device with elevated privileges: {}",
            stderr.trim()
        );
    }

    let file = File::open(&temp_path)
        .with_context(|| format!("failed to open temporary device image: {}", temp_path.display()))?;

    Ok(ElevatedSource {
        file,
        temp_path: Some(temp_path),
    })
}
