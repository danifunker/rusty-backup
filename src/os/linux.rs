use std::collections::HashMap;
use std::path::PathBuf;

use crate::device::{DiskDevice, MountedPartition};

/// Enumerate devices using sysinfo, grouping mounted volumes by parent device.
pub fn enumerate_devices() -> Vec<DiskDevice> {
    let disks = sysinfo::Disks::new_with_refreshed_list();

    let mut device_map: HashMap<String, DiskDevice> = HashMap::new();

    for disk in disks.list() {
        let disk_name = disk.name().to_string_lossy().to_string();
        let parent_name = parent_device_name(&disk_name);

        let partition = MountedPartition {
            name: disk_name.clone(),
            mount_point: disk.mount_point().to_path_buf(),
            filesystem: disk.file_system().to_string_lossy().to_string(),
            total_space: disk.total_space(),
            available_space: disk.available_space(),
        };

        let entry = device_map.entry(parent_name.clone()).or_insert_with(|| {
            DiskDevice {
                name: parent_name.clone(),
                path: PathBuf::from(format!("/dev/{parent_name}")),
                size_bytes: 0,
                is_removable: disk.is_removable(),
                is_read_only: disk.is_read_only(),
                is_system: false,
                bus_protocol: String::new(),
                media_name: String::new(),
                partitions: Vec::new(),
            }
        });

        entry.size_bytes = entry.size_bytes.max(
            entry
                .partitions
                .iter()
                .map(|p| p.total_space)
                .sum::<u64>()
                + partition.total_space,
        );
        if disk.is_removable() {
            entry.is_removable = true;
        }
        entry.partitions.push(partition);
    }

    let mut devices: Vec<DiskDevice> = device_map.into_values().collect();
    devices.sort_by(|a, b| a.name.cmp(&b.name));
    devices
}

/// Derive the parent device name from a partition name.
///
/// Examples:
/// - `sda1` -> `sda`
/// - `nvme0n1p1` -> `nvme0n1`
/// - `mmcblk0p1` -> `mmcblk0`
pub fn parent_device_name(partition_name: &str) -> String {
    // nvme0n1p1 -> nvme0n1
    if partition_name.starts_with("nvme") {
        if let Some(p_pos) = partition_name.rfind('p') {
            if partition_name[p_pos + 1..].chars().all(|c| c.is_ascii_digit())
                && !partition_name[p_pos + 1..].is_empty()
                && partition_name[..p_pos].contains('n')
            {
                return partition_name[..p_pos].to_string();
            }
        }
        return partition_name.to_string();
    }

    // mmcblk0p1 -> mmcblk0
    if partition_name.starts_with("mmcblk") {
        if let Some(p_pos) = partition_name.rfind('p') {
            if partition_name[p_pos + 1..].chars().all(|c| c.is_ascii_digit())
                && !partition_name[p_pos + 1..].is_empty()
            {
                return partition_name[..p_pos].to_string();
            }
        }
        return partition_name.to_string();
    }

    // sda1 -> sda, vda1 -> vda, hda1 -> hda, xvda1 -> xvda
    if partition_name.starts_with("sd")
        || partition_name.starts_with("vd")
        || partition_name.starts_with("hd")
        || partition_name.starts_with("xvd")
    {
        return partition_name
            .trim_end_matches(|c: char| c.is_ascii_digit())
            .to_string();
    }

    partition_name.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sd_parent_device() {
        assert_eq!(parent_device_name("sda1"), "sda");
        assert_eq!(parent_device_name("sda2"), "sda");
        assert_eq!(parent_device_name("sdb"), "sdb");
        assert_eq!(parent_device_name("sdz1"), "sdz");
    }

    #[test]
    fn test_nvme_parent_device() {
        assert_eq!(parent_device_name("nvme0n1p1"), "nvme0n1");
        assert_eq!(parent_device_name("nvme0n1p2"), "nvme0n1");
        assert_eq!(parent_device_name("nvme1n1p1"), "nvme1n1");
        assert_eq!(parent_device_name("nvme0n1"), "nvme0n1");
    }

    #[test]
    fn test_mmcblk_parent_device() {
        assert_eq!(parent_device_name("mmcblk0p1"), "mmcblk0");
        assert_eq!(parent_device_name("mmcblk0p2"), "mmcblk0");
        assert_eq!(parent_device_name("mmcblk1p1"), "mmcblk1");
        assert_eq!(parent_device_name("mmcblk0"), "mmcblk0");
    }

    #[test]
    fn test_vd_parent_device() {
        assert_eq!(parent_device_name("vda1"), "vda");
        assert_eq!(parent_device_name("vda"), "vda");
    }

    #[test]
    fn test_xvd_parent_device() {
        assert_eq!(parent_device_name("xvda1"), "xvda");
    }

    #[test]
    fn test_unknown_device() {
        assert_eq!(parent_device_name("something"), "something");
    }
}
