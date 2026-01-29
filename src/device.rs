use std::collections::HashMap;
use std::path::PathBuf;

/// Represents a physical disk device with its mounted partitions.
#[derive(Debug, Clone)]
pub struct DiskDevice {
    pub name: String,
    pub path: PathBuf,
    pub size_bytes: u64,
    pub is_removable: bool,
    pub is_read_only: bool,
    pub partitions: Vec<MountedPartition>,
}

impl DiskDevice {
    pub fn display_name(&self) -> String {
        let size = crate::partition::format_size(self.size_bytes);
        let removable = if self.is_removable { " [removable]" } else { "" };
        format!("{} ({size}){removable}", self.path.display())
    }
}

/// A mounted partition (volume) on a disk device.
#[derive(Debug, Clone)]
pub struct MountedPartition {
    pub name: String,
    pub mount_point: PathBuf,
    pub filesystem: String,
    pub total_space: u64,
    pub available_space: u64,
}

/// Enumerate disk devices using sysinfo.
///
/// Groups mounted volumes by their parent device. For example, on macOS
/// `disk2s1` and `disk2s2` are grouped under `/dev/disk2`.
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
            let device_path = device_path_from_name(&parent_name);
            DiskDevice {
                name: parent_name.clone(),
                path: device_path,
                size_bytes: 0,
                is_removable: disk.is_removable(),
                is_read_only: disk.is_read_only(),
                partitions: Vec::new(),
            }
        });

        // Accumulate total size from partitions
        entry.size_bytes = entry.size_bytes.max(
            entry
                .partitions
                .iter()
                .map(|p| p.total_space)
                .sum::<u64>()
                + partition.total_space,
        );
        // If any partition is removable, mark the device as removable
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
/// - macOS: `disk2s1` -> `disk2`
/// - Linux: `sda1` -> `sda`, `nvme0n1p1` -> `nvme0n1`, `mmcblk0p1` -> `mmcblk0`
pub fn parent_device_name(partition_name: &str) -> String {
    // macOS: disk2s1 -> disk2 (strip 's' + digits)
    if partition_name.starts_with("disk") {
        if let Some(s_pos) = partition_name.rfind('s') {
            if s_pos > 4 && partition_name[s_pos + 1..].chars().all(|c| c.is_ascii_digit()) {
                return partition_name[..s_pos].to_string();
            }
        }
        return partition_name.to_string();
    }

    // Linux: nvme0n1p1 -> nvme0n1 (strip 'p' + digits after nvme pattern)
    if partition_name.starts_with("nvme") {
        if let Some(p_pos) = partition_name.rfind('p') {
            // Make sure there's a digit after 'p' and 'n' before it
            if partition_name[p_pos + 1..].chars().all(|c| c.is_ascii_digit())
                && !partition_name[p_pos + 1..].is_empty()
                && partition_name[..p_pos].contains('n')
            {
                return partition_name[..p_pos].to_string();
            }
        }
        return partition_name.to_string();
    }

    // Linux: mmcblk0p1 -> mmcblk0 (strip 'p' + digits after mmcblk pattern)
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

    // Linux: sda1 -> sda, vda1 -> vda (strip trailing digits from sd/vd/hd devices)
    if partition_name.starts_with("sd")
        || partition_name.starts_with("vd")
        || partition_name.starts_with("hd")
        || partition_name.starts_with("xvd")
    {
        let stripped = partition_name.trim_end_matches(|c: char| c.is_ascii_digit());
        return stripped.to_string();
    }

    // Fallback: return as-is
    partition_name.to_string()
}

/// Convert a device name to its full path.
fn device_path_from_name(name: &str) -> PathBuf {
    if cfg!(target_os = "macos") {
        PathBuf::from(format!("/dev/{name}"))
    } else if cfg!(target_os = "linux") {
        PathBuf::from(format!("/dev/{name}"))
    } else if cfg!(target_os = "windows") {
        PathBuf::from(format!("\\\\.\\{name}"))
    } else {
        PathBuf::from(format!("/dev/{name}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_macos_parent_device() {
        assert_eq!(parent_device_name("disk2s1"), "disk2");
        assert_eq!(parent_device_name("disk2s2"), "disk2");
        assert_eq!(parent_device_name("disk0s1"), "disk0");
        assert_eq!(parent_device_name("disk10s3"), "disk10");
        assert_eq!(parent_device_name("disk2"), "disk2");
    }

    #[test]
    fn test_linux_sd_parent_device() {
        assert_eq!(parent_device_name("sda1"), "sda");
        assert_eq!(parent_device_name("sda2"), "sda");
        assert_eq!(parent_device_name("sdb"), "sdb");
        assert_eq!(parent_device_name("sdz1"), "sdz");
    }

    #[test]
    fn test_linux_nvme_parent_device() {
        assert_eq!(parent_device_name("nvme0n1p1"), "nvme0n1");
        assert_eq!(parent_device_name("nvme0n1p2"), "nvme0n1");
        assert_eq!(parent_device_name("nvme1n1p1"), "nvme1n1");
        assert_eq!(parent_device_name("nvme0n1"), "nvme0n1");
    }

    #[test]
    fn test_linux_mmcblk_parent_device() {
        assert_eq!(parent_device_name("mmcblk0p1"), "mmcblk0");
        assert_eq!(parent_device_name("mmcblk0p2"), "mmcblk0");
        assert_eq!(parent_device_name("mmcblk1p1"), "mmcblk1");
        assert_eq!(parent_device_name("mmcblk0"), "mmcblk0");
    }

    #[test]
    fn test_linux_vd_parent_device() {
        assert_eq!(parent_device_name("vda1"), "vda");
        assert_eq!(parent_device_name("vda"), "vda");
    }

    #[test]
    fn test_linux_xvd_parent_device() {
        assert_eq!(parent_device_name("xvda1"), "xvda");
    }

    #[test]
    fn test_unknown_device() {
        assert_eq!(parent_device_name("something"), "something");
    }
}
