use std::path::PathBuf;

/// Represents a physical disk device with its mounted partitions.
#[derive(Debug, Clone)]
pub struct DiskDevice {
    pub name: String,
    pub path: PathBuf,
    pub size_bytes: u64,
    pub is_removable: bool,
    pub is_read_only: bool,
    pub is_system: bool,
    pub bus_protocol: String,
    pub media_name: String,
    pub partitions: Vec<MountedPartition>,
}

impl DiskDevice {
    pub fn display_name(&self) -> String {
        let size = crate::partition::format_size(self.size_bytes);
        let mut tags = Vec::new();
        if self.is_removable {
            tags.push("removable");
        }
        if self.is_system {
            tags.push("system");
        }
        let suffix = if tags.is_empty() {
            String::new()
        } else {
            format!(" [{}]", tags.join(", "))
        };
        let description = if self.media_name.is_empty() {
            String::new()
        } else {
            format!(" - {}", self.media_name)
        };
        format!("{} ({size}){description}{suffix}", self.path.display())
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

/// Enumerate disk devices using platform-specific methods.
///
/// On macOS, uses IOKit/DiskArbitration for proper BSD device identifiers.
/// On Linux, reads sysfs (`/sys/block/`) and `/proc/self/mountinfo`.
/// On Windows, probes `\\.\PhysicalDriveN` via `CreateFileW`/`DeviceIoControl`.
pub fn enumerate_devices() -> Vec<DiskDevice> {
    crate::os::enumerate_devices()
}
