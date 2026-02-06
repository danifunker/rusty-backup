use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use nix::mount::{umount2, MntFlags};

use crate::device::{DiskDevice, MountedPartition};

/// A parsed entry from `/proc/self/mountinfo`.
struct MountInfoEntry {
    mount_point: String,
    fs_type: String,
    _source: String,
}

/// Read a sysfs attribute file, returning the trimmed contents or an empty
/// string on any failure.
fn read_sysfs_attr(path: &Path) -> String {
    fs::read_to_string(path)
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

/// Returns `true` for block device names that represent real physical devices
/// (as opposed to loop, ram, device-mapper, etc.).
fn is_physical_block_device(name: &str) -> bool {
    !name.starts_with("loop")
        && !name.starts_with("ram")
        && !name.starts_with("dm-")
        && !name.starts_with("zram")
        && !name.starts_with("sr")
        && !name.starts_with("fd")
        && !name.starts_with("nbd")
}

/// Unescape octal sequences in mountinfo fields (e.g. `\040` -> space).
fn unescape_mountinfo(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            // Try to read 3 octal digits
            let mut octal = String::with_capacity(3);
            for _ in 0..3 {
                if let Some(&next) = chars.as_str().chars().collect::<Vec<_>>().first() {
                    if next.is_ascii_digit() && next < '8' {
                        octal.push(next);
                        chars.next();
                    } else {
                        break;
                    }
                }
            }
            if octal.len() == 3 {
                if let Ok(byte) = u8::from_str_radix(&octal, 8) {
                    result.push(byte as char);
                } else {
                    result.push('\\');
                    result.push_str(&octal);
                }
            } else {
                result.push('\\');
                result.push_str(&octal);
            }
        } else {
            result.push(c);
        }
    }
    result
}

/// Parse the contents of `/proc/self/mountinfo` into a map from device path
/// (e.g. `/dev/sda1`) to mount information.
///
/// Format (space-separated):
///   id parent major:minor root mount_point options ... - fs_type source super_opts
fn parse_mountinfo_from_str(content: &str) -> HashMap<String, MountInfoEntry> {
    let mut map = HashMap::new();

    for line in content.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();

        // Find the `-` separator
        let dash_pos = match fields.iter().position(|f| *f == "-") {
            Some(p) => p,
            None => continue,
        };

        // Need at least: mount_point at index 4, and fs_type + source after dash
        if fields.len() < 5 || dash_pos + 2 >= fields.len() {
            continue;
        }

        let mount_point = unescape_mountinfo(fields[4]);
        let fs_type = fields[dash_pos + 1].to_string();
        let source = fields[dash_pos + 2].to_string();

        // Only track entries with real block device sources
        if source.starts_with("/dev/") {
            map.insert(
                source.clone(),
                MountInfoEntry {
                    mount_point,
                    fs_type,
                    _source: source,
                },
            );
        }
    }

    map
}

/// Detect bus protocol by resolving the sysfs device symlink and checking
/// path components for known bus keywords.
fn detect_bus_protocol(sysfs_dev: &Path) -> String {
    let device_path = sysfs_dev.join("device");
    let resolved = match fs::canonicalize(&device_path) {
        Ok(p) => p.to_string_lossy().to_string(),
        Err(_) => return String::new(),
    };

    if resolved.contains("/usb") {
        "USB".to_string()
    } else if resolved.contains("/nvme") {
        "NVMe".to_string()
    } else if resolved.contains("/mmc") {
        "MMC/SD".to_string()
    } else if resolved.contains("/ata") || resolved.contains("/sata") {
        "SATA".to_string()
    } else {
        String::new()
    }
}

/// Enumerate devices using sysfs and /proc/self/mountinfo.
pub fn enumerate_devices() -> Vec<DiskDevice> {
    let mountinfo_content = fs::read_to_string("/proc/self/mountinfo").unwrap_or_default();
    let mounts = parse_mountinfo_from_str(&mountinfo_content);

    let block_dir = Path::new("/sys/block");
    let entries = match fs::read_dir(block_dir) {
        Ok(e) => e,
        Err(_) => return Vec::new(),
    };

    let mut devices = Vec::new();

    for entry in entries.flatten() {
        let dev_name = entry.file_name().to_string_lossy().to_string();

        if !is_physical_block_device(&dev_name) {
            continue;
        }

        let sysfs_dev = block_dir.join(&dev_name);

        // Read device attributes
        let size_sectors: u64 = read_sysfs_attr(&sysfs_dev.join("size"))
            .parse()
            .unwrap_or(0);
        let size_bytes = size_sectors * 512;

        let is_removable = read_sysfs_attr(&sysfs_dev.join("removable")) == "1";
        let is_read_only = read_sysfs_attr(&sysfs_dev.join("ro")) == "1";
        let media_name = read_sysfs_attr(&sysfs_dev.join("device/model"));
        let bus_protocol = detect_bus_protocol(&sysfs_dev);

        // Discover partitions
        let mut partitions = Vec::new();
        let mut is_system = false;

        if let Ok(sub_entries) = fs::read_dir(&sysfs_dev) {
            for sub in sub_entries.flatten() {
                let part_name = sub.file_name().to_string_lossy().to_string();
                // A subdirectory is a partition if it contains a `partition` file
                if sub.path().join("partition").exists() {
                    let dev_path = format!("/dev/{part_name}");

                    let (mount_point, filesystem, total_space, available_space) =
                        if let Some(mi) = mounts.get(&dev_path) {
                            // Mounted - get space via statvfs
                            let (total, avail) = statvfs_space(&mi.mount_point);
                            (
                                PathBuf::from(&mi.mount_point),
                                mi.fs_type.clone(),
                                total,
                                avail,
                            )
                        } else {
                            // Not mounted - use sysfs partition size
                            let part_sectors: u64 = read_sysfs_attr(&sub.path().join("size"))
                                .parse()
                                .unwrap_or(0);
                            (PathBuf::new(), String::new(), part_sectors * 512, 0)
                        };

                    if mount_point == Path::new("/") {
                        is_system = true;
                    }

                    partitions.push(MountedPartition {
                        name: part_name,
                        mount_point,
                        filesystem,
                        total_space,
                        available_space,
                    });
                }
            }
        }

        partitions.sort_by(|a, b| a.name.cmp(&b.name));

        devices.push(DiskDevice {
            name: dev_name.clone(),
            path: PathBuf::from(format!("/dev/{dev_name}")),
            size_bytes,
            is_removable,
            is_read_only,
            is_system,
            bus_protocol,
            media_name,
            partitions,
        });
    }

    devices.sort_by(|a, b| a.name.cmp(&b.name));
    devices
}

/// Get total and available space for a mount point using `statvfs64`.
fn statvfs_space(mount_point: &str) -> (u64, u64) {
    use std::ffi::CString;

    let c_path = match CString::new(mount_point) {
        Ok(p) => p,
        Err(_) => return (0, 0),
    };

    unsafe {
        let mut stat: libc::statvfs64 = std::mem::zeroed();
        if libc::statvfs64(c_path.as_ptr(), &mut stat) == 0 {
            let total = stat.f_blocks as u64 * stat.f_frsize as u64;
            let avail = stat.f_bavail as u64 * stat.f_frsize as u64;
            (total, avail)
        } else {
            (0, 0)
        }
    }
}

/// Open a target device for writing on Linux.
///
/// Reads `/proc/self/mountinfo` to find mounted partitions on this device,
/// unmounts them with `umount2(MNT_DETACH)`, then opens the device for
/// read+write access.
pub fn open_target_for_writing(path: &Path) -> Result<File> {
    let path_str = path.to_string_lossy();

    if path_str.starts_with("/dev/") {
        let device_name = path_str.trim_start_matches("/dev/");
        let parent = parent_device_name(device_name);

        // Read mountinfo to find actually-mounted partitions
        let mountinfo_content = fs::read_to_string("/proc/self/mountinfo").unwrap_or_default();
        let mounts = parse_mountinfo_from_str(&mountinfo_content);

        // Unmount any mounted partitions belonging to this device
        for (dev_path, mi) in &mounts {
            let dev_part_name = dev_path.trim_start_matches("/dev/");
            if parent_device_name(dev_part_name) == parent {
                let _ = umount2(mi.mount_point.as_str(), MntFlags::MNT_DETACH);
            }
        }
    }

    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .with_context(|| format!("cannot open {} for writing", path.display()))
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
            if partition_name[p_pos + 1..]
                .chars()
                .all(|c| c.is_ascii_digit())
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
            if partition_name[p_pos + 1..]
                .chars()
                .all(|c| c.is_ascii_digit())
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

    #[test]
    fn test_parse_mountinfo() {
        let content = "\
22 1 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw
23 22 8:2 / /home rw,relatime shared:2 - ext4 /dev/sda2 rw
30 22 0:30 / /run/user/1000 rw,nosuid,nodev - tmpfs tmpfs rw
35 22 8:17 / /mnt/usb\\040drive rw,relatime shared:5 - vfat /dev/sdb1 rw
";
        let map = parse_mountinfo_from_str(content);
        assert_eq!(map.len(), 3); // tmpfs excluded (source not /dev/)

        let sda1 = map.get("/dev/sda1").unwrap();
        assert_eq!(sda1.mount_point, "/");
        assert_eq!(sda1.fs_type, "ext4");

        let sda2 = map.get("/dev/sda2").unwrap();
        assert_eq!(sda2.mount_point, "/home");

        let sdb1 = map.get("/dev/sdb1").unwrap();
        assert_eq!(sdb1.mount_point, "/mnt/usb drive");
        assert_eq!(sdb1.fs_type, "vfat");
    }

    #[test]
    fn test_unescape_mountinfo() {
        assert_eq!(unescape_mountinfo("hello"), "hello");
        assert_eq!(unescape_mountinfo("/mnt/usb\\040drive"), "/mnt/usb drive");
        assert_eq!(unescape_mountinfo("/mnt/a\\040b\\011c"), "/mnt/a b\tc");
        assert_eq!(unescape_mountinfo("no\\escape"), "no\\escape");
    }

    #[test]
    fn test_is_physical_block_device() {
        assert!(is_physical_block_device("sda"));
        assert!(is_physical_block_device("nvme0n1"));
        assert!(is_physical_block_device("mmcblk0"));
        assert!(is_physical_block_device("vda"));
        assert!(!is_physical_block_device("loop0"));
        assert!(!is_physical_block_device("loop1"));
        assert!(!is_physical_block_device("ram0"));
        assert!(!is_physical_block_device("dm-0"));
        assert!(!is_physical_block_device("zram0"));
        assert!(!is_physical_block_device("sr0"));
        assert!(!is_physical_block_device("fd0"));
        assert!(!is_physical_block_device("nbd0"));
    }
}

// ---------------------------------------------------------------------------
// Privileged disk access implementation
// ---------------------------------------------------------------------------

use std::fs::OpenOptions;
use std::io::{ErrorKind, Read, Seek, Write};
use std::os::unix::process::CommandExt;
use std::process::Command;

use crate::privileged::{AccessStatus, DiskHandle, PrivilegedDiskAccess};

/// Linux implementation of privileged disk access.
///
/// Uses direct file I/O when running as root. If not root, the app should
/// be relaunched with pkexec via `relaunch_with_elevation()`.
pub struct LinuxDiskAccess {
    open_handles: HashMap<u64, File>,
    next_handle: u64,
}

impl LinuxDiskAccess {
    pub fn new() -> Result<Self> {
        Ok(Self {
            open_handles: HashMap::new(),
            next_handle: 1,
        })
    }
}

impl PrivilegedDiskAccess for LinuxDiskAccess {
    fn check_status(&self) -> Result<AccessStatus> {
        // Check if we're running as root
        if nix::unistd::geteuid().is_root() {
            Ok(AccessStatus::Ready)
        } else {
            Ok(AccessStatus::NeedsElevation)
        }
    }

    fn open_disk_read(&mut self, path: &Path) -> Result<DiskHandle> {
        match File::open(path) {
            Ok(file) => {
                let handle = self.next_handle;
                self.next_handle += 1;
                self.open_handles.insert(handle, file);
                Ok(DiskHandle(handle))
            }
            Err(e) if e.kind() == ErrorKind::PermissionDenied => {
                anyhow::bail!(
                    "Permission denied accessing {}. Please restart with elevated privileges.",
                    path.display()
                )
            }
            Err(e) => Err(e).context(format!("Failed to open {} for reading", path.display())),
        }
    }

    fn open_disk_write(&mut self, path: &Path) -> Result<DiskHandle> {
        // Unmount volumes first (reuse existing unmount logic)
        let path_str = path.to_string_lossy();
        if path_str.starts_with("/dev/") {
            let device_name = path_str.trim_start_matches("/dev/");
            let parent = parent_device_name(device_name);

            let mountinfo_content = fs::read_to_string("/proc/self/mountinfo").unwrap_or_default();
            let mounts = parse_mountinfo_from_str(&mountinfo_content);

            for (dev_path, mi) in &mounts {
                let dev_part_name = dev_path.trim_start_matches("/dev/");
                if parent_device_name(dev_part_name) == parent {
                    let _ = umount2(mi.mount_point.as_str(), MntFlags::MNT_DETACH);
                }
            }
        }

        // Open for writing
        match OpenOptions::new().read(true).write(true).open(path) {
            Ok(file) => {
                let handle = self.next_handle;
                self.next_handle += 1;
                self.open_handles.insert(handle, file);
                Ok(DiskHandle(handle))
            }
            Err(e) if e.kind() == ErrorKind::PermissionDenied => {
                anyhow::bail!(
                    "Permission denied accessing {}. Please restart with elevated privileges.",
                    path.display()
                )
            }
            Err(e) => Err(e).context(format!("Failed to open {} for writing", path.display())),
        }
    }

    fn read_sectors(&mut self, handle: DiskHandle, lba: u64, count: u32) -> Result<Vec<u8>> {
        let file = self
            .open_handles
            .get_mut(&handle.0)
            .ok_or_else(|| anyhow::anyhow!("Invalid disk handle: {}", handle.0))?;

        let offset = lba * 512;
        let size = count as usize * 512;

        file.seek(std::io::SeekFrom::Start(offset))?;

        let mut buffer = vec![0u8; size];
        file.read_exact(&mut buffer)?;

        Ok(buffer)
    }

    fn write_sectors(&mut self, handle: DiskHandle, lba: u64, data: &[u8]) -> Result<()> {
        if data.len() % 512 != 0 {
            anyhow::bail!(
                "Data size must be a multiple of 512 bytes, got {}",
                data.len()
            );
        }

        let file = self
            .open_handles
            .get_mut(&handle.0)
            .ok_or_else(|| anyhow::anyhow!("Invalid disk handle: {}", handle.0))?;

        let offset = lba * 512;
        file.seek(std::io::SeekFrom::Start(offset))?;
        file.write_all(data)?;

        Ok(())
    }

    fn close_disk(&mut self, handle: DiskHandle) -> Result<()> {
        self.open_handles
            .remove(&handle.0)
            .ok_or_else(|| anyhow::anyhow!("Invalid disk handle: {}", handle.0))?;
        Ok(())
    }
}

/// Relaunch the application with pkexec for elevated privileges.
///
/// This function never returns on success - the current process is replaced.
pub fn relaunch_with_elevation() -> Result<()> {
    let current_exe = std::env::current_exe()?;
    let args: Vec<String> = std::env::args().skip(1).collect();

    // Check if pkexec is available
    let pkexec_check = Command::new("which")
        .arg("pkexec")
        .output()
        .context("Failed to check for pkexec")?;

    if !pkexec_check.status.success() {
        anyhow::bail!(
            "pkexec not found. Please install policykit-1 or run manually: sudo {}",
            current_exe.display()
        );
    }

    // Preserve environment variables since pkexec strips the environment.
    // Display vars allow the elevated process to connect to X11/Wayland.
    // User identity vars allow resolving the real user's home/config paths.
    let passthrough_vars = [
        "DISPLAY",
        "WAYLAND_DISPLAY",
        "WAYLAND_SOCKET",
        "XAUTHORITY",
        "XDG_RUNTIME_DIR",
        "HOME",
    ];
    let mut env_args: Vec<String> = Vec::new();
    for var in &passthrough_vars {
        if let Ok(val) = std::env::var(var) {
            env_args.push(format!("{}={}", var, val));
        }
    }

    // Capture real user identity before elevation replaces the process
    if let Ok(user) = std::env::var("USER") {
        env_args.push(format!("SUDO_USER={}", user));
    }
    env_args.push(format!("SUDO_UID={}", nix::unistd::getuid()));
    env_args.push(format!("SUDO_GID={}", nix::unistd::getgid()));

    // Use "env" to inject variables, --keep-cwd to preserve working directory.
    // Exec replaces current process - never returns on success
    let err = Command::new("pkexec")
        .arg("env")
        .args(&env_args)
        .arg(&current_exe)
        .args(&args)
        .exec();

    Err(anyhow::anyhow!(err).context("Failed to relaunch with pkexec"))
}

/// Returns the real (non-root) user's home directory when running elevated.
///
/// When the app is relaunched via pkexec, `HOME` and `SUDO_USER` are passed
/// through so we can resolve the original user's home. Falls back to
/// `dirs::home_dir()` if not elevated or if env vars aren't set.
pub fn real_user_home() -> Option<PathBuf> {
    if !nix::unistd::geteuid().is_root() {
        return dirs::home_dir();
    }
    // HOME was passed through pkexec env wrapper
    if let Ok(home) = std::env::var("HOME") {
        let p = PathBuf::from(&home);
        if p != PathBuf::from("/root") && p.exists() {
            return Some(p);
        }
    }
    // Fallback: resolve SUDO_USER via /home/<user>
    if let Ok(user) = std::env::var("SUDO_USER") {
        let home = PathBuf::from(format!("/home/{}", user));
        if home.exists() {
            return Some(home);
        }
    }
    dirs::home_dir()
}

/// Set permissive umask when running elevated so created files are accessible
/// to the real user. No-op if not root.
///
/// Sets umask to 000 so files are created with mode 666 and directories with
/// mode 777. The real security boundary is the pkexec prompt itself.
pub fn set_permissive_umask_if_elevated() {
    if nix::unistd::geteuid().is_root() {
        nix::sys::stat::umask(nix::sys::stat::Mode::empty());
    }
}
