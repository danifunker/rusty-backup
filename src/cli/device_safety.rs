//! Device-write safety checks for `rb-cli restore --device` and
//! `rb-cli write`. The CLI never tears down a live filesystem — it
//! refuses to write to a mounted device or the system boot disk
//! unless the user has explicitly opted in.
//!
//! Three layers:
//!
//! 1. **Path classification.** `looks_like_device_path` distinguishes
//!    real device paths (`/dev/sdX`, `/dev/diskN`, `\\.\PhysicalDriveN`)
//!    from mount points (`/Volumes/Foo`, `/mnt/backup`, `C:\...`).
//!    Writing to a mount point is treated as plain file I/O — no
//!    safety prompts.
//!
//! 2. **Preflight.** `preflight` enumerates host devices, finds the
//!    one matching the target path, and refuses to proceed if it's
//!    the system disk (unless `allow_system_disk`) or has mounted
//!    partitions. Returns the matched `DiskDevice` so callers can
//!    print an enriched summary.
//!
//! 3. **Summary printer.** `print_safety_summary` writes a standard
//!    one-block stderr summary used by both `restore` and `write`.
//!    Always printed — `--yes` skips the prompt but never the summary.

use anyhow::{bail, Result};
use std::path::Path;

use crate::cli::logging::log_stderr;
use crate::device::{enumerate_devices, DiskDevice};
use crate::partition::format_size;

/// True if `path` looks like a raw block-device path (per the OS
/// conventions documented in `docs/cli-todo.md` §"Device-write safety").
/// False for mount points and ordinary files.
pub fn looks_like_device_path(path: &Path) -> bool {
    let s = path.to_string_lossy();
    // Unix device files: /dev/...
    if s.starts_with("/dev/") {
        return true;
    }
    // Windows physical drive: \\.\PhysicalDriveN  (also \\?\PhysicalDriveN)
    let lower = s.to_ascii_lowercase();
    if lower.starts_with(r"\\.\physicaldrive") || lower.starts_with(r"\\?\physicaldrive") {
        return true;
    }
    false
}

/// Result of `preflight`. Always Ok if the target is safe to write to;
/// errors carry a human-readable explanation for the user.
pub struct PreflightOk {
    /// The matched `DiskDevice` from `crate::device::enumerate_devices()`.
    /// `None` when the device path exists but isn't in the enumeration
    /// (rare — happens for kernel devices the OS layer doesn't surface).
    pub device: Option<DiskDevice>,
}

/// Run device-write safety checks. Use *before* opening the device.
///
/// Errors when:
/// - `device_path` is a mount point (refuse with a "use device path" message)
/// - The target is the system disk and `allow_system_disk` is false
/// - The target has any mounted partition (refuse with mount list)
pub fn preflight(device_path: &Path, allow_system_disk: bool) -> Result<PreflightOk> {
    if !looks_like_device_path(device_path) {
        // Mount points sneak in as plain file paths. Be strict here:
        // if the user passed --device / used the `write` verb the
        // intent is clearly "raw block device," so a mount-point path
        // is almost certainly a mistake.
        if is_mount_point(device_path) {
            bail!(
                "{} is a mount point, not a block device. Pass the underlying \
                 device path (Linux: /dev/sdX; macOS: /dev/diskN; Windows: \"\\\\.\\PhysicalDriveN\") \
                 to write to the device, or drop the device flag to overwrite \
                 the file at that path.",
                device_path.display()
            );
        }
        bail!(
            "{} does not look like a device path. Expected /dev/* (Linux/macOS) \
             or \\\\.\\PhysicalDriveN (Windows).",
            device_path.display()
        );
    }

    let device = enumerate_devices()
        .into_iter()
        .find(|d| d.path == device_path);

    if let Some(d) = &device {
        if d.is_system && !allow_system_disk {
            bail!(
                "{} is the system disk. Pass --write-to-system-disk to override \
                 (this is almost always a bad idea).",
                device_path.display()
            );
        }
        let mounted: Vec<String> = d
            .partitions
            .iter()
            .filter(|p| !p.mount_point.as_os_str().is_empty())
            .map(|p| format!("{} at {}", p.name, p.mount_point.display()))
            .collect();
        if !mounted.is_empty() {
            bail!(
                "{} has mounted partitions; unmount them and retry:\n  - {}",
                device_path.display(),
                mounted.join("\n  - ")
            );
        }
    }
    Ok(PreflightOk { device })
}

/// Print the standard safety summary to stderr. `image_size` is bytes
/// from the source. `target_label` is the call-site description ("write
/// target", "restore target", …) used in the header line.
pub fn print_safety_summary(
    target_label: &str,
    source_display: &str,
    target_path: &Path,
    image_size: u64,
    device: Option<&DiskDevice>,
) {
    log_stderr(format!("=== rb-cli {target_label} — safety summary ==="));
    log_stderr(format!("  source:   {source_display}"));
    log_stderr(format!(
        "  size:     {} ({} bytes)",
        format_size(image_size),
        image_size
    ));
    log_stderr(format!("  target:   {}", target_path.display()));
    match device {
        Some(d) => {
            log_stderr(format!(
                "  device:   {} ({}) [{}]",
                d.media_name,
                format_size(d.size_bytes),
                d.bus_protocol
            ));
            if d.is_system {
                log_stderr("  WARN:     target is the system disk (--write-to-system-disk active)");
            }
            // Mounted partitions are refused in preflight, but if the
            // caller bypassed preflight we still want to surface them.
            for p in &d.partitions {
                if !p.mount_point.as_os_str().is_empty() {
                    log_stderr(format!(
                        "  mounted:  partition {:?} at {}",
                        p.name,
                        p.mount_point.display()
                    ));
                }
            }
        }
        None => {
            log_stderr("  (device not in os enumeration — caller must verify)");
        }
    }
    log_stderr("======================================");
}

/// Does this path look like an existing mount point? Cheap heuristic
/// only used to produce a friendlier error message — when in doubt we
/// fall through to the generic "not a device path" branch.
fn is_mount_point(path: &Path) -> bool {
    // Quick paths everybody knows.
    let s = path.to_string_lossy();
    if s.starts_with("/Volumes/") || s.starts_with("/mnt/") || s.starts_with("/media/") {
        return true;
    }
    // Otherwise consult the OS enumeration — any partition mount_point match.
    enumerate_devices()
        .iter()
        .flat_map(|d| d.partitions.iter())
        .any(|p| p.mount_point == path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn device_path_recognition() {
        assert!(looks_like_device_path(&PathBuf::from("/dev/sda")));
        assert!(looks_like_device_path(&PathBuf::from("/dev/disk2")));
        assert!(looks_like_device_path(&PathBuf::from("/dev/rdisk2")));
        assert!(looks_like_device_path(&PathBuf::from("/dev/nvme0n1")));
        assert!(looks_like_device_path(&PathBuf::from(
            r"\\.\PhysicalDrive0"
        )));
        assert!(looks_like_device_path(&PathBuf::from(
            r"\\?\PhysicalDrive1"
        )));

        assert!(!looks_like_device_path(&PathBuf::from("/Volumes/Foo")));
        assert!(!looks_like_device_path(&PathBuf::from("/mnt/backup")));
        assert!(!looks_like_device_path(&PathBuf::from("/tmp/disk.img")));
        assert!(!looks_like_device_path(&PathBuf::from(
            r"C:\Users\disk.img"
        )));
    }

    #[test]
    fn mount_point_heuristic() {
        assert!(is_mount_point(&PathBuf::from("/Volumes/Music")));
        assert!(is_mount_point(&PathBuf::from("/mnt/data")));
        assert!(is_mount_point(&PathBuf::from("/media/usb")));
        assert!(!is_mount_point(&PathBuf::from("/tmp/foo.img")));
    }
}
