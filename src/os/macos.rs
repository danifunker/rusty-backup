mod sudo;

use std::ffi::{c_void, CString};
use std::fs::File;
use std::os::unix::io::FromRawFd;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{bail, Context, Result};

pub use sudo::{request_app_elevation, sudo_execute};

use libc::statfs;
use objc2_core_foundation::{
    kCFRunLoopDefaultMode, CFBoolean, CFDictionary, CFMutableDictionary, CFNumber, CFRunLoop,
    CFString, CFURL,
};

use objc2_disk_arbitration::{
    kDADiskDescriptionDeviceInternalKey, kDADiskDescriptionDeviceModelKey,
    kDADiskDescriptionDeviceProtocolKey, kDADiskDescriptionMediaBSDNameKey,
    kDADiskDescriptionMediaRemovableKey, kDADiskDescriptionMediaSizeKey,
    kDADiskDescriptionMediaWritableKey, kDADiskDescriptionVolumeKindKey,
    kDADiskDescriptionVolumePathKey, kDADiskUnmountOptionForce, kDADiskUnmountOptionWhole, DADisk,
    DADiskUnmountCallback, DADissenter, DASession,
};
use objc2_io_kit::{
    kIOMainPortDefault, IOIteratorNext, IOObjectRelease, IORegistryEntryCreateCFProperties,
    IOServiceGetMatchingServices, IOServiceMatching,
};

use super::ElevatedSource;
use crate::device::{DiskDevice, MountedPartition};

// macOS ioctl constants for getting device size
// DKIOCGETBLOCKSIZE = _IOR('d', 24, u32) = 0x40046418
// DKIOCGETBLOCKCOUNT = _IOR('d', 25, u64) = 0x40086419
const DKIOCGETBLOCKSIZE: libc::c_ulong = 0x40046418;
const DKIOCGETBLOCKCOUNT: libc::c_ulong = 0x40086419;

/// Get the size of a macOS block device using ioctl.
///
/// `seek(SeekFrom::End(0))` returns 0 for macOS device files, so we must
/// use DKIOCGETBLOCKCOUNT × DKIOCGETBLOCKSIZE to get the actual size.
pub fn get_device_size(file: &std::fs::File) -> Option<u64> {
    use std::os::unix::io::AsRawFd;
    let fd = file.as_raw_fd();

    let mut block_size: u32 = 0;
    let mut block_count: u64 = 0;

    unsafe {
        if libc::ioctl(fd, DKIOCGETBLOCKSIZE, &mut block_size) != 0 {
            return None;
        }
        if libc::ioctl(fd, DKIOCGETBLOCKCOUNT, &mut block_count) != 0 {
            return None;
        }
    }

    if block_size == 0 {
        return None;
    }

    Some(block_count * block_size as u64)
}

// ---------------------------------------------------------------------------
// Helper: extract typed values from an untyped CFDictionary
// ---------------------------------------------------------------------------

/// Extract a `CFString` value from an untyped `CFDictionary` using a known key.
unsafe fn dict_get_string(dict: &CFDictionary, key: &CFString) -> Option<String> {
    let raw = unsafe { dict.value((key as *const CFString).cast()) };
    if raw.is_null() {
        return None;
    }
    let cf_str = unsafe { &*(raw as *const CFString) };
    Some(cf_str.to_string())
}

/// Extract a `CFBoolean` value from an untyped `CFDictionary`.
unsafe fn dict_get_bool(dict: &CFDictionary, key: &CFString) -> Option<bool> {
    let raw = unsafe { dict.value((key as *const CFString).cast()) };
    if raw.is_null() {
        return None;
    }
    let cf_bool = unsafe { &*(raw as *const CFBoolean) };
    Some(cf_bool.as_bool())
}

/// Extract a `CFNumber` value as `i64` from an untyped `CFDictionary`.
unsafe fn dict_get_number(dict: &CFDictionary, key: &CFString) -> Option<i64> {
    let raw = unsafe { dict.value((key as *const CFString).cast()) };
    if raw.is_null() {
        return None;
    }
    let cf_num = unsafe { &*(raw as *const CFNumber) };
    cf_num.as_i64()
}

/// Extract a `CFURL` value and convert to a `PathBuf`.
unsafe fn dict_get_url_path(dict: &CFDictionary, key: &CFString) -> Option<PathBuf> {
    let raw = unsafe { dict.value((key as *const CFString).cast()) };
    if raw.is_null() {
        return None;
    }
    let cf_url = unsafe { &*(raw as *const CFURL) };
    cf_url.to_file_path()
}

// ---------------------------------------------------------------------------
// IOKit enumeration of IOMedia entries
// ---------------------------------------------------------------------------

/// Information gathered from a single IOMedia entry via IOKit.
struct IOMediaEntry {
    bsd_name: String,
    is_whole: bool,
    size: u64,
}

/// Enumerate all IOMedia entries via IOKit and return their basic properties.
fn iokit_enumerate_media() -> Vec<IOMediaEntry> {
    let mut entries = Vec::new();

    unsafe {
        let matching = IOServiceMatching(c"IOMedia".as_ptr());
        let matching = match matching {
            Some(m) => m,
            None => return entries,
        };

        let mut iterator: u32 = 0;
        let kr = IOServiceGetMatchingServices(
            kIOMainPortDefault,
            // IOServiceGetMatchingServices consumes the matching dict (takes CFRetained).
            // We need to convert CFRetained<CFMutableDictionary> to Option<CFRetained<CFDictionary>>.
            Some(objc2_core_foundation::CFRetained::cast_unchecked(matching)),
            &mut iterator,
        );
        if kr != 0 {
            return entries;
        }

        loop {
            let entry = IOIteratorNext(iterator);
            if entry == 0 {
                break;
            }

            // Get all properties for this IOMedia entry
            let mut props_ptr: *mut CFMutableDictionary = ptr::null_mut();
            let kr = IORegistryEntryCreateCFProperties(
                entry,
                &mut props_ptr,
                None, // kCFAllocatorDefault
                0,
            );

            if kr == 0 && !props_ptr.is_null() {
                // Wrap in CFRetained for automatic release
                let props = objc2_core_foundation::CFRetained::<CFMutableDictionary>::from_raw(
                    NonNull::new_unchecked(props_ptr),
                );

                // Access as untyped CFDictionary
                let dict: &CFDictionary = &props;

                let bsd_name_key = CFString::from_static_str("BSD Name");
                let whole_key = CFString::from_static_str("Whole");
                let size_key = CFString::from_static_str("Size");

                if let Some(bsd_name) = dict_get_string(dict, &bsd_name_key) {
                    let is_whole = dict_get_bool(dict, &whole_key).unwrap_or(false);
                    let size = dict_get_number(dict, &size_key).unwrap_or(0) as u64;

                    entries.push(IOMediaEntry {
                        bsd_name,
                        is_whole,
                        size,
                    });
                }
            }

            IOObjectRelease(entry);
        }

        IOObjectRelease(iterator);
    }

    entries
}

// ---------------------------------------------------------------------------
// DiskArbitration helpers
// ---------------------------------------------------------------------------

/// Query DiskArbitration for a disk's description dictionary.
fn da_disk_description(session: &DASession, bsd_name: &str) -> Option<DiskDescription> {
    let c_name = CString::new(bsd_name).ok()?;
    unsafe {
        let disk = DADisk::from_bsd_name(None, session, NonNull::new(c_name.as_ptr() as *mut _)?)?;
        let desc = disk.description()?;
        let dict: &CFDictionary = &desc;

        let media_name = dict_get_string(dict, kDADiskDescriptionDeviceModelKey)
            .unwrap_or_default()
            .trim()
            .to_string();
        let bus_protocol =
            dict_get_string(dict, kDADiskDescriptionDeviceProtocolKey).unwrap_or_default();
        let is_removable =
            dict_get_bool(dict, kDADiskDescriptionMediaRemovableKey).unwrap_or(false);
        let is_writable = dict_get_bool(dict, kDADiskDescriptionMediaWritableKey).unwrap_or(true);
        let is_internal = dict_get_bool(dict, kDADiskDescriptionDeviceInternalKey).unwrap_or(false);
        let size = dict_get_number(dict, kDADiskDescriptionMediaSizeKey).unwrap_or(0) as u64;
        let bsd = dict_get_string(dict, kDADiskDescriptionMediaBSDNameKey)
            .unwrap_or_else(|| bsd_name.to_string());
        let volume_kind = dict_get_string(dict, kDADiskDescriptionVolumeKindKey);
        let volume_path = dict_get_url_path(dict, kDADiskDescriptionVolumePathKey);

        Some(DiskDescription {
            bsd_name: bsd,
            media_name,
            bus_protocol,
            is_removable,
            is_writable,
            is_internal,
            size,
            volume_kind,
            volume_path,
        })
    }
}

struct DiskDescription {
    bsd_name: String,
    media_name: String,
    bus_protocol: String,
    is_removable: bool,
    is_writable: bool,
    is_internal: bool,
    size: u64,
    volume_kind: Option<String>,
    volume_path: Option<PathBuf>,
}

// ---------------------------------------------------------------------------
// statfs helper for available space
// ---------------------------------------------------------------------------

fn get_available_space(mount_point: &Path) -> u64 {
    let c_path = match CString::new(mount_point.to_string_lossy().as_bytes()) {
        Ok(p) => p,
        Err(_) => return 0,
    };
    unsafe {
        let mut stat: statfs = std::mem::zeroed();
        if libc::statfs(c_path.as_ptr(), &mut stat) == 0 {
            stat.f_bavail * stat.f_bsize as u64
        } else {
            0
        }
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Enumerate devices using IOKit for disk discovery and DiskArbitration for properties.
pub fn enumerate_devices() -> Vec<DiskDevice> {
    let session = unsafe { DASession::new(None) };
    let session = match session {
        Some(s) => s,
        None => return Vec::new(),
    };

    let io_entries = iokit_enumerate_media();

    // Collect whole-disk BSD names
    let whole_disks: Vec<&IOMediaEntry> = io_entries.iter().filter(|e| e.is_whole).collect();

    let mut devices = Vec::new();
    for whole in &whole_disks {
        let desc = match da_disk_description(&session, &whole.bsd_name) {
            Some(d) => d,
            None => continue,
        };

        // Skip virtual / disk-image devices
        if desc.bus_protocol == "Disk Image" {
            continue;
        }

        // Collect partitions (non-whole IOMedia entries whose BSD name starts with this disk's name)
        let mut partitions = Vec::new();
        for part_entry in &io_entries {
            if part_entry.is_whole {
                continue;
            }
            // e.g. "disk2s1" starts with "disk2" — match partition to parent
            if !part_entry.bsd_name.starts_with(&whole.bsd_name) {
                continue;
            }
            // Ensure it's a direct child: after the prefix there should be 's' + digits
            let suffix = &part_entry.bsd_name[whole.bsd_name.len()..];
            if !suffix.starts_with('s') {
                continue;
            }

            if let Some(part_desc) = da_disk_description(&session, &part_entry.bsd_name) {
                if let Some(ref mount_point) = part_desc.volume_path {
                    let mp_str = mount_point.to_string_lossy();
                    if !mp_str.is_empty() {
                        let available = get_available_space(mount_point);
                        partitions.push(MountedPartition {
                            name: part_desc.bsd_name,
                            mount_point: mount_point.clone(),
                            filesystem: part_desc.volume_kind.unwrap_or_default(),
                            total_space: part_entry.size,
                            available_space: available,
                        });
                    }
                }
            }
        }

        let is_system = desc.is_internal && !desc.is_removable;

        devices.push(DiskDevice {
            name: whole.bsd_name.clone(),
            path: PathBuf::from(format!("/dev/{}", whole.bsd_name)),
            size_bytes: desc.size.max(whole.size), // prefer DA size, fallback to IOKit
            is_removable: desc.is_removable,
            is_read_only: !desc.is_writable,
            is_system,
            bus_protocol: desc.bus_protocol,
            media_name: desc.media_name,
            partitions,
        });
    }

    devices.sort_by(|a, b| a.name.cmp(&b.name));
    devices
}

// ---------------------------------------------------------------------------
// DiskArbitration unmount
// ---------------------------------------------------------------------------

/// Synchronously unmount all volumes on a disk via DiskArbitration.
///
/// Returns `Ok(())` on success, or an error if the unmount fails or times out.
fn da_unmount_disk(bsd_name: &str) -> Result<()> {
    let session =
        unsafe { DASession::new(None) }.context("failed to create DiskArbitration session")?;

    let c_name = CString::new(bsd_name).context("invalid BSD name")?;
    let disk = unsafe {
        DADisk::from_bsd_name(
            None,
            &session,
            NonNull::new(c_name.as_ptr() as *mut _).unwrap(),
        )
    }
    .context(format!("failed to create DADisk for {}", bsd_name))?;

    // Schedule the session on the current run loop so the callback fires
    let run_loop = CFRunLoop::current().context("failed to get current CFRunLoop")?;
    let mode = unsafe { kCFRunLoopDefaultMode.unwrap() };
    unsafe { session.schedule_with_run_loop(&run_loop, mode) };

    // Shared state for the callback
    static UNMOUNT_DONE: AtomicBool = AtomicBool::new(false);
    static UNMOUNT_OK: AtomicBool = AtomicBool::new(false);
    UNMOUNT_DONE.store(false, Ordering::SeqCst);
    UNMOUNT_OK.store(false, Ordering::SeqCst);

    unsafe extern "C-unwind" fn unmount_callback(
        _disk: NonNull<DADisk>,
        dissenter: *const DADissenter,
        _context: *mut c_void,
    ) {
        // NULL dissenter means success
        UNMOUNT_OK.store(dissenter.is_null(), Ordering::SeqCst);
        UNMOUNT_DONE.store(true, Ordering::SeqCst);
        if let Some(rl) = CFRunLoop::current() {
            rl.stop();
        }
    }

    let options = kDADiskUnmountOptionForce | kDADiskUnmountOptionWhole;
    let callback: DADiskUnmountCallback = Some(unmount_callback);

    unsafe {
        disk.unmount(options, callback, ptr::null_mut());
    }

    // Run the run loop with a timeout to wait for the callback
    let timeout_secs = 10.0;
    CFRunLoop::run_in_mode(Some(mode), timeout_secs, false);

    unsafe { session.unschedule_from_run_loop(&run_loop, mode) };

    if !UNMOUNT_DONE.load(Ordering::SeqCst) {
        bail!(
            "unmount of {} timed out after {:.0}s",
            bsd_name,
            timeout_secs
        );
    }
    if !UNMOUNT_OK.load(Ordering::SeqCst) {
        bail!("DiskArbitration failed to unmount {}", bsd_name);
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// O_EXLOCK exclusive device access
// ---------------------------------------------------------------------------

/// BSD exclusive lock flag — prevents any other open (including kernel mounts)
/// while the fd is held. This is how Balena Etcher prevents macOS from
/// mounting partitions mid-write.
const O_EXLOCK: libc::c_int = 0x0020;

/// macOS fcntl command to bypass the buffer cache (equivalent to O_DIRECT on Linux).
const F_NOCACHE: libc::c_int = 48;

/// Open a device path with `O_EXLOCK` for exclusive access.
///
/// Uses `libc::open()` directly because Rust's `OpenOptions` doesn't expose
/// BSD-specific flags. Sets `F_NOCACHE` for direct I/O. Retries up to 5 times
/// on EBUSY (unmount may not have fully propagated in the kernel).
fn open_with_exlock(path: &str, flags: libc::c_int) -> Result<File> {
    let c_path = CString::new(path).context("invalid device path")?;
    let mut last_err = None;

    for attempt in 0..5 {
        if attempt > 0 {
            std::thread::sleep(std::time::Duration::from_millis(500));
            eprintln!("Retrying open of {} (attempt {}/5)...", path, attempt + 1);
        }

        let fd = unsafe { libc::open(c_path.as_ptr(), flags | O_EXLOCK) };
        if fd >= 0 {
            // Set F_NOCACHE for direct I/O (bypass buffer cache)
            unsafe { libc::fcntl(fd, F_NOCACHE, 1) };
            let file = unsafe { File::from_raw_fd(fd) };
            eprintln!("Opened {} with O_EXLOCK (attempt {})", path, attempt + 1);
            return Ok(file);
        }

        let err = std::io::Error::last_os_error();
        let raw = err.raw_os_error().unwrap_or(0);

        if err.kind() == std::io::ErrorKind::PermissionDenied {
            // Don't retry — caller will handle elevation
            return Err(anyhow::anyhow!(err).context(format!("permission denied opening {}", path)));
        }

        if raw != libc::EBUSY {
            // Non-transient error — don't retry
            return Err(anyhow::anyhow!(err).context(format!("cannot open {}", path)));
        }

        last_err = Some(err);
    }

    Err(anyhow::anyhow!(last_err.unwrap())
        .context(format!("cannot open {} (EBUSY after 5 attempts)", path)))
}

/// Extract the BSD disk name from a device path like `/dev/diskN` or `/dev/rdiskN`.
fn bsd_name_from_path(path: &Path) -> &str {
    let path_str = path.to_str().unwrap_or("");
    if let Some(stripped) = path_str.strip_prefix("/dev/r") {
        stripped
    } else if let Some(stripped) = path_str.strip_prefix("/dev/") {
        stripped
    } else {
        path_str
    }
}

/// Open a target device for writing with exclusive access.
///
/// Unmounts all volumes via DiskArbitration, then opens the raw character
/// device (`/dev/rdiskN`) with `O_EXLOCK` for kernel-level exclusive access.
/// This prevents macOS from mounting any partition on the disk while the fd
/// is held — no DiskArbitration claim or mount-approval callbacks needed.
///
/// If the initial open fails with permission denied, falls back to `osascript`
/// to run `diskutil unmountDisk force` with admin privileges, then retries.
pub(crate) fn open_target_for_writing(path: &Path) -> Result<File> {
    let path_str = path.to_string_lossy();
    let disk_name = bsd_name_from_path(path);

    // Unmount all volumes before opening with exclusive lock
    if let Err(e) = da_unmount_disk(disk_name) {
        // Not fatal — the disk might not be mounted
        eprintln!("DA unmount warning: {}", e);
    }

    // Use the raw device (/dev/rdiskN) for faster unbuffered writes
    let raw_device = if path_str.starts_with("/dev/disk") {
        format!("/dev/r{}", &path_str[5..])
    } else {
        path_str.to_string()
    };

    // Try to open with O_EXLOCK
    match open_with_exlock(&raw_device, libc::O_RDWR) {
        Ok(file) => return Ok(file),
        Err(e) => {
            // Check if this is a permission error — if so, try elevation
            let is_permission = e.downcast_ref::<std::io::Error>().map_or(false, |io_err| {
                io_err.kind() == std::io::ErrorKind::PermissionDenied
            });
            if !is_permission {
                // Check the chain (anyhow wraps the io::Error as source)
                let is_permission_chain = e.chain().any(|cause| {
                    cause
                        .downcast_ref::<std::io::Error>()
                        .map_or(false, |io_err| {
                            io_err.kind() == std::io::ErrorKind::PermissionDenied
                        })
                });
                if !is_permission_chain {
                    return Err(e);
                }
            }
        }
    }

    // Permission denied — use osascript to unmount with elevated privileges,
    // then retry. This is the only remaining diskutil usage — specifically
    // for privilege elevation via the native macOS admin auth dialog.
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

    // Retry with O_EXLOCK after elevated unmount
    open_with_exlock(&raw_device, libc::O_RDWR).with_context(|| {
        format!(
            "cannot open {} for writing (even after elevated unmount)",
            raw_device
        )
    })
}

/// Open a source device or image file for reading with exclusive access.
///
/// For device paths (`/dev/disk*`), unmounts all volumes and opens with
/// `O_EXLOCK` for kernel-level exclusive access. For regular files, opens
/// normally. The application should already be running with elevated
/// privileges (via sudo at startup).
pub fn open_source_for_reading(path: &Path) -> Result<ElevatedSource> {
    let path_str = path.to_string_lossy();
    let is_device = path_str.starts_with("/dev/disk") || path_str.starts_with("/dev/rdisk");

    if is_device {
        let disk_name = bsd_name_from_path(path);

        // Unmount all volumes before opening with exclusive lock
        if let Err(e) = da_unmount_disk(disk_name) {
            eprintln!("DA unmount warning: {}", e);
        }

        // Use /dev/rdiskN (raw character device) for faster unbuffered reads
        let raw_device = if path_str.starts_with("/dev/disk") {
            format!("/dev/r{}", &path_str[5..])
        } else {
            path_str.to_string()
        };

        match open_with_exlock(&raw_device, libc::O_RDONLY) {
            Ok(file) => Ok(ElevatedSource {
                file,
                temp_path: None,
            }),
            Err(e) => {
                // Check if permission denied anywhere in the error chain
                let is_permission = e.chain().any(|cause| {
                    cause
                        .downcast_ref::<std::io::Error>()
                        .map_or(false, |io_err| {
                            io_err.kind() == std::io::ErrorKind::PermissionDenied
                        })
                });
                if is_permission {
                    bail!(
                        "Permission denied: {}.\n\n\
                         Rusty Backup requires administrator privileges to access disk devices.\n\
                         Please restart the application - you will be prompted for your password.",
                        path.display()
                    );
                }
                Err(e)
            }
        }
    } else {
        // Regular file — open normally
        let file = File::open(path).with_context(|| format!("cannot open {}", path.display()))?;
        Ok(ElevatedSource {
            file,
            temp_path: None,
        })
    }
}
