mod sudo;

use std::ffi::{c_void, CString};
use std::fs::File;
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
use std::sync::Mutex;

use objc2_disk_arbitration::{
    kDADiskClaimOptionDefault, kDADiskDescriptionDeviceInternalKey,
    kDADiskDescriptionDeviceModelKey, kDADiskDescriptionDeviceProtocolKey,
    kDADiskDescriptionMediaBSDNameKey, kDADiskDescriptionMediaRemovableKey,
    kDADiskDescriptionMediaSizeKey, kDADiskDescriptionMediaWritableKey,
    kDADiskDescriptionVolumeKindKey, kDADiskDescriptionVolumePathKey, kDADiskUnmountOptionForce,
    kDADiskUnmountOptionWhole, kDAReturnExclusiveAccess, DAApprovalSession, DADisk,
    DADiskClaimCallback, DADiskMountApprovalCallback, DADiskUnmountCallback, DADissenter,
    DASession,
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
// DiskArbitration exclusive claim + mount denial
// ---------------------------------------------------------------------------

// The crate's binding for DARegisterDiskMountApprovalCallback has the wrong
// session type (DASession instead of DAApprovalSession). Declare corrected externs.
extern "C-unwind" {
    fn DARegisterDiskMountApprovalCallback(
        session: &DAApprovalSession,
        r#match: Option<&CFDictionary>,
        callback: DADiskMountApprovalCallback,
        context: *mut c_void,
    );
    fn DAUnregisterApprovalCallback(
        session: &DAApprovalSession,
        callback: NonNull<c_void>,
        context: *mut c_void,
    );
}

/// Global state for the mount-denial callback: the BSD name prefix to deny
/// (e.g. "disk2" denies mounts on disk2, disk2s1, disk2s2, …).
static DENY_MOUNT_PREFIX: Mutex<Option<String>> = Mutex::new(None);

/// Mount approval callback: denies mounts on any disk whose BSD name starts
/// with the prefix stored in `DENY_MOUNT_PREFIX`.
unsafe extern "C-unwind" fn deny_mount_callback(
    disk: NonNull<DADisk>,
    _context: *mut c_void,
) -> *const DADissenter {
    let disk_ref = unsafe { disk.as_ref() };
    let name_ptr = unsafe { disk_ref.bsd_name() };
    if !name_ptr.is_null() {
        let name = unsafe { std::ffi::CStr::from_ptr(name_ptr) };
        if let Ok(name_str) = name.to_str() {
            let guard = DENY_MOUNT_PREFIX.lock().unwrap();
            if let Some(ref prefix) = *guard {
                if name_str.starts_with(prefix.as_str()) {
                    // Return a dissenter to deny this mount
                    let reason = CFString::from_str("Rusty Backup has exclusive access");
                    let dissenter =
                        unsafe { DADissenter::new(None, kDAReturnExclusiveAccess, Some(&reason)) };
                    // Leak one ref to the caller (caller owns the returned reference)
                    return objc2_core_foundation::CFRetained::into_raw(dissenter).as_ptr();
                }
            }
        }
    }
    ptr::null() // NULL = approve mount (not our disk)
}

/// RAII guard that holds an exclusive claim on a disk via DiskArbitration
/// and blocks all mount attempts on the disk and its partitions.
///
/// While held, the system and other applications (e.g. VMware) cannot mount
/// or claim the disk. Released when dropped.
pub(crate) struct DiskClaim {
    // Claim session + disk (prevents other apps from claiming)
    _claim_session: objc2_core_foundation::CFRetained<DASession>,
    claim_disk: objc2_core_foundation::CFRetained<DADisk>,
    claim_run_loop: objc2_core_foundation::CFRetained<CFRunLoop>,
    // Approval session (denies mount attempts on all partitions)
    approval_session: objc2_core_foundation::CFRetained<DAApprovalSession>,
    approval_run_loop: objc2_core_foundation::CFRetained<CFRunLoop>,
}

// Safety: DiskClaim only holds CF objects that are thread-safe (CFRetained
// pointers to immutable Core Foundation types). DADiskUnclaim and
// CFRunLoop unschedule are safe to call from any thread.
unsafe impl Send for DiskClaim {}

impl Drop for DiskClaim {
    fn drop(&mut self) {
        unsafe {
            // Unregister mount denial callback
            DAUnregisterApprovalCallback(
                &self.approval_session,
                NonNull::new_unchecked(deny_mount_callback as *mut c_void),
                ptr::null_mut(),
            );
            let mode = kCFRunLoopDefaultMode.unwrap();
            self.approval_session
                .unschedule_from_run_loop(&self.approval_run_loop, mode);

            // Clear the deny prefix
            *DENY_MOUNT_PREFIX.lock().unwrap() = None;

            // Release the whole-disk claim
            self.claim_disk.unclaim();
            self._claim_session
                .unschedule_from_run_loop(&self.claim_run_loop, mode);
        }
    }
}

/// Synchronously claim a disk for exclusive use and register a mount-denial
/// callback for all its partitions.
///
/// Returns `Ok(Some(guard))` on success, `Ok(None)` if setup fails
/// (non-fatal — the operation can proceed without exclusive access).
fn da_claim_disk(bsd_name: &str) -> Result<Option<DiskClaim>> {
    // --- 1. Claim the whole disk ---
    let claim_session =
        unsafe { DASession::new(None) }.context("failed to create DiskArbitration session")?;

    let c_name = CString::new(bsd_name).context("invalid BSD name")?;
    let claim_disk = unsafe {
        DADisk::from_bsd_name(
            None,
            &claim_session,
            NonNull::new(c_name.as_ptr() as *mut _).unwrap(),
        )
    }
    .context(format!("failed to create DADisk for {}", bsd_name))?;

    let claim_run_loop = CFRunLoop::current().context("failed to get current CFRunLoop")?;
    let mode = unsafe { kCFRunLoopDefaultMode.unwrap() };
    unsafe { claim_session.schedule_with_run_loop(&claim_run_loop, mode) };

    static CLAIM_DONE: AtomicBool = AtomicBool::new(false);
    static CLAIM_OK: AtomicBool = AtomicBool::new(false);
    CLAIM_DONE.store(false, Ordering::SeqCst);
    CLAIM_OK.store(false, Ordering::SeqCst);

    unsafe extern "C-unwind" fn claim_callback(
        _disk: NonNull<DADisk>,
        dissenter: *const DADissenter,
        _context: *mut c_void,
    ) {
        CLAIM_OK.store(dissenter.is_null(), Ordering::SeqCst);
        CLAIM_DONE.store(true, Ordering::SeqCst);
        if let Some(rl) = CFRunLoop::current() {
            rl.stop();
        }
    }

    let callback: DADiskClaimCallback = Some(claim_callback);
    unsafe {
        claim_disk.claim(
            kDADiskClaimOptionDefault,
            None,            // no release callback — claim can never be stolen
            ptr::null_mut(), // release context
            callback,
            ptr::null_mut(), // callback context
        );
    }

    let timeout_secs = 10.0;
    CFRunLoop::run_in_mode(Some(mode), timeout_secs, false);

    if !CLAIM_DONE.load(Ordering::SeqCst) {
        unsafe { claim_session.unschedule_from_run_loop(&claim_run_loop, mode) };
        eprintln!(
            "DA claim of {} timed out — proceeding without exclusive access",
            bsd_name
        );
        return Ok(None);
    }
    if !CLAIM_OK.load(Ordering::SeqCst) {
        unsafe { claim_session.unschedule_from_run_loop(&claim_run_loop, mode) };
        eprintln!(
            "DA claim of {} failed — proceeding without exclusive access",
            bsd_name
        );
        return Ok(None);
    }

    // --- 2. Register mount-denial callback for all partitions ---
    let approval_session =
        unsafe { DAApprovalSession::new(None) }.context("failed to create DAApprovalSession")?;

    let approval_run_loop = CFRunLoop::current().context("failed to get current CFRunLoop")?;
    unsafe { approval_session.schedule_with_run_loop(&approval_run_loop, mode) };

    // Set the prefix that the callback will match against
    *DENY_MOUNT_PREFIX.lock().unwrap() = Some(bsd_name.to_string());

    let approval_callback: DADiskMountApprovalCallback = Some(deny_mount_callback);
    unsafe {
        DARegisterDiskMountApprovalCallback(
            &approval_session,
            None, // match all disks — callback filters by prefix
            approval_callback,
            ptr::null_mut(),
        );
    }

    eprintln!(
        "DA: claimed {} and registered mount denial for {}*",
        bsd_name, bsd_name
    );

    Ok(Some(DiskClaim {
        _claim_session: claim_session,
        claim_disk,
        claim_run_loop,
        approval_session,
        approval_run_loop,
    }))
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

/// Open a target device for writing, claiming and unmounting it first.
///
/// Claims the disk for exclusive access via DiskArbitration (prevents other
/// apps like VMware from interfering), then unmounts all volumes. If normal
/// open fails with permission denied, falls back to `osascript` to run
/// `diskutil unmountDisk force` with admin privileges via the native macOS
/// authentication dialog (DA doesn't provide admin prompting).
///
/// Returns the file handle and an optional `DiskClaim` guard that must be
/// kept alive for the duration of the operation.
pub(crate) fn open_target_for_writing(path: &Path) -> Result<(File, Option<DiskClaim>)> {
    let path_str = path.to_string_lossy();
    let disk_name = bsd_name_from_path(path);

    // Claim the disk for exclusive access before unmounting
    let claim = match da_claim_disk(disk_name) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("DA claim warning: {}", e);
            None
        }
    };

    // Unmount all partitions on the disk first via DiskArbitration
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

    // Try normal open first
    match std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&raw_device)
    {
        Ok(file) => return Ok((file, claim)),
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
            // Fall through to elevation attempt
        }
        Err(e) => {
            return Err(
                anyhow::anyhow!(e).context(format!("cannot open {} for writing", raw_device))
            );
        }
    }

    // Use osascript to unmount with elevated privileges.
    // This is the only remaining diskutil usage — specifically for privilege
    // elevation via the native macOS admin auth dialog, which DA doesn't provide.
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
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&raw_device)
        .with_context(|| {
            format!(
                "cannot open {} for writing (even after elevated unmount)",
                raw_device
            )
        })?;
    Ok((file, claim))
}

/// Open a source device or image file for reading.
///
/// For device paths (`/dev/disk*`), claims the disk for exclusive access and
/// unmounts all volumes before opening. The application should already be
/// running with elevated privileges (via sudo at startup). If not elevated,
/// this will return a permission denied error with a helpful message.
pub fn open_source_for_reading(path: &Path) -> Result<ElevatedSource> {
    let path_str = path.to_string_lossy();
    let is_device = path_str.starts_with("/dev/disk") || path_str.starts_with("/dev/rdisk");

    // For devices: claim + unmount before opening
    let disk_claim = if is_device {
        let disk_name = bsd_name_from_path(path);

        let claim = match da_claim_disk(disk_name) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("DA claim warning: {}", e);
                None
            }
        };

        if let Err(e) = da_unmount_disk(disk_name) {
            eprintln!("DA unmount warning: {}", e);
        }

        claim
    } else {
        None
    };

    // Use /dev/rdiskN (raw character device) for faster unbuffered reads
    let actual_path = if path_str.starts_with("/dev/disk") {
        PathBuf::from(format!("/dev/r{}", &path_str[5..]))
    } else {
        path.to_path_buf()
    };

    // Try to open the file
    match File::open(&actual_path) {
        Ok(file) => Ok(ElevatedSource {
            file,
            temp_path: None,
            disk_claim,
        }),
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
            if path_str.starts_with("/dev/") {
                bail!(
                    "Permission denied: {}.\n\n\
                     Rusty Backup requires administrator privileges to access disk devices.\n\
                     Please restart the application - you will be prompted for your password.",
                    path.display()
                );
            } else {
                Err(anyhow::anyhow!(e).context(format!("cannot open {}", path.display())))
            }
        }
        Err(e) => Err(anyhow::anyhow!(e).context(format!("cannot open {}", path.display()))),
    }
}
