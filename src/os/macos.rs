//! macOS device access: enumeration (IOKit + DiskArbitration), unmounting and
//! claiming, and privilege escalation through `/usr/libexec/authopen`.
//!
//! # authopen's reply protocol
//!
//! Undocumented; read from the helper's disassembly on macOS 26 (R11). With
//! `-stdoutpipe` it answers with exactly one `sendmsg` carrying two data bytes.
//! On success the descriptor rides along as `SCM_RIGHTS` and both bytes are 0.
//! On failure there is no control message and byte 1 is the errno of whichever
//! open failed (1 when that errno's low byte is 0); the helper then exits 1.
//!
//! The helper first opens the path as the calling user. On EACCES it asks for
//! the right `sys.openfile.readonly|readwrite|readwritecreate.<path>` and maps
//! the authorization status to an errno: `errAuthorizationCanceled` (-60006)
//! becomes `ECANCELED`, `errAuthorizationDenied` (-60005) and
//! `errAuthorizationInteractionNotAllowed` (-60007) become `EACCES`, the
//! invalid-argument statuses `EINVAL`, anything else `EACCES`. Once authorized
//! it opens the path as root, and that open's errno is what comes back for a
//! write-protected card (EACCES) or a mounted disk (EBUSY). Its stderr names
//! the failing step: `AuthorizationCopyRights failed: <text>` for the dialog,
//! `couldn't open <path>: <text>` for the root open. The earlier cancel check
//! compared our own error text against "cancelled" and never matched.

mod sudo;

use std::ffi::{c_void, CString};
use std::fs::File;
use std::mem::ManuallyDrop;
use std::os::unix::io::{AsRawFd, FromRawFd, OwnedFd};
use std::path::{Path, PathBuf};
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{bail, Context, Result};

pub use sudo::{request_app_elevation, sudo_execute};

use libc::statfs;
use objc2_core_foundation::{
    kCFRunLoopDefaultMode, CFBoolean, CFDictionary, CFMutableDictionary, CFNumber, CFRunLoop,
    CFString, CFURL,
};

use objc2_disk_arbitration::{
    kDADiskClaimOptionDefault, kDADiskDescriptionDeviceInternalKey,
    kDADiskDescriptionDeviceModelKey, kDADiskDescriptionDeviceProtocolKey,
    kDADiskDescriptionMediaBSDNameKey, kDADiskDescriptionMediaRemovableKey,
    kDADiskDescriptionMediaSizeKey, kDADiskDescriptionMediaWritableKey,
    kDADiskDescriptionVolumeKindKey, kDADiskDescriptionVolumePathKey, kDADiskUnmountOptionForce,
    kDADiskUnmountOptionWhole, kDAReturnExclusiveAccess, DADisk, DADiskClaimCallback,
    DADiskClaimReleaseCallback, DADiskUnmountCallback, DADissenter, DASession,
};
use objc2_io_kit::{
    IOIteratorNext, IOObjectRelease, IORegistryEntryCreateCFProperties,
    IOServiceGetMatchingServices, IOServiceMatching,
};

use super::ElevatedSource;
use crate::device::{DiskDevice, MountedPartition};

// ---------------------------------------------------------------------------

// macOS ioctl constants for getting device size
// DKIOCGETBLOCKSIZE = _IOR('d', 24, u32) = 0x40046418
// DKIOCGETBLOCKCOUNT = _IOR('d', 25, u64) = 0x40086419
const DKIOCGETBLOCKSIZE: libc::c_ulong = 0x40046418;
const DKIOCGETBLOCKCOUNT: libc::c_ulong = 0x40086419;
// DKIOCISWRITABLE = _IOR('d', 29, u32) = 0x4004641d
const DKIOCISWRITABLE: libc::c_ulong = 0x4004641d;

/// macOS fcntl command to bypass the buffer cache (equivalent to O_DIRECT on Linux).
const F_NOCACHE: libc::c_int = 48;

/// Re-enable the buffer cache on a previously-opened raw device fd.
///
/// `open_device_for_inspect` and friends set `F_NOCACHE` so backup/restore
/// linear passes don't pollute the page cache. For B-tree-style filesystems
/// (HFS+ on heavily-used volumes) the catalog is read in many small chunks
/// scattered across the disk, and `F_NOCACHE` makes every read a synchronous
/// device round-trip with no readahead — observed at ~2.4 MB/s on a fast SD
/// card. Clearing the flag on the inspect fd lets the kernel cache + cluster
/// reads for the duration of the browse session.
pub fn clear_nocache(file: &File) -> std::io::Result<()> {
    use std::os::unix::io::AsRawFd;
    let r = unsafe { libc::fcntl(file.as_raw_fd(), F_NOCACHE, 0) };
    if r < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

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

/// Whether the kernel will accept writes to the media behind `file`; `None`
/// for anything that is not a disk device. A locked SD card answers `false`.
pub fn media_is_writable(file: &File) -> Option<bool> {
    use std::os::unix::io::AsRawFd;
    let mut writable: u32 = 0;
    let r = unsafe { libc::ioctl(file.as_raw_fd(), DKIOCISWRITABLE, &mut writable) };
    if r != 0 {
        return None;
    }
    Some(writable != 0)
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
// The IOKit "IOMedia" class name as a C-string pointer. `c"…"` literals need
// rustc 1.77, so the vintage macOS 10.7 build (Rust 1.73) uses a nul-terminated
// byte string. The modern `c"…"` form lives in its OWN FILE (macos_iomedia.rs):
// a `c"…"` token is a lexer-level feature, so 1.73 would reject it even inside a
// cfg'd-out block in the same file — an unloaded `#[cfg] mod` is never lexed.
#[cfg(not(feature = "rust173-polyfill"))]
#[path = "macos_iomedia.rs"]
mod iomedia;
#[cfg(not(feature = "rust173-polyfill"))]
use iomedia::iomedia_class_name;

#[cfg(feature = "rust173-polyfill")]
fn iomedia_class_name() -> *const std::os::raw::c_char {
    b"IOMedia\0".as_ptr() as *const std::os::raw::c_char
}

fn iokit_enumerate_media() -> Vec<IOMediaEntry> {
    let mut entries = Vec::new();

    unsafe {
        let matching = IOServiceMatching(iomedia_class_name());
        let matching = match matching {
            Some(m) => m,
            None => return entries,
        };

        let mut iterator: u32 = 0;
        let kr = IOServiceGetMatchingServices(
            // MACH_PORT_NULL (0) == the default I/O Kit port. We pass the literal
            // rather than the `kIOMainPortDefault` constant because that symbol is
            // the macOS 12+ rename of `kIOMasterPortDefault` and is absent from
            // pre-12 IOKit — referencing it dyld-traps the vintage 10.7 build at
            // launch. 0 behaves identically on every macOS version.
            0,
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

/// Whether DiskArbitration says the media accepts writes, without opening it;
/// `None` when it has no answer (no session, or not a disk it knows).
fn da_media_writable(bsd_name: &str) -> Option<bool> {
    let session = unsafe { DASession::new(None) }?;
    da_disk_description(&session, bsd_name).map(|d| d.is_writable)
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
// Per-call DiskArbitration callback state
// ---------------------------------------------------------------------------

/// Heap-allocated state passed via the DA callback context pointer.
///
/// Replaces module-level static `AtomicBool`s to prevent a race condition
/// when two operations start concurrently or a previous one is aborted mid-flight.
struct CallbackState {
    done: AtomicBool,
    ok: AtomicBool,
}

// ---------------------------------------------------------------------------
// DiskArbitration unmount (with per-tick retry)
// ---------------------------------------------------------------------------

/// Synchronously unmount all volumes on a disk via DiskArbitration.
///
/// Returns `Ok(())` on success, or an error if the unmount fails or times out
/// after 5 retry attempts (25 seconds total).
fn da_unmount_disk(bsd_name: &str) -> Result<()> {
    da_unmount_disk_attempt(bsd_name, 0)
}

fn da_unmount_disk_attempt(bsd_name: &str, attempt: u32) -> Result<()> {
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

    // Per-call state passed via context pointer — no static globals
    let state = Box::into_raw(Box::new(CallbackState {
        done: AtomicBool::new(false),
        ok: AtomicBool::new(false),
    }));

    unsafe extern "C-unwind" fn unmount_callback(
        _disk: NonNull<DADisk>,
        dissenter: *const DADissenter,
        context: *mut c_void,
    ) {
        let state = unsafe { &*(context as *const CallbackState) };
        state.ok.store(dissenter.is_null(), Ordering::SeqCst);
        state.done.store(true, Ordering::SeqCst);
        if let Some(rl) = CFRunLoop::current() {
            rl.stop();
        }
    }

    let options = kDADiskUnmountOptionForce | kDADiskUnmountOptionWhole;
    let callback: DADiskUnmountCallback = Some(unmount_callback);

    unsafe {
        disk.unmount(options, callback, state as *mut c_void);
    }

    // Run the run loop in 0.5s ticks (max 10 = 5s) waiting for the callback.
    // The callback calls rl.stop(), so run_in_mode returns early when it fires.
    let mut callback_fired = false;
    for _ in 0..10 {
        CFRunLoop::run_in_mode(Some(mode), 0.5, false);
        if unsafe { &*state }.done.load(Ordering::SeqCst) {
            callback_fired = true;
            break;
        }
    }

    unsafe { session.unschedule_from_run_loop(&run_loop, mode) };
    let state = unsafe { Box::from_raw(state) };

    if !callback_fired {
        if attempt < 5 {
            log::warn!(
                "DA unmount of {} timed out (attempt {}), retrying...",
                bsd_name,
                attempt + 1
            );
            return da_unmount_disk_attempt(bsd_name, attempt + 1);
        }
        bail!(
            "unmount of {} timed out after 5 attempts (25s total)",
            bsd_name
        );
    }

    if !state.ok.load(Ordering::SeqCst) {
        bail!("DiskArbitration failed to unmount {}", bsd_name);
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// DiskArbitration exclusive claim
// ---------------------------------------------------------------------------

/// RAII guard that holds an exclusive claim on a disk via DiskArbitration.
///
/// While held, other DiskArbitration clients (e.g. VMware Fusion) cannot
/// claim or interact with the disk. The claim session is scheduled on the
/// **main run loop** (GUI event loop) so the release-denial callback fires
/// even while the worker thread does blocking I/O.
pub(crate) struct DiskClaim {
    _session: objc2_core_foundation::CFRetained<DASession>,
    disk: objc2_core_foundation::CFRetained<DADisk>,
    main_run_loop: objc2_core_foundation::CFRetained<CFRunLoop>,
}

// Safety: DiskClaim only holds CF objects that are thread-safe (CFRetained
// pointers to immutable Core Foundation types). DADiskUnclaim and
// CFRunLoop unschedule are safe to call from any thread.
unsafe impl Send for DiskClaim {}

impl Drop for DiskClaim {
    fn drop(&mut self) {
        unsafe {
            let mode = kCFRunLoopDefaultMode.unwrap();
            self.disk.unclaim();
            self._session
                .unschedule_from_run_loop(&self.main_run_loop, mode);
        }
    }
}

/// Release-denial callback: DA calls this when another client (e.g. VMware)
/// tries to steal our claim. We return a dissenter to refuse.
unsafe extern "C-unwind" fn claim_release_deny(
    _disk: NonNull<DADisk>,
    _context: *mut c_void,
) -> *const DADissenter {
    log::warn!("DA: another client tried to steal our disk claim — denied");
    let reason = CFString::from_str("Rusty Backup has exclusive access");
    let dissenter = unsafe { DADissenter::new(None, kDAReturnExclusiveAccess, Some(&reason)) };
    objc2_core_foundation::CFRetained::into_raw(dissenter).as_ptr()
}

/// Synchronously claim a disk for exclusive use via DiskArbitration.
///
/// The claim session is scheduled on the **main run loop** so that:
/// 1. The release-denial callback fires when other DA clients try to steal
///    the claim (the worker thread's run loop is never pumped during I/O).
/// 2. The claim completion callback fires reliably.
///
/// We spin-wait for the completion callback since we can't pump the main
/// run loop from a worker thread (the GUI thread pumps it).
///
/// Non-fatal: returns `Ok(None)` if the claim fails.
fn da_claim_disk(bsd_name: &str) -> Result<Option<DiskClaim>> {
    // Fast path: the main run loop, which the GUI pumps. Short deadline — if
    // nobody is pumping it (rb-cli, rb-cli tui), the callback will never arrive
    // and waiting the full timeout just stalls the caller for nothing.
    match da_claim_disk_on(bsd_name, RunLoopTarget::Main, Duration::from_secs(1)) {
        Ok(Some(claim)) => return Ok(Some(claim)),
        Ok(None) => {}
        Err(e) => return Err(e),
    }

    // Fallback: schedule on *this* thread's run loop and pump it ourselves, so a
    // binary with no main-loop pump still gets a real claim instead of silently
    // running shared. The trade-off is that this loop is not pumped during the
    // blocking reads that follow, so the release-denial callback cannot fire —
    // another DA client could take the disk mid-read. That is strictly better
    // than holding no claim at all, which is what the timeout used to leave us
    // with.
    log::debug!("DA: main run loop is not being pumped — claiming on this thread instead");
    da_claim_disk_on(bsd_name, RunLoopTarget::Current, Duration::from_secs(5))
}

/// Which run loop a claim's callbacks are delivered on.
#[derive(Clone, Copy, PartialEq)]
enum RunLoopTarget {
    /// The process main run loop — correct when something pumps it (the GUI).
    Main,
    /// The calling thread's run loop, pumped inline while awaiting the claim.
    Current,
}

fn da_claim_disk_on(
    bsd_name: &str,
    target: RunLoopTarget,
    timeout: Duration,
) -> Result<Option<DiskClaim>> {
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

    // The main run loop keeps the release-denial callback live even while a
    // worker thread does blocking disk I/O — the reason it is preferred.
    let main_run_loop = match target {
        RunLoopTarget::Main => CFRunLoop::main().context("failed to get main CFRunLoop")?,
        RunLoopTarget::Current => {
            CFRunLoop::current().context("failed to get current CFRunLoop")?
        }
    };
    let mode = unsafe { kCFRunLoopDefaultMode.unwrap() };
    unsafe { session.schedule_with_run_loop(&main_run_loop, mode) };

    // Per-call state — no static globals, eliminates the concurrent-operation race
    let state = Box::into_raw(Box::new(CallbackState {
        done: AtomicBool::new(false),
        ok: AtomicBool::new(false),
    }));

    unsafe extern "C-unwind" fn claim_callback(
        _disk: NonNull<DADisk>,
        dissenter: *const DADissenter,
        context: *mut c_void,
    ) {
        let state = unsafe { &*(context as *const CallbackState) };
        state.ok.store(dissenter.is_null(), Ordering::SeqCst);
        state.done.store(true, Ordering::SeqCst);
    }

    let release_cb: DADiskClaimReleaseCallback = Some(claim_release_deny);
    let callback: DADiskClaimCallback = Some(claim_callback);
    unsafe {
        disk.claim(
            kDADiskClaimOptionDefault,
            release_cb,
            ptr::null_mut(), // release context
            callback,
            state as *mut c_void, // claim callback context
        );
    }

    // Await the claim completion callback. On the main loop we can only wait for
    // whoever pumps it; on our own loop we pump it ourselves.
    let deadline = std::time::Instant::now() + timeout;
    while !unsafe { &*state }.done.load(Ordering::SeqCst) {
        if std::time::Instant::now() >= deadline {
            unsafe {
                // The request may still land after we stop waiting; unclaim so a
                // late success cannot leave the disk claimed by a session we are
                // about to drop.
                disk.unclaim();
                session.unschedule_from_run_loop(&main_run_loop, mode);
                let _ = Box::from_raw(state);
            }
            match target {
                RunLoopTarget::Main => log::debug!(
                    "DA claim of {bsd_name} saw no main-loop callback within {timeout:?}"
                ),
                RunLoopTarget::Current => log::warn!(
                    "DA claim of {bsd_name} timed out — proceeding without exclusive access"
                ),
            }
            return Ok(None);
        }
        match target {
            RunLoopTarget::Current => {
                // Pumping is what makes the callback arrive here at all.
                CFRunLoop::run_in_mode(Some(mode), 0.05, false);
            }
            RunLoopTarget::Main => std::thread::sleep(std::time::Duration::from_millis(50)),
        }
    }

    let state = unsafe { Box::from_raw(state) };

    if !state.ok.load(Ordering::SeqCst) {
        unsafe { session.unschedule_from_run_loop(&main_run_loop, mode) };
        log::warn!(
            "DA claim of {} failed — proceeding without exclusive access",
            bsd_name
        );
        return Ok(None);
    }

    log::info!("DA: claimed {} for exclusive access", bsd_name);

    Ok(Some(DiskClaim {
        _session: session,
        disk,
        main_run_loop,
    }))
}

// ---------------------------------------------------------------------------
// authopen-based privileged device access
// ---------------------------------------------------------------------------

/// Privileged device descriptors kept for the life of the process, so a device
/// is escalated once instead of once per operation.
///
/// `authopen` issues no reusable credential — it opens one file, passes the
/// descriptor back over `SCM_RIGHTS`, and exits — so the descriptor itself is
/// the only cacheable artifact. Inspecting a disk and then backing it up used
/// to mean two authorization prompts for the same device; now the second call
/// finds the first call's descriptor.
///
/// Entries are `(raw device path, writable, fd)`. A writable entry satisfies a
/// read-only request; the reverse re-escalates.
static ELEVATED_DEVICES: Mutex<Vec<(String, bool, Arc<OwnedFd>)>> = Mutex::new(Vec::new());

/// A privileged descriptor shared between operations, each holding its own
/// file offset.
///
/// The cached fd is one open file *description*, so a plain `dup` would make
/// every holder share a single offset — an Inspect browse session and a running
/// backup would silently seek out from under each other. All I/O here is
/// positional (`pread` / `pwrite`) against a private `pos` instead.
pub struct SharedDevice {
    fd: Arc<OwnedFd>,
    pos: u64,
    /// Device length; raw devices can't answer `seek(End)`.
    len: u64,
}

impl SharedDevice {
    fn new(fd: Arc<OwnedFd>) -> Self {
        let len = {
            // get_device_size wants a File; borrow the fd without owning it.
            let borrowed = ManuallyDrop::new(unsafe { File::from_raw_fd(fd.as_raw_fd()) });
            get_device_size(&borrowed).unwrap_or(0)
        };
        Self { fd, pos: 0, len }
    }

    /// An independent handle onto the same descriptor, starting at offset 0.
    pub fn try_clone(&self) -> std::io::Result<Self> {
        Ok(Self {
            fd: Arc::clone(&self.fd),
            pos: 0,
            len: self.len,
        })
    }

    /// Device length in bytes, or 0 when the OS wouldn't say.
    pub fn byte_len(&self) -> u64 {
        self.len
    }

    /// Dup the cached descriptor into an owned `File`.
    ///
    /// The dup shares one file *description* — and therefore one file offset —
    /// with the cached entry. That's fine because `SharedDevice` itself only
    /// ever uses `pread` / `pwrite`, which leave that offset untouched, so the
    /// returned `File` is its sole user.
    pub fn dup_as_file(&self) -> std::io::Result<File> {
        let fd = unsafe { libc::dup(self.fd.as_raw_fd()) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(unsafe { File::from_raw_fd(fd) })
    }

    pub fn as_raw_fd(&self) -> libc::c_int {
        self.fd.as_raw_fd()
    }
}

impl std::io::Read for SharedDevice {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = unsafe {
            libc::pread(
                self.fd.as_raw_fd(),
                buf.as_mut_ptr() as *mut c_void,
                buf.len(),
                self.pos as libc::off_t,
            )
        };
        if n < 0 {
            return Err(std::io::Error::last_os_error());
        }
        self.pos += n as u64;
        Ok(n as usize)
    }
}

impl std::io::Write for SharedDevice {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = unsafe {
            libc::pwrite(
                self.fd.as_raw_fd(),
                buf.as_ptr() as *const c_void,
                buf.len(),
                self.pos as libc::off_t,
            )
        };
        if n < 0 {
            return Err(std::io::Error::last_os_error());
        }
        self.pos += n as u64;
        Ok(n as usize)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // Positional writes go straight to the device; F_NOCACHE is already set.
        Ok(())
    }
}

impl std::io::Seek for SharedDevice {
    fn seek(&mut self, from: std::io::SeekFrom) -> std::io::Result<u64> {
        use std::io::SeekFrom;
        let next = match from {
            SeekFrom::Start(n) => n as i64,
            SeekFrom::Current(d) => self.pos as i64 + d,
            SeekFrom::End(d) => self.len as i64 + d,
        };
        if next < 0 {
            return Err(crate::compat::io_other("seek before start of device"));
        }
        self.pos = next as u64;
        Ok(self.pos)
    }
}

/// A cached descriptor for `path` that covers the requested access, if any.
fn reuse_elevated_device(path: &str, needs_write: bool) -> Option<SharedDevice> {
    let cache = ELEVATED_DEVICES.lock().ok()?;
    for (cached_path, writable, fd) in cache.iter() {
        if cached_path == path && (*writable || !needs_write) {
            log::info!("reusing the elevated descriptor for {path} (no new prompt)");
            return Some(SharedDevice::new(Arc::clone(fd)));
        }
    }
    None
}

/// Escalate `path`, reusing a descriptor from an earlier prompt when one
/// covers the requested access.
fn cached_authopen(path: &str, flags: libc::c_int) -> Result<SharedDevice> {
    let needs_write = flags & libc::O_ACCMODE != libc::O_RDONLY;

    if let Some(shared) = reuse_elevated_device(path, needs_write) {
        return Ok(shared);
    }

    let file = authopen_device(path, flags)?;
    let fd = Arc::new(OwnedFd::from(file));
    if let Ok(mut cache) = ELEVATED_DEVICES.lock() {
        if needs_write {
            // A writable descriptor supersedes a read-only one for this device,
            // so drop the narrower entry and let every later operation — read
            // or write — reuse this one.
            cache.retain(|(cached, writable, _)| cached != path || *writable);
        }
        cache.push((path.to_string(), needs_write, Arc::clone(&fd)));
    }
    Ok(SharedDevice::new(fd))
}

/// Access mode to escalate a read with: `O_RDWR`, so one prompt also covers a
/// later restore — unless a volume is still mounted (EBUSY) or the media is
/// write-protected, which refuse it for root too (R6).
fn read_escalation_flags(unmounted: bool, saw_busy: bool, writable: bool) -> libc::c_int {
    if unmounted && !saw_busy && writable {
        libc::O_RDWR
    } else {
        libc::O_RDONLY
    }
}

/// Why a device opened read-only after refusing read-write (R6): write-protected
/// media is reported as such, never as a privilege problem.
fn log_read_only_open(path: &str, file: &File, rw_errno: i32) {
    match media_is_writable(file) {
        Some(false) => log::warn!(
            "{path} is write-protected (lock switch or read-only image); opened \
             read-only, a restore to it cannot work"
        ),
        _ => log::info!(
            "{path} opened read-only (read-write open failed: {}); a restore will \
             ask for administrator rights",
            std::io::Error::from_raw_os_error(rw_errno)
        ),
    }
}

/// TEMP-DIAG: report what access we hold on `path` right now, without
/// escalating. Tracking down a macOS restore that fails with EACCES at the very
/// end after an earlier authopen succeeded. Remove with the rest of TEMP-DIAG
/// once that's understood — grep the tag.
pub fn probe_device_access(path: &str) -> Vec<String> {
    let raw = raw_device_path(path);
    let mut out = Vec::new();

    out.push(format!(
        "[perm] euid={} ({}), can show an auth prompt: {}",
        unsafe { libc::geteuid() },
        if running_as_root() {
            "root"
        } else {
            "not elevated"
        },
        if session_can_prompt() { "yes" } else { "no" },
    ));

    // Plain open(2) only — probing must never raise a dialog of its own, or it
    // would change the very thing it is measuring.
    for (label, flags) in [("O_RDONLY", libc::O_RDONLY), ("O_RDWR", libc::O_RDWR)] {
        let Ok(c_path) = CString::new(raw.as_str()) else {
            continue;
        };
        let fd = unsafe { libc::open(c_path.as_ptr(), flags) };
        if fd >= 0 {
            unsafe { libc::close(fd) };
            out.push(format!("[perm] {raw}: {label} ok"));
        } else {
            let err = std::io::Error::last_os_error();
            out.push(format!(
                "[perm] {raw}: {label} failed - {} (errno {})",
                err,
                err.raw_os_error().unwrap_or(0),
            ));
        }
    }

    let cached = ELEVATED_DEVICES
        .lock()
        .map(|c| {
            c.iter()
                .filter(|(p, _, _)| p == &raw)
                .map(|(_, w, _)| if *w { "read-write" } else { "read-only" })
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_else(|_| "<cache lock poisoned>".to_string());
    out.push(format!(
        "[perm] {raw}: cached elevated descriptor: {}",
        if cached.is_empty() { "none" } else { &cached },
    ));

    out
}

/// Drop cached descriptors for `path` (all of them when `path` is `None`).
///
/// A held descriptor keeps the raw device open, which blocks a clean eject, so
/// the GUI releases them when it closes a disk or the user asks to eject.
pub fn release_elevated_devices(path: Option<&str>) {
    if let Ok(mut cache) = ELEVATED_DEVICES.lock() {
        match path {
            Some(p) => {
                let raw = raw_device_path(p);
                cache.retain(|(cached, _, _)| cached != &raw && cached != p);
            }
            None => cache.clear(),
        }
    }
}

/// Already root, so a privileged `open(2)` needs no escalation at all.
fn running_as_root() -> bool {
    unsafe { libc::geteuid() == 0 }
}

// Security.framework session attributes (AuthSession.h). `authopen` delegates
// to SecurityAgent, which can only draw its dialog in a session that has
// graphic access — without it the helper waits on input that can never come.
const CALLER_SECURITY_SESSION: libc::c_int = -1;
const SESSION_HAS_GRAPHIC_ACCESS: u32 = 0x0010;

#[link(name = "Security", kind = "framework")]
extern "C" {
    fn SessionGetInfo(
        session: libc::c_int,
        session_id: *mut libc::c_int,
        attributes: *mut u32,
    ) -> i32;
}

/// Whether this process sits in a session that can show an auth dialog.
///
/// Returns `true` when the query itself fails: an unknown session must not be
/// treated as headless, or we would refuse to escalate on a perfectly normal
/// desktop.
fn session_can_prompt() -> bool {
    let mut id: libc::c_int = 0;
    let mut attrs: u32 = 0;
    let status = unsafe { SessionGetInfo(CALLER_SECURITY_SESSION, &mut id, &mut attrs) };
    if status != 0 {
        log::debug!("SessionGetInfo failed ({status}); assuming a GUI session is present");
        return true;
    }
    attrs & SESSION_HAS_GRAPHIC_ACCESS != 0
}

/// Why `authopen` would not work right now, or `None` when it should.
///
/// Checked *before* forking the helper: authopen with no way to prompt hangs
/// indefinitely rather than failing, and the parent blocks in `recvmsg` behind
/// it. Diagnosing that from the outside just looks like the app is wedged.
fn authopen_blocked_reason() -> Option<String> {
    if running_as_root() {
        return Some("already running as root; a direct open needs no prompt".to_string());
    }
    if !session_can_prompt() {
        return Some(
            "this session has no GUI access (SSH / cron / launchd daemon), so the \
             administrator prompt cannot be shown -- re-run with sudo instead"
                .to_string(),
        );
    }
    None
}

/// How long to wait for `authopen` to hand back a descriptor. Generous, since
/// it spans the user reading and answering the password prompt; the point is
/// only that a wedged helper eventually becomes an error instead of a hang.
const AUTHOPEN_TIMEOUT: libc::time_t = 120;

/// `authopen` answered but handed back no descriptor (R11).
#[derive(Debug)]
pub struct AuthopenRefused {
    /// errno the helper reported; `ECANCELED` when the user dismissed the dialog.
    errno: i32,
    /// What the helper printed to stderr, if anything.
    stderr: String,
}

impl AuthopenRefused {
    /// The user dismissed the authorization dialog.
    pub fn cancelled(&self) -> bool {
        self.errno == libc::ECANCELED
    }
}

impl std::fmt::Display for AuthopenRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.cancelled() {
            return write!(f, "administrator authorization was cancelled");
        }
        // authopen's own stderr says which of its two opens failed (see the
        // module header); the errno alone cannot, both sides use EACCES.
        let detail = self.stderr.trim();
        let why = if detail.contains("AuthorizationCopyRights") {
            "administrator authorization was denied"
        } else if detail.contains("couldn't open") {
            "authopen could not open the device even as root"
        } else {
            "authopen returned no descriptor"
        };
        write!(
            f,
            "{why}: {}",
            std::io::Error::from_raw_os_error(self.errno)
        )?;
        if !detail.is_empty() {
            write!(f, " ({detail})")?;
        }
        Ok(())
    }
}

impl std::error::Error for AuthopenRefused {}

/// Whether an escalation error is the user cancelling the dialog, through any
/// context layers the callers added.
pub fn is_authorization_cancelled(err: &anyhow::Error) -> bool {
    err.downcast_ref::<AuthopenRefused>()
        .is_some_and(AuthopenRefused::cancelled)
}

/// Decode the two bytes authopen sends in place of a descriptor.
fn authopen_refusal(reply: &[u8], stderr: String) -> AuthopenRefused {
    // Byte 1 is the errno of the failed open, or 1 when the helper had none.
    let errno = reply.get(1).copied().unwrap_or(0) as i32;
    AuthopenRefused { errno, stderr }
}

/// One reply from authopen: the descriptor, or the two bytes it sent instead.
enum AuthopenReply {
    Fd(libc::c_int),
    Refused([u8; 2]),
}

/// Receive authopen's reply: a descriptor via `SCM_RIGHTS`, or its refusal.
fn receive_authopen_reply(sock: libc::c_int) -> Result<AuthopenReply> {
    use std::mem;

    let mut data = [0u8; 2];
    let mut iov = libc::iovec {
        iov_base: data.as_mut_ptr() as *mut c_void,
        iov_len: data.len(),
    };

    // Allocate a buffer large enough for one cmsghdr + one int (the fd)
    let cmsg_space =
        unsafe { libc::CMSG_SPACE(mem::size_of::<libc::c_int>() as libc::c_uint) as usize };
    let mut cmsg_buf = vec![0u8; cmsg_space];

    let mut msg: libc::msghdr = unsafe { mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = cmsg_buf.as_mut_ptr() as *mut c_void;
    msg.msg_controllen = cmsg_space as libc::socklen_t;

    let ret = loop {
        let r = unsafe { libc::recvmsg(sock, &mut msg, 0) };
        if r == -1 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            // EAGAIN == EWOULDBLOCK on macOS; SO_RCVTIMEO reports the timeout here.
            if err.raw_os_error() == Some(libc::EAGAIN) {
                bail!(
                    "authopen did not respond within {}s (its authorization prompt \
                     may not be reachable from this session)",
                    AUTHOPEN_TIMEOUT
                );
            }
            bail!("recvmsg failed: {}", err);
        }
        break r;
    };

    if ret <= 0 {
        bail!("recvmsg: authopen exited without answering");
    }

    let cmsg = unsafe { libc::CMSG_FIRSTHDR(&msg) };
    if cmsg.is_null() {
        return Ok(AuthopenReply::Refused(data));
    }

    let (cmsg_level, cmsg_type) = unsafe { ((*cmsg).cmsg_level, (*cmsg).cmsg_type) };
    if cmsg_level != libc::SOL_SOCKET || cmsg_type != libc::SCM_RIGHTS {
        bail!(
            "recvmsg: unexpected control message (level={}, type={})",
            cmsg_level,
            cmsg_type
        );
    }

    let fd = unsafe {
        let data_ptr = libc::CMSG_DATA(cmsg) as *const libc::c_int;
        std::ptr::read_unaligned(data_ptr)
    };

    Ok(AuthopenReply::Fd(fd))
}

/// Everything the helper wrote to the stderr pipe, once it has exited.
fn drain_stderr_pipe(fd: libc::c_int) -> String {
    use std::io::Read as _;
    let mut file = unsafe { File::from_raw_fd(fd) };
    let mut out = Vec::new();
    let _ = file.read_to_end(&mut out);
    String::from_utf8_lossy(&out).into_owned()
}

/// Open a device using `/usr/libexec/authopen`.
///
/// `authopen` is a macOS system binary that opens a device with root privileges
/// and passes the file descriptor back via a Unix domain socket (`SCM_RIGHTS`).
/// This avoids the unreliable `O_EXLOCK` flag on raw character devices.
///
/// authopen handles its own authorization and shows a single native macOS
/// auth dialog when needed. If already running as root it returns the fd
/// immediately without any dialog.
fn authopen_device(path: &str, flags: libc::c_int) -> Result<File> {
    if let Some(reason) = authopen_blocked_reason() {
        bail!("not using authopen: {reason}");
    }

    // 1. Create IPC socket pair:
    //    sv[0] — parent receives the fd via SCM_RIGHTS
    //    sv[1] — child (authopen) sends the fd via stdout
    let mut sv = [-1i32; 2];

    if unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, sv.as_mut_ptr()) } != 0 {
        bail!("socketpair failed: {}", std::io::Error::last_os_error());
    }

    // The helper's stderr says why it refused (R11); it writes a line or two,
    // far below the pipe buffer, so it is drained after the exit.
    let mut errp = [-1i32; 2];
    if unsafe { libc::pipe(errp.as_mut_ptr()) } != 0 {
        let err = std::io::Error::last_os_error();
        unsafe {
            libc::close(sv[0]);
            libc::close(sv[1]);
        }
        bail!("pipe failed: {err}");
    }

    // Bound the parent's wait so a helper that never answers surfaces as an
    // error instead of wedging the caller forever.
    let tv = libc::timeval {
        tv_sec: AUTHOPEN_TIMEOUT,
        tv_usec: 0,
    };
    unsafe {
        libc::setsockopt(
            sv[0],
            libc::SOL_SOCKET,
            libc::SO_RCVTIMEO,
            &tv as *const _ as *const c_void,
            std::mem::size_of::<libc::timeval>() as libc::socklen_t,
        );
    }

    // Prepare all execv arguments BEFORE fork (CStrings must stay alive until after exec)
    let authopen_path = CString::new("/usr/libexec/authopen").unwrap();
    let arg0 = CString::new("authopen").unwrap();
    let arg_stdoutpipe = CString::new("-stdoutpipe").unwrap();
    let arg_o = CString::new("-o").unwrap();
    let flags_str = CString::new(format!("{}", flags)).unwrap();
    let path_cstr = CString::new(path).context("path contains NUL byte")?;

    // null-terminated argv: authopen -stdoutpipe -o <flags> <path>
    let argv: [*const libc::c_char; 7] = [
        arg0.as_ptr(),
        arg_stdoutpipe.as_ptr(),
        arg_o.as_ptr(),
        flags_str.as_ptr(),
        path_cstr.as_ptr(),
        ptr::null(),
        ptr::null(),
    ];

    // 2. Fork — child execs authopen; parent receives the fd
    let pid = unsafe { libc::fork() };

    match pid {
        -1 => {
            unsafe {
                libc::close(sv[0]);
                libc::close(sv[1]);
                libc::close(errp[0]);
                libc::close(errp[1]);
            }
            bail!("fork failed: {}", std::io::Error::last_os_error());
        }
        0 => {
            // Child process — only async-signal-safe calls between fork and exec
            unsafe {
                // Close parent-side ends
                libc::close(sv[0]);
                libc::close(errp[0]);

                // Wire socket to stdout (authopen sends fd via -stdoutpipe on stdout)
                libc::dup2(sv[1], libc::STDOUT_FILENO);
                libc::close(sv[1]);
                libc::dup2(errp[1], libc::STDERR_FILENO);
                libc::close(errp[1]);

                libc::execv(authopen_path.as_ptr(), argv.as_ptr());
                libc::exit(-1); // exec failed
            }
        }
        child_pid => {
            // Parent process
            unsafe {
                // Close child-side ends
                libc::close(sv[1]);
                libc::close(errp[1]);
            }

            // 3. Receive the reply: a descriptor via SCM_RIGHTS, or a refusal
            let reply = receive_authopen_reply(sv[0]);
            unsafe { libc::close(sv[0]) };
            if reply.is_err() {
                unsafe {
                    // Kill before reaping: on the timeout path the helper is
                    // still sitting on a prompt, so a plain waitpid would
                    // block for exactly as long as we just refused to.
                    libc::kill(child_pid, libc::SIGKILL);
                    let mut wstatus = 0i32;
                    libc::waitpid(child_pid, &mut wstatus, 0);
                }
            }

            // 4. Wait for authopen to exit (retry on EINTR)
            let mut wstatus = 0i32;
            loop {
                let r = unsafe { libc::waitpid(child_pid, &mut wstatus, 0) };
                if r == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
                    continue;
                }
                break;
            }
            let stderr = drain_stderr_pipe(errp[0]);

            let received_fd = match reply? {
                AuthopenReply::Fd(fd) => fd,
                AuthopenReply::Refused(bytes) => {
                    let refused = authopen_refusal(&bytes, stderr);
                    log::info!("authopen refused {path}: {refused}");
                    return Err(refused.into());
                }
            };

            if libc::WIFEXITED(wstatus) && libc::WEXITSTATUS(wstatus) != 0 {
                unsafe { libc::close(received_fd) };
                bail!(
                    "authopen exited with error code {} ({})",
                    libc::WEXITSTATUS(wstatus),
                    stderr.trim()
                );
            }

            // 5. Bypass the buffer cache (equivalent to O_DIRECT on Linux)
            unsafe { libc::fcntl(received_fd, F_NOCACHE, 1) };

            Ok(unsafe { File::from_raw_fd(received_fd) })
        }
    }
}

/// Open a device path with privilege escalation via authopen when needed.
///
/// Tries `authopen` first (shows the native macOS auth dialog if not root).
/// A cancelled dialog is the user's answer and is returned as such (R11); any
/// other refusal falls back to a direct `open(2)`, which works under sudo.
///
/// Never uses `O_EXLOCK`, which is unreliable on raw character devices and
/// causes intermittent `EBUSY` errors even after a successful unmount.
fn open_device(path: &str, flags: libc::c_int) -> Result<File> {
    // Only escalate when escalation can actually work. Under `sudo` the old
    // unconditional authopen-first forked a helper that waited on a dialog no
    // terminal session can show, and the parent blocked in recvmsg behind it —
    // the process just appeared to hang.
    // Goes through the same descriptor cache as the read path, so a restore
    // that follows an inspect (or any second operation on the device) reuses
    // the first prompt's descriptor instead of raising another dialog.
    match authopen_blocked_reason() {
        Some(reason) => log::info!("skipping authopen for {path}: {reason}"),
        None => match cached_authopen(path, flags).and_then(|d| Ok(d.dup_as_file()?)) {
            Ok(file) => return Ok(file),
            Err(e) => {
                if is_authorization_cancelled(&e) {
                    return Err(e);
                }
                log::warn!("authopen warning: {} — falling back to direct open", e);
            }
        },
    }

    // Direct open fallback (used when the app is already running as root via sudo)
    let c_path = CString::new(path).context("invalid device path")?;
    let mut last_err: Option<std::io::Error> = None;

    for attempt in 0..5 {
        if attempt > 0 {
            std::thread::sleep(std::time::Duration::from_millis(500));
            log::info!("Retrying open of {} (attempt {}/5)...", path, attempt + 1);
        }

        let fd = unsafe { libc::open(c_path.as_ptr(), flags) };
        if fd >= 0 {
            unsafe { libc::fcntl(fd, F_NOCACHE, 1) };
            log::info!("Opened {} directly (attempt {})", path, attempt + 1);
            return Ok(unsafe { File::from_raw_fd(fd) });
        }

        let err = std::io::Error::last_os_error();
        let raw = err.raw_os_error().unwrap_or(0);

        if raw == libc::EPERM {
            let hint = if running_as_root() {
                "even as root -- the disk may be protected by SIP or be a live system volume"
            } else {
                "run with sudo, or launch the GUI so it can prompt for administrator rights"
            };
            return Err(anyhow::anyhow!(err)
                .context(format!("permission denied opening {} ({})", path, hint)));
        }
        if raw != libc::EBUSY {
            return Err(anyhow::anyhow!(err).context(format!("cannot open {}", path)));
        }

        last_err = Some(err);
    }

    Err(anyhow::anyhow!(last_err.unwrap()).context(format!(
        "{} is busy after 5 attempts -- something still holds it. Unmount every \
         volume on the disk first: `diskutil unmountDisk {}`",
        path,
        bsd_name_from_path(Path::new(path)),
    )))
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

/// Open a device for read-only inspection without unmounting or claiming.
///
/// Used by the Inspect tab, which runs on the GUI thread and cannot afford
/// the DA unmount/claim latency. Since inspect is non-destructive, exclusive
/// access is not required.
///
/// Tries a direct `O_RDONLY` open first; on `EPERM` or `EACCES`, escalates
/// via `authopen` so the user is prompted for administrator credentials once.
pub(crate) fn open_device_for_inspect(path: &Path) -> Result<File> {
    let path_str = path.to_string_lossy();
    let is_device = path_str.starts_with("/dev/disk") || path_str.starts_with("/dev/rdisk");

    if is_device {
        let raw_device = if path_str.starts_with("/dev/disk") {
            format!("/dev/r{}", &path_str[5..])
        } else {
            path_str.to_string()
        };

        let c_path = CString::new(raw_device.as_str()).context("invalid device path")?;
        let fd = unsafe { libc::open(c_path.as_ptr(), libc::O_RDONLY) };
        if fd >= 0 {
            unsafe { libc::fcntl(fd, F_NOCACHE, 1) };
            return Ok(unsafe { File::from_raw_fd(fd) });
        }

        let err = std::io::Error::last_os_error();
        // The unprivileged open failed. Escalate via authopen (the native admin
        // prompt, opening as root) for ANY errno — not just EPERM/EACCES. A
        // still-mounted or DiskArbitration-claimed disk returns EBUSY here,
        // which the old EPERM/EACCES-only gate let fall straight through to the
        // error below with no prompt at all (the reported symptom). Inspect is
        // read-only, so request O_RDONLY: root can open a mounted disk's raw
        // device read-only even when an O_RDWR open would be refused with EBUSY.
        // (Device *writes* go through open_target_for_writing, which unmounts
        // and opens O_RDWR separately.)
        log::warn!("direct open of {raw_device} failed ({err}); escalating via authopen");
        authopen_device(&raw_device, libc::O_RDONLY)
            .with_context(|| format!("cannot open {raw_device} for reading"))
    } else {
        File::open(path).with_context(|| format!("cannot open {}", path.display()))
    }
}

/// Open a target device for writing with exclusive access.
///
/// Strategy:
/// 1. `DADiskUnmount` — unmount all volumes (with retry).
/// 2. `DADiskClaim` — prevent other DiskArbitration clients from interacting.
/// 3. `authopen` — request root privileges via the native macOS auth dialog
///    and open the raw device. Falls back to a direct open if already root.
pub(crate) fn open_target_for_writing(path: &Path) -> Result<(File, Option<DiskClaim>)> {
    let path_str = path.to_string_lossy();
    let disk_name = bsd_name_from_path(path);

    // Write-protected media refuses O_RDWR for root as well (R6): say so before
    // unmounting anything or raising a prompt that cannot help.
    if da_media_writable(disk_name) == Some(false) {
        bail!(
            "{} is write-protected (media lock switch or read-only image); it cannot \
             be written to",
            path.display()
        );
    }

    // Unmount all volumes before claiming/opening
    if let Err(e) = da_unmount_disk(disk_name) {
        // Not fatal — the disk might not be mounted
        log::warn!("DA unmount warning: {}", e);
    }

    // Claim the disk to keep other DA clients (e.g. VMware) away
    let claim = match da_claim_disk(disk_name) {
        Ok(c) => c,
        Err(e) => {
            log::warn!("DA claim warning: {}", e);
            None
        }
    };

    // Use the raw device (/dev/rdiskN) for faster unbuffered writes
    let raw_device = if path_str.starts_with("/dev/disk") {
        format!("/dev/r{}", &path_str[5..])
    } else {
        path_str.to_string()
    };

    let file = open_device(&raw_device, libc::O_RDWR)?;
    Ok((file, claim))
}

/// Open a source device or image file for reading. Devices are unmounted and
/// claimed, then opened directly or via `authopen` ([`read_escalation_flags`]).
pub fn open_source_for_reading(path: &Path) -> Result<ElevatedSource> {
    let path_str = path.to_string_lossy();
    let is_device = path_str.starts_with("/dev/disk") || path_str.starts_with("/dev/rdisk");

    if is_device {
        let disk_name = bsd_name_from_path(path);

        // Unmount first; the outcome is also the mounted-ness signal below, which
        // a probe of a root-owned node can't give us (EACCES comes before EBUSY).
        let unmounted = match da_unmount_disk(disk_name) {
            Ok(()) => true,
            Err(e) => {
                log::warn!("DA unmount warning: {}", e);
                false
            }
        };

        // Claim the disk to keep other DA clients (e.g. VMware) away
        let disk_claim = match da_claim_disk(disk_name) {
            Ok(c) => c,
            Err(e) => {
                log::warn!("DA claim warning: {}", e);
                None
            }
        };

        // Use /dev/rdiskN (raw character device) for faster unbuffered reads
        let raw_device = if path_str.starts_with("/dev/disk") {
            format!("/dev/r{}", &path_str[5..])
        } else {
            path_str.to_string()
        };

        // 1. An earlier operation may already have escalated this device; reuse
        //    that descriptor rather than prompting again.
        if let Some(shared) = reuse_elevated_device(&raw_device, false) {
            return Ok(ElevatedSource {
                file: super::SourceHandle::Device(shared),
                temp_path: None,
                disk_claim,
            });
        }

        let c_path = CString::new(raw_device.as_str()).context("invalid device path")?;

        // 2. Direct open — we may be root already, or the media may need no
        //    privilege. O_RDWR first so the handle also covers a later write,
        //    then O_RDONLY regardless of why: write-protected media refuses
        //    O_RDWR with EACCES for root as well (R6), so a read-only success
        //    is the answer, not a reason to prompt.
        let mut busy = false;
        let mut rw_errno = None;
        let mut last_err = None;
        for flags in [libc::O_RDWR, libc::O_RDONLY] {
            let fd = unsafe { libc::open(c_path.as_ptr(), flags) };
            if fd >= 0 {
                unsafe { libc::fcntl(fd, F_NOCACHE, 1) };
                let file = unsafe { File::from_raw_fd(fd) };
                if let Some(rw_errno) = rw_errno {
                    log_read_only_open(&raw_device, &file, rw_errno);
                }
                return Ok(ElevatedSource {
                    file: super::SourceHandle::File(file),
                    temp_path: None,
                    disk_claim,
                });
            }
            let err = std::io::Error::last_os_error();
            busy |= err.raw_os_error() == Some(libc::EBUSY);
            if flags == libc::O_RDWR {
                rw_errno = Some(err.raw_os_error().unwrap_or(0));
            }
            last_err = Some(err);
        }

        let err = last_err.expect("the probe loop always records its last error");
        let raw = err.raw_os_error().unwrap_or(0);

        // 3. On EPERM or EACCES, escalate via authopen and cache the result, in
        //    the widest mode this disk can give ([`read_escalation_flags`]).
        if raw == libc::EPERM || raw == libc::EACCES {
            let writable = da_media_writable(disk_name).unwrap_or(true);
            let flags = read_escalation_flags(unmounted, busy, writable);
            if !writable {
                log::warn!(
                    "{raw_device} is write-protected; escalating read-only, a restore to \
                     it cannot work"
                );
            } else if flags == libc::O_RDONLY {
                log::info!("{raw_device} still has a volume mounted; escalating read-only");
            }
            // `busy` is unknowable here: the probe loop breaks on EACCES before
            // it can learn whether the device is also busy, so a read-write
            // request can be refused *after* the user has already authenticated
            // — authopen authorises, then its own open(2) returns EBUSY and it
            // exits non-zero. Reading is what was asked for, so fall back to
            // read-only rather than failing outright. A cancelled dialog is
            // the user's answer, though: no second prompt for it (R11).
            let shared = match cached_authopen(&raw_device, flags) {
                Ok(s) => s,
                Err(e) if flags != libc::O_RDONLY && !is_authorization_cancelled(&e) => {
                    log::warn!(
                        "read-write escalation of {raw_device} failed ({e:#});                          retrying read-only"
                    );
                    cached_authopen(&raw_device, libc::O_RDONLY).with_context(|| {
                        format!(
                            "cannot open {raw_device} for reading: authopen was refused                              read-write and read-only. If a volume on this disk is still                              mounted, eject it in Finder and retry"
                        )
                    })?
                }
                Err(e) => {
                    return Err(e).with_context(|| {
                        format!("cannot open {raw_device} for reading (authopen failed)")
                    })
                }
            };
            return Ok(ElevatedSource {
                file: super::SourceHandle::Device(shared),
                temp_path: None,
                disk_claim,
            });
        }

        // 4. Any other error is non-recoverable
        Err(anyhow::anyhow!(err).context(format!("cannot open {} for reading", raw_device)))
    } else {
        // Regular file — open normally
        let file = File::open(path).with_context(|| format!("cannot open {}", path.display()))?;
        Ok(ElevatedSource {
            file: super::SourceHandle::File(file),
            temp_path: None,
            disk_claim: None,
        })
    }
}

/// Convert a device path to its raw character-device form
/// (`/dev/disk6` -> `/dev/rdisk6`). Mirrors the spellings `cd-da-reader`
/// accepts, so the path we escalate is the same node it would have opened.
fn raw_device_path(device_path: &str) -> String {
    if device_path.starts_with("/dev/rdisk") {
        device_path.to_string()
    } else if let Some(rest) = device_path.strip_prefix("/dev/") {
        format!("/dev/r{rest}")
    } else if device_path.starts_with("rdisk") {
        format!("/dev/{device_path}")
    } else {
        format!("/dev/r{device_path}")
    }
}

/// Open an optical drive's raw device node for reading, escalating via
/// `authopen`.
///
/// The optical stack (`cd-da-reader`) opens `/dev/rdiskN` itself and has no
/// privilege escalation of its own, so an unprivileged app — notably the GUI,
/// which unlike a terminal has no Full Disk Access — just gets `EPERM` and the
/// user sees "failed to open drive" with no prompt and no way forward. This is
/// the same fallback [`open_source_for_reading`] performs for whole-disk
/// backups: `authopen` shows the native macOS authorization dialog and passes
/// the descriptor back over `SCM_RIGHTS`.
///
/// The flags match what `cd-da-reader` would have used on its own open, so the
/// descriptor behaves identically to the unescalated path. `O_RDONLY` is
/// deliberate: [`open_source_for_reading`] asks for `O_RDWR` because a backup
/// may later write, but a write-capable open of an optical device demands
/// exclusivity we neither need nor want for reading — and it would fail
/// outright on read-only media.
/// Take exclusive use of an optical drive for the duration of a read.
///
/// Unmounts the disc and claims it through DiskArbitration, so nothing else on
/// the system competes for the drive — Spotlight indexing, Finder, or another DA
/// client. That matters more on optical media than anywhere else: every stolen
/// read costs a physical seek, and the drive has only one head.
///
/// Reading itself does **not** require this. The raw `/dev/rdiskN` node reads
/// fine while the disc is mounted, which is exactly why the device-backed reader
/// works. This is purely about not sharing the head.
///
/// Deliberately non-fatal: a disc that will not unmount (a file still open on
/// the volume) or will not claim still reads correctly, just with competition.
/// The returned guard releases the claim on drop; `None` means we are proceeding
/// without exclusivity.
pub(crate) fn claim_optical_disc(device_path: &str) -> Option<DiskClaim> {
    let bsd = bsd_name_from_path(Path::new(device_path));

    if let Err(e) = da_unmount_disk(bsd) {
        log::info!("{device_path}: could not unmount before claiming ({e}) — continuing shared");
    }

    match da_claim_disk(bsd) {
        Ok(Some(claim)) => {
            log::info!("{device_path}: claimed for exclusive use");
            Some(claim)
        }
        Ok(None) => {
            log::info!("{device_path}: exclusive claim declined — continuing shared");
            None
        }
        Err(e) => {
            log::info!("{device_path}: exclusive claim failed ({e}) — continuing shared");
            None
        }
    }
}

pub fn authopen_optical_device(device_path: &str) -> Result<File> {
    authopen_device(
        &raw_device_path(device_path),
        libc::O_RDONLY | libc::O_NONBLOCK,
    )
}

#[cfg(test)]
mod authopen_reply_tests {
    use super::*;

    // Byte pairs are what the helper sends in place of a descriptor: byte 1 is
    // the errno its authorization-status table produced (see the module header).

    #[test]
    fn a_cancelled_dialog_is_named_as_such() {
        let r = authopen_refusal(
            &[0, libc::ECANCELED as u8],
            "AuthorizationCopyRights failed: The authorization was canceled by the user.\n"
                .to_string(),
        );
        assert!(r.cancelled());
        assert!(r.to_string().contains("cancelled"), "{r}");
        let err: anyhow::Error = r.into();
        let wrapped = err.context("cannot open /dev/rdisk5 for reading");
        assert!(
            is_authorization_cancelled(&wrapped),
            "must see through context"
        );
    }

    #[test]
    fn a_denied_dialog_is_not_a_cancel() {
        // Three wrong passwords: the same EACCES a write-protected card gives,
        // told apart by which of authopen's two opens complained.
        let r = authopen_refusal(
            &[0, libc::EACCES as u8],
            "AuthorizationCopyRights failed: The authorization was denied.\n".to_string(),
        );
        assert!(!r.cancelled());
        let text = r.to_string();
        assert!(text.contains("denied"), "{text}");
        assert!(!is_authorization_cancelled(&r.into()));
    }

    #[test]
    fn a_device_root_cannot_open_reports_the_errno() {
        let r = authopen_refusal(
            &[0, libc::EACCES as u8],
            "couldn't open /dev/rdisk7: Permission denied\n".to_string(),
        );
        let text = r.to_string();
        assert!(text.contains("even as root"), "{text}");
        assert!(text.contains("Permission denied"), "{text}");
    }

    #[test]
    fn a_busy_device_after_authorization_keeps_ebusy() {
        // What the read-only retry in open_source_for_reading keys on.
        let r = authopen_refusal(&[0, libc::EBUSY as u8], String::new());
        assert_eq!(r.errno, libc::EBUSY);
        assert!(!r.cancelled());
        assert!(r.to_string().contains("busy"), "{r}");
    }

    #[test]
    fn a_truncated_reply_is_not_a_cancel() {
        let r = authopen_refusal(&[0], String::new());
        assert!(!r.cancelled());
    }
}

#[cfg(test)]
mod optical_device_path_tests {
    use super::raw_device_path;

    #[test]
    fn maps_every_spelling_to_the_raw_node() {
        assert_eq!(raw_device_path("/dev/disk6"), "/dev/rdisk6");
        // Already raw — must not gain a second "r".
        assert_eq!(raw_device_path("/dev/rdisk6"), "/dev/rdisk6");
        assert_eq!(raw_device_path("disk6"), "/dev/rdisk6");
        assert_eq!(raw_device_path("rdisk6"), "/dev/rdisk6");
    }
}

#[cfg(test)]
mod escalation_gate_tests {
    use super::*;

    #[test]
    fn session_query_is_callable_and_total() {
        // Exercises the Security.framework binding: any answer is acceptable
        // (CI runners are headless), but it must not trap or hang.
        let _ = session_can_prompt();
    }

    #[test]
    fn root_never_takes_the_authopen_path() {
        // The sudo hang: authopen was forked while already root, then blocked on
        // a prompt no terminal session can show.
        let reason = authopen_blocked_reason();
        if running_as_root() {
            let reason = reason.expect("root must short-circuit authopen");
            assert!(reason.contains("root"), "unexpected reason: {reason}");
        } else if session_can_prompt() {
            assert!(reason.is_none(), "unexpected reason: {reason:?}");
        } else {
            // Headless and unprivileged: refused up front rather than hung.
            assert!(reason.is_some());
        }
    }
}

#[cfg(test)]
mod shared_device_tests {
    use super::*;
    use std::io::{Read, Seek, SeekFrom, Write};

    /// Build a `SharedDevice` over a temp file. `len` is 0 (the ioctl only
    /// answers for real devices), so `SeekFrom::End` is not exercised here.
    fn shared_over_temp(contents: &[u8]) -> (tempfile::NamedTempFile, SharedDevice) {
        let mut tmp = tempfile::NamedTempFile::new().expect("temp file");
        tmp.write_all(contents).expect("seed");
        tmp.flush().expect("flush");
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(tmp.path())
            .expect("reopen");
        let device = SharedDevice::new(Arc::new(OwnedFd::from(file)));
        (tmp, device)
    }

    #[test]
    fn two_handles_on_one_descriptor_keep_separate_offsets() {
        // The reason this type exists: a dup'd fd shares one file offset, so an
        // open Inspect session and a running backup would seek out from under
        // each other. Positional I/O keeps each handle independent.
        let (_tmp, mut a) = shared_over_temp(b"0123456789ABCDEF");
        let mut b = a.try_clone().expect("clone");

        a.seek(SeekFrom::Start(0)).unwrap();
        b.seek(SeekFrom::Start(8)).unwrap();

        let mut buf_a = [0u8; 4];
        let mut buf_b = [0u8; 4];
        a.read_exact(&mut buf_a).unwrap();
        b.read_exact(&mut buf_b).unwrap();
        assert_eq!(&buf_a, b"0123");
        assert_eq!(&buf_b, b"89AB");

        // Interleaving must not disturb the other handle's position either.
        a.read_exact(&mut buf_a).unwrap();
        b.read_exact(&mut buf_b).unwrap();
        assert_eq!(&buf_a, b"4567");
        assert_eq!(&buf_b, b"CDEF");
    }

    #[test]
    fn reads_advance_and_seek_current_is_relative() {
        let (_tmp, mut d) = shared_over_temp(b"abcdefghij");
        let mut buf = [0u8; 3];
        d.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"abc");
        assert_eq!(d.stream_position().unwrap(), 3);
        d.seek(SeekFrom::Current(2)).unwrap();
        d.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"fgh");
    }

    #[test]
    fn writes_are_positional_and_visible_to_the_other_handle() {
        let (_tmp, mut a) = shared_over_temp(b"...............");
        let mut b = a.try_clone().expect("clone");

        a.seek(SeekFrom::Start(4)).unwrap();
        a.write_all(b"XYZ").unwrap();
        // `a` advanced by exactly what it wrote, and left `b` where it was.
        assert_eq!(a.stream_position().unwrap(), 7);
        assert_eq!(b.stream_position().unwrap(), 0);

        let mut buf = [0u8; 3];
        b.seek(SeekFrom::Start(4)).unwrap();
        b.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"XYZ");
    }

    #[test]
    fn seeking_before_the_start_is_an_error_not_a_wrap() {
        let (_tmp, mut d) = shared_over_temp(b"data");
        assert!(d.seek(SeekFrom::Current(-1)).is_err());
        // The failed seek must not have moved us.
        assert_eq!(d.stream_position().unwrap(), 0);
    }

    #[test]
    fn dup_as_file_reads_the_same_bytes() {
        use std::io::Read as _;
        let (_tmp, d) = shared_over_temp(b"hello world");
        let mut file = d.dup_as_file().expect("dup");
        let mut s = String::new();
        file.read_to_string(&mut s).unwrap();
        assert_eq!(s, "hello world");
    }

    /// Serializes the tests that touch the process-wide cache — one clears it
    /// wholesale, which would otherwise yank entries out from under the other.
    static CACHE_TEST_LOCK: Mutex<()> = Mutex::new(());

    /// Seed the cache directly, standing in for a completed authopen.
    fn seed_cache(path: &str, writable: bool) -> tempfile::NamedTempFile {
        let tmp = tempfile::NamedTempFile::new().expect("temp file");
        let file = std::fs::File::open(tmp.path()).expect("open");
        ELEVATED_DEVICES.lock().unwrap().push((
            path.to_string(),
            writable,
            Arc::new(OwnedFd::from(file)),
        ));
        tmp
    }

    #[test]
    fn a_writable_entry_serves_reads_but_not_the_reverse() {
        // Decides how many prompts the user sees: a backup (read) after an
        // inspect (read) reuses the descriptor; a restore (write) after a
        // read-only inspect genuinely needs its own escalation.
        let _serialize = CACHE_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let path = "/dev/rdisk-test-rw";
        let _tmp = seed_cache(path, true);
        assert!(reuse_elevated_device(path, false).is_some(), "read hits");
        assert!(reuse_elevated_device(path, true).is_some(), "write hits");
        release_elevated_devices(Some(path));

        let path = "/dev/rdisk-test-ro";
        let _tmp = seed_cache(path, false);
        assert!(reuse_elevated_device(path, false).is_some(), "read hits");
        assert!(
            reuse_elevated_device(path, true).is_none(),
            "a read-only descriptor must not satisfy a write",
        );
        release_elevated_devices(Some(path));
        assert!(reuse_elevated_device(path, false).is_none());
    }

    #[test]
    fn a_read_escalates_read_write_unless_a_volume_is_still_mounted() {
        // One prompt for the whole session: the read path escalates read-write
        // so a later restore reuses the descriptor instead of prompting again.
        assert_eq!(read_escalation_flags(true, false, true), libc::O_RDWR);

        // ...except on a disk that would not unmount, where the kernel refuses
        // O_RDWR outright (EBUSY) and read-only is the only mode that works.
        assert_eq!(read_escalation_flags(false, false, true), libc::O_RDONLY);
        assert_eq!(read_escalation_flags(true, true, true), libc::O_RDONLY);
        assert_eq!(read_escalation_flags(false, true, true), libc::O_RDONLY);
    }

    #[test]
    fn write_protected_media_is_never_escalated_read_write() {
        // R6: a locked SD card refuses O_RDWR with EACCES for root too, so a
        // read-write prompt would authenticate the user for nothing and then
        // fail. Read-only is the only mode that can succeed.
        assert_eq!(read_escalation_flags(true, false, false), libc::O_RDONLY);
        assert_eq!(read_escalation_flags(false, true, false), libc::O_RDONLY);
    }

    #[test]
    fn a_regular_file_has_no_writability_verdict() {
        // DKIOCISWRITABLE only answers for disk devices; a plain file must give
        // None so the caller falls back to the plain "opened read-only" wording.
        let tmp = tempfile::NamedTempFile::new().expect("temp file");
        let file = std::fs::File::open(tmp.path()).expect("open");
        assert_eq!(media_is_writable(&file), None);
    }

    #[test]
    fn a_writable_descriptor_means_a_later_restore_never_prompts() {
        // What the read-write escalation buys: inspect caches a writable fd, and
        // open_target_for_writing's O_RDWR request finds it instead of prompting.
        let _serialize = CACHE_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let path = "/dev/rdisk-test-inspect-then-restore";
        let _tmp = seed_cache(path, true);
        let needs_write = libc::O_RDWR & libc::O_ACCMODE != libc::O_RDONLY;
        assert!(needs_write, "O_RDWR must read as a write request");
        assert!(
            reuse_elevated_device(path, needs_write).is_some(),
            "a restore after an inspect must reuse the inspect's descriptor",
        );
        release_elevated_devices(Some(path));
    }

    #[test]
    fn releasing_the_cache_is_idempotent_and_path_scoped() {
        // No entries for these paths, so both calls are no-ops — the point is
        // that neither panics or poisons the lock.
        let _serialize = CACHE_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        release_elevated_devices(Some("/dev/disk99"));
        release_elevated_devices(None);
        assert!(reuse_elevated_device("/dev/rdisk99", false).is_none());
    }
}
