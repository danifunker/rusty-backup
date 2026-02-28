mod sudo;

use std::ffi::{c_void, CString};
use std::fs::File;
use std::os::unix::io::FromRawFd;
use std::path::{Path, PathBuf};
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
    kDADiskClaimOptionDefault, kDADiskDescriptionDeviceInternalKey,
    kDADiskDescriptionDeviceModelKey, kDADiskDescriptionDeviceProtocolKey,
    kDADiskDescriptionMediaBSDNameKey, kDADiskDescriptionMediaRemovableKey,
    kDADiskDescriptionMediaSizeKey, kDADiskDescriptionMediaWritableKey,
    kDADiskDescriptionVolumeKindKey, kDADiskDescriptionVolumePathKey, kDADiskUnmountOptionForce,
    kDADiskUnmountOptionWhole, kDAReturnExclusiveAccess, DADisk, DADiskClaimCallback,
    DADiskClaimReleaseCallback, DADiskUnmountCallback, DADissenter, DASession,
};
use objc2_io_kit::{
    kIOMainPortDefault, IOIteratorNext, IOObjectRelease, IORegistryEntryCreateCFProperties,
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

/// macOS fcntl command to bypass the buffer cache (equivalent to O_DIRECT on Linux).
const F_NOCACHE: libc::c_int = 48;

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
            eprintln!(
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
    eprintln!("DA: another client tried to steal our disk claim — denied");
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

    // Schedule on the MAIN run loop so the release-denial callback fires
    // even while the worker thread does blocking disk I/O.
    let main_run_loop = CFRunLoop::main().context("failed to get main CFRunLoop")?;
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

    // Spin-wait for the claim completion callback (fires on main run loop,
    // which is pumped by the GUI thread).
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !unsafe { &*state }.done.load(Ordering::SeqCst) {
        if std::time::Instant::now() >= deadline {
            unsafe {
                session.unschedule_from_run_loop(&main_run_loop, mode);
                let _ = Box::from_raw(state);
            }
            eprintln!(
                "DA claim of {} timed out — proceeding without exclusive access",
                bsd_name
            );
            return Ok(None);
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }

    let state = unsafe { Box::from_raw(state) };

    if !state.ok.load(Ordering::SeqCst) {
        unsafe { session.unschedule_from_run_loop(&main_run_loop, mode) };
        eprintln!(
            "DA claim of {} failed — proceeding without exclusive access",
            bsd_name
        );
        return Ok(None);
    }

    eprintln!("DA: claimed {} for exclusive access", bsd_name);

    Ok(Some(DiskClaim {
        _session: session,
        disk,
        main_run_loop,
    }))
}

// ---------------------------------------------------------------------------
// authopen-based privileged device access
// ---------------------------------------------------------------------------

/// Receive a file descriptor sent via `SCM_RIGHTS` ancillary data on a Unix socket.
fn receive_fd_from_socket(sock: libc::c_int) -> Result<libc::c_int> {
    use std::mem;

    let mut data = 0u8;
    let mut iov = libc::iovec {
        iov_base: &mut data as *mut u8 as *mut c_void,
        iov_len: 1,
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
            bail!("recvmsg failed: {}", err);
        }
        break r;
    };

    if ret <= 0 {
        bail!("recvmsg: authopen did not send a file descriptor");
    }

    let cmsg = unsafe { libc::CMSG_FIRSTHDR(&msg) };
    if cmsg.is_null() {
        bail!("recvmsg: no ancillary control message (authopen may have failed)");
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

    Ok(fd)
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
    // 1. Create IPC socket pair:
    //    sv[0] — parent receives the fd via SCM_RIGHTS
    //    sv[1] — child (authopen) sends the fd via stdout
    let mut sv = [-1i32; 2];

    if unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, sv.as_mut_ptr()) } != 0 {
        bail!("socketpair failed: {}", std::io::Error::last_os_error());
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
            }
            bail!("fork failed: {}", std::io::Error::last_os_error());
        }
        0 => {
            // Child process — only async-signal-safe calls between fork and exec
            unsafe {
                // Close parent-side end
                libc::close(sv[0]);

                // Wire socket to stdout (authopen sends fd via -stdoutpipe on stdout)
                libc::dup2(sv[1], libc::STDOUT_FILENO);
                libc::close(sv[1]);

                libc::execv(authopen_path.as_ptr(), argv.as_ptr());
                libc::exit(-1); // exec failed
            }
        }
        child_pid => {
            // Parent process
            unsafe {
                // Close child-side end
                libc::close(sv[1]);
            }

            // 3. Receive the file descriptor via SCM_RIGHTS (retry on EINTR)
            let received_fd = match receive_fd_from_socket(sv[0]) {
                Ok(fd) => {
                    unsafe { libc::close(sv[0]) };
                    fd
                }
                Err(e) => {
                    unsafe {
                        libc::close(sv[0]);
                        // Reap child to avoid zombie
                        let mut wstatus = 0i32;
                        libc::waitpid(child_pid, &mut wstatus, 0);
                    }
                    return Err(e);
                }
            };

            // 4. Wait for authopen to exit (retry on EINTR)
            let mut wstatus = 0i32;
            loop {
                let r = unsafe { libc::waitpid(child_pid, &mut wstatus, 0) };
                if r == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
                    continue;
                }
                break;
            }

            if libc::WIFEXITED(wstatus) && libc::WEXITSTATUS(wstatus) != 0 {
                unsafe { libc::close(received_fd) };
                bail!(
                    "authopen exited with error code {}",
                    libc::WEXITSTATUS(wstatus)
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
/// If authopen fails for a reason other than user cancellation, falls back
/// to a direct `open(2)` — which works when the app is already root via sudo.
///
/// Never uses `O_EXLOCK`, which is unreliable on raw character devices and
/// causes intermittent `EBUSY` errors even after a successful unmount.
fn open_device(path: &str, flags: libc::c_int) -> Result<File> {
    // Try authopen first
    match authopen_device(path, flags) {
        Ok(file) => return Ok(file),
        Err(e) => {
            let msg = e.to_string().to_lowercase();
            if msg.contains("cancelled") || msg.contains("canceled") {
                return Err(e);
            }
            eprintln!("authopen warning: {} — falling back to direct open", e);
        }
    }

    // Direct open fallback (used when the app is already running as root via sudo)
    let c_path = CString::new(path).context("invalid device path")?;
    let mut last_err = None;

    for attempt in 0..5 {
        if attempt > 0 {
            std::thread::sleep(std::time::Duration::from_millis(500));
            eprintln!("Retrying open of {} (attempt {}/5)...", path, attempt + 1);
        }

        let fd = unsafe { libc::open(c_path.as_ptr(), flags) };
        if fd >= 0 {
            unsafe { libc::fcntl(fd, F_NOCACHE, 1) };
            eprintln!("Opened {} directly (attempt {})", path, attempt + 1);
            return Ok(unsafe { File::from_raw_fd(fd) });
        }

        let err = std::io::Error::last_os_error();
        let raw = err.raw_os_error().unwrap_or(0);

        if raw == libc::EPERM {
            return Err(anyhow::anyhow!(err).context(format!(
                "permission denied opening {} — run without sudo or check disk permissions",
                path
            )));
        }
        if raw != libc::EBUSY {
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
        let raw = err.raw_os_error().unwrap_or(0);

        if raw == libc::EPERM || raw == libc::EACCES {
            return authopen_device(&raw_device, libc::O_RDWR)
                .with_context(|| format!("cannot open {} for inspection", raw_device));
        }

        Err(anyhow::anyhow!(err).context(format!("cannot open {} for reading", raw_device)))
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

    // Unmount all volumes before claiming/opening
    if let Err(e) = da_unmount_disk(disk_name) {
        // Not fatal — the disk might not be mounted
        eprintln!("DA unmount warning: {}", e);
    }

    // Claim the disk to keep other DA clients (e.g. VMware) away
    let claim = match da_claim_disk(disk_name) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("DA claim warning: {}", e);
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

/// Open a source device or image file for reading.
///
/// For device paths (`/dev/disk*`), unmounts all volumes and opens with
/// `authopen` for privileged access. For regular files, opens normally.
///
/// Read path: tries a direct `O_RDONLY` open first (works on removable media
/// accessible without root), falling back to `authopen` (always `O_RDWR`)
/// only on `EPERM`.
pub fn open_source_for_reading(path: &Path) -> Result<ElevatedSource> {
    let path_str = path.to_string_lossy();
    let is_device = path_str.starts_with("/dev/disk") || path_str.starts_with("/dev/rdisk");

    if is_device {
        let disk_name = bsd_name_from_path(path);

        // Unmount all volumes before claiming/opening
        if let Err(e) = da_unmount_disk(disk_name) {
            eprintln!("DA unmount warning: {}", e);
        }

        // Claim the disk to keep other DA clients (e.g. VMware) away
        let disk_claim = match da_claim_disk(disk_name) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("DA claim warning: {}", e);
                None
            }
        };

        // Use /dev/rdiskN (raw character device) for faster unbuffered reads
        let raw_device = if path_str.starts_with("/dev/disk") {
            format!("/dev/r{}", &path_str[5..])
        } else {
            path_str.to_string()
        };

        let c_path = CString::new(raw_device.as_str()).context("invalid device path")?;

        // 1. Try a direct O_RDONLY open first — succeeds if we're already root
        //    or if the media is accessible without privileges.
        let fd = unsafe { libc::open(c_path.as_ptr(), libc::O_RDONLY) };
        if fd >= 0 {
            unsafe { libc::fcntl(fd, F_NOCACHE, 1) };
            return Ok(ElevatedSource {
                file: unsafe { File::from_raw_fd(fd) },
                temp_path: None,
                disk_claim,
            });
        }

        let err = std::io::Error::last_os_error();
        let raw = err.raw_os_error().unwrap_or(0);

        // 2. On EPERM or EACCES, escalate via authopen (authopen always opens
        //    O_RDWR, which is fine for reading).
        if raw == libc::EPERM || raw == libc::EACCES {
            let file = authopen_device(&raw_device, libc::O_RDWR).with_context(|| {
                format!("cannot open {} for reading (authopen failed)", raw_device)
            })?;
            return Ok(ElevatedSource {
                file,
                temp_path: None,
                disk_claim,
            });
        }

        // 3. Any other error is non-recoverable
        Err(anyhow::anyhow!(err).context(format!("cannot open {} for reading", raw_device)))
    } else {
        // Regular file — open normally
        let file = File::open(path).with_context(|| format!("cannot open {}", path.display()))?;
        Ok(ElevatedSource {
            file,
            temp_path: None,
            disk_claim: None,
        })
    }
}
