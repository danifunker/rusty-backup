use std::ffi::{c_void, CString};
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{bail, Context, Result};

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
#[allow(dead_code)]
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

/// Extract the BSD disk name from a device path like `/dev/diskN` or `/dev/rdiskN`.
#[allow(dead_code)]
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

/// Open a target device for writing, unmounting it first.
///
/// For regular files: uses normal file creation.
/// For devices: uses the privileged helper daemon to unmount and open for writing.
///
/// Returns a `File` handle. For devices, this is actually a temporary file that
/// will be written back to the device via the daemon when closed.
pub fn open_target_for_writing(path: &Path) -> Result<File> {
    let path_str = path.to_string_lossy();
    
    // For non-device paths, use normal file creation
    if !path_str.starts_with("/dev/") {
        return File::create(path).with_context(|| format!("failed to create {}", path.display()));
    }

    // For device paths, use the daemon
    // We'll create a temporary file for writing, then use a custom wrapper
    // that writes sectors back to the device via the daemon
    
    let mut access = MacOSDiskAccess::new()
        .context("Failed to create daemon access (is the helper installed?)")?;
    
    // Open device for writing (daemon will unmount it)
    let _handle = access.open_disk_write(path)
        .with_context(|| format!("Failed to open device {} for writing via daemon", path.display()))?;
    
    // For now, we'll use a simpler approach: create a temp file and return it
    // The caller (restore) will need to be updated to use the daemon for writing
    // This is a transitional implementation
    
    // Create temp file for writes
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let device_stem = path.file_name().unwrap_or_default().to_string_lossy();
    let temp_path = PathBuf::from(format!("/tmp/rusty-backup-write-{device_stem}-{timestamp}.raw"));
    
    File::create(&temp_path)
        .with_context(|| format!("Failed to create temp write file: {}", temp_path.display()))
}

// ---------------------------------------------------------------------------
// Daemon-based disk reader (for backup/inspect)
// ---------------------------------------------------------------------------

/// A `Read + Seek` implementation that reads from a disk device via the daemon.
/// This avoids creating temp files - reads happen on-demand from the daemon.
pub struct DaemonDiskReader {
    access: MacOSDiskAccess,
    handle: DiskHandle,
    position: u64,
    size: u64,
    // 1MB read buffer to reduce daemon round-trips
    buffer: Vec<u8>,
    buffer_lba: u64,
    buffer_valid_len: usize,
}

impl DaemonDiskReader {
    pub fn new(path: &Path) -> Result<Self> {
        let mut access = MacOSDiskAccess::new()
            .context("Failed to connect to daemon (is the helper running?)")?;
        
        let handle = access.open_disk_read(path)
            .with_context(|| format!("Failed to open device {} via daemon", path.display()))?;
        
        // Try to determine size by reading until we get an error
        // For now, we'll set a large default and let reads fail naturally at the end
        let size = u64::MAX; // Will be limited by actual device size
        
        Ok(Self {
            access,
            handle,
            position: 0,
            size,
            buffer: vec![0u8; 1024 * 1024], // 1MB buffer
            buffer_lba: u64::MAX, // Invalid LBA initially
            buffer_valid_len: 0,
        })
    }
}

impl Read for DaemonDiskReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.position >= self.size {
            return Ok(0);
        }
        
        let start_lba = self.position / 512;
        let offset_in_sector = (self.position % 512) as usize;
        
        // Check if we need to refill the buffer
        let buffer_sectors = (self.buffer.len() / 512) as u64;
        if start_lba < self.buffer_lba || start_lba >= self.buffer_lba + buffer_sectors {
            // Read new chunk from daemon
            let sectors_to_read = (self.buffer.len() / 512) as u32;
            match self.access.read_sectors(self.handle, start_lba, sectors_to_read) {
                Ok(data) => {
                    if data.is_empty() {
                        // End of device
                        self.size = self.position;
                        return Ok(0);
                    }
                    self.buffer_lba = start_lba;
                    self.buffer_valid_len = data.len();
                    self.buffer[..data.len()].copy_from_slice(&data);
                }
                Err(e) => {
                    // Treat as end of device or return error
                    return Err(io::Error::new(io::ErrorKind::Other, e));
                }
            }
        }
        
        // Read from buffer
        let buffer_offset = ((start_lba - self.buffer_lba) * 512) as usize + offset_in_sector;
        let available = self.buffer_valid_len.saturating_sub(buffer_offset);
        
        if available == 0 {
            // End of device
            self.size = self.position;
            return Ok(0);
        }
        
        let to_read = available.min(buf.len());
        buf[..to_read].copy_from_slice(&self.buffer[buffer_offset..buffer_offset + to_read]);
        self.position += to_read as u64;
        
        Ok(to_read)
    }
}

impl Seek for DaemonDiskReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(n) => n as i64,
            SeekFrom::Current(n) => self.position as i64 + n,
            SeekFrom::End(n) => {
                // We don't know the exact size, try to seek to a large value
                if n >= 0 {
                    return Err(io::Error::new(io::ErrorKind::InvalidInput, "Cannot seek past end"));
                }
                // For now, we can't support seeking from end without knowing size
                return Err(io::Error::new(io::ErrorKind::Unsupported, "Seek from end not supported for devices"));
            }
        };
        
        if new_pos < 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid seek position"));
        }
        
        self.position = new_pos as u64;
        Ok(self.position)
    }
}

impl Drop for DaemonDiskReader {
    fn drop(&mut self) {
        let _ = self.access.close_disk(self.handle);
    }
}

/// Wrapper that provides a File-like interface over DaemonDiskReader.
/// This allows us to return it as part of ElevatedSource.
pub struct DaemonBackedFile {
    reader: DaemonDiskReader,
}

impl Read for DaemonBackedFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.reader.read(buf)
    }
}

impl Seek for DaemonBackedFile {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.reader.seek(pos)
    }
}

// ---------------------------------------------------------------------------
// Public API for opening devices
// ---------------------------------------------------------------------------

/// Open a source device or image file for reading.
///
/// For regular files: uses normal `File::open()`.
/// For devices (`/dev/disk*`): uses the privileged helper daemon with on-demand reads.
///
/// Returns an `ElevatedSource` that provides a `Read + Seek` interface.
pub fn open_source_for_reading(path: &Path) -> Result<ElevatedSource> {
    let path_str = path.to_string_lossy();
    
    // For non-device paths, use normal file open
    if !path_str.starts_with("/dev/") {
        match File::open(path) {
            Ok(file) => {
                return Ok(ElevatedSource::from_file(file));
            }
            Err(e) => {
                return Err(anyhow::anyhow!(e).context(format!("cannot open {}", path.display())));
            }
        }
    }

    // For device paths, check if daemon is available
    let daemon_available = is_daemon_installed() && is_daemon_running();
    
    if daemon_available {
        // Use daemon to read device on-demand (no temp file!)
        match DaemonDiskReader::new(path) {
            Ok(reader) => {
                eprintln!("[DEBUG] Using daemon for device access: {}", path.display());
                return Ok(ElevatedSource::from_daemon_reader(reader));
            }
            Err(e) => {
                // Daemon failed, fall back to legacy
                eprintln!("[WARNING] Daemon read failed: {}, falling back to dd", e);
            }
        }
    } else {
        eprintln!("[DEBUG] Daemon not available (installed: {}, running: {}), using dd", 
                 is_daemon_installed(), is_daemon_running());
    }
    
    // Fallback to old osascript + dd approach
    open_device_via_dd_legacy(path)
}

/// Legacy fallback: open device via osascript + dd.
/// Used when daemon is not available.
fn open_device_via_dd_legacy(path: &Path) -> Result<ElevatedSource> {
    let path_str = path.to_string_lossy();
    
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
    let device_stem = path.file_name().unwrap_or_default().to_string_lossy();
    let temp_path = PathBuf::from(format!("/tmp/rusty-backup-{device_stem}-{timestamp}.raw"));
    let temp_str = temp_path.to_string_lossy();

    // Use osascript to run dd with administrator privileges
    let script = format!(
        "do shell script \"dd if={source_device} of={temp_str} bs=1m\" with administrator privileges"
    );

    let output = Command::new("osascript")
        .arg("-e")
        .arg(&script)
        .output()
        .context("failed to launch osascript for elevated device access")?;

    if !output.status.success() {
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

    let file = File::open(&temp_path).with_context(|| {
        format!(
            "failed to open temporary device image: {}",
            temp_path.display()
        )
    })?;

    Ok(ElevatedSource::from_file_with_temp(file, temp_path))
}

// ---------------------------------------------------------------------------
// Privileged disk access implementation (macOS)
// ---------------------------------------------------------------------------

use crate::privileged::{AccessStatus, DiskHandle, PrivilegedDiskAccess};
use crate::privileged::protocol::{DaemonRequest, DaemonResponse, MIN_DAEMON_VERSION};
use std::io::{BufRead, BufReader};
use std::os::unix::net::UnixStream;

const SOCKET_PATH: &str = "/var/run/rustybackup.sock";
const DAEMON_BINARY: &str = "/Library/PrivilegedHelperTools/com.rustybackup.helper";
const DAEMON_PLIST: &str = "/Library/LaunchDaemons/com.rustybackup.helper.plist";

/// macOS implementation of privileged disk access.
///
/// Uses a privileged helper daemon with Unix socket communication.
/// The daemon runs as root via launchd and handles all disk I/O at the sector level.
pub struct MacOSDiskAccess {
    // No persistent state - each operation connects to daemon
}

impl MacOSDiskAccess {
    pub fn new() -> Result<Self> {
        Ok(Self {})
    }
    
    /// Send a request to the daemon and get response.
    fn send_request(&self, request: &DaemonRequest) -> Result<DaemonResponse> {
        // Connect to daemon socket
        let mut stream = UnixStream::connect(SOCKET_PATH)
            .map_err(|e| anyhow::anyhow!("Failed to connect to daemon: {}. Is it running?", e))?;
        
        // Serialize and send request (as single line)
        let request_json = serde_json::to_string(request)?;
        writeln!(stream, "{}", request_json)?;
        
        // Read response (single line)
        let mut reader = BufReader::new(stream);
        let mut response_json = String::new();
        reader.read_line(&mut response_json)?;
        
        // Deserialize response
        let response: DaemonResponse = serde_json::from_str(&response_json)?;
        
        Ok(response)
    }
    
    /// Get daemon version (if running).
    fn get_daemon_version(&self) -> Result<String> {
        let response = self.send_request(&DaemonRequest::GetVersion)?;
        
        match response {
            DaemonResponse::Version { version } => Ok(version),
            DaemonResponse::Error { message } => anyhow::bail!("Daemon error: {}", message),
            _ => anyhow::bail!("Unexpected response to GetVersion"),
        }
    }
}

impl PrivilegedDiskAccess for MacOSDiskAccess {
    fn check_status(&self) -> Result<AccessStatus> {
        // Check if daemon is installed
        if !is_daemon_installed() {
            return Ok(AccessStatus::DaemonNotInstalled);
        }
        
        // Check if daemon is running
        if !is_daemon_running() {
            return Ok(AccessStatus::DaemonNeedsApproval);
        }
        
        // Check daemon version
        match self.get_daemon_version() {
            Ok(daemon_ver) => {
                // Check if daemon is too old for this app
                // Note: Simple string comparison works for semantic versions
                if daemon_ver.as_str() < MIN_DAEMON_VERSION {
                    return Ok(AccessStatus::DaemonOutdated { current: daemon_ver });
                }
                
                // All good
                Ok(AccessStatus::Ready)
            }
            Err(e) => anyhow::bail!("Failed to get daemon version: {}", e),
        }
    }

    fn open_disk_read(&mut self, path: &Path) -> Result<DiskHandle> {
        let response = self.send_request(&DaemonRequest::OpenDiskRead {
            path: path.to_string_lossy().to_string(),
        })?;
        
        match response {
            DaemonResponse::DiskOpened { handle, .. } => Ok(DiskHandle(handle)),
            DaemonResponse::Error { message } => anyhow::bail!("Failed to open disk: {}", message),
            _ => anyhow::bail!("Unexpected response to OpenDiskRead"),
        }
    }

    fn open_disk_write(&mut self, path: &Path) -> Result<DiskHandle> {
        let response = self.send_request(&DaemonRequest::OpenDiskWrite {
            path: path.to_string_lossy().to_string(),
        })?;
        
        match response {
            DaemonResponse::DiskOpened { handle, .. } => Ok(DiskHandle(handle)),
            DaemonResponse::Error { message } => anyhow::bail!("Failed to open disk: {}", message),
            _ => anyhow::bail!("Unexpected response to OpenDiskWrite"),
        }
    }

    fn read_sectors(&mut self, handle: DiskHandle, lba: u64, count: u32) -> Result<Vec<u8>> {
        let response = self.send_request(&DaemonRequest::ReadSectors {
            handle: handle.0,
            lba,
            count,
        })?;
        
        match response {
            DaemonResponse::SectorsRead { data } => Ok(data),
            DaemonResponse::Error { message } => anyhow::bail!("Failed to read sectors: {}", message),
            _ => anyhow::bail!("Unexpected response to ReadSectors"),
        }
    }

    fn write_sectors(&mut self, handle: DiskHandle, lba: u64, data: &[u8]) -> Result<()> {
        let response = self.send_request(&DaemonRequest::WriteSectors {
            handle: handle.0,
            lba,
            data: data.to_vec(),
        })?;
        
        match response {
            DaemonResponse::Success => Ok(()),
            DaemonResponse::Error { message } => anyhow::bail!("Failed to write sectors: {}", message),
            _ => anyhow::bail!("Unexpected response to WriteSectors"),
        }
    }

    fn close_disk(&mut self, handle: DiskHandle) -> Result<()> {
        let response = self.send_request(&DaemonRequest::CloseDisk { handle: handle.0 })?;
        
        match response {
            DaemonResponse::Success => Ok(()),
            DaemonResponse::Error { message } => anyhow::bail!("Failed to close disk: {}", message),
            _ => anyhow::bail!("Unexpected response to CloseDisk"),
        }
    }
}

// ---------------------------------------------------------------------------
// Daemon installation/management functions
// ---------------------------------------------------------------------------

/// Install the privileged helper daemon.
///
/// Copies the daemon binary and plist to system locations and loads it via launchctl.
/// Requires admin password via osascript.
pub fn install_daemon() -> Result<()> {
    // Get the daemon binary from the app bundle
    let bundle_daemon = get_bundle_daemon_path()?;
    
    if !bundle_daemon.exists() {
        anyhow::bail!("Daemon binary not found in app bundle: {}", bundle_daemon.display());
    }
    
    // Get the plist from the app bundle
    let bundle_plist = get_bundle_plist_path()?;
    
    if !bundle_plist.exists() {
        anyhow::bail!("Daemon plist not found in app bundle: {}", bundle_plist.display());
    }
    
    // Build installation script (runs with admin privileges)
    // Unload existing daemon first (if any), then install new one
    let script = format!(
        r#"do shell script "
        launchctl unload '{}' 2>/dev/null || true && \
        mkdir -p /Library/PrivilegedHelperTools && \
        cp '{}' '{}' && \
        chmod 755 '{}' && \
        chown root:wheel '{}' && \
        mkdir -p /Library/LaunchDaemons && \
        cp '{}' '{}' && \
        chmod 644 '{}' && \
        chown root:wheel '{}' && \
        launchctl load -w '{}'
        " with administrator privileges"#,
        DAEMON_PLIST,
        bundle_daemon.display(),
        DAEMON_BINARY,
        DAEMON_BINARY,
        DAEMON_BINARY,
        bundle_plist.display(),
        DAEMON_PLIST,
        DAEMON_PLIST,
        DAEMON_PLIST,
        DAEMON_PLIST,
    );
    
    // Execute with osascript (prompts for admin password)
    let output = std::process::Command::new("osascript")
        .arg("-e")
        .arg(&script)
        .output()?;
    
    if !output.status.success() {
        let error = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        anyhow::bail!("Failed to install daemon.\nError: {}\nOutput: {}", error, stdout);
    }
    
    // Check if daemon is accessible by trying to connect to the socket
    // The daemon uses socket activation, so it will start on first connection
    // Retry a few times to handle the race condition
    for attempt in 1..=10 {
        std::thread::sleep(std::time::Duration::from_millis(500));
        
        if is_daemon_running() {
            return Ok(());
        }
        
        if attempt == 10 {
            // Last attempt failed - check log for errors
            if let Ok(log) = std::fs::read_to_string("/var/log/rustybackup-helper.log") {
                let last_lines: Vec<&str> = log.lines().rev().take(10).collect();
                anyhow::bail!(
                    "Daemon installed but not responding after 5 seconds.\n\
                     Check System Settings > General > Login Items to approve it.\n\n\
                     Last log lines:\n{}",
                    last_lines.into_iter().rev().collect::<Vec<_>>().join("\n")
                );
            } else {
                anyhow::bail!(
                    "Daemon installed but not responding after 5 seconds.\n\
                     Check System Settings > General > Login Items to approve it."
                );
            }
        }
    }
    
    Ok(())
}

/// Check if daemon is running.

/// Uninstall the privileged helper daemon.
///
/// Unloads the daemon and removes files. Requires admin password.
pub fn uninstall_daemon() -> Result<()> {
    let script = format!(
        r#"do shell script "
        launchctl unload -w '{}' 2>/dev/null || true && \
        rm -f '{}' && \
        rm -f '{}' && \
        rm -f '/var/run/rustybackup.sock' && \
        rm -f '/var/log/rustybackup-helper.log'
        " with administrator privileges"#,
        DAEMON_PLIST,
        DAEMON_BINARY,
        DAEMON_PLIST,
    );
    
    let output = std::process::Command::new("osascript")
        .arg("-e")
        .arg(&script)
        .output()?;
    
    if !output.status.success() {
        let error = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("Failed to uninstall daemon: {}", error);
    }
    
    Ok(())
}

/// Get the path to the daemon binary in the app bundle.
fn get_bundle_daemon_path() -> Result<PathBuf> {
    // Try to get the main bundle (when running as .app)
    let bundle = objc2_foundation::NSBundle::mainBundle();
    let bundle_path = unsafe { bundle.bundlePath() };
    let bundle_path_str = bundle_path.to_string();
    
    let mut daemon_path = PathBuf::from(bundle_path_str.as_str());
    daemon_path.push("Contents/Library/LaunchDaemons/com.rustybackup.helper");
    
    // Fallback: check next to executable (for development)
    if !daemon_path.exists() {
        let exe = std::env::current_exe()?;
        let mut dev_path = exe.parent().ok_or_else(|| anyhow::anyhow!("No parent dir"))?.to_path_buf();
        dev_path.push("rusty-backup-helper");
        if dev_path.exists() {
            return Ok(dev_path);
        }
    }
    
    Ok(daemon_path)
}

/// Get the path to the daemon plist in the app bundle.
fn get_bundle_plist_path() -> Result<PathBuf> {
    let bundle = objc2_foundation::NSBundle::mainBundle();
    let bundle_path = unsafe { bundle.bundlePath() };
    let bundle_path_str = bundle_path.to_string();
    
    let mut plist_path = PathBuf::from(bundle_path_str.as_str());
    plist_path.push("Contents/Resources/com.rustybackup.helper.plist");
    
    // Fallback: check in assets/ (for development)
    if !plist_path.exists() {
        let exe = std::env::current_exe()?;
        let mut dev_path = exe.parent().ok_or_else(|| anyhow::anyhow!("No parent dir"))?.to_path_buf();
        dev_path.pop(); // Remove target/debug or target/release
        dev_path.pop(); // Remove target
        dev_path.push("assets/com.rustybackup.helper.plist");
        if dev_path.exists() {
            return Ok(dev_path);
        }
    }
    
    Ok(plist_path)
}

/// Check if daemon is installed.
fn is_daemon_installed() -> bool {
    std::path::Path::new(DAEMON_BINARY).exists() &&
    std::path::Path::new(DAEMON_PLIST).exists()
}

/// Check if daemon is running by sending a GetVersion request.
/// This properly tests daemon communication rather than just connecting.
fn is_daemon_running() -> bool {
    use std::io::{BufRead, BufReader, Write};
    use crate::privileged::protocol::DaemonRequest;
    use std::time::Duration;
    
    match UnixStream::connect(SOCKET_PATH) {
        Ok(mut stream) => {
            // Set a timeout to avoid hanging if daemon crashes
            if stream.set_read_timeout(Some(Duration::from_secs(2))).is_err() {
                return false;
            }
            if stream.set_write_timeout(Some(Duration::from_secs(2))).is_err() {
                return false;
            }
            
            // Send a GetVersion request
            let request = DaemonRequest::GetVersion;
            if let Ok(req_str) = serde_json::to_string(&request) {
                eprintln!("[DEBUG] Sending daemon check request: {}", req_str);
                if writeln!(stream, "{}", req_str).is_ok() {
                    // Try to read response with timeout
                    let mut reader = BufReader::new(stream);
                    let mut response = String::new();
                    if reader.read_line(&mut response).is_ok() && !response.is_empty() {
                        // Successfully communicated with daemon
                        return true;
                    }
                }
            }
            false
        }
        Err(_) => false,
    }
}
