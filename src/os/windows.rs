use std::collections::HashMap;
use std::env;
use std::ffi::c_void;
use std::fs::File;
use std::os::windows::io::FromRawHandle;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use windows::core::PCWSTR;
use windows::Win32::Foundation::{CloseHandle, HANDLE, HWND};
use windows::Win32::Security::{
    CheckTokenMembership, CreateWellKnownSid, WinBuiltinAdministratorsSid, PSID,
};
use windows::Win32::Storage::FileSystem::{
    CreateFileW, FindFirstVolumeW, FindNextVolumeW, FindVolumeClose, GetDiskFreeSpaceExW,
    GetVolumeInformationW, GetVolumePathNamesForVolumeNameW, QueryDosDeviceW,
    FILE_FLAGS_AND_ATTRIBUTES, FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_EXISTING,
};
use windows::Win32::System::IO::DeviceIoControl;
use windows::Win32::UI::Shell::{ShellExecuteW, SEE_MASK_NOCLOSEPROCESS, SHELLEXECUTEINFOW};
use windows::Win32::UI::WindowsAndMessaging::{SHOW_WINDOW_CMD, SW_SHOW};

use crate::device::{DiskDevice, MountedPartition};

// IOCTL control codes
const IOCTL_DISK_GET_DRIVE_GEOMETRY_EX: u32 = 0x000700A0;
const IOCTL_STORAGE_QUERY_PROPERTY: u32 = 0x002D1400;
const IOCTL_VOLUME_GET_VOLUME_DISK_EXTENTS: u32 = 0x00560000;
const IOCTL_DISK_IS_WRITABLE: u32 = 0x00070024;
const FSCTL_LOCK_VOLUME: u32 = 0x00090018;
const FSCTL_DISMOUNT_VOLUME: u32 = 0x00090020;

// Generic access rights
const GENERIC_READ_ACCESS: u32 = 0x80000000;
const GENERIC_WRITE_ACCESS: u32 = 0x40000000;

/// RAII wrapper for Win32 HANDLE that calls CloseHandle on drop.
struct SafeHandle(HANDLE);

impl Drop for SafeHandle {
    fn drop(&mut self) {
        if !self.0.is_invalid() && self.0 != HANDLE::default() {
            unsafe {
                let _ = CloseHandle(self.0);
            }
        }
    }
}

/// RAII guard holding locked and dismounted volume handles.
///
/// Volumes remain locked and dismounted until this is dropped. When dropped,
/// the volume handles are closed via `CloseHandle`, releasing the locks and
/// allowing Windows to re-mount the volumes.
pub(crate) struct VolumeLockSet {
    _handles: Vec<SafeHandle>,
}

// Safety: HANDLE is an OS-level integer handle, safe to send between threads.
unsafe impl Send for VolumeLockSet {}

impl VolumeLockSet {
    pub fn empty() -> Self {
        Self {
            _handles: Vec::new(),
        }
    }
}

/// Lock and dismount all volumes residing on the given physical drive.
///
/// Opens each volume with `FSCTL_LOCK_VOLUME` + `FSCTL_DISMOUNT_VOLUME`,
/// keeping the handles open so Windows cannot re-mount the filesystems
/// while we write to the physical drive.
fn lock_and_dismount_volumes(drive_num: u32) -> Result<VolumeLockSet> {
    let volumes = enumerate_volumes();
    let target_volumes: Vec<_> = volumes
        .iter()
        .filter(|v| v.disk_numbers.contains(&drive_num))
        .collect();

    if target_volumes.is_empty() {
        log::info!("No volumes found on PhysicalDrive{}", drive_num);
        return Ok(VolumeLockSet::empty());
    }

    let mut locked = Vec::new();

    for vol in &target_volumes {
        // The GUID device name reaches volumes with no drive letter too (R15).
        let volume_path = vol.device_path();
        log::info!(
            "Locking and dismounting volume {} ({})...",
            vol.display_name(),
            volume_path
        );

        let handle = match open_device(&volume_path, GENERIC_READ_ACCESS | GENERIC_WRITE_ACCESS) {
            Some(h) => h,
            None => {
                log::warn!("Could not open volume {}, skipping", volume_path);
                continue;
            }
        };

        // Lock the volume for exclusive access
        // Win7 writes through lpBytesReturned unconditionally; a NULL there access-violates.
        let mut returned = 0u32;
        let lock_result = unsafe {
            DeviceIoControl(
                handle.0,
                FSCTL_LOCK_VOLUME,
                None,
                0,
                None,
                0,
                Some(&mut returned),
                None,
            )
        };
        match lock_result {
            Ok(_) => log::info!("Volume {} locked successfully", volume_path),
            Err(e) => log::warn!(
                "FSCTL_LOCK_VOLUME failed for {}: {} (continuing anyway)",
                volume_path,
                e
            ),
        }

        // Dismount the volume's filesystem
        let dismount_result = unsafe {
            DeviceIoControl(
                handle.0,
                FSCTL_DISMOUNT_VOLUME,
                None,
                0,
                None,
                0,
                Some(&mut returned),
                None,
            )
        };
        match dismount_result {
            Ok(_) => log::info!("Volume {} dismounted successfully", volume_path),
            Err(e) => log::warn!(
                "FSCTL_DISMOUNT_VOLUME failed for {}: {} (continuing anyway)",
                volume_path,
                e
            ),
        }

        locked.push(handle);
    }

    log::info!(
        "Locked and dismounted {} volume(s) on PhysicalDrive{}",
        locked.len(),
        drive_num
    );
    Ok(VolumeLockSet { _handles: locked })
}

/// Convert a string to null-terminated UTF-16.
fn to_wide(s: &str) -> Vec<u16> {
    s.encode_utf16().chain(std::iter::once(0)).collect()
}

/// Check if the current process is running with administrator privileges.
pub fn is_elevated() -> bool {
    unsafe {
        let mut admin_sid_buffer = [0u8; 256];
        let mut admin_sid_size = admin_sid_buffer.len() as u32;
        let admin_sid = PSID(admin_sid_buffer.as_mut_ptr() as *mut c_void);

        // windows 0.61 wrapped this nullable pointer param in `Option`; 0.58
        // (vintage Win7 build) takes the bare `PSID`.
        #[cfg(not(feature = "windows-legacy"))]
        let sid_arg = Some(admin_sid);
        #[cfg(feature = "windows-legacy")]
        let sid_arg = admin_sid;
        if CreateWellKnownSid(
            WinBuiltinAdministratorsSid,
            None,
            sid_arg,
            &mut admin_sid_size,
        )
        .is_err()
        {
            return false;
        }

        let mut is_member = Default::default();
        CheckTokenMembership(None, admin_sid, &mut is_member).is_ok() && is_member.as_bool()
    }
}

/// Quote one argument the way `CommandLineToArgvW` undoes it, so the
/// relaunched process parses the same argv this one received.
fn quote_cmdline_arg(arg: &str) -> String {
    let needs_quotes = arg.is_empty() || arg.chars().any(|c| matches!(c, ' ' | '\t' | '\n' | '"'));
    if !needs_quotes {
        return arg.to_string();
    }
    let mut out = String::with_capacity(arg.len() + 2);
    out.push('"');
    let mut backslashes = 0usize;
    for c in arg.chars() {
        match c {
            '\\' => backslashes += 1,
            '"' => {
                // Backslashes before a quote are literal only when doubled.
                out.extend(crate::compat::repeat_n('\\', backslashes * 2 + 1));
                out.push('"');
                backslashes = 0;
            }
            _ => {
                out.extend(crate::compat::repeat_n('\\', backslashes));
                out.push(c);
                backslashes = 0;
            }
        }
    }
    // Trailing backslashes precede the closing quote, so they double too.
    out.extend(crate::compat::repeat_n('\\', backslashes * 2));
    out.push('"');
    out
}

/// Join arguments into a `lpParameters` string; `None` when there are none.
fn join_cmdline_args<I: IntoIterator<Item = String>>(args: I) -> Option<String> {
    let joined: Vec<String> = args.into_iter().map(|a| quote_cmdline_arg(&a)).collect();
    if joined.is_empty() {
        None
    } else {
        Some(joined.join(" "))
    }
}

/// Request elevation by relaunching the application with UAC prompt.
///
/// This uses `ShellExecuteW` with the "runas" verb to trigger the UAC dialog.
/// The current process will exit after launching the elevated instance.
pub fn request_elevation() -> Result<()> {
    let exe_path = env::current_exe().context("failed to get executable path")?;
    let exe_path_wide = to_wide(&exe_path.to_string_lossy());
    let verb = to_wide("runas");
    // A file-association launch carries the image path in argv (R14); the
    // elevated instance must see it or the double-clicked file is lost.
    let params = join_cmdline_args(
        env::args_os()
            .skip(1)
            .map(|a| a.to_string_lossy().into_owned()),
    );
    let params_wide = params.as_deref().map(to_wide);
    let params_ptr = params_wide
        .as_ref()
        .map_or(PCWSTR::null(), |w| PCWSTR(w.as_ptr()));

    let mut exec_info = SHELLEXECUTEINFOW {
        cbSize: std::mem::size_of::<SHELLEXECUTEINFOW>() as u32,
        fMask: SEE_MASK_NOCLOSEPROCESS,
        hwnd: HWND::default(),
        lpVerb: PCWSTR(verb.as_ptr()),
        lpFile: PCWSTR(exe_path_wide.as_ptr()),
        lpParameters: params_ptr,
        lpDirectory: PCWSTR::null(),
        nShow: SW_SHOW.0,
        hInstApp: Default::default(),
        ..Default::default()
    };

    unsafe {
        if shell_execute_ex_w(&mut exec_info).is_ok() {
            // Successfully launched elevated instance; exit this one
            std::process::exit(0);
        } else {
            bail!("failed to request elevation - user may have cancelled UAC prompt");
        }
    }
}

/// ShellExecuteExW is actually ShellExecuteW in the windows crate.
/// This wrapper provides the correct signature.
unsafe fn shell_execute_ex_w(info: *mut SHELLEXECUTEINFOW) -> windows::core::Result<()> {
    // windows 0.61 made the hwnd param `Option<HWND>`; 0.58 (vintage Win7) takes
    // the bare `HWND`.
    #[cfg(not(feature = "windows-legacy"))]
    let hwnd_arg = Some((*info).hwnd);
    #[cfg(feature = "windows-legacy")]
    let hwnd_arg = (*info).hwnd;
    let result = ShellExecuteW(
        hwnd_arg,
        PCWSTR((*info).lpVerb.0),
        PCWSTR((*info).lpFile.0),
        PCWSTR((*info).lpParameters.0),
        PCWSTR((*info).lpDirectory.0),
        SHOW_WINDOW_CMD((*info).nShow),
    );
    // ShellExecuteW returns > 32 on success
    if result.0 as usize > 32 {
        Ok(())
    } else {
        Err(windows::core::Error::from(std::io::Error::last_os_error()))
    }
}

/// Map STORAGE_BUS_TYPE value to a readable string.
fn bus_type_to_string(bus_type: u32) -> String {
    match bus_type {
        3 => "ATA".to_string(),
        7 => "USB".to_string(),
        11 => "SATA".to_string(),
        12 => "SD".to_string(),
        13 => "MMC".to_string(),
        17 => "NVMe".to_string(),
        _ => String::new(),
    }
}

/// Extract a null-terminated ASCII string from a byte buffer at the given offset.
fn string_from_buffer_offset(buf: &[u8], offset: u32) -> String {
    if offset == 0 || offset as usize >= buf.len() {
        return String::new();
    }
    let start = offset as usize;
    let end = buf[start..]
        .iter()
        .position(|&b| b == 0)
        .map_or(buf.len(), |p| start + p);
    String::from_utf8_lossy(&buf[start..end]).trim().to_string()
}

/// Parse a drive number from a physical drive path like `\\.\PhysicalDriveN`.
fn drive_number_from_path(path: &str) -> Option<u32> {
    path.strip_prefix(r"\\.\PhysicalDrive")
        .or_else(|| path.strip_prefix(r"\\.\physicaldrive"))
        .and_then(|n| n.parse().ok())
}

/// Open a device handle with the given access rights. Returns None if the
/// device does not exist or access is denied.
fn open_device(device_path: &str, access: u32) -> Option<SafeHandle> {
    open_device_with_flags(device_path, access, FILE_FLAGS_AND_ATTRIBUTES(0))
}

/// Open a device handle with custom flags (for raw disk I/O).
fn open_device_with_flags(
    device_path: &str,
    access: u32,
    flags: FILE_FLAGS_AND_ATTRIBUTES,
) -> Option<SafeHandle> {
    let wide = to_wide(device_path);
    unsafe {
        CreateFileW(
            PCWSTR(wide.as_ptr()),
            access,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            None,
            OPEN_EXISTING,
            flags,
            None,
        )
        .ok()
        .map(SafeHandle)
    }
}

/// Query disk size via IOCTL_DISK_GET_DRIVE_GEOMETRY_EX.
fn query_disk_size(handle: HANDLE) -> Option<u64> {
    let mut buf = [0u8; 256];
    let mut returned = 0u32;
    let result = unsafe {
        DeviceIoControl(
            handle,
            IOCTL_DISK_GET_DRIVE_GEOMETRY_EX,
            None,
            0,
            Some(buf.as_mut_ptr() as *mut c_void),
            buf.len() as u32,
            Some(&mut returned),
            None,
        )
    };
    if result.is_err() || returned < 32 {
        return None;
    }
    // DiskSize is at offset 24 in DISK_GEOMETRY_EX (after the 24-byte DISK_GEOMETRY)
    let disk_size = i64::from_ne_bytes(buf[24..32].try_into().ok()?);
    Some(disk_size as u64)
}

/// Get the size of a physical drive using Windows IOCTL.
/// For physical drives on Windows, seeking doesn't work, so we use DeviceIoControl instead.
pub fn get_physical_drive_size(file: &File) -> Result<u64> {
    use std::os::windows::io::AsRawHandle;

    let handle = HANDLE(file.as_raw_handle());
    query_disk_size(handle)
        .context("failed to query disk size via IOCTL_DISK_GET_DRIVE_GEOMETRY_EX")
}

/// Query storage device properties via IOCTL_STORAGE_QUERY_PROPERTY.
/// Returns (removable, bus_protocol, product_name).
fn query_device_properties(handle: HANDLE) -> (bool, String, String) {
    // STORAGE_PROPERTY_QUERY: PropertyId=0 (StorageDeviceProperty), QueryType=0 (Standard)
    let query = [0u8; 12];
    let mut buf = [0u8; 1024];
    let mut returned = 0u32;
    let result = unsafe {
        DeviceIoControl(
            handle,
            IOCTL_STORAGE_QUERY_PROPERTY,
            Some(query.as_ptr() as *const c_void),
            query.len() as u32,
            Some(buf.as_mut_ptr() as *mut c_void),
            buf.len() as u32,
            Some(&mut returned),
            None,
        )
    };
    if result.is_err() || returned < 36 {
        return (false, String::new(), String::new());
    }

    // STORAGE_DEVICE_DESCRIPTOR field offsets:
    //  10: RemovableMedia (u8, BOOLEAN)
    //  16: ProductIdOffset (u32)
    //  28: BusType (u32)
    let removable = buf[10] != 0;
    let product_offset = u32::from_ne_bytes(buf[16..20].try_into().unwrap_or([0; 4]));
    let bus_type = u32::from_ne_bytes(buf[28..32].try_into().unwrap_or([0; 4]));

    let product_name = string_from_buffer_offset(&buf, product_offset);
    let bus_string = bus_type_to_string(bus_type);

    (removable, bus_string, product_name)
}

/// Check if a disk is writable via IOCTL_DISK_IS_WRITABLE.
fn is_disk_writable(handle: HANDLE) -> bool {
    // Win7 writes through lpBytesReturned unconditionally; a NULL there access-violates.
    let mut returned = 0u32;
    unsafe {
        DeviceIoControl(
            handle,
            IOCTL_DISK_IS_WRITABLE,
            None,
            0,
            None,
            0,
            Some(&mut returned),
            None,
        )
        .is_ok()
    }
}

/// A volume the mount manager knows, lettered or not.
struct VolumeInfo {
    /// `\\?\Volume{GUID}\`, the name every volume query accepts.
    guid_path: String,
    /// Drive-letter roots and folder mount points; empty when unmounted.
    mount_paths: Vec<String>,
    /// Every physical drive the volume has an extent on (spanned volumes have several).
    disk_numbers: Vec<u32>,
    filesystem: String,
    total_bytes: u64,
    available_bytes: u64,
}

impl VolumeInfo {
    /// `\\.\Volume{GUID}`: the form `CreateFileW` opens for locking.
    fn device_path(&self) -> String {
        volume_device_path(&self.guid_path)
    }

    fn drive_letter(&self) -> Option<char> {
        self.mount_paths
            .iter()
            .find_map(|p| drive_letter_of_root(p))
    }

    /// `X:` when lettered, else the folder mount point, else the GUID name.
    fn display_name(&self) -> String {
        if let Some(letter) = self.drive_letter() {
            return format!("{letter}:");
        }
        match self.mount_paths.first() {
            Some(path) => path.clone(),
            None => self
                .guid_path
                .trim_start_matches(r"\\?\")
                .trim_end_matches('\\')
                .to_string(),
        }
    }
}

/// `\\?\Volume{GUID}\` -> `\\.\Volume{GUID}`.
fn volume_device_path(guid_path: &str) -> String {
    format!(
        r"\\.\{}",
        guid_path.trim_start_matches(r"\\?\").trim_end_matches('\\')
    )
}

/// The letter of a `X:\` root; `None` for folder mount points.
fn drive_letter_of_root(root: &str) -> Option<char> {
    let mut chars = root.chars();
    let letter = chars.next()?;
    let is_root = letter.is_ascii_alphabetic()
        && chars.next() == Some(':')
        && chars.next() == Some('\\')
        && chars.next().is_none();
    is_root.then(|| letter.to_ascii_uppercase())
}

/// Decode a NUL-terminated UTF-16 buffer.
fn wide_to_string(buf: &[u16]) -> String {
    let end = buf.iter().position(|&c| c == 0).unwrap_or(buf.len());
    String::from_utf16_lossy(&buf[..end])
}

/// Every physical drive the volume at `device_path` has an extent on.
fn volume_disk_numbers(device_path: &str) -> Vec<u32> {
    // Desired access 0 is enough for the extents query and needs no admin.
    let handle = match open_device(device_path, 0) {
        Some(h) => h,
        None => return Vec::new(),
    };
    let mut ext_buf = vec![0u8; 4096];
    let mut returned = 0u32;
    let result = unsafe {
        DeviceIoControl(
            handle.0,
            IOCTL_VOLUME_GET_VOLUME_DISK_EXTENTS,
            None,
            0,
            Some(ext_buf.as_mut_ptr() as *mut c_void),
            ext_buf.len() as u32,
            Some(&mut returned),
            None,
        )
    };
    if result.is_err() || returned < 8 {
        return Vec::new();
    }
    parse_disk_extent_numbers(&ext_buf[..returned as usize])
}

/// Disk numbers from a VOLUME_DISK_EXTENTS buffer: count at 0, 24-byte extents from 8.
fn parse_disk_extent_numbers(buf: &[u8]) -> Vec<u32> {
    let count = u32::from_ne_bytes(buf[0..4].try_into().unwrap_or([0; 4])) as usize;
    let fit = buf.len().saturating_sub(8) / 24;
    let mut numbers: Vec<u32> = (0..count.min(fit))
        .map(|i| {
            let at = 8 + i * 24;
            u32::from_ne_bytes(buf[at..at + 4].try_into().unwrap_or([0; 4]))
        })
        .collect();
    numbers.sort_unstable();
    numbers.dedup();
    numbers
}

/// Drive-letter roots and folder mount points of a volume, in mount-manager order.
fn volume_mount_paths(guid_path: &str) -> Vec<String> {
    let name = to_wide(guid_path);
    let mut buf = vec![0u16; 1024];
    let mut needed = 0u32;
    let mut ok = unsafe {
        GetVolumePathNamesForVolumeNameW(PCWSTR(name.as_ptr()), Some(&mut buf), &mut needed)
    }
    .is_ok();
    if !ok && needed as usize > buf.len() {
        buf = vec![0u16; needed as usize];
        ok = unsafe {
            GetVolumePathNamesForVolumeNameW(PCWSTR(name.as_ptr()), Some(&mut buf), &mut needed)
        }
        .is_ok();
    }
    if !ok {
        return Vec::new();
    }
    buf.split(|&c| c == 0)
        .filter(|s| !s.is_empty())
        .map(String::from_utf16_lossy)
        .collect()
}

/// Enumerate every mount-manager volume and map each to its physical drives.
fn enumerate_volumes() -> Vec<VolumeInfo> {
    let mut volumes = Vec::new();
    let mut name = [0u16; 64];
    let find = match unsafe { FindFirstVolumeW(&mut name) } {
        Ok(h) => h,
        Err(_) => return volumes,
    };
    loop {
        let guid_path = wide_to_string(&name);
        if let Some(v) = query_volume(&guid_path) {
            volumes.push(v);
        }
        if unsafe { FindNextVolumeW(find, &mut name) }.is_err() {
            break;
        }
    }
    unsafe {
        let _ = FindVolumeClose(find);
    }
    volumes
}

/// Describe one volume; `None` when it has no extent on a physical drive (optical, RAM disk).
fn query_volume(guid_path: &str) -> Option<VolumeInfo> {
    let disk_numbers = volume_disk_numbers(&volume_device_path(guid_path));
    if disk_numbers.is_empty() {
        return None;
    }
    let mount_paths = volume_mount_paths(guid_path);

    let root_wide = to_wide(guid_path);
    let mut fs_name_buf = vec![0u16; 64];
    let filesystem = unsafe {
        if GetVolumeInformationW(
            PCWSTR(root_wide.as_ptr()),
            None,
            None,
            None,
            None,
            Some(&mut fs_name_buf),
        )
        .is_ok()
        {
            wide_to_string(&fs_name_buf)
        } else {
            String::new()
        }
    };

    let mut free_to_caller: u64 = 0;
    let mut total_bytes: u64 = 0;
    unsafe {
        let _ = GetDiskFreeSpaceExW(
            PCWSTR(root_wide.as_ptr()),
            Some(&mut free_to_caller),
            Some(&mut total_bytes),
            None,
        );
    }

    Some(VolumeInfo {
        guid_path: guid_path.to_string(),
        mount_paths,
        disk_numbers,
        filesystem,
        total_bytes,
        available_bytes: free_to_caller,
    })
}

/// Every `PhysicalDriveN` the MS-DOS device namespace lists, in numeric order.
fn physical_drive_numbers() -> Vec<u32> {
    let mut buf = vec![0u16; 64 * 1024];
    loop {
        let len = unsafe { QueryDosDeviceW(PCWSTR::null(), Some(&mut buf)) };
        if len > 0 {
            return physical_drive_numbers_from_names(&buf[..len as usize]);
        }
        // ERROR_INSUFFICIENT_BUFFER is the only retryable failure; cap the growth.
        if buf.len() >= 8 * 1024 * 1024 {
            break;
        }
        let grown = buf.len() * 4;
        buf = vec![0u16; grown];
    }
    log::warn!("QueryDosDevice failed; probing PhysicalDrive0..15 instead");
    (0..16).collect()
}

/// Pick the `PhysicalDriveN` entries out of a NUL-separated device-name list.
fn physical_drive_numbers_from_names(names: &[u16]) -> Vec<u32> {
    let mut numbers: Vec<u32> = names
        .split(|&c| c == 0)
        .filter(|s| !s.is_empty())
        .map(String::from_utf16_lossy)
        .filter_map(|n| drive_number_from_path(&format!(r"\\.\{n}")))
        .collect();
    numbers.sort_unstable();
    numbers.dedup();
    numbers
}

/// Enumerate physical disk devices on Windows.
///
/// Opens every `\\.\PhysicalDriveN` the MS-DOS device namespace lists,
/// queries it with `DeviceIoControl`, then maps each mount-manager volume,
/// lettered or not, to the physical drives it has extents on.
///
/// In debug builds, if not elevated, automatically requests elevation via UAC.
pub fn enumerate_devices() -> Vec<DiskDevice> {
    // Physical-disk enumeration needs admin. The GUI now launches `asInvoker`
    // (no requireAdministrator manifest), so it is NOT elevated by default.
    // Elevation is an explicit, up-front user action: the top-bar "Show Physical
    // Devices" button calls `request_elevation()` (whole-process UAC relaunch),
    // and the elevated instance auto-enumerates on startup. We therefore never
    // relaunch from here — doing so could fire a UAC prompt (or worse, a
    // process exit) at an arbitrary time. When not elevated we simply report no
    // devices; file-only flows keep working.
    if !is_elevated() {
        log::info!(
            "Not elevated; physical-disk enumeration returns empty. \
             Use \"Show Physical Devices\" to elevate."
        );
        return Vec::new();
    }

    let volumes = enumerate_volumes();

    // Group volumes by physical drive number; a spanned volume lands in several.
    let mut vol_map: HashMap<u32, Vec<&VolumeInfo>> = HashMap::new();
    for vol in &volumes {
        for disk in &vol.disk_numbers {
            vol_map.entry(*disk).or_default().push(vol);
        }
    }

    // The system disk is whichever carries the Windows drive, usually C:.
    let system_letter = env::var("SystemDrive")
        .ok()
        .and_then(|d| d.chars().next())
        .map(|c| c.to_ascii_uppercase())
        .unwrap_or('C');
    let system_disks: Vec<u32> = volumes
        .iter()
        .filter(|v| v.drive_letter() == Some(system_letter))
        .flat_map(|v| v.disk_numbers.iter().copied())
        .collect();

    let mut devices = Vec::new();

    for i in physical_drive_numbers() {
        let drive_path = format!(r"\\.\PhysicalDrive{i}");
        let handle = match open_device(&drive_path, GENERIC_READ_ACCESS) {
            Some(h) => h,
            None => continue,
        };

        let size_bytes = query_disk_size(handle.0).unwrap_or(0);
        let (is_removable, bus_protocol, media_name) = query_device_properties(handle.0);
        let is_read_only = !is_disk_writable(handle.0);
        let is_system = system_disks.contains(&i);

        let partitions = vol_map
            .get(&i)
            .map(|vols| {
                vols.iter()
                    .map(|v| MountedPartition {
                        name: v.display_name(),
                        mount_point: v.mount_paths.first().map(PathBuf::from).unwrap_or_default(),
                        filesystem: v.filesystem.clone(),
                        total_space: v.total_bytes,
                        available_space: v.available_bytes,
                    })
                    .collect()
            })
            .unwrap_or_default();

        devices.push(DiskDevice {
            name: format!("PhysicalDrive{i}"),
            path: PathBuf::from(drive_path),
            size_bytes,
            is_removable,
            is_read_only,
            is_system,
            bus_protocol,
            media_name,
            partitions,
        });
    }

    devices
}

/// Open a target device for writing on Windows.
///
/// Finds all volumes residing on the target physical drive, locks and
/// dismounts each one, then opens the physical drive with read+write access.
///
/// If access is denied and running in debug mode without elevation,
/// automatically requests elevation via UAC.
pub(crate) fn open_target_for_writing(path: &Path) -> Result<(File, VolumeLockSet)> {
    let path_str = path.to_string_lossy();
    let drive_num = drive_number_from_path(&path_str).context("invalid physical drive path")?;

    // Physical-disk writes need admin. Both the GUI and CLI now run `asInvoker`.
    // In the GUI, the user reaches this path only after clicking "Show Physical
    // Devices" (which relaunches the whole process elevated), so we are already
    // elevated here. We do NOT relaunch from this function: `request_elevation()`
    // exits the process, which must never happen mid-write on a worker thread.
    // If somehow not elevated, fail with a clear message instead of a cryptic
    // "Access is denied" (OS error 5).
    if !is_elevated() {
        bail!(
            "Administrator privileges required to write to disk devices. \
             Use \"Show Physical Devices\" in the GUI, or re-run rb-cli from an \
             elevated terminal (right-click -> Run as administrator)."
        );
    }

    // Lock and dismount all volumes on the target drive BEFORE opening.
    // This is required on Windows: the OS denies write access to sectors
    // belonging to a mounted volume. Without this, MBR/gap writes succeed
    // but partition data writes fail with "Access is denied" (OS error 5).
    let volume_locks = lock_and_dismount_volumes(drive_num)?;

    let wide = to_wide(&path_str);
    let handle = unsafe {
        CreateFileW(
            PCWSTR(wide.as_ptr()),
            GENERIC_READ_ACCESS | GENERIC_WRITE_ACCESS,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            None,
            OPEN_EXISTING,
            FILE_FLAGS_AND_ATTRIBUTES(0),
            None,
        )
    }
    .with_context(|| format!("cannot open {} for writing", path.display()))?;

    // Convert HANDLE to File (takes ownership — do NOT also wrap in SafeHandle)
    let file = unsafe { File::from_raw_handle(handle.0) };
    Ok((file, volume_locks))
}

/// Open a source device for reading (backup operation).
///
/// For physical drives, opens with standard flags (no FILE_FLAG_NO_BUFFERING).
/// Unbuffered I/O requires aligned buffers which complicates read operations.
/// For backup (read-only), standard buffered I/O provides adequate performance.
///
/// For regular files (image files), opens normally.
pub fn open_source_for_reading(path: &Path) -> Result<crate::os::ElevatedSource> {
    let path_str = path.to_string_lossy();
    let is_physical_drive = path_str.starts_with(r"\\.\PhysicalDrive");

    if !is_physical_drive {
        // Regular file - just open normally
        let file = File::open(path).with_context(|| format!("cannot open {}", path.display()))?;
        return Ok(crate::os::ElevatedSource {
            file: crate::os::SourceHandle::File(file),
            temp_path: None,
        });
    }

    // Physical drive - open with standard flags (no NO_BUFFERING to avoid alignment issues on reads).
    // Physical-disk reads need admin. Both GUI and CLI run `asInvoker`; the GUI
    // reaches this only after the up-front "Show Physical Devices" elevation, so
    // we are already elevated. We never relaunch here (it would exit the process
    // mid-read). If not elevated, fail clearly.
    if !is_elevated() {
        bail!(
            "Administrator privileges required to read disk devices. \
             Use \"Show Physical Devices\" in the GUI, or re-run rb-cli from an \
             elevated terminal (right-click -> Run as administrator)."
        );
    }

    let wide = to_wide(&path_str);
    let handle = unsafe {
        CreateFileW(
            PCWSTR(wide.as_ptr()),
            GENERIC_READ_ACCESS,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            None,
            OPEN_EXISTING,
            FILE_FLAGS_AND_ATTRIBUTES(0), // Standard flags - no NO_BUFFERING
            None,
        )
    }
    .with_context(|| format!("cannot open {} for reading", path.display()))?;

    let file = unsafe { File::from_raw_handle(handle.0) };
    Ok(crate::os::ElevatedSource {
        file: crate::os::SourceHandle::File(file),
        temp_path: None,
    })
}

// ---------------------------------------------------------------------------
// Privileged disk access implementation (Windows)
// ---------------------------------------------------------------------------

use crate::privileged::{AccessStatus, DiskHandle, PrivilegedDiskAccess};

/// Windows implementation of privileged disk access.
///
/// Uses direct file I/O. The app should be launched with UAC elevation
/// (which it already requests via manifest).
pub struct WindowsDiskAccess {
    // TODO: Track open handles
}

impl WindowsDiskAccess {
    pub fn new() -> Result<Self> {
        Ok(Self {})
    }
}

impl PrivilegedDiskAccess for WindowsDiskAccess {
    fn check_status(&self) -> Result<AccessStatus> {
        // The GUI launches `asInvoker`; elevation is on-demand via the top-bar
        // "Show Physical Devices" button. Report NeedsElevation when not admin
        // so callers can surface that path instead of treating it as an error.
        if is_elevated() {
            Ok(AccessStatus::Ready)
        } else {
            Ok(AccessStatus::NeedsElevation)
        }
    }

    fn open_disk_read(&mut self, _path: &Path) -> Result<DiskHandle> {
        anyhow::bail!("Windows privileged disk access not yet implemented")
    }

    fn open_disk_write(&mut self, _path: &Path) -> Result<DiskHandle> {
        anyhow::bail!("Windows privileged disk access not yet implemented")
    }

    fn read_sectors(&mut self, _handle: DiskHandle, _lba: u64, _count: u32) -> Result<Vec<u8>> {
        anyhow::bail!("Windows privileged disk access not yet implemented")
    }

    fn write_sectors(&mut self, _handle: DiskHandle, _lba: u64, _data: &[u8]) -> Result<()> {
        anyhow::bail!("Windows privileged disk access not yet implemented")
    }

    fn close_disk(&mut self, _handle: DiskHandle) -> Result<()> {
        anyhow::bail!("Windows privileged disk access not yet implemented")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_drive_number_from_path() {
        assert_eq!(drive_number_from_path(r"\\.\PhysicalDrive0"), Some(0));
        assert_eq!(drive_number_from_path(r"\\.\PhysicalDrive1"), Some(1));
        assert_eq!(drive_number_from_path(r"\\.\PhysicalDrive15"), Some(15));
        assert_eq!(drive_number_from_path(r"\\.\PhysicalDriveABC"), None);
        assert_eq!(drive_number_from_path(r"C:\"), None);
        assert_eq!(drive_number_from_path(""), None);
    }

    #[test]
    fn test_bus_type_to_string() {
        assert_eq!(bus_type_to_string(7), "USB");
        assert_eq!(bus_type_to_string(17), "NVMe");
        assert_eq!(bus_type_to_string(11), "SATA");
        assert_eq!(bus_type_to_string(3), "ATA");
        assert_eq!(bus_type_to_string(12), "SD");
        assert_eq!(bus_type_to_string(13), "MMC");
        assert_eq!(bus_type_to_string(99), "");
        assert_eq!(bus_type_to_string(0), "");
    }

    #[test]
    fn physical_drives_come_from_the_dos_device_list_in_order() {
        let names = "Volume{1}\0PhysicalDrive3\0C:\0PhysicalDrive0\0PhysicalDrive21\0CdRom0\0\0";
        let wide: Vec<u16> = names.encode_utf16().collect();
        assert_eq!(physical_drive_numbers_from_names(&wide), vec![0, 3, 21]);
        assert!(physical_drive_numbers_from_names(&[0u16]).is_empty());
    }

    #[test]
    fn volume_names_map_to_device_paths_and_letters() {
        let guid = r"\\?\Volume{6f3b1c2e-0000-0000-0000-100000000000}\";
        assert_eq!(
            volume_device_path(guid),
            r"\\.\Volume{6f3b1c2e-0000-0000-0000-100000000000}"
        );
        assert_eq!(drive_letter_of_root(r"d:\"), Some('D'));
        assert_eq!(drive_letter_of_root(r"C:\mnt\data\"), None);
        assert_eq!(drive_letter_of_root(""), None);

        let lettered = VolumeInfo {
            guid_path: guid.to_string(),
            mount_paths: vec![r"C:\mnt\data\".to_string(), r"E:\".to_string()],
            disk_numbers: vec![2],
            filesystem: "NTFS".to_string(),
            total_bytes: 0,
            available_bytes: 0,
        };
        assert_eq!(lettered.display_name(), "E:");
        let bare = VolumeInfo {
            mount_paths: Vec::new(),
            ..lettered
        };
        assert_eq!(
            bare.display_name(),
            "Volume{6f3b1c2e-0000-0000-0000-100000000000}"
        );
    }

    #[test]
    fn disk_extents_yield_every_disk_once() {
        let mut buf = vec![0u8; 8 + 3 * 24];
        buf[0..4].copy_from_slice(&3u32.to_ne_bytes());
        for (i, disk) in [5u32, 1, 5].iter().enumerate() {
            let at = 8 + i * 24;
            buf[at..at + 4].copy_from_slice(&disk.to_ne_bytes());
        }
        assert_eq!(parse_disk_extent_numbers(&buf), vec![1, 5]);
        // A count larger than the buffer holds is clamped, not trusted.
        buf[0..4].copy_from_slice(&50u32.to_ne_bytes());
        assert_eq!(parse_disk_extent_numbers(&buf), vec![1, 5]);
    }

    #[test]
    fn cmdline_args_round_trip_through_windows_quoting() {
        assert_eq!(quote_cmdline_arg("plain.d88"), "plain.d88");
        assert_eq!(
            quote_cmdline_arg(r"C:\Disk Images\game.hdf"),
            r#""C:\Disk Images\game.hdf""#
        );
        assert_eq!(quote_cmdline_arg(""), r#""""#);
        assert_eq!(quote_cmdline_arg(r#"say "hi""#), r#""say \"hi\"""#);
        assert_eq!(quote_cmdline_arg(r"dir\"), r"dir\");
        assert_eq!(quote_cmdline_arg(r"a dir\"), r#""a dir\\""#);
        assert_eq!(quote_cmdline_arg(r#"x\"y"#), r#""x\\\"y""#);
        assert_eq!(join_cmdline_args(Vec::<String>::new()), None);
        assert_eq!(
            join_cmdline_args(vec!["--flag".to_string(), "a b".to_string()]),
            Some(r#"--flag "a b""#.to_string())
        );
    }

    #[test]
    fn test_to_wide() {
        let wide = to_wide("hello");
        assert_eq!(
            wide,
            vec!['h' as u16, 'e' as u16, 'l' as u16, 'l' as u16, 'o' as u16, 0]
        );
        assert_eq!(*wide.last().unwrap(), 0u16);

        let empty = to_wide("");
        assert_eq!(empty, vec![0u16]);
    }

    #[test]
    fn test_string_from_buffer_offset() {
        let buf = b"header\0\0Samsung SSD\0extra";
        assert_eq!(string_from_buffer_offset(buf, 8), "Samsung SSD");
        // Offset 0 means "field not present" in the STORAGE_DEVICE_DESCRIPTOR
        // layout this parses, so it deliberately yields an empty string even
        // though byte 0 happens to start "header".
        assert_eq!(string_from_buffer_offset(buf, 0), "");
        assert_eq!(string_from_buffer_offset(buf, 100), "");
    }

    /// Smoke test that `enumerate_devices()` finds at least one
    /// physical drive. Requires a real disk visible to the test
    /// process — CI runners and Windows VMs without
    /// `\\.\PhysicalDriveX` access surface an empty list, so this is
    /// marked `#[ignore]`. Run manually on a workstation with
    /// `cargo test -- --ignored`.
    #[cfg(target_os = "windows")]
    #[test]
    #[ignore = "needs real \\\\.\\PhysicalDriveX; run with --ignored"]
    fn test_enumerate_devices_nonempty() {
        let devices = enumerate_devices();
        assert!(
            !devices.is_empty(),
            "should find at least one physical drive"
        );
    }
}
