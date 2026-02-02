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
    CreateFileW, GetDiskFreeSpaceExW, GetLogicalDriveStringsW, GetVolumeInformationW,
    FILE_FLAGS_AND_ATTRIBUTES, FILE_FLAG_NO_BUFFERING, FILE_SHARE_READ, FILE_SHARE_WRITE,
    OPEN_EXISTING,
};
use windows::Win32::System::IO::DeviceIoControl;
use windows::Win32::UI::Shell::{ShellExecuteW, SHELLEXECUTEINFOW, SEE_MASK_NOCLOSEPROCESS};
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

        if CreateWellKnownSid(
            WinBuiltinAdministratorsSid,
            None,
            Some(admin_sid),
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

/// Request elevation by relaunching the application with UAC prompt.
///
/// This uses `ShellExecuteW` with the "runas" verb to trigger the UAC dialog.
/// The current process will exit after launching the elevated instance.
pub fn request_elevation() -> Result<()> {
    let exe_path = env::current_exe().context("failed to get executable path")?;
    let exe_path_wide = to_wide(&exe_path.to_string_lossy());
    let verb = to_wide("runas");

    let mut exec_info = SHELLEXECUTEINFOW {
        cbSize: std::mem::size_of::<SHELLEXECUTEINFOW>() as u32,
        fMask: SEE_MASK_NOCLOSEPROCESS,
        hwnd: HWND::default(),
        lpVerb: PCWSTR(verb.as_ptr()),
        lpFile: PCWSTR(exe_path_wide.as_ptr()),
        lpParameters: PCWSTR::null(),
        lpDirectory: PCWSTR::null(),
        nShow: SW_SHOW.0,
        hInstApp: Default::default(),
        ..Default::default()
    };

    unsafe {
        if ShellExecuteExW(&mut exec_info).is_ok() {
            // Successfully launched elevated instance; exit this one
            std::process::exit(0);
        } else {
            bail!("failed to request elevation - user may have cancelled UAC prompt");
        }
    }
}

/// ShellExecuteExW is actually ShellExecuteW in the windows crate.
/// This wrapper provides the correct signature.
unsafe fn ShellExecuteExW(info: *mut SHELLEXECUTEINFOW) -> windows::core::Result<()> {
    let result = ShellExecuteW(
        Some((*info).hwnd),
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
    unsafe { DeviceIoControl(handle, IOCTL_DISK_IS_WRITABLE, None, 0, None, 0, None, None).is_ok() }
}

/// Information about a mounted volume (drive letter).
struct VolumeInfo {
    drive_letter: char,
    disk_number: u32,
    filesystem: String,
    total_bytes: u64,
    available_bytes: u64,
}

/// Enumerate all drive-letter volumes and map each to its physical drive number.
fn enumerate_volumes() -> Vec<VolumeInfo> {
    let mut volumes = Vec::new();

    let mut buf = vec![0u16; 512];
    let len = unsafe { GetLogicalDriveStringsW(Some(&mut buf)) };
    if len == 0 {
        return volumes;
    }

    // Buffer contains null-separated root paths like "C:\", "D:\", ...
    let drive_roots: Vec<String> = buf[..len as usize]
        .split(|&c| c == 0)
        .filter(|s| !s.is_empty())
        .map(|s| String::from_utf16_lossy(s))
        .collect();

    for root in &drive_roots {
        let letter = match root.chars().next() {
            Some(c) if c.is_ascii_alphabetic() => c,
            _ => continue,
        };

        // Open \\.\X: to query which physical drive this volume lives on
        let volume_path = format!(r"\\.\{}:", letter);
        let disk_number = match open_device(&volume_path, GENERIC_READ_ACCESS) {
            Some(vol_handle) => {
                let mut ext_buf = [0u8; 256];
                let mut returned = 0u32;
                let result = unsafe {
                    DeviceIoControl(
                        vol_handle.0,
                        IOCTL_VOLUME_GET_VOLUME_DISK_EXTENTS,
                        None,
                        0,
                        Some(ext_buf.as_mut_ptr() as *mut c_void),
                        ext_buf.len() as u32,
                        Some(&mut returned),
                        None,
                    )
                };
                if result.is_err() || returned < 12 {
                    continue;
                }
                // VOLUME_DISK_EXTENTS layout:
                //   offset 0: NumberOfDiskExtents (u32)
                //   offset 8: first DISK_EXTENT.DiskNumber (u32, after alignment padding)
                let num = u32::from_ne_bytes(ext_buf[0..4].try_into().unwrap_or([0; 4]));
                if num == 0 {
                    continue;
                }
                u32::from_ne_bytes(ext_buf[8..12].try_into().unwrap_or([0; 4]))
            }
            None => continue,
        };

        // Query filesystem name
        let root_wide = to_wide(root);
        let mut fs_name_buf = vec![0u16; 64];
        let fs_name = unsafe {
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
                let end = fs_name_buf
                    .iter()
                    .position(|&c| c == 0)
                    .unwrap_or(fs_name_buf.len());
                String::from_utf16_lossy(&fs_name_buf[..end])
            } else {
                String::new()
            }
        };

        // Query disk space
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

        volumes.push(VolumeInfo {
            drive_letter: letter,
            disk_number,
            filesystem: fs_name,
            total_bytes,
            available_bytes: free_to_caller,
        });
    }

    volumes
}

/// Enumerate physical disk devices on Windows.
///
/// Probes `\\.\PhysicalDrive0` through `\\.\PhysicalDrive15` using
/// `CreateFileW` and `DeviceIoControl`, then maps mounted volumes
/// (drive letters) to their parent physical drives.
///
/// In debug builds, if not elevated, automatically requests elevation via UAC.
pub fn enumerate_devices() -> Vec<DiskDevice> {
    // In debug builds, check elevation and request if needed
    #[cfg(debug_assertions)]
    {
        if !is_elevated() {
            log::warn!("Not running with administrator privileges; requesting elevation...");
            if let Err(e) = request_elevation() {
                log::error!("Failed to request elevation: {}", e);
                // Return empty list if elevation failed
                return Vec::new();
            }
            // If we get here, elevation was cancelled or failed
            return Vec::new();
        }
    }

    let volumes = enumerate_volumes();

    // Group volumes by physical drive number
    let mut vol_map: HashMap<u32, Vec<&VolumeInfo>> = HashMap::new();
    for vol in &volumes {
        vol_map.entry(vol.disk_number).or_default().push(vol);
    }

    let c_drive_disk = volumes
        .iter()
        .find(|v| v.drive_letter == 'C')
        .map(|v| v.disk_number);

    let mut devices = Vec::new();

    for i in 0..16u32 {
        let drive_path = format!(r"\\.\PhysicalDrive{i}");
        let handle = match open_device(&drive_path, GENERIC_READ_ACCESS) {
            Some(h) => h,
            None => continue,
        };

        let size_bytes = query_disk_size(handle.0).unwrap_or(0);
        let (is_removable, bus_protocol, media_name) = query_device_properties(handle.0);
        let is_read_only = !is_disk_writable(handle.0);
        let is_system = c_drive_disk == Some(i);

        let partitions = vol_map
            .get(&i)
            .map(|vols| {
                vols.iter()
                    .map(|v| MountedPartition {
                        name: format!("{}:", v.drive_letter),
                        mount_point: PathBuf::from(format!("{}:\\", v.drive_letter)),
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
pub fn open_target_for_writing(path: &Path) -> Result<File> {
    let path_str = path.to_string_lossy();
    let drive_num = drive_number_from_path(&path_str).context("invalid physical drive path")?;

    // In debug builds, check elevation before attempting to open
    #[cfg(debug_assertions)]
    {
        if !is_elevated() {
            log::warn!(
                "Attempting to open {} for writing without admin privileges; requesting elevation...",
                path.display()
            );
            request_elevation()?;
            // If we get here, elevation was cancelled
            bail!("Administrator privileges required to write to disk devices");
        }
    }

    // Lock and dismount all volumes on this drive
    let volumes = enumerate_volumes();
    for vol in &volumes {
        if vol.disk_number != drive_num {
            continue;
        }
        let vol_path = format!(r"\\.\{}:", vol.drive_letter);
        if let Some(vol_handle) = open_device(&vol_path, GENERIC_READ_ACCESS | GENERIC_WRITE_ACCESS)
        {
            unsafe {
                let _ = DeviceIoControl(
                    vol_handle.0,
                    FSCTL_LOCK_VOLUME,
                    None,
                    0,
                    None,
                    0,
                    None,
                    None,
                );
                let _ = DeviceIoControl(
                    vol_handle.0,
                    FSCTL_DISMOUNT_VOLUME,
                    None,
                    0,
                    None,
                    0,
                    None,
                    None,
                );
            }
        }
    }

    // Open the physical drive for read+write
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
    Ok(unsafe { File::from_raw_handle(handle.0 as *mut c_void) })
}

/// Open a source device for reading (backup operation).
///
/// For physical drives, uses FILE_FLAG_NO_BUFFERING which is required for
/// reliable raw disk I/O on Windows. Without this flag, reads from physical
/// drives can fail with "Incorrect function" (error 1).
///
/// For regular files (image files), opens normally without buffering flags.
pub fn open_source_for_reading(path: &Path) -> Result<crate::os::ElevatedSource> {
    let path_str = path.to_string_lossy();
    let is_physical_drive = path_str.starts_with(r"\\.\PhysicalDrive");

    if !is_physical_drive {
        // Regular file - just open normally
        let file = File::open(path)
            .with_context(|| format!("cannot open {}", path.display()))?;
        return Ok(crate::os::ElevatedSource {
            file,
            temp_path: None,
        });
    }

    // Physical drive - use FILE_FLAG_NO_BUFFERING for raw disk access
    #[cfg(debug_assertions)]
    {
        if !is_elevated() {
            log::warn!(
                "Attempting to open {} without admin privileges; requesting elevation...",
                path.display()
            );
            request_elevation()?;
            bail!("Administrator privileges required to read disk devices");
        }
    }

    let wide = to_wide(&path_str);
    let handle = unsafe {
        CreateFileW(
            PCWSTR(wide.as_ptr()),
            GENERIC_READ_ACCESS,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            None,
            OPEN_EXISTING,
            FILE_FLAG_NO_BUFFERING,
            None,
        )
    }
    .with_context(|| format!("cannot open {} for reading", path.display()))?;

    let file = unsafe { File::from_raw_handle(handle.0 as *mut c_void) };
    Ok(crate::os::ElevatedSource {
        file,
        temp_path: None,
    })
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
        assert_eq!(string_from_buffer_offset(buf, 0), "header");
        assert_eq!(string_from_buffer_offset(buf, 100), "");
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn test_enumerate_devices_nonempty() {
        let devices = enumerate_devices();
        assert!(
            !devices.is_empty(),
            "should find at least one physical drive"
        );
    }
}
