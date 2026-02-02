//! Privileged disk access abstraction layer.
//!
//! Provides a unified interface for accessing disk devices across platforms:
//! - macOS: Uses SMAppService daemon with XPC communication
//! - Linux: Uses pkexec for privilege elevation
//! - Windows: Uses existing UAC elevation

use std::path::Path;

use anyhow::Result;

#[cfg(target_os = "macos")]
pub mod protocol;

/// Status of privileged disk access capability.
#[derive(Debug, Clone, PartialEq)]
pub enum AccessStatus {
    /// Ready to access disks (daemon running or already elevated)
    Ready,
    /// Linux: Needs pkexec relaunch
    NeedsElevation,
    /// macOS: Daemon not installed in app bundle
    DaemonNotInstalled,
    /// macOS: Daemon installed but user hasn't approved in System Settings
    DaemonNeedsApproval,
    /// macOS: Daemon version is outdated
    DaemonOutdated { current: String },
}

/// Opaque handle representing an open disk device.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DiskHandle(pub u64);

/// Trait for platform-specific privileged disk access.
///
/// All disk operations are performed at the sector level (512 bytes).
/// Implementations handle platform-specific privilege elevation and IPC.
pub trait PrivilegedDiskAccess: Send {
    /// Check if privileged access is available and ready.
    fn check_status(&self) -> Result<AccessStatus>;

    /// Open a disk device for reading.
    ///
    /// Returns a handle that can be used for subsequent read operations.
    fn open_disk_read(&mut self, path: &Path) -> Result<DiskHandle>;

    /// Open a disk device for writing.
    ///
    /// Unmounts any mounted volumes on the disk first, then returns a handle
    /// for subsequent write operations.
    fn open_disk_write(&mut self, path: &Path) -> Result<DiskHandle>;

    /// Read sectors from an open disk.
    ///
    /// # Arguments
    /// * `handle` - Handle from `open_disk_read` or `open_disk_write`
    /// * `lba` - Starting logical block address (sector number)
    /// * `count` - Number of sectors to read
    ///
    /// Returns a buffer of `count * 512` bytes.
    fn read_sectors(&mut self, handle: DiskHandle, lba: u64, count: u32) -> Result<Vec<u8>>;

    /// Write sectors to an open disk.
    ///
    /// # Arguments
    /// * `handle` - Handle from `open_disk_write`
    /// * `lba` - Starting logical block address (sector number)
    /// * `data` - Data to write (must be a multiple of 512 bytes)
    fn write_sectors(&mut self, handle: DiskHandle, lba: u64, data: &[u8]) -> Result<()>;

    /// Close an open disk handle.
    fn close_disk(&mut self, handle: DiskHandle) -> Result<()>;
}

/// Create a platform-appropriate disk access implementation.
pub fn create_disk_access() -> Result<Box<dyn PrivilegedDiskAccess>> {
    #[cfg(target_os = "macos")]
    {
        Ok(Box::new(crate::os::macos::MacOSDiskAccess::new()?))
    }
    #[cfg(target_os = "linux")]
    {
        Ok(Box::new(crate::os::linux::LinuxDiskAccess::new()?))
    }
    #[cfg(target_os = "windows")]
    {
        // Windows already uses UAC elevation at startup
        Ok(Box::new(crate::os::windows::WindowsDiskAccess::new()?))
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    {
        anyhow::bail!("Unsupported platform for privileged disk access")
    }
}
