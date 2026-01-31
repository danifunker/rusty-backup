#[cfg(target_os = "macos")]
mod macos;

#[cfg(target_os = "linux")]
mod linux;

#[cfg(target_os = "windows")]
mod windows;

use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};

use crate::device::DiskDevice;

/// Enumerate physical disk devices using platform-specific methods.
pub fn enumerate_devices() -> Vec<DiskDevice> {
    #[cfg(target_os = "macos")]
    {
        macos::enumerate_devices()
    }
    #[cfg(target_os = "linux")]
    {
        linux::enumerate_devices()
    }
    #[cfg(target_os = "windows")]
    {
        windows::enumerate_devices()
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    {
        Vec::new()
    }
}

/// Open a source device or image file for reading.
///
/// On macOS, if a `/dev/disk*` path returns permission denied, this will prompt
/// the user for administrator credentials via the native macOS authentication
/// dialog and create a temporary raw device image using `dd`.
///
/// Returns an `ElevatedSource` containing the opened file. Any temporary files
/// are automatically cleaned up when the `ElevatedSource` is dropped.
pub fn open_source_for_reading(path: &Path) -> Result<ElevatedSource> {
    #[cfg(target_os = "macos")]
    {
        macos::open_source_for_reading(path)
    }
    #[cfg(not(target_os = "macos"))]
    {
        let file = File::open(path)?;
        Ok(ElevatedSource {
            file,
            temp_path: None,
        })
    }
}

/// Open a target device or image file for writing (restore).
///
/// For regular files (`.img`): creates/truncates the file.
/// For devices: uses platform-specific methods to open for raw write access
/// (may unmount partitions, request elevation, etc.).
pub fn open_target_for_writing(path: &Path) -> Result<File> {
    let path_str = path.to_string_lossy();
    let is_device = path_str.starts_with("/dev/") || path_str.starts_with("\\\\.\\");

    if !is_device {
        // Regular file — just create/truncate
        return File::create(path)
            .with_context(|| format!("failed to create {}", path.display()));
    }

    #[cfg(target_os = "macos")]
    {
        macos::open_target_for_writing(path)
    }
    #[cfg(target_os = "linux")]
    {
        linux::open_target_for_writing(path)
    }
    #[cfg(target_os = "windows")]
    {
        windows::open_target_for_writing(path)
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    {
        bail!("device write access not supported on this platform")
    }
}

/// An opened source file that may be backed by a temporary device image.
///
/// Call `into_parts()` to get the file and a cleanup guard that auto-deletes
/// the temp file when dropped.
pub struct ElevatedSource {
    file: File,
    temp_path: Option<PathBuf>,
}

impl ElevatedSource {
    /// Returns the path to the temp file, if one was created.
    pub fn temp_path(&self) -> Option<&Path> {
        self.temp_path.as_deref()
    }

    /// Consume self and return the file plus a cleanup guard.
    /// Keep the guard alive until you're done with the file — dropping it
    /// deletes the temp file (if any).
    pub fn into_parts(self) -> (File, TempFileGuard) {
        (self.file, TempFileGuard(self.temp_path))
    }
}

/// RAII guard that deletes a temporary file when dropped.
pub struct TempFileGuard(Option<PathBuf>);

impl TempFileGuard {
    pub fn path(&self) -> Option<&Path> {
        self.0.as_deref()
    }
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if let Some(ref path) = self.0 {
            let _ = fs::remove_file(path);
        }
    }
}
