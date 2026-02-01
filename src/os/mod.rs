#[cfg(target_os = "macos")]
mod macos;

#[cfg(target_os = "linux")]
mod linux;

#[cfg(target_os = "windows")]
mod windows;

use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::device::DiskDevice;

const SECTOR_SIZE: usize = 512;
const WRITE_BUF_CAPACITY: usize = 256 * 1024; // 256 KB, must be a multiple of SECTOR_SIZE

/// Buffered writer that ensures all writes to the underlying file are
/// multiples of the sector size (512 bytes).
///
/// On macOS, raw character devices (`/dev/rdiskN`) reject writes that are not
/// sector-aligned with `EINVAL`. The decompression and zero-fill paths can
/// produce writes of arbitrary size, so this wrapper accumulates them and only
/// flushes complete sectors to the device.
///
/// `Read` and `Seek` flush the write buffer before delegating to the inner file.
pub struct SectorAlignedWriter {
    inner: File,
    buf: Vec<u8>,
}

impl SectorAlignedWriter {
    pub fn new(file: File) -> Self {
        Self {
            inner: file,
            buf: Vec::with_capacity(WRITE_BUF_CAPACITY),
        }
    }

    /// Write all complete sectors from the buffer to the device.
    fn flush_sectors(&mut self) -> io::Result<()> {
        let aligned_len = (self.buf.len() / SECTOR_SIZE) * SECTOR_SIZE;
        if aligned_len > 0 {
            self.inner.write_all(&self.buf[..aligned_len])?;
            self.buf.drain(..aligned_len);
        }
        Ok(())
    }

    /// Flush everything, padding the final partial sector with zeros.
    fn flush_padded(&mut self) -> io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let remainder = self.buf.len() % SECTOR_SIZE;
        if remainder != 0 {
            self.buf.resize(self.buf.len() + (SECTOR_SIZE - remainder), 0);
        }
        self.inner.write_all(&self.buf)?;
        self.buf.clear();
        Ok(())
    }
}

impl Write for SectorAlignedWriter {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.buf.extend_from_slice(data);
        if self.buf.len() >= WRITE_BUF_CAPACITY {
            self.flush_sectors()?;
        }
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flush_padded()?;
        self.inner.flush()
    }
}

impl Read for SectorAlignedWriter {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.flush_padded()?;
        self.inner.read(buf)
    }
}

impl Seek for SectorAlignedWriter {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.flush_padded()?;
        self.inner.seek(pos)
    }
}

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
/// For devices: uses platform-specific methods to open for raw write access.
/// On Linux, unmounts partitions via `umount2(MNT_DETACH)`.
/// On Windows, locks and dismounts volumes via `DeviceIoControl`.
/// On macOS, uses `diskutil` to unmount.
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
