#[cfg(target_os = "macos")]
pub mod macos;

#[cfg(target_os = "linux")]
pub mod linux;

#[cfg(target_os = "windows")]
pub mod windows;

use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::device::DiskDevice;

const SECTOR_SIZE: usize = 512;
const WRITE_BUF_CAPACITY: usize = 256 * 1024; // 256 KB, must be a multiple of SECTOR_SIZE

// Windows-specific aligned buffer implementation
#[cfg(target_os = "windows")]
mod aligned_buffer {
    use std::alloc::{self, Layout};
    use std::ptr;

    /// An aligned buffer for Windows FILE_FLAG_NO_BUFFERING operations.
    ///
    /// Windows requires that buffer addresses, file offsets, and sizes are all
    /// sector-aligned when using FILE_FLAG_NO_BUFFERING. This struct ensures
    /// the buffer address is properly aligned in memory.
    pub struct AlignedBuffer {
        ptr: *mut u8,
        layout: Layout,
        len: usize,
    }

    impl AlignedBuffer {
        /// Create a new aligned buffer with the specified capacity.
        /// Both capacity and alignment must be powers of 2.
        pub fn new(capacity: usize, alignment: usize) -> Self {
            assert!(capacity > 0, "capacity must be non-zero");
            assert!(alignment.is_power_of_two(), "alignment must be power of 2");
            assert!(
                capacity % alignment == 0,
                "capacity must be multiple of alignment"
            );

            let layout = Layout::from_size_align(capacity, alignment).expect("invalid layout");

            let ptr = unsafe { alloc::alloc(layout) };
            if ptr.is_null() {
                alloc::handle_alloc_error(layout);
            }

            Self {
                ptr,
                layout,
                len: 0,
            }
        }

        /// Get the capacity of the buffer.
        pub fn capacity(&self) -> usize {
            self.layout.size()
        }

        /// Get the current length of valid data in the buffer.
        pub fn len(&self) -> usize {
            self.len
        }

        /// Check if the buffer is empty.
        pub fn is_empty(&self) -> bool {
            self.len == 0
        }

        /// Get a slice of the valid data.
        pub fn as_slice(&self) -> &[u8] {
            unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
        }

        /// Get a mutable slice of the valid data.
        #[allow(dead_code)]
        pub fn as_mut_slice(&mut self) -> &mut [u8] {
            unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
        }

        /// Get a slice of the entire buffer capacity.
        #[allow(dead_code)]
        pub fn as_full_slice(&self) -> &[u8] {
            unsafe { std::slice::from_raw_parts(self.ptr, self.capacity()) }
        }

        /// Get a mutable slice of the entire buffer capacity.
        #[allow(dead_code)]
        pub fn as_full_mut_slice(&mut self) -> &mut [u8] {
            unsafe { std::slice::from_raw_parts_mut(self.ptr, self.capacity()) }
        }

        /// Append data to the buffer.
        pub fn extend_from_slice(&mut self, data: &[u8]) -> Result<(), ()> {
            if self.len + data.len() > self.capacity() {
                return Err(());
            }
            unsafe {
                ptr::copy_nonoverlapping(data.as_ptr(), self.ptr.add(self.len), data.len());
            }
            self.len += data.len();
            Ok(())
        }

        /// Set the length of valid data, zero-filling if extending.
        pub fn resize(&mut self, new_len: usize, fill: u8) {
            assert!(new_len <= self.capacity(), "new_len exceeds capacity");
            if new_len > self.len {
                unsafe {
                    ptr::write_bytes(self.ptr.add(self.len), fill, new_len - self.len);
                }
            }
            self.len = new_len;
        }

        /// Clear the buffer (set length to 0).
        pub fn clear(&mut self) {
            self.len = 0;
        }

        /// Remove the first `count` bytes from the buffer.
        pub fn drain(&mut self, count: usize) {
            assert!(count <= self.len, "drain count exceeds length");
            if count > 0 {
                unsafe {
                    ptr::copy(self.ptr.add(count), self.ptr, self.len - count);
                }
                self.len -= count;
            }
        }
    }

    impl Drop for AlignedBuffer {
        fn drop(&mut self) {
            unsafe {
                alloc::dealloc(self.ptr, self.layout);
            }
        }
    }

    // Safety: AlignedBuffer owns its memory and doesn't share it
    unsafe impl Send for AlignedBuffer {}
    unsafe impl Sync for AlignedBuffer {}
}

/// Buffered writer that ensures all writes to the underlying file are
/// multiples of the sector size (512 bytes).
///
/// On macOS, raw character devices (`/dev/rdiskN`) reject writes that are not
/// sector-aligned with `EINVAL`. On Windows with FILE_FLAG_NO_BUFFERING, both
/// buffer addresses and sizes must be sector-aligned. This wrapper accumulates
/// writes and only flushes complete sectors to the device.
///
/// `Read` and `Seek` flush the write buffer before delegating to the inner file.
#[cfg(not(target_os = "windows"))]
pub struct SectorAlignedWriter {
    inner: File,
    buf: Vec<u8>,
}

#[cfg(not(target_os = "windows"))]
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
            self.buf
                .resize(self.buf.len() + (SECTOR_SIZE - remainder), 0);
        }
        self.inner.write_all(&self.buf)?;
        self.buf.clear();
        Ok(())
    }

    /// Get mutable access to the inner File for operations requiring random access.
    ///
    /// This flushes the buffer first. Use this for filesystem operations like FAT
    /// resize that need to seek freely without triggering buffer flushes.
    pub fn inner_mut(&mut self) -> io::Result<&mut File> {
        self.flush_padded()?;
        Ok(&mut self.inner)
    }
}

#[cfg(not(target_os = "windows"))]
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

#[cfg(not(target_os = "windows"))]
impl Read for SectorAlignedWriter {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.flush_padded()?;
        self.inner.read(buf)
    }
}

#[cfg(not(target_os = "windows"))]
impl Seek for SectorAlignedWriter {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.flush_padded()?;
        self.inner.seek(pos)
    }
}

// Windows version using aligned buffers
#[cfg(target_os = "windows")]
pub struct SectorAlignedWriter {
    inner: File,
    buf: aligned_buffer::AlignedBuffer,
    /// Current file position (tracked for offset alignment)
    position: u64,
}

#[cfg(target_os = "windows")]
impl SectorAlignedWriter {
    pub fn new(file: File) -> Self {
        // For devices, metadata() typically fails so position defaults to 0.
        let position = file.metadata().map(|m| m.len()).unwrap_or(0);

        Self {
            inner: file,
            buf: aligned_buffer::AlignedBuffer::new(WRITE_BUF_CAPACITY, SECTOR_SIZE),
            position,
        }
    }

    /// Write all complete sectors from the buffer to the device.
    /// On Windows, this ensures the write is sector-aligned in both offset and size.
    fn flush_sectors(&mut self) -> io::Result<()> {
        let aligned_len = (self.buf.len() / SECTOR_SIZE) * SECTOR_SIZE;
        if aligned_len > 0 {
            // Ensure we're at a sector-aligned position
            let current_pos = self.inner.stream_position()?;
            if current_pos % SECTOR_SIZE as u64 != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("file position {} is not sector-aligned", current_pos),
                ));
            }

            self.inner.write_all(&self.buf.as_slice()[..aligned_len])?;
            self.buf.drain(aligned_len);
            self.position += aligned_len as u64;
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
            self.buf
                .resize(self.buf.len() + (SECTOR_SIZE - remainder), 0);
        }

        // Ensure we're at a sector-aligned position
        // Use our tracked position instead of stream_position() which fails with FILE_FLAG_NO_BUFFERING
        if self.position % SECTOR_SIZE as u64 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("file position {} is not sector-aligned", self.position),
            ));
        }

        // Seek to exact position before every write to keep file pointer in sync
        use std::io::Seek;
        self.inner.seek(std::io::SeekFrom::Start(self.position))?;
        self.inner.write_all(self.buf.as_slice())?;

        self.position += self.buf.len() as u64;
        self.buf.clear();
        Ok(())
    }

    /// Get mutable access to the inner File for operations requiring random access.
    ///
    /// This flushes the buffer first. Use this for filesystem operations like FAT
    /// resize that need to seek freely without triggering buffer flushes.
    pub fn inner_mut(&mut self) -> io::Result<&mut File> {
        self.flush_padded()?;
        Ok(&mut self.inner)
    }
}

#[cfg(target_os = "windows")]
impl Write for SectorAlignedWriter {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        // If data won't fit, flush sectors first
        if self.buf.len() + data.len() > WRITE_BUF_CAPACITY {
            self.flush_sectors()?;
        }

        // If data is still too large for the buffer, write it directly
        if data.len() > WRITE_BUF_CAPACITY {
            self.flush_padded()?;

            // Write large data directly, padding to sector boundary if needed
            let aligned_len = (data.len() / SECTOR_SIZE) * SECTOR_SIZE;
            if aligned_len > 0 {
                use std::io::Seek;
                self.inner.seek(std::io::SeekFrom::Start(self.position))?;
                self.inner.write_all(&data[..aligned_len])?;
                self.position += aligned_len as u64;
            }

            // Buffer any remaining partial sector
            let remainder = &data[aligned_len..];
            if !remainder.is_empty() {
                self.buf
                    .extend_from_slice(remainder)
                    .map_err(|_| io::Error::new(io::ErrorKind::OutOfMemory, "buffer full"))?;
            }
        } else {
            // Normal path: buffer the data
            self.buf
                .extend_from_slice(data)
                .map_err(|_| io::Error::new(io::ErrorKind::OutOfMemory, "buffer full"))?;

            if self.buf.len() >= WRITE_BUF_CAPACITY {
                self.flush_sectors()?;
            }
        }

        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flush_padded()?;
        self.inner.flush()
    }
}

#[cfg(target_os = "windows")]
impl Read for SectorAlignedWriter {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.flush_padded()?;
        let n = self.inner.read(buf)?;
        self.position += n as u64;
        Ok(n)
    }
}

#[cfg(target_os = "windows")]
impl Seek for SectorAlignedWriter {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.flush_padded()?;

        // Calculate target position
        let target = match pos {
            SeekFrom::Start(n) => n,
            SeekFrom::End(offset) => {
                let size = self.inner.metadata()?.len();
                (size as i64 + offset) as u64
            }
            SeekFrom::Current(offset) => (self.position as i64 + offset) as u64,
        };

        // Align down to sector boundary
        let aligned_target = (target / SECTOR_SIZE as u64) * SECTOR_SIZE as u64;

        let new_pos = self.inner.seek(SeekFrom::Start(aligned_target))?;
        self.position = new_pos;

        Ok(new_pos)
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
/// On Windows, physical drives are opened with FILE_FLAG_NO_BUFFERING for
/// proper raw disk I/O support.
///
/// Returns an `ElevatedSource` containing the opened file. Any temporary files
/// are automatically cleaned up when the `ElevatedSource` is dropped.
pub fn open_source_for_reading(path: &Path) -> Result<ElevatedSource> {
    #[cfg(target_os = "macos")]
    {
        macos::open_source_for_reading(path)
    }
    #[cfg(target_os = "windows")]
    {
        windows::open_source_for_reading(path)
    }
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
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
/// On macOS, uses DiskArbitration to unmount.
pub fn open_target_for_writing(path: &Path) -> Result<DeviceWriteHandle> {
    let path_str = path.to_string_lossy();
    let is_device = path_str.starts_with("/dev/") || path_str.starts_with("\\\\.\\");

    if !is_device {
        // Regular file — just create/truncate
        let file =
            File::create(path).with_context(|| format!("failed to create {}", path.display()))?;
        return Ok(DeviceWriteHandle::from_file(file));
    }

    #[cfg(target_os = "macos")]
    {
        macos::open_target_for_writing(path).map(DeviceWriteHandle::from_file)
    }
    #[cfg(target_os = "linux")]
    {
        linux::open_target_for_writing(path).map(DeviceWriteHandle::from_file)
    }
    #[cfg(target_os = "windows")]
    {
        let (file, locks) = windows::open_target_for_writing(path)?;
        Ok(DeviceWriteHandle {
            file,
            _volume_locks: locks,
        })
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

/// Handle to a device opened for writing with platform-specific locks.
///
/// On Windows, this holds volume lock handles that keep volumes on the target
/// drive locked and dismounted for the duration of the write. On other
/// platforms, this is a thin wrapper around `File`.
///
/// When this struct is dropped, all locks are released.
pub struct DeviceWriteHandle {
    /// The file handle for writing to the device.
    pub file: File,
    /// On Windows: locked volume handles kept alive to prevent re-mounting.
    #[cfg(target_os = "windows")]
    _volume_locks: windows::VolumeLockSet,
}

impl DeviceWriteHandle {
    /// Create a handle from a plain file (no platform locks).
    pub fn from_file(file: File) -> Self {
        Self {
            file,
            #[cfg(target_os = "windows")]
            _volume_locks: windows::VolumeLockSet::empty(),
        }
    }
}

/// Get the size of a file or device.
///
/// For regular files, uses standard seek to get size.
/// For Windows physical drives, uses IOCTL because seeking doesn't work.
/// For macOS devices, uses DKIOCGETBLOCKCOUNT/DKIOCGETBLOCKSIZE ioctl because
/// `seek(End(0))` returns 0 for device files.
#[allow(unused_variables)]
pub fn get_file_size(file: &File, path: &Path) -> Result<u64> {
    #[cfg(target_os = "windows")]
    {
        let path_str = path.to_string_lossy();
        if path_str.starts_with(r"\\.\PhysicalDrive") {
            return windows::get_physical_drive_size(file);
        }
    }

    #[cfg(target_os = "macos")]
    {
        let path_str = path.to_string_lossy();
        if path_str.starts_with("/dev/") {
            if let Some(size) = macos::get_device_size(file) {
                return Ok(size);
            }
        }
    }

    // For regular files or non-device paths, use seek
    let mut file = file;
    let size = file
        .seek(SeekFrom::End(0))
        .context("failed to seek to end of file")?;
    file.seek(SeekFrom::Start(0))
        .context("failed to seek back to start")?;
    Ok(size)
}

/// Check if the current process is running with elevated (administrator) privileges.
///
/// On Windows, checks for membership in the Administrators group.
/// On macOS and Linux, checks if running as root (UID 0).
/// On other platforms, returns false.
#[allow(unreachable_code)]
pub fn is_elevated() -> bool {
    #[cfg(target_os = "windows")]
    {
        return windows::is_elevated();
    }
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    {
        return unsafe { libc::geteuid() == 0 };
    }
    false
}

/// Request elevation by relaunching the application with administrator privileges.
///
/// On Windows, uses `ShellExecuteW` with the "runas" verb to trigger the UAC dialog.
/// On macOS, could use Authorization Services (not yet implemented).
/// On Linux, could use pkexec or sudo (not yet implemented).
///
/// This function will exit the current process if elevation is successful.
pub fn request_elevation() -> Result<()> {
    #[cfg(target_os = "windows")]
    {
        windows::request_elevation()
    }
    #[cfg(not(target_os = "windows"))]
    {
        anyhow::bail!("elevation request not implemented on this platform")
    }
}
