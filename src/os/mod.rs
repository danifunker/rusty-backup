// `powerpc-apple-darwin` reports `target_os = "macos"`, so the real macOS module
// would be compiled for it - and that module is IOKit + DiskArbitration through
// `objc2-*`, which cannot be transpiled for a 2005 PowerPC Mac. The `os-stub`
// feature swaps in a signature-compatible stand-in instead, which keeps every
// caller and everything portable in this file untouched while dropping `objc2-*`
// from the dependency graph. Off by default: an ordinary macOS build is
// bit-for-bit unaffected. See docs/native_osx_10_dot_3.md.
#[cfg(all(target_os = "macos", not(feature = "os-stub")))]
pub mod macos;
#[cfg(all(target_os = "macos", feature = "os-stub"))]
#[path = "macos_stub.rs"]
pub mod macos;

/// Runtime OS-version detection. Dependency-free, and always compiled: which
/// Mac OS X release we are on is the difference between 10.4 and 10.5, which is
/// load-bearing on PowerPC.
pub mod host_version;

/// Device-list assembly for the Darwin platform modules. Deliberately not
/// `cfg`-gated: it holds no syscalls, so keeping it compiled everywhere means
/// its tests run on the development machine rather than only on a PowerPC Mac.
pub mod darwin_devices;

#[cfg(target_os = "linux")]
pub mod linux;

#[cfg(target_os = "windows")]
pub mod windows;

#[cfg(target_os = "windows")]
pub mod file_assoc;

#[cfg(target_os = "windows")]
pub mod win_install;

pub mod wakelock;

#[cfg(feature = "rust173-polyfill")]
use crate::rust173_compat::IntIsMultipleOf as _;
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::device::DiskDevice;

const SECTOR_SIZE: usize = 512;
const WRITE_BUF_CAPACITY: usize = 256 * 1024; // 256 KB, must be a multiple of SECTOR_SIZE

/// A reader whose `SeekFrom::End` answers from [`get_file_size`], which a device handle cannot.
pub struct KnownLen<R> {
    inner: R,
    len: u64,
    pos: u64,
}

impl<R> KnownLen<R> {
    pub fn new(inner: R, len: u64) -> Self {
        Self { inner, len, pos: 0 }
    }
}

impl<R: Read> Read for KnownLen<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.pos += n as u64;
        Ok(n)
    }
}

impl<R: Seek> Seek for KnownLen<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        // Resolve End against the known length; the device itself cannot answer it.
        let target = match pos {
            SeekFrom::Start(n) => n as i128,
            SeekFrom::End(n) => self.len as i128 + n as i128,
            SeekFrom::Current(n) => self.pos as i128 + n as i128,
        };
        if target < 0 {
            return Err(crate::compat::io_other("seek before start of device"));
        }
        self.pos = self.inner.seek(SeekFrom::Start(target as u64))?;
        Ok(self.pos)
    }
}

/// Wrap a raw source so `SeekFrom::End` works even when the OS will not report a
/// device's length, and every read reaches the device sector-sized (R19).
pub fn known_len_reader(file: File, path: &Path) -> SectorAlignedReader<KnownLen<File>> {
    let len = get_file_size(&file, path).unwrap_or(0);
    SectorAlignedReader::new(KnownLen::new(file, len))
}

/// [`known_len_reader`] for an elevated handle, which on macOS may be a shared
/// authopen descriptor rather than an owned `File`.
pub fn known_len_source(handle: SourceHandle, path: &Path) -> KnownLen<SourceHandle> {
    let len = handle.byte_len(path).unwrap_or(0);
    KnownLen::new(handle, len)
}

/// A read/write handle to an elevated source.
///
/// Plain files (and every non-macOS platform) carry an owned `File`. On macOS a
/// raw device may instead be backed by a descriptor shared with earlier
/// operations, so the user is prompted for administrator rights once per device
/// rather than once per operation — see [`macos::SharedDevice`].
pub enum SourceHandle {
    File(File),
    #[cfg(target_os = "macos")]
    Device(macos::SharedDevice),
}

impl SourceHandle {
    /// An independent handle onto the same source, with its own file offset.
    pub fn try_clone(&self) -> io::Result<SourceHandle> {
        match self {
            SourceHandle::File(f) => f.try_clone().map(SourceHandle::File),
            #[cfg(target_os = "macos")]
            SourceHandle::Device(d) => d.try_clone().map(SourceHandle::Device),
        }
    }

    /// A concrete `File`, for consumers typed on one (backup's CHD staging).
    ///
    /// For a shared device this dups the cached descriptor. That is safe even
    /// while other handles are live: [`macos::SharedDevice`] reads and writes
    /// positionally and never moves the description's file offset, so the
    /// returned `File` is that offset's only user.
    pub fn into_file(self) -> io::Result<File> {
        match self {
            SourceHandle::File(f) => Ok(f),
            #[cfg(target_os = "macos")]
            SourceHandle::Device(d) => d.dup_as_file(),
        }
    }

    /// Total length in bytes, consulting the OS for devices that can't seek.
    pub fn byte_len(&self, path: &Path) -> Option<u64> {
        match self {
            SourceHandle::File(f) => get_file_size(f, path).ok(),
            #[cfg(target_os = "macos")]
            SourceHandle::Device(d) => match d.byte_len() {
                0 => None,
                n => Some(n),
            },
        }
    }
}

impl From<File> for SourceHandle {
    fn from(f: File) -> Self {
        SourceHandle::File(f)
    }
}

impl Read for SourceHandle {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            SourceHandle::File(f) => f.read(buf),
            #[cfg(target_os = "macos")]
            SourceHandle::Device(d) => d.read(buf),
        }
    }
}

impl Write for SourceHandle {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            SourceHandle::File(f) => f.write(buf),
            #[cfg(target_os = "macos")]
            SourceHandle::Device(d) => d.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            SourceHandle::File(f) => f.flush(),
            #[cfg(target_os = "macos")]
            SourceHandle::Device(d) => d.flush(),
        }
    }
}

impl Seek for SourceHandle {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match self {
            SourceHandle::File(f) => f.seek(pos),
            #[cfg(target_os = "macos")]
            SourceHandle::Device(d) => d.seek(pos),
        }
    }
}

/// TEMP-DIAG: describe the access we currently hold on `path`, for the GUI log.
///
/// Non-escalating and read-only in effect, so it is safe to call before any
/// operation. Added to diagnose a macOS restore failing with `Permission
/// denied` at the very end of an otherwise successful run; delete this and its
/// call sites (grep `TEMP-DIAG`) once that is resolved.
#[allow(unused_variables)]
pub fn describe_device_access(path: &Path) -> Vec<String> {
    #[cfg(target_os = "macos")]
    {
        let s = path.to_string_lossy();
        if s.starts_with("/dev/") {
            return macos::probe_device_access(&s);
        }
    }
    Vec::new()
}

/// Release cached privileged device descriptors so the disk can be ejected.
///
/// Pass `None` to release every cached device. A no-op off macOS, which has no
/// descriptor cache.
#[allow(unused_variables)]
pub fn release_elevated_devices(path: Option<&str>) {
    #[cfg(target_os = "macos")]
    macos::release_elevated_devices(path);
}

/// A read adapter that ensures all I/O to the underlying reader is performed
/// at sector-aligned offsets with sector-multiple sizes.
///
/// macOS raw character devices (`/dev/rdiskN`) require both seek positions and
/// read sizes to be multiples of 512 bytes, returning `EINVAL` otherwise.
/// Standard `BufReader` does not enforce this — it passes through arbitrary
/// seeks from filesystem code (e.g. FAT's `next_cluster` seeking to
/// `fat_offset + cluster*4`, or ext's `read_inode` seeking to an inode table
/// entry at an arbitrary byte offset).
///
/// This wrapper maintains a one-sector read-ahead buffer. On seek, it records
/// the logical position. On read, it aligns the underlying seek down to the
/// sector boundary, reads full sectors, and returns only the requested slice.
pub struct SectorAlignedReader<R> {
    inner: R,
    /// Logical byte position (what the caller thinks the position is).
    pos: u64,
    /// Cached sector data.
    buf: [u8; SECTOR_SIZE],
    /// The absolute byte offset of the cached sector (sector-aligned).
    buf_sector_start: u64,
    /// Number of valid bytes in `buf` (may be < SECTOR_SIZE at EOF).
    buf_valid: usize,
    /// Largest read the device has accepted; one sector once a bigger read
    /// failed and its first sector alone read fine (R19).
    max_read: usize,
}

impl<R: Read + Seek> SectorAlignedReader<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            pos: 0,
            buf: [0u8; SECTOR_SIZE],
            buf_sector_start: u64::MAX, // invalid — forces first read to fill
            buf_valid: 0,
            max_read: usize::MAX,
        }
    }

    /// A multi-sector read the device refused, retried one sector at a time.
    ///
    /// A USB floppy drive serves 512-byte reads and fails larger ones with
    /// EIO; a bad sector fails both ways and keeps its original error.
    fn read_after_refusal(&mut self, out: &mut [u8], refusal: io::Error) -> io::Result<usize> {
        self.inner.seek(SeekFrom::Start(self.pos))?;
        match self.inner.read(&mut out[..SECTOR_SIZE]) {
            Ok(n) => {
                if self.max_read != SECTOR_SIZE {
                    log::warn!(
                        "a {}-byte read failed ({refusal}) but one sector reads fine; \
                         continuing one sector at a time",
                        out.len()
                    );
                    self.max_read = SECTOR_SIZE;
                }
                Ok(n)
            }
            Err(_) => Err(refusal),
        }
    }

    /// Ensure `self.buf` contains the sector that covers `self.pos`.
    fn fill_buf(&mut self) -> io::Result<()> {
        let sector_start = (self.pos / SECTOR_SIZE as u64) * SECTOR_SIZE as u64;
        if sector_start == self.buf_sector_start {
            return Ok(()); // already cached
        }
        self.inner.seek(SeekFrom::Start(sector_start))?;
        // Read a full sector; short reads are fine at EOF.
        let mut total = 0;
        while total < SECTOR_SIZE {
            match self.inner.read(&mut self.buf[total..]) {
                Ok(0) => break,
                Ok(n) => total += n,
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
        self.buf_sector_start = sector_start;
        self.buf_valid = total;
        Ok(())
    }
}

impl<R: Read + Seek> Read for SectorAlignedReader<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }

        // Fast path: when both the position and at least one sector's worth of
        // the destination buffer are sector-aligned, read straight from the
        // underlying device into the caller's slice. This avoids the 512-byte
        // bounce buffer that would otherwise turn a single large read_exact
        // (e.g. HFS+ catalog at ~200 MB) into hundreds of thousands of
        // per-sector syscalls on `/dev/rdisk*`.
        if self.pos.is_multiple_of(SECTOR_SIZE as u64) && out.len() >= SECTOR_SIZE {
            let aligned_len = (out.len() - (out.len() % SECTOR_SIZE)).min(self.max_read);
            self.inner.seek(SeekFrom::Start(self.pos))?;
            let n = match self.inner.read(&mut out[..aligned_len]) {
                Ok(n) => n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => return Err(e),
                Err(e) if aligned_len > SECTOR_SIZE => self.read_after_refusal(out, e)?,
                Err(e) => return Err(e),
            };
            // The kernel may return a short read; only the part that landed
            // on a sector boundary is safe to surface to the caller. Round
            // down so the next call still starts sector-aligned.
            let aligned_n = n - (n % SECTOR_SIZE);
            self.pos += aligned_n as u64;
            // Cached sector is now stale relative to the new position;
            // force the slow path to re-fill on the next sub-sector read.
            self.buf_sector_start = u64::MAX;
            self.buf_valid = 0;
            return Ok(aligned_n);
        }

        let mut written = 0;
        while written < out.len() {
            self.fill_buf()?;
            let offset_in_sector = (self.pos % SECTOR_SIZE as u64) as usize;
            if offset_in_sector >= self.buf_valid {
                break; // EOF
            }
            let available = self.buf_valid - offset_in_sector;
            let to_copy = available.min(out.len() - written);
            out[written..written + to_copy]
                .copy_from_slice(&self.buf[offset_in_sector..offset_in_sector + to_copy]);
            written += to_copy;
            self.pos += to_copy as u64;
        }
        Ok(written)
    }
}

impl<R: Read + Seek> Seek for SectorAlignedReader<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(n) => n,
            SeekFrom::End(_) => {
                // Delegate to inner to resolve End-relative seeks, then
                // realign our logical position.
                let resolved = self.inner.seek(pos)?;
                self.pos = resolved;
                return Ok(resolved);
            }
            SeekFrom::Current(offset) => if offset >= 0 {
                self.pos.checked_add(offset as u64)
            } else {
                self.pos.checked_sub((-offset) as u64)
            }
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "seek out of range"))?,
        };
        self.pos = new_pos;
        Ok(new_pos)
    }
}

// SAFETY: SectorAlignedReader<R> only contains R and stack-local buffers.
// If R is Send, the whole wrapper is Send.
unsafe impl<R: Send> Send for SectorAlignedReader<R> {}

// Windows-specific aligned buffer implementation
#[cfg(target_os = "windows")]
mod aligned_buffer {
    // Nested module — the parent module's polyfill import doesn't reach here, so
    // bring the 1.73 `is_multiple_of` shim into this scope too.
    #[cfg(feature = "rust173-polyfill")]
    use crate::rust173_compat::IntIsMultipleOf as _;
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
                capacity.is_multiple_of(alignment),
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

    /// Flush, pad, and push everything to the medium before "complete" is said.
    pub fn sync_all(&mut self) -> io::Result<()> {
        self.flush_padded()?;
        self.inner.sync_all()
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
    pub fn new(mut file: File) -> Self {
        // `set_len()` grows a fresh image but leaves the cursor at 0; a device
        // handle also starts at 0. Use the current cursor position, NOT the
        // file length — metadata().len() returns the pre-sized length, which
        // made the first write land at EOF (Windows-only restore corruption).
        // Fall back to 0 if the handle can't report its position.
        let position = file.stream_position().unwrap_or(0);

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

    /// Flush, pad, and push everything to the medium before "complete" is said.
    pub fn sync_all(&mut self) -> io::Result<()> {
        self.flush_padded()?;
        self.inner.sync_all()
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
        if !self.position.is_multiple_of(SECTOR_SIZE as u64) {
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
        // Nothing buffered and a full-size run: straight to the device, no copy.
        if self.buf.is_empty() && data.len() >= WRITE_BUF_CAPACITY {
            if !self.position.is_multiple_of(SECTOR_SIZE as u64) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("file position {} is not sector-aligned", self.position),
                ));
            }
            let aligned_len = (data.len() / SECTOR_SIZE) * SECTOR_SIZE;
            use std::io::Seek;
            self.inner.seek(std::io::SeekFrom::Start(self.position))?;
            self.inner.write_all(&data[..aligned_len])?;
            self.position += aligned_len as u64;
            let remainder = &data[aligned_len..];
            if !remainder.is_empty() {
                self.buf
                    .extend_from_slice(remainder)
                    .map_err(|_| io::Error::new(io::ErrorKind::OutOfMemory, "buffer full"))?;
            }
            return Ok(data.len());
        }

        // flush_sectors keeps the trailing partial sector, so what fits has to be
        // re-measured every pass rather than inferred from data.len() alone.
        let mut rest = data;
        while !rest.is_empty() {
            let take = (WRITE_BUF_CAPACITY - self.buf.len()).min(rest.len());
            self.buf
                .extend_from_slice(&rest[..take])
                .map_err(|_| io::Error::new(io::ErrorKind::OutOfMemory, "buffer full"))?;
            rest = &rest[take..];
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
        // Names the elevation the caller needs; the CLI reaches this now too.
        let file = File::open(path).map_err(|e| device_open_error(path, e))?;
        Ok(ElevatedSource {
            file: SourceHandle::File(file),
            temp_path: None,
        })
    }
}

/// Turn a permission-denied raw-device open into an actionable message. The GUI
/// starts unprivileged, so this is now the ordinary first-time device failure.
/// macOS never reaches here: it escalates per operation through `authopen`.
#[cfg(not(target_os = "macos"))]
pub(crate) fn device_open_error(path: &Path, e: std::io::Error) -> anyhow::Error {
    if is_device_path(path) && e.kind() == std::io::ErrorKind::PermissionDenied {
        #[cfg(target_os = "linux")]
        return anyhow::anyhow!(
            "cannot open {} - permission denied. Raw disks belong to root: click \
             \"Unlock Physical Devices\" in the GUI top bar to restart elevated, or \
             run rb-cli under sudo.",
            path.display()
        );
        #[cfg(windows)]
        return anyhow::anyhow!(
            "cannot open {} - permission denied. Raw disks need administrator \
             rights: click \"Show Physical Devices\" in the GUI top bar, or run \
             rb-cli from an elevated prompt.",
            path.display()
        );
    }
    anyhow::Error::new(e).context(format!("cannot open {}", path.display()))
}

/// Open a target device or image file for writing (restore).
///
/// For regular files (`.img`): creates/truncates the file.
/// For devices: uses platform-specific methods to open for raw write access.
/// On Linux, unmounts partitions via `umount2(MNT_DETACH)`.
/// On Windows, locks and dismounts volumes via `DeviceIoControl`.
/// On macOS, uses DiskArbitration to unmount.
pub fn open_target_for_writing(path: &Path) -> Result<DeviceWriteHandle> {
    open_target_for_writing_inner(path, true)
}

/// [`open_target_for_writing`] that keeps a regular file's existing contents.
///
/// Writing into one partition must not disturb the rest of the target, and the
/// default path creates + truncates a regular file. Devices behave identically
/// either way — there is nothing to truncate.
pub fn open_target_preserving(path: &Path) -> Result<DeviceWriteHandle> {
    open_target_for_writing_inner(path, false)
}

/// Whether a target path names a raw device rather than a regular file.
/// Matches what [`open_target_for_writing`] keys off internally.
pub fn is_device_path(path: &Path) -> bool {
    let s = path.to_string_lossy();
    s.starts_with("/dev/") || s.starts_with("\\\\.\\")
}

fn open_target_for_writing_inner(path: &Path, truncate: bool) -> Result<DeviceWriteHandle> {
    let is_device = is_device_path(path);

    if !is_device && !truncate {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .with_context(|| format!("failed to open {} for writing", path.display()))?;
        return Ok(DeviceWriteHandle::from_file(file));
    }

    if !is_device {
        // Regular file — create/truncate AND open read+write. Restore's
        // `reconstruct_disk_from_backup` calls `patch_hidden_sectors_for`
        // after the per-partition write, which needs to *read* the boot
        // sector to detect FAT/NTFS/exFAT signatures. A write-only handle
        // (the `File::create` default on Unix) returns EBADF on read.
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .with_context(|| format!("failed to create {}", path.display()))?;
        return Ok(DeviceWriteHandle::from_file(file));
    }

    #[cfg(target_os = "macos")]
    {
        let (file, claim) = macos::open_target_for_writing(path)?;
        Ok(DeviceWriteHandle {
            file,
            _disk_claim: claim,
        })
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
        anyhow::bail!("device write access not supported on this platform")
    }
}

/// An opened source file that may be backed by a temporary device image.
///
/// Call `into_parts()` to get the file and a cleanup guard that auto-deletes
/// the temp file when dropped.
pub struct ElevatedSource {
    file: SourceHandle,
    temp_path: Option<PathBuf>,
    /// On macOS: exclusive disk claim kept alive for the duration of the backup.
    #[cfg(target_os = "macos")]
    disk_claim: Option<macos::DiskClaim>,
}

impl ElevatedSource {
    /// Returns the path to the temp file, if one was created.
    pub fn temp_path(&self) -> Option<&Path> {
        self.temp_path.as_deref()
    }

    /// Consume self and return the handle plus a cleanup guard.
    /// Keep the guard alive until you're done with the handle — dropping it
    /// deletes the temp file (if any) and releases the disk claim.
    pub fn into_parts(self) -> (SourceHandle, TempFileGuard) {
        (
            self.file,
            TempFileGuard {
                temp_path: self.temp_path,
                #[cfg(target_os = "macos")]
                _disk_claim: self.disk_claim,
            },
        )
    }
}

/// RAII guard that deletes a temporary file when dropped and holds the
/// macOS disk claim alive for the duration of the operation.
pub struct TempFileGuard {
    temp_path: Option<PathBuf>,
    /// On macOS: exclusive disk claim released when guard is dropped.
    #[cfg(target_os = "macos")]
    _disk_claim: Option<macos::DiskClaim>,
}

impl TempFileGuard {
    pub fn path(&self) -> Option<&Path> {
        self.temp_path.as_deref()
    }

    /// A guard that deletes `path` when dropped, with no disk claim. Used for
    /// engine-created scratch files (e.g. a remote disk materialized to a local
    /// temp before a CHD / shrink backup) so they're cleaned up on any exit.
    pub fn deleting(path: PathBuf) -> Self {
        Self {
            temp_path: Some(path),
            #[cfg(target_os = "macos")]
            _disk_claim: None,
        }
    }
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if let Some(ref path) = self.temp_path {
            let _ = fs::remove_file(path);
        }
    }
}

/// A reader that keeps its source's [`TempFileGuard`] — the macOS disk claim and
/// any temp file — alive exactly as long as the reader that reads through it.
pub struct GuardedReader<R> {
    inner: R,
    _guard: TempFileGuard,
}

impl<R> GuardedReader<R> {
    pub fn new(inner: R, guard: TempFileGuard) -> Self {
        Self {
            inner,
            _guard: guard,
        }
    }
}

impl<R: Read> Read for GuardedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

impl<R: Seek> Seek for GuardedReader<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.inner.seek(pos)
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
    /// On macOS: exclusive disk claim released when handle is dropped.
    #[cfg(target_os = "macos")]
    _disk_claim: Option<macos::DiskClaim>,
}

impl DeviceWriteHandle {
    /// Create a handle from a plain file (no platform locks or claims).
    pub fn from_file(file: File) -> Self {
        Self {
            file,
            #[cfg(target_os = "windows")]
            _volume_locks: windows::VolumeLockSet::empty(),
            #[cfg(target_os = "macos")]
            _disk_claim: None,
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

/// Free bytes available to the calling user on the filesystem that holds
/// `path`. Returns `None` if the query fails (path doesn't exist, FS is
/// unsupported, etc.) — callers should treat that as "can't tell" rather
/// than "zero free space".
///
/// `path` may be a file or a directory; the caller most often passes the
/// destination directory of a forthcoming write.
pub fn available_space(path: &Path) -> Option<u64> {
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    unsafe {
        use std::ffi::CString;
        let c = CString::new(path.to_string_lossy().as_bytes()).ok()?;
        let mut s: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(c.as_ptr(), &mut s) != 0 {
            return None;
        }
        // f_bavail is in units of f_frsize on Linux/macOS.
        Some((s.f_bavail as u64).saturating_mul(s.f_frsize as u64))
    }
    #[cfg(target_os = "windows")]
    {
        use ::windows::core::PCWSTR;
        use ::windows::Win32::Storage::FileSystem::GetDiskFreeSpaceExW;
        // GetDiskFreeSpaceExW accepts any path (file or directory) on the
        // target volume.
        let wide: Vec<u16> = path
            .to_string_lossy()
            .encode_utf16()
            .chain(std::iter::once(0))
            .collect();
        let mut free_to_caller: u64 = 0;
        unsafe {
            GetDiskFreeSpaceExW(PCWSTR(wide.as_ptr()), Some(&mut free_to_caller), None, None)
                .ok()?;
        }
        Some(free_to_caller)
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    {
        let _ = path;
        None
    }
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

/// Rename `src` over `dest`, retrying ~2.5s on the ERROR_ACCESS_DENIED /
/// ERROR_SHARING_VIOLATION a Windows scanner's handle causes. No-op on Unix.
pub fn replace_file(src: &Path, dest: &Path) -> io::Result<()> {
    const ATTEMPTS: u32 = 12;
    let mut delay_ms = 25u64;
    for attempt in 1..=ATTEMPTS {
        match std::fs::rename(src, dest) {
            Ok(()) => return Ok(()),
            Err(e) if attempt < ATTEMPTS && is_transient_share_error(&e) => {
                log::debug!(
                    "rename {} -> {} failed ({e}); retry {attempt}/{ATTEMPTS} in {delay_ms}ms",
                    src.display(),
                    dest.display(),
                );
                std::thread::sleep(std::time::Duration::from_millis(delay_ms));
                delay_ms = (delay_ms * 2).min(400);
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!("the final attempt returns rather than looping")
}

/// True only on Windows, and only for the two codes a scanner's handle yields.
fn is_transient_share_error(e: &io::Error) -> bool {
    cfg!(windows) && matches!(e.raw_os_error(), Some(5) | Some(32))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    // Read/Seek/SeekFrom/Write come in via the parent module's
    // `use std::io::{self, Read, Seek, SeekFrom, Write}` at the top of the file.

    /// A device that fails any read larger than `limit` with EIO, the way a
    /// USB floppy drive does, and fails `bad_sector` at any size (R19).
    struct SmallTransfers {
        data: Vec<u8>,
        pos: u64,
        limit: usize,
        bad_sector: Option<u64>,
    }

    impl SmallTransfers {
        fn new(len: usize, limit: usize, bad_sector: Option<u64>) -> Self {
            let data = (0..len).map(|i| (i / 7) as u8).collect();
            Self {
                data,
                pos: 0,
                limit,
                bad_sector,
            }
        }
    }

    impl Read for SmallTransfers {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if buf.len() > self.limit {
                return Err(crate::compat::io_other("Input/output error"));
            }
            let start = self.pos as usize;
            let end = (start + buf.len()).min(self.data.len());
            if let Some(bad) = self.bad_sector {
                let first = start as u64 / SECTOR_SIZE as u64;
                let last = (end.max(start + 1) - 1) as u64 / SECTOR_SIZE as u64;
                if (first..=last).contains(&bad) {
                    return Err(crate::compat::io_other("Input/output error"));
                }
            }
            buf[..end - start].copy_from_slice(&self.data[start..end]);
            self.pos = end as u64;
            Ok(end - start)
        }
    }

    impl Seek for SmallTransfers {
        fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
            self.pos = match pos {
                SeekFrom::Start(n) => n,
                SeekFrom::End(n) => (self.data.len() as i64 + n) as u64,
                SeekFrom::Current(n) => (self.pos as i64 + n) as u64,
            };
            Ok(self.pos)
        }
    }

    #[test]
    fn a_read_the_device_refuses_falls_back_to_one_sector_at_a_time() {
        // R19: the floppy served dd's 512-byte reads and failed ours.
        let device = SmallTransfers::new(8192, SECTOR_SIZE, None);
        let mut reader = SectorAlignedReader::new(device);
        let mut out = vec![0u8; 4096];
        reader.read_exact(&mut out).expect("falls back to sectors");
        assert_eq!(out, reader.inner.data[..4096]);
        assert_eq!(reader.max_read, SECTOR_SIZE, "remembers the device's limit");
        // And stays there: the next large read never trips the device again.
        reader.read_exact(&mut out).expect("second read");
        assert_eq!(out, reader.inner.data[4096..8192]);
    }

    #[test]
    fn a_healthy_device_keeps_its_large_reads() {
        let device = SmallTransfers::new(8192, usize::MAX, None);
        let mut reader = SectorAlignedReader::new(device);
        let mut out = vec![0u8; 4096];
        reader.read_exact(&mut out).unwrap();
        assert_eq!(reader.max_read, usize::MAX);
    }

    #[test]
    fn a_bad_sector_keeps_its_error_instead_of_a_downshift() {
        // The first sector of the request is the bad one: the retry fails too,
        // the original error comes back and the limit is left alone.
        let device = SmallTransfers::new(8192, usize::MAX, Some(0));
        let mut reader = SectorAlignedReader::new(device);
        let mut out = vec![0u8; 4096];
        assert!(reader.read_exact(&mut out).is_err());
        assert_eq!(reader.max_read, usize::MAX);

        // A bad sector further in: the downshift happens, then the per-sector
        // reads surface the error at exactly that sector.
        let device = SmallTransfers::new(8192, usize::MAX, Some(3));
        let mut reader = SectorAlignedReader::new(device);
        let mut out = vec![0u8; 4096];
        assert!(reader.read_exact(&mut out).is_err());
        assert_eq!(reader.stream_position().unwrap(), 3 * SECTOR_SIZE as u64);
    }

    /// Regression: `SectorAlignedWriter::new` must start writing at the current
    /// cursor (offset 0 for a fresh handle), NOT at `metadata().len()`. The
    /// re-resize restore path pre-sizes the target with `set_len()` and then
    /// expects the first write — the patched MBR — to land at sector 0. The
    /// prior Windows constructor seeded `position` from the file length, so
    /// the MBR was written at EOF and sector 0 stayed zero-filled, producing
    /// a restored image with a zeroed partition table.
    #[test]
    fn sector_aligned_writer_starts_at_cursor_not_eof() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("preallocated.img");

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .expect("create");
        // Pre-size to 1 MiB the same way the re-resize restore path does.
        file.set_len(1 << 20).expect("set_len");

        let mut writer = SectorAlignedWriter::new(file);

        // Write one full sector of 0xAB starting at offset 0.
        let payload = vec![0xABu8; SECTOR_SIZE];
        writer.write_all(&payload).expect("write");
        writer.flush().expect("flush");
        drop(writer);

        let mut check = OpenOptions::new().read(true).open(&path).expect("reopen");
        check.seek(SeekFrom::Start(0)).expect("seek");
        let mut first = vec![0u8; SECTOR_SIZE];
        check.read_exact(&mut first).expect("read sector 0");
        assert!(
            first.iter().all(|&b| b == 0xAB),
            "first sector should contain the payload, not zeros (was {:?}..)",
            &first[..16]
        );
    }

    /// Regression: a partial-sector write then a full-capacity one overflowed the
    /// Windows writer's fixed buffer -- restore died with "buffer full".
    #[test]
    fn sector_aligned_writer_survives_partial_then_full_chunk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("restore.img");

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .expect("create");
        let mut writer = SectorAlignedWriter::new(file);

        // zstd hands back arbitrary sizes, so a chunk that is not a whole number
        // of sectors leaves a remainder that flush_sectors deliberately keeps.
        let partial = vec![0xAAu8; 100];
        let full = vec![0xBBu8; WRITE_BUF_CAPACITY];
        writer.write_all(&partial).expect("partial chunk");
        writer
            .write_all(&full)
            .expect("full chunk after a partial one");
        writer.flush().expect("flush");
        drop(writer);

        let mut got = Vec::new();
        OpenOptions::new()
            .read(true)
            .open(&path)
            .expect("reopen")
            .read_to_end(&mut got)
            .expect("read back");

        // The tail is zero-padded up to a sector; everything before it is verbatim.
        assert!(got.len() >= partial.len() + full.len(), "short write");
        assert_eq!(
            &got[..partial.len()],
            &partial[..],
            "partial chunk corrupted"
        );
        assert_eq!(
            &got[partial.len()..partial.len() + full.len()],
            &full[..],
            "full chunk corrupted or misplaced"
        );
    }

    #[test]
    fn replace_file_overwrites_an_existing_destination() {
        let tmp = tempfile::TempDir::new().unwrap();
        let src = tmp.path().join("new");
        let dest = tmp.path().join("old");
        std::fs::write(&src, b"new").unwrap();
        std::fs::write(&dest, b"old").unwrap();

        replace_file(&src, &dest).unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), b"new");
        assert!(!src.exists(), "source should have been renamed away");
    }

    #[test]
    fn replace_file_still_reports_a_missing_source() {
        // A non-transient error must surface immediately, not after 12 retries.
        let tmp = tempfile::TempDir::new().unwrap();
        let err = replace_file(&tmp.path().join("nope"), &tmp.path().join("dest")).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    /// A Windows `\\.\PhysicalDriveN` handle: reads fine, but `SeekFrom::End` is ERROR_INVALID_FUNCTION.
    struct NoEndSeek(io::Cursor<Vec<u8>>);

    impl Read for NoEndSeek {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.0.read(buf)
        }
    }

    impl Seek for NoEndSeek {
        fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
            match pos {
                SeekFrom::End(_) => Err(io::Error::from_raw_os_error(1)),
                other => self.0.seek(other),
            }
        }
    }

    #[test]
    fn known_len_answers_end_seeks_a_device_cannot() {
        let data: Vec<u8> = (0..=255u8).cycle().take(4096).collect();
        let mut raw = NoEndSeek(io::Cursor::new(data.clone()));
        assert!(
            raw.seek(SeekFrom::End(0)).is_err(),
            "bare device must reject End"
        );

        let mut r = KnownLen::new(NoEndSeek(io::Cursor::new(data.clone())), 4096);
        assert_eq!(r.seek(SeekFrom::End(0)).unwrap(), 4096);
        assert_eq!(r.seek(SeekFrom::End(-512)).unwrap(), 3584);

        // Reads still land where the caller asked, and Current stays relative.
        let mut buf = [0u8; 4];
        r.seek(SeekFrom::Start(10)).unwrap();
        r.read_exact(&mut buf).unwrap();
        assert_eq!(buf, data[10..14]);
        assert_eq!(r.seek(SeekFrom::Current(-4)).unwrap(), 10);
        assert!(
            r.seek(SeekFrom::End(-8192)).is_err(),
            "negative target must error"
        );
    }
}
