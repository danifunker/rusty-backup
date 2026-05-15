//! Sliding-window buffered reader with a switchable fill size.
//!
//! `BulkBufReader` wraps a `Read + Seek` source and serves reads out of an
//! in-memory window. The window's capacity has two modes:
//!
//! - **Normal**: small (e.g. 64 KiB). Used during scattered / random-access
//!   phases (B-tree node reads, catalog walks) where a large fill would
//!   amplify reads many-fold for no payoff.
//! - **Bulk**: large (e.g. 512 MiB). Used during long sequential phases
//!   (HFS+ defrag-clone fork-data emit) where the source disk is being
//!   read mostly in order and a single multi-hundred-MiB fill collapses
//!   thousands of small `read` syscalls into one.
//!
//! The mode is controlled via a cloneable [`BulkModeHandle`] (an
//! `Arc<AtomicBool>` under the hood) so the caller can flip it from outside
//! the reader after the random-access phase finishes.
//!
//! Seeks are cheap: they only update the logical position. The next read
//! that lands outside the current window triggers a refill at the new
//! position; reads that fall inside the window are served from RAM.

use std::io::{self, Read, Seek, SeekFrom};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Control handle for switching a [`BulkBufReader`] between normal and
/// bulk fill modes. Cheap to clone; safe to share across threads.
#[derive(Clone, Default)]
pub struct BulkModeHandle(Arc<AtomicBool>);

impl BulkModeHandle {
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    pub fn enable(&self) {
        self.0.store(true, Ordering::Relaxed);
    }

    pub fn disable(&self) {
        self.0.store(false, Ordering::Relaxed);
    }

    pub fn is_enabled(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
}

pub struct BulkBufReader<R> {
    inner: R,
    buffer: Vec<u8>,
    /// Source byte offset corresponding to `buffer[0]`.
    window_start: u64,
    /// Valid bytes in `buffer` (`buffer[0..window_filled]`).
    window_filled: usize,
    /// Current logical read position (where the caller thinks the cursor is).
    logical_pos: u64,
    /// Where `inner` is actually seeked. `None` means "unknown — must seek
    /// before the next read".
    inner_pos: Option<u64>,
    capacity_normal: usize,
    capacity_bulk: usize,
    bulk_mode: BulkModeHandle,
}

impl<R: Read + Seek> BulkBufReader<R> {
    pub fn new(mut inner: R, capacity_normal: usize, capacity_bulk: usize) -> io::Result<Self> {
        // Capture the inner reader's starting position so the first read
        // doesn't blindly seek to 0 and break callers that pre-positioned
        // the source.
        let start = inner.stream_position()?;
        Ok(Self {
            inner,
            buffer: Vec::new(),
            window_start: start,
            window_filled: 0,
            logical_pos: start,
            inner_pos: Some(start),
            capacity_normal,
            capacity_bulk,
            bulk_mode: BulkModeHandle::new(),
        })
    }

    pub fn handle(&self) -> BulkModeHandle {
        self.bulk_mode.clone()
    }

    pub fn with_handle(mut self, handle: BulkModeHandle) -> Self {
        self.bulk_mode = handle;
        self
    }

    fn current_capacity(&self) -> usize {
        if self.bulk_mode.is_enabled() {
            self.capacity_bulk
        } else {
            self.capacity_normal
        }
    }

    /// True iff `logical_pos` is currently served by the buffer.
    fn window_covers_pos(&self) -> bool {
        self.window_filled > 0
            && self.logical_pos >= self.window_start
            && self.logical_pos < self.window_start + self.window_filled as u64
    }

    /// Drop the current buffer if its capacity doesn't match the active
    /// mode's target. Frees the 512 MiB allocation when bulk mode flips
    /// off again, and re-grows when it flips back on.
    fn resize_buffer_if_mode_changed(&mut self) {
        let target = self.current_capacity();
        if self.buffer.capacity() == target {
            // Reuse — just reset length on next fill.
            return;
        }
        // Drop the old allocation and acquire a fresh one at the new
        // capacity. Don't pre-fill — `read` paths only touch `[..filled]`.
        self.buffer = Vec::with_capacity(target);
    }

    /// Fill the buffer starting at `logical_pos`. After return the buffer
    /// holds up to `current_capacity()` bytes (or fewer at EOF) and
    /// `window_start == logical_pos`.
    fn refill(&mut self) -> io::Result<()> {
        self.resize_buffer_if_mode_changed();
        let target_pos = self.logical_pos;
        if self.inner_pos != Some(target_pos) {
            self.inner.seek(SeekFrom::Start(target_pos))?;
            self.inner_pos = Some(target_pos);
        }
        let cap = self.buffer.capacity();
        // SAFETY: we'll only ever read `filled` bytes; the rest is
        // uninitialised but never observed.
        unsafe {
            self.buffer.set_len(cap);
        }
        let mut filled = 0usize;
        while filled < cap {
            let n = match self.inner.read(&mut self.buffer[filled..cap]) {
                Ok(0) => break, // EOF
                Ok(n) => n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    // Leave the buffer in a known-empty state so a retry
                    // doesn't observe stale bytes.
                    self.buffer.clear();
                    self.window_filled = 0;
                    self.inner_pos = None;
                    return Err(e);
                }
            };
            filled += n;
        }
        // Trim the logical length down to what we actually filled.
        self.buffer.truncate(filled);
        self.window_start = target_pos;
        self.window_filled = filled;
        self.inner_pos = Some(target_pos + filled as u64);
        Ok(())
    }
}

impl<R: Read + Seek> Read for BulkBufReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        if !self.window_covers_pos() {
            self.refill()?;
            if self.window_filled == 0 {
                return Ok(0); // EOF
            }
        }
        let offset_in_window = (self.logical_pos - self.window_start) as usize;
        let available = self.window_filled - offset_in_window;
        let n = available.min(buf.len());
        buf[..n].copy_from_slice(&self.buffer[offset_in_window..offset_in_window + n]);
        self.logical_pos += n as u64;
        Ok(n)
    }
}

impl<R: Read + Seek> Seek for BulkBufReader<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(p) => p,
            SeekFrom::Current(delta) => {
                if delta >= 0 {
                    self.logical_pos.checked_add(delta as u64).ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidInput, "seek overflow")
                    })?
                } else {
                    self.logical_pos
                        .checked_sub((-delta) as u64)
                        .ok_or_else(|| {
                            io::Error::new(io::ErrorKind::InvalidInput, "seek underflow")
                        })?
                }
            }
            SeekFrom::End(_) => {
                // Pass-through: rare, and we'd have to invalidate the
                // window anyway. Forward to inner and adopt its answer.
                let p = self.inner.seek(pos)?;
                self.inner_pos = Some(p);
                self.logical_pos = p;
                return Ok(p);
            }
        };
        self.logical_pos = new_pos;
        Ok(new_pos)
    }

    fn stream_position(&mut self) -> io::Result<u64> {
        Ok(self.logical_pos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn make_source(len: usize) -> Cursor<Vec<u8>> {
        let data: Vec<u8> = (0..len).map(|i| (i & 0xff) as u8).collect();
        Cursor::new(data)
    }

    #[test]
    fn read_within_window() {
        let mut r = BulkBufReader::new(make_source(8192), 1024, 4096).unwrap();
        let mut buf = [0u8; 16];
        r.read_exact(&mut buf).unwrap();
        for i in 0..16 {
            assert_eq!(buf[i], (i & 0xff) as u8);
        }
    }

    #[test]
    fn seek_within_window_no_refill() {
        let mut r = BulkBufReader::new(make_source(8192), 4096, 4096).unwrap();
        let mut buf = [0u8; 16];
        r.read_exact(&mut buf).unwrap();
        r.seek(SeekFrom::Start(100)).unwrap();
        let mut b2 = [0u8; 8];
        r.read_exact(&mut b2).unwrap();
        for i in 0..8 {
            assert_eq!(b2[i], ((100 + i) & 0xff) as u8);
        }
    }

    #[test]
    fn seek_outside_window_refills() {
        let mut r = BulkBufReader::new(make_source(1 << 16), 256, 256).unwrap();
        let mut buf = [0u8; 16];
        r.read_exact(&mut buf).unwrap();
        r.seek(SeekFrom::Start(40_000)).unwrap();
        let mut b2 = [0u8; 16];
        r.read_exact(&mut b2).unwrap();
        for i in 0..16 {
            assert_eq!(b2[i], ((40_000 + i) & 0xff) as u8);
        }
    }

    #[test]
    fn bulk_mode_grows_buffer() {
        let mut r = BulkBufReader::new(make_source(1 << 20), 1024, 64 * 1024).unwrap();
        let h = r.handle();
        let mut buf = [0u8; 16];
        r.read_exact(&mut buf).unwrap();
        assert_eq!(r.buffer.capacity(), 1024);
        h.enable();
        r.seek(SeekFrom::Start(2048)).unwrap();
        r.read_exact(&mut buf).unwrap();
        assert_eq!(r.buffer.capacity(), 64 * 1024);
        h.disable();
        r.seek(SeekFrom::Start(100_000)).unwrap();
        r.read_exact(&mut buf).unwrap();
        assert_eq!(r.buffer.capacity(), 1024);
    }

    #[test]
    fn eof_returns_zero() {
        let mut r = BulkBufReader::new(make_source(32), 64, 64).unwrap();
        let mut buf = [0u8; 32];
        r.read_exact(&mut buf).unwrap();
        let mut tail = [0u8; 4];
        assert_eq!(r.read(&mut tail).unwrap(), 0);
    }
}
