//! `RemoteBlockReader` — seekable raw access to a remote image over the wire.
//!
//! Implements `Read + Seek` (and, when opened read-write, `Write`) by issuing
//! ranged `ReadBlock` / `WriteBlock` requests against a daemon, caching one
//! read-ahead window. The desktop engine then parses a remote disk image's
//! **partition table and filesystems** — and can inspect, back up, *and edit*
//! it in place — exactly as for a local file, **without downloading the whole
//! disk**. This is the "block tier" of `docs/remote_transfer_plan.md`: the
//! daemon is a dumb byte server; all parsing stays on the desktop.
//!
//! Needs a v2 daemon (block-tier verbs). One shared `RemoteConnection` can back
//! many readers (and the operation-level browse) at once — every call
//! serializes through the connection's lock.

use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};

use crate::remote::connection::RemoteConnection;

/// Size of one read-ahead fetch. Scattered small reads (partition tables, boot
/// sectors, BPBs) are served from a window; sequential reads pull a window at a
/// time. Kept under the server's `MAX_RANGE_READ` cap.
const WINDOW: u64 = 256 * 1024;

/// Largest payload per `WriteBlock` frame. A larger `write` is split across
/// frames (`Write` may return a short count; the engine's `write_all` loops).
/// Kept under the server's `MAX_RANGE_WRITE` cap.
const WRITE_CHUNK: usize = 1024 * 1024;

/// Lock the shared connection, mapping a poisoned mutex to an `anyhow` error.
/// Free function so the constructors can take the lock before `Self` exists.
fn lock(
    conn: &Arc<Mutex<RemoteConnection>>,
) -> anyhow::Result<std::sync::MutexGuard<'_, RemoteConnection>> {
    conn.lock()
        .map_err(|_| anyhow::anyhow!("remote connection lock poisoned"))
}

/// A `Read + Seek` view over a disk image on a remote daemon.
///
/// Holds a block **handle** that keeps the image open on the daemon for the
/// reader's lifetime (closed on drop), so the user works against one open image
/// rather than reopening it per read.
pub struct RemoteBlockReader {
    conn: Arc<Mutex<RemoteConnection>>,
    path: String,
    handle: u64,
    len: u64,
    pos: u64,
    /// Whether the daemon-side handle was opened read-write. `Write` calls
    /// fail fast (read-only) when this is false.
    writable: bool,
    /// Cached window covering `[cache_start, cache_start + cache.len())`.
    cache: Vec<u8>,
    cache_start: u64,
}

impl RemoteBlockReader {
    /// Open a host image file `path` on the daemon for ranged reading — the
    /// daemon keeps the file open and returns a handle + the image length.
    /// Blocking (one round-trip).
    pub fn open(conn: Arc<Mutex<RemoteConnection>>, path: &str) -> anyhow::Result<Self> {
        let (handle, len) = {
            let mut c = lock(&conn)?;
            c.open_block(path)?
        };
        Ok(Self::from_handle(conn, path, handle, len, false))
    }

    /// Open a host image file `path` on the daemon for ranged **read-write**
    /// access — feeds `open_editable_filesystem` so the engine edits a remote
    /// image in place over the wire. The daemon opens the file read+write and
    /// keeps it open; writes never grow it.
    pub fn open_rw(conn: Arc<Mutex<RemoteConnection>>, path: &str) -> anyhow::Result<Self> {
        let (handle, len) = {
            let mut c = lock(&conn)?;
            c.open_block_rw(path)?
        };
        Ok(Self::from_handle(conn, path, handle, len, true))
    }

    /// Open one of the daemon's enumerated **physical devices** for ranged
    /// reading (read-only). The device-backed sibling of [`Self::open`] — used
    /// to back up a remote drive over the wire.
    pub fn open_device(conn: Arc<Mutex<RemoteConnection>>, path: &str) -> anyhow::Result<Self> {
        let (handle, len) = {
            let mut c = lock(&conn)?;
            c.open_device(path)?
        };
        Ok(Self::from_handle(conn, path, handle, len, false))
    }

    fn from_handle(
        conn: Arc<Mutex<RemoteConnection>>,
        path: &str,
        handle: u64,
        len: u64,
        writable: bool,
    ) -> Self {
        Self {
            conn,
            path: path.to_string(),
            handle,
            len,
            pos: 0,
            writable,
            cache: Vec::new(),
            cache_start: 0,
        }
    }

    /// Whether this reader can write back to the remote image.
    pub fn is_writable(&self) -> bool {
        self.writable
    }

    /// Overwrite any cached bytes that `[at, at+data.len())` covers, so a read
    /// of just-written data stays consistent without re-fetching from the
    /// daemon. A no-op when the write misses the current window.
    fn patch_cache(&mut self, at: u64, data: &[u8]) {
        if self.cache.is_empty() {
            return;
        }
        let cstart = self.cache_start;
        let cend = cstart + self.cache.len() as u64;
        let wend = at + data.len() as u64;
        let ostart = at.max(cstart);
        let oend = wend.min(cend);
        if ostart < oend {
            let dst = (ostart - cstart) as usize;
            let src = (ostart - at) as usize;
            let cnt = (oend - ostart) as usize;
            self.cache[dst..dst + cnt].copy_from_slice(&data[src..src + cnt]);
        }
    }

    /// Total length of the remote image in bytes.
    pub fn len(&self) -> u64 {
        self.len
    }

    /// Whether the remote image is zero-length.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The remote path this reader streams from (relative to the serve root).
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Ensure the cache window covers `pos`, fetching a `WINDOW`-aligned window
    /// otherwise.
    fn ensure_cached(&mut self, pos: u64) -> io::Result<()> {
        let end = self.cache_start + self.cache.len() as u64;
        if !self.cache.is_empty() && pos >= self.cache_start && pos < end {
            return Ok(());
        }
        // Align the window down to a WINDOW boundary for locality.
        let start = (pos / WINDOW) * WINDOW;
        let want = WINDOW.min(self.len.saturating_sub(start));
        let buf = {
            let mut c = self
                .conn
                .lock()
                .map_err(|_| io::Error::other("remote connection lock poisoned"))?;
            c.read_block(self.handle, start, want as u32)
                .map_err(io::Error::other)?
        };
        self.cache_start = start;
        self.cache = buf;
        Ok(())
    }
}

impl Drop for RemoteBlockReader {
    fn drop(&mut self) {
        // Best-effort: close the daemon-side block handle so the image doesn't
        // stay open after the reader goes away. `try_lock` never blocks — if an
        // op holds the lock we skip and the daemon reaps it on disconnect.
        if let Ok(mut c) = self.conn.try_lock() {
            let _ = c.close_block(self.handle);
        }
    }
}

impl Read for RemoteBlockReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() || self.pos >= self.len {
            return Ok(0);
        }
        self.ensure_cached(self.pos)?;
        let off = (self.pos - self.cache_start) as usize;
        if off >= self.cache.len() {
            // The window came back short of `pos` (EOF / sparse tail).
            return Ok(0);
        }
        let n = (self.cache.len() - off).min(buf.len());
        buf[..n].copy_from_slice(&self.cache[off..off + n]);
        self.pos += n as u64;
        Ok(n)
    }
}

impl Write for RemoteBlockReader {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if !self.writable {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "remote block handle is read-only",
            ));
        }
        if buf.is_empty() {
            return Ok(0);
        }
        // One frame at a time; write_all loops for the rest. The daemon
        // bounds-checks `offset + len <= size`, so a write past the image end
        // surfaces as an error rather than corrupting / growing the file.
        let n = buf.len().min(WRITE_CHUNK);
        let pos = self.pos;
        {
            let mut c = self
                .conn
                .lock()
                .map_err(|_| io::Error::other("remote connection lock poisoned"))?;
            c.write_block(self.handle, pos, &buf[..n])
                .map_err(io::Error::other)?;
        }
        // Keep the read cache consistent with what we just wrote.
        self.patch_cache(pos, &buf[..n]);
        self.pos += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        // Read-only handles have nothing to flush; the engine still calls
        // flush() on the shared reader in some read paths.
        if !self.writable {
            return Ok(());
        }
        let mut c = self
            .conn
            .lock()
            .map_err(|_| io::Error::other("remote connection lock poisoned"))?;
        c.flush_block(self.handle).map_err(io::Error::other)
    }
}

impl Seek for RemoteBlockReader {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        let new = match from {
            SeekFrom::Start(o) => o as i64,
            SeekFrom::Current(d) => self.pos as i64 + d,
            SeekFrom::End(d) => self.len as i64 + d,
        };
        if new < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek before start of image",
            ));
        }
        self.pos = new as u64;
        Ok(self.pos)
    }
}
