//! `RemoteBlockReader` — seekable raw access to a remote image over the wire.
//!
//! Implements `Read + Seek` by issuing ranged `ReadHostRange` reads against a
//! daemon, caching one read-ahead window. The desktop engine then parses a
//! remote disk image's **partition table and filesystems** — and can run backup
//! / export / resize — exactly as for a local file, **without downloading the
//! whole disk**. This is the "block tier" of `docs/remote_transfer_plan.md`:
//! the daemon is a dumb byte server; all parsing stays on the desktop.
//!
//! Needs a v2 daemon (`HostFileSize` / `ReadHostRange`). One shared
//! `RemoteConnection` can back many readers (and the operation-level browse) at
//! once — every call serializes through the connection's lock.

use std::io::{self, Read, Seek, SeekFrom};
use std::sync::{Arc, Mutex};

use crate::remote::connection::RemoteConnection;

/// Size of one read-ahead fetch. Scattered small reads (partition tables, boot
/// sectors, BPBs) are served from a window; sequential reads pull a window at a
/// time. Kept under the server's `MAX_RANGE_READ` cap.
const WINDOW: u64 = 256 * 1024;

/// A `Read + Seek` view over a disk image on a remote daemon.
pub struct RemoteBlockReader {
    conn: Arc<Mutex<RemoteConnection>>,
    path: String,
    len: u64,
    pos: u64,
    /// Cached window covering `[cache_start, cache_start + cache.len())`.
    cache: Vec<u8>,
    cache_start: u64,
}

impl RemoteBlockReader {
    /// Open `path` on the daemon for ranged reading: fetch its length, ready to
    /// seek/read. Blocking (one round-trip for the size).
    pub fn open(conn: Arc<Mutex<RemoteConnection>>, path: &str) -> anyhow::Result<Self> {
        let len = {
            let mut c = conn
                .lock()
                .map_err(|_| anyhow::anyhow!("remote connection lock poisoned"))?;
            c.host_file_size(path)?
        };
        Ok(Self {
            conn,
            path: path.to_string(),
            len,
            pos: 0,
            cache: Vec::new(),
            cache_start: 0,
        })
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
            c.read_host_range(&self.path, start, want as u32)
                .map_err(io::Error::other)?
        };
        self.cache_start = start;
        self.cache = buf;
        Ok(())
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
