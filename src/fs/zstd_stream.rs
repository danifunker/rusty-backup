//! Streaming zstd reader with a growable in-memory buffer.
//!
//! `ZstdStreamCache` decompresses a zstd file forward-only into a growable
//! in-memory buffer.  Multiple `ZstdStreamReader` handles share a single cache
//! via `Arc<Mutex<...>>` and present a `Read + Seek` interface suitable for
//! passing to `fs::open_filesystem`.
//!
//! The buffer grows on demand as the filesystem reads metadata structures.
//! No arbitrary cap is applied — reads are always served if the decompressor
//! can produce the data.  When the seekable cache is ready, `zstd_cache` is
//! dropped and the buffer is freed.

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::{Arc, Mutex};

/// Shared state for the streaming zstd reader.
///
/// `data` is append-only: offsets `0..data.len()` are always valid after a
/// successful `fill_to` call.  The decoder is always positioned at `data.len()`.
pub struct ZstdStreamCache {
    /// Accumulated decompressed bytes.
    data: Vec<u8>,
    /// Streaming decoder.  Dropped once `exhausted` is `true`.
    /// `zstd::Decoder::new(file)` wraps `File` in a `BufReader` internally,
    /// yielding `Decoder<'static, BufReader<File>>`.
    decoder: Option<zstd::Decoder<'static, std::io::BufReader<File>>>,
    /// `true` when the decoder has reached EOF of the compressed stream.
    exhausted: bool,
}

// SAFETY: `zstd::Decoder` holds raw C pointers internally, but all access goes
// through the `Mutex` in `Arc<Mutex<ZstdStreamCache>>`, which serialises
// access.  Sending the cache to another thread is therefore safe.
unsafe impl Send for ZstdStreamCache {}

impl ZstdStreamCache {
    /// Open `path` and prepare for streaming decompression.
    pub fn new(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        // `Decoder::new` wraps `file` in a `BufReader` itself → Decoder<'static, BufReader<File>>
        let decoder = zstd::Decoder::new(file)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("zstd decoder: {e}")))?;
        Ok(Self {
            data: Vec::new(),
            decoder: Some(decoder),
            exhausted: false,
        })
    }

    /// Ensure `data.len() >= end`, decompressing forward as needed.
    ///
    /// Always fills to `end` on demand regardless of partition size.
    /// Sets `exhausted = true` only when the decompressor reaches EOF.
    /// Returns an error only if the decoder itself fails.
    fn fill_to(&mut self, end: u64) -> io::Result<()> {
        if self.exhausted {
            return Ok(());
        }

        // Clamp to usize max to avoid overflow on 32-bit platforms
        let target = end.min(usize::MAX as u64) as usize;
        if self.data.len() >= target {
            return Ok(());
        }

        let decoder = match self.decoder.as_mut() {
            Some(d) => d,
            None => {
                self.exhausted = true;
                return Ok(());
            }
        };

        let old_len = self.data.len();
        let need = target - old_len;
        self.data.resize(old_len + need, 0u8);

        let mut filled = 0;
        while filled < need {
            match decoder.read(&mut self.data[old_len + filled..old_len + need]) {
                Ok(0) => {
                    // EOF — shrink to what was actually written
                    self.data.truncate(old_len + filled);
                    self.decoder = None;
                    self.exhausted = true;
                    return Ok(());
                }
                Ok(n) => {
                    filled += n;
                }
                Err(e) => {
                    self.data.truncate(old_len + filled);
                    return Err(e);
                }
            }
        }

        Ok(())
    }
}

/// A `Read + Seek` handle backed by a shared `ZstdStreamCache`.
///
/// Multiple readers may share one cache safely; each tracks its own position.
pub struct ZstdStreamReader {
    cache: Arc<Mutex<ZstdStreamCache>>,
    position: u64,
}

impl ZstdStreamReader {
    pub fn new(cache: Arc<Mutex<ZstdStreamCache>>) -> Self {
        Self { cache, position: 0 }
    }
}

impl Read for ZstdStreamReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let end = self.position + buf.len() as u64;
        let mut cache = self
            .cache
            .lock()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("cache lock: {e}")))?;

        cache.fill_to(end)?;

        let buffered = cache.data.len() as u64;
        if self.position >= buffered {
            // Either genuine EOF (exhausted) or fill_to failed to reach end —
            // signal EOF either way; the filesystem will handle it gracefully.
            return Ok(0);
        }

        let avail = &cache.data[self.position as usize..];
        let n = buf.len().min(avail.len());
        buf[..n].copy_from_slice(&avail[..n]);
        self.position += n as u64;
        Ok(n)
    }
}

impl Seek for ZstdStreamReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_pos: i64 = match pos {
            SeekFrom::Start(offset) => offset as i64,
            SeekFrom::End(offset) => {
                // We don't know the total decompressed size upfront; use the
                // current buffered length as the "end" — sufficient for the
                // filesystem open path which seeks near the beginning.
                let cache = self.cache.lock().map_err(|e| {
                    io::Error::new(io::ErrorKind::Other, format!("cache lock: {e}"))
                })?;
                cache.data.len() as i64 + offset
            }
            SeekFrom::Current(offset) => self.position as i64 + offset,
        };
        if new_pos < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek to negative position",
            ));
        }
        self.position = new_pos as u64;
        Ok(self.position)
    }
}
