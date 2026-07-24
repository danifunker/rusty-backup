//! A read/write window onto part of a larger file.
//!
//! Some containers don't *encode* the image they hold — they simply put it at
//! an offset and leave it there. An AppImage is an ELF stub with a SquashFS
//! appended; a live-CD's `filesystem.squashfs` is a run of bytes inside an
//! ISO 9660 at a known extent. Neither needs decoding, so decompressing them to
//! a temp file (as the CHD / gzip / floppy containers do) would be wasted work
//! on something that is already a flat image.
//!
//! Presenting that region as a handle whose byte 0 *is* the image lets every
//! layer above stay unaware of the wrapper: partition detection, filesystem
//! dispatch and editing all behave exactly as they do for a bare file.
//!
//! Two shapes, and the difference matters for writing:
//!
//! - **Tail** — the region runs to the end of the file and may grow it. An
//!   AppImage's payload is the last thing in the file, so a rebuilt SquashFS
//!   that came out larger simply makes the AppImage larger.
//! - **Bounded** — the region is exactly `len` bytes with something after it.
//!   A write past the end is refused rather than corrupting whatever follows.

use std::io::{Read, Result as IoResult, Seek, SeekFrom, Write};

/// A window onto `inner` starting at `base`. See the module docs.
pub struct PayloadSlice<T> {
    inner: T,
    /// Byte offset in `inner` that the slice presents as position 0.
    base: u64,
    /// Length of the window, or `None` when it runs to the end of the file and
    /// may extend it.
    len: Option<u64>,
    /// Read/write cursor, relative to `base`.
    pos: u64,
}

impl<T> PayloadSlice<T> {
    /// A window that runs to the end of `inner` and may grow it.
    pub fn tail(inner: T, base: u64) -> Self {
        Self {
            inner,
            base,
            len: None,
            pos: 0,
        }
    }

    /// A window of exactly `len` bytes. Writes past the end are refused.
    pub fn bounded(inner: T, base: u64, len: u64) -> Self {
        Self {
            inner,
            base,
            len: Some(len),
            pos: 0,
        }
    }

    /// Where this window starts in the underlying file.
    pub fn base(&self) -> u64 {
        self.base
    }

    /// Consume the slice and return the handle it wrapped.
    pub fn into_inner(self) -> T {
        self.inner
    }
}

impl<T: Seek> PayloadSlice<T> {
    /// The window's length: its declared size, or whatever remains of the file
    /// after `base` for a tail.
    fn window_len(&mut self) -> IoResult<u64> {
        match self.len {
            Some(n) => Ok(n),
            None => {
                let end = self.inner.seek(SeekFrom::End(0))?;
                Ok(end.saturating_sub(self.base))
            }
        }
    }
}

impl<T: Read + Seek> Read for PayloadSlice<T> {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        let remaining = self.window_len()?.saturating_sub(self.pos);
        if remaining == 0 {
            return Ok(0);
        }
        let take = (buf.len() as u64).min(remaining) as usize;
        self.inner.seek(SeekFrom::Start(self.base + self.pos))?;
        let n = self.inner.read(&mut buf[..take])?;
        self.pos += n as u64;
        Ok(n)
    }
}

impl<T: Read + Write + Seek> Write for PayloadSlice<T> {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        // A bounded window has something after it; running past the end would
        // corrupt whatever that is, so refuse rather than truncate silently.
        if let Some(len) = self.len {
            let remaining = len.saturating_sub(self.pos);
            if (buf.len() as u64) > remaining {
                return Err(crate::compat::io_other(format!(
                    "write of {} bytes at offset {} would run {} bytes past the \
                     end of a {}-byte region embedded in a larger file",
                    buf.len(),
                    self.pos,
                    buf.len() as u64 - remaining,
                    len
                )));
            }
        }
        self.inner.seek(SeekFrom::Start(self.base + self.pos))?;
        let n = self.inner.write(buf)?;
        self.pos += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> IoResult<()> {
        self.inner.flush()
    }
}

impl<T: Seek> Seek for PayloadSlice<T> {
    fn seek(&mut self, from: SeekFrom) -> IoResult<u64> {
        let target = match from {
            SeekFrom::Start(n) => n as i64,
            SeekFrom::Current(d) => self.pos as i64 + d,
            SeekFrom::End(d) => self.window_len()? as i64 + d,
        };
        if target < 0 {
            return Err(crate::compat::io_other(
                "seek to a negative position inside a payload slice",
            ));
        }
        self.pos = target as u64;
        Ok(self.pos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn backing() -> Cursor<Vec<u8>> {
        // 16 bytes of prefix, then 16 bytes of payload.
        let mut v = vec![0xAAu8; 16];
        v.extend_from_slice(&[0xBB; 16]);
        Cursor::new(v)
    }

    #[test]
    fn reads_and_writes_are_relative_to_the_base() {
        let mut s = PayloadSlice::tail(backing(), 16);
        let mut buf = [0u8; 4];
        s.read_exact(&mut buf).unwrap();
        assert_eq!(buf, [0xBB; 4], "position 0 must be the base");

        s.seek(SeekFrom::Start(0)).unwrap();
        s.write_all(&[1, 2, 3, 4]).unwrap();
        let inner = s.into_inner().into_inner();
        assert_eq!(&inner[0..16], &[0xAA; 16], "the prefix must be untouched");
        assert_eq!(&inner[16..20], &[1, 2, 3, 4]);
    }

    /// The tail may grow the file — this is what makes an AppImage's appended
    /// SquashFS editable at all.
    #[test]
    fn a_tail_slice_grows_the_file() {
        let mut s = PayloadSlice::tail(backing(), 16);
        s.seek(SeekFrom::End(0)).unwrap();
        s.write_all(&[0xCC; 32]).unwrap();
        let inner = s.into_inner().into_inner();
        assert_eq!(inner.len(), 16 + 16 + 32, "the file did not grow");
        assert_eq!(&inner[0..16], &[0xAA; 16], "the prefix must be untouched");
    }

    /// A bounded slice has something after it, so overrunning must be an error
    /// rather than a silent write into a neighbour.
    #[test]
    fn a_bounded_slice_refuses_to_overrun() {
        let mut s = PayloadSlice::bounded(backing(), 16, 16);
        s.seek(SeekFrom::Start(12)).unwrap();
        let err = s.write_all(&[0xCC; 8]).expect_err("must refuse");
        assert!(err.to_string().contains("past the end"), "got: {err}");

        // Reading is clamped to the window, not the file.
        let mut s = PayloadSlice::bounded(backing(), 8, 8);
        let mut out = Vec::new();
        s.read_to_end(&mut out).unwrap();
        assert_eq!(out.len(), 8, "read past the window end");
    }
}
