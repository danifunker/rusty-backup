//! zstd backend shim — one code path, two backends.
//!
//! Production code goes through these helpers instead of naming a zstd crate
//! type directly, so the backend is chosen by a cargo feature:
//!
//! - `native-zstd` (desktop default) → the C libzstd via the `zstd` crate.
//! - `pure-zstd` (slim / i586-i486 Linux / armv7-MiSTer cross) → the pure-Rust,
//!   bit-exact `libzstd-bitexact-rs`. No C zstd to cross-compile.
//!
//! Both backends produce byte-identical output (bit-exact parity with libzstd
//! 1.5.7), so a backup made by either is interchangeable.
//!
//! The decode side is exposed as `decoder()` returning a `Box<dyn Read + Send>`
//! (callers never name the concrete decoder type); the encode side is a
//! `ZstdEncoder<W>` that implements `Write` + `finish()` over either backend.
//! The seekable-zstd format (zeekstd) is C-only and handled at its one call
//! site, not here.

use std::io::{self, Read, Write};

#[cfg(not(any(feature = "native-zstd", feature = "pure-zstd")))]
compile_error!(
    "exactly one zstd backend must be enabled: `native-zstd` (C libzstd) or \
     `pure-zstd` (libzstd-bitexact-rs). The default feature set enables \
     `native-zstd`; slim builds use `--no-default-features --features pure-zstd`."
);

// ---------------------------------------------------------------------------
// native-zstd: the C `zstd` crate.
// ---------------------------------------------------------------------------

/// Wrap a reader in a streaming zstd decoder. The C `Decoder::new` buffers the
/// source internally, so any `Read` works.
#[cfg(feature = "native-zstd")]
pub(crate) fn decoder<R: Read + Send + 'static>(r: R) -> io::Result<Box<dyn Read + Send>> {
    Ok(Box::new(::zstd::Decoder::new(r)?))
}

/// Decompress a full in-memory zstd frame sequence.
#[cfg(feature = "native-zstd")]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn decode_all(data: &[u8]) -> io::Result<Vec<u8>> {
    ::zstd::decode_all(data)
}

/// Compress a full buffer in one shot at `level`.
#[cfg(feature = "native-zstd")]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn encode_all<R: Read>(source: R, level: i32) -> io::Result<Vec<u8>> {
    ::zstd::encode_all(source, level)
}

/// Streaming zstd encoder that writes compressed bytes to `W`.
#[cfg(feature = "native-zstd")]
pub(crate) struct ZstdEncoder<W: Write>(::zstd::Encoder<'static, W>);

#[cfg(feature = "native-zstd")]
impl<W: Write> ZstdEncoder<W> {
    pub(crate) fn new(sink: W, level: i32) -> io::Result<Self> {
        Ok(Self(::zstd::Encoder::new(sink, level)?))
    }

    /// Flush the final frame and return the underlying sink.
    pub(crate) fn finish(self) -> io::Result<W> {
        self.0.finish()
    }
}

#[cfg(feature = "native-zstd")]
impl<W: Write> Write for ZstdEncoder<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

// ---------------------------------------------------------------------------
// pure-zstd: libzstd-bitexact-rs (used only when native-zstd is OFF).
// ---------------------------------------------------------------------------

#[cfg(all(not(feature = "native-zstd"), feature = "pure-zstd"))]
fn to_io<E: std::fmt::Display>(e: E) -> io::Error {
    io::Error::other(e.to_string())
}

#[cfg(all(not(feature = "native-zstd"), feature = "pure-zstd"))]
pub(crate) fn decoder<R: Read + Send + 'static>(r: R) -> io::Result<Box<dyn Read + Send>> {
    Ok(Box::new(libzstd_bitexact_rs::StreamDecoder::new(r)))
}

#[cfg(all(not(feature = "native-zstd"), feature = "pure-zstd"))]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn decode_all(data: &[u8]) -> io::Result<Vec<u8>> {
    libzstd_bitexact_rs::decompress(data).map_err(to_io)
}

#[cfg(all(not(feature = "native-zstd"), feature = "pure-zstd"))]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn encode_all<R: Read>(mut source: R, level: i32) -> io::Result<Vec<u8>> {
    let mut input = Vec::new();
    source.read_to_end(&mut input)?;
    libzstd_bitexact_rs::compress(&input, level).map_err(to_io)
}

/// `Write` adapter over `libzstd-bitexact-rs`'s push/pull `StreamEncoder`.
#[cfg(all(not(feature = "native-zstd"), feature = "pure-zstd"))]
pub(crate) struct ZstdEncoder<W: Write> {
    enc: Option<libzstd_bitexact_rs::StreamEncoder>,
    sink: W,
    scratch: Vec<u8>,
}

#[cfg(all(not(feature = "native-zstd"), feature = "pure-zstd"))]
impl<W: Write> ZstdEncoder<W> {
    pub(crate) fn new(sink: W, level: i32) -> io::Result<Self> {
        Ok(Self {
            enc: Some(libzstd_bitexact_rs::StreamEncoder::new(level)),
            sink,
            scratch: Vec::new(),
        })
    }

    pub(crate) fn finish(mut self) -> io::Result<W> {
        let enc = self
            .enc
            .take()
            .expect("ZstdEncoder::finish called more than once");
        self.scratch.clear();
        enc.finish(&[], &mut self.scratch).map_err(to_io)?;
        self.sink.write_all(&self.scratch)?;
        self.sink.flush()?;
        Ok(self.sink)
    }
}

#[cfg(all(not(feature = "native-zstd"), feature = "pure-zstd"))]
impl<W: Write> Write for ZstdEncoder<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.scratch.clear();
        self.enc
            .as_mut()
            .expect("ZstdEncoder used after finish")
            .compress(buf, &mut self.scratch)
            .map_err(to_io)?;
        self.sink.write_all(&self.scratch)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.scratch.clear();
        self.enc
            .as_mut()
            .expect("ZstdEncoder used after finish")
            .flush(&mut self.scratch)
            .map_err(to_io)?;
        self.sink.write_all(&self.scratch)?;
        self.sink.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn round_trip_through_backend() {
        let data = vec![7u8; 100_000];
        let mut out = Vec::new();
        let mut enc = ZstdEncoder::new(&mut out, 3).unwrap();
        enc.write_all(&data).unwrap();
        enc.finish().unwrap();
        assert!(out.len() < data.len());

        let mut dec = decoder(Cursor::new(out)).unwrap();
        let mut back = Vec::new();
        dec.read_to_end(&mut back).unwrap();
        assert_eq!(back, data);
    }
}
