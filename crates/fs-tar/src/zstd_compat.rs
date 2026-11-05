//! zstd through whichever backend the `native-zstd` / `pure-zstd` feature picks (a copy of
//! rusty-backup's `rbformats::zstd_compat`, trimmed to the streaming halves tar needs).

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
/// Compress a full buffer in one shot at `level`.
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
    disk_core::io_other(e.to_string())
}

#[cfg(all(not(feature = "native-zstd"), feature = "pure-zstd"))]
pub(crate) fn decoder<R: Read + Send + 'static>(r: R) -> io::Result<Box<dyn Read + Send>> {
    Ok(Box::new(libzstd_bitexact_rs::StreamDecoder::new(r)))
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
