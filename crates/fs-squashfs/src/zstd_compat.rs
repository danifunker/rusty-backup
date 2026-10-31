//! zstd through whichever backend the `native-zstd` / `pure-zstd` feature picks.

use std::io;

#[cfg(not(any(feature = "native-zstd", feature = "pure-zstd")))]
compile_error!("enable one zstd backend: `native-zstd` (C libzstd) or `pure-zstd` (pure Rust)");

/// Decompress a full in-memory zstd frame sequence.
#[cfg(feature = "native-zstd")]
pub(crate) fn decode_all(data: &[u8]) -> io::Result<Vec<u8>> {
    zstd::decode_all(data)
}

/// Compress a full buffer in one shot at `level`.
#[cfg(feature = "native-zstd")]
pub(crate) fn encode_all(data: &[u8], level: i32) -> io::Result<Vec<u8>> {
    zstd::encode_all(data, level)
}

/// Decompress a full in-memory zstd frame sequence.
#[cfg(all(not(feature = "native-zstd"), feature = "pure-zstd"))]
pub(crate) fn decode_all(data: &[u8]) -> io::Result<Vec<u8>> {
    libzstd_bitexact_rs::decompress(data).map_err(|e| disk_core::io_other(e.to_string()))
}

/// Compress a full buffer in one shot at `level`.
#[cfg(all(not(feature = "native-zstd"), feature = "pure-zstd"))]
pub(crate) fn encode_all(data: &[u8], level: i32) -> io::Result<Vec<u8>> {
    libzstd_bitexact_rs::compress(data, level).map_err(|e| disk_core::io_other(e.to_string()))
}
