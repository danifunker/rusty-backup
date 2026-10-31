//! Clonezilla backups: the partclone image reader (all of Clonezilla's compressors),
//! the image-folder metadata, and a block cache with a background metadata scan.

// `is_multiple_of` & co. come from disk-core's rust173 polyfill on 1.73; the real floor check is a 1.73 build.
#![allow(clippy::incompatible_msrv)]

#[cfg(not(any(feature = "native-zstd", feature = "pure-zstd")))]
compile_error!("enable one zstd backend: `native-zstd` (C libzstd) or `pure-zstd` (pure Rust)");

pub mod block_cache;
pub mod metadata;
pub mod metadata_scan;
pub mod partclone;

use std::io::{self, Read};

/// Wrap a reader in a streaming zstd decoder from whichever backend is enabled.
#[cfg(feature = "native-zstd")]
fn zstd_decoder<R: Read + Send + 'static>(r: R) -> io::Result<Box<dyn Read + Send>> {
    Ok(Box::new(zstd::Decoder::new(r)?))
}

/// Wrap a reader in a streaming zstd decoder from whichever backend is enabled.
#[cfg(all(not(feature = "native-zstd"), feature = "pure-zstd"))]
fn zstd_decoder<R: Read + Send + 'static>(r: R) -> io::Result<Box<dyn Read + Send>> {
    Ok(Box::new(libzstd_bitexact_rs::StreamDecoder::new(r)))
}
