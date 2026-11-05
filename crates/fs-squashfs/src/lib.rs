//! SquashFS: read ([`squashfs`]), build ([`squashfs_write`]), edit by rebuilding
//! ([`squashfs_edit`]) and verify ([`squashfs_verify`]). Compressors: gzip, xz,
//! lzma, lz4 and zstd (backend chosen by the `native-zstd` / `pure-zstd` feature).

// `is_multiple_of` & co. come from disk-core's rust173 polyfill on 1.73; the real floor check is a 1.73 build.
#![allow(clippy::incompatible_msrv)]

pub mod squashfs;
pub mod squashfs_edit;
pub mod squashfs_verify;
pub mod squashfs_write;
mod zstd_compat;
