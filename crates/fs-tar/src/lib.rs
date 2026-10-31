//! Tar archives in and out of any `disk-core` filesystem.
//!
//! - [`tar_export`]: write a filesystem (or a selection in it) as `.tar`, `.tar.gz` or `.tar.zst`.
//! - [`tar_import`]: unpack a tar into any editable filesystem, keeping modes, owners, times,
//!   symlinks and raw (non-UTF-8) names where the target can hold them.
//! - [`import_sink`]: the write half every "import a tree" flow shares (conflict policy,
//!   `mkdir -p`, attribute inheritance); a host-folder importer can push into it too.
//!
//! With the `appledouble` feature, macOS `._name` sidecars in an archive are joined to their
//! file as a resource fork and Finder info (that pulls in `fs-mac`); without it they are skipped
//! and counted.

// `is_multiple_of` & co. come from disk-core's rust173 polyfill on 1.73; the real floor check is a 1.73 build.
#![allow(clippy::incompatible_msrv)]

pub mod import_sink;
pub mod tar_export;
pub mod tar_import;
mod zstd_compat;

#[cfg(test)]
mod test_open;

/// Files up to this size are buffered in memory; larger ones spool to a temp file.
pub(crate) const SPOOL_THRESHOLD: usize = 16 * 1024 * 1024;
