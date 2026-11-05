//! CP/M 2.2 / 3.0 filesystems.
//!
//! [`cpm`] implements [`disk_core::filesystem::Filesystem`] /
//! [`disk_core::filesystem::EditableFilesystem`]; [`cpm_diskdefs`] is the catalog
//! of per-machine disk parameter blocks it opens volumes with.

// `is_multiple_of` & co. come from disk-core's rust173 polyfill on 1.73; the real floor check is a 1.73 build.
#![allow(clippy::incompatible_msrv)]

pub mod cpm;
pub mod cpm_diskdefs;
