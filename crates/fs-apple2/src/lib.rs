//! Apple II disk filesystems, each behind its own Cargo feature.
//!
//! | Feature | Module | Filesystem |
//! |---|---|---|
//! | `apple-dos` | [`apple_dos`] | Apple DOS 3.3 |
//! | `prodos` | [`prodos`], [`prodos_types`] | ProDOS (plus the file-type name table) |
//! | `ucsd` | [`ucsd`] | UCSD p-System (Apple Pascal and other p-System machines) |
//!
//! [`sector_order`] (always built) maps between DOS 3.3 and ProDOS sector orders in
//! 140 KB `.dsk` / `.do` / `.po` images.

// `is_multiple_of` & co. come from disk-core's rust173 polyfill on 1.73; the real floor check is a 1.73 build.
#![allow(clippy::incompatible_msrv)]

#[cfg(feature = "apple-dos")]
pub mod apple_dos;
#[cfg(feature = "prodos")]
pub mod prodos;
#[cfg(feature = "prodos")]
pub mod prodos_types;
pub mod sector_order;
#[cfg(feature = "ucsd")]
pub mod ucsd;
