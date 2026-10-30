//! Apple II era disk filesystems, each behind its own Cargo feature.
//!
//! | Feature | Module | Filesystem |
//! |---|---|---|
//! | `ucsd` | [`ucsd`] | UCSD p-System (Apple Pascal and other p-System machines) |
//!
//! Apple DOS 3.3 and ProDOS move here once their remaining app dependencies
//! (the DOS 3.3 sector-order helpers and the user-overridable ProDOS type table) are untangled.

// `is_multiple_of` & co. come from disk-core's rust173 polyfill on 1.73; the real floor check is a 1.73 build.
#![allow(clippy::incompatible_msrv)]

#[cfg(feature = "ucsd")]
pub mod ucsd;
