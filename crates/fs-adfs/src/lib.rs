//! Acorn ADFS / FileCore, in the [`adfs`] module.
//!
//! GPL-2.0-only: the new-map free-space walk (`AdfsFsm`) follows the Linux kernel's `fs/adfs/map.c`.

// `is_multiple_of` & co. come from disk-core's rust173 polyfill on 1.73; the real floor check is a 1.73 build.
#![allow(clippy::incompatible_msrv)]

pub mod adfs;
