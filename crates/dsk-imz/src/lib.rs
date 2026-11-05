//! WinImage IMZ (a ZIP holding one floppy image), in [`imz`]: open, extract, and with the
//! `crypto` feature, WinImage's own password scheme (AES-128-CBC).

// `is_multiple_of` & co. come from disk-core's rust173 polyfill on 1.73; the real floor check is a 1.73 build.
#![allow(clippy::incompatible_msrv)]

pub mod imz;
mod zip_compat;
