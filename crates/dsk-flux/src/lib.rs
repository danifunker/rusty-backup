//! Flux-level floppy captures decoded to flat sector images, in the [`flux`] module.

// `is_multiple_of` & co. come from disk-core's rust173 polyfill on 1.73; the real floor check is a 1.73 build.
#![allow(clippy::incompatible_msrv)]

pub mod flux;
