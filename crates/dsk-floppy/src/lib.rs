//! Floppy and emulator disk-image wrappers, each decoded to (and most encoded from) a flat
//! sector image. [`floppy_geom`] is the shared PC-98 / X68000 media-geometry table.

// `is_multiple_of` & co. come from disk-core's rust173 polyfill on 1.73; the real floor check is a 1.73 build.
#![allow(clippy::incompatible_msrv)]

pub mod atr;
pub mod d88;
pub mod dim;
pub mod edsk;
pub mod floppy_geom;
pub mod g64;
pub mod hdf;
pub mod hdm;
pub mod msa;
pub mod xdf;
