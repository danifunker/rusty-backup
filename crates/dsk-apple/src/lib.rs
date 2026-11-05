//! Apple disk-image containers, each behind its own Cargo feature.
//!
//! | Feature | Modules | Container |
//! |---|---|---|
//! | `dc42` | [`dc42`] | DiskCopy 4.2 (incl. Lisa / Twiggy images with tags) |
//! | `twomg` | [`twomg`] | 2IMG / 2MG (Apple II) |
//! | `woz` | [`woz`], [`woz_write`], [`moof`] | WOZ 1 / 2 (Apple II) and MOOF (Macintosh) bitstream images, read and write |
//! | `dart` | [`dart`], [`lzhuf`] | DART (Disk Archive / Retrieval Tool), RLE and LZH |

// `is_multiple_of` & co. come from disk-core's rust173 polyfill on 1.73; the real floor check is a 1.73 build.
#![allow(clippy::incompatible_msrv)]

#[cfg(feature = "dart")]
pub mod dart;
#[cfg(feature = "dc42")]
pub mod dc42;
#[cfg(feature = "dart")]
pub mod lzhuf;
#[cfg(feature = "woz")]
pub mod moof;
#[cfg(feature = "twomg")]
pub mod twomg;
#[cfg(feature = "woz")]
pub mod woz;
#[cfg(feature = "woz")]
pub mod woz_write;
