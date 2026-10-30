//! The base crate under every rusty-backup filesystem, partition and disk-image crate.
//!
//! Always available:
//! - [`io_other`] / [`repeat_n`]: std shims that build on both current Rust and
//!   Rust 1.73 (the last toolchain whose std targets macOS 10.7).
//! - [`display`]: small neutral formatters (sizes, Mac `OSType`, Unix modes).
//! - `rust173` (feature `rust173-polyfill` only): extension traits standing in
//!   for std methods missing on 1.73. Never enable it on a modern toolchain.
//!
//! With the `fs` feature (on by default), the interface every filesystem crate
//! (`fs-8bit`, `fs-mac`, …) implements:
//! - [`filesystem`]: `Filesystem` (read) and `EditableFilesystem` (write),
//!   `FilesystemError`, create/repair options.
//! - [`entry`]: `FileEntry`, `EntryType` and directory display ordering.
//! - [`fsck`]: integrity-check results shared by every checker.
//! - [`times`]: Unix timestamps and the calendar conversions filesystems need.
//! - [`xattr`]: extended-attribute names and values.
//!
//! Partition and disk-image crates depend on this with `default-features = false`
//! and so build without the filesystem types and their dependencies.

// `is_multiple_of` & co. come from the rust173 polyfill on 1.73; the real floor check is a 1.73 build.
#![allow(clippy::incompatible_msrv)]

pub mod display;
#[cfg(feature = "rust173-polyfill")]
pub mod rust173;

#[cfg(feature = "fs")]
pub mod entry;
#[cfg(feature = "fs")]
pub mod filesystem;
#[cfg(feature = "fs")]
pub mod fsck;
#[cfg(feature = "fs")]
pub mod times;
#[cfg(feature = "fs")]
pub mod xattr;

/// Construct an `io::Error` of kind `Other` wrapping `error`.
///
/// Modern: `io::Error::other` (stable since Rust 1.74). Vintage (1.73): the
/// equivalent `io::Error::new(ErrorKind::Other, ..)`.
#[inline]
pub fn io_other<E>(error: E) -> std::io::Error
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    #[cfg(not(feature = "rust173-polyfill"))]
    {
        std::io::Error::other(error)
    }
    #[cfg(feature = "rust173-polyfill")]
    {
        std::io::Error::new(std::io::ErrorKind::Other, error)
    }
}

/// An iterator that repeats `value` exactly `count` times.
///
/// Modern: `iter::repeat_n` (stable since Rust 1.87). Vintage (1.73): the
/// equivalent `iter::repeat(value).take(count)`.
#[inline]
pub fn repeat_n<T: Clone>(value: T, count: usize) -> impl Iterator<Item = T> {
    #[cfg(not(feature = "rust173-polyfill"))]
    {
        std::iter::repeat_n(value, count)
    }
    #[cfg(feature = "rust173-polyfill")]
    {
        std::iter::repeat(value).take(count)
    }
}
