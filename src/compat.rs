//! Thin wrappers over a few std APIs that differ between the modern desktop
//! toolchain and the Rust 1.73 vintage (macOS 10.7) build.
//!
//! Unlike [`rust173_compat`](crate::rust173_compat) (which is compiled *only*
//! for the vintage build), this module is **always compiled**: both builds call
//! these helpers. The body of each is `#[cfg]`-split so the **modern build uses
//! the current, clippy-clean std API** (`io::Error::other`, `iter::repeat_n`)
//! and only the vintage build falls back to the older form that compiles on
//! 1.73. This keeps the discouraged forms (which trip default-on lints
//! `clippy::io_other_error` / `clippy::manual_repeat_n`) out of the desktop
//! tree entirely, satisfying the repo's zero-warnings policy.
//!
//! When macOS 10.7 support is dropped, delete the `rust173-polyfill` feature and
//! the `#[cfg(feature = "rust173-polyfill")]` arms here; callers can then be
//! mechanically inlined back to the bare std calls if desired.

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

/// A zip entry's decoded name. zip 9 decodes it fallibly; the vintage builds' zip 2.4 returns `&str`.
pub trait ZipEntryName {
    /// The entry name, falling back to a lossy decode of the raw bytes when zip 9 cannot decode it.
    fn entry_name(&self) -> String;
}

#[cfg(not(feature = "rust173-polyfill"))]
impl<R: std::io::Read + ?Sized> ZipEntryName for zip::read::ZipFile<'_, R> {
    fn entry_name(&self) -> String {
        match self.name() {
            Ok(n) => n.into_owned(),
            Err(_) => String::from_utf8_lossy(self.name_raw()).into_owned(),
        }
    }
}

#[cfg(feature = "rust173-polyfill")]
impl ZipEntryName for zip::read::ZipFile<'_> {
    fn entry_name(&self) -> String {
        self.name().to_string()
    }
}
