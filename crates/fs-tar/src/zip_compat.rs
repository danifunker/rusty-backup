//! A zip entry's name across the two zip lines this crate builds against (zip 9 and zip 2.4,
//! the only line Rust 1.73 can build). Both have `name()` and `name_raw()`; zip 9's `name()`
//! decodes fallibly, zip 2.4's returns `&str`. Dispatching on that return type, not on a
//! feature, keeps every feature mix building whichever zip Cargo resolves (TAR-007).

use std::borrow::Cow;

/// What `ZipFile::name()` returns, turned into an owned name.
pub trait IntoEntryName {
    /// The name, falling back to a lossy decode of `raw` when zip 9 cannot decode it.
    fn into_entry_name(self, raw: &[u8]) -> String;
}

impl IntoEntryName for &str {
    fn into_entry_name(self, _raw: &[u8]) -> String {
        self.to_string()
    }
}

impl<E> IntoEntryName for Result<Cow<'_, str>, E> {
    fn into_entry_name(self, raw: &[u8]) -> String {
        match self {
            Ok(n) => n.into_owned(),
            Err(_) => String::from_utf8_lossy(raw).into_owned(),
        }
    }
}

/// A zip entry's decoded name on either zip line: `entry_name(f.name(), f.name_raw())`.
pub fn entry_name<N: IntoEntryName>(name: N, raw: &[u8]) -> String {
    name.into_entry_name(raw)
}
