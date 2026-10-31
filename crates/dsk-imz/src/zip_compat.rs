//! A zip entry's name across the two zip lines this crate builds against (a copy of
//! rusty-backup's `compat::ZipEntryName`): zip 9 decodes names fallibly, zip 2.4 (the only
//! line Rust 1.73 can build) returns `&str`. The `rust173-polyfill` feature picks the 2.4 form.

/// A zip entry's decoded name.
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
