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
//!
//! `io_other` and `repeat_n` live in the `disk-core` crate (shared with the
//! extracted `crates/`); the rest here depends on app-only crates like `zip`.

pub use disk_core::{io_other, repeat_n};

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

/// Write `bytes` to `path`, seeking over all-zero 1 MiB runs so a blank volume lands sparse.
pub fn write_sparse(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    write_sparse_parts(path, &[(0, bytes)], bytes.len() as u64)
}

/// [`write_sparse`] for pieces at byte offsets in a file of `total_len` (an MBR plus its volume).
pub fn write_sparse_parts(
    path: &std::path::Path,
    parts: &[(u64, &[u8])],
    total_len: u64,
) -> std::io::Result<()> {
    use std::io::{Seek, SeekFrom, Write};
    const RUN: usize = 1 << 20;
    let mut f = std::fs::File::create(path)?;
    for &(at, bytes) in parts {
        for (k, chunk) in bytes.chunks(RUN).enumerate() {
            if chunk.iter().all(|&b| b == 0) {
                continue;
            }
            f.seek(SeekFrom::Start(at + (k * RUN) as u64))?;
            f.write_all(chunk)?;
        }
    }
    f.set_len(total_len)?;
    f.sync_all()
}

/// Up to the first `n` bytes of `path`, for sniffing a header without reading the whole file.
pub fn read_head(path: &std::path::Path, n: usize) -> std::io::Result<Vec<u8>> {
    use std::io::Read;
    let mut buf = Vec::with_capacity(n);
    std::fs::File::open(path)?
        .take(n as u64)
        .read_to_end(&mut buf)?;
    Ok(buf)
}
