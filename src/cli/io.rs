//! Image-file open helpers shared across CLI verbs.
//!
//! These are intentionally trivial wrappers around `std::fs::OpenOptions`
//! with consistent error context. The CLI never opens raw block devices
//! from these helpers — that goes through `os::SectorAlignedWriter` and
//! the device-write safety checks (see Phase C).

use anyhow::{Context, Result};
use std::fs::{File, OpenOptions};
use std::path::Path;

/// Open a regular image file read-only.
pub fn open_image_ro(path: &Path) -> Result<File> {
    File::open(path).with_context(|| format!("opening {}", path.display()))
}

/// Open a regular image file read+write. The caller is responsible for
/// ensuring the path resolves to a regular file (block-device targets are
/// rejected upstream in Phase C).
pub fn open_image_rw(path: &Path) -> Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .with_context(|| format!("opening {} for read/write", path.display()))
}
