//! Path-based source openers that transparently unwrap CHD containers.
//!
//! For every code path that reads a partition out of a disk image by
//! `(path, partition_offset)`, the raw bytes at `partition_offset` are only
//! meaningful if the file is a flat image. CHD files store compressed hunks,
//! so callers have to swap in [`ChdReader`] before applying the offset.
//!
//! `is_chd_path` does the cheap magic sniff; `open_read` returns a boxed
//! `Read+Seek+Send` ready for `open_filesystem` / `partition_minimum_size`
//! and friends. CHD writes are not supported, so there is no `open_write`.

use std::fs::File;
use std::io::{BufReader, Read as _};
use std::path::Path;

use anyhow::{Context, Result};

use crate::rbformats::chd::ChdReader;
use crate::rbformats::ReadSeek;

/// Cheap magic sniff: returns true when `path` starts with `MComprHD`.
pub fn is_chd_path(path: &Path) -> bool {
    let Ok(mut f) = File::open(path) else {
        return false;
    };
    let mut magic = [0u8; 8];
    matches!(f.read(&mut magic), Ok(n) if n == 8) && &magic == b"MComprHD"
}

/// Open `path` for reading, transparently wrapping CHD files in [`ChdReader`].
///
/// Partition offsets passed to consumers (e.g. `open_filesystem`) match the
/// original raw image, so callers don't have to know whether the underlying
/// container is a CHD.
pub fn open_read(path: &Path) -> Result<Box<dyn ReadSeek>> {
    if is_chd_path(path) {
        let chd = ChdReader::open(path).with_context(|| format!("open CHD {}", path.display()))?;
        Ok(Box::new(chd))
    } else {
        let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
        Ok(Box::new(BufReader::new(f)))
    }
}
