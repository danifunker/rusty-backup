//! Path-based source openers that transparently unwrap container formats.
//!
//! For every code path that reads a partition out of a disk image by
//! `(path, partition_offset)`, the raw bytes at `partition_offset` are only
//! meaningful if the file is a flat image. CHD files store compressed hunks
//! and GHO files store a compressed sector stream, so callers have to swap
//! in [`ChdReader`] / [`GhoReader`] before applying the offset.
//!
//! `is_chd_path` / `is_gho_path` do cheap magic sniffs; `open_read` returns
//! a boxed `Read+Seek+Send` ready for `open_filesystem` /
//! `partition_minimum_size` and friends. Container writes are not supported,
//! so there is no `open_write`.

use std::fs::File;
use std::io::{BufReader, Read as _};
use std::path::Path;

use anyhow::{Context, Result};

use crate::rbformats::chd::ChdReader;
use crate::rbformats::gho::{GhoReader, GHO_MAGIC};
use crate::rbformats::ReadSeek;

/// Cheap magic sniff: returns true when `path` starts with `MComprHD`.
pub fn is_chd_path(path: &Path) -> bool {
    let Ok(mut f) = File::open(path) else {
        return false;
    };
    let mut magic = [0u8; 8];
    matches!(f.read(&mut magic), Ok(n) if n == 8) && &magic == b"MComprHD"
}

/// Cheap magic sniff: returns true when `path` starts with the Norton
/// Ghost container magic `FE EF`. Both `.gho` (primary) and `.ghs` (span)
/// files carry the same magic, so the result is true for any file in a
/// Ghost span set.
///
/// This does NOT distinguish SECTOR-mode from file-aware: the latter
/// still has the magic but `GhoReader::open` will refuse it. Callers
/// must be prepared for `open_read` to fail on file-aware GHOs.
pub fn is_gho_path(path: &Path) -> bool {
    let Ok(mut f) = File::open(path) else {
        return false;
    };
    let mut magic = [0u8; 2];
    matches!(f.read(&mut magic), Ok(n) if n == 2) && magic == GHO_MAGIC
}

/// Open `path` for reading, transparently wrapping CHD and SECTOR-mode
/// GHO files in their respective streaming readers.
///
/// Partition offsets passed to consumers (e.g. `open_filesystem`) match the
/// original raw image, so callers don't have to know whether the underlying
/// container is a CHD or GHO.
///
/// File-aware (`image_type=0x00`) and password-protected GHOs cause
/// `GhoReader::open` to error out; the caller can then fall back to the
/// legacy `materialize_gho_to_temp` path which decodes to a tempfile.
pub fn open_read(path: &Path) -> Result<Box<dyn ReadSeek>> {
    if is_chd_path(path) {
        let chd = ChdReader::open(path).with_context(|| format!("open CHD {}", path.display()))?;
        Ok(Box::new(chd))
    } else if is_gho_path(path) {
        let gho = GhoReader::open(path).with_context(|| format!("open GHO {}", path.display()))?;
        Ok(Box::new(gho))
    } else {
        let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
        Ok(Box::new(BufReader::new(f)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Seek, SeekFrom, Write};

    #[test]
    fn is_gho_path_rejects_non_gho() {
        let dir = tempfile::tempdir().unwrap();
        let plain = dir.path().join("not_gho.bin");
        std::fs::write(&plain, b"hello world").unwrap();
        assert!(!is_gho_path(&plain));
    }

    #[test]
    fn is_gho_path_accepts_fe_ef_magic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fake.gho");
        let mut f = File::create(&path).unwrap();
        f.write_all(&[0xFE, 0xEF, 0x01]).unwrap();
        assert!(is_gho_path(&path));
    }

    /// SECTOR-mode uncompressed GHO opens as a GhoReader via the
    /// unified opener and reads through transparently.
    #[test]
    fn open_read_routes_sector_gho_through_gho_reader() {
        use crate::rbformats::gho::GHO_SECTOR_SIZE;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sector.gho");

        // Container header (FE EF version=1 comp=0 ... image_type=01 pw=0)
        // + 4 sectors of padding + FEEF sub-header + 2 sectors of data.
        let mut buf = vec![0u8; 512];
        buf[0] = 0xFE;
        buf[1] = 0xEF;
        buf[2] = 0x01; // version
        buf[3] = 0x00; // compression=none
        buf[10] = 0x01; // image_type=SECTOR
                        // Pad to sector 4 then FEEF sub-header at sector 4, data at sector 5.
        buf.resize(4 * GHO_SECTOR_SIZE as usize, 0);
        let mut feef = vec![0u8; 512];
        feef[0] = 0xFE;
        feef[1] = 0xEF;
        buf.extend_from_slice(&feef);
        // Two sectors of data.
        let mut s0 = vec![0u8; 512];
        s0[..16].copy_from_slice(b"sector zero data");
        let mut s1 = vec![0u8; 512];
        s1[..16].copy_from_slice(b"sector one  data");
        buf.extend_from_slice(&s0);
        buf.extend_from_slice(&s1);
        std::fs::write(&path, &buf).unwrap();

        let mut r = open_read(&path).unwrap();
        let mut sector = [0u8; 512];
        r.read_exact(&mut sector).unwrap();
        assert_eq!(&sector[..16], b"sector zero data");
        r.seek(SeekFrom::Start(512)).unwrap();
        r.read_exact(&mut sector).unwrap();
        assert_eq!(&sector[..16], b"sector one  data");
    }
}
