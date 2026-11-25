//! Helpers several 8-bit drivers share byte for byte (B8-016): positioned sector I/O and the
//! space-padded 8.3 name codec of RS-DOS and Atari DOS.

use disk_core::filesystem::FilesystemError;
#[cfg(any(feature = "coco", feature = "trdos"))]
use std::io::{Seek, SeekFrom, Write};

/// Read `len` bytes at absolute offset `at`.
#[cfg(feature = "coco")]
pub(crate) fn read_vec_at<R: std::io::Read + Seek>(
    r: &mut R,
    at: u64,
    len: usize,
) -> Result<Vec<u8>, FilesystemError> {
    r.seek(SeekFrom::Start(at))?;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

/// Write `data` at absolute offset `at`.
#[cfg(any(feature = "coco", feature = "trdos"))]
pub(crate) fn write_all_at<W: Write + Seek>(
    w: &mut W,
    at: u64,
    data: &[u8],
) -> Result<(), FilesystemError> {
    w.seek(SeekFrom::Start(at))?;
    w.write_all(data)?;
    Ok(())
}

/// "NAME.EXT" from an 11-byte space-padded 8.3 name.
#[cfg(any(feature = "coco", feature = "atari-dos"))]
pub(crate) fn decode_space_padded_83(raw: &[u8; 11]) -> String {
    let name = String::from_utf8_lossy(&raw[0..8]).trim_end().to_string();
    let ext = String::from_utf8_lossy(&raw[8..11]).trim_end().to_string();
    if ext.is_empty() {
        name
    } else {
        format!("{name}.{ext}")
    }
}

/// Encode "NAME.EXT" as an 11-byte space-padded 8.3 name: A-Z / 0-9, starting with a letter.
#[cfg(any(feature = "coco", feature = "atari-dos"))]
pub(crate) fn encode_alnum_83(name: &str, family: &str) -> Result<[u8; 11], FilesystemError> {
    let (base, ext) = match name.split_once('.') {
        Some((b, e)) => (b, e),
        None => (name, ""),
    };
    if base.is_empty() || base.len() > 8 || ext.len() > 3 {
        return Err(FilesystemError::InvalidData(format!(
            "{family} filename '{name}' must be 1-8 chars + optional 1-3 char extension"
        )));
    }
    let mut out = [b' '; 11];
    let put = |slice: &str, dst: &mut [u8]| -> Result<(), FilesystemError> {
        for (i, c) in slice.chars().enumerate() {
            if !c.is_ascii() {
                return Err(FilesystemError::InvalidData(format!(
                    "{family} filename '{name}' contains non-ASCII character '{c}'"
                )));
            }
            let b = (c as u8).to_ascii_uppercase();
            if !b.is_ascii_uppercase() && !b.is_ascii_digit() {
                return Err(FilesystemError::InvalidData(format!(
                    "{family} filename '{name}' char '{c}' must be A-Z or 0-9"
                )));
            }
            dst[i] = b;
        }
        Ok(())
    };
    put(base, &mut out[0..8])?;
    put(ext, &mut out[8..11])?;
    if !out[0].is_ascii_uppercase() {
        return Err(FilesystemError::InvalidData(format!(
            "{family} filename '{name}' must start with a letter"
        )));
    }
    Ok(out)
}
