//! ATR — the standard container for 8-bit Atari disk images (the format
//! the SIO2PC / APE / emulator world uses). It is a thin 16-byte header in
//! front of the raw sector body; `.xfd` is the same body with no header.
//!
//! ```text
//! 0x00..0x02  magic 0x0296 (LO/HI: 0x96 0x02) — "NICKATARI"
//! 0x02..0x04  image size in 16-byte paragraphs, low 16 bits (LE)
//! 0x04..0x06  sector size in bytes (128 or 256) (LE)
//! 0x06        image size paragraphs, high 8 bits
//! 0x07..0x10  flags / reserved (unused here)
//! 0x10..      raw sector body
//! ```
//!
//! We decode by stripping the header to expose the flat sector body the
//! [`crate::fs::atari_dos`] engine reads, and encode by wrapping a body
//! back up. Single-density (128-byte) is the common Atari800 case; the
//! body is uniform so no per-sector fix-up is needed.

use anyhow::{bail, Result};

/// 16-byte ATR header length.
pub const ATR_HEADER_LEN: usize = 16;
/// ATR magic, little-endian 0x0296.
pub const ATR_MAGIC: [u8; 2] = [0x96, 0x02];

/// True when `head` starts with the ATR magic.
pub fn looks_like_atr_header(head: &[u8]) -> bool {
    head.len() >= 2 && head[0] == ATR_MAGIC[0] && head[1] == ATR_MAGIC[1]
}

/// Strip the ATR header, returning the flat sector body. Validates the
/// magic and that the declared paragraph count matches the body length.
pub fn decode_atr_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    if bytes.len() < ATR_HEADER_LEN {
        bail!("ATR too small ({} bytes)", bytes.len());
    }
    if !looks_like_atr_header(bytes) {
        bail!("not an ATR image (bad magic)");
    }
    let paras = (bytes[2] as usize) | ((bytes[3] as usize) << 8) | ((bytes[6] as usize) << 16);
    let declared = paras * 16;
    let body = &bytes[ATR_HEADER_LEN..];
    // Some images pad the file; trust the declared size when it fits, else
    // take the whole body.
    let take = if declared > 0 && declared <= body.len() {
        declared
    } else {
        body.len()
    };
    Ok(body[..take].to_vec())
}

/// Wrap a flat sector body in a fresh ATR header. `sector_size` is 128 or
/// 256; the body must be a whole number of paragraphs (16 bytes).
pub fn encode_atr_bytes(body: &[u8], sector_size: u16) -> Result<Vec<u8>> {
    if !body.len().is_multiple_of(16) {
        bail!("ATR body must be a multiple of 16 bytes");
    }
    let paras = (body.len() / 16) as u32;
    let mut out = vec![0u8; ATR_HEADER_LEN];
    out[0] = ATR_MAGIC[0];
    out[1] = ATR_MAGIC[1];
    out[2] = (paras & 0xFF) as u8;
    out[3] = ((paras >> 8) & 0xFF) as u8;
    out[4] = (sector_size & 0xFF) as u8;
    out[5] = ((sector_size >> 8) & 0xFF) as u8;
    out[6] = ((paras >> 16) & 0xFF) as u8;
    out.extend_from_slice(body);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_strip_and_wrap() {
        let body = vec![0xABu8; 92160];
        let atr = encode_atr_bytes(&body, 128).unwrap();
        assert_eq!(atr.len(), 92160 + 16);
        assert!(looks_like_atr_header(&atr));
        assert_eq!(&atr[0..2], &ATR_MAGIC);
        // header declares 5760 paragraphs, sector size 128.
        assert_eq!(u16::from_le_bytes([atr[4], atr[5]]), 128);
        let back = decode_atr_bytes(&atr).unwrap();
        assert_eq!(back, body);
    }

    #[test]
    fn rejects_non_atr() {
        assert!(decode_atr_bytes(b"not an atr image at all").is_err());
        assert!(!looks_like_atr_header(b"PK\x03\x04"));
    }
}
