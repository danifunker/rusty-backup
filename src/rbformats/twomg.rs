//! 2MG (2IMG) disk image format parser.
//!
//! 2MG is a wrapper format for Apple II disk images.  The 64-byte header
//! describes the location and size of the raw disk data within the file.

use std::io::Read;

/// Magic bytes at offset 0 of a 2MG file.
pub const TWOMG_MAGIC: &[u8; 4] = b"2IMG";

/// Image format constants (offset 0x0C).
pub const FORMAT_DOS33: u32 = 0;
pub const FORMAT_PRODOS: u32 = 1;
pub const FORMAT_NIB: u32 = 2;

/// Parsed 2MG header.
#[derive(Debug, Clone)]
pub struct TwoMgHeader {
    /// Byte offset where the disk data begins (typically 64).
    pub data_offset: u64,
    /// Length of the disk data in bytes.
    pub data_length: u64,
    /// Image format: 0 = DOS 3.3 order, 1 = ProDOS order, 2 = NIB.
    pub image_format: u32,
    /// Number of 512-byte ProDOS blocks (offset 0x14).
    pub prodos_blocks: u32,
}

/// Parse a 2MG header from the first 64 bytes of a reader.
///
/// Returns `None` if the magic doesn't match or the header is too short.
pub fn parse_twomg_header(reader: &mut impl Read) -> Option<TwoMgHeader> {
    let mut buf = [0u8; 64];
    reader.read_exact(&mut buf).ok()?;

    // Check magic
    if &buf[0..4] != TWOMG_MAGIC {
        return None;
    }

    // Header size at 0x08 (u16 LE) — sanity check
    let header_size = u16::from_le_bytes([buf[0x08], buf[0x09]]);
    if header_size < 64 {
        return None;
    }

    let image_format = u32::from_le_bytes([buf[0x0C], buf[0x0D], buf[0x0E], buf[0x0F]]);
    let prodos_blocks = u32::from_le_bytes([buf[0x14], buf[0x15], buf[0x16], buf[0x17]]);
    let data_offset = u32::from_le_bytes([buf[0x18], buf[0x19], buf[0x1A], buf[0x1B]]);
    let data_length = u32::from_le_bytes([buf[0x1C], buf[0x1D], buf[0x1E], buf[0x1F]]);

    Some(TwoMgHeader {
        data_offset: data_offset as u64,
        data_length: data_length as u64,
        image_format,
        prodos_blocks,
    })
}

impl TwoMgHeader {
    /// Human-readable description of the image format.
    pub fn format_name(&self) -> &'static str {
        match self.image_format {
            FORMAT_DOS33 => "DOS 3.3 order",
            FORMAT_PRODOS => "ProDOS order",
            FORMAT_NIB => "Nibblized",
            _ => "Unknown",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn make_header(format: u32, data_offset: u32, data_length: u32, blocks: u32) -> [u8; 64] {
        let mut buf = [0u8; 64];
        buf[0..4].copy_from_slice(b"2IMG");
        // Creator at 4..8
        buf[4..8].copy_from_slice(b"TEST");
        // Header size at 0x08 (u16 LE)
        buf[0x08..0x0A].copy_from_slice(&64u16.to_le_bytes());
        // Version at 0x0A (u16 LE)
        buf[0x0A..0x0C].copy_from_slice(&1u16.to_le_bytes());
        // Image format at 0x0C
        buf[0x0C..0x10].copy_from_slice(&format.to_le_bytes());
        // ProDOS blocks at 0x14
        buf[0x14..0x18].copy_from_slice(&blocks.to_le_bytes());
        // Data offset at 0x18
        buf[0x18..0x1C].copy_from_slice(&data_offset.to_le_bytes());
        // Data length at 0x1C
        buf[0x1C..0x20].copy_from_slice(&data_length.to_le_bytes());
        buf
    }

    #[test]
    fn test_parse_prodos_header() {
        let hdr = make_header(FORMAT_PRODOS, 64, 65536, 128);
        let mut cursor = Cursor::new(&hdr[..]);
        let parsed = parse_twomg_header(&mut cursor).unwrap();
        assert_eq!(parsed.data_offset, 64);
        assert_eq!(parsed.data_length, 65536);
        assert_eq!(parsed.image_format, FORMAT_PRODOS);
        assert_eq!(parsed.prodos_blocks, 128);
        assert_eq!(parsed.format_name(), "ProDOS order");
    }

    #[test]
    fn test_parse_dos33_header() {
        let hdr = make_header(FORMAT_DOS33, 64, 143360, 280);
        let mut cursor = Cursor::new(&hdr[..]);
        let parsed = parse_twomg_header(&mut cursor).unwrap();
        assert_eq!(parsed.image_format, FORMAT_DOS33);
        assert_eq!(parsed.format_name(), "DOS 3.3 order");
    }

    #[test]
    fn test_bad_magic_returns_none() {
        let mut hdr = make_header(FORMAT_PRODOS, 64, 65536, 128);
        hdr[0] = b'X';
        let mut cursor = Cursor::new(&hdr[..]);
        assert!(parse_twomg_header(&mut cursor).is_none());
    }

    #[test]
    fn test_short_read_returns_none() {
        let data = [0u8; 10];
        let mut cursor = Cursor::new(&data[..]);
        assert!(parse_twomg_header(&mut cursor).is_none());
    }

    #[test]
    fn test_small_header_size_returns_none() {
        let mut hdr = make_header(FORMAT_PRODOS, 64, 65536, 128);
        // Set header size to 32 (too small)
        hdr[0x08..0x0A].copy_from_slice(&32u16.to_le_bytes());
        let mut cursor = Cursor::new(&hdr[..]);
        assert!(parse_twomg_header(&mut cursor).is_none());
    }
}
