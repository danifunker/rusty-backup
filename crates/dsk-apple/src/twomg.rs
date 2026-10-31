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

    // Some 2MG files have data_length=0 but a valid prodos_blocks count.
    // Fall back to prodos_blocks * 512 when data_length is missing.
    let effective_length = if data_length > 0 {
        data_length as u64
    } else if prodos_blocks > 0 {
        prodos_blocks as u64 * 512
    } else {
        0
    };

    Some(TwoMgHeader {
        data_offset: data_offset as u64,
        data_length: effective_length,
        image_format,
        prodos_blocks,
    })
}

/// Build a 64-byte 2MG header for the given raw disk data length.
///
/// The header uses:
/// - Magic: `"2IMG"`
/// - Creator: `"rsbk"` (Rusty Backup)
/// - Header size: 64
/// - Version: 1
/// - Image format: ProDOS order (1)
/// - Data offset: 64
/// - Data length: `data_length`
/// - ProDOS blocks: `data_length / 512`
pub fn build_twomg_header(data_length: u64) -> [u8; 64] {
    let mut buf = [0u8; 64];
    // Magic (offset 0, 4 bytes)
    buf[0..4].copy_from_slice(b"2IMG");
    // Creator (offset 4, 4 bytes)
    buf[4..8].copy_from_slice(b"rsbk");
    // Header size (offset 0x08, u16 LE)
    buf[0x08..0x0A].copy_from_slice(&64u16.to_le_bytes());
    // Version (offset 0x0A, u16 LE)
    buf[0x0A..0x0C].copy_from_slice(&1u16.to_le_bytes());
    // Image format (offset 0x0C, u32 LE) — ProDOS order
    buf[0x0C..0x10].copy_from_slice(&FORMAT_PRODOS.to_le_bytes());
    // Flags (offset 0x10, u32 LE) — 0
    // ProDOS blocks (offset 0x14, u32 LE)
    let blocks = (data_length / 512) as u32;
    buf[0x14..0x18].copy_from_slice(&blocks.to_le_bytes());
    // Data offset (offset 0x18, u32 LE)
    buf[0x18..0x1C].copy_from_slice(&64u32.to_le_bytes());
    // Data length (offset 0x1C, u32 LE)
    buf[0x1C..0x20].copy_from_slice(&(data_length as u32).to_le_bytes());
    buf
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
    fn test_build_twomg_header() {
        let data_len: u64 = 65536;
        let hdr = build_twomg_header(data_len);

        // Verify magic
        assert_eq!(&hdr[0..4], b"2IMG");
        // Creator
        assert_eq!(&hdr[4..8], b"rsbk");
        // Header size
        assert_eq!(u16::from_le_bytes([hdr[0x08], hdr[0x09]]), 64);
        // Version
        assert_eq!(u16::from_le_bytes([hdr[0x0A], hdr[0x0B]]), 1);
        // Format = ProDOS
        assert_eq!(
            u32::from_le_bytes([hdr[0x0C], hdr[0x0D], hdr[0x0E], hdr[0x0F]]),
            FORMAT_PRODOS,
        );
        // ProDOS blocks
        assert_eq!(
            u32::from_le_bytes([hdr[0x14], hdr[0x15], hdr[0x16], hdr[0x17]]),
            128,
        );
        // Data offset
        assert_eq!(
            u32::from_le_bytes([hdr[0x18], hdr[0x19], hdr[0x1A], hdr[0x1B]]),
            64,
        );
        // Data length
        assert_eq!(
            u32::from_le_bytes([hdr[0x1C], hdr[0x1D], hdr[0x1E], hdr[0x1F]]),
            65536,
        );

        // Verify it round-trips through the parser
        let mut cursor = Cursor::new(&hdr[..]);
        let parsed = parse_twomg_header(&mut cursor).unwrap();
        assert_eq!(parsed.data_offset, 64);
        assert_eq!(parsed.data_length, data_len);
        assert_eq!(parsed.image_format, FORMAT_PRODOS);
        assert_eq!(parsed.prodos_blocks, 128);
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
