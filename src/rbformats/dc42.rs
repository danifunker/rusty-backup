//! DiskCopy 4.2 disk image format parser.
//!
//! DiskCopy 4.2 was the standard Macintosh and Apple IIgs floppy disk image
//! format from the late 1980s through the Mac OS 9 era.  Most classic Mac HFS
//! floppy images found in archives use this format.
//!
//! The format has an 84-byte header followed by raw sector data, then optional
//! tag data (12 bytes per sector for 800K GCR disks, unused for MFM disks).
//! The sector data is in logical order — no interleaving needed.

use std::io::Read;

/// Header size in bytes.
pub const DC42_HEADER_SIZE: u64 = 84;

/// The "private" field value that identifies a valid DiskCopy 4.2 file.
/// Located at offset 82-83, big-endian.  Must be 0x0100.
const DC42_PRIVATE_MAGIC: u16 = 0x0100;

/// Parsed DiskCopy 4.2 header.
#[derive(Debug, Clone)]
pub struct Dc42Header {
    /// Disk name (from the Pascal string at offset 0).
    pub disk_name: String,
    /// Size of the sector data in bytes (offset 64, u32 BE).
    pub data_size: u64,
    /// Size of the tag data in bytes (offset 68, u32 BE).
    pub tag_size: u64,
    /// Checksum of the sector data (offset 72, u32 BE).
    pub data_checksum: u32,
    /// Checksum of the tag data (offset 76, u32 BE).
    pub tag_checksum: u32,
    /// Disk format: 0=400K GCR, 1=800K GCR, 2=720K MFM, 3=1440K MFM, 0xFF=other.
    pub disk_format: u8,
    /// Format byte: 0x12=400K/800K Mac, 0x22=HD Mac, 0x24=ProDOS.
    pub format_byte: u8,
}

impl Dc42Header {
    /// Human-readable description of the disk format.
    pub fn format_name(&self) -> &'static str {
        match self.disk_format {
            0 => "400K GCR",
            1 => "800K GCR",
            2 => "720K MFM",
            3 => "1440K MFM",
            _ => "Unknown",
        }
    }
}

/// Parse a DiskCopy 4.2 header from the first 84 bytes of a reader.
///
/// Returns `None` if the data doesn't look like a valid DiskCopy 4.2 header.
/// Validation checks:
/// - Name length byte (offset 0) is 1-63
/// - Private field (offset 82-83) is 0x0100
/// - Data size is non-zero
pub fn parse_dc42_header(reader: &mut impl Read) -> Option<Dc42Header> {
    let mut buf = [0u8; 84];
    reader.read_exact(&mut buf).ok()?;

    // Byte 0 is Pascal string length (1-63)
    let name_len = buf[0] as usize;
    if name_len == 0 || name_len > 63 {
        return None;
    }

    // Private field at offset 82-83 must be 0x0100
    let private = u16::from_be_bytes([buf[82], buf[83]]);
    if private != DC42_PRIVATE_MAGIC {
        return None;
    }

    let data_size = u32::from_be_bytes([buf[64], buf[65], buf[66], buf[67]]) as u64;
    let tag_size = u32::from_be_bytes([buf[68], buf[69], buf[70], buf[71]]) as u64;
    let data_checksum = u32::from_be_bytes([buf[72], buf[73], buf[74], buf[75]]);
    let tag_checksum = u32::from_be_bytes([buf[76], buf[77], buf[78], buf[79]]);
    let disk_format = buf[80];
    let format_byte = buf[81];

    if data_size == 0 {
        return None;
    }

    // Extract disk name (bytes 1..1+name_len), ASCII/MacRoman
    let disk_name = String::from_utf8_lossy(&buf[1..1 + name_len]).into_owned();

    Some(Dc42Header {
        disk_name,
        data_size,
        tag_size,
        data_checksum,
        tag_checksum,
        disk_format,
        format_byte,
    })
}

/// Check whether a file could be DiskCopy 4.2 by reading the header and
/// verifying the file size matches `84 + data_size + tag_size`.
pub fn detect_dc42(reader: &mut impl Read, file_size: u64) -> Option<Dc42Header> {
    let header = parse_dc42_header(reader)?;
    let expected_size = DC42_HEADER_SIZE + header.data_size + header.tag_size;
    if file_size == expected_size {
        Some(header)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn make_dc42_header(name: &str, data_size: u32, tag_size: u32) -> Vec<u8> {
        let mut buf = vec![0u8; 84];
        // Pascal string: length byte + name
        let name_bytes = name.as_bytes();
        buf[0] = name_bytes.len() as u8;
        buf[1..1 + name_bytes.len()].copy_from_slice(name_bytes);
        // Data size (BE)
        buf[64..68].copy_from_slice(&data_size.to_be_bytes());
        // Tag size (BE)
        buf[68..72].copy_from_slice(&tag_size.to_be_bytes());
        // Checksums (arbitrary non-zero values)
        buf[72..76].copy_from_slice(&0x12345678u32.to_be_bytes());
        buf[76..80].copy_from_slice(&0x9ABCDEF0u32.to_be_bytes());
        // Disk format: 1 = 800K GCR
        buf[80] = 1;
        // Format byte: 0x12 = Mac 400K/800K
        buf[81] = 0x12;
        // Private field: 0x0100
        buf[82..84].copy_from_slice(&DC42_PRIVATE_MAGIC.to_be_bytes());
        buf
    }

    #[test]
    fn test_parse_valid_800k() {
        let header_bytes = make_dc42_header("Test Disk", 819200, 19200);
        let mut cursor = Cursor::new(&header_bytes);
        let hdr = parse_dc42_header(&mut cursor).expect("should parse");
        assert_eq!(hdr.disk_name, "Test Disk");
        assert_eq!(hdr.data_size, 819200);
        assert_eq!(hdr.tag_size, 19200);
        assert_eq!(hdr.disk_format, 1);
        assert_eq!(hdr.format_byte, 0x12);
        assert_eq!(hdr.format_name(), "800K GCR");
    }

    #[test]
    fn test_parse_valid_1440k() {
        let mut header_bytes = make_dc42_header("HD Disk", 1474560, 0);
        header_bytes[80] = 3; // 1440K MFM
        header_bytes[81] = 0x22; // HD Mac
        let mut cursor = Cursor::new(&header_bytes);
        let hdr = parse_dc42_header(&mut cursor).expect("should parse");
        assert_eq!(hdr.data_size, 1474560);
        assert_eq!(hdr.tag_size, 0);
        assert_eq!(hdr.format_name(), "1440K MFM");
    }

    #[test]
    fn test_reject_bad_private() {
        let mut header_bytes = make_dc42_header("Bad", 819200, 0);
        header_bytes[82] = 0;
        header_bytes[83] = 0;
        let mut cursor = Cursor::new(&header_bytes);
        assert!(parse_dc42_header(&mut cursor).is_none());
    }

    #[test]
    fn test_reject_zero_name() {
        let mut header_bytes = make_dc42_header("X", 819200, 0);
        header_bytes[0] = 0; // zero-length name
        let mut cursor = Cursor::new(&header_bytes);
        assert!(parse_dc42_header(&mut cursor).is_none());
    }

    #[test]
    fn test_reject_zero_data_size() {
        let header_bytes = make_dc42_header("Empty", 0, 0);
        let mut cursor = Cursor::new(&header_bytes);
        assert!(parse_dc42_header(&mut cursor).is_none());
    }

    #[test]
    fn test_detect_dc42_size_match() {
        let data_size: u32 = 819200;
        let tag_size: u32 = 19200;
        let header_bytes = make_dc42_header("Test", data_size, tag_size);
        let file_size = 84 + data_size as u64 + tag_size as u64;
        let mut cursor = Cursor::new(&header_bytes);
        assert!(detect_dc42(&mut cursor, file_size).is_some());
    }

    #[test]
    fn test_detect_dc42_size_mismatch() {
        let header_bytes = make_dc42_header("Test", 819200, 19200);
        let wrong_size = 900000u64; // doesn't match 84 + 819200 + 19200
        let mut cursor = Cursor::new(&header_bytes);
        assert!(detect_dc42(&mut cursor, wrong_size).is_none());
    }
}
