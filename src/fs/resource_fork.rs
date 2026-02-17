//! Resource fork serialization for HFS/HFS+ file extraction.
//!
//! Supports AppleDouble (._prefix sidecar), MacBinary III (.bin wrapper),
//! and separate .rsrc sidecar files.

use byteorder::{BigEndian, ByteOrder};

/// How to handle resource forks during extraction.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ResourceForkMode {
    /// AppleDouble format: ._prefix sidecar files (macOS native)
    AppleDouble,
    /// MacBinary III: single .bin file containing both forks
    MacBinary,
    /// Separate .rsrc sidecar files
    SeparateRsrc,
    /// Skip resource forks entirely
    DataForkOnly,
}

impl ResourceForkMode {
    pub fn label(&self) -> &'static str {
        match self {
            ResourceForkMode::AppleDouble => "AppleDouble (._file)",
            ResourceForkMode::MacBinary => "MacBinary III (.bin)",
            ResourceForkMode::SeparateRsrc => "Separate (.rsrc)",
            ResourceForkMode::DataForkOnly => "Data fork only",
        }
    }

    pub const ALL: [ResourceForkMode; 4] = [
        ResourceForkMode::AppleDouble,
        ResourceForkMode::MacBinary,
        ResourceForkMode::SeparateRsrc,
        ResourceForkMode::DataForkOnly,
    ];
}

/// Build an AppleDouble (version 2) sidecar file.
///
/// Contains Finder info (type/creator codes) and resource fork data.
/// Format: magic(4) + version(4) + filler(16) + num_entries(2) + entries...
pub fn build_appledouble(type_code: &[u8; 4], creator_code: &[u8; 4], rsrc_data: &[u8]) -> Vec<u8> {
    // AppleDouble header: 26 bytes
    // Entry 1: Finder Info (id=9), 32 bytes
    // Entry 2: Resource Fork (id=2), variable
    let finder_offset: u32 = 26 + 2 * 12; // header + 2 entry descriptors
    let finder_len: u32 = 32;
    let rsrc_offset: u32 = finder_offset + finder_len;
    let rsrc_len: u32 = rsrc_data.len() as u32;

    let total = rsrc_offset as usize + rsrc_data.len();
    let mut buf = vec![0u8; total];

    // Magic number
    BigEndian::write_u32(&mut buf[0..4], 0x00051607);
    // Version 2
    BigEndian::write_u32(&mut buf[4..8], 0x00020000);
    // Filler: 16 bytes of zeros (already zero)
    // Number of entries
    BigEndian::write_u16(&mut buf[24..26], 2);

    // Entry 1 descriptor: Finder Info
    BigEndian::write_u32(&mut buf[26..30], 9); // entry ID = Finder Info
    BigEndian::write_u32(&mut buf[30..34], finder_offset);
    BigEndian::write_u32(&mut buf[34..38], finder_len);

    // Entry 2 descriptor: Resource Fork
    BigEndian::write_u32(&mut buf[38..42], 2); // entry ID = Resource Fork
    BigEndian::write_u32(&mut buf[42..46], rsrc_offset);
    BigEndian::write_u32(&mut buf[46..50], rsrc_len);

    // Finder Info data (32 bytes): fdType(4) + fdCreator(4) + fdFlags(2) + fdLocation(4) + fdFldr(2) + extended(16)
    let fi_start = finder_offset as usize;
    buf[fi_start..fi_start + 4].copy_from_slice(type_code);
    buf[fi_start + 4..fi_start + 8].copy_from_slice(creator_code);
    // Rest is zeros

    // Resource fork data
    buf[rsrc_offset as usize..].copy_from_slice(rsrc_data);

    buf
}

/// Build a MacBinary III file containing both data and resource forks.
///
/// Format: 128-byte header + data fork (padded to 128) + resource fork (padded to 128).
/// The .bin file replaces the separate data fork file.
pub fn build_macbinary(
    filename: &str,
    type_code: &[u8; 4],
    creator_code: &[u8; 4],
    data_fork: &[u8],
    rsrc_data: &[u8],
) -> Vec<u8> {
    let data_padded = pad_to_128(data_fork.len());
    let rsrc_padded = pad_to_128(rsrc_data.len());
    let total = 128 + data_padded + rsrc_padded;
    let mut buf = vec![0u8; total];

    // Header byte 0: always 0
    // Header byte 1: filename length (max 63)
    let name_bytes = filename.as_bytes();
    let name_len = name_bytes.len().min(63);
    buf[1] = name_len as u8;
    buf[2..2 + name_len].copy_from_slice(&name_bytes[..name_len]);

    // Type code at 65-68, creator at 69-72
    buf[65..69].copy_from_slice(type_code);
    buf[69..73].copy_from_slice(creator_code);

    // Data fork length at 83-86 (big-endian)
    BigEndian::write_u32(&mut buf[83..87], data_fork.len() as u32);
    // Resource fork length at 87-90
    BigEndian::write_u32(&mut buf[87..91], rsrc_data.len() as u32);

    // MacBinary version: byte 122 = 130 (MacBinary III)
    buf[122] = 130;
    // Minimum version: byte 123 = 129 (MacBinary II)
    buf[123] = 129;

    // CRC-16 of header bytes 0-123 at bytes 124-125
    let crc = macbinary_crc16(&buf[0..124]);
    BigEndian::write_u16(&mut buf[124..126], crc);

    // Data fork
    buf[128..128 + data_fork.len()].copy_from_slice(data_fork);
    // Resource fork
    let rsrc_start = 128 + data_padded;
    buf[rsrc_start..rsrc_start + rsrc_data.len()].copy_from_slice(rsrc_data);

    buf
}

/// Sanitize a filename for the host OS.
/// Replaces characters that are invalid on common filesystems.
pub fn sanitize_filename(name: &str) -> String {
    name.chars()
        .map(|c| match c {
            ':' | '/' | '\\' | '\0' => '_',
            '<' | '>' | '"' | '|' | '?' | '*' => '_',
            _ => c,
        })
        .collect()
}

/// Round up to the next multiple of 128.
fn pad_to_128(len: usize) -> usize {
    (len + 127) & !127
}

/// CRC-16 used by MacBinary (CRC-CCITT with polynomial 0x1021).
fn macbinary_crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        crc ^= (byte as u16) << 8;
        for _ in 0..8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_filename() {
        assert_eq!(sanitize_filename("hello:world"), "hello_world");
        assert_eq!(sanitize_filename("file/name"), "file_name");
        assert_eq!(sanitize_filename("normal.txt"), "normal.txt");
        assert_eq!(sanitize_filename("a<b>c"), "a_b_c");
    }

    #[test]
    fn test_build_appledouble_header() {
        let ad = build_appledouble(b"TEXT", b"ttxt", &[0xDE, 0xAD]);
        // Magic
        assert_eq!(BigEndian::read_u32(&ad[0..4]), 0x00051607);
        // Version 2
        assert_eq!(BigEndian::read_u32(&ad[4..8]), 0x00020000);
        // 2 entries
        assert_eq!(BigEndian::read_u16(&ad[24..26]), 2);
        // Finder info entry has type/creator
        let fi_offset = BigEndian::read_u32(&ad[30..34]) as usize;
        assert_eq!(&ad[fi_offset..fi_offset + 4], b"TEXT");
        assert_eq!(&ad[fi_offset + 4..fi_offset + 8], b"ttxt");
        // Resource fork data at end
        let rsrc_offset = BigEndian::read_u32(&ad[42..46]) as usize;
        let rsrc_len = BigEndian::read_u32(&ad[46..50]) as usize;
        assert_eq!(rsrc_len, 2);
        assert_eq!(&ad[rsrc_offset..rsrc_offset + 2], &[0xDE, 0xAD]);
    }

    #[test]
    fn test_build_macbinary() {
        let mb = build_macbinary("test.txt", b"TEXT", b"ttxt", b"hello", &[1, 2, 3]);
        // Filename length
        assert_eq!(mb[1], 8);
        assert_eq!(&mb[2..10], b"test.txt");
        // Type/creator
        assert_eq!(&mb[65..69], b"TEXT");
        assert_eq!(&mb[69..73], b"ttxt");
        // Data fork length
        assert_eq!(BigEndian::read_u32(&mb[83..87]), 5);
        // Resource fork length
        assert_eq!(BigEndian::read_u32(&mb[87..91]), 3);
        // MacBinary III version
        assert_eq!(mb[122], 130);
        // Data fork content
        assert_eq!(&mb[128..133], b"hello");
        // Resource fork content (after data padded to 128)
        assert_eq!(&mb[256..259], &[1, 2, 3]);
    }

    #[test]
    fn test_macbinary_crc() {
        // Known test: CRC of all zeros should be 0
        let zeros = [0u8; 124];
        assert_eq!(macbinary_crc16(&zeros), 0);
    }

    #[test]
    fn test_pad_to_128() {
        assert_eq!(pad_to_128(0), 0);
        assert_eq!(pad_to_128(1), 128);
        assert_eq!(pad_to_128(128), 128);
        assert_eq!(pad_to_128(129), 256);
    }

    #[test]
    fn test_resource_fork_mode_labels() {
        assert_eq!(
            ResourceForkMode::AppleDouble.label(),
            "AppleDouble (._file)"
        );
        assert_eq!(ResourceForkMode::DataForkOnly.label(), "Data fork only");
    }
}
