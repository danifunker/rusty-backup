//! Resource fork serialization for HFS/HFS+ file extraction.
//!
//! Supports AppleDouble (._prefix sidecar), MacBinary III (.bin wrapper),
//! and separate .rsrc sidecar files.

use byteorder::{BigEndian, ByteOrder};

/// How to handle resource forks during extraction.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ResourceForkMode {
    /// Write resource fork directly to the file's native resource fork
    /// (macOS only — uses `filename/..namedfork/rsrc`).
    Native,
    /// AppleDouble format: ._prefix sidecar files
    AppleDouble,
    /// MacBinary III: single .bin file containing both forks
    MacBinary,
    /// BinHex 4.0: single .hqx text file containing both forks + Finder info
    BinHex,
    /// Separate .rsrc sidecar files
    SeparateRsrc,
    /// Skip resource forks entirely
    DataForkOnly,
}

impl ResourceForkMode {
    pub fn label(&self) -> &'static str {
        match self {
            ResourceForkMode::Native => "Native resource fork",
            ResourceForkMode::AppleDouble => "AppleDouble (._file)",
            ResourceForkMode::MacBinary => "MacBinary III (.bin)",
            ResourceForkMode::BinHex => "BinHex 4.0 (.hqx)",
            ResourceForkMode::SeparateRsrc => "Separate (.rsrc)",
            ResourceForkMode::DataForkOnly => "Data fork only",
        }
    }

    pub const ALL: [ResourceForkMode; 6] = [
        ResourceForkMode::Native,
        ResourceForkMode::AppleDouble,
        ResourceForkMode::MacBinary,
        ResourceForkMode::BinHex,
        ResourceForkMode::SeparateRsrc,
        ResourceForkMode::DataForkOnly,
    ];
}

/// Mac file dates as raw seconds since the classic Mac epoch (1904-01-01) —
/// the exact encoding MacBinary stores in its header and HFS/HFS+ record on
/// disk. A field of `0` means "unknown": MacBinary writes a zero date and
/// AppleDouble omits the File Dates Info entry, so callers with no dates get
/// byte-identical output to the pre-dates behavior.
#[derive(Debug, Clone, Copy, Default)]
pub struct MacFileDates {
    /// Creation time, seconds since 1904-01-01.
    pub created: u32,
    /// Modification time, seconds since 1904-01-01.
    pub modified: u32,
}

/// Seconds between the classic Mac epoch (1904-01-01) and the Unix epoch.
const MAC_EPOCH_UNIX_OFFSET: i64 = 2_082_844_800;
/// Unix seconds at the AppleDouble "File Dates Info" epoch (2000-01-01 GMT).
const APPLEDOUBLE_EPOCH_UNIX: i64 = 946_684_800;
/// Apple's "unknown date" sentinel for AppleSingle/AppleDouble date fields.
const APPLEDOUBLE_DATE_UNKNOWN: i32 = i32::MIN;

/// Convert a Mac-epoch (1904) second count to the AppleDouble "File Dates Info"
/// encoding: signed seconds relative to 2000-01-01 GMT. `0` (unknown) maps to
/// Apple's `0x80000000` sentinel; out-of-range values clamp into `i32`.
fn mac1904_to_appledouble(mac_secs: u32) -> i32 {
    if mac_secs == 0 {
        return APPLEDOUBLE_DATE_UNKNOWN;
    }
    let secs_2000 = mac_secs as i64 - MAC_EPOCH_UNIX_OFFSET - APPLEDOUBLE_EPOCH_UNIX;
    // Keep off i32::MIN so a real date can't collide with the "unknown" sentinel.
    secs_2000.clamp(i32::MIN as i64 + 1, i32::MAX as i64) as i32
}

/// Build an AppleDouble (version 2) sidecar file.
///
/// Contains Finder info (type/creator codes), file dates (when known), and
/// resource fork data.
/// Format: magic(4) + version(4) + filler(16) + num_entries(2) + entries...
pub fn build_appledouble(
    type_code: &[u8; 4],
    creator_code: &[u8; 4],
    dates: MacFileDates,
    rsrc_data: &[u8],
) -> Vec<u8> {
    // Entries, in on-disk order: Finder Info (id 9, 32B), then — only when we
    // have real dates — File Dates Info (id 8, 16B), then Resource Fork
    // (id 2, variable). Omitting the dates entry when unknown keeps the output
    // byte-identical to the pre-dates two-entry layout.
    let has_dates = dates.created != 0 || dates.modified != 0;
    let num_entries: u16 = if has_dates { 3 } else { 2 };
    let header_len = 26 + num_entries as usize * 12; // 26-byte header + descriptors

    let finder_offset = header_len as u32;
    let finder_len: u32 = 32;
    let dates_offset = finder_offset + finder_len; // meaningful only when has_dates
    let dates_len: u32 = 16;
    let rsrc_offset = if has_dates {
        dates_offset + dates_len
    } else {
        finder_offset + finder_len
    };
    let rsrc_len: u32 = rsrc_data.len() as u32;

    let total = rsrc_offset as usize + rsrc_data.len();
    let mut buf = vec![0u8; total];

    // Magic number
    BigEndian::write_u32(&mut buf[0..4], 0x00051607);
    // Version 2
    BigEndian::write_u32(&mut buf[4..8], 0x00020000);
    // Filler: 16 bytes of zeros (already zero)
    // Number of entries
    BigEndian::write_u16(&mut buf[24..26], num_entries);

    // Entry descriptors: 12 bytes each (id, offset, length), starting at 26.
    let mut d = 26;
    let mut write_desc = |buf: &mut [u8], id: u32, off: u32, len: u32| {
        BigEndian::write_u32(&mut buf[d..d + 4], id);
        BigEndian::write_u32(&mut buf[d + 4..d + 8], off);
        BigEndian::write_u32(&mut buf[d + 8..d + 12], len);
        d += 12;
    };
    write_desc(&mut buf, 9, finder_offset, finder_len); // Finder Info
    if has_dates {
        write_desc(&mut buf, 8, dates_offset, dates_len); // File Dates Info
    }
    write_desc(&mut buf, 2, rsrc_offset, rsrc_len); // Resource Fork

    // Finder Info data (32 bytes): fdType(4) + fdCreator(4) + fdFlags(2) + fdLocation(4) + fdFldr(2) + extended(16)
    let fi_start = finder_offset as usize;
    buf[fi_start..fi_start + 4].copy_from_slice(type_code);
    buf[fi_start + 4..fi_start + 8].copy_from_slice(creator_code);
    // Rest is zeros

    // File Dates Info (16 bytes): create/modify/backup/access, signed seconds
    // relative to 2000-01-01 GMT. We only know create/modify; the rest are the
    // "unknown" sentinel.
    if has_dates {
        let di = dates_offset as usize;
        BigEndian::write_i32(&mut buf[di..di + 4], mac1904_to_appledouble(dates.created));
        BigEndian::write_i32(
            &mut buf[di + 4..di + 8],
            mac1904_to_appledouble(dates.modified),
        );
        BigEndian::write_i32(&mut buf[di + 8..di + 12], APPLEDOUBLE_DATE_UNKNOWN);
        BigEndian::write_i32(&mut buf[di + 12..di + 16], APPLEDOUBLE_DATE_UNKNOWN);
    }

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
    dates: MacFileDates,
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

    // Creation date at 91-94, modification date at 95-98: u32 secs since 1904,
    // exactly what HFS/HFS+ record — so the raw on-disc timestamps drop in with
    // no conversion. Both stay zero ("unknown") when `dates` is defaulted. These
    // sit inside the CRC'd header region, so they must be written before the CRC.
    BigEndian::write_u32(&mut buf[91..95], dates.created);
    BigEndian::write_u32(&mut buf[95..99], dates.modified);

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

/// CRC-16 used by MacBinary (CRC-16/XMODEM: CCITT polynomial 0x1021, init 0,
/// no reflection, no final XOR). Computed over header bytes 0..124. Shared
/// with `macarchive::macbinary`, the canonical full-fidelity parser.
pub fn macbinary_crc16(data: &[u8]) -> u16 {
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

/// Metadata extracted from a resource fork container (AppleDouble, MacBinary, etc.).
#[derive(Debug, Clone)]
pub struct ImportedResourceFork {
    /// Raw resource fork bytes.
    pub data: Vec<u8>,
    /// The data fork bytes (only set for MacBinary, where the container wraps both forks).
    pub data_fork: Option<Vec<u8>>,
    /// 4-byte type code (e.g. `TEXT`), if found in the container.
    pub type_code: Option<[u8; 4]>,
    /// 4-byte creator code (e.g. `ttxt`), if found in the container.
    pub creator_code: Option<[u8; 4]>,
}

/// Detect and read a resource fork associated with `host_path` by probing
/// all supported formats in priority order:
///
/// 1. **Native** (macOS only): `host_path/..namedfork/rsrc`
/// 2. **AppleDouble**: `._<filename>` sidecar next to the file
/// 3. **MacBinary**: the file itself is a `.bin` MacBinary container
/// 4. **Separate .rsrc**: `<filename>.rsrc` sidecar next to the file
///
/// Returns `None` if no resource fork is found.
pub fn detect_resource_fork(host_path: &std::path::Path) -> Option<ImportedResourceFork> {
    // 1. Native macOS resource fork
    #[cfg(target_os = "macos")]
    {
        let rsrc_path = host_path.join("..namedfork/rsrc");
        if let Ok(data) = std::fs::read(&rsrc_path) {
            if !data.is_empty() {
                let (type_code, creator_code) = read_finder_info_xattr(host_path);
                return Some(ImportedResourceFork {
                    data,
                    data_fork: None,
                    type_code,
                    creator_code,
                });
            }
        }
    }

    let file_name = host_path.file_name()?.to_str()?;
    let parent = host_path.parent()?;

    // 2. AppleDouble sidecar
    let ad_path = parent.join(format!("._{file_name}"));
    if ad_path.is_file() {
        if let Ok(ad_data) = std::fs::read(&ad_path) {
            if let Some(parsed) = parse_appledouble(&ad_data) {
                return Some(parsed);
            }
        }
    }

    // 3. MacBinary — check if the file itself is a MacBinary container
    if let Ok(mb_data) = std::fs::read(host_path) {
        if let Some(parsed) = parse_macbinary(&mb_data) {
            return Some(parsed);
        }
    }

    // 4. Separate .rsrc sidecar
    let rsrc_path = parent.join(format!("{file_name}.rsrc"));
    if rsrc_path.is_file() {
        if let Ok(data) = std::fs::read(&rsrc_path) {
            if !data.is_empty() {
                return Some(ImportedResourceFork {
                    data,
                    data_fork: None,
                    type_code: None,
                    creator_code: None,
                });
            }
        }
    }

    None
}

/// Check if a host file is a resource fork sidecar that should be skipped
/// during import (because it will be consumed as part of its primary file).
pub fn is_resource_fork_sidecar(host_path: &std::path::Path) -> bool {
    let file_name = match host_path.file_name().and_then(|n| n.to_str()) {
        Some(n) => n,
        None => return false,
    };
    let parent = match host_path.parent() {
        Some(p) => p,
        None => return false,
    };

    // AppleDouble sidecar: ._<name> where <name> exists as a regular file
    if let Some(primary) = file_name.strip_prefix("._") {
        if parent.join(primary).is_file() {
            return true;
        }
    }

    // Separate .rsrc sidecar: <name>.rsrc where <name> exists as a regular file
    if let Some(stem) = file_name.strip_suffix(".rsrc") {
        if !stem.is_empty() && parent.join(stem).is_file() {
            return true;
        }
    }

    false
}

/// Parse an AppleDouble file and extract the resource fork and Finder info.
///
/// AppleDouble layout:
/// - `[0..4]` magic `0x00051607`
/// - `[4..8]` version `0x00020000`
/// - `[8..24]` filler (16 bytes)
/// - `[24..26]` number of entries (u16 BE)
/// - For each entry: `[+0..4]` entry ID, `[+4..8]` offset, `[+8..12]` length
///   - Entry ID 2 = resource fork
///   - Entry ID 9 = Finder info (type/creator at bytes 0-3 / 4-7)
pub fn parse_appledouble(data: &[u8]) -> Option<ImportedResourceFork> {
    if data.len() < 26 {
        return None;
    }
    // Check magic
    if BigEndian::read_u32(&data[0..4]) != 0x00051607 {
        return None;
    }

    let num_entries = BigEndian::read_u16(&data[24..26]) as usize;
    let mut rsrc_data: Option<Vec<u8>> = None;
    let mut type_code: Option<[u8; 4]> = None;
    let mut creator_code: Option<[u8; 4]> = None;

    for i in 0..num_entries {
        let desc_off = 26 + i * 12;
        if desc_off + 12 > data.len() {
            break;
        }
        let entry_id = BigEndian::read_u32(&data[desc_off..desc_off + 4]);
        let offset = BigEndian::read_u32(&data[desc_off + 4..desc_off + 8]) as usize;
        let length = BigEndian::read_u32(&data[desc_off + 8..desc_off + 12]) as usize;

        if offset + length > data.len() {
            continue;
        }

        match entry_id {
            2 => {
                // Resource fork
                rsrc_data = Some(data[offset..offset + length].to_vec());
            }
            9
                // Finder info — type at +0, creator at +4
                if length >= 8 => {
                    let mut tc = [0u8; 4];
                    let mut cc = [0u8; 4];
                    tc.copy_from_slice(&data[offset..offset + 4]);
                    cc.copy_from_slice(&data[offset + 4..offset + 8]);
                    if tc != [0; 4] {
                        type_code = Some(tc);
                    }
                    if cc != [0; 4] {
                        creator_code = Some(cc);
                    }
                }
            _ => {}
        }
    }

    // Accept the sidecar if it carries either a resource fork OR Finder info.
    // Files with type/creator but no rsrc (e.g. plain text, bookmark files)
    // still need to round-trip their FInfo.
    let rsrc = rsrc_data.unwrap_or_default();
    if rsrc.is_empty() && type_code.is_none() && creator_code.is_none() {
        return None;
    }

    Some(ImportedResourceFork {
        data: rsrc,
        data_fork: None,
        type_code,
        creator_code,
    })
}

/// Parse a MacBinary (II/III) file and extract both forks and Finder info.
///
/// MacBinary header (128 bytes):
/// - `[0]` must be 0
/// - `[1]` filename length (1-63)
/// - `[65..69]` type code
/// - `[69..73]` creator code
/// - `[83..87]` data fork length (u32 BE)
/// - `[87..91]` resource fork length (u32 BE)
/// - `[122]` MacBinary version (129=II, 130=III)
/// - `[124..126]` CRC-16 of bytes 0-123 (MacBinary II+)
///
/// Returns `None` if the file doesn't look like a valid MacBinary container.
pub fn parse_macbinary(data: &[u8]) -> Option<ImportedResourceFork> {
    if data.len() < 128 {
        return None;
    }
    // Byte 0 must be 0
    if data[0] != 0 {
        return None;
    }
    // Filename length must be 1-63
    let name_len = data[1] as usize;
    if name_len == 0 || name_len > 63 {
        return None;
    }
    // Check version byte — must be MacBinary II (129) or III (130)
    let version = data[122];
    if version != 129 && version != 130 {
        return None;
    }
    // Verify CRC
    let stored_crc = BigEndian::read_u16(&data[124..126]);
    let computed_crc = macbinary_crc16(&data[0..124]);
    if stored_crc != computed_crc {
        return None;
    }

    let type_code: [u8; 4] = data[65..69].try_into().unwrap();
    let creator_code: [u8; 4] = data[69..73].try_into().unwrap();

    let data_len = BigEndian::read_u32(&data[83..87]) as usize;
    let rsrc_len = BigEndian::read_u32(&data[87..91]) as usize;

    let data_start = 128;
    let data_padded = pad_to_128(data_len);
    let rsrc_start = data_start + data_padded;

    if rsrc_start + rsrc_len > data.len() || data_start + data_len > data.len() {
        return None;
    }

    if rsrc_len == 0 {
        return None;
    }

    Some(ImportedResourceFork {
        data: data[rsrc_start..rsrc_start + rsrc_len].to_vec(),
        data_fork: Some(data[data_start..data_start + data_len].to_vec()),
        type_code: if type_code != [0; 4] {
            Some(type_code)
        } else {
            None
        },
        creator_code: if creator_code != [0; 4] {
            Some(creator_code)
        } else {
            None
        },
    })
}

/// Read the macOS `com.apple.FinderInfo` extended attribute and extract
/// the 4-byte type and creator codes. Returns `(None, None)` if the xattr
/// is missing, too short, or both codes are zero.
#[cfg(target_os = "macos")]
fn read_finder_info_xattr(path: &std::path::Path) -> (Option<[u8; 4]>, Option<[u8; 4]>) {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = match CString::new(path.as_os_str().as_bytes()) {
        Ok(p) => p,
        Err(_) => return (None, None),
    };
    let c_name = match CString::new("com.apple.FinderInfo") {
        Ok(n) => n,
        Err(_) => return (None, None),
    };

    let mut buf = [0u8; 32];
    // getxattr(path, name, value, size, position, options)
    let n = unsafe {
        libc::getxattr(
            c_path.as_ptr(),
            c_name.as_ptr(),
            buf.as_mut_ptr() as *mut libc::c_void,
            buf.len(),
            0,
            0,
        )
    };
    if n < 8 {
        return (None, None);
    }

    let mut tc = [0u8; 4];
    let mut cc = [0u8; 4];
    tc.copy_from_slice(&buf[0..4]);
    cc.copy_from_slice(&buf[4..8]);

    let tc = if tc != [0; 4] { Some(tc) } else { None };
    let cc = if cc != [0; 4] { Some(cc) } else { None };
    (tc, cc)
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
        let ad = build_appledouble(b"TEXT", b"ttxt", MacFileDates::default(), &[0xDE, 0xAD]);
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
        let mb = build_macbinary(
            "test.txt",
            b"TEXT",
            b"ttxt",
            MacFileDates::default(),
            b"hello",
            &[1, 2, 3],
        );
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

    #[test]
    fn test_parse_appledouble_roundtrip() {
        let rsrc = vec![0xCA, 0xFE, 0xBA, 0xBE];
        let ad = build_appledouble(b"TEXT", b"ttxt", MacFileDates::default(), &rsrc);
        let parsed = parse_appledouble(&ad).expect("should parse");
        assert_eq!(parsed.data, rsrc);
        assert_eq!(parsed.type_code, Some(*b"TEXT"));
        assert_eq!(parsed.creator_code, Some(*b"ttxt"));
        assert!(parsed.data_fork.is_none());
    }

    #[test]
    fn test_parse_appledouble_bad_magic() {
        assert!(parse_appledouble(&[0; 30]).is_none());
    }

    #[test]
    fn test_parse_macbinary_roundtrip() {
        let data_fork = b"hello world";
        let rsrc = vec![1, 2, 3, 4, 5];
        let mb = build_macbinary(
            "test.txt",
            b"TEXT",
            b"ttxt",
            MacFileDates::default(),
            data_fork,
            &rsrc,
        );
        let parsed = parse_macbinary(&mb).expect("should parse");
        assert_eq!(parsed.data, rsrc);
        assert_eq!(parsed.data_fork.as_deref(), Some(data_fork.as_slice()));
        assert_eq!(parsed.type_code, Some(*b"TEXT"));
        assert_eq!(parsed.creator_code, Some(*b"ttxt"));
    }

    #[test]
    fn test_parse_macbinary_bad_header() {
        // Too short
        assert!(parse_macbinary(&[0; 50]).is_none());
        // Wrong byte 0
        let mut bad = vec![0; 256];
        bad[0] = 1;
        assert!(parse_macbinary(&bad).is_none());
    }

    #[test]
    fn test_parse_appledouble_finfo_only() {
        // Sidecar built for a file with type/creator but no resource fork
        // (e.g. a classic Mac bookmark or .txt file). Must round-trip.
        let ad = build_appledouble(b"MOSS", b"sigl", MacFileDates::default(), &[]);
        let parsed = parse_appledouble(&ad).expect("FInfo-only sidecar should parse");
        assert!(parsed.data.is_empty());
        assert_eq!(parsed.type_code, Some(*b"MOSS"));
        assert_eq!(parsed.creator_code, Some(*b"sigl"));
    }

    #[test]
    fn test_parse_appledouble_empty_finder_info_rejected() {
        // Sidecar with no rsrc and no FInfo carries no information.
        let ad = build_appledouble(&[0; 4], &[0; 4], MacFileDates::default(), &[]);
        assert!(parse_appledouble(&ad).is_none());
    }

    #[test]
    fn test_parse_macbinary_no_rsrc() {
        // Build a MacBinary with no resource fork
        let mb = build_macbinary(
            "test.txt",
            b"TEXT",
            b"ttxt",
            MacFileDates::default(),
            b"data",
            &[],
        );
        assert!(parse_macbinary(&mb).is_none());
    }

    #[test]
    fn test_build_macbinary_dates() {
        let dates = MacFileDates {
            created: 0x1234_5678,
            modified: 0x2345_6789,
        };
        let mb = build_macbinary("f", b"TEXT", b"ttxt", dates, b"x", &[9]);
        // Raw Mac-1904 seconds land at header offsets 91 (create) and 95 (modify).
        assert_eq!(BigEndian::read_u32(&mb[91..95]), 0x1234_5678);
        assert_eq!(BigEndian::read_u32(&mb[95..99]), 0x2345_6789);
        // Dates are inside the CRC'd header region, so the container still parses.
        assert!(parse_macbinary(&mb).is_some());
    }

    #[test]
    fn test_build_appledouble_dates() {
        // `created` maps to 100s after the 2000 epoch; `modified` stays unknown.
        let created = (MAC_EPOCH_UNIX_OFFSET + APPLEDOUBLE_EPOCH_UNIX + 100) as u32;
        let dates = MacFileDates {
            created,
            modified: 0,
        };
        let ad = build_appledouble(b"TEXT", b"ttxt", dates, &[0xAA]);
        // Real dates add a third entry (Finder Info, File Dates Info, Resource Fork).
        assert_eq!(BigEndian::read_u16(&ad[24..26]), 3);
        // Second descriptor is the File Dates Info entry (id 8).
        assert_eq!(BigEndian::read_u32(&ad[38..42]), 8);
        let dates_off = BigEndian::read_u32(&ad[42..46]) as usize;
        assert_eq!(BigEndian::read_i32(&ad[dates_off..dates_off + 4]), 100);
        // Unknown modify/backup/access all become the sentinel.
        assert_eq!(
            BigEndian::read_i32(&ad[dates_off + 4..dates_off + 8]),
            i32::MIN
        );
        // Finder info + resource fork still round-trip through the parser.
        let parsed = parse_appledouble(&ad).expect("parse");
        assert_eq!(parsed.type_code, Some(*b"TEXT"));
        assert_eq!(parsed.data, vec![0xAA]);
    }

    #[test]
    fn test_build_appledouble_no_dates_layout_unchanged() {
        // Default (unknown) dates keep the classic two-entry layout so callers
        // without timestamps emit byte-identical sidecars.
        let ad = build_appledouble(b"TEXT", b"ttxt", MacFileDates::default(), &[0xAA]);
        assert_eq!(BigEndian::read_u16(&ad[24..26]), 2);
        // Second descriptor is the resource fork (id 2), not a dates entry.
        assert_eq!(BigEndian::read_u32(&ad[38..42]), 2);
    }
}
