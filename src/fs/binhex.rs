//! BinHex 4.0 (`.hqx`) encode + decode.
//!
//! BinHex 4.0 is the classic Macintosh 7-bit-safe encoding that preserves a
//! file's **data fork, resource fork, and Finder info** (type/creator/flags) in
//! a single ASCII text file. Layout of the decoded byte stream:
//!
//! ```text
//! nameLen(1) name(nameLen) version(1=0) type(4) creator(4) flags(2)
//! dataLen(4) rsrcLen(4) headerCRC(2)
//! <data fork bytes> dataCRC(2)
//! <resource fork bytes> rsrcCRC(2)
//! ```
//!
//! That whole byte stream is RLE90-compressed (`0x90` run marker) and then
//! 6-bit encoded into the BinHex alphabet, wrapped between `:` markers under the
//! `(This file must be converted with BinHex 4.0)` banner.
//!
//! CRCs are CRC-16-CCITT (poly `0x1021`, init 0) in **augmented** form: the
//! running CRC is finalized by feeding two trailing zero bytes. This differs
//! from the MacBinary CRC in `resource_fork.rs` (which stores the CRC directly).
//! Reference: hfsutils `src/common/crc.c` `crc_binh` + `src/binhex/binhex.c`,
//! and XADMaster `XADBinHexParser.m`.

use anyhow::{bail, Result};
use byteorder::{BigEndian, ByteOrder};

/// The 64-character BinHex 4.0 alphabet (note the gaps: no `7 O W g n o`).
const ALPHABET: &[u8; 64] = b"!\"#$%&'()*+,-012345689@ABCDEFGHIJKLMNPQRSTUVXYZ[`abcdefhijklmpqr";

const BANNER: &str = "(This file must be converted with BinHex 4.0)";

/// A file decoded from / to be encoded into BinHex 4.0.
#[derive(Debug, Clone, PartialEq)]
pub struct BinHexFile {
    /// Macintosh filename (≤ 63 bytes).
    pub name: String,
    /// 4-byte Finder type code (e.g. `TEXT`).
    pub type_code: [u8; 4],
    /// 4-byte Finder creator code (e.g. `ttxt`).
    pub creator_code: [u8; 4],
    /// 16-bit Finder flags.
    pub flags: u16,
    /// Data fork bytes.
    pub data_fork: Vec<u8>,
    /// Resource fork bytes.
    pub resource_fork: Vec<u8>,
}

impl BinHexFile {
    /// True when the decoded payload looks like a wrapped archive/disk image
    /// (by filename extension) — useful for the auto-unwrap dispatch.
    pub fn looks_like_archive(&self) -> bool {
        let lower = self.name.to_ascii_lowercase();
        [
            ".sit", ".sitx", ".cpt", ".sea", ".bin", ".img", ".image", ".dsk",
        ]
        .iter()
        .any(|ext| lower.ends_with(ext))
    }
}

/// BinHex CRC-16-CCITT (augmented: data followed by two zero bytes).
fn binhex_crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    let mut feed = |byte: u8| {
        for i in (0..8).rev() {
            let bit = ((byte >> i) & 1) as u16;
            let msb = (crc >> 15) & 1;
            crc = (crc << 1) | bit;
            if msb == 1 {
                crc ^= 0x1021;
            }
        }
    };
    for &b in data {
        feed(b);
    }
    // Augment: finalize by clocking two zero bytes through the register.
    feed(0);
    feed(0);
    crc
}

/// RLE90 compress: a run of `b` repeated `n` (3..=255) times becomes
/// `b 0x90 n`; a literal `0x90` becomes `0x90 0x00`. Shared with the StuffIt
/// writer (compression method 1).
pub(crate) fn rle90_encode(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len());
    let mut i = 0;
    while i < data.len() {
        let b = data[i];
        let mut run = 1usize;
        while i + run < data.len() && data[i + run] == b && run < 255 {
            run += 1;
        }
        if b == 0x90 {
            // Emit one literal 0x90, then run-length the rest if needed.
            out.push(0x90);
            out.push(0x00);
            if run > 1 {
                out.push(0x90);
                out.push(run as u8);
            }
        } else if run >= 3 {
            out.push(b);
            out.push(0x90);
            out.push(run as u8);
        } else {
            for _ in 0..run {
                out.push(b);
            }
        }
        i += run;
    }
    out
}

/// RLE90 decompress (inverse of [`rle90_encode`]). Shared with the StuffIt
/// reader, which uses the same 0x90 run encoding for compression method 1.
pub(crate) fn rle90_decode(input: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(input.len() * 2);
    let mut i = 0;
    while i < input.len() {
        let b = input[i];
        i += 1;
        if b != 0x90 {
            out.push(b);
            continue;
        }
        if i >= input.len() {
            break; // dangling marker (padding); stop
        }
        let count = input[i];
        i += 1;
        if count == 0 {
            out.push(0x90);
        } else {
            let last = *out.last().unwrap_or(&0);
            for _ in 0..count.saturating_sub(1) {
                out.push(last);
            }
        }
    }
    out
}

/// 6-bit encode into the BinHex alphabet (no line wrapping).
fn sixbit_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len() * 4 / 3 + 4);
    let mut acc: u32 = 0;
    let mut bits = 0u32;
    for &b in data {
        acc = (acc << 8) | b as u32;
        bits += 8;
        while bits >= 6 {
            bits -= 6;
            out.push(ALPHABET[((acc >> bits) & 0x3f) as usize] as char);
        }
    }
    if bits > 0 {
        let idx = ((acc << (6 - bits)) & 0x3f) as usize;
        out.push(ALPHABET[idx] as char);
    }
    out
}

/// Encode a [`BinHexFile`] into a complete `.hqx` text document.
pub fn build_binhex(file: &BinHexFile) -> String {
    let name_bytes = file.name.as_bytes();
    let name_len = name_bytes.len().min(63);

    // Header: nameLen, name, version(0), type, creator, flags, dataLen, rsrcLen.
    let mut header = Vec::with_capacity(name_len + 20);
    header.push(name_len as u8);
    header.extend_from_slice(&name_bytes[..name_len]);
    header.push(0); // version
    header.extend_from_slice(&file.type_code);
    header.extend_from_slice(&file.creator_code);
    let mut tmp = [0u8; 4];
    BigEndian::write_u16(&mut tmp[..2], file.flags);
    header.extend_from_slice(&tmp[..2]);
    BigEndian::write_u32(&mut tmp, file.data_fork.len() as u32);
    header.extend_from_slice(&tmp);
    BigEndian::write_u32(&mut tmp, file.resource_fork.len() as u32);
    header.extend_from_slice(&tmp);

    // Assemble the full pre-RLE byte stream with all three CRCs.
    let mut raw = header.clone();
    push_u16(&mut raw, binhex_crc16(&header));
    raw.extend_from_slice(&file.data_fork);
    push_u16(&mut raw, binhex_crc16(&file.data_fork));
    raw.extend_from_slice(&file.resource_fork);
    push_u16(&mut raw, binhex_crc16(&file.resource_fork));

    let encoded = sixbit_encode(&rle90_encode(&raw));

    // Wrap: banner, blank line, ':' + 64-char lines + ':'.
    let mut out = String::with_capacity(encoded.len() + encoded.len() / 64 + BANNER.len() + 8);
    out.push_str(BANNER);
    out.push('\n');
    out.push('\n');
    out.push(':');
    for (i, ch) in encoded.chars().enumerate() {
        if i > 0 && i % 64 == 0 {
            out.push('\n');
        }
        out.push(ch);
    }
    out.push(':');
    out.push('\n');
    out
}

/// Decode a `.hqx` document into a [`BinHexFile`], verifying all CRCs.
pub fn parse_binhex(input: &[u8]) -> Result<BinHexFile> {
    // Inverse alphabet table (-1 == not a data character).
    let mut inv = [-1i8; 256];
    for (i, &c) in ALPHABET.iter().enumerate() {
        inv[c as usize] = i as i8;
    }

    let marker = BANNER.as_bytes();
    // Match on the stable prefix in case the version digits differ slightly.
    let prefix = b"(This file must be converted with BinHex";
    let pos = find_subslice(input, prefix)
        .or_else(|| find_subslice(input, marker))
        .ok_or_else(|| anyhow::anyhow!("BinHex: banner not found"))?;

    // Skip to the end of the banner line, then to the opening ':'.
    let mut i = pos + prefix.len();
    while i < input.len() && input[i] != b'\n' && input[i] != b'\r' {
        i += 1;
    }
    while i < input.len() && input[i] != b':' {
        i += 1;
    }
    if i >= input.len() {
        bail!("BinHex: opening ':' not found");
    }
    i += 1; // skip opening ':'

    // 6-bit decode until the closing ':'.
    let mut rle = Vec::new();
    let mut acc: u32 = 0;
    let mut bits = 0u32;
    while i < input.len() {
        let c = input[i];
        i += 1;
        if c == b':' {
            break;
        }
        let v = inv[c as usize];
        if v < 0 {
            continue; // whitespace / line breaks
        }
        acc = (acc << 6) | v as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            rle.push((acc >> bits) as u8);
        }
    }

    let raw = rle90_decode(&rle);
    parse_decoded(&raw)
}

fn parse_decoded(raw: &[u8]) -> Result<BinHexFile> {
    if raw.is_empty() {
        bail!("BinHex: empty payload");
    }
    let name_len = raw[0] as usize;
    if name_len == 0 || name_len > 63 {
        bail!("BinHex: bad name length {name_len}");
    }
    // nameLen(1) name version(1) type(4) creator(4) flags(2) dataLen(4) rsrcLen(4) crc(2)
    let header_len = 1 + name_len + 1 + 4 + 4 + 2 + 4 + 4;
    let need = header_len + 2;
    if raw.len() < need {
        bail!("BinHex: truncated header");
    }

    let name = String::from_utf8_lossy(&raw[1..1 + name_len]).into_owned();
    let mut p = 1 + name_len + 1; // skip version byte
    let type_code: [u8; 4] = raw[p..p + 4].try_into().unwrap();
    p += 4;
    let creator_code: [u8; 4] = raw[p..p + 4].try_into().unwrap();
    p += 4;
    let flags = BigEndian::read_u16(&raw[p..p + 2]);
    p += 2;
    let data_len = BigEndian::read_u32(&raw[p..p + 4]) as usize;
    p += 4;
    let rsrc_len = BigEndian::read_u32(&raw[p..p + 4]) as usize;
    p += 4;
    let header_crc = BigEndian::read_u16(&raw[p..p + 2]);

    let computed = binhex_crc16(&raw[0..header_len]);
    if computed != header_crc {
        bail!("BinHex: header CRC mismatch (got {computed:#06x}, expected {header_crc:#06x})");
    }

    // data fork + its CRC
    let data_start = header_len + 2;
    let data_end = data_start + data_len;
    if raw.len() < data_end + 2 {
        bail!("BinHex: truncated data fork");
    }
    let data_fork = raw[data_start..data_end].to_vec();
    let data_crc = BigEndian::read_u16(&raw[data_end..data_end + 2]);
    let computed = binhex_crc16(&data_fork);
    if computed != data_crc {
        bail!("BinHex: data fork CRC mismatch (got {computed:#06x}, expected {data_crc:#06x})");
    }

    // resource fork + its CRC
    let rsrc_start = data_end + 2;
    let rsrc_end = rsrc_start + rsrc_len;
    if raw.len() < rsrc_end + 2 {
        bail!("BinHex: truncated resource fork");
    }
    let resource_fork = raw[rsrc_start..rsrc_end].to_vec();
    let rsrc_crc = BigEndian::read_u16(&raw[rsrc_end..rsrc_end + 2]);
    let computed = binhex_crc16(&resource_fork);
    if computed != rsrc_crc {
        bail!("BinHex: resource fork CRC mismatch (got {computed:#06x}, expected {rsrc_crc:#06x})");
    }

    Ok(BinHexFile {
        name,
        type_code,
        creator_code,
        flags,
        data_fork,
        resource_fork,
    })
}

fn push_u16(buf: &mut Vec<u8>, v: u16) {
    buf.push((v >> 8) as u8);
    buf.push((v & 0xff) as u8);
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc_known_vector() {
        // BinHex CRC of empty input (just the two augment bytes) is 0.
        assert_eq!(binhex_crc16(&[]), 0);
        // Single 0x00 byte -> still 0 through the register.
        assert_eq!(binhex_crc16(&[0]), 0);
    }

    #[test]
    fn rle90_roundtrip() {
        let cases: Vec<Vec<u8>> = vec![
            vec![],
            vec![1, 2, 3],
            vec![5, 5, 5, 5, 5],
            vec![0x90],
            vec![0x90, 0x90, 0x90],
            vec![0, 0, 0, 0x90, 1, 1, 1, 1, 0x90, 0x90],
            (0..=255u8).collect(),
            vec![7u8; 300], // exceeds single-byte run cap
        ];
        for c in cases {
            let enc = rle90_encode(&c);
            let dec = rle90_decode(&enc);
            assert_eq!(dec, c, "rle90 roundtrip failed for {c:?}");
        }
    }

    #[test]
    fn sixbit_roundtrip_via_full() {
        // Exercised more fully by binhex_roundtrip; this checks alphabet sanity.
        assert_eq!(ALPHABET.len(), 64);
        let mut seen = std::collections::HashSet::new();
        for &c in ALPHABET.iter() {
            assert!(seen.insert(c), "duplicate alphabet char {c}");
        }
    }

    #[test]
    fn binhex_roundtrip() {
        let file = BinHexFile {
            name: "Test File".to_string(),
            type_code: *b"TEXT",
            creator_code: *b"ttxt",
            flags: 0x0000,
            data_fork: b"Hello, classic Mac world!\r\rThis has \x90\x90 markers.".to_vec(),
            resource_fork: vec![0xCA, 0xFE, 0xBA, 0xBE, 0, 0, 0, 1, 2, 3],
        };
        let text = build_binhex(&file);
        assert!(text.contains(BANNER));
        let parsed = parse_binhex(text.as_bytes()).expect("should parse");
        assert_eq!(parsed, file);
    }

    #[test]
    fn binhex_empty_forks() {
        let file = BinHexFile {
            name: "Empty".to_string(),
            type_code: *b"TEXT",
            creator_code: *b"ttxt",
            flags: 0,
            data_fork: vec![],
            resource_fork: vec![],
        };
        let text = build_binhex(&file);
        let parsed = parse_binhex(text.as_bytes()).expect("should parse");
        assert_eq!(parsed, file);
    }

    #[test]
    fn rejects_non_binhex() {
        assert!(parse_binhex(b"not a binhex file at all").is_err());
    }

    /// Validates against real `.hqx` samples if present locally (not in CI).
    /// Confirms the CRC + RLE + alphabet match real-world encoders.
    #[test]
    fn parse_real_samples_if_present() {
        let dir = std::path::Path::new("/Users/dani/Downloads");
        if !dir.is_dir() {
            return;
        }
        let mut checked = 0;
        for entry in std::fs::read_dir(dir).unwrap().flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("hqx") {
                continue;
            }
            let bytes = match std::fs::read(&path) {
                Ok(b) => b,
                Err(_) => continue,
            };
            let parsed = parse_binhex(&bytes).unwrap_or_else(|e| panic!("failed on {path:?}: {e}"));
            assert!(!parsed.name.is_empty(), "empty name in {path:?}");
            checked += 1;
            if checked >= 5 {
                break;
            }
        }
        eprintln!("validated {checked} real .hqx samples");
    }
}
