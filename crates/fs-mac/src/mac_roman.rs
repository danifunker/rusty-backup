// SPDX-License-Identifier: MIT
//! Mac Roman names: the codec every classic Mac format shares (HFS and MFS catalogs, StuffIt,
//! Compact Pro, MacBinary, BinHex, mar). Always built, so a crate that wants only names, BinHex
//! and resource forks (feature `codecs`) needs neither HFS nor the GPL `fs-mac-unicode` tables;
//! `hfs` re-exports it under its old paths.

use disk_core::filesystem::FilesystemError;

/// Mac Roman to Unicode lookup table for bytes 0x80-0xFF.
static MAC_ROMAN_TABLE: [char; 128] = [
    '\u{00C4}', '\u{00C5}', '\u{00C7}', '\u{00C9}', '\u{00D1}', '\u{00D6}', '\u{00DC}', '\u{00E1}',
    '\u{00E0}', '\u{00E2}', '\u{00E4}', '\u{00E3}', '\u{00E5}', '\u{00E7}', '\u{00E9}', '\u{00E8}',
    '\u{00EA}', '\u{00EB}', '\u{00ED}', '\u{00EC}', '\u{00EE}', '\u{00EF}', '\u{00F1}', '\u{00F3}',
    '\u{00F2}', '\u{00F4}', '\u{00F6}', '\u{00F5}', '\u{00FA}', '\u{00F9}', '\u{00FB}', '\u{00FC}',
    '\u{2020}', '\u{00B0}', '\u{00A2}', '\u{00A3}', '\u{00A7}', '\u{2022}', '\u{00B6}', '\u{00DF}',
    '\u{00AE}', '\u{00A9}', '\u{2122}', '\u{00B4}', '\u{00A8}', '\u{2260}', '\u{00C6}', '\u{00D8}',
    '\u{221E}', '\u{00B1}', '\u{2264}', '\u{2265}', '\u{00A5}', '\u{00B5}', '\u{2202}', '\u{2211}',
    '\u{220F}', '\u{03C0}', '\u{222B}', '\u{00AA}', '\u{00BA}', '\u{03A9}', '\u{00E6}', '\u{00F8}',
    '\u{00BF}', '\u{00A1}', '\u{00AC}', '\u{221A}', '\u{0192}', '\u{2248}', '\u{2206}', '\u{00AB}',
    '\u{00BB}', '\u{2026}', '\u{00A0}', '\u{00C0}', '\u{00C3}', '\u{00D5}', '\u{0152}', '\u{0153}',
    '\u{2013}', '\u{2014}', '\u{201C}', '\u{201D}', '\u{2018}', '\u{2019}', '\u{00F7}', '\u{25CA}',
    '\u{00FF}', '\u{0178}', '\u{2044}', '\u{20AC}', '\u{2039}', '\u{203A}', '\u{FB01}', '\u{FB02}',
    '\u{2021}', '\u{00B7}', '\u{201A}', '\u{201E}', '\u{2030}', '\u{00C2}', '\u{00CA}', '\u{00C1}',
    '\u{00CB}', '\u{00C8}', '\u{00CD}', '\u{00CE}', '\u{00CF}', '\u{00CC}', '\u{00D3}', '\u{00D4}',
    '\u{F8FF}', '\u{00D2}', '\u{00DA}', '\u{00DB}', '\u{00D9}', '\u{0131}', '\u{02C6}', '\u{02DC}',
    '\u{00AF}', '\u{02D8}', '\u{02D9}', '\u{02DA}', '\u{00B8}', '\u{02DD}', '\u{02DB}', '\u{02C7}',
];

/// Convert a UTF-8 string to Mac Roman bytes.
/// Returns Err if any character cannot be represented in Mac Roman.
pub fn utf8_to_mac_roman(s: &str) -> Result<Vec<u8>, FilesystemError> {
    let mut result = Vec::with_capacity(s.len());
    for ch in s.chars() {
        if (ch as u32) < 0x80 {
            result.push(ch as u8);
        } else {
            // Search the Mac Roman table for this character
            let mut found = false;
            for (i, &table_char) in MAC_ROMAN_TABLE.iter().enumerate() {
                if table_char == ch {
                    result.push((i as u8) + 0x80);
                    found = true;
                    break;
                }
            }
            if !found {
                return Err(FilesystemError::InvalidData(format!(
                    "character '{}' (U+{:04X}) cannot be encoded in Mac Roman",
                    ch, ch as u32
                )));
            }
        }
    }
    Ok(result)
}

/// Decode a Mac Roman byte string to UTF-8.
pub fn mac_roman_to_utf8(data: &[u8]) -> String {
    data.iter()
        .map(|&b| {
            if b < 0x80 {
                b as char
            } else {
                MAC_ROMAN_TABLE[(b - 0x80) as usize]
            }
        })
        .collect()
}

/// Decode an archive/container entry name whose encoding is ambiguous.
///
/// Vintage Mac archives (StuffIt, Compact Pro, MacBinary, MAR) store names in
/// Mac Roman, but some have been re-packed by modern tools that emit UTF-8 — so
/// a `™` arrives as the three UTF-8 bytes `E2 84 A2` instead of Mac Roman `0xAA`.
/// Decoding those blindly as Mac Roman yields mojibake (`‚Ñ¢`).
///
/// Heuristic: if the bytes are valid UTF-8 *and* carry at least one multi-byte
/// (non-ASCII) sequence, treat them as UTF-8; otherwise fall back to Mac Roman.
/// This is safe because a genuine Mac Roman high byte (e.g. `é` = `0x8E`) is a
/// lone byte ≥ 0x80, which is never a valid standalone UTF-8 sequence — so real
/// Mac Roman names always take the Mac Roman path. Pure-ASCII names decode
/// identically either way.
pub fn decode_mac_filename(data: &[u8]) -> String {
    if data.iter().any(|&b| b >= 0x80) {
        if let Ok(s) = std::str::from_utf8(data) {
            return s.to_string();
        }
    }
    mac_roman_to_utf8(data)
}

/// Render a 4-byte Mac OSType (a type or creator code) for display.
///
/// OSType codes are raw Mac Roman bytes, so high bytes like `•` (`0xA5`) must go
/// through the Mac Roman table rather than `from_utf8_lossy` (which renders them
/// as `�`). Control bytes (`< 0x20`) and `0x00` padding render as spaces so the
/// fixed 4-glyph shape is preserved. Trailing spaces are trimmed.
pub fn format_ostype(code: &[u8; 4]) -> String {
    code.iter()
        .map(|&b| match b {
            0x00..=0x1f => ' ',
            b if b < 0x80 => b as char,
            b => MAC_ROMAN_TABLE[(b - 0x80) as usize],
        })
        .collect::<String>()
        .trim_end()
        .to_string()
}
