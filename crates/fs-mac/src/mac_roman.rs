// SPDX-License-Identifier: MIT
//! Mac Roman names: the codec every classic Mac format shares (HFS and MFS catalogs, StuffIt,
//! Compact Pro, MacBinary, BinHex, mar). Always built, so a crate that wants only names, BinHex
//! and resource forks (feature `codecs`) needs neither HFS nor the `fs-mac-unicode` tables;
//! `hfs` re-exports it under its old paths.

use disk_core::filesystem::FilesystemError;

use disk_core::mac_roman::MAC_ROMAN_TABLE;

/// Convert a UTF-8 string to Mac Roman bytes.
/// Returns Err if any character cannot be represented in Mac Roman.
pub fn utf8_to_mac_roman(s: &str) -> Result<Vec<u8>, FilesystemError> {
    disk_core::mac_roman::encode(s).ok_or_else(|| {
        let ch = s
            .chars()
            .find(|c| disk_core::mac_roman::encode(c.encode_utf8(&mut [0; 4])).is_none())
            .unwrap_or('?');
        FilesystemError::InvalidData(format!(
            "character '{}' (U+{:04X}) cannot be encoded in Mac Roman",
            ch, ch as u32
        ))
    })
}

/// Decode a Mac Roman byte string to UTF-8.
pub fn mac_roman_to_utf8(data: &[u8]) -> String {
    disk_core::mac_roman::decode(data)
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
