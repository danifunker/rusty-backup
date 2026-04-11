//! ProDOS file type table.
//!
//! The default table (every row in `assets/prodos_file_types.json`) is
//! embedded at compile time via `include_str!`. Users can override or extend
//! it by placing a JSON file with the same shape at:
//!
//! - Linux:   `~/.config/rusty-backup/prodos_file_types.json`
//! - macOS:   `~/Library/Application Support/rusty-backup/prodos_file_types.json`
//! - Windows: `%APPDATA%\rusty-backup\prodos_file_types.json`
//!
//! User entries are merged on top of the embedded defaults: keys in the user
//! file replace matching entries in the embedded table, and extra keys are
//! added. Removing a default entry is not supported — start from a clean file
//! if you want a smaller table.
//!
//! The table is loaded once per process via `OnceLock`. To pick up changes
//! during development, restart the app.

use serde::Deserialize;
use std::collections::HashMap;
use std::sync::OnceLock;

const EMBEDDED_JSON: &str = include_str!("../../assets/prodos_file_types.json");

/// Information about a single ProDOS file type byte.
#[derive(Debug, Clone)]
pub struct ProdosTypeInfo {
    /// Three-letter abbreviation (e.g. "TXT", "BIN").
    pub abbr: String,
    /// Human-readable description (e.g. "ASCII Text").
    pub description: String,
    /// Broad category used for GUI icons / classification.
    /// Known values: text, binary, code, graphics, audio, document,
    /// directory, archive. Unknown values are passed through as-is.
    pub category: String,
}

/// Mapping from host-filename extension to ProDOS (type_byte, aux_type).
#[derive(Debug, Clone, Copy)]
pub struct ProdosExtensionInfo {
    pub type_byte: u8,
    pub aux_type: u16,
}

#[derive(Debug, Deserialize, Default)]
struct RawFile {
    #[serde(default)]
    types: HashMap<String, RawType>,
    #[serde(default)]
    extensions: HashMap<String, RawExt>,
}

#[derive(Debug, Deserialize)]
struct RawType {
    #[serde(default)]
    abbr: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    category: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawExt {
    #[serde(default, rename = "type")]
    type_code: Option<String>,
    #[serde(default)]
    aux_type: Option<String>,
}

struct ProdosTypeTable {
    by_byte: HashMap<u8, ProdosTypeInfo>,
    by_ext: HashMap<String, ProdosExtensionInfo>,
}

fn parse_hex_byte(s: &str) -> Option<u8> {
    let t = s.trim().trim_start_matches('$').trim_start_matches("0x");
    u8::from_str_radix(t, 16).ok()
}

fn parse_hex_u16(s: &str) -> Option<u16> {
    let t = s.trim().trim_start_matches('$').trim_start_matches("0x");
    u16::from_str_radix(t, 16).ok()
}

fn parse_json(text: &str) -> RawFile {
    serde_json::from_str(text).unwrap_or_default()
}

fn merge_raw(base: RawFile, overlay: RawFile) -> RawFile {
    let mut types = base.types;
    for (k, v) in overlay.types {
        // Skip keys beginning with "_" (reserved for comments/metadata).
        if k.starts_with('_') {
            continue;
        }
        types.insert(k, v);
    }
    let mut extensions = base.extensions;
    for (k, v) in overlay.extensions {
        if k.starts_with('_') {
            continue;
        }
        extensions.insert(k, v);
    }
    RawFile { types, extensions }
}

fn user_config_path() -> Option<std::path::PathBuf> {
    // Mirror the resolution used by src/update.rs so that pkexec-elevated
    // launches still resolve to the invoking user's config dir on Linux.
    #[cfg(target_os = "linux")]
    let base = crate::os::linux::real_user_home().map(|h| h.join(".config"));
    #[cfg(not(target_os = "linux"))]
    let base = dirs::config_dir();
    base.map(|d| d.join("rusty-backup").join("prodos_file_types.json"))
}

fn build_table() -> ProdosTypeTable {
    let embedded = parse_json(EMBEDDED_JSON);
    let merged = if let Some(path) = user_config_path() {
        match std::fs::read_to_string(&path) {
            Ok(user_text) => {
                let overlay = parse_json(&user_text);
                merge_raw(embedded, overlay)
            }
            Err(_) => embedded,
        }
    } else {
        embedded
    };

    let mut by_byte: HashMap<u8, ProdosTypeInfo> = HashMap::new();
    for (k, v) in merged.types {
        if k.starts_with('_') {
            continue;
        }
        let Some(byte) = parse_hex_byte(&k) else {
            continue;
        };
        let info = ProdosTypeInfo {
            abbr: v.abbr.unwrap_or_else(|| "???".to_string()),
            description: v.description.unwrap_or_default(),
            category: v.category.unwrap_or_else(|| "binary".to_string()),
        };
        by_byte.insert(byte, info);
    }

    let mut by_ext: HashMap<String, ProdosExtensionInfo> = HashMap::new();
    for (k, v) in merged.extensions {
        if k.starts_with('_') {
            continue;
        }
        let Some(tc) = v.type_code.as_deref().and_then(parse_hex_byte) else {
            continue;
        };
        let aux = v.aux_type.as_deref().and_then(parse_hex_u16).unwrap_or(0);
        by_ext.insert(
            k.to_ascii_lowercase(),
            ProdosExtensionInfo {
                type_byte: tc,
                aux_type: aux,
            },
        );
    }

    ProdosTypeTable { by_byte, by_ext }
}

fn table() -> &'static ProdosTypeTable {
    static TABLE: OnceLock<ProdosTypeTable> = OnceLock::new();
    TABLE.get_or_init(build_table)
}

/// Return all known ProDOS file types sorted ascending by type byte.
///
/// Each tuple is `(byte, info)`. Useful for populating GUI dropdowns.
pub fn all_types() -> Vec<(u8, &'static ProdosTypeInfo)> {
    let t = table();
    let mut v: Vec<(u8, &'static ProdosTypeInfo)> =
        t.by_byte.iter().map(|(k, v)| (*k, v)).collect();
    v.sort_by_key(|(k, _)| *k);
    v
}

/// Look up the 3-letter abbreviation for a ProDOS file type byte.
/// Returns "???" for unknown types.
pub fn type_abbr(file_type: u8) -> &'static str {
    table()
        .by_byte
        .get(&file_type)
        .map(|info| info.abbr.as_str())
        .unwrap_or("???")
}

/// Look up the description for a ProDOS file type byte. Returns empty
/// string for unknown types.
pub fn type_description(file_type: u8) -> &'static str {
    table()
        .by_byte
        .get(&file_type)
        .map(|info| info.description.as_str())
        .unwrap_or("")
}

/// Look up the category ("text", "binary", "code", ...) for a ProDOS file
/// type byte. Returns "binary" for unknown types.
pub fn type_category(file_type: u8) -> &'static str {
    table()
        .by_byte
        .get(&file_type)
        .map(|info| info.category.as_str())
        .unwrap_or("binary")
}

/// Format a ProDOS type byte for display as "$XX ABC".
pub fn format_type_code(file_type: u8) -> String {
    format!("${:02X} {}", file_type, type_abbr(file_type))
}

/// Look up the ProDOS (type_byte, aux_type) pair for a host filename
/// extension. Case-insensitive; pass without the leading dot.
pub fn type_for_extension(ext: &str) -> Option<ProdosExtensionInfo> {
    let lower = ext.trim_start_matches('.').to_ascii_lowercase();
    table().by_ext.get(&lower).copied()
}

/// Derive a ProDOS type byte + aux type from a filename, using the last
/// extension. Returns None if the filename has no recognized extension.
pub fn type_for_filename(name: &str) -> Option<ProdosExtensionInfo> {
    let dot = name.rfind('.')?;
    type_for_extension(&name[dot + 1..])
}

/// Encode a ProDOS `(type_byte, aux_type)` pair as a CiderPress-style
/// filename suffix, including the leading `#`. Uppercase hex, big-endian
/// aux (as written on the host filename).
///
/// Example: `encode_cp_suffix(0xFC, 0x0801) == "#FC0801"`.
pub fn encode_cp_suffix(type_byte: u8, aux_type: u16) -> String {
    format!("#{:02X}{:04X}", type_byte, aux_type)
}

/// Look for a trailing CiderPress `#TTAAAA` suffix on a host filename.
///
/// Returns `(stem, type_byte, aux_type)` if the filename ends with a valid
/// 6-digit hex suffix preceded by `#`. The `stem` is the filename with the
/// suffix removed.
///
/// - Hex digits are case-insensitive (`#fc0801` and `#FC0801` both parse).
/// - The suffix must be the literal tail of the filename. A trailing
///   dot-extension after the suffix (e.g. `FOO#FC0801.bak`) is not matched;
///   callers should fall back to extension-based detection.
pub fn decode_cp_suffix(filename: &str) -> Option<(&str, u8, u16)> {
    // Find the last '#' in the filename.
    let hash_idx = filename.rfind('#')?;
    let tail = &filename[hash_idx + 1..];
    if tail.len() != 6 {
        return None;
    }
    if !tail.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let type_byte = u8::from_str_radix(&tail[0..2], 16).ok()?;
    let aux_type = u16::from_str_radix(&tail[2..6], 16).ok()?;
    let stem = &filename[..hash_idx];
    Some((stem, type_byte, aux_type))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_table_has_core_entries() {
        assert_eq!(type_abbr(0x04), "TXT");
        assert_eq!(type_abbr(0x06), "BIN");
        assert_eq!(type_abbr(0x0F), "DIR");
        assert_eq!(type_abbr(0xFC), "BAS");
        assert_eq!(type_abbr(0xFF), "SYS");
    }

    #[test]
    fn unknown_types_return_placeholder() {
        // 0x99 isn't used in the standard table.
        assert_eq!(type_abbr(0x99), "???");
        assert_eq!(type_description(0x99), "");
        assert_eq!(type_category(0x99), "binary");
    }

    #[test]
    fn txt_is_text_category() {
        assert_eq!(type_category(0x04), "text");
    }

    #[test]
    fn format_type_code_matches_expected_shape() {
        assert_eq!(format_type_code(0x04), "$04 TXT");
        assert_eq!(format_type_code(0xFC), "$FC BAS");
    }

    #[test]
    fn extension_lookup_case_insensitive() {
        let txt = type_for_extension("txt").unwrap();
        assert_eq!(txt.type_byte, 0x04);
        let txt_upper = type_for_extension("TXT").unwrap();
        assert_eq!(txt_upper.type_byte, 0x04);
        let txt_dotted = type_for_extension(".Txt").unwrap();
        assert_eq!(txt_dotted.type_byte, 0x04);
    }

    #[test]
    fn basic_program_gets_0801_aux() {
        let bas = type_for_extension("bas").unwrap();
        assert_eq!(bas.type_byte, 0xFC);
        assert_eq!(bas.aux_type, 0x0801);
    }

    #[test]
    fn filename_lookup_uses_last_extension() {
        let info = type_for_filename("README.TXT").unwrap();
        assert_eq!(info.type_byte, 0x04);
        let info2 = type_for_filename("GAME.SRC.BAS").unwrap();
        assert_eq!(info2.type_byte, 0xFC);
        assert!(type_for_filename("NOEXT").is_none());
    }

    #[test]
    fn encode_cp_suffix_formats_uppercase_hex() {
        assert_eq!(encode_cp_suffix(0x04, 0x0000), "#040000");
        assert_eq!(encode_cp_suffix(0xFC, 0x0801), "#FC0801");
        assert_eq!(encode_cp_suffix(0xB3, 0xDB7F), "#B3DB7F");
        assert_eq!(encode_cp_suffix(0xFF, 0xFFFF), "#FFFFFF");
    }

    #[test]
    fn decode_cp_suffix_parses_valid_suffixes() {
        assert_eq!(
            decode_cp_suffix("README.TXT#040000"),
            Some(("README.TXT", 0x04, 0x0000))
        );
        assert_eq!(
            decode_cp_suffix("HELLO.BAS#FC0801"),
            Some(("HELLO.BAS", 0xFC, 0x0801))
        );
        assert_eq!(
            decode_cp_suffix("ANTETRIS#B3DB7F"),
            Some(("ANTETRIS", 0xB3, 0xDB7F))
        );
    }

    #[test]
    fn decode_cp_suffix_is_case_insensitive() {
        assert_eq!(
            decode_cp_suffix("hello.bas#fc0801"),
            Some(("hello.bas", 0xFC, 0x0801))
        );
        assert_eq!(
            decode_cp_suffix("HELLO.BAS#Fc0801"),
            Some(("HELLO.BAS", 0xFC, 0x0801))
        );
    }

    #[test]
    fn decode_cp_suffix_rejects_bad_input() {
        // No '#'.
        assert!(decode_cp_suffix("README.TXT").is_none());
        // 5 hex digits.
        assert!(decode_cp_suffix("README.TXT#FC080").is_none());
        // 7 hex digits.
        assert!(decode_cp_suffix("README.TXT#FC08011").is_none());
        // Trailing extension after the suffix.
        assert!(decode_cp_suffix("FOO#FC0801.bak").is_none());
        // Non-hex character.
        assert!(decode_cp_suffix("README.TXT#FCZ801").is_none());
    }

    #[test]
    fn encode_decode_round_trip() {
        for &(tt, aa) in &[
            (0x04u8, 0x0000u16),
            (0xFC, 0x0801),
            (0xB3, 0xDB7F),
            (0xFF, 0xFFFF),
            (0x00, 0x1234),
        ] {
            let suffix = encode_cp_suffix(tt, aa);
            let name = format!("NAME{}", suffix);
            let (stem, tt2, aa2) = decode_cp_suffix(&name).unwrap();
            assert_eq!(stem, "NAME");
            assert_eq!(tt, tt2);
            assert_eq!(aa, aa2);
        }
    }
}
