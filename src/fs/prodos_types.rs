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
}
