//! Shared file-detail rendering: read-only metadata rows, content-type
//! classification, and a hex/text preview.
//!
//! Extracted from `browse_view` (R0 of the Commander Mode plan, see
//! `docs/commander_mode.md` §3) so the classic browse view and Commander
//! Mode's File Info window render identical detail. Pure view helpers — no
//! `self`, no background work; they take a `&FileEntry` (and the decoded
//! bytes, for the preview) and draw into the supplied `egui::Ui`.

use rusty_backup::fs::entry::FileEntry;
use rusty_backup::partition;

/// Decoded file content for the preview pane: either a text rendering or the
/// raw bytes for a hex dump.
#[derive(Debug, Clone)]
pub enum FileContent {
    Binary(Vec<u8>),
    Text(String),
}

/// HFS/HFS+ file type classification from `assets/hfs_file_types.json`,
/// embedded at compile time.
mod hfs_file_types {
    use std::collections::HashMap;
    use std::sync::OnceLock;

    #[derive(serde::Deserialize)]
    struct TypeInfo {
        category: String,
    }

    #[derive(serde::Deserialize)]
    struct HfsFileTypes {
        types: HashMap<String, TypeInfo>,
    }

    static HFS_TYPES: OnceLock<HashMap<String, bool>> = OnceLock::new();
    const HFS_FILE_TYPES_JSON: &str = include_str!("../../assets/hfs_file_types.json");

    fn get() -> &'static HashMap<String, bool> {
        HFS_TYPES.get_or_init(|| {
            let ft: HfsFileTypes =
                serde_json::from_str(HFS_FILE_TYPES_JSON).unwrap_or(HfsFileTypes {
                    types: HashMap::new(),
                });
            ft.types
                .into_iter()
                .map(|(k, v)| (k, v.category == "text"))
                .collect()
        })
    }

    /// Returns Some(true) for known text type, Some(false) for known binary, None for unknown.
    pub fn classify(type_code: &str) -> Option<bool> {
        get().get(type_code).copied()
    }
}

/// Detect whether data is text or binary and return appropriate content.
pub fn detect_content_type(entry: &FileEntry, data: &[u8]) -> FileContent {
    if data.is_empty() {
        return FileContent::Text(String::new());
    }

    // Check HFS type code first (if present)
    if let Some(ref type_code) = entry.type_code {
        let type_str = rusty_backup::fs::hfs_common::decode_ostype(type_code);
        if let Some(is_text) = hfs_file_types::classify(&type_str) {
            if !is_text {
                return FileContent::Binary(data.to_vec());
            }
            // Known text type — decode as text (Mac files may not be UTF-8)
            if let Ok(text) = std::str::from_utf8(data) {
                return FileContent::Text(text.to_string());
            }
            // Not valid UTF-8 but known text type — show as lossy text
            let text = data
                .iter()
                .map(|&b| {
                    if b.is_ascii_graphic() || b.is_ascii_whitespace() {
                        b as char
                    } else {
                        '.'
                    }
                })
                .collect();
            return FileContent::Text(text);
        }
    }

    // Fall back to content heuristics
    // Try UTF-8 first
    if let Ok(text) = std::str::from_utf8(data) {
        let non_printable = text
            .chars()
            .filter(|c| !c.is_ascii_graphic() && !c.is_ascii_whitespace())
            .count();
        if non_printable * 10 < text.len() {
            return FileContent::Text(text.to_string());
        }
    }

    // Check if mostly printable bytes
    let printable = data
        .iter()
        .filter(|&&b| b.is_ascii_graphic() || b.is_ascii_whitespace())
        .count();
    if printable * 10 >= data.len() * 8 {
        // 80% printable
        let text = data
            .iter()
            .map(|&b| {
                if b.is_ascii_graphic() || b.is_ascii_whitespace() {
                    b as char
                } else {
                    '.'
                }
            })
            .collect();
        return FileContent::Text(text);
    }

    FileContent::Binary(data.to_vec())
}

/// Render a hex dump view of binary data.
pub fn render_hex_view(ui: &mut egui::Ui, data: &[u8]) {
    let bytes_per_line = 16;
    let lines = data.len().div_ceil(bytes_per_line);
    let max_lines = 256; // Limit display to ~4KB

    let display_lines = lines.min(max_lines);
    let mut hex_text = String::new();

    for i in 0..display_lines {
        let offset = i * bytes_per_line;
        hex_text.push_str(&format!("{offset:08X}  "));

        for j in 0..bytes_per_line {
            if offset + j < data.len() {
                hex_text.push_str(&format!("{:02X} ", data[offset + j]));
            } else {
                hex_text.push_str("   ");
            }
            if j == 7 {
                hex_text.push(' ');
            }
        }

        hex_text.push_str(" |");
        for j in 0..bytes_per_line {
            if offset + j < data.len() {
                let b = data[offset + j];
                hex_text.push(if b.is_ascii_graphic() || b == b' ' {
                    b as char
                } else {
                    '.'
                });
            }
        }
        hex_text.push_str("|\n");
    }

    if lines > max_lines {
        hex_text.push_str(&format!("... ({} more lines)\n", lines - max_lines));
    }

    ui.add(
        egui::TextEdit::multiline(&mut hex_text.as_str())
            .desired_width(f32::INFINITY)
            .font(egui::TextStyle::Monospace),
    );
}

/// Render the read-only metadata rows for a selected entry: a bold name
/// header followed by a horizontal row of size / modified / type / creator /
/// resource-fork size / permissions / owner / target / path, as the
/// filesystem populates them.
///
/// `suppress_type_creator` hides the inline Type/Creator labels for
/// filesystems (HFS/HFS+, ProDOS) that render those in a dedicated editor row
/// instead.
pub fn render_metadata_rows(ui: &mut egui::Ui, entry: &FileEntry, suppress_type_creator: bool) {
    // File info header
    ui.label(egui::RichText::new(&entry.name).strong());
    ui.horizontal(|ui| {
        ui.label(format!("Size: {}", entry.size_string()));
        if let Some(modified) = &entry.modified {
            if !modified.is_empty() {
                ui.label(format!("Modified: {modified}"));
            }
        }
        // HFS/HFS+ and ProDOS render Type/Creator (or Type/Aux) below in a
        // dedicated editor row. For other filesystems there's nothing extra to
        // show, so this block is empty.
        if !suppress_type_creator {
            if let Some(tc) = entry.type_code_display() {
                ui.label(format!("Type: {tc}"));
            }
            if let Some(cc) = entry.creator_code_display() {
                ui.label(format!("Creator: {cc}"));
            }
        }
        if let Some(ref rsrc) = entry.resource_fork_size {
            if *rsrc > 0 {
                ui.label(format!("Rsrc: {}", partition::format_size(*rsrc)));
            }
        }
        if let Some(mode_str) = entry.mode_string() {
            ui.label(format!("Permissions: {mode_str}"));
        }
        if let (Some(uid), Some(gid)) = (entry.uid, entry.gid) {
            ui.label(format!("Owner: {uid}:{gid}"));
        }
        if let Some(ref target) = entry.symlink_target {
            ui.label(format!("Target: {target}"));
        }
        if let Some(ref stype) = entry.special_type {
            ui.label(format!("Type: {stype}"));
        }
        ui.label(format!("Path: {}", entry.path));
    });
}
