//! Shared file-detail rendering: read-only metadata rows, content-type
//! classification, and a hex/text preview.
//!
//! Extracted from `browse_view` (R0 of the Commander Mode plan, see
//! `docs/commander_mode.md` §3) so the classic browse view and Commander
//! Mode's File Info window render identical detail. Pure view helpers — no
//! `self`, no background work; they take a `&FileEntry` (and the decoded
//! bytes, for the preview) and draw into the supplied `egui::Ui`. The
//! text-or-binary classification lives in `model::entry_detail`.

pub use rusty_backup::controller::commander::{detect_content_type, FileContent};
use rusty_backup::controller::commander::{
    format_size, FileEntry, FileEntryTypeDisplay as _, IdNameMap,
};

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
pub fn render_metadata_rows(
    ui: &mut egui::Ui,
    entry: &FileEntry,
    suppress_type_creator: bool,
    id_names: Option<&IdNameMap>,
) {
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
                ui.label(format!("Rsrc: {}", format_size(*rsrc)));
            }
        }
        if let Some(mode_str) = entry.mode_string() {
            ui.label(format!("Permissions: {mode_str}"));
        }
        if let (Some(uid), Some(gid)) = (entry.uid, entry.gid) {
            // Resolve to names via the image's own /etc/passwd + /etc/group when
            // available (e.g. "root:wheel (0:0)"); otherwise raw numbers.
            let owner = match id_names {
                Some(names) => names.format_owner(uid, gid),
                None => format!("{uid}:{gid}"),
            };
            ui.label(format!("Owner: {owner}"));
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
