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
use rusty_backup::controller::commander::{entry_facts, hex_dump, FileEntry, IdNameMap};

/// Render a hex dump view of binary data.
pub fn render_hex_view(ui: &mut egui::Ui, data: &[u8]) {
    let hex_text = hex_dump(data);
    ui.add(
        egui::TextEdit::multiline(&mut hex_text.as_str())
            .desired_width(f32::INFINITY)
            .font(egui::TextStyle::Monospace),
    );
}

/// Render the read-only metadata rows for a selected entry: a bold name header and the entry's facts.
///
/// `suppress_type_creator` hides the inline Type/Creator labels for filesystems (HFS/HFS+,
/// ProDOS) that render those in a dedicated editor row instead.
pub fn render_metadata_rows(
    ui: &mut egui::Ui,
    entry: &FileEntry,
    suppress_type_creator: bool,
    id_names: Option<&IdNameMap>,
) {
    ui.label(egui::RichText::new(&entry.name).strong());
    ui.horizontal(|ui| {
        for fact in entry_facts(entry, suppress_type_creator, id_names) {
            ui.label(fact);
        }
    });
}
