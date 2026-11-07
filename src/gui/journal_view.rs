//! Read-only HFS+ journal history viewer (Phase 9, Step 29 of
//! `docs/hfsplus_enhancements.md`).
//!
//! Shows the parsed journal transaction log so the user can see what edit-mode
//! (or macOS) wrote, in order, with enough detail to troubleshoot a volume that
//! won't mount. ASCII only — no emoji or arrows beyond `->` (CLAUDE.md rule).

use rusty_backup::controller::commander::{
    journal_status, journal_summary, journal_txn_blocks, journal_txn_rows, JournalDetail,
    JournalTone,
};

/// Modal state for the journal viewer. Created when the user clicks "Journal"
/// in the browse-view toolbar; holds the decoded journal plus the currently
/// selected transaction row.
pub struct JournalView {
    detail: JournalDetail,
    /// Index into `detail.transactions` of the expanded row, if any.
    selected: Option<usize>,
}

impl JournalView {
    pub fn new(detail: JournalDetail) -> Self {
        JournalView {
            detail,
            selected: None,
        }
    }

    /// Render the viewer window. Returns `false` once the user closes it so the
    /// caller can drop the view.
    pub fn show(&mut self, ctx: &egui::Context) -> bool {
        let mut open = true;
        egui::Window::new("HFS+ Journal")
            .open(&mut open)
            .resizable(true)
            .default_width(640.0)
            .default_height(480.0)
            .show(ctx, |ui| {
                self.render(ui);
            });
        open
    }

    fn render(&mut self, ui: &mut egui::Ui) {
        ui.label(egui::RichText::new("Volume journal").strong());
        for line in journal_summary(&self.detail) {
            ui.label(line);
        }
        for (tone, line) in journal_status(&self.detail) {
            let color = match tone {
                JournalTone::Good => super::theme::success(ui.visuals()),
                JournalTone::Warning => super::theme::warning(ui.visuals()),
                JournalTone::Fault => super::theme::danger(ui.visuals()),
            };
            ui.colored_label(color, line);
        }

        ui.separator();

        ui.label(egui::RichText::new("Transactions (newest first):").strong());
        if self.detail.transactions.is_empty() {
            ui.label("  (none)");
            return;
        }

        let rows = journal_txn_rows(&self.detail);
        egui::ScrollArea::vertical()
            .id_salt("journal_txns")
            .max_height(180.0)
            .show(ui, |ui| {
                for (idx, label) in &rows {
                    let selected = self.selected == Some(*idx);
                    if ui.selectable_label(selected, label).clicked() {
                        self.selected = if selected { None } else { Some(*idx) };
                    }
                }
            });

        let blocks = self
            .selected
            .and_then(|i| journal_txn_blocks(&self.detail, i));
        if let Some((title, lines)) = blocks {
            ui.separator();
            ui.label(egui::RichText::new(title).strong());
            egui::ScrollArea::vertical()
                .id_salt("journal_blocks")
                .max_height(160.0)
                .show(ui, |ui| {
                    for line in lines {
                        ui.label(line);
                    }
                });
        }
    }
}
