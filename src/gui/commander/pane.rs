//! One Commander pane.
//!
//! Shell only for now: a pane can pick a source file via the picker; the
//! partition dropdown, directory listing, sorting, selection, copy, and
//! per-pane staging queue land in later milestones (see
//! `docs/commander_mode.md`). Everything here is designed to grow into the
//! `PaneSource` / `DirListing` / `EditQueue` model described in the plan.

use eframe::egui;
use std::path::PathBuf;

use super::Side;

pub(crate) struct CommanderPane {
    side: Side,
    /// Loaded source path (image or device). `None` until the user opens one.
    source: Option<PathBuf>,
}

impl CommanderPane {
    pub(crate) fn new(side: Side) -> Self {
        Self { side, source: None }
    }

    /// Render the pane. Returns a status line when the user did something worth
    /// surfacing in the overlay's bottom bar.
    pub(crate) fn show(&mut self, ui: &mut egui::Ui) -> Option<String> {
        let mut status = None;

        // Source bar: open + (placeholder) partition selector. The partition
        // ComboBox is keyed by side so the two panes don't clash ids.
        ui.horizontal(|ui| {
            if ui.button("Open...").clicked() {
                // TODO(commander M1): route through the shared source picker
                // (device / image / host folder) extracted from the Inspect tab.
                if let Some(path) = super::super::file_dialog()
                    .add_filter(
                        "Disk Images",
                        rusty_backup::model::file_types::DISK_IMAGE_EXTS,
                    )
                    .add_filter("All Files", &["*"])
                    .pick_file()
                {
                    let display = path.display().to_string();
                    self.source = Some(path);
                    status = Some(format!(
                        "[{}] loaded {display} -- listing engine pending.",
                        self.side.label()
                    ));
                }
            }
            egui::ComboBox::from_id_salt(("commander_part", self.side.idx()))
                .selected_text("(no partitions)")
                .show_ui(ui, |_ui| {});
        });
        ui.separator();

        // Placeholder body until the directory grid lands.
        ui.centered_and_justified(|ui| match &self.source {
            Some(path) => {
                ui.label(format!(
                    "Loaded: {}\n\nDirectory listing, partition selection, and copy /\nstage actions arrive in the next milestone.",
                    path.display()
                ));
            }
            None => {
                ui.weak(
                    "Open a disk image / device, or drag a file here.\n(Commander listing engine: in progress.)",
                );
            }
        });

        status
    }
}
