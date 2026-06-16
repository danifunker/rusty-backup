//! Commander Mode -- full-page two-pane file explorer overlay.
//!
//! This is wired into [`crate::gui::RustyBackupApp`] as `Option<CommanderMode>`:
//! `Some` means the overlay is open and takes over the whole frame (the tab
//! strip is not drawn). Each pane opens a disk image / container and browses it
//! (listing, sort, multi-select, `..` / double-click navigation, per-pane staged
//! delete), backed by the
//! [`DirListing`](rusty_backup::model::dir_listing::DirListing) model. The middle
//! column stages a cross-volume copy of one pane's selection onto the other
//! pane's queue (image -> image, via
//! [`commander_ops::stage_copy`](rusty_backup::model::commander_ops::stage_copy)).
//!
//! NOTE: this crate uses a patched eframe whose panels are
//! `egui::Panel::*::show_inside` rather than the stock `TopBottomPanel`. The
//! main app's `ui()` method hands us its `&mut egui::Ui`, so we build the
//! overlay with `show_inside` against it, exactly like the rest of the GUI.

use eframe::egui;

use rusty_backup::model::commander_ops;

mod pane;

use pane::CommanderPane;

/// Which pane is which. Kept tiny and `Copy` so it can key per-pane widget ids
/// (Commander draws two near-identical panes in one `Ui` tree, so every
/// stateful widget must take a side-keyed `id_salt` or egui raises an ID clash).
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Side {
    Left,
    Right,
}

impl Side {
    pub(crate) fn idx(self) -> usize {
        match self {
            Side::Left => 0,
            Side::Right => 1,
        }
    }
    pub(crate) fn label(self) -> &'static str {
        match self {
            Side::Left => "L",
            Side::Right => "R",
        }
    }
    pub(crate) fn other(self) -> Side {
        match self {
            Side::Left => Side::Right,
            Side::Right => Side::Left,
        }
    }
}

/// Full-page Commander Mode overlay.
pub struct CommanderMode {
    left: CommanderPane,
    right: CommanderPane,
    status: String,
    /// Scratch dir holding files extracted for image -> image copies, kept
    /// alive until the destination queue is applied (or the overlay closes).
    /// Created lazily on the first copy.
    temp: Option<tempfile::TempDir>,
    /// Whether the unsaved-edits confirmation is showing (Close was clicked
    /// while a pane had staged edits).
    unsaved_close: bool,
}

impl Default for CommanderMode {
    fn default() -> Self {
        Self::new()
    }
}

impl CommanderMode {
    pub fn new() -> Self {
        Self {
            left: CommanderPane::new(Side::Left),
            right: CommanderPane::new(Side::Right),
            status: "Commander Mode -- open a disk image in each pane; select files and \
                     use the middle Copy buttons or right-click to stage a copy."
                .into(),
            temp: None,
            unsaved_close: false,
        }
    }

    /// Render the overlay into the app's root `Ui`. Returns `true` when the
    /// user asks to close it (the caller then drops the `CommanderMode`).
    pub fn show(&mut self, ui: &mut egui::Ui) -> bool {
        let mut close = false;

        egui::Panel::top("commander_top").show_inside(ui, |ui| {
            ui.add_space(2.0);
            ui.horizontal(|ui| {
                ui.heading("Commander Mode");
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui.button("Close").clicked() {
                        if self.left.staged_count() + self.right.staged_count() > 0 {
                            self.unsaved_close = true;
                        } else {
                            close = true;
                        }
                    }
                });
            });
            ui.add_space(2.0);
        });

        egui::Panel::bottom("commander_bottom").show_inside(ui, |ui| {
            ui.add_space(2.0);
            ui.horizontal(|ui| {
                ui.label("Status:");
                ui.label(&self.status);
            });
            ui.add_space(2.0);
        });

        egui::CentralPanel::default().show_inside(ui, |ui| {
            let full_h = ui.available_height();
            let full_w = ui.available_width();
            let mid_w = 132.0;
            let pane_w = ((full_w - mid_w) / 2.0 - 8.0).max(200.0);
            ui.horizontal_top(|ui| {
                ui.allocate_ui_with_layout(
                    egui::vec2(pane_w, full_h),
                    egui::Layout::top_down(egui::Align::Min),
                    |ui| {
                        let resp = self.left.show(ui);
                        if let Some(msg) = resp.status {
                            self.status = msg;
                        }
                        if resp.copy_to_other {
                            self.status = self.copy(Side::Left);
                        }
                    },
                );
                ui.separator();
                ui.allocate_ui_with_layout(
                    egui::vec2(mid_w, full_h),
                    egui::Layout::top_down(egui::Align::Center),
                    |ui| {
                        if let Some(msg) = self.render_middle(ui) {
                            self.status = msg;
                        }
                    },
                );
                ui.separator();
                ui.allocate_ui_with_layout(
                    egui::vec2(pane_w, full_h),
                    egui::Layout::top_down(egui::Align::Min),
                    |ui| {
                        let resp = self.right.show(ui);
                        if let Some(msg) = resp.status {
                            self.status = msg;
                        }
                        if resp.copy_to_other {
                            self.status = self.copy(Side::Right);
                        }
                    },
                );
            });
        });

        if self.unsaved_close {
            let n = self.left.staged_count() + self.right.staged_count();
            egui::Window::new("Unsaved staged edits")
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ui.ctx(), |ui| {
                    ui.label(format!(
                        "{n} staged edit(s) across the panes have not been applied."
                    ));
                    ui.label("Apply them per-pane first, or discard.");
                    ui.add_space(6.0);
                    ui.horizontal(|ui| {
                        if ui.button("Discard & Close").clicked() {
                            self.left.discard_edits();
                            self.right.discard_edits();
                            self.unsaved_close = false;
                            close = true;
                        }
                        if ui.button("Cancel").clicked() {
                            self.unsaved_close = false;
                        }
                    });
                });
        }

        close
    }

    /// Middle action column: stage a cross-volume copy of one pane's selection
    /// onto the other. Delete is reachable per-pane (right-click); Compare is a
    /// later milestone.
    fn render_middle(&mut self, ui: &mut egui::Ui) -> Option<String> {
        let mut status = None;
        ui.add_space(60.0);
        let w = egui::vec2(116.0, 26.0);

        // H1: only image -> image copies are wired; a host pane on either end is
        // handled by the host-write engine (H2).
        let l_can =
            self.left.has_selection() && !self.left.is_host_pane() && self.right.can_receive();
        let r_can =
            self.right.has_selection() && !self.right.is_host_pane() && self.left.can_receive();

        if ui
            .add_enabled(l_can, egui::Button::new("Copy L -> R").min_size(w))
            .clicked()
        {
            status = Some(self.copy(Side::Left));
        }
        ui.add_space(6.0);
        if ui
            .add_enabled(r_can, egui::Button::new("Copy R -> L").min_size(w))
            .clicked()
        {
            status = Some(self.copy(Side::Right));
        }
        ui.add_space(12.0);
        // Delete is staged per-pane via right-click; Compare is deferred.
        ui.add_enabled(false, egui::Button::new("Delete\n(row menu)").min_size(w));
        ui.add_space(12.0);
        ui.add_enabled(
            false,
            egui::Button::new("Compare\n(later)").min_size(egui::vec2(116.0, 30.0)),
        );
        status
    }

    /// Stage a copy of the `from` pane's selection into the other pane's current
    /// directory (image -> image). Files (with resource fork + type/creator) and
    /// directory subtrees are extracted to `temp` and queued on the destination.
    fn copy(&mut self, from: Side) -> String {
        if self.temp.is_none() {
            self.temp = tempfile::tempdir().ok();
        }
        let Some(temp_dir) = self.temp.as_ref().map(|t| t.path().to_path_buf()) else {
            return "Could not create a temp directory for the copy.".to_string();
        };

        let (src, dest) = match from {
            Side::Left => (&mut self.left, &mut self.right),
            Side::Right => (&mut self.right, &mut self.left),
        };

        let entries = src.selected_entries();
        if entries.is_empty() {
            return format!("Nothing selected in the {} pane to copy.", from.label());
        }
        if !dest.can_receive() {
            return format!(
                "The {} pane can't receive a copy (open a volume there first).",
                from.other().label()
            );
        }
        let Some(dest_parent) = dest.cwd_entry() else {
            return "Open a destination volume first.".to_string();
        };
        let Some(src_fs) = src.fs_mut() else {
            return "Source volume is not open.".to_string();
        };

        match commander_ops::stage_copy(src_fs, &entries, &dest_parent, &temp_dir) {
            Ok(edits) => {
                let n = dest.stage_edits(edits);
                format!(
                    "Staged copy of {n} item(s) into the {} pane. Apply to write.",
                    from.other().label()
                )
            }
            Err(e) => format!("Copy failed: {e:#}"),
        }
    }
}
