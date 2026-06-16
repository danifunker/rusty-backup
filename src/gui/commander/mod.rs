//! Commander Mode -- full-page two-pane file explorer overlay.
//!
//! This is the entry-point shell wired into [`crate::gui::RustyBackupApp`] as
//! `Option<CommanderMode>`: `Some` means the overlay is open and takes over the
//! whole frame (the tab strip is not drawn). The directory-listing, copy, and
//! staging engine lands in later milestones -- see `docs/commander_mode.md`.
//!
//! NOTE: this crate uses a patched eframe whose panels are
//! `egui::Panel::*::show_inside` rather than the stock `TopBottomPanel`. The
//! main app's `ui()` method hands us its `&mut egui::Ui`, so we build the
//! overlay with `show_inside` against it, exactly like the rest of the GUI.

use eframe::egui;

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
}

/// Full-page Commander Mode overlay.
pub struct CommanderMode {
    left: CommanderPane,
    right: CommanderPane,
    status: String,
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
            status: "Commander Mode -- open a source in each pane. (Listing engine: in progress.)"
                .into(),
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
                        close = true;
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
                        if let Some(msg) = self.left.show(ui) {
                            self.status = msg;
                        }
                    },
                );
                ui.separator();
                ui.allocate_ui_with_layout(
                    egui::vec2(mid_w, full_h),
                    egui::Layout::top_down(egui::Align::Center),
                    render_middle,
                );
                ui.separator();
                ui.allocate_ui_with_layout(
                    egui::vec2(pane_w, full_h),
                    egui::Layout::top_down(egui::Align::Min),
                    |ui| {
                        if let Some(msg) = self.right.show(ui) {
                            self.status = msg;
                        }
                    },
                );
            });
        });

        close
    }
}

/// Middle action column. The copy/delete controls are disabled until the
/// listing + staging engine lands (next milestone); this fixes their place in
/// the layout so the shell already reads like the final UI.
fn render_middle(ui: &mut egui::Ui) {
    ui.add_space(60.0);
    let w = egui::vec2(116.0, 26.0);
    ui.add_enabled(false, egui::Button::new("Copy L -> R").min_size(w));
    ui.add_space(6.0);
    ui.add_enabled(false, egui::Button::new("Copy R -> L").min_size(w));
    ui.add_space(12.0);
    ui.add_enabled(false, egui::Button::new("Delete").min_size(w));
    ui.add_space(12.0);
    ui.add_enabled(
        false,
        egui::Button::new("Compare\n(later)").min_size(egui::vec2(116.0, 30.0)),
    );
}
