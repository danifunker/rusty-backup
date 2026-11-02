//! "Resize Alto Disk..." dialog for the Inspect tab: a shell over `controller::alto_resize`.
//!
//! Draws the source summary, the target radios and the result, maps them to
//! [`AltoResizeAction`]s and answers the Save As picker through an [`EffectHost`].

use rusty_backup::controller::alto_resize::{AltoResizeAction, AltoResizeController, AltoTarget};
use rusty_backup::controller::log::LogStore;
use rusty_backup::controller::{Controller, Effect};

use super::effects::{EffectHost, Reply};

pub struct AltoResizeDialog {
    ctl: AltoResizeController,
    effects: EffectHost,
    pending_fx: Vec<Effect>,
}

impl AltoResizeDialog {
    /// Open the dialog on an Alto pack; its summary is read in a job.
    pub fn new(source_path: std::path::PathBuf) -> Self {
        let mut dlg = AltoResizeDialog {
            ctl: AltoResizeController::new(std::sync::Arc::new(super::ui_logger::EguiWake)),
            effects: EffectHost::default(),
            pending_fx: Vec::new(),
        };
        dlg.act(AltoResizeAction::Open(source_path));
        dlg
    }

    fn act(&mut self, action: AltoResizeAction) {
        let fx = self.ctl.apply(action);
        self.pending_fx.extend(fx);
    }

    /// Returns `true` while the dialog should stay open.
    pub fn show(&mut self, ctx: &egui::Context, log: &mut LogStore) -> bool {
        let fx = self.ctl.tick();
        self.pending_fx.extend(fx);
        drain_log(&mut self.ctl, log);
        if !self.ctl.is_open() {
            return false;
        }
        let mut actions = Vec::new();
        let mut window_open = true;
        egui::Window::new("Resize Alto Disk")
            .collapsible(false)
            .resizable(false)
            .open(&mut window_open)
            .show(ctx, |ui| {
                if let Some(path) = self.ctl.source() {
                    ui.label(format!("Source: {}", path.display()));
                }
                if let Some(snap) = self.ctl.opening() {
                    super::progress::show_job(ui, &snap);
                    return;
                }
                ui.label(self.ctl.summary_line().unwrap_or(""));
                ui.separator();
                ui.label("Target geometry:");
                ui.add_enabled_ui(!self.ctl.is_writing(), |ui| {
                    for t in AltoTarget::ALL {
                        if ui.radio(self.ctl.target() == t, t.label()).clicked() {
                            actions.push(AltoResizeAction::SetTarget(t));
                        }
                    }
                });
                if let Some(warning) = self.ctl.fit_warning() {
                    ui.colored_label(super::theme::danger(ui.visuals()), warning);
                }

                ui.separator();
                match self.ctl.result() {
                    Some(Ok(msg)) => {
                        ui.colored_label(super::theme::success(ui.visuals()), msg.clone());
                    }
                    Some(Err(e)) => {
                        ui.colored_label(super::theme::danger(ui.visuals()), format!("Error: {e}"));
                    }
                    None => {}
                }
                if self.ctl.is_writing() {
                    ui.horizontal(|ui| {
                        ui.spinner();
                        ui.label("Resizing...");
                    });
                }

                ui.horizontal(|ui| {
                    if ui
                        .add_enabled(
                            self.ctl.can_save(),
                            egui::Button::new("Resize and Save As..."),
                        )
                        .clicked()
                    {
                        actions.push(AltoResizeAction::SaveAs);
                    }
                    if ui
                        .add_enabled(!self.ctl.is_writing(), egui::Button::new("Close"))
                        .clicked()
                    {
                        actions.push(AltoResizeAction::Close);
                    }
                });
            });
        if !window_open {
            actions.push(AltoResizeAction::Close);
        }
        for a in actions {
            self.act(a);
        }
        self.run_effects(ctx, log);
        self.ctl.is_open()
    }

    fn run_effects(&mut self, ctx: &egui::Context, log: &mut LogStore) {
        let fx = std::mem::take(&mut self.pending_fx);
        let mut messages = Vec::new();
        let mut replies = self.effects.run(ctx, fx, &mut messages);
        replies.extend(self.effects.show(ctx, "Alto resize"));
        for msg in messages {
            log.info(msg);
        }
        for reply in replies {
            match reply {
                Reply::Picked { purpose, path } => {
                    self.act(AltoResizeAction::Picked { purpose, path })
                }
                Reply::Confirmed { .. } | Reply::Answered { .. } | Reply::CloseScreen => {}
            }
        }
    }
}

fn drain_log(ctl: &mut AltoResizeController, log: &mut LogStore) {
    super::progress::drain_log_lines(ctl.take_log(), log);
}
