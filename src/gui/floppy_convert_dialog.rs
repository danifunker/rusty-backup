//! "Convert Floppy Container..." dialog for the Inspect tab: a shell over
//! `controller::floppy_convert`.
//!
//! Draws the source, the target radios, the output and the run's result, maps them to
//! [`FloppyConvertAction`]s and answers the pickers through an [`EffectHost`]. Bulk conversion
//! lives in the Bulk Convert dialog and `rb-cli floppy convert <dir> <dir> --to <fmt>`.

use rusty_backup::controller::floppy_convert::{
    FloppyConvertAction, FloppyConvertController, FloppyTarget,
};
use rusty_backup::controller::log::LogStore;
use rusty_backup::controller::{Controller, Effect};

use super::effects::{EffectHost, Reply};

pub struct FloppyConvertDialog {
    ctl: FloppyConvertController,
    effects: EffectHost,
    pending_fx: Vec<Effect>,
}

impl FloppyConvertDialog {
    /// A conversion is in flight.
    pub fn is_running(&self) -> bool {
        self.ctl.is_running()
    }

    /// Open with `path` as the source; it is sniffed in a job.
    pub fn with_source(path: std::path::PathBuf) -> Self {
        let mut dlg = FloppyConvertDialog {
            ctl: FloppyConvertController::new(std::sync::Arc::new(super::ui_logger::EguiWake)),
            effects: EffectHost::default(),
            pending_fx: Vec::new(),
        };
        dlg.act(FloppyConvertAction::Open(Some(path)));
        dlg
    }

    fn act(&mut self, action: FloppyConvertAction) {
        let fx = self.ctl.apply(action);
        self.pending_fx.extend(fx);
    }

    /// Render the dialog. Returns `false` once the user closes it for good.
    pub fn show(&mut self, ctx: &egui::Context, log: &mut LogStore) -> bool {
        let fx = self.ctl.tick();
        self.pending_fx.extend(fx);
        super::progress::drain_log_lines(self.ctl.take_log(), log);
        if !self.ctl.is_open() {
            return false;
        }
        let mut actions = Vec::new();
        let busy = self.ctl.is_running();
        let mut window_open = true;
        // No title-bar X while the conversion runs: it is still writing the output file.
        let window = egui::Window::new("Convert Floppy Container...");
        let window = if busy {
            window
        } else {
            window.open(&mut window_open)
        };
        window.resizable(true).default_width(520.0).show(ctx, |ui| {
            ui.label(egui::RichText::new("Source").strong());
            ui.horizontal(|ui| {
                let label = self
                    .ctl
                    .source()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| "(not chosen)".to_string());
                ui.label(label);
                if ui
                    .add_enabled(!busy, egui::Button::new("Choose File..."))
                    .clicked()
                {
                    actions.push(FloppyConvertAction::PickSource);
                }
                if self.ctl.probing() {
                    ui.spinner();
                }
            });
            if let Some(kind) = self.ctl.source_kind() {
                ui.label(format!("Detected: {kind}"));
            }
            if let Some(err) = self.ctl.source_error() {
                ui.colored_label(super::theme::danger(ui.visuals()), err);
            }
            ui.label(
                egui::RichText::new(
                    "Converting a folder of floppies? Use the main Bulk Convert dialog \
                     (XDF / HDM / DIM / D88 are listed there as output formats).",
                )
                .small()
                .italics(),
            );

            ui.separator();
            ui.label(egui::RichText::new("Target").strong());

            ui.add_enabled_ui(self.ctl.editable(), |ui| {
                let target = self.ctl.target();
                for (i, pair) in FloppyTarget::ALL.chunks(2).enumerate() {
                    ui.horizontal(|ui| {
                        if i == 0 {
                            ui.label("Output format:");
                        } else {
                            ui.add_space(96.0);
                        }
                        for &t in pair {
                            if ui.radio(target == t, t.label()).clicked() {
                                actions.push(FloppyConvertAction::SetTarget(t));
                            }
                        }
                    });
                }

                ui.horizontal(|ui| {
                    ui.label("Output file:");
                    let path_text = self
                        .ctl
                        .output()
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|| "(not chosen)".to_string());
                    ui.label(path_text);
                    if ui.button("Save As...").clicked() {
                        actions.push(FloppyConvertAction::PickOutput);
                    }
                });

                if self.ctl.same_format() {
                    ui.label(
                        egui::RichText::new(
                            "Source and target match - the output will be a byte-identical copy.",
                        )
                        .small()
                        .italics(),
                    );
                }
            });

            ui.separator();
            if let Some(snap) = self.ctl.progress() {
                ui.horizontal(|ui| {
                    ui.spinner();
                    ui.label(snap.phase.as_str());
                });
            } else {
                match self.ctl.outcome() {
                    Some(Err(err)) => {
                        ui.colored_label(
                            super::theme::danger(ui.visuals()),
                            format!("Failed: {err}"),
                        );
                    }
                    Some(Ok(done)) => {
                        ui.colored_label(
                            super::theme::success(ui.visuals()),
                            done.summary.as_str(),
                        );
                        ui.label(done.line.as_str());
                    }
                    None => {}
                }
            }

            ui.horizontal(|ui| {
                if ui
                    .add_enabled(self.ctl.can_start(), egui::Button::new("Convert"))
                    .clicked()
                {
                    actions.push(FloppyConvertAction::Start);
                }
                let close_label = if self.ctl.outcome().is_some() {
                    "Close"
                } else {
                    "Cancel"
                };
                if ui
                    .add_enabled(!busy, egui::Button::new(close_label))
                    .clicked()
                {
                    actions.push(FloppyConvertAction::Close);
                }
            });
        });
        if !window_open {
            actions.push(FloppyConvertAction::Close);
        }
        for a in actions {
            self.act(a);
        }
        self.run_effects(ctx, log);
        if busy || self.ctl.probing() {
            ctx.request_repaint_after(std::time::Duration::from_millis(100));
        }
        self.ctl.is_open()
    }

    fn run_effects(&mut self, ctx: &egui::Context, log: &mut LogStore) {
        let fx = std::mem::take(&mut self.pending_fx);
        let mut messages = Vec::new();
        let mut replies = self.effects.run(ctx, fx, &mut messages);
        replies.extend(self.effects.show(ctx, "Floppy convert"));
        for msg in messages {
            log.info(msg);
        }
        for reply in replies {
            match reply {
                Reply::Picked { purpose, path } => {
                    self.act(FloppyConvertAction::Picked { purpose, path })
                }
                Reply::Confirmed { .. } | Reply::Answered { .. } | Reply::CloseScreen => {}
            }
        }
    }
}
