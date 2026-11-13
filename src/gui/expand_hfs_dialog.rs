//! "Expand HFS Volume..." dialog for the Inspect tab: a shell over `controller::expand_hfs`.
//!
//! Draws the source summary and the target form, maps them to [`ExpandHfsAction`]s, answers
//! the Save As picker through an [`EffectHost`] and shows the expand's step and lines.

use rusty_backup::controller::commander::format_size;
use rusty_backup::controller::expand_hfs::{ExpandHfsAction, ExpandHfsController, ExpandSource};
use rusty_backup::controller::log::LogStore;
use rusty_backup::controller::{Controller, Effect};

use super::effects::{EffectHost, Reply};

pub struct ExpandHfsDialog {
    ctl: ExpandHfsController,
    effects: EffectHost,
    pending_fx: Vec<Effect>,
}

impl ExpandHfsDialog {
    /// An expand is in flight.
    pub fn is_running(&self) -> bool {
        self.ctl.is_running()
    }

    pub fn new(source: ExpandSource) -> Self {
        let mut dlg = ExpandHfsDialog {
            ctl: ExpandHfsController::new(std::sync::Arc::new(super::ui_logger::EguiWake)),
            effects: EffectHost::default(),
            pending_fx: Vec::new(),
        };
        dlg.act(ExpandHfsAction::Open(source));
        dlg
    }

    fn act(&mut self, action: ExpandHfsAction) {
        let fx = self.ctl.apply(action);
        self.pending_fx.extend(fx);
    }

    /// Render the dialog. Returns `false` once the user closes it for good.
    pub fn show(&mut self, ctx: &egui::Context, log: &mut LogStore) -> bool {
        let fx = self.ctl.tick();
        self.pending_fx.extend(fx);
        super::progress::drain_log_lines(self.ctl.take_log(), log);
        let Some(src) = self.ctl.source().cloned() else {
            return false;
        };
        let mut actions = Vec::new();
        let mut window_open = true;
        // No title-bar X while the worker runs: it is still writing the output file.
        let busy = self.ctl.is_running();
        let window = egui::Window::new("Expand HFS Volume...");
        let window = if busy {
            window
        } else {
            window.open(&mut window_open)
        };
        window.resizable(true).default_width(520.0).show(ctx, |ui| {
            ui.label(egui::RichText::new("Source").strong());
            if let Some(line) = self.ctl.partition_line() {
                ui.label(line);
            }
            let used = src.used_bytes.min(src.partition_size);
            let share = used as f32 / src.partition_size.max(1) as f32;
            let text = format!(
                "{} used, {} free",
                format_size(used),
                format_size(src.partition_size - used)
            );
            ui.add(egui::ProgressBar::new(share).text(text));
            ui.label(format!("Volume: {}", src.volume_name));
            ui.label(format!(
                "Size: {} MiB ({} bytes)",
                src.partition_size / (1024 * 1024),
                src.partition_size
            ));
            ui.label(format!(
                "Allocation block size: {} KiB",
                src.source_block_size / 1024
            ));
            ui.label(format!(
                "Used: {} MiB across {} file(s) in {} folder(s)",
                src.used_bytes / (1024 * 1024),
                src.file_count,
                src.dir_count
            ));

            ui.separator();
            ui.label(egui::RichText::new("Target").strong());

            ui.add_enabled_ui(self.ctl.editable(), |ui| {
                ui.horizontal(|ui| {
                    ui.label("Output format:");
                    let hfv = self.ctl.flat_hfv();
                    if ui.radio(!hfv, "APM disk (.hda)").clicked() {
                        actions.push(ExpandHfsAction::SetFlatHfv(false));
                    }
                    if ui.radio(hfv, "Bare HFS image (.hfv / .img)").clicked() {
                        actions.push(ExpandHfsAction::SetFlatHfv(true));
                    }
                });
                if self.ctl.flat_hfv() {
                    ui.label(
                        egui::RichText::new(
                            "Bare classic-HFS volume with no partition table - mountable in \
                                 BasiliskII / SheepShaver as `.hfv`, or usable as a raw single- \
                                 partition image. Capped at 4063 MB (classic HFS limit).",
                        )
                        .small()
                        .italics(),
                    );
                }

                ui.horizontal(|ui| {
                    ui.label("Allocation block size:");
                    egui::ComboBox::new("expand_hfs_bs", "")
                        .selected_text(self.ctl.block_size_label())
                        .show_ui(ui, |ui| {
                            for choice in self.ctl.block_choices() {
                                let on = choice.bytes == self.ctl.block_size();
                                if ui.selectable_label(on, choice.label).clicked() {
                                    actions.push(ExpandHfsAction::SetBlockSize(choice.bytes));
                                }
                            }
                        });
                });

                let (min_mib, max_mib) = self.ctl.size_bounds();
                ui.horizontal(|ui| {
                    ui.label("Target size (MiB):");
                    let mut mib = self.ctl.size_mib();
                    if ui
                        .add(egui::Slider::new(&mut mib, min_mib..=max_mib))
                        .changed()
                    {
                        actions.push(ExpandHfsAction::SetSizeMib(mib));
                    }
                });

                ui.horizontal(|ui| {
                    ui.label("Output file:");
                    let path_text = self
                        .ctl
                        .output()
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|| "(not chosen)".to_string());
                    ui.label(path_text);
                    if ui.button("Save As...").clicked() {
                        actions.push(ExpandHfsAction::PickOutput);
                    }
                });
            });

            ui.separator();
            let lines = self.ctl.lines();
            if let Some(snap) = self.ctl.progress() {
                ui.horizontal(|ui| {
                    ui.spinner();
                    ui.label(if snap.phase.is_empty() {
                        "Starting..."
                    } else {
                        snap.phase.as_str()
                    });
                });
            } else if let Some(Err(err)) = self.ctl.outcome() {
                ui.colored_label(super::theme::danger(ui.visuals()), format!("Failed: {err}"));
            } else if self.ctl.outcome().is_some() {
                ui.colored_label(super::theme::success(ui.visuals()), "Expand complete.");
            }
            if !lines.is_empty() {
                egui::ScrollArea::vertical()
                    .max_height(140.0)
                    .id_salt("expand_hfs_log")
                    .show(ui, |ui| {
                        for m in &lines {
                            ui.label(m);
                        }
                    });
            }

            ui.horizontal(|ui| {
                if ui
                    .add_enabled(self.ctl.can_start(), egui::Button::new("Expand"))
                    .clicked()
                {
                    actions.push(ExpandHfsAction::Start);
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
                    actions.push(ExpandHfsAction::Close);
                }
            });
        });
        if !window_open {
            actions.push(ExpandHfsAction::Close);
        }
        for a in actions {
            self.act(a);
        }
        self.run_effects(ctx, log);
        if busy {
            ctx.request_repaint_after(std::time::Duration::from_millis(100));
        }
        self.ctl.is_open()
    }

    fn run_effects(&mut self, ctx: &egui::Context, log: &mut LogStore) {
        let fx = std::mem::take(&mut self.pending_fx);
        let mut messages = Vec::new();
        let mut replies = self.effects.run(ctx, fx, &mut messages);
        replies.extend(self.effects.show(ctx, "Expand HFS"));
        for msg in messages {
            log.info(msg);
        }
        for reply in replies {
            match reply {
                Reply::Picked { purpose, path } => {
                    self.act(ExpandHfsAction::Picked { purpose, path })
                }
                Reply::Confirmed { .. }
                | Reply::Answered { .. }
                | Reply::Chose { .. }
                | Reply::CloseScreen => {}
            }
        }
    }
}
