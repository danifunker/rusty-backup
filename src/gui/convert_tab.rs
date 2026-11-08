//! The Convert tab: a shell over `controller::convert` (docs/convert_tab.md).
//!
//! The controller owns the source, the per-kind formats, sizing, the file list and both jobs;
//! this view draws them, keeps one CHD options control per CHD profile, and answers pickers and
//! the Skip / Overwrite / Back question through its `EffectHost`.

use std::path::PathBuf;
use std::sync::Arc;

use rusty_backup::controller::convert::{
    floppy_size_choices, formats_for, hints, ChdChoices, ChdProfile, ConvertAction,
    ConvertController, ExtraUnit, FloppySize, ImageKind, SizeChoice, ONLY_FILES,
};
use rusty_backup::controller::log::LogStore;
use rusty_backup::controller::{Controller, Effect};

use super::chd_options_ui::ChdOptionsControl;
use super::effects::{EffectHost, Reply};

pub struct ConvertTab {
    ctl: ConvertController,
    effects: EffectHost,
    pending_fx: Vec<Effect>,
    chd_hd: ChdOptionsControl,
    chd_cd: ChdOptionsControl,
    chd_dvd: ChdOptionsControl,
}

impl Default for ConvertTab {
    fn default() -> Self {
        ConvertTab {
            ctl: ConvertController::new(Arc::new(super::ui_logger::EguiWake)),
            effects: EffectHost::default(),
            pending_fx: Vec::new(),
            chd_hd: ChdOptionsControl::new(ChdProfile::Hd),
            chd_cd: ChdOptionsControl::new(ChdProfile::Cd),
            chd_dvd: ChdOptionsControl::new(ChdProfile::Dvd),
        }
    }
}

fn help(ui: &mut egui::Ui, text: &str) {
    ui.indent(text, |ui| {
        ui.label(egui::RichText::new(text).small().weak());
    });
}

impl ConvertTab {
    pub fn is_running(&self) -> bool {
        self.ctl.is_running()
    }

    /// Load a file or folder another tab hands over.
    pub fn open(&mut self, path: PathBuf) {
        self.act(ConvertAction::SetSource(path));
    }

    /// The progress strip's fraction and text while a conversion runs.
    pub fn strip(&self) -> Option<(f32, String)> {
        self.ctl.strip()
    }

    fn act(&mut self, action: ConvertAction) {
        let fx = self.ctl.apply(action);
        self.pending_fx.extend(fx);
    }

    /// Poll the controller and drain its log; call every frame, shown or not.
    pub fn poll(&mut self, log: &mut LogStore) {
        let fx = self.ctl.tick();
        self.pending_fx.extend(fx);
        super::progress::drain_log_lines(self.ctl.take_log(), log);
    }

    fn chd_choices(&self) -> ChdChoices {
        let mut out = ChdChoices::default();
        for (_, profile) in self.ctl.chd_profiles() {
            match profile {
                ChdProfile::Hd => out.hd = Some(self.chd_hd.effective(ChdProfile::Hd)),
                ChdProfile::Cd => out.cd = Some(self.chd_cd.effective(ChdProfile::Cd)),
                ChdProfile::Dvd => out.dvd = Some(self.chd_dvd.effective(ChdProfile::Dvd)),
            }
        }
        out
    }

    pub fn show(&mut self, ui: &mut egui::Ui, log: &mut LogStore, busy_elsewhere: Option<&str>) {
        let fx = std::mem::take(&mut self.pending_fx);
        let mut messages = Vec::new();
        let mut replies = self.effects.run(ui.ctx(), fx, &mut messages);
        replies.extend(self.effects.show(ui.ctx(), "Convert"));
        for m in messages {
            log.info(m);
        }
        for reply in replies {
            match reply {
                Reply::Picked { purpose, path } => {
                    self.act(ConvertAction::Picked { purpose, path })
                }
                Reply::Chose { purpose, choice } => self.act(ConvertAction::Chose {
                    purpose,
                    index: choice,
                }),
                Reply::Confirmed { .. } | Reply::Answered { .. } | Reply::CloseScreen => {}
            }
        }
        let mut actions = Vec::new();
        egui::ScrollArea::vertical()
            .auto_shrink([false; 2])
            .show(ui, |ui| self.draw(ui, busy_elsewhere, &mut actions));
        for a in actions {
            self.act(a);
        }
    }

    fn draw(
        &mut self,
        ui: &mut egui::Ui,
        busy_elsewhere: Option<&str>,
        out: &mut Vec<ConvertAction>,
    ) {
        let running = self.ctl.is_running();
        ui.heading("Convert");
        ui.label(hints::INTRO);
        ui.add_space(8.0);

        ui.add_enabled_ui(!running, |ui| {
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new("Source:").strong());
                ui.label(ConvertController::path_text(self.ctl.source()));
            });
            ui.horizontal(|ui| {
                if ui.button("Choose Image...").clicked() {
                    out.push(ConvertAction::PickSourceFile);
                }
                if ui.button("Choose Folder...").clicked() {
                    out.push(ConvertAction::PickSourceFolder);
                }
                if self.ctl.source().is_some() && ui.button("Clear").clicked() {
                    out.push(ConvertAction::Clear);
                }
            });
            if self.ctl.source_is_folder() {
                let mut rec = self.ctl.recursive();
                if ui.checkbox(&mut rec, "Include subfolders").changed() {
                    out.push(ConvertAction::SetRecursive(rec));
                }
                help(ui, hints::RECURSIVE);
            }
            if self.ctl.source().is_some() {
                ui.add_space(4.0);
                ui.horizontal(|ui| {
                    ui.label(egui::RichText::new(format!("{}:", self.ctl.output_label())).strong());
                    ui.label(ConvertController::path_text(self.ctl.output()));
                    if ui.button("Choose...").clicked() {
                        out.push(ConvertAction::PickOutput);
                    }
                });
                help(ui, self.ctl.output_hint());
            }
            if let Some(snap) = self.ctl.scan_snapshot() {
                if super::progress::show_job(ui, &snap) {
                    out.push(ConvertAction::Cancel);
                }
            }
            if let Some(err) = self.ctl.scan_error() {
                ui.colored_label(super::theme::danger(ui.visuals()), err);
            }

            let kinds = self.ctl.shown_kinds();
            if !kinds.is_empty() {
                ui.add_space(8.0);
                ui.separator();
                for kind in &kinds {
                    self.format_row(ui, *kind, out);
                }
                self.chd_controls(ui);
                if self.ctl.multi_bin_shown() {
                    let mut on = self.ctl.multi_bin();
                    if ui.checkbox(&mut on, "One .bin per track").changed() {
                        out.push(ConvertAction::SetMultiBin(on));
                    }
                    help(ui, hints::MULTI_BIN);
                }
                if kinds.contains(&ImageKind::HardDisk) {
                    self.sizing(ui, out);
                }
                if kinds.contains(&ImageKind::Floppy) {
                    self.floppy_size(ui, out);
                }
                ui.add_space(6.0);
                let mut verify = self.ctl.verify();
                if ui
                    .checkbox(&mut verify, "Verify after converting")
                    .changed()
                {
                    out.push(ConvertAction::SetVerify(verify));
                }
                help(ui, hints::VERIFY);
            }
        });

        if !self.ctl.items().is_empty() {
            ui.add_space(8.0);
            ui.separator();
            self.file_list(ui, running, out);
        }

        ui.add_space(8.0);
        ui.horizontal(|ui| {
            let problem = busy_elsewhere
                .map(rusty_backup::controller::app::busy_elsewhere_notice)
                .or_else(|| self.ctl.start_problem());
            let start = ui
                .add_enabled(
                    problem.is_none() && !running,
                    egui::Button::new(self.ctl.start_label()),
                )
                .on_disabled_hover_text(problem.clone().unwrap_or_default());
            if start.clicked() {
                out.push(ConvertAction::Start(self.chd_choices()));
            }
            if running && ui.button("Cancel").clicked() {
                out.push(ConvertAction::Cancel);
            }
            if let (Some(p), false) = (problem, running) {
                ui.label(egui::RichText::new(p).weak());
            }
        });
        if let Some(snap) = self.ctl.run_snapshot() {
            if super::progress::show_job(ui, &snap) {
                out.push(ConvertAction::Cancel);
            }
        }
    }

    fn format_row(&mut self, ui: &mut egui::Ui, kind: ImageKind, out: &mut Vec<ConvertAction>) {
        let current = self.ctl.format(kind);
        let (format_id, ext_id) = rusty_backup::controller::convert::kind_ids(kind);
        ui.horizontal(|ui| {
            ui.label(egui::RichText::new(format!("{}:", kind.plural())).strong());
            egui::ComboBox::from_id_salt(format_id)
                .selected_text(current.label())
                .width(180.0)
                .show_ui(ui, |ui| {
                    for &f in formats_for(kind) {
                        let why = f.unavailable();
                        let r = ui.add_enabled(
                            why.is_none(),
                            egui::Button::selectable(f == current, f.label()),
                        );
                        let r = match why {
                            Some(w) => r.on_disabled_hover_text(w),
                            None => r.on_hover_text(f.hint()),
                        };
                        if r.clicked() && f != current {
                            out.push(ConvertAction::SetFormat(kind, f));
                        }
                    }
                });
            ui.label("Extension: .");
            let mut ext = self.ctl.extension(kind).to_string();
            let edit = ui
                .add(
                    egui::TextEdit::singleline(&mut ext)
                        .id_salt(ext_id)
                        .desired_width(60.0)
                        .hint_text(current.extension()),
                )
                .on_hover_text(hints::EXTENSION);
            if edit.changed() {
                out.push(ConvertAction::SetExtension(kind, ext.clone()));
            }
            if ext != current.extension() && ui.button("Reset").clicked() {
                out.push(ConvertAction::ResetExtension(kind));
            }
        });
        help(ui, current.hint());
    }

    fn chd_controls(&mut self, ui: &mut egui::Ui) {
        for (kind, profile) in self.ctl.chd_profiles() {
            ui.label(egui::RichText::new(format!("{} CHD options", kind.label())).weak());
            let control = match profile {
                ChdProfile::Hd => &mut self.chd_hd,
                ChdProfile::Cd => &mut self.chd_cd,
                ChdProfile::Dvd => &mut self.chd_dvd,
            };
            let prefix = format!("convert_chd_{}", kind.label());
            super::chd_options_ui::show(ui, prefix.as_str(), profile, control);
        }
    }

    fn sizing(&mut self, ui: &mut egui::Ui, out: &mut Vec<ConvertAction>) {
        ui.add_space(6.0);
        ui.label(egui::RichText::new("Hard disk size:").strong());
        ui.horizontal(|ui| {
            let mut choice = self.ctl.size_choice();
            for c in SizeChoice::ALL {
                ui.radio_value(&mut choice, c, c.label());
            }
            if choice != self.ctl.size_choice() {
                out.push(ConvertAction::SetSizing(choice));
            }
            if choice == SizeChoice::Extra {
                let mut amount = self.ctl.extra_amount();
                if ui
                    .add(egui::DragValue::new(&mut amount).range(0..=1_000_000))
                    .on_hover_text(hints::EXTRA)
                    .changed()
                {
                    out.push(ConvertAction::SetExtraAmount(amount));
                }
                let mut unit = self.ctl.extra_unit();
                egui::ComboBox::from_id_salt(rusty_backup::controller::convert::ids::EXTRA_UNIT)
                    .selected_text(unit.label())
                    .width(50.0)
                    .show_ui(ui, |ui| {
                        for u in ExtraUnit::ALL {
                            ui.selectable_value(&mut unit, u, u.label());
                        }
                    });
                if unit != self.ctl.extra_unit() {
                    out.push(ConvertAction::SetExtraUnit(unit));
                }
            }
        });
        help(ui, hints::SIZING);
        if self.ctl.size_choice() == SizeChoice::Extra {
            help(ui, hints::EXTRA);
        }
        let mut slow = self.ctl.measure_slow();
        ui.add_enabled_ui(self.ctl.measure_slow_enabled(), |ui| {
            if ui
                .checkbox(&mut slow, "Measure slow filesystems too")
                .changed()
            {
                out.push(ConvertAction::SetMeasureSlow(slow));
            }
        });
        help(ui, hints::measure_slow().as_str());
    }

    fn floppy_size(&mut self, ui: &mut egui::Ui, out: &mut Vec<ConvertAction>) {
        ui.add_space(6.0);
        let current = self.ctl.floppy_size();
        ui.horizontal(|ui| {
            ui.label(egui::RichText::new("Floppy size:").strong());
            egui::ComboBox::from_id_salt(rusty_backup::controller::convert::ids::FLOPPY_SIZE)
                .selected_text(current.label())
                .show_ui(ui, |ui| {
                    for s in floppy_size_choices() {
                        if ui.selectable_label(s == current, s.label()).clicked() && s != current {
                            out.push(ConvertAction::SetFloppySize(s));
                        }
                    }
                });
        });
        help(ui, hints::FLOPPY_SIZE);
        if current != FloppySize::Same {
            help(ui, ONLY_FILES);
        }
    }

    fn file_list(&mut self, ui: &mut egui::Ui, running: bool, out: &mut Vec<ConvertAction>) {
        ui.horizontal(|ui| {
            ui.label(self.ctl.summary());
            if self.ctl.items().len() > 1 && !running {
                if ui.button("Select All").clicked() {
                    out.push(ConvertAction::SelectAll);
                }
                if ui.button("Select None").clicked() {
                    out.push(ConvertAction::SelectNone);
                }
            }
        });
        let warn = super::theme::warning(ui.visuals());
        egui::ScrollArea::vertical()
            .id_salt(rusty_backup::controller::convert::ids::FILES)
            .max_height(320.0)
            .auto_shrink([false, true])
            .show(ui, |ui| {
                egui::Grid::new("convert_files")
                    .striped(true)
                    .num_columns(5)
                    .show(ui, |ui| {
                        for h in ["", "File", "Kind", "Read as", "Result"] {
                            ui.label(egui::RichText::new(h).strong());
                        }
                        ui.end_row();
                        for (i, row) in self.ctl.rows().into_iter().enumerate() {
                            let mut on = row.selected;
                            if ui
                                .add_enabled(!running, egui::Checkbox::without_text(&mut on))
                                .changed()
                            {
                                out.push(ConvertAction::SetSelected(i, on));
                            }
                            ui.label(row.name.as_str());
                            ui.label(row.kind.as_str());
                            match &row.note {
                                Some(note) => {
                                    ui.vertical(|ui| {
                                        ui.label(row.detail.as_str());
                                        ui.colored_label(warn, note.as_str());
                                    });
                                }
                                None => {
                                    ui.label(row.detail.as_str());
                                }
                            }
                            if row.skipped {
                                ui.colored_label(warn, row.outcome.as_str());
                            } else {
                                ui.label(row.outcome.as_str());
                            }
                            ui.end_row();
                        }
                    });
            });
    }
}
