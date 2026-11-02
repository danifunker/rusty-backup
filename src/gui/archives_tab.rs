//! Archives tab: browse and extract classic Macintosh archives, a shell over
//! `controller::archives`.
//!
//! Detection is magic-driven, so the picker accepts any file the user points at: classic
//! StuffIt (`SIT!`), StuffIt 5, SEA, BinHex 4.0 (loose `.hqx` carrying a single Mac file *or*
//! BinHex-wrapped SIT / SEA). Read-only: pick an archive, tick entries, extract all or the
//! ticked ones in a fork-preserving container, or mount a single-entry disk image in Inspect.

use std::path::PathBuf;
use std::sync::Arc;

use rusty_backup::controller::archives::{
    ArchivesAction, ArchivesController, ForkFormat, HostRequest,
};
use rusty_backup::controller::log::LogStore;
use rusty_backup::controller::{Controller as _, Effect};

use super::effects::{EffectHost, Reply};

/// A decoded archive payload for the Inspect tab; the guard keeps its temporary file alive.
pub struct PendingInspectOpen {
    pub path: PathBuf,
    pub guard: tempfile::TempDir,
}

pub struct ArchivesTab {
    ctl: ArchivesController,
    effects: EffectHost,
    pending_fx: Vec<Effect>,
    /// MRU of opened archive paths (newest first), mirrored to `config.json`.
    recent_files: Vec<String>,
    /// Queued by "Mount in new Inspect tab"; drained by the app via [`Self::take_pending_inspect_open`].
    pending_inspect_open: Option<PendingInspectOpen>,
}

impl Default for ArchivesTab {
    fn default() -> Self {
        Self {
            ctl: ArchivesController::new(Arc::new(super::ui_logger::EguiWake)),
            effects: EffectHost::default(),
            pending_fx: Vec::new(),
            recent_files: super::load_recent(rusty_backup::update::RecentMode::Archives),
            pending_inspect_open: None,
        }
    }
}

impl ArchivesTab {
    /// Drained by the app: open the payload in Inspect and switch there.
    pub fn take_pending_inspect_open(&mut self) -> Option<PendingInspectOpen> {
        self.pending_inspect_open.take()
    }

    /// Load `path` as the archive (Inspect hands a picked Mac archive over here).
    pub fn open_path(&mut self, path: PathBuf) {
        self.act(ArchivesAction::Open(path));
        self.carry_out_requests();
    }

    /// Drop the in-memory recent list after Settings cleared the persisted one.
    pub(crate) fn clear_recent_files(&mut self) {
        self.recent_files.clear();
    }

    fn act(&mut self, action: ArchivesAction) {
        let fx = self.ctl.apply(action);
        self.pending_fx.extend(fx);
    }

    fn carry_out_requests(&mut self) {
        for req in self.ctl.take_requests() {
            match req {
                HostRequest::RememberRecent(path) => {
                    self.recent_files =
                        super::push_recent(rusty_backup::update::RecentMode::Archives, &path);
                }
                HostRequest::OpenInInspect { path, guard } => {
                    self.pending_inspect_open = Some(PendingInspectOpen { path, guard });
                }
            }
        }
    }

    /// Run the controller's effects and its finished jobs; pickers answer back here.
    fn pump(&mut self, ctx: &egui::Context, log: &mut LogStore) {
        let fx = self.ctl.tick();
        self.pending_fx.extend(fx);
        let fx = std::mem::take(&mut self.pending_fx);
        let mut messages = Vec::new();
        let replies = self.effects.run(ctx, fx, &mut messages);
        for m in messages {
            log.info(m);
        }
        for reply in replies {
            match reply {
                Reply::Picked { purpose, path } => {
                    self.act(ArchivesAction::Picked { purpose, path })
                }
                Reply::Confirmed { .. } | Reply::Answered { .. } | Reply::CloseScreen => {}
            }
        }
        self.carry_out_requests();
        super::progress::drain_log_lines(self.ctl.take_log(), log);
    }

    pub fn show(&mut self, ui: &mut egui::Ui, log: &mut LogStore) {
        self.pump(ui.ctx(), log);
        let running = self.ctl.is_extracting();
        let mut actions: Vec<ArchivesAction> = Vec::new();

        ui.heading("Mac Archives");
        ui.label(
            "Browse and extract classic Mac archives. Detection is magic-driven, \
             so the picker accepts any file: StuffIt (.sit / .sea, classic + StuffIt \
             5) and BinHex 4.0 (loose .hqx or .sit.hqx / .sea.hqx wrappers).",
        );
        ui.separator();

        // Source picker.
        ui.horizontal(|ui| {
            ui.label("Archive:");
            let text = self
                .ctl
                .path()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "(none selected)".to_string());
            ui.label(text);
            if ui
                .add_enabled(!running, egui::Button::new("Browse..."))
                .clicked()
            {
                actions.push(ArchivesAction::Pick);
            }
            if self.ctl.path().is_some()
                && ui
                    .add_enabled(!running, egui::Button::new("Close"))
                    .on_hover_text("Unload the current archive")
                    .clicked()
            {
                actions.push(ArchivesAction::Close);
            }
            if let Some(path) = super::recent_combo(ui, "archives_recent", &self.recent_files, 0) {
                actions.push(ArchivesAction::Open(path));
            }
        });

        if let Some(snap) = self.ctl.load_snapshot() {
            super::progress::show_job(ui, &snap);
        }
        if let Some(err) = self.ctl.load_error() {
            ui.colored_label(
                super::theme::danger(ui.visuals()),
                format!("Error: {}", err.message),
            );
        }

        // Entry list.
        if self.ctl.is_loaded() {
            ui.separator();
            ui.label(format!("{} files in archive", self.ctl.file_count()));

            egui::ScrollArea::vertical()
                .max_height(320.0)
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    egui::Grid::new("archive_entries")
                        .striped(true)
                        .num_columns(5)
                        .spacing([12.0, 2.0])
                        .show(ui, |ui| {
                            ui.strong("Extract");
                            ui.strong("Name");
                            ui.strong("Type/Creator");
                            ui.strong("Size");
                            ui.strong("Method");
                            ui.end_row();

                            for (i, e) in self.ctl.rows().iter().enumerate() {
                                if e.is_dir {
                                    // Folder markers are always extracted, so no checkbox.
                                    ui.label("");
                                    ui.label(format!("[ {} ]", e.path));
                                    ui.label("");
                                    ui.label("");
                                    ui.label("");
                                    ui.end_row();
                                    continue;
                                }
                                let checked = self.ctl.is_selected(i);
                                let mut new_checked = checked;
                                ui.checkbox(&mut new_checked, "");
                                if new_checked != checked {
                                    actions.push(ArchivesAction::SetSelected(i, new_checked));
                                }
                                ui.label(e.path.as_str());
                                ui.label(format!("{} / {}", e.type_code, e.creator));
                                ui.label(human_size(e.size));
                                ui.label(e.method);
                                ui.end_row();
                            }
                        });
                });

            // Extraction controls.
            let selected_file_count = self.ctl.selected_file_count();
            ui.separator();
            ui.horizontal(|ui| {
                ui.label("Extract as:");
                let mut format = self.ctl.format();
                egui::ComboBox::from_id_salt("archive_fork_format")
                    .selected_text(format.label())
                    .show_ui(ui, |ui| {
                        for f in ForkFormat::ALL {
                            ui.selectable_value(&mut format, f, f.label());
                        }
                    });
                if format != self.ctl.format() {
                    actions.push(ArchivesAction::SetFormat(format));
                }

                if ui
                    .add_enabled(!running, egui::Button::new("Extract All..."))
                    .clicked()
                {
                    actions.push(ArchivesAction::ExtractAll);
                }

                let extract_selected_label = format!("Extract Selected... ({selected_file_count})");
                if ui
                    .add_enabled(
                        !running && selected_file_count > 0,
                        egui::Button::new(extract_selected_label),
                    )
                    .on_hover_text(
                        "Extract only the entries with their checkbox ticked. \
                         Folder markers are always extracted so the rebuilt \
                         tree stays rooted.",
                    )
                    .clicked()
                {
                    actions.push(ArchivesAction::ExtractSelected);
                }
            });

            // The archive's single entry decodes to a disk image Inspect can mount.
            if let Some(kind) = self.ctl.mountable() {
                ui.horizontal(|ui| {
                    let label = format!("Mount in new Inspect tab ({})", kind.label());
                    let tooltip = format!(
                        "The archive's contents are themselves a {} image. \
                         Open the decoded payload in a new Inspect tab \
                         instead of extracting it as a file.",
                        kind.label(),
                    );
                    if ui
                        .add_enabled(!running, egui::Button::new(label))
                        .on_hover_text(tooltip)
                        .clicked()
                    {
                        actions.push(ArchivesAction::Mount);
                    }
                });
            }
        }

        // Progress.
        if let Some((done, total, current)) = self.ctl.extract_progress() {
            let frac = if total > 0 {
                done as f32 / total as f32
            } else {
                0.0
            };
            ui.add(egui::ProgressBar::new(frac).text(format!("{done} / {total}  {current}")));
            ui.ctx().request_repaint();
        }

        for a in actions {
            self.act(a);
        }
        self.carry_out_requests();
        super::progress::drain_log_lines(self.ctl.take_log(), log);
    }
}

fn human_size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = bytes as f64;
    let mut u = 0;
    while v >= 1024.0 && u < UNITS.len() - 1 {
        v /= 1024.0;
        u += 1;
    }
    if u == 0 {
        format!("{bytes} B")
    } else {
        format!("{v:.1} {}", UNITS[u])
    }
}
