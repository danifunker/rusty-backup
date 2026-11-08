//! The Optical tab: a shell over `controller::optical`.
//!
//! Draws the source (drive or image file), the disc info, the output form and Start / Cancel,
//! maps them to [`OpticalAction`]s, answers pickers through an [`EffectHost`] and mirrors the
//! run into the shared progress bar. Widget state only: the CHD options control, the remote
//! address being typed, and the recent-files mirror. The disc browser is the Inspect tab's
//! browse view over the browser pane's optical profile, opened on `HostRequest::OpenBrowser`.

use rusty_backup::controller::app::{RecentHandle, RecentMode};
use rusty_backup::controller::log::LogStore;
use rusty_backup::controller::optical::{
    hints, ChdChoice, ChdProfile, HostRequest, OpticalAction, OpticalController, OutputFormat,
    SourceMode, Task,
};
use rusty_backup::controller::{Controller, Effect};

use super::chd_options_ui::{ChdOptionsControl, ChdOptionsMode};
use super::effects::{EffectHost, Reply};
use super::progress::ProgressState;

pub struct OpticalTab {
    ctl: OpticalController,
    effects: EffectHost,
    pending_fx: Vec<Effect>,
    /// MRU of opened disc-image paths (config.json), for the "Recent:" quick-pick row.
    recent_files: Vec<String>,
    /// The app's recent-files store; opens are recorded through it.
    recent: RecentHandle,
    chd_cd_control: ChdOptionsControl,
    /// The "Add remote daemon" address field.
    add_remote_addr: String,
    /// This tab drives the shared progress bar.
    showing_bar: bool,
    /// The disc browser: the shared browse view, `PaneProfile::optical`.
    browser: super::browse_view::BrowseView,
    /// The image "Convert..." hands to the Convert tab, taken by the app.
    pending_convert: Option<std::path::PathBuf>,
}

impl OpticalTab {
    pub fn new(recent: RecentHandle) -> Self {
        let mut chd = ChdOptionsControl::new(ChdProfile::Cd);
        chd.custom = OpticalController::saved_chd_custom(chd.custom.clone());
        Self {
            ctl: OpticalController::new(std::sync::Arc::new(super::ui_logger::EguiWake)),
            effects: EffectHost::default(),
            pending_fx: Vec::new(),
            recent_files: recent.list(RecentMode::Optical),
            recent,
            chd_cd_control: chd,
            add_remote_addr: String::new(),
            showing_bar: false,
            browser: super::browse_view::BrowseView::optical(),
            pending_convert: None,
        }
    }
}

impl OpticalTab {
    fn act(&mut self, action: OpticalAction) {
        let fx = self.ctl.apply(action);
        self.pending_fx.extend(fx);
    }

    /// Scan the drives at start-up; the count reaches the log when the scan ends.
    pub fn take_convert_request(&mut self) -> Option<std::path::PathBuf> {
        self.pending_convert.take()
    }

    pub fn find_drives(&mut self) {
        self.act(OpticalAction::FindDrives);
    }

    /// True while a disc's contents are browsed, so a drop does not replace the open disc.
    pub fn is_browsing(&self) -> bool {
        self.ctl.is_browsing()
    }

    /// Open `path` as the image-file source (a drop, or Inspect's hand-off).
    pub fn open_image(&mut self, path: std::path::PathBuf) {
        self.act(OpticalAction::OpenImage(path));
    }

    /// Drop the recent-files mirror after Settings cleared the persisted list.
    pub(crate) fn clear_recent_files(&mut self) {
        self.recent_files.clear();
    }

    pub fn is_running(&self) -> bool {
        self.ctl.is_running()
    }

    /// Drain the jobs while another tab is showing, so a run keeps reporting.
    pub fn poll_background(&mut self, log: &mut LogStore, progress: &mut ProgressState) {
        let fx = self.ctl.tick();
        self.pending_fx.extend(fx);
        self.mirror(log, progress);
    }

    fn mirror(&mut self, log: &mut LogStore, progress: &mut ProgressState) {
        super::progress::drain_log_lines(self.ctl.take_log(), log);
        for req in self.ctl.take_requests() {
            match req {
                HostRequest::RememberRecent(path) => {
                    self.recent_files =
                        super::push_recent(&self.recent, RecentMode::Optical, &path);
                }
                HostRequest::OpenBrowser(src) => self.browser.open_disc(src),
                HostRequest::CloseBrowser => self.browser.close(),
            }
        }
        match self.ctl.progress() {
            Some(bar) => {
                progress.active = true;
                progress.operation = bar.operation;
                progress.current_bytes = bar.current_bytes;
                progress.total_bytes = bar.total_bytes;
                progress.full_size_bytes = 0;
                progress.record_sample();
                self.showing_bar = true;
            }
            None if self.showing_bar => {
                progress.active = false;
                self.showing_bar = false;
            }
            None => {}
        }
    }

    fn chd_choice(&self) -> Option<ChdChoice> {
        (self.ctl.output_format() == OutputFormat::Chd).then(|| ChdChoice {
            options: self.chd_cd_control.effective(ChdProfile::Cd),
            custom: self.chd_cd_control.mode == ChdOptionsMode::Custom,
        })
    }

    pub fn show(
        &mut self,
        ui: &mut egui::Ui,
        log: &mut LogStore,
        progress: &mut ProgressState,
        busy_elsewhere: Option<&'static str>,
    ) {
        let fx = self.ctl.tick();
        self.pending_fx.extend(fx);
        self.mirror(log, progress);
        let mut actions = Vec::new();
        self.show_add_remote_dialog(ui.ctx(), &mut actions);

        ui.heading("Optical Disc");
        ui.add_space(8.0);

        if let Some(tab) = busy_elsewhere {
            ui.colored_label(
                super::theme::warning(ui.visuals()),
                super::context::busy_elsewhere_notice(tab),
            );
        }
        let controls_enabled = !self.ctl.is_running() && busy_elsewhere.is_none();

        ui.add_enabled_ui(controls_enabled, |ui| self.source_rows(ui, &mut actions));

        ui.add_space(4.0);
        if let Some(snap) = self.ctl.detecting() {
            super::progress::show_job(ui, &snap);
        }
        ui.horizontal(|ui| {
            ui.label("Disc Info:");
            if let Some(info) = self.ctl.disc() {
                ui.label(format!("Format: {}", info.format));
                ui.label(format!("FS: {}", info.filesystem));
                if let Some(label) = &info.volume {
                    ui.label(format!("Volume: {label}"));
                }
                // Physical media only: the drive's capacity, so a full rip's cost shows first.
                if let Some(size) = &info.size {
                    ui.label(format!("Size: {size}"));
                }
                if let Some(g) = &info.game {
                    ui.label(format!("Game: {g}"));
                }
            } else if let Some(err) = self.ctl.disc_error() {
                ui.colored_label(super::theme::danger(ui.visuals()), err);
            } else {
                ui.colored_label(super::theme::muted(ui.visuals()), "No source selected");
            }
        });

        if self.ctl.disc().is_some() {
            ui.horizontal(|ui| {
                ui.add_space(60.0);
                if !self.ctl.is_browsing() && ui.button("Browse Contents").clicked() {
                    actions.push(OpticalAction::BrowseContents);
                }
                if ui.button("Close Disc").clicked() {
                    actions.push(OpticalAction::CloseDisc);
                }
                if self.ctl.can_chd_info() && ui.button("CHD Info").clicked() {
                    actions.push(OpticalAction::ChdInfo);
                }
            });
        }

        #[cfg(feature = "audio")]
        super::optical_audio::show(ui, &self.ctl, &mut actions);

        if let Some(text) = self.ctl.chd_info() {
            let mut open = true;
            let mut buf = text.to_string();
            egui::Window::new("CHD Info")
                .open(&mut open)
                .resizable(true)
                .default_width(560.0)
                .default_height(420.0)
                .show(ui.ctx(), |ui| {
                    egui::ScrollArea::both()
                        .auto_shrink([false, false])
                        .show(ui, |ui| {
                            ui.add(
                                egui::TextEdit::multiline(&mut buf)
                                    .font(egui::TextStyle::Monospace)
                                    .desired_width(f32::INFINITY)
                                    .desired_rows(20),
                            );
                        });
                });
            if !open {
                actions.push(OpticalAction::CloseChdInfo);
            }
        }

        ui.separator();

        if self.ctl.task() == Task::Convert {
            // An image's format changes in the Convert tab; this tab rips drives.
            ui.horizontal(|ui| {
                let src = self.ctl.convert_source();
                if ui
                    .add_enabled(src.is_some(), egui::Button::new("Convert..."))
                    .on_hover_text(rusty_backup::controller::optical::hints::CONVERT)
                    .clicked()
                {
                    self.pending_convert = src;
                }
                ui.label(
                    egui::RichText::new(rusty_backup::controller::optical::hints::CONVERT_MOVED)
                        .weak(),
                );
            });
        } else {
            ui.add_enabled_ui(controls_enabled, |ui| self.output_rows(ui, &mut actions));

            ui.add_space(8.0);

            ui.horizontal(|ui| {
                if !self.ctl.is_running() {
                    let label = self.ctl.start_label();
                    if ui
                        .add_enabled(self.ctl.can_start(), egui::Button::new(label))
                        .clicked()
                    {
                        actions.push(OpticalAction::Start(self.chd_choice()));
                    }
                } else if ui.button("Cancel").clicked() {
                    actions.push(OpticalAction::Cancel);
                }
            });
        }

        ui.separator();

        self.browser.show(ui);
        if self.ctl.is_browsing() && !self.browser.is_active() {
            actions.push(OpticalAction::BrowserClosed);
        }

        for a in actions {
            self.act(a);
        }
        self.run_effects(ui.ctx(), log);
        self.mirror(log, progress);
    }

    fn source_rows(&mut self, ui: &mut egui::Ui, actions: &mut Vec<OpticalAction>) {
        ui.horizontal(|ui| {
            ui.label("Source:");
            // A local drive needs elevation (SCSI pass-through); a remote drive is the daemon's to open.
            let phys_enabled = super::physical_devices_available() || self.ctl.has_remote_daemons();
            let mode = self.ctl.source_mode();
            ui.add_enabled_ui(phys_enabled, |ui| {
                if ui
                    .radio(mode == SourceMode::PhysicalDrive, "Physical drive")
                    .on_disabled_hover_text(hints::PHYSICAL_DRIVE)
                    .clicked()
                {
                    actions.push(OpticalAction::SetSourceMode(SourceMode::PhysicalDrive));
                }
            });
            if ui
                .radio(mode == SourceMode::ImageFile, "Image file")
                .clicked()
            {
                actions.push(OpticalAction::SetSourceMode(SourceMode::ImageFile));
            }
            if ui.button("Add remote daemon...").clicked() {
                actions.push(OpticalAction::OpenAddRemote);
            }
        });

        match self.ctl.source_mode() {
            SourceMode::PhysicalDrive => {
                ui.horizontal(|ui| {
                    ui.add_space(60.0);
                    let labels = self.ctl.drive_labels();
                    let selected = self.ctl.selected_drive();
                    let current = selected
                        .and_then(|i| labels.get(i).cloned())
                        .unwrap_or_else(|| "Select a drive...".into());
                    egui::ComboBox::from_id_salt("optical_drive")
                        .selected_text(&current)
                        .width(360.0)
                        .show_ui(ui, |ui| {
                            for (i, label) in labels.iter().enumerate() {
                                if ui.selectable_label(selected == Some(i), label).clicked() {
                                    actions.push(OpticalAction::SelectDrive(Some(i)));
                                }
                            }
                        });
                    if self.ctl.scanning_drives() {
                        ui.spinner();
                    } else if ui.button("Refresh").clicked() {
                        actions.push(OpticalAction::RefreshDrives);
                    }
                    // Remote drives are the daemon's to eject; an empty drive has no device path.
                    ui.add_enabled_ui(self.ctl.can_eject(), |ui| {
                        if ui
                            .button("Eject")
                            .on_disabled_hover_text(hints::EJECT)
                            .clicked()
                        {
                            actions.push(OpticalAction::Eject);
                        }
                    });
                });
            }
            SourceMode::ImageFile => {
                ui.horizontal(|ui| {
                    ui.add_space(60.0);
                    let path_text = self
                        .ctl
                        .image()
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|| "No file selected".into());
                    ui.label(&path_text);
                    if ui.button("Browse...").clicked() {
                        actions.push(OpticalAction::PickImage);
                    }
                    if let Some(path) =
                        super::recent_combo(ui, "optical_recent", &self.recent_files, 5)
                    {
                        actions.push(OpticalAction::OpenImage(path));
                    }
                });
            }
        }
    }

    fn output_rows(&mut self, ui: &mut egui::Ui, actions: &mut Vec<OpticalAction>) {
        ui.horizontal(|ui| {
            ui.label("Action:");
            let _ = ui.radio(true, self.ctl.task_label());
        });

        ui.horizontal(|ui| {
            ui.label("Output:");
            let current = self.ctl.output_format();
            for (fmt, enabled) in self.ctl.format_choices() {
                let response =
                    ui.add_enabled(enabled, egui::RadioButton::new(current == fmt, fmt.label()));
                if response.clicked() {
                    actions.push(OpticalAction::SetOutputFormat(fmt));
                }
            }
        });

        if self.ctl.output_format() == OutputFormat::Chd {
            super::chd_options_ui::show(
                ui,
                "optical_chd",
                ChdProfile::Cd,
                &mut self.chd_cd_control,
            );
        }

        ui.horizontal(|ui| {
            ui.add_space(60.0);
            let path_text = self
                .ctl
                .output()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "No output path selected".into());
            ui.label(&path_text);
            if ui.button("Browse...").clicked() {
                actions.push(OpticalAction::PickOutput);
            }
        });

        if self.ctl.task() == Task::Rip {
            ui.horizontal(|ui| {
                ui.add_space(60.0);
                let mut eject = self.ctl.eject_after();
                if ui
                    .checkbox(&mut eject, "Eject after ripping")
                    .on_hover_text(hints::EJECT_AFTER)
                    .changed()
                {
                    actions.push(OpticalAction::SetEjectAfter(eject));
                }
            });
        }
    }

    /// The "Add remote daemon" window: an address field, Connect / Cancel and the recent addresses.
    fn show_add_remote_dialog(&mut self, ctx: &egui::Context, actions: &mut Vec<OpticalAction>) {
        if !self.ctl.add_remote_open() {
            self.add_remote_addr.clear();
            return;
        }
        let connecting = self.ctl.remote_connecting().is_some();
        let mut open = true;
        egui::Window::new("Add remote daemon")
            .collapsible(false)
            .resizable(false)
            .open(&mut open)
            .show(ctx, |ui| {
                ui.label("Daemon address (host:port):");
                let resp = ui.add_enabled(
                    !connecting,
                    egui::TextEdit::singleline(&mut self.add_remote_addr)
                        .hint_text("mister.local:7341"),
                );
                let submit = resp.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
                ui.horizontal(|ui| {
                    if ui
                        .add_enabled(!connecting, egui::Button::new("Connect"))
                        .clicked()
                        || (submit && !connecting)
                    {
                        actions.push(OpticalAction::ConnectRemote(self.add_remote_addr.clone()));
                    }
                    if ui.button("Cancel").clicked() {
                        actions.push(OpticalAction::CloseAddRemote);
                    }
                });
                if connecting {
                    ui.horizontal(|ui| {
                        ui.spinner();
                        ui.label("Connecting...");
                    });
                }
                if let Some(err) = self.ctl.add_remote_error() {
                    ui.colored_label(super::theme::danger(ui.visuals()), err);
                }
                if !self.ctl.recent_daemons().is_empty() {
                    ui.separator();
                    ui.label("Recent:");
                    for addr in self.ctl.recent_daemons() {
                        if ui
                            .add_enabled(!connecting, egui::Button::new(addr.as_str()).small())
                            .clicked()
                        {
                            self.add_remote_addr = addr.clone();
                            actions.push(OpticalAction::ConnectRemote(addr.clone()));
                        }
                    }
                }
            });
        if !open {
            actions.push(OpticalAction::CloseAddRemote);
        }
    }

    fn run_effects(&mut self, ctx: &egui::Context, log: &mut LogStore) {
        let fx = std::mem::take(&mut self.pending_fx);
        let mut messages = Vec::new();
        let mut replies = self.effects.run(ctx, fx, &mut messages);
        replies.extend(self.effects.show(ctx, "Optical"));
        for msg in messages {
            log.info(msg);
        }
        for reply in replies {
            match reply {
                Reply::Picked { purpose, path } => {
                    self.act(OpticalAction::Picked { purpose, path })
                }
                Reply::Confirmed { purpose, yes } => {
                    self.act(OpticalAction::Confirmed { purpose, yes })
                }
                Reply::Answered { .. } | Reply::Chose { .. } | Reply::CloseScreen => {}
            }
        }
    }
}
