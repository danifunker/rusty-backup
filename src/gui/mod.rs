mod alto_resize_dialog;
mod archives_tab;
mod backup_tab;
mod browse_view;
mod bulk_convert_dialog;
mod chd_options_ui;
mod commander;
mod context;
mod effects;
mod elevation_dialog;
mod expand_hfs_dialog;
mod file_detail;
mod floppy_convert_dialog;
pub mod fonts;
mod inspect_tab;
mod journal_view;
mod metadata_editor;
#[cfg(feature = "audio")]
mod optical_audio;
mod optical_tab;
mod partition_bar;
mod partition_editor_modal;
mod physical_disk_export;
mod progress;
mod remote_browser;
mod resize_popup;
mod restore_tab;
mod settings_dialog;
mod size_mode_row;
mod source_picker;
mod squashfs_budget_dialog;
pub(crate) mod theme;
pub mod ui_logger;

use archives_tab::ArchivesTab;
use backup_tab::BackupTab;
use bulk_convert_dialog::BulkConvertDialog;
use inspect_tab::InspectTab;
use optical_tab::OpticalTab;
use progress::ProgressState;
use restore_tab::RestoreTab;
use rusty_backup::controller::app::{
    busy_elsewhere, elevation_button_hover, elevation_button_label, elevation_gate, AppAction,
    AppController, AppTab, DropRoute, DropTargets, ElevationGate, InstallState, ShareStep,
    UpdateInfo,
};
use rusty_backup::controller::{Controller, Effect};
use rusty_backup::update::RecentMode;
use settings_dialog::SettingsDialog;
use std::path::PathBuf;

#[cfg(target_os = "linux")]
use elevation_dialog::{ElevationAction, ElevationDialog};

use std::sync::Arc;

pub(crate) use rusty_backup::controller::app::{
    no_devices_hint, physical_devices_available, unelevated_device_note,
};

/// Carry out view-model effects; no `_` arm, so a new `Effect` fails to compile here until handled.
pub(crate) fn run_effects(ctx: &egui::Context, effects: Vec<Effect>) {
    for effect in effects {
        match effect {
            Effect::CopyToClipboard(text) => ctx.copy_text(text),
            Effect::OpenUrl(url) => ctx.open_url(egui::OpenUrl::new_tab(url)),
            Effect::Message(text) => log::info!("{text}"),
            // Answered effects go through the asking controller's `effects::EffectHost`.
            Effect::PickFile(spec) | Effect::PickFolder(spec) => {
                log::error!("picker {:?} has no EffectHost to answer it", spec.purpose)
            }
            Effect::Confirm(spec) => {
                log::error!("confirm {:?} has no EffectHost to answer it", spec.purpose)
            }
            Effect::AskPassword(spec) => {
                log::error!("no egui handler yet for password {:?}", spec.purpose)
            }
            Effect::AskText(spec) => {
                log::error!(
                    "text prompt {:?} has no EffectHost to answer it",
                    spec.purpose
                )
            }
            Effect::CloseScreen => log::error!("CloseScreen has no screen to close here"),
            Effect::Quit => ctx.send_viewport_cmd(egui::ViewportCommand::Close),
        }
    }
}

/// Record `path` in the per-mode recent-files MRU (persisted in config.json)
/// and return the updated newest-first list for `mode`. Mirrors the Optical
/// tab's `remember_daemon_addr` write-through: config.json is the source of
/// truth and each tab keeps an in-memory mirror it refreshes from this return
/// value. Loading + saving on every open is cheap (a small JSON file) and keeps
/// the history durable across runs without an eframe `save()` hook.
pub(crate) fn push_recent(mode: RecentMode, path: &std::path::Path) -> Vec<String> {
    rusty_backup::update::push_recent(mode, &path.display().to_string())
}

/// The persisted recent-files list for `mode` (newest first). Used to seed a
/// tab's in-memory mirror at construction.
pub(crate) fn load_recent(mode: RecentMode) -> Vec<String> {
    rusty_backup::update::load_recent(mode)
}

/// A dedicated "Recent" dropdown (its own popup, so it never gets clipped inside
/// the source-picker combo). Lists each entry's basename with the full path on
/// hover; returns the picked path. Renders nothing for an empty list. `id_salt`
/// keys the ComboBox (callers that draw more than one per frame must differ).
///
/// `slots`: when non-zero, always draw exactly that many rows — the newest
/// entries fill them and the rest are dimmed placeholders — so the popup's
/// height never changes as the list grows, dodging egui's cached-popup-size bug
/// (see [`source_picker`]'s `recent_slots`). `0` draws one row per entry
/// (variable height). Either way the whole combo is hidden when there are no
/// recents, so an empty list never shows a dropdown of blanks.
pub(crate) fn recent_combo(
    ui: &mut egui::Ui,
    id_salt: &str,
    recent: &[String],
    slots: usize,
) -> Option<PathBuf> {
    if recent.is_empty() {
        return None;
    }
    let mut chosen = None;
    let row = |ui: &mut egui::Ui, chosen: &mut Option<PathBuf>, p: &str| {
        let label = std::path::Path::new(p)
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| p.to_string());
        if ui.selectable_label(false, label).on_hover_text(p).clicked() {
            *chosen = Some(PathBuf::from(p));
        }
    };
    egui::ComboBox::from_id_salt(id_salt)
        .selected_text("Recent...")
        .width(240.0)
        .height(400.0)
        .show_ui(ui, |ui| {
            if slots > 0 {
                for slot in 0..slots {
                    match recent.get(slot) {
                        Some(p) => row(ui, &mut chosen, p),
                        // Placeholder keeps the row count (and popup height) fixed.
                        None => {
                            ui.add_enabled(
                                false,
                                egui::Button::selectable(false, egui::RichText::new("-").weak()),
                            );
                        }
                    }
                }
            } else {
                for p in recent {
                    row(ui, &mut chosen, p);
                }
            }
        });
    chosen
}

/// An rfd dialog starting in the real user's home, even when running elevated through pkexec.
fn file_dialog() -> rfd::FileDialog {
    let dialog = rfd::FileDialog::new();
    match rusty_backup::controller::app::picker_start_folder() {
        Some(home) => dialog.set_directory(home),
        None => dialog,
    }
}

/// Main application state.
pub struct RustyBackupApp {
    backup_tab: BackupTab,
    restore_tab: RestoreTab,
    inspect_tab: InspectTab,
    optical_tab: OpticalTab,
    archives_tab: ArchivesTab,
    /// App-wide view-model: the log, the jobs, recent files, the tab, devices and updates.
    app: AppController,
    progress: ProgressState,
    settings_dialog: SettingsDialog,
    #[cfg(target_os = "linux")]
    elevation_dialog: ElevationDialog,
    /// Stylized shield texture for the "Show Physical Devices" elevation button.
    /// Loaded lazily on first use (the egui context isn't available at
    /// construction time).
    shield_texture: Option<egui::TextureHandle>,
    /// The Bulk Convert dialog and its running conversion.
    bulk_convert: BulkConvertDialog,
    /// File path passed on the command line (Windows file-association
    /// double-click, macOS `open file.d88`, Linux `.desktop` MimeType
    /// launch). Taken on the first `logic()` tick, routed into the
    /// Inspect tab, and the tab is switched to it. `None` for normal
    /// launches without a file argv.
    pending_initial_image: Option<PathBuf>,
    /// Full-page Commander Mode overlay. `Some` while it is open; it then
    /// takes over the whole frame and the tab strip is not drawn.
    commander: Option<commander::CommanderMode>,
}

impl Default for RustyBackupApp {
    fn default() -> Self {
        let mut app = AppController::new(Arc::new(ui_logger::EguiWake));
        app.log.info("Rusty Backup started");
        app.load_devices();
        app.start_update_check();

        // The scan runs in a job; "Found N optical drive(s)" is logged when it ends.
        let mut optical_tab = OpticalTab::default();
        optical_tab.find_drives();

        Self {
            backup_tab: BackupTab::default(),
            restore_tab: RestoreTab::default(),
            inspect_tab: InspectTab::default(),
            optical_tab,
            archives_tab: ArchivesTab::default(),
            app,
            progress: ProgressState::default(),
            settings_dialog: SettingsDialog::default(),
            #[cfg(target_os = "linux")]
            elevation_dialog: ElevationDialog::default(),
            shield_texture: None,
            bulk_convert: BulkConvertDialog::default(),
            pending_initial_image: None,
            commander: None,
        }
    }
}

impl RustyBackupApp {
    /// Construct the app with an optional file path to auto-open on first
    /// frame. Used by `main.rs` to wire up Windows file-association
    /// double-clicks, macOS `open file.ext` launches, and Linux .desktop
    /// MimeType handlers — all of them launch the binary with the file
    /// path in argv.
    pub fn with_initial_image(image: Option<PathBuf>) -> Self {
        // Struct-update syntax keeps `field_reassign_with_default` happy:
        // build everything through Default, then override the one field
        // we care about. Default does the heavy device-enumeration + log
        // priming so we don't want to duplicate it.
        Self {
            pending_initial_image: image,
            ..Self::default()
        }
    }

    /// Open a path in the Inspect tab and switch to it: the command-line hand-off and drops.
    /// A wrapper (`.adz`, `.hdz`, ...) is decoded in the Inspect open's job, as Open File does.
    fn open_in_inspect(&mut self, path: PathBuf) {
        self.app.log.info(format!("Opening {}", path.display()));
        self.inspect_tab.open_file(path);
        self.app.select_tab(AppTab::Inspect);
    }
}

impl RustyBackupApp {
    /// Lazily load (once) and return the stylized shield texture for the
    /// elevation button.
    fn shield_texture(&mut self, ctx: &egui::Context) -> egui::TextureHandle {
        if let Some(tex) = &self.shield_texture {
            return tex.clone();
        }
        let rgba = image::load_from_memory(include_bytes!("../../assets/icons/shield.png"))
            .expect("embedded shield.png is valid")
            .to_rgba8();
        let size = [rgba.width() as usize, rgba.height() as usize];
        let color_image = egui::ColorImage::from_rgba_unmultiplied(size, rgba.as_raw());
        let tex = ctx.load_texture("shield", color_image, egui::TextureOptions::LINEAR);
        self.shield_texture = Some(tex.clone());
        tex
    }

    /// Render the update-banner action buttons. On Windows with a matched
    /// release asset this offers in-app "Download & Install Update" (download
    /// progress -> "Restart now"); otherwise (other platforms, unmatched arch,
    /// or a missing asset) it opens the releases page in a browser.
    fn show_update_actions(&mut self, ui: &mut egui::Ui, info: &UpdateInfo) {
        let mut actions = Vec::new();
        let mut view_release = false;
        if self.app.can_install_update() {
            match self.app.install_state() {
                None => {
                    if ui.button("Download & Install Update").clicked() {
                        actions.push(AppAction::InstallUpdate);
                    }
                    view_release = ui.button("View Release").clicked();
                }
                Some(InstallState::Downloading(frac)) => {
                    ui.add(egui::Spinner::new());
                    if let Some(f) = frac {
                        ui.add(
                            egui::ProgressBar::new(f)
                                .desired_width(140.0)
                                .show_percentage(),
                        );
                    } else {
                        ui.label("Downloading update...");
                    }
                    ui.ctx().request_repaint();
                }
                Some(InstallState::Ready) => {
                    ui.label(
                        egui::RichText::new("Update installed.")
                            .color(theme::success(ui.visuals())),
                    );
                    if ui.button("Restart now").clicked() {
                        actions.push(AppAction::RestartApp);
                    }
                }
                Some(InstallState::Failed(e)) => {
                    ui.label(
                        egui::RichText::new(format!("Update failed: {e}"))
                            .color(theme::danger(ui.visuals())),
                    );
                    if ui.button("Retry").clicked() {
                        actions.push(AppAction::RetryUpdate);
                    }
                    view_release = ui.button("View Release").clicked();
                }
            }
        } else {
            view_release = ui.button("View Release").clicked();
        }
        if view_release {
            let _ = webbrowser::open(&info.releases_url);
        }
        for a in actions {
            let fx = self.app.apply(a);
            run_effects(ui.ctx(), fx);
        }
    }
}

impl RustyBackupApp {
    /// Another tab (not `tab`) with a disk job in flight; one job at a time
    /// keeps two drives from being hammered and the shared progress bar honest.
    fn busy_elsewhere(&self, tab: AppTab) -> Option<&'static str> {
        let on = |running: bool, name: &'static str| running.then_some(name);
        let running = [
            (AppTab::Backup, on(self.backup_tab.is_running(), "Backup")),
            (
                AppTab::Restore,
                on(self.restore_tab.is_running(), "Restore"),
            ),
            (
                AppTab::Optical,
                on(self.optical_tab.is_running(), "Optical"),
            ),
            (AppTab::Inspect, self.inspect_tab.running_job()),
        ];
        busy_elsewhere(tab, &running, self.bulk_convert.is_running())
    }
}

/// A dropped file's real filesystem path; egui reports an empty one for byte-only drops.
fn non_empty_path(path: &std::path::Path) -> Option<PathBuf> {
    (!path.as_os_str().is_empty()).then(|| path.to_path_buf())
}

impl eframe::App for RustyBackupApp {
    fn logic(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // Request repaint while backup/restore is running so progress updates are shown
        if self.progress.active
            || self.backup_tab.is_running()
            || self.restore_tab.is_running()
            || self.optical_tab.is_running()
            || self.bulk_convert.is_running()
        {
            ctx.request_repaint();
        }

        // Closing the window with staged Commander edits asks first; a yes closes it again.
        if ctx.input(|i| i.viewport().close_requested()) {
            if let Some(cmd) = self.commander.as_mut() {
                if cmd.hold_quit(ctx) {
                    ctx.send_viewport_cmd(egui::ViewportCommand::CancelClose);
                }
            }
        }

        // One-shot: drain the file path handed in on the command line
        // (Windows file-association double-click, macOS `open ...`, Linux
        // .desktop MimeType launch). Done in logic() rather than the
        // constructor because the inspect tab's lazy-load path needs the
        // egui context to be alive.
        if let Some(path) = self.pending_initial_image.take() {
            self.open_in_inspect(path);
        }

        // Drag-and-drop: route the first dropped file with a real filesystem
        // path to the *currently active* mode. `with_drag_and_drop(true)` is set
        // on the viewport in main.rs so eframe surfaces these events. This
        // reads (does not consume) `dropped_files`, exactly like the per-volume
        // browse handlers, so a drop into a loaded volume still reaches that
        // volume's own edit handler where applicable.
        //
        // Historically this always opened in Inspect, which made drag-and-drop
        // useless for the Optical / Archives / Restore tabs and Commander.
        let dropped: Option<PathBuf> = ctx.input(|i| {
            i.raw
                .dropped_files
                .iter()
                .find_map(|f| non_empty_path(f.path()))
        });
        if let Some(path) = dropped {
            if let Some(cmd) = self.commander.as_mut() {
                // Commander takes over the frame; the active pane opens the drop.
                cmd.open_dropped_path(ctx, path);
            } else {
                let targets = DropTargets {
                    inspect_remote: self.inspect_tab.is_remote_session(),
                    inspect_loaded: self.inspect_tab.has_loaded_source(),
                    optical_browsing: self.optical_tab.is_browsing(),
                };
                match self.app.route_drop(path, targets) {
                    DropRoute::Inspect(path) => self.open_in_inspect(path),
                    DropRoute::Optical(path) => self.optical_tab.open_image(path),
                    DropRoute::Archives(path) => self.archives_tab.open_path(path),
                    DropRoute::Restore(path) => self.restore_tab.open_dropped(path),
                    DropRoute::Backup(path) => self.backup_tab.open_image(path),
                    DropRoute::Ignore => {}
                }
            }
        }

        // Drains worker-thread `log::info!` records (the GHO reader's scan, say) into the log panel.
        // A finished Refresh Devices replaces the list; a finished update check sets the banner.
        let effects = self.app.tick();
        run_effects(ctx, effects);

        // Drain bulk-convert worker logs into the panel.
        self.bulk_convert.poll(&mut self.app.log);
    }

    fn ui(&mut self, ctx: &mut egui::Ui, _frame: &mut eframe::Frame) {
        // Full-page Commander Mode overlay: when open it takes over the whole
        // frame and the normal tab UI is not drawn.
        if self.commander.is_some() {
            let close = self.commander.as_mut().unwrap().show(ctx);
            if close {
                self.commander = None;
            }
            return;
        }

        // Top panel: tab bar
        egui::Panel::top("tab_bar").show(ctx, |ui| {
            ui.horizontal(|ui| {
                let mut tab = self.app.active_tab();
                ui.selectable_value(&mut tab, AppTab::Backup, "Backup");
                ui.selectable_value(&mut tab, AppTab::Restore, "Restore");
                ui.selectable_value(&mut tab, AppTab::Inspect, "Inspect");
                ui.selectable_value(&mut tab, AppTab::Optical, "Optical");
                ui.selectable_value(&mut tab, AppTab::Archives, "Archives");
                if tab != self.app.active_tab() {
                    self.app.apply(AppAction::SelectTab(tab));
                }
                ui.separator();
                if ui.button("Commander Mode").clicked() {
                    self.commander = Some(commander::CommanderMode::new(self.app.jobs().clone()));
                }

                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    // Version display
                    ui.label(format!("v{}", env!("APP_VERSION")));
                    ui.separator();

                    // Close Backup button (if backup loaded)
                    if self.app.shared_backup().is_some() {
                        if ui.button("Close Backup").clicked() {
                            self.app.close_shared_backup();
                            self.restore_tab.clear_backup();
                            self.inspect_tab.clear_backup();
                            self.app.log.info("Backup closed");
                        }
                        ui.separator();
                    }

                    if ui.button("Settings").clicked() {
                        self.settings_dialog.open_dialog();
                    }
                    ui.separator();

                    // The single global gate for physical devices: the GUI always
                    // launches un-elevated, and this relaunches it via UAC/pkexec.
                    match elevation_gate() {
                        ElevationGate::NeedsElevation => {
                            let tint = ui.visuals().widgets.active.fg_stroke.color;
                            let shield = self.shield_texture(ui.ctx());
                            let icon = egui::Image::from_texture(
                                egui::load::SizedTexture::from_handle(&shield),
                            )
                            .tint(tint)
                            .max_height(14.0);
                            let resp = ui
                                .add(egui::Button::image_and_text(
                                    icon,
                                    egui::RichText::new(elevation_button_label())
                                        .color(theme::warning(ui.visuals())),
                                ))
                                .on_hover_text(elevation_button_hover());
                            if resp.clicked() {
                                self.app.apply(AppAction::RequestElevation);
                            }
                            ui.separator();
                        }
                        ElevationGate::Disabled => {
                            ui.label(egui::RichText::new("Image files only").weak())
                                .on_hover_text(
                                    rusty_backup::controller::app::PHYSICAL_DEVICES_DISABLED,
                                );
                            ui.separator();
                        }
                        ElevationGate::NotApplicable | ElevationGate::Elevated => {
                            if self.app.devices_refreshing().is_some() {
                                ui.spinner();
                                ui.label("Looking for devices...");
                            } else if ui.button("Refresh Devices").clicked() {
                                self.app.apply(AppAction::RefreshDevices);
                            }
                        }
                    }
                    ui.separator();

                    // Bulk Convert — convert every disk image in a folder.
                    let bulk_running = self.bulk_convert.is_running();
                    if ui
                        .add_enabled(!bulk_running, egui::Button::new("Bulk Convert..."))
                        .on_hover_text(
                            "Convert every disk image in a folder to one chosen format, \
                             using the same parameters for every file.",
                        )
                        .clicked()
                    {
                        self.bulk_convert.open();
                    }
                    if bulk_running && ui.button("Cancel Bulk").clicked() {
                        self.bulk_convert.cancel();
                    }
                });
            });
        });

        // Bulk Convert dialog (modal-ish window, always reachable).
        self.bulk_convert.show(ctx, &mut self.app.log);

        // Bulk Convert progress strip, visible from any tab while running.
        if let Some((frac, text)) = self.bulk_convert.strip() {
            egui::Panel::top("bulk_convert_progress").show(ctx, |ui| {
                ui.add(egui::ProgressBar::new(frac).text(text));
            });
        }

        // Update notification banner (if update available and not dismissed).
        if let Some(info) = self.app.update_banner().cloned() {
            egui::Panel::top("update_banner").show(ctx, |ui| {
                ui.horizontal(|ui| {
                    ui.label(
                        egui::RichText::new("Update")
                            .color(theme::warning(ui.visuals()))
                            .strong(),
                    );
                    ui.label(format!(
                        "available: v{} -> v{}",
                        info.current_version, info.latest_version
                    ));
                    self.show_update_actions(ui, &info);
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if ui.button("Dismiss update notification").clicked() {
                            self.app.apply(AppAction::DismissUpdate);
                        }
                    });
                });
            });
        }
        // (end update banner)

        // Elevation dialog (Linux)
        #[cfg(target_os = "linux")]
        {
            // Linux asks first: the relaunch discards any unsaved edits.
            if self.app.take_elevation_prompt() {
                self.elevation_dialog.open();
            }
            match self.elevation_dialog.show(ctx) {
                ElevationAction::Elevate => {
                    self.app.apply(AppAction::ConfirmElevation(true));
                }
                ElevationAction::Cancel => {
                    self.app.apply(AppAction::ConfirmElevation(false));
                }
                ElevationAction::None => {}
            }
        }

        // Bottom panel: progress + log
        egui::Panel::bottom("log_panel")
            .resizable(true)
            .min_size(100.0)
            .default_size(180.0)
            .show(ctx, |ui| {
                self.progress.show(ui);
                if self.progress.active {
                    ui.separator();
                }
                progress::show_log(ui, &mut self.app);
            });

        // Poll the tabs that are not showing: a job keeps reporting on the
        // shared progress bar and its completion is not missed after a switch.
        let active = self.app.active_tab();
        {
            let (devices, log) = self.app.devices_and_log();
            let mut ctx = context::TabContext::new(devices, log);
            if active != AppTab::Backup {
                self.backup_tab
                    .poll_background(&mut ctx, &mut self.progress);
            }
            if active != AppTab::Restore {
                self.restore_tab
                    .poll_background(&mut ctx, &mut self.progress);
            }
        }
        if active != AppTab::Optical {
            self.optical_tab
                .poll_background(&mut self.app.log, &mut self.progress);
        }
        if active != AppTab::Inspect {
            let (devices, log) = self.app.devices_and_log();
            let mut ctx = context::TabContext::new(devices, log);
            self.inspect_tab.poll_background(&mut ctx);
        }

        // Central panel: active tab content
        egui::CentralPanel::default().show(ctx, |ui| match active {
            AppTab::Backup => {
                let busy = self.busy_elsewhere(AppTab::Backup);
                let (devices, log) = self.app.devices_and_log();
                let mut ctx = context::TabContext::new(devices, log).busy_elsewhere(busy);
                self.backup_tab.show(ui, &mut ctx, &mut self.progress);
            }
            AppTab::Restore => {
                // Share the backup folder: publish a new one, or adopt it only when empty, so a
                // tab the user pointed at an image does not have it re-installed each frame.
                let empty = !self.restore_tab.has_backup() && !self.restore_tab.has_other_source();
                match self
                    .app
                    .share_backup(self.restore_tab.get_loaded_backup(), empty)
                {
                    ShareStep::Publish(folder) => self.inspect_tab.load_backup(&folder),
                    ShareStep::Adopt(folder) => self.restore_tab.load_backup(&folder),
                    ShareStep::Nothing => {}
                }

                let busy = self.busy_elsewhere(AppTab::Restore);
                let (devices, log) = self.app.devices_and_log();
                let mut ctx = context::TabContext::new(devices, log).busy_elsewhere(busy);
                self.restore_tab.show(ui, &mut ctx, &mut self.progress);
            }
            AppTab::Inspect => {
                // The tab's own Close Backup closes it everywhere, before the share below
                // could re-load it from the shared folder on the next frame.
                if self.inspect_tab.take_close_backup_request() {
                    self.app.close_shared_backup();
                    self.restore_tab.clear_backup();
                }

                // Same rule: an image or device open in Inspect stays open.
                let empty = !self.inspect_tab.has_backup() && !self.inspect_tab.has_loaded_source();
                match self
                    .app
                    .share_backup(self.inspect_tab.get_loaded_backup(), empty)
                {
                    ShareStep::Publish(folder) => self.restore_tab.load_backup(&folder),
                    ShareStep::Adopt(folder) => self.inspect_tab.load_backup(&folder),
                    ShareStep::Nothing => {}
                }

                let (devices, log) = self.app.devices_and_log();
                let mut ctx = context::TabContext::new(devices, log);
                self.inspect_tab.show(ui, &mut ctx);
            }
            AppTab::Optical => {
                let busy = self.busy_elsewhere(AppTab::Optical);
                self.optical_tab
                    .show(ui, &mut self.app.log, &mut self.progress, busy);
            }
            AppTab::Archives => {
                self.archives_tab.show(ui, &mut self.app.log);
            }
        });

        // Workflow D.2 auto-unwrap: the Archives tab queues a payload
        // when the user clicks "Mount in new Inspect tab". Drain here,
        // hand it to the Inspect tab (which owns the tempdir guard so
        // the file survives), and switch tabs.
        if let Some(open) = self.archives_tab.take_pending_inspect_open() {
            self.inspect_tab
                .load_image_with_tempdir(open.path, Some(open.guard));
            self.app.select_tab(AppTab::Inspect);
        }

        // The reverse: a Mac archive picked in the Inspect source dropdown isn't
        // a disk image, so hand it to the Archives tab and switch there.
        if let Some(path) = self.inspect_tab.take_open_archive_request() {
            self.archives_tab.open_path(path);
            self.app.select_tab(AppTab::Archives);
        }

        // Likewise, an optical disc opened in Inspect (an ISO 9660 CD, a hybrid
        // Mac disc, an NKit v1 GameCube image, ...) has no partition table — the
        // open worker flags it, so open it in the Optical tab and switch there.
        if let Some(path) = self.inspect_tab.take_open_optical_request() {
            self.optical_tab.open_image(path);
            self.app.select_tab(AppTab::Optical);
        }

        // Show settings dialog if open
        self.settings_dialog.show(ctx);

        // The Settings "Clear Recent Files" button clears + persists the config
        // itself; here we also drop the in-memory mirrors each tab / Commander
        // pane holds (they only reload from config at construction otherwise).
        if self.settings_dialog.take_clear_recents() {
            self.inspect_tab.clear_recent_files();
            self.restore_tab.clear_recent_files();
            self.optical_tab.clear_recent_files();
            self.archives_tab.clear_recent_files();
            self.backup_tab.clear_recent_files();
            if let Some(cmd) = self.commander.as_mut() {
                cmd.clear_recent_files();
            }
        }
    }
}
