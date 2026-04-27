mod backup_tab;
mod browse_view;
mod bulk_convert_dialog;
mod elevation_dialog;
mod expand_hfs_dialog;
mod inspect_tab;
mod optical_tab;
mod progress;
mod resize_popup;
mod restore_tab;
mod settings_dialog;

use backup_tab::BackupTab;
use bulk_convert_dialog::{
    BulkConvertDialog, BulkConvertStatus, DialogAction as BulkConvertAction,
    LogLevel as BulkConvertLogLevel,
};
use inspect_tab::InspectTab;
use optical_tab::OpticalTab;
use progress::{LogPanel, ProgressState};
use restore_tab::RestoreTab;
use settings_dialog::SettingsDialog;
use std::path::PathBuf;

use rusty_backup::device::{self, DiskDevice};
use rusty_backup::rbformats::chd::detect_chdman;
use rusty_backup::update::{check_for_updates, UpdateConfig, UpdateInfo};

#[cfg(target_os = "linux")]
use elevation_dialog::{ElevationAction, ElevationDialog};

use std::sync::{Arc, Mutex};
use std::thread;

/// Create an `rfd::FileDialog` pre-configured to start in the real user's home
/// directory. This ensures file dialogs open in the right place even when the
/// app is running elevated via pkexec.
fn bytes_human(n: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    if n >= GB {
        format!("{:.2} GB", n as f64 / GB as f64)
    } else if n >= MB {
        format!("{:.1} MB", n as f64 / MB as f64)
    } else if n >= KB {
        format!("{:.1} KB", n as f64 / KB as f64)
    } else {
        format!("{} B", n)
    }
}

fn file_dialog() -> rfd::FileDialog {
    let dialog = rfd::FileDialog::new();
    #[cfg(target_os = "linux")]
    let dialog = if let Some(home) = rusty_backup::os::linux::real_user_home() {
        dialog.set_directory(home)
    } else {
        dialog
    };
    dialog
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Tab {
    Backup,
    Restore,
    Inspect,
    Optical,
}

/// Main application state.
pub struct RustyBackupApp {
    active_tab: Tab,
    backup_tab: BackupTab,
    restore_tab: RestoreTab,
    inspect_tab: InspectTab,
    optical_tab: OpticalTab,
    log_panel: LogPanel,
    progress: ProgressState,
    devices: Vec<DiskDevice>,
    update_info: Arc<Mutex<Option<UpdateInfo>>>,
    update_dismissed: bool,
    settings_dialog: SettingsDialog,
    #[cfg(target_os = "linux")]
    elevation_dialog: ElevationDialog,
    /// Shared backup folder path between restore and inspect tabs
    loaded_backup_folder: Option<PathBuf>,
    /// Bulk Convert dialog state (None = closed).
    bulk_convert_dialog: Option<BulkConvertDialog>,
    /// Background bulk-convert worker progress (None = idle).
    bulk_convert_status: Option<Arc<Mutex<BulkConvertStatus>>>,
}

impl Default for RustyBackupApp {
    fn default() -> Self {
        let mut log = LogPanel::default();
        log.info("Rusty Backup started");

        let devices = device::enumerate_devices();
        if devices.is_empty() {
            log.warn("No disk devices detected. You can still open image files.");
        } else {
            log.info(format!("Found {} device(s)", devices.len()));
        }

        // Check privilege status
        #[cfg(target_os = "linux")]
        let elevation_dialog = {
            let dialog = ElevationDialog::default();
            if !devices.is_empty() {
                // Check if we need elevation
                if let Ok(access) = rusty_backup::privileged::create_disk_access() {
                    if let Ok(status) = access.check_status() {
                        match status {
                            rusty_backup::privileged::AccessStatus::NeedsElevation => {
                                log.warn(
                                    "Running without elevated privileges. Click 'Request Elevation' to access devices.",
                                );
                            }
                            rusty_backup::privileged::AccessStatus::Ready => {
                                log.info("Running with elevated privileges");
                            }
                            _ => {}
                        }
                    }
                }
            }
            dialog
        };

        // Detect chdman availability
        let chdman_available = detect_chdman();
        if chdman_available {
            log.info("chdman detected: CHD compression available");
        } else {
            log.info("chdman not found: CHD compression disabled");
        }

        let mut backup_tab = BackupTab::default();
        backup_tab.set_chdman_available(chdman_available);

        let mut optical_tab = OpticalTab::default();
        optical_tab.set_chdman_available(chdman_available);
        optical_tab.refresh_drives();
        if optical_tab.drive_count() > 0 {
            log.info(format!(
                "Found {} optical drive(s)",
                optical_tab.drive_count()
            ));
        }

        // Start update check in background
        let update_info = Arc::new(Mutex::new(None));
        let update_info_clone = Arc::clone(&update_info);
        thread::spawn(move || {
            let config = UpdateConfig::load();
            if config.update_check.enabled {
                let current_version = env!("APP_VERSION");
                if let Ok(info) = check_for_updates(&config.update_check, current_version) {
                    *update_info_clone.lock().unwrap() = Some(info);
                }
            }
        });

        Self {
            active_tab: Tab::Inspect,
            backup_tab,
            restore_tab: RestoreTab::default(),
            inspect_tab: InspectTab::default(),
            optical_tab,
            log_panel: log,
            progress: ProgressState::default(),
            devices,
            update_info,
            update_dismissed: false,
            settings_dialog: SettingsDialog::default(),
            #[cfg(target_os = "linux")]
            elevation_dialog,
            loaded_backup_folder: None,
            bulk_convert_dialog: None,
            bulk_convert_status: None,
        }
    }
}

impl RustyBackupApp {
    /// Drain bulk-convert worker log messages into the GUI log panel and
    /// clear the status handle when the worker finishes.
    fn poll_bulk_convert(&mut self) {
        let status_arc = match &self.bulk_convert_status {
            Some(s) => Arc::clone(s),
            None => return,
        };
        let Ok(mut status) = status_arc.lock() else {
            return;
        };
        for (level, msg) in status.log_messages.drain(..) {
            match level {
                BulkConvertLogLevel::Info => self.log_panel.info(msg),
                BulkConvertLogLevel::Warn => self.log_panel.warn(msg),
                BulkConvertLogLevel::Error => self.log_panel.error(msg),
            }
        }
        if status.finished {
            drop(status);
            self.bulk_convert_status = None;
        }
    }
}

impl eframe::App for RustyBackupApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // Request repaint while backup/restore is running so progress updates are shown
        if self.progress.active
            || self.restore_tab.is_running()
            || self.optical_tab.is_running()
            || self.bulk_convert_status.is_some()
        {
            ctx.request_repaint();
        }

        // Drain bulk-convert worker logs into the panel and clear when done.
        self.poll_bulk_convert();

        // Top panel: tab bar
        egui::TopBottomPanel::top("tab_bar").show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.selectable_value(&mut self.active_tab, Tab::Backup, "Backup");
                ui.selectable_value(&mut self.active_tab, Tab::Restore, "Restore");
                ui.selectable_value(&mut self.active_tab, Tab::Inspect, "Inspect");
                ui.selectable_value(&mut self.active_tab, Tab::Optical, "Optical");

                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    // Version display
                    ui.label(format!("v{}", env!("APP_VERSION")));
                    ui.separator();

                    // Close Backup button (if backup loaded)
                    if self.loaded_backup_folder.is_some() {
                        if ui.button("Close Backup").clicked() {
                            self.loaded_backup_folder = None;
                            self.restore_tab.clear_backup();
                            self.inspect_tab.clear_backup();
                            self.log_panel.info("Backup closed");
                        }
                        ui.separator();
                    }

                    if ui.button("Settings").clicked() {
                        self.settings_dialog.open_dialog();
                    }
                    ui.separator();

                    // Elevation button (Linux only, when not root)
                    #[cfg(target_os = "linux")]
                    {
                        if let Ok(access) = rusty_backup::privileged::create_disk_access() {
                            if let Ok(status) = access.check_status() {
                                if status == rusty_backup::privileged::AccessStatus::NeedsElevation {
                                    if ui
                                        .button(egui::RichText::new("Request Elevation").color(egui::Color32::YELLOW))
                                        .on_hover_text("Restart with administrator privileges to access disk devices")
                                        .clicked()
                                    {
                                        self.elevation_dialog.open();
                                    }
                                    ui.separator();
                                }
                            }
                        }
                    }

                    if ui.button("Refresh Devices").clicked() {
                        self.devices = device::enumerate_devices();
                        self.log_panel
                            .info(format!("Refreshed: {} device(s) found", self.devices.len()));
                        ctx.request_repaint();
                    }
                    ui.separator();

                    // Bulk Convert — convert every disk image in a folder.
                    let bulk_running = self.bulk_convert_status.is_some();
                    if ui
                        .add_enabled(!bulk_running, egui::Button::new("Bulk Convert…"))
                        .on_hover_text(
                            "Convert every disk image in a folder to one chosen format, \
                             using the same parameters for every file.",
                        )
                        .clicked()
                    {
                        self.bulk_convert_dialog = Some(BulkConvertDialog::default());
                    }
                    if bulk_running {
                        if ui.button("Cancel Bulk").clicked() {
                            if let Some(ref s) = self.bulk_convert_status {
                                if let Ok(mut g) = s.lock() {
                                    g.cancel_requested = true;
                                }
                            }
                            self.log_panel.warn("Bulk convert cancellation requested...");
                        }
                    }
                });
            });
        });

        // Bulk Convert dialog (modal-ish window, always reachable).
        if let Some(ref mut dlg) = self.bulk_convert_dialog {
            match dlg.show(ctx) {
                BulkConvertAction::Start { files, extension } => {
                    if let Some(out) = dlg.output_folder.clone() {
                        let format = dlg.format;
                        let count = files.len();
                        self.bulk_convert_dialog = None;
                        self.log_panel.info(format!(
                            "Starting bulk convert: {count} file(s) -> {} ({}, .{extension})",
                            out.display(),
                            format.description(),
                        ));
                        self.bulk_convert_status = Some(bulk_convert_dialog::start_bulk_convert(
                            files, out, format, extension,
                        ));
                    }
                }
                BulkConvertAction::Cancel => {
                    self.bulk_convert_dialog = None;
                }
                BulkConvertAction::None => {}
            }
        }

        // Bulk Convert progress strip — visible from any tab while running.
        if let Some(ref status_arc) = self.bulk_convert_status {
            if let Ok(s) = status_arc.lock() {
                if !s.finished {
                    let frac = if s.current_total_bytes > 0 {
                        (s.current_bytes as f32 / s.current_total_bytes as f32).clamp(0.0, 1.0)
                    } else {
                        0.0
                    };
                    let text = if s.current_total_bytes > 0 {
                        format!(
                            "Bulk Convert: [{}/{}] {} — {} / {} ({:.0}%)",
                            s.current_index,
                            s.total_files,
                            s.current_file,
                            bytes_human(s.current_bytes),
                            bytes_human(s.current_total_bytes),
                            frac * 100.0,
                        )
                    } else {
                        format!(
                            "Bulk Convert: [{}/{}] {} — preparing…",
                            s.current_index, s.total_files, s.current_file,
                        )
                    };
                    egui::TopBottomPanel::top("bulk_convert_progress").show(ctx, |ui| {
                        ui.add(egui::ProgressBar::new(frac).text(text));
                    });
                }
            }
        }

        // Update notification banner (if update available and not dismissed)
        if !self.update_dismissed {
            if let Ok(Some(ref info)) = self.update_info.lock().map(|guard| guard.clone()) {
                if info.is_outdated {
                    egui::TopBottomPanel::top("update_banner").show(ctx, |ui| {
                        ui.horizontal(|ui| {
                            ui.label(
                                egui::RichText::new("Update")
                                    .color(egui::Color32::YELLOW)
                                    .strong(),
                            );
                            ui.label(format!(
                                "available: v{} -> v{}",
                                info.current_version, info.latest_version
                            ));
                            if ui.button("View Release").clicked() {
                                let _ = webbrowser::open(&info.releases_url);
                            }
                            ui.with_layout(
                                egui::Layout::right_to_left(egui::Align::Center),
                                |ui| {
                                    if ui.button("Dismiss update notification").clicked() {
                                        self.update_dismissed = true;
                                    }
                                },
                            );
                        });
                    });
                }
            }
        }

        // Elevation dialog (Linux)
        #[cfg(target_os = "linux")]
        {
            let action = self.elevation_dialog.show(ctx);
            match action {
                ElevationAction::Elevate => {
                    self.log_panel
                        .info("Relaunching with elevated privileges...");
                    if let Err(e) = rusty_backup::os::linux::relaunch_with_elevation() {
                        self.log_panel.error(format!("Failed to elevate: {}", e));
                    }
                    // If relaunch_with_elevation returns, something went wrong
                }
                ElevationAction::Cancel => {
                    self.log_panel
                        .warn("Elevation cancelled. Device operations will fail.");
                }
                ElevationAction::None => {}
            }
        }

        // Bottom panel: progress + log
        egui::TopBottomPanel::bottom("log_panel")
            .resizable(true)
            .min_height(100.0)
            .default_height(180.0)
            .show(ctx, |ui| {
                self.progress.show(ui);
                if self.progress.active {
                    ui.separator();
                }
                self.log_panel.show(ui);
            });

        // Central panel: active tab content
        egui::CentralPanel::default().show(ctx, |ui| match self.active_tab {
            Tab::Backup => {
                self.backup_tab
                    .show(ui, &self.devices, &mut self.log_panel, &mut self.progress);
            }
            Tab::Restore => {
                // Share backup folder between tabs
                if let Some(new_backup) = self.restore_tab.get_loaded_backup() {
                    if self.loaded_backup_folder.as_ref() != Some(&new_backup) {
                        self.loaded_backup_folder = Some(new_backup.clone());
                        self.inspect_tab.load_backup(&new_backup);
                    }
                } else if self.loaded_backup_folder.is_some() && !self.restore_tab.has_backup() {
                    self.restore_tab
                        .load_backup(self.loaded_backup_folder.as_ref().unwrap());
                }

                self.restore_tab
                    .show(ui, &self.devices, &mut self.log_panel, &mut self.progress);
            }
            Tab::Inspect => {
                // Share backup folder between tabs
                if let Some(new_backup) = self.inspect_tab.get_loaded_backup() {
                    if self.loaded_backup_folder.as_ref() != Some(&new_backup) {
                        self.loaded_backup_folder = Some(new_backup.clone());
                        self.restore_tab.load_backup(&new_backup);
                    }
                } else if self.loaded_backup_folder.is_some() && !self.inspect_tab.has_backup() {
                    self.inspect_tab
                        .load_backup(self.loaded_backup_folder.as_ref().unwrap());
                }

                self.inspect_tab
                    .show(ui, &self.devices, &mut self.log_panel);
            }
            Tab::Optical => {
                self.optical_tab
                    .show(ui, &mut self.log_panel, &mut self.progress);
            }
        });

        // Show settings dialog if open
        self.settings_dialog.show(ctx);
    }
}
