mod backup_tab;
mod browse_view;
mod elevation_dialog;
mod inspect_tab;
mod progress;
mod restore_tab;
mod settings_dialog;

use backup_tab::BackupTab;
use inspect_tab::InspectTab;
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
}

/// Main application state.
pub struct RustyBackupApp {
    active_tab: Tab,
    backup_tab: BackupTab,
    restore_tab: RestoreTab,
    inspect_tab: InspectTab,
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
            log_panel: log,
            progress: ProgressState::default(),
            devices,
            update_info,
            update_dismissed: false,
            settings_dialog: SettingsDialog::default(),
            #[cfg(target_os = "linux")]
            elevation_dialog,
            loaded_backup_folder: None,
        }
    }
}

impl eframe::App for RustyBackupApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // Request repaint while backup/restore is running so progress updates are shown
        if self.progress.active || self.restore_tab.is_running() {
            ctx.request_repaint();
        }

        // Top panel: tab bar
        egui::TopBottomPanel::top("tab_bar").show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.selectable_value(&mut self.active_tab, Tab::Backup, "Backup");
                ui.selectable_value(&mut self.active_tab, Tab::Restore, "Restore");
                ui.selectable_value(&mut self.active_tab, Tab::Inspect, "Inspect");

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
                                        .button(egui::RichText::new("⚠ Request Elevation").color(egui::Color32::YELLOW))
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
                });
            });
        });

        // Update notification banner (if update available and not dismissed)
        if !self.update_dismissed {
            if let Ok(Some(ref info)) = self.update_info.lock().map(|guard| guard.clone()) {
                if info.is_outdated {
                    egui::TopBottomPanel::top("update_banner").show(ctx, |ui| {
                        ui.horizontal(|ui| {
                            ui.label(
                                egui::RichText::new("⚠")
                                    .color(egui::Color32::YELLOW)
                                    .size(20.0),
                            );
                            ui.label(format!(
                                "Update available: v{} → v{}",
                                info.current_version, info.latest_version
                            ));
                            if ui.button("View Release").clicked() {
                                let _ = webbrowser::open(&info.releases_url);
                            }
                            ui.with_layout(
                                egui::Layout::right_to_left(egui::Align::Center),
                                |ui| {
                                    if ui.button("✖").clicked() {
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
        });

        // Show settings dialog if open
        self.settings_dialog.show(ctx);
    }
}
