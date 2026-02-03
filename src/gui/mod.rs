mod backup_tab;
mod browse_view;
mod elevation_dialog;
mod inspect_tab;
mod progress;
mod restore_tab;
mod settings_dialog;

#[cfg(target_os = "macos")]
mod daemon_dialog;

use std::path::PathBuf;
use backup_tab::BackupTab;
use inspect_tab::InspectTab;
use progress::{LogPanel, ProgressState};
use restore_tab::RestoreTab;
use settings_dialog::SettingsDialog;

use rusty_backup::device::{self, DiskDevice};
use rusty_backup::rbformats::chd::detect_chdman;
use rusty_backup::update::{check_for_updates, UpdateConfig, UpdateInfo};

#[cfg(target_os = "linux")]
use elevation_dialog::{ElevationDialog, ElevationAction};

#[cfg(target_os = "macos")]
use daemon_dialog::{DaemonDialog, DaemonAction};

use std::sync::{Arc, Mutex};
use std::thread;

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
    #[cfg(target_os = "macos")]
    daemon_dialog: DaemonDialog,
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
            let mut dialog = ElevationDialog::default();
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

        #[cfg(target_os = "macos")]
        let daemon_dialog = {
            let dialog = DaemonDialog::default();
            if !devices.is_empty() {
                // Check daemon status
                if let Ok(access) = rusty_backup::privileged::create_disk_access() {
                    if let Ok(status) = access.check_status() {
                        match status {
                            rusty_backup::privileged::AccessStatus::DaemonNotInstalled => {
                                log.warn("Privileged helper daemon not installed. Click 'Install Helper' to enable disk access.");
                            }
                            rusty_backup::privileged::AccessStatus::DaemonNeedsApproval => {
                                log.warn("Privileged helper daemon is not running. It may need approval in System Settings > Login Items.");
                            }
                            rusty_backup::privileged::AccessStatus::DaemonOutdated { current } => {
                                log.warn(format!("Privileged helper daemon is outdated (version {}). Please reinstall from Settings.", current));
                            }
                            rusty_backup::privileged::AccessStatus::Ready => {
                                log.info("Privileged helper daemon is ready");
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
            #[cfg(target_os = "macos")]
            daemon_dialog,
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
                    
                    // Daemon install button (macOS only, when daemon not ready)
                    #[cfg(target_os = "macos")]
                    {
                        if let Ok(access) = rusty_backup::privileged::create_disk_access() {
                            if let Ok(status) = access.check_status() {
                                match status {
                                    rusty_backup::privileged::AccessStatus::DaemonNotInstalled => {
                                        if ui
                                            .button(egui::RichText::new("⚠ Install Helper").color(egui::Color32::YELLOW))
                                            .on_hover_text("Install privileged helper daemon to access disk devices")
                                            .clicked()
                                        {
                                            self.daemon_dialog.show();
                                        }
                                        ui.separator();
                                    }
                                    rusty_backup::privileged::AccessStatus::DaemonNeedsApproval => {
                                        if ui
                                            .button(egui::RichText::new("⚠ Helper Needs Approval").color(egui::Color32::YELLOW))
                                            .on_hover_text("Open System Settings to approve the helper daemon")
                                            .clicked()
                                        {
                                            // Open System Settings
                                            let _ = std::process::Command::new("open")
                                                .arg("x-apple.systempreferences:com.apple.LoginItems-Settings.extension")
                                                .spawn();
                                        }
                                        ui.separator();
                                    }
                                    rusty_backup::privileged::AccessStatus::DaemonOutdated { .. } => {
                                        if ui
                                            .button(egui::RichText::new("⚠ Update Helper").color(egui::Color32::YELLOW))
                                            .on_hover_text("Reinstall helper daemon to update it")
                                            .clicked()
                                        {
                                            self.daemon_dialog.show();
                                        }
                                        ui.separator();
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                    
                    if ui.button("Refresh Devices").clicked() {
                        self.devices = device::enumerate_devices();
                        self.log_panel
                            .info(format!("Refreshed: {} device(s) found", self.devices.len()));
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
                            ui.label(egui::RichText::new("⚠").color(egui::Color32::YELLOW).size(20.0));
                            ui.label(format!(
                                "Update available: v{} → v{}",
                                info.current_version, info.latest_version
                            ));
                            if ui.button("View Release").clicked() {
                                let _ = webbrowser::open(&info.releases_url);
                            }
                            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                if ui.button("✖").clicked() {
                                    self.update_dismissed = true;
                                }
                            });
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
                    self.log_panel.info("Relaunching with elevated privileges...");
                    if let Err(e) = rusty_backup::os::linux::relaunch_with_elevation() {
                        self.log_panel.error(format!("Failed to elevate: {}", e));
                    }
                    // If relaunch_with_elevation returns, something went wrong
                }
                ElevationAction::Cancel => {
                    self.log_panel.warn("Elevation cancelled. Device operations will fail.");
                }
                ElevationAction::None => {}
            }
        }

        // Daemon installation dialog (macOS)
        #[cfg(target_os = "macos")]
        {
            self.daemon_dialog.render(ctx);
            
            match self.daemon_dialog.action() {
                DaemonAction::Install => {
                    self.log_panel.info("Installing privileged helper daemon...");
                    match rusty_backup::os::macos::install_daemon() {
                        Ok(()) => {
                            self.log_panel.info("Helper daemon installed successfully!");
                            // Recheck status
                            if let Ok(access) = rusty_backup::privileged::create_disk_access() {
                                if let Ok(status) = access.check_status() {
                                    match status {
                                        rusty_backup::privileged::AccessStatus::Ready => {
                                            self.log_panel.info("Daemon is ready to use");
                                        }
                                        rusty_backup::privileged::AccessStatus::DaemonNeedsApproval => {
                                            self.log_panel.warn("Please approve the daemon in System Settings > Login Items, then restart Rusty Backup");
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            self.log_panel.error(format!("Failed to install daemon: {}", e));
                        }
                    }
                    self.daemon_dialog.reset_action();
                }
                DaemonAction::Cancel => {
                    self.log_panel.info("Daemon installation cancelled");
                    self.daemon_dialog.reset_action();
                }
                DaemonAction::None => {}
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
                    self.restore_tab.load_backup(self.loaded_backup_folder.as_ref().unwrap());
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
                    self.inspect_tab.load_backup(self.loaded_backup_folder.as_ref().unwrap());
                }
                
                self.inspect_tab
                    .show(ui, &self.devices, &mut self.log_panel);
            }
        });

        // Show settings dialog if open
        self.settings_dialog.show(ctx);
    }
}
