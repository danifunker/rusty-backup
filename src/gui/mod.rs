mod backup_tab;
mod browse_view;
mod inspect_tab;
mod progress;
mod restore_tab;

use backup_tab::BackupTab;
use inspect_tab::InspectTab;
use progress::{LogPanel, ProgressState};
use restore_tab::RestoreTab;

use rusty_backup::device::{self, DiskDevice};
use rusty_backup::rbformats::chd::detect_chdman;
use rusty_backup::update::{check_for_updates, UpdateConfig, UpdateInfo};

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
                self.restore_tab
                    .show(ui, &self.devices, &mut self.log_panel, &mut self.progress);
            }
            Tab::Inspect => {
                self.inspect_tab
                    .show(ui, &self.devices, &mut self.log_panel);
            }
        });
    }
}
