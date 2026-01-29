mod backup_tab;
mod inspect_tab;
mod progress;
mod restore_tab;

use backup_tab::BackupTab;
use inspect_tab::InspectTab;
use progress::{LogPanel, ProgressState};
use restore_tab::RestoreTab;

use rusty_backup::device::{self, DiskDevice};

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

        Self {
            active_tab: Tab::Inspect,
            backup_tab: BackupTab::default(),
            restore_tab: RestoreTab::default(),
            inspect_tab: InspectTab::default(),
            log_panel: log,
            progress: ProgressState::default(),
            devices,
        }
    }
}

impl eframe::App for RustyBackupApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // Top panel: tab bar
        egui::TopBottomPanel::top("tab_bar").show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.selectable_value(&mut self.active_tab, Tab::Backup, "Backup");
                ui.selectable_value(&mut self.active_tab, Tab::Restore, "Restore");
                ui.selectable_value(&mut self.active_tab, Tab::Inspect, "Inspect");

                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui.button("Refresh Devices").clicked() {
                        self.devices = device::enumerate_devices();
                        self.log_panel
                            .info(format!("Refreshed: {} device(s) found", self.devices.len()));
                    }
                });
            });
        });

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
        egui::CentralPanel::default().show(ctx, |ui| {
            match self.active_tab {
                Tab::Backup => {
                    self.backup_tab
                        .show(ui, &self.devices, &mut self.log_panel);
                }
                Tab::Restore => {
                    self.restore_tab
                        .show(ui, &self.devices, &mut self.log_panel);
                }
                Tab::Inspect => {
                    self.inspect_tab
                        .show(ui, &self.devices, &mut self.log_panel);
                }
            }
        });
    }
}
