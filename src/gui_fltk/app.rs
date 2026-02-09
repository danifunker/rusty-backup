use super::{
    backup_tab::BackupTab, inspect_tab::InspectTab, log_panel::LogPanel, progress::ProgressState,
    restore_tab::RestoreTab,
};
use fltk::{prelude::*, *};
use rusty_backup::backup::metadata::BackupMetadata;
use rusty_backup::device::{self, DiskDevice};
use std::sync::{Arc, Mutex};

// Shared state for currently loaded backup
#[derive(Clone, Default)]
pub struct LoadedBackupState {
    pub metadata: Option<BackupMetadata>,
    pub backup_path: Option<String>,
}

// Shared state for currently loaded disk image file
#[derive(Clone, Default)]
pub struct LoadedFileState {
    pub file_path: Option<std::path::PathBuf>,
    pub partition_table: Option<rusty_backup::partition::PartitionTable>,
}

pub struct RustyBackupApp {
    wind: window::Window,
    _top_bar: group::Group,
    tabs: group::Tabs,
    log_panel: LogPanel,
    progress_state: ProgressState,
    devices: Vec<DiskDevice>,
    loaded_backup: Arc<Mutex<LoadedBackupState>>,
    loaded_file: Arc<Mutex<LoadedFileState>>,
}

impl RustyBackupApp {
    pub fn new() -> Self {
        let _app = app::App::default();
        app::set_visible_focus(false);

        // Create main window
        let mut wind = window::Window::default()
            .with_size(900, 700)
            .with_label(&format!("Rusty Backup v{}", env!("APP_VERSION")));
        wind.size_range(600, 400, 0, 0);
        wind.make_resizable(true);

        // Set window icon
        let icon_bytes = include_bytes!("../../assets/icons/icon-256.png");
        if let Ok(fltk_icon) = image::PngImage::from_data(icon_bytes) {
            wind.set_icon(Some(fltk_icon));
        }

        // Enumerate devices
        let devices = device::enumerate_devices();

        // Create shared loaded backup state
        let loaded_backup = Arc::new(Mutex::new(LoadedBackupState::default()));

        // Create shared loaded file state
        let loaded_file = Arc::new(Mutex::new(LoadedFileState::default()));

        // Create top bar (horizontal group for buttons and version)
        let top_bar = group::Group::new(5, 5, 890, 35, None);

        // Refresh Devices button
        let mut refresh_btn = button::Button::new(5, 5, 120, 30, "Refresh Devices");

        // Settings button
        let mut settings_btn = button::Button::new(130, 5, 80, 30, "Settings");

        // Close Backup button (initially hidden)
        let mut close_backup_btn = button::Button::new(215, 5, 100, 30, "Close Backup");
        close_backup_btn.hide();

        // Linux: Request Elevation button (conditional)
        #[cfg(target_os = "linux")]
        let mut elevate_btn = {
            let mut btn = button::Button::new(320, 5, 130, 30, "Request Elevation");
            if nix::unistd::geteuid().is_root() {
                btn.hide(); // Hide if already elevated
            }
            btn
        };

        // Version label (right-aligned)
        let mut version_label = frame::Frame::new(700, 5, 90, 30, None);
        version_label.set_label(&format!("v{}", env!("APP_VERSION")));

        #[cfg(feature = "update-checker")]
        // Update notification (initially hidden) - icon and button on top bar
        let mut update_icon = frame::Frame::new(795, 5, 30, 30, "🎉");
        #[cfg(feature = "update-checker")]
        update_icon.set_label_size(18);
        #[cfg(feature = "update-checker")]
        update_icon.hide();

        #[cfg(feature = "update-checker")]
        let mut view_update_btn = button::Button::new(825, 5, 65, 30, "Update");
        #[cfg(feature = "update-checker")]
        view_update_btn.set_color(enums::Color::from_rgb(0, 120, 200));
        #[cfg(feature = "update-checker")]
        view_update_btn.set_label_color(enums::Color::White);
        #[cfg(feature = "update-checker")]
        view_update_btn.hide();

        top_bar.end();

        // Create tabs (below top bar, no banner)
        let mut tabs = group::Tabs::new(5, 45, 890, 440, None);

        // Create shared state
        let progress_state = ProgressState::default();

        // Backup tab
        let backup_group = group::Group::new(5, 70, 890, 415, "Backup\t");
        // Note: log_panel will be created after tabs.end(), so pass None for now
        backup_group.end();

        // Restore tab
        let restore_group = group::Group::new(5, 70, 890, 415, "Restore\t");
        restore_group.end();

        // Inspect tab
        let inspect_group = group::Group::new(5, 70, 890, 415, "Inspect\t");
        inspect_group.end();

        tabs.end();
        tabs.auto_layout();

        // Create log panel AFTER tabs.end() so it's not inside the tabs
        let log_panel = LogPanel::new(5, 490, 890, 170);

        // Add log control buttons below the log panel
        let mut clear_btn = button::Button::new(5, 665, 100, 30, "Clear Logs");
        let mut copy_btn = button::Button::new(110, 665, 100, 30, "Copy Logs");

        // Wire up log button callbacks
        clear_btn.set_callback({
            let mut log = log_panel.clone();
            move |_| {
                log.clear();
            }
        });

        copy_btn.set_callback({
            let log = log_panel.clone();
            move |_| {
                if let Some(text) = log.get_text() {
                    app::copy(&text);
                }
            }
        });

        settings_btn.set_callback({
            let mut log = log_panel.clone();
            move |_| {
                log.info("Settings window not yet implemented");
                // TODO: Open settings window
            }
        });

        #[cfg(feature = "update-checker")]
        // Wire up update button
        let releases_url = Arc::new(Mutex::new(String::new()));

        #[cfg(feature = "update-checker")]
        view_update_btn.set_callback({
            let url = releases_url.clone();
            let mut log = log_panel.clone();
            move |_| {
                if let Ok(url_str) = url.lock() {
                    if !url_str.is_empty() {
                        match webbrowser::open(url_str.as_str()) {
                            Ok(_) => log.info("Opened release page in browser"),
                            Err(e) => log.warn(format!("Failed to open browser: {}", e)),
                        }
                    }
                }
            }
        });

        // Now create tab contents with log_panel available
        backup_group.begin();
        let backup_tab = BackupTab::new(
            5,
            70,
            890,
            415,
            &devices,
            log_panel.clone(),
            progress_state.clone(),
        );
        let backup_source_choice = backup_tab.get_source_choice();
        backup_group.end();

        restore_group.begin();
        let restore_tab = RestoreTab::new(
            5,
            70,
            890,
            415,
            &devices,
            log_panel.clone(),
            progress_state.clone(),
            loaded_backup.clone(),
            close_backup_btn.clone(),
        );
        let restore_target_choice = restore_tab.get_target_choice();
        restore_group.end();

        inspect_group.begin();
        let inspect_tab = InspectTab::new(
            5,
            70,
            890,
            415,
            &devices,
            log_panel.clone(),
            loaded_backup.clone(),
            loaded_file.clone(),
            close_backup_btn.clone(),
        );
        let inspect_source_choice = inspect_tab.get_source_choice();
        inspect_group.end();

        // Wire up refresh button callback (now that we have the choice widgets)
        refresh_btn.set_callback({
            let mut log = log_panel.clone();
            let mut backup_choice = backup_source_choice.clone();
            let mut restore_choice = restore_target_choice.clone();
            let mut inspect_choice = inspect_source_choice.clone();

            move |_| {
                log.info("Refreshing device list...");
                let new_devices = device::enumerate_devices();
                log.info(format!("Found {} device(s)", new_devices.len()));

                // Update all three dropdowns
                let backup_val = backup_choice.value();
                backup_choice.clear();
                backup_choice.add_choice("Select a device...");
                for device in new_devices.iter() {
                    backup_choice.add_choice(&device.display_name());
                }
                if backup_val < (new_devices.len() + 1) as i32 {
                    backup_choice.set_value(backup_val);
                } else {
                    backup_choice.set_value(0);
                }

                let restore_val = restore_choice.value();
                restore_choice.clear();
                restore_choice.add_choice("Select a device...");
                for device in new_devices.iter() {
                    restore_choice.add_choice(&device.display_name());
                }
                if restore_val < (new_devices.len() + 1) as i32 {
                    restore_choice.set_value(restore_val);
                } else {
                    restore_choice.set_value(0);
                }

                let inspect_val = inspect_choice.value();
                inspect_choice.clear();
                inspect_choice.add_choice("Select a source...");
                for device in new_devices.iter() {
                    inspect_choice.add_choice(&device.display_name());
                }
                if inspect_val < (new_devices.len() + 1) as i32 {
                    inspect_choice.set_value(inspect_val);
                } else {
                    inspect_choice.set_value(0);
                }

                log.info("Device list updated in all tabs");
            }
        });

        // Wire up close backup button to clear info displays
        {
            let mut restore_buf = restore_tab.get_info_buffer();
            let mut inspect_buf = inspect_tab.get_info_buffer();
            let mut restore_input = restore_tab.get_backup_input();

            close_backup_btn.set_callback({
                let mut log = log_panel.clone();
                let loaded = loaded_backup.clone();
                let mut btn = close_backup_btn.clone();
                move |_| {
                    if let Ok(mut state) = loaded.lock() {
                        state.metadata = None;
                        state.backup_path = None;
                        log.info("Backup closed");
                        btn.hide();
                        btn.parent().unwrap().redraw();

                        // Clear info displays
                        restore_buf.set_text("");
                        inspect_buf.set_text("");
                        restore_input.set_value("");
                    }
                }
            });
        }

        // Add initial log messages
        {
            let mut log = log_panel.clone();
            log.info("Rusty Backup started");

            if devices.is_empty() {
                log.warn("No disk devices detected. You can still open image files.");
            } else {
                log.info(format!("Found {} device(s)", devices.len()));
            }

            // Detect chdman
            let chdman_available = rusty_backup::rbformats::chd::detect_chdman();
            if chdman_available {
                log.info("chdman detected: CHD compression available");
            } else {
                log.info("chdman not found: CHD compression disabled");
            }

            log.refresh();
        }

        #[cfg(feature = "update-checker")]
        // Check for updates in background
        {
            let mut log = log_panel.clone();
            let mut icon = update_icon.clone();
            let mut button = view_update_btn.clone();
            let url_storage = releases_url.clone();

            std::thread::spawn(move || {
                let config = rusty_backup::update::UpdateConfig::load();
                if config.update_check.enabled {
                    match rusty_backup::update::check_for_updates(
                        &config.update_check,
                        env!("APP_VERSION"),
                    ) {
                        Ok(info) => {
                            if info.is_outdated {
                                // Store the URL for the View Update button
                                if let Ok(mut url) = url_storage.lock() {
                                    *url = info.releases_url.clone();
                                }

                                // Clone values for closures
                                let latest_ver = info.latest_version.clone();
                                let log_msg = format!(
                                    "Update available: v{} → v{}",
                                    info.current_version, latest_ver
                                );

                                // Update UI on main thread - show icon and button
                                app::awake_callback(move || {
                                    icon.set_tooltip(&format!("Update available: v{}", latest_ver));
                                    icon.show();
                                    button.show();
                                    if let Some(mut parent) = icon.parent() {
                                        parent.redraw();
                                    }
                                });

                                log.info(log_msg);
                            } else {
                                log.info("Software is up to date");
                            }
                        }
                        Err(e) => {
                            log.warn(format!("Update check failed: {}", e));
                        }
                    }
                } else {
                    log.info("Update checking is disabled");
                }
            });
        }

        wind.end();
        wind.show();

        Self {
            wind,
            _top_bar: top_bar,
            tabs,
            log_panel,
            progress_state,
            devices,
            loaded_backup,
            loaded_file,
        }
    }

    pub fn run(self) {
        let mut log = self.log_panel;
        let _progress = self.progress_state.clone();

        // Periodic refresh for log/progress updates
        app::add_idle3(move |_| {
            log.refresh();
            // TODO: refresh progress widget
        });

        app::App::default().run().unwrap();
    }
}

impl Default for RustyBackupApp {
    fn default() -> Self {
        Self::new()
    }
}
