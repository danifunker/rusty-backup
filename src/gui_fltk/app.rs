use super::{
    backup_tab::BackupTab, inspect_tab::InspectTab, log_panel::LogPanel, progress::ProgressState,
    restore_tab::RestoreTab,
};
use fltk::{prelude::*, *};
use rusty_backup::device::{self, DiskDevice};

pub struct RustyBackupApp {
    wind: window::Window,
    tabs: group::Tabs,
    log_panel: LogPanel,
    progress_state: ProgressState,
    devices: Vec<DiskDevice>,
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

        // Create tabs (top section)
        let mut tabs = group::Tabs::new(5, 5, 890, 480, None);

        // Create shared state
        let progress_state = ProgressState::default();

        // Backup tab
        let backup_group = group::Group::new(5, 30, 890, 455, "Backup\t");
        // Note: log_panel will be created after tabs.end(), so pass None for now
        backup_group.end();

        // Restore tab
        let restore_group = group::Group::new(5, 30, 890, 455, "Restore\t");
        restore_group.end();

        // Inspect tab
        let inspect_group = group::Group::new(5, 30, 890, 455, "Inspect\t");
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

        // Now create tab contents with log_panel available
        backup_group.begin();
        let _backup_tab = BackupTab::new(
            5,
            30,
            890,
            455,
            &devices,
            log_panel.clone(),
            progress_state.clone(),
        );
        backup_group.end();

        restore_group.begin();
        let _restore_tab = RestoreTab::new(
            5,
            30,
            890,
            455,
            &devices,
            log_panel.clone(),
            progress_state.clone(),
        );
        restore_group.end();

        inspect_group.begin();
        let _inspect_tab = InspectTab::new(5, 30, 890, 455, &devices, log_panel.clone());
        inspect_group.end();

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

        wind.end();
        wind.show();

        Self {
            wind,
            tabs,
            log_panel,
            progress_state,
            devices,
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
