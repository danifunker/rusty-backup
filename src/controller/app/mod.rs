//! `AppController`: app-wide state every screen shares (`docs/native_frontends_plan.md` §4.2).
//!
//! Holds the log, the job registry, the shell's wake handle, (in builds with the
//! `update` module) the recent-files history persisted in config.json, and the app
//! shell's rules in [`shell`]: the active tab, drops, the shared backup folder, the
//! device list and elevation. The update banner's checks are in [`update`] (GUI and `tui-update` builds).

use std::sync::Arc;

mod shell;
#[cfg(any(feature = "gui", feature = "tui-update"))]
mod update;

pub use shell::{
    about, app_version, busy_elsewhere, busy_elsewhere_notice, elevation_button_hover,
    elevation_button_label, elevation_confirm, elevation_gate, host_known_older_than,
    install_appimage_entry, no_devices_hint, physical_devices_available, picker_start_folder,
    progress_text, running_elevated, unelevated_device_note, AppTab, DiskDevice, DropRoute,
    DropTargets, ElevationGate, ShareStep, MENU_SHORTCUTS, PHYSICAL_DEVICES_DISABLED,
};
#[cfg(any(feature = "gui", feature = "tui-update"))]
pub use update::{InstallState, UpdateInfo};

use super::log::{capture, LogStore};
use super::{Controller, Effect};
use crate::model::job::{JobRegistry, NoNotify};

// What the shared progress widgets draw, so views never name the model (CONTRIBUTING.md MVC).
pub use crate::model::file_types::{archives_picker_exts, DISK_IMAGE_EXTS, MAC_ARCHIVE_EXTS};
/// The wake a shell implements so jobs and log lines can repaint it.
pub use crate::model::job::Notify;
pub use crate::model::job::{Progress, Snapshot};
pub use crate::model::rate_tracker::RateTracker;
pub use crate::model::size_mode::SizeMode;
/// The installer's association flags and the post-update ARP refresh, for a Windows GUI's `main`.
#[cfg(windows)]
pub use crate::os::file_assoc::launch_hooks;
pub use disk_core::display::format_size;

/// The CHD codec and profile choices the shared CHD options control offers.
#[cfg(feature = "chd")]
pub mod chd;

/// Stable widget ids, shared by every shell's accessibility identifiers.
pub mod ids {
    pub const LOG_COPY: &str = "log.copy";
    pub const LOG_CLEAR: &str = "log.clear";
    pub const LOG_AUTO_SCROLL: &str = "log.auto_scroll";
    pub const LOG_LIST: &str = "log.list";
    pub const REFRESH_DEVICES: &str = "app.refresh_devices";
    pub const ELEVATE: &str = "app.elevate";
    pub const COMMANDER: &str = "app.commander";
    pub const CLOSE_BACKUP: &str = "app.close_backup";
    pub const MENU: &str = "app.menu";
    pub const SETTINGS: &str = "app.settings";
    pub const BULK_CONVERT: &str = "app.bulk_convert";
    pub const ABOUT: &str = "app.about";
    pub const UPDATE_VIEW: &str = "app.update.view";
    pub const UPDATE_DISMISS: &str = "app.update.dismiss";
    pub const UPDATE_INSTALL: &str = "app.update.install";
    pub const UPDATE_RESTART: &str = "app.update.restart";
    pub const UPDATE_RETRY: &str = "app.update.retry";
    pub const UPDATE_CHECK: &str = "app.update.check";
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppAction {
    CopyLog,
    ClearLog,
    SetLogAutoScroll(bool),
    SelectTab(AppTab),
    /// List the devices again, in a job.
    RefreshDevices,
    /// The top bar's elevation button.
    RequestElevation,
    /// The answer to the shell's "restart as root?" dialog (Linux).
    ConfirmElevation(bool),
    DismissUpdate,
    /// Open the announced release's page.
    ViewRelease,
    /// Download and install the announced release (Windows).
    InstallUpdate,
    /// After a failed install, offer it again.
    RetryUpdate,
    /// Run the installed update (Windows, macOS; exits this process).
    RestartApp,
    /// Check the release feed now and say what it found (the menu's Check for Updates...).
    CheckForUpdates,
}

pub struct AppController {
    pub log: LogStore,
    log_auto_scroll: bool,
    jobs: JobRegistry,
    notify: Arc<dyn Notify>,
    #[cfg(any(feature = "gui", feature = "tui-update", feature = "tui"))]
    config: RecentHandle,
    shell: shell::ShellState,
    #[cfg(any(feature = "gui", feature = "tui-update"))]
    update: update::UpdateState,
}

impl AppController {
    /// An app view-model waking the shell through `notify` and persisting to the user's config.json.
    pub fn new(notify: Arc<dyn Notify>) -> Self {
        AppController {
            log: LogStore::default(),
            log_auto_scroll: true,
            jobs: JobRegistry::new(),
            notify,
            #[cfg(any(feature = "gui", feature = "tui-update", feature = "tui"))]
            config: RecentHandle::new(Box::new(UserConfig)),
            shell: shell::ShellState::default(),
            #[cfg(any(feature = "gui", feature = "tui-update"))]
            update: update::UpdateState::default(),
        }
    }

    /// For tests: no wake, and config kept in memory.
    pub fn headless() -> Self {
        AppController {
            log: LogStore::default(),
            log_auto_scroll: true,
            jobs: JobRegistry::new(),
            notify: Arc::new(NoNotify),
            #[cfg(any(feature = "gui", feature = "tui-update", feature = "tui"))]
            config: RecentHandle::new(Box::<MemoryConfig>::default()),
            shell: shell::ShellState::default(),
            #[cfg(any(feature = "gui", feature = "tui-update"))]
            update: update::UpdateState::default(),
        }
    }

    pub fn log_auto_scroll(&self) -> bool {
        self.log_auto_scroll
    }

    pub fn jobs(&self) -> &JobRegistry {
        &self.jobs
    }

    /// The handle a job started for this app wakes the shell with.
    pub fn notify(&self) -> Arc<dyn Notify> {
        Arc::clone(&self.notify)
    }
}

impl Controller for AppController {
    type Action = AppAction;

    fn apply(&mut self, action: AppAction) -> Vec<Effect> {
        match action {
            AppAction::CopyLog => vec![Effect::CopyToClipboard(self.log.to_text())],
            AppAction::ClearLog => {
                self.log.clear();
                Vec::new()
            }
            AppAction::SetLogAutoScroll(on) => {
                self.log_auto_scroll = on;
                Vec::new()
            }
            AppAction::SelectTab(tab) => {
                self.select_tab(tab);
                Vec::new()
            }
            AppAction::RefreshDevices => {
                self.refresh_devices();
                Vec::new()
            }
            AppAction::RequestElevation => {
                self.request_elevation();
                Vec::new()
            }
            AppAction::ConfirmElevation(yes) => {
                self.confirm_elevation(yes);
                Vec::new()
            }
            AppAction::DismissUpdate => {
                #[cfg(any(feature = "gui", feature = "tui-update"))]
                self.dismiss_update();
                Vec::new()
            }
            AppAction::ViewRelease => {
                #[cfg(any(feature = "gui", feature = "tui-update"))]
                return self.view_release();
                #[cfg(not(any(feature = "gui", feature = "tui-update")))]
                Vec::new()
            }
            AppAction::InstallUpdate => {
                #[cfg(any(feature = "gui", feature = "tui-update"))]
                self.install_update();
                Vec::new()
            }
            AppAction::RetryUpdate => {
                #[cfg(any(feature = "gui", feature = "tui-update"))]
                self.retry_update();
                Vec::new()
            }
            AppAction::CheckForUpdates => {
                #[cfg(any(feature = "gui", feature = "tui-update"))]
                self.check_updates_now();
                Vec::new()
            }
            AppAction::RestartApp => {
                #[cfg(any(
                    windows,
                    all(target_os = "macos", any(feature = "gui", feature = "tui-update"))
                ))]
                crate::update::restart_app();
                #[cfg(not(any(
                    windows,
                    all(target_os = "macos", any(feature = "gui", feature = "tui-update"))
                )))]
                Vec::new()
            }
        }
    }

    fn tick(&mut self) -> Vec<Effect> {
        self.log.extend_from_capture(capture::drain());
        self.poll_shell();
        #[cfg(any(feature = "gui", feature = "tui-update"))]
        return self.poll_update();
        #[cfg(not(any(feature = "gui", feature = "tui-update")))]
        Vec::new()
    }
}

/// Which tab a recent-files list belongs to.
#[cfg(any(feature = "gui", feature = "tui-update", feature = "tui"))]
pub use crate::update::RecentMode;
#[cfg(any(feature = "gui", feature = "tui-update", feature = "tui"))]
pub use recent::{CommanderRecent, ConfigStore, MemoryConfig, RecentHandle, UserConfig};

/// Check the release feed at `repo_url` now and say what it found (blocking: run it in a job).
#[cfg(any(feature = "gui", feature = "tui-update"))]
pub fn check_updates_now_text(repo_url: &str) -> String {
    let config = crate::update::UpdateCheckConfig {
        enabled: true,
        repository_url: repo_url.to_string(),
    };
    let current = env!("APP_VERSION");
    match crate::update::check_for_updates(&config, current) {
        Ok(info) if info.is_outdated => format!(
            "Update available: {} (current {current}). Run `rb-cli update --apply`.",
            info.latest_version
        ),
        Ok(_) => format!("Up to date (current {current})."),
        Err(e) => format!("Update check failed: {e}"),
    }
}

#[cfg(any(feature = "gui", feature = "tui-update", feature = "tui"))]
mod recent {
    use super::AppController;
    use crate::update::{RecentMode, UpdateConfig};
    use std::sync::{Arc, Mutex, MutexGuard};

    /// Where `AppController` loads and saves config.json; swapped for memory in tests.
    pub trait ConfigStore: Send {
        fn load(&self) -> UpdateConfig;
        fn save(&mut self, config: &UpdateConfig);
        /// Save and say why it failed; stores that cannot fail keep the default.
        fn try_save(&mut self, config: &UpdateConfig) -> Result<(), String> {
            self.save(config);
            Ok(())
        }
    }

    /// The user's real config.json.
    pub struct UserConfig;

    impl ConfigStore for UserConfig {
        fn load(&self) -> UpdateConfig {
            UpdateConfig::load()
        }

        fn save(&mut self, config: &UpdateConfig) {
            if let Err(e) = self.try_save(config) {
                log::warn!("could not save config.json: {e}");
            }
        }

        fn try_save(&mut self, config: &UpdateConfig) -> Result<(), String> {
            config.save().map_err(|e| e.to_string())
        }
    }

    /// An in-memory config, so tests never touch the user's file.
    #[derive(Default)]
    pub struct MemoryConfig(Option<UpdateConfig>);

    impl ConfigStore for MemoryConfig {
        fn load(&self) -> UpdateConfig {
            self.0.clone().unwrap_or_default()
        }

        fn save(&mut self, config: &UpdateConfig) {
            self.0 = Some(config.clone());
        }
    }

    /// Commander's recent files in a config store: lists the merged view, records under Commander.
    pub struct CommanderRecent<S: ConfigStore>(pub S);

    impl<S: ConfigStore> crate::controller::commander::RecentStore for CommanderRecent<S> {
        fn list(&self) -> Vec<String> {
            crate::update::merged_recent(&self.0.load())
        }

        fn remember(&mut self, path: &str) {
            let mut cfg = self.0.load();
            cfg.remember_file(RecentMode::Commander, path);
            self.0.save(&cfg);
        }

        fn listing_style(&self, view: &str) -> Option<String> {
            self.0.load().listing_styles.get(view).cloned()
        }

        fn remember_listing_style(&mut self, view: &str, style: &str) {
            let mut cfg = self.0.load();
            cfg.listing_styles
                .insert(view.to_string(), style.to_string());
            self.0.save(&cfg);
        }
    }

    /// The app's config store, shared so a screen built apart from the root records into it too.
    #[derive(Clone)]
    pub struct RecentHandle(Arc<Mutex<Box<dyn ConfigStore>>>);

    impl RecentHandle {
        pub(super) fn new(store: Box<dyn ConfigStore>) -> Self {
            RecentHandle(Arc::new(Mutex::new(store)))
        }

        fn lock(&self) -> MutexGuard<'_, Box<dyn ConfigStore>> {
            self.0.lock().unwrap_or_else(|e| e.into_inner())
        }

        pub(super) fn load_config(&self) -> UpdateConfig {
            self.lock().load()
        }

        /// The recent-files list for `mode`, newest first.
        pub fn list(&self, mode: RecentMode) -> Vec<String> {
            self.load_config().recent_files.list(mode).to_vec()
        }

        /// Record a successful open of `path` and return the updated list for `mode`.
        pub fn remember(&self, mode: RecentMode, path: &str) -> Vec<String> {
            let mut store = self.lock();
            let mut cfg = store.load();
            cfg.remember_file(mode, path);
            store.save(&cfg);
            cfg.recent_files.list(mode).to_vec()
        }

        /// Forget every mode's recent files.
        pub fn clear(&self) {
            let mut store = self.lock();
            let mut cfg = store.load();
            cfg.recent_files.clear();
            store.save(&cfg);
        }
    }

    /// Settings reads and saves through the app's store, so its Clear empties every screen's list.
    impl ConfigStore for RecentHandle {
        fn load(&self) -> UpdateConfig {
            self.load_config()
        }

        fn save(&mut self, config: &UpdateConfig) {
            self.lock().save(config);
        }

        fn try_save(&mut self, config: &UpdateConfig) -> Result<(), String> {
            self.lock().try_save(config)
        }
    }

    impl AppController {
        /// A handle on this app's recent files, for a screen that records opens on its own.
        pub fn recent_handle(&self) -> RecentHandle {
            self.config.clone()
        }

        /// The recent-files list for `mode`, newest first.
        pub fn recent(&self, mode: RecentMode) -> Vec<String> {
            self.config.list(mode)
        }

        /// Record a successful open of `path` and return the updated list for `mode`.
        pub fn remember(&mut self, mode: RecentMode, path: &str) -> Vec<String> {
            self.config.remember(mode, path)
        }

        /// Forget every mode's recent files.
        pub fn clear_recent(&mut self) {
            self.config.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_log_asks_the_shell_to_copy_every_line() {
        let mut app = AppController::headless();
        app.log.info("one");
        app.log.warn("two");
        let effects = app.apply(AppAction::CopyLog);
        assert_eq!(effects, vec![Effect::CopyToClipboard(app.log.to_text())]);
        assert!(app.log.to_text().contains("[WARN] two"));
    }

    #[test]
    fn clear_log_and_auto_scroll() {
        let mut app = AppController::headless();
        app.log.info("one");
        assert!(app.apply(AppAction::ClearLog).is_empty());
        assert!(app.log.entries().is_empty());
        assert!(app.log_auto_scroll());
        app.apply(AppAction::SetLogAutoScroll(false));
        assert!(!app.log_auto_scroll());
    }

    #[cfg(any(feature = "gui", feature = "tui-update", feature = "tui"))]
    #[test]
    fn recent_files_are_newest_first_and_per_mode() {
        use crate::update::RecentMode;
        let mut app = AppController::headless();
        app.remember(RecentMode::Inspect, "/a.img");
        app.remember(RecentMode::Inspect, "/b.img");
        let list = app.remember(RecentMode::Inspect, "/a.img");
        assert_eq!(list, ["/a.img", "/b.img"]);
        assert!(app.recent(RecentMode::Optical).is_empty());
        app.clear_recent();
        assert!(app.recent(RecentMode::Inspect).is_empty());
    }

    #[cfg(any(feature = "gui", feature = "tui-update", feature = "tui"))]
    #[test]
    fn a_recent_handle_shares_the_app_store() {
        let mut app = AppController::headless();
        let handle = app.recent_handle();
        handle.remember(RecentMode::Backup, "/src.img");
        assert_eq!(app.recent(RecentMode::Backup), ["/src.img"]);
        app.remember(RecentMode::Backup, "/b.img");
        assert_eq!(handle.list(RecentMode::Backup), ["/b.img", "/src.img"]);
        app.clear_recent();
        assert!(handle.list(RecentMode::Backup).is_empty());
    }

    #[cfg(any(feature = "gui", feature = "tui-update", feature = "tui"))]
    #[test]
    fn settings_clear_goes_through_the_app_store() {
        use crate::controller::settings::{SettingsAction, SettingsController};
        let mut app = AppController::headless();
        app.remember(RecentMode::Inspect, "/a.img");
        let mut s = SettingsController::with_store(Box::new(app.recent_handle()));
        s.apply(SettingsAction::Open);
        assert_eq!(s.recent_count(), 1);
        s.apply(SettingsAction::SetRepoUrl("https://github.com/o/r".into()));
        s.apply(SettingsAction::Save);
        s.apply(SettingsAction::ClearRecent);
        assert!(app.recent(RecentMode::Inspect).is_empty());
        let saved = app.recent_handle().load();
        assert_eq!(saved.update_check.repository_url, "https://github.com/o/r");
    }

    #[cfg(any(feature = "gui", feature = "tui-update", feature = "tui"))]
    #[test]
    fn commander_recent_merges_modes_and_records_under_commander() {
        use crate::controller::commander::RecentStore;
        let mut cfg = MemoryConfig::default();
        let mut seeded = cfg.load();
        seeded.remember_file(crate::update::RecentMode::Inspect, "/inspected.img");
        cfg.save(&seeded);
        let mut recent = CommanderRecent(cfg);
        recent.remember("/opened.img");
        assert_eq!(recent.list(), ["/inspected.img", "/opened.img"]);
        let saved = recent.0.load();
        let commander = saved
            .recent_files
            .list(crate::update::RecentMode::Commander);
        assert_eq!(commander, ["/opened.img"]);
    }

    #[cfg(any(feature = "gui", feature = "tui-update", feature = "tui"))]
    #[test]
    fn listing_styles_round_trip_and_an_old_config_has_none() {
        use crate::controller::browser::{ListingStyle, PaneServices};
        use crate::controller::commander::{BrowserPaneAction, RecentStore};
        let mut recent = CommanderRecent(MemoryConfig::default());
        assert_eq!(recent.listing_style("commander-left"), None);
        recent.remember_listing_style("commander-left", "combined-tree");
        assert_eq!(
            recent.listing_style("commander-left").as_deref(),
            Some("combined-tree")
        );
        // A pane remembers its choice under its view name and a new pane starts in it.
        struct Shared(Arc<std::sync::Mutex<MemoryConfig>>);
        impl ConfigStore for Shared {
            fn load(&self) -> crate::update::UpdateConfig {
                self.0.lock().unwrap().load()
            }
            fn save(&mut self, config: &crate::update::UpdateConfig) {
                self.0.lock().unwrap().save(config)
            }
        }
        let cfg = Arc::new(std::sync::Mutex::new(MemoryConfig::default()));
        let pane = |cfg: &Arc<std::sync::Mutex<MemoryConfig>>| {
            let mut p =
                crate::controller::browser::BrowserPaneController::with_services(PaneServices {
                    jobs: crate::model::job::JobRegistry::new(),
                    notify: Arc::new(NoNotify),
                    recent: Box::new(CommanderRecent(Shared(Arc::clone(cfg)))),
                });
            p.remember_style_as("explorer");
            p
        };
        let mut first = pane(&cfg);
        assert_eq!(first.listing_style(), ListingStyle::FlatList);
        first.apply(BrowserPaneAction::SetListingStyle(
            ListingStyle::TreeAndList,
        ));
        assert_eq!(pane(&cfg).listing_style(), ListingStyle::TreeAndList);
        // A config.json from before this field still parses, with no styles.
        let mut json = serde_json::to_value(crate::update::UpdateConfig::default()).unwrap();
        json.as_object_mut().unwrap().remove("listing_styles");
        let old: crate::update::UpdateConfig = serde_json::from_value(json).unwrap();
        assert!(old.listing_styles.is_empty());
    }
}
