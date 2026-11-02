//! `AppController`: app-wide state every screen shares (`docs/native_frontends_plan.md` §4.2).
//!
//! Holds the log, the job registry, the shell's wake handle, and (in builds
//! with the `update` module) the recent-files history persisted in
//! config.json. Devices, settings and cross-tab handoffs move here in Phase 2.

use std::sync::Arc;

use super::log::{capture, LogStore};
use super::{Controller, Effect};
use crate::model::job::{JobRegistry, NoNotify};

// What the shared progress widgets draw, so views never name the model (CONTRIBUTING.md MVC).
pub use crate::model::file_types::{archives_picker_exts, DISK_IMAGE_EXTS, MAC_ARCHIVE_EXTS};
/// The wake a shell implements so jobs and log lines can repaint it.
pub use crate::model::job::Notify;
pub use crate::model::job::Snapshot;
pub use crate::model::rate_tracker::RateTracker;
pub use crate::model::size_mode::SizeMode;
pub use disk_core::display::format_size;

/// The CHD codec and profile choices the shared CHD options control offers.
#[cfg(feature = "chd")]
pub mod chd {
    pub use crate::rbformats::chd_options::{
        codec_label, codec_long_name, is_codec_supported, ChdOptions, ChdProfile,
    };
    pub use libchdman_rs::{
        CHD_CODEC_CD_FLAC, CHD_CODEC_CD_LZMA, CHD_CODEC_CD_ZLIB, CHD_CODEC_CD_ZSTD, CHD_CODEC_FLAC,
        CHD_CODEC_HUFF, CHD_CODEC_LZMA, CHD_CODEC_NONE, CHD_CODEC_ZLIB, CHD_CODEC_ZSTD,
    };
}

/// Stable widget ids, shared by every shell's accessibility identifiers.
pub mod ids {
    pub const LOG_COPY: &str = "log.copy";
    pub const LOG_CLEAR: &str = "log.clear";
    pub const LOG_AUTO_SCROLL: &str = "log.auto_scroll";
    pub const LOG_LIST: &str = "log.list";
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppAction {
    CopyLog,
    ClearLog,
    SetLogAutoScroll(bool),
}

pub struct AppController {
    pub log: LogStore,
    log_auto_scroll: bool,
    jobs: JobRegistry,
    notify: Arc<dyn Notify>,
    #[cfg(any(feature = "gui", feature = "tui-update", feature = "tui"))]
    config: Box<dyn ConfigStore>,
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
            config: Box::new(UserConfig),
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
            config: Box::<MemoryConfig>::default(),
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
        }
    }

    fn tick(&mut self) -> Vec<Effect> {
        self.log.extend_from_capture(capture::drain());
        Vec::new()
    }
}

#[cfg(any(feature = "gui", feature = "tui-update", feature = "tui"))]
pub use recent::{CommanderRecent, ConfigStore, MemoryConfig, UserConfig};

#[cfg(any(feature = "gui", feature = "tui-update", feature = "tui"))]
mod recent {
    use super::AppController;
    use crate::update::{RecentMode, UpdateConfig};

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

    impl AppController {
        /// The recent-files list for `mode`, newest first.
        pub fn recent(&self, mode: RecentMode) -> Vec<String> {
            self.config.load().recent_files.list(mode).to_vec()
        }

        /// Record a successful open of `path` and return the updated list for `mode`.
        pub fn remember(&mut self, mode: RecentMode, path: &str) -> Vec<String> {
            let mut cfg = self.config.load();
            cfg.remember_file(mode, path);
            self.config.save(&cfg);
            cfg.recent_files.list(mode).to_vec()
        }

        /// Forget every mode's recent files.
        pub fn clear_recent(&mut self) {
            let mut cfg = self.config.load();
            cfg.recent_files.clear();
            self.config.save(&cfg);
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
