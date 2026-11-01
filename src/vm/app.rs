//! `AppVm`: app-wide state every screen shares (`docs/native_frontends_plan.md` §4.2).
//!
//! Holds the log, the job registry, the shell's wake handle, and (in builds
//! with the `update` module) the recent-files history persisted in
//! config.json. Devices, settings and cross-tab handoffs move here in Phase 2.

use std::sync::Arc;

use super::log::{capture, LogStore};
use super::{Effect, ViewModel};
use crate::model::job::{JobRegistry, NoNotify, Notify};

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

pub struct AppVm {
    pub log: LogStore,
    log_auto_scroll: bool,
    jobs: JobRegistry,
    notify: Arc<dyn Notify>,
    #[cfg(any(feature = "gui", feature = "tui-update", feature = "tui"))]
    config: Box<dyn ConfigStore>,
}

impl AppVm {
    /// An app view-model waking the shell through `notify` and persisting to the user's config.json.
    pub fn new(notify: Arc<dyn Notify>) -> Self {
        AppVm {
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
        AppVm {
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

impl ViewModel for AppVm {
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
pub use recent::{ConfigStore, MemoryConfig, UserConfig};

#[cfg(any(feature = "gui", feature = "tui-update", feature = "tui"))]
mod recent {
    use super::AppVm;
    use crate::update::{RecentMode, UpdateConfig};

    /// Where `AppVm` loads and saves config.json; swapped for memory in tests.
    pub trait ConfigStore: Send {
        fn load(&self) -> UpdateConfig;
        fn save(&mut self, config: &UpdateConfig);
    }

    /// The user's real config.json.
    pub struct UserConfig;

    impl ConfigStore for UserConfig {
        fn load(&self) -> UpdateConfig {
            UpdateConfig::load()
        }

        fn save(&mut self, config: &UpdateConfig) {
            if let Err(e) = config.save() {
                log::warn!("could not save config.json: {e}");
            }
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

    impl AppVm {
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
        let mut app = AppVm::headless();
        app.log.info("one");
        app.log.warn("two");
        let effects = app.apply(AppAction::CopyLog);
        assert_eq!(effects, vec![Effect::CopyToClipboard(app.log.to_text())]);
        assert!(app.log.to_text().contains("[WARN] two"));
    }

    #[test]
    fn clear_log_and_auto_scroll() {
        let mut app = AppVm::headless();
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
        let mut app = AppVm::headless();
        app.remember(RecentMode::Inspect, "/a.img");
        app.remember(RecentMode::Inspect, "/b.img");
        let list = app.remember(RecentMode::Inspect, "/a.img");
        assert_eq!(list, ["/a.img", "/b.img"]);
        assert!(app.recent(RecentMode::Optical).is_empty());
        app.clear_recent();
        assert!(app.recent(RecentMode::Inspect).is_empty());
    }
}
