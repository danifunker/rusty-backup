//! The Settings dialog: update check, file associations (Windows) and clearing recent files.
//!
//! The form edits a copy of config.json's fields; Save writes them back (and on Windows
//! registers or removes the file associations first), Clear Recent Files clears and saves at
//! once. Config goes through a [`ConfigStore`], so tests never touch the user's file.

use super::app::{ConfigStore, UserConfig};
use super::{Controller, Effect};

/// Stable widget ids.
pub mod ids {
    pub const UPDATE_CHECK: &str = "settings.update_check";
    pub const REPO_URL: &str = "settings.repo_url";
    pub const FILE_ASSOCIATIONS: &str = "settings.file_associations";
    pub const CLEAR_RECENT: &str = "settings.clear_recent";
    pub const SAVE: &str = "settings.save";
    pub const CANCEL: &str = "settings.cancel";
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SettingsAction {
    /// Load the form from config.json and show it.
    Open,
    SetUpdateCheck(bool),
    SetRepoUrl(String),
    /// Windows only; elsewhere the value is kept but nothing is registered.
    SetFileAssociations(bool),
    ClearRecent,
    Save,
    Close,
}

pub struct SettingsController {
    config: Box<dyn ConfigStore>,
    open: bool,
    update_check: bool,
    repo_url: String,
    file_associations: bool,
    recent_count: usize,
    status: Option<String>,
    cleared_recents: bool,
}

impl Default for SettingsController {
    fn default() -> Self {
        Self::with_store(Box::new(UserConfig))
    }
}

impl SettingsController {
    /// A dialog reading and writing `config` (the user's config.json in the app, memory in tests).
    pub fn with_store(config: Box<dyn ConfigStore>) -> Self {
        SettingsController {
            config,
            open: false,
            update_check: false,
            repo_url: String::new(),
            file_associations: false,
            recent_count: 0,
            status: None,
            cleared_recents: false,
        }
    }

    pub fn is_open(&self) -> bool {
        self.open
    }

    pub fn update_check(&self) -> bool {
        self.update_check
    }

    pub fn repo_url(&self) -> &str {
        self.repo_url.as_str()
    }

    pub fn file_associations(&self) -> bool {
        self.file_associations
    }

    /// Recent-file entries across every mode, for the Clear button's label.
    pub fn recent_count(&self) -> usize {
        self.recent_count
    }

    /// The last Save or Clear result; an error starts with "Error".
    pub fn status(&self) -> Option<&str> {
        self.status.as_deref()
    }

    /// True once after Clear Recent Files, so the shell drops its tabs' in-memory lists.
    pub fn take_cleared_recents(&mut self) -> bool {
        std::mem::take(&mut self.cleared_recents)
    }

    fn load_form(&mut self) {
        let config = self.config.load();
        self.update_check = config.update_check.enabled;
        self.repo_url = config.update_check.repository_url;
        self.recent_count = config.recent_files.total();
        self.file_associations = config.file_associations_enabled;
        self.cleared_recents = false;
        self.status = None;
        self.open = true;
    }

    fn clear_recent(&mut self) {
        let mut config = self.config.load();
        let count = config.recent_files.total();
        config.recent_files.clear();
        match self.config.try_save(&config) {
            Ok(()) => {
                self.recent_count = 0;
                self.cleared_recents = true;
                self.status = Some(if count == 1 {
                    "Cleared 1 recent file.".to_string()
                } else {
                    format!("Cleared {count} recent files.")
                });
            }
            Err(e) => self.status = Some(format!("Error clearing recent files: {e}")),
        }
    }

    fn save(&mut self) {
        let mut config = self.config.load();
        config.update_check.enabled = self.update_check;
        config.update_check.repository_url = self.repo_url.trim().to_string();
        #[cfg(windows)]
        {
            let was_enabled = config.file_associations_enabled;
            config.file_associations_enabled = self.file_associations;
            if self.file_associations {
                let _ = crate::os::file_assoc::register_file_associations();
                config.assoc_registered_version = Some(env!("APP_VERSION").to_string());
            } else if was_enabled {
                let _ = crate::os::file_assoc::unregister_file_associations();
                config.assoc_registered_version = None;
            }
        }
        self.status = Some(match self.config.try_save(&config) {
            Ok(()) => {
                "Settings saved successfully! Restart to apply update check changes.".to_string()
            }
            Err(e) => format!("Error saving settings: {e}"),
        });
    }
}

impl Controller for SettingsController {
    type Action = SettingsAction;

    fn apply(&mut self, action: SettingsAction) -> Vec<Effect> {
        match action {
            SettingsAction::Open => self.load_form(),
            SettingsAction::SetUpdateCheck(on) => self.update_check = on,
            SettingsAction::SetRepoUrl(url) => self.repo_url = url,
            SettingsAction::SetFileAssociations(on) => self.file_associations = on,
            SettingsAction::ClearRecent => self.clear_recent(),
            SettingsAction::Save => self.save(),
            SettingsAction::Close => self.open = false,
        }
        Vec::new()
    }

    fn tick(&mut self) -> Vec<Effect> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::app::MemoryConfig;
    use crate::update::RecentMode;

    #[test]
    fn save_writes_the_form_and_clear_empties_every_list() {
        let mut store = MemoryConfig::default();
        let mut cfg = store.load();
        cfg.remember_file(RecentMode::Inspect, "/a.img");
        cfg.remember_file(RecentMode::Optical, "/b.iso");
        store.save(&cfg);
        let mut s = SettingsController::with_store(Box::new(store));
        s.apply(SettingsAction::Open);
        assert!(s.is_open());
        assert_eq!(s.recent_count(), 2);
        s.apply(SettingsAction::SetUpdateCheck(true));
        s.apply(SettingsAction::SetRepoUrl(
            "  https://github.com/o/r ".into(),
        ));
        s.apply(SettingsAction::Save);
        assert!(s.status().unwrap().starts_with("Settings saved"));
        let saved = s.config.load();
        assert!(saved.update_check.enabled);
        assert_eq!(saved.update_check.repository_url, "https://github.com/o/r");
        s.apply(SettingsAction::ClearRecent);
        assert_eq!(s.status(), Some("Cleared 2 recent files."));
        assert!(s.take_cleared_recents());
        assert!(!s.take_cleared_recents());
        assert_eq!(s.config.load().recent_files.total(), 0);
        s.apply(SettingsAction::Close);
        assert!(!s.is_open());
    }
}
