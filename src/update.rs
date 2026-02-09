//! Update checking functionality and configuration

use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Deserialize, Serialize)]
pub struct UpdateConfig {
    #[cfg(feature = "update-checker")]
    pub update_check: UpdateCheckConfig,
    #[serde(default)]
    pub chdman_path: Option<String>,
}

#[cfg(feature = "update-checker")]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct UpdateCheckConfig {
    pub enabled: bool,
    pub repository_url: String,
}

#[cfg(feature = "update-checker")]
impl UpdateCheckConfig {
    /// Get the API URL for checking releases
    pub fn api_url(&self) -> String {
        // Convert https://github.com/owner/repo to https://api.github.com/repos/owner/repo/releases/latest
        if let Some(path) = self.repository_url.strip_prefix("https://github.com/") {
            format!(
                "https://api.github.com/repos/{}/releases/latest",
                path.trim_end_matches('/')
            )
        } else {
            // Fallback if URL doesn't match expected format
            self.repository_url.clone()
        }
    }

    /// Get the releases page URL
    pub fn releases_url(&self) -> String {
        format!("{}/releases", self.repository_url.trim_end_matches('/'))
    }
}

#[cfg(feature = "update-checker")]
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct GithubRelease {
    tag_name: String,
    html_url: String,
}

impl Default for UpdateConfig {
    fn default() -> Self {
        Self {
            #[cfg(feature = "update-checker")]
            update_check: UpdateCheckConfig {
                enabled: true,
                repository_url: "https://github.com/danifunker/rusty-backup".to_string(),
            },
            chdman_path: None,
        }
    }
}

impl UpdateConfig {
    /// Get the user config directory path.
    ///
    /// On Linux, uses `real_user_home()` to resolve the correct config directory
    /// even when running elevated via pkexec (where `dirs::config_dir()` would
    /// return `/root/.config`).
    pub fn user_config_dir() -> Option<PathBuf> {
        #[cfg(target_os = "linux")]
        let config_dir = crate::os::linux::real_user_home().map(|h| h.join(".config"));
        #[cfg(not(target_os = "linux"))]
        let config_dir = dirs::config_dir();

        config_dir.map(|d| d.join("rusty-backup"))
    }

    /// Get the user config file path
    pub fn user_config_path() -> Option<PathBuf> {
        Self::user_config_dir().map(|dir| dir.join("config.json"))
    }

    /// Load configuration from config.json
    pub fn load() -> Self {
        // Try to load from user config directory first (highest priority)
        if let Some(user_config) = Self::user_config_path() {
            if let Ok(config) = Self::load_from_path(&user_config) {
                return config;
            }
        }

        // Try to load from current directory
        if let Ok(config) = Self::load_from_path("config.json") {
            return config;
        }

        // Try to load from executable directory
        if let Ok(exe_path) = std::env::current_exe() {
            if let Some(exe_dir) = exe_path.parent() {
                let config_path = exe_dir.join("config.json");
                if let Ok(config) = Self::load_from_path(&config_path) {
                    return config;
                }
            }
        }

        // Return default if no config found
        Self::default()
    }

    /// Save configuration to user config directory
    pub fn save(&self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(config_dir) = Self::user_config_dir() {
            // Create directory if it doesn't exist
            fs::create_dir_all(&config_dir)?;

            let config_path = config_dir.join("config.json");
            let json = serde_json::to_string_pretty(self)?;
            fs::write(config_path, json)?;
            Ok(())
        } else {
            Err("Could not determine user config directory".into())
        }
    }

    fn load_from_path(path: impl Into<PathBuf>) -> Result<Self, Box<dyn std::error::Error>> {
        let content = fs::read_to_string(path.into())?;
        let config: UpdateConfig = serde_json::from_str(&content)?;
        Ok(config)
    }
}

#[cfg(feature = "update-checker")]
#[derive(Debug, Clone)]
pub struct UpdateInfo {
    pub current_version: String,
    pub latest_version: String,
    pub releases_url: String,
    pub is_outdated: bool,
}

#[cfg(feature = "update-checker")]
/// Check for updates from GitHub releases
pub fn check_for_updates(
    config: &UpdateCheckConfig,
    current_version: &str,
) -> Result<UpdateInfo, Box<dyn std::error::Error>> {
    let client = reqwest::blocking::Client::builder()
        .user_agent("Rusty-Backup")
        .build()?;

    let api_url = config.api_url();
    let response = client.get(&api_url).send()?;
    let release: GithubRelease = response.json()?;

    // Remove 'v' prefix if present
    let latest_version = release.tag_name.trim_start_matches('v').to_string();
    let current = current_version.trim_start_matches('v');

    let is_outdated = latest_version != current;

    Ok(UpdateInfo {
        current_version: current.to_string(),
        latest_version,
        releases_url: config.releases_url(),
        is_outdated,
    })
}
