//! Update checking functionality

use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Deserialize, Serialize)]
pub struct UpdateConfig {
    pub update_check: UpdateCheckConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct UpdateCheckConfig {
    pub enabled: bool,
    pub repository_url: String,
}

impl UpdateCheckConfig {
    /// Get the API URL for checking releases
    pub fn api_url(&self) -> String {
        // Convert https://github.com/owner/repo to https://api.github.com/repos/owner/repo/releases/latest
        if let Some(path) = self.repository_url.strip_prefix("https://github.com/") {
            format!("https://api.github.com/repos/{}/releases/latest", path.trim_end_matches('/'))
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

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct GithubRelease {
    tag_name: String,
    html_url: String,
}

impl Default for UpdateConfig {
    fn default() -> Self {
        Self {
            update_check: UpdateCheckConfig {
                enabled: true,
                repository_url: "https://github.com/dani/rusty-backup".to_string(),
            },
        }
    }
}

impl UpdateConfig {
    /// Load configuration from config.json
    pub fn load() -> Self {
        // Try to load from current directory first
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

    fn load_from_path(path: impl Into<PathBuf>) -> Result<Self, Box<dyn std::error::Error>> {
        let content = fs::read_to_string(path.into())?;
        let config: UpdateConfig = serde_json::from_str(&content)?;
        Ok(config)
    }
}

#[derive(Debug, Clone)]
pub struct UpdateInfo {
    pub current_version: String,
    pub latest_version: String,
    pub releases_url: String,
    pub is_outdated: bool,
}

/// Check for updates from GitHub releases
pub fn check_for_updates(config: &UpdateCheckConfig, current_version: &str) -> Result<UpdateInfo, Box<dyn std::error::Error>> {
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
