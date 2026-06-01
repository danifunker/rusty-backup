//! Update checking functionality

use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Deserialize, Serialize)]
pub struct UpdateConfig {
    pub update_check: UpdateCheckConfig,
    /// Last-used CHD codec spec (chdman-style, e.g. `"lzma,zlib,huff,flac"`).
    /// `None` = use the profile default.
    #[serde(default)]
    pub last_chd_codecs: Option<String>,
    /// Last-used CHD hunk size in bytes. `None` = use the profile default.
    #[serde(default)]
    pub last_chd_hunk_size: Option<u32>,
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

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct GithubRelease {
    tag_name: String,
    html_url: String,
    #[serde(default)]
    assets: Vec<GithubAsset>,
}

#[derive(Debug, Deserialize)]
struct GithubAsset {
    name: String,
    browser_download_url: String,
}

/// Asset-name arch tag used in the Windows release ZIPs
/// (`Rusty-Backup-windows-<tag>-<version>.zip`). `x86` = i686, `x64` = x86_64.
fn windows_arch_tag() -> &'static str {
    if cfg!(target_arch = "x86") {
        "x86"
    } else {
        "x64"
    }
}

impl Default for UpdateConfig {
    fn default() -> Self {
        Self {
            update_check: UpdateCheckConfig {
                enabled: true,
                repository_url: "https://github.com/danifunker/rusty-backup".to_string(),
            },
            last_chd_codecs: None,
            last_chd_hunk_size: None,
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

#[derive(Debug, Clone)]
pub struct UpdateInfo {
    pub current_version: String,
    pub latest_version: String,
    pub releases_url: String,
    pub is_outdated: bool,
    /// Download URL of the GUI ZIP asset matching this platform/arch, if the
    /// release published one. `None` on platforms/arches we don't self-update
    /// (or if the asset is missing). Drives the in-app "Download & Install
    /// Update" flow on Windows.
    pub asset_url: Option<String>,
    /// Download URL of the standalone `rb-cli` asset for this arch, if present.
    /// Updated best-effort alongside the GUI.
    pub cli_asset_url: Option<String>,
}

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

    // Match the GUI ZIP + CLI asset for this arch so the GUI can offer an
    // in-app update. Naming is set by .github/workflows/release.yml:
    //   Rusty-Backup-windows-<arch>-<version>.zip
    //   Rusty-Backup-CLI-windows-<arch>-<version>.exe
    let arch_pat = format!("windows-{}-", windows_arch_tag());
    let asset_url = release
        .assets
        .iter()
        .find(|a| {
            a.name.starts_with("Rusty-Backup-windows")
                && a.name.contains(&arch_pat)
                && a.name.to_ascii_lowercase().ends_with(".zip")
        })
        .map(|a| a.browser_download_url.clone());
    let cli_asset_url = release
        .assets
        .iter()
        .find(|a| a.name.starts_with("Rusty-Backup-CLI-windows") && a.name.contains(&arch_pat))
        .map(|a| a.browser_download_url.clone());

    Ok(UpdateInfo {
        current_version: current.to_string(),
        latest_version,
        releases_url: config.releases_url(),
        is_outdated,
        asset_url,
        cli_asset_url,
    })
}

/// Download a release asset into memory, reporting progress as
/// `(downloaded_bytes, total_bytes_if_known)`.
fn download_bytes(
    url: &str,
    progress: &dyn Fn(u64, Option<u64>),
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    use std::io::Read;

    let client = reqwest::blocking::Client::builder()
        .user_agent("Rusty-Backup")
        .build()?;
    let mut resp = client.get(url).send()?.error_for_status()?;
    let total = resp.content_length();
    let mut buf: Vec<u8> = Vec::with_capacity(total.unwrap_or(0) as usize);
    let mut chunk = [0u8; 65536];
    let mut downloaded: u64 = 0;
    loop {
        let n = resp.read(&mut chunk)?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        downloaded += n as u64;
        progress(downloaded, total);
    }
    Ok(buf)
}

/// Extract every file in a ZIP (flattened to basenames) into `dest`.
fn extract_zip_flat(
    bytes: &[u8],
    dest: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let reader = std::io::Cursor::new(bytes);
    let mut archive = zip::ZipArchive::new(reader)?;
    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;
        if entry.is_dir() {
            continue;
        }
        let name = entry
            .enclosed_name()
            .and_then(|p| p.file_name().map(|n| n.to_owned()))
            .ok_or("zip entry has no usable file name")?;
        let out_path = dest.join(name);
        let mut out = fs::File::create(&out_path)?;
        std::io::copy(&mut entry, &mut out)?;
    }
    Ok(())
}

/// Download the matched release ZIP, extract it, and replace the running
/// executable in place. On success the caller should prompt the user and then
/// call [`restart_app`] to relaunch into the new version.
///
/// Windows-only: the in-place running-exe replacement uses `self_replace`.
/// On other platforms this returns an error (macOS/Linux update via DMG /
/// AppImage instead).
pub fn download_and_apply_update(
    info: &UpdateInfo,
    progress: &dyn Fn(u64, Option<u64>),
) -> Result<(), Box<dyn std::error::Error>> {
    let asset_url = info
        .asset_url
        .as_ref()
        .ok_or("No downloadable update asset for this platform/arch")?;

    let zip_bytes = download_bytes(asset_url, progress)?;
    let staging = tempfile::Builder::new().prefix("rb-update-").tempdir()?;
    extract_zip_flat(&zip_bytes, staging.path())?;

    let new_exe = staging.path().join("rusty-backup.exe");
    if !new_exe.exists() {
        return Err("update archive did not contain rusty-backup.exe".into());
    }

    replace_running_exe(&new_exe)?;

    // Best-effort: refresh a side-by-side rb-cli.exe if the release shipped one
    // and we have a writable install dir. rb-cli is not running, so a plain
    // copy is safe. Failures here never abort the GUI update.
    if let Some(cli_url) = info.cli_asset_url.as_ref() {
        let _ = update_sidecar_cli(cli_url);
    }

    Ok(())
}

/// Replace the currently running executable's image with `new_exe`.
#[cfg(windows)]
fn replace_running_exe(new_exe: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    // self_replace handles the Windows "can't overwrite a running .exe" lock
    // by moving the running image aside and dropping the new one in place.
    self_replace::self_replace(new_exe)?;
    Ok(())
}

#[cfg(not(windows))]
fn replace_running_exe(_new_exe: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    Err("in-app self-update is only supported on Windows".into())
}

/// Download a fresh `rb-cli.exe` next to the installed one. Best-effort.
#[cfg(windows)]
fn update_sidecar_cli(cli_url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let exe = std::env::current_exe()?;
    let install_dir = exe.parent().ok_or("cannot determine install directory")?;
    // Installer places rb-cli.exe under bin\; portable ZIP keeps it alongside.
    let candidates = [
        install_dir.join("bin").join("rb-cli.exe"),
        install_dir.join("rb-cli.exe"),
    ];
    let Some(target) = candidates.into_iter().find(|p| p.exists()) else {
        return Ok(());
    };
    let bytes = download_bytes(cli_url, &|_, _| {})?;
    // rb-cli is not running -> a plain overwrite is safe.
    fs::write(&target, bytes)?;
    Ok(())
}

#[cfg(not(windows))]
fn update_sidecar_cli(_cli_url: &str) -> Result<(), Box<dyn std::error::Error>> {
    Ok(())
}

/// Relaunch the (now-updated) executable and exit the current process.
/// Call after [`download_and_apply_update`] succeeds and the user confirms.
#[cfg(windows)]
pub fn restart_app() -> ! {
    if let Ok(exe) = std::env::current_exe() {
        let _ = std::process::Command::new(exe).spawn();
    }
    std::process::exit(0);
}
