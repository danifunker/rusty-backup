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
    /// Windows only: whether disk-image file associations are registered for
    /// this user. Set by the installer (`--register-file-associations`) or the
    /// Settings toggle; drives the launch-time re-registration that picks up
    /// newly supported extensions after a self-update.
    #[serde(default)]
    pub file_associations_enabled: bool,
    /// `APP_VERSION` the associations were last registered for. When this no
    /// longer matches the running build, associations are refreshed on launch
    /// so a self-update that adds an extension registers it without a reinstall.
    #[serde(default)]
    pub assoc_registered_version: Option<String>,
    /// Most-recently-used rb-daemon addresses (`host:port`), newest first. Drives
    /// the GUI Optical tab's "Add remote daemon" quick-pick list. Capped to a
    /// handful of entries.
    #[serde(default)]
    pub recent_daemon_addrs: Vec<String>,
    /// Per-mode recent-files history for the GUI "Recent" quick-pick lists
    /// (Inspect, Restore, Optical, Archives, Commander). Each list is
    /// newest-first, deduped, and capped; see [`RecentFiles`].
    #[serde(default)]
    pub recent_files: RecentFiles,
}

/// Per-mode most-recently-used file/folder history for the GUI's "Recent"
/// pickers. Each mode keeps its own list so, e.g., disc images opened on the
/// Optical tab don't clutter the Inspect tab's history. Lists are newest-first,
/// deduped, and capped to [`RecentFiles::CAP`]; paths are stored as display
/// strings. Every field is `#[serde(default)]` so a config.json written by an
/// older build (no `recent_files` key, or missing a mode) still parses.
#[derive(Debug, Default, Deserialize, Serialize, Clone)]
pub struct RecentFiles {
    #[serde(default)]
    pub inspect: Vec<String>,
    #[serde(default)]
    pub restore: Vec<String>,
    #[serde(default)]
    pub optical: Vec<String>,
    #[serde(default)]
    pub archives: Vec<String>,
    #[serde(default)]
    pub commander: Vec<String>,
    #[serde(default)]
    pub backup: Vec<String>,
}

impl RecentFiles {
    /// Maximum entries retained per mode.
    pub const CAP: usize = 10;

    /// The (newest-first) recent list for `mode`.
    pub fn list(&self, mode: RecentMode) -> &[String] {
        match mode {
            RecentMode::Inspect => &self.inspect,
            RecentMode::Restore => &self.restore,
            RecentMode::Optical => &self.optical,
            RecentMode::Archives => &self.archives,
            RecentMode::Commander => &self.commander,
            RecentMode::Backup => &self.backup,
        }
    }

    fn list_mut(&mut self, mode: RecentMode) -> &mut Vec<String> {
        match mode {
            RecentMode::Inspect => &mut self.inspect,
            RecentMode::Restore => &mut self.restore,
            RecentMode::Optical => &mut self.optical,
            RecentMode::Archives => &mut self.archives,
            RecentMode::Commander => &mut self.commander,
            RecentMode::Backup => &mut self.backup,
        }
    }

    /// Total remembered entries across every mode. Drives the Settings "Clear
    /// Recent Files" affordance (count shown, button disabled when zero).
    pub fn total(&self) -> usize {
        self.inspect.len()
            + self.restore.len()
            + self.optical.len()
            + self.archives.len()
            + self.commander.len()
            + self.backup.len()
    }

    /// Forget every mode's recent list.
    pub fn clear(&mut self) {
        *self = Self::default();
    }
}

/// Which GUI mode a recent-files list belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecentMode {
    Inspect,
    Restore,
    Optical,
    Archives,
    Commander,
    Backup,
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
            file_associations_enabled: false,
            assoc_registered_version: None,
            recent_daemon_addrs: Vec::new(),
            recent_files: RecentFiles::default(),
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

    /// Record a successfully-used rb-daemon address in the MRU list (dedup,
    /// newest first, capped). No-op for a blank address.
    pub fn remember_daemon(&mut self, addr: &str) {
        let addr = addr.trim();
        if addr.is_empty() {
            return;
        }
        self.recent_daemon_addrs.retain(|a| a != addr);
        self.recent_daemon_addrs.insert(0, addr.to_string());
        self.recent_daemon_addrs.truncate(8);
    }

    /// Record a successfully-opened path in the per-mode recent-files MRU
    /// (dedup, newest first, capped to [`RecentFiles::CAP`]). No-op for a blank
    /// path.
    pub fn remember_file(&mut self, mode: RecentMode, path: &str) {
        let path = path.trim();
        if path.is_empty() {
            return;
        }
        let list = self.recent_files.list_mut(mode);
        list.retain(|p| p != path);
        list.insert(0, path.to_string());
        list.truncate(RecentFiles::CAP);
    }
}

/// Load the persisted recent-files list for `mode` (newest-first). A thin
/// convenience over `UpdateConfig::load` shared by every front-end (GUI, TUI).
pub fn load_recent(mode: RecentMode) -> Vec<String> {
    UpdateConfig::load().recent_files.list(mode).to_vec()
}

/// Record `path` as the most-recently-opened entry for `mode` (move-to-front,
/// deduped, capped), persist it, and return the updated newest-first list.
/// The single place any front-end should call on a successful open, so the MRU
/// always reflects the *most* recently opened, not a static order.
pub fn push_recent(mode: RecentMode, path: &str) -> Vec<String> {
    let mut cfg = UpdateConfig::load();
    cfg.remember_file(mode, path);
    let list = cfg.recent_files.list(mode).to_vec();
    let _ = cfg.save();
    list
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

/// Whether `latest` (a release tag) is newer than `current` (this build); a
/// `-dev` build is never outdated, so `update --apply` cannot downgrade it.
pub fn release_is_newer(latest: &str, current: &str) -> bool {
    let latest = latest.trim_start_matches('v');
    let current = current.trim_start_matches('v');
    if latest == current {
        return false;
    }
    // A development build is ahead of every release by definition.
    if current.ends_with("-dev") {
        return false;
    }
    let numeric = |s: &str| -> Option<Vec<u64>> {
        s.split('.')
            .map(|part| part.parse::<u64>().ok())
            .collect::<Option<Vec<u64>>>()
    };
    match (numeric(latest), numeric(current)) {
        // Date tags (`YYYYMMDDHHMM`) and dotted versions both order numerically.
        (Some(l), Some(c)) => l > c,
        // Unparseable on either side: fall back to "different means newer".
        _ => true,
    }
}

/// Check for updates from GitHub releases.
///
/// Uses reqwest, so it's gated on `gui` / `tui-update`. A plain-`tui` build
/// (e.g. the vintage macOS 10.7 CLI) compiles the MRU/config parts of this
/// module but not this network check.
#[cfg(any(feature = "gui", feature = "tui-update"))]
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

    let is_outdated = release_is_newer(&latest_version, current);

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
/// `(downloaded_bytes, total_bytes_if_known)`. reqwest-backed, so gated with the
/// rest of the network updater.
#[cfg(any(feature = "gui", feature = "tui-update"))]
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
#[cfg(any(feature = "gui", feature = "tui-update"))]
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

/// macOS / Linux in-place self-update for the **`rb-cli` binary**, via the
/// classic temp-file + `rename` swap: download the CLI asset, extract the
/// `rb-cli` executable, write it beside the running binary, mark it executable,
/// then atomically `rename` it over the running file. On Unix a running
/// program keeps its old inode open, so replacing the directory entry is safe;
/// the new bytes take effect on the next launch (or the re-exec the caller
/// performs). Returns the path that was replaced.
///
/// Conservative by design: it only proceeds when it can positively extract a
/// non-empty `rb-cli` executable from a recognized asset (`.zip`, `.tar.gz`, or
/// a raw ELF/Mach-O binary). Anything ambiguous errors out so the caller can
/// fall back to printing the download link rather than clobbering the binary.
#[cfg(all(unix, any(feature = "tui-update", feature = "gui")))]
pub fn download_and_apply_cli_update_unix(
    info: &UpdateInfo,
    progress: &dyn Fn(u64, Option<u64>),
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    use std::os::unix::fs::PermissionsExt;

    let asset_url = info
        .cli_asset_url
        .as_ref()
        .or(info.asset_url.as_ref())
        .ok_or("No downloadable rb-cli asset for this platform/arch")?;

    let exe = std::env::current_exe()?;
    let exe_name = exe
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("rb-cli")
        .to_string();
    let install_dir = exe
        .parent()
        .ok_or("cannot determine the install directory")?
        .to_path_buf();

    let bytes = download_bytes(asset_url, progress)?;
    let new_bytes = extract_cli_binary(asset_url, &bytes, &exe_name)?;
    if new_bytes.len() < 4 || !looks_like_unix_executable(&new_bytes) {
        return Err("downloaded asset did not contain a valid rb-cli executable".into());
    }

    // Stage into the install dir (same filesystem) so the final rename is atomic.
    let tmp = install_dir.join(format!(".{exe_name}.update.tmp"));
    {
        let mut f = fs::File::create(&tmp)?;
        use std::io::Write;
        f.write_all(&new_bytes)?;
        f.flush()?;
        f.sync_all()?;
        let mut perms = f.metadata()?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&tmp, perms)?;
    }
    // Atomic replace of the running binary's directory entry.
    if let Err(e) = fs::rename(&tmp, &exe) {
        let _ = fs::remove_file(&tmp);
        return Err(format!("could not replace {}: {e}", exe.display()).into());
    }
    Ok(exe)
}

/// Extract the `rb-cli` executable bytes from a downloaded asset. Handles a
/// `.zip`, a `.tar.gz`/`.tgz`, or a raw binary (the URL has no archive
/// extension and the bytes carry an ELF / Mach-O magic).
#[cfg(all(unix, any(feature = "tui-update", feature = "gui")))]
fn extract_cli_binary(
    url: &str,
    bytes: &[u8],
    exe_name: &str,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let lower = url.to_ascii_lowercase();
    let matches_name = |name: &str| {
        let leaf = name.rsplit('/').next().unwrap_or(name);
        leaf == exe_name || leaf == "rb-cli"
    };
    if lower.ends_with(".zip") {
        let reader = std::io::Cursor::new(bytes);
        let mut zip = zip::ZipArchive::new(reader)?;
        for i in 0..zip.len() {
            let mut entry = zip.by_index(i)?;
            if entry.is_file() && matches_name(entry.name()) {
                use std::io::Read;
                let mut out = Vec::new();
                entry.read_to_end(&mut out)?;
                return Ok(out);
            }
        }
        Err("rb-cli not found inside the .zip asset".into())
    } else if lower.ends_with(".tar.gz") || lower.ends_with(".tgz") {
        let gz = flate2::read::GzDecoder::new(std::io::Cursor::new(bytes));
        let mut tar = tar::Archive::new(gz);
        for entry in tar.entries()? {
            let mut entry = entry?;
            let path = entry.path()?.to_string_lossy().into_owned();
            if matches_name(&path) {
                use std::io::Read;
                let mut out = Vec::new();
                entry.read_to_end(&mut out)?;
                return Ok(out);
            }
        }
        Err("rb-cli not found inside the .tar.gz asset".into())
    } else if looks_like_unix_executable(bytes) {
        // A bare binary asset.
        Ok(bytes.to_vec())
    } else {
        Err("unrecognized rb-cli asset format (expected .zip, .tar.gz, or a raw binary)".into())
    }
}

/// True when `bytes` begins with an ELF (Linux) or Mach-O (macOS) magic number.
#[cfg(all(unix, any(feature = "tui-update", feature = "gui")))]
fn looks_like_unix_executable(bytes: &[u8]) -> bool {
    if bytes.len() < 4 {
        return false;
    }
    let m = &bytes[..4];
    // ELF: 0x7F 'E' 'L' 'F'. Mach-O (thin/fat, LE/BE): feedface / feedfacf /
    // cafebabe / cffaedfe / cefaedfe.
    m == [0x7f, b'E', b'L', b'F']
        || matches!(
            u32::from_be_bytes([m[0], m[1], m[2], m[3]]),
            0xFEED_FACE | 0xFEED_FACF | 0xCAFE_BABE | 0xCFFA_EDFE | 0xCEFA_EDFE
        )
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
/// reqwest-backed (via download_bytes), so gated with the rest of the updater —
/// the vintage Win7 build (no gui / tui-update) uses the stub below.
#[cfg(all(windows, any(feature = "gui", feature = "tui-update")))]
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

#[cfg(not(all(windows, any(feature = "gui", feature = "tui-update"))))]
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

/// Tests for the Unix CLI self-replace extraction (the `--apply` core). Gated
/// to the same cfg as the code under test so `cargo test` (default features
/// include `gui`) compiles and runs them.
#[cfg(all(test, unix, any(feature = "tui-update", feature = "gui")))]
mod cli_update_tests {
    use super::{extract_cli_binary, looks_like_unix_executable};
    use std::io::Write;

    /// A minimal ELF header magic + filler so the bytes read as an executable.
    fn fake_elf() -> Vec<u8> {
        let mut v = vec![0x7f, b'E', b'L', b'F'];
        v.extend_from_slice(&[0u8; 60]);
        v
    }

    #[test]
    fn detects_elf_and_macho_magic() {
        assert!(looks_like_unix_executable(&fake_elf()));
        assert!(looks_like_unix_executable(&[0xfe, 0xed, 0xfa, 0xcf, 0, 0])); // Mach-O 64
        assert!(!looks_like_unix_executable(b"PK\x03\x04")); // a zip header
        assert!(!looks_like_unix_executable(&[0u8; 2]));
    }

    #[test]
    fn extracts_rb_cli_from_zip() {
        let elf = fake_elf();
        let mut buf = Vec::new();
        {
            let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
            let opts: zip::write::FileOptions<'_, ()> = zip::write::FileOptions::default();
            zip.start_file("noise.txt", opts).unwrap();
            zip.write_all(b"ignore me").unwrap();
            zip.start_file("rb-cli", opts).unwrap();
            zip.write_all(&elf).unwrap();
            zip.finish().unwrap();
        }
        let got = extract_cli_binary("https://x/Rusty-Backup-linux.zip", &buf, "rb-cli").unwrap();
        assert_eq!(got, elf);
    }

    #[test]
    fn extracts_rb_cli_from_tar_gz() {
        let elf = fake_elf();
        let mut gz = Vec::new();
        {
            let enc = flate2::write::GzEncoder::new(&mut gz, flate2::Compression::default());
            let mut tar = tar::Builder::new(enc);
            let mut header = tar::Header::new_gnu();
            header.set_size(elf.len() as u64);
            header.set_mode(0o755);
            header.set_cksum();
            tar.append_data(&mut header, "rb-cli", elf.as_slice())
                .unwrap();
            tar.into_inner().unwrap().finish().unwrap();
        }
        let got = extract_cli_binary("https://x/rb-cli.tar.gz", &gz, "rb-cli").unwrap();
        assert_eq!(got, elf);
    }

    #[test]
    fn raw_binary_asset_passes_through() {
        let elf = fake_elf();
        let got = extract_cli_binary("https://x/rb-cli", &elf, "rb-cli").unwrap();
        assert_eq!(got, elf);
    }

    #[test]
    fn rejects_zip_without_rb_cli() {
        let mut buf = Vec::new();
        {
            let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
            let opts: zip::write::FileOptions<'_, ()> = zip::write::FileOptions::default();
            zip.start_file("rusty-backup.exe", opts).unwrap();
            zip.write_all(b"not the cli").unwrap();
            zip.finish().unwrap();
        }
        assert!(extract_cli_binary("https://x/win.zip", &buf, "rb-cli").is_err());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn release_is_newer_orders_tags_and_spares_dev_builds() {
        // Date tags order numerically, and equal means up to date.
        assert!(release_is_newer("202609011200", "202608311200"));
        assert!(!release_is_newer("202608311200", "202609011200"));
        assert!(!release_is_newer("v202609011200", "202609011200"));
        // Dotted versions the same way.
        assert!(release_is_newer("0.2.0", "0.1.9"));
        assert!(!release_is_newer("0.1.9", "0.2.0"));
        // A -dev build is never "outdated" (the old check offered a downgrade).
        assert!(!release_is_newer("202609011200", "0.1.0-dev"));
        assert!(!release_is_newer("202609011200", "202609020000-dev"));
    }

    #[test]
    fn remember_daemon_is_mru_deduped_capped() {
        let mut cfg = UpdateConfig::default();
        cfg.remember_daemon("a:1");
        cfg.remember_daemon("b:2");
        // Re-using an address moves it to the front (no duplicate).
        cfg.remember_daemon("a:1");
        assert_eq!(cfg.recent_daemon_addrs, vec!["a:1", "b:2"]);
        // Blank / whitespace is ignored.
        cfg.remember_daemon("   ");
        assert_eq!(cfg.recent_daemon_addrs.len(), 2);
        // Capped at 8, newest first.
        for i in 0..10 {
            cfg.remember_daemon(&format!("h:{i}"));
        }
        assert_eq!(cfg.recent_daemon_addrs.len(), 8);
        assert_eq!(cfg.recent_daemon_addrs[0], "h:9");
    }

    #[test]
    fn remember_file_is_per_mode_mru_deduped_capped() {
        let mut cfg = UpdateConfig::default();
        cfg.remember_file(RecentMode::Inspect, "/a.img");
        cfg.remember_file(RecentMode::Inspect, "/b.img");
        // Re-using a path moves it to the front (no duplicate).
        cfg.remember_file(RecentMode::Inspect, "/a.img");
        assert_eq!(
            cfg.recent_files.list(RecentMode::Inspect),
            ["/a.img", "/b.img"]
        );
        // Lists are per-mode: Optical is independent of Inspect.
        cfg.remember_file(RecentMode::Optical, "/disc.iso");
        assert_eq!(cfg.recent_files.list(RecentMode::Optical), ["/disc.iso"]);
        assert_eq!(cfg.recent_files.list(RecentMode::Inspect).len(), 2);
        // Blank / whitespace is ignored.
        cfg.remember_file(RecentMode::Inspect, "   ");
        assert_eq!(cfg.recent_files.list(RecentMode::Inspect).len(), 2);
        // Capped at CAP, newest first.
        for i in 0..(RecentFiles::CAP + 5) {
            cfg.remember_file(RecentMode::Inspect, &format!("/f{i}.img"));
        }
        assert_eq!(
            cfg.recent_files.list(RecentMode::Inspect).len(),
            RecentFiles::CAP
        );
        assert_eq!(
            cfg.recent_files.list(RecentMode::Inspect)[0],
            format!("/f{}.img", RecentFiles::CAP + 4)
        );
    }

    #[test]
    fn recent_files_total_and_clear() {
        let mut cfg = UpdateConfig::default();
        assert_eq!(cfg.recent_files.total(), 0);
        cfg.remember_file(RecentMode::Inspect, "/a.img");
        cfg.remember_file(RecentMode::Inspect, "/b.img");
        cfg.remember_file(RecentMode::Optical, "/disc.iso");
        cfg.remember_file(RecentMode::Commander, "/c.hda");
        // total() sums across every mode.
        assert_eq!(cfg.recent_files.total(), 4);
        // clear() forgets every mode at once.
        cfg.recent_files.clear();
        assert_eq!(cfg.recent_files.total(), 0);
        assert!(cfg.recent_files.list(RecentMode::Inspect).is_empty());
        assert!(cfg.recent_files.list(RecentMode::Optical).is_empty());
        assert!(cfg.recent_files.list(RecentMode::Commander).is_empty());
    }
}
