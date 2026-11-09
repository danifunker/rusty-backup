//! The app shell's rules: which tab is up, where a dropped file goes, which tab's job locks the
//! others, the backup folder the Backup-aware tabs share, the device list and the
//! physical-device elevation gate. The egui shell (`src/gui/mod.rs`) draws these.

use std::path::{Path, PathBuf};

use super::AppController;
use crate::controller::job_outcome;
use crate::model::job::{Job, Snapshot};

pub use crate::device::DiskDevice;

/// The top-level tabs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppTab {
    Backup,
    Restore,
    /// Build Disk: a new partitioned disk or volume.
    Build,
    Inspect,
    /// Convert: images to other formats, one or a folder at a time.
    Convert,
    Optical,
    Archives,
}

impl AppTab {
    /// The tabs in the order every window shell shows them (D8).
    pub const ALL: [AppTab; 7] = [
        AppTab::Backup,
        AppTab::Restore,
        AppTab::Build,
        AppTab::Inspect,
        AppTab::Convert,
        AppTab::Optical,
        AppTab::Archives,
    ];

    /// The tab's title.
    pub fn label(self) -> &'static str {
        match self {
            AppTab::Backup => "Backup",
            AppTab::Restore => "Restore",
            AppTab::Build => "Build Disk",
            AppTab::Inspect => "Inspect",
            AppTab::Convert => "Convert",
            AppTab::Optical => "Optical",
            AppTab::Archives => "Archives",
        }
    }

    /// The screen's toolbar icon, a file stem in `assets/icons/toolbar/` (D9).
    pub fn icon(self) -> &'static str {
        match self {
            AppTab::Backup => "backup",
            AppTab::Restore => "restore",
            AppTab::Build => "build",
            AppTab::Inspect => "inspect",
            AppTab::Convert => "convert",
            AppTab::Optical => "disc",
            AppTab::Archives => "archive",
        }
    }
}

/// What the tabs a drop may go to are doing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DropTargets {
    pub inspect_remote: bool,
    pub inspect_loaded: bool,
    pub optical_browsing: bool,
}

/// Where a dropped file goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DropRoute {
    /// Open it in Inspect and switch there.
    Inspect(PathBuf),
    Optical(PathBuf),
    Archives(PathBuf),
    Restore(PathBuf),
    Backup(PathBuf),
    Convert(PathBuf),
    /// The active tab keeps it (Inspect's open volume takes drops itself).
    Ignore,
}

/// What a Backup-aware tab does with the backup folder the tabs share.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShareStep {
    /// The tab opened a new backup: the other tab loads it too.
    Publish(PathBuf),
    /// The tab is empty: it loads the shared backup.
    Adopt(PathBuf),
    Nothing,
}

/// Whether the physical-device elevation gate applies, and its state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElevationGate {
    /// Not a gated platform (macOS escalates per operation): "Refresh Devices" applies.
    NotApplicable,
    /// Not elevated: show the elevation button.
    NeedsElevation,
    /// Elevated: devices are available, "Refresh Devices" applies.
    Elevated,
    /// A build without physical devices.
    Disabled,
}

/// Why physical devices are unavailable in this build, for a hover text.
pub const PHYSICAL_DEVICES_DISABLED: &str = crate::os::PHYSICAL_DEVICES_DISABLED;

/// The current elevation gate; macOS is always `NotApplicable`.
pub fn elevation_gate() -> ElevationGate {
    if !crate::os::PHYSICAL_DEVICES_ENABLED {
        return ElevationGate::Disabled;
    }
    if cfg!(any(windows, target_os = "linux")) {
        if crate::os::is_elevated() {
            ElevationGate::Elevated
        } else {
            ElevationGate::NeedsElevation
        }
    } else {
        ElevationGate::NotApplicable
    }
}

/// Whether this process runs as root / administrator (raw device access needs it).
pub fn running_elevated() -> bool {
    crate::os::is_elevated()
}

/// From an AppImage, install `<stem>.desktop` and its icon so the dock can match the window.
/// Skipped when elevated: a root instance must not write into the user's data dirs.
pub fn install_appimage_entry(stem: &str, desktop: &str, icon_png: &[u8]) {
    #[cfg(target_os = "linux")]
    if !running_elevated() {
        crate::os::linux::install_appimage_entry(stem, desktop, icon_png);
    }
    #[cfg(not(target_os = "linux"))]
    let _ = (stem, desktop, icon_png);
}

/// True only when the host OS version was read and is older than `major.minor`.
pub fn host_known_older_than(major: u32, minor: u32) -> bool {
    let host = crate::os::host_version::HostVersion::detect();
    host.version.is_some() && !host.at_least(major, minor)
}

/// Whether physical-device controls are usable now (Windows needs elevation to list them).
pub fn physical_devices_available() -> bool {
    if !crate::os::PHYSICAL_DEVICES_ENABLED {
        return false;
    }
    !cfg!(windows) || crate::os::is_elevated()
}

/// A device dropdown's hint when it lists nothing.
pub fn no_devices_hint() -> &'static str {
    if !crate::os::PHYSICAL_DEVICES_ENABLED {
        return "Physical devices are not available in this build. Use disk image files.";
    }
    if cfg!(windows) && !crate::os::is_elevated() {
        "Click \"Show Physical Devices\" (top bar) to access disks."
    } else {
        "No physical devices detected."
    }
}

/// The top bar's elevation button label.
pub fn elevation_button_label() -> &'static str {
    if cfg!(windows) {
        "Show Physical Devices"
    } else {
        "Unlock Physical Devices"
    }
}

/// The elevation button's hover text.
pub fn elevation_button_hover() -> &'static str {
    if cfg!(windows) {
        "Restart with administrator privileges to access physical disk devices \
         (will prompt for elevation)."
    } else {
        "Restart with root privileges (pkexec) to read and write physical disk \
         devices. Disk image files do not need this."
    }
}

/// The note beside a physical-device picker while unprivileged, naming the remedy.
/// The Linux "restart as root?" question; answer with `AppAction::ConfirmElevation`.
pub fn elevation_confirm() -> crate::controller::ConfirmSpec {
    crate::controller::ConfirmSpec {
        purpose: "elevate",
        title: "Unlock Physical Devices".to_string(),
        message: "Physical disk devices can only be opened by root. Rusty Backup will restart \
                  with root privileges via pkexec, and you will be prompted for your password. \
                  Disk image files do not need this. Any unsaved changes will be lost on restart."
            .to_string(),
        confirm_label: "Restart as Root".to_string(),
        destructive: false,
    }
}

pub fn unelevated_device_note() -> &'static str {
    if cfg!(windows) {
        "Note: physical devices need administrator privileges. Without it they may \
         not be listed, and Edit Mode will fail. Click \"Show Physical Devices\" in \
         the top bar. Disk image files are unaffected."
    } else if cfg!(target_os = "linux") {
        "Note: physical devices need root privileges to open, so Edit Mode and \
         device backups will fail. Click \"Unlock Physical Devices\" in the top bar \
         to restart as root. Disk image files are unaffected."
    } else {
        "Note: physical devices need root privileges (sudo). Without it they may \
         not be listed, and Edit Mode will fail. Disk image files are unaffected."
    }
}

/// Where file pickers start: the real user's home, even when running elevated through pkexec.
pub fn picker_start_folder() -> Option<PathBuf> {
    #[cfg(target_os = "linux")]
    {
        crate::os::linux::real_user_home()
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// The shell's part of [`AppController`]: the tab, the device list, the shared backup folder.
#[derive(Default)]
pub(super) struct ShellState {
    tab: Option<AppTab>,
    devices: Vec<DiskDevice>,
    devices_job: Option<Job<Vec<DiskDevice>>>,
    shared_backup: Option<PathBuf>,
    elevation_prompt: bool,
}

impl AppController {
    /// The tab on screen (Inspect at start).
    pub fn active_tab(&self) -> AppTab {
        self.shell.tab.unwrap_or(AppTab::Inspect)
    }

    pub fn select_tab(&mut self, tab: AppTab) {
        self.shell.tab = Some(tab);
    }

    pub fn devices(&self) -> &[DiskDevice] {
        self.shell.devices.as_slice()
    }

    /// The device list beside the log, for a shell that lends both to a tab at once.
    pub fn devices_and_log(&mut self) -> (&[DiskDevice], &mut crate::controller::log::LogStore) {
        (self.shell.devices.as_slice(), &mut self.log)
    }

    /// Tests: show `devices` as if they had been listed.
    #[cfg(test)]
    pub fn set_devices(&mut self, devices: Vec<DiskDevice>) {
        self.shell.devices = devices;
    }

    /// List the devices now, at start-up, and log what was found.
    pub fn load_devices(&mut self) {
        self.shell.devices = crate::device::enumerate_devices();
        if !crate::os::PHYSICAL_DEVICES_ENABLED {
            self.log.info(
                "This build works on disk image files only; physical device access is disabled.",
            );
        } else if self.shell.devices.is_empty() {
            self.log
                .warn("No disk devices detected. You can still open image files.");
        } else {
            self.log
                .info(format!("Found {} device(s)", self.shell.devices.len()));
        }
        #[cfg(target_os = "linux")]
        {
            // Devices are listed unprivileged from /sys/block; only opening them needs root.
            if crate::os::is_elevated() {
                self.log.info("Running with administrator privileges");
            } else if !self.shell.devices.is_empty() {
                self.log.warn(
                    "Running unprivileged: physical devices cannot be opened. Click \
                     'Unlock Physical Devices' in the top bar to restart as root. \
                     Disk image files are unaffected.",
                );
            }
        }
    }

    /// "Refresh Devices": list them again in a job (querying each disk takes a moment).
    pub(super) fn refresh_devices(&mut self) {
        if self.shell.devices_job.is_some() {
            return;
        }
        self.shell.devices_job = Some(Job::spawn("Looking for devices...", self.notify(), |_| {
            Ok(crate::device::enumerate_devices())
        }));
    }

    pub fn devices_refreshing(&self) -> Option<Snapshot> {
        self.shell.devices_job.as_ref().map(Job::snapshot)
    }

    pub(super) fn poll_shell(&mut self) {
        if let Some(done) = self.shell.devices_job.as_ref().and_then(job_outcome) {
            self.shell.devices_job = None;
            if let Ok(devices) = done {
                self.shell.devices = devices;
                self.log.info(format!(
                    "Refreshed: {} device(s) found",
                    self.shell.devices.len()
                ));
            }
        }
    }

    /// The top bar's elevation button: Windows asks UAC now, Linux asks the user first.
    pub(super) fn request_elevation(&mut self) {
        #[cfg(windows)]
        {
            self.log.info("Requesting administrator privileges...");
            if let Err(e) = crate::os::windows::request_elevation() {
                self.log
                    .error(format!("Elevation request failed or was cancelled: {e}"));
            }
        }
        // The relaunch discards unsaved edits, so the shell's dialog says so first.
        #[cfg(target_os = "linux")]
        {
            self.shell.elevation_prompt = true;
        }
    }

    /// True once after the elevation button, when the shell must ask before relaunching.
    pub fn take_elevation_prompt(&mut self) -> bool {
        std::mem::take(&mut self.shell.elevation_prompt)
    }

    // Only Linux relaunches here, so elsewhere the early return ends the body.
    #[cfg_attr(not(target_os = "linux"), allow(clippy::needless_return))]
    pub(super) fn confirm_elevation(&mut self, yes: bool) {
        // An if/else rather than an early return: off Linux the relaunch block
        // compiles away, which would leave the `return` as the last statement
        // and fail clippy's needless_return on Windows and macOS.
        if yes {
            #[cfg(target_os = "linux")]
            {
                self.log.info("Relaunching with elevated privileges...");
                if let Err(e) = crate::os::linux::relaunch_with_elevation() {
                    self.log.error(format!("Failed to elevate: {}", e));
                }
            }
        } else {
            self.log.warn(
                "Elevation cancelled. Physical devices stay unavailable; \
                 image files are unaffected.",
            );
        }
    }

    /// Where a file dropped on `active` goes.
    pub fn route_drop(&mut self, path: PathBuf, targets: DropTargets) -> DropRoute {
        match self.active_tab() {
            AppTab::Inspect if targets.inspect_remote => {
                self.log.warn(format!(
                    "Ignored dropped file {}: a remote session is open; close it first",
                    path.display()
                ));
                DropRoute::Ignore
            }
            // A loaded source keeps the drop: the open volume adds it in edit mode.
            AppTab::Inspect if targets.inspect_loaded => DropRoute::Ignore,
            AppTab::Inspect => DropRoute::Inspect(path),
            // An open disc browse has no drag-to-add of its own; don't clobber it.
            AppTab::Optical if targets.optical_browsing => DropRoute::Ignore,
            AppTab::Optical => DropRoute::Optical(path),
            AppTab::Archives => DropRoute::Archives(path),
            AppTab::Restore => DropRoute::Restore(path),
            // Build Disk starts from nothing; a drop has no meaning there.
            AppTab::Build => DropRoute::Ignore,
            AppTab::Backup => DropRoute::Backup(path),
            AppTab::Convert => DropRoute::Convert(path),
        }
    }

    /// The backup folder the Restore and Inspect tabs share.
    pub fn shared_backup(&self) -> Option<&Path> {
        self.shell.shared_backup.as_deref()
    }

    /// The top bar's Close Backup.
    pub fn close_shared_backup(&mut self) {
        self.shell.shared_backup = None;
    }

    /// Sync a tab with the shared folder: it opened a new backup, or (only when empty) adopts it.
    pub fn share_backup(&mut self, loaded: Option<PathBuf>, tab_empty: bool) -> ShareStep {
        if let Some(new) = loaded {
            if self.shell.shared_backup.as_ref() != Some(&new) {
                self.shell.shared_backup = Some(new.clone());
                return ShareStep::Publish(new);
            }
            return ShareStep::Nothing;
        }
        match (&self.shell.shared_backup, tab_empty) {
            (Some(folder), true) => ShareStep::Adopt(folder.clone()),
            _ => ShareStep::Nothing,
        }
    }
}

/// The one-line notice a tab shows while another tab's job locks its controls.
/// What an About window says.
pub mod about {
    pub const NAME: &str = "Rusty Backup";
    pub const SUMMARY: &str = "Back up, restore and inspect vintage computer disk images";
    pub const WEBSITE: &str = "https://github.com/danifunker/rusty-backup";
    pub const ISSUES: &str = "https://github.com/danifunker/rusty-backup/issues";
    pub const DEVELOPER: &str = "Dani Sarfati";
}

/// This build's version, as the shells show it.
pub fn app_version() -> &'static str {
    env!("APP_VERSION")
}

pub fn busy_elsewhere_notice(tab: &str) -> String {
    format!("A disk job is running ({tab}); start controls unlock when it finishes.")
}

/// The text on the shared progress bar; `full` is the untrimmed size when compaction saves space.
pub fn progress_text(current: u64, total: u64, full: u64, rate_eta: &str) -> String {
    use disk_core::display::format_size;
    let percent = current as f32 / total.max(1) as f32 * 100.0;
    if full > total {
        format!(
            "{} / {} to image of {} total ({percent:.0}%){rate_eta}",
            format_size(current),
            format_size(total),
            format_size(full),
        )
    } else {
        format!(
            "{} / {} ({percent:.0}%){rate_eta}",
            format_size(current),
            format_size(total),
        )
    }
}

/// Another tab (not `tab`) with a disk job in flight, so one job runs at a time.
pub fn busy_elsewhere(
    tab: AppTab,
    running: &[(AppTab, Option<&'static str>)],
) -> Option<&'static str> {
    running
        .iter()
        .find(|(owner, name)| name.is_some() && *owner != tab)
        .and_then(|(_, name)| *name)
}

#[cfg(test)]
mod tests {
    #[test]
    fn host_version_gate_needs_a_version_it_read() {
        assert!(!super::host_known_older_than(0, 0));
    }

    use super::*;

    #[test]
    fn drops_follow_the_active_tab() {
        let mut app = AppController::headless();
        let p = PathBuf::from("/x.img");
        assert_eq!(
            app.route_drop(p.clone(), DropTargets::default()),
            DropRoute::Inspect(p.clone())
        );
        let loaded = DropTargets {
            inspect_loaded: true,
            ..DropTargets::default()
        };
        assert_eq!(app.route_drop(p.clone(), loaded), DropRoute::Ignore);
        let remote = DropTargets {
            inspect_remote: true,
            ..DropTargets::default()
        };
        assert_eq!(app.route_drop(p.clone(), remote), DropRoute::Ignore);
        assert!(app.log.to_text().contains("a remote session is open"));
        app.select_tab(AppTab::Archives);
        assert_eq!(
            app.route_drop(p.clone(), loaded),
            DropRoute::Archives(p.clone())
        );
    }

    #[test]
    fn the_shared_backup_is_published_then_adopted_only_by_an_empty_tab() {
        let mut app = AppController::headless();
        let f = PathBuf::from("/bk");
        assert_eq!(
            app.share_backup(Some(f.clone()), false),
            ShareStep::Publish(f.clone())
        );
        assert_eq!(app.share_backup(Some(f.clone()), false), ShareStep::Nothing);
        assert_eq!(app.share_backup(None, false), ShareStep::Nothing);
        assert_eq!(app.share_backup(None, true), ShareStep::Adopt(f));
        app.close_shared_backup();
        assert_eq!(app.share_backup(None, true), ShareStep::Nothing);
    }

    #[test]
    fn one_disk_job_at_a_time() {
        let running = [
            (AppTab::Backup, Some("Backup")),
            (AppTab::Restore, None),
            (AppTab::Convert, None),
        ];
        assert_eq!(busy_elsewhere(AppTab::Restore, &running), Some("Backup"));
        assert_eq!(busy_elsewhere(AppTab::Backup, &running), None);
    }
}
