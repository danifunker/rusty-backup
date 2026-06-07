mod archives_tab;
mod backup_tab;
mod browse_view;
mod bulk_convert_dialog;
mod chd_options_ui;
mod context;
mod elevation_dialog;
mod expand_hfs_dialog;
mod floppy_convert_dialog;
pub mod fonts;
mod inspect_tab;
mod journal_view;
mod optical_tab;
mod partition_bar;
mod physical_disk_export;
mod progress;
mod resize_popup;
mod restore_tab;
mod settings_dialog;
mod size_mode_row;
pub mod ui_logger;

use archives_tab::ArchivesTab;
use backup_tab::BackupTab;
use bulk_convert_dialog::{BulkConvertDialog, DialogAction as BulkConvertAction};
use inspect_tab::InspectTab;
use optical_tab::OpticalTab;
use progress::{LogPanel, ProgressState};
use restore_tab::RestoreTab;
use rusty_backup::model::status::{BulkConvertLogLevel, BulkConvertStatus};
use settings_dialog::SettingsDialog;
use std::path::PathBuf;

use rusty_backup::device::{self, DiskDevice};
use rusty_backup::update::{check_for_updates, UpdateConfig, UpdateInfo};

#[cfg(target_os = "linux")]
use elevation_dialog::{ElevationAction, ElevationDialog};

use std::sync::{Arc, Mutex};
use std::thread;

/// Create an `rfd::FileDialog` pre-configured to start in the real user's home
/// directory. This ensures file dialogs open in the right place even when the
/// app is running elevated via pkexec.
fn bytes_human(n: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    if n >= GB {
        format!("{:.2} GB", n as f64 / GB as f64)
    } else if n >= MB {
        format!("{:.1} MB", n as f64 / MB as f64)
    } else if n >= KB {
        format!("{:.1} KB", n as f64 / KB as f64)
    } else {
        format!("{} B", n)
    }
}

/// Hint shown in a device dropdown when no physical devices are listed. On
/// Windows the empty list usually means the process isn't elevated yet, so we
/// point the user at the top-bar gate; when already elevated (or on other
/// platforms) it just reports that none were detected.
fn no_devices_hint() -> &'static str {
    #[cfg(windows)]
    {
        if rusty_backup::os::windows::is_elevated() {
            "No physical devices detected."
        } else {
            "Click \"Show Physical Devices\" (top bar) to access disks."
        }
    }
    #[cfg(not(windows))]
    {
        "No physical devices detected."
    }
}

/// Whether physical-device access is available right now. On Windows this means
/// the process is elevated (raw disk + SCSI optical access need admin); on other
/// platforms it is always true. Used to gray out physical-source controls until
/// the user elevates via the top-bar "Show Physical Devices" button.
fn physical_devices_available() -> bool {
    #[cfg(windows)]
    {
        rusty_backup::os::windows::is_elevated()
    }
    #[cfg(not(windows))]
    {
        true
    }
}

fn file_dialog() -> rfd::FileDialog {
    let dialog = rfd::FileDialog::new();
    #[cfg(target_os = "linux")]
    let dialog = if let Some(home) = rusty_backup::os::linux::real_user_home() {
        dialog.set_directory(home)
    } else {
        dialog
    };
    dialog
}

/// Prepare a disk image path for file-based access by downstream GUI
/// worker code. Returns a path to a raw, seekable image — possibly the
/// original (no work needed) or a tempfile.
///
/// Handles three wrapper formats whose raw stream isn't seekable / flat:
/// - `.adz` / `.hdz` — gzip-decompressed to a tempfile (gzip isn't
///   seekable).
/// - `.msa` — Atari ST Magic Shadow Archiver, decoded to a flat `.st`
///   tempfile.
/// - `.d88` — Sharp Japanese-emulator floppy container (X68000 / PC-88 /
///   PC-98 / MSX / FM-7), decoded to a flat raw-sector tempfile.
///
/// Everything else — including `.gho`/`.ghs`, `.imz`, `.chd` — passes
/// through unchanged, since the inspect worker opens those via
/// [`open_disk_image_for_reading`] (streaming `Read + Seek` readers,
/// no tempfile).
///
/// The output file lives inside a fresh `tempfile::tempdir`; callers
/// must hold the returned `TempDir` (second tuple element) for the
/// duration they need the materialized file.
pub fn prepare_disk_image_path(
    path: &std::path::Path,
) -> std::io::Result<(std::path::PathBuf, Option<tempfile::TempDir>)> {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|s| s.to_ascii_lowercase());

    // GHO/GHS and IMZ have streaming Read+Seek readers (GhoReader,
    // ImzReader) — the inspect worker opens them via open_read, so
    // the path passes through unchanged. No tempfile needed.

    let target_ext = match ext.as_deref() {
        Some("adz") => "adf",
        Some("hdz") => "hdf",
        Some("msa") => "st",
        Some("d88") => "img",
        Some("xdf") | Some("hdm") | Some("dim") => "img",
        _ => return Ok((path.to_path_buf(), None)),
    };
    let tmp = tempfile::tempdir()?;
    let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("image");
    let out_path = tmp.path().join(format!("{}.{}", stem, target_ext));

    if target_ext == "st" {
        // MSA → flat raw sectors, written to a tempfile so the rest of the
        // GUI pipeline (which addresses partitions by byte offset on a
        // path) sees a plain `.st` image.
        let bytes = std::fs::read(path)?;
        let flat = rusty_backup::rbformats::containers::msa::decode_msa_bytes(&bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{e:#}")))?;
        std::fs::write(&out_path, &flat)?;
        log::info!(
            "Materialized {} -> {} ({} bytes, MSA decoded)",
            path.display(),
            out_path.display(),
            flat.len()
        );
        return Ok((out_path, Some(tmp)));
    }
    if target_ext == "img" && ext.as_deref() == Some("d88") {
        // D88 → flat raw sectors. Same shape as MSA: small floppy
        // (~1.5 MB max for 2HD), in-memory decode, tempfile drop-in.
        let bytes = std::fs::read(path)?;
        let flat = rusty_backup::rbformats::containers::d88::decode_d88_bytes(&bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{e:#}")))?;
        std::fs::write(&out_path, &flat)?;
        log::info!(
            "Materialized {} -> {} ({} bytes, D88 decoded)",
            path.display(),
            out_path.display(),
            flat.len()
        );
        return Ok((out_path, Some(tmp)));
    }
    if target_ext == "img" && matches!(ext.as_deref(), Some("xdf") | Some("hdm") | Some("dim")) {
        let bytes = std::fs::read(path)?;
        let (flat, label) = match ext.as_deref() {
            Some("xdf") => (
                rusty_backup::rbformats::containers::xdf::decode_xdf_bytes(&bytes)
                    .map_err(|e| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{e:#}"))
                    })?
                    .0,
                "XDF",
            ),
            Some("hdm") => (
                rusty_backup::rbformats::containers::hdm::decode_hdm_bytes(&bytes)
                    .map_err(|e| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{e:#}"))
                    })?
                    .0,
                "HDM",
            ),
            Some("dim") => (
                rusty_backup::rbformats::containers::dim::decode_dim_bytes(&bytes)
                    .map_err(|e| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{e:#}"))
                    })?
                    .0,
                "DIM",
            ),
            _ => unreachable!("guarded by outer match arm"),
        };
        std::fs::write(&out_path, &flat)?;
        log::info!(
            "Materialized {} -> {} ({} bytes, {} decoded)",
            path.display(),
            out_path.display(),
            flat.len(),
            label
        );
        return Ok((out_path, Some(tmp)));
    }

    // Gzip-wrapped Amiga images (.adz / .hdz).
    let mut input = std::fs::File::open(path)?;
    // Spot-check the gzip magic so we surface a clear error if the
    // user renamed something to .adz that isn't actually gzipped.
    let mut magic = [0u8; 2];
    use std::io::Read;
    input.read_exact(&mut magic)?;
    if magic != [0x1F, 0x8B] {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("{} does not start with gzip magic 1f 8b", path.display()),
        ));
    }
    use std::io::Seek;
    input.seek(std::io::SeekFrom::Start(0))?;
    let mut decoder = flate2::read::GzDecoder::new(input);
    let mut out = std::fs::File::create(&out_path)?;
    std::io::copy(&mut decoder, &mut out)?;
    out.sync_all()?;
    log::info!(
        "Materialized {} -> {} ({} bytes)",
        path.display(),
        out_path.display(),
        out.metadata().map(|m| m.len()).unwrap_or(0)
    );
    Ok((out_path, Some(tmp)))
}

/// Open a disk image file for streaming reads, dispatching on container
/// type:
///
/// - `.chd` → `ChdReader` (decompresses hunks on demand)
/// - `.gho` / `.ghs` → `GhoReader` (streams SECTOR mode, builds an
///   in-RAM virtual FAT image for file-aware mode)
/// - `.imz` → `ImzReader` (streams ZIP-wrapped floppy image)
/// - anything else → buffered `File`
///
/// No tempfile is created for any of the streaming containers; only
/// gzip-wrapped Amiga images need [`prepare_disk_image_path`]
/// first (since gzip isn't seekable).
///
/// This is the GUI's entry point for opening image FILES (not raw
/// devices — those still need [`rusty_backup::os::open_source_for_reading`]
/// for elevation / volume locking).
#[allow(dead_code)] // First caller lands in the GUI worker refactor.
pub fn open_disk_image_for_reading(
    path: &std::path::Path,
) -> std::io::Result<Box<dyn rusty_backup::rbformats::ReadSeek>> {
    rusty_backup::model::source_reader::open_read(path)
        .map_err(|e| std::io::Error::other(format!("{e:#}")))
}

#[cfg(test)]
mod materialize_tests {
    use super::*;
    use std::io::Write as _;

    #[test]
    fn materialize_passes_raw_adf_through() {
        let tmp = tempfile::tempdir().unwrap();
        let raw = tmp.path().join("disk.adf");
        std::fs::write(&raw, b"raw bytes").unwrap();
        let (out, guard) = prepare_disk_image_path(&raw).unwrap();
        assert_eq!(out, raw);
        assert!(guard.is_none());
    }

    #[test]
    fn materialize_decompresses_adz() {
        let payload: &[u8] = b"PAYLOAD bytes for an adz round-trip";
        let tmp = tempfile::tempdir().unwrap();
        let adz_path = tmp.path().join("disk.adz");
        // Write a tiny gzipped file.
        let f = std::fs::File::create(&adz_path).unwrap();
        let mut enc = flate2::write::GzEncoder::new(f, flate2::Compression::default());
        enc.write_all(payload).unwrap();
        enc.finish().unwrap();
        let (out, guard) = prepare_disk_image_path(&adz_path).unwrap();
        assert!(guard.is_some(), "decompressed path needs a tempdir guard");
        assert_eq!(out.extension().and_then(|s| s.to_str()), Some("adf"));
        let actual = std::fs::read(&out).unwrap();
        assert_eq!(actual, payload);
    }

    #[test]
    fn materialize_rejects_non_gzip_adz() {
        let tmp = tempfile::tempdir().unwrap();
        let adz = tmp.path().join("bogus.adz");
        std::fs::write(&adz, b"not gzip data").unwrap();
        let err = prepare_disk_image_path(&adz).unwrap_err();
        assert!(err.to_string().contains("gzip magic"));
    }

    #[test]
    fn imz_passes_through_unchanged() {
        // IMZ has a streaming reader (ImzReader); prepare_disk_image_path
        // passes it through unchanged — the worker opens it via
        // open_disk_image_for_reading.
        let tmp = tempfile::tempdir().unwrap();
        let imz_path = tmp.path().join("floppy.imz");
        std::fs::write(&imz_path, b"PK\x03\x04anything").unwrap();
        let (out, guard) = prepare_disk_image_path(&imz_path).unwrap();
        assert_eq!(out, imz_path);
        assert!(guard.is_none());
    }

    #[test]
    fn gho_passes_through_unchanged() {
        let tmp = tempfile::tempdir().unwrap();
        let gho_path = tmp.path().join("test.gho");
        std::fs::write(&gho_path, [0xFE, 0xEF, 0x01, 0x00]).unwrap();
        let (out, guard) = prepare_disk_image_path(&gho_path).unwrap();
        assert_eq!(out, gho_path);
        assert!(guard.is_none());
    }

    #[test]
    fn hfv_passes_through_unchanged() {
        // HFV is a flat raw HFS volume opened directly at offset 0 by the
        // superfloppy path; prepare_disk_image_path must not materialize or
        // rewrite it. Covers both .hfv and .HFV.
        let tmp = tempfile::tempdir().unwrap();
        for name in ["disk.hfv", "DISK.HFV"] {
            let hfv_path = tmp.path().join(name);
            std::fs::write(&hfv_path, b"\x00\x00\x00\x00").unwrap();
            let (out, guard) = prepare_disk_image_path(&hfv_path).unwrap();
            assert_eq!(out, hfv_path);
            assert!(guard.is_none());
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Tab {
    Backup,
    Restore,
    Inspect,
    Optical,
    Archives,
}

/// Main application state.
pub struct RustyBackupApp {
    active_tab: Tab,
    backup_tab: BackupTab,
    restore_tab: RestoreTab,
    inspect_tab: InspectTab,
    optical_tab: OpticalTab,
    archives_tab: ArchivesTab,
    log_panel: LogPanel,
    progress: ProgressState,
    devices: Vec<DiskDevice>,
    update_info: Arc<Mutex<Option<UpdateInfo>>>,
    update_dismissed: bool,
    /// In-app self-update worker status (Windows only). `None` until the user
    /// clicks "Download & Install Update".
    #[cfg(windows)]
    update_run: Option<Arc<Mutex<rusty_backup::model::update_runner::UpdateRunStatus>>>,
    settings_dialog: SettingsDialog,
    #[cfg(target_os = "linux")]
    elevation_dialog: ElevationDialog,
    /// Stylized shield texture for the "Show Physical Devices" elevation button.
    /// Loaded lazily on first use (the egui context isn't available at
    /// construction time).
    shield_texture: Option<egui::TextureHandle>,
    /// Shared backup folder path between restore and inspect tabs
    loaded_backup_folder: Option<PathBuf>,
    /// Bulk Convert dialog state (None = closed).
    bulk_convert_dialog: Option<BulkConvertDialog>,
    /// Background bulk-convert worker progress (None = idle).
    bulk_convert_status: Option<Arc<Mutex<BulkConvertStatus>>>,
    /// File path passed on the command line (Windows file-association
    /// double-click, macOS `open file.d88`, Linux `.desktop` MimeType
    /// launch). Taken on the first `update()` tick, routed into the
    /// Inspect tab, and the tab is switched to it. `None` for normal
    /// launches without a file argv.
    pending_initial_image: Option<PathBuf>,
}

impl Default for RustyBackupApp {
    fn default() -> Self {
        let mut log = LogPanel::default();
        log.info("Rusty Backup started");

        let devices = device::enumerate_devices();
        if devices.is_empty() {
            log.warn("No disk devices detected. You can still open image files.");
        } else {
            log.info(format!("Found {} device(s)", devices.len()));
        }

        // Check privilege status
        #[cfg(target_os = "linux")]
        let elevation_dialog = {
            let dialog = ElevationDialog::default();
            if !devices.is_empty() {
                // Check if we need elevation
                if let Ok(access) = rusty_backup::privileged::create_disk_access() {
                    if let Ok(status) = access.check_status() {
                        match status {
                            rusty_backup::privileged::AccessStatus::NeedsElevation => {
                                log.warn(
                                    "Running without elevated privileges. Click 'Request Elevation' to access devices.",
                                );
                            }
                            rusty_backup::privileged::AccessStatus::Ready => {
                                log.info("Running with elevated privileges");
                            }
                            _ => {}
                        }
                    }
                }
            }
            dialog
        };

        let backup_tab = BackupTab::default();

        let mut optical_tab = OpticalTab::default();
        optical_tab.refresh_drives();
        if optical_tab.drive_count() > 0 {
            log.info(format!(
                "Found {} optical drive(s)",
                optical_tab.drive_count()
            ));
        }

        // Start update check in background
        let update_info = Arc::new(Mutex::new(None));
        let update_info_clone = Arc::clone(&update_info);
        thread::spawn(move || {
            let config = UpdateConfig::load();
            if config.update_check.enabled {
                let current_version = env!("APP_VERSION");
                if let Ok(info) = check_for_updates(&config.update_check, current_version) {
                    *update_info_clone.lock().unwrap() = Some(info);
                }
            }
        });

        Self {
            active_tab: Tab::Inspect,
            backup_tab,
            restore_tab: RestoreTab::default(),
            inspect_tab: InspectTab::default(),
            optical_tab,
            archives_tab: ArchivesTab::default(),
            log_panel: log,
            progress: ProgressState::default(),
            devices,
            update_info,
            update_dismissed: false,
            #[cfg(windows)]
            update_run: None,
            settings_dialog: SettingsDialog::default(),
            #[cfg(target_os = "linux")]
            elevation_dialog,
            shield_texture: None,
            loaded_backup_folder: None,
            bulk_convert_dialog: None,
            bulk_convert_status: None,
            pending_initial_image: None,
        }
    }
}

impl RustyBackupApp {
    /// Construct the app with an optional file path to auto-open on first
    /// frame. Used by `main.rs` to wire up Windows file-association
    /// double-clicks, macOS `open file.ext` launches, and Linux .desktop
    /// MimeType handlers — all of them launch the binary with the file
    /// path in argv.
    pub fn with_initial_image(image: Option<PathBuf>) -> Self {
        // Struct-update syntax keeps `field_reassign_with_default` happy:
        // build everything through Default, then override the one field
        // we care about. Default does the heavy device-enumeration + log
        // priming so we don't want to duplicate it.
        Self {
            pending_initial_image: image,
            ..Self::default()
        }
    }

    /// Open a path in the Inspect tab and switch to it. Used by both the
    /// command-line initial-image hand-off (one-shot at app start) and
    /// the drag-and-drop handler (any time during the session).
    ///
    /// Routes through [`prepare_disk_image_path`] so wrapper formats with
    /// no native seekable view (`.adz` / `.hdz` / `.msa` / `.d88` / `.xdf`
    /// / `.hdm` / `.dim`) are decompressed to a flat tempfile first. Same
    /// path the "Open File..." button uses — keeps DnD / double-click
    /// behavior identical to the picker.
    fn open_in_inspect(&mut self, path: PathBuf) {
        self.log_panel.info(format!("Opening {}", path.display()));
        match prepare_disk_image_path(&path) {
            Ok((materialized, guard)) => {
                self.inspect_tab
                    .load_image_with_tempdir(materialized, guard);
            }
            Err(e) => {
                self.log_panel.warn(format!(
                    "Failed to materialize {}: {} — opening raw bytes",
                    path.display(),
                    e
                ));
                self.inspect_tab.load_image_with_tempdir(path, None);
            }
        }
        self.active_tab = Tab::Inspect;
    }
}

/// Whether the physical-device elevation gate applies, and its current state.
/// `NeedsElevation`/`Elevated` are only constructed on Windows; on other
/// platforms `elevation_gate()` always returns `NotApplicable`.
#[derive(Clone, Copy, PartialEq)]
enum ElevationGate {
    /// Not a gated platform (Linux/macOS handle elevation their own way) — the
    /// normal "Refresh Devices" control applies. Only constructed on
    /// non-Windows builds; the cfg_attr below silences the resulting
    /// "variant never constructed" warning on Windows.
    #[cfg_attr(windows, allow(dead_code))]
    NotApplicable,
    /// Windows, not elevated — show the shielded "Show Physical Devices" button.
    /// Constructed only inside `#[cfg(windows)]` paths.
    #[cfg_attr(not(windows), allow(dead_code))]
    NeedsElevation,
    /// Windows, already elevated — devices are available, show "Refresh Devices".
    /// Constructed only inside `#[cfg(windows)]` paths.
    #[cfg_attr(not(windows), allow(dead_code))]
    Elevated,
}

impl RustyBackupApp {
    /// Current Windows elevation-gate state. The button + device-list behavior
    /// key off this; non-Windows is always `NotApplicable`.
    fn elevation_gate(&self) -> ElevationGate {
        #[cfg(windows)]
        {
            if rusty_backup::os::windows::is_elevated() {
                ElevationGate::Elevated
            } else {
                ElevationGate::NeedsElevation
            }
        }
        #[cfg(not(windows))]
        {
            ElevationGate::NotApplicable
        }
    }

    /// Lazily load (once) and return the stylized shield texture for the
    /// elevation button.
    fn shield_texture(&mut self, ctx: &egui::Context) -> egui::TextureHandle {
        if let Some(tex) = &self.shield_texture {
            return tex.clone();
        }
        let rgba = image::load_from_memory(include_bytes!("../../assets/icons/shield.png"))
            .expect("embedded shield.png is valid")
            .to_rgba8();
        let size = [rgba.width() as usize, rgba.height() as usize];
        let color_image = egui::ColorImage::from_rgba_unmultiplied(size, rgba.as_raw());
        let tex = ctx.load_texture("shield", color_image, egui::TextureOptions::LINEAR);
        self.shield_texture = Some(tex.clone());
        tex
    }

    /// Drain bulk-convert worker log messages into the GUI log panel and
    /// clear the status handle when the worker finishes.
    fn poll_bulk_convert(&mut self) {
        let status_arc = match &self.bulk_convert_status {
            Some(s) => Arc::clone(s),
            None => return,
        };
        let Ok(mut status) = status_arc.lock() else {
            return;
        };
        for (level, msg) in status.log_messages.drain(..) {
            match level {
                BulkConvertLogLevel::Info => self.log_panel.info(msg),
                BulkConvertLogLevel::Warn => self.log_panel.warn(msg),
                BulkConvertLogLevel::Error => self.log_panel.error(msg),
            }
        }
        if status.finished {
            drop(status);
            self.bulk_convert_status = None;
        }
    }

    /// Render the update-banner action buttons. On Windows with a matched
    /// release asset this offers in-app "Download & Install Update" (download
    /// progress -> "Restart now"); otherwise (other platforms, unmatched arch,
    /// or a missing asset) it opens the releases page in a browser.
    fn show_update_actions(&mut self, ui: &mut egui::Ui, info: &UpdateInfo) {
        #[cfg(windows)]
        {
            use rusty_backup::model::update_runner::{self, UpdateRunState};

            if info.asset_url.is_some() {
                let run_state = self.update_run.as_ref().map(|s| {
                    s.lock()
                        .map(|g| g.state.clone())
                        .unwrap_or(UpdateRunState::Idle)
                });

                match run_state {
                    None => {
                        if ui.button("Download & Install Update").clicked() {
                            let status =
                                Arc::new(Mutex::new(update_runner::UpdateRunStatus::default()));
                            update_runner::spawn(info.clone(), Arc::clone(&status));
                            self.update_run = Some(status);
                            self.log_panel.info("Downloading update...");
                        }
                        if ui.button("View Release").clicked() {
                            let _ = webbrowser::open(&info.releases_url);
                        }
                    }
                    Some(UpdateRunState::Idle) | Some(UpdateRunState::Downloading { .. }) => {
                        let frac = self
                            .update_run
                            .as_ref()
                            .and_then(|s| s.lock().ok().and_then(|g| g.progress_fraction()));
                        ui.add(egui::Spinner::new());
                        if let Some(f) = frac {
                            ui.add(
                                egui::ProgressBar::new(f)
                                    .desired_width(140.0)
                                    .show_percentage(),
                            );
                        } else {
                            ui.label("Downloading update...");
                        }
                        ui.ctx().request_repaint();
                    }
                    Some(UpdateRunState::Ready) => {
                        ui.label(
                            egui::RichText::new("Update installed.").color(egui::Color32::GREEN),
                        );
                        if ui.button("Restart now").clicked() {
                            rusty_backup::update::restart_app();
                        }
                    }
                    Some(UpdateRunState::Failed(e)) => {
                        ui.label(
                            egui::RichText::new(format!("Update failed: {e}"))
                                .color(egui::Color32::LIGHT_RED),
                        );
                        if ui.button("Retry").clicked() {
                            self.update_run = None;
                        }
                        if ui.button("View Release").clicked() {
                            let _ = webbrowser::open(&info.releases_url);
                        }
                    }
                }
                return;
            }
        }

        if ui.button("View Release").clicked() {
            let _ = webbrowser::open(&info.releases_url);
        }
    }
}

impl eframe::App for RustyBackupApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // Request repaint while backup/restore is running so progress updates are shown
        if self.progress.active
            || self.restore_tab.is_running()
            || self.optical_tab.is_running()
            || self.bulk_convert_status.is_some()
        {
            ctx.request_repaint();
        }

        // One-shot: drain the file path handed in on the command line
        // (Windows file-association double-click, macOS `open ...`, Linux
        // .desktop MimeType launch). Done in update() rather than the
        // constructor because the inspect tab's lazy-load path needs the
        // egui context to be alive.
        if let Some(path) = self.pending_initial_image.take() {
            self.open_in_inspect(path);
        }

        // Drag-and-drop: if the user dragged one or more files onto the
        // app window, take the first one with a real filesystem path and
        // open it in the Inspect tab. `with_drag_and_drop(true)` is set
        // on the viewport in main.rs so eframe surfaces these events.
        let dropped: Option<PathBuf> =
            ctx.input(|i| i.raw.dropped_files.iter().find_map(|f| f.path.clone()));
        if let Some(path) = dropped {
            self.open_in_inspect(path);
        }

        // Drain `log` crate records (incl. worker-thread log::info! from the
        // GHO reader) into the panel so they're visible in the UI.
        for (level, msg) in ui_logger::drain() {
            match level {
                log::Level::Error => self.log_panel.error(msg),
                log::Level::Warn => self.log_panel.warn(msg),
                _ => self.log_panel.info(msg),
            }
        }

        // Drain bulk-convert worker logs into the panel and clear when done.
        self.poll_bulk_convert();
    }

    fn ui(&mut self, ctx: &mut egui::Ui, _frame: &mut eframe::Frame) {
        // Top panel: tab bar
        egui::Panel::top("tab_bar").show_inside(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.selectable_value(&mut self.active_tab, Tab::Backup, "Backup");
                ui.selectable_value(&mut self.active_tab, Tab::Restore, "Restore");
                ui.selectable_value(&mut self.active_tab, Tab::Inspect, "Inspect");
                ui.selectable_value(&mut self.active_tab, Tab::Optical, "Optical");
                ui.selectable_value(&mut self.active_tab, Tab::Archives, "Archives");

                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    // Version display
                    ui.label(format!("v{}", env!("APP_VERSION")));
                    ui.separator();

                    // Close Backup button (if backup loaded)
                    if self.loaded_backup_folder.is_some() {
                        if ui.button("Close Backup").clicked() {
                            self.loaded_backup_folder = None;
                            self.restore_tab.clear_backup();
                            self.inspect_tab.clear_backup();
                            self.log_panel.info("Backup closed");
                        }
                        ui.separator();
                    }

                    if ui.button("Settings").clicked() {
                        self.settings_dialog.open_dialog();
                    }
                    ui.separator();

                    // Elevation button (Linux only, when not root)
                    #[cfg(target_os = "linux")]
                    {
                        if let Ok(access) = rusty_backup::privileged::create_disk_access() {
                            if let Ok(status) = access.check_status() {
                                if status == rusty_backup::privileged::AccessStatus::NeedsElevation {
                                    if ui
                                        .button(egui::RichText::new("Request Elevation").color(egui::Color32::YELLOW))
                                        .on_hover_text("Restart with administrator privileges to access disk devices")
                                        .clicked()
                                    {
                                        self.elevation_dialog.open();
                                    }
                                    ui.separator();
                                }
                            }
                        }
                    }

                    // Windows: physical-device access needs admin, and the GUI
                    // launches un-elevated (asInvoker). When not elevated, the
                    // single global gate is this shielded "Show Physical
                    // Devices" button, which relaunches the whole process via
                    // UAC; the elevated instance auto-enumerates on startup.
                    // Once elevated, it becomes the normal "Refresh Devices".
                    match self.elevation_gate() {
                        ElevationGate::NeedsElevation => {
                            let tint = ui.visuals().widgets.active.fg_stroke.color;
                            let shield = self.shield_texture(ui.ctx());
                            let icon = egui::Image::from_texture(
                                egui::load::SizedTexture::from_handle(&shield),
                            )
                            .tint(tint)
                            .max_height(14.0);
                            let resp = ui
                                .add(egui::Button::image_and_text(
                                    icon,
                                    egui::RichText::new("Show Physical Devices")
                                        .color(egui::Color32::YELLOW),
                                ))
                                .on_hover_text(
                                    "Restart with administrator privileges to access \
                                     physical disk devices (will prompt for elevation).",
                                );
                            if resp.clicked() {
                                #[cfg(windows)]
                                {
                                    self.log_panel
                                        .info("Requesting administrator privileges...");
                                    if let Err(e) = rusty_backup::os::windows::request_elevation() {
                                        self.log_panel.error(format!(
                                            "Elevation request failed or was cancelled: {e}"
                                        ));
                                    }
                                }
                            }
                            ui.separator();
                        }
                        ElevationGate::NotApplicable | ElevationGate::Elevated => {
                            if ui.button("Refresh Devices").clicked() {
                                self.devices = device::enumerate_devices();
                                self.log_panel.info(format!(
                                    "Refreshed: {} device(s) found",
                                    self.devices.len()
                                ));
                                ui.ctx().request_repaint();
                            }
                        }
                    }
                    ui.separator();

                    // Bulk Convert — convert every disk image in a folder.
                    let bulk_running = self.bulk_convert_status.is_some();
                    if ui
                        .add_enabled(!bulk_running, egui::Button::new("Bulk Convert…"))
                        .on_hover_text(
                            "Convert every disk image in a folder to one chosen format, \
                             using the same parameters for every file.",
                        )
                        .clicked()
                    {
                        self.bulk_convert_dialog = Some(BulkConvertDialog::default());
                    }
                    if bulk_running
                        && ui.button("Cancel Bulk").clicked() {
                            if let Some(ref s) = self.bulk_convert_status {
                                if let Ok(mut g) = s.lock() {
                                    g.cancel_requested = true;
                                }
                            }
                            self.log_panel.warn("Bulk convert cancellation requested...");
                        }
                });
            });
        });

        // Bulk Convert dialog (modal-ish window, always reachable).
        if let Some(ref mut dlg) = self.bulk_convert_dialog {
            match dlg.show(ctx) {
                BulkConvertAction::Start {
                    files,
                    extension,
                    chd_options,
                    bincue_multi_bin,
                } => {
                    if let Some(out) = dlg.output_folder.clone() {
                        let format = dlg.format;
                        let count = files.len();
                        self.bulk_convert_dialog = None;
                        self.log_panel.info(format!(
                            "Starting bulk convert: {count} file(s) -> {} ({}, .{extension})",
                            out.display(),
                            format.description(),
                        ));
                        self.bulk_convert_status = Some(bulk_convert_dialog::start_bulk_convert(
                            files,
                            out,
                            format,
                            extension,
                            chd_options,
                            bincue_multi_bin,
                        ));
                    }
                }
                BulkConvertAction::Cancel => {
                    self.bulk_convert_dialog = None;
                }
                BulkConvertAction::None => {}
            }
        }

        // Bulk Convert progress strip — visible from any tab while running.
        if let Some(ref status_arc) = self.bulk_convert_status {
            if let Ok(s) = status_arc.lock() {
                if !s.finished {
                    let frac = if s.current_total_bytes > 0 {
                        (s.current_bytes as f32 / s.current_total_bytes as f32).clamp(0.0, 1.0)
                    } else {
                        0.0
                    };
                    let text = if s.current_total_bytes > 0 {
                        format!(
                            "Bulk Convert: [{}/{}] {} — {} / {} ({:.0}%)",
                            s.current_index,
                            s.total_files,
                            s.current_file,
                            bytes_human(s.current_bytes),
                            bytes_human(s.current_total_bytes),
                            frac * 100.0,
                        )
                    } else {
                        format!(
                            "Bulk Convert: [{}/{}] {} — preparing…",
                            s.current_index, s.total_files, s.current_file,
                        )
                    };
                    egui::Panel::top("bulk_convert_progress").show_inside(ctx, |ui| {
                        ui.add(egui::ProgressBar::new(frac).text(text));
                    });
                }
            }
        }

        // Update notification banner (if update available and not dismissed).
        // Clone the info into an owned local first so the mutex guard is dropped
        // before the closure (which needs unique access to `self`).
        let banner_info: Option<UpdateInfo> = if self.update_dismissed {
            None
        } else {
            self.update_info.lock().ok().and_then(|g| g.clone())
        };
        if let Some(info) = banner_info {
            if info.is_outdated {
                egui::Panel::top("update_banner").show_inside(ctx, |ui| {
                    ui.horizontal(|ui| {
                        ui.label(
                            egui::RichText::new("Update")
                                .color(egui::Color32::YELLOW)
                                .strong(),
                        );
                        ui.label(format!(
                            "available: v{} -> v{}",
                            info.current_version, info.latest_version
                        ));
                        self.show_update_actions(ui, &info);
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            if ui.button("Dismiss update notification").clicked() {
                                self.update_dismissed = true;
                            }
                        });
                    });
                });
            }
        }
        // (end update banner)

        // Elevation dialog (Linux)
        #[cfg(target_os = "linux")]
        {
            let action = self.elevation_dialog.show(ctx);
            match action {
                ElevationAction::Elevate => {
                    self.log_panel
                        .info("Relaunching with elevated privileges...");
                    if let Err(e) = rusty_backup::os::linux::relaunch_with_elevation() {
                        self.log_panel.error(format!("Failed to elevate: {}", e));
                    }
                }
                ElevationAction::Cancel => {
                    self.log_panel
                        .warn("Elevation cancelled. Device operations will fail.");
                }
                ElevationAction::None => {}
            }
        }

        // Bottom panel: progress + log
        egui::Panel::bottom("log_panel")
            .resizable(true)
            .min_size(100.0)
            .default_size(180.0)
            .show_inside(ctx, |ui| {
                self.progress.show(ui);
                if self.progress.active {
                    ui.separator();
                }
                self.log_panel.show(ui);
            });

        // Central panel: active tab content
        egui::CentralPanel::default().show_inside(ctx, |ui| match self.active_tab {
            Tab::Backup => {
                let mut ctx = context::TabContext::new(&self.devices, &mut self.log_panel);
                self.backup_tab.show(ui, &mut ctx, &mut self.progress);
            }
            Tab::Restore => {
                // Share backup folder between tabs
                if let Some(new_backup) = self.restore_tab.get_loaded_backup() {
                    if self.loaded_backup_folder.as_ref() != Some(&new_backup) {
                        self.loaded_backup_folder = Some(new_backup.clone());
                        self.inspect_tab.load_backup(&new_backup);
                    }
                } else if !self.restore_tab.has_backup() {
                    if let Some(folder) = self.loaded_backup_folder.as_ref() {
                        self.restore_tab.load_backup(folder);
                    }
                }

                let mut ctx = context::TabContext::new(&self.devices, &mut self.log_panel);
                self.restore_tab.show(ui, &mut ctx, &mut self.progress);
            }
            Tab::Inspect => {
                // The inspect tab's own "Close Backup" button signals the
                // intent to fully close. Honor it before the share-state
                // logic below — otherwise the auto-reopen branch would
                // immediately re-load the backup from `loaded_backup_folder`
                // on the very next frame.
                if self.inspect_tab.take_close_backup_request() {
                    self.loaded_backup_folder = None;
                    self.restore_tab.clear_backup();
                }

                // Share backup folder between tabs
                if let Some(new_backup) = self.inspect_tab.get_loaded_backup() {
                    if self.loaded_backup_folder.as_ref() != Some(&new_backup) {
                        self.loaded_backup_folder = Some(new_backup.clone());
                        self.restore_tab.load_backup(&new_backup);
                    }
                } else if !self.inspect_tab.has_backup() {
                    if let Some(folder) = self.loaded_backup_folder.as_ref() {
                        self.inspect_tab.load_backup(folder);
                    }
                }

                let mut ctx = context::TabContext::new(&self.devices, &mut self.log_panel);
                self.inspect_tab.show(ui, &mut ctx);
            }
            Tab::Optical => {
                self.optical_tab
                    .show(ui, &mut self.log_panel, &mut self.progress);
            }
            Tab::Archives => {
                self.archives_tab.show(ui, &mut self.log_panel);
            }
        });

        // Workflow D.2 auto-unwrap: the Archives tab queues a payload
        // when the user clicks "Mount in new Inspect tab". Drain here,
        // hand it to the Inspect tab (which owns the tempdir guard so
        // the file survives), and switch tabs.
        if let Some(open) = self.archives_tab.take_pending_inspect_open() {
            self.inspect_tab
                .load_image_with_tempdir(open.path, Some(open.guard));
            self.active_tab = Tab::Inspect;
        }

        // Show settings dialog if open
        self.settings_dialog.show(ctx);
    }
}
