mod backup_tab;
mod browse_view;
mod bulk_convert_dialog;
mod chd_options_ui;
mod context;
mod elevation_dialog;
mod expand_hfs_dialog;
mod inspect_tab;
mod optical_tab;
mod partition_bar;
mod physical_disk_export;
mod progress;
mod resize_popup;
mod restore_tab;
mod settings_dialog;
mod size_mode_row;

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
/// Only handles `.adz` / `.hdz` (gzip-decompressed to a tempfile —
/// gzip isn't seekable). Everything else — including `.gho`/`.ghs`,
/// `.imz`, `.chd` — passes through unchanged, since the inspect worker
/// opens those via [`open_disk_image_for_reading`] (streaming
/// `Read + Seek` readers, no tempfile).
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
        _ => return Ok((path.to_path_buf(), None)),
    };
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
    let tmp = tempfile::tempdir()?;
    let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("amiga");
    let out_path = tmp.path().join(format!("{}.{}", stem, target_ext));
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
        std::fs::write(&gho_path, &[0xFE, 0xEF, 0x01, 0x00]).unwrap();
        let (out, guard) = prepare_disk_image_path(&gho_path).unwrap();
        assert_eq!(out, gho_path);
        assert!(guard.is_none());
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Tab {
    Backup,
    Restore,
    Inspect,
    Optical,
}

/// Main application state.
pub struct RustyBackupApp {
    active_tab: Tab,
    backup_tab: BackupTab,
    restore_tab: RestoreTab,
    inspect_tab: InspectTab,
    optical_tab: OpticalTab,
    log_panel: LogPanel,
    progress: ProgressState,
    devices: Vec<DiskDevice>,
    update_info: Arc<Mutex<Option<UpdateInfo>>>,
    update_dismissed: bool,
    settings_dialog: SettingsDialog,
    #[cfg(target_os = "linux")]
    elevation_dialog: ElevationDialog,
    /// Shared backup folder path between restore and inspect tabs
    loaded_backup_folder: Option<PathBuf>,
    /// Bulk Convert dialog state (None = closed).
    bulk_convert_dialog: Option<BulkConvertDialog>,
    /// Background bulk-convert worker progress (None = idle).
    bulk_convert_status: Option<Arc<Mutex<BulkConvertStatus>>>,
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
            log_panel: log,
            progress: ProgressState::default(),
            devices,
            update_info,
            update_dismissed: false,
            settings_dialog: SettingsDialog::default(),
            #[cfg(target_os = "linux")]
            elevation_dialog,
            loaded_backup_folder: None,
            bulk_convert_dialog: None,
            bulk_convert_status: None,
        }
    }
}

impl RustyBackupApp {
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

                    if ui.button("Refresh Devices").clicked() {
                        self.devices = device::enumerate_devices();
                        self.log_panel
                            .info(format!("Refreshed: {} device(s) found", self.devices.len()));
                        ui.ctx().request_repaint();
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

        // Update notification banner (if update available and not dismissed)
        if !self.update_dismissed {
            if let Ok(Some(ref info)) = self.update_info.lock().map(|guard| guard.clone()) {
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
                            if ui.button("View Release").clicked() {
                                let _ = webbrowser::open(&info.releases_url);
                            }
                            ui.with_layout(
                                egui::Layout::right_to_left(egui::Align::Center),
                                |ui| {
                                    if ui.button("Dismiss update notification").clicked() {
                                        self.update_dismissed = true;
                                    }
                                },
                            );
                        });
                    });
                }
            }
        }

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
        });

        // Show settings dialog if open
        self.settings_dialog.show(ctx);
    }
}
