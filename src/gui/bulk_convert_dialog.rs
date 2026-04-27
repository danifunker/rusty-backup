//! Bulk Convert dialog: convert every disk image in a folder to one chosen
//! output format, dropping results into a separate output folder.
//!
//! Reuses [`rusty_backup::rbformats::export::export_whole_disk`] (the same
//! single-file path the Inspect tab uses) — there is no per-partition sizing
//! in bulk mode, every disk exports at original size.
//!
//! Flow:
//! 1. **Setup** — pick source folder, output folder, output format.
//! 2. **Review** — scan source folder (no filtering); user un-checks any files
//!    they want to skip. Files that fail to open / detect during conversion
//!    surface as warnings in the log panel afterward.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use rusty_backup::rbformats::export::{export_whole_disk, ExportFormat};

fn fmt_bytes(n: u64) -> String {
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

/// Live state for a running bulk conversion job.
pub struct BulkConvertStatus {
    pub finished: bool,
    pub cancel_requested: bool,
    /// 1-based index of the file currently being processed.
    pub current_index: usize,
    pub total_files: usize,
    pub current_file: String,
    /// Bytes written for the currently-processing file (for the progress bar).
    pub current_bytes: u64,
    pub current_total_bytes: u64,
    pub succeeded: usize,
    pub failed: usize,
    /// Drained by the main thread into the GUI log panel each frame.
    pub log_messages: Vec<(LogLevel, String)>,
}

#[derive(Clone, Copy)]
pub enum LogLevel {
    Info,
    Warn,
    Error,
}

/// One entry in the review-phase checklist.
pub struct ScannedFile {
    pub path: PathBuf,
    pub size: u64,
    pub selected: bool,
}

/// One pending output filename collision.
struct Conflict {
    source: PathBuf,
    /// Existing destination file in the output folder.
    dest: PathBuf,
}

/// Which view of the dialog is active.
enum Phase {
    Setup,
    Review {
        files: Vec<ScannedFile>,
        /// User-editable output extension (no leading dot). Defaults to the
        /// recommended extension for the chosen format.
        extension: String,
        /// When the user clicks Start and one or more output files already
        /// exist in the destination, we stash the collisions here and render
        /// a confirmation modal instead of immediately emitting Start.
        pending_conflicts: Option<Vec<Conflict>>,
    },
}

/// Modal dialog state.
pub struct BulkConvertDialog {
    pub source_folder: Option<PathBuf>,
    pub output_folder: Option<PathBuf>,
    pub format: ExportFormat,
    phase: Phase,
    /// Set when scan returns no usable entries; surfaced inline in the dialog.
    scan_error: Option<String>,
}

impl Default for BulkConvertDialog {
    fn default() -> Self {
        Self {
            source_folder: None,
            output_folder: None,
            format: ExportFormat::Vhd,
            phase: Phase::Setup,
            scan_error: None,
        }
    }
}

/// Outcome of `show()` — caller acts on this to start the worker or close.
pub enum DialogAction {
    None,
    /// User confirmed Start in the Review phase. Carries the list of selected
    /// files so the worker doesn't have to re-scan.
    Start {
        files: Vec<PathBuf>,
        extension: String,
    },
    Cancel,
}

impl BulkConvertDialog {
    /// Render the dialog. Returns the user's intent for this frame.
    pub fn show(&mut self, ctx: &egui::Context) -> DialogAction {
        match self.phase {
            Phase::Setup => self.show_setup(ctx),
            Phase::Review { .. } => self.show_review(ctx),
        }
    }

    fn show_setup(&mut self, ctx: &egui::Context) -> DialogAction {
        let mut action = DialogAction::None;
        let mut go_to_review = false;

        egui::Window::new("Bulk Convert")
            .collapsible(false)
            .resizable(true)
            .default_width(560.0)
            .show(ctx, |ui| {
                ui.label(
                    "Convert every disk image in a folder to a single output format. \
                     All files use the same parameters; original size is preserved.",
                );
                ui.add_space(8.0);

                ui.horizontal(|ui| {
                    ui.label(egui::RichText::new("Source folder:").strong());
                    let label = self
                        .source_folder
                        .as_ref()
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|| "(not selected)".to_string());
                    ui.label(label);
                });
                if ui.button("Choose Source Folder…").clicked() {
                    if let Some(p) = super::file_dialog().pick_folder() {
                        self.source_folder = Some(p);
                    }
                }

                ui.add_space(6.0);

                ui.horizontal(|ui| {
                    ui.label(egui::RichText::new("Output folder:").strong());
                    let label = self
                        .output_folder
                        .as_ref()
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|| "(not selected)".to_string());
                    ui.label(label);
                });
                if ui.button("Choose Output Folder…").clicked() {
                    if let Some(p) = super::file_dialog().pick_folder() {
                        self.output_folder = Some(p);
                    }
                }

                ui.add_space(8.0);

                ui.label(egui::RichText::new("Output format:").strong());
                ui.horizontal(|ui| {
                    ui.radio_value(&mut self.format, ExportFormat::Vhd, "VHD");
                    ui.radio_value(&mut self.format, ExportFormat::Raw, "Raw (.img)");
                    ui.radio_value(&mut self.format, ExportFormat::TwoMg, "2MG (.2mg)");
                    ui.radio_value(&mut self.format, ExportFormat::Woz, "WOZ (.woz)")
                        .on_hover_text("Floppy only: 140K / 400K / 800K sources");
                    ui.radio_value(&mut self.format, ExportFormat::Dc42, "DiskCopy 4.2 (.dsk)")
                        .on_hover_text("Floppy only: 400K / 720K / 800K / 1440K sources");
                });

                if let (Some(src), Some(out)) = (&self.source_folder, &self.output_folder) {
                    if src == out {
                        ui.add_space(6.0);
                        ui.colored_label(
                            egui::Color32::YELLOW,
                            "Warning: source and output folders are the same — converted files may overwrite originals.",
                        );
                    }
                }

                if let Some(err) = &self.scan_error {
                    ui.add_space(6.0);
                    ui.colored_label(egui::Color32::from_rgb(255, 100, 100), err);
                }

                ui.add_space(10.0);
                ui.horizontal(|ui| {
                    let ready =
                        self.source_folder.is_some() && self.output_folder.is_some();
                    if ui
                        .add_enabled(ready, egui::Button::new("Scan…"))
                        .on_hover_text("Read the source folder and review the file list before converting.")
                        .clicked()
                    {
                        go_to_review = true;
                    }
                    if ui.button("Cancel").clicked() {
                        action = DialogAction::Cancel;
                    }
                });
            });

        if go_to_review {
            match scan_source_folder(self.source_folder.as_ref().unwrap()) {
                Ok(files) if files.is_empty() => {
                    self.scan_error = Some("No files found in source folder.".to_string());
                }
                Ok(files) => {
                    self.scan_error = None;
                    self.phase = Phase::Review {
                        files,
                        extension: self.format.extension().to_string(),
                        pending_conflicts: None,
                    };
                }
                Err(e) => {
                    self.scan_error = Some(format!("Scan failed: {e}"));
                }
            }
        }

        action
    }

    fn show_review(&mut self, ctx: &egui::Context) -> DialogAction {
        let mut action = DialogAction::None;
        let mut go_back = false;
        let mut start = false;
        let mut select_all = false;
        let mut select_none = false;

        let recommended_ext = self.format.extension();
        let output_folder = self.output_folder.clone();
        let Phase::Review {
            files,
            extension,
            pending_conflicts,
        } = &mut self.phase
        else {
            return action;
        };

        egui::Window::new("Bulk Convert — Review")
            .collapsible(false)
            .resizable(true)
            .default_width(640.0)
            .default_height(500.0)
            .show(ctx, |ui| {
                let total = files.len();
                let selected_count = files.iter().filter(|f| f.selected).count();

                ui.label(format!(
                    "Found {} file(s) in source folder. {} selected for conversion.",
                    total, selected_count,
                ));
                ui.label(
                    "Files that fail to open or aren't recognized as disk images will be \
                     reported as warnings in the log after conversion finishes.",
                );

                ui.add_space(6.0);

                // Extension field — pre-filled with the recommended extension
                // for the chosen format. The user can override (e.g. ".hda"
                // instead of ".vhd") and reset back to the recommendation.
                ui.horizontal(|ui| {
                    ui.label(egui::RichText::new("Output extension:").strong());
                    ui.label(".");
                    ui.add(
                        egui::TextEdit::singleline(extension)
                            .desired_width(80.0)
                            .hint_text(recommended_ext),
                    );
                    if ui
                        .add_enabled(
                            extension.as_str() != recommended_ext,
                            egui::Button::new("Reset"),
                        )
                        .on_hover_text(format!("Reset to recommended: .{recommended_ext}"))
                        .clicked()
                    {
                        *extension = recommended_ext.to_string();
                    }
                    ui.label(
                        egui::RichText::new(format!("(recommended: .{recommended_ext})")).weak(),
                    );
                });

                ui.add_space(6.0);
                ui.horizontal(|ui| {
                    if ui.button("Select All").clicked() {
                        select_all = true;
                    }
                    if ui.button("Select None").clicked() {
                        select_none = true;
                    }
                });

                ui.add_space(6.0);
                ui.separator();

                egui::ScrollArea::vertical()
                    .auto_shrink([false; 2])
                    .max_height(340.0)
                    .show(ui, |ui| {
                        egui::Grid::new("bulk_convert_files")
                            .striped(true)
                            .num_columns(3)
                            .min_col_width(40.0)
                            .show(ui, |ui| {
                                ui.label(egui::RichText::new("").strong());
                                ui.label(egui::RichText::new("File").strong());
                                ui.label(egui::RichText::new("Size").strong());
                                ui.end_row();

                                for f in files.iter_mut() {
                                    ui.checkbox(&mut f.selected, "");
                                    let name =
                                        f.path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                                    ui.label(name);
                                    ui.label(fmt_bytes(f.size));
                                    ui.end_row();
                                }
                            });
                    });

                ui.separator();
                ui.add_space(6.0);
                ui.horizontal(|ui| {
                    if ui.button("Back").clicked() {
                        go_back = true;
                    }
                    let ext_ok =
                        !extension.trim().is_empty() && !extension.contains(['/', '\\', '.', ' ']);
                    let can_start = selected_count > 0 && ext_ok;
                    let label = format!("Start Conversion ({selected_count})");
                    if ui
                        .add_enabled(can_start, egui::Button::new(label))
                        .clicked()
                    {
                        start = true;
                    }
                    if ui.button("Cancel").clicked() {
                        action = DialogAction::Cancel;
                    }
                });
            });

        if select_all {
            for f in files.iter_mut() {
                f.selected = true;
            }
        }
        if select_none {
            for f in files.iter_mut() {
                f.selected = false;
            }
        }
        // If the user clicked Start, detect output-folder collisions before
        // emitting the action. If any are found, populate `pending_conflicts`
        // so the modal renders next frame; otherwise emit Start immediately.
        let mut emit_start: Option<(Vec<PathBuf>, String)> = None;
        if start {
            let ext = extension.trim().to_string();
            let chosen_paths: Vec<PathBuf> = files
                .iter()
                .filter(|f| f.selected)
                .map(|f| f.path.clone())
                .collect();

            let conflicts = if let Some(out) = &output_folder {
                detect_conflicts(&chosen_paths, out, &ext)
            } else {
                Vec::new()
            };

            if conflicts.is_empty() {
                emit_start = Some((chosen_paths, ext));
            } else {
                *pending_conflicts = Some(conflicts);
            }
        }

        // Render the conflict modal on top of the Review window if collisions
        // are pending. Returns the user's resolution, if any.
        let resolution = if let Some(conflicts) = pending_conflicts.as_ref() {
            show_conflict_modal(ctx, conflicts)
        } else {
            ConflictResolution::None
        };

        match resolution {
            ConflictResolution::SkipConflicting => {
                let conflict_set: std::collections::HashSet<PathBuf> = pending_conflicts
                    .as_ref()
                    .map(|cs| cs.iter().map(|c| c.source.clone()).collect())
                    .unwrap_or_default();
                let chosen: Vec<PathBuf> = files
                    .iter()
                    .filter(|f| f.selected && !conflict_set.contains(&f.path))
                    .map(|f| f.path.clone())
                    .collect();
                let ext = extension.trim().to_string();
                *pending_conflicts = None;
                if !chosen.is_empty() {
                    emit_start = Some((chosen, ext));
                }
            }
            ConflictResolution::Overwrite => {
                let chosen: Vec<PathBuf> = files
                    .iter()
                    .filter(|f| f.selected)
                    .map(|f| f.path.clone())
                    .collect();
                let ext = extension.trim().to_string();
                *pending_conflicts = None;
                emit_start = Some((chosen, ext));
            }
            ConflictResolution::Back => {
                *pending_conflicts = None;
            }
            ConflictResolution::None => {}
        }

        // Drop the &mut borrow on self.phase before mutating it.
        if go_back {
            self.phase = Phase::Setup;
        }
        if let Some((files, extension)) = emit_start {
            action = DialogAction::Start { files, extension };
        }

        action
    }
}

#[derive(Clone, Copy)]
enum ConflictResolution {
    None,
    SkipConflicting,
    Overwrite,
    Back,
}

fn detect_conflicts(
    sources: &[PathBuf],
    output: &std::path::Path,
    extension: &str,
) -> Vec<Conflict> {
    let mut out = Vec::new();
    for src in sources {
        let stem = src.file_stem().and_then(|s| s.to_str()).unwrap_or("disk");
        let dest = output.join(format!("{stem}.{extension}"));
        if dest.exists() {
            out.push(Conflict {
                source: src.clone(),
                dest,
            });
        }
    }
    out
}

fn show_conflict_modal(ctx: &egui::Context, conflicts: &[Conflict]) -> ConflictResolution {
    let mut resolution = ConflictResolution::None;

    egui::Window::new("Output Files Already Exist")
        .collapsible(false)
        .resizable(true)
        .default_width(560.0)
        .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
        .show(ctx, |ui| {
            ui.label(format!(
                "{} file(s) in the output folder would be overwritten by this conversion:",
                conflicts.len(),
            ));
            ui.add_space(6.0);

            egui::ScrollArea::vertical()
                .auto_shrink([false; 2])
                .max_height(200.0)
                .show(ui, |ui| {
                    for c in conflicts {
                        let name = c.dest.file_name().and_then(|n| n.to_str()).unwrap_or("");
                        ui.label(format!("- {name}"));
                    }
                });

            ui.add_space(8.0);
            ui.label(
                "Skip: leave existing files alone, convert only the non-conflicting ones.\n\
                 Overwrite: replace the existing output files.",
            );
            ui.add_space(8.0);

            ui.horizontal(|ui| {
                if ui.button("Skip Conflicting").clicked() {
                    resolution = ConflictResolution::SkipConflicting;
                }
                if ui.button("Overwrite").clicked() {
                    resolution = ConflictResolution::Overwrite;
                }
                if ui.button("Back").clicked() {
                    resolution = ConflictResolution::Back;
                }
            });
        });

    resolution
}

/// Read the source folder and return one entry per regular file. No filtering
/// — the user picks what to skip in the Review phase. Hidden dotfiles are
/// still excluded since they're rarely user-visible disk images and clutter
/// the list (e.g. `.DS_Store` on macOS).
fn scan_source_folder(source: &std::path::Path) -> std::io::Result<Vec<ScannedFile>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let name = match path.file_name().and_then(|n| n.to_str()) {
            Some(s) => s,
            None => continue,
        };
        if name.starts_with('.') {
            continue;
        }
        let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
        out.push(ScannedFile {
            path,
            size,
            selected: true,
        });
    }
    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

/// Spawn a worker thread that converts the supplied files into `output`.
/// Returns the shared status handle.
pub fn start_bulk_convert(
    files: Vec<PathBuf>,
    output: PathBuf,
    format: ExportFormat,
    extension: String,
) -> Arc<Mutex<BulkConvertStatus>> {
    let status = Arc::new(Mutex::new(BulkConvertStatus {
        finished: false,
        cancel_requested: false,
        current_index: 0,
        total_files: files.len(),
        current_file: String::new(),
        current_bytes: 0,
        current_total_bytes: 0,
        succeeded: 0,
        failed: 0,
        log_messages: Vec::new(),
    }));

    let status_thread = Arc::clone(&status);
    std::thread::spawn(move || {
        let status_panic = Arc::clone(&status_thread);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_bulk_convert(files, output, format, extension, status_thread);
        }));
        if let Err(payload) = result {
            let msg = if let Some(s) = payload.downcast_ref::<&'static str>() {
                (*s).to_string()
            } else if let Some(s) = payload.downcast_ref::<String>() {
                s.clone()
            } else {
                "(unknown panic payload)".to_string()
            };
            if let Ok(mut s) = status_panic.lock() {
                s.log_messages.push((
                    LogLevel::Error,
                    format!("Bulk convert worker panicked: {msg}"),
                ));
                s.finished = true;
            }
        }
    });

    status
}

fn run_bulk_convert(
    files: Vec<PathBuf>,
    output: PathBuf,
    format: ExportFormat,
    extension: String,
    status: Arc<Mutex<BulkConvertStatus>>,
) {
    let push_log = |level: LogLevel, msg: String| {
        if let Ok(mut s) = status.lock() {
            s.log_messages.push((level, msg));
        }
    };

    push_log(
        LogLevel::Info,
        format!(
            "Bulk convert: {} file(s), output -> {}",
            files.len(),
            output.display(),
        ),
    );

    if let Err(e) = std::fs::create_dir_all(&output) {
        push_log(
            LogLevel::Error,
            format!("Failed to create output folder {}: {}", output.display(), e),
        );
        if let Ok(mut s) = status.lock() {
            s.finished = true;
        }
        return;
    }

    // Track skipped (failed) files so we can list them all in the final summary.
    let mut skipped: Vec<(PathBuf, String)> = Vec::new();

    for (idx, file) in files.iter().enumerate() {
        if status.lock().map(|s| s.cancel_requested).unwrap_or(false) {
            push_log(LogLevel::Warn, "Cancelled by user.".to_string());
            break;
        }

        let stem = file
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("disk")
            .to_string();
        let dest = output.join(format!("{stem}.{extension}"));

        let source_size = std::fs::metadata(file).map(|m| m.len()).unwrap_or(0);
        if let Ok(mut s) = status.lock() {
            s.current_index = idx + 1;
            s.current_file = file
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .to_string();
            s.current_bytes = 0;
            s.current_total_bytes = source_size;
        }

        push_log(
            LogLevel::Info,
            format!(
                "[{}/{}] Converting {} ({}) -> {}",
                idx + 1,
                files.len(),
                file.display(),
                fmt_bytes(source_size),
                dest.display(),
            ),
        );

        let status_progress = Arc::clone(&status);
        let status_cancel = Arc::clone(&status);
        let status_log = Arc::clone(&status);

        let started = Instant::now();
        let log_for_progress = Arc::clone(&status);
        let mut last_logged_bytes: u64 = 0;
        const LOG_EVERY_BYTES: u64 = 64 * 1024 * 1024;

        let result = export_whole_disk(
            format,
            file,
            None,
            None,
            &[],
            &dest,
            move |bytes| {
                if let Ok(mut s) = status_progress.lock() {
                    s.current_bytes = bytes;
                }
                if bytes >= last_logged_bytes + LOG_EVERY_BYTES {
                    last_logged_bytes = bytes;
                    let elapsed = started.elapsed().as_secs_f64().max(0.001);
                    let mbs = (bytes as f64 / 1_048_576.0) / elapsed;
                    if let Ok(mut s) = log_for_progress.lock() {
                        s.log_messages.push((
                            LogLevel::Info,
                            format!("    … {} written ({:.1} MB/s)", fmt_bytes(bytes), mbs),
                        ));
                    }
                }
            },
            move || {
                status_cancel
                    .lock()
                    .map(|s| s.cancel_requested)
                    .unwrap_or(false)
            },
            move |msg| {
                if let Ok(mut s) = status_log.lock() {
                    s.log_messages.push((LogLevel::Info, msg.to_string()));
                }
            },
        );

        match result {
            Ok(()) => {
                if let Ok(mut s) = status.lock() {
                    s.succeeded += 1;
                }
                let elapsed = started.elapsed().as_secs_f64();
                push_log(
                    LogLevel::Info,
                    format!("  OK: {} (in {:.1}s)", dest.display(), elapsed),
                );
            }
            Err(e) => {
                if let Ok(mut s) = status.lock() {
                    s.failed += 1;
                }
                let _ = std::fs::remove_file(&dest);
                let reason = format!("{e:#}");
                push_log(
                    LogLevel::Warn,
                    format!("  Skipped {} - {}", file.display(), reason),
                );
                skipped.push((file.clone(), reason));
            }
        }
    }

    // Final summary, including a per-file list of every skipped/failed entry.
    let (succeeded, failed) = if let Ok(s) = status.lock() {
        (s.succeeded, s.failed)
    } else {
        (0, 0)
    };
    push_log(
        LogLevel::Info,
        format!(
            "Bulk convert finished: {} succeeded, {} skipped/failed.",
            succeeded, failed,
        ),
    );
    if !skipped.is_empty() {
        push_log(
            LogLevel::Warn,
            format!("{} file(s) were skipped:", skipped.len()),
        );
        for (path, reason) in &skipped {
            push_log(
                LogLevel::Warn,
                format!("  - {}: {}", path.display(), reason),
            );
        }
    }
    if let Ok(mut s) = status.lock() {
        s.finished = true;
    }
}
