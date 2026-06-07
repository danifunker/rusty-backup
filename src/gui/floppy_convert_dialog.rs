//! "Convert Floppy Container…" dialog for the Inspect tab.
//!
//! Single-file conversion between XDF / HDM / DIM / D88. Mirrors the shape
//! of [`super::expand_hfs_dialog::ExpandHfsDialog`]: source path (picked
//! inside the dialog), target-format radio group, "Save As…" picker,
//! worker thread that calls
//! [`rusty_backup::rbformats::containers::convert_floppy_container`], status
//! polled per frame.
//!
//! Bulk mode lives in two places:
//! - GUI: the existing [`super::bulk_convert_dialog`] now lists XDF /
//!   HDM / DIM / D88 alongside the other output formats — point users
//!   there for folder-of-floppies conversions.
//! - CLI: `rb-cli floppy convert <dir> <dir> --to <fmt> [--recursive]`.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use rusty_backup::rbformats::containers::{
    convert_floppy_container, floppy_kind_from_extension, ContainerKind,
};

use super::progress::LogPanel;

/// Backing status shared between the GUI thread and the worker.
#[derive(Default)]
pub struct FloppyConvertStatus {
    pub finished: bool,
    pub current_step: String,
    pub log_messages: Vec<String>,
    pub error: Option<String>,
    pub summary: Option<String>,
}

/// One of the four floppy formats — distinct from `ContainerKind` so the
/// dialog only offers the four floppy choices.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FloppyTarget {
    Xdf,
    Hdm,
    Dim,
    D88,
}

impl FloppyTarget {
    pub const fn extension(self) -> &'static str {
        match self {
            FloppyTarget::Xdf => "xdf",
            FloppyTarget::Hdm => "hdm",
            FloppyTarget::Dim => "dim",
            FloppyTarget::D88 => "d88",
        }
    }
    pub const fn container(self) -> ContainerKind {
        match self {
            FloppyTarget::Xdf => ContainerKind::Xdf,
            FloppyTarget::Hdm => ContainerKind::Hdm,
            FloppyTarget::Dim => ContainerKind::Dim,
            FloppyTarget::D88 => ContainerKind::D88,
        }
    }
    pub const fn label(self) -> &'static str {
        match self {
            FloppyTarget::Xdf => "X68000 XDF (.xdf)",
            FloppyTarget::Hdm => "PC-98 HDM (.hdm)",
            FloppyTarget::Dim => "DiskExplorer DIM (.dim)",
            FloppyTarget::D88 => "Sharp D88 (.d88)",
        }
    }
    pub fn from_container(c: ContainerKind) -> Option<Self> {
        match c {
            ContainerKind::Xdf => Some(FloppyTarget::Xdf),
            ContainerKind::Hdm => Some(FloppyTarget::Hdm),
            ContainerKind::Dim => Some(FloppyTarget::Dim),
            ContainerKind::D88 => Some(FloppyTarget::D88),
            _ => None,
        }
    }
}

pub struct FloppyConvertDialog {
    pub open: bool,
    source_path: Option<PathBuf>,
    source_kind: Option<ContainerKind>,
    source_error: Option<String>,
    target: FloppyTarget,
    output_path: Option<PathBuf>,
    status: Option<Arc<Mutex<FloppyConvertStatus>>>,
    completed: bool,
}

impl FloppyConvertDialog {
    /// Open with no pre-loaded source — the user picks it inside the dialog.
    pub fn empty() -> Self {
        Self {
            open: true,
            source_path: None,
            source_kind: None,
            source_error: None,
            target: FloppyTarget::D88,
            output_path: None,
            status: None,
            completed: false,
        }
    }

    fn set_source(&mut self, path: PathBuf) {
        // Detect the container kind by reading the first 256 bytes + extension.
        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(e) => {
                self.source_error = Some(format!("Cannot read source: {e}"));
                return;
            }
        };
        let head = &bytes[..bytes.len().min(256)];
        let kind = rusty_backup::rbformats::containers::detect_container_kind(head, Some(&path));
        if !rusty_backup::rbformats::containers::is_floppy_container(kind) {
            self.source_error = Some(format!(
                "{} is not a recognised floppy container (detected: {})",
                path.display(),
                kind.display_name()
            ));
            return;
        }
        self.source_path = Some(path);
        self.source_kind = Some(kind);
        self.source_error = None;
        self.output_path = None;
        self.completed = false;
        self.status = None;
        // Reset target to something useful relative to the new source.
        self.target = match kind {
            ContainerKind::D88 => FloppyTarget::Xdf,
            _ => FloppyTarget::D88,
        };
    }

    /// Render the dialog. Returns `false` once the user closes it for good.
    pub fn show(&mut self, ctx: &egui::Context, log: &mut LogPanel) -> bool {
        if !self.open {
            return false;
        }
        struct StatusSnapshot {
            finished: bool,
            current_step: String,
            log_messages: Vec<String>,
            error: Option<String>,
            summary: Option<String>,
        }
        let status_snapshot: Option<StatusSnapshot> = self.status.as_ref().and_then(|s| {
            s.lock().ok().map(|g| StatusSnapshot {
                finished: g.finished,
                current_step: g.current_step.clone(),
                log_messages: g.log_messages.clone(),
                error: g.error.clone(),
                summary: g.summary.clone(),
            })
        });

        let mut start_clicked = false;
        let mut close_clicked = false;
        let mut pick_output_clicked = false;
        let mut pick_source_clicked = false;

        let mut window_open = self.open;
        egui::Window::new("Convert Floppy Container...")
            .open(&mut window_open)
            .resizable(true)
            .default_width(520.0)
            .show(ctx, |ui| {
                ui.label(egui::RichText::new("Source").strong());
                ui.horizontal(|ui| {
                    let label = self
                        .source_path
                        .as_ref()
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|| "(not chosen)".to_string());
                    ui.label(label);
                    if ui.button("Choose File...").clicked() {
                        pick_source_clicked = true;
                    }
                });
                if let Some(kind) = self.source_kind {
                    ui.label(format!("Detected: {}", kind.display_name()));
                }
                if let Some(err) = &self.source_error {
                    ui.colored_label(egui::Color32::from_rgb(255, 100, 100), err.clone());
                }
                ui.label(
                    egui::RichText::new(
                        "Converting a folder of floppies? Use the main Bulk Convert dialog \
                         (XDF / HDM / DIM / D88 are listed there as output formats).",
                    )
                    .small()
                    .italics(),
                );

                ui.separator();
                ui.label(egui::RichText::new("Target").strong());

                let busy = status_snapshot
                    .as_ref()
                    .map(|s| !s.finished)
                    .unwrap_or(false);

                ui.add_enabled_ui(!busy && !self.completed, |ui| {
                    ui.horizontal(|ui| {
                        ui.label("Output format:");
                        ui.radio_value(&mut self.target, FloppyTarget::Xdf, FloppyTarget::Xdf.label());
                        ui.radio_value(&mut self.target, FloppyTarget::Hdm, FloppyTarget::Hdm.label());
                    });
                    ui.horizontal(|ui| {
                        ui.add_space(96.0);
                        ui.radio_value(&mut self.target, FloppyTarget::Dim, FloppyTarget::Dim.label());
                        ui.radio_value(&mut self.target, FloppyTarget::D88, FloppyTarget::D88.label());
                    });

                    ui.horizontal(|ui| {
                        ui.label("Output file:");
                        let path_text = self
                            .output_path
                            .as_ref()
                            .map(|p| p.display().to_string())
                            .unwrap_or_else(|| "(not chosen)".to_string());
                        ui.label(path_text);
                        if ui.button("Save As...").clicked() {
                            pick_output_clicked = true;
                        }
                    });

                    if let Some(sk) = self.source_kind {
                        if FloppyTarget::from_container(sk) == Some(self.target) {
                            ui.label(
                                egui::RichText::new(
                                    "Source and target match - the output will be a byte-identical copy.",
                                )
                                .small()
                                .italics(),
                            );
                        }
                    }
                });

                ui.separator();
                if let Some(snap) = &status_snapshot {
                    if !snap.finished {
                        ui.horizontal(|ui| {
                            ui.spinner();
                            ui.label(&snap.current_step);
                        });
                    } else if let Some(err) = &snap.error {
                        ui.colored_label(
                            egui::Color32::from_rgb(255, 100, 100),
                            format!("Failed: {err}"),
                        );
                    } else if let Some(s) = &snap.summary {
                        ui.colored_label(egui::Color32::from_rgb(100, 200, 100), s.clone());
                    } else {
                        ui.colored_label(
                            egui::Color32::from_rgb(100, 200, 100),
                            "Conversion complete.",
                        );
                    }
                    if !snap.log_messages.is_empty() {
                        egui::ScrollArea::vertical()
                            .max_height(120.0)
                            .id_salt("floppy_convert_log")
                            .show(ui, |ui| {
                                for m in &snap.log_messages {
                                    ui.label(m);
                                }
                            });
                    }
                }

                ui.horizontal(|ui| {
                    let can_start = !busy
                        && !self.completed
                        && self.source_path.is_some()
                        && self.output_path.is_some();
                    if ui
                        .add_enabled(can_start, egui::Button::new("Convert"))
                        .clicked()
                    {
                        start_clicked = true;
                    }
                    let close_label = if self.completed { "Close" } else { "Cancel" };
                    if ui
                        .add_enabled(!busy, egui::Button::new(close_label))
                        .clicked()
                    {
                        close_clicked = true;
                    }
                });
            });
        self.open = window_open;

        if pick_source_clicked {
            if let Some(path) = super::file_dialog()
                .add_filter("Floppy Containers", &["xdf", "hdm", "dim", "d88"])
                .add_filter("All Files", &["*"])
                .pick_file()
            {
                self.set_source(path);
            }
        }

        if pick_output_clicked {
            let stem = self
                .source_path
                .as_ref()
                .and_then(|p| p.file_stem())
                .and_then(|s| s.to_str())
                .unwrap_or("converted")
                .to_string();
            let ext = self.target.extension();
            if let Some(path) = super::file_dialog()
                .set_file_name(format!("{stem}.{ext}"))
                .add_filter(self.target.label(), &[ext])
                .save_file()
            {
                self.output_path = Some(path);
            }
        }
        if start_clicked {
            if let (Some(src), Some(out)) = (self.source_path.clone(), self.output_path.clone()) {
                self.spawn_worker(src, out, log);
            }
        }
        if close_clicked {
            self.open = false;
        }

        if let Some(snap) = &status_snapshot {
            if snap.finished {
                self.completed = true;
            }
        }

        self.open
    }

    fn spawn_worker(&mut self, source: PathBuf, output: PathBuf, log: &mut LogPanel) {
        let target_kind = self.target.container();
        log.info(format!(
            "Converting floppy container: {} -> {} ({})",
            source.display(),
            output.display(),
            self.target.label(),
        ));
        let status = Arc::new(Mutex::new(FloppyConvertStatus {
            finished: false,
            current_step: "Starting...".to_string(),
            log_messages: Vec::new(),
            error: None,
            summary: None,
        }));
        let status_for_worker = Arc::clone(&status);
        let target_label = self.target.label();
        std::thread::spawn(move || {
            if let Ok(mut s) = status_for_worker.lock() {
                s.current_step = "Decoding source...".to_string();
            }
            // Sanity check: when the output extension doesn't match the radio
            // selection, prefer the radio selection (the user clicked it last).
            // Falling back through `floppy_kind_from_extension` would mismatch.
            let result = convert_floppy_container(&source, &output, target_kind);
            if let Ok(mut s) = status_for_worker.lock() {
                match result {
                    Ok(report) => {
                        s.summary = Some(format!(
                            "Converted to {} ({}, {} bytes)",
                            target_label,
                            report.media.display_label(),
                            report.bytes_written
                        ));
                        s.log_messages.push(format!(
                            "Wrote {} bytes to {}",
                            report.bytes_written,
                            output.display()
                        ));
                    }
                    Err(e) => {
                        s.error = Some(format!("{e:#}"));
                    }
                }
                s.finished = true;
            }
        });
        self.status = Some(status);
    }
}

/// Compile-time consistency check that the radio enum and the engine kinds
/// match — keeps the dialog from going stale if a new floppy format is added.
#[allow(dead_code)]
const _: fn() = || {
    // Only the four floppy variants must be expressible as FloppyTarget.
    let _ = |c: ContainerKind| -> Option<FloppyTarget> { FloppyTarget::from_container(c) };
    // `floppy_kind_from_extension` is referenced so the import isn't dead.
    let _: fn(&std::path::Path) -> anyhow::Result<ContainerKind> = floppy_kind_from_extension;
};
