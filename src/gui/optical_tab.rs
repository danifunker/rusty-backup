use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use opticaldiscs::detect::DiscImageInfo;
use opticaldiscs::drives::OpticalDrive;
use opticaldiscs::formats::DiscFormat;

use rusty_backup::backup::LogLevel as BackupLogLevel;
use rusty_backup::optical::browse_view::OpticalDiscBrowseView;
use rusty_backup::optical::convert::ConvertProgress;
use rusty_backup::optical::rip::{RipConfig, RipFormat, RipProgress};

use super::progress::{LogPanel, ProgressState};

#[derive(Debug, Clone, Copy, PartialEq)]
enum SourceMode {
    PhysicalDrive,
    ImageFile,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Action {
    Rip,
    Convert,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum OutputFormat {
    Iso,
    BinCue,
    Chd,
}

/// State for the Optical disc tab.
pub struct OpticalTab {
    source_mode: SourceMode,
    drives: Vec<OpticalDrive>,
    selected_drive_idx: Option<usize>,
    image_file_path: Option<PathBuf>,
    disc_info: Option<DiscImageInfo>,
    disc_info_error: Option<String>,
    action: Action,
    output_format: OutputFormat,
    output_path: Option<PathBuf>,
    eject_after: bool,
    rip_running: bool,
    rip_progress: Option<Arc<Mutex<RipProgress>>>,
    convert_running: bool,
    convert_progress: Option<Arc<Mutex<ConvertProgress>>>,
    browse_view: OpticalDiscBrowseView,
    chdman_available: bool,
    /// Track source changes for auto-detection
    prev_drive_idx: Option<usize>,
    prev_image_path: Option<PathBuf>,
}

impl Default for OpticalTab {
    fn default() -> Self {
        Self {
            source_mode: SourceMode::ImageFile,
            drives: Vec::new(),
            selected_drive_idx: None,
            image_file_path: None,
            disc_info: None,
            disc_info_error: None,
            action: Action::Rip,
            output_format: OutputFormat::Iso,
            output_path: None,
            eject_after: false,
            rip_running: false,
            rip_progress: None,
            convert_running: false,
            convert_progress: None,
            browse_view: OpticalDiscBrowseView::default(),
            chdman_available: false,
            prev_drive_idx: None,
            prev_image_path: None,
        }
    }
}

impl OpticalTab {
    pub fn set_chdman_available(&mut self, available: bool) {
        self.chdman_available = available;
        if !available && self.output_format == OutputFormat::Chd {
            self.output_format = OutputFormat::Iso;
        }
    }

    pub fn refresh_drives(&mut self) {
        self.drives = opticaldiscs::drives::list_drives();
    }

    pub fn drive_count(&self) -> usize {
        self.drives.len()
    }

    pub fn is_running(&self) -> bool {
        self.rip_running || self.convert_running
    }

    pub fn show(&mut self, ui: &mut egui::Ui, log: &mut LogPanel, progress: &mut ProgressState) {
        self.poll_progress(log, progress);

        ui.heading("Optical Disc");
        ui.add_space(8.0);

        let controls_enabled = !self.rip_running && !self.convert_running;

        // ── Source selection ─────────────────────────────────────────────────
        ui.add_enabled_ui(controls_enabled, |ui| {
            ui.horizontal(|ui| {
                ui.label("Source:");
                ui.radio_value(
                    &mut self.source_mode,
                    SourceMode::PhysicalDrive,
                    "Physical drive",
                );
                ui.radio_value(&mut self.source_mode, SourceMode::ImageFile, "Image file");
            });

            match self.source_mode {
                SourceMode::PhysicalDrive => {
                    ui.horizontal(|ui| {
                        ui.add_space(60.0);

                        let current_label = self
                            .selected_drive_idx
                            .and_then(|idx| self.drives.get(idx))
                            .map(|d| d.display_name.clone())
                            .unwrap_or_else(|| "Select a drive...".into());

                        egui::ComboBox::from_id_salt("optical_drive")
                            .selected_text(&current_label)
                            .width(300.0)
                            .show_ui(ui, |ui| {
                                for (i, drive) in self.drives.iter().enumerate() {
                                    let label = format!(
                                        "{} ({})",
                                        drive.display_name,
                                        drive.device_path.display()
                                    );
                                    ui.selectable_value(
                                        &mut self.selected_drive_idx,
                                        Some(i),
                                        &label,
                                    );
                                }
                            });

                        if ui.button("Refresh").clicked() {
                            self.refresh_drives();
                            log.info(format!(
                                "Refreshed: {} optical drive(s) found",
                                self.drives.len()
                            ));
                        }
                    });
                }
                SourceMode::ImageFile => {
                    ui.horizontal(|ui| {
                        ui.add_space(60.0);

                        let path_text = self
                            .image_file_path
                            .as_ref()
                            .map(|p| p.display().to_string())
                            .unwrap_or_else(|| "No file selected".into());
                        ui.label(&path_text);

                        if ui.button("Browse...").clicked() {
                            if let Some(path) = super::file_dialog()
                                .add_filter(
                                    "Disc Images",
                                    &["iso", "bin", "cue", "chd", "toast", "img"],
                                )
                                .add_filter("All Files", &["*"])
                                .pick_file()
                            {
                                self.image_file_path = Some(path);
                            }
                        }
                    });
                }
            }
        });

        // ── Auto-detect disc info on source change ──────────────────────────
        if controls_enabled {
            let source_changed = match self.source_mode {
                SourceMode::PhysicalDrive => self.selected_drive_idx != self.prev_drive_idx,
                SourceMode::ImageFile => self.image_file_path != self.prev_image_path,
            };

            if source_changed {
                self.prev_drive_idx = self.selected_drive_idx;
                self.prev_image_path = self.image_file_path.clone();
                self.detect_disc_info(log);
            }
        }

        // ── Disc info display ───────────────────────────────────────────────
        ui.add_space(4.0);
        ui.horizontal(|ui| {
            ui.label("Disc Info:");
            if let Some(info) = &self.disc_info {
                ui.label(format!("Format: {}", info.format.display_name()));
                ui.label(format!("FS: {}", info.filesystem.display_name()));
                if let Some(ref label) = info.volume_label {
                    ui.label(format!("Volume: {label}"));
                }
            } else if let Some(err) = &self.disc_info_error {
                ui.colored_label(egui::Color32::from_rgb(255, 100, 100), err);
            } else {
                ui.colored_label(egui::Color32::GRAY, "No source selected");
            }
        });

        // Browse Contents button
        if self.disc_info.is_some() && !self.browse_view.is_active() {
            ui.horizontal(|ui| {
                ui.add_space(60.0);
                if ui.button("Browse Contents").clicked() {
                    if let Some(path) = self.get_browsable_path() {
                        self.browse_view.open(&path);
                    }
                }
            });
        }

        ui.separator();

        // ── Action selection ────────────────────────────────────────────────
        ui.add_enabled_ui(controls_enabled, |ui| {
            ui.horizontal(|ui| {
                ui.label("Action:");

                let can_rip = self.source_mode == SourceMode::PhysicalDrive;
                let can_convert = self.source_mode == SourceMode::ImageFile;

                if can_rip {
                    ui.radio_value(&mut self.action, Action::Rip, "Rip to disk");
                }
                if can_convert {
                    ui.radio_value(&mut self.action, Action::Convert, "Convert format");
                }

                // Auto-select valid action when source mode changes
                if !can_rip && self.action == Action::Rip {
                    self.action = Action::Convert;
                }
                if !can_convert && self.action == Action::Convert {
                    self.action = Action::Rip;
                }
            });

            // ── Output format ───────────────────────────────────────────────
            ui.horizontal(|ui| {
                ui.label("Output:");

                let available_formats = match self.action {
                    Action::Rip => vec![
                        (OutputFormat::Iso, "ISO", true),
                        (OutputFormat::BinCue, "BIN/CUE", true),
                    ],
                    Action::Convert => {
                        let source_format = self.disc_info.as_ref().map(|i| &i.format);
                        vec![
                            (
                                OutputFormat::Iso,
                                "ISO",
                                source_format != Some(&DiscFormat::Iso),
                            ),
                            (
                                OutputFormat::BinCue,
                                "BIN/CUE",
                                source_format != Some(&DiscFormat::BinCue),
                            ),
                            (
                                OutputFormat::Chd,
                                "CHD",
                                self.chdman_available && source_format != Some(&DiscFormat::Chd),
                            ),
                        ]
                    }
                };

                for (fmt, label, enabled) in &available_formats {
                    let response = ui.add_enabled(
                        *enabled,
                        egui::RadioButton::new(self.output_format == *fmt, *label),
                    );
                    if response.clicked() {
                        self.output_format = *fmt;
                    }
                    if !enabled && *fmt == OutputFormat::Chd && !self.chdman_available {
                        response.on_disabled_hover_text(
                            "chdman not found. Configure path in Settings.",
                        );
                    }
                }
            });

            // ── Output path ─────────────────────────────────────────────────
            ui.horizontal(|ui| {
                ui.add_space(60.0);

                let path_text = self
                    .output_path
                    .as_ref()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| "No output path selected".into());
                ui.label(&path_text);

                if ui.button("Browse...").clicked() {
                    let ext = match self.output_format {
                        OutputFormat::Iso => "iso",
                        OutputFormat::BinCue => "cue",
                        OutputFormat::Chd => "chd",
                    };
                    if let Some(path) = super::file_dialog()
                        .add_filter("Output", &[ext])
                        .set_file_name(self.default_output_name())
                        .save_file()
                    {
                        self.output_path = Some(path);
                    }
                }
            });

            // ── Eject checkbox (rip only) ───────────────────────────────────
            if self.action == Action::Rip {
                ui.horizontal(|ui| {
                    ui.add_space(60.0);
                    ui.checkbox(&mut self.eject_after, "Eject after ripping");
                });
            }
        });

        ui.add_space(8.0);

        // ── Start / Cancel buttons ──────────────────────────────────────────
        ui.horizontal(|ui| {
            if !self.is_running() {
                let can_start = self.output_path.is_some() && self.has_valid_source();

                let btn_label = match self.action {
                    Action::Rip => "Start Rip",
                    Action::Convert => "Start Convert",
                };

                if ui
                    .add_enabled(can_start, egui::Button::new(btn_label))
                    .clicked()
                {
                    match self.action {
                        Action::Rip => self.start_rip(log),
                        Action::Convert => self.start_convert(log),
                    }
                }
            } else if ui.button("Cancel").clicked() {
                if let Some(ref p) = self.rip_progress {
                    if let Ok(mut progress) = p.lock() {
                        progress.cancel_requested = true;
                    }
                }
                if let Some(ref p) = self.convert_progress {
                    if let Ok(mut progress) = p.lock() {
                        progress.cancel_requested = true;
                    }
                }
                log.warn("Cancellation requested...");
            }
        });

        ui.separator();

        // ── Browse view ─────────────────────────────────────────────────────
        self.browse_view.show(ui);
    }

    fn has_valid_source(&self) -> bool {
        match self.source_mode {
            SourceMode::PhysicalDrive => self.selected_drive_idx.is_some(),
            SourceMode::ImageFile => self.image_file_path.is_some(),
        }
    }

    fn get_browsable_path(&self) -> Option<PathBuf> {
        match self.source_mode {
            SourceMode::ImageFile => self.image_file_path.clone(),
            SourceMode::PhysicalDrive => self
                .selected_drive_idx
                .and_then(|idx| self.drives.get(idx))
                .map(|d| d.device_path.clone()),
        }
    }

    fn detect_disc_info(&mut self, log: &mut LogPanel) {
        self.disc_info = None;
        self.disc_info_error = None;

        let path = match self.get_browsable_path() {
            Some(p) => p,
            None => return,
        };

        match DiscImageInfo::open(&path) {
            Ok(info) => {
                log.info(format!(
                    "Detected disc: {} / {} {}",
                    info.format.display_name(),
                    info.filesystem.display_name(),
                    info.volume_label.as_deref().unwrap_or(""),
                ));
                self.disc_info = Some(info);
            }
            Err(e) => {
                let msg = format!("Failed to detect disc info: {e}");
                log.warn(&msg);
                self.disc_info_error = Some(msg);
            }
        }
    }

    fn default_output_name(&self) -> String {
        let stem = if let Some(ref path) = self.image_file_path {
            path.file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("disc")
                .to_string()
        } else if let Some(ref label) = self.disc_info.as_ref().and_then(|i| i.volume_label.clone())
        {
            label.clone()
        } else {
            "disc".to_string()
        };

        let ext = match self.output_format {
            OutputFormat::Iso => "iso",
            OutputFormat::BinCue => "cue",
            OutputFormat::Chd => "chd",
        };

        format!("{stem}.{ext}")
    }

    fn start_rip(&mut self, log: &mut LogPanel) {
        let drive = match self.selected_drive_idx.and_then(|idx| self.drives.get(idx)) {
            Some(d) => d,
            None => {
                log.error("No drive selected");
                return;
            }
        };

        let output_path = match &self.output_path {
            Some(p) => p.clone(),
            None => {
                log.error("No output path selected");
                return;
            }
        };

        let format = match self.output_format {
            OutputFormat::Iso => RipFormat::Iso,
            OutputFormat::BinCue => RipFormat::BinCue,
            OutputFormat::Chd => {
                log.error("Cannot rip directly to CHD. Rip to BIN/CUE first, then convert.");
                return;
            }
        };

        let config = RipConfig {
            device_path: drive.device_path.clone(),
            output_path,
            format,
            eject_after: self.eject_after,
        };

        log.info(format!(
            "Starting rip: {} -> {}",
            config.device_path.display(),
            config.output_path.display()
        ));

        let progress_arc = Arc::new(Mutex::new(RipProgress::new()));
        self.rip_progress = Some(Arc::clone(&progress_arc));
        self.rip_running = true;

        std::thread::spawn(move || {
            if let Err(e) = rusty_backup::optical::run_rip(config, Arc::clone(&progress_arc)) {
                if let Ok(mut p) = progress_arc.lock() {
                    p.error = Some(format!("{e:#}"));
                    p.finished = true;
                }
            }
        });
    }

    fn start_convert(&mut self, log: &mut LogPanel) {
        let input_path = match &self.image_file_path {
            Some(p) => p.clone(),
            None => {
                log.error("No input file selected");
                return;
            }
        };

        let output_path = match &self.output_path {
            Some(p) => p.clone(),
            None => {
                log.error("No output path selected");
                return;
            }
        };

        let source_format = self.disc_info.as_ref().map(|i| i.format);

        log.info(format!(
            "Starting conversion: {} -> {}",
            input_path.display(),
            output_path.display()
        ));

        let progress_arc = Arc::new(Mutex::new(ConvertProgress::new()));
        self.convert_progress = Some(Arc::clone(&progress_arc));
        self.convert_running = true;

        let output_format = self.output_format;

        std::thread::spawn(move || {
            let result = run_conversion(
                &input_path,
                &output_path,
                source_format,
                output_format,
                Arc::clone(&progress_arc),
            );

            if let Err(e) = result {
                if let Ok(mut p) = progress_arc.lock() {
                    if !p.finished {
                        p.error = Some(format!("{e:#}"));
                        p.finished = true;
                    }
                }
            }
        });
    }

    fn poll_progress(&mut self, log: &mut LogPanel, progress_state: &mut ProgressState) {
        // Poll rip progress
        if let Some(progress_arc) = self.rip_progress.clone() {
            let mut finished = false;
            if let Ok(mut p) = progress_arc.lock() {
                while let Some(msg) = p.log_messages.pop_front() {
                    match msg.level {
                        BackupLogLevel::Info => log.info(msg.message),
                        BackupLogLevel::Warning => log.warn(msg.message),
                        BackupLogLevel::Error => log.error(msg.message),
                    }
                }

                progress_state.active = !p.finished;
                progress_state.operation = p.operation.clone();
                progress_state.current_bytes = p.current_bytes;
                progress_state.total_bytes = p.total_bytes;

                if p.finished {
                    if let Some(err) = &p.error {
                        log.error(format!("Rip failed: {err}"));
                    } else {
                        log.info("Rip completed successfully");
                    }
                    finished = true;
                }
            }
            if finished {
                self.rip_running = false;
                self.rip_progress = None;
            }
        }

        // Poll convert progress
        if let Some(progress_arc) = self.convert_progress.clone() {
            let mut finished = false;
            if let Ok(mut p) = progress_arc.lock() {
                while let Some(msg) = p.log_messages.pop_front() {
                    match msg.level {
                        BackupLogLevel::Info => log.info(msg.message),
                        BackupLogLevel::Warning => log.warn(msg.message),
                        BackupLogLevel::Error => log.error(msg.message),
                    }
                }

                progress_state.active = !p.finished;
                progress_state.operation = p.operation.clone();
                progress_state.current_bytes = p.current_bytes;
                progress_state.total_bytes = p.total_bytes;

                if p.finished {
                    if let Some(err) = &p.error {
                        log.error(format!("Conversion failed: {err}"));
                    } else {
                        log.info("Conversion completed successfully");
                    }
                    finished = true;
                }
            }
            if finished {
                self.convert_running = false;
                self.convert_progress = None;
            }
        }
    }
}

/// Dispatch to the correct conversion function based on source and output formats.
fn run_conversion(
    input_path: &std::path::Path,
    output_path: &std::path::Path,
    source_format: Option<DiscFormat>,
    output_format: OutputFormat,
    progress: Arc<Mutex<ConvertProgress>>,
) -> anyhow::Result<()> {
    use rusty_backup::optical::convert;

    match (source_format, output_format) {
        (Some(DiscFormat::Iso), OutputFormat::BinCue) => {
            let bin_path = output_path.with_extension("bin");
            convert::iso_to_bincue(input_path, &bin_path, output_path, progress)
        }
        (Some(DiscFormat::BinCue), OutputFormat::Iso) => {
            // input_path might be .cue or .bin — resolve to .cue
            let cue_path = if input_path
                .extension()
                .map(|e| e.eq_ignore_ascii_case("bin"))
                .unwrap_or(false)
            {
                input_path.with_extension("cue")
            } else {
                input_path.to_path_buf()
            };
            convert::bincue_to_iso(&cue_path, output_path, progress)
        }
        (Some(DiscFormat::Iso), OutputFormat::Chd)
        | (Some(DiscFormat::BinCue), OutputFormat::Chd) => {
            let chdman_input = if source_format == Some(DiscFormat::BinCue)
                && input_path
                    .extension()
                    .map(|e| e.eq_ignore_ascii_case("bin"))
                    .unwrap_or(false)
            {
                input_path.with_extension("cue")
            } else {
                input_path.to_path_buf()
            };
            convert::to_chd(&chdman_input, output_path, progress)
        }
        (Some(DiscFormat::Chd), OutputFormat::BinCue) => {
            convert::chd_to_bincue(input_path, output_path, progress)
        }
        (Some(DiscFormat::Chd), OutputFormat::Iso) => {
            convert::chd_to_iso(input_path, output_path, progress)
        }
        _ => {
            anyhow::bail!(
                "Unsupported conversion: {:?} -> {:?}",
                source_format,
                output_format
            );
        }
    }
}
