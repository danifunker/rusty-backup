use std::collections::HashSet;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use rusty_backup::backup::{
    BackupConfig, BackupProgress, ChecksumType, CompressionType, LogLevel as BackupLogLevel,
};
use rusty_backup::fs;
use rusty_backup::rbformats::chd_options::{
    codec_label, parse_codec_string, ChdOptions, ChdProfile,
};
use rusty_backup::update::UpdateConfig;

use super::chd_options_ui::{ChdOptionsControl, ChdOptionsMode};
use rusty_backup::model::size_mode::SizeMode;
use rusty_backup::model::status::VhdExportStatus;
use rusty_backup::partition::PartitionSizeOverride;
use rusty_backup::partition::{self, PartitionInfo, PartitionTable};
use rusty_backup::rbformats::vhd::export_whole_disk_vhd;

use super::context::TabContext;
use super::progress::ProgressState;

/// State for the Backup tab.
pub struct BackupTab {
    selected_device_idx: Option<usize>,
    image_file_path: Option<PathBuf>,
    destination_folder: Option<PathBuf>,
    backup_name: String,
    sector_by_sector: bool,
    resize_partitions: bool,
    split_archives: bool,
    split_size_mib: u32,
    checksum_type: ChecksumType,
    compression_type: CompressionType,
    chd_hd_control: ChdOptionsControl,
    chd_dvd_control: ChdOptionsControl,
    chdman_available: bool,
    backup_running: bool,
    backup_progress: Option<Arc<Mutex<BackupProgress>>>,
    /// Auto-loaded partition info for the selected source
    source_partitions: Vec<PartitionInfo>,
    /// Which partitions are selected for backup (by partition index)
    selected_partitions: HashSet<usize>,
    /// Minimum sizes per partition (indexed by partition index), from filesystem analysis
    partition_min_sizes: std::collections::HashMap<usize, u64>,
    /// Partitions whose minimum size requires an expensive volume walk; the
    /// VHD-export popup surfaces a "Calc min" button per index.
    deferred_min_sizes: std::collections::HashMap<usize, &'static str>,
    /// Currently-running per-partition min-size worker threads.
    pending_min_size_calcs: std::collections::HashMap<
        usize,
        Arc<Mutex<rusty_backup::model::min_size_runner::MinSizeStatus>>,
    >,
    /// Open file handle for the source (kept alive so min-size workers can clone it).
    source_file: Option<Arc<File>>,
    partition_load_error: Option<String>,
    /// Change detection for auto-load
    prev_device_idx: Option<usize>,
    prev_image_path: Option<PathBuf>,
    /// VHD backup popup state
    vhd_popup_open: bool,
    vhd_whole_disk: bool,
    vhd_partition_configs: Vec<VhdPartitionConfig>,
    /// VHD whole-disk export status (runs independently from run_backup)
    vhd_export_status: Option<Arc<Mutex<VhdExportStatus>>>,
}

/// Per-partition size config for VHD backup popup.
#[derive(Debug, Clone)]
struct VhdPartitionConfig {
    index: usize,
    type_name: String,
    start_lba: u64,
    original_size: u64,
    minimum_size: u64,
    choice: SizeMode,
    custom_size_mib: u32,
    /// Filesystem name when the minimum size requires an expensive walk
    /// (deferred at popup-open time). Cleared when the user clicks "Calc min"
    /// and the worker finishes successfully.
    deferred_fs: Option<&'static str>,
}

impl VhdPartitionConfig {
    fn effective_size(&self) -> u64 {
        self.choice
            .effective_size(self.original_size, self.minimum_size, self.custom_size_mib)
    }
}

impl Default for BackupTab {
    fn default() -> Self {
        Self {
            selected_device_idx: None,
            image_file_path: None,
            destination_folder: None,
            backup_name: String::new(),
            sector_by_sector: false,
            resize_partitions: true,
            split_archives: true,
            split_size_mib: 4000,
            checksum_type: ChecksumType::Sha256,
            compression_type: CompressionType::Zstd,
            chd_hd_control: ChdOptionsControl::new(ChdProfile::Hd),
            chd_dvd_control: ChdOptionsControl::new(ChdProfile::Dvd),
            chdman_available: false,
            backup_running: false,
            backup_progress: None,
            source_partitions: Vec::new(),
            selected_partitions: HashSet::new(),
            partition_min_sizes: std::collections::HashMap::new(),
            deferred_min_sizes: std::collections::HashMap::new(),
            pending_min_size_calcs: std::collections::HashMap::new(),
            source_file: None,
            partition_load_error: None,
            prev_device_idx: None,
            prev_image_path: None,
            vhd_popup_open: false,
            vhd_whole_disk: true,
            vhd_partition_configs: Vec::new(),
            vhd_export_status: None,
        }
    }
}

impl BackupTab {
    /// Set chdman availability (detected at app startup).
    pub fn set_chdman_available(&mut self, available: bool) {
        self.chdman_available = available;
        // Don't default to CHD if not available
        if !available
            && (self.compression_type == CompressionType::Chd
                || self.compression_type == CompressionType::Dvd)
        {
            self.compression_type = CompressionType::Zstd;
        }
        // Restore last-used codec/hunk choices from disk so users don't have
        // to retune every launch. Failures are silent — the in-struct chdman
        // defaults remain valid.
        // Restore last-used custom codec/hunk choices so users don't re-tune
        // every launch. Defaults/MiSTer presets are pure functions so we
        // don't persist mode — restored values land in the Custom slot, ready
        // to apply as soon as the user flips into Custom mode.
        let cfg = UpdateConfig::load();
        if let Some(spec) = cfg.last_chd_codecs.as_deref() {
            if let Ok(codecs) = parse_codec_string(spec) {
                self.chd_hd_control.custom.codecs = codecs;
                self.chd_dvd_control.custom.codecs = codecs;
            }
        }
        if let Some(hs) = cfg.last_chd_hunk_size {
            if hs % 512 == 0 {
                self.chd_hd_control.custom.hunk_size = hs;
            }
            if hs % 2048 == 0 {
                self.chd_dvd_control.custom.hunk_size = hs;
            }
        }
    }

    pub fn show(&mut self, ui: &mut egui::Ui, ctx: &mut TabContext, progress: &mut ProgressState) {
        // Poll background backup thread
        self.poll_progress(ctx, progress);

        // Poll any pending per-partition minimum-size calculations
        self.poll_min_size_calcs(ctx);

        ui.heading("Backup Disk");
        ui.add_space(8.0);

        let controls_enabled = !self.backup_running;

        // Source device / image
        ui.horizontal(|ui| {
            ui.label("Source:");
            ui.add_enabled_ui(controls_enabled, |ui| {
                let current_label = if let Some(path) = &self.image_file_path {
                    format!("Image: {}", path.display())
                } else if let Some(idx) = self.selected_device_idx {
                    ctx.devices
                        .get(idx)
                        .map(|d| d.display_name())
                        .unwrap_or_else(|| "Unknown".into())
                } else {
                    "Select a device or image...".into()
                };

                egui::ComboBox::from_id_salt("backup_source")
                    .selected_text(&current_label)
                    .width(400.0)
                    .height(400.0) // Allow more items to be visible without scrolling
                    .show_ui(ui, |ui| {
                        for (i, device) in ctx.devices.iter().enumerate() {
                            if ui
                                .selectable_value(
                                    &mut self.selected_device_idx,
                                    Some(i),
                                    device.display_name(),
                                )
                                .clicked()
                            {
                                self.image_file_path = None;
                                self.update_backup_name(ctx);
                            }
                        }
                        ui.separator();
                        if ui
                            .selectable_label(self.image_file_path.is_some(), "Open Image File...")
                            .clicked()
                        {
                            if let Some(path) = super::file_dialog()
                                .add_filter(
                                    "Disk Images",
                                    &[
                                        "img", "raw", "bin", "iso", "dd", "vhd", "hda", "hdv",
                                        "2mg", "dmg", "po", "do", "dsk", "dc42", "woz", "chd",
                                    ],
                                )
                                .add_filter("All Files", &["*"])
                                .pick_file()
                            {
                                self.selected_device_idx = None;
                                self.image_file_path = Some(path);
                                self.update_backup_name(ctx);
                            }
                        }
                    });
            });
        });

        // Auto-load partition info when source changes
        if !self.backup_running {
            let source_changed = self.selected_device_idx != self.prev_device_idx
                || self.image_file_path != self.prev_image_path;
            if source_changed {
                self.prev_device_idx = self.selected_device_idx;
                self.prev_image_path = self.image_file_path.clone();
                self.load_partition_preview(ctx);
            }
        }

        // Show partition selection checkboxes
        if !self.source_partitions.is_empty() {
            ui.add_space(4.0);
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new("Partitions:").strong());
                ui.add_enabled_ui(controls_enabled, |ui| {
                    if ui.small_button("All").clicked() {
                        for part in &self.source_partitions {
                            if !part.is_extended_container {
                                self.selected_partitions.insert(part.index);
                            }
                        }
                    }
                    if ui.small_button("None").clicked() {
                        self.selected_partitions.clear();
                    }
                });
            });
            ui.add_enabled_ui(controls_enabled, |ui| {
                for part in &self.source_partitions {
                    if part.is_extended_container {
                        continue;
                    }
                    let mut selected = self.selected_partitions.contains(&part.index);
                    let label = format!(
                        "#{}: {} ({})",
                        part.index,
                        part.type_name,
                        partition::format_size(part.size_bytes),
                    );
                    if ui.checkbox(&mut selected, label).changed() {
                        if selected {
                            self.selected_partitions.insert(part.index);
                        } else {
                            self.selected_partitions.remove(&part.index);
                        }
                    }
                }
            });
            ui.add_space(4.0);
        }
        if let Some(err) = &self.partition_load_error {
            ui.colored_label(egui::Color32::from_rgb(200, 150, 100), err);
        }

        // Destination folder
        ui.horizontal(|ui| {
            ui.label("Destination:");
            ui.add_enabled_ui(controls_enabled, |ui| {
                let label = self
                    .destination_folder
                    .as_ref()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| "No folder selected".into());
                ui.label(&label);
                if ui.button("Browse...").clicked() {
                    if let Some(path) = super::file_dialog().pick_folder() {
                        self.destination_folder = Some(path);
                    }
                }
            });
        });

        // Backup name
        ui.horizontal(|ui| {
            ui.label("Backup Name:");
            ui.add_enabled_ui(controls_enabled, |ui| {
                ui.text_edit_singleline(&mut self.backup_name);
            });
        });

        ui.add_space(12.0);
        ui.label(egui::RichText::new("Options").strong());
        ui.separator();

        ui.add_enabled_ui(controls_enabled, |ui| {
            // Output format
            ui.horizontal(|ui| {
                ui.label("Output Format:");
                let chd_label = if self.chdman_available {
                    "CHD (chdman)"
                } else {
                    "CHD (chdman not found)"
                };
                ui.add_enabled(
                    self.chdman_available,
                    egui::RadioButton::new(
                        self.compression_type == CompressionType::Chd,
                        chd_label,
                    ),
                )
                .clicked()
                .then(|| {
                    if self.chdman_available {
                        self.compression_type = CompressionType::Chd;
                    }
                });
                ui.add_enabled(
                    self.chdman_available,
                    egui::RadioButton::new(
                        self.compression_type == CompressionType::Dvd,
                        "DVD CHD",
                    ),
                )
                .clicked()
                .then(|| {
                    if self.chdman_available {
                        self.compression_type = CompressionType::Dvd;
                    }
                });
                ui.radio_value(&mut self.compression_type, CompressionType::Vhd, "VHD")
                    .clicked();
                ui.radio_value(&mut self.compression_type, CompressionType::Zstd, "zstd")
                    .clicked();
                if ui
                    .radio_value(&mut self.compression_type, CompressionType::None, "None")
                    .clicked()
                {}
            });

            // CHD codec / hunk-size knobs (only visible when a CHD profile is selected)
            if matches!(
                self.compression_type,
                CompressionType::Chd | CompressionType::Dvd
            ) {
                let (profile, control) = if self.compression_type == CompressionType::Dvd {
                    (ChdProfile::Dvd, &mut self.chd_dvd_control)
                } else {
                    (ChdProfile::Hd, &mut self.chd_hd_control)
                };
                super::chd_options_ui::show(ui, "backup_chd", profile, control);
            }

            // Sector-by-sector option
            ui.checkbox(
                &mut self.sector_by_sector,
                "Sector-by-sector copy (includes all blank space)",
            );

            // Resize option
            ui.checkbox(
                &mut self.resize_partitions,
                "Resize partitions to minimum size",
            );

            // Split archives (disabled for VHD output)
            let split_enabled = self.compression_type != CompressionType::Vhd;
            ui.horizontal(|ui| {
                ui.add_enabled_ui(split_enabled, |ui| {
                    ui.checkbox(&mut self.split_archives, "Split archives");
                    if self.split_archives && split_enabled {
                        ui.label("Split size (MiB):");
                        ui.add(egui::DragValue::new(&mut self.split_size_mib).range(100..=64000));
                    }
                });
                if !split_enabled {
                    ui.label("(not available for VHD)");
                }
            });

            // Checksum type
            ui.horizontal(|ui| {
                ui.label("Checksum:");
                ui.radio_value(&mut self.checksum_type, ChecksumType::Sha256, "SHA-256");
                ui.radio_value(&mut self.checksum_type, ChecksumType::Crc32, "CRC32");
            });
        });

        ui.add_space(16.0);

        // Poll VHD export thread
        self.poll_vhd_export(ctx, progress);

        // Action buttons
        let vhd_exporting = self.vhd_export_status.is_some();
        ui.horizontal(|ui| {
            if !self.backup_running && !vhd_exporting {
                let has_source =
                    self.selected_device_idx.is_some() || self.image_file_path.is_some();
                let has_partitions =
                    self.source_partitions.is_empty() || !self.selected_partitions.is_empty();
                let chd_ok = match self.compression_type {
                    CompressionType::Chd => self.chd_hd_control.is_valid(ChdProfile::Hd),
                    CompressionType::Dvd => self.chd_dvd_control.is_valid(ChdProfile::Dvd),
                    _ => true,
                };
                let can_start = has_source
                    && self.destination_folder.is_some()
                    && !self.backup_name.is_empty()
                    && has_partitions
                    && chd_ok;

                if ui
                    .add_enabled(can_start, egui::Button::new("Start Backup"))
                    .clicked()
                {
                    if self.compression_type == CompressionType::Vhd {
                        // Scan partitions if not already loaded
                        if self.source_partitions.is_empty() {
                            self.scan_source_partitions(ctx);
                        }
                        self.init_vhd_configs();
                        self.vhd_popup_open = true;
                    } else {
                        self.start_backup(ctx);
                    }
                }
            } else {
                if ui.button("Cancel").clicked() {
                    if let Some(ref progress_arc) = self.backup_progress {
                        if let Ok(mut p) = progress_arc.lock() {
                            p.cancel_requested = true;
                        }
                    }
                    if let Some(ref status) = self.vhd_export_status {
                        if let Ok(mut s) = status.lock() {
                            s.cancel_requested = true;
                        }
                    }
                    ctx.log.warn("Cancellation requested...");
                }
            }
        });

        // VHD backup popup
        if self.vhd_popup_open {
            self.show_vhd_popup(ui, ctx);
        }
    }

    fn init_vhd_configs(&mut self) {
        self.vhd_partition_configs.clear();
        for part in &self.source_partitions {
            if part.is_extended_container {
                continue;
            }
            let min_size = self
                .partition_min_sizes
                .get(&part.index)
                .copied()
                .unwrap_or(part.size_bytes);
            let deferred_fs = self.deferred_min_sizes.get(&part.index).copied();

            self.vhd_partition_configs.push(VhdPartitionConfig {
                index: part.index,
                type_name: part.type_name.clone(),
                start_lba: part.start_lba,
                original_size: part.size_bytes,
                minimum_size: min_size,
                choice: SizeMode::Original,
                custom_size_mib: (part.size_bytes / (1024 * 1024)) as u32,
                deferred_fs,
            });
        }
    }

    fn show_vhd_popup(&mut self, ui: &mut egui::Ui, ctx: &mut TabContext) {
        egui::Window::new("VHD Backup Options")
            .collapsible(false)
            .resizable(true)
            .default_width(500.0)
            .show(ui.ctx(), |ui| {
                ui.label("Configure VHD output.");
                ui.add_space(4.0);

                ui.radio_value(
                    &mut self.vhd_whole_disk,
                    true,
                    "Whole Disk (single .vhd file)",
                );
                ui.radio_value(
                    &mut self.vhd_whole_disk,
                    false,
                    "Per Partition (one .vhd per partition)",
                );

                ui.add_space(8.0);

                // Per-partition sizing
                if !self.vhd_partition_configs.is_empty() {
                    ui.label(egui::RichText::new("Partition Sizes:").strong());
                    egui::Grid::new("vhd_backup_partition_sizes")
                        .striped(true)
                        .min_col_width(50.0)
                        .show(ui, |ui| {
                            ui.label(egui::RichText::new("#").strong());
                            ui.label(egui::RichText::new("Type").strong());
                            ui.label(egui::RichText::new("Size Mode").strong());
                            ui.label(egui::RichText::new("Size (MiB)").strong());
                            ui.end_row();

                            let mut min_calc_request: Option<usize> = None;
                            for cfg in &mut self.vhd_partition_configs {
                                ui.label(format!("{}", cfg.index));
                                ui.label(&cfg.type_name);

                                let pending_phase = self
                                    .pending_min_size_calcs
                                    .get(&cfg.index)
                                    .and_then(|s| s.lock().ok().map(|st| st.phase.clone()));
                                let deferred = if let Some(phase) = &pending_phase {
                                    Some(super::size_mode_row::DeferredMin::Pending {
                                        phase: phase.as_str(),
                                    })
                                } else {
                                    cfg.deferred_fs.map(|fs_name| {
                                        super::size_mode_row::DeferredMin::Available { fs_name }
                                    })
                                };
                                let action = super::size_mode_row::size_mode_row(
                                    ui,
                                    &mut cfg.choice,
                                    &mut cfg.custom_size_mib,
                                    cfg.original_size,
                                    cfg.minimum_size,
                                    super::size_mode_row::SizeModeRowOptions {
                                        max_size: Some(cfg.original_size),
                                        deferred,
                                        ..Default::default()
                                    },
                                );
                                if action
                                    == super::size_mode_row::SizeModeRowAction::CalcMinRequested
                                {
                                    min_calc_request = Some(cfg.index);
                                }
                                ui.end_row();
                            }
                            if let Some(part_index) = min_calc_request {
                                self.start_min_size_calc(part_index, ctx);
                            }
                        });
                    ui.add_space(4.0);
                }

                ui.add_space(8.0);

                ui.horizontal(|ui| {
                    if ui.button("Start VHD Backup").clicked() {
                        self.vhd_popup_open = false;
                        if self.vhd_whole_disk {
                            self.start_vhd_whole_disk(ctx);
                        } else {
                            // Per-partition uses the normal backup flow
                            self.start_backup(ctx);
                        }
                    }
                    if ui.button("Cancel").clicked() {
                        self.vhd_popup_open = false;
                    }
                });
            });
    }

    fn start_vhd_whole_disk(&mut self, ctx: &mut TabContext) {
        let source_path = match self.source_path(ctx) {
            Some(p) => p,
            None => {
                ctx.log.error("No source selected");
                return;
            }
        };

        let dest_dir = match &self.destination_folder {
            Some(d) => d.clone(),
            None => {
                ctx.log.error("No destination folder selected");
                return;
            }
        };

        // Build destination path
        let dest_path = dest_dir.join(format!("{}.vhd", self.backup_name));

        // Build partition size overrides
        let overrides: Vec<PartitionSizeOverride> = self
            .vhd_partition_configs
            .iter()
            .map(|cfg| {
                PartitionSizeOverride::size_only(
                    cfg.index,
                    cfg.start_lba,
                    cfg.original_size,
                    cfg.effective_size(),
                )
            })
            .collect();

        let total_bytes: u64 = overrides.iter().map(|o| o.export_size).sum();

        let status = Arc::new(Mutex::new(VhdExportStatus {
            finished: false,
            error: None,
            current_bytes: 0,
            total_bytes,
            cancel_requested: false,
            log_messages: Vec::new(),
        }));
        self.vhd_export_status = Some(Arc::clone(&status));
        self.backup_running = true;

        ctx.log.info(format!(
            "Starting whole-disk VHD backup: {} -> {}",
            source_path.display(),
            dest_path.display()
        ));

        std::thread::spawn(move || {
            let status2 = Arc::clone(&status);
            let status3 = Arc::clone(&status);
            let result = export_whole_disk_vhd(
                &source_path,
                None,
                None,
                &overrides,
                &dest_path,
                move |bytes| {
                    if let Ok(mut s) = status2.lock() {
                        s.current_bytes = bytes;
                    }
                },
                move || status3.lock().map(|s| s.cancel_requested).unwrap_or(false),
                |msg| {
                    if let Ok(mut s) = status.lock() {
                        s.log_messages.push(msg.to_string());
                    }
                },
            );
            if let Ok(mut s) = status.lock() {
                s.finished = true;
                if let Err(e) = result {
                    s.error = Some(format!("{e:#}"));
                }
            }
        });
    }

    fn poll_vhd_export(&mut self, ctx: &mut TabContext, progress_state: &mut ProgressState) {
        let status_arc = match &self.vhd_export_status {
            Some(s) => Arc::clone(s),
            None => return,
        };

        let Ok(mut status) = status_arc.lock() else {
            return;
        };

        // Drain log messages
        for msg in status.log_messages.drain(..) {
            ctx.log.info(msg);
        }

        // Update progress bar
        progress_state.active = !status.finished;
        progress_state.operation = "Exporting VHD...".to_string();
        progress_state.current_bytes = status.current_bytes;
        progress_state.total_bytes = status.total_bytes;
        progress_state.full_size_bytes = 0;

        if status.finished {
            if let Some(err) = &status.error {
                ctx.log.error(format!("VHD backup failed: {err}"));
            } else {
                ctx.log.info("VHD backup completed successfully.");
            }
            drop(status);
            self.vhd_export_status = None;
            self.backup_running = false;
        }
    }

    /// Spawn a worker thread to compute the minimum size for a deferred partition.
    fn start_min_size_calc(&mut self, part_index: usize, ctx: &mut TabContext) {
        if self.pending_min_size_calcs.contains_key(&part_index) {
            return;
        }
        let Some(part) = self
            .source_partitions
            .iter()
            .find(|p| p.index == part_index)
            .cloned()
        else {
            return;
        };
        let path = self.source_path(ctx);
        let is_device = path
            .as_ref()
            .map(|p| {
                let s = p.to_string_lossy();
                s.starts_with("/dev/") || s.starts_with("\\\\.\\")
            })
            .unwrap_or(false);

        let source = match path.as_deref() {
            Some(p) if rusty_backup::model::source_reader::is_chd_path(p) => {
                rusty_backup::model::min_size_runner::MinSizeSource::Chd(p.to_path_buf())
            }
            _ => {
                let Some(file_arc) = self.source_file.clone() else {
                    ctx.log.error(format!(
                        "Cannot calculate minimum size for partition {part_index}: no open source handle",
                    ));
                    return;
                };
                rusty_backup::model::min_size_runner::MinSizeSource::File {
                    file: file_arc,
                    use_sector_aligned: is_device,
                }
            }
        };

        let fs_name = self
            .deferred_min_sizes
            .get(&part_index)
            .copied()
            .unwrap_or("unknown");
        ctx.log.info(format!(
            "Calculating minimum size for partition {part_index} ({fs_name})...",
        ));

        let req = rusty_backup::model::min_size_runner::MinSizeRequest {
            source,
            partition_offset: part.start_lba * 512,
            partition_type: part.partition_type_byte,
            partition_type_string: part.partition_type_string.clone(),
            partition_size: part.size_bytes,
            partition_index: part_index,
        };
        let status = rusty_backup::model::min_size_runner::spawn(req);
        self.pending_min_size_calcs.insert(part_index, status);
    }

    /// Poll all pending min-size workers; on completion, move results into
    /// `partition_min_sizes` and refresh any matching `vhd_partition_configs`.
    fn poll_min_size_calcs(&mut self, ctx: &mut TabContext) {
        let finished_indices: Vec<usize> = self
            .pending_min_size_calcs
            .iter()
            .filter_map(|(idx, status)| status.lock().ok().filter(|s| s.finished).map(|_| *idx))
            .collect();
        for idx in finished_indices {
            if let Some(status_arc) = self.pending_min_size_calcs.remove(&idx) {
                if let Ok(s) = status_arc.lock() {
                    if let Some(err) = &s.error {
                        ctx.log.error(format!(
                            "Minimum size calculation failed for partition {idx}: {err}",
                        ));
                    } else if let Some(min) = s.result {
                        let part_size = self
                            .source_partitions
                            .iter()
                            .find(|p| p.index == idx)
                            .map(|p| p.size_bytes)
                            .unwrap_or(min);
                        let clamped = min.min(part_size);
                        self.partition_min_sizes.insert(idx, clamped);
                        ctx.log.info(format!(
                            "Partition {idx}: minimum size {} (computed)",
                            partition::format_size(clamped),
                        ));
                        for cfg in &mut self.vhd_partition_configs {
                            if cfg.index == idx {
                                cfg.minimum_size = clamped;
                                cfg.deferred_fs = None;
                            }
                        }
                    } else {
                        ctx.log.info(format!(
                            "Partition {idx}: filesystem could not be opened for minimum-size calculation",
                        ));
                    }
                }
                self.deferred_min_sizes.remove(&idx);
            }
        }
    }

    /// Scan source partition table and compute minimum partition sizes.
    /// For devices, this uses OS-level elevation (may prompt for credentials).
    fn scan_source_partitions(&mut self, ctx: &mut TabContext) {
        self.source_partitions.clear();
        self.selected_partitions.clear();
        self.partition_min_sizes.clear();
        self.deferred_min_sizes.clear();
        self.pending_min_size_calcs.clear();
        self.source_file = None;
        self.partition_load_error = None;

        let path = match self.source_path(ctx) {
            Some(p) => p,
            None => return,
        };

        ctx.log
            .info(format!("Scanning partitions on {}...", path.display()));

        let path_str = path.to_string_lossy();
        let is_device = path_str.starts_with("/dev/") || path_str.starts_with("\\\\.\\");

        // Open file (with elevation for devices)
        let open_result: Result<File, String> = if is_device {
            match rusty_backup::os::open_source_for_reading(&path) {
                Ok(elevated) => {
                    let (file, _guard) = elevated.into_parts();
                    // Note: _guard is dropped here but the file handle remains valid
                    Ok(file)
                }
                Err(e) => Err(format!("Cannot access device: {e}")),
            }
        } else {
            File::open(&path).map_err(|e| format!("Cannot open source: {e}"))
        };

        let file = match open_result {
            Ok(f) => f,
            Err(msg) => {
                self.partition_load_error = Some(msg.clone());
                ctx.log.error(msg);
                return;
            }
        };

        let mut reader = BufReader::new(file);
        match PartitionTable::detect(&mut reader) {
            Ok(mut table) => {
                // Fix up superfloppy size: seek(End(0)) returns 0 for macOS devices
                if let PartitionTable::None { size_bytes, .. } = &mut table {
                    if *size_bytes == 0 {
                        if let Ok(real_size) =
                            rusty_backup::os::get_file_size(reader.get_ref(), &path)
                        {
                            *size_bytes = real_size;
                        }
                    }
                }
                self.source_partitions = table.partitions();
                self.select_all_partitions();
                let table_desc = if matches!(table, PartitionTable::None { .. }) {
                    "No partition table (superfloppy)".to_string()
                } else {
                    table.type_name().to_string()
                };
                ctx.log.info(format!(
                    "Found {} partition(s) ({})",
                    self.source_partitions.len(),
                    table_desc,
                ));

                // Compute minimum partition sizes using filesystem analysis.
                // Cheap filesystems (FAT/NTFS/exFAT) compute eagerly here; expensive
                // ones (HFS/HFS+/ext/btrfs/ProDOS) defer until the user clicks
                // "Calc min" in the per-partition resize popup.
                for part in &self.source_partitions {
                    if part.is_extended_container {
                        continue;
                    }
                    let offset = part.start_lba * 512;
                    let Ok(clone) = reader.get_ref().try_clone() else {
                        continue;
                    };
                    let result = fs::partition_minimum_size(
                        BufReader::new(clone),
                        offset,
                        part.partition_type_byte,
                        part.partition_type_string.as_deref(),
                        part.size_bytes,
                        false,
                        &|_| {},
                    );
                    match result {
                        fs::MinimumResult::Computed(Some(min_size)) => {
                            let clamped = min_size.min(part.size_bytes);
                            self.partition_min_sizes.insert(part.index, clamped);
                            ctx.log.info(format!(
                                "Partition {}: minimum size {} (original {})",
                                part.index,
                                partition::format_size(clamped),
                                partition::format_size(part.size_bytes),
                            ));
                        }
                        fs::MinimumResult::Computed(None) => {}
                        fs::MinimumResult::Deferred { fs_name } => {
                            ctx.log.info(format!(
                                "Partition {}: need to calculate minimum size due to filesystem {fs_name} (click \"Calc min\" to compute)",
                                part.index,
                            ));
                            self.deferred_min_sizes.insert(part.index, fs_name);
                        }
                    }
                }
                // Stash the open source file so worker threads can clone it later.
                if let Ok(f) = reader.get_ref().try_clone() {
                    self.source_file = Some(Arc::new(f));
                }
            }
            Err(e) => {
                self.partition_load_error = Some(format!("Could not read partition table: {e}"));
                ctx.log.error(format!("Partition scan failed: {e}"));
            }
        }
    }

    fn load_partition_preview(&mut self, ctx: &mut TabContext) {
        self.source_partitions.clear();
        self.selected_partitions.clear();
        self.partition_load_error = None;

        let path = match self.source_path(ctx) {
            Some(p) => p,
            None => return,
        };

        // Only auto-load for image files (not raw devices which need elevation)
        let path_str = path.to_string_lossy();
        if path_str.starts_with("/dev/") || path_str.starts_with("\\\\.\\") {
            // Device path -- skip auto-load to avoid permission prompts
            return;
        }

        match File::open(&path) {
            Ok(file) => {
                // Detect image format (VHD, 2MG, DMG, or raw)
                let detect_result =
                    match rusty_backup::rbformats::detect_image_format_with_path(file, Some(&path))
                    {
                        Ok(format) => {
                            let desc = format.description();
                            ctx.log.info(format!("Detected format: {}", desc));
                            match File::open(&path) {
                                Ok(f2) => {
                                    match rusty_backup::rbformats::wrap_image_reader(f2, format) {
                                        Ok((mut reader, _size)) => {
                                            PartitionTable::detect(&mut reader)
                                        }
                                        Err(e) => {
                                            // Fallback to raw
                                            ctx.log.warn(format!("Format wrap failed: {e}"));
                                            match File::open(&path) {
                                                Ok(f3) => {
                                                    let mut r = BufReader::new(f3);
                                                    PartitionTable::detect(&mut r)
                                                }
                                                Err(e) => Err(e.into()),
                                            }
                                        }
                                    }
                                }
                                Err(e) => Err(e.into()),
                            }
                        }
                        Err(_) => {
                            // Fallback to raw read
                            match File::open(&path) {
                                Ok(f2) => {
                                    let mut r = BufReader::new(f2);
                                    PartitionTable::detect(&mut r)
                                }
                                Err(e) => Err(e.into()),
                            }
                        }
                    };

                match detect_result {
                    Ok(table) => {
                        self.source_partitions = table.partitions();
                        self.select_all_partitions();
                        let table_desc = if matches!(table, PartitionTable::None { .. }) {
                            "No partition table (superfloppy)".to_string()
                        } else {
                            table.type_name().to_string()
                        };
                        ctx.log.info(format!(
                            "Source has {} partition(s) ({})",
                            self.source_partitions.len(),
                            table_desc,
                        ));
                    }
                    Err(e) => {
                        self.partition_load_error =
                            Some(format!("Could not read partition table: {e}"));
                    }
                }
            }
            Err(e) => {
                self.partition_load_error = Some(format!("Cannot open source: {e}"));
            }
        }
    }

    /// Select all non-extended partitions for backup (called after partition list loads).
    fn select_all_partitions(&mut self) {
        self.selected_partitions.clear();
        for part in &self.source_partitions {
            if !part.is_extended_container {
                self.selected_partitions.insert(part.index);
            }
        }
    }

    fn source_path(&self, ctx: &TabContext) -> Option<PathBuf> {
        if let Some(path) = &self.image_file_path {
            Some(path.clone())
        } else if let Some(idx) = self.selected_device_idx {
            ctx.devices.get(idx).map(|d| d.path.clone())
        } else {
            None
        }
    }

    fn update_backup_name(&mut self, ctx: &TabContext) {
        let path = match self.source_path(ctx) {
            Some(p) => p,
            None => return,
        };

        // Try to get device size and first volume label
        let device = self
            .selected_device_idx
            .and_then(|idx| ctx.devices.get(idx));

        let size_bytes = device
            .map(|d| d.size_bytes)
            .or_else(|| std::fs::metadata(&path).ok().map(|m| m.len()));

        let volume_label = device
            .and_then(|d| d.partitions.first())
            .map(|p| p.name.as_str())
            .filter(|n| !n.is_empty());

        self.backup_name =
            rusty_backup::backup::format::generate_backup_name(&path, size_bytes, volume_label);
    }

    /// Returns the ChdOptions matching the active compression type, or
    /// `None` when the selected output isn't a CHD profile.
    fn chd_options_for_compression(&self) -> Option<ChdOptions> {
        match self.compression_type {
            CompressionType::Chd => Some(self.chd_hd_control.effective(ChdProfile::Hd)),
            CompressionType::Dvd => Some(self.chd_dvd_control.effective(ChdProfile::Dvd)),
            _ => None,
        }
    }

    /// Persist only when the user is in Custom mode — Default/MiSTer are pure
    /// functions of the profile and don't need round-tripping.
    fn maybe_persist_chd_options(&self) {
        let custom = match self.compression_type {
            CompressionType::Chd if self.chd_hd_control.mode == ChdOptionsMode::Custom => {
                Some(&self.chd_hd_control.custom)
            }
            CompressionType::Dvd if self.chd_dvd_control.mode == ChdOptionsMode::Custom => {
                Some(&self.chd_dvd_control.custom)
            }
            _ => None,
        };
        if let Some(opts) = custom {
            self.persist_chd_options(opts);
        }
    }

    /// Persist the active CHD codec/hunk-size choice to `config.json`.
    /// Called at backup start so future launches restore the user's pick.
    fn persist_chd_options(&self, opts: &ChdOptions) {
        let mut cfg = UpdateConfig::load();
        cfg.last_chd_codecs = Some(
            opts.codecs
                .iter()
                .map(|c| codec_label(*c))
                .collect::<Vec<_>>()
                .join(","),
        );
        cfg.last_chd_hunk_size = Some(opts.hunk_size);
        let _ = cfg.save();
    }

    fn start_backup(&mut self, ctx: &mut TabContext) {
        let source_path = match self.source_path(ctx) {
            Some(p) => p,
            None => {
                ctx.log.error("No source selected");
                return;
            }
        };

        let destination_dir = match &self.destination_folder {
            Some(d) => d.clone(),
            None => {
                ctx.log.error("No destination folder selected");
                return;
            }
        };

        let partition_filter = if self.selected_partitions.len()
            < self
                .source_partitions
                .iter()
                .filter(|p| !p.is_extended_container)
                .count()
        {
            Some(self.selected_partitions.iter().copied().collect())
        } else {
            None // All selected — no filter needed
        };

        let config = BackupConfig {
            source_path,
            destination_dir,
            backup_name: self.backup_name.clone(),
            compression: self.compression_type,
            checksum: self.checksum_type,
            split_size_mib: if self.split_archives && self.compression_type != CompressionType::Vhd
            {
                Some(self.split_size_mib)
            } else {
                None
            },
            sector_by_sector: self.sector_by_sector,
            partition_filter,
            chd_options: self.chd_options_for_compression(),
        };

        self.maybe_persist_chd_options();

        let progress_arc = Arc::new(Mutex::new(BackupProgress::new()));
        self.backup_progress = Some(Arc::clone(&progress_arc));
        self.backup_running = true;

        ctx.log.info(format!(
            "Starting backup: {} -> {}",
            config.source_path.display(),
            config.destination_dir.join(&config.backup_name).display()
        ));

        std::thread::spawn(move || {
            if let Err(e) = rusty_backup::backup::run_backup(config, Arc::clone(&progress_arc)) {
                if let Ok(mut p) = progress_arc.lock() {
                    p.error = Some(format!("{e:#}"));
                    p.finished = true;
                }
            }
        });
    }

    fn poll_progress(&mut self, ctx: &mut TabContext, progress_state: &mut ProgressState) {
        let progress_arc = match &self.backup_progress {
            Some(p) => Arc::clone(p),
            None => return,
        };

        let Ok(mut p) = progress_arc.lock() else {
            return;
        };

        // Drain log messages
        while let Some(msg) = p.log_messages.pop_front() {
            match msg.level {
                BackupLogLevel::Info => ctx.log.info(msg.message),
                BackupLogLevel::Warning => ctx.log.warn(msg.message),
                BackupLogLevel::Error => ctx.log.error(msg.message),
            }
        }

        // Update progress bar
        progress_state.active = !p.finished;
        progress_state.operation = p.operation.clone();
        progress_state.current_bytes = p.current_bytes;
        progress_state.total_bytes = p.total_bytes;
        progress_state.full_size_bytes = p.full_size_bytes;

        if p.finished {
            if let Some(err) = &p.error {
                ctx.log.error(format!("Backup failed: {err}"));
            }
            self.backup_running = false;
            self.backup_progress = None;
        }
    }
}
