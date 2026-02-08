use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use rusty_backup::backup::{
    BackupConfig, BackupProgress, ChecksumType, CompressionType, LogLevel as BackupLogLevel,
};
use rusty_backup::device::DiskDevice;
use rusty_backup::fs;
use rusty_backup::partition::PartitionSizeOverride;
use rusty_backup::partition::{self, PartitionInfo, PartitionTable};
use rusty_backup::rbformats::vhd::export_whole_disk_vhd;

use super::progress::{LogPanel, ProgressState};

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
    chdman_available: bool,
    backup_running: bool,
    backup_progress: Option<Arc<Mutex<BackupProgress>>>,
    /// Auto-loaded partition info for the selected source
    source_partitions: Vec<PartitionInfo>,
    /// Minimum sizes per partition (indexed by partition index), from filesystem analysis
    partition_min_sizes: std::collections::HashMap<usize, u64>,
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
    choice: VhdSizeChoice,
    custom_size_mib: u32,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum VhdSizeChoice {
    Original,
    Minimum,
    Custom,
}

impl VhdPartitionConfig {
    fn effective_size(&self) -> u64 {
        match self.choice {
            VhdSizeChoice::Original => self.original_size,
            VhdSizeChoice::Minimum => self.minimum_size,
            VhdSizeChoice::Custom => self.custom_size_mib as u64 * 1024 * 1024,
        }
    }
}

/// Status of a VHD whole-disk export running on a background thread.
struct VhdExportStatus {
    finished: bool,
    error: Option<String>,
    current_bytes: u64,
    total_bytes: u64,
    cancel_requested: bool,
    log_messages: Vec<String>,
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
            chdman_available: false,
            backup_running: false,
            backup_progress: None,
            source_partitions: Vec::new(),
            partition_min_sizes: std::collections::HashMap::new(),
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
        if !available && self.compression_type == CompressionType::Chd {
            self.compression_type = CompressionType::Zstd;
        }
    }

    pub fn show(
        &mut self,
        ui: &mut egui::Ui,
        devices: &[DiskDevice],
        log: &mut LogPanel,
        progress: &mut ProgressState,
    ) {
        // Poll background backup thread
        self.poll_progress(log, progress);

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
                    devices
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
                        for (i, device) in devices.iter().enumerate() {
                            if ui
                                .selectable_value(
                                    &mut self.selected_device_idx,
                                    Some(i),
                                    device.display_name(),
                                )
                                .clicked()
                            {
                                self.image_file_path = None;
                                self.update_backup_name(devices);
                            }
                        }
                        ui.separator();
                        if ui
                            .selectable_label(self.image_file_path.is_some(), "Open Image File...")
                            .clicked()
                        {
                            if let Some(path) = super::file_dialog()
                                .add_filter("Disk Images", &["img", "raw", "bin", "iso", "dd"])
                                .add_filter("All Files", &["*"])
                                .pick_file()
                            {
                                self.selected_device_idx = None;
                                self.image_file_path = Some(path);
                                self.update_backup_name(devices);
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
                self.load_partition_preview(devices, log);
            }
        }

        // Show partition preview if loaded
        if !self.source_partitions.is_empty() {
            ui.add_space(4.0);
            ui.horizontal_wrapped(|ui| {
                ui.label(egui::RichText::new("Partitions:").strong());
                for part in &self.source_partitions {
                    if !part.is_extended_container {
                        let label = format!(
                            "#{}: {} ({})",
                            part.index,
                            part.type_name,
                            partition::format_size(part.size_bytes),
                        );
                        ui.label(&label);
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
                if ui
                    .radio_value(&mut self.compression_type, CompressionType::Vhd, "VHD")
                    .clicked()
                {}
                if ui
                    .radio_value(&mut self.compression_type, CompressionType::Zstd, "zstd")
                    .clicked()
                {}
                if ui
                    .radio_value(&mut self.compression_type, CompressionType::None, "None")
                    .clicked()
                {}
            });

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
        self.poll_vhd_export(log, progress);

        // Action buttons
        let vhd_exporting = self.vhd_export_status.is_some();
        ui.horizontal(|ui| {
            if !self.backup_running && !vhd_exporting {
                let has_source =
                    self.selected_device_idx.is_some() || self.image_file_path.is_some();
                let can_start =
                    has_source && self.destination_folder.is_some() && !self.backup_name.is_empty();

                if ui
                    .add_enabled(can_start, egui::Button::new("Start Backup"))
                    .clicked()
                {
                    if self.compression_type == CompressionType::Vhd {
                        // Scan partitions if not already loaded
                        if self.source_partitions.is_empty() {
                            self.scan_source_partitions(devices, log);
                        }
                        self.init_vhd_configs();
                        self.vhd_popup_open = true;
                    } else {
                        self.start_backup(devices, log);
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
                    log.warn("Cancellation requested...");
                }
            }
        });

        // VHD backup popup
        if self.vhd_popup_open {
            self.show_vhd_popup(ui, devices, log);
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

            self.vhd_partition_configs.push(VhdPartitionConfig {
                index: part.index,
                type_name: part.type_name.clone(),
                start_lba: part.start_lba,
                original_size: part.size_bytes,
                minimum_size: min_size,
                choice: VhdSizeChoice::Original,
                custom_size_mib: (part.size_bytes / (1024 * 1024)) as u32,
            });
        }
    }

    fn show_vhd_popup(&mut self, ui: &mut egui::Ui, devices: &[DiskDevice], log: &mut LogPanel) {
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

                            for cfg in &mut self.vhd_partition_configs {
                                ui.label(format!("{}", cfg.index));
                                ui.label(&cfg.type_name);

                                let prev_choice = cfg.choice;
                                ui.horizontal(|ui| {
                                    ui.radio_value(
                                        &mut cfg.choice,
                                        VhdSizeChoice::Original,
                                        "Original",
                                    );
                                    if cfg.minimum_size < cfg.original_size {
                                        ui.radio_value(
                                            &mut cfg.choice,
                                            VhdSizeChoice::Minimum,
                                            "Minimum",
                                        );
                                    }
                                    ui.radio_value(
                                        &mut cfg.choice,
                                        VhdSizeChoice::Custom,
                                        "Custom",
                                    );
                                });

                                if cfg.choice == VhdSizeChoice::Custom
                                    && prev_choice != VhdSizeChoice::Custom
                                {
                                    cfg.custom_size_mib =
                                        (cfg.minimum_size / (1024 * 1024)).max(1) as u32;
                                }

                                if cfg.choice == VhdSizeChoice::Custom {
                                    let min_mib = (cfg.minimum_size / (1024 * 1024)).max(1) as u32;
                                    let max_mib = (cfg.original_size / (1024 * 1024))
                                        .max(min_mib as u64)
                                        as u32;
                                    ui.add(
                                        egui::DragValue::new(&mut cfg.custom_size_mib)
                                            .range(min_mib..=max_mib),
                                    );
                                } else {
                                    ui.label(format!("{}", cfg.effective_size() / (1024 * 1024),));
                                }
                                ui.end_row();
                            }
                        });
                    ui.add_space(4.0);
                }

                ui.add_space(8.0);

                ui.horizontal(|ui| {
                    if ui.button("Start VHD Backup").clicked() {
                        self.vhd_popup_open = false;
                        if self.vhd_whole_disk {
                            self.start_vhd_whole_disk(devices, log);
                        } else {
                            // Per-partition uses the normal backup flow
                            self.start_backup(devices, log);
                        }
                    }
                    if ui.button("Cancel").clicked() {
                        self.vhd_popup_open = false;
                    }
                });
            });
    }

    fn start_vhd_whole_disk(&mut self, devices: &[DiskDevice], log: &mut LogPanel) {
        let source_path = match self.source_path(devices) {
            Some(p) => p,
            None => {
                log.error("No source selected");
                return;
            }
        };

        let dest_dir = match &self.destination_folder {
            Some(d) => d.clone(),
            None => {
                log.error("No destination folder selected");
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

        log.info(format!(
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

    fn poll_vhd_export(&mut self, log: &mut LogPanel, progress_state: &mut ProgressState) {
        let status_arc = match &self.vhd_export_status {
            Some(s) => Arc::clone(s),
            None => return,
        };

        let Ok(mut status) = status_arc.lock() else {
            return;
        };

        // Drain log messages
        for msg in status.log_messages.drain(..) {
            log.info(msg);
        }

        // Update progress bar
        progress_state.active = !status.finished;
        progress_state.operation = "Exporting VHD...".to_string();
        progress_state.current_bytes = status.current_bytes;
        progress_state.total_bytes = status.total_bytes;
        progress_state.full_size_bytes = 0;

        if status.finished {
            if let Some(err) = &status.error {
                log.error(format!("VHD backup failed: {err}"));
            } else {
                log.info("VHD backup completed successfully.");
            }
            drop(status);
            self.vhd_export_status = None;
            self.backup_running = false;
        }
    }

    /// Scan source partition table and compute minimum partition sizes.
    /// For devices, this uses OS-level elevation (may prompt for credentials).
    fn scan_source_partitions(&mut self, devices: &[DiskDevice], log: &mut LogPanel) {
        self.source_partitions.clear();
        self.partition_min_sizes.clear();
        self.partition_load_error = None;

        let path = match self.source_path(devices) {
            Some(p) => p,
            None => return,
        };

        log.info(format!("Scanning partitions on {}...", path.display()));

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
                log.error(msg);
                return;
            }
        };

        let mut reader = BufReader::new(file);
        match PartitionTable::detect(&mut reader) {
            Ok(table) => {
                self.source_partitions = table.partitions();
                log.info(format!(
                    "Found {} partition(s) ({})",
                    self.source_partitions.len(),
                    table.type_name(),
                ));

                // Compute minimum partition sizes using filesystem analysis
                for part in &self.source_partitions {
                    if part.is_extended_container {
                        continue;
                    }
                    let offset = part.start_lba * 512;
                    if let Ok(clone) = reader.get_ref().try_clone() {
                        if let Some(min_size) = fs::effective_partition_size(
                            BufReader::new(clone),
                            offset,
                            part.partition_type_byte,
                            part.partition_type_string.as_deref(),
                        ) {
                            let clamped = min_size.min(part.size_bytes);
                            self.partition_min_sizes.insert(part.index, clamped);
                            log.info(format!(
                                "Partition {}: minimum size {} (original {})",
                                part.index,
                                partition::format_size(clamped),
                                partition::format_size(part.size_bytes),
                            ));
                        }
                    }
                }
            }
            Err(e) => {
                self.partition_load_error = Some(format!("Could not read partition table: {e}"));
                log.error(format!("Partition scan failed: {e}"));
            }
        }
    }

    fn load_partition_preview(&mut self, devices: &[DiskDevice], log: &mut LogPanel) {
        self.source_partitions.clear();
        self.partition_load_error = None;

        let path = match self.source_path(devices) {
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
                let mut reader = BufReader::new(file);
                match PartitionTable::detect(&mut reader) {
                    Ok(table) => {
                        self.source_partitions = table.partitions();
                        log.info(format!(
                            "Source has {} partition(s) ({})",
                            self.source_partitions.len(),
                            table.type_name(),
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

    fn source_path(&self, devices: &[DiskDevice]) -> Option<PathBuf> {
        if let Some(path) = &self.image_file_path {
            Some(path.clone())
        } else if let Some(idx) = self.selected_device_idx {
            devices.get(idx).map(|d| d.path.clone())
        } else {
            None
        }
    }

    fn update_backup_name(&mut self, devices: &[DiskDevice]) {
        let path = match self.source_path(devices) {
            Some(p) => p,
            None => return,
        };

        // Try to get device size and first volume label
        let device = self.selected_device_idx.and_then(|idx| devices.get(idx));

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

    fn start_backup(&mut self, devices: &[DiskDevice], log: &mut LogPanel) {
        let source_path = match self.source_path(devices) {
            Some(p) => p,
            None => {
                log.error("No source selected");
                return;
            }
        };

        let destination_dir = match &self.destination_folder {
            Some(d) => d.clone(),
            None => {
                log.error("No destination folder selected");
                return;
            }
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
        };

        let progress_arc = Arc::new(Mutex::new(BackupProgress::new()));
        self.backup_progress = Some(Arc::clone(&progress_arc));
        self.backup_running = true;

        log.info(format!(
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

    fn poll_progress(&mut self, log: &mut LogPanel, progress_state: &mut ProgressState) {
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
                BackupLogLevel::Info => log.info(msg.message),
                BackupLogLevel::Warning => log.warn(msg.message),
                BackupLogLevel::Error => log.error(msg.message),
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
                log.error(format!("Backup failed: {err}"));
            }
            self.backup_running = false;
            self.backup_progress = None;
        }
    }
}
