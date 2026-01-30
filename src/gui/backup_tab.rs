use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use rusty_backup::backup::{
    BackupConfig, BackupProgress, ChecksumType, CompressionType, LogLevel as BackupLogLevel,
};
use rusty_backup::device::DiskDevice;
use rusty_backup::partition::{self, PartitionInfo, PartitionTable};

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
    partition_load_error: Option<String>,
    /// Change detection for auto-load
    prev_device_idx: Option<usize>,
    prev_image_path: Option<PathBuf>,
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
            partition_load_error: None,
            prev_device_idx: None,
            prev_image_path: None,
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
                            if let Some(path) = rfd::FileDialog::new()
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
                    if let Some(path) = rfd::FileDialog::new().pick_folder() {
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
            // Compression type
            ui.horizontal(|ui| {
                ui.label("Compression:");
                let chd_label = if self.chdman_available {
                    "CHD (chdman)"
                } else {
                    "CHD (chdman not found)"
                };
                ui.add_enabled(
                    self.chdman_available,
                    egui::RadioButton::new(self.compression_type == CompressionType::Chd, chd_label),
                )
                .clicked()
                .then(|| {
                    if self.chdman_available {
                        self.compression_type = CompressionType::Chd;
                    }
                });
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

            // Split archives
            ui.horizontal(|ui| {
                ui.checkbox(&mut self.split_archives, "Split archives");
                if self.split_archives {
                    ui.label("Split size (MiB):");
                    ui.add(egui::DragValue::new(&mut self.split_size_mib).range(100..=64000));
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

        // Action buttons
        ui.horizontal(|ui| {
            if !self.backup_running {
                let has_source =
                    self.selected_device_idx.is_some() || self.image_file_path.is_some();
                let can_start = has_source
                    && self.destination_folder.is_some()
                    && !self.backup_name.is_empty();

                if ui
                    .add_enabled(can_start, egui::Button::new("Start Backup"))
                    .clicked()
                {
                    self.start_backup(devices, log);
                }
            } else {
                if ui.button("Cancel").clicked() {
                    if let Some(ref progress_arc) = self.backup_progress {
                        if let Ok(mut p) = progress_arc.lock() {
                            p.cancel_requested = true;
                        }
                    }
                    log.warn("Cancellation requested...");
                }
            }
        });
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
                self.partition_load_error =
                    Some(format!("Cannot open source: {e}"));
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
        let device = self
            .selected_device_idx
            .and_then(|idx| devices.get(idx));

        let size_bytes = device.map(|d| d.size_bytes).or_else(|| {
            std::fs::metadata(&path).ok().map(|m| m.len())
        });

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
            split_size_mib: if self.split_archives {
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
            "Starting backup: {} -> {}/{}",
            config.source_path.display(),
            config.destination_dir.display(),
            config.backup_name
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
