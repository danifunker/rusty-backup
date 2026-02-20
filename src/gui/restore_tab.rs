use std::fs::File;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use rusty_backup::backup::metadata::BackupMetadata;
use rusty_backup::clonezilla;
use rusty_backup::clonezilla::metadata::ClonezillaImage;
use rusty_backup::device::DiskDevice;
use rusty_backup::partition;
use rusty_backup::restore::{
    self, RestoreAlignment, RestoreConfig, RestorePartitionSize, RestoreProgress, RestoreSizeChoice,
};

use super::progress::{LogPanel, ProgressState};

/// Per-partition restore configuration in the GUI.
#[derive(Debug, Clone)]
struct RestorePartitionConfig {
    index: usize,
    type_name: String,
    #[allow(dead_code)]
    start_lba: u64,
    original_size: u64,
    minimum_size: u64,
    choice: RestoreSizeUiChoice,
    custom_size_mib: u32,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum RestoreSizeUiChoice {
    Original,
    Minimum,
    Custom,
    FillRemaining,
}

impl RestorePartitionConfig {
    fn effective_size(&self) -> u64 {
        match self.choice {
            RestoreSizeUiChoice::Original => self.original_size,
            RestoreSizeUiChoice::Minimum => self.minimum_size,
            RestoreSizeUiChoice::Custom => self.custom_size_mib as u64 * 1024 * 1024,
            RestoreSizeUiChoice::FillRemaining => 0, // computed dynamically
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum AlignmentChoice {
    Original,
    Modern1MB,
    Custom,
}

/// State for the Restore tab.
pub struct RestoreTab {
    // Source
    backup_folder: Option<PathBuf>,
    backup_metadata: Option<BackupMetadata>,
    clonezilla_image: Option<ClonezillaImage>,
    mbr_bytes: Option<[u8; 512]>,
    metadata_error: Option<String>,
    prev_backup_folder: Option<PathBuf>,

    // Target
    selected_device_idx: Option<usize>,
    image_file_path: Option<PathBuf>,
    target_is_device: bool,

    // Alignment
    alignment_choice: AlignmentChoice,
    custom_alignment_sectors: u32,

    // Per-partition sizing
    partition_configs: Vec<RestorePartitionConfig>,

    // Confirmation
    confirm_popup_open: bool,

    // Operation state
    restore_running: bool,
    restore_progress: Option<Arc<Mutex<RestoreProgress>>>,
}

impl Default for RestoreTab {
    fn default() -> Self {
        Self {
            backup_folder: None,
            backup_metadata: None,
            clonezilla_image: None,
            mbr_bytes: None,
            metadata_error: None,
            prev_backup_folder: None,
            selected_device_idx: None,
            image_file_path: None,
            target_is_device: true,
            alignment_choice: AlignmentChoice::Original,
            custom_alignment_sectors: 2048,
            partition_configs: Vec::new(),
            confirm_popup_open: false,
            restore_running: false,
            restore_progress: None,
        }
    }
}

impl RestoreTab {
    pub fn is_running(&self) -> bool {
        self.restore_running
    }

    pub fn get_loaded_backup(&self) -> Option<PathBuf> {
        self.backup_folder.clone()
    }

    pub fn has_backup(&self) -> bool {
        self.backup_metadata.is_some() || self.clonezilla_image.is_some()
    }

    pub fn load_backup(&mut self, path: &PathBuf) {
        if self.backup_folder.as_ref() != Some(path) {
            self.backup_folder = Some(path.clone());
            // Force reload on next show
            self.prev_backup_folder = None;
        }
    }

    pub fn clear_backup(&mut self) {
        self.backup_folder = None;
        self.backup_metadata = None;
        self.clonezilla_image = None;
        self.mbr_bytes = None;
        self.metadata_error = None;
        self.prev_backup_folder = None;
        self.partition_configs.clear();
    }

    pub fn show(
        &mut self,
        ui: &mut egui::Ui,
        devices: &[DiskDevice],
        log: &mut LogPanel,
        progress: &mut ProgressState,
    ) {
        // Poll background restore thread
        self.poll_progress(log, progress);

        ui.heading("Restore Backup");
        ui.add_space(8.0);

        let controls_enabled = !self.restore_running;

        // --- Section 1: Backup Source ---
        ui.horizontal(|ui| {
            ui.label("Backup Folder:");
            ui.add_enabled_ui(controls_enabled, |ui| {
                let label = self
                    .backup_folder
                    .as_ref()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| "No folder selected".into());
                ui.label(&label);
                if ui.button("Browse...").clicked() {
                    if let Some(path) = super::file_dialog().pick_folder() {
                        self.backup_folder = Some(path);
                    }
                }
            });
        });

        // Auto-load metadata when folder changes
        if !self.restore_running {
            let folder_changed = self.backup_folder != self.prev_backup_folder;
            if folder_changed {
                self.prev_backup_folder = self.backup_folder.clone();
                self.load_backup_metadata(log);
            }
        }

        // Show backup summary
        if let Some(meta) = &self.backup_metadata {
            ui.add_space(4.0);
            ui.horizontal_wrapped(|ui| {
                ui.label(egui::RichText::new("Backup Info:").strong());
                ui.label(format!(
                    "{} partition(s), {}, {}, {}",
                    meta.partitions.len(),
                    meta.partition_table_type,
                    meta.compression_type,
                    &meta.created[..meta.created.len().min(10)],
                ));
            });
            ui.horizontal_wrapped(|ui| {
                ui.label(egui::RichText::new("Source:").strong());
                ui.label(format!(
                    "{} ({})",
                    meta.source_device,
                    partition::format_size(meta.source_size_bytes),
                ));
            });
        } else if let Some(cz) = &self.clonezilla_image {
            ui.add_space(4.0);
            ui.horizontal_wrapped(|ui| {
                ui.label(egui::RichText::new("Backup Info:").strong());
                ui.label(format!(
                    "Clonezilla image, {} partition(s), MBR",
                    cz.partitions.len(),
                ));
            });
            ui.horizontal_wrapped(|ui| {
                ui.label(egui::RichText::new("Source:").strong());
                ui.label(format!(
                    "/dev/{} ({})",
                    cz.disk_name,
                    partition::format_size(cz.source_size_bytes),
                ));
            });
        }
        if let Some(err) = &self.metadata_error {
            ui.colored_label(egui::Color32::from_rgb(200, 150, 100), err);
        }

        ui.add_space(8.0);

        // --- Section 2: Target ---
        ui.label(egui::RichText::new("Target").strong());
        ui.separator();

        ui.add_enabled_ui(controls_enabled, |ui| {
            ui.horizontal(|ui| {
                ui.radio_value(&mut self.target_is_device, true, "Write to device");
                ui.radio_value(&mut self.target_is_device, false, "Save as image file");
            });

            if self.target_is_device {
                ui.horizontal(|ui| {
                    ui.label("Device:");
                    let current_label = self
                        .selected_device_idx
                        .and_then(|idx| devices.get(idx))
                        .map(|d| d.display_name())
                        .unwrap_or_else(|| "Select a target device...".into());

                    egui::ComboBox::from_id_salt("restore_target_device")
                        .selected_text(&current_label)
                        .width(400.0)
                        .height(400.0) // Allow more items to be visible without scrolling
                        .show_ui(ui, |ui| {
                            for (i, device) in devices.iter().enumerate() {
                                let label = format!(
                                    "{} ({}){}",
                                    device.display_name(),
                                    partition::format_size(device.size_bytes),
                                    if device.is_system { " [SYSTEM]" } else { "" },
                                );
                                ui.selectable_value(&mut self.selected_device_idx, Some(i), label);
                            }
                        });
                });

                // Show warning for system disks
                if let Some(idx) = self.selected_device_idx {
                    if let Some(device) = devices.get(idx) {
                        if device.is_system {
                            ui.colored_label(
                                egui::Color32::from_rgb(255, 100, 100),
                                "WARNING: This is a system disk! Restoring will destroy your OS.",
                            );
                        }
                    }
                }
            } else {
                ui.horizontal(|ui| {
                    ui.label("Image File:");
                    let label = self
                        .image_file_path
                        .as_ref()
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|| "No file selected".into());
                    ui.label(&label);
                    if ui.button("Save As...").clicked() {
                        if let Some(path) = super::file_dialog()
                            .add_filter("Disk Images", &["img", "raw", "bin"])
                            .save_file()
                        {
                            self.image_file_path = Some(path);
                        }
                    }
                });
            }

            // Show target size comparison
            let source_size_opt = self
                .backup_metadata
                .as_ref()
                .map(|m| m.source_size_bytes)
                .or_else(|| self.clonezilla_image.as_ref().map(|c| c.source_size_bytes));
            if let Some(source_size) = source_size_opt {
                let target_size = self.get_target_size(devices);
                if target_size > 0 {
                    let color = if target_size >= source_size {
                        egui::Color32::GRAY
                    } else {
                        egui::Color32::from_rgb(255, 180, 100)
                    };
                    ui.colored_label(
                        color,
                        format!(
                            "Target: {} | Source was: {}{}",
                            partition::format_size(target_size),
                            partition::format_size(source_size),
                            if target_size < source_size {
                                " (target is smaller!)"
                            } else {
                                ""
                            },
                        ),
                    );
                }
            }
        });

        // --- Section 3: Alignment (only when metadata loaded) ---
        if self.backup_metadata.is_some() || self.clonezilla_image.is_some() {
            ui.add_space(8.0);
            ui.label(egui::RichText::new("Alignment").strong());
            ui.separator();

            ui.add_enabled_ui(controls_enabled, |ui| {
                let orig_label = if let Some(meta) = &self.backup_metadata {
                    format!(
                        "Original ({}, LBA {})",
                        meta.alignment.detected_type, meta.alignment.first_partition_lba,
                    )
                } else if let Some(cz) = &self.clonezilla_image {
                    let first_lba = cz
                        .partitions
                        .iter()
                        .filter(|p| !p.is_extended)
                        .map(|p| p.start_lba)
                        .min()
                        .unwrap_or(63);
                    format!("Original (LBA {})", first_lba)
                } else {
                    "Original".to_string()
                };

                ui.radio_value(
                    &mut self.alignment_choice,
                    AlignmentChoice::Original,
                    orig_label,
                );
                ui.radio_value(
                    &mut self.alignment_choice,
                    AlignmentChoice::Modern1MB,
                    "1 MB boundaries (modern, LBA 2048)",
                );
                ui.horizontal(|ui| {
                    ui.radio_value(
                        &mut self.alignment_choice,
                        AlignmentChoice::Custom,
                        "Custom:",
                    );
                    if self.alignment_choice == AlignmentChoice::Custom {
                        ui.add(
                            egui::DragValue::new(&mut self.custom_alignment_sectors)
                                .range(1..=65535),
                        );
                        ui.label("sectors");
                    }
                });
            });
        }

        // --- Section 4: Partition Layout (only when metadata loaded) ---
        if !self.partition_configs.is_empty() {
            ui.add_space(8.0);
            ui.label(egui::RichText::new("Partition Sizes").strong());
            ui.separator();

            ui.add_enabled_ui(controls_enabled, |ui| {
                let last_index = self.partition_configs.last().map(|c| c.index);

                egui::Grid::new("restore_partition_sizes")
                    .striped(true)
                    .min_col_width(50.0)
                    .show(ui, |ui| {
                        ui.label(egui::RichText::new("#").strong());
                        ui.label(egui::RichText::new("Type").strong());
                        ui.label(egui::RichText::new("Original").strong());
                        ui.label(egui::RichText::new("Minimum").strong());
                        ui.label(egui::RichText::new("Size Mode").strong());
                        ui.label(egui::RichText::new("Size (MiB)").strong());
                        ui.end_row();

                        for cfg in &mut self.partition_configs {
                            ui.label(format!("{}", cfg.index));
                            ui.label(&cfg.type_name);
                            ui.label(partition::format_size(cfg.original_size));
                            ui.label(partition::format_size(cfg.minimum_size));

                            let prev_choice = cfg.choice;
                            ui.horizontal(|ui| {
                                ui.radio_value(
                                    &mut cfg.choice,
                                    RestoreSizeUiChoice::Original,
                                    "Orig",
                                );
                                if cfg.minimum_size < cfg.original_size {
                                    ui.radio_value(
                                        &mut cfg.choice,
                                        RestoreSizeUiChoice::Minimum,
                                        "Min",
                                    );
                                }
                                ui.radio_value(
                                    &mut cfg.choice,
                                    RestoreSizeUiChoice::Custom,
                                    "Custom",
                                );
                                if Some(cfg.index) == last_index {
                                    ui.radio_value(
                                        &mut cfg.choice,
                                        RestoreSizeUiChoice::FillRemaining,
                                        "Fill",
                                    );
                                }
                            });

                            if cfg.choice == RestoreSizeUiChoice::Custom
                                && prev_choice != RestoreSizeUiChoice::Custom
                            {
                                cfg.custom_size_mib =
                                    (cfg.original_size / (1024 * 1024)).max(1) as u32;
                            }

                            if cfg.choice == RestoreSizeUiChoice::Custom {
                                let min_mib = (cfg.minimum_size / (1024 * 1024)).max(1) as u32;
                                ui.add(
                                    egui::DragValue::new(&mut cfg.custom_size_mib)
                                        .range(min_mib..=u32::MAX),
                                );
                            } else if cfg.choice == RestoreSizeUiChoice::FillRemaining {
                                ui.label("(auto)");
                            } else {
                                ui.label(format!("{}", cfg.effective_size() / (1024 * 1024),));
                            }
                            ui.end_row();
                        }
                    });

                // Total size summary
                let target_size = self.get_target_size(devices);
                if target_size > 0 {
                    let total: u64 = self
                        .partition_configs
                        .iter()
                        .map(|c| {
                            if c.choice == RestoreSizeUiChoice::FillRemaining {
                                0
                            } else {
                                c.effective_size()
                            }
                        })
                        .sum();
                    let color = if total <= target_size {
                        egui::Color32::GRAY
                    } else {
                        egui::Color32::from_rgb(255, 100, 100)
                    };
                    ui.colored_label(
                        color,
                        format!(
                            "Total: {} / {} available",
                            partition::format_size(total),
                            partition::format_size(target_size),
                        ),
                    );
                }
            });
        }

        ui.add_space(16.0);

        // --- Section 5: Action Buttons ---
        ui.horizontal(|ui| {
            if !self.restore_running {
                let can_start = (self.backup_metadata.is_some() || self.clonezilla_image.is_some())
                    && self.has_target(devices);
                if ui
                    .add_enabled(can_start, egui::Button::new("Restore"))
                    .clicked()
                {
                    self.confirm_popup_open = true;
                }
            } else {
                if ui.button("Cancel").clicked() {
                    if let Some(ref progress_arc) = self.restore_progress {
                        if let Ok(mut p) = progress_arc.lock() {
                            p.cancel_requested = true;
                        }
                    }
                    log.warn("Cancellation requested...");
                }
            }
        });

        // --- Confirmation popup ---
        if self.confirm_popup_open {
            self.show_confirmation_popup(ui, devices, log);
        }
    }

    fn load_backup_metadata(&mut self, log: &mut LogPanel) {
        self.backup_metadata = None;
        self.clonezilla_image = None;
        self.mbr_bytes = None;
        self.metadata_error = None;
        self.partition_configs.clear();

        let folder = match &self.backup_folder {
            Some(f) => f.clone(),
            None => return,
        };

        let metadata_path = folder.join("metadata.json");
        if metadata_path.exists() {
            self.load_native_backup_metadata(&folder, &metadata_path, log);
        } else if clonezilla::metadata::is_clonezilla_image(&folder) {
            self.load_clonezilla_backup_metadata(&folder, log);
        } else {
            self.metadata_error =
                Some("No backup found (metadata.json or Clonezilla image)".to_string());
        }
    }

    fn load_native_backup_metadata(
        &mut self,
        folder: &std::path::Path,
        metadata_path: &std::path::Path,
        log: &mut LogPanel,
    ) {
        let file = match File::open(metadata_path) {
            Ok(f) => f,
            Err(e) => {
                self.metadata_error = Some(format!("Cannot open metadata.json: {e}"));
                return;
            }
        };

        let metadata: BackupMetadata = match serde_json::from_reader(file) {
            Ok(m) => m,
            Err(e) => {
                self.metadata_error = Some(format!("Invalid metadata.json: {e}"));
                return;
            }
        };

        // Read MBR if available
        let mbr_path = folder.join("mbr.bin");
        if mbr_path.exists() {
            if let Ok(data) = std::fs::read(&mbr_path) {
                let mut buf = [0u8; 512];
                let copy_len = data.len().min(512);
                buf[..copy_len].copy_from_slice(&data[..copy_len]);
                self.mbr_bytes = Some(buf);
            }
        }

        // Build partition configs
        for pm in &metadata.partitions {
            // Prefer explicit minimum_size_bytes (computed at backup time via
            // last_data_byte). Fall back to imaged_size_bytes for older backups
            // that used packed compaction (where imaged < original), and finally
            // original_size_bytes when no sizing hint is available.
            let minimum = pm.minimum_size_bytes.filter(|&s| s > 0).unwrap_or_else(|| {
                if pm.imaged_size_bytes > 0 {
                    pm.imaged_size_bytes
                } else {
                    pm.original_size_bytes
                }
            });

            self.partition_configs.push(RestorePartitionConfig {
                index: pm.index,
                type_name: pm.type_name.clone(),
                start_lba: pm.start_lba,
                original_size: pm.original_size_bytes,
                minimum_size: minimum,
                choice: RestoreSizeUiChoice::Original,
                custom_size_mib: (pm.original_size_bytes / (1024 * 1024)) as u32,
            });
        }

        log.info(format!(
            "Loaded backup: {} partition(s) from {}",
            metadata.partitions.len(),
            metadata.source_device,
        ));

        self.backup_metadata = Some(metadata);
    }

    fn load_clonezilla_backup_metadata(&mut self, folder: &std::path::Path, log: &mut LogPanel) {
        let cz_image = match clonezilla::metadata::load(folder) {
            Ok(img) => img,
            Err(e) => {
                self.metadata_error = Some(format!("Failed to load Clonezilla image: {e}"));
                return;
            }
        };

        self.mbr_bytes = Some(cz_image.mbr_bytes);

        // Build partition configs from Clonezilla partitions (skip extended containers)
        for cz_part in &cz_image.partitions {
            if cz_part.is_extended {
                continue;
            }

            // Try to get minimum size from partclone header
            let minimum_size = if !cz_part.partclone_files.is_empty() {
                match clonezilla::partclone::read_partclone_header(&cz_part.partclone_files) {
                    Ok(header) => header.used_size(),
                    Err(_) => cz_part.size_bytes(),
                }
            } else {
                cz_part.size_bytes()
            };

            self.partition_configs.push(RestorePartitionConfig {
                index: cz_part.index,
                type_name: cz_part.type_name(),
                start_lba: cz_part.start_lba,
                original_size: cz_part.size_bytes(),
                minimum_size,
                choice: RestoreSizeUiChoice::Original,
                custom_size_mib: (cz_part.size_bytes() / (1024 * 1024)) as u32,
            });
        }

        log.info(format!(
            "Loaded Clonezilla image: {} partition(s) from /dev/{}",
            cz_image
                .partitions
                .iter()
                .filter(|p| !p.is_extended)
                .count(),
            cz_image.disk_name,
        ));

        self.clonezilla_image = Some(cz_image);
    }

    fn get_target_size(&self, devices: &[DiskDevice]) -> u64 {
        if self.target_is_device {
            self.selected_device_idx
                .and_then(|idx| devices.get(idx))
                .map(|d| d.size_bytes)
                .unwrap_or(0)
        } else {
            // For image files, use the source disk size as default target
            if let Some(meta) = &self.backup_metadata {
                meta.source_size_bytes
            } else if let Some(cz) = &self.clonezilla_image {
                cz.source_size_bytes
            } else {
                0
            }
        }
    }

    fn has_target(&self, devices: &[DiskDevice]) -> bool {
        if self.target_is_device {
            self.selected_device_idx
                .and_then(|idx| devices.get(idx))
                .is_some()
        } else {
            self.image_file_path.is_some()
        }
    }

    fn show_confirmation_popup(
        &mut self,
        ui: &mut egui::Ui,
        devices: &[DiskDevice],
        log: &mut LogPanel,
    ) {
        let mut close = false;

        egui::Window::new("Confirm Restore")
            .collapsible(false)
            .resizable(false)
            .default_width(400.0)
            .show(ui.ctx(), |ui| {
                let target_name = if self.target_is_device {
                    self.selected_device_idx
                        .and_then(|idx| devices.get(idx))
                        .map(|d| d.display_name())
                        .unwrap_or_else(|| "Unknown".into())
                } else {
                    self.image_file_path
                        .as_ref()
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|| "Unknown".into())
                };

                ui.colored_label(
                    egui::Color32::from_rgb(255, 100, 100),
                    format!(
                        "All data on {} will be permanently overwritten!",
                        target_name
                    ),
                );
                ui.add_space(8.0);

                // Show partition summary
                for cfg in &self.partition_configs {
                    let size_str = if cfg.choice == RestoreSizeUiChoice::FillRemaining {
                        "Fill remaining".to_string()
                    } else {
                        partition::format_size(cfg.effective_size())
                    };
                    ui.label(format!(
                        "  Partition {}: {} -> {}",
                        cfg.index, cfg.type_name, size_str,
                    ));
                }

                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    if ui
                        .button(
                            egui::RichText::new("I understand, proceed")
                                .color(egui::Color32::from_rgb(255, 100, 100)),
                        )
                        .clicked()
                    {
                        close = true;
                        self.start_restore(devices, log);
                    }
                    if ui.button("Cancel").clicked() {
                        close = true;
                    }
                });
            });

        if close {
            self.confirm_popup_open = false;
        }
    }

    fn start_restore(&mut self, devices: &[DiskDevice], log: &mut LogPanel) {
        let backup_folder = match &self.backup_folder {
            Some(f) => f.clone(),
            None => {
                log.error("No backup folder selected");
                return;
            }
        };

        let (target_path, target_is_device, target_size) = if self.target_is_device {
            let device = match self.selected_device_idx.and_then(|idx| devices.get(idx)) {
                Some(d) => d,
                None => {
                    log.error("No target device selected");
                    return;
                }
            };
            (device.path.clone(), true, device.size_bytes)
        } else {
            let path = match &self.image_file_path {
                Some(p) => p.clone(),
                None => {
                    log.error("No image file selected");
                    return;
                }
            };
            let size = self
                .backup_metadata
                .as_ref()
                .map(|m| m.source_size_bytes)
                .or_else(|| self.clonezilla_image.as_ref().map(|c| c.source_size_bytes))
                .unwrap_or(0);
            (path, false, size)
        };

        let alignment = match self.alignment_choice {
            AlignmentChoice::Original => RestoreAlignment::Original,
            AlignmentChoice::Modern1MB => RestoreAlignment::Modern1MB,
            AlignmentChoice::Custom => {
                RestoreAlignment::Custom(self.custom_alignment_sectors as u64)
            }
        };

        let partition_sizes: Vec<RestorePartitionSize> = self
            .partition_configs
            .iter()
            .map(|cfg| RestorePartitionSize {
                index: cfg.index,
                size_choice: match cfg.choice {
                    RestoreSizeUiChoice::Original => RestoreSizeChoice::Original,
                    RestoreSizeUiChoice::Minimum => {
                        // Pass the concrete minimum size so layout calculation
                        // doesn't need to re-derive it (especially for Clonezilla
                        // where the partclone header has the real used size).
                        RestoreSizeChoice::Custom(cfg.minimum_size)
                    }
                    RestoreSizeUiChoice::Custom => {
                        RestoreSizeChoice::Custom(cfg.custom_size_mib as u64 * 1024 * 1024)
                    }
                    RestoreSizeUiChoice::FillRemaining => RestoreSizeChoice::FillRemaining,
                },
            })
            .collect();

        let config = RestoreConfig {
            backup_folder,
            target_path: target_path.clone(),
            target_is_device,
            target_size,
            alignment,
            partition_sizes,
            write_zeros_to_unused: false, // Default: no zero-fill (FAT handles it)
        };

        let progress_arc = Arc::new(Mutex::new(RestoreProgress::new()));
        self.restore_progress = Some(Arc::clone(&progress_arc));
        self.restore_running = true;

        log.info(format!("Starting restore to {}", target_path.display(),));

        std::thread::spawn(move || {
            if let Err(e) = restore::run_restore(config, Arc::clone(&progress_arc)) {
                if let Ok(mut p) = progress_arc.lock() {
                    p.error = Some(format!("{e:#}"));
                    p.finished = true;
                }
            }
        });
    }

    fn poll_progress(&mut self, log: &mut LogPanel, progress_state: &mut ProgressState) {
        let progress_arc = match &self.restore_progress {
            Some(p) => Arc::clone(p),
            None => return,
        };

        let Ok(mut p) = progress_arc.lock() else {
            return;
        };

        // Drain log messages
        while let Some(msg) = p.log_messages.pop_front() {
            match msg.level {
                rusty_backup::backup::LogLevel::Info => log.info(msg.message),
                rusty_backup::backup::LogLevel::Warning => log.warn(msg.message),
                rusty_backup::backup::LogLevel::Error => log.error(msg.message),
            }
        }

        // Update progress bar
        progress_state.active = !p.finished;
        progress_state.operation = p.operation.clone();
        progress_state.current_bytes = p.current_bytes;
        progress_state.total_bytes = p.total_bytes;
        progress_state.full_size_bytes = 0;

        if p.finished {
            if let Some(err) = &p.error {
                log.error(format!("Restore failed: {err}"));
            } else {
                log.info("Restore completed successfully.");
            }
            drop(p);
            self.restore_running = false;
            self.restore_progress = None;
        }
    }
}
