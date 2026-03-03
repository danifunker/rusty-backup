use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use rusty_backup::backup::metadata::BackupMetadata;
use rusty_backup::clonezilla;
use rusty_backup::clonezilla::metadata::ClonezillaImage;
use rusty_backup::device::DiskDevice;
use rusty_backup::partition::{self, PartitionInfo, PartitionTable};
use rusty_backup::restore::single::{
    NewDiskConfig, NewTableType, SinglePartitionRestoreConfig, SinglePartitionSource,
};
use rusty_backup::restore::{
    self, RestoreAlignment, RestoreConfig, RestorePartitionSize, RestoreProgress, RestoreSizeChoice,
};

use super::progress::{LogPanel, ProgressState};

/// Which restore mode is active.
#[derive(Debug, Clone, Copy, PartialEq)]
enum RestoreMode {
    /// Existing behavior: restore entire disk from backup.
    FullDisk,
    /// Restore a single partition to an existing disk.
    SinglePartition,
    /// Restore a single partition to a new/empty disk with a fresh partition table.
    NewDisk,
}

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
    // Mode
    restore_mode: RestoreMode,

    // Source (Full Disk + Single Partition)
    backup_folder: Option<PathBuf>,
    backup_metadata: Option<BackupMetadata>,
    clonezilla_image: Option<ClonezillaImage>,
    mbr_bytes: Option<[u8; 512]>,
    metadata_error: Option<String>,
    prev_backup_folder: Option<PathBuf>,

    // Target (Full Disk)
    selected_device_idx: Option<usize>,
    image_file_path: Option<PathBuf>,
    target_is_device: bool,

    // Alignment (Full Disk)
    alignment_choice: AlignmentChoice,
    custom_alignment_sectors: u32,

    // Per-partition sizing (Full Disk)
    partition_configs: Vec<RestorePartitionConfig>,

    // Confirmation
    confirm_popup_open: bool,

    // Operation state
    restore_running: bool,
    restore_progress: Option<Arc<Mutex<RestoreProgress>>>,

    // --- Single Partition mode state ---
    /// Source image file for single-partition restore (alternative to backup folder)
    sp_image_file: Option<PathBuf>,
    /// Which partition index to restore (from backup)
    sp_source_partition_idx: Option<usize>,
    /// Target device for single-partition restore
    sp_target_device_idx: Option<usize>,
    /// Scanned target partition table
    sp_target_partitions: Vec<PartitionInfo>,
    /// Which target partition to overwrite
    sp_target_partition_idx: Option<usize>,
    /// Error from scanning target
    sp_scan_error: Option<String>,

    // --- New Disk mode state ---
    /// Table type for new disk
    nd_table_type: NewTableType,
    /// Alignment choice for new disk
    nd_alignment_choice: AlignmentChoice,
    nd_custom_alignment: u32,
    /// Bootable flag (MBR only)
    nd_bootable: bool,
    /// Target device or image file
    nd_target_device_idx: Option<usize>,
    nd_image_file_path: Option<PathBuf>,
    nd_target_is_device: bool,
}

impl Default for RestoreTab {
    fn default() -> Self {
        Self {
            restore_mode: RestoreMode::FullDisk,
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
            sp_image_file: None,
            sp_source_partition_idx: None,
            sp_target_device_idx: None,
            sp_target_partitions: Vec::new(),
            sp_target_partition_idx: None,
            sp_scan_error: None,
            nd_table_type: NewTableType::Mbr,
            nd_alignment_choice: AlignmentChoice::Modern1MB,
            nd_custom_alignment: 2048,
            nd_bootable: false,
            nd_target_device_idx: None,
            nd_image_file_path: None,
            nd_target_is_device: true,
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

        // --- Restore Mode Selector ---
        ui.add_enabled_ui(controls_enabled, |ui| {
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new("Mode:").strong());
                ui.radio_value(
                    &mut self.restore_mode,
                    RestoreMode::FullDisk,
                    "Full Disk Restore",
                );
                ui.radio_value(
                    &mut self.restore_mode,
                    RestoreMode::SinglePartition,
                    "Restore Single Partition",
                );
                ui.radio_value(
                    &mut self.restore_mode,
                    RestoreMode::NewDisk,
                    "Restore to New Disk",
                );
            });
        });
        ui.add_space(4.0);

        match self.restore_mode {
            RestoreMode::FullDisk => self.show_full_disk_mode(ui, devices, log, controls_enabled),
            RestoreMode::SinglePartition => {
                self.show_single_partition_mode(ui, devices, log, controls_enabled)
            }
            RestoreMode::NewDisk => self.show_new_disk_mode(ui, devices, log, controls_enabled),
        }

        // --- Confirmation popup ---
        if self.confirm_popup_open {
            self.show_confirmation_popup(ui, devices, log);
        }
    }

    fn show_full_disk_mode(
        &mut self,
        ui: &mut egui::Ui,
        devices: &[DiskDevice],
        log: &mut LogPanel,
        controls_enabled: bool,
    ) {
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
        self.show_action_buttons(ui, devices, log);
    }

    fn show_action_buttons(
        &mut self,
        ui: &mut egui::Ui,
        devices: &[DiskDevice],
        log: &mut LogPanel,
    ) {
        ui.add_space(16.0);
        ui.horizontal(|ui| {
            if !self.restore_running {
                let can_start = match self.restore_mode {
                    RestoreMode::FullDisk => {
                        (self.backup_metadata.is_some() || self.clonezilla_image.is_some())
                            && self.has_target(devices)
                    }
                    RestoreMode::SinglePartition => {
                        let has_source = (self.backup_metadata.is_some()
                            && self.sp_source_partition_idx.is_some())
                            || self.sp_image_file.is_some();
                        let has_target = self.sp_target_device_idx.is_some()
                            && self.sp_target_partition_idx.is_some();
                        has_source && has_target
                    }
                    RestoreMode::NewDisk => {
                        let has_source = (self.backup_metadata.is_some()
                            && self.sp_source_partition_idx.is_some())
                            || self.sp_image_file.is_some();
                        let has_target = if self.nd_target_is_device {
                            self.nd_target_device_idx.is_some()
                        } else {
                            self.nd_image_file_path.is_some()
                        };
                        has_source && has_target
                    }
                };
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
                            p.operation =
                                "Cancelling — waiting for current write to complete…".to_string();
                        }
                    }
                    log.warn(
                        "Cancellation requested — waiting for current disk write to complete...",
                    );
                }
            }
        });
    }

    fn show_single_partition_mode(
        &mut self,
        ui: &mut egui::Ui,
        devices: &[DiskDevice],
        log: &mut LogPanel,
        controls_enabled: bool,
    ) {
        // --- Source Section ---
        ui.label(egui::RichText::new("Source").strong());
        ui.separator();

        ui.add_enabled_ui(controls_enabled, |ui| {
            // Backup folder source
            ui.horizontal(|ui| {
                ui.label("Backup Folder:");
                let label = self
                    .backup_folder
                    .as_ref()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| "No folder selected".into());
                ui.label(&label);
                if ui.button("Browse...").clicked() {
                    if let Some(path) = super::file_dialog().pick_folder() {
                        self.backup_folder = Some(path);
                        self.sp_image_file = None;
                    }
                }
            });

            // Or image file source
            ui.horizontal(|ui| {
                ui.label("Or Image File:");
                let label = self
                    .sp_image_file
                    .as_ref()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| "No file selected".into());
                ui.label(&label);
                if ui.button("Browse...").clicked() {
                    if let Some(path) = super::file_dialog()
                        .add_filter(
                            "Disk Images",
                            &["img", "raw", "bin", "vhd", "2mg", "iso", "dd"],
                        )
                        .add_filter("All Files", &["*"])
                        .pick_file()
                    {
                        self.sp_image_file = Some(path);
                        self.backup_folder = None;
                        self.backup_metadata = None;
                        self.sp_source_partition_idx = None;
                        self.prev_backup_folder = None;
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

        // Show source partition selector for backup sources
        if let Some(meta) = &self.backup_metadata {
            ui.add_space(4.0);
            ui.add_enabled_ui(controls_enabled, |ui| {
                ui.horizontal(|ui| {
                    ui.label("Source Partition:");
                    let partitions = &meta.partitions;
                    let current_label = self
                        .sp_source_partition_idx
                        .and_then(|idx| partitions.iter().find(|p| p.index == idx))
                        .map(|p| {
                            format!(
                                "#{}: {} ({})",
                                p.index,
                                p.type_name,
                                partition::format_size(p.original_size_bytes),
                            )
                        })
                        .unwrap_or_else(|| "Select a partition...".into());

                    egui::ComboBox::from_id_salt("sp_source_partition")
                        .selected_text(&current_label)
                        .width(350.0)
                        .show_ui(ui, |ui| {
                            for pm in partitions {
                                let label = format!(
                                    "#{}: {} ({})",
                                    pm.index,
                                    pm.type_name,
                                    partition::format_size(pm.original_size_bytes),
                                );
                                ui.selectable_value(
                                    &mut self.sp_source_partition_idx,
                                    Some(pm.index),
                                    label,
                                );
                            }
                        });
                });
            });
        } else if self.sp_image_file.is_some() {
            ui.add_space(4.0);
            ui.label("Source: entire image file");
        }

        if let Some(err) = &self.metadata_error {
            ui.colored_label(egui::Color32::from_rgb(200, 150, 100), err);
        }

        ui.add_space(8.0);

        // --- Target Section ---
        ui.label(egui::RichText::new("Target").strong());
        ui.separator();

        ui.add_enabled_ui(controls_enabled, |ui| {
            ui.horizontal(|ui| {
                ui.label("Target Device:");
                let current_label = self
                    .sp_target_device_idx
                    .and_then(|idx| devices.get(idx))
                    .map(|d| d.display_name())
                    .unwrap_or_else(|| "Select a target device...".into());

                egui::ComboBox::from_id_salt("sp_target_device")
                    .selected_text(&current_label)
                    .width(400.0)
                    .height(400.0)
                    .show_ui(ui, |ui| {
                        for (i, device) in devices.iter().enumerate() {
                            let label = format!(
                                "{} ({}){}",
                                device.display_name(),
                                partition::format_size(device.size_bytes),
                                if device.is_system { " [SYSTEM]" } else { "" },
                            );
                            if ui
                                .selectable_value(&mut self.sp_target_device_idx, Some(i), label)
                                .clicked()
                            {
                                self.sp_target_partitions.clear();
                                self.sp_target_partition_idx = None;
                                self.sp_scan_error = None;
                            }
                        }
                    });

                if self.sp_target_device_idx.is_some() {
                    if ui.button("Scan Target").clicked() {
                        self.scan_target_partitions(devices, log);
                    }
                }
            });

            // System disk warning
            if let Some(idx) = self.sp_target_device_idx {
                if let Some(device) = devices.get(idx) {
                    if device.is_system {
                        ui.colored_label(
                            egui::Color32::from_rgb(255, 100, 100),
                            "WARNING: This is a system disk!",
                        );
                    }
                }
            }

            // Show scanned target partitions
            if !self.sp_target_partitions.is_empty() {
                ui.add_space(4.0);
                ui.label("Target Partition:");

                // Get source size for comparison
                let source_size = self.get_sp_source_size();

                egui::Grid::new("sp_target_partitions")
                    .striped(true)
                    .min_col_width(30.0)
                    .show(ui, |ui| {
                        ui.label(egui::RichText::new("").strong()); // radio
                        ui.label(egui::RichText::new("#").strong());
                        ui.label(egui::RichText::new("Type").strong());
                        ui.label(egui::RichText::new("Size").strong());
                        ui.label(egui::RichText::new("").strong()); // compat
                        ui.end_row();

                        for part in &self.sp_target_partitions {
                            if part.is_extended_container {
                                continue;
                            }
                            let selected = self.sp_target_partition_idx == Some(part.index);
                            if ui.radio(selected, "").clicked() {
                                self.sp_target_partition_idx = Some(part.index);
                            }
                            ui.label(format!("{}", part.index));
                            ui.label(&part.type_name);
                            ui.label(partition::format_size(part.size_bytes));

                            // Size compatibility indicator
                            if let Some(src_sz) = source_size {
                                if part.size_bytes < src_sz {
                                    ui.colored_label(
                                        egui::Color32::from_rgb(255, 100, 100),
                                        "too small",
                                    );
                                } else {
                                    ui.label("OK");
                                }
                            } else {
                                ui.label("");
                            }
                            ui.end_row();
                        }
                    });
            }

            if let Some(err) = &self.sp_scan_error {
                ui.colored_label(egui::Color32::from_rgb(200, 150, 100), err);
            }
        });

        // Action buttons
        self.show_action_buttons(ui, devices, log);
    }

    /// Get the source data size for the single-partition mode.
    fn get_sp_source_size(&self) -> Option<u64> {
        if let Some(meta) = &self.backup_metadata {
            if let Some(idx) = self.sp_source_partition_idx {
                return meta
                    .partitions
                    .iter()
                    .find(|p| p.index == idx)
                    .map(|p| p.imaged_size_bytes);
            }
        }
        if let Some(path) = &self.sp_image_file {
            return std::fs::metadata(path).ok().map(|m| m.len());
        }
        None
    }

    /// Scan the target device's partition table (requires elevation for devices).
    fn scan_target_partitions(&mut self, devices: &[DiskDevice], log: &mut LogPanel) {
        self.sp_target_partitions.clear();
        self.sp_target_partition_idx = None;
        self.sp_scan_error = None;

        let device = match self.sp_target_device_idx.and_then(|idx| devices.get(idx)) {
            Some(d) => d,
            None => return,
        };

        log.info(format!(
            "Scanning target partitions on {}...",
            device.path.display()
        ));

        match rusty_backup::os::open_source_for_reading(&device.path) {
            Ok(elevated) => {
                let (file, _guard) = elevated.into_parts();
                let mut reader = BufReader::new(file);
                match PartitionTable::detect(&mut reader) {
                    Ok(table) => {
                        self.sp_target_partitions = table.partitions();
                        log.info(format!(
                            "Found {} partition(s) on target ({})",
                            self.sp_target_partitions.len(),
                            table.type_name(),
                        ));
                    }
                    Err(e) => {
                        self.sp_scan_error = Some(format!("Could not read partition table: {e}"));
                        log.error(format!("Target scan failed: {e}"));
                    }
                }
            }
            Err(e) => {
                self.sp_scan_error = Some(format!("Cannot access device: {e}"));
                log.error(format!("Cannot access target device: {e}"));
            }
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
                match self.restore_mode {
                    RestoreMode::FullDisk => {
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
                    }
                    RestoreMode::SinglePartition => {
                        let target_name = self
                            .sp_target_device_idx
                            .and_then(|idx| devices.get(idx))
                            .map(|d| d.display_name())
                            .unwrap_or_else(|| "Unknown".into());
                        let target_part = self.sp_target_partition_idx.and_then(|idx| {
                            self.sp_target_partitions.iter().find(|p| p.index == idx)
                        });
                        let part_desc = target_part
                            .map(|p| {
                                format!(
                                    "Partition {} ({}, {})",
                                    p.index,
                                    p.type_name,
                                    partition::format_size(p.size_bytes),
                                )
                            })
                            .unwrap_or_else(|| "Unknown".into());

                        ui.colored_label(
                            egui::Color32::from_rgb(255, 180, 100),
                            format!("{} on {} will be overwritten.", part_desc, target_name,),
                        );
                        ui.label("The partition table will NOT be modified.");
                    }
                    RestoreMode::NewDisk => {
                        let target_name = if self.nd_target_is_device {
                            self.nd_target_device_idx
                                .and_then(|idx| devices.get(idx))
                                .map(|d| d.display_name())
                                .unwrap_or_else(|| "Unknown".into())
                        } else {
                            self.nd_image_file_path
                                .as_ref()
                                .map(|p| p.display().to_string())
                                .unwrap_or_else(|| "Unknown".into())
                        };
                        let table_type = match self.nd_table_type {
                            NewTableType::Mbr => "MBR",
                            NewTableType::Gpt => "GPT",
                            NewTableType::Apm => "APM",
                        };
                        ui.colored_label(
                            egui::Color32::from_rgb(255, 100, 100),
                            format!(
                                "A new {} partition table will be written to {}!",
                                table_type, target_name,
                            ),
                        );
                        ui.label("All existing data will be destroyed.");
                    }
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
                        match self.restore_mode {
                            RestoreMode::FullDisk => self.start_restore(devices, log),
                            RestoreMode::SinglePartition => {
                                self.start_single_partition_restore(devices, log)
                            }
                            RestoreMode::NewDisk => self.start_new_disk_restore(devices, log),
                        }
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

    fn start_single_partition_restore(&mut self, devices: &[DiskDevice], log: &mut LogPanel) {
        // Determine source
        let source = if let Some(path) = &self.sp_image_file {
            SinglePartitionSource::ImageFile { path: path.clone() }
        } else if let Some(folder) = &self.backup_folder {
            let idx = match self.sp_source_partition_idx {
                Some(i) => i,
                None => {
                    log.error("No source partition selected");
                    return;
                }
            };
            SinglePartitionSource::Backup {
                folder: folder.clone(),
                partition_index: idx,
            }
        } else {
            log.error("No source selected");
            return;
        };

        // Determine target
        let device = match self.sp_target_device_idx.and_then(|idx| devices.get(idx)) {
            Some(d) => d,
            None => {
                log.error("No target device selected");
                return;
            }
        };
        let target_part = match self
            .sp_target_partition_idx
            .and_then(|idx| self.sp_target_partitions.iter().find(|p| p.index == idx))
        {
            Some(p) => p.clone(),
            None => {
                log.error("No target partition selected");
                return;
            }
        };

        let source_start_lba = match &source {
            SinglePartitionSource::Backup {
                partition_index, ..
            } => self
                .backup_metadata
                .as_ref()
                .and_then(|m| m.partitions.iter().find(|p| p.index == *partition_index))
                .map(|p| p.start_lba)
                .unwrap_or(0),
            SinglePartitionSource::ImageFile { .. } => 0,
        };

        let config = SinglePartitionRestoreConfig {
            source,
            target_path: device.path.clone(),
            target_is_device: true,
            target_offset_bytes: target_part.start_lba * 512,
            target_size_bytes: Some(target_part.size_bytes),
            target_start_lba: target_part.start_lba,
            source_start_lba,
            new_disk: None,
        };

        let progress_arc = Arc::new(Mutex::new(RestoreProgress::new()));
        self.restore_progress = Some(Arc::clone(&progress_arc));
        self.restore_running = true;

        log.info(format!(
            "Starting single-partition restore to partition {} on {}",
            target_part.index,
            device.path.display(),
        ));

        std::thread::spawn(move || {
            if let Err(e) = rusty_backup::restore::single::run_single_partition_restore(
                config,
                Arc::clone(&progress_arc),
            ) {
                if let Ok(mut p) = progress_arc.lock() {
                    p.error = Some(format!("{e:#}"));
                    p.finished = true;
                }
            }
        });
    }

    fn show_new_disk_mode(
        &mut self,
        ui: &mut egui::Ui,
        devices: &[DiskDevice],
        log: &mut LogPanel,
        controls_enabled: bool,
    ) {
        // --- Source Section (same as single partition) ---
        ui.label(egui::RichText::new("Source").strong());
        ui.separator();

        ui.add_enabled_ui(controls_enabled, |ui| {
            ui.horizontal(|ui| {
                ui.label("Backup Folder:");
                let label = self
                    .backup_folder
                    .as_ref()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| "No folder selected".into());
                ui.label(&label);
                if ui.button("Browse...").clicked() {
                    if let Some(path) = super::file_dialog().pick_folder() {
                        self.backup_folder = Some(path);
                        self.sp_image_file = None;
                    }
                }
            });
            ui.horizontal(|ui| {
                ui.label("Or Image File:");
                let label = self
                    .sp_image_file
                    .as_ref()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| "No file selected".into());
                ui.label(&label);
                if ui.button("Browse...").clicked() {
                    if let Some(path) = super::file_dialog()
                        .add_filter(
                            "Disk Images",
                            &["img", "raw", "bin", "vhd", "2mg", "iso", "dd"],
                        )
                        .add_filter("All Files", &["*"])
                        .pick_file()
                    {
                        self.sp_image_file = Some(path);
                        self.backup_folder = None;
                        self.backup_metadata = None;
                        self.sp_source_partition_idx = None;
                        self.prev_backup_folder = None;
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

        // Source partition selector
        if let Some(meta) = &self.backup_metadata {
            ui.add_space(4.0);
            ui.add_enabled_ui(controls_enabled, |ui| {
                ui.horizontal(|ui| {
                    ui.label("Source Partition:");
                    let partitions = &meta.partitions;
                    let current_label = self
                        .sp_source_partition_idx
                        .and_then(|idx| partitions.iter().find(|p| p.index == idx))
                        .map(|p| {
                            format!(
                                "#{}: {} ({})",
                                p.index,
                                p.type_name,
                                partition::format_size(p.original_size_bytes),
                            )
                        })
                        .unwrap_or_else(|| "Select a partition...".into());

                    egui::ComboBox::from_id_salt("nd_source_partition")
                        .selected_text(&current_label)
                        .width(350.0)
                        .show_ui(ui, |ui| {
                            for pm in partitions {
                                let label = format!(
                                    "#{}: {} ({})",
                                    pm.index,
                                    pm.type_name,
                                    partition::format_size(pm.original_size_bytes),
                                );
                                ui.selectable_value(
                                    &mut self.sp_source_partition_idx,
                                    Some(pm.index),
                                    label,
                                );
                            }
                        });
                });
            });
        }

        if let Some(err) = &self.metadata_error {
            ui.colored_label(egui::Color32::from_rgb(200, 150, 100), err);
        }

        ui.add_space(8.0);

        // --- Target Section ---
        ui.label(egui::RichText::new("Target").strong());
        ui.separator();

        ui.add_enabled_ui(controls_enabled, |ui| {
            ui.horizontal(|ui| {
                ui.radio_value(&mut self.nd_target_is_device, true, "Write to device");
                ui.radio_value(&mut self.nd_target_is_device, false, "Save as image file");
            });

            if self.nd_target_is_device {
                ui.horizontal(|ui| {
                    ui.label("Device:");
                    let current_label = self
                        .nd_target_device_idx
                        .and_then(|idx| devices.get(idx))
                        .map(|d| d.display_name())
                        .unwrap_or_else(|| "Select a target device...".into());

                    egui::ComboBox::from_id_salt("nd_target_device")
                        .selected_text(&current_label)
                        .width(400.0)
                        .height(400.0)
                        .show_ui(ui, |ui| {
                            for (i, device) in devices.iter().enumerate() {
                                let label = format!(
                                    "{} ({}){}",
                                    device.display_name(),
                                    partition::format_size(device.size_bytes),
                                    if device.is_system { " [SYSTEM]" } else { "" },
                                );
                                ui.selectable_value(&mut self.nd_target_device_idx, Some(i), label);
                            }
                        });
                });
            } else {
                ui.horizontal(|ui| {
                    ui.label("Image File:");
                    let label = self
                        .nd_image_file_path
                        .as_ref()
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|| "No file selected".into());
                    ui.label(&label);
                    if ui.button("Save As...").clicked() {
                        if let Some(path) = super::file_dialog()
                            .add_filter("Disk Images", &["img", "raw", "bin"])
                            .save_file()
                        {
                            self.nd_image_file_path = Some(path);
                        }
                    }
                });
            }
        });

        ui.add_space(8.0);

        // --- Partition Table Options ---
        ui.label(egui::RichText::new("New Partition Table").strong());
        ui.separator();

        ui.add_enabled_ui(controls_enabled, |ui| {
            ui.horizontal(|ui| {
                ui.label("Table Type:");
                ui.radio_value(&mut self.nd_table_type, NewTableType::Mbr, "MBR");
                ui.radio_value(&mut self.nd_table_type, NewTableType::Gpt, "GPT");
                ui.radio_value(&mut self.nd_table_type, NewTableType::Apm, "APM");
            });

            ui.horizontal(|ui| {
                ui.label("Alignment:");
                ui.radio_value(
                    &mut self.nd_alignment_choice,
                    AlignmentChoice::Modern1MB,
                    "1 MB (LBA 2048)",
                );
                ui.radio_value(
                    &mut self.nd_alignment_choice,
                    AlignmentChoice::Original,
                    "DOS (LBA 63)",
                );
                ui.radio_value(
                    &mut self.nd_alignment_choice,
                    AlignmentChoice::Custom,
                    "Custom",
                );
                if self.nd_alignment_choice == AlignmentChoice::Custom {
                    ui.add(egui::DragValue::new(&mut self.nd_custom_alignment).range(1..=65535));
                    ui.label("sectors");
                }
            });

            if self.nd_table_type == NewTableType::Mbr {
                ui.checkbox(&mut self.nd_bootable, "Mark partition as bootable");
            }

            // Preview layout
            let alignment_sectors = match self.nd_alignment_choice {
                AlignmentChoice::Original => 63,
                AlignmentChoice::Modern1MB => 2048,
                AlignmentChoice::Custom => self.nd_custom_alignment as u64,
            };
            let start_lba = match self.nd_table_type {
                NewTableType::Gpt => {
                    // GPT needs LBAs 0-33 for protective MBR + header + entries
                    let min_lba = 34u64;
                    if alignment_sectors > 0 {
                        let rem = min_lba % alignment_sectors;
                        if rem != 0 {
                            min_lba + alignment_sectors - rem
                        } else {
                            min_lba
                        }
                    } else {
                        min_lba
                    }
                }
                _ => alignment_sectors,
            };
            ui.colored_label(
                egui::Color32::GRAY,
                format!("Partition will start at LBA {}", start_lba),
            );
        });

        self.show_action_buttons(ui, devices, log);
    }

    fn start_new_disk_restore(&mut self, devices: &[DiskDevice], log: &mut LogPanel) {
        // Determine source
        let source = if let Some(path) = &self.sp_image_file {
            SinglePartitionSource::ImageFile { path: path.clone() }
        } else if let Some(folder) = &self.backup_folder {
            let idx = match self.sp_source_partition_idx {
                Some(i) => i,
                None => {
                    log.error("No source partition selected");
                    return;
                }
            };
            SinglePartitionSource::Backup {
                folder: folder.clone(),
                partition_index: idx,
            }
        } else {
            log.error("No source selected");
            return;
        };

        // Determine target
        let (target_path, target_is_device, disk_size_bytes) = if self.nd_target_is_device {
            let device = match self.nd_target_device_idx.and_then(|idx| devices.get(idx)) {
                Some(d) => d,
                None => {
                    log.error("No target device selected");
                    return;
                }
            };
            (device.path.clone(), true, device.size_bytes)
        } else {
            let path = match &self.nd_image_file_path {
                Some(p) => p.clone(),
                None => {
                    log.error("No target file selected");
                    return;
                }
            };
            // For image files, estimate disk size from source partition size
            let source_size = self.get_sp_source_size().unwrap_or(0);
            let estimated = source_size + 2048 * 512; // add alignment overhead
            (path, false, estimated)
        };

        // Calculate partition start LBA
        let alignment_sectors = match self.nd_alignment_choice {
            AlignmentChoice::Original => 63u64,
            AlignmentChoice::Modern1MB => 2048,
            AlignmentChoice::Custom => self.nd_custom_alignment as u64,
        };
        let start_lba = match self.nd_table_type {
            NewTableType::Gpt => {
                let min_lba = 34u64;
                if alignment_sectors > 0 {
                    let rem = min_lba % alignment_sectors;
                    if rem != 0 {
                        min_lba + alignment_sectors - rem
                    } else {
                        min_lba
                    }
                } else {
                    min_lba
                }
            }
            _ => alignment_sectors,
        };

        let source_start_lba = match &source {
            SinglePartitionSource::Backup {
                partition_index, ..
            } => self
                .backup_metadata
                .as_ref()
                .and_then(|m| m.partitions.iter().find(|p| p.index == *partition_index))
                .map(|p| p.start_lba)
                .unwrap_or(0),
            SinglePartitionSource::ImageFile { .. } => 0,
        };

        // Determine partition type from backup metadata
        let partition_type_byte = self
            .backup_metadata
            .as_ref()
            .and_then(|m| {
                self.sp_source_partition_idx.and_then(|idx| {
                    m.partitions
                        .iter()
                        .find(|p| p.index == idx)
                        .map(|p| p.partition_type_byte)
                })
            })
            .unwrap_or(0x0C); // default to FAT32 LBA

        let partition_type_string = self.backup_metadata.as_ref().and_then(|m| {
            self.sp_source_partition_idx.and_then(|idx| {
                m.partitions
                    .iter()
                    .find(|p| p.index == idx)
                    .and_then(|p| p.partition_type_string.clone())
            })
        });

        let config = SinglePartitionRestoreConfig {
            source,
            target_path: target_path.clone(),
            target_is_device,
            target_offset_bytes: start_lba * 512,
            target_size_bytes: None, // use source size
            target_start_lba: start_lba,
            source_start_lba,
            new_disk: Some(NewDiskConfig {
                table_type: self.nd_table_type,
                alignment_sectors,
                partition_type_byte,
                partition_type_guid: None, // use default
                partition_type_string,
                bootable: self.nd_bootable,
                disk_size_bytes,
            }),
        };

        let progress_arc = Arc::new(Mutex::new(RestoreProgress::new()));
        self.restore_progress = Some(Arc::clone(&progress_arc));
        self.restore_running = true;

        log.info(format!(
            "Starting new-disk restore to {}",
            target_path.display(),
        ));

        std::thread::spawn(move || {
            if let Err(e) = rusty_backup::restore::single::run_single_partition_restore(
                config,
                Arc::clone(&progress_arc),
            ) {
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
