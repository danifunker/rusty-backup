use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use rusty_backup::backup::metadata::BackupMetadata;
use rusty_backup::clonezilla;
use rusty_backup::clonezilla::metadata::ClonezillaImage;
use rusty_backup::model::size_mode::SizeMode;
use rusty_backup::partition::{self, PartitionInfo, PartitionTable};

use super::partition_bar::{PartitionBar, Segment, SegmentKind};
use super::size_mode_row::{size_mode_row, SizeModeRowOptions};
use rusty_backup::restore::single::{
    NewDiskConfig, NewTableType, SinglePartitionRestoreConfig, SinglePartitionSource,
};
use rusty_backup::restore::{
    self, RestoreAlignment, RestoreConfig, RestorePartitionSize, RestoreProgress, RestoreSizeChoice,
};

use super::context::TabContext;
use super::progress::ProgressState;

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
    choice: SizeMode,
    custom_size_mib: u32,
}

impl RestorePartitionConfig {
    fn effective_size(&self) -> u64 {
        self.choice
            .effective_size(self.original_size, self.minimum_size, self.custom_size_mib)
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
    /// Keeps a decompressed `.adz` / `.hdz` source alive while
    /// `sp_image_file` references it.
    sp_amiga_tempdir: Option<tempfile::TempDir>,
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

    // --- "Add free space for in-OS expansion" (Phase 2 of disk_expansion plan) ---
    /// Enabled by the "Add free space for in-OS expansion" checkbox. Image
    /// targets only — disabled for device targets since physical disk size
    /// is fixed.
    expand_disk_enabled: bool,
    /// How much free space to leave at the end of the target image, in MiB.
    /// In Mode A the trailing region is unallocated and the user partitions
    /// it in their guest OS; in Mode B the last partition is extended over
    /// it so only a filesystem-side grow tool (xfs_growfs, resize2fs, ...)
    /// is needed.
    expand_free_space_mib: u32,
    /// True when the user chose "Extend last partition automatically"
    /// (Mode B). Forces the last partition's restore size to FillRemaining
    /// at restore-config build time. False is Mode A (the recommended
    /// default).
    expand_extend_last_partition: bool,

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
            sp_amiga_tempdir: None,
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
            expand_disk_enabled: false,
            expand_free_space_mib: 0,
            expand_extend_last_partition: false,
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

    pub fn show(&mut self, ui: &mut egui::Ui, ctx: &mut TabContext, progress: &mut ProgressState) {
        // Poll background restore thread
        self.poll_progress(ctx, progress);

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
            RestoreMode::FullDisk => self.show_full_disk_mode(ui, ctx, controls_enabled),
            RestoreMode::SinglePartition => {
                self.show_single_partition_mode(ui, ctx, controls_enabled)
            }
            RestoreMode::NewDisk => self.show_new_disk_mode(ui, ctx, controls_enabled),
        }

        // --- Confirmation popup ---
        if self.confirm_popup_open {
            self.show_confirmation_popup(ui, ctx);
        }
    }

    fn show_full_disk_mode(
        &mut self,
        ui: &mut egui::Ui,
        ctx: &mut TabContext,
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
                self.load_backup_metadata(ctx);
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
                        .and_then(|idx| ctx.devices.get(idx))
                        .map(|d| d.display_name())
                        .unwrap_or_else(|| "Select a target device...".into());

                    egui::ComboBox::from_id_salt("restore_target_device")
                        .selected_text(&current_label)
                        .width(400.0)
                        .height(400.0) // Allow more items to be visible without scrolling
                        .show_ui(ui, |ui| {
                            for (i, device) in ctx.devices.iter().enumerate() {
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
                    if let Some(device) = ctx.devices.get(idx) {
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
                let target_size = self.get_target_size(ctx);
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

        // --- Section 2b: Add free space for in-OS expansion ---
        // Image-file targets only — for device targets the disk size is
        // fixed by the hardware, so the toggle doesn't apply. Recommends
        // Mode A ("leave as unpartitioned free space") which works for every
        // filesystem on every guest OS; Mode B is added in Phase 4 of
        // docs/disk_expansion.md.
        let source_size_bytes = self
            .backup_metadata
            .as_ref()
            .map(|m| m.source_size_bytes)
            .or_else(|| self.clonezilla_image.as_ref().map(|c| c.source_size_bytes))
            .unwrap_or(0);
        if !self.target_is_device
            && source_size_bytes > 0
            && (self.backup_metadata.is_some() || self.clonezilla_image.is_some())
        {
            ui.add_space(8.0);
            ui.label(egui::RichText::new("Add free space for in-OS expansion").strong());
            ui.separator();

            ui.add_enabled_ui(controls_enabled, |ui| {
                ui.checkbox(
                    &mut self.expand_disk_enabled,
                    "Pad the target image with unallocated free space",
                )
                .on_hover_text(
                    "Useful for restoring a backup onto a larger virtual disk and \
                     letting the guest OS extend or add a partition there. After \
                     restore, run your OS's partitioner (parted, fdisk, IRIX fx, \
                     Disk Management) followed by your filesystem's grow tool \
                     (xfs_growfs, resize2fs, ...). Works for any filesystem.",
                );

                if self.expand_disk_enabled {
                    let source_mib = (source_size_bytes / (1024 * 1024)).max(1) as u32;
                    let cap = source_mib.saturating_mul(8).max(source_mib + 8192);
                    ui.horizontal(|ui| {
                        ui.label("Free space:");
                        ui.add(
                            egui::DragValue::new(&mut self.expand_free_space_mib)
                                .range(0..=cap)
                                .suffix(" MiB"),
                        );
                        let new_total_bytes = source_size_bytes
                            .saturating_add((self.expand_free_space_mib as u64) * 1024 * 1024);
                        ui.label(format!(
                            "(target size {} -> {})",
                            partition::format_size(source_size_bytes),
                            partition::format_size(new_total_bytes),
                        ));
                    });

                    // Mode A vs Mode B radio.
                    ui.add_space(4.0);
                    ui.radio_value(
                        &mut self.expand_extend_last_partition,
                        false,
                        "Leave as unpartitioned free space (recommended)",
                    )
                    .on_hover_text(
                        "Mode A: partition table stays unchanged; the trailing region is \
                         unallocated. After restore, run your OS's partitioner to extend or \
                         add a partition, then your filesystem's grow tool. Works for any \
                         filesystem on any OS.",
                    );
                    ui.radio_value(
                        &mut self.expand_extend_last_partition,
                        true,
                        "Extend last partition automatically",
                    )
                    .on_hover_text(
                        "Mode B: the last partition is sized to absorb the new free space \
                         during restore (equivalent to picking 'Fill' for the last partition \
                         in the size table below). After restore, only the filesystem-side \
                         grow tool (xfs_growfs, resize2fs, ...) is needed in the guest OS.",
                    );

                    // Visualization: current vs after PartitionBar pair.
                    self.show_expand_preview_bars(ui, source_size_bytes);
                }
            });
        } else if self.expand_disk_enabled {
            // Auto-disable when the user flips back to a device target so
            // the next file-target render doesn't surprise them with a
            // stale "free space" value still in the target-size readout.
            self.expand_disk_enabled = false;
        }

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

                            size_mode_row(
                                ui,
                                &mut cfg.choice,
                                &mut cfg.custom_size_mib,
                                cfg.original_size,
                                cfg.minimum_size,
                                SizeModeRowOptions {
                                    allow_fill: Some(cfg.index) == last_index,
                                    custom_seed: super::size_mode_row::CustomSeed::Original,
                                    ..Default::default()
                                },
                            );
                            ui.end_row();
                        }
                    });

                // Total size summary
                let target_size = self.get_target_size(ctx);
                if target_size > 0 {
                    let total: u64 = self
                        .partition_configs
                        .iter()
                        .map(|c| {
                            if c.choice == SizeMode::FillRemaining {
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
        self.show_action_buttons(ui, ctx);
    }

    fn show_action_buttons(&mut self, ui: &mut egui::Ui, ctx: &mut TabContext) {
        ui.add_space(16.0);
        ui.horizontal(|ui| {
            if !self.restore_running {
                let can_start = match self.restore_mode {
                    RestoreMode::FullDisk => {
                        (self.backup_metadata.is_some() || self.clonezilla_image.is_some())
                            && self.has_target(ctx)
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
            } else if ui.button("Cancel").clicked() {
                if let Some(ref progress_arc) = self.restore_progress {
                    if let Ok(mut p) = progress_arc.lock() {
                        p.cancel_requested = true;
                        p.operation =
                            "Cancelling — waiting for current write to complete…".to_string();
                    }
                }
                ctx.log
                    .warn("Cancellation requested — waiting for current disk write to complete...");
            }
        });
    }

    fn show_single_partition_mode(
        &mut self,
        ui: &mut egui::Ui,
        ctx: &mut TabContext,
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
                            &[
                                "img", "raw", "bin", "vhd", "2mg", "iso", "dd", "po", "do", "dsk",
                                "dc42", "woz", "adf", "hdf", "adz", "hdz", "imz", "vmdk", "qcow2",
                                "qcow", "gho", "ghs", "GHO", "GHS", "hfv", "HFV",
                            ],
                        )
                        .add_filter("All Files", &["*"])
                        .pick_file()
                    {
                        match super::prepare_disk_image_path(&path) {
                            Ok((materialized, guard)) => {
                                self.sp_image_file = Some(materialized);
                                self.sp_amiga_tempdir = guard;
                            }
                            Err(e) => {
                                log::error!("Failed to decompress {}: {}", path.display(), e);
                                self.sp_image_file = Some(path);
                                self.sp_amiga_tempdir = None;
                            }
                        }
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
                self.load_backup_metadata(ctx);
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
                    .and_then(|idx| ctx.devices.get(idx))
                    .map(|d| d.display_name())
                    .unwrap_or_else(|| "Select a target device...".into());

                egui::ComboBox::from_id_salt("sp_target_device")
                    .selected_text(&current_label)
                    .width(400.0)
                    .height(400.0)
                    .show_ui(ui, |ui| {
                        for (i, device) in ctx.devices.iter().enumerate() {
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

                if self.sp_target_device_idx.is_some() && ui.button("Scan Target").clicked() {
                    self.scan_target_partitions(ctx);
                }
            });

            // System disk warning
            if let Some(idx) = self.sp_target_device_idx {
                if let Some(device) = ctx.devices.get(idx) {
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
        self.show_action_buttons(ui, ctx);
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
    fn scan_target_partitions(&mut self, ctx: &mut TabContext) {
        self.sp_target_partitions.clear();
        self.sp_target_partition_idx = None;
        self.sp_scan_error = None;

        let device = match self
            .sp_target_device_idx
            .and_then(|idx| ctx.devices.get(idx))
        {
            Some(d) => d,
            None => return,
        };

        ctx.log.info(format!(
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
                        ctx.log.info(format!(
                            "Found {} partition(s) on target ({})",
                            self.sp_target_partitions.len(),
                            table.type_name(),
                        ));
                    }
                    Err(e) => {
                        self.sp_scan_error = Some(format!("Could not read partition table: {e}"));
                        ctx.log.error(format!("Target scan failed: {e}"));
                    }
                }
            }
            Err(e) => {
                self.sp_scan_error = Some(format!("Cannot access device: {e}"));
                ctx.log.error(format!("Cannot access target device: {e}"));
            }
        }
    }

    fn load_backup_metadata(&mut self, ctx: &mut TabContext) {
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
            self.load_native_backup_metadata(&folder, &metadata_path, ctx);
        } else if clonezilla::metadata::is_clonezilla_image(&folder) {
            self.load_clonezilla_backup_metadata(&folder, ctx);
        } else {
            self.metadata_error =
                Some("No backup found (metadata.json or Clonezilla image)".to_string());
        }
    }

    fn load_native_backup_metadata(
        &mut self,
        folder: &std::path::Path,
        metadata_path: &std::path::Path,
        ctx: &mut TabContext,
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
            // Prefer the smallest known-safe floor: defragmented_min when it
            // exists (CHD backups produced with compact-always have allocated
            // clusters packed at the start, so the defragmented floor is real),
            // then in-place minimum_size_bytes (last_data_byte), then
            // imaged_size_bytes for older packed backups, and finally
            // original_size_bytes when no sizing hint is available.
            let minimum = pm
                .defragmented_min_size_bytes
                .filter(|&s| s > 0)
                .or(pm.minimum_size_bytes.filter(|&s| s > 0))
                .unwrap_or({
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
                choice: SizeMode::Original,
                custom_size_mib: (pm.original_size_bytes / (1024 * 1024)) as u32,
            });
        }

        ctx.log.info(format!(
            "Loaded backup: {} partition(s) from {}",
            metadata.partitions.len(),
            metadata.source_device,
        ));

        self.backup_metadata = Some(metadata);
    }

    fn load_clonezilla_backup_metadata(&mut self, folder: &std::path::Path, ctx: &mut TabContext) {
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
                choice: SizeMode::Original,
                custom_size_mib: (cz_part.size_bytes() / (1024 * 1024)) as u32,
            });
        }

        ctx.log.info(format!(
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

    /// Render the before/after PartitionBar pair for the expand-disk preview.
    /// `source_size` is the original backup-source disk size in bytes; the
    /// "After" bar appends a `Free` segment sized by the expansion toggle.
    /// Both bars use the same byte-per-pixel scale, so the After bar is
    /// physically wider by the growth ratio — the visual "this disk is now
    /// bigger" cue.
    fn show_expand_preview_bars(&self, ui: &mut egui::Ui, source_size: u64) {
        let segments = self.build_expand_preview_segments();
        let added_bytes = self.expand_free_space_bytes();
        let new_total = source_size.saturating_add(added_bytes);

        ui.add_space(6.0);
        ui.label("Current:");
        let available_width = ui.available_width().max(120.0);
        // Avoid div-by-zero; clamp to 1 byte to make the math work even on
        // edge-case empty backups.
        let denom = new_total.max(1);
        let current_ratio = (source_size as f64 / denom as f64) as f32;
        let current_width = available_width * current_ratio;
        let current_segments = segments.clone();
        ui.scope(|ui| {
            ui.set_width(current_width.max(60.0));
            PartitionBar {
                segments: current_segments,
                show_inline_labels: true,
                show_legend: false,
            }
            .show(ui);
        });

        ui.add_space(6.0);
        ui.label(format!(
            "After  (+ {} free):",
            partition::format_size(added_bytes),
        ));
        let mut after_segments = segments;
        if added_bytes > 0 {
            if self.expand_extend_last_partition {
                // Mode B: the last partition absorbs the new free space.
                // Show this by growing its size_bytes; the bar renders the
                // last segment longer in its own color, signalling "this
                // partition will be larger after restore."
                if let Some(last) = after_segments.last_mut() {
                    last.size_bytes = last.size_bytes.saturating_add(added_bytes);
                }
            } else {
                // Mode A: a separate gray Free segment at the end.
                after_segments.push(Segment {
                    label: String::new(),
                    fs: String::new(),
                    size_bytes: added_bytes,
                    kind: SegmentKind::Free,
                });
            }
        }
        PartitionBar {
            segments: after_segments,
            show_inline_labels: true,
            show_legend: true,
        }
        .show(ui);
    }

    /// Build the partition-bar segments matching the current backup source.
    /// Skips extended-container entries; partitions get sequential color
    /// indices so the palette is stable across the current/after bars.
    fn build_expand_preview_segments(&self) -> Vec<Segment> {
        let mut color_index: usize = 0;
        let mut segs = Vec::new();

        if let Some(meta) = &self.backup_metadata {
            for pm in &meta.partitions {
                if pm.is_logical {
                    // Logical partitions live inside an extended container —
                    // we render the container, not the children, to keep the
                    // bar a flat view of the disk layout.
                    continue;
                }
                let kind = SegmentKind::Partition { color_index };
                color_index += 1;
                segs.push(Segment {
                    label: format!("Partition {}", pm.index + 1),
                    fs: pm.type_name.clone(),
                    size_bytes: pm.original_size_bytes,
                    kind,
                });
            }
        } else if let Some(cz) = &self.clonezilla_image {
            for cp in &cz.partitions {
                if cp.is_logical || cp.is_extended {
                    continue;
                }
                let kind = SegmentKind::Partition { color_index };
                color_index += 1;
                segs.push(Segment {
                    label: format!("Partition {}", cp.index + 1),
                    fs: cp.filesystem_type.clone(),
                    size_bytes: cp.size_sectors * 512,
                    kind,
                });
            }
        }
        segs
    }

    fn get_target_size(&self, ctx: &TabContext) -> u64 {
        if self.target_is_device {
            self.selected_device_idx
                .and_then(|idx| ctx.devices.get(idx))
                .map(|d| d.size_bytes)
                .unwrap_or(0)
        } else {
            // For image files, use the source disk size as default target.
            // The "Add free space for in-OS expansion" toggle (Mode A of
            // docs/disk_expansion.md) pads the trailing region of the
            // output image so the guest OS can extend or add partitions
            // there and absorb the space with its native grow tool.
            let base = if let Some(meta) = &self.backup_metadata {
                meta.source_size_bytes
            } else if let Some(cz) = &self.clonezilla_image {
                cz.source_size_bytes
            } else {
                0
            };
            base.saturating_add(self.expand_free_space_bytes())
        }
    }

    /// Free-space contribution from the expansion toggle, in bytes. Zero when
    /// the toggle is off (or when the target is a device, since physical disk
    /// size isn't user-controllable).
    pub fn expand_free_space_bytes(&self) -> u64 {
        if self.expand_disk_enabled && !self.target_is_device {
            (self.expand_free_space_mib as u64) * 1024 * 1024
        } else {
            0
        }
    }

    fn has_target(&self, ctx: &TabContext) -> bool {
        if self.target_is_device {
            self.selected_device_idx
                .and_then(|idx| ctx.devices.get(idx))
                .is_some()
        } else {
            self.image_file_path.is_some()
        }
    }

    fn show_confirmation_popup(&mut self, ui: &mut egui::Ui, ctx: &mut TabContext) {
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
                                .and_then(|idx| ctx.devices.get(idx))
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
                            let size_str = if cfg.choice == SizeMode::FillRemaining {
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
                            .and_then(|idx| ctx.devices.get(idx))
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
                                .and_then(|idx| ctx.devices.get(idx))
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
                            RestoreMode::FullDisk => self.start_restore(ctx),
                            RestoreMode::SinglePartition => {
                                self.start_single_partition_restore(ctx)
                            }
                            RestoreMode::NewDisk => self.start_new_disk_restore(ctx),
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

    fn start_restore(&mut self, ctx: &mut TabContext) {
        let backup_folder = match &self.backup_folder {
            Some(f) => f.clone(),
            None => {
                ctx.log.error("No backup folder selected");
                return;
            }
        };

        let (target_path, target_is_device, target_size) = if self.target_is_device {
            let device = match self
                .selected_device_idx
                .and_then(|idx| ctx.devices.get(idx))
            {
                Some(d) => d,
                None => {
                    ctx.log.error("No target device selected");
                    return;
                }
            };
            (device.path.clone(), true, device.size_bytes)
        } else {
            let path = match &self.image_file_path {
                Some(p) => p.clone(),
                None => {
                    ctx.log.error("No image file selected");
                    return;
                }
            };
            let base = self
                .backup_metadata
                .as_ref()
                .map(|m| m.source_size_bytes)
                .or_else(|| self.clonezilla_image.as_ref().map(|c| c.source_size_bytes))
                .unwrap_or(0);
            // Add the "Add free space for in-OS expansion" amount so the
            // target image is sized for both the original layout and the
            // requested trailing free space (Phase 2 of disk_expansion).
            let size = base.saturating_add(self.expand_free_space_bytes());
            (path, false, size)
        };

        let alignment = match self.alignment_choice {
            AlignmentChoice::Original => RestoreAlignment::Original,
            AlignmentChoice::Modern1MB => RestoreAlignment::Modern1MB,
            AlignmentChoice::Custom => {
                RestoreAlignment::Custom(self.custom_alignment_sectors as u64)
            }
        };

        // Mode B of the disk-expansion feature: when "Extend last partition
        // automatically" is selected alongside an image-file target, force
        // the last partition's size to FillRemaining so the restore engine
        // grows it over the trailing free space. Mode A (the default) skips
        // this — the trailing region stays unallocated.
        let last_idx = self.partition_configs.last().map(|c| c.index);
        let mode_b_active =
            self.expand_disk_enabled && self.expand_extend_last_partition && !self.target_is_device;

        let partition_sizes: Vec<RestorePartitionSize> = self
            .partition_configs
            .iter()
            .map(|cfg| RestorePartitionSize {
                index: cfg.index,
                size_choice: if mode_b_active && Some(cfg.index) == last_idx {
                    RestoreSizeChoice::FillRemaining
                } else {
                    match cfg.choice {
                        SizeMode::Original => RestoreSizeChoice::Original,
                        SizeMode::Minimum => {
                            // Pass the concrete minimum size so layout calculation
                            // doesn't need to re-derive it (especially for Clonezilla
                            // where the partclone header has the real used size).
                            RestoreSizeChoice::Custom(cfg.minimum_size)
                        }
                        SizeMode::MinPlus20 => {
                            RestoreSizeChoice::Custom(cfg.choice.effective_size(
                                cfg.original_size,
                                cfg.minimum_size,
                                cfg.custom_size_mib,
                            ))
                        }
                        SizeMode::Custom => {
                            RestoreSizeChoice::Custom(cfg.custom_size_mib as u64 * 1024 * 1024)
                        }
                        SizeMode::FillRemaining => RestoreSizeChoice::FillRemaining,
                    }
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

        ctx.log
            .info(format!("Starting restore to {}", target_path.display(),));

        std::thread::spawn(move || {
            let _wake = rusty_backup::os::wakelock::acquire("Rusty Backup: disk restore");
            if let Err(e) = restore::run_restore(config, Arc::clone(&progress_arc)) {
                if let Ok(mut p) = progress_arc.lock() {
                    p.error = Some(format!("{e:#}"));
                    p.finished = true;
                }
            }
        });
    }

    fn start_single_partition_restore(&mut self, ctx: &mut TabContext) {
        // Determine source
        let source = if let Some(path) = &self.sp_image_file {
            SinglePartitionSource::ImageFile { path: path.clone() }
        } else if let Some(folder) = &self.backup_folder {
            let idx = match self.sp_source_partition_idx {
                Some(i) => i,
                None => {
                    ctx.log.error("No source partition selected");
                    return;
                }
            };
            SinglePartitionSource::Backup {
                folder: folder.clone(),
                partition_index: idx,
            }
        } else {
            ctx.log.error("No source selected");
            return;
        };

        // Determine target
        let device = match self
            .sp_target_device_idx
            .and_then(|idx| ctx.devices.get(idx))
        {
            Some(d) => d,
            None => {
                ctx.log.error("No target device selected");
                return;
            }
        };
        let target_part = match self
            .sp_target_partition_idx
            .and_then(|idx| self.sp_target_partitions.iter().find(|p| p.index == idx))
        {
            Some(p) => p.clone(),
            None => {
                ctx.log.error("No target partition selected");
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

        ctx.log.info(format!(
            "Starting single-partition restore to partition {} on {}",
            target_part.index,
            device.path.display(),
        ));

        std::thread::spawn(move || {
            let _wake = rusty_backup::os::wakelock::acquire("Rusty Backup: partition restore");
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
        ctx: &mut TabContext,
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
                            &[
                                "img", "raw", "bin", "vhd", "2mg", "iso", "dd", "po", "do", "dsk",
                                "dc42", "woz", "adf", "hdf", "adz", "hdz", "imz", "vmdk", "qcow2",
                                "qcow", "gho", "ghs", "GHO", "GHS", "hfv", "HFV",
                            ],
                        )
                        .add_filter("All Files", &["*"])
                        .pick_file()
                    {
                        match super::prepare_disk_image_path(&path) {
                            Ok((materialized, guard)) => {
                                self.sp_image_file = Some(materialized);
                                self.sp_amiga_tempdir = guard;
                            }
                            Err(e) => {
                                log::error!("Failed to decompress {}: {}", path.display(), e);
                                self.sp_image_file = Some(path);
                                self.sp_amiga_tempdir = None;
                            }
                        }
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
                self.load_backup_metadata(ctx);
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
                        .and_then(|idx| ctx.devices.get(idx))
                        .map(|d| d.display_name())
                        .unwrap_or_else(|| "Select a target device...".into());

                    egui::ComboBox::from_id_salt("nd_target_device")
                        .selected_text(&current_label)
                        .width(400.0)
                        .height(400.0)
                        .show_ui(ui, |ui| {
                            for (i, device) in ctx.devices.iter().enumerate() {
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

        self.show_action_buttons(ui, ctx);
    }

    fn start_new_disk_restore(&mut self, ctx: &mut TabContext) {
        // Determine source
        let source = if let Some(path) = &self.sp_image_file {
            SinglePartitionSource::ImageFile { path: path.clone() }
        } else if let Some(folder) = &self.backup_folder {
            let idx = match self.sp_source_partition_idx {
                Some(i) => i,
                None => {
                    ctx.log.error("No source partition selected");
                    return;
                }
            };
            SinglePartitionSource::Backup {
                folder: folder.clone(),
                partition_index: idx,
            }
        } else {
            ctx.log.error("No source selected");
            return;
        };

        // Determine target
        let (target_path, target_is_device, disk_size_bytes) = if self.nd_target_is_device {
            let device = match self
                .nd_target_device_idx
                .and_then(|idx| ctx.devices.get(idx))
            {
                Some(d) => d,
                None => {
                    ctx.log.error("No target device selected");
                    return;
                }
            };
            (device.path.clone(), true, device.size_bytes)
        } else {
            let path = match &self.nd_image_file_path {
                Some(p) => p.clone(),
                None => {
                    ctx.log.error("No target file selected");
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

        ctx.log.info(format!(
            "Starting new-disk restore to {}",
            target_path.display(),
        ));

        std::thread::spawn(move || {
            let _wake = rusty_backup::os::wakelock::acquire("Rusty Backup: new-disk restore");
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

    fn poll_progress(&mut self, ctx: &mut TabContext, progress_state: &mut ProgressState) {
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
                rusty_backup::backup::LogLevel::Info => ctx.log.info(msg.message),
                rusty_backup::backup::LogLevel::Warning => ctx.log.warn(msg.message),
                rusty_backup::backup::LogLevel::Error => ctx.log.error(msg.message),
            }
        }

        // Update progress bar
        progress_state.active = !p.finished;
        progress_state.operation = p.operation.clone();
        progress_state.current_bytes = p.current_bytes;
        progress_state.total_bytes = p.total_bytes;
        progress_state.full_size_bytes = 0;
        progress_state.record_sample();

        if p.finished {
            if let Some(err) = &p.error {
                ctx.log.error(format!("Restore failed: {err}"));
            } else {
                ctx.log.info("Restore completed successfully.");
            }
            drop(p);
            self.restore_running = false;
            self.restore_progress = None;
        }
    }
}
