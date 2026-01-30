use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

use rusty_backup::backup::metadata::BackupMetadata;
use rusty_backup::device::DiskDevice;
use rusty_backup::partition::{self, detect_alignment, PartitionAlignment, PartitionInfo, PartitionTable};

use super::browse_view::BrowseView;
use super::progress::{LogPanel, LogLevel};

/// State for the Inspect tab.
pub struct InspectTab {
    /// Index into the device list, or None if "Open Image File..." is selected
    selected_device_idx: Option<usize>,
    /// Path to an image file (when using file picker instead of device)
    image_file_path: Option<PathBuf>,
    /// Path to a backup folder (loaded via metadata.json)
    backup_folder_path: Option<PathBuf>,
    /// Parsed partition table result
    partition_table: Option<PartitionTable>,
    /// Detected alignment
    alignment: Option<PartitionAlignment>,
    /// Unified partition list for display
    partitions: Vec<PartitionInfo>,
    /// Loaded backup metadata (from metadata.json)
    backup_metadata: Option<BackupMetadata>,
    /// Error from last inspection attempt
    last_error: Option<String>,
    /// Previous selection state for auto-inspect change detection
    prev_device_idx: Option<usize>,
    prev_image_path: Option<PathBuf>,
    prev_backup_path: Option<PathBuf>,
    /// Filesystem browser
    browse_view: BrowseView,
}

impl Default for InspectTab {
    fn default() -> Self {
        Self {
            selected_device_idx: None,
            image_file_path: None,
            backup_folder_path: None,
            partition_table: None,
            alignment: None,
            partitions: Vec::new(),
            backup_metadata: None,
            last_error: None,
            prev_device_idx: None,
            prev_image_path: None,
            prev_backup_path: None,
            browse_view: BrowseView::default(),
        }
    }
}

impl InspectTab {
    pub fn show(
        &mut self,
        ui: &mut egui::Ui,
        devices: &[DiskDevice],
        log: &mut LogPanel,
    ) {
        ui.heading("Inspect Disk / Image");
        ui.add_space(8.0);

        // Device / image selection
        ui.horizontal(|ui| {
            ui.label("Source:");

            let current_label = if let Some(path) = &self.backup_folder_path {
                format!("Backup: {}", path.display())
            } else if let Some(idx) = self.selected_device_idx {
                devices
                    .get(idx)
                    .map(|d| d.display_name())
                    .unwrap_or_else(|| "Unknown".into())
            } else {
                "Select a device or image...".into()
            };

            egui::ComboBox::from_id_salt("inspect_source")
                .selected_text(&current_label)
                .width(400.0)
                .show_ui(ui, |ui| {
                    for (i, device) in devices.iter().enumerate() {
                        let label = device.display_name();
                        if ui
                            .selectable_value(&mut self.selected_device_idx, Some(i), &label)
                            .clicked()
                        {
                            self.image_file_path = None;
                            self.backup_folder_path = None;
                            self.clear_results();
                        }
                    }
                    ui.separator();
                    if ui
                        .selectable_label(self.backup_folder_path.is_some(), "Open Backup Folder...")
                        .clicked()
                    {
                        self.pick_backup_folder();
                    }
                });
        });

        ui.add_space(4.0);

        // Auto-inspect on selection change
        let selection_changed = self.selected_device_idx != self.prev_device_idx
            || self.image_file_path != self.prev_image_path
            || self.backup_folder_path != self.prev_backup_path;

        if selection_changed {
            self.prev_device_idx = self.selected_device_idx;
            self.prev_image_path = self.image_file_path.clone();
            self.prev_backup_path = self.backup_folder_path.clone();
            if self.backup_folder_path.is_some() {
                self.load_backup_metadata(log);
            } else if self.selected_device_idx.is_some() || self.image_file_path.is_some() {
                self.run_inspect(devices, log);
            }
        }

        // Re-inspect button
        ui.horizontal(|ui| {
            let has_source = self.selected_device_idx.is_some()
                || self.image_file_path.is_some()
                || self.backup_folder_path.is_some();
            if ui
                .add_enabled(has_source, egui::Button::new("Re-inspect"))
                .clicked()
            {
                if self.backup_folder_path.is_some() {
                    self.load_backup_metadata(log);
                } else {
                    self.run_inspect(devices, log);
                }
            }
        });

        ui.add_space(12.0);

        // Show error if any
        if let Some(err) = &self.last_error {
            ui.colored_label(egui::Color32::from_rgb(255, 100, 100), format!("Error: {err}"));
            ui.add_space(8.0);
        }

        // Show backup metadata if loaded from folder
        if let Some(meta) = &self.backup_metadata {
            self.show_backup_metadata(ui, meta);
        }

        // Show results
        let has_table = self.partition_table.is_some();
        if has_table {
            self.show_results(ui, devices, log);
        } else if !self.partitions.is_empty() {
            // Partitions loaded from metadata (no partition table object)
            self.show_partition_list(ui, devices, log);
        }

        // Show filesystem browser if active
        if self.browse_view.is_active() {
            //ui.add_space(4.0);
            self.browse_view.show(ui);
        }
    }

    fn pick_backup_folder(&mut self) {
        if let Some(path) = rfd::FileDialog::new().pick_folder() {
            self.backup_folder_path = Some(path);
            self.selected_device_idx = None;
            self.image_file_path = None;
            self.clear_results();
        }
    }

    fn clear_results(&mut self) {
        self.partition_table = None;
        self.alignment = None;
        self.partitions.clear();
        self.backup_metadata = None;
        self.last_error = None;
    }

    fn load_backup_metadata(&mut self, log: &mut LogPanel) {
        self.partition_table = None;
        self.alignment = None;
        self.partitions.clear();
        self.last_error = None;

        let folder = match &self.backup_folder_path {
            Some(f) => f.clone(),
            None => return,
        };

        let metadata_path = folder.join("metadata.json");
        if !metadata_path.exists() {
            self.last_error = Some(format!(
                "No metadata.json found in {}",
                folder.display()
            ));
            return;
        }

        log.info(format!("Loading backup metadata from {}...", metadata_path.display()));

        match std::fs::read_to_string(&metadata_path) {
            Ok(json_str) => match serde_json::from_str::<BackupMetadata>(&json_str) {
                Ok(meta) => {
                    log.info(format!(
                        "Backup: {} ({} partition(s), {} compression)",
                        meta.source_device,
                        meta.partitions.len(),
                        meta.compression_type,
                    ));

                    self.backup_metadata = Some(meta);

                    // Try to parse the mbr.bin from the backup folder for the
                    // full partition table view (disk signature, alignment,
                    // partition type bytes for browse buttons, etc.)
                    let mbr_bin_path = folder.join("mbr.bin");
                    if mbr_bin_path.exists() {
                        match File::open(&mbr_bin_path) {
                            Ok(file) => {
                                let mut reader = BufReader::new(file);
                                match PartitionTable::detect(&mut reader) {
                                    Ok(table) => {
                                        let alignment = detect_alignment(&table);
                                        self.partitions = table.partitions();
                                        log.info(format!(
                                            "Parsed {}: {} partition table, {} partition(s), alignment: {}",
                                            mbr_bin_path.file_name().unwrap_or_default().to_string_lossy(),
                                            table.type_name(),
                                            self.partitions.len(),
                                            alignment.alignment_type,
                                        ));
                                        self.alignment = Some(alignment);
                                        self.partition_table = Some(table);
                                    }
                                    Err(e) => {
                                        log.warn(format!("Could not parse mbr.bin: {e}"));
                                        // Fall back to metadata-only partition list
                                        self.load_partitions_from_metadata();
                                    }
                                }
                            }
                            Err(e) => {
                                log.warn(format!("Could not open mbr.bin: {e}"));
                                self.load_partitions_from_metadata();
                            }
                        }
                    } else {
                        // No mbr.bin, use metadata-only partition list
                        self.load_partitions_from_metadata();
                    }
                }
                Err(e) => {
                    let msg = format!("Failed to parse metadata.json: {e}");
                    log.error(&msg);
                    self.last_error = Some(msg);
                }
            },
            Err(e) => {
                let msg = format!("Cannot read {}: {e}", metadata_path.display());
                log.add(LogLevel::Error, &msg);
                self.last_error = Some(msg);
            }
        }
    }

    /// Fallback: populate partition list from backup metadata when mbr.bin
    /// is unavailable or unparseable.
    fn load_partitions_from_metadata(&mut self) {
        if let Some(meta) = &self.backup_metadata {
            self.partitions = meta
                .partitions
                .iter()
                .map(|p| PartitionInfo {
                    index: p.index,
                    type_name: p.type_name.clone(),
                    partition_type_byte: 0,
                    start_lba: p.start_lba,
                    size_bytes: p.original_size_bytes,
                    bootable: false,
                    is_logical: p.index >= 4,
                    is_extended_container: false,
                })
                .collect();
        }
    }

    fn run_inspect(&mut self, devices: &[DiskDevice], log: &mut LogPanel) {
        self.clear_results();

        let path = if let Some(img_path) = &self.image_file_path {
            img_path.clone()
        } else if let Some(idx) = self.selected_device_idx {
            if let Some(device) = devices.get(idx) {
                device.path.clone()
            } else {
                self.last_error = Some("Selected device not found".into());
                return;
            }
        } else {
            return;
        };

        log.info(format!("Inspecting {}...", path.display()));

        match File::open(&path) {
            Ok(file) => {
                let mut reader = BufReader::new(file);
                match PartitionTable::detect(&mut reader) {
                    Ok(table) => {
                        let alignment = detect_alignment(&table);
                        self.partitions = table.partitions();
                        log.info(format!(
                            "Detected {} partition table with {} partition(s)",
                            table.type_name(),
                            self.partitions.len()
                        ));
                        log.info(format!(
                            "Alignment: {} (first LBA: {})",
                            alignment.alignment_type, alignment.first_lba
                        ));
                        self.alignment = Some(alignment);
                        self.partition_table = Some(table);
                    }
                    Err(e) => {
                        let msg = format!("Failed to parse partition table: {e}");
                        log.error(&msg);
                        self.last_error = Some(msg);
                    }
                }
            }
            Err(e) => {
                let msg = format!("Cannot open {}: {e}", path.display());
                log.add(LogLevel::Error, &msg);
                self.last_error = Some(msg);
            }
        }
    }

    fn show_backup_metadata(&self, ui: &mut egui::Ui, meta: &BackupMetadata) {
        ui.horizontal(|ui| {
            ui.label(egui::RichText::new("Backup Info:").strong());
            ui.label(format!("Source: {}", meta.source_device));
        });
        ui.horizontal(|ui| {
            ui.label(egui::RichText::new("Created:").strong());
            ui.label(&meta.created);
            ui.label(egui::RichText::new("Table:").strong());
            ui.label(&meta.partition_table_type);
        });
        ui.horizontal(|ui| {
            ui.label(egui::RichText::new("Compression:").strong());
            ui.label(&meta.compression_type);
            ui.label(egui::RichText::new("Checksum:").strong());
            ui.label(&meta.checksum_type);
            if meta.sector_by_sector {
                ui.label("(sector-by-sector)");
            }
        });
        ui.horizontal(|ui| {
            ui.label(egui::RichText::new("Source Size:").strong());
            ui.label(partition::format_size(meta.source_size_bytes));
            ui.label(egui::RichText::new("Alignment:").strong());
            ui.label(&meta.alignment.detected_type);
        });
        ui.add_space(8.0);
    }

    fn show_results(&mut self, ui: &mut egui::Ui, devices: &[DiskDevice], log: &mut LogPanel) {
        // Partition table type - extract info before mutable borrow
        let (type_name, disk_sig) = if let Some(table) = &self.partition_table {
            (table.type_name().to_string(), table.disk_signature())
        } else {
            return;
        };

        ui.horizontal(|ui| {
            ui.label(egui::RichText::new("Partition Table:").strong());
            ui.label(&type_name);
            ui.label(format!("(disk signature: 0x{disk_sig:08X})"));
        });

        // Alignment info
        if let Some(alignment) = &self.alignment {
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new("Alignment:").strong());
                ui.label(format!("{}", alignment.alignment_type));
            });
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new("First LBA:").strong());
                ui.label(format!("{}", alignment.first_lba));
                if alignment.heads > 0 {
                    ui.label(format!(
                        "  CHS Geometry: {} heads x {} sectors/track",
                        alignment.heads, alignment.sectors_per_track
                    ));
                }
            });
        }

        ui.add_space(8.0);
        self.show_partition_list(ui, devices, log);
    }

    fn show_partition_list(&mut self, ui: &mut egui::Ui, devices: &[DiskDevice], log: &mut LogPanel) {
        if self.partitions.is_empty() {
            ui.label("No partitions found.");
            return;
        }

        // Browse request: (partition_index, offset, partition_type_byte)
        let mut browse_request: Option<(usize, u64, u8)> = None;

        egui::Grid::new("partition_table")
            .striped(true)
            .min_col_width(60.0)
            .show(ui, |ui| {
                // Header
                ui.label(egui::RichText::new("#").strong());
                ui.label(egui::RichText::new("Type").strong());
                ui.label(egui::RichText::new("Start LBA").strong());
                ui.label(egui::RichText::new("Size").strong());
                ui.label(egui::RichText::new("Boot").strong());
                ui.label(egui::RichText::new("").strong());
                ui.end_row();

                for part in &self.partitions {
                    let index_label = if part.is_logical {
                        format!("  {} (logical)", part.index)
                    } else if part.is_extended_container {
                        format!("{} (container)", part.index)
                    } else {
                        format!("{}", part.index)
                    };
                    if part.is_extended_container {
                        ui.label(egui::RichText::new(index_label).color(egui::Color32::GRAY));
                        ui.label(egui::RichText::new(&part.type_name).color(egui::Color32::GRAY));
                        ui.label(egui::RichText::new(format!("{}", part.start_lba)).color(egui::Color32::GRAY));
                        ui.label(egui::RichText::new(partition::format_size(part.size_bytes)).color(egui::Color32::GRAY));
                        ui.label("");
                        ui.label("");
                    } else {
                        ui.label(index_label);
                        ui.label(&part.type_name);
                        ui.label(format!("{}", part.start_lba));
                        ui.label(partition::format_size(part.size_bytes));
                        ui.label(if part.bootable { "Yes" } else { "" });
                        if is_fat_type(part.partition_type_byte) {
                            if ui.small_button("Browse").clicked() {
                                browse_request = Some((
                                    part.index,
                                    part.start_lba * 512,
                                    part.partition_type_byte,
                                ));
                            }
                        } else {
                            ui.label("");
                        }
                    }
                    ui.end_row();
                }
            });

        // Handle browse request outside the grid (avoids borrow issues)
        if let Some((part_index, offset, ptype)) = browse_request {
            self.open_browse(part_index, offset, ptype, devices, log);
        }
    }

    /// Resolve the browse source and open the filesystem browser.
    ///
    /// For raw image files / devices, the partition data lives at `offset`
    /// within the image. For backup folders, the partition data is stored
    /// as a separate file (raw, zstd-compressed, or CHD-compressed).
    fn open_browse(
        &mut self,
        part_index: usize,
        offset: u64,
        ptype: u8,
        devices: &[DiskDevice],
        log: &mut LogPanel,
    ) {
        // Case 1: device or raw image file
        let device_path = self
            .selected_device_idx
            .and_then(|idx| devices.get(idx))
            .map(|d| d.path.clone());
        let source_path = device_path.or_else(|| self.image_file_path.clone());
        if let Some(path) = source_path {
            log.info(format!(
                "Browsing partition {} from {} at offset {}",
                part_index, path.display(), offset,
            ));
            self.browse_view.open(path, offset, ptype);
            return;
        }

        // Case 2: backup folder — find the partition's data file
        let (folder, meta) = match (&self.backup_folder_path, &self.backup_metadata) {
            (Some(f), Some(m)) => (f.clone(), m.clone()),
            _ => {
                log.error(format!(
                    "partition-{}: no source available for browsing",
                    part_index,
                ));
                return;
            }
        };

        // Look up partition metadata
        let part_meta = match meta.partitions.iter().find(|p| p.index == part_index) {
            Some(pm) => pm,
            None => {
                log.error(format!(
                    "partition-{}: not found in backup metadata",
                    part_index,
                ));
                return;
            }
        };

        if part_meta.compressed_files.is_empty() {
            log.error(format!(
                "partition-{}: no data files listed in backup metadata",
                part_index,
            ));
            return;
        }

        // Split files not supported for browsing
        if part_meta.compressed_files.len() > 1 {
            log.warn(format!(
                "partition-{}: browsing split backup files is not supported (files: {})",
                part_index,
                part_meta.compressed_files.join(", "),
            ));
            return;
        }

        let data_file = &part_meta.compressed_files[0];
        let data_path = folder.join(data_file);

        if !data_path.exists() {
            log.error(format!(
                "partition-{}: data file not found: {}",
                part_index,
                data_path.display(),
            ));
            return;
        }

        match meta.compression_type.as_str() {
            "none" => {
                // Raw file — partition data starts at offset 0
                log.info(format!(
                    "Browsing partition {} from {}",
                    part_index, data_file,
                ));
                self.browse_view.open(data_path, 0, ptype);
            }
            other => {
                log.warn(format!(
                    "partition-{}: browsing {} compressed backups is not yet supported (file: {})",
                    part_index, other, data_file,
                ));
            }
        }
    }

}

/// Check if a partition type byte corresponds to a FAT filesystem.
fn is_fat_type(ptype: u8) -> bool {
    matches!(
        ptype,
        0x01 | 0x04 | 0x06 | 0x0B | 0x0C | 0x0E
            | 0x11 | 0x14 | 0x16 | 0x1B | 0x1C | 0x1E
    )
}
