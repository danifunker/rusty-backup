use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::Context;

use rusty_backup::backup::metadata::BackupMetadata;
use rusty_backup::clonezilla;
use rusty_backup::clonezilla::metadata::ClonezillaImage;
use rusty_backup::device::DiskDevice;
use rusty_backup::fs;
use rusty_backup::fs::fat::resize_fat_in_place;
use rusty_backup::partition::PartitionSizeOverride;
use rusty_backup::partition::{
    self, detect_alignment, PartitionAlignment, PartitionInfo, PartitionTable,
};
use rusty_backup::rbformats::vhd::{
    build_vhd_footer, export_partition_vhd, export_whole_disk_vhd, VHD_COOKIE,
};

use super::browse_view::BrowseView;
use super::progress::{LogLevel, LogPanel};

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
    /// VHD export: true if the export popup is open
    export_vhd_popup: bool,
    /// VHD export mode: true = whole disk, false = per partition
    export_whole_disk: bool,
    /// Per-partition sizing configuration for VHD export
    export_partition_configs: Vec<PartitionExportConfig>,
    /// VHD export background thread status
    export_status: Option<Arc<Mutex<ExportStatus>>>,
    /// Filesystem-computed minimum partition sizes (partition index → bytes)
    partition_min_sizes: HashMap<usize, u64>,
    /// Clonezilla image metadata (when backup is a Clonezilla image)
    clonezilla_image: Option<ClonezillaImage>,
    /// Block caches per Clonezilla partition (for browse support)
    block_caches: HashMap<usize, Arc<Mutex<clonezilla::block_cache::PartcloneBlockCache>>>,
    /// Background block cache scan status
    block_cache_scan: Option<Arc<Mutex<BlockCacheScan>>>,
    /// Seekable zstd cache files for native zstd backups
    seekable_cache_files: HashMap<usize, PathBuf>,
    /// Background seekable cache creation status (native zstd only)
    cache_status: Option<Arc<Mutex<CacheStatus>>>,
}

/// Status of a background VHD export operation.
struct ExportStatus {
    finished: bool,
    error: Option<String>,
    log_messages: Vec<String>,
    current_bytes: u64,
    total_bytes: u64,
    cancel_requested: bool,
}

/// Per-partition size choice for VHD export.
#[derive(Debug, Clone, Copy, PartialEq)]
enum ExportSizeChoice {
    Original,
    Minimum,
    Custom,
}

/// Per-partition export configuration.
#[derive(Debug, Clone)]
struct PartitionExportConfig {
    index: usize,
    type_name: String,
    original_size: u64,
    minimum_size: u64,
    choice: ExportSizeChoice,
    custom_size_mib: u32,
}

impl PartitionExportConfig {
    fn effective_size(&self) -> u64 {
        match self.choice {
            ExportSizeChoice::Original => self.original_size,
            ExportSizeChoice::Minimum => self.minimum_size,
            ExportSizeChoice::Custom => self.custom_size_mib as u64 * 1024 * 1024,
        }
    }
}

/// Status of a background seekable cache creation (native zstd backups).
struct CacheStatus {
    finished: bool,
    error: Option<String>,
    partition_index: usize,
    cache_path: Option<PathBuf>,
    current_bytes: u64,
    total_bytes: u64,
}

/// Status of a background block cache metadata scan (Clonezilla images).
struct BlockCacheScan {
    finished: bool,
    error: Option<String>,
    partition_index: usize,
    partition_type: u8,
    cache: Arc<Mutex<clonezilla::block_cache::PartcloneBlockCache>>,
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
            export_vhd_popup: false,
            export_whole_disk: true,
            export_partition_configs: Vec::new(),
            export_status: None,
            partition_min_sizes: HashMap::new(),
            clonezilla_image: None,
            block_caches: HashMap::new(),
            block_cache_scan: None,
            seekable_cache_files: HashMap::new(),
            cache_status: None,
        }
    }
}

impl InspectTab {
    pub fn get_loaded_backup(&self) -> Option<PathBuf> {
        self.backup_folder_path.clone()
    }

    pub fn has_backup(&self) -> bool {
        self.backup_folder_path.is_some()
    }

    pub fn load_backup(&mut self, path: &PathBuf) {
        if self.backup_folder_path.as_ref() != Some(path) {
            self.backup_folder_path = Some(path.clone());
            // Force reload on next show
            self.prev_backup_path = None;
        }
    }

    pub fn clear_backup(&mut self) {
        self.backup_folder_path = None;
        self.backup_metadata = None;
        self.prev_backup_path = None;
        self.partitions.clear();
        self.partition_table = None;
        self.alignment = None;
        self.browse_view.close();
        self.partition_min_sizes.clear();
        self.clonezilla_image = None;
        self.block_caches.clear();
        self.block_cache_scan = None;
        self.seekable_cache_files.clear();
        self.cache_status = None;
    }

    pub fn show(&mut self, ui: &mut egui::Ui, devices: &[DiskDevice], log: &mut LogPanel) {
        ui.heading("Inspect Disk / Image");
        ui.add_space(8.0);

        // Device / image selection
        ui.horizontal(|ui| {
            ui.label("Source:");

            let current_label = if let Some(path) = &self.backup_folder_path {
                format!("Backup: {}", path.display())
            } else if let Some(path) = &self.image_file_path {
                format!("VHD: {}", path.display())
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
                .height(400.0) // Allow more items to be visible without scrolling
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
                        .selectable_label(self.image_file_path.is_some(), "Open VHD File...")
                        .clicked()
                    {
                        if let Some(path) = super::file_dialog()
                            .add_filter("VHD Files", &["vhd", "hda"])
                            .pick_file()
                        {
                            self.selected_device_idx = None;
                            self.backup_folder_path = None;
                            self.image_file_path = Some(path);
                            self.clear_results();
                        }
                    }
                    if ui
                        .selectable_label(
                            self.backup_folder_path.is_some(),
                            "Open Backup Folder...",
                        )
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

        // Poll export status
        self.poll_export_status(log);

        // Re-inspect + Export VHD buttons
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

            // Export VHD button — available when we have partition data and no export running
            let has_partitions = !self.partitions.is_empty();
            let export_running = self.export_status.is_some();
            if ui
                .add_enabled(
                    has_partitions && !export_running,
                    egui::Button::new("Export VHD..."),
                )
                .clicked()
            {
                self.init_export_configs();
                self.export_vhd_popup = true;
            }

            if export_running {
                if ui.button("Cancel Export").clicked() {
                    if let Some(ref status_arc) = self.export_status {
                        if let Ok(mut s) = status_arc.lock() {
                            s.cancel_requested = true;
                        }
                    }
                    log.warn("Export cancellation requested...");
                }
            }
        });

        // Export progress bar
        if let Some(ref status_arc) = self.export_status {
            if let Ok(s) = status_arc.lock() {
                if !s.finished && s.total_bytes > 0 {
                    let fraction = s.current_bytes as f32 / s.total_bytes as f32;
                    let text = format!(
                        "Exporting VHD: {} / {} ({:.0}%)",
                        partition::format_size(s.current_bytes),
                        partition::format_size(s.total_bytes),
                        fraction * 100.0,
                    );
                    ui.add(egui::ProgressBar::new(fraction).text(text).animate(true));
                } else if !s.finished {
                    ui.horizontal(|ui| {
                        ui.spinner();
                        ui.label("Exporting VHD...");
                    });
                }
            }
        }

        // Export VHD popup
        if self.export_vhd_popup {
            self.show_export_vhd_popup(ui, devices, log);
        }

        ui.add_space(12.0);

        // Show error if any
        if let Some(err) = &self.last_error {
            ui.colored_label(
                egui::Color32::from_rgb(255, 100, 100),
                format!("Error: {err}"),
            );
            ui.add_space(8.0);
        }

        // Poll block cache scan status (Clonezilla)
        self.poll_block_cache_scan(log);

        // Show block cache scan progress
        if let Some(ref scan_arc) = self.block_cache_scan {
            if let Ok(s) = scan_arc.lock() {
                if !s.finished {
                    if let Ok(c) = s.cache.lock() {
                        if c.total_used_blocks > 0 {
                            let fraction =
                                c.scanned_used_blocks as f32 / c.total_used_blocks as f32;
                            let text = format!(
                                "Scanning partition {} metadata: {} / {} blocks ({:.0}%)",
                                s.partition_index,
                                c.scanned_used_blocks,
                                c.total_used_blocks,
                                fraction * 100.0,
                            );
                            ui.add(egui::ProgressBar::new(fraction).text(text).animate(true));
                        } else {
                            ui.horizontal(|ui| {
                                ui.spinner();
                                ui.label(format!(
                                    "Scanning partition {} metadata...",
                                    s.partition_index,
                                ));
                            });
                        }
                    }
                }
            }
        }

        // Poll seekable cache creation status (native zstd)
        self.poll_cache_status(log);

        // Show seekable cache creation progress
        if let Some(ref status_arc) = self.cache_status {
            if let Ok(s) = status_arc.lock() {
                if !s.finished && s.total_bytes > 0 {
                    let fraction = s.current_bytes as f32 / s.total_bytes as f32;
                    let text = format!(
                        "Building seekable zstd index for partition {}: {} / {} ({:.0}%)",
                        s.partition_index,
                        partition::format_size(s.current_bytes),
                        partition::format_size(s.total_bytes),
                        fraction * 100.0,
                    );
                    ui.add(egui::ProgressBar::new(fraction).text(text).animate(true));
                } else if !s.finished {
                    ui.horizontal(|ui| {
                        ui.spinner();
                        ui.label(format!(
                            "Building seekable zstd index for partition {}...",
                            s.partition_index
                        ));
                    });
                }
            }
        }

        // Show backup metadata if loaded from folder
        if let Some(meta) = &self.backup_metadata {
            self.show_backup_metadata(ui, meta);
        }

        // Show Clonezilla metadata if loaded
        if let Some(cz) = &self.clonezilla_image.clone() {
            self.show_clonezilla_metadata(ui, cz);
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

    fn init_export_configs(&mut self) {
        self.export_partition_configs.clear();
        for part in &self.partitions {
            if part.is_extended_container {
                continue;
            }
            let min_size = self
                .backup_metadata
                .as_ref()
                .and_then(|m| {
                    m.partitions
                        .iter()
                        .find(|pm| pm.index == part.index)
                        .map(|pm| pm.imaged_size_bytes)
                })
                .filter(|&sz| sz > 0)
                .or_else(|| self.partition_min_sizes.get(&part.index).copied())
                .unwrap_or(part.size_bytes);

            self.export_partition_configs.push(PartitionExportConfig {
                index: part.index,
                type_name: part.type_name.clone(),
                original_size: part.size_bytes,
                minimum_size: min_size,
                choice: ExportSizeChoice::Original,
                custom_size_mib: (part.size_bytes / (1024 * 1024)) as u32,
            });
        }
    }

    fn show_export_vhd_popup(
        &mut self,
        ui: &mut egui::Ui,
        devices: &[DiskDevice],
        log: &mut LogPanel,
    ) {
        egui::Window::new("Export VHD")
            .collapsible(false)
            .resizable(true)
            .default_width(500.0)
            .show(ui.ctx(), |ui| {
                ui.label("Export partitions as Fixed VHD files.");
                ui.add_space(4.0);

                ui.radio_value(
                    &mut self.export_whole_disk,
                    true,
                    "Whole Disk (single .vhd file)",
                );
                ui.radio_value(
                    &mut self.export_whole_disk,
                    false,
                    "Per Partition (one .vhd per partition)",
                );

                ui.add_space(8.0);

                // Per-partition sizing
                if !self.export_partition_configs.is_empty() {
                    ui.label(egui::RichText::new("Partition Sizes:").strong());
                    egui::Grid::new("export_partition_sizes")
                        .striped(true)
                        .min_col_width(50.0)
                        .show(ui, |ui| {
                            ui.label(egui::RichText::new("#").strong());
                            ui.label(egui::RichText::new("Type").strong());
                            ui.label(egui::RichText::new("Size Mode").strong());
                            ui.label(egui::RichText::new("Size (MiB)").strong());
                            ui.end_row();

                            for cfg in &mut self.export_partition_configs {
                                ui.label(format!("{}", cfg.index));
                                ui.label(&cfg.type_name);

                                let prev_choice = cfg.choice;
                                ui.horizontal(|ui| {
                                    ui.radio_value(
                                        &mut cfg.choice,
                                        ExportSizeChoice::Original,
                                        "Original",
                                    );
                                    // Only show Minimum if it differs from original
                                    if cfg.minimum_size < cfg.original_size {
                                        ui.radio_value(
                                            &mut cfg.choice,
                                            ExportSizeChoice::Minimum,
                                            "Minimum",
                                        );
                                    }
                                    ui.radio_value(
                                        &mut cfg.choice,
                                        ExportSizeChoice::Custom,
                                        "Custom",
                                    );
                                });

                                // When switching to Custom, initialize to minimum size
                                if cfg.choice == ExportSizeChoice::Custom
                                    && prev_choice != ExportSizeChoice::Custom
                                {
                                    cfg.custom_size_mib =
                                        (cfg.minimum_size / (1024 * 1024)).max(1) as u32;
                                }

                                if cfg.choice == ExportSizeChoice::Custom {
                                    let min_mib = (cfg.minimum_size / (1024 * 1024)).max(1) as u32;
                                    // Allow growing beyond original (up to 2 TiB VHD max)
                                    let max_mib = 2_097_152u32;
                                    ui.add(
                                        egui::DragValue::new(&mut cfg.custom_size_mib)
                                            .range(min_mib..=max_mib),
                                    );
                                } else {
                                    ui.label(format!("{}", cfg.effective_size() / (1024 * 1024)));
                                }
                                ui.end_row();
                            }
                        });
                    ui.add_space(4.0);
                }

                ui.add_space(8.0);

                ui.horizontal(|ui| {
                    if ui.button("Export...").clicked() {
                        self.export_vhd_popup = false;
                        self.start_export_vhd(devices, log);
                    }
                    if ui.button("Cancel").clicked() {
                        self.export_vhd_popup = false;
                    }
                });
            });
    }

    fn start_export_vhd(&mut self, devices: &[DiskDevice], log: &mut LogPanel) {
        // Collect partition sizing info
        let size_map: HashMap<usize, u64> = self
            .export_partition_configs
            .iter()
            .map(|cfg| (cfg.index, cfg.effective_size()))
            .collect();

        // Build partition size overrides for whole-disk export
        let partition_overrides: Vec<PartitionSizeOverride> = self
            .export_partition_configs
            .iter()
            .map(|cfg| {
                let start_lba = self
                    .partitions
                    .iter()
                    .find(|p| p.index == cfg.index)
                    .map(|p| p.start_lba)
                    .unwrap_or(0);
                PartitionSizeOverride::size_only(
                    cfg.index,
                    start_lba,
                    cfg.original_size,
                    cfg.effective_size(),
                )
            })
            .collect();

        // Compute total bytes for progress tracking
        let total_bytes: u64 = size_map.values().sum();

        let new_status = || {
            Arc::new(Mutex::new(ExportStatus {
                finished: false,
                error: None,
                log_messages: Vec::new(),
                current_bytes: 0,
                total_bytes,
                cancel_requested: false,
            }))
        };

        if self.export_whole_disk {
            // Pick a single file destination
            let dialog = super::file_dialog()
                .set_file_name("disk.vhd")
                .add_filter("VHD Files", &["vhd", "hda"]);
            let dest = match dialog.save_file() {
                Some(p) => p,
                None => return,
            };

            // Clonezilla whole-disk export
            if let Some(cz_image) = self.clonezilla_image.clone() {
                let source = match &self.backup_folder_path {
                    Some(f) => f.clone(),
                    None => {
                        log.error("No backup folder for Clonezilla export");
                        return;
                    }
                };
                let overrides = partition_overrides;
                let status = new_status();
                self.export_status = Some(Arc::clone(&status));

                log.info(format!(
                    "Exporting Clonezilla image as whole-disk VHD to {}...",
                    dest.display()
                ));

                std::thread::spawn(move || {
                    let status2 = Arc::clone(&status);
                    let status3 = Arc::clone(&status);
                    let result = rusty_backup::rbformats::vhd::export_clonezilla_disk_vhd(
                        &cz_image,
                        &source,
                        &dest,
                        &overrides,
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
            } else {
                // Native backup or raw image export
                let source_path = self
                    .backup_folder_path
                    .clone()
                    .or_else(|| self.image_file_path.clone())
                    .or_else(|| {
                        self.selected_device_idx
                            .and_then(|idx| devices.get(idx))
                            .map(|d| d.path.clone())
                    });
                let source = match source_path {
                    Some(p) => p,
                    None => {
                        log.error("No source available for export");
                        return;
                    }
                };

                let meta = self.backup_metadata.clone();
                let overrides = partition_overrides;
                let status = new_status();
                self.export_status = Some(Arc::clone(&status));

                log.info(format!("Exporting whole-disk VHD to {}...", dest.display()));

                std::thread::spawn(move || {
                    let status2 = Arc::clone(&status);
                    let status3 = Arc::clone(&status);
                    let result = export_whole_disk_vhd(
                        &source,
                        meta.as_ref(),
                        None,
                        &overrides,
                        &dest,
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
        } else {
            // Per-partition: pick a folder
            let dest_folder = match super::file_dialog().pick_folder() {
                Some(p) => p,
                None => return,
            };

            let source_folder = self.backup_folder_path.clone();
            let source_image = self.image_file_path.clone().or_else(|| {
                self.selected_device_idx
                    .and_then(|idx| devices.get(idx))
                    .map(|d| d.path.clone())
            });
            let meta = self.backup_metadata.clone();
            let partitions = self.partitions.clone();
            let cz_image = self.clonezilla_image.clone();

            let status = new_status();
            self.export_status = Some(Arc::clone(&status));

            log.info(format!(
                "Exporting per-partition VHDs to {}...",
                dest_folder.display()
            ));

            std::thread::spawn(move || {
                let result = (|| -> anyhow::Result<()> {
                    let mut overall_written: u64 = 0;

                    if let (Some(cz), Some(_folder)) = (&cz_image, &source_folder) {
                        // Clonezilla per-partition export
                        for cz_part in &cz.partitions {
                            if status.lock().map(|s| s.cancel_requested).unwrap_or(false) {
                                anyhow::bail!("export cancelled");
                            }
                            if cz_part.is_extended || cz_part.partclone_files.is_empty() {
                                continue;
                            }

                            let export_size = size_map
                                .get(&cz_part.index)
                                .copied()
                                .unwrap_or(cz_part.size_bytes());
                            let dest_path =
                                dest_folder.join(format!("partition-{}.vhd", cz_part.index));

                            if let Ok(mut s) = status.lock() {
                                s.log_messages.push(format!(
                                    "Exporting partition-{} ({}) to {}",
                                    cz_part.index,
                                    partition::format_size(export_size),
                                    dest_path.display()
                                ));
                            }

                            let base_written = overall_written;
                            let status_progress = Arc::clone(&status);
                            let status_cancel = Arc::clone(&status);
                            rusty_backup::rbformats::vhd::export_clonezilla_partition_vhd(
                                &cz_part.partclone_files,
                                &dest_path,
                                Some(export_size),
                                move |bytes| {
                                    if let Ok(mut s) = status_progress.lock() {
                                        s.current_bytes = base_written + bytes;
                                    }
                                },
                                move || {
                                    status_cancel
                                        .lock()
                                        .map(|s| s.cancel_requested)
                                        .unwrap_or(false)
                                },
                                |msg| {
                                    if let Ok(mut s) = status.lock() {
                                        s.log_messages.push(msg.to_string());
                                    }
                                },
                            )?;

                            overall_written += export_size;
                        }
                    } else if let (Some(folder), Some(meta)) = (&source_folder, &meta) {
                        // Backup folder: export each partition file
                        for pm in &meta.partitions {
                            if status.lock().map(|s| s.cancel_requested).unwrap_or(false) {
                                anyhow::bail!("export cancelled");
                            }
                            if pm.compressed_files.is_empty() {
                                continue;
                            }

                            let export_size = size_map
                                .get(&pm.index)
                                .copied()
                                .unwrap_or(pm.original_size_bytes);
                            let data_file = &pm.compressed_files[0];
                            let data_path = folder.join(data_file);
                            let dest_path = dest_folder.join(format!("partition-{}.vhd", pm.index));

                            if let Ok(mut s) = status.lock() {
                                s.log_messages.push(format!(
                                    "Exporting partition-{} ({}) to {}",
                                    pm.index,
                                    partition::format_size(export_size),
                                    dest_path.display()
                                ));
                            }

                            let base_written = overall_written;
                            let status_progress = Arc::clone(&status);
                            let status_cancel = Arc::clone(&status);
                            export_partition_vhd(
                                &data_path,
                                &meta.compression_type,
                                &dest_path,
                                Some(export_size),
                                move |bytes| {
                                    if let Ok(mut s) = status_progress.lock() {
                                        s.current_bytes = base_written + bytes;
                                    }
                                },
                                move || {
                                    status_cancel
                                        .lock()
                                        .map(|s| s.cancel_requested)
                                        .unwrap_or(false)
                                },
                                |msg| {
                                    if let Ok(mut s) = status.lock() {
                                        s.log_messages.push(msg.to_string());
                                    }
                                },
                            )?;

                            // Resize FAT filesystem if the partition size changed
                            if export_size != pm.original_size_bytes {
                                let new_sectors = (export_size / 512) as u32;
                                let mut rw = std::fs::OpenOptions::new()
                                    .read(true)
                                    .write(true)
                                    .open(&dest_path)?;
                                resize_fat_in_place(&mut rw, 0, new_sectors, &mut |msg| {
                                    if let Ok(mut s) = status.lock() {
                                        s.log_messages.push(msg.to_string());
                                    }
                                })?;
                            }

                            overall_written += export_size;
                        }
                    } else if let Some(image_path) = &source_image {
                        // Raw image/device: extract each partition by offset
                        for part in &partitions {
                            if status.lock().map(|s| s.cancel_requested).unwrap_or(false) {
                                anyhow::bail!("export cancelled");
                            }
                            if part.is_extended_container {
                                continue;
                            }

                            let export_size = size_map
                                .get(&part.index)
                                .copied()
                                .unwrap_or(part.size_bytes);
                            let dest_path =
                                dest_folder.join(format!("partition-{}.vhd", part.index));
                            let offset = part.start_lba * 512;

                            if let Ok(mut s) = status.lock() {
                                s.log_messages.push(format!(
                                    "Exporting partition-{} ({}) to {}",
                                    part.index,
                                    partition::format_size(export_size),
                                    dest_path.display()
                                ));
                            }

                            // Extract partition data to VHD
                            // Read at most the original partition size from the source
                            // to avoid reading into adjacent partitions or the VHD footer
                            let read_limit = export_size.min(part.size_bytes);
                            let file = std::fs::File::open(image_path)?;
                            let mut reader = std::io::BufReader::new(file);
                            reader.seek(std::io::SeekFrom::Start(offset))?;
                            let mut limited = reader.take(read_limit);

                            let mut writer =
                                std::io::BufWriter::new(std::fs::File::create(&dest_path)?);
                            let mut buf = vec![0u8; 256 * 1024];
                            let mut total: u64 = 0;
                            let base_written = overall_written;
                            loop {
                                if status.lock().map(|s| s.cancel_requested).unwrap_or(false) {
                                    anyhow::bail!("export cancelled");
                                }
                                let n = limited.read(&mut buf)?;
                                if n == 0 {
                                    break;
                                }
                                writer.write_all(&buf[..n])?;
                                total += n as u64;
                                if let Ok(mut s) = status.lock() {
                                    s.current_bytes = base_written + total;
                                }
                            }

                            // Pad with zeros if export_size > data read (growing)
                            if total < export_size {
                                let pad = export_size - total;
                                let zeros = vec![0u8; 256 * 1024];
                                let mut remaining = pad;
                                while remaining > 0 {
                                    let n = (remaining as usize).min(zeros.len());
                                    writer.write_all(&zeros[..n])?;
                                    remaining -= n as u64;
                                }
                                total = export_size;
                            }
                            writer.flush()?;

                            // Resize FAT filesystem if the partition size changed
                            if export_size != part.size_bytes {
                                let new_sectors = (export_size / 512) as u32;
                                resize_fat_in_place(
                                    writer.get_mut(),
                                    0,
                                    new_sectors,
                                    &mut |msg| {
                                        if let Ok(mut s) = status.lock() {
                                            s.log_messages.push(msg.to_string());
                                        }
                                    },
                                )?;
                                // Seek back to end for footer append
                                writer.seek(std::io::SeekFrom::Start(total))?;
                            }

                            // Append VHD footer
                            let footer = build_vhd_footer(total);
                            writer.write_all(&footer)?;
                            writer.flush()?;

                            overall_written += export_size;
                        }
                    } else {
                        anyhow::bail!("no source available for export");
                    }
                    Ok(())
                })();

                if let Ok(mut s) = status.lock() {
                    s.finished = true;
                    if let Err(e) = result {
                        s.error = Some(format!("{e:#}"));
                    }
                }
            });
        }
    }

    fn poll_export_status(&mut self, log: &mut LogPanel) {
        let status_arc = match &self.export_status {
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

        if status.finished {
            if let Some(err) = &status.error {
                log.error(format!("VHD export failed: {err}"));
            } else {
                log.info("VHD export completed successfully.");
            }
            drop(status);
            self.export_status = None;
        }
    }

    fn pick_backup_folder(&mut self) {
        if let Some(path) = super::file_dialog().pick_folder() {
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
        self.partition_min_sizes.clear();
        self.clonezilla_image = None;
        self.block_caches.clear();
        self.block_cache_scan = None;
        self.seekable_cache_files.clear();
        self.cache_status = None;
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
            // Try Clonezilla image detection
            if clonezilla::metadata::is_clonezilla_image(&folder) {
                self.load_clonezilla_image(&folder, log);
                return;
            }
            self.last_error = Some(format!("No metadata.json found in {}", folder.display()));
            return;
        }

        log.info(format!(
            "Loading backup metadata from {}...",
            metadata_path.display()
        ));

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

                                        // mbr.bin is only 512 bytes, so EBR chain
                                        // parsing will have silently failed. Merge
                                        // logical partitions from metadata.
                                        self.merge_logical_partitions_from_metadata(log);
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
                    partition_type_string: p.partition_type_string.clone(),
                })
                .collect();
        }
    }

    /// After parsing mbr.bin (which is only 512 bytes and cannot contain the
    /// EBR chain), supplement the partition list with any logical partitions
    /// found in backup metadata.
    fn merge_logical_partitions_from_metadata(&mut self, log: &mut LogPanel) {
        let meta = match &self.backup_metadata {
            Some(m) => m,
            None => return,
        };

        // Check if there's an extended container but no logical partitions
        // were parsed (expected since mbr.bin has no EBR data).
        let has_container = self.partitions.iter().any(|p| p.is_extended_container);
        let has_logicals = self.partitions.iter().any(|p| p.is_logical);
        if !has_container || has_logicals {
            return;
        }

        // Add logical partitions from metadata (index >= 4 by convention)
        let mut added = 0;
        for pm in &meta.partitions {
            if pm.index >= 4 {
                self.partitions.push(PartitionInfo {
                    index: pm.index,
                    type_name: pm.type_name.clone(),
                    partition_type_byte: pm.partition_type_byte,
                    start_lba: pm.start_lba,
                    size_bytes: pm.original_size_bytes,
                    bootable: false,
                    is_logical: true,
                    is_extended_container: false,
                    partition_type_string: pm.partition_type_string.clone(),
                });
                added += 1;
            }
        }

        if added > 0 {
            log.info(format!(
                "Added {added} logical partition(s) from backup metadata"
            ));
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
                let file_size = file.metadata().map(|m| m.len()).unwrap_or(0);
                let mut reader = BufReader::new(file);

                // Detect VHD: check if the last 512 bytes contain the "conectix" cookie
                let data_size = if file_size >= 512 {
                    if let Ok(mut f) = File::open(&path) {
                        if f.seek(SeekFrom::End(-512)).is_ok() {
                            let mut cookie = [0u8; 8];
                            if f.read_exact(&mut cookie).is_ok() && &cookie == VHD_COOKIE {
                                let ds = file_size - 512;
                                log.info(format!("Detected Fixed VHD (data: {} bytes)", ds,));
                                Some(ds)
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };

                // Use a limited reader if VHD was detected
                let detect_result = if let Some(ds) = data_size {
                    // Reset reader to start and limit to data portion
                    let _ = reader.seek(SeekFrom::Start(0));
                    let mut limited = reader.take(ds);
                    PartitionTable::detect(&mut limited)
                } else {
                    PartitionTable::detect(&mut reader)
                };

                match detect_result {
                    Ok(mut table) => {
                        // Fix up superfloppy size: seek(End(0)) returns 0 for macOS devices
                        if let PartitionTable::None { size_bytes, .. } = &mut table {
                            if *size_bytes == 0 {
                                if let Ok(f) = File::open(&path) {
                                    if let Ok(real_size) =
                                        rusty_backup::os::get_file_size(&f, &path)
                                    {
                                        *size_bytes = real_size;
                                    }
                                }
                            }
                        }
                        let alignment = detect_alignment(&table);
                        self.partitions = table.partitions();

                        // For APM disks, probe "Apple_HFS" partitions to show the actual
                        // HFS variant (HFS vs HFS+ vs HFSX) in the type name.
                        if matches!(table, PartitionTable::Apm(_)) {
                            for part in &mut self.partitions {
                                if part.partition_type_string.as_deref() == Some("Apple_HFS") {
                                    if let Ok(f) = File::open(&path) {
                                        let mut br = BufReader::new(f);
                                        let detected =
                                            fs::probe_apple_hfs_type(&mut br, part.start_lba * 512);
                                        if detected == "HFS+" || detected == "HFSX" {
                                            part.type_name = part.type_name.replace(
                                                "Apple_HFS",
                                                &format!("Apple_HFS ({detected})"),
                                            );
                                        }
                                    }
                                }
                            }
                        }

                        if matches!(table, PartitionTable::None { .. }) {
                            log.info(format!(
                                "Detected superfloppy (no partition table) with {} partition(s)",
                                self.partitions.len()
                            ));
                        } else {
                            log.info(format!(
                                "Detected {} partition table with {} partition(s)",
                                table.type_name(),
                                self.partitions.len()
                            ));
                        }
                        log.info(format!(
                            "Alignment: {} (first LBA: {})",
                            alignment.alignment_type, alignment.first_lba
                        ));
                        self.alignment = Some(alignment);
                        self.partition_table = Some(table);

                        // Compute minimum partition sizes via filesystem analysis
                        for part in &self.partitions {
                            if part.is_extended_container {
                                continue;
                            }
                            if let Ok(f) = File::open(&path) {
                                if let Some(min_size) = fs::effective_partition_size(
                                    BufReader::new(f),
                                    part.start_lba * 512,
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

    fn load_clonezilla_image(&mut self, folder: &PathBuf, log: &mut LogPanel) {
        log.info(format!("Detected Clonezilla image in {}", folder.display()));

        match clonezilla::metadata::load(folder) {
            Ok(cz_image) => {
                log.info(format!(
                    "Clonezilla image: {} partition(s), disk: {}, source size: {}",
                    cz_image.partitions.len(),
                    cz_image.disk_name,
                    partition::format_size(cz_image.source_size_bytes),
                ));

                // Parse MBR from the loaded image
                let mut mbr_reader = std::io::Cursor::new(&cz_image.mbr_bytes[..]);
                match PartitionTable::detect(&mut mbr_reader) {
                    Ok(table) => {
                        let alignment = detect_alignment(&table);
                        log.info(format!(
                            "MBR: {} partition table, alignment: {}",
                            table.type_name(),
                            alignment.alignment_type,
                        ));
                        self.alignment = Some(alignment);
                        self.partition_table = Some(table);
                    }
                    Err(e) => {
                        log.warn(format!("Could not parse Clonezilla MBR: {e}"));
                    }
                }

                // Convert Clonezilla partitions to PartitionInfo for the grid
                self.partitions = cz_image
                    .partitions
                    .iter()
                    .map(|p| PartitionInfo {
                        index: p.index,
                        type_name: p.type_name(),
                        partition_type_byte: p.partition_type_byte,
                        start_lba: p.start_lba,
                        size_bytes: p.size_bytes(),
                        bootable: p.bootable,
                        is_logical: p.is_logical,
                        is_extended_container: p.is_extended,
                        partition_type_string: None,
                    })
                    .collect();

                // Compute minimum sizes by reading partclone headers
                for cz_part in &cz_image.partitions {
                    if cz_part.is_extended || cz_part.partclone_files.is_empty() {
                        continue;
                    }
                    match clonezilla::partclone::read_partclone_header(&cz_part.partclone_files) {
                        Ok(header) => {
                            let used = header.used_size();
                            if used > 0 && used < cz_part.size_bytes() {
                                self.partition_min_sizes.insert(cz_part.index, used);
                                log.info(format!(
                                    "Partition {} ({}): min {} (used {} of {} blocks, block size {})",
                                    cz_part.index,
                                    cz_part.device_name,
                                    partition::format_size(used),
                                    header.used_blocks,
                                    header.total_blocks,
                                    header.block_size,
                                ));
                            }
                        }
                        Err(e) => {
                            log.warn(format!(
                                "Could not read partclone header for {}: {e}",
                                cz_part.device_name,
                            ));
                        }
                    }
                }

                // Check for existing metadata cache files
                let mut cached_count = 0;
                for cz_part in &cz_image.partitions {
                    let cache_path =
                        folder.join(format!("_{}.metadata.cache", cz_part.device_name));
                    if cache_path.exists() {
                        cached_count += 1;
                    }
                }
                if cached_count > 0 {
                    log.info(format!(
                        "Found {} cached metadata file(s) for browsing",
                        cached_count,
                    ));
                }

                self.clonezilla_image = Some(cz_image);
            }
            Err(e) => {
                let msg = format!("Failed to parse Clonezilla image: {e:#}");
                log.error(&msg);
                self.last_error = Some(msg);
            }
        }
    }

    fn show_clonezilla_metadata(&self, ui: &mut egui::Ui, cz: &ClonezillaImage) {
        ui.horizontal(|ui| {
            ui.label(egui::RichText::new("Clonezilla Image:").strong());
            ui.label(format!("Disk: {}", cz.disk_name));
        });
        ui.horizontal(|ui| {
            ui.label(egui::RichText::new("Source Size:").strong());
            ui.label(format!(
                "{} ({} bytes)",
                format_size_decimal(cz.source_size_bytes),
                format_bytes_grouped(cz.source_size_bytes),
            ));
            if cz.heads > 0 {
                ui.label(format!(
                    "  CHS: {} / {} / {}",
                    cz.cylinders, cz.heads, cz.sectors_per_track
                ));
            }
        });
        // Show creation date from image info (first line)
        if let Some(first_line) = cz.image_info.lines().next() {
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new("Created:").strong());
                ui.label(first_line);
            });
        }
        ui.add_space(8.0);
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
        // Minimum restore size: sum of all partitions' imaged sizes, plus
        // the offset of the first partition (to account for the MBR/GPT area).
        let min_data_bytes: u64 = meta.partitions.iter().map(|pm| pm.imaged_size_bytes).sum();
        let pre_partition_bytes = meta.alignment.first_partition_lba * 512;
        let min_restore_bytes = min_data_bytes + pre_partition_bytes;

        ui.horizontal(|ui| {
            if min_restore_bytes > 0 {
                ui.label(egui::RichText::new("Minimum Size:").strong());
                ui.label(format!(
                    "{} ({} bytes)",
                    format_size_decimal(min_restore_bytes),
                    format_bytes_grouped(min_restore_bytes),
                ));
            } else if meta.source_size_bytes > 0 {
                ui.label(egui::RichText::new("Source Size:").strong());
                ui.label(partition::format_size(meta.source_size_bytes));
            }
            ui.label(egui::RichText::new("Alignment:").strong());
            ui.label(&meta.alignment.detected_type);
        });

        ui.add_space(8.0);
    }

    fn show_results(&mut self, ui: &mut egui::Ui, devices: &[DiskDevice], log: &mut LogPanel) {
        // Partition table type - extract info before mutable borrow
        let (type_name, disk_sig, is_superfloppy) = if let Some(table) = &self.partition_table {
            (
                table.type_name().to_string(),
                table.disk_signature(),
                matches!(table, PartitionTable::None { .. }),
            )
        } else {
            return;
        };

        ui.horizontal(|ui| {
            ui.label(egui::RichText::new("Partition Table:").strong());
            if is_superfloppy {
                ui.label("None (superfloppy)");
            } else {
                ui.label(&type_name);
                ui.label(format!("(disk signature: 0x{disk_sig:08X})"));
            }
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

    fn show_partition_list(
        &mut self,
        ui: &mut egui::Ui,
        devices: &[DiskDevice],
        log: &mut LogPanel,
    ) {
        if self.partitions.is_empty() {
            ui.label("No partitions found.");
            return;
        }

        // Browse request: (partition_index, offset, partition_type_byte)
        let mut browse_request: Option<(usize, u64, u8, Option<String>)> = None;

        egui::Grid::new("partition_table")
            .striped(true)
            .min_col_width(60.0)
            .show(ui, |ui| {
                // Header
                ui.label(egui::RichText::new("#").strong());
                ui.label(egui::RichText::new("Type").strong());
                ui.label(egui::RichText::new("Start LBA").strong());
                ui.label(egui::RichText::new("Size").strong());
                if self.backup_metadata.is_some() || !self.partition_min_sizes.is_empty() {
                    ui.label(egui::RichText::new("Min Size").strong());
                }
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
                        ui.label(
                            egui::RichText::new(format!("{}", part.start_lba))
                                .color(egui::Color32::GRAY),
                        );
                        ui.label(
                            egui::RichText::new(partition::format_size(part.size_bytes))
                                .color(egui::Color32::GRAY),
                        );
                        if self.backup_metadata.is_some() || !self.partition_min_sizes.is_empty() {
                            if let Some(meta) = &self.backup_metadata {
                                // Sum imaged_size_bytes of all logical partitions
                                let logical_sum: u64 = meta
                                    .partitions
                                    .iter()
                                    .filter(|pm| pm.index >= 4)
                                    .map(|pm| pm.imaged_size_bytes)
                                    .sum();
                                if logical_sum > 0 {
                                    ui.label(
                                        egui::RichText::new(partition::format_size(logical_sum))
                                            .color(egui::Color32::GRAY),
                                    );
                                } else {
                                    ui.label("");
                                }
                            } else {
                                ui.label("");
                            }
                        }
                        ui.label("");
                        ui.label("");
                    } else {
                        ui.label(index_label);
                        ui.label(&part.type_name);
                        ui.label(format!("{}", part.start_lba));
                        ui.label(partition::format_size(part.size_bytes));
                        if self.backup_metadata.is_some() || !self.partition_min_sizes.is_empty() {
                            let min_size = self
                                .backup_metadata
                                .as_ref()
                                .and_then(|m| {
                                    m.partitions
                                        .iter()
                                        .find(|pm| pm.index == part.index)
                                        .map(|pm| pm.imaged_size_bytes)
                                })
                                .filter(|&sz| sz > 0 && sz < part.size_bytes)
                                .or_else(|| {
                                    self.partition_min_sizes
                                        .get(&part.index)
                                        .copied()
                                        .filter(|&sz| sz < part.size_bytes)
                                });
                            if let Some(sz) = min_size {
                                ui.label(partition::format_size(sz));
                            } else {
                                ui.label("");
                            }
                        }
                        ui.label(if part.bootable { "Yes" } else { "" });
                        if is_browsable_type(part.partition_type_byte)
                            || is_fat_name(&part.type_name)
                            || is_browsable_type_string(part.partition_type_string.as_deref())
                            || is_browsable_superfloppy(part.partition_type_byte, &part.type_name)
                        {
                            if ui.small_button("Browse").clicked() {
                                // Use the stored type byte, or infer one
                                // from the name for old backups that didn't
                                // store partition_type_byte.
                                let ptype = if part.partition_type_byte != 0 {
                                    part.partition_type_byte
                                } else {
                                    infer_fat_type_byte(&part.type_name)
                                };
                                browse_request = Some((
                                    part.index,
                                    part.start_lba * 512,
                                    ptype,
                                    part.partition_type_string.clone(),
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
        if let Some((part_index, offset, ptype, type_string)) = browse_request {
            self.open_browse(part_index, offset, ptype, type_string, devices, log);
        }
    }

    /// Resolve the browse source and open the filesystem browser.
    ///
    /// For raw image files / devices, the partition data lives at `offset`
    /// within the image. For backup folders, the partition data is stored
    /// as a separate file (raw, zstd-compressed, or CHD-compressed).
    /// For Clonezilla images, data is in partclone format and needs a
    /// seekable zstd cache for browsing.
    fn open_browse(
        &mut self,
        part_index: usize,
        offset: u64,
        ptype: u8,
        partition_type_string: Option<String>,
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
                part_index,
                path.display(),
                offset,
            ));
            self.browse_view
                .open(path, offset, ptype, partition_type_string);
            return;
        }

        // Case 3: Clonezilla image — use seekable zstd cache
        if self.clonezilla_image.is_some() {
            self.open_browse_clonezilla(part_index, ptype, log);
            return;
        }

        // Case 2: native backup folder — find the partition's data file
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
                self.browse_view
                    .open(data_path, 0, ptype, partition_type_string.clone());
            }
            "zstd" => {
                // Zstd-compressed backup — create seekable cache for browsing
                self.open_browse_via_seekable_cache(
                    part_index,
                    ptype,
                    partition_type_string,
                    &data_path,
                    &folder,
                    &format!("partition-{}", part_index),
                    log,
                );
            }
            other => {
                log.warn(format!(
                    "partition-{}: browsing {} compressed backups is not yet supported (file: {})",
                    part_index, other, data_file,
                ));
            }
        }
    }

    /// Open browse for a Clonezilla partition using seekable zstd cache.
    fn open_browse_clonezilla(&mut self, part_index: usize, ptype: u8, log: &mut LogPanel) {
        let cz_image = match &self.clonezilla_image {
            Some(cz) => cz.clone(),
            None => return,
        };
        let folder = match &self.backup_folder_path {
            Some(f) => f.clone(),
            None => return,
        };

        let cz_part = match cz_image.partitions.iter().find(|p| p.index == part_index) {
            Some(p) => p.clone(),
            None => {
                log.error(format!(
                    "partition-{}: not found in Clonezilla image",
                    part_index,
                ));
                return;
            }
        };

        if cz_part.partclone_files.is_empty() {
            log.error(format!("partition-{}: no partclone data files", part_index,));
            return;
        }

        // Check if block cache already exists in memory
        if let Some(cache) = self.block_caches.get(&part_index) {
            if let Ok(c) = cache.lock() {
                if c.state == clonezilla::block_cache::CacheState::Ready {
                    drop(c);
                    log.info(format!(
                        "Browsing partition {} from block cache",
                        part_index
                    ));
                    self.browse_view.open_partclone(Arc::clone(cache), ptype);
                    return;
                }
            }
        }

        // Check if a scan is already running
        if self.block_cache_scan.is_some() {
            log.warn("A metadata scan is already in progress. Please wait.");
            return;
        }

        // Try to load persisted cache from disk
        let cache_path = folder.join(format!("_{}.metadata.cache", cz_part.device_name));
        if cache_path.exists() {
            match clonezilla::block_cache::PartcloneBlockCache::load_from_file(
                &cache_path,
                cz_part.partclone_files.clone(),
            ) {
                Ok(loaded) => {
                    log.info(format!(
                        "Loaded cached metadata for partition {} ({} blocks)",
                        part_index,
                        loaded.cached_block_count(),
                    ));
                    let cache = Arc::new(Mutex::new(loaded));
                    self.block_caches.insert(part_index, Arc::clone(&cache));
                    self.browse_view.open_partclone(cache, ptype);
                    return;
                }
                Err(e) => {
                    log::warn!("Failed to load metadata cache: {e}, will re-scan");
                    let _ = std::fs::remove_file(&cache_path);
                }
            }
        }

        // Start background metadata scan
        log.info(format!(
            "Scanning metadata for partition {} ({})...",
            part_index,
            partition::format_size(cz_part.size_bytes()),
        ));

        let cache = Arc::new(Mutex::new(
            clonezilla::block_cache::PartcloneBlockCache::new(cz_part.partclone_files.clone()),
        ));

        let scan = Arc::new(Mutex::new(BlockCacheScan {
            finished: false,
            error: None,
            partition_index: part_index,
            partition_type: ptype,
            cache: Arc::clone(&cache),
        }));
        self.block_cache_scan = Some(Arc::clone(&scan));

        let cache_for_thread = Arc::clone(&cache);
        std::thread::spawn(move || {
            let result =
                clonezilla::block_cache::scan_metadata(&cache_for_thread, ptype, Some(&cache_path));
            if let Ok(mut s) = scan.lock() {
                s.finished = true;
                if let Err(e) = result {
                    s.error = Some(format!("{e:#}"));
                }
            }
        });
    }

    /// Open browse for a native zstd-compressed backup via seekable cache.
    fn open_browse_via_seekable_cache(
        &mut self,
        part_index: usize,
        ptype: u8,
        partition_type_string: Option<String>,
        data_path: &PathBuf,
        folder: &PathBuf,
        cache_name: &str,
        log: &mut LogPanel,
    ) {
        // Check if seekable cache already exists
        if let Some(cache_path) = self.seekable_cache_files.get(&part_index).cloned() {
            if cache_path.exists() {
                log.info(format!(
                    "Browsing partition {} from cached seekable file",
                    part_index,
                ));
                self.browse_view
                    .open(cache_path, 0, ptype, partition_type_string.clone());
                return;
            }
            // Cache file was deleted (stale) — remove from map and recreate
            self.seekable_cache_files.remove(&part_index);
        }

        if self.cache_status.is_some() {
            log.warn("A seekable cache is already being created. Please wait.");
            return;
        }

        let cache_path = folder.join(format!("_{cache_name}.seekable.zst"));

        // Get file size for progress
        let total_bytes = std::fs::metadata(data_path).map(|m| m.len()).unwrap_or(0);

        log.info(format!(
            "Creating seekable cache for partition {}...",
            part_index,
        ));

        let status = Arc::new(Mutex::new(CacheStatus {
            finished: false,
            error: None,
            partition_index: part_index,
            cache_path: Some(cache_path.clone()),
            current_bytes: 0,
            total_bytes,
        }));
        self.cache_status = Some(Arc::clone(&status));

        let data_path = data_path.clone();
        std::thread::spawn(move || {
            let result = create_seekable_cache_from_zstd(&data_path, &cache_path, &status);
            if let Ok(mut s) = status.lock() {
                s.finished = true;
                if let Err(e) = result {
                    s.error = Some(format!("{e:#}"));
                }
            }
        });
    }

    fn poll_block_cache_scan(&mut self, log: &mut LogPanel) {
        let scan_arc = match &self.block_cache_scan {
            Some(s) => Arc::clone(s),
            None => return,
        };

        let Ok(scan) = scan_arc.lock() else {
            return;
        };

        if scan.finished {
            let part_index = scan.partition_index;
            let ptype = scan.partition_type;

            if let Some(err) = &scan.error {
                log.error(format!(
                    "Metadata scan failed for partition {}: {err}",
                    part_index,
                ));
            } else {
                log.info(format!(
                    "Metadata scan complete for partition {}.",
                    part_index,
                ));
                let cache = Arc::clone(&scan.cache);
                drop(scan);
                self.block_caches.insert(part_index, Arc::clone(&cache));
                self.block_cache_scan = None;

                // Auto-open the browser
                self.browse_view.open_partclone(cache, ptype);
                return;
            }

            drop(scan);
            self.block_cache_scan = None;
        }
    }

    fn poll_cache_status(&mut self, log: &mut LogPanel) {
        let status_arc = match &self.cache_status {
            Some(s) => Arc::clone(s),
            None => return,
        };

        let Ok(status) = status_arc.lock() else {
            return;
        };

        if status.finished {
            let part_index = status.partition_index;
            if let Some(err) = &status.error {
                log.error(format!(
                    "Failed to create seekable cache for partition {}: {err}",
                    part_index,
                ));
            } else if let Some(cache_path) = &status.cache_path {
                log.info(format!(
                    "Seekable cache ready for partition {}. Click Browse again.",
                    part_index,
                ));
                self.seekable_cache_files
                    .insert(part_index, cache_path.clone());
            }
            drop(status);
            self.cache_status = None;
        }
    }
}

/// A `Read` wrapper that counts bytes consumed from the inner reader.
///
/// Used to track how many compressed bytes have been read from the input zstd
/// file, so that progress can be reported as a fraction of the compressed size
/// rather than the (much larger) decompressed size.
struct CountingRead<R> {
    inner: R,
    count: Arc<Mutex<u64>>,
}

impl<R: Read> Read for CountingRead<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        if let Ok(mut c) = self.count.lock() {
            *c += n as u64;
        }
        Ok(n)
    }
}

/// Create a seekable zstd cache from a native zstd-compressed backup file.
///
/// Streams: zstd file → zstd decompress → zeekstd encode → cache file.
///
/// Progress (`status.current_bytes`) tracks compressed input bytes consumed so
/// that the fraction `current_bytes / total_bytes` stays in [0, 1].
fn create_seekable_cache_from_zstd(
    data_path: &PathBuf,
    cache_path: &PathBuf,
    status: &Arc<Mutex<CacheStatus>>,
) -> anyhow::Result<()> {
    use std::io::BufWriter;

    let input_file =
        File::open(data_path).with_context(|| format!("failed to open {}", data_path.display()))?;

    // Wrap the compressed input in a counting reader so we can track how many
    // compressed bytes have been consumed for accurate progress reporting.
    let compressed_count = Arc::new(Mutex::new(0u64));
    let counting = CountingRead {
        inner: BufReader::new(input_file),
        count: Arc::clone(&compressed_count),
    };
    let mut decoder = zstd::Decoder::new(counting).context("failed to create zstd decoder")?;

    let output_file = File::create(cache_path)
        .with_context(|| format!("failed to create cache file: {}", cache_path.display()))?;
    let buf_writer = BufWriter::new(output_file);

    let opts = zeekstd::EncodeOptions::new()
        .frame_size_policy(zeekstd::FrameSizePolicy::Uncompressed(2 * 1024 * 1024));
    let mut encoder = opts
        .into_encoder(buf_writer)
        .map_err(|e| anyhow::anyhow!("failed to create zeekstd encoder: {e}"))?;

    let mut buf = vec![0u8; 256 * 1024];

    loop {
        let n = decoder.read(&mut buf)?;
        if n == 0 {
            break;
        }
        encoder
            .compress(&buf[..n])
            .map_err(|e| anyhow::anyhow!("zeekstd compress error: {e}"))?;

        // Update progress with compressed bytes consumed (stays within [0, total_bytes])
        if let Ok(compressed) = compressed_count.lock() {
            if let Ok(mut s) = status.lock() {
                s.current_bytes = *compressed;
            }
        }
    }

    encoder
        .finish()
        .map_err(|e| anyhow::anyhow!("zeekstd finish error: {e}"))?;

    Ok(())
}

/// Check if a partition type byte corresponds to a browsable filesystem.
fn is_browsable_type(ptype: u8) -> bool {
    matches!(
        ptype,
        0x01 | 0x04 | 0x06 | 0x07 | 0x0B | 0x0C | 0x0E | 0x11 | 0x14 | 0x16 | 0x1B | 0x1C | 0x1E
    )
}

/// Check if an APM partition type string corresponds to a browsable filesystem.
fn is_browsable_type_string(type_str: Option<&str>) -> bool {
    matches!(
        type_str,
        Some("Apple_HFS" | "Apple_HFSX" | "Apple_HFS+" | "Apple_UNIX_SRVR2")
    )
}

/// Check if a superfloppy (type byte 0) has a browsable filesystem hint.
fn is_browsable_superfloppy(ptype: u8, type_name: &str) -> bool {
    if ptype != 0 {
        return false;
    }
    matches!(
        type_name,
        "FAT" | "HFS" | "HFS+" | "NTFS" | "exFAT" | "Unknown"
    )
}

/// Fallback check: detect FAT from the human-readable type name string.
/// Used for older backups where `partition_type_byte` was not stored.
fn is_fat_name(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    lower.contains("fat")
}

/// Infer an MBR partition type byte from the human-readable type name.
/// Used for older backups that didn't store `partition_type_byte`.
fn infer_fat_type_byte(name: &str) -> u8 {
    let lower = name.to_ascii_lowercase();
    if lower.contains("fat32") {
        0x0C // FAT32 LBA
    } else if lower.contains("fat16") {
        0x06 // FAT16
    } else if lower.contains("fat12") {
        0x01 // FAT12
    } else if lower.contains("fat") {
        0x0C // Default to FAT32 LBA
    } else {
        0
    }
}

/// Format a byte count using base-1000 (SI) units, matching how storage
/// media is marketed (e.g. "8 GB" on an SD card = 8,000,000,000 bytes).
fn format_size_decimal(bytes: u64) -> String {
    const GB: f64 = 1_000_000_000.0;
    const MB: f64 = 1_000_000.0;
    let b = bytes as f64;
    if b >= GB {
        format!("{:.2} GB", b / GB)
    } else {
        format!("{:.1} MB", b / MB)
    }
}

/// Format a byte count with digit grouping for readability
/// (e.g. 1,048,576).
fn format_bytes_grouped(bytes: u64) -> String {
    let s = bytes.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(ch);
    }
    result.chars().rev().collect()
}
