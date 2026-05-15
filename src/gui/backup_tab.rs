use std::collections::HashSet;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use rusty_backup::backup::{
    metadata::SizePolicy, BackupConfig, BackupProgress, ChecksumType, CompressionType,
    LogLevel as BackupLogLevel,
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
    backup_running: bool,
    backup_progress: Option<Arc<Mutex<BackupProgress>>>,
    /// Auto-loaded partition info for the selected source
    source_partitions: Vec<PartitionInfo>,
    /// Which partitions are selected for backup (by partition index)
    selected_partitions: HashSet<usize>,
    /// Minimum sizes per partition (indexed by partition index), from filesystem analysis
    partition_min_sizes: std::collections::HashMap<usize, u64>,
    /// Defragmented-minimum sizes per partition (HFS+/HFSX only — the smaller
    /// value the clone pipeline can achieve when defrag is opted in).
    partition_defrag_min_sizes: std::collections::HashMap<usize, u64>,
    /// Fragmentation percent per partition (HFS+/HFS, populated after the
    /// min-size walk completes). Drives the auto-check threshold on the
    /// per-partition Defrag checkbox.
    partition_fragmentation: std::collections::HashMap<usize, f32>,
    /// User-controlled per-partition Defrag toggle. Auto-checked when
    /// fragmentation >= 90%. Only meaningful for filesystems with a
    /// defragmenting writer (HFS+/HFSX); the disabled checkbox shown on
    /// other layout-preserving filesystems writes here but the backup
    /// engine ignores it (until those FSes get a real clone pipeline).
    partition_defrag_enabled: std::collections::HashMap<usize, bool>,
    /// Tracks which partition indices have already had their default
    /// (frag >= 90%) auto-applied. Prevents re-overriding the user's
    /// explicit toggle when min-size is re-run.
    partition_defrag_default_applied: std::collections::HashSet<usize>,
    /// Partitions whose minimum size requires an expensive volume walk; the
    /// VHD-export popup surfaces a "Calc min" button per index.
    deferred_min_sizes: std::collections::HashMap<usize, &'static str>,
    /// Currently-running per-partition min-size worker threads.
    pending_min_size_calcs: std::collections::HashMap<
        usize,
        Arc<Mutex<rusty_backup::model::min_size_runner::MinSizeStatus>>,
    >,
    /// Last phase string logged for each in-flight min-size calc, so we
    /// can emit a log line on each transition without spamming.
    last_logged_min_size_phase: std::collections::HashMap<usize, String>,
    /// Open file handle for the source (kept alive so min-size workers can clone it).
    source_file: Option<Arc<File>>,
    /// Human-readable description of the partition table type for the
    /// currently-loaded source (e.g. "MBR", "GPT", "APM"). Shown above
    /// the partition list.
    source_partition_table_desc: Option<String>,
    /// When the user clicks Start Backup with `resize_partitions` on but one
    /// or more selected partitions still need their min-size walked, we
    /// fire those calcs and arm this flag. As soon as the pending set is
    /// empty `show()` clears the flag and calls `start_backup` for real.
    pending_backup_after_min_sizes: bool,
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
            chd_hd_control: {
                let mut ctl = ChdOptionsControl::new(ChdProfile::Hd);
                // Restore last-used custom codec/hunk choices so users don't
                // re-tune every launch. Defaults/MiSTer presets are pure
                // functions so we don't persist mode — restored values land
                // in the Custom slot, ready to apply once the user flips into
                // Custom mode.
                let cfg = UpdateConfig::load();
                if let Some(spec) = cfg.last_chd_codecs.as_deref() {
                    if let Ok(codecs) = parse_codec_string(spec) {
                        ctl.custom.codecs = codecs;
                    }
                }
                if let Some(hs) = cfg.last_chd_hunk_size {
                    if hs % 512 == 0 {
                        ctl.custom.hunk_size = hs;
                    }
                }
                ctl
            },
            backup_running: false,
            backup_progress: None,
            source_partitions: Vec::new(),
            selected_partitions: HashSet::new(),
            partition_min_sizes: std::collections::HashMap::new(),
            partition_defrag_min_sizes: std::collections::HashMap::new(),
            partition_fragmentation: std::collections::HashMap::new(),
            partition_defrag_enabled: std::collections::HashMap::new(),
            partition_defrag_default_applied: std::collections::HashSet::new(),
            deferred_min_sizes: std::collections::HashMap::new(),
            pending_min_size_calcs: std::collections::HashMap::new(),
            last_logged_min_size_phase: std::collections::HashMap::new(),
            source_file: None,
            source_partition_table_desc: None,
            pending_backup_after_min_sizes: false,
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
    pub fn show(&mut self, ui: &mut egui::Ui, ctx: &mut TabContext, progress: &mut ProgressState) {
        // Poll background backup thread
        self.poll_progress(ctx, progress);

        // Poll any pending per-partition minimum-size calculations
        self.poll_min_size_calcs(ctx);

        // If the user clicked Start Backup while min-size calcs were still
        // pending, kick off the real backup as soon as the last one lands.
        if self.pending_backup_after_min_sizes
            && self.pending_min_size_calcs.is_empty()
            && !self.backup_running
        {
            self.pending_backup_after_min_sizes = false;
            self.start_backup(ctx);
        }

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
                                        "adf", "hdf", "adz", "hdz",
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
        // For devices we don't auto-scan (would prompt for elevation on every
        // dropdown change). Offer a button to load partitions on demand.
        if !self.backup_running
            && self.source_partitions.is_empty()
            && self.partition_load_error.is_none()
            && self.selected_device_idx.is_some()
            && self.image_file_path.is_none()
        {
            ui.add_space(4.0);
            ui.horizontal(|ui| {
                if ui.button("Load partition info").on_hover_text(
                    "Read the partition table from the selected device (may prompt for administrator credentials)",
                ).clicked() {
                    self.scan_source_partitions(ctx);
                }
            });
        }

        if !self.source_partitions.is_empty() {
            ui.add_space(4.0);
            if let Some(desc) = &self.source_partition_table_desc {
                ui.label(format!(
                    "Partition table: {} ({} partition{})",
                    desc,
                    self.source_partitions.len(),
                    if self.source_partitions.len() == 1 {
                        ""
                    } else {
                        "s"
                    },
                ));
            }
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
            let mut min_size_calc_request: Option<usize> = None;
            let mut defrag_toggles: Vec<(usize, bool)> = Vec::new();
            ui.add_enabled_ui(controls_enabled, |ui| {
                egui::Grid::new("backup_partitions_grid")
                    .num_columns(7)
                    .spacing([12.0, 4.0])
                    .show(ui, |ui| {
                        ui.label(egui::RichText::new("Include").strong());
                        ui.label(egui::RichText::new("#").strong());
                        ui.label(egui::RichText::new("Type").strong());
                        ui.label(egui::RichText::new("Size").strong());
                        ui.label(egui::RichText::new("Min Size").strong());
                        ui.label(egui::RichText::new("Frag.").strong())
                            .on_hover_text("Percent of files with data in more than one extent");
                        ui.label(egui::RichText::new("Compact").strong())
                            .on_hover_text(
                                "Compact Space re-emits every allocated fork back-to-back, \
                                 closing the holes between files and producing a smaller backup. \
                                 Drastically increases backup time — recommended only when the \
                                 Min Size column shows meaningful savings vs. the in-place trim. \
                                 Available for HFS+/HFSX today; other layout-preserving \
                                 filesystems show the toggle as not implemented.",
                            );
                        ui.end_row();

                        for part in &self.source_partitions {
                            if part.is_extended_container {
                                continue;
                            }
                            let mut selected = self.selected_partitions.contains(&part.index);
                            if ui.checkbox(&mut selected, "").changed() {
                                if selected {
                                    self.selected_partitions.insert(part.index);
                                } else {
                                    self.selected_partitions.remove(&part.index);
                                }
                            }
                            ui.label(format!("{}", part.index));
                            ui.label(&part.type_name);
                            ui.label(partition::format_size(part.size_bytes));

                            // Min-size cell: value (toggle-aware), spinner, "Calc min" button,
                            // or blank.
                            let effective = self.effective_min_size(
                                part.index,
                                part.partition_type_byte,
                                part.partition_type_string.as_deref(),
                            );
                            if let Some(min) = effective.filter(|&sz| sz < part.size_bytes) {
                                ui.label(partition::format_size(min));
                            } else if let Some(pending) =
                                self.pending_min_size_calcs.get(&part.index)
                            {
                                let phase = pending
                                    .lock()
                                    .map(|s| s.phase.clone())
                                    .unwrap_or_else(|_| "...".to_string());
                                ui.add(egui::Spinner::new()).on_hover_text(phase);
                            } else if self.deferred_min_sizes.contains_key(&part.index) {
                                if ui
                                    .small_button("Calc min")
                                    .on_hover_text(
                                        "Compute minimum size (requires a filesystem walk)",
                                    )
                                    .clicked()
                                {
                                    min_size_calc_request = Some(part.index);
                                }
                            } else {
                                ui.label("");
                            }

                            // Fragmentation %
                            match self.partition_fragmentation.get(&part.index).copied() {
                                Some(pct) => {
                                    ui.label(format!("{:.0}%", pct));
                                }
                                None => {
                                    ui.label("");
                                }
                            }

                            // Defrag checkbox: only shown for layout-preserving
                            // filesystems where defrag would matter; disabled
                            // (with "not implemented" tooltip) when the FS
                            // doesn't have a clone pipeline yet.
                            let is_layout = fs::is_layout_preserving_fs(
                                part.partition_type_byte,
                                part.partition_type_string.as_deref(),
                            );
                            let has_clone = fs::has_defragmenting_writer(
                                part.partition_type_byte,
                                part.partition_type_string.as_deref(),
                            );
                            if is_layout {
                                let mut enabled = self
                                    .partition_defrag_enabled
                                    .get(&part.index)
                                    .copied()
                                    .unwrap_or(false);
                                let resp = ui
                                    .add_enabled(has_clone, egui::Checkbox::new(&mut enabled, ""));
                                if !has_clone {
                                    resp.on_hover_text(
                                        "Compact Space not yet implemented for this filesystem",
                                    );
                                } else if resp.changed() {
                                    defrag_toggles.push((part.index, enabled));
                                }
                            } else {
                                // FAT/NTFS/exFAT: the packing CompactReader
                                // already emits a defragmented layout.
                                ui.label("");
                            }
                            ui.end_row();
                        }
                    });
            });
            for (idx, val) in defrag_toggles {
                self.partition_defrag_enabled.insert(idx, val);
            }
            if let Some(idx) = min_size_calc_request {
                self.start_min_size_calc(idx, ctx);
            }
            ui.add_space(4.0);
        }
        if let Some(err) = &self.partition_load_error {
            ui.colored_label(egui::Color32::from_rgb(200, 150, 100), err);
        }

        // Keep the UI ticking while background min-size workers run so the
        // spinner animates and results appear without requiring mouse input.
        if !self.pending_min_size_calcs.is_empty() {
            ui.ctx()
                .request_repaint_after(std::time::Duration::from_millis(80));
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
                ui.radio_value(
                    &mut self.compression_type,
                    CompressionType::Chd,
                    "CHD (Hard Disk)",
                );
                // DVD CHD is intentionally NOT offered here — DVDs are
                // optical media, handled by the Optical tab. The backup
                // tab is for hard disks only.
                ui.radio_value(&mut self.compression_type, CompressionType::Vhd, "VHD");
                ui.radio_value(&mut self.compression_type, CompressionType::Zstd, "zstd");
                ui.radio_value(&mut self.compression_type, CompressionType::None, "None");
            });

            // CHD codec / hunk-size knobs (only visible when CHD is selected)
            if matches!(self.compression_type, CompressionType::Chd) {
                ui.label(
                    egui::RichText::new("Output: single CHD file (chdman/MAME compatible).")
                        .small()
                        .weak(),
                );
                super::chd_options_ui::show(
                    ui,
                    "backup_chd",
                    ChdProfile::Hd,
                    &mut self.chd_hd_control,
                );
            }

            // Sector-by-sector option
            ui.checkbox(
                &mut self.sector_by_sector,
                "Sector-by-sector copy (includes all blank space)",
            );

            // Resize option. Sector-by-sector preserves the source byte-for-
            // byte (free space + unrecognized regions), so resizing would
            // defeat the point — force-disabled and unchecked while it's on.
            if self.sector_by_sector {
                self.resize_partitions = false;
            }
            ui.add_enabled_ui(!self.sector_by_sector, |ui| {
                ui.horizontal(|ui| {
                    ui.checkbox(
                        &mut self.resize_partitions,
                        "Resize partitions to minimum size",
                    );
                    if self.sector_by_sector {
                        ui.label("(not available with sector-by-sector copy)");
                    }
                });
            });

            // Split archives. Hidden for CHD/DVD (chdman + MAME don't
            // recognise split CHDs); disabled with a note for VHD (always
            // one .vhd file).
            let split_hidden = matches!(
                self.compression_type,
                CompressionType::Chd | CompressionType::Dvd
            );
            let split_enabled =
                !split_hidden && !matches!(self.compression_type, CompressionType::Vhd);
            if !split_hidden {
                ui.horizontal(|ui| {
                    ui.add_enabled_ui(split_enabled, |ui| {
                        ui.checkbox(&mut self.split_archives, "Split archives");
                        if self.split_archives && split_enabled {
                            ui.label("Split size (MiB):");
                            ui.add(
                                egui::DragValue::new(&mut self.split_size_mib).range(100..=64000),
                            );
                        }
                    });
                    if !split_enabled && matches!(self.compression_type, CompressionType::Vhd) {
                        ui.label("(not available for VHD)");
                    }
                });
            }

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
                    _ => true,
                };
                let can_start = has_source
                    && self.destination_folder.is_some()
                    && !self.backup_name.is_empty()
                    && has_partitions
                    && chd_ok;

                let button_label = if self.pending_backup_after_min_sizes {
                    "Computing minimum sizes..."
                } else {
                    "Start Backup"
                };
                let button_enabled = can_start && !self.pending_backup_after_min_sizes;
                if ui
                    .add_enabled(button_enabled, egui::Button::new(button_label))
                    .clicked()
                {
                    if self.compression_type == CompressionType::Vhd {
                        // Scan partitions if not already loaded
                        if self.source_partitions.is_empty() {
                            self.scan_source_partitions(ctx);
                        }
                        self.init_vhd_configs();
                        self.vhd_popup_open = true;
                    } else if self.compression_type == CompressionType::Chd
                        && !self.sector_by_sector
                        && self.resize_partitions
                        && self.has_deferred_selected_partitions()
                    {
                        // Resize-to-min was requested but one or more selected
                        // partitions still need a filesystem walk. Fire those
                        // calcs now; the poll loop in show() will call
                        // start_backup once they all land.
                        self.kick_off_pending_min_size_calcs(ctx);
                        self.pending_backup_after_min_sizes = true;
                    } else {
                        self.start_backup(ctx);
                    }
                }
                if self.pending_backup_after_min_sizes {
                    ui.add(egui::Spinner::new())
                        .on_hover_text("Waiting for minimum-size calculations to finish");
                    if ui.button("Cancel").clicked() {
                        // Calcs already running keep running (their results
                        // still populate `partition_min_sizes` and are useful
                        // later); we just stop auto-launching the backup.
                        self.pending_backup_after_min_sizes = false;
                        ctx.log.info("Auto-backup canceled; minimum-size calculations will still complete in the background");
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
            let _wake = rusty_backup::os::wakelock::acquire("Rusty Backup: whole-disk VHD backup");
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
        progress_state.record_sample();

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

    /// True if any selected (non-extended) partition has a deferred minimum
    /// size that hasn't been computed yet. Used to decide whether to fire
    /// background calcs before kicking off a resize-to-minimum CHD backup.
    fn has_deferred_selected_partitions(&self) -> bool {
        self.source_partitions.iter().any(|part| {
            !part.is_extended_container
                && self.selected_partitions.contains(&part.index)
                && !self.partition_min_sizes.contains_key(&part.index)
                && self.deferred_min_sizes.contains_key(&part.index)
        })
    }

    /// Fire `start_min_size_calc` for every selected partition that still
    /// has a deferred min size and isn't already running. Safe to call when
    /// some calcs are already in flight — duplicates are filtered out by
    /// `start_min_size_calc` itself.
    fn kick_off_pending_min_size_calcs(&mut self, ctx: &mut TabContext) {
        let indices: Vec<usize> = self
            .source_partitions
            .iter()
            .filter(|p| {
                !p.is_extended_container
                    && self.selected_partitions.contains(&p.index)
                    && !self.partition_min_sizes.contains_key(&p.index)
                    && self.deferred_min_sizes.contains_key(&p.index)
            })
            .map(|p| p.index)
            .collect();
        for idx in indices {
            self.start_min_size_calc(idx, ctx);
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
    /// Effective per-partition min size, honouring the per-partition Defrag
    /// checkbox: when defrag is enabled AND this filesystem has a
    /// defragmenting writer, prefer the (smaller) defragmented floor;
    /// otherwise use the in-place trim point. Falls back through.
    fn effective_min_size(
        &self,
        idx: usize,
        partition_type: u8,
        partition_type_string: Option<&str>,
    ) -> Option<u64> {
        let in_place = self.partition_min_sizes.get(&idx).copied();
        let defrag = self.partition_defrag_min_sizes.get(&idx).copied();
        let has_clone = fs::has_defragmenting_writer(partition_type, partition_type_string);
        let enabled = self
            .partition_defrag_enabled
            .get(&idx)
            .copied()
            .unwrap_or(false);
        if has_clone && enabled {
            defrag.or(in_place)
        } else {
            in_place.or(defrag)
        }
    }

    /// Common post-min-size-walk bookkeeping shared between the auto-load
    /// path (`load_partitions`) and the worker-poll path. Stores in-place
    /// + defrag floors, fragmentation %, and auto-checks the Compact Space
    /// toggle when compacting would save at least
    /// [`Self::COMPACT_AUTOCHECK_SAVINGS_PCT`] of the in-place trim size on
    /// a clone-capable filesystem. The user can still toggle the box
    /// manually to override the default — e.g. to squeeze the backup onto
    /// a slightly-smaller destination disk when the auto threshold didn't
    /// fire.
    fn record_min_size_result(
        &mut self,
        idx: usize,
        in_place: Option<u64>,
        defragmented: Option<u64>,
        fragmentation_percent: Option<f32>,
        partition_size: u64,
        partition_type: u8,
        partition_type_string: Option<&str>,
    ) {
        let in_place_clamped = in_place.map(|v| v.min(partition_size));
        let defrag_clamped = defragmented.map(|v| v.min(partition_size));
        if let Some(v) = in_place_clamped {
            self.partition_min_sizes.insert(idx, v);
        }
        if let Some(v) = defrag_clamped {
            self.partition_defrag_min_sizes.insert(idx, v);
        }
        if let Some(pct) = fragmentation_percent {
            self.partition_fragmentation.insert(idx, pct);
        }
        // Auto-check criterion: clone-capable FS AND space savings >=
        // threshold. File fragmentation % is shown for context but isn't
        // the decision signal — a volume with 0% file fragmentation can
        // still have meaningful inter-file holes that compaction recovers.
        if !self.partition_defrag_default_applied.contains(&idx)
            && fs::has_defragmenting_writer(partition_type, partition_type_string)
        {
            if let (Some(ip), Some(df)) = (in_place_clamped, defrag_clamped) {
                if ip > 0 && df < ip {
                    let savings_pct = 100.0 * (ip - df) as f32 / ip as f32;
                    if savings_pct >= Self::COMPACT_AUTOCHECK_SAVINGS_PCT {
                        self.partition_defrag_enabled.insert(idx, true);
                    }
                }
            }
        }
        self.partition_defrag_default_applied.insert(idx);
    }

    /// Minimum space-savings (% of in-place trim size) at which Compact
    /// Space is auto-checked. 5% catches volumes with a few GiB of holes
    /// on a 30+ GiB partition while staying off for the common case of a
    /// nearly-perfectly-packed disk where compaction would just be a long
    /// no-op.
    const COMPACT_AUTOCHECK_SAVINGS_PCT: f32 = 5.0;

    fn poll_min_size_calcs(&mut self, ctx: &mut TabContext) {
        // Emit a log line whenever a worker's `phase` string changes, so
        // long HFS+ catalog walks don't look like a hang. The phase set is
        // small (Opening filesystem -> Computing last data byte ->
        // Computing defragmented minimum) plus the one-shot
        // "open_filesystem failed: <err>" leak from `partition_minimum_size`.
        let phase_updates: Vec<(usize, Vec<String>)> = self
            .pending_min_size_calcs
            .iter()
            .filter_map(|(idx, status)| {
                status.lock().ok().map(|mut s| {
                    let drained = std::mem::take(&mut s.phase_log);
                    (*idx, drained)
                })
            })
            .collect();
        for (idx, phases) in phase_updates {
            for phase in phases {
                if phase.is_empty() {
                    continue;
                }
                ctx.log.info(format!("Partition {idx}: {phase}"));
                self.last_logged_min_size_phase.insert(idx, phase);
            }
        }
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
                    } else {
                        let part = self.source_partitions.iter().find(|p| p.index == idx);
                        let (part_size, ptype, pstring) = part
                            .map(|p| {
                                (
                                    p.size_bytes,
                                    p.partition_type_byte,
                                    p.partition_type_string.clone(),
                                )
                            })
                            .unwrap_or((u64::MAX, 0, None));
                        let frag_pct = s.fragmentation_percent;
                        let last_phase = s.phase.clone();
                        let in_place = s.result;
                        let defrag = s.defragmented_min;
                        drop(s);
                        self.record_min_size_result(
                            idx,
                            in_place,
                            defrag,
                            frag_pct,
                            part_size,
                            ptype,
                            pstring.as_deref(),
                        );
                        if let Some(min) = self.effective_min_size(idx, ptype, pstring.as_deref()) {
                            let suffix = if self
                                .partition_defrag_enabled
                                .get(&idx)
                                .copied()
                                .unwrap_or(false)
                            {
                                " (space compacted via clone)"
                            } else {
                                ""
                            };
                            let frag = frag_pct
                                .map(|p| format!(", fragmentation {:.1}%", p))
                                .unwrap_or_default();
                            ctx.log.info(format!(
                                "Partition {idx}: minimum size {} (computed){suffix}{frag}",
                                partition::format_size(min),
                            ));
                            for cfg in &mut self.vhd_partition_configs {
                                if cfg.index == idx {
                                    cfg.minimum_size = min;
                                    cfg.deferred_fs = None;
                                }
                            }
                        } else {
                            let s_phase = last_phase;
                            ctx.log.info(format!(
                                "Partition {idx}: filesystem could not be opened for minimum-size calculation (last phase: {s_phase})",
                            ));
                        }
                    }
                }
                self.deferred_min_sizes.remove(&idx);
                self.last_logged_min_size_phase.remove(&idx);
            }
        }
    }

    /// Scan source partition table and compute minimum partition sizes.
    /// For devices, this uses OS-level elevation (may prompt for credentials).
    fn scan_source_partitions(&mut self, ctx: &mut TabContext) {
        self.source_partitions.clear();
        self.selected_partitions.clear();
        self.partition_min_sizes.clear();
        self.partition_defrag_min_sizes.clear();
        self.partition_fragmentation.clear();
        self.partition_defrag_enabled.clear();
        self.partition_defrag_default_applied.clear();
        self.deferred_min_sizes.clear();
        self.pending_min_size_calcs.clear();
        self.source_file = None;
        self.source_partition_table_desc = None;
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
                self.source_partition_table_desc = Some(table_desc.clone());
                ctx.log.info(format!(
                    "Found {} partition(s) ({})",
                    self.source_partitions.len(),
                    table_desc,
                ));

                // Compute minimum partition sizes using filesystem analysis.
                // Cheap filesystems (FAT/NTFS/exFAT) compute eagerly here; expensive
                // ones (HFS/HFS+/ext/btrfs/ProDOS) defer until the user clicks
                // "Calc min" in the per-partition resize popup.
                let probes: Vec<(usize, u64, u64, u8, Option<String>, bool)> = self
                    .source_partitions
                    .iter()
                    .map(|p| {
                        (
                            p.index,
                            p.start_lba * 512,
                            p.size_bytes,
                            p.partition_type_byte,
                            p.partition_type_string.clone(),
                            p.is_extended_container,
                        )
                    })
                    .collect();
                for (idx, offset, size_bytes, type_byte, type_str, ext_container) in probes {
                    if ext_container {
                        continue;
                    }
                    let Ok(clone) = reader.get_ref().try_clone() else {
                        continue;
                    };
                    let result = fs::partition_minimum_size(
                        BufReader::new(clone),
                        offset,
                        type_byte,
                        type_str.as_deref(),
                        size_bytes,
                        false,
                        None,
                        &|_| {},
                    );
                    match result {
                        fs::MinimumResult::Computed {
                            in_place,
                            defragmented,
                            fragmentation_percent,
                        } => {
                            self.record_min_size_result(
                                idx,
                                in_place,
                                defragmented,
                                fragmentation_percent,
                                size_bytes,
                                type_byte,
                                type_str.as_deref(),
                            );
                            if let Some(min) =
                                self.effective_min_size(idx, type_byte, type_str.as_deref())
                            {
                                let suffix = if self
                                    .partition_defrag_enabled
                                    .get(&idx)
                                    .copied()
                                    .unwrap_or(false)
                                {
                                    " (space compacted via clone)"
                                } else {
                                    ""
                                };
                                let frag = fragmentation_percent
                                    .map(|p| format!(", fragmentation {:.1}%", p))
                                    .unwrap_or_default();
                                ctx.log.info(format!(
                                    "Partition {idx}: minimum size {} (original {}){suffix}{frag}",
                                    partition::format_size(min),
                                    partition::format_size(size_bytes),
                                ));
                            }
                        }
                        fs::MinimumResult::Deferred { fs_name } => {
                            ctx.log.info(format!(
                                "Partition {idx}: need to calculate minimum size due to filesystem {fs_name} (click \"Calc min\" to compute)",
                            ));
                            self.deferred_min_sizes.insert(idx, fs_name);
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
        self.partition_min_sizes.clear();
        self.partition_defrag_min_sizes.clear();
        self.partition_fragmentation.clear();
        self.partition_defrag_enabled.clear();
        self.partition_defrag_default_applied.clear();
        self.deferred_min_sizes.clear();
        self.pending_min_size_calcs.clear();
        self.source_partition_table_desc = None;
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
                        self.source_partition_table_desc = Some(table_desc.clone());
                        ctx.log.info(format!(
                            "Source has {} partition(s) ({})",
                            self.source_partitions.len(),
                            table_desc,
                        ));

                        // Compute cheap minimum sizes, defer expensive ones.
                        // Re-open the file for each probe so each partition's
                        // call gets an independent seek cursor; format wrapping
                        // (DMG/VHDX dynamic) is intentionally skipped here —
                        // raw VHD and plain images have their tables at the
                        // expected offsets.
                        let probes: Vec<(usize, u64, u64, u8, Option<String>, bool)> = self
                            .source_partitions
                            .iter()
                            .map(|p| {
                                (
                                    p.index,
                                    p.start_lba * 512,
                                    p.size_bytes,
                                    p.partition_type_byte,
                                    p.partition_type_string.clone(),
                                    p.is_extended_container,
                                )
                            })
                            .collect();
                        for (idx, offset, size_bytes, type_byte, type_str, ext_container) in probes
                        {
                            if ext_container {
                                continue;
                            }
                            let probe_reader = match File::open(&path) {
                                Ok(f) => BufReader::new(f),
                                Err(_) => continue,
                            };
                            let result = fs::partition_minimum_size(
                                probe_reader,
                                offset,
                                type_byte,
                                type_str.as_deref(),
                                size_bytes,
                                false,
                                None,
                                &|_| {},
                            );
                            match result {
                                fs::MinimumResult::Computed {
                                    in_place,
                                    defragmented,
                                    fragmentation_percent,
                                } => {
                                    self.record_min_size_result(
                                        idx,
                                        in_place,
                                        defragmented,
                                        fragmentation_percent,
                                        size_bytes,
                                        type_byte,
                                        type_str.as_deref(),
                                    );
                                    if let Some(min) =
                                        self.effective_min_size(idx, type_byte, type_str.as_deref())
                                    {
                                        let suffix = if self
                                            .partition_defrag_enabled
                                            .get(&idx)
                                            .copied()
                                            .unwrap_or(false)
                                        {
                                            " (space compacted via clone)"
                                        } else {
                                            ""
                                        };
                                        let frag = fragmentation_percent
                                            .map(|p| format!(", fragmentation {:.1}%", p))
                                            .unwrap_or_default();
                                        ctx.log.info(format!(
                                            "Partition {idx}: minimum size {} (original {}){suffix}{frag}",
                                            partition::format_size(min),
                                            partition::format_size(size_bytes),
                                        ));
                                    }
                                }
                                fs::MinimumResult::Deferred { fs_name } => {
                                    ctx.log.info(format!(
                                        "Partition {idx}: need to calculate minimum size due to filesystem {fs_name} (click \"Calc min\" to compute)",
                                    ));
                                    self.deferred_min_sizes.insert(idx, fs_name);
                                }
                            }
                        }

                        // Stash an unwrapped raw handle so the worker thread
                        // can clone it later. For image files this is the raw
                        // (still-wrapped) file; the worker will re-detect the
                        // format if needed.
                        if let Ok(f) = File::open(&path) {
                            self.source_file = Some(Arc::new(f));
                        }
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

        // Single-file CHD: derive size_policy + per-partition target sizes
        // from the simple "Resize partitions to minimum size" checkbox. When
        // the box is on, every partition with a known minimum gets MinPlus20;
        // partitions whose minimum hasn't been computed fall back to Original.
        //
        // HFS+/HFSX partitions are intentionally *skipped* from the list:
        // their shrink is performed by the HFS+ defrag-clone pipeline that
        // `run_backup` activates via `shrink_to_minimum`. The clone uses the
        // partition's defragmented minimum (a true lower bound) — emitting a
        // MinPlus20-of-the-in-place-trim entry here would either no-op
        // (MinPlus20 ≥ original collapses to original) or override the
        // clone target.
        let resize_for_chd = matches!(self.compression_type, CompressionType::Chd)
            && !self.sector_by_sector
            && self.resize_partitions;
        let (size_policy, partition_target_sizes) = if resize_for_chd {
            let mut targets: Vec<(usize, u64)> = Vec::new();
            let mut used_min_plus_20 = false;
            for part in &self.source_partitions {
                if part.is_extended_container {
                    continue;
                }
                // HFS+/HFSX partitions with the per-partition Defrag toggle
                // ON are handled by the clone pipeline (via
                // `defrag_partition_indices`), so don't seed a target_size
                // here — the clone emits its own size. With Defrag OFF, fall
                // through to the in-place trim path: seed a target so the
                // staging path resizes the partition to its in-place minimum.
                let defrag_enabled = self
                    .partition_defrag_enabled
                    .get(&part.index)
                    .copied()
                    .unwrap_or(false);
                let has_clone = fs::has_defragmenting_writer(
                    part.partition_type_byte,
                    part.partition_type_string.as_deref(),
                );
                if defrag_enabled && has_clone {
                    continue;
                }
                let effective = self.effective_min_size(
                    part.index,
                    part.partition_type_byte,
                    part.partition_type_string.as_deref(),
                );
                let target = match effective {
                    Some(min) if min < part.size_bytes => {
                        used_min_plus_20 = true;
                        SizeMode::MinPlus20.effective_size(part.size_bytes, min, 0)
                    }
                    _ => part.size_bytes,
                };
                targets.push((part.index, target));
            }
            let policy = if used_min_plus_20 {
                SizePolicy::MinPlus20
            } else {
                SizePolicy::Original
            };
            let targets_opt = if targets.is_empty() {
                None
            } else {
                Some(targets)
            };
            (Some(policy), targets_opt)
        } else {
            (None, None)
        };

        let split_disabled_for_compression = matches!(
            self.compression_type,
            CompressionType::Vhd | CompressionType::Chd | CompressionType::Dvd
        );
        // Per-partition defrag opt-in: only HFS+/HFSX partitions whose
        // checkbox is checked AND whose FS has a defragmenting writer.
        let mut defrag_set: std::collections::HashSet<usize> = std::collections::HashSet::new();
        for part in &self.source_partitions {
            if part.is_extended_container {
                continue;
            }
            if !self
                .partition_defrag_enabled
                .get(&part.index)
                .copied()
                .unwrap_or(false)
            {
                continue;
            }
            if !fs::has_defragmenting_writer(
                part.partition_type_byte,
                part.partition_type_string.as_deref(),
            ) {
                continue;
            }
            defrag_set.insert(part.index);
        }

        // Send the toggle-aware effective minimum (defrag if checked, else
        // in-place) for every partition that has a precomputed value. The
        // engine uses this to seed `defragmented_min_sizes[i]` and to
        // short-circuit volume walks it already ran in the GUI.
        let precomputed: Vec<(usize, u64)> = self
            .source_partitions
            .iter()
            .filter(|p| !p.is_extended_container)
            .filter_map(|p| {
                self.effective_min_size(
                    p.index,
                    p.partition_type_byte,
                    p.partition_type_string.as_deref(),
                )
                .map(|sz| (p.index, sz))
            })
            .collect();

        let config = BackupConfig {
            source_path,
            destination_dir,
            backup_name: self.backup_name.clone(),
            compression: self.compression_type,
            checksum: self.checksum_type,
            split_size_mib: if self.split_archives && !split_disabled_for_compression {
                Some(self.split_size_mib)
            } else {
                None
            },
            sector_by_sector: self.sector_by_sector,
            partition_filter,
            chd_options: self.chd_options_for_compression(),
            size_policy,
            partition_target_sizes,
            // For single-file CHD output the "Resize partitions to minimum
            // size" checkbox enables the HFS+ defrag-clone pipeline (other
            // FSes are sized via `partition_target_sizes` above). For
            // per-partition (zstd / raw / per-partition VHD) output the
            // same flag enables the regular HFS+ clone branch in
            // `run_backup`. Sector-by-sector keeps it off. With the new
            // per-partition Defrag checkbox, the actual set of partitions
            // that get the clone is further narrowed by
            // `defrag_partition_indices` below.
            shrink_to_minimum: self.resize_partitions && !self.sector_by_sector,
            precomputed_minimum_sizes: if precomputed.is_empty() {
                None
            } else {
                Some(precomputed)
            },
            defrag_partition_indices: Some(defrag_set),
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
            let _wake = rusty_backup::os::wakelock::acquire("Rusty Backup: disk backup");
            // Catch panics from `run_backup` so the GUI poll loop always
            // observes `finished = true`. Without this, a panic in a
            // sub-thread (or in any of the producer/consumer threads
            // run_backup spawns) silently kills the worker, leaving the
            // GUI stuck at whatever progress was last written. The panic
            // message is preserved as the displayed error so we can
            // diagnose it from the log instead of having to attach a
            // debugger.
            let progress_for_panic = Arc::clone(&progress_arc);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                rusty_backup::backup::run_backup(config, Arc::clone(&progress_arc))
            }));
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    if let Ok(mut p) = progress_for_panic.lock() {
                        p.error = Some(format!("{e:#}"));
                        p.finished = true;
                    }
                }
                Err(panic_payload) => {
                    let msg = if let Some(s) = panic_payload.downcast_ref::<&str>() {
                        (*s).to_string()
                    } else if let Some(s) = panic_payload.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        format!("<non-string panic payload: {:?}>", panic_payload.type_id())
                    };
                    eprintln!("[backup-thread] PANIC: {msg}");
                    if let Ok(mut p) = progress_for_panic.lock() {
                        p.error = Some(format!("backup worker panicked: {msg}"));
                        p.finished = true;
                    }
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
        progress_state.record_sample();

        if p.finished {
            if let Some(err) = &p.error {
                ctx.log.error(format!("Backup failed: {err}"));
            }
            self.backup_running = false;
            self.backup_progress = None;
        }
    }
}
