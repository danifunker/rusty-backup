use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::Context;

use rusty_backup::backup::metadata::BackupMetadata;
use rusty_backup::clonezilla;
use rusty_backup::clonezilla::metadata::ClonezillaImage;
use rusty_backup::model::export_runner::{
    self, ExportStatus, PartitionExportConfig, PerPartitionInputs,
};
use rusty_backup::model::partition_editor::PartitionEditor;
use rusty_backup::model::status::{BlockCacheScan, CacheStatus, InspectStatus};
use rusty_backup::partition::editor as pt_editor;
use rusty_backup::partition::{
    self, detect_alignment, PartitionAlignment, PartitionInfo, PartitionTable,
};
use rusty_backup::rbformats::chd_options::ChdProfile;
use rusty_backup::rbformats::export::ExportFormat;

use super::chd_options_ui::ChdOptionsControl;

use super::browse_view::BrowseView;
use super::context::TabContext;
use super::expand_hfs_dialog::{summarize_source as summarize_hfs_source, ExpandHfsDialog};
use super::physical_disk_export::{PhysicalDiskExport, PhysicalDiskExportSource};

/// State for the Inspect tab.
pub struct InspectTab {
    /// Index into the device list, or None if "Open Image File..." is selected
    selected_device_idx: Option<usize>,
    /// Path to an image file (when using file picker instead of device)
    image_file_path: Option<PathBuf>,
    /// Keeps the temporary decompressed `.adz` / `.hdz` payload alive for
    /// as long as `image_file_path` points at it. Cleared whenever the
    /// user picks a different file or closes the image.
    amiga_tempdir: Option<tempfile::TempDir>,
    /// Detected image format label (e.g. "WOZ 3.5\"", "Fixed VHD", "2MG")
    image_format_label: Option<String>,
    /// Path to a backup folder (loaded via metadata.json)
    backup_folder_path: Option<PathBuf>,
    /// Set when the user explicitly closes a backup via "Close Backup".
    /// `gui::mod::update` consumes this signal via `take_close_backup_request`
    /// to clear the cross-tab `loaded_backup_folder` so the auto-reopen
    /// fallback (which re-loads from shared state when the tab has no
    /// backup) doesn't immediately re-open what the user just closed.
    close_backup_requested: bool,
    /// Set when a partition's `type_name` changes (e.g. a volume-label probe
    /// finishes and appends the label). The next `show` consumes it to force
    /// one extra repaint so the partition-table Grid re-measures column
    /// widths instead of clipping the wider text.
    partitions_layout_dirty: bool,
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
    /// Export: true if the export popup is open
    export_popup: bool,
    /// Export mode: true = whole disk, false = per partition
    export_whole_disk: bool,
    /// Export format selection
    export_format: ExportFormat,
    /// CHD (Hard Disk) codec/hunk-size choice.
    export_chd_hd_control: ChdOptionsControl,
    /// Per-partition sizing configuration for VHD export
    export_partition_configs: Vec<PartitionExportConfig>,
    /// VHD export background thread status
    export_status: Option<Arc<Mutex<ExportStatus>>>,
    /// Rate / ETA estimator for the VHD-export progress bar. Reset
    /// when a new export starts and consulted each frame to render the
    /// `N/s, ETA Hh Mm` suffix shared with the Backup / Restore bars.
    export_rate: super::progress::RateTracker,
    /// Rate / ETA estimator for the seekable-zstd cache-build progress
    /// bar (native zstd Inspect path).
    cache_rate: super::progress::RateTracker,
    /// Filesystem-computed in-place minimum partition sizes (partition index → bytes).
    /// "In-place" = the smallest size achievable by trim alone, no data movement.
    partition_min_sizes: HashMap<usize, u64>,
    /// Defragmented minimum (after a clone-shrink). Populated for filesystems
    /// with a defragmenting compaction path (FAT, HFS+) when it differs
    /// meaningfully from `partition_min_sizes`.
    partition_defrag_min_sizes: HashMap<usize, u64>,
    /// Per-partition volume labels (e.g. FAT label, HFS+ volume name) for
    /// display in the inspect grid. Populated during partition probing.
    partition_volume_labels: HashMap<usize, String>,
    /// Partitions whose minimum size requires an expensive volume walk; the
    /// per-row UI surfaces a "Calc min" button that spawns a worker.
    deferred_min_sizes: HashMap<usize, &'static str>,
    /// Currently-running per-partition min-size worker threads.
    pending_min_size_calcs:
        HashMap<usize, Arc<Mutex<rusty_backup::model::min_size_runner::MinSizeStatus>>>,
    /// Last phase string we already logged for each in-flight min-size
    /// calc, so each phase transition produces exactly one log line.
    last_logged_min_size_phase: HashMap<usize, String>,
    /// Currently-running per-partition HFS+ volume-label worker threads. Each
    /// resolves to either a label that gets appended to `partitions[i].type_name`
    /// or an error logged via `ctx.log`. Spawned during `apply_backup_outcome`
    /// for backup folders since their data lives in compressed files that
    /// can't be probed synchronously without freezing the GUI.
    pending_volume_label_probes:
        HashMap<usize, Arc<Mutex<rusty_backup::model::volume_label_runner::VolumeLabelStatus>>>,
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
    /// Background disk inspect status (physical device paths)
    inspect_status: Option<Arc<Mutex<InspectStatus>>>,
    /// Open device file reused by BrowseView (avoids re-open / re-prompt).
    /// Wrapping in Arc lets BrowseView call try_clone() for each open_fs() call.
    open_device_file: Option<Arc<File>>,
    /// Guard that keeps the DiskClaim (and any temp file) alive while browsing.
    open_device_guard: Option<rusty_backup::os::TempFileGuard>,
    /// When the source is a CHD container, we route every reader through a
    /// fresh `ChdReader` opened from this path. `None` for raw devices/images.
    chd_image_path: Option<PathBuf>,
    /// Cached total disk size in bytes for the currently-loaded source.
    /// Filled lazily by `actual_disk_size_bytes()` so the inspect tab's
    /// disk-layout bar + "Add Partition" enable check don't re-open the
    /// CHD container on every frame (which makes the UI visibly laggy).
    /// Cleared whenever the source changes.
    cached_disk_size: Option<u64>,
    /// Partition table editor popup
    editor_popup: bool,
    /// "Expand Image..." dialog open (Phase 6b of disk_expansion.md).
    expand_image_dialog_open: bool,
    /// MiB to add when the user clicks Expand in the Expand Image dialog.
    expand_image_add_mib: u32,
    /// Status of the background CHD-expand worker (Phase 6c). `None` when no
    /// expansion has been started in this session.
    chd_expand_status: Option<Arc<Mutex<rusty_backup::model::status::ChdExpandStatus>>>,
    /// Partition table editor model state.
    editor: PartitionEditor,
    /// Resize popup (in-place partition resizing)
    resize_popup: Option<Box<super::resize_popup::ResizePopup>>,
    /// Last filesystem check result (for popup display).
    fsck_result: Option<rusty_backup::fs::FsckResult>,
    /// Whether to show the fsck results popup.
    show_fsck_popup: bool,
    /// Whether to show debug-level fsck messages.
    show_fsck_debug: bool,
    /// Whether to show the repair confirmation dialog.
    show_repair_confirm: bool,
    /// Result of a repair operation.
    repair_report: Option<rusty_backup::fs::RepairReport>,
    /// Context for pending repair: (offset, ptype, type_string).
    repair_context: Option<(u64, u8, Option<String>)>,
    /// "Expand HFS Volume…" dialog (classic HFS only).
    expand_hfs_dialog: Option<ExpandHfsDialog>,
    /// CHD info popup text. `Some` while the popup is open.
    chd_info_text: Option<String>,
    /// When the user opens a single-file-chd backup folder, the redirect
    /// swaps the source to `<folder>/<container>` and inspects it as a
    /// raw CHD. We remember the original backup folder here so per-
    /// partition browse can hand it to BrowseView, which uses it to
    /// refresh `metadata.json` after a successful chd_edit save.
    single_file_chd_backup_folder: Option<PathBuf>,
    /// "Physical Disk Export" sub-window state.
    physical_disk_export: PhysicalDiskExport,
}

impl Default for InspectTab {
    fn default() -> Self {
        Self {
            selected_device_idx: None,
            image_file_path: None,
            amiga_tempdir: None,
            image_format_label: None,
            backup_folder_path: None,
            close_backup_requested: false,
            partitions_layout_dirty: false,
            partition_table: None,
            alignment: None,
            partitions: Vec::new(),
            backup_metadata: None,
            last_error: None,
            prev_device_idx: None,
            prev_image_path: None,
            prev_backup_path: None,
            browse_view: BrowseView::default(),
            export_popup: false,
            export_whole_disk: true,
            export_format: ExportFormat::Vhd,
            export_chd_hd_control: ChdOptionsControl::new(ChdProfile::Hd),
            export_partition_configs: Vec::new(),
            export_status: None,
            export_rate: super::progress::RateTracker::default(),
            cache_rate: super::progress::RateTracker::default(),
            partition_min_sizes: HashMap::new(),
            partition_defrag_min_sizes: HashMap::new(),
            partition_volume_labels: HashMap::new(),
            deferred_min_sizes: HashMap::new(),
            pending_min_size_calcs: HashMap::new(),
            last_logged_min_size_phase: HashMap::new(),
            pending_volume_label_probes: HashMap::new(),
            clonezilla_image: None,
            block_caches: HashMap::new(),
            block_cache_scan: None,
            seekable_cache_files: HashMap::new(),
            cache_status: None,
            inspect_status: None,
            open_device_file: None,
            open_device_guard: None,
            chd_image_path: None,
            cached_disk_size: None,
            editor_popup: false,
            expand_image_dialog_open: false,
            expand_image_add_mib: 0,
            chd_expand_status: None,
            editor: PartitionEditor::new(),
            resize_popup: None,
            fsck_result: None,
            show_fsck_popup: false,
            show_fsck_debug: false,
            show_repair_confirm: false,
            repair_report: None,
            repair_context: None,
            expand_hfs_dialog: None,
            chd_info_text: None,
            single_file_chd_backup_folder: None,
            physical_disk_export: PhysicalDiskExport::default(),
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

    /// Returns and clears the "user clicked Close Backup" signal. The App-level
    /// update loop calls this each frame to know when to clear the cross-tab
    /// `loaded_backup_folder` (which would otherwise re-open the backup via
    /// the auto-load fallback in `gui::mod::update`).
    pub fn take_close_backup_request(&mut self) -> bool {
        std::mem::take(&mut self.close_backup_requested)
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
        self.cached_disk_size = None;
        self.alignment = None;
        self.browse_view.close();
        self.partition_min_sizes.clear();
        self.partition_defrag_min_sizes.clear();
        self.partition_volume_labels.clear();
        self.deferred_min_sizes.clear();
        self.pending_min_size_calcs.clear();
        self.pending_volume_label_probes.clear();
        self.clonezilla_image = None;
        self.block_caches.clear();
        self.block_cache_scan = None;
        self.seekable_cache_files.clear();
        self.cache_status = None;
    }

    pub fn show(&mut self, ui: &mut egui::Ui, ctx: &mut TabContext) {
        ui.heading("Inspect Disk / Image");
        ui.add_space(8.0);

        // Device / image selection
        ui.horizontal(|ui| {
            ui.label("Source:");

            let current_label = if let Some(path) = &self.backup_folder_path {
                format!("Backup: {}", path.display())
            } else if let Some(path) = &self.image_file_path {
                if let Some(label) = &self.image_format_label {
                    format!(
                        "{}: {}",
                        label,
                        path.file_name().unwrap_or_default().to_string_lossy()
                    )
                } else {
                    format!(
                        "Image: {}",
                        path.file_name().unwrap_or_default().to_string_lossy()
                    )
                }
            } else if let Some(idx) = self.selected_device_idx {
                ctx.devices
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
                    for (i, device) in ctx.devices.iter().enumerate() {
                        let label = device.display_name();
                        if ui
                            .selectable_value(&mut self.selected_device_idx, Some(i), &label)
                            .clicked()
                        {
                            self.image_file_path = None;
                            self.amiga_tempdir = None;
                            self.backup_folder_path = None;
                            self.clear_results();
                        }
                    }
                    ui.separator();
                    if ui
                        .selectable_label(self.image_file_path.is_some(), "Open VHD/Disk Image...")
                        .clicked()
                    {
                        if let Some(path) = super::file_dialog()
                            .add_filter(
                                "Disk Images",
                                &[
                                    "vhd", "img", "raw", "bin", "iso", "dd", "hda", "hdv", "2mg",
                                    "dmg", "po", "do", "dsk", "dc42", "woz", "chd", "adf", "hdf",
                                    "adz", "hdz",
                                ],
                            )
                            .add_filter("All Files", &["*"])
                            .pick_file()
                        {
                            self.selected_device_idx = None;
                            self.backup_folder_path = None;
                            // Transparently decompress .adz / .hdz so the
                            // rest of the pipeline (PartitionTable::detect,
                            // backup, browse) sees a raw image.
                            match super::materialize_amiga_image_path(&path) {
                                Ok((materialized, guard)) => {
                                    self.image_file_path = Some(materialized);
                                    self.amiga_tempdir = guard;
                                }
                                Err(e) => {
                                    log::error!("Failed to decompress {}: {}", path.display(), e);
                                    self.image_file_path = Some(path);
                                    self.amiga_tempdir = None;
                                }
                            }
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
            if self.browse_view.is_active() && !self.browse_view.close_or_prompt() {
                // Unsaved-edits dialog shown; revert the selection until the
                // user resolves it. They can re-pick the new source after.
                self.selected_device_idx = self.prev_device_idx;
                self.image_file_path = self.prev_image_path.clone();
                self.backup_folder_path = self.prev_backup_path.clone();
            } else {
                self.prev_device_idx = self.selected_device_idx;
                self.prev_image_path = self.image_file_path.clone();
                self.prev_backup_path = self.backup_folder_path.clone();
                if self.backup_folder_path.is_some() {
                    self.load_backup_metadata(ctx);
                } else if self.selected_device_idx.is_some() || self.image_file_path.is_some() {
                    self.run_inspect(ctx);
                }
            }
        }

        // Poll background inspect (physical device)
        self.poll_inspect_status(ctx);

        // Poll export status
        self.poll_export_status(ctx);

        // Poll any pending per-partition minimum-size calculations
        self.poll_min_size_calcs(ctx);
        self.poll_volume_label_probes(ctx);
        self.poll_chd_expand_status(ctx);

        // Background workers (min-size + volume-label probes) finish off the
        // GUI thread; without an explicit repaint request egui only paints
        // on user input, so the labels would only appear when the mouse
        // moves. Keep a short poll cadence while anything's pending, and
        // pulse one extra repaint on the frame after a label arrives so
        // the Grid re-measures the now-wider Type column.
        if !self.pending_min_size_calcs.is_empty() || !self.pending_volume_label_probes.is_empty() {
            ui.ctx()
                .request_repaint_after(std::time::Duration::from_millis(80));
        }
        if self.partitions_layout_dirty {
            self.partitions_layout_dirty = false;
            ui.ctx().request_repaint();
        }

        // Re-inspect + Export VHD buttons
        ui.horizontal(|ui| {
            let has_source = self.selected_device_idx.is_some()
                || self.image_file_path.is_some()
                || self.backup_folder_path.is_some();
            let inspect_running = self.inspect_status.is_some();
            if ui
                .add_enabled(
                    has_source && !inspect_running,
                    egui::Button::new("Re-inspect"),
                )
                .clicked()
            {
                if self.backup_folder_path.is_some() {
                    self.load_backup_metadata(ctx);
                } else {
                    self.run_inspect(ctx);
                }
            }

            // Export button — available when we have partition data and no export running
            let has_partitions = !self.partitions.is_empty();
            let export_running = self.export_status.is_some();
            if ui
                .add_enabled(
                    has_partitions && !export_running,
                    egui::Button::new("Export Disk Image..."),
                )
                .clicked()
            {
                self.init_export_configs();
                self.export_popup = true;
            }

            // Edit Partition Table button — only for devices and image files (not backups)
            let is_editable_source = self.partition_table.is_some()
                && self.backup_folder_path.is_none()
                && self.clonezilla_image.is_none()
                && !matches!(
                    self.partition_table.as_ref(),
                    Some(PartitionTable::None { .. })
                );
            if ui
                .add_enabled(
                    is_editable_source && !export_running,
                    egui::Button::new("Edit Partition Table..."),
                )
                .clicked()
            {
                self.init_editor();
            }

            // Add Partition button (Phase 5 of disk_expansion.md) — opens
            // the partition editor with the new-entry inputs pre-filled
            // for the trailing free space, so the user doesn't have to
            // hand-compute the start LBA + size. Only meaningful when
            // there's >= 1 MiB of trailing free space.
            let trailing_free = self
                .actual_disk_size_bytes()
                .map(|disk| {
                    let used = self
                        .partitions
                        .iter()
                        .filter(|p| !p.is_extended_container)
                        .map(|p| p.start_lba.saturating_mul(512).saturating_add(p.size_bytes))
                        .max()
                        .unwrap_or(0);
                    disk.saturating_sub(used)
                })
                .unwrap_or(0);
            if ui
                .add_enabled(
                    is_editable_source && !export_running && trailing_free >= 1024 * 1024,
                    egui::Button::new("Add Partition..."),
                )
                .on_hover_text(if trailing_free >= 1024 * 1024 {
                    format!(
                        "Add a new partition into the {} of trailing free space",
                        partition::format_size(trailing_free),
                    )
                } else {
                    "No trailing free space available".to_string()
                })
                .clicked()
            {
                self.init_add_partition(trailing_free);
            }

            // Expand Image button (Phases 6b + 6c of disk_expansion.md) —
            // grows the underlying image file with zero-padding so the
            // user can then partition the new trailing space via Add
            // Partition or grow an existing partition via the editor.
            // Raw + VHD via set_len in-place; CHD via a background
            // re-encode worker (the CHD hunk layout is fixed at creation,
            // so growing means writing a fresh CHD).
            let is_image_file = self.image_file_path.is_some()
                && self.selected_device_idx.is_none()
                && self.backup_folder_path.is_none()
                && self.clonezilla_image.is_none();
            let chd_expand_running = self
                .chd_expand_status
                .as_ref()
                .map(|s| s.lock().map(|g| !g.finished).unwrap_or(false))
                .unwrap_or(false);
            if ui
                .add_enabled(
                    is_image_file && !export_running && !chd_expand_running,
                    egui::Button::new("Expand Image..."),
                )
                .on_hover_text(if !is_image_file {
                    "Only image files can be expanded — devices have fixed size.".to_string()
                } else if chd_expand_running {
                    "CHD expansion already running.".to_string()
                } else {
                    "Grow the image file with zero-padding so you can add or extend \
                     partitions in the new free space."
                        .to_string()
                })
                .clicked()
            {
                self.expand_image_dialog_open = true;
                self.expand_image_add_mib = 0;
            }

            // Resize Partitions button — same conditions as Edit Partition Table
            let resize_running = self.resize_popup.as_ref().is_some_and(|p| p.is_running());
            if ui
                .add_enabled(
                    is_editable_source && !export_running && !resize_running,
                    egui::Button::new("Resize Partitions..."),
                )
                .clicked()
            {
                self.init_resize_popup(ctx);
            }

            // CHD Info — visible whenever the source is a .chd file
            if let Some(chd_path) = self.chd_image_path.clone() {
                if ui.button("CHD Info").clicked() {
                    match rusty_backup::rbformats::chd::format_chd_info(&chd_path) {
                        Ok(text) => self.chd_info_text = Some(text),
                        Err(e) => {
                            ctx.log.error(format!("CHD Info failed: {}", e));
                        }
                    }
                }
            }

            // Close button — releases the device/image and clears results
            if self.selected_device_idx.is_some()
                && !inspect_running
                && !export_running
                && ui.button("Close Device").clicked()
            {
                self.browse_view.close();
                self.clear_results();
                self.selected_device_idx = None;
                self.prev_device_idx = None;
                ctx.log.info("Device closed and remounted.");
            }
            if self.image_file_path.is_some()
                && !inspect_running
                && !export_running
                && ui.button("Close Image").clicked()
            {
                self.browse_view.close();
                self.clear_results();
                self.image_file_path = None;
                self.amiga_tempdir = None;
                self.prev_image_path = None;
                ctx.log.info("Image file closed.");
            }
            if self.backup_folder_path.is_some()
                && !inspect_running
                && !export_running
                && ui.button("Close Backup").clicked()
            {
                self.browse_view.close();
                self.clear_results();
                self.backup_folder_path = None;
                self.prev_backup_path = None;
                // Signal to gui::mod that the cross-tab `loaded_backup_folder`
                // should be cleared too — otherwise its auto-reopen fallback
                // re-loads the backup on the very next frame.
                self.close_backup_requested = true;
                ctx.log.info("Backup folder closed.");
            }

            if export_running && ui.button("Cancel Export").clicked() {
                if let Some(ref status_arc) = self.export_status {
                    if let Ok(mut s) = status_arc.lock() {
                        s.cancel_requested = true;
                    }
                }
                ctx.log.warn("Export cancellation requested...");
            }
        });

        // Export progress bar
        if let Some(ref status_arc) = self.export_status {
            if let Ok(s) = status_arc.lock() {
                if !s.finished && s.total_bytes > 0 {
                    self.export_rate.record(s.current_bytes, "Exporting");
                    let fraction = s.current_bytes as f32 / s.total_bytes as f32;
                    let suffix = self.export_rate.suffix(s.current_bytes, s.total_bytes);
                    let text = format!(
                        "Exporting: {} / {} ({:.0}%){}",
                        partition::format_size(s.current_bytes),
                        partition::format_size(s.total_bytes),
                        fraction * 100.0,
                        suffix,
                    );
                    ui.add(egui::ProgressBar::new(fraction).text(text).animate(true));
                } else if !s.finished {
                    ui.horizontal(|ui| {
                        ui.spinner();
                        ui.label("Exporting...");
                    });
                }
            }
        }

        // Inspect-in-progress spinner
        if let Some(ref status_arc) = self.inspect_status {
            if let Ok(s) = status_arc.lock() {
                if !s.finished {
                    ui.horizontal(|ui| {
                        ui.spinner();
                        ui.label(&s.current_step);
                    });
                }
            }
        }

        // Export popup
        if self.export_popup {
            self.show_export_popup(ui, ctx);
        }

        // Editor popup
        if self.editor_popup {
            self.show_editor_popup(ui, ctx);
        }

        // Expand Image dialog (Phase 6b of disk_expansion.md)
        if self.expand_image_dialog_open {
            self.show_expand_image_dialog(ui, ctx);
        }

        // Resize popup
        if let Some(ref mut popup) = self.resize_popup {
            popup.poll_status(ctx.log);
            if !popup.show(ui, ctx.devices, ctx.log) {
                self.resize_popup = None;
            }
        }

        // Physical Disk Export sub-window
        if self.physical_disk_export.open {
            let _ = self.physical_disk_export.show(ui.ctx(), ctx);
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
        self.poll_block_cache_scan(ctx);

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
        self.poll_cache_status(ctx);

        // Show seekable cache creation progress
        if let Some(ref status_arc) = self.cache_status {
            if let Ok(s) = status_arc.lock() {
                if !s.finished && s.total_bytes > 0 {
                    let stage = format!("zstd-index-{}", s.partition_index);
                    self.cache_rate.record(s.current_bytes, &stage);
                    let fraction = s.current_bytes as f32 / s.total_bytes as f32;
                    let suffix = self.cache_rate.suffix(s.current_bytes, s.total_bytes);
                    let text = format!(
                        "Building seekable zstd index for partition {}: {} / {} ({:.0}%){}",
                        s.partition_index,
                        partition::format_size(s.current_bytes),
                        partition::format_size(s.total_bytes),
                        fraction * 100.0,
                        suffix,
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
            self.show_results(ui, ctx);
        } else if !self.partitions.is_empty() {
            // Partitions loaded from metadata (no partition table object)
            self.show_partition_list(ui, ctx);
        }

        // Show filesystem browser if active
        if self.browse_view.is_active() {
            //ui.add_space(4.0);
            self.browse_view.show(ui);
        }
    }

    fn init_editor(&mut self) {
        self.editor
            .seed_from_with_minimums(&self.partitions, &self.partition_min_sizes);
        self.editor_popup = true;
    }

    /// Open the partition-table editor pre-filled to add one new partition
    /// into the trailing free space. The user reviews/changes the size +
    /// type, then clicks Validate + Apply through the existing editor flow.
    ///
    /// Defaults:
    ///   - Start LBA: first sector past the existing partitions
    ///   - Size: all trailing free space, in MiB (rounded down)
    ///   - Type: platform-appropriate (XFS for SGI, 0x83 for MBR Linux,
    ///     Linux Filesystem GUID for GPT, Apple_HFS for APM)
    fn init_add_partition(&mut self, trailing_free_bytes: u64) {
        self.editor
            .seed_from_with_minimums(&self.partitions, &self.partition_min_sizes);

        let first_free_lba = self
            .partitions
            .iter()
            .filter(|p| !p.is_extended_container)
            .map(|p| p.start_lba + (p.size_bytes / 512))
            .max()
            .unwrap_or(0);
        let size_mib = (trailing_free_bytes / (1024 * 1024)).max(1);

        // Pick a sensible default partition type per table layout.
        let default_type: &str = match self.partition_table.as_ref() {
            Some(PartitionTable::Sgi(_)) => "XFS",
            Some(PartitionTable::Apm(_)) => "Apple_HFS",
            Some(PartitionTable::Gpt { .. }) => "0FC63DAF-8483-4772-8E79-3D69D8477DE4",
            Some(PartitionTable::Mbr(_)) => "83",
            _ => "83",
        };

        self.editor.add_start_lba = first_free_lba.to_string();
        self.editor.add_size_mb = format!("{}", size_mib);
        self.editor.add_type = default_type.to_string();
        self.editor.add_bootable = false;
        self.editor_popup = true;
    }

    /// Render the Expand Image dialog. Lets the user grow the current raw
    /// or VHD image file by N MiB of zero-padding at the end. After the
    /// growth completes the cached disk size is invalidated and the user
    /// can re-inspect to see the new free space + use Add Partition.
    fn show_expand_image_dialog(&mut self, ui: &mut egui::Ui, ctx: &mut TabContext) {
        let mut open = self.expand_image_dialog_open;
        let mut do_expand = false;
        let current_size = self.actual_disk_size_bytes().unwrap_or(0);

        egui::Window::new("Expand Image")
            .open(&mut open)
            .collapsible(false)
            .resizable(false)
            .default_width(420.0)
            .show(ui.ctx(), |ui| {
                ui.label(format!(
                    "Current size: {}",
                    partition::format_size(current_size),
                ));
                ui.add_space(4.0);

                ui.horizontal(|ui| {
                    ui.label("Add free space:");
                    ui.add(
                        egui::DragValue::new(&mut self.expand_image_add_mib)
                            .range(0..=4_194_304u32) // 4 TiB upper bound
                            .suffix(" MiB"),
                    );
                });

                let add_bytes = (self.expand_image_add_mib as u64) * 1024 * 1024;
                let new_size = current_size.saturating_add(add_bytes);
                ui.label(format!("New size: {}", partition::format_size(new_size),));

                ui.add_space(6.0);
                ui.label(
                    egui::RichText::new(
                        "After expansion, click Re-inspect to refresh the partition \
                         list, then use 'Add Partition...' or 'Edit Partition Table...' \
                         to allocate the new space.",
                    )
                    .weak(),
                );

                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    if ui
                        .add_enabled(self.expand_image_add_mib > 0, egui::Button::new("Expand"))
                        .clicked()
                    {
                        do_expand = true;
                    }
                    if ui.button("Cancel").clicked() {
                        do_expand = false;
                    }
                });
            });

        if !open {
            self.expand_image_dialog_open = false;
            return;
        }

        if do_expand {
            let path = match &self.image_file_path {
                Some(p) => p.clone(),
                None => {
                    ctx.log.error("Expand Image: no image file loaded");
                    self.expand_image_dialog_open = false;
                    return;
                }
            };
            let add_bytes = (self.expand_image_add_mib as u64) * 1024 * 1024;
            let is_chd = self.chd_image_path.is_some();
            ctx.log.info(format!(
                "Expanding image '{}' by {}...",
                path.display(),
                partition::format_size(add_bytes),
            ));
            if is_chd {
                // Route through the background re-encode worker. Status is
                // polled each frame; on completion we invalidate the cached
                // disk size so the user's next re-inspect sees the new
                // logical size.
                let status = rusty_backup::model::chd_expand_runner::spawn(path, add_bytes);
                self.chd_expand_status = Some(status);
                ctx.log.info(
                    "CHD expansion runs in the background — log lines will appear as it progresses.",
                );
            } else {
                match rusty_backup::partition::resize::expand_image_file(
                    &path,
                    add_bytes,
                    &mut |msg| ctx.log.info(msg),
                ) {
                    Ok(new_total) => {
                        ctx.log.info(format!(
                            "Image expanded to {}. Click Re-inspect to refresh.",
                            partition::format_size(new_total),
                        ));
                        self.cached_disk_size = None;
                    }
                    Err(e) => {
                        ctx.log.error(format!("Expand Image failed: {e}"));
                    }
                }
            }
            self.expand_image_dialog_open = false;
        } else {
            self.expand_image_dialog_open = open;
        }
    }

    /// Assemble a snapshot of the currently-loaded source for the Physical
    /// Disk Export sub-window. Returns `None` if no raw image file is loaded.
    fn build_physical_disk_export_source(&self) -> Option<PhysicalDiskExportSource> {
        let path = self.image_file_path.as_ref()?.clone();
        let size_bytes = std::fs::metadata(&path).ok().map(|m| m.len()).unwrap_or(0);
        let (fs_hint, has_partition_table) = match &self.partition_table {
            Some(PartitionTable::None { fs_hint, .. }) => (fs_hint.clone(), false),
            Some(_) => ("partitioned".to_string(), true),
            None => ("Unknown".to_string(), false),
        };
        Some(PhysicalDiskExportSource {
            path,
            size_bytes,
            fs_hint,
            has_partition_table,
        })
    }

    fn init_resize_popup(&mut self, ctx: &mut TabContext) {
        let table = match &self.partition_table {
            Some(t) => t.clone(),
            None => return,
        };

        let source_path = self.image_file_path.clone().or_else(|| {
            self.selected_device_idx
                .and_then(|idx| ctx.devices.get(idx))
                .map(|d| d.path.clone())
        });
        let path = match source_path {
            Some(p) => p,
            None => return,
        };

        let is_device = self.selected_device_idx.is_some();

        // Determine disk size
        let disk_size = self
            .partitions
            .iter()
            .map(|p| (p.start_lba * 512) + p.size_bytes)
            .max()
            .unwrap_or(0);

        let alignment_sectors = self
            .alignment
            .as_ref()
            .map(|a| a.alignment_sectors)
            .unwrap_or(0);

        self.resize_popup = Some(Box::new(super::resize_popup::ResizePopup::new(
            &self.partitions,
            table,
            &self.partition_min_sizes,
            alignment_sectors,
            disk_size,
            is_device,
            path,
        )));
    }

    fn show_editor_popup(&mut self, ui: &mut egui::Ui, ctx: &mut TabContext) {
        let mut open = true;
        let mut apply_requested = false;

        let table_type = match &self.partition_table {
            Some(PartitionTable::Mbr(_)) => "MBR",
            Some(PartitionTable::Gpt { .. }) => "GPT",
            Some(PartitionTable::Apm(_)) => "APM",
            _ => "Unknown",
        };

        egui::Window::new("Edit Partition Table")
            .open(&mut open)
            .resizable(true)
            .default_width(700.0)
            .show(ui.ctx(), |ui| {
                ui.label(format!("Table type: {}", table_type));
                ui.add_space(4.0);

                // Before / After disk layout. "Current" reads the loaded
                // partition list, "After" walks the editor entries +
                // add-partition inputs so the bar updates live as the user
                // edits sizes, marks rows deleted, or fills in the
                // Add Partition fields.
                show_editor_disk_layout_bars(ui, &self.partitions, &self.editor);
                ui.add_space(8.0);

                // Partition entry grid
                egui::Grid::new("editor_grid")
                    .striped(true)
                    .min_col_width(50.0)
                    .show(ui, |ui| {
                        ui.label(egui::RichText::new("#").strong());
                        ui.label(egui::RichText::new("Type").strong());
                        ui.label(egui::RichText::new("Start LBA").strong());
                        ui.label(egui::RichText::new("Size Mode").strong());
                        ui.label(egui::RichText::new("Size (MiB)").strong());
                        ui.label(egui::RichText::new("Boot").strong());
                        ui.label(egui::RichText::new("").strong());
                        ui.end_row();

                        for i in 0..self.editor.entries.len() {
                            // Copy values we need for display to avoid holding
                            // an immutable borrow across mutable TextEdit borrows.
                            let idx = self.editor.entries[i].index;
                            let deleted = self.editor.entries[i].deleted;
                            let is_ext = self.editor.entries[i].is_extended_container;
                            let is_logical = self.editor.entries[i].is_logical;
                            let start_lba = self.editor.entries[i].start_lba;
                            let size_bytes = self.editor.entries[i].size_bytes;
                            let bootable = self.editor.entries[i].bootable;
                            let type_name = self.editor.entries[i].type_name.clone();

                            if deleted {
                                ui.label(
                                    egui::RichText::new(format!("{}", idx))
                                        .color(egui::Color32::GRAY)
                                        .strikethrough(),
                                );
                                ui.label(
                                    egui::RichText::new(&type_name)
                                        .color(egui::Color32::GRAY)
                                        .strikethrough(),
                                );
                                ui.label(
                                    egui::RichText::new(format!("{}", start_lba))
                                        .color(egui::Color32::GRAY),
                                );
                                ui.label("");
                                ui.label(egui::RichText::new("deleted").color(egui::Color32::GRAY));
                                ui.label("");
                                if ui.small_button("Undo").clicked() {
                                    self.editor.entries[i].deleted = false;
                                }
                                ui.end_row();
                                continue;
                            }

                            if is_ext {
                                ui.label(
                                    egui::RichText::new(format!("{} (ext)", idx))
                                        .color(egui::Color32::GRAY),
                                );
                                ui.label(
                                    egui::RichText::new(&type_name).color(egui::Color32::GRAY),
                                );
                                ui.label(
                                    egui::RichText::new(format!("{}", start_lba))
                                        .color(egui::Color32::GRAY),
                                );
                                ui.label("");
                                let size_mib = size_bytes as f64 / (1024.0 * 1024.0);
                                ui.label(
                                    egui::RichText::new(format!("{:.2}", size_mib))
                                        .color(egui::Color32::GRAY),
                                );
                                ui.label("");
                                ui.label("");
                                ui.end_row();
                                continue;
                            }

                            let label = if is_logical {
                                format!("  {}", idx)
                            } else {
                                format!("{}", idx)
                            };
                            ui.label(label);

                            // Type field (editable)
                            let type_id = format!("ed_type_{}", i);
                            ui.add(
                                egui::TextEdit::singleline(&mut self.editor.entries[i].type_text)
                                    .desired_width(100.0)
                                    .id(egui::Id::new(&type_id)),
                            );

                            // Start LBA (read-only)
                            ui.label(format!("{}", start_lba));

                            // Size-mode radios (Original / Minimum / Custom).
                            // Selecting a non-Custom mode re-stamps
                            // `size_text` to the canonical "{:.2}" MiB for
                            // that target. Minimum is hidden when the
                            // per-partition min size isn't known (the
                            // editor doesn't perform its own FS analysis;
                            // it relies on whatever min sizes were probed
                            // by the inspect tab and seeded in via
                            // `seed_from_with_minimums`).
                            {
                                use rusty_backup::model::size_mode::SizeMode;
                                let minimum_size = self.editor.entries[i].minimum_size;
                                let original_size = size_bytes;
                                let prev = self.editor.entries[i].choice;
                                ui.horizontal(|ui| {
                                    ui.radio_value(
                                        &mut self.editor.entries[i].choice,
                                        SizeMode::Original,
                                        "Original",
                                    );
                                    if minimum_size > 0 && minimum_size < original_size {
                                        ui.radio_value(
                                            &mut self.editor.entries[i].choice,
                                            SizeMode::Minimum,
                                            "Minimum",
                                        );
                                    }
                                    ui.radio_value(
                                        &mut self.editor.entries[i].choice,
                                        SizeMode::Custom,
                                        "Custom",
                                    );
                                });
                                if self.editor.entries[i].choice != prev {
                                    match self.editor.entries[i].choice {
                                        SizeMode::Original => {
                                            self.editor.entries[i].size_text = format!(
                                                "{:.2}",
                                                original_size as f64 / (1024.0 * 1024.0),
                                            );
                                        }
                                        SizeMode::Minimum => {
                                            self.editor.entries[i].size_text = format!(
                                                "{:.2}",
                                                minimum_size as f64 / (1024.0 * 1024.0),
                                            );
                                        }
                                        _ => {}
                                    }
                                }
                            }

                            // Size (MiB) - editable when Custom; otherwise
                            // displayed as a read-only field stamped by the
                            // radio above.
                            let size_editable = matches!(
                                self.editor.entries[i].choice,
                                rusty_backup::model::size_mode::SizeMode::Custom,
                            );
                            let size_id = format!("ed_size_{}", i);
                            ui.add_enabled(
                                size_editable,
                                egui::TextEdit::singleline(&mut self.editor.entries[i].size_text)
                                    .desired_width(80.0)
                                    .id(egui::Id::new(&size_id)),
                            );

                            // Bootable cell — table-type-specific.
                            // MBR carries a checkbox (legacy behaviour);
                            // RDB renders explicit "boot" / "no boot" radio
                            // buttons so the choice is impossible to flip
                            // accidentally. GPT / APM have no per-partition
                            // bootable bit, so the cell is read-only there.
                            if table_type == "MBR" {
                                ui.checkbox(&mut self.editor.entries[i].bootable, "");
                            } else if table_type == "RDB" {
                                ui.horizontal(|ui| {
                                    let tip = "Whether this RDB partition is eligible to boot. \
                                               Multiple partitions can be set to boot at once — \
                                               the Amiga ROM picks the one with the highest boot \
                                               priority among them.";
                                    let mut val = self.editor.entries[i].bootable;
                                    if ui
                                        .radio_value(&mut val, true, "boot")
                                        .on_hover_text(tip)
                                        .changed()
                                    {
                                        self.editor.entries[i].bootable = val;
                                    }
                                    if ui
                                        .radio_value(&mut val, false, "no boot")
                                        .on_hover_text(tip)
                                        .changed()
                                    {
                                        self.editor.entries[i].bootable = val;
                                    }
                                });
                            } else {
                                ui.label(if bootable { "Yes" } else { "" });
                            }

                            // Delete button
                            if !is_logical {
                                if ui
                                    .small_button("Delete")
                                    .on_hover_text("Mark partition for deletion")
                                    .clicked()
                                {
                                    self.editor.entries[i].deleted = true;
                                }
                            } else {
                                ui.label("");
                            }

                            ui.end_row();
                        }
                    });

                ui.add_space(8.0);

                // Add partition section
                ui.collapsing("Add Partition", |ui| {
                    ui.horizontal(|ui| {
                        ui.label("Start LBA:");
                        ui.add(
                            egui::TextEdit::singleline(&mut self.editor.add_start_lba)
                                .desired_width(80.0),
                        );
                        ui.label("Size (MiB):");
                        ui.add(
                            egui::TextEdit::singleline(&mut self.editor.add_size_mb)
                                .desired_width(80.0),
                        );
                        ui.label("Type:");
                        ui.add(
                            egui::TextEdit::singleline(&mut self.editor.add_type)
                                .desired_width(80.0),
                        );
                        if table_type == "MBR" {
                            ui.checkbox(&mut self.editor.add_bootable, "Bootable");
                        }
                        if ui.button("Add").clicked() {
                            self.editor.add_entry_from_inputs(table_type == "MBR");
                        }
                    });
                });

                ui.add_space(8.0);

                // Show validation errors
                for err in &self.editor.errors {
                    ui.colored_label(egui::Color32::from_rgb(255, 100, 100), err);
                }

                // Show status
                if let Some(status) = &self.editor.status {
                    ui.colored_label(egui::Color32::from_rgb(100, 255, 100), status);
                }

                ui.add_space(4.0);

                // Action buttons
                ui.horizontal(|ui| {
                    if ui.button("Validate").clicked() {
                        self.build_and_validate_edits();
                    }

                    let can_apply = self.editor.errors.is_empty()
                        && self.partition_table.is_some()
                        && self.backup_folder_path.is_none();
                    if ui
                        .add_enabled(can_apply, egui::Button::new("Apply Changes"))
                        .clicked()
                    {
                        // Build edits first
                        self.build_and_validate_edits();
                        if self.editor.errors.is_empty() && !self.editor.edits.is_empty() {
                            apply_requested = true;
                        }
                    }

                    if ui.button("Cancel").clicked() {
                        self.editor_popup = false;
                    }
                });
            });

        if !open {
            self.editor_popup = false;
        }

        if apply_requested {
            self.apply_editor_changes(ctx);
        }
    }

    fn build_and_validate_edits(&mut self) {
        if let Some(table) = self.partition_table.as_ref() {
            self.editor.build_and_validate(table);
        }
    }

    fn apply_editor_changes(&mut self, ctx: &mut TabContext) {
        let table = match &self.partition_table {
            Some(t) => t.clone(),
            None => {
                ctx.log.error("No partition table loaded");
                return;
            }
        };

        let source_path = self.image_file_path.clone().or_else(|| {
            self.selected_device_idx
                .and_then(|idx| ctx.devices.get(idx))
                .map(|d| d.path.clone())
        });

        let path = match source_path {
            Some(p) => p,
            None => {
                ctx.log.error("No source path for editing");
                return;
            }
        };

        let edits = self.editor.edits.clone();
        let disk_size = self.editor.disk_size;

        // For devices, we need write access
        let is_device = self.selected_device_idx.is_some();

        ctx.log.info(format!(
            "Applying {} partition table edit(s) to {}...",
            edits.len(),
            path.display()
        ));

        let result = if is_device {
            use rusty_backup::os::open_target_for_writing;
            match open_target_for_writing(&path) {
                Ok(mut handle) => pt_editor::apply_edits(
                    &mut handle.file,
                    &table,
                    &edits,
                    disk_size,
                    &mut |msg| ctx.log.info(msg),
                ),
                Err(e) => Err(e),
            }
        } else {
            // Image file — open for read+write
            match std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
            {
                Ok(mut file) => {
                    pt_editor::apply_edits(&mut file, &table, &edits, disk_size, &mut |msg| {
                        ctx.log.info(msg)
                    })
                }
                Err(e) => Err(e.into()),
            }
        };

        match result {
            Ok(()) => {
                ctx.log.info("Partition table updated successfully.");
                self.editor.status = Some(
                    "Changes applied successfully! Close and re-inspect to see updates."
                        .to_string(),
                );
                self.editor.edits.clear();
            }
            Err(e) => {
                ctx.log.error(format!("Failed to apply edits: {:#}", e));
                self.editor.status = Some(format!("Error: {:#}", e));
            }
        }
    }

    fn init_export_configs(&mut self) {
        self.export_partition_configs = export_runner::build_partition_configs(
            &self.partitions,
            self.backup_metadata.as_ref(),
            &self.partition_min_sizes,
            &self.partition_defrag_min_sizes,
            self.image_file_path.as_ref(),
        );
    }

    fn show_export_popup(&mut self, ui: &mut egui::Ui, ctx: &mut TabContext) {
        // CHD (Hard Disk) requires a raw image / device source. The
        // underlying export_whole_disk_chd bails on backup folders, and
        // Clonezilla disks go through a VHD-temp convert path that doesn't
        // reach the CHD encoder. DVD CHD / CD CHD / BIN-CUE belong on the
        // optical tab — they're not offered here.
        let chd_hd_enabled = self.backup_folder_path.is_none() && self.clonezilla_image.is_none();

        // If CHD was selected and the source no longer supports it, fall
        // back to VHD before the popup renders.
        if self.export_format == ExportFormat::Chd && !chd_hd_enabled {
            self.export_format = ExportFormat::Vhd;
        }

        let is_chd_format = self.export_format == ExportFormat::Chd;
        let is_dynamic_vhd = self.export_format == ExportFormat::VhdDynamic;
        let is_qcow2 = self.export_format == ExportFormat::Qcow2;
        // Force whole-disk for formats that don't make sense per-partition:
        // CHD (headless slice no emulator consumes), dynamic VHD, and QCOW2
        // (sparse layouts that wrap whole-disk geometry — a single partition
        // isn't mostly-zero so sparsity buys nothing and there's no
        // MBR/GPT/APM for the emulator to read).
        if is_chd_format || is_dynamic_vhd || is_qcow2 {
            self.export_whole_disk = true;
        }
        let whole_disk_only = is_chd_format || is_dynamic_vhd || is_qcow2;

        egui::Window::new("Export Disk Image")
            .collapsible(false)
            .resizable(true)
            .default_width(500.0)
            .show(ui.ctx(), |ui| {
                ui.label("Export partitions as disk image files.");
                ui.add_space(4.0);

                // Current vs After disk layout. Current = loaded partition
                // sizes; After applies each export config's effective_size
                // (which honours Original / Minimum / Custom / FillRemaining
                // exactly the way the export engine will). Both bars share
                // a byte-per-pixel scale so shrinks/grows are immediately
                // visible.
                show_export_disk_layout_bars(
                    ui,
                    &self.partitions,
                    &self.export_partition_configs,
                );
                ui.add_space(8.0);

                // Export mode
                ui.radio_value(
                    &mut self.export_whole_disk,
                    true,
                    "Whole Disk (single file)",
                );
                let per_part_resp = ui.add_enabled(
                    !whole_disk_only,
                    egui::RadioButton::new(
                        !self.export_whole_disk && !whole_disk_only,
                        "Per Partition (one file per partition)",
                    ),
                );
                if per_part_resp.clicked() && !whole_disk_only {
                    self.export_whole_disk = false;
                }
                if is_chd_format {
                    per_part_resp.on_hover_text(
                        "Per-partition export is not supported for CHD — each \
                         partition would be a headless slice with no MBR/GPT, \
                         which no emulator can consume. Use whole-disk export \
                         instead.",
                    );
                } else if is_dynamic_vhd {
                    per_part_resp.on_hover_text(
                        "Per-partition export is not supported for dynamic VHD — \
                         the sparse layout wraps a whole disk and buys nothing on \
                         a single mostly-used partition. Use fixed VHD for \
                         per-partition export.",
                    );
                } else if is_qcow2 {
                    per_part_resp.on_hover_text(
                        "Per-partition export is not supported for QCOW2 — \
                         the sparse layout wraps a whole-disk geometry that \
                         QEMU/UTM expect to find an MBR/GPT/APM at sector 0.",
                    );
                }

                ui.add_space(4.0);

                // Export format
                ui.label(egui::RichText::new("Format:").strong());
                ui.horizontal_wrapped(|ui| {
                    ui.radio_value(&mut self.export_format, ExportFormat::Vhd, "VHD");
                    ui.radio_value(
                        &mut self.export_format,
                        ExportFormat::VhdDynamic,
                        "VHD (Dynamic)",
                    )
                    .on_hover_text(
                        "Sparse VHD — all-zero blocks are omitted. Same .vhd extension; \
                         readable by Hyper-V, qemu-img, Disk Management.",
                    );
                    ui.radio_value(&mut self.export_format, ExportFormat::Qcow2, "QCOW2")
                        .on_hover_text(
                            "QCOW2 v3 — sparse, uncompressed. The container UTM uses \
                             for classic-Mac PPC guests; opens in QEMU, virt-manager.",
                        );
                    ui.radio_value(&mut self.export_format, ExportFormat::Raw, "Raw (.img)");
                    ui.radio_value(&mut self.export_format, ExportFormat::TwoMg, "2MG (.2mg)");
                    ui.radio_value(&mut self.export_format, ExportFormat::Woz, "WOZ (.woz)")
                        .on_hover_text(
                            "Apple II WOZ 2.0 (floppy only: 140K / 400K / 800K sources)",
                        );
                    ui.radio_value(
                        &mut self.export_format,
                        ExportFormat::Dc42,
                        "DiskCopy 4.2 (.dsk)",
                    )
                    .on_hover_text(
                        "Mac / Apple IIgs DiskCopy 4.2 (floppy only: 400K / 720K / 800K / 1440K)",
                    );

                    let hd_resp = ui.add_enabled(
                        chd_hd_enabled,
                        egui::RadioButton::new(
                            self.export_format == ExportFormat::Chd,
                            "CHD (Hard Disk)",
                        ),
                    );
                    if hd_resp.clicked() {
                        self.export_format = ExportFormat::Chd;
                    }
                    if chd_hd_enabled {
                        hd_resp.on_hover_text(
                            "MAME hard-disk CHD (512-byte unit). DVD / CD CHD and BIN/CUE \
                             extraction live on the Optical tab.",
                        );
                    } else {
                        hd_resp.on_hover_text(
                            "CHD export needs a raw image or device source — backup \
                             folders and Clonezilla images aren't supported.",
                        );
                    }
                });

                if self.export_format == ExportFormat::Chd {
                    ui.add_space(4.0);
                    super::chd_options_ui::show(
                        ui,
                        "inspect_chd",
                        ChdProfile::Hd,
                        &mut self.export_chd_hd_control,
                    );
                }

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

                                let max_size_hover = cfg.max_size.map(|b| {
                                    let max_mib = (b / (1024 * 1024)).max(1) as u32;
                                    format!(
                                        "HFS volumes are capped at 65535 allocation blocks. \
                                         For this volume's block size, the maximum is {max_mib} MiB. \
                                         Growing further requires reformatting.",
                                    )
                                });
                                super::size_mode_row::size_mode_row(
                                    ui,
                                    &mut cfg.choice,
                                    &mut cfg.custom_size_mib,
                                    cfg.original_size,
                                    cfg.minimum_size,
                                    super::size_mode_row::SizeModeRowOptions {
                                        max_size: cfg.max_size,
                                        max_size_hover: max_size_hover.as_deref(),
                                        ..Default::default()
                                    },
                                );
                                ui.end_row();
                            }
                        });
                    ui.add_space(4.0);
                }

                // Limitations banner when at least one PFS3 partition is
                // being shrunk. Export goes through the defragmenting
                // clone path (`stream_defragmented_pfs3`), which rebuilds
                // the volume layout — surface the user-visible side
                // effects up-front so they're not surprised after the
                // export completes.
                let pfs3_shrink_partitions: Vec<&str> = self
                    .export_partition_configs
                    .iter()
                    .filter(|cfg| cfg.effective_size() < cfg.original_size)
                    .filter_map(|cfg| {
                        self.partitions
                            .iter()
                            .find(|p| p.index == cfg.index)
                            .and_then(|p| p.partition_type_string.as_deref())
                            .filter(|s| rusty_backup::fs::is_amiga_pfs3_type(s))
                            .map(|_| cfg.type_name.as_str())
                    })
                    .collect();
                if !pfs3_shrink_partitions.is_empty() {
                    egui::Frame::group(ui.style())
                        .fill(ui.visuals().widgets.noninteractive.bg_fill)
                        .show(ui, |ui| {
                            ui.label(
                                egui::RichText::new("PFS3 shrink: volume will be rebuilt")
                                    .strong(),
                            );
                            ui.label(format!(
                                "Affected partitions: {}",
                                pfs3_shrink_partitions.join(", ")
                            ));
                            ui.label(
                                "PFS3's allocator scatters blocks throughout the volume, so \
                                 in-place trim can't free up space. The export pipeline will \
                                 instead walk the source and rebuild a packed copy at the new \
                                 size — limitations to be aware of:",
                            );
                            ui.label(
                                "  - PFS3 trashcan (deldir) contents are NOT preserved.",
                            );
                            ui.label(
                                "  - File data, directory tree, protection bits, comments, \
                                 dates, softlinks, and hardlinks (both directions) ARE preserved.",
                            );
                            ui.label(
                                "  - Operation is slower than a verbatim copy: every file is \
                                 read from the source and re-written to the destination.",
                            );
                            ui.label(
                                "  - Uses a tempfile for the rebuilt image; ensure your \
                                 temp directory has enough free space for the target partition size.",
                            );
                        });
                    ui.add_space(4.0);
                }

                ui.add_space(8.0);

                // CHD re-export workflow + disk-usage banner. Visible
                // only for whole-disk CHD output. Free-space check runs
                // at Export-click time, not here, because we don't know
                // the destination directory until the user picks one.
                if self.export_whole_disk
                    && matches!(self.export_format, ExportFormat::Chd)
                    && self.partition_table.is_some()
                {
                    let is_chd_source = self
                        .image_file_path
                        .as_deref()
                        .map(rusty_backup::model::source_reader::is_chd_path)
                        .unwrap_or(false);
                    let has_resize = self.export_picker_has_resize();
                    let workflow = match (is_chd_source, has_resize) {
                        (true, true) => {
                            "Workflow: decompress source CHD to a temp file -> apply \
                             resize plan -> recompress to destination CHD."
                        }
                        (true, false) => {
                            "Workflow: stream source CHD -> recompress to destination CHD \
                             (no scratch file)."
                        }
                        (false, true) => {
                            "Workflow: build resized image in a sparse temp file -> \
                             compress to destination CHD."
                        }
                        (false, false) => {
                            "Workflow: stream source -> compress to destination CHD \
                             (no scratch file)."
                        }
                    };
                    ui.label(egui::RichText::new(workflow).small().italics());

                    // Disk-usage estimate. Source CHD file size feeds the
                    // output upper bound; raw-image source falls back to
                    // logical disk size.
                    let source_chd_file_size = if is_chd_source {
                        self.image_file_path
                            .as_ref()
                            .and_then(|p| std::fs::metadata(p).ok())
                            .map(|m| m.len())
                    } else {
                        None
                    };
                    let logical_size = self
                        .partitions
                        .iter()
                        .map(|p| p.start_lba.saturating_mul(512).saturating_add(p.size_bytes))
                        .max()
                        .unwrap_or(0);
                    let est = rusty_backup::backup::single_file_chd::estimate_export_disk_usage(
                        logical_size,
                        source_chd_file_size,
                        &self.partitions,
                        has_resize,
                    );
                    ui.label(format!(
                        "Estimated peak new disk usage: {} MiB \
                         (scratch {} MiB + output {} MiB). \
                         Available space is checked when you pick a destination.",
                        est.required_total() / (1024 * 1024),
                        est.scratch_bytes / (1024 * 1024),
                        est.output_bytes / (1024 * 1024),
                    ));

                    // Sector-by-sector source warning. Only shown when
                    // resizing — re-export at original sizes preserves
                    // the byte-for-byte property recorded in the source
                    // backup.
                    if has_resize && self.source_is_sector_by_sector_backup() {
                        ui.add_space(4.0);
                        ui.label(
                            egui::RichText::new(
                                "Warning: the source is a sector-by-sector backup. \
                                 Re-exporting with new partition sizes drops the \
                                 byte-for-byte preservation of free space and \
                                 unrecognized filesystem regions — only smart-compact \
                                 areas of recognized partitions carry over.",
                            )
                            .color(egui::Color32::from_rgb(220, 160, 70)),
                        );
                    }
                    ui.add_space(8.0);
                }

                ui.horizontal(|ui| {
                    if ui.button("Export...").clicked() {
                        self.export_popup = false;
                        self.start_export(ctx);
                    }
                    let can_physical = self.image_file_path.is_some();
                    let pde_btn = ui.add_enabled(
                        can_physical,
                        egui::Button::new("Physical Disk Export..."),
                    );
                    if !can_physical {
                        pde_btn.clone().on_hover_text(
                            "Physical Disk Export currently supports raw image file \
                             sources only. Backup folders, devices, and Clonezilla \
                             images aren't wired in yet.",
                        );
                    }
                    if pde_btn.clicked() {
                        if let Some(src) = self.build_physical_disk_export_source() {
                            self.export_popup = false;
                            self.physical_disk_export
                                .open_for(src, self.partition_table.as_ref());
                        }
                    }
                    if ui.button("Cancel").clicked() {
                        self.export_popup = false;
                    }
                });
            });
    }

    /// True if the source backup is sector-by-sector (preserves free
    /// space + unrecognized FS regions byte-for-byte). Only meaningful
    /// when the inspect tab is looking at a single-file-CHD backup body
    /// (the Stage 6 redirect set `single_file_chd_backup_folder`); falls
    /// back to `false` for plain CHDs and raw images.
    fn source_is_sector_by_sector_backup(&self) -> bool {
        let folder = match self.single_file_chd_backup_folder.as_ref() {
            Some(f) => f,
            None => return false,
        };
        let path = folder.join("metadata.json");
        let bytes = match std::fs::read_to_string(&path) {
            Ok(s) => s,
            Err(_) => return false,
        };
        match serde_json::from_str::<rusty_backup::backup::metadata::BackupMetadata>(&bytes) {
            Ok(m) => m.sector_by_sector,
            Err(_) => false,
        }
    }

    /// Sum of "we'd actually emit a different-sized partition" across the
    /// per-partition picker. Used to gate the resize-only banners.
    fn export_picker_has_resize(&self) -> bool {
        self.export_partition_configs.iter().any(|cfg| {
            self.partitions
                .iter()
                .find(|p| p.index == cfg.index)
                .map(|p| p.size_bytes != cfg.effective_size())
                .unwrap_or(false)
        })
    }

    /// Build the partition context for a CHD whole-disk export, when the
    /// source has a parsed table the single-file-CHD pipeline supports.
    /// Returns `None` (and falls back to the legacy raw-stream path) when
    /// there's no table or it's an unsupported variant — that case loses
    /// resize but still produces a valid CHD via the streaming path.
    fn build_chd_partition_context(
        &self,
        source: &Path,
        ctx: &mut TabContext,
    ) -> Option<export_runner::NativeWholeDiskChdPartitionContext> {
        use rusty_backup::backup::single_file_chd;
        let table = self.partition_table.clone()?;
        if !single_file_chd::is_supported(&table) {
            return None;
        }
        let partitions = self.partitions.clone();
        let alignment_sectors = self
            .alignment
            .as_ref()
            .map(|a| a.alignment_sectors)
            .unwrap_or(0);

        // Sector 0 of the source. Routed through ChdReader for CHD
        // sources so we don't have to special-case the inspect-tab CHD
        // path here. Falls back to a plain file read for raw images and
        // device paths.
        let mut sector0 = [0u8; 512];
        let read_result = if rusty_backup::model::source_reader::is_chd_path(source) {
            match rusty_backup::rbformats::chd::ChdReader::open(source) {
                Ok(mut r) => r.read_exact(&mut sector0),
                Err(e) => {
                    ctx.log.error(format!(
                        "Couldn't read CHD source for partition-table sector: {e}; \
                             falling back to streaming export."
                    ));
                    return None;
                }
            }
        } else {
            match std::fs::File::open(source) {
                Ok(mut f) => f.read_exact(&mut sector0),
                Err(e) => {
                    ctx.log.error(format!(
                        "Couldn't open source to read partition-table sector: {e}; \
                             falling back to streaming export."
                    ));
                    return None;
                }
            }
        };
        if let Err(e) = read_result {
            ctx.log.error(format!(
                "Couldn't read sector 0 from source: {e}; \
                 falling back to streaming export."
            ));
            return None;
        }

        // Build resize targets from the picker. When the user kept
        // every partition at Original, the runner sees an empty Vec and
        // chooses the no-scratch fast path.
        let resize_targets: Vec<(usize, u64)> = self
            .export_partition_configs
            .iter()
            .filter_map(|cfg| {
                let new = cfg.effective_size();
                if let Some(part) = partitions.iter().find(|p| p.index == cfg.index) {
                    if part.size_bytes != new {
                        return Some((cfg.index, new));
                    }
                }
                None
            })
            .collect();

        Some(export_runner::NativeWholeDiskChdPartitionContext {
            partition_table: table,
            partitions,
            source_partition_table_bytes: sector0,
            alignment_sectors,
            resize_targets,
        })
    }

    fn start_export(&mut self, ctx: &mut TabContext) {
        let format = self.export_format;
        let format_desc = format.description();

        let size_map = export_runner::build_size_map(&self.export_partition_configs);
        let overrides =
            export_runner::build_size_overrides(&self.export_partition_configs, &self.partitions);
        let total_bytes: u64 = size_map.values().sum();

        if self.export_whole_disk {
            let (filter_label, filter_exts) = format.dialog_filter();
            let dialog = super::file_dialog()
                .set_file_name(format.default_filename("disk"))
                .add_filter(filter_label, filter_exts);
            let dest = match dialog.save_file() {
                Some(p) => p,
                None => return,
            };

            // CHD whole-disk: check free space at the picked destination
            // against the disk-usage estimate. Only blocks when we can
            // actually measure free space (returns None on unsupported
            // FS). Underlying writes still error cleanly if disk fills up
            // mid-encode, so this is just a friendly heads-up.
            if matches!(format, ExportFormat::Chd) && self.partition_table.is_some() {
                let is_chd_source = self
                    .image_file_path
                    .as_deref()
                    .map(rusty_backup::model::source_reader::is_chd_path)
                    .unwrap_or(false);
                let source_chd_size = if is_chd_source {
                    self.image_file_path
                        .as_ref()
                        .and_then(|p| std::fs::metadata(p).ok())
                        .map(|m| m.len())
                } else {
                    None
                };
                let logical_size = self
                    .partitions
                    .iter()
                    .map(|p| p.start_lba.saturating_mul(512).saturating_add(p.size_bytes))
                    .max()
                    .unwrap_or(0);
                let est = rusty_backup::backup::single_file_chd::estimate_export_disk_usage(
                    logical_size,
                    source_chd_size,
                    &self.partitions,
                    self.export_picker_has_resize(),
                );
                let dest_dir = dest.parent().unwrap_or(Path::new("."));
                if let Some(avail) = rusty_backup::os::available_space(dest_dir) {
                    if est.required_total() > avail {
                        let msg = format!(
                            "Estimated peak new disk usage ({} MiB) exceeds available \
                             space at {} ({} MiB). Free up space or pick a different \
                             destination, then try again.",
                            est.required_total() / (1024 * 1024),
                            dest_dir.display(),
                            avail / (1024 * 1024),
                        );
                        ctx.log.error(&msg);
                        self.last_error = Some(msg);
                        return;
                    }
                }
            }

            let status = if let Some(cz_image) = self.clonezilla_image.clone() {
                let source = match &self.backup_folder_path {
                    Some(f) => f.clone(),
                    None => {
                        ctx.log.error("No backup folder for Clonezilla export");
                        return;
                    }
                };
                ctx.log.info(format!(
                    "Exporting Clonezilla image as whole-disk {} to {}...",
                    format_desc,
                    dest.display()
                ));
                export_runner::start_clonezilla_whole_disk(
                    format,
                    cz_image,
                    source,
                    dest,
                    overrides,
                    total_bytes,
                )
            } else {
                let source_path = self
                    .backup_folder_path
                    .clone()
                    .or_else(|| self.image_file_path.clone())
                    .or_else(|| {
                        self.selected_device_idx
                            .and_then(|idx| ctx.devices.get(idx))
                            .map(|d| d.path.clone())
                    });
                let source = match source_path {
                    Some(p) => p,
                    None => {
                        ctx.log.error("No source available for export");
                        return;
                    }
                };
                ctx.log.info(format!(
                    "Exporting whole-disk {} to {}...",
                    format_desc,
                    dest.display()
                ));
                if format == ExportFormat::Chd {
                    let opts = self.export_chd_hd_control.effective(ChdProfile::Hd);
                    let partition_context = self.build_chd_partition_context(&source, ctx);
                    export_runner::start_native_whole_disk_chd(
                        format,
                        source,
                        dest,
                        Some(opts),
                        false,
                        total_bytes,
                        partition_context,
                    )
                } else {
                    export_runner::start_native_whole_disk(
                        format,
                        source,
                        self.backup_metadata.clone(),
                        overrides,
                        dest,
                        total_bytes,
                    )
                }
            };
            self.export_status = Some(status);
            self.export_rate.reset();
        } else {
            let dest_folder = match super::file_dialog().pick_folder() {
                Some(p) => p,
                None => return,
            };

            let source_image = self.image_file_path.clone().or_else(|| {
                self.selected_device_idx
                    .and_then(|idx| ctx.devices.get(idx))
                    .map(|d| d.path.clone())
            });

            ctx.log.info(format!(
                "Exporting per-partition {} files to {}...",
                format_desc,
                dest_folder.display()
            ));

            let status = export_runner::start_per_partition(PerPartitionInputs {
                format,
                source_folder: self.backup_folder_path.clone(),
                source_image,
                meta: self.backup_metadata.clone(),
                partitions: self.partitions.clone(),
                cz_image: self.clonezilla_image.clone(),
                size_map,
                dest_folder,
                total_bytes,
            });
            self.export_status = Some(status);
            self.export_rate.reset();
        }
    }

    fn poll_inspect_status(&mut self, ctx: &mut TabContext) {
        let status_arc = match &self.inspect_status {
            Some(s) => Arc::clone(s),
            None => return,
        };

        let Ok(mut status) = status_arc.lock() else {
            return;
        };

        // Drain log messages into the panel
        for msg in status.log_messages.drain(..) {
            ctx.log.info(msg);
        }

        if status.finished {
            if let Some(err) = &status.error {
                let msg = format!("Inspect failed: {err}");
                ctx.log.error(&msg);
                self.last_error = Some(msg);
            } else {
                self.partition_table = status.partition_table.take();
                self.alignment = status.alignment.take();
                self.partitions = std::mem::take(&mut status.partitions);
                self.partition_min_sizes = std::mem::take(&mut status.partition_min_sizes);
                self.partition_defrag_min_sizes =
                    std::mem::take(&mut status.partition_defrag_min_sizes);
                self.partition_volume_labels = std::mem::take(&mut status.partition_volume_labels);
                self.deferred_min_sizes = std::mem::take(&mut status.deferred_min_sizes);
                self.image_format_label = status.format_label.take();
                // macOS only: capture the open device fd + guard so BrowseView
                // can reuse it without re-opening (and without another auth dialog).
                #[cfg(target_os = "macos")]
                {
                    if let Some(f) = status.device_file.take() {
                        self.open_device_file = Some(Arc::new(f));
                    }
                    self.open_device_guard = status.device_guard.take();
                }
            }
            drop(status);
            self.inspect_status = None;
        }
    }

    fn poll_export_status(&mut self, ctx: &mut TabContext) {
        let status_arc = match &self.export_status {
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

        if status.finished {
            if let Some(err) = &status.error {
                ctx.log.error(format!("Export failed: {err}"));
            } else {
                ctx.log.info("Export completed successfully.");
            }
            drop(status);
            self.export_status = None;
        }
    }

    /// Drain log messages from the CHD-expand worker into the global log
    /// panel and react to completion (success/error). On success we
    /// invalidate `cached_disk_size` so the user's next re-inspect picks
    /// up the new logical size.
    fn poll_chd_expand_status(&mut self, ctx: &mut TabContext) {
        let arc = match &self.chd_expand_status {
            Some(s) => s.clone(),
            None => return,
        };
        let Ok(mut status) = arc.lock() else { return };
        for msg in status.log_messages.drain(..) {
            ctx.log.info(msg);
        }
        if status.finished {
            if let Some(err) = &status.error {
                ctx.log.error(format!("Expand Image (CHD) failed: {err}"));
            } else {
                ctx.log
                    .info("CHD expansion complete. Click Re-inspect to refresh.");
                self.cached_disk_size = None;
            }
            drop(status);
            self.chd_expand_status = None;
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
        self.cached_disk_size = None;
        self.backup_metadata = None;
        self.last_error = None;
        self.image_format_label = None;
        self.partition_min_sizes.clear();
        self.partition_defrag_min_sizes.clear();
        self.partition_volume_labels.clear();
        self.deferred_min_sizes.clear();
        self.pending_min_size_calcs.clear();
        self.pending_volume_label_probes.clear();
        self.clonezilla_image = None;
        self.block_caches.clear();
        self.block_cache_scan = None;
        self.seekable_cache_files.clear();
        self.cache_status = None;
        self.inspect_status = None;
        // Release the open device fd and disk claim (remounts the disk).
        self.open_device_file = None;
        self.open_device_guard = None;
        self.chd_image_path = None;
        self.cached_disk_size = None;
        self.single_file_chd_backup_folder = None;
    }

    fn load_backup_metadata(&mut self, ctx: &mut TabContext) {
        self.partition_table = None;
        self.alignment = None;
        self.partitions.clear();
        self.last_error = None;

        let folder = match &self.backup_folder_path {
            Some(f) => f.clone(),
            None => return,
        };

        ctx.log
            .info(format!("Loading backup folder {}...", folder.display()));
        match rusty_backup::model::backup_loader::load_backup(&folder) {
            Ok(rusty_backup::model::backup_loader::LoadOutcome::Backup(out)) => {
                self.apply_backup_outcome(out, ctx);
            }
            Ok(rusty_backup::model::backup_loader::LoadOutcome::Clonezilla(out)) => {
                self.apply_clonezilla_outcome(out, ctx);
            }
            Err(e) => {
                let msg = format!("{e:#}");
                ctx.log.error(&msg);
                self.last_error = Some(msg);
            }
        }
    }

    fn apply_backup_outcome(
        &mut self,
        out: rusty_backup::model::backup_loader::BackupLoadOutcome,
        ctx: &mut TabContext,
    ) {
        for msg in &out.info {
            ctx.log.info(msg);
        }
        for msg in &out.warnings {
            ctx.log.warn(msg);
        }
        // Single-file-CHD backups put the entire disk image inside
        // `disk.chd`. The existing per-partition inspect/browse path
        // doesn't know how to slice byte ranges out of the container, so
        // redirect the user to "Open VHD/Disk Image" on the CHD itself —
        // that path already handles CHD via ChdReader and gives full
        // browse + CHD-info access.
        if matches!(
            out.metadata.layout,
            rusty_backup::backup::metadata::BackupLayout::SingleFileChd
        ) {
            if let Some(folder) = &self.backup_folder_path {
                let container = out.metadata.container.as_deref().unwrap_or("disk.chd");
                let chd_path = folder.join(container);
                ctx.log.info(format!(
                    "Single-file-CHD backup detected; opening {} as a disk image \
                     for inspection (instead of the backup folder).",
                    chd_path.display(),
                ));
                let original_folder = folder.clone();
                self.backup_folder_path = None;
                self.prev_backup_path = None;
                self.image_file_path = Some(chd_path);
                self.prev_image_path = self.image_file_path.clone();
                self.run_inspect(ctx);
                // Set after run_inspect, since run_inspect → clear_results
                // would otherwise wipe this field. Edit-mode save path on
                // the now-open CHD will use it to refresh metadata.json.
                self.single_file_chd_backup_folder = Some(original_folder);
                return;
            }
        }
        self.backup_metadata = Some(out.metadata);
        self.partition_table = out.partition_table;
        self.alignment = out.alignment;
        self.partitions = out.partitions;
        self.spawn_volume_label_probes(ctx);
    }

    fn run_inspect(&mut self, ctx: &mut TabContext) {
        self.clear_results();

        let path = if let Some(img_path) = &self.image_file_path {
            img_path.clone()
        } else if let Some(idx) = self.selected_device_idx {
            if let Some(device) = ctx.devices.get(idx) {
                device.path.clone()
            } else {
                self.last_error = Some("Selected device not found".into());
                return;
            }
        } else {
            return;
        };

        ctx.log.info(format!("Inspecting {}...", path.display()));

        // Cache CHD path: subsequent per-partition probes (HFS variant probe,
        // hfs_block_size, partition_minimum_size) need a fresh ChdReader rather
        // than reading partition_offset off the raw .chd file.
        self.chd_image_path = if rusty_backup::model::source_reader::is_chd_path(&path) {
            Some(path.clone())
        } else {
            None
        };

        // All device I/O runs on a background thread so DA unmount/claim
        // (which can block up to ~5 s) never freezes the GUI.
        let status = Arc::new(Mutex::new(InspectStatus {
            finished: false,
            error: None,
            log_messages: Vec::new(),
            current_step: "Requesting exclusive disk access...".into(),
            partition_table: None,
            alignment: None,
            partitions: Vec::new(),
            partition_min_sizes: HashMap::new(),
            partition_defrag_min_sizes: HashMap::new(),
            partition_volume_labels: HashMap::new(),
            deferred_min_sizes: HashMap::new(),
            format_label: None,
            device_file: None,
            device_guard: None,
        }));
        self.inspect_status = Some(Arc::clone(&status));

        std::thread::spawn(move || {
            // Helpers that post into the shared status without blocking the GUI.
            let push_log = |msg: String| {
                if let Ok(mut s) = status.lock() {
                    s.log_messages.push(msg);
                }
            };
            let set_step = |step: &'static str| {
                if let Ok(mut s) = status.lock() {
                    s.current_step = step.into();
                }
            };
            let finish_err = |err: String| {
                if let Ok(mut s) = status.lock() {
                    s.error = Some(err);
                    s.finished = true;
                }
            };

            // Open the device/file with full elevation: unmounts volumes and
            // claims exclusive DA access for the duration of the inspect.
            let elevated = match rusty_backup::os::open_source_for_reading(&path) {
                Ok(e) => e,
                Err(e) => {
                    finish_err(format!("Cannot open {}: {e}", path.display()));
                    return;
                }
            };
            // Decompose into file + guard (keeps DiskClaim alive until we
            // explicitly drop it by storing/releasing from the main thread).
            let (device_file, guard) = elevated.into_parts();

            set_step("Reading partition table...");

            // Clone for the BufReader; keep `device_file` for additional clones.
            let mut reader = match device_file.try_clone() {
                Ok(f) => BufReader::new(f),
                Err(e) => {
                    finish_err(format!("Cannot clone file handle: {e}"));
                    return;
                }
            };

            // Detect image format (VHD, 2MG, DMG, or raw)
            let is_device = path.to_string_lossy().starts_with("/dev/")
                || path.to_string_lossy().starts_with("\\\\.\\");

            let detect_result = if !is_device {
                // For image files, use unified format detection
                match device_file.try_clone() {
                    Ok(clone) => match rusty_backup::rbformats::detect_image_format_with_path(
                        clone,
                        Some(&path),
                    ) {
                        Ok(format) => {
                            let desc = format.description();
                            push_log(format!("Detected format: {}", desc));
                            if let Ok(mut s) = status.lock() {
                                s.format_label = Some(desc.clone());
                            }
                            match rusty_backup::rbformats::wrap_image_reader(
                                device_file.try_clone().unwrap_or_else(|_| {
                                    std::fs::File::open(&path).expect("reopen failed")
                                }),
                                format,
                            ) {
                                Ok((mut wrapped_reader, _data_size)) => {
                                    PartitionTable::detect(&mut wrapped_reader)
                                }
                                Err(e) => {
                                    push_log(format!("Format wrap failed, trying raw: {e}"));
                                    let _ = reader.seek(SeekFrom::Start(0));
                                    PartitionTable::detect(&mut reader)
                                }
                            }
                        }
                        Err(e) => {
                            push_log(format!("Format detection failed, trying raw: {e}"));
                            PartitionTable::detect(&mut reader)
                        }
                    },
                    Err(_) => PartitionTable::detect(&mut reader),
                }
            } else {
                // Device path — always treat as raw
                PartitionTable::detect(&mut reader)
            };

            match detect_result {
                Ok(mut table) => {
                    // Fix up superfloppy size: seek(End(0)) returns 0 for macOS devices
                    if let PartitionTable::None { size_bytes, .. } = &mut table {
                        if *size_bytes == 0 {
                            if let Ok(f) = device_file.try_clone() {
                                if let Ok(real_size) = rusty_backup::os::get_file_size(&f, &path) {
                                    *size_bytes = real_size;
                                }
                            }
                        }
                    }

                    let alignment = detect_alignment(&table);
                    let mut partitions = table.partitions();

                    // CHD-aware reader factory for per-partition probes. For
                    // CHD sources the raw .chd file is compressed at byte
                    // offsets, so partition_offset reads off it return garbage
                    // (e.g. 0x504D "PM" inside an APM map). Open a fresh
                    // ChdReader instead so partition_offset addresses the
                    // unwrapped disk image.
                    let is_chd = rusty_backup::model::source_reader::is_chd_path(&path);
                    let make_probe_reader =
                        || -> Option<Box<dyn rusty_backup::rbformats::ReadSeek>> {
                            if is_chd {
                                rusty_backup::rbformats::chd::ChdReader::open(&path)
                                    .ok()
                                    .map(|r| {
                                        Box::new(r) as Box<dyn rusty_backup::rbformats::ReadSeek>
                                    })
                            } else {
                                device_file.try_clone().ok().map(|f| {
                                    Box::new(BufReader::new(f))
                                        as Box<dyn rusty_backup::rbformats::ReadSeek>
                                })
                            }
                        };

                    // For APM disks, probe "Apple_HFS" partitions to show the
                    // actual HFS variant (HFS vs HFS+ vs HFSX) in the type
                    // name, and prefer the HFS+ volume header label over the
                    // (often auto-generated) APM partition name. The APM-only
                    // type_name format from `PartitionTable::partitions()` is
                    // `"{type_string} ({apm_name})"`; we rebuild it here.
                    if matches!(table, PartitionTable::Apm(_)) {
                        for part in &mut partitions {
                            let Some(type_string) = part.partition_type_string.as_deref() else {
                                continue;
                            };
                            // Apple_HFS is the ambiguous APM tag (HFS, HFS+, or
                            // wrapped HFS+); Apple_HFSX is unambiguous but
                            // benefits from the same volume-label lookup so
                            // the inspect grid surfaces the user-facing name
                            // rather than just the type string.
                            if type_string != "Apple_HFS" && type_string != "Apple_HFSX" {
                                continue;
                            }
                            // Extract the APM name from the existing
                            // "Apple_HFS (NAME)" formatted string.
                            let apm_name = part
                                .type_name
                                .strip_prefix(type_string)
                                .and_then(|s| s.trim_start().strip_prefix('('))
                                .and_then(|s| s.strip_suffix(')'))
                                .unwrap_or("")
                                .to_string();

                            let part_offset = part.start_lba * 512;
                            let detected = make_probe_reader()
                                .map(|mut br| {
                                    rusty_backup::fs::probe_apple_hfs_type(&mut br, part_offset)
                                        .to_string()
                                })
                                .unwrap_or_default();
                            let variant_tag: Option<&'static str> = match detected.as_str() {
                                "HFS+" => Some("HFS+"),
                                "HFSX" => Some("HFSX"),
                                _ => None,
                            };

                            // Probe HFS+ volume label cheaply (header + first
                            // catalog extent only). HFS variant doesn't matter
                            // for the helper — it bails on non-HFS+ signatures.
                            let label = if variant_tag.is_some() {
                                make_probe_reader().and_then(|mut br| {
                                    rusty_backup::fs::hfsplus::probe_hfsplus_volume_label(
                                        &mut br,
                                        part_offset,
                                    )
                                })
                            } else {
                                None
                            };

                            // Pick the most informative display name:
                            //   - HFS+ label if we have one.
                            //   - Otherwise APM name, but suppress the noisy
                            //     auto-generated "Apple_HFS_Untitled_N" form
                            //     macOS Disk Utility writes when no name was
                            //     provided.
                            let apm_is_default = apm_name.starts_with("Apple_HFS_Untitled");
                            let display_name = match (label.as_deref(), apm_name.as_str()) {
                                (Some(lab), _) if !lab.is_empty() => lab.to_string(),
                                (_, n) if !n.is_empty() && !apm_is_default => n.to_string(),
                                _ => String::new(),
                            };
                            // If both a real label and a non-default APM name
                            // exist and they differ, surface the APM name too.
                            let trailing_apm = match (label.as_deref(), apm_name.as_str()) {
                                (Some(lab), n)
                                    if !lab.is_empty()
                                        && !n.is_empty()
                                        && !apm_is_default
                                        && lab != n =>
                                {
                                    Some(n.to_string())
                                }
                                _ => None,
                            };

                            // Use the partition's actual type string so
                            // Apple_HFSX volumes don't get re-labelled
                            // "Apple_HFS (HFSX)". Drop the redundant tag
                            // when type_string already encodes it.
                            let mut new_name = match variant_tag {
                                Some(tag) if tag == "HFSX" && type_string == "Apple_HFSX" => {
                                    type_string.to_string()
                                }
                                Some(tag) => format!("{type_string} ({tag})"),
                                None => type_string.to_string(),
                            };
                            if !display_name.is_empty() {
                                new_name.push(' ');
                                new_name.push_str(&display_name);
                            }
                            if let Some(extra) = trailing_apm {
                                new_name.push_str(&format!(" [APM: {extra}]"));
                            }
                            part.type_name = new_name;
                        }
                    }

                    // MBR type 0x83 is officially "Linux", but MSX HDD formatters
                    // (Nextor and similar) reuse it for FAT12/16. Probe the VBR
                    // and replace the generic "Linux" label with the real
                    // filesystem family so the inspect grid reflects what the
                    // browse/edit dispatch will actually open. For FAT VBRs at
                    // 0x83, also tag "(MSX)" since standard PC tooling never
                    // writes FAT under 0x83 — it's specific to MSX HDD layouts.
                    for part in &mut partitions {
                        if part.is_extended_container {
                            continue;
                        }
                        // MBR 0x83 (Linux native) or GPT "Linux Filesystem"
                        // GUID — both can hold ext, btrfs, or XFS. Probe the
                        // VBR to label the inspect grid with the real family.
                        let is_linux_mbr = part.partition_type_byte == 0x83;
                        let is_linux_gpt = part.partition_type_string.as_deref()
                            == Some("0FC63DAF-8483-4772-8E79-3D69D8477DE4");
                        if !is_linux_mbr && !is_linux_gpt {
                            continue;
                        }
                        if let Some(mut br) = make_probe_reader() {
                            match rusty_backup::fs::probe_0x83_fs_type(
                                &mut br,
                                part.start_lba * 512,
                            ) {
                                Some("FAT") if is_linux_mbr => {
                                    let part_offset = part.start_lba * 512;
                                    let subtype = make_probe_reader().and_then(|br2| {
                                        rusty_backup::fs::fat::FatFilesystem::open(br2, part_offset)
                                            .ok()
                                            .map(|fs| {
                                                use rusty_backup::fs::Filesystem;
                                                fs.fs_type().to_string()
                                            })
                                    });
                                    part.type_name = match subtype {
                                        Some(t) => format!("{t} (MSX)"),
                                        None => "FAT (MSX)".to_string(),
                                    };
                                }
                                Some(other) => {
                                    part.type_name = other.to_string();
                                }
                                None => {}
                            }
                        }
                    }

                    // Probe HFS/HFS+ partitions for their allocation block size
                    // so the inspect grid can show it. Covers APM Apple_HFS,
                    // MBR type 0xAF, and HFS/HFS+ superfloppies.
                    for part in &mut partitions {
                        if part.is_extended_container {
                            continue;
                        }
                        let is_hfs_apm = part
                            .partition_type_string
                            .as_deref()
                            .map(|s| s.eq_ignore_ascii_case("Apple_HFS"))
                            .unwrap_or(false);
                        let is_hfs_mbr = part.partition_type_byte == 0xAF;
                        let is_hfs_superfloppy = matches!(
                            &table,
                            PartitionTable::None { fs_hint, .. }
                                if fs_hint == "HFS" || fs_hint == "HFS+"
                        );
                        if !(is_hfs_apm || is_hfs_mbr || is_hfs_superfloppy) {
                            continue;
                        }
                        if let Some(mut br) = make_probe_reader() {
                            part.hfs_block_size = rusty_backup::fs::hfs_block_size_at_offset(
                                &mut br,
                                part.start_lba * 512,
                            );
                        }
                    }

                    if let PartitionTable::None { fs_hint, .. } = &table {
                        push_log(format!(
                            "Warning: image has no partition table (no MBR/GPT/APM/SGI signature). Detected bare {} filesystem at sector 0; synthesizing a single-partition view in memory so the volume can be browsed. The source file is not modified.",
                            fs_hint
                        ));
                    } else {
                        push_log(format!(
                            "Detected {} partition table with {} partition(s)",
                            table.type_name(),
                            partitions.len()
                        ));
                    }
                    push_log(format!(
                        "Alignment: {} (first LBA: {})",
                        alignment.alignment_type, alignment.first_lba
                    ));

                    // Probe volume labels cheaply (boot/root block reads only)
                    // so the inspect grid's Volume column has a value. HFS+
                    // labels are still folded into type_name and could move
                    // here in a future pass.
                    let mut partition_volume_labels: HashMap<usize, String> = HashMap::new();
                    for part in &partitions {
                        if part.is_extended_container {
                            continue;
                        }
                        let is_fat_byte = matches!(
                            part.partition_type_byte,
                            0x01 | 0x04
                                | 0x06
                                | 0x0B
                                | 0x0C
                                | 0x0E
                                | 0x14
                                | 0x16
                                | 0x1B
                                | 0x1C
                                | 0x1E
                        );
                        let is_fat_superfloppy = matches!(
                            &table,
                            PartitionTable::None { fs_hint, .. } if fs_hint == "FAT"
                        );
                        let is_amiga_dos = part
                            .partition_type_string
                            .as_deref()
                            .map(rusty_backup::fs::is_amiga_dos_type)
                            .unwrap_or(false);
                        let is_amiga_pfs3 = part
                            .partition_type_string
                            .as_deref()
                            .map(rusty_backup::fs::is_amiga_pfs3_type)
                            .unwrap_or(false);
                        let is_amiga_sfs = part
                            .partition_type_string
                            .as_deref()
                            .map(rusty_backup::fs::is_amiga_sfs_type)
                            .unwrap_or(false);

                        let label_opt: Option<String> = if is_fat_byte || is_fat_superfloppy {
                            make_probe_reader().and_then(|br| {
                                rusty_backup::fs::fat::FatFilesystem::open(br, part.start_lba * 512)
                                    .ok()
                                    .and_then(|fs| {
                                        use rusty_backup::fs::Filesystem;
                                        fs.volume_label().map(str::to_string)
                                    })
                            })
                        } else if is_amiga_dos {
                            make_probe_reader().and_then(|br| {
                                rusty_backup::fs::affs::AffsFilesystem::open(
                                    br,
                                    part.start_lba * 512,
                                )
                                .ok()
                                .and_then(|fs| {
                                    use rusty_backup::fs::Filesystem;
                                    fs.volume_label().map(str::to_string)
                                })
                            })
                        } else if is_amiga_pfs3 {
                            make_probe_reader().and_then(|br| {
                                rusty_backup::fs::pfs3::Pfs3Filesystem::open(
                                    br,
                                    part.start_lba * 512,
                                )
                                .ok()
                                .and_then(|fs| {
                                    use rusty_backup::fs::Filesystem;
                                    fs.volume_label().map(str::to_string)
                                })
                            })
                        } else if is_amiga_sfs {
                            make_probe_reader().and_then(|br| {
                                rusty_backup::fs::sfs::SfsFilesystem::open(br, part.start_lba * 512)
                                    .ok()
                                    .and_then(|fs| {
                                        use rusty_backup::fs::Filesystem;
                                        fs.volume_label().map(str::to_string)
                                    })
                            })
                        } else {
                            None
                        };

                        // For Amiga RDB partitions, combine drive name + volume
                        // label so the column shows both ("DH0 - Workbench").
                        // Drive name alone is also useful when the volume name
                        // is empty, so include it as a fallback. See
                        // partition::PartitionInfo::drv_name for the source.
                        let combined_label = match (part.drv_name.as_deref(), label_opt.as_deref())
                        {
                            (Some(drv), Some(vol)) if !drv.is_empty() && !vol.trim().is_empty() => {
                                Some(format!("{drv} - {}", vol.trim()))
                            }
                            (Some(drv), _) if !drv.is_empty() => Some(drv.to_string()),
                            (_, Some(vol)) if !vol.trim().is_empty() => {
                                Some(vol.trim().to_string())
                            }
                            _ => None,
                        };

                        if let Some(label) = combined_label {
                            partition_volume_labels.insert(part.index, label);
                        }
                    }

                    // Compute minimum partition sizes via filesystem analysis.
                    // Cheap filesystems (FAT/NTFS/exFAT) are computed eagerly here;
                    // expensive ones (HFS/HFS+/ext/btrfs/ProDOS) come back as
                    // Deferred and are surfaced in the UI as a per-row button.
                    set_step("Analyzing filesystem sizes...");
                    let mut partition_min_sizes = HashMap::new();
                    let mut partition_defrag_min_sizes: HashMap<usize, u64> = HashMap::new();
                    let mut deferred_min_sizes: HashMap<usize, &'static str> = HashMap::new();
                    for part in &partitions {
                        if part.is_extended_container {
                            continue;
                        }
                        if part.partition_type_byte == 0xEE {
                            continue;
                        }
                        let result = if is_chd {
                            let Ok(reader) = rusty_backup::rbformats::chd::ChdReader::open(&path)
                            else {
                                continue;
                            };
                            rusty_backup::fs::partition_minimum_size(
                                reader,
                                part.start_lba * 512,
                                part.partition_type_byte,
                                part.partition_type_string.as_deref(),
                                part.size_bytes,
                                false,
                                None,
                                &|_| {},
                            )
                        } else {
                            let Ok(f) = device_file.try_clone() else {
                                continue;
                            };
                            if is_device {
                                rusty_backup::fs::partition_minimum_size(
                                    rusty_backup::os::SectorAlignedReader::new(f),
                                    part.start_lba * 512,
                                    part.partition_type_byte,
                                    part.partition_type_string.as_deref(),
                                    part.size_bytes,
                                    false,
                                    None,
                                    &|_| {},
                                )
                            } else {
                                rusty_backup::fs::partition_minimum_size(
                                    BufReader::new(f),
                                    part.start_lba * 512,
                                    part.partition_type_byte,
                                    part.partition_type_string.as_deref(),
                                    part.size_bytes,
                                    false,
                                    None,
                                    &|_| {},
                                )
                            }
                        };
                        match result {
                            rusty_backup::fs::MinimumResult::Computed {
                                in_place: Some(min_size),
                                defragmented,
                                fragmentation_percent: _,
                            } => {
                                let clamped = min_size.min(part.size_bytes);
                                partition_min_sizes.insert(part.index, clamped);
                                if let Some(d) = defragmented {
                                    let d_clamped = d.min(part.size_bytes);
                                    if d_clamped < clamped {
                                        partition_defrag_min_sizes.insert(part.index, d_clamped);
                                    }
                                }
                                push_log(format!(
                                    "Partition {}: minimum size {} (original {})",
                                    part.index,
                                    rusty_backup::partition::format_size(clamped),
                                    rusty_backup::partition::format_size(part.size_bytes),
                                ));
                            }
                            rusty_backup::fs::MinimumResult::Computed {
                                in_place: None, ..
                            } => {}
                            rusty_backup::fs::MinimumResult::Deferred { fs_name } => {
                                push_log(format!(
                                    "Partition {}: need to calculate minimum size due to filesystem {fs_name} (click \"Calc min\" to compute)",
                                    part.index,
                                ));
                                deferred_min_sizes.insert(part.index, fs_name);
                            }
                        }
                    }

                    // Write results and signal completion.
                    if let Ok(mut s) = status.lock() {
                        s.partition_table = Some(table);
                        s.alignment = Some(alignment);
                        s.partitions = partitions;
                        s.partition_min_sizes = partition_min_sizes;
                        s.partition_defrag_min_sizes = partition_defrag_min_sizes;
                        s.partition_volume_labels = partition_volume_labels;
                        s.deferred_min_sizes = deferred_min_sizes;
                        // On macOS, pass the open fd + claim to the main thread
                        // so BrowseView can reuse it without re-opening/re-prompting.
                        #[cfg(target_os = "macos")]
                        {
                            s.device_file = Some(device_file);
                            s.device_guard = Some(guard);
                        }
                        s.finished = true;
                    }
                }
                Err(e) => {
                    finish_err(format!("Failed to parse partition table: {e}"));
                }
            }
        });
    }

    fn apply_clonezilla_outcome(
        &mut self,
        out: rusty_backup::model::backup_loader::ClonezillaLoadOutcome,
        ctx: &mut TabContext,
    ) {
        for msg in &out.info {
            ctx.log.info(msg);
        }
        for msg in &out.warnings {
            ctx.log.warn(msg);
        }
        if out.cached_metadata_count > 0 {
            ctx.log.info(format!(
                "Found {} cached metadata file(s) for browsing",
                out.cached_metadata_count,
            ));
        }
        self.partition_table = out.partition_table;
        self.alignment = out.alignment;
        self.partitions = out.partitions;
        self.partition_min_sizes = out.partition_min_sizes;
        self.clonezilla_image = Some(out.image);
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

    fn show_results(&mut self, ui: &mut egui::Ui, ctx: &mut TabContext) {
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
        self.show_disk_layout_section(ui);
        self.show_partition_list(ui, ctx);
    }

    fn show_disk_layout_section(&mut self, ui: &mut egui::Ui) {
        if self.partitions.is_empty() {
            return;
        }
        let actual_disk_size = self.actual_disk_size_bytes();
        egui::CollapsingHeader::new("Disk layout")
            .default_open(true)
            .show(ui, |ui| {
                let segments = build_disk_layout_segments(
                    &self.partitions,
                    &self.partition_volume_labels,
                    actual_disk_size,
                );
                super::partition_bar::PartitionBar::new(segments).show(ui);
            });
        ui.add_space(4.0);
    }

    /// Best-effort estimate of the total disk size (bytes), used to draw the
    /// trailing-free-space segment on the disk-layout bar. Sources:
    ///
    /// * Backup metadata — `source_size_bytes` is the authoritative recorded
    ///   disk size, including any trailing free space the source had.
    /// * Clonezilla image — same idea via `source_size_bytes`.
    /// * Live device — physical disk size.
    /// * Raw image file — filesystem-reported file length.
    ///
    /// Returns `None` when no source applies (e.g. before inspection has
    /// finished); callers should suppress the trailing-free segment then.
    fn actual_disk_size_bytes(&mut self) -> Option<u64> {
        if let Some(cached) = self.cached_disk_size {
            return Some(cached);
        }
        let computed = self.compute_actual_disk_size_bytes();
        self.cached_disk_size = computed;
        computed
    }

    fn compute_actual_disk_size_bytes(&self) -> Option<u64> {
        if let Some(meta) = &self.backup_metadata {
            if meta.source_size_bytes > 0 {
                return Some(meta.source_size_bytes);
            }
        }
        if let Some(cz) = &self.clonezilla_image {
            if cz.source_size_bytes > 0 {
                return Some(cz.source_size_bytes);
            }
        }
        // For raw .chd image files, the file size on disk is the
        // *compressed* size — the logical disk seen by partition-table
        // parsing comes from the CHD header. Use that, otherwise the
        // trailing-free-space calculation thinks the disk is much smaller
        // than it actually is and we wrongly disable "Add Partition...".
        if let Some(chd_path) = &self.chd_image_path {
            if let Ok(reader) = rusty_backup::rbformats::chd::ChdReader::open(chd_path) {
                let logical = reader.logical_size();
                if logical > 0 {
                    return Some(logical);
                }
            }
        }
        if let Some(path) = &self.image_file_path {
            if let Ok(meta) = std::fs::metadata(path) {
                let len = meta.len();
                if len > 0 {
                    return Some(len);
                }
            }
        }
        None
    }

    fn show_partition_list(&mut self, ui: &mut egui::Ui, ctx: &mut TabContext) {
        if self.partitions.is_empty() {
            ui.label("No partitions found.");
            return;
        }

        // Browse request: (partition_index, offset, partition_type_byte)
        let mut browse_request: Option<(usize, u64, u8, Option<String>)> = None;
        // Check (fsck) request: same tuple format
        let mut check_request: Option<(u64, u8, Option<String>)> = None;
        // Expand-HFS request: (offset, partition_size_bytes)
        let mut expand_request: Option<(u64, u64)> = None;
        // "Calc min" button click: partition index to compute minimum size for.
        let mut min_size_calc_request: Option<usize> = None;

        egui::Grid::new("partition_table")
            .striped(true)
            .min_col_width(60.0)
            .show(ui, |ui| {
                // Header
                ui.label(egui::RichText::new("#").strong());
                ui.label(egui::RichText::new("Type").strong());
                ui.label(egui::RichText::new("Volume").strong());
                ui.label(egui::RichText::new("Start LBA").strong());
                ui.label(egui::RichText::new("Size").strong());
                let show_min_col = self.backup_metadata.is_some()
                    || !self.partition_min_sizes.is_empty()
                    || !self.deferred_min_sizes.is_empty()
                    || !self.pending_min_size_calcs.is_empty();
                if show_min_col {
                    ui.label(egui::RichText::new("Min Size").strong())
                        .on_hover_text("Smallest size achievable by trim alone (no data movement)");
                    ui.label(egui::RichText::new("Defrag Min").strong())
                        .on_hover_text(
                            "Smallest size achievable by cloning into a packed target. \
                             Used when restoring with \"Resize partitions to minimum size.\"",
                        );
                }
                ui.label(egui::RichText::new("Block Size").strong());
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
                        // Volume column — extended containers have no filesystem.
                        ui.label("");
                        ui.label(
                            egui::RichText::new(format!("{}", part.start_lba))
                                .color(egui::Color32::GRAY),
                        );
                        ui.label(
                            egui::RichText::new(partition::format_size(part.size_bytes))
                                .color(egui::Color32::GRAY),
                        );
                        if show_min_col {
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
                            // Defrag Min column.
                            ui.label("");
                        }
                        ui.label("");
                        ui.label("");
                        ui.label("");
                    } else {
                        ui.label(index_label);
                        ui.label(&part.type_name);
                        // Volume column. Always probed live from the
                        // partition itself — never persisted to backup
                        // metadata.
                        let volume_label = self.partition_volume_labels.get(&part.index);
                        ui.label(volume_label.map(String::as_str).unwrap_or(""));
                        ui.label(format!("{}", part.start_lba));
                        ui.label(partition::format_size(part.size_bytes));
                        if show_min_col {
                            let in_place_min = self
                                .backup_metadata
                                .as_ref()
                                .and_then(|m| {
                                    m.partitions
                                        .iter()
                                        .find(|pm| pm.index == part.index)
                                        .and_then(|pm| {
                                            // Prefer explicit minimum_size_bytes (computed at
                                            // backup time); fall back to imaged_size_bytes for
                                            // older backups that lack the field.
                                            pm.minimum_size_bytes.or({
                                                if pm.imaged_size_bytes > 0
                                                    && pm.imaged_size_bytes < pm.original_size_bytes
                                                {
                                                    Some(pm.imaged_size_bytes)
                                                } else {
                                                    None
                                                }
                                            })
                                        })
                                })
                                .filter(|&sz| sz > 0 && sz < part.size_bytes)
                                .or_else(|| {
                                    self.partition_min_sizes
                                        .get(&part.index)
                                        .copied()
                                        .filter(|&sz| sz < part.size_bytes)
                                });
                            let defrag_min = self
                                .backup_metadata
                                .as_ref()
                                .and_then(|m| {
                                    m.partitions
                                        .iter()
                                        .find(|pm| pm.index == part.index)
                                        .and_then(|pm| pm.defragmented_min_size_bytes)
                                })
                                .filter(|&sz| sz > 0 && sz < part.size_bytes)
                                .or_else(|| {
                                    self.partition_defrag_min_sizes
                                        .get(&part.index)
                                        .copied()
                                        .filter(|&sz| sz < part.size_bytes)
                                });
                            // Min Size column (in-place trim point).
                            if let Some(sz) = in_place_min {
                                ui.label(partition::format_size(sz));
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
                            // Defrag Min column.
                            match (defrag_min, in_place_min) {
                                (Some(d), Some(m)) if d < m => {
                                    ui.label(partition::format_size(d));
                                }
                                (Some(d), None) => {
                                    ui.label(partition::format_size(d));
                                }
                                _ => {
                                    ui.label("");
                                }
                            }
                        }
                        if let Some(bs) = part.hfs_block_size {
                            ui.label(format_block_size(bs));
                        } else {
                            ui.label("");
                        }
                        ui.label(if part.bootable { "Yes" } else { "" });
                        if is_browsable_type(part.partition_type_byte)
                            || is_fat_name(&part.type_name)
                            || is_browsable_type_string(part.partition_type_string.as_deref())
                            || is_browsable_superfloppy(part.partition_type_byte, &part.type_name)
                        {
                            let ptype = if part.partition_type_byte != 0 {
                                part.partition_type_byte
                            } else {
                                rusty_backup::model::backup_loader::infer_fat_type_byte(
                                    &part.type_name,
                                )
                            };
                            if ui.small_button("Browse").clicked() {
                                browse_request = Some((
                                    part.index,
                                    part.start_lba * 512,
                                    ptype,
                                    part.partition_type_string.clone(),
                                ));
                            }
                            if is_checkable_type(ptype, part.partition_type_string.as_deref())
                                && ui.small_button("Check").clicked()
                            {
                                check_request = Some((
                                    part.start_lba * 512,
                                    ptype,
                                    part.partition_type_string.clone(),
                                ));
                            }
                            if is_classic_hfs(
                                part.partition_type_byte,
                                part.partition_type_string.as_deref(),
                                &part.type_name,
                            ) && ui.small_button("Expand…").clicked()
                            {
                                expand_request = Some((part.start_lba * 512, part.size_bytes));
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
            self.open_browse(part_index, offset, ptype, type_string, ctx);
        }

        // Handle check (fsck) request
        if let Some((offset, ptype, type_string)) = check_request {
            self.run_fsck(offset, ptype, type_string.clone(), ctx);
            self.repair_context = Some((offset, ptype, type_string));
            self.repair_report = None;
        }

        // Handle expand-HFS request: probe the source, then open the dialog.
        if let Some((offset, partition_size)) = expand_request {
            self.open_expand_hfs(offset, partition_size, ctx);
        }

        // Handle "Calc min" click: spawn a worker for this partition.
        if let Some(part_index) = min_size_calc_request {
            self.start_min_size_calc(part_index, ctx);
        }

        // Fsck results popup
        self.render_fsck_popup(ui, ctx);

        // CHD Info popup
        self.render_chd_info_popup(ui);

        // Expand-HFS dialog
        if let Some(dlg) = &mut self.expand_hfs_dialog {
            let still_open = dlg.show(ui.ctx(), ctx.log);
            if !still_open {
                self.expand_hfs_dialog = None;
            }
        }
    }

    /// Resolve the source path and open the Expand-HFS dialog.
    fn open_expand_hfs(&mut self, offset: u64, partition_size: u64, ctx: &mut TabContext) {
        let source_path = self
            .selected_device_idx
            .and_then(|idx| ctx.devices.get(idx))
            .map(|d| d.path.clone())
            .or_else(|| self.image_file_path.clone());
        let path = match source_path {
            Some(p) => p,
            None => {
                ctx.log.error("No source available for HFS expand");
                return;
            }
        };
        match summarize_hfs_source(&path, offset, partition_size) {
            Ok(source) => {
                self.expand_hfs_dialog = Some(ExpandHfsDialog::new(source));
            }
            Err(e) => {
                ctx.log.error(format!("Cannot read source HFS volume: {e}"));
            }
        }
    }

    /// Run filesystem check on a partition and display results.
    fn run_fsck(
        &mut self,
        offset: u64,
        ptype: u8,
        type_string: Option<String>,
        ctx: &mut TabContext,
    ) {
        let source_path = self
            .selected_device_idx
            .and_then(|idx| ctx.devices.get(idx))
            .map(|d| d.path.clone())
            .or_else(|| self.image_file_path.clone());

        let path = match source_path {
            Some(p) => p,
            None => {
                ctx.log.error("No source available for filesystem check");
                return;
            }
        };

        match rusty_backup::model::fsck_runner::run_fsck(
            &path,
            offset,
            ptype,
            type_string.as_deref(),
        ) {
            Ok(Some(result)) => {
                if result.is_clean() {
                    ctx.log.info(format!(
                        "Filesystem check: clean ({} files, {} dirs)",
                        result.stats.files_checked, result.stats.directories_checked,
                    ));
                } else {
                    let visible_warns = result.warnings.iter().filter(|w| !w.debug).count();
                    ctx.log.warn(format!(
                        "Filesystem check: {} error(s), {} warning(s)",
                        result.errors.len(),
                        visible_warns,
                    ));
                }
                self.fsck_result = Some(result);
                self.show_fsck_popup = true;
            }
            Ok(None) => {
                ctx.log
                    .info("Filesystem check not supported for this filesystem type");
            }
            Err(e) => {
                ctx.log.error(format!("{e:#}"));
            }
        }
    }

    /// Render the filesystem check results popup.
    fn render_chd_info_popup(&mut self, ui: &mut egui::Ui) {
        let Some(text) = self.chd_info_text.clone() else {
            return;
        };
        let mut open = true;
        let mut buf = text.clone();
        egui::Window::new("CHD Info")
            .open(&mut open)
            .resizable(true)
            .default_width(560.0)
            .default_height(420.0)
            .show(ui.ctx(), |ui| {
                egui::ScrollArea::both()
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        ui.add(
                            egui::TextEdit::multiline(&mut buf)
                                .font(egui::TextStyle::Monospace)
                                .desired_width(f32::INFINITY)
                                .desired_rows(20)
                                .interactive(true),
                        );
                    });
            });
        if !open {
            self.chd_info_text = None;
        }
    }

    fn render_fsck_popup(&mut self, ui: &mut egui::Ui, ctx: &mut TabContext) {
        if !self.show_fsck_popup {
            return;
        }
        let mut open = true;
        let mut do_repair = false;
        egui::Window::new("Filesystem Check Results")
            .open(&mut open)
            .resizable(true)
            .default_width(500.0)
            .show(ui.ctx(), |ui| {
                if let Some(result) = &self.fsck_result {
                    if result.is_clean() {
                        ui.colored_label(
                            egui::Color32::from_rgb(100, 200, 100),
                            "Filesystem is clean.",
                        );
                    } else {
                        ui.colored_label(
                            egui::Color32::from_rgb(255, 100, 100),
                            format!("{} error(s) found.", result.errors.len()),
                        );
                    }

                    ui.separator();

                    // Stats
                    let mut stats_line = format!(
                        "Files: {}  Directories: {}",
                        result.stats.files_checked, result.stats.directories_checked,
                    );
                    for (label, value) in &result.stats.extra {
                        stats_line.push_str(&format!("  {}: {}", label, value));
                    }
                    ui.label(stats_line);

                    // Errors
                    if !result.errors.is_empty() {
                        ui.separator();
                        ui.label(egui::RichText::new("Errors:").strong());
                        egui::ScrollArea::vertical()
                            .id_salt("fsck_errors_inspect")
                            .max_height(200.0)
                            .show(ui, |ui| {
                                for issue in &result.errors {
                                    ui.colored_label(
                                        egui::Color32::from_rgb(255, 100, 100),
                                        format!("[{}] {}", issue.code, issue.message),
                                    );
                                }
                            });
                    }

                    // Warnings (filter out debug-level unless toggled)
                    let visible_warnings: Vec<_> = result
                        .warnings
                        .iter()
                        .filter(|w| !w.debug || self.show_fsck_debug)
                        .collect();
                    let has_debug = result.warnings.iter().any(|w| w.debug);
                    if !visible_warnings.is_empty() {
                        ui.separator();
                        ui.label(egui::RichText::new("Warnings:").strong());
                        egui::ScrollArea::vertical()
                            .id_salt("fsck_warnings_inspect")
                            .max_height(200.0)
                            .show(ui, |ui| {
                                for issue in &visible_warnings {
                                    if issue.debug {
                                        ui.colored_label(
                                            egui::Color32::from_rgb(150, 150, 150),
                                            format!("[DEBUG] {}", issue.message),
                                        );
                                    } else {
                                        ui.colored_label(
                                            egui::Color32::from_rgb(255, 200, 100),
                                            format!("[{}] {}", issue.code, issue.message),
                                        );
                                    }
                                }
                            });
                    }
                    if has_debug {
                        ui.checkbox(&mut self.show_fsck_debug, "Show debug messages");
                    }

                    // Repair button
                    if result.repairable {
                        ui.separator();
                        if ui.button("Repair").clicked() {
                            do_repair = true;
                        }
                    }

                    // Repair report
                    if let Some(ref report) = self.repair_report {
                        ui.separator();
                        ui.label(egui::RichText::new("Repair Report:").strong());
                        if !report.fixes_applied.is_empty() {
                            for fix in &report.fixes_applied {
                                ui.colored_label(
                                    egui::Color32::from_rgb(100, 200, 100),
                                    format!("  {}", fix),
                                );
                            }
                        }
                        if !report.fixes_failed.is_empty() {
                            for fail in &report.fixes_failed {
                                ui.colored_label(
                                    egui::Color32::from_rgb(255, 100, 100),
                                    format!("  {}", fail),
                                );
                            }
                        }
                    }
                }
            });
        if !open {
            self.show_fsck_popup = false;
        }
        if do_repair {
            self.show_repair_confirm = true;
        }

        // Repair confirmation dialog
        self.render_repair_confirm_inspect(ui, ctx);
    }

    /// Render the repair confirmation dialog for the inspect tab.
    fn render_repair_confirm_inspect(&mut self, ui: &mut egui::Ui, ctx: &mut TabContext) {
        if !self.show_repair_confirm {
            return;
        }
        let repairable_count = self
            .fsck_result
            .as_ref()
            .map(|r| r.errors.iter().filter(|e| e.repairable).count())
            .unwrap_or(0);

        let mut confirmed = false;
        let mut cancelled = false;
        egui::Window::new("Repair Filesystem?")
            .collapsible(false)
            .resizable(false)
            .show(ui.ctx(), |ui| {
                ui.label(format!(
                    "This will attempt to fix {} repairable error(s). \
                     Unrepairable issues (B-tree structural damage) will be skipped.\n\n\
                     Continue?",
                    repairable_count
                ));
                ui.horizontal(|ui| {
                    if ui.button("OK").clicked() {
                        confirmed = true;
                    }
                    if ui.button("Cancel").clicked() {
                        cancelled = true;
                    }
                });
            });

        if cancelled {
            self.show_repair_confirm = false;
        }
        if confirmed {
            self.show_repair_confirm = false;
            self.run_repair_inspect(ctx);
        }
    }

    /// Execute repair on the filesystem via inspect tab context.
    fn run_repair_inspect(&mut self, ctx: &mut TabContext) {
        let (offset, ptype, type_string) = match &self.repair_context {
            Some(ctx) => ctx.clone(),
            None => {
                ctx.log.error("No repair context available");
                return;
            }
        };

        let source_path = self
            .selected_device_idx
            .and_then(|idx| ctx.devices.get(idx))
            .map(|d| d.path.clone())
            .or_else(|| self.image_file_path.clone());

        let path = match source_path {
            Some(p) => p,
            None => {
                ctx.log.error("No source available for repair");
                return;
            }
        };

        match rusty_backup::model::fsck_runner::run_repair(
            &path,
            offset,
            ptype,
            type_string.as_deref(),
        ) {
            Ok(report) => {
                ctx.log.info(format!(
                    "Repair complete: {} fix(es) applied, {} failed",
                    report.fixes_applied.len(),
                    report.fixes_failed.len(),
                ));
                self.repair_report = Some(report);
                // Re-run fsck to show updated state
                self.run_fsck(offset, ptype, type_string, ctx);
            }
            Err(e) => {
                ctx.log.error(format!("{e:#}"));
            }
        }
    }

    /// Spawn a worker thread to compute the minimum size for a deferred partition.
    fn start_min_size_calc(&mut self, part_index: usize, ctx: &mut TabContext) {
        if self.pending_min_size_calcs.contains_key(&part_index) {
            return; // already running
        }
        let Some(part) = self
            .partitions
            .iter()
            .find(|p| p.index == part_index)
            .cloned()
        else {
            return;
        };
        let is_device = self
            .selected_device_idx
            .and_then(|idx| ctx.devices.get(idx))
            .map(|d| {
                let s = d.path.to_string_lossy();
                s.starts_with("/dev/") || s.starts_with("\\\\.\\")
            })
            .unwrap_or(false);

        // Prefer a CHD path source over the raw device handle: opening the
        // raw .chd file at partition_offset would read compressed bytes.
        let source = if let Some(chd_path) = self.chd_image_path.clone() {
            rusty_backup::model::min_size_runner::MinSizeSource::Chd(chd_path)
        } else if let Some(file_arc) = self.open_device_file.clone() {
            rusty_backup::model::min_size_runner::MinSizeSource::File {
                file: file_arc,
                use_sector_aligned: is_device,
            }
        } else {
            ctx.log.error(format!(
                "Cannot calculate minimum size for partition {part_index}: no open source handle",
            ));
            return;
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
    /// `partition_min_sizes` and clear the deferred entry.
    fn poll_min_size_calcs(&mut self, ctx: &mut TabContext) {
        // Emit a log line whenever a worker's `phase` advances so long HFS+
        // catalog walks don't look like a hang. Mirrors backup_tab.rs.
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
                    } else if let Some(min) = s.result {
                        let part_size = self
                            .partitions
                            .iter()
                            .find(|p| p.index == idx)
                            .map(|p| p.size_bytes)
                            .unwrap_or(min);
                        let clamped = min.min(part_size);
                        self.partition_min_sizes.insert(idx, clamped);
                        if let Some(d) = s.defragmented_min {
                            let d_clamped = d.min(part_size);
                            if d_clamped < clamped {
                                self.partition_defrag_min_sizes.insert(idx, d_clamped);
                                ctx.log.info(format!(
                                    "Partition {idx}: minimum size {} (computed); defragmented {}",
                                    partition::format_size(clamped),
                                    partition::format_size(d_clamped),
                                ));
                            } else {
                                ctx.log.info(format!(
                                    "Partition {idx}: minimum size {} (computed)",
                                    partition::format_size(clamped),
                                ));
                            }
                        } else {
                            ctx.log.info(format!(
                                "Partition {idx}: minimum size {} (computed)",
                                partition::format_size(clamped),
                            ));
                        }
                    } else {
                        ctx.log.info(format!(
                            "Partition {idx}: filesystem could not be opened for minimum-size calculation (last phase: {})",
                            s.phase,
                        ));
                    }
                }
                self.deferred_min_sizes.remove(&idx);
                self.last_logged_min_size_phase.remove(&idx);
            }
        }
    }

    /// Spawn one volume-label probe per HFS+ partition in the loaded backup.
    /// The worker reads the volume header + first catalog leaf out of the
    /// partition's data file (raw / zstd / per-partition CHD) and posts the
    /// label back through `pending_volume_label_probes`. Single-file-CHD
    /// backups bypass this path because they re-enter `run_inspect` which
    /// uses the synchronous live-disk probe.
    fn spawn_volume_label_probes(&mut self, ctx: &mut TabContext) {
        use rusty_backup::model::volume_label_runner::{
            spawn as spawn_label, VolumeLabelRequest, VolumeLabelSource,
        };

        let folder = match &self.backup_folder_path {
            Some(f) => f.clone(),
            None => return,
        };
        let meta = match &self.backup_metadata {
            Some(m) => m.clone(),
            None => return,
        };

        let compression = meta.compression_type.clone();
        for part in &self.partitions {
            // Only HFS+/HFSX partitions have a label worth probing here.
            // We use the same set of partition-type checks as the inspect-tab
            // live-disk path (Apple_HFS APM string + 0xAF MBR byte).
            let is_hfs_plus = part.partition_type_byte == 0xAF
                || part.partition_type_string.as_deref() == Some("Apple_HFS")
                || part.partition_type_string.as_deref() == Some("Apple_HFS+")
                || part.partition_type_string.as_deref() == Some("Apple_HFSX");
            if !is_hfs_plus {
                continue;
            }
            let pm = match meta.partitions.iter().find(|p| p.index == part.index) {
                Some(pm) => pm,
                None => continue,
            };
            // Skip partitions whose data file is split across multiple parts
            // (the probe needs random-access read; concatenating splits is
            // out of scope here).
            if pm.compressed_files.len() != 1 {
                continue;
            }
            let data_path = folder.join(&pm.compressed_files[0]);
            if !data_path.exists() {
                continue;
            }
            let source = match compression.as_str() {
                "none" => VolumeLabelSource::Raw {
                    path: data_path,
                    partition_offset: 0,
                },
                "zstd" => VolumeLabelSource::Zstd { path: data_path },
                "chd" => VolumeLabelSource::Chd {
                    path: data_path,
                    partition_offset: 0,
                },
                _ => continue,
            };
            let status = spawn_label(VolumeLabelRequest {
                source,
                partition_index: part.index,
            });
            self.pending_volume_label_probes.insert(part.index, status);
        }
        let count = self.pending_volume_label_probes.len();
        if count > 0 {
            ctx.log.info(format!(
                "Probing {count} HFS+ partition label(s) from backup data..."
            ));
        }
    }

    /// Poll all pending volume-label workers; on completion, append the label
    /// to the matching partition's `type_name` so the partition list shows
    /// "Apple_HFS (HFS+) Ariel-backup" instead of just the type.
    fn poll_volume_label_probes(&mut self, ctx: &mut TabContext) {
        let finished_indices: Vec<usize> = self
            .pending_volume_label_probes
            .iter()
            .filter_map(|(idx, status)| status.lock().ok().filter(|s| s.finished).map(|_| *idx))
            .collect();
        for idx in finished_indices {
            if let Some(status_arc) = self.pending_volume_label_probes.remove(&idx) {
                if let Ok(s) = status_arc.lock() {
                    if let Some(err) = &s.error {
                        ctx.log.warn(format!(
                            "Volume label probe failed for partition {idx}: {err}",
                        ));
                    } else if let Some(label) = s.label.clone() {
                        if let Some(part) = self.partitions.iter_mut().find(|p| p.index == idx) {
                            let new_name = apply_volume_label(&part.type_name, &label);
                            if part.type_name != new_name {
                                part.type_name = new_name;
                                self.partitions_layout_dirty = true;
                            }
                            ctx.log.info(format!("Partition {idx} label: {label}"));
                        }
                    }
                }
            }
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
        ctx: &mut TabContext,
    ) {
        // Case 1: device or raw image file
        let device_path = self
            .selected_device_idx
            .and_then(|idx| ctx.devices.get(idx))
            .map(|d| d.path.clone());
        let source_path = device_path.or_else(|| self.image_file_path.clone());
        if let Some(path) = source_path {
            ctx.log.info(format!(
                "Browsing partition {} from {} at offset {}",
                part_index,
                path.display(),
                offset,
            ));
            // macOS only: reuse the fd opened during inspect so BrowseView
            // never needs to re-open the raw device (which would re-prompt).
            // On Linux/Windows the app runs as root, so File::open works fine.
            #[cfg(target_os = "macos")]
            let preopen = if path.to_string_lossy().starts_with("/dev/") {
                self.open_device_file
                    .as_ref()
                    .and_then(|arc| arc.try_clone().ok())
            } else {
                None
            };
            #[cfg(not(target_os = "macos"))]
            let preopen = None;
            self.browse_view
                .open(path, offset, ptype, partition_type_string, preopen);
            // Surface the Amiga drive name ("DH0") in the browser header so
            // users tracking several RDB partitions can tell which one is
            // open without going back to the inspect grid. The volume label
            // (populated after open) gets concatenated as "DH0 - <label>".
            if let Some(part) = self.partitions.iter().find(|p| p.index == part_index) {
                if let Some(drv) = part.drv_name.as_deref() {
                    if !drv.is_empty() {
                        self.browse_view.set_label_prefix(drv.to_string());
                    }
                }
            }
            // If this CHD is the body of a single-file-chd backup, hand
            // the backup folder to BrowseView so a successful chd_edit
            // save refreshes metadata.json (per-partition checksums +
            // container SHA-1). Plain-CHD images won't have this set.
            if let Some(folder) = self.single_file_chd_backup_folder.clone() {
                self.browse_view.set_single_file_chd_backup_folder(folder);
            }
            return;
        }

        // Case 3: Clonezilla image — use seekable zstd cache
        if self.clonezilla_image.is_some() {
            self.open_browse_clonezilla(part_index, ptype, ctx);
            return;
        }

        // Case 2: native backup folder — find the partition's data file
        let (folder, meta) = match (&self.backup_folder_path, &self.backup_metadata) {
            (Some(f), Some(m)) => (f.clone(), m.clone()),
            _ => {
                ctx.log.error(format!(
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
                ctx.log.error(format!(
                    "partition-{}: not found in backup metadata",
                    part_index,
                ));
                return;
            }
        };

        if part_meta.compressed_files.is_empty() {
            ctx.log.error(format!(
                "partition-{}: no data files listed in backup metadata",
                part_index,
            ));
            return;
        }

        // Split files not supported for browsing
        if part_meta.compressed_files.len() > 1 {
            ctx.log.warn(format!(
                "partition-{}: browsing split backup files is not supported (files: {})",
                part_index,
                part_meta.compressed_files.join(", "),
            ));
            return;
        }

        let data_file = &part_meta.compressed_files[0];
        let data_path = folder.join(data_file);

        if !data_path.exists() {
            ctx.log.error(format!(
                "partition-{}: data file not found: {}",
                part_index,
                data_path.display(),
            ));
            return;
        }

        let metadata_path = folder.join("metadata.json");
        let compression_type_str = meta.compression_type.clone();
        let checksum_type = meta.checksum_type.clone();

        match meta.compression_type.as_str() {
            "none" => {
                // Raw file — partition data starts at offset 0
                ctx.log.info(format!(
                    "Browsing partition {} from {}",
                    part_index, data_file,
                ));
                self.browse_view.open(
                    data_path.clone(),
                    0,
                    ptype,
                    partition_type_string.clone(),
                    None,
                );
                // Raw backups support direct editing (no decompress needed)
            }
            "zstd" => {
                // Zstd-compressed backup — open streaming immediately while
                // seekable cache builds in the background
                self.open_browse_zstd(
                    part_index,
                    ptype,
                    partition_type_string,
                    &data_path,
                    &folder,
                    &format!("partition-{}", part_index),
                    ctx,
                );
                // Set archive edit context for decompress→edit→recompress flow
                self.browse_view.set_archive_edit_context(
                    data_path,
                    compression_type_str,
                    part_meta.original_size_bytes,
                    part_meta.compacted,
                    metadata_path,
                    part_index,
                    checksum_type,
                );
            }
            "chd" | "chd-dvd" => {
                // CHD-compressed backup — open directly via ChdReader (on-demand
                // decompression). Editing flows through `chd_edit::ChdEditSession`
                // (diff-against-parent for compressed, in-place for uncompressed),
                // so no `archive_edit_ctx` is set here. NOTE: backup metadata.json
                // checksum / compressed_files entries are NOT auto-updated after
                // a CHD edit yet — that lives on the Phase 2 follow-up TODO.
                ctx.log.info(format!(
                    "Browsing partition {} from CHD: {}",
                    part_index, data_file,
                ));
                self.browse_view.open(
                    data_path.clone(),
                    0,
                    ptype,
                    partition_type_string.clone(),
                    None,
                );
                let _ = (metadata_path, checksum_type, compression_type_str);
            }
            "woz" => {
                ctx.log.info(format!(
                    "Browsing partition {} from WOZ: {}",
                    part_index, data_file,
                ));
                self.browse_view.open(
                    data_path.clone(),
                    0,
                    ptype,
                    partition_type_string.clone(),
                    None,
                );
                self.browse_view.set_archive_edit_context(
                    data_path,
                    compression_type_str,
                    part_meta.original_size_bytes,
                    part_meta.compacted,
                    metadata_path,
                    part_index,
                    checksum_type,
                );
            }
            other => {
                ctx.log.warn(format!(
                    "partition-{}: browsing {} compressed backups is not yet supported (file: {})",
                    part_index, other, data_file,
                ));
            }
        }
    }

    /// Open browse for a Clonezilla partition using seekable zstd cache.
    fn open_browse_clonezilla(&mut self, part_index: usize, ptype: u8, ctx: &mut TabContext) {
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
                ctx.log.error(format!(
                    "partition-{}: not found in Clonezilla image",
                    part_index,
                ));
                return;
            }
        };

        if cz_part.partclone_files.is_empty() {
            ctx.log
                .error(format!("partition-{}: no partclone data files", part_index,));
            return;
        }

        // Check if block cache already exists in memory
        if let Some(cache) = self.block_caches.get(&part_index) {
            if let Ok(c) = cache.lock() {
                if c.state == clonezilla::block_cache::CacheState::Ready {
                    drop(c);
                    ctx.log.info(format!(
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
            ctx.log
                .warn("A metadata scan is already in progress. Please wait.");
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
                    ctx.log.info(format!(
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
        ctx.log.info(format!(
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
            let _wake =
                rusty_backup::os::wakelock::acquire("Rusty Backup: Clonezilla metadata scan");
            let result = clonezilla::metadata_scan::scan_metadata(
                &cache_for_thread,
                ptype,
                Some(&cache_path),
            );
            if let Ok(mut s) = scan.lock() {
                s.finished = true;
                if let Err(e) = result {
                    s.error = Some(format!("{e:#}"));
                }
            }
        });
    }

    /// Open browse for a native zstd-compressed backup.
    ///
    /// If a seekable cache already exists, opens it directly.  Otherwise,
    /// opens the browser immediately via a streaming reader and starts a
    /// background thread to build the seekable cache.  When the background
    /// build completes, `poll_cache_status` will upgrade the browser
    /// automatically.
    fn open_browse_zstd(
        &mut self,
        part_index: usize,
        ptype: u8,
        partition_type_string: Option<String>,
        data_path: &PathBuf,
        folder: &PathBuf,
        cache_name: &str,
        ctx: &mut TabContext,
    ) {
        // 1. If seekable cache already exists, use it directly
        if let Some(cache_path) = self.seekable_cache_files.get(&part_index).cloned() {
            if cache_path.exists() {
                ctx.log.info(format!(
                    "Browsing partition {} from cached seekable file",
                    part_index,
                ));
                self.browse_view
                    .open(cache_path, 0, ptype, partition_type_string.clone(), None);
                return;
            }
            // Cache file was deleted (stale) — remove from map and recreate
            self.seekable_cache_files.remove(&part_index);
        }

        // 2. If a cache build is already running, warn and return
        if self.cache_status.is_some() {
            ctx.log
                .warn("A seekable cache is already being created. Please wait.");
            return;
        }

        // 3. Open the browser immediately via streaming reader
        ctx.log.info(format!(
            "Opening partition {} via streaming reader (seekable cache building in background)...",
            part_index,
        ));
        self.browse_view
            .open_streaming(data_path.clone(), ptype, partition_type_string.clone());

        // 4. Start background thread to build seekable cache
        let cache_path = folder.join(format!("_{cache_name}.seekable.zst"));
        let total_bytes = std::fs::metadata(data_path).map(|m| m.len()).unwrap_or(0);

        ctx.log.info(format!(
            "Creating seekable cache for partition {} in the background...",
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
        self.cache_rate.reset();

        let data_path = data_path.clone();
        std::thread::spawn(move || {
            let _wake =
                rusty_backup::os::wakelock::acquire("Rusty Backup: build zstd seekable cache");
            let result = create_seekable_cache_from_zstd(&data_path, &cache_path, &status);
            if let Ok(mut s) = status.lock() {
                s.finished = true;
                if let Err(e) = result {
                    s.error = Some(format!("{e:#}"));
                }
            }
        });
    }

    fn poll_block_cache_scan(&mut self, ctx: &mut TabContext) {
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
                ctx.log.error(format!(
                    "Metadata scan failed for partition {}: {err}",
                    part_index,
                ));
            } else {
                ctx.log.info(format!(
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

    fn poll_cache_status(&mut self, ctx: &mut TabContext) {
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
                ctx.log.error(format!(
                    "Failed to create seekable cache for partition {}: {err}",
                    part_index,
                ));
                drop(status);
                self.cache_status = None;
            } else if let Some(cache_path) = &status.cache_path {
                ctx.log.info(
                    "Seekable cache ready — browser upgraded to full seek support.".to_string(),
                );
                let cache_path = cache_path.clone();
                self.seekable_cache_files
                    .insert(part_index, cache_path.clone());
                drop(status);
                self.cache_status = None;
                self.browse_view.upgrade_to_seekable_cache(cache_path);
            } else {
                drop(status);
                self.cache_status = None;
            }
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
        0x01 | 0x04
            | 0x06
            | 0x07
            | 0x0B
            | 0x0C
            | 0x0E
            | 0x11
            | 0x14
            | 0x16
            | 0x1B
            | 0x1C
            | 0x1E
            | 0x83
            | 0xA0
            | 0xA1
            | 0xA8
            | 0xAF
    )
}

/// Heuristic: is this partition classic HFS (not HFS+/HFSX)?
/// Used to gate the "Expand HFS Volume…" action, which only handles classic
/// HFS. APM rows where `probe_apple_hfs_type` flagged HFS+/HFSX get a tag in
/// `type_name` like `Apple_HFS (HFS+)`; those are excluded here.
fn is_classic_hfs(ptype: u8, type_string: Option<&str>, type_name: &str) -> bool {
    let apm_hfs = type_string
        .map(|s| s.eq_ignore_ascii_case("Apple_HFS"))
        .unwrap_or(false);
    let mbr_hfs = ptype == 0xAF;
    if !(apm_hfs || mbr_hfs) {
        return false;
    }
    !(type_name.contains("HFS+") || type_name.contains("HFSX"))
}

/// Check if an APM partition type string corresponds to a browsable filesystem.
fn is_browsable_type_string(type_str: Option<&str>) -> bool {
    let Some(s) = type_str else {
        return false;
    };
    // AmigaDOS DosType tags (DOS\0..DOS\7) — RDB-partitioned hard drives and
    // single-partition HDFs / ADFs both route through this string.
    if rusty_backup::fs::is_amiga_dos_type(s) {
        return true;
    }
    if rusty_backup::fs::is_amiga_pfs3_type(s) {
        return true;
    }
    if rusty_backup::fs::is_amiga_sfs_type(s) {
        return true;
    }
    matches!(
        s,
        "Apple_HFS"
            | "Apple_HFSX"
            | "Apple_HFS+"
            | "Apple_UNIX_SVR2"
            | "Apple_UNIX_SRVR2"
            | "Apple_PRODOS"
            | "Apple_ProDOS"
            // GPT "Linux Filesystem" GUID — ext, btrfs, or xfs at runtime.
            | "0FC63DAF-8483-4772-8E79-3D69D8477DE4"
    )
}

/// Check if a superfloppy (type byte 0) has a browsable filesystem hint.
fn is_browsable_superfloppy(ptype: u8, type_name: &str) -> bool {
    if ptype != 0 {
        return false;
    }
    matches!(
        type_name,
        "FAT" | "HFS" | "HFS+" | "NTFS" | "exFAT" | "ProDOS" | "XFS" | "ext" | "btrfs" | "Unknown"
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

/// Check if a partition type supports filesystem checking (fsck).
/// Classic HFS (0xAF or APM "Apple_HFS") and AmigaDOS OFS/FFS variants.
fn is_checkable_type(ptype: u8, type_str: Option<&str>) -> bool {
    if ptype == 0xAF || matches!(type_str, Some("Apple_HFS")) {
        return true;
    }
    type_str
        .map(rusty_backup::fs::is_amiga_dos_type)
        .unwrap_or(false)
}

/// Format an HFS allocation block size as "N KiB" when it's a whole
/// number of kibibytes, else "N B".
fn format_block_size(bytes: u32) -> String {
    if bytes >= 1024 && bytes.is_multiple_of(1024) {
        format!("{} KiB", bytes / 1024)
    } else {
        format!("{} B", bytes)
    }
}

/// Apply a probed volume label to a partition's `type_name`, replacing any
/// trailing parenthetical garbage that older metadata.json files baked in.
///
/// Old backups can carry `type_name` strings like
/// `Apple_HFS (HFS+) (Apple_HFS (HFS+)_Untitled_1)` because the inspect-tab
/// relabel logic that produces them ran before the auto-generated APM-name
/// suppression existed. Once we have a real volume label from a fresh probe,
/// the noise parenthetical adds no information — strip it and append the
/// label so the row reads `Apple_HFS (HFS+) Ariel-backup`.
fn apply_volume_label(type_name: &str, label: &str) -> String {
    let trimmed = strip_apm_noise_parenthetical(type_name);
    if trimmed.trim_end().ends_with(label) {
        trimmed.to_string()
    } else {
        format!("{} {}", trimmed.trim_end(), label)
    }
}

/// Drop any trailing parenthetical that lives *after* the variant tag for
/// HFS-family type names. `Apple_HFS (HFS+) (Untitled_1)` -> `Apple_HFS (HFS+)`,
/// `Apple_HFS (HFSX) (whatever)` -> `Apple_HFS (HFSX)`, leave non-HFS rows
/// alone.
fn strip_apm_noise_parenthetical(s: &str) -> String {
    for tag in ["(HFS+)", "(HFSX)", "(HFS)"] {
        if let Some(idx) = s.find(tag) {
            let end = idx + tag.len();
            return s[..end].to_string();
        }
    }
    s.to_string()
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

/// Build the `Vec<Segment>` driving the inspect tab's disk-layout bar from the
/// current partition list and any volume labels that have been probed.
///
/// Partitions are listed in their existing order (which already reflects the
/// partition table). Extended-container entries are skipped. APM
/// `Apple_partition_map` and SGI volume-header entries render dimmed. Color
/// indices are assigned sequentially across the real (non-dimmed) partitions
/// so the palette cycles predictably.
fn build_disk_layout_segments(
    partitions: &[PartitionInfo],
    volume_labels: &HashMap<usize, String>,
    actual_disk_size: Option<u64>,
) -> Vec<super::partition_bar::Segment> {
    use super::partition_bar::{Segment, SegmentKind};

    let mut color_index: usize = 0;
    let mut segments = Vec::with_capacity(partitions.len());
    let mut max_end_bytes: u64 = 0;
    for part in partitions {
        if part.is_extended_container {
            continue;
        }
        let part_end = part
            .start_lba
            .saturating_mul(512)
            .saturating_add(part.size_bytes);
        if part_end > max_end_bytes {
            max_end_bytes = part_end;
        }
        let label = volume_labels
            .get(&part.index)
            .cloned()
            .or_else(|| part.drv_name.clone())
            .unwrap_or_else(|| format!("Partition {}", part.index + 1));
        let fs = part.type_name.clone();

        let is_dimmed = matches!(
            part.partition_type_string.as_deref(),
            Some("Apple_partition_map") | Some("SGI_volhdr") | Some("SGI_volume"),
        );
        let kind = if is_dimmed {
            SegmentKind::Dimmed
        } else {
            let k = SegmentKind::Partition { color_index };
            color_index += 1;
            k
        };
        segments.push(Segment {
            label,
            fs,
            size_bytes: part.size_bytes,
            kind,
        });
    }

    // Trailing free space at the end of the disk — only surfaced when we
    // know the real disk size (from backup metadata, the loaded file, or a
    // live device). Threshold of 1 MiB avoids cluttering the bar with the
    // rounding-noise gaps that show up on real disks (e.g. GPT secondary
    // header reserves 33 sectors; partition alignment may leave 1-2 MiB
    // unused — those are not user-actionable "free space").
    if let Some(disk_size) = actual_disk_size {
        if disk_size > max_end_bytes {
            let free = disk_size - max_end_bytes;
            if free >= 1024 * 1024 {
                segments.push(Segment {
                    label: String::new(),
                    fs: String::new(),
                    size_bytes: free,
                    kind: SegmentKind::Free,
                });
            }
        }
    }

    segments
}

/// Render the Current vs After PartitionBar pair inside the Export Disk
/// Image popup. "After" reuses each `PartitionExportConfig::effective_size`,
/// so what the user sees in the bar is exactly the size the export engine
/// will write.
fn show_export_disk_layout_bars(
    ui: &mut egui::Ui,
    partitions: &[PartitionInfo],
    configs: &[PartitionExportConfig],
) {
    use super::partition_bar::{PartitionBar, Segment, SegmentKind};

    let mut current = Vec::new();
    let mut color_index = 0usize;
    for p in partitions {
        if p.is_extended_container || p.is_logical {
            continue;
        }
        let kind = SegmentKind::Partition { color_index };
        color_index += 1;
        current.push(Segment {
            label: format!("Partition {}", p.index + 1),
            fs: p.type_name.clone(),
            size_bytes: p.size_bytes,
            kind,
        });
    }

    let mut after = Vec::new();
    color_index = 0;
    for p in partitions {
        if p.is_extended_container || p.is_logical {
            continue;
        }
        let cfg = configs.iter().find(|c| c.index == p.index);
        let size = match cfg {
            Some(c) => {
                let eff = c.effective_size();
                if eff == 0 {
                    // FillRemaining — keep the segment visible at original
                    // size so it doesn't disappear from the bar.
                    p.size_bytes
                } else {
                    eff
                }
            }
            None => p.size_bytes,
        };
        let kind = SegmentKind::Partition { color_index };
        color_index += 1;
        after.push(Segment {
            label: format!("Partition {}", p.index + 1),
            fs: p.type_name.clone(),
            size_bytes: size,
            kind,
        });
    }

    let current_total: u64 = current.iter().map(|s| s.size_bytes).sum();
    let after_total: u64 = after.iter().map(|s| s.size_bytes).sum();
    let max_total = current_total.max(after_total).max(1);
    let available_width = ui.available_width().max(120.0);

    ui.label("Current:");
    let current_w = available_width * (current_total as f64 / max_total as f64) as f32;
    ui.scope(|ui| {
        ui.set_width(current_w.max(60.0));
        PartitionBar {
            segments: current,
            show_inline_labels: true,
            show_legend: false,
        }
        .show(ui);
    });

    ui.add_space(4.0);
    ui.label(format!(
        "After  ({} -> {}):",
        partition::format_size(current_total),
        partition::format_size(after_total),
    ));
    let after_w = available_width * (after_total as f64 / max_total as f64) as f32;
    ui.scope(|ui| {
        ui.set_width(after_w.max(60.0));
        PartitionBar {
            segments: after,
            show_inline_labels: true,
            show_legend: true,
        }
        .show(ui);
    });
}

/// Render the Current vs After PartitionBar pair inside the partition-table
/// editor popup. "Current" mirrors the loaded `partitions` list; "After"
/// walks the editor's working entries — applying parsed sizes, hiding
/// deleted entries, and appending the pending Add-Partition row when its
/// size field has a positive value. Both bars share the byte-per-pixel
/// scale, so the After bar grows/shrinks visibly with the working edits.
fn show_editor_disk_layout_bars(
    ui: &mut egui::Ui,
    partitions: &[PartitionInfo],
    editor: &rusty_backup::model::partition_editor::PartitionEditor,
) {
    use super::partition_bar::{PartitionBar, Segment, SegmentKind};

    fn parse_mib(text: &str) -> Option<u64> {
        text.trim()
            .parse::<f64>()
            .ok()
            .filter(|v| *v > 0.0)
            .map(|v| ((v * 1024.0 * 1024.0) as u64 / 512) * 512)
    }

    // Current bar from the loaded partition list (same shape as the
    // inspect-tab disk-layout bar minus the trailing-free segment, since
    // the editor's free space is implicit in the bar-pair scale).
    let mut current = Vec::new();
    let mut color_index = 0usize;
    for p in partitions {
        if p.is_extended_container || p.is_logical {
            continue;
        }
        let kind = SegmentKind::Partition { color_index };
        color_index += 1;
        current.push(Segment {
            label: format!("Partition {}", p.index + 1),
            fs: p.type_name.clone(),
            size_bytes: p.size_bytes,
            kind,
        });
    }

    // After bar from the editor working state. We preserve original entry
    // order (partition table order) and reuse the same color cycle so the
    // before/after pair stays visually aligned.
    let mut after = Vec::new();
    color_index = 0;
    for entry in &editor.entries {
        if entry.deleted || entry.is_extended_container || entry.is_logical {
            // Reserve the color slot anyway so the next entry's color
            // matches its Current counterpart.
            if !entry.is_extended_container && !entry.is_logical {
                color_index += 1;
            }
            continue;
        }
        let size_bytes = parse_mib(&entry.size_text).unwrap_or(entry.size_bytes);
        let kind = SegmentKind::Partition { color_index };
        color_index += 1;
        after.push(Segment {
            label: format!("Partition {}", entry.index + 1),
            fs: entry.type_name.clone(),
            size_bytes,
            kind,
        });
    }

    // Pending Add Partition row (rendered as a sequential segment in the
    // current color slot). Surfaces while the user is filling in the
    // Add-Partition fields, so they can see the addition in the bar before
    // clicking Validate.
    if let Some(add_size) = parse_mib(&editor.add_size_mb) {
        after.push(Segment {
            label: "New".to_string(),
            fs: if editor.add_type.trim().is_empty() {
                String::new()
            } else {
                editor.add_type.trim().to_string()
            },
            size_bytes: add_size,
            kind: SegmentKind::Partition { color_index },
        });
    }

    let current_total: u64 = current.iter().map(|s| s.size_bytes).sum();
    let after_total: u64 = after.iter().map(|s| s.size_bytes).sum();
    let max_total = current_total.max(after_total).max(1);
    let available_width = ui.available_width().max(120.0);

    ui.label("Current:");
    let current_w = available_width * (current_total as f64 / max_total as f64) as f32;
    ui.scope(|ui| {
        ui.set_width(current_w.max(60.0));
        PartitionBar {
            segments: current,
            show_inline_labels: true,
            show_legend: false,
        }
        .show(ui);
    });

    ui.add_space(4.0);
    ui.label(format!(
        "After  ({} -> {}):",
        partition::format_size(current_total),
        partition::format_size(after_total),
    ));
    let after_w = available_width * (after_total as f64 / max_total as f64) as f32;
    ui.scope(|ui| {
        ui.set_width(after_w.max(60.0));
        PartitionBar {
            segments: after,
            show_inline_labels: true,
            show_legend: true,
        }
        .show(ui);
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_volume_label_strips_legacy_apm_garbage() {
        // Old metadata carried a doubly-nested parenthetical from the days
        // before the relabel logic suppressed Disk-Utility-default names.
        assert_eq!(
            apply_volume_label(
                "Apple_HFS (HFS+) (Apple_HFS (HFS+)_Untitled_1)",
                "Ariel-backup",
            ),
            "Apple_HFS (HFS+) Ariel-backup",
        );
    }

    #[test]
    fn apply_volume_label_appends_to_clean_name() {
        assert_eq!(
            apply_volume_label("Apple_HFS (HFS+)", "Ariel-backup"),
            "Apple_HFS (HFS+) Ariel-backup",
        );
    }

    #[test]
    fn apply_volume_label_idempotent_when_already_present() {
        assert_eq!(
            apply_volume_label("Apple_HFS (HFS+) Ariel-backup", "Ariel-backup"),
            "Apple_HFS (HFS+) Ariel-backup",
        );
    }

    #[test]
    fn apply_volume_label_handles_hfsx_and_classic_hfs() {
        assert_eq!(
            apply_volume_label("Apple_HFS (HFSX) (Untitled_2)", "Foo"),
            "Apple_HFS (HFSX) Foo",
        );
        assert_eq!(
            apply_volume_label("Apple_HFS (HFS) (Old)", "Bar"),
            "Apple_HFS (HFS) Bar",
        );
    }

    #[test]
    fn apply_volume_label_non_hfs_left_alone() {
        // No HFS variant tag found -> nothing to strip; just append.
        assert_eq!(apply_volume_label("Linux", "label"), "Linux label");
    }
}
