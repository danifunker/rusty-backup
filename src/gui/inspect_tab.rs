use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::Context;

use rusty_backup::backup::metadata::BackupMetadata;
// Partition-capability gates — shared across all UIs, defined in the engine.
use rusty_backup::clonezilla::metadata::ClonezillaImage;
use rusty_backup::fs::{
    is_checkable_type, is_classic_hfs, is_superfloppy_hfs, partition_is_browsable,
};
use rusty_backup::model::cache_runner;
use rusty_backup::model::commander_source::{self, ClonezillaOpen, PartcloneLookup};
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

use super::alto_resize_dialog::AltoResizeDialog;
use super::browse_view::BrowseView;
use super::context::TabContext;
use super::expand_hfs_dialog::{summarize_source as summarize_hfs_source, ExpandHfsDialog};
use super::floppy_convert_dialog::FloppyConvertDialog;
use super::physical_disk_export::{PhysicalDiskExport, PhysicalDiskExportSource};

/// Upper size bound for a file to be considered a floppy container by
/// [`InspectTab::detect_source_is_floppy`]. The largest format we handle
/// is a 1.44 MB 2HD image (1,474,560 B flat); D88's per-sector overhead
/// pushes a worst-case `.d88` to roughly 1.52 MB. 2 MiB leaves headroom
/// for any odd dump while still excluding every HDD image (the smallest
/// vintage HDDs are several MB), which is what stops a zeroed-header
/// `.hdf` from false-positiving against the loose D88 sniffer.
const MAX_FLOPPY_CONTAINER_BYTES: u64 = 2 * 1024 * 1024;

/// State for the Inspect tab.
/// A source change (open or close) deferred behind the unsaved-edits confirm,
/// so the live source is never mutated — and a new disk never partially loads —
/// until the user resolves the prompt.
enum PendingSourceChange {
    Open(super::source_picker::SourceEvent),
    Close,
}

/// True when `path` is a classic Mac archive (StuffIt / BinHex / Compact Pro)
/// by extension — those route to the Archives tab, not disk inspection.
fn is_mac_archive_path(path: &std::path::Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .map(|ext| {
            rusty_backup::model::file_types::MAC_ARCHIVE_EXTS
                .iter()
                .any(|m| m.eq_ignore_ascii_case(ext))
        })
        .unwrap_or(false)
}

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
    /// True when the currently-loaded image file sniffs as a floppy
    /// container (XDF / HDM / DIM / D88). Drives the visibility of the
    /// "Convert Floppy Container..." button so it only appears when there
    /// is actually a floppy to convert. Recomputed when the source path
    /// changes; `false` for devices, backups, and non-floppy images.
    source_is_floppy: bool,
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
    /// A source open/close held until the user resolves the unsaved-edits
    /// prompt (deferred-switch guard). `None` when nothing is pending.
    pending_source_change: Option<PendingSourceChange>,
    /// A Mac-archive file the user picked in the source dropdown — the app
    /// consumes this to switch to the Archives tab instead of inspecting it as
    /// a disk image. `None` when nothing is pending.
    pending_open_archive: Option<PathBuf>,
    /// Filesystem browser
    browse_view: BrowseView,
    /// "Connect to Remote..." — an inline panel browsing an `rb-cli serve`
    /// daemon (connect + pick an image on one persistent connection).
    remote_browser: super::remote_browser::RemoteBrowsePanel,
    /// When set, the inspected source is a **remote image** opened over the
    /// block tier: `(shared connection, remote path)`. `run_inspect` builds a
    /// `RemoteBlockReader` from it and parses the partition table over the wire,
    /// so the whole Inspect view works on a remote image with no download.
    remote_inspect: Option<(
        std::sync::Arc<std::sync::Mutex<rusty_backup::remote::RemoteConnection>>,
        String,
    )>,
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
    /// Scanned Clonezilla block caches reused across opens, plus the
    /// in-memory/on-disk/scan decision tree — the same shared store Commander
    /// uses (see `commander_source::PartcloneCacheStore`).
    cache_store: commander_source::PartcloneCacheStore,
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
    /// "Resize Alto Disk…" dialog (Alto BFS only).
    alto_resize_dialog: Option<AltoResizeDialog>,
    /// "Convert Floppy Container..." dialog (XDF/HDM/DIM/D88).
    floppy_convert_dialog: Option<FloppyConvertDialog>,
    /// Pending Human68k defragment awaiting confirmation: (partition byte
    /// offset, partition label). `Some` while the confirm modal is open.
    pending_repack: Option<(u64, String)>,
    /// Status of the background Human68k defragment (repack) worker. `None`
    /// when none is running.
    repack_status: Option<Arc<Mutex<rusty_backup::model::status::RepackStatus>>>,
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
    /// Status of a background "Back Up Image…" pull of the currently-inspected
    /// remote image to a local folder. `None` when none is running.
    remote_backup_status: Option<Arc<Mutex<rusty_backup::backup::BackupProgress>>>,
}

impl Default for InspectTab {
    fn default() -> Self {
        Self {
            selected_device_idx: None,
            image_file_path: None,
            amiga_tempdir: None,
            image_format_label: None,
            source_is_floppy: false,
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
            pending_source_change: None,
            pending_open_archive: None,
            browse_view: BrowseView::default(),
            remote_browser: super::remote_browser::RemoteBrowsePanel::default(),
            remote_inspect: None,
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
            cache_store: commander_source::PartcloneCacheStore::new(),
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
            alto_resize_dialog: None,
            floppy_convert_dialog: None,
            pending_repack: None,
            repack_status: None,
            chd_info_text: None,
            single_file_chd_backup_folder: None,
            physical_disk_export: PhysicalDiskExport::default(),
            remote_backup_status: None,
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

    /// True when a source (image file, backup folder, or selected physical
    /// device) is currently loaded in the Inspect tab. The App uses this to
    /// decide whether a dropped file should open as a NEW source (only when
    /// nothing is loaded) or be left to the filesystem browser's own
    /// drag-and-drop handler (which adds the file into the open volume while
    /// in edit mode). Without this gate the App's drop handler would re-open
    /// the dropped file over the loaded one, making drag-to-add impossible.
    pub fn has_loaded_source(&self) -> bool {
        self.image_file_path.is_some()
            || self.backup_folder_path.is_some()
            || self.selected_device_idx.is_some()
    }

    /// Returns and clears the "user clicked Close Backup" signal. The App-level
    /// update loop calls this each frame to know when to clear the cross-tab
    /// `loaded_backup_folder` (which would otherwise re-open the backup via
    /// the auto-load fallback in `gui::mod::update`).
    pub fn take_close_backup_request(&mut self) -> bool {
        std::mem::take(&mut self.close_backup_requested)
    }

    /// Returns and clears a Mac-archive file the user picked in the source
    /// dropdown. The app routes it to the Archives tab (a `.sit`/`.hqx` isn't a
    /// disk image, so inspecting it would just fail).
    pub fn take_open_archive_request(&mut self) -> Option<PathBuf> {
        self.pending_open_archive.take()
    }

    pub fn load_backup(&mut self, path: &PathBuf) {
        if self.backup_folder_path.as_ref() != Some(path) {
            self.backup_folder_path = Some(path.clone());
            // Force reload on next show
            self.prev_backup_path = None;
        }
    }

    /// Programmatically open a disk-image path in this tab, optionally
    /// owning a [`tempfile::TempDir`] that contains it. Used by the
    /// Archives tab's "Mount in new Inspect tab" auto-unwrap hook
    /// (Workflow D.2) to hand off a decoded payload extracted from a
    /// `.hqx` / `.sit` / `.sea` to this tab without going through the
    /// user-facing file-picker. The tempdir guard is kept alive on this
    /// tab so the temp file outlives the Archives tab's frame; clearing
    /// the tab or picking a new file drops it.
    pub fn load_image_with_tempdir(&mut self, path: PathBuf, guard: Option<tempfile::TempDir>) {
        self.selected_device_idx = None;
        self.backup_folder_path = None;
        self.image_file_path = Some(path);
        self.amiga_tempdir = guard;
        self.clear_results();
    }

    /// Sniff `path` to decide whether it is a floppy container (XDF / HDM
    /// / DIM / D88) and thus a valid source for the floppy converter.
    ///
    /// Two gates, both required:
    ///  1. **Size.** `detect_container_kind`'s D88 / DIM sniffers are
    ///     deliberately loose (D88 has no magic string — an all-zero
    ///     header trivially matches "empty 2D disk"), so a raw HDD image
    ///     with a zeroed header region would otherwise false-positive.
    ///     Real floppy containers top out around 1.5 MB (1.44 MB flat
    ///     plus D88 per-sector overhead), so anything larger than
    ///     [`MAX_FLOPPY_CONTAINER_BYTES`] cannot be one. This is what
    ///     keeps X68000 `.hdf` images (and every other HDD) from showing
    ///     the button.
    ///  2. **Container kind.** Within the size bound, the header + the
    ///     extension must classify as one of the four floppy formats.
    ///
    /// Returns `false` on any read error.
    fn detect_source_is_floppy(path: &Path) -> bool {
        // Size gate first — a stat() is cheaper than a read and rejects
        // every HDD image outright.
        match std::fs::metadata(path) {
            Ok(m) if m.len() <= MAX_FLOPPY_CONTAINER_BYTES => {}
            _ => return false,
        }
        let mut head = [0u8; 256];
        let n = match File::open(path).and_then(|mut f| f.read(&mut head)) {
            Ok(n) => n,
            Err(_) => return false,
        };
        let kind =
            rusty_backup::rbformats::containers::detect_container_kind(&head[..n], Some(path));
        rusty_backup::rbformats::containers::is_floppy_container(kind)
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
        self.cache_store.clear();
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

            let current_label = if let Some((_, rpath)) = &self.remote_inspect {
                let base = rpath.rsplit('/').find(|s| !s.is_empty()).unwrap_or(rpath);
                format!("Remote image: {base}")
            } else if let Some(path) = &self.backup_folder_path {
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

            // Shared source picker (R1): same ComboBox widget the Commander
            // panes use. It owns the rfd dialogs + image filter/materialize and
            // emits a SourceEvent; we map that onto our source state here.
            let cfg = super::source_picker::PickerConfig {
                show_devices: true,
                show_image: true,
                show_host_folder: false,
                show_backup_folder: true,
                show_remote: true,
                materialize_image: true,
                include_mac_archives: true,
                width: 400.0,
            };
            let state = super::source_picker::PickerState {
                selected_device_idx: self.selected_device_idx,
                image_active: self.image_file_path.is_some(),
                host_active: false,
                backup_active: self.backup_folder_path.is_some(),
                devices: ctx.devices,
            };
            if let Some(ev) =
                super::source_picker::show(ui, "inspect_source", &cfg, &current_label, &state)
            {
                // "Connect to Remote..." opens the inline remote panel; any other
                // source ends a remote session and switches to that local source.
                if matches!(ev, super::source_picker::SourceEvent::Remote) {
                    self.remote_browser.open();
                } else {
                    self.remote_browser.disconnect();
                    self.remote_inspect = None;
                    if self.browse_view.has_unsaved_edits() {
                        // Gate the switch on unsaved browse edits: defer it behind
                        // the confirm prompt so the live source is untouched (and
                        // no new disk partially loads) until the user resolves it.
                        self.pending_source_change = Some(PendingSourceChange::Open(ev));
                    } else {
                        self.apply_source_event(ev);
                    }
                }
            }

            // Remote picker controls (Disconnect) sit beside the source dropdown
            // while connecting / picking.
            self.remote_browser.source_bar_controls(ui);

            // While inspecting a remote image: switch to another image on the
            // SAME connection (no reconnect), or close the session entirely.
            if let Some((conn, rpath)) = self.remote_inspect.clone() {
                if ui
                    .button("Pick Another Image")
                    .on_hover_text("Browse the daemon for another image (no reconnect)")
                    .clicked()
                {
                    self.remote_browser.browse_on(conn.clone());
                }
                // Pull a full backup of this remote image to a local folder.
                // Read-only over the block tier (no CHD / shrink-to-minimum).
                let backup_running = self.remote_backup_status.is_some();
                if ui
                    .add_enabled(
                        !self.partitions.is_empty() && !backup_running,
                        egui::Button::new("Back Up Image..."),
                    )
                    .on_hover_text("Pull a full backup of this remote image to a local folder")
                    .clicked()
                {
                    self.start_remote_backup(ctx, conn.clone(), rpath.clone());
                }
                if backup_running {
                    ui.add(egui::Spinner::new());
                    ui.label("Backing up...");
                }
                if ui
                    .add_enabled(!backup_running, egui::Button::new("Close Remote"))
                    .on_hover_text("Disconnect from the remote daemon")
                    .clicked()
                {
                    self.remote_inspect = None;
                    self.clear_results();
                }
            }
        });

        ui.add_space(4.0);

        // While the remote picker is active it takes over the main area; once
        // the user picks an image it hands it off here and collapses.
        let remote_active = self.remote_browser.show(ui);
        for line in self.remote_browser.take_log() {
            ctx.log.info(format!("Remote: {line}"));
        }
        // Picked a remote image: switch the inspected source to it (block tier)
        // and run the full inspect over the wire — no download.
        if let Some((conn, rpath)) = self.remote_browser.take_inspect_request() {
            self.browse_view.close();
            self.image_file_path = None;
            self.amiga_tempdir = None;
            self.selected_device_idx = None;
            self.backup_folder_path = None;
            self.remote_inspect = Some((conn, rpath));
            self.run_inspect(ctx);
        }
        if remote_active {
            return;
        }

        // Auto-inspect on selection change
        let selection_changed = self.selected_device_idx != self.prev_device_idx
            || self.image_file_path != self.prev_image_path
            || self.backup_folder_path != self.prev_backup_path;

        if selection_changed {
            // The source only ever changes through `apply_source_event` /
            // `do_close` (both gated upstream on unsaved edits), so by here the
            // switch is committed — just record it and load.
            self.prev_device_idx = self.selected_device_idx;
            self.prev_image_path = self.image_file_path.clone();
            self.prev_backup_path = self.backup_folder_path.clone();
            // Re-sniff floppy-ness on every source change so the
            // "Convert Floppy Container..." button tracks the source.
            self.source_is_floppy = self
                .image_file_path
                .as_deref()
                .map(Self::detect_source_is_floppy)
                .unwrap_or(false);
            if self.backup_folder_path.is_some() {
                self.load_backup_metadata(ctx);
            } else if self.selected_device_idx.is_some() || self.image_file_path.is_some() {
                self.run_inspect(ctx);
            }
        }

        // Deferred source-switch confirm (shown when a switch/close was
        // requested while the browse view had unsaved staged edits).
        self.render_source_switch_guard(ui, ctx);

        // Poll background inspect (physical device)
        self.poll_inspect_status(ctx);

        // Poll export status
        self.poll_export_status(ctx);

        // Poll a running "Back Up Image…" pull of a remote image
        self.poll_remote_backup_status(ctx);

        // Poll any pending per-partition minimum-size calculations
        self.poll_min_size_calcs(ctx);
        self.poll_volume_label_probes(ctx);
        self.poll_chd_expand_status(ctx);
        self.poll_repack_status(ctx);

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
                || self.backup_folder_path.is_some()
                || self.remote_inspect.is_some();
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

            // Export button — available when we have partition data and no export
            // running. Not for a remote image yet (export is path-based).
            let has_partitions = !self.partitions.is_empty();
            let export_running = self.export_status.is_some();
            if ui
                .add_enabled(
                    has_partitions && !export_running && self.remote_inspect.is_none(),
                    egui::Button::new("Export Disk Image..."),
                )
                .clicked()
            {
                self.init_export_configs();
                self.export_popup = true;
            }

            // Floppy container converter (XDF / HDM / DIM / D88). Shown
            // only when the loaded image is itself a floppy container, so
            // the long button doesn't crowd the bar for the common HDD
            // case. The current source path is pre-loaded into the dialog.
            if self.source_is_floppy {
                if let Some(src) = self.image_file_path.clone() {
                    if ui
                        .button("Convert Floppy Container...")
                        .on_hover_text(
                            "Convert between XDF, HDM, DIM, and D88 floppy container \
                             formats (X68000 / PC-98 / FM-7).",
                        )
                        .clicked()
                    {
                        self.floppy_convert_dialog = Some(FloppyConvertDialog::with_source(src));
                    }
                }
            }

            // Edit Partition Table button — only for devices and image files
            // (not backups, not remote images — the editor writes back locally).
            let is_editable_source = self.partition_table.is_some()
                && self.backup_folder_path.is_none()
                && self.clonezilla_image.is_none()
                && self.remote_inspect.is_none()
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

            // Make Bootable button — auto-detect what this Mac disk needs to
            // boot (SCSI driver + DDR on a full APM disk, boot blocks, blessed
            // System Folder) and apply only the missing pieces. A flat HFV is
            // kept flat. See src/fs/make_bootable.rs.
            let has_mac_hfs = self.partitions.iter().any(|p| {
                is_classic_hfs(
                    p.partition_type_byte,
                    p.partition_type_string.as_deref(),
                    &p.type_name,
                )
            });
            if ui
                .add_enabled(
                    is_image_file && has_mac_hfs && !export_running && !chd_expand_running,
                    egui::Button::new("Make Bootable..."),
                )
                .on_hover_text(
                    "Auto-detect what this Mac disk needs to boot (SCSI driver + DDR on a \
                     full APM disk, boot blocks, blessed System Folder) and apply only the \
                     missing pieces. A flat HFV is kept flat; you'll be asked for a donor \
                     disk only if boot blocks are missing.",
                )
                .clicked()
            {
                self.run_make_bootable(ctx);
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

            // One consolidated Close button: clears whatever source is open
            // (device / image / backup). Guarded by unsaved browse edits.
            let any_source = self.selected_device_idx.is_some()
                || self.image_file_path.is_some()
                || self.backup_folder_path.is_some();
            if any_source && !inspect_running && !export_running && ui.button("Close").clicked() {
                if self.browse_view.has_unsaved_edits() {
                    self.pending_source_change = Some(PendingSourceChange::Close);
                } else {
                    self.do_close(ctx);
                }
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
        let mut cancel = false;
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
                        cancel = true;
                    }
                });
            });

        // Close on the titlebar X (`!open`) or the Cancel button (`cancel`).
        // Cancel previously only reset `do_expand`, which was already false, so
        // the dialog never closed — same bug class as issue #43.
        if !open || cancel {
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

    /// Auto-detect what the loaded Mac disk needs to boot and apply only the
    /// missing pieces (driver + DDR on a full APM disk, boot blocks, blessed
    /// System Folder). Prompts for a donor disk only when boot blocks are
    /// absent. Runs synchronously — each step is a small metadata/sector write.
    fn run_make_bootable(&mut self, ctx: &mut TabContext) {
        use rusty_backup::fs::make_bootable::{
            assess_bootability, make_bootable, BootDiskKind, DriverSource, MakeBootableOptions,
        };

        let Some(path) = self.image_file_path.clone() else {
            return;
        };

        let assessment = match assess_bootability(&path) {
            Ok(a) => a,
            Err(e) => {
                ctx.log.error(format!("Make Bootable: {e}"));
                return;
            }
        };
        let kind = match assessment.kind {
            BootDiskKind::FlatHfs => "flat HFS image (kept flat)",
            BootDiskKind::ApmHfs => "full APM disk",
        };
        ctx.log.info(format!(
            "Make Bootable: detected {kind} - boot blocks {}, System Folder {}",
            if assessment.has_boot_blocks {
                "present"
            } else {
                "absent"
            },
            match &assessment.blessed_folder {
                Some(n) => format!("blessed ({n})"),
                None => "not blessed".to_string(),
            }
        ));

        if assessment.is_bootable() {
            ctx.log
                .info("Make Bootable: disk is already bootable; nothing to do.");
            return;
        }

        // Boot blocks can't be synthesized — ask for a donor disk if missing.
        // Cancelling the picker aborts without writing anything.
        let mut donor = None;
        if !assessment.has_boot_blocks {
            match rfd::FileDialog::new()
                .set_title("Boot blocks needed - pick a bootable donor disk")
                .add_filter("Disk images", &["dsk", "hfv", "img", "hda", "raw"])
                .pick_file()
            {
                Some(p) => donor = Some(p),
                None => {
                    ctx.log
                        .info("Make Bootable: cancelled (boot blocks need a donor disk).");
                    return;
                }
            }
        }

        let opts = MakeBootableOptions {
            donor,
            driver: DriverSource::Builtin,
            bless_path: None,
            dry_run: false,
        };
        match make_bootable(&path, &opts) {
            Ok(report) => {
                for s in &report.applied {
                    ctx.log.info(format!("Make Bootable: {s}"));
                }
                for s in &report.skipped {
                    ctx.log.info(format!("Make Bootable: skip - {s}"));
                }
                for s in &report.still_missing {
                    ctx.log.warn(format!("Make Bootable: MISSING - {s}"));
                }
                if report.now_bootable {
                    ctx.log.info("Make Bootable: disk is now bootable.");
                } else {
                    ctx.log
                        .warn("Make Bootable: disk is not yet bootable (see MISSING above).");
                }
                // Force re-detection so the partition list / statuses refresh.
                self.prev_image_path = None;
            }
            Err(e) => ctx.log.error(format!("Make Bootable failed: {e}")),
        }
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
            .map(|p| p.byte_offset() + p.size_bytes)
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
        let is_vmdk_flat = self.export_format == ExportFormat::VmdkFlat;
        let is_vmdk_sparse = self.export_format == ExportFormat::VmdkSparse;
        // Force whole-disk for formats that don't make sense per-partition:
        // CHD (headless slice no emulator consumes), dynamic VHD, QCOW2, and
        // both VMDK variants (each wraps a whole-disk geometry the emulator
        // expects to find an MBR/GPT/APM at sector 0 of).
        if is_chd_format || is_dynamic_vhd || is_qcow2 || is_vmdk_flat || is_vmdk_sparse {
            self.export_whole_disk = true;
        }
        let whole_disk_only =
            is_chd_format || is_dynamic_vhd || is_qcow2 || is_vmdk_flat || is_vmdk_sparse;
        // HFV is the inverse: a flat HFV is a single classic-HFS volume, so it
        // is per-partition only (one .hfv per HFS partition). Force it off
        // whole-disk and disable the whole-disk radio.
        let is_hfv = self.export_format == ExportFormat::Hfv;
        if is_hfv {
            self.export_whole_disk = false;
        }

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
                let whole_disk_resp = ui.add_enabled(
                    !is_hfv,
                    egui::RadioButton::new(self.export_whole_disk, "Whole Disk (single file)"),
                );
                if whole_disk_resp.clicked() && !is_hfv {
                    self.export_whole_disk = true;
                }
                if is_hfv {
                    whole_disk_resp.on_hover_text(
                        "HFV is a single classic-HFS volume with no partition table, so it is \
                         exported per-partition (one .hfv per HFS partition).",
                    );
                }
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
                } else if is_vmdk_flat {
                    per_part_resp.on_hover_text(
                        "Per-partition export is not supported for VMDK flat — \
                         the descriptor wraps a whole-disk geometry that \
                         VMware/qemu-img/VirtualBox expect to find a partition \
                         table at sector 0 of.",
                    );
                } else if is_vmdk_sparse {
                    per_part_resp.on_hover_text(
                        "Per-partition export is not supported for VMDK sparse — \
                         the grain directory wraps a whole-disk geometry that \
                         VMware/qemu-img/VirtualBox expect to find a partition \
                         table at sector 0 of.",
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
                    ui.radio_value(&mut self.export_format, ExportFormat::VmdkFlat, "VMDK (Flat)")
                        .on_hover_text(
                            "monolithicFlat VMDK — emits <name>.vmdk descriptor + \
                             <name>-flat.vmdk raw extent. Opens in VMware Workstation/\
                             Fusion, VirtualBox, qemu-img.",
                        );
                    ui.radio_value(
                        &mut self.export_format,
                        ExportFormat::VmdkSparse,
                        "VMDK (Sparse)",
                    )
                    .on_hover_text(
                        "monolithicSparse VMDK — single self-contained .vmdk; \
                         zero grains omitted. Opens in VMware, VirtualBox, qemu-img.",
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
                    ui.radio_value(&mut self.export_format, ExportFormat::Hfv, "HFV (BasiliskII)")
                        .on_hover_text(
                            "Flat classic-HFS volume for BasiliskII / SheepShaver (no partition \
                             table, classic-HFS-only, max 2047 MB). Exported per-partition: one \
                             .hfv per HFS partition, cloned and re-floored to the chosen size.",
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

        // HFV is not a streaming codec: clone each classic-HFS partition into a
        // flat .hfv. Route here before the streaming whole-disk/per-partition
        // paths. Needs a raw image / device source (not a backup folder or
        // Clonezilla image — those go through restore-as-HFV instead).
        if format == ExportFormat::Hfv {
            let source_image = self.image_file_path.clone().or_else(|| {
                self.selected_device_idx
                    .and_then(|idx| ctx.devices.get(idx))
                    .map(|d| d.path.clone())
            });
            let Some(source_image) = source_image else {
                ctx.log
                    .error("HFV export needs a raw image or device source.");
                return;
            };
            let dest_folder = match super::file_dialog().pick_folder() {
                Some(p) => p,
                None => return,
            };
            ctx.log.info(format!(
                "Exporting classic-HFS partition(s) as flat HFV file(s) to {}...",
                dest_folder.display()
            ));
            self.export_status = Some(export_runner::start_per_partition_hfv(
                source_image,
                self.partitions.clone(),
                size_map,
                dest_folder,
                total_bytes,
            ));
            self.export_rate.reset();
            return;
        }

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

    /// Kick off a background "Back Up Image…" pull of the currently-inspected
    /// remote image to a user-picked local folder. Read-only over the block
    /// tier: Zstd per-partition output, no CHD / shrink-to-minimum (those need
    /// `File`-specific access the daemon doesn't expose). GUI-only entry point;
    /// the engine work is [`rusty_backup::backup::run_backup_from`].
    fn start_remote_backup(
        &mut self,
        ctx: &mut TabContext,
        conn: Arc<Mutex<rusty_backup::remote::RemoteConnection>>,
        rpath: String,
    ) {
        let Some(dest) = super::file_dialog().pick_folder() else {
            return;
        };
        let stem = std::path::Path::new(&rpath)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("remote-image");
        let backup_name = format!("{stem}-backup");
        let display = {
            let addr = conn
                .lock()
                .ok()
                .map(|c| c.addr().to_string())
                .unwrap_or_default();
            let path_part = if rpath.starts_with('/') {
                rpath.clone()
            } else {
                format!("/{rpath}")
            };
            format!("rb://{addr}{path_part}")
        };

        let config = rusty_backup::backup::BackupConfig {
            source_path: std::path::PathBuf::from(&rpath),
            destination_dir: dest.clone(),
            backup_name: backup_name.clone(),
            // Zstd per-partition is the only remote-supported output (CHD needs
            // whole-disk staging the block tier doesn't expose).
            compression: rusty_backup::backup::CompressionType::Zstd,
            checksum: rusty_backup::backup::ChecksumType::Sha256,
            split_size_mib: None,
            sector_by_sector: false,
            partition_filter: None,
            chd_options: None,
            size_policy: None,
            partition_target_sizes: None,
            shrink_to_minimum: false,
            precomputed_minimum_sizes: None,
            defrag_partition_indices: None,
            defrag_fat: false,
            // Remote/block-tier backup: swap exclusion happens agent-side
            // (cb-dos CRUSTYBK already zeros swap), so this is moot here.
            keep_swap: false,
        };

        let progress = Arc::new(Mutex::new(rusty_backup::backup::BackupProgress::new()));
        self.remote_backup_status = Some(Arc::clone(&progress));
        ctx.log.info(format!(
            "Backing up remote image {display} -> {}",
            dest.join(&backup_name).display()
        ));

        let source = rusty_backup::backup::BackupSource::Remote {
            conn,
            path: rpath,
            display,
            is_device: false,
        };
        std::thread::spawn(move || {
            let _wake = rusty_backup::os::wakelock::acquire("Rusty Backup: remote backup");
            let progress_for_panic = Arc::clone(&progress);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                rusty_backup::backup::run_backup_from(source, config, Arc::clone(&progress))
            }));
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    if let Ok(mut p) = progress_for_panic.lock() {
                        p.error = Some(format!("{e:#}"));
                        p.finished = true;
                    }
                }
                Err(_) => {
                    if let Ok(mut p) = progress_for_panic.lock() {
                        p.error = Some("remote backup worker panicked".to_string());
                        p.finished = true;
                    }
                }
            }
        });
    }

    /// Drain log messages from the remote-backup worker into the global log
    /// panel and react to completion (success/error).
    fn poll_remote_backup_status(&mut self, ctx: &mut TabContext) {
        let arc = match &self.remote_backup_status {
            Some(s) => Arc::clone(s),
            None => return,
        };
        let Ok(mut status) = arc.lock() else { return };
        for msg in status.log_messages.drain(..) {
            match msg.level {
                rusty_backup::backup::LogLevel::Error => ctx.log.error(msg.message),
                rusty_backup::backup::LogLevel::Warning => ctx.log.warn(msg.message),
                rusty_backup::backup::LogLevel::Info => ctx.log.info(msg.message),
            }
        }
        if status.finished {
            if let Some(err) = &status.error {
                ctx.log.error(format!("Remote backup failed: {err}"));
            } else {
                ctx.log.info("Remote backup completed successfully.");
            }
            drop(status);
            self.remote_backup_status = None;
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

    /// Apply a picked source: close any open browse view, set the source
    /// fields, and clear stale results. The subsequent `selection_changed`
    /// pass loads the new source. Unsaved-edit gating happens at the call site.
    fn apply_source_event(&mut self, ev: super::source_picker::SourceEvent) {
        use super::source_picker::SourceEvent;
        self.browse_view.close();
        match ev {
            SourceEvent::Device(i) => {
                self.selected_device_idx = Some(i);
                self.image_file_path = None;
                self.amiga_tempdir = None;
                self.backup_folder_path = None;
                self.clear_results();
            }
            SourceEvent::Image { path, tempdir } => {
                // A Mac archive (.sit/.hqx/...) isn't a disk image — hand it to
                // the Archives tab instead of failing to parse a partition table.
                if is_mac_archive_path(&path) {
                    self.pending_open_archive = Some(path);
                    return;
                }
                self.selected_device_idx = None;
                self.backup_folder_path = None;
                self.image_file_path = Some(path);
                self.amiga_tempdir = tempdir;
                self.clear_results();
            }
            SourceEvent::BackupFolder(path) => {
                self.backup_folder_path = Some(path);
                self.selected_device_idx = None;
                self.image_file_path = None;
                self.clear_results();
            }
            // Inspect's picker doesn't offer a host folder.
            SourceEvent::HostFolder(_) => {}
            // "Connect to Remote..." is intercepted at the call site (it pops a
            // window rather than changing the source); never reaches here.
            SourceEvent::Remote => {}
        }
    }

    /// Close whatever source is open (the consolidated Close button). Unsaved
    /// edits are gated at the call site.
    fn do_close(&mut self, ctx: &mut TabContext) {
        let was_backup = self.backup_folder_path.is_some();
        self.browse_view.close();
        self.clear_results();
        self.selected_device_idx = None;
        self.prev_device_idx = None;
        self.image_file_path = None;
        self.amiga_tempdir = None;
        self.prev_image_path = None;
        self.backup_folder_path = None;
        self.prev_backup_path = None;
        if was_backup {
            // Otherwise gui::mod's auto-reopen fallback re-loads the backup.
            self.close_backup_requested = true;
        }
        ctx.log.info("Source closed.");
    }

    /// Modal shown when a source open/close was requested while the browse view
    /// had unsaved staged edits: discard and proceed, or cancel and keep the
    /// current source fully intact (it was never touched).
    fn render_source_switch_guard(&mut self, ui: &mut egui::Ui, ctx: &mut TabContext) {
        if self.pending_source_change.is_none() {
            return;
        }
        let mut confirm = false;
        let mut cancel = false;
        egui::Window::new("Discard unsaved edits?")
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .show(ui.ctx(), |ui| {
                ui.label("The open volume has unapplied staged edits.");
                ui.label("Switching or closing the source will discard them.");
                ui.add_space(6.0);
                ui.horizontal(|ui| {
                    if ui.button("Discard & continue").clicked() {
                        confirm = true;
                    }
                    if ui.button("Cancel").clicked() {
                        cancel = true;
                    }
                });
            });
        if confirm {
            self.browse_view.close();
            match self.pending_source_change.take() {
                Some(PendingSourceChange::Open(ev)) => self.apply_source_event(ev),
                Some(PendingSourceChange::Close) => self.do_close(ctx),
                None => {}
            }
        } else if cancel {
            self.pending_source_change = None;
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
        self.cache_store.clear();
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
        // redirect the user to "Open File" on the CHD itself —
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

        // A remote image (block tier) takes precedence: there's no local path,
        // so `path` is synthetic (display/logging only) and the worker reads via
        // a `RemoteBlockReader` instead of opening a local file/device.
        let remote = self.remote_inspect.clone();

        let path = if let Some((_, rpath)) = &remote {
            PathBuf::from(rpath)
        } else if let Some(img_path) = &self.image_file_path {
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

        if remote.is_some() {
            ctx.log
                .info(format!("Inspecting remote image {}...", path.display()));
        } else {
            ctx.log.info(format!("Inspecting {}...", path.display()));
        }

        // Cache CHD path: subsequent per-partition probes (HFS variant probe,
        // hfs_block_size, partition_minimum_size) need a fresh ChdReader rather
        // than reading partition_offset off the raw .chd file. A remote image is
        // never a local CHD.
        self.chd_image_path =
            if remote.is_none() && rusty_backup::model::source_reader::is_chd_path(&path) {
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

            // A remote image is a raw disk over the wire — never a local device
            // or container, so these path-specific flags are all false for it.
            let is_device = remote.is_none()
                && (path.to_string_lossy().starts_with("/dev/")
                    || path.to_string_lossy().starts_with("\\\\.\\"));

            // File containers decode to a flat sector stream via
            // source_reader::open_read — no tempfile, no elevation needed.
            // This covers GHO/IMZ plus the floppy wrappers (MSA / EDSK / D88
            // / DIM / XDF / HDM / Apple-II / Arculator HDF). CHD is also a
            // container but keeps its own specialized detection below (CD-CHD
            // cooked sectors, format metadata + sidecar), so exclude it here.
            let uses_streaming_reader = remote.is_none()
                && !is_device
                && rusty_backup::model::source_reader::is_container_path(&path)
                && !rusty_backup::model::source_reader::is_chd_path(&path);

            // Open the device/file with full elevation: unmounts volumes and
            // claims exclusive DA access for the duration of the inspect.
            // Streaming-reader containers and remote images skip this (they open
            // via open_read / a RemoteBlockReader).
            // `guard` is held purely for RAII: it keeps the volume locks / DA
            // claim alive for the duration of the inspect. On macOS the fd +
            // claim are handed off to the main thread below; on every other
            // platform `guard` is unused, so silence the lint there.
            #[cfg_attr(not(target_os = "macos"), allow(unused_variables))]
            let (device_file, guard) = if remote.is_some() || uses_streaming_reader {
                (None, None)
            } else {
                let elevated = match rusty_backup::os::open_source_for_reading(&path) {
                    Ok(e) => e,
                    Err(e) => {
                        finish_err(format!("Cannot open {}: {e}", path.display()));
                        return;
                    }
                };
                let (f, g) = elevated.into_parts();
                (Some(f), Some(g))
            };

            set_step("Reading partition table...");

            // Build the initial reader: a RemoteBlockReader for a remote image, a
            // streaming reader for GHO/IMZ, a cloned File for everything else.
            let mut reader: Box<dyn rusty_backup::rbformats::ReadSeek> =
                if let Some((conn, rpath)) = &remote {
                    match rusty_backup::remote::RemoteBlockReader::open(conn.clone(), rpath) {
                        Ok(r) => Box::new(r),
                        Err(e) => {
                            finish_err(format!("Cannot open remote image {rpath}: {e:#}"));
                            return;
                        }
                    }
                } else if uses_streaming_reader {
                    match rusty_backup::model::source_reader::open_read(&path) {
                        Ok(r) => r,
                        Err(e) => {
                            finish_err(format!("Cannot open {}: {e:#}", path.display()));
                            return;
                        }
                    }
                } else {
                    match device_file.as_ref().unwrap().try_clone() {
                        Ok(f) => Box::new(BufReader::new(f)),
                        Err(e) => {
                            finish_err(format!("Cannot clone file handle: {e}"));
                            return;
                        }
                    }
                };

            let detect_result =
                if remote.is_some() {
                    // A remote image is served as a raw disk; parse the table
                    // directly over the wire (no local format detection).
                    PartitionTable::detect(&mut reader)
                } else if uses_streaming_reader {
                    // Streaming containers (GHO, IMZ) already provide a decoded
                    // raw disk image; skip VHD/2MG/DMG format detection.
                    let ext = path
                        .extension()
                        .and_then(|e| e.to_str())
                        .map(|s| s.to_ascii_lowercase());
                    let label = match ext.as_deref() {
                        Some("gho") | Some("ghs") => "Norton Ghost (GHO)",
                        Some("imz") => "WinImage (IMZ)",
                        Some("msa") => "Atari ST MSA floppy",
                        Some("dsk") => "CPCEMU DSK/EDSK floppy",
                        Some("d88") => "Sharp .d88 floppy",
                        Some("dim") => "DiskExplorer DIM floppy",
                        Some("xdf") => "X68000 XDF floppy",
                        Some("hdm") => "PC-98 HDM floppy",
                        Some("hdf") => "Acorn ADFS image",
                        Some("do") | Some("po") => "Apple II floppy",
                        _ => "Streaming image",
                    };
                    push_log(format!("Detected format: {}", label));

                    // Surface GHO metadata (description, compression, etc.)
                    if matches!(ext.as_deref(), Some("gho") | Some("ghs")) {
                        if let Ok(info) = rusty_backup::rbformats::gho::format_gho_info(&path) {
                            for line in info.lines() {
                                if !line.is_empty() {
                                    push_log(line.to_string());
                                }
                            }
                        }
                        set_step("Reconstructing partitions from Ghost image...");
                    }

                    if let Ok(mut s) = status.lock() {
                        s.format_label = Some(label.to_string());
                    }
                    PartitionTable::detect(&mut reader)
                } else if !is_device {
                    // For image files, use unified format detection
                    match device_file.as_ref().unwrap().try_clone() {
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
                                    device_file.as_ref().unwrap().try_clone().unwrap_or_else(
                                        |_| std::fs::File::open(&path).expect("reopen failed"),
                                    ),
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
                            if let Some(ref df) = device_file {
                                if let Ok(f) = df.try_clone() {
                                    if let Ok(real_size) =
                                        rusty_backup::os::get_file_size(&f, &path)
                                    {
                                        *size_bytes = real_size;
                                    }
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
                    let is_chd =
                        remote.is_none() && rusty_backup::model::source_reader::is_chd_path(&path);
                    let make_probe_reader =
                        || -> Option<Box<dyn rusty_backup::rbformats::ReadSeek>> {
                            if let Some((conn, rpath)) = &remote {
                                // Each probe gets its own block reader on the same
                                // connection (a fresh daemon-side open handle).
                                rusty_backup::remote::RemoteBlockReader::open(conn.clone(), rpath)
                                    .ok()
                                    .map(|r| {
                                        Box::new(r) as Box<dyn rusty_backup::rbformats::ReadSeek>
                                    })
                            } else if uses_streaming_reader || is_chd {
                                rusty_backup::model::source_reader::open_read(&path).ok()
                            } else {
                                device_file.as_ref()?.try_clone().ok().map(|f| {
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

                            let part_offset = part.byte_offset();
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
                            match rusty_backup::fs::probe_0x83_fs_type(&mut br, part.byte_offset())
                            {
                                Some("FAT") if is_linux_mbr => {
                                    let part_offset = part.byte_offset();
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
                                part.byte_offset(),
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
                                rusty_backup::fs::fat::FatFilesystem::open(br, part.byte_offset())
                                    .ok()
                                    .and_then(|fs| {
                                        use rusty_backup::fs::Filesystem;
                                        fs.volume_label().map(str::to_string)
                                    })
                            })
                        } else if is_amiga_dos {
                            make_probe_reader().and_then(|br| {
                                rusty_backup::fs::affs::AffsFilesystem::open(br, part.byte_offset())
                                    .ok()
                                    .and_then(|fs| {
                                        use rusty_backup::fs::Filesystem;
                                        fs.volume_label().map(str::to_string)
                                    })
                            })
                        } else if is_amiga_pfs3 {
                            make_probe_reader().and_then(|br| {
                                rusty_backup::fs::pfs3::Pfs3Filesystem::open(br, part.byte_offset())
                                    .ok()
                                    .and_then(|fs| {
                                        use rusty_backup::fs::Filesystem;
                                        fs.volume_label().map(str::to_string)
                                    })
                            })
                        } else if is_amiga_sfs {
                            make_probe_reader().and_then(|br| {
                                rusty_backup::fs::sfs::SfsFilesystem::open(br, part.byte_offset())
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
                        let result = if let Some((conn, rpath)) = &remote {
                            // Remote image: compute the minimum over a block
                            // reader on the same connection (cheap FS read a few
                            // ranges; expensive FS come back Deferred).
                            match rusty_backup::remote::RemoteBlockReader::open(conn.clone(), rpath)
                            {
                                Ok(r) => rusty_backup::fs::partition_minimum_size(
                                    r,
                                    part.byte_offset(),
                                    part.partition_type_byte,
                                    part.partition_type_string.as_deref(),
                                    part.size_bytes,
                                    false,
                                    None,
                                    &|_| {},
                                ),
                                Err(_) => continue,
                            }
                        } else if uses_streaming_reader || is_chd {
                            // GHO containers with compressed NTFS
                            // require decompressing the entire file to
                            // compute minimum sizes — defer to avoid
                            // blocking the inspect tab for minutes.
                            let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
                            if ext.eq_ignore_ascii_case("gho") || ext.eq_ignore_ascii_case("ghs") {
                                rusty_backup::fs::MinimumResult::Deferred {
                                    fs_name: rusty_backup::fs::fs_name_for(
                                        part.partition_type_byte,
                                        part.partition_type_string.as_deref(),
                                    ),
                                }
                            } else {
                                let Ok(r) = rusty_backup::model::source_reader::open_read(&path)
                                else {
                                    continue;
                                };
                                rusty_backup::fs::partition_minimum_size(
                                    r,
                                    part.byte_offset(),
                                    part.partition_type_byte,
                                    part.partition_type_string.as_deref(),
                                    part.size_bytes,
                                    false,
                                    None,
                                    &|_| {},
                                )
                            }
                        } else {
                            let Ok(f) = device_file.as_ref().unwrap().try_clone() else {
                                continue;
                            };
                            if is_device {
                                rusty_backup::fs::partition_minimum_size(
                                    rusty_backup::os::SectorAlignedReader::new(f),
                                    part.byte_offset(),
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
                                    part.byte_offset(),
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
                            if let Some(df) = device_file {
                                s.device_file = Some(df);
                            }
                            if let Some(g) = guard {
                                s.device_guard = Some(g);
                            }
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
        // Defragment (Human68k repack) request: (offset, partition label).
        let mut defrag_request: Option<(u64, String)> = None;
        // "Calc min" button click: partition index to compute minimum size for.
        let mut min_size_calc_request: Option<usize> = None;
        // "Resize…" click on an Alto BFS row.
        let mut resize_request = false;

        // The in-place Human68k defragment worker rewrites the partition
        // region of the underlying file, so it's offered for image-file
        // sources only (not devices or loaded backups).
        let is_image_source = self.image_file_path.is_some()
            && self.selected_device_idx.is_none()
            && self.backup_folder_path.is_none();
        // A remote image is read-only over the wire: the HFS Expand/Export action
        // re-floors or rewrites the volume (path-based), so it's disabled. Browse,
        // Calc min, and Check (fsck) all work over the block reader.
        let is_remote = self.remote_inspect.is_some();

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
                            } else if let Some(&computed) =
                                self.partition_min_sizes.get(&part.index)
                            {
                                // Computed, but equals the full partition size
                                // (in_place_min filters those out). Show it so
                                // the user sees the result rather than a blank;
                                // it means the volume can't shrink in place.
                                ui.label(partition::format_size(computed));
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
                        if partition_is_browsable(
                            part.partition_type_byte,
                            part.partition_type_string.as_deref(),
                            &part.type_name,
                        ) {
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
                                    part.byte_offset(),
                                    ptype,
                                    part.partition_type_string.clone(),
                                ));
                            }
                            if (is_checkable_type(ptype, part.partition_type_string.as_deref())
                                || is_superfloppy_hfs(part.partition_type_byte, &part.type_name))
                                && ui.small_button("Check").clicked()
                            {
                                check_request = Some((
                                    part.byte_offset(),
                                    ptype,
                                    part.partition_type_string.clone(),
                                ));
                            }
                            if is_classic_hfs(
                                part.partition_type_byte,
                                part.partition_type_string.as_deref(),
                                &part.type_name,
                            ) && !is_remote
                                && ui
                                    .small_button("Expand/Export…")
                                    .on_hover_text(
                                        "Re-floor this classic-HFS volume to a new size / block \
                                         size (APM .hda) or export it as a flat BasiliskII HFV.",
                                    )
                                    .clicked()
                            {
                                expand_request = Some((part.byte_offset(), part.size_bytes));
                            }
                            // Defragment (repack) a Human68k volume in place.
                            // Image files only — the worker rewrites the
                            // partition region of the file on disk.
                            if part.partition_type_string.as_deref() == Some("human68k")
                                && is_image_source
                                && self.repack_status.is_none()
                                && ui
                                    .small_button("Defragment…")
                                    .on_hover_text(
                                        "Repack this Human68k volume so its files are stored \
                                         contiguously, reclaiming holes left by deleted files. \
                                         Rewrites the partition in place — back up first.",
                                    )
                                    .clicked()
                            {
                                defrag_request = Some((part.byte_offset(), part.type_name.clone()));
                            }
                            // Resize an Alto BFS volume onto a new geometry.
                            // Reads the image and writes a brand-new PDI, so it's
                            // offered for image-file sources only.
                            if part.type_name == "Alto BFS"
                                && is_image_source
                                && ui
                                    .small_button("Resize…")
                                    .on_hover_text(
                                        "Re-lay this Alto volume onto a different disk geometry \
                                         (Diablo 31 / 44, grow or shrink) and save it as a PDI.",
                                    )
                                    .clicked()
                            {
                                resize_request = true;
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

        // Handle Alto resize request: open the dialog on the loaded image.
        if resize_request {
            match self.image_file_path.clone() {
                Some(path) => match AltoResizeDialog::new(path) {
                    Ok(dlg) => self.alto_resize_dialog = Some(dlg),
                    Err(e) => ctx.log.error(format!("Cannot open resize dialog: {e}")),
                },
                None => ctx.log.error("Resize: no image file loaded"),
            }
        }

        // Handle defragment request: stash it for the confirmation modal
        // (the worker rewrites the partition in place).
        if let Some((offset, label)) = defrag_request {
            self.pending_repack = Some((offset, label));
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

        // Resize-Alto dialog
        if let Some(dlg) = &mut self.alto_resize_dialog {
            let still_open = dlg.show(ui.ctx(), ctx.log);
            if !still_open {
                self.alto_resize_dialog = None;
            }
        }

        // Floppy-container convert dialog
        if let Some(dlg) = &mut self.floppy_convert_dialog {
            let still_open = dlg.show(ui.ctx(), ctx.log);
            if !still_open {
                self.floppy_convert_dialog = None;
            }
        }

        // Human68k defragment: confirmation modal + in-progress window.
        self.render_repack_ui(ui, ctx);
    }

    /// Render the Human68k defragment confirmation modal (when a partition's
    /// "Defragment…" button was clicked) and the in-progress window (while
    /// the worker runs). Split out of `show` to keep that method readable.
    fn render_repack_ui(&mut self, ui: &mut egui::Ui, ctx: &mut TabContext) {
        if let Some((offset, label)) = self.pending_repack.clone() {
            let mut keep_open = true;
            let mut start = false;
            let mut cancel = false;
            egui::Window::new("Defragment Human68k partition")
                .collapsible(false)
                .resizable(false)
                .open(&mut keep_open)
                .show(ui.ctx(), |ui| {
                    ui.label(format!("Defragment \"{label}\"?"));
                    ui.label(
                        "Repacks the volume so files are stored contiguously, reclaiming \
                         holes left by deleted files.",
                    );
                    ui.colored_label(
                        egui::Color32::from_rgb(220, 180, 80),
                        "The partition is rewritten in place. Back up the image first.",
                    );
                    ui.add_space(6.0);
                    ui.horizontal(|ui| {
                        if ui.button("Defragment").clicked() {
                            start = true;
                        }
                        if ui.button("Cancel").clicked() {
                            cancel = true;
                        }
                    });
                });
            if start {
                self.pending_repack = None;
                self.start_repack(offset, ctx);
            } else if cancel || !keep_open {
                self.pending_repack = None;
            }
        }

        if let Some(arc) = &self.repack_status {
            let frac = arc
                .lock()
                .ok()
                .map(|s| {
                    if s.total_bytes > 0 {
                        s.current_bytes as f32 / s.total_bytes as f32
                    } else {
                        0.0
                    }
                })
                .unwrap_or(0.0);
            egui::Window::new("Defragmenting…")
                .collapsible(false)
                .resizable(false)
                .show(ui.ctx(), |ui| {
                    ui.add(egui::ProgressBar::new(frac).show_percentage());
                    ui.label("Repacking Human68k volume — please wait.");
                });
            // Keep the UI repainting so progress + completion are reflected
            // without needing mouse movement.
            ui.ctx()
                .request_repaint_after(std::time::Duration::from_millis(120));
        }
    }

    /// Spawn the background defragment worker for the Human68k partition at
    /// `offset` in the currently-loaded image file. Releases any cached
    /// browse handle first so the worker can rewrite the file.
    fn start_repack(&mut self, offset: u64, ctx: &mut TabContext) {
        let path = match &self.image_file_path {
            Some(p) => p.clone(),
            None => {
                ctx.log.error("No image file loaded to defragment.");
                return;
            }
        };
        self.browse_view.close();
        ctx.log.info("Starting Human68k defragment (repack)...");
        self.repack_status = Some(rusty_backup::model::repack_runner::spawn(path, offset));
    }

    /// Drain log lines from the defragment worker and finalize on
    /// completion (re-inspect so the refreshed layout is shown).
    fn poll_repack_status(&mut self, ctx: &mut TabContext) {
        let arc = match &self.repack_status {
            Some(s) => s.clone(),
            None => return,
        };
        let Ok(mut status) = arc.lock() else { return };
        for msg in status.log_messages.drain(..) {
            ctx.log.info(msg);
        }
        if status.finished {
            let ok = status.error.is_none();
            if let Some(err) = &status.error {
                ctx.log.error(format!("Defragment failed: {err}"));
            } else {
                ctx.log.info("Defragment complete.");
            }
            drop(status);
            self.repack_status = None;
            if ok {
                // Layout inside the partition changed; refresh the view.
                self.run_inspect(ctx);
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
        // Remote image: check over the block reader (read-only). This runs
        // synchronously like the local path; a large remote volume will pause
        // the UI for the duration of the check.
        let result = if let Some((conn, rpath)) = &self.remote_inspect {
            match rusty_backup::remote::RemoteBlockReader::open(std::sync::Arc::clone(conn), rpath)
            {
                Ok(reader) => rusty_backup::model::fsck_runner::run_fsck_reader(
                    reader,
                    offset,
                    ptype,
                    type_string.as_deref(),
                ),
                Err(e) => Err(e),
            }
        } else {
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
            rusty_backup::model::fsck_runner::run_fsck(&path, offset, ptype, type_string.as_deref())
        };

        match result {
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

        // Remote image: repair in place over the block reader (read-write).
        // Runs synchronously like the local path; a large remote volume pauses
        // the UI for the duration of the repair.
        let repair_result = if let Some((conn, rpath)) = &self.remote_inspect {
            match rusty_backup::remote::RemoteBlockReader::open_rw(
                std::sync::Arc::clone(conn),
                rpath,
            ) {
                Ok(reader) => rusty_backup::model::fsck_runner::run_repair_reader(
                    reader,
                    offset,
                    ptype,
                    type_string.as_deref(),
                ),
                Err(e) => Err(e),
            }
        } else {
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

            rusty_backup::model::fsck_runner::run_repair(
                &path,
                offset,
                ptype,
                type_string.as_deref(),
            )
        };

        match repair_result {
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

        // Remote image: compute over a block reader on the shared connection.
        let source = if let Some((conn, rpath)) = &self.remote_inspect {
            rusty_backup::model::min_size_runner::MinSizeSource::Remote {
                conn: std::sync::Arc::clone(conn),
                path: rpath.clone(),
                // The Inspect tab inspects a remote image *file*, not a device.
                is_device: false,
            }
        // Prefer a CHD path source over the raw device handle: opening the
        // raw .chd file at partition_offset would read compressed bytes.
        } else if let Some(chd_path) = self.chd_image_path.clone() {
            rusty_backup::model::min_size_runner::MinSizeSource::Chd(chd_path)
        } else if let Some(gho_path) = self
            .image_file_path
            .as_ref()
            .filter(|p| rusty_backup::model::source_reader::is_gho_path(p))
        {
            // GHO/GHS images decompress through GhoReader; reading the raw
            // file at partition_offset would hit compressed bytes.
            rusty_backup::model::min_size_runner::MinSizeSource::Gho(gho_path.clone())
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
            partition_offset: part.byte_offset(),
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
        // Case 0: remote image over the block tier — browse the partition's
        // filesystem over the wire (a fresh RemoteBlockReader per operation on
        // the open daemon-side image). Read-only.
        if let Some((conn, rpath)) = &self.remote_inspect {
            ctx.log.info(format!(
                "Browsing remote partition {part_index} at offset {offset}"
            ));
            let mut session = rusty_backup::model::browse_session::BrowseSession::new();
            session.partition_offset = offset;
            session.partition_type = ptype;
            session.partition_type_string = partition_type_string;
            session.remote = Some((std::sync::Arc::clone(conn), rpath.clone()));
            self.browse_view.open_with_session(session);
            // Editing a remote image is supported over the block tier (a
            // read-write RemoteBlockReader). Enable the Edit Mode toggle; the
            // open_editable probe surfaces a clear error for any FS that can't
            // be edited, exactly as on the local path.
            self.browse_view.mark_edit_supported();
            if let Some(part) = self.partitions.iter().find(|p| p.index == part_index) {
                if let Some(drv) = part.drv_name.as_deref() {
                    if !drv.is_empty() {
                        self.browse_view.set_label_prefix(drv.to_string());
                    }
                }
            }
            return;
        }

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

        // Case 2: native backup folder. Source resolution (data-file lookup,
        // split/exists checks, the per-compression reader) and the edit-context
        // mapping live in the shared model (commander_source) — the same path
        // Commander uses — instead of an inline per-compression ladder. The view
        // keeps only the zstd seekable-cache upgrade and the unsupported message.
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

        // zstd opens streaming immediately, then upgrades to a background-built
        // seekable cache (and reuses one already built this session). That
        // machinery stays in the view; the model supplies only the validated
        // data file and the archive-edit context.
        if meta.compression_type == "zstd" {
            let data_path = match commander_source::single_data_file(&folder, &meta, part_index) {
                Ok(p) => p,
                Err(e) => {
                    ctx.log.error(format!("partition-{part_index}: {e:#}"));
                    return;
                }
            };
            self.open_browse_zstd(
                part_index,
                ptype,
                partition_type_string,
                &data_path,
                &folder,
                &format!("partition-{part_index}"),
                ctx,
            );
            self.apply_backup_edit(&folder, &meta, part_index);
            return;
        }

        // Every other per-partition compression resolves to a ready session.
        match commander_source::session_for_backup_partition(&folder, &meta, part_index) {
            Ok(session) => {
                ctx.log.info(format!(
                    "Browsing partition {part_index} from {} backup",
                    meta.compression_type
                ));
                self.browse_view.open_with_session(session);
                // Layer edit support from the model's compression -> flow
                // mapping (raw edits directly, CHD via chd_edit; the edit-mode
                // toggle picks the mechanism by sniffing the source).
                self.apply_backup_edit(&folder, &meta, part_index);
            }
            // none / chd / chd-dvd / woz only fail on a real error (missing or
            // split data file); any other compression isn't browsable yet.
            Err(e) => match meta.compression_type.as_str() {
                "none" | "chd" | "chd-dvd" | "woz" => {
                    ctx.log.error(format!("partition-{part_index}: {e:#}"));
                }
                other => {
                    ctx.log.warn(format!(
                        "partition-{part_index}: browsing {other} compressed backups \
                         is not yet supported"
                    ));
                }
            },
        }
    }

    /// Layer edit support onto the just-opened backup partition from the shared
    /// compression -> edit-flow mapping ([`commander_source::backup_edit_for`]):
    /// raw / CHD edit in place; zstd / woz go through the
    /// decompress -> edit -> recompress archive flow; anything else stays
    /// read-only.
    fn apply_backup_edit(&mut self, folder: &Path, meta: &BackupMetadata, part_index: usize) {
        match commander_source::backup_edit_for(folder, meta, part_index) {
            Ok(commander_source::BackupEdit::InPlace) => self.browse_view.mark_edit_supported(),
            Ok(commander_source::BackupEdit::Archive(plan)) => {
                self.browse_view.set_archive_edit_context(
                    plan.archive_path,
                    plan.compression_type,
                    plan.original_size,
                    plan.compacted,
                    plan.metadata_path,
                    plan.partition_index,
                    plan.checksum_type,
                );
            }
            Ok(commander_source::BackupEdit::ReadOnly) => {}
            Err(e) => log::warn!("no edit context for partition {part_index}: {e:#}"),
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

        // Shared decision tree (same store Commander uses): a cache scanned
        // earlier this session is reused in memory, else a prior on-disk scan
        // loads, else a background scan builds one. `poll_block_cache_scan`
        // memoizes the result and opens it.
        let open = ClonezillaOpen {
            partclone_files: cz_part.partclone_files.clone(),
            partition_type: ptype,
            cache_path: folder.join(format!("_{}.metadata.cache", cz_part.device_name)),
        };
        match self.cache_store.resolve(part_index, &open) {
            PartcloneLookup::Ready(cache) => {
                ctx.log.info(format!(
                    "Browsing partition {} from cached metadata",
                    part_index
                ));
                self.browse_view.open_partclone(cache, ptype);
            }
            PartcloneLookup::NeedsScan { cache, cache_path } => {
                if self.block_cache_scan.is_some() {
                    ctx.log
                        .warn("A metadata scan is already in progress. Please wait.");
                    return;
                }
                ctx.log.info(format!(
                    "Scanning metadata for partition {} ({})...",
                    part_index,
                    partition::format_size(cz_part.size_bytes()),
                ));
                // The scan runs on a worker thread; the spawn lives in the
                // shared model runner so Commander Mode uses the same path.
                self.block_cache_scan = Some(cache_runner::spawn_partclone_scan(
                    cache, part_index, ptype, cache_path,
                ));
            }
        }
    }

    /// Open browse for a native zstd-compressed backup.
    ///
    /// If a seekable cache already exists, opens it directly.  Otherwise,
    /// opens the browser immediately via a streaming reader and starts a
    /// background thread to build the seekable cache.  When the background
    /// build completes, `poll_cache_status` will upgrade the browser
    /// automatically.
    #[allow(clippy::too_many_arguments)] // open-by-zstd threads partition info, paths, ctx
    fn open_browse_zstd(
        &mut self,
        part_index: usize,
        ptype: u8,
        partition_type_string: Option<String>,
        data_path: &Path,
        folder: &Path,
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
        self.browse_view.open_streaming(
            data_path.to_path_buf(),
            ptype,
            partition_type_string.clone(),
        );

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

        let data_path = data_path.to_path_buf();
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
                // Memoize so a later open of this partition reuses it in memory.
                self.cache_store.insert(part_index, Arc::clone(&cache));
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

    /// A 10 MB X68000 `.hdf` whose header region is mostly zero (the real
    /// shape — see the Populous/Lemmings fixtures) must NOT be treated as
    /// a floppy: it trips the loose D88 sniffer but is far over the size
    /// cap. Regression for the "Convert Floppy Container..." button
    /// showing on every HDD image.
    #[test]
    fn detect_source_is_floppy_rejects_zeroed_hdf() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("Populous.hdf");
        // 10 MB of zeros with the X68000 SCSI signature at byte 0 — the
        // header region (write-protect / media-type / first-track-offset
        // bytes the D88 sniffer reads) is all zero, the worst case.
        let mut bytes = vec![0u8; 10 * 1024 * 1024];
        bytes[0] = 0x60;
        bytes[1] = 0x00;
        bytes[2] = 0x00;
        bytes[3] = 0xca;
        std::fs::write(&path, &bytes).unwrap();
        assert!(
            !InspectTab::detect_source_is_floppy(&path),
            "10 MB HDF should not be classified as a floppy container"
        );
    }

    /// A floppy-sized container DOES surface the button. Uses a 1.2 MB
    /// `.xdf` (X68000 2HD flat size) — XDF is detected by extension, so
    /// this exercises the positive path without relying on the loose
    /// header sniffers.
    #[test]
    fn detect_source_is_floppy_accepts_xdf() {
        use rusty_backup::rbformats::containers::floppy_geom::FloppyMedia;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("game.xdf");
        let flat = vec![0u8; FloppyMedia::Hd1232.geometry().flat_size()];
        assert!(flat.len() as u64 <= MAX_FLOPPY_CONTAINER_BYTES);
        std::fs::write(&path, &flat).unwrap();
        assert!(
            InspectTab::detect_source_is_floppy(&path),
            "a 1.2 MB .xdf should be classified as a floppy container"
        );
    }
}
