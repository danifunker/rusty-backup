use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use rusty_backup::clonezilla::block_cache::PartcloneBlockCache;
use rusty_backup::fs::entry::{EntryType, FileEntry};
use rusty_backup::fs::filesystem::Filesystem;
use rusty_backup::fs::resource_fork::{self, ResourceForkMode};
use rusty_backup::fs::zstd_stream::ZstdStreamCache;
use rusty_backup::model::archive_edit::{self, ArchiveEditContext, ArchiveEditProgress};
use rusty_backup::model::browse_session::{BrowseOpenStatus, BrowseSession};
use rusty_backup::model::edit_queue::{self, EditQueue, StagedEdit};
use rusty_backup::model::status::ExtractionProgress;
use rusty_backup::partition;
use rusty_backup::rbformats::chd_edit::{
    self, is_compressed_chd, make_backup_copy, ChdEditSession,
};

const MAX_PREVIEW_SIZE: usize = 1024 * 1024; // 1 MB max file preview

/// Filesystem browser view for inspecting partition contents.
pub struct BrowseView {
    /// Root entry of the filesystem.
    root: Option<FileEntry>,
    /// Cached directory listings keyed by path.
    directory_cache: HashMap<String, Vec<FileEntry>>,
    /// Which directories are expanded in the tree.
    expanded_paths: HashSet<String>,
    /// Currently selected file entry.
    selected_entry: Option<FileEntry>,
    /// Cached content of the selected file.
    content: Option<FileContent>,
    /// View mode for file content.
    #[allow(dead_code)]
    view_mode: ViewMode,
    /// Error message to display.
    error: Option<String>,
    /// Filesystem info.
    fs_type: String,
    volume_label: String,
    /// Where to open the filesystem from (path / preopen handle / caches).
    /// `Clone` so background workers can be handed a session value.
    session: BrowseSession,
    /// Whether the browser is active (filesystem loaded).
    active: bool,
    /// Resource fork handling mode (HFS/HFS+ only).
    resource_fork_mode: ResourceForkMode,
    /// How to encode ProDOS type/aux on extract (ProDOS only).
    prodos_export_mode: ProdosExportMode,
    /// Active extraction progress (shared with background thread).
    extraction_progress: Option<Arc<Mutex<ExtractionProgress>>>,
    /// Message to show after extraction completes.
    extraction_result: Option<String>,
    /// Whether edit mode is enabled (allows adding/deleting files).
    edit_mode: bool,
    /// Whether editing is possible for this source (not Clonezilla/streaming).
    edit_supported: bool,
    /// Status message from last edit operation.
    edit_result: Option<String>,
    /// Pending "new folder" name input.
    new_folder_name: String,
    /// Whether the "new folder" dialog is open.
    show_new_folder_dialog: bool,
    /// Entry pending deletion (for confirmation dialog).
    pending_delete: Option<(FileEntry, FileEntry, bool)>, // (parent, entry, recursive)
    /// Context for backup archive editing (decompress → edit → recompress).
    archive_edit_ctx: Option<ArchiveEditContext>,
    /// Progress for archive edit background operations.
    archive_edit_progress: Option<Arc<Mutex<ArchiveEditProgress>>>,
    /// Temp file path while editing an archive (cleaned up on close/save).
    archive_temp_path: Option<PathBuf>,
    /// Blessed (bootable) system folder info (HFS/HFS+ only).
    blessed_folder: Option<(u64, String)>,
    /// Last filesystem check result (for popup display).
    fsck_result: Option<rusty_backup::fs::FsckResult>,
    /// Whether to show the fsck results popup.
    show_fsck_popup: bool,
    /// CHD info popup text. `Some` while the popup is open.
    chd_info_text: Option<String>,
    /// Whether to show debug-level fsck messages.
    show_fsck_debug: bool,
    /// Whether to show the repair confirmation dialog.
    show_repair_confirm: bool,
    /// Result of a repair operation.
    repair_report: Option<rusty_backup::fs::RepairReport>,
    /// Cached tree-view text output.
    tree_text: Option<String>,
    /// Whether to show the tree-view popup.
    show_tree_popup: bool,
    /// Whether the tree popup shows filesystem IDs.
    tree_show_ids: bool,
    /// Set while a worker thread is running `format_tree`. The thread takes
    /// ownership of the cached filesystem; we poll the status each frame and
    /// hand the fs back to `cached_fs` once the walk completes.
    pending_tree: Option<Arc<Mutex<TreeStatus>>>,
    /// Queued edit operations awaiting "Apply Edits".
    staged_edits: EditQueue,
    /// Whether to show the "unsaved changes" confirmation dialog.
    show_unsaved_dialog: bool,
    /// When true, a successful Discard/Apply from the unsaved dialog should
    /// fully close the browse view rather than just leaving edit mode. Set by
    /// `close_or_prompt()` when the caller wants to dismiss the view but
    /// staged edits force the dialog first.
    pending_close: bool,
    /// Inline ProDOS type/aux editor state, keyed by entry path. Reset when
    /// the selection changes.
    prodos_type_editor: Option<ProdosTypeEditorState>,
    /// Inline HFS type/creator editor state, keyed by entry path. The fields
    /// are kept as 4-char-clamped strings so the user can type freely; on
    /// "Set" we encode and stage. `None` means the editor is closed for that
    /// entry; on first open we seed from current FInfo or the dictionary.
    hfs_type_editor: Option<HfsTypeEditorState>,
    /// Files that failed to stage (bad name, IO error, etc.). When non-empty,
    /// `show_staging_errors` drives a modal dialog listing each failure.
    staging_errors: Vec<(PathBuf, String)>,
    /// Whether the staging-errors modal dialog is open.
    show_staging_errors: bool,
    /// Extraction parameters awaiting overwrite confirmation.
    pending_extraction: Option<PendingExtraction>,
    /// Live CHD edit-mode state. Present while editing a CHD via
    /// [`chd_edit::ChdEditSession`] (diff-against-parent for compressed,
    /// in-place for uncompressed). The `Arc<Mutex<ChdEditSession>>` itself
    /// lives on `self.session.chd_edit_session`; this struct just holds the
    /// paths needed to flatten / clean up on exit.
    chd_edit: Option<ChdEditState>,
    /// Background flatten progress (compressed CHD apply): merges the diff
    /// into a fresh compressed CHD that overwrites the parent.
    chd_flatten_progress: Option<Arc<Mutex<ChdFlattenProgress>>>,
    /// When the CHD being edited is the body of a single-file-chd backup,
    /// this carries the backup folder path so that on flatten-success we
    /// can refresh `metadata.json` (per-partition checksums + container
    /// SHA-1). `None` for plain CHD images that aren't part of a backup.
    single_file_chd_backup_folder: Option<PathBuf>,

    /// Set while the worker spawned by `BrowseSession::spawn_open` is loading
    /// the filesystem and root listing. Polled each frame in `show()`; once
    /// the worker reports `finished`, its results are drained into this view's
    /// state. Showing a spinner + phase here is much friendlier than freezing
    /// the UI for the seconds a 500k-file HFS+ open can take.
    pending_open: Option<Arc<Mutex<BrowseOpenStatus>>>,

    /// One open `Filesystem` instance reused across read-only operations
    /// (directory listings, file previews, fsck, tree dumps). Re-opening for
    /// every operation forces a re-read of the entire catalog — fine for
    /// FAT, slow for HFS+ on a heavily-used volume. Read-only paths borrow
    /// this via `take_or_open_fs` and return it via `return_fs`. Any code
    /// that mutates the volume (sync_metadata, archive recompress, edit
    /// apply) calls `invalidate_cached_fs` so the next read sees disk truth.
    cached_fs: Option<Box<dyn Filesystem>>,
}

/// State carried for the duration of a CHD edit-mode session.
struct ChdEditState {
    /// Original CHD file the user is editing (and which `flatten_to_parent`
    /// will overwrite on exit).
    parent_path: PathBuf,
    /// Companion diff file. `Some` for compressed CHDs; `None` when editing
    /// an uncompressed CHD in place.
    diff_path: Option<PathBuf>,
}

/// Background flatten progress for compressed CHD edit-apply. Mirrors
/// [`ArchiveEditProgress`] but lives in the GUI crate so the worker thread
/// can update it without pulling chd_edit into model/.
struct ChdFlattenProgress {
    current: u64,
    total: u64,
    finished: bool,
    error: Option<String>,
    cancel_requested: bool,
}

/// Captured state from `start_extraction` so the extraction can resume after
/// the user answers the overwrite confirmation dialog.
struct PendingExtraction {
    entry: FileEntry,
    dest: PathBuf,
    /// Path(s) at `dest` that already exist and would be replaced.
    conflicts: Vec<PathBuf>,
}

/// Transient state for the "Set ProDOS Type…" dialog.
#[derive(Debug, Clone)]
/// Inline editor state for an HFS/HFS+ file's type and creator codes.
struct HfsTypeEditorState {
    /// Path of the entry being edited — invalidates when selection changes.
    entry_path: String,
    /// 4-char freeform input for the type code (clamped on every frame).
    type_input: String,
    /// 4-char freeform input for the creator code.
    creator_input: String,
}

/// Inline editor state for a ProDOS file's type byte and aux type.
struct ProdosTypeEditorState {
    /// Path of the entry being edited — invalidates when selection changes.
    entry_path: String,
    /// 2-char hex input for the type byte.
    type_input: String,
    /// 4-char hex input for the aux type.
    aux_input: String,
}

#[derive(Debug, Clone)]
enum FileContent {
    Binary(Vec<u8>),
    Text(String),
}

/// How to encode ProDOS file type/aux when extracting files to a host
/// directory. Only meaningful for ProDOS filesystems.
#[derive(Debug, Clone, Copy, PartialEq)]
enum ProdosExportMode {
    /// Append a CiderPress `#TTAAAA` suffix to each extracted filename so
    /// type and aux survive a round-trip back into ProDOS.
    WithTypeSuffix,
    /// Write the bare ProDOS name (lossy — type/aux are dropped).
    Plain,
}

impl ProdosExportMode {
    const ALL: [ProdosExportMode; 2] = [ProdosExportMode::WithTypeSuffix, ProdosExportMode::Plain];

    fn label(&self) -> &'static str {
        match self {
            ProdosExportMode::WithTypeSuffix => "Name + #TTAAAA suffix",
            ProdosExportMode::Plain => "Bare name (lossy)",
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq)]
enum ViewMode {
    Auto,
    Hex,
    Text,
}

impl Default for BrowseView {
    fn default() -> Self {
        Self {
            root: None,
            directory_cache: HashMap::new(),
            expanded_paths: HashSet::new(),
            selected_entry: None,
            content: None,
            view_mode: ViewMode::Auto,
            error: None,
            fs_type: String::new(),
            volume_label: String::new(),
            session: BrowseSession::new(),
            active: false,
            resource_fork_mode: ResourceForkMode::AppleDouble,
            prodos_export_mode: ProdosExportMode::WithTypeSuffix,
            extraction_progress: None,
            extraction_result: None,
            edit_mode: false,
            edit_supported: false,
            edit_result: None,
            new_folder_name: String::new(),
            show_new_folder_dialog: false,
            pending_delete: None,
            archive_edit_ctx: None,
            archive_edit_progress: None,
            archive_temp_path: None,
            blessed_folder: None,
            fsck_result: None,
            show_fsck_popup: false,
            chd_info_text: None,
            show_fsck_debug: false,
            show_repair_confirm: false,
            repair_report: None,
            tree_text: None,
            show_tree_popup: false,
            tree_show_ids: false,
            staged_edits: EditQueue::new(),
            show_unsaved_dialog: false,
            pending_close: false,
            prodos_type_editor: None,
            hfs_type_editor: None,
            staging_errors: Vec::new(),
            show_staging_errors: false,
            pending_extraction: None,
            chd_edit: None,
            chd_flatten_progress: None,
            single_file_chd_backup_folder: None,
            pending_open: None,
            cached_fs: None,
            pending_tree: None,
        }
    }
}

/// Shared state between the GUI and the worker thread that runs
/// `format_tree`. The walk can take seconds on large HFS+ volumes, so it
/// runs off the UI thread and the GUI polls this struct each frame. The
/// `fs` slot lets us hand the filesystem back to `cached_fs` after the
/// walk so subsequent reads don't re-open.
pub struct TreeStatus {
    pub finished: bool,
    pub text: Option<String>,
    pub error: Option<String>,
    pub fs: Option<Box<dyn Filesystem>>,
}

impl BrowseView {
    /// Initialize the browser for a partition within a source image/device.
    ///
    /// `preopen_file` — if provided, this already-elevated file handle is used
    /// for all filesystem reads instead of re-opening `source_path`.  Pass
    /// `Some(file)` when browsing a raw device on macOS to avoid a second auth
    /// prompt; pass `None` for backup files / image files.
    pub fn open(
        &mut self,
        source_path: PathBuf,
        partition_offset: u64,
        partition_type: u8,
        partition_type_string: Option<String>,
        preopen_file: Option<File>,
    ) {
        self.close();
        self.session.source_path = Some(source_path.clone());
        self.session.partition_offset = partition_offset;
        self.session.partition_type = partition_type;
        self.session.partition_type_string = partition_type_string;
        self.session.preopen_file = preopen_file.map(std::sync::Arc::new);
        // Editing is supported for regular files (not Clonezilla/streaming)
        self.edit_supported = true;

        // WOZ images still go through the decompress→edit→recompress
        // archive flow (no in-place / diff support). CHDs use the lighter
        // chd_edit path: nothing to set up here — the edit-mode toggle
        // calls `enter_chd_edit_mode` which opens a `ChdEditSession`
        // (diff-against-parent for compressed, in-place for uncompressed).
        let ext = source_path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        if ext == "woz" {
            let original_size = rusty_backup::rbformats::woz::WozReader::open(&source_path)
                .map(|r| r.len())
                .unwrap_or(0);
            log::info!(
                "Container edit context set for {} ({} decoded bytes)",
                source_path.display(),
                original_size
            );
            self.archive_edit_ctx = Some(ArchiveEditContext {
                archive_path: source_path.clone(),
                compression_type: "woz".to_string(),
                original_size,
                compacted: false,
                metadata_path: PathBuf::new(),
                partition_index: 0,
                checksum_type: String::new(),
            });
        }

        // Open + initial root listing run on a worker thread so the UI can
        // paint a spinner + phase while we wait. `show()` drains the result
        // each frame.
        self.active = true;
        self.pending_open = Some(self.session.spawn_open());
    }

    /// Open the browser using a partclone block cache (for Clonezilla images).
    pub fn open_partclone(&mut self, cache: Arc<Mutex<PartcloneBlockCache>>, partition_type: u8) {
        self.close();
        self.session.partclone_cache = Some(cache);
        self.session.partition_type = partition_type;
        self.session.partition_offset = 0;
        // Same async pattern as `open` / `open_streaming`: hand the open and
        // initial root listing to a worker so the UI can paint a spinner.
        self.active = true;
        self.pending_open = Some(self.session.spawn_open());
    }

    /// Open the browser by streaming a native zstd-compressed partition image.
    ///
    /// The filesystem opens immediately via a `ZstdStreamReader` backed by a
    /// 256 MB in-memory buffer.  Call `upgrade_to_seekable_cache` once the
    /// background seekable cache is ready to enable full random access.
    pub fn open_streaming(&mut self, path: PathBuf, ptype: u8, ptype_str: Option<String>) {
        self.close();
        self.session.partition_type = ptype;
        self.session.partition_type_string = ptype_str;
        self.session.partition_offset = 0;

        let cache = match ZstdStreamCache::new(&path) {
            Ok(c) => Arc::new(Mutex::new(c)),
            Err(e) => {
                self.error = Some(format!("Cannot open zstd stream: {e}"));
                self.active = true;
                return;
            }
        };
        self.session.zstd_cache = Some(cache);

        // Open + initial root listing on a worker thread so the UI can paint
        // a spinner while a slow first read (NAS, large HFS+ catalog) runs.
        // `show()` drains the result each frame.
        self.active = true;
        self.pending_open = Some(self.session.spawn_open());
    }

    /// Set up archive edit context so that toggling edit mode triggers
    /// decompress → edit → recompress flow instead of direct editing.
    pub fn set_archive_edit_context(
        &mut self,
        archive_path: PathBuf,
        compression_type: String,
        original_size: u64,
        compacted: bool,
        metadata_path: PathBuf,
        partition_index: usize,
        checksum_type: String,
    ) {
        self.archive_edit_ctx = Some(ArchiveEditContext {
            archive_path,
            compression_type,
            original_size,
            compacted,
            metadata_path,
            partition_index,
            checksum_type,
        });
        self.edit_supported = true;
    }

    /// Mark the currently-open CHD as the body of a single-file-chd backup.
    /// On a successful chd_edit flatten we'll refresh the backup's
    /// `metadata.json` (per-partition SHA-256 + container SHA-1).
    pub fn set_single_file_chd_backup_folder(&mut self, backup_folder: PathBuf) {
        self.single_file_chd_backup_folder = Some(backup_folder);
    }

    /// (`zstd_cache` is `Some`).  The directory cache is preserved so the
    /// user stays in the same place in the tree.
    pub fn upgrade_to_seekable_cache(&mut self, cache_path: PathBuf) {
        if !self.active || self.session.zstd_cache.is_none() {
            return;
        }
        self.session.source_path = Some(cache_path);
        self.session.zstd_cache = None;
    }

    pub fn close(&mut self) {
        self.root = None;
        self.directory_cache.clear();
        self.expanded_paths.clear();
        self.selected_entry = None;
        self.content = None;
        self.error = None;
        self.active = false;
        self.fs_type.clear();
        self.volume_label.clear();
        self.session = BrowseSession::new();
        self.extraction_progress = None;
        self.extraction_result = None;
        self.edit_mode = false;
        self.edit_supported = false;
        self.edit_result = None;
        self.new_folder_name.clear();
        self.show_new_folder_dialog = false;
        self.pending_delete = None;
        self.blessed_folder = None;
        self.fsck_result = None;
        self.show_fsck_popup = false;
        self.show_repair_confirm = false;
        self.repair_report = None;
        self.tree_text = None;
        self.show_tree_popup = false;
        self.tree_show_ids = false;
        self.pending_tree = None;
        self.staged_edits.clear();
        self.show_unsaved_dialog = false;
        self.pending_close = false;
        // Detach any in-flight open worker. The thread keeps running but its
        // result will be dropped when the Arc dies on completion.
        self.pending_open = None;
        self.cached_fs = None;
        // Clean up archive temp file if present
        if let Some(temp) = self.archive_temp_path.take() {
            let _ = std::fs::remove_file(&temp);
        }
        self.archive_edit_ctx = None;
        self.archive_edit_progress = None;
        // Drop any live CHD edit session and remove its diff. The parent
        // CHD is left untouched; user's `.chd_backup` is preserved.
        self.discard_chd_edit_session();
        self.chd_flatten_progress = None;
        self.single_file_chd_backup_folder = None;
    }

    pub fn is_active(&self) -> bool {
        self.active
    }

    /// Close the browse view, prompting first if there are unsaved staged
    /// edits. Returns true if the view is now closed; false if the unsaved
    /// dialog was shown and the caller should retry on a later frame.
    pub fn close_or_prompt(&mut self) -> bool {
        if !self.active {
            return true;
        }
        if self.edit_mode && !self.staged_edits.is_empty() {
            self.show_unsaved_dialog = true;
            self.pending_close = true;
            return false;
        }
        self.close();
        true
    }

    /// Returns true if the current filesystem is HFS or HFS+.
    fn is_hfs_type(&self) -> bool {
        let ft = self.fs_type.as_str();
        ft == "HFS" || ft == "HFS+" || ft == "HFSX"
    }

    /// Returns true if the current filesystem is ProDOS.
    fn is_prodos_type(&self) -> bool {
        self.fs_type == "ProDOS"
    }

    /// Validate a filename against the current filesystem's rules at
    /// staging time, so invalid names are rejected before apply.
    fn validate_staged_name(&self, name: &str) -> Result<(), String> {
        let fs = self.session.open().map_err(|e| e.to_string())?;
        fs.validate_name(name).map_err(|e| e.to_string())
    }

    pub fn show(&mut self, ui: &mut egui::Ui) {
        if !self.active {
            return;
        }

        // If a background open is in progress, drain its status and either
        // render a spinner or finalize the open with the results it produced.
        if self.pending_open.is_some() {
            self.poll_pending_open(ui);
            if self.pending_open.is_some() {
                // Still loading — show only the progress indicator.
                return;
            }
        }

        // Poll background tree-view generation. Only renders a spinner here;
        // the rest of the UI stays interactive while the walk runs.
        if self.pending_tree.is_some() {
            self.poll_pending_tree(ui);
        }

        // Poll extraction progress
        self.poll_extraction(ui);

        // Handle drag-and-drop from host OS
        self.handle_dropped_files(ui);

        // Header
        ui.horizontal(|ui| {
            ui.label(egui::RichText::new("Filesystem Browser").strong());
            ui.label(format!("[{}]", self.fs_type));
            if !self.volume_label.is_empty() {
                ui.label(format!("Label: {}", self.volume_label));
            }
            if let Some((_, ref name)) = self.blessed_folder {
                ui.label(format!("Blessed: {}", name));
            }

            // Edit mode toggle
            if self.edit_supported {
                let busy = self.extraction_progress.is_some()
                    || self.archive_edit_progress.is_some()
                    || self.chd_flatten_progress.is_some();
                ui.add_space(8.0);
                let edit_btn = if self.edit_mode {
                    egui::Button::new("Edit Mode ON")
                } else {
                    egui::Button::new("Edit Mode")
                };
                let btn = ui.add_enabled(!busy, edit_btn);
                if btn.clicked() {
                    log::info!(
                        "Edit Mode clicked: archive_edit_ctx={}, chd_edit={}, edit_mode={}",
                        self.archive_edit_ctx.is_some(),
                        self.chd_edit.is_some(),
                        self.edit_mode
                    );
                    if self.is_chd_source() {
                        // CHD editing: open ChdEditSession on entry; flatten
                        // the diff back into the parent on exit.
                        if !self.edit_mode {
                            self.enter_chd_edit_mode();
                        } else if !self.staged_edits.is_empty() {
                            self.show_unsaved_dialog = true;
                        } else {
                            self.exit_edit_mode();
                        }
                    } else if self.archive_edit_ctx.is_some() {
                        // Archive editing: toggle triggers decompress/recompress
                        if !self.edit_mode {
                            log::info!("Starting archive extract for editing");
                            self.start_archive_extract();
                        } else if !self.staged_edits.is_empty() {
                            self.show_unsaved_dialog = true;
                        } else {
                            self.start_archive_compress();
                        }
                    } else {
                        // Direct editing (raw image / device)
                        if self.edit_mode && !self.staged_edits.is_empty() {
                            self.show_unsaved_dialog = true;
                        } else if !self.edit_mode {
                            // Probe the editable open up-front so guards like
                            // the journaled-HFS+ refusal surface as a toast
                            // instead of being deferred to the first edit.
                            match self.session.open_editable() {
                                Ok(_efs) => {
                                    self.edit_mode = true;
                                }
                                Err(e) => {
                                    self.error = Some(format!("Cannot enter edit mode: {e}"));
                                }
                            }
                        } else {
                            self.edit_mode = false;
                            self.edit_result = None;
                            self.show_new_folder_dialog = false;
                            self.pending_delete = None;
                            self.staged_edits.clear();
                        }
                    }
                }
                if !self.edit_mode && btn.hovered() {
                    btn.on_hover_text("Enable editing to add or delete files on this image");
                }
            }

            // Check filesystem button (HFS only for now)
            if self.fs_type == "HFS" && ui.button("Check").clicked() {
                match self.take_or_open_fs() {
                    Some(mut fs) => {
                        match fs.fsck() {
                            Some(Ok(result)) => {
                                self.fsck_result = Some(result);
                                self.show_fsck_popup = true;
                            }
                            Some(Err(e)) => {
                                self.error = Some(format!("Filesystem check failed: {}", e));
                            }
                            None => {
                                self.error =
                                    Some("Filesystem check not supported for this type".into());
                            }
                        }
                        self.return_fs(fs);
                    }
                    None => {
                        self.error = Some("Failed to open filesystem".into());
                    }
                }
            }

            if self.is_chd_source() {
                if let Some(chd_path) = self.session.source_path.clone() {
                    if ui.button("CHD Info").clicked() {
                        match rusty_backup::rbformats::chd::format_chd_info(&chd_path) {
                            Ok(text) => self.chd_info_text = Some(text),
                            Err(e) => {
                                self.error = Some(format!("CHD Info failed: {}", e));
                            }
                        }
                    }
                }
            }

            if ui.button("Tree").clicked() {
                self.generate_tree_text();
            }

            if ui.button("Close").clicked() {
                if self.edit_mode && !self.staged_edits.is_empty() {
                    self.show_unsaved_dialog = true;
                } else {
                    self.close();
                }
            }
        });

        if let Some(err) = &self.error {
            ui.colored_label(egui::Color32::from_rgb(255, 100, 100), err);
        }

        // Edit result message
        if let Some(result) = &self.edit_result.clone() {
            ui.horizontal(|ui| {
                let color = if result.starts_with("Error") {
                    egui::Color32::from_rgb(255, 100, 100)
                } else {
                    egui::Color32::from_rgb(100, 200, 100)
                };
                ui.colored_label(color, result);
                if ui.button("OK").clicked() {
                    self.edit_result = None;
                }
            });
        }

        // Extraction result message
        if let Some(result) = &self.extraction_result.clone() {
            ui.horizontal(|ui| {
                ui.label(result);
                if ui.button("OK").clicked() {
                    self.extraction_result = None;
                }
            });
        }

        // Archive edit progress bar
        self.poll_archive_edit(ui);
        if let Some(progress) = &self.archive_edit_progress {
            if let Ok(p) = progress.lock() {
                ui.horizontal(|ui| {
                    ui.spinner();
                    ui.label(format!("{}...", p.phase));
                    if p.total > 0 {
                        let frac = p.current as f32 / p.total as f32;
                        ui.add(egui::ProgressBar::new(frac).show_percentage());
                    }
                });
                ui.ctx().request_repaint();
            }
        }

        // CHD flatten progress bar (compressed CHD edit-apply)
        self.poll_chd_flatten(ui);
        if let Some(progress) = &self.chd_flatten_progress {
            if let Ok(p) = progress.lock() {
                ui.horizontal(|ui| {
                    ui.spinner();
                    ui.label("Flattening CHD...");
                    if p.total > 0 {
                        let frac = p.current as f32 / p.total as f32;
                        ui.add(egui::ProgressBar::new(frac).show_percentage());
                    }
                });
                ui.ctx().request_repaint();
            }
        }

        // Info banner while editing an archive
        if self.edit_mode && self.archive_edit_ctx.is_some() {
            ui.colored_label(
                egui::Color32::from_rgb(100, 160, 255),
                "Editing temporary copy. Click 'Apply Edits' to write changes.",
            );
        }
        if self.edit_mode && self.chd_edit.is_some() {
            ui.colored_label(
                egui::Color32::from_rgb(100, 160, 255),
                "Editing CHD via diff. Click 'Apply Edits' to flush, or exit Edit Mode to merge into the CHD.",
            );
        }

        // Edit mode toolbar
        if self.edit_mode {
            self.render_edit_toolbar(ui);
        }

        ui.separator();

        // Show drop target overlay when files are being dragged over
        if self.edit_mode {
            let hovered = ui.ctx().input(|i| !i.raw.hovered_files.is_empty());
            if hovered {
                ui.colored_label(
                    egui::Color32::from_rgb(100, 180, 255),
                    "Drop files here to add them to the current directory",
                );
                ui.separator();
            }
        }

        // Delete confirmation dialog
        self.render_delete_dialog(ui);

        // Unsaved changes dialog
        self.render_unsaved_dialog(ui);

        // New folder dialog
        self.render_new_folder_dialog(ui);

        // ProDOS "Set Type…" dialog

        // Staging errors dialog
        self.render_staging_errors_dialog(ui);

        // Extraction overwrite-confirm dialog
        self.render_extract_overwrite_dialog(ui);

        // Two-panel layout: tree | content
        let available = ui.available_size();
        let tree_width = (available.x * 0.4).max(200.0).min(400.0);
        let panel_height = available.y;

        ui.horizontal(|ui| {
            ui.set_min_height(panel_height);

            // Left panel: file tree
            ui.vertical(|ui| {
                ui.set_width(tree_width);
                ui.set_min_height(panel_height);
                egui::ScrollArea::vertical()
                    .id_salt("browse_tree")
                    .max_height(panel_height)
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        if let Some(root) = self.root.clone() {
                            self.render_tree_entry(ui, &root);
                        }
                    });
            });

            ui.separator();

            // Right panel: file content / details
            ui.vertical(|ui| {
                ui.set_min_width(available.x - tree_width - 20.0);
                ui.set_min_height(panel_height);
                self.render_content_panel(ui, panel_height);
            });
        });

        // Fsck results popup window
        self.render_fsck_popup(ui);
        self.render_chd_info_popup(ui);
        self.render_tree_popup(ui);
    }

    fn render_tree_entry(&mut self, ui: &mut egui::Ui, entry: &FileEntry) {
        let pending_del = self.edit_mode && self.staged_edits.is_pending_delete(&entry.path);
        let dimmed = egui::Color32::from_rgb(120, 120, 120);

        match entry.entry_type {
            EntryType::Directory => {
                let path = entry.path.clone();
                let has_children = self.directory_cache.contains_key(&path);

                let dir_label = if self
                    .blessed_folder
                    .as_ref()
                    .map(|(cnid, _)| *cnid == entry.location)
                    .unwrap_or(false)
                {
                    format!("{} [System]", entry.name)
                } else {
                    entry.name.clone()
                };

                let id = ui.make_persistent_id(&path);
                let mut state = egui::collapsing_header::CollapsingState::load_with_default_open(
                    ui.ctx(),
                    id,
                    path == "/",
                );

                let is_selected = self
                    .selected_entry
                    .as_ref()
                    .map(|s| s.path == entry.path)
                    .unwrap_or(false);

                let header_res = ui.horizontal(|ui| {
                    state.show_toggle_button(ui, egui::collapsing_header::paint_default_icon);
                    if pending_del {
                        let text = egui::RichText::new(&dir_label)
                            .color(dimmed)
                            .strikethrough();
                        ui.label(text);
                    } else if ui.selectable_label(is_selected, &dir_label).clicked() {
                        self.selected_entry = Some(entry.clone());
                        self.content = None;
                        self.error = None;
                    }
                });

                state.show_body_indented(&header_res.response, ui, |ui| {
                    if let Some(children) = self.directory_cache.get(&path).cloned() {
                        for child in &children {
                            self.render_tree_entry(ui, child);
                        }
                    } else {
                        ui.label("Loading...");
                    }

                    // Append pending-add entries for this directory
                    if self.edit_mode {
                        let pending = self.staged_edits.pending_adds_for(&path);
                        for pentry in &pending {
                            self.render_pending_add_entry(ui, pentry);
                        }
                    }
                });

                let is_now_open = state.is_open();

                // Load directory contents on first expansion
                if is_now_open {
                    if !has_children {
                        self.load_directory(entry);
                    }
                    self.expanded_paths.insert(path.clone());
                } else {
                    self.expanded_paths.remove(&path);
                }
            }
            EntryType::File => {
                let is_selected = self
                    .selected_entry
                    .as_ref()
                    .map(|s| s.path == entry.path)
                    .unwrap_or(false);

                let size_str = entry.size_string();
                // ProDOS entries get an inline "$XX ABC" type badge so the user
                // can see the file's type/aux without clicking into it.
                let prodos_badge = if self.is_prodos_type() {
                    entry.type_code.as_deref().map(|tc| tc.to_string())
                } else {
                    None
                };
                let hover_text = prodos_badge.as_ref().map(|_| {
                    let tc = entry.type_code.as_deref().unwrap_or("");
                    let tt = tc
                        .split_whitespace()
                        .next()
                        .and_then(|s| u8::from_str_radix(s.trim_start_matches('$'), 16).ok())
                        .unwrap_or(0);
                    let desc = rusty_backup::fs::prodos_types::type_description(tt);
                    let aux = entry.aux_type.unwrap_or(0);
                    if desc.is_empty() {
                        format!("{tc} ${:04X}", aux)
                    } else {
                        format!("{desc} ${:04X}", aux)
                    }
                });

                if pending_del {
                    let label = if let Some(ref b) = prodos_badge {
                        format!("{}  [{}]  ({})", entry.name, b, size_str)
                    } else {
                        format!("{}  ({})", entry.name, size_str)
                    };
                    let text = egui::RichText::new(&label).color(dimmed).strikethrough();
                    ui.label(text);
                } else {
                    let label = if let Some(ref b) = prodos_badge {
                        format!("{}  [{}]  ({})", entry.name, b, size_str)
                    } else {
                        format!("{}  ({})", entry.name, size_str)
                    };
                    let resp = ui.selectable_label(is_selected, &label);
                    let resp = if let Some(h) = hover_text {
                        resp.on_hover_text(h)
                    } else {
                        resp
                    };
                    if resp.clicked() {
                        self.select_file(entry);
                    }
                }
            }
            EntryType::Symlink => {
                let is_selected = self
                    .selected_entry
                    .as_ref()
                    .map(|s| s.path == entry.path)
                    .unwrap_or(false);

                let target = entry.symlink_target.as_deref().unwrap_or("?");
                let label = format!("{} -> {}", entry.name, target);

                if pending_del {
                    let text = egui::RichText::new(&label).color(dimmed).strikethrough();
                    ui.label(text);
                } else if ui.selectable_label(is_selected, &label).clicked() {
                    self.select_file(entry);
                }
            }
            EntryType::Special => {
                let is_selected = self
                    .selected_entry
                    .as_ref()
                    .map(|s| s.path == entry.path)
                    .unwrap_or(false);

                let stype = entry.special_type.as_deref().unwrap_or("special");
                let label = format!("{}  ({})", entry.name, stype);

                if pending_del {
                    let text = egui::RichText::new(&label).color(dimmed).strikethrough();
                    ui.label(text);
                } else if ui.selectable_label(is_selected, &label).clicked() {
                    self.selected_entry = Some(entry.clone());
                    self.content = None;
                }
            }
        }
    }

    /// Render a pending-add entry with green "+" prefix (not selectable for content).
    fn render_pending_add_entry(&mut self, ui: &mut egui::Ui, entry: &FileEntry) {
        let green = egui::Color32::from_rgb(100, 200, 100);
        let blue = egui::Color32::from_rgb(120, 160, 220);
        // On HFS/HFS+ and ProDOS, surface files that will be added without a
        // resolved type by tinting them blue so the user can fix them before
        // Apply. HFS: no FInfo + no dict match + no override. ProDOS: type byte
        // not in the registry.
        let color = if self.is_hfs_type() && entry.is_file() {
            let (t, c) = self.staged_edits.resolved_hfs_type_creator(entry);
            if t == [0; 4] && c == [0; 4] {
                blue
            } else {
                green
            }
        } else if self.is_prodos_type() && entry.is_file() {
            let (t, _) = self.resolved_prodos_type(entry);
            if !rusty_backup::fs::prodos_types::is_known_type(t) {
                blue
            } else {
                green
            }
        } else {
            green
        };
        let is_selected = self
            .selected_entry
            .as_ref()
            .map(|s| s.path == entry.path)
            .unwrap_or(false);

        // Look up the staged AddFile for this entry to surface rsrc/type info.
        let rsrc_badge: Option<String> = self
            .staged_edits
            .pending_resource_fork_for(&entry.path)
            .map(|imp| {
                let tc = imp
                    .type_code
                    .map(|c| String::from_utf8_lossy(&c).to_string());
                let cc = imp
                    .creator_code
                    .map(|c| String::from_utf8_lossy(&c).to_string());
                let codes = match (tc, cc) {
                    (Some(t), Some(c)) => format!("{t}/{c} "),
                    (Some(t), None) => format!("{t} "),
                    (None, Some(c)) => format!("?/{c} "),
                    (None, None) => String::new(),
                };
                format!(
                    "[{}+rsrc {}]",
                    codes,
                    partition::format_size(imp.data.len() as u64)
                )
            });

        let label = if entry.is_directory() {
            format!("+ {}", entry.name)
        } else if let Some(ref badge) = rsrc_badge {
            format!("+ {}  {}  ({})", entry.name, badge, entry.size_string())
        } else {
            format!("+ {}  ({})", entry.name, entry.size_string())
        };

        let text = egui::RichText::new(&label).color(color);

        if entry.is_directory() {
            // Pending directories collapse open just like real ones, and their
            // staged children render underneath via recursion.
            let id = ui.make_persistent_id(("pending-dir", &entry.path));
            let mut state = egui::collapsing_header::CollapsingState::load_with_default_open(
                ui.ctx(),
                id,
                true,
            );
            let header_res = ui.horizontal(|ui| {
                state.show_toggle_button(ui, egui::collapsing_header::paint_default_icon);
                if ui.selectable_label(is_selected, text).clicked() {
                    self.selected_entry = Some(entry.clone());
                    self.content = None;
                }
            });
            state.show_body_indented(&header_res.response, ui, |ui| {
                let nested = self.staged_edits.pending_adds_for(&entry.path);
                for child in &nested {
                    self.render_pending_add_entry(ui, child);
                }
            });
        } else if ui.selectable_label(is_selected, text).clicked() {
            // Allow selecting pending-add entries (for unstaging via delete)
            self.selected_entry = Some(entry.clone());
            self.content = None;
        }
    }

    fn load_directory(&mut self, entry: &FileEntry) {
        if let Some(mut fs) = self.take_or_open_fs() {
            match fs.list_directory(entry) {
                Ok(entries) => {
                    self.directory_cache.insert(entry.path.clone(), entries);
                }
                Err(e) => {
                    self.error = Some(format!("Failed to read {}: {e}", entry.path));
                }
            }
            self.return_fs(fs);
        }
    }

    fn select_file(&mut self, entry: &FileEntry) {
        self.selected_entry = Some(entry.clone());
        self.content = None;
        self.error = None;

        // Symlinks: show the target path as text content
        if entry.is_symlink() {
            let target = entry
                .symlink_target
                .as_deref()
                .unwrap_or("(unknown target)");
            self.content = Some(FileContent::Text(format!("Symlink target: {target}")));
            return;
        }

        // Special files: no content to preview
        if entry.is_special() {
            let stype = entry.special_type.as_deref().unwrap_or("special file");
            self.content = Some(FileContent::Text(format!(
                "{} -- no preview available",
                stype
            )));
            return;
        }

        if entry.size > MAX_PREVIEW_SIZE as u64 {
            // Don't auto-load large files
            return;
        }

        if let Some(mut fs) = self.take_or_open_fs() {
            match fs.read_file(entry, MAX_PREVIEW_SIZE) {
                Ok(data) => {
                    self.content = Some(detect_content_type(entry, &data));
                }
                Err(e) => {
                    self.error = Some(format!("Failed to read file: {e}"));
                }
            }
            self.return_fs(fs);
        }
    }

    fn render_content_panel(&mut self, ui: &mut egui::Ui, panel_height: f32) {
        // Extraction progress bar
        if let Some(progress) = &self.extraction_progress {
            if let Ok(p) = progress.lock() {
                let fraction = if p.total_bytes > 0 {
                    p.current_bytes as f32 / p.total_bytes as f32
                } else if p.total_files > 0 {
                    p.files_extracted as f32 / p.total_files as f32
                } else {
                    0.0
                };
                let text = format!(
                    "Extracting {}/{} files: {}",
                    p.files_extracted, p.total_files, p.current_file
                );
                ui.add(egui::ProgressBar::new(fraction).text(text));
                if !p.finished {
                    // Clone progress Arc to allow the mutable borrow for the button
                    let progress_clone = Arc::clone(progress);
                    drop(p);
                    if ui.button("Cancel").clicked() {
                        if let Ok(mut p) = progress_clone.lock() {
                            p.cancel_requested = true;
                        }
                    }
                    ui.separator();
                }
            }
        }

        match &self.selected_entry {
            None => {
                ui.colored_label(egui::Color32::GRAY, "Select a file to view its contents.");
            }
            Some(entry) => {
                let entry = entry.clone();
                // File info header
                ui.label(egui::RichText::new(&entry.name).strong());
                ui.horizontal(|ui| {
                    ui.label(format!("Size: {}", entry.size_string()));
                    if let Some(modified) = &entry.modified {
                        if !modified.is_empty() {
                            ui.label(format!("Modified: {modified}"));
                        }
                    }
                    // HFS/HFS+ and ProDOS render Type/Creator (or Type/Aux)
                    // below in a dedicated editor row. For other filesystems
                    // there's nothing extra to show, so this block is empty.
                    if !self.is_hfs_type() && !self.is_prodos_type() {
                        if let Some(ref tc) = entry.type_code {
                            ui.label(format!("Type: {tc}"));
                        }
                        if let Some(ref cc) = entry.creator_code {
                            ui.label(format!("Creator: {cc}"));
                        }
                    }
                    if let Some(ref rsrc) = entry.resource_fork_size {
                        if *rsrc > 0 {
                            ui.label(format!("Rsrc: {}", partition::format_size(*rsrc)));
                        }
                    }
                    if let Some(mode_str) = entry.mode_string() {
                        ui.label(format!("Permissions: {mode_str}"));
                    }
                    if let (Some(uid), Some(gid)) = (entry.uid, entry.gid) {
                        ui.label(format!("Owner: {uid}:{gid}"));
                    }
                    if let Some(ref target) = entry.symlink_target {
                        ui.label(format!("Target: {target}"));
                    }
                    if let Some(ref stype) = entry.special_type {
                        ui.label(format!("Type: {stype}"));
                    }
                    ui.label(format!("Path: {}", entry.path));
                });

                // HFS/HFS+ type/creator row — read-only labels normally, full
                // editor (text fields + dictionary pulldown) when in edit mode.
                if self.is_hfs_type() && entry.is_file() {
                    self.render_hfs_type_row(ui, &entry);
                }

                // ProDOS type/aux row — read-only normally, type pulldown + hex
                // inputs when in edit mode.
                if self.is_prodos_type() && entry.is_file() {
                    self.render_prodos_type_row(ui, &entry);
                }

                // ProDOS/GS-OS leaves $CB..$EE unassigned in the official
                // type registry; vintage apps often picked bytes out of
                // that range for their own data files. Surface a note so
                // the user knows the file isn't corrupt — the type and
                // aux round-trip via the CiderPress #TTAAAA suffix.
                if self.is_prodos_type() {
                    let tt = entry.type_code.as_deref().and_then(|tc| {
                        tc.split_whitespace()
                            .next()
                            .and_then(|s| u8::from_str_radix(s.trim_start_matches('$'), 16).ok())
                    });
                    if let Some(tt) = tt {
                        if !rusty_backup::fs::prodos_types::is_known_type(tt) {
                            let aux = entry.aux_type.unwrap_or(0);
                            ui.colored_label(
                                egui::Color32::from_rgb(220, 200, 120),
                                format!(
                                    "Note: ${:02X} is not in the ProDOS type registry — {}. The type and aux (${:04X}) will be preserved on export via the CiderPress #{:02X}{:04X} filename suffix.",
                                    tt,
                                    rusty_backup::fs::prodos_types::UNKNOWN_TYPE_NOTE,
                                    aux,
                                    tt,
                                    aux,
                                ),
                            );
                        }
                    }
                }

                // Extract controls row
                let extraction_running = self.extraction_progress.is_some();
                let is_extractable = entry.is_file() || entry.is_directory() || entry.is_symlink();

                if is_extractable && !extraction_running {
                    ui.horizontal(|ui| {
                        // Resource fork mode dropdown (HFS/HFS+ only)
                        if self.is_hfs_type() {
                            ui.label("Resource forks:");
                            let current_label = self.resource_fork_mode.label();
                            egui::ComboBox::from_id_salt("rsrc_mode")
                                .selected_text(current_label)
                                .show_ui(ui, |ui| {
                                    for mode in &ResourceForkMode::ALL {
                                        ui.selectable_value(
                                            &mut self.resource_fork_mode,
                                            *mode,
                                            mode.label(),
                                        );
                                    }
                                });
                            ui.add_space(8.0);
                        }

                        // ProDOS export mode dropdown (ProDOS only)
                        if self.is_prodos_type() {
                            ui.label("Export name:");
                            let current_label = self.prodos_export_mode.label();
                            egui::ComboBox::from_id_salt("prodos_export_mode")
                                .selected_text(current_label)
                                .show_ui(ui, |ui| {
                                    for mode in &ProdosExportMode::ALL {
                                        ui.selectable_value(
                                            &mut self.prodos_export_mode,
                                            *mode,
                                            mode.label(),
                                        );
                                    }
                                });
                            ui.add_space(8.0);
                        }

                        let btn_label = if entry.is_directory() {
                            "Extract Folder..."
                        } else {
                            "Extract File..."
                        };
                        if ui.button(btn_label).clicked() {
                            self.start_extraction(&entry);
                        }
                    });
                }

                ui.separator();

                if entry.size > MAX_PREVIEW_SIZE as u64 && entry.is_file() {
                    ui.label(format!(
                        "File too large to preview ({}).",
                        partition::format_size(entry.size)
                    ));
                    return;
                }

                if entry.is_directory() {
                    return;
                }

                // Remaining height for the scroll area after the header
                let content_height = ui.available_height().min(panel_height);

                match &self.content {
                    None => {
                        if entry.is_file() {
                            ui.spinner();
                            ui.label("Loading...");
                        }
                    }
                    Some(FileContent::Text(text)) => {
                        egui::ScrollArea::vertical()
                            .id_salt("file_content")
                            .max_height(content_height)
                            .auto_shrink([false, false])
                            .show(ui, |ui| {
                                ui.add(
                                    egui::TextEdit::multiline(&mut text.as_str())
                                        .desired_width(f32::INFINITY)
                                        .font(egui::TextStyle::Monospace),
                                );
                            });
                    }
                    Some(FileContent::Binary(data)) => {
                        egui::ScrollArea::vertical()
                            .id_salt("file_content")
                            .max_height(content_height)
                            .auto_shrink([false, false])
                            .show(ui, |ui| {
                                render_hex_view(ui, data);
                            });
                    }
                }
            }
        }
    }

    /// Start extracting the selected entry to a user-chosen folder.
    fn start_extraction(&mut self, entry: &FileEntry) {
        // Pick destination folder
        let dest = match rfd::FileDialog::new()
            .set_title("Extract to folder")
            .pick_folder()
        {
            Some(d) => d,
            None => return,
        };

        let conflicts = self.detect_extract_conflicts(entry, &dest);
        if !conflicts.is_empty() {
            self.pending_extraction = Some(PendingExtraction {
                entry: entry.clone(),
                dest,
                conflicts,
            });
            return;
        }

        self.launch_extraction(entry.clone(), dest);
    }

    /// Collect existing paths under `dest` that the extraction would overwrite.
    /// Only top-level collisions are considered (the primary output plus any
    /// sidecar file for HFS resource-fork modes).
    fn detect_extract_conflicts(&self, entry: &FileEntry, dest: &std::path::Path) -> Vec<PathBuf> {
        let base = resource_fork::sanitize_filename(&entry.name);
        let safe_name = if self.is_prodos_type()
            && entry.is_file()
            && self.prodos_export_mode == ProdosExportMode::WithTypeSuffix
        {
            let tt = entry
                .type_code
                .as_deref()
                .and_then(|tc| {
                    tc.split_whitespace()
                        .next()
                        .and_then(|s| u8::from_str_radix(s.trim_start_matches('$'), 16).ok())
                })
                .unwrap_or(0x06);
            let aux = entry.aux_type.unwrap_or(0);
            format!(
                "{}{}",
                base,
                rusty_backup::fs::prodos_types::encode_cp_suffix(tt, aux)
            )
        } else {
            base
        };

        let mut candidates: Vec<PathBuf> = Vec::new();
        let has_rsrc = self.is_hfs_type()
            && entry.is_file()
            && entry.resource_fork_size.map(|s| s > 0).unwrap_or(false);

        if has_rsrc && self.resource_fork_mode == ResourceForkMode::MacBinary {
            candidates.push(dest.join(format!("{safe_name}.bin")));
        } else {
            candidates.push(dest.join(&safe_name));
            if has_rsrc {
                match self.resource_fork_mode {
                    ResourceForkMode::AppleDouble => {
                        candidates.push(dest.join(format!("._{safe_name}")));
                    }
                    ResourceForkMode::SeparateRsrc => {
                        candidates.push(dest.join(format!("{safe_name}.rsrc")));
                    }
                    _ => {}
                }
            }
        }

        candidates.into_iter().filter(|p| p.exists()).collect()
    }

    /// Spawn the extraction thread. Separated from `start_extraction` so the
    /// overwrite confirmation dialog can resume it after the user confirms.
    fn launch_extraction(&mut self, entry: FileEntry, dest: PathBuf) {
        let session = self.session.clone();
        let resource_fork_mode = self.resource_fork_mode;
        let is_hfs = self.is_hfs_type();
        let is_prodos = self.is_prodos_type();
        let prodos_export_mode = self.prodos_export_mode;

        let progress = Arc::new(Mutex::new(ExtractionProgress {
            current_bytes: 0,
            total_bytes: 0,
            current_file: String::new(),
            files_extracted: 0,
            total_files: 0,
            finished: false,
            error: None,
            cancel_requested: false,
        }));

        self.extraction_progress = Some(Arc::clone(&progress));
        self.extraction_result = None;

        std::thread::spawn(move || {
            let _wake = rusty_backup::os::wakelock::acquire("Rusty Backup: extract files");
            let result = run_extraction(
                &session,
                &entry,
                &dest,
                resource_fork_mode,
                is_hfs,
                is_prodos,
                prodos_export_mode,
                &progress,
            );

            if let Ok(mut p) = progress.lock() {
                p.finished = true;
                if let Err(e) = result {
                    p.error = Some(format!("{e}"));
                }
            }
        });
    }

    /// Get the parent entry for the currently selected entry, or root if nothing selected.
    fn current_parent_entry(&self) -> FileEntry {
        // If a directory is selected, use it as the parent
        if let Some(ref sel) = self.selected_entry {
            if sel.is_directory() {
                return sel.clone();
            }
            // If a file is selected, use the parent directory
            if let Some(parent_path) =
                sel.path
                    .rsplit_once('/')
                    .map(|(p, _)| if p.is_empty() { "/" } else { p })
            {
                // Find the parent entry from cache
                if parent_path == "/" {
                    if let Some(ref root) = self.root {
                        return root.clone();
                    }
                }
                // Search expanded directories for one matching parent_path
                for (path, entries) in &self.directory_cache {
                    for entry in entries {
                        if entry.path == parent_path && entry.is_directory() {
                            return entry.clone();
                        }
                    }
                    // Also check if the cache key itself matches
                    if path == parent_path {
                        // We need the entry for this path, find it
                        for siblings in self.directory_cache.values() {
                            for e in siblings {
                                if e.path == *path && e.is_directory() {
                                    return e.clone();
                                }
                            }
                        }
                    }
                }
            }
        }
        // Default to root
        self.root.clone().unwrap_or_else(FileEntry::root)
    }

    /// Start background extraction of an archive to a temp file for editing.
    fn start_archive_extract(&mut self) {
        let Some(ctx) = self.archive_edit_ctx.clone() else {
            return;
        };

        // Create temp file next to the archive
        let parent = ctx
            .archive_path
            .parent()
            .unwrap_or(std::path::Path::new("."));
        let temp_path = parent.join(format!(
            "{}-edit.img",
            ctx.archive_path
                .file_stem()
                .unwrap_or_default()
                .to_string_lossy()
        ));

        self.archive_edit_progress = Some(archive_edit::start_extract(&ctx, temp_path));
    }

    /// Start background recompression of the edited temp file back to the archive.
    fn start_archive_compress(&mut self) {
        let Some(ctx) = self.archive_edit_ctx.clone() else {
            return;
        };
        let Some(temp_path) = self.archive_temp_path.clone() else {
            return;
        };

        // Disable edit mode immediately
        self.edit_mode = false;
        self.edit_result = None;
        self.show_new_folder_dialog = false;
        self.pending_delete = None;

        // Switch browse source back to archive (read-only) — close the browser
        // while compressing to release the temp file.
        self.root = None;
        self.directory_cache.clear();
        self.selected_entry = None;
        self.content = None;

        self.archive_edit_progress = Some(archive_edit::start_compress(&ctx, temp_path));
    }

    /// Poll archive edit background operations for completion.
    fn poll_archive_edit(&mut self, ui: &egui::Ui) {
        let progress_arc = match &self.archive_edit_progress {
            Some(p) => Arc::clone(p),
            None => return,
        };

        let Ok(p) = progress_arc.lock() else {
            return;
        };

        if !p.finished {
            ui.ctx().request_repaint();
            return;
        }

        let phase = p.phase.clone();
        let error = p.error.clone();
        let temp_path = p.temp_path.clone();
        drop(p);

        self.archive_edit_progress = None;

        if let Some(err) = error {
            self.edit_result = Some(format!("Error: {err}"));
            return;
        }

        if phase == "Extracting" {
            // Extraction done — switch source to temp file, enable editing.
            // `partition_offset` is preserved: the temp file is byte-identical
            // to the archive's decompressed content, so a partition that lived
            // at offset N inside the original (e.g. an HFS volume inside a
            // whole-disk APM CHD) is at offset N in the temp too. Resetting
            // it to 0 here used to break edits of any non-single-partition
            // container.
            if let Some(temp) = temp_path {
                self.archive_temp_path = Some(temp.clone());
                self.session.source_path = Some(temp);
                self.session.zstd_cache = None;
                self.edit_mode = true;

                // Re-open filesystem from temp file. Invalidate the cache
                // first because the source bytes have changed.
                self.invalidate_cached_fs();
                match self.session.open() {
                    Ok(mut fs) => {
                        self.fs_type = fs.fs_type().to_string();
                        self.volume_label = fs.volume_label().unwrap_or("").to_string();
                        self.blessed_folder = fs.blessed_system_folder();
                        if let Ok(root) = fs.root() {
                            self.root = Some(root);
                        }
                        self.directory_cache.clear();
                        self.expanded_paths.clear();
                        self.selected_entry = None;
                        self.content = None;
                        self.return_fs(fs);
                    }
                    Err(e) => {
                        self.edit_result = Some(format!("Error opening temp file: {e}"));
                    }
                }
            }
        } else {
            // Compression done — re-open original archive for browsing.
            // Preserve `partition_offset` for the same reason as above: the
            // recompressed archive's logical bytes match the temp file, so
            // the partition is still at the original offset.
            if let Some(ctx) = &self.archive_edit_ctx {
                self.session.source_path = Some(ctx.archive_path.clone());
                self.archive_temp_path = None;
                self.invalidate_cached_fs();

                match self.session.open() {
                    Ok(mut fs) => {
                        self.fs_type = fs.fs_type().to_string();
                        self.volume_label = fs.volume_label().unwrap_or("").to_string();
                        self.blessed_folder = fs.blessed_system_folder();
                        if let Ok(root) = fs.root() {
                            self.root = Some(root);
                        }
                        self.directory_cache.clear();
                        self.expanded_paths.clear();
                        self.selected_entry = None;
                        self.content = None;
                        self.edit_result = Some("Changes saved successfully.".to_string());
                        self.return_fs(fs);
                    }
                    Err(e) => {
                        self.edit_result =
                            Some(format!("Saved but failed to re-open archive: {e}"));
                    }
                }
            }
        }
    }

    /// True if the current session is editing a CHD file (extension match —
    /// the chd_edit path covers both compressed and uncompressed CHDs).
    fn is_chd_source(&self) -> bool {
        self.session
            .source_path
            .as_ref()
            .and_then(|p| p.extension())
            .and_then(|e| e.to_str())
            .map(|e| e.eq_ignore_ascii_case("chd"))
            .unwrap_or(false)
    }

    /// Diff path for a compressed CHD edit session: sibling `<stem>.edit-diff.chd`.
    fn diff_path_for(parent: &Path) -> PathBuf {
        let parent_dir = parent.parent().unwrap_or(Path::new("."));
        let stem = parent
            .file_stem()
            .map(|s| s.to_os_string())
            .unwrap_or_default();
        let mut diff_name = stem;
        diff_name.push(".edit-diff.chd");
        parent_dir.join(diff_name)
    }

    /// Open a [`ChdEditSession`] for the current source CHD and install it
    /// on `self.session.chd_edit_session` so subsequent reads / writes flow
    /// through the diff (or in-place for uncompressed). Reloads the
    /// filesystem from the live session so the browser reflects the
    /// current state. On failure leaves edit_mode false and reports the
    /// error via `edit_result`.
    fn enter_chd_edit_mode(&mut self) {
        let Some(parent_path) = self.session.source_path.clone() else {
            self.edit_result = Some("Error: no source path".to_string());
            return;
        };

        // Backup copy first — preserves the original CHD as `<name>.chd_backup`
        // so the user has an easy revert point. No-op if the backup already
        // exists from a previous edit session.
        if let Err(e) = make_backup_copy(&parent_path) {
            self.edit_result = Some(format!("Error creating backup: {e}"));
            return;
        }

        let compressed = match is_compressed_chd(&parent_path) {
            Ok(c) => c,
            Err(e) => {
                self.edit_result = Some(format!("Error inspecting CHD: {e}"));
                return;
            }
        };

        let (session, diff_path) = if compressed {
            let diff = Self::diff_path_for(&parent_path);
            let res = if diff.exists() {
                ChdEditSession::reopen_with_diff(&parent_path, &diff)
            } else {
                ChdEditSession::open_with_diff(&parent_path, &diff)
            };
            match res {
                Ok(s) => (s, Some(diff)),
                Err(e) => {
                    self.edit_result = Some(format!("Error opening CHD diff: {e}"));
                    return;
                }
            }
        } else {
            match ChdEditSession::open_uncompressed(&parent_path) {
                Ok(s) => (s, None),
                Err(e) => {
                    self.edit_result = Some(format!("Error opening CHD: {e}"));
                    return;
                }
            }
        };

        let arc = Arc::new(Mutex::new(session));
        self.session.chd_edit_session = Some(Arc::clone(&arc));
        self.chd_edit = Some(ChdEditState {
            parent_path,
            diff_path,
        });
        self.edit_mode = true;

        // Reload the filesystem through the session so any pre-existing
        // diff content is visible.
        self.invalidate_all_caches();
        log::info!(
            "Entered CHD edit mode (compressed={compressed}, diff={})",
            self.chd_edit
                .as_ref()
                .and_then(|s| s.diff_path.as_ref())
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "<none>".into())
        );
    }

    /// Discard the live CHD edit session, deleting the diff (if any). The
    /// parent CHD is left untouched. Caller is responsible for any UI state
    /// transitions (clearing `edit_mode`, etc.).
    fn discard_chd_edit_session(&mut self) {
        self.session.chd_edit_session = None;
        if let Some(state) = self.chd_edit.take() {
            if let Some(diff) = state.diff_path {
                if diff.exists() {
                    let _ = std::fs::remove_file(&diff);
                }
            }
        }
    }

    /// Apply the current CHD edit session by flattening the diff back into
    /// the parent CHD. For uncompressed CHDs there's no diff — writes were
    /// already in place — so this just drops the session and reloads.
    /// Compressed CHDs spawn a background worker that calls
    /// `flatten_to_parent`; progress is polled by `poll_chd_flatten`.
    fn start_chd_flatten(&mut self) {
        let Some(state) = self.chd_edit.take() else {
            return;
        };

        // Drop the session so the diff handle is released before flatten
        // re-opens it via `reopen_with_diff`.
        self.session.chd_edit_session = None;

        let Some(diff_path) = state.diff_path else {
            // Uncompressed: writes already landed in the parent. Just reload.
            self.invalidate_all_caches();
            self.edit_result = Some("Changes saved successfully.".to_string());
            return;
        };

        let parent_path = state.parent_path;
        let total = std::fs::metadata(&diff_path).map(|m| m.len()).unwrap_or(0);

        let progress = Arc::new(Mutex::new(ChdFlattenProgress {
            current: 0,
            total,
            finished: false,
            error: None,
            cancel_requested: false,
        }));
        let progress_thread = Arc::clone(&progress);
        std::thread::spawn(move || {
            let _wake = rusty_backup::os::wakelock::acquire("Rusty Backup: CHD diff flatten");
            let cancel = {
                let p = Arc::clone(&progress_thread);
                move || p.lock().map(|g| g.cancel_requested).unwrap_or(false)
            };
            let result = chd_edit::flatten_to_parent(
                &parent_path,
                &diff_path,
                None,
                &mut |bytes| {
                    if let Ok(mut p) = progress_thread.lock() {
                        p.current = bytes;
                    }
                },
                &cancel,
                &mut |msg| log::info!("{msg}"),
            );
            if let Ok(mut p) = progress_thread.lock() {
                p.finished = true;
                if let Err(e) = result {
                    p.error = Some(format!("{e:#}"));
                }
            }
        });
        self.chd_flatten_progress = Some(progress);
    }

    /// Poll the background CHD flatten worker. On completion, reload the
    /// filesystem from the freshly-merged parent CHD.
    fn poll_chd_flatten(&mut self, ui: &egui::Ui) {
        let progress_arc = match &self.chd_flatten_progress {
            Some(p) => Arc::clone(p),
            None => return,
        };
        let Ok(p) = progress_arc.lock() else { return };
        if !p.finished {
            ui.ctx().request_repaint();
            return;
        }
        let error = p.error.clone();
        drop(p);
        self.chd_flatten_progress = None;

        if let Some(err) = error {
            self.edit_result = Some(format!("Error flattening CHD: {err}"));
            return;
        }
        // Re-open the now-replaced parent CHD for browsing.
        self.invalidate_all_caches();

        // If the CHD is the body of a single-file-chd backup, refresh
        // metadata.json so per-partition checksums + container SHA-1 stay
        // in sync with the new container contents. Best-effort: failure
        // surfaces in the edit result but doesn't roll back the save —
        // the bytes on disk are already correct.
        let mut result_msg = String::from("Changes saved successfully.");
        if let Some(folder) = self.single_file_chd_backup_folder.clone() {
            let mut log_lines: Vec<String> = Vec::new();
            let mut log_cb = |s: &str| log_lines.push(s.to_string());
            match rusty_backup::backup::single_file_chd::refresh_metadata_after_edit(
                &folder,
                &mut log_cb,
            ) {
                Ok(()) => {
                    result_msg.push_str(" Backup metadata.json refreshed.");
                }
                Err(e) => {
                    result_msg.push_str(&format!(
                        " (Warning: failed to refresh backup metadata.json: {e:#})"
                    ));
                }
            }
        }
        self.edit_result = Some(result_msg);
    }

    /// Render the edit mode toolbar with action buttons and free space.
    fn render_edit_toolbar(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            let parent = self.current_parent_entry();
            let parent_name = if parent.path == "/" {
                "root".to_string()
            } else {
                parent.name.clone()
            };
            ui.label(
                egui::RichText::new(format!("Editing: /{parent_name}"))
                    .color(egui::Color32::from_rgb(100, 180, 255)),
            );

            ui.add_space(8.0);

            if ui.button("Add File...").clicked() {
                self.add_file_dialog();
            }

            if ui.button("New Folder...").clicked() {
                self.new_folder_name.clear();
                self.show_new_folder_dialog = true;
            }

            // Delete button (enabled when something is selected)
            let has_selection = self.selected_entry.is_some()
                && self
                    .selected_entry
                    .as_ref()
                    .map(|e| e.path != "/")
                    .unwrap_or(false);
            if ui
                .add_enabled(has_selection, egui::Button::new("Delete"))
                .clicked()
            {
                if let Some(ref sel) = self.selected_entry.clone() {
                    // If it's a pending-add, just remove it from staged_edits.
                    // For pending directories, also drop every nested staged
                    // edit so we don't leave AddFile orphans whose parent.path
                    // no longer resolves at apply time.
                    if self.staged_edits.is_pending_add(&sel.path) {
                        let removed = if sel.is_directory() {
                            self.staged_edits.remove_pending_subtree(&sel.path)
                        } else {
                            self.staged_edits.remove_pending_add(&sel.path) as usize
                        };
                        self.edit_result = if sel.is_directory() && removed > 1 {
                            Some(format!(
                                "Unstaged '{}' and {} nested edit(s)",
                                sel.name,
                                removed - 1
                            ))
                        } else {
                            Some(format!("Unstaged '{}'", sel.name))
                        };
                        self.selected_entry = None;
                        self.content = None;
                    } else {
                        let parent = self.current_parent_entry();
                        let is_non_empty_dir = sel.is_directory()
                            && self
                                .directory_cache
                                .get(&sel.path)
                                .map(|c| !c.is_empty())
                                .unwrap_or(true); // assume non-empty if not cached
                        self.pending_delete = Some((parent, sel.clone(), is_non_empty_dir));
                    }
                }
            }

            // Set ProDOS Type… button (ProDOS only, file selected)
            // Bless Folder button (HFS/HFS+ only)
            if self.is_hfs_type() {
                let can_bless = has_selection
                    && self
                        .selected_entry
                        .as_ref()
                        .map(|e| e.is_directory())
                        .unwrap_or(false);
                if ui
                    .add_enabled(can_bless, egui::Button::new("Bless Folder"))
                    .clicked()
                {
                    if let Some(ref sel) = self.selected_entry.clone() {
                        self.staged_edits
                            .push(StagedEdit::BlessFolder { entry: sel.clone() });
                        self.edit_result = Some(format!("Staged bless folder '{}'", sel.name));
                    }
                }
            }

            ui.add_space(8.0);
            ui.separator();
            ui.add_space(8.0);

            // Apply button — only shown when edits are staged
            if !self.staged_edits.is_empty() {
                let label = format!("Apply Edits ({})", self.staged_edits.len());
                if ui.button(label).clicked() {
                    self.apply_staged_edits();
                    if self.archive_edit_ctx.is_some() && !self.has_edit_error() {
                        self.start_archive_compress();
                    }
                }
            }

            ui.add_space(8.0);

            // Free space indicator with projected space after staged edits
            if let Ok(mut efs) = self.session.open_editable() {
                if let Ok(free) = efs.free_space() {
                    ui.label(format!("Free: {}", partition::format_size(free)));

                    if !self.staged_edits.is_empty() {
                        let delta = self.staged_edits.space_delta();
                        let projected =
                            free.saturating_add(delta.freed).saturating_sub(delta.added);
                        let color = if delta.added > free + delta.freed {
                            egui::Color32::from_rgb(255, 100, 100) // red — won't fit
                        } else if projected < free / 10 {
                            egui::Color32::from_rgb(255, 200, 100) // yellow — tight
                        } else {
                            egui::Color32::from_rgb(100, 200, 100) // green — ok
                        };
                        ui.colored_label(
                            color,
                            format!("After: {}", partition::format_size(projected)),
                        );
                    }
                }
            }
        });
    }

    /// Show the "Add File" file picker dialog and add files.
    fn add_file_dialog(&mut self) {
        let files = rfd::FileDialog::new()
            .set_title("Select files to add")
            .pick_files();

        if let Some(paths) = files {
            self.add_host_paths(&paths);
        }
    }

    /// Stage files/folders from host paths for adding to the current directory.
    ///
    /// Individual failures (invalid filename, IO error, …) are collected into
    /// `staging_errors` and surfaced via the staging-errors modal dialog. The
    /// rest of the batch is still staged so the user does not lose successful
    /// items just because one file had a bad name.
    fn add_host_paths(&mut self, paths: &[PathBuf]) {
        let parent = self.current_parent_entry();
        let mut errors: Vec<(PathBuf, String)> = Vec::new();
        let mut staged_count = 0usize;

        for path in paths {
            if path.is_dir() {
                staged_count += self.stage_host_directory(path, &parent, &mut errors);
            } else if path.is_file() {
                staged_count += self.stage_host_file(path, &parent, &mut errors);
            }
        }

        if !errors.is_empty() {
            self.staging_errors = errors;
            self.show_staging_errors = true;
            self.edit_result = Some(format!(
                "Staged {staged_count} item(s); {} failed — see dialog",
                self.staging_errors.len()
            ));
        } else if staged_count > 0 {
            self.edit_result = Some(format!("Staged {staged_count} item(s)"));
        }
    }

    /// Try to stage a single host file, pushing any error into `errors`.
    /// Returns 1 on success, 0 on failure/skip.
    fn stage_host_file(
        &mut self,
        host_path: &std::path::Path,
        parent: &FileEntry,
        errors: &mut Vec<(PathBuf, String)>,
    ) -> usize {
        // Silently skip resource fork sidecars — they'll be consumed
        // with their primary file.
        if resource_fork::is_resource_fork_sidecar(host_path) {
            return 0;
        }
        match self.add_host_file(host_path, parent) {
            Ok(()) => 1,
            Err(e) => {
                errors.push((host_path.to_path_buf(), e));
                0
            }
        }
    }

    /// Try to stage a host directory (and its contents), continuing past
    /// individual child failures so one bad file does not abort the whole
    /// tree. Returns the number of items that were successfully staged.
    fn stage_host_directory(
        &mut self,
        host_path: &std::path::Path,
        parent: &FileEntry,
        errors: &mut Vec<(PathBuf, String)>,
    ) -> usize {
        let name = match host_path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|s| s.to_string())
        {
            Some(n) => n,
            None => {
                errors.push((host_path.to_path_buf(), "invalid directory name".into()));
                return 0;
            }
        };

        // Validate the directory name against the target filesystem's rules
        // before staging the CreateDirectory edit.
        if let Err(e) = self.validate_staged_name(&name) {
            errors.push((host_path.to_path_buf(), e));
            return 0;
        }

        // Duplicate-name check against pending adds in this parent.
        let pending = self.staged_edits.pending_adds_for(&parent.path);
        if pending.iter().any(|e| e.name == name) {
            errors.push((
                host_path.to_path_buf(),
                format!("'{name}' is already staged in this directory"),
            ));
            return 0;
        }

        self.staged_edits.push(StagedEdit::CreateDirectory {
            parent: parent.clone(),
            name: name.clone(),
        });

        // Build a synthetic FileEntry for children to reference as parent.
        let dir_path = if parent.path == "/" {
            format!("/{name}")
        } else {
            format!("{}/{name}", parent.path)
        };
        let new_dir = FileEntry::new_directory(name, dir_path, 0);

        let mut count = 1usize; // count the directory itself

        let entries = match std::fs::read_dir(host_path) {
            Ok(e) => e,
            Err(e) => {
                errors.push((host_path.to_path_buf(), e.to_string()));
                return count;
            }
        };
        for entry in entries {
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    errors.push((host_path.to_path_buf(), e.to_string()));
                    continue;
                }
            };
            let child_path = entry.path();
            if child_path.is_dir() {
                count += self.stage_host_directory(&child_path, &new_dir, errors);
            } else if child_path.is_file() {
                count += self.stage_host_file(&child_path, &new_dir, errors);
            }
        }

        count
    }

    /// Add a single host file to a parent directory on the image.
    fn add_host_file(
        &mut self,
        host_path: &std::path::Path,
        parent: &FileEntry,
    ) -> Result<(), String> {
        let raw_name = host_path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or("invalid filename")?
            .to_string();

        // ProDOS targets: strip any trailing CiderPress `#TTAAAA` suffix from
        // the host filename and use the decoded (type, aux) as staged
        // overrides so the round-trip `drag-out → drag-back-in` is lossless.
        let (name, prodos_type, prodos_aux) = if self.is_prodos_type() {
            match rusty_backup::fs::prodos_types::decode_cp_suffix(&raw_name) {
                Some((stem, tt, aa)) => (stem.to_string(), Some(tt), Some(aa)),
                None => (raw_name, None, None),
            }
        } else {
            (raw_name, None, None)
        };

        // Detect resource fork from host (native, AppleDouble, MacBinary, .rsrc)
        let rsrc_import = if self.is_hfs_type() {
            resource_fork::detect_resource_fork(host_path)
        } else {
            None
        };

        // For MacBinary imports the "file" is a container — use the data fork
        // size from inside the container, not the container's file size.
        let (effective_path, size) = if let Some(ref imp) = rsrc_import {
            if let Some(ref data_fork) = imp.data_fork {
                // MacBinary: we'll write the extracted data fork, not the .bin file
                (host_path.to_path_buf(), data_fork.len() as u64)
            } else {
                let sz = std::fs::metadata(host_path)
                    .map_err(|e| e.to_string())?
                    .len();
                (host_path.to_path_buf(), sz)
            }
        } else {
            let sz = std::fs::metadata(host_path)
                .map_err(|e| e.to_string())?
                .len();
            (host_path.to_path_buf(), sz)
        };

        // Validate the name against the target filesystem's rules before
        // staging so that clearly-bad names are rejected up-front.
        self.validate_staged_name(&name)
            .map_err(|e| format!("'{name}': {e}"))?;

        // Check for duplicate name in pending adds for this parent
        let pending = self.staged_edits.pending_adds_for(&parent.path);
        if pending.iter().any(|e| e.name == name) {
            return Err(format!("'{name}' is already staged in this directory"));
        }

        self.staged_edits.push(StagedEdit::AddFile {
            parent: parent.clone(),
            name,
            host_path: effective_path,
            size,
            prodos_type,
            prodos_aux,
            resource_fork: rsrc_import,
            hfs_type_override: None,
            hfs_creator_override: None,
        });
        Ok(())
    }

    /// Stage a delete operation.
    fn perform_delete(&mut self, parent: &FileEntry, entry: &FileEntry, recursive: bool) {
        let entry_name = entry.name.clone();

        if recursive {
            self.staged_edits.push(StagedEdit::DeleteRecursive {
                parent: parent.clone(),
                entry: entry.clone(),
            });
        } else {
            self.staged_edits.push(StagedEdit::DeleteEntry {
                parent: parent.clone(),
                entry: entry.clone(),
            });
        }

        self.edit_result = Some(format!("Staged delete of '{entry_name}'"));
    }

    /// Apply all staged edits to the filesystem in a single batch.
    fn apply_staged_edits(&mut self) {
        log::info!(
            "Applying {} staged edit(s) to {}",
            self.staged_edits.len(),
            self.session
                .source_path
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_default()
        );
        let mut efs = match self.session.open_editable() {
            Ok(fs) => fs,
            Err(e) => {
                log::error!("Failed to open editable filesystem: {e}");
                self.edit_result = Some(format!("Error opening filesystem: {e}"));
                return;
            }
        };

        let edits: Vec<StagedEdit> = self.staged_edits.drain().collect();
        let total = edits.len();

        for (i, edit) in edits.iter().enumerate() {
            if let Err(e) = edit_queue::apply_edit(&mut *efs, edit) {
                self.edit_result = Some(format!("Error on edit {}/{total}: {e}", i + 1));
                return;
            }
        }

        if let Err(e) = efs.sync_metadata() {
            self.edit_result = Some(format!("Error saving to disk: {e}"));
            return;
        }

        self.edit_result = Some(format!("Applied {total} edit(s) successfully"));
        self.invalidate_all_caches();

        // Update blessed folder info after apply. The volume bytes were
        // mutated by the apply pass, so any cached read-only fs is stale —
        // invalidate before re-opening.
        self.invalidate_cached_fs();
        if let Some(mut fs) = self.take_or_open_fs() {
            self.blessed_folder = fs.blessed_system_folder();
            self.return_fs(fs);
        }
    }

    fn has_edit_error(&self) -> bool {
        self.edit_result
            .as_ref()
            .map(|r| r.starts_with("Error"))
            .unwrap_or(false)
    }

    /// Invalidate all cached directory listings and reload root.
    ///
    /// Called after edit-apply / sync_metadata writes. The cached read-only
    /// filesystem has stale in-memory tables (catalog, bitmap), so drop it
    /// and re-open against the freshly written disk bytes.
    fn invalidate_all_caches(&mut self) {
        self.directory_cache.clear();
        self.selected_entry = None;
        self.content = None;
        self.invalidate_cached_fs();
        if let Some(mut fs) = self.take_or_open_fs() {
            if let Ok(root) = fs.root() {
                if let Ok(children) = fs.list_directory(&root) {
                    self.directory_cache.insert(root.path.clone(), children);
                }
                self.root = Some(root);
            }
            self.return_fs(fs);
        }
    }

    /// Render the HFS/HFS+ type/creator row beneath the file-info header.
    /// Read-only when not in edit mode; full editor (text + pulldown + Set
    /// button) when edit mode is on. For directories, no-op.
    fn render_hfs_type_row(&mut self, ui: &mut egui::Ui, entry: &FileEntry) {
        let (cur_t, cur_c) = self.staged_edits.resolved_hfs_type_creator(entry);
        let cur_t_str = String::from_utf8_lossy(&cur_t).to_string();
        let cur_c_str = String::from_utf8_lossy(&cur_c).to_string();
        let cur_desc = rusty_backup::fs::hfs_common::describe_type_creator(&cur_t, &cur_c);

        if !self.edit_mode {
            ui.horizontal(|ui| {
                if cur_t != [0; 4] || cur_c != [0; 4] {
                    let label = match cur_desc {
                        Some(d) => format!("Type: {cur_t_str}  Creator: {cur_c_str}  ({d})"),
                        None => format!("Type: {cur_t_str}  Creator: {cur_c_str}"),
                    };
                    ui.label(label);
                } else {
                    ui.colored_label(
                        egui::Color32::from_rgb(120, 160, 220),
                        "Type: (none)  Creator: (none)",
                    );
                }
            });
            return;
        }

        // Edit mode — seed editor state if it's a different entry.
        let needs_seed = self
            .hfs_type_editor
            .as_ref()
            .map(|s| s.entry_path != entry.path)
            .unwrap_or(true);
        if needs_seed {
            self.hfs_type_editor = Some(HfsTypeEditorState {
                entry_path: entry.path.clone(),
                type_input: cur_t_str.clone(),
                creator_input: cur_c_str.clone(),
            });
        }

        let mut stage_action: Option<([u8; 4], [u8; 4])> = None;
        let mut reset_action = false;

        // Borrow editor mutably inside one ui.horizontal closure.
        if let Some(state) = self.hfs_type_editor.as_mut() {
            ui.horizontal(|ui| {
                ui.label("Type:");
                ui.add(
                    egui::TextEdit::singleline(&mut state.type_input)
                        .desired_width(48.0)
                        .char_limit(4)
                        .font(egui::TextStyle::Monospace),
                );
                ui.label("Creator:");
                ui.add(
                    egui::TextEdit::singleline(&mut state.creator_input)
                        .desired_width(48.0)
                        .char_limit(4)
                        .font(egui::TextStyle::Monospace),
                );

                // Pulldown with current FInfo first, then dictionary by name.
                let combo_label = match cur_desc {
                    Some(d) => format!("(current) {cur_t_str}/{cur_c_str} — {d}"),
                    None if cur_t != [0; 4] || cur_c != [0; 4] => {
                        format!("(current) {cur_t_str}/{cur_c_str}")
                    }
                    None => "Pick from dictionary…".to_string(),
                };
                let mut picked: Option<([u8; 4], [u8; 4])> = None;
                egui::ComboBox::from_id_salt(("hfs_tc_dict", &entry.path))
                    .selected_text(combo_label)
                    .width(280.0)
                    .show_ui(ui, |ui| {
                        if cur_t != [0; 4] || cur_c != [0; 4] {
                            let label = match cur_desc {
                                Some(d) => {
                                    format!("(current) {cur_t_str}/{cur_c_str} — {d}")
                                }
                                None => format!("(current) {cur_t_str}/{cur_c_str}"),
                            };
                            if ui.selectable_label(false, label).clicked() {
                                picked = Some((cur_t, cur_c));
                            }
                            ui.separator();
                        }
                        for e in rusty_backup::fs::hfs_common::known_type_creators() {
                            let label =
                                format!("{}/{} — {}", e.type_str(), e.creator_str(), e.description);
                            if ui.selectable_label(false, label).clicked() {
                                picked = Some((e.type_code, e.creator_code));
                            }
                        }
                    });
                if let Some((t, c)) = picked {
                    state.type_input = String::from_utf8_lossy(&t).to_string();
                    state.creator_input = String::from_utf8_lossy(&c).to_string();
                }

                // Encode current text into 4-byte arrays (space-padded).
                let typed_t = rusty_backup::fs::hfs_common::encode_fourcc(&state.type_input);
                let typed_c = rusty_backup::fs::hfs_common::encode_fourcc(&state.creator_input);
                let changed = typed_t != cur_t || typed_c != cur_c;

                if ui
                    .add_enabled(changed, egui::Button::new("Set"))
                    .on_hover_text("Stage type/creator change")
                    .clicked()
                {
                    stage_action = Some((typed_t, typed_c));
                }
                if ui
                    .button("Reset")
                    .on_hover_text("Revert editor to current values")
                    .clicked()
                {
                    reset_action = true;
                }
            });
        }

        if reset_action {
            if let Some(state) = self.hfs_type_editor.as_mut() {
                state.type_input = cur_t_str;
                state.creator_input = cur_c_str;
            }
        }

        if let Some((t, c)) = stage_action {
            if self
                .staged_edits
                .set_pending_hfs_override(&entry.path, t, c)
            {
                self.edit_result = Some(format!(
                    "Updated pending '{}' to {}/{}",
                    entry.name,
                    String::from_utf8_lossy(&t),
                    String::from_utf8_lossy(&c),
                ));
            } else {
                self.staged_edits.replace_set_type_creator(entry, t, c);
                self.edit_result = Some(format!(
                    "Staged type/creator {}/{} on '{}'",
                    String::from_utf8_lossy(&t),
                    String::from_utf8_lossy(&c),
                    entry.name,
                ));
            }
        }
    }

    /// Resolve the effective ProDOS (type_byte, aux_type) for an entry,
    /// considering any pending `AddFile` overrides. Returns `(0x00, 0x0000)`
    /// when nothing is known.
    fn resolved_prodos_type(&self, entry: &FileEntry) -> (u8, u16) {
        // 1) Pending AddFile override
        for edit in self.staged_edits.iter() {
            if let StagedEdit::AddFile {
                parent,
                name,
                prodos_type,
                prodos_aux,
                ..
            } = edit
            {
                let path = if parent.path == "/" {
                    format!("/{name}")
                } else {
                    format!("{}/{name}", parent.path)
                };
                if path != entry.path {
                    continue;
                }
                let t = prodos_type.unwrap_or(0);
                let a = prodos_aux.unwrap_or(0);
                return (t, a);
            }
        }
        // 2) On-disk catalog values: parse "$XX ABC" out of entry.type_code
        let t = entry
            .type_code
            .as_deref()
            .and_then(|tc| {
                tc.split_whitespace()
                    .next()
                    .and_then(|s| u8::from_str_radix(s.trim_start_matches('$'), 16).ok())
            })
            .unwrap_or(0);
        let a = entry.aux_type.unwrap_or(0);
        (t, a)
    }

    /// Render the ProDOS type/aux row beneath the file-info header. Read-only
    /// when not in edit mode; full editor (type pulldown + 2-char/4-char hex
    /// inputs + Set/Reset) when edit mode is on.
    fn render_prodos_type_row(&mut self, ui: &mut egui::Ui, entry: &FileEntry) {
        use rusty_backup::fs::prodos_types as pt;

        let (cur_t, cur_a) = self.resolved_prodos_type(entry);
        let cur_known = pt::is_known_type(cur_t);
        let cur_abbr = pt::type_abbr(cur_t);
        let cur_desc = pt::type_description(cur_t);

        if !self.edit_mode {
            ui.horizontal(|ui| {
                let label =
                    format!("Type: ${cur_t:02X} {cur_abbr}  Aux: ${cur_a:04X}  ({cur_desc})");
                if cur_known {
                    ui.label(label);
                } else {
                    ui.colored_label(egui::Color32::from_rgb(120, 160, 220), label);
                }
            });
            return;
        }

        // Edit mode — seed editor state if it's a different entry.
        let needs_seed = self
            .prodos_type_editor
            .as_ref()
            .map(|s| s.entry_path != entry.path)
            .unwrap_or(true);
        if needs_seed {
            self.prodos_type_editor = Some(ProdosTypeEditorState {
                entry_path: entry.path.clone(),
                type_input: format!("{cur_t:02X}"),
                aux_input: format!("{cur_a:04X}"),
            });
        }

        let mut stage_action: Option<(u8, u16)> = None;
        let mut reset_action = false;

        if let Some(state) = self.prodos_type_editor.as_mut() {
            ui.horizontal(|ui| {
                ui.label("Type pulldown:");
                let combo_label = format!("${cur_t:02X} {cur_abbr} — {cur_desc}");
                let mut picked: Option<u8> = None;
                egui::ComboBox::from_id_salt(("prodos_type_combo", &entry.path))
                    .selected_text(combo_label)
                    .width(320.0)
                    .show_ui(ui, |ui| {
                        for (byte, info) in &pt::all_types() {
                            let label =
                                format!("${:02X} {} — {}", byte, info.abbr, info.description);
                            if ui.selectable_label(*byte == cur_t, label).clicked() {
                                picked = Some(*byte);
                            }
                        }
                    });
                if let Some(b) = picked {
                    state.type_input = format!("{b:02X}");
                }
            });

            ui.horizontal(|ui| {
                ui.label("Type ($):");
                ui.add(
                    egui::TextEdit::singleline(&mut state.type_input)
                        .desired_width(40.0)
                        .char_limit(2)
                        .font(egui::TextStyle::Monospace),
                );
                ui.label("Aux ($):");
                ui.add(
                    egui::TextEdit::singleline(&mut state.aux_input)
                        .desired_width(60.0)
                        .char_limit(4)
                        .font(egui::TextStyle::Monospace),
                );

                let parsed_t = u8::from_str_radix(state.type_input.trim(), 16).ok();
                let parsed_a = u16::from_str_radix(state.aux_input.trim(), 16).ok();
                let valid = parsed_t.is_some() && parsed_a.is_some();
                let changed = match (parsed_t, parsed_a) {
                    (Some(t), Some(a)) => t != cur_t || a != cur_a,
                    _ => false,
                };

                if ui
                    .add_enabled(valid && changed, egui::Button::new("Set"))
                    .on_hover_text("Stage type/aux change")
                    .clicked()
                {
                    if let (Some(t), Some(a)) = (parsed_t, parsed_a) {
                        stage_action = Some((t, a));
                    }
                }
                if ui
                    .button("Reset")
                    .on_hover_text("Revert editor to current values")
                    .clicked()
                {
                    reset_action = true;
                }
                if !valid {
                    ui.colored_label(
                        egui::Color32::from_rgb(255, 120, 120),
                        "Type must be 2 hex digits, Aux must be 4",
                    );
                }
            });
        }

        if reset_action {
            if let Some(state) = self.prodos_type_editor.as_mut() {
                state.type_input = format!("{cur_t:02X}");
                state.aux_input = format!("{cur_a:04X}");
            }
        }

        if let Some((t, a)) = stage_action {
            if self
                .staged_edits
                .set_pending_prodos_override(&entry.path, t, a)
            {
                self.edit_result = Some(format!(
                    "Updated pending '{}' to ${t:02X}/${a:04X}",
                    entry.name,
                ));
            } else {
                self.staged_edits.replace_set_prodos_type(entry, t, a);
                self.edit_result =
                    Some(format!("Staged type ${t:02X}/${a:04X} on '{}'", entry.name,));
            }
        }
    }

    /// Render the delete confirmation dialog.
    fn render_delete_dialog(&mut self, ui: &mut egui::Ui) {
        if let Some((parent, entry, is_non_empty)) = self.pending_delete.clone() {
            let title = if is_non_empty {
                format!("Delete '{}' and all its contents?", entry.name)
            } else {
                format!("Delete '{}'?", entry.name)
            };
            ui.horizontal(|ui| {
                ui.colored_label(egui::Color32::from_rgb(255, 200, 100), &title);
                if ui.button("Yes, delete").clicked() {
                    self.pending_delete = None;
                    self.perform_delete(&parent, &entry, is_non_empty);
                }
                if ui.button("Cancel").clicked() {
                    self.pending_delete = None;
                }
            });
        }
    }

    /// Render the unsaved changes confirmation dialog.
    fn render_unsaved_dialog(&mut self, ui: &mut egui::Ui) {
        if !self.show_unsaved_dialog {
            return;
        }

        let count = self.staged_edits.len();

        egui::Window::new("Unsaved Changes")
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .show(ui.ctx(), |ui| {
                ui.label(format!("You have {count} unsaved edit(s)."));
                ui.add_space(4.0);

                // Summary
                let mut files_added = 0usize;
                let mut dirs_added = 0usize;
                let mut entries_deleted = 0usize;
                let mut bytes_added: u64 = 0;
                for edit in self.staged_edits.iter() {
                    match edit {
                        StagedEdit::AddFile { size, .. } => {
                            files_added += 1;
                            bytes_added += size;
                        }
                        StagedEdit::CreateDirectory { .. } => dirs_added += 1,
                        StagedEdit::DeleteEntry { .. } | StagedEdit::DeleteRecursive { .. } => {
                            entries_deleted += 1
                        }
                        _ => {}
                    }
                }
                let mut parts = Vec::new();
                if files_added > 0 {
                    parts.push(format!("{files_added} file(s) added"));
                }
                if dirs_added > 0 {
                    parts.push(format!("{dirs_added} folder(s) added"));
                }
                if entries_deleted > 0 {
                    parts.push(format!("{entries_deleted} item(s) deleted"));
                }
                if !parts.is_empty() {
                    ui.label(parts.join(", "));
                }
                if bytes_added > 0 {
                    ui.label(format!(
                        "Net data: +{}",
                        partition::format_size(bytes_added)
                    ));
                }

                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    if ui.button("Discard Edits").clicked() {
                        self.staged_edits.clear();
                        self.show_unsaved_dialog = false;
                        // For CHD edit sessions, "Discard" must also drop
                        // the diff so prior Apply Edits aren't flattened
                        // into the parent. Without this, a discard followed
                        // by exit_edit_mode would still flatten any work
                        // that was previously written to the diff.
                        if self.chd_edit.is_some() {
                            self.discard_chd_edit_session();
                        }
                        if self.pending_close {
                            self.close();
                        } else {
                            self.exit_edit_mode();
                        }
                    }
                    if ui.button("Apply Edits").clicked() {
                        self.show_unsaved_dialog = false;
                        self.apply_staged_edits();
                        let ok = self
                            .edit_result
                            .as_ref()
                            .map(|r| !r.starts_with("Error"))
                            .unwrap_or(true);
                        if ok {
                            if self.pending_close {
                                self.close();
                            } else {
                                self.exit_edit_mode();
                            }
                        }
                    }
                    if ui.button("Cancel").clicked() {
                        self.show_unsaved_dialog = false;
                        self.pending_close = false;
                    }
                });
            });
    }

    /// Render the overwrite confirmation dialog for extraction.
    fn render_extract_overwrite_dialog(&mut self, ui: &mut egui::Ui) {
        if self.pending_extraction.is_none() {
            return;
        }
        let (conflict_list, count) = {
            let pe = self.pending_extraction.as_ref().unwrap();
            let list: Vec<String> = pe
                .conflicts
                .iter()
                .map(|p| {
                    p.file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or("?")
                        .to_string()
                })
                .collect();
            (list, pe.conflicts.len())
        };

        let mut action: Option<bool> = None; // Some(true)=overwrite, Some(false)=cancel
        egui::Window::new("Overwrite existing file?")
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .show(ui.ctx(), |ui| {
                if count == 1 {
                    ui.label(format!(
                        "'{}' already exists at the destination.",
                        conflict_list[0]
                    ));
                } else {
                    ui.label(format!("{count} items already exist at the destination:"));
                    for name in &conflict_list {
                        ui.label(format!("  - {name}"));
                    }
                }
                ui.add_space(4.0);
                ui.label("Overwriting will replace the existing items.");
                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    if ui.button("Overwrite").clicked() {
                        action = Some(true);
                    }
                    if ui.button("Cancel").clicked() {
                        action = Some(false);
                    }
                });
            });

        match action {
            Some(true) => {
                let pe = self.pending_extraction.take().unwrap();
                for path in &pe.conflicts {
                    let res = if path.is_dir() {
                        std::fs::remove_dir_all(path)
                    } else {
                        std::fs::remove_file(path)
                    };
                    if let Err(e) = res {
                        self.extraction_result =
                            Some(format!("Could not remove '{}': {e}", path.display()));
                        return;
                    }
                }
                self.launch_extraction(pe.entry, pe.dest);
            }
            Some(false) => {
                self.pending_extraction = None;
            }
            None => {}
        }
    }

    /// Exit edit mode, clearing all staged state.
    fn exit_edit_mode(&mut self) {
        self.edit_mode = false;
        self.edit_result = None;
        self.show_new_folder_dialog = false;
        self.pending_delete = None;
        self.staged_edits.clear();

        if self.chd_edit.is_some() {
            self.start_chd_flatten();
        } else if self.archive_edit_ctx.is_some() {
            self.start_archive_compress();
        }
    }

    /// Render the new folder name dialog.
    fn render_new_folder_dialog(&mut self, ui: &mut egui::Ui) {
        if !self.show_new_folder_dialog {
            return;
        }

        ui.horizontal(|ui| {
            ui.label("Folder name:");
            let response = ui.text_edit_singleline(&mut self.new_folder_name);
            if response.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)) {
                self.create_new_folder();
                return;
            }
            if ui.button("Create").clicked() {
                self.create_new_folder();
                return;
            }
            if ui.button("Cancel").clicked() {
                self.show_new_folder_dialog = false;
            }
        });
    }

    /// Render a modal listing files that failed to stage, with the reason
    /// for each failure. The list is built by `add_host_paths` and persists
    /// until the user dismisses the dialog.
    fn render_staging_errors_dialog(&mut self, ui: &mut egui::Ui) {
        if !self.show_staging_errors {
            return;
        }
        let mut open = true;
        let mut dismiss = false;
        egui::Window::new("Could not add some files")
            .open(&mut open)
            .resizable(true)
            .collapsible(false)
            .default_width(520.0)
            .default_height(320.0)
            .show(ui.ctx(), |ui| {
                ui.colored_label(
                    egui::Color32::from_rgb(255, 150, 100),
                    format!(
                        "{} item(s) could not be staged for the [{}] filesystem:",
                        self.staging_errors.len(),
                        self.fs_type
                    ),
                );
                ui.add_space(4.0);
                ui.separator();
                egui::ScrollArea::vertical()
                    .auto_shrink([false, false])
                    .max_height(220.0)
                    .show(ui, |ui| {
                        for (path, reason) in &self.staging_errors {
                            let label = path
                                .file_name()
                                .and_then(|n| n.to_str())
                                .unwrap_or_else(|| path.to_str().unwrap_or("?"));
                            ui.horizontal_wrapped(|ui| {
                                ui.label(egui::RichText::new(label).strong());
                                ui.label("—");
                                ui.colored_label(egui::Color32::from_rgb(255, 120, 120), reason);
                            });
                            ui.label(
                                egui::RichText::new(path.display().to_string())
                                    .small()
                                    .color(egui::Color32::from_gray(150)),
                            );
                            ui.add_space(4.0);
                        }
                    });
                ui.separator();
                ui.horizontal(|ui| {
                    if ui.button("OK").clicked() {
                        dismiss = true;
                    }
                });
            });
        if !open || dismiss {
            self.show_staging_errors = false;
            self.staging_errors.clear();
        }
    }

    /// Render the filesystem check results popup.
    fn render_chd_info_popup(&mut self, ui: &mut egui::Ui) {
        let Some(text) = self.chd_info_text.clone() else {
            return;
        };
        let mut open = true;
        let mut buf = text;
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
                                .desired_rows(20),
                        );
                    });
            });
        if !open {
            self.chd_info_text = None;
        }
    }

    fn render_fsck_popup(&mut self, ui: &mut egui::Ui) {
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
                        result.stats.files_checked,
                        result.stats.directories_checked,
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
                            .id_salt("fsck_errors")
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

                    // Orphaned entries (files/dirs with missing parents)
                    if !result.orphaned_entries.is_empty() {
                        ui.separator();
                        // Group by missing parent CNID
                        let mut by_parent: std::collections::BTreeMap<u64, Vec<&rusty_backup::fs::OrphanedEntry>> =
                            std::collections::BTreeMap::new();
                        for entry in &result.orphaned_entries {
                            by_parent
                                .entry(entry.missing_parent_id)
                                .or_default()
                                .push(entry);
                        }
                        ui.colored_label(
                            egui::Color32::from_rgb(255, 100, 100),
                            format!(
                                "{} file(s)/folder(s) reference {} missing parent director{} — not repairable.",
                                result.orphaned_entries.len(),
                                by_parent.len(),
                                if by_parent.len() == 1 { "y" } else { "ies" },
                            ),
                        );
                        ui.label("These entries exist in the catalog but their parent directory is gone. \
                                  This typically indicates severe directory corruption.");
                        egui::ScrollArea::vertical()
                            .id_salt("fsck_orphans")
                            .max_height(200.0)
                            .show(ui, |ui| {
                                for (parent_cnid, entries) in &by_parent {
                                    ui.colored_label(
                                        egui::Color32::from_rgb(255, 200, 100),
                                        format!("Missing parent ID {}:", parent_cnid),
                                    );
                                    for entry in entries {
                                        let kind = if entry.is_directory { "dir" } else { "file" };
                                        ui.label(format!(
                                            "    {} \"{}\" (ID {})",
                                            kind, entry.name, entry.id
                                        ));
                                    }
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
                            .id_salt("fsck_warnings")
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
                        ui.checkbox(
                            &mut self.show_fsck_debug,
                            "Show debug messages",
                        );
                    }

                    // Repair button — only for repairable errors on non-archive sources
                    if result.repairable && self.archive_edit_ctx.is_none() {
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
                        if report.unrepairable_count > 0 {
                            ui.colored_label(
                                egui::Color32::from_rgb(255, 200, 100),
                                format!(
                                    "{} error(s) could not be repaired (missing parent directories).",
                                    report.unrepairable_count
                                ),
                            );
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
        self.render_repair_confirm(ui);
    }

    /// Render the repair confirmation dialog and execute repair if confirmed.
    fn render_repair_confirm(&mut self, ui: &mut egui::Ui) {
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
            self.run_repair();
        }
    }

    /// Execute repair on the filesystem and re-run check.
    fn run_repair(&mut self) {
        match self.session.open_editable() {
            Ok(mut efs) => match efs.repair() {
                Ok(report) => {
                    self.repair_report = Some(report);
                    // Re-run fsck to show updated state. Repair wrote to
                    // disk, so any cached read-only fs is stale.
                    drop(efs);
                    self.invalidate_cached_fs();
                    match self.take_or_open_fs() {
                        Some(mut fs) => {
                            if let Some(Ok(result)) = fs.fsck() {
                                self.fsck_result = Some(result);
                            }
                            self.return_fs(fs);
                        }
                        None => {
                            self.error = Some("Failed to re-check after repair".into());
                        }
                    }
                    // Invalidate directory cache
                    self.directory_cache.clear();
                }
                Err(e) => {
                    self.error = Some(format!("Repair failed: {}", e));
                }
            },
            Err(e) => {
                self.error = Some(format!("Failed to open filesystem for repair: {}", e));
            }
        }
    }

    fn generate_tree_text(&mut self) {
        if self.pending_tree.is_some() {
            return;
        }
        let Some(fs) = self.take_or_open_fs() else {
            self.error = Some("Failed to open filesystem".into());
            return;
        };
        let show_ids = self.tree_show_ids;
        let status = Arc::new(Mutex::new(TreeStatus {
            finished: false,
            text: None,
            error: None,
            fs: None,
        }));
        let status_thread = Arc::clone(&status);
        std::thread::spawn(move || {
            let mut fs = fs;
            let result = if show_ids {
                rusty_backup::fs::tree::format_tree_with_ids(&mut *fs)
            } else {
                rusty_backup::fs::tree::format_tree(&mut *fs)
            };
            if let Ok(mut g) = status_thread.lock() {
                match result {
                    Ok(text) => g.text = Some(text),
                    Err(e) => g.error = Some(format!("Failed to generate tree: {e}")),
                }
                g.fs = Some(fs);
                g.finished = true;
            }
        });
        self.pending_tree = Some(status);
    }

    fn poll_pending_tree(&mut self, ui: &mut egui::Ui) {
        let mut take_now = false;
        if let Some(arc) = &self.pending_tree {
            if let Ok(g) = arc.lock() {
                if g.finished {
                    take_now = true;
                } else {
                    ui.horizontal(|ui| {
                        ui.add(egui::Spinner::new());
                        ui.label("Generating tree view...");
                    });
                    ui.ctx().request_repaint();
                }
            }
        }
        if take_now {
            if let Some(arc) = self.pending_tree.take() {
                if let Ok(mut g) = arc.lock() {
                    if let Some(fs) = g.fs.take() {
                        self.cached_fs = Some(fs);
                    }
                    if let Some(text) = g.text.take() {
                        self.tree_text = Some(text);
                        self.show_tree_popup = true;
                    }
                    if let Some(err) = g.error.take() {
                        self.error = Some(err);
                    }
                }
            }
        }
    }

    fn render_tree_popup(&mut self, ui: &mut egui::Ui) {
        if !self.show_tree_popup {
            return;
        }
        let mut open = true;
        let mut save_requested = false;
        let mut regenerate = false;
        egui::Window::new("Tree View")
            .open(&mut open)
            .resizable(true)
            .default_width(600.0)
            .default_height(500.0)
            .show(ui.ctx(), |ui| {
                ui.horizontal(|ui| {
                    if ui.button("Copy to Clipboard").clicked() {
                        if let Some(text) = &self.tree_text {
                            ui.ctx().copy_text(text.clone());
                        }
                    }
                    if ui.button("Save to File...").clicked() {
                        save_requested = true;
                    }
                    ui.add_space(16.0);
                    if ui.checkbox(&mut self.tree_show_ids, "Show IDs").changed() {
                        regenerate = true;
                    }
                });
                ui.separator();
                if let Some(text) = &self.tree_text {
                    egui::ScrollArea::both()
                        .auto_shrink([false, false])
                        .show(ui, |ui| {
                            ui.add(
                                egui::TextEdit::multiline(&mut text.as_str())
                                    .font(egui::TextStyle::Monospace)
                                    .desired_width(f32::INFINITY),
                            );
                        });
                }
            });
        if !open {
            self.show_tree_popup = false;
        }
        if regenerate {
            self.generate_tree_text();
        }
        if save_requested {
            if let Some(text) = &self.tree_text {
                let text = text.clone();
                if let Some(path) = rfd::FileDialog::new()
                    .set_title("Save tree view")
                    .set_file_name("tree.txt")
                    .add_filter("Text files", &["txt"])
                    .save_file()
                {
                    if let Err(e) = std::fs::write(&path, &text) {
                        self.error = Some(format!("Failed to save tree: {}", e));
                    }
                }
            }
        }
    }

    /// Create a new folder with the name from the dialog.
    fn create_new_folder(&mut self) {
        let name = self.new_folder_name.trim().to_string();
        self.show_new_folder_dialog = false;

        if name.is_empty() {
            self.edit_result = Some("Error: folder name cannot be empty".into());
            return;
        }

        if let Err(e) = self.validate_staged_name(&name) {
            self.edit_result = Some(format!("Error: {e}"));
            return;
        }

        let parent = self.current_parent_entry();

        // Check for duplicate name in pending adds
        let pending = self.staged_edits.pending_adds_for(&parent.path);
        if pending.iter().any(|e| e.name == name) {
            self.edit_result = Some(format!(
                "Error: '{name}' is already staged in this directory"
            ));
            return;
        }

        self.staged_edits.push(StagedEdit::CreateDirectory {
            parent,
            name: name.clone(),
        });

        self.edit_result = Some(format!("Staged folder '{name}'"));
    }

    /// Handle files dropped from the host OS onto the window.
    fn handle_dropped_files(&mut self, ui: &mut egui::Ui) {
        if !self.edit_mode {
            return;
        }

        let dropped: Vec<PathBuf> = ui.ctx().input(|i| {
            i.raw
                .dropped_files
                .iter()
                .filter_map(|f| f.path.clone())
                .collect()
        });

        if !dropped.is_empty() {
            self.add_host_paths(&dropped);
        }
    }

    /// Poll extraction progress and update UI state.
    /// Drain the background-open worker. While it's still running, render a
    /// spinner + phase label and request a repaint. Once finished, copy the
    /// fs metadata + root listing into self and clear `pending_open`.
    fn poll_pending_open(&mut self, ui: &mut egui::Ui) {
        let mut take_now = false;
        if let Some(arc) = &self.pending_open {
            if let Ok(g) = arc.lock() {
                if g.finished {
                    take_now = true;
                } else {
                    ui.horizontal(|ui| {
                        ui.add(egui::Spinner::new());
                        ui.label(format!("Loading filesystem: {}", g.phase));
                    });
                    ui.label(
                        "Large volumes (e.g. HFS+ catalogs with hundreds of \
                         thousands of files) can take several seconds.",
                    );
                    ui.ctx().request_repaint();
                }
            }
        }
        if take_now {
            if let Some(arc) = self.pending_open.take() {
                if let Ok(mut g) = arc.lock() {
                    self.fs_type = std::mem::take(&mut g.fs_type);
                    self.volume_label = std::mem::take(&mut g.volume_label);
                    self.blessed_folder = g.blessed_folder.take();
                    if let Some(root) = g.root.take() {
                        if let Some(entries) = g.root_entries.take() {
                            self.directory_cache.insert("/".into(), entries);
                            self.expanded_paths.insert("/".into());
                        }
                        self.root = Some(root);
                    }
                    if let Some(err) = g.error.take() {
                        self.error = Some(err);
                    }
                    // Hand the live filesystem into the read cache so we
                    // don't re-open (and re-read the catalog) for the first
                    // user action.
                    self.cached_fs = g.fs.take();
                }
            }
        }
    }

    /// Borrow the cached read-only filesystem, opening fresh if the cache is
    /// empty. The caller MUST return the fs via [`return_fs`](Self::return_fs)
    /// after use so subsequent calls can reuse it. Returning an `Option` (vs.
    /// a `&mut`) keeps the borrow off `self` so callers can also mutate other
    /// `BrowseView` fields in the same scope.
    fn take_or_open_fs(&mut self) -> Option<Box<dyn Filesystem>> {
        if let Some(fs) = self.cached_fs.take() {
            return Some(fs);
        }
        match self.session.open() {
            Ok(fs) => Some(fs),
            Err(e) => {
                log::debug!("session.open() failed: {e}");
                None
            }
        }
    }

    /// Return a previously taken filesystem to the cache.
    fn return_fs(&mut self, fs: Box<dyn Filesystem>) {
        self.cached_fs = Some(fs);
    }

    /// Drop the cached filesystem so the next read re-opens from disk.
    /// Call this whenever the underlying volume bytes may have changed
    /// (after sync_metadata, archive recompress, etc.).
    fn invalidate_cached_fs(&mut self) {
        self.cached_fs = None;
    }

    fn poll_extraction(&mut self, ui: &egui::Ui) {
        let finished_msg = if let Some(progress) = &self.extraction_progress {
            if let Ok(p) = progress.lock() {
                if p.finished {
                    Some(if let Some(ref err) = p.error {
                        format!("Extraction failed: {err}")
                    } else {
                        format!(
                            "Extraction complete: {} files extracted.",
                            p.files_extracted
                        )
                    })
                } else {
                    ui.ctx().request_repaint();
                    None
                }
            } else {
                None
            }
        } else {
            None
        };
        if let Some(msg) = finished_msg {
            self.extraction_result = Some(msg);
            self.extraction_progress = None;
        }
    }
}

/// Run the extraction in a background thread.
fn run_extraction(
    session: &BrowseSession,
    entry: &FileEntry,
    dest: &std::path::Path,
    resource_fork_mode: ResourceForkMode,
    is_hfs: bool,
    is_prodos: bool,
    prodos_export_mode: ProdosExportMode,
    progress: &Arc<Mutex<ExtractionProgress>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut counting_fs = session.open()?;

    // Pre-count files and bytes for progress tracking
    let (total_files, total_bytes) = count_entry(&mut *counting_fs, entry)?;
    if let Ok(mut p) = progress.lock() {
        p.total_files = total_files;
        p.total_bytes = total_bytes;
    }
    drop(counting_fs);

    // Open a fresh filesystem for extraction
    let mut fs = session.open()?;

    extract_entry(
        &mut *fs,
        entry,
        dest,
        resource_fork_mode,
        is_hfs,
        is_prodos,
        prodos_export_mode,
        progress,
    )?;

    Ok(())
}

/// Recursively count files and total bytes for progress tracking.
fn count_entry(
    fs: &mut dyn Filesystem,
    entry: &FileEntry,
) -> Result<(u32, u64), Box<dyn std::error::Error + Send + Sync>> {
    match entry.entry_type {
        EntryType::File => {
            let rsrc = entry.resource_fork_size.unwrap_or(0);
            Ok((1, entry.size + rsrc))
        }
        EntryType::Symlink => Ok((1, 0)),
        EntryType::Directory => {
            let children = fs.list_directory(entry)?;
            let mut total_files = 0u32;
            let mut total_bytes = 0u64;
            for child in &children {
                let (f, b) = count_entry(fs, child)?;
                total_files += f;
                total_bytes += b;
            }
            Ok((total_files, total_bytes))
        }
        EntryType::Special => Ok((0, 0)),
    }
}

/// Recursively extract an entry to the destination path.
#[allow(clippy::too_many_arguments)]
fn extract_entry(
    fs: &mut dyn Filesystem,
    entry: &FileEntry,
    dest: &std::path::Path,
    resource_fork_mode: ResourceForkMode,
    is_hfs: bool,
    is_prodos: bool,
    prodos_export_mode: ProdosExportMode,
    progress: &Arc<Mutex<ExtractionProgress>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Check for cancellation
    if let Ok(p) = progress.lock() {
        if p.cancel_requested {
            return Err("Extraction cancelled".into());
        }
    }

    let base_name = resource_fork::sanitize_filename(&entry.name);
    // For ProDOS files (not directories) in WithTypeSuffix mode, append the
    // CiderPress `#TTAAAA` suffix so type and aux round-trip through host.
    let safe_name =
        if is_prodos && entry.is_file() && prodos_export_mode == ProdosExportMode::WithTypeSuffix {
            let tt = entry
                .type_code
                .as_deref()
                .and_then(|tc| {
                    tc.split_whitespace()
                        .next()
                        .and_then(|s| u8::from_str_radix(s.trim_start_matches('$'), 16).ok())
                })
                .unwrap_or(0x06);
            let aux = entry.aux_type.unwrap_or(0);
            format!(
                "{}{}",
                base_name,
                rusty_backup::fs::prodos_types::encode_cp_suffix(tt, aux)
            )
        } else {
            base_name
        };

    match entry.entry_type {
        EntryType::File => {
            // Update progress with current file name
            if let Ok(mut p) = progress.lock() {
                p.current_file = entry.path.clone();
            }

            let has_rsrc = is_hfs && entry.resource_fork_size.map(|s| s > 0).unwrap_or(false);

            if has_rsrc && resource_fork_mode == ResourceForkMode::MacBinary {
                // MacBinary: single .bin file containing both forks
                let data = fs.read_file(entry, usize::MAX)?;
                let mut rsrc_buf = Vec::new();
                fs.write_resource_fork_to(entry, &mut rsrc_buf)?;

                let type_code = entry
                    .type_code
                    .as_ref()
                    .map(|s| fourcc_bytes(s))
                    .unwrap_or([0; 4]);
                let creator_code = entry
                    .creator_code
                    .as_ref()
                    .map(|s| fourcc_bytes(s))
                    .unwrap_or([0; 4]);

                let mb = resource_fork::build_macbinary(
                    &safe_name,
                    &type_code,
                    &creator_code,
                    &data,
                    &rsrc_buf,
                );
                let out_path = dest.join(format!("{safe_name}.bin"));
                let mut f = BufWriter::new(File::create(&out_path)?);
                f.write_all(&mb)?;
                f.flush()?;

                if let Ok(mut p) = progress.lock() {
                    p.current_bytes += data.len() as u64 + rsrc_buf.len() as u64;
                    p.files_extracted += 1;
                }
            } else {
                // Write data fork
                let out_path = dest.join(&safe_name);
                let mut f = BufWriter::new(File::create(&out_path)?);
                let written = fs.write_file_to(entry, &mut f)?;
                f.flush()?;

                if let Ok(mut p) = progress.lock() {
                    p.current_bytes += written;
                }

                if is_hfs && resource_fork_mode != ResourceForkMode::DataForkOnly {
                    let type_code = entry
                        .type_code
                        .as_ref()
                        .map(|s| fourcc_bytes(s))
                        .unwrap_or([0; 4]);
                    let creator_code = entry
                        .creator_code
                        .as_ref()
                        .map(|s| fourcc_bytes(s))
                        .unwrap_or([0; 4]);
                    let has_finfo = type_code != [0; 4] || creator_code != [0; 4];

                    let mut rsrc_buf = Vec::new();
                    if has_rsrc {
                        fs.write_resource_fork_to(entry, &mut rsrc_buf)?;
                    }

                    if has_rsrc || has_finfo {
                        match resource_fork_mode {
                            ResourceForkMode::Native => {
                                if has_rsrc {
                                    let rsrc_path = out_path.join("..namedfork/rsrc");
                                    let mut rf = BufWriter::new(File::create(&rsrc_path)?);
                                    rf.write_all(&rsrc_buf)?;
                                    rf.flush()?;
                                }
                            }
                            ResourceForkMode::AppleDouble => {
                                let ad = resource_fork::build_appledouble(
                                    &type_code,
                                    &creator_code,
                                    &rsrc_buf,
                                );
                                let ad_path = dest.join(format!("._{safe_name}"));
                                let mut af = BufWriter::new(File::create(&ad_path)?);
                                af.write_all(&ad)?;
                                af.flush()?;
                            }
                            ResourceForkMode::SeparateRsrc => {
                                if has_rsrc {
                                    let rsrc_path = dest.join(format!("{safe_name}.rsrc"));
                                    let mut rf = BufWriter::new(File::create(&rsrc_path)?);
                                    rf.write_all(&rsrc_buf)?;
                                    rf.flush()?;
                                }
                            }
                            _ => {}
                        }

                        if let Ok(mut p) = progress.lock() {
                            p.current_bytes += rsrc_buf.len() as u64;
                        }
                    }
                }

                if let Ok(mut p) = progress.lock() {
                    p.files_extracted += 1;
                }
            }
        }
        EntryType::Directory => {
            let dir_path = dest.join(&safe_name);
            std::fs::create_dir_all(&dir_path)?;

            let children = fs.list_directory(entry)?;
            for child in &children {
                extract_entry(
                    fs,
                    child,
                    &dir_path,
                    resource_fork_mode,
                    is_hfs,
                    is_prodos,
                    prodos_export_mode,
                    progress,
                )?;
            }
        }
        EntryType::Symlink => {
            #[cfg(unix)]
            {
                let target = entry.symlink_target.as_deref().unwrap_or("");
                let link_path = dest.join(&safe_name);
                // Ignore errors for symlinks (target may not exist on host)
                let _ = std::os::unix::fs::symlink(target, &link_path);
            }
            if let Ok(mut p) = progress.lock() {
                p.files_extracted += 1;
            }
        }
        EntryType::Special => {
            // Skip special files (block devices, etc.)
        }
    }

    Ok(())
}

/// Convert a 4-character type/creator string to a byte array.
fn fourcc_bytes(s: &str) -> [u8; 4] {
    let bytes = s.as_bytes();
    let mut result = [b' '; 4];
    for (i, &b) in bytes.iter().take(4).enumerate() {
        result[i] = b;
    }
    result
}

/// Detect whether data is text or binary and return appropriate content.
/// HFS/HFS+ file type classification from assets/hfs_file_types.json, embedded at compile time.
mod hfs_file_types {
    use std::collections::HashMap;
    use std::sync::OnceLock;

    #[derive(serde::Deserialize)]
    struct TypeInfo {
        category: String,
    }

    #[derive(serde::Deserialize)]
    struct HfsFileTypes {
        types: HashMap<String, TypeInfo>,
    }

    static HFS_TYPES: OnceLock<HashMap<String, bool>> = OnceLock::new();
    const HFS_FILE_TYPES_JSON: &str = include_str!("../../assets/hfs_file_types.json");

    fn get() -> &'static HashMap<String, bool> {
        HFS_TYPES.get_or_init(|| {
            let ft: HfsFileTypes =
                serde_json::from_str(HFS_FILE_TYPES_JSON).unwrap_or(HfsFileTypes {
                    types: HashMap::new(),
                });
            ft.types
                .into_iter()
                .map(|(k, v)| (k, v.category == "text"))
                .collect()
        })
    }

    /// Returns Some(true) for known text type, Some(false) for known binary, None for unknown.
    pub fn classify(type_code: &str) -> Option<bool> {
        get().get(type_code).copied()
    }
}

fn detect_content_type(entry: &FileEntry, data: &[u8]) -> FileContent {
    if data.is_empty() {
        return FileContent::Text(String::new());
    }

    // Check HFS type code first (if present)
    if let Some(ref type_code) = entry.type_code {
        if let Some(is_text) = hfs_file_types::classify(type_code) {
            if !is_text {
                return FileContent::Binary(data.to_vec());
            }
            // Known text type — decode as text (Mac files may not be UTF-8)
            if let Ok(text) = std::str::from_utf8(data) {
                return FileContent::Text(text.to_string());
            }
            // Not valid UTF-8 but known text type — show as lossy text
            let text = data
                .iter()
                .map(|&b| {
                    if b.is_ascii_graphic() || b.is_ascii_whitespace() {
                        b as char
                    } else {
                        '.'
                    }
                })
                .collect();
            return FileContent::Text(text);
        }
    }

    // Fall back to content heuristics
    // Try UTF-8 first
    if let Ok(text) = std::str::from_utf8(data) {
        let non_printable = text
            .chars()
            .filter(|c| !c.is_ascii_graphic() && !c.is_ascii_whitespace())
            .count();
        if non_printable * 10 < text.len() {
            return FileContent::Text(text.to_string());
        }
    }

    // Check if mostly printable bytes
    let printable = data
        .iter()
        .filter(|&&b| b.is_ascii_graphic() || b.is_ascii_whitespace())
        .count();
    if printable * 10 >= data.len() * 8 {
        // 80% printable
        let text = data
            .iter()
            .map(|&b| {
                if b.is_ascii_graphic() || b.is_ascii_whitespace() {
                    b as char
                } else {
                    '.'
                }
            })
            .collect();
        return FileContent::Text(text);
    }

    FileContent::Binary(data.to_vec())
}

/// Render a hex dump view of binary data.
fn render_hex_view(ui: &mut egui::Ui, data: &[u8]) {
    let bytes_per_line = 16;
    let lines = data.len().div_ceil(bytes_per_line);
    let max_lines = 256; // Limit display to ~4KB

    let display_lines = lines.min(max_lines);
    let mut hex_text = String::new();

    for i in 0..display_lines {
        let offset = i * bytes_per_line;
        hex_text.push_str(&format!("{offset:08X}  "));

        for j in 0..bytes_per_line {
            if offset + j < data.len() {
                hex_text.push_str(&format!("{:02X} ", data[offset + j]));
            } else {
                hex_text.push_str("   ");
            }
            if j == 7 {
                hex_text.push(' ');
            }
        }

        hex_text.push_str(" |");
        for j in 0..bytes_per_line {
            if offset + j < data.len() {
                let b = data[offset + j];
                hex_text.push(if b.is_ascii_graphic() || b == b' ' {
                    b as char
                } else {
                    '.'
                });
            }
        }
        hex_text.push_str("|\n");
    }

    if lines > max_lines {
        hex_text.push_str(&format!("... ({} more lines)\n", lines - max_lines));
    }

    ui.add(
        egui::TextEdit::multiline(&mut hex_text.as_str())
            .desired_width(f32::INFINITY)
            .font(egui::TextStyle::Monospace),
    );
}
