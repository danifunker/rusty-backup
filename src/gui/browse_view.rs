use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use rusty_backup::clonezilla::block_cache::{PartcloneBlockCache, PartcloneBlockReader};
use rusty_backup::fs;
use rusty_backup::fs::entry::{EntryType, FileEntry};
use rusty_backup::fs::filesystem::ResourceForkSource;
use rusty_backup::fs::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};
use rusty_backup::fs::resource_fork::{self, ResourceForkMode};
use rusty_backup::fs::zstd_stream::{ZstdStreamCache, ZstdStreamReader};
use rusty_backup::partition;
use rusty_backup::rbformats::chd::ChdReader;

const MAX_PREVIEW_SIZE: usize = 1024 * 1024; // 1 MB max file preview

/// A staged edit operation that will be applied when the user clicks "Apply Edits".
#[derive(Debug, Clone)]
enum StagedEdit {
    AddFile {
        parent: FileEntry,
        name: String,
        host_path: PathBuf,
        size: u64,
        /// ProDOS-specific overrides. None means "auto-detect from the host
        /// filename extension at apply time".
        prodos_type: Option<u8>,
        prodos_aux: Option<u16>,
        /// Resource fork data detected from the host (HFS/HFS+ only).
        resource_fork: Option<resource_fork::ImportedResourceFork>,
    },
    CreateDirectory {
        parent: FileEntry,
        name: String,
    },
    DeleteEntry {
        parent: FileEntry,
        entry: FileEntry,
    },
    DeleteRecursive {
        parent: FileEntry,
        entry: FileEntry,
    },
    SetProdosType {
        entry: FileEntry,
        type_byte: u8,
        aux_type: u16,
    },
    BlessFolder {
        entry: FileEntry,
    },
}

/// Walk an editable filesystem from the root to the directory at `path`,
/// returning its live `FileEntry`.
///
/// Staged edits capture a `parent` `FileEntry` at staging time, but for a
/// pending-add directory the `location` (CNID/cluster) field is a placeholder
/// because the directory does not yet exist on disk. Re-resolving by path at
/// apply time picks up the real identifier assigned when the earlier
/// `CreateDirectory` edit ran.
fn resolve_dir_by_path(
    efs: &mut dyn EditableFilesystem,
    path: &str,
) -> Result<FileEntry, FilesystemError> {
    let mut current = efs.root()?;
    if path == "/" || path.is_empty() {
        return Ok(current);
    }
    for component in path.trim_start_matches('/').split('/') {
        if component.is_empty() {
            continue;
        }
        let children = efs.list_directory(&current)?;
        current = children
            .into_iter()
            .find(|e| e.is_directory() && e.name == component)
            .ok_or_else(|| {
                FilesystemError::NotFound(format!(
                    "directory '{component}' not found while resolving '{path}'"
                ))
            })?;
    }
    Ok(current)
}

/// Context for editing a backup archive partition (decompress → edit → recompress).
struct ArchiveEditContext {
    /// Path to the compressed archive file (e.g. partition-0.zst or partition-0.chd).
    archive_path: PathBuf,
    /// Compression type string ("zstd", "chd", etc.).
    compression_type: String,
    /// Original uncompressed partition size in bytes.
    original_size: u64,
    /// Whether the partition was compacted (stream < original_size).
    compacted: bool,
    /// Path to metadata.json for updating checksums.
    metadata_path: PathBuf,
    /// Partition index in metadata.
    partition_index: usize,
    /// Checksum type ("sha256" or "crc32").
    checksum_type: String,
}

/// Progress state for archive edit extract/compress operations.
struct ArchiveEditProgress {
    phase: String, // "Extracting" or "Compressing"
    current: u64,
    total: u64,
    finished: bool,
    error: Option<String>,
    cancel_requested: bool,
    /// Path to the temp file (set after extraction completes).
    temp_path: Option<PathBuf>,
}

/// Shared extraction progress state between UI and background thread.
struct ExtractionProgress {
    current_bytes: u64,
    total_bytes: u64,
    current_file: String,
    files_extracted: u32,
    total_files: u32,
    finished: bool,
    error: Option<String>,
    cancel_requested: bool,
}

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
    /// Source file path + partition info for re-opening the filesystem.
    source_path: Option<PathBuf>,
    partition_offset: u64,
    partition_type: u8,
    /// APM partition type string (e.g. "Apple_HFS"). None for MBR/GPT.
    partition_type_string: Option<String>,
    /// Pre-opened device file (macOS raw disk, already elevated).
    /// Wrapped in Arc so each open_fs() call can try_clone() an independent fd.
    preopen_file: Option<std::sync::Arc<File>>,
    /// Whether the browser is active (filesystem loaded).
    active: bool,
    /// Shared block cache for Clonezilla partclone browsing.
    partclone_cache: Option<Arc<Mutex<PartcloneBlockCache>>>,
    /// Streaming zstd cache for native zstd backups (before seekable cache is ready).
    zstd_cache: Option<Arc<Mutex<ZstdStreamCache>>>,
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
    /// Queued edit operations awaiting "Apply Edits".
    staged_edits: Vec<StagedEdit>,
    /// Whether to show the "unsaved changes" confirmation dialog.
    show_unsaved_dialog: bool,
    /// State for the "Set ProDOS Type…" dialog (None == closed).
    prodos_type_dialog: Option<ProdosTypeDialogState>,
    /// Files that failed to stage (bad name, IO error, etc.). When non-empty,
    /// `show_staging_errors` drives a modal dialog listing each failure.
    staging_errors: Vec<(PathBuf, String)>,
    /// Whether the staging-errors modal dialog is open.
    show_staging_errors: bool,
    /// Extraction parameters awaiting overwrite confirmation.
    pending_extraction: Option<PendingExtraction>,
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
struct ProdosTypeDialogState {
    /// Target entry (existing file) — cloned at dialog-open time.
    entry: FileEntry,
    /// If `Some(i)`, the entry is a pending `StagedEdit::AddFile` at index `i`
    /// in `staged_edits` and applying the dialog should mutate it in place.
    staged_index: Option<usize>,
    /// Current type byte.
    type_byte: u8,
    /// Current aux type.
    aux_type: u16,
    /// Freeform hex input for the type byte (kept in sync with the dropdown,
    /// also used for types not in the embedded table).
    type_hex: String,
    /// Freeform hex input for the aux type.
    aux_hex: String,
    /// Transient validation error for display below the inputs.
    error: Option<String>,
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
            source_path: None,
            partition_offset: 0,
            partition_type: 0,
            partition_type_string: None,
            preopen_file: None,
            active: false,
            partclone_cache: None,
            zstd_cache: None,
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
            show_fsck_debug: false,
            show_repair_confirm: false,
            repair_report: None,
            tree_text: None,
            show_tree_popup: false,
            tree_show_ids: false,
            staged_edits: Vec::new(),
            show_unsaved_dialog: false,
            prodos_type_dialog: None,
            staging_errors: Vec::new(),
            show_staging_errors: false,
            pending_extraction: None,
        }
    }
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
        self.source_path = Some(source_path.clone());
        self.partition_offset = partition_offset;
        self.partition_type = partition_type;
        self.partition_type_string = partition_type_string;
        self.preopen_file = preopen_file.map(std::sync::Arc::new);
        // Editing is supported for regular files (not Clonezilla/streaming)
        self.edit_supported = true;

        // Container formats (WOZ, CHD) can't be edited in-place — route
        // through the decompress→edit→recompress flow automatically.
        let ext = source_path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        if ext == "woz" || ext == "chd" {
            let original_size = match ext.as_str() {
                "woz" => rusty_backup::rbformats::woz::WozReader::open(&source_path)
                    .map(|r| r.len())
                    .unwrap_or(0),
                "chd" => std::fs::metadata(&source_path)
                    .map(|m| m.len())
                    .unwrap_or(0),
                _ => 0,
            };
            log::info!(
                "Container edit context set for {} ({} decoded bytes)",
                source_path.display(),
                original_size
            );
            self.archive_edit_ctx = Some(ArchiveEditContext {
                archive_path: source_path.clone(),
                compression_type: ext.clone(),
                original_size,
                compacted: false,
                metadata_path: PathBuf::new(),
                partition_index: 0,
                checksum_type: String::new(),
            });
        }

        match self.open_fs() {
            Ok(mut fs) => {
                self.fs_type = fs.fs_type().to_string();
                self.volume_label = fs.volume_label().unwrap_or("").to_string();
                self.blessed_folder = fs.blessed_system_folder();
                self.active = true;

                match fs.root() {
                    Ok(root) => {
                        // Auto-expand and cache root directory
                        match fs.list_directory(&root) {
                            Ok(entries) => {
                                self.directory_cache.insert("/".into(), entries);
                                self.expanded_paths.insert("/".into());
                            }
                            Err(e) => {
                                self.error = Some(format!("Failed to read root directory: {e}"));
                            }
                        }
                        self.root = Some(root);
                    }
                    Err(e) => {
                        self.error = Some(format!("Failed to get root: {e}"));
                    }
                }
            }
            Err(e) => {
                self.error = Some(format!("Cannot open filesystem: {e}"));
                self.active = true;
            }
        }
    }

    /// Open the browser using a partclone block cache (for Clonezilla images).
    pub fn open_partclone(&mut self, cache: Arc<Mutex<PartcloneBlockCache>>, partition_type: u8) {
        self.close();
        self.partclone_cache = Some(cache.clone());
        self.partition_type = partition_type;
        self.partition_offset = 0;

        let reader = PartcloneBlockReader::new(cache);
        match fs::open_filesystem(reader, 0, partition_type, None) {
            Ok(mut fs) => {
                self.fs_type = fs.fs_type().to_string();
                self.volume_label = fs.volume_label().unwrap_or("").to_string();
                self.blessed_folder = fs.blessed_system_folder();
                self.active = true;

                match fs.root() {
                    Ok(root) => {
                        match fs.list_directory(&root) {
                            Ok(entries) => {
                                self.directory_cache.insert("/".into(), entries);
                                self.expanded_paths.insert("/".into());
                            }
                            Err(e) => {
                                self.error = Some(format!("Failed to read root directory: {e}"));
                            }
                        }
                        self.root = Some(root);
                    }
                    Err(e) => {
                        self.error = Some(format!("Failed to get root: {e}"));
                    }
                }
            }
            Err(e) => {
                self.error = Some(format!("Cannot open filesystem: {e}"));
                self.active = true;
            }
        }
    }

    /// Open the browser by streaming a native zstd-compressed partition image.
    ///
    /// The filesystem opens immediately via a `ZstdStreamReader` backed by a
    /// 256 MB in-memory buffer.  Call `upgrade_to_seekable_cache` once the
    /// background seekable cache is ready to enable full random access.
    pub fn open_streaming(&mut self, path: PathBuf, ptype: u8, ptype_str: Option<String>) {
        self.close();
        self.partition_type = ptype;
        self.partition_type_string = ptype_str;
        self.partition_offset = 0;

        let cache = match ZstdStreamCache::new(&path) {
            Ok(c) => Arc::new(Mutex::new(c)),
            Err(e) => {
                self.error = Some(format!("Cannot open zstd stream: {e}"));
                self.active = true;
                return;
            }
        };
        self.zstd_cache = Some(cache);

        match self.open_fs() {
            Ok(mut fs) => {
                self.fs_type = fs.fs_type().to_string();
                self.volume_label = fs.volume_label().unwrap_or("").to_string();
                self.blessed_folder = fs.blessed_system_folder();
                self.active = true;

                match fs.root() {
                    Ok(root) => {
                        match fs.list_directory(&root) {
                            Ok(entries) => {
                                self.directory_cache.insert("/".into(), entries);
                                self.expanded_paths.insert("/".into());
                            }
                            Err(e) => {
                                self.error = Some(format!("Failed to read root directory: {e}"));
                            }
                        }
                        self.root = Some(root);
                    }
                    Err(e) => {
                        self.error = Some(format!("Failed to get root: {e}"));
                    }
                }
            }
            Err(e) => {
                self.error = Some(format!("Cannot open filesystem: {e}"));
                self.active = true;
            }
        }
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

    /// (`zstd_cache` is `Some`).  The directory cache is preserved so the
    /// user stays in the same place in the tree.
    pub fn upgrade_to_seekable_cache(&mut self, cache_path: PathBuf) {
        if !self.active || self.zstd_cache.is_none() {
            return;
        }
        self.source_path = Some(cache_path);
        self.zstd_cache = None;
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
        self.partclone_cache = None;
        self.zstd_cache = None;
        self.partition_type_string = None;
        self.preopen_file = None;
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
        self.staged_edits.clear();
        self.show_unsaved_dialog = false;
        // Clean up archive temp file if present
        if let Some(temp) = self.archive_temp_path.take() {
            let _ = std::fs::remove_file(&temp);
        }
        self.archive_edit_ctx = None;
        self.archive_edit_progress = None;
    }

    pub fn is_active(&self) -> bool {
        self.active
    }

    fn open_fs(&self) -> Result<Box<dyn Filesystem>, FilesystemError> {
        create_filesystem(
            self.source_path.as_ref(),
            self.partition_offset,
            self.partition_type,
            self.partition_type_string.as_deref(),
            self.partclone_cache.as_ref(),
            self.zstd_cache.as_ref(),
            self.preopen_file.as_ref(),
        )
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
        let fs = self.open_fs().map_err(|e| e.to_string())?;
        fs.validate_name(name).map_err(|e| e.to_string())
    }

    pub fn show(&mut self, ui: &mut egui::Ui) {
        if !self.active {
            return;
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
                let busy =
                    self.extraction_progress.is_some() || self.archive_edit_progress.is_some();
                ui.add_space(8.0);
                let edit_btn = if self.edit_mode {
                    egui::Button::new("Edit Mode ON")
                } else {
                    egui::Button::new("Edit Mode")
                };
                let btn = ui.add_enabled(!busy, edit_btn);
                if btn.clicked() {
                    log::info!(
                        "Edit Mode clicked: archive_edit_ctx={}, edit_mode={}",
                        self.archive_edit_ctx.is_some(),
                        self.edit_mode
                    );
                    if self.archive_edit_ctx.is_some() {
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
                        } else {
                            self.edit_mode = !self.edit_mode;
                            if !self.edit_mode {
                                self.edit_result = None;
                                self.show_new_folder_dialog = false;
                                self.pending_delete = None;
                                self.staged_edits.clear();
                            }
                        }
                    }
                }
                if !self.edit_mode && btn.hovered() {
                    btn.on_hover_text("Enable editing to add or delete files on this image");
                }
            }

            // Check filesystem button (HFS only for now)
            if self.fs_type == "HFS" && ui.button("Check").clicked() {
                match self.open_fs() {
                    Ok(mut fs) => match fs.fsck() {
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
                    },
                    Err(e) => {
                        self.error = Some(format!("Failed to open filesystem: {}", e));
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
                    return;
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

        // Info banner while editing an archive
        if self.edit_mode && self.archive_edit_ctx.is_some() {
            ui.colored_label(
                egui::Color32::from_rgb(100, 160, 255),
                "Editing temporary copy. Click 'Apply Edits' to write changes.",
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
        self.render_prodos_type_dialog(ui);

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
        self.render_tree_popup(ui);
    }

    fn render_tree_entry(&mut self, ui: &mut egui::Ui, entry: &FileEntry) {
        let pending_del = self.edit_mode && self.is_pending_delete(entry);
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
                        let pending = self.pending_adds_for(&path);
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
                        format!("{tc} • ${:04X}", aux)
                    } else {
                        format!("{desc} • ${:04X}", aux)
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
        let is_selected = self
            .selected_entry
            .as_ref()
            .map(|s| s.path == entry.path)
            .unwrap_or(false);

        // Look up the staged AddFile for this entry to surface rsrc/type info.
        let rsrc_badge: Option<String> = self.staged_edits.iter().find_map(|e| match e {
            StagedEdit::AddFile {
                parent,
                name,
                resource_fork: Some(imp),
                ..
            } => {
                let path = if parent.path == "/" {
                    format!("/{name}")
                } else {
                    format!("{}/{name}", parent.path)
                };
                if path != entry.path {
                    return None;
                }
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
                Some(format!(
                    "[{}+rsrc {}]",
                    codes,
                    partition::format_size(imp.data.len() as u64)
                ))
            }
            _ => None,
        });

        let label = if entry.is_directory() {
            format!("+ {}", entry.name)
        } else if let Some(ref badge) = rsrc_badge {
            format!("+ {}  {}  ({})", entry.name, badge, entry.size_string())
        } else {
            format!("+ {}  ({})", entry.name, entry.size_string())
        };

        let text = egui::RichText::new(&label).color(green);
        if ui.selectable_label(is_selected, text).clicked() {
            // Allow selecting pending-add entries (for unstaging via delete)
            self.selected_entry = Some(entry.clone());
            self.content = None;
        }
    }

    fn load_directory(&mut self, entry: &FileEntry) {
        if let Ok(mut fs) = self.open_fs() {
            match fs.list_directory(entry) {
                Ok(entries) => {
                    self.directory_cache.insert(entry.path.clone(), entries);
                }
                Err(e) => {
                    self.error = Some(format!("Failed to read {}: {e}", entry.path));
                }
            }
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

        if let Ok(mut fs) = self.open_fs() {
            match fs.read_file(entry, MAX_PREVIEW_SIZE) {
                Ok(data) => {
                    self.content = Some(detect_content_type(entry, &data));
                }
                Err(e) => {
                    self.error = Some(format!("Failed to read file: {e}"));
                }
            }
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
                    if let Some(ref tc) = entry.type_code {
                        ui.label(format!("Type: {tc}"));
                    }
                    if let Some(ref cc) = entry.creator_code {
                        ui.label(format!("Creator: {cc}"));
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
        let source_path = self.source_path.clone();
        let partition_offset = self.partition_offset;
        let partition_type = self.partition_type;
        let partition_type_string = self.partition_type_string.clone();
        let partclone_cache = self.partclone_cache.clone();
        let zstd_cache = self.zstd_cache.clone();
        let preopen_file = self.preopen_file.clone();
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
            let result = run_extraction(
                source_path.as_ref(),
                partition_offset,
                partition_type,
                partition_type_string.as_deref(),
                partclone_cache.as_ref(),
                zstd_cache.as_ref(),
                preopen_file.as_ref(),
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

    /// Open a writable filesystem for editing operations.
    fn open_editable_fs(&self) -> Result<Box<dyn EditableFilesystem>, FilesystemError> {
        let path = self
            .source_path
            .as_ref()
            .ok_or_else(|| FilesystemError::Parse("no source path set".into()))?;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(FilesystemError::Io)?;

        fs::open_editable_filesystem(
            file,
            self.partition_offset,
            self.partition_type,
            self.partition_type_string.as_deref(),
        )
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
                        for (_, siblings) in &self.directory_cache {
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
        let ctx = match &self.archive_edit_ctx {
            Some(c) => c,
            None => return,
        };

        let archive_path = ctx.archive_path.clone();
        let compression_type = ctx.compression_type.clone();
        let original_size = ctx.original_size;
        let compacted = ctx.compacted;

        // Create temp file next to the archive
        let parent = archive_path.parent().unwrap_or(std::path::Path::new("."));
        let temp_path = parent.join(format!(
            "{}-edit.img",
            archive_path
                .file_stem()
                .unwrap_or_default()
                .to_string_lossy()
        ));

        let progress = Arc::new(Mutex::new(ArchiveEditProgress {
            phase: "Extracting".to_string(),
            current: 0,
            total: original_size,
            finished: false,
            error: None,
            cancel_requested: false,
            temp_path: Some(temp_path.clone()),
        }));
        self.archive_edit_progress = Some(Arc::clone(&progress));

        let progress_thread = Arc::clone(&progress);
        std::thread::spawn(move || {
            let cancel = {
                let p = Arc::clone(&progress_thread);
                move || p.lock().map(|g| g.cancel_requested).unwrap_or(false)
            };
            let result = rusty_backup::rbformats::decompress_partition_to_file(
                &archive_path,
                &compression_type,
                &temp_path,
                original_size,
                compacted,
                &mut |bytes| {
                    if let Ok(mut p) = progress_thread.lock() {
                        p.current = bytes;
                    }
                },
                &cancel,
            );
            if let Ok(mut p) = progress_thread.lock() {
                p.finished = true;
                if let Err(e) = result {
                    p.error = Some(format!("{e:#}"));
                    // Clean up temp file on error
                    let _ = std::fs::remove_file(&temp_path);
                }
            }
        });
    }

    /// Start background recompression of the edited temp file back to the archive.
    fn start_archive_compress(&mut self) {
        let ctx = match &self.archive_edit_ctx {
            Some(c) => c,
            None => return,
        };

        let temp_path = match &self.archive_temp_path {
            Some(p) => p.clone(),
            None => return,
        };

        // Disable edit mode immediately
        self.edit_mode = false;
        self.edit_result = None;
        self.show_new_folder_dialog = false;
        self.pending_delete = None;

        let archive_path = ctx.archive_path.clone();
        let compression_type = ctx.compression_type.clone();
        let metadata_path = ctx.metadata_path.clone();
        let partition_index = ctx.partition_index;
        let checksum_type = ctx.checksum_type.clone();

        let input_size = std::fs::metadata(&temp_path).map(|m| m.len()).unwrap_or(0);

        let progress = Arc::new(Mutex::new(ArchiveEditProgress {
            phase: "Compressing".to_string(),
            current: 0,
            total: input_size,
            finished: false,
            error: None,
            cancel_requested: false,
            temp_path: None,
        }));
        self.archive_edit_progress = Some(Arc::clone(&progress));

        // Switch browse source back to archive (read-only) — close the browser
        // while compressing to release the temp file
        self.root = None;
        self.directory_cache.clear();
        self.selected_entry = None;
        self.content = None;

        let progress_thread = Arc::clone(&progress);
        std::thread::spawn(move || {
            let cancel = {
                let p = Arc::clone(&progress_thread);
                move || p.lock().map(|g| g.cancel_requested).unwrap_or(false)
            };

            // Compute checksum of the temp file before compressing
            let checksum_result = compute_file_checksum(&temp_path, &checksum_type);

            // Remove old archive, compress temp → new archive
            let archive_base = archive_path.with_extension("");
            log::info!(
                "Compressing {} → {} (type={})",
                temp_path.display(),
                archive_base.display(),
                compression_type
            );
            let result = rusty_backup::rbformats::compress_file_to_archive(
                &temp_path,
                &archive_base,
                &compression_type,
                &mut |bytes| {
                    if let Ok(mut p) = progress_thread.lock() {
                        p.current = bytes;
                    }
                },
                &cancel,
                &mut |msg| log::info!("{}", msg),
            );

            if let Ok(mut p) = progress_thread.lock() {
                p.finished = true;
                match result {
                    Ok(new_files) => {
                        log::info!("Compress succeeded: {:?}", new_files);
                        // Remove old archive file (in case extension changed)
                        if archive_path.exists() {
                            // The new file may have the same path; only remove if different
                            let new_path = archive_path
                                .parent()
                                .unwrap_or(std::path::Path::new("."))
                                .join(&new_files[0]);
                            if new_path != archive_path {
                                let _ = std::fs::remove_file(&archive_path);
                            }
                        }

                        // Update metadata checksum (skip for standalone
                        // container files where metadata_path is empty)
                        if !metadata_path.as_os_str().is_empty() {
                            if let Ok(checksum) = checksum_result {
                                if let Err(e) =
                                    rusty_backup::backup::metadata::update_partition_checksum(
                                        &metadata_path,
                                        partition_index,
                                        &checksum,
                                        Some(&new_files),
                                    )
                                {
                                    p.error = Some(format!(
                                        "Saved archive but failed to update metadata: {e}"
                                    ));
                                }
                            }
                        }

                        // Clean up temp file
                        let _ = std::fs::remove_file(&temp_path);
                    }
                    Err(e) => {
                        p.error = Some(format!("{e:#}"));
                        // Keep temp file on error so user can retry
                    }
                }
            }
        });
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
            // Extraction done — switch source to temp file, enable editing
            if let Some(temp) = temp_path {
                self.archive_temp_path = Some(temp.clone());
                self.source_path = Some(temp);
                self.partition_offset = 0;
                self.zstd_cache = None;
                self.edit_mode = true;

                // Re-open filesystem from temp file
                match self.open_fs() {
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
                    }
                    Err(e) => {
                        self.edit_result = Some(format!("Error opening temp file: {e}"));
                    }
                }
            }
        } else {
            // Compression done — re-open original archive for browsing
            if let Some(ctx) = &self.archive_edit_ctx {
                self.source_path = Some(ctx.archive_path.clone());
                self.partition_offset = 0;
                self.archive_temp_path = None;

                match self.open_fs() {
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
                    }
                    Err(e) => {
                        self.edit_result =
                            Some(format!("Saved but failed to re-open archive: {e}"));
                    }
                }
            }
        }
    }

    /// Check if an entry is pending deletion in the staged edits.
    fn is_pending_delete(&self, entry: &FileEntry) -> bool {
        self.staged_edits.iter().any(|edit| match edit {
            StagedEdit::DeleteEntry { entry: e, .. }
            | StagedEdit::DeleteRecursive { entry: e, .. } => e.path == entry.path,
            _ => false,
        })
    }

    /// Check if an entry is a pending add (not yet on disk).
    fn is_pending_add(&self, entry_path: &str) -> bool {
        self.staged_edits.iter().any(|edit| match edit {
            StagedEdit::AddFile { parent, name, .. } => {
                let path = if parent.path == "/" {
                    format!("/{name}")
                } else {
                    format!("{}/{name}", parent.path)
                };
                path == entry_path
            }
            StagedEdit::CreateDirectory { parent, name, .. } => {
                let path = if parent.path == "/" {
                    format!("/{name}")
                } else {
                    format!("{}/{name}", parent.path)
                };
                path == entry_path
            }
            _ => false,
        })
    }

    /// Collect pending add entries for a given parent path.
    fn pending_adds_for(&self, parent_path: &str) -> Vec<FileEntry> {
        self.staged_edits
            .iter()
            .filter_map(|edit| match edit {
                StagedEdit::AddFile {
                    parent, name, size, ..
                } if parent.path == parent_path => {
                    let path = if parent.path == "/" {
                        format!("/{name}")
                    } else {
                        format!("{}/{name}", parent.path)
                    };
                    Some(FileEntry::new_file(name.clone(), path, *size, 0))
                }
                StagedEdit::CreateDirectory { parent, name, .. } if parent.path == parent_path => {
                    let path = if parent.path == "/" {
                        format!("/{name}")
                    } else {
                        format!("{}/{name}", parent.path)
                    };
                    Some(FileEntry::new_directory(name.clone(), path, 0))
                }
                _ => None,
            })
            .collect()
    }

    /// Remove a pending-add entry from staged_edits by path. Returns true if found.
    fn remove_pending_add(&mut self, entry_path: &str) -> bool {
        let before = self.staged_edits.len();
        self.staged_edits.retain(|edit| match edit {
            StagedEdit::AddFile { parent, name, .. }
            | StagedEdit::CreateDirectory { parent, name, .. } => {
                let path = if parent.path == "/" {
                    format!("/{name}")
                } else {
                    format!("{}/{name}", parent.path)
                };
                path != entry_path
            }
            _ => true,
        });
        self.staged_edits.len() < before
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
                    // If it's a pending-add, just remove it from staged_edits
                    if self.is_pending_add(&sel.path) {
                        self.remove_pending_add(&sel.path);
                        self.edit_result = Some(format!("Unstaged '{}'", sel.name));
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
            if self.is_prodos_type() {
                let can_set_type = has_selection
                    && self
                        .selected_entry
                        .as_ref()
                        .map(|e| e.is_file())
                        .unwrap_or(false);
                if ui
                    .add_enabled(can_set_type, egui::Button::new("Set Type…"))
                    .clicked()
                {
                    if let Some(ref sel) = self.selected_entry.clone() {
                        self.open_prodos_type_dialog(sel.clone());
                    }
                }
            }

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
            if let Ok(mut efs) = self.open_editable_fs() {
                if let Ok(free) = efs.free_space() {
                    ui.label(format!("Free: {}", partition::format_size(free)));

                    if !self.staged_edits.is_empty() {
                        let mut bytes_added: u64 = 0;
                        let mut bytes_freed: u64 = 0;
                        for edit in &self.staged_edits {
                            match edit {
                                StagedEdit::AddFile { size, .. } => bytes_added += size,
                                StagedEdit::DeleteEntry { entry, .. }
                                | StagedEdit::DeleteRecursive { entry, .. } => {
                                    if !entry.is_directory() {
                                        bytes_freed += entry.size;
                                    }
                                }
                                _ => {}
                            }
                        }
                        let projected =
                            free.saturating_add(bytes_freed).saturating_sub(bytes_added);
                        let color = if bytes_added > free + bytes_freed {
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
        let pending = self.pending_adds_for(&parent.path);
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
        let pending = self.pending_adds_for(&parent.path);
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
            self.source_path
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_default()
        );
        let mut efs = match self.open_editable_fs() {
            Ok(fs) => fs,
            Err(e) => {
                log::error!("Failed to open editable filesystem: {e}");
                self.edit_result = Some(format!("Error opening filesystem: {e}"));
                return;
            }
        };

        let edits: Vec<StagedEdit> = self.staged_edits.drain(..).collect();
        let total = edits.len();

        for (i, edit) in edits.iter().enumerate() {
            let result = match edit {
                StagedEdit::AddFile {
                    parent,
                    name,
                    host_path,
                    size,
                    prodos_type,
                    prodos_aux,
                    resource_fork: rsrc_import,
                } => {
                    // Build CreateFileOptions with resource fork and type/creator
                    // from the detected import, if any.
                    let mut opts = CreateFileOptions {
                        type_code: prodos_type.map(|t| format!("${:02X}", t)),
                        aux_type: *prodos_aux,
                        ..Default::default()
                    };

                    if let Some(ref imp) = rsrc_import {
                        opts.resource_fork = Some(ResourceForkSource::Data(imp.data.clone()));
                        // Type/creator from container overrides auto-detect,
                        // but not explicit ProDOS overrides.
                        if opts.type_code.is_none() {
                            if let Some(tc) = imp.type_code {
                                opts.type_code = Some(String::from_utf8_lossy(&tc).to_string());
                            }
                        }
                        if opts.creator_code.is_none() {
                            if let Some(cc) = imp.creator_code {
                                opts.creator_code = Some(String::from_utf8_lossy(&cc).to_string());
                            }
                        }
                    }

                    // For MacBinary imports, use the extracted data fork
                    // instead of the raw .bin file.
                    if let Some(ref imp) = rsrc_import {
                        if let Some(ref data_fork) = imp.data_fork {
                            let mut cursor = std::io::Cursor::new(data_fork);
                            let df_size = data_fork.len() as u64;
                            resolve_dir_by_path(&mut *efs, &parent.path).and_then(
                                |resolved_parent| {
                                    efs.create_file(
                                        &resolved_parent,
                                        name,
                                        &mut cursor,
                                        df_size,
                                        &opts,
                                    )
                                    .map(|_| ())
                                },
                            )
                        } else {
                            match File::open(host_path) {
                                Ok(mut file) => resolve_dir_by_path(&mut *efs, &parent.path)
                                    .and_then(|resolved_parent| {
                                        efs.create_file(
                                            &resolved_parent,
                                            name,
                                            &mut file,
                                            *size,
                                            &opts,
                                        )
                                        .map(|_| ())
                                    }),
                                Err(e) => Err(FilesystemError::Io(e)),
                            }
                        }
                    } else {
                        match File::open(host_path) {
                            Ok(mut file) => resolve_dir_by_path(&mut *efs, &parent.path).and_then(
                                |resolved_parent| {
                                    efs.create_file(&resolved_parent, name, &mut file, *size, &opts)
                                        .map(|_| ())
                                },
                            ),
                            Err(e) => Err(FilesystemError::Io(e)),
                        }
                    }
                }
                StagedEdit::CreateDirectory { parent, name } => {
                    resolve_dir_by_path(&mut *efs, &parent.path).and_then(|resolved_parent| {
                        efs.create_directory(
                            &resolved_parent,
                            name,
                            &CreateDirectoryOptions::default(),
                        )
                        .map(|_| ())
                    })
                }
                StagedEdit::DeleteEntry { parent, entry } => efs.delete_entry(parent, entry),
                StagedEdit::DeleteRecursive { parent, entry } => {
                    efs.delete_recursive(parent, entry)
                }
                StagedEdit::SetProdosType {
                    entry,
                    type_byte,
                    aux_type,
                } => efs.set_prodos_type(entry, *type_byte, *aux_type),
                StagedEdit::BlessFolder { entry } => efs.set_blessed_folder(entry),
            };

            if let Err(e) = result {
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

        // Update blessed folder info after apply
        if let Ok(mut fs) = self.open_fs() {
            self.blessed_folder = fs.blessed_system_folder();
        }
    }

    fn has_edit_error(&self) -> bool {
        self.edit_result
            .as_ref()
            .map(|r| r.starts_with("Error"))
            .unwrap_or(false)
    }

    /// Invalidate all cached directory listings and reload root.
    fn invalidate_all_caches(&mut self) {
        self.directory_cache.clear();
        self.selected_entry = None;
        self.content = None;
        // Reload root from filesystem
        if let Ok(mut fs) = self.open_fs() {
            if let Ok(root) = fs.root() {
                if let Ok(children) = fs.list_directory(&root) {
                    self.directory_cache.insert(root.path.clone(), children);
                }
                self.root = Some(root);
            }
        }
    }

    /// Open the "Set ProDOS Type…" dialog for a given file entry. If the
    /// entry is a pending `AddFile` staged edit, the dialog will mutate it
    /// in place on Apply instead of pushing a new `SetProdosType` edit.
    fn open_prodos_type_dialog(&mut self, entry: FileEntry) {
        // Seed the dialog from whatever we currently know about this entry.
        // For an existing on-disk file, `entry.aux_type` is populated by
        // `list_prodos_directory` and `type_code` looks like "$XX ABC".
        // For a pending add we read the staged override if set.
        let (mut type_byte, mut aux_type) = {
            let parsed_tc = entry
                .type_code
                .as_ref()
                .and_then(|tc| {
                    tc.split_whitespace()
                        .next()
                        .and_then(|s| u8::from_str_radix(s.trim_start_matches('$'), 16).ok())
                })
                .unwrap_or(0x06);
            (parsed_tc, entry.aux_type.unwrap_or(0))
        };

        let mut staged_index: Option<usize> = None;
        let entry_full_path = entry.path.clone();
        for (i, edit) in self.staged_edits.iter().enumerate() {
            if let StagedEdit::AddFile {
                parent,
                name,
                prodos_type,
                prodos_aux,
                ..
            } = edit
            {
                let p = if parent.path == "/" {
                    format!("/{name}")
                } else {
                    format!("{}/{name}", parent.path)
                };
                if p == entry_full_path {
                    staged_index = Some(i);
                    if let Some(t) = prodos_type {
                        type_byte = *t;
                    }
                    if let Some(a) = prodos_aux {
                        aux_type = *a;
                    }
                    break;
                }
            }
        }

        self.prodos_type_dialog = Some(ProdosTypeDialogState {
            entry,
            staged_index,
            type_byte,
            aux_type,
            type_hex: format!("{:02X}", type_byte),
            aux_hex: format!("{:04X}", aux_type),
            error: None,
        });
    }

    /// Render the "Set ProDOS Type…" dialog. No-op when closed.
    fn render_prodos_type_dialog(&mut self, ui: &mut egui::Ui) {
        let Some(mut state) = self.prodos_type_dialog.take() else {
            return;
        };
        let mut keep_open = true;
        let mut apply_clicked = false;

        egui::Window::new("Set ProDOS Type")
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .show(ui.ctx(), |ui| {
                ui.label(format!("File: {}", state.entry.path));
                ui.add_space(6.0);

                // Type dropdown — all entries in the embedded prodos_types table.
                let types = rusty_backup::fs::prodos_types::all_types();
                let current_label = format!(
                    "${:02X} {} — {}",
                    state.type_byte,
                    rusty_backup::fs::prodos_types::type_abbr(state.type_byte),
                    rusty_backup::fs::prodos_types::type_description(state.type_byte),
                );
                ui.horizontal(|ui| {
                    ui.label("Type:");
                    egui::ComboBox::from_id_salt("prodos_type_combo")
                        .selected_text(current_label)
                        .width(320.0)
                        .show_ui(ui, |ui| {
                            for (byte, info) in &types {
                                let label =
                                    format!("${:02X} {} — {}", byte, info.abbr, info.description);
                                if ui
                                    .selectable_label(*byte == state.type_byte, label)
                                    .clicked()
                                {
                                    state.type_byte = *byte;
                                    state.type_hex = format!("{:02X}", byte);
                                }
                            }
                        });
                });

                ui.horizontal(|ui| {
                    ui.label("Custom type (hex):  $");
                    let r = ui.add(
                        egui::TextEdit::singleline(&mut state.type_hex)
                            .desired_width(40.0)
                            .char_limit(2),
                    );
                    if r.changed() {
                        if let Ok(v) = u8::from_str_radix(state.type_hex.trim(), 16) {
                            state.type_byte = v;
                            state.error = None;
                        }
                    }
                });

                ui.horizontal(|ui| {
                    ui.label("Aux type (hex):      $");
                    let r = ui.add(
                        egui::TextEdit::singleline(&mut state.aux_hex)
                            .desired_width(60.0)
                            .char_limit(4),
                    );
                    if r.changed() {
                        if let Ok(v) = u16::from_str_radix(state.aux_hex.trim(), 16) {
                            state.aux_type = v;
                            state.error = None;
                        }
                    }
                });

                if let Some(err) = &state.error {
                    ui.colored_label(egui::Color32::from_rgb(255, 120, 120), err);
                }

                ui.add_space(6.0);
                ui.horizontal(|ui| {
                    if ui.button("Apply").clicked() {
                        // Re-validate hex inputs on apply in case the user typed
                        // something invalid and never blurred the field.
                        let tb = u8::from_str_radix(state.type_hex.trim(), 16);
                        let aux = u16::from_str_radix(state.aux_hex.trim(), 16);
                        match (tb, aux) {
                            (Ok(t), Ok(a)) => {
                                state.type_byte = t;
                                state.aux_type = a;
                                apply_clicked = true;
                                keep_open = false;
                            }
                            _ => {
                                state.error =
                                    Some("Type and aux must be valid hex (2 and 4 digits)".into());
                            }
                        }
                    }
                    if ui.button("Cancel").clicked() {
                        keep_open = false;
                    }
                });
            });

        if apply_clicked {
            self.apply_prodos_type_dialog(&state);
        }
        if keep_open {
            self.prodos_type_dialog = Some(state);
        }
    }

    /// Commit the dialog's result to `staged_edits`.
    fn apply_prodos_type_dialog(&mut self, state: &ProdosTypeDialogState) {
        if let Some(i) = state.staged_index {
            // Mutate the pending AddFile in place.
            if let Some(StagedEdit::AddFile {
                prodos_type,
                prodos_aux,
                ..
            }) = self.staged_edits.get_mut(i)
            {
                *prodos_type = Some(state.type_byte);
                *prodos_aux = Some(state.aux_type);
                self.edit_result = Some(format!(
                    "Staged type ${:02X} / ${:04X} on '{}'",
                    state.type_byte, state.aux_type, state.entry.name
                ));
                return;
            }
        }
        // Existing on-disk file → push a SetProdosType edit.
        self.staged_edits.push(StagedEdit::SetProdosType {
            entry: state.entry.clone(),
            type_byte: state.type_byte,
            aux_type: state.aux_type,
        });
        self.edit_result = Some(format!(
            "Staged type ${:02X} / ${:04X} on '{}'",
            state.type_byte, state.aux_type, state.entry.name
        ));
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
                for edit in &self.staged_edits {
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
                        self.exit_edit_mode();
                    }
                    if ui.button("Apply Edits").clicked() {
                        self.show_unsaved_dialog = false;
                        self.apply_staged_edits();
                        if self
                            .edit_result
                            .as_ref()
                            .map(|r| !r.starts_with("Error"))
                            .unwrap_or(true)
                        {
                            self.exit_edit_mode();
                        }
                    }
                    if ui.button("Cancel").clicked() {
                        self.show_unsaved_dialog = false;
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
                        ui.label(format!("  • {name}"));
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

        if self.archive_edit_ctx.is_some() {
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
        match self.open_editable_fs() {
            Ok(mut efs) => match efs.repair() {
                Ok(report) => {
                    self.repair_report = Some(report);
                    // Re-run fsck to show updated state
                    drop(efs);
                    match self.open_fs() {
                        Ok(mut fs) => {
                            if let Some(Ok(result)) = fs.fsck() {
                                self.fsck_result = Some(result);
                            }
                        }
                        Err(e) => {
                            self.error = Some(format!("Failed to re-check after repair: {}", e));
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
        match self.open_fs() {
            Ok(mut fs) => {
                let result = if self.tree_show_ids {
                    rusty_backup::fs::tree::format_tree_with_ids(&mut *fs)
                } else {
                    rusty_backup::fs::tree::format_tree(&mut *fs)
                };
                match result {
                    Ok(text) => {
                        self.tree_text = Some(text);
                        self.show_tree_popup = true;
                    }
                    Err(e) => {
                        self.error = Some(format!("Failed to generate tree: {}", e));
                    }
                }
            }
            Err(e) => {
                self.error = Some(format!("Failed to open filesystem: {}", e));
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
        let pending = self.pending_adds_for(&parent.path);
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

/// Compute a checksum (SHA256 or CRC32) of a file.
fn compute_file_checksum(
    path: &std::path::Path,
    checksum_type: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    use std::io::Read;
    let mut file = File::open(path)?;
    let mut buf = vec![0u8; 256 * 1024];

    match checksum_type {
        "sha256" => {
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            loop {
                let n = file.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
            }
            Ok(format!("{:x}", hasher.finalize()))
        }
        "crc32" => {
            let mut hasher = crc32fast::Hasher::new();
            loop {
                let n = file.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
            }
            Ok(format!("{:08x}", hasher.finalize()))
        }
        other => Err(format!("unsupported checksum type: {other}").into()),
    }
}

/// Create a filesystem instance from the browse view's configuration.
/// This is a standalone function so it can be called from background threads.
fn create_filesystem(
    source_path: Option<&PathBuf>,
    partition_offset: u64,
    partition_type: u8,
    partition_type_string: Option<&str>,
    partclone_cache: Option<&Arc<Mutex<PartcloneBlockCache>>>,
    zstd_cache: Option<&Arc<Mutex<ZstdStreamCache>>>,
    preopen_file: Option<&std::sync::Arc<File>>,
) -> Result<Box<dyn Filesystem>, FilesystemError> {
    // If we have a partclone block cache, use it
    if let Some(cache) = partclone_cache {
        let reader = PartcloneBlockReader::new(Arc::clone(cache));
        return fs::open_filesystem(reader, 0, partition_type, None);
    }

    // If we have a streaming zstd cache, use it
    if let Some(cache) = zstd_cache {
        let reader = ZstdStreamReader::new(Arc::clone(cache));
        return fs::open_filesystem(
            reader,
            partition_offset,
            partition_type,
            partition_type_string,
        );
    }

    // Pre-opened device file (already elevated, e.g. macOS raw disk).
    // try_clone() gives an independent fd so each open_fs() call can seek freely.
    // Wrap in SectorAlignedReader because macOS raw character devices (/dev/rdiskN)
    // require all seeks and reads to be sector-aligned (512-byte multiples).
    // Filesystem code (FAT's next_cluster, ext's read_inode, etc.) performs
    // sub-sector seeks, so a plain BufReader would cause EINVAL errors.
    if let Some(arc) = preopen_file {
        let file = arc.try_clone().map_err(FilesystemError::Io)?;
        let reader = rusty_backup::os::SectorAlignedReader::new(file);
        return fs::open_filesystem(
            reader,
            partition_offset,
            partition_type,
            partition_type_string,
        );
    }

    let path = source_path.ok_or_else(|| FilesystemError::Parse("no source path set".into()))?;

    // Sniff first 8 bytes to detect content-addressable formats regardless of
    // file extension (users often have mislabeled files).
    let mut magic = [0u8; 8];
    if let Ok(mut f) = File::open(path) {
        use std::io::Read as _;
        let _ = f.read(&mut magic);
    }

    // CHD — 8-byte magic "MComprHD" at offset 0.
    if &magic == b"MComprHD" {
        let chd_reader = ChdReader::open(path)
            .map_err(|e| FilesystemError::Parse(format!("failed to open CHD: {e}")))?;
        return fs::open_filesystem(chd_reader, 0, partition_type, partition_type_string);
    }

    // Seekable zstd cache files — keep extension-based detection since there
    // is no reliable content-level signal distinguishing them from other zstd.
    let is_seekable_zst = path.extension().map(|e| e == "zst").unwrap_or(false)
        && path
            .file_stem()
            .and_then(|s| s.to_str())
            .map(|s| s.ends_with(".seekable"))
            .unwrap_or(false);

    if is_seekable_zst {
        let file = File::open(path).map_err(FilesystemError::Io)?;
        let decoder = match zeekstd::Decoder::new(file) {
            Ok(d) => d,
            Err(e) => {
                // Stale or corrupt cache file — delete it so it gets recreated
                let _ = std::fs::remove_file(path);
                return Err(FilesystemError::Parse(format!(
                    "stale seekable zstd cache removed ({e}). Click Browse again to rebuild."
                )));
            }
        };
        fs::open_filesystem(
            decoder,
            partition_offset,
            partition_type,
            partition_type_string,
        )
    } else {
        // For all other formats (raw images, VHD, 2MG, DiskCopy 4.2, DOS-order, etc.),
        // use the unified format detection pipeline so container wrappers are peeled off.
        let file = File::open(path).map_err(FilesystemError::Io)?;
        match rusty_backup::rbformats::detect_image_format_with_path(file, Some(path)) {
            Ok(format) if !matches!(format, rusty_backup::rbformats::ImageFormat::Raw) => {
                let file2 = File::open(path).map_err(FilesystemError::Io)?;
                let (reader, _size) = rusty_backup::rbformats::wrap_image_reader(file2, format)
                    .map_err(|e| FilesystemError::Parse(format!("failed to unwrap image: {e}")))?;
                // Container formats present unwrapped data starting at offset 0;
                // use partition_offset=0 for superfloppies.
                let effective_offset = if partition_type == 0 {
                    0
                } else {
                    partition_offset
                };
                fs::open_filesystem(
                    reader,
                    effective_offset,
                    partition_type,
                    partition_type_string,
                )
            }
            _ => {
                let file = File::open(path).map_err(FilesystemError::Io)?;
                let reader = BufReader::new(file);
                fs::open_filesystem(
                    reader,
                    partition_offset,
                    partition_type,
                    partition_type_string,
                )
            }
        }
    }
}

/// Run the extraction in a background thread.
#[allow(clippy::too_many_arguments)]
fn run_extraction(
    source_path: Option<&PathBuf>,
    partition_offset: u64,
    partition_type: u8,
    partition_type_string: Option<&str>,
    partclone_cache: Option<&Arc<Mutex<PartcloneBlockCache>>>,
    zstd_cache: Option<&Arc<Mutex<ZstdStreamCache>>>,
    preopen_file: Option<&std::sync::Arc<File>>,
    entry: &FileEntry,
    dest: &std::path::Path,
    resource_fork_mode: ResourceForkMode,
    is_hfs: bool,
    is_prodos: bool,
    prodos_export_mode: ProdosExportMode,
    progress: &Arc<Mutex<ExtractionProgress>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut counting_fs = create_filesystem(
        source_path,
        partition_offset,
        partition_type,
        partition_type_string,
        partclone_cache,
        zstd_cache,
        preopen_file,
    )?;

    // Pre-count files and bytes for progress tracking
    let (total_files, total_bytes) = count_entry(&mut *counting_fs, entry)?;
    if let Ok(mut p) = progress.lock() {
        p.total_files = total_files;
        p.total_bytes = total_bytes;
    }
    drop(counting_fs);

    // Open a fresh filesystem for extraction
    let mut fs = create_filesystem(
        source_path,
        partition_offset,
        partition_type,
        partition_type_string,
        partclone_cache,
        zstd_cache,
        preopen_file,
    )?;

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

                // Handle resource fork
                if has_rsrc && resource_fork_mode != ResourceForkMode::DataForkOnly {
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

                    let mut rsrc_buf = Vec::new();
                    fs.write_resource_fork_to(entry, &mut rsrc_buf)?;

                    match resource_fork_mode {
                        ResourceForkMode::Native => {
                            let rsrc_path = out_path.join("..namedfork/rsrc");
                            let mut rf = BufWriter::new(File::create(&rsrc_path)?);
                            rf.write_all(&rsrc_buf)?;
                            rf.flush()?;
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
                            let rsrc_path = dest.join(format!("{safe_name}.rsrc"));
                            let mut rf = BufWriter::new(File::create(&rsrc_path)?);
                            rf.write_all(&rsrc_buf)?;
                            rf.flush()?;
                        }
                        _ => {}
                    }

                    if let Ok(mut p) = progress.lock() {
                        p.current_bytes += rsrc_buf.len() as u64;
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
    let lines = (data.len() + bytes_per_line - 1) / bytes_per_line;
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
