use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use rusty_backup::clonezilla::block_cache::{PartcloneBlockCache, PartcloneBlockReader};
use rusty_backup::fs;
use rusty_backup::fs::entry::{EntryType, FileEntry};
use rusty_backup::fs::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};
use rusty_backup::fs::resource_fork::{self, ResourceForkMode};
use rusty_backup::fs::zstd_stream::{ZstdStreamCache, ZstdStreamReader};
use rusty_backup::partition;

const MAX_PREVIEW_SIZE: usize = 1024 * 1024; // 1 MB max file preview

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
}

#[derive(Debug, Clone)]
enum FileContent {
    Binary(Vec<u8>),
    Text(String),
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
            extraction_progress: None,
            extraction_result: None,
            edit_mode: false,
            edit_supported: false,
            edit_result: None,
            new_folder_name: String::new(),
            show_new_folder_dialog: false,
            pending_delete: None,
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

        match self.open_fs() {
            Ok(mut fs) => {
                self.fs_type = fs.fs_type().to_string();
                self.volume_label = fs.volume_label().unwrap_or("").to_string();
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

    /// Switch the active streaming view to use a completed seekable cache file.
    ///
    /// Only acts if the browser is currently in active streaming mode
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

            // Edit mode toggle
            if self.edit_supported {
                let extraction_running = self.extraction_progress.is_some();
                ui.add_space(8.0);
                let edit_btn = if self.edit_mode {
                    egui::Button::new("Edit Mode ON")
                } else {
                    egui::Button::new("Edit Mode")
                };
                let btn = ui.add_enabled(!extraction_running, edit_btn);
                if btn.clicked() {
                    self.edit_mode = !self.edit_mode;
                    if !self.edit_mode {
                        self.edit_result = None;
                        self.show_new_folder_dialog = false;
                        self.pending_delete = None;
                    }
                }
                if !self.edit_mode && btn.hovered() {
                    btn.on_hover_text("Enable editing to add or delete files on this image");
                }
            }

            if ui.button("Close").clicked() {
                self.close();
                return;
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

        // New folder dialog
        self.render_new_folder_dialog(ui);

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
    }

    fn render_tree_entry(&mut self, ui: &mut egui::Ui, entry: &FileEntry) {
        match entry.entry_type {
            EntryType::Directory => {
                let path = entry.path.clone();
                let has_children = self.directory_cache.contains_key(&path);

                let header = egui::CollapsingHeader::new(&entry.name)
                    .id_salt(&path)
                    .default_open(path == "/")
                    .show(ui, |ui| {
                        if let Some(children) = self.directory_cache.get(&path).cloned() {
                            for child in &children {
                                self.render_tree_entry(ui, child);
                            }
                        } else {
                            ui.label("Loading...");
                        }
                    });

                // Track expansion state after the header is shown
                let is_now_open = header.body_returned.is_some();

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
                let label = format!("{}  ({})", entry.name, size_str);

                if ui.selectable_label(is_selected, &label).clicked() {
                    self.select_file(entry);
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

                if ui.selectable_label(is_selected, &label).clicked() {
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

                if ui.selectable_label(is_selected, &label).clicked() {
                    self.selected_entry = Some(entry.clone());
                    self.content = None;
                }
            }
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

        let entry = entry.clone();
        let source_path = self.source_path.clone();
        let partition_offset = self.partition_offset;
        let partition_type = self.partition_type;
        let partition_type_string = self.partition_type_string.clone();
        let partclone_cache = self.partclone_cache.clone();
        let zstd_cache = self.zstd_cache.clone();
        let preopen_file = self.preopen_file.clone();
        let resource_fork_mode = self.resource_fork_mode;
        let is_hfs = self.is_hfs_type();

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

    /// Invalidate directory cache for a path and its ancestors.
    fn invalidate_cache_for(&mut self, dir_path: &str) {
        self.directory_cache.remove(dir_path);
        // Also reload from filesystem
        let entry = if dir_path == "/" {
            self.root.clone()
        } else {
            // Find the entry in the cache
            let mut found = None;
            for entries in self.directory_cache.values() {
                for e in entries {
                    if e.path == dir_path && e.is_directory() {
                        found = Some(e.clone());
                        break;
                    }
                }
                if found.is_some() {
                    break;
                }
            }
            found
        };
        if let Some(entry) = entry {
            self.load_directory(&entry);
        }
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

            ui.add_space(16.0);

            // Free space indicator
            if let Ok(mut efs) = self.open_editable_fs() {
                if let Ok(free) = efs.free_space() {
                    ui.label(format!("Free: {}", partition::format_size(free)));
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

    /// Add files/folders from host paths to the current directory.
    fn add_host_paths(&mut self, paths: &[PathBuf]) {
        let parent = self.current_parent_entry();
        let parent_path = parent.path.clone();
        let mut last_error: Option<String> = None;
        let mut added_count = 0usize;

        for path in paths {
            if path.is_dir() {
                match self.add_host_directory(path, &parent) {
                    Ok(n) => added_count += n,
                    Err(e) => last_error = Some(format!("Error adding '{}': {e}", path.display())),
                }
            } else if path.is_file() {
                match self.add_host_file(path, &parent) {
                    Ok(()) => added_count += 1,
                    Err(e) => last_error = Some(format!("Error adding '{}': {e}", path.display())),
                }
            }
        }

        self.invalidate_cache_for(&parent_path);

        if let Some(err) = last_error {
            self.edit_result = Some(err);
        } else if added_count > 0 {
            self.edit_result = Some(format!("Added {added_count} item(s)"));
        }
    }

    /// Add a single host file to a parent directory on the image.
    fn add_host_file(
        &mut self,
        host_path: &std::path::Path,
        parent: &FileEntry,
    ) -> Result<(), String> {
        let name = host_path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or("invalid filename")?
            .to_string();

        let metadata = std::fs::metadata(host_path).map_err(|e| e.to_string())?;
        let data_len = metadata.len();

        let mut file = File::open(host_path).map_err(|e| e.to_string())?;
        let mut efs = self.open_editable_fs().map_err(|e| e.to_string())?;

        efs.create_file(
            parent,
            &name,
            &mut file,
            data_len,
            &CreateFileOptions::default(),
        )
        .map_err(|e| e.to_string())?;

        efs.sync_metadata().map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Recursively add a host directory and its contents to the image.
    fn add_host_directory(
        &mut self,
        host_path: &std::path::Path,
        parent: &FileEntry,
    ) -> Result<usize, String> {
        let name = host_path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or("invalid directory name")?
            .to_string();

        let mut efs = self.open_editable_fs().map_err(|e| e.to_string())?;
        let new_dir = efs
            .create_directory(parent, &name, &CreateDirectoryOptions::default())
            .map_err(|e| e.to_string())?;
        efs.sync_metadata().map_err(|e| e.to_string())?;
        drop(efs);

        let mut count = 1usize; // count the directory itself

        let entries = std::fs::read_dir(host_path).map_err(|e| e.to_string())?;
        for entry in entries {
            let entry = entry.map_err(|e| e.to_string())?;
            let child_path = entry.path();
            if child_path.is_dir() {
                count += self.add_host_directory(&child_path, &new_dir)?;
            } else if child_path.is_file() {
                self.add_host_file(&child_path, &new_dir)?;
                count += 1;
            }
        }

        Ok(count)
    }

    /// Perform a delete operation.
    fn perform_delete(&mut self, parent: &FileEntry, entry: &FileEntry, recursive: bool) {
        let parent_path = parent.path.clone();
        let entry_name = entry.name.clone();

        let result = (|| -> Result<(), String> {
            let mut efs = self.open_editable_fs().map_err(|e| e.to_string())?;
            if recursive {
                efs.delete_recursive(parent, entry)
                    .map_err(|e| e.to_string())?;
            } else {
                efs.delete_entry(parent, entry).map_err(|e| e.to_string())?;
            }
            efs.sync_metadata().map_err(|e| e.to_string())?;
            Ok(())
        })();

        match result {
            Ok(()) => {
                self.edit_result = Some(format!("Deleted '{entry_name}'"));
                self.selected_entry = None;
                self.content = None;
            }
            Err(e) => {
                self.edit_result = Some(format!("Error deleting '{entry_name}': {e}"));
            }
        }

        self.invalidate_cache_for(&parent_path);
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

    /// Create a new folder with the name from the dialog.
    fn create_new_folder(&mut self) {
        let name = self.new_folder_name.trim().to_string();
        self.show_new_folder_dialog = false;

        if name.is_empty() {
            self.edit_result = Some("Error: folder name cannot be empty".into());
            return;
        }

        let parent = self.current_parent_entry();
        let parent_path = parent.path.clone();

        let result = (|| -> Result<(), String> {
            let mut efs = self.open_editable_fs().map_err(|e| e.to_string())?;
            efs.create_directory(&parent, &name, &CreateDirectoryOptions::default())
                .map_err(|e| e.to_string())?;
            efs.sync_metadata().map_err(|e| e.to_string())?;
            Ok(())
        })();

        match result {
            Ok(()) => {
                self.edit_result = Some(format!("Created folder '{name}'"));
            }
            Err(e) => {
                self.edit_result = Some(format!("Error: {e}"));
            }
        }

        self.invalidate_cache_for(&parent_path);
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
    if let Some(arc) = preopen_file {
        let file = arc.try_clone().map_err(FilesystemError::Io)?;
        let reader = BufReader::new(file);
        return fs::open_filesystem(
            reader,
            partition_offset,
            partition_type,
            partition_type_string,
        );
    }

    let path = source_path.ok_or_else(|| FilesystemError::Parse("no source path set".into()))?;

    // Detect seekable zstd files by extension
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

/// Run the extraction in a background thread.
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

    extract_entry(&mut *fs, entry, dest, resource_fork_mode, is_hfs, progress)?;

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
fn extract_entry(
    fs: &mut dyn Filesystem,
    entry: &FileEntry,
    dest: &std::path::Path,
    resource_fork_mode: ResourceForkMode,
    is_hfs: bool,
    progress: &Arc<Mutex<ExtractionProgress>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Check for cancellation
    if let Ok(p) = progress.lock() {
        if p.cancel_requested {
            return Err("Extraction cancelled".into());
        }
    }

    let safe_name = resource_fork::sanitize_filename(&entry.name);

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
                extract_entry(fs, child, &dir_path, resource_fork_mode, is_hfs, progress)?;
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
