use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use rusty_backup::clonezilla::block_cache::{PartcloneBlockCache, PartcloneBlockReader};
use rusty_backup::fs;
use rusty_backup::fs::entry::{EntryType, FileEntry};
use rusty_backup::fs::filesystem::{Filesystem, FilesystemError};
use rusty_backup::fs::resource_fork::{self, ResourceForkMode};
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
    /// Whether the browser is active (filesystem loaded).
    active: bool,
    /// Shared block cache for Clonezilla partclone browsing.
    partclone_cache: Option<Arc<Mutex<PartcloneBlockCache>>>,
    /// Resource fork handling mode (HFS/HFS+ only).
    resource_fork_mode: ResourceForkMode,
    /// Active extraction progress (shared with background thread).
    extraction_progress: Option<Arc<Mutex<ExtractionProgress>>>,
    /// Message to show after extraction completes.
    extraction_result: Option<String>,
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
            active: false,
            partclone_cache: None,
            resource_fork_mode: ResourceForkMode::AppleDouble,
            extraction_progress: None,
            extraction_result: None,
        }
    }
}

impl BrowseView {
    /// Initialize the browser for a partition within a source image/device.
    pub fn open(
        &mut self,
        source_path: PathBuf,
        partition_offset: u64,
        partition_type: u8,
        partition_type_string: Option<String>,
    ) {
        self.close();
        self.source_path = Some(source_path.clone());
        self.partition_offset = partition_offset;
        self.partition_type = partition_type;
        self.partition_type_string = partition_type_string;

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
        self.partition_type_string = None;
        self.extraction_progress = None;
        self.extraction_result = None;
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

        // Header
        ui.horizontal(|ui| {
            ui.label(egui::RichText::new("Filesystem Browser").strong());
            ui.label(format!("[{}]", self.fs_type));
            if !self.volume_label.is_empty() {
                ui.label(format!("Label: {}", self.volume_label));
            }
            if ui.button("Close").clicked() {
                self.close();
                return;
            }
        });

        if let Some(err) = &self.error {
            ui.colored_label(egui::Color32::from_rgb(255, 100, 100), err);
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

        ui.separator();

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
) -> Result<Box<dyn Filesystem>, FilesystemError> {
    // If we have a partclone block cache, use it
    if let Some(cache) = partclone_cache {
        let reader = PartcloneBlockReader::new(Arc::clone(cache));
        return fs::open_filesystem(reader, 0, partition_type, None);
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
