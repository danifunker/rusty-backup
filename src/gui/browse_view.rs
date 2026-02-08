use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use rusty_backup::clonezilla::block_cache::{PartcloneBlockCache, PartcloneBlockReader};
use rusty_backup::fs;
use rusty_backup::fs::entry::{EntryType, FileEntry};
use rusty_backup::fs::filesystem::{Filesystem, FilesystemError};
use rusty_backup::partition;

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
    }

    pub fn is_active(&self) -> bool {
        self.active
    }

    fn open_fs(&self) -> Result<Box<dyn Filesystem>, FilesystemError> {
        // If we have a partclone block cache, use it
        if let Some(cache) = &self.partclone_cache {
            let reader = PartcloneBlockReader::new(Arc::clone(cache));
            return fs::open_filesystem(reader, 0, self.partition_type, None);
        }

        let path = self
            .source_path
            .as_ref()
            .ok_or_else(|| FilesystemError::Parse("no source path set".into()))?;

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
                self.partition_offset,
                self.partition_type,
                self.partition_type_string.as_deref(),
            )
        } else {
            let file = File::open(path).map_err(FilesystemError::Io)?;
            let reader = BufReader::new(file);
            fs::open_filesystem(
                reader,
                self.partition_offset,
                self.partition_type,
                self.partition_type_string.as_deref(),
            )
        }
    }

    pub fn show(&mut self, ui: &mut egui::Ui) {
        if !self.active {
            return;
        }

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

    fn render_content_panel(&self, ui: &mut egui::Ui, panel_height: f32) {
        match &self.selected_entry {
            None => {
                ui.colored_label(egui::Color32::GRAY, "Select a file to view its contents.");
            }
            Some(entry) => {
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
                    ui.label(format!("Path: {}", entry.path));
                });
                ui.separator();

                if entry.size > MAX_PREVIEW_SIZE as u64 {
                    ui.label(format!(
                        "File too large to preview ({}).",
                        partition::format_size(entry.size)
                    ));
                    return;
                }

                // Remaining height for the scroll area after the header
                let content_height = ui.available_height().min(panel_height);

                match &self.content {
                    None => {
                        ui.spinner();
                        ui.label("Loading...");
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
