use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use opticaldiscs::browse::entry::{EntryType, FileEntry};
use opticaldiscs::browse::filesystem::{Filesystem, FilesystemError};
use opticaldiscs::browse::open_disc_filesystem;
use opticaldiscs::detect::DiscImageInfo;

const MAX_PREVIEW_SIZE: usize = 1024 * 1024; // 1 MB

/// Optical disc image filesystem browser.
///
/// Mirrors the pattern of the main `BrowseView` but uses the opticaldiscs
/// crate's `Filesystem` trait instead of rusty-backup's internal one.
#[allow(dead_code)]
pub struct OpticalDiscBrowseView {
    disc_path: Option<PathBuf>,
    disc_info: Option<DiscImageInfo>,
    root: Option<FileEntry>,
    directory_cache: HashMap<String, Vec<FileEntry>>,
    expanded_paths: HashSet<String>,
    selected_entry: Option<FileEntry>,
    content: Option<FileContent>,
    view_mode: ViewMode,
    error: Option<String>,
    active: bool,
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

impl Default for OpticalDiscBrowseView {
    fn default() -> Self {
        Self {
            disc_path: None,
            disc_info: None,
            root: None,
            directory_cache: HashMap::new(),
            expanded_paths: HashSet::new(),
            selected_entry: None,
            content: None,
            view_mode: ViewMode::Auto,
            error: None,
            active: false,
        }
    }
}

impl OpticalDiscBrowseView {
    /// Open a disc image for browsing.
    pub fn open(&mut self, path: &Path) {
        self.close();
        self.disc_path = Some(path.to_path_buf());

        match DiscImageInfo::open(path) {
            Ok(info) => {
                match open_disc_filesystem(&info) {
                    Ok(mut fs) => {
                        match fs.root() {
                            Ok(root) => {
                                match fs.list_directory(&root) {
                                    Ok(entries) => {
                                        self.directory_cache.insert("/".into(), entries);
                                        self.expanded_paths.insert("/".into());
                                    }
                                    Err(e) => {
                                        self.error =
                                            Some(format!("Failed to read root directory: {e}"));
                                    }
                                }
                                self.root = Some(root);
                            }
                            Err(e) => {
                                self.error = Some(format!("Failed to get root: {e}"));
                            }
                        }
                        self.active = true;
                    }
                    Err(e) => {
                        self.error = Some(format!("Cannot open filesystem: {e}"));
                        self.active = true;
                    }
                }
                self.disc_info = Some(info);
            }
            Err(e) => {
                self.error = Some(format!("Failed to open disc image: {e}"));
                self.active = true;
            }
        }
    }

    pub fn close(&mut self) {
        self.disc_path = None;
        self.disc_info = None;
        self.root = None;
        self.directory_cache.clear();
        self.expanded_paths.clear();
        self.selected_entry = None;
        self.content = None;
        self.error = None;
        self.active = false;
    }

    pub fn is_active(&self) -> bool {
        self.active
    }

    /// Create a new filesystem instance for each operation (don't cache state).
    fn open_fs(&self) -> Result<Box<dyn Filesystem>, FilesystemError> {
        let info = self
            .disc_info
            .as_ref()
            .ok_or_else(|| FilesystemError::Parse("no disc info available".into()))?;
        open_disc_filesystem(info)
    }

    pub fn show(&mut self, ui: &mut egui::Ui) {
        if !self.active {
            return;
        }

        // Header
        ui.horizontal(|ui| {
            ui.label(egui::RichText::new("Disc Browser").strong());
            if let Some(info) = &self.disc_info {
                ui.label(format!("[{}]", info.filesystem.display_name()));
                if let Some(ref label) = info.volume_label {
                    ui.label(format!("Label: {label}"));
                }
            }
            if ui.button("Close").clicked() {
                self.close();
            }
        });

        if let Some(err) = &self.error {
            ui.colored_label(egui::Color32::from_rgb(255, 100, 100), err);
        }

        ui.separator();

        // Two-panel layout: tree | content
        let available = ui.available_size();
        let tree_width = (available.x * 0.4).clamp(200.0, 400.0);
        let panel_height = available.y;

        ui.horizontal(|ui| {
            ui.set_min_height(panel_height);

            // Left panel: file tree
            ui.vertical(|ui| {
                ui.set_width(tree_width);
                ui.set_min_height(panel_height);
                egui::ScrollArea::vertical()
                    .id_salt("optical_browse_tree")
                    .max_height(panel_height)
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        if let Some(root) = self.root.clone() {
                            self.render_tree_entry(ui, &root);
                        }
                    });
            });

            ui.separator();

            // Right panel: file content
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

                let is_now_open = header.body_returned.is_some();

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
            return;
        }

        if let Ok(mut fs) = self.open_fs() {
            match fs.read_file(entry) {
                Ok(data) => {
                    self.content = Some(detect_content_type(&data));
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
                    ui.label(format!("Path: {}", entry.path));
                });

                ui.separator();

                if entry.size > MAX_PREVIEW_SIZE as u64 && entry.is_file() {
                    ui.label(format!(
                        "File too large to preview ({}).",
                        entry.size_string()
                    ));
                    return;
                }

                if entry.is_directory() {
                    return;
                }

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
                            .id_salt("optical_file_content")
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
                            .id_salt("optical_file_content")
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

fn detect_content_type(data: &[u8]) -> FileContent {
    if data.is_empty() {
        return FileContent::Text(String::new());
    }

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

    // Check if mostly printable bytes (80%+)
    let printable = data
        .iter()
        .filter(|&&b| b.is_ascii_graphic() || b.is_ascii_whitespace())
        .count();
    if printable * 10 >= data.len() * 8 {
        let text: String = data
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

fn render_hex_view(ui: &mut egui::Ui, data: &[u8]) {
    let bytes_per_line = 16;
    let lines = data.len().div_ceil(bytes_per_line);
    let max_lines = 256;

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
