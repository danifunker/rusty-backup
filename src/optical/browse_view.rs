use std::collections::{HashMap, HashSet};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use opticaldiscs::browse::entry::{EntryType, FileEntry};
use opticaldiscs::browse::filesystem::{Filesystem, FilesystemError};
use opticaldiscs::browse::open_disc_filesystem;
use opticaldiscs::detect::DiscImageInfo;
use opticaldiscs::formats::FilesystemType;

use crate::fs::resource_fork::{self, ResourceForkMode};

const MAX_PREVIEW_SIZE: usize = 1024 * 1024; // 1 MB

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
    resource_fork_mode: ResourceForkMode,
    extraction_progress: Option<Arc<Mutex<ExtractionProgress>>>,
    extraction_result: Option<String>,
    tree_text: Option<String>,
    show_tree_popup: bool,
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
            resource_fork_mode: ResourceForkMode::AppleDouble,
            extraction_progress: None,
            extraction_result: None,
            tree_text: None,
            show_tree_popup: false,
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
        self.extraction_progress = None;
        self.extraction_result = None;
        self.tree_text = None;
        self.show_tree_popup = false;
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

    /// Whether the disc filesystem is HFS or HFS+.
    fn is_hfs_type(&self) -> bool {
        self.disc_info
            .as_ref()
            .map(|info| {
                matches!(
                    info.filesystem,
                    FilesystemType::Hfs | FilesystemType::HfsPlus
                )
            })
            .unwrap_or(false)
    }

    pub fn show(&mut self, ui: &mut egui::Ui) {
        if !self.active {
            return;
        }

        self.poll_extraction(ui);

        // Header
        ui.horizontal(|ui| {
            ui.label(egui::RichText::new("Disc Browser").strong());
            if let Some(info) = &self.disc_info {
                ui.label(format!("[{}]", info.filesystem.display_name()));
                if let Some(ref label) = info.volume_label {
                    ui.label(format!("Label: {label}"));
                }
            }
            if ui.button("Tree").clicked() {
                self.generate_tree_text();
            }
            if ui.button("Close").clicked() {
                self.close();
            }
        });

        self.render_tree_popup(ui);

        if let Some(err) = &self.error {
            ui.colored_label(egui::Color32::from_rgb(255, 100, 100), err);
        }

        if let Some(ref msg) = self.extraction_result {
            ui.colored_label(egui::Color32::from_rgb(100, 200, 100), msg);
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
                    if let Some(ref tc) = entry.type_code {
                        ui.label(format!("Type: {tc}"));
                    }
                    if let Some(ref cc) = entry.creator_code {
                        ui.label(format!("Creator: {cc}"));
                    }
                    if let Some(rsrc) = entry.resource_fork_size {
                        if rsrc > 0 {
                            ui.label(format!("Rsrc: {}", format_size(rsrc)));
                        }
                    }
                    ui.label(format!("Path: {}", entry.path));
                });

                // Extract controls
                let extraction_running = self.extraction_progress.is_some();

                if (entry.is_file() || entry.is_directory()) && !extraction_running {
                    ui.horizontal(|ui| {
                        // Resource fork mode dropdown (HFS/HFS+ only)
                        if self.is_hfs_type() {
                            ui.label("Resource forks:");
                            let current_label = self.resource_fork_mode.label();
                            egui::ComboBox::from_id_salt("optical_rsrc_mode")
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

    fn generate_tree_text(&mut self) {
        match self.open_fs() {
            Ok(mut fs) => match generate_optical_tree(&mut *fs) {
                Ok(text) => {
                    self.tree_text = Some(text);
                    self.show_tree_popup = true;
                }
                Err(e) => {
                    self.error = Some(format!("Failed to generate tree: {e}"));
                }
            },
            Err(e) => {
                self.error = Some(format!("Failed to open filesystem: {e}"));
            }
        }
    }

    fn render_tree_popup(&mut self, ui: &mut egui::Ui) {
        if !self.show_tree_popup {
            return;
        }
        let mut open = true;
        let mut save_requested = false;
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
                        self.error = Some(format!("Failed to save tree: {e}"));
                    }
                }
            }
        }
    }

    /// Start extracting the selected entry to a user-chosen folder.
    fn start_extraction(&mut self, entry: &FileEntry) {
        let dest = match rfd::FileDialog::new()
            .set_title("Extract to folder")
            .pick_folder()
        {
            Some(d) => d,
            None => return,
        };

        let entry = entry.clone();
        let disc_path = self.disc_path.clone().unwrap();
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
            let result = (|| -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                let info = DiscImageInfo::open(&disc_path)?;

                // Count files and bytes for progress
                let mut counting_fs = open_disc_filesystem(&info)?;
                let (total_files, total_bytes) = count_entry(&mut *counting_fs, &entry)?;
                if let Ok(mut p) = progress.lock() {
                    p.total_files = total_files;
                    p.total_bytes = total_bytes;
                }
                drop(counting_fs);

                // Open a fresh filesystem for extraction
                let mut fs = open_disc_filesystem(&info)?;
                extract_entry(
                    &mut *fs,
                    &entry,
                    &dest,
                    resource_fork_mode,
                    is_hfs,
                    &progress,
                )?;

                Ok(())
            })();

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

/// Generate a GNU tree-style listing of all files and directories.
fn generate_optical_tree(fs: &mut dyn Filesystem) -> Result<String, FilesystemError> {
    let root = fs.root()?;
    let label = fs.volume_name().unwrap_or("/").to_owned();
    let mut out = String::new();
    out.push_str(&label);
    out.push('\n');
    let mut dir_count: u64 = 0;
    let mut file_count: u64 = 0;
    walk_optical_tree(fs, &root, "", &mut out, &mut dir_count, &mut file_count)?;
    out.push_str(&format!(
        "\n{} directories, {} files\n",
        dir_count, file_count
    ));
    Ok(out)
}

fn walk_optical_tree(
    fs: &mut dyn Filesystem,
    dir: &FileEntry,
    prefix: &str,
    out: &mut String,
    dir_count: &mut u64,
    file_count: &mut u64,
) -> Result<(), FilesystemError> {
    let children = fs.list_directory(dir)?;
    let count = children.len();

    for (i, child) in children.iter().enumerate() {
        let is_last = i == count - 1;
        let connector = if is_last { "└── " } else { "├── " };

        out.push_str(prefix);
        out.push_str(connector);
        out.push_str(&child.name);

        if child.is_file() {
            let total = child.total_size();
            out.push_str(&format!("  [{}]", format_size(total)));
            if let Some(rsrc) = child.resource_fork_size {
                if rsrc > 0 {
                    out.push_str(&format!(" (rsrc: {})", format_size(rsrc)));
                }
            }
            if let Some(ref tc) = child.type_code {
                out.push_str(&format!("  {tc}"));
                if let Some(ref cc) = child.creator_code {
                    out.push_str(&format!("/{cc}"));
                }
            }
        }

        out.push('\n');

        if child.is_directory() {
            *dir_count += 1;
            let child_prefix = if is_last {
                format!("{prefix}    ")
            } else {
                format!("{prefix}│   ")
            };
            walk_optical_tree(fs, child, &child_prefix, out, dir_count, file_count)?;
        } else {
            *file_count += 1;
        }
    }

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
    }
}

/// Recursively extract an entry to the destination path.
fn extract_entry(
    fs: &mut dyn Filesystem,
    entry: &FileEntry,
    dest: &Path,
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
            if let Ok(mut p) = progress.lock() {
                p.current_file = entry.path.clone();
            }

            let has_rsrc = is_hfs && entry.resource_fork_size.map(|s| s > 0).unwrap_or(false);

            if has_rsrc && resource_fork_mode == ResourceForkMode::MacBinary {
                // MacBinary: single .bin file containing both forks
                let data = fs.read_file(entry)?;
                let rsrc_data = fs.read_resource_fork(entry)?.unwrap_or_default();

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
                    &rsrc_data,
                );
                let out_path = dest.join(format!("{safe_name}.bin"));
                let mut f = BufWriter::new(std::fs::File::create(&out_path)?);
                f.write_all(&mb)?;
                f.flush()?;

                if let Ok(mut p) = progress.lock() {
                    p.current_bytes += data.len() as u64 + rsrc_data.len() as u64;
                    p.files_extracted += 1;
                }
            } else {
                // Write data fork
                let data = fs.read_file(entry)?;
                let out_path = dest.join(&safe_name);
                let mut f = BufWriter::new(std::fs::File::create(&out_path)?);
                f.write_all(&data)?;
                f.flush()?;

                if let Ok(mut p) = progress.lock() {
                    p.current_bytes += data.len() as u64;
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

                    let rsrc_data = fs.read_resource_fork(entry)?.unwrap_or_default();

                    match resource_fork_mode {
                        ResourceForkMode::AppleDouble => {
                            let ad = resource_fork::build_appledouble(
                                &type_code,
                                &creator_code,
                                &rsrc_data,
                            );
                            let ad_path = dest.join(format!("._{safe_name}"));
                            let mut af = BufWriter::new(std::fs::File::create(&ad_path)?);
                            af.write_all(&ad)?;
                            af.flush()?;
                        }
                        ResourceForkMode::SeparateRsrc => {
                            let rsrc_path = dest.join(format!("{safe_name}.rsrc"));
                            let mut rf = BufWriter::new(std::fs::File::create(&rsrc_path)?);
                            rf.write_all(&rsrc_data)?;
                            rf.flush()?;
                        }
                        _ => {}
                    }

                    if let Ok(mut p) = progress.lock() {
                        p.current_bytes += rsrc_data.len() as u64;
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

/// Human-friendly size string.
fn format_size(bytes: u64) -> String {
    match bytes {
        s if s < 1_024 => format!("{} B", s),
        s if s < 1_024 * 1_024 => format!("{:.1} KB", s as f64 / 1_024.0),
        s if s < 1_024 * 1_024 * 1_024 => format!("{:.1} MB", s as f64 / (1_024.0 * 1_024.0)),
        s => format!("{:.2} GB", s as f64 / (1_024.0 * 1_024.0 * 1_024.0)),
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
