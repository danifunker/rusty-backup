use std::collections::{HashMap, HashSet};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use opticaldiscs::browse::entry::{EntryType, FileEntry};
use opticaldiscs::browse::filesystem::{Filesystem, FilesystemError};
use opticaldiscs::browse::{open_disc_filesystem, open_hybrid_filesystem};
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
    /// Selected filesystem on a hybrid disc: `0` = the primary, `N` =
    /// `disc_info.hybrid_filesystems[N - 1]` (the Mac HFS side). Always `0` for
    /// an ordinary single-filesystem disc.
    selected_fs: usize,
    root: Option<FileEntry>,
    directory_cache: HashMap<String, Vec<FileEntry>>,
    expanded_paths: HashSet<String>,
    selected_entry: Option<FileEntry>,
    /// Checkbox-marked entries for multi-select export to a single archive,
    /// keyed by path. Independent of the single-click `selected_entry` preview.
    marked: std::collections::BTreeMap<String, FileEntry>,
    /// Chosen output format for the "Export selected" pulldown.
    export_format: crate::fs::export_selection::ExportFormat,
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
            selected_fs: 0,
            root: None,
            directory_cache: HashMap::new(),
            expanded_paths: HashSet::new(),
            selected_entry: None,
            marked: std::collections::BTreeMap::new(),
            export_format: crate::fs::export_selection::ExportFormat::MacArchive,
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
        self.selected_fs = 0;

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
                        // Standalone NKit v1 discs open fine (opticaldiscs
                        // reconstructs them); if an NKit image reaches here it
                        // couldn't be reconstructed (v2 / corrupt), so append the
                        // convert-it-first hint to the specific reader error,
                        // matching the Commander and `optical browse` paths.
                        let msg = crate::cli::optical_hint::with_nkit_hint(
                            anyhow::anyhow!("Cannot open filesystem: {e}"),
                            path,
                        );
                        self.error = Some(format!("{msg:#}"));
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
        self.selected_fs = 0;
        self.root = None;
        self.directory_cache.clear();
        self.expanded_paths.clear();
        self.selected_entry = None;
        self.marked.clear();
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
    /// Honors the hybrid-disc selection: `selected_fs == 0` is the primary,
    /// otherwise the co-resident HFS partition at index `selected_fs - 1`.
    fn open_fs(&self) -> Result<Box<dyn Filesystem>, FilesystemError> {
        let info = self
            .disc_info
            .as_ref()
            .ok_or_else(|| FilesystemError::Parse("no disc info available".into()))?;
        if self.selected_fs == 0 {
            open_disc_filesystem(info)
        } else {
            open_hybrid_filesystem(info, self.selected_fs - 1)
        }
    }

    /// The [`FilesystemType`] currently selected for browsing (primary or, on a
    /// hybrid disc, the chosen HFS side).
    fn selected_fs_type(&self) -> Option<FilesystemType> {
        let info = self.disc_info.as_ref()?;
        if self.selected_fs == 0 {
            Some(info.filesystem)
        } else {
            info.hybrid_filesystems
                .get(self.selected_fs - 1)
                .map(|h| h.filesystem)
        }
    }

    /// Whether the *selected* filesystem is HFS or HFS+ (gates the resource-fork
    /// controls), so browsing the Mac side of a hybrid disc enables them.
    fn is_hfs_type(&self) -> bool {
        matches!(
            self.selected_fs_type(),
            Some(FilesystemType::Hfs | FilesystemType::HfsPlus)
        )
    }

    /// Switch which filesystem is browsed on a hybrid disc, re-reading the root
    /// from the newly selected volume. No-op if `idx` is already selected.
    fn select_fs(&mut self, idx: usize) {
        if idx == self.selected_fs {
            return;
        }
        self.selected_fs = idx;
        // Drop everything tied to the previous volume, then re-read the root.
        self.directory_cache.clear();
        self.expanded_paths.clear();
        self.selected_entry = None;
        self.marked.clear();
        self.content = None;
        self.error = None;
        match self.open_fs() {
            Ok(mut fs) => match fs.root() {
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
                Err(e) => self.error = Some(format!("Failed to get root: {e}")),
            },
            Err(e) => self.error = Some(format!("Cannot open filesystem: {e}")),
        }
    }

    /// Human label for one selectable filesystem index (for the picker).
    fn fs_choice_label(&self, idx: usize) -> String {
        let Some(info) = self.disc_info.as_ref() else {
            return "?".into();
        };
        if idx == 0 {
            match &info.volume_label {
                Some(l) => format!("{} ({l})", info.filesystem.display_name()),
                None => info.filesystem.display_name().to_string(),
            }
        } else if let Some(h) = info.hybrid_filesystems.get(idx - 1) {
            let name = h.filesystem.display_name();
            match &h.volume_label {
                Some(l) => format!("{name} ({l})"),
                None => name.to_string(),
            }
        } else {
            "?".into()
        }
    }

    pub fn show(&mut self, ui: &mut egui::Ui) {
        if !self.active {
            return;
        }

        self.poll_extraction(ui);

        // Header. On a hybrid Mac/PC disc (>1 selectable filesystem) the plain
        // filesystem label becomes a picker; precompute the labels so the combo
        // closure doesn't re-borrow `self`.
        let num_choices = self
            .disc_info
            .as_ref()
            .map(|i| 1 + i.hybrid_filesystems.len())
            .unwrap_or(0);
        let choice_labels: Vec<String> =
            (0..num_choices).map(|i| self.fs_choice_label(i)).collect();
        let game_label = self
            .disc_info
            .as_ref()
            .and_then(|i| i.game.as_ref())
            .map(crate::optical::format_game_identity);
        let mut new_selection: Option<usize> = None;
        ui.horizontal(|ui| {
            ui.label(egui::RichText::new("Disc Browser").strong());
            if num_choices > 1 {
                ui.label("Filesystem:");
                egui::ComboBox::from_id_salt("optical_fs_picker")
                    .selected_text(choice_labels[self.selected_fs].clone())
                    .show_ui(ui, |ui| {
                        for (idx, label) in choice_labels.iter().enumerate() {
                            if ui
                                .selectable_label(idx == self.selected_fs, label)
                                .clicked()
                            {
                                new_selection = Some(idx);
                            }
                        }
                    });
            } else if num_choices == 1 {
                ui.label(format!("[{}]", choice_labels[0]));
            }
            if let Some(g) = &game_label {
                ui.label(format!("Game: {g}"));
            }
            if ui.button("Tree").clicked() {
                self.generate_tree_text();
            }
            if ui.button("Close").clicked() {
                self.close();
            }
        });
        if let Some(idx) = new_selection {
            self.select_fs(idx);
        }

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
                self.render_selection_bar(ui);
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

                let is_selected = self
                    .selected_entry
                    .as_ref()
                    .map(|s| s.path == entry.path)
                    .unwrap_or(false);

                // A CollapsingState (toggle triangle + a separately clickable
                // label) rather than a plain CollapsingHeader, so a directory —
                // the disc root included — can be *selected* for "Extract
                // Folder..." while the triangle still expands it. display_name()
                // renders the empty-named root as "/".
                let id = ui.make_persistent_id(&path);
                let mut state = egui::collapsing_header::CollapsingState::load_with_default_open(
                    ui.ctx(),
                    id,
                    path == "/",
                );

                let header_res = ui.horizontal(|ui| {
                    state.show_toggle_button(ui, egui::collapsing_header::paint_default_icon);
                    self.mark_checkbox(ui, entry);
                    if ui
                        .selectable_label(is_selected, display_name(entry))
                        .clicked()
                    {
                        self.select_dir(entry);
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
                });

                let is_now_open = state.is_open();
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
                let label = match &entry.symlink_target {
                    Some(target) => format!("{} -> {}  ({})", entry.name, target, size_str),
                    None => format!("{}  ({})", entry.name, size_str),
                };

                ui.horizontal(|ui| {
                    self.mark_checkbox(ui, entry);
                    if ui.selectable_label(is_selected, &label).clicked() {
                        self.select_file(entry);
                    }
                });
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

    /// Select a directory (for "Extract Folder..."). A directory has no content
    /// to preview, so just record the selection and clear any stale content.
    fn select_dir(&mut self, entry: &FileEntry) {
        self.selected_entry = Some(entry.clone());
        self.content = None;
        self.error = None;
    }

    /// Render the multi-select checkbox for `entry`, toggling its mark. Marked
    /// entries feed the "Export selected..." bar; independent of the single-click
    /// `selected_entry` preview.
    fn mark_checkbox(&mut self, ui: &mut egui::Ui, entry: &FileEntry) {
        let mut marked = self.marked.contains_key(&entry.path);
        if ui
            .checkbox(&mut marked, "")
            .on_hover_text("Mark for multi-select export")
            .changed()
        {
            if marked {
                self.marked.insert(entry.path.clone(), entry.clone());
            } else {
                self.marked.remove(&entry.path);
            }
        }
    }

    /// Marked entries to export, with any entry that lives under another marked
    /// directory dropped — that ancestor's recursive walk already includes it, so
    /// nothing is archived twice.
    fn marked_export_entries(&self) -> Vec<FileEntry> {
        fn is_ancestor(a: &str, p: &str) -> bool {
            if a == p {
                return false;
            }
            if a == "/" {
                return p.len() > 1 && p.starts_with('/');
            }
            p.starts_with(a) && p.as_bytes().get(a.len()) == Some(&b'/')
        }
        let marked_paths: Vec<&str> = self.marked.keys().map(String::as_str).collect();
        self.marked
            .iter()
            .filter(|(p, _)| !marked_paths.iter().any(|a| is_ancestor(a, p)))
            .map(|(_, e)| e.clone())
            .collect()
    }

    /// Bar shown above the tree when entries are checkbox-marked: the count, a
    /// format pulldown, an Export button, and Clear.
    fn render_selection_bar(&mut self, ui: &mut egui::Ui) {
        use crate::fs::export_selection::ExportFormat;
        if self.marked.is_empty() {
            return;
        }
        let mut fmt = self.export_format;
        let mut do_export = false;
        let mut clear = false;
        ui.horizontal_wrapped(|ui| {
            ui.label(format!("{} selected", self.marked.len()));
            ui.label("Export as:");
            egui::ComboBox::from_id_salt("optical_export_fmt")
                .selected_text(fmt.label())
                .show_ui(ui, |ui| {
                    for f in ExportFormat::ALL {
                        ui.selectable_value(&mut fmt, f, f.label());
                    }
                });
            if ui.button("Export...").clicked() {
                do_export = true;
            }
            if ui.button("Clear").clicked() {
                clear = true;
            }
        });
        ui.separator();
        self.export_format = fmt;
        if clear {
            self.marked.clear();
        }
        if do_export {
            self.export_marked(fmt);
        }
    }

    /// Export the checkbox-marked entries together as one `format` output. The CD
    /// browser holds opticaldiscs' own FileEntry, so it wraps the live filesystem
    /// in a rusty-backup `OpticalFilesystem` (which resolves entries by path) and
    /// runs the shared multi-entry `export_selection` engine over the translated
    /// entries — giving the same format list as the Inspect tab, forks included.
    /// Foreground execution; classic-Mac payloads are typically small.
    fn export_marked(&mut self, format: crate::fs::export_selection::ExportFormat) {
        use crate::fs::export_selection::{export_to_file, export_to_folder};
        let entries = self.marked_export_entries();
        if entries.is_empty() {
            self.error = Some("Nothing selected to export.".into());
            return;
        }
        let fork_mode = self.resource_fork_mode;
        let rb_entries: Vec<crate::fs::entry::FileEntry> = entries
            .iter()
            .map(crate::fs::optical_fs::translate)
            .collect();
        let fs_type = self
            .selected_fs_type()
            .map(|t| format!("{t:?}"))
            .unwrap_or_default();
        let noop = |_: &str, _: usize, _: u64| {};
        let nocancel = || false;

        let (path, is_folder) = if format.is_single_file() {
            let ext = format
                .file_extension()
                .map(|e| format!(".{e}"))
                .unwrap_or_default();
            let name = format!("selection{ext}");
            match rfd::FileDialog::new()
                .set_title("Export selection")
                .set_file_name(&name)
                .save_file()
            {
                Some(p) => (p, false),
                None => return,
            }
        } else {
            match rfd::FileDialog::new()
                .set_title("Export selection to folder")
                .pick_folder()
            {
                Some(p) => (p, true),
                None => return,
            }
        };

        let result = (|| -> Result<crate::fs::export_selection::ExportSummary, String> {
            let inner = self.open_fs().map_err(|e| e.to_string())?;
            let mut rb_fs =
                crate::fs::optical_fs::OpticalFilesystem::from_inner(inner, fs_type, None)
                    .map_err(|e| e.to_string())?;
            if is_folder {
                export_to_folder(
                    &mut rb_fs,
                    &rb_entries,
                    &path,
                    format,
                    fork_mode,
                    &noop,
                    &nocancel,
                )
                .map_err(|e| e.to_string())
            } else {
                export_to_file(&mut rb_fs, &rb_entries, &path, format, &noop, &nocancel)
                    .map_err(|e| e.to_string())
            }
        })();

        match result {
            Ok(s) => {
                self.extraction_result = Some(format!(
                    "Exported {} item(s) to {}",
                    s.files,
                    path.display()
                ));
            }
            Err(e) => self.error = Some(format!("Export failed: {e}")),
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
                ui.label(egui::RichText::new(display_name(&entry)).strong());
                ui.horizontal(|ui| {
                    ui.label(format!("Size: {}", entry.size_string()));
                    if let Some(tc) = entry.type_code_string() {
                        ui.label(format!("Type: {tc}"));
                    }
                    if let Some(cc) = entry.creator_code_string() {
                        ui.label(format!("Creator: {cc}"));
                    }
                    if let Some(rsrc) = entry.resource_fork_size {
                        if rsrc > 0 {
                            ui.label(format!("Rsrc: {}", format_size(rsrc)));
                        }
                    }
                    if let Some(ref target) = entry.symlink_target {
                        ui.label(format!("-> {target}"));
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
            let _wake = crate::os::wakelock::acquire("Rusty Backup: extract optical disc files");
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
                extract_entry(&mut *fs, &entry, &dest, resource_fork_mode, &progress)?;

                Ok(())
            })();

            if let Ok(mut p) = progress.lock() {
                p.finished = true;
                if let Err(e) = result {
                    let msg = crate::cli::optical_hint::with_nkit_hint(
                        anyhow::anyhow!("{e}"),
                        &disc_path,
                    );
                    p.error = Some(format!("{msg:#}"));
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
        // ASCII tree connectors so the egui Monospace text-edit (and any
        // other consumer that doesn't have full Unicode font coverage)
        // renders correctly. CLAUDE.md "no Unicode glyphs in UI text" rule.
        let connector = if is_last { "\\-- " } else { "|-- " };

        out.push_str(prefix);
        out.push_str(connector);
        out.push_str(&child.name);

        if let Some(ref target) = child.symlink_target {
            out.push_str(&format!(" -> {target}"));
        }

        if child.is_file() {
            let total = child.total_size();
            out.push_str(&format!("  [{}]", format_size(total)));
            if let Some(rsrc) = child.resource_fork_size {
                if rsrc > 0 {
                    out.push_str(&format!(" (rsrc: {})", format_size(rsrc)));
                }
            }
            if let Some(tc) = child.type_code_string() {
                out.push_str(&format!("  {tc}"));
                if let Some(cc) = child.creator_code_string() {
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
                // ASCII vertical-bar continuation, paired with the ASCII
                // `|--` / `\\--` connectors above. Keeps the tree readable
                // in egui's default font (no Unicode box-drawing glyphs).
                format!("{prefix}|   ")
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

            // A non-zero resource-fork size only appears on fork-capable
            // filesystems (HFS / HFS+); ISO 9660 reports `None`. This covers
            // every fork filesystem opticaldiscs supports without re-checking
            // the filesystem type.
            let has_rsrc = entry.resource_fork_size.map(|s| s > 0).unwrap_or(false);

            if has_rsrc && resource_fork_mode == ResourceForkMode::MacBinary {
                // MacBinary: single .bin file containing both forks
                let data = fs.read_file(entry)?;
                let rsrc_data = fs.read_resource_fork(entry)?.unwrap_or_default();

                let type_code = entry.type_code.unwrap_or([0; 4]);
                let creator_code = entry.creator_code.unwrap_or([0; 4]);
                let dates = crate::optical::mac_dates_from(&entry.timestamps);

                let mb = resource_fork::build_macbinary(
                    &safe_name,
                    &type_code,
                    &creator_code,
                    dates,
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
                    let type_code = entry.type_code.unwrap_or([0; 4]);
                    let creator_code = entry.creator_code.unwrap_or([0; 4]);
                    let dates = crate::optical::mac_dates_from(&entry.timestamps);

                    let rsrc_data = fs.read_resource_fork(entry)?.unwrap_or_default();

                    match resource_fork_mode {
                        ResourceForkMode::Native => {
                            let rsrc_path = out_path.join("..namedfork/rsrc");
                            let mut rf = BufWriter::new(std::fs::File::create(&rsrc_path)?);
                            rf.write_all(&rsrc_data)?;
                            rf.flush()?;
                        }
                        ResourceForkMode::AppleDouble => {
                            let ad = resource_fork::build_appledouble(
                                &type_code,
                                &creator_code,
                                dates,
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
                extract_entry(fs, child, &dir_path, resource_fork_mode, progress)?;
            }
        }
    }

    Ok(())
}

/// User-facing name for an entry: opticaldiscs names the disc root "" (empty),
/// so surface it as "/". Scoped to the root (empty name AND path "/"), so a
/// future opticaldiscs that names the root passes its real name through unchanged.
fn display_name(entry: &FileEntry) -> &str {
    if entry.name.is_empty() && entry.path == "/" {
        "/"
    } else {
        &entry.name
    }
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

#[cfg(test)]
mod nkit_hint_tests {
    use super::*;
    use std::io::Write;

    /// A standalone NKit v1 GameCube ISO opens directly (opticaldiscs 0.12+
    /// reconstructs it); an NKit image that CAN'T be reconstructed (v2 / corrupt
    /// — here a truncated stub) must surface the actionable "convert it" hint in
    /// the GUI Optical browser, not a bare reader error. Regression for the
    /// Optical tab, which called `open_disc_filesystem` without the hint the
    /// Commander/CLI paths apply.
    #[test]
    fn open_unreconstructable_nkit_iso_shows_actionable_hint() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("game.nkit.iso");
        // GameCube magic at 0x1C so it identifies as a Nintendo disc, + the NKIT
        // marker at 0x200 — but no real reconstructable body, so the reader fails.
        let mut buf = vec![0u8; 4 * 1024 * 1024];
        buf[0x1C..0x20].copy_from_slice(&[0xC2, 0x33, 0x9F, 0x3D]);
        buf[0x200..0x204].copy_from_slice(b"NKIT");
        std::fs::File::create(&path)
            .unwrap()
            .write_all(&buf)
            .unwrap();

        let mut view = OpticalDiscBrowseView::default();
        view.open(&path);
        let err = view
            .error
            .expect("an unreconstructable NKit image must report an error");
        assert!(
            err.contains("NKit v1"),
            "expected the NKit hint, got: {err}"
        );
        assert!(
            err.contains("Convert it back"),
            "expected actionable advice, got: {err}"
        );
    }
}
