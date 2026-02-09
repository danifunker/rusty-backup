use fltk::{prelude::*, *};
use rusty_backup::fs::entry::{EntryType, FileEntry};
use rusty_backup::fs::filesystem::Filesystem;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Separate window for browsing filesystem contents with preview
pub struct FilesystemBrowserWindow {
    wind: window::Window,
    tree: tree::Tree,
    preview_buffer: text::TextBuffer,
    preview_display: text::TextDisplay,
    path_display: frame::Frame,
    export_btn: button::Button,
    current_path: Arc<Mutex<String>>,
    filesystem: Arc<Mutex<Option<Box<dyn Filesystem>>>>,
    volume_label: String,
    // Cache for file entries by tree path
    entry_cache: Arc<Mutex<HashMap<String, FileEntry>>>,
}

impl FilesystemBrowserWindow {
    pub fn new(
        window_title: &str,
        volume_label: &str,
        filesystem: Arc<Mutex<Option<Box<dyn Filesystem>>>>,
    ) -> Self {
        let wind = window::Window::default()
            .with_size(900, 700)
            .with_label(window_title);

        // Top bar with current path
        let mut path_display = frame::Frame::new(10, 10, 880, 30, None);
        path_display.set_label(&format!("Path: {}/", volume_label));
        path_display.set_align(enums::Align::Left | enums::Align::Inside);
        path_display.set_frame(enums::FrameType::DownBox);

        // Left side: Tree browser for navigation
        let mut tree = tree::Tree::new(10, 50, 440, 580, None);
        tree.set_show_root(true);
        tree.set_select_mode(tree::TreeSelect::Single);

        // Right side: Preview pane
        frame::Frame::new(460, 50, 430, 20, "Preview:")
            .with_align(enums::Align::Left | enums::Align::Inside);

        let preview_buffer = text::TextBuffer::default();
        let mut preview_display = text::TextDisplay::new(460, 75, 430, 505, None);
        preview_display.set_buffer(preview_buffer.clone());
        preview_display.wrap_mode(text::WrapMode::None, 0);
        preview_display.set_text_font(enums::Font::Courier);
        preview_display.set_text_size(11);

        // File info line
        let mut info_frame = frame::Frame::new(460, 585, 430, 45, "Select a file to preview");
        info_frame.set_align(enums::Align::Left | enums::Align::Inside | enums::Align::Wrap);
        info_frame.set_label_size(11);

        // Bottom button bar
        let mut export_btn = button::Button::new(10, 640, 100, 30, "Export...");
        export_btn.deactivate(); // Enable when file selected

        let refresh_btn = button::Button::new(120, 640, 100, 30, "Refresh");

        let mut close_btn = button::Button::new(790, 640, 100, 30, "Close");

        // Setup close button callback before wind.end()
        close_btn.set_callback({
            let mut w = wind.clone();
            move |_| {
                w.hide();
            }
        });

        wind.end();

        let current_path = Arc::new(Mutex::new(String::from("/")));
        let entry_cache = Arc::new(Mutex::new(HashMap::new()));

        let mut browser = Self {
            wind,
            tree,
            preview_buffer,
            preview_display,
            path_display,
            export_btn,
            current_path,
            filesystem,
            volume_label: volume_label.to_string(),
            entry_cache,
        };

        // Load root directory
        browser.load_root();
        browser.setup_callbacks();

        browser
    }

    fn load_root(&mut self) {
        // Clear existing tree and cache
        self.tree.clear();
        if let Ok(mut cache) = self.entry_cache.lock() {
            cache.clear();
        }

        // Get filesystem and load root
        if let Ok(mut fs_guard) = self.filesystem.lock() {
            if let Some(ref mut fs) = *fs_guard {
                match fs.root() {
                    Ok(root) => {
                        self.tree.begin();

                        // Use volume label for root node
                        let root_label = if self.volume_label.is_empty() {
                            "ROOT"
                        } else {
                            &self.volume_label
                        };

                        // Recursively load entire directory structure
                        Self::load_directory_recursive(
                            &mut self.tree,
                            &self.entry_cache,
                            fs.as_mut(),
                            &root,
                            root_label,
                        );

                        self.tree.end();
                    }
                    Err(_e) => {
                        // Failed to get root
                    }
                }
            }
        }

        self.tree.redraw();

        // Close all items first
        if let Some(mut item) = self.tree.first() {
            loop {
                item.close();
                if let Some(next) = item.next() {
                    item = next;
                } else {
                    break;
                }
            }
        }

        // Then expand ROOT and the volume level to show root directory contents
        let root_label = if self.volume_label.is_empty() {
            "ROOT"
        } else {
            &self.volume_label
        };

        // Open ROOT first
        if self.tree.open("ROOT", false).is_ok() {
            // Then open the volume level inside ROOT
            let volume_path = format!("ROOT/{}", root_label);
            let _ = self.tree.open(&volume_path, false);
        }

        self.tree.redraw();
    }

    fn load_directory_recursive(
        tree: &mut tree::Tree,
        cache: &Arc<Mutex<HashMap<String, FileEntry>>>,
        fs: &mut dyn Filesystem,
        dir_entry: &FileEntry,
        tree_path: &str,
    ) {
        // Load directory contents
        match fs.list_directory(dir_entry) {
            Ok(entries) => {
                for entry in entries {
                    let icon = if entry.entry_type == EntryType::Directory {
                        "📁 "
                    } else {
                        "📄 "
                    };
                    let label = format!("{}{}", icon, entry.name);
                    let full_path = format!("{}/{}", tree_path, label);

                    if let Some(_item) = tree.add(&full_path) {
                        // Cache the entry with the full path
                        if let Ok(mut c) = cache.lock() {
                            c.insert(full_path.clone(), entry.clone());
                        }

                        // Recursively load subdirectories
                        if entry.entry_type == EntryType::Directory {
                            Self::load_directory_recursive(tree, cache, fs, &entry, &full_path);
                        }
                    }
                }
            }
            Err(_e) => {
                // Failed to read directory
            }
        }
    }

    fn setup_callbacks(&mut self) {
        // Tree selection callback - for single-click preview
        self.tree.set_callback({
            let mut preview_buffer = self.preview_buffer.clone();
            let mut export_btn = self.export_btn.clone();
            let fs = self.filesystem.clone();
            let cache = self.entry_cache.clone();

            move |tree| {
                if let Some(item) = tree.first_selected_item() {
                    // Get full path in tree
                    let path = match tree.item_pathname(&item) {
                        Ok(p) => p,
                        Err(_) => return,
                    };

                    // Skip if just the root node
                    if path == "ROOT" || path.ends_with(" (>32MB)") || path.ends_with(" (<32MB)") {
                        preview_buffer.set_text("");
                        export_btn.deactivate();
                        return;
                    }

                    // Strip "ROOT/" prefix if present
                    let cache_key = if let Some(stripped) = path.strip_prefix("ROOT/") {
                        stripped
                    } else {
                        &path
                    };

                    // Get entry from cache
                    let entry_opt = if let Ok(cache) = cache.lock() {
                        cache.get(cache_key).cloned()
                    } else {
                        None
                    };

                    if let Some(entry) = entry_opt {
                        if entry.entry_type == EntryType::Directory {
                            // Directory - show info in preview
                            export_btn.deactivate();
                            preview_buffer.set_text(&format!(
                                "Directory: {}\n\nDouble-click to expand.",
                                entry.name
                            ));
                        } else {
                            // File - enable export and show preview
                            export_btn.activate();

                            // Load file preview
                            if let Ok(mut fs_guard) = fs.lock() {
                                if let Some(ref mut filesystem) = *fs_guard {
                                    match filesystem.read_file(&entry, 65536) {
                                        Ok(data) => {
                                            // Try to display as text, otherwise hex dump
                                            if let Ok(text) = String::from_utf8(data.clone()) {
                                                preview_buffer.set_text(&format!(
                                                    "File: {} ({} bytes)\n\n{}",
                                                    entry.name, entry.size, text
                                                ));
                                            } else {
                                                // Show hex dump
                                                let hex = format_hex_dump(&data);
                                                preview_buffer.set_text(&format!(
                                                    "File: {} ({} bytes) [Binary]\n\n{}",
                                                    entry.name, entry.size, hex
                                                ));
                                            }
                                        }
                                        Err(e) => {
                                            preview_buffer
                                                .set_text(&format!("Error reading file: {}", e));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        // Export button callback
        self.export_btn.set_callback({
            let tree = self.tree.clone();
            let fs = self.filesystem.clone();
            let cache = self.entry_cache.clone();

            move |_| {
                if let Some(item) = tree.first_selected_item() {
                    // Get full path in tree
                    let path = match tree.item_pathname(&item) {
                        Ok(p) => p,
                        Err(_) => return,
                    };

                    // Strip "ROOT/" prefix if present
                    let cache_key = if let Some(stripped) = path.strip_prefix("ROOT/") {
                        stripped
                    } else {
                        &path
                    };

                    // Get entry from cache
                    let entry_opt = if let Ok(cache) = cache.lock() {
                        cache.get(cache_key).cloned()
                    } else {
                        None
                    };

                    if let Some(entry) = entry_opt {
                        if entry.entry_type != EntryType::Directory {
                            // Show file save dialog
                            if let Some(save_path) = rfd::FileDialog::new()
                                .set_title("Export File")
                                .set_file_name(&entry.name)
                                .save_file()
                            {
                                // Read full file and save
                                if let Ok(mut fs_guard) = fs.lock() {
                                    if let Some(ref mut filesystem) = *fs_guard {
                                        match filesystem.read_file(&entry, usize::MAX) {
                                            Ok(data) => match std::fs::write(&save_path, &data) {
                                                Ok(_) => {
                                                    dialog::message_default(&format!(
                                                        "Exported {} ({} bytes)",
                                                        entry.name,
                                                        data.len()
                                                    ));
                                                }
                                                Err(e) => {
                                                    dialog::message_default(&format!(
                                                        "Failed to write file: {}",
                                                        e
                                                    ));
                                                }
                                            },
                                            Err(e) => {
                                                dialog::message_default(&format!(
                                                    "Failed to read file: {}",
                                                    e
                                                ));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    pub fn show(&mut self) {
        self.wind.show();
        self.wind.set_visible_focus();
        app::awake(); // Make sure event loop processes the show
    }

    pub fn hide(&mut self) {
        self.wind.hide();
    }
}

/// Format binary data as hex dump (first 1024 bytes max)
fn format_hex_dump(data: &[u8]) -> String {
    let mut result = String::new();
    let limit = data.len().min(1024);

    for (i, chunk) in data[..limit].chunks(16).enumerate() {
        // Offset
        result.push_str(&format!("{:08x}  ", i * 16));

        // Hex bytes
        for (j, byte) in chunk.iter().enumerate() {
            if j == 8 {
                result.push(' ');
            }
            result.push_str(&format!("{:02x} ", byte));
        }

        // Padding if less than 16 bytes
        for j in chunk.len()..16 {
            if j == 8 {
                result.push(' ');
            }
            result.push_str("   ");
        }

        // ASCII representation
        result.push_str(" |");
        for byte in chunk {
            if byte.is_ascii_graphic() || *byte == b' ' {
                result.push(*byte as char);
            } else {
                result.push('.');
            }
        }
        result.push_str("|\n");
    }

    if data.len() > limit {
        result.push_str(&format!("\n... ({} more bytes)", data.len() - limit));
    }

    result
}
