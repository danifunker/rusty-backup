use super::log_panel::LogPanel;
use crate::gui_fltk::app::LoadedBackupState;
use fltk::{prelude::*, *};
use rusty_backup::device::DiskDevice;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

pub struct InspectTab {
    // Source selection
    source_choice: menu::Choice,
    open_file_btn: button::Button,
    open_backup_btn: button::Button,

    // Info display
    info_display: text::TextDisplay,
    info_buffer: text::TextBuffer,

    // Action buttons
    view_partitions_btn: button::Button,
    browse_filesystem_btn: button::Button,
    export_vhd_btn: button::Button,

    // State
    selected_device_idx: Option<usize>,
    image_file_path: Option<PathBuf>,

    // Shared state
    loaded_backup: Arc<Mutex<LoadedBackupState>>,
    close_backup_btn: button::Button,
    log_panel: LogPanel,
    devices: Vec<DiskDevice>,
}

impl InspectTab {
    pub fn new(
        x: i32,
        y: i32,
        w: i32,
        _h: i32,
        devices: &[DiskDevice],
        log_panel: LogPanel,
        loaded_backup: Arc<Mutex<LoadedBackupState>>,
        close_backup_btn: button::Button,
    ) -> Self {
        let mut y_pos = y + 10;
        let label_w = 100;
        let field_w = w - label_w - 240;
        let row_h = 30;
        let spacing = 10;

        // Source selection
        frame::Frame::new(x + 10, y_pos, label_w, row_h, "Inspect:");
        let mut source_choice = menu::Choice::new(x + label_w + 10, y_pos, field_w, row_h, None);
        source_choice.add_choice("Select a source...");
        for device in devices.iter() {
            source_choice.add_choice(&device.display_name());
        }
        source_choice.set_value(0);

        let open_file_btn = button::Button::new(
            x + label_w + field_w + 20,
            y_pos,
            100,
            row_h,
            "Open File...",
        );
        let open_backup_btn = button::Button::new(
            x + label_w + field_w + 130,
            y_pos,
            100,
            row_h,
            "Open Backup...",
        );
        y_pos += row_h + spacing * 2;

        // Info display area
        frame::Frame::new(x + 10, y_pos, w - 20, 20, "Information:")
            .with_align(enums::Align::Left | enums::Align::Inside);
        y_pos += 25;

        let buffer = text::TextBuffer::default();
        let mut info_display = text::TextDisplay::new(x + 10, y_pos, w - 20, 200, None);
        info_display.set_buffer(buffer.clone());
        info_display.wrap_mode(text::WrapMode::AtBounds, 0);
        y_pos += 205;

        // Action buttons
        let mut view_partitions_btn =
            button::Button::new(x + 10, y_pos, 150, 35, "View Partition Table");
        view_partitions_btn.deactivate();

        let mut browse_filesystem_btn =
            button::Button::new(x + 170, y_pos, 150, 35, "Browse Filesystem");
        browse_filesystem_btn.deactivate();

        let mut export_vhd_btn = button::Button::new(x + 330, y_pos, 150, 35, "Export to VHD");
        export_vhd_btn.deactivate();

        let devices_clone = devices.to_vec();
        let mut tab = Self {
            source_choice,
            open_file_btn,
            open_backup_btn,
            info_display,
            info_buffer: buffer,
            view_partitions_btn,
            browse_filesystem_btn,
            export_vhd_btn,
            selected_device_idx: None,
            image_file_path: None,
            loaded_backup,
            close_backup_btn,
            log_panel,
            devices: devices_clone,
        };

        tab.setup_callbacks();
        tab
    }

    fn setup_callbacks(&mut self) {
        // Source device selection
        self.source_choice.set_callback(|choice| {
            let val = choice.value();
            if val > 0 {
                // Device selected (index is val - 1)
                // TODO: Load device info and enable buttons
            }
        });

        // Open file button
        self.open_file_btn.set_callback(|_| {
            if let Some(path) = rfd::FileDialog::new()
                .set_title("Select Disk Image to Inspect")
                .add_filter(
                    "Disk Images",
                    &["img", "raw", "bin", "iso", "vhd", "vhdx", "vmdk", "qcow2"],
                )
                .add_filter("All Files", &["*"])
                .pick_file()
            {
                // TODO: Load image info and enable buttons
                dialog::message_default(&format!("Inspecting: {}", path.display()));
            }
        });

        // Open backup button
        self.open_backup_btn.set_callback({
            let mut info_buffer = self.info_buffer.clone();
            let mut view_btn = self.view_partitions_btn.clone();
            let mut browse_btn = self.browse_filesystem_btn.clone();
            let mut export_btn = self.export_vhd_btn.clone();
            let log = self.log_panel.clone();
            let loaded = self.loaded_backup.clone();
            let mut close_btn = self.close_backup_btn.clone();

            move |_| {
                if let Some(path) = rfd::FileDialog::new()
                    .set_title("Select Backup Folder to Inspect")
                    .pick_folder()
                {
                    // Check if valid backup
                    let metadata_path = path.join("metadata.json");

                    if metadata_path.exists() {
                        // Load and display backup metadata
                        if let Ok(json) = std::fs::read_to_string(&metadata_path) {
                            if let Ok(metadata) = serde_json::from_str::<
                                rusty_backup::backup::metadata::BackupMetadata,
                            >(&json)
                            {
                                let mut info_text = String::new();
                                info_text.push_str(&format!(
                                    "Backup: {}\n",
                                    path.file_name().unwrap_or_default().to_string_lossy()
                                ));
                                info_text.push_str(&format!("Version: {}\n", metadata.version));
                                info_text.push_str(&format!(
                                    "Alignment: {} ({} sectors)\n\n",
                                    metadata.alignment.detected_type,
                                    metadata.alignment.alignment_sectors
                                ));
                                info_text.push_str(&format!(
                                    "Partitions: {}\n",
                                    metadata.partitions.len()
                                ));

                                for part in &metadata.partitions {
                                    info_text.push_str(&format!(
                                        "  [{}] {} - {} bytes\n",
                                        part.index, part.type_name, part.original_size_bytes
                                    ));
                                }

                                info_buffer.set_text(&info_text);

                                // Update shared state
                                if let Ok(mut state) = loaded.lock() {
                                    state.metadata = Some(metadata.clone());
                                    state.backup_path = Some(path.to_string_lossy().to_string());
                                }

                                // Show close backup button
                                close_btn.show();
                                close_btn.parent().unwrap().redraw();

                                // Enable action buttons
                                view_btn.activate();
                                browse_btn.activate();
                                export_btn.activate();

                                log.info("Loaded backup info");
                            } else {
                                log.error("Failed to parse metadata.json");
                            }
                        } else {
                            log.error("Failed to read metadata.json");
                        }
                    } else {
                        log.error("Not a valid backup folder (metadata.json not found)");
                    }
                }
            }
        });

        // View partitions button
        self.view_partitions_btn.set_callback(|_| {
            // TODO: Open partition viewer window
            dialog::message_default("Partition table viewer will open here");
        });

        // Browse filesystem button
        self.browse_filesystem_btn.set_callback({
            let log = self.log_panel.clone();
            let loaded = self.loaded_backup.clone();

            move |_| {
                // Get backup info from shared state
                let backup_path: PathBuf;
                let metadata: rusty_backup::backup::metadata::BackupMetadata;

                if let Ok(state) = loaded.lock() {
                    if let (Some(ref meta), Some(ref path)) = (&state.metadata, &state.backup_path) {
                        backup_path = PathBuf::from(path);
                        metadata = meta.clone();
                    } else {
                        dialog::message_default("No backup loaded");
                        return;
                    }
                } else {
                    return;
                }

                if metadata.partitions.is_empty() {
                    dialog::message_default("No partitions found in backup");
                    return;
                }

                // Build partition choices for selector
                let partition_choices: Vec<(usize, String, String, u64)> = metadata
                    .partitions
                    .iter()
                    .map(|p| {
                        (
                            p.index,
                            p.type_name.clone(),
                            String::new(), // Volume label - we'll load it when opening
                            p.original_size_bytes,
                        )
                    })
                    .collect();

                // Show partition selector dialog
                let selection = match super::partition_selector::show_partition_selector(&partition_choices) {
                    Some(sel) => sel,
                    None => return, // User cancelled
                };

                // Get selected partition info
                let part_info = &metadata.partitions[selection];

                log.info(format!("Opening filesystem browser for partition {}", part_info.index));

                // Load filesystem from backup
                log.info(format!("Loading partition {} from backup...", part_info.index));

                // Create fltk channel for thread-safe communication
                let (sender, receiver) = app::channel::<(String, String, std::sync::Arc<std::sync::Mutex<Option<Box<dyn rusty_backup::fs::filesystem::Filesystem>>>>)>();

                let log_clone = log.clone();
                let backup_path_clone = backup_path.clone();
                let part_idx = part_info.index;
                let type_byte = part_info.partition_type_byte;
                let compression_type = metadata.compression_type.clone();
                let type_name = part_info.type_name.clone();

                std::thread::spawn(move || {
                    match crate::gui_fltk::fs_loader::load_filesystem_from_backup(
                        &backup_path_clone,
                        part_idx,
                        type_byte,
                        &compression_type,
                    ) {
                        Ok(filesystem) => {
                            let fs_type = filesystem.fs_type().to_string();
                            log_clone.info(format!("Filesystem loaded: {}", fs_type));

                            // Wrap filesystem in Arc<Mutex<Option<>>> for FilesystemBrowserWindow
                            let fs_wrapped = std::sync::Arc::new(std::sync::Mutex::new(Some(filesystem)));
                            let title = format!("Browse: {}", type_name);

                            // Send to main thread via fltk channel
                            sender.send((title, type_name.clone(), fs_wrapped));
                        }
                        Err(e) => {
                            log_clone.error(format!("Failed to load filesystem: {}", e));
                        }
                    }
                });

                // Poll receiver on main thread (this runs periodically in the event loop)
                std::thread::spawn(move || {
                    // Wait briefly for the main app to be ready
                    std::thread::sleep(std::time::Duration::from_millis(100));

                    loop {
                        if let Some((title, volume_label, filesystem)) = receiver.recv() {
                            // Use awake to trigger window creation on main thread
                            app::awake_callback({
                                let title = title.clone();
                                let volume_label = volume_label.clone();
                                let filesystem = filesystem.clone();
                                move || {
                                    let mut browser = crate::gui_fltk::filesystem_browser::FilesystemBrowserWindow::new(
                                        &title,
                                        &volume_label,
                                        filesystem.clone(),
                                    );
                                    browser.show();
                                }
                            });
                            break;
                        }
                        std::thread::sleep(std::time::Duration::from_millis(10));
                    }
                });
            }
        });

        // Export VHD button
        self.export_vhd_btn.set_callback(|_| {
            // TODO: Open VHD export configuration window
            dialog::message_default("VHD export configuration will open here");
        });
    }

    pub fn update_info(&mut self, info: &str) {
        self.info_buffer.set_text(info);
    }

    pub fn clear_info(&mut self) {
        self.info_buffer.set_text("");
    }

    pub fn get_info_buffer(&self) -> text::TextBuffer {
        self.info_buffer.clone()
    }
}
