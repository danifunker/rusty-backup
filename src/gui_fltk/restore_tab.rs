use super::log_panel::LogPanel;
use super::progress::ProgressState;
use crate::gui_fltk::app::LoadedBackupState;
use fltk::{prelude::*, *};
use rusty_backup::device::DiskDevice;
use std::sync::{Arc, Mutex};

use rusty_backup::restore::RestoreAlignment;
use std::path::PathBuf;

pub struct RestoreTab {
    // Source (backup folder)
    backup_input: input::Input,
    backup_browse_btn: button::Button,

    // Info display
    info_display: text::TextDisplay,
    info_buffer: text::TextBuffer,

    // Target selection
    target_choice: menu::Choice,
    open_file_btn: button::Button,

    // Alignment
    alignment_original_radio: button::RadioButton,
    alignment_details_frame: frame::Frame,
    alignment_modern_radio: button::RadioButton,
    alignment_custom_radio: button::RadioButton,
    custom_alignment_input: input::IntInput,

    // Actions
    view_partitions_btn: button::Button,
    configure_sizes_btn: button::Button,
    start_btn: button::Button,

    // State
    selected_device_idx: Option<usize>,
    target_file_path: Option<PathBuf>,

    // Shared state
    loaded_backup: Arc<Mutex<LoadedBackupState>>,
    close_backup_btn: button::Button,
    log_panel: LogPanel,
    progress_state: ProgressState,
    devices: Vec<DiskDevice>,
}

impl RestoreTab {
    pub fn new(
        x: i32,
        y: i32,
        w: i32,
        _h: i32,
        devices: &[DiskDevice],
        log_panel: LogPanel,
        progress_state: ProgressState,
        loaded_backup: Arc<Mutex<LoadedBackupState>>,
        close_backup_btn: button::Button,
    ) -> Self {
        let mut y_pos = y + 10;
        let label_w = 120;
        let field_w = w - label_w - 120;
        let row_h = 30;
        let spacing = 10;

        // Backup folder selection
        frame::Frame::new(x + 10, y_pos, label_w, row_h, "Backup Folder:");
        let backup_input = input::Input::new(x + label_w + 10, y_pos, field_w, row_h, None);
        let backup_browse_btn =
            button::Button::new(x + label_w + field_w + 20, y_pos, 90, row_h, "Browse...");
        y_pos += row_h + spacing;

        // Info display area (shows backup details when loaded)
        let info_buffer = text::TextBuffer::default();
        let mut info_display = text::TextDisplay::new(x + 10, y_pos, w - 20, 70, None);
        info_display.set_buffer(info_buffer.clone());
        info_display.wrap_mode(text::WrapMode::AtBounds, 0);
        info_display.set_text_size(12);
        y_pos += 75;

        // Target selection
        frame::Frame::new(x + 10, y_pos, label_w, row_h, "Restore To:");
        let mut target_choice = menu::Choice::new(x + label_w + 10, y_pos, field_w, row_h, None);
        target_choice.add_choice("Select a device...");
        for device in devices.iter() {
            target_choice.add_choice(&device.display_name());
        }
        target_choice.set_value(0);

        let open_file_btn =
            button::Button::new(x + label_w + field_w + 20, y_pos, 90, row_h, "To File...");
        y_pos += row_h + spacing * 2;

        // Alignment section
        frame::Frame::new(x + 10, y_pos, w - 20, 20, "Partition Alignment:")
            .with_align(enums::Align::Left | enums::Align::Inside);
        y_pos += 25;

        let mut alignment_original_radio =
            button::RadioButton::new(x + 20, y_pos, 180, row_h, "Original (from backup)");
        alignment_original_radio.set_value(true);

        // Frame to display detected alignment details (e.g., "Modern1MB - 2048 sectors")
        let mut alignment_details_frame = frame::Frame::new(x + 210, y_pos, 350, row_h, "");
        alignment_details_frame.set_align(enums::Align::Left | enums::Align::Inside);
        alignment_details_frame.set_label_color(enums::Color::from_rgb(100, 100, 100));
        y_pos += row_h + 5;

        let mut alignment_modern_radio =
            button::RadioButton::new(x + 20, y_pos, 200, row_h, "Modern (1MB alignment)");
        y_pos += row_h + 5;

        let mut alignment_custom_radio =
            button::RadioButton::new(x + 20, y_pos, 150, row_h, "Custom:");
        let mut custom_alignment_input = input::IntInput::new(x + 180, y_pos, 100, row_h, None);
        custom_alignment_input.set_value("2048");
        custom_alignment_input.deactivate(); // disabled by default
        frame::Frame::new(x + 290, y_pos, 100, row_h, "sectors");
        y_pos += row_h + spacing * 2;

        // Wire up custom alignment enable/disable
        alignment_custom_radio.set_callback({
            let mut input = custom_alignment_input.clone();
            move |radio| {
                if radio.value() {
                    input.activate();
                } else {
                    input.deactivate();
                }
            }
        });

        alignment_original_radio.set_callback({
            let mut input = custom_alignment_input.clone();
            move |_| {
                input.deactivate();
            }
        });

        alignment_modern_radio.set_callback({
            let mut input = custom_alignment_input.clone();
            move |_| {
                input.deactivate();
            }
        });

        // Partition info buttons
        let mut view_partitions_btn =
            button::Button::new(x + 10, y_pos, 180, 35, "View Partition Table");
        view_partitions_btn.deactivate(); // Enable when backup loaded

        let mut configure_sizes_btn =
            button::Button::new(x + 200, y_pos, 180, 35, "Configure Partition Sizes");
        configure_sizes_btn.deactivate(); // Enable when backup loaded
        y_pos += 40 + spacing * 2;

        // Start button
        let mut start_btn = button::Button::new(x + 10, y_pos, 150, 30, "Start Restore");
        start_btn.set_color(enums::Color::from_rgb(0, 100, 180));
        start_btn.set_label_color(enums::Color::White);
        start_btn.deactivate(); // Enable when ready

        // Set up callbacks
        let devices_clone = devices.to_vec();
        let mut tab = Self {
            backup_input,
            backup_browse_btn,
            info_display,
            info_buffer,
            target_choice,
            open_file_btn,
            alignment_original_radio,
            alignment_details_frame,
            alignment_modern_radio,
            alignment_custom_radio,
            custom_alignment_input,
            view_partitions_btn,
            configure_sizes_btn,
            start_btn,
            selected_device_idx: None,
            target_file_path: None,
            loaded_backup,
            close_backup_btn,
            log_panel,
            progress_state,
            devices: devices_clone,
        };

        tab.setup_callbacks();
        tab
    }

    fn setup_callbacks(&mut self) {
        // Backup browse button
        self.backup_browse_btn.set_callback({
            let mut input = self.backup_input.clone();
            let mut info_buffer = self.info_buffer.clone();
            let mut view_btn = self.view_partitions_btn.clone();
            let mut config_btn = self.configure_sizes_btn.clone();
            let mut start_btn = self.start_btn.clone();
            let mut details_frame = self.alignment_details_frame.clone();
            let log = self.log_panel.clone();

            move |_| {
                if let Some(dirname) = rfd::FileDialog::new()
                    .set_title("Select Backup Folder (containing metadata.json or Clonezilla image)")
                    .pick_folder()
                {
                    // Check if this looks like a backup folder
                    let metadata_path = dirname.join("metadata.json");

                    if metadata_path.exists() {
                        // Try to load metadata
                        match std::fs::read_to_string(&metadata_path) {
                            Ok(json) => {
                                match serde_json::from_str::<rusty_backup::backup::metadata::BackupMetadata>(&json) {
                                    Ok(metadata) => {
                                        input.set_value(&dirname.to_string_lossy());

                                        // Update alignment details frame with detected value
                                        let alignment_text = format!("({} - {} sectors)",
                                            metadata.alignment.detected_type,
                                            metadata.alignment.alignment_sectors);
                                        details_frame.set_label(&alignment_text);

                                        // Display backup info in text area
                                        let mut info_text = String::new();
                                        info_text.push_str(&format!("Backup: {}\n", dirname.file_name().unwrap_or_default().to_string_lossy()));
                                        info_text.push_str(&format!("Partitions: {}  |  Alignment: {} ({} sectors)\n",
                                            metadata.partitions.len(),
                                            metadata.alignment.detected_type,
                                            metadata.alignment.alignment_sectors));
                                        info_text.push_str(&format!("Compression: {}  |  Checksum: {}",
                                            metadata.compression_type,
                                            metadata.checksum_type));
                                        info_buffer.set_text(&info_text);

                                        // Enable partition buttons
                                        view_btn.activate();
                                        config_btn.activate();
                                        start_btn.activate();

                                        log.info(format!("Loaded backup: {} partitions", metadata.partitions.len()));

                                        // TODO: Store metadata for later use
                                    }
                                    Err(e) => {
                                        dialog::message_default(&format!("Error parsing metadata: {}", e));
                                    }
                                }
                            }
                            Err(e) => {
                                dialog::message_default(&format!("Error reading metadata: {}", e));
                            }
                        }
                    } else if rusty_backup::clonezilla::metadata::is_clonezilla_image(&dirname) {
                        input.set_value(&dirname.to_string_lossy());
                        view_btn.activate();
                        config_btn.activate();
                        start_btn.activate();
                        log.info("Loaded Clonezilla backup");
                    } else {
                        dialog::message_default("Selected folder does not contain a valid backup.\n\nLooking for metadata.json or Clonezilla image.");
                    }
                }
            }
        });

        // Target device selection
        self.target_choice.set_callback(|choice| {
            let val = choice.value();
            if val > 0 {
                // Device selected (index is val - 1)
                // TODO: Store selected device index
            }
        });

        // Target file button
        self.open_file_btn.set_callback(|_| {
            if let Some(path) = rfd::FileDialog::new()
                .set_title("Restore to Image File")
                .add_filter("Disk Images", &["img", "raw", "bin", "iso"])
                .save_file()
            {
                // TODO: Set target_file_path and update UI to show selected file
                dialog::message_default(&format!("Will restore to: {}", path.display()));
            }
        });

        // View partitions button - need to pass backup path so it can load metadata
        self.view_partitions_btn.set_callback({
            let backup_input = self.backup_input.clone();
            let log = self.log_panel.clone();

            move |_| {
                let backup_path = PathBuf::from(backup_input.value());
                let metadata_path = backup_path.join("metadata.json");

                if let Ok(json) = std::fs::read_to_string(&metadata_path) {
                    if let Ok(metadata) = serde_json::from_str::<
                        rusty_backup::backup::metadata::BackupMetadata,
                    >(&json)
                    {
                        // Convert to PartitionInfo for viewer
                        let partitions: Vec<rusty_backup::partition::PartitionInfo> = metadata
                            .partitions
                            .iter()
                            .map(|p| {
                                rusty_backup::partition::PartitionInfo {
                                    index: p.index,
                                    partition_type_byte: p.partition_type_byte,
                                    type_name: p.type_name.clone(),
                                    start_lba: p.start_lba,
                                    size_bytes: p.original_size_bytes,
                                    bootable: false, // Not stored in metadata
                                    is_logical: p.is_logical,
                                    is_extended_container: false,
                                    partition_type_string: None,
                                }
                            })
                            .collect();

                        // Open partition viewer window
                        let mut viewer = super::partition_viewer::PartitionViewerWindow::new(
                            &partitions,
                            &backup_path
                                .file_name()
                                .unwrap_or_default()
                                .to_string_lossy(),
                        );
                        viewer.show();
                    } else {
                        log.error("Failed to parse backup metadata");
                    }
                } else {
                    log.error("Could not read backup metadata");
                }
            }
        });

        // Configure sizes button
        self.configure_sizes_btn.set_callback({
            let backup_input = self.backup_input.clone();
            let log = self.log_panel.clone();

            move |_| {
                let backup_path = PathBuf::from(backup_input.value());
                let metadata_path = backup_path.join("metadata.json");

                if let Ok(json) = std::fs::read_to_string(&metadata_path) {
                    if let Ok(metadata) = serde_json::from_str::<
                        rusty_backup::backup::metadata::BackupMetadata,
                    >(&json)
                    {
                        // Convert to PartitionInfo for configuration
                        let partitions: Vec<rusty_backup::partition::PartitionInfo> = metadata
                            .partitions
                            .iter()
                            .map(|p| rusty_backup::partition::PartitionInfo {
                                index: p.index,
                                partition_type_byte: p.partition_type_byte,
                                type_name: p.type_name.clone(),
                                start_lba: p.start_lba,
                                size_bytes: p.original_size_bytes,
                                bootable: false,
                                is_logical: p.is_logical,
                                is_extended_container: false,
                                partition_type_string: None,
                            })
                            .collect();

                        // Open VHD configuration window (repurpose for partition sizing)
                        let mut config = super::vhd_config::VhdConfigWindow::new(
                            &partitions,
                            "Restore - Partition Sizes",
                        );
                        if let Some(_result) = config.show() {
                            // TODO: Store partition size preferences
                            log.info("Partition sizes configured");
                        }
                    } else {
                        log.error("Failed to parse backup metadata");
                    }
                } else {
                    log.error("Could not read backup metadata");
                }
            }
        });

        // Start restore button
        self.start_btn.set_callback({
            let mut btn = self.start_btn.clone();
            let backup_input = self.backup_input.clone();
            let target_choice = self.target_choice.clone();
            let devices = self.devices.clone();
            let loaded_backup = self.loaded_backup.clone();
            let log_panel = self.log_panel.clone();
            let progress_state = self.progress_state.clone();
            let alignment_original = self.alignment_original_radio.clone();
            let alignment_modern = self.alignment_modern_radio.clone();
            let custom_alignment_input = self.custom_alignment_input.clone();

            move |_| {
                // Validate backup folder is loaded
                let backup_path = PathBuf::from(backup_input.value());
                if backup_path.as_os_str().is_empty() {
                    dialog::message_default("Please select a backup folder");
                    return;
                }

                // Validate target is selected
                if target_choice.value() == 0 {
                    dialog::message_default("Please select a target device");
                    return;
                }

                let target_idx = (target_choice.value() - 1) as usize;
                if target_idx >= devices.len() {
                    dialog::message_default("Invalid target device selected");
                    return;
                }
                let target_device = &devices[target_idx];
                let target_path = PathBuf::from(&target_device.path);

                // Get alignment choice
                let alignment = if alignment_original.value() {
                    rusty_backup::restore::RestoreAlignment::Original
                } else if alignment_modern.value() {
                    rusty_backup::restore::RestoreAlignment::Modern1MB
                } else {
                    let sectors = custom_alignment_input
                        .value()
                        .parse()
                        .unwrap_or(2048);
                    rusty_backup::restore::RestoreAlignment::Custom(sectors)
                };

                // Get backup metadata from loaded state
                let metadata_opt = if let Ok(state) = loaded_backup.lock() {
                    state.metadata.clone()
                } else {
                    None
                };

                let metadata = match metadata_opt {
                    Some(m) => m,
                    None => {
                        dialog::message_default("Backup metadata not loaded. Please browse to a backup folder first.");
                        return;
                    }
                };

                // Calculate required size from partitions
                let required_bytes: u64 = metadata
                    .partitions
                    .iter()
                    .map(|p| p.imaged_size_bytes)
                    .sum();
                let target_bytes = target_device.size_bytes;

                if target_bytes < required_bytes {
                    dialog::message_default(&format!(
                        "Target device is too small!\n\nRequired: {} bytes\nTarget: {} bytes",
                        required_bytes, target_bytes
                    ));
                    return;
                }

                // Confirmation dialog
                let msg = format!(
                    "⚠ WARNING: This will OVERWRITE ALL DATA on:\n\n{}\n\nSize: {} bytes\n\nAre you ABSOLUTELY SURE?",
                    target_device.display_name(),
                    target_bytes
                );
                let result = dialog::choice2_default(&msg, "Yes, RESTORE", "Cancel", "");
                if result != Some(0) {
                    return; // User cancelled
                }

                // Build partition sizes (for now, use Original for all)
                let partition_sizes: Vec<rusty_backup::restore::RestorePartitionSize> = metadata
                    .partitions
                    .iter()
                    .enumerate()
                    .map(|(i, _p)| rusty_backup::restore::RestorePartitionSize {
                        index: i,
                        size_choice: rusty_backup::restore::RestoreSizeChoice::Original,
                    })
                    .collect();

                let config = rusty_backup::restore::RestoreConfig {
                    backup_folder: backup_path.clone(),
                    target_path: target_path.clone(),
                    target_is_device: true,
                    target_size: target_bytes,
                    alignment,
                    partition_sizes,
                    write_zeros_to_unused: false,
                };

                // Disable button during operation
                btn.deactivate();
                progress_state.set_active(true);
                log_panel.info(format!("Starting restore to: {}", target_device.display_name()));

                // Spawn restore thread
                let log_panel_thread = log_panel.clone();
                let progress_state_thread = progress_state.clone();
                let mut btn_clone = btn.clone();

                std::thread::spawn(move || {
                    let progress = Arc::new(Mutex::new(rusty_backup::restore::RestoreProgress::new()));
                    let progress_clone = progress.clone();

                    // Set up progress polling
                    let progress_poll = progress.clone();
                    let progress_state_poll = progress_state_thread.clone();
                    let log_poll = log_panel_thread.clone();

                    let poll_handle = {
                        let progress = progress_poll.clone();
                        let progress_state = progress_state_poll.clone();
                        let mut log = log_poll.clone();

                        app::add_timeout3(0.2, move |handle| {
                            if let Ok(p) = progress.lock() {
                                // Update progress state
                                progress_state.update(p.current_bytes, p.total_bytes);
                                progress_state.set_operation(&p.operation);

                                // Process log messages
                                let messages: Vec<_> = p
                                    .log_messages
                                    .iter()
                                    .map(|m| (m.level, m.message.clone()))
                                    .collect();
                                let finished = p.finished;
                                drop(p);

                                for (level, msg) in messages {
                                    match level {
                                        rusty_backup::backup::LogLevel::Info => log.info(&msg),
                                        rusty_backup::backup::LogLevel::Warning => log.warn(&msg),
                                        rusty_backup::backup::LogLevel::Error => log.error(&msg),
                                    }
                                }

                                // Clear processed messages
                                if let Ok(mut p) = progress.lock() {
                                    p.log_messages.clear();
                                }

                                // Check if finished
                                if finished {
                                    app::remove_timeout3(handle);
                                    return;
                                }
                            }

                            // Wake UI
                            app::awake();
                            app::repeat_timeout3(0.2, handle);
                        })
                    };

                    // Run restore
                    let result = rusty_backup::restore::run_restore(config, progress_clone);

                    // Handle result
                    match result {
                        Ok(_) => {
                            log_panel_thread.info("Restore completed successfully!");
                        }
                        Err(e) => {
                            log_panel_thread.error(format!("Restore failed: {}", e));
                        }
                    }

                    // Mark finished and clean up
                    if let Ok(mut p) = progress.lock() {
                        p.finished = true;
                    }

                    progress_state_thread.set_active(false);
                    app::remove_timeout3(poll_handle);
                    fltk::app::awake();

                    // Re-enable button
                    app::awake_callback(move || {
                        btn_clone.activate();
                    });
                });
            }
        });
    }

    pub fn get_selected_alignment(&self) -> RestoreAlignment {
        if self.alignment_original_radio.value() {
            RestoreAlignment::Original
        } else if self.alignment_modern_radio.value() {
            RestoreAlignment::Modern1MB
        } else {
            let sectors = self.custom_alignment_input.value().parse().unwrap_or(2048);
            RestoreAlignment::Custom(sectors)
        }
    }

    pub fn get_info_buffer(&self) -> text::TextBuffer {
        self.info_buffer.clone()
    }

    pub fn get_backup_input(&self) -> input::Input {
        self.backup_input.clone()
    }

    pub fn get_target_choice(&self) -> menu::Choice {
        self.target_choice.clone()
    }

    pub fn update_device_list(&mut self, devices: &[DiskDevice]) {
        self.devices = devices.to_vec();
        let current_value = self.target_choice.value();
        self.target_choice.clear();
        self.target_choice.add_choice("Select a device...");
        for device in devices.iter() {
            self.target_choice.add_choice(&device.display_name());
        }
        if current_value < (devices.len() + 1) as i32 {
            self.target_choice.set_value(current_value);
        } else {
            self.target_choice.set_value(0);
        }
    }
}
