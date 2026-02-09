use super::log_panel::LogPanel;
use super::progress::ProgressState;
use fltk::{prelude::*, *};
use rusty_backup::backup::{BackupConfig, BackupProgress, ChecksumType, CompressionType};
use rusty_backup::device::DiskDevice;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

pub struct BackupTab {
    // Source selection
    source_choice: menu::Choice,
    open_file_btn: button::Button,

    // Destination
    dest_input: input::Input,
    dest_browse_btn: button::Button,

    // Backup name
    name_input: input::Input,

    // Options
    sector_by_sector_check: button::CheckButton,
    resize_partitions_check: button::CheckButton,
    split_archives_check: button::CheckButton,
    split_size_input: input::IntInput,

    // Compression
    compression_choice: menu::Choice,

    // Checksum
    checksum_choice: menu::Choice,

    // Actions
    start_btn: button::Button,

    // State
    selected_device_idx: Option<usize>,
    image_file_path: Option<PathBuf>,
    destination_folder: Option<PathBuf>,
    chdman_available: bool,

    // Shared state
    log_panel: LogPanel,
    progress_state: ProgressState,
    devices: Vec<DiskDevice>,
}

impl BackupTab {
    pub fn new(
        x: i32,
        y: i32,
        w: i32,
        _h: i32,
        devices: &[DiskDevice],
        log_panel: LogPanel,
        progress_state: ProgressState,
    ) -> Self {
        let mut y_pos = y + 10;
        let label_w = 100;
        let field_w = w - label_w - 120;
        let row_h = 30;
        let spacing = 10;

        // Source selection
        frame::Frame::new(x + 10, y_pos, label_w, row_h, "Source:");
        let mut source_choice = menu::Choice::new(x + label_w + 10, y_pos, field_w, row_h, None);
        source_choice.add_choice("Select a device...");
        for device in devices.iter() {
            source_choice.add_choice(&device.display_name());
        }
        source_choice.set_value(0);

        let open_file_btn =
            button::Button::new(x + label_w + field_w + 20, y_pos, 90, row_h, "Open File...");
        y_pos += row_h + spacing;

        // Destination folder
        frame::Frame::new(x + 10, y_pos, label_w, row_h, "Destination:");
        let dest_input = input::Input::new(x + label_w + 10, y_pos, field_w, row_h, None);
        let dest_browse_btn =
            button::Button::new(x + label_w + field_w + 20, y_pos, 90, row_h, "Browse...");
        y_pos += row_h + spacing;

        // Backup name
        frame::Frame::new(x + 10, y_pos, label_w, row_h, "Backup Name:");
        let name_input = input::Input::new(x + label_w + 10, y_pos, field_w, row_h, None);
        y_pos += row_h + spacing * 2;

        // Options section
        frame::Frame::new(x + 10, y_pos, w - 20, 20, "Options:")
            .with_align(enums::Align::Left | enums::Align::Inside);
        y_pos += 25;

        let sector_by_sector_check = button::CheckButton::new(
            x + 20,
            y_pos,
            w - 40,
            row_h,
            "Sector-by-sector backup (slower, preserves empty space)",
        );
        y_pos += row_h + 5;

        let resize_partitions_check = button::CheckButton::new(
            x + 20,
            y_pos,
            w - 40,
            row_h,
            "Smart resize partitions (recommended)",
        );
        resize_partitions_check.set_checked(true);
        y_pos += row_h + 5;

        let mut split_archives_check =
            button::CheckButton::new(x + 20, y_pos, 200, row_h, "Split archives");
        split_archives_check.set_checked(true);

        frame::Frame::new(x + 230, y_pos, 80, row_h, "Split size:");
        let mut split_size_input = input::IntInput::new(x + 315, y_pos, 80, row_h, None);
        split_size_input.set_value("4000");

        let _split_size_label = frame::Frame::new(x + 400, y_pos, 50, row_h, "MiB");
        y_pos += row_h + spacing * 2;

        // Wire up split archives checkbox to enable/disable split size input
        split_archives_check.set_callback({
            let mut input = split_size_input.clone();
            move |check| {
                if check.is_checked() {
                    input.activate();
                } else {
                    input.deactivate();
                }
            }
        });

        // Compression
        frame::Frame::new(x + 10, y_pos, label_w, row_h, "Compression:");
        let mut compression_choice = menu::Choice::new(x + label_w + 10, y_pos, 200, row_h, None);
        compression_choice.add_choice("Zstd (recommended)");
        compression_choice.add_choice("CHD (MAME format)");
        compression_choice.add_choice("VHD (Virtual Hard Disk)");
        compression_choice.add_choice("None (uncompressed)");
        compression_choice.set_value(0);
        y_pos += row_h + spacing;

        // Checksum
        frame::Frame::new(x + 10, y_pos, label_w, row_h, "Checksum:");
        let mut checksum_choice = menu::Choice::new(x + label_w + 10, y_pos, 200, row_h, None);
        checksum_choice.add_choice("SHA-256 (recommended)");
        checksum_choice.add_choice("CRC-32 (faster)");
        checksum_choice.set_value(0);
        y_pos += row_h + spacing; // Minimal spacing before Start button

        // Start button
        let mut start_btn = button::Button::new(x + 10, y_pos, 150, 30, "Start Backup");
        start_btn.set_color(enums::Color::from_rgb(0, 120, 0));
        start_btn.set_label_color(enums::Color::White);

        // Set up callbacks
        let devices_clone = devices.to_vec();
        let mut tab = Self {
            source_choice,
            open_file_btn,
            dest_input,
            dest_browse_btn,
            name_input,
            sector_by_sector_check,
            resize_partitions_check,
            split_archives_check,
            split_size_input,
            compression_choice,
            checksum_choice,
            start_btn,
            selected_device_idx: None,
            image_file_path: None,
            destination_folder: None,
            chdman_available: true,
            log_panel,
            progress_state,
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
                // Device selected (index is val - 1, since 0 is "Select a device...")
                // TODO: Store selected device index
            }
        });

        // Open file button
        self.open_file_btn.set_callback(|_| {
            if let Some(path) = rfd::FileDialog::new()
                .set_title("Select Disk Image or Device")
                .add_filter(
                    "Disk Images",
                    &["img", "raw", "bin", "iso", "vhd", "vhdx", "vmdk", "qcow2"],
                )
                .add_filter("All Files", &["*"])
                .pick_file()
            {
                // TODO: Set image_file_path and update UI
                dialog::message_default(&format!("Selected: {}", path.display()));
            }
        });

        // Destination browse button
        self.dest_browse_btn.set_callback({
            let mut dest_input = self.dest_input.clone();
            move |_| {
                if let Some(path) = rfd::FileDialog::new()
                    .set_title("Select Backup Destination Folder")
                    .pick_folder()
                {
                    dest_input.set_value(&path.to_string_lossy());
                }
            }
        });

        // Start backup button
        self.start_btn.set_callback({
            let mut btn = self.start_btn.clone();
            let devices = self.devices.clone();
            let source_choice = self.source_choice.clone();
            let dest_input = self.dest_input.clone();
            let name_input = self.name_input.clone();
            let sector_check = self.sector_by_sector_check.clone();
            let _resize_check = self.resize_partitions_check.clone();
            let split_check = self.split_archives_check.clone();
            let split_size_input = self.split_size_input.clone();
            let compression_choice = self.compression_choice.clone();
            let checksum_choice = self.checksum_choice.clone();
            let log_panel = self.log_panel.clone();
            let progress_state = self.progress_state.clone();

            move |_| {
                // Validate inputs
                let dest_path = dest_input.value();
                if dest_path.is_empty() {
                    dialog::message_default("Please select a destination folder");
                    return;
                }

                let backup_name = name_input.value();
                if backup_name.is_empty() {
                    dialog::message_default("Please enter a backup name");
                    return;
                }

                // Determine source
                let source_path = if source_choice.value() > 0 {
                    // Device selected
                    let idx = (source_choice.value() - 1) as usize;
                    if idx < devices.len() {
                        PathBuf::from(&devices[idx].path)
                    } else {
                        dialog::message_default("Invalid device selected");
                        return;
                    }
                } else {
                    dialog::message_default("Please select a source device");
                    return;
                };

                // Build configuration
                let compression = match compression_choice.value() {
                    0 => CompressionType::Zstd,
                    1 => CompressionType::Chd,
                    2 => CompressionType::Vhd,
                    3 => CompressionType::None,
                    _ => CompressionType::Zstd,
                };

                let checksum = match checksum_choice.value() {
                    0 => ChecksumType::Sha256,
                    1 => ChecksumType::Crc32,
                    _ => ChecksumType::Sha256,
                };

                let split_size_mib = if split_check.is_checked() {
                    if let Ok(size) = split_size_input.value().parse::<u32>() {
                        Some(size)
                    } else {
                        None
                    }
                } else {
                    None
                };

                let config = BackupConfig {
                    source_path,
                    destination_dir: PathBuf::from(dest_path),
                    backup_name,
                    compression,
                    checksum,
                    split_size_mib,
                    sector_by_sector: sector_check.is_checked(),
                };

                // Disable button during operation
                btn.deactivate();
                progress_state.set_active(true);
                log_panel.info(format!("Starting backup: {}", config.source_path.display()));

                // Spawn backup thread
                let log_panel_thread = log_panel.clone();
                let progress_state_thread = progress_state.clone();
                let mut btn_clone = btn.clone();

                std::thread::spawn(move || {
                    let progress = Arc::new(Mutex::new(BackupProgress::new()));
                    let progress_clone = progress.clone();

                    // Run backup
                    let result = rusty_backup::backup::run_backup(config, progress_clone);

                    // Handle result
                    match result {
                        Ok(_) => {
                            log_panel_thread.info("Backup completed successfully!");
                        }
                        Err(e) => {
                            log_panel_thread.error(format!("Backup failed: {}", e));
                        }
                    }

                    // Re-enable button and reset progress
                    progress_state_thread.set_active(false);
                    fltk::app::awake(); // Wake UI thread to refresh

                    // Re-enable button on main thread
                    app::awake_callback(move || {
                        btn_clone.activate();
                    });
                });
            }
        });
    }

    pub fn set_chdman_available(&mut self, available: bool) {
        self.chdman_available = available;
        if !available {
            // Disable CHD option if not available
            // Note: fltk doesn't have easy way to disable specific menu items
            // For now, just track the state
        }
    }

    pub fn get_selected_compression(&self) -> CompressionType {
        match self.compression_choice.value() {
            0 => CompressionType::Zstd,
            1 => CompressionType::Chd,
            2 => CompressionType::Vhd,
            3 => CompressionType::None,
            _ => CompressionType::Zstd,
        }
    }

    pub fn get_selected_checksum(&self) -> ChecksumType {
        match self.checksum_choice.value() {
            0 => ChecksumType::Sha256,
            1 => ChecksumType::Crc32,
            _ => ChecksumType::Sha256,
        }
    }
}
