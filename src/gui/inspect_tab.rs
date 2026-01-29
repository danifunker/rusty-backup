use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

use rusty_backup::device::DiskDevice;
use rusty_backup::partition::{self, detect_alignment, PartitionAlignment, PartitionInfo, PartitionTable};

use super::progress::{LogPanel, LogLevel};

/// State for the Inspect tab.
pub struct InspectTab {
    /// Index into the device list, or None if "Open Image File..." is selected
    selected_device_idx: Option<usize>,
    /// Path to an image file (when using file picker instead of device)
    image_file_path: Option<PathBuf>,
    /// Parsed partition table result
    partition_table: Option<PartitionTable>,
    /// Detected alignment
    alignment: Option<PartitionAlignment>,
    /// Unified partition list for display
    partitions: Vec<PartitionInfo>,
    /// Error from last inspection attempt
    last_error: Option<String>,
}

impl Default for InspectTab {
    fn default() -> Self {
        Self {
            selected_device_idx: None,
            image_file_path: None,
            partition_table: None,
            alignment: None,
            partitions: Vec::new(),
            last_error: None,
        }
    }
}

impl InspectTab {
    pub fn show(
        &mut self,
        ui: &mut egui::Ui,
        devices: &[DiskDevice],
        log: &mut LogPanel,
    ) {
        ui.heading("Inspect Disk / Image");
        ui.add_space(8.0);

        // Device / image selection
        ui.horizontal(|ui| {
            ui.label("Source:");

            let current_label = if let Some(path) = &self.image_file_path {
                format!("Image: {}", path.display())
            } else if let Some(idx) = self.selected_device_idx {
                devices
                    .get(idx)
                    .map(|d| d.display_name())
                    .unwrap_or_else(|| "Unknown".into())
            } else {
                "Select a device or image...".into()
            };

            egui::ComboBox::from_id_salt("inspect_source")
                .selected_text(&current_label)
                .width(400.0)
                .show_ui(ui, |ui| {
                    for (i, device) in devices.iter().enumerate() {
                        let label = device.display_name();
                        if ui
                            .selectable_value(&mut self.selected_device_idx, Some(i), &label)
                            .clicked()
                        {
                            self.image_file_path = None;
                            self.clear_results();
                        }
                    }
                    ui.separator();
                    if ui
                        .selectable_label(self.image_file_path.is_some(), "Open Image File...")
                        .clicked()
                    {
                        self.pick_image_file();
                    }
                });
        });

        ui.add_space(4.0);

        // Inspect button
        ui.horizontal(|ui| {
            let has_source = self.selected_device_idx.is_some() || self.image_file_path.is_some();
            if ui
                .add_enabled(has_source, egui::Button::new("Inspect"))
                .clicked()
            {
                self.run_inspect(devices, log);
            }
        });

        ui.add_space(12.0);

        // Show error if any
        if let Some(err) = &self.last_error {
            ui.colored_label(egui::Color32::from_rgb(255, 100, 100), format!("Error: {err}"));
            ui.add_space(8.0);
        }

        // Show results
        if let Some(table) = &self.partition_table {
            self.show_results(ui, table);
        }
    }

    fn pick_image_file(&mut self) {
        if let Some(path) = rfd::FileDialog::new()
            .add_filter("Disk Images", &["img", "raw", "bin", "iso", "dd"])
            .add_filter("All Files", &["*"])
            .pick_file()
        {
            self.image_file_path = Some(path);
            self.selected_device_idx = None;
            self.clear_results();
        }
    }

    fn clear_results(&mut self) {
        self.partition_table = None;
        self.alignment = None;
        self.partitions.clear();
        self.last_error = None;
    }

    fn run_inspect(&mut self, devices: &[DiskDevice], log: &mut LogPanel) {
        self.clear_results();

        let path = if let Some(img_path) = &self.image_file_path {
            img_path.clone()
        } else if let Some(idx) = self.selected_device_idx {
            if let Some(device) = devices.get(idx) {
                device.path.clone()
            } else {
                self.last_error = Some("Selected device not found".into());
                return;
            }
        } else {
            return;
        };

        log.info(format!("Inspecting {}...", path.display()));

        match File::open(&path) {
            Ok(file) => {
                let mut reader = BufReader::new(file);
                match PartitionTable::detect(&mut reader) {
                    Ok(table) => {
                        let alignment = detect_alignment(&table);
                        self.partitions = table.partitions();
                        log.info(format!(
                            "Detected {} partition table with {} partition(s)",
                            table.type_name(),
                            self.partitions.len()
                        ));
                        log.info(format!(
                            "Alignment: {} (first LBA: {})",
                            alignment.alignment_type, alignment.first_lba
                        ));
                        self.alignment = Some(alignment);
                        self.partition_table = Some(table);
                    }
                    Err(e) => {
                        let msg = format!("Failed to parse partition table: {e}");
                        log.error(&msg);
                        self.last_error = Some(msg);
                    }
                }
            }
            Err(e) => {
                let msg = format!("Cannot open {}: {e}", path.display());
                log.add(LogLevel::Error, &msg);
                self.last_error = Some(msg);
            }
        }
    }

    fn show_results(&self, ui: &mut egui::Ui, table: &PartitionTable) {
        // Partition table type
        ui.horizontal(|ui| {
            ui.label(egui::RichText::new("Partition Table:").strong());
            ui.label(table.type_name());
            ui.label(format!("(disk signature: 0x{:08X})", table.disk_signature()));
        });

        // Alignment info
        if let Some(alignment) = &self.alignment {
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new("Alignment:").strong());
                ui.label(format!("{}", alignment.alignment_type));
            });
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new("First LBA:").strong());
                ui.label(format!("{}", alignment.first_lba));
                if alignment.heads > 0 {
                    ui.label(format!(
                        "  CHS Geometry: {} heads x {} sectors/track",
                        alignment.heads, alignment.sectors_per_track
                    ));
                }
            });
        }

        ui.add_space(8.0);

        // Partition table
        if self.partitions.is_empty() {
            ui.label("No partitions found.");
            return;
        }

        egui::Grid::new("partition_table")
            .striped(true)
            .min_col_width(60.0)
            .show(ui, |ui| {
                // Header
                ui.label(egui::RichText::new("#").strong());
                ui.label(egui::RichText::new("Type").strong());
                ui.label(egui::RichText::new("Start LBA").strong());
                ui.label(egui::RichText::new("Size").strong());
                ui.label(egui::RichText::new("Boot").strong());
                ui.end_row();

                for part in &self.partitions {
                    ui.label(format!("{}", part.index));
                    ui.label(&part.type_name);
                    ui.label(format!("{}", part.start_lba));
                    ui.label(partition::format_size(part.size_bytes));
                    ui.label(if part.bootable { "Yes" } else { "" });
                    ui.end_row();
                }
            });
    }
}
