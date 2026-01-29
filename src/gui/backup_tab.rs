use std::path::PathBuf;

use rusty_backup::device::DiskDevice;

use super::progress::LogPanel;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ChecksumType {
    Crc32,
    Sha256,
}

/// State for the Backup tab (UI skeleton for Phase 1).
pub struct BackupTab {
    selected_device_idx: Option<usize>,
    destination_folder: Option<PathBuf>,
    resize_partitions: bool,
    split_archives: bool,
    split_size_mib: u32,
    checksum_type: ChecksumType,
}

impl Default for BackupTab {
    fn default() -> Self {
        Self {
            selected_device_idx: None,
            destination_folder: None,
            resize_partitions: true,
            split_archives: true,
            split_size_mib: 4000,
            checksum_type: ChecksumType::Sha256,
        }
    }
}

impl BackupTab {
    pub fn show(
        &mut self,
        ui: &mut egui::Ui,
        devices: &[DiskDevice],
        _log: &mut LogPanel,
    ) {
        ui.heading("Backup Disk");
        ui.add_space(8.0);

        // Source device
        ui.horizontal(|ui| {
            ui.label("Source Device:");

            let current_label = self
                .selected_device_idx
                .and_then(|idx| devices.get(idx))
                .map(|d| d.display_name())
                .unwrap_or_else(|| "Select a device...".into());

            egui::ComboBox::from_id_salt("backup_source")
                .selected_text(&current_label)
                .width(400.0)
                .show_ui(ui, |ui| {
                    for (i, device) in devices.iter().enumerate() {
                        ui.selectable_value(
                            &mut self.selected_device_idx,
                            Some(i),
                            device.display_name(),
                        );
                    }
                });
        });

        // Destination folder
        ui.horizontal(|ui| {
            ui.label("Destination:");
            let label = self
                .destination_folder
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "No folder selected".into());
            ui.label(&label);
            if ui.button("Browse...").clicked() {
                if let Some(path) = rfd::FileDialog::new().pick_folder() {
                    self.destination_folder = Some(path);
                }
            }
        });

        ui.add_space(12.0);
        ui.label(egui::RichText::new("Options").strong());
        ui.separator();

        // Resize option
        ui.checkbox(&mut self.resize_partitions, "Resize partitions to minimum size");

        // Split archives
        ui.horizontal(|ui| {
            ui.checkbox(&mut self.split_archives, "Split archives");
            if self.split_archives {
                ui.label("Split size (MiB):");
                ui.add(egui::DragValue::new(&mut self.split_size_mib).range(100..=64000));
            }
        });

        // Checksum type
        ui.horizontal(|ui| {
            ui.label("Checksum:");
            ui.radio_value(&mut self.checksum_type, ChecksumType::Sha256, "SHA-256");
            ui.radio_value(&mut self.checksum_type, ChecksumType::Crc32, "CRC32");
        });

        ui.add_space(16.0);

        // Action buttons
        ui.horizontal(|ui| {
            let can_start =
                self.selected_device_idx.is_some() && self.destination_folder.is_some();
            if ui
                .add_enabled(can_start, egui::Button::new("Start Backup"))
                .on_hover_text("Not yet implemented")
                .clicked()
            {
                // Phase 1: not yet implemented
            }
            if ui
                .add_enabled(false, egui::Button::new("Cancel"))
                .clicked()
            {
                // Phase 1: not yet implemented
            }
        });

        ui.add_space(8.0);
        ui.colored_label(
            egui::Color32::GRAY,
            "Backup functionality will be available in a future update.",
        );
    }
}
