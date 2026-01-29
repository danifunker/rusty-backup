use std::path::PathBuf;

use rusty_backup::device::DiskDevice;

use super::progress::LogPanel;

/// State for the Restore tab (UI skeleton for Phase 1).
pub struct RestoreTab {
    backup_folder: Option<PathBuf>,
    selected_device_idx: Option<usize>,
}

impl Default for RestoreTab {
    fn default() -> Self {
        Self {
            backup_folder: None,
            selected_device_idx: None,
        }
    }
}

impl RestoreTab {
    pub fn show(
        &mut self,
        ui: &mut egui::Ui,
        devices: &[DiskDevice],
        _log: &mut LogPanel,
    ) {
        ui.heading("Restore Backup");
        ui.add_space(8.0);

        // Backup folder selection
        ui.horizontal(|ui| {
            ui.label("Backup Folder:");
            let label = self
                .backup_folder
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "No folder selected".into());
            ui.label(&label);
            if ui.button("Browse...").clicked() {
                if let Some(path) = rfd::FileDialog::new().pick_folder() {
                    self.backup_folder = Some(path);
                }
            }
        });

        // Target device
        ui.horizontal(|ui| {
            ui.label("Target Device:");

            let current_label = self
                .selected_device_idx
                .and_then(|idx| devices.get(idx))
                .map(|d| d.display_name())
                .unwrap_or_else(|| "Select a target device...".into());

            egui::ComboBox::from_id_salt("restore_target")
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

        ui.add_space(12.0);

        // Placeholder for partition layout configuration
        if self.backup_folder.is_some() {
            ui.label(egui::RichText::new("Partition Layout").strong());
            ui.separator();
            ui.colored_label(
                egui::Color32::GRAY,
                "Partition layout configuration will appear here once a backup is loaded.",
            );
            ui.add_space(12.0);
        }

        // Action buttons
        ui.horizontal(|ui| {
            let can_start =
                self.backup_folder.is_some() && self.selected_device_idx.is_some();
            if ui
                .add_enabled(can_start, egui::Button::new("Restore"))
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
            "Restore functionality will be available in a future update.",
        );
    }
}
