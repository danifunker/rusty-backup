use rusty_backup::update::UpdateConfig;

/// Settings dialog state
#[derive(Default)]
pub struct SettingsDialog {
    pub open: bool,
    update_check_enabled: bool,
    update_repo_url: String,
    status_message: Option<String>,
    /// Windows only: register Rusty Backup as a handler for disk-image files.
    #[cfg(windows)]
    file_associations_enabled: bool,
}

impl SettingsDialog {
    pub fn show(&mut self, ctx: &egui::Context) {
        if !self.open {
            return;
        }

        let mut open = self.open;
        let mut close_requested = false;
        egui::Window::new("Settings")
            .open(&mut open)
            .resizable(false)
            .collapsible(false)
            .show(ctx, |ui| {
                egui::ScrollArea::vertical().show(ui, |ui| {
                    // macOS permissions info
                    #[cfg(target_os = "macos")]
                    {
                        ui.heading("macOS Permissions");
                        ui.add_space(10.0);

                        ui.horizontal(|ui| {
                            ui.label(
                                egui::RichText::new("Info:")
                                    .size(14.0)
                                    .color(egui::Color32::from_rgb(0, 122, 255))
                            );
                            ui.vertical(|ui| {
                                ui.label("Rusty Backup uses sudo to request administrator privileges.");
                                ui.label("You'll be prompted for your password when accessing disk devices.");
                            });
                        });

                        ui.add_space(20.0);

                        ui.separator();
                        ui.add_space(10.0);
                    }

                    ui.heading("Update Check");
                    ui.add_space(10.0);

                    ui.checkbox(&mut self.update_check_enabled, "Check for updates at startup");
                    ui.add_space(5.0);

                    ui.label("GitHub repository URL:");
                    ui.text_edit_singleline(&mut self.update_repo_url);
                    ui.label("(e.g., https://github.com/owner/repo)");
                    ui.add_space(10.0);

                    #[cfg(windows)]
                    {
                        ui.separator();
                        ui.add_space(10.0);
                        ui.heading("File Associations");
                        ui.add_space(10.0);
                        ui.checkbox(
                            &mut self.file_associations_enabled,
                            "Associate disk image files with Rusty Backup",
                        );
                        ui.label(
                            "Adds Rusty Backup to the Windows \"Open with\" list for disk \
                             image files (.img, .vhd, .chd, .adf, ...).",
                        );
                        ui.add_space(10.0);
                    }

                    if let Some(ref msg) = self.status_message {
                        ui.colored_label(
                            if msg.starts_with("Error") {
                                egui::Color32::RED
                            } else {
                                egui::Color32::GREEN
                            },
                            msg,
                        );
                    }

                    ui.add_space(10.0);
                    ui.horizontal(|ui| {
                        if ui.button("Save").clicked() {
                            self.save_settings();
                        }
                        if ui.button("Cancel").clicked() {
                            close_requested = true;
                        }
                    });
                });
            });

        // Close if the titlebar X cleared `open`, or the Cancel button asked to.
        // (Writing `self.open = open` alone would clobber a Cancel-driven close,
        // which is the bug behind issue #43.)
        self.open = open && !close_requested;
    }

    pub fn open_dialog(&mut self) {
        let config = UpdateConfig::load();
        self.update_check_enabled = config.update_check.enabled;
        self.update_repo_url = config.update_check.repository_url;
        #[cfg(windows)]
        {
            self.file_associations_enabled = config.file_associations_enabled;
        }
        self.status_message = None;
        self.open = true;
    }

    fn save_settings(&mut self) {
        let mut config = UpdateConfig::load();

        config.update_check.enabled = self.update_check_enabled;
        config.update_check.repository_url = self.update_repo_url.trim().to_string();

        // Windows: apply the file-association toggle (register/unregister under
        // HKCU) before persisting the new state.
        #[cfg(windows)]
        {
            let was_enabled = config.file_associations_enabled;
            config.file_associations_enabled = self.file_associations_enabled;
            if self.file_associations_enabled {
                let _ = rusty_backup::os::file_assoc::register_file_associations();
                config.assoc_registered_version = Some(env!("APP_VERSION").to_string());
            } else if was_enabled {
                let _ = rusty_backup::os::file_assoc::unregister_file_associations();
                config.assoc_registered_version = None;
            }
        }

        match config.save() {
            Ok(_) => {
                self.status_message = Some(
                    "Settings saved successfully! Restart to apply update check changes."
                        .to_string(),
                );
            }
            Err(e) => {
                self.status_message = Some(format!("Error saving settings: {}", e));
            }
        }
    }
}
