use rusty_backup::update::UpdateConfig;

/// Settings dialog state
#[derive(Default)]
pub struct SettingsDialog {
    pub open: bool,
    chdman_path: String,
    update_check_enabled: bool,
    update_repo_url: String,
    status_message: Option<String>,
}

impl SettingsDialog {
    pub fn show(&mut self, ctx: &egui::Context) {
        if !self.open {
            return;
        }

        let mut open = self.open;
        egui::Window::new("Settings")
            .open(&mut open)
            .resizable(false)
            .collapsible(false)
            .show(ctx, |ui| {
                egui::ScrollArea::vertical().show(ui, |ui| {
                    ui.heading("chdman Configuration");
                    ui.add_space(10.0);

                    ui.label("Path to chdman executable:");
                    ui.horizontal(|ui| {
                        ui.text_edit_singleline(&mut self.chdman_path);
                        if ui.button("Browse...").clicked() {
                            if let Some(path) = rfd::FileDialog::new()
                                .set_title("Select chdman executable")
                                .pick_file()
                            {
                                self.chdman_path = path.to_string_lossy().to_string();
                            }
                        }
                    });

                    ui.label("Leave empty to use system PATH");
                    ui.add_space(20.0);

                    ui.separator();
                    ui.add_space(10.0);

                    // macOS permissions info
                    #[cfg(target_os = "macos")]
                    {
                        ui.heading("macOS Permissions");
                        ui.add_space(10.0);

                        ui.horizontal(|ui| {
                            ui.label(
                                egui::RichText::new("ℹ")
                                    .size(16.0)
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
                            self.open = false;
                        }
                    });
                });
            });

        self.open = open;
    }

    pub fn open_dialog(&mut self) {
        // Load current config
        let config = UpdateConfig::load();
        self.chdman_path = config.chdman_path.unwrap_or_default();
        self.update_check_enabled = config.update_check.enabled;
        self.update_repo_url = config.update_check.repository_url;
        self.status_message = None;
        self.open = true;
    }

    fn save_settings(&mut self) {
        let mut config = UpdateConfig::load();

        // Update chdman path
        config.chdman_path = if self.chdman_path.trim().is_empty() {
            None
        } else {
            Some(self.chdman_path.trim().to_string())
        };

        // Update update check settings
        config.update_check.enabled = self.update_check_enabled;
        config.update_check.repository_url = self.update_repo_url.trim().to_string();

        match config.save() {
            Ok(_) => {
                self.status_message = Some(
                    "Settings saved successfully! Restart to apply update check changes."
                        .to_string(),
                );
                // Note: chdman detection will use new path on next backup
                // Note: update check settings take effect on next app start
            }
            Err(e) => {
                self.status_message = Some(format!("Error saving settings: {}", e));
            }
        }
    }
}
