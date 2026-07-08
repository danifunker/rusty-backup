use rusty_backup::update::UpdateConfig;

/// Settings dialog state
#[derive(Default)]
pub struct SettingsDialog {
    pub open: bool,
    update_check_enabled: bool,
    update_repo_url: String,
    status_message: Option<String>,
    /// Total recent-file entries across all modes, shown on the "Clear Recent
    /// Files" button. Refreshed when the dialog opens and zeroed after a clear.
    recent_count: usize,
    /// Set when the user clears the recent lists, so the app can also drop the
    /// in-memory mirrors each tab / Commander pane holds (config alone is the
    /// source of truth, but those mirrors only reload at construction). Consumed
    /// via [`Self::take_clear_recents`].
    clear_recents_requested: bool,
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

                    ui.separator();
                    ui.add_space(10.0);
                    ui.heading("Recent Files");
                    ui.add_space(10.0);
                    ui.label(
                        "Forget the recently-opened file lists shown in the Inspect, \
                         Restore, Optical, Archives, and Commander source pickers.",
                    );
                    ui.add_space(5.0);
                    let clear_label = if self.recent_count > 0 {
                        format!("Clear Recent Files ({})", self.recent_count)
                    } else {
                        "Clear Recent Files".to_string()
                    };
                    if ui
                        .add_enabled(self.recent_count > 0, egui::Button::new(clear_label))
                        .clicked()
                    {
                        self.clear_recent_files();
                    }
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
        self.recent_count = config.recent_files.total();
        self.clear_recents_requested = false;
        #[cfg(windows)]
        {
            self.file_associations_enabled = config.file_associations_enabled;
        }
        self.status_message = None;
        self.open = true;
    }

    /// Forget every mode's recent-files list: clear + persist config now, and
    /// flag the app to drop the tabs' in-memory mirrors (see
    /// [`Self::take_clear_recents`]).
    fn clear_recent_files(&mut self) {
        let mut config = UpdateConfig::load();
        let count = config.recent_files.total();
        config.recent_files.clear();
        match config.save() {
            Ok(_) => {
                self.recent_count = 0;
                self.clear_recents_requested = true;
                self.status_message = Some(if count == 1 {
                    "Cleared 1 recent file.".to_string()
                } else {
                    format!("Cleared {count} recent files.")
                });
            }
            Err(e) => {
                self.status_message = Some(format!("Error clearing recent files: {e}"));
            }
        }
    }

    /// True once (then resets) after the user clears recents, so the app can
    /// wipe the in-memory mirrors each tab / Commander pane holds.
    pub fn take_clear_recents(&mut self) -> bool {
        std::mem::take(&mut self.clear_recents_requested)
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
