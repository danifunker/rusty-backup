use rusty_backup::controller::settings::{SettingsAction, SettingsController};
use rusty_backup::controller::Controller as _;

/// The Settings window over `SettingsController`; the form's state lives there.
pub struct SettingsDialog {
    ctl: SettingsController,
}

impl SettingsDialog {
    /// The dialog over the app's config store, so its Clear goes through the app's recent files.
    pub fn new(store: rusty_backup::controller::app::RecentHandle) -> Self {
        SettingsDialog {
            ctl: SettingsController::with_store(Box::new(store)),
        }
    }

    pub fn show(&mut self, ctx: &egui::Context) {
        if !self.ctl.is_open() {
            return;
        }

        let mut open = true;
        let mut close_requested = false;
        let mut update_check = self.ctl.update_check();
        let mut repo_url = self.ctl.repo_url().to_string();
        #[cfg(windows)]
        let mut file_associations = self.ctl.file_associations();
        let mut actions: Vec<SettingsAction> = Vec::new();
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
                                    .color(super::theme::info(ui.visuals()))
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

                    if ui
                        .checkbox(&mut update_check, "Check for updates at startup")
                        .changed()
                    {
                        actions.push(SettingsAction::SetUpdateCheck(update_check));
                    }
                    ui.add_space(5.0);

                    ui.label("GitHub repository URL:");
                    if ui.text_edit_singleline(&mut repo_url).changed() {
                        actions.push(SettingsAction::SetRepoUrl(repo_url.clone()));
                    }
                    ui.label("(e.g., https://github.com/owner/repo)");
                    ui.add_space(10.0);

                    #[cfg(windows)]
                    {
                        ui.separator();
                        ui.add_space(10.0);
                        ui.heading("File Associations");
                        ui.add_space(10.0);
                        if ui
                            .checkbox(
                                &mut file_associations,
                                "Associate disk image files with Rusty Backup",
                            )
                            .changed()
                        {
                            actions.push(SettingsAction::SetFileAssociations(file_associations));
                        }
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
                    let clear_label = self.ctl.clear_recent_label();
                    if ui
                        .add_enabled(self.ctl.can_clear_recent(), egui::Button::new(clear_label))
                        .clicked()
                    {
                        actions.push(SettingsAction::ClearRecent);
                    }
                    ui.add_space(10.0);

                    if let Some(msg) = self.ctl.status() {
                        ui.colored_label(
                            if msg.starts_with("Error") {
                                super::theme::danger(ui.visuals())
                            } else {
                                super::theme::success(ui.visuals())
                            },
                            msg,
                        );
                    }

                    ui.add_space(10.0);
                    ui.horizontal(|ui| {
                        if ui.button("Save").clicked() {
                            actions.push(SettingsAction::Save);
                        }
                        if ui.button("Cancel").clicked() {
                            close_requested = true;
                        }
                    });
                });
            });

        for action in actions {
            self.ctl.apply(action);
        }
        // Close if the titlebar X cleared `open`, or the Cancel button asked to (issue #43).
        if !open || close_requested {
            self.ctl.apply(SettingsAction::Close);
        }
    }

    pub fn open_dialog(&mut self) {
        self.ctl.apply(SettingsAction::Open);
    }

    /// True once after the user cleared recents, so the app can wipe each tab's in-memory list.
    pub fn take_clear_recents(&mut self) -> bool {
        self.ctl.take_cleared_recents()
    }
}
