//! macOS daemon installation dialog.

use egui::{Button, Color32, RichText, Window};

/// Dialog for installing the privileged helper daemon on macOS.
#[derive(Default)]
pub struct DaemonDialog {
    show: bool,
    action: DaemonAction,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DaemonAction {
    None,
    Install,
    Cancel,
}

impl Default for DaemonAction {
    fn default() -> Self {
        DaemonAction::None
    }
}

impl DaemonDialog {
    pub fn show(&mut self) {
        self.show = true;
        self.action = DaemonAction::None;
    }

    pub fn hide(&mut self) {
        self.show = false;
    }

    pub fn action(&self) -> DaemonAction {
        self.action
    }

    pub fn reset_action(&mut self) {
        self.action = DaemonAction::None;
    }

    pub fn render(&mut self, ctx: &egui::Context) {
        if !self.show {
            return;
        }

        Window::new("Install Privileged Helper")
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .show(ctx, |ui| {
                ui.vertical(|ui| {
                    ui.spacing_mut().item_spacing.y = 10.0;

                    // Warning icon and title
                    ui.horizontal(|ui| {
                        ui.label(RichText::new("⚠").size(32.0).color(Color32::from_rgb(255, 165, 0)));
                        ui.vertical(|ui| {
                            ui.label(RichText::new("Privileged Helper Required").strong().size(16.0));
                            ui.label("Rusty Backup needs to install a helper daemon to access disks.");
                        });
                    });

                    ui.separator();

                    // Explanation
                    ui.label("What will happen:");
                    ui.indent("install_steps", |ui| {
                        ui.label("• macOS will prompt for your administrator password");
                        ui.label("• A helper daemon will be installed to /Library/PrivilegedHelperTools/");
                        ui.label("• The daemon runs only when needed (on-demand via launchd)");
                        ui.label("• You can uninstall it anytime from Settings");
                    });

                    ui.add_space(10.0);

                    ui.label(RichText::new("This is a one-time setup.").italics());

                    ui.add_space(10.0);
                    ui.separator();

                    // Buttons
                    ui.horizontal(|ui| {
                        if ui.button("Cancel").clicked() {
                            self.action = DaemonAction::Cancel;
                            self.hide();
                        }

                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            if ui
                                .add(Button::new(RichText::new("Install Helper").strong()).fill(Color32::from_rgb(0, 122, 255)))
                                .clicked()
                            {
                                self.action = DaemonAction::Install;
                                self.hide();
                            }
                        });
                    });
                });
            });
    }
}
