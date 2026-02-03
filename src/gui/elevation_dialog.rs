//! Privilege elevation dialog for Linux.

#![cfg(target_os = "linux")]

/// State for the privilege elevation dialog.
#[derive(Default)]
pub struct ElevationDialog {
    open: bool,
}

impl ElevationDialog {
    pub fn show(&mut self, ctx: &egui::Context) -> ElevationAction {
        let mut action = ElevationAction::None;

        if !self.open {
            return action;
        }

        egui::Window::new("Elevated Privileges Required")
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .show(ctx, |ui| {
                ui.set_min_width(400.0);

                ui.vertical_centered(|ui| {
                    ui.add_space(10.0);
                    ui.label(
                        egui::RichText::new("⚠")
                            .color(egui::Color32::YELLOW)
                            .size(48.0),
                    );
                    ui.add_space(10.0);
                });

                ui.label("Rusty Backup needs elevated privileges to access disk devices.");
                ui.add_space(10.0);
                ui.label("The application will restart with administrator access.");
                ui.add_space(10.0);
                ui.label("You will be prompted to enter your password.");

                ui.add_space(20.0);
                ui.separator();
                ui.add_space(10.0);

                ui.horizontal(|ui| {
                    if ui.button("Cancel").clicked() {
                        action = ElevationAction::Cancel;
                        self.open = false;
                    }

                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if ui
                            .button(egui::RichText::new("Restart with Admin Access").strong())
                            .clicked()
                        {
                            action = ElevationAction::Elevate;
                            self.open = false;
                        }
                    });
                });
            });

        action
    }

    pub fn open(&mut self) {
        self.open = true;
    }

    pub fn is_open(&self) -> bool {
        self.open
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ElevationAction {
    None,
    Cancel,
    Elevate,
}
