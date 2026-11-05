//! Remote image picker for the Inspect tab: a shell over `controller::remote_pick`.
//!
//! Draws the daemon's host listing inline in the Inspect main area, maps double-clicks to
//! [`RemotePickAction`]s and answers the connect prompt through an [`EffectHost`]. A picked image
//! comes back from [`RemoteBrowsePanel::take_inspect_request`] for Inspect to open over the block
//! tier; the panel then collapses.

use eframe::egui;

use rusty_backup::controller::remote_pick::{RemoteImage, RemotePickAction, RemotePickController};
use rusty_backup::controller::{Controller, Effect};

use super::effects::{EffectHost, Reply};

/// Inline remote browse panel for the Inspect tab.
pub struct RemoteBrowsePanel {
    ctl: RemotePickController,
    effects: EffectHost,
    pending_fx: Vec<Effect>,
    /// Prompt answers and messages for the log, drained by the caller.
    messages: Vec<String>,
}

impl Default for RemoteBrowsePanel {
    fn default() -> Self {
        RemoteBrowsePanel {
            ctl: RemotePickController::new(std::sync::Arc::new(super::ui_logger::EguiWake)),
            effects: EffectHost::default(),
            pending_fx: Vec::new(),
            messages: Vec::new(),
        }
    }
}

impl RemoteBrowsePanel {
    fn act(&mut self, action: RemotePickAction) {
        let fx = self.ctl.apply(action);
        self.pending_fx.extend(fx);
    }

    /// Start the connect flow (asks for the daemon's host:port).
    pub fn open(&mut self) {
        self.act(RemotePickAction::Connect);
    }

    /// Re-open the host picker on the connection of an image already open (no reconnect).
    pub fn browse_on(&mut self, image: RemoteImage) {
        self.act(RemotePickAction::BrowseOn(image));
    }

    /// Drop the browser and reset.
    pub fn disconnect(&mut self) {
        self.act(RemotePickAction::Disconnect);
    }

    /// Status lines since the last call.
    pub fn take_log(&mut self) -> Vec<String> {
        let mut lines = std::mem::take(&mut self.messages);
        lines.extend(self.ctl.take_log());
        lines
    }

    /// A picked remote image for Inspect to open over the block tier.
    pub fn take_inspect_request(&mut self) -> Option<RemoteImage> {
        self.ctl.take_request()
    }

    /// The Disconnect button, inside the Inspect source-bar row.
    pub fn source_bar_controls(&mut self, ui: &mut egui::Ui) {
        if !self.ctl.can_disconnect() {
            return;
        }
        if ui
            .add_enabled(
                !self.ctl.transition_running(),
                egui::Button::new(format!("Disconnect from {}", self.ctl.addr())),
            )
            .on_hover_text("Close the connection to the remote daemon")
            .clicked()
        {
            self.act(RemotePickAction::Disconnect);
        }
    }

    /// Render the panel; returns `true` while it owns the Inspect main area.
    pub fn show(&mut self, ui: &mut egui::Ui) -> bool {
        let fx = self.ctl.tick();
        self.pending_fx.extend(fx);
        self.run_effects(ui.ctx());
        if !self.ctl.is_active() {
            return false;
        }

        ui.separator();
        let mut action = None;
        if let Some(snap) = self.ctl.working() {
            ui.add_space(16.0);
            super::progress::show_job(ui, &snap);
        } else if let Some(e) = self.ctl.error() {
            ui.add_space(8.0);
            ui.colored_label(super::theme::danger_muted(ui.visuals()), e);
        } else if self.ctl.is_loaded() {
            ui.label(egui::RichText::new(self.ctl.hint()).weak().small());
            action = self.render_grid(ui);
        }
        if let Some(a) = action {
            self.act(a);
            self.run_effects(ui.ctx());
        }
        true
    }

    fn run_effects(&mut self, ctx: &egui::Context) {
        let fx = std::mem::take(&mut self.pending_fx);
        let mut replies = self.effects.run(ctx, fx, &mut self.messages);
        replies.extend(self.effects.show(ctx, "Remote"));
        for reply in replies {
            match reply {
                Reply::Answered { purpose, text } => {
                    self.act(RemotePickAction::Answered { purpose, text })
                }
                Reply::Picked { .. }
                | Reply::Confirmed { .. }
                | Reply::Chose { .. }
                | Reply::CloseScreen => {}
            }
        }
        if !self.pending_fx.is_empty() {
            ctx.request_repaint();
        }
    }

    /// The listing grid; returns what a double-click asked for.
    fn render_grid(&self, ui: &mut egui::Ui) -> Option<RemotePickAction> {
        let mut action = None;
        egui::ScrollArea::vertical()
            .auto_shrink([false, false])
            .show(ui, |ui| {
                egui::Grid::new("remote_inspect_grid")
                    .num_columns(3)
                    .striped(true)
                    .spacing([12.0, 2.0])
                    .show(ui, |ui| {
                        ui.strong("Name");
                        ui.strong("Size");
                        ui.strong("Type");
                        ui.end_row();

                        for row in self.ctl.rows() {
                            if row.parent {
                                if ui.selectable_label(false, "..").double_clicked() {
                                    action = Some(RemotePickAction::Up);
                                }
                                ui.label("");
                                ui.label("dir");
                                ui.end_row();
                                continue;
                            }
                            let color = if row.is_dir {
                                super::theme::info(ui.visuals())
                            } else if row.openable {
                                super::theme::accent(ui.visuals())
                            } else {
                                ui.visuals().text_color()
                            };
                            let mut resp = ui.selectable_label(
                                false,
                                egui::RichText::new(row.name.as_str()).color(color),
                            );
                            if row.openable {
                                resp = resp.on_hover_text("Double-click to open this image");
                            }
                            if resp.double_clicked() {
                                if row.is_dir {
                                    action = Some(RemotePickAction::Enter(row.name.clone()));
                                } else if row.openable {
                                    action = Some(RemotePickAction::OpenImage(row.path.clone()));
                                }
                            }
                            ui.label(row.size.as_str());
                            ui.label(row.type_tag.as_str());
                            ui.end_row();
                        }
                    });
            });
        action
    }
}
