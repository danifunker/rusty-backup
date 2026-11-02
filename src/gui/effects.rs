//! egui's side of controller effects that answer back: file pickers and confirmations.
//!
//! A view that owns a controller keeps one [`EffectHost`], hands it the controller's effects,
//! and maps each [`Reply`] to the controller's own answer action, so an answer always returns
//! to the controller that asked. Pickers are blocking rfd dialogs on the UI thread, as the
//! source picker's were; a confirmation is a modal window drawn by [`EffectHost::show`].

use std::path::PathBuf;

use rusty_backup::controller::{ConfirmSpec, Effect, PickSpec};

/// An answer to an effect, for the view to turn into its controller's action.
pub(crate) enum Reply {
    Picked {
        purpose: &'static str,
        path: Option<PathBuf>,
    },
    Confirmed {
        purpose: &'static str,
        yes: bool,
    },
}

#[derive(Default)]
pub(crate) struct EffectHost {
    confirm: Option<ConfirmSpec>,
}

impl EffectHost {
    /// Carry out `effects`: messages land in `messages`, picker answers come back now.
    pub(crate) fn run(
        &mut self,
        ctx: &egui::Context,
        effects: Vec<Effect>,
        messages: &mut Vec<String>,
    ) -> Vec<Reply> {
        let mut replies = Vec::new();
        for effect in effects {
            match effect {
                Effect::Message(text) => messages.push(text),
                Effect::PickFile(spec) => replies.push(Reply::Picked {
                    purpose: spec.purpose,
                    path: pick(&spec, false),
                }),
                Effect::PickFolder(spec) => replies.push(Reply::Picked {
                    purpose: spec.purpose,
                    path: pick(&spec, true),
                }),
                Effect::Confirm(spec) => self.confirm = Some(spec),
                Effect::AskPassword(spec) => {
                    log::error!("no egui handler yet for password {:?}", spec.purpose)
                }
                Effect::CopyToClipboard(text) => ctx.copy_text(text),
                Effect::OpenUrl(url) => ctx.open_url(egui::OpenUrl::new_tab(url)),
            }
        }
        replies
    }

    /// True while a confirmation waits for an answer.
    pub(crate) fn modal_open(&self) -> bool {
        self.confirm.is_some()
    }

    /// Draw the pending confirmation; `whose` tells two hosts' windows apart.
    pub(crate) fn show(&mut self, ctx: &egui::Context, whose: &str) -> Option<Reply> {
        let spec = self.confirm.as_ref()?;
        let mut answer = None;
        let title = if whose.is_empty() {
            spec.title.clone()
        } else {
            format!("{} ({whose})", spec.title)
        };
        egui::Window::new(title)
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .show(ctx, |ui| {
                ui.label(&spec.message);
                ui.add_space(6.0);
                ui.horizontal(|ui| {
                    let mut label = egui::RichText::new(&spec.confirm_label);
                    if spec.destructive {
                        label = label.color(super::theme::danger_muted(ui.visuals()));
                    }
                    if ui.button(label).clicked() {
                        answer = Some(true);
                    }
                    if ui.button("Cancel").clicked() {
                        answer = Some(false);
                    }
                });
            });
        let yes = answer?;
        let spec = self.confirm.take()?;
        Some(Reply::Confirmed {
            purpose: spec.purpose,
            yes,
        })
    }
}

/// Run a picker dialog for `spec`; blocks the UI thread until the user answers.
fn pick(spec: &PickSpec, folder: bool) -> Option<PathBuf> {
    let mut dialog = super::file_dialog().set_title(spec.title.as_str());
    for (name, exts) in &spec.filters {
        dialog = dialog.add_filter(name.as_str(), exts);
    }
    match (folder, spec.save) {
        (true, _) => dialog.pick_folder(),
        (false, true) => dialog.save_file(),
        (false, false) => dialog.pick_file(),
    }
}
