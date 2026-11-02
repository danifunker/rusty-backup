//! egui's side of controller effects that answer back: file pickers, confirmations and text prompts.
//!
//! A view that owns a controller keeps one [`EffectHost`], hands it the controller's effects,
//! and maps each [`Reply`] to the controller's own answer action, so an answer always returns
//! to the controller that asked. Pickers are blocking rfd dialogs on the UI thread, as the
//! source picker's were; a confirmation or a text prompt is a modal window drawn by
//! [`EffectHost::show`]. A text prompt runs the controller's validator on every edit: its
//! message shows inline and OK (or Enter) works only while it passes.

use std::path::PathBuf;

use rusty_backup::controller::{ConfirmSpec, Effect, PickSpec, TextSpec};

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
    Answered {
        purpose: &'static str,
        text: Option<String>,
    },
}

/// A text prompt on screen: its spec, the field's text, and whether the user has typed yet.
struct TextPrompt {
    spec: TextSpec,
    input: String,
    edited: bool,
    focused: bool,
}

#[derive(Default)]
pub(crate) struct EffectHost {
    confirm: Option<ConfirmSpec>,
    text: Option<TextPrompt>,
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
                Effect::AskText(spec) => {
                    self.text = Some(TextPrompt {
                        input: spec.initial.clone(),
                        spec,
                        edited: false,
                        focused: false,
                    })
                }
                Effect::AskPassword(spec) => {
                    log::error!("no egui handler yet for password {:?}", spec.purpose)
                }
                Effect::CopyToClipboard(text) => ctx.copy_text(text),
                Effect::OpenUrl(url) => ctx.open_url(egui::OpenUrl::new_tab(url)),
            }
        }
        replies
    }

    /// True while a confirmation or text prompt waits for an answer.
    pub(crate) fn modal_open(&self) -> bool {
        self.confirm.is_some() || self.text.is_some()
    }

    /// Draw the pending confirmation or prompt; `whose` tells two hosts' windows apart.
    pub(crate) fn show(&mut self, ctx: &egui::Context, whose: &str) -> Option<Reply> {
        if self.confirm.is_some() {
            return self.show_confirm(ctx, whose);
        }
        self.show_text(ctx, whose)
    }

    fn show_confirm(&mut self, ctx: &egui::Context, whose: &str) -> Option<Reply> {
        let spec = self.confirm.as_ref()?;
        let mut answer = None;
        egui::Window::new(titled(&spec.title, whose))
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

    fn show_text(&mut self, ctx: &egui::Context, whose: &str) -> Option<Reply> {
        let p = self.text.as_mut()?;
        let mut answer: Option<Option<String>> = None;
        egui::Window::new(titled(&p.spec.title, whose))
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .show(ctx, |ui| {
                ui.label(&p.spec.prompt);
                let resp = ui.text_edit_singleline(&mut p.input);
                if !p.focused {
                    resp.request_focus();
                    p.focused = true;
                }
                if resp.changed() {
                    p.edited = true;
                }
                let check = p.spec.validate(&p.input);
                // The controller's refusal stands until the user edits; then the live rule speaks.
                let shown = match (&p.spec.error, p.edited) {
                    (Some(e), false) => Some(e.clone()),
                    _ => check.as_ref().err().filter(|e| !e.is_empty()).cloned(),
                };
                if let Some(e) = shown {
                    ui.colored_label(super::theme::danger_muted(ui.visuals()), e);
                }
                let valid = check.is_ok();
                ui.add_space(6.0);
                ui.horizontal(|ui| {
                    let ok = egui::Button::new(p.spec.ok_label.as_str());
                    if ui.add_enabled(valid, ok).clicked() {
                        answer = Some(Some(p.input.clone()));
                    }
                    if ui.button("Cancel").clicked() {
                        answer = Some(None);
                    }
                });
                let enter = resp.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
                if valid && enter {
                    answer = Some(Some(p.input.clone()));
                }
                if ui.input(|i| i.key_pressed(egui::Key::Escape)) {
                    answer = Some(None);
                }
            });
        let text = answer?;
        let p = self.text.take()?;
        Some(Reply::Answered {
            purpose: p.spec.purpose,
            text,
        })
    }
}

fn titled(title: &str, whose: &str) -> String {
    if whose.is_empty() {
        title.to_string()
    } else {
        format!("{title} ({whose})")
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
