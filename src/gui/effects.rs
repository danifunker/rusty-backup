//! egui's side of controller effects that answer back: file pickers, confirmations and text prompts.
//!
//! A view that owns a controller keeps one [`EffectHost`], hands it the controller's effects,
//! and maps each [`Reply`] to the controller's own answer action, so an answer always returns
//! to the controller that asked. Pickers are async rfd dialogs ([`super::picker`]): the answer
//! comes back from a later `run` or `show`, one picker at a time (a second is answered as a
//! cancel). A confirmation or a text prompt is a modal window drawn by [`EffectHost::show`].
//! A text prompt runs the controller's validator on every edit: its message shows inline and
//! OK (or Enter) works only while it passes.

use std::path::PathBuf;

use rusty_backup::controller::{ChoiceSpec, ConfirmSpec, Effect, PickSpec, TextSpec};

use super::picker;

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
    Chose {
        purpose: &'static str,
        choice: usize,
    },
    /// The controller asked to leave its screen.
    CloseScreen,
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
    choice: Option<ChoiceSpec>,
    text: Option<TextPrompt>,
    /// The picker on screen and the purpose its answer goes back under.
    pick: Option<(&'static str, picker::Pending<Option<PathBuf>>)>,
}

impl EffectHost {
    /// Carry out `effects`: messages land in `messages`; a finished picker's answer comes back too.
    pub(crate) fn run(
        &mut self,
        ctx: &egui::Context,
        effects: Vec<Effect>,
        messages: &mut Vec<String>,
    ) -> Vec<Reply> {
        let mut replies: Vec<Reply> = self.poll_pick().into_iter().collect();
        for effect in effects {
            match effect {
                Effect::Message(text) => messages.push(text),
                Effect::PickFile(spec) => {
                    let kind = if spec.save {
                        picker::Kind::Save
                    } else {
                        picker::Kind::Open
                    };
                    replies.extend(self.pick_own(ctx, spec.purpose, dialog_for(&spec), kind));
                }
                Effect::PickFolder(spec) => {
                    let dialog = dialog_for(&spec);
                    replies.extend(self.pick_own(ctx, spec.purpose, dialog, picker::Kind::Folder));
                }
                Effect::Confirm(spec) => self.confirm = Some(spec),
                Effect::Choose(spec) => self.choice = Some(spec),
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
                Effect::CloseScreen => replies.push(Reply::CloseScreen),
                Effect::Quit => ctx.send_viewport_cmd(egui::ViewportCommand::Close),
            }
        }
        replies
    }

    /// Open `dialog` for the view itself; the answer comes back as `Reply::Picked { purpose, .. }`.
    /// With a picker already on screen it is refused, and the refusal answers now as a cancel.
    pub(crate) fn pick_own(
        &mut self,
        ctx: &egui::Context,
        purpose: &'static str,
        dialog: rfd::AsyncFileDialog,
        kind: picker::Kind,
    ) -> Option<Reply> {
        let pending = match self.pick {
            Some(_) => None,
            None => picker::path(ctx, dialog, kind),
        };
        match pending {
            Some(p) => {
                self.pick = Some((purpose, p));
                self.poll_pick()
            }
            None => {
                log::info!("A file dialog is already open");
                Some(Reply::Picked {
                    purpose,
                    path: None,
                })
            }
        }
    }

    /// The picker's answer once its dialog has closed.
    fn poll_pick(&mut self) -> Option<Reply> {
        let (purpose, pending) = self.pick.as_mut()?;
        let purpose = *purpose;
        let path = pending.poll()?;
        self.pick = None;
        Some(Reply::Picked { purpose, path })
    }

    /// True while a picker, a confirmation or a text prompt waits for an answer.
    pub(crate) fn modal_open(&self) -> bool {
        let prompt = self.confirm.is_some() || self.choice.is_some() || self.text.is_some();
        prompt || self.pick.is_some()
    }

    /// The `purpose` of the confirmation or prompt on screen.
    pub(crate) fn pending_purpose(&self) -> Option<&'static str> {
        let confirm = self.confirm.as_ref().map(|c| c.purpose);
        let choice = self.choice.as_ref().map(|c| c.purpose);
        confirm
            .or(choice)
            .or_else(|| self.text.as_ref().map(|t| t.spec.purpose))
    }

    /// Draw the pending confirmation or prompt; `whose` tells two hosts' windows apart.
    pub(crate) fn show(&mut self, ctx: &egui::Context, whose: &str) -> Option<Reply> {
        if let Some(reply) = self.poll_pick() {
            return Some(reply);
        }
        if self.confirm.is_some() {
            return self.show_confirm(ctx, whose);
        }
        if self.choice.is_some() {
            return self.show_choice(ctx, whose);
        }
        self.show_text(ctx, whose)
    }

    fn show_choice(&mut self, ctx: &egui::Context, whose: &str) -> Option<Reply> {
        let spec = self.choice.as_ref()?;
        let mut answer = None;
        egui::Window::new(titled(&spec.title, whose))
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .show(ctx, |ui| {
                ui.set_max_width(520.0);
                ui.label(&spec.message);
                ui.add_space(6.0);
                ui.horizontal(|ui| {
                    for (i, c) in spec.choices.iter().enumerate() {
                        if ui.button(c.label.as_str()).clicked() {
                            answer = Some(i);
                        }
                    }
                });
                if ui.input(|i| i.key_pressed(egui::Key::Escape)) {
                    answer = Some(spec.dismiss);
                }
            });
        let choice = answer?;
        let spec = self.choice.take()?;
        Some(Reply::Chose {
            purpose: spec.purpose,
            choice,
        })
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

/// The dialog `spec` describes: its title, default name and filters.
fn dialog_for(spec: &PickSpec) -> rfd::AsyncFileDialog {
    let mut dialog = super::file_dialog().set_title(spec.title.as_str());
    if !spec.file_name.is_empty() {
        dialog = dialog.set_file_name(spec.file_name.as_str());
    }
    for (name, exts) in &spec.filters {
        dialog = dialog.add_filter(name.as_str(), exts);
    }
    dialog
}
