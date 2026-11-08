//! GTK's side of controller effects: portal-aware pickers and libadwaita alerts.
//!
//! Each view hands its controller's effects to [`EffectHost::run`] with a callback that maps a
//! [`Reply`] to that controller's answer action, so an answer returns to whoever asked.

use std::path::PathBuf;
use std::rc::Rc;

use adw::prelude::*;
use gtk::{gio, glib};
use rusty_backup::controller::app::picker_start_folder;
use rusty_backup::controller::{ChoiceSpec, ConfirmSpec, Effect, PasswordSpec, PickSpec, TextSpec};

use crate::widgets::plain;

/// An answer to an effect, for the view to turn into its controller's action.
pub enum Reply {
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
    CloseScreen,
}

pub type ReplyFn = Rc<dyn Fn(Reply)>;

/// The GUI suite's answer to a picker, from `RUSTY_BACKUP_TEST_PICKS` (`purpose<TAB>path` lines).
/// Read on every pick so the suite can change it between steps; an unlisted purpose cancels.
fn scripted_pick(purpose: &str) -> Option<Option<PathBuf>> {
    let file = std::env::var_os("RUSTY_BACKUP_TEST_PICKS")?;
    let text = std::fs::read_to_string(file).unwrap_or_default();
    let found = text.lines().find_map(|l| {
        let (p, path) = l.split_once('\t')?;
        (p == purpose).then(|| PathBuf::from(path))
    });
    Some(found)
}

#[derive(Clone)]
pub struct EffectHost {
    window: adw::ApplicationWindow,
    toasts: adw::ToastOverlay,
}

impl EffectHost {
    pub fn new(window: adw::ApplicationWindow, toasts: adw::ToastOverlay) -> Self {
        EffectHost { window, toasts }
    }

    /// Carry out `effects`; no `_` arm, so a new `Effect` fails to compile until handled here.
    pub fn run(&self, effects: Vec<Effect>, reply: &ReplyFn) {
        for effect in effects {
            match effect {
                Effect::PickFile(spec) => self.pick(spec, false, reply.clone()),
                Effect::PickFolder(spec) => self.pick(spec, true, reply.clone()),
                Effect::Confirm(spec) => self.confirm(spec, reply.clone()),
                Effect::Choose(spec) => self.choose(spec, reply.clone()),
                Effect::AskText(spec) => self.ask_text(spec, reply.clone()),
                Effect::AskPassword(spec) => self.ask_password(spec, reply.clone()),
                Effect::Message(text) => self.toasts.add_toast(adw::Toast::new(&plain(&text))),
                Effect::OpenUrl(url) => {
                    gtk::UriLauncher::new(&url).launch(
                        Some(&self.window),
                        gio::Cancellable::NONE,
                        |_| {},
                    );
                }
                Effect::CopyToClipboard(text) => self.window.clipboard().set_text(&plain(&text)),
                Effect::CloseScreen => reply(Reply::CloseScreen),
                Effect::Quit => self.window.close(),
            }
        }
    }

    /// True while a dialog covers the window, so its keys are the dialog's.
    pub fn dialog_open(&self) -> bool {
        self.window.visible_dialog().is_some()
    }

    /// True while another dialog covers `below`, such as the question its own button asked.
    pub fn dialog_over(&self, below: &adw::Dialog) -> bool {
        self.window.visible_dialog().is_some_and(|d| &d != below)
    }

    /// Open `dialog` over the window.
    pub fn present(&self, dialog: &adw::Dialog) {
        dialog.present(Some(&self.window));
    }

    /// Pick several host files; `done` gets none when cancelled.
    pub fn pick_files(&self, title: &str, done: impl FnOnce(Vec<PathBuf>) + 'static) {
        let dialog = gtk::FileDialog::builder().title(title).modal(true).build();
        if let Some(home) = picker_start_folder() {
            dialog.set_initial_folder(Some(&gio::File::for_path(home)));
        }
        dialog.open_multiple(Some(&self.window), gio::Cancellable::NONE, move |result| {
            let files = result.ok().map(|m| {
                let items = (0..m.n_items()).filter_map(|i| m.item(i));
                let files = items.filter_map(|o| o.downcast::<gio::File>().ok());
                files.filter_map(|f| f.path()).collect()
            });
            done(files.unwrap_or_default());
        });
    }

    fn pick(&self, spec: PickSpec, folder: bool, reply: ReplyFn) {
        if let Some(answer) = scripted_pick(spec.purpose) {
            let purpose = spec.purpose;
            glib::idle_add_local_once(move || {
                reply(Reply::Picked {
                    purpose,
                    path: answer,
                })
            });
            return;
        }
        let dialog = gtk::FileDialog::builder()
            .title(plain(&spec.title).as_ref())
            .modal(true)
            .build();
        if let Some(home) = picker_start_folder() {
            dialog.set_initial_folder(Some(&gio::File::for_path(home)));
        }
        if !spec.file_name.is_empty() {
            dialog.set_initial_name(Some(&plain(&spec.file_name)));
        }
        if !spec.filters.is_empty() {
            let filters = gio::ListStore::new::<gtk::FileFilter>();
            for (name, exts) in &spec.filters {
                let filter = gtk::FileFilter::new();
                filter.set_name(Some(name));
                for ext in exts {
                    match ext.as_str() {
                        "*" => filter.add_pattern("*"),
                        _ => filter.add_suffix(ext),
                    }
                }
                filters.append(&filter);
            }
            dialog.set_filters(Some(&filters));
        }
        let purpose = spec.purpose;
        let done = move |result: Result<gio::File, glib::Error>| {
            let path = result.ok().and_then(|f| f.path());
            reply(Reply::Picked { purpose, path });
        };
        let parent = Some(&self.window);
        if folder {
            dialog.select_folder(parent, gio::Cancellable::NONE, done);
        } else if spec.save {
            dialog.save(parent, gio::Cancellable::NONE, done);
        } else {
            dialog.open(parent, gio::Cancellable::NONE, done);
        }
    }

    fn confirm(&self, spec: ConfirmSpec, reply: ReplyFn) {
        let dialog = adw::AlertDialog::new(Some(&plain(&spec.title)), Some(&plain(&spec.message)));
        dialog.add_response("cancel", "Cancel");
        dialog.add_response("ok", &spec.confirm_label);
        dialog.set_close_response("cancel");
        dialog.set_default_response(Some("cancel"));
        if spec.destructive {
            dialog.set_response_appearance("ok", adw::ResponseAppearance::Destructive);
        }
        let purpose = spec.purpose;
        dialog.connect_response(None, move |_, id| {
            reply(Reply::Confirmed {
                purpose,
                yes: id == "ok",
            });
        });
        dialog.present(Some(&self.window));
    }

    fn choose(&self, spec: ChoiceSpec, reply: ReplyFn) {
        let dialog = adw::AlertDialog::new(Some(&plain(&spec.title)), Some(&plain(&spec.message)));
        for (i, choice) in spec.choices.iter().enumerate() {
            dialog.add_response(&i.to_string(), &plain(&choice.label));
        }
        dialog.set_close_response(&spec.dismiss.to_string());
        let purpose = spec.purpose;
        let dismiss = spec.dismiss;
        dialog.connect_response(None, move |_, id| {
            let choice = id.parse().unwrap_or(dismiss);
            reply(Reply::Chose { purpose, choice });
        });
        dialog.present(Some(&self.window));
    }

    /// The controller's validator runs on every edit: its message shows inline and gates OK.
    fn ask_text(&self, spec: TextSpec, reply: ReplyFn) {
        let dialog = adw::AlertDialog::new(Some(&plain(&spec.title)), Some(&plain(&spec.prompt)));
        dialog.add_response("cancel", "Cancel");
        dialog.add_response("ok", &spec.ok_label);
        dialog.set_close_response("cancel");
        dialog.set_default_response(Some("ok"));

        let entry = gtk::Entry::builder()
            .text(plain(&spec.initial).as_ref())
            .activates_default(true)
            .build();
        let error = gtk::Label::builder().xalign(0.0).wrap(true).build();
        error.add_css_class("error");
        error.set_text(&plain(spec.error.as_deref().unwrap_or("")));
        let body = gtk::Box::new(gtk::Orientation::Vertical, 6);
        body.append(&entry);
        body.append(&error);
        dialog.set_extra_child(Some(&body));
        dialog.set_response_enabled("ok", spec.validate(&spec.initial).is_ok());

        let purpose = spec.purpose;
        let check = dialog.clone();
        let shown = error.clone();
        entry.connect_changed(move |e| {
            let verdict = spec.validate(&e.text());
            shown.set_text(&plain(verdict.as_ref().err().map_or("", |m| m.as_str())));
            check.set_response_enabled("ok", verdict.is_ok());
        });
        dialog.connect_response(None, move |_, id| {
            let text = (id == "ok").then(|| entry.text().to_string());
            reply(Reply::Answered { purpose, text });
        });
        dialog.present(Some(&self.window));
    }

    fn ask_password(&self, spec: PasswordSpec, reply: ReplyFn) {
        let dialog = adw::AlertDialog::new(Some("Password required"), Some(&plain(&spec.prompt)));
        dialog.add_response("cancel", "Cancel");
        dialog.add_response("ok", "OK");
        dialog.set_close_response("cancel");
        dialog.set_default_response(Some("ok"));
        let entry = gtk::PasswordEntry::builder()
            .show_peek_icon(true)
            .activates_default(true)
            .build();
        dialog.set_extra_child(Some(&entry));
        let purpose = spec.purpose;
        dialog.connect_response(None, move |_, id| {
            let text = (id == "ok").then(|| entry.text().to_string());
            reply(Reply::Answered { purpose, text });
        });
        dialog.present(Some(&self.window));
    }
}
