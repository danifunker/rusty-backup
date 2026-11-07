//! Resize Alto Disk over `controller::alto_resize`; `src/gui/alto_resize_dialog.rs` is the reference.

use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use adw::prelude::*;
use rusty_backup::controller::alto_resize::{
    ids, AltoResizeAction, AltoResizeController, AltoTarget,
};
use rusty_backup::controller::app::{Notify, Snapshot};
use rusty_backup::controller::{Controller, Effect};

use super::{button_row, fact_line, set_radio, window};
use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::form::{button, set_line};
use crate::progress::JobList;
use crate::sync::SyncGuard;
use crate::widgets::set_text;

struct Shown {
    open: bool,
    source: String,
    opening: Option<Snapshot>,
    summary: String,
    target: AltoTarget,
    warning: Option<String>,
    result: Option<Result<String, String>>,
    writing: bool,
    can_save: bool,
}

pub struct AltoDialog {
    ctrl: RefCell<AltoResizeController>,
    host: EffectHost,
    sync: SyncGuard,
    me: Weak<AltoDialog>,
    dialog: adw::Dialog,
    source: gtk::Label,
    opening: JobList,
    form: gtk::Box,
    summary: gtk::Label,
    targets: Vec<gtk::CheckButton>,
    warning: gtk::Label,
    result: gtk::Label,
    writing: gtk::Box,
    save: gtk::Button,
    close: gtk::Button,
}

impl AltoDialog {
    pub fn open(notify: Arc<dyn Notify>, host: EffectHost, path: PathBuf) -> Rc<Self> {
        let d = Rc::new_cyclic(|me: &Weak<AltoDialog>| {
            let send: Rc<dyn Fn(AltoResizeAction)> = {
                let me = me.clone();
                Rc::new(move |action| {
                    if let Some(d) = me.upgrade() {
                        if !d.sync.active() {
                            d.apply(action);
                        }
                    }
                })
            };
            let source = fact_line();
            let opening = JobList::new(ids::CLOSE, Rc::new(|| {}));
            let summary = fact_line();
            let geometry = gtk::Label::builder()
                .label("Target geometry:")
                .xalign(0.0)
                .build();
            let list = gtk::Box::new(gtk::Orientation::Vertical, 4);
            list.set_widget_name(ids::TARGET);
            let mut targets: Vec<gtk::CheckButton> = Vec::new();
            for t in AltoTarget::ALL {
                let radio = gtk::CheckButton::with_label(t.label());
                if let Some(first) = targets.first() {
                    radio.set_group(Some(first));
                }
                let s = send.clone();
                radio.connect_toggled(move |r| {
                    if r.is_active() {
                        s(AltoResizeAction::SetTarget(t));
                    }
                });
                list.append(&radio);
                targets.push(radio);
            }
            let warning = fact_line();
            warning.add_css_class("error");
            let form = gtk::Box::new(gtk::Orientation::Vertical, 6);
            form.append(&summary);
            form.append(&gtk::Separator::new(gtk::Orientation::Horizontal));
            form.append(&geometry);
            form.append(&list);
            form.append(&warning);
            let result = fact_line();
            let spinner = gtk::Spinner::builder().spinning(true).build();
            let writing = gtk::Box::new(gtk::Orientation::Horizontal, 6);
            writing.append(&spinner);
            writing.append(&gtk::Label::new(Some("Resizing...")));

            let save = button("Resize and Save As...", ids::SAVE_AS);
            save.add_css_class("suggested-action");
            let s = send.clone();
            save.connect_clicked(move |_| s(AltoResizeAction::SaveAs));
            let close = button("Close", ids::CLOSE);
            let s = send.clone();
            close.connect_clicked(move |_| s(AltoResizeAction::Close));

            let body = crate::browser::dialogs::page();
            body.append(&source);
            body.append(opening.root());
            body.append(&form);
            body.append(&result);
            body.append(&writing);
            body.append(&button_row(&[&save, &close]));
            let s = send.clone();
            let dialog = window(
                "Resize Alto Disk",
                520,
                380,
                &body,
                Rc::new(move || s(AltoResizeAction::Close)),
            );
            AltoDialog {
                ctrl: RefCell::new(AltoResizeController::new(notify)),
                host,
                sync: SyncGuard::default(),
                me: me.clone(),
                dialog,
                source,
                opening,
                form,
                summary,
                targets,
                warning,
                result,
                writing,
                save,
                close,
            }
        });
        d.apply(AltoResizeAction::Open(path));
        d.host.present(&d.dialog);
        d
    }

    pub fn apply(&self, action: AltoResizeAction) {
        let effects = self.ctrl.borrow_mut().apply(action);
        self.after(effects);
    }

    pub fn tick(&self) {
        let effects = self.ctrl.borrow_mut().tick();
        self.after(effects);
    }

    pub fn is_open(&self) -> bool {
        self.ctrl.borrow().is_open()
    }

    fn reply(&self) -> ReplyFn {
        let me = self.me.clone();
        Rc::new(move |reply| {
            let Some(d) = me.upgrade() else {
                return;
            };
            let action = match reply {
                Reply::Picked { purpose, path } => AltoResizeAction::Picked { purpose, path },
                // The Alto resize asks only for its output file.
                Reply::Confirmed { .. }
                | Reply::Answered { .. }
                | Reply::Chose { .. }
                | Reply::CloseScreen => return,
            };
            d.apply(action);
        })
    }

    fn after(&self, effects: Vec<Effect>) {
        self.host.run(effects, &self.reply());
        let log = self.ctrl.borrow_mut().take_log();
        crate::app::append_log(log);
        let shown = self.read();
        if !shown.open {
            self.dialog.force_close();
            return;
        }
        self.sync.run(|| self.show(shown));
    }

    fn read(&self) -> Shown {
        let ctrl = self.ctrl.borrow();
        Shown {
            open: ctrl.is_open(),
            source: ctrl
                .source()
                .map(|p| format!("Source: {}", p.display()))
                .unwrap_or_default(),
            opening: ctrl.opening(),
            summary: ctrl.summary_line().unwrap_or("").to_string(),
            target: ctrl.target(),
            warning: ctrl.fit_warning(),
            result: ctrl.result().cloned(),
            writing: ctrl.is_writing(),
            can_save: ctrl.can_save(),
        }
    }

    fn show(&self, s: Shown) {
        set_text(&self.source, &s.source);
        let opening: Vec<(Snapshot, bool)> = s.opening.iter().map(|j| (j.clone(), false)).collect();
        self.opening.show(&opening);
        self.form.set_visible(s.opening.is_none());
        set_text(&self.summary, &s.summary);
        let at = AltoTarget::ALL
            .iter()
            .position(|t| *t == s.target)
            .unwrap_or(0);
        set_radio(&self.targets, at);
        for radio in &self.targets {
            radio.set_sensitive(!s.writing);
        }
        set_line(&self.warning, s.warning.as_deref());
        self.result.remove_css_class("error");
        self.result.remove_css_class("success");
        let result = match &s.result {
            Some(Ok(msg)) => {
                self.result.add_css_class("success");
                Some(msg.clone())
            }
            Some(Err(e)) => {
                self.result.add_css_class("error");
                Some(format!("Error: {e}"))
            }
            None => None,
        };
        set_line(&self.result, result.as_deref());
        self.writing.set_visible(s.writing);
        self.save.set_sensitive(s.can_save);
        self.close.set_sensitive(!s.writing);
    }
}
