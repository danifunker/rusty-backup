//! Convert Floppy Container over `controller::floppy_convert`; `src/gui/floppy_convert_dialog.rs`
//! is the reference.

use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use adw::prelude::*;
use rusty_backup::controller::app::Notify;
use rusty_backup::controller::floppy_convert::{
    ids, path_label, FloppyConvertAction, FloppyConvertController, FloppyTarget, BULK_NOTE,
    SAME_FORMAT_NOTE,
};
use rusty_backup::controller::{Controller, Effect};

use super::{button_row, fact_line, set_radio, window};
use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::form::{button, set_line};
use crate::sync::SyncGuard;
use crate::widgets::set_text;

struct Shown {
    open: bool,
    source: String,
    probing: bool,
    kind: Option<String>,
    error: Option<String>,
    editable: bool,
    target: FloppyTarget,
    output: String,
    same: bool,
    phase: Option<String>,
    outcome: Option<Result<(String, String), String>>,
    running: bool,
    can_start: bool,
    close_label: &'static str,
}

pub struct FloppyDialog {
    ctrl: RefCell<FloppyConvertController>,
    host: EffectHost,
    sync: SyncGuard,
    me: Weak<FloppyDialog>,
    dialog: adw::Dialog,
    source: gtk::Label,
    choose: gtk::Button,
    probing: gtk::Spinner,
    kind: gtk::Label,
    error: gtk::Label,
    form: gtk::Box,
    targets: Vec<gtk::CheckButton>,
    output: gtk::Label,
    same: gtk::Label,
    spinner: gtk::Spinner,
    phase: gtk::Label,
    outcome: gtk::Label,
    detail: gtk::Label,
    convert: gtk::Button,
    close: gtk::Button,
}

fn row(title: &str, widget: &impl IsA<gtk::Widget>) -> gtk::Box {
    let b = gtk::Box::new(gtk::Orientation::Horizontal, 8);
    b.append(&gtk::Label::new(Some(title)));
    b.append(widget);
    b
}

fn heading(text: &str) -> gtk::Label {
    let l = gtk::Label::builder().label(text).xalign(0.0).build();
    l.add_css_class("heading");
    l
}

fn path_text() -> gtk::Label {
    gtk::Label::builder()
        .xalign(0.0)
        .hexpand(true)
        .ellipsize(gtk::pango::EllipsizeMode::Middle)
        .build()
}

impl FloppyDialog {
    /// Open with `source` to sniff, or empty.
    pub fn open(notify: Arc<dyn Notify>, host: EffectHost, source: Option<PathBuf>) -> Rc<Self> {
        let d = Rc::new_cyclic(|me: &Weak<FloppyDialog>| {
            let send: Rc<dyn Fn(FloppyConvertAction)> = {
                let me = me.clone();
                Rc::new(move |action| {
                    if let Some(d) = me.upgrade().filter(|d| !d.sync.active()) {
                        d.apply(action);
                    }
                })
            };
            let source_label = path_text();
            let choose = button("Choose File...", ids::CHOOSE_SOURCE);
            let probing = gtk::Spinner::new();
            let source_row = row("", &source_label);
            source_row.append(&choose);
            source_row.append(&probing);
            let kind = fact_line();
            let error = fact_line();
            error.add_css_class("error");
            let bulk = fact_line();
            bulk.set_label(BULK_NOTE);
            bulk.add_css_class("dim-label");
            bulk.add_css_class("caption");

            // Two per line, as egui lays them out: four in a row is wider than the window.
            let target_row = gtk::Grid::builder()
                .column_spacing(12)
                .row_spacing(4)
                .build();
            target_row.set_widget_name(ids::TARGET);
            let mut targets: Vec<gtk::CheckButton> = Vec::new();
            for (i, target) in FloppyTarget::ALL.iter().enumerate() {
                let radio = gtk::CheckButton::with_label(target.label());
                if let Some(first) = targets.first() {
                    radio.set_group(Some(first));
                }
                target_row.attach(&radio, (i % 2) as i32, (i / 2) as i32, 1, 1);
                targets.push(radio);
            }
            for (radio, target) in targets.iter().zip(FloppyTarget::ALL) {
                let s = send.clone();
                radio.connect_toggled(move |r| {
                    if r.is_active() {
                        s(FloppyConvertAction::SetTarget(target));
                    }
                });
            }
            let output = path_text();
            let save = button("Save As...", ids::SAVE_AS);
            let out_row = row("Output file:", &output);
            out_row.append(&save);
            let same = fact_line();
            same.add_css_class("dim-label");
            same.add_css_class("caption");
            let form = gtk::Box::new(gtk::Orientation::Vertical, 6);
            let format_row = row("Output format:", &target_row);
            format_row.set_valign(gtk::Align::Start);
            form.append(&format_row);
            form.append(&out_row);
            form.append(&same);

            let spinner = gtk::Spinner::new();
            let phase = gtk::Label::builder().xalign(0.0).build();
            let status = gtk::Box::new(gtk::Orientation::Horizontal, 6);
            status.append(&spinner);
            status.append(&phase);
            let outcome = fact_line();
            let detail = fact_line();
            let convert = button("Convert", ids::CONVERT);
            convert.add_css_class("suggested-action");
            let close = button("Cancel", ids::CLOSE);

            let body = crate::browser::dialogs::page();
            body.set_spacing(8);
            body.append(&heading("Source"));
            body.append(&source_row);
            body.append(&kind);
            body.append(&error);
            body.append(&bulk);
            body.append(&gtk::Separator::new(gtk::Orientation::Horizontal));
            body.append(&heading("Target"));
            body.append(&form);
            body.append(&gtk::Separator::new(gtk::Orientation::Horizontal));
            body.append(&status);
            body.append(&outcome);
            body.append(&detail);
            body.append(&button_row(&[&close, &convert]));
            let s = send.clone();
            let dialog = window(
                "Convert Floppy Container",
                560,
                440,
                &body,
                Rc::new(move || s(FloppyConvertAction::Close)),
            );

            let s = send.clone();
            choose.connect_clicked(move |_| s(FloppyConvertAction::PickSource));
            let s = send.clone();
            save.connect_clicked(move |_| s(FloppyConvertAction::PickOutput));
            let s = send.clone();
            close.connect_clicked(move |_| s(FloppyConvertAction::Close));
            let me2 = me.clone();
            convert.connect_clicked(move |_| {
                let Some(d) = me2.upgrade().filter(|d| !d.sync.active()) else {
                    return;
                };
                // Asked again at the click: the button only knows what the last refresh knew.
                if d.ctrl.borrow().can_start() {
                    d.apply(FloppyConvertAction::Start);
                }
            });

            FloppyDialog {
                ctrl: RefCell::new(FloppyConvertController::new(notify)),
                host,
                sync: SyncGuard::default(),
                me: me.clone(),
                dialog,
                source: source_label,
                choose,
                probing,
                kind,
                error,
                form,
                targets,
                output,
                same,
                spinner,
                phase,
                outcome,
                detail,
                convert,
                close,
            }
        });
        d.apply(FloppyConvertAction::Open(source));
        d.host.present(&d.dialog);
        d
    }

    pub fn apply(&self, action: FloppyConvertAction) {
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

    pub fn is_running(&self) -> bool {
        self.ctrl.borrow().is_running()
    }

    fn reply(&self) -> ReplyFn {
        let me = self.me.clone();
        Rc::new(move |reply| {
            let Some(d) = me.upgrade() else {
                return;
            };
            let action = match reply {
                Reply::Picked { purpose, path } => FloppyConvertAction::Picked { purpose, path },
                // Floppy Convert asks only for its source and output files.
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
            source: path_label(ctrl.source()),
            probing: ctrl.probing(),
            kind: ctrl.source_kind().map(|k| format!("Detected: {k}")),
            error: ctrl.source_error().map(str::to_string),
            editable: ctrl.editable(),
            target: ctrl.target(),
            output: path_label(ctrl.output()),
            same: ctrl.same_format(),
            phase: ctrl.progress().map(|s| s.phase),
            outcome: ctrl.outcome().map(|o| match o {
                Ok(done) => Ok((done.summary.clone(), done.line.clone())),
                Err(e) => Err(format!("Failed: {e}")),
            }),
            running: ctrl.is_running(),
            can_start: ctrl.can_start(),
            close_label: ctrl.close_label(),
        }
    }

    fn show(&self, s: Shown) {
        set_text(&self.source, &s.source);
        self.choose.set_sensitive(!s.running);
        self.probing.set_visible(s.probing);
        self.probing.set_spinning(s.probing);
        set_line(&self.kind, s.kind.as_deref());
        set_line(&self.error, s.error.as_deref());
        self.form.set_sensitive(s.editable);
        let at = FloppyTarget::ALL
            .iter()
            .position(|t| *t == s.target)
            .unwrap_or(0);
        set_radio(&self.targets, at);
        set_text(&self.output, &s.output);
        set_line(&self.same, s.same.then_some(SAME_FORMAT_NOTE));
        self.spinner.set_visible(s.phase.is_some());
        self.spinner.set_spinning(s.phase.is_some());
        set_line(&self.phase, s.phase.as_deref());
        let (outcome, detail, ok) = match (&s.phase, &s.outcome) {
            (None, Some(Ok((summary, line)))) => {
                (Some(summary.as_str()), Some(line.as_str()), true)
            }
            (None, Some(Err(e))) => (Some(e.as_str()), None, false),
            _ => (None, None, true),
        };
        self.outcome
            .remove_css_class(if ok { "error" } else { "success" });
        self.outcome
            .add_css_class(if ok { "success" } else { "error" });
        set_line(&self.outcome, outcome);
        set_line(&self.detail, detail);
        self.convert.set_sensitive(s.can_start);
        self.close.set_sensitive(!s.running);
        if self.close.label().as_deref() != Some(s.close_label) {
            self.close.set_label(s.close_label);
        }
    }
}
