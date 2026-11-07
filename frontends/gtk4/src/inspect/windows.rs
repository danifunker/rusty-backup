//! Inspect's own windows, each up while the controller (or, for Expand Image, the view) says so:
//! Check results, CHD Info, Expand Image and Edit Partition Table.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use adw::prelude::*;
use rusty_backup::controller::inspect::{
    format_size, ids, CheckReport, InspectAction, EXPAND_IMAGE_MIB_MAX,
};
use rusty_backup::controller::partition_edit::{TableEditView, TableKind};

use super::editor::EditorParts;
use super::ActionFn;
use crate::browser::dialogs::{page, text_view, window, wrapped, Slot};
use crate::effects::EffectHost;
use crate::form::button;
use crate::widgets::set_text;

/// Which window a user closed.
#[derive(Clone, Copy)]
pub enum Dismiss {
    Check,
    Chd,
    Editor,
    Expand,
}

/// What the windows show, read while the controller is borrowed.
pub struct WindowsShown {
    pub check: Option<CheckReport>,
    pub chd: Option<String>,
    pub editor: Option<(TableEditView, Option<String>)>,
    /// Expand Image's current and new size, while its window is open.
    pub expand: Option<(u64, u64)>,
}

struct ExpandParts {
    current: gtk::Label,
    new: gtk::Label,
    start: gtk::Button,
}

pub struct Windows {
    host: EffectHost,
    send: ActionFn,
    dismiss: Rc<dyn Fn(Dismiss)>,
    check: Slot,
    chd: Slot,
    editor: Slot,
    editor_parts: RefCell<Option<EditorParts>>,
    expand: Slot,
    expand_parts: RefCell<Option<ExpandParts>>,
    /// Expand Image is the view's own window: open, and the MiB typed so far.
    pub expand_open: Cell<bool>,
    pub expand_mib: Rc<Cell<u32>>,
    /// Runs when the typed MiB changes, so the new size is read again.
    on_mib: Rc<dyn Fn()>,
}

fn toned(text: &str, class: &str) -> gtk::Label {
    let l = wrapped(text);
    l.add_css_class(class);
    l
}

impl Windows {
    pub fn new(
        host: EffectHost,
        send: ActionFn,
        dismiss: Rc<dyn Fn(Dismiss)>,
        on_mib: Rc<dyn Fn()>,
    ) -> Windows {
        Windows {
            host,
            send,
            dismiss,
            check: Slot::default(),
            chd: Slot::default(),
            editor: Slot::default(),
            editor_parts: RefCell::default(),
            expand: Slot::default(),
            expand_parts: RefCell::default(),
            expand_open: Cell::new(false),
            expand_mib: Rc::new(Cell::new(0)),
            on_mib,
        }
    }

    /// Run inside the screen's `SyncGuard`.
    pub fn show(&self, s: WindowsShown) {
        self.show_check(s.check);
        let close = Some(Dismiss::Chd);
        let text = s.chd.clone();
        self.chd.sync(&self.host, &self.dismiss, s.chd, close, || {
            let body = page();
            body.append(&text_view(text.as_deref().unwrap_or("")));
            window("CHD Info", 560, 420, &body)
        });
        self.show_editor(s.editor);
        self.show_expand(s.expand);
    }

    fn show_check(&self, report: Option<CheckReport>) {
        let key = report.as_ref().map(|r| format!("{r:?}"));
        let send = self.send.clone();
        self.check
            .sync(&self.host, &self.dismiss, key, Some(Dismiss::Check), || {
                check_window(report.as_ref(), send)
            });
    }

    fn show_editor(&self, editor: Option<(TableEditView, Option<String>)>) {
        let key = editor
            .as_ref()
            .map(|(v, _)| format!("editor:{}", v.kind.label()));
        let send = self.send.clone();
        let kind = editor.as_ref().map(|(v, _)| v.kind);
        let built = RefCell::new(None);
        self.editor.sync(
            &self.host,
            &self.dismiss,
            key,
            Some(Dismiss::Editor),
            || {
                let (body, parts) = EditorParts::build(&send, kind.unwrap_or(TableKind::Other));
                *built.borrow_mut() = Some(parts);
                let scroll = crate::volume::scroller(&body);
                window("Edit Partition Table", 880, 700, &scroll)
            },
        );
        if let Some(parts) = built.into_inner() {
            *self.editor_parts.borrow_mut() = Some(parts);
        }
        if let (Some((view, target)), Some(parts)) = (&editor, &*self.editor_parts.borrow()) {
            parts.show(view, target.as_deref());
        }
        if editor.is_none() {
            self.editor_parts.borrow_mut().take();
        }
    }

    fn show_expand(&self, sizes: Option<(u64, u64)>) {
        let key = sizes.map(|_| "expand".to_string());
        let built = RefCell::new(None);
        self.expand.sync(
            &self.host,
            &self.dismiss,
            key,
            Some(Dismiss::Expand),
            || {
                let (dialog, parts) = self.expand_window();
                *built.borrow_mut() = Some(parts);
                dialog
            },
        );
        if let Some(parts) = built.into_inner() {
            *self.expand_parts.borrow_mut() = Some(parts);
        }
        let parts = self.expand_parts.borrow();
        if let (Some((current, new)), Some(p)) = (sizes, parts.as_ref()) {
            set_text(
                &p.current,
                &format!("Current size: {}", format_size(current)),
            );
            set_text(&p.new, &format!("New size: {}", format_size(new)));
            p.start.set_sensitive(self.expand_mib.get() > 0);
        }
    }

    fn expand_window(&self) -> (adw::Dialog, ExpandParts) {
        let body = page();
        let current = wrapped("");
        let spin = gtk::SpinButton::with_range(0.0, f64::from(EXPAND_IMAGE_MIB_MAX), 1.0);
        spin.set_widget_name(ids::EXPAND_IMAGE_ADD);
        spin.set_value(f64::from(self.expand_mib.get()));
        let mib = self.expand_mib.clone();
        let on_mib = self.on_mib.clone();
        spin.connect_value_changed(move |s| {
            mib.set(s.value() as u32);
            on_mib();
        });
        let row = gtk::Box::new(gtk::Orientation::Horizontal, 8);
        row.append(&gtk::Label::new(Some("Add free space:")));
        row.append(&spin);
        row.append(&gtk::Label::new(Some("MiB")));
        let new = wrapped("");
        let note = wrapped(
            "After expansion, click Re-inspect to refresh the partition list, then use 'Add \
             Partition...' or 'Edit Partition Table...' to allocate the new space.",
        );
        note.add_css_class("dim-label");
        let start = button("Expand", ids::EXPAND_IMAGE_START);
        start.add_css_class("suggested-action");
        let (send, mib, dismiss) = (
            self.send.clone(),
            self.expand_mib.clone(),
            self.dismiss.clone(),
        );
        start.connect_clicked(move |_| {
            let add = rusty_backup::controller::inspect::expand_image_bytes(mib.get());
            if add > 0 {
                send(InspectAction::ExpandImage(add));
                dismiss(Dismiss::Expand);
            }
        });
        let cancel = gtk::Button::with_label("Cancel");
        let dismiss = self.dismiss.clone();
        cancel.connect_clicked(move |_| dismiss(Dismiss::Expand));
        body.append(&current);
        body.append(&row);
        body.append(&new);
        body.append(&note);
        body.append(&crate::volume::button_row(&[&start, &cancel]));
        let dialog = window("Expand Image", 440, -1, &body);
        (
            dialog,
            ExpandParts {
                current,
                new,
                start,
            },
        )
    }
}

fn check_window(report: Option<&CheckReport>, send: ActionFn) -> adw::Dialog {
    let body = page();
    let Some(r) = report else {
        return window("Filesystem Check Results", 520, 360, &body);
    };
    let check = &r.check;
    if check.clean {
        body.append(&toned("Filesystem is clean.", "success"));
    } else {
        body.append(&toned(
            &format!("{} error(s) found.", check.errors.len()),
            "error",
        ));
    }
    body.append(&wrapped(&check.stats_line));
    let mut text = Vec::new();
    if !check.errors.is_empty() {
        text.push("Errors:".to_string());
        text.extend(check.errors.iter().cloned());
    }
    for (parent, entries) in &check.orphans {
        text.push(format!("Missing parent ID {parent}:"));
        text.extend(entries.iter().map(|e| format!("    {e}")));
    }
    let shown: Vec<&String> = check
        .warnings
        .iter()
        .filter(|(_, debug)| !debug || r.show_debug)
        .map(|(line, _)| line)
        .collect();
    if !shown.is_empty() {
        text.push("Warnings:".to_string());
        text.extend(shown.into_iter().cloned());
    }
    if let Some(repair) = &r.repair {
        text.push("Repair Report:".to_string());
        text.extend(repair.applied.iter().map(|f| format!("  {f}")));
        text.extend(repair.failed.iter().map(|f| format!("  {f}")));
    }
    if !text.is_empty() {
        body.append(&text_view(&text.join("\n")));
    }
    if check.warnings.iter().any(|(_, d)| *d) {
        let toggle = gtk::CheckButton::with_label("Show debug messages");
        toggle.set_widget_name(ids::CHECK_DEBUG);
        toggle.set_active(r.show_debug);
        let s = send.clone();
        toggle.connect_toggled(move |b| s(InspectAction::ShowCheckDebug(b.is_active())));
        body.append(&toggle);
    }
    if check.repairable {
        let repair = button("Repair", ids::CHECK_REPAIR);
        repair.set_halign(gtk::Align::Start);
        repair.connect_clicked(move |_| send(InspectAction::Repair));
        body.append(&repair);
    }
    window("Filesystem Check Results", 560, 440, &body)
}
