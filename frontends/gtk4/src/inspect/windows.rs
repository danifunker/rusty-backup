//! Inspect's own windows, each up while the controller says so: Check results, CHD Info, Edit
//! Partition Table and Export Disk Image.

use std::cell::RefCell;
use std::rc::Rc;

use adw::prelude::*;
use rusty_backup::controller::inspect::{ids, CheckReport, InspectAction};
use rusty_backup::controller::partition_edit::{TableEditView, TableKind};

use super::editor::EditorParts;
use super::export::{ExportParts, ExportShown};
use super::ActionFn;
use crate::browser::dialogs::{page, text_view, window, wrapped, Slot};
use crate::effects::EffectHost;
use crate::form::button;

/// Which window a user closed.
#[derive(Clone, Copy)]
pub enum Dismiss {
    Check,
    Chd,
    Editor,
    Export,
}

/// What the windows show, read while the controller is borrowed.
pub struct WindowsShown {
    pub check: Option<CheckReport>,
    pub chd: Option<String>,
    pub editor: Option<(TableEditView, Option<String>)>,
    pub export: Option<ExportShown>,
}

pub struct Windows {
    host: EffectHost,
    send: ActionFn,
    dismiss: Rc<dyn Fn(Dismiss)>,
    check: Slot,
    chd: Slot,
    editor: Slot,
    editor_parts: RefCell<Option<EditorParts>>,
    export: Slot,
    export_parts: RefCell<Option<ExportParts>>,
    /// Hands the image to Physical Disk Export.
    physical: Rc<dyn Fn()>,
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
        physical: Rc<dyn Fn()>,
    ) -> Windows {
        Windows {
            host,
            send,
            dismiss,
            check: Slot::default(),
            chd: Slot::default(),
            editor: Slot::default(),
            editor_parts: RefCell::default(),
            export: Slot::default(),
            export_parts: RefCell::default(),
            physical,
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
        self.show_export(s.export);
    }

    fn show_export(&self, export: Option<ExportShown>) {
        let key = export.as_ref().map(|_| "export".to_string());
        let built = RefCell::new(None);
        let (send, physical) = (self.send.clone(), self.physical.clone());
        self.export.sync(
            &self.host,
            &self.dismiss,
            key,
            Some(Dismiss::Export),
            || {
                let (dialog, parts) = ExportParts::build(&send, physical);
                *built.borrow_mut() = Some(parts);
                dialog
            },
        );
        if let Some(parts) = built.into_inner() {
            *self.export_parts.borrow_mut() = Some(parts);
        }
        match export {
            Some(shown) => {
                if let Some(parts) = &*self.export_parts.borrow() {
                    parts.show(shown);
                }
            }
            None => {
                self.export_parts.borrow_mut().take();
            }
        }
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
