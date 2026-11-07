//! File Info: the entry's facts, prompts for its fields, and a preview, in a window or a quick view.
//!
//! The field prompts are the controller's own (`EditDetail`), so a value is validated and staged
//! as every other view does it. The graphical metadata editors are slice 7.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use adw::prelude::*;
use rusty_backup::controller::commander::{
    entry_facts, CommanderAction as A, CommanderController, DetailField, DetailMetadata, FileDetail,
};

use super::detail::{preview_of, Preview};
use super::SendFn;
use crate::widgets::{flow, plain, set_text};

/// What identifies a body already on screen: the entry and whether its extras have arrived.
pub type InfoKey = (u64, bool);

/// What File Info shows; `body` is left out while the entry on screen has not changed.
pub struct InfoShown {
    key: InfoKey,
    body: Option<Body>,
    facts: Vec<String>,
    fields: Vec<DetailField>,
    result: Option<String>,
}

struct Body {
    name: String,
    xattrs: Vec<String>,
    budget: Vec<String>,
    preview: Preview,
}

pub struct FileInfo {
    root: gtk::Box,
    name: gtk::Label,
    facts: gtk::Label,
    fields: gtk::FlowBox,
    xattrs: gtk::Label,
    budget: gtk::Label,
    reading: gtk::Label,
    result: gtk::Label,
    preview: gtk::TextView,
    send: SendFn,
    key: Cell<Option<InfoKey>>,
    shown_fields: RefCell<Vec<DetailField>>,
}

fn left_label() -> gtk::Label {
    gtk::Label::builder()
        .xalign(0.0)
        .wrap(true)
        .wrap_mode(gtk::pango::WrapMode::WordChar)
        .selectable(true)
        .build()
}

/// The staged-over values of a Mac or ProDOS entry, a line each.
fn metadata_lines(meta: &DetailMetadata) -> Vec<String> {
    let mut lines = Vec::new();
    if let Some(tc) = &meta.type_creator {
        lines.push(format!("Type: {}", tc.type_text));
        lines.push(format!("Creator: {}", tc.creator_text));
    }
    if let Some(p) = &meta.prodos {
        lines.push(format!(
            "ProDOS type: ${:02X} aux ${:04X}",
            p.type_byte, p.aux_type
        ));
    }
    if let Some(dates) = &meta.dates {
        for (label, date) in ["Created", "Modified", "Backed up"].iter().zip(dates) {
            let shown = date.as_deref().unwrap_or("none");
            lines.push(format!("{label}: {shown}"));
        }
    }
    lines
}

impl FileInfo {
    pub fn build(send: &SendFn) -> Rc<FileInfo> {
        let name = left_label();
        name.add_css_class("heading");
        let facts = left_label();
        let fields = flow();
        let xattrs = left_label();
        let budget = left_label();
        budget.add_css_class("monospace");
        let reading = gtk::Label::builder()
            .label("Reading...")
            .xalign(0.0)
            .build();
        reading.add_css_class("dim-label");
        let result = left_label();
        result.add_css_class("success");
        let preview = gtk::TextView::builder()
            .editable(false)
            .monospace(true)
            .cursor_visible(false)
            .wrap_mode(gtk::WrapMode::None)
            .build();
        let preview_scroll = gtk::ScrolledWindow::builder()
            .child(&preview)
            .vexpand(true)
            .min_content_height(160)
            .build();
        let root = gtk::Box::new(gtk::Orientation::Vertical, 4);
        root.set_margin_start(8);
        root.set_margin_end(8);
        root.set_margin_top(6);
        root.set_margin_bottom(8);
        root.append(&name);
        root.append(&facts);
        root.append(&fields);
        root.append(&xattrs);
        root.append(&budget);
        root.append(&reading);
        root.append(&result);
        root.append(&preview_scroll);
        Rc::new(FileInfo {
            root,
            name,
            facts,
            fields,
            xattrs,
            budget,
            reading,
            result,
            preview,
            send: send.clone(),
            key: Cell::new(None),
            shown_fields: RefCell::default(),
        })
    }

    pub fn root(&self) -> &gtk::Box {
        &self.root
    }

    pub fn key(&self) -> Option<InfoKey> {
        self.key.get()
    }

    /// Read `d`; `known` is the key on screen, so an unchanged body costs no hex dump.
    pub fn read(ctl: &CommanderController, d: &FileDetail, known: Option<InfoKey>) -> InfoShown {
        let key = (d.serial, d.loading);
        let is_open = ctl.detail().is_some_and(|o| o.serial == d.serial);
        let meta = ctl.detail_metadata().filter(|_| is_open);
        let lines = meta.as_ref().map(metadata_lines).unwrap_or_default();
        let mut facts = entry_facts(&d.entry, !lines.is_empty(), Some(&d.id_names));
        facts.extend(lines);
        let body = (known != Some(key)).then(|| Body {
            name: d.entry.name.clone(),
            xattrs: d
                .xattrs
                .iter()
                .map(|x| format!("{} ({} bytes)", x.name, x.value.len()))
                .collect(),
            budget: d.size_budget_lines(),
            preview: preview_of(d),
        });
        InfoShown {
            key,
            body,
            facts,
            fields: match is_open {
                true => ctl.detail_fields(),
                false => Vec::new(),
            },
            result: d.result.clone(),
        }
    }

    pub fn show(&self, s: InfoShown) {
        self.key.set(Some(s.key));
        if let Some(b) = s.body {
            set_text(&self.name, &b.name);
            set_text(&self.xattrs, &b.xattrs.join("\n"));
            self.xattrs.set_visible(!b.xattrs.is_empty());
            set_text(&self.budget, &b.budget.join("\n"));
            self.budget.set_visible(!b.budget.is_empty());
            self.preview.buffer().set_text(&plain(b.preview.text()));
        }
        set_text(&self.facts, &s.facts.join("\n"));
        self.reading.set_visible(s.key.1);
        set_text(&self.result, s.result.as_deref().unwrap_or(""));
        self.result.set_visible(s.result.is_some());
        if *self.shown_fields.borrow() != s.fields {
            self.show_fields(&s.fields);
            *self.shown_fields.borrow_mut() = s.fields;
        }
    }

    /// Nothing under the cursor: an empty panel.
    pub fn clear(&self) {
        self.key.set(None);
        set_text(&self.name, "");
        set_text(&self.facts, "No file or folder under the cursor.");
        self.xattrs.set_visible(false);
        self.budget.set_visible(false);
        self.reading.set_visible(false);
        self.result.set_visible(false);
        self.preview.buffer().set_text("");
    }

    fn show_fields(&self, fields: &[DetailField]) {
        self.fields.remove_all();
        for field in fields {
            let button = gtk::Button::with_label(&format!("Edit {}...", field.label()));
            let (send, field) = (self.send.clone(), *field);
            button.connect_clicked(move |_| send(A::EditDetail(field)));
            self.fields.append(&button);
        }
        self.fields.set_visible(!fields.is_empty());
    }
}
