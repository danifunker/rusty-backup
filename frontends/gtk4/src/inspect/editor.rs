//! Edit Partition Table over Inspect's `TableEditView`; `src/gui/partition_editor_modal.rs` is
//! the reference. Apply Changes only sends `ApplyTableEdits`: the write begins on the
//! controller's own question, and the name above the button is the one that question uses.

use std::cell::RefCell;
use std::rc::Rc;

use adw::prelude::*;
use rusty_backup::controller::inspect::InspectAction;
use rusty_backup::controller::partition_edit::{
    ids, type_choices, type_note, BootStyle, SizeMode, TableEditAction, TableEditView, TableKind,
};

use super::ActionFn;
use crate::form::{button, cell, clear_grid, grid, head, set_line};
use crate::volume::CaptionedBar;
use crate::widgets::{plain, set_text};

/// The cells a refresh rebuilds: a change here throws the grid away.
#[derive(PartialEq)]
struct Shape {
    kind: TableKind,
    boot: BootStyle,
    rows: Vec<(bool, bool, bool, Vec<SizeMode>)>,
}

impl Shape {
    fn of(v: &TableEditView) -> Shape {
        let rows = v
            .rows
            .iter()
            .map(|r| (r.deleted, r.extended, r.logical, r.mode_choices()));
        Shape {
            kind: v.kind,
            boot: v.boot_style,
            rows: rows.collect(),
        }
    }
}

/// One editable row's live widgets; a deleted or extended row has none.
struct RowCells {
    kind: Option<(gtk::Entry, gtk::Label)>,
    mode: Option<(gtk::DropDown, Vec<SizeMode>)>,
    size: Option<gtk::Entry>,
    boot: Vec<gtk::CheckButton>,
    boot_label: Option<gtk::Label>,
}

/// The window's live parts.
pub struct EditorParts {
    kind_line: gtk::Label,
    current: CaptionedBar,
    after: CaptionedBar,
    rows: gtk::Grid,
    shape: RefCell<Option<Shape>>,
    cells: RefCell<Vec<RowCells>>,
    gaps: gtk::Box,
    gaps_shown: RefCell<Option<Vec<(u64, u64, String)>>>,
    add_start: gtk::Entry,
    add_size: gtk::Entry,
    add_type: gtk::Entry,
    add_note: gtk::Label,
    add_boot: gtk::CheckButton,
    next_slot: gtk::Label,
    errors: gtk::Label,
    status: gtk::Label,
    target: gtk::Label,
    apply: gtk::Button,
    send: ActionFn,
}

fn set_entry(entry: &gtk::Entry, text: &str) {
    let text = plain(text);
    if entry.text() != text.as_ref() {
        entry.set_text(&text);
    }
}

fn table(edit: TableEditAction) -> InspectAction {
    InspectAction::Table(edit)
}

/// "Pick...": a type from the catalog, stamped into its field.
fn pick_menu(kind: TableKind, on_pick: Rc<dyn Fn(String)>) -> gtk::MenuButton {
    let list = gtk::ListBox::builder()
        .selection_mode(gtk::SelectionMode::None)
        .activate_on_single_click(true)
        .build();
    let picks = type_choices(kind);
    for c in picks {
        let text = format!("{}  -  {}", c.value, c.label);
        list.append(&gtk::Label::builder().label(text).xalign(0.0).build());
    }
    let scroll = gtk::ScrolledWindow::builder()
        .child(&list)
        .hscrollbar_policy(gtk::PolicyType::Never)
        .propagate_natural_width(true)
        .propagate_natural_height(true)
        .max_content_height(360)
        .build();
    let popover = gtk::Popover::builder().child(&scroll).build();
    let menu = gtk::MenuButton::builder()
        .label("Pick...")
        .popover(&popover)
        .build();
    menu.set_tooltip_text(Some("Choose a well-known partition type"));
    list.connect_row_activated(move |_, line| {
        popover.popdown();
        if let Some(c) = picks.get(line.index() as usize) {
            on_pick(c.value.to_string());
        }
    });
    menu
}

/// A type field: free text, the catalog's Pick menu, and the resolved name.
fn type_field(kind: TableKind, send: Rc<dyn Fn(String)>) -> (gtk::Box, gtk::Entry, gtk::Label) {
    let gpt = kind == TableKind::Gpt;
    let entry = gtk::Entry::builder()
        .width_chars(if gpt { 36 } else { 8 })
        .build();
    entry.set_tooltip_text(Some(kind.field_hint()));
    let s = send.clone();
    entry.connect_changed(move |e| s(e.text().to_string()));
    let note = gtk::Label::builder()
        .xalign(0.0)
        .ellipsize(gtk::pango::EllipsizeMode::End)
        .max_width_chars(22)
        .build();
    note.add_css_class("dim-label");
    let b = gtk::Box::new(gtk::Orientation::Horizontal, 6);
    b.append(&entry);
    if !type_choices(kind).is_empty() {
        b.append(&pick_menu(kind, send));
    }
    b.append(&note);
    (b, entry, note)
}

fn show_note(label: &gtk::Label, kind: TableKind, text: &str) {
    set_text(label, type_note(kind, text).unwrap_or(""));
}

impl EditorParts {
    /// Build the window; its widgets send through `send`, which a sync mutes.
    pub fn build(send: &ActionFn, kind: TableKind) -> (gtk::Box, EditorParts) {
        let kind_line = gtk::Label::builder().xalign(0.0).build();
        let current = CaptionedBar::new(false);
        let after = CaptionedBar::new(true);
        let rows = grid();

        let add_head = gtk::Label::builder()
            .label("Add Partition")
            .xalign(0.0)
            .build();
        add_head.add_css_class("heading");
        let gaps = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        let add_start = gtk::Entry::builder().width_chars(10).build();
        add_start.set_widget_name(ids::TABLE_ADD_START);
        add_start.set_tooltip_text(Some(
            "First 512-byte sector of the new partition. It does not have to be past the \
             existing partitions -- any free gap works.",
        ));
        let s = send.clone();
        add_start.connect_changed(move |e| s(table(TableEditAction::SetAddStart(e.text().into()))));
        let add_size = gtk::Entry::builder().width_chars(8).build();
        add_size.set_widget_name(ids::TABLE_ADD_SIZE);
        let s = send.clone();
        add_size.connect_changed(move |e| s(table(TableEditAction::SetAddSize(e.text().into()))));
        let add_boot = gtk::CheckButton::with_label("Bootable");
        add_boot.set_widget_name(ids::TABLE_ADD_BOOT);
        let s = send.clone();
        add_boot.connect_toggled(move |b| s(table(TableEditAction::SetAddBootable(b.is_active()))));
        let add = button("Add", ids::TABLE_ADD);
        let s = send.clone();
        add.connect_clicked(move |_| s(table(TableEditAction::Add)));
        let add_row = gtk::FlowBox::builder()
            .selection_mode(gtk::SelectionMode::None)
            .max_children_per_line(8)
            .column_spacing(6)
            .row_spacing(4)
            .build();
        let s = send.clone();
        let (type_box, add_type, add_note) = type_field(
            kind,
            Rc::new(move |t| s(table(TableEditAction::SetAddType(t)))),
        );
        add_type.set_widget_name(ids::TABLE_ADD_TYPE);
        let labelled = |title: &str, w: &gtk::Widget| {
            let b = gtk::Box::new(gtk::Orientation::Horizontal, 6);
            b.append(&gtk::Label::new(Some(title)));
            b.append(w);
            b
        };
        add_row.append(&labelled("Start LBA:", add_start.upcast_ref()));
        add_row.append(&labelled("Size (MiB):", add_size.upcast_ref()));
        add_row.append(&labelled("Type:", type_box.upcast_ref()));
        add_row.append(&add_boot);
        add_row.append(&add);
        let next_slot = gtk::Label::builder().xalign(0.0).wrap(true).build();
        next_slot.add_css_class("dim-label");

        let errors = crate::form::line();
        errors.add_css_class("error");
        let status = crate::form::line();
        status.add_css_class("success");
        let target = crate::form::line();
        target.add_css_class("heading");
        let validate = button("Validate", ids::TABLE_VALIDATE);
        let s = send.clone();
        validate.connect_clicked(move |_| s(table(TableEditAction::Validate)));
        let apply = button("Apply Changes", ids::TABLE_APPLY);
        apply.add_css_class("destructive-action");
        let s = send.clone();
        apply.connect_clicked(move |_| s(InspectAction::ApplyTableEdits));
        let cancel = button("Cancel", ids::CANCEL);
        let s = send.clone();
        cancel.connect_clicked(move |_| s(InspectAction::CloseEditor));

        let body = crate::browser::dialogs::page();
        body.append(&kind_line);
        body.append(current.root());
        body.append(after.root());
        body.append(&rows);
        body.append(&add_head);
        body.append(&gaps);
        body.append(&add_row);
        body.append(&next_slot);
        body.append(&errors);
        body.append(&status);
        body.append(&target);
        body.append(&crate::volume::button_row(&[&validate, &apply, &cancel]));
        let parts = EditorParts {
            kind_line,
            current,
            after,
            rows,
            shape: RefCell::default(),
            cells: RefCell::default(),
            gaps,
            gaps_shown: RefCell::default(),
            add_start,
            add_size,
            add_type,
            add_note,
            add_boot,
            next_slot,
            errors,
            status,
            target,
            apply,
            send: send.clone(),
        };
        (body, parts)
    }

    /// Run inside the screen's `SyncGuard`.
    pub fn show(&self, v: &TableEditView, target: Option<&str>) {
        set_text(&self.kind_line, &format!("Table type: {}", v.kind.label()));
        self.current.show(&v.current, 1.0);
        self.after.show(&v.after, 1.0);
        let shape = Shape::of(v);
        if self.shape.borrow().as_ref() != Some(&shape) {
            self.build_rows(v);
            *self.shape.borrow_mut() = Some(shape);
        }
        for (row, c) in v.rows.iter().zip(self.cells.borrow().iter()) {
            if let Some((entry, note)) = &c.kind {
                set_entry(entry, &row.type_text);
                show_note(note, v.kind, &row.type_text);
            }
            if let Some((mode, choices)) = &c.mode {
                let at = choices
                    .iter()
                    .position(|m| *m == row.size_mode)
                    .unwrap_or(0) as u32;
                if mode.selected() != at {
                    mode.set_selected(at);
                }
            }
            if let Some(size) = &c.size {
                set_entry(size, &row.size_text);
                size.set_sensitive(row.size_mode == SizeMode::Custom);
            }
            match c.boot.as_slice() {
                [one] => {
                    if one.is_active() != row.bootable {
                        one.set_active(row.bootable);
                    }
                }
                [yes, no] => {
                    let pick = if row.bootable { yes } else { no };
                    if !pick.is_active() {
                        pick.set_active(true);
                    }
                }
                _ => {}
            }
            if let Some(l) = &c.boot_label {
                set_text(l, if row.bootable { "Yes" } else { "" });
            }
        }
        self.show_gaps(&v.gaps);
        set_entry(&self.add_start, &v.add_start);
        set_entry(&self.add_size, &v.add_size);
        set_entry(&self.add_type, &v.add_type);
        show_note(&self.add_note, v.kind, &v.add_type);
        crate::widgets::flow_show(&self.add_boot, v.add_bootable_shown);
        if self.add_boot.is_active() != v.add_bootable {
            self.add_boot.set_active(v.add_bootable);
        }
        set_text(
            &self.next_slot,
            &format!(
                "Will be added as partition {} (slot assigned automatically; a slot freed by a \
                 Delete above is reused).",
                v.next_slot
            ),
        );
        let errors = v.errors.join("\n");
        set_line(
            &self.errors,
            (!errors.is_empty()).then_some(errors.as_str()),
        );
        set_line(&self.status, v.status.as_deref());
        let target = target.map(|t| format!("Writes to: {t}"));
        set_line(&self.target, target.as_deref());
        self.apply.set_sensitive(v.can_apply);
    }

    fn show_gaps(&self, gaps: &[(u64, u64, String)]) {
        if self.gaps_shown.borrow().as_deref() == Some(gaps) {
            return;
        }
        while let Some(child) = self.gaps.first_child() {
            self.gaps.remove(&child);
        }
        if gaps.is_empty() {
            let none = gtk::Label::new(Some("No unallocated space on this disk."));
            none.add_css_class("dim-label");
            self.gaps.append(&none);
        } else {
            self.gaps.append(&gtk::Label::new(Some("Free space:")));
            for (start_lba, size_bytes, label) in gaps {
                let b = gtk::Button::with_label(label);
                b.set_widget_name(ids::TABLE_FREE_GAP);
                b.set_tooltip_text(Some("Fill this gap with the new partition"));
                let s = self.send.clone();
                let (start_lba, size_bytes) = (*start_lba, *size_bytes);
                b.connect_clicked(move |_| {
                    s(table(TableEditAction::FillGap {
                        start_lba,
                        size_bytes,
                    }))
                });
                self.gaps.append(&b);
            }
        }
        *self.gaps_shown.borrow_mut() = Some(gaps.to_vec());
    }

    fn build_rows(&self, v: &TableEditView) {
        let g = &self.rows;
        clear_grid(g);
        for (i, title) in [
            "#",
            "Type",
            "Start LBA",
            "Size Mode",
            "Size (MiB)",
            "Boot",
            "",
        ]
        .iter()
        .enumerate()
        {
            head(g, i as i32, title, None);
        }
        let mut cells = Vec::new();
        for (i, row) in v.rows.iter().enumerate() {
            let top = i as i32 + 1;
            let number = cell(0.0);
            let text = match (row.extended, row.logical) {
                (true, _) => format!("{} (ext)", row.index),
                (false, true) => format!("  {}", row.index),
                _ => row.index.to_string(),
            };
            number.set_label(&text);
            g.attach(&number, 0, top, 1, 1);
            let start = cell(1.0);
            start.set_label(&row.start_lba.to_string());
            g.attach(&start, 2, top, 1, 1);
            let mut c = RowCells {
                kind: None,
                mode: None,
                size: None,
                boot: Vec::new(),
                boot_label: None,
            };
            if !row.editable() {
                let kind = cell(0.0);
                kind.set_label(&plain(&row.type_name));
                g.attach(&kind, 1, top, 1, 1);
                let size = cell(1.0);
                if row.deleted {
                    size.set_label("deleted");
                    let undo = button("Undo", &format!("{}.{i}", ids::TABLE_ROW_UNDO));
                    undo.add_css_class("flat");
                    let s = self.send.clone();
                    undo.connect_clicked(move |_| s(table(TableEditAction::Undo(i))));
                    g.attach(&undo, 6, top, 1, 1);
                } else {
                    size.set_label(&format!("{:.2}", row.size_bytes as f64 / 1048576.0));
                }
                g.attach(&size, 4, top, 1, 1);
                for l in [&number, &kind, &start, &size] {
                    l.add_css_class("dim-label");
                }
                cells.push(c);
                continue;
            }
            let s = self.send.clone();
            let (type_box, entry, note) = type_field(
                v.kind,
                Rc::new(move |text| s(table(TableEditAction::SetType { row: i, text }))),
            );
            entry.set_widget_name(&format!("{}.{i}", ids::TABLE_ROW_TYPE));
            g.attach(&type_box, 1, top, 1, 1);
            c.kind = Some((entry, note));

            let choices = row.mode_choices();
            let labels: Vec<&str> = choices.iter().map(|m| m.label()).collect();
            let mode = gtk::DropDown::from_strings(&labels);
            mode.set_widget_name(&format!("{}.{i}", ids::TABLE_ROW_MODE));
            let held = choices.clone();
            let s = self.send.clone();
            mode.connect_selected_notify(move |d| {
                if let Some(mode) = held.get(d.selected() as usize).copied() {
                    s(table(TableEditAction::SetSizeMode { row: i, mode }));
                }
            });
            g.attach(&mode, 3, top, 1, 1);
            c.mode = Some((mode, choices));
            let size = gtk::Entry::builder().width_chars(10).build();
            size.set_widget_name(&format!("{}.{i}", ids::TABLE_ROW_SIZE));
            let s = self.send.clone();
            size.connect_changed(move |e| {
                let text = e.text().to_string();
                s(table(TableEditAction::SetSizeText { row: i, text }));
            });
            g.attach(&size, 4, top, 1, 1);
            c.size = Some(size);

            match v.boot_style {
                BootStyle::Checkbox => {
                    let b = gtk::CheckButton::new();
                    b.set_widget_name(&format!("{}.{i}", ids::TABLE_ROW_BOOT));
                    let s = self.send.clone();
                    b.connect_toggled(move |b| {
                        s(table(TableEditAction::SetBootable {
                            row: i,
                            on: b.is_active(),
                        }))
                    });
                    g.attach(&b, 5, top, 1, 1);
                    c.boot = vec![b];
                }
                BootStyle::Radios => {
                    let (rb, list) = crate::volume::radios(&["boot", "no boot"]);
                    rb.set_widget_name(&format!("{}.{i}", ids::TABLE_ROW_BOOT));
                    rb.set_tooltip_text(Some(
                        "Whether this RDB partition is eligible to boot. Multiple partitions can \
                         be set to boot at once - the Amiga ROM picks the one with the highest \
                         boot priority among them.",
                    ));
                    for (k, r) in list.iter().enumerate() {
                        let s = self.send.clone();
                        r.connect_toggled(move |r| {
                            if r.is_active() {
                                s(table(TableEditAction::SetBootable { row: i, on: k == 0 }));
                            }
                        });
                    }
                    g.attach(&rb, 5, top, 1, 1);
                    c.boot = list;
                }
                BootStyle::Label => {
                    let l = cell(0.0);
                    g.attach(&l, 5, top, 1, 1);
                    c.boot_label = Some(l);
                }
            }
            if row.can_delete() {
                let delete = button("Delete", &format!("{}.{i}", ids::TABLE_ROW_DELETE));
                delete.add_css_class("flat");
                delete.set_tooltip_text(Some("Mark partition for deletion"));
                let s = self.send.clone();
                delete.connect_clicked(move |_| s(table(TableEditAction::Delete(i))));
                g.attach(&delete, 6, top, 1, 1);
            }
            cells.push(c);
        }
        *self.cells.borrow_mut() = cells;
    }
}
