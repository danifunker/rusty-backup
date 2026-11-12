//! Edit Partition Table over Inspect's `TableEditView`; the Win32 shell's `editor.rs` is the
//! reference. Apply Changes only sends `ApplyTableEdits`: the write begins on the controller's own
//! question, and the name above the button is the one that question uses.
//!
//! The row grid and the gap buttons are built again only when their shape changes. A type's
//! Pick... is a pull-down of the table's catalog, so accessibility reaches every entry.

use std::cell::RefCell;
use std::rc::{Rc, Weak};

use objc2::rc::Retained;
use objc2::MainThreadMarker;
use objc2_app_kit::NSWindow;
use rusty_backup::controller::inspect::InspectAction;
use rusty_backup::controller::partition_edit::{
    ids, type_choices, type_note, BootStyle, LayoutBar, SizeMode, TableEditAction, TableEditView,
    TableKind,
};

use crate::form::Form;
use crate::layout::{self, Node, Size};
use crate::partition_bar::{self, Bar};
use crate::sync::SyncGuard;
use crate::ui::{self, CloseDelegate};

const HEADS: [&str; 7] = [
    "#",
    "Type",
    "Start LBA",
    "Size Mode",
    "Size (MiB)",
    "Boot",
    "",
];

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum K {
    KindLine,
    CurrentCaption,
    Current,
    AfterCaption,
    After,
    Head(usize),
    Number(usize),
    Type(usize),
    Pick(usize),
    PickItem(usize, usize),
    Note(usize),
    TypeLabel(usize),
    Start(usize),
    Mode(usize),
    Size(usize),
    SizeLabel(usize),
    Boot(usize),
    BootNo(usize),
    BootLabel(usize),
    Delete(usize),
    Undo(usize),
    GapsCaption,
    Gap(usize),
    AddHead,
    AddCaption(usize),
    AddStart,
    AddSize,
    AddType,
    AddPick,
    AddPickItem(usize),
    AddNote,
    AddBoot,
    Add,
    NextSlot,
    Errors,
    Status,
    Target,
    Validate,
    Apply,
    Cancel,
}

fn is_grid_key(k: &K) -> bool {
    matches!(
        k,
        K::Number(_)
            | K::Type(_)
            | K::Pick(_)
            | K::Note(_)
            | K::TypeLabel(_)
            | K::Start(_)
            | K::Mode(_)
            | K::Size(_)
            | K::SizeLabel(_)
            | K::Boot(_)
            | K::BootNo(_)
            | K::BootLabel(_)
            | K::Delete(_)
            | K::Undo(_)
            | K::Gap(_)
    )
}

/// What the grid was built for: a change here throws it away.
#[derive(PartialEq)]
struct Shape {
    kind: TableKind,
    boot: BootStyle,
    rows: Vec<(bool, bool, bool, Vec<SizeMode>)>,
    gaps: Vec<(u64, u64, String)>,
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
            gaps: v.gaps.clone(),
        }
    }
}

pub struct EditorWindow {
    window: Retained<NSWindow>,
    _delegate: Retained<CloseDelegate>,
    form: Form<K>,
    current: Retained<Bar>,
    after: Retained<Bar>,
    send: Rc<dyn Fn(InspectAction)>,
    sync: SyncGuard,
    shape: RefCell<Option<Shape>>,
}

fn table(edit: TableEditAction) -> InspectAction {
    InspectAction::Table(edit)
}

impl EditorWindow {
    /// The window; `send` delivers its edits to the Inspect controller.
    pub fn open(mtm: MainThreadMarker, send: Rc<dyn Fn(InspectAction)>) -> Rc<EditorWindow> {
        Rc::new_cyclic(|me: &Weak<EditorWindow>| {
            let close = send.clone();
            let (window, delegate) = ui::window(
                mtm,
                "Edit Partition Table",
                820.0,
                640.0,
                Box::new(move || {
                    let close = close.clone();
                    crate::app::later(Box::new(move || close(InspectAction::CloseEditor)));
                }),
            );
            let resize = me.clone();
            delegate.on_resize(Box::new(move || {
                if let Some(w) = resize.upgrade() {
                    w.layout();
                }
            }));
            let body = window.contentView().expect("the window's content view");
            let fire = me.clone();
            let form = Form::new(
                mtm,
                body,
                Rc::new(move |k| {
                    if let Some(w) = fire.upgrade() {
                        w.fired(k);
                    }
                }),
            );
            let f = &form;
            f.label(K::KindLine, "");
            f.label(K::CurrentCaption, "");
            let current = Bar::new(mtm, "table.current_bar");
            current.set_options(1.0, false);
            f.add(K::Current, &current);
            f.label(K::AfterCaption, "");
            let after = Bar::new(mtm, "table.after_bar");
            f.add(K::After, &after);
            for (i, t) in HEADS.iter().enumerate() {
                f.label(K::Head(i), t);
            }
            f.label(K::GapsCaption, "");
            f.label(K::AddHead, "Add Partition");
            for (i, t) in ["Start LBA:", "Size (MiB):", "Type:"].iter().enumerate() {
                f.label(K::AddCaption(i), t);
            }
            f.field(K::AddStart, ids::TABLE_ADD_START, "Start LBA", false);
            f.field(K::AddSize, ids::TABLE_ADD_SIZE, "Size (MiB)", false);
            f.field(K::AddType, ids::TABLE_ADD_TYPE, "Type", false);
            f.pulldown(K::AddPick, "table.add_pick");
            f.label(K::AddNote, "");
            f.check(K::AddBoot, "Bootable", ids::TABLE_ADD_BOOT);
            f.button(K::Add, "Add", ids::TABLE_ADD);
            f.wrap(K::NextSlot, "");
            f.wrap(K::Errors, "");
            f.label(K::Status, "");
            f.label(K::Target, "");
            f.button(K::Validate, "Validate", ids::TABLE_VALIDATE);
            f.button(K::Apply, "Apply Changes", ids::TABLE_APPLY);
            f.button(K::Cancel, "Cancel", ids::CANCEL);
            window.makeKeyAndOrderFront(None);
            EditorWindow {
                window,
                _delegate: delegate,
                form,
                current,
                after,
                send,
                sync: SyncGuard::default(),
                shape: RefCell::default(),
            }
        })
    }

    pub fn show(&self, v: &TableEditView, target: Option<&str>) {
        let f = &self.form;
        self.sync.run(|| {
            f.set_text(K::KindLine, &format!("Table type: {}", v.kind.label()));
            self.show_bar(K::CurrentCaption, &self.current, &v.current);
            self.show_bar(K::AfterCaption, &self.after, &v.after);
            let shape = Shape::of(v);
            if self.shape.borrow().as_ref() != Some(&shape) {
                self.build(v);
                *self.shape.borrow_mut() = Some(shape);
            }
            for (i, row) in v.rows.iter().enumerate() {
                if !row.editable() {
                    continue;
                }
                f.set_text(K::Type(i), &row.type_text);
                f.set_text(K::Note(i), type_note(v.kind, &row.type_text).unwrap_or(""));
                let choices = row.mode_choices();
                let at = choices
                    .iter()
                    .position(|m| *m == row.size_mode)
                    .unwrap_or(0);
                f.select(K::Mode(i), at);
                f.set_text(K::Size(i), &row.size_text);
                f.enable(K::Size(i), row.size_mode == SizeMode::Custom);
                match v.boot_style {
                    BootStyle::Checkbox => f.set_check(K::Boot(i), row.bootable),
                    BootStyle::Radios => {
                        f.set_check(K::Boot(i), row.bootable);
                        f.set_check(K::BootNo(i), !row.bootable);
                    }
                    BootStyle::Label => {
                        f.set_text(K::BootLabel(i), if row.bootable { "Yes" } else { "" })
                    }
                }
            }
            f.set_text(K::AddStart, &v.add_start);
            f.set_text(K::AddSize, &v.add_size);
            f.set_text(K::AddType, &v.add_type);
            f.set_text(K::AddNote, type_note(v.kind, &v.add_type).unwrap_or(""));
            f.show(K::AddPick, !type_choices(v.kind).is_empty());
            f.show(K::AddBoot, v.add_bootable_shown);
            f.set_check(K::AddBoot, v.add_bootable);
            f.set_text(
                K::NextSlot,
                &format!(
                    "Will be added as partition {} (slot assigned automatically; a slot freed by a \
                     Delete above is reused).",
                    v.next_slot
                ),
            );
            f.set_text(K::Errors, &v.errors.join("\n"));
            f.set_text(K::Status, v.status.as_deref().unwrap_or(""));
            let writes = target
                .map(|t| format!("Writes to: {t}"))
                .unwrap_or_default();
            f.set_text(K::Target, &writes);
            f.enable(K::Apply, v.can_apply);
        });
        self.layout();
    }

    fn show_bar(&self, caption: K, bar: &Bar, b: &LayoutBar) {
        self.form.set_text(caption, &b.caption);
        bar.set_options(1.0, b.legend);
        bar.show(b.layout_segments(), None);
    }

    /// The catalog's entries as pull-down items acting as `key(j)`.
    fn picks(kind: TableKind, key: impl Fn(usize) -> K) -> Vec<(String, Option<K>, bool)> {
        type_choices(kind)
            .iter()
            .enumerate()
            .map(|(j, c)| (format!("{}  -  {}", c.value, c.label), Some(key(j)), true))
            .collect()
    }

    /// Build the row grid and the gap buttons again for `v`'s shape.
    fn build(&self, v: &TableEditView) {
        let f = &self.form;
        f.remove(is_grid_key);
        let picks = !type_choices(v.kind).is_empty();
        for (i, row) in v.rows.iter().enumerate() {
            let number = match (row.extended, row.logical) {
                (true, _) => format!("{} (ext)", row.index),
                (false, true) => format!("  {}", row.index),
                _ => row.index.to_string(),
            };
            f.label(K::Number(i), &number);
            f.label(K::Start(i), &row.start_lba.to_string());
            if !row.editable() {
                f.label(K::TypeLabel(i), &row.type_name);
                if row.deleted {
                    f.label(K::SizeLabel(i), "deleted");
                    let id = format!("{}.{i}", ids::TABLE_ROW_UNDO);
                    f.button(K::Undo(i), "Undo", &id);
                } else {
                    let mib = format!("{:.2}", row.size_bytes as f64 / 1048576.0);
                    f.label(K::SizeLabel(i), &mib);
                }
                continue;
            }
            f.field(
                K::Type(i),
                &format!("{}.{i}", ids::TABLE_ROW_TYPE),
                "Type",
                false,
            );
            f.pulldown(K::Pick(i), &format!("table.row_pick.{i}"));
            f.set_pulldown(
                K::Pick(i),
                "Pick...",
                &Self::picks(v.kind, |j| K::PickItem(i, j)),
            );
            f.show(K::Pick(i), picks);
            f.label(K::Note(i), "");
            let names: Vec<String> = row
                .mode_choices()
                .iter()
                .map(|m| m.label().to_string())
                .collect();
            f.popup(K::Mode(i), &names, &format!("{}.{i}", ids::TABLE_ROW_MODE));
            f.field(
                K::Size(i),
                &format!("{}.{i}", ids::TABLE_ROW_SIZE),
                "Size (MiB)",
                false,
            );
            let boot_id = format!("{}.{i}", ids::TABLE_ROW_BOOT);
            match v.boot_style {
                BootStyle::Checkbox => {
                    f.check(K::Boot(i), "", &boot_id);
                }
                BootStyle::Radios => {
                    f.radio(K::Boot(i), "boot", &boot_id);
                    f.radio(K::BootNo(i), "no boot", "");
                }
                BootStyle::Label => {
                    f.label(K::BootLabel(i), "");
                }
            }
            if row.can_delete() {
                let id = format!("{}.{i}", ids::TABLE_ROW_DELETE);
                f.button(K::Delete(i), "Delete", &id);
            }
        }
        let caption = if v.gaps.is_empty() {
            "No unallocated space on this disk."
        } else {
            "Free space:"
        };
        f.set_text(K::GapsCaption, caption);
        for (n, (_, _, label)) in v.gaps.iter().enumerate() {
            f.button(K::Gap(n), label, ids::TABLE_FREE_GAP);
        }
        f.set_pulldown(K::AddPick, "Pick...", &Self::picks(v.kind, K::AddPickItem));
    }

    fn layout(&self) {
        let Some(body) = self.window.contentView() else {
            return;
        };
        let f = &self.form;
        let shape = self.shape.borrow();
        let Some(shape) = shape.as_ref() else {
            return;
        };
        let line =
            |k: K, n: i32| Node::leaf(k, Size::Stretch(1), Size::Fixed((layout::ROW - 6) * n + 4));
        let cell = |k: K, w: i32| Node::leaf(k, Size::Fixed(w), Size::Fixed(layout::ROW));
        let pop = |k: K, w: i32| Node::leaf(k, Size::Fixed(w), Size::Fixed(layout::ROW + 2));
        let gpt = shape.kind == TableKind::Gpt;
        let type_w = if gpt { 300 } else { 220 };
        let widths = [40, type_w, 100, 120, 90, 130, 80];
        let mut col = vec![
            line(K::KindLine, 1),
            line(K::CurrentCaption, 1),
            Node::leaf(
                K::Current,
                Size::Stretch(1),
                Size::Fixed(partition_bar::BAR_ONLY),
            ),
            line(K::AfterCaption, 1),
            Node::leaf(
                K::After,
                Size::Stretch(1),
                Size::Fixed(partition_bar::HEIGHT),
            ),
            Node::row(
                (0..HEADS.len())
                    .map(|h| cell(K::Head(h), widths[h]))
                    .collect(),
            ),
        ];
        for i in 0..shape.rows.len() {
            let mut r = vec![cell(K::Number(i), widths[0])];
            if f.has(K::Type(i)) {
                let (ew, pw) = if gpt { (190, 70) } else { (60, 70) };
                let mut t = vec![cell(K::Type(i), ew)];
                let mut left = widths[1] - ew - 4;
                if f.shown(K::Pick(i)) {
                    t.push(pop(K::Pick(i), pw));
                    left -= pw + 4;
                }
                t.push(cell(K::Note(i), left.max(0)));
                r.push(Node::Row {
                    children: t,
                    gap: 4,
                });
                r.push(cell(K::Start(i), widths[2]));
                r.push(pop(K::Mode(i), widths[3]));
                r.push(cell(K::Size(i), widths[4]));
            } else {
                r.push(cell(K::TypeLabel(i), widths[1]));
                r.push(cell(K::Start(i), widths[2]));
                r.push(Node::Space(Size::Fixed(widths[3])));
                r.push(cell(K::SizeLabel(i), widths[4]));
            }
            if f.has(K::BootNo(i)) {
                r.push(Node::Row {
                    children: vec![cell(K::Boot(i), 55), cell(K::BootNo(i), 75)],
                    gap: 4,
                });
            } else if f.has(K::Boot(i)) {
                r.push(cell(K::Boot(i), widths[5]));
            } else if f.has(K::BootLabel(i)) {
                r.push(cell(K::BootLabel(i), widths[5]));
            } else {
                r.push(Node::Space(Size::Fixed(widths[5])));
            }
            if f.has(K::Delete(i)) {
                r.push(Node::button(K::Delete(i), widths[6]));
            } else if f.has(K::Undo(i)) {
                r.push(Node::button(K::Undo(i), widths[6]));
            }
            col.push(Node::Row {
                children: r,
                gap: layout::GAP,
            });
        }
        col.push(line(K::AddHead, 1));
        let mut gap_row = vec![Node::leaf(
            K::GapsCaption,
            Size::Fixed(230),
            Size::Fixed(layout::ROW),
        )];
        gap_row.extend((0..shape.gaps.len()).map(|n| Node::button(K::Gap(n), 210)));
        col.push(Node::row(gap_row));
        let mut add_row = vec![
            cell(K::AddCaption(0), 75),
            cell(K::AddStart, 90),
            cell(K::AddCaption(1), 80),
            cell(K::AddSize, 70),
            cell(K::AddCaption(2), 45),
            cell(K::AddType, if gpt { 200 } else { 60 }),
        ];
        if f.shown(K::AddPick) {
            add_row.push(pop(K::AddPick, 70));
        }
        add_row.push(Node::leaf(
            K::AddNote,
            Size::Stretch(1),
            Size::Fixed(layout::ROW),
        ));
        if f.shown(K::AddBoot) {
            add_row.push(cell(K::AddBoot, 90));
        }
        add_row.push(Node::button(K::Add, 70));
        col.push(Node::row(add_row));
        col.push(line(K::NextSlot, 2));
        col.push(Node::leaf(K::Errors, Size::Stretch(1), Size::Stretch(1)));
        col.push(line(K::Status, 1));
        col.push(line(K::Target, 1));
        col.push(Node::row(vec![
            Node::Space(Size::Stretch(1)),
            Node::button(K::Validate, 95),
            Node::button(K::Apply, 125),
            Node::button(K::Cancel, 90),
        ]));
        let tree = Node::inset(Node::column(col), layout::MARGIN);
        f.place_shown(tree, ui::area(&body));
    }

    fn fired(&self, k: K) {
        if self.sync.active() {
            return;
        }
        let f = &self.form;
        let shape = self.shape.borrow();
        let Some(shape) = shape.as_ref() else {
            return;
        };
        let pick = |j: usize| type_choices(shape.kind).get(j).map(|c| c.value.to_string());
        let action = match k {
            K::Type(row) => table(TableEditAction::SetType {
                row,
                text: f.text(k),
            }),
            K::PickItem(row, j) => match pick(j) {
                Some(text) => table(TableEditAction::SetType { row, text }),
                None => return,
            },
            K::Mode(row) => {
                let mode = shape
                    .rows
                    .get(row)
                    .and_then(|r| r.3.get(f.picked(k)?).copied());
                match mode {
                    Some(mode) => table(TableEditAction::SetSizeMode { row, mode }),
                    None => return,
                }
            }
            K::Size(row) => table(TableEditAction::SetSizeText {
                row,
                text: f.text(k),
            }),
            K::Boot(row) => {
                let on = shape.boot != BootStyle::Checkbox || f.checked(k);
                table(TableEditAction::SetBootable { row, on })
            }
            K::BootNo(row) => table(TableEditAction::SetBootable { row, on: false }),
            K::Delete(row) => table(TableEditAction::Delete(row)),
            K::Undo(row) => table(TableEditAction::Undo(row)),
            K::Gap(n) => match shape.gaps.get(n) {
                Some((start_lba, size_bytes, _)) => table(TableEditAction::FillGap {
                    start_lba: *start_lba,
                    size_bytes: *size_bytes,
                }),
                None => return,
            },
            K::AddStart => table(TableEditAction::SetAddStart(f.text(k))),
            K::AddSize => table(TableEditAction::SetAddSize(f.text(k))),
            K::AddType => table(TableEditAction::SetAddType(f.text(k))),
            K::AddPickItem(j) => match pick(j) {
                Some(t) => table(TableEditAction::SetAddType(t)),
                None => return,
            },
            K::AddBoot => table(TableEditAction::SetAddBootable(f.checked(k))),
            K::Add => table(TableEditAction::Add),
            K::Validate => table(TableEditAction::Validate),
            K::Apply => InspectAction::ApplyTableEdits,
            K::Cancel => InspectAction::CloseEditor,
            _ => return,
        };
        // The grid may be built again by what this sends: act once the handler returns.
        let send = self.send.clone();
        crate::app::later(Box::new(move || send(action)));
    }
}

impl Drop for EditorWindow {
    fn drop(&mut self) {
        self.window.setDelegate(None);
        self.window.orderOut(None);
    }
}
