//! Edit Partition Table over Inspect's `TableEditView`; `src/gui/partition_editor_modal.rs` is the
//! reference. Apply Changes only sends `ApplyTableEdits`: the write begins on the controller's own
//! question, and the name above the button is the one that question uses.
//!
//! The row grid and the gap buttons are rebuilt only when their shape changes; every control maps
//! to one [`Cmd`], so a `WM_COMMAND` is one lookup.

use std::cell::RefCell;
use std::rc::{Rc, Weak};

use rusty_backup::controller::inspect::InspectAction;
use rusty_backup::controller::partition_edit::{
    ids, type_choices, type_note, BootStyle, LayoutBar, SizeMode, TableEditAction, TableEditView,
    TableKind,
};
use windows::core::{w, PCWSTR};
use windows::Win32::Foundation::{HWND, LPARAM, LRESULT, RECT, WPARAM};
use windows::Win32::UI::Controls::NMHDR;
use windows::Win32::UI::Input::KeyboardAndMouse::GetFocus;
use windows::Win32::UI::WindowsAndMessaging::{
    AppendMenuW, CreatePopupMenu, DestroyMenu, DestroyWindow, GetWindowRect, SendMessageW,
    ShowWindow, TrackPopupMenu, BM_GETCHECK, BM_SETCHECK, BN_CLICKED, BS_AUTOCHECKBOX,
    BS_AUTORADIOBUTTON, CBN_SELCHANGE, CBS_DROPDOWNLIST, CB_ADDSTRING, CB_GETCURSEL, CB_SETCURSEL,
    EN_CHANGE, MF_STRING, SW_SHOW, TPM_RETURNCMD, WINDOW_EX_STYLE, WINDOW_STYLE, WS_GROUP,
    WS_TABSTOP, WS_VSCROLL,
};

use crate::layout::{self, Node, Size};
use crate::panel::{self, Handler};
use crate::partition_bar;
use crate::sync::SyncGuard;
use crate::ui;

/// What a control means.
#[derive(Clone, Copy, PartialEq)]
enum Cmd {
    RowType(usize),
    RowPick(usize),
    RowMode(usize),
    RowSize(usize),
    RowBoot(usize, bool),
    RowDelete(usize),
    RowUndo(usize),
    Gap(u64, u64),
    AddStart,
    AddSize,
    AddType,
    AddPick,
    AddBoot,
    Add,
    Validate,
    Apply,
    Cancel,
}

/// The cells a refresh rebuilds: a change here throws the grid away.
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

/// One row's controls, in grid order; an uneditable row has only labels and maybe Undo.
#[derive(Default)]
struct RowCells {
    number: HWND,
    kind: Option<(HWND, HWND, Option<HWND>)>,
    kind_label: Option<HWND>,
    start: HWND,
    mode: Option<(HWND, Vec<SizeMode>)>,
    size: Option<HWND>,
    size_label: Option<HWND>,
    boot: Vec<HWND>,
    boot_label: Option<HWND>,
    action: Option<HWND>,
}

pub struct EditorWindow {
    window: HWND,
    send: Rc<dyn Fn(InspectAction)>,
    sync: SyncGuard,
    me: Weak<EditorWindow>,
    kind_line: HWND,
    current_caption: HWND,
    current: HWND,
    after_caption: HWND,
    after: HWND,
    heads: [HWND; 7],
    rows: RefCell<Vec<RowCells>>,
    shape: RefCell<Option<Shape>>,
    gaps_caption: HWND,
    gaps: RefCell<Vec<HWND>>,
    add_head: HWND,
    add_captions: [HWND; 3],
    add_start: HWND,
    add_size: HWND,
    add_type: HWND,
    add_pick: HWND,
    add_note: HWND,
    add_boot: HWND,
    add: HWND,
    next_slot: HWND,
    errors: HWND,
    status: HWND,
    target: HWND,
    validate: HWND,
    apply: HWND,
    cancel: HWND,
    cmds: RefCell<Vec<(HWND, Cmd)>>,
    kind: RefCell<Option<TableKind>>,
}

fn table(edit: TableEditAction) -> InspectAction {
    InspectAction::Table(edit)
}

fn set_check(h: HWND, on: bool) {
    // SAFETY: plain button messages.
    unsafe {
        if (SendMessageW(h, BM_GETCHECK, None, None).0 == 1) != on {
            SendMessageW(h, BM_SETCHECK, Some(WPARAM(on as usize)), None);
        }
    }
}

/// Set a field's text unless the user is typing in it.
fn set_field(h: HWND, text: &str) {
    // SAFETY: a plain focus query.
    if unsafe { GetFocus() } != h {
        ui::set_text(h, text);
    }
}

impl EditorWindow {
    /// The window; `send` delivers its edits to the Inspect controller.
    pub fn open(owner: HWND, send: Rc<dyn Fn(InspectAction)>) -> Rc<Self> {
        let w = panel::dialog(owner, "Edit Partition Table", 820, 640);
        let heads = [
            "#",
            "Type",
            "Start LBA",
            "Size Mode",
            "Size (MiB)",
            "Boot",
            "",
        ]
        .map(|t| ui::label(w, t));
        let add_captions = ["Start LBA:", "Size (MiB):", "Type:"].map(|t| ui::cell_label(w, t));
        let me = Rc::new_cyclic(|me: &Weak<EditorWindow>| EditorWindow {
            window: w,
            send,
            sync: SyncGuard::default(),
            me: me.clone(),
            kind_line: ui::label(w, ""),
            current_caption: ui::label(w, ""),
            current: partition_bar::build(w, Box::new(|_| {})),
            after_caption: ui::label(w, ""),
            after: partition_bar::build(w, Box::new(|_| {})),
            heads,
            rows: RefCell::default(),
            shape: RefCell::default(),
            gaps_caption: ui::cell_label(w, ""),
            gaps: RefCell::default(),
            add_head: ui::label(w, "Add Partition"),
            add_captions,
            add_start: ui::edit(w, ids::TABLE_ADD_START),
            add_size: ui::edit(w, ids::TABLE_ADD_SIZE),
            add_type: ui::edit(w, ids::TABLE_ADD_TYPE),
            add_pick: ui::button(w, "Pick...", ""),
            add_note: ui::cell_label(w, ""),
            add_boot: ui::checkbox(w, "Bootable", ids::TABLE_ADD_BOOT),
            add: ui::button(w, "Add", ids::TABLE_ADD),
            next_slot: ui::label(w, ""),
            errors: ui::label(w, ""),
            status: ui::label(w, ""),
            target: ui::path_label(w, ""),
            validate: ui::button(w, "Validate", ids::TABLE_VALIDATE),
            apply: ui::button(w, "Apply Changes", ids::TABLE_APPLY),
            cancel: ui::button(w, "Cancel", ids::CANCEL),
            cmds: RefCell::default(),
            kind: RefCell::default(),
        });
        partition_bar::set_options(me.current, 1.0, false);
        let weak: Weak<dyn Handler> = me.me.clone();
        panel::register(w, weak);
        // SAFETY: showing the window we created.
        unsafe {
            let _ = ShowWindow(w, SW_SHOW);
        }
        me
    }

    /// The fixed controls' commands; the rebuilt ones are added by `build`.
    fn fixed_cmds(&self) -> Vec<(HWND, Cmd)> {
        vec![
            (self.add_start, Cmd::AddStart),
            (self.add_size, Cmd::AddSize),
            (self.add_type, Cmd::AddType),
            (self.add_pick, Cmd::AddPick),
            (self.add_boot, Cmd::AddBoot),
            (self.add, Cmd::Add),
            (self.validate, Cmd::Validate),
            (self.apply, Cmd::Apply),
            (self.cancel, Cmd::Cancel),
        ]
    }

    pub fn show(&self, v: &TableEditView, target: Option<&str>) {
        self.sync.run(|| {
            *self.kind.borrow_mut() = Some(v.kind);
            ui::set_text(self.kind_line, &format!("Table type: {}", v.kind.label()));
            self.show_bar(self.current_caption, self.current, &v.current);
            self.show_bar(self.after_caption, self.after, &v.after);
            let shape = Shape::of(v);
            let rebuilt = self.shape.borrow().as_ref() != Some(&shape);
            if rebuilt {
                self.build(v);
                *self.shape.borrow_mut() = Some(shape);
            }
            for (row, c) in v.rows.iter().zip(self.rows.borrow().iter()) {
                if let Some((entry, _, note)) = &c.kind {
                    set_field(*entry, &row.type_text);
                    if let Some(n) = note {
                        ui::set_text(*n, type_note(v.kind, &row.type_text).unwrap_or(""));
                    }
                }
                if let Some((mode, choices)) = &c.mode {
                    let at = choices
                        .iter()
                        .position(|m| *m == row.size_mode)
                        .unwrap_or(0);
                    // SAFETY: plain combo box messages.
                    unsafe {
                        if SendMessageW(*mode, CB_GETCURSEL, None, None).0 as usize != at {
                            SendMessageW(*mode, CB_SETCURSEL, Some(WPARAM(at)), None);
                        }
                    }
                }
                if let Some(size) = c.size {
                    set_field(size, &row.size_text);
                    ui::set_enabled(size, row.size_mode == SizeMode::Custom);
                }
                match c.boot.as_slice() {
                    [one] => set_check(*one, row.bootable),
                    [yes, no] => {
                        set_check(*yes, row.bootable);
                        set_check(*no, !row.bootable);
                    }
                    _ => {}
                }
                if let Some(l) = c.boot_label {
                    ui::set_text(l, if row.bootable { "Yes" } else { "" });
                }
            }
            set_field(self.add_start, &v.add_start);
            set_field(self.add_size, &v.add_size);
            set_field(self.add_type, &v.add_type);
            ui::set_text(self.add_note, type_note(v.kind, &v.add_type).unwrap_or(""));
            ui::set_visible(self.add_pick, !type_choices(v.kind).is_empty());
            ui::set_visible(self.add_boot, v.add_bootable_shown);
            set_check(self.add_boot, v.add_bootable);
            ui::set_text(
                self.next_slot,
                &format!(
                    "Will be added as partition {} (slot assigned automatically; a slot freed by a \
                     Delete above is reused).",
                    v.next_slot
                ),
            );
            ui::set_text(self.errors, &v.errors.join("\r\n"));
            ui::set_text(self.status, v.status.as_deref().unwrap_or(""));
            ui::set_text(
                self.target,
                &target
                    .map(|t| format!("Writes to: {t}"))
                    .unwrap_or_default(),
            );
            ui::set_enabled(self.apply, v.can_apply);
            if rebuilt {
                self.layout();
            }
        });
    }

    fn show_bar(&self, caption: HWND, bar: HWND, b: &LayoutBar) {
        ui::set_text(caption, &b.caption);
        partition_bar::set_options(bar, 1.0, b.legend);
        partition_bar::show(bar, b.layout_segments(), None);
    }

    /// Rebuild the row grid and the gap buttons for `v`'s shape.
    fn build(&self, v: &TableEditView) {
        let w = self.window;
        for c in self.rows.borrow_mut().drain(..) {
            let mut all = vec![c.number, c.start];
            all.extend(
                c.kind
                    .iter()
                    .flat_map(|(a, b, n)| [Some(*a), Some(*b), *n])
                    .flatten(),
            );
            all.extend(
                c.kind_label
                    .iter()
                    .chain(c.size.iter())
                    .chain(c.size_label.iter()),
            );
            all.extend(c.mode.iter().map(|(m, _)| *m));
            all.extend(
                c.boot
                    .iter()
                    .chain(c.boot_label.iter())
                    .chain(c.action.iter()),
            );
            for h in all {
                // SAFETY: our own controls, rebuilt for the new shape.
                let _ = unsafe { DestroyWindow(h) };
            }
        }
        for h in self.gaps.borrow_mut().drain(..) {
            // SAFETY: as above.
            let _ = unsafe { DestroyWindow(h) };
        }
        let mut cmds = self.fixed_cmds();
        let mut rows = Vec::new();
        let picks = !type_choices(v.kind).is_empty();
        for (i, row) in v.rows.iter().enumerate() {
            let number = match (row.extended, row.logical) {
                (true, _) => format!("{} (ext)", row.index),
                (false, true) => format!("  {}", row.index),
                _ => row.index.to_string(),
            };
            let mut c = RowCells {
                number: ui::cell_label(w, &number),
                start: ui::cell_label(w, &row.start_lba.to_string()),
                ..RowCells::default()
            };
            if !row.editable() {
                c.kind_label = Some(ui::cell_label(w, &row.type_name));
                if row.deleted {
                    c.size_label = Some(ui::cell_label(w, "deleted"));
                    let undo = ui::button(w, "Undo", &format!("{}.{i}", ids::TABLE_ROW_UNDO));
                    cmds.push((undo, Cmd::RowUndo(i)));
                    c.action = Some(undo);
                } else {
                    c.size_label = Some(ui::cell_label(
                        w,
                        &format!("{:.2}", row.size_bytes as f64 / 1048576.0),
                    ));
                }
                rows.push(c);
                continue;
            }
            let entry = ui::edit(w, &format!("{}.{i}", ids::TABLE_ROW_TYPE));
            cmds.push((entry, Cmd::RowType(i)));
            let pick = ui::button(w, "Pick...", "");
            ui::set_visible(pick, picks);
            cmds.push((pick, Cmd::RowPick(i)));
            c.kind = Some((entry, pick, Some(ui::cell_label(w, ""))));
            let style = WINDOW_STYLE((CBS_DROPDOWNLIST as u32) | WS_TABSTOP.0 | WS_VSCROLL.0);
            let mode = ui::child(
                w,
                w!("COMBOBOX"),
                "",
                style,
                WINDOW_EX_STYLE(0),
                &format!("{}.{i}", ids::TABLE_ROW_MODE),
            );
            let choices = row.mode_choices();
            for m in &choices {
                let t = ui::wide(m.label());
                // SAFETY: the combo copies the string.
                unsafe {
                    SendMessageW(mode, CB_ADDSTRING, None, Some(LPARAM(t.as_ptr() as isize)));
                }
            }
            cmds.push((mode, Cmd::RowMode(i)));
            c.mode = Some((mode, choices));
            let size = ui::edit(w, &format!("{}.{i}", ids::TABLE_ROW_SIZE));
            cmds.push((size, Cmd::RowSize(i)));
            c.size = Some(size);
            let boot_id = format!("{}.{i}", ids::TABLE_ROW_BOOT);
            match v.boot_style {
                BootStyle::Checkbox => {
                    let b = ui::child(
                        w,
                        w!("BUTTON"),
                        "",
                        WINDOW_STYLE((BS_AUTOCHECKBOX as u32) | WS_TABSTOP.0),
                        WINDOW_EX_STYLE(0),
                        &boot_id,
                    );
                    cmds.push((b, Cmd::RowBoot(i, true)));
                    c.boot = vec![b];
                }
                BootStyle::Radios => {
                    let style = |first: bool| {
                        WINDOW_STYLE(
                            (BS_AUTORADIOBUTTON as u32)
                                | WS_TABSTOP.0
                                | if first { WS_GROUP.0 } else { 0 },
                        )
                    };
                    let yes = ui::child(
                        w,
                        w!("BUTTON"),
                        "boot",
                        style(true),
                        WINDOW_EX_STYLE(0),
                        &boot_id,
                    );
                    let no = ui::child(
                        w,
                        w!("BUTTON"),
                        "no boot",
                        style(false),
                        WINDOW_EX_STYLE(0),
                        "",
                    );
                    cmds.push((yes, Cmd::RowBoot(i, true)));
                    cmds.push((no, Cmd::RowBoot(i, false)));
                    c.boot = vec![yes, no];
                }
                BootStyle::Label => c.boot_label = Some(ui::cell_label(w, "")),
            }
            if row.can_delete() {
                let delete = ui::button(w, "Delete", &format!("{}.{i}", ids::TABLE_ROW_DELETE));
                cmds.push((delete, Cmd::RowDelete(i)));
                c.action = Some(delete);
            }
            rows.push(c);
        }
        *self.rows.borrow_mut() = rows;
        let caption = if v.gaps.is_empty() {
            "No unallocated space on this disk."
        } else {
            "Free space:"
        };
        ui::set_text(self.gaps_caption, caption);
        let gaps = v
            .gaps
            .iter()
            .map(|(start, size, label)| {
                let b = ui::button(w, label, ids::TABLE_FREE_GAP);
                cmds.push((b, Cmd::Gap(*start, *size)));
                b
            })
            .collect();
        *self.gaps.borrow_mut() = gaps;
        *self.cmds.borrow_mut() = cmds;
    }

    fn layout(&self) {
        let (area, dpi) = panel::client(self.window);
        let line =
            |h: HWND, n: i32| Node::leaf(h, Size::Stretch(1), Size::Fixed((layout::ROW - 8) * n));
        let cell = |h: HWND, w: i32| Node::leaf(h, Size::Fixed(w), Size::Fixed(layout::ROW));
        let gpt = *self.kind.borrow() == Some(TableKind::Gpt);
        let type_w = if gpt { 300 } else { 220 };
        let mut col = vec![
            line(self.kind_line, 1),
            line(self.current_caption, 1),
            Node::leaf(
                self.current,
                Size::Stretch(1),
                Size::Fixed(partition_bar::BAR_ONLY),
            ),
            line(self.after_caption, 1),
            Node::leaf(
                self.after,
                Size::Stretch(1),
                Size::Fixed(partition_bar::HEIGHT),
            ),
        ];
        let widths = [30, type_w, 100, 120, 90, 110, 80];
        col.push(Node::row(
            self.heads
                .iter()
                .zip(widths)
                .map(|(h, w)| cell(*h, w))
                .collect(),
        ));
        let mut combos = Vec::new();
        for c in self.rows.borrow().iter() {
            let mut r = vec![cell(c.number, widths[0])];
            match (&c.kind, c.kind_label) {
                (Some((entry, pick, note)), _) => {
                    let (ew, pw) = if gpt { (190, 60) } else { (60, 60) };
                    let mut t = vec![cell(*entry, ew)];
                    let mut left = widths[1] - ew - 4;
                    if ui::shown(*pick) {
                        t.push(cell(*pick, pw));
                        left -= pw + 4;
                    }
                    // A nested row takes its children's width, so the note fills the column itself.
                    t.extend(note.map(|n| cell(n, left.max(0))));
                    r.push(Node::Row {
                        children: t,
                        gap: 4,
                    });
                }
                (None, Some(l)) => r.push(cell(l, widths[1])),
                (None, None) => r.push(Node::Space(Size::Fixed(widths[1]))),
            }
            r.push(cell(c.start, widths[2]));
            match &c.mode {
                Some((m, _)) => {
                    combos.push(*m);
                    r.push(cell(*m, widths[3]));
                }
                None => r.push(Node::Space(Size::Fixed(widths[3]))),
            }
            r.push(cell(c.size.or(c.size_label).unwrap_or(c.start), widths[4]));
            match (c.boot.as_slice(), c.boot_label) {
                ([one], _) => r.push(cell(*one, widths[5])),
                ([yes, no], _) => r.push(Node::Row {
                    children: vec![cell(*yes, 50), cell(*no, 70)],
                    gap: 4,
                }),
                (_, Some(l)) => r.push(cell(l, widths[5])),
                _ => r.push(Node::Space(Size::Fixed(widths[5]))),
            }
            if let Some(a) = c.action {
                r.push(cell(a, widths[6]));
            }
            col.push(Node::Row {
                children: r,
                gap: layout::GAP,
            });
        }
        col.push(line(self.add_head, 1));
        let mut gap_row = vec![Node::leaf(
            self.gaps_caption,
            Size::Fixed(220),
            Size::Fixed(layout::ROW),
        )];
        gap_row.extend(self.gaps.borrow().iter().map(|g| Node::button(*g, 200)));
        col.push(Node::row(gap_row));
        let mut add_row = vec![
            Node::leaf(
                self.add_captions[0],
                Size::Fixed(70),
                Size::Fixed(layout::ROW),
            ),
            cell(self.add_start, 90),
            Node::leaf(
                self.add_captions[1],
                Size::Fixed(75),
                Size::Fixed(layout::ROW),
            ),
            cell(self.add_size, 70),
            Node::leaf(
                self.add_captions[2],
                Size::Fixed(40),
                Size::Fixed(layout::ROW),
            ),
            cell(self.add_type, if gpt { 200 } else { 60 }),
        ];
        if ui::shown(self.add_pick) {
            add_row.push(cell(self.add_pick, 60));
        }
        add_row.push(Node::leaf(
            self.add_note,
            Size::Stretch(1),
            Size::Fixed(layout::ROW),
        ));
        if ui::shown(self.add_boot) {
            add_row.push(cell(self.add_boot, 90));
        }
        add_row.push(cell(self.add, 70));
        col.push(Node::row(add_row));
        col.push(line(self.next_slot, 1));
        col.push(Node::leaf(self.errors, Size::Stretch(1), Size::Stretch(1)));
        col.push(line(self.status, 1));
        col.push(line(self.target, 1));
        col.push(Node::row(vec![
            Node::Space(Size::Stretch(1)),
            Node::button(self.validate, 90),
            Node::button(self.apply, 120),
            Node::button(self.cancel, 90),
        ]));
        let tree = Node::inset(Node::column(col), layout::MARGIN);
        for (h, mut r) in layout::layout(&tree, area, dpi) {
            if combos.contains(&h) {
                // A drop-down list's window height includes its open list.
                r.h *= 8;
            }
            ui::move_to(h, r);
        }
    }

    /// "Pick...": a type from the catalog, stamped into its field.
    fn pick_type(&self, from: HWND) -> Option<String> {
        let kind = (*self.kind.borrow())?;
        let picks = type_choices(kind);
        // SAFETY: a popup menu we create, track and destroy; each string outlives its append.
        let picked = unsafe {
            let menu = CreatePopupMenu().ok()?;
            for (i, c) in picks.iter().enumerate() {
                let t = ui::wide(&format!("{}  -  {}", c.value, c.label));
                let _ = AppendMenuW(menu, MF_STRING, i + 1, PCWSTR(t.as_ptr()));
            }
            let mut r = RECT::default();
            let _ = GetWindowRect(from, &mut r);
            let id = TrackPopupMenu(
                menu,
                TPM_RETURNCMD,
                r.left,
                r.bottom,
                None,
                self.window,
                None,
            )
            .0 as usize;
            let _ = DestroyMenu(menu);
            id
        };
        picks
            .get(picked.checked_sub(1)?)
            .map(|c| c.value.to_string())
    }
}

impl Drop for EditorWindow {
    fn drop(&mut self) {
        panel::unregister(self.window);
        // SAFETY: the controller closed the editor; the window goes with it.
        let _ = unsafe { DestroyWindow(self.window) };
    }
}

impl Handler for EditorWindow {
    fn command(&self, from: HWND, code: u32) {
        if self.sync.active() {
            return;
        }
        let Some(cmd) = self
            .cmds
            .borrow()
            .iter()
            .find(|(h, _)| *h == from)
            .map(|(_, c)| *c)
        else {
            return;
        };
        let clicked = code == BN_CLICKED;
        let changed = code == EN_CHANGE;
        let checked = || {
            // SAFETY: a plain button query.
            unsafe { SendMessageW(from, BM_GETCHECK, None, None).0 == 1 }
        };
        let action = match cmd {
            Cmd::RowType(row) if changed => table(TableEditAction::SetType {
                row,
                text: ui::text(from),
            }),
            Cmd::RowPick(row) if clicked => match self.pick_type(from) {
                Some(text) => table(TableEditAction::SetType { row, text }),
                None => return,
            },
            Cmd::RowMode(row) if code == CBN_SELCHANGE => {
                // SAFETY: a plain combo box query.
                let i = unsafe { SendMessageW(from, CB_GETCURSEL, None, None).0 };
                let rows = self.rows.borrow();
                let mode = rows
                    .get(row)
                    .and_then(|c| c.mode.as_ref())
                    .and_then(|(_, ch)| usize::try_from(i).ok().and_then(|i| ch.get(i).copied()));
                match mode {
                    Some(mode) => table(TableEditAction::SetSizeMode { row, mode }),
                    None => return,
                }
            }
            Cmd::RowSize(row) if changed => table(TableEditAction::SetSizeText {
                row,
                text: ui::text(from),
            }),
            Cmd::RowBoot(row, yes) if clicked => {
                let on = if self
                    .rows
                    .borrow()
                    .get(row)
                    .is_some_and(|c| c.boot.len() == 1)
                {
                    checked()
                } else {
                    yes
                };
                table(TableEditAction::SetBootable { row, on })
            }
            Cmd::RowDelete(row) if clicked => table(TableEditAction::Delete(row)),
            Cmd::RowUndo(row) if clicked => table(TableEditAction::Undo(row)),
            Cmd::Gap(start_lba, size_bytes) if clicked => table(TableEditAction::FillGap {
                start_lba,
                size_bytes,
            }),
            Cmd::AddStart if changed => table(TableEditAction::SetAddStart(ui::text(from))),
            Cmd::AddSize if changed => table(TableEditAction::SetAddSize(ui::text(from))),
            Cmd::AddType if changed => table(TableEditAction::SetAddType(ui::text(from))),
            Cmd::AddPick if clicked => match self.pick_type(from) {
                Some(t) => table(TableEditAction::SetAddType(t)),
                None => return,
            },
            Cmd::AddBoot if clicked => table(TableEditAction::SetAddBootable(checked())),
            Cmd::Add if clicked => table(TableEditAction::Add),
            Cmd::Validate if clicked => table(TableEditAction::Validate),
            Cmd::Apply if clicked => InspectAction::ApplyTableEdits,
            Cmd::Cancel if clicked => InspectAction::CloseEditor,
            _ => return,
        };
        (self.send)(action);
    }

    fn notify(&self, _hdr: &NMHDR, _lp: LPARAM) -> Option<LRESULT> {
        None
    }

    fn resized(&self) {
        self.layout();
    }

    fn close(&self) {
        (self.send)(InspectAction::CloseEditor);
    }
}
