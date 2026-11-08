//! The Partitions step: the layout bar, one pair of grid lines per row, handlers and the Mac driver.

use std::cell::RefCell;
use std::path::PathBuf;

use adw::prelude::*;
use rusty_backup::controller::build_disk::{
    format_size, hints, ids, BuildDiskAction, BuildDiskController, FsKind, MacDriver, RowContent,
    TableKind,
};
use rusty_backup::controller::inspect::{LayoutSegment, SegmentKind};
use rusty_backup::controller::partition_edit::{
    type_choices, type_note, BarSegment, BuildAction, BuildRowView, BuildView,
};

use super::ActionFn;
use crate::form::{
    button, cell, clear_grid, combo, grid, group, head, line, set_combo, set_labels, set_line,
};
use crate::inspect::bar::PartitionBar;
use crate::widgets::{plain, set_text};

/// A row's content as the dropdown offers it.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Choice {
    Copy,
    Empty,
    Fs(FsKind),
}

/// Copy where the row has a donor, Empty, then every filesystem.
fn choices(can_copy: bool) -> Vec<Choice> {
    let copy = can_copy.then_some(Choice::Copy);
    let fs = FsKind::ALL.iter().map(|k| Choice::Fs(*k));
    copy.into_iter().chain([Choice::Empty]).chain(fs).collect()
}

fn label_of(c: Choice) -> &'static str {
    match c {
        Choice::Copy => "Copy",
        Choice::Empty => "Empty",
        Choice::Fs(k) => k.display_name(),
    }
}

fn segments(list: &[BarSegment]) -> Vec<LayoutSegment> {
    let each = list.iter().map(|s| LayoutSegment {
        label: s.label.clone(),
        fs: s.fs.clone(),
        size_bytes: s.size_bytes,
        kind: match s.color {
            Some(color_index) => SegmentKind::Partition { color_index },
            None => SegmentKind::Free,
        },
        partition: None,
    });
    each.collect()
}

/// A row's content, read beside its view.
struct RowState {
    content: RowContent,
    fs: Option<FsKind>,
    can_copy: bool,
}

/// What the step shows.
pub struct RowsShown {
    view: BuildView,
    rows: Vec<RowState>,
    probing: bool,
    takes_handlers: bool,
    handlers: Vec<(String, PathBuf)>,
    missing: Vec<String>,
    takes_driver: bool,
    driver: MacDriver,
}

impl RowsShown {
    pub fn read(ctrl: &BuildDiskController) -> RowsShown {
        let view = ctrl.table_view();
        let rows = (0..view.rows.len()).map(|i| RowState {
            content: ctrl.row_content(i),
            fs: ctrl.row_fs(i),
            can_copy: ctrl.row_can_copy(i),
        });
        let rows = rows.collect();
        RowsShown {
            view,
            rows,
            probing: ctrl.probing(),
            takes_handlers: ctrl.takes_handlers(),
            handlers: ctrl.handlers().to_vec(),
            missing: ctrl.handlers_missing(),
            takes_driver: ctrl.takes_mac_driver(),
            driver: ctrl.mac_driver().clone(),
        }
    }
}

/// Two lines of the grid: the row's fields, then its content and its moves.
struct RowCells {
    size: gtk::Entry,
    kind: gtk::Entry,
    note: gtk::Label,
    name: gtk::Entry,
    boot: gtk::CheckButton,
    start: gtk::Label,
    content: gtk::DropDown,
    choices: Vec<Choice>,
    image: gtk::Button,
    source: gtk::Label,
    clear: gtk::Button,
    up: gtk::Button,
    down: gtk::Button,
    remove: gtk::Button,
}

/// What the cells were built for: a change here rebuilds them.
#[derive(PartialEq, Eq)]
struct Shape {
    kind: TableKind,
    names: bool,
    boot: Option<bool>,
    fixed: bool,
    copies: Vec<bool>,
}

fn entry(id: String, chars: i32, hint: Option<&str>) -> gtk::Entry {
    let entry = gtk::Entry::builder().width_chars(chars).build();
    entry.set_widget_name(&id);
    entry.set_tooltip_text(hint);
    entry
}

fn set_field(entry: &gtk::Entry, text: &str) {
    let text = plain(text);
    if entry.text() != text.as_ref() {
        entry.set_text(&text);
    }
}

/// "Pick...": a type from the catalog, stamped into the row's field.
fn pick_menu(kind: TableKind, row: usize, send: &ActionFn) -> gtk::MenuButton {
    let list = gtk::ListBox::builder()
        .selection_mode(gtk::SelectionMode::None)
        .activate_on_single_click(true)
        .build();
    let picks = type_choices(kind);
    for c in picks {
        let text = format!("{}  -  {}", c.value, c.label);
        let label = gtk::Label::builder().label(text).xalign(0.0).build();
        list.append(&label);
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
    menu.set_widget_name(&format!("{}.{row}", ids::ROW_PICK_TYPE));
    menu.set_tooltip_text(Some(hints::ROW_PICK_TYPE));
    let s = send.clone();
    list.connect_row_activated(move |_, line| {
        popover.popdown();
        if let Some(c) = picks.get(line.index() as usize) {
            s(BuildDiskAction::Table(BuildAction::SetRowType {
                row,
                text: c.value.to_string(),
            }));
        }
    });
    menu
}

fn small(label: &str, id: String) -> gtk::Button {
    let b = button(label, &id);
    b.add_css_class("flat");
    b
}

impl RowCells {
    fn build(table: &gtk::Grid, shape: &Shape, i: usize, send: &ActionFn) -> RowCells {
        let table_edit = |action: BuildAction| BuildDiskAction::Table(action);
        let top = 1 + 2 * i as i32;
        let number = cell(1.0);
        number.set_label(&(i + 1).to_string());
        table.attach(&number, 0, top, 1, 1);

        let size = entry(format!("{}.{i}", ids::ROW_SIZE), 8, Some(hints::ROW_SIZE));
        let s = send.clone();
        size.connect_changed(move |e| {
            let text = e.text().to_string();
            s(table_edit(BuildAction::SetRowSize { row: i, text }));
        });
        table.attach(&size, 1, top, 1, 1);

        let gpt = shape.kind == TableKind::Gpt;
        let chars = if gpt { 20 } else { 10 };
        let hint = match shape.fixed {
            true => hints::ROW_FIXED_TYPE,
            false => shape.kind.field_hint(),
        };
        let kind = entry(format!("{}.{i}", ids::ROW_TYPE), chars, Some(hint));
        kind.set_max_width_chars(if gpt { 36 } else { 10 });
        kind.set_sensitive(!shape.fixed);
        let s = send.clone();
        kind.connect_changed(move |e| {
            let text = e.text().to_string();
            s(table_edit(BuildAction::SetRowType { row: i, text }));
        });
        let note = gtk::Label::builder()
            .xalign(0.0)
            .ellipsize(gtk::pango::EllipsizeMode::End)
            .max_width_chars(24)
            .build();
        note.add_css_class("dim-label");
        let type_box = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        type_box.append(&kind);
        if !shape.fixed && !type_choices(shape.kind).is_empty() {
            type_box.append(&pick_menu(shape.kind, i, send));
        }
        type_box.append(&note);
        table.attach(&type_box, 2, top, 1, 1);

        let name = entry(format!("{}.{i}", ids::ROW_NAME), 12, None);
        name.set_sensitive(!shape.fixed);
        let s = send.clone();
        name.connect_changed(move |e| {
            let text = e.text().to_string();
            s(table_edit(BuildAction::SetRowName { row: i, text }));
        });
        if shape.names {
            table.attach(&name, 3, top, 1, 1);
        }

        let boot = gtk::CheckButton::new();
        boot.set_widget_name(&format!("{}.{i}", ids::ROW_BOOT));
        boot.set_tooltip_text(Some(match shape.boot {
            Some(true) => hints::BOOT_MBR,
            _ => hints::BOOT_RDB,
        }));
        let s = send.clone();
        boot.connect_toggled(move |b| {
            let on = b.is_active();
            s(table_edit(BuildAction::SetRowBootable { row: i, on }));
        });
        if shape.boot.is_some() {
            table.attach(&boot, 4, top, 1, 1);
        }

        let start = cell(1.0);
        table.attach(&start, 5, top, 1, 1);

        let can_copy = shape.copies.get(i).copied().unwrap_or(false);
        let list = choices(can_copy);
        let labels: Vec<&str> = list.iter().map(|c| label_of(*c)).collect();
        let content = gtk::DropDown::from_strings(&labels);
        content.set_widget_name(&format!("{}.{i}", ids::ROW_CONTENT));
        if can_copy {
            content.set_tooltip_text(Some(hints::ROW_COPY));
        }
        let held = list.clone();
        let s = send.clone();
        content.connect_selected_notify(move |d| match held.get(d.selected() as usize).copied() {
            Some(Choice::Copy) => s(BuildDiskAction::SetRowCopy(i)),
            Some(Choice::Empty) => s(BuildDiskAction::SetRowFs { row: i, fs: None }),
            Some(Choice::Fs(k)) => s(BuildDiskAction::SetRowFs {
                row: i,
                fs: Some(k),
            }),
            None => {}
        });
        let image = small("Image...", format!("{}.{i}", ids::ROW_IMAGE));
        image.set_tooltip_text(Some(hints::ROW_IMAGE));
        let s = send.clone();
        image.connect_clicked(move |_| s(table_edit(BuildAction::PickRowSource(i))));
        let source = gtk::Label::builder()
            .xalign(0.0)
            .ellipsize(gtk::pango::EllipsizeMode::Middle)
            .hexpand(true)
            .build();
        let clear = small("Clear", format!("{}.{i}", ids::ROW_CLEAR));
        let s = send.clone();
        clear.connect_clicked(move |_| s(table_edit(BuildAction::ClearRowSource(i))));
        let up = small("Up", format!("{}.{i}", ids::ROW_UP));
        let s = send.clone();
        up.connect_clicked(move |_| {
            s(table_edit(BuildAction::MoveRow { row: i, delta: -1 }));
        });
        let down = small("Down", format!("{}.{i}", ids::ROW_DOWN));
        let s = send.clone();
        down.connect_clicked(move |_| {
            s(table_edit(BuildAction::MoveRow { row: i, delta: 1 }));
        });
        let remove = small("Remove", format!("{}.{i}", ids::ROW_REMOVE));
        let s = send.clone();
        remove.connect_clicked(move |_| s(table_edit(BuildAction::RemoveRow(i))));
        let lower = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        lower.set_margin_bottom(8);
        lower.append(&content);
        lower.append(&image);
        lower.append(&source);
        lower.append(&clear);
        lower.append(&up);
        lower.append(&down);
        lower.append(&remove);
        table.attach(&lower, 1, top + 1, 5, 1);
        RowCells {
            size,
            kind,
            note,
            name,
            boot,
            start,
            content,
            choices: list,
            image,
            source,
            clear,
            up,
            down,
            remove,
        }
    }

    fn show(&self, row: &BuildRowView, state: &RowState, s: &RowsShown, i: usize) {
        set_field(&self.size, &row.size_text);
        set_field(&self.kind, &row.type_text);
        set_text(
            &self.note,
            type_note(s.view.kind, &row.type_text).unwrap_or(""),
        );
        set_field(&self.name, &row.name);
        if self.boot.is_active() != row.bootable {
            self.boot.set_active(row.bootable);
        }
        match (row.start_lba, row.planned_size) {
            (Some(lba), Some(size)) => {
                set_text(&self.start, &lba.to_string());
                self.start.set_tooltip_text(Some(&format_size(size)));
            }
            _ => {
                set_text(&self.start, "-");
                self.start.set_tooltip_text(None);
            }
        }

        let choice = match (state.content, state.fs) {
            (RowContent::Copy, _) => Choice::Copy,
            (RowContent::Format, Some(k)) => Choice::Fs(k),
            _ => Choice::Empty,
        };
        let at = self.choices.iter().position(|c| *c == choice).unwrap_or(0) as u32;
        if self.content.selected() != at {
            self.content.set_selected(at);
        }
        self.image.set_sensitive(!s.probing);
        let source = row
            .source
            .as_ref()
            .filter(|_| state.content == RowContent::Image);
        self.clear.set_visible(source.is_some());
        let name = source
            .and_then(|p| p.file_name())
            .map(|n| n.to_string_lossy());
        let text = name.map(|n| format!("{n} ({})", format_size(row.source_size)));
        set_text(&self.source, text.as_deref().unwrap_or(""));
        let path = source.map(|p| p.display().to_string());
        self.source
            .set_tooltip_text(path.as_deref().map(plain).as_deref());

        let n = s.view.rows.len();
        self.up.set_sensitive(!s.view.fixed && i > 0);
        self.down.set_sensitive(!s.view.fixed && i + 1 < n);
        self.remove.set_sensitive(!s.view.fixed && n > 1);
    }
}

/// The handlers embedded, each a DosType and its load file, and the DosTypes without one.
type HandlerList = (Vec<(String, PathBuf)>, Vec<String>);

/// The RDB's handlers and the DosTypes still without one, rebuilt when they change.
struct Handlers {
    root: adw::PreferencesGroup,
    list: gtk::Box,
    shown: RefCell<HandlerList>,
    send: ActionFn,
}

impl Handlers {
    fn build(send: &ActionFn) -> Handlers {
        let root = group("Filesystem handlers in the RDB");
        let list = gtk::Box::new(gtk::Orientation::Vertical, 4);
        root.add(&list);
        Handlers {
            root,
            list,
            shown: RefCell::default(),
            send: send.clone(),
        }
    }

    fn show(&self, on: bool, handlers: &[(String, PathBuf)], missing: &[String]) {
        self.root.set_visible(on);
        let now = (handlers.to_vec(), missing.to_vec());
        if *self.shown.borrow() == now {
            return;
        }
        *self.shown.borrow_mut() = now;
        while let Some(child) = self.list.first_child() {
            self.list.remove(&child);
        }
        for (i, (dos_type, path)) in handlers.iter().enumerate() {
            let text = format!("{dos_type}: {}", path.display());
            let label = gtk::Label::builder()
                .label(plain(&text).as_ref())
                .xalign(0.0)
                .hexpand(true)
                .ellipsize(gtk::pango::EllipsizeMode::Middle)
                .build();
            let remove = small("Remove", format!("{}.{i}", ids::REMOVE_HANDLER));
            let s = self.send.clone();
            remove.connect_clicked(move |_| s(BuildDiskAction::RemoveHandler(i)));
            let row = gtk::Box::new(gtk::Orientation::Horizontal, 6);
            row.append(&label);
            row.append(&remove);
            self.list.append(&row);
        }
        for (i, dos_type) in missing.iter().enumerate() {
            let add = button(
                &format!("Add {dos_type} handler..."),
                &format!("{}.{i}", ids::ADD_HANDLER),
            );
            add.set_tooltip_text(Some(hints::HANDLER));
            add.set_halign(gtk::Align::Start);
            let s = self.send.clone();
            let dos_type = dos_type.clone();
            add.connect_clicked(move |_| s(BuildDiskAction::AddHandler(dos_type.clone())));
            self.list.append(&add);
        }
    }
}

/// The driver an APM disk gets: bundled, none, or the one a picked disk holds.
struct DriverRows {
    root: adw::PreferencesGroup,
    driver: adw::ComboRow,
}

impl DriverRows {
    fn build(send: &ActionFn) -> DriverRows {
        let root = group("Mac driver");
        let driver = combo("Driver", ids::MAC_DRIVER, &[]);
        driver.set_tooltip_text(Some(hints::MAC_DRIVER));
        let s = send.clone();
        driver.connect_selected_notify(move |r| match r.selected() {
            0 => s(BuildDiskAction::SetMacDriver(MacDriver::Bundled)),
            1 => s(BuildDiskAction::SetMacDriver(MacDriver::Off)),
            _ => {}
        });
        let pick = button("From a disk...", ids::MAC_DRIVER_DISK);
        pick.set_valign(gtk::Align::Center);
        let s = send.clone();
        pick.connect_clicked(move |_| s(BuildDiskAction::PickMacDriver));
        driver.add_suffix(&pick);
        root.add(&driver);
        DriverRows { root, driver }
    }

    fn show(&self, on: bool, current: &MacDriver) {
        self.root.set_visible(on);
        let mut labels = vec![MacDriver::Bundled.label(), MacDriver::Off.label()];
        let at = match current {
            MacDriver::Bundled => 0,
            MacDriver::Off => 1,
            MacDriver::FromDisk(_) => {
                labels.push(current.label());
                2
            }
        };
        set_labels(&self.driver, &labels);
        set_combo(&self.driver, at);
    }
}

pub struct RowsStep {
    root: gtk::Box,
    caption: gtk::Label,
    bar: PartitionBar,
    error: gtk::Label,
    grid: gtk::Grid,
    shape: RefCell<Option<Shape>>,
    rows: RefCell<Vec<RowCells>>,
    add: gtk::Button,
    slots: gtk::Label,
    probing: gtk::Label,
    handlers: Handlers,
    driver: DriverRows,
    send: ActionFn,
}

impl RowsStep {
    pub fn build(send: &ActionFn) -> RowsStep {
        let layout = group("Partitions");
        let caption = line();
        let bar = PartitionBar::build(|_| {});
        let error = line();
        error.add_css_class("error");
        let table = grid();
        let add = button("Add Partition", ids::ADD_ROW);
        add.set_halign(gtk::Align::Start);
        let s = send.clone();
        add.connect_clicked(move |_| s(BuildDiskAction::Table(BuildAction::AddRow)));
        let slots = gtk::Label::new(None);
        slots.add_css_class("dim-label");
        let probing = gtk::Label::new(Some("Measuring the source image..."));
        probing.add_css_class("dim-label");
        let foot = gtk::Box::new(gtk::Orientation::Horizontal, 8);
        foot.append(&add);
        foot.append(&slots);
        foot.append(&probing);
        layout.add(&caption);
        layout.add(bar.root());
        layout.add(&error);
        layout.add(&table);
        layout.add(&foot);

        let handlers = Handlers::build(send);
        let driver = DriverRows::build(send);
        let root = gtk::Box::new(gtk::Orientation::Vertical, 12);
        root.append(&layout);
        root.append(&handlers.root);
        root.append(&driver.root);
        RowsStep {
            root,
            caption,
            bar,
            error,
            grid: table,
            shape: RefCell::default(),
            rows: RefCell::default(),
            add,
            slots,
            probing,
            handlers,
            driver,
            send: send.clone(),
        }
    }

    pub fn root(&self) -> &gtk::Box {
        &self.root
    }

    fn build_rows(&self, rows: &mut Vec<RowCells>, shape: &Shape) {
        clear_grid(&self.grid);
        rows.clear();
        head(&self.grid, 0, "#", None);
        head(&self.grid, 1, "Size", None);
        head(&self.grid, 2, "Type", None);
        if shape.names {
            head(&self.grid, 3, "Name", None);
        }
        if shape.boot.is_some() {
            head(&self.grid, 4, "Boot", None);
        }
        head(&self.grid, 5, "Start LBA", None);
        for i in 0..shape.copies.len() {
            rows.push(RowCells::build(&self.grid, shape, i, &self.send));
        }
    }

    pub fn show(&self, s: RowsShown) {
        let v = &s.view;
        match &v.layout {
            Ok(bar) => {
                set_line(&self.caption, Some(&bar.caption));
                set_line(&self.error, None);
                self.bar.root().set_visible(true);
                self.bar.show(segments(&bar.segments), None);
            }
            Err(e) => {
                set_line(&self.caption, None);
                set_line(&self.error, Some(e));
                self.bar.root().set_visible(false);
            }
        }

        let shape = Shape {
            kind: v.kind,
            names: v.names,
            boot: v.boot,
            fixed: v.fixed,
            copies: s.rows.iter().map(|r| r.can_copy).collect(),
        };
        let mut rows = self.rows.borrow_mut();
        if self.shape.borrow().as_ref() != Some(&shape) {
            self.build_rows(&mut rows, &shape);
            *self.shape.borrow_mut() = Some(shape);
        }
        for (i, cells) in rows.iter().enumerate() {
            if let (Some(row), Some(state)) = (v.rows.get(i), s.rows.get(i)) {
                cells.show(row, state, &s, i);
            }
        }

        self.add.set_sensitive(v.can_add_row);
        let slots = v
            .slots
            .map(|(used, all)| format!("{used} of {all} slots used"));
        set_text(&self.slots, slots.as_deref().unwrap_or(""));
        self.probing.set_visible(s.probing);
        self.handlers
            .show(s.takes_handlers, &s.handlers, &s.missing);
        self.driver.show(s.takes_driver, &s.driver);
    }
}
