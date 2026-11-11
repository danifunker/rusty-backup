//! Build Disk's Partitions and Options steps, whose controls are built from the plan.
//!
//! A row is two lines of controls: its size, type, name, bootable flag and start, then its
//! content and moves. The Options step is one group per formatted row (or the volume): its label
//! and the filesystem's own options. Both rebuild their controls only when the plan's shape
//! changes (rows, table kind, columns, filesystems), so typing in a field keeps its focus.

use std::cell::RefCell;
use std::path::PathBuf;

use rusty_backup::controller::build_disk::{
    cpm_presets, format_size, geometries, hints, ids, parse_size_field, BuildDiskAction,
    BuildDiskController, FormatTarget, FsKind, FsOption, MacDriver, OptionEdit, RowContent,
    TableKind, AFFS_VARIANTS, SECTOR_SIZES,
};
use rusty_backup::controller::partition_edit::{type_choices, type_note, BuildAction};
use windows::Win32::Foundation::{HWND, RECT};
use windows::Win32::UI::WindowsAndMessaging::{
    DestroyWindow, GetAncestor, GetWindowRect, BN_CLICKED, CBN_SELCHANGE, EN_CHANGE, GA_ROOT,
};

use crate::build_disk::{checked, set_check, set_field, set_line};
use crate::combo::Combo;
use crate::layout::{self, Node, Size};
use crate::partition_bar;
use crate::ui;

/// The handlers embedded, each a DosType and its load file, and the DosTypes without one.
type HandlerList = (Vec<(String, PathBuf)>, Vec<String>);

/// A row's content as its list offers it.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Choice {
    Copy,
    Empty,
    Fs(FsKind),
    /// A source image is poured in; picked with Image..., not from the list.
    Image,
}

fn destroy(hs: &[HWND]) {
    for h in hs {
        // SAFETY: our own controls, built again for a new shape.
        let _ = unsafe { DestroyWindow(*h) };
    }
}

fn row_h() -> Size {
    Size::Fixed(layout::ROW)
}

fn fixed(h: HWND, w: i32) -> Node<HWND> {
    Node::leaf(h, Size::Fixed(w), row_h())
}

fn stretch(h: HWND) -> Node<HWND> {
    Node::leaf(h, Size::Stretch(1), row_h())
}

/// One partition's controls.
struct RowCells {
    number: HWND,
    size: HWND,
    kind: HWND,
    pick: HWND,
    note: HWND,
    name: HWND,
    boot: HWND,
    start: HWND,
    content: Combo,
    choices: Vec<Choice>,
    image: HWND,
    source: HWND,
    clear: HWND,
    up: HWND,
    down: HWND,
    remove: HWND,
}

impl RowCells {
    fn build(page: HWND, i: usize, choices: Vec<Choice>) -> RowCells {
        let id = |base: &str| format!("{base}.{i}");
        let names: Vec<String> = choices
            .iter()
            .map(|c| match c {
                Choice::Copy => hints::ROW_COPY.to_string(),
                Choice::Empty => "Empty".to_string(),
                Choice::Fs(k) => k.display_name().to_string(),
                Choice::Image => "Image file".to_string(),
            })
            .collect();
        let content = Combo::build(page, &id(ids::ROW_CONTENT), &[]);
        content.set_items(&names);
        RowCells {
            number: ui::cell_label(page, &(i + 1).to_string()),
            size: ui::edit(page, &id(ids::ROW_SIZE)),
            kind: ui::edit(page, &id(ids::ROW_TYPE)),
            pick: ui::button(page, "Pick...", &id(ids::ROW_PICK_TYPE)),
            note: ui::cell_label(page, ""),
            name: ui::edit(page, &id(ids::ROW_NAME)),
            boot: ui::checkbox(page, "Boot", &id(ids::ROW_BOOT)),
            start: ui::cell_label(page, ""),
            content,
            choices,
            image: ui::button(page, "Image...", &id(ids::ROW_IMAGE)),
            source: ui::path_label(page, ""),
            clear: ui::button(page, "Clear", &id(ids::ROW_CLEAR)),
            up: ui::button(page, "Up", &id(ids::ROW_UP)),
            down: ui::button(page, "Down", &id(ids::ROW_DOWN)),
            remove: ui::button(page, "Remove", &id(ids::ROW_REMOVE)),
        }
    }

    fn all(&self) -> [HWND; 15] {
        [
            self.number,
            self.size,
            self.kind,
            self.pick,
            self.note,
            self.name,
            self.boot,
            self.start,
            self.content.h,
            self.image,
            self.source,
            self.clear,
            self.up,
            self.down,
            self.remove,
        ]
    }
}

/// What the rows were built for: a change here rebuilds them.
#[derive(Clone, PartialEq, Eq)]
struct Shape {
    kind: TableKind,
    names: bool,
    boot: bool,
    fixed: bool,
    copies: Vec<bool>,
}

pub struct RowsStep {
    page: HWND,
    caption: HWND,
    bar: HWND,
    error: HWND,
    heads: [HWND; 7],
    rows: RefCell<Vec<RowCells>>,
    shape: RefCell<Option<Shape>>,
    add: HWND,
    status: HWND,
    handlers_title: HWND,
    handlers: RefCell<Vec<(HWND, Option<HWND>)>>,
    handlers_shown: RefCell<HandlerList>,
    driver_caption: HWND,
    driver: Combo,
    driver_disk: HWND,
}

impl RowsStep {
    pub fn build(page: HWND) -> RowsStep {
        let bar = partition_bar::build(page, Box::new(|_| {}));
        partition_bar::set_options(bar, 1.0, true);
        RowsStep {
            page,
            caption: ui::cell_label(page, ""),
            bar,
            error: ui::label(page, ""),
            heads: ["#", "Size", "Type", "", "Name", "", "Start"].map(|t| ui::cell_label(page, t)),
            rows: RefCell::default(),
            shape: RefCell::default(),
            add: ui::button(page, "Add Partition", ids::ADD_ROW),
            status: ui::cell_label(page, ""),
            handlers_title: ui::cell_label(page, "Filesystem handlers in the RDB:"),
            handlers: RefCell::default(),
            handlers_shown: RefCell::default(),
            driver_caption: ui::cell_label(page, "Mac driver:"),
            driver: Combo::build(page, ids::MAC_DRIVER, &[]),
            driver_disk: ui::button(page, "From a disk...", ids::MAC_DRIVER_DISK),
        }
    }

    fn choices(ctrl: &BuildDiskController, i: usize) -> Vec<Choice> {
        let mut v = Vec::new();
        if ctrl.row_can_copy(i) {
            v.push(Choice::Copy);
        }
        v.push(Choice::Empty);
        v.extend(ctrl.filesystems().iter().map(|k| Choice::Fs(*k)));
        v.push(Choice::Image);
        v
    }

    pub fn show(&self, ctrl: &BuildDiskController, on: bool) {
        let view = ctrl.table_view();
        let n = view.rows.len();
        let shape = Shape {
            kind: view.kind,
            names: view.names,
            boot: view.boot.is_some(),
            fixed: view.fixed,
            copies: (0..n).map(|i| ctrl.row_can_copy(i)).collect(),
        };
        if self.shape.borrow().as_ref() != Some(&shape) {
            for r in self.rows.borrow_mut().drain(..) {
                destroy(&r.all());
            }
            let built = (0..n)
                .map(|i| RowCells::build(self.page, i, Self::choices(ctrl, i)))
                .collect();
            *self.rows.borrow_mut() = built;
            *self.shape.borrow_mut() = Some(shape.clone());
        }
        match &view.layout {
            Ok(bar) => {
                set_line(self.caption, on.then_some(bar.caption.as_str()), None);
                set_line(self.error, None, None);
                partition_bar::show(self.bar, bar.layout_segments(), None);
            }
            Err(e) => {
                set_line(self.caption, None, None);
                let tone = rusty_backup::controller::commander::Tone::Error;
                set_line(self.error, on.then_some(e.as_str()), Some(tone));
            }
        }
        ui::set_visible(self.bar, on && view.layout.is_ok());
        for (h, shown) in
            self.heads
                .iter()
                .zip([true, true, true, false, shape.names, shape.boot, true])
        {
            ui::set_visible(*h, on && shown);
        }
        for (i, (cells, row)) in self.rows.borrow().iter().zip(&view.rows).enumerate() {
            for h in cells.all() {
                ui::set_visible(h, on);
            }
            ui::set_visible(cells.name, on && shape.names);
            ui::set_visible(cells.boot, on && shape.boot);
            set_field(cells.size, &row.size_text);
            set_field(cells.kind, &row.type_text);
            ui::set_enabled(cells.kind, !shape.fixed);
            ui::set_enabled(cells.pick, !shape.fixed);
            ui::set_text(
                cells.note,
                type_note(view.kind, &row.type_text).unwrap_or(""),
            );
            set_field(cells.name, &row.name);
            ui::set_enabled(cells.name, !shape.fixed);
            set_check(cells.boot, row.bootable);
            let start = match (row.start_lba, row.planned_size) {
                (Some(lba), Some(size)) => format!("{lba} ({})", format_size(size)),
                _ => "-".to_string(),
            };
            ui::set_text(cells.start, &start);
            let choice = match (ctrl.row_content(i), ctrl.row_fs(i)) {
                (RowContent::Copy, _) => Choice::Copy,
                (RowContent::Format, Some(k)) => Choice::Fs(k),
                (RowContent::Image, _) => Choice::Image,
                _ => Choice::Empty,
            };
            let at = cells.choices.iter().position(|c| *c == choice).unwrap_or(0);
            cells.content.select(at);
            ui::set_enabled(cells.image, !ctrl.probing());
            let source = row
                .source
                .as_ref()
                .filter(|_| ctrl.row_content(i) == RowContent::Image);
            ui::set_visible(cells.clear, on && source.is_some());
            let text = source.map(|p| {
                let name = p.file_name().map(|n| n.to_string_lossy().into_owned());
                format!(
                    "{} ({})",
                    name.unwrap_or_default(),
                    format_size(row.source_size)
                )
            });
            ui::set_text(cells.source, text.as_deref().unwrap_or(""));
            ui::set_enabled(cells.up, !shape.fixed && i > 0);
            ui::set_enabled(cells.down, !shape.fixed && i + 1 < n);
            ui::set_enabled(cells.remove, !shape.fixed && n > 1);
        }
        ui::set_visible(self.add, on);
        ui::set_enabled(self.add, view.can_add_row && !shape.fixed);
        let mut status: Vec<String> = view.errors.clone();
        status.extend(view.status.clone());
        status.extend(
            view.slots
                .map(|(used, all)| format!("{used} of {all} slots used")),
        );
        ui::set_text(self.status, &status.join("  -  "));
        ui::set_visible(self.status, on);
        self.show_handlers(ctrl, on && ctrl.takes_handlers());
        let mac = on && ctrl.takes_mac_driver();
        for h in [self.driver_caption, self.driver.h, self.driver_disk] {
            ui::set_visible(h, mac);
        }
        let current = ctrl.mac_driver();
        let mut names = vec![
            MacDriver::Bundled.label().to_string(),
            MacDriver::Off.label().to_string(),
        ];
        let at = match current {
            MacDriver::Bundled => 0,
            MacDriver::Off => 1,
            MacDriver::FromDisk(_) => {
                names.push(current.label().to_string());
                2
            }
        };
        self.driver.set_items(&names);
        self.driver.select(at);
    }

    fn show_handlers(&self, ctrl: &BuildDiskController, on: bool) {
        ui::set_visible(self.handlers_title, on);
        let now = (ctrl.handlers().to_vec(), ctrl.handlers_missing());
        if *self.handlers_shown.borrow() != now {
            for (a, b) in self.handlers.borrow_mut().drain(..) {
                destroy(&[a]);
                destroy(&b.into_iter().collect::<Vec<_>>());
            }
            let mut v = Vec::new();
            for (i, (dos, path)) in now.0.iter().enumerate() {
                let line = ui::path_label(self.page, &format!("{dos}: {}", path.display()));
                let remove =
                    ui::button(self.page, "Remove", &format!("{}.{i}", ids::REMOVE_HANDLER));
                v.push((line, Some(remove)));
            }
            for (i, dos) in now.1.iter().enumerate() {
                let add = ui::button(
                    self.page,
                    &format!("Add {dos} handler..."),
                    &format!("{}.{i}", ids::ADD_HANDLER),
                );
                v.push((add, None));
            }
            *self.handlers.borrow_mut() = v;
            *self.handlers_shown.borrow_mut() = now;
        }
        for (a, b) in self.handlers.borrow().iter() {
            ui::set_visible(*a, on);
            if let Some(b) = b {
                ui::set_visible(*b, on);
            }
        }
    }

    pub fn shape(&self) -> Vec<usize> {
        let mut v = vec![
            self.rows.borrow().len(),
            usize::from(ui::shown(self.add)),
            usize::from(ui::shown(self.bar)),
            usize::from(ui::shown(self.error)),
            usize::from(ui::shown(self.driver.h)),
            self.handlers.borrow().len(),
            usize::from(ui::shown(self.handlers_title)),
        ];
        v.extend(
            self.rows
                .borrow()
                .iter()
                .map(|r| usize::from(ui::shown(r.clear))),
        );
        v.extend(
            self.rows
                .borrow()
                .iter()
                .map(|r| usize::from(ui::shown(r.name))),
        );
        v
    }

    pub fn nodes(&self) -> Vec<Node<HWND>> {
        if !ui::shown(self.add) {
            return Vec::new();
        }
        let mut col = Vec::new();
        if ui::shown(self.caption) {
            col.push(stretch(self.caption));
        }
        if ui::shown(self.bar) {
            col.push(Node::leaf(
                self.bar,
                Size::Stretch(1),
                Size::Fixed(partition_bar::HEIGHT),
            ));
        }
        if ui::shown(self.error) {
            col.push(stretch(self.error));
        }
        let widths = [30, 90, 90, 60, 140, 60, 180];
        let shown_heads: Vec<Node<HWND>> = self
            .heads
            .iter()
            .zip(widths)
            .map(|(h, w)| fixed(*h, w))
            .collect();
        col.push(Node::row(shown_heads));
        for r in self.rows.borrow().iter() {
            let mut first = vec![
                fixed(r.number, widths[0]),
                fixed(r.size, widths[1]),
                fixed(r.kind, widths[2]),
                fixed(r.pick, widths[3]),
            ];
            first.push(if ui::shown(r.name) {
                fixed(r.name, widths[4])
            } else {
                Node::Space(Size::Fixed(widths[4]))
            });
            first.push(if ui::shown(r.boot) {
                fixed(r.boot, widths[5])
            } else {
                Node::Space(Size::Fixed(widths[5]))
            });
            first.push(fixed(r.start, widths[6]));
            first.push(stretch(r.note));
            col.push(Node::row(first));
            let mut second = vec![
                Node::Space(Size::Fixed(widths[0])),
                fixed(r.content.h, 260),
                Node::button(r.image, 80),
                stretch(r.source),
            ];
            if ui::shown(r.clear) {
                second.push(Node::button(r.clear, 60));
            }
            second.extend([
                Node::button(r.up, 50),
                Node::button(r.down, 60),
                Node::button(r.remove, 75),
            ]);
            col.push(Node::row(second));
        }
        col.push(Node::row(vec![
            Node::button(self.add, 120),
            stretch(self.status),
        ]));
        if ui::shown(self.handlers_title) {
            col.push(stretch(self.handlers_title));
            for (a, b) in self.handlers.borrow().iter() {
                col.push(match b {
                    Some(b) => Node::row(vec![stretch(*a), Node::button(*b, 80)]),
                    None => Node::row(vec![Node::button(*a, 220), Node::Space(Size::Stretch(1))]),
                });
            }
        }
        if ui::shown(self.driver.h) {
            col.push(Node::row(vec![
                fixed(self.driver_caption, 130),
                fixed(self.driver.h, 260),
                Node::button(self.driver_disk, 120),
                Node::Space(Size::Stretch(1)),
            ]));
        }
        col
    }

    /// "Pick...": a type from the catalog, stamped into the row's field.
    fn pick_type(&self, kind: TableKind, row: usize, from: HWND) -> Option<BuildDiskAction> {
        let picks = type_choices(kind);
        let entries: Vec<(String, bool)> = picks
            .iter()
            .map(|c| (format!("{}  -  {}", c.value, c.label), true))
            .collect();
        let mut r = RECT::default();
        // SAFETY: plain queries of our own windows.
        let at = unsafe {
            let _ = GetWindowRect(from, &mut r);
            crate::menu::choose(GetAncestor(self.page, GA_ROOT), &entries, r.left, r.bottom)
        }?;
        let text = picks.get(at)?.value.to_string();
        Some(BuildDiskAction::Table(BuildAction::SetRowType {
            row,
            text,
        }))
    }

    pub fn command(
        &self,
        ctrl: &BuildDiskController,
        from: HWND,
        code: u32,
    ) -> Option<BuildDiskAction> {
        let table = |a: BuildAction| Some(BuildDiskAction::Table(a));
        if from == self.add && code == BN_CLICKED {
            return table(BuildAction::AddRow);
        }
        if from == self.driver.h && code == CBN_SELCHANGE {
            return match self.driver.picked()? {
                0 => Some(BuildDiskAction::SetMacDriver(MacDriver::Bundled)),
                1 => Some(BuildDiskAction::SetMacDriver(MacDriver::Off)),
                _ => None,
            };
        }
        if from == self.driver_disk && code == BN_CLICKED {
            return Some(BuildDiskAction::PickMacDriver);
        }
        let handlers = self.handlers.borrow();
        if let Some(i) = handlers
            .iter()
            .position(|(a, b)| *a == from || *b == Some(from))
        {
            if code != BN_CLICKED {
                return None;
            }
            let shown = self.handlers_shown.borrow();
            return match handlers[i].1 {
                Some(_) => Some(BuildDiskAction::RemoveHandler(i)),
                None => {
                    let dos = shown.1.get(i - shown.0.len())?.clone();
                    Some(BuildDiskAction::AddHandler(dos))
                }
            };
        }
        drop(handlers);
        let rows = self.rows.borrow();
        let (i, r) = rows
            .iter()
            .enumerate()
            .find(|(_, r)| r.all().contains(&from))?;
        let text = || ui::text(from);
        match code {
            EN_CHANGE if from == r.size => table(BuildAction::SetRowSize {
                row: i,
                text: text(),
            }),
            EN_CHANGE if from == r.kind => table(BuildAction::SetRowType {
                row: i,
                text: text(),
            }),
            EN_CHANGE if from == r.name => table(BuildAction::SetRowName {
                row: i,
                text: text(),
            }),
            BN_CLICKED if from == r.boot => table(BuildAction::SetRowBootable {
                row: i,
                on: checked(from),
            }),
            BN_CLICKED if from == r.pick => self.pick_type(ctrl.table_view().kind, i, from),
            BN_CLICKED if from == r.image => table(BuildAction::PickRowSource(i)),
            BN_CLICKED if from == r.clear => table(BuildAction::ClearRowSource(i)),
            BN_CLICKED if from == r.up => table(BuildAction::MoveRow { row: i, delta: -1 }),
            BN_CLICKED if from == r.down => table(BuildAction::MoveRow { row: i, delta: 1 }),
            BN_CLICKED if from == r.remove => table(BuildAction::RemoveRow(i)),
            CBN_SELCHANGE if from == r.content.h => match r.choices.get(r.content.picked()?)? {
                Choice::Copy => Some(BuildDiskAction::SetRowCopy(i)),
                Choice::Empty => Some(BuildDiskAction::SetRowFs { row: i, fs: None }),
                Choice::Fs(k) => Some(BuildDiskAction::SetRowFs {
                    row: i,
                    fs: Some(*k),
                }),
                Choice::Image => table(BuildAction::PickRowSource(i)),
            },
            _ => None,
        }
    }
}

/// One option's control.
enum Field {
    Size(
        HWND,
        fn(&FormatTarget) -> Option<u64>,
        fn(Option<u64>) -> OptionEdit,
    ),
    Check(HWND, fn(&FormatTarget) -> bool, fn(bool) -> OptionEdit),
    /// A list, with each item's edit.
    Pick(Combo, Vec<OptionEdit>),
}

impl Field {
    fn controls(&self) -> Vec<HWND> {
        match self {
            Field::Size(h, ..) | Field::Check(h, ..) => vec![*h],
            Field::Pick(c, _) => vec![c.h],
        }
    }
}

/// One formatted target's controls: its title, label and options.
struct TargetCells {
    row: Option<usize>,
    title: HWND,
    error: HWND,
    label_caption: HWND,
    label: HWND,
    fields: Vec<(HWND, Field)>,
}

fn build_field(page: HWND, t: &FormatTarget, opt: FsOption, id: &str) -> Option<(HWND, Field)> {
    let size = |title: &str, read, make| {
        let caption = ui::cell_label(page, &format!("{title}:"));
        Some((caption, Field::Size(ui::edit(page, id), read, make)))
    };
    let check = |title: &str, read, make| {
        Some((
            ui::cell_label(page, ""),
            Field::Check(ui::checkbox(page, title, id), read, make),
        ))
    };
    let pick = |title: &str, items: Vec<(String, OptionEdit)>| {
        let caption = ui::cell_label(page, &format!("{title}:"));
        let names: Vec<&str> = items.iter().map(|(n, _)| n.as_str()).collect();
        let combo = Combo::build(page, id, &names);
        Some((
            caption,
            Field::Pick(combo, items.into_iter().map(|i| i.1).collect()),
        ))
    };
    match opt {
        FsOption::BlockSize => size(
            "Block size",
            |t: &FormatTarget| t.opts.block_size.map(u64::from),
            OptionEdit::BlockSize,
        ),
        FsOption::Inodes => size(
            "Inodes",
            |t: &FormatTarget| t.opts.inodes,
            OptionEdit::Inodes,
        ),
        FsOption::BytesPerInode => size(
            "Bytes per inode",
            |t: &FormatTarget| t.opts.bytes_per_inode,
            OptionEdit::BytesPerInode,
        ),
        FsOption::ClusterSize => size(
            "Cluster size",
            |t: &FormatTarget| t.opts.cluster_bytes.map(u64::from),
            OptionEdit::ClusterSize,
        ),
        FsOption::BigEndian => check(
            "Big-endian",
            |t: &FormatTarget| t.opts.big_endian,
            OptionEdit::BigEndian,
        ),
        FsOption::Fat32 => check(
            "FAT32 whatever the size",
            |t: &FormatTarget| t.opts.fat32,
            OptionEdit::Fat32,
        ),
        FsOption::CaseSensitive => check(
            "Case-sensitive (HFSX)",
            |t: &FormatTarget| t.opts.case_sensitive,
            OptionEdit::CaseSensitive,
        ),
        FsOption::AffsVariant => pick(
            "Variant",
            AFFS_VARIANTS
                .iter()
                .map(|(b, n)| (n.to_string(), OptionEdit::AffsVariant(*b)))
                .collect(),
        ),
        FsOption::CpmPreset => pick(
            "Disk format",
            cpm_presets()
                .into_iter()
                .map(|p| (p.to_string(), OptionEdit::CpmPreset(p.to_string())))
                .collect(),
        ),
        FsOption::Geometry => pick(
            "Geometry",
            geometries(t.fs)
                .iter()
                .map(|(id, n)| (n.to_string(), OptionEdit::Geometry(id.to_string())))
                .collect(),
        ),
        FsOption::SectorSize => pick(
            "Sector size",
            SECTOR_SIZES
                .iter()
                .map(|s| (format!("{s} bytes"), OptionEdit::SectorSize(*s)))
                .collect(),
        ),
        FsOption::BtreeSizes | FsOption::MinCatalog => None,
    }
}

/// The edit a list item stands for, as the target holds it now: the item to select.
fn picked_index(t: &FormatTarget, edits: &[OptionEdit]) -> usize {
    let o = &t.opts;
    let at = edits.iter().position(|e| match e {
        OptionEdit::AffsVariant(v) => *v == o.affs_variant,
        OptionEdit::CpmPreset(p) => o.cpm_preset.as_deref() == Some(p.as_str()),
        OptionEdit::Geometry(g) => o.geometry.as_deref() == Some(g.as_str()),
        OptionEdit::SectorSize(s) => o.sector_size.unwrap_or(512) == *s,
        _ => false,
    });
    at.unwrap_or(0)
}

pub struct OptionsStep {
    page: HWND,
    targets: RefCell<Vec<TargetCells>>,
    /// What the targets were built for: each one's row and filesystem.
    built: RefCell<Vec<(Option<usize>, FsKind)>>,
}

impl OptionsStep {
    pub fn new(page: HWND) -> OptionsStep {
        OptionsStep {
            page,
            targets: RefCell::default(),
            built: RefCell::default(),
        }
    }

    fn rebuild(&self, targets: &[FormatTarget]) {
        for t in self.targets.borrow_mut().drain(..) {
            destroy(&[t.title, t.error, t.label_caption, t.label]);
            for (c, f) in &t.fields {
                destroy(&[*c]);
                destroy(&f.controls());
            }
        }
        let page = self.page;
        let built = targets
            .iter()
            .enumerate()
            .map(|(n, t)| {
                let id = |base: &str| format!("{base}.{n}");
                let fields = t
                    .fields()
                    .into_iter()
                    .enumerate()
                    .filter_map(|(k, opt)| {
                        build_field(page, t, opt, &format!("{}.{n}.{k}", ids::OPTION))
                    })
                    .collect();
                TargetCells {
                    row: t.row,
                    title: ui::cell_label(page, &t.title),
                    error: ui::label(page, ""),
                    label_caption: ui::cell_label(page, "Label:"),
                    label: ui::edit(page, &id(ids::LABEL)),
                    fields,
                }
            })
            .collect();
        *self.targets.borrow_mut() = built;
    }

    pub fn show(&self, ctrl: &BuildDiskController, on: bool) {
        let targets = ctrl.format_targets();
        let key: Vec<(Option<usize>, FsKind)> = targets.iter().map(|t| (t.row, t.fs)).collect();
        if *self.built.borrow() != key {
            self.rebuild(&targets);
            *self.built.borrow_mut() = key;
        }
        for (cells, t) in self.targets.borrow().iter().zip(&targets) {
            ui::set_visible(cells.title, on);
            let error = t.error.as_deref().filter(|_| on);
            let tone = rusty_backup::controller::commander::Tone::Error;
            set_line(cells.error, error, Some(tone));
            let labelled = on && t.caps.label_max > 0;
            ui::set_visible(cells.label_caption, labelled);
            ui::set_visible(cells.label, labelled);
            set_field(cells.label, &t.opts.label);
            for (caption, f) in &cells.fields {
                ui::set_visible(*caption, on);
                for h in f.controls() {
                    ui::set_visible(h, on);
                }
                match f {
                    Field::Size(h, read, _) => {
                        // "4K" stays as typed while it means the controller's value.
                        let typed = parse_size_field(&ui::text(*h)).ok();
                        if typed != Some(read(t)) {
                            set_field(*h, &read(t).map(|v| v.to_string()).unwrap_or_default());
                        }
                    }
                    Field::Check(h, read, _) => set_check(*h, read(t)),
                    Field::Pick(c, edits) => c.select(picked_index(t, edits)),
                }
            }
        }
    }

    pub fn shape(&self) -> Vec<usize> {
        let targets = self.targets.borrow();
        let mut v = vec![targets.len()];
        for t in targets.iter() {
            v.push(t.fields.len());
            v.push(usize::from(ui::shown(t.title)));
            v.push(usize::from(ui::shown(t.error)));
            v.push(usize::from(ui::shown(t.label)));
        }
        v
    }

    pub fn nodes(&self) -> Vec<Node<HWND>> {
        let mut col = Vec::new();
        for t in self.targets.borrow().iter() {
            if !ui::shown(t.title) {
                continue;
            }
            col.push(stretch(t.title));
            if ui::shown(t.error) {
                col.push(stretch(t.error));
            }
            if ui::shown(t.label) {
                col.push(Node::row(vec![
                    Node::Space(Size::Fixed(20)),
                    fixed(t.label_caption, 140),
                    fixed(t.label, 200),
                    Node::Space(Size::Stretch(1)),
                ]));
            }
            for (caption, f) in &t.fields {
                let control = match f {
                    Field::Size(h, ..) => fixed(*h, 120),
                    Field::Check(h, ..) => fixed(*h, 300),
                    Field::Pick(c, _) => fixed(c.h, 300),
                };
                col.push(Node::row(vec![
                    Node::Space(Size::Fixed(20)),
                    fixed(*caption, 140),
                    control,
                    Node::Space(Size::Stretch(1)),
                ]));
            }
        }
        col
    }

    pub fn command(
        &self,
        ctrl: &BuildDiskController,
        from: HWND,
        code: u32,
    ) -> Option<BuildDiskAction> {
        let targets = self.targets.borrow();
        let cells = targets.iter().find(|t| {
            t.label == from || t.fields.iter().any(|(_, f)| f.controls().contains(&from))
        })?;
        let t = ctrl
            .format_targets()
            .into_iter()
            .find(|t| t.row == cells.row)?;
        let edit = if from == cells.label {
            (code == EN_CHANGE).then(|| OptionEdit::Label(ui::text(from)))?
        } else {
            let (_, f) = cells
                .fields
                .iter()
                .find(|(_, f)| f.controls().contains(&from))?;
            match f {
                Field::Size(h, _, make) if code == EN_CHANGE => {
                    make(parse_size_field(&ui::text(*h)).ok()?)
                }
                Field::Check(h, _, make) if code == BN_CLICKED => make(checked(*h)),
                Field::Pick(c, edits) if code == CBN_SELCHANGE => edits.get(c.picked()?)?.clone(),
                _ => return None,
            }
        };
        let opts = t.edited(edit);
        Some(match t.row {
            Some(row) => BuildDiskAction::SetRowOptions { row, opts },
            None => BuildDiskAction::SetVolumeOptions(opts),
        })
    }
}
