//! Build Disk's Partitions and Options steps, whose controls are built from the plan; the Win32
//! shell's `build_rows.rs` is the reference.
//!
//! A row is two lines of controls: its size, type (with a Pick... pull-down of the table's
//! catalog), name, bootable flag and start, then its content and moves. The Options step is one
//! group per formatted row (or the volume): its label and the filesystem's own options. Both build
//! their controls again only when the plan's shape changes, so typing in a field keeps its focus.

use std::cell::RefCell;
use std::path::PathBuf;

use rusty_backup::controller::build_disk::{
    cpm_presets, format_size, geometries, hints, ids, parse_size_field, BuildDiskAction,
    BuildDiskController, FormatTarget, FsChoice, FsKind, FsOption, MacDriver, OptionEdit,
    RowContent, TableKind, AFFS_VARIANTS, NTFS_VERSIONS, SECTOR_SIZES,
};
use rusty_backup::controller::commander::Tone;
use rusty_backup::controller::partition_edit::{type_choices, type_note, BuildAction};

use crate::build_disk::K;
use crate::form::Form;
use crate::layout::{self, Node, Size};
use crate::partition_bar::{self, Bar};

/// The handlers embedded, each a DosType and its load file, and the DosTypes without one.
type HandlerList = (Vec<(String, PathBuf)>, Vec<String>);

const HEADS: [&str; 7] = ["#", "Size", "Type", "", "Name", "", "Start"];
const WIDTHS: [i32; 7] = [30, 90, 90, 70, 140, 60, 180];

/// A row's content as its list offers it.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Choice {
    Copy,
    Empty,
    Fs(FsChoice),
    /// A source image is poured in; picked with Image..., not from the list.
    Image,
}

fn row_h() -> Size {
    Size::Fixed(layout::ROW)
}

fn fixed(k: K, w: i32) -> Node<K> {
    Node::leaf(k, Size::Fixed(w), row_h())
}

fn pop(k: K, w: i32) -> Node<K> {
    Node::leaf(k, Size::Fixed(w), Size::Fixed(layout::ROW + 2))
}

fn stretch(k: K) -> Node<K> {
    Node::leaf(k, Size::Stretch(1), row_h())
}

/// Set a line's text and tone; hidden when there is none.
pub fn line(f: &Form<K>, k: K, text: Option<&str>, tone: Option<Tone>) {
    f.set_text(k, text.unwrap_or(""));
    f.show(k, text.is_some());
    f.set_tone(k, tone);
}

/// What the rows were built for: a change here builds them again.
#[derive(Clone, PartialEq, Eq)]
struct Shape {
    kind: TableKind,
    names: bool,
    boot: bool,
    fixed: bool,
    copies: Vec<bool>,
}

fn is_row_key(k: &K) -> bool {
    matches!(
        k,
        K::RowNumber(_)
            | K::RowSize(_)
            | K::RowType(_)
            | K::RowPick(_)
            | K::RowNote(_)
            | K::RowName(_)
            | K::RowBoot(_)
            | K::RowStart(_)
            | K::RowContent(_)
            | K::RowImage(_)
            | K::RowSource(_)
            | K::RowClear(_)
            | K::RowUp(_)
            | K::RowDown(_)
            | K::RowRemove(_)
    )
}

pub struct Rows {
    choices: RefCell<Vec<Vec<Choice>>>,
    shape: RefCell<Option<Shape>>,
    handlers: RefCell<HandlerList>,
}

impl Rows {
    /// The step's fixed controls; the rows come with the plan.
    pub fn build(f: &Form<K>, bar: &Bar) -> Rows {
        f.label(K::RowsCaption, "");
        f.add(K::Bar, bar);
        f.wrap(K::RowsError, "");
        for (i, t) in HEADS.iter().enumerate() {
            f.label(K::RowHead(i), t);
        }
        f.button(K::AddRow, "Add Partition", ids::ADD_ROW);
        f.label(K::RowsStatus, "");
        f.label(K::HandlersTitle, "Filesystem handlers in the RDB:");
        f.label(K::DriverCaption, "Mac driver:");
        f.popup(K::Driver, &[], ids::MAC_DRIVER);
        f.button(K::DriverDisk, "From a disk...", ids::MAC_DRIVER_DISK);
        Rows {
            choices: RefCell::default(),
            shape: RefCell::default(),
            handlers: RefCell::default(),
        }
    }

    fn choices(ctrl: &BuildDiskController, i: usize) -> Vec<Choice> {
        let mut v = Vec::new();
        if ctrl.row_can_copy(i) {
            v.push(Choice::Copy);
        }
        v.push(Choice::Empty);
        v.extend(ctrl.filesystems().into_iter().map(Choice::Fs));
        v.push(Choice::Image);
        v
    }

    fn rebuild(&self, f: &Form<K>, ctrl: &BuildDiskController, kind: TableKind, n: usize) {
        f.remove(is_row_key);
        let picks = type_choices(kind);
        let mut all = Vec::new();
        for i in 0..n {
            let id = |base: &str| format!("{base}.{i}");
            let choices = Self::choices(ctrl, i);
            let names: Vec<String> = choices
                .iter()
                .map(|c| match c {
                    Choice::Copy => hints::ROW_COPY.to_string(),
                    Choice::Empty => "Empty".to_string(),
                    Choice::Fs(k) => k.label(),
                    Choice::Image => "Image file".to_string(),
                })
                .collect();
            f.label(K::RowNumber(i), &(i + 1).to_string());
            f.field(K::RowSize(i), &id(ids::ROW_SIZE), "Size", false);
            f.field(K::RowType(i), &id(ids::ROW_TYPE), "Type", false);
            f.pulldown(K::RowPick(i), &id(ids::ROW_PICK_TYPE));
            let items: Vec<(String, Option<K>, bool)> = picks
                .iter()
                .enumerate()
                .map(|(j, c)| {
                    let label = format!("{}  -  {}", c.value, c.label);
                    (label, Some(K::RowPickItem(i, j)), true)
                })
                .collect();
            f.set_pulldown(K::RowPick(i), "Pick...", &items);
            f.label(K::RowNote(i), "");
            f.field(K::RowName(i), &id(ids::ROW_NAME), "Name", false);
            f.check(K::RowBoot(i), "Boot", &id(ids::ROW_BOOT));
            f.label(K::RowStart(i), "");
            f.popup(K::RowContent(i), &names, &id(ids::ROW_CONTENT));
            f.button(K::RowImage(i), "Image...", &id(ids::ROW_IMAGE));
            f.label(K::RowSource(i), "");
            f.button(K::RowClear(i), "Clear", &id(ids::ROW_CLEAR));
            f.button(K::RowUp(i), "Up", &id(ids::ROW_UP));
            f.button(K::RowDown(i), "Down", &id(ids::ROW_DOWN));
            f.button(K::RowRemove(i), "Remove", &id(ids::ROW_REMOVE));
            all.push(choices);
        }
        *self.choices.borrow_mut() = all;
    }

    pub fn show(&self, f: &Form<K>, bar: &Bar, ctrl: &BuildDiskController, on: bool) {
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
            self.rebuild(f, ctrl, view.kind, n);
            *self.shape.borrow_mut() = Some(shape.clone());
        }
        match &view.layout {
            Ok(b) => {
                line(f, K::RowsCaption, on.then_some(b.caption.as_str()), None);
                line(f, K::RowsError, None, None);
                bar.show(b.layout_segments(), None);
            }
            Err(e) => {
                line(f, K::RowsCaption, None, None);
                line(f, K::RowsError, on.then_some(e.as_str()), Some(Tone::Error));
            }
        }
        f.show(K::Bar, on && view.layout.is_ok());
        let heads = [true, true, true, false, shape.names, shape.boot, true];
        for (h, shown) in heads.iter().enumerate() {
            f.show(K::RowHead(h), on && *shown);
        }
        let choices = self.choices.borrow();
        for (i, row) in view.rows.iter().enumerate() {
            for k in [
                K::RowNumber(i),
                K::RowSize(i),
                K::RowType(i),
                K::RowPick(i),
                K::RowNote(i),
                K::RowStart(i),
                K::RowContent(i),
                K::RowImage(i),
                K::RowSource(i),
                K::RowUp(i),
                K::RowDown(i),
                K::RowRemove(i),
            ] {
                f.show(k, on);
            }
            f.show(K::RowName(i), on && shape.names);
            f.show(K::RowBoot(i), on && shape.boot);
            f.set_text(K::RowSize(i), &row.size_text);
            f.set_text(K::RowType(i), &row.type_text);
            f.enable(K::RowType(i), !shape.fixed);
            f.enable(K::RowPick(i), !shape.fixed);
            f.set_text(
                K::RowNote(i),
                type_note(view.kind, &row.type_text).unwrap_or(""),
            );
            f.set_text(K::RowName(i), &row.name);
            f.enable(K::RowName(i), !shape.fixed);
            f.set_check(K::RowBoot(i), row.bootable);
            let start = match (row.start_lba, row.planned_size) {
                (Some(lba), Some(size)) => format!("{lba} ({})", format_size(size)),
                _ => "-".to_string(),
            };
            f.set_text(K::RowStart(i), &start);
            let choice = match (ctrl.row_content(i), ctrl.row_fs_choice(i)) {
                (RowContent::Copy, _) => Choice::Copy,
                (RowContent::Format, Some(k)) => Choice::Fs(k),
                (RowContent::Image, _) => Choice::Image,
                _ => Choice::Empty,
            };
            let list = choices.get(i).map(Vec::as_slice).unwrap_or(&[]);
            let at = list.iter().position(|c| *c == choice).unwrap_or(0);
            f.select(K::RowContent(i), at);
            f.enable(K::RowImage(i), !ctrl.probing());
            let source = row
                .source
                .as_ref()
                .filter(|_| ctrl.row_content(i) == RowContent::Image);
            f.show(K::RowClear(i), on && source.is_some());
            let text = source.map(|p| {
                let name = p.file_name().map(|n| n.to_string_lossy().into_owned());
                format!(
                    "{} ({})",
                    name.unwrap_or_default(),
                    format_size(row.source_size)
                )
            });
            f.set_text(K::RowSource(i), text.as_deref().unwrap_or(""));
            f.enable(K::RowUp(i), !shape.fixed && i > 0);
            f.enable(K::RowDown(i), !shape.fixed && i + 1 < n);
            f.enable(K::RowRemove(i), !shape.fixed && n > 1);
        }
        f.show(K::AddRow, on);
        f.enable(K::AddRow, view.can_add_row && !shape.fixed);
        let mut status: Vec<String> = view.errors.clone();
        status.extend(view.status.clone());
        status.extend(
            view.slots
                .map(|(used, all)| format!("{used} of {all} slots used")),
        );
        f.set_text(K::RowsStatus, &status.join("  -  "));
        f.show(K::RowsStatus, on);
        self.show_handlers(f, ctrl, on && ctrl.takes_handlers());
        let mac = on && ctrl.takes_mac_driver();
        for k in [K::DriverCaption, K::Driver, K::DriverDisk] {
            f.show(k, mac);
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
        f.set_items(K::Driver, &names);
        f.select(K::Driver, at);
    }

    fn show_handlers(&self, f: &Form<K>, ctrl: &BuildDiskController, on: bool) {
        f.show(K::HandlersTitle, on);
        let now = (ctrl.handlers().to_vec(), ctrl.handlers_missing());
        if *self.handlers.borrow() != now {
            f.remove(|k| {
                matches!(
                    k,
                    K::HandlerLine(_) | K::HandlerRemove(_) | K::HandlerAdd(_)
                )
            });
            for (i, (dos, path)) in now.0.iter().enumerate() {
                f.label(K::HandlerLine(i), &format!("{dos}: {}", path.display()));
                let id = format!("{}.{i}", ids::REMOVE_HANDLER);
                f.button(K::HandlerRemove(i), "Remove", &id);
            }
            for (i, dos) in now.1.iter().enumerate() {
                let id = format!("{}.{i}", ids::ADD_HANDLER);
                f.button(K::HandlerAdd(i), &format!("Add {dos} handler..."), &id);
            }
            *self.handlers.borrow_mut() = now;
        }
        let shown = self.handlers.borrow();
        for i in 0..shown.0.len() {
            f.show(K::HandlerLine(i), on);
            f.show(K::HandlerRemove(i), on);
        }
        for i in 0..shown.1.len() {
            f.show(K::HandlerAdd(i), on);
        }
    }

    pub fn nodes(&self, f: &Form<K>) -> Vec<Node<K>> {
        if !f.shown(K::AddRow) {
            return Vec::new();
        }
        let mut col = Vec::new();
        if f.shown(K::RowsCaption) {
            col.push(stretch(K::RowsCaption));
        }
        if f.shown(K::Bar) {
            col.push(Node::leaf(
                K::Bar,
                Size::Stretch(1),
                Size::Fixed(partition_bar::HEIGHT),
            ));
        }
        if f.shown(K::RowsError) {
            col.push(stretch(K::RowsError));
        }
        col.push(Node::row(
            (0..HEADS.len())
                .map(|h| match f.shown(K::RowHead(h)) {
                    true => fixed(K::RowHead(h), WIDTHS[h]),
                    false => Node::Space(Size::Fixed(WIDTHS[h])),
                })
                .collect(),
        ));
        let n = self.choices.borrow().len();
        for i in 0..n {
            let maybe = |k: K, w: i32| match f.shown(k) {
                true => fixed(k, w),
                false => Node::Space(Size::Fixed(w)),
            };
            col.push(Node::row(vec![
                fixed(K::RowNumber(i), WIDTHS[0]),
                fixed(K::RowSize(i), WIDTHS[1]),
                fixed(K::RowType(i), WIDTHS[2]),
                pop(K::RowPick(i), WIDTHS[3]),
                maybe(K::RowName(i), WIDTHS[4]),
                maybe(K::RowBoot(i), WIDTHS[5]),
                fixed(K::RowStart(i), WIDTHS[6]),
                stretch(K::RowNote(i)),
            ]));
            let mut second = vec![
                Node::Space(Size::Fixed(WIDTHS[0])),
                pop(K::RowContent(i), 260),
                Node::button(K::RowImage(i), 85),
                stretch(K::RowSource(i)),
            ];
            if f.shown(K::RowClear(i)) {
                second.push(Node::button(K::RowClear(i), 65));
            }
            second.extend([
                Node::button(K::RowUp(i), 55),
                Node::button(K::RowDown(i), 65),
                Node::button(K::RowRemove(i), 80),
            ]);
            col.push(Node::row(second));
        }
        col.push(Node::row(vec![
            Node::button(K::AddRow, 125),
            stretch(K::RowsStatus),
        ]));
        if f.shown(K::HandlersTitle) {
            col.push(stretch(K::HandlersTitle));
            let shown = self.handlers.borrow();
            for i in 0..shown.0.len() {
                col.push(Node::row(vec![
                    stretch(K::HandlerLine(i)),
                    Node::button(K::HandlerRemove(i), 85),
                ]));
            }
            for i in 0..shown.1.len() {
                col.push(Node::row(vec![
                    Node::button(K::HandlerAdd(i), 220),
                    Node::Space(Size::Stretch(1)),
                ]));
            }
        }
        if f.shown(K::Driver) {
            col.push(Node::row(vec![
                fixed(K::DriverCaption, 130),
                pop(K::Driver, 260),
                Node::button(K::DriverDisk, 125),
                Node::Space(Size::Stretch(1)),
            ]));
        }
        col
    }

    /// A row control's action, if `k` is one of the rows'.
    pub fn fired(&self, f: &Form<K>, ctrl: &BuildDiskController, k: K) -> Option<BuildDiskAction> {
        let table = |a: BuildAction| Some(BuildDiskAction::Table(a));
        match k {
            K::AddRow => table(BuildAction::AddRow),
            K::Driver => match f.picked(k)? {
                0 => Some(BuildDiskAction::SetMacDriver(MacDriver::Bundled)),
                1 => Some(BuildDiskAction::SetMacDriver(MacDriver::Off)),
                _ => None,
            },
            K::DriverDisk => Some(BuildDiskAction::PickMacDriver),
            K::HandlerRemove(i) => Some(BuildDiskAction::RemoveHandler(i)),
            K::HandlerAdd(i) => {
                let dos = self.handlers.borrow().1.get(i)?.clone();
                Some(BuildDiskAction::AddHandler(dos))
            }
            K::RowSize(row) => table(BuildAction::SetRowSize {
                row,
                text: f.text(k),
            }),
            K::RowType(row) => table(BuildAction::SetRowType {
                row,
                text: f.text(k),
            }),
            K::RowName(row) => table(BuildAction::SetRowName {
                row,
                text: f.text(k),
            }),
            K::RowBoot(row) => table(BuildAction::SetRowBootable {
                row,
                on: f.checked(k),
            }),
            K::RowPickItem(row, j) => {
                let picks = type_choices(ctrl.table_view().kind);
                let text = picks.get(j)?.value.to_string();
                table(BuildAction::SetRowType { row, text })
            }
            K::RowImage(i) => table(BuildAction::PickRowSource(i)),
            K::RowClear(i) => table(BuildAction::ClearRowSource(i)),
            K::RowUp(row) => table(BuildAction::MoveRow { row, delta: -1 }),
            K::RowDown(row) => table(BuildAction::MoveRow { row, delta: 1 }),
            K::RowRemove(i) => table(BuildAction::RemoveRow(i)),
            K::RowContent(i) => {
                let choice = *self.choices.borrow().get(i)?.get(f.picked(k)?)?;
                match choice {
                    Choice::Copy => Some(BuildDiskAction::SetRowCopy(i)),
                    Choice::Empty => Some(BuildDiskAction::SetRowFs { row: i, fs: None }),
                    Choice::Fs(fs) => Some(BuildDiskAction::SetRowFs {
                        row: i,
                        fs: Some(fs),
                    }),
                    Choice::Image => table(BuildAction::PickRowSource(i)),
                }
            }
            _ => None,
        }
    }
}

/// One option's control.
enum Field {
    Size(
        fn(&FormatTarget) -> Option<u64>,
        fn(Option<u64>) -> OptionEdit,
    ),
    Check(fn(&FormatTarget) -> bool, fn(bool) -> OptionEdit),
    /// A list, with each item's edit.
    Pick(Vec<OptionEdit>),
}

/// Build option `opt`'s caption and control as `(n, k)`; the field it is, or None for none.
fn build_field(f: &Form<K>, t: &FormatTarget, opt: FsOption, n: usize, k: usize) -> Option<Field> {
    let id = format!("{}.{n}.{k}", ids::OPTION);
    let size = |title: &str, read, make| {
        f.label(K::OptCaption(n, k), &format!("{title}:"));
        f.field(K::Opt(n, k), &id, title, false);
        Some(Field::Size(read, make))
    };
    let check = |title: &str, read, make| {
        f.label(K::OptCaption(n, k), "");
        f.check(K::Opt(n, k), title, &id);
        Some(Field::Check(read, make))
    };
    let pick = |title: &str, items: Vec<(String, OptionEdit)>| {
        f.label(K::OptCaption(n, k), &format!("{title}:"));
        let names: Vec<String> = items.iter().map(|(n, _)| n.clone()).collect();
        f.popup(K::Opt(n, k), &names, &id);
        Some(Field::Pick(items.into_iter().map(|i| i.1).collect()))
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
        FsOption::NtfsVersion => pick(
            "NTFS version",
            NTFS_VERSIONS
                .iter()
                .map(|(v, n)| (n.to_string(), OptionEdit::NtfsVersion(*v)))
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
        OptionEdit::NtfsVersion(v) => o.ntfs_version.unwrap_or_default() == *v,
        _ => false,
    });
    at.unwrap_or(0)
}

fn is_option_key(k: &K) -> bool {
    matches!(
        k,
        K::OptTitle(_)
            | K::OptError(_)
            | K::OptLabelCaption(_)
            | K::OptLabel(_)
            | K::OptCaption(..)
            | K::Opt(..)
    )
}

#[derive(Default)]
pub struct Options {
    /// Each target's fields, as built.
    fields: RefCell<Vec<Vec<(usize, Field)>>>,
    /// What the targets were built for: each one's row and filesystem.
    built: RefCell<Vec<(Option<usize>, FsKind)>>,
}

impl Options {
    fn rebuild(&self, f: &Form<K>, targets: &[FormatTarget]) {
        f.remove(is_option_key);
        let mut all = Vec::new();
        for (n, t) in targets.iter().enumerate() {
            f.label(K::OptTitle(n), &t.title);
            f.wrap(K::OptError(n), "");
            f.label(K::OptLabelCaption(n), "Label:");
            let id = format!("{}.{n}", ids::LABEL);
            f.field(K::OptLabel(n), &id, "Label", false);
            let fields = t
                .fields()
                .into_iter()
                .enumerate()
                .filter_map(|(k, opt)| build_field(f, t, opt, n, k).map(|x| (k, x)))
                .collect();
            all.push(fields);
        }
        *self.fields.borrow_mut() = all;
    }

    pub fn show(&self, f: &Form<K>, ctrl: &BuildDiskController, on: bool) {
        let targets = ctrl.format_targets();
        let key: Vec<(Option<usize>, FsKind)> = targets.iter().map(|t| (t.row, t.fs)).collect();
        if *self.built.borrow() != key {
            self.rebuild(f, &targets);
            *self.built.borrow_mut() = key;
        }
        for (n, (fields, t)) in self.fields.borrow().iter().zip(&targets).enumerate() {
            f.show(K::OptTitle(n), on);
            line(
                f,
                K::OptError(n),
                t.error.as_deref().filter(|_| on),
                Some(Tone::Error),
            );
            let labelled = on && t.caps.label_max > 0;
            f.show(K::OptLabelCaption(n), labelled);
            f.show(K::OptLabel(n), labelled);
            f.set_text(K::OptLabel(n), &t.opts.label);
            for (k, field) in fields {
                let (cap, ctl) = (K::OptCaption(n, *k), K::Opt(n, *k));
                f.show(cap, on);
                f.show(ctl, on);
                match field {
                    Field::Size(read, _) => {
                        // "4K" stays as typed while it means the controller's value.
                        let typed = parse_size_field(&f.text(ctl)).ok();
                        if typed != Some(read(t)) {
                            f.set_text(ctl, &read(t).map(|v| v.to_string()).unwrap_or_default());
                        }
                    }
                    Field::Check(read, _) => f.set_check(ctl, read(t)),
                    Field::Pick(edits) => f.select(ctl, picked_index(t, edits)),
                }
            }
        }
    }

    pub fn nodes(&self, f: &Form<K>) -> Vec<Node<K>> {
        let mut col = Vec::new();
        for (n, fields) in self.fields.borrow().iter().enumerate() {
            if !f.shown(K::OptTitle(n)) {
                continue;
            }
            col.push(stretch(K::OptTitle(n)));
            if f.shown(K::OptError(n)) {
                col.push(stretch(K::OptError(n)));
            }
            if f.shown(K::OptLabel(n)) {
                col.push(Node::row(vec![
                    Node::Space(Size::Fixed(20)),
                    fixed(K::OptLabelCaption(n), 140),
                    fixed(K::OptLabel(n), 200),
                    Node::Space(Size::Stretch(1)),
                ]));
            }
            for (k, field) in fields {
                let ctl = K::Opt(n, *k);
                let control = match field {
                    Field::Size(..) => fixed(ctl, 120),
                    Field::Check(..) => fixed(ctl, 300),
                    Field::Pick(_) => pop(ctl, 300),
                };
                col.push(Node::row(vec![
                    Node::Space(Size::Fixed(20)),
                    fixed(K::OptCaption(n, *k), 140),
                    control,
                    Node::Space(Size::Stretch(1)),
                ]));
            }
        }
        col
    }

    /// An option control's action, if `k` is one of the options'.
    pub fn fired(&self, f: &Form<K>, ctrl: &BuildDiskController, k: K) -> Option<BuildDiskAction> {
        let n = match k {
            K::OptLabel(n) | K::Opt(n, _) => n,
            _ => return None,
        };
        let t = ctrl.format_targets().into_iter().nth(n)?;
        let edit = match k {
            K::Opt(_, j) => {
                let fields = self.fields.borrow();
                let (_, field) = fields.get(n)?.iter().find(|(x, _)| *x == j)?;
                match field {
                    Field::Size(_, make) => make(parse_size_field(&f.text(k)).ok()?),
                    Field::Check(_, make) => make(f.checked(k)),
                    Field::Pick(edits) => edits.get(f.picked(k)?)?.clone(),
                }
            }
            _ => OptionEdit::Label(f.text(k)),
        };
        let opts = t.edited(edit);
        Some(match t.row {
            Some(row) => BuildDiskAction::SetRowOptions { row, opts },
            None => BuildDiskAction::SetVolumeOptions(opts),
        })
    }
}
