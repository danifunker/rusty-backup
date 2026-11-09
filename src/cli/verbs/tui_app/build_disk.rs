//! The TUI Build Disk screen: the GUI's Build Disk tab over the same `BuildDiskController`.
//!
//! The wizard's steps, what blocks Next and the build are the controller's; this screen keeps a
//! row cursor and a text buffer, turns keys into `BuildDiskAction`s and answers the controller's
//! pickers with the shared `FilePicker` and the device list. See docs/build_disk_plan.md § 7.

use std::sync::Arc;

use crossterm::event::KeyCode;
use ratatui::layout::Rect;
use ratatui::style::Modifier;
use ratatui::text::{Line, Text};
use ratatui::widgets::{Paragraph, Wrap};
use ratatui::Frame;

use super::table_edit::bar_line;
use super::{hint, App, FilePicker, KeyHint, PickKind, PickResult};
use crate::controller::build_disk::{
    containers, cpm_presets, format_size, geometries, hints, labels, parse_size_field, purpose,
    BuildDiskAction, BuildDiskController, CdLayout, CdOption, CdSidecars, DiskKind, FormatOptions,
    FsKind, FsOption, MacDriver, NoNotify, OptionEdit, PresetKind, RowContent, Step, TableKind,
    AFFS_VARIANTS, CD_AUTO_SIZE, SECTOR_SIZES,
};
use crate::controller::partition_edit::BuildAction;
use crate::controller::{Controller as _, Effect};

pub(super) const HINTS_BUILD_DISK: &[KeyHint] = &[
    hint("Up/Down", "Move between rows"),
    hint("Left/Right, Space", "Change a choice or toggle"),
    hint("Enter", "Edit a field, pick a file or device"),
    hint("n / PgDn", "Next step (Create on Review)"),
    hint("b / PgUp", "Previous step"),
    hint("+ / -", "Add or remove a partition"),
    hint("[ / ]", "Move a partition up or down"),
    hint("a", "Build another (after a build)"),
    hint("Esc", "Cancel an edit, a picker, or a running build"),
];

/// One row of the current step.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Row {
    Kind,
    OutputKind,
    OutputImage,
    Container,
    OutputDevice,
    Table,
    /// Start from a blank table or a donor disk's layout.
    Donor,
    Size,
    Align,
    Heads,
    Sectors,
    VolumeFs,
    Preset,
    X68kScsi,
    X68kParts,
    SystemDisk,
    SgiLabel,
    CdLayout,
    CdName,
    CdAutoSize,
    CdFolder,
    CdOpt(CdOption),
    CdSidecars,
    PSize(usize),
    PType(usize),
    PName(usize),
    PContent(usize),
    PBoot(usize),
    AddPartition,
    /// An embedded RDB handler (Enter removes it).
    Handler(usize),
    /// Add a handler for the n-th DosType that has none.
    AddHandler(usize),
    /// The APM driver: Left / Right switch bundled and none, Enter picks a disk.
    MacDriver,
    Label(Option<usize>),
    Opt(Option<usize>, FsOption),
    Confirm,
}

pub(super) struct BuildDiskState {
    pub(super) ctl: BuildDiskController,
    pub(super) sel: usize,
    /// The text being typed into the selected row.
    pub(super) edit: Option<String>,
    /// A picker the controller asked for, with its purpose.
    pub(super) picker: Option<(&'static str, FilePicker)>,
    /// The device list is open for the Output row.
    pub(super) device_pick: bool,
    pub(super) device_sel: usize,
    pub(super) status: Option<String>,
    log: Vec<String>,
}

impl Default for BuildDiskState {
    fn default() -> Self {
        BuildDiskState {
            ctl: BuildDiskController::new(Arc::new(NoNotify)),
            sel: 0,
            edit: None,
            picker: None,
            device_pick: false,
            device_sel: 0,
            status: None,
            log: Vec::new(),
        }
    }
}

/// Every content choice of a partition row, in cycling order: empty, each filesystem, an image.
/// A row's content as the cycle steps through it.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Content {
    Copy,
    Empty,
    Fs(FsKind),
    Image,
}

/// Every content choice of a partition row, in cycling order.
fn contents(can_copy: bool) -> Vec<Content> {
    let mut v = Vec::new();
    if can_copy {
        v.push(Content::Copy);
    }
    v.push(Content::Empty);
    v.extend(FsKind::ALL.iter().map(|k| Content::Fs(*k)));
    v.push(Content::Image);
    v
}

fn cycle<T: PartialEq + Copy>(all: &[T], cur: T, forward: bool) -> T {
    let n = all.len();
    match all.iter().position(|x| *x == cur) {
        Some(i) if forward => all[(i + 1) % n],
        Some(i) => all[(i + n - 1) % n],
        None => all[0],
    }
}

impl BuildDiskState {
    pub(super) fn act(&mut self, action: BuildDiskAction) {
        let fx = self.ctl.apply(action);
        self.absorb(fx);
    }

    fn absorb(&mut self, fx: Vec<Effect>) {
        for e in fx {
            match e {
                Effect::PickFile(spec) => {
                    let kind = if spec.save {
                        PickKind::Save
                    } else {
                        PickKind::File
                    };
                    self.picker = Some((spec.purpose, FilePicker::new(kind, spec.title)));
                }
                Effect::Message(m) => self.status = Some(m),
                Effect::PickFolder(spec) => {
                    self.picker = Some((spec.purpose, FilePicker::new(PickKind::Dir, spec.title)));
                }
                Effect::Confirm(_)
                | Effect::AskPassword(_)
                | Effect::AskText(_)
                | Effect::Choose(_)
                | Effect::OpenUrl(_)
                | Effect::CopyToClipboard(_)
                | Effect::CloseScreen
                | Effect::Quit => {}
            }
        }
    }

    pub(super) fn tick(&mut self) {
        let fx = self.ctl.tick();
        self.absorb(fx);
        self.log
            .extend(self.ctl.take_log().into_iter().map(|(_, m)| m));
        let n = self.log.len();
        if n > 8 {
            self.log.drain(..n - 8);
        }
        let rows = self.rows();
        if self.sel >= rows.len() {
            self.sel = rows.len().saturating_sub(1);
        }
    }

    /// The rows of the current step, in drawing order.
    pub(super) fn rows(&self) -> Vec<Row> {
        let c = &self.ctl;
        match c.step() {
            Step::Kind => vec![Row::Kind],
            Step::Output => {
                let mut r = vec![Row::OutputKind];
                if c.output_is_device() {
                    r.push(Row::OutputDevice);
                } else {
                    r.extend([Row::OutputImage, Row::Container]);
                }
                r
            }
            Step::Disk => {
                let mut r = Vec::new();
                match c.kind() {
                    DiskKind::Cd => {
                        r.extend([Row::CdLayout, Row::CdName, Row::CdAutoSize]);
                        if !c.cd_auto_size() {
                            r.push(Row::Size);
                        }
                        r.push(Row::CdFolder);
                        r.extend(CdOption::ALL.iter().map(|o| Row::CdOpt(*o)));
                        r.push(Row::CdSidecars);
                    }
                    DiskKind::Preset => {
                        r.push(Row::Preset);
                        match c.preset().kind {
                            PresetKind::X68k => {
                                r.extend([Row::X68kScsi, Row::X68kParts, Row::SystemDisk])
                            }
                            PresetKind::SgiEfs => r.push(Row::SgiLabel),
                        }
                        if !c.output_is_device() {
                            r.push(Row::Size);
                        }
                    }
                    DiskKind::Volume => {
                        r.push(Row::VolumeFs);
                        if !c.output_is_device() && !c.volume_fs().caps().fixed_size {
                            r.push(Row::Size);
                        }
                    }
                    DiskKind::Partitioned => {
                        r.push(Row::Donor);
                        r.push(Row::Table);
                        if !c.output_is_device() {
                            r.push(Row::Size);
                        }
                        r.push(Row::Align);
                        if c.table_view().geometry.is_some() {
                            r.extend([Row::Heads, Row::Sectors]);
                        }
                    }
                }
                r
            }
            Step::Partitions => {
                let v = c.table_view();
                let mut r = Vec::new();
                for i in 0..v.rows.len() {
                    r.extend([Row::PSize(i), Row::PType(i)]);
                    if v.names {
                        r.push(Row::PName(i));
                    }
                    r.push(Row::PContent(i));
                    if v.boot.is_some() {
                        r.push(Row::PBoot(i));
                    }
                }
                if v.can_add_row {
                    r.push(Row::AddPartition);
                }
                if c.takes_handlers() {
                    r.extend((0..c.handlers().len()).map(Row::Handler));
                    r.extend((0..c.handlers_missing().len()).map(Row::AddHandler));
                }
                if c.takes_mac_driver() {
                    r.push(Row::MacDriver);
                }
                r
            }
            Step::Options => {
                let mut r = Vec::new();
                for t in c.format_targets() {
                    if t.caps.label_max > 0 {
                        r.push(Row::Label(t.row));
                    }
                    r.extend(t.fields().into_iter().map(|o| Row::Opt(t.row, o)));
                }
                r
            }
            Step::Review => {
                if c.confirm_expected().is_some() {
                    vec![Row::Confirm]
                } else {
                    Vec::new()
                }
            }
            Step::Build => Vec::new(),
        }
    }

    fn current(&self) -> Option<Row> {
        self.rows().get(self.sel).copied()
    }

    fn opts_of(&self, row: Option<usize>) -> Option<FormatOptions> {
        self.ctl
            .format_targets()
            .into_iter()
            .find(|t| t.row == row)
            .map(|t| t.opts)
    }

    fn set_opts(&mut self, row: Option<usize>, opts: FormatOptions) {
        match row {
            Some(row) => self.act(BuildDiskAction::SetRowOptions { row, opts }),
            None => self.act(BuildDiskAction::SetVolumeOptions(opts)),
        }
    }

    /// A row's value as its text field starts editing.
    fn text_of(&self, row: Row) -> Option<String> {
        let c = &self.ctl;
        let v = c.table_view();
        let size = |n: Option<u64>| n.map(|n| n.to_string()).unwrap_or_default();
        Some(match row {
            Row::OutputImage => c
                .image()
                .map(|p| p.display().to_string())
                .unwrap_or_default(),
            Row::Size => c.size_text().to_string(),
            Row::Align => v.align_text,
            Row::Heads => v.geometry?.0.to_string(),
            Row::Sectors => v.geometry?.1.to_string(),
            Row::PSize(i) => v.rows.get(i)?.size_text.clone(),
            Row::PType(i) => v.rows.get(i)?.type_text.clone(),
            Row::PName(i) => v.rows.get(i)?.name.clone(),
            Row::Label(r) => self.opts_of(r)?.label,
            Row::Opt(r, FsOption::BlockSize) => size(self.opts_of(r)?.block_size.map(u64::from)),
            Row::Opt(r, FsOption::Inodes) => size(self.opts_of(r)?.inodes),
            Row::Opt(r, FsOption::BytesPerInode) => size(self.opts_of(r)?.bytes_per_inode),
            Row::Opt(r, FsOption::ClusterSize) => {
                size(self.opts_of(r)?.cluster_bytes.map(u64::from))
            }
            Row::Confirm => c.confirm_text().to_string(),
            Row::X68kParts => c.preset().x68k_partitions.to_string(),
            Row::SgiLabel => c.preset().sgi_label.clone(),
            Row::CdName => c.cd().name.clone(),
            _ => return None,
        })
    }

    /// A finished text edit lands on its row.
    fn commit(&mut self, row: Row, text: String) {
        match row {
            Row::OutputImage => {
                let path = (!text.trim().is_empty()).then(|| std::path::PathBuf::from(text.trim()));
                self.act(BuildDiskAction::Picked {
                    purpose: purpose::OUTPUT_IMAGE,
                    path,
                });
            }
            Row::Size => self.act(BuildDiskAction::SetSize(text)),
            Row::Align => self.act(BuildDiskAction::Table(BuildAction::SetAlign(text))),
            Row::Heads => {
                if let Ok(h) = text.trim().parse() {
                    self.act(BuildDiskAction::Table(BuildAction::SetHeads(h)));
                }
            }
            Row::Sectors => {
                if let Ok(s) = text.trim().parse() {
                    self.act(BuildDiskAction::Table(BuildAction::SetSectorsPerTrack(s)));
                }
            }
            Row::PSize(row) => self.act(BuildDiskAction::Table(BuildAction::SetRowSize {
                row,
                text,
            })),
            Row::PType(row) => self.act(BuildDiskAction::Table(BuildAction::SetRowType {
                row,
                text,
            })),
            Row::PName(row) => self.act(BuildDiskAction::Table(BuildAction::SetRowName {
                row,
                text,
            })),
            Row::Label(r) => {
                if let Some(mut o) = self.opts_of(r) {
                    o.label = text;
                    self.set_opts(r, o);
                }
            }
            Row::Opt(r, opt) => {
                let Ok(v) = parse_size_field(text.as_str()) else {
                    self.status = Some(format!("Not a size: {text}"));
                    return;
                };
                let Some(mut o) = self.opts_of(r) else { return };
                let edit = match opt {
                    FsOption::BlockSize => OptionEdit::BlockSize(v),
                    FsOption::Inodes => OptionEdit::Inodes(v),
                    FsOption::BytesPerInode => OptionEdit::BytesPerInode(v),
                    FsOption::ClusterSize => OptionEdit::ClusterSize(v),
                    _ => return,
                };
                edit.apply(&mut o);
                self.set_opts(r, o);
            }
            Row::Confirm => self.act(BuildDiskAction::SetConfirmText(text)),
            Row::X68kParts => {
                if let Ok(n) = text.trim().parse() {
                    let mut p = self.ctl.preset().clone();
                    p.x68k_partitions = n;
                    self.act(BuildDiskAction::SetPreset(p));
                }
            }
            Row::SgiLabel => {
                let mut p = self.ctl.preset().clone();
                p.sgi_label = text;
                self.act(BuildDiskAction::SetPreset(p));
            }
            Row::CdName => {
                let mut cd = self.ctl.cd().clone();
                cd.name = text;
                self.act(BuildDiskAction::SetCd(cd));
            }
            _ => {}
        }
    }

    /// Left / Right / Space on a choice row. Returns `false` when the row has no choice.
    fn change(&mut self, row: Row, forward: bool) -> bool {
        let c = &self.ctl;
        match row {
            Row::Kind => {
                let k = cycle(&DiskKind::ALL, c.kind(), forward);
                self.act(BuildDiskAction::SetKind(k));
            }
            Row::OutputKind => {
                let on = !c.output_is_device();
                self.act(BuildDiskAction::SetOutputIsDevice(on));
            }
            Row::Container => {
                let k = cycle(containers().as_slice(), c.container(), forward);
                self.act(BuildDiskAction::SetContainer(k));
            }
            Row::Table => {
                let v = c.table_view();
                let k: TableKind = cycle(v.kinds, v.kind, forward);
                self.act(BuildDiskAction::Table(BuildAction::SetKind(k)));
            }
            Row::VolumeFs => {
                let k = cycle(&FsKind::ALL, c.volume_fs(), forward);
                self.act(BuildDiskAction::SetVolumeFs(k));
            }
            Row::Preset => {
                let mut p = c.preset().clone();
                p.kind = cycle(&PresetKind::ALL, p.kind, forward);
                self.act(BuildDiskAction::SetPreset(p));
            }
            Row::X68kScsi => {
                let mut p = c.preset().clone();
                p.x68k_scsi = !p.x68k_scsi;
                self.act(BuildDiskAction::SetPreset(p));
            }
            Row::CdLayout => {
                let mut cd = c.cd().clone();
                cd.layout = cycle(&CdLayout::ALL, cd.layout, forward);
                self.act(BuildDiskAction::SetCd(cd));
            }
            Row::CdAutoSize => {
                let text = if c.cd_auto_size() {
                    "600M"
                } else {
                    CD_AUTO_SIZE
                };
                self.act(BuildDiskAction::SetSize(text.to_string()));
            }
            Row::CdOpt(option) => {
                let row = c.cd_options().into_iter().find(|r| r.option == option);
                match row {
                    Some(r) if r.unavailable.is_none() => {
                        self.act(BuildDiskAction::SetCdOption(option, !r.on));
                    }
                    Some(r) => self.status = r.unavailable.map(str::to_string),
                    None => {}
                }
            }
            Row::CdSidecars => match c.cd_sidecars_unavailable() {
                None => {
                    let all = CdSidecars::choices(c.cd().layout);
                    let next = cycle(all, c.cd_sidecars(), forward);
                    self.act(BuildDiskAction::SetCdSidecars(next));
                }
                why => self.status = why.map(str::to_string),
            },
            Row::MacDriver => {
                let next = match c.mac_driver() {
                    MacDriver::Bundled => MacDriver::Off,
                    _ => MacDriver::Bundled,
                };
                self.act(BuildDiskAction::SetMacDriver(next));
            }
            Row::PBoot(i) => {
                let on = !c.table_view().rows.get(i).is_some_and(|r| r.bootable);
                self.act(BuildDiskAction::Table(BuildAction::SetRowBootable {
                    row: i,
                    on,
                }));
            }
            Row::PContent(i) => {
                let cur = match c.row_content(i) {
                    RowContent::Image => Content::Image,
                    RowContent::Empty => Content::Empty,
                    RowContent::Copy => Content::Copy,
                    RowContent::Format => c.row_fs(i).map_or(Content::Empty, Content::Fs),
                };
                match cycle(contents(c.row_can_copy(i)).as_slice(), cur, forward) {
                    Content::Copy => self.act(BuildDiskAction::SetRowCopy(i)),
                    Content::Empty => self.act(BuildDiskAction::SetRowFs { row: i, fs: None }),
                    Content::Fs(fs) => self.act(BuildDiskAction::SetRowFs {
                        row: i,
                        fs: Some(fs),
                    }),
                    Content::Image => {
                        self.act(BuildDiskAction::Table(BuildAction::PickRowSource(i)))
                    }
                }
            }
            Row::Donor => {
                if c.donor().is_some() {
                    self.act(BuildDiskAction::ClearDonor);
                } else {
                    self.act(BuildDiskAction::PickDonor);
                }
            }
            Row::Opt(r, opt) => {
                let Some(mut o) = self.opts_of(r) else {
                    return false;
                };
                match opt {
                    FsOption::BigEndian => o.big_endian = !o.big_endian,
                    FsOption::Fat32 => o.fat32 = !o.fat32,
                    FsOption::CaseSensitive => o.case_sensitive = !o.case_sensitive,
                    FsOption::AffsVariant => {
                        let all = AFFS_VARIANTS.map(|v| v.0);
                        o.affs_variant = cycle(&all, o.affs_variant, forward)
                    }
                    FsOption::CpmPreset => {
                        let all = cpm_presets();
                        let cur = o.cpm_preset.as_deref().unwrap_or("");
                        let cur = all.iter().copied().find(|p| *p == cur).unwrap_or(all[0]);
                        o.cpm_preset = Some(cycle(all.as_slice(), cur, forward).to_string());
                    }
                    FsOption::Geometry => {
                        let kind = self
                            .ctl
                            .format_targets()
                            .into_iter()
                            .find(|t| t.row == r)
                            .map(|t| t.fs);
                        let Some(kind) = kind else { return false };
                        let all: Vec<&str> = geometries(kind).iter().map(|g| g.0).collect();
                        let cur = o.geometry.as_deref().unwrap_or(all[0]);
                        let cur = all.iter().copied().find(|g| *g == cur).unwrap_or(all[0]);
                        o.geometry = Some(cycle(all.as_slice(), cur, forward).to_string());
                    }
                    FsOption::SectorSize => {
                        let s = cycle(&SECTOR_SIZES, o.sector_size.unwrap_or(512), forward);
                        OptionEdit::SectorSize(s).apply(&mut o);
                    }
                    _ => return false,
                }
                self.set_opts(r, o);
            }
            _ => return false,
        }
        true
    }
}

impl App {
    /// Build Disk keys; `false` lets the key fall through to the shell (tab switching, quit).
    pub(super) fn handle_build_disk_key(&mut self, code: KeyCode) -> bool {
        let devices = self.shell.devices().to_vec();
        let s = self.build_disk.get_or_insert_with(BuildDiskState::default);
        if let Some((which, picker)) = s.picker.as_mut() {
            let which = *which;
            match picker.handle_key(code) {
                Some(PickResult::Cancel) => {
                    s.picker = None;
                    s.act(BuildDiskAction::Picked {
                        purpose: which,
                        path: None,
                    });
                }
                Some(PickResult::Confirm(path)) => {
                    s.picker = None;
                    s.act(BuildDiskAction::Picked {
                        purpose: which,
                        path: Some(path),
                    });
                }
                None => {}
            }
            return true;
        }
        if s.device_pick {
            match code {
                KeyCode::Up | KeyCode::Char('k') => s.device_sel = s.device_sel.saturating_sub(1),
                KeyCode::Down | KeyCode::Char('j') => {
                    s.device_sel = (s.device_sel + 1).min(devices.len().saturating_sub(1))
                }
                KeyCode::Esc => s.device_pick = false,
                KeyCode::Enter => {
                    s.device_pick = false;
                    if let Some(device) = devices.get(s.device_sel).cloned() {
                        s.act(BuildDiskAction::SelectDevice {
                            row: s.device_sel,
                            device,
                        });
                    }
                }
                _ => {}
            }
            return true;
        }
        if let Some(buf) = s.edit.as_mut() {
            match code {
                KeyCode::Esc => s.edit = None,
                KeyCode::Enter => {
                    let text = s.edit.take().unwrap_or_default();
                    if let Some(row) = s.current() {
                        s.commit(row, text);
                    }
                }
                KeyCode::Backspace => {
                    buf.pop();
                }
                KeyCode::Char(ch) => buf.push(ch),
                _ => {}
            }
            return true;
        }
        let step = s.ctl.step();
        if step == Step::Build {
            match code {
                KeyCode::Esc if s.ctl.is_running() => s.act(BuildDiskAction::Cancel),
                KeyCode::Char('a') if !s.ctl.is_running() => {
                    s.act(BuildDiskAction::BuildAnother);
                    s.sel = 0;
                }
                _ => return false,
            }
            return true;
        }
        let rows = s.rows();
        let row = rows.get(s.sel).copied();
        match code {
            KeyCode::Up | KeyCode::Char('k') => s.sel = s.sel.saturating_sub(1),
            KeyCode::Down | KeyCode::Char('j') => {
                s.sel = (s.sel + 1).min(rows.len().saturating_sub(1))
            }
            KeyCode::Char('n') | KeyCode::PageDown => {
                s.act(BuildDiskAction::Next);
                s.sel = 0;
            }
            KeyCode::Char('b') | KeyCode::PageUp => {
                s.act(BuildDiskAction::Back);
                s.sel = 0;
            }
            KeyCode::Char('+') if step == Step::Partitions => {
                s.act(BuildDiskAction::Table(BuildAction::AddRow))
            }
            KeyCode::Char('-') | KeyCode::Char('[') | KeyCode::Char(']')
                if step == Step::Partitions =>
            {
                let part = match row {
                    Some(
                        Row::PSize(i)
                        | Row::PType(i)
                        | Row::PName(i)
                        | Row::PContent(i)
                        | Row::PBoot(i),
                    ) => i,
                    _ => return true,
                };
                let a = match code {
                    KeyCode::Char('-') => BuildAction::RemoveRow(part),
                    KeyCode::Char('[') => BuildAction::MoveRow {
                        row: part,
                        delta: -1,
                    },
                    _ => BuildAction::MoveRow {
                        row: part,
                        delta: 1,
                    },
                };
                s.act(BuildDiskAction::Table(a));
            }
            KeyCode::Left | KeyCode::Right | KeyCode::Char(' ') => {
                let Some(row) = row else {
                    return code == KeyCode::Char(' ');
                };
                if !s.change(row, code != KeyCode::Left) {
                    return code == KeyCode::Char(' ');
                }
            }
            KeyCode::Enter => match row {
                Some(Row::OutputDevice) => {
                    s.device_pick = true;
                    s.device_sel = s.ctl.device_row().unwrap_or(0);
                }
                Some(Row::OutputImage) => s.act(BuildDiskAction::PickOutputImage),
                Some(Row::AddPartition) => s.act(BuildDiskAction::Table(BuildAction::AddRow)),
                Some(Row::SystemDisk) => s.act(BuildDiskAction::PickSystemDisk),
                Some(Row::CdFolder) => s.act(BuildDiskAction::PickCdFolder),
                Some(Row::Handler(i)) => s.act(BuildDiskAction::RemoveHandler(i)),
                Some(Row::AddHandler(i)) => {
                    if let Some(t) = s.ctl.handlers_missing().get(i).cloned() {
                        s.act(BuildDiskAction::AddHandler(t));
                    }
                }
                Some(Row::MacDriver) => s.act(BuildDiskAction::PickMacDriver),
                Some(r) => match s.text_of(r) {
                    Some(t) => s.edit = Some(t),
                    None => {
                        s.change(r, true);
                    }
                },
                None => s.act(BuildDiskAction::Next),
            },
            _ => return false,
        }
        true
    }

    pub(super) fn build_disk_tick(&mut self) {
        if let Some(s) = self.build_disk.as_mut() {
            s.ctl.reconcile_devices(self.shell.devices());
            s.tick();
        }
    }

    pub(super) fn build_disk_footer_keys(&self) -> Vec<(&'static str, &'static str)> {
        let Some(s) = self.build_disk.as_ref() else {
            return Vec::new();
        };
        if s.edit.is_some() {
            return vec![("Enter", "Set"), ("Esc", "Cancel")];
        }
        if s.ctl.step() == Step::Build {
            return if s.ctl.is_running() {
                vec![("Esc", "Cancel")]
            } else {
                vec![("a", labels::BUILD_ANOTHER), ("<-/->", "Tabs")]
            };
        }
        vec![
            ("Up/Dn", "Row"),
            ("<-/->", "Change"),
            ("Enter", "Edit"),
            ("n", "Next"),
            ("b", "Back"),
            ("?", "Help"),
        ]
    }

    pub(super) fn draw_build_disk(&self, frame: &mut Frame, area: Rect, s: &BuildDiskState) {
        let pal = self.palette;
        let c = &s.ctl;
        let steps = c.steps();
        let here = steps.iter().position(|x| *x == c.step()).unwrap_or(0);
        let mut lines: Vec<Line<'static>> = Vec::new();
        lines.push(Line::raw(
            steps
                .iter()
                .enumerate()
                .map(|(i, st)| {
                    if i == here {
                        format!("[{}. {}]", i + 1, st.label())
                    } else {
                        format!("{}. {}", i + 1, st.label())
                    }
                })
                .collect::<Vec<_>>()
                .join(" -> "),
        ));
        lines.push(Line::raw(""));
        let v = c.table_view();
        match c.step() {
            Step::Partitions => match &v.layout {
                Ok(bar) => lines.push(Line::styled(bar_line(bar), pal.dim())),
                Err(e) => lines.push(Line::styled(e.clone(), pal.warn())),
            },
            Step::Review => {
                for l in c.review_lines() {
                    lines.push(Line::raw(l));
                }
                for w in c.warnings() {
                    lines.push(Line::styled(w, pal.warn()));
                }
                if let Some(want) = c.confirm_expected() {
                    lines.push(Line::styled(
                        format!("Everything on {want} will be erased. Type its path to confirm."),
                        pal.warn(),
                    ));
                }
                lines.push(Line::raw(""));
            }
            Step::Build => {
                if let Some(snap) = c.progress() {
                    let phase = if snap.phase.is_empty() {
                        snap.label.clone()
                    } else {
                        snap.phase.clone()
                    };
                    lines.push(Line::raw(phase));
                }
                match c.outcome() {
                    Some(Ok(())) => lines.push(Line::styled(labels::DONE, pal.accent())),
                    Some(Err(e)) => lines.push(Line::styled(format!("Error: {e}"), pal.warn())),
                    None => {}
                }
                if let Some(p) = c.built_image() {
                    lines.push(Line::raw(format!("Wrote {}", p.display())));
                }
            }
            _ => {}
        }
        let targets = c.format_targets();
        let opts = |r: Option<usize>| targets.iter().find(|t| t.row == r).map(|t| t.opts.clone());
        let mut last_part = None;
        for (i, row) in s.rows().iter().enumerate() {
            if let Row::PSize(p) = row {
                if last_part != Some(*p) {
                    last_part = Some(*p);
                    let placed = v.rows.get(*p).and_then(|r| r.planned_size.zip(r.start_lba));
                    let at = placed
                        .map(|(sz, lba)| format!("  {} at LBA {lba}", format_size(sz)))
                        .unwrap_or_default();
                    lines.push(Line::styled(
                        format!("Partition {}{at}", p + 1),
                        pal.accent(),
                    ));
                }
            }
            if let Row::Label(r) = row {
                if let Some(t) = targets.iter().find(|t| t.row == *r) {
                    lines.push(Line::styled(t.title.clone(), pal.accent()));
                }
            }
            let (label, value): (String, String) = match row {
                Row::Kind => (labels::KIND.into(), format!("< {} >", c.kind().label())),
                Row::OutputKind => (
                    "Output".into(),
                    if c.output_is_device() {
                        "< Physical disk >".into()
                    } else {
                        "< Image file >".into()
                    },
                ),
                Row::OutputImage => (
                    "Image file".into(),
                    format!(
                        "{}  [{}]",
                        c.image()
                            .map(|p| p.display().to_string())
                            .unwrap_or_else(|| "(Enter to choose)".into()),
                        labels::PICK_IMAGE
                    ),
                ),
                Row::Container => (
                    "Format".into(),
                    format!("< {} >", c.container().description()),
                ),
                Row::OutputDevice => (
                    "Device".into(),
                    c.device()
                        .map(|d| d.display_name())
                        .unwrap_or_else(|| "(Enter to choose)".into()),
                ),
                Row::Table => ("Table".into(), format!("< {} >", v.kind.label())),
                Row::Donor => (
                    "Start from".into(),
                    match (c.donor(), c.donor_reading(), c.donor_error()) {
                        (Some((p, t)), _, _) => {
                            format!("{t} layout of {}  (Enter: blank)", p.display())
                        }
                        (None, true, _) => "reading the donor...".into(),
                        (None, false, Some(e)) => format!("blank table ({e})"),
                        (None, false, None) => {
                            "blank table  (Enter: an existing disk's layout)".into()
                        }
                    },
                ),
                Row::Size => (
                    "Size".into(),
                    format!(
                        "{}  ({})",
                        c.size_text(),
                        c.disk_size().map(format_size).unwrap_or_else(|e| e)
                    ),
                ),
                Row::Align => (
                    "Alignment".into(),
                    if v.align_text.is_empty() {
                        format!("(default {})", v.align_hint)
                    } else {
                        v.align_text.clone()
                    },
                ),
                Row::Heads => (
                    "Heads".into(),
                    v.geometry.map(|g| g.0).unwrap_or(0).to_string(),
                ),
                Row::Sectors => (
                    "Sectors/track".into(),
                    v.geometry.map(|g| g.1).unwrap_or(0).to_string(),
                ),
                Row::VolumeFs => {
                    let range = c
                        .volume_size_range()
                        .unwrap_or_else(|| "fixed size".to_string());
                    (
                        "Filesystem".into(),
                        format!("< {} >  {range}", c.volume_fs().display_name()),
                    )
                }
                Row::Preset => ("Preset".into(), format!("< {} >", c.preset().kind.label())),
                Row::X68kScsi => (
                    "Controller".into(),
                    if c.preset().x68k_scsi {
                        "< SCSI (1024-byte sectors) >".into()
                    } else {
                        "< SASI (256-byte sectors) >".into()
                    },
                ),
                Row::X68kParts => ("Partitions".into(), c.preset().x68k_partitions.to_string()),
                Row::SystemDisk => (
                    "System disk".into(),
                    c.preset()
                        .x68k_system_disk
                        .as_ref()
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|| "(none: seed files; Enter to choose)".into()),
                ),
                Row::SgiLabel => ("EFS label".into(), c.preset().sgi_label.clone()),
                Row::CdLayout => (
                    labels::CD_LAYOUT.into(),
                    format!("< {} >", c.cd().layout.label()),
                ),
                Row::CdName => ("Volume name".into(), c.cd().name.clone()),
                Row::CdAutoSize => (
                    labels::CD_AUTO_SIZE.into(),
                    if c.cd_auto_size() {
                        "< on >".into()
                    } else {
                        "< off >".into()
                    },
                ),
                Row::CdFolder => (
                    "Fill from".into(),
                    format!(
                        "{}  [{}]",
                        c.cd()
                            .from_dir
                            .as_ref()
                            .map(|p| p.display().to_string())
                            .unwrap_or_else(|| "(blank disc)".into()),
                        labels::CD_FOLDER
                    ),
                ),
                Row::CdOpt(option) => {
                    let row = c.cd_options().into_iter().find(|r| r.option == *option);
                    let on = row.as_ref().is_some_and(|r| r.on);
                    let (label, value) = tick(option.label(), on);
                    match row.and_then(|r| r.unavailable) {
                        Some(why) => (label, format!("{value}  (n/a: {why})")),
                        None => (label, value),
                    }
                }
                Row::CdSidecars => (
                    "macOS ._ files".into(),
                    match c.cd_sidecars_unavailable() {
                        Some(why) => format!("< {} >  (n/a: {why})", c.cd_sidecars().label()),
                        None => format!("< {} >", c.cd_sidecars().label()),
                    },
                ),
                Row::PSize(p) => (
                    "  Size".into(),
                    v.rows
                        .get(*p)
                        .map(|r| r.size_text.clone())
                        .unwrap_or_default(),
                ),
                Row::PType(p) => (
                    "  Type".into(),
                    v.rows
                        .get(*p)
                        .map(|r| r.type_text.clone())
                        .unwrap_or_default(),
                ),
                Row::PName(p) => (
                    "  Name".into(),
                    v.rows.get(*p).map(|r| r.name.clone()).unwrap_or_default(),
                ),
                Row::PContent(p) => (
                    "  Content".into(),
                    match c.row_content(*p) {
                        RowContent::Empty => "< Empty >".into(),
                        RowContent::Copy => "< Copy from the donor >".into(),
                        RowContent::Format => format!(
                            "< {} >",
                            c.row_fs(*p).map(|k| k.display_name()).unwrap_or("?")
                        ),
                        RowContent::Image => {
                            let r = v.rows.get(*p);
                            let path = r
                                .and_then(|r| r.source.as_ref())
                                .map(|p| p.display().to_string())
                                .unwrap_or_default();
                            let size = r.map(|r| format_size(r.source_size)).unwrap_or_default();
                            format!("< Image: {path} ({size}) >")
                        }
                    },
                ),
                Row::PBoot(p) => (
                    "  Bootable".into(),
                    if v.rows.get(*p).is_some_and(|r| r.bootable) {
                        "[x]".into()
                    } else {
                        "[ ]".into()
                    },
                ),
                Row::AddPartition => ("".into(), "[ Add partition ]".into()),
                Row::MacDriver => (
                    "Mac driver".into(),
                    format!("< {} >  (Enter: from a disk)", c.mac_driver().label()),
                ),
                Row::Handler(i) => (
                    "Handler".into(),
                    c.handlers()
                        .get(*i)
                        .map(|(t, p)| format!("{t}: {}  (Enter removes)", p.display()))
                        .unwrap_or_default(),
                ),
                Row::AddHandler(i) => (
                    "".into(),
                    c.handlers_missing()
                        .get(*i)
                        .map(|t| format!("[ Add {t} handler ]"))
                        .unwrap_or_default(),
                ),
                Row::Label(r) => (
                    "  Label".into(),
                    opts(*r).map(|o| o.label).unwrap_or_default(),
                ),
                Row::Opt(r, o) => {
                    let Some(x) = opts(*r) else { continue };
                    let auto = |n: Option<u64>| n.map(|n| n.to_string()).unwrap_or("auto".into());
                    let tick = |b: bool| if b { "[x]" } else { "[ ]" }.to_string();
                    match o {
                        FsOption::BlockSize => {
                            ("  Block size".into(), auto(x.block_size.map(u64::from)))
                        }
                        FsOption::BigEndian => ("  Big-endian".into(), tick(x.big_endian)),
                        FsOption::Fat32 => ("  FAT32 always".into(), tick(x.fat32)),
                        FsOption::CaseSensitive => {
                            ("  Case-sensitive".into(), tick(x.case_sensitive))
                        }
                        FsOption::AffsVariant => {
                            ("  Variant".into(), format!("< DOS\\{} >", x.affs_variant))
                        }
                        FsOption::CpmPreset => (
                            "  Disk format".into(),
                            format!("< {} >", x.cpm_preset.unwrap_or_default()),
                        ),
                        FsOption::Inodes => ("  Inodes".into(), auto(x.inodes)),
                        FsOption::BytesPerInode => {
                            ("  Bytes/inode".into(), auto(x.bytes_per_inode))
                        }
                        FsOption::ClusterSize => (
                            "  Cluster size".into(),
                            auto(x.cluster_bytes.map(u64::from)),
                        ),
                        FsOption::Geometry => {
                            let t = targets.iter().find(|t| t.row == *r);
                            let label = t
                                .and_then(|t| {
                                    let all = geometries(t.fs);
                                    let id = x.geometry.as_deref().unwrap_or(all.first()?.0);
                                    all.iter().find(|g| g.0 == id).map(|g| g.1)
                                })
                                .unwrap_or("?");
                            ("  Geometry".into(), format!("< {label} >"))
                        }
                        FsOption::SectorSize => (
                            "  Sector size".into(),
                            format!("< {} >", x.sector_size.unwrap_or(512)),
                        ),
                        FsOption::BtreeSizes | FsOption::MinCatalog => continue,
                    }
                }
                Row::Confirm => ("Confirm".into(), c.confirm_text().to_string()),
            };
            let sel = i == s.sel;
            let value = match (&s.edit, sel) {
                (Some(buf), true) => format!("{buf}_"),
                _ => value,
            };
            let marker = if sel { "> " } else { "  " };
            let style = if sel {
                pal.accent().add_modifier(Modifier::BOLD)
            } else {
                ratatui::style::Style::default()
            };
            lines.push(Line::styled(format!("{marker}{label:<18} {value}"), style));
            if sel {
                if let Some(help) = row_help(c, row) {
                    lines.push(Line::styled(format!("    {help}"), pal.dim()));
                }
            }
        }
        if c.step() == Step::Options {
            for t in &targets {
                if let Some(e) = &t.error {
                    lines.push(Line::styled(format!("{}: {e}", t.title), pal.warn()));
                }
            }
        }
        lines.push(Line::raw(""));
        if c.step() != Step::Build {
            match c.next_blocked() {
                Some(why) => lines.push(Line::styled(why, pal.warn())),
                None if c.step() == Step::Review => {
                    lines.push(Line::styled("Press n to create the disk.", pal.accent()))
                }
                None => lines.push(Line::styled("Press n for the next step.", pal.dim())),
            }
        }
        if let Some(st) = &s.status {
            lines.push(Line::styled(st.clone(), pal.warn()));
        }
        for l in &s.log {
            lines.push(Line::styled(l.clone(), pal.dim()));
        }
        let title = format!("Build Disk  -  {}", c.step().label());
        frame.render_widget(
            Paragraph::new(Text::from(lines))
                .block(self.pane_block(title.as_str(), true))
                .wrap(Wrap { trim: false }),
            area,
        );
        if let Some((_, picker)) = &s.picker {
            picker.draw(frame, area, self.palette, self.border);
        }
        if s.device_pick {
            self.draw_device_pick(frame, area, s.device_sel, "Pick the device to build on");
        }
    }
}

/// The plain-language help shown under a selected CD option.
fn row_help(c: &BuildDiskController, row: &Row) -> Option<String> {
    match row {
        Row::CdOpt(o) => Some(o.hint().to_string()),
        Row::CdSidecars => Some(format!("{} {}", hints::CD_SIDECARS, c.cd_sidecars().hint())),
        _ => None,
    }
}

/// A check row's label and its `[x]` / `[ ]` value.
fn tick(label: &str, on: bool) -> (String, String) {
    (
        label.to_string(),
        if on { "[x]".into() } else { "[ ]".into() },
    )
}

#[cfg(test)]
mod tests {
    use super::super::{App, TabId, TABS};
    use super::*;
    use crossterm::event::{KeyEvent, KeyModifiers};

    fn key(app: &mut App, code: KeyCode) {
        app.on_key(KeyEvent::new(code, KeyModifiers::NONE));
    }

    fn keys(app: &mut App, text: &str) {
        for c in text.chars() {
            key(app, KeyCode::Char(c));
        }
    }

    fn screen(app: &App) -> String {
        let backend = ratatui::backend::TestBackend::new(100, 40);
        let mut term = ratatui::Terminal::new(backend).unwrap();
        term.draw(|f| app.draw(f)).unwrap();
        let buf = term.backend().buffer().clone();
        let mut out = String::new();
        for y in 0..buf.area.height {
            for x in 0..buf.area.width {
                out.push_str(buf[(x, y)].symbol());
            }
            out.push('\n');
        }
        out
    }

    /// Keys alone build a partitioned image with a FAT partition, the same path the GUI walks.
    #[test]
    fn keys_walk_the_wizard_and_build_a_formatted_disk() {
        let dir = tempfile::tempdir().unwrap();
        let img = dir.path().join("tui.img");
        let tab = TABS.iter().position(|t| t.0 == TabId::Build).unwrap();
        let mut app = App::new_on(tab);
        app.build_disk = Some(BuildDiskState::default());
        assert!(screen(&app).contains("Build Disk"));
        key(&mut app, KeyCode::Char('n'));
        key(&mut app, KeyCode::Down);
        key(&mut app, KeyCode::Enter);
        assert!(
            app.build_disk.as_ref().unwrap().picker.is_some(),
            "Save As opens a picker"
        );
        keys(&mut app, img.display().to_string().as_str());
        key(&mut app, KeyCode::Enter);
        assert_eq!(
            app.build_disk.as_ref().unwrap().ctl.image(),
            Some(img.as_path())
        );
        key(&mut app, KeyCode::Char('n'));
        // Disk step: Start from, Table, Size.
        key(&mut app, KeyCode::Down);
        key(&mut app, KeyCode::Down);
        key(&mut app, KeyCode::Enter);
        for _ in 0..8 {
            key(&mut app, KeyCode::Backspace);
        }
        keys(&mut app, "64M");
        key(&mut app, KeyCode::Enter);
        key(&mut app, KeyCode::Char('n'));
        assert_eq!(
            app.build_disk.as_ref().unwrap().ctl.step(),
            Step::Partitions
        );
        key(&mut app, KeyCode::Down);
        key(&mut app, KeyCode::Down);
        for _ in 0..4 {
            key(&mut app, KeyCode::Right);
        }
        assert_eq!(
            app.build_disk.as_ref().unwrap().ctl.row_fs(0),
            Some(FsKind::Fat)
        );
        assert!(screen(&app).contains("FAT12 / FAT16 / FAT32"));
        for _ in 0..3 {
            key(&mut app, KeyCode::Char('n'));
        }
        let c = &app.build_disk.as_ref().unwrap().ctl;
        assert_eq!(c.step(), Step::Build, "{:?}", c.next_blocked());
        let start = std::time::Instant::now();
        while app.build_disk.as_ref().unwrap().ctl.is_running() {
            assert!(start.elapsed().as_secs() < 60, "the build hangs");
            std::thread::sleep(std::time::Duration::from_millis(20));
            app.tick();
        }
        app.tick();
        assert_eq!(
            app.build_disk.as_ref().unwrap().ctl.outcome(),
            Some(&Ok(()))
        );
        assert!(screen(&app).contains("Disk build complete"));
        assert_eq!(std::fs::metadata(&img).unwrap().len(), 64 << 20);
    }
}
