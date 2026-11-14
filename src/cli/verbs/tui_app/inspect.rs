//! The TUI Inspect screen: a source's partition grid and disk tools over a headless
//! `InspectController`.
//!
//! The same controller drives the egui Inspect tab, so the screen offers its sources (a disk from
//! the list, an image file, a backup folder or Clonezilla image, a remote image), its grid (min
//! sizes, Calc min, volume labels, a text layout line) and its tools: Check / Repair, Make
//! Bootable, Expand Image, Defragment (Human68k), the word-order swap, CHD Info, Export (whole
//! disk, per partition, HFV, the CHD estimate) with Physical Disk Export (the shared writer
//! window), the remote Back Up Image, and Browse (the Explorer). A disk opens through the
//! controller, which elevates as the GUI does (macOS shows its administrator dialog); when that
//! open fails the table is read again without elevation and listed read-only. The GUI's Inspect
//! dialogs open over the grid: Edit Partition Table and Add Partition (`table_edit.rs`), Resize
//! Partitions (`resize.rs`), and Resize Alto Disk, Expand HFS Volume and Convert Floppy Container
//! (`dialogs.rs`). See docs/phase2_commander_plan.md § "Slice 26" and § "Slice 27".

use std::path::PathBuf;
use std::sync::Arc;

use crossterm::event::KeyCode;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::Modifier;
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Clear, Paragraph};
use ratatui::Frame;

use super::ask::Ask;
use super::dialogs::{DRow, DialogKey, DialogWindow, HINTS_ALTO_RESIZE, HINTS_EXPAND_HFS};
use super::resize::{ResizeKey, ResizeWindow, HINTS_RESIZE};
use super::resize_image::{WizardKey, WizardWindow, HINTS_RESIZE_IMAGE};
use super::table_edit::{TableKey, TableWindow, HINTS_TABLE_EDIT};
use super::writer::{WriterKey, WriterWindow, HINTS_WRITER};
use super::{centered_rect, hint, App, FilePicker, KeyHint, PartRow, PickKind, PickResult};
use crate::controller::alto_resize::{AltoResizeAction, AltoResizeController};
#[cfg(feature = "chd")]
use crate::controller::app::chd::{
    codec_choices, codec_label, hunk_presets, modes_for, unit_size_for, ChdOptionsControl,
    ChdOptionsMode,
};
use crate::controller::app::{DiskDevice, RecentHandle, RecentMode};
use crate::controller::disk_write::{DiskWriteAction, DiskWriteController, DiskWriteServices};
use crate::controller::expand_hfs::{ExpandHfsAction, ExpandHfsController};
use crate::controller::inspect::{
    format_size, per_partition_refusal, BrowseOpen, ChdOptions, ChdProfile, ExportFormat,
    HostRequest, InspectAction, InspectController, InspectServices, InspectSource, JobRegistry,
    LayoutSegment, MinCell, NoNotify, SegmentKind, SizeMode, CHD_REFUSAL,
};
#[cfg(feature = "remote")]
use crate::controller::remote_pick::{RemotePickAction, RemotePickController};
use crate::controller::resize::{ResizeAction, ResizeController};
use crate::controller::resize_image::{ResizeImageAction, ResizeImageController};
use crate::controller::{Controller as _, Effect, PickSpec};

/// Which controller asked a question.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[cfg_attr(not(feature = "remote"), allow(dead_code))]
pub(super) enum Who {
    Inspect,
    Writer,
    Remote,
    Resize,
    Alto,
    Hfs,
    Wizard,
}

/// A window of the screen's own, drawn over the grid.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[cfg_attr(not(feature = "remote"), allow(dead_code))]
pub(super) enum Win {
    Export,
    Remote,
}

/// One row of the Export window.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum ERow {
    Layout,
    Format,
    ChdMode,
    ChdHunk,
    ChdCodec(usize),
    /// A partition's size mode, by its position in the window's list.
    Size(usize),
    SizeMib(usize),
    Export,
    Physical,
}

/// What a confirmed picker answers: the controller that asked, or the writer's image field.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Pick {
    Of(Who, &'static str),
    WriterImage,
}

pub(super) const HINTS_INSPECT_SOURCE: &[KeyHint] = &[
    hint("Up/Down", "Move between partitions  (also k/j)"),
    hint("g/G", "Top / bottom  (also Home/End)"),
    hint("Enter", "Browse the partition in the Explorer  (also b)"),
    hint("c", "Check the partition's filesystem"),
    hint("m", "Calc min: walk the volume for its minimum size"),
    hint("d", "Defragment a Human68k partition"),
    hint("r", "Re-inspect the source"),
    hint("e", "Export the disk image (and Physical Disk Export)"),
    hint(
        "x",
        "Expand Image: the Resize Image wizard (size, partitions, filesystems)",
    ),
    hint("B", "Make Bootable (classic Mac disks)"),
    hint("w", "Swap the 16-bit word order into a new image"),
    hint("i", "CHD Info"),
    hint("u", "Back up the remote image to a folder"),
    hint("R", "Remote: connect, or pick another remote image"),
    hint("t", "Edit Partition Table"),
    hint("a", "Add Partition into the largest free gap"),
    hint("s", "Resize Partitions"),
    hint("H", "Expand HFS Volume (APM disk or flat HFV)"),
    hint("A", "Resize Alto Disk (Diablo 31 / 44)"),
    hint("f", "Convert this image in the Convert tab"),
    hint("o", "Open another image, backup folder or Clonezilla image"),
    hint("Esc", "Close the source (cancels a running export first)"),
];

pub(super) const HINTS_INSPECT_EXPORT: &[KeyHint] = &[
    hint("Up/Down", "Move between rows"),
    hint("Left/Right", "Change the layout, format or a size mode"),
    hint("Space", "Change the row's choice"),
    hint("0-9", "Type a Custom size in MiB"),
    hint("Enter", "Export, or open Physical Disk Export"),
    hint("s", "Export"),
    hint("Esc", "Close the window"),
];

pub(super) const HINTS_INSPECT_WINDOW: &[KeyHint] = &[
    hint("Up/Down", "Scroll, or move in a list"),
    hint(
        "Enter",
        "Act on the window (expand, open the folder or image)",
    ),
    hint("0-9", "Type the MiB to add (Expand Image)"),
    hint("r", "Repair (check results)"),
    hint("d", "Show debug messages (check results)"),
    hint("Esc", "Close the window"),
];

pub(super) struct InspectState {
    pub(super) ctl: InspectController,
    /// The app's recent-files store; an open is recorded through it.
    recent: RecentHandle,
    /// Physical Disk Export's writer and its window's cursor.
    pub(super) writer: DiskWriteController,
    pub(super) win: WriterWindow,
    #[cfg(feature = "remote")]
    pub(super) remote: RemotePickController,
    #[cfg_attr(not(feature = "remote"), allow(dead_code))]
    remote_sel: usize,
    /// The Inspect dialogs' controllers and their windows' cursors.
    pub(super) resize: ResizeController,
    pub(super) rwin: ResizeWindow,
    /// Resize Image, and its cursor.
    pub(super) wizard: ResizeImageController,
    pub(super) iwin: WizardWindow,
    pub(super) twin: TableWindow,
    pub(super) alto: AltoResizeController,
    pub(super) hfs: ExpandHfsController,
    pub(super) dwin: DialogWindow,
    /// Add Partition applies (refreshed after every action; the check reads the disk size).
    can_add: bool,
    /// The disk's facts for the header, when the source came from the disk list.
    pub(super) disk: Option<Box<DiskDevice>>,
    /// A disk's rows read without elevation, after the controller's open failed.
    pub(super) plain: Option<Vec<PartRow>>,
    plain_for: Option<PathBuf>,
    pub(super) sel: usize,
    pub(super) window: Option<Win>,
    pub(super) esel: ERow,
    #[cfg(feature = "chd")]
    chd: ChdOptionsControl,
    /// The layout line's segments, refreshed each tick (the controller caches the disk size).
    layout: Vec<LayoutSegment>,
    scroll: usize,
    pub(super) ask: Option<Ask<Who>>,
    picker: Option<(Pick, FilePicker)>,
    device_pick: bool,
    device_sel: usize,
    /// The controllers' latest log lines.
    pub(super) log: Vec<String>,
    /// A line the screen shows until the next action (a hand-off it cannot take, a result).
    pub(super) note: Option<String>,
    /// Hand-offs for the app: the Explorer to open or close, a seekable reopen, an archive.
    browse: Option<(Box<BrowseOpen>, String)>,
    close_explorer: bool,
    upgrade: Option<PathBuf>,
    archive: Option<PathBuf>,
    /// An optical disc the open found, for the Optical tab.
    optical: Option<PathBuf>,
    /// The image `f` hands to the Convert tab.
    convert: Option<PathBuf>,
    /// The writer's run is on screen; its end becomes the note.
    writing: bool,
    /// A backup was closed here; the app closes the shared folder (Restore too).
    pub(super) closed_backup: bool,
}

impl InspectState {
    pub(super) fn new(recent: RecentHandle) -> Self {
        InspectState {
            ctl: InspectController::new(InspectServices {
                jobs: JobRegistry::new(),
                notify: Arc::new(NoNotify),
            }),
            recent,
            writer: DiskWriteController::new(DiskWriteServices {
                jobs: crate::controller::disk_write::JobRegistry::new(),
                notify: Arc::new(crate::controller::disk_write::NoNotify),
            }),
            win: WriterWindow::default(),
            #[cfg(feature = "remote")]
            remote: RemotePickController::new(Arc::new(NoNotify)),
            remote_sel: 0,
            resize: ResizeController::new(Arc::new(NoNotify)),
            rwin: ResizeWindow::default(),
            wizard: ResizeImageController::new(Arc::new(NoNotify)),
            iwin: WizardWindow::default(),
            twin: TableWindow::default(),
            alto: AltoResizeController::new(Arc::new(NoNotify)),
            hfs: ExpandHfsController::new(Arc::new(NoNotify)),
            dwin: DialogWindow::default(),
            can_add: false,
            disk: None,
            plain: None,
            plain_for: None,
            sel: 0,
            window: None,
            esel: ERow::Format,
            #[cfg(feature = "chd")]
            chd: ChdOptionsControl::new(ChdProfile::Hd),
            layout: Vec::new(),
            scroll: 0,
            ask: None,
            picker: None,
            device_pick: false,
            device_sel: 0,
            log: Vec::new(),
            note: None,
            browse: None,
            close_explorer: false,
            upgrade: None,
            archive: None,
            optical: None,
            convert: None,
            writing: false,
            closed_backup: false,
        }
    }

    /// A source is open or opening: the grid owns the screen.
    pub(super) fn source_open(&self) -> bool {
        self.ctl.has_source() || self.ctl.is_preparing()
    }

    /// A job runs whose end the screen waits for, so the event loop polls faster.
    pub(super) fn busy(&self) -> bool {
        let remote = {
            #[cfg(feature = "remote")]
            {
                self.remote.working().is_some()
            }
            #[cfg(not(feature = "remote"))]
            {
                false
            }
        };
        self.ctl.loading().is_some()
            || self.ctl.probing()
            || !self.ctl.tool_jobs().is_empty()
            || self.ctl.export_running()
            || self.ctl.remote_backup_running()
            || self.ctl.swabbing()
            || !self.ctl.browse_jobs().is_empty()
            || self.writer.is_writing()
            || self.writer.opening().is_some()
            || self.ctl.summarizing_hfs()
            || self.resize.is_running()
            || self.wizard.is_running()
            || self.alto.opening().is_some()
            || self.alto.is_writing()
            || self.hfs.is_running()
            || remote
    }

    pub(super) fn act(&mut self, action: InspectAction) {
        self.note = None;
        let fx = self.ctl.apply(action);
        self.absorb(Who::Inspect, fx);
        self.after();
    }

    pub(super) fn wact(&mut self, action: DiskWriteAction) {
        let fx = self.writer.apply(action);
        self.absorb(Who::Writer, fx);
        self.after();
    }

    #[cfg(feature = "remote")]
    pub(super) fn ract(&mut self, action: RemotePickAction) {
        let fx = self.remote.apply(action);
        self.absorb(Who::Remote, fx);
        self.after();
    }

    pub(super) fn zact(&mut self, action: ResizeAction) {
        let fx = self.resize.apply(action);
        self.absorb(Who::Resize, fx);
        self.after();
    }

    pub(super) fn iact(&mut self, action: ResizeImageAction) {
        let fx = self.wizard.apply(action);
        self.absorb(Who::Wizard, fx);
        self.after();
        self.iwin.settle(&self.wizard);
    }

    pub(super) fn aact(&mut self, action: AltoResizeAction) {
        let fx = self.alto.apply(action);
        self.absorb(Who::Alto, fx);
        self.after();
    }

    pub(super) fn hact(&mut self, action: ExpandHfsAction) {
        let fx = self.hfs.apply(action);
        self.absorb(Who::Hfs, fx);
        self.after();
    }

    /// Turn the controllers' effects into the screen's pickers and questions.
    fn absorb(&mut self, who: Who, fx: Vec<Effect>) {
        for e in fx {
            match e {
                Effect::PickFile(spec) => self.open_picker(who, spec, false),
                Effect::PickFolder(spec) => self.open_picker(who, spec, true),
                Effect::Confirm(spec) => self.ask = Some(Ask::Confirm(who, spec)),
                Effect::AskText(spec) => self.ask = Some(Ask::text(who, spec)),
                Effect::Message(msg) => self.log.push(msg),
                // None of the screen's controllers asks for these.
                Effect::AskPassword(_)
                | Effect::Choose(_)
                | Effect::OpenUrl(_)
                | Effect::CopyToClipboard(_)
                | Effect::CloseScreen
                | Effect::Quit => {}
            }
        }
    }

    fn open_picker(&mut self, who: Who, spec: PickSpec, folder: bool) {
        let kind = if folder {
            PickKind::Dir
        } else if spec.save {
            PickKind::Save
        } else {
            PickKind::File
        };
        let mut p = FilePicker::new(kind, spec.title.as_str());
        if spec.save {
            // Beside the image (else the working folder), as a save dialog would open.
            let dir = self
                .ctl
                .image_path()
                .and_then(|p| p.parent())
                .map(|p| p.to_path_buf())
                .or_else(|| std::env::current_dir().ok())
                .unwrap_or_default();
            p.input = dir.join(spec.file_name.as_str()).display().to_string();
        }
        self.picker = Some((Pick::Of(who, spec.purpose), p));
    }

    /// Carry out the controllers' requests and keep their latest log lines.
    fn after(&mut self) {
        for req in self.ctl.take_requests() {
            match req {
                HostRequest::CloseBrowse => {
                    self.browse = None;
                    self.close_explorer = true;
                }
                HostRequest::OpenBrowse(open) => {
                    let label = self.browse_label();
                    self.browse = Some((open, label));
                }
                HostRequest::UpgradeSeekable(path) => self.upgrade = Some(path),
                HostRequest::OpenArchive(path) => self.archive = Some(path),
                HostRequest::OpenOptical(path) => {
                    if cfg!(feature = "optical") {
                        self.optical = Some(path);
                    } else {
                        let what = path.display();
                        let why = "this build has no Optical tab.";
                        self.note = Some(format!("{what} is an optical disc; {why}"));
                    }
                }
                HostRequest::BackupClosed => self.closed_backup = true,
                HostRequest::ExpandHfs(source) => {
                    let fx = self.hfs.apply(ExpandHfsAction::Open(*source));
                    self.absorb(Who::Hfs, fx);
                    self.dwin.reset(DRow::HfsFormat);
                }
            }
        }
        #[cfg(feature = "remote")]
        {
            let lines = self.remote.take_log();
            self.log
                .extend(lines.into_iter().map(|l| format!("Remote: {l}")));
            if let Some(conn) = self.remote.take_request() {
                self.window = None;
                self.disk = None;
                self.plain = None;
                self.sel = 0;
                let fx = self
                    .ctl
                    .apply(InspectAction::Open(InspectSource::Remote { conn }));
                self.absorb(Who::Inspect, fx);
            }
        }
        if let Some(path) = self.wizard.take_open_request() {
            let fx = self.wizard.apply(ResizeImageAction::Close);
            self.absorb(Who::Wizard, fx);
            self.disk = None;
            self.plain = None;
            self.sel = 0;
            let fx = self
                .ctl
                .apply(InspectAction::Open(InspectSource::ImageFile(path)));
            self.absorb(Who::Inspect, fx);
        }
        // A resize rewrote the table under the grid: read it again.
        if self.resize.take_disk_changed() | self.wizard.take_disk_changed() {
            let fx = self.ctl.apply(InspectAction::DiskChanged);
            self.absorb(Who::Inspect, fx);
        }
        let lines: Vec<String> = self
            .ctl
            .take_log()
            .into_iter()
            .chain(self.writer.take_log())
            .chain(self.resize.take_log())
            .chain(self.wizard.take_log())
            .chain(self.alto.take_log())
            .chain(self.hfs.take_log())
            .map(|(_, msg)| msg)
            .collect();
        self.log.extend(lines);
        let n = self.log.len();
        if n > 8 {
            self.log.drain(..n - 8);
        }
        let rows = self.row_count();
        self.sel = self.sel.min(rows.saturating_sub(1));
        self.can_add = self.ctl.can_add_partition();
    }

    /// The Explorer's title for the selected partition.
    fn browse_label(&self) -> String {
        match self.ctl.partitions().get(self.sel) {
            Some(p) => {
                let name = self
                    .ctl
                    .volume_label(p.index)
                    .map(str::to_string)
                    .unwrap_or_else(|| p.type_name.clone());
                format!("#{}  {name}", self.sel + 1)
            }
            None => "Whole volume".to_string(),
        }
    }

    /// A picker the controller asked for is up.
    #[cfg(test)]
    pub(super) fn picker_open(&self) -> bool {
        self.picker.is_some()
    }

    pub(super) fn row_count(&self) -> usize {
        match &self.plain {
            Some(rows) if self.ctl.partitions().is_empty() => rows.len(),
            _ => self.ctl.partitions().len(),
        }
    }

    /// The selected partition's index in the controller's list.
    fn selected(&self) -> Option<usize> {
        self.ctl.partitions().get(self.sel).map(|p| p.index)
    }

    /// A picker's answer, to the controller that asked; a cancel is `None`.
    fn picked(&mut self, pick: Pick, path: Option<PathBuf>) {
        match pick {
            Pick::Of(Who::Inspect, purpose) => self.act(InspectAction::Picked { purpose, path }),
            Pick::Of(Who::Alto, purpose) => self.aact(AltoResizeAction::Picked { purpose, path }),
            Pick::Of(Who::Hfs, purpose) => self.hact(ExpandHfsAction::Picked { purpose, path }),
            Pick::Of(Who::Writer, purpose) => self.wact(DiskWriteAction::Picked { purpose, path }),
            Pick::Of(Who::Wizard, purpose) => {
                self.iact(ResizeImageAction::Picked { purpose, path })
            }
            // The remote picker and Resize ask for no file.
            Pick::Of(Who::Remote, _) | Pick::Of(Who::Resize, _) => {}
            Pick::WriterImage => {
                if let Some(path) = path {
                    self.win.image = path.display().to_string();
                    if let Some(a) = self.win.image_target() {
                        self.wact(a);
                    }
                }
            }
        }
    }

    /// Answer the open question.
    fn answer(&mut self, yes: bool, text: Option<String>) {
        let Some(ask) = self.ask.take() else {
            return;
        };
        match ask {
            Ask::Confirm(Who::Inspect, spec) => self.act(InspectAction::Confirmed {
                purpose: spec.purpose,
                yes,
            }),
            Ask::Confirm(Who::Writer, spec) => self.wact(DiskWriteAction::Confirmed {
                purpose: spec.purpose,
                yes,
            }),
            Ask::Text {
                who: Who::Writer,
                spec,
                ..
            } => self.wact(DiskWriteAction::Answered {
                purpose: spec.purpose,
                text,
            }),
            #[cfg(feature = "remote")]
            Ask::Text {
                who: Who::Remote,
                spec,
                ..
            } => self.ract(RemotePickAction::Answered {
                purpose: spec.purpose,
                text,
            }),
            Ask::Confirm(Who::Resize, spec) => self.zact(ResizeAction::Confirmed {
                purpose: spec.purpose,
                yes,
            }),
            Ask::Confirm(Who::Wizard, spec) => self.iact(ResizeImageAction::Confirmed {
                purpose: spec.purpose,
                yes,
            }),
            // The Inspect controller asks no text; the others ask no y / n.
            Ask::Text { .. }
            | Ask::Confirm(Who::Remote, _)
            | Ask::Confirm(Who::Alto, _)
            | Ask::Confirm(Who::Hfs, _) => {}
        }
    }

    // --- the Export window --------------------------------------------------------------

    fn chd_shown(&self) -> bool {
        cfg!(feature = "chd") && self.ctl.export_settings().format == ExportFormat::Chd
    }

    #[cfg(feature = "chd")]
    fn chd_custom(&self) -> bool {
        self.chd.mode == ChdOptionsMode::Custom
    }

    #[cfg(not(feature = "chd"))]
    fn chd_custom(&self) -> bool {
        false
    }

    pub(super) fn erows(&self) -> Vec<ERow> {
        let mut r = vec![ERow::Layout, ERow::Format];
        if self.chd_shown() {
            r.push(ERow::ChdMode);
            if self.chd_custom() {
                r.push(ERow::ChdHunk);
                r.extend((0..4).map(ERow::ChdCodec));
            }
        }
        for (i, cfg) in self.ctl.export_settings().configs.iter().enumerate() {
            r.push(ERow::Size(i));
            if cfg.choice == SizeMode::Custom {
                r.push(ERow::SizeMib(i));
            }
        }
        r.extend([ERow::Export, ERow::Physical]);
        r
    }

    fn emove(&mut self, delta: isize) {
        let rows = self.erows();
        let n = rows.len() as isize;
        let at = rows.iter().position(|r| *r == self.esel).unwrap_or(0);
        self.esel = rows[((at as isize + delta).rem_euclid(n)) as usize];
    }

    /// Left / Right / Space on a choice row; `false` when the row has none.
    fn echange(&mut self, row: ERow, forward: bool) -> bool {
        fn next<T: Copy + PartialEq>(list: &[T], cur: T, forward: bool) -> T {
            let n = list.len();
            match list.iter().position(|x| *x == cur) {
                Some(i) if forward => list[(i + 1) % n],
                Some(i) => list[(i + n - 1) % n],
                None => list[0],
            }
        }
        let formats: Vec<_> = self
            .ctl
            .export_formats()
            .into_iter()
            .filter(|(_, on)| *on)
            .map(|(c, _)| c.format)
            .collect();
        let s = self.ctl.export_settings_mut();
        match row {
            ERow::Layout => {
                let whole = s.whole_disk;
                s.set_whole_disk(!whole);
            }
            ERow::Format => s.format = next(formats.as_slice(), s.format, forward),
            ERow::Size(i) => {
                let Some(cfg) = s.configs.get_mut(i) else {
                    return false;
                };
                let mode = next(cfg.mode_choices().as_slice(), cfg.choice, forward);
                cfg.set_choice(mode);
            }
            ERow::ChdMode | ERow::ChdHunk | ERow::ChdCodec(_) => {
                return self.change_chd(row, forward)
            }
            _ => return false,
        }
        true
    }

    #[cfg(feature = "chd")]
    fn change_chd(&mut self, row: ERow, forward: bool) -> bool {
        fn next<T: Copy + PartialEq>(list: &[T], cur: T, forward: bool) -> T {
            let n = list.len();
            match list.iter().position(|x| *x == cur) {
                Some(i) if forward => list[(i + 1) % n],
                Some(i) => list[(i + n - 1) % n],
                None => list[0],
            }
        }
        match row {
            ERow::ChdMode => {
                self.chd.mode = next(modes_for(ChdProfile::Hd).as_slice(), self.chd.mode, forward)
            }
            ERow::ChdHunk => {
                let cur = self.chd.custom.hunk_size;
                self.chd.custom.hunk_size = next(hunk_presets(ChdProfile::Hd), cur, forward);
            }
            ERow::ChdCodec(slot) => {
                let cur = self.chd.custom.codecs[slot];
                let choices = codec_choices(ChdProfile::Hd);
                self.chd.custom.codecs[slot] = next(choices.as_slice(), cur, forward);
            }
            _ => return false,
        }
        true
    }

    #[cfg(not(feature = "chd"))]
    fn change_chd(&mut self, _row: ERow, _forward: bool) -> bool {
        false
    }

    /// A digit typed on a Custom MiB row; `None` is Backspace.
    fn edigit(&mut self, c: Option<char>) -> bool {
        let ERow::SizeMib(i) = self.esel else {
            return false;
        };
        let Some(cfg) = self.ctl.export_settings_mut().configs.get_mut(i) else {
            return false;
        };
        let n = cfg.custom_size_mib;
        cfg.custom_size_mib = match c.and_then(|c| c.to_digit(10)) {
            Some(d) => n.saturating_mul(10).saturating_add(d),
            None if c.is_none() => n / 10,
            None => return false,
        };
        true
    }

    /// The CHD options Export carries, or why they cannot be used.
    #[cfg(feature = "chd")]
    fn chd_options(&self) -> Result<ChdOptions, String> {
        self.chd
            .validate(ChdProfile::Hd)
            .map_err(|e| format!("CHD options: {e}."))?;
        Ok(self.chd.effective(ChdProfile::Hd))
    }

    #[cfg(not(feature = "chd"))]
    fn chd_options(&self) -> Result<ChdOptions, String> {
        Ok(ChdOptions::defaults_for(ChdProfile::Hd))
    }

    fn start_export(&mut self) {
        match self.chd_options() {
            Ok(opts) => {
                self.window = None;
                self.act(InspectAction::StartExport(opts));
            }
            Err(e) => self.note = Some(e),
        }
    }

    /// Hand the image to the writer window, as the GUI's Physical Disk Export button does.
    fn physical_export(&mut self) {
        let Some(src) = self.ctl.physical_source() else {
            self.note = Some(
                "Physical Disk Export takes a raw image file only; backup folders, devices \
                 and Clonezilla images are not wired in yet."
                    .to_string(),
            );
            return;
        };
        self.window = None;
        self.act(InspectAction::CloseExport);
        self.win = WriterWindow::default();
        self.wact(DiskWriteAction::Open(src));
    }

    fn export_key(&mut self, code: KeyCode) {
        let row = self.esel;
        match code {
            KeyCode::Esc => {
                self.window = None;
                self.act(InspectAction::CloseExport);
            }
            KeyCode::Down => self.emove(1),
            KeyCode::Up | KeyCode::BackTab => self.emove(-1),
            KeyCode::Enter => match row {
                ERow::Export => self.start_export(),
                ERow::Physical => self.physical_export(),
                _ => self.emove(1),
            },
            KeyCode::Char('s') => self.start_export(),
            KeyCode::Char(' ') => {
                self.echange(row, true);
            }
            KeyCode::Left | KeyCode::Right => {
                self.echange(row, code == KeyCode::Right);
            }
            KeyCode::Backspace => {
                self.edigit(None);
            }
            KeyCode::Char(c) => {
                self.edigit(Some(c));
            }
            _ => {}
        }
        if !self.erows().contains(&self.esel) {
            self.esel = ERow::Format;
        }
    }

    // --- the other windows --------------------------------------------------------------

    fn check_key(&mut self, code: KeyCode) {
        let (repairable, has_debug, debug) = match self.ctl.fsck_view() {
            Some(v) => (
                v.result.repairable,
                v.result.warnings.iter().any(|w| w.debug),
                v.show_debug,
            ),
            None => return,
        };
        match code {
            KeyCode::Esc => {
                self.scroll = 0;
                self.act(InspectAction::CloseCheck);
            }
            KeyCode::Char('r') if repairable => self.act(InspectAction::Repair),
            KeyCode::Char('d') if has_debug => self.act(InspectAction::ShowCheckDebug(!debug)),
            KeyCode::Up | KeyCode::Char('k') => self.scroll = self.scroll.saturating_sub(1),
            KeyCode::Down | KeyCode::Char('j') => self.scroll += 1,
            _ => {}
        }
    }

    fn chd_info_key(&mut self, code: KeyCode) {
        match code {
            KeyCode::Esc => {
                self.scroll = 0;
                self.act(InspectAction::CloseChdInfo);
            }
            KeyCode::Up | KeyCode::Char('k') => self.scroll = self.scroll.saturating_sub(1),
            KeyCode::Down | KeyCode::Char('j') => self.scroll += 1,
            _ => {}
        }
    }

    #[cfg(feature = "remote")]
    fn remote_key(&mut self, code: KeyCode) {
        let rows = self.remote.rows();
        match code {
            KeyCode::Esc => {
                self.window = None;
                self.ract(RemotePickAction::Disconnect);
            }
            KeyCode::Up | KeyCode::Char('k') => self.remote_sel = self.remote_sel.saturating_sub(1),
            KeyCode::Down | KeyCode::Char('j') => {
                self.remote_sel = (self.remote_sel + 1).min(rows.len().saturating_sub(1))
            }
            KeyCode::Backspace => {
                self.remote_sel = 0;
                self.ract(RemotePickAction::Up);
            }
            KeyCode::Char('c') => self.ract(RemotePickAction::Connect),
            KeyCode::Enter => {
                let Some(row) = rows.get(self.remote_sel).cloned() else {
                    return;
                };
                if row.parent {
                    self.remote_sel = 0;
                    self.ract(RemotePickAction::Up);
                } else if row.is_dir {
                    self.remote_sel = 0;
                    self.ract(RemotePickAction::Enter(row.name));
                } else if row.openable {
                    self.ract(RemotePickAction::OpenImage(row.path));
                } else {
                    self.note = Some(format!("{} is not a disk image.", row.name));
                }
            }
            _ => {}
        }
    }

    #[cfg(not(feature = "remote"))]
    fn remote_key(&mut self, _code: KeyCode) {
        self.window = None;
    }

    /// `R`: connect to a daemon, or browse again on the open remote image's connection.
    fn open_remote(&mut self) {
        #[cfg(feature = "remote")]
        {
            self.remote_sel = 0;
            self.window = Some(Win::Remote);
            match self.ctl.remote().cloned() {
                Some(image) => self.ract(RemotePickAction::BrowseOn(image)),
                None if self.remote.is_active() => {}
                None => self.ract(RemotePickAction::Connect),
            }
        }
        #[cfg(not(feature = "remote"))]
        {
            self.note = Some("This build has no remote support.".to_string());
        }
    }

    // --- the Inspect dialogs -------------------------------------------------------------

    fn table_key(&mut self, code: KeyCode) {
        let Some(view) = self.ctl.table_editor() else {
            return;
        };
        match self.twin.key(&view, code) {
            TableKey::Done => {}
            TableKey::Edit(edit) => self.act(InspectAction::Table(edit)),
            TableKey::Apply if view.can_apply => self.act(InspectAction::ApplyTableEdits),
            TableKey::Apply => {}
            TableKey::Close => self.act(InspectAction::CloseEditor),
        }
        if let Some(view) = self.ctl.table_editor() {
            self.twin.settle(&view);
        }
    }

    fn wizard_key(&mut self, code: KeyCode) {
        match self.iwin.key(&self.wizard, code) {
            WizardKey::Done => {}
            WizardKey::Act(a) => self.iact(a),
        }
        self.iwin.settle(&self.wizard);
    }

    fn open_resize_image(&mut self) {
        match self.ctl.resize_image_source() {
            Ok(source) => {
                self.iact(ResizeImageAction::Open(Box::new(source)));
                self.iwin = WizardWindow::default();
                self.iwin.settle(&self.wizard);
            }
            Err(e) => self.note = Some(e),
        }
    }

    fn resize_key(&mut self, code: KeyCode) {
        match self.rwin.key(&self.resize, code) {
            ResizeKey::Done => {}
            ResizeKey::Act(a) => self.zact(a),
        }
        self.rwin.settle(&self.resize);
    }

    /// A key in whichever of the small dialogs is up.
    fn dialog_key(&mut self, code: KeyCode) {
        let key = if self.hfs.is_open() {
            self.dwin.hfs_key(&self.hfs, code)
        } else {
            self.dwin.alto_key(&self.alto, code)
        };
        match key {
            DialogKey::Done => {}
            DialogKey::Alto(a) => self.aact(a),
            DialogKey::Hfs(a) => self.hact(a),
        }
    }

    /// The table editor's cursor is on a free-text field, where `?` is a character.
    fn typing(&self) -> bool {
        use super::table_edit::TRow;
        self.ctl.editor_open() && matches!(self.twin.sel, TRow::Type(_) | TRow::AddType)
    }

    /// A dialog of the screen's own is up over the grid.
    pub(super) fn dialog_open(&self) -> bool {
        self.ctl.editor_open()
            || self.resize.is_open()
            || self.wizard.is_open()
            || self.alto.is_open()
            || self.hfs.is_open()
    }

    fn open_table_editor(&mut self, add: bool) {
        self.twin = TableWindow::default();
        if add {
            self.act(InspectAction::AddPartition);
            self.twin.sel = super::table_edit::TRow::Add;
        } else {
            self.act(InspectAction::EditTable);
        }
    }

    fn open_resize(&mut self) {
        if let Some(plan) = self.ctl.resize_plan() {
            self.zact(ResizeAction::Open(Box::new(plan)));
            self.rwin.reset(&self.resize);
        }
    }

    // --- the grid -----------------------------------------------------------------------

    /// One key on the grid; `false` lets it fall through (tab switching, quit, open).
    fn grid_key(&mut self, code: KeyCode) -> bool {
        let rows = self.row_count();
        let part = self.ctl.partitions().get(self.sel).cloned();
        let actions = part.as_ref().map(|p| self.ctl.row_actions(p));
        let locked = self.ctl.rows_locked();
        match code {
            KeyCode::Up | KeyCode::Char('k') => self.sel = self.sel.saturating_sub(1),
            KeyCode::Down | KeyCode::Char('j') => {
                self.sel = (self.sel + 1).min(rows.saturating_sub(1))
            }
            KeyCode::Home | KeyCode::Char('g') => self.sel = 0,
            KeyCode::End | KeyCode::Char('G') => self.sel = rows.saturating_sub(1),
            KeyCode::Enter | KeyCode::Char('b') => match (self.selected(), actions) {
                (Some(i), Some(a)) if a.browse && !locked => self.act(InspectAction::Browse(i)),
                (Some(_), _) => self.note = Some("This partition cannot be browsed.".into()),
                (None, _) if self.plain.is_some() => {
                    self.note = Some(
                        "Read without elevation: browsing and the disk tools need the \
                         elevated open (re-run elevated, or answer the system prompt)."
                            .into(),
                    )
                }
                (None, _) => {}
            },
            KeyCode::Char('c') => match (self.selected(), actions) {
                (Some(i), Some(a)) if a.check && !locked => self.act(InspectAction::Check(i)),
                _ => self.note = Some("This partition has no filesystem check.".into()),
            },
            KeyCode::Char('m') => match (self.selected(), part) {
                (Some(i), Some(p)) if self.ctl.min_cell(&p) == MinCell::CanCalc => {
                    self.act(InspectAction::CalcMin(i))
                }
                _ => self.note = Some("No minimum to calculate for this partition.".into()),
            },
            KeyCode::Char('d') => match (self.selected(), actions) {
                (Some(i), Some(a)) if a.defragment => self.act(InspectAction::Defragment(i)),
                _ => {
                    self.note =
                        Some("Defragment takes a Human68k partition of an image file.".into())
                }
            },
            KeyCode::Char('r') if self.ctl.can_reinspect() => {
                self.plain = None;
                self.plain_for = None;
                self.act(InspectAction::Reinspect);
            }
            KeyCode::Char('e') if self.ctl.can_export() => {
                self.act(InspectAction::OpenExport);
                self.window = Some(Win::Export);
                self.esel = ERow::Format;
            }
            KeyCode::Char('x') if self.ctl.can_resize_image() && !self.wizard.is_running() => {
                self.open_resize_image()
            }
            KeyCode::Char('B') if self.ctl.can_make_bootable() => {
                self.act(InspectAction::MakeBootable)
            }
            KeyCode::Char('w')
                if self
                    .ctl
                    .table_summary()
                    .is_some_and(|t| t.byte_order.is_some())
                    && !self.ctl.swabbing() =>
            {
                self.act(InspectAction::SwapWordOrder)
            }
            KeyCode::Char('i') if self.ctl.chd_path().is_some() => {
                self.scroll = 0;
                self.act(InspectAction::ChdInfo)
            }
            KeyCode::Char('u')
                if self.ctl.is_remote()
                    && !self.ctl.partitions().is_empty()
                    && !self.ctl.remote_backup_running() =>
            {
                self.act(InspectAction::BackupRemote)
            }
            KeyCode::Char('R') if InspectController::remote_available() => self.open_remote(),
            KeyCode::Char('t') if self.ctl.can_edit_table() => self.open_table_editor(false),
            KeyCode::Char('a') if self.ctl.can_edit_table() => {
                if self.ctl.can_add_partition() {
                    self.open_table_editor(true);
                } else {
                    self.note = Some(self.ctl.add_partition_hint());
                }
            }
            KeyCode::Char('s') if self.ctl.can_edit_table() && !self.resize.is_running() => {
                self.open_resize()
            }
            KeyCode::Char('H') => match (self.selected(), actions) {
                (Some(i), Some(a)) if a.expand_hfs => self.act(InspectAction::ExpandHfs(i)),
                _ => self.note = Some("Expand HFS takes a classic HFS partition.".into()),
            },
            KeyCode::Char('A') => match actions {
                Some(a) if a.resize_alto => match self.ctl.alto_resize_source() {
                    Ok(path) => {
                        self.dwin.reset(DRow::AltoTarget);
                        self.aact(AltoResizeAction::Open(path));
                    }
                    Err(e) => self.note = Some(e),
                },
                _ => self.note = Some("Resize Alto Disk takes an Alto pack image.".into()),
            },
            KeyCode::Char('f') => match self.ctl.convert_source() {
                Some(path) => self.convert = Some(path),
                None => self.note = Some("Convert takes an image file.".into()),
            },
            // A tool that does not apply here says so rather than reaching the disk list's keys.
            KeyCode::Char(c) if "rexBwiuRtas".contains(c) => {
                self.note = Some("That tool does not apply to this source.".into())
            }
            KeyCode::Esc => {
                if self.ctl.export_running() {
                    self.act(InspectAction::CancelExport);
                } else if self.ctl.is_remote() && !self.ctl.remote_backup_running() {
                    self.act(InspectAction::Unload { quiet: true });
                } else if self.ctl.can_close() {
                    self.act(InspectAction::Close);
                }
            }
            _ => return false,
        }
        true
    }
}

impl App {
    /// The Inspect screen's name and key list, for the `?` window; `None` on the disk list.
    pub(super) fn inspect_key_context(&self) -> Option<(&'static str, &'static [KeyHint])> {
        let s = self.inspect.as_ref()?;
        if s.writer.is_open() {
            return Some(("Inspect - Physical Disk Export", HINTS_WRITER));
        }
        match s.window {
            Some(Win::Export) => return Some(("Inspect - export", HINTS_INSPECT_EXPORT)),
            Some(_) => return Some(("Inspect - window", HINTS_INSPECT_WINDOW)),
            None => {}
        }
        if s.ctl.fsck_view().is_some() || s.ctl.chd_info_text().is_some() {
            return Some(("Inspect - window", HINTS_INSPECT_WINDOW));
        }
        if s.ctl.editor_open() {
            return Some(("Inspect - partition table", HINTS_TABLE_EDIT));
        }
        if s.resize.is_open() {
            return Some(("Inspect - resize", HINTS_RESIZE));
        }
        if s.wizard.is_open() {
            return Some(("Inspect - resize image", HINTS_RESIZE_IMAGE));
        }
        if s.hfs.is_open() {
            return Some(("Inspect - expand HFS", HINTS_EXPAND_HFS));
        }
        if s.alto.is_open() {
            return Some(("Inspect - resize Alto", HINTS_ALTO_RESIZE));
        }
        s.source_open()
            .then_some(("Inspect - partitions", HINTS_INSPECT_SOURCE))
    }

    /// The footer's short key strip while a source is open.
    pub(super) fn inspect_footer_keys(&self) -> Option<Vec<(&'static str, &'static str)>> {
        let s = self.inspect.as_ref()?;
        if s.ask.is_some() {
            return Some(vec![("y/n", "Answer"), ("Esc", "Cancel")]);
        }
        if s.writer.is_open() {
            return Some(vec![
                ("Up/Dn", "Row"),
                ("<-/->", "Change"),
                ("Enter", "Act"),
                ("w", "Write"),
                ("Esc", "Close"),
            ]);
        }
        if s.window == Some(Win::Export) {
            return Some(vec![
                ("Up/Dn", "Row"),
                ("<-/->", "Change"),
                ("s", "Export"),
                ("Esc", "Close"),
                ("?", "Keys"),
            ]);
        }
        if s.window.is_some() || s.ctl.fsck_view().is_some() || s.ctl.chd_info_text().is_some() {
            return Some(vec![("Enter", "Act"), ("Esc", "Close"), ("?", "Keys")]);
        }
        if s.dialog_open() {
            return Some(vec![
                ("Up/Dn", "Row"),
                ("<-/->", "Change"),
                ("Enter", "Act"),
                ("Esc", "Close"),
                ("?", "Keys"),
            ]);
        }
        if !s.source_open() {
            return None;
        }
        Some(vec![
            ("Up/Dn", "Select"),
            ("Enter", "Browse"),
            ("c", "Check"),
            ("e", "Export"),
            ("Esc", "Close"),
            ("?", "Keys"),
        ])
    }

    fn inspect_state(&mut self) -> &mut InspectState {
        if self.inspect.is_none() {
            self.inspect = Some(InspectState::new(self.shell.recent_handle()));
        }
        self.inspect.as_mut().unwrap()
    }

    /// Open a path from the picker: a folder is a backup or Clonezilla image, a listed disk a
    /// device, anything else an image file (a wrapper is decoded in the controller's job).
    pub(super) fn inspect_open_path(&mut self, path: PathBuf) {
        let slot = self.shell.devices().iter().position(|d| d.path == path);
        if let Some(slot) = slot {
            self.inspect_open_device(slot);
            return;
        }
        let source = if path.is_dir() {
            InspectSource::Backup {
                folder: path.clone(),
            }
        } else {
            InspectSource::ImageFile(path.clone())
        };
        let s = self.inspect_state();
        s.recent
            .remember(RecentMode::Inspect, path.display().to_string().as_str());
        s.disk = None;
        s.plain = None;
        s.plain_for = None;
        s.sel = 0;
        s.act(InspectAction::Open(source));
        self.after_inspect();
    }

    /// Open a payload the Archives screen wrote; `guard` keeps its temp folder alive.
    pub(super) fn inspect_open_payload(&mut self, path: PathBuf, guard: tempfile::TempDir) {
        let s = self.inspect_state();
        s.disk = None;
        s.plain = None;
        s.plain_for = None;
        s.sel = 0;
        s.act(InspectAction::Open(InspectSource::Image {
            path,
            temp: Some(guard),
        }));
        self.after_inspect();
    }

    /// Open the backup folder Restore shares (not recorded in the recent list, as the GUI's).
    pub(super) fn inspect_load_backup(&mut self, folder: PathBuf) {
        let s = self.inspect_state();
        if s.ctl.backup_folder() == Some(folder.as_path()) {
            return;
        }
        s.disk = None;
        s.plain = None;
        s.plain_for = None;
        s.sel = 0;
        s.act(InspectAction::Open(InspectSource::Backup { folder }));
        self.after_inspect();
    }

    /// Open the disk at `slot` of the device list through the controller.
    pub(super) fn inspect_open_device(&mut self, slot: usize) {
        let Some(device) = self.shell.devices().get(slot).cloned() else {
            return;
        };
        let s = self.inspect_state();
        s.disk = Some(Box::new(device.clone()));
        s.plain = None;
        s.plain_for = None;
        s.sel = 0;
        s.act(InspectAction::Open(InspectSource::Device {
            slot,
            identity: device.identity(),
        }));
        self.after_inspect();
    }

    /// Inspect keys. Returns `true` when consumed; the disk list falls through to the shell.
    pub(super) fn handle_inspect_key(&mut self, code: KeyCode) -> bool {
        let ndisks = self.shell.devices().len();
        let devices = self.shell.devices().to_vec();
        let s = self.inspect_state();
        if s.ask.is_some() {
            if let Some((yes, text)) = s.ask.as_mut().and_then(|a| a.key(code)) {
                s.answer(yes, text);
            }
            self.after_inspect();
            return true;
        }
        if let Some((pick, picker)) = s.picker.as_mut() {
            let pick = *pick;
            match picker.handle_key(code) {
                Some(PickResult::Cancel) => {
                    s.picker = None;
                    s.picked(pick, None);
                }
                Some(PickResult::Confirm(path)) => {
                    s.picker = None;
                    s.picked(pick, Some(path));
                }
                None => {}
            }
            self.after_inspect();
            return true;
        }
        if s.device_pick {
            match code {
                KeyCode::Up | KeyCode::Char('k') => s.device_sel = s.device_sel.saturating_sub(1),
                KeyCode::Down | KeyCode::Char('j') => {
                    s.device_sel = (s.device_sel + 1).min(ndisks.saturating_sub(1))
                }
                KeyCode::Esc => s.device_pick = false,
                KeyCode::Enter => {
                    let row = s.device_sel;
                    if let Some(device) = devices.get(row).cloned() {
                        s.device_pick = false;
                        s.win.image.clear();
                        s.wact(DiskWriteAction::SelectDevice { row, device });
                    }
                }
                _ => {}
            }
            return true;
        }
        // `?` and F1 reach the key list from every window, unless `?` is typed into a field.
        if code == KeyCode::F(1) || (code == KeyCode::Char('?') && !s.typing()) {
            return false;
        }
        if s.writer.is_open() {
            if s.writer.is_writing() {
                if code == KeyCode::Esc {
                    s.wact(DiskWriteAction::Cancel);
                }
                return true;
            }
            match s.win.key(&s.writer, code) {
                WriterKey::Done => {}
                WriterKey::Act(a) => s.wact(a),
                WriterKey::PickDevice => {
                    s.device_sel = s.writer.target_row().unwrap_or(0);
                    s.device_pick = true;
                }
                WriterKey::BrowseImage => {
                    let mut p = FilePicker::new(PickKind::Save, "Write the image to this file");
                    p.input = s.win.image.clone();
                    s.picker = Some((Pick::WriterImage, p));
                }
                WriterKey::Write => {
                    s.writing = true;
                    s.wact(DiskWriteAction::Write);
                }
                WriterKey::Close => s.wact(DiskWriteAction::Close),
            }
            s.win.settle(&s.writer);
            return true;
        }
        match s.window {
            Some(Win::Remote) => {
                s.remote_key(code);
                self.after_inspect();
                return true;
            }
            Some(Win::Export) => {
                s.export_key(code);
                return true;
            }
            None => {}
        }
        if s.ctl.fsck_view().is_some() {
            s.check_key(code);
            return true;
        }
        if s.ctl.chd_info_text().is_some() {
            s.chd_info_key(code);
            return true;
        }
        if s.ctl.editor_open() {
            s.table_key(code);
            self.after_inspect();
            return true;
        }
        if s.resize.is_open() {
            s.resize_key(code);
            self.after_inspect();
            return true;
        }
        if s.wizard.is_open() {
            s.wizard_key(code);
            self.after_inspect();
            return true;
        }
        if s.hfs.is_open() || s.alto.is_open() {
            s.dialog_key(code);
            self.after_inspect();
            return true;
        }
        if !s.source_open() {
            // The disk list: only the remote window is the screen's own.
            if code == KeyCode::Char('R') && InspectController::remote_available() {
                s.open_remote();
                return true;
            }
            return false;
        }
        let consumed = s.grid_key(code);
        self.after_inspect();
        consumed
    }

    /// Drive the controllers: jobs, the device list, the hand-offs and the plain-read fallback.
    pub(super) fn inspect_tick(&mut self) {
        let devices = self.shell.devices().to_vec();
        let Some(s) = self.inspect.as_mut() else {
            return;
        };
        let dirty = self.explorer.as_ref().is_some_and(|e| e.has_staged_edits());
        s.ctl.set_browse_dirty(dirty);
        s.ctl.reconcile_devices(devices.as_slice());
        s.writer.reconcile_devices(devices.as_slice());
        s.resize.reconcile_devices(devices.as_slice());
        let fx = s.ctl.tick();
        s.absorb(Who::Inspect, fx);
        let fx = s.writer.tick();
        s.absorb(Who::Writer, fx);
        #[cfg(feature = "remote")]
        {
            let fx = s.remote.tick();
            s.absorb(Who::Remote, fx);
        }
        let fx = s.resize.tick();
        s.absorb(Who::Resize, fx);
        let fx = s.wizard.tick();
        s.absorb(Who::Wizard, fx);
        s.iwin.settle(&s.wizard);
        let fx = s.alto.tick();
        s.absorb(Who::Alto, fx);
        let fx = s.hfs.tick();
        s.absorb(Who::Hfs, fx);
        s.after();
        if s.writing && !s.writer.is_writing() {
            s.writing = false;
            s.note = match s.writer.result() {
                Some(Ok(n)) => Some(format!("Wrote {} to the target.", format_size(*n))),
                Some(Err(e)) => Some(format!("Image write failed: {e}")),
                None => None,
            };
            s.wact(DiskWriteAction::Close);
        }
        // The disk's facts and its plain read belong to the open device.
        if s.ctl.device_path().is_none() {
            s.disk = None;
            s.plain = None;
            s.plain_for = None;
        }
        s.layout = s.ctl.layout();
        self.after_inspect();
        self.inspect_plain_fallback();
    }

    /// A disk the controller could not open is read again without elevation, listed read-only.
    fn inspect_plain_fallback(&mut self) {
        let Some(s) = self.inspect.as_mut() else {
            return;
        };
        let Some(path) = s.ctl.device_path().map(|p| p.to_path_buf()) else {
            return;
        };
        let failed = s.ctl.loading().is_none()
            && s.ctl.last_error().is_some()
            && s.ctl.partitions().is_empty();
        if !failed || s.plain_for.as_deref() == Some(path.as_path()) {
            return;
        }
        s.plain_for = Some(path.clone());
        let size = s.disk.as_ref().map_or(0, |d| d.size_bytes);
        self.start_task("Reading the partition table", move |job| {
            job.set_can_cancel(false);
            let rows = super::parse_partitions(path.as_path(), size);
            Ok(Box::new(move |app: &mut App| {
                let Some(s) = app.inspect.as_mut() else {
                    return;
                };
                match rows {
                    Ok(rows) => s.plain = Some(rows),
                    Err(e) => {
                        s.plain = Some(Vec::new());
                        s.note = Some(App::access_hint(path.display().to_string().as_str(), &e));
                    }
                }
            }) as super::TaskDone)
        });
    }

    /// Carry out what the screen handed over: the Explorer, a seekable reopen, an archive.
    fn after_inspect(&mut self) {
        let Some(s) = self.inspect.as_mut() else {
            return;
        };
        if std::mem::take(&mut s.close_explorer) {
            if let Some(ex) = self.explorer.take() {
                ex.release();
            }
        }
        let browse = s.browse.take();
        let upgrade = s.upgrade.take();
        let archive = s.archive.take();
        let optical = s.optical.take();
        let convert = s.convert.take();
        if let Some((open, label)) = browse {
            self.explorer = Some(super::commander::CommanderView::explorer_open(*open, label));
            self.status = None;
        }
        if let Some(path) = upgrade {
            if let Some(ex) = self.explorer.as_mut() {
                ex.upgrade_seekable(path);
            }
        }
        if let Some(path) = archive {
            if let Some(idx) = super::TABS
                .iter()
                .position(|t| t.0 == super::TabId::Archives)
            {
                self.active = idx;
                self.scroll = 0;
                self.on_tab_changed();
                self.load_archive(path);
            }
        }
        // As the GUI does: the disc opens on the Optical tab.
        #[cfg(feature = "optical")]
        if let Some(path) = optical {
            if let Some(idx) = super::TABS
                .iter()
                .position(|t| t.0 == super::TabId::Optical)
            {
                self.active = idx;
                self.scroll = 0;
                self.on_tab_changed();
                self.optical_open_image(path);
            }
        }
        #[cfg(not(feature = "optical"))]
        let _ = optical;
        if let Some(path) = convert {
            if let Some(idx) = super::TABS
                .iter()
                .position(|t| t.0 == super::TabId::Convert)
            {
                self.active = idx;
                self.scroll = 0;
                self.on_tab_changed();
                self.convert_open(path);
            }
        }
    }

    // --- drawing ------------------------------------------------------------------------

    pub(super) fn draw_inspect(&self, frame: &mut Frame, area: Rect, s: &InspectState) {
        let area = match s.disk.as_deref() {
            Some(d) => {
                let header = self.disk_header_lines(d);
                let height = (header.len() as u16 + 2).min(area.height.saturating_sub(6));
                let rows =
                    Layout::vertical([Constraint::Length(height), Constraint::Min(3)]).split(area);
                frame.render_widget(
                    Paragraph::new(Text::from(header)).block(
                        self.pane_block(format!("Disk  {}", d.path.display()).as_str(), false),
                    ),
                    rows[0],
                );
                rows[1]
            }
            None => area,
        };
        let mut lines: Vec<Line<'static>> = Vec::new();
        let focus_line = self.inspect_lines(s, &mut lines);
        let inner = area.height.saturating_sub(2) as usize;
        let scroll = if inner > 2 && focus_line + 3 > inner {
            (focus_line + 3 - inner) as u16
        } else {
            0
        };
        let title = s.ctl.source_label(self.shell.devices());
        frame.render_widget(
            Paragraph::new(Text::from(lines))
                .block(self.pane_block(format!("Inspect  {title}").as_str(), true))
                .scroll((scroll, 0)),
            area,
        );
        self.draw_inspect_windows(frame, area, s);
    }

    /// The windows over the grid; also drawn over the disk list (the remote window).
    pub(super) fn draw_inspect_windows(&self, frame: &mut Frame, area: Rect, s: &InspectState) {
        match s.window {
            Some(Win::Export) => self.draw_inspect_export(frame, area, s),
            Some(Win::Remote) => self.draw_inspect_remote(frame, area, s),
            None => {}
        }
        if s.ctl.fsck_view().is_some() {
            self.draw_inspect_check(frame, area, s);
        }
        if let Some(text) = s.ctl.chd_info_text() {
            let lines: Vec<Line<'static>> = text
                .lines()
                .skip(s.scroll)
                .map(|l| Line::raw(format!(" {l}")))
                .collect();
            let popup = centered_rect(
                area.width.saturating_sub(4).min(100),
                area.height.saturating_sub(2),
                area,
            );
            frame.render_widget(Clear, popup);
            frame.render_widget(
                Paragraph::new(Text::from(lines))
                    .block(self.pane_block("CHD Info  (Up/Down scroll, Esc close)", true)),
                popup,
            );
        }
        if let Some(view) = s.ctl.table_editor() {
            let target = s.ctl.table_write_target();
            self.draw_table_editor(frame, area, &view, &s.twin, target.as_deref());
        }
        if s.resize.is_open() {
            self.draw_resize(frame, area, &s.resize, &s.rwin);
        }
        if s.wizard.is_open() {
            self.draw_resize_image(frame, area, &s.wizard, &s.iwin);
        }
        if s.alto.is_open() {
            self.draw_alto_resize(frame, area, &s.alto, &s.dwin);
        }
        if s.hfs.is_open() {
            self.draw_expand_hfs(frame, area, &s.hfs, &s.dwin);
        }
        if s.writer.is_open() {
            self.draw_writer(frame, area, "Physical Disk Export", &s.writer, &s.win);
        }
        if let Some(snap) = s.resize.progress() {
            self.draw_job_box(frame, area, "Resizing (Esc asks to cancel)", &snap);
        }
        for snap in s.ctl.tool_jobs() {
            self.draw_job_box(frame, area, snap.label.as_str(), &snap);
        }
        if let Some(snap) = s.ctl.export_job() {
            self.draw_job_box(frame, area, "Export", &snap);
        }
        if let Some(snap) = s.writer.progress().filter(|_| s.writer.is_writing()) {
            self.draw_job_box(frame, area, "Writing image", &snap);
        }
        if let Some((_, picker)) = &s.picker {
            picker.draw(frame, area, self.palette, self.border);
        }
        if s.device_pick {
            self.draw_device_pick(frame, area, s.device_sel, "Pick a target device");
        }
        if let Some(ask) = &s.ask {
            self.draw_ask(frame, area, ask, s.writer.is_open());
        }
    }

    /// The grid's lines; returns the selected row's line.
    fn inspect_lines(&self, s: &InspectState, lines: &mut Vec<Line<'static>>) -> usize {
        let pal = self.palette;
        let c = &s.ctl;
        let kv = |k: &str, v: String| {
            Line::from(vec![
                Span::styled(format!("{k:<17}"), pal.accent()),
                Span::raw(v),
            ])
        };
        if InspectController::needs_elevation_note() && s.disk.is_some() {
            lines.push(Line::styled(
                "Note: a disk needs elevation (macOS asks; elsewhere re-run elevated).",
                pal.warn(),
            ));
        }
        if let Some(report) = c.flux_faults() {
            lines.push(Line::styled(
                format!(
                    "Warning: flux decode: {}. Files using these sectors are damaged.",
                    report.summary
                ),
                pal.warn(),
            ));
        }
        if let Some(snap) = c.loading() {
            let what = if snap.phase.is_empty() {
                snap.label
            } else {
                snap.phase
            };
            lines.push(Line::styled(format!("{what}..."), pal.dim()));
        }
        if let Some(err) = c.last_error() {
            lines.push(Line::styled(format!("Error: {err}"), pal.warn()));
        }
        if let Some(note) = s.note.as_ref().or(self.status.as_ref()) {
            lines.push(Line::styled(note.clone(), pal.warn()));
        }
        if let Some(m) = c.backup_summary() {
            lines.push(kv("Backup of:", m.source.clone()));
            lines.push(kv("Created:", m.created.clone()));
            lines.push(kv(
                "Format:",
                format!("{}  checksum {}", m.compression, m.checksum),
            ));
            if m.source_size_bytes > 0 {
                lines.push(kv("Source size:", format_size(m.source_size_bytes)));
            }
            if m.min_restore_bytes > 0 {
                lines.push(kv("Min restore size:", format_size(m.min_restore_bytes)));
            }
            if m.sector_by_sector {
                lines.push(Line::styled(
                    "Sector-by-sector backup (free space kept byte for byte).",
                    pal.dim(),
                ));
            }
        }
        if let Some(cz) = c.clonezilla_summary() {
            lines.push(kv("Clonezilla disk:", cz.disk_name.clone()));
            if cz.source_size_bytes > 0 {
                lines.push(kv("Source size:", format_size(cz.source_size_bytes)));
            }
            if let Some((cyl, heads, spt)) = cz.chs {
                lines.push(kv("CHS:", format!("{cyl} / {heads} / {spt}")));
            }
            if let Some(created) = cz.created.clone() {
                lines.push(kv("Created:", created));
            }
        }
        if let Some(t) = c.table_summary() {
            lines.push(kv(
                "Partition table:",
                if t.superfloppy {
                    "None (superfloppy)".to_string()
                } else {
                    format!(
                        "{}  (disk signature 0x{:08X})",
                        t.type_name, t.disk_signature
                    )
                },
            ));
            if let Some(order) = t.byte_order {
                let tail = if c.swabbing() {
                    "  swapping..."
                } else {
                    "  w swaps the word order"
                };
                lines.push(kv("Word order:", format!("{order} (16-bit words){tail}")));
            }
        }
        if let Some(a) = c.alignment() {
            let chs = if a.heads > 0 {
                format!(
                    "  CHS {} heads x {} sectors/track",
                    a.heads, a.sectors_per_track
                )
            } else {
                String::new()
            };
            lines.push(kv(
                "Alignment:",
                format!("{}  first LBA {}{chs}", a.kind, a.first_lba),
            ));
        }
        let layout = self.inspect_layout_line(s);
        if !layout.is_empty() {
            lines.push(kv("Layout:", layout));
        }
        lines.push(Line::raw(""));
        let focus = self.inspect_grid(s, lines);
        for snap in c.browse_jobs() {
            lines.push(Line::styled(
                format!("{}  {}", snap.label, snap.detail()),
                pal.dim(),
            ));
        }
        if c.remote_backup_running() {
            lines.push(Line::styled("Backing up the remote image...", pal.dim()));
        }
        if c.summarizing_hfs() {
            lines.push(Line::styled("Reading the HFS volume...", pal.dim()));
        }
        lines.push(Line::raw(""));
        for msg in &s.log {
            lines.push(Line::styled(format!("  {msg}"), pal.dim()));
        }
        lines.push(Line::styled(self.inspect_tools_line(s), pal.dim()));
        focus
    }

    /// The disk layout as one text line: each volume, then the trailing free space.
    fn inspect_layout_line(&self, s: &InspectState) -> String {
        s.layout
            .iter()
            .map(|seg| match seg.kind {
                SegmentKind::Free => format!("[free {}]", format_size(seg.size_bytes)),
                SegmentKind::Dimmed => format!("({} {})", seg.label, format_size(seg.size_bytes)),
                SegmentKind::Partition { .. } => {
                    format!("[{} {} {}]", seg.label, seg.fs, format_size(seg.size_bytes))
                }
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// The partition grid; returns the selected row's line.
    fn inspect_grid(&self, s: &InspectState, lines: &mut Vec<Line<'static>>) -> usize {
        let pal = self.palette;
        let c = &s.ctl;
        let mut focus = lines.len();
        if c.partitions().is_empty() {
            match &s.plain {
                Some(rows) if !rows.is_empty() => {
                    lines.push(Line::styled(
                        "Read without elevation (no tools; re-run elevated for them):",
                        pal.warn(),
                    ));
                    for (i, r) in rows.iter().enumerate() {
                        let sel = i == s.sel;
                        if sel {
                            focus = lines.len();
                        }
                        let style = if sel {
                            pal.accent().add_modifier(Modifier::REVERSED)
                        } else {
                            pal.accent()
                        };
                        lines.push(Line::styled(
                            format!(
                                "{}{:<24} {:>10}  {}",
                                if sel { "> " } else { "  " },
                                r.label,
                                format_size(r.size),
                                r.fs_hint
                            ),
                            style,
                        ));
                    }
                }
                _ if c.loading().is_none() && !c.is_preparing() => {
                    lines.push(Line::raw("No partitions found."))
                }
                _ => {}
            }
            return focus;
        }
        let has_slots = c.table_summary().is_some_and(|t| t.has_slots);
        let mins = c.shows_min_columns();
        let slot_head = if has_slots { "Slot  " } else { "" };
        let min_head = if mins { "  Min size" } else { "" };
        lines.push(Line::styled(
            format!(
                "  {:<4}{slot_head}{:<16} {:<10} {:>10} {:>9}{min_head}",
                "#", "Type", "Volume", "Start LBA", "Size"
            ),
            pal.dim().add_modifier(Modifier::BOLD),
        ));
        for (pos, part) in c.partitions().iter().enumerate() {
            let sel = pos == s.sel;
            if sel {
                focus = lines.len();
            }
            let num = if part.is_logical {
                format!("{}L", pos + 1)
            } else if part.is_extended_container {
                format!("{}E", pos + 1)
            } else {
                format!("{}", pos + 1)
            };
            let slot = if has_slots {
                let slot: String = c.slot(part).chars().take(5).collect();
                format!("{slot:<6}")
            } else {
                String::new()
            };
            let min = if mins {
                match c.min_cell(part) {
                    MinCell::Size(sz) => format!("  {:>9}", format_size(sz)),
                    MinCell::Pending(phase) => format!("  {phase}"),
                    MinCell::CanCalc => "  m=Calc min".to_string(),
                    MinCell::Empty => String::new(),
                }
            } else {
                String::new()
            };
            let type_name: String = part.type_name.chars().take(16).collect();
            let volume: String = c
                .volume_label(part.index)
                .unwrap_or("")
                .chars()
                .take(10)
                .collect();
            let style = if sel {
                pal.accent().add_modifier(Modifier::REVERSED)
            } else if part.is_extended_container {
                pal.dim()
            } else {
                pal.accent()
            };
            lines.push(Line::styled(
                format!(
                    "{}{num:<4}{slot}{type_name:<16} {volume:<10} {:>10} {:>9}{min}",
                    if sel { "> " } else { "  " },
                    part.start_lba,
                    format_size(part.size_bytes),
                ),
                style,
            ));
            if sel {
                lines.push(Line::styled(self.inspect_row_detail(s, pos), pal.dim()));
            }
        }
        focus
    }

    /// The selected row's extras: defragmented minimum, block size, boot flag, and its keys.
    fn inspect_row_detail(&self, s: &InspectState, pos: usize) -> String {
        let c = &s.ctl;
        let Some(part) = c.partitions().get(pos) else {
            return String::new();
        };
        let mut out = String::from("      ");
        if let Some(d) = c.defrag_min(part) {
            out.push_str(format!("Defrag min {}  ", format_size(d)).as_str());
        }
        if let Some(bs) = part.hfs_block_size {
            out.push_str(format!("Block {}  ", format_size(u64::from(bs))).as_str());
        }
        if part.bootable {
            out.push_str("Boot  ");
        }
        let a = c.row_actions(part);
        let mut keys = Vec::new();
        if a.browse && !c.rows_locked() {
            keys.push("Enter browse");
        }
        if a.check {
            keys.push("c check");
        }
        if c.min_cell(part) == MinCell::CanCalc {
            keys.push("m Calc min");
        }
        if a.defragment {
            keys.push("d defragment");
        }
        if a.expand_hfs {
            keys.push("H expand HFS");
        }
        if a.resize_alto {
            keys.push("A resize Alto");
        }
        if !keys.is_empty() {
            out.push_str(&keys.join("  "));
        }
        out
    }

    /// The disk tools that apply now.
    fn inspect_tools_line(&self, s: &InspectState) -> String {
        let c = &s.ctl;
        let mut keys: Vec<&str> = Vec::new();
        if c.can_reinspect() {
            keys.push("r re-inspect");
        }
        if c.can_export() {
            keys.push("e export");
        }
        if c.can_expand_image() {
            keys.push("x expand image");
        }
        if c.can_make_bootable() {
            keys.push("B make bootable");
        }
        if c.can_edit_table() {
            keys.push("t edit table");
        }
        if s.can_add {
            keys.push("a add partition");
        }
        if c.can_edit_table() && !s.resize.is_running() {
            keys.push("s resize");
        }
        if c.convert_source().is_some() {
            keys.push("f convert");
        }
        if c.table_summary().is_some_and(|t| t.byte_order.is_some()) {
            keys.push("w swap words");
        }
        if c.chd_path().is_some() {
            keys.push("i CHD info");
        }
        if c.is_remote() {
            keys.push("u back up");
        }
        if InspectController::remote_available() {
            keys.push("R remote");
        }
        keys.push("o open");
        keys.push(if c.export_running() {
            "Esc cancel export"
        } else {
            "Esc close"
        });
        format!("Tools: {}", keys.join("  "))
    }

    fn draw_inspect_export(&self, frame: &mut Frame, area: Rect, s: &InspectState) {
        let pal = self.palette;
        let c = &s.ctl;
        let settings = c.export_settings();
        let style_for = |sel: bool| {
            if sel {
                pal.accent()
                    .add_modifier(Modifier::REVERSED | Modifier::BOLD)
            } else {
                pal.accent()
            }
        };
        let layout = c.export_layout();
        let bar = |segs: &[LayoutSegment]| {
            segs.iter()
                .map(|seg| format!("[{} {}]", seg.fs, format_size(seg.size_bytes)))
                .collect::<Vec<_>>()
                .join(" ")
        };
        let mut lines: Vec<Line<'static>> = vec![
            Line::styled(
                format!("  Current: {}", bar(layout.current.as_slice())),
                pal.dim(),
            ),
            Line::styled(
                format!("  After:   {}", bar(layout.after.as_slice())),
                pal.dim(),
            ),
            Line::raw(""),
        ];
        let mut focus = 0usize;
        let formats = c.export_formats();
        let format = formats
            .iter()
            .find(|(f, _)| f.format == settings.format)
            .map(|(f, _)| *f);
        for row in s.erows() {
            let sel = s.esel == row;
            if sel {
                focus = lines.len();
            }
            let m = if sel { "> " } else { "  " };
            let text = match row {
                ERow::Layout => format!(
                    "Layout:  < {} >",
                    if settings.whole_disk {
                        "Whole disk (single file)"
                    } else {
                        "Per partition (one file per partition)"
                    }
                ),
                ERow::Format => format!("Format:  < {} >", format.map_or("", |f| f.label)),
                ERow::ChdMode | ERow::ChdHunk | ERow::ChdCodec(_) => self.inspect_chd_row(s, row),
                ERow::Size(i) => match settings.configs.get(i) {
                    Some(cfg) => format!(
                        "#{} {:<18} < {:?} >  {}",
                        cfg.index,
                        cfg.type_name,
                        cfg.choice,
                        format_size(cfg.effective_size())
                    ),
                    None => String::new(),
                },
                ERow::SizeMib(i) => match settings.configs.get(i) {
                    Some(cfg) => {
                        let (lo, hi) = cfg.custom_bounds_mib();
                        format!(
                            "    MiB: {}{}  ({lo}-{hi})",
                            cfg.custom_size_mib,
                            if sel { "_" } else { "" }
                        )
                    }
                    None => String::new(),
                },
                ERow::Export => {
                    lines.push(Line::raw(""));
                    if sel {
                        focus = lines.len();
                    }
                    "[ Export... ]".to_string()
                }
                ERow::Physical => {
                    if c.can_export_physical() {
                        "[ Physical Disk Export... ]".to_string()
                    } else {
                        "[ Physical Disk Export... ]  (raw image files only)".to_string()
                    }
                }
            };
            lines.push(Line::styled(format!("{m}{text}"), style_for(sel)));
            if row == ERow::Layout {
                if let Some(why) = per_partition_refusal(settings.format) {
                    lines.push(Line::styled(format!("    {why}"), pal.dim()));
                }
            }
            if row == ERow::Format {
                if let Some(hint) = format.map(|f| f.hint).filter(|h| !h.is_empty()) {
                    lines.push(Line::styled(format!("    {hint}"), pal.dim()));
                }
                if !c.chd_export_allowed() {
                    lines.push(Line::styled(format!("    {CHD_REFUSAL}"), pal.dim()));
                }
            }
        }
        let notes = c.export_notes();
        if !notes.pfs3_shrink.is_empty() {
            lines.push(Line::styled(
                format!(
                    "  PFS3 shrink: volume will be rebuilt (trashcan lost): {}",
                    notes.pfs3_shrink.join(", ")
                ),
                pal.warn(),
            ));
        }
        if let Some(workflow) = notes.chd_workflow {
            lines.push(Line::styled(format!("  {workflow}"), pal.dim()));
            if let Some((total, scratch, output)) = notes.chd_estimate {
                lines.push(Line::styled(
                    format!(
                        "  Estimated peak new disk usage: {} MiB (scratch {} MiB + output {} \
                         MiB). Available space is checked when you pick a destination.",
                        total / (1024 * 1024),
                        scratch / (1024 * 1024),
                        output / (1024 * 1024)
                    ),
                    pal.dim(),
                ));
            }
            if notes.sector_by_sector_warning {
                lines.push(Line::styled(
                    "  Warning: the source is a sector-by-sector backup. Re-exporting with new \
                     partition sizes drops the byte-for-byte preservation of free space.",
                    pal.warn(),
                ));
            }
        }
        if let Some(note) = &s.note {
            lines.push(Line::styled(format!("  {note}"), pal.warn()));
        }
        self.draw_inspect_popup(frame, area, "Export Disk Image", lines, focus);
    }

    #[cfg(feature = "chd")]
    fn inspect_chd_row(&self, s: &InspectState, row: ERow) -> String {
        match row {
            ERow::ChdMode => format!("CHD:     < {} >", s.chd.mode.label()),
            ERow::ChdHunk => {
                let err = s
                    .chd
                    .validate(ChdProfile::Hd)
                    .err()
                    .map(|e| format!("  {e}"))
                    .unwrap_or_default();
                format!(
                    "  Hunk:  < {} >  (unit {} bytes){err}",
                    s.chd.custom.hunk_size,
                    unit_size_for(ChdProfile::Hd)
                )
            }
            ERow::ChdCodec(slot) => format!(
                "  Codec {}: < {} >",
                slot + 1,
                codec_label(s.chd.custom.codecs[slot])
            ),
            _ => String::new(),
        }
    }

    #[cfg(not(feature = "chd"))]
    fn inspect_chd_row(&self, _s: &InspectState, _row: ERow) -> String {
        String::new()
    }

    fn draw_inspect_check(&self, frame: &mut Frame, area: Rect, s: &InspectState) {
        let pal = self.palette;
        let Some(view) = s.ctl.fsck_view() else {
            return;
        };
        let r = view.result;
        let mut lines: Vec<Line<'static>> = Vec::new();
        if r.is_clean() {
            lines.push(Line::styled("  Filesystem is clean.", pal.accent()));
        } else {
            lines.push(Line::styled(
                format!("  {} error(s) found.", r.errors.len()),
                pal.warn(),
            ));
        }
        let mut stats = format!(
            "  Files: {}  Directories: {}",
            r.stats.files_checked, r.stats.directories_checked
        );
        for (label, value) in &r.stats.extra {
            stats.push_str(format!("  {label}: {value}").as_str());
        }
        lines.push(Line::raw(stats));
        if !r.errors.is_empty() {
            lines.push(Line::styled("  Errors:", pal.accent()));
            for issue in &r.errors {
                lines.push(Line::styled(
                    format!("    [{}] {}", issue.code, issue.message),
                    pal.warn(),
                ));
            }
        }
        let has_debug = r.warnings.iter().any(|w| w.debug);
        let visible: Vec<_> = r
            .warnings
            .iter()
            .filter(|w| !w.debug || view.show_debug)
            .collect();
        if !visible.is_empty() {
            lines.push(Line::styled("  Warnings:", pal.accent()));
            for w in visible {
                lines.push(if w.debug {
                    Line::styled(format!("    [DEBUG] {}", w.message), pal.dim())
                } else {
                    Line::raw(format!("    [{}] {}", w.code, w.message))
                });
            }
        }
        if let Some(report) = view.repair {
            lines.push(Line::styled("  Repair report:", pal.accent()));
            for fix in &report.fixes_applied {
                lines.push(Line::raw(format!("    {fix}")));
            }
            for fail in &report.fixes_failed {
                lines.push(Line::styled(format!("    {fail}"), pal.warn()));
            }
        }
        lines.push(Line::raw(""));
        let mut keys = vec!["Up/Down scroll"];
        if r.repairable {
            keys.push("r repair");
        }
        if has_debug {
            keys.push(if view.show_debug {
                "d hide debug"
            } else {
                "d show debug"
            });
        }
        keys.push("Esc close");
        lines.push(Line::styled(format!("  {}", keys.join("   ")), pal.dim()));
        let lines: Vec<Line<'static>> = lines.into_iter().skip(s.scroll).collect();
        self.draw_inspect_popup(frame, area, "Filesystem Check Results", lines, 0);
    }

    #[cfg(feature = "remote")]
    fn draw_inspect_remote(&self, frame: &mut Frame, area: Rect, s: &InspectState) {
        let pal = self.palette;
        let r = &s.remote;
        let mut lines: Vec<Line<'static>> = Vec::new();
        if !r.addr().is_empty() {
            lines.push(Line::styled(
                format!("  Connected to {}", r.addr()),
                pal.dim(),
            ));
        }
        if let Some(snap) = r.working() {
            lines.push(Line::styled(format!("  {}", snap.label), pal.dim()));
        }
        if let Some(err) = r.error() {
            lines.push(Line::styled(format!("  {err}"), pal.warn()));
        }
        let mut focus = 0usize;
        if r.is_loaded() {
            lines.push(Line::styled(format!("  {}", r.hint()), pal.dim()));
            for (i, row) in r.rows().iter().enumerate() {
                let sel = i == s.remote_sel;
                if sel {
                    focus = lines.len();
                }
                let style = if sel {
                    pal.accent().add_modifier(Modifier::REVERSED)
                } else if row.openable || row.is_dir {
                    pal.accent()
                } else {
                    pal.dim()
                };
                let name = if row.is_dir && !row.parent {
                    format!("{}/", row.name)
                } else {
                    row.name.clone()
                };
                lines.push(Line::styled(
                    format!(
                        "{}{name:<40} {:>10}  {}",
                        if sel { "> " } else { "  " },
                        row.size,
                        row.type_tag
                    ),
                    style,
                ));
            }
        } else if !r.is_active() {
            lines.push(Line::raw("  Not connected."));
        }
        lines.push(Line::raw(""));
        lines.push(Line::styled(
            "  Enter open   Backspace up   c connect   Esc disconnect",
            pal.dim(),
        ));
        self.draw_inspect_popup(frame, area, "Remote image", lines, focus);
    }

    #[cfg(not(feature = "remote"))]
    fn draw_inspect_remote(&self, _frame: &mut Frame, _area: Rect, _s: &InspectState) {}

    /// A window over `area`, scrolled so the `focus` line shows.
    pub(super) fn draw_inspect_popup(
        &self,
        frame: &mut Frame,
        area: Rect,
        title: &str,
        lines: Vec<Line<'static>>,
        focus: usize,
    ) {
        let h = (lines.len() as u16 + 2).min(area.height.saturating_sub(2));
        let inner = h.saturating_sub(2) as usize;
        let scroll = if inner > 2 && focus + 2 > inner {
            (focus + 2 - inner) as u16
        } else {
            0
        };
        let popup = centered_rect(area.width.saturating_sub(4).min(100), h, area);
        frame.render_widget(Clear, popup);
        frame.render_widget(
            Paragraph::new(Text::from(lines))
                .block(self.pane_block(title, true))
                .scroll((scroll, 0)),
            popup,
        );
    }
}
