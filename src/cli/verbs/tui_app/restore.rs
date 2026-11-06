//! The TUI Restore screen: one form per mode, then the run, over a headless `RestoreController`.
//!
//! The same controller drives the egui Restore tab, so the screen offers its modes: Full Disk
//! (backup folder, Clonezilla image or `.cbk`; device, image or remote target; trailing free
//! space; alignment; per-partition sizes with the layout-fit check), Single Partition (target
//! scan and partition pick), New Disk (table, alignment, bootable) and Write Image File, whose
//! image goes to a headless `DiskWriteController` drawn as the GUI's Physical Disk Export window
//! (target, region, synthesized table, the typed device name). Building a fresh disk is the Build
//! Disk screen's. See docs/phase2_commander_plan.md § "Slice 25".

use std::sync::{Arc, Mutex};

use crossterm::event::KeyCode;
use ratatui::layout::Rect;
use ratatui::style::Modifier;
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Paragraph, Wrap};
use ratatui::Frame;

use super::ask::Ask;
use super::writer::{WriterKey, WriterWindow, HINTS_WRITER};
use super::{
    expand_tilde, hint, App, FilePicker, KeyHint, PickKind, PickResult, Progress, ProgressShared,
};
use crate::controller::app::{RateTracker, RecentHandle, RecentMode};
use crate::controller::disk_write::{DiskWriteAction, DiskWriteController, DiskWriteServices};
use crate::controller::restore::{
    format_size, purpose, AlignmentChoice, CaveatLevel, HostRequest, JobRegistry, LayoutFit,
    NewTableType, NoNotify, RestoreAction, RestoreController, RestoreMode, RestoreServices,
    SizeMode, TargetSlot,
};
use crate::controller::{Controller as _, Effect, PickSpec};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum RestoreStep {
    Form,
    Run,
}

/// The modes the screen offers, in order.
const MODES: &[(RestoreMode, &str)] = &[
    (RestoreMode::FullDisk, "Full Disk Restore"),
    (RestoreMode::SinglePartition, "Restore Single Partition"),
    (RestoreMode::NewDisk, "Restore to New Disk"),
    (RestoreMode::WriteImage, "Write Image File"),
];

/// One row of the form.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Field {
    Mode,
    Backup,
    SourceImage,
    SourcePartition,
    Remote,
    RemoteHost,
    RemoteDrive(usize),
    RemoteImage,
    TargetKind,
    TargetDevice,
    TargetImage,
    Scan,
    /// A partition of the scanned single-partition target, by its index.
    TargetPartition(usize),
    Expand,
    ExpandMib,
    ExtendLast,
    Alignment,
    AlignCustom,
    /// A partition's size row, by its index.
    Size(usize),
    NdTable,
    NdAlign,
    NdAlignCustom,
    NdBootable,
    WriteSource,
    WriteTarget,
    Start,
}

impl Field {
    /// Typed into: letter shortcuts do not act here.
    fn is_text(self) -> bool {
        matches!(
            self,
            Field::TargetImage | Field::RemoteHost | Field::RemoteImage
        )
    }
}

/// Which controller asked a question.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Who {
    Restore,
    Writer,
}

/// Where a picked device goes.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum DeviceFor {
    Slot(TargetSlot),
    Writer,
}

/// What a confirmed picker answers.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Pick {
    /// A controller's picker, answered with `RestoreAction::Picked`.
    Restore(&'static str),
    /// The writer's image-file target.
    WriterImage,
}

pub(super) const HINTS_RESTORE_FORM: &[KeyHint] = &[
    hint("Up/Down", "Move between rows  (Shift-Tab goes up)"),
    hint(
        "Left/Right",
        "Change a choice (mode, target kind, alignment, size mode)",
    ),
    hint("Space", "Toggle the row, or change its choice"),
    hint(
        "Enter",
        "Act on the row (browse, pick a device, scan, connect, start)",
    ),
    hint("Tab", "Browse for the image target"),
    hint("0-9", "Type a number (free space, alignment, Custom MiB)"),
    hint("o", "Choose the backup folder or .cbk"),
    hint(
        "c",
        "Close the backup (Backup row) or the drive (Device row)",
    ),
    hint(
        "s",
        "Start the restore  (Write Image File: choose the target)",
    ),
];

pub(super) const HINTS_RESTORE_RUN: &[KeyHint] = &[hint(
    "Esc",
    "Cancel the run; back to the form once it ended",
)];

pub(super) struct RestoreState {
    pub(super) step: RestoreStep,
    /// The app's recent-files store; a successful open is recorded through it.
    recent: RecentHandle,
    pub(super) ctl: RestoreController,
    /// Writes Write Image File's image (the GUI's Physical Disk Export window).
    pub(super) writer: DiskWriteController,
    pub(super) focus: Field,
    picker: Option<(Pick, FilePicker)>,
    device_pick: Option<DeviceFor>,
    device_sel: usize,
    pub(super) ask: Option<Ask<Who>>,
    /// The typed image targets (the controller holds the expanded path).
    pub(super) full_target: String,
    pub(super) nd_target: String,
    /// The controllers' latest log lines.
    pub(super) log: Vec<String>,
    /// The writer window's cursor and its typed image file (set with Enter).
    pub(super) win: WriterWindow,
    /// The run on screen is a write (else a restore).
    writing: bool,
    /// Esc asked the running restore or write to stop.
    cancel_note: bool,
    /// The backup the image target was defaulted for.
    defaulted_for: Option<std::path::PathBuf>,
    /// The user closed the backup here; the app closes the shared folder (Inspect too).
    pub(super) closed_backup: bool,
}

impl RestoreState {
    pub(super) fn new(recent: RecentHandle) -> Self {
        RestoreState {
            step: RestoreStep::Form,
            recent,
            ctl: RestoreController::new(RestoreServices {
                jobs: JobRegistry::new(),
                notify: Arc::new(NoNotify),
            }),
            writer: DiskWriteController::new(DiskWriteServices {
                jobs: crate::controller::disk_write::JobRegistry::new(),
                notify: Arc::new(crate::controller::disk_write::NoNotify),
            }),
            focus: Field::Backup,
            picker: None,
            device_pick: None,
            device_sel: 0,
            ask: None,
            full_target: String::new(),
            nd_target: String::new(),
            log: Vec::new(),
            win: WriterWindow::default(),
            writing: false,
            cancel_note: false,
            defaulted_for: None,
            closed_backup: false,
        }
    }

    pub(super) fn act(&mut self, action: RestoreAction) {
        let fx = self.ctl.apply(action);
        self.absorb(Who::Restore, fx);
        self.after();
    }

    pub(super) fn wact(&mut self, action: DiskWriteAction) {
        let fx = self.writer.apply(action);
        self.absorb(Who::Writer, fx);
        self.after();
    }

    /// Turn the controllers' effects into the screen's pickers and questions.
    fn absorb(&mut self, who: Who, fx: Vec<Effect>) {
        for e in fx {
            match e {
                Effect::PickFile(spec) | Effect::PickFolder(spec) => self.open_picker(spec),
                Effect::Confirm(spec) => self.ask = Some(Ask::Confirm(who, spec)),
                Effect::AskText(spec) => self.ask = Some(Ask::text(who, spec)),
                Effect::Message(msg) => self.log.push(msg),
                // Neither controller asks for these.
                Effect::AskPassword(_)
                | Effect::Choose(_)
                | Effect::OpenUrl(_)
                | Effect::CopyToClipboard(_)
                | Effect::CloseScreen
                | Effect::Quit => {}
            }
        }
    }

    fn open_picker(&mut self, spec: PickSpec) {
        let recent = if spec.purpose == purpose::BACKUP_FOLDER {
            self.recent.list(RecentMode::Restore)
        } else {
            Vec::new()
        };
        let kind = if spec.save {
            PickKind::Save
        } else if spec.purpose == purpose::BACKUP_FOLDER {
            PickKind::Any
        } else {
            PickKind::File
        };
        let mut p = FilePicker::new(kind, spec.title.as_str()).with_recent(recent);
        if spec.save {
            p.input = match spec.purpose {
                purpose::NEW_DISK_IMAGE => self.nd_target.clone(),
                _ => self.full_target.clone(),
            };
            if p.input.is_empty() {
                p.input = spec.file_name.clone();
            }
        }
        self.picker = Some((Pick::Restore(spec.purpose), p));
    }

    /// Carry out the controllers' requests and keep their latest log lines.
    fn after(&mut self) {
        for req in self.ctl.take_requests() {
            match req {
                HostRequest::RememberRecent(path) => {
                    self.recent
                        .remember(RecentMode::Restore, path.to_string_lossy().as_ref());
                }
                HostRequest::WriteImage(open) => {
                    self.win = WriterWindow::default();
                    let fx = self.writer.apply(DiskWriteAction::Open(open));
                    self.absorb(Who::Writer, fx);
                }
            }
        }
        let lines: Vec<String> = self
            .ctl
            .take_log()
            .into_iter()
            .chain(self.writer.take_log())
            .map(|(_, msg)| msg)
            .collect();
        self.log.extend(lines);
        let n = self.log.len();
        if n > 6 {
            self.log.drain(..n - 6);
        }
        // A newly loaded backup's image target defaults beside it, as the old screen did.
        let folder = self.ctl.backup_folder().map(|p| p.to_path_buf());
        if self.ctl.has_backup() && folder != self.defaulted_for {
            self.defaulted_for = folder;
            if let Some(p) = self.ctl.default_target_image() {
                if self.full_target.is_empty() {
                    self.set_target_text(TargetSlot::FullDisk, p.display().to_string());
                }
            }
        }
    }

    /// The typed image target; the controller gets it at once (`~` expanded).
    pub(super) fn set_target_text(&mut self, slot: TargetSlot, text: String) {
        let path = expand_tilde(text.trim());
        let image = (!path.as_os_str().is_empty()).then_some(path);
        if slot == TargetSlot::NewDisk {
            self.nd_target = text;
        } else {
            self.full_target = text;
        }
        let fx = self.ctl.apply(RestoreAction::SetTargetImage(slot, image));
        self.absorb(Who::Restore, fx);
    }

    /// The slot the form's target rows edit.
    fn slot(&self) -> TargetSlot {
        match self.ctl.mode() {
            RestoreMode::SinglePartition => TargetSlot::SinglePartition,
            RestoreMode::NewDisk => TargetSlot::NewDisk,
            RestoreMode::FullDisk | RestoreMode::WriteImage => TargetSlot::FullDisk,
        }
    }

    /// A restore or a write runs.
    pub(super) fn busy(&self) -> bool {
        self.ctl.is_running() || self.writer.is_writing()
    }

    /// How the run ended (text, is an error); `None` while it runs.
    pub(super) fn result(&self) -> Option<(String, bool)> {
        if self.busy() {
            return self
                .cancel_note
                .then(|| ("Cancel requested...".to_string(), true));
        }
        if self.writing {
            return match self.writer.result()? {
                Ok(n) => Some((format!("Wrote {} to the target.", format_size(*n)), false)),
                Err(e) => Some((format!("Image write failed: {e}"), true)),
            };
        }
        match self.ctl.last_result()? {
            Ok(()) => Some(("Restore complete.".to_string(), false)),
            Err(e) => Some((e.clone(), true)),
        }
    }

    /// The form's rows, in drawing order (they follow the mode and what was found).
    pub(super) fn fields(&self) -> Vec<Field> {
        let c = &self.ctl;
        let mut f = vec![Field::Mode];
        match c.mode() {
            RestoreMode::FullDisk => {
                f.push(Field::Backup);
                if RestoreController::remote_available() {
                    f.push(Field::Remote);
                }
                if c.remote_active() {
                    f.push(Field::RemoteHost);
                    if c.remote_connected() {
                        f.extend((0..c.remote_drives().len()).map(Field::RemoteDrive));
                        f.push(Field::RemoteImage);
                    }
                } else {
                    f.push(Field::TargetKind);
                    f.push(if c.target_is_device(TargetSlot::FullDisk) {
                        Field::TargetDevice
                    } else {
                        Field::TargetImage
                    });
                }
                if c.expand_shown() {
                    f.push(Field::Expand);
                    if c.expand().enabled {
                        f.extend([Field::ExpandMib, Field::ExtendLast]);
                    }
                }
                if c.has_backup() {
                    f.push(Field::Alignment);
                    if c.alignment().0 == AlignmentChoice::Custom {
                        f.push(Field::AlignCustom);
                    }
                }
                f.extend(c.size_rows().iter().map(|r| Field::Size(r.index)));
                f.push(Field::Start);
            }
            RestoreMode::SinglePartition | RestoreMode::NewDisk => {
                f.extend([Field::Backup, Field::SourceImage]);
                if c.has_native_backup() {
                    f.push(Field::SourcePartition);
                }
                if c.mode() == RestoreMode::SinglePartition {
                    f.push(Field::TargetDevice);
                    if c.device(TargetSlot::SinglePartition).is_some() {
                        f.push(Field::Scan);
                    }
                    f.extend(
                        c.target_partitions()
                            .iter()
                            .map(|p| Field::TargetPartition(p.index)),
                    );
                } else {
                    f.push(Field::TargetKind);
                    f.push(if c.target_is_device(TargetSlot::NewDisk) {
                        Field::TargetDevice
                    } else {
                        Field::TargetImage
                    });
                    f.extend([Field::NdTable, Field::NdAlign]);
                    let nd = c.new_disk();
                    if nd.alignment == AlignmentChoice::Custom {
                        f.push(Field::NdAlignCustom);
                    }
                    if nd.table == NewTableType::Mbr {
                        f.push(Field::NdBootable);
                    }
                }
                f.push(Field::Start);
            }
            RestoreMode::WriteImage => {
                f.push(Field::WriteSource);
                if c.write_image().is_some_and(|(_, n)| n > 0) {
                    f.push(Field::WriteTarget);
                }
            }
        }
        f
    }

    fn move_focus(&mut self, delta: isize) {
        let fields = self.fields();
        let n = fields.len() as isize;
        let at = fields.iter().position(|f| *f == self.focus).unwrap_or(0);
        self.focus = fields[((at as isize + delta).rem_euclid(n)) as usize];
    }

    /// Keep the focus on a row that still exists (the form follows the controller).
    fn settle_focus(&mut self) {
        let fields = self.fields();
        if !fields.contains(&self.focus) {
            self.focus = fields.get(1).copied().unwrap_or(Field::Mode);
        }
    }

    /// Space / Left / Right on a choice or toggle row; `false` when the row has none.
    fn change(&mut self, field: Field, forward: bool) -> bool {
        fn step<T: Copy + PartialEq>(list: &[T], cur: T, forward: bool) -> T {
            let n = list.len();
            match list.iter().position(|x| *x == cur) {
                Some(i) if forward => list[(i + 1) % n],
                Some(i) => list[(i + n - 1) % n],
                None => list[0],
            }
        }
        let slot = self.slot();
        match field {
            Field::Mode => {
                let modes: Vec<RestoreMode> = MODES.iter().map(|m| m.0).collect();
                let m = step(modes.as_slice(), self.ctl.mode(), forward);
                self.act(RestoreAction::SetMode(m));
                self.focus = Field::Mode;
            }
            Field::Remote => {
                let on = !self.ctl.remote_active();
                self.act(RestoreAction::SetRemote(on));
            }
            Field::TargetKind => {
                let on = !self.ctl.target_is_device(slot);
                self.act(RestoreAction::SetTargetIsDevice(slot, on));
                self.focus = Field::TargetKind;
            }
            Field::Expand => {
                let on = !self.ctl.expand().enabled;
                self.act(RestoreAction::SetExpand(on));
            }
            Field::ExtendLast => {
                let on = !self.ctl.expand().extend_last;
                self.act(RestoreAction::SetExtendLast(on));
            }
            Field::Alignment => {
                let all = [
                    AlignmentChoice::Original,
                    AlignmentChoice::Modern1MB,
                    AlignmentChoice::Custom,
                ];
                let a = step(all.as_slice(), self.ctl.alignment().0, forward);
                self.act(RestoreAction::SetAlignment(a));
            }
            Field::Size(index) => {
                let Some(row) = self.ctl.size_rows().iter().find(|r| r.index == index) else {
                    return false;
                };
                let (cur, custom) = (row.mode, row.custom_mib);
                let modes = self.ctl.size_mode_choices(index);
                if modes.is_empty() {
                    return false;
                }
                let mode = step(modes.as_slice(), cur, forward);
                self.act(RestoreAction::SetPartitionSize {
                    index,
                    mode,
                    custom_mib: custom,
                });
            }
            Field::SourcePartition => {
                let parts: Vec<Option<usize>> = self
                    .ctl
                    .source_partitions()
                    .iter()
                    .map(|p| Some(p.index))
                    .collect();
                if parts.is_empty() {
                    return false;
                }
                let next = step(parts.as_slice(), self.ctl.source_partition(), forward);
                self.act(RestoreAction::SetSourcePartition(next));
            }
            Field::TargetPartition(index) => self.act(RestoreAction::SetTargetPartition(index)),
            Field::NdTable | Field::NdAlign | Field::NdBootable => {
                let mut nd = self.ctl.new_disk();
                match field {
                    Field::NdTable => {
                        let all = [NewTableType::Mbr, NewTableType::Gpt, NewTableType::Apm];
                        nd.table = step(all.as_slice(), nd.table, forward);
                    }
                    Field::NdAlign => {
                        let all = [
                            AlignmentChoice::Modern1MB,
                            AlignmentChoice::Original,
                            AlignmentChoice::Custom,
                        ];
                        nd.alignment = step(all.as_slice(), nd.alignment, forward);
                    }
                    _ => nd.bootable = !nd.bootable,
                }
                self.act(RestoreAction::SetNewDisk(nd));
            }
            _ => return false,
        }
        self.settle_focus();
        true
    }

    /// A digit typed on a number row; `false` when the row takes none.
    fn digit(&mut self, field: Field, d: Option<u32>) -> bool {
        let edit = |n: u32| match d {
            Some(d) => n.saturating_mul(10).saturating_add(d),
            None => n / 10,
        };
        match field {
            Field::ExpandMib => {
                let n = edit(self.ctl.expand().free_mib);
                self.act(RestoreAction::SetExpandMib(n));
            }
            Field::AlignCustom => {
                let n = edit(self.ctl.alignment().1);
                self.act(RestoreAction::SetCustomAlignment(n));
            }
            Field::NdAlignCustom => {
                let mut nd = self.ctl.new_disk();
                nd.custom_alignment = edit(nd.custom_alignment);
                self.act(RestoreAction::SetNewDisk(nd));
            }
            Field::Size(index) => {
                let Some(row) = self.ctl.size_rows().iter().find(|r| r.index == index) else {
                    return false;
                };
                if row.mode != SizeMode::Custom {
                    return false;
                }
                let custom_mib = edit(row.custom_mib);
                self.act(RestoreAction::SetPartitionSize {
                    index,
                    mode: SizeMode::Custom,
                    custom_mib,
                });
            }
            _ => return false,
        }
        true
    }

    fn type_char(&mut self, c: char) {
        match self.focus {
            Field::TargetImage => {
                let slot = self.slot();
                let mut t = self.target_text(slot);
                t.push(c);
                self.set_target_text(slot, t);
            }
            Field::RemoteHost => {
                let mut a = self.ctl.remote_address().to_string();
                a.push(c);
                self.act(RestoreAction::RemoteSetAddress(a));
            }
            Field::RemoteImage => {
                let mut a = self.ctl.remote_image_name().to_string();
                a.push(c);
                self.act(RestoreAction::RemoteSetImageName(a));
            }
            _ => {}
        }
    }

    fn backspace(&mut self) {
        match self.focus {
            Field::TargetImage => {
                let slot = self.slot();
                let mut t = self.target_text(slot);
                t.pop();
                self.set_target_text(slot, t);
            }
            Field::RemoteHost => {
                let mut a = self.ctl.remote_address().to_string();
                a.pop();
                self.act(RestoreAction::RemoteSetAddress(a));
            }
            Field::RemoteImage => {
                let mut a = self.ctl.remote_image_name().to_string();
                a.pop();
                self.act(RestoreAction::RemoteSetImageName(a));
            }
            f => {
                self.digit(f, None);
            }
        }
    }

    fn target_text(&self, slot: TargetSlot) -> String {
        if slot == TargetSlot::NewDisk {
            self.nd_target.clone()
        } else {
            self.full_target.clone()
        }
    }

    /// Press the controller's Start; its confirmation comes back as a question.
    fn start(&mut self) {
        self.cancel_note = false;
        self.writing = false;
        self.act(RestoreAction::Start);
    }

    /// Press the writer's Write; its question comes back as an `Ask`.
    fn write(&mut self) {
        self.cancel_note = false;
        self.writing = true;
        self.wact(DiskWriteAction::Write);
    }

    /// Answer the open question.
    fn answer(&mut self, yes: bool, text: Option<String>) {
        let Some(ask) = self.ask.take() else {
            return;
        };
        match ask {
            Ask::Confirm(Who::Restore, spec) => self.act(RestoreAction::Confirmed {
                purpose: spec.purpose,
                yes,
            }),
            Ask::Confirm(Who::Writer, spec) => self.wact(DiskWriteAction::Confirmed {
                purpose: spec.purpose,
                yes,
            }),
            Ask::Text {
                who: Who::Restore,
                spec,
                ..
            } => self.act(RestoreAction::Answered {
                purpose: spec.purpose,
                text,
            }),
            Ask::Text {
                who: Who::Writer,
                spec,
                ..
            } => self.wact(DiskWriteAction::Answered {
                purpose: spec.purpose,
                text,
            }),
        }
    }

    fn ask_key(&mut self, code: KeyCode) {
        if let Some((yes, text)) = self.ask.as_mut().and_then(|a| a.key(code)) {
            self.answer(yes, text);
        }
    }
}

impl App {
    /// The Restore screen's name and key list, for the `?` window.
    pub(super) fn restore_key_context(&self) -> (&'static str, &'static [KeyHint]) {
        let Some(r) = self.restore.as_ref() else {
            return ("Restore", HINTS_RESTORE_FORM);
        };
        if r.step == RestoreStep::Run {
            return ("Restore - run", HINTS_RESTORE_RUN);
        }
        if r.writer.is_open() {
            return ("Restore - write image", HINTS_WRITER);
        }
        ("Restore", HINTS_RESTORE_FORM)
    }

    /// The footer's short key strip for the Restore screen.
    pub(super) fn restore_footer_keys(&self) -> Vec<(&'static str, &'static str)> {
        let Some(r) = self.restore.as_ref() else {
            return Vec::new();
        };
        if r.ask.is_some() {
            return vec![("y/n", "Answer"), ("Esc", "Cancel")];
        }
        if r.step == RestoreStep::Run {
            return vec![("Esc", "Back")];
        }
        if r.writer.is_open() {
            return vec![
                ("Up/Dn", "Row"),
                ("<-/->", "Change"),
                ("Enter", "Act"),
                ("w", "Write"),
                ("Esc", "Close"),
            ];
        }
        vec![
            ("Up/Dn", "Row"),
            ("<-/->", "Change"),
            ("Enter", "Act"),
            ("s", "Start"),
            ("?", "Keys"),
        ]
    }

    /// Restore screen keys. Returns `true` when consumed; `false` lets a key fall through to
    /// the shell (Left/Right switch tabs, `q` quits).
    pub(super) fn handle_restore_key(&mut self, code: KeyCode) -> bool {
        if self.restore.is_none() {
            self.restore = Some(RestoreState::new(self.shell.recent_handle()));
        }
        let r = self.restore.as_mut().unwrap();
        if r.ask.is_some() {
            r.ask_key(code);
            self.restore_run_started();
            return true;
        }
        if let Some((pick, picker)) = r.picker.as_mut() {
            let pick = *pick;
            match picker.handle_key(code) {
                Some(PickResult::Cancel) => {
                    r.picker = None;
                    if let Pick::Restore(purpose) = pick {
                        r.act(RestoreAction::Picked {
                            purpose,
                            path: None,
                        });
                    }
                }
                Some(PickResult::Confirm(path)) => {
                    r.picker = None;
                    match pick {
                        Pick::Restore(purpose) => {
                            r.act(RestoreAction::Picked {
                                purpose,
                                path: Some(path),
                            });
                            // A picked image target shows in its typed field.
                            let slot = r.slot();
                            if let Some(p) = r.ctl.target_image(slot) {
                                let text = p.display().to_string();
                                if slot == TargetSlot::NewDisk {
                                    r.nd_target = text;
                                } else {
                                    r.full_target = text;
                                }
                            }
                        }
                        Pick::WriterImage => {
                            r.win.image = path.display().to_string();
                            if let Some(a) = r.win.image_target() {
                                r.wact(a);
                            }
                        }
                    }
                }
                None => {}
            }
            r.settle_focus();
            return true;
        }
        if let Some(target) = r.device_pick {
            let ndisks = self.shell.devices().len();
            match code {
                KeyCode::Up | KeyCode::Char('k') => r.device_sel = r.device_sel.saturating_sub(1),
                KeyCode::Down | KeyCode::Char('j') => {
                    r.device_sel = (r.device_sel + 1).min(ndisks.saturating_sub(1))
                }
                KeyCode::Esc => r.device_pick = None,
                KeyCode::Enter => {
                    let row = r.device_sel;
                    if let Some(device) = self.shell.devices().get(row).cloned() {
                        let r = self.restore.as_mut().unwrap();
                        r.device_pick = None;
                        match target {
                            DeviceFor::Slot(slot) => {
                                r.act(RestoreAction::SelectDevice { slot, row, device })
                            }
                            DeviceFor::Writer => {
                                r.win.image.clear();
                                r.wact(DiskWriteAction::SelectDevice { row, device });
                            }
                        }
                        r.settle_focus();
                    }
                }
                _ => {}
            }
            return true;
        }
        if r.step == RestoreStep::Run {
            if code == KeyCode::Esc {
                if r.result().is_none() {
                    self.cancel_progress();
                } else {
                    self.progress = None;
                    let r = self.restore.as_mut().unwrap();
                    r.cancel_note = false;
                    r.step = RestoreStep::Form;
                    if r.writing {
                        r.wact(DiskWriteAction::Close);
                    }
                }
            }
            return true;
        }
        if r.writer.is_open() {
            self.restore_writer_key(code);
            self.restore_run_started();
            return true;
        }
        let consumed = self.restore_form_key(code);
        self.restore_run_started();
        consumed
    }

    fn restore_writer_key(&mut self, code: KeyCode) {
        let r = self.restore.as_mut().unwrap();
        match r.win.key(&r.writer, code) {
            WriterKey::Done => {}
            WriterKey::Act(a) => r.wact(a),
            WriterKey::PickDevice => {
                r.device_sel = r.writer.target_row().unwrap_or(0);
                r.device_pick = Some(DeviceFor::Writer);
            }
            WriterKey::BrowseImage => {
                let mut p = FilePicker::new(PickKind::Save, "Write the image to this file");
                p.input = r.win.image.clone();
                r.picker = Some((Pick::WriterImage, p));
            }
            WriterKey::Write => r.write(),
            WriterKey::Close => r.wact(DiskWriteAction::Close),
        }
        r.win.settle(&r.writer);
    }

    fn restore_form_key(&mut self, code: KeyCode) -> bool {
        let r = self.restore.as_mut().unwrap();
        r.settle_focus();
        let focus = r.focus;
        let slot = r.slot();
        match code {
            KeyCode::Down => r.move_focus(1),
            KeyCode::Up | KeyCode::BackTab => r.move_focus(-1),
            KeyCode::Tab if focus == Field::TargetImage => {
                r.act(RestoreAction::PickTargetImage(slot));
            }
            KeyCode::Enter => match focus {
                Field::Backup => r.act(RestoreAction::PickBackupFolder),
                Field::SourceImage => r.act(RestoreAction::PickSourceImage),
                Field::TargetDevice => {
                    r.device_sel = r.ctl.device_row(slot).unwrap_or(0);
                    r.device_pick = Some(DeviceFor::Slot(slot));
                }
                Field::Scan => r.act(RestoreAction::ScanTarget),
                Field::TargetPartition(index) => r.act(RestoreAction::SetTargetPartition(index)),
                Field::RemoteHost => r.act(RestoreAction::RemoteConnect),
                Field::RemoteDrive(i) => {
                    if let Some(d) = r.ctl.remote_drives().get(i).cloned() {
                        r.act(RestoreAction::RemotePickDevice {
                            path: d.path,
                            size: d.size_bytes,
                        });
                    }
                }
                Field::RemoteImage => r.act(RestoreAction::RemoteUseImage),
                Field::WriteSource => r.act(RestoreAction::PickWriteImage),
                Field::WriteTarget => r.act(RestoreAction::WriteImageTarget),
                Field::Start => r.start(),
                _ => r.move_focus(1),
            },
            KeyCode::Char(' ') if !focus.is_text() => {
                r.change(focus, true);
            }
            KeyCode::Left | KeyCode::Right => {
                // Rows without a choice leave Left/Right to tab switching.
                return r.change(focus, code == KeyCode::Right);
            }
            KeyCode::Backspace => r.backspace(),
            KeyCode::Char(c) if focus.is_text() && !c.is_control() => r.type_char(c),
            KeyCode::Char(c) if c.is_ascii_digit() => {
                return r.digit(focus, c.to_digit(10));
            }
            KeyCode::Char('o') => r.act(RestoreAction::PickBackupFolder),
            KeyCode::Char('c') => match focus {
                Field::Backup => {
                    r.closed_backup = r.ctl.has_backup();
                    r.act(RestoreAction::ClearBackup);
                }
                Field::TargetDevice => r.act(RestoreAction::ClearDevice(slot)),
                _ => return false,
            },
            KeyCode::Char('s') => {
                if r.ctl.mode() == RestoreMode::WriteImage {
                    r.act(RestoreAction::WriteImageTarget);
                } else {
                    r.start();
                }
            }
            _ => return false,
        }
        if let Some(r) = self.restore.as_mut() {
            r.settle_focus();
        }
        true
    }

    /// A restore or write just started: on to the Run step with the bottom bar.
    fn restore_run_started(&mut self) {
        let Some(r) = self.restore.as_mut() else {
            return;
        };
        if r.step == RestoreStep::Run || !r.busy() {
            return;
        }
        r.writing = r.writer.is_writing();
        r.step = RestoreStep::Run;
        self.progress = Some(Progress {
            shared: Arc::new(Mutex::new(ProgressShared::default())),
            tracker: RateTracker::default(),
            label: if r.writing {
                "Writing image".to_string()
            } else {
                "Restoring".to_string()
            },
        });
    }

    /// Esc during the run: ask the restore or the write to stop.
    pub(super) fn restore_cancel(&mut self) {
        let Some(r) = self.restore.as_mut() else {
            return;
        };
        if r.ctl.restore_running() {
            r.act(RestoreAction::Cancel);
            r.cancel_note = true;
        }
        if r.writer.is_writing() {
            r.wact(DiskWriteAction::Cancel);
            r.cancel_note = true;
        }
    }

    /// Drive both controllers and mirror the run into the visual bar.
    pub(super) fn restore_tick(&mut self) {
        let Some(r) = self.restore.as_mut() else {
            return;
        };
        // A device refresh (Inspect's `r`) may have moved or dropped a target disk.
        r.ctl.reconcile_devices(self.shell.devices());
        r.writer.reconcile_devices(self.shell.devices());
        let fx = r.ctl.tick();
        r.absorb(Who::Restore, fx);
        let fx = r.writer.tick();
        r.absorb(Who::Writer, fx);
        r.after();
        if r.step == RestoreStep::Form && !r.writer.is_open() {
            r.settle_focus();
        }
        if r.step != RestoreStep::Run {
            return;
        }
        let (current, total) = if r.writing {
            match r.writer.progress().map(|s| s.progress) {
                Some(crate::controller::disk_write::Progress::Determinate {
                    done, total, ..
                }) => (done, total),
                _ => (0, 0),
            }
        } else {
            r.ctl
                .progress()
                .map_or((0, 0), |b| (b.current_bytes, b.total_bytes))
        };
        let done = !r.busy();
        if let Some(prog) = self.progress.as_ref() {
            if let Ok(mut sh) = prog.shared.lock() {
                if total > 0 {
                    sh.current = current;
                    sh.total = total;
                }
                sh.done = done;
            }
        }
    }

    pub(super) fn draw_restore(&self, frame: &mut Frame, area: Rect, r: &RestoreState) {
        let title = match r.step {
            RestoreStep::Form => "Restore",
            RestoreStep::Run => "Restore  -  run",
        };
        let mut lines: Vec<Line<'static>> = Vec::new();
        let mut focus_line = 0usize;
        match r.step {
            RestoreStep::Form => focus_line = self.restore_form_lines(r, &mut lines),
            RestoreStep::Run => self.restore_run_lines(r, &mut lines),
        }
        let inner = area.height.saturating_sub(2) as usize;
        let scroll = if inner > 2 && focus_line + 2 > inner {
            (focus_line + 2 - inner) as u16
        } else {
            0
        };
        let para = Paragraph::new(Text::from(lines)).block(self.pane_block(title, true));
        let para = if r.step == RestoreStep::Form {
            para.scroll((scroll, 0))
        } else {
            para.wrap(Wrap { trim: false })
        };
        frame.render_widget(para, area);

        if r.step == RestoreStep::Form && r.writer.is_open() {
            self.draw_writer(frame, area, "Write Image File", &r.writer, &r.win);
        }
        if let Some((_, picker)) = &r.picker {
            picker.draw(frame, area, self.palette, self.border);
        }
        if r.device_pick.is_some() {
            self.draw_device_pick(frame, area, r.device_sel, "Pick a target device");
        }
        if let Some(ask) = &r.ask {
            self.draw_ask(frame, area, ask, true);
        }
    }

    fn restore_form_lines(&self, r: &RestoreState, lines: &mut Vec<Line<'static>>) -> usize {
        let c = &r.ctl;
        let pal = self.palette;
        let mut focus_line = 0usize;
        let check = |on: bool| if on { "[x]" } else { "[ ]" };
        let dim = |lines: &mut Vec<Line<'static>>, text: String| {
            lines.push(Line::styled(format!("             {text}"), pal.dim()));
        };
        let warn = |lines: &mut Vec<Line<'static>>, text: String| {
            lines.push(Line::styled(format!("             {text}"), pal.warn()));
        };
        let slot = r.slot();
        for f in r.fields() {
            if r.focus == f {
                focus_line = lines.len();
            }
            let sel = r.focus == f;
            let (m, style) = if sel {
                ("> ", pal.accent().add_modifier(Modifier::BOLD))
            } else {
                ("  ", pal.accent())
            };
            let cursor = if sel && f.is_text() {
                Span::styled(" ", pal.accent().add_modifier(Modifier::REVERSED))
            } else {
                Span::raw("")
            };
            let (label, value): (String, String) = match f {
                Field::Mode => {
                    let name = MODES.iter().find(|m| m.0 == c.mode()).map_or("?", |m| m.1);
                    ("Mode:".into(), format!("< {name} >"))
                }
                Field::Backup => (
                    "Backup:".into(),
                    c.backup_folder()
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|| {
                            "(Enter chooses a folder, .cbk or Clonezilla image)".into()
                        }),
                ),
                Field::SourceImage => (
                    "Or image:".into(),
                    c.source_image()
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|| "(Enter chooses a partition image)".into()),
                ),
                Field::SourcePartition => {
                    let parts = c.source_partitions();
                    let label = c
                        .source_partition()
                        .and_then(|i| parts.iter().find(|p| p.index == i))
                        .map_or("Select a partition...".to_string(), |p| p.label.clone());
                    ("Partition:".into(), format!("< {label} >"))
                }
                Field::Remote => (
                    "Remote:".into(),
                    format!(
                        "{} Restore to a remote machine (rb-cli serve)",
                        check(c.remote_active())
                    ),
                ),
                Field::RemoteHost => {
                    let a = c.remote_address();
                    let shown = if a.is_empty() && !sel {
                        "192.168.1.50:7341 (Enter connects)".to_string()
                    } else {
                        a.to_string()
                    };
                    ("  Host:".into(), shown)
                }
                Field::RemoteDrive(i) => {
                    let drives = c.remote_drives();
                    let d = drives.get(i);
                    let on = d.is_some_and(|d| c.remote_selected_device() == Some(d.path.as_str()));
                    (
                        if i == 0 { "  Drives:" } else { "" }.into(),
                        format!(
                            "{} {}",
                            if on { "(*)" } else { "( )" },
                            d.map(|d| d.label.clone()).unwrap_or_default()
                        ),
                    )
                }
                Field::RemoteImage => {
                    let n = c.remote_image_name();
                    let shown = if n.is_empty() && !sel {
                        "restored.img (Enter uses it)".to_string()
                    } else {
                        n.to_string()
                    };
                    ("  Or file:".into(), shown)
                }
                Field::TargetKind => (
                    "Target:".into(),
                    format!(
                        "< {} >",
                        if c.target_is_device(slot) {
                            "Write to device"
                        } else {
                            "Save as image file"
                        }
                    ),
                ),
                Field::TargetDevice => (
                    "Device:".into(),
                    c.device(slot)
                        .map(|d| format!("{} ({})", d.display_name(), format_size(d.size_bytes)))
                        .unwrap_or_else(|| "(Enter picks a target device)".into()),
                ),
                Field::TargetImage => {
                    let t = r.target_text(slot);
                    (
                        "Image file:".into(),
                        if t.is_empty() && !sel {
                            "(type a path; Tab browses)".into()
                        } else {
                            t
                        },
                    )
                }
                Field::Scan => (
                    "".into(),
                    match c.target_scan() {
                        Some(_) => "Scanning the target...".into(),
                        None => "[ Scan Target ]".into(),
                    },
                ),
                Field::TargetPartition(index) => {
                    let parts = c.target_partitions();
                    let Some(p) = parts.iter().find(|p| p.index == index) else {
                        continue;
                    };
                    let fits = match p.fits {
                        Some(false) => "  too small",
                        Some(true) => "  OK",
                        None => "",
                    };
                    let first = parts.first().map(|q| q.index) == Some(index);
                    (
                        if first { "Partition:" } else { "" }.into(),
                        format!(
                            "{} #{} {} ({}){fits}",
                            if c.target_partition() == Some(index) {
                                "(*)"
                            } else {
                                "( )"
                            },
                            p.position,
                            p.type_name,
                            format_size(p.size_bytes),
                        ),
                    )
                }
                Field::Expand => (
                    "Free space:".into(),
                    format!(
                        "{} Pad the target image with unallocated free space",
                        check(c.expand().enabled)
                    ),
                ),
                Field::ExpandMib => {
                    let src = c.source_size().unwrap_or(0);
                    (
                        "  MiB:".into(),
                        format!(
                            "{}  (0-{}; target size {} -> {})",
                            c.expand().free_mib,
                            c.expand_cap_mib(),
                            format_size(src),
                            format_size(src.saturating_add(c.expand_free_bytes())),
                        ),
                    )
                }
                Field::ExtendLast => (
                    "  Use:".into(),
                    format!(
                        "< {} >",
                        if c.expand().extend_last {
                            "Extend last partition automatically"
                        } else {
                            "Leave as unpartitioned free space (recommended)"
                        }
                    ),
                ),
                Field::Alignment => {
                    let label = match c.alignment().0 {
                        AlignmentChoice::Original => c.original_alignment_label(),
                        AlignmentChoice::Modern1MB => "1 MB boundaries (modern, LBA 2048)".into(),
                        AlignmentChoice::Custom => "Custom".into(),
                    };
                    ("Alignment:".into(), format!("< {label} >"))
                }
                Field::AlignCustom => (
                    "  Sectors:".into(),
                    format!("{}  (1-65535)", c.alignment().1),
                ),
                Field::Size(index) => {
                    let rows = c.size_rows();
                    let pos = rows.iter().position(|row| row.index == index);
                    let Some(row) = pos.and_then(|i| rows.get(i)) else {
                        continue;
                    };
                    let mode = match row.mode {
                        SizeMode::Original => "Original",
                        SizeMode::Minimum => "Minimum",
                        SizeMode::MinPlus20 => "Min+20%",
                        SizeMode::Custom => "Custom",
                        SizeMode::FillRemaining => "Fill",
                    };
                    let size = match row.mode {
                        SizeMode::Custom => {
                            let (lo, hi) = row.custom_bounds_mib();
                            if row.custom_mib_effective() == row.custom_mib {
                                format!("{} MiB", row.custom_mib)
                            } else {
                                format!(
                                    "{} MiB ({lo}-{hi}; {} used)",
                                    row.custom_mib,
                                    row.custom_mib_effective()
                                )
                            }
                        }
                        SizeMode::FillRemaining => "(auto)".into(),
                        _ => format_size(row.effective_size()),
                    };
                    let mut type_name = row.type_name.clone();
                    if type_name.chars().count() > 16 {
                        type_name = type_name.chars().take(15).collect::<String>() + "~";
                    }
                    if pos == Some(0) {
                        lines.push(Line::styled(
                            format!(
                                "  Sizes      {:>2}  {:<16} {:>10} {:>10}  Mode",
                                "#", "Type", "Original", "Minimum"
                            ),
                            pal.dim(),
                        ));
                        if sel {
                            focus_line = lines.len();
                        }
                    }
                    lines.push(Line::styled(
                        format!(
                            "{m}           {:>2}  {:<16} {:>10} {:>10}  < {:<8} > {}",
                            pos.unwrap_or(0) + 1,
                            type_name,
                            format_size(row.original_size),
                            format_size(row.minimum_size),
                            mode,
                            size
                        ),
                        style,
                    ));
                    if pos == Some(rows.len() - 1) {
                        self.restore_fit_lines(r, lines);
                    }
                    continue;
                }
                Field::NdTable => (
                    "Table:".into(),
                    format!(
                        "< {} >",
                        match c.new_disk().table {
                            NewTableType::Mbr => "MBR",
                            NewTableType::Gpt => "GPT",
                            NewTableType::Apm => "APM",
                        }
                    ),
                ),
                Field::NdAlign => (
                    "Alignment:".into(),
                    format!(
                        "< {} >   (starts at LBA {})",
                        match c.new_disk().alignment {
                            AlignmentChoice::Modern1MB => "1 MB (LBA 2048)",
                            AlignmentChoice::Original => "DOS (LBA 63)",
                            AlignmentChoice::Custom => "Custom",
                        },
                        c.new_disk_start_lba()
                    ),
                ),
                Field::NdAlignCustom => (
                    "  Sectors:".into(),
                    format!("{}  (1-65535)", c.new_disk().custom_alignment),
                ),
                Field::NdBootable => (
                    "Bootable:".into(),
                    format!(
                        "{} Mark partition as bootable",
                        check(c.new_disk().bootable)
                    ),
                ),
                Field::WriteSource => (
                    "Image:".into(),
                    c.write_image()
                        .map(|(p, _)| p.display().to_string())
                        .unwrap_or_else(|| "(Enter chooses the image to write)".into()),
                ),
                Field::WriteTarget => {
                    lines.push(Line::raw(""));
                    if sel {
                        focus_line = lines.len();
                    }
                    let style = if sel {
                        pal.accent()
                            .add_modifier(Modifier::REVERSED | Modifier::BOLD)
                    } else {
                        pal.accent()
                    };
                    lines.push(Line::styled(
                        format!("{m}[ Choose target and write... ]"),
                        style,
                    ));
                    continue;
                }
                Field::Start => {
                    lines.push(Line::raw(""));
                    if sel {
                        focus_line = lines.len();
                    }
                    let style = if sel {
                        pal.accent()
                            .add_modifier(Modifier::REVERSED | Modifier::BOLD)
                    } else {
                        pal.accent()
                    };
                    let why = c.validate().err().unwrap_or_default();
                    lines.push(Line::from(vec![
                        Span::styled(format!("{m}[ Start restore ]"), style),
                        Span::styled(format!("  {why}"), pal.dim()),
                    ]));
                    continue;
                }
            };
            lines.push(Line::from(vec![
                Span::styled(format!("{m}{label:<11}"), style),
                Span::raw(value),
                cursor,
            ]));
            // What each row found, under it.
            match f {
                Field::Backup => {
                    if let Some(snap) = c.loading() {
                        dim(lines, format!("{}...", snap.label));
                    }
                    if let Some(err) = c.load_error() {
                        warn(lines, err.to_string());
                    }
                    if c.mode() == RestoreMode::FullDisk {
                        if let Some((info, source)) = c.backup_summary() {
                            dim(lines, info);
                            dim(lines, format!("Source: {source}"));
                        }
                    }
                }
                Field::SourceImage => {
                    if let Some(snap) = c.decoding() {
                        dim(lines, snap.phase);
                    } else if !c.has_native_backup() && c.source_image().is_some() {
                        dim(lines, "Source: entire image file".into());
                    }
                }
                Field::RemoteHost => {
                    if c.remote_busy() {
                        dim(lines, "Connecting...".into());
                    }
                    if let Some(err) = c.remote_error() {
                        warn(lines, err.to_string());
                    }
                    if c.remote_connected() && c.remote_drives().is_empty() {
                        dim(
                            lines,
                            "No physical drives reported (the daemon may need to run as root)."
                                .into(),
                        );
                    }
                }
                Field::RemoteImage => {
                    if let Some(label) = c.remote_target_label() {
                        dim(lines, format!("Selected target: {label}"));
                    }
                }
                Field::TargetDevice | Field::TargetImage => {
                    if c.device(slot).is_some_and(|d| d.is_system) && f == Field::TargetDevice {
                        warn(
                            lines,
                            "WARNING: This is a system disk! Restoring will destroy your OS."
                                .into(),
                        );
                    }
                    if f == Field::TargetDevice {
                        if let Some(note) = self.device_note() {
                            lines.push(note);
                        }
                    }
                    if c.mode() == RestoreMode::FullDisk {
                        self.restore_size_line(r, lines);
                    }
                }
                Field::Scan => {
                    if let Some(err) = c.target_scan_error() {
                        warn(lines, err.to_string());
                    }
                }
                Field::WriteSource => {
                    if let Some(snap) = c.probing() {
                        dim(lines, snap.phase);
                    }
                    match c.write_image() {
                        Some((_, n)) if n > 0 => {
                            dim(lines, format!("Decoded size: {} (the file may be smaller if compressed)", format_size(n)));
                            if let Some(fs) = c.write_image_superfloppy() {
                                dim(lines, format!("A {fs} volume with no partition table: the writer can wrap it in one."));
                            }
                        }
                        Some(_) => warn(lines, "That file could not be opened as a disk image.".into()),
                        None => dim(lines, "Any readable format works: raw, DMG, CHD, VHD, 2MG, GHO, IMZ, gzip / zip.".into()),
                    }
                }
                Field::ExtendLast => self.restore_expand_preview(r, lines),
                _ => {}
            }
        }
        lines.push(Line::raw(""));
        for c in r.ctl.size_caveats() {
            let style = match c.caveat.level {
                CaveatLevel::Info => pal.dim(),
                CaveatLevel::Warning | CaveatLevel::Blocked => pal.warn(),
            };
            lines.push(Line::styled(format!("  {}", c.line()), style));
        }
        if let Some(last) = r.log.last() {
            lines.push(Line::styled(format!("  {last}"), pal.dim()));
        }
        if let Some((res, is_error)) = r.result() {
            let style = if is_error { pal.warn() } else { pal.accent() };
            lines.push(Line::styled(format!("  {res}"), style));
        }
        lines.push(Line::styled(
            "Up/Down row  Left/Right change  Enter act  Tab browse  s start  ? keys",
            pal.dim(),
        ));
        focus_line
    }

    /// "Target: x | Source was: y" under a full-disk target.
    fn restore_size_line(&self, r: &RestoreState, lines: &mut Vec<Line<'static>>) {
        let c = &r.ctl;
        let (Some(source), target) = (c.source_size(), c.target_size()) else {
            return;
        };
        if target == 0 {
            return;
        }
        let small = target < source;
        lines.push(Line::styled(
            format!(
                "             Target: {} | Source was: {}{}",
                format_size(target),
                format_size(source),
                if small { " (target is smaller!)" } else { "" }
            ),
            if small {
                self.palette.warn()
            } else {
                self.palette.dim()
            },
        ));
    }

    /// The partition plan before and after the free space, in words (the GUI draws bars).
    fn restore_expand_preview(&self, r: &RestoreState, lines: &mut Vec<Line<'static>>) {
        let c = &r.ctl;
        let segs = c.plan_segments();
        let added = c.expand_free_bytes();
        let now: Vec<String> = segs
            .iter()
            .map(|s| format!("{} {}", s.label, format_size(s.size_bytes)))
            .collect();
        let mut after = now.clone();
        if added > 0 {
            if c.expand().extend_last {
                if let (Some(last), Some(s)) = (after.last_mut(), segs.last()) {
                    *last = format!("{} {}", s.label, format_size(s.size_bytes + added));
                }
            } else {
                after.push(format!("free {}", format_size(added)));
            }
        }
        let pal = self.palette;
        lines.push(Line::styled(
            format!("             Current: {}", now.join(" | ")),
            pal.dim(),
        ));
        lines.push(Line::styled(
            format!("             After:   {}", after.join(" | ")),
            pal.dim(),
        ));
    }

    /// The size total and the engine's layout check, under the last size row.
    fn restore_fit_lines(&self, r: &RestoreState, lines: &mut Vec<Line<'static>>) {
        let c = &r.ctl;
        let target = c.target_size();
        if target == 0 {
            return;
        }
        let pal = self.palette;
        let total = c.fixed_total();
        lines.push(Line::styled(
            format!(
                "             Total: {} / {} available",
                format_size(total),
                format_size(target)
            ),
            if total <= target {
                pal.dim()
            } else {
                pal.warn()
            },
        ));
        match c.layout_fit() {
            Some(LayoutFit::Refused { message, original }) => {
                lines.push(Line::styled(
                    format!("             This layout will not fit: {message}"),
                    pal.warn(),
                ));
                if original {
                    lines.push(Line::styled(
                        "             Original alignment keeps every start; choose 1 MB or Custom to repack.",
                        pal.warn(),
                    ));
                }
            }
            Some(LayoutFit::Overrun { end, target }) => lines.push(Line::styled(
                format!(
                    "             Layout ends at {} but the target holds {}",
                    format_size(end),
                    format_size(target)
                ),
                pal.warn(),
            )),
            Some(LayoutFit::Fits) | None => {}
        }
    }

    fn restore_run_lines(&self, r: &RestoreState, lines: &mut Vec<Line<'static>>) {
        let pal = self.palette;
        let c = &r.ctl;
        if r.writing {
            if let Some(src) = r.writer.source() {
                lines.push(Line::from(vec![
                    Span::styled("Image:  ", pal.dim()),
                    Span::raw(src.path.display().to_string()),
                ]));
            }
            let target = r
                .writer
                .target_device()
                .map(|d| d.display_name())
                .or_else(|| r.writer.target_image().map(|p| p.display().to_string()))
                .unwrap_or_default();
            lines.push(Line::from(vec![
                Span::styled("Target: ", pal.dim()),
                Span::raw(target),
            ]));
        } else {
            let source = c
                .backup_folder()
                .or_else(|| c.source_image())
                .map(|p| p.display().to_string())
                .unwrap_or_default();
            lines.push(Line::from(vec![
                Span::styled("Source: ", pal.dim()),
                Span::raw(source),
            ]));
        }
        lines.push(Line::raw(""));
        if let Some(bar) = c.progress().filter(|_| !r.writing) {
            lines.push(Line::from(vec![
                Span::styled("Step: ", pal.dim()),
                Span::raw(bar.operation),
            ]));
        } else if let Some(snap) = r.writer.progress().filter(|_| r.writing) {
            lines.push(Line::from(vec![
                Span::styled("Step: ", pal.dim()),
                Span::raw(if snap.phase.is_empty() {
                    snap.label
                } else {
                    snap.phase
                }),
            ]));
        }
        for msg in &r.log {
            lines.push(Line::styled(format!("  {msg}"), pal.dim()));
        }
        if let Some((res, is_error)) = r.result() {
            lines.push(Line::raw(""));
            let style = if is_error { pal.warn() } else { pal.accent() };
            lines.push(Line::styled(format!("  {res}"), style));
            lines.push(Line::styled("  Esc to return to the form.", pal.dim()));
        }
    }
}
