//! The TUI Optical screen: a disc from a drive (local, or a remote daemon's) or an image file,
//! ripped or converted over a headless `OpticalController`.
//!
//! The same controller drives the egui Optical tab, so the screen offers its source (the drive
//! list with Refresh, Eject and Add remote daemon, or an image file with the recent list), the
//! detected disc's info line, Browse Contents (the disc in the Explorer under
//! `PaneProfile::optical`), Close Disc, CHD Info and the output form (ISO, BIN/CUE or CHD with the
//! CD codec control, the output file, Eject after ripping) with Start and Cancel. The form's rows
//! are a list of [`ORow`]s; keys act on the focused one. The CD-DA player stays in the GUI. See
//! docs/phase2_commander_plan.md § "Slice 28".

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crossterm::event::KeyCode;
use ratatui::layout::Rect;
use ratatui::style::Modifier;
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::Paragraph;
use ratatui::Frame;

use super::ask::Ask;
use super::commander::CommanderView;
use super::{
    expand_tilde, hint, App, FilePicker, KeyHint, PickKind, PickResult, Progress, ProgressShared,
};
#[cfg(feature = "chd")]
use crate::controller::app::chd::{
    codec_choices, codec_label, hunk_presets, modes_for, unit_size_for, ChdOptionsControl,
    ChdOptionsMode,
};
use crate::controller::app::{RateTracker, RecentHandle, RecentMode};
#[cfg(feature = "chd")]
use crate::controller::optical::ChdProfile;
use crate::controller::optical::{
    hints, labels, purpose, ChdChoice, DiscSource, HostRequest, NoNotify, OpticalAction,
    OpticalController, SourceMode, Task, CHD_NOT_BUILT,
};
use crate::controller::{Controller as _, Effect, PickSpec};

/// One row of the form.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum ORow {
    Source,
    Drive,
    Image,
    Format,
    ChdMode,
    ChdHunk,
    ChdCodec(usize),
    Output,
    EjectAfter,
    Start,
    /// An image's format changes in the Convert tab: this row hands it over.
    Convert,
}

/// What a confirmed picker answers.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Pick {
    Image,
    Output,
}

pub(super) const HINTS_OPTICAL: &[KeyHint] = &[
    hint("Up/Down", "Move between rows"),
    hint(
        "Left/Right",
        "Change the source, drive, output format or a CHD choice",
    ),
    hint("Space", "Change the row's choice (Eject after ripping)"),
    hint(
        "Enter",
        "Act on the row: choose the image, browse the output, Start",
    ),
    hint("Tab", "Browse for the output file (Output row)"),
    hint("o", "Open a disc image"),
    hint(
        "r",
        "Refresh the drive list (and connected daemons' drives)",
    ),
    hint("e", "Eject the selected local drive's disc"),
    hint("R", "Add remote daemon: a drive on an rb-cli serve host"),
    hint("b", "Browse Contents: the disc in the Explorer"),
    hint("c", "Close Disc"),
    hint("C", "Convert the image in the Convert tab"),
    hint("i", "CHD Info (a .chd image)"),
    hint("s", "Start the rip or conversion"),
    hint(
        "Esc",
        "Cancel a running rip or conversion; close a finished bar",
    ),
];

pub(super) const HINTS_OPTICAL_WINDOW: &[KeyHint] = &[
    hint(
        "Up/Down",
        "Scroll CHD Info, or move through the recent daemons",
    ),
    hint("Enter", "Connect to the typed or highlighted daemon"),
    hint("Esc", "Close the window"),
];

pub(super) struct OpticalState {
    pub(super) ctl: OpticalController,
    /// The app's recent-files store; an opened image is recorded through it.
    recent: RecentHandle,
    pub(super) focus: ORow,
    /// The Output row as typed; the controller gets it at once (`~` expanded).
    pub(super) output: String,
    #[cfg(feature = "chd")]
    pub(super) chd: ChdOptionsControl,
    picker: Option<(Pick, FilePicker)>,
    /// Start's question about replacing files.
    ask: Option<Ask<()>>,
    /// The remote window's typed address and the highlighted recent address.
    pub(super) remote_addr: String,
    remote_sel: Option<usize>,
    chd_scroll: usize,
    /// The controller's latest log lines.
    pub(super) log: Vec<String>,
    /// A line the screen shows until the next action (a refusal, a hand-off it cannot take).
    pub(super) note: Option<String>,
    /// The disc browser to open, or that it should close (`HostRequest`s for the app).
    browse: Option<DiscSource>,
    /// The disc image `C` hands to the Convert tab.
    convert: Option<std::path::PathBuf>,
    close_browser: bool,
    /// A run is on screen: "Rip" or "Conversion"; its end becomes the result line.
    running: Option<&'static str>,
    pub(super) result: Option<(String, bool)>,
}

impl OpticalState {
    /// A fresh screen whose drive scan has started.
    pub(super) fn new(recent: RecentHandle) -> Self {
        let mut ctl = OpticalController::new(Arc::new(NoNotify));
        ctl.apply(OpticalAction::FindDrives);
        OpticalState {
            ctl,
            recent,
            focus: ORow::Source,
            output: String::new(),
            #[cfg(feature = "chd")]
            chd: {
                let mut c = ChdOptionsControl::new(ChdProfile::Cd);
                c.custom = OpticalController::saved_chd_custom(c.custom.clone());
                c
            },
            picker: None,
            ask: None,
            remote_addr: String::new(),
            remote_sel: None,
            chd_scroll: 0,
            log: Vec::new(),
            note: None,
            browse: None,
            convert: None,
            close_browser: false,
            running: None,
            result: None,
        }
    }

    pub(super) fn act(&mut self, action: OpticalAction) {
        let fx = self.ctl.apply(action);
        self.absorb(fx);
        self.after();
    }

    /// A job runs whose end the screen waits for, so the event loop polls faster.
    pub(super) fn busy(&self) -> bool {
        self.ctl.is_running()
            || self.ctl.scanning_drives()
            || self.ctl.detecting().is_some()
            || self.ctl.chd_info_running()
            || self.ctl.remote_connecting().is_some()
    }

    fn absorb(&mut self, fx: Vec<Effect>) {
        for e in fx {
            match e {
                Effect::PickFile(spec) => self.open_picker(spec),
                Effect::Message(msg) => self.log.push(msg),
                Effect::Confirm(spec) => self.ask = Some(Ask::Confirm((), spec)),
                // The Optical controller asks for nothing else.
                Effect::PickFolder(_)
                | Effect::AskText(_)
                | Effect::AskPassword(_)
                | Effect::Choose(_)
                | Effect::OpenUrl(_)
                | Effect::CopyToClipboard(_)
                | Effect::CloseScreen
                | Effect::Quit => {}
            }
        }
    }

    fn open_picker(&mut self, spec: PickSpec) {
        if spec.purpose == purpose::OUTPUT {
            let mut p = FilePicker::new(PickKind::Save, spec.title.as_str());
            // Beside the image (else the working folder), as a save dialog would open.
            let dir = self
                .ctl
                .image()
                .and_then(|p| p.parent())
                .map(|p| p.to_path_buf())
                .or_else(|| std::env::current_dir().ok())
                .unwrap_or_default();
            p.input = if self.output.trim().is_empty() {
                dir.join(spec.file_name.as_str()).display().to_string()
            } else {
                self.output.clone()
            };
            self.picker = Some((Pick::Output, p));
        } else {
            let recent = self.recent.list(RecentMode::Optical);
            let p = FilePicker::new(PickKind::File, spec.title.as_str()).with_recent(recent);
            self.picker = Some((Pick::Image, p));
        }
    }

    fn picked(&mut self, pick: Pick, path: Option<PathBuf>) {
        let purpose = match pick {
            Pick::Image => purpose::IMAGE,
            Pick::Output => purpose::OUTPUT,
        };
        if let (Pick::Output, Some(p)) = (pick, path.as_ref()) {
            self.output = p.display().to_string();
        }
        self.act(OpticalAction::Picked { purpose, path });
    }

    /// Keep the controller's requests and log lines, and land a finished run.
    fn after(&mut self) {
        for req in self.ctl.take_requests() {
            match req {
                HostRequest::RememberRecent(path) => {
                    let path = path.display().to_string();
                    self.recent.remember(RecentMode::Optical, path.as_str());
                }
                HostRequest::OpenBrowser(src) => {
                    self.browse = Some(src);
                    self.close_browser = false;
                }
                HostRequest::CloseBrowser => {
                    self.browse = None;
                    self.close_browser = true;
                }
            }
        }
        let lines = self.ctl.take_log();
        self.log.extend(lines.into_iter().map(|(_, msg)| msg));
        let n = self.log.len();
        if n > 6 {
            self.log.drain(..n - 6);
        }
        if let Some(what) = self.running.filter(|_| !self.ctl.is_running()) {
            self.running = None;
            self.result = match self.ctl.last_result() {
                Some(Ok(())) => Some((format!("{what} complete."), false)),
                Some(Err(e)) => Some((format!("{what} failed: {e}"), true)),
                None => None,
            };
        }
        let rows = self.rows();
        if !rows.contains(&self.focus) {
            self.focus = match self.focus {
                ORow::Drive | ORow::Image => rows[1],
                _ => rows[2],
            };
        }
    }

    fn chd_shown(&self) -> bool {
        cfg!(feature = "chd")
            && self.ctl.output_format() == crate::controller::optical::OutputFormat::Chd
    }

    #[cfg(feature = "chd")]
    fn chd_custom(&self) -> bool {
        self.chd.mode == ChdOptionsMode::Custom
    }

    #[cfg(not(feature = "chd"))]
    fn chd_custom(&self) -> bool {
        false
    }

    /// The form's rows, in order, for the current source and output.
    pub(super) fn rows(&self) -> Vec<ORow> {
        let mut rows = vec![ORow::Source];
        rows.push(match self.ctl.source_mode() {
            SourceMode::PhysicalDrive => ORow::Drive,
            SourceMode::ImageFile => ORow::Image,
        });
        if self.ctl.task() == Task::Convert {
            rows.push(ORow::Convert);
            return rows;
        }
        rows.push(ORow::Format);
        if self.chd_shown() {
            rows.push(ORow::ChdMode);
            if self.chd_custom() {
                rows.push(ORow::ChdHunk);
                rows.extend((0..4).map(ORow::ChdCodec));
            }
        }
        rows.push(ORow::Output);
        if self.ctl.task() == Task::Rip {
            rows.push(ORow::EjectAfter);
        }
        rows.push(ORow::Start);
        rows
    }

    fn move_focus(&mut self, delta: isize) {
        let rows = self.rows();
        let at = rows.iter().position(|r| *r == self.focus).unwrap_or(0) as isize;
        let n = rows.len() as isize;
        self.focus = rows[((at + delta).rem_euclid(n)) as usize];
    }

    /// Step the focused row's choice; `false` leaves Left / Right to tab switching.
    fn change(&mut self, row: ORow, forward: bool) -> bool {
        match row {
            ORow::Source => {
                let mode = match self.ctl.source_mode() {
                    SourceMode::PhysicalDrive => SourceMode::ImageFile,
                    SourceMode::ImageFile => SourceMode::PhysicalDrive,
                };
                self.act(OpticalAction::SetSourceMode(mode));
            }
            ORow::Drive => {
                let n = self.ctl.drive_count();
                if n > 0 {
                    let next = match self.ctl.selected_drive() {
                        Some(i) if forward => (i + 1) % n,
                        Some(i) => (i + n - 1) % n,
                        None => 0,
                    };
                    self.act(OpticalAction::SelectDrive(Some(next)));
                }
            }
            ORow::Format => {
                let offered: Vec<_> = self
                    .ctl
                    .format_choices()
                    .into_iter()
                    .filter(|(_, on)| *on)
                    .map(|(f, _)| f)
                    .collect();
                if !offered.is_empty() {
                    let next = step(offered.as_slice(), self.ctl.output_format(), forward);
                    self.act(OpticalAction::SetOutputFormat(next));
                    // The controller renames an output that carried the old format's extension.
                    if let Some(path) = self.ctl.output() {
                        self.output = path.display().to_string();
                    }
                }
            }
            ORow::ChdMode | ORow::ChdHunk | ORow::ChdCodec(_) => {
                return self.change_chd(row, forward)
            }
            ORow::EjectAfter => {
                let on = !self.ctl.eject_after();
                self.act(OpticalAction::SetEjectAfter(on));
            }
            ORow::Image | ORow::Output | ORow::Start | ORow::Convert => return false,
        }
        true
    }

    #[cfg(feature = "chd")]
    fn change_chd(&mut self, row: ORow, forward: bool) -> bool {
        match row {
            ORow::ChdMode => {
                let modes = modes_for(ChdProfile::Cd);
                self.chd.mode = step(modes.as_slice(), self.chd.mode, forward);
            }
            ORow::ChdHunk => {
                let cur = self.chd.custom.hunk_size;
                self.chd.custom.hunk_size = step(hunk_presets(ChdProfile::Cd), cur, forward);
            }
            ORow::ChdCodec(slot) => {
                let cur = self.chd.custom.codecs[slot];
                let choices = codec_choices(ChdProfile::Cd);
                self.chd.custom.codecs[slot] = step(choices.as_slice(), cur, forward);
            }
            _ => return false,
        }
        self.note = None;
        true
    }

    #[cfg(not(feature = "chd"))]
    fn change_chd(&mut self, _row: ORow, _forward: bool) -> bool {
        false
    }

    /// The CHD options Start carries, or why they cannot be used.
    #[cfg(feature = "chd")]
    fn chd_choice(&self) -> Result<Option<ChdChoice>, String> {
        if !self.chd_shown() {
            return Ok(None);
        }
        self.chd
            .validate(ChdProfile::Cd)
            .map_err(|e| format!("CHD options: {e}."))?;
        Ok(Some(ChdChoice {
            options: self.chd.effective(ChdProfile::Cd),
            custom: self.chd_custom(),
        }))
    }

    #[cfg(not(feature = "chd"))]
    fn chd_choice(&self) -> Result<Option<ChdChoice>, String> {
        Ok(None)
    }

    /// The typed output file; the controller gets it at once.
    pub(super) fn set_output(&mut self, text: String) {
        let path = expand_tilde(text.trim());
        if path.as_os_str().is_empty() {
            self.ctl.clear_output();
        } else {
            self.ctl.set_output_path(path);
        }
        self.output = text;
    }

    /// Press Start: the CHD options first, then the controller's own refusal.
    /// Give the open image to the Convert tab, where a disc's format changes.
    fn hand_to_convert(&mut self) {
        match self.ctl.convert_source() {
            Some(path) => self.convert = Some(path),
            None if self.ctl.task() == Task::Convert => {
                self.note = Some("Choose a disc image first.".into())
            }
            None => self.note = Some("Convert takes a disc image; a drive is ripped here.".into()),
        }
    }

    pub(super) fn start(&mut self) {
        self.note = None;
        if self.ctl.task() == Task::Convert {
            self.note = Some(hints::CONVERT_MOVED.to_string());
            return;
        }
        let chd = match self.chd_choice() {
            Ok(c) => c,
            Err(e) => {
                self.note = Some(e);
                return;
            }
        };
        if let Some(why) = self.ctl.start_refusal() {
            self.note = Some(format!("Cannot start: {why}"));
            return;
        }
        self.result = None;
        self.act(OpticalAction::Start(chd));
        self.note_run();
    }

    /// A run started, at Start or on the answer to its question: its end becomes the result line.
    fn note_run(&mut self) {
        if self.ctl.is_running() {
            self.running = Some(match self.ctl.task() {
                Task::Rip => "Rip",
                Task::Convert => "Conversion",
            });
        }
    }

    /// One key for Start's question; the answer goes back to the controller.
    fn ask_key(&mut self, code: KeyCode) {
        let Some(ask) = self.ask.as_mut() else {
            return;
        };
        let Some((yes, _)) = ask.key(code) else {
            return;
        };
        if let Some(Ask::Confirm((), spec)) = self.ask.take() {
            let purpose = spec.purpose;
            self.act(OpticalAction::Confirmed { purpose, yes });
            self.note_run();
        }
    }

    fn chd_info_key(&mut self, code: KeyCode) {
        match code {
            KeyCode::Esc | KeyCode::Enter => {
                self.chd_scroll = 0;
                self.act(OpticalAction::CloseChdInfo);
            }
            KeyCode::Up | KeyCode::Char('k') => self.chd_scroll = self.chd_scroll.saturating_sub(1),
            KeyCode::Down | KeyCode::Char('j') => self.chd_scroll += 1,
            KeyCode::PageUp => self.chd_scroll = self.chd_scroll.saturating_sub(10),
            KeyCode::PageDown => self.chd_scroll += 10,
            _ => {}
        }
    }

    fn remote_key(&mut self, code: KeyCode) {
        let recent = self.ctl.recent_daemons().to_vec();
        let connecting = self.ctl.remote_connecting().is_some();
        match code {
            KeyCode::Esc => {
                self.remote_addr.clear();
                self.remote_sel = None;
                self.act(OpticalAction::CloseAddRemote);
            }
            _ if connecting => {}
            KeyCode::Enter => {
                let addr = match self.remote_sel.and_then(|i| recent.get(i)) {
                    Some(a) => a.clone(),
                    None => self.remote_addr.clone(),
                };
                self.remote_addr = addr.clone();
                self.remote_sel = None;
                self.act(OpticalAction::ConnectRemote(addr));
            }
            KeyCode::Down if !recent.is_empty() => {
                let last = recent.len() - 1;
                self.remote_sel = Some(self.remote_sel.map_or(0, |i| (i + 1).min(last)));
            }
            KeyCode::Up => self.remote_sel = self.remote_sel.and_then(|i| i.checked_sub(1)),
            KeyCode::Backspace => {
                self.remote_sel = None;
                self.remote_addr.pop();
            }
            KeyCode::Char(c) if !c.is_control() => {
                self.remote_sel = None;
                self.remote_addr.push(c);
            }
            _ => {}
        }
    }

    /// Keys of the form itself; `false` hands a key to the tab shell.
    fn form_key(&mut self, code: KeyCode) -> bool {
        let focus = self.focus;
        let text = focus == ORow::Output;
        match code {
            KeyCode::Down => self.move_focus(1),
            KeyCode::Up | KeyCode::BackTab => self.move_focus(-1),
            KeyCode::Enter => match focus {
                ORow::Image => self.act(OpticalAction::PickImage),
                ORow::Output => self.act(OpticalAction::PickOutput),
                ORow::Start => self.start(),
                ORow::Convert => self.hand_to_convert(),
                ORow::Source
                | ORow::Drive
                | ORow::Format
                | ORow::ChdMode
                | ORow::ChdHunk
                | ORow::ChdCodec(_)
                | ORow::EjectAfter => {
                    self.change(focus, true);
                }
            },
            KeyCode::Tab if text => self.act(OpticalAction::PickOutput),
            KeyCode::Left | KeyCode::Right => return self.change(focus, code == KeyCode::Right),
            KeyCode::Char(' ') if !text => {
                self.change(focus, true);
            }
            KeyCode::Backspace if text => {
                let mut t = self.output.clone();
                t.pop();
                self.set_output(t);
            }
            KeyCode::Char(c) if text && !c.is_control() => {
                let mut t = self.output.clone();
                t.push(c);
                self.set_output(t);
            }
            KeyCode::Char('o') => self.act(OpticalAction::PickImage),
            KeyCode::Char('r') => self.act(OpticalAction::RefreshDrives),
            KeyCode::Char('e') => {
                if self.ctl.can_eject() {
                    self.act(OpticalAction::Eject);
                } else {
                    self.note = Some("Select a local drive with a disc in it to eject.".into());
                }
            }
            KeyCode::Char('R') => {
                self.remote_addr.clear();
                self.remote_sel = None;
                self.act(OpticalAction::OpenAddRemote);
            }
            KeyCode::Char('b') => {
                if self.ctl.disc().is_some() {
                    self.act(OpticalAction::BrowseContents);
                } else {
                    self.note = Some("No disc is detected to browse.".into());
                }
            }
            KeyCode::Char('c') => self.act(OpticalAction::CloseDisc),
            KeyCode::Char('C') => self.hand_to_convert(),
            KeyCode::Char('i') => {
                if self.ctl.can_chd_info() {
                    self.act(OpticalAction::ChdInfo);
                } else if self.ctl.is_chd_image() && !cfg!(feature = "chd") {
                    self.note = Some(CHD_NOT_BUILT.to_string());
                } else {
                    self.note = Some("CHD Info reads a .chd image.".into());
                }
            }
            KeyCode::Char('s') => self.start(),
            _ => return false,
        }
        true
    }
}

/// The next (or previous) entry of `list` after `cur`, wrapping; the first when `cur` is absent.
fn step<T: Copy + PartialEq>(list: &[T], cur: T, forward: bool) -> T {
    let n = list.len();
    match list.iter().position(|x| *x == cur) {
        Some(i) if forward => list[(i + 1) % n],
        Some(i) => list[(i + n - 1) % n],
        None => list[0],
    }
}

impl App {
    /// The Optical screen's name and key list, for the `?` window.
    pub(super) fn optical_key_context(&self) -> (&'static str, &'static [KeyHint]) {
        match self.optical.as_ref() {
            Some(o) if o.ctl.chd_info().is_some() || o.ctl.add_remote_open() => {
                ("Optical - window", HINTS_OPTICAL_WINDOW)
            }
            _ => ("Optical", HINTS_OPTICAL),
        }
    }

    /// The footer's short key strip for the Optical screen.
    pub(super) fn optical_footer_keys(&self) -> Vec<(&'static str, &'static str)> {
        let Some(o) = self.optical.as_ref() else {
            return Vec::new();
        };
        if o.ask.is_some() {
            return vec![("y", "Yes"), ("n", "No")];
        }
        if o.ctl.chd_info().is_some() {
            return vec![("Up/Dn", "Scroll"), ("Esc", "Close"), ("?", "Keys")];
        }
        if o.ctl.add_remote_open() {
            return vec![("Enter", "Connect"), ("Up/Dn", "Recent"), ("Esc", "Close")];
        }
        if o.ctl.is_running() {
            return vec![("Esc", "Cancel")];
        }
        vec![
            ("Up/Dn", "Row"),
            ("<-/->", "Change"),
            ("Enter", "Act"),
            ("b", "Browse"),
            ("s", "Start"),
            ("?", "Keys"),
        ]
    }

    fn optical_state(&mut self) -> &mut OpticalState {
        if self.optical.is_none() {
            self.optical = Some(OpticalState::new(self.shell.recent_handle()));
        }
        self.optical.as_mut().unwrap()
    }

    /// Open `path` as the image-file source: an optical disc the Inspect open handed over.
    pub(super) fn optical_open_image(&mut self, path: PathBuf) {
        self.optical_state().act(OpticalAction::OpenImage(path));
        self.after_optical();
    }

    /// Optical screen keys. Returns `true` when consumed.
    pub(super) fn handle_optical_key(&mut self, code: KeyCode) -> bool {
        let o = self.optical_state();
        if let Some((pick, picker)) = o.picker.as_mut() {
            let pick = *pick;
            match picker.handle_key(code) {
                Some(PickResult::Cancel) => {
                    o.picker = None;
                    o.picked(pick, None);
                }
                Some(PickResult::Confirm(path)) => {
                    o.picker = None;
                    o.picked(pick, Some(path));
                }
                None => {}
            }
            self.after_optical();
            return true;
        }
        if o.ask.is_some() {
            o.ask_key(code);
            self.show_optical_bar();
            self.after_optical();
            return true;
        }
        // `?` and F1 reach the key list from every window, unless `?` is typed into a field.
        let typing = o.ctl.add_remote_open() || o.focus == ORow::Output;
        if code == KeyCode::F(1) || (code == KeyCode::Char('?') && !typing) {
            return false;
        }
        if o.ctl.chd_info().is_some() {
            o.chd_info_key(code);
            return true;
        }
        if o.ctl.add_remote_open() {
            o.remote_key(code);
            return true;
        }
        if o.ctl.is_running() {
            if code == KeyCode::Esc {
                self.cancel_progress();
            }
            return true;
        }
        if code == KeyCode::Esc && self.progress.is_some() {
            self.progress = None;
            return true;
        }
        let o = self.optical_state();
        let consumed = o.form_key(code);
        self.show_optical_bar();
        self.after_optical();
        consumed
    }

    /// Put up the bar once a run has started.
    fn show_optical_bar(&mut self) {
        let Some(o) = self.optical.as_ref() else {
            return;
        };
        let label = match o.ctl.task() {
            Task::Rip => "Ripping",
            Task::Convert => "Converting",
        };
        if o.running.is_some() && self.progress.is_none() {
            self.progress = Some(Progress {
                shared: Arc::new(Mutex::new(ProgressShared::default())),
                tracker: RateTracker::default(),
                label: label.to_string(),
            });
        }
    }

    /// Drive the controller and mirror its run into the bar.
    pub(super) fn optical_tick(&mut self) {
        let Some(o) = self.optical.as_mut() else {
            return;
        };
        let fx = o.ctl.tick();
        o.absorb(fx);
        let was_running = o.running.is_some();
        o.after();
        let bar = o.ctl.progress();
        let ended = was_running && o.running.is_none();
        if let Some(prog) = self.progress.as_ref().filter(|_| bar.is_some() || ended) {
            if let Ok(mut sh) = prog.shared.lock() {
                if let Some(bar) = &bar {
                    sh.current = bar.current_bytes;
                    sh.total = bar.total_bytes;
                }
                sh.done = ended;
            }
        }
        self.after_optical();
    }

    /// Open or close the disc browser as the controller asked, and report the user's close.
    fn after_optical(&mut self) {
        let Some(o) = self.optical.as_mut() else {
            return;
        };
        let disc_open = self.explorer.as_ref().is_some_and(|e| e.is_disc());
        if std::mem::take(&mut o.close_browser) && disc_open {
            if let Some(ex) = self.explorer.take() {
                ex.release();
            }
        }
        if let Some(src) = o.browse.take() {
            let label = match o.ctl.disc() {
                Some(d) => match d.volume {
                    Some(v) => format!("{v} ({})", d.format),
                    None => d.format,
                },
                None => src.path.display().to_string(),
            };
            self.explorer = Some(CommanderView::disc_open(src, label));
            self.status = None;
        }
        let disc_open = self.explorer.as_ref().is_some_and(|e| e.is_disc());
        if o.ctl.is_browsing() && !disc_open {
            o.act(OpticalAction::BrowserClosed);
        }
        if let Some(path) = o.convert.take() {
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

    pub(super) fn draw_optical(&self, frame: &mut Frame, area: Rect, o: &OpticalState) {
        let mut lines: Vec<Line<'static>> = Vec::new();
        let focus_line = self.optical_lines(o, &mut lines);
        let inner = area.height.saturating_sub(2) as usize;
        let scroll = if inner > 2 && focus_line + 3 > inner {
            (focus_line + 3 - inner) as u16
        } else {
            0
        };
        frame.render_widget(
            Paragraph::new(Text::from(lines))
                .block(self.pane_block("Optical Disc", true))
                .scroll((scroll, 0)),
            area,
        );
        if let Some((_, picker)) = &o.picker {
            picker.draw(frame, area, self.palette, self.border);
        }
        if let Some(text) = o.ctl.chd_info() {
            self.draw_optical_chd_info(frame, area, o, text);
        }
        if o.ctl.add_remote_open() {
            self.draw_optical_remote(frame, area, o);
        }
        if let Some(ask) = &o.ask {
            self.draw_ask(frame, area, ask, false);
        }
    }

    /// One form row: the focus marker, the label and its value.
    fn optical_row(&self, focused: bool, label: &str, value: String) -> Line<'static> {
        let pal = self.palette;
        let (marker, style) = if focused {
            ("> ", pal.accent().add_modifier(Modifier::BOLD))
        } else {
            ("  ", pal.accent())
        };
        Line::from(vec![
            Span::styled(format!("{marker}{label:<10}"), style),
            Span::raw(value),
        ])
    }

    /// The form's lines; returns the focused row's line.
    fn optical_lines(&self, o: &OpticalState, lines: &mut Vec<Line<'static>>) -> usize {
        let pal = self.palette;
        let ctl = &o.ctl;
        let mut focus_line = 0;
        for row in o.rows() {
            let focused = row == o.focus;
            if focused {
                focus_line = lines.len();
            }
            match row {
                ORow::Source => {
                    let mode = match ctl.source_mode() {
                        SourceMode::PhysicalDrive => "Physical drive",
                        SourceMode::ImageFile => "Image file",
                    };
                    lines.push(self.optical_row(focused, "Source:", format!("< {mode} >")));
                }
                ORow::Drive => {
                    let labels = ctl.drive_labels();
                    let value = match ctl.selected_drive().and_then(|i| labels.get(i)) {
                        Some(l) => format!("< {l} >"),
                        None if labels.is_empty() => "No optical drives found".to_string(),
                        None => "< Select a drive... >".to_string(),
                    };
                    lines.push(self.optical_row(focused, "Drive:", value));
                    let state = if ctl.scanning_drives() {
                        "Looking for optical drives...".to_string()
                    } else {
                        let n = ctl.drive_count();
                        let mut s = format!("{n} drive(s)   r Refresh   e Eject");
                        if OpticalController::remote_available() {
                            s.push_str("   R Add remote daemon");
                        }
                        s
                    };
                    lines.push(Line::styled(format!("            {state}"), pal.dim()));
                    if let Some(note) = self.device_note() {
                        lines.push(note);
                    }
                }
                ORow::Image => {
                    let value = ctl
                        .image()
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|| "No file selected  (Enter or o to choose)".into());
                    lines.push(self.optical_row(focused, "Image:", value));
                }
                ORow::Format => {
                    self.optical_disc_lines(o, lines);
                    lines.push(Line::raw(""));
                    let action = match ctl.task() {
                        Task::Rip => "Rip to disk",
                        Task::Convert => "Convert format",
                    };
                    lines.push(Line::from(vec![
                        Span::styled(format!("  {:<10}", "Action:"), pal.dim()),
                        Span::raw(action),
                    ]));
                    if focused {
                        focus_line = lines.len();
                    }
                    let current = ctl.output_format();
                    let offered: Vec<&str> = ctl
                        .format_choices()
                        .into_iter()
                        .filter(|(_, on)| *on)
                        .map(|(f, _)| f.label())
                        .collect();
                    let offered = offered.join(", ");
                    let mut value = format!("< {} >   offered: {offered}", current.label());
                    if let Some(why) = ctl.format_refusal(current) {
                        value.push_str(format!("   {why}").as_str());
                    }
                    lines.push(self.optical_row(focused, "Output:", value));
                }
                ORow::ChdMode | ORow::ChdHunk | ORow::ChdCodec(_) => {
                    let (label, value) = self.optical_chd_row(o, row);
                    lines.push(self.optical_row(focused, label, value));
                }
                ORow::Output => {
                    let mut spans = vec![
                        Span::styled(
                            format!("{}{:<10}", if focused { "> " } else { "  " }, "File:"),
                            if focused {
                                pal.accent().add_modifier(Modifier::BOLD)
                            } else {
                                pal.accent()
                            },
                        ),
                        Span::raw(o.output.clone()),
                    ];
                    if focused {
                        spans.push(Span::styled(
                            " ",
                            pal.accent().add_modifier(Modifier::REVERSED),
                        ));
                        spans.push(Span::styled("  (Tab browses)", pal.dim()));
                    }
                    lines.push(Line::from(spans));
                }
                ORow::EjectAfter => {
                    let mark = if ctl.eject_after() { "x" } else { " " };
                    let value =
                        format!("[{mark}] Eject after ripping (on the drive's own machine)");
                    lines.push(self.optical_row(focused, "", value));
                }
                ORow::Start => {
                    lines.push(Line::raw(""));
                    if focused {
                        focus_line = lines.len();
                    }
                    let style = if focused {
                        pal.accent()
                            .add_modifier(Modifier::REVERSED | Modifier::BOLD)
                    } else {
                        pal.accent()
                    };
                    let mut spans = vec![Span::styled(
                        format!(
                            "{}[ {} ]",
                            if focused { "> " } else { "  " },
                            ctl.start_label()
                        ),
                        style,
                    )];
                    if let Some(why) = ctl.start_refusal().filter(|_| !ctl.is_running()) {
                        spans.push(Span::styled(format!("   {why}"), pal.dim()));
                    }
                    lines.push(Line::from(spans));
                }
                ORow::Convert => {
                    self.optical_disc_lines(o, lines);
                    lines.push(Line::raw(""));
                    if focused {
                        focus_line = lines.len();
                    }
                    let style = if focused {
                        pal.accent()
                            .add_modifier(Modifier::REVERSED | Modifier::BOLD)
                    } else {
                        pal.accent()
                    };
                    let marker = if focused { "> " } else { "  " };
                    lines.push(Line::from(vec![
                        Span::styled(format!("{marker}[ {} ]", labels::CONVERT), style),
                        Span::styled(format!("   {}", hints::CONVERT_MOVED), pal.dim()),
                    ]));
                }
            }
        }
        if let Some(note) = &o.note {
            lines.push(Line::styled(format!("  {note}"), pal.warn()));
        }
        if let Some((text, err)) = &o.result {
            let style = if *err { pal.warn() } else { pal.accent() };
            lines.push(Line::styled(format!("  {text}"), style));
        }
        if !o.log.is_empty() {
            lines.push(Line::raw(""));
            for l in &o.log {
                lines.push(Line::styled(format!("  {l}"), pal.dim()));
            }
        }
        focus_line
    }

    /// The disc info line and what can be done with the disc.
    fn optical_disc_lines(&self, o: &OpticalState, lines: &mut Vec<Line<'static>>) {
        let pal = self.palette;
        let ctl = &o.ctl;
        let label = Span::styled(format!("  {:<10}", "Disc:"), pal.dim());
        if ctl.detecting().is_some() {
            lines.push(Line::from(vec![label, Span::raw("Detecting the disc...")]));
        } else if let Some(d) = ctl.disc() {
            let mut info = format!("Format: {}   FS: {}", d.format, d.filesystem);
            if let Some(v) = &d.volume {
                info.push_str(format!("   Volume: {v}").as_str());
            }
            if let Some(s) = &d.size {
                info.push_str(format!("   Size: {s}").as_str());
            }
            lines.push(Line::from(vec![label, Span::raw(info)]));
            if let Some(g) = &d.game {
                lines.push(Line::raw(format!("            Game: {g}")));
            }
            let mut acts = if ctl.is_browsing() {
                "(browsing)   c Close Disc".to_string()
            } else {
                "b Browse Contents   c Close Disc".to_string()
            };
            if ctl.can_chd_info() {
                acts.push_str("   i CHD Info");
            }
            lines.push(Line::styled(format!("            {acts}"), pal.dim()));
        } else if let Some(e) = ctl.disc_error() {
            lines.push(Line::from(vec![
                label,
                Span::styled(e.to_string(), pal.warn()),
            ]));
        } else {
            lines.push(Line::from(vec![
                label,
                Span::styled("No source selected", pal.dim()),
            ]));
        }
    }

    #[cfg(feature = "chd")]
    fn optical_chd_row(&self, o: &OpticalState, row: ORow) -> (&'static str, String) {
        match row {
            ORow::ChdMode => ("CHD:", format!("< {} >", o.chd.mode.label())),
            ORow::ChdHunk => {
                let h = o.chd.custom.hunk_size;
                let unit = unit_size_for(ChdProfile::Cd);
                let err = o
                    .chd
                    .validate(ChdProfile::Cd)
                    .err()
                    .map(|e| format!("  {e}"))
                    .unwrap_or_default();
                ("  Hunk:", format!("< {h} >  (unit {unit} bytes){err}"))
            }
            ORow::ChdCodec(slot) => {
                let label = ["  Codec 1:", "  Codec 2:", "  Codec 3:", "  Codec 4:"][slot.min(3)];
                (
                    label,
                    format!("< {} >", codec_label(o.chd.custom.codecs[slot])),
                )
            }
            _ => ("", String::new()),
        }
    }

    #[cfg(not(feature = "chd"))]
    fn optical_chd_row(&self, _o: &OpticalState, _row: ORow) -> (&'static str, String) {
        ("", String::new())
    }

    fn draw_optical_chd_info(&self, frame: &mut Frame, area: Rect, o: &OpticalState, text: &str) {
        let pal = self.palette;
        let height = area.height.saturating_sub(4) as usize;
        let all: Vec<&str> = text.lines().collect();
        let start = o.chd_scroll.min(all.len().saturating_sub(1));
        let mut lines: Vec<Line<'static>> = all
            .iter()
            .skip(start)
            .take(height.saturating_sub(3))
            .map(|l| Line::raw(format!(" {l}")))
            .collect();
        lines.push(Line::raw(""));
        lines.push(Line::styled(" Up/Down scroll   Esc close", pal.dim()));
        self.draw_cmd_box_auto(frame, area, 96, "CHD Info", lines);
    }

    fn draw_optical_remote(&self, frame: &mut Frame, area: Rect, o: &OpticalState) {
        let pal = self.palette;
        let ctl = &o.ctl;
        let mut lines = vec![
            Line::raw(""),
            Line::styled("  Daemon address (host:port):", pal.dim()),
            Line::from(vec![
                Span::raw(format!("  {}", o.remote_addr)),
                Span::styled(" ", pal.accent().add_modifier(Modifier::REVERSED)),
            ]),
        ];
        if ctl.remote_connecting().is_some() {
            lines.push(Line::styled("  Connecting...", pal.accent()));
        }
        if let Some(e) = ctl.add_remote_error() {
            lines.push(Line::styled(format!("  {e}"), pal.warn()));
        }
        if !ctl.recent_daemons().is_empty() {
            lines.push(Line::raw(""));
            lines.push(Line::styled("  Recent:", pal.dim()));
            for (i, addr) in ctl.recent_daemons().iter().enumerate() {
                let on = o.remote_sel == Some(i);
                let style = if on {
                    pal.accent()
                        .add_modifier(Modifier::REVERSED | Modifier::BOLD)
                } else {
                    pal.accent()
                };
                lines.push(Line::styled(format!("  {addr}"), style));
            }
        }
        lines.push(Line::raw(""));
        lines.push(Line::styled(
            "  Enter connect   Up/Down recent   Esc close",
            pal.dim(),
        ));
        self.draw_cmd_box_auto(frame, area, 64, "Add remote daemon", lines);
    }
}

#[cfg(all(test, feature = "native-zstd"))]
mod tests {
    use std::path::Path;
    use std::time::{Duration, Instant};

    use crossterm::event::{KeyEvent, KeyModifiers};

    use super::super::{TabId, TABS};
    use super::*;
    use crate::controller::commander::Side;
    use crate::controller::optical::OutputFormat;

    fn press(app: &mut App, code: KeyCode) {
        app.on_key(KeyEvent::new(code, KeyModifiers::NONE));
    }

    fn ctrl(app: &mut App, c: char) {
        app.on_key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL));
    }

    fn typing(app: &mut App, text: &str) {
        for c in text.chars() {
            press(app, KeyCode::Char(c));
        }
    }

    fn tab(id: TabId) -> usize {
        TABS.iter().position(|(t, _)| *t == id).unwrap()
    }

    fn optical_app() -> App {
        App::headless_on(tab(TabId::Optical))
    }

    fn o(app: &App) -> &OpticalState {
        app.optical.as_ref().expect("the Optical screen")
    }

    /// A committed disc fixture, unpacked into `dir`.
    fn fixture(dir: &Path, name: &str) -> PathBuf {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let packed = format!("tests/fixtures/optical/{name}.iso.zst");
        let packed = std::fs::read(root.join(packed.as_str())).unwrap();
        let iso = zstd::stream::decode_all(std::io::Cursor::new(packed)).unwrap();
        let path = dir.join(format!("{name}.iso"));
        std::fs::write(&path, iso).unwrap();
        path
    }

    /// The hybrid ISO 9660 + HFS+ disc.
    fn hybrid(dir: &Path) -> PathBuf {
        fixture(dir, "hybrid_rsrc")
    }

    /// Tick until the screen's jobs, the disc browser's and the app's task have ended.
    fn settle(app: &mut App) {
        let start = Instant::now();
        loop {
            app.tick();
            let busy = app.optical.as_ref().is_some_and(|o| o.busy())
                || app.explorer.as_ref().is_some_and(|e| e.is_working())
                || app.task.is_active();
            if !busy {
                app.tick();
                return;
            }
            assert!(
                start.elapsed() < Duration::from_secs(120),
                "a job never ended"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn focus_to(app: &mut App, row: ORow) {
        for _ in 0..16 {
            if o(app).focus == row {
                return;
            }
            press(app, KeyCode::Down);
        }
        panic!("no row {row:?} in {:?}", o(app).rows());
    }

    /// Open `iso` through the picker, as `o` does.
    fn open_image(app: &mut App, iso: &Path) {
        press(app, KeyCode::Char('o'));
        typing(app, iso.display().to_string().as_str());
        press(app, KeyCode::Enter);
        settle(app);
    }

    #[test]
    fn the_optical_screen_hands_an_image_to_the_convert_tab() {
        let dir = tempfile::tempdir().unwrap();
        let iso = hybrid(dir.path());
        let mut app = optical_app();
        settle(&mut app);
        open_image(&mut app, iso.as_path());
        let ctl = &o(&app).ctl;
        assert_eq!(ctl.image(), Some(iso.as_path()));
        assert_eq!(ctl.disc().map(|d| d.format).as_deref(), Some("ISO image"));
        let recent = app.shell.recent(RecentMode::Optical);
        assert_eq!(recent, [iso.display().to_string()]);
        assert_eq!(o(&app).rows(), [ORow::Source, ORow::Image, ORow::Convert]);

        // Start does not convert here: an image's format changes in the Convert tab.
        press(&mut app, KeyCode::Char('s'));
        assert_eq!(o(&app).note.as_deref(), Some(hints::CONVERT_MOVED));
        assert!(app.progress.is_none());
        focus_to(&mut app, ORow::Convert);
        press(&mut app, KeyCode::Enter);
        settle(&mut app);
        assert!(app.current() == TabId::Convert);
        let c = app.convert.as_ref().expect("the Convert screen");
        assert_eq!(c.ctl.source(), Some(iso.as_path()));
    }

    #[cfg(feature = "chd")]
    #[test]
    fn the_optical_screen_shows_chd_info() {
        let dir = tempfile::tempdir().unwrap();
        let iso = hybrid(dir.path());
        let mut app = optical_app();
        open_image(&mut app, iso.as_path());
        // The CHD comes from the controller's own convert, which no screen reaches any more.
        let chd = dir.path().join("out.chd");
        let st = app.optical.as_mut().expect("the Optical screen");
        st.act(OpticalAction::SetOutputFormat(OutputFormat::Chd));
        st.set_output(chd.display().to_string());
        let choice = st.chd_choice().expect("CHD defaults");
        st.act(OpticalAction::Start(choice));
        settle(&mut app);
        assert!(std::fs::metadata(&chd).unwrap().len() > 0);

        app.optical_open_image(chd.clone());
        settle(&mut app);
        let format = o(&app).ctl.disc().map(|d| d.format).unwrap_or_default();
        assert!(format.starts_with("CHD"), "{format}");
        press(&mut app, KeyCode::Char('i'));
        settle(&mut app);
        assert!(o(&app).ctl.chd_info().is_some_and(|t| !t.is_empty()));
        assert_eq!(app.optical_key_context().0, "Optical - window");
        press(&mut app, KeyCode::Esc);
        assert!(o(&app).ctl.chd_info().is_none());
    }

    /// Move the disc browser's cursor to the row named `name`.
    fn cursor_to(app: &mut App, name: &str) {
        press(app, KeyCode::Home);
        for _ in 0..64 {
            let v = app.explorer.as_ref().unwrap();
            let pane = v.ctl.pane(Side::Left);
            let at = pane.cursor_index().map(|i| pane.rows()[i].name.clone());
            if at.as_deref() == Some(name) {
                return;
            }
            press(app, KeyCode::Down);
        }
        panic!("no row {name}");
    }

    fn browser_status(app: &App) -> String {
        app.explorer
            .as_ref()
            .map(|v| v.ctl.status().to_string())
            .unwrap_or_default()
    }

    #[test]
    fn browse_contents_opens_the_disc_and_extracts_lists_and_closes_it() {
        let dir = tempfile::tempdir().unwrap();
        let iso = hybrid(dir.path());
        let mut app = optical_app();
        press(&mut app, KeyCode::Char('b'));
        assert_eq!(
            o(&app).note.as_deref(),
            Some("No disc is detected to browse.")
        );
        open_image(&mut app, iso.as_path());
        press(&mut app, KeyCode::Char('b'));
        settle(&mut app);
        let v = app.explorer.as_ref().expect("the disc browser");
        assert!(v.is_disc() && o(&app).ctl.is_browsing());
        assert!(v.explorer_title().starts_with("Disc Browser  "));
        assert!(v.ctl.pane(Side::Left).profile().read_only);

        // The filesystem picker: the ISO side and the HFS+ side.
        ctrl(&mut app, 'p');
        let labels = app
            .explorer
            .as_ref()
            .unwrap()
            .ctl
            .pane(Side::Left)
            .partition_labels();
        assert_eq!(labels.len(), 2, "{labels:?}");
        let hfs = labels
            .iter()
            .position(|l| l.contains("HFS"))
            .expect("an HFS side");
        for _ in 0..hfs {
            press(&mut app, KeyCode::Down);
        }
        press(&mut app, KeyCode::Enter);
        settle(&mut app);
        let pane = app.explorer.as_ref().unwrap().ctl.pane(Side::Left);
        assert_eq!(pane.volume().fs_type, "HFS+");

        cursor_to(&mut app, "Apps");
        press(&mut app, KeyCode::Right);
        settle(&mut app);
        cursor_to(&mut app, "GameApp");

        // Extract asks for the fork mode (AppleDouble first), then the folder.
        ctrl(&mut app, 'x');
        press(&mut app, KeyCode::Enter);
        let out = dir.path().join("out");
        std::fs::create_dir(&out).unwrap();
        typing(&mut app, out.display().to_string().as_str());
        press(&mut app, KeyCode::Enter);
        settle(&mut app);
        assert!(
            browser_status(&app).starts_with("Copied 1 file(s)"),
            "{}",
            browser_status(&app)
        );
        assert_eq!(
            std::fs::read(out.join("GameApp")).unwrap(),
            b"data fork bytes"
        );
        assert!(out.join("._GameApp").exists());

        // MacBinary instead: the window remembers nothing, Down picks the next mode.
        ctrl(&mut app, 'x');
        let rows = app.explorer.as_ref().unwrap().extract_rows();
        let mb = rows.iter().position(|r| r.contains("MacBinary")).unwrap();
        let ad = rows.iter().position(|r| r.contains("AppleDouble")).unwrap();
        for _ in ad..mb {
            press(&mut app, KeyCode::Down);
        }
        press(&mut app, KeyCode::Enter);
        let out_mb = dir.path().join("out_mb");
        std::fs::create_dir(&out_mb).unwrap();
        typing(&mut app, out_mb.display().to_string().as_str());
        press(&mut app, KeyCode::Enter);
        settle(&mut app);
        assert!(
            out_mb.join("GameApp.bin").exists(),
            "{}",
            browser_status(&app)
        );

        // A failed entry asks Skip / Continue with all errors / Stop (slice 21).
        let blocked = dir.path().join("blocked");
        std::fs::create_dir_all(blocked.join("Apps/GameApp")).unwrap();
        cursor_to(&mut app, "Apps");
        ctrl(&mut app, 'x');
        press(&mut app, KeyCode::Enter);
        typing(&mut app, blocked.display().to_string().as_str());
        press(&mut app, KeyCode::Enter);
        settle(&mut app);
        let start = Instant::now();
        while !app.explorer.as_ref().unwrap().choosing() {
            assert!(
                start.elapsed() < Duration::from_secs(60),
                "{}",
                browser_status(&app)
            );
            if app.explorer.as_ref().unwrap().ctl.host_conflict().is_some() {
                press(&mut app, KeyCode::Char('o'));
            }
            std::thread::sleep(Duration::from_millis(5));
            app.tick();
        }
        press(&mut app, KeyCode::Char('c'));
        settle(&mut app);
        let report = app.explorer.as_ref().unwrap().ctl.skipped_report().cloned();
        assert_eq!(report.map(|r| r.skipped.len()), Some(1));
        press(&mut app, KeyCode::Enter);

        // The tree, saved to a file.
        ctrl(&mut app, 'd');
        settle(&mut app);
        let tree = app
            .explorer
            .as_ref()
            .unwrap()
            .tree_text()
            .expect("the tree window");
        assert!(tree.contains("GameApp"), "{tree}");
        press(&mut app, KeyCode::Char('s'));
        // The save picker starts on a suggested name; clear it first.
        for _ in 0..512 {
            press(&mut app, KeyCode::Backspace);
        }
        let saved = dir.path().join("tree.txt");
        typing(&mut app, saved.display().to_string().as_str());
        press(&mut app, KeyCode::Enter);
        settle(&mut app);
        assert_eq!(std::fs::read_to_string(&saved).unwrap(), tree);
        press(&mut app, KeyCode::Esc);
        assert!(app.explorer.as_ref().unwrap().tree_text().is_none());

        // Closing the browser (Esc asks, as the Explorer does) tells the controller.
        press(&mut app, KeyCode::Esc);
        press(&mut app, KeyCode::Char('y'));
        app.tick();
        assert!(app.explorer.is_none());
        assert!(!o(&app).ctl.is_browsing());
        press(&mut app, KeyCode::Char('b'));
        assert!(app.explorer.is_some());
        settle(&mut app);
        press(&mut app, KeyCode::Esc);
        press(&mut app, KeyCode::Char('y'));
        press(&mut app, KeyCode::Char('c'));
        app.tick();
        assert!(o(&app).ctl.disc().is_none() && o(&app).ctl.image().is_none());
    }

    #[test]
    fn closing_the_disc_from_the_screen_closes_its_browser() {
        let dir = tempfile::tempdir().unwrap();
        let iso = hybrid(dir.path());
        let mut app = optical_app();
        open_image(&mut app, iso.as_path());
        press(&mut app, KeyCode::Char('b'));
        settle(&mut app);
        assert!(app.explorer.as_ref().is_some_and(|e| e.is_disc()));
        app.optical.as_mut().unwrap().act(OpticalAction::CloseDisc);
        app.tick();
        assert!(
            app.explorer.is_none(),
            "CloseBrowser closes the disc browser"
        );
    }

    #[test]
    fn an_optical_disc_opened_on_inspect_moves_to_the_optical_tab() {
        let dir = tempfile::tempdir().unwrap();
        // A plain ISO 9660 disc: no partition table, so Inspect hands it over.
        let iso = fixture(dir.path(), "rock_ridge");
        let mut app = App::headless_on(tab(TabId::Inspect));
        app.inspect_open_path(iso.clone());
        let start = Instant::now();
        while app.current() != TabId::Optical {
            assert!(
                start.elapsed() < Duration::from_secs(60),
                "never handed over"
            );
            std::thread::sleep(Duration::from_millis(5));
            app.tick();
        }
        settle(&mut app);
        let ctl = &o(&app).ctl;
        assert_eq!(ctl.image(), Some(iso.as_path()));
        assert_eq!(ctl.source_mode(), SourceMode::ImageFile);
        assert_eq!(ctl.disc().map(|d| d.format).as_deref(), Some("ISO image"));
    }

    #[test]
    fn the_drive_rows_remote_window_and_refusals_follow_the_controller() {
        let mut app = optical_app();
        settle(&mut app);
        press(&mut app, KeyCode::Right);
        assert_eq!(o(&app).ctl.source_mode(), SourceMode::PhysicalDrive);
        assert_eq!(o(&app).rows()[1], ORow::Drive);
        assert!(o(&app).rows().contains(&ORow::EjectAfter));
        assert_eq!(o(&app).ctl.start_label(), "Start Rip");
        press(&mut app, KeyCode::Char('e'));
        let eject = "Select a local drive with a disc in it to eject.";
        assert_eq!(o(&app).note.as_deref(), Some(eject));
        focus_to(&mut app, ORow::EjectAfter);
        press(&mut app, KeyCode::Char(' '));
        assert!(o(&app).ctl.eject_after());
        press(&mut app, KeyCode::Char('s'));
        let note = o(&app).note.clone().unwrap_or_default();
        assert!(note.starts_with("Cannot start: "), "{note}");
        press(&mut app, KeyCode::Char('R'));
        if OpticalController::remote_available() {
            assert!(o(&app).ctl.add_remote_open());
            typing(&mut app, "x?");
            assert_eq!(o(&app).remote_addr, "x?", "`?` is typed, not the key list");
            press(&mut app, KeyCode::Esc);
            assert!(!o(&app).ctl.add_remote_open());
        } else {
            let not_built = crate::controller::backup::REMOTE_NOT_BUILT;
            assert!(o(&app).log.iter().any(|l| l == not_built));
        }
        press(&mut app, KeyCode::Char('i'));
        assert_eq!(
            o(&app).note.as_deref(),
            Some("CHD Info reads a .chd image.")
        );
    }

    fn screen(app: &App) -> String {
        let backend = ratatui::backend::TestBackend::new(80, 24);
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

    #[test]
    fn the_optical_screen_and_its_windows_draw_in_a_small_terminal() {
        let dir = tempfile::tempdir().unwrap();
        let iso = hybrid(dir.path());
        let mut app = optical_app();
        let text = screen(&app);
        assert!(
            text.contains("Optical Disc") && text.contains("Source:"),
            "{text}"
        );
        open_image(&mut app, iso.as_path());
        let text = screen(&app);
        assert!(text.contains("Format: ISO image"), "{text}");
        assert!(text.contains(labels::CONVERT), "{text}");
        press(&mut app, KeyCode::Char('?'));
        assert!(screen(&app).contains("Browse Contents"));
        press(&mut app, KeyCode::Esc);
        press(&mut app, KeyCode::Char('R'));
        if OpticalController::remote_available() {
            assert!(screen(&app).contains("Add remote daemon"));
            press(&mut app, KeyCode::Esc);
        }
        press(&mut app, KeyCode::Char('b'));
        settle(&mut app);
        let text = screen(&app);
        assert!(text.contains("Disc Browser"), "{text}");
        ctrl(&mut app, 'd');
        settle(&mut app);
        assert!(screen(&app).contains("Tree"));
    }
}
