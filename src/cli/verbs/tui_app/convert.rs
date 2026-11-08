//! The Convert screen over `controller::convert`, the GUI's Convert tab (docs/convert_tab.md).
//!
//! One form: the source (an image or a folder), subfolders, the output, each kind's format and
//! extension, the CHD options for each CHD format, hard disk sizing, the floppy size, verify,
//! the file list and Convert. The row under the cursor shows its help. The controller scans,
//! asks Skip / Overwrite / Back when outputs exist, and converts in a job.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crossterm::event::KeyCode;
use ratatui::layout::Rect;
use ratatui::style::Modifier;
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Paragraph, Wrap};
use ratatui::Frame;

use super::{hint, App, FilePicker, KeyHint, PickKind, PickResult, Progress, ProgressShared};
#[cfg(feature = "chd")]
use crate::controller::app::chd::{
    codec_choices, codec_label, hunk_presets, modes_for, unit_size_for, ChdOptionsControl,
};
use crate::controller::app::RateTracker;
use crate::controller::convert::{
    floppy_size_choices, formats_for, hints, purpose, ChdChoices, ChdProfile, ConvertAction,
    ConvertController, ExtraUnit, FloppySize, ImageKind, NoNotify, SizeChoice, ONLY_FILES,
};
use crate::controller::{ChoiceSpec, Controller as _, Effect};

pub(super) const HINTS_CONVERT: &[KeyHint] = &[
    hint("Up/Down", "Move between rows"),
    hint(
        "Enter",
        "Choose the image or folder; Convert on the Convert row",
    ),
    hint(
        "Left/Right",
        "Change a format, size or CHD option; flip a switch",
    ),
    hint("Space", "Flip a switch; tick or untick a file"),
    hint("a / n", "Tick all / no files (on the file rows)"),
    hint(
        "Type",
        "Edit the output path, an extension or the extra amount",
    ),
    hint("Tab", "Browse for the output folder; reset an extension"),
    hint(
        "s / o / b",
        "Skip / overwrite / back when outputs already exist",
    ),
    hint("Esc", "Cancel a running conversion"),
];

/// One row of the form.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum CRow {
    SourceFile,
    SourceFolder,
    Recursive,
    Output,
    Format(ImageKind),
    Extension(ImageKind),
    ChdMode(ChdProfile),
    ChdHunk(ChdProfile),
    ChdCodec(ChdProfile, usize),
    MultiBin,
    Sizing,
    Extra,
    ExtraUnit,
    MeasureSlow,
    FloppySize,
    Verify,
    File(usize),
    Start,
}

/// The screen's widget state around a headless `ConvertController`.
pub(super) struct ConvertState {
    pub(super) ctl: ConvertController,
    pub(super) field: usize,
    picker: Option<(FilePicker, &'static str)>,
    /// The Skip / Overwrite / Back question, while it is up.
    question: Option<ChoiceSpec>,
    /// The CHD options per profile: Hd, Cd, Dvd.
    #[cfg(feature = "chd")]
    chd: [ChdOptionsControl; 3],
    pub(super) result: Option<String>,
    is_error: bool,
    awaiting: bool,
}

impl Default for ConvertState {
    fn default() -> Self {
        ConvertState {
            ctl: ConvertController::new(Arc::new(NoNotify)),
            field: 0,
            picker: None,
            question: None,
            #[cfg(feature = "chd")]
            chd: [
                ChdOptionsControl::new(ChdProfile::Hd),
                ChdOptionsControl::new(ChdProfile::Cd),
                ChdOptionsControl::new(ChdProfile::Dvd),
            ],
            result: None,
            is_error: false,
            awaiting: false,
        }
    }
}

fn step<T: Copy + PartialEq>(list: &[T], cur: T, forward: bool) -> T {
    let n = list.len();
    match list.iter().position(|x| *x == cur) {
        Some(i) if forward => list[(i + 1) % n],
        Some(i) => list[(i + n - 1) % n],
        None => list[0],
    }
}

impl ConvertState {
    pub(super) fn act(&mut self, action: ConvertAction) {
        let fx = self.ctl.apply(action);
        self.absorb(fx);
    }

    fn absorb(&mut self, fx: Vec<Effect>) {
        for e in fx {
            match e {
                Effect::Choose(spec) => self.question = Some(spec),
                Effect::PickFile(spec) | Effect::PickFolder(spec) => {
                    let kind = if spec.purpose == purpose::SOURCE_FILE {
                        PickKind::File
                    } else {
                        PickKind::Dir
                    };
                    self.picker = Some((FilePicker::new(kind, spec.title), spec.purpose));
                }
                Effect::Message(m) => self.result = Some(m),
                Effect::Confirm(_)
                | Effect::AskPassword(_)
                | Effect::AskText(_)
                | Effect::OpenUrl(_)
                | Effect::CopyToClipboard(_)
                | Effect::CloseScreen
                | Effect::Quit => {}
            }
        }
    }

    #[cfg(feature = "chd")]
    fn chd_index(profile: ChdProfile) -> usize {
        match profile {
            ChdProfile::Hd => 0,
            ChdProfile::Cd => 1,
            ChdProfile::Dvd => 2,
        }
    }

    /// The form's rows in drawing order.
    pub(super) fn rows(&self) -> Vec<CRow> {
        let mut r = vec![CRow::SourceFile, CRow::SourceFolder];
        if self.ctl.source_is_folder() {
            r.push(CRow::Recursive);
        }
        if self.ctl.source().is_some() {
            r.push(CRow::Output);
        }
        let kinds = self.ctl.shown_kinds();
        for &k in &kinds {
            r.push(CRow::Format(k));
            r.push(CRow::Extension(k));
        }
        #[cfg(feature = "chd")]
        for (_, profile) in self.ctl.chd_profiles() {
            r.push(CRow::ChdMode(profile));
            let c = &self.chd[Self::chd_index(profile)];
            if c.mode == crate::controller::app::chd::ChdOptionsMode::Custom {
                r.push(CRow::ChdHunk(profile));
                r.extend((0..4).map(|n| CRow::ChdCodec(profile, n)));
            }
        }
        if self.ctl.multi_bin_shown() {
            r.push(CRow::MultiBin);
        }
        if kinds.contains(&ImageKind::HardDisk) {
            r.push(CRow::Sizing);
            if self.ctl.size_choice() == SizeChoice::Extra {
                r.push(CRow::Extra);
                r.push(CRow::ExtraUnit);
            }
            r.push(CRow::MeasureSlow);
        }
        if kinds.contains(&ImageKind::Floppy) {
            r.push(CRow::FloppySize);
        }
        if !kinds.is_empty() {
            r.push(CRow::Verify);
        }
        r.extend((0..self.ctl.items().len()).map(CRow::File));
        r.push(CRow::Start);
        r
    }

    pub(super) fn row(&self) -> CRow {
        let rows = self.rows();
        rows[self.field.min(rows.len() - 1)]
    }

    fn clamp(&mut self) {
        self.field = self.field.min(self.rows().len() - 1);
    }

    /// The help shown under the selected row.
    fn row_help(&self, row: CRow) -> Option<String> {
        Some(match row {
            CRow::SourceFile => "Convert one disk image.".to_string(),
            CRow::SourceFolder => "Convert every disk image in a folder.".to_string(),
            CRow::Recursive => hints::RECURSIVE.to_string(),
            CRow::Output => self.ctl.output_hint().to_string(),
            CRow::Format(k) => self.ctl.format(k).hint().to_string(),
            CRow::Extension(_) => hints::EXTENSION.to_string(),
            CRow::MultiBin => hints::MULTI_BIN.to_string(),
            CRow::Sizing => hints::SIZING.to_string(),
            CRow::Extra | CRow::ExtraUnit => hints::EXTRA.to_string(),
            CRow::MeasureSlow => hints::measure_slow(),
            CRow::FloppySize if self.ctl.floppy_size() != FloppySize::Same => {
                format!("{} {ONLY_FILES}", hints::FLOPPY_SIZE)
            }
            CRow::FloppySize => hints::FLOPPY_SIZE.to_string(),
            CRow::Verify => hints::VERIFY.to_string(),
            CRow::File(i) => {
                let row = self.ctl.rows().into_iter().nth(i)?;
                row.note.unwrap_or(row.outcome)
            }
            CRow::ChdMode(_) | CRow::ChdHunk(_) | CRow::ChdCodec(..) | CRow::Start => return None,
        })
    }

    fn output_purpose(&self) -> &'static str {
        if self.ctl.source_is_folder() {
            purpose::OUTPUT_FOLDER
        } else {
            purpose::OUTPUT_FILE
        }
    }

    fn set_output_text(&mut self, text: String) {
        let purpose = self.output_purpose();
        self.act(ConvertAction::Picked {
            purpose,
            path: Some(PathBuf::from(text)),
        });
    }

    fn flip(&mut self, row: CRow) {
        let a = match row {
            CRow::Recursive => ConvertAction::SetRecursive(!self.ctl.recursive()),
            CRow::MultiBin => ConvertAction::SetMultiBin(!self.ctl.multi_bin()),
            CRow::MeasureSlow => ConvertAction::SetMeasureSlow(!self.ctl.measure_slow()),
            CRow::Verify => ConvertAction::SetVerify(!self.ctl.verify()),
            CRow::File(i) => {
                let on = self.ctl.items().get(i).is_some_and(|f| f.selected);
                ConvertAction::SetSelected(i, !on)
            }
            _ => return,
        };
        self.act(a);
    }

    /// Left / Right on a row; `false` when the row has nothing to change.
    fn change(&mut self, row: CRow, forward: bool) -> bool {
        match row {
            CRow::Format(k) => {
                let usable: Vec<_> = formats_for(k)
                    .iter()
                    .copied()
                    .filter(|f| f.unavailable().is_none())
                    .collect();
                let next = step(usable.as_slice(), self.ctl.format(k), forward);
                self.act(ConvertAction::SetFormat(k, next));
            }
            CRow::Sizing => {
                let next = step(&SizeChoice::ALL, self.ctl.size_choice(), forward);
                self.act(ConvertAction::SetSizing(next));
            }
            CRow::ExtraUnit => {
                let next = step(&ExtraUnit::ALL, self.ctl.extra_unit(), forward);
                self.act(ConvertAction::SetExtraUnit(next));
            }
            CRow::FloppySize => {
                let all = floppy_size_choices();
                let next = step(all.as_slice(), self.ctl.floppy_size(), forward);
                self.act(ConvertAction::SetFloppySize(next));
            }
            CRow::Recursive | CRow::MultiBin | CRow::MeasureSlow | CRow::Verify => self.flip(row),
            CRow::ChdMode(_) | CRow::ChdHunk(_) | CRow::ChdCodec(..) => {
                self.change_chd(row, forward)
            }
            _ => return false,
        }
        self.clamp();
        true
    }

    #[cfg(feature = "chd")]
    fn change_chd(&mut self, row: CRow, forward: bool) {
        let profile = match row {
            CRow::ChdMode(p) | CRow::ChdHunk(p) | CRow::ChdCodec(p, _) => p,
            _ => return,
        };
        let c = &mut self.chd[Self::chd_index(profile)];
        match row {
            CRow::ChdMode(_) => c.mode = step(modes_for(profile).as_slice(), c.mode, forward),
            CRow::ChdHunk(_) => {
                c.custom.hunk_size = step(hunk_presets(profile), c.custom.hunk_size, forward)
            }
            CRow::ChdCodec(_, slot) => {
                let choices = codec_choices(profile);
                c.custom.codecs[slot] = step(choices.as_slice(), c.custom.codecs[slot], forward);
            }
            _ => {}
        }
    }

    #[cfg(not(feature = "chd"))]
    fn change_chd(&mut self, _row: CRow, _forward: bool) {}

    /// The CHD options Start carries, or why one of them cannot be used.
    #[cfg(feature = "chd")]
    fn chd_choices(&self) -> Result<ChdChoices, String> {
        let mut out = ChdChoices::default();
        for (_, profile) in self.ctl.chd_profiles() {
            let c = &self.chd[Self::chd_index(profile)];
            c.validate(profile)
                .map_err(|e| format!("CHD options: {e}."))?;
            let opts = Some(c.effective(profile));
            match profile {
                ChdProfile::Hd => out.hd = opts,
                ChdProfile::Cd => out.cd = opts,
                ChdProfile::Dvd => out.dvd = opts,
            }
        }
        Ok(out)
    }

    #[cfg(not(feature = "chd"))]
    fn chd_choices(&self) -> Result<ChdChoices, String> {
        Ok(ChdChoices::default())
    }

    fn type_char(&mut self, c: char) -> bool {
        match self.row() {
            CRow::Output => {
                let mut t = ConvertController::path_text(self.ctl.output());
                if self.ctl.output().is_none() {
                    t.clear();
                }
                t.push(c);
                self.set_output_text(t);
            }
            CRow::Extension(k) => {
                let mut ext = self.ctl.extension(k).to_string();
                ext.push(c);
                self.act(ConvertAction::SetExtension(k, ext));
            }
            CRow::Extra if c.is_ascii_digit() => {
                let text = format!("{}{c}", self.ctl.extra_amount());
                let n = text.trim_start_matches('0').parse().unwrap_or(0);
                self.act(ConvertAction::SetExtraAmount(n));
            }
            CRow::File(_) | CRow::Start if c == 'a' => self.act(ConvertAction::SelectAll),
            CRow::File(_) | CRow::Start if c == 'n' => self.act(ConvertAction::SelectNone),
            _ => return false,
        }
        true
    }

    fn backspace(&mut self) {
        match self.row() {
            CRow::Output => {
                if let Some(out) = self.ctl.output() {
                    let mut t = out.display().to_string();
                    t.pop();
                    self.set_output_text(t);
                }
            }
            CRow::Extension(k) => {
                let mut ext = self.ctl.extension(k).to_string();
                ext.pop();
                self.act(ConvertAction::SetExtension(k, ext));
            }
            CRow::Extra => self.act(ConvertAction::SetExtraAmount(self.ctl.extra_amount() / 10)),
            _ => {}
        }
    }
}

impl App {
    fn convert_state(&mut self) -> &mut ConvertState {
        if self.convert.is_none() {
            self.convert = Some(ConvertState::default());
        }
        self.convert.as_mut().unwrap()
    }

    /// Load a file or folder handed over by Inspect or Optical.
    pub(super) fn convert_open(&mut self, path: PathBuf) {
        let c = self.convert_state();
        c.result = None;
        c.field = 0;
        c.act(ConvertAction::SetSource(path));
    }

    pub(super) fn convert_key_context(&self) -> (&'static str, &'static [KeyHint]) {
        ("Convert", HINTS_CONVERT)
    }

    pub(super) fn convert_footer_keys(&self) -> Vec<(&'static str, &'static str)> {
        let running = self.convert.as_ref().is_some_and(|c| c.ctl.is_running());
        if running {
            return vec![("Esc", "Cancel")];
        }
        vec![
            ("Up/Dn", "Move"),
            ("<-/->", "Change"),
            ("Space", "Toggle"),
            ("Enter", "Choose / Convert"),
            ("?", "Keys"),
        ]
    }

    /// Drive the controller and mirror the running file into the bar.
    pub(super) fn convert_tick(&mut self) {
        let Some(c) = self.convert.as_mut() else {
            return;
        };
        let fx = c.ctl.tick();
        c.absorb(fx);
        let _ = c.ctl.take_log();
        c.clamp();
        if let (Some((done, total)), Some(prog)) = (c.ctl.run_bytes(), self.progress.as_ref()) {
            if let Ok(mut sh) = prog.shared.lock() {
                sh.current = done;
                sh.total = total;
            }
        }
        if c.awaiting && !c.ctl.is_running() {
            let done = match c.ctl.last_result() {
                Some(Ok(r)) => Some((
                    format!(
                        "Converted {}, failed {}, skipped {}.",
                        r.succeeded, r.failed, r.skipped
                    ),
                    r.failed > 0,
                )),
                Some(Err(e)) => Some((format!("Convert stopped: {e}"), true)),
                None => None,
            };
            if let Some((text, err)) = done {
                c.awaiting = false;
                c.result = Some(text);
                c.is_error = err;
                if let Some(prog) = self.progress.as_ref() {
                    if let Ok(mut sh) = prog.shared.lock() {
                        sh.done = true;
                    }
                }
            }
        }
    }

    /// Convert screen keys. Returns `true` when consumed.
    pub(super) fn handle_convert_key(&mut self, code: KeyCode) -> bool {
        let c = self.convert_state();
        if let Some((picker, purpose)) = c.picker.as_mut() {
            let purpose = *purpose;
            match picker.handle_key(code) {
                Some(PickResult::Cancel) => c.picker = None,
                Some(PickResult::Confirm(path)) => {
                    c.picker = None;
                    // A folder chosen for a one-file output keeps the suggested name.
                    let path = if purpose == purpose::OUTPUT_FILE && path.is_dir() {
                        let name = c
                            .ctl
                            .output()
                            .and_then(|p| p.file_name())
                            .map(|n| n.to_os_string())
                            .unwrap_or_else(|| "converted.img".into());
                        path.join(name)
                    } else {
                        path
                    };
                    c.act(ConvertAction::Picked {
                        purpose,
                        path: Some(path),
                    });
                    c.field = 0;
                }
                None => {}
            }
            return true;
        }
        if c.question.is_some() {
            let index = match code {
                KeyCode::Char('s') => 0,
                KeyCode::Char('o') => 1,
                KeyCode::Char('b') | KeyCode::Esc => 2,
                _ => return true,
            };
            c.question = None;
            c.act(ConvertAction::Chose {
                purpose: purpose::CONFLICTS,
                index,
            });
            self.after_convert_start();
            return true;
        }
        if c.ctl.is_running() {
            if code == KeyCode::Esc {
                c.act(ConvertAction::Cancel);
                c.result = Some("Stopping after the current file...".to_string());
                c.is_error = true;
                return true;
            }
            return false;
        }
        let row = c.row();
        let n = c.rows().len();
        match code {
            KeyCode::Down => c.field = (c.field + 1).min(n - 1),
            KeyCode::Up => c.field = c.field.saturating_sub(1),
            KeyCode::Enter => match row {
                CRow::SourceFile => c.act(ConvertAction::PickSourceFile),
                CRow::SourceFolder => c.act(ConvertAction::PickSourceFolder),
                CRow::Start => self.start_convert(),
                _ => c.field = (c.field + 1).min(n - 1),
            },
            KeyCode::Left | KeyCode::Right => return c.change(row, code == KeyCode::Right),
            KeyCode::Char(' ') => c.flip(row),
            KeyCode::Tab => match row {
                CRow::Output => {
                    let purpose = c.output_purpose();
                    c.picker = Some((
                        FilePicker::new(PickKind::Dir, "Choose the output folder"),
                        purpose,
                    ));
                }
                CRow::Extension(k) => c.act(ConvertAction::ResetExtension(k)),
                _ => return false,
            },
            KeyCode::Backspace => c.backspace(),
            KeyCode::Char(ch) if !ch.is_control() => return c.type_char(ch),
            _ => return false,
        }
        true
    }

    /// Start converting the ticked files through the controller.
    pub(super) fn start_convert(&mut self) {
        let c = self.convert_state();
        let chd = match c.chd_choices() {
            Ok(chd) => chd,
            Err(e) => {
                c.result = Some(e);
                c.is_error = true;
                return;
            }
        };
        if let Some(problem) = c.ctl.start_problem() {
            c.result = Some(problem);
            c.is_error = true;
            return;
        }
        c.act(ConvertAction::Start(chd));
        self.after_convert_start();
    }

    fn after_convert_start(&mut self) {
        let Some(c) = self.convert.as_mut() else {
            return;
        };
        if !c.ctl.is_running() {
            return;
        }
        c.result = None;
        c.is_error = false;
        c.awaiting = true;
        self.progress = Some(Progress {
            shared: Arc::new(Mutex::new(ProgressShared::default())),
            tracker: RateTracker::default(),
            label: "Converting".to_string(),
        });
    }

    #[cfg(feature = "chd")]
    fn convert_chd_row(c: &ConvertState, row: CRow) -> (String, String) {
        let profile = match row {
            CRow::ChdMode(p) | CRow::ChdHunk(p) | CRow::ChdCodec(p, _) => p,
            _ => return (String::new(), String::new()),
        };
        let o = &c.chd[ConvertState::chd_index(profile)];
        match row {
            CRow::ChdMode(_) => (
                format!("{profile:?} CHD:"),
                format!("< {} >", o.mode.label()),
            ),
            CRow::ChdHunk(_) => {
                let h = o.custom.hunk_size;
                let unit = unit_size_for(profile);
                let warn = if h % unit == 0 {
                    String::new()
                } else {
                    format!("  (must be a multiple of {unit})")
                };
                ("  Hunk:".to_string(), format!("< {h} bytes >{warn}"))
            }
            CRow::ChdCodec(_, slot) => (
                format!("  Codec {}:", slot + 1),
                format!("< {} >", codec_label(o.custom.codecs[slot])),
            ),
            _ => (String::new(), String::new()),
        }
    }

    #[cfg(not(feature = "chd"))]
    fn convert_chd_row(_c: &ConvertState, _row: CRow) -> (String, String) {
        (String::new(), String::new())
    }

    pub(super) fn draw_convert(&self, frame: &mut Frame, area: Rect, c: &ConvertState) {
        let mut lines: Vec<Line<'static>> = vec![
            Line::styled(hints::INTRO, self.palette.dim()),
            Line::raw(""),
        ];
        let cur = c.row();
        let style = |row: CRow| {
            if row == cur {
                self.palette
                    .accent()
                    .add_modifier(Modifier::REVERSED | Modifier::BOLD)
            } else {
                self.palette.accent()
            }
        };
        let marker = |row: CRow| if row == cur { "> " } else { "  " };
        let tick = |on: bool| if on { "[x]" } else { "[ ]" };
        let source = ConvertController::path_text(c.ctl.source());
        let rows = c.ctl.rows();
        let avail = (area.height as usize).saturating_sub(24).max(3);
        let at = match cur {
            CRow::File(i) => i,
            _ => 0,
        };
        let first_file = at.saturating_sub(avail.saturating_sub(1));
        for row in c.rows() {
            let (label, value): (String, String) = match row {
                CRow::SourceFile => (
                    "Image:".to_string(),
                    if c.ctl.source_is_folder() || c.ctl.source().is_none() {
                        "[Enter to choose]".to_string()
                    } else {
                        source.clone()
                    },
                ),
                CRow::SourceFolder => (
                    "Folder:".to_string(),
                    if c.ctl.source_is_folder() {
                        source.clone()
                    } else {
                        "[Enter to choose]".to_string()
                    },
                ),
                CRow::Recursive => (
                    "Subfolders:".to_string(),
                    format!("{} Include subfolders", tick(c.ctl.recursive())),
                ),
                CRow::Output => (
                    format!("{}:", c.ctl.output_label()),
                    ConvertController::path_text(c.ctl.output()),
                ),
                CRow::Format(k) => {
                    let f = c.ctl.format(k);
                    (format!("{}:", k.plural()), format!("< {} >", f.label()))
                }
                CRow::Extension(k) => (
                    "  Extension:".to_string(),
                    format!(".{}", c.ctl.extension(k)),
                ),
                CRow::ChdMode(_) | CRow::ChdHunk(_) | CRow::ChdCodec(..) => {
                    Self::convert_chd_row(c, row)
                }
                CRow::MultiBin => (
                    "BIN/CUE:".to_string(),
                    format!("{} One .bin per track", tick(c.ctl.multi_bin())),
                ),
                CRow::Sizing => (
                    "Hard disk size:".to_string(),
                    format!("< {} >", c.ctl.size_choice().label()),
                ),
                CRow::Extra => ("  Extra:".to_string(), c.ctl.extra_amount().to_string()),
                CRow::ExtraUnit => (
                    "  Unit:".to_string(),
                    format!("< {} >", c.ctl.extra_unit().label()),
                ),
                CRow::MeasureSlow => (
                    "Slow filesystems:".to_string(),
                    format!(
                        "{} Measure slow filesystems too{}",
                        tick(c.ctl.measure_slow()),
                        if c.ctl.measure_slow_enabled() {
                            ""
                        } else {
                            "  (only when shrinking)"
                        }
                    ),
                ),
                CRow::FloppySize => (
                    "Floppy size:".to_string(),
                    format!("< {} >", c.ctl.floppy_size().label()),
                ),
                CRow::Verify => (
                    "Verify:".to_string(),
                    format!("{} Verify after converting", tick(c.ctl.verify())),
                ),
                CRow::File(i) => {
                    if i == 0 {
                        lines.push(Line::raw(""));
                        lines.push(Line::styled(c.ctl.summary(), self.palette.accent()));
                    }
                    if i < first_file || i >= first_file + avail {
                        continue;
                    }
                    let r = &rows[i];
                    let st = if row == cur {
                        style(row)
                    } else if r.skipped || !r.selected {
                        self.palette.dim()
                    } else {
                        self.palette.accent()
                    };
                    lines.push(Line::styled(
                        format!(
                            "{}{} {:<32} {:<9} {}",
                            marker(row),
                            tick(r.selected),
                            r.name,
                            r.kind,
                            r.outcome
                        ),
                        st,
                    ));
                    continue;
                }
                CRow::Start => {
                    lines.push(Line::raw(""));
                    lines.push(Line::styled(
                        format!("{}[ {} ]", marker(row), c.ctl.start_label()),
                        style(row),
                    ));
                    continue;
                }
            };
            lines.push(Line::from(vec![
                Span::styled(format!("{}{label:<18}", marker(row)), style(row)),
                Span::raw(value),
            ]));
            if row == cur {
                if let Some(help) = c.row_help(row) {
                    lines.push(Line::styled(format!("    {help}"), self.palette.dim()));
                }
            }
        }
        if let Some(snap) = c.ctl.scan_snapshot() {
            lines.push(Line::styled(
                format!("  {} {}", snap.label, snap.phase),
                self.palette.dim(),
            ));
        }
        if let Some(e) = c.ctl.scan_error() {
            lines.push(Line::styled(format!("  {e}"), self.palette.warn()));
        }
        if let Some(q) = &c.question {
            lines.push(Line::raw(""));
            for l in q.message.lines() {
                lines.push(Line::styled(format!("  {l}"), self.palette.warn()));
            }
            lines.push(Line::styled(
                "  s Skip   o Overwrite   b Back",
                self.palette.warn(),
            ));
        }
        if let Some(p) = c.ctl.progress() {
            lines.push(Line::styled(
                format!(
                    "  Converting [{}/{}] {}  (Esc stops)",
                    p.index, p.total, p.file
                ),
                self.palette.accent(),
            ));
        }
        if let Some(res) = &c.result {
            let st = if c.is_error {
                self.palette.warn()
            } else {
                self.palette.accent()
            };
            lines.push(Line::styled(format!("  {res}"), st));
        }
        frame.render_widget(
            Paragraph::new(Text::from(lines))
                .block(self.pane_block("Convert", true))
                .wrap(Wrap { trim: false }),
            area,
        );
        if let Some((picker, _)) = &c.picker {
            picker.draw(frame, area, self.palette, self.border);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::{TabId, TABS};
    use super::*;

    fn on_convert() -> App {
        let at = TABS.iter().position(|t| t.0 == TabId::Convert).unwrap();
        App::headless_on(at)
    }

    fn settle(app: &mut App) {
        app.convert.as_mut().unwrap().ctl.wait_idle();
        app.tick();
    }

    fn focus(app: &mut App, row: CRow) {
        for _ in 0..80 {
            app.handle_convert_key(KeyCode::Up);
        }
        for _ in 0..80 {
            if app.convert.as_ref().unwrap().row() == row {
                return;
            }
            app.handle_convert_key(KeyCode::Down);
        }
        panic!("no row {row:?}");
    }

    #[test]
    fn the_convert_screen_refits_a_floppy_with_verify() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("disk.img");
        crate::controller::test_fixtures::dos_floppy(&src);
        let mut app = on_convert();
        app.convert_open(src);
        settle(&mut app);
        focus(&mut app, CRow::Format(ImageKind::Floppy));
        while app.convert.as_ref().unwrap().ctl.format(ImageKind::Floppy)
            != crate::controller::convert::Format::Dc42
        {
            app.handle_convert_key(KeyCode::Right);
        }
        focus(&mut app, CRow::FloppySize);
        while app.convert.as_ref().unwrap().ctl.floppy_size() != FloppySize::Refit(737_280) {
            app.handle_convert_key(KeyCode::Right);
        }
        focus(&mut app, CRow::Verify);
        app.handle_convert_key(KeyCode::Char(' '));
        focus(&mut app, CRow::Start);
        app.handle_convert_key(KeyCode::Enter);
        settle(&mut app);
        let c = app.convert.as_ref().unwrap();
        assert_eq!(
            c.result.as_deref(),
            Some("Converted 1, failed 0, skipped 0."),
            "{:?}",
            c.ctl.last_result()
        );
        assert!(dir.path().join("disk.dsk").is_file());

        // Again: the output exists, so the screen asks; Back converts nothing.
        app.start_convert();
        assert!(app.convert.as_ref().unwrap().question.is_some());
        app.handle_convert_key(KeyCode::Char('b'));
        assert!(!app.convert.as_ref().unwrap().ctl.is_running());
    }
}
