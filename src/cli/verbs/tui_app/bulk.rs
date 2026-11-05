//! The Bulk Convert screen over `controller::bulk_convert`, the GUI's Bulk Convert dialog.
//!
//! Step 1 picks the source folder; step 2 is one form: every output format the dialog offers
//! (the CD, floppy and CHD ones with their notes), the CHD options for a CHD format (Default /
//! MiSTer / Custom, hunk, four codecs), multi-bin for BIN/CUE, the output folder, the output
//! extension once a scan has listed the files, the ticked files and Start; step 3 shows the run.
//! The controller scans, asks about outputs that already exist and converts in jobs. See
//! docs/phase2_commander_plan.md § "Slice 29".

use std::sync::{Arc, Mutex};

use crossterm::event::KeyCode;
use ratatui::layout::Rect;
use ratatui::style::Modifier;
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Paragraph, Wrap};
use ratatui::Frame;

use super::{
    basename, expand_tilde, hint, App, FilePicker, KeyHint, PickKind, PickResult, Progress,
    ProgressShared,
};
#[cfg(feature = "chd")]
use crate::controller::app::chd::{
    codec_choices, codec_label, hunk_presets, modes_for, unit_size_for, ChdOptionsControl,
};
use crate::controller::app::{format_size, RateTracker};
#[cfg(feature = "chd")]
use crate::controller::bulk_convert::ChdProfile;
use crate::controller::bulk_convert::{
    chd_profile, format_label, format_note, purpose, BulkConvertAction, BulkConvertController,
    BulkPhase, ChdOptions, ConflictChoice, ExportFormat, NoNotify, ScannedFile, FORMATS,
    MULTI_BIN_LABEL, MULTI_BIN_NOTE,
};
use crate::controller::Controller as _;

pub(super) const HINTS_BULK: &[KeyHint] = &[
    hint("Enter/o", "Choose the source folder (step 1)"),
    hint("Up/Down", "Move between rows"),
    hint("Left/Right", "Change the format, a CHD option or multi-bin"),
    hint("Space", "Tick or untick a file; toggle multi-bin"),
    hint("a / n", "Tick all / no files (on the file rows)"),
    hint("Type", "Edit the output folder or the output extension"),
    hint("Tab", "Browse for the output folder; reset the extension"),
    hint("Enter", "Start on the Convert row"),
    hint(
        "s / o / Esc",
        "Skip / overwrite / go back when outputs already exist",
    ),
    hint("Esc", "Back to the source step; while running, cancel"),
];

/// Which step of the screen is up.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum BulkStep {
    Source,
    Config,
    Run,
}

/// One row of the step-2 form.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum BRow {
    Format,
    ChdMode,
    ChdHunk,
    ChdCodec(usize),
    MultiBin,
    Output,
    Extension,
    File(usize),
    Start,
}

/// The screen's widget state around a headless `BulkConvertController`.
pub(super) struct BulkState {
    pub(super) step: BulkStep,
    pub(super) ctl: BulkConvertController,
    pub(super) source: String,
    pub(super) out_dir: String,
    /// The cursor's position in [`BulkState::rows`].
    pub(super) field: usize,
    picker: Option<FilePicker>,
    picker_target: bool,
    /// The CHD options per profile, as the GUI keeps one control each.
    #[cfg(feature = "chd")]
    chd: [ChdOptionsControl; 3],
    op: String,
    pub(super) result: Option<String>,
    is_error: bool,
    /// A conversion was started and its result is not shown yet.
    awaiting: bool,
}

impl Default for BulkState {
    fn default() -> Self {
        let mut ctl = BulkConvertController::new(Arc::new(NoNotify));
        ctl.apply(BulkConvertAction::Open);
        BulkState {
            step: BulkStep::Source,
            ctl,
            source: String::new(),
            out_dir: String::new(),
            field: 0,
            picker: None,
            picker_target: false,
            #[cfg(feature = "chd")]
            chd: [
                ChdOptionsControl::new(ChdProfile::Hd),
                ChdOptionsControl::new(ChdProfile::Dvd),
                ChdOptionsControl::new(ChdProfile::Cd),
            ],
            op: String::new(),
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

impl BulkState {
    pub(super) fn files(&self) -> &[ScannedFile] {
        self.ctl.files()
    }

    pub(super) fn act(&mut self, action: BulkConvertAction) {
        let _ = self.ctl.apply(action);
    }

    #[cfg(feature = "chd")]
    fn chd_for(&self, profile: ChdProfile) -> &ChdOptionsControl {
        match profile {
            ChdProfile::Hd => &self.chd[0],
            ChdProfile::Dvd => &self.chd[1],
            ChdProfile::Cd => &self.chd[2],
        }
    }

    #[cfg(feature = "chd")]
    fn chd_for_mut(&mut self, profile: ChdProfile) -> &mut ChdOptionsControl {
        match profile {
            ChdProfile::Hd => &mut self.chd[0],
            ChdProfile::Dvd => &mut self.chd[1],
            ChdProfile::Cd => &mut self.chd[2],
        }
    }

    /// The CHD rows apply: a CHD format in a build with `chd`.
    fn chd_shown(&self) -> bool {
        cfg!(feature = "chd") && chd_profile(self.ctl.format()).is_some()
    }

    #[cfg(feature = "chd")]
    fn chd_custom(&self) -> bool {
        use crate::controller::app::chd::ChdOptionsMode;
        chd_profile(self.ctl.format())
            .is_some_and(|p| self.chd_for(p).mode == ChdOptionsMode::Custom)
    }

    #[cfg(not(feature = "chd"))]
    fn chd_custom(&self) -> bool {
        false
    }

    /// The form's rows in drawing order.
    pub(super) fn rows(&self) -> Vec<BRow> {
        let mut r = vec![BRow::Format];
        if self.chd_shown() {
            r.push(BRow::ChdMode);
            if self.chd_custom() {
                r.push(BRow::ChdHunk);
                r.extend((0..4).map(BRow::ChdCodec));
            }
        }
        if self.ctl.format() == ExportFormat::BinCue {
            r.push(BRow::MultiBin);
        }
        r.push(BRow::Output);
        if self.ctl.phase() == BulkPhase::Review {
            r.push(BRow::Extension);
        }
        r.extend((0..self.files().len()).map(BRow::File));
        r.push(BRow::Start);
        r
    }

    pub(super) fn row(&self) -> BRow {
        let rows = self.rows();
        rows[self.field.min(rows.len() - 1)]
    }

    fn move_to(&mut self, row: BRow) {
        if let Some(i) = self.rows().iter().position(|r| *r == row) {
            self.field = i;
        }
    }

    fn clamp(&mut self) {
        self.field = self.field.min(self.rows().len() - 1);
    }

    /// Hand the typed output folder to the controller (the source when it is blank, for a scan).
    fn send_output(&mut self, for_scan: bool) {
        let typed = self.out_dir.trim();
        let path = if typed.is_empty() && for_scan {
            expand_tilde(self.source.trim())
        } else {
            expand_tilde(typed)
        };
        self.act(BulkConvertAction::Picked {
            purpose: purpose::OUTPUT,
            path: Some(path),
        });
    }

    /// Read the source folder for the chosen format (the filter depends on it).
    pub(super) fn rescan(&mut self) {
        // In Review a format change already rescans; otherwise ask for the first scan.
        if self.ctl.phase() == BulkPhase::Setup || !self.ctl.is_scanning() {
            self.send_output(true);
            self.act(BulkConvertAction::Scan);
        }
        self.clamp();
    }

    fn change_format(&mut self, forward: bool) {
        let next = step(FORMATS, self.ctl.format(), forward);
        let in_review = self.ctl.phase() == BulkPhase::Review;
        self.act(BulkConvertAction::SetFormat(next));
        if !in_review {
            self.rescan();
        }
        self.move_to(BRow::Format);
    }

    #[cfg(feature = "chd")]
    fn change_chd(&mut self, row: BRow, forward: bool) {
        let Some(profile) = chd_profile(self.ctl.format()) else {
            return;
        };
        let c = self.chd_for_mut(profile);
        match row {
            BRow::ChdMode => c.mode = step(modes_for(profile).as_slice(), c.mode, forward),
            BRow::ChdHunk => {
                c.custom.hunk_size = step(hunk_presets(profile), c.custom.hunk_size, forward)
            }
            BRow::ChdCodec(slot) => {
                let choices = codec_choices(profile);
                c.custom.codecs[slot] = step(choices.as_slice(), c.custom.codecs[slot], forward);
            }
            _ => {}
        }
        self.clamp();
    }

    #[cfg(not(feature = "chd"))]
    fn change_chd(&mut self, _row: BRow, _forward: bool) {}

    /// The CHD options Start carries, or why they cannot be used.
    #[cfg(feature = "chd")]
    fn chd_options(&self) -> Result<Option<ChdOptions>, String> {
        let Some(profile) = chd_profile(self.ctl.format()) else {
            return Ok(None);
        };
        let c = self.chd_for(profile);
        c.validate(profile)
            .map_err(|e| format!("CHD options: {e}."))?;
        Ok(Some(c.effective(profile)))
    }

    #[cfg(not(feature = "chd"))]
    fn chd_options(&self) -> Result<Option<ChdOptions>, String> {
        Ok(None)
    }

    fn type_char(&mut self, c: char) -> bool {
        match self.row() {
            BRow::Output => self.out_dir.push(c),
            BRow::Extension => {
                let mut ext = self.ctl.extension().to_string();
                ext.push(c);
                self.act(BulkConvertAction::SetExtension(ext));
            }
            _ => return false,
        }
        true
    }

    fn backspace(&mut self) {
        match self.row() {
            BRow::Output => {
                self.out_dir.pop();
            }
            BRow::Extension => {
                let mut ext = self.ctl.extension().to_string();
                ext.pop();
                self.act(BulkConvertAction::SetExtension(ext));
            }
            _ => {}
        }
    }
}

impl App {
    fn bulk_state(&mut self) -> &mut BulkState {
        if self.bulk.is_none() {
            self.bulk = Some(BulkState::default());
        }
        self.bulk.as_mut().unwrap()
    }

    pub(super) fn bulk_key_context(&self) -> (&'static str, &'static [KeyHint]) {
        ("Bulk Convert", HINTS_BULK)
    }

    /// Drive the controller and mirror its file count and bytes into the bar.
    pub(super) fn bulk_tick(&mut self) {
        let Some(b) = self.bulk.as_mut() else {
            return;
        };
        let _ = b.ctl.tick();
        let _ = b.ctl.take_log();
        b.clamp();
        if let Some(p) = b.ctl.progress() {
            b.op = format!("{}/{}  {}", p.index, p.total_files, basename(&p.file));
            if let Some(prog) = self.progress.as_ref() {
                if let Ok(mut sh) = prog.shared.lock() {
                    sh.current = p.bytes;
                    sh.total = p.total_bytes;
                }
            }
        }
        if b.awaiting && !b.ctl.is_running() {
            let done = match b.ctl.last_result() {
                Some(Ok(r)) => Some((
                    format!("Converted {} ok, {} failed.", r.succeeded, r.failed),
                    r.failed > 0,
                )),
                Some(Err(e)) => Some((format!("Bulk convert failed: {e}"), true)),
                None => None,
            };
            if let Some((text, err)) = done {
                b.awaiting = false;
                b.result = Some(text);
                b.is_error = err;
                if let Some(prog) = self.progress.as_ref() {
                    if let Ok(mut sh) = prog.shared.lock() {
                        sh.done = true;
                    }
                }
            }
        }
    }

    /// Bulk convert screen keys. Returns `true` when consumed.
    pub(super) fn handle_bulk_key(&mut self, code: KeyCode) -> bool {
        let b = self.bulk_state();
        // Source / output folder picker (modal).
        if let Some(picker) = b.picker.as_mut() {
            match picker.handle_key(code) {
                Some(PickResult::Cancel) => b.picker = None,
                Some(PickResult::Confirm(path)) => {
                    b.picker = None;
                    if b.picker_target {
                        b.out_dir = path.to_string_lossy().into_owned();
                    } else {
                        b.source = path.to_string_lossy().into_owned();
                        if b.out_dir.is_empty() {
                            b.out_dir = b.source.clone();
                        }
                        b.step = BulkStep::Config;
                        b.field = 0;
                        b.act(BulkConvertAction::Back);
                        b.act(BulkConvertAction::Picked {
                            purpose: purpose::SOURCE,
                            path: Some(expand_tilde(b.source.trim())),
                        });
                        b.rescan();
                    }
                }
                None => {}
            }
            return true;
        }
        match b.step {
            BulkStep::Source => match code {
                KeyCode::Enter | KeyCode::Char('o') => {
                    b.picker = Some(FilePicker::new(PickKind::Dir, "Choose source folder"));
                    b.picker_target = false;
                    true
                }
                _ => false,
            },
            BulkStep::Config => self.bulk_config_key(code),
            BulkStep::Run => {
                if code == KeyCode::Esc {
                    if b.ctl.is_running() {
                        self.cancel_progress();
                        if let Some(b) = self.bulk.as_mut() {
                            b.result = Some("Cancel requested...".to_string());
                            b.is_error = true;
                        }
                    } else {
                        b.result = None;
                        b.step = BulkStep::Config;
                        self.progress = None;
                    }
                }
                true
            }
        }
    }

    fn bulk_config_key(&mut self, code: KeyCode) -> bool {
        let b = self.bulk_state();
        // Outputs that already exist: Skip / Overwrite / Esc back to the list.
        if b.ctl.conflicts().is_some() {
            let choice = match code {
                KeyCode::Char('s') => ConflictChoice::SkipConflicting,
                KeyCode::Char('o') => ConflictChoice::Overwrite,
                KeyCode::Esc => ConflictChoice::Back,
                _ => return true,
            };
            b.act(BulkConvertAction::ResolveConflicts(choice));
            self.after_bulk_start();
            return true;
        }
        let row = b.row();
        let n = b.rows().len();
        match code {
            KeyCode::Esc => b.step = BulkStep::Source,
            KeyCode::Down => b.field = (b.field + 1).min(n - 1),
            KeyCode::Up => b.field = b.field.saturating_sub(1),
            KeyCode::Enter if row == BRow::Start => self.start_bulk(),
            KeyCode::Enter => b.field = (b.field + 1).min(n - 1),
            KeyCode::Left | KeyCode::Right => {
                let forward = code == KeyCode::Right;
                match row {
                    BRow::Format => b.change_format(forward),
                    BRow::ChdMode | BRow::ChdHunk | BRow::ChdCodec(_) => b.change_chd(row, forward),
                    BRow::MultiBin => {
                        let on = !b.ctl.multi_bin();
                        b.act(BulkConvertAction::SetMultiBin(on));
                    }
                    // Left / Right switch tabs from the other rows.
                    _ => return false,
                }
            }
            KeyCode::Char(' ') if row == BRow::MultiBin => {
                let on = !b.ctl.multi_bin();
                b.act(BulkConvertAction::SetMultiBin(on));
            }
            KeyCode::Char(' ') => {
                if let BRow::File(i) = row {
                    let on = !b.files()[i].selected;
                    b.act(BulkConvertAction::SetSelected(i, on));
                }
            }
            KeyCode::Char('a') if matches!(row, BRow::File(_) | BRow::Start) => {
                b.act(BulkConvertAction::SelectAll)
            }
            KeyCode::Char('n') if matches!(row, BRow::File(_) | BRow::Start) => {
                b.act(BulkConvertAction::SelectNone)
            }
            KeyCode::Tab if row == BRow::Output => {
                let mut p = FilePicker::new(PickKind::Dir, "Choose output folder");
                p.input = b.out_dir.clone();
                b.picker = Some(p);
                b.picker_target = true;
            }
            KeyCode::Tab if row == BRow::Extension => b.act(BulkConvertAction::ResetExtension),
            KeyCode::Backspace => b.backspace(),
            KeyCode::Char(c) if !c.is_control() => return b.type_char(c),
            _ => return false,
        }
        true
    }

    /// Start the conversion of the ticked files through the controller.
    pub(super) fn start_bulk(&mut self) {
        let b = self.bulk_state();
        b.send_output(false);
        let refusal = match b.chd_options() {
            Err(e) => Some(e),
            Ok(_) => b.ctl.start_problem().map(str::to_string),
        };
        if let Some(problem) = refusal {
            b.result = Some(problem);
            b.is_error = true;
            b.step = BulkStep::Run;
            return;
        }
        let chd = b.chd_options().ok().flatten();
        b.act(BulkConvertAction::Start(chd));
        self.after_bulk_start();
    }

    /// Move to the Run step once the controller started a conversion.
    fn after_bulk_start(&mut self) {
        let Some(b) = self.bulk.as_mut() else {
            return;
        };
        if !b.ctl.is_running() {
            return;
        }
        b.result = None;
        b.is_error = false;
        b.awaiting = true;
        b.op = "Starting...".to_string();
        b.step = BulkStep::Run;
        self.progress = Some(Progress {
            shared: Arc::new(Mutex::new(ProgressShared::default())),
            tracker: RateTracker::default(),
            label: "Converting".to_string(),
        });
    }

    /// One CHD row's label and value.
    #[cfg(feature = "chd")]
    fn bulk_chd_row(&self, b: &BulkState, row: BRow) -> (&'static str, String) {
        let Some(profile) = chd_profile(b.ctl.format()) else {
            return ("", String::new());
        };
        let c = b.chd_for(profile);
        match row {
            BRow::ChdMode => ("CHD:", format!("< {} >", c.mode.label())),
            BRow::ChdHunk => {
                let h = c.custom.hunk_size;
                let unit = unit_size_for(profile);
                let warn = if h % unit == 0 {
                    String::new()
                } else {
                    format!("  (must be a multiple of {unit})")
                };
                ("  Hunk:", format!("< {h} bytes >{warn}"))
            }
            BRow::ChdCodec(slot) => {
                let label = ["  Codec 1:", "  Codec 2:", "  Codec 3:", "  Codec 4:"][slot.min(3)];
                (label, format!("< {} >", codec_label(c.custom.codecs[slot])))
            }
            _ => ("", String::new()),
        }
    }

    #[cfg(not(feature = "chd"))]
    fn bulk_chd_row(&self, _b: &BulkState, _row: BRow) -> (&'static str, String) {
        ("", String::new())
    }

    pub(super) fn draw_bulk(&self, frame: &mut Frame, area: Rect, b: &BulkState) {
        let title = match b.step {
            BulkStep::Source => "Bulk convert  -  step 1/3: source folder",
            BulkStep::Config => "Bulk convert  -  step 2/3: format & files",
            BulkStep::Run => "Bulk convert  -  step 3/3: run",
        };
        let mut lines: Vec<Line> = Vec::new();
        match b.step {
            BulkStep::Source => {
                lines.push(Line::styled(
                    "Convert every image in a folder to one format.",
                    self.palette.accent(),
                ));
                lines.push(Line::raw(""));
                lines.push(Line::from(vec![
                    Span::styled("  Enter / o  ", self.palette.accent()),
                    Span::raw("choose the source folder"),
                ]));
            }
            BulkStep::Config => self.bulk_form_lines(b, area, &mut lines),
            BulkStep::Run => {
                lines.push(Line::from(vec![
                    Span::styled("Format: ", self.palette.dim()),
                    Span::raw(format_label(b.ctl.format()).to_string()),
                    Span::styled("   Output: ", self.palette.dim()),
                    Span::raw(b.out_dir.clone()),
                ]));
                lines.push(Line::raw(""));
                if !b.op.is_empty() {
                    lines.push(Line::from(vec![
                        Span::styled("Converting: ", self.palette.dim()),
                        Span::raw(b.op.clone()),
                    ]));
                }
                if let Some(res) = &b.result {
                    lines.push(Line::raw(""));
                    let style = if b.is_error {
                        self.palette.warn()
                    } else {
                        self.palette.accent()
                    };
                    lines.push(Line::styled(format!("  {res}"), style));
                    lines.push(Line::styled(
                        "  Esc to return to options.",
                        self.palette.dim(),
                    ));
                }
            }
        }
        frame.render_widget(
            Paragraph::new(Text::from(lines))
                .block(self.pane_block(title, true))
                .wrap(Wrap { trim: false }),
            area,
        );
        if let Some(picker) = &b.picker {
            picker.draw(frame, area, self.palette, self.border);
        }
    }

    fn bulk_form_lines(&self, b: &BulkState, area: Rect, lines: &mut Vec<Line<'static>>) {
        let cur = b.row();
        let style = |row: BRow| {
            if row == cur {
                self.palette
                    .accent()
                    .add_modifier(Modifier::REVERSED | Modifier::BOLD)
            } else {
                self.palette.accent()
            }
        };
        let marker = |row: BRow| if row == cur { "> " } else { "  " };
        let cursor = |row: BRow| {
            if row == cur {
                Span::styled(" ", self.palette.accent().add_modifier(Modifier::REVERSED))
            } else {
                Span::raw("")
            }
        };
        let format = b.ctl.format();
        let files = b.files().len();
        for row in b.rows() {
            match row {
                BRow::Format => {
                    lines.push(Line::from(vec![
                        Span::styled(format!("{}Format:    ", marker(row)), style(row)),
                        Span::raw(format!("< {} >", format_label(format))),
                    ]));
                    let unbuilt = chd_profile(format).is_some() || format == ExportFormat::BinCue;
                    if unbuilt && !cfg!(feature = "chd") {
                        lines.push(Line::styled(
                            "             This build has no CHD support; the conversion will refuse.",
                            self.palette.warn(),
                        ));
                    } else if let Some(note) = format_note(format) {
                        lines.push(Line::styled(
                            format!("             {note}"),
                            self.palette.dim(),
                        ));
                    }
                    if b.ctl.is_scanning() {
                        lines.push(Line::styled(
                            "  Scanning the source folder...",
                            self.palette.dim(),
                        ));
                    }
                }
                BRow::ChdMode | BRow::ChdHunk | BRow::ChdCodec(_) => {
                    let (label, value) = self.bulk_chd_row(b, row);
                    lines.push(Line::from(vec![
                        Span::styled(format!("{}{label:<11}", marker(row)), style(row)),
                        Span::raw(value),
                    ]));
                }
                BRow::MultiBin => {
                    let tick = if b.ctl.multi_bin() { "[x]" } else { "[ ]" };
                    lines.push(Line::styled(
                        format!("{}{tick} {MULTI_BIN_LABEL}", marker(row)),
                        style(row),
                    ));
                    lines.push(Line::styled(
                        format!("             {MULTI_BIN_NOTE}"),
                        self.palette.dim(),
                    ));
                }
                BRow::Output => {
                    lines.push(Line::from(vec![
                        Span::styled(format!("{}Output:    ", marker(row)), style(row)),
                        Span::raw(b.out_dir.clone()),
                        cursor(row),
                    ]));
                    if b.ctl.same_folders() {
                        lines.push(Line::styled(
                            "  Warning: source and output folders are the same - converted \
                             files may overwrite originals.",
                            self.palette.warn(),
                        ));
                    }
                }
                BRow::Extension => {
                    lines.push(Line::from(vec![
                        Span::styled(format!("{}Extension: .", marker(row)), style(row)),
                        Span::raw(b.ctl.extension().to_string()),
                        cursor(row),
                        Span::styled(
                            format!(
                                "   (recommended .{}; Tab resets)",
                                b.ctl.recommended_extension()
                            ),
                            self.palette.dim(),
                        ),
                    ]));
                    lines.push(Line::raw(""));
                    lines.push(Line::styled(
                        format!(
                            "Files ({} of {files} selected)  -  Space ticks, a all, n none:",
                            b.ctl.selected_count()
                        ),
                        self.palette.accent(),
                    ));
                    if let Some(conflicts) = b.ctl.conflicts() {
                        lines.push(Line::styled(
                            format!(
                                "  {} output file(s) already exist:  s Skip them   o Overwrite   \
                                 Esc Back",
                                conflicts.len()
                            ),
                            self.palette.warn(),
                        ));
                        for c in conflicts.iter().take(5) {
                            lines.push(Line::styled(
                                format!("    - {}", basename(&c.dest.to_string_lossy())),
                                self.palette.warn(),
                            ));
                        }
                    }
                    if files == 0 {
                        lines.push(Line::styled(
                            "  (no matching files in this folder for this format)",
                            self.palette.dim(),
                        ));
                    }
                }
                BRow::File(i) => {
                    // Window the list so the cursor stays visible.
                    let avail = (area.height as usize).saturating_sub(16).max(3);
                    let at = match cur {
                        BRow::File(c) => c,
                        _ => 0,
                    };
                    let first = at.saturating_sub(avail.saturating_sub(1));
                    if i < first || i >= first + avail {
                        continue;
                    }
                    let f = &b.files()[i];
                    let tick = if f.selected { "[x]" } else { "[ ]" };
                    let name = basename(&f.path.to_string_lossy());
                    let st = if row == cur {
                        style(row)
                    } else if f.selected {
                        self.palette.accent()
                    } else {
                        self.palette.dim()
                    };
                    lines.push(Line::styled(
                        format!("{}{tick} {:<40} {}", marker(row), name, format_size(f.size)),
                        st,
                    ));
                }
                BRow::Start => {
                    if b.ctl.phase() == BulkPhase::Setup {
                        if let Some(e) = b.ctl.scan_error() {
                            lines.push(Line::styled(format!("  {e}"), self.palette.warn()));
                        }
                    }
                    lines.push(Line::raw(""));
                    lines.push(Line::styled(
                        format!(
                            "{}[ Convert {} file(s) ]",
                            marker(row),
                            b.ctl.selected_count()
                        ),
                        style(row),
                    ));
                }
            }
        }
        lines.push(Line::raw(""));
        lines.push(Line::styled(
            "Up/Down move   Left/Right change   Space tick   Tab browse / reset   Enter Start   \
             Esc back   ? keys",
            self.palette.dim(),
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::super::BACKUP_TAB;
    use super::*;

    fn bulk_on(src: &std::path::Path, out: &std::path::Path) -> App {
        let mut app = App::headless_on(BACKUP_TAB);
        app.bulk = Some(BulkState::default());
        let b = app.bulk.as_mut().unwrap();
        b.source = src.display().to_string();
        b.out_dir = out.display().to_string();
        b.step = BulkStep::Config;
        b.act(BulkConvertAction::Picked {
            purpose: purpose::SOURCE,
            path: Some(src.to_path_buf()),
        });
        app
    }

    fn settle(app: &mut App) {
        app.bulk.as_mut().unwrap().ctl.wait_idle();
        app.tick();
    }

    fn focus(app: &mut App, row: BRow) {
        for _ in 0..40 {
            if app.bulk.as_ref().unwrap().row() == row {
                return;
            }
            app.handle_bulk_key(KeyCode::Down);
        }
        panic!("no row {row:?}");
    }

    fn format_to(app: &mut App, want: ExportFormat) {
        focus_top(app);
        for _ in 0..FORMATS.len() {
            if app.bulk.as_ref().unwrap().ctl.format() == want {
                return;
            }
            app.handle_bulk_key(KeyCode::Right);
        }
        panic!("format {want:?} not offered");
    }

    fn focus_top(app: &mut App) {
        for _ in 0..60 {
            app.handle_bulk_key(KeyCode::Up);
        }
    }

    #[test]
    fn the_bulk_screen_converts_through_the_controller() {
        let src = tempfile::tempdir().expect("tempdir");
        let out = tempfile::tempdir().expect("tempdir");
        for n in ["a.img", "b.img"] {
            std::fs::write(src.path().join(n), vec![0u8; 4096]).unwrap();
        }
        std::fs::write(out.path().join("a.img"), b"keep").unwrap();
        let mut app = bulk_on(src.path(), out.path());
        format_to(&mut app, ExportFormat::Raw);
        settle(&mut app);
        assert_eq!(app.bulk.as_ref().unwrap().files().len(), 2);
        app.start_bulk();
        assert!(app.bulk.as_ref().unwrap().ctl.conflicts().is_some());
        app.handle_bulk_key(KeyCode::Char('s'));
        let b = app.bulk.as_mut().unwrap();
        assert!(b.step == BulkStep::Run);
        b.ctl.wait_idle();
        app.tick();
        let b = app.bulk.as_ref().unwrap();
        assert_eq!(b.result.as_deref(), Some("Converted 1 ok, 0 failed."));
        assert_eq!(std::fs::read(out.path().join("a.img")).unwrap(), b"keep");
        assert!(out.path().join("b.img").exists());
    }

    /// Every dialog format is offered; BIN/CUE has multi-bin; the extension is typed and reset.
    #[test]
    fn the_form_offers_every_format_multi_bin_and_the_extension() {
        let src = tempfile::tempdir().expect("tempdir");
        let out = tempfile::tempdir().expect("tempdir");
        std::fs::write(src.path().join("a.img"), vec![0u8; 4096]).unwrap();
        let mut app = bulk_on(src.path(), out.path());
        for f in [ExportFormat::Xdf, ExportFormat::D88, ExportFormat::Woz] {
            format_to(&mut app, f);
        }
        format_to(&mut app, ExportFormat::BinCue);
        let b = app.bulk.as_ref().unwrap();
        assert!(b.rows().contains(&BRow::MultiBin));
        focus(&mut app, BRow::MultiBin);
        app.handle_bulk_key(KeyCode::Char(' '));
        assert!(app.bulk.as_ref().unwrap().ctl.multi_bin());
        format_to(&mut app, ExportFormat::Raw);
        settle(&mut app);
        assert!(!app.bulk.as_ref().unwrap().rows().contains(&BRow::MultiBin));
        focus(&mut app, BRow::Extension);
        app.handle_bulk_key(KeyCode::Backspace);
        app.handle_bulk_key(KeyCode::Char('x'));
        assert_eq!(app.bulk.as_ref().unwrap().ctl.extension(), "imx");
        app.handle_bulk_key(KeyCode::Tab);
        assert_eq!(app.bulk.as_ref().unwrap().ctl.extension(), "img");
        focus(&mut app, BRow::File(0));
        app.handle_bulk_key(KeyCode::Char('n'));
        assert_eq!(app.bulk.as_ref().unwrap().ctl.selected_count(), 0);
        app.handle_bulk_key(KeyCode::Char('a'));
        assert_eq!(app.bulk.as_ref().unwrap().ctl.selected_count(), 1);
    }

    /// A CHD format shows the CHD rows; a Custom hunk off the unit is refused before Start.
    #[cfg(feature = "chd")]
    #[test]
    fn chd_options_reach_the_start_and_an_off_unit_hunk_is_refused() {
        use crate::controller::app::chd::ChdOptionsMode;
        let src = tempfile::tempdir().expect("tempdir");
        let out = tempfile::tempdir().expect("tempdir");
        let data: Vec<u8> = (0..1u32 << 20).map(|i| (i % 251) as u8).collect();
        std::fs::write(src.path().join("a.img"), data).unwrap();
        let mut app = bulk_on(src.path(), out.path());
        format_to(&mut app, ExportFormat::Chd);
        settle(&mut app);
        assert!(app.bulk.as_ref().unwrap().rows().contains(&BRow::ChdMode));
        focus(&mut app, BRow::ChdMode);
        app.handle_bulk_key(KeyCode::Right);
        let b = app.bulk.as_ref().unwrap();
        assert_eq!(b.chd_for(ChdProfile::Hd).mode, ChdOptionsMode::Custom);
        assert!(b.rows().contains(&BRow::ChdCodec(3)));
        app.bulk.as_mut().unwrap().chd[0].custom.hunk_size = 1000;
        app.start_bulk();
        let b = app.bulk.as_ref().unwrap();
        assert!(!b.ctl.is_running());
        assert!(b.result.as_deref().unwrap().contains("multiple of 512"));
        app.bulk.as_mut().unwrap().chd[0].custom.hunk_size = 4096;
        app.bulk.as_mut().unwrap().step = BulkStep::Config;
        app.start_bulk();
        settle(&mut app);
        let b = app.bulk.as_ref().unwrap();
        assert_eq!(b.result.as_deref(), Some("Converted 1 ok, 0 failed."));
        assert!(out.path().join("a.chd").exists());
    }
}
