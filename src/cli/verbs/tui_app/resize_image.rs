//! The Resize Image wizard over `controller::resize_image`, as the GUIs show it.
//!
//! The Inspect screen owns the controller (opened from Inspect's `ResizeImageSource`); this
//! window keeps only its cursor and turns a key into one [`ResizeImageAction`]. Each page's rows
//! are the controls the GUIs draw; the bars are text lines. See docs/resize_image_wizard_plan.md.

use crossterm::event::KeyCode;
use ratatui::layout::Rect;
use ratatui::style::Modifier;
use ratatui::text::Line;
use ratatui::Frame;

use super::{hint, App, KeyHint};
use crate::controller::inspect::{LayoutSegment, SegmentKind};
use crate::controller::resize_image::{
    format_size, labels, Page, PartMode, ResizeImageAction as A, ResizeImageController, StepState,
    Verdict,
};

/// One row of the window; `usize` is the position in the controller's rows or checks.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum WRow {
    Disk,
    Check,
    Repair(usize),
    Anyway,
    Mode(usize),
    Size(usize),
    Copy,
    InPlace,
    Output,
    Back,
    Next,
    Open,
    Close,
}

pub(super) const HINTS_RESIZE_IMAGE: &[KeyHint] = &[
    hint("Up/Down", "Move between rows"),
    hint(
        "Left/Right",
        "Change a choice (a partition's mode, copy or in place)",
    ),
    hint("Space", "Change a choice, or tick a box"),
    hint("0-9 K M G", "Type a size"),
    hint("Enter", "Press the row's button; on a field, go on"),
    hint("n / b", "Next / Back"),
    hint("Esc", "Close the window (while resizing: stop)"),
];

/// What a key asks the host to do.
pub(super) enum WizardKey {
    Done,
    Act(A),
}

/// The window's cursor.
pub(super) struct WizardWindow {
    pub(super) sel: WRow,
}

impl Default for WizardWindow {
    fn default() -> Self {
        WizardWindow { sel: WRow::Disk }
    }
}

fn repairable(v: &Option<Verdict>) -> bool {
    matches!(
        v,
        Some(Verdict::Errors {
            repairable: true,
            ..
        })
    )
}

impl WizardWindow {
    /// The rows of the page the controller is on, in drawing order.
    pub(super) fn rows(c: &ResizeImageController) -> Vec<WRow> {
        let mut r = Vec::new();
        match c.page() {
            Page::Size => r.push(WRow::Disk),
            Page::Check => {
                r.push(WRow::Check);
                for (i, row) in c.checks().iter().enumerate() {
                    if repairable(&row.verdict) {
                        r.push(WRow::Repair(i));
                    }
                }
                if c.check_trouble() {
                    r.push(WRow::Anyway);
                }
            }
            Page::Partitions => {
                for (i, row) in c.rows().iter().enumerate() {
                    r.push(WRow::Mode(i));
                    if row.fixed.is_none() && row.mode == PartMode::Custom {
                        r.push(WRow::Size(i));
                    }
                }
            }
            Page::Review => {
                r.push(WRow::Copy);
                if !c.in_place() {
                    r.push(WRow::Output);
                }
                r.push(WRow::InPlace);
            }
            Page::Work => {}
            Page::Done => {
                if c.outcome().is_some_and(|o| o.is_ok()) {
                    r.push(WRow::Open);
                }
            }
        }
        if matches!(c.page(), Page::Check | Page::Partitions | Page::Review) {
            r.push(WRow::Back);
        }
        if !matches!(c.page(), Page::Work | Page::Done) {
            r.push(WRow::Next);
        }
        r.push(WRow::Close);
        r
    }

    /// Keep the cursor on a row of the page; a new page starts on its first row.
    pub(super) fn settle(&mut self, c: &ResizeImageController) {
        let rows = Self::rows(c);
        if !rows.contains(&self.sel) {
            self.sel = match self.sel {
                WRow::Size(i) => WRow::Mode(i),
                _ => rows.first().copied().unwrap_or(WRow::Close),
            };
            if !rows.contains(&self.sel) {
                self.sel = rows.first().copied().unwrap_or(WRow::Close);
            }
        }
    }

    fn step(&mut self, c: &ResizeImageController, delta: isize) {
        let rows = Self::rows(c);
        let n = rows.len() as isize;
        let at = rows.iter().position(|r| *r == self.sel).unwrap_or(0);
        self.sel = rows[((at as isize + delta).rem_euclid(n)) as usize];
    }

    fn change(c: &ResizeImageController, row: WRow, forward: bool) -> Option<A> {
        match row {
            WRow::Check => Some(A::SetCheck(!c.check_enabled())),
            WRow::Anyway => Some(A::ContinueAnyway(!c.continue_anyway())),
            WRow::Copy | WRow::InPlace => Some(A::SetInPlace(!c.in_place())),
            WRow::Mode(i) => {
                let r = c.rows().get(i)?;
                let modes = r.modes();
                let n = modes.len();
                let mode = match modes.iter().position(|m| *m == r.mode) {
                    Some(k) if forward => modes[(k + 1) % n],
                    Some(k) => modes[(k + n - 1) % n],
                    None => modes[0],
                };
                Some(A::SetMode {
                    index: r.index,
                    mode,
                })
            }
            _ => None,
        }
    }

    /// A character typed on a size field (`None` is Backspace).
    fn typed(c: &ResizeImageController, row: WRow, ch: Option<char>) -> Option<A> {
        let edit = |mut text: String| {
            match ch {
                Some(ch) => text.push(ch),
                None => {
                    text.pop();
                }
            }
            text
        };
        match row {
            WRow::Disk => Some(A::SetDiskSize(edit(c.disk_text().to_string()))),
            WRow::Size(i) => {
                let r = c.rows().get(i)?;
                Some(A::SetSizeText {
                    index: r.index,
                    text: edit(r.text.clone()),
                })
            }
            _ => None,
        }
    }

    fn next(c: &ResizeImageController) -> A {
        match c.page() {
            Page::Review => A::Start,
            _ => A::Next,
        }
    }

    pub(super) fn key(&mut self, c: &ResizeImageController, code: KeyCode) -> WizardKey {
        let row = self.sel;
        let act = |a: Option<A>| a.map_or(WizardKey::Done, WizardKey::Act);
        if c.is_running() {
            return match code {
                KeyCode::Esc => WizardKey::Act(A::Cancel),
                _ => WizardKey::Done,
            };
        }
        let field = matches!(row, WRow::Disk | WRow::Size(_));
        match code {
            KeyCode::Esc => WizardKey::Act(A::Close),
            KeyCode::Down | KeyCode::Tab => {
                self.step(c, 1);
                WizardKey::Done
            }
            KeyCode::Up | KeyCode::BackTab => {
                self.step(c, -1);
                WizardKey::Done
            }
            KeyCode::Left | KeyCode::Right => act(Self::change(c, row, code == KeyCode::Right)),
            KeyCode::Char(' ') => act(Self::change(c, row, true)),
            KeyCode::Backspace => act(Self::typed(c, row, None)),
            KeyCode::Char(ch)
                if field && (ch.is_ascii_digit() || ch == '.' || "kKmMgGtT".contains(ch)) =>
            {
                act(Self::typed(c, row, Some(ch)))
            }
            KeyCode::Char('n') => WizardKey::Act(Self::next(c)),
            KeyCode::Char('b') => WizardKey::Act(A::Back),
            KeyCode::Enter => match row {
                WRow::Repair(i) => act(c.checks().get(i).map(|r| A::Repair(r.index))),
                WRow::Output => WizardKey::Act(A::PickOutput),
                WRow::Back => WizardKey::Act(A::Back),
                WRow::Next => WizardKey::Act(Self::next(c)),
                WRow::Open => WizardKey::Act(A::OpenResult),
                WRow::Close => WizardKey::Act(A::Close),
                WRow::Disk | WRow::Size(_) => WizardKey::Act(Self::next(c)),
                _ => act(Self::change(c, row, true)),
            },
            _ => WizardKey::Done,
        }
    }
}

/// A bar as text: `[1: Data 16.0 MiB] [free 39.0 MiB]`.
fn bar_text(segments: &[LayoutSegment]) -> String {
    segments
        .iter()
        .map(|s| match s.kind {
            SegmentKind::Free => format!("[free {}]", format_size(s.size_bytes)),
            _ => format!("[{} {}]", s.label, format_size(s.size_bytes)),
        })
        .collect::<Vec<_>>()
        .join(" ")
}

impl App {
    /// The Resize Image window over `area`.
    pub(super) fn draw_resize_image(
        &self,
        frame: &mut Frame,
        area: Rect,
        c: &ResizeImageController,
        w: &WizardWindow,
    ) {
        let pal = self.palette;
        let style_for = |sel: bool| match sel {
            true => pal
                .accent()
                .add_modifier(Modifier::REVERSED | Modifier::BOLD),
            false => pal.accent(),
        };
        let mut lines: Vec<Line<'static>> = vec![Line::styled(
            format!("  {}", c.page().heading()),
            pal.accent().add_modifier(Modifier::BOLD),
        )];
        lines.push(Line::raw(""));
        let mut focus = 0usize;
        let mut row = |lines: &mut Vec<Line<'static>>, r: WRow, text: String| {
            let sel = w.sel == r;
            if sel {
                focus = lines.len();
            }
            let mark = if sel { "> " } else { "  " };
            lines.push(Line::styled(format!("{mark}{text}"), style_for(sel)));
        };
        match c.page() {
            Page::Size => {
                let name = c
                    .source_path()
                    .and_then(|p| p.file_name())
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_default();
                lines.push(Line::raw(format!(
                    "  {name}: {} now.",
                    format_size(c.current_size())
                )));
                let cursor = if w.sel == WRow::Disk { "_" } else { "" };
                row(
                    &mut lines,
                    WRow::Disk,
                    format!("{}: {}{cursor}", labels::DISK_SIZE, c.disk_text()),
                );
                lines.push(Line::styled(format!("  {}", labels::SIZE_HINT), pal.dim()));
                lines.push(Line::raw(format!("  {}", c.disk_change())));
            }
            Page::Check => {
                let tick = if c.check_enabled() { "x" } else { " " };
                row(
                    &mut lines,
                    WRow::Check,
                    format!("[{tick}] {}", labels::CHECK),
                );
                if !c.check_enabled() {
                    lines.push(Line::styled(
                        "  The volumes will not be checked.",
                        pal.dim(),
                    ));
                }
                for (i, r) in c.checks().iter().enumerate() {
                    let v = r.verdict.as_ref().map(|v| v.text());
                    let v = v.unwrap_or_else(|| "Checking...".to_string());
                    lines.push(Line::raw(format!("  {}: {v}", r.title)));
                    if repairable(&r.verdict) {
                        row(&mut lines, WRow::Repair(i), "  [ Repair... ]".to_string());
                    }
                }
                if c.check_trouble() {
                    let tick = if c.continue_anyway() { "x" } else { " " };
                    let text = format!("[{tick}] {}", labels::CONTINUE_ANYWAY);
                    row(&mut lines, WRow::Anyway, text);
                }
            }
            Page::Partitions => {
                let (before, after) = c.bars();
                lines.push(Line::styled(
                    format!("  Now:   {}", bar_text(&before)),
                    pal.dim(),
                ));
                lines.push(Line::styled(
                    format!("  After: {}", bar_text(&after)),
                    pal.dim(),
                ));
                lines.push(Line::raw(""));
                let fill = c.final_sizes().unwrap_or_default();
                for (i, r) in c.rows().iter().enumerate() {
                    let size = match r.mode {
                        PartMode::Fill => fill
                            .iter()
                            .find(|(x, _)| *x == r.index)
                            .map(|s| format!("{:.2}", s.1 as f64 / 1048576.0))
                            .unwrap_or_else(|| r.text.clone()),
                        _ => r.text.clone(),
                    };
                    let text = format!("{}  < {} >  {size} MiB", r.title, r.mode.label());
                    row(&mut lines, WRow::Mode(i), text);
                    if r.fixed.is_none() && r.mode == PartMode::Custom {
                        let cursor = if w.sel == WRow::Size(i) { "_" } else { "" };
                        let text = format!("    New size MiB: {}{cursor}", r.text);
                        row(&mut lines, WRow::Size(i), text);
                    }
                    match c.row_warning(r.index) {
                        Some(warn) => lines.push(Line::styled(format!("    {warn}"), pal.warn())),
                        None => lines.push(Line::styled(format!("    {}", r.note()), pal.dim())),
                    }
                }
                if let Some(f) = c.free_at_end() {
                    lines.push(Line::raw(format!(
                        "  Free space left at the end of the disk: {}",
                        format_size(f)
                    )));
                }
            }
            Page::Review => {
                for line in c.review() {
                    lines.push(Line::raw(format!("  {line}")));
                }
                lines.push(Line::raw(""));
                let copy = if c.in_place() { "( )" } else { "(o)" };
                row(
                    &mut lines,
                    WRow::Copy,
                    format!("{copy} Write a resized copy"),
                );
                if !c.in_place() {
                    let out = c.output().map(|p| p.display().to_string());
                    let text = format!("    {}  [ Change... ]", out.unwrap_or_default());
                    row(&mut lines, WRow::Output, text);
                }
                let inp = if c.in_place() { "(o)" } else { "( )" };
                row(
                    &mut lines,
                    WRow::InPlace,
                    format!("{inp} {}", labels::IN_PLACE),
                );
            }
            Page::Work | Page::Done => {
                for (step, state) in c.steps() {
                    let (mark, note) = match &state {
                        StepState::Pending => ("     ", String::new()),
                        StepState::Running => ("...  ", String::new()),
                        StepState::Done(n) => ("OK   ", format!(" - {n}")),
                        StepState::Failed(n) => ("FAIL ", format!(" - {n}")),
                    };
                    lines.push(Line::raw(format!("  {mark}{}{note}", step.label())));
                }
                if let Some(s) = c.progress() {
                    let pct = s
                        .progress
                        .fraction()
                        .map(|x| format!(" ({:.0}%)", x * 100.0));
                    lines.push(Line::styled(
                        format!("  {}{}", s.phase, pct.unwrap_or_default()),
                        pal.dim(),
                    ));
                }
                for r in c.after_checks() {
                    let v = r.verdict.as_ref().map(|v| v.text()).unwrap_or_default();
                    lines.push(Line::raw(format!("  {}: {v}", r.title)));
                }
                match c.outcome() {
                    Some(Ok(())) => {
                        let out = c.output().map(|p| p.display().to_string());
                        lines.push(Line::styled(
                            format!("  Done. The resized image is {}.", out.unwrap_or_default()),
                            pal.accent(),
                        ));
                    }
                    Some(Err(e)) => lines.push(Line::styled(format!("  Failed: {e}"), pal.warn())),
                    None => {}
                }
            }
        }
        let error = match c.page() {
            Page::Partitions => c
                .live_error()
                .or_else(|| c.plan_error().map(str::to_string)),
            Page::Review => c.plan_error().map(str::to_string),
            Page::Size => c.disk_size().err(),
            _ => None,
        };
        if let Some(e) = error {
            lines.push(Line::styled(format!("  {e}"), pal.warn()));
        }
        lines.push(Line::raw(""));
        let next = match c.page() {
            Page::Review => labels::START,
            Page::Partitions if c.planning() => "Checking...",
            _ => "Next",
        };
        let close = match c.page() {
            Page::Done => "Close",
            Page::Work => "Stop (Esc)",
            _ => "Cancel",
        };
        for (r, label) in [
            (WRow::Open, labels::OPEN_RESULT),
            (WRow::Back, "Back"),
            (WRow::Next, next),
            (WRow::Close, close),
        ] {
            if Self::wizard_has(c, r) {
                row(&mut lines, r, format!("[ {label} ]"));
            }
        }
        self.draw_inspect_popup(frame, area, labels::TITLE, lines, focus);
    }

    fn wizard_has(c: &ResizeImageController, r: WRow) -> bool {
        WizardWindow::rows(c).contains(&r)
    }
}
