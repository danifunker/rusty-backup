//! Inspect's small dialogs: Resize Alto Disk, Expand HFS Volume and Convert Floppy Container.
//!
//! Each is a window over its controller (`controller::alto_resize`, `controller::expand_hfs`,
//! `controller::floppy_convert`), the same ones the egui dialogs drive. The Inspect screen owns
//! the controllers and answers their pickers; this module keeps the cursor (one dialog is up at a
//! time) and the Expand HFS size being typed, and turns a key into one action. See
//! docs/phase2_commander_plan.md § "Slice 27".

use crossterm::event::KeyCode;
use ratatui::layout::Rect;
use ratatui::style::Modifier;
use ratatui::text::Line;
use ratatui::Frame;

use super::{hint, App, KeyHint};
use crate::controller::alto_resize::{AltoResizeAction, AltoResizeController, AltoTarget};
use crate::controller::expand_hfs::{ExpandHfsAction, ExpandHfsController};
use crate::controller::floppy_convert::{
    FloppyConvertAction, FloppyConvertController, FloppyTarget,
};

/// One row of whichever dialog is up.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum DRow {
    AltoTarget,
    AltoSave,
    HfsFormat,
    HfsBlock,
    HfsSize,
    HfsOutput,
    HfsExpand,
    FloppySource,
    FloppyTarget,
    FloppyOutput,
    FloppyConvert,
}

const ALTO_ROWS: &[DRow] = &[DRow::AltoTarget, DRow::AltoSave];
const HFS_ROWS: &[DRow] = &[
    DRow::HfsFormat,
    DRow::HfsBlock,
    DRow::HfsSize,
    DRow::HfsOutput,
    DRow::HfsExpand,
];
const FLOPPY_ROWS: &[DRow] = &[
    DRow::FloppySource,
    DRow::FloppyTarget,
    DRow::FloppyOutput,
    DRow::FloppyConvert,
];

pub(super) const HINTS_ALTO_RESIZE: &[KeyHint] = &[
    hint("Up/Down", "Move between rows"),
    hint("Left/Right", "Choose the target geometry  (also Space)"),
    hint("Enter", "Resize and Save As... (asks for the .pdi)"),
    hint("s", "Resize and Save As..."),
    hint("Esc", "Close (not while writing)"),
];

pub(super) const HINTS_EXPAND_HFS: &[KeyHint] = &[
    hint("Up/Down", "Move between rows"),
    hint(
        "Left/Right",
        "Output format, block size, or the size by 1 MiB",
    ),
    hint("0-9", "Type the target size in MiB (Enter sets it)"),
    hint("Enter", "Set the size, pick the output file, or Expand"),
    hint("s", "Expand"),
    hint("Esc", "Close (not while expanding)"),
];

pub(super) const HINTS_FLOPPY_CONVERT: &[KeyHint] = &[
    hint("Up/Down", "Move between rows"),
    hint("Left/Right", "Choose the output format  (also Space)"),
    hint("Enter", "Choose the source or output file, or Convert"),
    hint("s", "Convert"),
    hint("Esc", "Close (not while converting)"),
];

/// What a key asks the host to do.
pub(super) enum DialogKey {
    Done,
    Alto(AltoResizeAction),
    Hfs(ExpandHfsAction),
    Floppy(FloppyConvertAction),
}

/// The open dialog's cursor and the Expand HFS size while it is typed.
pub(super) struct DialogWindow {
    pub(super) sel: DRow,
    pub(super) size_edit: Option<String>,
}

impl Default for DialogWindow {
    fn default() -> Self {
        DialogWindow {
            sel: DRow::AltoTarget,
            size_edit: None,
        }
    }
}

/// The next item of `list` after `cur` (or before it), wrapping.
fn cycle<T: Copy + PartialEq>(list: &[T], cur: T, forward: bool) -> Option<T> {
    let n = list.len();
    if n == 0 {
        return None;
    }
    Some(match list.iter().position(|x| *x == cur) {
        Some(i) if forward => list[(i + 1) % n],
        Some(i) => list[(i + n - 1) % n],
        None => list[0],
    })
}

impl DialogWindow {
    /// A dialog opened: the cursor goes to its first row.
    pub(super) fn reset(&mut self, first: DRow) {
        self.sel = first;
        self.size_edit = None;
    }

    fn step(&mut self, rows: &[DRow], delta: isize) {
        let n = rows.len() as isize;
        let at = rows.iter().position(|r| *r == self.sel).unwrap_or(0);
        self.sel = rows[((at as isize + delta).rem_euclid(n)) as usize];
    }

    /// Move the cursor; `None` when the key is not a move.
    fn moved(&mut self, rows: &[DRow], code: KeyCode) -> Option<()> {
        if !rows.contains(&self.sel) {
            self.sel = rows[0];
        }
        match code {
            KeyCode::Down | KeyCode::Tab => self.step(rows, 1),
            KeyCode::Up | KeyCode::BackTab => self.step(rows, -1),
            _ => return None,
        }
        Some(())
    }

    pub(super) fn alto_key(&mut self, c: &AltoResizeController, code: KeyCode) -> DialogKey {
        if self.moved(ALTO_ROWS, code).is_some() {
            return DialogKey::Done;
        }
        let target = |forward| {
            cycle(AltoTarget::ALL.as_slice(), c.target(), forward).map_or(DialogKey::Done, |t| {
                DialogKey::Alto(AltoResizeAction::SetTarget(t))
            })
        };
        match (code, self.sel) {
            (KeyCode::Esc, _) => DialogKey::Alto(AltoResizeAction::Close),
            (KeyCode::Left, DRow::AltoTarget) => target(false),
            (KeyCode::Right | KeyCode::Char(' '), DRow::AltoTarget) => target(true),
            (KeyCode::Enter, DRow::AltoTarget) => {
                self.step(ALTO_ROWS, 1);
                DialogKey::Done
            }
            (KeyCode::Enter, _) | (KeyCode::Char('s'), _) => {
                DialogKey::Alto(AltoResizeAction::SaveAs)
            }
            _ => DialogKey::Done,
        }
    }

    pub(super) fn hfs_key(&mut self, c: &ExpandHfsController, code: KeyCode) -> DialogKey {
        // Leaving the size row, or Enter on it, sets what was typed.
        let typed = self
            .size_edit
            .as_deref()
            .and_then(|t| t.parse::<u32>().ok());
        let leaving = matches!(
            code,
            KeyCode::Down | KeyCode::Tab | KeyCode::Up | KeyCode::BackTab | KeyCode::Enter
        );
        if self.sel == DRow::HfsSize && leaving && self.size_edit.is_some() {
            self.size_edit = None;
            if code == KeyCode::Enter {
                self.step(HFS_ROWS, 1);
            } else {
                self.moved(HFS_ROWS, code);
            }
            return typed.map_or(DialogKey::Done, |n| {
                DialogKey::Hfs(ExpandHfsAction::SetSizeMib(n))
            });
        }
        if self.moved(HFS_ROWS, code).is_some() {
            return DialogKey::Done;
        }
        let row = self.sel;
        let change = |forward: bool| -> Option<ExpandHfsAction> {
            Some(match row {
                DRow::HfsFormat => ExpandHfsAction::SetFlatHfv(!c.flat_hfv()),
                DRow::HfsBlock => {
                    let blocks: Vec<u32> = c.block_choices().iter().map(|b| b.bytes).collect();
                    ExpandHfsAction::SetBlockSize(cycle(
                        blocks.as_slice(),
                        c.block_size(),
                        forward,
                    )?)
                }
                DRow::HfsSize => ExpandHfsAction::SetSizeMib(if forward {
                    c.size_mib().saturating_add(1)
                } else {
                    c.size_mib().saturating_sub(1)
                }),
                _ => return None,
            })
        };
        let act = |a: Option<ExpandHfsAction>| a.map_or(DialogKey::Done, DialogKey::Hfs);
        match code {
            KeyCode::Esc => DialogKey::Hfs(ExpandHfsAction::Close),
            KeyCode::Left => act(change(false)),
            KeyCode::Right => act(change(true)),
            KeyCode::Char(' ') if row != DRow::HfsSize => act(change(true)),
            KeyCode::Char(d) if row == DRow::HfsSize && d.is_ascii_digit() => {
                let mut t = self.size_edit.take().unwrap_or_default();
                t.push(d);
                self.size_edit = Some(t);
                DialogKey::Done
            }
            KeyCode::Backspace if row == DRow::HfsSize => {
                let mut t = self
                    .size_edit
                    .take()
                    .unwrap_or_else(|| c.size_mib().to_string());
                t.pop();
                self.size_edit = Some(t);
                DialogKey::Done
            }
            KeyCode::Enter => match row {
                DRow::HfsOutput => DialogKey::Hfs(ExpandHfsAction::PickOutput),
                DRow::HfsExpand => DialogKey::Hfs(ExpandHfsAction::Start),
                _ => {
                    self.step(HFS_ROWS, 1);
                    DialogKey::Done
                }
            },
            KeyCode::Char('s') => DialogKey::Hfs(ExpandHfsAction::Start),
            _ => DialogKey::Done,
        }
    }

    pub(super) fn floppy_key(&mut self, c: &FloppyConvertController, code: KeyCode) -> DialogKey {
        if self.moved(FLOPPY_ROWS, code).is_some() {
            return DialogKey::Done;
        }
        let target = |forward| {
            cycle(FloppyTarget::ALL.as_slice(), c.target(), forward).map_or(DialogKey::Done, |t| {
                DialogKey::Floppy(FloppyConvertAction::SetTarget(t))
            })
        };
        match (code, self.sel) {
            (KeyCode::Esc, _) => DialogKey::Floppy(FloppyConvertAction::Close),
            (KeyCode::Left, DRow::FloppyTarget) => target(false),
            (KeyCode::Right | KeyCode::Char(' '), DRow::FloppyTarget) => target(true),
            (KeyCode::Enter, DRow::FloppySource) => {
                DialogKey::Floppy(FloppyConvertAction::PickSource)
            }
            (KeyCode::Enter, DRow::FloppyOutput) => {
                DialogKey::Floppy(FloppyConvertAction::PickOutput)
            }
            (KeyCode::Enter, DRow::FloppyConvert) | (KeyCode::Char('s'), _) => {
                DialogKey::Floppy(FloppyConvertAction::Start)
            }
            (KeyCode::Enter, _) => {
                self.step(FLOPPY_ROWS, 1);
                DialogKey::Done
            }
            _ => DialogKey::Done,
        }
    }
}

impl App {
    fn dialog_style(&self, sel: bool) -> ratatui::style::Style {
        if sel {
            self.palette
                .accent()
                .add_modifier(Modifier::REVERSED | Modifier::BOLD)
        } else {
            self.palette.accent()
        }
    }

    /// A row of a dialog, marked when the cursor is on it; returns the line's index.
    fn dialog_row(
        &self,
        lines: &mut Vec<Line<'static>>,
        w: &DialogWindow,
        row: DRow,
        text: String,
    ) -> Option<usize> {
        let sel = w.sel == row;
        let at = lines.len();
        lines.push(Line::styled(
            format!("{}{text}", if sel { "> " } else { "  " }),
            self.dialog_style(sel),
        ));
        sel.then_some(at)
    }

    pub(super) fn draw_alto_resize(
        &self,
        frame: &mut Frame,
        area: Rect,
        c: &AltoResizeController,
        w: &DialogWindow,
    ) {
        let pal = self.palette;
        let mut lines: Vec<Line<'static>> = Vec::new();
        if let Some(path) = c.source() {
            lines.push(Line::raw(format!("  Source: {}", path.display())));
        }
        if c.opening().is_some() {
            lines.push(Line::styled("  Reading the Alto pack...", pal.dim()));
            self.draw_inspect_popup(frame, area, "Resize Alto Disk", lines, 0);
            return;
        }
        lines.push(Line::raw(format!("  {}", c.summary_line().unwrap_or(""))));
        lines.push(Line::raw(""));
        let mut focus = 0;
        let row = format!("Target geometry:  < {} >", c.target().label());
        focus = self
            .dialog_row(&mut lines, w, DRow::AltoTarget, row)
            .unwrap_or(focus);
        if let Some(warning) = c.fit_warning() {
            lines.push(Line::styled(format!("  {warning}"), pal.warn()));
        }
        lines.push(Line::raw(""));
        let save = if c.can_save() {
            "[ Resize and Save As... ]"
        } else {
            "[ Resize and Save As... ]  (waiting)"
        };
        focus = self
            .dialog_row(&mut lines, w, DRow::AltoSave, save.to_string())
            .unwrap_or(focus);
        if c.is_writing() {
            lines.push(Line::styled("  Resizing...", pal.dim()));
        }
        match c.result() {
            Some(Ok(msg)) => lines.push(Line::styled(format!("  {msg}"), pal.accent())),
            Some(Err(e)) => lines.push(Line::styled(format!("  Error: {e}"), pal.warn())),
            None => {}
        }
        self.draw_inspect_popup(frame, area, "Resize Alto Disk", lines, focus);
    }

    pub(super) fn draw_expand_hfs(
        &self,
        frame: &mut Frame,
        area: Rect,
        c: &ExpandHfsController,
        w: &DialogWindow,
    ) {
        let pal = self.palette;
        let Some(src) = c.source() else {
            return;
        };
        const MIB: u64 = 1024 * 1024;
        let mut lines: Vec<Line<'static>> = vec![
            Line::styled("  Source", pal.accent()),
            Line::raw(format!("  Volume: {}", src.volume_name)),
            Line::raw(format!(
                "  Size: {} MiB ({} bytes)",
                src.partition_size / MIB,
                src.partition_size
            )),
            Line::raw(format!(
                "  Allocation block size: {} KiB",
                src.source_block_size / 1024
            )),
            Line::raw(format!(
                "  Used: {} MiB across {} file(s) in {} folder(s)",
                src.used_bytes / MIB,
                src.file_count,
                src.dir_count
            )),
            Line::raw(""),
            Line::styled("  Target", pal.accent()),
        ];
        let mut focus = 0;
        let format = if c.flat_hfv() {
            "Bare HFS image (.hfv / .img)"
        } else {
            "APM disk (.hda)"
        };
        let (lo, hi) = c.size_bounds();
        let size = match &w.size_edit {
            Some(t) => format!("{t}_"),
            None => c.size_mib().to_string(),
        };
        let output = c
            .output()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "(not chosen)".to_string());
        let expand = if c.can_start() {
            "[ Expand ]"
        } else if c.editable() {
            "[ Expand ]  (choose the output file first)"
        } else {
            "[ Expand ]  (done; Esc closes)"
        };
        let rows = [
            (DRow::HfsFormat, format!("Output format:  < {format} >")),
            (
                DRow::HfsBlock,
                format!("Allocation block size:  < {} >", c.block_size_label()),
            ),
            (
                DRow::HfsSize,
                format!("Target size MiB:  {size}  ({lo}-{hi})"),
            ),
            (
                DRow::HfsOutput,
                format!("Output file:  {output}  [Save As...]"),
            ),
            (DRow::HfsExpand, expand.to_string()),
        ];
        for (row, text) in rows {
            focus = self.dialog_row(&mut lines, w, row, text).unwrap_or(focus);
            if row == DRow::HfsFormat && c.flat_hfv() {
                lines.push(Line::styled(
                    "    Bare classic-HFS volume with no partition table (BasiliskII / \
                     SheepShaver .hfv). Capped at 4063 MB.",
                    pal.dim(),
                ));
            }
        }
        lines.push(Line::raw(""));
        if let Some(snap) = c.progress() {
            let step = if snap.phase.is_empty() {
                "Starting...".to_string()
            } else {
                snap.phase
            };
            lines.push(Line::styled(format!("  {step}"), pal.dim()));
        } else if let Some(Err(e)) = c.outcome() {
            lines.push(Line::styled(format!("  Failed: {e}"), pal.warn()));
        } else if c.outcome().is_some() {
            lines.push(Line::styled("  Expand complete.", pal.accent()));
        }
        let all = c.lines();
        let skip = all.len().saturating_sub(6);
        for l in all.into_iter().skip(skip) {
            lines.push(Line::styled(format!("  {l}"), pal.dim()));
        }
        self.draw_inspect_popup(frame, area, "Expand HFS Volume", lines, focus);
    }

    pub(super) fn draw_floppy_convert(
        &self,
        frame: &mut Frame,
        area: Rect,
        c: &FloppyConvertController,
        w: &DialogWindow,
    ) {
        let pal = self.palette;
        let mut lines: Vec<Line<'static>> = Vec::new();
        let mut focus = 0;
        let source = c
            .source()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "(not chosen)".to_string());
        let probing = if c.probing() { "  reading..." } else { "" };
        focus = self
            .dialog_row(
                &mut lines,
                w,
                DRow::FloppySource,
                format!("Source:  {source}  [Choose File...]{probing}"),
            )
            .unwrap_or(focus);
        if let Some(kind) = c.source_kind() {
            lines.push(Line::raw(format!("    Detected: {kind}")));
        }
        if let Some(err) = c.source_error() {
            lines.push(Line::styled(format!("    {err}"), pal.warn()));
        }
        lines.push(Line::styled(
            "    A folder of floppies converts on the Bulk tab.",
            pal.dim(),
        ));
        lines.push(Line::raw(""));
        let output = c
            .output()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "(not chosen)".to_string());
        let convert = if c.can_start() {
            "[ Convert ]"
        } else if c.editable() {
            "[ Convert ]  (choose the source and output first)"
        } else {
            "[ Convert ]  (done; Esc closes)"
        };
        let rows = [
            (
                DRow::FloppyTarget,
                format!("Output format:  < {} >", c.target().label()),
            ),
            (
                DRow::FloppyOutput,
                format!("Output file:  {output}  [Save As...]"),
            ),
            (DRow::FloppyConvert, convert.to_string()),
        ];
        for (row, text) in rows {
            focus = self.dialog_row(&mut lines, w, row, text).unwrap_or(focus);
            if row == DRow::FloppyOutput && c.same_format() {
                lines.push(Line::styled(
                    "    Source and target match - the output will be a byte-identical copy.",
                    pal.dim(),
                ));
            }
        }
        lines.push(Line::raw(""));
        if let Some(snap) = c.progress() {
            lines.push(Line::styled(format!("  {}", snap.phase), pal.dim()));
        } else {
            match c.outcome() {
                Some(Err(e)) => lines.push(Line::styled(format!("  Failed: {e}"), pal.warn())),
                Some(Ok(done)) => {
                    lines.push(Line::styled(format!("  {}", done.summary), pal.accent()));
                    lines.push(Line::raw(format!("  {}", done.line)));
                }
                None => {}
            }
        }
        self.draw_inspect_popup(frame, area, "Convert Floppy Container", lines, focus);
    }
}
