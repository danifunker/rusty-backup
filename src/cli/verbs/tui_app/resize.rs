//! The Resize Partitions window over `controller::resize`, the GUI's resize popup.
//!
//! The Inspect screen owns the controller (opened from Inspect's `ResizePlan`); this window keeps
//! only its cursor and turns a key into one [`ResizeAction`]. Rows: the device-risk box (devices
//! only), each partition's size mode and Custom size, Preview and Apply; the Current / After
//! layout is a text line and the preview a table. The write's Cancel only logs a line, as in the
//! GUI (`apply_resize` has no stop point). See docs/phase2_commander_plan.md § "Slice 27".

use crossterm::event::KeyCode;
use ratatui::layout::Rect;
use ratatui::style::Modifier;
use ratatui::text::Line;
use ratatui::Frame;

use super::table_edit::bar_line;
use super::{hint, App, KeyHint};
use crate::controller::resize::{format_size, ResizeAction, ResizeController, SizeMode};

/// One row of the window; `usize` is the position in the controller's rows.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum RRow {
    Risk,
    Mode(usize),
    Size(usize),
    Preview,
    Apply,
}

pub(super) const HINTS_RESIZE: &[KeyHint] = &[
    hint("Up/Down", "Move between rows"),
    hint("Left/Right", "Change a partition's size mode"),
    hint("Space", "Change the size mode, or tick the device-risk box"),
    hint("0-9", "Type a Custom size in MiB"),
    hint("Enter", "Preview or Apply"),
    hint("p", "Preview the new layout"),
    hint("w", "Apply (after a clean Preview)"),
    hint("Esc", "Close the window (while writing: ask to cancel)"),
];

/// What a key asks the host to do.
pub(super) enum ResizeKey {
    Done,
    Act(ResizeAction),
}

/// The window's cursor.
pub(super) struct ResizeWindow {
    pub(super) sel: RRow,
}

impl Default for ResizeWindow {
    fn default() -> Self {
        ResizeWindow { sel: RRow::Preview }
    }
}

impl ResizeWindow {
    /// The rows in drawing order: extended containers are shown, not edited.
    pub(super) fn rows(c: &ResizeController) -> Vec<RRow> {
        let mut r = Vec::new();
        if c.is_device() {
            r.push(RRow::Risk);
        }
        for (i, row) in c.rows().iter().enumerate() {
            if row.extended {
                continue;
            }
            r.push(RRow::Mode(i));
            if row.mode == SizeMode::Custom {
                r.push(RRow::Size(i));
            }
        }
        r.extend([RRow::Preview, RRow::Apply]);
        r
    }

    /// The window opened: the cursor starts on the first row.
    pub(super) fn reset(&mut self, c: &ResizeController) {
        self.sel = Self::rows(c).first().copied().unwrap_or(RRow::Preview);
    }

    fn step(&mut self, c: &ResizeController, delta: isize) {
        let rows = Self::rows(c);
        let n = rows.len() as isize;
        let at = rows.iter().position(|r| *r == self.sel).unwrap_or(0);
        self.sel = rows[((at as isize + delta).rem_euclid(n)) as usize];
    }

    /// Keep the cursor on a row that is still shown.
    pub(super) fn settle(&mut self, c: &ResizeController) {
        let rows = Self::rows(c);
        if !rows.contains(&self.sel) {
            self.sel = match self.sel {
                RRow::Size(i) => RRow::Mode(i),
                _ => RRow::Preview,
            };
        }
    }

    fn change(c: &ResizeController, row: RRow, forward: bool) -> Option<ResizeAction> {
        match row {
            RRow::Risk => Some(ResizeAction::AcceptDeviceRisk(!c.device_risk_accepted())),
            RRow::Mode(i) => {
                let r = c.rows().get(i)?;
                let modes = r.mode_choices();
                let n = modes.len();
                let mode = match modes.iter().position(|m| *m == r.mode) {
                    Some(k) if forward => modes[(k + 1) % n],
                    Some(k) => modes[(k + n - 1) % n],
                    None => modes[0],
                };
                Some(ResizeAction::SetMode {
                    index: r.index,
                    mode,
                })
            }
            _ => None,
        }
    }

    /// A character typed on a Custom size row (`None` is Backspace).
    fn typed(c: &ResizeController, row: RRow, ch: Option<char>) -> Option<ResizeAction> {
        let RRow::Size(i) = row else {
            return None;
        };
        let r = c.rows().get(i)?;
        let mut text = r.text.clone();
        match ch {
            Some(ch) => text.push(ch),
            None => {
                text.pop();
            }
        }
        Some(ResizeAction::SetSizeText {
            index: r.index,
            text,
        })
    }

    /// One key; the Custom size row takes digits and a decimal point.
    pub(super) fn key(&mut self, c: &ResizeController, code: KeyCode) -> ResizeKey {
        let row = self.sel;
        let act = |a: Option<ResizeAction>| a.map_or(ResizeKey::Done, ResizeKey::Act);
        if c.is_running() {
            return match code {
                KeyCode::Esc => ResizeKey::Act(ResizeAction::Cancel),
                _ => ResizeKey::Done,
            };
        }
        match code {
            KeyCode::Esc => ResizeKey::Act(ResizeAction::Close),
            KeyCode::Down | KeyCode::Tab => {
                self.step(c, 1);
                ResizeKey::Done
            }
            KeyCode::Up | KeyCode::BackTab => {
                self.step(c, -1);
                ResizeKey::Done
            }
            KeyCode::Left | KeyCode::Right => act(Self::change(c, row, code == KeyCode::Right)),
            KeyCode::Char(' ') => act(Self::change(c, row, true)),
            KeyCode::Enter => match row {
                RRow::Preview => ResizeKey::Act(ResizeAction::Preview),
                RRow::Apply => ResizeKey::Act(ResizeAction::Apply),
                _ => {
                    self.step(c, 1);
                    ResizeKey::Done
                }
            },
            KeyCode::Backspace => act(Self::typed(c, row, None)),
            KeyCode::Char(ch) if ch.is_ascii_digit() || ch == '.' => {
                act(Self::typed(c, row, Some(ch)))
            }
            KeyCode::Char('p') => ResizeKey::Act(ResizeAction::Preview),
            KeyCode::Char('w') => ResizeKey::Act(ResizeAction::Apply),
            _ => ResizeKey::Done,
        }
    }
}

impl App {
    /// The Resize Partitions window over `area`.
    pub(super) fn draw_resize(
        &self,
        frame: &mut Frame,
        area: Rect,
        c: &ResizeController,
        w: &ResizeWindow,
    ) {
        let pal = self.palette;
        let style_for = |sel: bool| {
            if sel {
                pal.accent()
                    .add_modifier(Modifier::REVERSED | Modifier::BOLD)
            } else {
                pal.accent()
            }
        };
        let mut lines: Vec<Line<'static>> = Vec::new();
        if c.is_device() {
            lines.push(Line::styled(
                "  WARNING: This will directly modify the physical device. Data loss is \
                 permanent and irreversible!",
                pal.warn(),
            ));
        }
        let mut size = format!("  Disk size: {}", format_size(c.disk_size()));
        if c.can_grow_container() {
            size.push_str("  - to make more room, close this and use Expand Image (x)");
        }
        lines.push(Line::styled(size, pal.dim()));
        let (current, after, _) = c.layout();
        lines.push(Line::styled(
            format!("  {} {}", current.caption, bar_line(&current)),
            pal.dim(),
        ));
        lines.push(Line::styled(
            format!("  {} {}", after.caption, bar_line(&after)),
            pal.dim(),
        ));
        lines.push(Line::raw(""));
        let mut focus = 0usize;
        for (i, r) in c.rows().iter().enumerate() {
            if r.extended {
                lines.push(Line::styled(
                    format!(
                        "  #{:<3} {} (extended)  {}",
                        r.index,
                        r.type_name,
                        format_size(r.original_size)
                    ),
                    pal.dim(),
                ));
                continue;
            }
            let min = if r.minimum_size > 0 {
                format!("  min {}", format_size(r.minimum_size))
            } else {
                String::new()
            };
            let sel = w.sel == RRow::Mode(i);
            if sel {
                focus = lines.len();
            }
            lines.push(Line::styled(
                format!(
                    "{}#{:<3} {:<16} {}{min}  < {} >  {} MiB",
                    if sel { "> " } else { "  " },
                    r.index,
                    r.type_name,
                    format_size(r.original_size),
                    r.mode.label(),
                    r.text
                ),
                style_for(sel),
            ));
            if r.mode == SizeMode::Custom {
                let sel = w.sel == RRow::Size(i);
                if sel {
                    focus = lines.len();
                }
                lines.push(Line::styled(
                    format!(
                        "{}    New size MiB: {}{}",
                        if sel { "> " } else { "  " },
                        r.text,
                        if sel { "_" } else { "" }
                    ),
                    style_for(sel),
                ));
            }
        }
        lines.push(Line::raw(""));
        if c.is_device() {
            let sel = w.sel == RRow::Risk;
            if sel {
                focus = lines.len();
            }
            lines.push(Line::styled(
                format!(
                    "{}[{}] I understand the risks and want to proceed",
                    if sel { "> " } else { "  " },
                    if c.device_risk_accepted() { "x" } else { " " }
                ),
                style_for(sel),
            ));
        }
        if let Some(err) = c.plan_error() {
            lines.push(Line::styled(format!("  Error: {err}"), pal.warn()));
        }
        if let Some(preview) = c.preview() {
            lines.push(Line::styled("  Preview:", pal.accent()));
            for p in preview {
                lines.push(Line::raw(format!(
                    "    #{:<3} {} {} -> {} {}  {}",
                    p.index, p.old_start, p.old_size, p.new_start, p.new_size, p.action
                )));
            }
        }
        for (row, label) in [(RRow::Preview, "[ Preview ]"), (RRow::Apply, "[ Apply ]")] {
            let sel = w.sel == row;
            if sel {
                focus = lines.len();
            }
            let note = match row {
                RRow::Apply if c.is_running() => "  (writing...)",
                RRow::Apply if c.is_device() && !c.device_risk_accepted() => {
                    "  (tick the risk box)"
                }
                RRow::Apply if !c.can_apply() => "  (Preview first)",
                _ => "",
            };
            lines.push(Line::styled(
                format!("{}{label}{note}", if sel { "> " } else { "  " }),
                style_for(sel),
            ));
        }
        self.draw_inspect_popup(frame, area, "Resize Partitions", lines, focus);
    }
}
