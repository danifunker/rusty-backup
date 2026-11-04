//! The Edit Partition Table window over `controller::partition_edit`, the GUI's partition window.
//!
//! The Inspect controller owns the editor; this window keeps only its cursor and turns a key into
//! one [`TableEditAction`] (or Apply / Close) the host sends as `InspectAction::Table`. Each row
//! has its type (typed, or Left / Right through the catalog), size mode, Custom size and boot
//! flag; then the free gaps, the Add fields, Validate and Apply. The Current / After layout is a
//! text line. See docs/phase2_commander_plan.md § "Slice 27".

use crossterm::event::KeyCode;
use ratatui::layout::Rect;
use ratatui::style::Modifier;
use ratatui::text::Line;
use ratatui::Frame;

use super::{hint, App, KeyHint};
use crate::controller::partition_edit::{
    describe_type, format_size, type_choices, type_note, BootStyle, LayoutBar, SizeMode,
    TableEditAction, TableEditView, TableKind,
};

/// One row of the window; `usize` is the partition's position in the view's rows.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum TRow {
    Part(usize),
    Type(usize),
    Mode(usize),
    Size(usize),
    Boot(usize),
    Gap(usize),
    AddStart,
    AddSize,
    AddType,
    AddBoot,
    Add,
    Validate,
    Apply,
}

pub(super) const HINTS_TABLE_EDIT: &[KeyHint] = &[
    hint("Up/Down", "Move between rows"),
    hint(
        "Left/Right",
        "Change a size mode or boot flag; step through the type catalog",
    ),
    hint("Space", "Change the row's choice or toggle a flag"),
    hint("a-z 0-9", "Type into a Type, Size or Add field"),
    hint(
        "Delete",
        "Delete the partition, or undo the delete  (also x)",
    ),
    hint("Enter", "Fill a free gap, Add, Validate or Apply"),
    hint("v", "Validate the edits"),
    hint("w", "Apply Changes (after a clean Validate)"),
    hint("Esc", "Close the window"),
];

/// What a key asks the host to do.
pub(super) enum TableKey {
    Done,
    Edit(TableEditAction),
    Apply,
    Close,
}

/// The window's cursor.
pub(super) struct TableWindow {
    pub(super) sel: TRow,
}

impl Default for TableWindow {
    fn default() -> Self {
        TableWindow { sel: TRow::Part(0) }
    }
}

impl TableWindow {
    /// The rows in drawing order; they follow each partition's state and the table kind.
    pub(super) fn rows(v: &TableEditView) -> Vec<TRow> {
        let mut r = Vec::new();
        for (i, row) in v.rows.iter().enumerate() {
            r.push(TRow::Part(i));
            if row.editable() {
                r.extend([TRow::Type(i), TRow::Mode(i)]);
                if row.size_mode == SizeMode::Custom {
                    r.push(TRow::Size(i));
                }
                if v.boot_style != BootStyle::Label {
                    r.push(TRow::Boot(i));
                }
            }
        }
        r.extend((0..v.gaps.len()).map(TRow::Gap));
        r.extend([TRow::AddStart, TRow::AddSize, TRow::AddType]);
        if v.add_bootable_shown {
            r.push(TRow::AddBoot);
        }
        r.extend([TRow::Add, TRow::Validate, TRow::Apply]);
        r
    }

    fn step(&mut self, v: &TableEditView, delta: isize) {
        let rows = Self::rows(v);
        let n = rows.len() as isize;
        let at = rows.iter().position(|r| *r == self.sel).unwrap_or(0);
        self.sel = rows[((at as isize + delta).rem_euclid(n)) as usize];
    }

    /// Keep the cursor on a row that is still shown.
    pub(super) fn settle(&mut self, v: &TableEditView) {
        let rows = Self::rows(v);
        if !rows.contains(&self.sel) {
            self.sel = match self.sel {
                TRow::Type(i) | TRow::Mode(i) | TRow::Size(i) | TRow::Boot(i)
                    if rows.contains(&TRow::Part(i)) =>
                {
                    TRow::Part(i)
                }
                _ => rows.first().copied().unwrap_or(TRow::Apply),
            };
        }
    }

    /// The typed text of a text row.
    fn text(v: &TableEditView, row: TRow) -> Option<String> {
        Some(match row {
            TRow::Type(i) => v.rows.get(i)?.type_text.clone(),
            TRow::Size(i) => v.rows.get(i)?.size_text.clone(),
            TRow::AddStart => v.add_start.clone(),
            TRow::AddSize => v.add_size.clone(),
            TRow::AddType => v.add_type.clone(),
            _ => return None,
        })
    }

    fn set_text(row: TRow, text: String) -> Option<TableEditAction> {
        Some(match row {
            TRow::Type(i) => TableEditAction::SetType { row: i, text },
            TRow::Size(i) => TableEditAction::SetSizeText { row: i, text },
            TRow::AddStart => TableEditAction::SetAddStart(text),
            TRow::AddSize => TableEditAction::SetAddSize(text),
            TRow::AddType => TableEditAction::SetAddType(text),
            _ => return None,
        })
    }

    /// The partition a row belongs to.
    fn part_of(row: TRow) -> Option<usize> {
        match row {
            TRow::Part(i) | TRow::Type(i) | TRow::Mode(i) | TRow::Size(i) | TRow::Boot(i) => {
                Some(i)
            }
            _ => None,
        }
    }

    /// The catalog's next value after `text` (the first when `text` is not in it).
    fn next_type(kind: TableKind, text: &str, forward: bool) -> Option<&'static str> {
        let list = type_choices(kind);
        let n = list.len();
        if n == 0 {
            return None;
        }
        let at = describe_type(kind, text).and_then(|l| list.iter().position(|c| c.label == l));
        let i = match at {
            Some(i) if forward => (i + 1) % n,
            Some(i) => (i + n - 1) % n,
            None => 0,
        };
        Some(list[i].value)
    }

    /// Left / Right / Space on a choice row.
    fn change(v: &TableEditView, row: TRow, forward: bool) -> Option<TableEditAction> {
        match row {
            TRow::Mode(i) => {
                let r = v.rows.get(i)?;
                let modes = r.mode_choices();
                let n = modes.len();
                let at = modes.iter().position(|m| *m == r.size_mode);
                let mode = match at {
                    Some(k) if forward => modes[(k + 1) % n],
                    Some(k) => modes[(k + n - 1) % n],
                    None => modes[0],
                };
                Some(TableEditAction::SetSizeMode { row: i, mode })
            }
            TRow::Boot(i) => Some(TableEditAction::SetBootable {
                row: i,
                on: !v.rows.get(i)?.bootable,
            }),
            TRow::AddBoot => Some(TableEditAction::SetAddBootable(!v.add_bootable)),
            TRow::Type(_) | TRow::AddType => {
                let text = Self::text(v, row)?;
                let value = Self::next_type(v.kind, text.as_str(), forward)?;
                Self::set_text(row, value.to_string())
            }
            _ => None,
        }
    }

    /// Delete a partition, or undo its delete.
    fn toggle_delete(v: &TableEditView, i: usize) -> Option<TableEditAction> {
        let r = v.rows.get(i)?;
        if r.deleted {
            Some(TableEditAction::Undo(i))
        } else if r.can_delete() {
            Some(TableEditAction::Delete(i))
        } else {
            None
        }
    }

    /// One key; text rows take characters, the others take the letter shortcuts.
    pub(super) fn key(&mut self, v: &TableEditView, code: KeyCode) -> TableKey {
        let row = self.sel;
        let text = Self::text(v, row);
        let edit = |a: Option<TableEditAction>| a.map_or(TableKey::Done, TableKey::Edit);
        match code {
            KeyCode::Esc => TableKey::Close,
            KeyCode::Down | KeyCode::Tab => {
                self.step(v, 1);
                TableKey::Done
            }
            KeyCode::Up | KeyCode::BackTab => {
                self.step(v, -1);
                TableKey::Done
            }
            KeyCode::Left | KeyCode::Right => edit(Self::change(v, row, code == KeyCode::Right)),
            KeyCode::Delete => edit(Self::part_of(row).and_then(|i| Self::toggle_delete(v, i))),
            KeyCode::Enter => match row {
                TRow::Gap(k) => edit(v.gaps.get(k).map(|g| TableEditAction::FillGap {
                    start_lba: g.0,
                    size_bytes: g.1,
                })),
                TRow::Add => TableKey::Edit(TableEditAction::Add),
                TRow::Validate => TableKey::Edit(TableEditAction::Validate),
                TRow::Apply => TableKey::Apply,
                _ => {
                    self.step(v, 1);
                    TableKey::Done
                }
            },
            KeyCode::Backspace => match text {
                Some(mut t) => {
                    t.pop();
                    edit(Self::set_text(row, t))
                }
                None => TableKey::Done,
            },
            KeyCode::Char(c) if text.is_some() && !c.is_control() => {
                let mut t = text.unwrap_or_default();
                t.push(c);
                edit(Self::set_text(row, t))
            }
            KeyCode::Char(' ') => edit(Self::change(v, row, true)),
            KeyCode::Char('x') => edit(Self::part_of(row).and_then(|i| Self::toggle_delete(v, i))),
            KeyCode::Char('v') => TableKey::Edit(TableEditAction::Validate),
            KeyCode::Char('w') => TableKey::Apply,
            _ => TableKey::Done,
        }
    }
}

/// A layout bar as one text line: each partition, and free space between.
pub(super) fn bar_line(bar: &LayoutBar) -> String {
    bar.segments
        .iter()
        .map(|seg| match seg.color {
            Some(_) => format!("[{} {} {}]", seg.label, seg.fs, format_size(seg.size_bytes)),
            None => format!("[free {}]", format_size(seg.size_bytes)),
        })
        .collect::<Vec<_>>()
        .join(" ")
}

impl App {
    /// The Edit Partition Table window over `area`.
    pub(super) fn draw_table_editor(
        &self,
        frame: &mut Frame,
        area: Rect,
        v: &TableEditView,
        w: &TableWindow,
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
        let mut lines: Vec<Line<'static>> = vec![
            Line::raw(format!("  Table type: {}", v.kind.label())),
            Line::styled(format!("  {}", v.current.caption), pal.dim()),
            Line::styled(format!("    {}", bar_line(&v.current)), pal.dim()),
            Line::styled(format!("  {}", v.after.caption), pal.dim()),
            Line::styled(format!("    {}", bar_line(&v.after)), pal.dim()),
            Line::raw(""),
        ];
        let mut focus = 0usize;
        let rows = TableWindow::rows(v);
        for row in rows {
            let sel = w.sel == row;
            let m = if sel { "> " } else { "  " };
            let cursor = if sel { "_" } else { "" };
            let text = match row {
                TRow::Part(i) => {
                    let Some(r) = v.rows.get(i) else {
                        continue;
                    };
                    let num = if r.logical {
                        format!("  {}", r.index)
                    } else {
                        format!("{}", r.index)
                    };
                    let state = if r.deleted {
                        "  deleted (Delete undoes)"
                    } else if r.extended {
                        "  (extended)"
                    } else {
                        ""
                    };
                    let boot = if r.bootable && v.boot_style == BootStyle::Label {
                        "  boot"
                    } else {
                        ""
                    };
                    format!(
                        "#{num:<4} {:<18} LBA {:<10} {}{boot}{state}",
                        r.type_name,
                        r.start_lba,
                        format_size(r.size_bytes)
                    )
                }
                TRow::Type(i) => {
                    let t = v
                        .rows
                        .get(i)
                        .map(|r| r.type_text.clone())
                        .unwrap_or_default();
                    let note = type_note(v.kind, t.as_str())
                        .map(|n| format!("  ({n})"))
                        .unwrap_or_default();
                    format!("    Type:  {t}{cursor}{note}")
                }
                TRow::Mode(i) => match v.rows.get(i) {
                    Some(r) => format!(
                        "    Size:  < {} >  {} MiB",
                        r.size_mode.label(),
                        r.size_text
                    ),
                    None => continue,
                },
                TRow::Size(i) => {
                    let t = v
                        .rows
                        .get(i)
                        .map(|r| r.size_text.clone())
                        .unwrap_or_default();
                    format!("    MiB:   {t}{cursor}")
                }
                TRow::Boot(i) => {
                    let on = v.rows.get(i).is_some_and(|r| r.bootable);
                    match v.boot_style {
                        BootStyle::Radios => {
                            format!("    Boot:  < {} >", if on { "boot" } else { "no boot" })
                        }
                        _ => format!("    Boot:  [{}]", if on { "x" } else { " " }),
                    }
                }
                TRow::Gap(k) => {
                    let label = v.gaps.get(k).map(|g| g.2.clone()).unwrap_or_default();
                    if k == 0 {
                        lines.push(Line::raw(""));
                    }
                    format!("[ Free: {label} ]  (Enter fills the Add fields)")
                }
                TRow::AddStart => {
                    lines.push(Line::raw(""));
                    lines.push(Line::styled(
                        format!(
                            "  Add Partition (will be partition {}; a deleted slot is reused):",
                            v.next_slot
                        ),
                        pal.dim(),
                    ));
                    if v.gaps.is_empty() {
                        lines.push(Line::styled(
                            "  No unallocated space on this disk.",
                            pal.dim(),
                        ));
                    }
                    format!("  Start LBA: {}{cursor}", v.add_start)
                }
                TRow::AddSize => format!("  Size MiB:  {}{cursor}", v.add_size),
                TRow::AddType => {
                    let note = type_note(v.kind, v.add_type.as_str())
                        .map(|n| format!("  ({n})"))
                        .unwrap_or_default();
                    format!("  Type:      {}{cursor}{note}", v.add_type)
                }
                TRow::AddBoot => {
                    format!("  Bootable:  [{}]", if v.add_bootable { "x" } else { " " })
                }
                TRow::Add => "[ Add ]".to_string(),
                TRow::Validate => {
                    lines.push(Line::raw(""));
                    "[ Validate ]".to_string()
                }
                TRow::Apply => {
                    if v.can_apply {
                        "[ Apply Changes ]".to_string()
                    } else {
                        "[ Apply Changes ]  (Validate first)".to_string()
                    }
                }
            };
            if sel {
                focus = lines.len();
            }
            lines.push(Line::styled(format!("{m}{text}"), style_for(sel)));
        }
        for err in &v.errors {
            lines.push(Line::styled(format!("  {err}"), pal.warn()));
        }
        if let Some(status) = &v.status {
            lines.push(Line::styled(format!("  {status}"), pal.accent()));
        }
        if !type_choices(v.kind).is_empty() {
            lines.push(Line::styled(
                format!("  {}", v.kind.field_hint()),
                pal.dim(),
            ));
        }
        self.draw_inspect_popup(frame, area, "Edit Partition Table", lines, focus);
    }
}
