//! The disk-writer window (the GUI's Physical Disk Export) over a `DiskWriteController`.
//!
//! The Restore screen's Write Image File and the Inspect screen's Physical Disk Export both open
//! it: target disk or image file, the region of the disk, and the synthesized-table form. The host
//! owns the controller; this window keeps only its cursor and the typed image path, and a key
//! comes back as a [`WriterKey`] the host carries out. See docs/phase2_commander_plan.md
//! § "Slice 26".

use crossterm::event::KeyCode;
use ratatui::layout::Rect;
use ratatui::style::Modifier;
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Clear, Paragraph};
use ratatui::Frame;

use super::{centered_rect, expand_tilde, hint, App, KeyHint};
use crate::controller::disk_write::{
    format_size, DiskWriteAction, DiskWriteController, SizeMode, TableKind, WrapAlign,
    TYPE_BYTE_PRESETS,
};

/// One row of the window.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum WRow {
    Device,
    Image,
    Region,
    Synthesize,
    Table,
    Align,
    AlignCustom,
    TypeByte,
    Size,
    SizeMib,
    Write,
}

pub(super) const HINTS_WRITER: &[KeyHint] = &[
    hint("Up/Down", "Move between rows"),
    hint("Enter", "Pick a device, set the image file, or write"),
    hint("Tab", "Browse for the image file"),
    hint("Left/Right", "Change the region or a table choice"),
    hint("Space", "Toggle Synthesize, or change a choice"),
    hint("0-9 a-f", "Type the type byte in hex; digits for sizes"),
    hint("w", "Write to the target"),
    hint("Esc", "Close the window"),
];

/// What a key in the window asks the host to do.
pub(super) enum WriterKey {
    /// Nothing more: the cursor moved, text was typed, or the window does not take the key.
    Done,
    Act(DiskWriteAction),
    PickDevice,
    BrowseImage,
    Write,
    Close,
}

/// The window's cursor and its typed image file (set with Enter).
pub(super) struct WriterWindow {
    pub(super) sel: WRow,
    pub(super) image: String,
}

impl Default for WriterWindow {
    fn default() -> Self {
        WriterWindow {
            sel: WRow::Device,
            image: String::new(),
        }
    }
}

impl WriterWindow {
    /// The rows in drawing order; they follow the target and the synthesize form.
    pub(super) fn rows(w: &DiskWriteController) -> Vec<WRow> {
        let mut r = vec![WRow::Device, WRow::Image];
        if w.target_device().is_some() {
            r.push(WRow::Region);
        }
        if w.can_synthesize() {
            r.push(WRow::Synthesize);
            let form = w.form();
            if form.synthesize {
                r.extend([WRow::Table, WRow::Align]);
                if form.alignment == WrapAlign::Custom {
                    r.push(WRow::AlignCustom);
                }
                r.extend([WRow::TypeByte, WRow::Size]);
                if form.size_mode == SizeMode::Custom {
                    r.push(WRow::SizeMib);
                }
            }
        }
        r.push(WRow::Write);
        r
    }

    fn step(&mut self, w: &DiskWriteController, delta: isize) {
        let rows = Self::rows(w);
        let n = rows.len() as isize;
        let at = rows.iter().position(|r| *r == self.sel).unwrap_or(0);
        self.sel = rows[((at as isize + delta).rem_euclid(n)) as usize];
    }

    /// Keep the cursor on a row that is still shown.
    pub(super) fn settle(&mut self, w: &DiskWriteController) {
        if !Self::rows(w).contains(&self.sel) {
            self.sel = WRow::Device;
        }
    }

    /// The typed image file as the controller's target, when one was typed.
    pub(super) fn image_target(&self) -> Option<DiskWriteAction> {
        let path = expand_tilde(self.image.trim());
        (!path.as_os_str().is_empty()).then_some(DiskWriteAction::SetTargetImage(path))
    }

    fn change(w: &DiskWriteController, row: WRow, forward: bool) -> Option<DiskWriteAction> {
        let form = *w.form();
        let pick = |n: usize, i: usize| {
            if forward {
                (i + 1) % n
            } else {
                (i + n - 1) % n
            }
        };
        Some(match row {
            WRow::Region => DiskWriteAction::CycleTargetPartition,
            WRow::Synthesize => DiskWriteAction::SetSynthesize(!form.synthesize),
            WRow::Table => DiskWriteAction::SetTableKind(match form.table_kind {
                TableKind::Mbr => TableKind::Gpt,
                TableKind::Gpt => TableKind::Mbr,
            }),
            WRow::Align => {
                let all = [
                    WrapAlign::DosTraditional,
                    WrapAlign::Modern1MB,
                    WrapAlign::Custom,
                ];
                let i = all.iter().position(|a| *a == form.alignment).unwrap_or(0);
                DiskWriteAction::SetAlignment(all[pick(all.len(), i)])
            }
            WRow::TypeByte => {
                let n = TYPE_BYTE_PRESETS.len();
                let i = match TYPE_BYTE_PRESETS
                    .iter()
                    .position(|(b, _)| *b == form.type_byte)
                {
                    Some(i) => pick(n, i),
                    None => 0,
                };
                DiskWriteAction::SetTypeByte(TYPE_BYTE_PRESETS[i].0)
            }
            WRow::Size => {
                let all = w.size_choices();
                let i = all.iter().position(|m| *m == form.size_mode).unwrap_or(0);
                DiskWriteAction::SetSize {
                    mode: all[pick(all.len(), i)],
                    custom_mib: form.custom_size_mib,
                }
            }
            _ => return None,
        })
    }

    /// A digit (or hex digit on the type byte) typed on `row`; `None` is Backspace.
    fn digit(w: &DiskWriteController, row: WRow, c: Option<char>) -> Option<DiskWriteAction> {
        let form = *w.form();
        Some(match row {
            WRow::TypeByte => {
                let b = match c.and_then(|c| c.to_digit(16)) {
                    Some(d) => (form.type_byte << 4) | d as u8,
                    None if c.is_none() => form.type_byte >> 4,
                    None => return None,
                };
                DiskWriteAction::SetTypeByte(b)
            }
            WRow::AlignCustom => {
                let n = form.custom_alignment_sectors;
                DiskWriteAction::SetCustomAlignment(match c.and_then(|c| c.to_digit(10)) {
                    Some(d) => n.saturating_mul(10).saturating_add(d as u64),
                    None if c.is_none() => n / 10,
                    None => return None,
                })
            }
            WRow::SizeMib => {
                let n = form.custom_size_mib;
                DiskWriteAction::SetSize {
                    mode: SizeMode::Custom,
                    custom_mib: match c.and_then(|c| c.to_digit(10)) {
                        Some(d) => n.saturating_mul(10).saturating_add(d),
                        None if c.is_none() => n / 10,
                        None => return None,
                    },
                }
            }
            _ => return None,
        })
    }

    /// One key in the window.
    pub(super) fn key(&mut self, w: &DiskWriteController, code: KeyCode) -> WriterKey {
        let row = self.sel;
        let act = |a: Option<DiskWriteAction>| a.map_or(WriterKey::Done, WriterKey::Act);
        match code {
            KeyCode::Esc => WriterKey::Close,
            KeyCode::Down => {
                self.step(w, 1);
                WriterKey::Done
            }
            KeyCode::Up | KeyCode::BackTab => {
                self.step(w, -1);
                WriterKey::Done
            }
            KeyCode::Tab if row == WRow::Image => WriterKey::BrowseImage,
            KeyCode::Enter => match row {
                WRow::Device => WriterKey::PickDevice,
                WRow::Image => act(self.image_target()),
                WRow::Write => WriterKey::Write,
                _ => {
                    self.step(w, 1);
                    WriterKey::Done
                }
            },
            KeyCode::Char(' ') if row != WRow::Image => act(Self::change(w, row, true)),
            KeyCode::Left | KeyCode::Right => act(Self::change(w, row, code == KeyCode::Right)),
            KeyCode::Backspace if row == WRow::Image => {
                self.image.pop();
                WriterKey::Done
            }
            KeyCode::Backspace => act(Self::digit(w, row, None)),
            KeyCode::Char(c) if row == WRow::Image && !c.is_control() => {
                self.image.push(c);
                WriterKey::Done
            }
            KeyCode::Char('w') => WriterKey::Write,
            KeyCode::Char(c) => act(Self::digit(w, row, Some(c))),
            _ => WriterKey::Done,
        }
    }
}

impl App {
    /// The writer window over `area`, titled `title`.
    pub(super) fn draw_writer(
        &self,
        frame: &mut Frame,
        area: Rect,
        title: &str,
        w: &DiskWriteController,
        win: &WriterWindow,
    ) {
        let pal = self.palette;
        let mut lines: Vec<Line<'static>> = Vec::new();
        if let Some(snap) = w.opening() {
            lines.push(Line::styled(format!("  {}", snap.label), pal.dim()));
        }
        if let Some(src) = w.source() {
            lines.push(Line::styled(
                format!("  Source: {}", src.path.display()),
                pal.dim(),
            ));
            lines.push(Line::styled(
                format!(
                    "  Size: {}  -  {}  -  {}",
                    format_size(src.size_bytes),
                    src.fs_hint,
                    if src.has_partition_table {
                        "has partition table"
                    } else {
                        "superfloppy (no partition table)"
                    }
                ),
                pal.dim(),
            ));
        }
        lines.push(Line::raw(""));
        let style_for = |sel: bool| {
            if sel {
                pal.accent()
                    .add_modifier(Modifier::REVERSED | Modifier::BOLD)
            } else {
                pal.accent()
            }
        };
        let form = *w.form();
        let mut focus_line = 0usize;
        for row in WriterWindow::rows(w) {
            let sel = win.sel == row;
            if sel {
                focus_line = lines.len();
            }
            let m = if sel { "> " } else { "  " };
            let text = match row {
                WRow::Device => format!(
                    "Target disk: {}",
                    w.target_device()
                        .map(|d| format!("{} ({})", d.display_name(), format_size(d.size_bytes)))
                        .unwrap_or_else(|| "(Enter picks a device)".into())
                ),
                WRow::Image => format!(
                    "Or file:     {}{}{}",
                    win.image,
                    if sel { "_" } else { "" },
                    match w.target_image() {
                        Some(p) => format!("   [target: {}]", p.display()),
                        None if win.image.is_empty() && !sel => {
                            "(type a path, Enter sets it; Tab browses)".into()
                        }
                        None => String::new(),
                    }
                ),
                WRow::Region => {
                    if w.target_scan().is_some() {
                        "Write to:    (reading the target's partition table...)".into()
                    } else {
                        let parts = w.target_partitions();
                        let size = w.target_device().map_or(0, |d| d.size_bytes);
                        let region = match w
                            .target_partition()
                            .and_then(|i| parts.iter().find(|p| p.index == i))
                        {
                            Some(p) => p.label.clone(),
                            None => format!(
                                "Entire disk ({}) - replaces the partition table",
                                format_size(size)
                            ),
                        };
                        format!("Write to:    < {region} >")
                    }
                }
                WRow::Synthesize => format!(
                    "{} Synthesize partition table",
                    if form.synthesize { "[x]" } else { "[ ]" }
                ),
                WRow::Table => format!(
                    "  Table:     < {} >",
                    match form.table_kind {
                        TableKind::Mbr => "MBR",
                        TableKind::Gpt => "GPT",
                    }
                ),
                WRow::Align => format!(
                    "  Alignment: < {} >",
                    match form.alignment {
                        WrapAlign::DosTraditional => "DOS legacy (LBA 63)",
                        WrapAlign::Modern1MB => "Modern (LBA 2048)",
                        WrapAlign::Custom => "Custom",
                    }
                ),
                WRow::AlignCustom => {
                    format!("  Sectors:   {}", form.custom_alignment_sectors)
                }
                WRow::TypeByte => format!(
                    "  Type:      < {} >  (0-9 a-f types a byte)",
                    TYPE_BYTE_PRESETS
                        .iter()
                        .find(|(b, _)| *b == form.type_byte)
                        .map(|(_, n)| (*n).to_string())
                        .unwrap_or_else(|| format!("0x{:02X} (custom)", form.type_byte))
                ),
                WRow::Size => format!(
                    "  Size:      < {} >",
                    match form.size_mode {
                        SizeMode::Custom => "Custom",
                        SizeMode::FillRemaining => "Fill the disk",
                        _ => "Original",
                    }
                ),
                WRow::SizeMib => {
                    let (lo, hi) = w.custom_bounds_mib();
                    format!("  MiB:       {}  ({lo}-{hi})", form.custom_size_mib)
                }
                WRow::Write => {
                    lines.push(Line::raw(""));
                    if sel {
                        focus_line = lines.len();
                    }
                    let hint = if w.target_partition().is_some() {
                        "replaces the selected partition"
                    } else {
                        "erases all data on the target"
                    };
                    lines.push(Line::from(vec![
                        Span::styled(format!("{m}[ Write to disk... ]"), style_for(sel)),
                        Span::styled(format!("  {hint}"), pal.dim()),
                    ]));
                    continue;
                }
            };
            lines.push(Line::styled(format!("{m}{text}"), style_for(sel)));
            if row == WRow::Region {
                if let Some(warning) = w.partition_warning() {
                    lines.push(Line::styled(format!("    {warning}"), pal.warn()));
                }
            }
        }
        if !w.can_synthesize() && w.source().is_some() {
            lines.push(Line::styled(
                "  Synthesize partition table: the source already has one.",
                pal.dim(),
            ));
        }
        if let Some(err) = w.error() {
            lines.push(Line::styled(format!("  {err}"), pal.warn()));
        }
        lines.push(Line::raw(""));
        lines.push(Line::styled(
            "Up/Down row  Left/Right change  Enter act  w write  Esc close",
            pal.dim(),
        ));
        let h = (lines.len() as u16 + 2).min(area.height.saturating_sub(2));
        let inner = h.saturating_sub(2) as usize;
        let scroll = if inner > 2 && focus_line + 2 > inner {
            (focus_line + 2 - inner) as u16
        } else {
            0
        };
        let popup = centered_rect(90.min(area.width), h, area);
        frame.render_widget(Clear, popup);
        frame.render_widget(
            Paragraph::new(Text::from(lines))
                .block(self.pane_block(title, true))
                .scroll((scroll, 0)),
            popup,
        );
    }
}
