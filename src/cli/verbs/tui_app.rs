//! `rb-cli tui` — a full-screen terminal UI (milestone M1: the tab shell).
//!
//! This mirrors the desktop GUI: a top tab bar (Backup / Restore / Inspect /
//! New Disk / Optical / Archives / Commander / Bulk / Settings) over the same
//! shared `src/model/` runner layer the GUI uses. Real tabs so far: **Inspect**
//! (a selectable disk list; a disk, image, backup or remote image opens on the
//! shared Inspect controller with its grid and disk tools, see
//! `tui_app/inspect.rs`, each partition opening a filesystem **Explorer**, one
//! browser pane over the partition: see `tui_app/explorer.rs`); **New Disk** (a wizard that
//! creates blank images through the shared `new` verb); and **Settings** (an
//! interactive `UpdateConfig` editor). The progress overlay (live rate + ETA via
//! `RateTracker`) is wired as a preview. Later milestones fill the remaining
//! tabs (Backup / Restore / Optical / Archives / Commander / Bulk). See
//! `docs/tui_plan.md`.
//!
//! Conventions (Midnight Commander / CUA + k9s / lazygit): Left/Right change
//! tabs, Up/Down move the selection within a tab, Enter drills in, Esc backs
//! out; a persistent context-sensitive footer key bar, a `?` help overlay,
//! selection via reversed video, a privilege badge, and a bottom progress bar.
//!
//! Portability: pure Rust (ratatui + crossterm, no C libraries, no OS-version
//! floor beyond the toolchain's). Borders adapt to the terminal — rounded
//! Unicode box-drawing on capable UTF-8 terminals, an ASCII fallback (`+ - |`)
//! on serial / MiSTer / old vt (see [`choose_border_set`]; `RB_TUI_ASCII=1`
//! forces the fallback). `$NO_COLOR` is honored, and it degrades gracefully
//! below a minimum terminal size instead of crashing.

use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::layout::{Alignment, Constraint, Direction, Flex, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::symbols::border;

// `symbols::border::Set` gained a lifetime parameter in ratatui 0.30
// (`Set<'a>`); the vintage macOS 10.7 build pins ratatui 0.29, where it's a
// plain `Set`. This alias lets the same source compile against both — the
// `ratatui-legacy` feature is enabled only by the vintage manifest's `tui`.
#[cfg(feature = "ratatui-legacy")]
type BorderSet = border::Set;
#[cfg(not(feature = "ratatui-legacy"))]
type BorderSet = border::Set<'static>;
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph, Tabs, Wrap};
use ratatui::{DefaultTerminal, Frame};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::cli::verbs::new::{FloppyArgs, FloppyFs, HdCommand, NewCommand, VolumeArgs, VolumeFs};
use crate::controller::app::{format_size, RateTracker};
use crate::controller::app::{DiskDevice, RecentMode};

mod archives;
mod ask;
mod backup;
mod commander;
mod dialogs;
mod explorer;
mod inspect;
#[cfg(feature = "optical")]
mod optical;
mod resize;
mod restore;
mod settings;
mod shell;
mod table_edit;
mod writer;

use archives::ArchiveState;
use backup::BackupState;
use inspect::InspectState;
#[cfg(feature = "optical")]
use optical::OpticalState;
use restore::RestoreState;
use settings::SettingsState;

/// Shared state for a running progress task: a worker thread advances
/// `current` toward `total`; the UI thread reads it each frame.
#[derive(Default)]
struct ProgressShared {
    current: u64,
    total: u64,
    done: bool,
    cancel: bool,
}

/// A running progress task the UI is showing. The `RateTracker` lives on the UI
/// side and is fed the shared byte count each tick — the same speed/ETA
/// estimator the GUI and CLI use.
struct Progress {
    shared: Arc<Mutex<ProgressShared>>,
    tracker: RateTracker,
    label: String,
}

/// One row of a disk's partition table read without elevation (the Inspect screen's fallback).
struct PartRow {
    label: String,
    fs_hint: String,
    size: u64,
}

/// Render raw bytes for the View overlay: as text lines, or a hex dump when the
/// content looks binary.
fn bytes_to_lines(bytes: &[u8]) -> Vec<String> {
    if bytes.is_empty() {
        return vec!["(empty)".to_string()];
    }
    let mut lines = Vec::new();
    if looks_textual(bytes) {
        for line in String::from_utf8_lossy(bytes).lines().take(5000) {
            lines.push(line.replace('\t', "    "));
        }
    } else {
        for (i, chunk) in bytes.chunks(16).take(4096).enumerate() {
            let hex: String = chunk.iter().map(|b| format!("{b:02x} ")).collect();
            let ascii: String = chunk
                .iter()
                .map(|&b| {
                    if (0x20..0x7f).contains(&b) {
                        b as char
                    } else {
                        '.'
                    }
                })
                .collect();
            lines.push(format!("{:08x}  {hex:<48} {ascii}", i * 16));
        }
    }
    lines
}

/// One row in the Browse file navigator.
struct BrowseEntry {
    name: String,
    is_dir: bool,
    /// A directory that is itself a rusty-backup (holds `metadata.json`).
    is_backup: bool,
    /// The synthetic "[Use this directory]" row (directory-pick mode only).
    select_current: bool,
    path: std::path::PathBuf,
}

/// What a [`FilePicker`] is allowed to confirm.
#[derive(Clone, Copy, PartialEq, Eq)]
enum PickKind {
    /// An existing file or directory (open: image file or backup folder).
    Any,
    /// An existing file (import source).
    File,
    /// An existing directory (export destination).
    Dir,
    /// A file to write: not a directory, in a folder that exists.
    Save,
}

/// The outcome of feeding a key to a [`FilePicker`].
enum PickResult {
    Cancel,
    Confirm(std::path::PathBuf),
}

/// A shared modal file/path picker used by every path dialog (Open, Export,
/// Import). Offers a text input, an optional recent-files (MRU) list, and a
/// **Tab-to-browse** filesystem navigator — so all dialogs behave identically.
struct FilePicker {
    kind: PickKind,
    title: String,
    input: String,
    error: Option<String>,
    /// Optional recent list (Open only); empty means no recent group.
    recent: Vec<String>,
    recent_sel: usize,
    /// Browse sub-mode.
    browse: bool,
    dir: std::path::PathBuf,
    entries: Vec<BrowseEntry>,
    sel: usize,
    /// While browsing in Dir mode: the name being typed for a new folder.
    new_folder: Option<String>,
}

impl FilePicker {
    fn new(kind: PickKind, title: impl Into<String>) -> Self {
        FilePicker {
            kind,
            title: title.into(),
            input: String::new(),
            error: None,
            recent: Vec::new(),
            recent_sel: 0,
            browse: false,
            dir: dirs::home_dir().unwrap_or_else(|| std::path::PathBuf::from("/")),
            entries: Vec::new(),
            sel: 0,
            new_folder: None,
        }
    }
    fn with_recent(mut self, recent: Vec<String>) -> Self {
        self.recent = recent;
        self
    }

    fn page(&self) -> usize {
        let rows = crossterm::terminal::size().map(|(_, r)| r).unwrap_or(24);
        (rows.saturating_sub(6) as usize).max(1)
    }

    /// Feed a key. Returns `Some(Cancel/Confirm)` when the dialog is done.
    fn handle_key(&mut self, code: KeyCode) -> Option<PickResult> {
        if self.browse {
            return self.browse_key(code);
        }
        match code {
            KeyCode::Esc => Some(PickResult::Cancel),
            KeyCode::Enter => self.confirm_input(),
            KeyCode::Tab => {
                self.enter_browse();
                None
            }
            KeyCode::Up => {
                self.recent_sel = self.recent_sel.saturating_sub(1);
                None
            }
            KeyCode::Down => {
                if !self.recent.is_empty() {
                    self.recent_sel = (self.recent_sel + 1).min(self.recent.len() - 1);
                }
                None
            }
            KeyCode::Backspace => {
                self.input.pop();
                self.error = None;
                None
            }
            KeyCode::Char(c) if !c.is_control() => {
                self.input.push(c);
                self.error = None;
                None
            }
            _ => None,
        }
    }

    fn confirm_input(&mut self) -> Option<PickResult> {
        let raw = if !self.input.trim().is_empty() {
            self.input.trim().to_string()
        } else if let Some(p) = self.recent.get(self.recent_sel) {
            p.clone()
        } else {
            self.error = Some("Type a path, or Tab to browse.".to_string());
            return None;
        };
        self.validate(expand_tilde(&raw))
    }

    fn validate(&mut self, path: std::path::PathBuf) -> Option<PickResult> {
        let ok = match self.kind {
            PickKind::Any => path.exists(),
            // A raw device (`/dev/sda`, `/dev/disk0`) is neither a regular file
            // nor a directory, so `is_file()` rejected every one of them with
            // "No such file" - for something plainly present. Accepting
            // "exists and is not a directory" lets a device be typed wherever an
            // image can be, which is what makes Commander able to open a raw
            // partition at all. Everything downstream already copes: the
            // partition probe reads through a plain reader, and sizes come from
            // seeking rather than `metadata.len()` (which is 0 for a device).
            PickKind::File => path.exists() && !path.is_dir(),
            PickKind::Dir => path.is_dir(),
            PickKind::Save => {
                let parent = path.parent().filter(|p| !p.as_os_str().is_empty());
                !path.is_dir() && parent.is_some_and(|p| p.is_dir())
            }
        };
        if ok {
            Some(PickResult::Confirm(path))
        } else {
            let what = match self.kind {
                PickKind::Dir => "directory",
                PickKind::File | PickKind::Save => "file",
                PickKind::Any => "path",
            };
            // Distinguish "wrong kind" from "absent"; the old message claimed
            // absence for both.
            self.error = Some(if path.exists() {
                format!("Not a {what}: {}", path.display())
            } else {
                format!("No such {what}: {}", path.display())
            });
            None
        }
    }

    fn enter_browse(&mut self) {
        let typed = expand_tilde(self.input.trim());
        let start = if !self.input.trim().is_empty() && typed.is_dir() {
            typed
        } else if let Some(parent) = typed.parent().filter(|p| p.is_dir()) {
            parent.to_path_buf()
        } else {
            self.dir.clone()
        };
        self.browse = true;
        self.sel = 0;
        self.populate(start);
    }

    fn populate(&mut self, dir: std::path::PathBuf) {
        let mut entries = Vec::new();
        // Directory-pick mode: a row to choose the directory you're standing in.
        if self.kind == PickKind::Dir {
            entries.push(BrowseEntry {
                name: "[Use this directory]".to_string(),
                is_dir: true,
                is_backup: false,
                select_current: true,
                path: dir.clone(),
            });
        }
        if let Some(parent) = dir.parent() {
            entries.push(BrowseEntry {
                name: "..".to_string(),
                is_dir: true,
                is_backup: false,
                select_current: false,
                path: parent.to_path_buf(),
            });
        }
        match std::fs::read_dir(&dir) {
            Ok(rd) => {
                let mut items: Vec<BrowseEntry> = rd
                    .flatten()
                    .filter_map(|e| {
                        let name = e.file_name().to_string_lossy().into_owned();
                        if name.starts_with('.') {
                            return None;
                        }
                        let path = e.path();
                        let is_dir = path.is_dir();
                        let is_backup = is_dir && path.join("metadata.json").is_file();
                        Some(BrowseEntry {
                            name,
                            is_dir,
                            is_backup,
                            select_current: false,
                            path,
                        })
                    })
                    .collect();
                items.sort_by(|a, b| {
                    b.is_dir
                        .cmp(&a.is_dir)
                        .then_with(|| a.name.to_lowercase().cmp(&b.name.to_lowercase()))
                });
                entries.extend(items);
                self.error = None;
            }
            Err(e) => self.error = Some(format!("{}: {e}", dir.display())),
        }
        self.dir = dir;
        self.entries = entries;
    }

    fn browse_key(&mut self, code: KeyCode) -> Option<PickResult> {
        // New-folder name sub-prompt (Dir mode).
        if self.new_folder.is_some() {
            match code {
                KeyCode::Esc => self.new_folder = None,
                KeyCode::Enter => self.create_folder(),
                KeyCode::Backspace => {
                    if let Some(n) = self.new_folder.as_mut() {
                        n.pop();
                    }
                }
                KeyCode::Char(c) if !c.is_control() => {
                    if let Some(n) = self.new_folder.as_mut() {
                        n.push(c);
                    }
                }
                _ => {}
            }
            return None;
        }

        let page = self.page();
        let last = self.entries.len().saturating_sub(1);
        match code {
            KeyCode::Esc | KeyCode::Tab => {
                self.browse = false;
                self.error = None;
                None
            }
            // Make a new folder here (destination-directory mode only).
            KeyCode::Char('n') if self.kind == PickKind::Dir => {
                self.new_folder = Some(String::new());
                None
            }
            KeyCode::Up => {
                self.sel = self.sel.saturating_sub(1);
                None
            }
            KeyCode::Down => {
                if !self.entries.is_empty() {
                    self.sel = (self.sel + 1).min(last);
                }
                None
            }
            KeyCode::PageUp => {
                self.sel = self.sel.saturating_sub(page);
                None
            }
            KeyCode::PageDown => {
                if !self.entries.is_empty() {
                    self.sel = (self.sel + page).min(last);
                }
                None
            }
            KeyCode::Home => {
                self.sel = 0;
                None
            }
            KeyCode::End => {
                self.sel = last;
                None
            }
            KeyCode::Backspace | KeyCode::Left => {
                if let Some(parent) = self.dir.parent().map(|p| p.to_path_buf()) {
                    self.populate(parent);
                    self.sel = 0;
                }
                None
            }
            KeyCode::Enter | KeyCode::Right => self.browse_activate(),
            _ => None,
        }
    }

    fn browse_activate(&mut self) -> Option<PickResult> {
        let e = self.entries.get(self.sel)?;
        if e.select_current {
            return Some(PickResult::Confirm(self.dir.clone()));
        }
        let (path, is_dir, is_backup) = (e.path.clone(), e.is_dir, e.is_backup);
        match self.kind {
            // Dir mode: descend into directories; files are inert.
            PickKind::Dir => {
                if is_dir {
                    self.populate(path);
                    self.sel = 0;
                }
                None
            }
            // File mode: descend dirs, confirm files; Save overwrites the file picked.
            PickKind::File | PickKind::Save => {
                if is_dir {
                    self.populate(path);
                    self.sel = 0;
                    None
                } else {
                    Some(PickResult::Confirm(path))
                }
            }
            // Any mode: a backup folder confirms; other dirs descend; files confirm.
            PickKind::Any => {
                if is_dir && !is_backup {
                    self.populate(path);
                    self.sel = 0;
                    None
                } else {
                    Some(PickResult::Confirm(path))
                }
            }
        }
    }

    /// Create the pending new folder inside the current browse directory, then
    /// refresh and select it.
    fn create_folder(&mut self) {
        let name = self.new_folder.take().unwrap_or_default();
        let name = name.trim();
        if name.is_empty() {
            return;
        }
        let target = self.dir.join(name);
        match std::fs::create_dir(&target) {
            Ok(()) => {
                let dir = self.dir.clone();
                self.populate(dir);
                if let Some(idx) = self.entries.iter().position(|e| e.path == target) {
                    self.sel = idx;
                }
            }
            Err(e) => self.error = Some(format!("create folder: {e}")),
        }
    }

    fn draw(&self, frame: &mut Frame, area: Rect, pal: Palette, border: BorderSet) {
        if self.browse {
            self.draw_browse(frame, area, pal, border);
        } else {
            self.draw_input(frame, area, pal, border);
        }
    }

    fn draw_input(&self, frame: &mut Frame, area: Rect, pal: Palette, border: BorderSet) {
        let list_rows = self.recent.len().min(8) as u16;
        let has_recent = !self.recent.is_empty();
        // Content lines: Path + error + hint (3), plus "Recent:" header + list
        // when a recent group is shown. Borders add 2.
        let body_h = if has_recent { 4 + list_rows } else { 3 };
        let popup = centered_rect(70, body_h + 2, area);

        let mut lines = vec![
            Line::from(vec![
                Span::styled("Path: ", pal.accent()),
                Span::raw(self.input.clone()),
                Span::styled(" ", pal.accent().add_modifier(Modifier::REVERSED)),
            ]),
            if let Some(err) = &self.error {
                Line::styled(format!("  {err}"), pal.warn())
            } else {
                Line::raw("")
            },
        ];
        if has_recent {
            lines.push(Line::styled("Recent:", pal.dim()));
            for (i, p) in self.recent.iter().take(8).enumerate() {
                let marker = if i == self.recent_sel { "> " } else { "  " };
                let style = if i == self.recent_sel {
                    Style::default().add_modifier(Modifier::REVERSED | Modifier::BOLD)
                } else {
                    Style::default()
                };
                lines.push(Line::styled(format!("{marker}{}", basename(p)), style));
            }
        }
        lines.push(Line::styled(
            "Enter confirm   Tab browse   Esc cancel",
            pal.dim(),
        ));

        frame.render_widget(Clear, popup);
        frame.render_widget(
            Paragraph::new(Text::from(lines)).block(pane_block_with(&self.title, pal, border)),
            popup,
        );
    }

    fn draw_browse(&self, frame: &mut Frame, area: Rect, pal: Palette, border: BorderSet) {
        let popup = centered_rect(72, area.height.saturating_sub(4).clamp(8, 20), area);
        let visible = popup.height.saturating_sub(4) as usize;
        let start = self.sel.saturating_sub(visible.saturating_sub(1));

        let mut lines = vec![Line::styled(
            format!("Dir: {}", self.dir.display()),
            pal.accent().add_modifier(Modifier::BOLD),
        )];
        if let Some(err) = &self.error {
            lines.push(Line::styled(format!("  {err}"), pal.warn()));
        }
        if self.entries.is_empty() {
            lines.push(Line::styled("  (empty)", pal.dim()));
        } else {
            for (i, e) in self.entries.iter().enumerate().skip(start).take(visible) {
                let label = if e.select_current {
                    e.name.clone()
                } else if e.is_backup {
                    format!("[backup] {}", e.name)
                } else if e.is_dir {
                    format!("{}/", e.name)
                } else {
                    e.name.clone()
                };
                let marker = if i == self.sel { "> " } else { "  " };
                let base = if e.is_dir {
                    pal.accent()
                } else {
                    Style::default()
                };
                let style = if i == self.sel {
                    base.add_modifier(Modifier::REVERSED | Modifier::BOLD)
                } else {
                    base
                };
                lines.push(Line::styled(format!("{marker}{label}"), style));
            }
        }
        let hint = if self.kind == PickKind::Dir {
            "Enter open/pick   n new folder   Bksp up   Tab back   Esc cancel"
        } else {
            "Enter open/pick   Bksp up   Up/Dn move   Tab back   Esc cancel"
        };
        lines.push(Line::styled(hint, pal.dim()));

        frame.render_widget(Clear, popup);
        frame.render_widget(
            Paragraph::new(Text::from(lines)).block(pane_block_with("Browse", pal, border)),
            popup,
        );

        // New-folder name prompt.
        if let Some(name) = &self.new_folder {
            let np = centered_rect(52, 5, area);
            frame.render_widget(Clear, np);
            frame.render_widget(
                Paragraph::new(Text::from(vec![
                    Line::raw(""),
                    Line::from(vec![
                        Span::styled("  Name: ", pal.accent()),
                        Span::raw(name.clone()),
                        Span::styled(" ", pal.accent().add_modifier(Modifier::REVERSED)),
                    ]),
                    Line::styled("  Enter create   Esc cancel", pal.dim()),
                ]))
                .block(pane_block_with("New folder", pal, border)),
                np,
            );
        }
    }
}

/// ASCII fallback border set (`+ - |`) for terminals that can't be trusted
/// with Unicode box-drawing glyphs — a serial console, the MiSTer framebuffer,
/// an old vt. Capable terminals get [`border::ROUNDED`] instead; see
/// [`choose_border_set`].
const ASCII_BORDER: border::Set = border::Set {
    top_left: "+",
    top_right: "+",
    bottom_left: "+",
    bottom_right: "+",
    vertical_left: "|",
    vertical_right: "|",
    horizontal_top: "-",
    horizontal_bottom: "-",
};

/// Below this the layout can't render usefully, so we show a resize hint
/// instead (the conventional minimum-terminal guard).
const MIN_WIDTH: u16 = 64;
const MIN_HEIGHT: u16 = 18;

/// Pick the border glyphs by terminal capability — the "design in layers,
/// degrade gracefully" convention. Modern UTF-8 terminals get clean rounded
/// box-drawing (as lazygit / k9s / htop use); terminals we can't trust with
/// UTF-8 fall back to [`ASCII_BORDER`]. `RB_TUI_ASCII=1` forces the fallback.
fn choose_border_set() -> BorderSet {
    if std::env::var_os("RB_TUI_ASCII").is_some() {
        return ASCII_BORDER;
    }
    if locale_is_utf8() {
        border::ROUNDED
    } else {
        ASCII_BORDER
    }
}

/// True if the effective POSIX locale advertises UTF-8. Honors the standard
/// precedence: the first of `LC_ALL` / `LC_CTYPE` / `LANG` that is set and
/// non-empty decides (so `LC_ALL=C` correctly overrides a UTF-8 `LANG`).
fn locale_is_utf8() -> bool {
    for key in ["LC_ALL", "LC_CTYPE", "LANG"] {
        if let Some(val) = std::env::var_os(key) {
            let val = val.to_string_lossy();
            if val.is_empty() {
                continue;
            }
            let val = val.to_ascii_lowercase();
            return val.contains("utf-8") || val.contains("utf8");
        }
    }
    false
}

/// crossterm writes every colour as `38;5;N`, which an 8/16-colour terminal cannot parse at all.
///
/// Keyed on the OS as well as `$TERM`: no terminal shipped for Mac OS X 10.5 or earlier does 256
/// colours, and third-party ones (iTerm) report a `$TERM` no blocklist can enumerate.
fn low_color_terminal() -> bool {
    const LOW: [&str; 5] = ["xterm-color", "vt100", "vt102", "ansi", "dumb"];
    if std::env::var("TERM").map(|t| LOW.contains(&t.as_str())) == Ok(true) {
        return true;
    }
    // Only on a version we positively read: an unknown one keeps colour rather than dropping it.
    cfg!(target_vendor = "apple") && crate::controller::app::host_known_older_than(10, 6)
}

/// Semantic 16-color palette. Colors *reinforce* meaning already carried by
/// layout and reversed video; they never carry it alone. Honors `$NO_COLOR` by
/// collapsing every slot to the terminal default.
#[derive(Clone, Copy)]
struct Palette {
    color: bool,
}

impl Palette {
    fn detect() -> Self {
        Palette {
            color: std::env::var_os("NO_COLOR").is_none() && !low_color_terminal(),
        }
    }
    fn styled(self, c: Color) -> Style {
        if self.color {
            Style::default().fg(c)
        } else {
            Style::default()
        }
    }
    fn accent(self) -> Style {
        self.styled(Color::Cyan)
    }
    fn dim(self) -> Style {
        self.styled(Color::DarkGray)
    }
    fn warn(self) -> Style {
        self.styled(Color::Yellow)
    }
}

/// Top-level tabs, mirroring the desktop GUI (the first five) plus Commander,
/// Bulk, and Settings promoted to their own tabs.
#[derive(Clone, Copy, PartialEq, Eq)]
enum TabId {
    Backup,
    Restore,
    Inspect,
    NewDisk,
    Optical,
    Archives,
    Commander,
    Bulk,
    Settings,
}

const TABS: &[(TabId, &str)] = &[
    (TabId::Backup, "Backup"),
    (TabId::Restore, "Restore"),
    (TabId::Inspect, "Inspect"),
    (TabId::NewDisk, "New Disk"),
    (TabId::Optical, "Optical"),
    (TabId::Archives, "Archives"),
    (TabId::Commander, "Commander"),
    (TabId::Bulk, "Bulk"),
    (TabId::Settings, "Settings"),
];

/// Inspect is the GUI's default tab; open on it too.
const DEFAULT_TAB: usize = 2;

// ---------------------------------------------------------------- key hints --
//
// One table per screen, and both the footer and the `?` overlay render from it.
// They used to be written out by hand in each draw fn, which is how `E` (edit)
// came to exist in the key handler and nowhere on screen at all: there was no
// place that had to list it. A binding added here shows up in both, or in
// neither - it cannot show up in one.

/// One key binding, as shown in the footer strip and the `?` window.
struct KeyHint {
    /// Printable key, e.g. `"d"`, `"F2"`, `"Up/Down"`.
    keys: &'static str,
    /// What it does, phrased to fit a footer strip.
    desc: &'static str,
}

const fn hint(keys: &'static str, desc: &'static str) -> KeyHint {
    KeyHint { keys, desc }
}

/// Keys that work on every screen. Appended to each context in the `?` window,
/// and deliberately left out of the footer strip, which has no room to repeat
/// them on every screen.
const HINTS_GLOBAL: &[KeyHint] = &[
    hint("?", "This key list  (also F1)"),
    hint(":", "Command palette - run any rb-cli verb"),
    hint("q", "Quit"),
    hint("Ctrl-C", "Quit immediately"),
];

const HINTS_EDITOR: &[KeyHint] = &[
    hint("F2", "Save"),
    hint("F3", "Line endings (LF/CRLF/CR)"),
    hint("Esc", "Close  (prompts if modified)"),
    hint("Arrows", "Move cursor"),
    hint("PgUp/PgDn", "Page up / down"),
    hint("Home/End", "Start / end of line"),
    hint("Enter", "New line"),
    hint("Tab", "Insert four spaces"),
    hint("Backspace", "Delete back  (Del deletes forward)"),
];

const HINTS_INSPECT_DISKS: &[KeyHint] = &[
    hint("Up/Down", "Move  (also k/j)"),
    hint("Enter", "Open the disk's partition table and tools"),
    hint("o", "Open an image file, backup folder or Clonezilla image"),
    hint("R", "Open a remote image (an rb-cli serve daemon)"),
    hint("r", "Rescan disks"),
    hint("g/G", "Top / bottom  (also Home/End)"),
];

const HINTS_TAB: &[KeyHint] = &[
    hint("Left/Right", "Previous / next tab  (also h/l, Tab)"),
    hint("1-9", "Jump to a tab by number"),
    hint("Up/Down", "Move selection / scroll  (also k/j)"),
    hint("g/G", "Top / bottom  (also Home/End)"),
    hint("Enter", "Open / activate"),
    hint("Esc", "Back, or cancel a running task"),
];

/// Render hints as a single footer strip, clipped to `width`.
///
/// Drops whole hints rather than cutting one mid-word, and appends `?` as the
/// last thing standing so there is always a route to the full list even on a
/// narrow terminal.
fn footer_strip(hints: &[KeyHint], width: usize) -> String {
    const TAIL: &str = "  ?=keys";
    let budget = width.saturating_sub(TAIL.len() + 1);
    let mut out = String::new();
    for h in hints {
        let piece = format!(" {} {} ", h.keys, h.desc);
        if out.chars().count() + piece.chars().count() > budget {
            break;
        }
        out.push_str(&piece);
    }
    out.push_str(TAIL);
    out
}

/// Entry point for the `tui` verb. Guards the terminal, runs the event loop,
/// and always restores the terminal (ratatui installs a panic hook that does
/// the same on an unwind).
pub fn run() -> Result<()> {
    run_on(DEFAULT_TAB, "rb-cli tui")
}

/// Launch the TUI opened on a specific tab. `menu` uses this to absorb the old
/// appliance verb: `rb-cli menu` opens the full TUI on the Backup tab (the
/// appliance's backup/restore focus) instead of running a separate screen.
/// `label` names the entry verb for the interactive-TTY guard message.
pub fn run_on(initial_tab: usize, label: &'static str) -> Result<()> {
    crate::cli::tui::require_interactive_tty(
        label,
        "run it directly in a terminal, not from a pipe or CI",
    )?;

    let mut terminal = ratatui::init();
    // An old Terminal.app may not give a blank alternate screen, and ratatui only redraws changed cells.
    let _ = terminal.clear();
    let mut app = App::new_on(initial_tab);
    app.start_update_check();
    let outcome = app.run(&mut terminal);
    ratatui::restore();
    outcome
}

/// Tab index of the Backup screen (used by the `menu` alias entry).
pub const BACKUP_TAB: usize = 0;

/// The New Disk creation wizard: a three-step form (media class → filesystem →
/// path/size/name) that creates a blank image through the shared `new` verb
/// (`crate::cli::verbs::new::run`), so every filesystem funnels through the same
/// validation and formatter the CLI uses. HFV/HD/CD-ROM caveats mirror `new.rs`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum WizStep {
    Class,
    Fs,
    Details,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DiskClass {
    Floppy,
    Volume,
    Hd,
    Cdrom,
}

/// The media classes the wizard can create end-to-end. The hard-disk and
/// CD-ROM targets build with their defaults (path + size is enough); their
/// donor / custom partition options — `--system-disk`, `--boot-sector-donor`,
/// `--partitions`, EFS inode density — remain CLI-only.
const NEW_CLASSES: &[(&str, DiskClass, &str)] = &[
    (
        "Floppy",
        DiskClass::Floppy,
        "Bare floppy-geometry volume: FAT / HFS and the fixed-geometry retro filesystems.",
    ),
    (
        "Volume",
        DiskClass::Volume,
        "Bare single volume of any size (a superfloppy): NTFS, ext, HFS+, EFS, ...",
    ),
    (
        "Hard disk",
        DiskClass::Hd,
        "Partition-table-wrapped, self-bootable HDD image (Sharp X68000, SGI IRIX).",
    ),
    (
        "CD-ROM",
        DiskClass::Cdrom,
        "Bootable-media disc image (.iso). Optionally filled from a host folder.",
    ),
];

/// Disc targets offered under the "CD-ROM" class. Each maps to one
/// `optical new <target>` verb.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum CdTarget {
    SgiEfs,
    MacHfs,
    MacHfsPlus,
    NextUfs,
}

const CD_TARGETS: &[(&str, CdTarget)] = &[
    ("SGI IRIX (EFS CD, slot-7 SYSV)", CdTarget::SgiEfs),
    ("Classic Mac (APM + HFS)", CdTarget::MacHfs),
    ("Mac OS 8.1+ (APM + HFS+)", CdTarget::MacHfsPlus),
    ("NeXTSTEP (NeXT label + 4.3BSD UFS)", CdTarget::NextUfs),
];

/// Bootable hard-disk platforms offered under the "Hard disk" class. Each
/// builds with its defaults; donor-cloning and custom partition layouts stay on
/// the CLI (`rb-cli new hd ...`).
#[derive(Clone, Copy, PartialEq, Eq)]
enum HdPlatform {
    X68k,
    SgiEfs,
    /// Generic tables. The wizard builds one partition filling the disk;
    /// multi-partition layouts stay on the CLI, which takes `--partition`.
    Mbr,
    Gpt,
    Apm,
    /// Partition table only — no EFS format, no IPL stub, unlike the two above.
    SgiTable,
    X68kTable,
    Rdb,
    Sun,
    Atari,
}

/// Wrap the shared args with the default cylinder geometry, for the two
/// tables whose partitions land on cylinder boundaries.
fn cylinder_args(
    common: crate::cli::verbs::new_partitioned_hd::PartitionedHdArgs,
) -> crate::cli::verbs::new_partitioned_hd::CylinderHdArgs {
    crate::cli::verbs::new_partitioned_hd::CylinderHdArgs {
        common,
        heads: crate::controller::new_disk::DEFAULT_HEADS,
        sectors: crate::controller::new_disk::DEFAULT_SECTORS_PER_TRACK,
    }
}

const HD_PLATFORMS: &[(&str, HdPlatform)] = &[
    ("Sharp X68000 (Human68k)", HdPlatform::X68k),
    ("SGI IRIX (EFS, dvh)", HdPlatform::SgiEfs),
    ("MBR (DOS / PC), one partition", HdPlatform::Mbr),
    ("GPT (UEFI), one partition", HdPlatform::Gpt),
    ("APM (Apple / PowerPC), one partition", HdPlatform::Apm),
    (
        "SGI volume header (IRIX), one partition",
        HdPlatform::SgiTable,
    ),
    ("X68000 table only, one partition", HdPlatform::X68kTable),
    ("Amiga RDB, one partition", HdPlatform::Rdb),
    ("Sun disk label (SPARC), one slice", HdPlatform::Sun),
    ("Atari ST AHDI, one partition", HdPlatform::Atari),
];

/// Filesystems offered under `new floppy` in the wizard. CP/M is omitted here
/// because it requires a `--cpm-preset` (use the CLI: `rb-cli new floppy cpm`).
const FLOPPY_FS: &[(&str, FloppyFs)] = &[
    ("FAT12 / FAT16", FloppyFs::Fat),
    ("HFS (Mac OS Standard)", FloppyFs::Hfs),
    ("Atari DOS 2.0S", FloppyFs::Atari),
    ("Apple DOS 3.3", FloppyFs::AppleDos),
    ("OS-9 / NitrOS-9", FloppyFs::Os9),
    ("UCSD p-System", FloppyFs::Ucsd),
    ("TR-DOS (ZX Spectrum)", FloppyFs::Trdos),
    ("TI-99/4A", FloppyFs::Ti99),
    ("MFS (Mac, pre-HFS)", FloppyFs::Mfs),
    ("Acorn ADFS (E-format)", FloppyFs::Adfs),
    ("Minix V1", FloppyFs::Minix),
];

/// Filesystems offered under `new volume` in the wizard.
const VOLUME_FS: &[(&str, VolumeFs)] = &[
    ("HFS (Mac OS Standard)", VolumeFs::Hfs),
    ("HFS+ / HFSX", VolumeFs::Hfsplus),
    ("HFV (BasiliskII, <= 2047 MB)", VolumeFs::Hfv),
    ("FAT12 / 16 / 32", VolumeFs::Fat),
    ("NTFS", VolumeFs::Ntfs),
    ("ext2", VolumeFs::Ext),
    ("ext3", VolumeFs::Ext3),
    ("ext4", VolumeFs::Ext4),
    ("Amiga FFS / OFS", VolumeFs::Affs),
    ("ProDOS (<= ~32 MiB)", VolumeFs::Prodos),
    ("IRIX EFS", VolumeFs::Efs),
    ("Minix V2", VolumeFs::Minix2),
    ("Minix V3", VolumeFs::Minix3),
];

/// The details form's text fields: path, size, name — plus, for a CD-ROM, an
/// optional source folder and the expand-archives toggle. See
/// [`NewWizard::field_count`].
const WIZ_FIELDS: usize = 3;

/// Index of the expand-archives toggle on the CD-ROM details form. The only
/// non-text row, so the key handler special-cases it.
const WIZ_FIELD_EXPAND: usize = 4;

/// Index of the expand-gunzip toggle, a row only the NeXT target has (`--expand-gunzip`).
const WIZ_FIELD_GUNZIP: usize = 5;

struct NewWizard {
    step: WizStep,
    class_sel: usize,
    fs_sel: usize,
    /// Details form: 0 = path, 1 = size, 2 = name, 3 = source folder (CD only),
    /// 4 = expand-archives toggle (CD only).
    field: usize,
    path: String,
    size: String,
    name: String,
    /// Host folder to fill a CD-ROM from, empty for a blank disc. Only
    /// reachable on the CD-ROM class; `rb-cli optical new sgi-efs --from-dir`.
    from_dir: String,
    /// Unpack tar / classic-Mac archives found in [`Self::from_dir`] instead of
    /// copying them in packed; `--expand-archives` on either CD verb. Also
    /// changes what `--size auto` measures (what the archives unpack to).
    expand_archives: bool,
    /// Strip only the gzip layer of `.gz` files in [`Self::from_dir`]; `--expand-gunzip` (NeXT only).
    expand_gunzip: bool,
    /// Tab-to-browse picker for the path field.
    picker: Option<FilePicker>,
    /// Result of the last create attempt (success or error text).
    status: Option<String>,
    is_error: bool,
}

impl Default for NewWizard {
    fn default() -> Self {
        NewWizard {
            step: WizStep::Class,
            class_sel: 0,
            fs_sel: 0,
            field: 0,
            path: String::new(),
            size: "800K".to_string(),
            name: "rusty-backup".to_string(),
            from_dir: String::new(),
            expand_archives: false,
            expand_gunzip: false,
            picker: None,
            status: None,
            is_error: false,
        }
    }
}

impl NewWizard {
    fn class(&self) -> DiskClass {
        NEW_CLASSES[self.class_sel.min(NEW_CLASSES.len() - 1)].1
    }
    fn class_label(&self) -> &'static str {
        NEW_CLASSES[self.class_sel.min(NEW_CLASSES.len() - 1)].0
    }
    /// Re-seed the size field with a sensible default for the selected class,
    /// but only while it still holds another class's default — so a size the
    /// user typed is never clobbered by arrowing through the class list.
    fn seed_size_for_class(&mut self) {
        const DEFAULTS: [&str; 4] = ["800K", "64M", "32M", "600M"];
        let cur = self.size.trim();
        // `auto` is a size the CD class can produce for itself, so treat it as
        // a default too — otherwise arrowing off CD-ROM and back would leave a
        // stale `auto` on a class that can't use it.
        if cur.is_empty() || cur.eq_ignore_ascii_case("auto") || DEFAULTS.contains(&cur) {
            self.size = match self.class() {
                DiskClass::Floppy => DEFAULTS[0],
                DiskClass::Volume => DEFAULTS[1],
                DiskClass::Hd => DEFAULTS[2],
                DiskClass::Cdrom => DEFAULTS[3],
            }
            .to_string();
        }
    }

    /// Rows on the details form. The CD-ROM class adds the optional
    /// source-folder row and the expand-archives toggle; every other class
    /// stops at name.
    fn field_count(&self) -> usize {
        match self.class() {
            DiskClass::Cdrom if self.cd_target() == CdTarget::NextUfs => WIZ_FIELD_GUNZIP + 1,
            DiskClass::Cdrom => WIZ_FIELD_EXPAND + 1,
            _ => WIZ_FIELDS,
        }
    }

    fn cd_target(&self) -> CdTarget {
        CD_TARGETS[self.fs_sel.min(CD_TARGETS.len() - 1)].1
    }

    fn fs_count(&self) -> usize {
        match self.class() {
            DiskClass::Floppy => FLOPPY_FS.len(),
            DiskClass::Volume => VOLUME_FS.len(),
            // The "filesystem" step picks the target platform / disc layout for
            // a hard disk or a CD.
            DiskClass::Hd => HD_PLATFORMS.len(),
            DiskClass::Cdrom => CD_TARGETS.len(),
        }
    }
    fn fs_label(&self, i: usize) -> &'static str {
        match self.class() {
            DiskClass::Floppy => FLOPPY_FS[i.min(FLOPPY_FS.len() - 1)].0,
            DiskClass::Volume => VOLUME_FS[i.min(VOLUME_FS.len() - 1)].0,
            DiskClass::Hd => HD_PLATFORMS[i.min(HD_PLATFORMS.len() - 1)].0,
            DiskClass::Cdrom => CD_TARGETS[i.min(CD_TARGETS.len() - 1)].0,
        }
    }
}

/// The Bulk convert screen: pick a source folder, choose a format, review /
/// un-check the scanned files, then convert them all via `bulk_convert_runner`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum BulkStep {
    Source,
    Config,
    Run,
}

/// Output formats offered for bulk convert (the general whole-disk formats;
/// CD/floppy-specific ones with input constraints are left to the CLI).
const BULK_FORMATS: &[(&str, crate::controller::bulk_convert::ExportFormat)] = {
    use crate::controller::bulk_convert::ExportFormat as F;
    &[
        ("Raw (.img)", F::Raw),
        ("VHD", F::Vhd),
        ("VHD Dynamic", F::VhdDynamic),
        ("QCOW2", F::Qcow2),
        ("VMDK Flat", F::VmdkFlat),
        ("VMDK Sparse", F::VmdkSparse),
        ("CHD (hard disk)", F::Chd),
        ("DVD CHD", F::ChdDvd),
    ]
};

/// The screen's widget state around a headless `BulkConvertController`.
struct BulkState {
    step: BulkStep,
    ctl: crate::controller::bulk_convert::BulkConvertController,
    source: String,
    out_dir: String,
    format_sel: usize,
    /// Flat cursor over the config rows: 0 = Format, 1 = Output, then one row per
    /// scanned file, and a final "Start" row.
    field: usize,
    picker: Option<FilePicker>,
    picker_target: bool,
    op: String,
    result: Option<String>,
    is_error: bool,
    /// A conversion was started and its result is not shown yet.
    awaiting: bool,
}

impl Default for BulkState {
    fn default() -> Self {
        use crate::controller::bulk_convert::{BulkConvertAction, BulkConvertController, NoNotify};
        use crate::controller::Controller as _;
        let mut ctl = BulkConvertController::new(Arc::new(NoNotify));
        ctl.apply(BulkConvertAction::Open);
        ctl.apply(BulkConvertAction::SetFormat(BULK_FORMATS[0].1));
        BulkState {
            step: BulkStep::Source,
            ctl,
            source: String::new(),
            out_dir: String::new(),
            format_sel: 0,
            field: 0,
            picker: None,
            picker_target: false,
            op: String::new(),
            result: None,
            is_error: false,
            awaiting: false,
        }
    }
}

impl BulkState {
    fn files(&self) -> &[crate::controller::bulk_convert::ScannedFile] {
        self.ctl.files()
    }
    fn start_row(&self) -> usize {
        2 + self.files().len()
    }
    /// The file index at the current cursor, if the cursor is on a file row.
    fn file_at_cursor(&self) -> Option<usize> {
        if self.field >= 2 && self.field < 2 + self.files().len() {
            Some(self.field - 2)
        } else {
            None
        }
    }
    fn act(&mut self, action: crate::controller::bulk_convert::BulkConvertAction) {
        use crate::controller::Controller as _;
        let _ = self.ctl.apply(action);
    }
    /// Hand the typed output folder to the controller (the source when it is blank, for a scan).
    fn send_output(&mut self, for_scan: bool) {
        use crate::controller::bulk_convert::{purpose, BulkConvertAction};
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
}

/// What a finished [`TuiTask`] does to the app, run on the UI thread.
type TaskDone = Box<dyn FnOnce(&mut App) + Send>;

/// A slow TUI operation off the UI thread; the screen keeps drawing and the result lands in `tick`.
type TuiTask = crate::controller::task::ShellTask<TaskDone>;

struct App {
    palette: Palette,
    border: BorderSet,
    /// Whether we're running with elevated privileges (root / admin). Device
    /// operations (raw backup / restore / write) require it.
    elevated: bool,
    active: usize,
    scroll: u16,
    /// Cursor index into the active tab's selectable rows (Inspect disk list).
    selection: usize,
    /// The Inspect screen: the open source, its grid and tools (lazily created on first visit).
    inspect: Option<InspectState>,
    /// The shared "Open file / backup" picker (path + recent + Tab-browse).
    open_picker: Option<FilePicker>,
    /// The per-partition filesystem Explorer overlay, when open.
    explorer: Option<commander::CommanderView>,
    /// Transient status/error line shown on the Inspect screen.
    status: Option<String>,
    /// A running progress task (the preview / future backup-restore-convert).
    progress: Option<Progress>,
    /// The app shell's rules: the device list (`shell.devices()`) and its refresh job.
    shell: crate::controller::app::AppController,
    /// The device list is listed when the Inspect or Backup tab is first shown.
    devices_loaded: bool,
    /// `r` started a device refresh whose end the status line reports.
    refreshing_devices: bool,
    /// The New Disk creation wizard state (lazily created on first visit).
    newdisk: Option<NewWizard>,
    /// The Backup screen state (lazily created on first visit).
    backup: Option<BackupState>,
    /// The Restore screen state (lazily created on first visit).
    restore: Option<RestoreState>,
    /// The Bulk convert screen state (lazily created on first visit).
    bulk: Option<BulkState>,
    /// The Optical screen state (lazily created on first visit).
    #[cfg(feature = "optical")]
    optical: Option<OpticalState>,
    /// The Archives screen state (lazily created on first visit).
    archive: Option<ArchiveState>,
    /// The Commander dual-pane state (lazily created on first visit).
    commander: Option<commander::CommanderView>,
    /// The tab Commander's Close returns to; `None` when the TUI started on Commander.
    tab_before_commander: Option<usize>,
    /// The Settings tab state (lazily loaded on first visit).
    settings: Option<SettingsState>,
    /// The `:` command-palette input line being typed, if open.
    palette_input: Option<String>,
    /// A command-palette line ready to run (the run loop suspends the TUI,
    /// executes it, and re-enters).
    pending_palette: Option<String>,
    show_help: bool,
    /// First visible row of the `?` window. The list is per-screen and the
    /// Explorer's is longer than a short terminal, so it scrolls rather than
    /// silently stopping at the bottom edge.
    help_scroll: usize,
    should_quit: bool,
    /// A slow operation running as a job behind a "Working" box; keys wait until it ends.
    task: TuiTask,
}

impl App {
    fn new_on(initial_tab: usize) -> Self {
        let notify = Arc::new(crate::controller::task::NoNotify);
        Self::with_shell(
            initial_tab,
            crate::controller::app::AppController::new(notify),
        )
    }

    /// For tests: recent files and other config kept in memory.
    #[cfg(test)]
    fn headless_on(initial_tab: usize) -> Self {
        Self::with_shell(
            initial_tab,
            crate::controller::app::AppController::headless(),
        )
    }

    fn with_shell(initial_tab: usize, shell: crate::controller::app::AppController) -> Self {
        let active = initial_tab.min(TABS.len() - 1);
        let mut app = App {
            palette: Palette::detect(),
            border: choose_border_set(),
            elevated: crate::controller::app::running_elevated(),
            active,
            scroll: 0,
            selection: 0,
            inspect: None,
            open_picker: None,
            explorer: None,
            status: None,
            progress: None,
            shell,
            devices_loaded: false,
            refreshing_devices: false,
            newdisk: None,
            backup: None,
            restore: None,
            bulk: None,
            #[cfg(feature = "optical")]
            optical: None,
            archive: None,
            commander: None,
            tab_before_commander: None,
            settings: None,
            palette_input: None,
            pending_palette: None,
            show_help: false,
            help_scroll: 0,
            should_quit: false,
            task: TuiTask::new(Arc::new(crate::controller::task::NoNotify)),
        };
        app.on_tab_changed();
        app
    }

    /// An Optical job runs whose end the screen shows.
    fn optical_busy(&self) -> bool {
        #[cfg(feature = "optical")]
        {
            self.optical.as_ref().is_some_and(|o| o.busy())
        }
        #[cfg(not(feature = "optical"))]
        {
            false
        }
    }

    /// Short privilege label for the status line.
    fn privilege_label(&self) -> &'static str {
        if !self.elevated {
            "user"
        } else if cfg!(windows) {
            "admin"
        } else {
            "root"
        }
    }

    /// A yellow caution line for tabs whose real operations touch a physical
    /// disk, shown only when we're not elevated (so those actions will be
    /// disabled once wired up). `None` when elevated.
    fn device_note(&self) -> Option<Line<'static>> {
        if self.elevated {
            None
        } else {
            Some(Line::styled(
                "Note: operations on a physical disk are disabled - re-run elevated \
                 (sudo / Run as administrator).",
                self.palette.warn(),
            ))
        }
    }

    fn run(mut self, terminal: &mut DefaultTerminal) -> Result<()> {
        while !self.should_quit {
            terminal.draw(|frame| self.draw(frame))?;
            self.handle_events()?;
            if let Some(cmd) = self.pending_palette.take() {
                self.run_palette(terminal, &cmd)?;
            }
        }
        Ok(())
    }

    /// Suspend the alt-screen, run a `rb-cli` verb line to the real terminal,
    /// wait for a keypress, then re-enter the TUI. This is the command palette's
    /// escape hatch to any flat CLI verb.
    fn run_palette(&mut self, terminal: &mut DefaultTerminal, input: &str) -> Result<()> {
        use crossterm::terminal::{
            disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
        };
        let _ = disable_raw_mode();
        let _ = crossterm::execute!(std::io::stdout(), LeaveAlternateScreen);
        println!("$ rb-cli {input}\n");
        match run_palette_command(input) {
            Ok(()) => {}
            Err(e) => eprintln!("error: {e:#}"),
        }
        println!("\n[Press Enter to return to the TUI]");
        let mut line = String::new();
        let _ = std::io::stdin().read_line(&mut line);
        let _ = enable_raw_mode();
        let _ = crossterm::execute!(std::io::stdout(), EnterAlternateScreen);
        let _ = terminal.clear();
        Ok(())
    }

    fn current(&self) -> TabId {
        TABS[self.active].0
    }

    // --- input -----------------------------------------------------------

    fn handle_events(&mut self) -> Result<()> {
        // Advance progress every tick, even when no key arrives, so the bar and
        // its rate / ETA animate.
        self.tick();
        // Commander jobs finish without a keypress, so poll faster while one runs.
        let working = self.commander.as_ref().is_some_and(|c| c.is_working())
            || self.explorer.as_ref().is_some_and(|c| c.is_working())
            || self.inspect.as_ref().is_some_and(|s| s.busy())
            || self.optical_busy();
        let wait = if working { 50 } else { 200 };
        if !event::poll(Duration::from_millis(wait))? {
            return Ok(());
        }
        let Event::Key(key) = event::read()? else {
            return Ok(());
        };
        if key.kind != KeyEventKind::Press {
            return Ok(());
        }
        self.on_key(key);
        Ok(())
    }

    /// One key press, routed to whatever owns the keyboard.
    fn on_key(&mut self, key: crossterm::event::KeyEvent) {
        self.on_key_inner(key);
        self.leave_commander_if_closed();
    }

    fn on_key_inner(&mut self, key: crossterm::event::KeyEvent) {
        if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
            self.should_quit = true;
            return;
        }
        // A running task owns the keyboard: Esc asks it to stop, everything else waits.
        if self.task.is_active() {
            if key.code == KeyCode::Esc {
                self.task.cancel();
            }
            return;
        }

        // The help overlay is modal. It scrolls: the Explorer's list is longer
        // than a short terminal can show, and a list that silently stops at the
        // bottom edge is the same discoverability bug this window exists to fix.
        if self.show_help {
            match key.code {
                KeyCode::Esc | KeyCode::Char('?') | KeyCode::Enter | KeyCode::F(1) => {
                    self.show_help = false;
                    self.help_scroll = 0;
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    self.help_scroll = self.help_scroll.saturating_sub(1)
                }
                KeyCode::Down | KeyCode::Char('j') => self.help_scroll += 1,
                KeyCode::PageUp => self.help_scroll = self.help_scroll.saturating_sub(10),
                KeyCode::PageDown => self.help_scroll += 10,
                KeyCode::Home => self.help_scroll = 0,
                // Clamped against the real row count when drawn, so overshooting
                // here just parks at the bottom.
                KeyCode::End => self.help_scroll = usize::MAX / 2,
                _ => {}
            }
            return;
        }

        // The `:` command palette input is modal while open.
        if self.palette_input.is_some() {
            match key.code {
                KeyCode::Enter => {
                    let cmd = self.palette_input.take().unwrap_or_default();
                    if !cmd.trim().is_empty() {
                        self.pending_palette = Some(cmd);
                    }
                }
                KeyCode::Esc => self.palette_input = None,
                KeyCode::Backspace => {
                    if let Some(s) = self.palette_input.as_mut() {
                        s.pop();
                    }
                }
                KeyCode::Char(c) if !c.is_control() => {
                    if let Some(s) = self.palette_input.as_mut() {
                        s.push(c);
                    }
                }
                _ => {}
            }
            return;
        }

        // The shared Open picker (path + recent + Tab-browse).
        if self.open_picker.is_some() {
            match self.open_picker.as_mut().unwrap().handle_key(key.code) {
                Some(PickResult::Cancel) => self.open_picker = None,
                Some(PickResult::Confirm(path)) => {
                    self.open_picker = None;
                    self.open_target(path);
                }
                None => {}
            }
            return;
        }

        // F12 hides the update banner wherever the keyboard is.
        if key.code == KeyCode::F(12) && self.update_banner().is_some() {
            use crate::controller::Controller as _;
            self.shell
                .apply(crate::controller::app::AppAction::DismissUpdate);
            return;
        }

        // The filesystem Explorer is a modal window over the Inspect tab.
        if let Some(ex) = self.explorer.as_mut() {
            // F1 inside the editor shows the editor's keys; `?` there is just a character.
            if key.code == KeyCode::F(1) && ex.editor_open() {
                self.show_help = true;
                self.help_scroll = 0;
                return;
            }
            if !ex.handle_key(key) {
                match key.code {
                    KeyCode::Char(':') => self.palette_input = Some(String::new()),
                    KeyCode::Char('q') => self.request_quit(),
                    _ => {}
                }
            }
            self.after_explorer();
            return;
        }

        // The Inspect screen owns the keys of an open source and of its windows.
        if self.current() == TabId::Inspect && self.handle_inspect_key(key.code) {
            return;
        }

        // The New Disk wizard owns most keys on its tab (Left/Right still switch
        // tabs; typed text in the details form is consumed here).
        if self.current() == TabId::NewDisk && self.handle_newdisk_key(key.code) {
            return;
        }

        // The Backup screen owns most keys on its tab.
        if self.current() == TabId::Backup && self.handle_backup_key(key.code) {
            return;
        }

        // The Restore screen owns most keys on its tab.
        if self.current() == TabId::Restore && self.handle_restore_key(key.code) {
            return;
        }

        // The Bulk convert screen owns most keys on its tab.
        if self.current() == TabId::Bulk && self.handle_bulk_key(key.code) {
            return;
        }

        // The Optical screen owns most keys on its tab.
        #[cfg(feature = "optical")]
        if self.current() == TabId::Optical && self.handle_optical_key(key.code) {
            return;
        }

        // The Archives screen owns most keys on its tab.
        if self.current() == TabId::Archives && self.handle_archive_key(key.code) {
            return;
        }

        // The Commander screen hands its keys to its controller; unbound ones fall through.
        if self.current() == TabId::Commander {
            if let Some(c) = self.commander.as_mut() {
                if c.handle_key(key) {
                    return;
                }
            }
        }

        // The Settings tab owns Up/Down/Enter (Left/Right still switch tabs).
        if self.current() == TabId::Settings && self.handle_settings_key(key.code) {
            return;
        }

        match key.code {
            KeyCode::Char('?') | KeyCode::F(1) => self.show_help = true,
            KeyCode::Char(':') => self.palette_input = Some(String::new()),
            KeyCode::Char('q') => self.request_quit(),
            KeyCode::Esc => self.on_back(),
            // Left / Right (and Tab, and vim h/l) change tabs ("windows").
            KeyCode::Tab | KeyCode::Right | KeyCode::Char('l') => self.switch_tab(1),
            KeyCode::BackTab | KeyCode::Left | KeyCode::Char('h') => self.switch_tab(-1),
            KeyCode::Char(c @ '1'..='9') => {
                let idx = (c as u8 - b'1') as usize;
                if idx < TABS.len() {
                    self.active = idx;
                    self.scroll = 0;
                    self.on_tab_changed();
                }
            }
            // Up / Down move the selection within the pane (or scroll it when
            // the pane has no selectable rows).
            KeyCode::Up | KeyCode::Char('k') => self.move_cursor(-1),
            KeyCode::Down | KeyCode::Char('j') => self.move_cursor(1),
            KeyCode::Char('g') | KeyCode::Home => {
                self.selection = 0;
                self.scroll = 0;
            }
            KeyCode::Char('G') | KeyCode::End => self.cursor_end(),
            KeyCode::Enter => self.activate(),
            // Open a file / backup (Inspect tab): the shared picker + MRU.
            KeyCode::Char('o') if self.current() == TabId::Inspect => {
                let recent = self.shell.recent(RecentMode::Inspect);
                self.open_picker =
                    Some(FilePicker::new(PickKind::Any, "Open file / backup").with_recent(recent));
            }
            // Rescan on the Inspect tab (the k9s `r`-to-refresh idiom).
            KeyCode::Char('r') if self.current() == TabId::Inspect => {
                use crate::controller::Controller as _;
                self.shell
                    .apply(crate::controller::app::AppAction::RefreshDevices);
                self.devices_loaded = true;
                self.refreshing_devices = true;
                self.status = Some("Looking for devices...".to_string());
                self.selection = 0;
            }
            _ => {}
        }
    }

    /// Open a picked path on the Inspect screen: a folder is a backup or Clonezilla image, a
    /// file an image, a listed disk its device.
    fn open_target(&mut self, path: std::path::PathBuf) {
        self.selection = 0;
        self.status = None;
        if path.exists() {
            // `exists()`, not `is_file()`: a raw device is neither a file nor a directory.
            self.inspect_open_path(path);
        } else {
            self.status = Some(format!("No such file or directory: {}", path.display()));
        }
    }

    /// Explain an open failure, naming elevation when that is the actual cause.
    /// A bare "Permission denied" on `/dev/sda` reads like a bug rather than the
    /// expected cost of raw device access.
    fn access_hint(what: &str, e: &std::io::Error) -> String {
        if e.kind() == std::io::ErrorKind::PermissionDenied {
            format!("{what}: {e} - raw device access needs elevation; re-run rb-cli tui elevated")
        } else {
            format!("{what}: {e}")
        }
    }

    fn switch_tab(&mut self, delta: isize) {
        let len = TABS.len() as isize;
        self.active = (((self.active as isize + delta) % len + len) % len) as usize;
        self.scroll = 0;
        self.on_tab_changed();
    }

    fn move_cursor(&mut self, delta: isize) {
        let rows = self.row_count();
        if rows > 0 {
            let max = rows as isize - 1;
            let next = (self.selection as isize + delta).clamp(0, max);
            self.selection = next as usize;
        } else if delta < 0 {
            self.scroll = self.scroll.saturating_sub(1);
        } else {
            let max = self.content_line_count().saturating_sub(1);
            self.scroll = (self.scroll + 1).min(max);
        }
    }

    fn cursor_end(&mut self) {
        let rows = self.row_count();
        if rows > 0 {
            self.selection = rows - 1;
        } else {
            self.scroll = self.content_line_count().saturating_sub(1);
        }
    }

    /// Enter: drill into the selected item / start the tab's action.
    fn activate(&mut self) {
        match self.current() {
            // Disk list: Enter opens the disk through the Inspect controller, its hardware
            // facts shown above its partition table (one Enter, one screen).
            TabId::Inspect if self.row_count() > 0 => {
                let slot = self.selection;
                self.selection = 0;
                self.inspect_open_device(slot);
            }
            _ => {}
        }
    }

    /// Esc: unwind one level — cancel a running task, then close a drill-down,
    /// then quit.
    /// Quit, unless Commander holds staged edits: then its controller asks first, on its tab.
    fn request_quit(&mut self) {
        if !self
            .commander
            .as_ref()
            .is_some_and(|c| c.has_staged_edits())
        {
            self.should_quit = true;
            return;
        }
        if let Some(idx) = TABS.iter().position(|t| t.0 == TabId::Commander) {
            if self.active != idx {
                self.active = idx;
                self.scroll = 0;
                self.on_tab_changed();
            }
        }
        if let Some(c) = self.commander.as_mut() {
            c.request_quit();
        }
    }

    /// Commander's Close: drop its controller and go back to the tab it was opened from.
    fn leave_commander_if_closed(&mut self) {
        if self.commander.as_ref().is_some_and(|c| c.quit()) {
            self.should_quit = true;
            return;
        }
        if !self.commander.as_ref().is_some_and(|c| c.closed()) {
            return;
        }
        self.commander = None;
        match self.tab_before_commander {
            Some(tab) => {
                self.active = tab;
                self.scroll = 0;
                self.on_tab_changed();
            }
            None => self.should_quit = true,
        }
    }

    fn on_back(&mut self) {
        if self.progress.is_some() {
            self.cancel_progress();
        } else {
            self.request_quit();
        }
    }

    fn cancel_progress(&mut self) {
        if let Some(p) = &self.progress {
            if let Ok(mut s) = p.shared.lock() {
                s.cancel = true;
            }
        }
        // Also signal a running backup / restore to stop.
        if let Some(b) = self.backup.as_mut() {
            use crate::controller::backup::BackupAction;
            if b.ctl.is_running() {
                b.act(BackupAction::Cancel);
                b.cancel_note = true;
            } else if b.ctl.start_pending() {
                b.act(BackupAction::CancelPendingStart);
            }
        }
        self.restore_cancel();
        if let Some(b) = self.bulk.as_mut() {
            if b.ctl.is_running() {
                b.act(crate::controller::bulk_convert::BulkConvertAction::Cancel);
            }
        }
        #[cfg(feature = "optical")]
        if let Some(o) = self.optical.as_mut() {
            if o.ctl.is_running() {
                o.act(crate::controller::optical::OpticalAction::Cancel);
            }
        }
        self.progress = None;
    }

    /// Land a finished device refresh on the status line.
    fn poll_devices(&mut self) {
        use crate::controller::Controller as _;
        let _ = self.shell.tick();
        if self.refreshing_devices && self.shell.devices_refreshing().is_none() {
            self.refreshing_devices = false;
            let n = self.shell.devices().len();
            self.selection = self.selection.min(n.saturating_sub(1));
            self.status = Some(format!("Refreshed: {n} device(s) found"));
        }
    }

    /// List the devices the first time a tab needs them (a plain listing; nothing elevates).
    fn load_devices_once(&mut self) {
        if !self.devices_loaded {
            self.shell.load_devices();
            self.devices_loaded = true;
        }
    }

    /// Tests: show `devices` as the device list.
    #[cfg(test)]
    fn set_test_devices(&mut self, devices: Vec<DiskDevice>) {
        self.shell.set_devices(devices);
        self.devices_loaded = true;
    }

    /// Lazily load anything a freshly-shown tab needs; reset per-tab cursor.
    fn on_tab_changed(&mut self) {
        self.selection = 0;
        self.open_picker = None;
        self.explorer = None;
        self.status = None;
        if self.current() == TabId::Inspect {
            self.load_devices_once();
            if self.inspect.is_none() {
                self.inspect = Some(InspectState::new(self.shell.recent_handle()));
            }
        }
        if self.current() == TabId::NewDisk && self.newdisk.is_none() {
            self.newdisk = Some(NewWizard::default());
        }
        if self.current() == TabId::Backup {
            if self.backup.is_none() {
                self.backup = Some(BackupState::new(self.shell.recent_handle()));
            }
            self.load_devices_once();
        }
        if self.current() == TabId::Restore && self.restore.is_none() {
            self.restore = Some(RestoreState::new(self.shell.recent_handle()));
        }
        if self.current() == TabId::Bulk && self.bulk.is_none() {
            self.bulk = Some(BulkState::default());
        }
        #[cfg(feature = "optical")]
        if self.current() == TabId::Optical && self.optical.is_none() {
            self.optical = Some(OpticalState::new(self.shell.recent_handle()));
        }
        if self.current() == TabId::Archives && self.archive.is_none() {
            self.archive = Some(ArchiveState::new(self.shell.recent_handle()));
        }
        if self.current() == TabId::Commander && self.commander.is_none() {
            self.commander = Some(commander::CommanderView::new());
        }
        if self.current() != TabId::Commander {
            self.tab_before_commander = Some(self.active);
        }
        if self.current() == TabId::Settings && self.settings.is_none() {
            self.settings = Some(SettingsState::open(self.shell.recent_handle()));
        }
    }

    /// New Disk wizard keys. Returns `true` when consumed; `false` lets the key
    /// fall through to the shell (so Left/Right still switch tabs, `q` still
    /// quits from the class/fs steps, etc.).
    fn handle_newdisk_key(&mut self, code: KeyCode) -> bool {
        if self.newdisk.is_none() {
            self.newdisk = Some(NewWizard::default());
        }
        // The path field's Tab-to-browse picker is modal while open.
        if self
            .newdisk
            .as_ref()
            .map(|w| w.picker.is_some())
            .unwrap_or(false)
        {
            let res = self
                .newdisk
                .as_mut()
                .unwrap()
                .picker
                .as_mut()
                .unwrap()
                .handle_key(code);
            match res {
                Some(PickResult::Cancel) => self.newdisk.as_mut().unwrap().picker = None,
                Some(PickResult::Confirm(path)) => {
                    let w = self.newdisk.as_mut().unwrap();
                    w.picker = None;
                    // Route to whichever field opened the picker — field 3 is
                    // the CD-ROM source folder, everything else the image path.
                    let picked = path.to_string_lossy().into_owned();
                    if w.field == 3 {
                        w.from_dir = picked;
                    } else {
                        w.path = picked;
                    }
                }
                None => {}
            }
            return true;
        }

        let step = self.newdisk.as_ref().unwrap().step;
        // Create is handled outside the borrow below.
        if step == WizStep::Details && code == KeyCode::Enter {
            self.newdisk_create();
            return true;
        }
        let w = self.newdisk.as_mut().unwrap();
        match step {
            WizStep::Class => match code {
                KeyCode::Up | KeyCode::Char('k') => {
                    w.class_sel = w.class_sel.saturating_sub(1);
                    w.fs_sel = 0;
                    w.seed_size_for_class();
                    true
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    w.class_sel = (w.class_sel + 1).min(NEW_CLASSES.len() - 1);
                    w.fs_sel = 0;
                    w.seed_size_for_class();
                    true
                }
                KeyCode::Enter => {
                    w.step = WizStep::Fs;
                    w.status = None;
                    true
                }
                _ => false,
            },
            WizStep::Fs => match code {
                KeyCode::Up | KeyCode::Char('k') => {
                    w.fs_sel = w.fs_sel.saturating_sub(1);
                    true
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    w.fs_sel = (w.fs_sel + 1).min(w.fs_count() - 1);
                    true
                }
                KeyCode::Enter => {
                    w.step = WizStep::Details;
                    w.field = 0;
                    true
                }
                KeyCode::Esc => {
                    w.step = WizStep::Class;
                    true
                }
                _ => false,
            },
            WizStep::Details => match code {
                KeyCode::Esc => {
                    w.step = WizStep::Fs;
                    w.status = None;
                    true
                }
                KeyCode::Down => {
                    let n = w.field_count();
                    w.field = (w.field + 1) % n;
                    true
                }
                KeyCode::Up | KeyCode::BackTab => {
                    let n = w.field_count();
                    w.field = (w.field + n - 1) % n;
                    true
                }
                // Tab browses on the two path-shaped fields, else advances.
                KeyCode::Tab => {
                    match w.field {
                        0 => {
                            w.picker = Some(FilePicker::new(PickKind::Any, "Browse for image path"))
                        }
                        3 => {
                            w.picker =
                                Some(FilePicker::new(PickKind::Dir, "Browse for source folder"))
                        }
                        _ => {
                            let n = w.field_count();
                            w.field = (w.field + 1) % n;
                        }
                    }
                    true
                }
                KeyCode::Backspace => {
                    w.status = None;
                    match w.field {
                        0 => w.path.pop(),
                        1 => w.size.pop(),
                        2 => w.name.pop(),
                        // The toggle rows hold no text to erase.
                        WIZ_FIELD_EXPAND | WIZ_FIELD_GUNZIP => None,
                        _ => w.from_dir.pop(),
                    };
                    true
                }
                // Space flips the toggle rather than typing into it; every
                // other row takes it as an ordinary character (a volume name
                // may well contain one).
                KeyCode::Char(' ') if w.field == WIZ_FIELD_EXPAND => {
                    w.status = None;
                    w.expand_archives = !w.expand_archives;
                    true
                }
                KeyCode::Char(' ') if w.field == WIZ_FIELD_GUNZIP => {
                    w.status = None;
                    w.expand_gunzip = !w.expand_gunzip;
                    true
                }
                KeyCode::Char(c) if !c.is_control() => {
                    w.status = None;
                    match w.field {
                        0 => w.path.push(c),
                        1 => w.size.push(c),
                        2 => w.name.push(c),
                        WIZ_FIELD_EXPAND | WIZ_FIELD_GUNZIP => {}
                        _ => w.from_dir.push(c),
                    }
                    true
                }
                _ => false,
            },
        }
    }

    /// Build a `NewCommand` from the wizard and run it through the shared `new`
    /// verb, which validates and dispatches to the per-fs formatter.
    fn newdisk_create(&mut self) {
        let Some(w) = self.newdisk.as_ref() else {
            return;
        };
        let path = expand_tilde(w.path.trim());
        if path.as_os_str().is_empty() {
            let w = self.newdisk.as_mut().unwrap();
            w.status = Some("Enter a path for the new image.".to_string());
            w.is_error = true;
            return;
        }
        let size = w.size.trim().to_string();
        let name = w.name.trim().to_string();
        let from_dir = w.from_dir.trim().to_string();
        // A CD-ROM is built by `optical new <target>`, which is a different
        // command tree from `new` — so the wizard produces one of two shapes
        // and dispatches accordingly rather than forcing everything through
        // `NewCommand`.
        if w.class() == DiskClass::Cdrom {
            let target = w.cd_target();
            let expand = w.expand_archives;
            let gunzip = w.expand_gunzip && target == CdTarget::NextUfs;
            self.newdisk_create_cdrom(target, path, size, name, from_dir, expand, gunzip);
            return;
        }
        let cmd = match w.class() {
            DiskClass::Floppy => NewCommand::Floppy(FloppyArgs {
                fs: FLOPPY_FS[w.fs_sel.min(FLOPPY_FS.len() - 1)].1,
                image: path.clone(),
                size,
                name,
                block_size: None,
                catalog_size: None,
                extents_size: None,
                cpm_preset: None,
                // The TUI wizard has no FAT32 toggle yet; size-based selection
                // is the existing behaviour. `rb-cli new ... --fat32` forces it.
                fat32: false,
            }),
            DiskClass::Volume => NewCommand::Volume(VolumeArgs {
                fs: VOLUME_FS[w.fs_sel.min(VOLUME_FS.len() - 1)].1,
                image: path.clone(),
                size,
                name,
                block_size: None,
                big_endian: false,
                catalog_size: None,
                extents_size: None,
                case_sensitive: false,
                min_catalog: None,
                affs_variant: 1,
                fat32: false,
                inodes: None,
                bytes_per_inode: None,
                cluster_size: None,
                sector_size: None,
            }),
            // Handled above — a disc goes through `optical new`, not `new`.
            DiskClass::Cdrom => unreachable!("CD-ROM dispatched before this match"),
            // Hard disks build with their defaults; the donor-clone and custom
            // partition-layout options stay on the CLI (`rb-cli new hd ...`).
            DiskClass::Hd => {
                let platform = HD_PLATFORMS[w.fs_sel.min(HD_PLATFORMS.len() - 1)].1;
                NewCommand::Hd {
                    cmd: match platform {
                        // Same defaults the CLI's clap definitions carry:
                        // SASI variant, the printing IPL stub, one partition.
                        HdPlatform::X68k => {
                            HdCommand::X68k(crate::cli::verbs::new_x68k_hdd::NewX68kHddArgs {
                                image: path.clone(),
                                size,
                                variant: crate::cli::verbs::new_x68k_hdd::CliVariant::Sasi,
                                stub: crate::cli::verbs::new_x68k_hdd::CliStub::Print,
                                partitions: 1,
                                system_disk: None,
                                boot_sector_donor: None,
                                builtin_boot_sector: false,
                            })
                        }
                        HdPlatform::SgiEfs => {
                            HdCommand::SgiEfs(crate::cli::verbs::new_sgi_hdd::NewSgiHddArgs {
                                image: path.clone(),
                                size,
                                name,
                                fs: crate::cli::verbs::new_sgi_hdd::SgiFs::Efs,
                                heads: crate::controller::new_disk::DEFAULT_HEADS,
                                sectors: crate::controller::new_disk::DEFAULT_SECTORS_PER_TRACK,
                                inodes: None,
                                bytes_per_inode: None,
                                // The TUI's "new disk" flow formats only; it
                                // has no folder picker, so populating stays a
                                // separate `import` step.
                                from_dir: None,
                                expand_archives: false,
                                flatten_folders: false,
                                force: false,
                                no_permissions: false,
                                include_appledouble: false,
                            })
                        }
                        HdPlatform::Mbr
                        | HdPlatform::Gpt
                        | HdPlatform::Apm
                        | HdPlatform::SgiTable
                        | HdPlatform::X68kTable
                        | HdPlatform::Rdb
                        | HdPlatform::Sun
                        | HdPlatform::Atari => {
                            // One partition over the whole disk, at the CLI's
                            // default type and alignment.
                            let args = crate::cli::verbs::new_partitioned_hd::PartitionedHdArgs {
                                image: path.clone(),
                                size: size.clone(),
                                partitions: vec!["rest".to_string()],
                                fills: Vec::new(),
                                filesystems: Vec::new(),
                                align: "1M".to_string(),
                                force: false,
                            };
                            match platform {
                                HdPlatform::Mbr => HdCommand::Mbr(args),
                                HdPlatform::Gpt => HdCommand::Gpt(args),
                                HdPlatform::X68kTable => HdCommand::X68kTable(args),
                                HdPlatform::Atari => HdCommand::Atari(args),
                                HdPlatform::SgiTable => HdCommand::Sgi(cylinder_args(args)),
                                HdPlatform::Rdb => HdCommand::Rdb(cylinder_args(args)),
                                HdPlatform::Sun => HdCommand::Sun(cylinder_args(args)),
                                _ => HdCommand::Apm(args),
                            }
                        }
                    },
                }
            }
        };
        // A task: building (and filling) a big image takes a while. `new::run` logs
        // advisories to stderr, which the task silences so they don't land on the alt-screen.
        self.start_task("Creating the disk image", move |job| {
            job.set_can_cancel(false);
            let outcome = crate::cli::verbs::new::run(cmd);
            Ok(Box::new(move |app: &mut App| {
                let Some(w) = app.newdisk.as_mut() else {
                    return;
                };
                match outcome {
                    Ok(()) => {
                        let size_note = std::fs::metadata(&path)
                            .map(|m| format!(" ({})", format_size(m.len())))
                            .unwrap_or_default();
                        w.status = Some(format!("Created {}{size_note}", path.display()));
                        w.is_error = false;
                    }
                    Err(e) => {
                        // `{e:#}` shows the whole anyhow context chain (root cause).
                        w.status = Some(format!("{e:#}"));
                        w.is_error = true;
                    }
                }
            }) as TaskDone)
        });
    }

    /// Build a CD-ROM disc image, optionally filling it from a host folder.
    ///
    /// Mirrors `rb-cli optical new <target>`, including `--size auto` and
    /// `--expand-archives`. Only `--flatten-folders` stays CLI-only: it is
    /// meaningful for an IRIX `inst` distribution and near-meaningless
    /// elsewhere, so it would be a fourth row that misleads on two of the
    /// three targets.
    #[allow(clippy::too_many_arguments)]
    fn newdisk_create_cdrom(
        &mut self,
        target: CdTarget,
        path: std::path::PathBuf,
        size: String,
        name: String,
        from_dir: String,
        expand_archives: bool,
        expand_gunzip: bool,
    ) {
        let from = (!from_dir.is_empty()).then(|| expand_tilde(&from_dir));
        if let Some(d) = &from {
            if !d.is_dir() {
                let w = self.newdisk.as_mut().unwrap();
                w.status = Some(format!("Source folder not found: {}", d.display()));
                w.is_error = true;
                return;
            }
        }
        if from.is_none() && size.eq_ignore_ascii_case("auto") {
            let w = self.newdisk.as_mut().unwrap();
            w.status = Some("Size 'auto' needs a source folder to measure.".to_string());
            w.is_error = true;
            return;
        }
        // A task like `new::run` above; the builders' stderr advisories are silenced.
        self.start_task("Building the CD-ROM image", move |job| {
            job.set_can_cancel(false);
            let outcome = match target {
                CdTarget::SgiEfs => crate::cli::verbs::new_sgi_cdrom::run(
                    crate::cli::verbs::new_sgi_cdrom::NewSgiCdromArgs {
                        image: path.clone(),
                        size,
                        name,
                        inodes: None,
                        bytes_per_inode: None,
                        from_dir: from,
                        expand_archives,
                        flatten_folders: false,
                        force: false,
                        no_permissions: false,
                        include_appledouble: false,
                    },
                ),
                CdTarget::MacHfs | CdTarget::MacHfsPlus => {
                    let fs = if target == CdTarget::MacHfs {
                        crate::controller::new_disk::MacCdFs::Hfs
                    } else {
                        crate::controller::new_disk::MacCdFs::HfsPlus
                    };
                    crate::cli::verbs::new_mac_cdrom::run(
                        fs,
                        crate::cli::verbs::new_mac_cdrom::NewMacCdromArgs {
                            image: path.clone(),
                            size,
                            name,
                            from_dir: from,
                            expand_archives,
                            force: false,
                            include_appledouble: false,
                            block_size: None,
                        },
                    )
                }
                CdTarget::NextUfs => crate::cli::verbs::new_next_cdrom::run(
                    crate::cli::verbs::new_next_cdrom::NewNextCdromArgs {
                        image: path.clone(),
                        size,
                        name,
                        from_dir: from,
                        expand_archives,
                        expand_gunzip,
                        flatten_folders: false,
                        force: false,
                        no_permissions: false,
                        include_appledouble: false,
                        bytes_per_inode: None,
                    },
                ),
            };
            Ok(Box::new(move |app: &mut App| {
                let Some(w) = app.newdisk.as_mut() else {
                    return;
                };
                match outcome {
                    Ok(()) => {
                        let size_note = std::fs::metadata(&path)
                            .map(|m| format!(" ({})", format_size(m.len())))
                            .unwrap_or_default();
                        w.status = Some(format!("Created {}{size_note}", path.display()));
                        w.is_error = false;
                    }
                    Err(e) => {
                        // `{e:#}` shows the whole anyhow context chain (root cause).
                        w.status = Some(format!("{e:#}"));
                        w.is_error = true;
                    }
                }
            }) as TaskDone)
        });
    }

    /// Bulk convert screen keys. Returns `true` when consumed.
    fn handle_bulk_key(&mut self, code: KeyCode) -> bool {
        if self.bulk.is_none() {
            self.bulk = Some(BulkState::default());
        }
        // Source / output folder picker (modal).
        if self
            .bulk
            .as_ref()
            .map(|b| b.picker.is_some())
            .unwrap_or(false)
        {
            let for_target = self.bulk.as_ref().unwrap().picker_target;
            let res = self
                .bulk
                .as_mut()
                .unwrap()
                .picker
                .as_mut()
                .unwrap()
                .handle_key(code);
            match res {
                Some(PickResult::Cancel) => self.bulk.as_mut().unwrap().picker = None,
                Some(PickResult::Confirm(path)) => {
                    if for_target {
                        let b = self.bulk.as_mut().unwrap();
                        b.out_dir = path.to_string_lossy().into_owned();
                        b.picker = None;
                    } else {
                        use crate::controller::bulk_convert::{purpose, BulkConvertAction};
                        let b = self.bulk.as_mut().unwrap();
                        b.picker = None;
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
                        self.bulk_rescan();
                    }
                }
                None => {}
            }
            return true;
        }

        let step = self.bulk.as_ref().unwrap().step;
        // Outputs that already exist: Skip / Overwrite / Esc back to the list.
        if step == BulkStep::Config && self.bulk.as_ref().unwrap().ctl.conflicts().is_some() {
            use crate::controller::bulk_convert::{BulkConvertAction, ConflictChoice};
            let choice = match code {
                KeyCode::Char('s') => ConflictChoice::SkipConflicting,
                KeyCode::Char('o') => ConflictChoice::Overwrite,
                KeyCode::Esc => ConflictChoice::Back,
                _ => return true,
            };
            self.bulk
                .as_mut()
                .unwrap()
                .act(BulkConvertAction::ResolveConflicts(choice));
            self.after_bulk_start();
            return true;
        }
        // Start handled outside the borrow.
        if step == BulkStep::Config
            && code == KeyCode::Enter
            && self
                .bulk
                .as_ref()
                .map(|b| b.field == b.start_row())
                .unwrap_or(false)
        {
            self.start_bulk();
            return true;
        }
        // Format change needs a rescan (borrow released first).
        if step == BulkStep::Config
            && self.bulk.as_ref().map(|b| b.field == 0).unwrap_or(false)
            && matches!(code, KeyCode::Left | KeyCode::Right)
        {
            if let Some(b) = self.bulk.as_mut() {
                let n = BULK_FORMATS.len();
                b.format_sel = if code == KeyCode::Left {
                    (b.format_sel + n - 1) % n
                } else {
                    (b.format_sel + 1) % n
                };
                let format = BULK_FORMATS[b.format_sel].1;
                b.act(crate::controller::bulk_convert::BulkConvertAction::SetFormat(format));
            }
            self.bulk_rescan();
            return true;
        }

        let b = self.bulk.as_mut().unwrap();
        match step {
            BulkStep::Source => match code {
                KeyCode::Enter | KeyCode::Char('o') => {
                    b.picker = Some(FilePicker::new(PickKind::Dir, "Choose source folder"));
                    b.picker_target = false;
                    true
                }
                _ => false,
            },
            BulkStep::Config => match code {
                KeyCode::Esc => {
                    b.step = BulkStep::Source;
                    true
                }
                KeyCode::Down => {
                    b.field = (b.field + 1).min(b.start_row());
                    true
                }
                KeyCode::Up => {
                    b.field = b.field.saturating_sub(1);
                    true
                }
                KeyCode::Char(' ') => {
                    if let Some(i) = b.file_at_cursor() {
                        let on = !b.files()[i].selected;
                        b.act(
                            crate::controller::bulk_convert::BulkConvertAction::SetSelected(i, on),
                        );
                    }
                    true
                }
                KeyCode::Tab if b.field == 1 => {
                    let mut p = FilePicker::new(PickKind::Dir, "Choose output folder");
                    p.input = b.out_dir.clone();
                    b.picker = Some(p);
                    b.picker_target = true;
                    true
                }
                KeyCode::Enter => {
                    b.field = (b.field + 1).min(b.start_row());
                    true
                }
                KeyCode::Backspace if b.field == 1 => {
                    b.out_dir.pop();
                    true
                }
                KeyCode::Char(c) if !c.is_control() && b.field == 1 => {
                    b.out_dir.push(c);
                    true
                }
                _ => false,
            },
            BulkStep::Run => match code {
                KeyCode::Esc => {
                    let running = b.ctl.is_running();
                    if running {
                        self.cancel_progress();
                        if let Some(b) = self.bulk.as_mut() {
                            b.result = Some("Cancel requested...".to_string());
                            b.is_error = true;
                        }
                    } else {
                        self.progress = None;
                        if let Some(b) = self.bulk.as_mut() {
                            b.result = None;
                            b.step = BulkStep::Config;
                        }
                    }
                    true
                }
                _ => true,
            },
        }
    }

    /// Rescan the source folder for the currently-chosen format (the filter
    /// depends on the format); the list is replaced when the scan job ends.
    fn bulk_rescan(&mut self) {
        use crate::controller::bulk_convert::{BulkConvertAction, BulkPhase};
        let Some(b) = self.bulk.as_mut() else {
            return;
        };
        // In Review a format change already rescans; otherwise ask for the first scan.
        if b.ctl.phase() == BulkPhase::Setup || !b.ctl.is_scanning() {
            b.send_output(true);
            b.act(BulkConvertAction::Scan);
        }
        // Keep the cursor on the Format row after a rescan.
        b.field = b.field.min(b.start_row());
    }

    /// Start the bulk conversion of the selected files through the controller.
    fn start_bulk(&mut self) {
        use crate::controller::bulk_convert::BulkConvertAction;
        let Some(b) = self.bulk.as_mut() else {
            return;
        };
        b.send_output(false);
        if let Some(problem) = b.ctl.start_problem() {
            b.result = Some(problem.to_string());
            b.is_error = true;
            b.step = BulkStep::Run;
            return;
        }
        b.act(BulkConvertAction::Start(None));
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

    /// Commander screen keys. Returns `true` when consumed.
    /// Whether the Inspect tab is showing its top-level selectable disk list.
    fn inspect_list_active(&self) -> bool {
        self.current() == TabId::Inspect && !self.inspect.as_ref().is_some_and(|s| s.source_open())
    }

    /// Number of selectable rows in the active tab (0 = the pane just scrolls).
    fn row_count(&self) -> usize {
        if self.inspect_list_active() {
            self.shell.devices().len()
        } else {
            0
        }
    }

    /// Per-frame update: feed the progress tracker and drop a finished task's
    /// worker (the bar stays until the user presses Esc, showing 100%).
    fn tick(&mut self) {
        self.poll_task();
        self.poll_devices();
        self.share_backup_tick();
        self.backup_tick();
        self.restore_tick();
        self.inspect_tick();
        // Drive the archives controller: a finished open or extraction becomes the status.
        self.archive_tick();
        // Drive the bulk-convert controller and mirror its file count and bytes.
        if let Some(b) = self.bulk.as_mut() {
            use crate::controller::Controller as _;
            let _ = b.ctl.tick();
            let _ = b.ctl.take_log();
            b.field = b.field.min(b.start_row());
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
        #[cfg(feature = "optical")]
        self.optical_tick();
        if let Some(p) = &mut self.progress {
            let cur = p.shared.lock().map(|s| s.current).unwrap_or(0);
            p.tracker.record(cur, &p.label);
        }
        if let Some(c) = self.commander.as_mut() {
            c.tick();
        }
        if let Some(ex) = self.explorer.as_mut() {
            ex.tick();
        }
        self.after_explorer();
        self.leave_commander_if_closed();
    }

    // --- rendering -------------------------------------------------------

    /// Run `body` as a [`TuiTask`]; its returned closure updates the app when it ends.
    fn start_task(
        &mut self,
        label: impl Into<String>,
        body: impl FnOnce(&crate::controller::task::JobCtx) -> anyhow::Result<TaskDone> + Send + 'static,
    ) {
        let started = self
            .task
            .start(label, move |ctx| with_stderr_only_suppressed(|| body(ctx)));
        if let Err(busy) = started {
            self.report_task_status(busy.to_string());
        }
    }

    /// Put a task's status on the screen's line.
    fn report_task_status(&mut self, msg: String) {
        self.status = Some(msg);
    }

    /// Drop a closed Explorer, quit when it passed a quit, and take a transform's command line.
    fn after_explorer(&mut self) {
        let Some(ex) = self.explorer.as_mut() else {
            return;
        };
        if let Some(cmd) = ex.take_palette() {
            self.palette_input = Some(cmd);
        }
        if ex.quit() {
            self.should_quit = true;
        }
        if ex.closed() {
            self.explorer = None;
        }
    }

    /// Tests: wait for the running task and apply it, as `tick` would.
    #[cfg(test)]
    fn finish_task(&mut self) {
        self.task.wait();
        self.poll_task();
    }

    /// Apply a finished task, or report how it ended.
    fn poll_task(&mut self) {
        use crate::controller::task::TaskEnd;
        match self.task.poll() {
            Some(TaskEnd::Done(apply)) => apply(self),
            Some(TaskEnd::Failed { label, error }) => {
                self.report_task_status(format!("{label} failed: {error}"))
            }
            Some(TaskEnd::Cancelled { label }) => {
                self.report_task_status(format!("{label} cancelled."))
            }
            None => {}
        }
    }

    /// The "Working" box over everything while a task runs: step, bar, speed and time.
    fn draw_task(&self, frame: &mut Frame, area: Rect) {
        let Some(snap) = self.task.snapshot() else {
            return;
        };
        self.draw_job_box(frame, area, "Working", &snap);
    }

    /// A job's step, bar, speed and time in a centred box titled `title`.
    fn draw_job_box(
        &self,
        frame: &mut Frame,
        area: Rect,
        title: &str,
        snap: &crate::controller::task::Snapshot,
    ) {
        let what = if snap.phase.is_empty() {
            &snap.label
        } else {
            &snap.phase
        };
        let w = area.width.saturating_sub(6).clamp(30, 70);
        let inner = w.saturating_sub(4) as usize;
        let (full, empty) = if self.border == ASCII_BORDER {
            ('#', '-')
        } else {
            ('\u{2588}', '\u{2591}')
        };
        let bar_w = inner.saturating_sub(7);
        let bar: String = match snap.progress.fraction() {
            Some(f) => {
                let filled = ((f as f64) * bar_w as f64).round() as usize;
                let cells: String = crate::compat::repeat_n(full, filled)
                    .chain(crate::compat::repeat_n(empty, bar_w.saturating_sub(filled)))
                    .collect();
                format!("{cells} {:>3.0}%", f * 100.0)
            }
            // No total: a block sweeps across so it reads as alive, not as a percentage.
            None => {
                let span = bar_w.max(1);
                let block = (span / 6).max(2);
                let travel = span.saturating_sub(block).max(1);
                let step = (snap.elapsed.as_millis() / 120) as usize % (travel * 2);
                let pos = if step < travel {
                    step
                } else {
                    travel * 2 - step
                };
                (0..span)
                    .map(|i| {
                        if i >= pos && i < pos + block {
                            full
                        } else {
                            empty
                        }
                    })
                    .collect()
            }
        };
        let hint = if snap.cancel_requested {
            "cancelling..."
        } else if snap.can_cancel {
            "Esc cancel"
        } else {
            "please wait"
        };
        let r = centered_rect(w, 6, area);
        frame.render_widget(Clear, r);
        frame.render_widget(
            Paragraph::new(Text::from(vec![
                Line::raw(format!(" {what}")),
                Line::styled(format!(" {bar}"), self.palette.accent()),
                Line::raw(format!(" {}", snap.detail())),
                Line::styled(format!(" {hint}"), self.palette.dim()),
            ]))
            .block(self.pane_block(title, true)),
            r,
        );
    }

    fn draw(&self, frame: &mut Frame) {
        let area = frame.area();
        if area.width < MIN_WIDTH || area.height < MIN_HEIGHT {
            self.draw_too_small(frame, area);
            return;
        }

        let shell_line = self.shell_line();
        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1), // tab bar + version
                Constraint::Length(u16::from(shell_line.is_some())),
                Constraint::Min(3),    // body
                Constraint::Length(1), // footer key bar
            ])
            .split(area);

        self.draw_tab_bar(frame, rows[0]);
        if let Some((text, warn)) = shell_line {
            let style = if warn {
                self.palette.warn()
            } else {
                self.palette.dim()
            };
            frame.render_widget(
                Paragraph::new(Line::styled(format!(" {text}"), style)),
                rows[1],
            );
        }
        self.draw_body(frame, rows[2]);
        self.draw_footer(frame, rows[3]);

        if self.progress.is_some() {
            self.draw_progress(frame, area);
        }
        if let Some(ex) = &self.explorer {
            let popup = centered_rect(
                area.width.saturating_sub(4).min(124),
                area.height.saturating_sub(2),
                area,
            );
            let outer = self.pane_block(ex.explorer_title().as_str(), true);
            let inner = outer.inner(popup);
            frame.render_widget(Clear, popup);
            frame.render_widget(outer, popup);
            self.draw_commander(frame, inner, ex);
        }
        if let Some(picker) = &self.open_picker {
            picker.draw(frame, area, self.palette, self.border);
        }
        if self.show_help {
            self.draw_help(frame, area);
        }
        self.draw_task(frame, area);
        if let Some(input) = &self.palette_input {
            let cp = centered_rect(70, 5, area);
            frame.render_widget(Clear, cp);
            frame.render_widget(
                Paragraph::new(Text::from(vec![
                    Line::from(vec![
                        Span::styled("  : ", self.palette.accent()),
                        Span::raw(input.clone()),
                        Span::styled(" ", self.palette.accent().add_modifier(Modifier::REVERSED)),
                    ]),
                    Line::raw(""),
                    Line::styled(
                        "  Any rb-cli verb (e.g. `ls disk.img @1`).  Enter run   Esc cancel",
                        self.palette.dim(),
                    ),
                ]))
                .block(self.pane_block("Command palette", true)),
                cp,
            );
        }
    }

    fn draw_too_small(&self, frame: &mut Frame, area: Rect) {
        let msg = Text::from(vec![
            Line::raw("Terminal too small."),
            Line::raw(format!(
                "Resize to at least {MIN_WIDTH}x{MIN_HEIGHT} (now {}x{}).",
                area.width, area.height
            )),
        ]);
        frame.render_widget(
            Paragraph::new(msg)
                .alignment(Alignment::Center)
                .wrap(Wrap { trim: true }),
            area,
        );
    }

    fn draw_tab_bar(&self, frame: &mut Frame, area: Rect) {
        // No numeric prefixes: the names alone keep all nine tabs on one line
        // even at 80 columns. The `1`-`9` jump keys still work (see the footer).
        let titles: Vec<Line> = TABS.iter().map(|(_, name)| Line::raw(*name)).collect();
        // No per-tab padding: reclaim the width so all eight fit on one line.
        let tabs = Tabs::new(titles)
            .select(self.active)
            .style(self.palette.dim())
            .highlight_style(
                self.palette
                    .accent()
                    .add_modifier(Modifier::REVERSED | Modifier::BOLD),
            )
            .divider("|")
            .padding("", "");
        // Inset one column so the bar lines up with the bordered body below.
        let inset = Layout::horizontal([Constraint::Length(1), Constraint::Min(1)]).split(area);
        frame.render_widget(tabs, inset[1]);
    }

    fn draw_body(&self, frame: &mut Frame, area: Rect) {
        // The Inspect disk list is a real selectable widget; an open source is the
        // Inspect screen. Everything else (placeholder tabs) is scrollable text.
        if self.inspect_list_active() {
            self.draw_inspect_list(frame, area);
            if let Some(s) = &self.inspect {
                self.draw_inspect_windows(frame, area, s);
            }
            return;
        }
        if self.current() == TabId::Inspect {
            if let Some(s) = &self.inspect {
                self.draw_inspect(frame, area, s);
                return;
            }
        }
        if self.current() == TabId::NewDisk {
            if let Some(w) = &self.newdisk {
                self.draw_newdisk(frame, area, w);
                return;
            }
        }
        if self.current() == TabId::Backup {
            if let Some(b) = &self.backup {
                self.draw_backup(frame, area, b);
                return;
            }
        }
        if self.current() == TabId::Restore {
            if let Some(r) = &self.restore {
                self.draw_restore(frame, area, r);
                return;
            }
        }
        if self.current() == TabId::Bulk {
            if let Some(b) = &self.bulk {
                self.draw_bulk(frame, area, b);
                return;
            }
        }
        #[cfg(feature = "optical")]
        if self.current() == TabId::Optical {
            if let Some(o) = &self.optical {
                self.draw_optical(frame, area, o);
                return;
            }
        }
        if self.current() == TabId::Archives {
            if let Some(a) = &self.archive {
                self.draw_archive(frame, area, a);
                return;
            }
        }
        if self.current() == TabId::Commander {
            if let Some(c) = &self.commander {
                self.draw_commander(frame, area, c);
                return;
            }
        }
        if self.current() == TabId::Settings {
            if let Some(s) = &self.settings {
                self.draw_settings_tab(frame, area, s);
                return;
            }
        }
        let (_, name) = TABS[self.active];
        let total = self.content_line_count();
        let title = if total > 0 {
            format!("{name}  [{}/{}]", self.scroll + 1, total)
        } else {
            name.to_string()
        };
        frame.render_widget(
            Paragraph::new(self.tab_content())
                .block(self.pane_block(&title, true))
                .wrap(Wrap { trim: false })
                .scroll((self.scroll, 0)),
            area,
        );
    }

    fn draw_inspect_list(&self, frame: &mut Frame, area: Rect) {
        let disks = self.shell.devices();
        if disks.is_empty() {
            frame.render_widget(
                Paragraph::new("No disks detected. Press `r` to rescan.")
                    .block(self.pane_block("Inspect", true)),
                area,
            );
            return;
        }
        let items: Vec<ListItem> = disks
            .iter()
            .map(|d| {
                let flags = if d.is_removable { "removable" } else { "fixed" };
                let media = if d.media_name.is_empty() {
                    d.name.clone()
                } else {
                    d.media_name.clone()
                };
                ListItem::new(Line::from(vec![
                    Span::styled(
                        format!("{:<14}", d.path.display().to_string()),
                        self.palette.accent(),
                    ),
                    Span::raw(format!(
                        " {:>9}  {}  [{}]",
                        format_size(d.size_bytes),
                        media,
                        flags
                    )),
                ]))
            })
            .collect();
        let title = format!(
            "Inspect  [{} disk(s)]  Enter=partitions  o=open file",
            disks.len()
        );
        let list = List::new(items)
            .block(self.pane_block(&title, true))
            .highlight_style(Style::default().add_modifier(Modifier::REVERSED | Modifier::BOLD))
            .highlight_symbol("> ");
        let mut state = ListState::default();
        state.select(Some(self.selection.min(disks.len() - 1)));
        frame.render_stateful_widget(list, area, &mut state);
    }

    /// The hardware facts for the header above a disk's partition table: what
    /// the enumerator knows, plus what the OS has mounted.
    ///
    /// The mounted list is worth keeping visible precisely because it is *not*
    /// the partition table - a volume the host cannot mount (ext on Mac OS X,
    /// say) is absent from it while being present in the table below, and seeing
    /// both together is what makes that difference legible.
    fn disk_header_lines(&self, d: &DiskDevice) -> Vec<Line<'static>> {
        let field = |k: &str, v: String| {
            Line::from(vec![
                Span::styled(format!("{k:<9}"), self.palette.accent()),
                Span::raw(v),
            ])
        };
        let media = if d.media_name.is_empty() {
            d.name.clone()
        } else {
            d.media_name.clone()
        };
        let mut lines = vec![
            field(
                "Media:",
                format!(
                    "{media}  ({})  {}",
                    format_size(d.size_bytes),
                    d.bus_protocol
                ),
            ),
            field(
                "Flags:",
                format!(
                    "{}{}{}",
                    if d.is_removable { "removable" } else { "fixed" },
                    if d.is_read_only { ", read-only" } else { "" },
                    if d.is_system { ", system" } else { "" },
                ),
            ),
        ];
        if d.partitions.is_empty() {
            lines.push(field("Mounted:", "(none)".to_string()));
        } else {
            let mounted: Vec<String> = d
                .partitions
                .iter()
                .map(|p| format!("{} {}", p.name, p.mount_point.display()))
                .collect();
            lines.push(field("Mounted:", mounted.join(", ")));
        }
        lines
    }

    /// The New Disk wizard: media class → filesystem → path/size/name, drawn as
    /// a single-pane form that changes with the current step.
    fn draw_newdisk(&self, frame: &mut Frame, area: Rect, w: &NewWizard) {
        let mut lines: Vec<Line> = Vec::new();
        let (title, step_no): (&str, &str) = match w.step {
            WizStep::Class => ("New Disk  -  step 1/3: media class", "class"),
            WizStep::Fs => ("New Disk  -  step 2/3: filesystem", "fs"),
            WizStep::Details => ("New Disk  -  step 3/3: details", "details"),
        };
        let _ = step_no;

        match w.step {
            WizStep::Class => {
                lines.push(Line::styled("Pick a media class:", self.palette.accent()));
                lines.push(Line::raw(""));
                for (i, (label, _, desc)) in NEW_CLASSES.iter().enumerate() {
                    let sel = i == w.class_sel;
                    let marker = if sel { "> " } else { "  " };
                    let style = if sel {
                        self.palette
                            .accent()
                            .add_modifier(Modifier::REVERSED | Modifier::BOLD)
                    } else {
                        self.palette.accent()
                    };
                    lines.push(Line::styled(format!("{marker}{label}"), style));
                    lines.push(Line::styled(format!("     {desc}"), self.palette.dim()));
                }
                lines.push(Line::raw(""));
                lines.push(Line::styled(
                    "Hard disks build with their defaults - the table-only entries get \
                     one partition over the whole disk, empty. Donor-cloning and \
                     multi-partition layouts need the CLI \
                     (`rb-cli new hd <table> --partition ...`). \
                     CD-ROM images: the Optical tab.",
                    self.palette.dim(),
                ));
                lines.push(Line::raw(""));
                lines.push(Line::styled(
                    "Up/Down select   Enter next   Left/Right change tab",
                    self.palette.dim(),
                ));
            }
            WizStep::Fs => {
                lines.push(Line::from(vec![
                    Span::styled("Class: ", self.palette.dim()),
                    Span::styled(w.class_label(), self.palette.accent()),
                ]));
                lines.push(Line::raw(""));
                lines.push(Line::styled("Pick a filesystem:", self.palette.accent()));
                lines.push(Line::raw(""));
                let visible = area.height.saturating_sub(8) as usize;
                let start = w.fs_sel.saturating_sub(visible.saturating_sub(1));
                for i in start..w.fs_count().min(start + visible.max(1)) {
                    let sel = i == w.fs_sel;
                    let marker = if sel { "> " } else { "  " };
                    let style = if sel {
                        self.palette
                            .accent()
                            .add_modifier(Modifier::REVERSED | Modifier::BOLD)
                    } else {
                        self.palette.accent()
                    };
                    lines.push(Line::styled(format!("{marker}{}", w.fs_label(i)), style));
                }
                lines.push(Line::raw(""));
                lines.push(Line::styled(
                    "Up/Down select   Enter next   Esc back",
                    self.palette.dim(),
                ));
            }
            WizStep::Details => {
                let fs_label = w.fs_label(w.fs_sel);
                lines.push(Line::from(vec![
                    Span::styled("Creating: ", self.palette.dim()),
                    Span::styled(
                        format!("{} / {fs_label}", w.class_label()),
                        self.palette.accent(),
                    ),
                ]));
                lines.push(Line::raw(""));
                let field = |idx: usize, label: &str, val: &str, hint: &str| -> Line<'static> {
                    let active = w.field == idx;
                    let cursor = if active {
                        Span::styled(" ", self.palette.accent().add_modifier(Modifier::REVERSED))
                    } else {
                        Span::raw("")
                    };
                    let shown = if val.is_empty() && !active {
                        hint.to_string()
                    } else {
                        val.to_string()
                    };
                    let val_style = if val.is_empty() && !active {
                        self.palette.dim()
                    } else {
                        Style::default()
                    };
                    Line::from(vec![
                        Span::styled(format!("  {label:<8}"), self.palette.accent()),
                        Span::styled(shown, val_style),
                        cursor,
                    ])
                };
                lines.push(field(
                    0,
                    "Path:",
                    &w.path,
                    "(type a path, or Tab to browse)",
                ));
                lines.push(field(
                    1,
                    "Size:",
                    &w.size,
                    if w.class() == DiskClass::Cdrom {
                        "600M (or auto)"
                    } else {
                        "800K"
                    },
                ));
                lines.push(field(2, "Name:", &w.name, "rusty-backup"));
                if w.class() == DiskClass::Cdrom {
                    lines.push(field(
                        3,
                        "From:",
                        &w.from_dir,
                        "(optional: folder to fill the disc from, Tab to browse)",
                    ));
                    // The toggles only bite with a source folder, so they read
                    // dim until one is set.
                    let armed = !w.from_dir.trim().is_empty();
                    let toggle = |row: usize, label: &str, on: bool, text: &str| {
                        let active = w.field == row;
                        let box_ = if on { "[x]" } else { "[ ]" };
                        let label_style = if armed || active {
                            Style::default()
                        } else {
                            self.palette.dim()
                        };
                        Line::from(vec![
                            Span::styled(format!("  {label:<8}"), self.palette.accent()),
                            Span::styled(format!("{box_} {text}"), label_style),
                            if active {
                                Span::styled(
                                    " ",
                                    self.palette.accent().add_modifier(Modifier::REVERSED),
                                )
                            } else {
                                Span::raw("")
                            },
                        ])
                    };
                    lines.push(toggle(
                        WIZ_FIELD_EXPAND,
                        "Expand:",
                        w.expand_archives,
                        "unpack .tar / .sit / .cpt / .hqx into the disc",
                    ));
                    if w.cd_target() == CdTarget::NextUfs {
                        lines.push(toggle(
                            WIZ_FIELD_GUNZIP,
                            "Gunzip:",
                            w.expand_gunzip,
                            "strip the gzip layer only (.tar.gz -> .tar)",
                        ));
                    }
                }
                lines.push(Line::raw(""));
                if let Some(s) = &w.status {
                    let style = if w.is_error {
                        self.palette.warn()
                    } else {
                        self.palette.accent()
                    };
                    lines.push(Line::styled(format!("  {s}"), style));
                    lines.push(Line::raw(""));
                }
                lines.push(Line::styled(
                    if w.class() == DiskClass::Cdrom {
                        "Up/Down field   Tab browse (path/folder)   Space toggle   Enter create   Esc back"
                    } else {
                        "Up/Down field   Tab browse (path/folder)   Enter create   Esc back"
                    },
                    self.palette.dim(),
                ));
            }
        }

        frame.render_widget(
            Paragraph::new(Text::from(lines))
                .block(self.pane_block(title, true))
                .wrap(Wrap { trim: false }),
            area,
        );

        // The path field's browse picker draws on top.
        if let Some(picker) = &w.picker {
            picker.draw(frame, area, self.palette, self.border);
        }
    }

    /// The Bulk convert screen: source -> config (format + review list) -> run.
    fn draw_bulk(&self, frame: &mut Frame, area: Rect, b: &BulkState) {
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
            BulkStep::Config => {
                let sel_row = |active: bool| {
                    if active {
                        self.palette.accent().add_modifier(Modifier::BOLD)
                    } else {
                        self.palette.accent()
                    }
                };
                // Format row (0).
                lines.push(Line::from(vec![
                    Span::styled(
                        format!("{}Format:    ", if b.field == 0 { "> " } else { "  " }),
                        sel_row(b.field == 0),
                    ),
                    Span::raw(format!("< {} >", BULK_FORMATS[b.format_sel].0)),
                ]));
                if b.ctl.is_scanning() {
                    lines.push(Line::styled(
                        "  Scanning the source folder...",
                        self.palette.dim(),
                    ));
                }
                // Output row (1).
                let out_cursor = if b.field == 1 {
                    Span::styled(" ", self.palette.accent().add_modifier(Modifier::REVERSED))
                } else {
                    Span::raw("")
                };
                lines.push(Line::from(vec![
                    Span::styled(
                        format!("{}Output:    ", if b.field == 1 { "> " } else { "  " }),
                        sel_row(b.field == 1),
                    ),
                    Span::raw(b.out_dir.clone()),
                    out_cursor,
                ]));
                lines.push(Line::raw(""));
                // File review list.
                let selected = b.ctl.selected_count();
                lines.push(Line::styled(
                    format!(
                        "Files ({} of {} selected)  -  Space toggles:",
                        selected,
                        b.files().len()
                    ),
                    self.palette.accent(),
                ));
                if let Some(conflicts) = b.ctl.conflicts() {
                    lines.push(Line::styled(
                        format!(
                            "  {} output file(s) already exist:  s Skip them   o Overwrite   Esc Back",
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
                if b.files().is_empty() {
                    lines.push(Line::styled(
                        "  (no matching files in this folder for this format)",
                        self.palette.dim(),
                    ));
                } else {
                    // Window the list so the cursor stays visible.
                    let avail = (area.height as usize).saturating_sub(12).max(3);
                    let cur_file = b.file_at_cursor().unwrap_or(0);
                    let start = cur_file.saturating_sub(avail.saturating_sub(1));
                    for (i, f) in b.files().iter().enumerate().skip(start).take(avail) {
                        let on_row = b.field == i + 2;
                        let box_ = if f.selected { "[x]" } else { "[ ]" };
                        let name = basename(&f.path.to_string_lossy());
                        let style = if on_row {
                            self.palette
                                .accent()
                                .add_modifier(Modifier::REVERSED | Modifier::BOLD)
                        } else if f.selected {
                            self.palette.accent()
                        } else {
                            self.palette.dim()
                        };
                        lines.push(Line::styled(
                            format!(
                                "{}{box_} {:<40} {}",
                                if on_row { "> " } else { "  " },
                                name,
                                format_size(f.size)
                            ),
                            style,
                        ));
                    }
                }
                lines.push(Line::raw(""));
                let start_sel = b.field == b.start_row();
                lines.push(Line::styled(
                    format!(
                        "{}[ Convert {} file(s) ]",
                        if start_sel { "> " } else { "  " },
                        selected
                    ),
                    if start_sel {
                        self.palette
                            .accent()
                            .add_modifier(Modifier::REVERSED | Modifier::BOLD)
                    } else {
                        self.palette.accent()
                    },
                ));
                lines.push(Line::raw(""));
                lines.push(Line::styled(
                    "Up/Down move   Left/Right format   Space toggle   Tab browse (Output)   \
                     Enter Start   Esc back",
                    self.palette.dim(),
                ));
            }
            BulkStep::Run => {
                lines.push(Line::from(vec![
                    Span::styled("Format: ", self.palette.dim()),
                    Span::raw(BULK_FORMATS[b.format_sel].0.to_string()),
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

    /// The Commander dual-pane file manager.
    /// The physical-disk chooser overlay used by the Backup source step.
    /// Shared physical-disk chooser overlay (Backup source + Restore target).
    fn draw_device_pick(&self, frame: &mut Frame, area: Rect, device_sel: usize, title: &str) {
        let disks = self.shell.devices();
        let mut lines: Vec<Line> = Vec::new();
        if disks.is_empty() {
            lines.push(Line::raw("  No disks detected."));
        } else {
            for (i, d) in disks.iter().enumerate() {
                let sel = i == device_sel;
                let marker = if sel { "> " } else { "  " };
                let style = if sel {
                    self.palette
                        .accent()
                        .add_modifier(Modifier::REVERSED | Modifier::BOLD)
                } else {
                    self.palette.accent()
                };
                let media = if d.media_name.is_empty() {
                    d.name.clone()
                } else {
                    d.media_name.clone()
                };
                lines.push(Line::styled(
                    format!(
                        "{marker}{:<14} {:>9}  {}",
                        d.path.display().to_string(),
                        format_size(d.size_bytes),
                        media
                    ),
                    style,
                ));
            }
        }
        lines.push(Line::raw(""));
        lines.push(Line::styled(
            "Up/Down select   Enter choose   Esc cancel",
            self.palette.dim(),
        ));
        let h = (lines.len() as u16 + 2).min(area.height.saturating_sub(2));
        let popup = centered_rect(72, h, area);
        frame.render_widget(Clear, popup);
        frame.render_widget(
            Paragraph::new(Text::from(lines)).block(self.pane_block(title, true)),
            popup,
        );
    }

    /// A bottom-anchored progress bar: percentage + bytes + live rate + ETA,
    /// drawn ASCII-safe (Unicode blocks on capable terminals). Driven by the
    /// shared `RateTracker` the GUI and CLI also use.
    fn draw_progress(&self, frame: &mut Frame, area: Rect) {
        let Some(p) = &self.progress else { return };
        let (cur, total, done) = p
            .shared
            .lock()
            .map(|s| (s.current, s.total, s.done))
            .unwrap_or((0, 0, false));
        let ratio = if total > 0 {
            (cur as f64 / total as f64).clamp(0.0, 1.0)
        } else {
            0.0
        };
        let pct = (ratio * 100.0).round() as u16;

        // Bar geometry: fit inside a bottom overlay box.
        let w = area.width.saturating_sub(6).clamp(20, 70);
        let box_h = 5u16; // border (2) + bar + stats + hint
        let x = area.x + (area.width - w) / 2;
        // Sit fully inside the body, one row clear of its bottom border.
        let y = area.y + area.height.saturating_sub(box_h + 2);
        let popup = Rect {
            x,
            y,
            width: w,
            height: box_h,
        };

        let inner_w = w.saturating_sub(4) as usize; // borders + brackets
        let (full, empty) = if self.border == ASCII_BORDER {
            ('#', '-')
        } else {
            ('\u{2588}', '\u{2591}') // full block / light shade
        };
        let filled = (ratio * inner_w as f64).round() as usize;
        let bar: String = crate::compat::repeat_n(full, filled)
            .chain(crate::compat::repeat_n(
                empty,
                inner_w.saturating_sub(filled),
            ))
            .collect();

        let stats = format!(
            "{pct:>3}%   {} / {}{}",
            format_size(cur),
            format_size(total),
            p.tracker.suffix(cur, total),
        );
        let hint = if done {
            "done - Esc to close".to_string()
        } else {
            format!("{} - Esc to cancel", p.label)
        };

        let text = Text::from(vec![
            Line::styled(bar, self.palette.accent()),
            Line::raw(stats),
            Line::styled(hint, self.palette.dim()),
        ]);
        frame.render_widget(Clear, popup);
        frame.render_widget(
            Paragraph::new(text).block(self.pane_block("Working", true)),
            popup,
        );
    }

    fn draw_footer(&self, frame: &mut Frame, area: Rect) {
        // Keys on the left, version pinned to the right (as the GUI shows it).
        let cols = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Min(10), Constraint::Length(24)])
            .split(area);

        // Context-sensitive footer. Inside the Explorer this defers to
        // `key_context`, the same table its own footer and `?` render from,
        // rather than a second hand-written list. The hand-written one had
        // drifted: it never mentioned edit, metadata, new, delete, bless,
        // check, repair or transform.
        let explorer_keys = self.explorer.as_ref().map(|ex| ex.footer_keys());
        let keys: Vec<(&str, &str)> = if self.explorer.as_ref().is_some_and(|e| e.editor_open()) {
            HINTS_EDITOR.iter().map(|h| (h.keys, h.desc)).collect()
        } else if let Some(keys) = &explorer_keys {
            keys.iter().map(|(k, d)| (k.as_str(), *d)).collect()
        } else if self.progress.is_some() {
            vec![("Esc", "Cancel")]
        } else if self.current() == TabId::Backup {
            self.backup_footer_keys()
        } else if self.current() == TabId::Restore {
            self.restore_footer_keys()
        } else if self.current() == TabId::Bulk {
            let step = self.bulk.as_ref().map(|b| b.step);
            match step {
                Some(BulkStep::Source) => {
                    vec![("<-/->", "Tabs"), ("Enter/o", "Folder"), ("?", "Help")]
                }
                Some(BulkStep::Config) => vec![
                    ("Up/Dn", "Move"),
                    ("<-/->", "Format"),
                    ("Space", "Toggle"),
                    ("Enter", "Start"),
                    ("Esc", "Back"),
                ],
                _ => vec![("Esc", "Back")],
            }
        } else if self.current() == TabId::Optical {
            #[cfg(feature = "optical")]
            {
                self.optical_footer_keys()
            }
            // Optical support not compiled in (e.g. vintage build): the tab shows
            // an explanatory stub, so only the tab-navigation hints apply.
            #[cfg(not(feature = "optical"))]
            {
                vec![("<-/->", "Tabs"), ("?", "Help")]
            }
        } else if self.current() == TabId::Archives {
            let loaded = self
                .archive
                .as_ref()
                .map(|a| a.ctl.is_loaded())
                .unwrap_or(false);
            if loaded {
                vec![
                    ("Up/Dn", "Scroll"),
                    ("f", "Fork fmt"),
                    ("e", "Extract"),
                    ("Esc", "Close"),
                ]
            } else {
                vec![("<-/->", "Tabs"), ("Enter/o", "Open"), ("?", "Help")]
            }
        } else if self.current() == TabId::Commander {
            let keys = self
                .commander
                .as_ref()
                .map(|c| c.footer_keys())
                .unwrap_or_default();
            let refs: Vec<(&str, &str)> = keys.iter().map(|(k, d)| (k.as_str(), *d)).collect();
            frame.render_widget(Paragraph::new(self.key_bar(&refs)), cols[0]);
            self.draw_footer_badge(frame, cols[1]);
            return;
        } else if self.current() == TabId::NewDisk {
            let step = self.newdisk.as_ref().map(|w| w.step);
            match step {
                Some(WizStep::Details) => {
                    let mut keys = vec![("<-/->", "Tabs"), ("Up/Dn", "Field"), ("Tab", "Browse")];
                    // The toggle row only exists on the CD-ROM class.
                    if self.newdisk.as_ref().map(|w| w.class()) == Some(DiskClass::Cdrom) {
                        keys.push(("Space", "Toggle"));
                    }
                    keys.push(("Enter", "Create"));
                    keys.push(("Esc", "Back"));
                    keys
                }
                _ => vec![
                    ("<-/->", "Tabs"),
                    ("Up/Dn", "Select"),
                    ("Enter", "Next"),
                    ("Esc", "Back"),
                    ("?", "Help"),
                ],
            }
        } else if self.current() == TabId::Settings {
            vec![
                ("<-/->", "Tabs"),
                ("Up/Dn", "Select"),
                ("Enter", "Toggle"),
                ("?", "Help"),
                ("q", "Quit"),
            ]
        } else if let Some(keys) = self
            .inspect_footer_keys()
            .filter(|_| self.current() == TabId::Inspect)
        {
            keys
        } else {
            // On the disk list, name what Enter actually produces. "Open" was
            // vague enough that the partition table read as a hidden second step
            // rather than the destination.
            let enter = if self.inspect_list_active() {
                ("Enter", "Partitions")
            } else {
                ("Enter", "Open")
            };
            let mut k = vec![("<-/->", "Tabs"), ("Up/Dn", "Select"), enter];
            if self.inspect_list_active() {
                k.push(("o", "Open file"));
                k.push(("r", "Rescan"));
            }
            k.push(("?", "Help"));
            k.push(("q", "Quit"));
            k
        };
        frame.render_widget(Paragraph::new(self.key_bar(&keys)), cols[0]);

        self.draw_footer_badge(frame, cols[1]);
    }

    /// Privilege badge + version, right-aligned (as the GUI shows version).
    fn draw_footer_badge(&self, frame: &mut Frame, area: Rect) {
        let priv_style = if self.elevated {
            self.palette.accent()
        } else {
            self.palette.dim()
        };
        let right = Line::from(vec![
            Span::styled(self.privilege_label(), priv_style),
            Span::styled(format!(" v{} ", env!("APP_VERSION")), self.palette.dim()),
        ]);
        frame.render_widget(Paragraph::new(right).right_aligned(), area);
    }

    fn key_bar(&self, keys: &[(&str, &str)]) -> Line<'static> {
        let mut spans = Vec::new();
        for (k, label) in keys {
            spans.push(Span::styled(
                format!(" {k} "),
                self.palette.accent().add_modifier(Modifier::REVERSED),
            ));
            spans.push(Span::raw(format!(" {label}   ")));
        }
        Line::from(spans)
    }

    /// The active screen's name and key list.
    ///
    /// Ordered innermost-first: the editor sits over the Explorer, which sits
    /// over the Inspect tab, and the keys that work are the innermost one's.
    /// The footer and the `?` window both call this, so what is drawn at the
    /// bottom of the screen is by construction what `?` will list.
    fn key_context(&self) -> (&'static str, &'static [KeyHint]) {
        if self.explorer.as_ref().is_some_and(|e| e.editor_open()) {
            return ("Text editor", HINTS_EDITOR);
        }
        if self.current() == TabId::Inspect {
            return self
                .inspect_key_context()
                .unwrap_or(("Inspect - disks", HINTS_INSPECT_DISKS));
        }
        if self.current() == TabId::Backup {
            return self.backup_key_context();
        }
        if self.current() == TabId::Restore {
            return self.restore_key_context();
        }
        #[cfg(feature = "optical")]
        if self.current() == TabId::Optical {
            return self.optical_key_context();
        }
        if self.current() == TabId::Settings {
            return self.settings_key_context();
        }
        if self.current() == TabId::Archives {
            return self.archives_key_context();
        }
        ("Tabs", HINTS_TAB)
    }

    fn draw_help(&self, frame: &mut Frame, area: Rect) {
        let (title, hints) = self.key_context();

        // Every key that works here, then the ones that work everywhere. Built
        // as rows first so the scroll maths below counts what is really drawn.
        let mut rows: Vec<(String, String)> = Vec::new();
        for h in hints {
            rows.push((h.keys.to_string(), h.desc.to_string()));
        }
        // Blank spacer, then a heading, then the always-available keys. A row
        // with an empty description renders as a heading; both empty is a gap.
        rows.push((String::new(), String::new()));
        rows.push(("Anywhere".to_string(), String::new()));
        for h in HINTS_GLOBAL {
            rows.push((h.keys.to_string(), h.desc.to_string()));
        }
        if self.update_banner().is_some() {
            rows.push(("F12".to_string(), "Hide the update banner".to_string()));
        }
        let key_w = rows
            .iter()
            .map(|(k, _)| k.chars().count())
            .max()
            .unwrap_or(8)
            .max(8);

        let popup = centered_rect(
            72.min(area.width.saturating_sub(4)),
            area.height.saturating_sub(4).min(24),
            area,
        );
        // Two chrome rows for the border, one for the scroll hint at the foot.
        let view_h = popup.height.saturating_sub(3) as usize;
        let total = rows.len();
        let max_scroll = total.saturating_sub(view_h);
        let scroll = self.help_scroll.min(max_scroll);

        let mut lines: Vec<Line> = Vec::new();
        for (k, d) in rows.iter().skip(scroll).take(view_h) {
            if k.is_empty() && d.is_empty() {
                lines.push(Line::raw(""));
            } else if d.is_empty() {
                lines.push(Line::styled(format!("  {k}"), self.palette.accent()));
            } else {
                lines.push(Line::from(vec![
                    Span::styled(format!("  {k:<key_w$}  "), self.palette.accent()),
                    Span::raw(d.clone()),
                ]));
            }
        }
        let more = total > view_h;
        lines.push(Line::styled(
            if more {
                format!(
                    "  {}-{} of {}   Up/Down PgUp/PgDn scroll   Esc close",
                    scroll + 1,
                    (scroll + view_h).min(total),
                    total
                )
            } else {
                "  Esc close".to_string()
            },
            self.palette.dim(),
        ));

        frame.render_widget(Clear, popup);
        frame.render_widget(
            Paragraph::new(Text::from(lines))
                .block(self.pane_block(&format!("Keys - {title}"), true)),
            popup,
        );
    }

    // --- per-tab content (placeholders until each milestone lands) --------

    fn tab_content(&self) -> Text<'static> {
        let mut text = match self.current() {
            TabId::Backup => self.stub(
                "Back up a disk image or device to a backup folder.",
                &[
                    "Pick a source (image file or a physical disk).",
                    "Choose format (Zstd / Raw / CHD / VHD) and checksums.",
                    "Run with live progress; verify on completion.",
                ],
                "M3 - reuses backup + physical_write_runner.",
            ),
            TabId::Restore => self.stub(
                "Restore a backup folder or .cbk to a target image or device.",
                &[
                    "Load a backup folder / .cbk (backup_loader).",
                    "Choose partition sizing (original / minimum / custom).",
                    "Write to an image or a physical disk, with progress.",
                ],
                "M3 - reuses restore + backup_loader + physical_write_runner.",
            ),
            TabId::Inspect => self.inspect_content(),
            TabId::Optical => self.stub(
                "Optical discs: list drives, rip, convert, browse, extract, new.",
                &[
                    "Enumerate optical drives (optical_devices).",
                    "Rip a disc to ISO / BIN-CUE / CHD.",
                    "Browse / extract an image; create a new CD-ROM image.",
                ],
                "M4 - reuses optical_devices + src/optical/.",
            ),
            TabId::Archives => self.stub(
                "Classic Mac archives: list, extract, create.",
                &[
                    "Open .sit / .sea / .cpt / .mar (and .hqx-wrapped forms).",
                    "Extract to host, preserving both forks + Finder info.",
                    "Create a new archive from image contents.",
                ],
                "M5 - reuses archive_edit.",
            ),
            // The Commander view draws its own tab.
            TabId::Commander => Text::default(),
            TabId::Bulk => self.stub(
                "Bulk operations across a folder of images.",
                &[
                    "Convert every image in a folder to one output format.",
                    "Review the scan; un-check any files to skip.",
                    "Bulk floppy-container conversion too.",
                ],
                "M8 - reuses bulk_convert_runner.",
            ),
            TabId::NewDisk => self.newdisk_content(),
            TabId::Settings => self.settings_content(),
        };
        // Tabs whose real operations touch a physical disk carry an elevation
        // caution when we're not running elevated.
        if matches!(
            self.current(),
            TabId::Backup | TabId::Restore | TabId::Inspect
        ) {
            if let Some(note) = self.device_note() {
                text.lines.push(Line::raw(""));
                text.lines.push(note);
            }
        }
        text
    }

    fn newdisk_content(&self) -> Text<'static> {
        self.stub(
            "Create a blank disk image - the home for all `new` operations.",
            &[
                "Floppy   - FAT/HFS + the retro filesystems at floppy geometry.",
                "Volume   - a bare superfloppy (NTFS, ext4, HFS+, ...) any size.",
                "Hard disk- a partition-table-wrapped bootable image (x68k, SGI).",
                "CD-ROM   - an optical image (via the Optical tab).",
            ],
            "M-create - drives `new floppy/volume/hd` + `optical new`.",
        )
    }

    /// A placeholder body: one-line summary, bullet steps, and the milestone /
    /// shared-runner note, so the intended content is visible before it lands.
    fn stub(&self, summary: &str, bullets: &[&str], milestone: &str) -> Text<'static> {
        let mut lines = vec![Line::raw(summary.to_string()), Line::raw("")];
        for b in bullets {
            lines.push(Line::from(vec![
                Span::styled("  - ", self.palette.accent()),
                Span::raw(b.to_string()),
            ]));
        }
        lines.push(Line::raw(""));
        lines.push(Line::styled(milestone.to_string(), self.palette.dim()));
        Text::from(lines)
    }

    /// Rendered as scrollable text only when drilled into a disk (the list is a
    /// widget). Shows the real device + mounted-partition summary.
    fn inspect_content(&self) -> Text<'static> {
        // No disk selected yet. The disk list is the selectable widget; this
        // text is what the scrollable pane shows behind it.
        Text::from(
            "Select a disk and press Enter to see its partitions, or `o` to open a file/backup.",
        )
    }

    fn settings_content(&self) -> Text<'static> {
        // The updater is feature-gated; report whether it's built in.
        let updater = if cfg!(feature = "tui-update") {
            "built in (rb-cli update available)"
        } else {
            "not built (rebuild with --features tui-update)"
        };
        Text::from(vec![
            Line::raw("Settings and maintenance."),
            Line::raw(""),
            Line::from(vec![
                Span::styled("Platform:      ", self.palette.accent()),
                Span::raw(format!(
                    "{} / {}",
                    std::env::consts::OS,
                    std::env::consts::ARCH
                )),
            ]),
            Line::from(vec![
                Span::styled("Privilege:     ", self.palette.accent()),
                Span::styled(
                    if self.elevated {
                        format!("{} - device operations enabled", self.privilege_label())
                    } else {
                        "normal user - device operations disabled (need elevation)".to_string()
                    },
                    if self.elevated {
                        self.palette.accent()
                    } else {
                        self.palette.warn()
                    },
                ),
            ]),
            Line::from(vec![
                Span::styled("Color:         ", self.palette.accent()),
                Span::raw(if self.palette.color {
                    "on"
                } else {
                    "off (NO_COLOR)"
                }),
            ]),
            Line::from(vec![
                Span::styled("Borders:       ", self.palette.accent()),
                Span::raw(if self.border == ASCII_BORDER {
                    "ASCII fallback"
                } else {
                    "Unicode rounded"
                }),
            ]),
            Line::from(vec![
                Span::styled("Updater:       ", self.palette.accent()),
                Span::raw(updater),
            ]),
            Line::raw(""),
            Line::raw("Will hold: update check (opt-in), recent files, defaults."),
            Line::styled(
                "M10 - reuses update::UpdateConfig; updater via `rb-cli update`.".to_string(),
                self.palette.dim(),
            ),
        ])
    }

    fn content_line_count(&self) -> u16 {
        self.tab_content().lines.len() as u16
    }

    // --- shared widgets --------------------------------------------------

    fn pane_block(&self, title: &str, focused: bool) -> Block<'static> {
        let border_style = if focused {
            self.palette.accent()
        } else {
            self.palette.dim()
        };
        let title_style = if focused {
            self.palette
                .accent()
                .add_modifier(Modifier::REVERSED | Modifier::BOLD)
        } else {
            self.palette.dim().add_modifier(Modifier::BOLD)
        };
        Block::default()
            .borders(Borders::ALL)
            .border_set(self.border)
            .border_style(border_style)
            .title(Span::styled(format!(" {title} "), title_style))
    }
}

/// Heuristic: does this content look like text (worth showing verbatim) rather
/// than binary (better as a hex dump)? A NUL byte or a high ratio of control
/// characters means binary.
fn looks_textual(bytes: &[u8]) -> bool {
    let sample = &bytes[..bytes.len().min(8192)];
    if sample.contains(&0) {
        return false;
    }
    let ctrl = sample
        .iter()
        .filter(|&&b| b < 0x20 && !matches!(b, b'\t' | b'\n' | b'\r'))
        .count();
    (ctrl as f64) / (sample.len() as f64) < 0.15
}

/// Read a disk's partition table without elevation, for the Inspect screen's fallback.
///
/// Returns the open error rather than an empty list: a raw device is unreadable
/// without elevation, and swallowing that produced an image view with no
/// partitions and no explanation - indistinguishable from a genuinely empty
/// disk. `Ok(_)` always has at least one row (a table-less image is reported as
/// one whole-volume row).
fn parse_partitions(path: &std::path::Path, total: u64) -> std::io::Result<Vec<PartRow>> {
    let Some(table) = crate::controller::inspect::read_table_unelevated(path)? else {
        // No detectable table: a raw single-volume image (superfloppy).
        return Ok(vec![PartRow {
            label: "Whole volume (no partition table)".to_string(),
            fs_hint: String::new(),
            size: total,
        }]);
    };
    if table.partitions.is_empty() {
        return Ok(vec![PartRow {
            label: format!("Whole volume ({})", table.type_name),
            fs_hint: table.type_name,
            size: total,
        }]);
    }
    Ok(table
        .partitions
        .into_iter()
        .enumerate()
        .map(|(k, p)| {
            let n = (k + 1) as u32;
            PartRow {
                label: format!("#{n}  {}", p.type_name),
                fs_hint: p.type_string.unwrap_or(p.type_name),
                size: p.size_bytes,
            }
        })
        .collect())
}

/// Rows a full-screen list shows at once: the PageUp / PageDown step.
fn page_step() -> usize {
    let rows = crossterm::terminal::size().map(|(_, r)| r).unwrap_or(24);
    (rows.saturating_sub(6) as usize).max(1)
}

/// A bordered pane block with the given (already-resolved) palette + border set.
/// The free-function form so shared components (e.g. [`FilePicker`]) can build
/// blocks without an `App` receiver.
fn pane_block_with(title: &str, pal: Palette, border: BorderSet) -> Block<'static> {
    Block::default()
        .borders(Borders::ALL)
        .border_set(border)
        .border_style(pal.accent())
        .title(Span::styled(
            format!(" {title} "),
            pal.accent()
                .add_modifier(Modifier::REVERSED | Modifier::BOLD),
        ))
}

/// The final path component (file / folder name) of a path string.
fn basename(path: &str) -> String {
    std::path::Path::new(path)
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.to_string())
}

/// Parse a `rb-cli` verb line and dispatch it (the command palette). Reuses the
/// same `no_binary_name` clap wrapper the interactive `terminal` REPL uses.
fn run_palette_command(input: &str) -> anyhow::Result<()> {
    use clap::Parser;
    #[derive(Parser)]
    #[command(name = "", no_binary_name = true, disable_help_subcommand = true)]
    struct PaletteCli {
        #[command(subcommand)]
        command: crate::cli::Command,
    }
    let argv = shell_words::split(input)?;
    if argv.is_empty() {
        return Ok(());
    }
    let parsed = PaletteCli::try_parse_from(&argv)?;
    crate::cli::dispatch(parsed.command)
}

/// Stderr pointed at the null device until dropped, so a worker's advisories cannot scribble on the screen.
#[cfg(unix)]
struct MutedStderr(libc::c_int);

#[cfg(unix)]
impl MutedStderr {
    fn new() -> Option<Self> {
        use std::os::unix::io::AsRawFd;
        let devnull = std::fs::OpenOptions::new()
            .write(true)
            .open("/dev/null")
            .ok()?;
        // SAFETY: plain descriptor duplication; the saved copy is restored and closed on drop.
        unsafe {
            let saved = libc::dup(libc::STDERR_FILENO);
            if saved < 0 {
                return None;
            }
            libc::dup2(devnull.as_raw_fd(), libc::STDERR_FILENO);
            Some(MutedStderr(saved))
        }
    }
}

#[cfg(unix)]
impl Drop for MutedStderr {
    fn drop(&mut self) {
        // SAFETY: `self.0` is the descriptor `new` saved and nothing else closes it.
        unsafe {
            libc::dup2(self.0, libc::STDERR_FILENO);
            libc::close(self.0);
        }
    }
}

#[cfg(not(unix))]
struct MutedStderr;

#[cfg(not(unix))]
impl MutedStderr {
    fn new() -> Option<Self> {
        Some(MutedStderr)
    }
}

/// For task bodies on a worker: silence stderr only, since stdout carries the TUI's own drawing.
#[cfg(unix)]
fn with_stderr_only_suppressed<T>(f: impl FnOnce() -> T) -> T {
    use std::os::unix::io::AsRawFd;
    unsafe {
        let saved_err = libc::dup(libc::STDERR_FILENO);
        if saved_err < 0 {
            return f();
        }
        match std::fs::OpenOptions::new().write(true).open("/dev/null") {
            Ok(devnull) => {
                libc::dup2(devnull.as_raw_fd(), libc::STDERR_FILENO);
                let out = f();
                libc::dup2(saved_err, libc::STDERR_FILENO);
                libc::close(saved_err);
                out
            }
            Err(_) => {
                libc::close(saved_err);
                f()
            }
        }
    }
}

#[cfg(not(unix))]
fn with_stderr_only_suppressed<T>(f: impl FnOnce() -> T) -> T {
    f()
}

fn expand_tilde(raw: &str) -> std::path::PathBuf {
    if let Some(rest) = raw.strip_prefix("~/") {
        if let Some(home) = dirs::home_dir() {
            return home.join(rest);
        }
    } else if raw == "~" {
        if let Some(home) = dirs::home_dir() {
            return home;
        }
    }
    std::path::PathBuf::from(raw)
}

/// Center a `width` x `height` rect inside `area` (used for the help overlay).
fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let vertical = Layout::vertical([Constraint::Length(height)])
        .flex(Flex::Center)
        .split(area);
    Layout::horizontal([Constraint::Length(width)])
        .flex(Flex::Center)
        .split(vertical[0])[0]
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `g`/`G` is the deliberate exception - the vim top/bottom pair. It is
    /// kept *and* spelled out in the hint tables, rather than left to be
    /// discovered.
    #[test]
    fn the_vim_top_bottom_pair_is_listed() {
        for table in [
            HINTS_TAB,
            HINTS_INSPECT_DISKS,
            inspect::HINTS_INSPECT_SOURCE,
        ] {
            assert!(
                table.iter().any(|h| h.keys == "g/G"),
                "a screen using g/G must say so in its key list"
            );
        }
    }

    /// The footer clips to the terminal, and never at the cost of the one hint
    /// that leads to all the others.
    #[test]
    fn the_footer_always_offers_the_key_list() {
        for width in [12usize, 24, 40, 80, 200] {
            let strip = footer_strip(HINTS_EDITOR, width);
            assert!(
                strip.contains("?=keys"),
                "width {width} dropped the route to the full list: {strip:?}"
            );
        }
        // Wide enough to show the save key.
        assert!(footer_strip(HINTS_EDITOR, 200).contains("F2"));
    }

    /// F2 saves, not Ctrl-S: Ctrl-S is XOFF and freezes a serial console, which
    /// is exactly where this editor is worth having.
    #[test]
    fn the_editor_lists_save_and_line_endings() {
        assert!(HINTS_EDITOR.iter().any(|h| h.keys == "F2"));
        assert!(HINTS_EDITOR.iter().any(|h| h.keys == "F3"));
        assert!(!HINTS_EDITOR.iter().any(|h| h.keys.contains("Ctrl-S")));
    }

    /// A disk-list entry pointing at a real path, so the open path can run.
    fn fake_disk(path: &std::path::Path, size: u64) -> DiskDevice {
        DiskDevice {
            name: "test".into(),
            path: path.to_path_buf(),
            size_bytes: size,
            is_removable: false,
            is_read_only: false,
            is_system: false,
            bus_protocol: "test".into(),
            media_name: "Test Disk".into(),
            partitions: Vec::new(),
        }
    }

    /// Build an MBR disk with a FAT16 and an ext partition - the shape of an
    /// ordinary Linux system disk (ESP + root).
    fn mbr_with_ext(dir: &std::path::Path) -> std::path::PathBuf {
        const START: u32 = 2048;
        let vol = crate::controller::test_fixtures::blank_ext4(16 * 1024 * 1024, "ROOT");
        let img = dir.join("disk.img");
        let mut bytes = vec![0u8; START as usize * 512 + vol.len()];
        bytes[START as usize * 512..].copy_from_slice(&vol);
        let e = 0x1BE;
        bytes[e + 4] = 0x83; // Linux
        bytes[e + 8..e + 12].copy_from_slice(&START.to_le_bytes());
        bytes[e + 12..e + 16].copy_from_slice(&((vol.len() / 512) as u32).to_le_bytes());
        bytes[510] = 0x55;
        bytes[511] = 0xAA;
        std::fs::write(&img, &bytes).expect("write img");
        img
    }

    /// A **single** Enter on a disk must reach its partition table, with the
    /// disk's own facts carried along for the header.
    ///
    /// The disk list used to dead-end: the first Enter opened a pane listing
    /// only the partitions the *OS* had mounted, and a second Enter did nothing
    /// whatsoever because `activate` required `detail.is_none()` - while the
    /// footer still advertised "Enter Open". An unmounted volume was therefore
    /// unreachable, and a mounted one could only be read about, never opened.
    /// Wiring the second press up fixed the dead end but left the extra step,
    /// which nothing on screen mentioned; the two screens described the same
    /// disk, so they are now one. This asserts the count of keypresses, because
    /// that is the part a user notices.
    #[test]
    fn one_enter_on_a_disk_reaches_its_partition_table() {
        let dir = tempfile::tempdir().expect("tempdir");
        let img = mbr_with_ext(dir.path());
        let size = std::fs::metadata(&img).unwrap().len();

        let mut app = App::new_on(DEFAULT_TAB);
        app.set_test_devices(vec![fake_disk(&img, size)]);
        app.selection = 0;
        assert!(app.inspect_list_active());

        app.activate(); // one press
        inspect_settle(&mut app);

        let s = app.inspect.as_ref().unwrap();
        assert!(s.source_open(), "a single Enter should open the disk");
        assert_eq!(
            s.ctl.device_slot(),
            Some(0),
            "opened through the controller"
        );
        assert_eq!(s.ctl.partitions().len(), 1, "the ext partition is listed");
        assert!(
            s.disk.is_some(),
            "the disk's hardware facts must come along so the header can show \
             them instead of needing a screen of their own"
        );
    }

    /// The Backup screen runs a backup through the controller and reports its end.
    #[test]
    fn the_backup_screen_backs_up_a_disk_through_the_controller() {
        let dir = tempfile::tempdir().expect("tempdir");
        let out = tempfile::tempdir().expect("tempdir");
        let img = mbr_with_ext(dir.path());
        let size = std::fs::metadata(&img).unwrap().len();

        let mut app = App::new_on(BACKUP_TAB);
        app.set_test_devices(vec![fake_disk(&img, size)]);
        assert!(app.handle_backup_key(KeyCode::Char('d')));
        assert!(app.handle_backup_key(KeyCode::Enter));
        let b = app.backup.as_mut().unwrap();
        assert!(b.step == backup::BackupStep::Config);
        assert!(!b.ctl.name().is_empty());
        b.out_dir = out.path().display().to_string();
        b.focus = backup::Field::Start;
        assert!(app.handle_backup_key(KeyCode::Enter));
        assert!(app.progress.is_some());
        let b = run_backup_to_end(&mut app);
        assert_eq!(b.result(), Some(("Backup complete.".to_string(), false)));
        let folder = out.path().join(b.ctl.name());
        assert!(folder.join("metadata.json").exists());
    }

    /// Type `text` and press Enter (a picker's path field, or a form row).
    fn type_line(app: &mut App, text: &str) {
        for c in text.chars() {
            app.handle_backup_key(KeyCode::Char(c));
        }
        app.handle_backup_key(KeyCode::Enter);
    }

    /// The Backup screen with `img` opened through `o` and its preview read.
    fn backup_on_image(img: &std::path::Path) -> App {
        let mut app = App::headless_on(BACKUP_TAB);
        assert!(app.handle_backup_key(KeyCode::Char('o')));
        type_line(&mut app, img.display().to_string().as_str());
        let b = app.backup.as_mut().unwrap();
        assert!(b.step == backup::BackupStep::Config);
        b.ctl.wait_idle();
        app.tick();
        app
    }

    fn focus_on(app: &mut App, field: backup::Field) {
        for _ in 0..40 {
            if app.backup.as_ref().unwrap().focus == field {
                return;
            }
            app.handle_backup_key(KeyCode::Down);
        }
        panic!("no row {field:?} on the form");
    }

    fn run_backup_to_end(app: &mut App) -> &BackupState {
        for _ in 0..6000 {
            app.tick();
            if app.backup.as_ref().unwrap().result().is_some() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        app.backup.as_ref().unwrap()
    }

    /// The partition grid: Space includes, a / n select all / none, c runs Calc min as a job.
    #[test]
    fn the_backup_grid_selects_partitions_and_calcs_min() {
        use crate::controller::backup::MinCell;
        let dir = tempfile::tempdir().expect("tempdir");
        let img = mbr_with_ext(dir.path());
        let mut app = backup_on_image(&img);
        let b = app.backup.as_ref().unwrap();
        assert_eq!(b.ctl.partition_count(), 1);
        assert_eq!(b.ctl.selected_count(), 1);
        let index = b.ctl.rows()[0].index;
        assert_eq!(b.ctl.rows()[0].min, MinCell::CanCalc);
        assert_eq!(b.fields()[0], backup::Field::Partition(index));

        app.handle_backup_key(KeyCode::Up);
        assert_eq!(
            app.backup.as_ref().unwrap().focus,
            backup::Field::Partition(index)
        );
        app.handle_backup_key(KeyCode::Char(' '));
        assert_eq!(app.backup.as_ref().unwrap().ctl.selected_count(), 0);
        app.handle_backup_key(KeyCode::Char('a'));
        assert_eq!(app.backup.as_ref().unwrap().ctl.selected_count(), 1);
        app.handle_backup_key(KeyCode::Char('n'));
        assert_eq!(app.backup.as_ref().unwrap().ctl.selected_count(), 0);
        app.handle_backup_key(KeyCode::Char(' '));
        assert_eq!(app.backup.as_ref().unwrap().ctl.selected_count(), 1);

        app.handle_backup_key(KeyCode::Char('c'));
        let b = app.backup.as_mut().unwrap();
        b.ctl.wait_idle();
        app.tick();
        let b = app.backup.as_ref().unwrap();
        assert!(
            matches!(b.ctl.rows()[0].min, MinCell::Size(_) | MinCell::Empty),
            "{:?}",
            b.ctl.rows()[0].min
        );
    }

    /// Split, sector-by-sector and the CHD codec rows reach the controller.
    #[test]
    fn the_backup_options_reach_the_controller() {
        let dir = tempfile::tempdir().expect("tempdir");
        let img = mbr_with_ext(dir.path());
        let mut app = backup_on_image(&img);

        focus_on(&mut app, backup::Field::SplitMib);
        for _ in 0..5 {
            app.handle_backup_key(KeyCode::Backspace);
        }
        for c in "750".chars() {
            app.handle_backup_key(KeyCode::Char(c));
        }
        let opts = app.backup.as_ref().unwrap().ctl.options().clone();
        assert_eq!(opts.split_size(), Some(750));
        focus_on(&mut app, backup::Field::Split);
        app.handle_backup_key(KeyCode::Char(' '));
        assert_eq!(
            app.backup.as_ref().unwrap().ctl.options().split_size(),
            None
        );

        focus_on(&mut app, backup::Field::SectorBySector);
        app.handle_backup_key(KeyCode::Char(' '));
        let opts = app.backup.as_ref().unwrap().ctl.options().clone();
        assert!(opts.sector_by_sector && !opts.resize_effective());
        // Resize is locked while sector-by-sector is on.
        focus_on(&mut app, backup::Field::Resize);
        app.handle_backup_key(KeyCode::Char(' '));
        assert!(!app.backup.as_ref().unwrap().ctl.options().resize);

        #[cfg(feature = "chd")]
        {
            use crate::controller::app::chd::{hunk_presets, ChdOptionsMode, ChdProfile};
            use crate::controller::backup::CompressionType;
            focus_on(&mut app, backup::Field::Format);
            app.handle_backup_key(KeyCode::Right);
            let b = app.backup.as_ref().unwrap();
            assert_eq!(b.ctl.options().compression, CompressionType::Chd);
            assert!(!b.fields().contains(&backup::Field::Split));
            focus_on(&mut app, backup::Field::ChdMode);
            app.handle_backup_key(KeyCode::Right);
            assert_eq!(
                app.backup.as_ref().unwrap().chd.mode,
                ChdOptionsMode::Custom
            );
            focus_on(&mut app, backup::Field::ChdHunk);
            let before = app.backup.as_ref().unwrap().chd.custom.hunk_size;
            app.handle_backup_key(KeyCode::Right);
            let hunk = app.backup.as_ref().unwrap().chd.custom.hunk_size;
            assert_ne!(hunk, before);
            assert!(hunk_presets(ChdProfile::Hd).contains(&hunk));
            focus_on(&mut app, backup::Field::ChdCodec(0));
            let codec = app.backup.as_ref().unwrap().chd.custom.codecs[0];
            app.handle_backup_key(KeyCode::Right);
            let codec2 = app.backup.as_ref().unwrap().chd.custom.codecs[0];
            assert_ne!(codec, codec2);

            // A hunk off the unit is refused by the form, as the GUI disables Start.
            let out = tempfile::tempdir().expect("tempdir");
            let b = app.backup.as_mut().unwrap();
            b.out_dir = out.path().display().to_string();
            b.chd.custom.hunk_size = 1000;
            b.focus = backup::Field::Format;
            app.handle_backup_key(KeyCode::Char('s'));
            assert!(!app.backup.as_ref().unwrap().busy());
            app.backup.as_mut().unwrap().chd.custom.hunk_size = hunk;
            app.handle_backup_key(KeyCode::Char('s'));
            let b = app.backup.as_mut().unwrap();
            let chd = b
                .ctl
                .options()
                .chd
                .clone()
                .expect("the CHD choice went with Start");
            assert_eq!(chd.hunk_size, hunk);
            assert_eq!(chd.codecs[0], codec2);
            app.handle_backup_key(KeyCode::Esc);
            app.backup.as_mut().unwrap().ctl.wait_finished();
        }
    }

    /// The VHD window: per-partition layout, a Custom size typed in MiB, then the backup runs.
    #[test]
    fn the_backup_vhd_window_sets_sizes_and_starts() {
        use crate::controller::backup::{CompressionType, SizeMode};
        let dir = tempfile::tempdir().expect("tempdir");
        let out = tempfile::tempdir().expect("tempdir");
        let img = mbr_with_ext(dir.path());
        let mut app = backup_on_image(&img);
        app.backup.as_mut().unwrap().out_dir = out.path().display().to_string();
        focus_on(&mut app, backup::Field::Format);
        while app.backup.as_ref().unwrap().ctl.options().compression != CompressionType::Vhd {
            app.handle_backup_key(KeyCode::Right);
        }
        app.handle_backup_key(KeyCode::Char('s'));
        let b = app.backup.as_ref().unwrap();
        assert!(b.ctl.vhd_open());
        assert!(b.ctl.vhd_whole_disk());
        app.handle_backup_key(KeyCode::Right);
        assert!(!app.backup.as_ref().unwrap().ctl.vhd_whole_disk());
        app.handle_backup_key(KeyCode::Down);
        app.handle_backup_key(KeyCode::Left);
        let row = app.backup.as_ref().unwrap().ctl.vhd_rows()[0].clone();
        assert_eq!(row.mode, SizeMode::Custom);
        assert_eq!(row.custom_mib, row.custom_bounds_mib().0);
        app.handle_backup_key(KeyCode::Backspace);
        app.handle_backup_key(KeyCode::Backspace);
        app.handle_backup_key(KeyCode::Char('9'));
        assert_eq!(app.backup.as_ref().unwrap().ctl.vhd_rows()[0].custom_mib, 9);
        app.handle_backup_key(KeyCode::Down);
        app.handle_backup_key(KeyCode::Enter);
        assert!(!app.backup.as_ref().unwrap().ctl.vhd_open());
        assert!(app.progress.is_some());
        let b = run_backup_to_end(&mut app);
        assert_eq!(b.result(), Some(("Backup complete.".to_string(), false)));
        assert!(out.path().join(b.ctl.name()).join("metadata.json").exists());
    }

    fn backup_screen(app: &App) -> String {
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

    /// The form scrolls to its focused row in a small terminal; the VHD window draws over it.
    #[test]
    fn the_backup_form_and_windows_draw_in_a_small_terminal() {
        use crate::controller::backup::CompressionType;
        let dir = tempfile::tempdir().expect("tempdir");
        let img = mbr_with_ext(dir.path());
        let mut app = backup_on_image(&img);
        let text = backup_screen(&app);
        assert!(
            text.contains("Partitions") && text.contains("Output:"),
            "{text}"
        );
        focus_on(&mut app, backup::Field::Start);
        let text = backup_screen(&app);
        assert!(text.contains("[ Start backup ]"), "{text}");
        eprintln!("{text}");
        focus_on(&mut app, backup::Field::Format);
        while app.backup.as_ref().unwrap().ctl.options().compression != CompressionType::Vhd {
            app.handle_backup_key(KeyCode::Right);
        }
        app.backup.as_mut().unwrap().out_dir = dir.path().display().to_string();
        app.handle_backup_key(KeyCode::Char('s'));
        let text = backup_screen(&app);
        assert!(
            text.contains("VHD Backup Options") && text.contains("Start VHD backup"),
            "{text}"
        );
        eprintln!("{text}");
        assert_eq!(app.key_context().0, "Backup - VHD options");
    }

    /// `r` opens the Remote Drive window, or says the build has no remote support.
    #[test]
    fn the_backup_remote_drive_opens_or_is_refused() {
        use crate::controller::backup::{BackupController, REMOTE_NOT_BUILT};
        let mut app = App::headless_on(BACKUP_TAB);
        assert!(app.handle_backup_key(KeyCode::Char('r')));
        let open = app.backup.as_ref().unwrap().ctl.remote_dialog_open();
        if BackupController::remote_available() {
            assert!(open);
            for c in "127.0.0.1".chars() {
                app.handle_backup_key(KeyCode::Char(c));
            }
            assert_eq!(
                app.backup.as_ref().unwrap().ctl.remote_address(),
                "127.0.0.1"
            );
            app.handle_backup_key(KeyCode::Esc);
            assert!(!app.backup.as_ref().unwrap().ctl.remote_dialog_open());
        } else {
            assert!(!open);
            let b = app.backup.as_ref().unwrap();
            assert!(b.log.iter().any(|l| l == REMOTE_NOT_BUILT));
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
        let mut app = App::new_on(BACKUP_TAB);
        app.bulk = Some(BulkState::default());
        let b = app.bulk.as_mut().unwrap();
        b.source = src.path().display().to_string();
        b.out_dir = out.path().display().to_string();
        b.step = BulkStep::Config;
        b.act(crate::controller::bulk_convert::BulkConvertAction::Picked {
            purpose: crate::controller::bulk_convert::purpose::SOURCE,
            path: Some(src.path().to_path_buf()),
        });
        app.bulk_rescan();
        app.bulk.as_mut().unwrap().ctl.wait_idle();
        app.tick();
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

    /// A zstd backup of a 2 MiB blank FAT image, made through the Backup controller.
    fn restore_fixture(dir: &std::path::Path) -> (std::path::PathBuf, Vec<u8>) {
        use crate::controller::backup::{BackupAction, BackupController, BackupServices};
        use crate::controller::Controller as _;
        let img = dir.join("vol.img");
        let bytes = crate::controller::test_fixtures::blank_fat(2 * 1024 * 1024, Some("T"));
        std::fs::write(&img, &bytes).unwrap();
        let mut b = BackupController::new(BackupServices {
            jobs: crate::controller::backup::JobRegistry::new(),
            notify: Arc::new(crate::controller::backup::NoNotify),
        });
        b.apply(BackupAction::OpenImage {
            path: img,
            rename: false,
        });
        b.wait_idle();
        b.apply(BackupAction::SetDestination(Some(dir.to_path_buf())));
        b.apply(BackupAction::SetName("bk".into()));
        b.apply(BackupAction::SetResize(false));
        b.apply(BackupAction::Start(None));
        b.wait_finished();
        (dir.join("bk"), bytes)
    }

    fn restore_keys(app: &mut App, text: &str) {
        for c in text.chars() {
            app.handle_restore_key(KeyCode::Char(c));
        }
    }

    /// Let the controllers' jobs end and the screen follow them.
    fn restore_settle(app: &mut App) {
        let r = app.restore.as_mut().unwrap();
        r.ctl.wait_idle();
        r.writer.wait_idle();
        app.tick();
    }

    fn restore_focus(app: &mut App, field: restore::Field) {
        for _ in 0..60 {
            if app.restore.as_ref().unwrap().focus == field {
                return;
            }
            app.handle_restore_key(KeyCode::Down);
        }
        panic!("no row {field:?} on the form");
    }

    fn restore_wfocus(app: &mut App, row: writer::WRow) {
        for _ in 0..30 {
            if app.restore.as_ref().unwrap().win.sel == row {
                return;
            }
            app.handle_restore_key(KeyCode::Down);
        }
        panic!("no row {row:?} in the writer");
    }

    fn restore_run_to_end(app: &mut App) -> (String, bool) {
        for _ in 0..6000 {
            app.tick();
            if let Some(res) = app.restore.as_ref().unwrap().result() {
                if !app.restore.as_ref().unwrap().busy() {
                    return res;
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        panic!("the run did not end");
    }

    /// The Restore screen with the fixture backup opened through `o` and the picker.
    fn restore_on_backup(dir: &std::path::Path) -> (App, std::path::PathBuf, Vec<u8>) {
        let (folder, bytes) = restore_fixture(dir);
        let mut app = App::headless_on(BACKUP_TAB + 1);
        assert!(app.handle_restore_key(KeyCode::Char('o')));
        restore_keys(&mut app, folder.display().to_string().as_str());
        app.handle_restore_key(KeyCode::Enter);
        restore_settle(&mut app);
        assert!(app.restore.as_ref().unwrap().ctl.has_backup());
        (app, folder, bytes)
    }

    fn restore_screen(app: &App) -> String {
        backup_screen(app)
    }

    /// A disk-image file standing in for a device, with one MBR partition at LBA 2048.
    fn restore_fake_disk(app: &mut App, path: &std::path::Path, sectors: u32) -> Vec<u8> {
        let mut d = vec![0u8; 8 << 20];
        d[0x1BE + 4] = 0x06;
        d[0x1BE + 8..0x1BE + 12].copy_from_slice(&2048u32.to_le_bytes());
        d[0x1BE + 12..0x1BE + 16].copy_from_slice(&sectors.to_le_bytes());
        d[510] = 0x55;
        d[511] = 0xAA;
        std::fs::write(path, &d).unwrap();
        app.set_test_devices(vec![DiskDevice {
            name: "fake".into(),
            path: path.to_path_buf(),
            size_bytes: 8 << 20,
            is_removable: true,
            is_read_only: false,
            is_system: false,
            bus_protocol: "USB".into(),
            media_name: "Fake".into(),
            partitions: Vec::new(),
        }]);
        d
    }

    /// Full Disk: sizes, alignment and free space reach the controller; the run restores bytes.
    #[test]
    fn the_restore_full_disk_form_restores_a_backup() {
        use crate::controller::restore::{AlignmentChoice, SizeMode};
        use restore::Field;
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut app, _, bytes) = restore_on_backup(dir.path());
        let out = dir.path().join("bk_restored.img");
        let r = app.restore.as_ref().unwrap();
        assert_eq!(r.full_target, out.display().to_string());
        let index = r.ctl.size_rows()[0].index;
        // The target defaults to a device; Space switches it to the image file.
        restore_focus(&mut app, Field::TargetKind);
        app.handle_restore_key(KeyCode::Char(' '));
        let r = app.restore.as_ref().unwrap();
        assert_eq!(
            r.ctl
                .target_image(crate::controller::restore::TargetSlot::FullDisk),
            Some(out.as_path())
        );
        // Size: Right steps through the row's modes; Custom starts at the original and types.
        restore_focus(&mut app, Field::Size(index));
        while app.restore.as_ref().unwrap().ctl.size_rows()[0].mode != SizeMode::Custom {
            assert!(app.handle_restore_key(KeyCode::Right));
        }
        assert_eq!(
            app.restore.as_ref().unwrap().ctl.size_rows()[0].custom_mib,
            2
        );
        app.handle_restore_key(KeyCode::Char('3'));
        assert_eq!(
            app.restore.as_ref().unwrap().ctl.size_rows()[0].custom_mib,
            23
        );
        while app.restore.as_ref().unwrap().ctl.size_rows()[0].mode != SizeMode::Original {
            app.handle_restore_key(KeyCode::Left);
        }
        // Alignment: Custom opens its sector row.
        restore_focus(&mut app, Field::Alignment);
        while app.restore.as_ref().unwrap().ctl.alignment().0 != AlignmentChoice::Custom {
            app.handle_restore_key(KeyCode::Right);
        }
        restore_focus(&mut app, Field::AlignCustom);
        app.handle_restore_key(KeyCode::Backspace);
        assert_eq!(app.restore.as_ref().unwrap().ctl.alignment().1, 204);
        restore_focus(&mut app, Field::Alignment);
        app.handle_restore_key(KeyCode::Right);
        assert_eq!(
            app.restore.as_ref().unwrap().ctl.alignment().0,
            AlignmentChoice::Original
        );
        // Free space: the toggle opens MiB and extend-last; the screen shows the preview.
        restore_focus(&mut app, Field::Expand);
        app.handle_restore_key(KeyCode::Char(' '));
        restore_focus(&mut app, Field::ExpandMib);
        restore_keys(&mut app, "4");
        restore_focus(&mut app, Field::ExtendLast);
        app.handle_restore_key(KeyCode::Char(' '));
        let plan = app.restore.as_ref().unwrap().ctl.expand();
        assert!(plan.enabled && plan.extend_last && plan.free_mib == 4);
        let text = restore_screen(&app);
        assert!(text.contains("After:"), "{text}");
        restore_focus(&mut app, Field::Expand);
        app.handle_restore_key(KeyCode::Char(' '));
        // Start asks; `y` runs it.
        app.handle_restore_key(KeyCode::Char('s'));
        // The question's own text: the box wraps it by the temp path's length.
        let Some(ask::Ask::Confirm(_, spec)) = &app.restore.as_ref().unwrap().ask else {
            panic!("Start asks first");
        };
        assert!(
            spec.message.contains("permanently overwritten"),
            "{}",
            spec.message
        );
        assert!(restore_screen(&app).contains("permanently"));
        app.handle_restore_key(KeyCode::Char('y'));
        assert!(app.progress.is_some());
        let (res, err) = restore_run_to_end(&mut app);
        assert_eq!((res.as_str(), err), ("Restore complete.", false));
        assert_eq!(std::fs::read(&out).unwrap(), bytes);
        app.handle_restore_key(KeyCode::Esc);
        assert_eq!(
            app.restore.as_ref().unwrap().step,
            restore::RestoreStep::Form
        );
    }

    /// New Disk: a backup partition onto an image under a fresh bootable MBR.
    #[test]
    fn the_restore_new_disk_mode_writes_a_fresh_table() {
        use crate::controller::restore::{NewTableType, RestoreMode, TargetSlot};
        use restore::Field;
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut app, _, _) = restore_on_backup(dir.path());
        restore_focus(&mut app, Field::Mode);
        while app.restore.as_ref().unwrap().ctl.mode() != RestoreMode::NewDisk {
            app.handle_restore_key(KeyCode::Right);
        }
        restore_focus(&mut app, Field::SourcePartition);
        app.handle_restore_key(KeyCode::Char(' '));
        assert!(app
            .restore
            .as_ref()
            .unwrap()
            .ctl
            .source_partition()
            .is_some());
        restore_focus(&mut app, Field::TargetKind);
        app.handle_restore_key(KeyCode::Char(' '));
        restore_focus(&mut app, Field::TargetImage);
        let out = dir.path().join("new.img");
        restore_keys(&mut app, out.display().to_string().as_str());
        assert_eq!(
            app.restore
                .as_ref()
                .unwrap()
                .ctl
                .target_image(TargetSlot::NewDisk),
            Some(out.as_path())
        );
        restore_focus(&mut app, Field::NdTable);
        app.handle_restore_key(KeyCode::Right);
        assert_eq!(
            app.restore.as_ref().unwrap().ctl.new_disk().table,
            NewTableType::Gpt
        );
        app.handle_restore_key(KeyCode::Left);
        restore_focus(&mut app, Field::NdBootable);
        app.handle_restore_key(KeyCode::Char(' '));
        assert!(app.restore.as_ref().unwrap().ctl.new_disk().bootable);
        let text = restore_screen(&app);
        assert!(text.contains("starts at LBA 2048"), "{text}");
        app.handle_restore_key(KeyCode::Char('s'));
        app.handle_restore_key(KeyCode::Char('y'));
        let (res, err) = restore_run_to_end(&mut app);
        assert_eq!((res.as_str(), err), ("Restore complete.", false), "{res}");
        let d = std::fs::read(&out).unwrap();
        assert_eq!(&d[510..512], &[0x55, 0xAA]);
        assert_eq!(d[0x1BE], 0x80);
        assert_eq!(&d[0x1BE + 8..0x1BE + 12], &2048u32.to_le_bytes());
    }

    /// Single Partition: pick the device, scan it, pick its partition; Start names it.
    #[test]
    fn the_restore_single_partition_mode_scans_and_picks_a_partition() {
        use crate::controller::restore::RestoreMode;
        use restore::Field;
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut app, _, _) = restore_on_backup(dir.path());
        restore_fake_disk(&mut app, dir.path().join("disk.img").as_path(), 8192);
        restore_focus(&mut app, Field::Mode);
        app.handle_restore_key(KeyCode::Right);
        assert_eq!(
            app.restore.as_ref().unwrap().ctl.mode(),
            RestoreMode::SinglePartition
        );
        restore_focus(&mut app, Field::SourcePartition);
        app.handle_restore_key(KeyCode::Char(' '));
        restore_focus(&mut app, Field::TargetDevice);
        app.handle_restore_key(KeyCode::Enter);
        app.handle_restore_key(KeyCode::Enter);
        restore_focus(&mut app, Field::Scan);
        app.handle_restore_key(KeyCode::Enter);
        restore_settle(&mut app);
        let parts = app.restore.as_ref().unwrap().ctl.target_partitions();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].fits, Some(true));
        restore_focus(&mut app, Field::TargetPartition(parts[0].index));
        app.handle_restore_key(KeyCode::Enter);
        let r = app.restore.as_ref().unwrap();
        assert_eq!(r.ctl.target_partition(), Some(parts[0].index));
        assert_eq!(r.ctl.validate(), Ok(()));
        app.handle_restore_key(KeyCode::Char('s'));
        let Some(ask::Ask::Confirm(_, spec)) = &app.restore.as_ref().unwrap().ask else {
            panic!("Start asks first");
        };
        assert!(
            spec.message.contains("will NOT be modified"),
            "{}",
            spec.message
        );
        // `n` leaves the device alone.
        app.handle_restore_key(KeyCode::Char('n'));
        let r = app.restore.as_ref().unwrap();
        assert!(r.ask.is_none() && !r.busy());
    }

    /// Write Image File: the writer window wraps a superfloppy into an image file, then writes
    /// into one partition of a disk-image "device" behind its typed name.
    #[test]
    fn the_restore_write_image_mode_writes_through_the_writer() {
        use crate::controller::restore::RestoreMode;
        use restore::Field;
        use writer::WRow;
        let dir = tempfile::tempdir().expect("tempdir");
        let vol = dir.path().join("vol.img");
        let bytes = crate::controller::test_fixtures::blank_fat(1024 * 1024, Some("T"));
        std::fs::write(&vol, &bytes).unwrap();
        let mut app = App::headless_on(BACKUP_TAB + 1);
        restore_focus(&mut app, Field::Mode);
        while app.restore.as_ref().unwrap().ctl.mode() != RestoreMode::WriteImage {
            app.handle_restore_key(KeyCode::Right);
        }
        restore_focus(&mut app, Field::WriteSource);
        app.handle_restore_key(KeyCode::Enter);
        restore_keys(&mut app, vol.display().to_string().as_str());
        app.handle_restore_key(KeyCode::Enter);
        restore_settle(&mut app);
        assert!(restore_screen(&app).contains("the writer can wrap it"));
        restore_focus(&mut app, Field::WriteTarget);
        app.handle_restore_key(KeyCode::Enter);
        restore_settle(&mut app);
        assert!(app.restore.as_ref().unwrap().writer.is_open());
        assert_eq!(app.key_context().0, "Restore - write image");
        // An image-file target, wrapped in a fresh MBR.
        let out = dir.path().join("out.img");
        restore_wfocus(&mut app, WRow::Image);
        restore_keys(&mut app, out.display().to_string().as_str());
        app.handle_restore_key(KeyCode::Enter);
        restore_wfocus(&mut app, WRow::Synthesize);
        app.handle_restore_key(KeyCode::Char(' '));
        restore_wfocus(&mut app, WRow::TypeByte);
        restore_keys(&mut app, "0e");
        assert_eq!(app.restore.as_ref().unwrap().writer.form().type_byte, 0x0E);
        let text = restore_screen(&app);
        assert!(text.contains("Synthesize partition table"), "{text}");
        app.handle_restore_key(KeyCode::Char('w'));
        let r = app.restore.as_ref().unwrap();
        assert!(
            matches!(r.ask, Some(ask::Ask::Confirm(..))),
            "{:?} {:?}",
            r.writer.error(),
            r.writer.target_image()
        );
        app.handle_restore_key(KeyCode::Char('y'));
        let (res, err) = restore_run_to_end(&mut app);
        assert!(!err && res.starts_with("Wrote "), "{res}");
        let d = std::fs::read(&out).unwrap();
        assert_eq!(&d[510..512], &[0x55, 0xAA]);
        assert_eq!(d[0x1BE + 4], 0x0E);
        // The volume follows at LBA 2048, its VBR's hidden sectors patched to match.
        assert_eq!(&d[(1 << 20) + 512..2 << 20], &bytes[512..]);
        assert_eq!(
            &d[(1 << 20) + 0x1C..(1 << 20) + 0x20],
            &2048u32.to_le_bytes()
        );
        app.handle_restore_key(KeyCode::Esc);
        assert!(!app.restore.as_ref().unwrap().writer.is_open());

        // Into the partition of a stand-in device; the name must be typed exactly.
        let disk = dir.path().join("disk.img");
        let before = restore_fake_disk(&mut app, disk.as_path(), 4096);
        restore_focus(&mut app, Field::WriteTarget);
        app.handle_restore_key(KeyCode::Enter);
        restore_settle(&mut app);
        restore_wfocus(&mut app, WRow::Device);
        app.handle_restore_key(KeyCode::Enter);
        app.handle_restore_key(KeyCode::Enter);
        restore_settle(&mut app);
        restore_wfocus(&mut app, WRow::Region);
        app.handle_restore_key(KeyCode::Right);
        assert!(app
            .restore
            .as_ref()
            .unwrap()
            .writer
            .target_partition()
            .is_some());
        restore_wfocus(&mut app, WRow::Write);
        app.handle_restore_key(KeyCode::Enter);
        assert!(matches!(
            app.restore.as_ref().unwrap().ask,
            Some(ask::Ask::Text { .. })
        ));
        restore_keys(&mut app, "wrong");
        app.handle_restore_key(KeyCode::Enter);
        assert!(app.restore.as_ref().unwrap().ask.is_some());
        for _ in 0..5 {
            app.handle_restore_key(KeyCode::Backspace);
        }
        let name = app.restore.as_ref().unwrap().writer.device_name().unwrap();
        restore_keys(&mut app, name.as_str());
        app.handle_restore_key(KeyCode::Enter);
        let (res, err) = restore_run_to_end(&mut app);
        assert!(!err && res.starts_with("Wrote "), "{res}");
        let after = std::fs::read(&disk).unwrap();
        assert_eq!(&after[..512], &before[..512]);
        assert_eq!(&after[1 << 20..2 << 20], &bytes[..]);
    }

    /// The remote target turns on and takes a host only in a build with `remote`.
    #[test]
    fn the_restore_remote_target_opens_or_is_absent() {
        use crate::controller::restore::RestoreController;
        use restore::Field;
        let mut app = App::headless_on(BACKUP_TAB + 1);
        app.handle_restore_key(KeyCode::Down);
        let fields = app.restore.as_ref().unwrap().fields();
        if !RestoreController::remote_available() {
            assert!(!fields.contains(&Field::Remote));
            return;
        }
        restore_focus(&mut app, Field::Remote);
        app.handle_restore_key(KeyCode::Char(' '));
        assert!(app.restore.as_ref().unwrap().ctl.remote_active());
        restore_focus(&mut app, Field::RemoteHost);
        restore_keys(&mut app, "127.0.0.1");
        assert_eq!(
            app.restore.as_ref().unwrap().ctl.remote_address(),
            "127.0.0.1"
        );
        assert!(app
            .restore
            .as_ref()
            .unwrap()
            .ctl
            .remote_target_label()
            .is_none());
    }

    /// Every mode's form and the writer window draw in an 80x24 terminal.
    #[test]
    fn the_restore_forms_draw_in_a_small_terminal() {
        use crate::controller::restore::RestoreMode;
        use restore::Field;
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut app, _, _) = restore_on_backup(dir.path());
        restore_focus(&mut app, Field::Start);
        let text = restore_screen(&app);
        assert!(text.contains("[ Start restore ]"), "{text}");
        assert_eq!(app.key_context().0, "Restore");
        restore_focus(&mut app, Field::Mode);
        for mode in [
            RestoreMode::SinglePartition,
            RestoreMode::NewDisk,
            RestoreMode::WriteImage,
        ] {
            app.handle_restore_key(KeyCode::Right);
            assert_eq!(app.restore.as_ref().unwrap().ctl.mode(), mode);
            let text = restore_screen(&app);
            assert!(text.contains("Mode:"), "{text}");
        }
        // Build Disk is not on the TUI's mode list; Right wraps to Full Disk.
        app.handle_restore_key(KeyCode::Right);
        assert_eq!(
            app.restore.as_ref().unwrap().ctl.mode(),
            RestoreMode::FullDisk
        );
    }

    /// The file picker must accept a raw device path.
    ///
    /// `PickKind::File` gated on `is_file()`, which is false for a block device,
    /// so every picker in the TUI - Commander's image open included - refused
    /// `/dev/sda` with "No such file". That single line was what stopped
    /// Commander from opening a raw disk partition at all.
    #[test]
    fn the_file_picker_accepts_a_device_and_still_rejects_a_directory() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file = dir.path().join("img.bin");
        std::fs::write(&file, b"x").unwrap();

        let mut p = FilePicker::new(PickKind::File, "Open");
        assert!(
            p.validate(file.clone()).is_some(),
            "a regular file must still be accepted"
        );

        // A directory is the wrong kind and must stay rejected, with a message
        // that says so rather than claiming the path is absent.
        let mut p = FilePicker::new(PickKind::File, "Open");
        assert!(p.validate(dir.path().to_path_buf()).is_none());
        let err = p.error.clone().unwrap_or_default();
        assert!(
            err.contains("Not a file"),
            "an existing directory should report the wrong kind, got: {err}"
        );

        // Character devices exist on every platform this runs on and are, like
        // block devices, neither file nor directory - the exact shape that used
        // to be rejected.
        let devnull = std::path::PathBuf::from("/dev/null");
        if devnull.exists() {
            let mut p = FilePicker::new(PickKind::File, "Open");
            assert!(
                p.validate(devnull).is_some(),
                "a device node must be accepted: {:?}",
                p.error
            );
        }
    }

    /// Esc from the merged view goes straight back to the disk list - one level,
    /// since there is no longer an intermediate pane to unwind through.
    /// A task keeps the screen live, applies its result in `tick`, and reports a panic instead of hanging.
    #[test]
    fn a_task_lands_in_tick_and_a_panicking_one_reports_its_failure() {
        let wait = |app: &mut App| {
            app.task.wait();
            app.poll_task();
        };
        let mut app = App::new_on(DEFAULT_TAB);
        app.start_task("Counting", |_| {
            Ok(Box::new(|app: &mut App| app.status = Some("counted".into())) as TaskDone)
        });
        assert!(app.task.is_active());
        app.start_task("Second", |_| Ok(Box::new(|_: &mut App| {}) as TaskDone));
        wait(&mut app);
        assert_eq!(app.status.as_deref(), Some("counted"));
        assert!(!app.task.is_active());

        app.start_task("Exploding", |_| -> anyhow::Result<TaskDone> {
            panic!("boom")
        });
        wait(&mut app);
        let status = app.status.clone().unwrap_or_default();
        assert!(status.starts_with("Exploding failed:"), "{status}");
        assert!(status.contains("boom"), "{status}");
    }

    #[test]
    fn esc_from_the_partition_view_returns_to_the_disk_list() {
        let dir = tempfile::tempdir().expect("tempdir");
        let img = mbr_with_ext(dir.path());
        let size = std::fs::metadata(&img).unwrap().len();

        let mut app = App::new_on(DEFAULT_TAB);
        app.set_test_devices(vec![fake_disk(&img, size)]);
        app.activate();
        inspect_settle(&mut app);
        assert!(!app.inspect_list_active());

        ikey(&mut app, KeyCode::Esc);
        assert!(
            !app.inspect.as_ref().unwrap().ctl.has_source(),
            "Esc should close the source"
        );
        assert!(
            app.inspect_list_active(),
            "and land back on the selectable disk list"
        );
        assert!(!app.should_quit, "Esc must not quit from one level in");
    }

    /// A raw device path is neither a file nor a directory, so the picker's
    /// `is_file()` test sent every one of them to "No such file or directory" -
    /// a flat denial for something plainly present.
    #[test]
    fn opening_a_path_that_is_not_a_regular_file_does_not_claim_it_is_missing() {
        let mut app = App::new_on(DEFAULT_TAB);
        // A directory that is not a backup folder stands in for the shape of the
        // bug without needing a real device: it exists, so whatever the status
        // says, it must not be "No such file or directory".
        let dir = tempfile::tempdir().expect("tempdir");
        app.open_target(dir.path().to_path_buf());
        inspect_settle(&mut app);
        let status = app.status.clone().unwrap_or_default();
        assert!(
            !status.contains("No such file or directory"),
            "an existing path must not be reported as missing: {status}"
        );
    }

    /// An Inspect open is recorded through the app controller, and `o` offers it next time.
    #[test]
    fn an_image_open_is_remembered_through_the_app_controller() {
        let mut app = App::headless_on(tab_index(TabId::Inspect));
        let dir = tempfile::tempdir().unwrap();
        let path = crate::controller::commander::blank_fat_image(dir.path());
        let shown = vec![path.display().to_string()];
        app.open_target(path);
        assert_eq!(app.shell.recent(RecentMode::Inspect), shown);
        inspect_settle(&mut app);
        ikey(&mut app, KeyCode::Esc);
        app.on_key_inner(crossterm::event::KeyEvent::new(
            KeyCode::Char('o'),
            KeyModifiers::NONE,
        ));
        let picker = app.open_picker.as_ref().expect("o opens the picker");
        assert_eq!(picker.recent, shown);
    }

    /// `parse_partitions` must report an unreadable target rather than return an
    /// empty list, which the image view rendered as a disk with no partitions -
    /// indistinguishable from a genuinely empty one.
    #[test]
    fn parse_partitions_reports_an_unreadable_target() {
        let missing = std::path::Path::new("/nonexistent/rb-tui-test/disk.img");
        assert!(
            parse_partitions(missing, 0).is_err(),
            "an unreadable path must surface its error"
        );
    }

    fn ikey(app: &mut App, code: KeyCode) {
        app.on_key(crossterm::event::KeyEvent::new(code, KeyModifiers::NONE));
    }

    fn ikeys(app: &mut App, text: &str) {
        for c in text.chars() {
            ikey(app, KeyCode::Char(c));
        }
    }

    /// Tick until the Inspect screen's jobs and the app's task have ended.
    fn inspect_settle(app: &mut App) {
        let start = std::time::Instant::now();
        loop {
            app.tick();
            let busy = app.inspect.as_ref().is_some_and(|s| s.busy()) || app.task.is_active();
            if !busy {
                app.tick();
                return;
            }
            assert!(
                start.elapsed() < Duration::from_secs(120),
                "the Inspect jobs never ended"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// The Inspect screen on `path`, opened through `o` and the picker.
    fn inspect_on(path: &std::path::Path) -> App {
        let mut app = App::headless_on(tab_index(TabId::Inspect));
        ikey(&mut app, KeyCode::Char('o'));
        ikeys(&mut app, path.display().to_string().as_str());
        ikey(&mut app, KeyCode::Enter);
        inspect_settle(&mut app);
        assert!(app.inspect.as_ref().unwrap().source_open());
        app
    }

    fn inspect_note(app: &App) -> String {
        app.inspect
            .as_ref()
            .unwrap()
            .note
            .clone()
            .unwrap_or_default()
    }

    /// The grid: Calc min on a volume that needs a walk, Check with its results window, and
    /// Browse handing the partition to the Explorer.
    #[test]
    fn the_inspect_grid_calcs_min_checks_and_browses() {
        use crate::controller::inspect::MinCell;
        let dir = tempfile::tempdir().expect("tempdir");
        let img = mbr_with_ext(dir.path());
        let mut app = inspect_on(&img);
        let part = app.inspect.as_ref().unwrap().ctl.partitions()[0].clone();
        assert_eq!(
            app.inspect.as_ref().unwrap().ctl.min_cell(&part),
            MinCell::CanCalc
        );
        let text = backup_screen(&app);
        assert!(text.contains("m=Calc min"), "{text}");
        ikey(&mut app, KeyCode::Char('m'));
        inspect_settle(&mut app);
        let min = app.inspect.as_ref().unwrap().ctl.min_cell(&part);
        assert!(matches!(min, MinCell::Size(_)), "{min:?}");
        ikey(&mut app, KeyCode::Enter);
        inspect_settle(&mut app);
        assert!(app.explorer.is_some(), "Enter opens the Explorer");
        app.explorer = None;

        let fat = tempfile::tempdir().expect("tempdir");
        let vol = crate::controller::commander::blank_fat_image(fat.path());
        let mut app = inspect_on(&vol);
        ikey(&mut app, KeyCode::Char('c'));
        inspect_settle(&mut app);
        let s = app.inspect.as_ref().unwrap();
        assert!(s.ctl.fsck_view().is_some_and(|v| v.result.is_clean()));
        assert_eq!(app.key_context().0, "Inspect - window");
        let text = backup_screen(&app);
        assert!(text.contains("Filesystem is clean."), "{text}");
        ikey(&mut app, KeyCode::Esc);
        assert!(app.inspect.as_ref().unwrap().ctl.fsck_view().is_none());
        assert_eq!(app.key_context().0, "Inspect - partitions");
    }

    /// Export writes a raw whole-disk copy; its Physical Disk Export opens the shared writer
    /// window, which wraps the superfloppy in a table and writes an image file.
    #[test]
    fn the_inspect_export_window_exports_and_hands_over_to_the_writer() {
        use crate::controller::inspect::ExportFormat;
        let dir = tempfile::tempdir().expect("tempdir");
        let img = crate::controller::commander::blank_fat_image(dir.path());
        let bytes = std::fs::read(&img).unwrap();
        let mut app = inspect_on(&img);
        ikey(&mut app, KeyCode::Char('e'));
        assert_eq!(app.key_context().0, "Inspect - export");
        while app.inspect.as_ref().unwrap().ctl.export_settings().format != ExportFormat::Raw {
            ikey(&mut app, KeyCode::Right);
        }
        let text = backup_screen(&app);
        assert!(text.contains("Export Disk Image"), "{text}");
        ikey(&mut app, KeyCode::Char('s'));
        assert!(app.inspect.as_ref().unwrap().picker_open());
        for _ in 0..400 {
            ikey(&mut app, KeyCode::Backspace);
        }
        let out = dir.path().join("whole.img");
        ikeys(&mut app, out.display().to_string().as_str());
        ikey(&mut app, KeyCode::Enter);
        inspect_settle(&mut app);
        assert_eq!(std::fs::read(&out).unwrap(), bytes);

        ikey(&mut app, KeyCode::Char('e'));
        while app.inspect.as_ref().unwrap().esel != inspect::ERow::Physical {
            ikey(&mut app, KeyCode::Down);
        }
        ikey(&mut app, KeyCode::Enter);
        inspect_settle(&mut app);
        assert!(app.inspect.as_ref().unwrap().writer.is_open());
        assert_eq!(app.key_context().0, "Inspect - Physical Disk Export");
        ikey(&mut app, KeyCode::Down);
        let wrapped = dir.path().join("wrapped.img");
        ikeys(&mut app, wrapped.display().to_string().as_str());
        ikey(&mut app, KeyCode::Enter);
        ikey(&mut app, KeyCode::Down);
        ikey(&mut app, KeyCode::Char('w'));
        let s = app.inspect.as_ref().unwrap();
        assert!(
            matches!(s.ask, Some(ask::Ask::Confirm(..))),
            "{:?} {:?} {:?}",
            s.writer.error(),
            s.writer.target_image(),
            s.log
        );
        ikey(&mut app, KeyCode::Char('y'));
        inspect_settle(&mut app);
        let note = inspect_note(&app);
        assert!(note.starts_with("Wrote "), "{note}");
        assert!(!app.inspect.as_ref().unwrap().writer.is_open());
        let d = std::fs::read(&wrapped).unwrap();
        assert_eq!(&d[510..512], &[0x55, 0xAA]);
        assert!(d.len() > bytes.len());
    }

    /// Expand Image grows the file; tools that do not apply say so instead of acting.
    #[test]
    fn the_inspect_tools_expand_the_image_and_refuse_what_does_not_apply() {
        let dir = tempfile::tempdir().expect("tempdir");
        let img = crate::controller::commander::blank_fat_image(dir.path());
        let before = std::fs::metadata(&img).unwrap().len();
        let mut app = inspect_on(&img);
        ikey(&mut app, KeyCode::Char('x'));
        ikeys(&mut app, "3");
        let text = backup_screen(&app);
        assert!(
            text.contains("Expand Image") && text.contains("New size"),
            "{text}"
        );
        ikey(&mut app, KeyCode::Enter);
        inspect_settle(&mut app);
        assert_eq!(
            std::fs::metadata(&img).unwrap().len(),
            before + 3 * 1024 * 1024
        );
        for k in ['B', 'w', 'i', 'u'] {
            ikey(&mut app, KeyCode::Char(k));
            assert!(inspect_note(&app).contains("does not apply"), "{k}");
        }
        ikey(&mut app, KeyCode::Char('d'));
        assert!(inspect_note(&app).contains("Human68k"));
    }

    /// A backup folder opens through the controller, its summary shown and a partition browsable.
    #[test]
    fn the_inspect_screen_opens_a_backup_folder_and_browses_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (folder, _) = restore_fixture(dir.path());
        let mut app = inspect_on(&folder);
        let s = app.inspect.as_ref().unwrap();
        assert!(s.ctl.backup_folder().is_some());
        assert_eq!(s.ctl.partitions().len(), 1);
        let text = backup_screen(&app);
        assert!(text.contains("Backup of:"), "{text}");
        ikey(&mut app, KeyCode::Enter);
        inspect_settle(&mut app);
        assert!(app.explorer.is_some());
    }

    /// A backup opened on Inspect is shared with Restore, and closing it on Restore closes both.
    #[test]
    fn restore_and_inspect_share_the_backup_folder() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (folder, _) = restore_fixture(dir.path());
        let mut app = inspect_on(&folder);
        let opened = app
            .inspect
            .as_ref()
            .unwrap()
            .ctl
            .backup_folder()
            .map(|p| p.to_path_buf());
        assert_eq!(app.shell.shared_backup(), opened.as_deref());
        let line = app.shell_line().expect("the shell line names the backup").0;
        assert!(line.starts_with("Backup: "), "{line}");
        app.restore
            .as_mut()
            .expect("Restore got it")
            .ctl
            .wait_idle();
        app.tick();
        let r = app.restore.as_ref().unwrap();
        assert_eq!(r.ctl.backup_folder(), opened.as_deref());
        app.active = tab_index(TabId::Restore);
        app.on_tab_changed();
        restore_focus(&mut app, restore::Field::Backup);
        assert!(app.handle_restore_key(KeyCode::Char('c')));
        app.tick();
        assert!(app.shell.shared_backup().is_none());
        assert!(app.inspect.as_ref().unwrap().ctl.backup_folder().is_none());
        assert!(!app.restore.as_ref().unwrap().ctl.has_backup());
        // Opened on Restore, an empty Inspect adopts it when shown.
        assert!(app.handle_restore_key(KeyCode::Char('o')));
        restore_keys(&mut app, folder.display().to_string().as_str());
        app.handle_restore_key(KeyCode::Enter);
        restore_settle(&mut app);
        assert!(app.shell.shared_backup().is_some());
        app.active = tab_index(TabId::Inspect);
        app.on_tab_changed();
        inspect_settle(&mut app);
        let s = app.inspect.as_ref().unwrap();
        assert_eq!(s.ctl.backup_folder(), app.shell.shared_backup());
    }

    /// Without `tui-update` there is no banner; with it the shell's announced release shows.
    #[test]
    fn the_update_banner_follows_the_shell_and_f12_hides_it() {
        let mut app = App::headless_on(tab_index(TabId::Settings));
        assert!(app.update_banner().is_none());
        #[cfg(feature = "tui-update")]
        {
            app.shell
                .set_update_info(crate::controller::app::UpdateInfo {
                    current_version: "1.0.0".into(),
                    latest_version: "2.0.0".into(),
                    releases_url: String::new(),
                    is_outdated: true,
                    asset_url: None,
                    cli_asset_url: None,
                });
            let line = app.shell_line().unwrap().0;
            assert!(
                line.contains("2.0.0") && line.contains("update --apply"),
                "{line}"
            );
            ikey(&mut app, KeyCode::F(12));
            assert!(app.update_banner().is_none());
        }
        ikey(&mut app, KeyCode::F(12));
        assert!(!app.show_help && !app.should_quit);
    }

    /// A disk the controller cannot open falls back to the plain read, which reports why.
    #[test]
    fn a_disk_that_cannot_be_opened_falls_back_to_the_plain_read() {
        let mut app = App::headless_on(tab_index(TabId::Inspect));
        let missing = std::path::Path::new("/nonexistent/rb-tui-test/disk");
        app.set_test_devices(vec![fake_disk(missing, 1 << 20)]);
        ikey(&mut app, KeyCode::Enter);
        inspect_settle(&mut app);
        let s = app.inspect.as_ref().unwrap();
        assert!(s.ctl.last_error().is_some());
        assert!(s.plain.is_some());
        assert!(inspect_note(&app).contains("/nonexistent/rb-tui-test/disk"));
        let text = backup_screen(&app);
        assert!(text.contains("Error:"), "{text}");
        ikey(&mut app, KeyCode::Esc);
        assert!(app.inspect_list_active());
    }

    /// `R` asks for a daemon in a build with `remote`; without it the key does nothing.
    #[test]
    fn the_inspect_remote_window_asks_for_a_daemon_or_is_absent() {
        use crate::controller::inspect::InspectController;
        let mut app = App::headless_on(tab_index(TabId::Inspect));
        ikey(&mut app, KeyCode::Char('R'));
        let s = app.inspect.as_ref().unwrap();
        if InspectController::remote_available() {
            assert!(matches!(s.ask, Some(ask::Ask::Text { .. })));
            let text = backup_screen(&app);
            assert!(text.contains("Connect to remote"), "{text}");
            ikey(&mut app, KeyCode::Esc);
            assert!(app.inspect.as_ref().unwrap().ask.is_none());
            ikey(&mut app, KeyCode::Esc);
            assert!(app.inspect.as_ref().unwrap().window.is_none());
        } else {
            assert!(s.ask.is_none() && s.window.is_none());
        }
        assert!(app.inspect_list_active());
    }

    /// The grid and its windows draw in an 80x24 terminal.
    #[test]
    fn the_inspect_screen_and_windows_draw_in_a_small_terminal() {
        let dir = tempfile::tempdir().expect("tempdir");
        let img = mbr_with_ext(dir.path());
        let size = std::fs::metadata(&img).unwrap().len();
        let mut app = App::headless_on(tab_index(TabId::Inspect));
        app.set_test_devices(vec![fake_disk(&img, size)]);
        ikey(&mut app, KeyCode::Enter);
        inspect_settle(&mut app);
        let text = backup_screen(&app);
        assert!(
            text.contains("Partition table:") && text.contains("Layout:"),
            "{text}"
        );
        assert!(
            text.contains("Disk "),
            "the disk's facts head the screen: {text}"
        );
        eprintln!("{text}");
        ikey(&mut app, KeyCode::Char('e'));
        let text = backup_screen(&app);
        assert!(text.contains("Export Disk Image"), "{text}");
        eprintln!("{text}");
        ikey(&mut app, KeyCode::Esc);
        ikey(&mut app, KeyCode::Char('?'));
        let text = backup_screen(&app);
        assert!(text.contains("Calc min"), "{text}");
    }

    /// Move the table editor's cursor to `row` with the arrow keys.
    fn table_cursor_to(app: &mut App, row: table_edit::TRow) {
        for _ in 0..64 {
            if app.inspect.as_ref().unwrap().twin.sel == row {
                return;
            }
            ikey(app, KeyCode::Up);
        }
        panic!("the editor has no row {row:?}");
    }

    /// Clear a picker's prefilled path and confirm `path`.
    fn pick_path(app: &mut App, path: &std::path::Path) {
        assert!(app.inspect.as_ref().unwrap().picker_open());
        for _ in 0..400 {
            ikey(app, KeyCode::Backspace);
        }
        ikeys(app, path.display().to_string().as_str());
        ikey(app, KeyCode::Enter);
    }

    /// Add Partition fills the free gap; the type is typed and stepped through the catalog, a
    /// delete is undone, and Validate + Apply write the table, which the grid reads again.
    #[test]
    fn the_inspect_table_editor_adds_a_partition_and_applies() {
        use table_edit::TRow;
        let dir = tempfile::tempdir().expect("tempdir");
        let img = crate::controller::test_fixtures::mbr_with_gap(dir.path());
        let mut app = inspect_on(&img);
        assert_eq!(app.inspect.as_ref().unwrap().ctl.partitions().len(), 1);
        ikey(&mut app, KeyCode::Char('a'));
        assert_eq!(app.key_context().0, "Inspect - partition table");
        let s = app.inspect.as_ref().unwrap();
        assert_eq!(s.twin.sel, TRow::Add);
        let v = s.ctl.table_editor().expect("the editor is open");
        assert!(!v.add_start.is_empty() && v.add_type == "83");
        ikey(&mut app, KeyCode::Enter);
        let view = |app: &App| app.inspect.as_ref().unwrap().ctl.table_editor().unwrap();
        assert_eq!(view(&app).rows.len(), 2);

        table_cursor_to(&mut app, TRow::Type(0));
        ikey(&mut app, KeyCode::Backspace);
        ikey(&mut app, KeyCode::Backspace);
        ikeys(&mut app, "0E");
        assert_eq!(view(&app).rows[0].type_text, "0E");
        ikey(&mut app, KeyCode::Right);
        let stepped = view(&app).rows[0].type_text.clone();
        assert_ne!(stepped, "0E", "Right steps through the catalog");
        ikey(&mut app, KeyCode::Left);
        assert_eq!(view(&app).rows[0].type_text, "0E");
        table_cursor_to(&mut app, TRow::Mode(0));
        ikey(&mut app, KeyCode::Char(' '));
        assert_ne!(
            view(&app).rows[0].size_mode,
            crate::controller::partition_edit::SizeMode::Original
        );
        while view(&app).rows[0].size_mode != crate::controller::partition_edit::SizeMode::Original
        {
            ikey(&mut app, KeyCode::Right);
        }
        table_cursor_to(&mut app, TRow::Part(1));
        ikey(&mut app, KeyCode::Delete);
        assert!(view(&app).rows[1].deleted);
        ikey(&mut app, KeyCode::Char('x'));
        assert!(!view(&app).rows[1].deleted);

        ikey(&mut app, KeyCode::Char('v'));
        let v = view(&app);
        assert!(v.errors.is_empty() && v.can_apply, "{:?}", v.errors);
        ikey(&mut app, KeyCode::Char('w'));
        inspect_settle(&mut app);
        let s = app.inspect.as_ref().unwrap();
        assert_eq!(s.ctl.partitions().len(), 2, "{:?}", s.log);
        assert!(s
            .log
            .iter()
            .any(|l| l == "Partition table updated successfully."));
        let mbr = std::fs::read(&img).unwrap();
        assert_eq!(mbr[0x1BE + 4], 0x0E);
        assert_eq!(mbr[0x1CE + 4], 0x83);
        ikey(&mut app, KeyCode::Esc);
        assert!(!app.inspect.as_ref().unwrap().dialog_open());
        assert_eq!(app.key_context().0, "Inspect - partitions");
    }

    /// Resize Partitions shrinks an EFS partition: Custom size typed, Preview, Apply, and the grid
    /// is read again after the write.
    #[test]
    fn the_inspect_resize_window_previews_and_shrinks_a_partition() {
        use crate::controller::partition_edit::SizeMode;
        let dir = tempfile::tempdir().expect("tempdir");
        let img = crate::controller::test_fixtures::sgi_efs_disk(dir.path());
        let mut app = inspect_on(&img);
        ikey(&mut app, KeyCode::Char('s'));
        assert_eq!(app.key_context().0, "Inspect - resize");
        let s = app.inspect.as_ref().unwrap();
        assert!(s.resize.is_open() && !s.resize.is_device());
        let efs = s
            .resize
            .rows()
            .iter()
            .position(|r| r.type_name.contains("EFS"))
            .expect("an EFS row");
        while app.inspect.as_ref().unwrap().rwin.sel != resize::RRow::Mode(efs) {
            ikey(&mut app, KeyCode::Down);
        }
        while app.inspect.as_ref().unwrap().resize.rows()[efs].mode != SizeMode::Custom {
            ikey(&mut app, KeyCode::Right);
        }
        ikey(&mut app, KeyCode::Down);
        assert_eq!(
            app.inspect.as_ref().unwrap().rwin.sel,
            resize::RRow::Size(efs)
        );
        for _ in 0..12 {
            ikey(&mut app, KeyCode::Backspace);
        }
        ikeys(&mut app, "32");
        ikey(&mut app, KeyCode::Char('w'));
        assert!(
            !app.inspect.as_ref().unwrap().resize.is_running(),
            "Apply waits for Preview"
        );
        ikey(&mut app, KeyCode::Char('p'));
        let s = app.inspect.as_ref().unwrap();
        assert!(
            s.resize.preview().is_some() && s.resize.can_apply(),
            "{:?}",
            s.resize.plan_error()
        );
        let text = backup_screen(&app);
        assert!(text.contains("Resize Partitions"), "{text}");
        ikey(&mut app, KeyCode::Char('w'));
        inspect_settle(&mut app);
        let s = app.inspect.as_ref().unwrap();
        assert!(
            s.log
                .iter()
                .any(|l| l.starts_with("Partition resize completed successfully")),
            "{:?}",
            s.log
        );
        assert_eq!(std::fs::metadata(&img).unwrap().len() >> 20, 34);
        let part = s
            .ctl
            .partitions()
            .iter()
            .find(|p| p.type_name.contains("EFS"))
            .expect("the EFS row after the re-read");
        assert_eq!(part.size_bytes, 32 << 20);
        ikey(&mut app, KeyCode::Esc);
        assert!(!app.inspect.as_ref().unwrap().resize.is_open());
    }

    /// Resize Alto Disk, Expand HFS Volume and Convert Floppy Container, each from its key on
    /// the grid through its controller's window and Save As picker.
    #[test]
    fn the_inspect_dialogs_resize_alto_expand_hfs_and_convert_a_floppy() {
        use crate::controller::alto_resize::AltoTarget;
        use crate::controller::floppy_convert::FloppyTarget;
        let dir = tempfile::tempdir().expect("tempdir");
        let fx = crate::controller::test_fixtures::alto_pack(dir.path());
        let mut app = inspect_on(&fx);
        ikey(&mut app, KeyCode::Char('A'));
        inspect_settle(&mut app);
        assert_eq!(app.key_context().0, "Inspect - resize Alto");
        let s = app.inspect.as_ref().unwrap();
        assert!(s.alto.summary_line().is_some() && s.alto.can_save());
        while app.inspect.as_ref().unwrap().alto.target() != AltoTarget::Diablo44 {
            ikey(&mut app, KeyCode::Right);
        }
        ikey(&mut app, KeyCode::Enter);
        ikey(&mut app, KeyCode::Enter);
        let out = dir.path().join("big.pdi");
        pick_path(&mut app, &out);
        inspect_settle(&mut app);
        let s = app.inspect.as_ref().unwrap();
        assert!(
            matches!(s.alto.result(), Some(Ok(_))),
            "{:?}",
            s.alto.result()
        );
        assert!(std::fs::metadata(&out).unwrap().len() > std::fs::metadata(&fx).unwrap().len());
        ikey(&mut app, KeyCode::Esc);
        assert!(!app.inspect.as_ref().unwrap().alto.is_open());

        let hfv = crate::controller::test_fixtures::hfs_volume(dir.path());
        let mut app = inspect_on(&hfv);
        ikey(&mut app, KeyCode::Char('H'));
        inspect_settle(&mut app);
        assert_eq!(app.key_context().0, "Inspect - expand HFS");
        let s = app.inspect.as_ref().unwrap();
        assert!(s.hfs.flat_hfv() && s.hfs.size_mib() == 4);
        ikey(&mut app, KeyCode::Down);
        ikey(&mut app, KeyCode::Down);
        ikeys(&mut app, "8");
        ikey(&mut app, KeyCode::Enter);
        assert_eq!(app.inspect.as_ref().unwrap().hfs.size_mib(), 8);
        ikey(&mut app, KeyCode::Enter);
        let big = dir.path().join("big.hfv");
        pick_path(&mut app, &big);
        assert_eq!(
            app.inspect.as_ref().unwrap().hfs.output(),
            Some(big.as_path())
        );
        ikey(&mut app, KeyCode::Char('s'));
        inspect_settle(&mut app);
        let s = app.inspect.as_ref().unwrap();
        assert_eq!(s.hfs.outcome(), Some(&Ok(())), "{:?}", s.hfs.lines());
        let len = std::fs::metadata(&big).unwrap().len();
        assert!(len > 7 << 20 && len <= 8 << 20, "{len}");
        ikey(&mut app, KeyCode::Esc);
        assert!(!app.inspect.as_ref().unwrap().hfs.is_open());

        let xdf = crate::controller::test_fixtures::blank_xdf(dir.path());
        let mut app = inspect_on(&xdf);
        ikey(&mut app, KeyCode::Char('f'));
        inspect_settle(&mut app);
        assert_eq!(app.key_context().0, "Inspect - convert floppy");
        assert!(app.inspect.as_ref().unwrap().floppy.source_kind().is_some());
        ikey(&mut app, KeyCode::Down);
        while app.inspect.as_ref().unwrap().floppy.target() != FloppyTarget::Hdm {
            ikey(&mut app, KeyCode::Right);
        }
        ikey(&mut app, KeyCode::Down);
        ikey(&mut app, KeyCode::Enter);
        let hdm = dir.path().join("out.hdm");
        pick_path(&mut app, &hdm);
        ikey(&mut app, KeyCode::Down);
        ikey(&mut app, KeyCode::Enter);
        inspect_settle(&mut app);
        let s = app.inspect.as_ref().unwrap();
        assert!(
            matches!(s.floppy.outcome(), Some(Ok(_))),
            "{:?}",
            s.floppy.outcome().map(|o| o.is_ok())
        );
        assert!(hdm.exists());
        ikey(&mut app, KeyCode::Esc);
        assert!(!app.inspect.as_ref().unwrap().floppy.is_open());
    }

    /// The new keys refuse a source they do not apply to, and the dialogs draw in 80x24.
    #[test]
    fn the_inspect_dialogs_refuse_what_does_not_apply_and_draw_in_a_small_terminal() {
        let dir = tempfile::tempdir().expect("tempdir");
        let img = crate::controller::commander::blank_fat_image(dir.path());
        let mut app = inspect_on(&img);
        for k in ['t', 'a', 's'] {
            ikey(&mut app, KeyCode::Char(k));
            assert!(inspect_note(&app).contains("does not apply"), "{k}");
        }
        ikey(&mut app, KeyCode::Char('H'));
        assert!(inspect_note(&app).contains("classic HFS"));
        ikey(&mut app, KeyCode::Char('A'));
        assert!(inspect_note(&app).contains("Alto"));
        ikey(&mut app, KeyCode::Char('f'));
        assert!(inspect_note(&app).contains("XDF"));
        assert!(!app.inspect.as_ref().unwrap().dialog_open());

        let gap = crate::controller::test_fixtures::mbr_with_gap(dir.path());
        let mut app = inspect_on(&gap);
        let text = backup_screen(&app);
        assert!(
            text.contains("t edit table") || text.contains("Tools:"),
            "{text}"
        );
        ikey(&mut app, KeyCode::Char('t'));
        let text = backup_screen(&app);
        assert!(text.contains("Edit Partition Table"), "{text}");
        ikey(&mut app, KeyCode::Char('?'));
        let text = backup_screen(&app);
        assert!(text.contains("Validate"), "{text}");
        ikey(&mut app, KeyCode::Esc);
        ikey(&mut app, KeyCode::Esc);
        ikey(&mut app, KeyCode::Char('s'));
        let text = backup_screen(&app);
        assert!(
            text.contains("Resize Partitions") && text.contains("Preview"),
            "{text}"
        );
        ikey(&mut app, KeyCode::Esc);
        assert!(!app.inspect.as_ref().unwrap().dialog_open());
    }

    /// Walk the wizard's class list to a given class the way the arrow keys do.
    fn wizard_on(class: DiskClass) -> NewWizard {
        let mut w = NewWizard {
            class_sel: NEW_CLASSES
                .iter()
                .position(|c| c.1 == class)
                .expect("class"),
            ..Default::default()
        };
        w.seed_size_for_class();
        w
    }

    /// The CD-ROM class carries two extra rows — "source folder" and the
    /// expand-archives toggle. If `field_count` didn't grow with them, Up/Down
    /// would never reach them and Tab from Name would wrap back to Path.
    #[test]
    fn cdrom_class_exposes_the_source_folder_field() {
        assert_eq!(
            wizard_on(DiskClass::Cdrom).field_count(),
            WIZ_FIELD_EXPAND + 1
        );
        for other in [DiskClass::Floppy, DiskClass::Volume, DiskClass::Hd] {
            assert_eq!(wizard_on(other).field_count(), WIZ_FIELDS);
        }
    }

    /// Space flips the expand-archives toggle on its own row, and stays an
    /// ordinary character everywhere else — a volume name may contain one.
    #[test]
    fn space_toggles_expand_archives_only_on_its_row() {
        let mut app = App::new_on(DEFAULT_TAB);
        app.newdisk = Some(wizard_on(DiskClass::Cdrom));
        {
            let w = app.newdisk.as_mut().unwrap();
            w.step = WizStep::Details;
            w.name = "My".to_string();
            w.field = 2;
        }
        app.handle_newdisk_key(KeyCode::Char(' '));
        {
            let w = app.newdisk.as_ref().unwrap();
            assert_eq!(w.name, "My ", "Space types into the name field");
            assert!(!w.expand_archives);
        }

        app.newdisk.as_mut().unwrap().field = WIZ_FIELD_EXPAND;
        app.handle_newdisk_key(KeyCode::Char(' '));
        assert!(app.newdisk.as_ref().unwrap().expand_archives, "toggled on");
        app.handle_newdisk_key(KeyCode::Char(' '));
        assert!(
            !app.newdisk.as_ref().unwrap().expand_archives,
            "toggled back off"
        );

        // The toggle row holds no text: typing and Backspace must not corrupt
        // the source-folder field it sits under.
        app.newdisk.as_mut().unwrap().from_dir = "/tmp/src".to_string();
        app.handle_newdisk_key(KeyCode::Char('x'));
        app.handle_newdisk_key(KeyCode::Backspace);
        assert_eq!(app.newdisk.as_ref().unwrap().from_dir, "/tmp/src");
    }

    fn wizard_on_next_cd() -> NewWizard {
        let mut w = wizard_on(DiskClass::Cdrom);
        w.fs_sel = CD_TARGETS
            .iter()
            .position(|(_, t)| *t == CdTarget::NextUfs)
            .expect("NeXT target listed");
        w
    }

    /// Only the NeXT target has the gunzip row; Space flips it and it holds no text.
    #[test]
    fn next_cd_target_adds_a_gunzip_toggle_row() {
        assert_eq!(wizard_on_next_cd().field_count(), WIZ_FIELD_GUNZIP + 1);
        assert_eq!(
            wizard_on(DiskClass::Cdrom).field_count(),
            WIZ_FIELD_EXPAND + 1
        );

        let mut app = App::new_on(DEFAULT_TAB);
        app.newdisk = Some(wizard_on_next_cd());
        {
            let w = app.newdisk.as_mut().unwrap();
            w.step = WizStep::Details;
            w.from_dir = "/tmp/src".to_string();
            w.field = WIZ_FIELD_GUNZIP;
        }
        app.handle_newdisk_key(KeyCode::Char(' '));
        let w = app.newdisk.as_ref().unwrap();
        assert!(
            w.expand_gunzip && !w.expand_archives,
            "only the gunzip row flipped"
        );
        app.handle_newdisk_key(KeyCode::Char('x'));
        app.handle_newdisk_key(KeyCode::Backspace);
        assert_eq!(app.newdisk.as_ref().unwrap().from_dir, "/tmp/src");
    }

    /// The wizard's NeXT target reaches `optical new next-ufs` and leaves a NeXT disc.
    #[test]
    fn wizard_builds_a_next_cd() {
        let dir = tempfile::tempdir().unwrap();
        let iso = dir.path().join("next.iso");
        let mut app = App::new_on(DEFAULT_TAB);
        let mut w = wizard_on_next_cd();
        w.step = WizStep::Details;
        w.path = iso.to_string_lossy().into_owned();
        w.size = "8M".to_string();
        app.newdisk = Some(w);
        app.newdisk_create();
        app.finish_task();
        let w = app.newdisk.as_ref().unwrap();
        assert!(!w.is_error, "{:?}", w.status);
        assert!(
            crate::controller::test_fixtures::has_next_label(iso.as_path()),
            "a NeXT label at block 0"
        );
    }

    /// Every class needs its own sensible default size, and `auto` — which
    /// only the CD class can act on — must not survive a move to a class that
    /// would choke on it.
    #[test]
    fn size_default_follows_the_selected_class() {
        assert_eq!(wizard_on(DiskClass::Floppy).size, "800K");
        assert_eq!(wizard_on(DiskClass::Volume).size, "64M");
        assert_eq!(wizard_on(DiskClass::Hd).size, "32M");
        assert_eq!(wizard_on(DiskClass::Cdrom).size, "600M");

        // `auto` set on the CD class, then arrow away: must be re-seeded.
        let mut w = wizard_on(DiskClass::Cdrom);
        w.size = "auto".to_string();
        w.class_sel = NEW_CLASSES
            .iter()
            .position(|c| c.1 == DiskClass::Volume)
            .unwrap();
        w.seed_size_for_class();
        assert_eq!(w.size, "64M", "'auto' must not leak onto a non-CD class");

        // A size the user typed is still never clobbered.
        let mut w = wizard_on(DiskClass::Floppy);
        w.size = "1440K".to_string();
        w.class_sel = NEW_CLASSES
            .iter()
            .position(|c| c.1 == DiskClass::Cdrom)
            .unwrap();
        w.seed_size_for_class();
        assert_eq!(w.size, "1440K");
    }

    /// The "filesystem" step doubles as the disc-layout picker for a CD, so
    /// its list has to be non-empty and indexable without panicking.
    #[test]
    fn cdrom_class_offers_a_disc_target() {
        let w = wizard_on(DiskClass::Cdrom);
        assert_eq!(w.fs_count(), CD_TARGETS.len());
        assert!(w.fs_count() > 0);
        // Out-of-range selection clamps rather than panicking, matching the
        // other classes.
        assert_eq!(w.fs_label(999), CD_TARGETS[CD_TARGETS.len() - 1].0);
        assert_eq!(CD_TARGETS[0].1, CdTarget::SgiEfs);
        assert!(
            CD_TARGETS
                .iter()
                .any(|(_, t)| *t == CdTarget::MacHfs || *t == CdTarget::MacHfsPlus),
            "the Mac disc targets are reachable from the wizard"
        );
    }

    fn tab_index(tab: TabId) -> usize {
        TABS.iter().position(|(t, _)| *t == tab).unwrap()
    }

    /// `r` refreshes the device list through the app shell's job and reports the count.
    #[test]
    fn r_refreshes_the_device_list_in_the_shell_job() {
        let mut app = App::new_on(tab_index(TabId::Inspect));
        app.on_key_inner(crossterm::event::KeyEvent::new(
            KeyCode::Char('r'),
            KeyModifiers::NONE,
        ));
        assert!(app.devices_loaded);
        assert_eq!(app.status.as_deref(), Some("Looking for devices..."));
        let start = std::time::Instant::now();
        while app.refreshing_devices && start.elapsed() < Duration::from_secs(60) {
            app.tick();
            std::thread::sleep(Duration::from_millis(5));
        }
        let status = app.status.clone().unwrap_or_default();
        assert!(status.starts_with("Refreshed: "), "{status}");
        assert!(status.ends_with("device(s) found"), "{status}");
    }
}
