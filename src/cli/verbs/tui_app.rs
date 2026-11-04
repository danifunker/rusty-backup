//! `rb-cli tui` — a full-screen terminal UI (milestone M1: the tab shell).
//!
//! This mirrors the desktop GUI: a top tab bar (Backup / Restore / Inspect /
//! New Disk / Optical / Archives / Commander / Bulk / Settings) over the same
//! shared `src/model/` runner layer the GUI uses. Real tabs so far: **Inspect**
//! (a selectable disk list; opening an image shows its **partition table** via
//! `PartitionTable::detect`, each partition opening a per-partition filesystem
//! **Explorer**, one browser pane over the partition with its volume tools: see
//! `tui_app/explorer.rs`); **New Disk** (a wizard that
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
use crate::device::{enumerate_devices, DiskDevice};

mod commander;
mod explorer;

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

/// Something the user opened from the Inspect tab. A file is treated as a disk
/// image (its partition table is parsed); a directory is routed through the
/// shared backup loader.
enum Opened {
    Image {
        path: String,
        size: u64,
        parts: Vec<PartRow>,
        /// Set when this was opened from the disk list, so the view can show
        /// the hardware facts (media, bus, flags, what the OS has mounted)
        /// above the partition table instead of on a separate screen.
        disk: Option<Box<DiskDevice>>,
    },
    Backup {
        path: String,
        kind: String,
        partitions: usize,
        info: Vec<String>,
    },
}

/// One row of an opened image's partition table, plus the `@N` selector the
/// shared `resolve` path uses to open its filesystem (`None` = superfloppy).
struct PartRow {
    selector: Option<crate::cli::img_at::PartSelector>,
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
    hint("Enter", "Open the disk's partition table"),
    hint("o", "Open an image file or backup folder"),
    hint("r", "Rescan disks"),
    hint("g/G", "Top / bottom  (also Home/End)"),
];

const HINTS_INSPECT_IMAGE: &[KeyHint] = &[
    hint("Up/Down", "Move  (also k/j)"),
    hint("Enter", "Browse this partition's filesystem"),
    hint("o", "Open another image or backup folder"),
    hint("Esc", "Back to the disk list"),
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
    let outcome = App::new_on(initial_tab).run(&mut terminal);
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

/// The Backup screen: a guided source -> config -> run flow over a headless
/// `controller::backup::BackupController`, the controller behind the GUI's Backup tab.
/// The run's progress is mirrored into the shell's bottom progress bar.
#[derive(Clone, Copy, PartialEq, Eq)]
enum BackupStep {
    Source,
    Config,
    Run,
}

/// Output formats offered on the Backup config form (label -> compression).
const BACKUP_FORMATS: &[(&str, crate::controller::backup::CompressionType)] = &[
    ("Zstd", crate::controller::backup::CompressionType::Zstd),
    (
        "CHD (single-file)",
        crate::controller::backup::CompressionType::Chd,
    ),
    ("VHD", crate::controller::backup::CompressionType::Vhd),
    ("gzip", crate::controller::backup::CompressionType::Gzip),
    ("LZ4", crate::controller::backup::CompressionType::Lz4),
    (
        "Raw (uncompressed)",
        crate::controller::backup::CompressionType::None,
    ),
];

/// Checksum choices on the Backup config form (label -> algorithm).
const BACKUP_CHECKSUMS: &[(&str, crate::controller::backup::ChecksumType)] = &[
    ("SHA-256", crate::controller::backup::ChecksumType::Sha256),
    ("CRC32", crate::controller::backup::ChecksumType::Crc32),
];

/// Config-form rows: 0 output dir, 1 name, 2 format, 3 checksum, 4 "Start".
const BACKUP_FIELDS: usize = 5;

struct BackupState {
    step: BackupStep,
    ctl: crate::controller::backup::BackupController,
    /// Source image file picker, when open.
    picker: Option<FilePicker>,
    /// Physical-disk chooser sub-mode + its cursor.
    device_pick: bool,
    device_sel: usize,
    /// The typed output folder (`~` expands on Start).
    out_dir: String,
    format_sel: usize,
    checksum_sel: usize,
    field: usize,
    /// Latest operation string from the run (shown during Run).
    op: String,
    /// The controller's latest log lines, shown under the run.
    log: Vec<String>,
    /// Esc asked the running backup to stop.
    cancel_note: bool,
}

impl Default for BackupState {
    fn default() -> Self {
        use crate::controller::backup::{
            BackupAction, BackupController, BackupServices, JobRegistry, NoNotify,
        };
        use crate::controller::Controller as _;
        let mut ctl = BackupController::new(BackupServices {
            jobs: JobRegistry::new(),
            notify: Arc::new(NoNotify),
        });
        // The TUI keeps each partition's size and writes one archive per partition.
        ctl.apply(BackupAction::SetResize(false));
        ctl.apply(BackupAction::SetSplit(false));
        BackupState {
            step: BackupStep::Source,
            ctl,
            picker: None,
            device_pick: false,
            device_sel: 0,
            out_dir: String::new(),
            format_sel: 0,
            checksum_sel: 0,
            field: 0,
            op: String::new(),
            log: Vec::new(),
            cancel_note: false,
        }
    }
}

impl BackupState {
    fn act(&mut self, action: crate::controller::backup::BackupAction) {
        use crate::controller::Controller as _;
        // The controller's pickers are the TUI's own file picker, opened by key.
        let _ = self.ctl.apply(action);
        self.after();
    }

    /// Carry out the controller's requests and keep its latest log lines.
    fn after(&mut self) {
        use crate::controller::backup::HostRequest;
        for req in self.ctl.take_requests() {
            match req {
                HostRequest::RememberRecent(path) => {
                    crate::update::push_recent(
                        crate::update::RecentMode::Backup,
                        &path.to_string_lossy(),
                    );
                }
            }
        }
        self.log
            .extend(self.ctl.take_log().into_iter().map(|(_, msg)| msg));
        let n = self.log.len();
        if n > 6 {
            self.log.drain(..n - 6);
        }
    }

    /// A backup runs or waits to start.
    fn busy(&self) -> bool {
        self.ctl.is_running() || self.ctl.start_pending() || self.ctl.vhd_open()
    }

    /// How the run ended (text, is an error); `None` while it runs.
    fn result(&self) -> Option<(String, bool)> {
        if self.busy() {
            return self
                .cancel_note
                .then(|| ("Cancel requested...".to_string(), true));
        }
        match self.ctl.last_result()? {
            Ok(()) => Some(("Backup complete.".to_string(), false)),
            Err(e) => Some((e.clone(), true)),
        }
    }

    fn source_text(&self) -> String {
        self.ctl
            .source_path()
            .map(|p| p.display().to_string())
            .unwrap_or_default()
    }
}

/// The Restore screen: source (backup folder / `.cbk`) -> config (target /
/// sizing / alignment) -> run, over a headless `controller::restore::RestoreController`,
/// the controller behind the GUI's Restore tab. An image source is poured by a headless
/// `controller::disk_write::DiskWriteController`, the GUI's Physical Disk Export window.
#[derive(Clone, Copy, PartialEq, Eq)]
enum RestoreStep {
    Source,
    Config,
    Run,
}

/// Partition-sizing policy applied to every partition; index 1 is the controller's Minimum.
const RESTORE_SIZE_LABELS: &[&str] = &["Original", "Minimum"];
/// Restore alignment choices; index must match `start_restore`.
const RESTORE_ALIGN_LABELS: &[&str] = &["Original", "Modern 1MB"];
/// Config-form rows: 0 target, 1 size, 2 alignment, 3 "Start".
const RESTORE_FIELDS: usize = 4;

struct RestoreState {
    step: RestoreStep,
    ctl: crate::controller::restore::RestoreController,
    /// The backup folder or `.cbk` (or, for an image source, the image) as picked.
    backup_folder: String,
    /// Original disk size from the backup metadata (default target size).
    source_size: u64,
    part_count: usize,
    loaded: bool,
    /// The controller is loading the picked backup.
    loading: bool,
    /// The controller runs the restore this screen started.
    restoring: bool,
    /// Shared picker; `picker_target` says whether a confirm sets the target.
    picker: Option<FilePicker>,
    picker_target: bool,
    /// Config form.
    target: String,
    size_sel: usize,
    align_sel: usize,
    field: usize,
    op: String,
    result: Option<String>,
    is_error: bool,
    /// The "Target type" chooser (Image file / Physical device) opened with Tab
    /// on the target field; its selected index when open.
    target_type_menu: Option<usize>,
    /// Physical-device target chooser (modal) + its selection.
    device_pick: bool,
    device_sel: usize,
    /// True when `target` names a physical disk (restore writes to the raw
    /// device, `RestoreConfig::target_is_device`) rather than an image file.
    target_is_device: bool,
    /// The device path + label pending a destructive-write confirmation.
    confirm_device: Option<(String, String)>,
    /// Source kind: `false` restores a backup folder, `true` pours a plain
    /// image file onto the target (any format the engine can read).
    source_is_image: bool,
    /// Decoded length of the chosen image; a container's file size is not it.
    image_size: u64,
    /// Pours an image source: the target, its partitions and the write.
    writer: crate::controller::disk_write::DiskWriteController,
    /// The writer runs the write this screen started.
    writing: bool,
}

impl Default for RestoreState {
    fn default() -> Self {
        use crate::controller::restore::{
            JobRegistry, NoNotify, RestoreController, RestoreServices,
        };
        RestoreState {
            step: RestoreStep::Source,
            ctl: RestoreController::new(RestoreServices {
                jobs: JobRegistry::new(),
                notify: Arc::new(NoNotify),
            }),
            backup_folder: String::new(),
            source_size: 0,
            part_count: 0,
            loaded: false,
            loading: false,
            restoring: false,
            picker: None,
            picker_target: false,
            target: String::new(),
            size_sel: 0,
            align_sel: 0,
            field: 0,
            op: String::new(),
            result: None,
            is_error: false,
            target_type_menu: None,
            device_pick: false,
            device_sel: 0,
            target_is_device: false,
            confirm_device: None,
            source_is_image: false,
            image_size: 0,
            writer: crate::controller::disk_write::DiskWriteController::new(
                crate::controller::disk_write::DiskWriteServices {
                    jobs: crate::controller::disk_write::JobRegistry::new(),
                    notify: Arc::new(crate::controller::disk_write::NoNotify),
                },
            ),
            writing: false,
        }
    }
}

impl RestoreState {
    fn act(&mut self, action: crate::controller::restore::RestoreAction) {
        use crate::controller::Controller as _;
        // The TUI answers the controller's confirmation with its own device prompt.
        let _ = self.ctl.apply(action);
        self.after();
    }

    /// Carry out the controller's requests and follow its load and run.
    fn after(&mut self) {
        use crate::controller::restore::HostRequest;
        for req in self.ctl.take_requests() {
            match req {
                HostRequest::RememberRecent(path) => {
                    crate::update::push_recent(
                        crate::update::RecentMode::Restore,
                        &path.to_string_lossy(),
                    );
                }
                // The TUI pours an image itself; it never asks for the GUI's writer.
                HostRequest::WriteImage { .. } => {}
            }
        }
        let _ = self.ctl.take_log();
        if self.loading && self.ctl.loading().is_none() {
            self.loading = false;
            self.loaded_backup();
        }
        if let Some(bar) = self.ctl.progress() {
            self.op = bar.operation;
        }
        if self.restoring && !self.ctl.is_running() {
            self.restoring = false;
            match self.ctl.last_result() {
                Some(Ok(())) => {
                    self.result = Some("Restore complete.".to_string());
                    self.is_error = false;
                }
                Some(Err(e)) => {
                    self.result = Some(e.clone());
                    self.is_error = true;
                }
                None => {}
            }
        }
    }

    /// The load ended: on to the options, or the reason on the source step.
    fn loaded_backup(&mut self) {
        if !self.ctl.has_backup() {
            self.result = Some(
                self.ctl
                    .load_error()
                    .unwrap_or("Not a backup folder.")
                    .to_string(),
            );
            self.is_error = true;
            return;
        }
        let folder = std::path::PathBuf::from(self.backup_folder.as_str());
        let default_target = folder
            .parent()
            .map(|par| {
                let stem = folder
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_else(|| "restore".to_string());
                par.join(format!("{stem}_restored.img"))
                    .display()
                    .to_string()
            })
            .unwrap_or_default();
        self.source_size = self.ctl.source_size().unwrap_or(0);
        self.part_count = self.ctl.size_rows().len();
        self.loaded = true;
        if self.target.is_empty() {
            self.target = default_target;
        }
        self.step = RestoreStep::Config;
        self.field = 0;
        self.result = None;
        self.is_error = false;
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

/// The Optical screen (rip): choose a drive -> config (output / format / eject)
/// -> run, through a headless `controller::optical::OpticalController`.
#[cfg(feature = "optical")]
#[derive(Clone, Copy, PartialEq, Eq)]
enum OpticalStep {
    Drives,
    Config,
    Run,
}

/// Rip output formats.
#[cfg(feature = "optical")]
const RIP_FORMATS: &[(&str, crate::controller::optical::OutputFormat)] = &[
    ("ISO (data)", crate::controller::optical::OutputFormat::Iso),
    (
        "BIN/CUE (raw)",
        crate::controller::optical::OutputFormat::BinCue,
    ),
];
/// Config-form rows: 0 output, 1 format, 2 eject, 3 "Start".
#[cfg(feature = "optical")]
const OPTICAL_FIELDS: usize = 4;

/// Optical *image* operations (as opposed to ripping a physical drive), offered
/// by the Optical tab's `i` launcher. Optical browse/extract run on the
/// `opticaldiscs` crate's own filesystem trait — a distinct code path from the
/// partition Explorer — so, like the Explorer's transforms, they're launched
/// through the `:` command palette (shared CLI `dispatch`) with a template the
/// user completes. `(label, verb-template)`.
#[cfg(feature = "optical")]
const OPTICAL_IMAGE_OPS: &[(&str, &str)] = &[
    ("Browse an optical image", "optical browse \"<IMAGE.iso>\""),
    (
        "Extract files to a folder",
        "optical extract --to \"<OUTPUT_DIR>\" \"<IMAGE.iso>\"",
    ),
    ("Show volume info", "optical info \"<IMAGE.iso>\""),
    (
        "Convert to another format",
        "optical convert --format iso \"<IMAGE>\" \"<OUTPUT.iso>\"",
    ),
    (
        "New blank CD-ROM (SGI EFS)",
        "optical new sgi-efs \"<OUTPUT.iso>\"",
    ),
    (
        "New blank CD-ROM (Mac HFS)",
        "optical new mac-hfs \"<OUTPUT.iso>\"",
    ),
    (
        "New blank CD-ROM (Mac HFS+)",
        "optical new mac-hfsplus \"<OUTPUT.iso>\"",
    ),
    (
        "New blank CD-ROM (NeXTSTEP UFS)",
        "optical new next-ufs \"<OUTPUT.iso>\"",
    ),
];

#[cfg(feature = "optical")]
struct OpticalState {
    step: OpticalStep,
    /// The drive list, the detect and the rip job.
    ctl: crate::controller::optical::OpticalController,
    drive_sel: usize,
    device_path: String,
    device_name: String,
    output: String,
    format_sel: usize,
    eject: bool,
    field: usize,
    picker: Option<FilePicker>,
    op: String,
    result: Option<String>,
    is_error: bool,
    /// Esc asked the running rip to stop; the final result replaces the note.
    cancel_note: bool,
    /// The optical-image-operations launcher menu (selected index), when open.
    image_menu: Option<usize>,
}

#[cfg(feature = "optical")]
impl Default for OpticalState {
    fn default() -> Self {
        use crate::controller::optical::{OpticalAction, OpticalController, SourceMode};
        use crate::controller::Controller as _;
        let mut ctl = OpticalController::new(Arc::new(crate::controller::optical::NoNotify));
        ctl.apply(OpticalAction::SetSourceMode(SourceMode::PhysicalDrive));
        OpticalState {
            step: OpticalStep::Drives,
            ctl,
            drive_sel: 0,
            device_path: String::new(),
            device_name: String::new(),
            output: String::new(),
            format_sel: 0,
            eject: false,
            field: 0,
            picker: None,
            op: String::new(),
            result: None,
            is_error: false,
            cancel_note: false,
            image_menu: None,
        }
    }
}

#[cfg(feature = "optical")]
impl OpticalState {
    /// A fresh screen whose drive scan has started.
    fn scanned() -> Self {
        use crate::controller::Controller as _;
        let mut o = OpticalState::default();
        o.ctl
            .apply(crate::controller::optical::OpticalAction::FindDrives);
        o
    }
}

/// Fork container formats for archive extraction.
const ARCHIVE_FORK_FORMATS: &[(&str, crate::controller::archives::ForkFormat)] = {
    use crate::controller::archives::ForkFormat as F;
    &[
        ("BinHex (.hqx)", F::BinHex),
        ("MacBinary (.bin)", F::MacBinary),
        ("AppleDouble", F::AppleDouble),
        ("Raw (.rsrc)", F::Raw),
    ]
};

/// The Archives screen: open a classic Mac archive (`.sit` / `.sea` / `.cpt` /
/// `.mar` / `.hqx`), list its entries, and extract to the host preserving forks,
/// through a headless `controller::archives::ArchivesController`.
struct ArchiveState {
    ctl: crate::controller::archives::ArchivesController,
    picker: Option<FilePicker>,
    /// When true, a picker confirm chooses the extract destination folder.
    picker_dest: bool,
    archive_path: String,
    /// Pre-rendered entry lines for display.
    entries: Vec<String>,
    list_sel: usize,
    fork_fmt_sel: usize,
    status: Option<String>,
    is_error: bool,
    /// An open or an extraction was started and its outcome is not shown yet.
    awaiting_open: Option<std::path::PathBuf>,
    awaiting_extract: bool,
}

impl Default for ArchiveState {
    fn default() -> Self {
        use crate::controller::archives::{ArchivesController, NoNotify};
        ArchiveState {
            ctl: ArchivesController::new(Arc::new(NoNotify)),
            picker: None,
            picker_dest: false,
            archive_path: String::new(),
            entries: Vec::new(),
            list_sel: 0,
            fork_fmt_sel: 0,
            status: None,
            is_error: false,
            awaiting_open: None,
            awaiting_extract: false,
        }
    }
}

impl ArchiveState {
    fn act(&mut self, action: crate::controller::archives::ArchivesAction) {
        use crate::controller::Controller as _;
        let _ = self.ctl.apply(action);
    }

    /// Drain the controller and turn a finished open or extraction into the status line.
    fn poll(&mut self) {
        use crate::controller::Controller as _;
        let _ = self.ctl.tick();
        let _ = self.ctl.take_log();
        // Recent files are recorded below on a successful open, as this screen always did.
        let _ = self.ctl.take_requests();
        if self.awaiting_open.is_some() && !self.ctl.is_loading() {
            let path = self.awaiting_open.take().unwrap_or_default();
            if let Some(e) = self.ctl.load_error() {
                self.status = Some(format!("Cannot open archive: {}", e.detail));
                self.is_error = true;
            } else if self.ctl.is_loaded() {
                crate::update::push_recent(
                    crate::update::RecentMode::Archives,
                    &path.to_string_lossy(),
                );
                self.archive_path = path.display().to_string();
                self.entries = self.ctl.rows().iter().map(archive_row_line).collect();
                self.list_sel = 0;
                self.status = Some(format!("Opened ({} entries).", self.entries.len()));
                self.is_error = false;
            }
        }
        if self.awaiting_extract && !self.ctl.is_extracting() {
            self.awaiting_extract = false;
            match self.ctl.last_extract() {
                Some(Ok((stats, dest))) => {
                    self.status = Some(format!(
                        "Extracted {} file(s){} to {}",
                        stats.files,
                        if stats.skipped > 0 {
                            format!(" ({} skipped)", stats.skipped)
                        } else {
                            String::new()
                        },
                        dest.display()
                    ));
                    self.is_error = false;
                }
                Some(Err(e)) => {
                    self.status = Some(format!("Extract failed: {}", e.detail));
                    self.is_error = true;
                }
                None => {}
            }
        }
    }
}

/// One archive entry as the screen lists it.
fn archive_row_line(e: &crate::controller::archives::ArchiveEntryRow) -> String {
    if e.is_dir {
        return format!("DIR   {}/", e.path);
    }
    let data = e
        .data_len
        .map(|n| format!("data {}", format_size(n)))
        .unwrap_or_default();
    let rsrc = e
        .rsrc_len
        .map(|n| format!("rsrc {}", format_size(n)))
        .unwrap_or_default();
    format!(
        "FILE  {:<34} {:>4} {:>4}  {}  {}",
        e.path, e.type_code, e.creator, data, rsrc
    )
}

/// The Settings tab state: the loaded `UpdateConfig` plus a cursor over the
/// editable preference toggles. Edits save straight back to `config.json` via
/// `UpdateConfig::save` (the same file the GUI reads/writes).
struct SettingsState {
    config: crate::update::UpdateConfig,
    sel: usize,
    status: Option<String>,
}

/// Number of editable toggle rows on the Settings tab.
const SETTINGS_TOGGLES: usize = 2;

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
    /// When the Inspect tab has drilled into a disk, the index of that disk.
    /// An image/backup opened from the Inspect tab (takes over the Inspect body).
    opened: Option<Opened>,
    /// The shared "Open file / backup" picker (path + recent + Tab-browse).
    open_picker: Option<FilePicker>,
    /// The per-partition filesystem Explorer overlay, when open.
    explorer: Option<commander::CommanderView>,
    /// Transient status/error line shown on the Inspect screen.
    status: Option<String>,
    /// A running progress task (the preview / future backup-restore-convert).
    progress: Option<Progress>,
    /// Lazily populated when the Inspect tab is first shown / rescanned.
    disks: Option<Vec<DiskDevice>>,
    /// The New Disk creation wizard state (lazily created on first visit).
    newdisk: Option<NewWizard>,
    /// The Backup screen state (lazily created on first visit).
    backup: Option<BackupState>,
    /// The Restore screen state (lazily created on first visit).
    restore: Option<RestoreState>,
    /// The Bulk convert screen state (lazily created on first visit).
    bulk: Option<BulkState>,
    /// The Optical (rip) screen state (lazily created on first visit).
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
        let active = initial_tab.min(TABS.len() - 1);
        let mut app = App {
            palette: Palette::detect(),
            border: choose_border_set(),
            elevated: crate::controller::app::running_elevated(),
            active,
            scroll: 0,
            selection: 0,
            opened: None,
            open_picker: None,
            explorer: None,
            status: None,
            progress: None,
            disks: None,
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
            || self.explorer.as_ref().is_some_and(|c| c.is_working());
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
                let recent = crate::update::load_recent(crate::update::RecentMode::Inspect);
                self.open_picker =
                    Some(FilePicker::new(PickKind::Any, "Open file / backup").with_recent(recent));
            }
            // Rescan on the Inspect tab (the k9s `r`-to-refresh idiom).
            KeyCode::Char('r') if self.current() == TabId::Inspect => {
                self.disks = Some(enumerate_devices());
                self.selection = 0;
            }
            _ => {}
        }
    }

    /// targets. A directory is routed through the shared `backup_loader`
    /// (rusty-backup folder or Clonezilla image); a file is a disk image. On
    /// success the path is recorded in the MRU (move-to-front).
    fn open_target(&mut self, path: std::path::PathBuf) {
        self.selection = 0;
        self.status = None;
        if path.is_dir() {
            self.start_task("Loading the backup", move |job| {
                job.set_can_cancel(false);
                let opened = open_backup_headless(path.clone());
                let display = path.display().to_string();
                Ok(Box::new(move |app: &mut App| match opened {
                    Ok(opened) => {
                        crate::update::push_recent(crate::update::RecentMode::Inspect, &display);
                        app.opened = Some(opened);
                    }
                    Err(e) => app.status = Some(format!("Cannot open: {e}")),
                }) as TaskDone)
            });
        } else if path.exists() {
            // `exists()` rather than `is_file()`: a raw device (`/dev/sda`) is
            // neither a file nor a directory, so `is_file()` sent every device
            // path to the "No such file or directory" arm below - a flat denial
            // for something plainly present, with no hint that the tab simply
            // did not handle devices. Opening one is the whole point of the
            // disk list next to it.
            self.open_image_at(path, None);
        } else {
            self.status = Some(format!("No such file or directory: {}", path.display()));
        }
    }

    /// Open an image *or* a raw device as the Inspect tab's image view.
    ///
    /// `size` is looked up via [`size_unelevated`](crate::controller::inspect::size_unelevated) so block devices come out at
    /// their true size instead of the zero `metadata` reports for them. Pass
    /// `disk` when opening from the disk list so the view can show the hardware
    /// facts alongside the partition table.
    fn open_image_at(&mut self, path: std::path::PathBuf, disk: Option<Box<DiskDevice>>) {
        // Sizing a device and reading its table happen on the worker; a slow disk keeps the screen live.
        self.start_task("Reading the partition table", move |job| {
            job.set_can_cancel(false);
            let size = crate::controller::inspect::size_unelevated(path.as_path());
            // An image file is read by the Inspect controller; a device keeps the plain, unelevated read.
            let inspected = (disk.is_none() && path.is_file())
                .then(|| inspected_rows(path.clone()))
                .flatten();
            let parts = size.as_ref().ok().map(|&s| match inspected {
                Some(rows) => Ok(rows),
                None => parse_partitions(path.as_path(), s),
            });
            Ok(
                Box::new(move |app: &mut App| app.apply_image_open(path, disk, size, parts))
                    as TaskDone,
            )
        });
    }

    /// Show an image or device opened by [`open_image_at`](Self::open_image_at).
    fn apply_image_open(
        &mut self,
        path: std::path::PathBuf,
        disk: Option<Box<DiskDevice>>,
        size: std::io::Result<u64>,
        parts: Option<std::io::Result<Vec<PartRow>>>,
    ) {
        let display = path.display().to_string();
        let size = match size {
            Ok(s) => s,
            // Fall back to the size the enumerator already reported. Reading a
            // raw device needs elevation, and refusing to show anything at all
            // is a worse answer than showing the hardware facts plus why the
            // table could not be read.
            Err(e) => match disk.as_ref().map(|d| d.size_bytes).filter(|&s| s > 0) {
                Some(s) => {
                    self.status = Some(Self::access_hint(&display, &e));
                    s
                }
                None => {
                    self.status = Some(Self::access_hint(&display, &e));
                    return;
                }
            },
        };
        let parts = match parts {
            Some(p) => p,
            None => parse_partitions(&path, size),
        };
        match parts {
            Ok(parts) => {
                crate::update::push_recent(crate::update::RecentMode::Inspect, &display);
                self.status = None;
                self.opened = Some(Opened::Image {
                    path: display,
                    size,
                    parts,
                    disk,
                });
            }
            Err(e) => {
                self.status = Some(Self::access_hint(&display, &e));
                // A device we cannot read still has facts worth showing, and
                // leaving the user on the list with only a status line makes it
                // look as though Enter did nothing.
                if disk.is_some() {
                    self.opened = Some(Opened::Image {
                        path: display,
                        size,
                        parts: Vec::new(),
                        disk,
                    });
                }
            }
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
            // Opened image: Enter a partition → open its filesystem Explorer.
            TabId::Inspect if matches!(self.opened, Some(Opened::Image { .. })) => {
                let sel = self.selection;
                let target = if let Some(Opened::Image { path, parts, .. }) = &self.opened {
                    parts
                        .get(sel)
                        .map(|r| (path.clone(), r.selector.clone(), r.label.clone()))
                } else {
                    None
                };
                if let Some((path, selector, label)) = target {
                    // The table's rows are the pane's partitions, in the same order.
                    let part = selector.map(|_| sel);
                    let path = std::path::PathBuf::from(path);
                    self.explorer = Some(commander::CommanderView::explorer(path, part, label));
                    self.status = None;
                }
            }
            // Disk list: Enter → the disk's own partition table, with its
            // hardware facts shown above it.
            //
            // One Enter, one screen. This used to take two: the first opened a
            // read-only pane of media/bus/flags plus the partitions the *OS* had
            // mounted, and a second - undiscoverable, and for a while not wired
            // up at all - reached the real table. Nothing on screen said the
            // second press existed, and the two screens were describing the same
            // disk, so they are now one view.
            TabId::Inspect if self.row_count() > 0 => {
                let target = self
                    .disks
                    .as_ref()
                    .and_then(|d| d.get(self.selection))
                    .map(|d| (d.path.clone(), Box::new(d.clone())));
                if let Some((path, disk)) = target {
                    self.selection = 0;
                    self.open_image_at(path, Some(disk));
                }
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
        } else if self.opened.is_some() {
            // One level to unwind now that the disk's facts and its partition
            // table are a single view: Esc goes straight back to the disk list.
            self.opened = None;
            self.selection = 0;
            self.status = None;
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
        if let Some(r) = self.restore.as_mut() {
            if r.ctl.restore_running() {
                r.act(crate::controller::restore::RestoreAction::Cancel);
            }
        }
        if let Some(b) = self.bulk.as_mut() {
            if b.ctl.is_running() {
                b.act(crate::controller::bulk_convert::BulkConvertAction::Cancel);
            }
        }
        #[cfg(feature = "optical")]
        if let Some(o) = self.optical.as_mut() {
            if o.ctl.is_running() {
                use crate::controller::Controller as _;
                o.ctl
                    .apply(crate::controller::optical::OpticalAction::Cancel);
                o.cancel_note = true;
            }
        }
        self.progress = None;
    }

    /// Lazily load anything a freshly-shown tab needs; reset per-tab cursor.
    fn on_tab_changed(&mut self) {
        self.selection = 0;
        self.opened = None;
        self.open_picker = None;
        self.explorer = None;
        self.status = None;
        if self.current() == TabId::Inspect && self.disks.is_none() {
            self.disks = Some(enumerate_devices());
        }
        if self.current() == TabId::NewDisk && self.newdisk.is_none() {
            self.newdisk = Some(NewWizard::default());
        }
        if self.current() == TabId::Backup {
            if self.backup.is_none() {
                self.backup = Some(BackupState::default());
            }
            if self.disks.is_none() {
                self.disks = Some(enumerate_devices());
            }
        }
        if self.current() == TabId::Restore && self.restore.is_none() {
            self.restore = Some(RestoreState::default());
        }
        if self.current() == TabId::Bulk && self.bulk.is_none() {
            self.bulk = Some(BulkState::default());
        }
        #[cfg(feature = "optical")]
        if self.current() == TabId::Optical && self.optical.is_none() {
            self.optical = Some(OpticalState::scanned());
        }
        if self.current() == TabId::Archives && self.archive.is_none() {
            self.archive = Some(ArchiveState::default());
        }
        if self.current() == TabId::Commander && self.commander.is_none() {
            self.commander = Some(commander::CommanderView::new());
        }
        if self.current() != TabId::Commander {
            self.tab_before_commander = Some(self.active);
        }
        if self.current() == TabId::Settings && self.settings.is_none() {
            self.settings = Some(SettingsState {
                config: crate::update::UpdateConfig::load(),
                sel: 0,
                status: None,
            });
        }
    }

    /// Settings tab keys. Returns `true` when consumed (Left/Right fall through
    /// so they still switch tabs).
    fn handle_settings_key(&mut self, code: KeyCode) -> bool {
        if self.settings.is_none() {
            self.settings = Some(SettingsState {
                config: crate::update::UpdateConfig::load(),
                sel: 0,
                status: None,
            });
        }
        let s = self.settings.as_mut().unwrap();
        match code {
            KeyCode::Up | KeyCode::Char('k') => {
                s.sel = s.sel.saturating_sub(1);
                true
            }
            KeyCode::Down | KeyCode::Char('j') => {
                s.sel = (s.sel + 1).min(SETTINGS_TOGGLES - 1);
                true
            }
            KeyCode::Enter | KeyCode::Char(' ') => {
                self.settings_toggle();
                true
            }
            // Check for updates now.
            KeyCode::Char('c') => {
                self.settings_check_now();
                true
            }
            _ => false,
        }
    }

    /// Run an update check against GitHub and report the result on the Settings
    /// status line. Only functional in a `tui-update` build (which links the
    /// reqwest client); otherwise it explains how to enable it.
    fn settings_check_now(&mut self) {
        #[cfg(feature = "tui-update")]
        {
            let repo = self
                .settings
                .as_ref()
                .map(|s| s.config.update_check.clone());
            let Some(cfg) = repo else { return };
            // A network round-trip: run it as a task so the screen stays live.
            self.start_task("Checking for updates", move |job| {
                job.set_can_cancel(false);
                let current = env!("APP_VERSION");
                let msg = match crate::update::check_for_updates(&cfg, current) {
                    Ok(info) if info.is_outdated => format!(
                        "Update available: {} (current {current}). Run `rb-cli update --apply`.",
                        info.latest_version
                    ),
                    Ok(_) => format!("Up to date (current {current})."),
                    Err(e) => format!("Update check failed: {e}"),
                };
                Ok(Box::new(move |app: &mut App| {
                    if let Some(s) = app.settings.as_mut() {
                        s.status = Some(msg);
                    }
                }) as TaskDone)
            });
        }
        #[cfg(not(feature = "tui-update"))]
        {
            if let Some(s) = self.settings.as_mut() {
                s.status = Some(
                    "Update checking isn't compiled into this build \
                     (rebuild with `--features tui-update`)."
                        .to_string(),
                );
            }
        }
    }

    /// Flip the selected preference and persist the whole `UpdateConfig`.
    fn settings_toggle(&mut self) {
        let Some(s) = self.settings.as_mut() else {
            return;
        };
        match s.sel {
            0 => s.config.update_check.enabled = !s.config.update_check.enabled,
            1 => s.config.file_associations_enabled = !s.config.file_associations_enabled,
            _ => {}
        }
        match s.config.save() {
            Ok(()) => s.status = Some("Saved to config.json.".to_string()),
            Err(e) => s.status = Some(format!("Save failed: {e}")),
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

    /// Backup screen keys. Returns `true` when consumed; `false` lets a key fall
    /// through to the shell (Left/Right switch tabs, `q` quits from idle steps).
    fn handle_backup_key(&mut self, code: KeyCode) -> bool {
        use crate::controller::backup::BackupAction;
        if self.backup.is_none() {
            self.backup = Some(BackupState::default());
        }
        // Source image file picker (modal).
        if let Some(b) = self.backup.as_mut() {
            if let Some(picker) = b.picker.as_mut() {
                let is_dir = matches!(picker.kind, PickKind::Dir);
                match picker.handle_key(code) {
                    Some(PickResult::Cancel) => b.picker = None,
                    Some(PickResult::Confirm(path)) => {
                        b.picker = None;
                        if is_dir {
                            b.out_dir = path.to_string_lossy().into_owned();
                        } else {
                            b.act(BackupAction::OpenImage { path, rename: true });
                            self.backup_source_chosen();
                        }
                    }
                    None => {}
                }
                return true;
            }
        }

        let step = self.backup.as_ref().unwrap().step;
        // Physical-disk chooser sub-mode.
        if self.backup.as_ref().map(|b| b.device_pick).unwrap_or(false) {
            let ndisks = self.disks.as_ref().map_or(0, |d| d.len());
            let b = self.backup.as_mut().unwrap();
            match code {
                KeyCode::Up | KeyCode::Char('k') => b.device_sel = b.device_sel.saturating_sub(1),
                KeyCode::Down | KeyCode::Char('j') => {
                    b.device_sel = (b.device_sel + 1).min(ndisks.saturating_sub(1))
                }
                KeyCode::Esc => b.device_pick = false,
                KeyCode::Enter => {
                    let sel = b.device_sel;
                    let dev = self.disks.as_ref().and_then(|d| d.get(sel)).cloned();
                    if let Some(device) = dev {
                        let b = self.backup.as_mut().unwrap();
                        b.device_pick = false;
                        b.act(BackupAction::SelectDevice { slot: sel, device });
                        self.backup_source_chosen();
                    }
                }
                _ => {}
            }
            return true;
        }

        // Start is handled outside the mutable borrow below.
        if step == BackupStep::Config
            && code == KeyCode::Enter
            && self.backup.as_ref().map(|b| b.field).unwrap_or(0) == 4
        {
            self.start_backup();
            return true;
        }

        let b = self.backup.as_mut().unwrap();
        match step {
            BackupStep::Source => match code {
                KeyCode::Enter | KeyCode::Char('o') => {
                    let recent = crate::update::load_recent(crate::update::RecentMode::Backup);
                    b.picker = Some(
                        FilePicker::new(PickKind::File, "Choose source image").with_recent(recent),
                    );
                    true
                }
                KeyCode::Char('d') => {
                    b.device_pick = true;
                    b.device_sel = 0;
                    true
                }
                _ => false,
            },
            BackupStep::Config => match code {
                KeyCode::Esc => {
                    b.step = BackupStep::Source;
                    true
                }
                KeyCode::Down => {
                    b.field = (b.field + 1) % BACKUP_FIELDS;
                    true
                }
                KeyCode::Up | KeyCode::BackTab => {
                    b.field = (b.field + BACKUP_FIELDS - 1) % BACKUP_FIELDS;
                    true
                }
                // Format / checksum fields: Left/Right (and Space) cycle choices;
                // on other fields Left/Right fall through to tab switching.
                KeyCode::Left | KeyCode::Right | KeyCode::Char(' ') if b.field == 2 => {
                    let n = BACKUP_FORMATS.len();
                    if code == KeyCode::Left {
                        b.format_sel = (b.format_sel + n - 1) % n;
                    } else {
                        b.format_sel = (b.format_sel + 1) % n;
                    }
                    let format = BACKUP_FORMATS[b.format_sel].1;
                    b.act(BackupAction::SetFormat(format));
                    true
                }
                KeyCode::Left | KeyCode::Right | KeyCode::Char(' ') if b.field == 3 => {
                    let n = BACKUP_CHECKSUMS.len();
                    if code == KeyCode::Left {
                        b.checksum_sel = (b.checksum_sel + n - 1) % n;
                    } else {
                        b.checksum_sel = (b.checksum_sel + 1) % n;
                    }
                    let checksum = BACKUP_CHECKSUMS[b.checksum_sel].1;
                    b.act(BackupAction::SetChecksum(checksum));
                    true
                }
                KeyCode::Tab if b.field == 0 => {
                    // Browse for the output folder (Dir picker); the confirm
                    // branch routes it to out_dir by its PickKind.
                    let mut p = FilePicker::new(PickKind::Dir, "Choose output folder");
                    p.input = b.out_dir.clone();
                    b.picker = Some(p);
                    true
                }
                KeyCode::Enter => {
                    b.field = (b.field + 1) % BACKUP_FIELDS;
                    true
                }
                KeyCode::Backspace => {
                    match b.field {
                        0 => {
                            b.out_dir.pop();
                        }
                        1 => {
                            let mut name = b.ctl.name().to_string();
                            name.pop();
                            b.act(BackupAction::SetName(name));
                        }
                        _ => {}
                    }
                    true
                }
                KeyCode::Char(c) if !c.is_control() && (b.field == 0 || b.field == 1) => {
                    if b.field == 0 {
                        b.out_dir.push(c);
                    } else {
                        let mut name = b.ctl.name().to_string();
                        name.push(c);
                        b.act(BackupAction::SetName(name));
                    }
                    true
                }
                _ => false,
            },
            BackupStep::Run => match code {
                KeyCode::Esc => {
                    if b.result().is_none() {
                        // Request cancel; the bar clears via cancel_progress.
                        self.cancel_progress();
                        if let Some(b) = self.backup.as_mut() {
                            if !b.busy() && b.result().is_none() {
                                b.step = BackupStep::Config;
                            }
                        }
                    } else {
                        // Done: clear and return to the config form.
                        self.progress = None;
                        b.cancel_note = false;
                        b.step = BackupStep::Config;
                    }
                    true
                }
                _ => true, // swallow keys while a backup is on screen
            },
        }
    }

    /// A source was picked: on to the config form, defaulting the output folder to the image's.
    fn backup_source_chosen(&mut self) {
        let Some(b) = self.backup.as_mut() else {
            return;
        };
        if b.out_dir.is_empty() {
            if let Some(dir) = b.ctl.default_destination() {
                b.out_dir = dir.display().to_string();
            }
        }
        b.step = BackupStep::Config;
        b.field = 0;
    }

    /// Hand the form to the controller and press its Start; it validates and runs the backup.
    fn start_backup(&mut self) {
        use crate::controller::backup::BackupAction;
        let Some(b) = self.backup.as_mut() else {
            return;
        };
        let out_dir = expand_tilde(b.out_dir.trim());
        let dest = (!out_dir.as_os_str().is_empty()).then_some(out_dir);
        b.act(BackupAction::SetDestination(dest));
        b.act(BackupAction::Start(None));
        b.cancel_note = false;
        b.op = "Starting...".to_string();
        b.step = BackupStep::Run;
        if b.busy() {
            self.progress = Some(Progress {
                shared: Arc::new(Mutex::new(ProgressShared::default())),
                tracker: RateTracker::default(),
                label: "Backing up".to_string(),
            });
        }
    }

    /// Restore screen keys. Returns `true` when consumed.
    fn handle_restore_key(&mut self, code: KeyCode) -> bool {
        if self.restore.is_none() {
            self.restore = Some(RestoreState::default());
        }
        // Source / target picker (modal).
        if self
            .restore
            .as_ref()
            .map(|r| r.picker.is_some())
            .unwrap_or(false)
        {
            let for_target = self.restore.as_ref().unwrap().picker_target;
            let res = self
                .restore
                .as_mut()
                .unwrap()
                .picker
                .as_mut()
                .unwrap()
                .handle_key(code);
            match res {
                Some(PickResult::Cancel) => self.restore.as_mut().unwrap().picker = None,
                Some(PickResult::Confirm(path)) => {
                    if for_target {
                        let r = self.restore.as_mut().unwrap();
                        r.target = path.to_string_lossy().into_owned();
                        r.picker = None;
                    } else {
                        self.restore.as_mut().unwrap().picker = None;
                        self.load_restore_source(path);
                    }
                }
                None => {}
            }
            return true;
        }

        // "Target type" chooser (modal): Image file vs Physical device.
        if self
            .restore
            .as_ref()
            .map(|r| r.target_type_menu.is_some())
            .unwrap_or(false)
        {
            let sel = self
                .restore
                .as_ref()
                .and_then(|r| r.target_type_menu)
                .unwrap_or(0);
            match code {
                KeyCode::Esc => self.restore.as_mut().unwrap().target_type_menu = None,
                KeyCode::Up | KeyCode::Char('k') => {
                    self.restore.as_mut().unwrap().target_type_menu = Some(sel.saturating_sub(1));
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    self.restore.as_mut().unwrap().target_type_menu = Some((sel + 1).min(1));
                }
                KeyCode::Enter => {
                    let r = self.restore.as_mut().unwrap();
                    r.target_type_menu = None;
                    if sel == 0 {
                        // Image file: open the shared path picker.
                        let mut p = FilePicker::new(PickKind::Any, "Choose target image path");
                        p.input = if r.target_is_device {
                            String::new()
                        } else {
                            r.target.clone()
                        };
                        r.picker = Some(p);
                        r.picker_target = true;
                        r.target_is_device = false;
                    } else {
                        // Physical device: open the device chooser.
                        r.device_pick = true;
                        r.device_sel = 0;
                    }
                }
                _ => {}
            }
            return true;
        }

        // Physical-device target chooser (modal).
        if self
            .restore
            .as_ref()
            .map(|r| r.device_pick)
            .unwrap_or(false)
        {
            let ndisks = self.disks.as_ref().map_or(0, |d| d.len());
            let r = self.restore.as_mut().unwrap();
            match code {
                KeyCode::Up | KeyCode::Char('k') => r.device_sel = r.device_sel.saturating_sub(1),
                KeyCode::Down | KeyCode::Char('j') => {
                    r.device_sel = (r.device_sel + 1).min(ndisks.saturating_sub(1))
                }
                KeyCode::Esc => r.device_pick = false,
                KeyCode::Enter => {
                    let sel = r.device_sel;
                    let dev = self
                        .disks
                        .as_ref()
                        .and_then(|d| d.get(sel))
                        .map(|d| d.path.display().to_string());
                    if let Some(path) = dev {
                        let r = self.restore.as_mut().unwrap();
                        r.device_pick = false;
                        r.target = path;
                        r.target_is_device = true;
                        // The writer reads the target's partitions, so an image can go into one.
                        self.select_write_device();
                    }
                }
                _ => {}
            }
            return true;
        }

        // Destructive device-write confirmation (modal): only `y` proceeds.
        if self
            .restore
            .as_ref()
            .map(|r| r.confirm_device.is_some())
            .unwrap_or(false)
        {
            match code {
                KeyCode::Char('y') | KeyCode::Char('Y') => {
                    if let Some(r) = self.restore.as_mut() {
                        r.confirm_device = None;
                    }
                    self.start_restore_confirmed();
                }
                _ => {
                    if let Some(r) = self.restore.as_mut() {
                        r.confirm_device = None;
                    }
                }
            }
            return true;
        }

        let step = self.restore.as_ref().unwrap().step;
        // Start handled outside the borrow. A device target routes through a
        // destructive-write confirmation first; an image target starts directly.
        if step == RestoreStep::Config
            && code == KeyCode::Enter
            && self.restore.as_ref().map(|r| r.field).unwrap_or(0) == 3
        {
            self.request_restore_start();
            return true;
        }

        let r = self.restore.as_mut().unwrap();
        match step {
            RestoreStep::Source => match code {
                KeyCode::Enter | KeyCode::Char('o') => {
                    let recent = crate::update::load_recent(crate::update::RecentMode::Restore);
                    let prompt = if r.source_is_image {
                        "Choose a disk image to write"
                    } else {
                        "Choose backup folder or .cbk"
                    };
                    r.picker = Some(FilePicker::new(PickKind::Any, prompt).with_recent(recent));
                    r.picker_target = false;
                    true
                }
                // Same toggle key the rest of the wizard uses for a choice.
                KeyCode::Left | KeyCode::Right | KeyCode::Char(' ') => {
                    r.source_is_image = !r.source_is_image;
                    r.loaded = false;
                    r.result = None;
                    true
                }
                _ => false,
            },
            RestoreStep::Config => match code {
                KeyCode::Esc => {
                    r.step = RestoreStep::Source;
                    true
                }
                KeyCode::Down => {
                    r.field = (r.field + 1) % RESTORE_FIELDS;
                    true
                }
                KeyCode::Up | KeyCode::BackTab => {
                    r.field = (r.field + RESTORE_FIELDS - 1) % RESTORE_FIELDS;
                    true
                }
                // Image mode reuses this row for the target region, since size
                // mode means nothing when pouring a fixed image.
                KeyCode::Left | KeyCode::Right | KeyCode::Char(' ')
                    if r.field == 1 && r.source_is_image =>
                {
                    use crate::controller::Controller as _;
                    r.writer.apply(
                        crate::controller::disk_write::DiskWriteAction::CycleTargetPartition,
                    );
                    true
                }
                KeyCode::Left | KeyCode::Right | KeyCode::Char(' ') if r.field == 1 => {
                    let n = RESTORE_SIZE_LABELS.len();
                    r.size_sel = if code == KeyCode::Left {
                        (r.size_sel + n - 1) % n
                    } else {
                        (r.size_sel + 1) % n
                    };
                    true
                }
                KeyCode::Left | KeyCode::Right | KeyCode::Char(' ') if r.field == 2 => {
                    let n = RESTORE_ALIGN_LABELS.len();
                    r.align_sel = if code == KeyCode::Left {
                        (r.align_sel + n - 1) % n
                    } else {
                        (r.align_sel + 1) % n
                    };
                    true
                }
                // Tab on the target field opens the "Target type" chooser
                // (Image file vs Physical device), so the destructive device
                // path is an explicit, discoverable choice — not a keystroke
                // that collides with typing a path.
                KeyCode::Tab if r.field == 0 => {
                    r.target_type_menu = Some(0);
                    true
                }
                KeyCode::Enter => {
                    r.field = (r.field + 1) % RESTORE_FIELDS;
                    true
                }
                KeyCode::Backspace if r.field == 0 => {
                    r.target.pop();
                    r.target_is_device = false;
                    true
                }
                KeyCode::Char(c) if !c.is_control() && r.field == 0 => {
                    r.target.push(c);
                    r.target_is_device = false;
                    true
                }
                _ => false,
            },
            RestoreStep::Run => match code {
                KeyCode::Esc => {
                    let running = r.result.is_none();
                    if running {
                        self.cancel_progress();
                        if let Some(r) = self.restore.as_mut() {
                            r.result = Some("Cancel requested...".to_string());
                            r.is_error = true;
                        }
                    } else {
                        self.progress = None;
                        if let Some(r) = self.restore.as_mut() {
                            r.result = None;
                            r.step = RestoreStep::Config;
                        }
                    }
                    true
                }
                _ => true,
            },
        }
    }

    /// Load a chosen backup source (native folder or `.cbk`), read its metadata
    /// for defaults, and advance to the config form. Errors show on the source
    /// screen.
    /// Hand the device named in the target field to the writer, which reads its partitions.
    fn select_write_device(&mut self) {
        use crate::controller::disk_write::DiskWriteAction;
        use crate::controller::Controller as _;
        let Some(r) = self.restore.as_mut() else {
            return;
        };
        let target = std::path::PathBuf::from(r.target.trim());
        let picked = self.disks.as_ref().and_then(|d| {
            d.iter()
                .enumerate()
                .find(|(_, dev)| dev.path == target)
                .map(|(row, dev)| (row, dev.clone()))
        });
        if let Some((row, device)) = picked {
            r.writer
                .apply(DiskWriteAction::SelectDevice { row, device });
            r.writer.wait_idle();
        }
    }

    /// Pour the chosen image onto the target, whole-disk or into one partition.
    fn start_image_write(&mut self, target: std::path::PathBuf, is_device: bool) {
        use crate::controller::disk_write::{DiskWriteAction, DiskWriteController};
        use crate::controller::{Controller as _, Effect};
        let same_device = self
            .restore
            .as_ref()
            .and_then(|r| r.writer.target_device())
            .is_some_and(|d| d.path == target);
        if is_device && !same_device {
            self.select_write_device();
        }
        let Some(r) = self.restore.as_mut() else {
            return;
        };
        if !is_device {
            r.writer.apply(DiskWriteAction::SetTargetImage(target));
        }
        // The device was confirmed by this screen's own prompt, so the writer's question is answered here.
        fn answer(w: &mut DiskWriteController, fx: Vec<Effect>) {
            for e in fx {
                let reply = match e {
                    Effect::AskText(spec) => DiskWriteAction::Answered {
                        purpose: spec.purpose,
                        text: spec.taken.first().cloned(),
                    },
                    Effect::Confirm(spec) => DiskWriteAction::Confirmed {
                        purpose: spec.purpose,
                        yes: true,
                    },
                    Effect::PickFile(_)
                    | Effect::PickFolder(_)
                    | Effect::AskPassword(_)
                    | Effect::Message(_)
                    | Effect::OpenUrl(_)
                    | Effect::CopyToClipboard(_)
                    | Effect::CloseScreen
                    | Effect::Quit => continue,
                };
                w.apply(reply);
            }
        }
        if !r.writer.can_write() {
            r.result = Some("Choose a target.".to_string());
            r.is_error = true;
            r.step = RestoreStep::Run;
            return;
        }
        let fx = r.writer.apply(DiskWriteAction::Write);
        answer(&mut r.writer, fx);
        r.step = RestoreStep::Run;
        if !r.writer.is_writing() {
            r.result = Some(
                r.writer
                    .error()
                    .unwrap_or("The image write did not start.")
                    .to_string(),
            );
            r.is_error = true;
            return;
        }
        r.writing = true;
        r.op = "Writing image...".to_string();
        r.result = None;
        r.is_error = false;
    }

    /// Follow the writer's job into the shared result/op fields the Run step already renders.
    fn poll_image_write(&mut self) {
        use crate::controller::Controller as _;
        let Some(r) = self.restore.as_mut() else {
            return;
        };
        let _ = r.writer.tick();
        let _ = r.writer.take_log();
        if !r.writing {
            return;
        }
        if r.writer.is_writing() {
            if let Some(snap) = r.writer.progress() {
                if let crate::controller::disk_write::Progress::Determinate {
                    done, total, ..
                } = snap.progress
                {
                    if total > 0 {
                        r.op = format!(
                            "Writing image: {} / {}",
                            format_size(done),
                            format_size(total),
                        );
                    }
                }
            }
            return;
        }
        r.writing = false;
        r.op = String::new();
        match r.writer.result() {
            Some(Ok(n)) => {
                r.result = Some(format!("Wrote {} to the target.", format_size(*n)));
                r.is_error = false;
            }
            Some(Err(e)) => {
                r.result = Some(format!("Image write failed: {e}"));
                r.is_error = true;
            }
            None => {}
        }
    }

    fn load_restore_source(&mut self, path: std::path::PathBuf) {
        use crate::controller::restore::RestoreAction;
        let Some(r) = self.restore.as_mut() else {
            return;
        };
        // Image-write source: no metadata to read, just the decoded length.
        if r.source_is_image {
            r.act(RestoreAction::OpenWriteImage(path.clone()));
            r.ctl.wait_idle();
            r.after();
            match r.ctl.write_image().map(|(_, n)| n).filter(|&n| n > 0) {
                Some(n) => {
                    crate::update::push_recent(
                        crate::update::RecentMode::Restore,
                        &path.to_string_lossy(),
                    );
                    r.backup_folder = path.to_string_lossy().into_owned();
                    r.image_size = n;
                    r.source_size = n;
                    r.part_count = 0;
                    r.loaded = true;
                    r.result = None;
                    r.is_error = false;
                    r.step = RestoreStep::Config;
                    use crate::controller::Controller as _;
                    r.writer
                        .apply(crate::controller::disk_write::DiskWriteAction::Open(
                            crate::controller::disk_write::WriteOpen::image(path.clone(), n),
                        ));
                    r.writer.wait_idle();
                    // A new source starts the writer with no target; keep the one on the form.
                    if r.target_is_device {
                        self.select_write_device();
                    }
                }
                None => {
                    r.result = Some(format!("Cannot read {} as a disk image", path.display()));
                    r.is_error = true;
                }
            }
            return;
        }
        // The controller unpacks a .cbk and reads the metadata in a job; `tick` follows it.
        r.backup_folder = path.to_string_lossy().into_owned();
        r.result = None;
        r.is_error = false;
        r.loading = true;
        r.act(RestoreAction::OpenBackup(path));
    }

    /// Handle the "Start" action: an image-file target starts the restore
    /// directly; a physical-device target first raises a destructive-write
    /// confirmation (naming the device) that `start_restore_confirmed` clears.
    fn request_restore_start(&mut self) {
        let Some(r) = self.restore.as_ref() else {
            return;
        };
        if r.target_is_device {
            if r.target.trim().is_empty() {
                let r = self.restore.as_mut().unwrap();
                r.result = Some("Choose a target device.".to_string());
                r.is_error = true;
                r.step = RestoreStep::Run;
                return;
            }
            // Name the device (with its media label if we have it) in the prompt.
            let path = r.target.clone();
            let label = self
                .disks
                .as_ref()
                .and_then(|d| d.iter().find(|dev| dev.path.display().to_string() == path))
                .map(|dev| dev.display_name())
                .unwrap_or_else(|| path.clone());
            self.restore.as_mut().unwrap().confirm_device = Some((path, label));
        } else {
            self.start_restore_confirmed();
        }
    }

    /// Hand the form to the controller, press its Start and answer its confirmation.
    fn start_restore_confirmed(&mut self) {
        use crate::controller::restore::{
            AlignmentChoice, RestoreAction, RestoreMode, SizeMode, TargetSlot,
        };
        let Some(r) = self.restore.as_ref() else {
            return;
        };
        let is_device = r.target_is_device;
        let target = if is_device {
            std::path::PathBuf::from(r.target.trim())
        } else {
            expand_tilde(r.target.trim())
        };
        if target.as_os_str().is_empty() {
            let r = self.restore.as_mut().unwrap();
            r.result = Some("Enter a target image path.".to_string());
            r.is_error = true;
            r.step = RestoreStep::Run;
            return;
        }
        if r.source_is_image {
            self.start_image_write(target, is_device);
            return;
        }
        let device = if is_device {
            self.disks.as_ref().and_then(|d| {
                d.iter()
                    .enumerate()
                    .find(|(_, dev)| dev.path == target)
                    .map(|(row, dev)| (row, dev.clone()))
            })
        } else {
            None
        };
        let r = self.restore.as_mut().unwrap();
        r.act(RestoreAction::SetMode(RestoreMode::FullDisk));
        r.act(RestoreAction::SetRemote(false));
        r.act(RestoreAction::SetTargetIsDevice(
            TargetSlot::FullDisk,
            is_device,
        ));
        match device {
            Some((row, device)) => r.act(RestoreAction::SelectDevice {
                slot: TargetSlot::FullDisk,
                row,
                device,
            }),
            None => r.act(RestoreAction::ClearDevice(TargetSlot::FullDisk)),
        }
        r.act(RestoreAction::SetTargetImage(
            TargetSlot::FullDisk,
            (!is_device).then_some(target),
        ));
        r.act(RestoreAction::SetAlignment(if r.align_sel == 1 {
            AlignmentChoice::Modern1MB
        } else {
            AlignmentChoice::Original
        }));
        let mode = if r.size_sel == 1 {
            SizeMode::Minimum
        } else {
            SizeMode::Original
        };
        let rows: Vec<(usize, u32)> = r
            .ctl
            .size_rows()
            .iter()
            .map(|row| (row.index, row.custom_mib))
            .collect();
        for (index, custom_mib) in rows {
            r.act(RestoreAction::SetPartitionSize {
                index,
                mode,
                custom_mib,
            });
        }
        use crate::controller::Controller as _;
        let fx = r.ctl.apply(RestoreAction::Start);
        r.after();
        r.step = RestoreStep::Run;
        // The device prompt (or, for an image file, the path typed) was the confirmation.
        let Some(crate::controller::Effect::Confirm(spec)) = fx.into_iter().next() else {
            r.result = Some(match r.ctl.last_result() {
                Some(Err(e)) => e.clone(),
                _ => "Restore could not start.".to_string(),
            });
            r.is_error = true;
            return;
        };
        r.act(RestoreAction::Confirmed {
            purpose: spec.purpose,
            yes: true,
        });
        r.restoring = true;
        r.result = None;
        r.is_error = false;
        r.op = "Starting...".to_string();
        self.progress = Some(Progress {
            shared: Arc::new(Mutex::new(ProgressShared::default())),
            tracker: RateTracker::default(),
            label: "Restoring".to_string(),
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

    /// Optical (rip) screen keys. Returns `true` when consumed.
    #[cfg(feature = "optical")]
    fn handle_optical_key(&mut self, code: KeyCode) -> bool {
        if self.optical.is_none() {
            self.optical = Some(OpticalState::scanned());
        }
        // Output-path picker (modal).
        if self
            .optical
            .as_ref()
            .map(|o| o.picker.is_some())
            .unwrap_or(false)
        {
            let res = self
                .optical
                .as_mut()
                .unwrap()
                .picker
                .as_mut()
                .unwrap()
                .handle_key(code);
            match res {
                Some(PickResult::Cancel) => self.optical.as_mut().unwrap().picker = None,
                Some(PickResult::Confirm(path)) => {
                    let o = self.optical.as_mut().unwrap();
                    o.output = path.to_string_lossy().into_owned();
                    o.picker = None;
                }
                None => {}
            }
            return true;
        }

        // Optical-image-operations launcher menu (modal).
        if self
            .optical
            .as_ref()
            .map(|o| o.image_menu.is_some())
            .unwrap_or(false)
        {
            let sel = self
                .optical
                .as_ref()
                .and_then(|o| o.image_menu)
                .unwrap_or(0);
            match code {
                KeyCode::Esc => self.optical.as_mut().unwrap().image_menu = None,
                KeyCode::Up | KeyCode::Char('k') => {
                    self.optical.as_mut().unwrap().image_menu = Some(sel.saturating_sub(1));
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    self.optical.as_mut().unwrap().image_menu =
                        Some((sel + 1).min(OPTICAL_IMAGE_OPS.len() - 1));
                }
                KeyCode::Enter => {
                    self.optical.as_mut().unwrap().image_menu = None;
                    let tmpl = OPTICAL_IMAGE_OPS.get(sel).map(|t| t.1).unwrap_or("");
                    self.palette_input = Some(tmpl.to_string());
                }
                _ => {}
            }
            return true;
        }

        let step = self.optical.as_ref().unwrap().step;
        // `i` opens the optical-image-operations launcher from any step.
        if code == KeyCode::Char('i') {
            self.optical.as_mut().unwrap().image_menu = Some(0);
            return true;
        }
        if step == OpticalStep::Config
            && code == KeyCode::Enter
            && self.optical.as_ref().map(|o| o.field).unwrap_or(0) == 3
        {
            self.start_optical_rip();
            return true;
        }

        let o = self.optical.as_mut().unwrap();
        match step {
            OpticalStep::Drives => match code {
                KeyCode::Up | KeyCode::Char('k') => {
                    o.drive_sel = o.drive_sel.saturating_sub(1);
                    true
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    o.drive_sel = (o.drive_sel + 1).min(o.ctl.drive_count().saturating_sub(1));
                    true
                }
                KeyCode::Char('r') => {
                    use crate::controller::Controller as _;
                    o.ctl
                        .apply(crate::controller::optical::OpticalAction::RefreshDrives);
                    o.drive_sel = 0;
                    true
                }
                KeyCode::Enter => {
                    if let Some((name, path)) = o.ctl.drive_names().get(o.drive_sel).cloned() {
                        use crate::controller::Controller as _;
                        o.ctl
                            .apply(crate::controller::optical::OpticalAction::SelectDrive(
                                Some(o.drive_sel),
                            ));
                        o.device_path = path;
                        o.device_name = name;
                        if o.output.is_empty() {
                            let ext = if o.format_sel == 1 { "cue" } else { "iso" };
                            let home = dirs::home_dir().unwrap_or_default();
                            o.output = home.join(format!("disc.{ext}")).display().to_string();
                        }
                        o.step = OpticalStep::Config;
                        o.field = 0;
                    }
                    true
                }
                _ => false,
            },
            OpticalStep::Config => match code {
                KeyCode::Esc => {
                    o.step = OpticalStep::Drives;
                    true
                }
                KeyCode::Down => {
                    o.field = (o.field + 1) % OPTICAL_FIELDS;
                    true
                }
                KeyCode::Up | KeyCode::BackTab => {
                    o.field = (o.field + OPTICAL_FIELDS - 1) % OPTICAL_FIELDS;
                    true
                }
                KeyCode::Left | KeyCode::Right | KeyCode::Char(' ') if o.field == 1 => {
                    let n = RIP_FORMATS.len();
                    o.format_sel = if code == KeyCode::Left {
                        (o.format_sel + n - 1) % n
                    } else {
                        (o.format_sel + 1) % n
                    };
                    true
                }
                KeyCode::Char(' ') if o.field == 2 => {
                    o.eject = !o.eject;
                    true
                }
                KeyCode::Tab if o.field == 0 => {
                    let mut p = FilePicker::new(PickKind::Any, "Choose output path");
                    p.input = o.output.clone();
                    o.picker = Some(p);
                    true
                }
                KeyCode::Enter => {
                    o.field = (o.field + 1) % OPTICAL_FIELDS;
                    true
                }
                KeyCode::Backspace if o.field == 0 => {
                    o.output.pop();
                    true
                }
                KeyCode::Char(c) if !c.is_control() && o.field == 0 => {
                    o.output.push(c);
                    true
                }
                _ => false,
            },
            OpticalStep::Run => match code {
                KeyCode::Esc => {
                    let running = o.ctl.is_running();
                    if running {
                        self.cancel_progress();
                        if let Some(o) = self.optical.as_mut() {
                            o.result = Some("Cancel requested...".to_string());
                            o.is_error = true;
                        }
                    } else {
                        self.progress = None;
                        if let Some(o) = self.optical.as_mut() {
                            o.result = None;
                            o.cancel_note = false;
                            o.step = OpticalStep::Config;
                        }
                    }
                    true
                }
                _ => true,
            },
        }
    }

    /// Hand the form to the controller and start the rip.
    #[cfg(feature = "optical")]
    fn start_optical_rip(&mut self) {
        use crate::controller::optical::OpticalAction;
        use crate::controller::Controller as _;
        let Some(o) = self.optical.as_mut() else {
            return;
        };
        let output = expand_tilde(o.output.trim());
        if output.as_os_str().is_empty() {
            o.result = Some("Enter an output path.".to_string());
            o.is_error = true;
            o.step = OpticalStep::Run;
            return;
        }
        let format = RIP_FORMATS[o.format_sel.min(RIP_FORMATS.len() - 1)].1;
        o.ctl.apply(OpticalAction::SetOutputFormat(format));
        o.ctl.apply(OpticalAction::SetEjectAfter(o.eject));
        o.ctl.set_output_path(output);
        o.ctl.apply(OpticalAction::Start(None));
        o.result = None;
        o.is_error = false;
        o.cancel_note = false;
        o.step = OpticalStep::Run;
        if !o.ctl.is_running() {
            let why = o
                .ctl
                .take_log()
                .into_iter()
                .last()
                .map(|(_, m)| m)
                .unwrap_or_else(|| "No drive selected".to_string());
            o.result = Some(format!("Cannot start the rip: {why}"));
            o.is_error = true;
            return;
        }
        o.op = "Starting...".to_string();
        self.progress = Some(Progress {
            shared: Arc::new(Mutex::new(ProgressShared::default())),
            tracker: RateTracker::default(),
            label: "Ripping".to_string(),
        });
    }

    /// Archives screen keys. Returns `true` when consumed.
    fn handle_archive_key(&mut self, code: KeyCode) -> bool {
        if self.archive.is_none() {
            self.archive = Some(ArchiveState::default());
        }
        // Archive / destination picker (modal).
        if self
            .archive
            .as_ref()
            .map(|a| a.picker.is_some())
            .unwrap_or(false)
        {
            let for_dest = self.archive.as_ref().unwrap().picker_dest;
            let res = self
                .archive
                .as_mut()
                .unwrap()
                .picker
                .as_mut()
                .unwrap()
                .handle_key(code);
            match res {
                Some(PickResult::Cancel) => self.archive.as_mut().unwrap().picker = None,
                Some(PickResult::Confirm(path)) => {
                    self.archive.as_mut().unwrap().picker = None;
                    if for_dest {
                        self.archive_extract(path);
                    } else {
                        self.load_archive(path);
                    }
                }
                None => {}
            }
            return true;
        }

        let loaded = self
            .archive
            .as_ref()
            .map(|a| a.ctl.is_loaded())
            .unwrap_or(false);
        let page = page_step() as isize;
        let a = self.archive.as_mut().unwrap();
        if !loaded {
            return match code {
                KeyCode::Enter | KeyCode::Char('o') => {
                    let recent = crate::update::load_recent(crate::update::RecentMode::Archives);
                    a.picker = Some(
                        FilePicker::new(PickKind::File, "Open a Mac archive").with_recent(recent),
                    );
                    a.picker_dest = false;
                    true
                }
                // `c` prefills the `:` palette with `archive create` (from host
                // files — the supported create form; the palette runs it through
                // shared CLI dispatch). Creating from image contents stays a
                // stage-to-host-then-archive job, deferred.
                KeyCode::Char('c') => {
                    self.palette_input =
                        Some("archive create \"<OUTPUT.sit>\" <HOST_FILE> ...".to_string());
                    true
                }
                _ => false,
            };
        }
        // Archive loaded: browse the entry list, choose format, extract.
        let n = a.entries.len() as isize;
        match code {
            KeyCode::Up | KeyCode::Char('k') => {
                a.list_sel = a.list_sel.saturating_sub(1);
                true
            }
            KeyCode::Down | KeyCode::Char('j') => {
                a.list_sel = ((a.list_sel as isize + 1).min(n - 1).max(0)) as usize;
                true
            }
            KeyCode::PageUp => {
                a.list_sel = (a.list_sel as isize - page).max(0) as usize;
                true
            }
            KeyCode::PageDown => {
                a.list_sel = ((a.list_sel as isize + page).min(n - 1).max(0)) as usize;
                true
            }
            KeyCode::Home => {
                a.list_sel = 0;
                true
            }
            KeyCode::End => {
                a.list_sel = (n - 1).max(0) as usize;
                true
            }
            KeyCode::Char('f') => {
                a.fork_fmt_sel = (a.fork_fmt_sel + 1) % ARCHIVE_FORK_FORMATS.len();
                true
            }
            KeyCode::Char('e') => {
                a.picker = Some(FilePicker::new(PickKind::Dir, "Extract to folder"));
                a.picker_dest = true;
                a.status = None;
                true
            }
            KeyCode::Esc => {
                // Close the archive, back to the open prompt.
                *a = ArchiveState::default();
                true
            }
            _ => false,
        }
    }

    /// Open a Mac archive in the controller; the list is rendered when the job ends.
    fn load_archive(&mut self, path: std::path::PathBuf) {
        let Some(a) = self.archive.as_mut() else {
            return;
        };
        a.act(crate::controller::archives::ArchivesAction::Open(
            path.clone(),
        ));
        a.awaiting_open = Some(path);
        a.status = Some("Opening the archive...".to_string());
        a.is_error = false;
    }

    /// Extract the open archive to `dest` in the chosen fork format.
    fn archive_extract(&mut self, dest: std::path::PathBuf) {
        use crate::controller::archives::ArchivesAction;
        let Some(a) = self.archive.as_mut() else {
            return;
        };
        if !a.ctl.is_loaded() || a.ctl.is_extracting() {
            return;
        }
        let format = ARCHIVE_FORK_FORMATS[a.fork_fmt_sel.min(ARCHIVE_FORK_FORMATS.len() - 1)].1;
        a.act(ArchivesAction::SetFormat(format));
        a.act(ArchivesAction::ExtractAllTo(dest));
        a.awaiting_extract = true;
        a.status = Some("Extracting...".to_string());
        a.is_error = false;
    }

    /// Commander screen keys. Returns `true` when consumed.
    /// Whether the Inspect tab is showing its top-level selectable disk list.
    fn inspect_list_active(&self) -> bool {
        self.current() == TabId::Inspect && self.opened.is_none()
    }

    /// Number of selectable rows in the active tab (0 = the pane just scrolls).
    fn row_count(&self) -> usize {
        // Opened image → its partition table is the selectable list.
        if let Some(Opened::Image { parts, .. }) = &self.opened {
            return parts.len();
        }
        if self.inspect_list_active() {
            self.disks.as_ref().map_or(0, |d| d.len())
        } else {
            0
        }
    }

    /// Per-frame update: feed the progress tracker and drop a finished task's
    /// worker (the bar stays until the user presses Esc, showing 100%).
    fn tick(&mut self) {
        self.poll_task();
        // Drive the Backup controller and mirror its run into the visual bar.
        if let Some(b) = self.backup.as_mut() {
            use crate::controller::backup::BackupAction;
            use crate::controller::Controller as _;
            let _ = b.ctl.tick();
            // The TUI has no VHD window: one .vhd per partition, as before.
            if b.ctl.vhd_open() {
                b.act(BackupAction::VhdWholeDisk(false));
                b.act(BackupAction::VhdStart);
            }
            b.after();
            let bar = b.ctl.progress();
            if let Some(bar) = &bar {
                b.op = bar.operation.clone();
            }
            if b.step == BackupStep::Run {
                let done = !b.busy();
                if let Some(prog) = self.progress.as_ref() {
                    if let Ok(mut sh) = prog.shared.lock() {
                        if let Some(bar) = &bar {
                            sh.current = bar.current_bytes;
                            sh.total = bar.total_bytes;
                        }
                        sh.done = done;
                    }
                }
            }
        }
        // An image write reports through the writer's job.
        let writing = self.restore.as_ref().is_some_and(|r| r.writing);
        if writing {
            self.poll_image_write();
            if let Some(r) = self.restore.as_ref() {
                let snap = r.writer.progress();
                if let (Some(prog), Some(snap)) = (self.progress.as_ref(), snap) {
                    if let Ok(mut sh) = prog.shared.lock() {
                        if let crate::controller::disk_write::Progress::Determinate {
                            done,
                            total,
                            ..
                        } = snap.progress
                        {
                            sh.current = done;
                            sh.total = total;
                        }
                        sh.done = !r.writer.is_writing();
                    }
                }
            }
        }
        // Drive the Restore controller and mirror its run into the visual bar.
        if let Some(r) = self.restore.as_mut() {
            use crate::controller::Controller as _;
            let _ = r.ctl.tick();
            r.after();
            if r.step == RestoreStep::Run && !writing {
                let bar = r.ctl.progress();
                if let Some(prog) = self.progress.as_ref() {
                    if let Ok(mut sh) = prog.shared.lock() {
                        if let Some(bar) = &bar {
                            sh.current = bar.current_bytes;
                            sh.total = bar.total_bytes;
                        }
                        sh.done = !r.ctl.is_running();
                    }
                }
            }
        }
        // Drive the archives controller: a finished open or extraction becomes the status.
        if let Some(a) = self.archive.as_mut() {
            a.poll();
        }
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
        // Drive the Optical controller and mirror its rip into the visual bar.
        #[cfg(feature = "optical")]
        if let Some(o) = self.optical.as_mut() {
            use crate::controller::Controller as _;
            let _ = o.ctl.tick();
            let _ = o.ctl.take_log();
            let running = o.ctl.is_running();
            if let Some(bar) = o.ctl.progress() {
                o.op = bar.operation.clone();
                if let Some(prog) = self.progress.as_ref() {
                    if let Ok(mut sh) = prog.shared.lock() {
                        sh.current = bar.current_bytes;
                        sh.total = bar.total_bytes;
                        sh.done = false;
                    }
                }
            }
            if o.step == OpticalStep::Run && !running && (o.result.is_none() || o.cancel_note) {
                if let Some(last) = o.ctl.last_result() {
                    if let Some(prog) = self.progress.as_ref() {
                        if let Ok(mut sh) = prog.shared.lock() {
                            sh.done = true;
                        }
                    }
                    match last {
                        Err(e) => {
                            o.result = Some(format!("Rip failed: {e}"));
                            o.is_error = true;
                        }
                        Ok(()) => {
                            o.result = Some("Rip complete.".to_string());
                            o.is_error = false;
                        }
                    }
                    o.cancel_note = false;
                }
            }
        }
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

        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1), // tab bar + version
                Constraint::Min(3),    // body
                Constraint::Length(1), // footer key bar
            ])
            .split(area);

        self.draw_tab_bar(frame, rows[0]);
        self.draw_body(frame, rows[1]);
        self.draw_footer(frame, rows[2]);

        if self.progress.is_some() {
            self.draw_progress(frame, area);
        }
        if let Some(ex) = &self.explorer {
            let popup = centered_rect(
                area.width.saturating_sub(4).min(124),
                area.height.saturating_sub(2),
                area,
            );
            let outer = self.pane_block(&format!("Explorer  {}", ex.explorer_label()), true);
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
        // The Inspect disk list is a real selectable widget; so is an opened
        // image's partition table. Everything else (drill-down detail, opened
        // backup, placeholder tabs) is scrollable text.
        if self.inspect_list_active() {
            self.draw_inspect_list(frame, area);
            return;
        }
        if let Some(Opened::Image {
            path,
            size,
            parts,
            disk,
        }) = &self.opened
        {
            self.draw_partition_list(frame, area, path, *size, parts, disk.as_deref());
            return;
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
        let disks = self.disks.as_deref().unwrap_or(&[]);
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

    /// The opened image's partition table: a selectable list; Enter opens the
    /// selected partition's filesystem in the Explorer.
    fn draw_partition_list(
        &self,
        frame: &mut Frame,
        area: Rect,
        path: &str,
        size: u64,
        parts: &[PartRow],
        disk: Option<&DiskDevice>,
    ) {
        let name = basename(path);

        // Opened from the disk list: the hardware facts live in a header above
        // the table rather than on a screen of their own. They describe the same
        // disk, and splitting them meant an extra keypress nothing advertised.
        let area = match disk {
            Some(d) => {
                let header = self.disk_header_lines(d);
                // +2 for the block's own borders.
                let height = (header.len() as u16 + 2).min(area.height.saturating_sub(3));
                let rows =
                    Layout::vertical([Constraint::Length(height), Constraint::Min(3)]).split(area);
                frame.render_widget(
                    Paragraph::new(Text::from(header))
                        .block(self.pane_block(&format!("Disk  {}", d.path.display()), false)),
                    rows[0],
                );
                rows[1]
            }
            None => area,
        };

        if parts.is_empty() {
            let msg = match &self.status {
                // A device that could not be read: say so here, where the user
                // is looking, not only in the footer.
                Some(s) => format!("No partition table could be read.\n\n{s}"),
                None => "No partitions detected.".to_string(),
            };
            frame.render_widget(
                Paragraph::new(msg).block(self.pane_block(&format!("Partitions  {name}"), true)),
                area,
            );
            return;
        }
        let items: Vec<ListItem> = parts
            .iter()
            .map(|p| {
                ListItem::new(Line::from(vec![
                    Span::styled(format!("{:<22}", p.label), self.palette.accent()),
                    Span::raw(format!("{:>10}  {}", format_size(p.size), p.fs_hint)),
                ]))
            })
            .collect();
        // With a header above, the title names the table rather than repeating
        // the disk; on its own it still identifies the image.
        let subject = if disk.is_some() {
            format!("Partitions on {name}")
        } else {
            format!("Image  {name} ({})", format_size(size))
        };
        let title = match &self.status {
            Some(s) => format!("{subject}  -  {s}"),
            None => format!("{subject}  [{} partition(s)]  Enter=browse", parts.len()),
        };
        let list = List::new(items)
            .block(self.pane_block(&title, true))
            .highlight_style(Style::default().add_modifier(Modifier::REVERSED | Modifier::BOLD))
            .highlight_symbol("> ");
        let mut state = ListState::default();
        state.select(Some(self.selection.min(parts.len() - 1)));
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

    /// The Backup screen: source -> config -> run. The progress bar overlay
    /// (drawn by `draw_progress`) sits on top during the Run step.
    fn draw_backup(&self, frame: &mut Frame, area: Rect, b: &BackupState) {
        let title = match b.step {
            BackupStep::Source => "Backup  -  step 1/3: source",
            BackupStep::Config => "Backup  -  step 2/3: options",
            BackupStep::Run => "Backup  -  step 3/3: run",
        };
        let mut lines: Vec<Line> = Vec::new();
        match b.step {
            BackupStep::Source => {
                lines.push(Line::styled(
                    "Choose what to back up:",
                    self.palette.accent(),
                ));
                lines.push(Line::raw(""));
                lines.push(Line::from(vec![
                    Span::styled("  Enter / o  ", self.palette.accent()),
                    Span::raw("choose a source image file"),
                ]));
                let dev_line = if self.elevated {
                    Span::raw("choose a physical disk")
                } else {
                    Span::styled(
                        "choose a physical disk (may prompt for admin)",
                        self.palette.dim(),
                    )
                };
                lines.push(Line::from(vec![
                    Span::styled("  d          ", self.palette.accent()),
                    dev_line,
                ]));
                let source = b.source_text();
                if !source.is_empty() {
                    lines.push(Line::raw(""));
                    lines.push(Line::from(vec![
                        Span::styled("Current source: ", self.palette.dim()),
                        Span::raw(source),
                    ]));
                }
                lines.push(Line::raw(""));
                if let Some(note) = self.device_note() {
                    lines.push(note);
                }
            }
            BackupStep::Config => {
                lines.push(Line::from(vec![
                    Span::styled("Source: ", self.palette.dim()),
                    Span::raw(b.source_text()),
                    Span::styled(
                        if b.ctl.device_slot().is_some() {
                            "  [device]"
                        } else {
                            ""
                        },
                        self.palette.dim(),
                    ),
                ]));
                if let Some(snap) = b.ctl.decoding().or_else(|| b.ctl.scanning()) {
                    let what = if snap.phase.is_empty() {
                        snap.label
                    } else {
                        snap.phase
                    };
                    lines.push(Line::styled(format!("  {what}"), self.palette.dim()));
                } else if let Some(table) = b.ctl.table_desc() {
                    lines.push(Line::styled(
                        format!("  {} partition(s) ({table})", b.ctl.partition_count()),
                        self.palette.dim(),
                    ));
                } else if let Some(err) = b.ctl.load_error() {
                    lines.push(Line::styled(format!("  {err}"), self.palette.warn()));
                }
                lines.push(Line::raw(""));
                let row = |idx: usize, label: &str, val: String, editable: bool| -> Line<'static> {
                    let active = b.field == idx;
                    let cursor = if active && editable {
                        Span::styled(" ", self.palette.accent().add_modifier(Modifier::REVERSED))
                    } else {
                        Span::raw("")
                    };
                    let marker = if active { "> " } else { "  " };
                    let mstyle = if active {
                        self.palette.accent().add_modifier(Modifier::BOLD)
                    } else {
                        self.palette.accent()
                    };
                    Line::from(vec![
                        Span::styled(format!("{marker}{label:<10}"), mstyle),
                        Span::raw(val),
                        cursor,
                    ])
                };
                let out_disp = if b.out_dir.is_empty() && b.field != 0 {
                    "(output folder)".to_string()
                } else {
                    b.out_dir.clone()
                };
                lines.push(row(0, "Output:", out_disp, true));
                lines.push(row(1, "Name:", b.ctl.name().to_string(), true));
                lines.push(row(
                    2,
                    "Format:",
                    format!("< {} >", BACKUP_FORMATS[b.format_sel].0),
                    false,
                ));
                lines.push(row(
                    3,
                    "Checksum:",
                    format!("< {} >", BACKUP_CHECKSUMS[b.checksum_sel].0),
                    false,
                ));
                lines.push(Line::raw(""));
                let start_sel = b.field == 4;
                let start_style = if start_sel {
                    self.palette
                        .accent()
                        .add_modifier(Modifier::REVERSED | Modifier::BOLD)
                } else {
                    self.palette.accent()
                };
                lines.push(Line::styled(
                    format!("{}[ Start backup ]", if start_sel { "> " } else { "  " }),
                    start_style,
                ));
                lines.push(Line::raw(""));
                lines.push(Line::styled(
                    "Up/Down field   Left/Right change Format/Checksum   Tab browse (Output)   \
                     Enter next / Start   Esc back",
                    self.palette.dim(),
                ));
            }
            BackupStep::Run => {
                lines.push(Line::from(vec![
                    Span::styled("Source: ", self.palette.dim()),
                    Span::raw(b.source_text()),
                ]));
                lines.push(Line::from(vec![
                    Span::styled("Output: ", self.palette.dim()),
                    Span::raw(format!(
                        "{}/{}  ({}, {})",
                        b.out_dir,
                        b.ctl.name(),
                        BACKUP_FORMATS[b.format_sel].0,
                        BACKUP_CHECKSUMS[b.checksum_sel].0
                    )),
                ]));
                lines.push(Line::raw(""));
                if !b.op.is_empty() {
                    lines.push(Line::from(vec![
                        Span::styled("Step: ", self.palette.dim()),
                        Span::raw(b.op.clone()),
                    ]));
                }
                for msg in &b.log {
                    lines.push(Line::styled(format!("  {msg}"), self.palette.dim()));
                }
                if let Some((r, is_error)) = b.result() {
                    lines.push(Line::raw(""));
                    let style = if is_error {
                        self.palette.warn()
                    } else {
                        self.palette.accent()
                    };
                    lines.push(Line::styled(format!("  {r}"), style));
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

        // Source picker overlay.
        if let Some(picker) = &b.picker {
            picker.draw(frame, area, self.palette, self.border);
        }
        // Physical-disk chooser overlay.
        if b.device_pick {
            self.draw_backup_device_pick(frame, area, b);
        }
    }

    /// The Restore screen: source -> config -> run. The progress bar overlay
    /// sits on top during the Run step.
    fn draw_restore(&self, frame: &mut Frame, area: Rect, r: &RestoreState) {
        let title = match r.step {
            RestoreStep::Source => "Restore  -  step 1/3: backup",
            RestoreStep::Config => "Restore  -  step 2/3: options",
            RestoreStep::Run => "Restore  -  step 3/3: run",
        };
        let mut lines: Vec<Line> = Vec::new();
        match r.step {
            RestoreStep::Source => {
                lines.push(Line::styled(
                    "Choose what to restore:",
                    self.palette.accent(),
                ));
                lines.push(Line::raw(""));
                let (folder_mark, image_mark) = if r.source_is_image {
                    ("  ", "> ")
                } else {
                    ("> ", "  ")
                };
                lines.push(Line::styled(
                    format!("{folder_mark}Backup folder or .cbk"),
                    if r.source_is_image {
                        self.palette.dim()
                    } else {
                        self.palette.accent().add_modifier(Modifier::BOLD)
                    },
                ));
                lines.push(Line::styled(
                    format!("{image_mark}Disk image file (raw, DMG, CHD, VHD, ...)"),
                    if r.source_is_image {
                        self.palette.accent().add_modifier(Modifier::BOLD)
                    } else {
                        self.palette.dim()
                    },
                ));
                lines.push(Line::raw(""));
                lines.push(Line::from(vec![
                    Span::styled("  Space      ", self.palette.accent()),
                    Span::raw("switch between the two"),
                ]));
                lines.push(Line::from(vec![
                    Span::styled("  Enter / o  ", self.palette.accent()),
                    Span::raw(if r.source_is_image {
                        "choose the image file to write"
                    } else {
                        "choose a backup folder or .cbk file"
                    }),
                ]));
                if r.loading {
                    lines.push(Line::raw(""));
                    lines.push(Line::styled("  Loading the backup...", self.palette.dim()));
                }
                if let Some(res) = &r.result {
                    lines.push(Line::raw(""));
                    lines.push(Line::styled(format!("  {res}"), self.palette.warn()));
                }
            }
            RestoreStep::Config => {
                lines.push(Line::from(vec![
                    Span::styled(
                        if r.source_is_image {
                            "Image:  "
                        } else {
                            "Backup: "
                        },
                        self.palette.dim(),
                    ),
                    Span::raw(r.backup_folder.clone()),
                ]));
                lines.push(Line::from(vec![
                    Span::styled("Contents: ", self.palette.dim()),
                    Span::raw(if r.source_is_image {
                        format!("{} decoded", format_size(r.image_size))
                    } else {
                        format!(
                            "{} partition(s), original size {}",
                            r.part_count,
                            format_size(r.source_size)
                        )
                    }),
                ]));
                lines.push(Line::raw(""));
                let row = |idx: usize, label: &str, val: String, editable: bool| -> Line<'static> {
                    let active = r.field == idx;
                    let cursor = if active && editable {
                        Span::styled(" ", self.palette.accent().add_modifier(Modifier::REVERSED))
                    } else {
                        Span::raw("")
                    };
                    let marker = if active { "> " } else { "  " };
                    let mstyle = if active {
                        self.palette.accent().add_modifier(Modifier::BOLD)
                    } else {
                        self.palette.accent()
                    };
                    Line::from(vec![
                        Span::styled(format!("{marker}{label:<11}"), mstyle),
                        Span::raw(val),
                        cursor,
                    ])
                };
                let tgt_disp = if r.target.is_empty() && r.field != 0 {
                    "(target image path, or `d` for a device)".to_string()
                } else if r.target_is_device {
                    format!("{}  [device]", r.target)
                } else {
                    r.target.clone()
                };
                lines.push(row(0, "Target:", tgt_disp, !r.target_is_device));
                if r.source_is_image {
                    // Size mode and alignment don't apply to a fixed image;
                    // the choice that matters is where on the target it lands.
                    let parts = r.writer.target_partitions();
                    let region = match r.writer.target_partition() {
                        None => "< Entire disk >".to_string(),
                        // Position, matching every other surface; selection keys on `p.index`.
                        Some(idx) => match parts.iter().find(|p| p.index == idx) {
                            Some(p) => format!(
                                "< Partition {} - {} ({}) >",
                                p.position,
                                p.type_name,
                                format_size(p.size_bytes),
                            ),
                            None => "< Entire disk >".to_string(),
                        },
                    };
                    lines.push(row(1, "Write to:", region, false));
                    if r.target_is_device && parts.is_empty() {
                        lines.push(Line::styled(
                            "             (no partition table on the target)",
                            self.palette.dim(),
                        ));
                    }
                } else {
                    lines.push(row(
                        1,
                        "Size:",
                        format!("< {} >", RESTORE_SIZE_LABELS[r.size_sel]),
                        false,
                    ));
                    lines.push(row(
                        2,
                        "Alignment:",
                        format!("< {} >", RESTORE_ALIGN_LABELS[r.align_sel]),
                        false,
                    ));
                }
                lines.push(Line::raw(""));
                let start_sel = r.field == 3;
                let start_style = if start_sel {
                    self.palette
                        .accent()
                        .add_modifier(Modifier::REVERSED | Modifier::BOLD)
                } else {
                    self.palette.accent()
                };
                lines.push(Line::styled(
                    format!(
                        "{}[ {} ]",
                        if start_sel { "> " } else { "  " },
                        if r.source_is_image {
                            "Write image"
                        } else {
                            "Start restore"
                        },
                    ),
                    start_style,
                ));
                lines.push(Line::raw(""));
                lines.push(Line::styled(
                    "Up/Down field   Left/Right Size/Align   Tab choose target (file/device)   \
                     Enter Start   Esc back",
                    self.palette.dim(),
                ));
                if r.target_is_device {
                    if let Some(note) = self.device_note() {
                        lines.push(note);
                    }
                }
            }
            RestoreStep::Run => {
                lines.push(Line::from(vec![
                    Span::styled("Backup: ", self.palette.dim()),
                    Span::raw(r.backup_folder.clone()),
                ]));
                lines.push(Line::from(vec![
                    Span::styled("Target: ", self.palette.dim()),
                    Span::raw(format!(
                        "{}  ({}, {} align)",
                        r.target,
                        RESTORE_SIZE_LABELS[r.size_sel],
                        RESTORE_ALIGN_LABELS[r.align_sel]
                    )),
                ]));
                lines.push(Line::raw(""));
                if !r.op.is_empty() {
                    lines.push(Line::from(vec![
                        Span::styled("Step: ", self.palette.dim()),
                        Span::raw(r.op.clone()),
                    ]));
                }
                if let Some(res) = &r.result {
                    lines.push(Line::raw(""));
                    let style = if r.is_error {
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
        if let Some(picker) = &r.picker {
            picker.draw(frame, area, self.palette, self.border);
        }
        // "Target type" chooser overlay (Image file / Physical device).
        if let Some(sel) = r.target_type_menu {
            let opts = [
                "Image file (write to a .img / container)",
                "Physical device (raw disk)",
            ];
            let mut ml: Vec<Line> = vec![Line::raw("")];
            for (i, label) in opts.iter().enumerate() {
                let on = i == sel;
                let marker = if on { "> " } else { "  " };
                let style = if on {
                    self.palette
                        .accent()
                        .add_modifier(Modifier::REVERSED | Modifier::BOLD)
                } else {
                    self.palette.accent()
                };
                ml.push(Line::styled(format!("{marker}{label}"), style));
            }
            ml.push(Line::raw(""));
            ml.push(Line::styled(
                "  Enter choose   Esc cancel",
                self.palette.dim(),
            ));
            let h = (ml.len() as u16 + 2).min(area.height.saturating_sub(2));
            let popup = centered_rect(60, h, area);
            frame.render_widget(Clear, popup);
            frame.render_widget(
                Paragraph::new(Text::from(ml)).block(self.pane_block("Restore target", true)),
                popup,
            );
        }
        // Physical-device target chooser overlay.
        if r.device_pick {
            self.draw_device_pick(frame, area, r.device_sel, "Pick a target device (RESTORE)");
        }
        // Destructive device-write confirmation overlay.
        if let Some((path, label)) = &r.confirm_device {
            let mut cl = vec![
                Line::raw(""),
                Line::styled(
                    "  Restore will OVERWRITE the entire device:",
                    self.palette.warn(),
                ),
                Line::styled(format!("    {label}"), self.palette.accent()),
                Line::styled(format!("    {path}"), self.palette.dim()),
                Line::raw(""),
                Line::styled(
                    "  All existing data on it will be lost.  Proceed?  (y / n)",
                    self.palette.warn(),
                ),
            ];
            if !self.elevated {
                cl.push(Line::raw(""));
                cl.push(Line::styled(
                    "  Note: writing a device needs elevation; you may be prompted, \
                     or it may fail if unavailable.",
                    self.palette.dim(),
                ));
            }
            let h = (cl.len() as u16 + 2).min(area.height.saturating_sub(2));
            let popup = centered_rect(74, h, area);
            frame.render_widget(Clear, popup);
            frame.render_widget(
                Paragraph::new(Text::from(cl)).block(self.pane_block("Confirm device write", true)),
                popup,
            );
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
    /// The Archives screen: open a Mac archive, list its entries, extract.
    fn draw_archive(&self, frame: &mut Frame, area: Rect, a: &ArchiveState) {
        let mut lines: Vec<Line> = Vec::new();
        if !a.ctl.is_loaded() {
            lines.push(Line::styled(
                "Open a classic Mac archive (.sit / .sea / .cpt / .mar / .hqx):",
                self.palette.accent(),
            ));
            lines.push(Line::raw(""));
            lines.push(Line::from(vec![
                Span::styled("  Enter / o  ", self.palette.accent()),
                Span::raw("choose an archive file"),
            ]));
            lines.push(Line::from(vec![
                Span::styled("  c         ", self.palette.accent()),
                Span::raw("create an archive from host files (via the command line)"),
            ]));
            if let Some(s) = &a.status {
                lines.push(Line::raw(""));
                let style = if a.is_error {
                    self.palette.warn()
                } else {
                    self.palette.dim()
                };
                lines.push(Line::styled(format!("  {s}"), style));
            }
            frame.render_widget(
                Paragraph::new(Text::from(lines))
                    .block(self.pane_block("Archives", true))
                    .wrap(Wrap { trim: false }),
                area,
            );
            if let Some(picker) = &a.picker {
                picker.draw(frame, area, self.palette, self.border);
            }
            return;
        }

        // Loaded: header + scrollable entry list.
        let title = format!(
            "Archives  {}  [{} entries]  fork: {}",
            basename(&a.archive_path),
            a.entries.len(),
            ARCHIVE_FORK_FORMATS[a.fork_fmt_sel].0
        );
        let visible = area.height.saturating_sub(4) as usize;
        let start = a.list_sel.saturating_sub(visible.saturating_sub(1));
        for (i, line) in a.entries.iter().enumerate().skip(start).take(visible) {
            let sel = i == a.list_sel;
            let style = if sel {
                self.palette
                    .accent()
                    .add_modifier(Modifier::REVERSED | Modifier::BOLD)
            } else {
                Style::default()
            };
            lines.push(Line::styled(format!(" {line}"), style));
        }
        if let Some(s) = &a.status {
            lines.push(Line::raw(""));
            let style = if a.is_error {
                self.palette.warn()
            } else {
                self.palette.accent()
            };
            lines.push(Line::styled(format!(" {s}"), style));
        }
        frame.render_widget(
            Paragraph::new(Text::from(lines)).block(self.pane_block(&title, true)),
            area,
        );
        if let Some(picker) = &a.picker {
            picker.draw(frame, area, self.palette, self.border);
        }
    }

    /// The Optical rip screen: drive list -> config -> run.
    #[cfg(feature = "optical")]
    fn draw_optical(&self, frame: &mut Frame, area: Rect, o: &OpticalState) {
        let title = match o.step {
            OpticalStep::Drives => "Optical  -  step 1/3: drive",
            OpticalStep::Config => "Optical  -  step 2/3: rip options",
            OpticalStep::Run => "Optical  -  step 3/3: run",
        };
        let mut lines: Vec<Line> = Vec::new();
        match o.step {
            OpticalStep::Drives => {
                lines.push(Line::styled(
                    "Rip an optical disc. Pick a drive:",
                    self.palette.accent(),
                ));
                lines.push(Line::raw(""));
                let drives = o.ctl.drive_names();
                if o.ctl.scanning_drives() {
                    lines.push(Line::styled(
                        "  Looking for optical drives...",
                        self.palette.dim(),
                    ));
                } else if drives.is_empty() {
                    lines.push(Line::styled(
                        "  No optical drives detected. Press `r` to rescan.",
                        self.palette.dim(),
                    ));
                } else {
                    for (i, (name, path)) in drives.iter().enumerate() {
                        let sel = i == o.drive_sel;
                        let marker = if sel { "> " } else { "  " };
                        let style = if sel {
                            self.palette
                                .accent()
                                .add_modifier(Modifier::REVERSED | Modifier::BOLD)
                        } else {
                            self.palette.accent()
                        };
                        lines.push(Line::styled(format!("{marker}{name}  ({path})"), style));
                    }
                }
                lines.push(Line::raw(""));
                lines.push(Line::styled(
                    "Up/Down select   Enter next   r rescan   i image ops (browse/extract/new)",
                    self.palette.dim(),
                ));
                if let Some(note) = self.device_note() {
                    lines.push(Line::raw(""));
                    lines.push(note);
                }
            }
            OpticalStep::Config => {
                lines.push(Line::from(vec![
                    Span::styled("Drive: ", self.palette.dim()),
                    Span::raw(format!("{} ({})", o.device_name, o.device_path)),
                ]));
                if let Some(d) = o.ctl.disc() {
                    lines.push(Line::from(vec![
                        Span::styled("Disc:  ", self.palette.dim()),
                        Span::raw(format!(
                            "{} / {} {}",
                            d.format,
                            d.filesystem,
                            d.volume.unwrap_or_default()
                        )),
                    ]));
                } else if o.ctl.detecting().is_some() {
                    lines.push(Line::styled("Disc:  reading...", self.palette.dim()));
                }
                lines.push(Line::raw(""));
                let row = |idx: usize, label: &str, val: String, editable: bool| -> Line<'static> {
                    let active = o.field == idx;
                    let cursor = if active && editable {
                        Span::styled(" ", self.palette.accent().add_modifier(Modifier::REVERSED))
                    } else {
                        Span::raw("")
                    };
                    let marker = if active { "> " } else { "  " };
                    let mstyle = if active {
                        self.palette.accent().add_modifier(Modifier::BOLD)
                    } else {
                        self.palette.accent()
                    };
                    Line::from(vec![
                        Span::styled(format!("{marker}{label:<9}"), mstyle),
                        Span::raw(val),
                        cursor,
                    ])
                };
                lines.push(row(0, "Output:", o.output.clone(), true));
                lines.push(row(
                    1,
                    "Format:",
                    format!("< {} >", RIP_FORMATS[o.format_sel].0),
                    false,
                ));
                lines.push(row(
                    2,
                    "Eject:",
                    format!("[{}]", if o.eject { "x" } else { " " }),
                    false,
                ));
                lines.push(Line::raw(""));
                let start_sel = o.field == 3;
                lines.push(Line::styled(
                    format!("{}[ Start rip ]", if start_sel { "> " } else { "  " }),
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
                    "Up/Down field   Left/Right format   Space eject   Tab browse (Output)   \
                     Enter Start   Esc back",
                    self.palette.dim(),
                ));
            }
            OpticalStep::Run => {
                lines.push(Line::from(vec![
                    Span::styled("Drive: ", self.palette.dim()),
                    Span::raw(o.device_name.clone()),
                    Span::styled("   Output: ", self.palette.dim()),
                    Span::raw(o.output.clone()),
                ]));
                lines.push(Line::raw(""));
                if !o.op.is_empty() {
                    lines.push(Line::from(vec![
                        Span::styled("Step: ", self.palette.dim()),
                        Span::raw(o.op.clone()),
                    ]));
                }
                if let Some(res) = &o.result {
                    lines.push(Line::raw(""));
                    let style = if o.is_error {
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
        if let Some(picker) = &o.picker {
            picker.draw(frame, area, self.palette, self.border);
        }
        // Optical-image-operations launcher menu overlay.
        if let Some(sel) = o.image_menu {
            let mut mlines: Vec<Line> = vec![Line::raw("")];
            for (i, (label, _)) in OPTICAL_IMAGE_OPS.iter().enumerate() {
                let on = i == sel;
                let marker = if on { "> " } else { "  " };
                let style = if on {
                    self.palette
                        .accent()
                        .add_modifier(Modifier::REVERSED | Modifier::BOLD)
                } else {
                    self.palette.accent()
                };
                mlines.push(Line::styled(format!("{marker}{label}"), style));
            }
            mlines.push(Line::raw(""));
            mlines.push(Line::styled(
                "  Enter -> prefills the command line; edit and run.  Esc cancel",
                self.palette.dim(),
            ));
            let h = (mlines.len() as u16 + 2).min(area.height.saturating_sub(2));
            let popup = centered_rect(60, h, area);
            frame.render_widget(Clear, popup);
            frame.render_widget(
                Paragraph::new(Text::from(mlines))
                    .block(self.pane_block("Optical image operations", true)),
                popup,
            );
        }
    }

    /// The physical-disk chooser overlay used by the Backup source step.
    fn draw_backup_device_pick(&self, frame: &mut Frame, area: Rect, b: &BackupState) {
        self.draw_device_pick(frame, area, b.device_sel, "Pick a physical disk");
    }

    /// Shared physical-disk chooser overlay (Backup source + Restore target).
    fn draw_device_pick(&self, frame: &mut Frame, area: Rect, device_sel: usize, title: &str) {
        let disks = self.disks.as_deref().unwrap_or(&[]);
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

    /// The interactive Settings tab: environment info (read-only) plus editable
    /// preference toggles persisted to `config.json` via `UpdateConfig::save`.
    fn draw_settings_tab(&self, frame: &mut Frame, area: Rect, s: &SettingsState) {
        let updater = if cfg!(feature = "tui-update") {
            "built in (rb-cli update available)"
        } else {
            "not built (rebuild with --features tui-update)"
        };
        let info = |k: &str, v: String| -> Line<'static> {
            Line::from(vec![
                Span::styled(format!("{k:<14}"), self.palette.accent()),
                Span::raw(v),
            ])
        };
        let mut lines = vec![
            info(
                "Platform:",
                format!("{} / {}", std::env::consts::OS, std::env::consts::ARCH),
            ),
            {
                let (txt, style) = if self.elevated {
                    (
                        format!("{} - device operations enabled", self.privilege_label()),
                        self.palette.accent(),
                    )
                } else {
                    (
                        "normal user - device operations disabled".to_string(),
                        self.palette.warn(),
                    )
                };
                Line::from(vec![
                    Span::styled(format!("{:<14}", "Privilege:"), self.palette.accent()),
                    Span::styled(txt, style),
                ])
            },
            info(
                "Color:",
                if self.palette.color {
                    "on".to_string()
                } else {
                    "off (NO_COLOR)".to_string()
                },
            ),
            info(
                "Borders:",
                if self.border == ASCII_BORDER {
                    "ASCII fallback".to_string()
                } else {
                    "Unicode rounded".to_string()
                },
            ),
            info("Updater:", updater.to_string()),
            info(
                "Config file:",
                crate::update::UpdateConfig::user_config_path()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| "(no config dir)".to_string()),
            ),
            info("Update repo:", s.config.update_check.repository_url.clone()),
            Line::raw(""),
            Line::styled(
                "Preferences  (Enter / Space toggles):",
                self.palette.accent(),
            ),
            Line::raw(""),
        ];

        let toggles = [
            (
                "Check for updates on startup",
                s.config.update_check.enabled,
            ),
            (
                "Register file associations",
                s.config.file_associations_enabled,
            ),
        ];
        for (i, (label, on)) in toggles.iter().enumerate() {
            let selected = i == s.sel;
            let marker = if selected { "> " } else { "  " };
            let boxed = if *on { "[x]" } else { "[ ]" };
            let style = if selected {
                self.palette
                    .accent()
                    .add_modifier(Modifier::REVERSED | Modifier::BOLD)
            } else {
                self.palette.accent()
            };
            lines.push(Line::styled(format!("{marker}{boxed} {label}"), style));
        }

        lines.push(Line::raw(""));
        if let Some(st) = &s.status {
            lines.push(Line::styled(format!("  {st}"), self.palette.dim()));
            lines.push(Line::raw(""));
        }
        lines.push(Line::styled(
            "Up/Down select   Enter/Space toggle   c check for updates   Left/Right change tab",
            self.palette.dim(),
        ));

        frame.render_widget(
            Paragraph::new(Text::from(lines))
                .block(self.pane_block("Settings", true))
                .wrap(Wrap { trim: false }),
            area,
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
            let step = self.backup.as_ref().map(|b| b.step);
            match step {
                Some(BackupStep::Source) => vec![
                    ("<-/->", "Tabs"),
                    ("Enter/o", "Image"),
                    ("d", "Disk"),
                    ("?", "Help"),
                ],
                Some(BackupStep::Config) => vec![
                    ("Up/Dn", "Field"),
                    ("<-/->", "Format"),
                    ("Tab", "Browse"),
                    ("Enter", "Next/Start"),
                    ("Esc", "Back"),
                ],
                _ => vec![("Esc", "Back")],
            }
        } else if self.current() == TabId::Restore {
            let step = self.restore.as_ref().map(|r| r.step);
            match step {
                Some(RestoreStep::Source) => {
                    vec![("<-/->", "Tabs"), ("Enter/o", "Backup"), ("?", "Help")]
                }
                Some(RestoreStep::Config) => vec![
                    ("Up/Dn", "Field"),
                    ("<-/->", "Size/Align"),
                    ("Tab", "Browse"),
                    ("Enter", "Next/Start"),
                    ("Esc", "Back"),
                ],
                _ => vec![("Esc", "Back")],
            }
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
                let step = self.optical.as_ref().map(|o| o.step);
                match step {
                    Some(OpticalStep::Drives) => vec![
                        ("<-/->", "Tabs"),
                        ("Up/Dn", "Select"),
                        ("Enter", "Next"),
                        ("r", "Rescan"),
                    ],
                    Some(OpticalStep::Config) => vec![
                        ("Up/Dn", "Field"),
                        ("<-/->", "Format"),
                        ("Tab", "Browse"),
                        ("Enter", "Start"),
                        ("Esc", "Back"),
                    ],
                    _ => vec![("Esc", "Back")],
                }
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
        } else if matches!(self.opened, Some(Opened::Image { .. })) {
            vec![
                ("<-/->", "Tabs"),
                ("Up/Dn", "Select"),
                ("Enter", "Browse"),
                ("Esc", "Close"),
                ("?", "Help"),
            ]
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
            if self.opened.is_some() {
                k.push(("Esc", "Back"));
            }
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
            return match self.opened {
                Some(Opened::Image { .. }) => ("Inspect - partitions", HINTS_INSPECT_IMAGE),
                _ => ("Inspect - disks", HINTS_INSPECT_DISKS),
            };
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
        // An opened image renders as the partition-list widget, not here.
        if let Some(Opened::Backup {
            path,
            kind,
            partitions,
            info,
        }) = &self.opened
        {
            let mut lines = vec![
                Line::styled(
                    format!("Backup  {}", basename(path)),
                    self.palette.accent().add_modifier(Modifier::BOLD),
                ),
                Line::raw(""),
                self.kv("Path:", path.clone()),
                self.kv("Kind:", kind.clone()),
                self.kv("Partitions:", partitions.to_string()),
            ];
            if !info.is_empty() {
                lines.push(Line::raw(""));
                for msg in info {
                    lines.push(Line::styled(format!("  {msg}"), self.palette.dim()));
                }
            }
            lines.push(Line::raw(""));
            lines.push(Line::styled(
                "Read by the shared Inspect controller (same as the GUI).".to_string(),
                self.palette.dim(),
            ));
            lines.push(Line::raw("Esc to close."));
            return Text::from(lines);
        }
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

    /// An accent-labelled `key   value` line.
    fn kv(&self, key: &str, value: String) -> Line<'static> {
        Line::from(vec![
            Span::styled(format!("{key:<12}"), self.palette.accent()),
            Span::raw(value),
        ])
    }

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

/// A headless Inspect controller that opened `source`, its open and probes finished.
fn inspect_headless(
    source: crate::controller::inspect::InspectSource,
) -> crate::controller::inspect::InspectController {
    use crate::controller::inspect::{InspectAction, InspectController, InspectServices};
    use crate::controller::Controller as _;
    let mut ctl = InspectController::new(InspectServices {
        jobs: crate::controller::inspect::JobRegistry::new(),
        notify: Arc::new(crate::controller::inspect::NoNotify),
    });
    ctl.apply(InspectAction::Load(source));
    ctl.wait_opened();
    ctl
}

/// The Inspect tab's rows for what the controller found; `None` when it found no table.
fn rows_from(ctl: &mut crate::controller::inspect::InspectController) -> Option<Vec<PartRow>> {
    let table = ctl.table_summary()?;
    let parts = ctl.partitions();
    if parts.is_empty() {
        let size = ctl.disk_size().unwrap_or(0);
        return Some(vec![PartRow {
            selector: None,
            label: format!("Whole volume ({})", table.type_name),
            fs_hint: table.type_name,
            size,
        }]);
    }
    Some(
        parts
            .iter()
            .enumerate()
            .map(|(k, p)| {
                let n = (k + 1) as u32;
                PartRow {
                    selector: Some(n.into()),
                    label: format!("#{n}  {}", p.type_name),
                    fs_hint: p
                        .partition_type_string
                        .clone()
                        .unwrap_or_else(|| p.type_name.clone()),
                    size: p.size_bytes,
                }
            })
            .collect(),
    )
}

/// An image file's rows as the GUI's Inspect tab reads them (containers, real filesystem names).
fn inspected_rows(path: std::path::PathBuf) -> Option<Vec<PartRow>> {
    let mut ctl =
        inspect_headless(crate::controller::inspect::InspectSource::Image { path, temp: None });
    rows_from(&mut ctl)
}

/// A backup folder through the Inspect controller; a single-file-CHD backup opens as its disk image.
fn open_backup_headless(folder: std::path::PathBuf) -> std::result::Result<Opened, String> {
    use crate::controller::log::LogLevel;
    let mut ctl = inspect_headless(crate::controller::inspect::InspectSource::Backup { folder });
    if let Some(err) = ctl.last_error() {
        return Err(err.to_string());
    }
    let info: Vec<String> = ctl
        .take_log()
        .into_iter()
        .filter(|(level, _)| *level == LogLevel::Info)
        .map(|(_, msg)| msg)
        .collect();
    if let Some(image) = ctl.image_path().map(|p| p.to_path_buf()) {
        let size = ctl.disk_size().unwrap_or(0);
        let parts = rows_from(&mut ctl).ok_or_else(|| "no partition table".to_string())?;
        return Ok(Opened::Image {
            path: image.display().to_string(),
            size,
            parts,
            disk: None,
        });
    }
    let kind = if ctl.clonezilla_summary().is_some() {
        "Clonezilla"
    } else {
        "rusty-backup"
    };
    Ok(Opened::Backup {
        path: ctl
            .backup_folder()
            .map(|p| p.display().to_string())
            .unwrap_or_default(),
        kind: kind.to_string(),
        partitions: ctl.partitions().len(),
        info,
    })
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

/// Parse an image's partition table into displayable rows, mirroring the shared
/// `resolve` semantics: `@N` selects `partitions()[N-1]`, and an image with no
/// table is a single superfloppy volume (selector `None`). `total` is the file
/// size, used for the superfloppy row.
/// Read the partition table for the Inspect tab's image view.
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
            selector: None,
            label: "Whole volume (no partition table)".to_string(),
            fs_hint: String::new(),
            size: total,
        }]);
    };
    if table.partitions.is_empty() {
        return Ok(vec![PartRow {
            selector: None,
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
                selector: Some(n.into()),
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
        for table in [HINTS_TAB, HINTS_INSPECT_DISKS, HINTS_INSPECT_IMAGE] {
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
        app.disks = Some(vec![fake_disk(&img, size)]);
        app.selection = 0;
        assert!(app.opened.is_none());

        app.activate(); // one press
        app.finish_task();

        let Some(Opened::Image { parts, disk, .. }) = &app.opened else {
            panic!("a single Enter should have opened the disk's partition table");
        };
        assert!(
            parts.iter().any(|p| p.selector == Some(1u32.into())),
            "the ext partition should be selectable, got {:?}",
            parts.iter().map(|p| &p.label).collect::<Vec<_>>()
        );
        assert!(
            disk.is_some(),
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
        app.disks = Some(vec![fake_disk(&img, size)]);
        assert!(app.handle_backup_key(KeyCode::Char('d')));
        assert!(app.handle_backup_key(KeyCode::Enter));
        let b = app.backup.as_mut().unwrap();
        assert!(b.step == BackupStep::Config);
        assert!(!b.ctl.name().is_empty());
        b.out_dir = out.path().display().to_string();
        b.field = 4;
        assert!(app.handle_backup_key(KeyCode::Enter));
        assert!(app.progress.is_some());
        for _ in 0..6000 {
            app.tick();
            if app.backup.as_ref().unwrap().result().is_some() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let b = app.backup.as_ref().unwrap();
        assert_eq!(b.result(), Some(("Backup complete.".to_string(), false)));
        let folder = out.path().join(b.ctl.name());
        assert!(folder.join("metadata.json").exists());
    }

    #[cfg(feature = "optical")]
    #[test]
    fn the_optical_screen_starts_the_rip_through_the_controller() {
        let mut app = App::new_on(BACKUP_TAB);
        app.optical = Some(OpticalState {
            step: OpticalStep::Config,
            field: 3,
            ..OpticalState::default()
        });
        app.start_optical_rip();
        let o = app.optical.as_ref().unwrap();
        assert_eq!(o.result.as_deref(), Some("Enter an output path."));
        let dir = tempfile::tempdir().expect("tempdir");
        let o = app.optical.as_mut().unwrap();
        o.output = dir.path().join("disc.iso").display().to_string();
        o.format_sel = 1;
        app.start_optical_rip();
        app.tick();
        let o = app.optical.as_ref().unwrap();
        assert_eq!(
            o.result.as_deref(),
            Some("Cannot start the rip: No drive selected")
        );
        assert!(o.is_error && !o.ctl.is_running() && app.progress.is_none());
        assert_eq!(
            o.ctl.output_format(),
            crate::controller::optical::OutputFormat::BinCue
        );
    }

    #[test]
    fn the_archives_screen_opens_and_extracts_through_the_controller() {
        let dir = tempfile::tempdir().expect("tempdir");
        let sit = crate::controller::archives::sample_archive(dir.path(), &[("readme", b"hello")]);
        let mut app = App::new_on(BACKUP_TAB);
        app.archive = Some(ArchiveState::default());
        app.load_archive(sit);
        app.archive.as_mut().unwrap().ctl.wait_idle();
        app.tick();
        let a = app.archive.as_ref().unwrap();
        assert_eq!(a.status.as_deref(), Some("Opened (1 entries)."));
        assert!(a.entries[0].starts_with("FILE  readme"), "{:?}", a.entries);
        let out = dir.path().join("out");
        app.archive.as_mut().unwrap().fork_fmt_sel = 3;
        app.archive_extract(out.clone());
        app.archive.as_mut().unwrap().ctl.wait_idle();
        app.tick();
        let a = app.archive.as_ref().unwrap();
        assert_eq!(
            a.status.as_deref(),
            Some(format!("Extracted 1 file(s) to {}", out.display()).as_str())
        );
        assert_eq!(std::fs::read(out.join("readme")).unwrap(), b"hello");
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

    #[test]
    fn the_restore_screen_restores_a_backup_through_the_controller() {
        use crate::controller::backup::{BackupAction, BackupController, BackupServices};
        use crate::controller::Controller as _;
        let dir = tempfile::tempdir().expect("tempdir");
        let img = dir.path().join("vol.img");
        let bytes = crate::controller::test_fixtures::blank_fat(2 * 1024 * 1024, Some("T"));
        std::fs::write(&img, &bytes).unwrap();
        let mut b = BackupController::new(BackupServices {
            jobs: crate::controller::backup::JobRegistry::new(),
            notify: Arc::new(crate::controller::backup::NoNotify),
        });
        b.apply(BackupAction::OpenImage {
            path: img.clone(),
            rename: false,
        });
        b.wait_idle();
        b.apply(BackupAction::SetDestination(Some(dir.path().to_path_buf())));
        b.apply(BackupAction::SetName("bk".into()));
        b.apply(BackupAction::Start(None));
        b.wait_finished();
        let folder = dir.path().join("bk");

        let mut app = App::new_on(BACKUP_TAB + 1);
        app.restore = Some(RestoreState::default());
        app.load_restore_source(folder.clone());
        for _ in 0..3000 {
            app.tick();
            if app.restore.as_ref().unwrap().step == RestoreStep::Config {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let r = app.restore.as_mut().unwrap();
        assert!(r.step == RestoreStep::Config, "{:?}", r.result);
        assert_eq!(r.part_count, 1);
        assert_eq!(
            r.target,
            dir.path().join("bk_restored.img").display().to_string()
        );
        r.field = 3;
        assert!(app.handle_restore_key(KeyCode::Enter));
        assert!(app.progress.is_some());
        for _ in 0..6000 {
            app.tick();
            if app.restore.as_ref().unwrap().result.is_some() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let r = app.restore.as_ref().unwrap();
        assert_eq!(r.result.as_deref(), Some("Restore complete."));
        let out = std::fs::read(dir.path().join("bk_restored.img")).unwrap();
        assert_eq!(out, bytes);
    }

    /// An image source pours through the disk writer: whole into an image file, then into one
    /// partition of a disk-image file standing in for a device (never a real disk).
    #[test]
    fn the_restore_screen_writes_an_image_through_the_disk_writer() {
        let dir = tempfile::tempdir().expect("tempdir");
        let img = dir.path().join("src.img");
        let bytes: Vec<u8> = (0..(1usize << 20)).map(|i| (i % 249) as u8).collect();
        std::fs::write(&img, &bytes).unwrap();
        let run = |app: &mut App| {
            for _ in 0..3000 {
                app.tick();
                if app.restore.as_ref().unwrap().result.is_some() {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        };

        let mut app = App::new_on(BACKUP_TAB + 1);
        app.restore = Some(RestoreState {
            source_is_image: true,
            ..RestoreState::default()
        });
        app.load_restore_source(img.clone());
        let r = app.restore.as_mut().unwrap();
        assert!(r.step == RestoreStep::Config, "{:?}", r.result);
        let out = dir.path().join("out.img");
        r.target = out.display().to_string();
        r.field = 3;
        assert!(app.handle_restore_key(KeyCode::Enter));
        run(&mut app);
        let r = app.restore.as_ref().unwrap();
        assert_eq!(
            r.result.as_deref(),
            Some("Wrote 1.0 MiB to the target."),
            "{:?}",
            r.result
        );
        assert_eq!(std::fs::read(&out).unwrap(), bytes);

        // A "device" with one MBR partition at LBA 2048; the table must survive.
        let disk = dir.path().join("disk.img");
        let mut d = vec![0u8; 4 << 20];
        d[0x1BE + 4] = 0x06;
        d[0x1BE + 8..0x1BE + 12].copy_from_slice(&2048u32.to_le_bytes());
        d[0x1BE + 12..0x1BE + 16].copy_from_slice(&4096u32.to_le_bytes());
        d[510] = 0x55;
        d[511] = 0xAA;
        std::fs::write(&disk, &d).unwrap();
        app.disks = Some(vec![DiskDevice {
            name: "fake".into(),
            path: disk.clone(),
            size_bytes: 4 << 20,
            is_removable: true,
            is_read_only: false,
            is_system: false,
            bus_protocol: "USB".into(),
            media_name: "Fake".into(),
            partitions: Vec::new(),
        }]);
        let r = app.restore.as_mut().unwrap();
        r.result = None;
        r.step = RestoreStep::Config;
        r.target = disk.display().to_string();
        r.target_is_device = true;
        app.select_write_device();
        let r = app.restore.as_mut().unwrap();
        assert_eq!(r.writer.target_partitions().len(), 1);
        r.field = 1;
        assert!(app.handle_restore_key(KeyCode::Right));
        let r = app.restore.as_mut().unwrap();
        assert!(r.writer.target_partition().is_some());
        r.field = 3;
        assert!(app.handle_restore_key(KeyCode::Enter));
        assert!(app.restore.as_ref().unwrap().confirm_device.is_some());
        assert!(app.handle_restore_key(KeyCode::Char('y')));
        run(&mut app);
        let r = app.restore.as_ref().unwrap();
        assert_eq!(
            r.result.as_deref(),
            Some("Wrote 1.0 MiB to the target."),
            "{:?}",
            r.result
        );
        let after = std::fs::read(&disk).unwrap();
        assert_eq!(&after[..512], &d[..512]);
        assert_eq!(&after[1 << 20..2 << 20], &bytes[..]);
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
        app.disks = Some(vec![fake_disk(&img, size)]);
        app.activate();
        app.finish_task();
        assert!(app.opened.is_some());

        app.on_back();
        assert!(app.opened.is_none(), "Esc should close the partition view");
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
        app.finish_task();
        let status = app.status.clone().unwrap_or_default();
        assert!(
            !status.contains("No such file or directory"),
            "an existing path must not be reported as missing: {status}"
        );
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

    /// An image file's rows come from the Inspect controller, which names the volume's filesystem.
    #[test]
    fn an_image_file_is_read_by_the_inspect_controller() {
        let dir = tempfile::tempdir().unwrap();
        let img = crate::controller::commander::blank_fat_image(dir.path());
        let rows = inspected_rows(img).expect("a FAT volume has a table");
        assert_eq!(rows.len(), 1);
        assert!(rows[0].fs_hint.contains("FAT"), "{}", rows[0].fs_hint);
        assert!(open_backup_headless(dir.path().join("missing")).is_err());
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
}
