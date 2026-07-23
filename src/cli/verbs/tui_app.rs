//! `rb-cli tui` — a full-screen terminal UI (milestone M1: the tab shell).
//!
//! This mirrors the desktop GUI: a top tab bar (Backup / Restore / Inspect /
//! New Disk / Optical / Archives / Commander / Bulk / Settings) over the same
//! shared `src/model/` runner layer the GUI uses. Real tabs so far: **Inspect**
//! (a selectable disk list; opening an image shows its **partition table** via
//! `PartitionTable::detect`, each partition opening a per-partition filesystem
//! **Explorer** window that reuses the shared `resolve` + `open_filesystem` +
//! `list_directory` — the `ls`/`get` core — with browse/view/export/import/
//! metadata/bless and an Esc confirmation to close); **New Disk** (a wizard that
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

use crate::cli::verbs::new::{FloppyArgs, FloppyFs, NewCommand, VolumeArgs, VolumeFs};
use crate::device::{enumerate_devices, DiskDevice};
use crate::fs::entry::{EntryType, FileEntry};
use crate::model::rate_tracker::RateTracker;
use crate::partition::format_size;

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
    selector: Option<u32>,
    label: String,
    fs_hint: String,
    size: u64,
}

/// One node in the Explorer's left-hand directory tree (flattened depth-first;
/// expanding a node splices its child directories in right after it).
struct TreeNode {
    dir: crate::fs::entry::FileEntry,
    depth: usize,
    expanded: bool,
}

/// Which pane of the two-pane Explorer has the keyboard.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ExFocus {
    Tree,
    List,
}

/// The per-partition filesystem Explorer overlay: a two-pane view — a directory
/// **tree** on the left, the selected directory's **listing** on the right —
/// over the shared `Filesystem` trait (the same core the `ls`/`get` verbs use).
struct Explorer {
    fs: Box<dyn crate::fs::filesystem::Filesystem>,
    /// The source image + partition selector, kept so import can reopen the
    /// partition read-write and the view can refresh afterwards.
    image_path: String,
    selector: Option<u32>,
    part_label: String,
    volume: Option<String>,
    /// The currently-blessed (bootable) System Folder, if any — HFS/HFS+ only.
    /// Shown in the footer; refreshed after a bless.
    blessed: Option<(u64, String)>,
    /// Left pane: the flattened directory tree, and the selected node.
    tree: Vec<TreeNode>,
    tree_sel: usize,
    /// Right pane: the selected directory's entries, and the selected row.
    list: Vec<FileEntry>,
    list_sel: usize,
    /// Space-marked entries for multi-select export, keyed by path. Markable in
    /// both panes (a directory in the left tree, a file or folder in the right
    /// list) and kept across navigation so a selection can span directories.
    marked: std::collections::BTreeMap<String, FileEntry>,
    focus: ExFocus,
    /// A one-line result of the last export/import, shown in the footer area.
    status: Option<String>,
    /// An active Export (Dir) / Import (File) file picker, if any.
    picker: Option<FilePicker>,
    /// The "Export as" format menu (selected index), shown before the picker.
    export_menu: Option<usize>,
    /// The export format chosen from that menu, used when the picker confirms.
    export_fmt: ExportFormat,
    /// A file preview overlay, if open.
    preview: Option<Preview>,
    /// The metadata editor (HFS type/creator + dates), if open.
    metadata: Option<MetaEdit>,
    /// True while the "close?" confirmation prompt is showing.
    confirm_close: bool,
    /// The `(mac_path, leaf_name)` of a folder pending a bless confirmation, if
    /// the "bless?" prompt is showing.
    confirm_bless: Option<(String, String)>,
    /// A new-folder name being typed, if the mkdir prompt is showing.
    mkdir_input: Option<String>,
    /// The `(name, is_dir)` of an entry pending a delete confirmation.
    confirm_delete: Option<(String, bool)>,
    /// A scrollable fsck check / repair report overlay, when open.
    fsck_report: Option<FsckReportView>,
    /// The whole-image "Transform" launcher menu (selected index), when open.
    /// Its entries prefill the `:` command palette with a verb for this image.
    transform_menu: Option<usize>,
}

/// Whole-image / partition-table transforms offered by the Explorer's `t`
/// launcher. Each is executed through the `:` command palette (the shared CLI
/// `dispatch`), prefilled with the current image path — the palette is the
/// proven execution path, so the Explorer doesn't reimplement these wizards.
/// `(label, verb-template)`; `{IMG}` / `{STEM}` are substituted at launch.
const TRANSFORMS: &[(&str, &str)] = &[
    // `convert`'s second argument is a destination *folder* (created if absent);
    // the converted file lands inside it with the input's stem + new extension.
    (
        "Convert to another format",
        "convert \"{IMG}\" \"{STEM}-converted\" --format vhd",
    ),
    (
        "Resize a partition in place",
        "resize --size <SIZE> \"{IMG}\"",
    ),
    (
        "Expand into a larger new image",
        "expand --size <SIZE> --output \"{STEM}-expanded.img\" \"{IMG}\"",
    ),
    // `partmap` is edit-only (add/resize/move/delete/set-type/set-bootable);
    // `inspect` shows the table to start from, then edit via `:` partmap ...
    ("Partition table (inspect / partmap)", "inspect \"{IMG}\""),
];

/// A scrollable fsck / repair result overlay (title + pre-formatted lines).
struct FsckReportView {
    title: String,
    lines: Vec<String>,
    scroll: usize,
}

/// The HFS/HFS+ metadata editor: edit a file's Type, Creator, and modified date.
struct MetaEdit {
    entry_name: String,
    type_code: String,
    creator: String,
    modified: String,
    /// Focused field: 0 = type, 1 = creator, 2 = modified.
    field: usize,
    error: Option<String>,
}

/// Seconds between the Mac (1904) and Unix (1970) epochs.
const MAC_EPOCH_OFFSET: i64 = 2_082_844_800;

/// Format a Mac-epoch timestamp as `YYYY-MM-DD HH:MM:SS` (UTC).
fn format_mac_date(mac: u32) -> String {
    let unix = mac as i64 - MAC_EPOCH_OFFSET;
    chrono::DateTime::from_timestamp(unix, 0)
        .map(|dt| dt.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_default()
}

/// Parse `YYYY-MM-DD HH:MM:SS` back to a Mac-epoch timestamp.
fn parse_mac_date(s: &str) -> Option<u32> {
    let ndt = chrono::NaiveDateTime::parse_from_str(s.trim(), "%Y-%m-%d %H:%M:%S").ok()?;
    let mac = ndt.and_utc().timestamp() + MAC_EPOCH_OFFSET;
    (0..=u32::MAX as i64).contains(&mac).then_some(mac as u32)
}

/// A 4-byte Mac OSType as an editable ASCII string (non-printable → `?`).
fn code_to_string(code: Option<[u8; 4]>) -> String {
    code.map(|b| {
        b.iter()
            .map(|&c| {
                if (0x20..0x7f).contains(&c) {
                    c as char
                } else {
                    '?'
                }
            })
            .collect()
    })
    .unwrap_or_default()
}

/// An export target chosen from the "Export as" menu.
#[derive(Clone, Copy)]
enum ExportFormat {
    /// A single file, written into the destination dir via the shared
    /// `fork_export::export_file_with_fork` (data / AppleDouble / MacBinary / …).
    Fork(crate::fs::resource_fork::ResourceForkMode),
    /// The whole selection bundled into one archive (or folder) via the shared
    /// `export_selection` engine — tar / zip / StuffIt / MacArchive, etc. Every
    /// Space-marked row is included (folders walk recursively).
    Archive(crate::fs::export_selection::ExportFormat),
}

use crate::fs::export_selection::ExportFormat as EsFormat;
use crate::fs::resource_fork::ResourceForkMode as RfMode;

/// The formats offered by the "Export as" menu.
const EXPORT_FORMATS: &[(&str, ExportFormat)] = &[
    (
        "AppleDouble (._file)",
        ExportFormat::Fork(RfMode::AppleDouble),
    ),
    ("Data fork only", ExportFormat::Fork(RfMode::DataForkOnly)),
    ("MacBinary (.bin)", ExportFormat::Fork(RfMode::MacBinary)),
    ("BinHex 4.0 (.hqx)", ExportFormat::Fork(RfMode::BinHex)),
    (
        "Separate .rsrc sidecar",
        ExportFormat::Fork(RfMode::SeparateRsrc),
    ),
    ("tar archive (.tar)", ExportFormat::Archive(EsFormat::Tar)),
    (
        "tar + gzip (.tar.gz)",
        ExportFormat::Archive(EsFormat::TarGz),
    ),
    (
        "tar + zstd (.tar.zst)",
        ExportFormat::Archive(EsFormat::TarZstd),
    ),
    ("Zip (.zip)", ExportFormat::Archive(EsFormat::Zip)),
    ("StuffIt (.sit)", ExportFormat::Archive(EsFormat::StuffIt)),
    (
        "Mac Archive (.mar)",
        ExportFormat::Archive(EsFormat::MacArchive),
    ),
];

/// A scrollable file view (text lines, or a hex dump for binary content).
struct Preview {
    name: String,
    /// HFS `TYPE/crea` or ProDOS `$XX` filetype label, when the fs has one.
    type_label: Option<String>,
    /// Rendered data fork.
    data: Vec<String>,
    /// Rendered resource fork, when the file has one.
    rsrc: Option<Vec<String>>,
    /// Whether the resource fork is currently shown (toggle with `r`).
    showing_rsrc: bool,
    scroll: usize,
}

impl Preview {
    /// The lines of the fork currently being shown.
    fn lines(&self) -> &[String] {
        if self.showing_rsrc {
            self.rsrc.as_deref().unwrap_or(&[])
        } else {
            &self.data
        }
    }
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
            PickKind::File => path.is_file(),
            PickKind::Dir => path.is_dir(),
        };
        if ok {
            Some(PickResult::Confirm(path))
        } else {
            let what = match self.kind {
                PickKind::Dir => "directory",
                PickKind::File => "file",
                PickKind::Any => "path",
            };
            self.error = Some(format!("No such {what}: {}", path.display()));
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
            // File mode: descend dirs, confirm files.
            PickKind::File => {
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
            color: std::env::var_os("NO_COLOR").is_none(),
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
}

/// The two media classes the wizard can create end-to-end. `new hd` (x68k /
/// sgi-efs) and CD-ROM (`optical new`) need donor / partition options, so they
/// stay CLI-driven for now and are surfaced as a note, not a class.
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

/// The details form's three text fields.
const WIZ_FIELDS: usize = 3;

struct NewWizard {
    step: WizStep,
    class_sel: usize,
    fs_sel: usize,
    /// Details form: 0 = path, 1 = size, 2 = name.
    field: usize,
    path: String,
    size: String,
    name: String,
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
    fn fs_count(&self) -> usize {
        match self.class() {
            DiskClass::Floppy => FLOPPY_FS.len(),
            DiskClass::Volume => VOLUME_FS.len(),
        }
    }
    fn fs_label(&self, i: usize) -> &'static str {
        match self.class() {
            DiskClass::Floppy => FLOPPY_FS[i.min(FLOPPY_FS.len() - 1)].0,
            DiskClass::Volume => VOLUME_FS[i.min(VOLUME_FS.len() - 1)].0,
        }
    }
}

/// The Backup screen: a guided source -> config -> run flow driving the shared
/// `backup::run_backup` on a worker thread (the same runner the GUI and CLI
/// use). Progress is polled from the runner's `BackupProgress` and mirrored into
/// the shell's bottom progress bar.
#[derive(Clone, Copy, PartialEq, Eq)]
enum BackupStep {
    Source,
    Config,
    Run,
}

/// Output formats offered on the Backup config form (label -> compression).
const BACKUP_FORMATS: &[(&str, crate::backup::CompressionType)] = &[
    ("Zstd", crate::backup::CompressionType::Zstd),
    ("CHD (single-file)", crate::backup::CompressionType::Chd),
    ("VHD", crate::backup::CompressionType::Vhd),
    ("gzip", crate::backup::CompressionType::Gzip),
    ("LZ4", crate::backup::CompressionType::Lz4),
    ("Raw (uncompressed)", crate::backup::CompressionType::None),
];

/// Checksum choices on the Backup config form (label -> algorithm).
const BACKUP_CHECKSUMS: &[(&str, crate::backup::ChecksumType)] = &[
    ("SHA-256", crate::backup::ChecksumType::Sha256),
    ("CRC32", crate::backup::ChecksumType::Crc32),
];

/// Config-form rows: 0 output dir, 1 name, 2 format, 3 checksum, 4 "Start".
const BACKUP_FIELDS: usize = 5;

struct BackupState {
    step: BackupStep,
    /// Source path (an image file, or a physical device path).
    source: String,
    from_device: bool,
    /// Source image file picker, when open.
    picker: Option<FilePicker>,
    /// Physical-disk chooser sub-mode + its cursor.
    device_pick: bool,
    device_sel: usize,
    /// Config form.
    out_dir: String,
    name: String,
    format_sel: usize,
    checksum_sel: usize,
    field: usize,
    /// A running backup's shared progress, once started.
    run: Option<Arc<Mutex<crate::backup::BackupProgress>>>,
    /// Latest operation string from the runner (shown during Run).
    op: String,
    /// Terminal result (success or error) once finished.
    result: Option<String>,
    is_error: bool,
}

impl Default for BackupState {
    fn default() -> Self {
        BackupState {
            step: BackupStep::Source,
            source: String::new(),
            from_device: false,
            picker: None,
            device_pick: false,
            device_sel: 0,
            out_dir: String::new(),
            name: String::new(),
            format_sel: 0,
            checksum_sel: 0,
            field: 0,
            run: None,
            op: String::new(),
            result: None,
            is_error: false,
        }
    }
}

/// The Restore screen: source (backup folder / `.cbk`) -> config (target /
/// sizing / alignment) -> run, driving `restore::run_restore` on a worker
/// thread. Mirrors the Backup screen.
#[derive(Clone, Copy, PartialEq, Eq)]
enum RestoreStep {
    Source,
    Config,
    Run,
}

/// Partition-sizing policy applied to every partition (a first cut; per-partition
/// custom sizing is a later phase). Index into these must match `start_restore`.
const RESTORE_SIZE_LABELS: &[&str] = &["Original", "Minimum"];
/// Restore alignment choices; index must match `start_restore`.
const RESTORE_ALIGN_LABELS: &[&str] = &["Original", "Modern 1MB"];
/// Config-form rows: 0 target, 1 size, 2 alignment, 3 "Start".
const RESTORE_FIELDS: usize = 4;

struct RestoreState {
    step: RestoreStep,
    /// The loaded backup folder (a native folder, or a temp dir from a `.cbk`).
    backup_folder: String,
    /// Original disk size from the backup metadata (default target size).
    source_size: u64,
    part_count: usize,
    loaded: bool,
    /// Shared picker; `picker_target` says whether a confirm sets the target.
    picker: Option<FilePicker>,
    picker_target: bool,
    /// Config form.
    target: String,
    size_sel: usize,
    align_sel: usize,
    field: usize,
    /// A running restore's shared progress, once started.
    run: Option<Arc<Mutex<crate::restore::RestoreProgress>>>,
    op: String,
    result: Option<String>,
    is_error: bool,
    /// Keeps a `.cbk` temp dir alive until a restore from it finishes.
    cbk_guard: Option<tempfile::TempDir>,
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
}

impl Default for RestoreState {
    fn default() -> Self {
        RestoreState {
            step: RestoreStep::Source,
            backup_folder: String::new(),
            source_size: 0,
            part_count: 0,
            loaded: false,
            picker: None,
            picker_target: false,
            target: String::new(),
            size_sel: 0,
            align_sel: 0,
            field: 0,
            run: None,
            op: String::new(),
            result: None,
            is_error: false,
            cbk_guard: None,
            target_type_menu: None,
            device_pick: false,
            device_sel: 0,
            target_is_device: false,
            confirm_device: None,
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
const BULK_FORMATS: &[(&str, crate::rbformats::export::ExportFormat)] = &[
    ("Raw (.img)", crate::rbformats::export::ExportFormat::Raw),
    ("VHD", crate::rbformats::export::ExportFormat::Vhd),
    (
        "VHD Dynamic",
        crate::rbformats::export::ExportFormat::VhdDynamic,
    ),
    ("QCOW2", crate::rbformats::export::ExportFormat::Qcow2),
    (
        "VMDK Flat",
        crate::rbformats::export::ExportFormat::VmdkFlat,
    ),
    (
        "VMDK Sparse",
        crate::rbformats::export::ExportFormat::VmdkSparse,
    ),
    (
        "CHD (hard disk)",
        crate::rbformats::export::ExportFormat::Chd,
    ),
    ("DVD CHD", crate::rbformats::export::ExportFormat::ChdDvd),
];

struct BulkState {
    step: BulkStep,
    source: String,
    out_dir: String,
    files: Vec<crate::model::bulk_convert_runner::ScannedFile>,
    format_sel: usize,
    /// Flat cursor over the config rows: 0 = Format, 1 = Output, then one row per
    /// scanned file, and a final "Start" row.
    field: usize,
    picker: Option<FilePicker>,
    picker_target: bool,
    run: Option<Arc<Mutex<crate::model::status::BulkConvertStatus>>>,
    op: String,
    result: Option<String>,
    is_error: bool,
}

impl Default for BulkState {
    fn default() -> Self {
        BulkState {
            step: BulkStep::Source,
            source: String::new(),
            out_dir: String::new(),
            files: Vec::new(),
            format_sel: 0,
            field: 0,
            picker: None,
            picker_target: false,
            run: None,
            op: String::new(),
            result: None,
            is_error: false,
        }
    }
}

impl BulkState {
    fn format(&self) -> crate::rbformats::export::ExportFormat {
        BULK_FORMATS[self.format_sel.min(BULK_FORMATS.len() - 1)].1
    }
    fn start_row(&self) -> usize {
        2 + self.files.len()
    }
    /// The file index at the current cursor, if the cursor is on a file row.
    fn file_at_cursor(&self) -> Option<usize> {
        if self.field >= 2 && self.field < 2 + self.files.len() {
            Some(self.field - 2)
        } else {
            None
        }
    }
}

/// The Optical screen (rip): choose a drive -> config (output / format / eject)
/// -> run `optical::rip::run_rip` on a worker thread.
#[cfg(feature = "optical")]
#[derive(Clone, Copy, PartialEq, Eq)]
enum OpticalStep {
    Drives,
    Config,
    Run,
}

/// Rip output formats.
#[cfg(feature = "optical")]
const RIP_FORMATS: &[(&str, crate::optical::rip::RipFormat)] = &[
    ("ISO (data)", crate::optical::rip::RipFormat::Iso),
    ("BIN/CUE (raw)", crate::optical::rip::RipFormat::BinCue),
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
];

#[cfg(feature = "optical")]
struct OpticalState {
    step: OpticalStep,
    drives: Vec<crate::model::optical_devices::RipDevice>,
    drive_sel: usize,
    device_path: String,
    device_name: String,
    output: String,
    format_sel: usize,
    eject: bool,
    field: usize,
    picker: Option<FilePicker>,
    run: Option<Arc<Mutex<crate::optical::rip::RipProgress>>>,
    op: String,
    result: Option<String>,
    is_error: bool,
    /// The optical-image-operations launcher menu (selected index), when open.
    image_menu: Option<usize>,
}

#[cfg(feature = "optical")]
impl Default for OpticalState {
    fn default() -> Self {
        OpticalState {
            step: OpticalStep::Drives,
            drives: Vec::new(),
            drive_sel: 0,
            device_path: String::new(),
            device_name: String::new(),
            output: String::new(),
            format_sel: 0,
            eject: false,
            field: 0,
            picker: None,
            run: None,
            op: String::new(),
            result: None,
            is_error: false,
            image_menu: None,
        }
    }
}

/// Which Commander pane has focus.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
enum Side {
    #[default]
    Left,
    Right,
}

/// One Commander pane: a host folder or an image partition, browsed through the
/// shared `DirListing`. Image panes keep a `BrowseSession` so a host->image copy
/// can be applied through `commander_ops::apply_edits`.
struct CmdPane {
    listing: crate::model::dir_listing::DirListing,
    session: Option<crate::model::browse_session::BrowseSession>,
    is_host: bool,
    loaded: bool,
    label: String,
    sel: usize,
    /// Open flow: a folder/file picker (host vs image by `picker_host`).
    picker: Option<FilePicker>,
    picker_host: bool,
    /// Partition chooser for a multi-partition image.
    parts: Vec<crate::partition::PartitionInfo>,
    part_pick: bool,
    part_sel: usize,
    pending_img: Option<std::path::PathBuf>,
    /// Set when this pane is showing an optical disc image's filesystem (a
    /// hybrid CD's ISO 9660 / HFS side). Read-only; carries what a refresh /
    /// re-switch needs. `None` for host / partition-image / archive panes.
    optical: Option<OpticalPane>,
    /// The optical-filesystem chooser modal (ISO 9660 vs HFS side) + its cursor.
    optical_pick: bool,
    optical_sel: usize,
    /// Set when this pane is browsing a remote daemon (host FS or an image on
    /// it). The listing reads over the wire; uploads go to the daemon. `None`
    /// for local panes.
    #[cfg(feature = "remote")]
    remote: Option<crate::model::remote_browser::RemoteBrowser>,
}

/// An optical disc image opened in a Commander pane: the path, its selectable
/// filesystems (primary ISO 9660 + any hybrid Mac side), and the one currently
/// shown. Read-only; a refresh / re-switch reopens via `commander_descend`.
#[derive(Clone)]
struct OpticalPane {
    path: std::path::PathBuf,
    choices: Vec<crate::model::commander_descend::OpticalFsChoice>,
    sel: usize,
}

impl Default for CmdPane {
    fn default() -> Self {
        CmdPane {
            listing: crate::model::dir_listing::DirListing::new(),
            session: None,
            is_host: false,
            loaded: false,
            label: String::new(),
            sel: 0,
            picker: None,
            picker_host: false,
            parts: Vec::new(),
            part_pick: false,
            part_sel: 0,
            pending_img: None,
            optical: None,
            optical_pick: false,
            optical_sel: 0,
            #[cfg(feature = "remote")]
            remote: None,
        }
    }
}

impl CmdPane {
    fn rows_len(&self) -> usize {
        self.listing.current_rows().len()
    }
    /// The `FileEntry` under the cursor, if the cursor is on a real entry.
    fn selected(&self) -> Option<FileEntry> {
        match self.listing.current_rows().get(self.sel) {
            Some(crate::model::dir_listing::Row::Entry(e)) => Some((*e).clone()),
            _ => None,
        }
    }

    /// The entries a copy/delete should act on: the multi-selection if any is
    /// marked (Space), else the single entry under the cursor.
    fn action_entries(&self) -> Vec<FileEntry> {
        if !self.listing.selection().is_empty() {
            self.listing
                .selected_entries()
                .into_iter()
                .cloned()
                .collect()
        } else {
            self.selected().into_iter().collect()
        }
    }

    /// Toggle the multi-selection state of the entry under the cursor.
    fn toggle_mark(&mut self) {
        if let Some(e) = self.selected() {
            self.listing.ctrl_click(&e.name);
        }
    }

    /// Stage the optical-filesystem chooser (used when a disc carries more than
    /// one filesystem — e.g. a hybrid CD's ISO 9660 + HFS sides). Nothing is
    /// loaded until the user confirms a choice.
    fn optical_choices_stage(
        &mut self,
        choices: Vec<crate::model::commander_descend::OpticalFsChoice>,
        path: std::path::PathBuf,
    ) {
        self.optical = Some(OpticalPane {
            path,
            choices,
            sel: 0,
        });
        self.optical_pick = true;
        self.optical_sel = 0;
    }

    /// True when this pane is browsing a remote daemon (host FS or an image).
    #[cfg(feature = "remote")]
    fn is_remote(&self) -> bool {
        self.remote.is_some()
    }

    /// True when this pane is browsing a remote daemon's *host* filesystem
    /// (vs. an image opened on it).
    #[cfg(feature = "remote")]
    fn is_remote_host(&self) -> bool {
        matches!(
            self.remote.as_ref().map(|b| b.mode()),
            Some(crate::model::remote_browser::BrowseMode::Host)
        )
    }

    /// The daemon address (`host:port`) this pane is connected to, if remote.
    #[cfg(feature = "remote")]
    fn remote_addr(&self) -> Option<String> {
        self.remote.as_ref().map(|b| b.addr().to_string())
    }

    /// The remote image `(image_path, partition)` this pane is browsing, if it
    /// is inside a disk image on the daemon (vs. the host file browser).
    #[cfg(feature = "remote")]
    fn remote_image_target(&self) -> Option<(String, Option<u32>)> {
        match self.remote.as_ref().map(|b| b.mode()) {
            Some(crate::model::remote_browser::BrowseMode::Image { path, partition }) => {
                Some((path.clone(), *partition))
            }
            _ => None,
        }
    }
}

/// The Commander dual-pane file manager.
#[derive(Default)]
struct CommanderState {
    left: CmdPane,
    right: CmdPane,
    active: Side,
    status: Option<String>,
    is_error: bool,
    /// A new-folder name being typed for the active pane, if the mkdir prompt
    /// is showing.
    mkdir_input: Option<String>,
    /// The noun phrase (e.g. `folder "System"` or `3 items`) of the active
    /// pane's pending delete, shown in the confirmation prompt.
    confirm_delete: Option<String>,
    /// A running checksum job (CRC32/MD5/SHA1/SHA256 of the marked files),
    /// polled each `tick`. `None` when idle.
    checksum: Option<Arc<Mutex<crate::model::checksum::ChecksumStatus>>>,
    /// The scrollable checksum-results overlay once a job completes.
    checksum_report: Option<ChecksumReportView>,
    /// The File Info overlay for the active pane's cursor entry (`i`), holding
    /// the rendered attribute lines. `None` when closed.
    info: Option<InfoView>,
    /// An attribute edit being typed from the info overlay: which field, and the
    /// text so far. `None` when no prompt is showing.
    attr_input: Option<AttrPrompt>,
    /// A `host:port` being typed in the "Connect to remote" prompt, if showing.
    #[cfg(feature = "remote")]
    connect_input: Option<String>,
    /// An in-flight daemon connection (blocking, so it runs on a worker and is
    /// polled each `tick`); carries the side to install the result into.
    #[cfg(feature = "remote")]
    pending_connect: Option<(Side, Arc<Mutex<ConnectStatus>>)>,
}

/// The File Info overlay: the entry it describes plus its rendered lines.
struct InfoView {
    /// The entry being described (re-resolved on each edit so staged changes
    /// show immediately).
    entry: crate::fs::entry::FileEntry,
    /// On-disk extended attributes, read when the overlay opened.
    xattrs: Vec<crate::fs::xattr::Xattr>,
    /// uid/gid -> name map from the image's own account files, if any.
    id_names: crate::fs::id_names::IdNameMap,
}

/// Which attribute the user is typing a new value for, from the info overlay.
#[derive(Clone, Copy, PartialEq, Eq)]
enum AttrField {
    /// Octal permission bits.
    Mode,
    /// `uid:gid`.
    Owner,
    /// `name=value` (value may be `0x`-hex); an empty value deletes.
    Xattr,
}

impl AttrField {
    /// The prompt label shown above the input line.
    fn label(self) -> &'static str {
        match self {
            Self::Mode => "New permissions (octal, e.g. 755):",
            Self::Owner => "New owner (uid:gid, e.g. 0:0):",
            Self::Xattr => "Xattr as name=value (0x.. for hex; empty value deletes):",
        }
    }
}

/// An in-progress attribute edit typed in the info overlay.
struct AttrPrompt {
    field: AttrField,
    text: String,
}

/// Result of a background [`RemoteBrowser::connect`]: the browser plus its first
/// browse target, or an error string.
#[cfg(feature = "remote")]
type ConnectResult = Result<
    (
        crate::model::remote_browser::RemoteBrowser,
        crate::model::remote_browser::BrowseTarget,
    ),
    String,
>;

/// Shared state for a background connect, polled in `tick`.
#[cfg(feature = "remote")]
#[derive(Default)]
struct ConnectStatus {
    finished: bool,
    result: Option<ConnectResult>,
}

/// A scrollable checksum-results overlay (per-file SHA256 + CRC32).
struct ChecksumReportView {
    lines: Vec<String>,
    scroll: usize,
}

impl CommanderState {
    fn pane(&self, side: Side) -> &CmdPane {
        match side {
            Side::Left => &self.left,
            Side::Right => &self.right,
        }
    }
    fn pane_mut(&mut self, side: Side) -> &mut CmdPane {
        match side {
            Side::Left => &mut self.left,
            Side::Right => &mut self.right,
        }
    }

    /// Load a host folder into the active pane.
    fn open_host(&mut self, path: std::path::PathBuf) {
        let side = self.active;
        match cmd_load_host(self.pane_mut(side), path) {
            Ok(()) => {
                self.status = None;
                self.is_error = false;
            }
            Err(e) => {
                self.status = Some(e);
                self.is_error = true;
            }
        }
    }

    /// Open an image file into the active pane: show the partition chooser when
    /// there's more than one, else load directly (superfloppy = whole disk).
    fn open_image(&mut self, path: std::path::PathBuf) {
        let side = self.active;

        // Optical disc image first: a hybrid Mac/PC CD carries an ISO 9660 volume
        // *and* an HFS side that the partition table (APM) hides, so enumerate
        // its filesystems via opticaldiscs and offer them in the chooser.
        if is_optical_image_path(&path) {
            let choices = crate::model::commander_descend::optical_filesystems(&path);
            if !choices.is_empty() {
                if choices.len() > 1 {
                    let p = self.pane_mut(side);
                    p.optical_choices_stage(choices, path);
                } else {
                    match cmd_load_optical(self.pane_mut(side), &path, choices, 0) {
                        Ok(()) => {
                            self.status = None;
                            self.is_error = false;
                        }
                        Err(e) => {
                            self.status = Some(e);
                            self.is_error = true;
                        }
                    }
                }
                return;
            }
        }

        let parts = crate::model::commander_source::probe_partitions(&path).unwrap_or_default();
        if parts.len() > 1 {
            let p = self.pane_mut(side);
            p.parts = parts;
            p.part_pick = true;
            p.part_sel = 0;
            p.pending_img = Some(path);
        } else {
            let part = parts.first().cloned();
            match cmd_load_image(self.pane_mut(side), &path, part.as_ref()) {
                Ok(()) => {
                    self.status = None;
                    self.is_error = false;
                }
                Err(e) => {
                    self.status = Some(e);
                    self.is_error = true;
                }
            }
        }
    }

    /// Confirm the partition chooser and open the selected partition.
    fn choose_partition(&mut self) {
        let side = self.active;
        let (path, part) = {
            let p = self.pane_mut(side);
            (p.pending_img.clone(), p.parts.get(p.part_sel).cloned())
        };
        if let Some(path) = path {
            match cmd_load_image(self.pane_mut(side), &path, part.as_ref()) {
                Ok(()) => {
                    self.status = None;
                    self.is_error = false;
                }
                Err(e) => {
                    self.status = Some(e);
                    self.is_error = true;
                }
            }
        }
    }

    /// Confirm the optical-filesystem chooser and open the selected side.
    fn choose_optical(&mut self) {
        let side = self.active;
        let (path, choices, idx) = {
            let p = self.pane(side);
            match &p.optical {
                Some(op) => (op.path.clone(), op.choices.clone(), p.optical_sel),
                None => return,
            }
        };
        match cmd_load_optical(self.pane_mut(side), &path, choices, idx) {
            Ok(()) => {
                self.status = None;
                self.is_error = false;
            }
            Err(e) => {
                self.status = Some(e);
                self.is_error = true;
            }
        }
    }

    /// Reopen the filesystem chooser for the active pane so the user can switch
    /// sides of a hybrid optical disc (ISO 9660 <-> HFS) after opening.
    fn reopen_chooser(&mut self) {
        let side = self.active;
        let p = self.pane_mut(side);
        match &p.optical {
            Some(op) if op.choices.len() > 1 => {
                p.optical_sel = op.sel;
                p.optical_pick = true;
            }
            _ => {
                self.status = Some("Filesystem switching is for hybrid optical discs.".to_string());
                self.is_error = false;
            }
        }
    }

    fn refresh_active(&mut self) {
        let side = self.active;
        cmd_refresh(self.pane_mut(side));
        self.status = Some("Refreshed.".to_string());
        self.is_error = false;
    }

    /// Start hashing the active pane's marked files (CRC32/MD5/SHA1/SHA256) on a
    /// worker thread; `tick` polls it and shows the results overlay when done.
    /// Directories in the selection are skipped.
    fn start_checksum(&mut self) {
        if self.checksum.is_some() {
            self.status = Some("A checksum is already running.".to_string());
            self.is_error = true;
            return;
        }
        let side = self.active;
        let pane = self.pane(side);
        if !pane.loaded {
            self.status = Some("Open a source first.".to_string());
            self.is_error = true;
            return;
        }
        let entries: Vec<FileEntry> = pane
            .action_entries()
            .into_iter()
            .filter(|e| matches!(e.entry_type, EntryType::File))
            .collect();
        if entries.is_empty() {
            self.status =
                Some("Select one or more files to checksum (Space marks more).".to_string());
            self.is_error = true;
            return;
        }
        let n = entries.len();
        let job = if pane.is_host {
            crate::model::checksum::ChecksumJob::Host { entries }
        } else {
            match cmd_checksum_source(pane) {
                Some(source) => crate::model::checksum::ChecksumJob::Image { source, entries },
                None => {
                    self.status = Some("Source volume is not open.".to_string());
                    self.is_error = true;
                    return;
                }
            }
        };
        self.checksum = Some(crate::model::checksum::spawn(job));
        self.checksum_report = None;
        self.status = Some(format!("Hashing {n} file(s)..."));
        self.is_error = false;
    }

    /// Begin connecting the active pane to a daemon at `addr` (a bare host uses
    /// the default port). Blocking, so it runs on a worker thread; `tick`
    /// installs the resulting remote listing into the pane.
    #[cfg(feature = "remote")]
    fn start_connect(&mut self, addr: String) {
        if self.pending_connect.is_some() {
            self.status = Some("A connection is already in progress.".to_string());
            self.is_error = true;
            return;
        }
        let side = self.active;
        let addr = if addr.contains(':') {
            addr
        } else {
            format!("{addr}:{}", crate::remote::protocol::DEFAULT_PORT)
        };
        let status = Arc::new(Mutex::new(ConnectStatus::default()));
        let st = Arc::clone(&status);
        let addr_worker = addr.clone();
        std::thread::spawn(move || {
            let r = crate::model::remote_browser::RemoteBrowser::connect(&addr_worker, "/")
                .map_err(|e| format!("{e:#}"));
            if let Ok(mut g) = st.lock() {
                g.result = Some(r);
                g.finished = true;
            }
        });
        self.pending_connect = Some((side, status));
        self.status = Some(format!("Connecting to {addr}..."));
        self.is_error = false;
    }

    /// Copy the selected entry from the active pane to the other pane's cwd.
    /// host->image stages + applies edits; image->host exports the fork; host->
    /// host is a plain copy; image->image extracts+stages. A remote destination
    /// uploads over the wire.
    fn copy(&mut self) {
        let (src, dst) = match self.active {
            Side::Left => (&mut self.left, &mut self.right),
            Side::Right => (&mut self.right, &mut self.left),
        };
        if !src.loaded || !dst.loaded {
            self.status = Some("Both panes must be open to copy.".to_string());
            self.is_error = true;
            return;
        }
        let entries = src.action_entries();
        if entries.is_empty() {
            self.status = Some("Select a file to copy (Space marks more).".to_string());
            self.is_error = true;
            return;
        }
        // A short summary of what was copied for the status line.
        let label = if entries.len() == 1 {
            entries[0].name.clone()
        } else {
            format!("{} items", entries.len())
        };

        // Remote destination: upload over the wire (a remote pane has
        // is_host == false, so it would otherwise misroute to the image path,
        // which needs a local write session it doesn't have). Downloads FROM a
        // remote pane fall through to the normal image-source branches below,
        // which already read over the wire via the boxed remote filesystem.
        #[cfg(feature = "remote")]
        if dst.is_remote() {
            use crate::model::commander_ops;
            let result: Result<String, String> = (|| {
                let addr = dst
                    .remote_addr()
                    .ok_or_else(|| "the remote connection is unavailable".to_string())?;
                let dest_parent = dst
                    .listing
                    .cwd()
                    .cloned()
                    .ok_or_else(|| "no destination directory".to_string())?;
                if dst.is_remote_host() {
                    // Remote host filesystem: upload files/dirs directly.
                    if src.is_host {
                        commander_ops::upload_host_entries_to_remote(
                            &entries,
                            &addr,
                            &dest_parent.path,
                        )
                        .map_err(|e| format!("Upload failed: {e:#}"))?;
                    } else {
                        let fs = src
                            .listing
                            .fs_mut()
                            .ok_or_else(|| "source has no filesystem".to_string())?;
                        commander_ops::upload_fs_entries_to_remote(
                            fs,
                            &entries,
                            &addr,
                            &dest_parent.path,
                            RfMode::AppleDouble,
                        )
                        .map_err(|e| format!("Upload failed: {e:#}"))?;
                    }
                } else {
                    // Remote disk image: stage AddFile/CreateDirectory edits and
                    // apply them over the daemon's Family-F write path.
                    let (image_path, partition) = dst
                        .remote_image_target()
                        .ok_or_else(|| "the remote image target is unavailable".to_string())?;
                    if src.is_host {
                        let edits = commander_ops::stage_host_to_image(&entries, &dest_parent);
                        commander_ops::apply_edits_to_remote_image(
                            &addr,
                            &image_path,
                            partition,
                            &edits,
                        )
                        .map_err(|e| format!("Upload failed: {e:#}"))?;
                    } else {
                        // image -> remote image: extract to a temp dir, stage the
                        // AddFiles (which point into it), then apply. The temp dir
                        // must outlive the apply.
                        let src_fs = src
                            .listing
                            .fs_mut()
                            .ok_or_else(|| "source has no filesystem".to_string())?;
                        let temp = tempfile::tempdir().map_err(|e| format!("temp dir: {e}"))?;
                        let edits = with_stderr_suppressed(|| {
                            commander_ops::stage_copy(
                                src_fs,
                                &entries,
                                &dest_parent,
                                temp.path(),
                                true,
                            )
                        })
                        .map_err(|e| format!("Copy failed: {e:#}"))?;
                        commander_ops::apply_edits_to_remote_image(
                            &addr,
                            &image_path,
                            partition,
                            &edits,
                        )
                        .map_err(|e| format!("Upload failed: {e:#}"))?;
                    }
                }
                Ok(format!("Uploaded {label} to the remote."))
            })();
            if result.is_ok() {
                cmd_refresh(dst);
                src.listing.clear_selection();
            }
            match result {
                Ok(m) => {
                    self.status = Some(m);
                    self.is_error = false;
                }
                Err(e) => {
                    self.status = Some(e);
                    self.is_error = true;
                }
            }
            return;
        }

        let result: Result<String, String> = if src.is_host && !dst.is_host {
            let dest_parent = match dst.listing.cwd() {
                Some(e) => e.clone(),
                None => {
                    self.status = Some("No destination directory.".to_string());
                    self.is_error = true;
                    return;
                }
            };
            let session = match dst.session.clone() {
                Some(s) => s,
                None => {
                    self.status = Some("Destination has no write session.".to_string());
                    self.is_error = true;
                    return;
                }
            };
            let edits = crate::model::commander_ops::stage_host_to_image(&entries, &dest_parent);
            with_stderr_suppressed(|| crate::model::commander_ops::apply_edits(&session, &edits))
                .map(|()| format!("Copied {label} into the image."))
                .map_err(|e| format!("Copy failed: {e:#}"))
        } else if !src.is_host && dst.is_host {
            let dest_dir = match dst.listing.cwd() {
                Some(e) => std::path::PathBuf::from(&e.path),
                None => {
                    self.status = Some("No destination directory.".to_string());
                    self.is_error = true;
                    return;
                }
            };
            let fs = match src.listing.fs_mut() {
                Some(f) => f,
                None => {
                    self.status = Some("Source has no filesystem.".to_string());
                    self.is_error = true;
                    return;
                }
            };
            with_stderr_suppressed(|| -> Result<(), String> {
                for e in &entries {
                    crate::fs::fork_export::export_file_with_fork(
                        fs,
                        e,
                        &dest_dir,
                        &e.name,
                        RfMode::AppleDouble,
                    )
                    .map_err(|err| format!("Copy failed on {}: {err:#}", e.name))?;
                }
                Ok(())
            })
            .map(|()| format!("Copied {label} to the host."))
        } else if src.is_host && dst.is_host {
            let dest_dir = match dst.listing.cwd() {
                Some(e) => std::path::PathBuf::from(&e.path),
                None => {
                    self.status = Some("No destination directory.".to_string());
                    self.is_error = true;
                    return;
                }
            };
            (|| -> Result<(), String> {
                for e in &entries {
                    std::fs::copy(std::path::PathBuf::from(&e.path), dest_dir.join(&e.name))
                        .map_err(|err| format!("Copy failed on {}: {err}", e.name))?;
                }
                Ok(())
            })()
            .map(|()| format!("Copied {label}."))
        } else {
            // image -> image: extract the entries to a temp dir, stage them as
            // AddFiles on the destination's browse session, then apply. The temp
            // dir must outlive apply_edits (the staged edits point into it).
            let dest_parent = match dst.listing.cwd() {
                Some(e) => e.clone(),
                None => {
                    self.status = Some("No destination directory.".to_string());
                    self.is_error = true;
                    return;
                }
            };
            let session = match dst.session.clone() {
                Some(s) => s,
                None => {
                    self.status = Some("Destination has no write session.".to_string());
                    self.is_error = true;
                    return;
                }
            };
            let src_fs = match src.listing.fs_mut() {
                Some(f) => f,
                None => {
                    self.status = Some("Source has no filesystem.".to_string());
                    self.is_error = true;
                    return;
                }
            };
            (|| -> Result<String, String> {
                let temp = tempfile::tempdir().map_err(|e| format!("temp dir: {e}"))?;
                let edits = with_stderr_suppressed(|| {
                    crate::model::commander_ops::stage_copy(
                        src_fs,
                        &entries,
                        &dest_parent,
                        temp.path(),
                        true,
                    )
                })
                .map_err(|e| format!("Copy failed: {e:#}"))?;
                with_stderr_suppressed(|| {
                    crate::model::commander_ops::apply_edits(&session, &edits)
                })
                .map_err(|e| format!("Copy failed: {e:#}"))?;
                Ok(format!("Copied {label} into the image."))
            })()
        };
        // The destination refreshes after the source borrow is released
        // (host or image, both kinds); the source's marks are consumed.
        if result.is_ok() {
            cmd_refresh(dst);
            src.listing.clear_selection();
        }
        match result {
            Ok(m) => {
                self.status = Some(m);
                self.is_error = false;
            }
            Err(e) => {
                self.status = Some(e);
                self.is_error = true;
            }
        }
    }

    /// Set the status line from a `Result`, error styling on `Err`.
    fn set_result(&mut self, r: Result<String, String>) {
        match r {
            Ok(m) => {
                self.status = Some(m);
                self.is_error = false;
            }
            Err(e) => {
                self.status = Some(e);
                self.is_error = true;
            }
        }
    }

    /// Create a directory named `name` in the active pane's cwd (host: real
    /// `create_dir`; image: a `CreateDirectory` staged edit applied through the
    /// pane's browse session), then refresh the pane.
    fn mkdir(&mut self, name: &str) {
        let name = name.trim();
        if name.is_empty() {
            self.status = Some("Folder name can't be empty.".to_string());
            self.is_error = true;
            return;
        }
        let side = self.active;
        let pane = self.pane_mut(side);
        let result: Result<String, String> = if pane.is_host {
            match pane.listing.cwd() {
                Some(cwd) => std::fs::create_dir(std::path::PathBuf::from(&cwd.path).join(name))
                    .map(|()| format!("Created folder {name}."))
                    .map_err(|e| format!("New folder failed: {e}")),
                None => Err("No current directory.".to_string()),
            }
        } else {
            let parent = match pane.listing.cwd() {
                Some(e) => e.clone(),
                None => {
                    self.status = Some("No current directory.".to_string());
                    self.is_error = true;
                    return;
                }
            };
            let session = match pane.session.clone() {
                Some(s) => s,
                None => {
                    self.status = Some("Pane has no write session.".to_string());
                    self.is_error = true;
                    return;
                }
            };
            let edits = vec![crate::model::edit_queue::StagedEdit::CreateDirectory {
                parent,
                name: name.to_string(),
            }];
            with_stderr_suppressed(|| crate::model::commander_ops::apply_edits(&session, &edits))
                .map(|()| format!("Created folder {name}."))
                .map_err(|e| format!("New folder failed: {e:#}"))
        };
        if result.is_ok() {
            cmd_refresh(self.pane_mut(side));
        }
        self.set_result(result);
    }

    /// Open the File Info overlay on the active pane's cursor entry, reading its
    /// extended attributes and the image's uid/gid name map. Best-effort: an
    /// entry with no POSIX metadata still opens, just with fewer lines.
    fn open_info(&mut self) {
        let side = self.active;
        let pane = self.pane_mut(side);
        let Some(entry) = pane.selected() else {
            self.status = Some("Nothing selected.".to_string());
            self.is_error = true;
            return;
        };
        // Read xattrs + the account maps through the pane's filesystem when it
        // has one (a host-folder pane has none; the overlay still shows size and
        // name).
        let (xattrs, id_names) = match pane.session.clone() {
            Some(session) => with_stderr_suppressed(|| {
                let mut fs = match session.open() {
                    Ok(fs) => fs,
                    Err(_) => return (Vec::new(), Default::default()),
                };
                let x = if fs.supports_xattrs() {
                    fs.list_xattrs(&entry).unwrap_or_default()
                } else {
                    Vec::new()
                };
                let names = crate::fs::id_names::IdNameMap::from_filesystem(&mut *fs);
                (x, names)
            }),
            None => (Vec::new(), Default::default()),
        };
        self.info = Some(InfoView {
            entry,
            xattrs,
            id_names,
        });
    }

    /// The rendered lines of the info overlay.
    fn info_lines(&self) -> Vec<String> {
        let Some(info) = &self.info else {
            return Vec::new();
        };
        let e = &info.entry;
        let mut out = vec![
            format!("Name:  {}", e.name),
            format!("Path:  {}", e.path),
            format!("Size:  {}", e.size),
        ];
        if let Some(m) = e.mode {
            out.push(format!(
                "Mode:  {}  ({:04o})",
                e.mode_string().unwrap_or_default(),
                m & 0o7777
            ));
        }
        if let (Some(u), Some(g)) = (e.uid, e.gid) {
            out.push(format!("Owner: {}", info.id_names.format_owner(u, g)));
        }
        if let Some(t) = &e.symlink_target {
            out.push(format!("Link:  -> {t}"));
        }
        if let Some(s) = &e.special_type {
            out.push(format!("Type:  {s}"));
        }
        if let Some(m) = &e.modified {
            out.push(format!("Mtime: {m}"));
        }
        if !info.xattrs.is_empty() {
            out.push(String::new());
            out.push("Extended attributes:".to_string());
            for x in &info.xattrs {
                out.push(format!("  {} = {}", x.name, x.value_display()));
            }
        }
        out
    }

    /// Apply the attribute edit typed in the info overlay's prompt.
    fn apply_attr_edit(&mut self, field: AttrField, text: &str) {
        let side = self.active;
        let Some(info) = &self.info else { return };
        let entry = info.entry.clone();
        let Some(session) = self.pane_mut(side).session.clone() else {
            self.set_result(Err("Pane has no write session.".to_string()));
            return;
        };
        let text = text.trim();

        let edit = match field {
            AttrField::Mode => match u32::from_str_radix(text, 8) {
                Ok(m) if m <= 0o7777 => crate::model::edit_queue::StagedEdit::SetPermissions {
                    entry: entry.clone(),
                    // The drivers take the complete mode; keep the entry's type
                    // bits and replace only the permission bits.
                    mode: (entry.mode.unwrap_or(0) & !0o7777) | m,
                },
                _ => {
                    self.set_result(Err(format!(
                        "Bad octal mode {text:?} (1-4 digits, <= 7777)"
                    )));
                    return;
                }
            },
            AttrField::Owner => {
                let parsed = text
                    .split_once(':')
                    .and_then(|(u, g)| Some((u.trim().parse().ok()?, g.trim().parse().ok()?)));
                match parsed {
                    Some((uid, gid)) => crate::model::edit_queue::StagedEdit::SetOwner {
                        entry: entry.clone(),
                        uid,
                        gid,
                    },
                    None => {
                        self.set_result(Err(format!("Bad owner {text:?} (want uid:gid)")));
                        return;
                    }
                }
            }
            AttrField::Xattr => {
                let (name, value) = match text.split_once('=') {
                    Some((n, v)) => (n.trim().to_string(), v),
                    None => {
                        self.set_result(Err("Want name=value (empty value deletes)".to_string()));
                        return;
                    }
                };
                if !crate::fs::xattr::has_valid_namespace(&name) {
                    self.set_result(Err(format!(
                        "Xattr {name:?} needs a user. / trusted. / security. / system. prefix"
                    )));
                    return;
                }
                if value.trim().is_empty() {
                    crate::model::edit_queue::StagedEdit::RemoveXattr {
                        entry: entry.clone(),
                        name,
                    }
                } else {
                    match crate::fs::xattr::parse_value(value) {
                        Ok(v) => crate::model::edit_queue::StagedEdit::SetXattr {
                            entry: entry.clone(),
                            name,
                            value: v,
                        },
                        Err(e) => {
                            self.set_result(Err(e));
                            return;
                        }
                    }
                }
            }
        };

        let result = with_stderr_suppressed(|| {
            crate::model::commander_ops::apply_edits(&session, std::slice::from_ref(&edit))
        })
        .map(|()| "Attribute updated.".to_string())
        .map_err(|e| format!("Attribute edit failed: {e:#}"));
        let ok = result.is_ok();
        self.set_result(result);
        if ok {
            cmd_refresh(self.pane_mut(side));
            // Re-read so the overlay reflects what actually landed.
            self.open_info();
        }
    }

    /// Delete the active pane's selected entry (host: `remove_file` /
    /// `remove_dir_all`; image: a `DeleteRecursive` staged edit applied through
    /// the pane's browse session), then refresh the pane.
    fn delete_selected(&mut self) {
        let side = self.active;
        let pane = self.pane_mut(side);
        let entries = pane.action_entries();
        if entries.is_empty() {
            self.status = Some("Select a file or folder to delete.".to_string());
            self.is_error = true;
            return;
        }
        let label = if entries.len() == 1 {
            entries[0].name.clone()
        } else {
            format!("{} items", entries.len())
        };
        let result: Result<String, String> = if pane.is_host {
            (|| -> Result<(), String> {
                for e in &entries {
                    let path = std::path::PathBuf::from(&e.path);
                    let r = if matches!(e.entry_type, EntryType::Directory) {
                        std::fs::remove_dir_all(&path)
                    } else {
                        std::fs::remove_file(&path)
                    };
                    r.map_err(|err| format!("Delete failed on {}: {err}", e.name))?;
                }
                Ok(())
            })()
            .map(|()| format!("Deleted {label}."))
        } else {
            let parent = match pane.listing.cwd() {
                Some(e) => e.clone(),
                None => {
                    self.status = Some("No current directory.".to_string());
                    self.is_error = true;
                    return;
                }
            };
            let session = match pane.session.clone() {
                Some(s) => s,
                None => {
                    self.status = Some("Pane has no write session.".to_string());
                    self.is_error = true;
                    return;
                }
            };
            let edits: Vec<_> = entries
                .iter()
                .map(|e| crate::model::edit_queue::StagedEdit::DeleteRecursive {
                    parent: parent.clone(),
                    entry: e.clone(),
                })
                .collect();
            with_stderr_suppressed(|| crate::model::commander_ops::apply_edits(&session, &edits))
                .map(|()| format!("Deleted {label}."))
                .map_err(|e| format!("Delete failed: {e:#}"))
        };
        if result.is_ok() {
            let p = self.pane_mut(side);
            p.listing.clear_selection();
            cmd_refresh(p);
            p.sel = p.sel.min(p.rows_len().saturating_sub(1));
        }
        self.set_result(result);
    }
}

/// Load a host folder into a Commander pane.
fn cmd_load_host(pane: &mut CmdPane, path: std::path::PathBuf) -> Result<(), String> {
    pane.listing
        .load_host_root(path.clone())
        .map_err(|e| format!("{e}"))?;
    pane.session = None;
    pane.is_host = true;
    pane.loaded = true;
    pane.label = format!("host: {}", path.display());
    pane.sel = 0;
    pane.optical = None;
    pane.optical_pick = false;
    pane.part_pick = false;
    pane.parts.clear();
    Ok(())
}

/// Build a `BrowseSession` for a partition (or the whole disk when `part` is
/// `None`, i.e. a superfloppy).
fn cmd_build_session(
    path: &std::path::Path,
    part: Option<&crate::partition::PartitionInfo>,
) -> crate::model::browse_session::BrowseSession {
    match part {
        Some(p) => crate::model::commander_source::session_for(path, p),
        None => crate::model::browse_session::BrowseSession {
            source_path: Some(path.to_path_buf()),
            partition_offset: 0,
            partition_type: 0,
            ..Default::default()
        },
    }
}

/// Open an image partition into a Commander pane and list its root.
fn cmd_load_image(
    pane: &mut CmdPane,
    path: &std::path::Path,
    part: Option<&crate::partition::PartitionInfo>,
) -> Result<(), String> {
    let session = cmd_build_session(path, part);
    let mut fs = session
        .open()
        .map_err(|e| format!("opening filesystem: {e}"))?;
    let root = fs.root().map_err(|e| format!("reading root: {e}"))?;
    let entries = fs.list_directory(&root).unwrap_or_default();
    pane.listing.load_root(fs, root, entries, false);
    pane.session = Some(session);
    pane.is_host = false;
    pane.loaded = true;
    pane.label = format!("img: {}", basename(&path.to_string_lossy()));
    pane.sel = 0;
    pane.part_pick = false;
    pane.parts.clear();
    pane.pending_img = None;
    pane.optical = None;
    pane.optical_pick = false;
    Ok(())
}

/// Build a reopen-able [`StageSource`](crate::model::commander_ops::StageSource)
/// for a non-host pane, so a checksum worker can read its files off a fresh
/// handle. A session image reopens by session; an optical pane reopens the
/// selected filesystem (primary / hybrid). `None` for a host pane or an
/// unopened source.
fn cmd_checksum_source(pane: &CmdPane) -> Option<crate::model::commander_ops::StageSource> {
    use crate::model::commander_ops::StageSource;
    if let Some(op) = &pane.optical {
        let label = op
            .path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned());
        let recipe = match op.choices.get(op.sel).and_then(|c| c.hybrid_index) {
            Some(index) => crate::model::commander_descend::ReopenRecipe::OpticalHybrid {
                path: op.path.clone(),
                index,
                label,
            },
            None => crate::model::commander_descend::ReopenRecipe::Optical {
                path: op.path.clone(),
                label,
            },
        };
        return Some(StageSource::Reopen(recipe));
    }
    pane.session.clone().map(StageSource::Session)
}

/// True when `path` names an optical disc image (by extension) we can browse via
/// `opticaldiscs` — the discriminator for routing to [`cmd_load_optical`]
/// instead of the partition-table path.
fn is_optical_image_path(path: &std::path::Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .and_then(crate::model::commander_descend::classify)
        == Some(crate::model::commander_descend::DescendKind::Optical)
}

/// Open the filesystem at `choices[idx]` of the optical disc image at `path`
/// (the primary ISO 9660, or a hybrid Mac HFS/HFS+ side) into a Commander pane
/// and list its root. Read-only and session-less — like a Mac archive.
fn cmd_load_optical(
    pane: &mut CmdPane,
    path: &std::path::Path,
    choices: Vec<crate::model::commander_descend::OpticalFsChoice>,
    idx: usize,
) -> Result<(), String> {
    let choice = choices
        .get(idx)
        .cloned()
        .ok_or_else(|| "no such filesystem on this disc".to_string())?;
    let label = path.file_name().map(|n| n.to_string_lossy().into_owned());
    let mut fs = match choice.hybrid_index {
        None => crate::model::commander_descend::open_optical(path, label),
        Some(h) => crate::model::commander_descend::open_optical_hybrid(path, h, label),
    }
    .map_err(|e| format!("{e:#}"))?;
    let root = fs.root().map_err(|e| format!("reading root: {e}"))?;
    let entries = fs.list_directory(&root).unwrap_or_default();
    pane.listing.load_root(fs, root, entries, false);
    pane.session = None;
    pane.is_host = false;
    pane.loaded = true;
    pane.label = format!(
        "cd: {} [{}]",
        basename(&path.to_string_lossy()),
        choice.label
    );
    pane.sel = 0;
    pane.part_pick = false;
    pane.parts.clear();
    pane.pending_img = None;
    pane.optical_pick = false;
    pane.optical_sel = idx;
    pane.optical = Some(OpticalPane {
        path: path.to_path_buf(),
        choices,
        sel: idx,
    });
    Ok(())
}

/// Refresh a pane after a write: host panes reload; image panes reopen from the
/// session and re-navigate to the same directory.
fn cmd_refresh(pane: &mut CmdPane) {
    let cwd_path = pane.listing.cwd().map(|e| e.path.clone());
    #[cfg(feature = "remote")]
    if pane.is_remote() {
        // A remote pane reads over the wire through the boxed filesystem in its
        // listing; a plain reload re-lists the current directory (no session).
        let _ = pane.listing.reload();
        return;
    }
    if pane.is_host {
        let _ = pane.listing.reload();
    } else if let Some(op) = pane.optical.clone() {
        // Optical panes have no `BrowseSession`; reopen the selected filesystem
        // (primary or hybrid) fresh from the disc image.
        let label = op
            .path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned());
        let reopened = op.choices.get(op.sel).and_then(|c| match c.hybrid_index {
            None => crate::model::commander_descend::open_optical(&op.path, label).ok(),
            Some(h) => {
                crate::model::commander_descend::open_optical_hybrid(&op.path, h, label).ok()
            }
        });
        if let Some(mut fs) = reopened {
            if let Ok(root) = fs.root() {
                let entries = fs.list_directory(&root).unwrap_or_default();
                pane.listing.load_root(fs, root, entries, false);
                if let Some(p) = cwd_path {
                    let _ = pane.listing.navigate_to(&p);
                }
            }
        }
    } else if let Some(session) = pane.session.clone() {
        if let Ok(mut fs) = session.open() {
            if let Ok(root) = fs.root() {
                let entries = fs.list_directory(&root).unwrap_or_default();
                pane.listing.load_root(fs, root, entries, false);
                if let Some(p) = cwd_path {
                    let _ = pane.listing.navigate_to(&p);
                }
            }
        }
    }
    let n = pane.listing.current_rows().len();
    if pane.sel >= n {
        pane.sel = n.saturating_sub(1);
    }
}

/// Enter the row under a pane's cursor (`..` = up, directory = descend).
fn cmd_enter(pane: &mut CmdPane) -> Option<Result<String, String>> {
    enum Act {
        Up,
        Enter(String),
        #[cfg(feature = "remote")]
        OpenRemoteImage(String, String),
        None,
    }
    let act = match pane.listing.current_rows().get(pane.sel) {
        Some(crate::model::dir_listing::Row::Parent) => Act::Up,
        Some(crate::model::dir_listing::Row::Entry(e)) if e.is_directory() => {
            Act::Enter(e.name.clone())
        }
        // On a remote *host* pane, Enter on a file opens it as a disk image on
        // the same daemon connection (no reconnect) — the TUI half of "browse
        // remote disk images".
        #[cfg(feature = "remote")]
        Some(crate::model::dir_listing::Row::Entry(e)) if e.is_file() && pane.is_remote_host() => {
            Act::OpenRemoteImage(e.path.clone(), e.name.clone())
        }
        _ => Act::None,
    };
    match act {
        Act::Up => {
            pane.listing.up();
            pane.sel = 0;
            None
        }
        Act::Enter(n) => {
            let _ = pane.listing.enter(&n);
            pane.sel = 0;
            None
        }
        #[cfg(feature = "remote")]
        Act::OpenRemoteImage(path, name) => Some(cmd_open_remote_image(pane, &path, &name)),
        Act::None => None,
    }
}

/// Open the remote host file at `path` as a disk image on the pane's existing
/// daemon connection, replacing the pane's listing with the image's root.
#[cfg(feature = "remote")]
fn cmd_open_remote_image(pane: &mut CmdPane, path: &str, name: &str) -> Result<String, String> {
    let cwd = pane
        .listing
        .cwd()
        .map(|e| e.path.clone())
        .unwrap_or_else(|| "/".to_string());
    let browser = pane
        .remote
        .as_mut()
        .ok_or_else(|| "the remote connection is unavailable".to_string())?;
    let target = browser
        .open_image(path, None, &cwd)
        .map_err(|e| format!("{name} is not a disk image we can open: {e:#}"))?;
    let label = format!("remote img: {name} [{}]", target.fs_type);
    pane.listing
        .load_root(target.fs, target.root, target.entries, false);
    pane.label = label;
    pane.sel = 0;
    Ok(format!("Opened {name} on the remote."))
}

/// Step a remote-image pane back out to the daemon's host file browser (the TUI
/// analog of the GUI's "close image"). Returns true when it handled the step
/// (the pane was a remote image at its root); the caller falls back to a normal
/// `up()` otherwise.
#[cfg(feature = "remote")]
fn cmd_remote_up(pane: &mut CmdPane) -> bool {
    if !pane.is_remote() || pane.is_remote_host() || !pane.listing.at_root() {
        return false;
    }
    if let Some(browser) = pane.remote.as_mut() {
        if let Ok(target) = browser.close_image() {
            let addr = browser.addr().to_string();
            pane.listing
                .load_root(target.fs, target.root, target.entries, false);
            pane.label = format!("remote {addr}: /");
            pane.sel = 0;
            return true;
        }
    }
    false
}

/// Fork container formats for archive extraction.
const ARCHIVE_FORK_FORMATS: &[(&str, crate::macarchive::extract::ForkFormat)] = &[
    (
        "BinHex (.hqx)",
        crate::macarchive::extract::ForkFormat::BinHex,
    ),
    (
        "MacBinary (.bin)",
        crate::macarchive::extract::ForkFormat::MacBinary,
    ),
    (
        "AppleDouble",
        crate::macarchive::extract::ForkFormat::AppleDouble,
    ),
    ("Raw (.rsrc)", crate::macarchive::extract::ForkFormat::Raw),
];

/// The Archives screen: open a classic Mac archive (`.sit` / `.sea` / `.cpt` /
/// `.mar` / `.hqx`), list its entries, and extract to the host preserving forks.
/// Extraction is synchronous (archives are small) via `macarchive::extract`.
#[derive(Default)]
struct ArchiveState {
    picker: Option<FilePicker>,
    /// When true, a picker confirm chooses the extract destination folder.
    picker_dest: bool,
    archive_path: String,
    bytes: Vec<u8>,
    archive: Option<crate::macarchive::stuffit::StuffItArchive>,
    /// Pre-rendered entry lines for display.
    entries: Vec<String>,
    list_sel: usize,
    fork_fmt_sel: usize,
    status: Option<String>,
    is_error: bool,
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
    detail: Option<usize>,
    /// An image/backup opened from the Inspect tab (takes over the Inspect body).
    opened: Option<Opened>,
    /// The shared "Open file / backup" picker (path + recent + Tab-browse).
    open_picker: Option<FilePicker>,
    /// The per-partition filesystem Explorer overlay, when open.
    explorer: Option<Explorer>,
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
    commander: Option<CommanderState>,
    /// The Settings tab state (lazily loaded on first visit).
    settings: Option<SettingsState>,
    /// The `:` command-palette input line being typed, if open.
    palette_input: Option<String>,
    /// A command-palette line ready to run (the run loop suspends the TUI,
    /// executes it, and re-enters).
    pending_palette: Option<String>,
    show_help: bool,
    should_quit: bool,
}

impl App {
    fn new_on(initial_tab: usize) -> Self {
        let active = initial_tab.min(TABS.len() - 1);
        let mut app = App {
            palette: Palette::detect(),
            border: choose_border_set(),
            elevated: crate::os::is_elevated(),
            active,
            scroll: 0,
            selection: 0,
            detail: None,
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
            settings: None,
            palette_input: None,
            pending_palette: None,
            show_help: false,
            should_quit: false,
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
        if !event::poll(Duration::from_millis(200))? {
            return Ok(());
        }
        let Event::Key(key) = event::read()? else {
            return Ok(());
        };
        if key.kind != KeyEventKind::Press {
            return Ok(());
        }

        if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
            self.should_quit = true;
            return Ok(());
        }

        // The help overlay is modal.
        if self.show_help {
            if matches!(
                key.code,
                KeyCode::Esc | KeyCode::Char('?') | KeyCode::Enter | KeyCode::F(1)
            ) {
                self.show_help = false;
            }
            return Ok(());
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
            return Ok(());
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
            return Ok(());
        }

        // The filesystem Explorer is a modal window over the Inspect tab.
        if self.explorer.is_some() {
            self.handle_explorer_key(key.code);
            return Ok(());
        }

        // The New Disk wizard owns most keys on its tab (Left/Right still switch
        // tabs; typed text in the details form is consumed here).
        if self.current() == TabId::NewDisk && self.handle_newdisk_key(key.code) {
            return Ok(());
        }

        // The Backup screen owns most keys on its tab.
        if self.current() == TabId::Backup && self.handle_backup_key(key.code) {
            return Ok(());
        }

        // The Restore screen owns most keys on its tab.
        if self.current() == TabId::Restore && self.handle_restore_key(key.code) {
            return Ok(());
        }

        // The Bulk convert screen owns most keys on its tab.
        if self.current() == TabId::Bulk && self.handle_bulk_key(key.code) {
            return Ok(());
        }

        // The Optical screen owns most keys on its tab.
        #[cfg(feature = "optical")]
        if self.current() == TabId::Optical && self.handle_optical_key(key.code) {
            return Ok(());
        }

        // The Archives screen owns most keys on its tab.
        if self.current() == TabId::Archives && self.handle_archive_key(key.code) {
            return Ok(());
        }

        // The Commander screen owns most keys on its tab.
        if self.current() == TabId::Commander && self.handle_commander_key(key.code) {
            return Ok(());
        }

        // The Settings tab owns Up/Down/Enter (Left/Right still switch tabs).
        if self.current() == TabId::Settings && self.handle_settings_key(key.code) {
            return Ok(());
        }

        match key.code {
            KeyCode::Char('?') | KeyCode::F(1) => self.show_help = true,
            KeyCode::Char(':') => self.palette_input = Some(String::new()),
            KeyCode::Char('q') => self.should_quit = true,
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
                self.detail = None;
            }
            _ => {}
        }
        Ok(())
    }

    /// targets. A directory is routed through the shared `backup_loader`
    /// (rusty-backup folder or Clonezilla image); a file is a disk image. On
    /// success the path is recorded in the MRU (move-to-front).
    fn open_target(&mut self, path: std::path::PathBuf) {
        self.detail = None;
        self.selection = 0;
        self.status = None;
        if path.is_dir() {
            match crate::model::backup_loader::load_backup(&path) {
                Ok(outcome) => {
                    use crate::model::backup_loader::LoadOutcome;
                    let display = path.display().to_string();
                    crate::update::push_recent(crate::update::RecentMode::Inspect, &display);
                    let (kind, partitions, info) = match outcome {
                        LoadOutcome::Backup(b) => {
                            ("rusty-backup".to_string(), b.partitions.len(), b.info)
                        }
                        LoadOutcome::Clonezilla(c) => {
                            ("Clonezilla".to_string(), c.partitions.len(), c.info)
                        }
                    };
                    self.opened = Some(Opened::Backup {
                        path: display,
                        kind,
                        partitions,
                        info,
                    });
                }
                Err(e) => self.status = Some(format!("Cannot open: {e}")),
            }
        } else if path.is_file() {
            match std::fs::metadata(&path) {
                Ok(m) => {
                    let display = path.display().to_string();
                    crate::update::push_recent(crate::update::RecentMode::Inspect, &display);
                    let parts = parse_partitions(&path, m.len());
                    self.opened = Some(Opened::Image {
                        path: display,
                        size: m.len(),
                        parts,
                    });
                }
                Err(e) => self.status = Some(format!("{}: {e}", path.display())),
            }
        } else {
            self.status = Some(format!("No such file or directory: {}", path.display()));
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
                        .map(|r| (path.clone(), r.selector, r.label.clone()))
                } else {
                    None
                };
                if let Some((path, selector, label)) = target {
                    self.open_explorer(&path, selector, label);
                }
            }
            // Disk list: Enter a disk → its detail view.
            TabId::Inspect if self.detail.is_none() && self.row_count() > 0 => {
                self.detail = Some(self.selection);
            }
            _ => {}
        }
    }

    /// Open the per-partition filesystem Explorer, reusing the shared
    /// `resolve` + `open_filesystem` path (the same core `ls`/`get` use).
    fn open_explorer(&mut self, image_path: &str, selector: Option<u32>, part_label: String) {
        use crate::cli::resolve::resolve_partition_streaming_forced_inside;
        let path = std::path::Path::new(image_path);
        let built = (|| -> anyhow::Result<Explorer> {
            let (reader, ctx) =
                resolve_partition_streaming_forced_inside(path, selector, None, None, None)?;
            let mut fs = crate::fs::open_filesystem_with_passphrase(
                reader,
                ctx.offset,
                ctx.type_byte,
                ctx.type_string.as_deref(),
                None,
            )
            .map_err(|e| anyhow::anyhow!("opening filesystem: {e}"))?;
            let volume = fs.volume_label().map(|s| s.to_string());
            let blessed = fs.blessed_system_folder();
            let root = fs
                .root()
                .map_err(|e| anyhow::anyhow!("reading root: {e}"))?;
            let mut list = fs
                .list_directory(&root)
                .map_err(|e| anyhow::anyhow!("listing root: {e}"))?;
            sort_entries(&mut list);
            let mut ex = Explorer {
                fs,
                image_path: image_path.to_string(),
                selector,
                part_label,
                volume,
                blessed,
                tree: vec![TreeNode {
                    dir: root,
                    depth: 0,
                    expanded: false,
                }],
                tree_sel: 0,
                list,
                list_sel: 0,
                marked: std::collections::BTreeMap::new(),
                focus: ExFocus::Tree,
                status: None,
                picker: None,
                export_menu: None,
                export_fmt: ExportFormat::Fork(RfMode::AppleDouble),
                preview: None,
                metadata: None,
                confirm_close: false,
                confirm_bless: None,
                mkdir_input: None,
                confirm_delete: None,
                fsck_report: None,
                transform_menu: None,
            };
            // Expand the root so its subdirectories show in the tree immediately.
            ex.tree_expand();
            Ok(ex)
        })();
        match built {
            Ok(ex) => {
                self.explorer = Some(ex);
                self.status = None;
            }
            Err(e) => {
                // An optical `.iso` (incl. NKit-scrubbed GC/Wii) has no MBR/GPT,
                // so it fails here with a cryptic parse error. Give NKit images
                // the convert-it-first hint and other optical images a redirect
                // to the Commander's optical open / the `optical` verbs.
                let hinted = if crate::cli::optical_hint::is_nkit_image(path) {
                    crate::cli::optical_hint::with_nkit_hint(e, path)
                } else {
                    crate::cli::optical_hint::with_optical_hint(e, path)
                };
                self.status = Some(format!("Cannot browse: {hinted:#}"));
            }
        }
    }

    /// Rows the Explorer shows at once — the PageUp/PageDown step.
    fn explorer_page(&self) -> usize {
        let rows = crossterm::terminal::size().map(|(_, r)| r).unwrap_or(24);
        (rows.saturating_sub(6) as usize).max(1)
    }

    /// Keys while the Explorer overlay is open (modal). A path prompt (export /
    /// import) and the close confirmation are sub-modes that take priority.
    fn handle_explorer_key(&mut self, code: KeyCode) {
        // fsck / repair report overlay (modal): scroll, Esc/q/Enter to close.
        if self
            .explorer
            .as_ref()
            .map(|e| e.fsck_report.is_some())
            .unwrap_or(false)
        {
            if matches!(code, KeyCode::Esc | KeyCode::Char('q') | KeyCode::Enter) {
                if let Some(ex) = self.explorer.as_mut() {
                    ex.fsck_report = None;
                }
                return;
            }
            let page = self.explorer_page();
            if let Some(rv) = self.explorer.as_mut().and_then(|e| e.fsck_report.as_mut()) {
                let max = rv.lines.len().saturating_sub(1);
                match code {
                    KeyCode::Up | KeyCode::Char('k') => rv.scroll = rv.scroll.saturating_sub(1),
                    KeyCode::Down | KeyCode::Char('j') => rv.scroll = (rv.scroll + 1).min(max),
                    KeyCode::PageUp => rv.scroll = rv.scroll.saturating_sub(page),
                    KeyCode::PageDown => rv.scroll = (rv.scroll + page).min(max),
                    KeyCode::Home => rv.scroll = 0,
                    KeyCode::End => rv.scroll = max,
                    _ => {}
                }
            }
            return;
        }

        // Transform launcher menu (modal): pick a verb to prefill the palette.
        if self
            .explorer
            .as_ref()
            .map(|e| e.transform_menu.is_some())
            .unwrap_or(false)
        {
            let sel = self
                .explorer
                .as_ref()
                .and_then(|e| e.transform_menu)
                .unwrap_or(0);
            match code {
                KeyCode::Esc => {
                    if let Some(ex) = self.explorer.as_mut() {
                        ex.transform_menu = None;
                    }
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    if let Some(ex) = self.explorer.as_mut() {
                        ex.transform_menu = Some(sel.saturating_sub(1));
                    }
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    if let Some(ex) = self.explorer.as_mut() {
                        ex.transform_menu = Some((sel + 1).min(TRANSFORMS.len() - 1));
                    }
                }
                KeyCode::Enter => self.launch_transform(sel),
                _ => {}
            }
            return;
        }

        // File preview overlay (modal): scroll, Esc/q to close.
        if self
            .explorer
            .as_ref()
            .map(|e| e.preview.is_some())
            .unwrap_or(false)
        {
            if matches!(code, KeyCode::Esc | KeyCode::Char('q')) {
                if let Some(ex) = self.explorer.as_mut() {
                    ex.preview = None;
                }
                return;
            }
            let page = self.explorer_page();
            if let Some(pv) = self.explorer.as_mut().and_then(|e| e.preview.as_mut()) {
                let max = pv.lines().len().saturating_sub(1);
                match code {
                    KeyCode::Up | KeyCode::Char('k') => pv.scroll = pv.scroll.saturating_sub(1),
                    KeyCode::Down | KeyCode::Char('j') => pv.scroll = (pv.scroll + 1).min(max),
                    KeyCode::PageUp => pv.scroll = pv.scroll.saturating_sub(page),
                    KeyCode::PageDown => pv.scroll = (pv.scroll + page).min(max),
                    KeyCode::Home => pv.scroll = 0,
                    KeyCode::End => pv.scroll = max,
                    // Toggle data / resource fork (files that have one).
                    KeyCode::Char('r') if pv.rsrc.is_some() => {
                        pv.showing_rsrc = !pv.showing_rsrc;
                        pv.scroll = 0;
                    }
                    _ => {}
                }
            }
            return;
        }

        // Metadata editor (modal): edit fields, Tab/Up/Down switch, Enter apply.
        if self
            .explorer
            .as_ref()
            .map(|e| e.metadata.is_some())
            .unwrap_or(false)
        {
            match code {
                KeyCode::Esc => {
                    if let Some(ex) = self.explorer.as_mut() {
                        ex.metadata = None;
                    }
                }
                KeyCode::Enter => self.apply_metadata(),
                _ => {
                    if let Some(m) = self.explorer.as_mut().and_then(|e| e.metadata.as_mut()) {
                        match code {
                            KeyCode::Tab | KeyCode::Down => m.field = (m.field + 1) % 3,
                            KeyCode::BackTab | KeyCode::Up => m.field = (m.field + 2) % 3,
                            KeyCode::Backspace => {
                                m.error = None;
                                match m.field {
                                    0 => m.type_code.pop(),
                                    1 => m.creator.pop(),
                                    _ => m.modified.pop(),
                                };
                            }
                            KeyCode::Char(c) if !c.is_control() => {
                                m.error = None;
                                match m.field {
                                    0 => m.type_code.push(c),
                                    1 => m.creator.push(c),
                                    _ => m.modified.push(c),
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
            return;
        }

        // "Export as" format menu → choose a format, then open the Dir picker.
        if self
            .explorer
            .as_ref()
            .map(|e| e.export_menu.is_some())
            .unwrap_or(false)
        {
            if let Some(ex) = self.explorer.as_mut() {
                let sel = ex.export_menu.unwrap_or(0);
                match code {
                    KeyCode::Esc => ex.export_menu = None,
                    KeyCode::Up | KeyCode::Char('k') => {
                        ex.export_menu = Some(sel.saturating_sub(1))
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        ex.export_menu = Some((sel + 1).min(EXPORT_FORMATS.len() - 1))
                    }
                    KeyCode::Enter => {
                        ex.export_fmt = EXPORT_FORMATS[sel].1;
                        ex.export_menu = None;
                        ex.picker = Some(FilePicker::new(PickKind::Dir, "Export to directory"));
                    }
                    _ => {}
                }
            }
            return;
        }

        // Export / import file picker (shared FilePicker: path + Tab-browse).
        if self
            .explorer
            .as_ref()
            .map(|e| e.picker.is_some())
            .unwrap_or(false)
        {
            let action = self
                .explorer
                .as_mut()
                .and_then(|e| e.picker.as_mut())
                .and_then(|p| p.handle_key(code));
            match action {
                Some(PickResult::Cancel) => {
                    if let Some(ex) = self.explorer.as_mut() {
                        ex.picker = None;
                    }
                }
                Some(PickResult::Confirm(path)) => {
                    // Dir-kind picker = export destination; File-kind = import source.
                    let import = self
                        .explorer
                        .as_ref()
                        .and_then(|e| e.picker.as_ref())
                        .map(|p| p.kind == PickKind::File)
                        .unwrap_or(false);
                    if let Some(ex) = self.explorer.as_mut() {
                        ex.picker = None;
                    }
                    if import {
                        self.explorer_import(&path);
                    } else {
                        self.explorer_export(&path);
                    }
                }
                None => {}
            }
            return;
        }

        // Close confirmation.
        let confirming = self
            .explorer
            .as_ref()
            .map(|e| e.confirm_close)
            .unwrap_or(false);
        if confirming {
            match code {
                KeyCode::Char('y') | KeyCode::Char('Y') | KeyCode::Enter => self.explorer = None,
                KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => {
                    if let Some(ex) = self.explorer.as_mut() {
                        ex.confirm_close = false;
                    }
                }
                _ => {}
            }
            return;
        }

        // Bless confirmation.
        let bless_pending = self
            .explorer
            .as_ref()
            .map(|e| e.confirm_bless.is_some())
            .unwrap_or(false);
        if bless_pending {
            match code {
                KeyCode::Char('y') | KeyCode::Char('Y') | KeyCode::Enter => self.bless_confirmed(),
                KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => {
                    if let Some(ex) = self.explorer.as_mut() {
                        ex.confirm_bless = None;
                    }
                }
                _ => {}
            }
            return;
        }

        // New-folder name prompt.
        let mkdir_pending = self
            .explorer
            .as_ref()
            .map(|e| e.mkdir_input.is_some())
            .unwrap_or(false);
        if mkdir_pending {
            match code {
                KeyCode::Enter => self.do_mkdir(),
                KeyCode::Esc => {
                    if let Some(ex) = self.explorer.as_mut() {
                        ex.mkdir_input = None;
                    }
                }
                KeyCode::Backspace => {
                    if let Some(s) = self.explorer.as_mut().and_then(|e| e.mkdir_input.as_mut()) {
                        s.pop();
                    }
                }
                KeyCode::Char(c) if !c.is_control() => {
                    if let Some(s) = self.explorer.as_mut().and_then(|e| e.mkdir_input.as_mut()) {
                        s.push(c);
                    }
                }
                _ => {}
            }
            return;
        }

        // Delete confirmation.
        let delete_pending = self
            .explorer
            .as_ref()
            .map(|e| e.confirm_delete.is_some())
            .unwrap_or(false);
        if delete_pending {
            match code {
                KeyCode::Char('y') | KeyCode::Char('Y') | KeyCode::Enter => self.do_delete(),
                KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => {
                    if let Some(ex) = self.explorer.as_mut() {
                        ex.confirm_delete = None;
                    }
                }
                _ => {}
            }
            return;
        }

        // `e` (export) / `i` (import) start a path prompt.
        match code {
            KeyCode::Char('e') => {
                if let Some(ex) = self.explorer.as_mut() {
                    if ex.selected_entry().is_some() {
                        ex.export_menu = Some(0);
                        ex.status = None;
                    } else {
                        ex.status = Some("Nothing selected to export.".to_string());
                    }
                }
                return;
            }
            KeyCode::Char('i') => {
                if let Some(ex) = self.explorer.as_mut() {
                    ex.picker = Some(FilePicker::new(PickKind::File, "Import host file"));
                    ex.status = None;
                }
                return;
            }
            // Edit metadata (HFS/HFS+ type/creator + modified date).
            KeyCode::Char('m') => {
                if let Some(ex) = self.explorer.as_mut() {
                    match ex.selected_entry() {
                        Some(e)
                            if matches!(e.entry_type, EntryType::File)
                                && (e.type_code.is_some() || e.mac_dates.is_some()) =>
                        {
                            ex.metadata = Some(MetaEdit {
                                entry_name: e.name.clone(),
                                type_code: code_to_string(e.type_code),
                                creator: code_to_string(e.creator_code),
                                modified: e
                                    .mac_dates
                                    .map(|d| format_mac_date(d.1))
                                    .unwrap_or_default(),
                                field: 0,
                                error: None,
                            });
                            ex.status = None;
                        }
                        _ => {
                            ex.status = Some("Metadata editing is for HFS/HFS+ files.".to_string())
                        }
                    }
                }
                return;
            }
            // Bless the selected folder as the bootable System Folder (HFS/HFS+):
            // stage a confirmation prompt (the actual bless happens on `y`).
            KeyCode::Char('b') => {
                if let Some(ex) = self.explorer.as_mut() {
                    match ex.bless_target() {
                        Some(target) => {
                            ex.confirm_bless = Some(target);
                            ex.status = None;
                        }
                        None => {
                            ex.status =
                                Some("Select a folder to bless as the System Folder.".to_string());
                        }
                    }
                }
                return;
            }
            // New folder in the current directory.
            KeyCode::Char('n') => {
                if let Some(ex) = self.explorer.as_mut() {
                    ex.mkdir_input = Some(String::new());
                    ex.status = None;
                }
                return;
            }
            // Check (fsck) this partition's filesystem.
            KeyCode::Char('f') => {
                self.explorer_fsck();
                return;
            }
            // Repair this partition's filesystem in place (with confirmation).
            KeyCode::Char('F') => {
                self.explorer_repair();
                return;
            }
            // Whole-image transform launcher (convert / resize / expand / partmap).
            KeyCode::Char('t') => {
                if let Some(ex) = self.explorer.as_mut() {
                    ex.transform_menu = Some(0);
                    ex.status = None;
                }
                return;
            }
            // Delete the selected entry (with confirmation).
            KeyCode::Char('x') | KeyCode::Delete => {
                if let Some(ex) = self.explorer.as_mut() {
                    match ex.selected_entry() {
                        Some(e) => {
                            ex.confirm_delete = Some((
                                e.name.clone(),
                                matches!(e.entry_type, EntryType::Directory),
                            ));
                            ex.status = None;
                        }
                        None => ex.status = Some("Nothing selected to delete.".to_string()),
                    }
                }
                return;
            }
            _ => {}
        }

        let page = self.explorer_page() as isize;
        let Some(ex) = self.explorer.as_mut() else {
            return;
        };
        match code {
            KeyCode::Esc => ex.confirm_close = true,
            KeyCode::Tab | KeyCode::BackTab => ex.toggle_focus(),
            _ => match ex.focus {
                // Left pane: navigate + expand/collapse the directory tree.
                ExFocus::Tree => match code {
                    KeyCode::Up | KeyCode::Char('k') => ex.tree_move(-1),
                    KeyCode::Down | KeyCode::Char('j') => ex.tree_move(1),
                    KeyCode::PageUp => ex.tree_move(-page),
                    KeyCode::PageDown => ex.tree_move(page),
                    KeyCode::Home => ex.tree_first(),
                    KeyCode::End => ex.tree_last(),
                    KeyCode::Right | KeyCode::Char('l') | KeyCode::Enter => ex.tree_expand(),
                    KeyCode::Left | KeyCode::Char('h') => ex.tree_collapse(),
                    KeyCode::Char(' ') => ex.toggle_mark_tree(),
                    _ => {}
                },
                // Right pane: navigate the listing; Enter opens dir / views file.
                ExFocus::List => match code {
                    KeyCode::Up | KeyCode::Char('k') => ex.list_move(-1),
                    KeyCode::Down | KeyCode::Char('j') => ex.list_move(1),
                    KeyCode::PageUp => ex.list_move(-page),
                    KeyCode::PageDown => ex.list_move(page),
                    KeyCode::Home => ex.list_first(),
                    KeyCode::End => ex.list_last(),
                    KeyCode::Enter | KeyCode::Right | KeyCode::Char('l') => ex.list_enter(),
                    KeyCode::Left | KeyCode::Char('h') => ex.focus = ExFocus::Tree,
                    KeyCode::Char(' ') => ex.toggle_mark(),
                    _ => {}
                },
            },
        }
    }

    /// Export the selection to a host directory (already validated by the
    /// picker), in the format chosen from the Export-as menu: a single file
    /// (fork modes), a `.tar[.gz]` archive, or a `.mar`.
    fn explorer_export(&mut self, dest: &std::path::Path) {
        let result = (|| -> anyhow::Result<String> {
            let ex = self
                .explorer
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("no explorer"))?;
            let entry = ex
                .selected_entry()
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("nothing selected"))?;
            if !dest.is_dir() {
                anyhow::bail!("not a directory: {}", dest.display());
            }
            let name = entry.name.clone();
            let is_dir = matches!(entry.entry_type, EntryType::Directory);
            match ex.export_fmt {
                ExportFormat::Fork(mode) => {
                    if is_dir {
                        anyhow::bail!("that format exports a single file, not a directory");
                    }
                    let n = crate::fs::fork_export::export_file_with_fork(
                        &mut *ex.fs,
                        &entry,
                        dest,
                        &name,
                        mode,
                    )?;
                    Ok(format!(
                        "Exported {name} ({}) to {}",
                        format_size(n),
                        dest.display()
                    ))
                }
                ExportFormat::Archive(fmt) => {
                    // The whole selection into one archive via the shared engine:
                    // every Space-marked row (a marked folder walks recursively),
                    // or the highlighted entry when nothing is marked.
                    use crate::fs::export_selection::{export_to_file, export_to_folder};
                    let entries: Vec<FileEntry> = if ex.marked.is_empty() {
                        vec![entry.clone()]
                    } else {
                        ex.marked_entries()
                    };
                    if entries.is_empty() {
                        anyhow::bail!("nothing selected");
                    }
                    let noop = |_: &str, _: usize, _: u64| {};
                    let nocancel = || false;
                    if fmt.is_single_file() {
                        let ext = fmt
                            .file_extension()
                            .map(|e| format!(".{e}"))
                            .unwrap_or_default();
                        let out_name = if entries.len() == 1 {
                            format!("{}{ext}", entries[0].name)
                        } else {
                            format!("selection{ext}")
                        };
                        let out_path = dest.join(&out_name);
                        let summary = export_to_file(
                            &mut *ex.fs,
                            &entries,
                            &out_path,
                            fmt,
                            &noop,
                            &nocancel,
                        )?;
                        Ok(format!(
                            "Exported {} item(s) to {}",
                            summary.files,
                            out_path.display()
                        ))
                    } else {
                        let summary = export_to_folder(
                            &mut *ex.fs,
                            &entries,
                            dest,
                            fmt,
                            RfMode::AppleDouble,
                            &noop,
                            &nocancel,
                        )?;
                        Ok(format!(
                            "Exported {} item(s) to {}",
                            summary.files,
                            dest.display()
                        ))
                    }
                }
            }
        })();
        if let Some(ex) = self.explorer.as_mut() {
            ex.status = Some(match result {
                Ok(msg) => msg,
                Err(e) => format!("Export failed: {e}"),
            });
        }
    }

    /// Import a host file (already validated by the picker) into the current
    /// Explorer directory (opens the partition read-write, reusing the `put`
    /// core), then refresh the view.
    fn explorer_import(&mut self, host: &std::path::Path) {
        let (image_path, selector, part_label, cur_path, comps) = match self.explorer.as_ref() {
            Some(ex) => (
                ex.image_path.clone(),
                ex.selector,
                ex.part_label.clone(),
                ex.path_display(),
                ex.dir_components(),
            ),
            None => return,
        };
        match import_host_file(&image_path, selector, &cur_path, host) {
            Ok(name) => {
                // Re-open read-only at the same path so the new file shows.
                self.reopen_explorer(
                    &image_path,
                    selector,
                    part_label,
                    &comps,
                    Some(format!("Imported {name}")),
                );
            }
            Err(e) => {
                if let Some(ex) = self.explorer.as_mut() {
                    ex.status = Some(format!("Import failed: {e}"));
                }
            }
        }
    }

    /// Apply the metadata editor's Type / Creator / Modified changes to the
    /// selected file (opens the partition read-write), then refresh.
    fn apply_metadata(&mut self) {
        let Some(ex) = self.explorer.as_ref() else {
            return;
        };
        let Some(m) = ex.metadata.as_ref() else {
            return;
        };
        let (image_path, selector, part_label, cur_dir, comps) = (
            ex.image_path.clone(),
            ex.selector,
            ex.part_label.clone(),
            ex.path_display(),
            ex.dir_components(),
        );
        let (name, ty, cr, modstr) = (
            m.entry_name.clone(),
            m.type_code.clone(),
            m.creator.clone(),
            m.modified.clone(),
        );
        let modify_mac = if modstr.trim().is_empty() {
            None
        } else {
            match parse_mac_date(&modstr) {
                Some(v) => Some(v),
                None => {
                    if let Some(m) = self.explorer.as_mut().and_then(|e| e.metadata.as_mut()) {
                        m.error = Some("Bad date — use YYYY-MM-DD HH:MM:SS".to_string());
                    }
                    return;
                }
            }
        };
        match apply_metadata_edit(&image_path, selector, &cur_dir, &name, &ty, &cr, modify_mac) {
            Ok(()) => {
                if let Some(ex) = self.explorer.as_mut() {
                    ex.metadata = None;
                }
                self.reopen_explorer(
                    &image_path,
                    selector,
                    part_label,
                    &comps,
                    Some(format!("Updated {name}")),
                );
            }
            Err(e) => {
                if let Some(m) = self.explorer.as_mut().and_then(|e| e.metadata.as_mut()) {
                    m.error = Some(format!("{e}"));
                }
            }
        }
    }

    /// Close the transform menu and prefill the `:` command palette with the
    /// chosen verb template, substituting the current image path. The user
    /// completes any `<SIZE>` placeholder and presses Enter to run it through the
    /// shared CLI dispatch (the same suspended-alt-screen path `:` uses).
    fn launch_transform(&mut self, sel: usize) {
        let Some(ex) = self.explorer.as_mut() else {
            return;
        };
        ex.transform_menu = None;
        let img = ex.image_path.clone();
        let stem = std::path::Path::new(&img)
            .with_extension("")
            .to_string_lossy()
            .into_owned();
        let template = TRANSFORMS.get(sel).map(|t| t.1).unwrap_or("");
        let cmd = template.replace("{IMG}", &img).replace("{STEM}", &stem);
        // Hand off to the palette modal (checked before the Explorer in
        // handle_events), so the user can edit and run it.
        self.palette_input = Some(cmd);
    }

    /// Run fsck on the Explorer's open partition and show the result overlay.
    /// Read-only — checks the already-open filesystem in place.
    fn explorer_fsck(&mut self) {
        let Some(ex) = self.explorer.as_mut() else {
            return;
        };
        let lines = match with_stderr_suppressed(|| ex.fs.fsck()) {
            Some(Ok(result)) => format_fsck_lines(&result),
            Some(Err(e)) => vec![format!("Check failed: {e:#}")],
            None => vec![
                "This filesystem has no integrity checker.".to_string(),
                String::new(),
                "(fsck is available for FAT/exFAT, ext*, NTFS, HFS/HFS+, and the".to_string(),
                " supported retro filesystems.)".to_string(),
            ],
        };
        ex.fsck_report = Some(FsckReportView {
            title: format!("fsck - {}", ex.part_label),
            lines,
            scroll: 0,
        });
        ex.status = None;
    }

    /// Repair the Explorer's open partition in place (reopen read-write via the
    /// shared repair core), then reopen the view and show a combined
    /// repair + re-check report.
    fn explorer_repair(&mut self) {
        let Some(ex) = self.explorer.as_ref() else {
            return;
        };
        let (image_path, selector, part_label, comps) = (
            ex.image_path.clone(),
            ex.selector,
            ex.part_label.clone(),
            ex.dir_components(),
        );
        match with_stderr_suppressed(|| apply_repair(&image_path, selector)) {
            Ok(report) => {
                let mut lines = format_repair_lines(&report);
                self.reopen_explorer(
                    &image_path,
                    selector,
                    part_label.clone(),
                    &comps,
                    Some("Repair complete".to_string()),
                );
                // Re-check the freshly reopened filesystem to show updated state.
                if let Some(ex) = self.explorer.as_mut() {
                    lines.push(String::new());
                    lines.push("--- re-check after repair ---".to_string());
                    match with_stderr_suppressed(|| ex.fs.fsck()) {
                        Some(Ok(result)) => lines.extend(format_fsck_lines(&result)),
                        Some(Err(e)) => lines.push(format!("Re-check failed: {e:#}")),
                        None => lines.push("(no checker for re-check)".to_string()),
                    }
                    ex.fsck_report = Some(FsckReportView {
                        title: format!("repair - {part_label}"),
                        lines,
                        scroll: 0,
                    });
                }
            }
            Err(e) => {
                if let Some(ex) = self.explorer.as_mut() {
                    ex.status = Some(format!("Repair failed: {e:#}"));
                }
            }
        }
    }

    /// Apply the staged bless (the folder captured in `confirm_bless` when the
    /// user pressed `b`) after they confirm: opens the partition read-write via
    /// the shared bless core, then refreshes.
    fn bless_confirmed(&mut self) {
        let Some(ex) = self.explorer.as_ref() else {
            return;
        };
        let Some((dir_path, name)) = ex.confirm_bless.clone() else {
            return;
        };
        let (image_path, selector, part_label, comps) = (
            ex.image_path.clone(),
            ex.selector,
            ex.part_label.clone(),
            ex.dir_components(),
        );
        match apply_bless_folder(&image_path, selector, &dir_path) {
            Ok(()) => self.reopen_explorer(
                &image_path,
                selector,
                part_label,
                &comps,
                Some(format!("Blessed {name}")),
            ),
            Err(e) => {
                if let Some(ex) = self.explorer.as_mut() {
                    ex.confirm_bless = None;
                    ex.status = Some(format!("Bless failed: {e}"));
                }
            }
        }
    }

    /// Create the typed folder in the Explorer's current directory, then refresh.
    fn do_mkdir(&mut self) {
        let Some(ex) = self.explorer.as_ref() else {
            return;
        };
        let name = ex
            .mkdir_input
            .clone()
            .unwrap_or_default()
            .trim()
            .to_string();
        let (image_path, selector, part_label, cur_dir, comps) = (
            ex.image_path.clone(),
            ex.selector,
            ex.part_label.clone(),
            ex.path_display(),
            ex.dir_components(),
        );
        if name.is_empty() {
            if let Some(ex) = self.explorer.as_mut() {
                ex.mkdir_input = None;
                ex.status = Some("Folder name was empty.".to_string());
            }
            return;
        }
        match apply_mkdir(&image_path, selector, &cur_dir, &name) {
            Ok(()) => {
                if let Some(ex) = self.explorer.as_mut() {
                    ex.mkdir_input = None;
                }
                self.reopen_explorer(
                    &image_path,
                    selector,
                    part_label,
                    &comps,
                    Some(format!("Created folder {name}")),
                );
            }
            Err(e) => {
                if let Some(ex) = self.explorer.as_mut() {
                    ex.mkdir_input = None;
                    ex.status = Some(format!("{e:#}"));
                }
            }
        }
    }

    /// Delete the confirmed entry from the Explorer's current directory, refresh.
    fn do_delete(&mut self) {
        let Some(ex) = self.explorer.as_ref() else {
            return;
        };
        let Some((name, _)) = ex.confirm_delete.clone() else {
            return;
        };
        let (image_path, selector, part_label, cur_dir, comps) = (
            ex.image_path.clone(),
            ex.selector,
            ex.part_label.clone(),
            ex.path_display(),
            ex.dir_components(),
        );
        match apply_delete(&image_path, selector, &cur_dir, &name) {
            Ok(()) => {
                if let Some(ex) = self.explorer.as_mut() {
                    ex.confirm_delete = None;
                }
                self.reopen_explorer(
                    &image_path,
                    selector,
                    part_label,
                    &comps,
                    Some(format!("Deleted {name}")),
                );
            }
            Err(e) => {
                if let Some(ex) = self.explorer.as_mut() {
                    ex.confirm_delete = None;
                    ex.status = Some(format!("{e:#}"));
                }
            }
        }
    }

    /// Rebuild the Explorer read-only and descend through `comps` (dir names),
    /// so a post-import view reflects the change.
    fn reopen_explorer(
        &mut self,
        image_path: &str,
        selector: Option<u32>,
        part_label: String,
        comps: &[String],
        status: Option<String>,
    ) {
        self.open_explorer(image_path, selector, part_label);
        if let Some(ex) = self.explorer.as_mut() {
            // Walk the saved path, expanding and selecting each component in the
            // freshly-built tree so the view returns to where it was.
            for comp in comps {
                ex.tree_expand();
                let child = ex
                    .tree
                    .iter()
                    .position(|n| n.depth == ex.tree[ex.tree_sel].depth + 1 && n.dir.name == *comp);
                match child {
                    Some(idx) => ex.tree_sel = idx,
                    None => break,
                }
            }
            ex.reload_list();
            ex.status = status;
        }
    }

    /// Esc: unwind one level — cancel a running task, then close a drill-down,
    /// then quit.
    fn on_back(&mut self) {
        if self.progress.is_some() {
            self.cancel_progress();
        } else if self.opened.is_some() {
            self.opened = None;
        } else if self.detail.is_some() {
            self.detail = None;
        } else {
            self.should_quit = true;
        }
    }

    fn cancel_progress(&mut self) {
        if let Some(p) = &self.progress {
            if let Ok(mut s) = p.shared.lock() {
                s.cancel = true;
            }
        }
        // Also signal a running backup / restore to stop.
        if let Some(run) = self.backup.as_ref().and_then(|b| b.run.clone()) {
            if let Ok(mut p) = run.lock() {
                p.cancel_requested = true;
            }
        }
        if let Some(run) = self.restore.as_ref().and_then(|r| r.run.clone()) {
            if let Ok(mut p) = run.lock() {
                p.cancel_requested = true;
            }
        }
        if let Some(run) = self.bulk.as_ref().and_then(|b| b.run.clone()) {
            if let Ok(mut s) = run.lock() {
                s.cancel_requested = true;
            }
        }
        #[cfg(feature = "optical")]
        if let Some(run) = self.optical.as_ref().and_then(|o| o.run.clone()) {
            if let Ok(mut p) = run.lock() {
                p.cancel_requested = true;
            }
        }
        self.progress = None;
    }

    /// Lazily load anything a freshly-shown tab needs; reset per-tab cursor.
    fn on_tab_changed(&mut self) {
        self.selection = 0;
        self.detail = None;
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
            self.optical = Some(OpticalState {
                drives: crate::model::optical_devices::list_local_rip_devices(),
                ..OpticalState::default()
            });
        }
        if self.current() == TabId::Archives && self.archive.is_none() {
            self.archive = Some(ArchiveState::default());
        }
        if self.current() == TabId::Commander && self.commander.is_none() {
            self.commander = Some(CommanderState::default());
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
            let current = env!("APP_VERSION");
            let msg =
                match with_stderr_suppressed(|| crate::update::check_for_updates(&cfg, current)) {
                    Ok(info) if info.is_outdated => format!(
                        "Update available: {} (current {current}). Run `rb-cli update --apply`.",
                        info.latest_version
                    ),
                    Ok(_) => format!("Up to date (current {current})."),
                    Err(e) => format!("Update check failed: {e}"),
                };
            if let Some(s) = self.settings.as_mut() {
                s.status = Some(msg);
            }
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
                    w.path = path.to_string_lossy().into_owned();
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
                    true
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    w.class_sel = (w.class_sel + 1).min(NEW_CLASSES.len() - 1);
                    w.fs_sel = 0;
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
                    w.field = (w.field + 1) % WIZ_FIELDS;
                    true
                }
                KeyCode::Up | KeyCode::BackTab => {
                    w.field = (w.field + WIZ_FIELDS - 1) % WIZ_FIELDS;
                    true
                }
                // Tab browses for a path when on the path field, else advances.
                KeyCode::Tab => {
                    if w.field == 0 {
                        w.picker = Some(FilePicker::new(PickKind::Any, "Browse for image path"));
                    } else {
                        w.field = (w.field + 1) % WIZ_FIELDS;
                    }
                    true
                }
                KeyCode::Backspace => {
                    w.status = None;
                    match w.field {
                        0 => w.path.pop(),
                        1 => w.size.pop(),
                        _ => w.name.pop(),
                    };
                    true
                }
                KeyCode::Char(c) if !c.is_control() => {
                    w.status = None;
                    match w.field {
                        0 => w.path.push(c),
                        1 => w.size.push(c),
                        _ => w.name.push(c),
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
            }),
            DiskClass::Volume => NewCommand::Volume(VolumeArgs {
                fs: VOLUME_FS[w.fs_sel.min(VOLUME_FS.len() - 1)].1,
                image: path.clone(),
                size,
                name,
                block_size: None,
                catalog_size: None,
                extents_size: None,
                case_sensitive: false,
                min_catalog: None,
                affs_variant: 1,
                inodes: None,
                bytes_per_inode: None,
                cluster_size: None,
                sector_size: None,
            }),
        };
        // `new::run` prints advisories to stdout/stderr, which would land on the
        // alt-screen; run it with both streams redirected to null.
        let outcome = with_stderr_suppressed(|| crate::cli::verbs::new::run(cmd));
        let w = self.newdisk.as_mut().unwrap();
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
    }

    /// Backup screen keys. Returns `true` when consumed; `false` lets a key fall
    /// through to the shell (Left/Right switch tabs, `q` quits from idle steps).
    fn handle_backup_key(&mut self, code: KeyCode) -> bool {
        if self.backup.is_none() {
            self.backup = Some(BackupState::default());
        }
        // Source image file picker (modal).
        if self
            .backup
            .as_ref()
            .map(|b| b.picker.is_some())
            .unwrap_or(false)
        {
            let is_dir = matches!(
                self.backup.as_ref().unwrap().picker.as_ref().unwrap().kind,
                PickKind::Dir
            );
            let res = self
                .backup
                .as_mut()
                .unwrap()
                .picker
                .as_mut()
                .unwrap()
                .handle_key(code);
            match res {
                Some(PickResult::Cancel) => self.backup.as_mut().unwrap().picker = None,
                Some(PickResult::Confirm(path)) => {
                    if is_dir {
                        // Output-folder browse.
                        let b = self.backup.as_mut().unwrap();
                        b.out_dir = path.to_string_lossy().into_owned();
                        b.picker = None;
                    } else {
                        crate::update::push_recent(
                            crate::update::RecentMode::Backup,
                            &path.to_string_lossy(),
                        );
                        self.backup_set_source(path.to_string_lossy().into_owned(), false);
                    }
                }
                None => {}
            }
            return true;
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
                    let dev = self
                        .disks
                        .as_ref()
                        .and_then(|d| d.get(sel))
                        .map(|d| d.path.display().to_string());
                    if let Some(path) = dev {
                        self.backup.as_mut().unwrap().device_pick = false;
                        self.backup_set_source(path, true);
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
                    true
                }
                KeyCode::Left | KeyCode::Right | KeyCode::Char(' ') if b.field == 3 => {
                    let n = BACKUP_CHECKSUMS.len();
                    if code == KeyCode::Left {
                        b.checksum_sel = (b.checksum_sel + n - 1) % n;
                    } else {
                        b.checksum_sel = (b.checksum_sel + 1) % n;
                    }
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
                        0 => b.out_dir.pop(),
                        1 => b.name.pop(),
                        _ => None,
                    };
                    true
                }
                KeyCode::Char(c) if !c.is_control() && (b.field == 0 || b.field == 1) => {
                    match b.field {
                        0 => b.out_dir.push(c),
                        _ => b.name.push(c),
                    }
                    true
                }
                _ => false,
            },
            BackupStep::Run => match code {
                KeyCode::Esc => {
                    let running = b.result.is_none();
                    if running {
                        // Request cancel; the bar clears via cancel_progress.
                        self.cancel_progress();
                        if let Some(b) = self.backup.as_mut() {
                            b.result = Some("Cancel requested...".to_string());
                            b.is_error = true;
                        }
                    } else {
                        // Done: clear and return to the config form.
                        self.progress = None;
                        if let Some(b) = self.backup.as_mut() {
                            b.run = None;
                            b.result = None;
                            b.step = BackupStep::Config;
                        }
                    }
                    true
                }
                _ => true, // swallow keys while a backup is on screen
            },
        }
    }

    /// Record a chosen backup source and advance to the config form, defaulting
    /// the output folder (source's parent) and backup name (source stem + date).
    fn backup_set_source(&mut self, source: String, from_device: bool) {
        let Some(b) = self.backup.as_mut() else {
            return;
        };
        b.source = source.clone();
        b.from_device = from_device;
        b.picker = None;
        // Default the name from the source's file stem (or device name).
        let stem = std::path::Path::new(&source)
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "backup".to_string());
        if b.name.is_empty() {
            b.name = stem;
        }
        // Default the output folder to the source's parent (for a file source).
        if b.out_dir.is_empty() && !from_device {
            if let Some(parent) = std::path::Path::new(&source).parent() {
                b.out_dir = parent.display().to_string();
            }
        }
        b.step = BackupStep::Config;
        b.field = 0;
    }

    /// Validate the config form and start `backup::run_backup` on a worker
    /// thread, mirroring its `BackupProgress` into the shell's progress bar.
    fn start_backup(&mut self) {
        let Some(b) = self.backup.as_ref() else {
            return;
        };
        let source = expand_tilde(b.source.trim());
        if source.as_os_str().is_empty() || (!b.from_device && !source.exists()) {
            let b = self.backup.as_mut().unwrap();
            b.result = Some("Source not found.".to_string());
            b.is_error = true;
            b.step = BackupStep::Run;
            return;
        }
        let out_dir = expand_tilde(b.out_dir.trim());
        if out_dir.as_os_str().is_empty() {
            let b = self.backup.as_mut().unwrap();
            b.result = Some("Enter an output folder.".to_string());
            b.is_error = true;
            b.step = BackupStep::Run;
            return;
        }
        let name = if b.name.trim().is_empty() {
            "backup".to_string()
        } else {
            b.name.trim().to_string()
        };
        let compression = BACKUP_FORMATS[b.format_sel.min(BACKUP_FORMATS.len() - 1)].1;
        let checksum = BACKUP_CHECKSUMS[b.checksum_sel.min(BACKUP_CHECKSUMS.len() - 1)].1;
        let config = crate::backup::BackupConfig {
            source_path: source,
            destination_dir: out_dir,
            backup_name: name,
            compression,
            checksum,
            split_size_mib: None,
            sector_by_sector: false,
            partition_filter: None,
            chd_options: None,
            size_policy: None,
            partition_target_sizes: None,
            shrink_to_minimum: false,
            precomputed_minimum_sizes: None,
            defrag_partition_indices: None,
            defrag_fat: false,
            keep_swap: false,
        };
        let progress = Arc::new(Mutex::new(crate::backup::BackupProgress::new()));
        let worker = Arc::clone(&progress);
        std::thread::spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                crate::backup::run_backup(config, Arc::clone(&worker))
            }));
            if let Ok(mut p) = worker.lock() {
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        if p.error.is_none() {
                            p.error = Some(format!("{e:#}"));
                        }
                    }
                    Err(_) => {
                        if p.error.is_none() {
                            p.error = Some("backup thread panicked".to_string());
                        }
                    }
                }
                p.finished = true; // always unstick the poll loop
            }
        });
        let b = self.backup.as_mut().unwrap();
        b.run = Some(progress);
        b.result = None;
        b.is_error = false;
        b.op = "Starting...".to_string();
        b.step = BackupStep::Run;
        self.progress = Some(Progress {
            shared: Arc::new(Mutex::new(ProgressShared::default())),
            tracker: RateTracker::default(),
            label: "Backing up".to_string(),
        });
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
                    r.picker = Some(
                        FilePicker::new(PickKind::Any, "Choose backup folder or .cbk")
                            .with_recent(recent),
                    );
                    r.picker_target = false;
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
                            r.run = None;
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
    fn load_restore_source(&mut self, path: std::path::PathBuf) {
        // Materialize a .cbk container to a temp folder we keep alive.
        let (folder, guard): (std::path::PathBuf, Option<tempfile::TempDir>) =
            if path.is_file() && crate::rbformats::cbk::is_cbk(&path) {
                match tempfile::TempDir::new().and_then(|tmp| {
                    crate::rbformats::cbk::materialize_cbk_to_folder(&path, tmp.path())
                        .map(|_| tmp)
                        .map_err(crate::compat::io_other)
                }) {
                    Ok(tmp) => (tmp.path().to_path_buf(), Some(tmp)),
                    Err(e) => {
                        if let Some(r) = self.restore.as_mut() {
                            r.result = Some(format!("Cannot open .cbk: {e}"));
                            r.is_error = true;
                        }
                        return;
                    }
                }
            } else {
                (path.clone(), None)
            };

        match crate::model::backup_loader::load_backup(&folder) {
            Ok(crate::model::backup_loader::LoadOutcome::Backup(b)) => {
                crate::update::push_recent(
                    crate::update::RecentMode::Restore,
                    &path.to_string_lossy(),
                );
                let default_target = folder
                    .parent()
                    .map(|par| {
                        par.join(format!(
                            "{}_restored.img",
                            folder
                                .file_name()
                                .map(|n| n.to_string_lossy().into_owned())
                                .unwrap_or_else(|| "restore".to_string())
                        ))
                        .display()
                        .to_string()
                    })
                    .unwrap_or_default();
                if let Some(r) = self.restore.as_mut() {
                    r.backup_folder = folder.display().to_string();
                    r.source_size = b.metadata.source_size_bytes;
                    r.part_count = b.partitions.len();
                    r.loaded = true;
                    r.cbk_guard = guard;
                    if r.target.is_empty() {
                        r.target = default_target;
                    }
                    r.step = RestoreStep::Config;
                    r.field = 0;
                    r.result = None;
                    r.is_error = false;
                }
            }
            Ok(crate::model::backup_loader::LoadOutcome::Clonezilla(_)) => {
                if let Some(r) = self.restore.as_mut() {
                    r.result = Some(
                        "Clonezilla images: restore via the CLI (`rb-cli restore`) for now."
                            .to_string(),
                    );
                    r.is_error = true;
                }
            }
            Err(e) => {
                if let Some(r) = self.restore.as_mut() {
                    r.result = Some(format!("Not a rusty-backup folder: {e}"));
                    r.is_error = true;
                }
            }
        }
    }

    /// Validate the config and start `restore::run_restore` on a worker thread.
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

    fn start_restore_confirmed(&mut self) {
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
        let partition_sizes = if r.size_sel == 1 {
            (0..r.part_count)
                .map(|i| crate::restore::RestorePartitionSize {
                    index: i,
                    size_choice: crate::restore::RestoreSizeChoice::Minimum,
                })
                .collect()
        } else {
            Vec::new()
        };
        let alignment = if r.align_sel == 1 {
            crate::restore::RestoreAlignment::Modern1MB
        } else {
            crate::restore::RestoreAlignment::Original
        };
        let config = crate::restore::RestoreConfig {
            backup_folder: std::path::PathBuf::from(&r.backup_folder),
            target_path: target,
            target_is_device: is_device,
            target_size: r.source_size,
            alignment,
            partition_sizes,
            write_zeros_to_unused: false,
        };
        let progress = Arc::new(Mutex::new(crate::restore::RestoreProgress::new()));
        let worker = Arc::clone(&progress);
        // Move the .cbk temp-dir guard into the worker so it outlives the restore.
        let guard = self.restore.as_mut().and_then(|r| r.cbk_guard.take());
        std::thread::spawn(move || {
            let _guard = guard;
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                crate::restore::run_restore(config, Arc::clone(&worker))
            }));
            if let Ok(mut p) = worker.lock() {
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        if p.error.is_none() {
                            p.error = Some(format!("{e:#}"));
                        }
                    }
                    Err(_) => {
                        if p.error.is_none() {
                            p.error = Some("restore thread panicked".to_string());
                        }
                    }
                }
                p.finished = true;
            }
        });
        let r = self.restore.as_mut().unwrap();
        r.run = Some(progress);
        r.result = None;
        r.is_error = false;
        r.op = "Starting...".to_string();
        r.step = RestoreStep::Run;
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
                        let b = self.bulk.as_mut().unwrap();
                        b.picker = None;
                        b.source = path.to_string_lossy().into_owned();
                        if b.out_dir.is_empty() {
                            b.out_dir = b.source.clone();
                        }
                        b.step = BulkStep::Config;
                        b.field = 0;
                        self.bulk_rescan();
                    }
                }
                None => {}
            }
            return true;
        }

        let step = self.bulk.as_ref().unwrap().step;
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
                        b.files[i].selected = !b.files[i].selected;
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
                    let running = b.result.is_none();
                    if running {
                        self.cancel_progress();
                        if let Some(b) = self.bulk.as_mut() {
                            b.result = Some("Cancel requested...".to_string());
                            b.is_error = true;
                        }
                    } else {
                        self.progress = None;
                        if let Some(b) = self.bulk.as_mut() {
                            b.run = None;
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
    /// depends on the format), resetting the review list.
    fn bulk_rescan(&mut self) {
        let Some(b) = self.bulk.as_mut() else {
            return;
        };
        let src = expand_tilde(b.source.trim());
        let format = b.format();
        b.files =
            crate::model::bulk_convert_runner::scan_source_folder(&src, format).unwrap_or_default();
        // Keep the cursor on the Format row after a rescan.
        b.field = b.field.min(b.start_row());
    }

    /// Start the bulk conversion of the selected files. `start_bulk_convert`
    /// spawns its own worker thread and returns the shared status handle.
    fn start_bulk(&mut self) {
        let Some(b) = self.bulk.as_ref() else {
            return;
        };
        let files: Vec<std::path::PathBuf> = b
            .files
            .iter()
            .filter(|f| f.selected)
            .map(|f| f.path.clone())
            .collect();
        if files.is_empty() {
            let b = self.bulk.as_mut().unwrap();
            b.result = Some("No files selected to convert.".to_string());
            b.is_error = true;
            b.step = BulkStep::Run;
            return;
        }
        let out_dir = expand_tilde(b.out_dir.trim());
        if out_dir.as_os_str().is_empty() {
            let b = self.bulk.as_mut().unwrap();
            b.result = Some("Enter an output folder.".to_string());
            b.is_error = true;
            b.step = BulkStep::Run;
            return;
        }
        let format = b.format();
        let extension = format.extension().to_string();
        let run = crate::model::bulk_convert_runner::start_bulk_convert(
            files, out_dir, format, extension, None, false,
        );
        let b = self.bulk.as_mut().unwrap();
        b.run = Some(run);
        b.result = None;
        b.is_error = false;
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
            self.optical = Some(OpticalState {
                drives: crate::model::optical_devices::list_local_rip_devices(),
                ..OpticalState::default()
            });
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
                    o.drive_sel = (o.drive_sel + 1).min(o.drives.len().saturating_sub(1));
                    true
                }
                KeyCode::Char('r') => {
                    o.drives = crate::model::optical_devices::list_local_rip_devices();
                    o.drive_sel = 0;
                    true
                }
                KeyCode::Enter => {
                    if let Some(d) = o.drives.get(o.drive_sel) {
                        o.device_path = d.device_path.clone();
                        o.device_name = d.display_name.clone();
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
                    let running = o.result.is_none();
                    if running {
                        self.cancel_progress();
                        if let Some(o) = self.optical.as_mut() {
                            o.result = Some("Cancel requested...".to_string());
                            o.is_error = true;
                        }
                    } else {
                        self.progress = None;
                        if let Some(o) = self.optical.as_mut() {
                            o.run = None;
                            o.result = None;
                            o.step = OpticalStep::Config;
                        }
                    }
                    true
                }
                _ => true,
            },
        }
    }

    /// Start `optical::rip::run_rip` on a worker thread for the selected drive.
    #[cfg(feature = "optical")]
    fn start_optical_rip(&mut self) {
        let Some(o) = self.optical.as_ref() else {
            return;
        };
        let output = expand_tilde(o.output.trim());
        if output.as_os_str().is_empty() {
            let o = self.optical.as_mut().unwrap();
            o.result = Some("Enter an output path.".to_string());
            o.is_error = true;
            o.step = OpticalStep::Run;
            return;
        }
        let device = match crate::optical::rip::OpticalTarget::resolve(&o.device_path) {
            Ok(t) => t,
            Err(e) => {
                let o = self.optical.as_mut().unwrap();
                o.result = Some(format!("Cannot open drive: {e:#}"));
                o.is_error = true;
                o.step = OpticalStep::Run;
                return;
            }
        };
        let format = RIP_FORMATS[o.format_sel.min(RIP_FORMATS.len() - 1)].1;
        let config = crate::optical::rip::RipConfig {
            device,
            output_path: output,
            format,
            eject_after: o.eject,
        };
        let progress = Arc::new(Mutex::new(crate::optical::rip::RipProgress::new()));
        let worker = Arc::clone(&progress);
        std::thread::spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                crate::optical::rip::run_rip(config, Arc::clone(&worker))
            }));
            if let Ok(mut p) = worker.lock() {
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        if p.error.is_none() {
                            p.error = Some(format!("{e:#}"));
                        }
                    }
                    Err(_) => {
                        if p.error.is_none() {
                            p.error = Some("rip thread panicked".to_string());
                        }
                    }
                }
                p.finished = true;
            }
        });
        let o = self.optical.as_mut().unwrap();
        o.run = Some(progress);
        o.result = None;
        o.is_error = false;
        o.op = "Starting...".to_string();
        o.step = OpticalStep::Run;
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
            .map(|a| a.archive.is_some())
            .unwrap_or(false);
        let page = self.explorer_page() as isize;
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

    /// Open a Mac archive and pre-render its entry list.
    fn load_archive(&mut self, path: std::path::PathBuf) {
        match crate::macarchive::extract::open(&path) {
            Ok((bytes, archive)) => {
                let mut entries = Vec::new();
                for e in &archive.entries {
                    if e.is_dir {
                        entries.push(format!("DIR   {}/", e.display_path()));
                        continue;
                    }
                    let data = e
                        .data
                        .as_ref()
                        .filter(|f| f.uncompressed_len > 0)
                        .map(|f| format!("data {}", format_size(f.uncompressed_len as u64)))
                        .unwrap_or_default();
                    let rsrc = e
                        .rsrc
                        .as_ref()
                        .filter(|f| f.uncompressed_len > 0)
                        .map(|f| format!("rsrc {}", format_size(f.uncompressed_len as u64)))
                        .unwrap_or_default();
                    let ty = crate::fs::hfs::format_ostype(&e.type_code);
                    let cr = crate::fs::hfs::format_ostype(&e.creator_code);
                    entries.push(format!(
                        "FILE  {:<34} {:>4} {:>4}  {}  {}",
                        e.display_path(),
                        ty,
                        cr,
                        data,
                        rsrc
                    ));
                }
                crate::update::push_recent(
                    crate::update::RecentMode::Archives,
                    &path.to_string_lossy(),
                );
                if let Some(a) = self.archive.as_mut() {
                    a.archive_path = path.display().to_string();
                    a.bytes = bytes;
                    a.archive = Some(archive);
                    a.entries = entries;
                    a.list_sel = 0;
                    a.status = Some(format!("Opened ({} entries).", a.entries.len()));
                    a.is_error = false;
                }
            }
            Err(e) => {
                if let Some(a) = self.archive.as_mut() {
                    a.status = Some(format!("Cannot open archive: {e:#}"));
                    a.is_error = true;
                }
            }
        }
    }

    /// Extract the open archive to `dest` in the chosen fork format.
    fn archive_extract(&mut self, dest: std::path::PathBuf) {
        let Some(a) = self.archive.as_ref() else {
            return;
        };
        let Some(archive) = a.archive.as_ref() else {
            return;
        };
        let format = ARCHIVE_FORK_FORMATS[a.fork_fmt_sel.min(ARCHIVE_FORK_FORMATS.len() - 1)].1;
        let result = crate::macarchive::extract::extract_all(
            &a.bytes,
            archive,
            &dest,
            format,
            |_, _, _| {},
            |_| {},
        );
        let a = self.archive.as_mut().unwrap();
        match result {
            Ok(stats) => {
                a.status = Some(format!(
                    "Extracted {} file(s){} to {}",
                    stats.files,
                    if stats.skipped > 0 {
                        format!(" ({} skipped)", stats.skipped)
                    } else {
                        String::new()
                    },
                    dest.display()
                ));
                a.is_error = false;
            }
            Err(e) => {
                a.status = Some(format!("Extract failed: {e:#}"));
                a.is_error = true;
            }
        }
    }

    /// Commander screen keys. Returns `true` when consumed.
    fn handle_commander_key(&mut self, code: KeyCode) -> bool {
        if self.commander.is_none() {
            self.commander = Some(CommanderState::default());
        }
        let page = self.explorer_page() as isize;
        let c = self.commander.as_mut().unwrap();
        let active = c.active;

        // Open picker on the active pane (modal).
        if c.pane(active).picker.is_some() {
            let host = c.pane(active).picker_host;
            let res = c.pane_mut(active).picker.as_mut().unwrap().handle_key(code);
            match res {
                Some(PickResult::Cancel) => c.pane_mut(active).picker = None,
                Some(PickResult::Confirm(path)) => {
                    c.pane_mut(active).picker = None;
                    if host {
                        c.open_host(path);
                    } else {
                        c.open_image(path);
                    }
                }
                None => {}
            }
            return true;
        }

        // Attribute-edit prompt from the info overlay (modal, innermost).
        if let Some(p) = c.attr_input.as_mut() {
            match code {
                KeyCode::Esc => c.attr_input = None,
                KeyCode::Enter => {
                    let p = c.attr_input.take().unwrap();
                    c.apply_attr_edit(p.field, &p.text);
                }
                KeyCode::Backspace => {
                    p.text.pop();
                }
                KeyCode::Char(ch) => p.text.push(ch),
                _ => {}
            }
            return true;
        }

        // File Info overlay (modal): m/o/x start an attribute edit, Esc closes.
        if c.info.is_some() {
            match code {
                KeyCode::Esc | KeyCode::Char('q') | KeyCode::Enter => c.info = None,
                KeyCode::Char('m') => {
                    c.attr_input = Some(AttrPrompt {
                        field: AttrField::Mode,
                        text: String::new(),
                    })
                }
                KeyCode::Char('o') => {
                    c.attr_input = Some(AttrPrompt {
                        field: AttrField::Owner,
                        text: String::new(),
                    })
                }
                KeyCode::Char('x') => {
                    c.attr_input = Some(AttrPrompt {
                        field: AttrField::Xattr,
                        text: String::new(),
                    })
                }
                _ => {}
            }
            return true;
        }

        // Checksum results overlay (modal): scroll, Esc/q/Enter to close.
        if let Some(rv) = c.checksum_report.as_mut() {
            let page = page.max(1) as usize;
            let max = rv.lines.len().saturating_sub(1);
            match code {
                KeyCode::Esc | KeyCode::Char('q') | KeyCode::Enter => c.checksum_report = None,
                KeyCode::Up | KeyCode::Char('k') => rv.scroll = rv.scroll.saturating_sub(1),
                KeyCode::Down | KeyCode::Char('j') => rv.scroll = (rv.scroll + 1).min(max),
                KeyCode::PageUp => rv.scroll = rv.scroll.saturating_sub(page),
                KeyCode::PageDown => rv.scroll = (rv.scroll + page).min(max),
                KeyCode::Home => rv.scroll = 0,
                KeyCode::End => rv.scroll = max,
                _ => {}
            }
            return true;
        }

        // New-folder name prompt (modal).
        if c.mkdir_input.is_some() {
            match code {
                KeyCode::Esc => c.mkdir_input = None,
                KeyCode::Enter => {
                    let name = c.mkdir_input.take().unwrap_or_default();
                    c.mkdir(&name);
                }
                KeyCode::Backspace => {
                    if let Some(s) = c.mkdir_input.as_mut() {
                        s.pop();
                    }
                }
                KeyCode::Char(ch) => {
                    if let Some(s) = c.mkdir_input.as_mut() {
                        s.push(ch);
                    }
                }
                _ => {}
            }
            return true;
        }

        // Connect-to-remote host:port prompt (modal).
        #[cfg(feature = "remote")]
        if c.connect_input.is_some() {
            match code {
                KeyCode::Esc => c.connect_input = None,
                KeyCode::Enter => {
                    let addr = c.connect_input.take().unwrap_or_default();
                    let addr = addr.trim().to_string();
                    if addr.is_empty() {
                        c.status = Some("Enter a host:port to connect.".to_string());
                        c.is_error = true;
                    } else {
                        c.start_connect(addr);
                    }
                }
                KeyCode::Backspace => {
                    if let Some(s) = c.connect_input.as_mut() {
                        s.pop();
                    }
                }
                KeyCode::Char(ch) => {
                    if let Some(s) = c.connect_input.as_mut() {
                        s.push(ch);
                    }
                }
                _ => {}
            }
            return true;
        }

        // Delete confirmation (modal).
        if c.confirm_delete.is_some() {
            match code {
                KeyCode::Char('y') | KeyCode::Char('Y') | KeyCode::Enter => {
                    c.confirm_delete = None;
                    c.delete_selected();
                }
                _ => c.confirm_delete = None,
            }
            return true;
        }

        // Partition chooser on the active pane (modal).
        if c.pane(active).part_pick {
            let np = c.pane(active).parts.len();
            match code {
                KeyCode::Up | KeyCode::Char('k') => {
                    let p = c.pane_mut(active);
                    p.part_sel = p.part_sel.saturating_sub(1);
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    let p = c.pane_mut(active);
                    p.part_sel = (p.part_sel + 1).min(np.saturating_sub(1));
                }
                KeyCode::Esc => {
                    let p = c.pane_mut(active);
                    p.part_pick = false;
                    p.pending_img = None;
                    p.parts.clear();
                }
                KeyCode::Enter => c.choose_partition(),
                _ => {}
            }
            return true;
        }

        // Optical-filesystem chooser on the active pane (modal): pick the ISO
        // 9660 or HFS side of a hybrid disc.
        if c.pane(active).optical_pick {
            let nfs = c
                .pane(active)
                .optical
                .as_ref()
                .map(|o| o.choices.len())
                .unwrap_or(0);
            match code {
                KeyCode::Up | KeyCode::Char('k') => {
                    let p = c.pane_mut(active);
                    p.optical_sel = p.optical_sel.saturating_sub(1);
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    let p = c.pane_mut(active);
                    p.optical_sel = (p.optical_sel + 1).min(nfs.saturating_sub(1));
                }
                KeyCode::Esc => {
                    let p = c.pane_mut(active);
                    p.optical_pick = false;
                    // If nothing was ever loaded (first open), drop the staged
                    // optical state so the pane returns to empty.
                    if !p.loaded {
                        p.optical = None;
                    }
                }
                KeyCode::Enter => c.choose_optical(),
                _ => {}
            }
            return true;
        }

        // Tab / BackTab switch the focused pane.
        if matches!(code, KeyCode::Tab | KeyCode::BackTab) {
            c.active = match active {
                Side::Left => Side::Right,
                Side::Right => Side::Left,
            };
            return true;
        }

        // Connect the active pane to a remote daemon (works whether or not a
        // source is already open — it replaces the pane's source).
        #[cfg(feature = "remote")]
        if code == KeyCode::Char('R') {
            c.connect_input = Some(String::new());
            c.status = None;
            c.is_error = false;
            return true;
        }

        let loaded = c.pane(active).loaded;
        if !loaded {
            return match code {
                KeyCode::Char('o') => {
                    let p = c.pane_mut(active);
                    p.picker = Some(FilePicker::new(PickKind::Dir, "Open host folder"));
                    p.picker_host = true;
                    true
                }
                KeyCode::Char('i') => {
                    let p = c.pane_mut(active);
                    p.picker = Some(FilePicker::new(PickKind::File, "Open image file"));
                    p.picker_host = false;
                    true
                }
                _ => false,
            };
        }

        let n = c.pane(active).rows_len() as isize;
        match code {
            KeyCode::Up | KeyCode::Char('k') => {
                let p = c.pane_mut(active);
                p.sel = p.sel.saturating_sub(1);
                true
            }
            KeyCode::Down | KeyCode::Char('j') => {
                let p = c.pane_mut(active);
                p.sel = ((p.sel as isize + 1).min(n - 1).max(0)) as usize;
                true
            }
            KeyCode::PageUp => {
                let p = c.pane_mut(active);
                p.sel = (p.sel as isize - page).max(0) as usize;
                true
            }
            KeyCode::PageDown => {
                let p = c.pane_mut(active);
                p.sel = ((p.sel as isize + page).min(n - 1).max(0)) as usize;
                true
            }
            KeyCode::Home => {
                c.pane_mut(active).sel = 0;
                true
            }
            KeyCode::End => {
                c.pane_mut(active).sel = (n - 1).max(0) as usize;
                true
            }
            KeyCode::Enter | KeyCode::Right | KeyCode::Char('l') => {
                if let Some(r) = cmd_enter(c.pane_mut(active)) {
                    match r {
                        Ok(m) => {
                            c.status = Some(m);
                            c.is_error = false;
                        }
                        Err(e) => {
                            c.status = Some(e);
                            c.is_error = true;
                        }
                    }
                }
                true
            }
            KeyCode::Backspace | KeyCode::Left | KeyCode::Char('h') => {
                // In a remote image at its root, step back out to the daemon's
                // host browser (close the image); otherwise navigate up normally.
                #[cfg(feature = "remote")]
                if cmd_remote_up(c.pane_mut(active)) {
                    return true;
                }
                let p = c.pane_mut(active);
                p.listing.up();
                p.sel = 0;
                true
            }
            KeyCode::Char('o') => {
                let p = c.pane_mut(active);
                p.picker = Some(FilePicker::new(PickKind::Dir, "Open host folder"));
                p.picker_host = true;
                true
            }
            KeyCode::Char('i') => {
                let p = c.pane_mut(active);
                p.picker = Some(FilePicker::new(PickKind::File, "Open image file"));
                p.picker_host = false;
                true
            }
            // File Info overlay: permissions / owner / xattrs, with m/o/x to
            // edit them. F3 mirrors the Midnight Commander "View" key.
            KeyCode::Char('v') | KeyCode::F(3) => {
                c.open_info();
                true
            }
            KeyCode::Char('r') => {
                c.refresh_active();
                true
            }
            // Switch the filesystem shown for a hybrid optical disc (ISO 9660
            // <-> HFS side).
            KeyCode::Char('p') => {
                c.reopen_chooser();
                true
            }
            KeyCode::Char('c') | KeyCode::F(5) => {
                c.copy();
                true
            }
            // Checksum the marked files (CRC32 / SHA256), shown in an overlay.
            KeyCode::Char('#') => {
                c.start_checksum();
                true
            }
            KeyCode::Char('n') => {
                c.mkdir_input = Some(String::new());
                c.status = None;
                c.is_error = false;
                true
            }
            KeyCode::Char('x') | KeyCode::Delete | KeyCode::F(8) => {
                let entries = c.pane(active).action_entries();
                match entries.len() {
                    0 => {
                        c.status = Some("Select a file or folder to delete.".to_string());
                        c.is_error = true;
                    }
                    1 => {
                        let e = &entries[0];
                        let kind = if matches!(e.entry_type, EntryType::Directory) {
                            "folder"
                        } else {
                            "file"
                        };
                        c.confirm_delete = Some(format!("{kind} \"{}\"", e.name));
                    }
                    n => c.confirm_delete = Some(format!("{n} items")),
                }
                true
            }
            // Space marks/unmarks the entry under the cursor and advances.
            KeyCode::Char(' ') => {
                let p = c.pane_mut(active);
                p.toggle_mark();
                p.sel = ((p.sel as isize + 1).min(n - 1).max(0)) as usize;
                true
            }
            // `s` cycles the sort column; `S` flips the current direction.
            KeyCode::Char('s') => {
                use crate::model::dir_listing::SortColumn::*;
                let p = c.pane_mut(active);
                let next = match p.listing.sort_column() {
                    Name => Size,
                    Size => Modified,
                    Modified => Type,
                    Type => Name,
                };
                p.listing.resort(next);
                c.status = Some(format!("Sorted by {next:?}."));
                c.is_error = false;
                true
            }
            KeyCode::Char('S') => {
                let p = c.pane_mut(active);
                let col = p.listing.sort_column();
                p.listing.resort(col);
                true
            }
            _ => false,
        }
    }

    /// Whether the Inspect tab is showing its top-level selectable disk list.
    fn inspect_list_active(&self) -> bool {
        self.current() == TabId::Inspect && self.detail.is_none() && self.opened.is_none()
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
        // Mirror a running backup's shared progress into the visual bar, and
        // capture its terminal result once it finishes.
        if let Some(run) = self.backup.as_ref().and_then(|b| b.run.clone()) {
            let snap = run.lock().ok().map(|p| {
                (
                    p.current_bytes,
                    p.total_bytes,
                    p.finished,
                    p.error.clone(),
                    p.operation.clone(),
                )
            });
            if let Some((cur, total, done, err, op)) = snap {
                if let Some(prog) = self.progress.as_ref() {
                    if let Ok(mut sh) = prog.shared.lock() {
                        sh.current = cur;
                        sh.total = total;
                        sh.done = done;
                    }
                }
                if let Some(b) = self.backup.as_mut() {
                    b.op = op;
                    if done && b.result.is_none() {
                        match err {
                            Some(e) => {
                                b.result = Some(format!("Backup failed: {e}"));
                                b.is_error = true;
                            }
                            None => {
                                b.result = Some("Backup complete.".to_string());
                                b.is_error = false;
                            }
                        }
                    }
                }
            }
        }
        // Mirror a running restore's progress into the visual bar (same shape).
        if let Some(run) = self.restore.as_ref().and_then(|r| r.run.clone()) {
            let snap = run.lock().ok().map(|p| {
                (
                    p.current_bytes,
                    p.total_bytes,
                    p.finished,
                    p.error.clone(),
                    p.operation.clone(),
                )
            });
            if let Some((cur, total, done, err, op)) = snap {
                if let Some(prog) = self.progress.as_ref() {
                    if let Ok(mut sh) = prog.shared.lock() {
                        sh.current = cur;
                        sh.total = total;
                        sh.done = done;
                    }
                }
                if let Some(r) = self.restore.as_mut() {
                    r.op = op;
                    if done && r.result.is_none() {
                        match err {
                            Some(e) => {
                                r.result = Some(format!("Restore failed: {e}"));
                                r.is_error = true;
                            }
                            None => {
                                r.result = Some("Restore complete.".to_string());
                                r.is_error = false;
                            }
                        }
                    }
                }
            }
        }
        // Mirror a running bulk convert (file-count + per-file byte progress).
        if let Some(run) = self.bulk.as_ref().and_then(|b| b.run.clone()) {
            let snap = run.lock().ok().map(|s| {
                (
                    s.current_bytes,
                    s.current_total_bytes,
                    s.finished,
                    s.current_index,
                    s.total_files,
                    s.current_file.clone(),
                    s.succeeded,
                    s.failed,
                )
            });
            if let Some((cur, total, done, idx, n, file, ok, fail)) = snap {
                if let Some(prog) = self.progress.as_ref() {
                    if let Ok(mut sh) = prog.shared.lock() {
                        sh.current = cur;
                        sh.total = total;
                        sh.done = done;
                    }
                }
                if let Some(b) = self.bulk.as_mut() {
                    b.op = format!("{idx}/{n}  {}", basename(&file));
                    if done && b.result.is_none() {
                        b.result = Some(format!("Converted {ok} ok, {fail} failed."));
                        b.is_error = fail > 0;
                    }
                }
            }
        }
        // Mirror a running optical rip's progress into the visual bar.
        #[cfg(feature = "optical")]
        if let Some(run) = self.optical.as_ref().and_then(|o| o.run.clone()) {
            let snap = run.lock().ok().map(|p| {
                (
                    p.current_bytes,
                    p.total_bytes,
                    p.finished,
                    p.error.clone(),
                    p.operation.clone(),
                )
            });
            if let Some((cur, total, done, err, op)) = snap {
                if let Some(prog) = self.progress.as_ref() {
                    if let Ok(mut sh) = prog.shared.lock() {
                        sh.current = cur;
                        sh.total = total;
                        sh.done = done;
                    }
                }
                if let Some(o) = self.optical.as_mut() {
                    o.op = op;
                    if done && o.result.is_none() {
                        match err {
                            Some(e) => {
                                o.result = Some(format!("Rip failed: {e}"));
                                o.is_error = true;
                            }
                            None => {
                                o.result = Some("Rip complete.".to_string());
                                o.is_error = false;
                            }
                        }
                    }
                }
            }
        }
        if let Some(p) = &mut self.progress {
            let cur = p.shared.lock().map(|s| s.current).unwrap_or(0);
            p.tracker.record(cur, &p.label);
        }
        // Poll a running Commander checksum job: mirror progress to the status
        // line while it runs, and pop the results overlay when it finishes.
        if let Some(job) = self.commander.as_ref().and_then(|c| c.checksum.clone()) {
            let (done, report, progress_msg) = match job.lock() {
                Ok(s) if s.finished => (true, Some(build_checksum_lines(&s)), None),
                Ok(s) => (
                    false,
                    None,
                    Some(format!(
                        "Hashing {}/{} - {}",
                        s.done_files, s.total_files, s.current_file
                    )),
                ),
                Err(_) => (false, None, None),
            };
            if let Some(c) = self.commander.as_mut() {
                if done {
                    c.checksum = None;
                    c.checksum_report = report.map(|lines| ChecksumReportView { lines, scroll: 0 });
                    c.status = None;
                    c.is_error = false;
                } else if let Some(m) = progress_msg {
                    c.status = Some(m);
                    c.is_error = false;
                }
            }
        }
        // Poll a background Commander connect: install the remote listing into
        // the target pane when it lands, or surface the error.
        #[cfg(feature = "remote")]
        {
            let pending = self
                .commander
                .as_ref()
                .and_then(|c| c.pending_connect.as_ref().map(|(s, j)| (*s, Arc::clone(j))));
            if let Some((side, job)) = pending {
                let outcome: Option<ConnectResult> = match job.lock() {
                    Ok(mut g) if g.finished => g.result.take(),
                    _ => None,
                };
                if let Some(result) = outcome {
                    if let Some(c) = self.commander.as_mut() {
                        c.pending_connect = None;
                        match result {
                            Ok((browser, target)) => {
                                let addr = browser.addr().to_string();
                                let p = c.pane_mut(side);
                                p.session = None;
                                p.is_host = false;
                                p.optical = None;
                                p.parts.clear();
                                p.pending_img = None;
                                p.listing
                                    .load_root(target.fs, target.root, target.entries, false);
                                p.remote = Some(browser);
                                p.loaded = true;
                                p.sel = 0;
                                p.label = format!("remote {addr}: /");
                                c.status = Some(format!("Connected to {addr}."));
                                c.is_error = false;
                            }
                            Err(e) => {
                                c.status = Some(format!("Connect failed: {e}"));
                                c.is_error = true;
                            }
                        }
                    }
                }
            }
        }
    }

    // --- rendering -------------------------------------------------------

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
            self.draw_explorer(frame, area, ex);
        }
        if let Some(picker) = &self.open_picker {
            picker.draw(frame, area, self.palette, self.border);
        }
        if self.show_help {
            self.draw_help(frame, area);
        }
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

    /// The per-partition filesystem Explorer — a near-full-screen window over
    /// the Inspect tab, with its own breadcrumb, entry list, and footer.
    fn draw_explorer(&self, frame: &mut Frame, area: Rect, ex: &Explorer) {
        let popup = centered_rect(
            area.width.saturating_sub(4).min(124),
            area.height.saturating_sub(2),
            area,
        );
        // Outer window frame so the panes read as one dialog over the Inspect
        // tab (rather than the Inspect layer peeking through at the edges).
        let outer = self.pane_block(&format!("Explorer  {}", ex.part_label), true);
        let inner = outer.inner(popup);
        frame.render_widget(Clear, popup);
        frame.render_widget(outer, popup);

        let has_status = ex.status.is_some();
        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1),                              // crumb
                Constraint::Min(3),                                 // panes
                Constraint::Length(if has_status { 2 } else { 1 }), // footer (+status)
            ])
            .split(inner);

        // Crumb line across the top.
        let vol = ex.volume.as_deref().unwrap_or("");
        let crumb = Line::from(vec![
            Span::styled(" Path: ", self.palette.accent()),
            Span::raw(ex.path_display()),
            Span::styled(
                if vol.is_empty() {
                    String::new()
                } else {
                    format!("   [{vol}]")
                },
                self.palette.dim(),
            ),
            Span::styled(
                match &ex.blessed {
                    Some((_, name)) => format!("   Blessed: {name}"),
                    None => String::new(),
                },
                self.palette.dim(),
            ),
        ]);
        frame.render_widget(Paragraph::new(crumb), rows[0]);

        // Two panes: directory tree (left) + listing (right).
        let cols = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Length(32), Constraint::Min(24)])
            .split(rows[1]);
        self.draw_tree_pane(frame, cols[0], ex);
        self.draw_list_pane(frame, cols[1], ex);

        // Footer (+ status line above it).
        let mut fl = Vec::new();
        if let Some(s) = &ex.status {
            fl.push(Line::styled(format!(" {s}"), self.palette.warn()));
        }
        fl.push(Line::styled(
            " Enter open  e Exp i Imp  n New x Del  m Meta b Bless  f Chk F Rep  t Transform  Esc close",
            self.palette.dim(),
        ));
        frame.render_widget(Paragraph::new(Text::from(fl)), rows[2]);

        // File preview overlay.
        if let Some(pv) = &ex.preview {
            self.draw_preview(frame, area, pv);
        }

        // fsck / repair report overlay.
        if let Some(rv) = &ex.fsck_report {
            let popup = centered_rect(78, area.height.saturating_sub(4).clamp(8, 22), area);
            frame.render_widget(Clear, popup);
            let inner_h = popup.height.saturating_sub(2) as usize;
            let start = rv.scroll.min(rv.lines.len().saturating_sub(1));
            let body: Vec<Line> = rv
                .lines
                .iter()
                .skip(start)
                .take(inner_h.saturating_sub(1))
                .map(|l| {
                    let style = if l.contains("CLEAN") || l.trim_start().starts_with('+') {
                        self.palette.accent()
                    } else if l.starts_with("Result:")
                        || l.contains("error")
                        || l.contains("failed")
                    {
                        self.palette.warn()
                    } else {
                        self.palette.dim()
                    };
                    Line::styled(format!(" {l}"), style)
                })
                .collect();
            let mut lines = body;
            lines.push(Line::styled(
                " Up/Down scroll   Esc close",
                self.palette.dim(),
            ));
            frame.render_widget(
                Paragraph::new(Text::from(lines)).block(self.pane_block(&rv.title, true)),
                popup,
            );
        }

        // Transform launcher menu.
        if let Some(sel) = ex.transform_menu {
            let mut lines: Vec<Line> = vec![Line::raw("")];
            for (i, (label, _)) in TRANSFORMS.iter().enumerate() {
                let on = i == sel;
                let marker = if on { "> " } else { "  " };
                let style = if on {
                    self.palette
                        .accent()
                        .add_modifier(Modifier::REVERSED | Modifier::BOLD)
                } else {
                    self.palette.accent()
                };
                lines.push(Line::styled(format!("{marker}{label}"), style));
            }
            lines.push(Line::raw(""));
            lines.push(Line::styled(
                "  Enter -> prefills the command line; edit and run.  Esc cancel",
                self.palette.dim(),
            ));
            let h = (lines.len() as u16 + 2).min(area.height.saturating_sub(2));
            let popup = centered_rect(66, h, area);
            frame.render_widget(Clear, popup);
            frame.render_widget(
                Paragraph::new(Text::from(lines)).block(self.pane_block("Transform image", true)),
                popup,
            );
        }

        // Metadata editor form.
        if let Some(m) = &ex.metadata {
            let field = |idx: usize, label: &str, val: &str| {
                let cursor = if m.field == idx {
                    Span::styled(" ", self.palette.accent().add_modifier(Modifier::REVERSED))
                } else {
                    Span::raw("")
                };
                Line::from(vec![
                    Span::styled(format!("  {label:<10}"), self.palette.accent()),
                    Span::raw(val.to_string()),
                    cursor,
                ])
            };
            let mut ml = vec![
                Line::styled(format!("  {}", m.entry_name), self.palette.dim()),
                Line::raw(""),
                field(0, "Type:", &m.type_code),
                field(1, "Creator:", &m.creator),
                field(2, "Modified:", &m.modified),
            ];
            if let Some(e) = &m.error {
                ml.push(Line::styled(format!("  {e}"), self.palette.warn()));
            }
            ml.push(Line::raw(""));
            ml.push(Line::styled(
                "  Tab next field   Enter apply   Esc cancel",
                self.palette.dim(),
            ));
            let mp = centered_rect(60, ml.len() as u16 + 2, area);
            frame.render_widget(Clear, mp);
            frame.render_widget(
                Paragraph::new(Text::from(ml)).block(self.pane_block("Edit metadata", true)),
                mp,
            );
        }

        // "Export as" format menu.
        if let Some(sel) = ex.export_menu {
            let mut ml = vec![Line::raw("")];
            for (i, (label, _)) in EXPORT_FORMATS.iter().enumerate() {
                let marker = if i == sel { "> " } else { "  " };
                let style = if i == sel {
                    Style::default().add_modifier(Modifier::REVERSED | Modifier::BOLD)
                } else {
                    Style::default()
                };
                ml.push(Line::styled(format!("{marker}{label}"), style));
            }
            ml.push(Line::styled(
                "  Enter choose   Esc cancel",
                self.palette.dim(),
            ));
            let mp = centered_rect(46, EXPORT_FORMATS.len() as u16 + 4, area);
            frame.render_widget(Clear, mp);
            frame.render_widget(
                Paragraph::new(Text::from(ml)).block(self.pane_block("Export as", true)),
                mp,
            );
        }

        // Export / import file picker (shared FilePicker).
        if let Some(picker) = &ex.picker {
            picker.draw(frame, area, self.palette, self.border);
        }

        if ex.confirm_close {
            let cp = centered_rect(40, 5, area);
            frame.render_widget(Clear, cp);
            frame.render_widget(
                Paragraph::new(Text::from(vec![
                    Line::raw(""),
                    Line::styled("  Close the explorer?  (y / n)", self.palette.warn()),
                ]))
                .block(self.pane_block("Confirm", true)),
                cp,
            );
        }

        // Bless confirmation: name the folder being blessed, and the one it
        // replaces (if any) so the user knows what boot pointer is changing.
        if let Some((_, name)) = &ex.confirm_bless {
            let mut lines = vec![Line::raw("")];
            match &ex.blessed {
                Some((_, cur)) if cur != name => {
                    lines.push(Line::styled(
                        format!("  The current folder \"{cur}\" is blessed."),
                        self.palette.dim(),
                    ));
                    lines.push(Line::styled(
                        format!("  Bless \"{name}\" instead?  (y / n)"),
                        self.palette.warn(),
                    ));
                }
                Some(_) => {
                    lines.push(Line::styled(
                        format!("  \"{name}\" is already blessed. Re-bless it?  (y / n)"),
                        self.palette.warn(),
                    ));
                }
                None => {
                    lines.push(Line::styled(
                        format!("  Bless \"{name}\" as the System Folder?  (y / n)"),
                        self.palette.warn(),
                    ));
                }
            }
            let h = lines.len() as u16 + 2;
            let cp = centered_rect(64, h, area);
            frame.render_widget(Clear, cp);
            frame.render_widget(
                Paragraph::new(Text::from(lines)).block(self.pane_block("Confirm bless", true)),
                cp,
            );
        }

        // New-folder name prompt.
        if let Some(name) = &ex.mkdir_input {
            let cp = centered_rect(50, 5, area);
            frame.render_widget(Clear, cp);
            frame.render_widget(
                Paragraph::new(Text::from(vec![
                    Line::from(vec![
                        Span::styled("  Name: ", self.palette.accent()),
                        Span::raw(name.clone()),
                        Span::styled(" ", self.palette.accent().add_modifier(Modifier::REVERSED)),
                    ]),
                    Line::raw(""),
                    Line::styled("  Enter create   Esc cancel", self.palette.dim()),
                ]))
                .block(self.pane_block("New folder", true)),
                cp,
            );
        }

        // Delete confirmation.
        if let Some((name, is_dir)) = &ex.confirm_delete {
            let cp = centered_rect(56, 5, area);
            frame.render_widget(Clear, cp);
            let what = if *is_dir {
                "folder (and its contents)"
            } else {
                "file"
            };
            frame.render_widget(
                Paragraph::new(Text::from(vec![
                    Line::raw(""),
                    Line::styled(
                        format!("  Delete the {what} \"{name}\"?  (y / n)"),
                        self.palette.warn(),
                    ),
                ]))
                .block(self.pane_block("Confirm delete", true)),
                cp,
            );
        }
    }

    /// Left Explorer pane: the expandable directory tree.
    fn draw_tree_pane(&self, frame: &mut Frame, area: Rect, ex: &Explorer) {
        let focused = ex.focus == ExFocus::Tree;
        let visible = area.height.saturating_sub(2) as usize;
        let start = ex.tree_sel.saturating_sub(visible.saturating_sub(1));
        let mut lines = Vec::new();
        for (i, n) in ex.tree.iter().enumerate().skip(start).take(visible) {
            let indent = "  ".repeat(n.depth);
            let mark = if n.expanded { "[-] " } else { "[+] " };
            let name = if n.depth == 0 {
                "/".to_string()
            } else {
                format!("{}/", n.dir.name)
            };
            // A leading '*' marks Space-selected folders (multi-select export).
            let marked = ex.marked.contains_key(&n.dir.path);
            let selm = if marked { "*" } else { " " };
            let mut style = if i == ex.tree_sel {
                self.palette
                    .accent()
                    .add_modifier(Modifier::REVERSED | Modifier::BOLD)
            } else {
                self.palette.accent()
            };
            if marked {
                style = style.add_modifier(Modifier::BOLD);
            }
            lines.push(Line::styled(format!("{selm}{indent}{mark}{name}"), style));
        }
        let title = if ex.marked.is_empty() {
            "Tree".to_string()
        } else {
            format!("Tree ({} marked)", ex.marked.len())
        };
        frame.render_widget(
            Paragraph::new(Text::from(lines)).block(self.pane_block(&title, focused)),
            area,
        );
    }

    /// Right Explorer pane: the selected directory's listing with detail columns.
    fn draw_list_pane(&self, frame: &mut Frame, area: Rect, ex: &Explorer) {
        let focused = ex.focus == ExFocus::List;
        let visible = (area.height as usize).saturating_sub(3).max(1); // borders + header
        let start = ex.list_sel.saturating_sub(visible.saturating_sub(1));
        let mut lines = vec![Line::styled(
            format!(
                "{:<24.24} {:>9}  {:<17.17} {:<9.9} {}",
                "Name", "Size", "Modified", "Type", "Rsrc"
            ),
            self.palette.dim(),
        )];
        if ex.list.is_empty() {
            lines.push(Line::styled("(empty directory)", self.palette.dim()));
        } else {
            for (i, e) in ex.list.iter().enumerate().skip(start).take(visible) {
                let is_dir = matches!(e.entry_type, EntryType::Directory);
                let name_col = if is_dir {
                    format!("{}/", e.name)
                } else {
                    e.name.clone()
                };
                let date = e.modified.clone().unwrap_or_default();
                let detail = match e.entry_type {
                    EntryType::Symlink => {
                        format!("-> {}", e.symlink_target.as_deref().unwrap_or("?"))
                    }
                    EntryType::Directory => format!("{:>9}  {:<17.17}", "", date),
                    _ => {
                        let ty = type_label(e).unwrap_or_default();
                        let rsrc = match e.resource_fork_size {
                            Some(r) if r > 0 => format!("rsrc {}", format_size(r)),
                            _ => String::new(),
                        };
                        format!(
                            "{:>9}  {:<17.17} {:<9.9} {}",
                            format_size(e.size),
                            date,
                            ty,
                            rsrc
                        )
                    }
                };
                let base = if is_dir {
                    self.palette.accent()
                } else {
                    Style::default()
                };
                let marked = ex.marked.contains_key(&e.path);
                let mut style = if i == ex.list_sel && focused {
                    base.add_modifier(Modifier::REVERSED | Modifier::BOLD)
                } else if i == ex.list_sel {
                    base.add_modifier(Modifier::BOLD)
                } else {
                    base
                };
                if marked {
                    style = style.add_modifier(Modifier::BOLD);
                }
                // A leading '*' marks Space-selected rows (multi-select export).
                let mark = if marked { "*" } else { " " };
                lines.push(Line::styled(
                    format!("{mark}{name_col:<23.23} {detail}"),
                    style,
                ));
            }
        }
        let dir = ex.current_dir();
        let title = if dir.name.is_empty() { "/" } else { &dir.name };
        frame.render_widget(
            Paragraph::new(Text::from(lines)).block(self.pane_block(title, focused)),
            area,
        );
    }

    /// The file preview overlay: a near-full-screen scrollable text / hex view.
    fn draw_preview(&self, frame: &mut Frame, area: Rect, pv: &Preview) {
        let popup = centered_rect(
            area.width.saturating_sub(4).min(120),
            area.height.saturating_sub(2),
            area,
        );
        let inner = popup.height.saturating_sub(2) as usize; // borders
        let body_rows = inner.saturating_sub(1); // reserve a footer row
        let content = pv.lines();
        let total = content.len();
        let start = pv.scroll.min(total.saturating_sub(1));

        let mut lines: Vec<Line> = content
            .iter()
            .skip(start)
            .take(body_rows)
            .map(|l| Line::raw(l.clone()))
            .collect();
        let fork_hint = if pv.rsrc.is_some() {
            "   r data/rsrc fork"
        } else {
            ""
        };
        lines.push(Line::styled(
            format!(
                "Up/Dn scroll   PgUp/PgDn   Home/End{fork_hint}   Esc close ({})",
                pv.name
            ),
            self.palette.dim(),
        ));

        let type_str = pv
            .type_label
            .as_ref()
            .map(|t| format!(" ({t})"))
            .unwrap_or_default();
        let fork = if pv.showing_rsrc {
            "  [resource fork]"
        } else if pv.rsrc.is_some() {
            "  [data fork]"
        } else {
            ""
        };
        let title = format!(
            "View  {}{type_str}{fork}  [{}/{}]",
            pv.name,
            start + 1,
            total.max(1)
        );
        frame.render_widget(Clear, popup);
        frame.render_widget(
            Paragraph::new(Text::from(lines)).block(self.pane_block(&title, true)),
            popup,
        );
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
        if let Some(Opened::Image { path, size, parts }) = &self.opened {
            self.draw_partition_list(frame, area, path, *size, parts);
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
            "Inspect  [{} disk(s)]  Enter=disk  o=open file",
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
    ) {
        let name = basename(path);
        if parts.is_empty() {
            frame.render_widget(
                Paragraph::new("No partitions detected.")
                    .block(self.pane_block(&format!("Image  {name}"), true)),
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
        let title = match &self.status {
            Some(s) => format!("Image  {name} ({})  -  {s}", format_size(size)),
            None => format!(
                "Image  {name} ({})  [{} partition(s)]  Enter=browse",
                format_size(size),
                parts.len()
            ),
        };
        let list = List::new(items)
            .block(self.pane_block(&title, true))
            .highlight_style(Style::default().add_modifier(Modifier::REVERSED | Modifier::BOLD))
            .highlight_symbol("> ");
        let mut state = ListState::default();
        state.select(Some(self.selection.min(parts.len() - 1)));
        frame.render_stateful_widget(list, area, &mut state);
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
                    "Hard disk (x68k / sgi-efs) and CD-ROM images: use the CLI \
                     (`rb-cli new hd ...`) or the Optical tab.",
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
                lines.push(field(1, "Size:", &w.size, "800K"));
                lines.push(field(2, "Name:", &w.name, "rusty-backup"));
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
                    "Up/Down field   Tab browse (path)   Enter create   Esc back",
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
                if !b.source.is_empty() {
                    lines.push(Line::raw(""));
                    lines.push(Line::from(vec![
                        Span::styled("Current source: ", self.palette.dim()),
                        Span::raw(b.source.clone()),
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
                    Span::raw(b.source.clone()),
                    Span::styled(
                        if b.from_device { "  [device]" } else { "" },
                        self.palette.dim(),
                    ),
                ]));
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
                lines.push(row(1, "Name:", b.name.clone(), true));
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
                    Span::raw(b.source.clone()),
                ]));
                lines.push(Line::from(vec![
                    Span::styled("Output: ", self.palette.dim()),
                    Span::raw(format!(
                        "{}/{}  ({}, {})",
                        b.out_dir,
                        b.name,
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
                if let Some(r) = &b.result {
                    lines.push(Line::raw(""));
                    let style = if b.is_error {
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
                    "Choose a backup to restore:",
                    self.palette.accent(),
                ));
                lines.push(Line::raw(""));
                lines.push(Line::from(vec![
                    Span::styled("  Enter / o  ", self.palette.accent()),
                    Span::raw("choose a backup folder or .cbk file"),
                ]));
                if let Some(res) = &r.result {
                    lines.push(Line::raw(""));
                    lines.push(Line::styled(format!("  {res}"), self.palette.warn()));
                }
            }
            RestoreStep::Config => {
                lines.push(Line::from(vec![
                    Span::styled("Backup: ", self.palette.dim()),
                    Span::raw(r.backup_folder.clone()),
                ]));
                lines.push(Line::from(vec![
                    Span::styled("Contents: ", self.palette.dim()),
                    Span::raw(format!(
                        "{} partition(s), original size {}",
                        r.part_count,
                        format_size(r.source_size)
                    )),
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
                    format!("{}[ Start restore ]", if start_sel { "> " } else { "  " }),
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
                let selected = b.files.iter().filter(|f| f.selected).count();
                lines.push(Line::styled(
                    format!(
                        "Files ({} of {} selected)  -  Space toggles:",
                        selected,
                        b.files.len()
                    ),
                    self.palette.accent(),
                ));
                if b.files.is_empty() {
                    lines.push(Line::styled(
                        "  (no matching files in this folder for this format)",
                        self.palette.dim(),
                    ));
                } else {
                    // Window the list so the cursor stays visible.
                    let avail = (area.height as usize).saturating_sub(12).max(3);
                    let cur_file = b.file_at_cursor().unwrap_or(0);
                    let start = cur_file.saturating_sub(avail.saturating_sub(1));
                    for (i, f) in b.files.iter().enumerate().skip(start).take(avail) {
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
    fn draw_commander(&self, frame: &mut Frame, area: Rect, c: &CommanderState) {
        // Body: two panes over a one-line status.
        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(3), Constraint::Length(1)])
            .split(area);
        let cols = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(rows[0]);
        self.draw_commander_pane(frame, cols[0], c.pane(Side::Left), c.active == Side::Left);
        self.draw_commander_pane(frame, cols[1], c.pane(Side::Right), c.active == Side::Right);

        let status = match &c.status {
            Some(s) => s.clone(),
            None => {
                "Tab pane  Enter open  Space mark  c copy  v info  # hash  n mkdir  x del  s sort  p fs  r refresh"
                    .to_string()
            }
        };
        let style = if c.is_error {
            self.palette.warn()
        } else {
            self.palette.dim()
        };
        frame.render_widget(
            Paragraph::new(Line::styled(format!(" {status}"), style)),
            rows[1],
        );

        // A pane's open picker / partition chooser draws on top.
        for side in [Side::Left, Side::Right] {
            let pane = c.pane(side);
            if let Some(picker) = &pane.picker {
                picker.draw(frame, area, self.palette, self.border);
            }
            if pane.part_pick {
                self.draw_commander_partpick(frame, area, pane);
            }
            if pane.optical_pick {
                self.draw_commander_opticalpick(frame, area, pane);
            }
        }

        // New-folder name prompt (active pane).
        if let Some(name) = &c.mkdir_input {
            let cp = centered_rect(50, 5, area);
            frame.render_widget(Clear, cp);
            frame.render_widget(
                Paragraph::new(Text::from(vec![
                    Line::from(vec![
                        Span::styled("  Name: ", self.palette.accent()),
                        Span::raw(name.clone()),
                        Span::styled(" ", self.palette.accent().add_modifier(Modifier::REVERSED)),
                    ]),
                    Line::raw(""),
                    Line::styled("  Enter create   Esc cancel", self.palette.dim()),
                ]))
                .block(self.pane_block("New folder", true)),
                cp,
            );
        }

        // Connect-to-remote host:port prompt (active pane).
        #[cfg(feature = "remote")]
        if let Some(addr) = &c.connect_input {
            let cp = centered_rect(56, 5, area);
            frame.render_widget(Clear, cp);
            frame.render_widget(
                Paragraph::new(Text::from(vec![
                    Line::from(vec![
                        Span::styled("  Host:port: ", self.palette.accent()),
                        Span::raw(addr.clone()),
                        Span::styled(" ", self.palette.accent().add_modifier(Modifier::REVERSED)),
                    ]),
                    Line::raw(""),
                    Line::styled("  Enter connect   Esc cancel", self.palette.dim()),
                ]))
                .block(self.pane_block("Connect to remote", true)),
                cp,
            );
        }

        // Delete confirmation (active pane).
        if let Some(phrase) = &c.confirm_delete {
            let cp = centered_rect(60, 5, area);
            frame.render_widget(Clear, cp);
            frame.render_widget(
                Paragraph::new(Text::from(vec![
                    Line::raw(""),
                    Line::styled(format!("  Delete {phrase}?  (y / n)"), self.palette.warn()),
                ]))
                .block(self.pane_block("Confirm delete", true)),
                cp,
            );
        }

        // Checksum results overlay (scrollable).
        if let Some(rv) = &c.checksum_report {
            let popup = centered_rect(84, area.height.saturating_sub(4).clamp(8, 24), area);
            frame.render_widget(Clear, popup);
            let inner_h = popup.height.saturating_sub(2) as usize;
            let start = rv.scroll.min(rv.lines.len().saturating_sub(1));
            let mut lines: Vec<Line> = rv
                .lines
                .iter()
                .skip(start)
                .take(inner_h.saturating_sub(1))
                .map(|l| {
                    let style = if l.trim_start().starts_with("SHA256")
                        || l.trim_start().starts_with("CRC32")
                    {
                        self.palette.accent()
                    } else if l.contains("failed") || l.starts_with("Error") {
                        self.palette.warn()
                    } else {
                        self.palette.dim()
                    };
                    Line::styled(format!(" {l}"), style)
                })
                .collect();
            lines.push(Line::styled(
                " Up/Down scroll   Esc close",
                self.palette.dim(),
            ));
            frame.render_widget(
                Paragraph::new(Text::from(lines)).block(self.pane_block("Checksums", true)),
                popup,
            );
        }

        // File Info overlay: attributes, with the edit keys along the bottom.
        if c.info.is_some() {
            let body = c.info_lines();
            let h = (body.len() as u16 + 4).clamp(8, area.height.saturating_sub(2));
            let popup = centered_rect(76, h, area);
            frame.render_widget(Clear, popup);
            let mut lines: Vec<Line> = body
                .iter()
                .map(|l| {
                    let style = if l.starts_with("Extended attributes") {
                        self.palette.accent()
                    } else {
                        self.palette.dim()
                    };
                    Line::styled(format!(" {l}"), style)
                })
                .collect();
            lines.push(Line::raw(""));
            lines.push(Line::styled(
                " m mode   o owner   x xattr   Esc close",
                self.palette.dim(),
            ));
            frame.render_widget(
                Paragraph::new(Text::from(lines)).block(self.pane_block("File Info", true)),
                popup,
            );
        }

        // Attribute-edit prompt (innermost overlay).
        if let Some(p) = &c.attr_input {
            let popup = centered_rect(66, 6, area);
            frame.render_widget(Clear, popup);
            frame.render_widget(
                Paragraph::new(Text::from(vec![
                    Line::raw(""),
                    Line::styled(format!("  {}", p.field.label()), self.palette.dim()),
                    Line::styled(format!("  > {}", p.text), self.palette.accent()),
                    Line::styled("  Enter apply   Esc cancel", self.palette.dim()),
                ]))
                .block(self.pane_block("Edit attribute", true)),
                popup,
            );
        }
    }

    fn draw_commander_pane(&self, frame: &mut Frame, area: Rect, pane: &CmdPane, focused: bool) {
        if !pane.loaded {
            let body = Text::from(vec![
                Line::raw(""),
                Line::styled("  Empty pane.", self.palette.dim()),
                Line::raw(""),
                Line::from(vec![
                    Span::styled("  o  ", self.palette.accent()),
                    Span::raw("open a host folder"),
                ]),
                Line::from(vec![
                    Span::styled("  i  ", self.palette.accent()),
                    Span::raw("open an image file"),
                ]),
            ]);
            frame.render_widget(
                Paragraph::new(body).block(self.pane_block("(empty)", focused)),
                area,
            );
            return;
        }
        let rows = pane.listing.current_rows();
        let visible = area.height.saturating_sub(2) as usize;
        let start = pane.sel.saturating_sub(visible.saturating_sub(1));
        let mut lines = Vec::new();
        for (i, row) in rows.iter().enumerate().skip(start).take(visible) {
            let on = focused && i == pane.sel;
            let (text, is_dir, marked) = match row {
                crate::model::dir_listing::Row::Parent => ("..".to_string(), true, false),
                crate::model::dir_listing::Row::Entry(e) => {
                    let marked = pane.listing.is_selected(&e.name);
                    if e.is_directory() {
                        (format!("{}/", e.name), true, marked)
                    } else {
                        (
                            format!("{:<28} {:>9}", e.name, format_size(e.size)),
                            false,
                            marked,
                        )
                    }
                }
            };
            let base = if is_dir {
                self.palette.accent()
            } else {
                Style::default()
            };
            let style = if on {
                base.add_modifier(Modifier::REVERSED | Modifier::BOLD)
            } else if marked {
                base.add_modifier(Modifier::BOLD)
            } else {
                base
            };
            // A leading '*' marks multi-selected entries (Space toggles).
            let prefix = if marked { "*" } else { " " };
            lines.push(Line::styled(format!("{prefix}{text}"), style));
        }
        let title = if pane.label.len() > area.width.saturating_sub(4) as usize {
            format!(
                "...{}",
                &pane.label[pane.label.len().saturating_sub(area.width as usize - 8)..]
            )
        } else {
            pane.label.clone()
        };
        frame.render_widget(
            Paragraph::new(Text::from(lines)).block(self.pane_block(&title, focused)),
            area,
        );
    }

    fn draw_commander_partpick(&self, frame: &mut Frame, area: Rect, pane: &CmdPane) {
        let mut lines: Vec<Line> = vec![Line::raw("")];
        for (i, _p) in pane.parts.iter().enumerate() {
            let sel = i == pane.part_sel;
            let marker = if sel { "> " } else { "  " };
            let style = if sel {
                self.palette
                    .accent()
                    .add_modifier(Modifier::REVERSED | Modifier::BOLD)
            } else {
                self.palette.accent()
            };
            lines.push(Line::styled(format!("{marker}Partition {}", i + 1), style));
        }
        lines.push(Line::raw(""));
        lines.push(Line::styled(
            "Up/Down select   Enter open   Esc cancel",
            self.palette.dim(),
        ));
        let h = (lines.len() as u16 + 2).min(area.height.saturating_sub(2));
        let popup = centered_rect(46, h, area);
        frame.render_widget(Clear, popup);
        frame.render_widget(
            Paragraph::new(Text::from(lines)).block(self.pane_block("Choose partition", true)),
            popup,
        );
    }

    /// The optical-filesystem chooser overlay (ISO 9660 / HFS side of a hybrid
    /// disc). Mirrors `draw_commander_partpick` but lists the disc's filesystems.
    fn draw_commander_opticalpick(&self, frame: &mut Frame, area: Rect, pane: &CmdPane) {
        let Some(op) = &pane.optical else { return };
        let mut lines: Vec<Line> = vec![Line::raw("")];
        for (i, choice) in op.choices.iter().enumerate() {
            let sel = i == pane.optical_sel;
            let marker = if sel { "> " } else { "  " };
            let style = if sel {
                self.palette
                    .accent()
                    .add_modifier(Modifier::REVERSED | Modifier::BOLD)
            } else {
                self.palette.accent()
            };
            lines.push(Line::styled(format!("{marker}{}", choice.label), style));
        }
        lines.push(Line::raw(""));
        lines.push(Line::styled(
            "Up/Down select   Enter open   Esc cancel",
            self.palette.dim(),
        ));
        let h = (lines.len() as u16 + 2).min(area.height.saturating_sub(2));
        let popup = centered_rect(52, h, area);
        frame.render_widget(Clear, popup);
        frame.render_widget(
            Paragraph::new(Text::from(lines))
                .block(self.pane_block("Choose disc filesystem", true)),
            popup,
        );
    }

    /// The Archives screen: open a Mac archive, list its entries, extract.
    fn draw_archive(&self, frame: &mut Frame, area: Rect, a: &ArchiveState) {
        let mut lines: Vec<Line> = Vec::new();
        if a.archive.is_none() {
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
                if o.drives.is_empty() {
                    lines.push(Line::styled(
                        "  No optical drives detected. Press `r` to rescan.",
                        self.palette.dim(),
                    ));
                } else {
                    for (i, d) in o.drives.iter().enumerate() {
                        let sel = i == o.drive_sel;
                        let marker = if sel { "> " } else { "  " };
                        let style = if sel {
                            self.palette
                                .accent()
                                .add_modifier(Modifier::REVERSED | Modifier::BOLD)
                        } else {
                            self.palette.accent()
                        };
                        lines.push(Line::styled(
                            format!("{marker}{}  ({})", d.display_name, d.device_path),
                            style,
                        ));
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

        // Context-sensitive footer.
        let explorer_preview = self
            .explorer
            .as_ref()
            .map(|e| e.preview.is_some())
            .unwrap_or(false);
        let keys: Vec<(&str, &str)> = if explorer_preview {
            vec![("Up/Dn", "Scroll"), ("PgUp/Dn", "Page"), ("Esc", "Close")]
        } else if self.explorer.is_some() {
            vec![
                ("Tab", "Pane"),
                ("Up/Dn", "Move"),
                ("Enter", "Open/View"),
                ("Space", "Mark"),
                ("e/i", "Export/Import"),
                ("Esc", "Close"),
            ]
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
                .map(|a| a.archive.is_some())
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
            vec![
                ("Tab", "Pane"),
                ("Enter", "Open"),
                ("o/i", "Host/Img"),
                ("c", "Copy"),
                ("q", "Quit"),
            ]
        } else if self.current() == TabId::NewDisk {
            let step = self.newdisk.as_ref().map(|w| w.step);
            match step {
                Some(WizStep::Details) => vec![
                    ("<-/->", "Tabs"),
                    ("Up/Dn", "Field"),
                    ("Tab", "Browse"),
                    ("Enter", "Create"),
                    ("Esc", "Back"),
                ],
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
            let mut k = vec![("<-/->", "Tabs"), ("Up/Dn", "Select"), ("Enter", "Open")];
            if self.detail.is_some() || self.opened.is_some() {
                k.push(("Esc", "Back"));
            }
            if self.inspect_list_active() {
                k.push(("o", "Open"));
                k.push(("r", "Rescan"));
            }
            k.push(("?", "Help"));
            k.push(("q", "Quit"));
            k
        };
        frame.render_widget(Paragraph::new(self.key_bar(&keys)), cols[0]);

        // Privilege badge + version, right-aligned (as the GUI shows version).
        let priv_style = if self.elevated {
            self.palette.accent()
        } else {
            self.palette.dim()
        };
        let right = Line::from(vec![
            Span::styled(self.privilege_label(), priv_style),
            Span::styled(format!(" v{} ", env!("APP_VERSION")), self.palette.dim()),
        ]);
        frame.render_widget(Paragraph::new(right).right_aligned(), cols[1]);
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

    fn draw_help(&self, frame: &mut Frame, area: Rect) {
        let body = Text::from(vec![
            Line::raw(""),
            Line::raw("  Tabs (windows)"),
            Line::raw("    Left / Right   Previous / next tab   (also h / l, Tab)"),
            Line::raw("    1 - 9          Jump to a tab by number"),
            Line::raw(""),
            Line::raw("  Within a tab"),
            Line::raw("    Up / Down      Move selection (or scroll)   (also j / k)"),
            Line::raw("    g / G          Top / bottom"),
            Line::raw("    Enter          Open / activate the selection"),
            Line::raw("    Esc            Back (or cancel a running task)"),
            Line::raw(""),
            Line::raw("  Global"),
            Line::raw("    o              Open a file/backup (Inspect): path, recent, Tab=browse"),
            Line::raw("    r              Rescan disks (Inspect)"),
            Line::raw("    ? / F1         Toggle this help        q  Quit"),
            Line::raw(""),
            Line::raw("  Press Esc to close."),
        ]);
        let popup = centered_rect(62, 21, area);
        frame.render_widget(Clear, popup);
        frame.render_widget(
            Paragraph::new(body).block(self.pane_block("Help", true)),
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
            TabId::Commander => self.stub(
                "Commander Mode: a dual-pane file explorer.",
                &[
                    "Two panes, each an image / container OR a host folder.",
                    "Copy between panes: image<->image, host<->image.",
                    "Multi-select, sort, delete, checksum - like the GUI overlay.",
                ],
                "M7 - reuses dir_listing + commander_ops + commander_descend.",
            ),
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
                "Parsed via the shared model::backup_loader (same as the GUI).".to_string(),
                self.palette.dim(),
            ));
            lines.push(Line::raw("Esc to close."));
            return Text::from(lines);
        }
        let Some(i) = self.detail else {
            return Text::from("Select a disk and press Enter, or `o` to open a file/backup.");
        };
        let disks = self.disks.as_deref().unwrap_or(&[]);
        let Some(d) = disks.get(i) else {
            return Text::from("That disk is no longer present. Press Esc.");
        };

        let field = |k: &str, v: String| {
            Line::from(vec![
                Span::styled(format!("{k:<11}"), self.palette.accent()),
                Span::raw(v),
            ])
        };
        let mut lines = vec![
            Line::styled(
                format!("Disk {}", d.path.display()),
                self.palette.accent().add_modifier(Modifier::BOLD),
            ),
            Line::raw(""),
            field(
                "Media:",
                if d.media_name.is_empty() {
                    d.name.clone()
                } else {
                    d.media_name.clone()
                },
            ),
            field("Size:", format_size(d.size_bytes)),
            field("Bus:", d.bus_protocol.clone()),
            field(
                "Flags:",
                format!(
                    "{}{}{}",
                    if d.is_removable { "removable" } else { "fixed" },
                    if d.is_read_only { ", read-only" } else { "" },
                    if d.is_system { ", system" } else { "" },
                ),
            ),
            Line::raw(""),
            Line::styled(
                format!("Mounted partitions ({}):", d.partitions.len()),
                self.palette.accent().add_modifier(Modifier::BOLD),
            ),
        ];
        if d.partitions.is_empty() {
            lines.push(Line::raw("  (none mounted)"));
        } else {
            for p in &d.partitions {
                let used = p.total_space.saturating_sub(p.available_space);
                lines.push(Line::from(vec![
                    Span::styled(format!("  {:<12}", p.name), self.palette.accent()),
                    Span::raw(format!(
                        " {:<8} {} used / {}  {}",
                        p.filesystem,
                        format_size(used),
                        format_size(p.total_space),
                        p.mount_point.display(),
                    )),
                ]));
            }
        }
        lines.push(Line::raw(""));
        lines.push(Line::styled(
            "Full MBR/GPT/APM partition-table parse (via wrapper_tree) lands in M2.".to_string(),
            self.palette.dim(),
        ));
        Text::from(lines)
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

/// Expand a leading `~` / `~/` to the user's home directory.
impl Explorer {
    /// The directory the tree cursor currently sits on.
    fn current_dir(&self) -> &FileEntry {
        &self.tree[self.tree_sel.min(self.tree.len().saturating_sub(1))].dir
    }
    /// Current directory path for the breadcrumb.
    fn path_display(&self) -> String {
        let p = &self.current_dir().path;
        if p.is_empty() {
            "/".to_string()
        } else {
            p.clone()
        }
    }
    /// Path components (dir names) from root to the current directory.
    fn dir_components(&self) -> Vec<String> {
        self.current_dir()
            .path
            .split('/')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect()
    }
    /// The entry highlighted in the right-hand listing.
    fn selected_entry(&self) -> Option<&FileEntry> {
        self.list.get(self.list_sel)
    }
    /// The folder to bless: the highlighted directory in the listing when the
    /// list pane has focus, otherwise the current tree directory (never the
    /// volume root). Returns `(mac_path, leaf_name)`, or `None` when there's no
    /// eligible folder selected.
    fn bless_target(&self) -> Option<(String, String)> {
        match self.focus {
            ExFocus::List => {
                let e = self.selected_entry()?;
                if !e.is_directory() {
                    return None;
                }
                let cur = self.path_display();
                let path = if cur == "/" {
                    format!("/{}", e.name)
                } else {
                    format!("{cur}/{}", e.name)
                };
                Some((path, e.name.clone()))
            }
            ExFocus::Tree => {
                let d = self.current_dir();
                // The volume root is never a bless target — a System Folder is a
                // subdirectory. Its path is empty or "/" depending on the driver.
                if d.path.is_empty() || d.path == "/" {
                    None
                } else {
                    Some((d.path.clone(), d.name.clone()))
                }
            }
        }
    }
    /// Reload the right pane for the selected tree directory. Marks are kept
    /// (keyed by path), so a selection can span directories as you navigate.
    fn reload_list(&mut self) {
        let dir = self.current_dir().clone();
        let mut list = self.fs.list_directory(&dir).unwrap_or_default();
        sort_entries(&mut list);
        self.list = list;
        self.list_sel = 0;
    }

    /// Toggle the Space-mark on the highlighted list row (a file or a folder),
    /// then advance the cursor so a run can be marked with repeated Space presses.
    fn toggle_mark(&mut self) {
        if let Some(e) = self.list.get(self.list_sel) {
            let (path, entry) = (e.path.clone(), e.clone());
            if self.marked.remove(&path).is_none() {
                self.marked.insert(path, entry);
            }
        }
        self.list_move(1);
    }

    /// Toggle the Space-mark on the selected directory in the left tree pane,
    /// so folders can be multi-selected for export without descending into them.
    fn toggle_mark_tree(&mut self) {
        if let Some(n) = self.tree.get(self.tree_sel) {
            let (path, entry) = (n.dir.path.clone(), n.dir.clone());
            if self.marked.remove(&path).is_none() {
                self.marked.insert(path, entry);
            }
        }
    }

    /// Marked entries for export, dropping any entry that lives under another
    /// marked directory (that ancestor's recursive walk already includes it).
    fn marked_entries(&self) -> Vec<FileEntry> {
        fn is_ancestor(a: &str, p: &str) -> bool {
            if a == p {
                return false;
            }
            if a == "/" {
                return p.len() > 1 && p.starts_with('/');
            }
            p.starts_with(a) && p.as_bytes().get(a.len()) == Some(&b'/')
        }
        let paths: Vec<&str> = self.marked.keys().map(String::as_str).collect();
        self.marked
            .iter()
            .filter(|(p, _)| !paths.iter().any(|a| is_ancestor(a, p)))
            .map(|(_, e)| e.clone())
            .collect()
    }
    fn toggle_focus(&mut self) {
        self.focus = match self.focus {
            ExFocus::Tree => ExFocus::List,
            ExFocus::List => ExFocus::Tree,
        };
    }

    // --- tree pane -------------------------------------------------------
    fn tree_move(&mut self, delta: isize) {
        if self.tree.is_empty() {
            return;
        }
        let max = self.tree.len() as isize - 1;
        self.tree_sel = (self.tree_sel as isize + delta).clamp(0, max) as usize;
        self.reload_list();
    }
    /// Expand the selected node: splice its child directories in after it.
    fn tree_expand(&mut self) {
        if self.tree.is_empty() || self.tree[self.tree_sel].expanded {
            return;
        }
        let depth = self.tree[self.tree_sel].depth;
        let dir = self.tree[self.tree_sel].dir.clone();
        let mut kids: Vec<FileEntry> = self
            .fs
            .list_directory(&dir)
            .unwrap_or_default()
            .into_iter()
            .filter(|e| matches!(e.entry_type, EntryType::Directory))
            .collect();
        sort_entries(&mut kids);
        self.tree[self.tree_sel].expanded = true;
        let nodes: Vec<TreeNode> = kids
            .into_iter()
            .map(|d| TreeNode {
                dir: d,
                depth: depth + 1,
                expanded: false,
            })
            .collect();
        let at = self.tree_sel + 1;
        self.tree.splice(at..at, nodes);
    }
    /// Collapse the selected node (removing its descendants), or if already
    /// collapsed, jump to its parent.
    fn tree_collapse(&mut self) {
        if self.tree.is_empty() {
            return;
        }
        if self.tree[self.tree_sel].expanded {
            let depth = self.tree[self.tree_sel].depth;
            let start = self.tree_sel + 1;
            let mut end = start;
            while end < self.tree.len() && self.tree[end].depth > depth {
                end += 1;
            }
            self.tree.drain(start..end);
            self.tree[self.tree_sel].expanded = false;
        } else if self.tree[self.tree_sel].depth > 0 {
            let depth = self.tree[self.tree_sel].depth;
            let mut i = self.tree_sel;
            while i > 0 {
                i -= 1;
                if self.tree[i].depth < depth {
                    break;
                }
            }
            self.tree_sel = i;
            self.reload_list();
        }
    }
    fn tree_first(&mut self) {
        self.tree_sel = 0;
        self.reload_list();
    }
    fn tree_last(&mut self) {
        self.tree_sel = self.tree.len().saturating_sub(1);
        self.reload_list();
    }

    // --- list pane -------------------------------------------------------
    fn list_move(&mut self, delta: isize) {
        if self.list.is_empty() {
            return;
        }
        let max = self.list.len() as isize - 1;
        self.list_sel = (self.list_sel as isize + delta).clamp(0, max) as usize;
    }
    fn list_first(&mut self) {
        self.list_sel = 0;
    }
    fn list_last(&mut self) {
        self.list_sel = self.list.len().saturating_sub(1);
    }
    /// Enter the selected listing row: descend into a directory (syncing the
    /// tree) or view a file.
    fn list_enter(&mut self) {
        let Some(entry) = self.list.get(self.list_sel).cloned() else {
            return;
        };
        if matches!(entry.entry_type, EntryType::Directory) {
            if !self.tree[self.tree_sel].expanded {
                self.tree_expand();
            }
            if let Some(idx) = self.tree.iter().position(|n| n.dir.path == entry.path) {
                self.tree_sel = idx;
                self.reload_list();
            }
        } else {
            self.preview = Some(self.build_preview(&entry));
        }
    }
    /// Read up to 128 KiB of each fork and render text / hex lines. The
    /// resource fork is captured too (when present) so the View can toggle to it.
    fn build_preview(&mut self, entry: &FileEntry) -> Preview {
        const LIMIT: usize = 128 * 1024;
        let name = entry.name.clone();
        let type_label = type_label(entry);
        let data = match self.fs.read_file(entry, LIMIT) {
            Ok(mut b) => {
                let truncated = b.len() >= LIMIT;
                b.truncate(LIMIT);
                let mut lines = bytes_to_lines(&b);
                if truncated {
                    lines.push("... (truncated at 128 KiB)".to_string());
                }
                lines
            }
            Err(e) => vec![format!("(cannot read: {e})")],
        };
        // Capture the resource fork if the file has one.
        let rsrc = if entry.resource_fork_size.unwrap_or(0) > 0 {
            let mut buf = Vec::new();
            match self.fs.write_resource_fork_to(entry, &mut buf) {
                Ok(_) => {
                    buf.truncate(LIMIT);
                    Some(bytes_to_lines(&buf))
                }
                Err(e) => Some(vec![format!("(cannot read resource fork: {e})")]),
            }
        } else {
            None
        };
        Preview {
            name,
            type_label,
            data,
            rsrc,
            showing_rsrc: false,
            scroll: 0,
        }
    }
}

/// Import a host file into `cur_dir` of the given partition: open it read-write
/// (the shared `resolve` + `open_editable_filesystem` + `create_file` path the
/// `put` verb uses), write the file, and commit. Returns the created name.
/// Open the partition read-write and set the file's Type/Creator and (when
/// given) modified date, then commit. Errors (e.g. a non-HFS filesystem) bubble
/// up to the editor's status line.
fn apply_metadata_edit(
    image_path: &str,
    selector: Option<u32>,
    cur_dir: &str,
    name: &str,
    type_code: &str,
    creator: &str,
    modify_mac: Option<u32>,
) -> anyhow::Result<()> {
    use crate::cli::resolve::resolve_partition_rw_forced;
    let dst = if cur_dir == "/" {
        format!("/{name}")
    } else {
        format!("{cur_dir}/{name}")
    };
    let (file, ctx, commit) =
        resolve_partition_rw_forced(std::path::Path::new(image_path), selector, None)?;
    let mut fs = crate::fs::open_editable_filesystem(
        file,
        ctx.offset,
        ctx.type_byte,
        ctx.type_string.as_deref(),
    )
    .map_err(|e| anyhow::anyhow!("opening filesystem for write: {e}"))?;
    let entry = crate::cli::verbs::ls::resolve_path(fs.as_filesystem_mut(), &dst)?;
    fs.set_type_creator(&entry, type_code, creator)
        .map_err(|e| anyhow::anyhow!("set type/creator: {e}"))?;
    if let Some(modify) = modify_mac {
        let (create, _, backup) = entry.mac_dates.unwrap_or((modify, modify, modify));
        fs.set_dates(&entry, create, modify, backup)
            .map_err(|e| anyhow::anyhow!("set dates: {e}"))?;
    }
    fs.sync_metadata()
        .map_err(|e| anyhow::anyhow!("sync_metadata: {e}"))?;
    drop(fs);
    commit.commit()?;
    Ok(())
}

/// Open the partition read-write and run the filesystem's in-place repair, then
/// commit. Mirrors the GUI browse view's repair (open_editable -> repair ->
/// commit); errors (unsupported fs, CHD source, etc.) bubble up.
fn apply_repair(
    image_path: &str,
    selector: Option<u32>,
) -> anyhow::Result<crate::fs::RepairReport> {
    use crate::cli::resolve::resolve_partition_rw_forced;
    let (file, ctx, commit) =
        resolve_partition_rw_forced(std::path::Path::new(image_path), selector, None)?;
    let mut efs = crate::fs::open_editable_filesystem(
        file,
        ctx.offset,
        ctx.type_byte,
        ctx.type_string.as_deref(),
    )
    .map_err(|e| anyhow::anyhow!("opening filesystem for repair: {e}"))?;
    let report = efs.repair().map_err(|e| anyhow::anyhow!("repair: {e}"))?;
    efs.sync_metadata()
        .map_err(|e| anyhow::anyhow!("sync_metadata: {e}"))?;
    drop(efs);
    commit.commit()?;
    Ok(report)
}

/// Format an [`FsckResult`](crate::fs::FsckResult) into display lines for the
/// Explorer's report overlay.
fn format_fsck_lines(result: &crate::fs::FsckResult) -> Vec<String> {
    let mut lines = Vec::new();
    let s = &result.stats;
    lines.push(format!(
        "{} files / {} dirs checked",
        s.files_checked, s.directories_checked
    ));
    for (label, value) in &s.extra {
        lines.push(format!("  {label}: {value}"));
    }
    lines.push(String::new());
    if result.is_clean() {
        lines.push("Result: CLEAN - no errors found.".to_string());
    } else {
        lines.push(format!("Result: {} error(s).", result.errors.len()));
    }
    if result.repairable {
        lines.push("Some errors are repairable - press F to repair.".to_string());
    }
    if !result.errors.is_empty() {
        lines.push(String::new());
        lines.push("Errors:".to_string());
        for e in result.errors.iter().filter(|e| !e.debug) {
            lines.push(format!("  [{}] {}", e.code, e.message));
        }
    }
    if !result.warnings.is_empty() {
        lines.push(String::new());
        lines.push("Warnings:".to_string());
        for w in result.warnings.iter().filter(|w| !w.debug) {
            lines.push(format!("  [{}] {}", w.code, w.message));
        }
    }
    if !result.orphaned_entries.is_empty() {
        lines.push(String::new());
        lines.push(format!(
            "Orphaned entries (unrepairable): {}",
            result.orphaned_entries.len()
        ));
        for o in result.orphaned_entries.iter().take(20) {
            let kind = if o.is_directory { "dir " } else { "file" };
            lines.push(format!(
                "  {kind} {} (parent {})",
                o.name, o.missing_parent_id
            ));
        }
    }
    lines
}

/// Format a [`RepairReport`](crate::fs::RepairReport) into display lines.
fn format_repair_lines(report: &crate::fs::RepairReport) -> Vec<String> {
    let mut lines = Vec::new();
    lines.push(format!(
        "Applied {} fix(es); {} failed; {} unrepairable.",
        report.fixes_applied.len(),
        report.fixes_failed.len(),
        report.unrepairable_count
    ));
    if !report.fixes_applied.is_empty() {
        lines.push(String::new());
        lines.push("Fixes applied:".to_string());
        for f in &report.fixes_applied {
            lines.push(format!("  + {f}"));
        }
    }
    if !report.fixes_failed.is_empty() {
        lines.push(String::new());
        lines.push("Fixes failed:".to_string());
        for f in &report.fixes_failed {
            lines.push(format!("  - {f}"));
        }
    }
    lines
}

/// Format a finished [`ChecksumStatus`](crate::model::checksum::ChecksumStatus)
/// into display lines for the Commander's results overlay: each file's SHA256
/// and CRC32 (or the error that stopped it), plus any fatal job error.
fn build_checksum_lines(status: &crate::model::checksum::ChecksumStatus) -> Vec<String> {
    let mut lines = Vec::new();
    if let Some(e) = &status.error {
        lines.push(format!("Error: {e}"));
        lines.push(String::new());
    }
    let ok = status.results.iter().filter(|r| r.set.is_some()).count();
    lines.push(format!("Hashed {ok}/{} file(s).", status.results.len()));
    for r in &status.results {
        lines.push(String::new());
        lines.push(format!("{}  ({})", r.name, format_size(r.size)));
        match (&r.set, &r.error) {
            (Some(set), _) => {
                lines.push(format!("  SHA256  {}", set.sha256_hex()));
                lines.push(format!("  CRC32   {}", set.crc32_hex()));
            }
            (None, Some(err)) => lines.push(format!("  failed: {err}")),
            (None, None) => lines.push("  (no result)".to_string()),
        }
    }
    lines
}

/// Open the partition read-write and bless the directory at `dir_path` as the
/// bootable System Folder, then commit. Mirrors the `bless set` verb's core
/// (`set_blessed_folder`); errors (non-HFS filesystem, missing folder) bubble up
/// to the Explorer status line.
fn apply_bless_folder(
    image_path: &str,
    selector: Option<u32>,
    dir_path: &str,
) -> anyhow::Result<()> {
    use crate::cli::resolve::resolve_partition_rw_forced;
    let (file, ctx, commit) =
        resolve_partition_rw_forced(std::path::Path::new(image_path), selector, None)?;
    let mut fs = crate::fs::open_editable_filesystem(
        file,
        ctx.offset,
        ctx.type_byte,
        ctx.type_string.as_deref(),
    )
    .map_err(|e| anyhow::anyhow!("opening filesystem for write: {e}"))?;
    let entry = crate::cli::verbs::ls::resolve_path(fs.as_filesystem_mut(), dir_path)?;
    if !entry.is_directory() {
        anyhow::bail!("{dir_path} is not a directory");
    }
    fs.set_blessed_folder(&entry)
        .map_err(|e| anyhow::anyhow!("set_blessed_folder: {e}"))?;
    fs.sync_metadata()
        .map_err(|e| anyhow::anyhow!("sync_metadata: {e}"))?;
    drop(fs);
    commit.commit()?;
    Ok(())
}

/// Open the partition read-write and create `name` as a subdirectory of the
/// Explorer's current directory, then commit.
fn apply_mkdir(
    image_path: &str,
    selector: Option<u32>,
    cur_dir: &str,
    name: &str,
) -> anyhow::Result<()> {
    use crate::cli::resolve::resolve_partition_rw_forced;
    let (file, ctx, commit) =
        resolve_partition_rw_forced(std::path::Path::new(image_path), selector, None)?;
    let mut fs = crate::fs::open_editable_filesystem(
        file,
        ctx.offset,
        ctx.type_byte,
        ctx.type_string.as_deref(),
    )
    .map_err(|e| anyhow::anyhow!("opening filesystem for write: {e}"))?;
    let parent = if cur_dir == "/" {
        fs.root()
            .map_err(|e| anyhow::anyhow!("reading root: {e}"))?
    } else {
        crate::cli::verbs::ls::resolve_path(fs.as_filesystem_mut(), cur_dir)?
    };
    fs.create_directory(
        &parent,
        name,
        &crate::fs::filesystem::CreateDirectoryOptions::default(),
    )
    .map_err(|e| anyhow::anyhow!("create_directory: {e}"))?;
    fs.sync_metadata()
        .map_err(|e| anyhow::anyhow!("sync_metadata: {e}"))?;
    drop(fs);
    commit.commit()?;
    Ok(())
}

/// Open the partition read-write and delete `name` from the Explorer's current
/// directory (recursively for a folder), then commit.
fn apply_delete(
    image_path: &str,
    selector: Option<u32>,
    cur_dir: &str,
    name: &str,
) -> anyhow::Result<()> {
    use crate::cli::resolve::resolve_partition_rw_forced;
    let (file, ctx, commit) =
        resolve_partition_rw_forced(std::path::Path::new(image_path), selector, None)?;
    let mut fs = crate::fs::open_editable_filesystem(
        file,
        ctx.offset,
        ctx.type_byte,
        ctx.type_string.as_deref(),
    )
    .map_err(|e| anyhow::anyhow!("opening filesystem for write: {e}"))?;
    let parent = if cur_dir == "/" {
        fs.root()
            .map_err(|e| anyhow::anyhow!("reading root: {e}"))?
    } else {
        crate::cli::verbs::ls::resolve_path(fs.as_filesystem_mut(), cur_dir)?
    };
    let child_path = if cur_dir == "/" {
        format!("/{name}")
    } else {
        format!("{cur_dir}/{name}")
    };
    let entry = crate::cli::verbs::ls::resolve_path(fs.as_filesystem_mut(), &child_path)?;
    fs.delete_recursive(&parent, &entry)
        .map_err(|e| anyhow::anyhow!("delete: {e}"))?;
    fs.sync_metadata()
        .map_err(|e| anyhow::anyhow!("sync_metadata: {e}"))?;
    drop(fs);
    commit.commit()?;
    Ok(())
}

fn import_host_file(
    image_path: &str,
    selector: Option<u32>,
    cur_dir: &str,
    host: &std::path::Path,
) -> anyhow::Result<String> {
    use crate::cli::resolve::resolve_partition_rw_forced;
    let meta = std::fs::metadata(host)?;
    if !meta.is_file() {
        anyhow::bail!("not a file: {}", host.display());
    }
    let name = host
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .ok_or_else(|| anyhow::anyhow!("no filename in {}", host.display()))?;
    let dst = if cur_dir == "/" {
        format!("/{name}")
    } else {
        format!("{cur_dir}/{name}")
    };

    let (file, ctx, commit) =
        resolve_partition_rw_forced(std::path::Path::new(image_path), selector, None)?;
    let mut fs = crate::fs::open_editable_filesystem(
        file,
        ctx.offset,
        ctx.type_byte,
        ctx.type_string.as_deref(),
    )
    .map_err(|e| anyhow::anyhow!("opening filesystem for write: {e}"))?;

    let (parent, leaf) = crate::cli::verbs::ls::resolve_parent(fs.as_filesystem_mut(), &dst)?;
    if !parent.is_directory() {
        anyhow::bail!("parent is not a directory");
    }
    let exists = fs
        .list_directory(&parent)
        .map_err(|e| anyhow::anyhow!("list_directory: {e}"))?
        .into_iter()
        .any(|e| e.name == leaf);
    if exists {
        anyhow::bail!("{leaf} already exists");
    }

    let len = meta.len();
    let mut hf = std::fs::File::open(host)?;
    let options = crate::fs::filesystem::CreateFileOptions::default();
    fs.create_file(&parent, &leaf, &mut hf, len, &options)
        .map_err(|e| anyhow::anyhow!("create_file: {e}"))?;
    fs.sync_metadata()
        .map_err(|e| anyhow::anyhow!("sync_metadata: {e}"))?;
    drop(fs);
    commit.commit()?;
    Ok(name)
}

/// A short filetype label for a file entry: HFS/HFS+/MFS `TYPE/crea`, a ProDOS
/// `$XX` type, or `None` on filesystems without file types (FAT, ext, …).
fn type_label(e: &FileEntry) -> Option<String> {
    let t = e.type_code_display()?;
    match e.creator_code_display() {
        Some(c) => Some(format!("{t}/{c}")),
        None => Some(t),
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

/// Directories first, then case-insensitive by name.
fn sort_entries(entries: &mut [FileEntry]) {
    entries.sort_by(|a, b| {
        let ad = matches!(a.entry_type, EntryType::Directory);
        let bd = matches!(b.entry_type, EntryType::Directory);
        bd.cmp(&ad)
            .then_with(|| a.name.to_lowercase().cmp(&b.name.to_lowercase()))
    });
}

/// Parse an image's partition table into displayable rows, mirroring the shared
/// `resolve` semantics: `@N` selects `partitions()[N-1]`, and an image with no
/// table is a single superfloppy volume (selector `None`). `total` is the file
/// size, used for the superfloppy row.
fn parse_partitions(path: &std::path::Path, total: u64) -> Vec<PartRow> {
    use crate::partition::PartitionTable;
    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(_) => return Vec::new(),
    };
    let mut reader = std::io::BufReader::new(file);
    match PartitionTable::detect(&mut reader) {
        Ok(pt) => {
            let parts = pt.partitions();
            if parts.is_empty() {
                return vec![PartRow {
                    selector: None,
                    label: format!("Whole volume ({})", pt.type_name()),
                    fs_hint: pt.type_name().to_string(),
                    size: total,
                }];
            }
            parts
                .iter()
                .enumerate()
                .map(|(k, p)| {
                    let n = (k + 1) as u32;
                    let fs_hint = p
                        .partition_type_string
                        .clone()
                        .unwrap_or_else(|| p.type_name.clone());
                    PartRow {
                        selector: Some(n),
                        label: format!("#{n}  {}", p.type_name),
                        fs_hint,
                        size: p.size_bytes,
                    }
                })
                .collect()
        }
        // No detectable table: a raw single-volume image (superfloppy).
        Err(_) => vec![PartRow {
            selector: None,
            label: "Whole volume (no partition table)".to_string(),
            fs_hint: String::new(),
            size: total,
        }],
    }
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

/// Run `f` with the process's stdout AND stderr redirected to the null device,
/// so shared code that prints advisories (`new::run` logs to both streams
/// depending on the fs) doesn't scribble on the TUI's alternate screen. Safe
/// because ratatui isn't writing during the synchronous call. Restores both
/// afterward. No-op off Unix.
#[cfg(unix)]
fn with_stderr_suppressed<T>(f: impl FnOnce() -> T) -> T {
    use std::os::unix::io::AsRawFd;
    unsafe {
        // Flush ratatui's buffered stdout before swapping the fd out.
        use std::io::Write;
        let _ = std::io::stdout().flush();
        let saved_out = libc::dup(libc::STDOUT_FILENO);
        let saved_err = libc::dup(libc::STDERR_FILENO);
        if saved_out < 0 || saved_err < 0 {
            if saved_out >= 0 {
                libc::close(saved_out);
            }
            if saved_err >= 0 {
                libc::close(saved_err);
            }
            return f();
        }
        match std::fs::OpenOptions::new().write(true).open("/dev/null") {
            Ok(devnull) => {
                let nfd = devnull.as_raw_fd();
                libc::dup2(nfd, libc::STDOUT_FILENO);
                libc::dup2(nfd, libc::STDERR_FILENO);
                let out = f();
                let _ = std::io::stdout().flush();
                libc::dup2(saved_out, libc::STDOUT_FILENO);
                libc::dup2(saved_err, libc::STDERR_FILENO);
                libc::close(saved_out);
                libc::close(saved_err);
                out
            }
            Err(_) => {
                libc::close(saved_out);
                libc::close(saved_err);
                f()
            }
        }
    }
}

#[cfg(not(unix))]
fn with_stderr_suppressed<T>(f: impl FnOnce() -> T) -> T {
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
