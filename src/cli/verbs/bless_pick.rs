//! `rb-cli bless pick IMG@N` — interactive, arrow-key folder picker for
//! choosing which directory to bless as the bootable System Folder.
//!
//! The terminal-free navigation logic lives in [`PickerState`] (unit-tested
//! below); the crossterm event loop in [`run`] is a thin shell around it.
//!
//! MiSTer notes: this is built to run on the FPGA's Linux framebuffer console
//! (launched from the Scripts menu with a USB keyboard) as well as over SSH.
//! Rendering is pure ASCII — the console font has no box-drawing/arrow glyphs.
//! When stdin/stdout isn't an interactive TTY (a piped or automated script
//! run) we refuse raw mode and point the user at `bless set` instead, and a
//! RAII guard always restores the terminal so a botched exit can't wedge the
//! console.

use std::io::{self, IsTerminal, Write};

use anyhow::{anyhow, bail, Result};
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::style::{Attribute, Print, SetAttribute};
use crossterm::{cursor, execute, queue, terminal};

use super::bless::{apply_bless, BlessPickArgs};
use crate::cli::logging::{log_stderr, out_stdout};
use crate::cli::resolve::resolve_partition_streaming;
use crate::fs::entry::FileEntry;
use crate::fs::filesystem::Filesystem;

/// Smallest list viewport we'll render, even on a tiny console.
const MIN_VIEWPORT: usize = 3;
/// Fallback terminal dimensions when `terminal::size()` is unavailable
/// (some MiSTer console setups report nothing useful).
const FALLBACK_COLS: u16 = 80;
const FALLBACK_ROWS: u16 = 24;
/// Header lines (title + selected-path) + footer lines (rule + help) that
/// frame the scrolling list.
const CHROME_ROWS: usize = 5;

pub fn run(args: BlessPickArgs) -> Result<()> {
    // Refuse to drive a TUI on a non-interactive stream. This keeps automated
    // MiSTer scripts (piped stdin / no controlling terminal) from hanging or
    // corrupting their output; they should call `bless set <PATH>` instead.
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        bail!(
            "bless pick needs an interactive terminal; \
             use `rb-cli bless set <IMG@N> <PATH>` for scripts"
        );
    }

    let (reader, ctx) = resolve_partition_streaming(&args.image.path, args.image.partition)?;
    log_stderr(&ctx.label);
    let mut fs = crate::fs::open_filesystem(
        reader,
        ctx.offset,
        ctx.type_byte,
        ctx.type_string.as_deref(),
    )
    .map_err(|e| anyhow!("opening filesystem: {e}"))?;

    let root = fs.root().map_err(|e| anyhow!("root: {e}"))?;
    let root_dirs = load_child_dirs(&mut *fs, &root);
    if root_dirs.is_empty() {
        bail!("no folders to bless on this volume");
    }

    let mut state = PickerState::new(root_dirs);
    let chosen = event_loop(&mut state, &mut *fs)?;
    // Drop the read-only handle before `apply_bless` re-opens read-write.
    drop(fs);

    match chosen {
        Some(path) => {
            apply_bless(&args.image, &path)?;
            out_stdout(format!("Blessed {path}"));
        }
        None => log_stderr("bless pick: cancelled, no changes made"),
    }
    Ok(())
}

/// List a directory's child *folders* (files are never bless targets),
/// sorted case-insensitively for stable display. Errors collapse to empty
/// (the folder is then treated as a leaf).
fn load_child_dirs(fs: &mut dyn Filesystem, parent: &FileEntry) -> Vec<FileEntry> {
    let mut dirs: Vec<FileEntry> = match fs.list_directory(parent) {
        Ok(children) => children.into_iter().filter(|e| e.is_directory()).collect(),
        Err(_) => Vec::new(),
    };
    dirs.sort_by_key(|e| e.name.to_lowercase());
    dirs
}

// ---------------------------------------------------------------------------
// Pure navigation core (no terminal I/O — unit-tested at the bottom).
// ---------------------------------------------------------------------------

/// Whether a node's children have been examined yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChildState {
    /// Never expanded — we don't yet know if it has subfolders.
    Unknown,
    /// Expanded once and found to have no subfolders (a leaf).
    Leaf,
    /// Children are known (currently expanded or collapsed).
    Loaded,
}

/// One row in the flattened, partially-expanded directory tree.
pub struct Node {
    entry: FileEntry,
    name: String,
    depth: usize,
    /// Full Mac path, e.g. `/System Folder/Extensions`.
    path: String,
    expanded: bool,
    child_state: ChildState,
}

impl Node {
    fn new(entry: FileEntry, depth: usize, parent_path: &str) -> Self {
        let name = entry.name.clone();
        let path = format!("{parent_path}/{name}");
        Node {
            entry,
            name,
            depth,
            path,
            expanded: false,
            child_state: ChildState::Unknown,
        }
    }
}

/// Terminal-independent state machine for the picker: a flattened tree plus a
/// selection cursor and scroll window. All terminal rendering reads from here.
pub struct PickerState {
    nodes: Vec<Node>,
    selected: usize,
    /// Index of the first visible row.
    scroll: usize,
    /// Number of list rows the terminal can show.
    viewport: usize,
}

impl PickerState {
    pub fn new(root_dirs: Vec<FileEntry>) -> Self {
        let nodes = root_dirs.into_iter().map(|e| Node::new(e, 0, "")).collect();
        PickerState {
            nodes,
            selected: 0,
            scroll: 0,
            viewport: MIN_VIEWPORT,
        }
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn selected_index(&self) -> usize {
        self.selected
    }

    /// Full path of the currently selected folder.
    pub fn selected_path(&self) -> &str {
        &self.nodes[self.selected].path
    }

    /// Resize the visible window (e.g. on terminal resize). Always leaves at
    /// least [`MIN_VIEWPORT`] rows and keeps the selection in view.
    pub fn set_viewport(&mut self, rows: usize) {
        self.viewport = rows.max(MIN_VIEWPORT);
        self.clamp_scroll();
    }

    pub fn move_up(&mut self) {
        if self.selected > 0 {
            self.selected -= 1;
            self.clamp_scroll();
        }
    }

    pub fn move_down(&mut self) {
        if self.selected + 1 < self.nodes.len() {
            self.selected += 1;
            self.clamp_scroll();
        }
    }

    /// Keep `selected` within the `[scroll, scroll + viewport)` window.
    fn clamp_scroll(&mut self) {
        if self.selected < self.scroll {
            self.scroll = self.selected;
        } else if self.selected >= self.scroll + self.viewport {
            self.scroll = self.selected + 1 - self.viewport;
        }
        // Don't scroll past the end when the list shrinks (after collapse).
        let max_scroll = self.nodes.len().saturating_sub(self.viewport);
        if self.scroll > max_scroll {
            self.scroll = max_scroll;
        }
    }

    /// Inclusive-exclusive range of node indices currently visible.
    pub fn visible_range(&self) -> (usize, usize) {
        let end = (self.scroll + self.viewport).min(self.nodes.len());
        (self.scroll, end)
    }

    /// Right-arrow: expand the selected folder, loading its child folders via
    /// `loader`. If already expanded, step into the first child. Loading an
    /// empty folder marks it a leaf. `loader` returns the child folders of the
    /// given entry (already filtered to directories).
    pub fn expand<F>(&mut self, mut loader: F)
    where
        F: FnMut(&FileEntry) -> Vec<FileEntry>,
    {
        let idx = self.selected;
        if self.nodes[idx].expanded {
            // Already open — move into the first child if there is one.
            if idx + 1 < self.nodes.len() && self.nodes[idx + 1].depth > self.nodes[idx].depth {
                self.move_down();
            }
            return;
        }
        if self.nodes[idx].child_state == ChildState::Leaf {
            return;
        }
        let parent_depth = self.nodes[idx].depth;
        let parent_path = self.nodes[idx].path.clone();
        let children = loader(&self.nodes[idx].entry);
        if children.is_empty() {
            self.nodes[idx].child_state = ChildState::Leaf;
            return;
        }
        let new_nodes: Vec<Node> = children
            .into_iter()
            .map(|e| Node::new(e, parent_depth + 1, &parent_path))
            .collect();
        // Splice the children in right after the parent.
        let tail = self.nodes.split_off(idx + 1);
        self.nodes.extend(new_nodes);
        self.nodes.extend(tail);
        self.nodes[idx].expanded = true;
        self.nodes[idx].child_state = ChildState::Loaded;
        self.clamp_scroll();
    }

    /// Left-arrow: collapse the selected folder if open; otherwise jump to its
    /// parent. Collapsing removes the contiguous block of deeper descendants.
    pub fn collapse(&mut self) {
        let idx = self.selected;
        if self.nodes[idx].expanded {
            let depth = self.nodes[idx].depth;
            let mut end = idx + 1;
            while end < self.nodes.len() && self.nodes[end].depth > depth {
                end += 1;
            }
            self.nodes.drain(idx + 1..end);
            self.nodes[idx].expanded = false;
            self.clamp_scroll();
            return;
        }
        // Not expanded: move selection up to the parent (nearest shallower row).
        let depth = self.nodes[idx].depth;
        if depth == 0 {
            return;
        }
        for i in (0..idx).rev() {
            if self.nodes[i].depth < depth {
                self.selected = i;
                self.clamp_scroll();
                return;
            }
        }
    }

    /// Render data for one visible row: (indent depth, expand marker, name,
    /// is_selected).
    fn row_view(&self, idx: usize) -> RowView<'_> {
        let n = &self.nodes[idx];
        let marker = match (n.child_state, n.expanded) {
            (ChildState::Leaf, _) => "   ",
            (_, true) => "[-]",
            _ => "[+]",
        };
        RowView {
            depth: n.depth,
            marker,
            name: &n.name,
            selected: idx == self.selected,
        }
    }
}

struct RowView<'a> {
    depth: usize,
    marker: &'a str,
    name: &'a str,
    selected: bool,
}

// ---------------------------------------------------------------------------
// crossterm shell
// ---------------------------------------------------------------------------

/// RAII terminal mode guard: enters raw mode + the alternate screen on
/// construction and unconditionally restores both (and the cursor) on drop,
/// including during unwinding — critical so a panic can't wedge the console.
struct TermGuard;

impl TermGuard {
    fn enter() -> io::Result<Self> {
        terminal::enable_raw_mode()?;
        execute!(io::stdout(), terminal::EnterAlternateScreen, cursor::Hide)?;
        Ok(TermGuard)
    }
}

impl Drop for TermGuard {
    fn drop(&mut self) {
        let _ = execute!(io::stdout(), cursor::Show, terminal::LeaveAlternateScreen);
        let _ = terminal::disable_raw_mode();
    }
}

/// Compute the list viewport (rows available for folders) from the terminal
/// height, falling back to a sane default if the size query fails.
fn viewport_from_terminal() -> usize {
    let (_, rows) = terminal::size().unwrap_or((FALLBACK_COLS, FALLBACK_ROWS));
    (rows as usize)
        .saturating_sub(CHROME_ROWS)
        .max(MIN_VIEWPORT)
}

/// Drive the picker until the user blesses a folder (returns `Some(path)`) or
/// cancels (`None`). `fs` is used to lazily load child folders on expand.
fn event_loop(state: &mut PickerState, fs: &mut dyn Filesystem) -> Result<Option<String>> {
    let _guard = TermGuard::enter().map_err(|e| anyhow!("entering raw terminal mode: {e}"))?;
    state.set_viewport(viewport_from_terminal());

    loop {
        render_list(state)?;
        // Non-key events (resize, mouse, paste) fall through to the viewport
        // re-read at the bottom of the loop.
        if let Event::Key(key) = event::read().map_err(|e| anyhow!("reading key: {e}"))? {
            if key.kind == KeyEventKind::Release {
                continue; // Windows fires press+release; act on press only.
            }
            let ctrl_c = key.modifiers.contains(KeyModifiers::CONTROL)
                && matches!(key.code, KeyCode::Char('c'));
            match key.code {
                KeyCode::Up | KeyCode::Char('k') => state.move_up(),
                KeyCode::Down | KeyCode::Char('j') => state.move_down(),
                KeyCode::Right | KeyCode::Char('l') => {
                    state.expand(|entry| load_child_dirs(fs, entry));
                }
                KeyCode::Left | KeyCode::Char('h') => state.collapse(),
                KeyCode::Enter => {
                    let path = state.selected_path().to_string();
                    if confirm_dialog(&path)? {
                        return Ok(Some(path));
                    }
                    // Cancelled at the confirm step -> back to the list.
                }
                KeyCode::Esc | KeyCode::Char('q') => return Ok(None),
                _ if ctrl_c => return Ok(None),
                _ => {}
            }
        }
        state.set_viewport(viewport_from_terminal());
    }
}

/// Draw the scrolling folder list with a scrollbar and key help.
fn render_list(state: &PickerState) -> Result<()> {
    let mut out = io::stdout();
    let (cols, _) = terminal::size().unwrap_or((FALLBACK_COLS, FALLBACK_ROWS));
    let width = cols as usize;

    queue!(
        out,
        terminal::Clear(terminal::ClearType::All),
        cursor::MoveTo(0, 0)
    )?;
    queue!(
        out,
        Print("Pick a folder to bless  (HFS / HFS+ System Folder)")
    )?;
    queue!(out, cursor::MoveTo(0, 1))?;
    let sel = truncate(state.selected_path(), width.saturating_sub(11));
    queue!(out, Print(format!("Selected:  {sel}")))?;
    queue!(out, cursor::MoveTo(0, 2))?;
    queue!(out, Print(ascii_rule(width)))?;

    let (start, end) = state.visible_range();
    let total = state.len();
    for (row, idx) in (start..end).enumerate() {
        let y = (row + 3) as u16;
        let v = state.row_view(idx);
        let bar = scrollbar_char(row, state.viewport, total, state.scroll);
        let cursor_mark = if v.selected { '>' } else { ' ' };
        let indent = "  ".repeat(v.depth);
        // Leave 2 columns at the right edge for the scrollbar.
        let body_width = width.saturating_sub(4);
        let body = truncate(
            &format!("{cursor_mark} {indent}{} {}", v.marker, v.name),
            body_width,
        );
        queue!(out, cursor::MoveTo(0, y))?;
        if v.selected {
            queue!(out, SetAttribute(Attribute::Reverse))?;
            queue!(out, Print(format!("{body:<body_width$}")))?;
            queue!(out, SetAttribute(Attribute::Reset))?;
        } else {
            queue!(out, Print(&body))?;
        }
        queue!(out, cursor::MoveTo((width.saturating_sub(1)) as u16, y))?;
        queue!(out, Print(bar))?;
    }

    let help_y = (3 + state.viewport + 1) as u16;
    queue!(out, cursor::MoveTo(0, help_y.saturating_sub(1)))?;
    queue!(out, Print(ascii_rule(width)))?;
    queue!(out, cursor::MoveTo(0, help_y))?;
    queue!(
        out,
        Print("Up/Down move   Right expand   Left collapse   Enter bless   Esc/q cancel")
    )?;
    out.flush()?;
    Ok(())
}

/// Modal "are you sure?" screen with OK / Cancel. Returns whether the user
/// confirmed. Left/Right/Tab toggle the buttons; Enter accepts the focused
/// one; `y`/`n` are shortcuts; Esc cancels.
fn confirm_dialog(path: &str) -> Result<bool> {
    let mut ok_focused = true; // default focus on OK
    loop {
        let mut out = io::stdout();
        let (cols, _) = terminal::size().unwrap_or((FALLBACK_COLS, FALLBACK_ROWS));
        let width = cols as usize;
        queue!(
            out,
            terminal::Clear(terminal::ClearType::All),
            cursor::MoveTo(0, 1)
        )?;
        queue!(
            out,
            Print("Bless this folder as the bootable System Folder?")
        )?;
        queue!(out, cursor::MoveTo(0, 3))?;
        queue!(
            out,
            Print(format!("    {}", truncate(path, width.saturating_sub(4))))
        )?;
        queue!(out, cursor::MoveTo(0, 5))?;
        // Buttons; focused one rendered in reverse video.
        queue!(out, Print("    "))?;
        draw_button(&mut out, "[ OK ]", ok_focused)?;
        queue!(out, Print("    "))?;
        draw_button(&mut out, "[ Cancel ]", !ok_focused)?;
        queue!(out, cursor::MoveTo(0, 7))?;
        queue!(
            out,
            Print("Left/Right or Tab to choose, Enter to confirm, Esc to cancel")
        )?;
        out.flush()?;

        let Event::Key(key) = event::read().map_err(|e| anyhow!("reading key: {e}"))? else {
            continue;
        };
        if key.kind == KeyEventKind::Release {
            continue;
        }
        match key.code {
            KeyCode::Left
            | KeyCode::Right
            | KeyCode::Tab
            | KeyCode::Char('h')
            | KeyCode::Char('l') => {
                ok_focused = !ok_focused;
            }
            KeyCode::Char('y') | KeyCode::Char('Y') => return Ok(true),
            KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => return Ok(false),
            KeyCode::Enter => return Ok(ok_focused),
            _ => {}
        }
    }
}

fn draw_button(out: &mut io::Stdout, label: &str, focused: bool) -> Result<()> {
    if focused {
        queue!(out, SetAttribute(Attribute::Reverse))?;
        queue!(out, Print(label))?;
        queue!(out, SetAttribute(Attribute::Reset))?;
    } else {
        queue!(out, Print(label))?;
    }
    Ok(())
}

/// One character of an ASCII vertical scrollbar for visible `row` of
/// `viewport`, given `total` items scrolled to `scroll`. Returns a space when
/// everything fits.
fn scrollbar_char(row: usize, viewport: usize, total: usize, scroll: usize) -> char {
    if total <= viewport {
        return ' ';
    }
    let thumb_len = (viewport * viewport / total).max(1);
    // `total > viewport` is guaranteed by the early return above, so
    // `max_scroll >= 1` and the division is always safe.
    let max_scroll = total - viewport;
    let track = viewport.saturating_sub(thumb_len);
    let thumb_pos = scroll * track / max_scroll;
    if row >= thumb_pos && row < thumb_pos + thumb_len {
        '#'
    } else {
        '|'
    }
}

fn ascii_rule(width: usize) -> String {
    "-".repeat(width.min(120))
}

/// Truncate to `max` display columns, appending `..` when clipped. ASCII-only
/// so the byte length equals the column count.
fn truncate(s: &str, max: usize) -> String {
    if max == 0 {
        return String::new();
    }
    if s.chars().count() <= max {
        return s.to_string();
    }
    if max <= 2 {
        return ".".repeat(max);
    }
    let keep: String = s.chars().take(max - 2).collect();
    format!("{keep}..")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir(name: &str, cnid: u64) -> FileEntry {
        FileEntry::new_directory(name.to_string(), format!("/{name}"), cnid)
    }

    fn names(state: &PickerState) -> Vec<String> {
        state.nodes.iter().map(|n| n.name.clone()).collect()
    }

    fn state_with(n: usize) -> PickerState {
        let dirs = (0..n).map(|i| dir(&format!("d{i}"), i as u64)).collect();
        let mut s = PickerState::new(dirs);
        s.set_viewport(4);
        s
    }

    #[test]
    fn move_up_down_clamps_at_ends() {
        let mut s = state_with(3);
        assert_eq!(s.selected_index(), 0);
        s.move_up(); // clamp at top
        assert_eq!(s.selected_index(), 0);
        s.move_down();
        s.move_down();
        s.move_down(); // clamp at bottom
        assert_eq!(s.selected_index(), 2);
    }

    #[test]
    fn scroll_follows_selection_within_viewport() {
        let mut s = state_with(10); // viewport 4
        for _ in 0..5 {
            s.move_down();
        }
        // selected == 5 must be visible
        let (start, end) = s.visible_range();
        assert!(start <= 5 && 5 < end, "5 not in [{start},{end})");
        assert_eq!(end - start, 4);
    }

    #[test]
    fn expand_splices_children_after_parent() {
        let mut s = state_with(2); // d0, d1
                                   // select d0, expand with two children
        s.expand(|_| vec![dir("childA", 100), dir("childB", 101)]);
        assert_eq!(names(&s), vec!["d0", "childA", "childB", "d1"]);
        // child paths are nested under the parent
        assert_eq!(s.nodes[1].path, "/d0/childA");
        assert_eq!(s.nodes[1].depth, 1);
    }

    #[test]
    fn expand_empty_marks_leaf_and_adds_nothing() {
        let mut s = state_with(2);
        s.expand(|_| vec![]);
        assert_eq!(names(&s), vec!["d0", "d1"]);
        assert_eq!(s.nodes[0].child_state, ChildState::Leaf);
        // A second expand on a known leaf is a no-op (loader not consulted).
        s.expand(|_| panic!("loader must not run on a known leaf"));
    }

    #[test]
    fn collapse_removes_descendants() {
        let mut s = state_with(2);
        s.expand(|_| vec![dir("childA", 100), dir("childB", 101)]);
        assert_eq!(s.len(), 4);
        s.collapse(); // selection still on d0
        assert_eq!(names(&s), vec!["d0", "d1"]);
        assert!(!s.nodes[0].expanded);
    }

    #[test]
    fn collapse_on_child_jumps_to_parent() {
        let mut s = state_with(2);
        s.expand(|_| vec![dir("childA", 100), dir("childB", 101)]);
        s.move_down(); // select childA (idx 1, depth 1)
        assert_eq!(s.selected_index(), 1);
        s.collapse(); // not expanded -> jump to parent d0 (idx 0)
        assert_eq!(s.selected_index(), 0);
        // tree unchanged (parent still expanded)
        assert_eq!(s.len(), 4);
    }

    #[test]
    fn selected_path_reports_full_nested_path() {
        let mut s = state_with(1);
        s.expand(|_| vec![dir("Extensions", 100)]);
        s.move_down();
        assert_eq!(s.selected_path(), "/d0/Extensions");
    }

    #[test]
    fn scrollbar_hidden_when_everything_fits() {
        assert_eq!(scrollbar_char(0, 8, 5, 0), ' ');
    }

    #[test]
    fn scrollbar_thumb_tracks_scroll() {
        // 20 items, viewport 5, scrolled to top -> thumb at row 0.
        assert_eq!(scrollbar_char(0, 5, 20, 0), '#');
        // scrolled to bottom -> thumb covers the last row.
        let max_scroll = 20 - 5;
        assert_eq!(scrollbar_char(4, 5, 20, max_scroll), '#');
        assert_eq!(scrollbar_char(0, 5, 20, max_scroll), '|');
    }

    #[test]
    fn truncate_appends_ellipsis_when_clipped() {
        assert_eq!(truncate("System Folder", 7), "Syste..");
        assert_eq!(truncate("abc", 10), "abc");
        assert_eq!(truncate("anything", 0), "");
    }
}
