//! The TUI Explorer: one browser pane under the Explorer profile, drawn over the Inspect tab.
//!
//! The Explorer is the Commander screen with one pane ([`CommanderController::single`]) whose
//! pane runs [`PaneProfile::explorer`]: the partition picked on the Inspect tab, a folder tree
//! beside the listing, edits written at once, and the volume tools (check, repair, bless, text
//! editor, import, metadata, transforms). [`CommanderView`] carries the keys, effects and the
//! shared windows; this file adds what only the Explorer draws: the tree, the text editor (its
//! buffer is widget state), the metadata form, the check / repair report and the transforms menu,
//! whose chosen command goes to the TUI's `:` command line. Extract (with the volume's fork,
//! ProDOS and QNX options) and the tree window serve every Explorer. The Optical tab's disc
//! browser is the same window over `optical::disc_browser` (`PaneProfile::optical`, read-only,
//! a filesystem picker and the game line), opened by [`CommanderView::disc_open`].
//!
//! Every writable Inspect partition opens through the controller's Inspect modes, as the egui
//! browse view does. A source edited in place enters Edit Mode by itself once listed (a SquashFS
//! asks for its size budget first), so edits are written at once as before. A source edited
//! through a copy (a zstd backup partition decompressed to a temp file, a CHD over a diff;
//! `BrowseOpen::edits_through_modes`) stays read-only until Ctrl+W, each edit is written to the
//! copy, and leaving Edit Mode (or closing) saves it after the Discard / Apply / Cancel question.

use std::path::PathBuf;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::Modifier;
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Clear, Paragraph};
use ratatui::Frame;

use super::commander::CommanderView;
use super::{centered_rect, footer_strip, App, HINTS_EDITOR};
use crate::controller::browser::{
    BrowserPaneAction, LineEnding, MetadataFields, PaneProfile, PaneServices, TextDoc, TextShape,
};
use crate::controller::commander::{
    extract_applies, extract_fork_modes, format_size, BudgetAction, BudgetMode, CommanderAction,
    CommanderController, ExtractOptions, ModeAction, ResourceForkMode, Side, UnsavedChoice,
};
use crate::controller::inspect::{BrowseEdit, BrowseOpen, BrowseSession};
use crate::controller::keys::KeyMap;

type P = BrowserPaneAction;

/// What only the Explorer shows: its title, editor, form and menus.
pub(super) struct ExplorerState {
    pub(super) label: String,
    editor: Option<TextEditor>,
    meta: Option<MetaInput>,
    transforms: Option<usize>,
    report_scroll: usize,
    /// A transform's command line, for the TUI's `:` prompt.
    palette: Option<String>,
    /// The session the Inspect controller handed over, for a seekable-cache reopen.
    session: Option<BrowseSession>,
    /// The Optical tab's disc browser (`PaneProfile::optical`) rather than an Inspect partition.
    disc: bool,
    /// The last tree listing and its scroll, while its window is up.
    tree: Option<(String, usize)>,
    extract: Option<ExtractMenu>,
    /// Opened through the controller's Inspect modes.
    modes: bool,
    /// Edit Mode works on a copy (zstd temp, CHD diff), entered with Ctrl+W.
    copy: bool,
    /// An in-place source enters Edit Mode once its listing is loaded.
    auto_edit: bool,
    /// The last Edit Mode notice or error, under the listing.
    mode_note: Option<String>,
}

/// The Extract window: the options the volume offers, then Extract.
struct ExtractMenu {
    path: String,
    rows: Vec<XRow>,
    sel: usize,
    options: ExtractOptions,
}

/// One row of the Extract window.
#[derive(Clone, Copy, PartialEq, Debug)]
enum XRow {
    Fork(ResourceForkMode),
    ProdosSuffix,
    RsToLf,
    Go,
}

/// The metadata form's text fields while the user types; the controller checks them on Enter.
struct MetaInput {
    fields: MetadataFields,
    field: usize,
}

const META_LABELS: [&str; 6] = [
    "Type:",
    "Creator:",
    "Modified:",
    "Created:",
    "Mode:",
    "Owner:",
];

impl MetaInput {
    fn focused_mut(&mut self) -> &mut String {
        let f = &mut self.fields;
        match self.field {
            1 => &mut f.creator,
            2 => &mut f.modified,
            3 => &mut f.created,
            4 => &mut f.mode,
            5 => &mut f.owner,
            _ => &mut f.type_code,
        }
    }

    fn values(&self) -> [&str; 6] {
        let f = &self.fields;
        [
            &f.type_code,
            &f.creator,
            &f.modified,
            &f.created,
            &f.mode,
            &f.owner,
        ]
    }
}

/// A small text editor over one file inside an image (no undo, search or wrapping), for fixing a
/// config line where there is no `$EDITOR`; the controller reads and writes it.
pub(super) struct TextEditor {
    name: String,
    folder: String,
    /// One entry per line, never empty (an empty file is one empty line).
    lines: Vec<String>,
    row: usize,
    col: usize,
    scroll: usize,
    dirty: bool,
    shape: TextShape,
    status: Option<String>,
    /// Esc with unsaved changes asks rather than discarding them.
    confirm_discard: bool,
}

impl TextEditor {
    pub(super) fn new(doc: TextDoc) -> Self {
        // `split`, not `lines`: `lines` drops a trailing newline, so every save would lose it.
        let lines: Vec<String> = doc.text.split('\n').map(str::to_string).collect();
        Self {
            name: doc.name,
            folder: doc.folder,
            lines,
            row: 0,
            col: 0,
            scroll: 0,
            dirty: false,
            shape: doc.shape,
            status: None,
            confirm_discard: false,
        }
    }

    /// The edited text, LF-only.
    pub(super) fn text(&self) -> String {
        self.lines.join("\n")
    }

    fn cur_len(&self) -> usize {
        self.lines[self.row].chars().count()
    }

    /// Byte offset of character `col` in the current line (decoded text can be multi-byte).
    fn byte_at(&self, col: usize) -> usize {
        let line = &self.lines[self.row];
        line.char_indices().nth(col).map_or(line.len(), |(i, _)| i)
    }

    pub(super) fn insert(&mut self, c: char) {
        let at = self.byte_at(self.col);
        self.lines[self.row].insert(at, c);
        self.col += 1;
        self.dirty = true;
    }

    pub(super) fn newline(&mut self) {
        let at = self.byte_at(self.col);
        let rest = self.lines[self.row].split_off(at);
        self.lines.insert(self.row + 1, rest);
        self.row += 1;
        self.col = 0;
        self.dirty = true;
    }

    pub(super) fn backspace(&mut self) {
        if self.col > 0 {
            let at = self.byte_at(self.col - 1);
            self.lines[self.row].remove(at);
            self.col -= 1;
            self.dirty = true;
        } else if self.row > 0 {
            let cur = self.lines.remove(self.row);
            self.row -= 1;
            self.col = self.cur_len();
            self.lines[self.row].push_str(&cur);
            self.dirty = true;
        }
    }

    fn delete(&mut self) {
        if self.col < self.cur_len() {
            let at = self.byte_at(self.col);
            self.lines[self.row].remove(at);
            self.dirty = true;
        } else if self.row + 1 < self.lines.len() {
            let next = self.lines.remove(self.row + 1);
            self.lines[self.row].push_str(&next);
            self.dirty = true;
        }
    }

    pub(super) fn move_to(&mut self, row: usize, col: usize) {
        self.row = row.min(self.lines.len() - 1);
        self.col = col.min(self.cur_len());
    }

    /// Keep the cursor on screen for a viewport of `height` lines.
    pub(super) fn follow_cursor(&mut self, height: usize) {
        if height == 0 {
            return;
        }
        if self.row < self.scroll {
            self.scroll = self.row;
        } else if self.row >= self.scroll + height {
            self.scroll = self.row + 1 - height;
        }
    }
}

fn page() -> usize {
    let rows = crossterm::terminal::size().map(|(_, r)| r).unwrap_or(24);
    (rows.saturating_sub(6) as usize).max(1)
}

#[cfg(not(test))]
fn load_keymap() -> (KeyMap, Vec<String>) {
    KeyMap::load_explorer_terminal(KeyMap::user_file().as_deref())
}

#[cfg(test)]
fn load_keymap() -> (KeyMap, Vec<String>) {
    (KeyMap::explorer_terminal(), Vec::new())
}

impl CommanderView {
    /// The Explorer over a partition the Inspect controller handed over; only an in-place
    /// source may be written (a compressed backup partition opens read-only).
    pub(super) fn explorer_open(open: BrowseOpen, label: String) -> Self {
        let (keymap, problems) = load_keymap();
        let mut services = PaneServices::headless();
        services.recent = super::commander::recent_store();
        let copy = open.edits_through_modes();
        let modes = !matches!(open.edit, BrowseEdit::ReadOnly);
        let mut profile = PaneProfile::explorer();
        profile.read_only = !modes;
        let mut ctl = CommanderController::single(services, keymap, profile);
        ctl.report_keymap_problems(problems);
        let title = match &open.label_prefix {
            Some(prefix) => format!("{prefix}  {label}"),
            None => label,
        };
        let mut fx = Vec::new();
        if modes {
            fx = ctl.open_inspect(open.session.clone(), title.as_str());
            match open.edit {
                BrowseEdit::Archive(ctx) => ctl.inspect_edit_archive(ctx),
                BrowseEdit::InPlace => ctl.inspect_edit_in_place(),
                BrowseEdit::ReadOnly => {}
            }
            if let Some(folder) = open.backup_folder {
                ctl.inspect_backup_folder(folder);
            }
        } else {
            ctl.open_fixed_session(open.session.clone(), title.as_str());
        }
        let mut v = CommanderView::with_ctl(ctl);
        v.ex = Some(ExplorerState {
            label: title,
            editor: None,
            meta: None,
            transforms: None,
            report_scroll: 0,
            palette: None,
            session: Some(open.session),
            disc: false,
            tree: None,
            extract: None,
            modes,
            copy,
            auto_edit: modes && !copy,
            mode_note: None,
        });
        v.run(fx);
        v
    }

    /// The disc browser over the Optical tab's disc: one pane under `PaneProfile::optical`.
    #[cfg(feature = "optical")]
    pub(super) fn disc_open(src: crate::controller::browser::DiscSource, label: String) -> Self {
        let (keymap, problems) = load_keymap();
        let mut services = PaneServices::headless();
        services.recent = super::commander::recent_store();
        let mut ctl = crate::controller::optical::disc_browser(services, keymap);
        ctl.report_keymap_problems(problems);
        let fx = ctl.open_disc(src);
        let mut v = CommanderView::with_ctl(ctl);
        v.ex = Some(ExplorerState {
            label,
            editor: None,
            meta: None,
            transforms: None,
            report_scroll: 0,
            palette: None,
            session: None,
            disc: true,
            tree: None,
            extract: None,
            modes: false,
            copy: false,
            auto_edit: false,
            mode_note: None,
        });
        v.run(fx);
        v
    }

    /// This is the Optical tab's disc browser.
    pub(super) fn is_disc(&self) -> bool {
        self.ex.as_ref().is_some_and(|e| e.disc)
    }

    /// The window title: the disc browser or the Explorer, and the label.
    pub(super) fn explorer_title(&self) -> String {
        let what = if self.is_disc() {
            "Disc Browser"
        } else {
            "Explorer"
        };
        let mode = if !self.ex.as_ref().is_some_and(|e| e.copy) {
            ""
        } else if self.ctl.edit_mode() {
            "  [Edit Mode - Ctrl+W saves]"
        } else {
            "  [read-only - Ctrl+W Edit Mode]"
        };
        let space = match self.ctl.space().filter(|_| self.edits_through_modes()) {
            Some(sp) => match sp.after {
                Some(_) if sp.overflows => {
                    format!(
                        "  Free {} - the staged edits do not fit",
                        format_size(sp.free)
                    )
                }
                Some(after) => format!("  Free {} -> {}", format_size(sp.free), format_size(after)),
                None => format!("  Free {}", format_size(sp.free)),
            },
            None => String::new(),
        };
        format!("{what}  {}{mode}{space}", self.explorer_label())
    }

    /// Opened through the controller's Inspect modes.
    pub(super) fn edits_through_modes(&self) -> bool {
        self.ex.as_ref().is_some_and(|e| e.modes)
    }

    /// Edit Mode works on a copy that Ctrl+W saves (a compressed backup, a CHD).
    pub(super) fn edits_a_copy(&self) -> bool {
        self.ex.as_ref().is_some_and(|e| e.copy)
    }

    /// Writes in Edit Mode that leaving it (or closing) would save or drop.
    pub(super) fn has_unsaved_mode_edits(&self) -> bool {
        self.edits_through_modes() && self.ctl.has_unsaved_edits()
    }

    /// Close through the modes' question (a quit or a hand-off while Edit Mode holds writes).
    pub(super) fn request_mode_close(&mut self) {
        self.send(CommanderAction::Mode(ModeAction::RequestClose));
        self.explorer_tick();
    }

    /// Let go of the source: a CHD diff or archive temp the modes made is removed after the readers.
    pub(super) fn release(self) {
        if self.edits_through_modes() {
            self.ctl.close_inspect();
        }
    }

    /// Extract the cursor entry: at once, or after the Extract window when the volume has options.
    pub(super) fn open_extract(&mut self) {
        let pane = self.ctl.pane(Side::Left);
        let path = pane
            .cursor_index()
            .and_then(|i| pane.rows().get(i))
            .and_then(|r| pane.row_entry(r))
            .map(|e| e.path)
            .unwrap_or_else(|| "/".to_string());
        let applies = extract_applies(pane.volume().fs_type.as_str());
        let options = ExtractOptions::default();
        let mut rows: Vec<XRow> = Vec::new();
        if applies.forks {
            rows.extend(extract_fork_modes().into_iter().map(XRow::Fork));
        }
        if applies.prodos_suffix {
            rows.push(XRow::ProdosSuffix);
        }
        if applies.rs_to_lf {
            rows.push(XRow::RsToLf);
        }
        if rows.is_empty() || self.ex.is_none() {
            self.send(CommanderAction::Extract {
                from: Side::Left,
                path,
                options,
                dest: None,
            });
            return;
        }
        rows.push(XRow::Go);
        let sel = rows
            .iter()
            .position(|r| *r == XRow::Fork(options.fork_mode))
            .unwrap_or(0);
        if let Some(ex) = self.ex.as_mut() {
            ex.extract = Some(ExtractMenu {
                path,
                rows,
                sel,
                options,
            });
        }
    }

    /// The Extract window's rows as drawn, for tests.
    #[cfg(test)]
    pub(super) fn extract_rows(&self) -> Vec<String> {
        let rows = self.ex.as_ref().and_then(|e| e.extract.as_ref());
        let label = |r: &XRow| match r {
            XRow::Fork(m) => m.label().to_string(),
            other => format!("{other:?}"),
        };
        rows.map(|m| m.rows.iter().map(label).collect())
            .unwrap_or_default()
    }

    /// The tree window's text, while it is up.
    #[cfg(test)]
    pub(super) fn tree_text(&self) -> Option<String> {
        let tree = self.ex.as_ref().and_then(|e| e.tree.as_ref());
        tree.map(|(text, _)| text.clone())
    }

    /// The Extract window's keys.
    fn extract_key(&mut self, code: KeyCode) {
        let Some(m) = self.ex.as_mut().and_then(|e| e.extract.as_mut()) else {
            return;
        };
        let row = m.rows[m.sel];
        let mut go = false;
        match code {
            KeyCode::Esc => {
                if let Some(ex) = self.ex.as_mut() {
                    ex.extract = None;
                }
                return;
            }
            KeyCode::Up => m.sel = m.sel.saturating_sub(1),
            KeyCode::Down => m.sel = (m.sel + 1).min(m.rows.len() - 1),
            KeyCode::Char(' ') | KeyCode::Enter => {
                match row {
                    XRow::Fork(mode) => m.options.fork_mode = mode,
                    XRow::ProdosSuffix => m.options.prodos_suffix = !m.options.prodos_suffix,
                    XRow::RsToLf => m.options.rs_to_lf = !m.options.rs_to_lf,
                    XRow::Go => {}
                }
                // Enter on a fork mode picks it and extracts; on a box it only toggles.
                go = code == KeyCode::Enter && matches!(row, XRow::Fork(_) | XRow::Go);
            }
            _ => {}
        }
        if go {
            let Some(m) = self.ex.as_mut().and_then(|e| e.extract.take()) else {
                return;
            };
            self.send(CommanderAction::Extract {
                from: Side::Left,
                path: m.path,
                options: m.options,
                dest: None,
            });
        }
    }

    /// The tree window's keys: scroll, Save, close.
    fn tree_key(&mut self, code: KeyCode) {
        let Some((text, scroll)) = self.ex.as_mut().and_then(|e| e.tree.as_mut()) else {
            return;
        };
        let max = text.lines().count().saturating_sub(1);
        match code {
            KeyCode::Esc | KeyCode::Enter | KeyCode::Char('q') => {
                if let Some(ex) = self.ex.as_mut() {
                    ex.tree = None;
                }
            }
            KeyCode::Char('s') => self.send(CommanderAction::Pane(Side::Left, P::SaveTree)),
            KeyCode::Up => *scroll = scroll.saturating_sub(1),
            KeyCode::Down => *scroll = (*scroll + 1).min(max),
            KeyCode::PageUp => *scroll = scroll.saturating_sub(page()),
            KeyCode::PageDown => *scroll = (*scroll + page()).min(max),
            KeyCode::Home => *scroll = 0,
            KeyCode::End => *scroll = max,
            _ => {}
        }
    }

    /// A streamed backup partition's seekable cache is ready: reopen on it, keeping the folder.
    pub(super) fn upgrade_seekable(&mut self, path: PathBuf) {
        let Some(ex) = self.ex.as_mut() else { return };
        let Some(session) = ex.session.as_mut().filter(|s| s.zstd_cache.is_some()) else {
            return;
        };
        session.source_path = Some(path.clone());
        session.zstd_cache = None;
        if ex.modes {
            self.send(CommanderAction::Mode(ModeAction::UpgradeSeekable(path)));
            return;
        }
        let (session, label) = (session.clone(), ex.label.clone());
        self.ctl.open_fixed_session(session, label.as_str());
    }

    /// The partition's label from the Inspect tab, for the window title.
    pub(super) fn explorer_label(&self) -> &str {
        self.ex.as_ref().map_or("", |e| e.label.as_str())
    }

    /// A transform's command line, once, for the TUI's `:` prompt.
    pub(super) fn take_palette(&mut self) -> Option<String> {
        self.ex.as_mut().and_then(|e| e.palette.take())
    }

    /// True while the text editor has the keyboard (every key is text).
    pub(super) fn editor_open(&self) -> bool {
        self.ex.as_ref().is_some_and(|e| e.editor.is_some())
    }

    /// Pick up what the pane handed over: a file read for the editor, the form opening or closing.
    pub(super) fn explorer_tick(&mut self) {
        let doc = match self.ex.as_ref() {
            Some(_) => self.ctl.take_text_doc(),
            None => return,
        };
        let tree = self.ctl.host_pane().take_tree_text();
        let form = self.ctl.pane(Side::Left).metadata_form().cloned();
        let listed = self.ctl.pane(Side::Left).listing.is_loaded();
        let start_edit = self
            .ex
            .as_ref()
            .is_some_and(|e| e.auto_edit && listed && !self.ctl.is_working());
        if start_edit {
            if let Some(ex) = self.ex.as_mut() {
                ex.auto_edit = false;
            }
            self.send(CommanderAction::Mode(ModeAction::ToggleEditMode));
        }
        let notice = self.ctl.take_mode_notice();
        let error = self.ctl.take_mode_error();
        if self.ctl.take_close_request() {
            self.closed = true;
        }
        let Some(ex) = self.ex.as_mut() else { return };
        if let Some(said) = error.or(notice) {
            ex.mode_note = Some(said);
        }
        if let Some(doc) = doc {
            ex.editor = Some(TextEditor::new(doc));
        }
        if let Some(tree) = tree {
            ex.tree = Some((tree.text, 0));
        }
        match (form, ex.meta.is_some()) {
            (Some(f), false) => {
                ex.meta = Some(MetaInput {
                    fields: f.initial,
                    field: 0,
                })
            }
            (None, true) => ex.meta = None,
            _ => {}
        }
    }

    pub(super) fn open_transforms(&mut self) {
        let has = !self.ctl.pane(Side::Left).transforms().is_empty();
        if let Some(ex) = self.ex.as_mut().filter(|_| has) {
            ex.transforms = Some(0);
        }
    }

    /// Keys for the Explorer's own windows; `None` when none is up.
    pub(super) fn explorer_key(&mut self, key: KeyEvent) -> Option<bool> {
        self.ex.as_ref()?;
        if self.editor_open() {
            return Some(self.editor_key(key));
        }
        let code = key.code;
        // Edit Mode's unsaved-writes question: Discard / Apply / Cancel.
        if self.ctl.unsaved_shown() {
            let choice = match code {
                KeyCode::Char('d') => Some(UnsavedChoice::Discard),
                KeyCode::Char('a') | KeyCode::Enter => Some(UnsavedChoice::Apply),
                KeyCode::Esc | KeyCode::Char('c') => Some(UnsavedChoice::Cancel),
                _ => None,
            };
            if let Some(c) = choice {
                self.send(CommanderAction::Mode(ModeAction::Unsaved(c)));
                self.explorer_tick();
            }
            return Some(true);
        }
        // The SquashFS size budget asked before Edit Mode.
        if let Some(form) = self.ctl.size_budget().copied() {
            let mib = match form.mode() {
                BudgetMode::Absolute => Some(form.absolute_mib()),
                BudgetMode::Headroom => Some(form.headroom_mib()),
                BudgetMode::Fit => None,
            };
            let set = |n: u64| match form.mode() {
                BudgetMode::Absolute => Some(BudgetAction::SetAbsoluteMib(n)),
                BudgetMode::Headroom => Some(BudgetAction::SetHeadroomMib(n)),
                BudgetMode::Fit => None,
            };
            let modes = [BudgetMode::Fit, BudgetMode::Absolute, BudgetMode::Headroom];
            let at = modes.iter().position(|m| *m == form.mode()).unwrap_or(0);
            let edit = match code {
                KeyCode::Up => Some(BudgetAction::SetMode(modes[(at + 2) % 3])),
                KeyCode::Down => Some(BudgetAction::SetMode(modes[(at + 1) % 3])),
                KeyCode::Char(c) if c.is_ascii_digit() => mib.and_then(|n| {
                    let d = u64::from(c.to_digit(10).unwrap_or(0));
                    set(n.saturating_mul(10).saturating_add(d).min(1 << 30))
                }),
                KeyCode::Backspace => mib.and_then(|n| set(n / 10)),
                KeyCode::Enter => Some(BudgetAction::Start),
                KeyCode::Esc => Some(BudgetAction::Cancel),
                _ => None,
            };
            if let Some(edit) = edit {
                self.send(CommanderAction::Mode(ModeAction::Budget(edit)));
                self.explorer_tick();
            }
            return Some(true);
        }
        // An Edit Mode job (decompress, recompress, CHD open or flatten) holds the keys.
        if self.edits_through_modes() && self.ctl.mode_working().is_some() {
            return Some(true);
        }
        if let Some(lines) = self
            .ctl
            .pane(Side::Left)
            .tool_report()
            .map(|r| r.lines.len())
        {
            let ex = self.ex.as_mut()?;
            let max = lines.saturating_sub(1);
            match code {
                KeyCode::Esc | KeyCode::Enter | KeyCode::Char('q') => {
                    ex.report_scroll = 0;
                    self.send(CommanderAction::Pane(Side::Left, P::DismissToolReport));
                }
                KeyCode::Up => ex.report_scroll = ex.report_scroll.saturating_sub(1),
                KeyCode::Down => ex.report_scroll = (ex.report_scroll + 1).min(max),
                KeyCode::PageUp => ex.report_scroll = ex.report_scroll.saturating_sub(page()),
                KeyCode::PageDown => ex.report_scroll = (ex.report_scroll + page()).min(max),
                KeyCode::Home => ex.report_scroll = 0,
                KeyCode::End => ex.report_scroll = max,
                _ => {}
            }
            return Some(true);
        }
        if self.ex.as_ref()?.meta.is_some() {
            let ex = self.ex.as_mut()?;
            let m = ex.meta.as_mut()?;
            let n = META_LABELS.len();
            match code {
                KeyCode::Esc => self.send(CommanderAction::Pane(Side::Left, P::CancelMetadata)),
                KeyCode::Enter => {
                    let fields = m.fields.clone();
                    self.send(CommanderAction::Pane(
                        Side::Left,
                        P::SubmitMetadata(Box::new(fields)),
                    ));
                }
                KeyCode::Tab | KeyCode::Down => m.field = (m.field + 1) % n,
                KeyCode::BackTab | KeyCode::Up => m.field = (m.field + n - 1) % n,
                KeyCode::Backspace => {
                    m.focused_mut().pop();
                }
                KeyCode::Char(c) if !c.is_control() => m.focused_mut().push(c),
                _ => {}
            }
            self.explorer_tick();
            return Some(true);
        }
        if self.ex.as_ref()?.extract.is_some() {
            self.extract_key(code);
            return Some(true);
        }
        if self.ex.as_ref()?.tree.is_some() {
            self.tree_key(code);
            return Some(true);
        }
        if let Some(sel) = self.ex.as_ref()?.transforms {
            let list = self.ctl.pane(Side::Left).transforms();
            let ex = self.ex.as_mut()?;
            match code {
                KeyCode::Esc => ex.transforms = None,
                KeyCode::Up => ex.transforms = Some(sel.saturating_sub(1)),
                KeyCode::Down => ex.transforms = Some((sel + 1).min(list.len().saturating_sub(1))),
                KeyCode::Enter => {
                    ex.transforms = None;
                    ex.palette = list.get(sel).map(|(_, cmd)| cmd.clone());
                }
                _ => {}
            }
            return Some(true);
        }
        None
    }

    /// The editor's keys; `false` hands F1 to the shell's key list.
    fn editor_key(&mut self, key: KeyEvent) -> bool {
        let code = key.code;
        let page = page();
        let Some(ed) = self.ex.as_mut().and_then(|e| e.editor.as_mut()) else {
            return true;
        };
        if ed.confirm_discard {
            if matches!(code, KeyCode::Char('y') | KeyCode::Char('Y')) {
                if let Some(e) = self.ex.as_mut() {
                    e.editor = None;
                }
            } else {
                ed.confirm_discard = false;
            }
            return true;
        }
        ed.status = None;
        match code {
            KeyCode::F(1) => return false,
            // F2, not Ctrl-S: on a serial console Ctrl-S is XOFF and freezes the terminal.
            KeyCode::F(2) => {
                let save = P::SaveText {
                    name: ed.name.clone(),
                    folder: ed.folder.clone(),
                    text: ed.text(),
                    shape: ed.shape,
                };
                ed.dirty = false;
                ed.status = Some(format!("Saving {}...", ed.name));
                self.send(CommanderAction::Pane(Side::Left, save));
                return true;
            }
            // Endings are read from the file; this is for when the file itself is wrong.
            KeyCode::F(3) => {
                ed.shape.ending = match ed.shape.ending {
                    LineEnding::Lf => LineEnding::CrLf,
                    LineEnding::CrLf => LineEnding::Cr,
                    LineEnding::Cr => LineEnding::Lf,
                };
                ed.dirty = true;
                ed.status = Some(format!(
                    "Line endings will be saved as {}",
                    ed.shape.ending.label()
                ));
            }
            KeyCode::Esc if ed.dirty => ed.confirm_discard = true,
            KeyCode::Esc => {
                if let Some(e) = self.ex.as_mut() {
                    e.editor = None;
                }
                return true;
            }
            KeyCode::Char(c) if !key.modifiers.contains(KeyModifiers::CONTROL) => ed.insert(c),
            KeyCode::Enter => ed.newline(),
            KeyCode::Backspace => ed.backspace(),
            KeyCode::Delete => ed.delete(),
            KeyCode::Tab => (0..4).for_each(|_| ed.insert(' ')),
            KeyCode::Left if ed.col > 0 => ed.col -= 1,
            KeyCode::Left if ed.row > 0 => {
                let r = ed.row - 1;
                let c = ed.lines[r].chars().count();
                ed.move_to(r, c);
            }
            KeyCode::Right if ed.col < ed.cur_len() => ed.col += 1,
            KeyCode::Right if ed.row + 1 < ed.lines.len() => ed.move_to(ed.row + 1, 0),
            KeyCode::Up => ed.move_to(ed.row.saturating_sub(1), ed.col),
            KeyCode::Down => ed.move_to(ed.row + 1, ed.col),
            KeyCode::Home => ed.col = 0,
            KeyCode::End => ed.col = ed.cur_len(),
            KeyCode::PageUp => ed.move_to(ed.row.saturating_sub(page), ed.col),
            KeyCode::PageDown => ed.move_to(ed.row + page, ed.col),
            _ => {}
        }
        ed.follow_cursor(page);
        true
    }
}

// --- drawing ------------------------------------------------------------------

impl App {
    /// The Explorer's body: the folder tree beside the listing (in that style), and the status line.
    pub(super) fn draw_explorer_body(&self, frame: &mut Frame, area: Rect, v: &CommanderView) {
        let ctl = &v.ctl;
        let pane = ctl.pane(Side::Left);
        let pal = self.palette;
        let game = pane.disc_game().map(|g| format!(" Game: {g}"));
        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(3),
                Constraint::Length(game.is_some() as u16),
                Constraint::Length(1),
            ])
            .split(area);
        if let Some(g) = game {
            frame.render_widget(Paragraph::new(Line::styled(g, pal.dim())), rows[1]);
        }
        // Another listing style, chosen in the View menu, draws the pane alone.
        if pane.has_folder_tree() {
            let cols = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Length(32), Constraint::Min(24)])
                .split(rows[0]);
            self.draw_folder_tree(frame, cols[0], pane, pane.tree_focused());
            self.draw_cmd_pane(frame, cols[1], v, Side::Left);
        } else {
            self.draw_cmd_pane(frame, rows[0], v, Side::Left);
        }
        let note = v.ex.as_ref().and_then(|e| e.mode_note.clone());
        let (line, style) = match note {
            Some(n) if n.starts_with("Error") || n.starts_with("Cannot") => (n, pal.warn()),
            Some(n) => (n, pal.accent()),
            None => (ctl.status().to_string(), pal.dim()),
        };
        frame.render_widget(
            Paragraph::new(Line::styled(format!(" {line}"), style)),
            rows[2],
        );
    }

    /// The Explorer's own windows over everything else.
    pub(super) fn draw_explorer_windows(&self, frame: &mut Frame, area: Rect, v: &CommanderView) {
        let Some(ex) = v.ex.as_ref() else { return };
        let pal = self.palette;
        let pane = v.ctl.pane(Side::Left);
        if let Some(snap) = pane.tool_working() {
            self.draw_job_box(frame, area, "Working", &snap);
        }
        if let Some(snap) = v.ctl.mode_working().filter(|_| ex.modes) {
            self.draw_job_box(frame, area, "Edit Mode", &snap);
        }
        if let Some(form) = v.ctl.size_budget() {
            let plan = form.plan();
            let mark = |m: BudgetMode| if form.mode() == m { "(*)" } else { "( )" };
            let mut lines = vec![
                Line::raw(" SquashFS has no free space and rebuilds the whole image on a save:"),
                Line::raw(" choose how large the result may be."),
                Line::raw(""),
                Line::raw(format!(" Image occupies   {}", format_size(plan.image_len))),
                Line::raw(format!(
                    " File content     {} uncompressed",
                    format_size(plan.content_len)
                )),
                Line::raw(format!(" Compressed to    {}", plan.describe_ratio())),
                Line::raw(match (plan.capacity, plan.headroom) {
                    (Some(cap), Some(head)) => format!(
                        " Room available   {} ({} unused)",
                        format_size(cap),
                        format_size(head)
                    ),
                    _ => " Room available   unbounded - this file is the filesystem".to_string(),
                }),
                Line::raw(""),
                Line::styled(
                    format!(
                        " {} Fit - accept whatever it becomes",
                        mark(BudgetMode::Fit)
                    ),
                    pal.accent(),
                ),
                Line::styled(
                    format!(
                        " {} At most {} MiB",
                        mark(BudgetMode::Absolute),
                        form.absolute_mib()
                    ),
                    pal.accent(),
                ),
                Line::styled(
                    format!(
                        " {} Grow by at most {} MiB",
                        mark(BudgetMode::Headroom),
                        form.headroom_mib()
                    ),
                    pal.accent(),
                ),
                Line::raw(""),
                Line::raw(match form.ceiling() {
                    Some(c) => format!(" Ceiling: {}", format_size(c)),
                    None => " Ceiling: none beyond what the container allows".to_string(),
                }),
            ];
            if form.over_capacity() {
                lines.push(Line::styled(
                    format!(
                        " That is more than the {} available where this image lives.",
                        format_size(plan.capacity.unwrap_or(0))
                    ),
                    pal.warn(),
                ));
            }
            lines.push(Line::raw(""));
            lines.push(Line::styled(
                " Up/Down choose   0-9 MiB   Enter start editing   Esc stay read-only",
                pal.dim(),
            ));
            self.draw_cmd_box_auto(frame, area, 76, "SquashFS size budget", lines);
        }
        if v.ctl.unsaved_shown() {
            let mut lines = vec![Line::raw(
                " Edit Mode holds changes that are not saved to the source yet:",
            )];
            for l in pane.staged_summary() {
                lines.push(Line::raw(format!("   {l}")));
            }
            lines.push(Line::raw(""));
            lines.push(Line::styled(
                " a Apply (save)   d Discard   Esc Cancel",
                pal.accent(),
            ));
            self.draw_cmd_box_auto(frame, area, 70, "Unsaved Changes", lines);
        }
        if let Some(r) = pane.tool_report() {
            let h = area.height.saturating_sub(4).clamp(8, 22);
            let inner = h.saturating_sub(3) as usize;
            let start = ex.report_scroll.min(r.lines.len().saturating_sub(1));
            let mut lines: Vec<Line> = r
                .lines
                .iter()
                .skip(start)
                .take(inner)
                .map(|l| {
                    let style = if l.contains("CLEAN") || l.trim_start().starts_with('+') {
                        pal.accent()
                    } else if l.starts_with("Result:")
                        || l.contains("error")
                        || l.contains("failed")
                    {
                        pal.warn()
                    } else {
                        pal.dim()
                    };
                    Line::styled(format!(" {l}"), style)
                })
                .collect();
            lines.push(Line::styled(" Up/Down scroll   Esc close", pal.dim()));
            let w = area.width.saturating_sub(4).min(80);
            self.draw_cmd_box(frame, centered_rect(w, h, area), &r.title, lines);
        }
        if let Some(sel) = ex.transforms {
            let mut lines = vec![Line::raw("")];
            for (i, (label, _)) in pane.transforms().iter().enumerate() {
                let (marker, style) = if i == sel {
                    (
                        "> ",
                        pal.accent()
                            .add_modifier(Modifier::REVERSED | Modifier::BOLD),
                    )
                } else {
                    ("  ", pal.accent())
                };
                lines.push(Line::styled(format!("{marker}{label}"), style));
            }
            lines.push(Line::raw(""));
            lines.push(Line::styled(
                "  Enter -> prefills the command line; edit and run.  Esc cancel",
                pal.dim(),
            ));
            self.draw_cmd_box_auto(frame, area, 66, "Transform image", lines);
        }
        if let (Some(m), Some(form)) = (&ex.meta, pane.metadata_form()) {
            let mut lines = vec![
                Line::styled(format!("  {}", form.entry.name), pal.dim()),
                Line::raw(""),
            ];
            for (i, (label, val)) in META_LABELS.iter().zip(m.values()).enumerate() {
                let cursor = if m.field == i {
                    Span::styled(" ", pal.accent().add_modifier(Modifier::REVERSED))
                } else {
                    Span::raw("")
                };
                lines.push(Line::from(vec![
                    Span::styled(format!("  {label:<10}"), pal.accent()),
                    Span::raw(val.to_string()),
                    cursor,
                ]));
            }
            if let Some(e) = &form.error {
                lines.push(Line::styled(format!("  {e}"), pal.warn()));
            }
            lines.push(Line::raw(""));
            lines.push(Line::styled(
                "  Tab next field   Enter apply   Esc cancel   (blank = leave alone)",
                pal.dim(),
            ));
            self.draw_cmd_box_auto(frame, area, 64, "Edit metadata", lines);
        }
        if let Some((text, scroll)) = &ex.tree {
            let h = area.height.saturating_sub(2);
            let inner = h.saturating_sub(4) as usize;
            let mut lines: Vec<Line<'static>> = text
                .lines()
                .skip(*scroll)
                .take(inner)
                .map(|l| Line::raw(format!(" {l}")))
                .collect();
            lines.push(Line::raw(""));
            lines.push(Line::styled(
                " Up/Down scroll   s save to a file   Esc close",
                pal.dim(),
            ));
            let w = area.width.saturating_sub(4).min(110);
            self.draw_cmd_box(frame, centered_rect(w, h, area), "Tree", lines);
        }
        if let Some(m) = &ex.extract {
            let mut lines = vec![Line::styled(format!("  {}", m.path), pal.dim())];
            let mut forks = false;
            for (i, row) in m.rows.iter().enumerate() {
                let (text, style) = match row {
                    XRow::Fork(mode) => {
                        if !forks {
                            forks = true;
                            lines.push(Line::styled("  Resource forks:", pal.dim()));
                        }
                        let on = if m.options.fork_mode == *mode {
                            "(*)"
                        } else {
                            "( )"
                        };
                        (format!("  {on} {}", mode.label()), pal.accent())
                    }
                    XRow::ProdosSuffix => {
                        let on = if m.options.prodos_suffix { "x" } else { " " };
                        (
                            format!("  [{on}] ProDOS type suffix (#TTAAAA)"),
                            pal.accent(),
                        )
                    }
                    XRow::RsToLf => {
                        let on = if m.options.rs_to_lf { "x" } else { " " };
                        (format!("  [{on}] Text: RS to LF"), pal.accent())
                    }
                    XRow::Go => ("  [ Extract... ]".to_string(), pal.accent()),
                };
                let style = if i == m.sel {
                    style.add_modifier(Modifier::REVERSED | Modifier::BOLD)
                } else {
                    style
                };
                lines.push(Line::styled(text, style));
            }
            lines.push(Line::raw(""));
            lines.push(Line::styled(
                "  Space choose   Enter extract (asks for the folder)   Esc cancel",
                pal.dim(),
            ));
            self.draw_cmd_box_auto(frame, area, 70, "Extract", lines);
        }
        if let Some(ed) = &ex.editor {
            self.draw_editor(frame, area, ed, v.ctl.status());
        }
    }

    fn draw_editor(&self, frame: &mut Frame, area: Rect, ed: &TextEditor, status: &str) {
        let pal = self.palette;
        let ep = centered_rect(
            area.width.saturating_sub(4),
            area.height.saturating_sub(4),
            area,
        );
        frame.render_widget(Clear, ep);
        let title = format!("Edit  {}{}", ed.name, if ed.dirty { " *" } else { "" });
        let outer = self.pane_block(&title, true);
        let inner = outer.inner(ep);
        frame.render_widget(outer, ep);
        // The state and keys get their own rows, so they never read as file content.
        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(1), Constraint::Length(2)])
            .split(inner);
        let visible: Vec<Line> = ed
            .lines
            .iter()
            .enumerate()
            .skip(ed.scroll)
            .take(rows[0].height as usize)
            .map(|(i, l)| {
                // A marked cursor line: a block cursor cannot be drawn portably here.
                if i == ed.row {
                    Line::styled(format!("{:>4} >{l}", i + 1), pal.accent())
                } else {
                    Line::raw(format!("{:>4}  {l}", i + 1))
                }
            })
            .collect();
        frame.render_widget(Paragraph::new(Text::from(visible)), rows[0]);
        let (state, warn) = match &ed.status {
            Some(msg) if !ed.dirty && msg.starts_with("Saving") => (format!(" {status}"), false),
            Some(msg) => (msg.clone(), true),
            None => (
                format!(
                    " line {}/{} col {}  |  {} {}{}",
                    ed.row + 1,
                    ed.lines.len(),
                    ed.col + 1,
                    ed.shape.encoding.label(),
                    ed.shape.ending.label(),
                    if ed.dirty { "   *modified" } else { "" },
                ),
                false,
            ),
        };
        frame.render_widget(
            Paragraph::new(Text::from(vec![
                Line::styled(state, if warn { pal.warn() } else { pal.dim() }),
                Line::styled(
                    footer_strip(HINTS_EDITOR, rows[1].width as usize),
                    pal.dim(),
                ),
            ])),
            rows[1],
        );
        if ed.confirm_discard {
            let lines = vec![
                Line::raw(""),
                Line::styled("  Discard unsaved changes?  (y / n)", pal.warn()),
            ];
            self.draw_cmd_box_auto(frame, area, 52, "Unsaved changes", lines);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::{App, DEFAULT_TAB};
    use super::*;
    use crate::controller::keys::Command;
    use std::path::Path;
    use std::time::{Duration, Instant};

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn ctrl(c: char) -> KeyEvent {
        KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL)
    }

    use crate::controller::browser::explorer_tests::{fat_volume, root_file};

    /// The Explorer on the image's first partition, as the Inspect controller's Browse opens it.
    fn explorer_app(img: &Path) -> App {
        use crate::controller::inspect::{
            HostRequest, InspectAction, InspectController, InspectServices, InspectSource,
            JobRegistry, NoNotify,
        };
        use crate::controller::Controller as _;
        let mut ctl = InspectController::new(InspectServices {
            jobs: JobRegistry::new(),
            notify: std::sync::Arc::new(NoNotify),
        });
        ctl.apply(InspectAction::Load(InspectSource::Image {
            path: img.to_path_buf(),
            temp: None,
        }));
        ctl.wait_opened();
        let index = ctl.partitions()[0].index;
        ctl.take_requests();
        ctl.apply(InspectAction::Browse(index));
        let open = ctl
            .take_requests()
            .into_iter()
            .find_map(|r| match r {
                HostRequest::OpenBrowse(open) => Some(open),
                _ => None,
            })
            .expect("Browse hands over a session");
        let mut app = App::new_on(DEFAULT_TAB);
        app.explorer = Some(CommanderView::explorer_open(*open, "Whole volume".into()));
        settle(&mut app);
        app
    }

    fn view(app: &App) -> &CommanderView {
        app.explorer.as_ref().expect("the Explorer is open")
    }

    fn settle(app: &mut App) {
        let start = Instant::now();
        loop {
            app.tick();
            let busy = app.explorer.as_ref().is_some_and(|v| v.is_working());
            if !busy {
                break;
            }
            assert!(
                start.elapsed() < Duration::from_secs(60),
                "a job never ended"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        app.tick();
    }

    fn press(app: &mut App, k: KeyEvent) {
        app.on_key(k);
        settle(app);
    }

    fn cursor_to(app: &mut App, name: &str) {
        for _ in 0..20 {
            let pane = view(app).ctl.pane(Side::Left);
            let at = pane.cursor_index().map(|i| pane.rows()[i].name.clone());
            if at.as_deref() == Some(name) {
                return;
            }
            press(app, key(KeyCode::Down));
        }
        panic!("{name} not found");
    }

    #[test]
    fn the_explorer_opens_with_its_tree_and_tab_switches_to_the_list() {
        let dir = tempfile::tempdir().unwrap();
        let mut app = explorer_app(&fat_volume(dir.path()));
        let pane = view(&app).ctl.pane(Side::Left);
        assert!(pane.listing.is_loaded());
        assert!(pane.tree_focused());
        assert_eq!(pane.folder_tree().len(), 2, "the root and SUB");
        press(&mut app, key(KeyCode::Down));
        assert!(view(&app)
            .ctl
            .pane(Side::Left)
            .listing
            .cwd_path()
            .ends_with("SUB"));
        press(&mut app, key(KeyCode::Up));
        press(&mut app, key(KeyCode::Tab));
        assert!(!view(&app).ctl.pane(Side::Left).tree_focused());
    }

    #[test]
    fn the_editor_saves_with_f2_and_keeps_the_line_endings() {
        let dir = tempfile::tempdir().unwrap();
        let img = fat_volume(dir.path());
        let mut app = explorer_app(&img);
        press(&mut app, key(KeyCode::Tab));
        cursor_to(&mut app, "HELLO.TXT");
        press(&mut app, key(KeyCode::F(7)));
        assert!(view(&app).editor_open(), "F7 opens the editor");
        press(&mut app, key(KeyCode::End));
        for c in "!!".chars() {
            press(&mut app, key(KeyCode::Char(c)));
        }
        press(&mut app, key(KeyCode::F(2)));
        assert_eq!(root_file(&img, "HELLO.TXT").unwrap(), b"hello!!\r\n");
        press(&mut app, key(KeyCode::Esc));
        assert!(!view(&app).editor_open());
    }

    #[test]
    fn delete_asks_and_then_writes_at_once() {
        let dir = tempfile::tempdir().unwrap();
        let img = fat_volume(dir.path());
        let mut app = explorer_app(&img);
        press(&mut app, key(KeyCode::Tab));
        cursor_to(&mut app, "HELLO.TXT");
        press(&mut app, key(KeyCode::F(8)));
        press(&mut app, key(KeyCode::Char('y')));
        assert!(root_file(&img, "HELLO.TXT").is_none());
        assert!(view(&app).ctl.pane(Side::Left).queue.is_empty());
    }

    #[test]
    fn check_shows_a_report_and_a_transform_prefills_the_command_line() {
        let dir = tempfile::tempdir().unwrap();
        let mut app = explorer_app(&fat_volume(dir.path()));
        press(&mut app, ctrl('f'));
        let report = view(&app).ctl.pane(Side::Left).tool_report().cloned();
        assert!(report.is_some(), "Ctrl+F shows the check");
        press(&mut app, key(KeyCode::Esc));
        assert!(view(&app).ctl.pane(Side::Left).tool_report().is_none());
        press(&mut app, ctrl('t'));
        press(&mut app, key(KeyCode::Enter));
        let cmd = app
            .palette_input
            .clone()
            .expect("the transform prefills the command line");
        assert!(cmd.starts_with("convert "), "{cmd}");
    }

    #[test]
    fn esc_asks_before_closing_and_the_help_lists_every_bound_command() {
        let dir = tempfile::tempdir().unwrap();
        let mut app = explorer_app(&fat_volume(dir.path()));
        let help = view(&app).ctl.key_help();
        for cmd in Command::EXPLORER {
            assert!(
                help.iter().any(|(label, _)| *label == cmd.label()),
                "the Explorer binds {} but never lists it",
                cmd.name()
            );
        }
        press(&mut app, key(KeyCode::Esc));
        assert!(app.explorer.is_some(), "Esc asks first");
        press(&mut app, key(KeyCode::Char('y')));
        assert!(app.explorer.is_none());
    }

    fn screen(app: &App) -> String {
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

    #[test]
    fn the_tree_list_and_windows_draw_in_a_small_terminal() {
        let dir = tempfile::tempdir().unwrap();
        let mut app = explorer_app(&fat_volume(dir.path()));
        let text = screen(&app);
        assert!(text.contains("Explorer  Whole volume"), "{text}");
        assert!(text.contains("[-] /") && text.contains("SUB/"), "{text}");
        assert!(text.contains("HELLO.TXT"), "{text}");
        press(&mut app, ctrl('f'));
        assert!(screen(&app).contains("fsck - "), "{}", screen(&app));
        press(&mut app, key(KeyCode::Esc));
        press(&mut app, key(KeyCode::Tab));
        cursor_to(&mut app, "HELLO.TXT");
        press(&mut app, key(KeyCode::Enter));
        let text = screen(&app);
        assert!(
            text.contains("File Info: HELLO.TXT") && text.contains("hello"),
            "{text}"
        );
        press(&mut app, key(KeyCode::Esc));
        press(&mut app, ctrl('e'));
        assert!(screen(&app).contains("Export as"));
        press(&mut app, key(KeyCode::Esc));
        press(&mut app, key(KeyCode::F(7)));
        assert!(screen(&app).contains("Edit  HELLO.TXT"));
    }

    fn editor(text: &str) -> TextEditor {
        TextEditor::new(TextDoc {
            name: "f.txt".into(),
            folder: "/".into(),
            text: text.into(),
            shape: TextShape {
                encoding: crate::controller::browser::TextEncoding::Utf8,
                ending: LineEnding::Lf,
                mixed_endings: false,
            },
        })
    }

    #[test]
    fn the_editor_edits_text_the_way_typing_would() {
        let mut ed = editor("one\ntwo\nthree\n");
        // A trailing newline is a trailing empty line; losing it would rewrite untouched files.
        assert_eq!(ed.lines, vec!["one", "two", "three", ""]);
        assert!(!ed.dirty);
        ed.move_to(1, 3);
        for c in "-and-a-half".chars() {
            ed.insert(c);
        }
        assert_eq!(ed.lines[1], "two-and-a-half");
        assert!(ed.dirty);
        // Split a line, then join it back with backspace at column zero.
        ed.move_to(0, 3);
        ed.newline();
        assert_eq!((ed.lines[0].as_str(), ed.lines[1].as_str()), ("one", ""));
        ed.backspace();
        assert_eq!(ed.lines[0], "one");
        assert_eq!(ed.lines.len(), 4);
        // Delete at end-of-line joins with the next.
        ed.move_to(2, 5);
        ed.delete();
        assert_eq!(ed.lines[2], "three");
        assert_eq!(ed.text(), "one\ntwo-and-a-half\nthree");
    }

    /// The cursor is a character index, not a byte index.
    #[test]
    fn the_editor_handles_multibyte_characters() {
        let mut ed = editor("caf\u{e9} ok\n");
        ed.move_to(0, 4);
        ed.insert('!');
        assert_eq!(ed.lines[0], "caf\u{e9}! ok");
        ed.backspace();
        assert_eq!(ed.lines[0], "caf\u{e9} ok");
        ed.move_to(0, 3);
        ed.delete();
        assert_eq!(ed.lines[0], "caf ok");
    }

    #[test]
    fn the_editor_scrolls_to_keep_the_cursor_visible() {
        let body = (0..50).map(|i| format!("line {i}")).collect::<Vec<_>>();
        let mut ed = editor(&body.join("\n"));
        ed.move_to(40, 0);
        ed.follow_cursor(10);
        assert!(
            ed.scroll <= 40 && 40 < ed.scroll + 10,
            "scroll={}",
            ed.scroll
        );
        ed.move_to(0, 0);
        ed.follow_cursor(10);
        assert_eq!(ed.scroll, 0, "scrolling back up follows too");
    }
    /// An in-place source enters Edit Mode once listed; a SquashFS asks for its size budget first.
    #[test]
    fn a_squashfs_volume_asks_for_its_size_budget_before_edit_mode() {
        let dir = tempfile::tempdir().unwrap();
        let fat = explorer_app(&fat_volume(dir.path()));
        assert!(
            view(&fat).ctl.edit_mode(),
            "a FAT image is editable at once"
        );
        assert!(!view(&fat).ctl.pane(Side::Left).is_read_only());
        let img = crate::controller::test_fixtures::squashfs_volume(dir.path());
        let open = BrowseOpen {
            session: BrowseSession {
                source_path: Some(img),
                ..Default::default()
            },
            edit: BrowseEdit::InPlace,
            label_prefix: None,
            backup_folder: None,
        };
        let mut app = App::new_on(DEFAULT_TAB);
        app.explorer = Some(CommanderView::explorer_open(open, "Whole volume".into()));
        settle(&mut app);
        let form = view(&app)
            .ctl
            .size_budget()
            .copied()
            .expect("the budget is asked");
        assert_eq!(form.mode(), BudgetMode::Fit);
        assert!(!view(&app).ctl.edit_mode());
        press(&mut app, key(KeyCode::Down));
        assert_eq!(
            view(&app).ctl.size_budget().unwrap().mode(),
            BudgetMode::Absolute
        );
        press(&mut app, key(KeyCode::Down));
        let headroom = view(&app).ctl.size_budget().unwrap();
        assert_eq!(headroom.mode(), BudgetMode::Headroom);
        let before = headroom.headroom_mib();
        press(&mut app, key(KeyCode::Backspace));
        press(&mut app, key(KeyCode::Char('8')));
        assert_eq!(
            view(&app).ctl.size_budget().unwrap().headroom_mib(),
            before / 10 * 10 + 8
        );
        press(&mut app, key(KeyCode::Enter));
        assert!(view(&app).ctl.size_budget().is_none());
        assert!(view(&app).ctl.edit_mode(), "Start enters Edit Mode");
    }
}
