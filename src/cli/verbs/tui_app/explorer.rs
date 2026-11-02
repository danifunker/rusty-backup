//! The TUI Explorer: one browser pane under the Explorer profile, drawn over the Inspect tab.
//!
//! The Explorer is the Commander screen with one pane ([`CommanderController::single`]) whose
//! pane runs [`PaneProfile::explorer`]: the partition picked on the Inspect tab, a folder tree
//! beside the listing, edits written at once, and the volume tools (check, repair, bless, text
//! editor, import, metadata, transforms). [`CommanderView`] carries the keys, effects and the
//! shared windows; this file adds what only the Explorer draws: the tree, the text editor (its
//! buffer is widget state), the metadata form, the check / repair report and the transforms menu,
//! whose chosen command goes to the TUI's `:` command line.

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
use crate::controller::commander::{CommanderAction, CommanderController, Side};
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
    /// The Explorer over `path`, showing its `partition` (the first browsable one when `None`).
    pub(super) fn explorer(path: PathBuf, partition: Option<usize>, label: String) -> Self {
        let (keymap, problems) = load_keymap();
        let mut services = PaneServices::headless();
        // The Explorer remembers its listing style under "explorer" (recent files are not kept).
        services.recent = super::commander::recent_store();
        let mut ctl = CommanderController::single(services, keymap, PaneProfile::explorer());
        ctl.report_keymap_problems(problems);
        ctl.open_fixed(path, partition);
        let mut v = CommanderView::with_ctl(ctl);
        v.ex = Some(ExplorerState {
            label,
            editor: None,
            meta: None,
            transforms: None,
            report_scroll: 0,
            palette: None,
        });
        v
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
        let form = self.ctl.pane(Side::Left).metadata_form().cloned();
        let Some(ex) = self.ex.as_mut() else { return };
        if let Some(doc) = doc {
            ex.editor = Some(TextEditor::new(doc));
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
        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(3), Constraint::Length(1)])
            .split(area);
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
        frame.render_widget(
            Paragraph::new(Line::styled(format!(" {}", ctl.status()), pal.dim())),
            rows[1],
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

    fn explorer_app(img: &Path) -> App {
        let mut app = App::new_on(DEFAULT_TAB);
        app.explorer = Some(CommanderView::explorer(
            img.to_path_buf(),
            None,
            "Whole volume".into(),
        ));
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
}
