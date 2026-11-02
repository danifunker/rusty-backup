//! The TUI Commander: a view over [`CommanderController`] (CONTRIBUTING.md § Architecture policy (MVC)).
//!
//! Key presses become [`Key`] + [`Mods`] and go to the controller, whose key map decides what they
//! mean; a key it does not bind falls through to the tab shell (tab switching, quit, the palette).
//! This file draws the controller's state and carries out its effects: pickers through the TUI's
//! [`FilePicker`], confirmations and text prompts as small modals (a prompt runs the controller's
//! validator on every keystroke and Enter submits only while it passes), and the clipboard as a
//! text window, since a terminal has none. The windows the controller holds open (apply review and
//! report, host conflicts, the rename notice, staged edits, partitions, help, File Info,
//! checksums, the log) take their own buttons' keys here, as a dialog's buttons do in any view.

use std::collections::VecDeque;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Text};
use ratatui::widgets::{Clear, Paragraph, Wrap};
use ratatui::Frame;

use super::{centered_rect, App, FilePicker, PickKind, PickResult};
use crate::controller::commander::{
    format_size, purpose, CommanderAction, CommanderController, BrowserPaneAction,
    BrowserPaneController, DetailField, DisplayRow, FileDetail, HashAlgo, OnConflict, RowKind,
    Side, MAX_CONSECUTIVE_FAILURES,
};
use crate::controller::keys::{Command, Key, KeyMap, Mods};
use crate::controller::{ConfirmSpec, Controller, Effect, PasswordSpec, TextSpec};

type A = CommanderAction;
type P = BrowserPaneAction;

/// An effect waiting on the user, drawn over everything else.
enum Modal {
    Pick {
        purpose: &'static str,
        picker: FilePicker,
    },
    Confirm(ConfirmSpec),
    Text {
        spec: TextSpec,
        input: String,
        edited: bool,
    },
    Password {
        spec: PasswordSpec,
        input: String,
    },
    /// Text the controller wanted on the clipboard (or a link to open), shown instead.
    Show {
        title: String,
        text: String,
    },
}

/// The scrolling windows, topmost first, so a scroll position belongs to one of them.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Window {
    Help,
    Detail,
    Checksums,
    Log,
}

pub(super) struct CommanderView {
    ctl: CommanderController,
    modal: Option<Modal>,
    /// Effects that need the user, answered one at a time.
    waiting: VecDeque<Effect>,
    /// The unlock field of the active pane's locked source.
    password: String,
    review_replace: bool,
    part_sel: usize,
    edit_sel: usize,
    scroll: usize,
    scroll_window: Option<Window>,
    closed: bool,
    /// The controller passed a quit (no staged edits, or the user discarded them).
    quit: bool,
    /// Held while jobs run, whose engine code may print to stderr.
    muted: Option<super::MutedStderr>,
}

impl CommanderView {
    pub(super) fn new() -> Self {
        // The event loop polls every 50 ms while a job runs, so the controller needs no wake.
        let (keymap, problems) = load_keymap();
        let mut ctl = CommanderController::polled(recent_store(), recent_store(), keymap);
        ctl.report_keymap_problems(problems);
        Self {
            ctl,
            modal: None,
            waiting: VecDeque::new(),
            password: String::new(),
            review_replace: false,
            part_sel: 0,
            edit_sel: 0,
            scroll: 0,
            scroll_window: None,
            closed: false,
            quit: false,
            muted: None,
        }
    }

    pub(super) fn closed(&self) -> bool {
        self.closed
    }

    pub(super) fn quit(&self) -> bool {
        self.quit
    }

    pub(super) fn has_staged_edits(&self) -> bool {
        self.ctl.has_staged_edits()
    }

    /// Ask the controller to quit; it confirms first while edits are staged.
    pub(super) fn request_quit(&mut self) {
        self.send(A::RequestQuit);
    }

    pub(super) fn is_working(&self) -> bool {
        self.ctl.is_working()
    }

    /// Drain finished jobs; called on every turn of the event loop.
    pub(super) fn tick(&mut self) {
        let fx = self.ctl.tick();
        self.run(fx);
    }

    fn send(&mut self, action: CommanderAction) {
        let fx = self.ctl.apply(action);
        self.run(fx);
    }

    /// Mute stderr while a job runs (tests keep it, so their failures stay readable).
    fn sync_mute(&mut self) {
        if !self.ctl.is_working() || cfg!(test) {
            self.muted = None;
        } else if self.muted.is_none() {
            self.muted = super::MutedStderr::new();
        }
    }

    fn run(&mut self, fx: Vec<Effect>) {
        for effect in fx {
            match effect {
                Effect::CloseScreen => self.closed = true,
                Effect::Quit => self.quit = true,
                // The controller turns pane messages into its status line; a stray one goes there too.
                Effect::Message(_) => {}
                other => self.waiting.push_back(other),
            }
        }
        self.next_modal();
        self.sync_mute();
    }

    fn next_modal(&mut self) {
        while self.modal.is_none() {
            let Some(effect) = self.waiting.pop_front() else {
                return;
            };
            self.modal = match effect {
                Effect::PickFile(spec) => {
                    let kind = if spec.save {
                        PickKind::Save
                    } else {
                        PickKind::File
                    };
                    let mut picker = FilePicker::new(kind, spec.title);
                    if spec.save {
                        picker.input = picker.dir.join(&spec.file_name).display().to_string();
                    }
                    let recent = match self.ctl.asker(spec.purpose) {
                        Some(side) if spec.purpose == purpose::OPEN_IMAGE => {
                            self.ctl.pane(side).recent().to_vec()
                        }
                        _ => Vec::new(),
                    };
                    Some(Modal::Pick {
                        purpose: spec.purpose,
                        picker: picker.with_recent(recent),
                    })
                }
                Effect::PickFolder(spec) => Some(Modal::Pick {
                    purpose: spec.purpose,
                    picker: FilePicker::new(PickKind::Dir, spec.title),
                }),
                Effect::Confirm(spec) => Some(Modal::Confirm(spec)),
                Effect::AskText(spec) => Some(Modal::Text {
                    input: spec.initial.clone(),
                    spec,
                    edited: false,
                }),
                Effect::AskPassword(spec) => Some(Modal::Password {
                    spec,
                    input: String::new(),
                }),
                Effect::CopyToClipboard(text) => Some(Modal::Show {
                    title: "Copy (no clipboard in a terminal)".into(),
                    text,
                }),
                Effect::OpenUrl(url) => Some(Modal::Show {
                    title: "Open in a browser".into(),
                    text: url,
                }),
                Effect::Message(_) | Effect::CloseScreen | Effect::Quit => None,
            };
        }
    }

    // --- keys ---------------------------------------------------------------

    /// Handle a key press; `false` hands it back to the tab shell.
    pub(super) fn handle_key(&mut self, key: KeyEvent) -> bool {
        if self.modal.is_some() {
            self.modal_key(key);
            return true;
        }
        self.sync_scroll();
        if let Some(done) = self.window_key(key) {
            return done;
        }
        // The shell's help key opens the screen's own key list.
        if key.code == KeyCode::Char('?') {
            self.send(A::ShowHelp(true));
            return true;
        }
        let Some((k, m)) = translate(key) else {
            return false;
        };
        if !self.ctl.binds(k, m) {
            return false;
        }
        let page = page_rows();
        self.ctl.apply(A::SetPageRows(Side::Left, page));
        self.ctl.apply(A::SetPageRows(Side::Right, page));
        self.send(A::Key(k, m));
        true
    }

    fn top_window(&self) -> Option<Window> {
        if self.ctl.help_shown() {
            Some(Window::Help)
        } else if self.ctl.detail().is_some() {
            Some(Window::Detail)
        } else if self.ctl.checksums().is_some() {
            Some(Window::Checksums)
        } else if self.ctl.log_shown() {
            Some(Window::Log)
        } else {
            None
        }
    }

    fn sync_scroll(&mut self) {
        let top = self.top_window();
        if top != self.scroll_window {
            self.scroll_window = top;
            self.scroll = 0;
        }
    }

    /// Keys for the window on top; `None` lets the key map have the key.
    fn window_key(&mut self, key: KeyEvent) -> Option<bool> {
        let code = key.code;
        if let Some((_, snap)) = self.ctl.copy_job() {
            if code == KeyCode::Esc && snap.can_cancel {
                self.send(A::CancelCopy);
            }
            return Some(true);
        }
        let sides = [Side::Left, Side::Right];
        if sides.iter().any(|s| {
            let p = self.ctl.pane(*s);
            p.applying().is_some() || p.checking().is_some()
        }) {
            return Some(true);
        }
        if self.ctl.host_conflict().is_some() {
            match code {
                KeyCode::Char('o') => self.send(A::HostConflict(OnConflict::Replace)),
                KeyCode::Char('s') => self.send(A::HostConflict(OnConflict::Skip)),
                KeyCode::Esc | KeyCode::Char('c') => self.send(A::HostConflictCancel),
                _ => {}
            }
            return Some(true);
        }
        if self.ctl.rename_notice().is_some() {
            match code {
                KeyCode::Enter | KeyCode::Esc => self.send(A::RenameNoticeOk),
                KeyCode::Char('u') => self.send(A::RenameNoticeUndo),
                _ => {}
            }
            return Some(true);
        }
        for side in sides {
            let pane = self.ctl.pane(side);
            if let Some(review) = pane.apply_review() {
                let conflicts = !review.scan.conflicts.is_empty();
                match code {
                    KeyCode::Left | KeyCode::Right | KeyCode::Char(' ') if conflicts => {
                        self.review_replace = !self.review_replace
                    }
                    KeyCode::Enter => {
                        let on = if self.review_replace {
                            OnConflict::Replace
                        } else {
                            OnConflict::Skip
                        };
                        self.review_replace = false;
                        self.send(A::Pane(side, P::ApplyReviewed(on)));
                    }
                    KeyCode::Esc => {
                        self.review_replace = false;
                        self.send(A::Pane(side, P::DismissReview));
                    }
                    _ => {}
                }
                return Some(true);
            }
            if pane.apply_report().is_some() {
                let still = !pane.queue.is_empty();
                let then = match code {
                    KeyCode::Char('r') if still => Some(Some(P::Apply)),
                    KeyCode::Char('d') if still => Some(Some(P::Discard)),
                    KeyCode::Enter | KeyCode::Esc => Some(None),
                    _ => None,
                };
                if let Some(then) = then {
                    self.send(A::Pane(side, P::DismissReport));
                    if let Some(action) = then {
                        self.send(A::Pane(side, action));
                    }
                }
                return Some(true);
            }
        }
        if let Some(side) = self.ctl.partitions_shown() {
            let n = self.ctl.pane(side).partition_labels().len();
            match code {
                KeyCode::Up => self.part_sel = self.part_sel.saturating_sub(1),
                KeyCode::Down => self.part_sel = (self.part_sel + 1).min(n.saturating_sub(1)),
                KeyCode::Enter => self.send(A::Pane(side, P::SelectPartition(self.part_sel))),
                KeyCode::Esc => self.send(A::ShowPartitions(false)),
                _ => {}
            }
            return Some(true);
        } else {
            let side = self.ctl.active();
            self.part_sel = self.ctl.pane(side).selected_partition().unwrap_or(0);
        }
        let active = self.ctl.active();
        if self.ctl.pane(active).edits_shown() {
            let n = self.ctl.pane(active).edit_lines().len();
            self.edit_sel = self.edit_sel.min(n.saturating_sub(1));
            match code {
                KeyCode::Up => self.edit_sel = self.edit_sel.saturating_sub(1),
                KeyCode::Down => self.edit_sel = (self.edit_sel + 1).min(n.saturating_sub(1)),
                KeyCode::Delete | KeyCode::Char('x') => {
                    self.send(A::Pane(active, P::Unstage(self.edit_sel)))
                }
                KeyCode::Char('d') => self.send(A::Pane(active, P::Discard)),
                KeyCode::Esc => self.send(A::Pane(active, P::ShowEdits(false))),
                _ => return None,
            }
            return Some(true);
        }
        if let Some(window) = self.top_window() {
            if self.scroll_key(code) {
                return Some(true);
            }
            let button = match (window, code) {
                (Window::Checksums, KeyCode::Char('c')) => Some(A::CopyChecksums),
                (Window::Log, KeyCode::Char('c')) => Some(A::CopyLog),
                (Window::Log, KeyCode::Char('x')) => Some(A::ClearLog),
                (Window::Detail, KeyCode::Char('m')) => Some(A::EditDetail(DetailField::Mode)),
                (Window::Detail, KeyCode::Char('o')) => Some(A::EditDetail(DetailField::Owner)),
                (Window::Detail, KeyCode::Char('x')) => Some(A::EditDetail(DetailField::Xattr)),
                _ => None,
            };
            if let Some(action) = button {
                self.send(action);
                return Some(true);
            }
            return None;
        }
        let pane = self.ctl.pane(active);
        if pane.needs_password() {
            let plain = !key
                .modifiers
                .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT);
            match code {
                KeyCode::Char(c) if plain => self.password.push(c),
                KeyCode::Backspace => {
                    self.password.pop();
                }
                KeyCode::Enter => {
                    let secret = std::mem::take(&mut self.password);
                    self.send(A::Pane(active, P::SubmitPassword(secret)));
                }
                _ => return None,
            }
            return Some(true);
        }
        if code == KeyCode::Esc && pane.opening().is_some() {
            self.send(A::Pane(active, P::CancelOpen));
            return Some(true);
        }
        None
    }

    fn scroll_key(&mut self, code: KeyCode) -> bool {
        let page = page_rows().max(1) as usize;
        match code {
            KeyCode::Up => self.scroll = self.scroll.saturating_sub(1),
            KeyCode::Down => self.scroll += 1,
            KeyCode::PageUp => self.scroll = self.scroll.saturating_sub(page),
            KeyCode::PageDown => self.scroll += page,
            KeyCode::Home => self.scroll = 0,
            // Clamped when drawn.
            KeyCode::End => self.scroll = usize::MAX / 2,
            _ => return false,
        }
        true
    }

    fn modal_key(&mut self, key: KeyEvent) {
        let Some(modal) = self.modal.as_mut() else {
            return;
        };
        let code = key.code;
        let answer = match modal {
            Modal::Pick { purpose, picker } => match picker.handle_key(code) {
                Some(PickResult::Cancel) => Some(A::Picked {
                    purpose,
                    path: None,
                }),
                Some(PickResult::Confirm(path)) => Some(A::Picked {
                    purpose,
                    path: Some(path),
                }),
                None => None,
            },
            Modal::Confirm(spec) => match code {
                KeyCode::Char('y') | KeyCode::Char('Y') | KeyCode::Enter => Some(A::Confirmed {
                    purpose: spec.purpose,
                    yes: true,
                }),
                KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => Some(A::Confirmed {
                    purpose: spec.purpose,
                    yes: false,
                }),
                _ => None,
            },
            Modal::Text {
                spec,
                input,
                edited,
            } => match code {
                KeyCode::Esc => Some(A::Answered {
                    purpose: spec.purpose,
                    text: None,
                }),
                KeyCode::Enter if spec.validate(input).is_ok() => Some(A::Answered {
                    purpose: spec.purpose,
                    text: Some(input.clone()),
                }),
                KeyCode::Backspace => {
                    input.pop();
                    *edited = true;
                    None
                }
                KeyCode::Char(c) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                    input.push(c);
                    *edited = true;
                    None
                }
                _ => None,
            },
            Modal::Password { spec, input } => match code {
                KeyCode::Esc => Some(A::Answered {
                    purpose: spec.purpose,
                    text: None,
                }),
                KeyCode::Enter => Some(A::Answered {
                    purpose: spec.purpose,
                    text: Some(std::mem::take(input)),
                }),
                KeyCode::Backspace => {
                    input.pop();
                    None
                }
                KeyCode::Char(c) => {
                    input.push(c);
                    None
                }
                _ => None,
            },
            Modal::Show { .. } => {
                if matches!(code, KeyCode::Esc | KeyCode::Enter) {
                    self.modal = None;
                    self.next_modal();
                }
                return;
            }
        };
        if let Some(action) = answer {
            self.modal = None;
            self.send(action);
        }
    }

    /// The footer's key hints, from the key map.
    pub(super) fn footer_keys(&self) -> Vec<(String, &'static str)> {
        let cmds = [
            (Command::SwitchPane, "Pane"),
            (Command::Copy, "Copy"),
            (Command::Delete, "Delete"),
            (Command::Apply, "Apply"),
            (Command::OpenImage, "Image"),
            (Command::OpenFolder, "Folder"),
            (Command::Help, "Keys"),
            (Command::Close, "Close"),
        ];
        let hints = cmds.iter().map(|(c, d)| (self.ctl.key_hint(*c), *d));
        hints.filter(|(k, _)| !k.is_empty()).collect()
    }
}

/// A terminal key as the key map names it; crossterm reports Shift+Tab as `BackTab`.
fn translate(key: KeyEvent) -> Option<(Key, Mods)> {
    let m = key.modifiers;
    let mut mods = Mods {
        ctrl: m.contains(KeyModifiers::CONTROL),
        alt: m.contains(KeyModifiers::ALT),
        shift: m.contains(KeyModifiers::SHIFT),
        cmd: false,
    };
    let k = match key.code {
        KeyCode::F(n) => Key::F(n),
        KeyCode::Char(' ') => Key::Space,
        KeyCode::Char(c) => {
            mods.shift |= c.is_ascii_uppercase();
            Key::Char(c.to_ascii_lowercase())
        }
        KeyCode::Tab => Key::Tab,
        KeyCode::BackTab => {
            mods.shift = true;
            Key::Tab
        }
        KeyCode::Enter => Key::Enter,
        KeyCode::Esc => Key::Escape,
        KeyCode::Backspace => Key::Backspace,
        KeyCode::Delete => Key::Delete,
        KeyCode::Insert => Key::Insert,
        KeyCode::Up => Key::Up,
        KeyCode::Down => Key::Down,
        KeyCode::Left => Key::Left,
        KeyCode::Right => Key::Right,
        KeyCode::PageUp => Key::PageUp,
        KeyCode::PageDown => Key::PageDown,
        KeyCode::Home => Key::Home,
        KeyCode::End => Key::End,
        _ => return None,
    };
    Some((k, mods))
}

/// Listing rows that fit a pane: the terminal less the tab bar, footer, status, borders and path.
fn page_rows() -> i32 {
    let rows = crossterm::terminal::size().map(|(_, r)| r).unwrap_or(24);
    i32::from(rows.saturating_sub(6)).max(1)
}

#[cfg(not(test))]
fn recent_store() -> Box<dyn crate::controller::commander::RecentStore> {
    use crate::controller::app::{CommanderRecent, UserConfig};
    Box::new(CommanderRecent(UserConfig))
}

/// Tests never touch the user's recent files.
#[cfg(test)]
fn recent_store() -> Box<dyn crate::controller::commander::RecentStore> {
    Box::new(crate::controller::commander::NoRecent)
}

#[cfg(not(test))]
fn load_keymap() -> (KeyMap, Vec<String>) {
    KeyMap::load_terminal(KeyMap::user_file().as_deref())
}

#[cfg(test)]
fn load_keymap() -> (KeyMap, Vec<String>) {
    (KeyMap::for_terminal(), Vec::new())
}

/// Control characters in a name (the CR in `Icon\r`) as escapes, so a row stays one line.
fn printable(name: &str) -> String {
    name.chars()
        .map(|c| {
            if (c as u32) < 0x20 {
                format!("\\x{:02X}", c as u32)
            } else {
                c.to_string()
            }
        })
        .collect()
}

fn clip(text: &str, width: usize) -> String {
    let n = text.chars().count();
    if n <= width {
        return format!("{text:<width$}");
    }
    let keep: String = text.chars().take(width.saturating_sub(3)).collect();
    format!("{keep}...")
}

// --- drawing ------------------------------------------------------------------

impl App {
    pub(super) fn draw_commander(&self, frame: &mut Frame, area: Rect, v: &CommanderView) {
        let ctl = &v.ctl;
        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(3), Constraint::Length(1)])
            .split(area);
        let cols = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(rows[0]);
        for (side, col) in [(Side::Left, cols[0]), (Side::Right, cols[1])] {
            self.draw_cmd_pane(frame, col, v, side);
        }
        frame.render_widget(
            Paragraph::new(Line::styled(
                format!(" {}", ctl.status()),
                self.palette.dim(),
            )),
            rows[1],
        );
        self.draw_cmd_windows(frame, area, v);
        self.draw_cmd_reviews(frame, area, v);
        if let Some((title, snap)) = ctl.copy_job() {
            self.draw_job_box(frame, area, title, &snap);
        }
        for side in [Side::Left, Side::Right] {
            if let Some(snap) = ctl.pane(side).applying() {
                let title = format!("Applying {} pane edits", side.label());
                self.draw_job_box(frame, area, &title, &snap);
            }
        }
        if let Some(snap) = ctl.checksums().and_then(|c| c.running()) {
            self.draw_job_box(frame, area, "Checksums", &snap);
        }
        self.draw_cmd_modal(frame, area, v);
    }

    fn pane_title(&self, pane: &BrowserPaneController, side: Side) -> String {
        let source = if let Some(label) = pane.remote_label() {
            label
        } else if let Some(path) = pane.source_path() {
            if pane.listing.is_host() {
                path.display().to_string()
            } else {
                let name = path.file_name().map(|n| n.to_string_lossy().into_owned());
                name.unwrap_or_else(|| path.display().to_string())
            }
        } else {
            "(empty)".to_string()
        };
        let mut t = format!("{}: {source}", side.label());
        let vol = pane.volume().name();
        if pane.listing.is_loaded() && !pane.listing.is_host() && !vol.is_empty() {
            t.push_str(&format!(" - {vol}"));
        }
        if pane.is_read_only() {
            t.push_str(" [read-only]");
        }
        let n = pane.queue.len();
        if n > 0 {
            t.push_str(&format!(" [{n} staged]"));
        }
        t
    }

    fn draw_cmd_pane(&self, frame: &mut Frame, area: Rect, v: &CommanderView, side: Side) {
        let ctl = &v.ctl;
        let pane = ctl.pane(side);
        let focused = ctl.active() == side;
        let pal = self.palette;
        let width = area.width.saturating_sub(2) as usize;
        let mut title = self.pane_title(pane, side);
        if title.chars().count() > width.saturating_sub(2) {
            let skip = title.chars().count() - width.saturating_sub(5);
            title = format!("...{}", title.chars().skip(skip).collect::<String>());
        }
        let mut lines: Vec<Line> = Vec::new();
        if pane.listing.is_loaded() {
            let mut path = pane.listing.cwd_path().to_string();
            let labels = pane.partition_labels();
            if let (true, Some(i)) = (labels.len() > 1, pane.selected_partition()) {
                path = format!("[{}] {path}", labels[i]);
            }
            lines.push(Line::styled(clip(&path, width), pal.dim()));
        }
        if let Some((name, snap)) = pane.wrapper_opening() {
            let what = if snap.phase.is_empty() {
                format!("Opening {name}...")
            } else {
                format!("{name}: {}", snap.phase)
            };
            lines.push(Line::styled(what, pal.accent()));
        }
        if pane.checking().is_some() {
            lines.push(Line::styled("Checking staged edits...", pal.accent()));
        }
        if let Some(snap) = pane.host_writing() {
            lines.push(Line::styled(
                format!("{}  {}", snap.label, snap.detail()),
                pal.accent(),
            ));
        }
        if pane.applying().is_some() {
            lines.push(Line::styled("Applying staged edits...", pal.accent()));
        } else if let Some(snap) = pane.opening() {
            let what = if snap.phase.is_empty() {
                snap.label.clone()
            } else {
                snap.phase.clone()
            };
            lines.push(Line::raw(""));
            lines.push(Line::styled(format!(" {what}"), pal.accent()));
            lines.push(Line::raw(format!(" {}", snap.detail())));
            lines.push(Line::styled(" Esc cancel", pal.dim()));
        } else if pane.is_opening() {
            lines.push(Line::styled(" Opening...", pal.accent()));
        } else if pane.needs_password() {
            let apfs = pane.volume().fs_type == "APFS";
            lines.push(Line::raw(""));
            lines.push(Line::raw(if apfs {
                " This APFS volume is encrypted with FileVault."
            } else {
                " This source is password-protected."
            }));
            let label = if apfs { "Passphrase" } else { "Password" };
            let typed = if focused { v.password.len() } else { 0 };
            let stars: String = "*".repeat(typed);
            lines.push(Line::styled(format!(" {label}: {stars}_"), pal.accent()));
            lines.push(Line::styled(" Enter unlock", pal.dim()));
        } else if let Some(err) = pane.source_error() {
            lines.push(Line::raw(""));
            lines.push(Line::styled(format!(" {err}"), pal.warn()));
        } else if pane.listing.is_loaded() {
            let visible = (area.height as usize).saturating_sub(2 + lines.len());
            self.cmd_rows(pane, focused, width, visible, &mut lines);
        } else {
            lines.push(Line::raw(""));
            lines.push(Line::styled(" Empty pane.", pal.dim()));
            lines.push(Line::raw(""));
            let opens = [
                (Command::OpenImage, "open a disk image or archive"),
                (Command::OpenFolder, "open a host folder"),
                (Command::OpenBackup, "open a backup folder"),
                (Command::Connect, "connect to an rb-cli serve daemon"),
            ];
            for (cmd, what) in opens {
                let hint = ctl.key_hint(cmd);
                if !hint.is_empty() {
                    lines.push(Line::raw(format!(" {hint:<8} {what}")));
                }
            }
        }
        frame.render_widget(
            Paragraph::new(Text::from(lines)).block(self.pane_block(&title, focused)),
            area,
        );
    }

    fn cmd_rows(
        &self,
        pane: &BrowserPaneController,
        focused: bool,
        width: usize,
        visible: usize,
        lines: &mut Vec<Line<'static>>,
    ) {
        let rows = pane.rows();
        let cursor = pane.cursor_index().unwrap_or(0);
        let start = cursor.saturating_sub(visible.saturating_sub(1));
        let name_w = width.saturating_sub(2 + 11 + 7).max(8);
        for (i, row) in rows.iter().enumerate().skip(start).take(visible) {
            let marked = if row.is_tree {
                pane.tree_selected.contains(&row.node_id)
            } else {
                !row.is_parent() && pane.listing.is_selected(&row.name)
            };
            let text = row_text(row, marked, name_w);
            let mut style = match row.kind {
                RowKind::PendingAdd => self.palette.styled(Color::Green),
                RowKind::PendingDelete => self
                    .palette
                    .styled(Color::Red)
                    .add_modifier(Modifier::CROSSED_OUT),
                RowKind::PendingRename => self.palette.warn(),
                _ if row.is_image || row.is_wrapper => self.palette.styled(Color::Magenta),
                _ if row.is_dir => self.palette.accent(),
                _ => Style::default(),
            };
            if marked {
                style = style.add_modifier(Modifier::BOLD);
            }
            if focused && i == cursor {
                style = style.add_modifier(Modifier::REVERSED);
            }
            lines.push(Line::styled(text, style));
        }
    }

    /// The help, File Info, checksum and log windows; only the topmost is drawn.
    fn draw_cmd_windows(&self, frame: &mut Frame, area: Rect, v: &CommanderView) {
        let ctl = &v.ctl;
        let pal = self.palette;
        let (title, mut lines, foot) = match v.top_window() {
            Some(Window::Help) => {
                let keys = ctl.key_help();
                let lines = keys
                    .into_iter()
                    .map(|(label, chord)| Line::raw(format!(" {chord:<12} {label}")))
                    .collect();
                (
                    "Keyboard shortcuts".to_string(),
                    lines,
                    ctl.key_help_footer(),
                )
            }
            Some(Window::Detail) => {
                let Some(d) = ctl.detail() else { return };
                let mut lines = detail_lines(d)
                    .into_iter()
                    .map(|l| Line::raw(format!(" {l}")))
                    .collect::<Vec<_>>();
                if let Some(result) = &d.result {
                    lines.push(Line::raw(""));
                    lines.push(Line::styled(format!(" {result}"), pal.accent()));
                }
                let foot = if d.editable {
                    "m mode   o owner   x xattr   Esc close"
                } else {
                    "Esc close"
                };
                (format!("File Info: {}", d.entry.name), lines, foot.into())
            }
            Some(Window::Checksums) => {
                let Some(c) = ctl.checksums() else { return };
                let mut lines = Vec::new();
                if let Some(err) = &c.error {
                    lines.push(Line::styled(format!(" {err}"), pal.warn()));
                }
                for row in &c.rows {
                    let size = format_size(row.size);
                    lines.push(Line::styled(
                        format!(" {}  ({size})", row.name),
                        pal.accent(),
                    ));
                    if let Some(err) = &row.error {
                        lines.push(Line::styled(format!("   failed: {err}"), pal.warn()));
                    }
                    if let Some(values) = &row.values {
                        for (algo, value) in HashAlgo::ALL.iter().zip(values) {
                            lines.push(Line::raw(format!("   {:<7} {value}", algo.label())));
                        }
                    }
                }
                (c.title.clone(), lines, "c copy all   Esc close".into())
            }
            Some(Window::Log) => {
                let log = ctl.log();
                let mut lines: Vec<Line> = log.iter().map(|l| Line::raw(format!(" {l}"))).collect();
                if lines.is_empty() {
                    lines.push(Line::styled(" No operations yet.", pal.dim()));
                }
                (
                    "Session log".to_string(),
                    lines,
                    "c copy all   x clear   Esc close".into(),
                )
            }
            None => return,
        };
        let h = area.height.saturating_sub(4).clamp(8, 30);
        let inner = h.saturating_sub(3) as usize;
        let max = lines.len().saturating_sub(inner);
        let start = v.scroll.min(max);
        lines = lines.into_iter().skip(start).take(inner).collect();
        lines.push(Line::styled(format!(" {foot}   Up/Down scroll"), pal.dim()));
        let w = area.width.saturating_sub(4).min(96);
        self.draw_cmd_box(frame, centered_rect(w, h, area), &title, lines);
    }

    /// Apply review and report, host conflicts, the rename notice, staged edits and partitions.
    fn draw_cmd_reviews(&self, frame: &mut Frame, area: Rect, v: &CommanderView) {
        let ctl = &v.ctl;
        let pal = self.palette;
        let active = ctl.active();
        if let Some(side) = ctl.partitions_shown() {
            let pane = ctl.pane(side);
            let mut lines = Vec::new();
            for (i, label) in pane.partition_labels().iter().enumerate() {
                let style = if i == v.part_sel {
                    pal.accent().add_modifier(Modifier::REVERSED)
                } else {
                    Style::default()
                };
                lines.push(Line::styled(format!(" {label}"), style));
            }
            lines.push(Line::styled(" Enter open   Esc cancel", pal.dim()));
            let title = format!("Choose partition ({})", side.label());
            self.draw_cmd_box_auto(frame, area, 64, &title, lines);
        }
        let pane = ctl.pane(active);
        if pane.edits_shown() {
            let mut lines = Vec::new();
            for (i, line) in pane.edit_lines().iter().enumerate() {
                let style = if i == v.edit_sel {
                    pal.accent().add_modifier(Modifier::REVERSED)
                } else {
                    Style::default()
                };
                lines.push(Line::styled(format!(" {line}"), style));
            }
            lines.push(Line::styled(
                " x remove   d discard all   Esc close",
                pal.dim(),
            ));
            let title = format!("Staged edits ({})", active.label());
            self.draw_cmd_box_auto(frame, area, 80, &title, lines);
        }
        for side in [Side::Left, Side::Right] {
            let pane = ctl.pane(side);
            if let Some(p) = pane.apply_review() {
                let mut lines = Vec::new();
                if p.shortfall() > 0 {
                    lines.push(Line::styled(
                        format!(
                            " Not enough space: needs about {}, {} free on {}.",
                            format_size(p.bytes_needed),
                            format_size(p.bytes_free),
                            p.fs_type
                        ),
                        pal.warn(),
                    ));
                }
                if !p.scan.conflicts.is_empty() {
                    lines.push(Line::raw(format!(
                        " {} item(s) already exist on {}:",
                        p.scan.conflicts.len(),
                        p.fs_type
                    )));
                    for c in p.scan.conflicts.iter().take(8) {
                        lines.push(Line::raw(format!("   {}", c.0)));
                    }
                    let (skip, over) = if v.review_replace {
                        ("( )", "(*)")
                    } else {
                        ("(*)", "( )")
                    };
                    lines.push(Line::styled(
                        format!(" {skip} Skip existing   {over} Overwrite   (Space switches)"),
                        pal.accent(),
                    ));
                }
                if !p.scan.blocked_folders.is_empty() {
                    lines.push(Line::raw(format!(
                        " {} folder(s) can't be created because a file has the same name.",
                        p.scan.blocked_folders.len()
                    )));
                }
                if !p.scan.missing_folders.is_empty() {
                    lines.push(Line::raw(format!(
                        " {} destination folder(s) no longer exist.",
                        p.scan.missing_folders.len()
                    )));
                }
                lines.push(Line::raw(
                    " Items that fail are listed at the end; everything else is still written.",
                ));
                let go = if p.shortfall() > 0 {
                    "Apply anyway"
                } else {
                    "Apply"
                };
                lines.push(Line::styled(format!(" Enter {go}   Esc cancel"), pal.dim()));
                let title = format!("Before applying ({})", side.label());
                self.draw_cmd_box_auto(frame, area, 84, &title, lines);
            }
            if let Some(r) = pane.apply_report() {
                let still = pane.queue.len();
                let mut lines = Vec::new();
                if let Some(fatal) = &r.fatal {
                    lines.push(Line::styled(format!(" {fatal}"), pal.warn()));
                }
                lines.push(Line::raw(format!(
                    " Applied {} of {} edit(s). {still} still staged.",
                    r.applied, r.total
                )));
                for f in r.failed.iter().take(12) {
                    lines.push(Line::styled(
                        format!("   {}: {}", f.label, f.error),
                        pal.warn(),
                    ));
                }
                if r.failed.len() > 12 {
                    let more = r.failed.len() - 12;
                    lines.push(Line::raw(format!("   ... and {more} more (see the log)")));
                }
                if r.not_tried > 0 {
                    lines.push(Line::raw(format!(
                        " Stopped after {MAX_CONSECUTIVE_FAILURES} failures in a row; \
                         {} edit(s) were not tried.",
                        r.not_tried
                    )));
                }
                let keys = if still > 0 {
                    " r retry   d discard remaining   Enter close"
                } else {
                    " Enter close"
                };
                lines.push(Line::styled(keys, pal.dim()));
                let title = format!("Apply report ({})", side.label());
                self.draw_cmd_box_auto(frame, area, 84, &title, lines);
            }
        }
        if let Some(names) = ctl.host_conflict() {
            let mut lines = vec![Line::raw(format!(
                " {} item(s) already exist in the destination folder:",
                names.len()
            ))];
            for name in names.iter().take(10) {
                lines.push(Line::raw(format!("   {}", printable(name))));
            }
            if names.len() > 10 {
                lines.push(Line::raw(format!("   ... and {} more", names.len() - 10)));
            }
            lines.push(Line::styled(
                " o overwrite   s skip existing   Esc cancel",
                pal.dim(),
            ));
            self.draw_cmd_box_auto(frame, area, 72, "Files already exist", lines);
        }
        if let Some(notice) = ctl.rename_notice() {
            let out = &notice.outcome;
            let mut lines = Vec::new();
            if !out.renamed.is_empty() {
                lines.push(Line::raw(format!(
                    " {} item(s) have names {} can't store and were renamed:",
                    out.renamed.len(),
                    out.fs_type
                )));
                for c in out.renamed.iter().take(10) {
                    lines.push(Line::raw(format!("   {} -> {}", printable(&c.from), c.to)));
                }
            }
            if !out.dropped.is_empty() {
                lines.push(Line::raw(format!(
                    " {} item(s) have no usable name on {} and were left out:",
                    out.dropped.len(),
                    out.fs_type
                )));
                for (path, why) in out.dropped.iter().take(6) {
                    lines.push(Line::raw(format!("   {} ({why})", printable(path))));
                }
            }
            lines.push(Line::styled(" Enter OK   u undo copy", pal.dim()));
            let title = format!("Names changed for {}", out.fs_type);
            self.draw_cmd_box_auto(frame, area, 80, &title, lines);
        }
    }

    fn draw_cmd_modal(&self, frame: &mut Frame, area: Rect, v: &CommanderView) {
        let pal = self.palette;
        let Some(modal) = &v.modal else { return };
        let whose = |purpose: &str| {
            let side = v.ctl.asker(purpose);
            side.map(|s| format!(" ({})", s.label()))
                .unwrap_or_default()
        };
        match modal {
            Modal::Pick { picker, .. } => picker.draw(frame, area, pal, self.border),
            Modal::Confirm(spec) => {
                let style = if spec.destructive {
                    pal.warn()
                } else {
                    pal.accent()
                };
                let lines = vec![
                    Line::raw(format!(" {}", spec.message)),
                    Line::raw(""),
                    Line::styled(format!(" y {}   n cancel", spec.confirm_label), style),
                ];
                let title = format!("{}{}", spec.title, whose(spec.purpose));
                self.draw_cmd_box_auto(frame, area, 70, &title, lines);
            }
            Modal::Text {
                spec,
                input,
                edited,
            } => {
                let error = match (&spec.error, *edited) {
                    (Some(e), false) => Some(e.clone()),
                    _ => spec.validate(input).err().filter(|e| !e.is_empty()),
                };
                let ok = spec.validate(input).is_ok();
                let mut lines = vec![
                    Line::raw(format!(" {}", spec.prompt)),
                    Line::styled(format!(" > {input}_"), pal.accent()),
                ];
                lines.push(match error {
                    Some(e) => Line::styled(format!(" {e}"), pal.warn()),
                    None => Line::raw(""),
                });
                let enter = if ok {
                    format!(" Enter {}   Esc cancel", spec.ok_label)
                } else {
                    " Esc cancel".to_string()
                };
                lines.push(Line::styled(enter, pal.dim()));
                let title = format!("{}{}", spec.title, whose(spec.purpose));
                self.draw_cmd_box_auto(frame, area, 66, &title, lines);
            }
            Modal::Password { spec, input } => {
                let lines = vec![
                    Line::raw(format!(" {}", spec.prompt)),
                    Line::styled(format!(" > {}_", "*".repeat(input.len())), pal.accent()),
                    Line::styled(" Enter submit   Esc cancel", pal.dim()),
                ];
                self.draw_cmd_box_auto(frame, area, 60, "Password", lines);
            }
            Modal::Show { title, text } => {
                let mut lines: Vec<Line> = text
                    .lines()
                    .take(area.height.saturating_sub(6) as usize)
                    .map(|l| Line::raw(format!(" {l}")))
                    .collect();
                lines.push(Line::styled(
                    " Select the text with the mouse to copy it.   Esc close",
                    pal.dim(),
                ));
                self.draw_cmd_box_auto(frame, area, 96, title, lines);
            }
        }
    }

    /// A box sized to its lines (capped by `area`).
    fn draw_cmd_box_auto(
        &self,
        frame: &mut Frame,
        area: Rect,
        width: u16,
        title: &str,
        lines: Vec<Line<'static>>,
    ) {
        let w = width.min(area.width.saturating_sub(2));
        let h = (lines.len() as u16 + 2).min(area.height.saturating_sub(2));
        self.draw_cmd_box(frame, centered_rect(w, h, area), title, lines);
    }

    fn draw_cmd_box(&self, frame: &mut Frame, r: Rect, title: &str, lines: Vec<Line<'static>>) {
        frame.render_widget(Clear, r);
        frame.render_widget(
            Paragraph::new(Text::from(lines))
                .wrap(Wrap { trim: false })
                .block(self.pane_block(title, true)),
            r,
        );
    }
}

/// One listing row: mark, staged flag, wrapper disclosure, name, size and type.
fn row_text(row: &DisplayRow, marked: bool, name_w: usize) -> String {
    let mark = if marked { '*' } else { ' ' };
    let flag = match row.kind {
        RowKind::PendingAdd => '+',
        RowKind::PendingDelete => '-',
        RowKind::PendingRename => '~',
        _ if row.meta_changed => 'm',
        _ => ' ',
    };
    let indent = "  ".repeat(row.depth);
    let disclosure = match (row.expandable, row.expanded) {
        (true, true) => "[-] ",
        (true, false) => "[+] ",
        _ => "",
    };
    let mut name = printable(&row.name);
    if let Some(to) = &row.rename_to {
        name = format!("{name} -> {}", printable(to));
    }
    if row.is_dir && !row.is_parent() {
        name.push('/');
    }
    let name = clip(&format!("{indent}{disclosure}{name}"), name_w);
    let size = if row.is_dir {
        String::new()
    } else {
        format_size(row.size)
    };
    format!("{mark}{flag}{name} {size:>10} {}", row.type_tag)
}

/// File Info as text: the entry's metadata and extended attributes.
fn detail_lines(d: &FileDetail) -> Vec<String> {
    let e = &d.entry;
    let mut out = vec![
        format!("Name:  {}", printable(&e.name)),
        format!("Path:  {}", printable(&e.path)),
        format!("Size:  {} ({} bytes)", format_size(e.size), e.size),
    ];
    if !d.fs_type.is_empty() {
        out.push(format!("FS:    {}", d.fs_type));
    }
    if let Some(m) = e.mode {
        out.push(format!("Mode:  {:04o}", m & 0o7777));
    }
    if let Some(owner) = d.owner() {
        out.push(format!("Owner: {owner}"));
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
    if let Some(rs) = e.resource_fork_size.filter(|n| *n > 0) {
        out.push(format!("Rsrc:  {}", format_size(rs)));
    }
    if !d.xattrs.is_empty() {
        out.push(String::new());
        out.push("Extended attributes:".to_string());
        for x in &d.xattrs {
            out.push(format!("  {} = {}", x.name, x.value_display()));
        }
    }
    if let Some(p) = &d.preview {
        out.push(String::new());
        out.push(format!("Preview: first {} byte(s) read.", p.len()));
    }
    let budget = d.size_budget_lines();
    if !budget.is_empty() {
        out.push(String::new());
        out.push("SquashFS size budget:".to_string());
        out.extend(budget.into_iter().map(|l| format!("  {l}")));
    }
    if d.loading {
        out.push(String::new());
        out.push("Reading...".to_string());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::super::{App, TabId, DEFAULT_TAB, TABS};
    use super::*;
    use std::path::Path;
    use std::time::{Duration, Instant};

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn ctrl(c: char) -> KeyEvent {
        KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL)
    }

    /// The TUI on Inspect, switched to the Commander tab by its number key.
    fn commander_app() -> App {
        let mut app = App::new_on(DEFAULT_TAB);
        let tab = TABS
            .iter()
            .position(|(t, _)| *t == TabId::Commander)
            .unwrap();
        app.on_key(key(KeyCode::Char(char::from(b'1' + tab as u8))));
        assert!(app.current() == TabId::Commander && app.commander.is_some());
        app
    }

    fn view(app: &App) -> &CommanderView {
        app.commander.as_ref().expect("Commander is open")
    }

    /// Ticks the event loop until no Commander job runs.
    fn settle(app: &mut App) {
        let start = Instant::now();
        app.tick();
        while app.commander.as_ref().is_some_and(|c| c.is_working()) {
            assert!(
                start.elapsed() < Duration::from_secs(60),
                "a job never finished"
            );
            std::thread::sleep(Duration::from_millis(5));
            app.tick();
        }
        app.tick();
    }

    fn typing(app: &mut App, text: &str) {
        for c in text.chars() {
            app.on_key(key(KeyCode::Char(c)));
        }
    }

    /// Answer the open picker with `path`, typed into its path field.
    fn pick(app: &mut App, path: &Path) {
        assert!(
            matches!(view(app).modal, Some(Modal::Pick { .. })),
            "no picker"
        );
        typing(app, &path.display().to_string());
        app.on_key(key(KeyCode::Enter));
        settle(app);
        assert!(view(app).modal.is_none(), "the picker refused {path:?}");
    }

    fn open_folder(app: &mut App, dir: &Path) {
        app.on_key(ctrl('d'));
        pick(app, dir);
        let side = view(app).ctl.active();
        assert!(
            view(app).ctl.pane(side).listing.is_host(),
            "{}",
            status(app)
        );
    }

    fn open_image(app: &mut App, img: &Path) {
        app.on_key(ctrl('o'));
        pick(app, img);
        let side = view(app).ctl.active();
        assert!(
            view(app).ctl.pane(side).listing.is_loaded(),
            "{}",
            status(app)
        );
    }

    fn status(app: &App) -> String {
        view(app).ctl.status().to_string()
    }

    fn row<'a>(app: &'a App, side: Side, name: &str) -> Option<&'a DisplayRow> {
        let rows = view(app).ctl.pane(side).rows();
        rows.iter().find(|r| r.name == name)
    }

    fn staged(app: &App, side: Side) -> usize {
        view(app).ctl.pane(side).queue.len()
    }

    /// Put the active pane's cursor on `name`.
    fn cursor_to(app: &mut App, name: &str) {
        app.on_key(key(KeyCode::Home));
        for _ in 0..64 {
            let pane = view(app).ctl.pane(view(app).ctl.active());
            let at = pane.cursor_index().map(|i| pane.rows()[i].name.clone());
            if at.as_deref() == Some(name) {
                return;
            }
            app.on_key(key(KeyCode::Down));
        }
        panic!("no row {name}");
    }

    /// Left: a host folder holding `files`; right: a blank FAT image. The left pane is active.
    fn host_and_image(files: &[&str]) -> (tempfile::TempDir, tempfile::TempDir, App) {
        let host = tempfile::tempdir().unwrap();
        for f in files {
            std::fs::write(host.path().join(f), f.as_bytes()).unwrap();
        }
        let img = tempfile::tempdir().unwrap();
        let path = crate::controller::commander::blank_fat_image(img.path());
        let mut app = commander_app();
        open_folder(&mut app, host.path());
        app.on_key(key(KeyCode::Tab));
        open_image(&mut app, &path);
        app.on_key(key(KeyCode::Tab));
        assert_eq!(view(&app).ctl.active(), Side::Left);
        (host, img, app)
    }

    /// Copy `names` from the host pane, then apply them on the image pane with F10.
    fn copy_and_apply(app: &mut App, names: &[&str]) {
        for name in names {
            cursor_to(app, name);
            app.on_key(key(KeyCode::F(6)));
        }
        assert_eq!(staged(app, Side::Right), names.len(), "{}", status(app));
        app.on_key(key(KeyCode::Tab));
        app.on_key(key(KeyCode::F(10)));
        settle(app);
        assert_eq!(staged(app, Side::Right), 0, "{}", status(app));
    }

    #[test]
    fn tab_switches_panes_and_each_opens_a_host_folder() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.txt"), b"a").unwrap();
        let mut app = commander_app();
        open_folder(&mut app, dir.path());
        assert_eq!(view(&app).ctl.active(), Side::Left);
        app.on_key(key(KeyCode::Tab));
        assert_eq!(view(&app).ctl.active(), Side::Right);
        open_folder(&mut app, dir.path());
        for side in [Side::Left, Side::Right] {
            assert!(row(&app, side, "a.txt").is_some(), "{side:?}");
        }
        // An unbound key goes back to the tab shell.
        assert!(!app
            .commander
            .as_mut()
            .unwrap()
            .handle_key(key(KeyCode::Char('z'))));
    }

    #[test]
    fn a_copy_into_an_image_stages_and_f10_applies_it() {
        let (_h, _i, mut app) = host_and_image(&["NEW.TXT"]);
        cursor_to(&mut app, "NEW.TXT");
        app.on_key(key(KeyCode::F(6)));
        assert_eq!(staged(&app, Side::Right), 1, "{}", status(&app));
        let r = row(&app, Side::Right, "NEW.TXT").expect("the staged add is listed");
        assert_eq!(r.kind, RowKind::PendingAdd);
        app.on_key(key(KeyCode::Tab));
        app.on_key(key(KeyCode::F(10)));
        settle(&mut app);
        assert_eq!(staged(&app, Side::Right), 0, "{}", status(&app));
        let r = row(&app, Side::Right, "NEW.TXT").expect("the file landed");
        assert_eq!(r.kind, RowKind::Normal);
    }

    #[test]
    fn a_delete_is_staged_then_applied() {
        let (_h, _i, mut app) = host_and_image(&["GONE.TXT"]);
        copy_and_apply(&mut app, &["GONE.TXT"]);
        cursor_to(&mut app, "GONE.TXT");
        app.on_key(key(KeyCode::F(8)));
        assert_eq!(staged(&app, Side::Right), 1, "{}", status(&app));
        let r = row(&app, Side::Right, "GONE.TXT").unwrap();
        assert_eq!(r.kind, RowKind::PendingDelete);
        app.on_key(key(KeyCode::F(10)));
        settle(&mut app);
        assert!(
            row(&app, Side::Right, "GONE.TXT").is_none(),
            "{}",
            status(&app)
        );
    }

    #[test]
    fn rename_refuses_a_taken_name_in_the_prompt() {
        let (_h, _i, mut app) = host_and_image(&["A.TXT", "B.TXT"]);
        copy_and_apply(&mut app, &["A.TXT", "B.TXT"]);
        cursor_to(&mut app, "A.TXT");
        app.on_key(key(KeyCode::F(2)));
        assert!(matches!(view(&app).modal, Some(Modal::Text { .. })));
        for _ in 0.."A.TXT".len() {
            app.on_key(key(KeyCode::Backspace));
        }
        typing(&mut app, "B.TXT");
        let Some(Modal::Text { spec, input, .. }) = &view(&app).modal else {
            panic!("the prompt closed");
        };
        assert!(spec.validate(input).unwrap_err().contains("already exists"));
        app.on_key(key(KeyCode::Enter));
        assert!(view(&app).modal.is_some(), "Enter is refused while invalid");
        app.on_key(key(KeyCode::Backspace));
        app.on_key(key(KeyCode::Backspace));
        app.on_key(key(KeyCode::Backspace));
        typing(&mut app, "DOC");
        app.on_key(key(KeyCode::Enter));
        assert!(view(&app).modal.is_none());
        assert_eq!(staged(&app, Side::Right), 1, "{}", status(&app));
        let r = row(&app, Side::Right, "A.TXT").unwrap();
        assert_eq!(r.kind, RowKind::PendingRename);
        assert_eq!(r.rename_to.as_deref(), Some("B.DOC"));
    }

    #[test]
    fn ctrl_n_stages_a_new_folder() {
        let (_h, _i, mut app) = host_and_image(&[]);
        app.on_key(key(KeyCode::Tab));
        app.on_key(ctrl('n'));
        assert!(matches!(view(&app).modal, Some(Modal::Text { .. })));
        typing(&mut app, "STUFF");
        app.on_key(key(KeyCode::Enter));
        assert_eq!(staged(&app, Side::Right), 1, "{}", status(&app));
        assert_eq!(
            row(&app, Side::Right, "STUFF").map(|r| r.kind),
            Some(RowKind::PendingAdd)
        );
    }

    #[test]
    fn esc_with_staged_edits_asks_before_leaving_commander() {
        let (_h, _i, mut app) = host_and_image(&["KEEP.TXT"]);
        cursor_to(&mut app, "KEEP.TXT");
        app.on_key(key(KeyCode::F(6)));
        app.on_key(key(KeyCode::Esc));
        assert!(matches!(view(&app).modal, Some(Modal::Confirm(_))));
        app.on_key(key(KeyCode::Char('n')));
        assert!(app.commander.is_some() && staged(&app, Side::Right) == 1);
        app.on_key(key(KeyCode::Esc));
        app.on_key(key(KeyCode::Char('y')));
        assert!(app.commander.is_none(), "Close leaves Commander");
        assert!(
            app.current() == TabId::Inspect,
            "back to the tab it came from"
        );
    }

    #[test]
    fn q_with_staged_edits_asks_before_quitting() {
        let (_h, _i, mut app) = host_and_image(&["KEEP.TXT"]);
        cursor_to(&mut app, "KEEP.TXT");
        app.on_key(key(KeyCode::F(6)));
        // From another tab too: the quit comes back to Commander to ask.
        app.on_key(key(KeyCode::Char('1')));
        assert!(app.current() != TabId::Commander);
        app.on_key(key(KeyCode::Char('q')));
        assert!(!app.should_quit, "q must not drop staged edits");
        assert!(app.current() == TabId::Commander);
        assert!(matches!(view(&app).modal, Some(Modal::Confirm(_))));
        app.on_key(key(KeyCode::Char('n')));
        assert!(!app.should_quit && staged(&app, Side::Right) == 1);
        app.on_key(key(KeyCode::Char('q')));
        app.on_key(key(KeyCode::Char('y')));
        assert!(app.should_quit, "yes quits");
    }

    #[test]
    fn q_without_staged_edits_quits_at_once() {
        let mut app = commander_app();
        app.on_key(key(KeyCode::Char('q')));
        assert!(app.should_quit);
    }

    #[test]
    fn the_help_window_lists_the_key_map() {
        let mut app = commander_app();
        app.on_key(key(KeyCode::F(1)));
        assert!(view(&app).ctl.help_shown());
        let help = view(&app).ctl.key_help();
        assert!(help
            .iter()
            .any(|(l, k)| *l == "Open image" && k == "Ctrl+O"));
        app.on_key(key(KeyCode::Esc));
        assert!(!view(&app).ctl.help_shown());
        assert!(
            app.commander.is_some(),
            "Esc closed the window, not Commander"
        );
    }

    /// Draw the whole TUI at 80x24 and return the screen's text.
    fn screen(app: &App) -> String {
        let backend = ratatui::backend::TestBackend::new(80, 24);
        let mut term = ratatui::Terminal::new(backend).unwrap();
        term.draw(|f| app.draw(f)).unwrap();
        let buf = term.backend().buffer().clone();
        let area = buf.area;
        let mut out = String::new();
        for y in 0..area.height {
            for x in 0..area.width {
                out.push_str(buf[(x, y)].symbol());
            }
            out.push('\n');
        }
        out
    }

    #[test]
    fn the_panes_and_windows_draw_in_a_small_terminal() {
        let (_h, _i, mut app) = host_and_image(&["SHOWN.TXT"]);
        cursor_to(&mut app, "SHOWN.TXT");
        app.on_key(key(KeyCode::F(6)));
        let text = screen(&app);
        assert!(text.contains("[1 staged]"), "{text}");
        assert!(text.contains("+SHOWN.TXT"), "{text}");
        app.on_key(key(KeyCode::F(1)));
        assert!(screen(&app).contains("Keyboard shortcuts"));
        app.on_key(key(KeyCode::Esc));
        app.on_key(key(KeyCode::Tab));
        app.on_key(ctrl('u'));
        assert!(screen(&app).contains("Staged edits (R)"));
        app.on_key(key(KeyCode::Esc));
        app.on_key(ctrl('n'));
        assert!(screen(&app).contains("New Folder (R)"), "{}", screen(&app));
    }
}
