//! The Archives screen over `controller::archives`, the GUI's Archives tab.
//!
//! Open a classic Mac archive (`.sit` / `.sea` / `.cpt` / `.mar` / `.hqx`) in a job, list its
//! entries with a tick each, extract all or the ticked ones in one of four fork formats (the
//! controller asks for the folder), and hand a single-entry disk-image payload to the Inspect
//! screen (Mount in Inspect). See docs/phase2_commander_plan.md § "Slice 29".

use std::path::PathBuf;
use std::sync::Arc;

use crossterm::event::KeyCode;
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Paragraph, Wrap};
use ratatui::Frame;

use super::{
    basename, hint, page_step, App, FilePicker, KeyHint, PickKind, PickResult, RecentMode, TabId,
    TABS,
};
use crate::controller::app::{format_size, RecentHandle};
use crate::controller::archives::{
    row_label, ArchiveEntryRow, ArchivesAction, ArchivesController, ForkFormat, HostRequest,
    NoNotify,
};
use crate::controller::{Controller as _, Effect};

/// Fork container formats for archive extraction.
pub(super) const ARCHIVE_FORK_FORMATS: &[(&str, ForkFormat)] = &[
    ("BinHex (.hqx)", ForkFormat::BinHex),
    ("MacBinary (.bin)", ForkFormat::MacBinary),
    ("AppleDouble", ForkFormat::AppleDouble),
    ("Raw (.rsrc)", ForkFormat::Raw),
];

pub(super) const HINTS_ARCHIVES_OPEN: &[KeyHint] = &[
    hint("Enter/o", "Choose an archive file (with the recent list)"),
    hint("c", "Create an archive from host files (the command line)"),
    hint("Left/Right", "Previous / next tab"),
];

pub(super) const HINTS_ARCHIVES: &[KeyHint] = &[
    hint("Up/Down", "Move  (also k/j, PgUp/PgDn, Home/End)"),
    hint("Space", "Tick or untick the entry for Extract Selected"),
    hint("f", "Next fork format"),
    hint("e", "Extract All to a folder"),
    hint("s", "Extract Selected (the ticked entries) to a folder"),
    hint("m", "Mount in Inspect (an archive holding one disk image)"),
    hint("o", "Open another archive"),
    hint("Esc", "Close the archive"),
];

/// What the open picker answers.
enum PickFor {
    Archive,
    /// A folder the controller asked for (its `purpose`).
    Folder(&'static str),
}

/// The Archives screen's widget state around a headless `ArchivesController`.
pub(super) struct ArchiveState {
    pub(super) ctl: ArchivesController,
    /// The app's recent-files store; a successful open is recorded through it.
    pub(super) recent: RecentHandle,
    picker: Option<(PickFor, FilePicker)>,
    pub(super) archive_path: String,
    pub(super) list_sel: usize,
    pub(super) fork_fmt_sel: usize,
    pub(super) status: Option<String>,
    pub(super) is_error: bool,
    /// An open or an extraction was started and its outcome is not shown yet.
    awaiting_open: Option<PathBuf>,
    awaiting_extract: bool,
    /// A Mount payload for the Inspect screen; the guard keeps its temp folder alive.
    pub(super) mount: Option<(PathBuf, tempfile::TempDir)>,
}

impl ArchiveState {
    pub(super) fn new(recent: RecentHandle) -> Self {
        ArchiveState {
            ctl: ArchivesController::new(Arc::new(NoNotify)),
            recent,
            picker: None,
            archive_path: String::new(),
            list_sel: 0,
            fork_fmt_sel: 0,
            status: None,
            is_error: false,
            awaiting_open: None,
            awaiting_extract: false,
            mount: None,
        }
    }

    pub(super) fn act(&mut self, action: ArchivesAction) {
        let fx = self.ctl.apply(action);
        self.absorb(fx);
        self.take_requests();
    }

    /// The controller's pickers become the screen's; it asks nothing else.
    fn absorb(&mut self, fx: Vec<Effect>) {
        for e in fx {
            match e {
                Effect::PickFile(_) => self.open_archive_picker(),
                Effect::PickFolder(spec) => {
                    let p = FilePicker::new(PickKind::Dir, spec.title.as_str());
                    self.picker = Some((PickFor::Folder(spec.purpose), p));
                }
                Effect::Message(msg) => self.status = Some(msg),
                Effect::Confirm(_)
                | Effect::AskPassword(_)
                | Effect::AskText(_)
                | Effect::Choose(_)
                | Effect::OpenUrl(_)
                | Effect::CopyToClipboard(_)
                | Effect::CloseScreen
                | Effect::Quit => {}
            }
        }
    }

    fn take_requests(&mut self) {
        for req in self.ctl.take_requests() {
            match req {
                // Recorded below once the open succeeded, as this screen always did.
                HostRequest::RememberRecent(_) => {}
                HostRequest::OpenInInspect { path, guard } => self.mount = Some((path, guard)),
            }
        }
    }

    fn open_archive_picker(&mut self) {
        let recent = self.recent.list(RecentMode::Archives);
        let p = FilePicker::new(PickKind::File, "Open a Mac archive").with_recent(recent);
        self.picker = Some((PickFor::Archive, p));
    }

    /// Drain the controller and turn a finished open or extraction into the status line.
    pub(super) fn poll(&mut self) {
        let fx = self.ctl.tick();
        self.absorb(fx);
        let _ = self.ctl.take_log();
        self.take_requests();
        if self.awaiting_open.is_some() && !self.ctl.is_loading() {
            let path = self.awaiting_open.take().unwrap_or_default();
            if let Some(e) = self.ctl.load_error() {
                self.status = Some(format!("Cannot open archive: {}", e.detail));
                self.is_error = true;
            } else if self.ctl.is_loaded() {
                self.recent
                    .remember(RecentMode::Archives, path.to_string_lossy().as_ref());
                self.archive_path = path.display().to_string();
                self.list_sel = 0;
                let n = self.ctl.rows().len();
                self.status = Some(match self.ctl.mountable() {
                    Some(kind) => format!(
                        "Opened ({n} entries): a {} disk image; m mounts it in Inspect.",
                        kind.label()
                    ),
                    None => format!("Opened ({n} entries)."),
                });
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

    /// Open a Mac archive in the controller; the list is drawn when the job ends.
    pub(super) fn load(&mut self, path: PathBuf) {
        self.act(ArchivesAction::Open(path.clone()));
        self.awaiting_open = Some(path);
        self.status = Some("Opening the archive...".to_string());
        self.is_error = false;
    }

    /// Ask the controller to extract (all, or the ticked entries) in the chosen fork format.
    fn extract(&mut self, selected: bool) {
        if !self.ctl.is_loaded() || self.ctl.is_extracting() {
            return;
        }
        if selected && self.ctl.selected_file_count() == 0 {
            self.status = Some("No entries are ticked.".to_string());
            self.is_error = true;
            return;
        }
        let format = ARCHIVE_FORK_FORMATS[self.fork_fmt_sel.min(ARCHIVE_FORK_FORMATS.len() - 1)].1;
        self.act(ArchivesAction::SetFormat(format));
        self.act(if selected {
            ArchivesAction::ExtractSelected
        } else {
            ArchivesAction::ExtractAll
        });
    }

    /// Extract every entry to `dest` at once, without the folder prompt.
    #[cfg(test)]
    pub(super) fn extract_all_to(&mut self, dest: PathBuf) {
        let format = ARCHIVE_FORK_FORMATS[self.fork_fmt_sel.min(ARCHIVE_FORK_FORMATS.len() - 1)].1;
        self.act(ArchivesAction::SetFormat(format));
        self.act(ArchivesAction::ExtractAllTo(dest));
        self.started_extract();
    }

    fn started_extract(&mut self) {
        if self.ctl.is_extracting() {
            self.awaiting_extract = true;
            self.status = Some("Extracting...".to_string());
            self.is_error = false;
        }
    }

    /// A picker answer: the archive to open, or the folder the controller asked for.
    fn picked(&mut self, what: PickFor, path: Option<PathBuf>) {
        match (what, path) {
            (PickFor::Archive, Some(p)) => self.load(p),
            (PickFor::Archive, None) => {}
            (PickFor::Folder(purpose), path) => {
                let some = path.is_some();
                self.act(ArchivesAction::Picked { purpose, path });
                if some {
                    self.started_extract();
                }
            }
        }
    }

    fn toggle_tick(&mut self) {
        let i = self.list_sel;
        if i < self.ctl.rows().len() {
            let on = !self.ctl.is_selected(i);
            self.act(ArchivesAction::SetSelected(i, on));
        }
    }
}

/// One archive entry as the screen lists it, with its tick.
fn archive_row_line(ticked: bool, e: &ArchiveEntryRow) -> String {
    let tick = if ticked { "[x]" } else { "[ ]" };
    if e.is_dir {
        return format!("{tick} {}", row_label(e));
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
        "{tick} {:<40} {:>4} {:>4}  {}  {}",
        row_label(e),
        e.type_code,
        e.creator,
        data,
        rsrc
    )
}

impl App {
    fn archive_state(&mut self) -> &mut ArchiveState {
        if self.archive.is_none() {
            self.archive = Some(ArchiveState::new(self.shell.recent_handle()));
        }
        self.archive.as_mut().unwrap()
    }

    /// The Archives keys for the `?` window.
    pub(super) fn archives_key_context(&self) -> (&'static str, &'static [KeyHint]) {
        if self.archive.as_ref().is_some_and(|a| a.ctl.is_loaded()) {
            ("Archives", HINTS_ARCHIVES)
        } else {
            ("Archives - open", HINTS_ARCHIVES_OPEN)
        }
    }

    /// Drive the controller; a Mount payload switches to the Inspect screen with it.
    pub(super) fn archive_tick(&mut self) {
        let Some(a) = self.archive.as_mut() else {
            return;
        };
        a.poll();
        if let Some((path, guard)) = a.mount.take() {
            if let Some(idx) = TABS.iter().position(|t| t.0 == TabId::Inspect) {
                self.active = idx;
                self.scroll = 0;
                self.on_tab_changed();
                self.inspect_open_payload(path, guard);
            }
        }
    }

    /// Open a Mac archive (from the picker or the Inspect hand-off).
    pub(super) fn load_archive(&mut self, path: PathBuf) {
        self.archive_state().load(path);
    }

    /// Archives screen keys. Returns `true` when consumed.
    pub(super) fn handle_archive_key(&mut self, code: KeyCode) -> bool {
        let a = self.archive_state();
        // The archive / folder picker (modal).
        if let Some((_, picker)) = a.picker.as_mut() {
            match picker.handle_key(code) {
                Some(PickResult::Cancel) => {
                    if let Some((what, _)) = a.picker.take() {
                        a.picked(what, None);
                    }
                }
                Some(PickResult::Confirm(path)) => {
                    if let Some((what, _)) = a.picker.take() {
                        a.picked(what, Some(path));
                    }
                }
                None => {}
            }
            self.archive_tick();
            return true;
        }
        if !a.ctl.is_loaded() {
            return match code {
                KeyCode::Enter | KeyCode::Char('o') => {
                    a.open_archive_picker();
                    true
                }
                // `c` prefills the `:` palette with `archive create` (from host files; the
                // palette runs it through the shared CLI dispatch).
                KeyCode::Char('c') => {
                    self.palette_input =
                        Some("archive create \"<OUTPUT.sit>\" <HOST_FILE> ...".to_string());
                    true
                }
                _ => false,
            };
        }
        let page = page_step() as isize;
        let n = a.ctl.rows().len() as isize;
        let last = (n - 1).max(0) as usize;
        match code {
            KeyCode::Up | KeyCode::Char('k') => a.list_sel = a.list_sel.saturating_sub(1),
            KeyCode::Down | KeyCode::Char('j') => a.list_sel = (a.list_sel + 1).min(last),
            KeyCode::PageUp => a.list_sel = (a.list_sel as isize - page).max(0) as usize,
            KeyCode::PageDown => a.list_sel = ((a.list_sel as isize + page) as usize).min(last),
            KeyCode::Home => a.list_sel = 0,
            KeyCode::End => a.list_sel = last,
            KeyCode::Char(' ') => a.toggle_tick(),
            KeyCode::Char('f') => {
                a.fork_fmt_sel = (a.fork_fmt_sel + 1) % ARCHIVE_FORK_FORMATS.len();
            }
            KeyCode::Char('e') => a.extract(false),
            KeyCode::Char('s') => a.extract(true),
            KeyCode::Char('m') => {
                if a.ctl.mountable().is_some() {
                    a.act(ArchivesAction::Mount);
                    self.archive_tick();
                } else {
                    a.status = Some("Mount needs an archive holding one disk image.".to_string());
                    a.is_error = true;
                }
            }
            KeyCode::Char('o') => a.open_archive_picker(),
            KeyCode::Esc => {
                if !a.ctl.is_extracting() {
                    a.act(ArchivesAction::Close);
                    *a = ArchiveState::new(a.recent.clone());
                }
            }
            _ => return false,
        }
        true
    }

    /// The Archives screen: the open prompt, or the entry list with its ticks.
    pub(super) fn draw_archive(&self, frame: &mut Frame, area: Rect, a: &ArchiveState) {
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
            if let Some((_, picker)) = &a.picker {
                picker.draw(frame, area, self.palette, self.border);
            }
            return;
        }

        let rows = a.ctl.rows();
        let title = format!(
            "Archives  {}  {}  [{} of {} files ticked]  fork: {}",
            basename(a.archive_path.as_str()),
            a.ctl.summary(),
            a.ctl.selected_file_count(),
            a.ctl.file_count(),
            ARCHIVE_FORK_FORMATS[a.fork_fmt_sel].0
        );
        let visible = area.height.saturating_sub(5) as usize;
        let start = a.list_sel.saturating_sub(visible.saturating_sub(1));
        for (i, e) in rows.iter().enumerate().skip(start).take(visible) {
            let style = if i == a.list_sel {
                self.palette
                    .accent()
                    .add_modifier(Modifier::REVERSED | Modifier::BOLD)
            } else {
                Style::default()
            };
            let text = archive_row_line(a.ctl.is_selected(i), e);
            lines.push(Line::styled(format!(" {text}"), style));
        }
        lines.push(Line::raw(""));
        if let Some((done, total, name)) = a.ctl.extract_progress() {
            lines.push(Line::styled(
                format!(" Extracting {done}/{total}: {name}"),
                self.palette.dim(),
            ));
        } else if let Some(s) = &a.status {
            let style = if a.is_error {
                self.palette.warn()
            } else {
                self.palette.accent()
            };
            lines.push(Line::styled(format!(" {s}"), style));
        }
        let mount = if a.ctl.mountable().is_some() {
            "   m Mount in Inspect"
        } else {
            ""
        };
        lines.push(Line::styled(
            format!(" Space tick   e Extract All   s Extract Selected   f fork{mount}   ? keys"),
            self.palette.dim(),
        ));
        frame.render_widget(
            Paragraph::new(Text::from(lines)).block(self.pane_block(&title, true)),
            area,
        );
        if let Some((_, picker)) = &a.picker {
            picker.draw(frame, area, self.palette, self.border);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::BACKUP_TAB;
    use super::*;

    fn typed(app: &mut App, text: &str) {
        for c in text.chars() {
            app.handle_archive_key(KeyCode::Char(c));
        }
    }

    fn settle(app: &mut App) {
        app.archive.as_mut().unwrap().ctl.wait_idle();
        app.tick();
    }

    #[test]
    fn the_archives_screen_opens_and_extracts_through_the_controller() {
        let dir = tempfile::tempdir().expect("tempdir");
        let sit = crate::controller::archives::sample_archive(dir.path(), &[("readme", b"hello")]);
        let mut app = App::headless_on(BACKUP_TAB);
        app.archive = Some(ArchiveState::new(app.shell.recent_handle()));
        app.load_archive(sit.clone());
        settle(&mut app);
        let a = app.archive.as_ref().unwrap();
        assert_eq!(a.status.as_deref(), Some("Opened (1 entries)."));
        let recent = app.shell.recent(RecentMode::Archives);
        assert_eq!(recent, [sit.to_string_lossy().into_owned()]);
        let out = dir.path().join("out");
        let a = app.archive.as_mut().unwrap();
        a.fork_fmt_sel = 3;
        a.extract_all_to(out.clone());
        settle(&mut app);
        let a = app.archive.as_ref().unwrap();
        assert_eq!(
            a.status.as_deref(),
            Some(format!("Extracted 1 file(s) to {}", out.display()).as_str())
        );
        assert_eq!(std::fs::read(out.join("readme")).unwrap(), b"hello");
    }

    /// Space unticks an entry; `s` asks for a folder and extracts only the ticked ones.
    #[test]
    fn ticked_entries_extract_through_the_controllers_folder_prompt() {
        let dir = tempfile::tempdir().expect("tempdir");
        let files: &[(&str, &[u8])] = &[("one", b"1"), ("two", b"2")];
        let sit = crate::controller::archives::sample_archive(dir.path(), files);
        let mut app = App::headless_on(BACKUP_TAB);
        app.archive = Some(ArchiveState::new(app.shell.recent_handle()));
        app.load_archive(sit);
        settle(&mut app);
        assert!(app.handle_archive_key(KeyCode::Char(' ')));
        assert_eq!(app.archive.as_ref().unwrap().ctl.selected_file_count(), 1);
        for _ in 0..3 {
            app.handle_archive_key(KeyCode::Char('f'));
        }
        assert!(app.handle_archive_key(KeyCode::Char('s')));
        assert!(
            app.archive.as_ref().unwrap().picker.is_some(),
            "a folder is asked for"
        );
        let out = dir.path().join("sel");
        std::fs::create_dir(&out).unwrap();
        typed(&mut app, out.display().to_string().as_str());
        app.handle_archive_key(KeyCode::Enter);
        settle(&mut app);
        assert!(!out.join("one").exists());
        assert_eq!(std::fs::read(out.join("two")).unwrap(), b"2");
        let status = app.archive.as_ref().unwrap().status.clone().unwrap();
        assert!(status.starts_with("Extracted 1 file(s)"), "{status}");
        // Not a disk image: Mount says why.
        app.handle_archive_key(KeyCode::Char('m'));
        let a = app.archive.as_ref().unwrap();
        assert!(a.is_error && a.status.as_deref().unwrap().starts_with("Mount needs"));
    }

    /// A one-entry archive holding a raw HFS disk mounts on the Inspect screen.
    #[test]
    fn mount_in_inspect_opens_the_payload_on_the_inspect_screen() {
        let dir = tempfile::tempdir().expect("tempdir");
        let sit = crate::controller::test_fixtures::sit_holding_hfs_disk(dir.path());
        let archives = TABS.iter().position(|t| t.0 == TabId::Archives).unwrap();
        let mut app = App::headless_on(archives);
        app.load_archive(sit);
        settle(&mut app);
        let status = app.archive.as_ref().unwrap().status.clone().unwrap();
        assert!(status.contains("raw HFS"), "{status}");
        assert!(app.handle_archive_key(KeyCode::Char('m')));
        assert!(app.current() == TabId::Inspect);
        let start = std::time::Instant::now();
        while app.inspect.as_ref().unwrap().busy() {
            assert!(start.elapsed().as_secs() < 60, "the open never ended");
            std::thread::sleep(std::time::Duration::from_millis(5));
            app.tick();
        }
        app.tick();
        let s = app.inspect.as_ref().unwrap();
        assert!(s.source_open());
        let path = s.ctl.image_path().unwrap().to_path_buf();
        assert!(path.exists(), "the temp folder lives while Inspect has it");
        assert_eq!(path.file_name().unwrap(), "Disk.image");
    }
}
