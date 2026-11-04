//! The Settings screen over `controller::settings`, the GUI's Settings dialog.
//!
//! Rows: the start-up update check, file associations (saved on every OS, registered on Windows),
//! the repository URL (Enter edits it, Enter again saves) and Clear Recent Files. A toggle or an
//! edited URL is saved at once; the controller reads and writes config.json through the app's
//! store, so Clear Recent Files empties every screen's list. `c` checks for updates now in a
//! `tui-update` build. See docs/phase2_commander_plan.md § "Slice 29".

use crossterm::event::KeyCode;
use ratatui::layout::Rect;
use ratatui::style::Modifier;
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Paragraph, Wrap};
use ratatui::Frame;

use super::{hint, App, KeyHint, ASCII_BORDER};
use crate::controller::app::RecentHandle;
use crate::controller::settings::{SettingsAction, SettingsController};
use crate::controller::Controller as _;

pub(super) const HINTS_SETTINGS: &[KeyHint] = &[
    hint("Up/Down", "Move between rows  (also k/j)"),
    hint(
        "Enter/Space",
        "Toggle, edit the repository URL, or clear the recent files",
    ),
    hint("c", "Check for updates now (tui-update builds)"),
    hint("Left/Right", "Previous / next tab"),
];

pub(super) const HINTS_SETTINGS_URL: &[KeyHint] = &[
    hint("Type", "Edit the repository URL"),
    hint("Enter", "Save it"),
    hint("Esc", "Leave it as it was"),
];

/// One row of the form.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum SRow {
    UpdateCheck,
    FileAssociations,
    RepoUrl,
    ClearRecent,
}

const ROWS: [SRow; 4] = [
    SRow::UpdateCheck,
    SRow::FileAssociations,
    SRow::RepoUrl,
    SRow::ClearRecent,
];

/// The Settings tab: the shared Settings form and its cursor.
pub(super) struct SettingsState {
    pub(super) ctl: SettingsController,
    pub(super) sel: usize,
    /// The repository URL being typed, until Enter saves or Esc drops it.
    pub(super) editing: Option<String>,
    /// The last save, clear or update-check result.
    pub(super) status: Option<String>,
}

impl SettingsState {
    /// The form loaded through the app's config store (memory in tests).
    pub(super) fn open(store: RecentHandle) -> Self {
        let mut ctl = SettingsController::with_store(Box::new(store));
        ctl.apply(SettingsAction::Open);
        SettingsState {
            ctl,
            sel: 0,
            editing: None,
            status: None,
        }
    }

    pub(super) fn row(&self) -> SRow {
        ROWS[self.sel.min(ROWS.len() - 1)]
    }

    /// Send `action`, then save; the controller's result becomes the status line.
    fn save_with(&mut self, action: SettingsAction) {
        self.ctl.apply(action);
        self.ctl.apply(SettingsAction::Save);
        self.status = self.ctl.status().map(str::to_string);
    }

    /// Enter or Space on the cursor row.
    fn activate(&mut self) {
        match self.row() {
            SRow::UpdateCheck => {
                let on = !self.ctl.update_check();
                self.save_with(SettingsAction::SetUpdateCheck(on));
            }
            SRow::FileAssociations => {
                let on = !self.ctl.file_associations();
                self.save_with(SettingsAction::SetFileAssociations(on));
            }
            SRow::RepoUrl => self.editing = Some(self.ctl.repo_url().to_string()),
            SRow::ClearRecent => {
                if self.ctl.can_clear_recent() {
                    self.ctl.apply(SettingsAction::ClearRecent);
                    self.status = self.ctl.status().map(str::to_string);
                } else {
                    self.status = Some("There are no recent files to clear.".to_string());
                }
            }
        }
    }

    /// A key while the URL is being typed; every key is taken.
    fn url_key(&mut self, code: KeyCode) {
        let Some(text) = self.editing.as_mut() else {
            return;
        };
        match code {
            KeyCode::Enter => {
                let url = self.editing.take().unwrap_or_default();
                self.save_with(SettingsAction::SetRepoUrl(url));
            }
            KeyCode::Esc => self.editing = None,
            KeyCode::Backspace => {
                text.pop();
            }
            KeyCode::Char(c) if !c.is_control() => text.push(c),
            _ => {}
        }
    }
}

impl App {
    fn settings_state(&mut self) -> &mut SettingsState {
        if self.settings.is_none() {
            self.settings = Some(SettingsState::open(self.shell.recent_handle()));
        }
        self.settings.as_mut().unwrap()
    }

    /// The Settings keys for the `?` window.
    pub(super) fn settings_key_context(&self) -> (&'static str, &'static [KeyHint]) {
        if self.settings.as_ref().is_some_and(|s| s.editing.is_some()) {
            ("Settings - repository URL", HINTS_SETTINGS_URL)
        } else {
            ("Settings", HINTS_SETTINGS)
        }
    }

    /// Settings tab keys. Returns `true` when consumed (Left/Right fall through to switch tabs).
    pub(super) fn handle_settings_key(&mut self, code: KeyCode) -> bool {
        let s = self.settings_state();
        if s.editing.is_some() {
            s.url_key(code);
            return true;
        }
        match code {
            KeyCode::Up | KeyCode::Char('k') => {
                s.sel = s.sel.saturating_sub(1);
                true
            }
            KeyCode::Down | KeyCode::Char('j') => {
                s.sel = (s.sel + 1).min(ROWS.len() - 1);
                true
            }
            KeyCode::Enter | KeyCode::Char(' ') => {
                s.activate();
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

    /// Run an update check against GitHub and report the result on the Settings status line.
    /// Only functional in a `tui-update` build (which links the reqwest client).
    fn settings_check_now(&mut self) {
        #[cfg(feature = "tui-update")]
        {
            let repo = self.settings.as_ref().map(|s| s.ctl.repo_url().to_string());
            let Some(repo) = repo else { return };
            // A network round-trip: run it as a task so the screen stays live.
            self.start_task("Checking for updates", move |job| {
                job.set_can_cancel(false);
                let msg = crate::controller::app::check_updates_now_text(repo.as_str());
                Ok(Box::new(move |app: &mut App| {
                    if let Some(s) = app.settings.as_mut() {
                        s.status = Some(msg);
                    }
                }) as super::TaskDone)
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

    /// The Settings form: environment facts (read-only), then the editable rows.
    pub(super) fn draw_settings_tab(&self, frame: &mut Frame, area: Rect, s: &SettingsState) {
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
        let (privilege, privilege_style) = if self.elevated {
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
        let mut lines = vec![
            info(
                "Platform:",
                format!("{} / {}", std::env::consts::OS, std::env::consts::ARCH),
            ),
            Line::from(vec![
                Span::styled(format!("{:<14}", "Privilege:"), self.palette.accent()),
                Span::styled(privilege, privilege_style),
            ]),
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
                SettingsController::config_file()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| "(no config dir)".to_string()),
            ),
            Line::raw(""),
            Line::styled("Preferences:", self.palette.accent()),
            Line::raw(""),
        ];
        for (i, row) in ROWS.iter().enumerate() {
            let selected = i == s.sel;
            let marker = if selected { "> " } else { "  " };
            let style = if selected {
                self.palette
                    .accent()
                    .add_modifier(Modifier::REVERSED | Modifier::BOLD)
            } else {
                self.palette.accent()
            };
            let boxed = |on: bool| if on { "[x]" } else { "[ ]" };
            let line = match row {
                SRow::UpdateCheck => Line::styled(
                    format!(
                        "{marker}{} Check for updates at startup",
                        boxed(s.ctl.update_check())
                    ),
                    style,
                ),
                SRow::FileAssociations => Line::styled(
                    format!(
                        "{marker}{} Associate disk image files (Windows)",
                        boxed(s.ctl.file_associations())
                    ),
                    style,
                ),
                SRow::RepoUrl => {
                    let mut spans = vec![Span::styled(
                        format!("{marker}GitHub repository URL: "),
                        style,
                    )];
                    match &s.editing {
                        Some(text) => {
                            spans.push(Span::raw(text.clone()));
                            spans.push(Span::styled(
                                " ",
                                self.palette.accent().add_modifier(Modifier::REVERSED),
                            ));
                        }
                        None => spans.push(Span::raw(s.ctl.repo_url().to_string())),
                    }
                    Line::from(spans)
                }
                SRow::ClearRecent => Line::styled(
                    format!("{marker}[ {} ]", s.ctl.clear_recent_label()),
                    if s.ctl.can_clear_recent() {
                        style
                    } else {
                        self.palette.dim()
                    },
                ),
            };
            lines.push(line);
        }
        lines.push(Line::raw(""));
        if let Some(st) = &s.status {
            let style = if st.starts_with("Error") {
                self.palette.warn()
            } else {
                self.palette.dim()
            };
            lines.push(Line::styled(format!("  {st}"), style));
            lines.push(Line::raw(""));
        }
        let keys = if s.editing.is_some() {
            "Type the URL   Enter save   Esc cancel"
        } else {
            "Up/Down select   Enter/Space toggle or edit   c check for updates   ? keys"
        };
        lines.push(Line::styled(keys, self.palette.dim()));
        frame.render_widget(
            Paragraph::new(Text::from(lines))
                .block(self.pane_block("Settings", true))
                .wrap(Wrap { trim: false }),
            area,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::super::{TabId, TABS};
    use super::*;
    use crate::controller::app::RecentMode;

    fn tab_index_of(id: TabId) -> usize {
        TABS.iter().position(|(t, _)| *t == id).unwrap()
    }

    /// Each toggle saves through the Settings controller (memory config in tests).
    #[test]
    fn a_settings_toggle_saves_through_the_settings_controller() {
        let mut app = App::headless_on(tab_index_of(TabId::Settings));
        let before = app.settings.as_ref().unwrap().ctl.update_check();
        assert!(app.handle_settings_key(KeyCode::Enter));
        let s = app.settings.as_ref().unwrap();
        assert_eq!(s.ctl.update_check(), !before);
        let status = s.status.clone().unwrap_or_default();
        assert!(status.starts_with("Settings saved"), "{status}");
        assert!(app.handle_settings_key(KeyCode::Down));
        assert!(app.handle_settings_key(KeyCode::Char(' ')));
        assert!(app.settings.as_ref().unwrap().ctl.file_associations());
    }

    /// The URL is typed in place and saved; Clear Recent Files empties the app's lists.
    #[test]
    fn the_repository_url_is_edited_and_recent_files_are_cleared_through_the_app() {
        let mut app = App::headless_on(tab_index_of(TabId::Settings));
        app.shell.remember(RecentMode::Inspect, "/a.img");
        app.shell.remember(RecentMode::Archives, "/b.sit");
        // The form opened before those were recorded; a fresh visit reads them.
        app.settings = None;
        for _ in 0..2 {
            app.handle_settings_key(KeyCode::Down);
        }
        assert_eq!(app.settings.as_ref().unwrap().row(), SRow::RepoUrl);
        app.handle_settings_key(KeyCode::Enter);
        assert!(app.settings.as_ref().unwrap().editing.is_some());
        for _ in 0..200 {
            app.handle_settings_key(KeyCode::Backspace);
        }
        for c in "https://github.com/o/r?x".chars() {
            assert!(app.handle_settings_key(KeyCode::Char(c)), "{c} typed");
        }
        assert!(!app.show_help, "? is typed into the URL, not taken");
        app.handle_settings_key(KeyCode::Enter);
        let s = app.settings.as_ref().unwrap();
        assert!(s.editing.is_none());
        assert_eq!(s.ctl.repo_url(), "https://github.com/o/r?x");
        let saved = crate::controller::app::ConfigStore::load(&app.shell.recent_handle());
        assert_eq!(
            saved.update_check.repository_url,
            "https://github.com/o/r?x"
        );
        // Esc leaves an edit as it was.
        app.handle_settings_key(KeyCode::Enter);
        app.handle_settings_key(KeyCode::Char('z'));
        app.handle_settings_key(KeyCode::Esc);
        assert_eq!(
            app.settings.as_ref().unwrap().ctl.repo_url(),
            "https://github.com/o/r?x"
        );
        app.handle_settings_key(KeyCode::Down);
        assert_eq!(
            app.settings.as_ref().unwrap().ctl.clear_recent_label(),
            "Clear Recent Files (2)"
        );
        app.handle_settings_key(KeyCode::Enter);
        let status = app.settings.as_ref().unwrap().status.clone();
        assert_eq!(status.as_deref(), Some("Cleared 2 recent files."));
        assert!(app.shell.recent(RecentMode::Inspect).is_empty());
        assert!(app.shell.recent(RecentMode::Archives).is_empty());
    }

    /// Without `tui-update` the check stays out of the build and says so.
    #[cfg(not(feature = "tui-update"))]
    #[test]
    fn check_now_without_tui_update_says_it_is_not_built() {
        let mut app = App::headless_on(tab_index_of(TabId::Settings));
        assert!(app.handle_settings_key(KeyCode::Char('c')));
        assert!(!app.task.is_active());
        let status = app.settings.as_ref().unwrap().status.clone();
        assert!(status.unwrap_or_default().contains("tui-update"));
    }
}
