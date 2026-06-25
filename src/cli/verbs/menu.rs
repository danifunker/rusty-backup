//! `rb-cli menu` — an interactive backup/restore TUI.
//!
//! Built for the Linux appliance (boots straight into it — see
//! `docs/linux_486_appliance.md`) but useful anywhere: it lists the machine's
//! disks, you pick one, then choose Inspect / Backup / Restore. Each action
//! just builds the args for the existing `inspect` / `backup` / `restore` verbs
//! and calls them — no logic is duplicated here.
//!
//! Follows the `bless pick` conventions: a terminal-free state core
//! ([`MenuState`], unit-tested below) under a thin crossterm shell, ASCII-only,
//! TTY-guarded, with an RAII guard that restores the terminal.

use std::io::{self, BufRead, Write};
use std::path::PathBuf;

use anyhow::{bail, Result};
use clap::Args;
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::style::{Attribute, Print, SetAttribute};
use crossterm::{cursor, queue, terminal};

use super::backup::{self, BackupArgs, BackupFormat};
use super::inspect::{self, InspectArgs};
use super::restore::{self, RestoreArgs};
use crate::cli::output::OutputFormat;
use crate::cli::tui::{
    ascii_rule, require_interactive_tty, truncate, TermGuard, FALLBACK_COLS, FALLBACK_ROWS,
};
use crate::device::{enumerate_devices, DiskDevice};

#[derive(Debug, Args)]
pub struct MenuArgs {}

/// The operation chosen for a disk.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Action {
    Inspect,
    Backup,
    Restore,
}

/// The action list shown for a selected disk (the 4th is "back").
const ACTIONS: [&str; 4] = [
    "Inspect  - show this disk's partition layout",
    "Backup   - image this disk to a folder",
    "Restore  - write a saved backup onto this disk",
    "Back     - pick a different disk",
];

// ---------------------------------------------------------------------------
// Pure state core (no terminal I/O — unit-tested at the bottom).
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Screen {
    Disks,
    Actions,
}

/// What [`MenuState::on_enter`] decided.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum EnterResult {
    /// Stay in the TUI (a screen transition happened).
    Stay,
    /// The user committed to running `Action` on disk index `usize`.
    Run(usize, Action),
}

const MIN_VIEWPORT: usize = 3;

/// Terminal-independent menu state: which screen, the selection cursors, and
/// the disk-list scroll window.
pub struct MenuState {
    disk_labels: Vec<String>,
    screen: Screen,
    disk_sel: usize,
    action_sel: usize,
    scroll: usize,
    viewport: usize,
}

impl MenuState {
    pub fn new(disk_labels: Vec<String>) -> Self {
        MenuState {
            disk_labels,
            screen: Screen::Disks,
            disk_sel: 0,
            action_sel: 0,
            scroll: 0,
            viewport: MIN_VIEWPORT,
        }
    }

    pub fn screen(&self) -> Screen {
        self.screen
    }
    pub fn disk_sel(&self) -> usize {
        self.disk_sel
    }
    pub fn action_sel(&self) -> usize {
        self.action_sel
    }
    pub fn disk_count(&self) -> usize {
        self.disk_labels.len()
    }

    pub fn set_viewport(&mut self, rows: usize) {
        self.viewport = rows.max(MIN_VIEWPORT);
        self.clamp_scroll();
    }

    /// Inclusive-exclusive range of disk indices currently visible.
    pub fn visible_range(&self) -> (usize, usize) {
        let end = (self.scroll + self.viewport).min(self.disk_labels.len());
        (self.scroll, end)
    }

    pub fn move_up(&mut self) {
        match self.screen {
            Screen::Disks => {
                if self.disk_sel > 0 {
                    self.disk_sel -= 1;
                    self.clamp_scroll();
                }
            }
            Screen::Actions => {
                if self.action_sel > 0 {
                    self.action_sel -= 1;
                }
            }
        }
    }

    pub fn move_down(&mut self) {
        match self.screen {
            Screen::Disks => {
                if self.disk_sel + 1 < self.disk_labels.len() {
                    self.disk_sel += 1;
                    self.clamp_scroll();
                }
            }
            Screen::Actions => {
                if self.action_sel + 1 < ACTIONS.len() {
                    self.action_sel += 1;
                }
            }
        }
    }

    /// Enter: Disks -> Actions; on Actions, run the chosen action (or go Back).
    pub fn on_enter(&mut self) -> EnterResult {
        match self.screen {
            Screen::Disks => {
                if self.disk_labels.is_empty() {
                    return EnterResult::Stay;
                }
                self.screen = Screen::Actions;
                self.action_sel = 0;
                EnterResult::Stay
            }
            Screen::Actions => match self.action_sel {
                0 => EnterResult::Run(self.disk_sel, Action::Inspect),
                1 => EnterResult::Run(self.disk_sel, Action::Backup),
                2 => EnterResult::Run(self.disk_sel, Action::Restore),
                _ => {
                    self.screen = Screen::Disks;
                    EnterResult::Stay
                }
            },
        }
    }

    /// Esc/q: on the Actions screen go back to the disk list; on the disk list
    /// it means "quit" (returns `true`).
    pub fn on_back(&mut self) -> bool {
        match self.screen {
            Screen::Disks => true,
            Screen::Actions => {
                self.screen = Screen::Disks;
                false
            }
        }
    }

    fn clamp_scroll(&mut self) {
        if self.disk_sel < self.scroll {
            self.scroll = self.disk_sel;
        } else if self.disk_sel >= self.scroll + self.viewport {
            self.scroll = self.disk_sel + 1 - self.viewport;
        }
        let max_scroll = self.disk_labels.len().saturating_sub(self.viewport);
        if self.scroll > max_scroll {
            self.scroll = max_scroll;
        }
    }
}

// ---------------------------------------------------------------------------
// Entry point + outer loop
// ---------------------------------------------------------------------------

pub fn run(_args: MenuArgs) -> Result<()> {
    require_interactive_tty(
        "rb-cli menu",
        "use the `inspect` / `backup` / `restore` verbs directly for scripts",
    )?;

    loop {
        let devices = enumerate_devices();
        if devices.is_empty() {
            bail!("no disks detected (raw device access usually needs root)");
        }
        let labels: Vec<String> = devices.iter().map(DiskDevice::display_name).collect();

        match pick_action(&labels)? {
            None => {
                println!("Goodbye.");
                return Ok(());
            }
            Some((idx, action)) => run_action(&devices[idx], action)?,
        }
    }
}

/// Run the TUI until the user commits to an action (`Some`) or quits (`None`).
/// The terminal guard is scoped to this function, so the chosen action runs
/// afterwards with a normal terminal (scrolling output, progress, prompts).
fn pick_action(labels: &[String]) -> Result<Option<(usize, Action)>> {
    let _guard = TermGuard::enter()?;
    let mut state = MenuState::new(labels.to_vec());
    state.set_viewport(viewport_from_terminal());

    loop {
        render(&state)?;
        let Event::Key(key) = event::read()? else {
            state.set_viewport(viewport_from_terminal());
            continue;
        };
        if key.kind == KeyEventKind::Release {
            continue;
        }
        let ctrl_c =
            key.modifiers.contains(KeyModifiers::CONTROL) && matches!(key.code, KeyCode::Char('c'));
        match key.code {
            KeyCode::Up | KeyCode::Char('k') => state.move_up(),
            KeyCode::Down | KeyCode::Char('j') => state.move_down(),
            KeyCode::Enter | KeyCode::Right | KeyCode::Char('l') => {
                if let EnterResult::Run(idx, action) = state.on_enter() {
                    return Ok(Some((idx, action)));
                }
            }
            KeyCode::Esc | KeyCode::Char('q') | KeyCode::Left | KeyCode::Char('h')
                if state.on_back() =>
            {
                return Ok(None);
            }
            _ if ctrl_c => return Ok(None),
            _ => {}
        }
        state.set_viewport(viewport_from_terminal());
    }
}

/// Leave the TUI behind (already done by the caller) and run the verb, with a
/// couple of plain-stdin prompts where a path / confirmation is needed.
fn run_action(disk: &DiskDevice, action: Action) -> Result<()> {
    println!();
    match action {
        Action::Inspect => {
            let r = inspect::run(InspectArgs {
                image: disk.path.clone(),
                format: OutputFormat::Text,
                password: None,
                inside: None,
            });
            report(r, "inspect");
        }
        Action::Backup => {
            match pick_dir("Pick a folder to write the backup into", false)? {
                None => println!("Backup cancelled."),
                Some(dest) => {
                    println!(
                        "Backing up {} -> {} (zstd) ...",
                        disk.path.display(),
                        dest.display()
                    );
                    let r = backup::run(BackupArgs {
                        source: disk.path.clone(),
                        dest,
                        name: None,
                        // The slim appliance build has no CHD; zstd is pure-Rust.
                        format: Some(BackupFormat::Zstd),
                        checksum: None,
                        sector_by_sector: false,
                        defrag: false,
                        partitions: None,
                        split_size_mib: None,
                    });
                    report(r, "backup");
                }
            }
        }
        Action::Restore => {
            match pick_dir(
                "Pick the backup folder to restore from (marked [backup])",
                true,
            )? {
                None => println!("Restore cancelled."),
                Some(src) => {
                    println!(
                        "This will OVERWRITE {} with the backup at {}.",
                        disk.path.display(),
                        src.display()
                    );
                    let yes = matches!(
                        prompt("Proceed? [y/N]: ")?.chars().next(),
                        Some('y') | Some('Y')
                    );
                    if yes {
                        let r = restore::run(RestoreArgs {
                            backup_dir: src,
                            target: disk.path.clone(),
                            target_size: None,
                            size: None,
                            alignment: None,
                            device: true,
                            yes: true,
                            write_to_system_disk: false,
                            write_zeros_to_unused: false,
                        });
                        report(r, "restore");
                    } else {
                        println!("Restore cancelled.");
                    }
                }
            }
        }
    }
    prompt("\nPress Enter to return to the menu...")?;
    Ok(())
}

fn report(r: Result<()>, what: &str) {
    match r {
        Ok(()) => println!("\n{what}: done."),
        Err(e) => eprintln!("\n{what} failed: {e:#}"),
    }
}

/// Browse for a folder, starting at `/mnt` (the appliance's usual destination
/// mount) when it exists, else `/`.
fn pick_dir(title: &str, require_backup: bool) -> Result<Option<PathBuf>> {
    let mnt = PathBuf::from("/mnt");
    let start = if mnt.is_dir() {
        mnt
    } else {
        PathBuf::from("/")
    };
    crate::cli::dir_picker::pick_directory(&start, title, require_backup)
}

/// Read one trimmed line from stdin (normal cooked mode — used between TUI
/// sessions). Returns an empty string on EOF.
fn prompt(msg: &str) -> Result<String> {
    print!("{msg}");
    io::stdout().flush()?;
    let mut line = String::new();
    io::stdin().lock().read_line(&mut line)?;
    Ok(line.trim().to_string())
}

// ---------------------------------------------------------------------------
// crossterm rendering
// ---------------------------------------------------------------------------

fn viewport_from_terminal() -> usize {
    let (_, rows) = terminal::size().unwrap_or((FALLBACK_COLS, FALLBACK_ROWS));
    // title + rule + footer rule + help = 5 rows of chrome (+1 subtitle on the
    // actions screen, which only ever has 4 items so it always fits).
    (rows as usize).saturating_sub(6).max(MIN_VIEWPORT)
}

fn render(state: &MenuState) -> Result<()> {
    match state.screen {
        Screen::Disks => render_disks(state),
        Screen::Actions => render_actions(state),
    }
}

fn render_disks(state: &MenuState) -> Result<()> {
    let mut out = io::stdout();
    let (cols, _) = terminal::size().unwrap_or((FALLBACK_COLS, FALLBACK_ROWS));
    let width = cols as usize;

    queue!(
        out,
        terminal::Clear(terminal::ClearType::All),
        cursor::MoveTo(0, 0),
        Print("rusty-backup  -  select a disk to back up or restore"),
        cursor::MoveTo(0, 1),
        Print(ascii_rule(width)),
    )?;

    let (start, end) = state.visible_range();
    for (row, idx) in (start..end).enumerate() {
        let y = (row + 2) as u16;
        let selected = idx == state.disk_sel();
        let mark = if selected { '>' } else { ' ' };
        let body = truncate(
            &format!("{mark} {}", state.disk_labels[idx]),
            width.saturating_sub(1),
        );
        queue!(out, cursor::MoveTo(0, y))?;
        emit_row(&mut out, &body, width, selected)?;
    }

    let help_y = (2 + state.viewport + 1) as u16;
    queue!(
        out,
        cursor::MoveTo(0, help_y.saturating_sub(1)),
        Print(ascii_rule(width)),
        cursor::MoveTo(0, help_y),
        Print("Up/Down move   Enter select   q/Esc quit"),
    )?;
    out.flush()?;
    Ok(())
}

fn render_actions(state: &MenuState) -> Result<()> {
    let mut out = io::stdout();
    let (cols, _) = terminal::size().unwrap_or((FALLBACK_COLS, FALLBACK_ROWS));
    let width = cols as usize;
    let disk = &state.disk_labels[state.disk_sel()];

    queue!(
        out,
        terminal::Clear(terminal::ClearType::All),
        cursor::MoveTo(0, 0),
        Print("rusty-backup  -  choose an action"),
        cursor::MoveTo(0, 1),
        Print(truncate(&format!("Disk: {disk}"), width)),
        cursor::MoveTo(0, 2),
        Print(ascii_rule(width)),
    )?;

    for (i, label) in ACTIONS.iter().enumerate() {
        let y = (i + 3) as u16;
        let selected = i == state.action_sel();
        let mark = if selected { '>' } else { ' ' };
        let body = truncate(&format!("{mark} {label}"), width.saturating_sub(1));
        queue!(out, cursor::MoveTo(0, y))?;
        emit_row(&mut out, &body, width, selected)?;
    }

    let help_y = (3 + ACTIONS.len() + 1) as u16;
    queue!(
        out,
        cursor::MoveTo(0, help_y.saturating_sub(1)),
        Print(ascii_rule(width)),
        cursor::MoveTo(0, help_y),
        Print("Up/Down move   Enter run   q/Esc/Left back"),
    )?;
    out.flush()?;
    Ok(())
}

/// Print one list row, reverse-video when selected (padded to the width so the
/// highlight spans the line).
fn emit_row(out: &mut io::Stdout, body: &str, width: usize, selected: bool) -> Result<()> {
    if selected {
        let w = width.saturating_sub(1);
        queue!(
            out,
            SetAttribute(Attribute::Reverse),
            Print(format!("{body:<w$}")),
            SetAttribute(Attribute::Reset),
        )?;
    } else {
        queue!(out, Print(body))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(n: usize) -> MenuState {
        let labels = (0..n).map(|i| format!("/dev/sd{i}")).collect();
        let mut s = MenuState::new(labels);
        s.set_viewport(4);
        s
    }

    #[test]
    fn disks_nav_clamps() {
        let mut s = state(3);
        s.move_up();
        assert_eq!(s.disk_sel(), 0);
        s.move_down();
        s.move_down();
        s.move_down();
        assert_eq!(s.disk_sel(), 2);
    }

    #[test]
    fn enter_goes_to_actions_then_runs() {
        let mut s = state(2);
        s.move_down(); // disk 1
        assert_eq!(s.on_enter(), EnterResult::Stay);
        assert_eq!(s.screen(), Screen::Actions);
        // action 0 = Inspect
        assert_eq!(s.on_enter(), EnterResult::Run(1, Action::Inspect));
        // action 1 = Backup
        s.move_down();
        assert_eq!(s.on_enter(), EnterResult::Run(1, Action::Backup));
        // action 2 = Restore
        s.move_down();
        assert_eq!(s.on_enter(), EnterResult::Run(1, Action::Restore));
    }

    #[test]
    fn back_action_returns_to_disks() {
        let mut s = state(2);
        s.on_enter(); // -> Actions
        s.move_down();
        s.move_down();
        s.move_down(); // action 3 = Back
        assert_eq!(s.on_enter(), EnterResult::Stay);
        assert_eq!(s.screen(), Screen::Disks);
    }

    #[test]
    fn esc_quits_from_disks_but_returns_from_actions() {
        let mut s = state(2);
        // From actions, back returns to disks (not quit).
        s.on_enter();
        assert!(!s.on_back());
        assert_eq!(s.screen(), Screen::Disks);
        // From disks, back means quit.
        assert!(s.on_back());
    }

    #[test]
    fn scroll_follows_selection() {
        let mut s = state(10); // viewport 4
        for _ in 0..6 {
            s.move_down();
        }
        let (start, end) = s.visible_range();
        assert!(start <= 6 && 6 < end);
        assert_eq!(end - start, 4);
    }

    #[test]
    fn action_cursor_clamps_to_four() {
        let mut s = state(1);
        s.on_enter();
        for _ in 0..9 {
            s.move_down();
        }
        assert_eq!(s.action_sel(), ACTIONS.len() - 1);
    }
}
