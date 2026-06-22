//! A host-filesystem directory picker (arrow-key, ASCII, console-safe).
//!
//! Used by `rb-cli menu` so the appliance never needs typing: pick a backup
//! *destination* folder, or a backup *source* folder to restore from.
//!
//! Each folder's list starts with `[ Use THIS folder ]` (Enter picks the folder
//! you're in), then `..` (up), then its subfolders. So: open folders with Enter
//! until you're in the one you want, then Enter `[ Use THIS folder ]` (or press
//! `s` from anywhere). Folders holding a `metadata.json` are tagged `[backup]`;
//! in restore mode only a backup folder can be used.
//!
//! Same conventions as `bless pick` / `menu`: an RAII terminal guard, ASCII-only
//! rendering, the shared [`ListNav`] cursor. `std::fs` is the only I/O;
//! navigation is plain `PathBuf` arithmetic.

use std::io::{self, Write};
use std::path::{Path, PathBuf};

use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::style::{Attribute, Print, SetAttribute};
use crossterm::{cursor, queue, terminal};

use crate::cli::tui::{
    ascii_rule, truncate, ListNav, TermGuard, FALLBACK_COLS, FALLBACK_ROWS, MIN_VIEWPORT,
};

enum Kind {
    /// "[ Use THIS folder ]" — pick the directory we're currently in.
    UseCurrent,
    /// ".." — go to the parent.
    Up,
    /// A subfolder — Enter descends into it.
    Child,
}

struct Entry {
    label: String,
    path: PathBuf,
    kind: Kind,
}

fn has_metadata(dir: &Path) -> bool {
    dir.join("metadata.json").is_file()
}

/// Build the row list for `dir`: the "use this folder" action, then `..`, then
/// subfolders (sorted, `[backup]`-tagged when they hold a `metadata.json`).
fn entries_for(dir: &Path) -> Vec<Entry> {
    let mut rows = vec![Entry {
        label: "[ Use THIS folder ]".to_string(),
        path: dir.to_path_buf(),
        kind: Kind::UseCurrent,
    }];
    if let Some(parent) = dir.parent() {
        rows.push(Entry {
            label: "..  (up one folder)".to_string(),
            path: parent.to_path_buf(),
            kind: Kind::Up,
        });
    }
    if let Ok(read) = std::fs::read_dir(dir) {
        let mut dirs: Vec<(String, PathBuf)> = read
            .flatten()
            .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
            .map(|e| (e.file_name().to_string_lossy().into_owned(), e.path()))
            .collect();
        dirs.sort_by_key(|(n, _)| n.to_lowercase());
        for (name, path) in dirs {
            let tag = if has_metadata(&path) {
                "   [backup]"
            } else {
                ""
            };
            rows.push(Entry {
                label: format!("{name}/{tag}"),
                path,
                kind: Kind::Child,
            });
        }
    }
    rows
}

/// Run the picker starting at `start`. Returns the chosen directory, or `None`
/// if cancelled. With `require_backup`, only a folder holding `metadata.json`
/// can be chosen (for restore). The caller must already be on an interactive
/// terminal.
pub fn pick_directory(start: &Path, title: &str, require_backup: bool) -> Result<Option<PathBuf>> {
    let _guard = TermGuard::enter()?;

    let mut current = start
        .canonicalize()
        .ok()
        .filter(|p| p.is_dir())
        .unwrap_or_else(|| PathBuf::from("/"));
    let mut entries = entries_for(&current);
    let mut nav = ListNav::new(entries.len());
    nav.set_viewport(viewport());
    let mut message = String::new();

    // Pick `current`, honoring the restore-only backup check.
    macro_rules! try_pick {
        ($dir:expr) => {{
            if require_backup && !has_metadata($dir) {
                message = "Not a backup folder (no metadata.json)".to_string();
            } else {
                return Ok(Some($dir.to_path_buf()));
            }
        }};
    }

    loop {
        render(&current, &entries, &nav, title, &message)?;
        message.clear();

        let Event::Key(key) = event::read()? else {
            nav.set_viewport(viewport());
            continue;
        };
        if key.kind == KeyEventKind::Release {
            continue;
        }
        let ctrl_c =
            key.modifiers.contains(KeyModifiers::CONTROL) && matches!(key.code, KeyCode::Char('c'));

        match key.code {
            KeyCode::Up | KeyCode::Char('k') => nav.move_up(),
            KeyCode::Down | KeyCode::Char('j') => nav.move_down(),
            KeyCode::Enter | KeyCode::Right | KeyCode::Char('l') => {
                match entries.get(nav.selected()) {
                    Some(Entry {
                        kind: Kind::UseCurrent,
                        ..
                    }) => try_pick!(&current),
                    // Up or Child: navigate (both target real directories).
                    Some(e) if e.path.is_dir() => {
                        current = e.path.clone();
                        entries = entries_for(&current);
                        nav.set_len(entries.len());
                        nav.set_viewport(viewport());
                    }
                    _ => {}
                }
            }
            KeyCode::Left | KeyCode::Char('h') | KeyCode::Backspace => {
                if let Some(parent) = current.parent().map(Path::to_path_buf) {
                    current = parent;
                    entries = entries_for(&current);
                    nav.set_len(entries.len());
                    nav.set_viewport(viewport());
                }
            }
            // `s` is a shortcut for "use the folder I'm in", from any row.
            KeyCode::Char('s') => try_pick!(&current),
            KeyCode::Esc | KeyCode::Char('q') => return Ok(None),
            _ if ctrl_c => return Ok(None),
            _ => {}
        }
        nav.set_viewport(viewport());
    }
}

fn viewport() -> usize {
    let (_, rows) = terminal::size().unwrap_or((FALLBACK_COLS, FALLBACK_ROWS));
    // title + path + rule + footer rule + help + message = 6 chrome rows.
    (rows as usize).saturating_sub(6).max(MIN_VIEWPORT)
}

fn render(
    current: &Path,
    entries: &[Entry],
    nav: &ListNav,
    title: &str,
    message: &str,
) -> Result<()> {
    let mut out = io::stdout();
    let (cols, _) = terminal::size().unwrap_or((FALLBACK_COLS, FALLBACK_ROWS));
    let width = cols as usize;

    queue!(
        out,
        terminal::Clear(terminal::ClearType::All),
        cursor::MoveTo(0, 0),
        Print(truncate(title, width)),
        cursor::MoveTo(0, 1),
        Print(truncate(&format!("Folder: {}", current.display()), width)),
        cursor::MoveTo(0, 2),
        Print(ascii_rule(width)),
    )?;

    let (start, end) = nav.visible_range();
    for (row, idx) in (start..end).enumerate() {
        let y = (row + 3) as u16;
        let selected = idx == nav.selected();
        let mark = if selected { '>' } else { ' ' };
        let body = truncate(
            &format!("{mark} {}", entries[idx].label),
            width.saturating_sub(1),
        );
        queue!(out, cursor::MoveTo(0, y))?;
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
    }

    let footer_y = (3 + viewport_rows(nav) + 1) as u16;
    queue!(
        out,
        cursor::MoveTo(0, footer_y.saturating_sub(1)),
        Print(ascii_rule(width)),
        cursor::MoveTo(0, footer_y),
        Print("Up/Down move   Enter open/use   s use this folder   q cancel"),
    )?;
    if !message.is_empty() {
        queue!(
            out,
            cursor::MoveTo(0, footer_y + 1),
            Print(truncate(message, width)),
        )?;
    }
    out.flush()?;
    Ok(())
}

/// Rows the viewport actually shows (so the footer sits just below the list).
fn viewport_rows(nav: &ListNav) -> usize {
    let (s, e) = nav.visible_range();
    (e - s).max(MIN_VIEWPORT)
}
