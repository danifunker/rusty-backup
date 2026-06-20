//! Shared crossterm primitives for rb-cli's interactive screens.
//!
//! ASCII-only and console-safe (MiSTer framebuffer / serial): a TTY guard that
//! refuses to drive a TUI on a non-interactive stream, and an RAII terminal
//! guard that always restores raw mode + the alternate screen on drop — even
//! during a panic — so a botched exit can't wedge the console.
//!
//! `bless pick` (`verbs/bless_pick.rs`) predates this module and keeps private
//! copies of the same helpers; it can migrate here later.

use std::io::{self, IsTerminal};

use anyhow::{bail, Result};
use crossterm::{cursor, execute, terminal};

/// Fallback terminal size when `terminal::size()` reports nothing useful
/// (some console setups do).
pub const FALLBACK_COLS: u16 = 80;
pub const FALLBACK_ROWS: u16 = 24;

/// Bail (with a scripting hint) unless both stdin and stdout are interactive
/// terminals. Keeps automated/piped runs from hanging or corrupting output.
pub fn require_interactive_tty(what: &str, script_hint: &str) -> Result<()> {
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        bail!("{what} needs an interactive terminal; {script_hint}");
    }
    Ok(())
}

/// RAII raw-mode + alternate-screen guard. Restores everything (cursor too) on
/// drop, including while unwinding.
pub struct TermGuard;

impl TermGuard {
    pub fn enter() -> io::Result<Self> {
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

/// Truncate to `max` display columns, appending `..` when clipped. ASCII-only,
/// so byte length equals column count.
pub fn truncate(s: &str, max: usize) -> String {
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

/// An ASCII horizontal rule, capped so it never wraps absurdly wide consoles.
pub fn ascii_rule(width: usize) -> String {
    "-".repeat(width.min(120))
}

/// Smallest list viewport we'll ever render, even on a tiny console.
pub const MIN_VIEWPORT: usize = 3;

/// A reusable list cursor + scroll window, decoupled from what's being listed.
/// The owner stores the items; `ListNav` tracks the selection and which slice is
/// visible. Re-point it at a new list with [`set_len`](ListNav::set_len) (used
/// when the directory picker descends into a folder).
#[derive(Debug, Clone)]
pub struct ListNav {
    len: usize,
    sel: usize,
    scroll: usize,
    viewport: usize,
}

impl ListNav {
    pub fn new(len: usize) -> Self {
        ListNav {
            len,
            sel: 0,
            scroll: 0,
            viewport: MIN_VIEWPORT,
        }
    }

    pub fn selected(&self) -> usize {
        self.sel
    }
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Replace the item count (e.g. after navigating to a new folder), resetting
    /// the cursor to the top.
    pub fn set_len(&mut self, len: usize) {
        self.len = len;
        self.sel = 0;
        self.scroll = 0;
    }

    pub fn set_viewport(&mut self, rows: usize) {
        self.viewport = rows.max(MIN_VIEWPORT);
        self.clamp();
    }

    pub fn move_up(&mut self) {
        if self.sel > 0 {
            self.sel -= 1;
            self.clamp();
        }
    }

    pub fn move_down(&mut self) {
        if self.sel + 1 < self.len {
            self.sel += 1;
            self.clamp();
        }
    }

    /// Inclusive-exclusive range of item indices currently visible.
    pub fn visible_range(&self) -> (usize, usize) {
        let end = (self.scroll + self.viewport).min(self.len);
        (self.scroll, end)
    }

    fn clamp(&mut self) {
        if self.sel < self.scroll {
            self.scroll = self.sel;
        } else if self.sel >= self.scroll + self.viewport {
            self.scroll = self.sel + 1 - self.viewport;
        }
        let max_scroll = self.len.saturating_sub(self.viewport);
        if self.scroll > max_scroll {
            self.scroll = max_scroll;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_clips_with_ellipsis() {
        assert_eq!(truncate("System Folder", 7), "Syste..");
        assert_eq!(truncate("abc", 10), "abc");
        assert_eq!(truncate("anything", 0), "");
        assert_eq!(truncate("anything", 2), "..");
    }

    #[test]
    fn listnav_moves_and_clamps() {
        let mut n = ListNav::new(3);
        n.set_viewport(4);
        n.move_up();
        assert_eq!(n.selected(), 0);
        n.move_down();
        n.move_down();
        n.move_down();
        assert_eq!(n.selected(), 2);
    }

    #[test]
    fn listnav_scroll_follows_selection() {
        let mut n = ListNav::new(10);
        n.set_viewport(4);
        for _ in 0..6 {
            n.move_down();
        }
        let (start, end) = n.visible_range();
        assert!(start <= 6 && 6 < end);
        assert_eq!(end - start, 4);
    }

    #[test]
    fn listnav_set_len_resets() {
        let mut n = ListNav::new(10);
        n.set_viewport(4);
        n.move_down();
        n.move_down();
        n.set_len(2);
        assert_eq!(n.selected(), 0);
        assert_eq!(n.len(), 2);
    }
}
