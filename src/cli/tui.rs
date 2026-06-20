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
}
