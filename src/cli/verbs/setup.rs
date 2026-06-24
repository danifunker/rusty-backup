//! `rb-cli serve setup` — the interactive daemon console.
//!
//! This is the **UI** half of the MiSTer "rb-daemon": the screen the Scripts
//! menu brings up (via the `rb-daemon.sh` shim → `rb-cli serve setup`). It
//! reflects the current state — running or not, set to start on boot or not,
//! and the IP:port to connect to — and offers a tiny set of actions:
//!
//! ```text
//!  Rusty Backup Daemon (rb-daemon)        [ INACTIVE ]
//!  autostart on boot: disabled
//!  connect to:  192.168.1.42:7341  (eth0)
//!  -------------------------------------------------------
//!    > Start Now
//!      Install Autostart (start on every boot)
//!      Quit
//!  -------------------------------------------------------
//!  Up/Down move   Enter select   q/Esc quit
//! ```
//!
//! It changes nothing on launch — it only reflects state until a key is pressed
//! (plan §14, "default to maintain existing configuration"). All logic lives in
//! [`crate::remote::service`]; this file is just rendering + key handling, built
//! on the shared crossterm primitives in [`crate::cli::tui`]. ASCII-only per the
//! no-Unicode-glyph rule.

use std::io::{self, Write};

use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::style::{Attribute, Print, SetAttribute};
use crossterm::{cursor, queue, terminal};

use crate::cli::tui::{ascii_rule, require_interactive_tty, truncate, TermGuard, FALLBACK_COLS};
use crate::remote::service::{self as svc, ServicePaths, ServiceStatus, SetupAction};

/// Entry point for `rb-cli serve setup`.
pub fn run() -> Result<()> {
    require_interactive_tty(
        "rb-daemon setup",
        "to script it, use `rb-cli serve service <start|stop|install|uninstall|status>`",
    )?;
    let paths = ServicePaths::detect()?;
    let _guard = TermGuard::enter()?;

    let mut sel: usize = 0;
    let mut message = String::new();

    loop {
        let status = svc::status(&paths);
        let actions = svc::setup_actions(&status);
        if sel >= actions.len() {
            sel = actions.len().saturating_sub(1);
        }
        render(&status, &actions, sel, &message)?;

        let Event::Key(key) = event::read()? else {
            continue;
        };
        if key.kind == KeyEventKind::Release {
            continue;
        }
        let ctrl_c =
            key.modifiers.contains(KeyModifiers::CONTROL) && matches!(key.code, KeyCode::Char('c'));
        if ctrl_c {
            break;
        }
        match key.code {
            KeyCode::Up | KeyCode::Char('k') => sel = sel.saturating_sub(1),
            KeyCode::Down | KeyCode::Char('j') if sel + 1 < actions.len() => sel += 1,
            KeyCode::Esc | KeyCode::Char('q') => break,
            KeyCode::Enter | KeyCode::Char(' ') => {
                let action = actions[sel];
                if action == SetupAction::Quit {
                    break;
                }
                message = match svc::apply_setup_action(&paths, action) {
                    Ok(msg) => msg,
                    Err(e) => format!("Error: {e:#}"),
                };
            }
            _ => {}
        }
    }
    Ok(())
}

/// One line describing where clients should point — first LAN address with the
/// bind port, plus a hint about any extras, or a clear "no network" note.
fn connect_line(status: &ServiceStatus) -> String {
    let port = svc::bind_port(&status.bind);
    match status.addrs.first() {
        None => format!("connect to:  (no network detected)   port {port}"),
        Some(primary) => {
            let mut line = format!("connect to:  {}:{}", primary.ip, port);
            if !primary.iface.is_empty() {
                line.push_str(&format!("  ({})", primary.iface));
            }
            if status.addrs.len() > 1 {
                let extra: Vec<String> = status.addrs[1..].iter().map(|a| a.ip.clone()).collect();
                line.push_str(&format!("   also {}", extra.join(", ")));
            }
            line
        }
    }
}

fn render(
    status: &ServiceStatus,
    actions: &[SetupAction],
    sel: usize,
    message: &str,
) -> Result<()> {
    let mut out = io::stdout();
    let width = terminal::size().map(|(c, _)| c).unwrap_or(FALLBACK_COLS) as usize;

    let state = if status.active {
        "[ ACTIVE ]"
    } else {
        "[ INACTIVE ]"
    };
    let title = "Rusty Backup Daemon (rb-daemon)";
    let header = format!("{title:<39} {state}");
    let boot = format!(
        "autostart on boot: {}",
        if status.enabled {
            "enabled"
        } else {
            "disabled"
        }
    );

    queue!(
        out,
        terminal::Clear(terminal::ClearType::All),
        cursor::MoveTo(0, 0),
        Print(truncate(&header, width)),
        cursor::MoveTo(0, 1),
        Print(truncate(&boot, width)),
        cursor::MoveTo(0, 2),
        Print(truncate(&connect_line(status), width)),
        cursor::MoveTo(0, 3),
        Print(ascii_rule(width)),
    )?;

    let mut y: u16 = 4;
    for (i, action) in actions.iter().enumerate() {
        queue!(out, cursor::MoveTo(0, y))?;
        let marker = if i == sel { ">" } else { " " };
        let label = format!("  {marker} {}", action.label());
        if i == sel {
            queue!(
                out,
                SetAttribute(Attribute::Reverse),
                Print(truncate(&label, width)),
                SetAttribute(Attribute::Reset),
            )?;
        } else {
            queue!(out, Print(truncate(&label, width)))?;
        }
        y += 1;
    }

    queue!(
        out,
        cursor::MoveTo(0, y + 1),
        Print(ascii_rule(width)),
        cursor::MoveTo(0, y + 2),
        Print("Up/Down move   Enter select   q/Esc quit"),
    )?;
    if !message.is_empty() {
        queue!(
            out,
            cursor::MoveTo(0, y + 3),
            Print(truncate(message, width))
        )?;
    }
    out.flush()?;
    Ok(())
}
