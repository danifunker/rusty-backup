//! `rb-cli terminal` — interactive REPL on top of the one-shot verbs.
//!
//! Each input line is parsed with the same clap grammar `rb-cli` uses
//! one-shot, so anything that works on the command line works in here
//! too: `ls disk.hda@1 /System`, `put foo.bin disk.hda@1 /Apps`, etc.
//! Type `help` for the verb list, `exit` / `quit` / Ctrl-D to leave.
//!
//! History is persisted to a platform-default path so completions
//! across sessions are useful: `$XDG_STATE_HOME/rb-cli/history`
//! (Linux/macOS) or `%LOCALAPPDATA%\rb-cli\history` (Windows).

use anyhow::Result;
use clap::{CommandFactory, Parser};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::path::PathBuf;

use crate::cli::logging::{log_stderr, out_stdout};
use crate::cli::Command as CliCommand;

#[derive(Parser, Debug)]
#[command(
    name = "",
    no_binary_name = true,
    about = "rb-cli interactive shell",
    disable_help_subcommand = true,
    disable_help_flag = false
)]
struct ReplCli {
    #[command(subcommand)]
    command: CliCommand,
}

pub fn run() -> Result<()> {
    out_stdout("rb-cli interactive shell. Type 'help' for the verb list or Ctrl-D to exit.");

    let mut rl = DefaultEditor::new().map_err(|e| anyhow::anyhow!("init rustyline: {e}"))?;
    let history_path = history_file();
    if let Some(p) = &history_path {
        if p.exists() {
            let _ = rl.load_history(p);
        } else if let Some(parent) = p.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
    }

    loop {
        match rl.readline("rb-cli> ") {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let _ = rl.add_history_entry(trimmed);

                match trimmed {
                    "exit" | "quit" => break,
                    "help" => {
                        let mut cmd = ReplCli::command();
                        cmd.print_help().ok();
                        println!();
                        continue;
                    }
                    _ => {}
                }

                let argv = match shell_words::split(trimmed) {
                    Ok(v) => v,
                    Err(e) => {
                        log_stderr(format!("parse error: {e}"));
                        continue;
                    }
                };

                let parsed = match ReplCli::try_parse_from(&argv) {
                    Ok(p) => p,
                    Err(e) => {
                        // clap prints its own error to stderr; print() emits
                        // the help/error message to whichever stream matches
                        // its kind, and we keep the loop going.
                        e.print().ok();
                        continue;
                    }
                };

                if let Err(e) = crate::cli::dispatch(parsed.command) {
                    log_stderr(format!("error: {e:#}"));
                }
            }
            Err(ReadlineError::Interrupted) => continue, // Ctrl-C: clear current line
            Err(ReadlineError::Eof) => break,            // Ctrl-D: leave
            Err(e) => {
                log_stderr(format!("readline error: {e}"));
                break;
            }
        }
    }

    if let Some(p) = &history_path {
        let _ = rl.save_history(p);
    }
    Ok(())
}

fn history_file() -> Option<PathBuf> {
    if cfg!(windows) {
        std::env::var_os("LOCALAPPDATA").map(|v| PathBuf::from(v).join("rb-cli/history"))
    } else {
        let base = std::env::var_os("XDG_STATE_HOME")
            .map(PathBuf::from)
            .or_else(|| dirs::home_dir().map(|h| h.join(".local/state")));
        base.map(|b| b.join("rb-cli/history"))
    }
}
