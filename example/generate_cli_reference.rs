//! Generate `docs/cli-reference.md` from the clap definition.
//!
//! Run with `cargo run --example generate_cli_reference -- OUT_PATH`,
//! defaulting to `docs/cli-reference.md`. Walks every subcommand
//! recursively and emits a Markdown chapter per verb, with usage,
//! flags, and argument descriptions sourced from clap.
//!
//! The output is the source-of-truth verb reference shipped with
//! every release. Hand-edited cookbook + examples live in separate
//! Markdown files (`docs/cli-cookbook.md`, `docs/cli-examples.md`).

use clap::{Arg, ArgAction, Command, CommandFactory};
use rusty_backup::cli::Cli;
use std::fmt::Write as _;

fn main() -> std::io::Result<()> {
    let out_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "docs/cli-reference.md".to_string());

    let cmd = Cli::command();
    let bin = cmd.get_name().to_string();
    let mut md = String::new();

    writeln!(
        md,
        "# `{bin}` reference\n\n\
         _Auto-generated from the clap argument definitions in `src/cli/`.  \
         Re-run `cargo run --example generate_cli_reference` after grammar \
         changes._\n"
    )
    .unwrap();

    writeln!(md, "## Synopsis\n").unwrap();
    writeln!(md, "```\n{}\n```\n", cmd.clone().render_usage()).unwrap();

    writeln!(md, "## Global options\n").unwrap();
    render_args(&mut md, &cmd, &[]);

    writeln!(md, "## Verbs\n").unwrap();
    let mut sub_path = Vec::new();
    render_subcommands(&mut md, &cmd, &mut sub_path);

    std::fs::write(&out_path, &md)?;
    eprintln!("Wrote {} ({} bytes)", out_path, md.len());
    Ok(())
}

fn render_subcommands(out: &mut String, cmd: &Command, path: &mut Vec<String>) {
    let mut subs: Vec<&Command> = cmd.get_subcommands().collect();
    subs.sort_by_key(|c| c.get_name());
    for sub in subs {
        if sub.get_name() == "help" {
            continue;
        }
        path.push(sub.get_name().to_string());
        let heading = path.join(" ");
        writeln!(out, "### `{heading}`\n").unwrap();
        if let Some(about) = sub.get_about() {
            writeln!(out, "{about}\n").unwrap();
        }
        writeln!(out, "```\n{}\n```\n", sub.clone().render_usage()).unwrap();
        render_args(out, sub, path);
        render_subcommands(out, sub, path);
        path.pop();
    }
}

fn render_args(out: &mut String, cmd: &Command, _path: &[String]) {
    let positionals: Vec<&Arg> = cmd.get_positionals().collect();
    let flags: Vec<&Arg> = cmd
        .get_arguments()
        .filter(|a| !a.is_positional())
        .filter(|a| !is_help_flag(a))
        .collect();

    if !positionals.is_empty() {
        writeln!(out, "**Arguments**\n").unwrap();
        for a in &positionals {
            write_arg(out, a);
        }
        writeln!(out).unwrap();
    }
    if !flags.is_empty() {
        writeln!(out, "**Options**\n").unwrap();
        for a in &flags {
            write_arg(out, a);
        }
        writeln!(out).unwrap();
    }
}

fn is_help_flag(a: &Arg) -> bool {
    a.get_id().as_str() == "help"
        || matches!(
            a.get_action(),
            ArgAction::Help | ArgAction::HelpLong | ArgAction::HelpShort | ArgAction::Version
        )
}

fn write_arg(out: &mut String, a: &Arg) {
    let name = a.get_id().as_str();
    let long = a.get_long().map(|s| format!("--{s}"));
    let short = a.get_short().map(|c| format!("-{c}"));
    let label = if a.is_positional() {
        format!("`<{}>`", name.to_uppercase())
    } else {
        match (short, long) {
            (Some(s), Some(l)) => format!("`{s}` / `{l}`"),
            (None, Some(l)) => format!("`{l}`"),
            (Some(s), None) => format!("`{s}`"),
            _ => format!("`{name}`"),
        }
    };
    let help = a
        .get_help()
        .map(|h| h.to_string())
        .or_else(|| a.get_long_help().map(|h| h.to_string()))
        .unwrap_or_default();
    writeln!(out, "- {label} — {help}").unwrap();
}
