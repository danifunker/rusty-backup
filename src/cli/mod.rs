//! Command-line surface for rusty-backup (`rb-cli`).
//!
//! The grammar follows two axes:
//!
//! 1. **Flat verbs** at the crate root — `backup`, `restore`, `inspect`,
//!    `ls`, `put`, `get`, `rm`, `fsck`, `new`, `expand`, `resize`,
//!    `shrink`, `convert`, `optical`, `write`, `batch`, `batch-template`,
//!    `terminal`, `show`, `completions`, `install-completions`. These are
//!    the stable, documented surface scripts should target.
//!
//! 2. **`api` namespace** (deprecated) — the original scratch grammar
//!    (`api hfs put …`, `api apm info`, `api sgi shrink`). Kept working
//!    through the transition so existing pipelines don't break.
//!
//! Mac paths on the CLI use `/` as the separator. `/` is illegal in HFS
//! filenames so there's no ambiguity. The HFS native separator `:` is
//! not accepted to keep the surface small and shell-friendly.

use anyhow::Result;
use clap::{Parser, Subcommand};

pub mod api;
pub mod exit;
pub mod io;
pub mod parse;

#[derive(Parser, Debug)]
#[command(
    name = "rb-cli",
    about = "Headless image-construction CLI for rusty-backup",
    disable_help_subcommand = true
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Unstable scratch namespace for low-level operations. Grammar inside
    /// `api` is expected to churn — do not depend on it from durable scripts.
    Api {
        #[command(subcommand)]
        group: api::ApiGroup,
    },
}

/// Run the parsed CLI and return on completion. Errors propagate to the
/// `main` shim, which maps them onto the exit-code table in
/// `crate::cli::exit`.
pub fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Command::Api { group } => api::run(group),
    }
}
