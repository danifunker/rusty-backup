//! `rb-cli config <subcommand>` — manage the rbcli.conf file.

use anyhow::{Context, Result};
use clap::Subcommand;

use crate::cli::config::{default_path, TEMPLATE};
use crate::cli::logging::{log_stderr, out_stdout};

#[derive(Debug, Subcommand)]
pub enum ConfigCommand {
    /// Write a commented template to the user's config location (or
    /// to `--path PATH`). Refuses to overwrite an existing file
    /// unless `--force`.
    Init {
        /// Override the destination path.
        #[arg(long)]
        path: Option<std::path::PathBuf>,
        /// Overwrite an existing file.
        #[arg(long)]
        force: bool,
    },
    /// Print the resolved config-file path (whether it exists or not).
    Path,
    /// Print the loaded config as `section.key = value` lines.
    Show {
        #[arg(long)]
        path: Option<std::path::PathBuf>,
    },
}

pub fn run(cmd: ConfigCommand) -> Result<()> {
    match cmd {
        ConfigCommand::Init { path, force } => init(path, force),
        ConfigCommand::Path => path_cmd(),
        ConfigCommand::Show { path } => show(path),
    }
}

fn init(path: Option<std::path::PathBuf>, force: bool) -> Result<()> {
    let dest = match path {
        Some(p) => p,
        None => default_path()
            .ok_or_else(|| anyhow::anyhow!("can't locate config home (set HOME or pass --path)"))?,
    };
    if dest.exists() && !force {
        anyhow::bail!(
            "{} already exists; pass --force to overwrite",
            dest.display()
        );
    }
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    std::fs::write(&dest, TEMPLATE).with_context(|| format!("writing {}", dest.display()))?;
    log_stderr(format!("Wrote template config to {}", dest.display()));
    Ok(())
}

fn path_cmd() -> Result<()> {
    let p =
        default_path().ok_or_else(|| anyhow::anyhow!("can't locate config home (HOME unset)"))?;
    out_stdout(p.display().to_string());
    Ok(())
}

fn show(path: Option<std::path::PathBuf>) -> Result<()> {
    let p = match path {
        Some(p) => p,
        None => default_path().ok_or_else(|| anyhow::anyhow!("can't locate config home"))?,
    };
    let cfg = crate::cli::config::Config::load(&p)?;
    if cfg.sections.is_empty() {
        log_stderr(format!("{} is empty or missing", p.display()));
        return Ok(());
    }
    for (section, entries) in &cfg.sections {
        for (k, v) in entries {
            out_stdout(format!("{section}.{k} = {v}"));
        }
    }
    Ok(())
}
