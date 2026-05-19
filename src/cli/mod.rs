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
pub mod config;
pub mod device_safety;
pub mod exit;
pub mod glob;
pub mod img_at;
pub mod io;
pub mod logging;
pub mod output;
pub mod parse;
pub mod resolve;
pub mod verbs;

#[derive(Parser, Debug)]
#[command(
    name = "rb-cli",
    about = "Headless image-construction CLI for rusty-backup",
    disable_help_subcommand = true
)]
pub struct Cli {
    /// Global verbosity / progress / color / log-file flags applied to
    /// every verb (see `logging::GlobalFlags`).
    #[command(flatten)]
    pub globals: logging::GlobalFlags,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Create a blank single-partition image (superfloppy or, in Phase
    /// D, partition-table-wrapped).
    New(verbs::new::NewArgs),

    /// List a directory inside a filesystem.
    Ls(verbs::ls::LsArgs),

    /// Copy a host file (or zero-fill / write boot blocks) into a filesystem.
    Put(verbs::put::PutArgs),

    /// Extract a file from a filesystem to the host.
    Get(verbs::get::GetArgs),

    /// Delete a file or directory from a filesystem.
    Rm(verbs::rm::RmArgs),

    /// Create a directory inside a filesystem.
    Mkdir(verbs::mkdir::MkdirArgs),

    /// Check (and optionally repair) a filesystem.
    Fsck(verbs::fsck::FsckArgs),

    /// Re-encode a disk image into a CHD with trailing zero padding
    /// dropped (SGI/IRIX today).
    Shrink(verbs::shrink::ShrinkArgs),

    /// Resize the filesystem at IMG@N to a new size (FAT/NTFS/exFAT/HFS+/
    /// ext/btrfs/SFS/PFS3/AFFS/EFS — whichever magic matches).
    Resize(verbs::resize::ResizeArgs),

    /// Expand a classic-HFS volume to a new size + allocation block size
    /// by cloning into a fresh APM disk image.
    Expand(verbs::expand::ExpandArgs),

    /// Whole-disk aggregate read-only view (partition table + per-partition
    /// summary + CHD metadata when applicable).
    Inspect(verbs::inspect::InspectArgs),

    /// Back up a disk image or device to a backup folder.
    Backup(verbs::backup::BackupArgs),

    /// Restore a backup folder to a target image or device.
    Restore(verbs::restore::RestoreArgs),

    /// Stream an image file onto a block device.
    Write(verbs::write::WriteArgs),

    /// Re-encode one or more disk images into a chosen output format.
    Convert(verbs::convert::ConvertArgs),

    /// Apply a JSON-described sequence of FS operations to an image as
    /// one transaction-like batch.
    Batch(verbs::batch::BatchArgs),

    /// Generate a starter `batch` JSON script from a host directory.
    #[command(name = "batch-template")]
    BatchTemplate(verbs::batch_template::BatchTemplateArgs),

    /// Manage the rbcli.conf config file.
    Config {
        #[command(subcommand)]
        cmd: verbs::config::ConfigCommand,
    },

    /// Focused read-only queries.
    Show {
        #[command(subcommand)]
        cmd: verbs::show::ShowCommand,
    },

    /// Emit a shell-completion script to stdout.
    #[command(name = "completions")]
    Completions(verbs::completions::EmitArgs),

    /// Install shell completions to the user-scoped canonical location.
    #[command(name = "install-completions")]
    InstallCompletions(verbs::completions::InstallArgs),

    /// Unstable scratch namespace for low-level operations. Kept as a
    /// deprecated alias for the flat verbs above; grammar inside `api`
    /// is expected to churn — do not depend on it from durable scripts.
    #[command(hide = true)]
    Api {
        #[command(subcommand)]
        group: api::ApiGroup,
    },
}

/// Run the parsed CLI and return on completion. Installs logging from
/// the global flags before dispatching. Errors propagate to the `main`
/// shim, which maps them onto the exit-code table in
/// [`crate::cli::exit`].
pub fn run(cli: Cli) -> Result<()> {
    logging::install(&cli.globals)?;
    match cli.command {
        Command::New(args) => verbs::new::run(args),
        Command::Ls(args) => verbs::ls::run(args),
        Command::Put(args) => verbs::put::run(args),
        Command::Get(args) => verbs::get::run(args),
        Command::Rm(args) => verbs::rm::run(args),
        Command::Mkdir(args) => verbs::mkdir::run(args),
        Command::Fsck(args) => verbs::fsck::run(args),
        Command::Shrink(args) => verbs::shrink::run(args),
        Command::Resize(args) => verbs::resize::run(args),
        Command::Expand(args) => verbs::expand::run(args),
        Command::Inspect(args) => verbs::inspect::run(args),
        Command::Backup(args) => verbs::backup::run(args),
        Command::Restore(args) => verbs::restore::run(args),
        Command::Write(args) => verbs::write::run(args),
        Command::Convert(args) => verbs::convert::run(args),
        Command::Batch(args) => verbs::batch::run(args),
        Command::BatchTemplate(args) => verbs::batch_template::run(args),
        Command::Config { cmd } => verbs::config::run(cmd),
        Command::Show { cmd } => verbs::show::run(cmd),
        Command::Completions(args) => verbs::completions::run_emit(args),
        Command::InstallCompletions(args) => verbs::completions::run_install(args),
        Command::Api { group } => api::run(group),
    }
}
