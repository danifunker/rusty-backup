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
    // Same compile-time APP_VERSION the GUI reports (build.rs bakes it from
    // the CI RELEASE_VERSION, falling back to CARGO_PKG_VERSION). Gives the
    // CLI a `--version` / `-V` flag at parity with the GUI's version display.
    version = env!("APP_VERSION"),
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

    /// Build a self-bootable Sharp X68000 HDD image (SASI / SCSI) with
    /// an X68K partition table + IPL stub + Human68k partition,
    /// optionally pre-populated by cloning a Human68k donor floppy.
    #[command(name = "new-x68k-hdd")]
    NewX68kHdd(verbs::new_x68k_hdd::NewX68kHddArgs),

    /// Install an Apple SCSI driver + Driver Descriptor Record into an APM
    /// disk so a classic-Mac ROM (e.g. Quadra 800) registers it over SCSI.
    /// Operates in place; partition data is never moved. (This registers the
    /// driver so the ROM can read the disk — it does not change HFS
    /// boot-block behavior.)
    #[command(name = "mac-scsi-bless")]
    MacScsiBless(verbs::mac_scsi_bless::MacScsiBlessArgs),

    /// List a directory inside a filesystem.
    Ls(verbs::ls::LsArgs),

    /// Print the absolute byte offset and length of a file inside an
    /// image (HFS only today). Output is JSON so build scripts that
    /// patch disk offsets into boot blocks can parse it with `jq`.
    Locate(verbs::locate::LocateArgs),

    /// Copy a host file (or zero-fill / write boot blocks) into a filesystem.
    Put(verbs::put::PutArgs),

    /// Extract a file, directory tree, or glob match from a filesystem
    /// to the host.
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

    /// Inspect or set the bootable System Folder on an HFS / HFS+ volume
    /// (`set` / `show` / `pick`).
    Bless {
        #[command(subcommand)]
        cmd: verbs::bless::BlessCommand,
    },

    /// Change the type and/or creator code on an existing HFS / HFS+ /
    /// ProDOS file.
    Chmeta(verbs::chmeta::ChmetaArgs),

    /// Write the resource fork of an existing HFS / HFS+ file from a
    /// host file.
    Setrsrc(verbs::setrsrc::SetRsrcArgs),

    /// Rename the volume at IMG[@N] (HFS only today).
    Setvolname(verbs::setvolname::SetVolNameArgs),

    /// Reformat a partition in place, leaving the partition table intact
    /// (HFS only today).
    Reformat(verbs::reformat::ReformatArgs),

    /// Put a MacBinary I / II archive: both forks + full Finder info in
    /// one shot (HFS today).
    #[command(name = "put-macbinary")]
    PutMacBinary(verbs::put_macbinary::PutMacBinaryArgs),

    /// Decode a BinHex 4.0 (.hqx) file and write it (both forks + Finder
    /// info) into a filesystem.
    #[command(name = "put-binhex")]
    PutBinHex(verbs::binhex::PutBinHexArgs),

    /// Extract a file and encode it as BinHex 4.0 (.hqx), preserving both
    /// forks and the type/creator codes.
    #[command(name = "get-binhex")]
    GetBinHex(verbs::binhex::GetBinHexArgs),

    /// Resize the filesystem at IMG@N to a new size (FAT/NTFS/exFAT/HFS+/
    /// ext/btrfs/SFS/PFS3/AFFS/EFS — whichever magic matches).
    Resize(verbs::resize::ResizeArgs),

    /// Defragment a Human68k (X68000) partition in place: clone it into a
    /// fresh, contiguously-packed volume and write that back. Reclaims
    /// holes the in-place resizer can't (it keeps cluster byte-offsets).
    Repack(verbs::repack::RepackArgs),

    /// Expand a classic-HFS volume to a new size + allocation block size
    /// by cloning into a fresh APM disk image (default) or a bare HFS
    /// image (`--to-hfv`). Accepts APM-wrapped sources or raw single-
    /// partition HFS images.
    Expand(verbs::expand::ExpandArgs),

    /// Grow a disk image by `--add SIZE` of trailing zero-padding so a
    /// subsequent `partmap` edit can place a new partition.
    Grow(verbs::grow::GrowArgs),

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

    /// Optical-media verbs (rip / convert / browse / extract).
    #[cfg(feature = "optical")]
    Optical {
        #[command(subcommand)]
        cmd: verbs::optical::OpticalCommand,
    },

    /// Floppy-container verbs (convert / info) for XDF, HDM, DIM, D88.
    Floppy {
        #[command(subcommand)]
        cmd: verbs::floppy::FloppyCommand,
    },

    /// Edit the partition table (add / resize / delete / set-type /
    /// set-bootable). Partition *data* is never moved.
    Partmap {
        #[command(subcommand)]
        cmd: verbs::partmap::PartmapCommand,
    },

    /// Read classic StuffIt and Compact Pro archives (list / extract; accepts
    /// .sit, .sea, .cpt, and their BinHex-wrapped .hqx forms).
    Sit {
        #[command(subcommand)]
        cmd: verbs::sit::SitCommand,
    },

    /// Open an interactive rb-cli shell (rustyline-based REPL).
    Terminal,

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
    dispatch(cli.command)
}

/// Dispatch one parsed `Command` to its verb. Separated from `run` so the
/// REPL (`verbs::terminal`) can dispatch follow-up commands without
/// re-installing logging.
pub fn dispatch(command: Command) -> Result<()> {
    match command {
        Command::New(args) => verbs::new::run(args),
        Command::NewX68kHdd(args) => verbs::new_x68k_hdd::run(args),
        Command::MacScsiBless(args) => verbs::mac_scsi_bless::run(args),
        Command::Ls(args) => verbs::ls::run(args),
        Command::Locate(args) => verbs::locate::run(args),
        Command::Put(args) => verbs::put::run(args),
        Command::Get(args) => verbs::get::run(args),
        Command::Rm(args) => verbs::rm::run(args),
        Command::Mkdir(args) => verbs::mkdir::run(args),
        Command::Fsck(args) => verbs::fsck::run(args),
        Command::Shrink(args) => verbs::shrink::run(args),
        Command::Bless { cmd } => verbs::bless::run(cmd),
        Command::Chmeta(args) => verbs::chmeta::run(args),
        Command::Setrsrc(args) => verbs::setrsrc::run(args),
        Command::Setvolname(args) => verbs::setvolname::run(args),
        Command::Reformat(args) => verbs::reformat::run(args),
        Command::PutMacBinary(args) => verbs::put_macbinary::run(args),
        Command::PutBinHex(args) => verbs::binhex::run_put(args),
        Command::GetBinHex(args) => verbs::binhex::run_get(args),
        Command::Resize(args) => verbs::resize::run(args),
        Command::Repack(args) => verbs::repack::run(args),
        Command::Expand(args) => verbs::expand::run(args),
        Command::Grow(args) => verbs::grow::run(args),
        Command::Inspect(args) => verbs::inspect::run(args),
        Command::Backup(args) => verbs::backup::run(args),
        Command::Restore(args) => verbs::restore::run(args),
        Command::Write(args) => verbs::write::run(args),
        Command::Convert(args) => verbs::convert::run(args),
        Command::Batch(args) => verbs::batch::run(args),
        Command::BatchTemplate(args) => verbs::batch_template::run(args),
        Command::Config { cmd } => verbs::config::run(cmd),
        Command::Show { cmd } => verbs::show::run(cmd),
        #[cfg(feature = "optical")]
        Command::Optical { cmd } => verbs::optical::run(cmd),
        Command::Floppy { cmd } => verbs::floppy::run(cmd),
        Command::Partmap { cmd } => verbs::partmap::run(cmd),
        Command::Sit { cmd } => verbs::sit::run(cmd),
        Command::Terminal => verbs::terminal::run(),
        Command::Completions(args) => verbs::completions::run_emit(args),
        Command::InstallCompletions(args) => verbs::completions::run_install(args),
        Command::Api { group } => api::run(group),
    }
}
