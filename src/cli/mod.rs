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
pub mod backup_edit;
pub mod config;
pub mod copy_paths;
pub mod device_safety;
pub mod dir_picker;
pub mod exit;
pub mod glob;
pub mod img_at;
pub mod io;
pub mod logging;
pub mod optical_hint;
pub mod output;
pub mod parse;
pub mod resolve;
pub mod tui;
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
    pub command: Option<Command>,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Create a blank image, grouped by media class: `new floppy <fs>`,
    /// `new volume <fs>` (bare superfloppy), or `new hd {x68k|sgi-efs}`
    /// (partition-table-wrapped, bootable). CD-ROM images are under
    /// `optical new`; multi-partition images go through `batch`.
    New {
        #[command(subcommand)]
        cmd: verbs::new::NewCommand,
    },

    /// Install an Apple SCSI driver + Driver Descriptor Record into an APM
    /// disk so a classic-Mac ROM (e.g. Quadra 800) registers it over SCSI.
    /// Operates in place; partition data is never moved. (This registers the
    /// driver so the ROM can read the disk — it does not change HFS
    /// boot-block behavior.)
    #[command(name = "mac-scsi-bless")]
    MacScsiBless(verbs::mac_scsi_bless::MacScsiBlessArgs),

    /// Auto-detect what a Mac disk needs to boot and apply only the missing
    /// pieces: SCSI driver + DDR (full APM disks), boot blocks (copied from a
    /// `--boot-from` donor), and a blessed System Folder. Idempotent; a flat
    /// HFV is kept flat. Works on flat HFVs and full APM disks alike.
    #[command(name = "make-bootable")]
    MakeBootable(verbs::make_bootable::MakeBootableArgs),

    /// List a directory inside a filesystem.
    Ls(verbs::ls::LsArgs),

    /// Recursive both-fork (data + resource) disk usage of paths inside a
    /// filesystem. Unlike `ls`, which reports data-fork sizes only, `du`
    /// counts the resource fork too — essential for classic-Mac apps whose
    /// code lives in a resource fork over a 0-byte data fork.
    Du(verbs::du::DuArgs),

    /// Print the absolute byte offset and length of a file inside an
    /// image (HFS only today). Output is JSON so build scripts that
    /// patch disk offsets into boot blocks can parse it with `jq`.
    Locate(verbs::locate::LocateArgs),

    /// Copy a host file (or zero-fill / write boot blocks) into a filesystem.
    Put(verbs::put::PutArgs),

    /// Extract a file, directory tree, or glob match from a filesystem
    /// to the host.
    Get(verbs::get::GetArgs),

    /// Archive a filesystem (or a subtree) to a single `.tar.gz` /
    /// `.tar.zst` / `.tar`. Preserves exact case-sensitive names and real
    /// symlinks, so extracting on a case-insensitive host won't clobber
    /// files that differ only in case.
    Tar(verbs::tar::TarArgs),

    /// Import a `.tar.gz` / `.tar.zst` / `.tar` archive's contents INTO a
    /// filesystem in an image (the inverse of `tar`). Recreates the tree,
    /// streams files in, and recreates symlinks where the target FS
    /// supports them.
    Untar(verbs::untar::UntarArgs),

    /// Copy files / directory trees between two disk images without
    /// staging through the host. SRC may be a glob; DST follows `cp`
    /// semantics (into an existing directory, or rename to a target).
    Cp(verbs::cp::CpArgs),

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

    /// Write the resource fork of an existing HFS / HFS+ / MFS file from a
    /// host file.
    Setrsrc(verbs::setrsrc::SetRsrcArgs),

    /// Rename the volume at IMG[@N] (HFS only today).
    Setvolname(verbs::setvolname::SetVolNameArgs),

    /// Reformat a partition in place, leaving the partition table intact
    /// (HFS only today).
    Reformat(verbs::reformat::ReformatArgs),

    /// Put a MacBinary I / II archive: both forks + full Finder info in
    /// one shot (HFS; on MFS both forks + type/creator, extended Finder
    /// flags/dates skipped).
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

    /// Run the network daemon so a remote `rb-cli` can browse and read
    /// files inside images this host holds (`rb://host:port/img@N`).
    /// Family F read-only (Phase 0). See docs/remote_transfer_plan.md.
    #[cfg(feature = "remote")]
    Serve(verbs::serve::ServeArgs),

    /// Back up a disk image or device to a backup folder.
    Backup(verbs::backup::BackupArgs),

    /// Restore a backup folder to a target image or device.
    Restore(verbs::restore::RestoreArgs),

    /// Pack a backup folder into a single `.cbk` container, or unpack one
    /// (`cbk pack` / `cbk unpack`). `restore` also reads a `.cbk` directly.
    Cbk(verbs::cbk::CbkArgs),

    /// Interactive backup/restore menu (the appliance UI): pick a disk, then
    /// Inspect / Backup / Restore. Needs an interactive terminal.
    Menu(verbs::menu::MenuArgs),

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

    /// Optical-media verbs (drives / rip / convert / browse / extract).
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

    /// Read/write classic Mac archives (list / extract / create; accepts
    /// .sit, .sea, .cpt, .mar, and their BinHex-wrapped .hqx forms).
    #[command(alias = "sit")]
    Archive {
        #[command(subcommand)]
        cmd: verbs::archive::ArchiveCommand,
    },

    /// Open an interactive rb-cli shell (rustyline-based REPL).
    Terminal,

    /// Check for a newer release and (when built with `--features tui-update`)
    /// self-update. Without that feature it reports that updates weren't compiled
    /// in and prints the releases URL, exiting non-zero.
    Update,

    /// Launch the full-screen terminal UI (preview): a menu-driven ratatui
    /// app that runs anywhere rusty-backup does, including serial consoles and
    /// vintage terminals. Needs an interactive terminal.
    #[cfg(feature = "tui")]
    Tui,

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
        Some(command) => dispatch(command),
        None => no_command(),
    }
}

/// Handle `rb-cli` with no subcommand. On an interactive terminal this offers to
/// launch the TUI (respecting the `[tui] launch` config preference); otherwise —
/// or when the `tui` feature isn't built — it prints the help text, so a bare
/// `rb-cli` in a script or pipe still behaves like `--help`.
fn no_command() -> Result<()> {
    #[cfg(feature = "tui")]
    {
        use std::io::IsTerminal;
        if std::io::stdin().is_terminal() && std::io::stdout().is_terminal() {
            return match tui_launch_pref() {
                TuiLaunch::Never => print_help(),
                TuiLaunch::Always => verbs::tui_app::run(),
                TuiLaunch::Ask => prompt_and_launch_tui(),
            };
        }
    }
    print_help()
}

/// Print the top-level `--help` text (used when there's no subcommand and no
/// interactive launch).
fn print_help() -> Result<()> {
    use clap::CommandFactory;
    Cli::command().print_help()?;
    println!();
    Ok(())
}

/// The `[tui] launch` preference: `ask` (default), `always`, or `never`.
#[cfg(feature = "tui")]
enum TuiLaunch {
    Ask,
    Always,
    Never,
}

#[cfg(feature = "tui")]
fn tui_launch_pref() -> TuiLaunch {
    let val = config::default_path().and_then(|p| {
        config::Config::load(&p)
            .ok()
            .and_then(|c| c.get("tui", "launch").map(|s| s.to_ascii_lowercase()))
    });
    match val.as_deref() {
        Some("never") | Some("off") | Some("false") => TuiLaunch::Never,
        Some("always") | Some("on") | Some("true") => TuiLaunch::Always,
        _ => TuiLaunch::Ask,
    }
}

/// Ask whether to launch the TUI. Enter / `y` launches it; `n` prints help;
/// `never` persists `[tui] launch = never` and prints help (so a bare `rb-cli`
/// will show help from then on).
#[cfg(feature = "tui")]
fn prompt_and_launch_tui() -> Result<()> {
    use std::io::Write;

    eprintln!(
        "rusty-backup {} — interactive terminal UI",
        env!("APP_VERSION")
    );
    eprint!("Launch the TUI now? [Y/n]  (type 'never' to stop asking): ");
    std::io::stderr().flush().ok();

    let mut line = String::new();
    if std::io::stdin().read_line(&mut line)? == 0 {
        // EOF (e.g. stdin closed): fall back to help.
        return print_help();
    }
    match line.trim().to_ascii_lowercase().as_str() {
        "" | "y" | "yes" => verbs::tui_app::run(),
        "never" => {
            if let Some(path) = config::default_path() {
                match config::append_setting(&path, "tui", "launch", "never") {
                    Ok(()) => eprintln!(
                        "Saved: bare `rb-cli` will show help. Run `rb-cli tui` to open the UI, \
                         or set `[tui] launch = ask` in {} to be asked again.",
                        path.display()
                    ),
                    Err(e) => eprintln!("warning: could not save preference: {e:#}"),
                }
            }
            print_help()
        }
        _ => print_help(),
    }
}

/// Dispatch one parsed `Command` to its verb. Separated from `run` so the
/// REPL (`verbs::terminal`) can dispatch follow-up commands without
/// re-installing logging.
pub fn dispatch(command: Command) -> Result<()> {
    match command {
        Command::New { cmd } => verbs::new::run(cmd),
        Command::MacScsiBless(args) => verbs::mac_scsi_bless::run(args),
        Command::MakeBootable(args) => verbs::make_bootable::run(args),
        Command::Ls(args) => verbs::ls::run(args),
        Command::Du(args) => verbs::du::run(args),
        Command::Locate(args) => verbs::locate::run(args),
        Command::Put(args) => verbs::put::run(args),
        Command::Get(args) => verbs::get::run(args),
        Command::Tar(args) => verbs::tar::run(args),
        Command::Untar(args) => verbs::untar::run(args),
        Command::Cp(args) => verbs::cp::run(args),
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
        #[cfg(feature = "remote")]
        Command::Serve(args) => verbs::serve::run(args),
        Command::Backup(args) => verbs::backup::run(args),
        Command::Restore(args) => verbs::restore::run(args),
        Command::Cbk(args) => verbs::cbk::run(args),
        Command::Menu(args) => verbs::menu::run(args),
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
        Command::Archive { cmd } => verbs::archive::run(cmd),
        Command::Terminal => verbs::terminal::run(),
        Command::Update => verbs::update::run(),
        #[cfg(feature = "tui")]
        Command::Tui => verbs::tui_app::run(),
        Command::Completions(args) => verbs::completions::run_emit(args),
        Command::InstallCompletions(args) => verbs::completions::run_install(args),
        Command::Api { group } => api::run(group),
    }
}
