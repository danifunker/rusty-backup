//! `rb-cli optical <SUBCOMMAND>` — CD/DVD-focused verbs that mirror the
//! GUI's Optical tab. Thin CLI over the existing engine layer in
//! `src/optical/`.
//!
//! Subcommands:
//! - `drives` — list connected physical optical drives
//! - `rip` — rip a physical disc to ISO or BIN/CUE
//! - `convert` — re-encode an optical image (ISO ↔ BIN/CUE ↔ CHD)
//! - `browse` — list files on an optical image (ISO9660 / Joliet / HFS)
//! - `extract` — extract files from an optical image to a host folder
//!
//! The GUI's interactive drive picker has no terminal equivalent; run
//! `rb-cli optical drives` to find a drive path, then pass it as
//! `--device PATH` to `rip`.

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand, ValueEnum};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::backup::LogMessage;
use crate::cli::logging::log_stderr;
use crate::cli::output::{emit_envelope, require_non_flat, Envelope, OutputFormat};
use crate::optical::{
    convert::{bincue_to_iso, chd_to_bincue, chd_to_iso, iso_to_bincue, to_chd, ConvertProgress},
    rip::{run_rip, OpticalTarget, RipConfig, RipFormat, RipProgress},
};
use crate::partition::format_size;
use crate::rbformats::chd_options::{ChdOptions, ChdProfile};

#[derive(Debug, Subcommand)]
pub enum OpticalCommand {
    /// List connected physical optical drives and their device paths.
    Drives(DrivesArgs),
    /// Rip a physical CD/DVD drive to a disk image file.
    Rip(RipArgs),
    /// Re-encode an optical image into a different format.
    Convert(ConvertArgs),
    /// List the file tree on an optical disc image.
    Browse(BrowseArgs),
    /// Recursive both-fork (data + resource) disk usage of paths on an optical
    /// disc image — the disc counterpart of the top-level `du` verb, for
    /// hybrid Mac discs whose apps keep code in the resource fork.
    Du(OpticalDuArgs),
    /// Print volume-level metadata for an optical disc image (leniently).
    Info(InfoArgs),
    /// Extract files from an optical disc image into a host folder.
    Extract(ExtractArgs),
    /// Work with El Torito boot images (extract / replace).
    Boot {
        #[command(subcommand)]
        cmd: BootCommand,
    },
}

pub fn run(cmd: OpticalCommand) -> Result<()> {
    match cmd {
        OpticalCommand::Drives(a) => run_drives_verb(a),
        OpticalCommand::Rip(a) => run_rip_verb(a),
        OpticalCommand::Convert(a) => run_convert_verb(a),
        OpticalCommand::Browse(a) => run_browse_verb(a),
        OpticalCommand::Du(a) => run_du_verb(a),
        OpticalCommand::Info(a) => run_info_verb(a),
        OpticalCommand::Extract(a) => run_extract_verb(a),
        OpticalCommand::Boot { cmd } => run_boot(cmd),
    }
}

/// `optical boot <SUBCOMMAND>` — El Torito boot-image operations. The disc layer
/// (this crate + opticaldiscs) extracts / places the boot image as an opaque
/// blob; the boot image *is* a nested disk image, so its filesystem is browsed
/// and edited with rusty-backup's ordinary disk-image verbs (`ls`, `inspect`,
/// `get`, `put`, `rm`, `mkdir`, `fsck`, …).
#[derive(Debug, Subcommand)]
pub enum BootCommand {
    /// Extract a boot image to a file — then inspect or edit it with the
    /// disk-image verbs, and put it back with `optical boot replace`.
    Extract(BootExtractArgs),
    /// Replace a boot image with the bytes of a (edited) disk-image file.
    /// Raw `.iso` only; same-size replaces in place, a grown image relocates.
    Replace(BootReplaceArgs),
}

fn run_boot(cmd: BootCommand) -> Result<()> {
    match cmd {
        BootCommand::Extract(a) => run_boot_extract(a),
        BootCommand::Replace(a) => run_boot_replace(a),
    }
}

#[derive(Debug, Args)]
pub struct BootExtractArgs {
    /// Bootable optical disc image (.iso, …).
    pub source: PathBuf,
    /// Destination file for the extracted boot image.
    #[arg(long)]
    pub to: PathBuf,
    /// Which boot entry to extract (default 0; see `optical info`).
    #[arg(long, default_value_t = 0)]
    pub index: usize,
}

#[derive(Debug, Args)]
pub struct BootReplaceArgs {
    /// Bootable optical image to edit — raw `.iso` only.
    pub source: PathBuf,
    /// Disk-image file whose bytes become the new boot image.
    #[arg(long)]
    pub from: PathBuf,
    /// Which boot entry to replace (default 0).
    #[arg(long, default_value_t = 0)]
    pub index: usize,
    /// Override the emulation/media type (default: keep the entry's current
    /// one). One of `floppy1.2` / `floppy1.44` / `floppy2.88` / `no-emulation`
    /// / `harddisk`.
    #[arg(long)]
    pub media: Option<String>,
}

fn run_boot_extract(args: BootExtractArgs) -> Result<()> {
    use opticaldiscs::detect::DiscImageInfo;

    let info = DiscImageInfo::open(&args.source)
        .with_context(|| format!("opening {}", args.source.display()))?;
    let catalog = info.el_torito.as_ref().ok_or_else(|| {
        anyhow::anyhow!("{}: not an El Torito bootable disc", args.source.display())
    })?;
    let entry = catalog.entries.get(args.index).ok_or_else(|| {
        anyhow::anyhow!(
            "boot entry {} out of range ({} present)",
            args.index,
            catalog.entries.len()
        )
    })?;
    let mut reader = open_sector_reader(&info).ok_or_else(|| {
        anyhow::anyhow!(
            "raw-sector reads are unavailable for the {} container",
            disc_format_token(info.format)
        )
    })?;
    let bytes = opticaldiscs::read_boot_image(&mut *reader, entry)
        .map_err(|e| anyhow::anyhow!("reading boot image: {e}"))?;
    std::fs::write(&args.to, &bytes).with_context(|| format!("writing {}", args.to.display()))?;
    log_stderr(format!(
        "Extracted boot image {} ({}) to {}",
        args.index,
        format_size(bytes.len() as u64),
        args.to.display()
    ));
    if let Some(fs) = detect_boot_image_fs(&bytes) {
        log_stderr(format!(
            "Filesystem: {fs} -- inspect with `rb-cli ls {}` / `inspect`, edit with \
             `rb-cli put`/`rm`/`mkdir`, then put it back with `optical boot replace`.",
            args.to.display()
        ));
    }
    Ok(())
}

fn run_boot_replace(args: BootReplaceArgs) -> Result<()> {
    use opticaldiscs::el_torito_edit::ElToritoEditor;

    let bytes =
        std::fs::read(&args.from).with_context(|| format!("reading {}", args.from.display()))?;
    let mut editor = ElToritoEditor::open_path(&args.source)
        .map_err(|e| anyhow::anyhow!("opening {} for editing: {e}", args.source.display()))?;

    let entries = editor.entries();
    let media = match &args.media {
        Some(s) => {
            media_type_from_str(s).ok_or_else(|| anyhow::anyhow!("unknown --media '{s}'"))?
        }
        None => {
            entries
                .get(args.index)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "boot entry {} out of range ({} present)",
                        args.index,
                        entries.len()
                    )
                })?
                .media_type
        }
    };
    drop(entries);

    editor
        .replace_image(args.index, &bytes, media)
        .map_err(|e| anyhow::anyhow!("replacing boot image: {e}"))?;
    editor
        .commit()
        .map_err(|e| anyhow::anyhow!("writing changes to {}: {e}", args.source.display()))?;
    log_stderr(format!(
        "Replaced boot image {} in {} ({})",
        args.index,
        args.source.display(),
        format_size(bytes.len() as u64)
    ));
    Ok(())
}

fn media_type_from_str(s: &str) -> Option<opticaldiscs::BootMediaType> {
    use opticaldiscs::BootMediaType as M;
    match s
        .to_ascii_lowercase()
        .replace(['_', '-', '.', ' '], "")
        .as_str()
    {
        "floppy12m" | "floppy12" | "12m" => Some(M::Floppy1_2M),
        "floppy144m" | "floppy144" | "144m" => Some(M::Floppy1_44M),
        "floppy288m" | "floppy288" | "288m" => Some(M::Floppy2_88M),
        "noemulation" | "noemul" | "none" => Some(M::NoEmulation),
        "harddisk" | "hdd" | "hd" => Some(M::HardDisk),
        _ => None,
    }
}

// ---------------- drives ----------------

#[derive(Debug, Args)]
pub struct DrivesArgs {
    /// Also query these daemons for their optical drives (repeatable), e.g.
    /// `--remote mister.local:7341`. Remote rows print an `rb://...` device arg
    /// you can pass straight to `optical rip --device`.
    #[arg(long = "remote", value_name = "HOST:PORT")]
    pub remotes: Vec<String>,
}

/// List optical drives via the unified picker core: local drives, plus the
/// drives of each `--remote` daemon. Prints one drive per line to stdout as
/// `<device-arg>  <display-name>` — `<device-arg>` is a local path or an
/// `rb://host:port/dev/sr0` URL, feedable straight to `optical rip --device`.
/// ASCII only (no glyphs) per the project's terminal-output rule.
fn run_drives_verb(args: DrivesArgs) -> Result<()> {
    // `devices` is only mutated by the remote arm below.
    #[cfg_attr(not(feature = "remote"), allow(unused_mut))]
    let mut devices = crate::model::optical_devices::list_local_rip_devices();
    #[cfg(feature = "remote")]
    for addr in &args.remotes {
        let conn = crate::remote::connection::RemoteConnection::connect_shared(addr)
            .with_context(|| format!("connecting to {addr}"))?;
        crate::model::optical_devices::append_remote_rip_devices(&mut devices, &conn);
    }
    #[cfg(not(feature = "remote"))]
    if !args.remotes.is_empty() {
        bail!("--remote needs the `remote` feature; this binary was built without it");
    }

    if devices.is_empty() {
        log_stderr("No optical drives found.");
        return Ok(());
    }
    log_stderr(format!("Found {} optical drive(s):", devices.len()));
    for d in &devices {
        println!("{}  {}", d.cli_device_arg(), d.display_name);
    }
    Ok(())
}

// ---------------- rip ----------------

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum RipFmt {
    /// Single .iso (data tracks only, 2048-byte sectors).
    Iso,
    /// BIN/CUE pair (raw 2352-byte sectors, all tracks).
    Bincue,
}

impl From<RipFmt> for RipFormat {
    fn from(f: RipFmt) -> Self {
        match f {
            RipFmt::Iso => RipFormat::Iso,
            RipFmt::Bincue => RipFormat::BinCue,
        }
    }
}

#[derive(Debug, Args)]
pub struct RipArgs {
    /// Source drive: a local path (e.g. `/dev/sr0`, `disk6`, `\\.\E:`) or a
    /// remote daemon's drive as `rb://host:port/dev/sr0` (the daemon issues the
    /// SCSI reads; this side does the encoding). `rb-cli optical drives` lists
    /// local drives.
    #[arg(long)]
    pub device: PathBuf,

    /// Output path: `.iso` for `--format iso`, `.cue` for `--format bincue`.
    #[arg(long)]
    pub output: PathBuf,

    #[arg(long, value_enum, default_value = "iso")]
    pub format: RipFmt,

    /// Eject the disc after a successful rip.
    #[arg(long)]
    pub eject: bool,
}

fn run_rip_verb(args: RipArgs) -> Result<()> {
    log_stderr(format!(
        "rb-cli optical rip: {} -> {} (format: {:?})",
        args.device.display(),
        args.output.display(),
        args.format
    ));
    let config = RipConfig {
        device: OpticalTarget::resolve(&args.device.to_string_lossy())?,
        output_path: args.output,
        format: args.format.into(),
        eject_after: args.eject,
    };
    let progress = Arc::new(Mutex::new(RipProgress::new()));
    let progress_thread = Arc::clone(&progress);
    let worker = std::thread::spawn(move || run_rip(config, progress_thread));
    drain_rip(progress)?;
    worker
        .join()
        .map_err(|_| anyhow::anyhow!("rip worker thread panicked"))?
        .context("rip failed")
}

fn drain_rip(progress: Arc<Mutex<RipProgress>>) -> Result<()> {
    let mut last_op = String::new();
    let mut last_pct: i32 = -1;
    let mut tracker = crate::model::rate_tracker::RateTracker::default();
    loop {
        std::thread::sleep(Duration::from_millis(250));
        let (logs, op, cur, total, finished, error) = match progress.lock() {
            Ok(mut p) => {
                let drained: Vec<LogMessage> = p.log_messages.drain(..).collect();
                (
                    drained,
                    p.operation.clone(),
                    p.current_bytes,
                    p.total_bytes,
                    p.finished,
                    p.error.clone(),
                )
            }
            Err(_) => bail!("rip worker poisoned its status mutex"),
        };
        for LogMessage { level, message } in logs {
            log_stderr(format!("[{level:?}] {message}"));
        }
        // Sample every tick (the stage label resets the window between phases),
        // even though we only print every 5%, so the rate/ETA is warm by then.
        tracker.record(cur, &op);
        if op != last_op {
            log_stderr(format!("status: {op}"));
            last_op = op;
            last_pct = -1;
        }
        if total > 0 {
            let pct = ((cur as f64 / total as f64) * 100.0) as i32;
            if pct / 5 != last_pct / 5 {
                log_stderr(format!(
                    "  progress: {pct:>3}% ({}/{}){}",
                    format_size(cur),
                    format_size(total),
                    tracker.suffix(cur, total),
                ));
                last_pct = pct;
            }
        }
        if finished {
            if let Some(e) = error {
                bail!("rip failed: {e}");
            }
            return Ok(());
        }
    }
}

// ---------------- convert ----------------

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum OpticalConvertFmt {
    /// Output a plain `.iso` (data tracks only).
    Iso,
    /// Output a BIN/CUE pair.
    Bincue,
    /// Output a CD CHD via libchdman-rs.
    Chd,
}

#[derive(Debug, Args)]
pub struct ConvertArgs {
    /// Source image (.iso, .cue, or .chd).
    pub source: PathBuf,
    /// Destination file. Extension is *not* auto-derived — pass it explicitly.
    pub dest: PathBuf,
    /// Output format.
    #[arg(long, value_enum)]
    pub format: OpticalConvertFmt,
}

fn run_convert_verb(args: ConvertArgs) -> Result<()> {
    let src_ext = args
        .source
        .extension()
        .and_then(|e| e.to_str())
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_default();
    log_stderr(format!(
        "rb-cli optical convert: {} -> {} (format: {:?})",
        args.source.display(),
        args.dest.display(),
        args.format
    ));
    let progress = Arc::new(Mutex::new(ConvertProgress::new()));
    let progress_thread = Arc::clone(&progress);
    let dest = args.dest.clone();
    let source = args.source.clone();
    let chd_options = Some(ChdOptions::defaults_for(ChdProfile::Cd));
    let worker = std::thread::spawn(move || -> Result<()> {
        match args.format {
            OpticalConvertFmt::Chd => to_chd(&source, &dest, chd_options, progress_thread),
            OpticalConvertFmt::Iso => match src_ext.as_str() {
                "cue" => bincue_to_iso(&source, &dest, progress_thread),
                "chd" => chd_to_iso(&source, &dest, progress_thread),
                _ => bail!(
                    "don't know how to convert {} to ISO; source must be .cue or .chd",
                    source.display()
                ),
            },
            OpticalConvertFmt::Bincue => match src_ext.as_str() {
                "iso" => {
                    let bin = dest.with_extension("bin");
                    iso_to_bincue(&source, &bin, &dest, progress_thread)
                }
                "chd" => chd_to_bincue(&source, &dest, progress_thread),
                _ => bail!(
                    "don't know how to convert {} to BIN/CUE; source must be .iso or .chd",
                    source.display()
                ),
            },
        }
    });
    drain_convert(progress)?;
    worker
        .join()
        .map_err(|_| anyhow::anyhow!("convert worker thread panicked"))?
        .context("convert failed")
}

fn drain_convert(progress: Arc<Mutex<ConvertProgress>>) -> Result<()> {
    let mut last_op = String::new();
    let mut last_pct: i32 = -1;
    let mut tracker = crate::model::rate_tracker::RateTracker::default();
    loop {
        std::thread::sleep(Duration::from_millis(250));
        let (logs, op, cur, total, finished, error) = match progress.lock() {
            Ok(mut p) => {
                let drained: Vec<LogMessage> = p.log_messages.drain(..).collect();
                (
                    drained,
                    p.operation.clone(),
                    p.current_bytes,
                    p.total_bytes,
                    p.finished,
                    p.error.clone(),
                )
            }
            Err(_) => bail!("convert worker poisoned its status mutex"),
        };
        for LogMessage { level, message } in logs {
            log_stderr(format!("[{level:?}] {message}"));
        }
        // Sample every tick (the stage label resets the window between phases),
        // even though we only print every 5%, so the rate/ETA is warm by then.
        tracker.record(cur, &op);
        if op != last_op {
            log_stderr(format!("status: {op}"));
            last_op = op;
            last_pct = -1;
        }
        if total > 0 {
            let pct = ((cur as f64 / total as f64) * 100.0) as i32;
            if pct / 5 != last_pct / 5 {
                log_stderr(format!(
                    "  progress: {pct:>3}% ({}/{}){}",
                    format_size(cur),
                    format_size(total),
                    tracker.suffix(cur, total),
                ));
                last_pct = pct;
            }
        }
        if finished {
            if let Some(e) = error {
                bail!("convert failed: {e}");
            }
            return Ok(());
        }
    }
}

// ---------------- browse ----------------

/// Per-file content-hash algorithm for `optical browse --hash`.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum HashAlgo {
    Sha256,
}

/// Which filesystem to open on a disc that carries more than one (a hybrid
/// Mac/PC disc: an ISO 9660 volume plus an Apple HFS partition on one track).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum)]
pub enum FilesystemSelect {
    /// The primary filesystem opticaldiscs detects — ISO 9660 on a hybrid disc.
    #[default]
    Auto,
    /// The ISO 9660 / High Sierra volume (the PC side).
    Iso,
    /// The Apple HFS / HFS+ volume (the Mac side of a hybrid disc).
    Hfs,
}

/// Open the filesystem the `--filesystem` selection asks for, returning it along
/// with the [`FilesystemType`](opticaldiscs::FilesystemType) actually opened (so
/// callers report the real tree, not the primary). Errors clearly when the
/// requested side isn't on the disc.
///
/// Shared by `optical browse` and `optical extract` so the two never diverge.
fn open_selected_filesystem(
    info: &opticaldiscs::detect::DiscImageInfo,
    select: FilesystemSelect,
) -> Result<(
    Box<dyn opticaldiscs::browse::filesystem::Filesystem>,
    opticaldiscs::FilesystemType,
)> {
    use opticaldiscs::browse::{open_disc_filesystem, open_hybrid_filesystem};
    use opticaldiscs::FilesystemType as F;

    let primary = || {
        open_disc_filesystem(info)
            .map(|fs| (fs, info.filesystem))
            .map_err(|e| anyhow::anyhow!("opening disc filesystem: {e}"))
    };

    match select {
        FilesystemSelect::Auto => primary(),
        FilesystemSelect::Hfs => {
            if matches!(info.filesystem, F::Hfs | F::HfsPlus) {
                // A pure-Mac disc: the primary already is the HFS volume.
                primary()
            } else if let Some(idx) = info
                .hybrid_filesystems
                .iter()
                .position(|h| matches!(h.filesystem, F::Hfs | F::HfsPlus))
            {
                let ty = info.hybrid_filesystems[idx].filesystem;
                open_hybrid_filesystem(info, idx)
                    .map(|fs| (fs, ty))
                    .map_err(|e| anyhow::anyhow!("opening hybrid HFS filesystem: {e}"))
            } else {
                bail!(
                    "no HFS filesystem on this disc (not a hybrid Mac/PC disc); its filesystem is {}",
                    fs_token(info.filesystem)
                )
            }
        }
        FilesystemSelect::Iso => {
            if matches!(info.filesystem, F::Iso9660 | F::HighSierra) {
                primary()
            } else {
                bail!(
                    "no ISO 9660 filesystem to select on this disc; its primary filesystem is {}",
                    fs_token(info.filesystem)
                )
            }
        }
    }
}

#[derive(Debug, Args)]
pub struct BrowseArgs {
    /// Optical disc image (.iso, .cue, .chd).
    pub source: PathBuf,
    /// Output format. `text` (default) prints the human file tree unchanged;
    /// `json` / `yaml` emit a machine-readable, deterministically path-sorted
    /// listing.
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    pub format: OutputFormat,
    /// Per-file content hash to attach to each file entry. Structured output
    /// only (`--format json`). Currently only `sha256`.
    #[arg(long, value_enum)]
    pub hash: Option<HashAlgo>,
    /// Which filesystem to browse on a hybrid Mac/PC disc. `auto` (default)
    /// opens the primary (ISO 9660); `hfs` opens the Apple HFS side; `iso`
    /// forces the ISO 9660 tree. See `optical info` to see what a disc carries.
    #[arg(long = "filesystem", value_enum, default_value_t = FilesystemSelect::Auto)]
    pub filesystem: FilesystemSelect,
}

fn run_browse_verb(args: BrowseArgs) -> Result<()> {
    use opticaldiscs::detect::DiscImageInfo;

    require_non_flat(args.format, "optical browse")?;
    if args.hash.is_some() && !args.format.is_structured() {
        bail!(
            "--hash requires --format json (per-file hashes are only emitted in structured output)"
        );
    }

    let info = DiscImageInfo::open(&args.source)
        .with_context(|| format!("opening {}", args.source.display()))?;
    let (mut fs, opened_fs) = open_selected_filesystem(&info, args.filesystem)?;
    let root = fs
        .root()
        .map_err(|e| anyhow::anyhow!("reading root: {e}"))?;

    if args.format.is_structured() {
        let mut entries = Vec::new();
        collect_entries(&mut *fs, &root, args.hash.is_some(), &mut entries)?;
        // Deterministic, byte-wise sort so consumers can fingerprint the listing.
        entries.sort_by(|a, b| a.path.as_bytes().cmp(b.path.as_bytes()));
        let payload = BrowsePayload {
            image: args.source.display().to_string(),
            volume_name: fs.volume_name().map(str::to_owned),
            filesystem: fs_token(opened_fs),
            game: info.game.as_ref().map(crate::optical::format_game_identity),
            entries,
        };
        return emit_envelope(args.format, &Envelope::ok(payload));
    }

    // Text (default): the human ASCII tree, byte-for-byte unchanged.
    let label = fs.volume_name().unwrap_or("/").to_owned();
    let mut out = String::new();
    if let Some(g) = &info.game {
        out.push_str(&format!(
            "Game disc: {}\n",
            crate::optical::format_game_identity(g)
        ));
    }
    out.push_str(&label);
    out.push('\n');
    let mut dirs = 0u64;
    let mut files = 0u64;
    walk_tree(&mut *fs, &root, "", &mut out, &mut dirs, &mut files)?;
    out.push_str(&format!("\n{dirs} directories, {files} files\n"));
    print!("{out}");
    Ok(())
}

/// A single entry in the flat, machine-readable browse listing.
#[derive(Debug, Serialize)]
struct BrowseEntry {
    path: String,
    /// `file` | `dir` | `symlink`.
    kind: &'static str,
    /// Data-fork size, files only.
    #[serde(skip_serializing_if = "Option::is_none")]
    size_bytes: Option<u64>,
    /// HFS/HFS+ resource-fork size, present only when non-empty.
    #[serde(skip_serializing_if = "Option::is_none")]
    resource_fork_bytes: Option<u64>,
    /// HFS/HFS+ Finder type / creator codes, files only.
    #[serde(skip_serializing_if = "Option::is_none")]
    hfs_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    hfs_creator: Option<String>,
    /// Data-fork content hash, present only with `--hash`.
    #[serde(skip_serializing_if = "Option::is_none")]
    sha256: Option<String>,
    /// Resource-fork content hash, present with `--hash` when a fork exists.
    #[serde(skip_serializing_if = "Option::is_none")]
    resource_fork_sha256: Option<String>,
    /// Rock Ridge / UDF / Mac-alias symlink target, symlinks only.
    #[serde(skip_serializing_if = "Option::is_none")]
    symlink_target: Option<String>,
}

#[derive(Debug, Serialize)]
struct BrowsePayload {
    image: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    volume_name: Option<String>,
    filesystem: String,
    /// Game-disc identity string, when the image is a recognized game disc.
    #[serde(skip_serializing_if = "Option::is_none")]
    game: Option<String>,
    entries: Vec<BrowseEntry>,
}

/// Stable lowercase machine token for a detected filesystem, for JSON output.
fn fs_token(ft: opticaldiscs::FilesystemType) -> String {
    use opticaldiscs::FilesystemType as F;
    match ft {
        F::Iso9660 => "iso9660",
        F::HighSierra => "high_sierra",
        F::Joliet => "joliet",
        F::Udf => "udf",
        F::Hfs => "hfs",
        F::HfsPlus => "hfsplus",
        F::Efs => "efs",
        F::Ufs => "ufs",
        F::Ods2 => "ods2",
        F::GameCube => "gamecube",
        F::Wii => "wii",
        F::Cdi => "cdi",
        F::Opera => "opera",
        F::Xdvdfs => "xdvdfs",
        F::Unknown => "unknown",
    }
    .to_string()
}

/// 1 MiB streaming-hash chunk — keeps a full-disc walk bounded regardless of
/// individual file size.
const HASH_CHUNK: usize = 1 << 20;

fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn hash_data_fork(
    fs: &mut dyn opticaldiscs::browse::filesystem::Filesystem,
    entry: &opticaldiscs::browse::entry::FileEntry,
) -> Result<String> {
    let mut hasher = Sha256::new();
    let size = entry.size;
    let mut off = 0u64;
    while off < size {
        let want = (size - off).min(HASH_CHUNK as u64) as usize;
        let buf = fs
            .read_file_range(entry, off, want)
            .map_err(|e| anyhow::anyhow!("read_file_range: {e}"))?;
        if buf.is_empty() {
            break;
        }
        hasher.update(&buf);
        off += buf.len() as u64;
    }
    Ok(hex_lower(&hasher.finalize()))
}

fn hash_resource_fork(
    fs: &mut dyn opticaldiscs::browse::filesystem::Filesystem,
    entry: &opticaldiscs::browse::entry::FileEntry,
    size: u64,
) -> Result<Option<String>> {
    let mut hasher = Sha256::new();
    let mut off = 0u64;
    let mut any = false;
    while off < size {
        let want = (size - off).min(HASH_CHUNK as u64) as usize;
        match fs
            .read_resource_fork_range(entry, off, want)
            .map_err(|e| anyhow::anyhow!("read_resource_fork_range: {e}"))?
        {
            Some(buf) if !buf.is_empty() => {
                any = true;
                hasher.update(&buf);
                off += buf.len() as u64;
            }
            _ => break,
        }
    }
    Ok(any.then(|| hex_lower(&hasher.finalize())))
}

/// Recursively collect a flat listing of `dir`'s subtree into `out`.
fn collect_entries(
    fs: &mut dyn opticaldiscs::browse::filesystem::Filesystem,
    dir: &opticaldiscs::browse::entry::FileEntry,
    hash: bool,
    out: &mut Vec<BrowseEntry>,
) -> Result<()> {
    let children = fs
        .list_directory(dir)
        .map_err(|e| anyhow::anyhow!("list_directory: {e}"))?;
    for child in children {
        // A symlink is an `entry_type == File` whose target resolved, so test
        // the target before the file/dir split.
        let kind = if child.symlink_target.is_some() {
            "symlink"
        } else if child.is_directory() {
            "dir"
        } else {
            "file"
        };
        let mut entry = BrowseEntry {
            path: child.path.clone(),
            kind,
            size_bytes: None,
            resource_fork_bytes: None,
            hfs_type: None,
            hfs_creator: None,
            sha256: None,
            resource_fork_sha256: None,
            symlink_target: child.symlink_target.clone(),
        };
        if kind == "file" {
            entry.size_bytes = Some(child.size);
            let rsrc = child.resource_fork_size.filter(|&r| r > 0);
            entry.resource_fork_bytes = rsrc;
            entry.hfs_type = child.type_code_string();
            entry.hfs_creator = child.creator_code_string();
            if hash {
                entry.sha256 = Some(hash_data_fork(fs, &child)?);
                if let Some(rs) = rsrc {
                    entry.resource_fork_sha256 = hash_resource_fork(fs, &child, rs)?;
                }
            }
        }
        out.push(entry);
        // Recurse only into real directories (never through a symlink).
        if kind == "dir" {
            collect_entries(fs, &child, hash, out)?;
        }
    }
    Ok(())
}

fn walk_tree(
    fs: &mut dyn opticaldiscs::browse::filesystem::Filesystem,
    dir: &opticaldiscs::browse::entry::FileEntry,
    prefix: &str,
    out: &mut String,
    dirs: &mut u64,
    files: &mut u64,
) -> Result<()> {
    let children = fs
        .list_directory(dir)
        .map_err(|e| anyhow::anyhow!("list_directory: {e}"))?;
    let count = children.len();
    for (i, child) in children.iter().enumerate() {
        let is_last = i == count - 1;
        let connector = if is_last { "`-- " } else { "|-- " };
        out.push_str(prefix);
        out.push_str(connector);
        out.push_str(&child.name);
        if let Some(t) = &child.symlink_target {
            out.push_str(&format!(" -> {t}"));
        }
        if child.is_file() {
            out.push_str(&format!("  [{}]", format_size(child.total_size())));
            if let Some(rs) = child.resource_fork_size {
                if rs > 0 {
                    out.push_str(&format!(" (rsrc: {})", format_size(rs)));
                }
            }
            if let Some(tc) = child.type_code_string() {
                out.push_str(&format!("  {tc}"));
                if let Some(cc) = child.creator_code_string() {
                    out.push_str(&format!("/{cc}"));
                }
            }
        }
        out.push('\n');
        if child.is_directory() {
            *dirs += 1;
            let new_prefix = if is_last {
                format!("{prefix}    ")
            } else {
                format!("{prefix}|   ")
            };
            walk_tree(fs, child, &new_prefix, out, dirs, files)?;
        } else {
            *files += 1;
        }
    }
    Ok(())
}

// ---------------- du ----------------

#[derive(Debug, Args)]
pub struct OpticalDuArgs {
    /// Optical disc image (.iso, .cue, .chd).
    pub source: PathBuf,
    /// One or more paths inside the disc filesystem (use `/` as the
    /// separator). Defaults to the volume root when none are given.
    #[arg(value_name = "PATH")]
    pub paths: Vec<String>,
    /// Report subdirectory totals down to this many levels below each PATH
    /// (`0`, the default, prints only the totals for the path itself). The
    /// full subtree is always summed regardless.
    #[arg(long, default_value_t = 0)]
    pub depth: u32,
    /// Emit machine-readable JSON. Shorthand for `--format json`.
    #[arg(long, conflicts_with = "format")]
    pub json: bool,
    /// Output format.
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    pub format: OutputFormat,
    /// Which filesystem to measure on a hybrid Mac/PC disc. `auto` (default)
    /// opens the primary (ISO 9660); `hfs` opens the Apple HFS side — the one
    /// carrying resource forks. See `optical info` for what a disc holds.
    #[arg(long = "filesystem", value_enum, default_value_t = FilesystemSelect::Auto)]
    pub filesystem: FilesystemSelect,
}

fn run_du_verb(args: OpticalDuArgs) -> Result<()> {
    use opticaldiscs::detect::DiscImageInfo;

    let format = if args.json {
        OutputFormat::Json
    } else {
        args.format
    };
    require_non_flat(format, "optical du")?;

    let info = DiscImageInfo::open(&args.source)
        .with_context(|| format!("opening {}", args.source.display()))?;
    let (inner, opened_fs) = open_selected_filesystem(&info, args.filesystem)?;

    // Wrap the selected opticaldiscs filesystem in our adapter so the shared
    // `du` engine (both-fork sums + allocation-block rounding via the adapter's
    // allocation_unit) runs identically to the top-level `du` verb.
    // Label defaults to the inner filesystem's own volume name (see
    // `OpticalFilesystem::volume_label`).
    let mut fs = crate::fs::optical_fs::OpticalFilesystem::from_inner(
        inner,
        fs_token(opened_fs).to_string(),
        None,
    )
    .map_err(|e| anyhow::anyhow!("opening disc filesystem: {e}"))?;

    crate::cli::verbs::du::emit_du(&mut fs, args.paths, args.depth, format)
}

// ---------------- info ----------------

#[derive(Debug, Args)]
pub struct InfoArgs {
    /// Optical disc image (.iso, .cue, .chd).
    pub source: PathBuf,
    /// Output format: `text` (default), `json`, or `yaml`.
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    pub format: OutputFormat,
}

#[derive(Debug, Serialize)]
struct Iso9660Info {
    volume_id: String,
    system_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    publisher_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    preparer_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    application_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    volume_creation_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    volume_modification_date: Option<String>,
    volume_space_size: u32,
    logical_block_size: u16,
    has_rock_ridge: bool,
    has_joliet: bool,
    has_udf: bool,
}

#[derive(Debug, Serialize)]
struct HfsInfo {
    volume_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    created: Option<String>,
    partition_table: String,
    ddr_present: bool,
}

/// A filesystem co-resident with the primary on a hybrid Mac/PC disc — the
/// Apple_HFS side an ISO 9660 primary otherwise hides. Mirrors
/// `opticaldiscs::detect::HybridFilesystem`.
#[derive(Debug, Serialize)]
struct HybridFsInfo {
    /// `hfs` or `hfsplus`.
    filesystem: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    volume_name: Option<String>,
    /// Byte offset of the partition within the data track.
    partition_offset: u64,
}

#[derive(Debug, Serialize)]
struct ElToritoInfo {
    present: bool,
    entries: Vec<BootEntryInfo>,
}

#[derive(Debug, Serialize)]
struct BootEntryInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    platform: Option<String>,
    bootable: bool,
    media_type: String,
    load_rba: u32,
    boot_image_size_bytes: u64,
    /// The filesystem *inside* the boot image, detected by rusty-backup's own
    /// engine — the crate hands over an opaque blob and we interpret it.
    #[serde(skip_serializing_if = "Option::is_none")]
    boot_image_filesystem: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    boot_image_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
}

#[derive(Debug, Serialize)]
struct InfoPayload {
    image: String,
    /// Disc data size. For BIN/CUE this is the referenced BIN file(s), not the
    /// tiny CUE text; for a plain ISO/CHD it is the file itself.
    size_bytes: u64,
    /// Size of the sidecar `.cue`, when the image is a BIN/CUE pair and the cue
    /// exists. `None` for single-file containers.
    #[serde(skip_serializing_if = "Option::is_none")]
    cue_size_bytes: Option<u64>,
    container: String,
    filesystems: Vec<String>,
    /// Co-resident filesystems on a hybrid disc (the Mac HFS side beside an ISO
    /// 9660 primary). Empty for a single-filesystem disc.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    hybrid_filesystems: Vec<HybridFsInfo>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    warnings: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    iso9660: Option<Iso9660Info>,
    #[serde(skip_serializing_if = "Option::is_none")]
    hfs: Option<HfsInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    el_torito: Option<ElToritoInfo>,
}

fn run_info_verb(args: InfoArgs) -> Result<()> {
    use opticaldiscs::detect::DiscImageInfo;

    require_non_flat(args.format, "optical info")?;
    let image = args.source.display().to_string();
    let size_bytes = std::fs::metadata(&args.source)
        .map(|m| m.len())
        .unwrap_or(0);

    // Opening detects container + filesystem. A hard I/O failure is an error;
    // an unrecognized/unsupported disc is DATA (exit 0 + a warning), because
    // this verb exists precisely to survive discs strict parsers reject.
    let info = match DiscImageInfo::open(&args.source) {
        Ok(i) => i,
        Err(opticaldiscs::OpticaldiscsError::Io(e)) => {
            bail!("reading {}: {e}", args.source.display());
        }
        Err(e) => {
            return emit_info(
                args.format,
                InfoPayload {
                    image,
                    size_bytes,
                    container: "unknown".to_string(),
                    cue_size_bytes: None,
                    filesystems: Vec::new(),
                    hybrid_filesystems: Vec::new(),
                    warnings: vec![format!("unrecognized disc image: {e}")],
                    iso9660: None,
                    hfs: None,
                    el_torito: None,
                },
            );
        }
    };

    // For BIN/CUE, report the BIN data size (not the tiny CUE text the initial
    // `size_bytes` captured) plus the sidecar CUE size.
    let (size_bytes, cue_size_bytes) = optical_data_and_cue_sizes(&args.source, info.format);

    let mut warnings: Vec<String> = Vec::new();
    let mut reader = open_sector_reader(&info);
    if reader.is_none() && !matches!(info.format, opticaldiscs::DiscFormat::Iso) {
        warnings.push(format!(
            "raw-sector metadata (preparer id, El Torito) unavailable for the {} container",
            disc_format_token(info.format)
        ));
    }

    // A single borrow of the raw sector reader gathers everything needing
    // direct sector access: the preparer id, RR/Joliet/UDF presence, the LE/BE
    // consistency checks, and the El Torito boot catalog.
    let mut preparer_id = None;
    let mut has_rock_ridge = false;
    let mut has_joliet = false;
    let mut has_udf = false;
    if let Some(r) = reader.as_deref_mut() {
        if let Some(pvd) = info.pvd.as_ref() {
            if let Ok(sec) = r.read_sector(16) {
                if sec.len() >= 574 {
                    preparer_id = iso_trim(&String::from_utf8_lossy(&sec[446..574]));
                }
                if sec.len() >= 132 {
                    let sp_le = u32::from_le_bytes([sec[80], sec[81], sec[82], sec[83]]);
                    let sp_be = u32::from_be_bytes([sec[84], sec[85], sec[86], sec[87]]);
                    if sp_le != sp_be {
                        warnings.push(format!(
                            "PVD volume_space_size little/big-endian disagree ({sp_le} vs {sp_be}); using little-endian"
                        ));
                    }
                    let bs_le = u16::from_le_bytes([sec[128], sec[129]]);
                    let bs_be = u16::from_be_bytes([sec[130], sec[131]]);
                    if bs_le != bs_be {
                        warnings.push(format!(
                            "PVD logical_block_size little/big-endian disagree ({bs_le} vs {bs_be}); using little-endian"
                        ));
                    }
                }
            }
            has_rock_ridge = probe_rock_ridge(r, pvd);
            has_joliet = opticaldiscs::JolietVolumeDescriptor::find(r)
                .ok()
                .flatten()
                .is_some();
            has_udf = opticaldiscs::browse::udf::detect_udf(r);
        }
    }

    // El Torito: the crate parses the boot catalog into `info.el_torito`; per the
    // disc-layer / disk-image-layer boundary we (rusty-backup) hash each boot
    // image and detect its *nested* filesystem with our own engine.
    let el_torito = build_el_torito_info(info.el_torito.as_ref(), &mut reader);

    let iso9660 = info.pvd.as_ref().map(|pvd| Iso9660Info {
        volume_id: pvd.volume_id.trim().to_string(),
        system_id: iso_trim(&pvd.system_id).unwrap_or_default(),
        publisher_id: iso_trim(&pvd.publisher_id),
        preparer_id,
        application_id: iso_trim(&pvd.application_id),
        volume_creation_date: pvd.creation_date.as_ref().map(|d| d.to_iso8601()),
        volume_modification_date: pvd.modification_date.as_ref().map(|d| d.to_iso8601()),
        volume_space_size: pvd.volume_space_size,
        logical_block_size: pvd.logical_block_size,
        has_rock_ridge,
        has_joliet,
        has_udf,
    });

    // Partition table / HFS. Our own detector handles the zeroed-DDR case.
    let apm = probe_apm(&args.source);
    let (partition_table, ddr_present) = match &apm {
        Some(a) => ("APM".to_string(), a.ddr_present),
        None => ("none".to_string(), false),
    };
    let apm_has_hfs = apm.as_ref().is_some_and(|a| {
        a.entries
            .iter()
            .any(|e| e.partition_type.to_ascii_uppercase().contains("HFS"))
    });
    let hfs = build_hfs_info(&info, partition_table, ddr_present);

    // Assemble the filesystems list from everything detected.
    let mut filesystems: Vec<String> = Vec::new();
    if let Some(pvd) = &info.pvd {
        add_fs(
            &mut filesystems,
            if pvd.high_sierra {
                "high_sierra"
            } else {
                "iso9660"
            },
        );
    }
    if let Some(iso) = &iso9660 {
        if iso.has_joliet {
            add_fs(&mut filesystems, "joliet");
        }
        if iso.has_udf {
            add_fs(&mut filesystems, "udf");
        }
    }
    if info.hfs_mdb.is_some() || apm_has_hfs {
        add_fs(&mut filesystems, "hfs");
    }
    if info.hfsplus_header.is_some() {
        add_fs(&mut filesystems, "hfsplus");
    }
    // Hybrid Mac/PC disc: opticaldiscs detects the Apple_HFS side that the ISO
    // 9660 primary hides. Surface it in the token list and as structured detail
    // so a user knows the Mac tree is there (and can reach it with the browse /
    // extract filesystem selector).
    let hybrid_filesystems: Vec<HybridFsInfo> = info
        .hybrid_filesystems
        .iter()
        .map(|h| {
            let token = fs_token(h.filesystem);
            add_fs(&mut filesystems, &token);
            HybridFsInfo {
                filesystem: token,
                volume_name: h.volume_label.clone(),
                partition_offset: h.partition_offset,
            }
        })
        .collect();
    // A non-ISO, non-HFS primary (EFS / UFS / ODS-2 / game formats).
    if info.pvd.is_none() && info.hfs_mdb.is_none() && info.hfsplus_header.is_none() {
        let token = fs_token(info.filesystem);
        if token != "unknown" {
            add_fs(&mut filesystems, &token);
        }
    }
    if filesystems.is_empty() {
        warnings.push("no supported filesystem identified on this image".to_string());
    }

    emit_info(
        args.format,
        InfoPayload {
            image,
            size_bytes,
            container: disc_format_token(info.format),
            cue_size_bytes,
            filesystems,
            hybrid_filesystems,
            warnings,
            iso9660,
            hfs,
            el_torito,
        },
    )
}

fn add_fs(list: &mut Vec<String>, token: &str) {
    if !list.iter().any(|x| x == token) {
        list.push(token.to_string());
    }
}

/// Lowercase machine token for the container format (`iso`, `bincue`, `chd`, …).
fn disc_format_token(f: opticaldiscs::DiscFormat) -> String {
    format!("{f:?}").to_ascii_lowercase()
}

/// A raw sector reader for the raw-PVD / El Torito reads, for the containers we
/// can address directly. `None` (with a warning) for the rest — the
/// `DiscImageInfo`-derived fields still populate.
fn open_sector_reader(
    info: &opticaldiscs::detect::DiscImageInfo,
) -> Option<Box<dyn opticaldiscs::SectorReader>> {
    match info.format {
        opticaldiscs::DiscFormat::Iso => {
            opticaldiscs::sector_reader::IsoSectorReader::new(&info.path)
                .ok()
                .map(|r| Box::new(r) as Box<dyn opticaldiscs::SectorReader>)
        }
        _ => None,
    }
}

/// Detect the partition table with our own engine (which handles zeroed DDRs).
/// Resolve the disc *data* size and the sidecar `.cue` size for `optical info`.
///
/// A BIN/CUE image passed by its `.cue` would otherwise report the CUE's tiny
/// text size as "the disc" — misleading for a multi-hundred-MB rip. Here
/// `size_bytes` becomes the referenced BIN file(s) (summed, de-duplicated for
/// multi-FILE cues) and the CUE is reported alongside. Every other container is
/// a single file with no cue.
fn optical_data_and_cue_sizes(
    source: &Path,
    format: opticaldiscs::DiscFormat,
) -> (u64, Option<u64>) {
    let file_size = std::fs::metadata(source).map(|m| m.len()).unwrap_or(0);
    if format != opticaldiscs::DiscFormat::BinCue {
        return (file_size, None);
    }
    // Locate the .cue: the source itself, or the sibling of a .bin.
    let cue_path = if source
        .extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("cue"))
    {
        Some(source.to_path_buf())
    } else {
        let stem = source.file_stem().unwrap_or_default();
        let sibling = source.with_file_name(format!("{}.cue", stem.to_string_lossy()));
        sibling.exists().then_some(sibling)
    };
    let Some(cue_path) = cue_path else {
        return (file_size, None); // a bare .bin with no cue beside it
    };
    let cue_size = std::fs::metadata(&cue_path).map(|m| m.len()).ok();
    // Sum the unique BIN file(s) the cue references (opticaldiscs resolves each
    // path, including the stem fallback for a mismatched FILE line).
    let data_size = opticaldiscs::bincue::parse_cue_tracks(&cue_path)
        .ok()
        .map(|tracks| {
            let mut seen = std::collections::HashSet::new();
            tracks
                .iter()
                .filter(|t| seen.insert(t.bin_path.clone()))
                .map(|t| std::fs::metadata(&t.bin_path).map(|m| m.len()).unwrap_or(0))
                .sum::<u64>()
        })
        .filter(|&n| n > 0)
        .unwrap_or(file_size);
    (data_size, cue_size)
}

fn probe_apm(path: &Path) -> Option<crate::partition::apm::Apm> {
    let f = std::fs::File::open(path).ok()?;
    let mut r = std::io::BufReader::new(f);
    match crate::partition::PartitionTable::detect(&mut r) {
        Ok(crate::partition::PartitionTable::Apm(apm)) => Some(apm),
        _ => None,
    }
}

fn iso_trim(s: &str) -> Option<String> {
    let t = s.trim_end_matches([' ', '\0']).trim().to_string();
    (!t.is_empty()).then_some(t)
}

/// Probe the root directory's `.` record System-Use area for SUSP/Rock Ridge.
fn probe_rock_ridge(
    reader: &mut dyn opticaldiscs::SectorReader,
    pvd: &opticaldiscs::PrimaryVolumeDescriptor,
) -> bool {
    let dir = match reader.read_sector(pvd.root_directory_lba as u64) {
        Ok(d) => d,
        Err(_) => return false,
    };
    if dir.len() < 34 {
        return false;
    }
    let rec_len = dir[0] as usize;
    let name_len = dir[32] as usize;
    let mut su_start = 33 + name_len;
    if su_start % 2 == 1 {
        su_start += 1; // pad to even
    }
    if rec_len < 34 || rec_len > dir.len() || su_start >= rec_len {
        return false;
    }
    opticaldiscs::browse::rockridge::detect(&dir[su_start..rec_len])
}

fn platform_token(p: &opticaldiscs::Platform) -> String {
    use opticaldiscs::Platform as P;
    match p {
        P::X86 => "x86".to_string(),
        P::PowerPc => "ppc".to_string(),
        P::Mac => "mac".to_string(),
        P::Efi => "efi".to_string(),
        P::Other(v) => format!("0x{v:02x}"),
    }
}

fn media_type_token(m: &opticaldiscs::BootMediaType) -> String {
    use opticaldiscs::BootMediaType as M;
    match m {
        M::NoEmulation => "no_emulation".to_string(),
        M::Floppy1_2M => "floppy_1.2m".to_string(),
        M::Floppy1_44M => "floppy_1.44m".to_string(),
        M::Floppy2_88M => "floppy_2.88m".to_string(),
        M::HardDisk => "hard_disk".to_string(),
        M::Other(v) => format!("0x{v:02x}"),
    }
}

/// Detect the filesystem *inside* a boot image, using rusty-backup's own disk
/// engine — a floppy-emulation image is a FAT superfloppy; an HDD-emulation
/// image carries a partition table. Returns a short token / summary.
fn detect_boot_image_fs(bytes: &[u8]) -> Option<String> {
    use crate::partition::PartitionTable;
    let mut cur = std::io::Cursor::new(bytes);
    let pt = PartitionTable::detect(&mut cur).ok()?;
    match &pt {
        PartitionTable::None { fs_hint, .. } => Some(fs_hint.to_ascii_lowercase()),
        _ => {
            let types: Vec<String> = pt
                .partitions()
                .iter()
                .map(|p| p.type_name.clone())
                .filter(|t| !t.is_empty())
                .collect();
            Some(if types.is_empty() {
                pt.type_name().to_string()
            } else {
                types.join(", ")
            })
        }
    }
}

/// Build the `info` El Torito section from the crate's parsed boot catalog.
/// The crate owns the catalog; we (the disk-image layer) hash each boot image
/// and interpret its nested filesystem.
fn build_el_torito_info(
    catalog: Option<&opticaldiscs::ElTorito>,
    reader: &mut Option<Box<dyn opticaldiscs::SectorReader>>,
) -> Option<ElToritoInfo> {
    let catalog = catalog?;
    let mut entries = Vec::with_capacity(catalog.entries.len());
    for e in &catalog.entries {
        // Read the boot image blob once; hash it and detect its nested FS.
        let (sha256, fs) = match reader.as_deref_mut() {
            Some(r) => match opticaldiscs::read_boot_image(r, e) {
                Ok(bytes) if !bytes.is_empty() => (
                    Some(hex_lower(&Sha256::digest(&bytes))),
                    detect_boot_image_fs(&bytes),
                ),
                _ => (None, None),
            },
            None => (None, None),
        };
        entries.push(BootEntryInfo {
            platform: Some(platform_token(&e.platform)),
            bootable: e.bootable,
            media_type: media_type_token(&e.media_type),
            load_rba: e.load_rba,
            boot_image_size_bytes: e.image_size,
            boot_image_filesystem: fs,
            boot_image_sha256: sha256,
            id: e.id.clone(),
        });
    }
    Some(ElToritoInfo {
        present: true,
        entries,
    })
}

fn mac_date_string(mac_secs: u32) -> Option<String> {
    if mac_secs == 0 {
        return None;
    }
    let unix = mac_secs as i64 - 2_082_844_800;
    chrono::DateTime::from_timestamp(unix, 0)
        .map(|dt| dt.naive_utc().format("%Y-%m-%dT%H:%M:%S").to_string())
}

fn build_hfs_info(
    info: &opticaldiscs::detect::DiscImageInfo,
    partition_table: String,
    ddr_present: bool,
) -> Option<HfsInfo> {
    if let Some(mdb) = &info.hfs_mdb {
        return Some(HfsInfo {
            volume_name: mdb.volume_name.clone(),
            created: mac_date_string(mdb.creation_date),
            partition_table,
            ddr_present,
        });
    }
    if let Some(h) = &info.hfsplus_header {
        return Some(HfsInfo {
            volume_name: info.volume_label.clone().unwrap_or_default(),
            created: mac_date_string(h.create_date),
            partition_table,
            ddr_present,
        });
    }
    None
}

fn emit_info(format: OutputFormat, payload: InfoPayload) -> Result<()> {
    if format.is_structured() {
        return emit_envelope(format, &Envelope::ok(payload));
    }
    // Human text.
    println!("Image:       {}", payload.image);
    println!("Size:        {}", format_size(payload.size_bytes));
    if let Some(cue) = payload.cue_size_bytes {
        println!("Cue:         {}", format_size(cue));
    }
    println!("Container:   {}", payload.container);
    println!(
        "Filesystems: {}",
        if payload.filesystems.is_empty() {
            "(none recognized)".to_string()
        } else {
            payload.filesystems.join(", ")
        }
    );
    if let Some(iso) = &payload.iso9660 {
        println!("ISO 9660:");
        println!("  Volume id:   {}", iso.volume_id);
        if let Some(p) = &iso.preparer_id {
            println!("  Preparer:    {p}");
        }
        if let Some(d) = &iso.volume_creation_date {
            println!("  Created:     {d}");
        }
        println!(
            "  Space size:  {} blocks x {} B",
            iso.volume_space_size, iso.logical_block_size
        );
        println!(
            "  Extensions:  rock_ridge={} joliet={} udf={}",
            iso.has_rock_ridge, iso.has_joliet, iso.has_udf
        );
    }
    if let Some(hfs) = &payload.hfs {
        println!("HFS:");
        println!("  Volume:      {}", hfs.volume_name);
        println!(
            "  Partitions:  {} (ddr_present={})",
            hfs.partition_table, hfs.ddr_present
        );
    }
    if !payload.hybrid_filesystems.is_empty() {
        println!("Hybrid (Mac side):");
        for h in &payload.hybrid_filesystems {
            println!(
                "  {}  volume {:?}  @ offset {}",
                h.filesystem,
                h.volume_name.as_deref().unwrap_or("(unnamed)"),
                h.partition_offset
            );
        }
        println!("  browse with: optical browse --filesystem hfs <image>");
    }
    if let Some(et) = &payload.el_torito {
        println!("El Torito: {} boot image(s)", et.entries.len());
        for (i, e) in et.entries.iter().enumerate() {
            println!(
                "  [{i}] platform={} bootable={} media={} image={} fs={}",
                e.platform.as_deref().unwrap_or("?"),
                e.bootable,
                e.media_type,
                format_size(e.boot_image_size_bytes),
                e.boot_image_filesystem.as_deref().unwrap_or("?"),
            );
        }
    }
    for w in &payload.warnings {
        log_stderr(format!("warning: {w}"));
    }
    Ok(())
}

// ---------------- extract ----------------

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum CliResourceForkMode {
    /// Drop resource forks entirely; write data forks only.
    DataOnly,
    /// macOS native: write the resource fork to `<file>/..namedfork/rsrc`.
    Native,
    /// AppleDouble sidecar: `._<filename>` next to each file.
    Appledouble,
    /// Separate `<filename>.rsrc` file alongside the data fork.
    SeparateRsrc,
    /// MacBinary: single `<filename>.bin` containing data + resource fork.
    Macbinary,
}

impl From<CliResourceForkMode> for crate::fs::resource_fork::ResourceForkMode {
    fn from(m: CliResourceForkMode) -> Self {
        use crate::fs::resource_fork::ResourceForkMode as M;
        match m {
            CliResourceForkMode::DataOnly => M::DataForkOnly,
            CliResourceForkMode::Native => M::Native,
            CliResourceForkMode::Appledouble => M::AppleDouble,
            CliResourceForkMode::SeparateRsrc => M::SeparateRsrc,
            CliResourceForkMode::Macbinary => M::MacBinary,
        }
    }
}

#[derive(Debug, Args)]
pub struct ExtractArgs {
    /// Optical disc image (.iso, .cue, .chd).
    pub source: PathBuf,

    /// Destination folder (created if absent).
    #[arg(long)]
    pub to: PathBuf,

    /// How to handle HFS resource forks. Ignored on non-HFS discs.
    /// Defaults to `appledouble`, or `[optical] resource-forks` from
    /// the config file when set.
    #[arg(long = "resource-forks", value_enum)]
    pub resource_forks: Option<CliResourceForkMode>,

    /// What to do when two names on a **case-sensitive** disc (UFS, NeXT,
    /// Rock Ridge, …) collide only by case on a **case-insensitive**
    /// destination (e.g. macOS). Defaults to `rename`, or `[optical]
    /// on-collision` from the config. Ignored when the destination is
    /// case-sensitive — everything extracts verbatim there.
    #[arg(long = "on-collision", value_enum)]
    pub on_collision: Option<CliCaseCollisionMode>,

    /// Which filesystem to extract from on a hybrid Mac/PC disc. `auto`
    /// (default) uses the primary (ISO 9660); `hfs` extracts the Apple HFS
    /// side; `iso` forces the ISO 9660 tree. See `optical info`.
    #[arg(long = "filesystem", value_enum, default_value_t = FilesystemSelect::Auto)]
    pub filesystem: FilesystemSelect,
}

/// How to resolve case-insensitive filename collisions during extraction.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum CliCaseCollisionMode {
    /// Disambiguate by appending `~N` to later colliding names (nothing lost).
    Rename,
    /// Skip later colliding entries (log a warning).
    Skip,
    /// Treat a collision as a hard error (the old strict behaviour).
    Fail,
}

fn run_extract_verb(args: ExtractArgs) -> Result<()> {
    use opticaldiscs::detect::DiscImageInfo;

    std::fs::create_dir_all(&args.to).with_context(|| format!("creating {}", args.to.display()))?;

    let info = DiscImageInfo::open(&args.source)
        .with_context(|| format!("opening {}", args.source.display()))?;
    let (mut fs, _opened_fs) = open_selected_filesystem(&info, args.filesystem)?;
    let root = fs
        .root()
        .map_err(|e| anyhow::anyhow!("reading root: {e}"))?;

    let rf_mode = args
        .resource_forks
        .or_else(|| {
            crate::cli::logging::loaded_config()
                .and_then(|c| c.get("optical", "resource-forks"))
                .and_then(parse_resource_fork_mode)
        })
        .unwrap_or(CliResourceForkMode::Appledouble);
    log_stderr(format!(
        "rb-cli optical extract: {} -> {} (resource forks: {:?})",
        args.source.display(),
        args.to.display(),
        rf_mode
    ));

    let collision = args
        .on_collision
        .map(CaseCollisionMode::from)
        .or_else(|| {
            crate::cli::logging::loaded_config()
                .and_then(|c| c.get("optical", "on-collision"))
                .and_then(parse_collision_mode)
        })
        .unwrap_or(CaseCollisionMode::Rename);

    // Only disambiguate when the destination genuinely can't tell the names
    // apart; on a case-sensitive host everything extracts verbatim.
    let case_insensitive_dest = dest_is_case_insensitive(&args.to);

    let mut ctx = ExtractCtx {
        fork_mode: rf_mode.into(),
        collision,
        case_insensitive_dest,
        count: 0,
        skipped: 0,
        errors: 0,
    };

    let mut used = std::collections::HashSet::new();
    for child in fs
        .list_directory(&root)
        .map_err(|e| anyhow::anyhow!("list_directory: {e}"))?
    {
        extract(&mut *fs, &child, &args.to, &mut ctx, &mut used);
    }

    let mut summary = format!("extracted {} entry/entries", ctx.count);
    if ctx.skipped > 0 {
        summary.push_str(&format!(", skipped {}", ctx.skipped));
    }
    if ctx.errors > 0 {
        summary.push_str(&format!(", {} error(s)", ctx.errors));
    }
    log_stderr(summary);

    // Only a total failure (nothing extracted, at least one error) is fatal.
    if ctx.count == 0 && ctx.errors > 0 {
        anyhow::bail!(
            "extraction failed: {} error(s), 0 files written",
            ctx.errors
        );
    }
    Ok(())
}

/// How to resolve case-insensitive filename collisions (core enum).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CaseCollisionMode {
    Rename,
    Skip,
    Fail,
}

impl From<CliCaseCollisionMode> for CaseCollisionMode {
    fn from(m: CliCaseCollisionMode) -> Self {
        match m {
            CliCaseCollisionMode::Rename => CaseCollisionMode::Rename,
            CliCaseCollisionMode::Skip => CaseCollisionMode::Skip,
            CliCaseCollisionMode::Fail => CaseCollisionMode::Fail,
        }
    }
}

fn parse_collision_mode(s: &str) -> Option<CaseCollisionMode> {
    match s.trim().to_ascii_lowercase().as_str() {
        "rename" => Some(CaseCollisionMode::Rename),
        "skip" => Some(CaseCollisionMode::Skip),
        "fail" => Some(CaseCollisionMode::Fail),
        _ => None,
    }
}

/// Probe whether `dir` lives on a case-insensitive filesystem by creating a
/// mixed-case marker and checking if its lowercased path resolves to it.
fn dest_is_case_insensitive(dir: &Path) -> bool {
    let upper = dir.join(".rb_case_probe_Aa");
    if std::fs::File::create(&upper).is_err() {
        return false; // can't tell — assume case-sensitive (extract verbatim)
    }
    let lower = dir.join(".rb_case_probe_aa");
    let insensitive = lower.exists();
    let _ = std::fs::remove_file(&upper);
    insensitive
}

/// Mutable state threaded through the recursive extraction.
struct ExtractCtx {
    fork_mode: crate::fs::resource_fork::ResourceForkMode,
    collision: CaseCollisionMode,
    case_insensitive_dest: bool,
    count: u64,
    skipped: u64,
    errors: u64,
}

impl ExtractCtx {
    /// Resolve the on-disk name for `name` within a directory whose already-used
    /// (lowercased) names are in `used`. Returns `None` when the entry should be
    /// skipped. Records skip/error bookkeeping.
    fn resolve_name(
        &mut self,
        name: &str,
        used: &mut std::collections::HashSet<String>,
    ) -> Option<String> {
        if !self.case_insensitive_dest {
            return Some(name.to_string());
        }
        let key = name.to_lowercase();
        if !used.contains(&key) {
            used.insert(key);
            return Some(name.to_string());
        }
        // Collision on a case-insensitive destination.
        match self.collision {
            CaseCollisionMode::Fail => {
                log_stderr(format!(
                    "warning: name collision (case-insensitive destination): {name} - skipping (use --on-collision rename to keep both)"
                ));
                self.errors += 1;
                None
            }
            CaseCollisionMode::Skip => {
                log_stderr(format!("warning: skipping case-collision: {name}"));
                self.skipped += 1;
                None
            }
            CaseCollisionMode::Rename => {
                for n in 1..10_000u32 {
                    let candidate = format!("{name}~{n}");
                    let ckey = candidate.to_lowercase();
                    if !used.contains(&ckey) {
                        used.insert(ckey);
                        log_stderr(format!("info: case-collision: {name} -> {candidate}"));
                        return Some(candidate);
                    }
                }
                self.skipped += 1;
                None
            }
        }
    }
}

/// Extract one entry, recording success / skip / error into `ctx` and never
/// aborting the whole run on a single failure. `used` holds the lowercased names
/// already written into `dest` (for case-insensitive collision handling).
fn extract(
    fs: &mut dyn opticaldiscs::browse::filesystem::Filesystem,
    entry: &opticaldiscs::browse::entry::FileEntry,
    dest: &Path,
    ctx: &mut ExtractCtx,
    used: &mut std::collections::HashSet<String>,
) {
    if let Err(e) = extract_one(fs, entry, dest, ctx, used) {
        log_stderr(format!("warning: skipping {}: {e}", entry.path));
        ctx.errors += 1;
    }
}

fn extract_one(
    fs: &mut dyn opticaldiscs::browse::filesystem::Filesystem,
    entry: &opticaldiscs::browse::entry::FileEntry,
    dest: &Path,
    ctx: &mut ExtractCtx,
    used: &mut std::collections::HashSet<String>,
) -> Result<()> {
    use crate::fs::resource_fork::{self, ResourceForkMode as M};
    use opticaldiscs::browse::entry::EntryType;
    use std::io::{BufWriter, Write};

    let sanitized = resource_fork::sanitize_filename(&entry.name);
    let safe_name = match ctx.resolve_name(&sanitized, used) {
        Some(n) => n,
        None => return Ok(()), // skipped per collision policy
    };
    let mode = ctx.fork_mode;
    match entry.entry_type {
        EntryType::File => {
            // A non-zero resource-fork size only appears on fork-capable
            // filesystems (HFS / HFS+); ISO 9660 reports `None`. Keying off it
            // covers every current and future fork filesystem opticaldiscs
            // supports without re-checking the filesystem type.
            let has_rsrc = entry.resource_fork_size.map(|s| s > 0).unwrap_or(false);
            if has_rsrc && mode == M::MacBinary {
                let data = fs
                    .read_file(entry)
                    .map_err(|e| anyhow::anyhow!("read_file: {e}"))?;
                let rsrc = fs
                    .read_resource_fork(entry)
                    .map_err(|e| anyhow::anyhow!("read_resource_fork: {e}"))?
                    .unwrap_or_default();
                let type_code = entry.type_code.unwrap_or([0; 4]);
                let creator_code = entry.creator_code.unwrap_or([0; 4]);
                let dates = crate::optical::mac_dates_from(&entry.timestamps);
                let mb = resource_fork::build_macbinary(
                    &safe_name,
                    &type_code,
                    &creator_code,
                    dates,
                    &data,
                    &rsrc,
                );
                let out_path = dest.join(format!("{safe_name}.bin"));
                let mut f = BufWriter::new(std::fs::File::create(&out_path)?);
                f.write_all(&mb)?;
                f.flush()?;
            } else {
                let data = fs
                    .read_file(entry)
                    .map_err(|e| anyhow::anyhow!("read_file: {e}"))?;
                let out_path = dest.join(&safe_name);
                let mut f = BufWriter::new(std::fs::File::create(&out_path)?);
                f.write_all(&data)?;
                f.flush()?;
                if has_rsrc && mode != M::DataForkOnly {
                    let type_code = entry.type_code.unwrap_or([0; 4]);
                    let creator_code = entry.creator_code.unwrap_or([0; 4]);
                    let dates = crate::optical::mac_dates_from(&entry.timestamps);
                    let rsrc = fs
                        .read_resource_fork(entry)
                        .map_err(|e| anyhow::anyhow!("read_resource_fork: {e}"))?
                        .unwrap_or_default();
                    match mode {
                        M::Native => {
                            let rp = out_path.join("..namedfork/rsrc");
                            let mut rf = BufWriter::new(std::fs::File::create(&rp)?);
                            rf.write_all(&rsrc)?;
                            rf.flush()?;
                        }
                        M::AppleDouble => {
                            let ad = resource_fork::build_appledouble(
                                &type_code,
                                &creator_code,
                                dates,
                                &rsrc,
                            );
                            let ap = dest.join(format!("._{safe_name}"));
                            let mut af = BufWriter::new(std::fs::File::create(&ap)?);
                            af.write_all(&ad)?;
                            af.flush()?;
                        }
                        M::SeparateRsrc => {
                            let sp = dest.join(format!("{safe_name}.rsrc"));
                            let mut rf = BufWriter::new(std::fs::File::create(&sp)?);
                            rf.write_all(&rsrc)?;
                            rf.flush()?;
                        }
                        _ => {}
                    }
                }
            }
            ctx.count += 1;
        }
        EntryType::Directory => {
            let dir_path = dest.join(&safe_name);
            std::fs::create_dir_all(&dir_path)?;
            let children = fs
                .list_directory(entry)
                .map_err(|e| anyhow::anyhow!("list_directory: {e}"))?;
            // Each directory gets its own name-collision namespace.
            let mut child_used = std::collections::HashSet::new();
            for child in &children {
                extract(fs, child, &dir_path, ctx, &mut child_used);
            }
        }
    }
    Ok(())
}

fn parse_resource_fork_mode(s: &str) -> Option<CliResourceForkMode> {
    match s.to_ascii_lowercase().replace('-', "").as_str() {
        "dataonly" | "data" => Some(CliResourceForkMode::DataOnly),
        "native" => Some(CliResourceForkMode::Native),
        "appledouble" => Some(CliResourceForkMode::Appledouble),
        "separatersrc" | "separate" => Some(CliResourceForkMode::SeparateRsrc),
        "macbinary" => Some(CliResourceForkMode::Macbinary),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal FAT12 boot-floppy image — a valid-enough FAT BPB at sector 0
    /// in a 1.44 MB buffer, which is what an El Torito floppy-emulation boot
    /// image looks like once extracted.
    fn fat12_floppy() -> Vec<u8> {
        let mut data = vec![0u8; 1_474_560];
        data[0..3].copy_from_slice(&[0xEB, 0x3C, 0x90]);
        data[3..11].copy_from_slice(b"MSDOS5.0");
        data[11] = 0x00; // bytes/sector = 512
        data[12] = 0x02;
        data[13] = 1; // sectors/cluster
        data[14] = 1; // reserved sectors
        data[16] = 2; // number of FATs
        data[21] = 0xF0; // media descriptor (floppy)
        data[510] = 0x55;
        data[511] = 0xAA;
        data
    }

    #[test]
    fn detect_boot_image_fs_names_fat_floppy() {
        // rusty-backup interprets the boot image the crate hands back.
        let fs = detect_boot_image_fs(&fat12_floppy()).expect("should detect a filesystem");
        assert!(fs.contains("fat"), "expected a FAT token, got {fs:?}");
    }

    #[test]
    fn detect_boot_image_fs_none_on_garbage() {
        // An unrecognizable blob must not panic and must not claim a bogus FS.
        assert!(detect_boot_image_fs(&vec![0u8; 64 * 1024]).is_none());
    }

    #[test]
    fn el_torito_platform_and_media_tokens() {
        use opticaldiscs::{BootMediaType, Platform};
        assert_eq!(platform_token(&Platform::X86), "x86");
        assert_eq!(platform_token(&Platform::Efi), "efi");
        assert_eq!(platform_token(&Platform::Other(0x42)), "0x42");
        assert_eq!(
            media_type_token(&BootMediaType::Floppy1_44M),
            "floppy_1.44m"
        );
        assert_eq!(
            media_type_token(&BootMediaType::NoEmulation),
            "no_emulation"
        );
        assert_eq!(media_type_token(&BootMediaType::HardDisk), "hard_disk");
    }

    /// A synthetic bootable ISO whose single boot image is a 1.44 MB FAT12
    /// floppy (media type 2), pointed at boot image LBA 20.
    fn build_bootable_fat_iso() -> Vec<u8> {
        const SECTOR: usize = 2048;
        let image_lba: u32 = 20;
        let floppy = fat12_floppy(); // 1_474_560 bytes
        let total = image_lba as usize + floppy.len().div_ceil(SECTOR) + 1;
        let mut img = vec![0u8; total * SECTOR];

        // PVD @16 (root dir at LBA 18) — the crate helper builds an acceptable one.
        let pvd = opticaldiscs::iso9660::build_test_pvd_sector("BOOTFAT", 18, SECTOR as u32);
        img[16 * SECTOR..16 * SECTOR + pvd.len()].copy_from_slice(&pvd);

        // Boot Record VD @17 -> boot catalog at LBA 19.
        {
            let vd = &mut img[17 * SECTOR..18 * SECTOR];
            vd[0] = 0x00;
            vd[1..6].copy_from_slice(b"CD001");
            vd[6] = 1;
            vd[7..30].copy_from_slice(b"EL TORITO SPECIFICATION");
            vd[71..75].copy_from_slice(&19u32.to_le_bytes());
        }

        // Boot catalog @19: validation entry (x86, valid checksum) + a bootable
        // 1.44 MB floppy-emulation entry pointing at the boot image LBA.
        {
            let cat = 19 * SECTOR;
            let mut v = [0u8; 32];
            v[0] = 0x01;
            v[1] = 0x00; // x86
            v[30] = 0x55;
            v[31] = 0xAA;
            let mut sum: u16 = 0;
            for w in v.chunks_exact(2) {
                sum = sum.wrapping_add(u16::from_le_bytes([w[0], w[1]]));
            }
            v[28..30].copy_from_slice(&0u16.wrapping_sub(sum).to_le_bytes());
            img[cat..cat + 32].copy_from_slice(&v);

            let mut e = [0u8; 32];
            e[0] = 0x88; // bootable
            e[1] = 0x02; // media type 2 = 1.44 MB floppy
            e[8..12].copy_from_slice(&image_lba.to_le_bytes());
            img[cat + 32..cat + 64].copy_from_slice(&e);
        }

        // The boot image: the FAT12 floppy.
        img[image_lba as usize * SECTOR..image_lba as usize * SECTOR + floppy.len()]
            .copy_from_slice(&floppy);
        img
    }

    fn write_temp_iso(bytes: &[u8]) -> tempfile::NamedTempFile {
        use std::io::Write;
        let mut f = tempfile::Builder::new().suffix(".iso").tempfile().unwrap();
        f.write_all(bytes).unwrap();
        f.flush().unwrap();
        f
    }

    /// End-to-end: `optical info`'s builder surfaces the crate-parsed entry and —
    /// per the disc-layer / disk-image-layer boundary — detects the *nested* FAT
    /// filesystem with rusty-backup's own engine.
    #[test]
    fn info_reports_el_torito_with_nested_fat_fs() {
        let f = write_temp_iso(&build_bootable_fat_iso());
        let info = opticaldiscs::detect::DiscImageInfo::open(f.path()).expect("open bootable ISO");
        let mut reader = open_sector_reader(&info);
        let et = build_el_torito_info(info.el_torito.as_ref(), &mut reader).expect("el_torito");
        assert!(et.present);
        assert_eq!(et.entries.len(), 1);
        let e0 = &et.entries[0];
        assert_eq!(e0.platform.as_deref(), Some("x86"));
        assert!(e0.bootable);
        assert_eq!(e0.media_type, "floppy_1.44m");
        assert_eq!(e0.boot_image_size_bytes, 1_474_560);
        assert!(
            e0.boot_image_filesystem
                .as_deref()
                .is_some_and(|fs| fs.contains("fat")),
            "nested FS should be FAT, got {:?}",
            e0.boot_image_filesystem
        );
        assert!(e0.boot_image_sha256.is_some());
    }

    /// End-to-end: extract a boot image (as `optical boot extract` does), edit a
    /// byte, and put it back same-size in place (as `optical boot replace` does),
    /// then confirm the new bytes re-read through the catalog.
    #[test]
    fn boot_extract_replace_round_trips() {
        use opticaldiscs::el_torito_edit::ElToritoEditor;
        use opticaldiscs::BootMediaType;

        let f = write_temp_iso(&build_bootable_fat_iso());

        // Extract.
        let info = opticaldiscs::detect::DiscImageInfo::open(f.path()).expect("open");
        let entry = info.el_torito.as_ref().unwrap().entries[0].clone();
        let mut reader = open_sector_reader(&info).unwrap();
        let extracted = opticaldiscs::read_boot_image(&mut *reader, &entry).expect("read image");
        assert_eq!(extracted.len(), 1_474_560);
        drop(reader);

        // Edit a byte (same size) and replace in place.
        let mut edited = extracted.clone();
        edited[600_000] ^= 0xFF;
        let mut editor = ElToritoEditor::open_path(f.path()).expect("open editor");
        editor
            .replace_image(0, &edited, BootMediaType::Floppy1_44M)
            .expect("replace");
        editor.commit().expect("commit");

        // Re-read through the catalog — the new bytes must be there.
        let info2 = opticaldiscs::detect::DiscImageInfo::open(f.path()).expect("reopen");
        let entry2 = info2.el_torito.as_ref().unwrap().entries[0].clone();
        let mut reader2 = open_sector_reader(&info2).unwrap();
        let after = opticaldiscs::read_boot_image(&mut *reader2, &entry2).expect("re-read");
        assert_eq!(after, edited);
        assert_ne!(after, extracted);
    }
}
