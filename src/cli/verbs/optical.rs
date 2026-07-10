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
    /// Print volume-level metadata for an optical disc image (leniently).
    Info(InfoArgs),
    /// Extract files from an optical disc image into a host folder.
    Extract(ExtractArgs),
}

pub fn run(cmd: OpticalCommand) -> Result<()> {
    match cmd {
        OpticalCommand::Drives(a) => run_drives_verb(a),
        OpticalCommand::Rip(a) => run_rip_verb(a),
        OpticalCommand::Convert(a) => run_convert_verb(a),
        OpticalCommand::Browse(a) => run_browse_verb(a),
        OpticalCommand::Info(a) => run_info_verb(a),
        OpticalCommand::Extract(a) => run_extract_verb(a),
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
}

fn run_browse_verb(args: BrowseArgs) -> Result<()> {
    use opticaldiscs::browse::open_disc_filesystem;
    use opticaldiscs::detect::DiscImageInfo;

    require_non_flat(args.format, "optical browse")?;
    if args.hash.is_some() && !args.format.is_structured() {
        bail!(
            "--hash requires --format json (per-file hashes are only emitted in structured output)"
        );
    }

    let info = DiscImageInfo::open(&args.source)
        .with_context(|| format!("opening {}", args.source.display()))?;
    let mut fs =
        open_disc_filesystem(&info).map_err(|e| anyhow::anyhow!("opening disc filesystem: {e}"))?;
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
            filesystem: fs_token(info.filesystem),
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

#[derive(Debug, Serialize)]
struct ElToritoInfo {
    present: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    platform: Option<String>,
    bootable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    boot_image_size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    boot_image_sha256: Option<String>,
}

#[derive(Debug, Serialize)]
struct InfoPayload {
    image: String,
    size_bytes: u64,
    container: String,
    filesystems: Vec<String>,
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
                    filesystems: Vec::new(),
                    warnings: vec![format!("unrecognized disc image: {e}")],
                    iso9660: None,
                    hfs: None,
                    el_torito: None,
                },
            );
        }
    };

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
    let mut el_torito = None;
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
        el_torito = parse_el_torito(r);
    }

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
            filesystems,
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

/// Parse the El Torito boot catalog (Boot Record VD at sector 17 → catalog →
/// initial/default entry). Returns `None` when the disc isn't El Torito.
fn parse_el_torito(reader: &mut dyn opticaldiscs::SectorReader) -> Option<ElToritoInfo> {
    let br = reader.read_sector(17).ok()?;
    if br.len() < 2048 || br[0] != 0x00 || &br[1..6] != b"CD001" {
        return None;
    }
    if &br[7..30] != b"EL TORITO SPECIFICATION" {
        return None;
    }
    let catalog_lba = u32::from_le_bytes([br[71], br[72], br[73], br[74]]) as u64;
    let cat = reader.read_sector(catalog_lba).ok()?;
    if cat.len() < 64 || cat[0] != 0x01 {
        return None; // validation entry header id
    }
    let platform = match cat[1] {
        0x00 => Some("x86"),
        0x01 => Some("ppc"),
        0x02 => Some("mac"),
        0xEF => Some("efi"),
        _ => None,
    }
    .map(str::to_string);

    // Initial / default entry at offset 32.
    let bootable = cat[32] == 0x88;
    let media = cat[33];
    let sector_count = u16::from_le_bytes([cat[38], cat[39]]) as u64;
    let load_rba = u32::from_le_bytes([cat[40], cat[41], cat[42], cat[43]]) as u64;

    // Floppy emulation encodes the size in the media type; no-emulation uses the
    // virtual-512-byte sector count.
    let size = match media {
        1 => 1_228_800,          // 1.2 MB
        2 => 1_474_560,          // 1.44 MB
        3 => 2_949_120,          // 2.88 MB
        _ => sector_count * 512, // no-emulation / hard-disk: best effort
    };

    // Hash the boot image extent when it's a sane size.
    let boot_image_sha256 = if size > 0 && size <= 128 * 1024 * 1024 {
        reader
            .read_bytes(load_rba * 2048, size as usize)
            .ok()
            .map(|b| hex_lower(&Sha256::digest(&b)))
    } else {
        None
    };

    Some(ElToritoInfo {
        present: true,
        platform,
        bootable,
        boot_image_size_bytes: (size > 0).then_some(size),
        boot_image_sha256,
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
    if let Some(et) = &payload.el_torito {
        println!("El Torito:");
        println!(
            "  bootable={} platform={} image={}",
            et.bootable,
            et.platform.as_deref().unwrap_or("?"),
            et.boot_image_size_bytes
                .map(format_size)
                .unwrap_or_else(|| "?".to_string())
        );
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
    use opticaldiscs::browse::open_disc_filesystem;
    use opticaldiscs::detect::DiscImageInfo;

    std::fs::create_dir_all(&args.to).with_context(|| format!("creating {}", args.to.display()))?;

    let info = DiscImageInfo::open(&args.source)
        .with_context(|| format!("opening {}", args.source.display()))?;
    let mut fs =
        open_disc_filesystem(&info).map_err(|e| anyhow::anyhow!("opening disc filesystem: {e}"))?;
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

    /// A minimal in-memory [`opticaldiscs::SectorReader`] over a fixed set of
    /// 2048-byte sectors, for exercising the El Torito parser.
    struct VecReader {
        sectors: Vec<Vec<u8>>,
    }
    impl opticaldiscs::SectorReader for VecReader {
        fn read_sector(&mut self, lba: u64) -> opticaldiscs::error::Result<Vec<u8>> {
            Ok(self
                .sectors
                .get(lba as usize)
                .cloned()
                .unwrap_or_else(|| vec![0u8; 2048]))
        }
    }

    #[test]
    fn parse_el_torito_no_emulation() {
        let mut sectors = vec![vec![0u8; 2048]; 20];
        // Boot Record Volume Descriptor at sector 17.
        {
            let br = &mut sectors[17];
            br[0] = 0x00;
            br[1..6].copy_from_slice(b"CD001");
            br[6] = 1;
            br[7..30].copy_from_slice(b"EL TORITO SPECIFICATION");
            br[71..75].copy_from_slice(&18u32.to_le_bytes()); // boot catalog at LBA 18
        }
        // Boot catalog at sector 18: validation entry + initial/default entry.
        {
            let cat = &mut sectors[18];
            cat[0] = 0x01; // validation entry header id
            cat[1] = 0x00; // platform 0x00 = x86
            cat[30] = 0x55;
            cat[31] = 0xAA;
            cat[32] = 0x88; // bootable
            cat[33] = 0x00; // media type 0 = no emulation
            cat[38..40].copy_from_slice(&4u16.to_le_bytes()); // 4 virtual 512-B sectors = 2048 B
            cat[40..44].copy_from_slice(&19u32.to_le_bytes()); // boot image at LBA 19
        }
        // The boot image itself.
        sectors[19] = vec![0xABu8; 2048];

        let mut r = VecReader { sectors };
        let et = parse_el_torito(&mut r).expect("El Torito must parse");
        assert!(et.present);
        assert!(et.bootable);
        assert_eq!(et.platform.as_deref(), Some("x86"));
        assert_eq!(et.boot_image_size_bytes, Some(2048));

        let expected = {
            let mut h = Sha256::new();
            h.update(vec![0xABu8; 2048]);
            hex_lower(&h.finalize())
        };
        assert_eq!(et.boot_image_sha256.as_deref(), Some(expected.as_str()));
    }

    #[test]
    fn parse_el_torito_floppy_media_size() {
        let mut sectors = vec![vec![0u8; 2048]; 20];
        sectors[17][0] = 0x00;
        sectors[17][1..6].copy_from_slice(b"CD001");
        sectors[17][7..30].copy_from_slice(b"EL TORITO SPECIFICATION");
        sectors[17][71..75].copy_from_slice(&18u32.to_le_bytes());
        sectors[18][0] = 0x01;
        sectors[18][1] = 0x01; // platform ppc
        sectors[18][32] = 0x88; // bootable
        sectors[18][33] = 0x02; // media type 2 = 1.44 MB floppy
        sectors[18][40..44].copy_from_slice(&19u32.to_le_bytes());

        let mut r = VecReader {
            sectors: {
                // A 1.44 MB image spans 720 cooked sectors starting at LBA 19.
                let mut s = sectors;
                s.resize(19 + 720, vec![0u8; 2048]);
                s
            },
        };
        let et = parse_el_torito(&mut r).expect("El Torito must parse");
        assert_eq!(et.platform.as_deref(), Some("ppc"));
        assert_eq!(et.boot_image_size_bytes, Some(1_474_560));
        assert!(et.boot_image_sha256.is_some());
    }

    #[test]
    fn parse_el_torito_absent_when_no_boot_record() {
        let mut r = VecReader {
            sectors: vec![vec![0u8; 2048]; 20],
        };
        assert!(parse_el_torito(&mut r).is_none());
    }
}
