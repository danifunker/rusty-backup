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

use crate::backup::LogMessage;
use crate::cli::logging::log_stderr;
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
    /// Extract files from an optical disc image into a host folder.
    Extract(ExtractArgs),
}

pub fn run(cmd: OpticalCommand) -> Result<()> {
    match cmd {
        OpticalCommand::Drives(a) => run_drives_verb(a),
        OpticalCommand::Rip(a) => run_rip_verb(a),
        OpticalCommand::Convert(a) => run_convert_verb(a),
        OpticalCommand::Browse(a) => run_browse_verb(a),
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

#[derive(Debug, Args)]
pub struct BrowseArgs {
    /// Optical disc image (.iso, .cue, .chd).
    pub source: PathBuf,
}

fn run_browse_verb(args: BrowseArgs) -> Result<()> {
    use opticaldiscs::browse::open_disc_filesystem;
    use opticaldiscs::detect::DiscImageInfo;

    let info = DiscImageInfo::open(&args.source)
        .with_context(|| format!("opening {}", args.source.display()))?;
    let mut fs =
        open_disc_filesystem(&info).map_err(|e| anyhow::anyhow!("opening disc filesystem: {e}"))?;
    let root = fs
        .root()
        .map_err(|e| anyhow::anyhow!("reading root: {e}"))?;
    let label = fs.volume_name().unwrap_or("/").to_owned();

    let mut out = String::new();
    out.push_str(&label);
    out.push('\n');
    let mut dirs = 0u64;
    let mut files = 0u64;
    walk_tree(&mut *fs, &root, "", &mut out, &mut dirs, &mut files)?;
    out.push_str(&format!("\n{dirs} directories, {files} files\n"));
    print!("{out}");
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
            if let Some(tc) = &child.type_code {
                out.push_str(&format!("  {tc}"));
                if let Some(cc) = &child.creator_code {
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
}

fn run_extract_verb(args: ExtractArgs) -> Result<()> {
    use opticaldiscs::browse::open_disc_filesystem;
    use opticaldiscs::detect::DiscImageInfo;
    use opticaldiscs::formats::FilesystemType;

    std::fs::create_dir_all(&args.to).with_context(|| format!("creating {}", args.to.display()))?;

    let info = DiscImageInfo::open(&args.source)
        .with_context(|| format!("opening {}", args.source.display()))?;
    let is_hfs = matches!(info.filesystem, FilesystemType::Hfs);
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

    let mode = rf_mode.into();
    let mut count = 0u64;
    for child in fs
        .list_directory(&root)
        .map_err(|e| anyhow::anyhow!("list_directory: {e}"))?
    {
        extract(&mut *fs, &child, &args.to, mode, is_hfs, &mut count)?;
    }
    log_stderr(format!("extracted {count} entry/entries"));
    Ok(())
}

fn extract(
    fs: &mut dyn opticaldiscs::browse::filesystem::Filesystem,
    entry: &opticaldiscs::browse::entry::FileEntry,
    dest: &Path,
    mode: crate::fs::resource_fork::ResourceForkMode,
    is_hfs: bool,
    count: &mut u64,
) -> Result<()> {
    use crate::fs::resource_fork::{self, ResourceForkMode as M};
    use opticaldiscs::browse::entry::EntryType;
    use std::io::{BufWriter, Write};

    let safe_name = resource_fork::sanitize_filename(&entry.name);
    match entry.entry_type {
        EntryType::File => {
            let has_rsrc = is_hfs && entry.resource_fork_size.map(|s| s > 0).unwrap_or(false);
            if has_rsrc && mode == M::MacBinary {
                let data = fs
                    .read_file(entry)
                    .map_err(|e| anyhow::anyhow!("read_file: {e}"))?;
                let rsrc = fs
                    .read_resource_fork(entry)
                    .map_err(|e| anyhow::anyhow!("read_resource_fork: {e}"))?
                    .unwrap_or_default();
                let type_code = fourcc(entry.type_code.as_deref());
                let creator_code = fourcc(entry.creator_code.as_deref());
                let mb = resource_fork::build_macbinary(
                    &safe_name,
                    &type_code,
                    &creator_code,
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
                    let type_code = fourcc(entry.type_code.as_deref());
                    let creator_code = fourcc(entry.creator_code.as_deref());
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
                            let ad =
                                resource_fork::build_appledouble(&type_code, &creator_code, &rsrc);
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
            *count += 1;
        }
        EntryType::Directory => {
            let dir_path = dest.join(&safe_name);
            std::fs::create_dir_all(&dir_path)?;
            let children = fs
                .list_directory(entry)
                .map_err(|e| anyhow::anyhow!("list_directory: {e}"))?;
            for child in &children {
                extract(fs, child, &dir_path, mode, is_hfs, count)?;
            }
        }
    }
    Ok(())
}

fn fourcc(s: Option<&str>) -> [u8; 4] {
    let mut out = [0u8; 4];
    if let Some(code) = s {
        let bytes = code.as_bytes();
        let n = bytes.len().min(4);
        out[..n].copy_from_slice(&bytes[..n]);
    }
    out
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
