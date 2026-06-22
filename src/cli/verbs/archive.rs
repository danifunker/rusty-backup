//! `rb-cli archive <SUBCOMMAND>` — read/write classic Mac archives (StuffIt,
//! Compact Pro, MAR, BinHex). (`sit` is kept as a hidden back-compat alias.)
//!
//! `archive list ARCHIVE` prints the entries; `archive extract ARCHIVE DEST`
//! unpacks them to the host, preserving both forks and Finder info via a chosen
//! container format (BinHex by default).
//!
//! ARCHIVE may be a raw `.sit` / `.cpt` / `.mar`, a self-extracting `.sea`, or
//! any of the StuffIt/Compact-Pro forms BinHex-wrapped as `.hqx` — the wrapper
//! is decoded transparently, and the format (classic StuffIt, StuffIt 5,
//! Compact Pro, or MAR) is detected from content. StuffIt X (`.sitx`) is
//! recognized but not yet extractable; LZW/LZ4-compressed MAR isn't yet either.

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand, ValueEnum};
use std::path::{Path, PathBuf};

use crate::cli::logging::log_stderr;
use crate::fs::binhex;
use crate::fs::resource_fork;
use crate::macarchive::extract;
use crate::macarchive::mar;
use crate::macarchive::stuffit;

#[derive(Debug, Subcommand)]
pub enum ArchiveCommand {
    /// List the entries in a StuffIt archive.
    List(ListArgs),
    /// Extract a StuffIt archive to a directory on the host.
    Extract(ExtractArgs),
    /// Create a StuffIt or MAR archive from host files (.hqx / .bin / plain).
    Create(CreateArgs),
}

#[derive(Debug, Args)]
pub struct CreateArgs {
    /// Output path. `.sit` writes a raw StuffIt archive; `.hqx` BinHex-wraps it
    /// (the classic `.sit.hqx` format); `.mar` writes a stored MAR archive
    /// (a single file, or several wrapped in a folder named after the output).
    pub output: PathBuf,

    /// Input files. Each may be a BinHex `.hqx`, a MacBinary `.bin`, or a
    /// plain file (with an optional `._name` / `.rsrc` sidecar).
    #[arg(required = true)]
    pub inputs: Vec<PathBuf>,

    /// Compress forks with RLE90 (method 1) instead of storing uncompressed.
    #[arg(long)]
    pub rle: bool,
}

#[derive(Debug, Args)]
pub struct ListArgs {
    /// StuffIt, Compact Pro, or MAR archive (`.sit`, `.sea`, `.cpt`, `.mar`, or `.hqx`).
    pub archive: PathBuf,
}

#[derive(Debug, Args)]
pub struct ExtractArgs {
    /// StuffIt, Compact Pro, or MAR archive (`.sit`, `.sea`, `.cpt`, `.mar`, or `.hqx`).
    pub archive: PathBuf,

    /// Destination directory on the host (created if missing).
    pub dest: PathBuf,

    /// Container format for the extracted files.
    #[arg(long, value_enum, default_value_t = ForkFormat::BinHex)]
    pub format: ForkFormat,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ForkFormat {
    /// One `.hqx` per file (both forks + Finder info).
    #[value(name = "binhex")]
    BinHex,
    /// One `.bin` MacBinary III per file.
    #[value(name = "macbinary")]
    MacBinary,
    /// Data fork + `._name` AppleDouble sidecar.
    #[value(name = "appledouble")]
    AppleDouble,
    /// Data fork + `name.rsrc` sidecar (resource fork only).
    #[value(name = "raw")]
    Raw,
}

impl ForkFormat {
    fn to_core(self) -> extract::ForkFormat {
        match self {
            ForkFormat::BinHex => extract::ForkFormat::BinHex,
            ForkFormat::MacBinary => extract::ForkFormat::MacBinary,
            ForkFormat::AppleDouble => extract::ForkFormat::AppleDouble,
            ForkFormat::Raw => extract::ForkFormat::Raw,
        }
    }
}

pub fn run(cmd: ArchiveCommand) -> Result<()> {
    match cmd {
        ArchiveCommand::List(args) => run_list(args),
        ArchiveCommand::Extract(args) => run_extract(args),
        ArchiveCommand::Create(args) => run_create(args),
    }
}

fn run_list(args: ListArgs) -> Result<()> {
    let (_, archive) =
        extract::open(&args.archive).with_context(|| args.archive.display().to_string())?;
    for e in &archive.entries {
        if e.is_dir {
            println!("DIR   {}/", e.display_path());
            continue;
        }
        let data_desc = e
            .data
            .as_ref()
            .filter(|f| f.uncompressed_len > 0)
            .map(|f| format!("data {} ({})", f.uncompressed_len, f.method_name()))
            .unwrap_or_default();
        let rsrc_desc = e
            .rsrc
            .as_ref()
            .filter(|f| f.uncompressed_len > 0)
            .map(|f| format!("rsrc {} ({})", f.uncompressed_len, f.method_name()))
            .unwrap_or_default();
        let type_str = String::from_utf8_lossy(&e.type_code);
        let creator_str = String::from_utf8_lossy(&e.creator_code);
        println!(
            "FILE  {:<40} {:>4} {:>4}  {}  {}",
            e.display_path(),
            type_str,
            creator_str,
            data_desc,
            rsrc_desc,
        );
    }
    Ok(())
}

fn run_extract(args: ExtractArgs) -> Result<()> {
    let (bytes, archive) =
        extract::open(&args.archive).with_context(|| args.archive.display().to_string())?;

    let stats = extract::extract_all(
        &bytes,
        &archive,
        &args.dest,
        args.format.to_core(),
        |_done, _total, _name| {},
        log_stderr,
    )?;

    log_stderr(format!(
        "archive extract: {} files extracted to {}{}",
        stats.files,
        args.dest.display(),
        if stats.skipped > 0 {
            format!(" ({} skipped)", stats.skipped)
        } else {
            String::new()
        }
    ));
    Ok(())
}

fn run_create(args: CreateArgs) -> Result<()> {
    let method = if args.rle {
        stuffit::WriteMethod::Rle
    } else {
        stuffit::WriteMethod::Store
    };

    let mut files = Vec::new();
    for input in &args.inputs {
        // Skip AppleDouble / .rsrc sidecars — they're consumed with their
        // primary file.
        if resource_fork::is_resource_fork_sidecar(input) {
            continue;
        }
        files.push(host_file_to_input(input)?);
    }
    if files.is_empty() {
        bail!("no input files to archive");
    }

    // A `.mar` output writes a MAR archive instead of StuffIt. MAR holds a
    // single root (file or folder), so multiple inputs are wrapped in a folder
    // named after the output; MAR is always stored (no `--rle`).
    let out_ext = args
        .output
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase());
    if out_ext.as_deref() == Some("mar") {
        let root_name = args
            .output
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("archive");
        let nodes: Vec<stuffit::StuffItInputNode> = files
            .iter()
            .cloned()
            .map(stuffit::StuffItInputNode::File)
            .collect();
        let bytes = mar::build_archive(root_name, &nodes)?;
        std::fs::write(&args.output, &bytes)
            .with_context(|| format!("writing {}", args.output.display()))?;
        log_stderr(format!(
            "archive create: {} file(s) -> {} (mar format, {} bytes)",
            files.len(),
            args.output.display(),
            bytes.len()
        ));
        return Ok(());
    }

    let sit_bytes = stuffit::build_archive(&files, method)?;

    // If the output ends in `.hqx`, BinHex-wrap the archive (the classic
    // `.sit.hqx` distribution format).
    let wrap_hqx = args
        .output
        .extension()
        .map(|e| e.eq_ignore_ascii_case("hqx"))
        .unwrap_or(false);

    let (out_bytes, kind) = if wrap_hqx {
        let bh = binhex::BinHexFile {
            name: inner_sit_name(&args.output),
            type_code: *b"SITD",
            creator_code: *b"SIT!",
            flags: 0,
            data_fork: sit_bytes,
            resource_fork: Vec::new(),
        };
        (binhex::build_binhex(&bh).into_bytes(), "sit.hqx")
    } else {
        (sit_bytes, "sit")
    };

    std::fs::write(&args.output, &out_bytes)
        .with_context(|| format!("writing {}", args.output.display()))?;

    log_stderr(format!(
        "archive create: {} file(s) -> {} ({} format, {} bytes)",
        files.len(),
        args.output.display(),
        kind,
        out_bytes.len()
    ));
    Ok(())
}

/// Derive the inner StuffIt filename for a `.sit.hqx` wrapper from the output
/// path, ensuring it ends in `.sit`.
fn inner_sit_name(output: &Path) -> String {
    // Strip the trailing `.hqx`, then ensure a `.sit` suffix.
    let stem = output
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("archive");
    if stem.to_ascii_lowercase().ends_with(".sit") {
        stem.to_string()
    } else {
        format!("{stem}.sit")
    }
}

/// Build a [`stuffit::StuffItInput`] from a host file, recognizing BinHex and
/// MacBinary containers and plain files (with optional fork sidecars).
fn host_file_to_input(path: &Path) -> Result<stuffit::StuffItInput> {
    let raw = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;

    // BinHex: full fidelity (name + both forks + Finder info).
    if let Ok(bh) = binhex::parse_binhex(&raw) {
        return Ok(stuffit::StuffItInput {
            name: bh.name,
            type_code: bh.type_code,
            creator_code: bh.creator_code,
            finder_flags: bh.flags,
            create_date: 0,
            mod_date: 0,
            data_fork: bh.data_fork,
            resource_fork: bh.resource_fork,
        });
    }

    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("untitled")
        .to_string();
    let full_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("untitled")
        .to_string();

    // MacBinary container.
    if let Some(mb) = resource_fork::parse_macbinary(&raw) {
        return Ok(stuffit::StuffItInput {
            name: stem,
            type_code: mb.type_code.unwrap_or(*b"????"),
            creator_code: mb.creator_code.unwrap_or(*b"????"),
            finder_flags: 0,
            create_date: 0,
            mod_date: 0,
            data_fork: mb.data_fork.unwrap_or_default(),
            resource_fork: mb.data,
        });
    }

    // Plain file, picking up an AppleDouble / .rsrc sidecar if present.
    let detected = resource_fork::detect_resource_fork(path);
    let (rsrc, type_code, creator_code) = detected
        .map(|d| {
            (
                d.data,
                d.type_code.unwrap_or(*b"????"),
                d.creator_code.unwrap_or(*b"????"),
            )
        })
        .unwrap_or_else(|| (Vec::new(), *b"????", *b"????"));

    Ok(stuffit::StuffItInput {
        name: full_name,
        type_code,
        creator_code,
        finder_flags: 0,
        create_date: 0,
        mod_date: 0,
        data_fork: raw,
        resource_fork: rsrc,
    })
}
