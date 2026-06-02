//! `rb-cli sit <SUBCOMMAND>` — read classic StuffIt archives.
//!
//! `sit list ARCHIVE` prints the entries; `sit extract ARCHIVE DEST` unpacks
//! them to the host, preserving both forks and Finder info via a chosen
//! container format (BinHex by default).
//!
//! ARCHIVE may be a raw `.sit`, a self-extracting `.sea`, or a BinHex-wrapped
//! `.sit.hqx` — the latter is decoded transparently. Only the classic StuffIt
//! format (`SIT!`) is supported; the newer StuffIt 5 (`.sitx`-era) format is a
//! separate container and is rejected with a clear message.

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand, ValueEnum};
use std::path::{Path, PathBuf};

use crate::cli::logging::log_stderr;
use crate::fs::binhex;
use crate::fs::resource_fork::{self, sanitize_filename};
use crate::macarchive::stuffit::{self, StuffItArchive};
use crate::macarchive::stuffit5;

#[derive(Debug, Subcommand)]
pub enum SitCommand {
    /// List the entries in a StuffIt archive.
    List(ListArgs),
    /// Extract a StuffIt archive to a directory on the host.
    Extract(ExtractArgs),
    /// Create a StuffIt archive from host files (.hqx / .bin / plain).
    Create(CreateArgs),
}

#[derive(Debug, Args)]
pub struct CreateArgs {
    /// Output path. A `.sit` extension writes a raw archive; a `.hqx`
    /// extension BinHex-wraps it (the classic `.sit.hqx` format).
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
    /// StuffIt archive (`.sit`, `.sea`, or `.sit.hqx`).
    pub archive: PathBuf,
}

#[derive(Debug, Args)]
pub struct ExtractArgs {
    /// StuffIt archive (`.sit`, `.sea`, or `.sit.hqx`).
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

pub fn run(cmd: SitCommand) -> Result<()> {
    match cmd {
        SitCommand::List(args) => run_list(args),
        SitCommand::Extract(args) => run_extract(args),
        SitCommand::Create(args) => run_create(args),
    }
}

/// Load an archive, transparently BinHex-decoding a `.sit.hqx` wrapper and
/// routing classic StuffIt (`SIT!`), StuffIt 5, and `.sea` to the right parser.
fn load_archive(path: &Path) -> Result<(Vec<u8>, StuffItArchive)> {
    let raw = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    // If it's a BinHex document, the StuffIt stream is its data fork.
    let bytes = match binhex::parse_binhex(&raw) {
        Ok(bh) => bh.data_fork,
        Err(_) => raw,
    };
    let archive = if stuffit5::is_stuffit5(&bytes) {
        stuffit5::parse(&bytes)?
    } else if stuffit::find_sea_archive(&bytes).is_some() {
        stuffit::parse(&bytes)?
    } else {
        bail!(
            "{}: not a recognized StuffIt archive (classic SIT! / StuffIt 5; .sitx is not supported)",
            path.display()
        );
    };
    Ok((bytes, archive))
}

fn run_list(args: ListArgs) -> Result<()> {
    let (_, archive) = load_archive(&args.archive)?;
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
    let (bytes, archive) = load_archive(&args.archive)?;
    std::fs::create_dir_all(&args.dest)
        .with_context(|| format!("creating {}", args.dest.display()))?;

    let mut files = 0u32;
    let mut skipped = 0u32;
    for e in &archive.entries {
        // Build the host-side relative path from sanitized components.
        let mut rel = PathBuf::new();
        for comp in &e.path {
            rel.push(sanitize_filename(comp));
        }
        let target = args.dest.join(&rel);

        if e.is_dir {
            std::fs::create_dir_all(&target)?;
            continue;
        }
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let data = match e.data.as_ref() {
            Some(f) if f.uncompressed_len > 0 => match stuffit::decompress_fork(&bytes, f) {
                Ok(d) => d,
                Err(err) => {
                    log_stderr(format!("Skipped {}: {err}", e.display_path()));
                    skipped += 1;
                    continue;
                }
            },
            _ => Vec::new(),
        };
        let rsrc = match e.rsrc.as_ref() {
            Some(f) if f.uncompressed_len > 0 => match stuffit::decompress_fork(&bytes, f) {
                Ok(d) => d,
                Err(err) => {
                    log_stderr(format!("Skipped {}: {err}", e.display_path()));
                    skipped += 1;
                    continue;
                }
            },
            _ => Vec::new(),
        };

        write_entry(
            &target,
            &e.name,
            e.type_code,
            e.creator_code,
            e.finder_flags,
            &data,
            &rsrc,
            args.format,
        )?;
        files += 1;
    }

    log_stderr(format!(
        "sit extract: {files} files extracted to {}{}",
        args.dest.display(),
        if skipped > 0 {
            format!(" ({skipped} skipped)")
        } else {
            String::new()
        }
    ));
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn write_entry(
    target: &Path,
    mac_name: &str,
    type_code: [u8; 4],
    creator_code: [u8; 4],
    flags: u16,
    data: &[u8],
    rsrc: &[u8],
    format: ForkFormat,
) -> Result<()> {
    match format {
        ForkFormat::BinHex => {
            let bh = binhex::BinHexFile {
                name: mac_name.to_string(),
                type_code,
                creator_code,
                flags,
                data_fork: data.to_vec(),
                resource_fork: rsrc.to_vec(),
            };
            let path = with_added_extension(target, "hqx");
            std::fs::write(&path, binhex::build_binhex(&bh).as_bytes())?;
        }
        ForkFormat::MacBinary => {
            let mb =
                resource_fork::build_macbinary(mac_name, &type_code, &creator_code, data, rsrc);
            let path = with_added_extension(target, "bin");
            std::fs::write(&path, mb)?;
        }
        ForkFormat::AppleDouble => {
            std::fs::write(target, data)?;
            if !rsrc.is_empty() || type_code != [0; 4] || creator_code != [0; 4] {
                let ad = resource_fork::build_appledouble(&type_code, &creator_code, rsrc);
                let ad_path = sidecar_path(target, "._");
                std::fs::write(ad_path, ad)?;
            }
        }
        ForkFormat::Raw => {
            std::fs::write(target, data)?;
            if !rsrc.is_empty() {
                let path = with_added_extension(target, "rsrc");
                std::fs::write(path, rsrc)?;
            }
        }
    }
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
        "sit create: {} file(s) -> {} ({} format, {} bytes)",
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

/// Append `.ext` to a path (keeping any existing extension).
fn with_added_extension(path: &Path, ext: &str) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".");
    name.push(ext);
    path.with_file_name(name)
}

/// Build a `._name` AppleDouble sidecar path next to `target`.
fn sidecar_path(target: &Path, prefix: &str) -> PathBuf {
    let name = target.file_name().unwrap_or_default().to_string_lossy();
    target.with_file_name(format!("{prefix}{name}"))
}
