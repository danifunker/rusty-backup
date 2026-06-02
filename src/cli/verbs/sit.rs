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

#[derive(Debug, Subcommand)]
pub enum SitCommand {
    /// List the entries in a StuffIt archive.
    List(ListArgs),
    /// Extract a StuffIt archive to a directory on the host.
    Extract(ExtractArgs),
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
    }
}

/// Load an archive, transparently BinHex-decoding a `.sit.hqx` wrapper.
fn load_archive(path: &Path) -> Result<(Vec<u8>, StuffItArchive)> {
    let raw = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    // If it's a BinHex document, the StuffIt stream is its data fork.
    let bytes = match binhex::parse_binhex(&raw) {
        Ok(bh) => bh.data_fork,
        Err(_) => raw,
    };
    if stuffit::find_sea_archive(&bytes).is_none() {
        bail!(
            "{}: not a classic StuffIt archive (newer StuffIt 5 / .sitx is not supported)",
            path.display()
        );
    }
    let archive = stuffit::parse(&bytes)?;
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
