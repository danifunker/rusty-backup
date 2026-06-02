//! Shared archive open + extraction logic used by both the CLI (`sit` verb)
//! and the GUI archive-browse tab.

use anyhow::{bail, Context, Result};
use std::path::{Path, PathBuf};

use crate::fs::binhex;
use crate::fs::resource_fork::{self, sanitize_filename};

use super::stuffit::{self, StuffItArchive, StuffItEntry};
use super::stuffit5;

/// Container format for an extracted file's forks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForkFormat {
    /// One `.hqx` per file (both forks + Finder info).
    BinHex,
    /// One `.bin` MacBinary III per file.
    MacBinary,
    /// Data fork + `._name` AppleDouble sidecar.
    AppleDouble,
    /// Data fork + `name.rsrc` sidecar (resource fork only).
    Raw,
}

impl ForkFormat {
    pub const ALL: [ForkFormat; 4] = [
        ForkFormat::BinHex,
        ForkFormat::MacBinary,
        ForkFormat::AppleDouble,
        ForkFormat::Raw,
    ];

    pub fn label(&self) -> &'static str {
        match self {
            ForkFormat::BinHex => "BinHex (.hqx)",
            ForkFormat::MacBinary => "MacBinary (.bin)",
            ForkFormat::AppleDouble => "AppleDouble (._name)",
            ForkFormat::Raw => "Raw + .rsrc",
        }
    }
}

/// Result of an extraction run.
#[derive(Debug, Default, Clone, Copy)]
pub struct ExtractStats {
    pub files: usize,
    pub skipped: usize,
}

/// Read an archive file and parse it, transparently BinHex-decoding a
/// `.sit.hqx` wrapper and routing classic StuffIt (`SIT!`), StuffIt 5, and
/// `.sea` to the right parser. Returns the decoded archive bytes (offsets in
/// the entries are relative to these) plus the parsed directory.
pub fn open(path: &Path) -> Result<(Vec<u8>, StuffItArchive)> {
    let raw = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    open_bytes(raw)
}

/// Like [`open`] but from in-memory bytes (already-read file contents).
pub fn open_bytes(raw: Vec<u8>) -> Result<(Vec<u8>, StuffItArchive)> {
    // If it's a BinHex document, the StuffIt stream is its data fork.
    let bytes = match binhex::parse_binhex(&raw) {
        Ok(bh) => bh.data_fork,
        Err(_) => raw,
    };
    let archive = if stuffit5::is_stuffit5(&bytes) {
        stuffit5::parse(&bytes)?
    } else if is_stuffitx(&bytes) {
        bail!(
            "StuffIt X (.sitx) recognized, but native extraction is not yet implemented \
             (its catalog + data streams use the Brimstone / PPMd-variant-G codec). \
             Use `unar` for now."
        );
    } else if stuffit::find_sea_archive(&bytes).is_some() {
        stuffit::parse(&bytes)?
    } else {
        bail!("not a recognized StuffIt archive (classic SIT! / StuffIt 5)");
    };
    Ok((bytes, archive))
}

/// Detect the StuffIt X container ("StuffIt!" / "StuffIt?"). Distinct from
/// StuffIt 5, whose 8th byte is a space ("StuffIt (c)1997…").
pub fn is_stuffitx(data: &[u8]) -> bool {
    data.len() >= 8 && &data[..7] == b"StuffIt" && (data[7] == b'!' || data[7] == b'?')
}

/// Extract every file in `archive` to `dest`, rebuilding the directory tree.
/// `progress(done, total, name)` is called before each file entry.
pub fn extract_all(
    bytes: &[u8],
    archive: &StuffItArchive,
    dest: &Path,
    format: ForkFormat,
    mut progress: impl FnMut(usize, usize, &str),
    mut log: impl FnMut(String),
) -> Result<ExtractStats> {
    std::fs::create_dir_all(dest).with_context(|| format!("creating {}", dest.display()))?;

    let total = archive.entries.iter().filter(|e| !e.is_dir).count();
    let mut stats = ExtractStats::default();
    let mut done = 0usize;

    for e in &archive.entries {
        let mut rel = PathBuf::new();
        for comp in &e.path {
            rel.push(sanitize_filename(comp));
        }
        let target = dest.join(&rel);

        if e.is_dir {
            std::fs::create_dir_all(&target)?;
            continue;
        }
        progress(done, total, &e.name);
        done += 1;
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let data = match decompress_named(bytes, e.data.as_ref(), e, &mut log) {
            Ok(d) => d,
            Err(()) => {
                stats.skipped += 1;
                continue;
            }
        };
        let rsrc = match decompress_named(bytes, e.rsrc.as_ref(), e, &mut log) {
            Ok(d) => d,
            Err(()) => {
                stats.skipped += 1;
                continue;
            }
        };

        write_entry(&target, e, &data, &rsrc, format)?;
        stats.files += 1;
    }

    Ok(stats)
}

fn decompress_named(
    bytes: &[u8],
    fork: Option<&stuffit::ForkInfo>,
    e: &StuffItEntry,
    log: &mut impl FnMut(String),
) -> Result<Vec<u8>, ()> {
    match fork {
        Some(f) if f.uncompressed_len > 0 => match stuffit::decompress_fork(bytes, f) {
            Ok(d) => Ok(d),
            Err(err) => {
                log(format!("Skipped {}: {err}", e.display_path()));
                Err(())
            }
        },
        _ => Ok(Vec::new()),
    }
}

/// Write one extracted file's forks to the host in the chosen container format.
pub fn write_entry(
    target: &Path,
    entry: &StuffItEntry,
    data: &[u8],
    rsrc: &[u8],
    format: ForkFormat,
) -> Result<()> {
    match format {
        ForkFormat::BinHex => {
            let bh = binhex::BinHexFile {
                name: entry.name.clone(),
                type_code: entry.type_code,
                creator_code: entry.creator_code,
                flags: entry.finder_flags,
                data_fork: data.to_vec(),
                resource_fork: rsrc.to_vec(),
            };
            std::fs::write(
                with_added_extension(target, "hqx"),
                binhex::build_binhex(&bh).as_bytes(),
            )?;
        }
        ForkFormat::MacBinary => {
            let mb = resource_fork::build_macbinary(
                &entry.name,
                &entry.type_code,
                &entry.creator_code,
                data,
                rsrc,
            );
            std::fs::write(with_added_extension(target, "bin"), mb)?;
        }
        ForkFormat::AppleDouble => {
            std::fs::write(target, data)?;
            if !rsrc.is_empty() || entry.type_code != [0; 4] || entry.creator_code != [0; 4] {
                let ad =
                    resource_fork::build_appledouble(&entry.type_code, &entry.creator_code, rsrc);
                std::fs::write(sidecar_path(target, "._"), ad)?;
            }
        }
        ForkFormat::Raw => {
            std::fs::write(target, data)?;
            if !rsrc.is_empty() {
                std::fs::write(with_added_extension(target, "rsrc"), rsrc)?;
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

/// Build a `._name`-style sidecar path next to `target`.
fn sidecar_path(target: &Path, prefix: &str) -> PathBuf {
    let name = target.file_name().unwrap_or_default().to_string_lossy();
    target.with_file_name(format!("{prefix}{name}"))
}
