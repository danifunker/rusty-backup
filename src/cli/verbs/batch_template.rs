//! `rb-cli batch-template HOSTDIR --target IMG@N --dst /MAC/PATH` —
//! scan a host directory and emit a `rb-cli batch` JSON script that
//! mirrors the tree under `--dst` on the target. The user reviews +
//! tweaks the script, then feeds it to `rb-cli batch`.
//!
//! Phase D scope: **add-to-existing-image only** (the host tree is
//! turned into `mkdir` + `put` ops). Whole-volume creation via a
//! leading `format` op is gated on the FAT/EFS/AFFS blank creators
//! still pending in Phase B, so `--format` is not accepted yet.
//!
//! Type/creator inference is intentionally minimal — a small builtin
//! extension table for common HFS cases, otherwise `BINA`/`????`. A
//! `--type-table FILE.json` flag will be added once a real table
//! ships in `assets/`.

use anyhow::{anyhow, bail, Context, Result};
use clap::Args;
use serde::Serialize;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::cli::glob::Pattern;
use crate::cli::img_at::ImageRef;
use crate::cli::logging::log_stderr;

#[derive(Debug, Args)]
pub struct BatchTemplateArgs {
    /// Host directory to mirror.
    #[arg(value_name = "HOSTDIR")]
    pub source: PathBuf,

    /// Target image (and optional partition) the batch script will modify.
    /// Written verbatim into the script's `target` field.
    #[arg(long)]
    pub target: ImageRef,

    /// Destination directory inside the target filesystem (`/` for root).
    #[arg(long, default_value = "/")]
    pub dst: String,

    /// Write the script here. Defaults to stdout.
    #[arg(long)]
    pub out: Option<PathBuf>,

    /// Include only paths matching these globs (repeatable). Default is "all".
    #[arg(long = "include")]
    pub includes: Vec<String>,

    /// Exclude paths matching these globs (repeatable). Exclude wins on conflict.
    #[arg(long = "exclude")]
    pub excludes: Vec<String>,

    /// Glob matching is case-insensitive.
    #[arg(long = "icase")]
    pub icase: bool,

    /// Default HFS type code for files with no extension match.
    #[arg(long = "default-type", default_value = "BINA")]
    pub default_type: String,

    /// Default HFS creator code for files with no extension match.
    #[arg(long = "default-creator", default_value = "????")]
    pub default_creator: String,
}

#[derive(Serialize)]
struct Script {
    schema: String,
    target: String,
    created_by: String,
    operations: Vec<Op>,
}

#[derive(Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum Op {
    Mkdir {
        path: String,
    },
    Put {
        src: PathBuf,
        dst: String,
        #[serde(rename = "type")]
        type_code: String,
        creator: String,
    },
}

pub fn run(args: BatchTemplateArgs) -> Result<()> {
    let meta =
        fs::metadata(&args.source).with_context(|| format!("stat {}", args.source.display()))?;
    if !meta.is_dir() {
        bail!("{} is not a directory", args.source.display());
    }

    let dst_root = normalize_dst(&args.dst)?;
    let includes = compile_patterns(&args.includes, args.icase)?;
    let excludes = compile_patterns(&args.excludes, args.icase)?;

    let mut ops: Vec<Op> = Vec::new();
    walk(
        &args.source,
        &args.source,
        &dst_root,
        &includes,
        &excludes,
        &args.default_type,
        &args.default_creator,
        &mut ops,
    )?;

    let target_str = match args.target.partition {
        Some(n) => format!("{}@{n}", args.target.path.display()),
        None => args.target.path.display().to_string(),
    };
    let script = Script {
        schema: "rb-cli-batch/1".to_string(),
        target: target_str,
        created_by: format!(
            "rb-cli batch-template {} --target {} --dst {}",
            args.source.display(),
            args.target.path.display(),
            args.dst
        ),
        operations: ops,
    };

    let json = serde_json::to_string_pretty(&script)?;
    match &args.out {
        Some(p) => {
            fs::write(p, &json).with_context(|| format!("writing {}", p.display()))?;
            log_stderr(format!(
                "wrote {} ({} op(s))",
                p.display(),
                script.operations.len()
            ));
        }
        None => {
            let mut stdout = std::io::stdout().lock();
            stdout.write_all(json.as_bytes())?;
            stdout.write_all(b"\n")?;
        }
    }
    Ok(())
}

fn normalize_dst(dst: &str) -> Result<String> {
    if !dst.starts_with('/') {
        bail!("--dst must be an absolute Mac path starting with '/'");
    }
    // Strip trailing slash, keep root as "/" itself.
    let trimmed = dst.trim_end_matches('/');
    if trimmed.is_empty() {
        Ok(String::new()) // root → empty prefix; child becomes "/name"
    } else {
        Ok(trimmed.to_string())
    }
}

fn compile_patterns(patterns: &[String], icase: bool) -> Result<Vec<Pattern>> {
    let mut out = Vec::new();
    for p in patterns {
        out.extend(crate::cli::glob::compile_patterns(p, icase)?);
    }
    Ok(out)
}

fn matches(includes: &[Pattern], excludes: &[Pattern], rel: &str) -> bool {
    if excludes.iter().any(|p| p.is_match(rel)) {
        return false;
    }
    if includes.is_empty() {
        return true;
    }
    includes.iter().any(|p| p.is_match(rel))
}

#[allow(clippy::too_many_arguments)] // recursive directory walker threads filters + sinks
fn walk(
    root: &Path,
    cur: &Path,
    dst_prefix: &str,
    includes: &[Pattern],
    excludes: &[Pattern],
    default_type: &str,
    default_creator: &str,
    ops: &mut Vec<Op>,
) -> Result<()> {
    let mut entries: Vec<_> = fs::read_dir(cur)
        .with_context(|| format!("read_dir {}", cur.display()))?
        .filter_map(|e| e.ok())
        .collect();
    entries.sort_by_key(|e| e.file_name());

    for entry in entries {
        let path = entry.path();
        let name = match path.file_name().and_then(|n| n.to_str()) {
            Some(s) => s,
            None => continue,
        };
        if name.starts_with('.') {
            continue;
        }
        let rel = path
            .strip_prefix(root)
            .unwrap_or(&path)
            .to_string_lossy()
            .replace('\\', "/");
        if !matches(includes, excludes, &rel) {
            continue;
        }
        let dst = format!("{dst_prefix}/{name}");
        let ft = entry.file_type().ok();
        if ft.map(|t| t.is_dir()).unwrap_or(false) {
            ops.push(Op::Mkdir { path: dst.clone() });
            walk(
                root,
                &path,
                &dst,
                includes,
                excludes,
                default_type,
                default_creator,
                ops,
            )?;
        } else if ft.map(|t| t.is_file()).unwrap_or(false) {
            let (type_code, creator) = infer_type_creator(name, default_type, default_creator);
            ops.push(Op::Put {
                src: path.clone(),
                dst,
                type_code,
                creator,
            });
        } else {
            return Err(anyhow!(
                "{} is neither a regular file nor a directory",
                path.display()
            ));
        }
    }
    Ok(())
}

/// Tiny built-in extension table for the most common classic-Mac cases.
/// Anything not listed falls back to the user-supplied defaults.
fn infer_type_creator(name: &str, default_type: &str, default_creator: &str) -> (String, String) {
    let ext = Path::new(name)
        .extension()
        .and_then(|e| e.to_str())
        .map(|s| s.to_ascii_lowercase());
    let pair = match ext.as_deref() {
        Some("txt") => Some(("TEXT", "ttxt")),
        Some("html" | "htm") => Some(("TEXT", "MSIE")),
        Some("rtf") => Some(("TEXT", "MSWD")),
        Some("pict" | "pct") => Some(("PICT", "ttxt")),
        Some("gif") => Some(("GIFf", "ogle")),
        Some("jpg" | "jpeg") => Some(("JPEG", "ogle")),
        Some("png") => Some(("PNGf", "ogle")),
        Some("hqx") => Some(("TEXT", "SITx")),
        Some("sit") => Some(("SIT!", "SIT!")),
        Some("zip") => Some(("ZIP ", "SITx")),
        Some("dmg") => Some(("devr", "ddsk")),
        Some("pdf") => Some(("PDF ", "CARO")),
        _ => None,
    };
    match pair {
        Some((t, c)) => (t.to_string(), c.to_string()),
        None => (default_type.to_string(), default_creator.to_string()),
    }
}
