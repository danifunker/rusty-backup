//! `rb-cli floppy <SUBCOMMAND>` — single-file and bulk conversion between
//! the four floppy-container formats: XDF, HDM, DIM, and D88. Thin CLI
//! over [`crate::rbformats::containers::convert_floppy_container`].
//!
//! Subcommands:
//! - `convert` — convert one floppy image, or every floppy in a folder
//! - `info`    — print the detected container kind + media geometry

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};
use std::path::{Path, PathBuf};

use crate::cli::logging::log_stderr;
use crate::rbformats::containers::{
    convert_floppy_container, detect_container_kind, floppy_kind_from_extension,
    is_floppy_container, ContainerKind,
};

#[derive(Debug, Subcommand)]
pub enum FloppyCommand {
    /// Convert a floppy image between XDF / HDM / DIM / D88 formats. The
    /// output format is inferred from the destination extension.
    Convert(ConvertArgs),
    /// Print the detected container kind and geometry for a floppy image.
    Info(InfoArgs),
}

pub fn run(cmd: FloppyCommand) -> Result<()> {
    match cmd {
        FloppyCommand::Convert(a) => run_convert(a),
        FloppyCommand::Info(a) => run_info(a),
    }
}

#[derive(Debug, Args)]
pub struct ConvertArgs {
    /// Source floppy image (.xdf, .hdm, .dim, .d88) — or a directory of
    /// floppy images when paired with `--to`.
    pub input: PathBuf,
    /// Destination path. For a file input the target format is taken from
    /// the extension; for a directory input pass a directory here and use
    /// `--to <fmt>` to pick the output format.
    pub output: PathBuf,
    /// Output format for directory (bulk) mode. Required when `input` is a
    /// directory; ignored for single-file mode (extension wins there).
    #[arg(long, value_parser = ["xdf", "hdm", "dim", "d88"])]
    pub to: Option<String>,
    /// Walk the input directory recursively. Only meaningful in bulk mode.
    #[arg(long)]
    pub recursive: bool,
}

fn run_convert(args: ConvertArgs) -> Result<()> {
    if args.input.is_dir() {
        run_convert_bulk(args)
    } else {
        run_convert_single(args)
    }
}

fn run_convert_single(args: ConvertArgs) -> Result<()> {
    let target = floppy_kind_from_extension(&args.output).with_context(|| {
        format!(
            "could not determine target format from {}",
            args.output.display()
        )
    })?;
    let report = convert_floppy_container(&args.input, &args.output, target)?;
    if report.identity {
        log_stderr(format!(
            "Copied {} -> {} ({} bytes, identity {} -> {})",
            args.input.display(),
            args.output.display(),
            report.bytes_written,
            report.source.display_name(),
            report.target.display_name(),
        ));
    } else {
        log_stderr(format!(
            "Converted {} ({}) -> {} ({}, media: {}, {} bytes)",
            args.input.display(),
            report.source.display_name(),
            args.output.display(),
            report.target.display_name(),
            report.media.display_label(),
            report.bytes_written,
        ));
    }
    Ok(())
}

fn run_convert_bulk(args: ConvertArgs) -> Result<()> {
    let target_str = args.to.as_deref().ok_or_else(|| {
        anyhow::anyhow!("bulk mode requires --to <xdf|hdm|dim|d88> when the input is a directory")
    })?;
    let target_ext = target_str.to_ascii_lowercase();
    let target = floppy_kind_from_extension(Path::new(&format!("dummy.{target_ext}")))?;

    std::fs::create_dir_all(&args.output)
        .with_context(|| format!("creating output directory {}", args.output.display()))?;

    let mut candidates: Vec<PathBuf> = Vec::new();
    collect_floppy_files(&args.input, args.recursive, &mut candidates)?;
    candidates.sort();
    if candidates.is_empty() {
        log_stderr(format!(
            "No floppy-container files found under {} (looked for .xdf/.hdm/.dim/.d88)",
            args.input.display()
        ));
        return Ok(());
    }

    let total = candidates.len();
    let mut converted = 0usize;
    let mut copied = 0usize;
    let mut failed = 0usize;

    for (idx, src) in candidates.iter().enumerate() {
        let stem = src.file_stem().and_then(|s| s.to_str()).unwrap_or("disk");
        let dst = args.output.join(format!("{stem}.{target_ext}"));
        match convert_floppy_container(src, &dst, target) {
            Ok(report) => {
                if report.identity {
                    copied += 1;
                    log_stderr(format!(
                        "[{}/{}] Copied (identity) {} -> {} ({} bytes)",
                        idx + 1,
                        total,
                        src.display(),
                        dst.display(),
                        report.bytes_written
                    ));
                } else {
                    converted += 1;
                    log_stderr(format!(
                        "[{}/{}] {} ({}) -> {} ({}, {} bytes)",
                        idx + 1,
                        total,
                        src.display(),
                        report.source.display_name(),
                        dst.display(),
                        report.media.display_label(),
                        report.bytes_written
                    ));
                }
            }
            Err(e) => {
                failed += 1;
                log_stderr(format!(
                    "[{}/{}] FAILED {} -> {}: {:#}",
                    idx + 1,
                    total,
                    src.display(),
                    dst.display(),
                    e
                ));
            }
        }
    }

    log_stderr(format!(
        "Bulk complete: {converted} converted, {copied} identity-copied, \
         {failed} failed (total {total})"
    ));
    if failed > 0 {
        bail!("{failed} of {total} conversions failed");
    }
    Ok(())
}

fn collect_floppy_files(dir: &Path, recursive: bool, out: &mut Vec<PathBuf>) -> Result<()> {
    for entry in
        std::fs::read_dir(dir).with_context(|| format!("reading directory {}", dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            if recursive {
                collect_floppy_files(&path, true, out)?;
            }
            continue;
        }
        if !path.is_file() {
            continue;
        }
        let Some(ext) = path
            .extension()
            .and_then(|e| e.to_str())
            .map(|s| s.to_ascii_lowercase())
        else {
            continue;
        };
        if matches!(ext.as_str(), "xdf" | "hdm" | "dim" | "d88") {
            out.push(path);
        }
    }
    Ok(())
}

#[derive(Debug, Args)]
pub struct InfoArgs {
    /// Floppy image to inspect.
    pub input: PathBuf,
}

fn run_info(args: InfoArgs) -> Result<()> {
    let bytes =
        std::fs::read(&args.input).with_context(|| format!("reading {}", args.input.display()))?;
    let head = &bytes[..bytes.len().min(256)];
    let kind = detect_container_kind(head, Some(&args.input));
    if !is_floppy_container(kind) {
        bail!(
            "{} is not a recognised floppy container (detected: {})",
            args.input.display(),
            kind.display_name()
        );
    }
    // Decode once so we can derive geometry from the flat size.
    let (flat, media) = match kind {
        ContainerKind::Xdf => crate::rbformats::containers::xdf::decode_xdf_bytes(&bytes)?,
        ContainerKind::Hdm => crate::rbformats::containers::hdm::decode_hdm_bytes(&bytes)?,
        ContainerKind::Dim => crate::rbformats::containers::dim::decode_dim_bytes(&bytes)?,
        ContainerKind::D88 => {
            let flat = crate::rbformats::containers::d88::decode_d88_bytes(&bytes)?;
            let media =
                crate::rbformats::containers::floppy_geom::require_media_from_size(flat.len())?;
            (flat, media)
        }
        _ => unreachable!("guarded by is_floppy_container above"),
    };
    let geom = media.geometry();
    println!("File:        {}", args.input.display());
    println!("File size:   {} bytes", bytes.len());
    println!("Container:   {}", kind.display_name());
    println!("Media:       {} ({})", media.display_label(), media.flag());
    println!(
        "Geometry:    {} cyl x {} heads x {} sec x {} B (flat = {} bytes)",
        geom.cyls,
        geom.heads,
        geom.spt,
        geom.sec_size,
        flat.len()
    );
    Ok(())
}
