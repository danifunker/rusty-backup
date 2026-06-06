//! `rb-cli floppy <SUBCOMMAND>` — single-file and (in a follow-up phase)
//! bulk conversion between the four floppy-container formats: XDF, HDM,
//! DIM, and D88. Thin CLI over
//! [`crate::rbformats::containers::convert_floppy_container`].
//!
//! Subcommands:
//! - `convert` — convert one floppy image to another format
//! - `info`    — print the detected container kind + media geometry

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};
use std::path::PathBuf;

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
    /// Source floppy image (.xdf, .hdm, .dim, or .d88).
    pub input: PathBuf,
    /// Destination path. The floppy container format is taken from the
    /// extension (.xdf / .hdm / .dim / .d88).
    pub output: PathBuf,
}

fn run_convert(args: ConvertArgs) -> Result<()> {
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
