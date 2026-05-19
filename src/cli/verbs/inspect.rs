//! `rb-cli inspect IMG` — whole-disk aggregate view.
//!
//! Phase A scope: partition-table summary + per-partition type / size /
//! bootable info, sourced entirely from `partition::PartitionTable::detect`
//! and `PartitionInfo::type_name`. For CHD images, the upstream CHD info
//! is appended.
//!
//! Phase B / C widen this by opening each partition's filesystem to
//! print volume name, free space, and label; that depends on the
//! `open_filesystem`-dispatched FS reads which are generic-fs-aware
//! work (Phase B). The `--format` schema is forward-compatible: the
//! existing field set keeps its shape and the volume-level fields are
//! added next to it.

use anyhow::Result;
use clap::Args;
use serde::Serialize;
use std::path::PathBuf;

use crate::cli::logging::out_stdout;
use crate::cli::output::{emit_envelope, require_non_flat, Envelope, OutputFormat};
use crate::partition::{format_size, PartitionTable};

#[derive(Debug, Args)]
pub struct InspectArgs {
    /// Image path. `inspect` always reads the whole disk — there is no
    /// `@N` form. For per-partition detail use `show fs-info IMG@N`.
    pub image: PathBuf,

    /// Output format.
    #[arg(long, value_enum, default_value_t = OutputFormat::Text, global = false)]
    pub format: OutputFormat,
}

pub fn run(args: InspectArgs) -> Result<()> {
    require_non_flat(args.format, "inspect")?;
    let mut file = crate::cli::io::open_image_ro(&args.image)?;
    let pt = PartitionTable::detect(&mut file)?;
    let partitions = pt.partitions();
    let chd_report = if args
        .image
        .extension()
        .and_then(|s| s.to_str())
        .map(|s| s.eq_ignore_ascii_case("chd"))
        .unwrap_or(false)
    {
        Some(crate::rbformats::chd::format_chd_info(&args.image)?)
    } else {
        None
    };

    match args.format {
        OutputFormat::Text => emit_text(&args.image, &pt, &partitions, chd_report.as_deref()),
        OutputFormat::Json | OutputFormat::Yaml => emit_structured(
            args.format,
            &args.image,
            &pt,
            &partitions,
            chd_report.as_deref(),
        ),
        _ => unreachable!(),
    }
}

fn emit_text(
    image: &std::path::Path,
    pt: &PartitionTable,
    partitions: &[crate::partition::PartitionInfo],
    chd_report: Option<&str>,
) -> Result<()> {
    out_stdout(format!("Image:           {}", image.display()));
    out_stdout(format!("Partition table: {}", pt.type_name()));
    if let Some(report) = chd_report {
        out_stdout("");
        out_stdout("CHD info:");
        for line in report.lines() {
            out_stdout(format!("  {line}"));
        }
    }
    if partitions.is_empty() {
        out_stdout("\nNo partitions detected.");
        return Ok(());
    }
    out_stdout("");
    out_stdout(format!(
        "{:>3}  {:<24}  {:>12}  {:>14}  {}",
        "idx", "type", "start_lba", "size", "flags"
    ));
    for p in partitions {
        let mut flags = Vec::new();
        if p.bootable {
            flags.push("boot");
        }
        if p.is_logical {
            flags.push("logical");
        }
        if p.is_extended_container {
            flags.push("extended");
        }
        out_stdout(format!(
            "{:>3}  {:<24}  {:>12}  {:>14}  {}",
            p.index,
            p.type_name,
            p.start_lba,
            format_size(p.size_bytes),
            flags.join(",")
        ));
    }
    Ok(())
}

fn emit_structured(
    format: OutputFormat,
    image: &std::path::Path,
    pt: &PartitionTable,
    partitions: &[crate::partition::PartitionInfo],
    chd_report: Option<&str>,
) -> Result<()> {
    let payload = InspectPayload {
        image: image.display().to_string(),
        partition_table: pt.type_name().to_string(),
        chd_info: chd_report.map(|s| s.to_string()),
        partitions: partitions
            .iter()
            .map(|p| PartitionRow {
                index: p.index,
                type_name: p.type_name.clone(),
                partition_type_byte: p.partition_type_byte,
                partition_type_string: p.partition_type_string.clone(),
                start_lba: p.start_lba,
                size_bytes: p.size_bytes,
                bootable: p.bootable,
                is_logical: p.is_logical,
                is_extended_container: p.is_extended_container,
            })
            .collect(),
    };
    emit_envelope(format, &Envelope::ok(payload))
}

#[derive(Debug, Serialize)]
struct InspectPayload {
    image: String,
    partition_table: String,
    /// CHD `chdman info`-style report, when the image is a `.chd`.
    /// Structured per-field accessors are planned for a later phase.
    #[serde(skip_serializing_if = "Option::is_none")]
    chd_info: Option<String>,
    partitions: Vec<PartitionRow>,
}

#[derive(Debug, Serialize)]
struct PartitionRow {
    index: usize,
    type_name: String,
    partition_type_byte: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    partition_type_string: Option<String>,
    start_lba: u64,
    size_bytes: u64,
    bootable: bool,
    is_logical: bool,
    is_extended_container: bool,
}
