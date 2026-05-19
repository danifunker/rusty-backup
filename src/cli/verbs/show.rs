//! `rb-cli show <SUBCOMMAND>` — focused read-only queries.
//!
//! Subcommands:
//! - `partmap IMG` — partition table summary (APM today; MBR / GPT
//!   added in Phase B / C as the unified PT inspector lands).
//! - `fs-info IMG[@N]` — filesystem-level info (volume name, sizes,
//!   counts). HFS today; widens through `open_filesystem` in Phase B.
//! - `chd-info IMG` — CHD metadata (codecs, hunk size, compression,
//!   metadata records).
//! - `devices` — host block-device enumeration via `src/os/`.
//!
//! `show` is intentionally the place where each query has its own
//! single shape; the whole-disk aggregate is the separate `inspect`
//! verb (Phase C).

use anyhow::Result;
use clap::Subcommand;
use serde::Serialize;
use std::path::PathBuf;

use crate::cli::img_at::ImageRef;
use crate::cli::logging::out_stdout;
use crate::cli::output::{emit_envelope, require_non_flat, Envelope, OutputFormat};

#[derive(Debug, Subcommand)]
pub enum ShowCommand {
    /// Print the partition table of a disk image (APM-only today).
    Partmap {
        image: PathBuf,
        /// Output format. `csv`/`tsv` produce one row per partition entry.
        #[arg(long, value_enum, default_value_t = OutputFormat::Text, global = false)]
        format: OutputFormat,
    },
    /// Print filesystem-level metadata (volume name, sizes, counts).
    FsInfo {
        image: ImageRef,
        #[arg(long, value_enum, default_value_t = OutputFormat::Text, global = false)]
        format: OutputFormat,
    },
    /// Print CHD metadata for a `.chd` image.
    ChdInfo {
        image: PathBuf,
        #[arg(long, value_enum, default_value_t = OutputFormat::Text, global = false)]
        format: OutputFormat,
    },
    /// List host block devices (disks attached to this machine).
    Devices {
        /// Filter to removable devices only.
        #[arg(long)]
        removable_only: bool,
        #[arg(long, value_enum, default_value_t = OutputFormat::Text, global = false)]
        format: OutputFormat,
    },
}

pub fn run(cmd: ShowCommand) -> Result<()> {
    match cmd {
        ShowCommand::Partmap { image, format } => show_partmap(image, format),
        ShowCommand::FsInfo { image, format } => show_fs_info(image, format),
        ShowCommand::ChdInfo { image, format } => show_chd_info(image, format),
        ShowCommand::Devices {
            removable_only,
            format,
        } => show_devices(removable_only, format),
    }
}

// ---------------------------------------------------------------------------
// partmap
// ---------------------------------------------------------------------------

fn show_partmap(image: PathBuf, format: OutputFormat) -> Result<()> {
    if format == OutputFormat::Text {
        return crate::cli::api::apm::cmd_info(image);
    }
    // Structured forms: re-parse so we can emit a programmatic shape.
    let mut file = crate::cli::io::open_image_ro(&image)?;
    let apm = crate::partition::apm::Apm::parse(&mut file)
        .map_err(|e| anyhow::anyhow!("parsing APM: {e}"))?;
    let bs = apm.ddr.block_size as u64;
    let rows: Vec<PartmapRow> = apm
        .entries
        .iter()
        .enumerate()
        .map(|(i, e)| PartmapRow {
            index: i + 1,
            type_: e.partition_type.clone(),
            name: e.name.clone(),
            start_block: e.start_block,
            block_count: e.block_count,
            size_bytes: e.size_bytes(bs as u16),
            status: e.status,
        })
        .collect();
    match format {
        OutputFormat::Json | OutputFormat::Yaml => {
            let payload = PartmapPayload {
                kind: "apm".to_string(),
                block_size: apm.ddr.block_size,
                map_entry_count: apm.map_entry_count,
                entries: rows,
            };
            emit_envelope(format, &Envelope::ok(payload))
        }
        OutputFormat::Csv | OutputFormat::Tsv => emit_csv_or_tsv(format, &rows),
        OutputFormat::Text => unreachable!(),
    }
}

#[derive(Debug, Serialize)]
struct PartmapPayload {
    kind: String,
    block_size: u16,
    map_entry_count: u32,
    entries: Vec<PartmapRow>,
}

#[derive(Debug, Serialize)]
struct PartmapRow {
    index: usize,
    #[serde(rename = "type")]
    type_: String,
    name: String,
    start_block: u32,
    block_count: u32,
    size_bytes: u64,
    status: u32,
}

// ---------------------------------------------------------------------------
// fs-info
// ---------------------------------------------------------------------------

fn show_fs_info(image: ImageRef, format: OutputFormat) -> Result<()> {
    require_non_flat(format, "show fs-info")?;
    if format == OutputFormat::Text {
        return crate::cli::api::hfs::cmd_info(image.path, image.partition);
    }
    // Structured: open the FS and emit volume_summary.
    let mut file = crate::cli::io::open_image_ro(&image.path)?;
    let (offset, _label) = crate::cli::api::hfs::resolve_hfs_offset(&mut file, image.partition)?;
    let fs = crate::fs::hfs::HfsFilesystem::open(file, offset)
        .map_err(|e| anyhow::anyhow!("opening HFS: {e}"))?;
    let s = fs.volume_summary();
    let payload = FsInfoPayload {
        filesystem: "hfs".to_string(),
        volume_name: s.volume_name,
        block_size: s.block_size,
        total_blocks: s.total_blocks,
        free_blocks: s.free_blocks,
        used_bytes: s.used_bytes,
        file_count: s.file_count,
        folder_count: s.folder_count,
    };
    emit_envelope(format, &Envelope::ok(payload))
}

#[derive(Debug, Serialize)]
struct FsInfoPayload {
    filesystem: String,
    volume_name: String,
    block_size: u32,
    total_blocks: u16,
    free_blocks: u16,
    used_bytes: u64,
    file_count: u32,
    folder_count: u32,
}

// ---------------------------------------------------------------------------
// chd-info
// ---------------------------------------------------------------------------

fn show_chd_info(image: PathBuf, format: OutputFormat) -> Result<()> {
    require_non_flat(format, "show chd-info")?;
    let info = crate::rbformats::chd::format_chd_info(&image)?;
    match format {
        OutputFormat::Text => {
            out_stdout(&info);
            Ok(())
        }
        OutputFormat::Json | OutputFormat::Yaml => {
            // We don't have a structured CHD-info shape today (the chd
            // crate returns formatted text). Wrap the raw report as a
            // single string so consumers at least get the envelope —
            // structured parsing comes when we extract per-field
            // accessors from libchdman-rs in a later phase.
            let payload = ChdInfoPayload { report: info };
            emit_envelope(format, &Envelope::ok(payload))
        }
        _ => unreachable!(),
    }
}

#[derive(Debug, Serialize)]
struct ChdInfoPayload {
    /// Raw `chdman info`-style report. Structured per-field shape will
    /// be added when the upstream library exposes accessors.
    report: String,
}

// ---------------------------------------------------------------------------
// devices
// ---------------------------------------------------------------------------

fn show_devices(removable_only: bool, format: OutputFormat) -> Result<()> {
    let all = crate::device::enumerate_devices();
    let filtered: Vec<DeviceRow> = all
        .into_iter()
        .filter(|d| !removable_only || d.is_removable)
        .map(|d| DeviceRow {
            path: d.path.display().to_string(),
            name: d.name,
            size_bytes: d.size_bytes,
            removable: d.is_removable,
            read_only: d.is_read_only,
            system_disk: d.is_system,
            bus_protocol: d.bus_protocol,
            media_name: d.media_name,
            mount_points: d
                .partitions
                .iter()
                .map(|p| p.mount_point.display().to_string())
                .collect(),
        })
        .collect();

    match format {
        OutputFormat::Text => {
            out_stdout(format!(
                "{:<30}  {:>14}  {:<10}  {:<12}  {}",
                "path", "size", "removable", "bus", "name"
            ));
            for d in &filtered {
                out_stdout(format!(
                    "{:<30}  {:>14}  {:<10}  {:<12}  {}",
                    d.path,
                    crate::partition::format_size(d.size_bytes),
                    if d.removable { "yes" } else { "no" },
                    d.bus_protocol,
                    d.name
                ));
            }
            Ok(())
        }
        OutputFormat::Json | OutputFormat::Yaml => emit_envelope(format, &Envelope::ok(filtered)),
        OutputFormat::Csv | OutputFormat::Tsv => emit_csv_or_tsv(format, &filtered),
    }
}

#[derive(Debug, Serialize)]
struct DeviceRow {
    path: String,
    name: String,
    size_bytes: u64,
    removable: bool,
    read_only: bool,
    system_disk: bool,
    bus_protocol: String,
    media_name: String,
    mount_points: Vec<String>,
}

// ---------------------------------------------------------------------------
// shared CSV/TSV helper
// ---------------------------------------------------------------------------

fn emit_csv_or_tsv<T: Serialize>(format: OutputFormat, rows: &[T]) -> Result<()> {
    let delim = if format == OutputFormat::Tsv {
        b'\t'
    } else {
        b','
    };
    let mut wtr = csv::WriterBuilder::new()
        .delimiter(delim)
        .from_writer(std::io::stdout().lock());
    for row in rows {
        wtr.serialize(row)?;
    }
    wtr.flush()?;
    Ok(())
}
