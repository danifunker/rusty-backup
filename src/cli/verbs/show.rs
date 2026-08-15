//! `rb-cli show <SUBCOMMAND>` — focused read-only queries.
//!
//! Subcommands:
//! - `partmap IMG` — partition table summary (APM today; MBR / GPT
//!   added in Phase B / C as the unified PT inspector lands). Includes the
//!   Driver Descriptor Record's driver map (`ddBlock`/`ddSize`/`ddType`) and
//!   each entry's boot fields (`pmBootSize`/`pmBootCksum`/`pmProcessor`/
//!   `pmPad`) — useful for auditing `mac-scsi-bless` output.
//! - `fs-info IMG[@N]` — filesystem-level info (type, volume label,
//!   used / free space) for **any** filesystem `open_filesystem` can
//!   open — FAT/exFAT/NTFS/HFS/HFS+/ext/btrfs/ProDOS/... plus the
//!   superfloppy magic-byte sniffer (FAT / HFS / HFV at byte 0). Routes
//!   through the same `resolve_partition_streaming` + `open_filesystem`
//!   path `ls` / `get` use, so detection matches the GUI exactly.
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
use crate::cli::logging::{log_stderr, out_stdout};
use crate::cli::output::{emit_envelope, require_non_flat, Envelope, OutputFormat};
use crate::partition::apm::Apm;
use crate::partition::format_size;

#[derive(Debug, Subcommand)]
pub enum ShowCommand {
    /// Print the partition table of a disk image (APM-only today), including
    /// the Driver Descriptor Record's driver map and each entry's boot fields.
    Partmap {
        image: PathBuf,
        /// Output format. `csv`/`tsv` produce one row per partition entry.
        #[arg(long, value_enum, default_value_t = OutputFormat::Text, global = false)]
        format: OutputFormat,
        /// Password for encrypted containers (WinImage IMZ, password-protected
        /// `.zip` disks).
        #[arg(long)]
        password: Option<String>,
        /// For a `.zip` holding more than one disk image, the archive entry to
        /// open (e.g. `--inside disk.hda`).
        #[arg(long = "inside", value_name = "NAME")]
        inside: Option<String>,
    },
    /// Print filesystem-level metadata (type, volume label, used / free
    /// space) for any filesystem the engine can open.
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
        ShowCommand::Partmap {
            image,
            format,
            password,
            inside,
        } => show_partmap(image, format, password, inside),
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

fn show_partmap(
    image: PathBuf,
    format: OutputFormat,
    password: Option<String>,
    inside: Option<String>,
) -> Result<()> {
    // Whole-disk read, peeling any container / wrapper (CHD, zip, GHO, VHD,
    // DMG, ...). A plain `File::open` here read the container's own bytes and
    // reported them as a bad DDR signature — `0x504B` for a zip, i.e. "PK".
    let mut file = crate::model::source_reader::open_peeled_read_with_entry(
        &image,
        password.as_deref().map(|s| s.as_bytes()),
        inside.as_deref(),
    )?;
    // `partmap` predates every non-Apple table and went straight to APM, so an
    // SGI disk `inspect` reads fine failed with "bad DDR signature: 0x0BE5" —
    // the leading half of the SGI volume-header magic (R-026). Detect first.
    // APM keeps its full DDR + driver detail; every other table renders the
    // generic partition list, which is all those tables have.
    let table = crate::partition::PartitionTable::detect(&mut file)
        .map_err(|e| anyhow::anyhow!("detecting partition table: {e}"))?;
    if !matches!(table, crate::partition::PartitionTable::Apm(_)) {
        return show_partmap_generic(&table, format);
    }
    file.seek(std::io::SeekFrom::Start(0))
        .map_err(|e| anyhow::anyhow!("rewinding after detection: {e}"))?;
    let apm = Apm::parse(&mut file).map_err(|e| anyhow::anyhow!("parsing APM: {e}"))?;
    let bs = apm.ddr.block_size as u64;

    if format == OutputFormat::Text {
        print_partmap_text(&apm);
        return Ok(());
    }

    // Structured forms: emit a programmatic shape.
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
            boot_start: e.boot_start,
            boot_size: e.boot_size,
            boot_load: e.boot_load,
            boot_entry: e.boot_entry,
            boot_checksum: e.boot_checksum,
            processor: e.processor.clone(),
            pad_present: e.pad.iter().any(|&b| b != 0),
        })
        .collect();
    match format {
        OutputFormat::Json | OutputFormat::Yaml => {
            let payload = PartmapPayload {
                kind: "apm".to_string(),
                block_size: apm.ddr.block_size,
                block_count: apm.ddr.block_count,
                map_entry_count: apm.map_entry_count,
                driver_count: apm.ddr.driver_count,
                drivers: apm
                    .ddr
                    .driver_info
                    .iter()
                    .enumerate()
                    .map(|(i, d)| DriverRow {
                        index: i,
                        block: d.block,
                        size_blocks: d.size,
                        size_bytes: d.size as u64 * bs,
                        kind: d.kind,
                    })
                    .collect(),
                entries: rows,
            };
            emit_envelope(format, &Envelope::ok(payload))
        }
        OutputFormat::Csv | OutputFormat::Tsv => emit_csv_or_tsv(format, &rows),
        OutputFormat::Text => unreachable!(),
    }
}

/// Render any non-APM partition table: the partition list, which is the part
/// every table has. APM's DDR and driver-descriptor detail has no counterpart
/// elsewhere, so it stays on the APM path rather than being faked here.
fn show_partmap_generic(
    table: &crate::partition::PartitionTable,
    format: OutputFormat,
) -> Result<()> {
    let parts = table.partitions();
    if format == OutputFormat::Text {
        out_stdout(format!("Partition table: {}", table.type_name()));
        out_stdout(format!(
            "{:>3}  {:<28}  {:>12}  {:>14}",
            "idx", "type", "start_lba", "bytes"
        ));
        for p in &parts {
            out_stdout(format!(
                "{:>3}  {:<28}  {:>12}  {:>14}",
                p.index + 1,
                p.type_name,
                p.start_lba,
                p.size_bytes
            ));
        }
        return Ok(());
    }
    let rows: Vec<GenericPartRow> = parts
        .iter()
        .map(|p| GenericPartRow {
            index: p.index + 1,
            type_: p.type_name.clone(),
            type_string: p.partition_type_string.clone(),
            start_lba: p.start_lba,
            size_bytes: p.size_bytes,
        })
        .collect();
    match format {
        OutputFormat::Json | OutputFormat::Yaml => emit_envelope(
            format,
            &Envelope::ok(GenericPartmapPayload {
                kind: table.type_name().to_ascii_lowercase(),
                entries: rows,
            }),
        ),
        OutputFormat::Csv | OutputFormat::Tsv => emit_csv_or_tsv(format, &rows),
        OutputFormat::Text => unreachable!(),
    }
}

#[derive(serde::Serialize)]
struct GenericPartmapPayload {
    kind: String,
    entries: Vec<GenericPartRow>,
}

#[derive(serde::Serialize)]
struct GenericPartRow {
    index: usize,
    #[serde(rename = "type")]
    type_: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    type_string: Option<String>,
    start_lba: u64,
    size_bytes: u64,
}

/// Render the APM as text: DDR + driver descriptor map, then one row per
/// partition entry with boot metadata appended for driver partitions.
fn print_partmap_text(apm: &Apm) {
    let bs = apm.ddr.block_size as u64;
    out_stdout(format!(
        "DDR: block_size={} block_count={} drivers={}",
        apm.ddr.block_size, apm.ddr.block_count, apm.ddr.driver_count
    ));
    for (i, d) in apm.ddr.driver_info.iter().enumerate() {
        out_stdout(format!(
            "  driver[{i}]: ddBlock={} ddSize={} blocks ({} bytes) ddType={}",
            d.block,
            d.size,
            d.size as u64 * bs,
            d.kind
        ));
    }
    out_stdout(format!("Map entries: {}", apm.map_entry_count));
    out_stdout(format!(
        "{:>3}  {:<24}  {:<24}  {:>10}  {:>10}  {:>10}  status",
        "idx", "type", "name", "start", "blocks", "bytes"
    ));
    for (i, e) in apm.entries.iter().enumerate() {
        let mut line = format!(
            "{:>3}  {:<24}  {:<24}  {:>10}  {:>10}  {:>10}  0x{:08x}",
            i + 1,
            e.partition_type,
            e.name,
            e.start_block,
            e.block_count,
            e.size_bytes(bs as u16),
            e.status
        );
        // Boot fields are only meaningful for driver partitions; show them
        // inline when present so normal rows stay terse.
        if e.boot_size != 0 || e.boot_checksum != 0 || !e.processor.is_empty() {
            line.push_str(&format!(
                "  boot: size={} cksum=0x{:08x}",
                e.boot_size, e.boot_checksum
            ));
            if !e.processor.is_empty() {
                line.push_str(&format!(" proc={}", e.processor));
            }
            if e.pad.iter().any(|&b| b != 0) {
                line.push_str(" pad");
            }
        }
        out_stdout(line);
    }
}

#[derive(Debug, Serialize)]
struct PartmapPayload {
    kind: String,
    block_size: u16,
    block_count: u32,
    map_entry_count: u32,
    driver_count: u16,
    drivers: Vec<DriverRow>,
    entries: Vec<PartmapRow>,
}

/// One DDR driver-descriptor-map entry.
#[derive(Debug, Serialize)]
struct DriverRow {
    index: usize,
    block: u32,
    size_blocks: u16,
    size_bytes: u64,
    kind: u16,
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
    boot_start: u32,
    boot_size: u32,
    boot_load: u64,
    boot_entry: u64,
    boot_checksum: u32,
    processor: String,
    pad_present: bool,
}

// ---------------------------------------------------------------------------
// fs-info
// ---------------------------------------------------------------------------

fn show_fs_info(image: ImageRef, format: OutputFormat) -> Result<()> {
    require_non_flat(format, "show fs-info")?;

    // Route through the same generic detection path `ls` / `get` use, rather
    // than the old HFS-only opener. The previous code hardcoded
    // `HfsFilesystem::open` at the resolved offset, so a FAT (or any non-HFS)
    // superfloppy was misdetected as HFS and died with "bad MDB signature".
    // `resolve_partition_streaming` + `open_filesystem` dispatch by partition
    // type / superfloppy magic exactly as the GUI inspect path does.
    let (reader, ctx) =
        crate::cli::resolve::resolve_partition_streaming(&image.path, image.partition)?;
    log_stderr(&ctx.label);
    let fs = crate::fs::open_filesystem(
        reader,
        ctx.offset,
        ctx.type_byte,
        ctx.type_string.as_deref(),
    )
    .map_err(|e| anyhow::anyhow!("opening filesystem: {e}"))?;

    let volume_name = fs.volume_label().map(|s| s.to_string());
    let filesystem = fs.fs_type().to_string();
    let total_bytes = fs.total_size();
    let used_bytes = fs.used_size();
    let free_bytes = total_bytes.saturating_sub(used_bytes);
    let allocation_unit = fs.allocation_unit();

    match format {
        OutputFormat::Text => {
            out_stdout(format!("Filesystem:  {filesystem}"));
            out_stdout(format!(
                "Volume:      {}",
                volume_name.as_deref().unwrap_or("(unnamed)")
            ));
            out_stdout(format!(
                "Total:       {} ({total_bytes} bytes)",
                format_size(total_bytes)
            ));
            out_stdout(format!(
                "Used:        {} ({used_bytes} bytes)",
                format_size(used_bytes)
            ));
            out_stdout(format!(
                "Free:        {} ({free_bytes} bytes)",
                format_size(free_bytes)
            ));
            if let Some(unit) = allocation_unit {
                out_stdout(format!("Alloc unit:  {unit} bytes"));
            }
            Ok(())
        }
        OutputFormat::Json | OutputFormat::Yaml => {
            let payload = FsInfoPayload {
                filesystem,
                volume_name,
                total_bytes,
                used_bytes,
                free_bytes,
                allocation_unit,
            };
            emit_envelope(format, &Envelope::ok(payload))
        }
        // csv / tsv rejected above by require_non_flat.
        _ => unreachable!(),
    }
}

#[derive(Debug, Serialize)]
struct FsInfoPayload {
    /// Filesystem type as the driver reports it (e.g. `FAT12`, `exFAT`,
    /// `HFS`). Was previously a hardcoded `"hfs"`.
    filesystem: String,
    /// Volume label / name, when the filesystem carries one.
    #[serde(skip_serializing_if = "Option::is_none")]
    volume_name: Option<String>,
    total_bytes: u64,
    used_bytes: u64,
    free_bytes: u64,
    /// Allocation unit (cluster / block) size in bytes, when fixed.
    #[serde(skip_serializing_if = "Option::is_none")]
    allocation_unit: Option<u64>,
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
    crate::cli::output::emit_csv_or_tsv(format, rows)
}
