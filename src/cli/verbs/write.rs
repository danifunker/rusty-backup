//! `rb-cli write IMAGE DEVICE --yes` — pour an image file onto a block
//! device. Phase D scope: full safety machinery via
//! [`crate::cli::device_safety`] — system-disk refusal (overridable
//! with `--write-to-system-disk`), mounted-target refusal, mount-point
//! vs device-path disambiguation. `--yes` skips the confirmation
//! prompt but never the safety summary.

use anyhow::{bail, Context, Result};
use clap::Args;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::cli::device_safety::{preflight, print_safety_summary};
use crate::cli::logging::log_stderr;
use crate::model::physical_write_runner::{
    self, PhysicalWriteRequest, PhysicalWriteSource, WriteExtent,
};
use crate::partition::format_size;

#[derive(Debug, Args)]
pub struct WriteArgs {
    /// Source image file.
    pub image: PathBuf,

    /// Destination block-device path:
    ///   - Linux: `/dev/sdX` or `/dev/nvmeXnY`
    ///   - macOS: `/dev/diskN` / `/dev/rdiskN`
    ///   - Windows: `"\\.\PhysicalDriveN"` (quote for PowerShell)
    pub device: PathBuf,

    /// Write into one partition instead of the whole disk: `N` (the `idx`
    /// column of `inspect`), `sN` (the table's own slot) or an AmigaDOS device
    /// name. The partition's bounds cap the write; the rest of the disk,
    /// including the partition table, is left untouched.
    #[arg(long, value_parser = parse_partition_selector)]
    pub partition: Option<crate::cli::img_at::PartSelector>,

    /// Required confirmation. Skips the prompt but never the safety
    /// summary printed on stderr.
    #[arg(long)]
    pub yes: bool,

    /// Allow writing to the system boot disk (refused by default).
    #[arg(long = "write-to-system-disk")]
    pub write_to_system_disk: bool,
}

/// Resolve `--partition N` to the region it occupies on `device`.
fn partition_extent(
    device: &std::path::Path,
    index: &crate::cli::img_at::PartSelector,
) -> Result<WriteExtent> {
    let mut probe = crate::model::source_reader::open_peeled_read_with_entry(device, None, None)
        .with_context(|| format!("opening {} to read its partition table", device.display()))?;
    let table = crate::partition::PartitionTable::detect(&mut probe).map_err(|e| {
        anyhow::anyhow!("detecting the partition table on {}: {e}", device.display())
    })?;
    let partitions = table.partitions();
    // Positional, matching `inspect`'s `idx` column. This used to key on
    // `PartitionInfo::index + 1` — the raw table slot — so on an APM disk
    // `--partition 2` silently wrote to the first partition.
    let part = crate::cli::resolve::select_partition(&table, &partitions, index)
        .with_context(|| format!("selecting a partition on {}", device.display()))?;
    Ok(WriteExtent::partition_at(
        part.byte_offset(),
        part.size_bytes,
    ))
}

pub fn run(args: WriteArgs) -> Result<()> {
    if !args.yes {
        bail!(
            "writing to {} is destructive; pass --yes to confirm",
            args.device.display()
        );
    }

    let pre = preflight(&args.device, args.write_to_system_disk)?;

    let device_size = pre.device.as_ref().map(|d| d.size_bytes).unwrap_or(0);
    let extent = match &args.partition {
        Some(n) => partition_extent(&args.device, n)?,
        None => WriteExtent::whole_disk(device_size),
    };

    // The decoded size, which for a compressed container is nothing like the
    // file's byte count on disk.
    let src_size = decoded_size(&args.image)?;

    let target_label = match &args.partition {
        Some(n) => format!("{} partition {}", args.device.display(), n),
        None => args.device.display().to_string(),
    };
    print_safety_summary(
        "write",
        &args.image.display().to_string(),
        &args.device,
        src_size,
        pre.device.as_ref(),
    );
    log_stderr(format!(
        "  target region: {} at byte offset {}",
        format_size(extent.capacity),
        extent.offset,
    ));

    let req = PhysicalWriteRequest {
        source: PhysicalWriteSource::Image(args.image.clone()),
        target_device_path: args.device.clone(),
        target_size_bytes: device_size,
        extent,
        wrap: None,
    };

    let status = Arc::new(Mutex::new(new_cli_status(src_size)));
    physical_write_runner::run_worker(&req, Arc::clone(&status))
        .with_context(|| format!("writing {} to {}", args.image.display(), target_label))?;

    log_stderr(format!(
        "done: wrote {} to {}",
        format_size(src_size),
        target_label
    ));
    Ok(())
}

/// Decoded length of `path`, peeling any container/wrapper layer.
fn decoded_size(path: &std::path::Path) -> Result<u64> {
    use std::io::Seek;
    let mut src = crate::model::source_reader::open_peeled_read_with_entry(path, None, None)
        .with_context(|| format!("opening {}", path.display()))?;
    Ok(src.seek(std::io::SeekFrom::End(0))?)
}

fn new_cli_status(total: u64) -> physical_write_runner::PhysicalWriteStatus {
    physical_write_runner::PhysicalWriteStatus {
        finished: false,
        error: None,
        log_messages: Vec::new(),
        current_bytes: 0,
        total_bytes: total,
        cancel_requested: false,
    }
}

/// Parse `--partition`: the same forms the `IMG@…` suffix accepts.
fn parse_partition_selector(s: &str) -> Result<crate::cli::img_at::PartSelector, String> {
    crate::cli::img_at::ImageRef::parse(&format!("x@{s}"))
        .map_err(|e| e.to_string())?
        .partition
        .ok_or_else(|| "empty partition selector".to_string())
}
