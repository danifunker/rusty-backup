//! `rb-cli repack IMG[@N]` — defragment a Human68k (X68000) partition in
//! place. Clones the volume into a fresh, contiguously-packed copy and
//! writes that back over the partition region.
//!
//! **Scope.** This repacks the *filesystem* inside the partition — it does
//! NOT modify the partition table. With `--size` smaller than the
//! partition, the volume is packed into the smaller footprint and the
//! freed tail is left unused (shrinking the partition table itself is a
//! separate operation, out of scope for v1). Human68k FAT16 volumes only
//! (the real X68000 SCSI/SASI HDD shape); FAT12 floppies use the
//! floppy-container converter.
//!
//! Why this exists: the in-place resizer
//! ([`crate::fs::human68k::resize_human68k_in_place`]) keeps every cluster
//! at its original byte offset so existing files stay byte-exact, which
//! means it can only trim trailing free space. `repack` reclaims holes
//! left by deleted/relocated files.

use anyhow::{bail, Context, Result};
use clap::Args;
use std::io::{Seek, SeekFrom, Write};

use crate::cli::img_at::ImageRef;
use crate::cli::logging::log_stderr;
use crate::cli::parse::parse_size;
use crate::cli::resolve::{resolve_partition_ro, resolve_partition_rw};
use crate::fs::filesystem::Filesystem;
use crate::fs::human68k::Human68kFilesystem;
use crate::fs::human68k_clone::stream_defragmented_human68k;
use crate::partition::format_size;

#[derive(Debug, Args)]
pub struct RepackArgs {
    /// Image reference (`path` or `path@N` for the 1-based partition index).
    pub image: ImageRef,

    /// New filesystem size in bytes (default: the partition's current
    /// size). Accepts suffixes (`K`, `M`, `G`). Must not exceed the
    /// partition capacity.
    #[arg(long)]
    pub size: Option<String>,
}

pub fn run(args: RepackArgs) -> Result<()> {
    // Read-only handle for the clone source. Opening the filesystem also
    // validates that the selected partition is actually a Human68k volume.
    let (ro_file, ctx) = resolve_partition_ro(&args.image.path, args.image.partition)?;
    log_stderr(&ctx.label);
    let mut source = Human68kFilesystem::open(ro_file, ctx.offset).map_err(|e| {
        anyhow::anyhow!(
            "partition at offset {} is not a Human68k volume: {e}",
            ctx.offset
        )
    })?;

    // Default to the filesystem's own declared size (BPB total_sectors ×
    // sector size), NOT the partition-table length: on 256-byte-SASI
    // X68000 disks the partition is sized in 256-byte units while the FS
    // sector is 1024, so the partition length isn't a whole number of FS
    // sectors. The FS size always is.
    let fs_size = source.total_size();
    let target_size = match &args.size {
        Some(s) => parse_size(s).context("parsing --size")?,
        None => fs_size,
    };
    if target_size > ctx.size {
        bail!(
            "requested size {} exceeds partition capacity {}",
            format_size(target_size),
            format_size(ctx.size),
        );
    }
    log_stderr(format!(
        "repack: packing into {} ({} bytes) at partition offset {}",
        format_size(target_size),
        target_size,
        ctx.offset,
    ));

    // The clone reads the source fully into a tempfile before draining it
    // back to the writer, so the separate RW handle below never races the
    // read side even though both point at the same file.
    // repack is documented HDD-only (FAT16); .hda/.hdf images aren't editable
    // containers, so this commit is a no-op. (A floppy-container path fails
    // earlier at the read-only Human68k open above, which is the intended
    // scope boundary — floppies use `floppy convert` / put/rm/mkdir.)
    let (mut rw_file, _, commit) = resolve_partition_rw(&args.image.path, args.image.partition)?;
    rw_file.seek(SeekFrom::Start(ctx.offset))?;

    let mut log_cb = |s: &str| log_stderr(format!("  {s}"));
    let report = stream_defragmented_human68k(
        &mut source,
        target_size,
        &mut rw_file,
        &mut log_cb,
        &mut |_| {},
    )
    .context("repack failed")?;
    rw_file.flush().context("flushing repacked partition")?;
    drop(rw_file);
    commit.commit()?;

    for w in &report.warnings {
        log_stderr(format!("  warning: {w}"));
    }
    log_stderr(format!(
        "repack complete: {} dir(s) / {} file(s) / {} packed",
        report.dirs_copied,
        report.files_copied,
        format_size(report.bytes_copied),
    ));
    Ok(())
}
