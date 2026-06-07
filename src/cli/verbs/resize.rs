//! `rb-cli resize IMG@N --size BYTES` — resize the filesystem at the
//! selected partition to fit a new partition size. Thin CLI over the
//! per-FS in-place resize functions, dispatched via
//! [`crate::fs::resize_filesystem_for`].
//!
//! **Scope.** This verb resizes the *filesystem* inside the partition
//! — it does NOT modify the partition table. The partition itself must
//! already be the size you're asking the FS to inhabit. To change
//! partition sizes, use `rb-cli restore` (which lays out a fresh
//! disk from a backup with new sizes).
//!
//! Each per-FS resize is a no-op when the on-disk magic doesn't match,
//! so we never need to know up front which filesystem we're growing or
//! shrinking. The supported set follows `fs::resize_filesystem_for`:
//! FAT, NTFS, exFAT, HFS, HFS+, ext{2,3,4}, btrfs, SFS, PFS3, AFFS, EFS.

use anyhow::{Context, Result};
use clap::Args;

use crate::cli::img_at::ImageRef;
use crate::cli::logging::log_stderr;
use crate::cli::parse::parse_size;
use crate::cli::resolve::resolve_partition_rw;
use crate::partition::format_size;

#[derive(Debug, Args)]
pub struct ResizeArgs {
    /// Image reference (`path` or `path@N` for the 1-based partition index).
    pub image: ImageRef,

    /// New filesystem size in bytes. Accepts suffixes (`K`, `M`, `G`).
    #[arg(long)]
    pub size: String,
}

pub fn run(args: ResizeArgs) -> Result<()> {
    let new_size = parse_size(&args.size).context("parsing --size")?;
    let (mut file, ctx, commit) = resolve_partition_rw(&args.image.path, args.image.partition)?;
    log_stderr(&ctx.label);
    log_stderr(format!(
        "resize: target size {} ({} bytes), partition offset {} ({} bytes available)",
        format_size(new_size),
        new_size,
        ctx.offset,
        ctx.size,
    ));
    if new_size > ctx.size {
        log_stderr(format!(
            "warning: requested size {} exceeds partition capacity {}; the FS may refuse",
            format_size(new_size),
            format_size(ctx.size),
        ));
    }

    let mut log_cb = |s: &str| log_stderr(format!("  {s}"));
    crate::fs::resize_filesystem_for(&mut file, ctx.offset, new_size, &mut log_cb)
        .context("resize failed")?;
    drop(file);
    // No-op for raw images. For a fixed-geometry floppy container a resize that
    // changed the flat length can't be re-encoded; commit() surfaces that as a
    // clear error rather than writing a malformed container.
    commit.commit()?;
    log_stderr("resize complete");
    Ok(())
}
