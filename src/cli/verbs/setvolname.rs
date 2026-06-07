//! `rb-cli setvolname IMG[@N] NAME` — rename an HFS volume in place.
//!
//! Updates the MDB `drVN`, the root directory's catalog key, and the
//! root thread record's `thdCName`. Without all three, classic Mac OS
//! refuses to mount the volume (-127). HFS limits names to 27 Mac
//! Roman bytes; empty is rejected.

use anyhow::{anyhow, Result};
use clap::Args;

use crate::cli::img_at::ImageRef;
use crate::cli::logging::log_stderr;
use crate::cli::resolve::resolve_partition_rw;

#[derive(Debug, Args)]
pub struct SetVolNameArgs {
    /// Image reference (`path` or `path@N`).
    pub image: ImageRef,
    /// New volume name. HFS: 1..=27 Mac Roman bytes.
    pub name: String,
}

pub fn run(args: SetVolNameArgs) -> Result<()> {
    let (file, ctx, commit) = resolve_partition_rw(&args.image.path, args.image.partition)?;
    log_stderr(&ctx.label);
    let mut fs = crate::fs::open_editable_filesystem(
        file,
        ctx.offset,
        ctx.type_byte,
        ctx.type_string.as_deref(),
    )
    .map_err(|e| anyhow!("opening filesystem for write: {e}"))?;

    fs.set_volume_name(&args.name)
        .map_err(|e| anyhow!("set_volume_name: {e}"))?;
    fs.sync_metadata()
        .map_err(|e| anyhow!("sync_metadata: {e}"))?;
    drop(fs);
    commit.commit()?;
    log_stderr(format!("renamed volume to {:?}", args.name));
    Ok(())
}
