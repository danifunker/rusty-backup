//! `rb-cli bless IMG@N PATH` — mark a folder as the bootable
//! System Folder. HFS / HFS+ only; other filesystems return
//! `Unsupported`. Wraps [`EditableFilesystem::set_blessed_folder`].

use anyhow::{anyhow, bail, Result};
use clap::Args;

use crate::cli::img_at::ImageRef;
use crate::cli::logging::log_stderr;
use crate::cli::resolve::resolve_partition_rw;

#[derive(Debug, Args)]
pub struct BlessArgs {
    /// Image reference (`path` or `path@N`).
    pub image: ImageRef,
    /// Absolute Mac path of the folder to bless (e.g. `/System Folder`).
    pub path: String,
}

pub fn run(args: BlessArgs) -> Result<()> {
    let (file, ctx) = resolve_partition_rw(&args.image.path, args.image.partition)?;
    log_stderr(&ctx.label);
    let mut fs = crate::fs::open_editable_filesystem(
        file,
        ctx.offset,
        ctx.type_byte,
        ctx.type_string.as_deref(),
    )
    .map_err(|e| anyhow!("opening filesystem for write: {e}"))?;

    let entry = super::ls::resolve_path(&mut *fs, &args.path)?;
    if !entry.is_directory() {
        bail!("bless: {} is not a directory", args.path);
    }
    fs.set_blessed_folder(&entry)
        .map_err(|e| anyhow!("set_blessed_folder: {e}"))?;
    fs.sync_metadata()
        .map_err(|e| anyhow!("sync_metadata: {e}"))?;
    log_stderr(format!("blessed {}", args.path));
    Ok(())
}
