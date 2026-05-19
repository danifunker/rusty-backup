//! `rb-cli rm IMG[@N] PATH` — delete a file or directory from a
//! filesystem. Generic across every EditableFilesystem.

use anyhow::{anyhow, bail, Result};
use clap::Args;

use crate::cli::img_at::ImageRef;
use crate::cli::logging::log_stderr;
use crate::cli::parse::split_mac_path;
use crate::cli::resolve::resolve_partition_rw;

#[derive(Debug, Args)]
pub struct RmArgs {
    /// Image reference (`path` or `path@N` for the 1-based partition index).
    pub image: ImageRef,

    /// Path inside the filesystem.
    pub path: String,

    /// Recursively delete a directory and its contents.
    #[arg(long, short = 'r')]
    pub recursive: bool,
}

pub fn run(args: RmArgs) -> Result<()> {
    let (parent_path, name) = split_mac_path(&args.path)?;
    if name.is_empty() {
        bail!("path has no basename");
    }

    let (file, ctx) = resolve_partition_rw(&args.image.path, args.image.partition)?;
    log_stderr(&ctx.label);
    let mut fs = crate::fs::open_editable_filesystem(
        file,
        ctx.offset,
        ctx.type_byte,
        ctx.type_string.as_deref(),
    )
    .map_err(|e| anyhow!("opening filesystem for write: {e}"))?;

    let parent = super::ls::resolve_path(&mut *fs, &parent_path)?;
    let children = fs
        .list_directory(&parent)
        .map_err(|e| anyhow!("list_directory: {e}"))?;
    let entry = children
        .into_iter()
        .find(|c| c.name == name)
        .ok_or_else(|| anyhow!("not found: {}", args.path))?;

    if entry.is_directory() {
        if !args.recursive {
            bail!(
                "{} is a directory; pass -r / --recursive to delete it and its contents",
                args.path
            );
        }
        fs.delete_recursive(&parent, &entry)
            .map_err(|e| anyhow!("delete_recursive: {e}"))?;
    } else {
        fs.delete_entry(&parent, &entry)
            .map_err(|e| anyhow!("delete_entry: {e}"))?;
    }
    fs.sync_metadata()
        .map_err(|e| anyhow!("sync_metadata: {e}"))?;
    Ok(())
}
