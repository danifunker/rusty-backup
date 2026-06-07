//! `rb-cli mkdir IMG[@N] PATH` — create a directory inside a filesystem.
//! Generic across every EditableFilesystem.

use anyhow::{anyhow, bail, Result};
use clap::Args;

use crate::cli::img_at::ImageRef;
use crate::cli::logging::log_stderr;
use crate::cli::parse::split_mac_path;
use crate::cli::resolve::resolve_partition_rw;
use crate::fs::filesystem::CreateDirectoryOptions;

#[derive(Debug, Args)]
pub struct MkdirArgs {
    /// Image reference (`path` or `path@N` for the 1-based partition index).
    pub image: ImageRef,

    /// Directory path to create. The parent must exist (no `-p`-style
    /// auto-creation in Phase B).
    pub path: String,
}

pub fn run(args: MkdirArgs) -> Result<()> {
    let (parent_path, name) = split_mac_path(&args.path)?;
    if name.is_empty() {
        bail!("directory path has no basename");
    }

    let (file, ctx, commit) = resolve_partition_rw(&args.image.path, args.image.partition)?;
    log_stderr(&ctx.label);
    let mut fs = crate::fs::open_editable_filesystem(
        file,
        ctx.offset,
        ctx.type_byte,
        ctx.type_string.as_deref(),
    )
    .map_err(|e| anyhow!("opening filesystem for write: {e}"))?;

    let parent = super::ls::resolve_path(&mut *fs, &parent_path)?;
    if !parent.is_directory() {
        bail!("parent is not a directory: {parent_path}");
    }

    // Early duplicate check so the user-facing message names the path.
    let already = fs
        .list_directory(&parent)
        .map_err(|e| anyhow!("list_directory: {e}"))?
        .into_iter()
        .any(|e| e.name == name);
    if already {
        bail!("{} already exists", args.path);
    }

    fs.create_directory(&parent, &name, &CreateDirectoryOptions::default())
        .map_err(|e| anyhow!("create_directory: {e}"))?;
    fs.sync_metadata()
        .map_err(|e| anyhow!("sync_metadata: {e}"))?;
    drop(fs);
    commit.commit()?;
    Ok(())
}
