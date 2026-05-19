//! `rb-cli mkdir IMG[@N] PATH` — create a new directory inside a filesystem.
//!
//! Phase A: HFS only. Phase B widens to every EditableFilesystem.

use anyhow::{anyhow, bail, Result};
use clap::Args;

use crate::cli::api::hfs::resolve_hfs_offset;
use crate::cli::img_at::ImageRef;
use crate::cli::io::open_image_rw;
use crate::cli::parse::split_mac_path;
use crate::fs::filesystem::{CreateDirectoryOptions, EditableFilesystem, Filesystem};
use crate::fs::hfs::HfsFilesystem;

#[derive(Debug, Args)]
pub struct MkdirArgs {
    /// Image reference (`path` or `path@N` for the 1-based partition index).
    pub image: ImageRef,

    /// Directory path to create. The parent must exist (no `-p`-style
    /// auto-creation in Phase A).
    pub path: String,
}

pub fn run(args: MkdirArgs) -> Result<()> {
    let (parent_path, name) = split_mac_path(&args.path)?;
    if name.is_empty() {
        bail!("directory path has no basename");
    }

    let mut file = open_image_rw(&args.image.path)?;
    let (offset, label) = resolve_hfs_offset(&mut file, args.image.partition)?;
    if let Some(l) = &label {
        eprintln!("{l}");
    }
    let mut fs = HfsFilesystem::open(file, offset).map_err(|e| anyhow!("opening HFS: {e}"))?;
    let parent =
        crate::cli::api::hfs::resolve_path(&mut fs, &parent_path).map_err(|e| anyhow!("{e}"))?;
    if !parent.is_directory() {
        bail!("parent is not a directory: {parent_path}");
    }

    // Reject duplicate names early — HFS's create_directory raises AlreadyExists
    // but the user-facing message benefits from spelling out the path.
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
    Ok(())
}
