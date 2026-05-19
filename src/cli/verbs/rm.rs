//! `rb-cli rm IMG[@N] PATH` — delete a file from a filesystem.

use anyhow::Result;
use clap::Args;

use crate::cli::img_at::ImageRef;

#[derive(Debug, Args)]
pub struct RmArgs {
    /// Image reference (`path` or `path@N` for the 1-based partition index).
    pub image: ImageRef,

    /// Path inside the filesystem.
    pub path: String,
}

pub fn run(args: RmArgs) -> Result<()> {
    crate::cli::api::hfs::cmd_rm(args.image.path, &args.path, args.image.partition)
}
