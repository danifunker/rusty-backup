//! `rb-cli ls IMG[@N] [PATH]` — list a directory inside a filesystem.
//!
//! Phase A: routes to the HFS implementation only. Phase B generalizes
//! through `open_filesystem(...)` dispatch by partition type byte.

use anyhow::Result;
use clap::Args;

use crate::cli::img_at::ImageRef;

#[derive(Debug, Args)]
pub struct LsArgs {
    /// Image reference (`path` or `path@N` for the 1-based partition index).
    pub image: ImageRef,

    /// Path inside the filesystem (use `/` as the separator). Defaults to
    /// the volume root.
    #[arg(default_value = "/")]
    pub path: String,
}

pub fn run(args: LsArgs) -> Result<()> {
    crate::cli::api::hfs::cmd_ls(args.image.path, &args.path, args.image.partition)
}
