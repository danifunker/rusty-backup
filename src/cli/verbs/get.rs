//! `rb-cli get IMG[@N] SRC HOST` — extract a file from a filesystem.

use anyhow::Result;
use clap::Args;
use std::path::PathBuf;

use crate::cli::img_at::ImageRef;

#[derive(Debug, Args)]
pub struct GetArgs {
    /// Image reference (`path` or `path@N` for the 1-based partition index).
    pub image: ImageRef,

    /// Source path inside the filesystem.
    pub src: String,

    /// Destination path on the host. A trailing `/` means "into this
    /// directory" (Phase B will honor this for recursive get); for now
    /// `get` only accepts file targets.
    pub dst: PathBuf,
}

pub fn run(args: GetArgs) -> Result<()> {
    crate::cli::api::hfs::cmd_get(args.image.path, &args.src, args.dst, args.image.partition)
}
