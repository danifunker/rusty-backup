//! `rb-cli put` — copy a host file (or zero-fill) into a filesystem.
//!
//! Three shapes:
//! - `put IMG[@N] HOST DST [--type X --creator Y --force]` — normal copy.
//! - `put IMG[@N] --zero BYTES DST [--type X --creator Y --force]` —
//!   pre-allocate a zero-filled file.
//! - `put IMG[@N] --boot BB_FILE` — write the 1024-byte boot block region
//!   verbatim. Mutually exclusive with everything else.
//!
//! Phase A: HFS only. Phase B generalizes via the editable FS dispatch.

use anyhow::{bail, Result};
use clap::Args;
use std::path::PathBuf;

use crate::cli::img_at::ImageRef;

#[derive(Debug, Args)]
pub struct PutArgs {
    /// Image reference (`path` or `path@N` for the 1-based partition index).
    pub image: ImageRef,

    /// Host file to copy. Required unless `--zero` or `--boot` is given.
    pub host_file: Option<PathBuf>,

    /// Destination path inside the filesystem. Required unless `--boot`.
    pub dst: Option<String>,

    /// Pre-allocate N zero bytes instead of copying a host file.
    /// Mutually exclusive with `host_file`.
    #[arg(long, conflicts_with_all = ["host_file", "boot"])]
    pub zero: Option<u64>,

    /// Write the 1024-byte boot-block region of the image verbatim.
    /// Mutually exclusive with all the per-file options.
    #[arg(long, conflicts_with_all = ["host_file", "dst", "zero", "type_code", "creator", "force"])]
    pub boot: Option<PathBuf>,

    /// HFS 4-character type code (HFS / HFS+ only). Defaults to `BINA`.
    #[arg(long = "type", default_value = "BINA")]
    pub type_code: String,

    /// HFS 4-character creator code (HFS / HFS+ only). Defaults to `????`.
    #[arg(long, default_value = "????")]
    pub creator: String,

    /// Overwrite an existing entry at the destination path.
    #[arg(long)]
    pub force: bool,
}

pub fn run(args: PutArgs) -> Result<()> {
    if let Some(bb_file) = args.boot {
        return crate::cli::api::hfs::cmd_put_boot(args.image.path, bb_file);
    }

    let dst = match args.dst {
        Some(d) => d,
        None => bail!("destination path required (or pass --boot for boot-block mode)"),
    };

    if let Some(n) = args.zero {
        return crate::cli::api::hfs::cmd_put(
            args.image.path,
            None,
            &dst,
            &args.type_code,
            &args.creator,
            Some(n),
            args.force,
            args.image.partition,
        );
    }

    let host = args.host_file.ok_or_else(|| {
        anyhow::anyhow!(
            "host file required (or pass --zero N for zero-fill, --boot FILE for boot blocks)"
        )
    })?;
    crate::cli::api::hfs::cmd_put(
        args.image.path,
        Some(host),
        &dst,
        &args.type_code,
        &args.creator,
        None,
        args.force,
        args.image.partition,
    )
}
