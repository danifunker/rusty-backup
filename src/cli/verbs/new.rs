//! `rb-cli new IMG` — create a blank single-partition image.
//!
//! Phase A scope: only HFS via `--fs hfs`. Phase B widens to
//! `--fs fat|hfsplus|pfs3|sfs|...` and Phase D adds `--pt apm|mbr|gpt`
//! for PT-wrapped single-partition images. Multi-partition images go
//! through the `batch` verb (Phase D).

use anyhow::{bail, Result};
use clap::{Args, ValueEnum};
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum FsKind {
    /// Classic HFS (MFS-compatible Mac OS Standard).
    Hfs,
}

#[derive(Debug, Args)]
pub struct NewArgs {
    /// Image file to create. Overwritten if it already exists.
    pub image: PathBuf,

    /// Filesystem to format.
    #[arg(long, value_enum)]
    pub fs: FsKind,

    /// Volume size, accepting plain bytes or `K`/`KiB`/`M`/`MiB`/`G`/`GiB`
    /// suffixes (e.g. `800K`, `5M`). Defaults to 800K (an 800 KiB floppy).
    #[arg(long, default_value = "800K")]
    pub size: String,

    /// HFS volume name (1..=27 Mac Roman bytes for HFS). Defaults to `MacIIBench`.
    #[arg(long, default_value = "MacIIBench")]
    pub name: String,

    /// HFS allocation block size in bytes. Must be a non-zero multiple of
    /// 512. When unset, the smallest size that keeps `total_blocks <=
    /// 65535` is chosen automatically.
    #[arg(long = "block-size")]
    pub block_size: Option<u32>,
}

pub fn run(args: NewArgs) -> Result<()> {
    match args.fs {
        FsKind::Hfs => {
            crate::cli::api::hfs::cmd_new(args.image, &args.size, &args.name, args.block_size)
        }
        #[allow(unreachable_patterns)]
        _ => bail!("--fs {:?} not yet supported (Phase B)", args.fs),
    }
}
