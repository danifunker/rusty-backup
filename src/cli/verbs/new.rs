//! `rb-cli new IMG --fs {hfs|fat|efs|affs}` — create a blank
//! single-partition image (superfloppy). Phase D will add `--pt
//! apm|mbr|gpt` for partition-table-wrapped single-partition images;
//! multi-partition images go through the `batch` verb.

use anyhow::{Context, Result};
use clap::{Args, ValueEnum};
use std::path::PathBuf;

use crate::cli::logging::log_stderr;
use crate::cli::parse::parse_size;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum FsKind {
    /// Classic HFS (Mac OS Standard).
    Hfs,
    /// FAT12 (≤ 32 MiB) or FAT16 (≤ 2 GiB), auto-selected by size.
    Fat,
    /// IRIX EFS (single cylinder group).
    Efs,
    /// Amiga FFS / OFS (variant selected via --affs-variant).
    Affs,
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

    /// Volume label/name. Defaults to `rusty-backup`. HFS: up to 27 Mac
    /// Roman bytes. FAT: up to 11 chars (uppercased; non-ASCII → `_`).
    /// EFS: 6-byte fname/fpack. AFFS: up to 30 bytes.
    #[arg(long, default_value = "rusty-backup")]
    pub name: String,

    /// HFS allocation block size in bytes. Must be a non-zero multiple of
    /// 512. When unset, the smallest size that keeps `total_blocks <=
    /// 65535` is chosen automatically. Ignored for other filesystems.
    #[arg(long = "block-size")]
    pub block_size: Option<u32>,

    /// AFFS variant byte (0=OFS, 1=FFS, 2=OFS+intl, 3=FFS+intl,
    /// 4=OFS+dircache, 5=FFS+dircache). Defaults to 1 (FFS).
    #[arg(long = "affs-variant", default_value = "1")]
    pub affs_variant: u8,
}

pub fn run(args: NewArgs) -> Result<()> {
    match args.fs {
        FsKind::Hfs => {
            crate::cli::api::hfs::cmd_new(args.image, &args.size, &args.name, args.block_size)
        }
        FsKind::Fat => format_and_write(&args.image, &args.size, &args.name, |size, name| {
            crate::fs::fat::create_blank_fat(size, Some(name))
        }),
        FsKind::Efs => format_and_write(&args.image, &args.size, &args.name, |size, name| {
            crate::fs::efs::create_blank_efs(size, name)
        }),
        FsKind::Affs => {
            let variant = args.affs_variant;
            format_and_write(&args.image, &args.size, &args.name, |size, name| {
                crate::fs::affs::create_blank_affs(size, variant, name)
            })
        }
    }
}

fn format_and_write(
    image: &std::path::Path,
    size_str: &str,
    name: &str,
    formatter: impl FnOnce(u64, &str) -> anyhow::Result<Vec<u8>>,
) -> Result<()> {
    let size = parse_size(size_str).context("parsing --size")?;
    let bytes = formatter(size, name)?;
    std::fs::write(image, &bytes).with_context(|| format!("writing {}", image.display()))?;
    log_stderr(format!(
        "wrote {} ({} bytes, volume {:?})",
        image.display(),
        bytes.len(),
        name
    ));
    Ok(())
}
