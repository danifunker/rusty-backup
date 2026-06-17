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
    /// BasiliskII HFV — a flat classic-HFS volume, no partition table.
    /// Same on-disk bytes as `hfs`, but capped at 2047 MB and with the
    /// allocation block size auto-floored so the result is mountable by
    /// BasiliskII / SheepShaver and classic Mac OS.
    Hfv,
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

    /// HFS Catalog B-tree initial size in bytes (rounded up to a whole
    /// allocation block). When unset, scales with volume size like
    /// hformat (~0.5%, clump-aligned, 24-block floor). Ignored for other
    /// filesystems.
    #[arg(long = "catalog-size")]
    pub catalog_size: Option<String>,

    /// HFS Extents-overflow B-tree initial size in bytes (rounded up to a
    /// whole allocation block). When unset, ~half the catalog size.
    /// Ignored for other filesystems.
    #[arg(long = "extents-size")]
    pub extents_size: Option<String>,

    /// AFFS variant byte (0=OFS, 1=FFS, 2=OFS+intl, 3=FFS+intl,
    /// 4=OFS+dircache, 5=FFS+dircache). Defaults to 1 (FFS).
    #[arg(long = "affs-variant", default_value = "1")]
    pub affs_variant: u8,

    /// EFS only: approximate total inode count. The formatter scales its
    /// cylinder groups to hit roughly this many inodes. Mutually exclusive with
    /// `--bytes-per-inode`; default density is ~1 inode/4 KiB.
    #[arg(long, conflicts_with = "bytes_per_inode")]
    pub inodes: Option<u64>,

    /// EFS only: inode density in bytes per inode (smaller = more inodes),
    /// floored at one inode per 512-byte block. Mutually exclusive with
    /// `--inodes`.
    #[arg(long)]
    pub bytes_per_inode: Option<u64>,
}

pub fn run(args: NewArgs) -> Result<()> {
    if (args.inodes.is_some() || args.bytes_per_inode.is_some()) && args.fs != FsKind::Efs {
        anyhow::bail!("--inodes / --bytes-per-inode are only valid with --fs efs");
    }
    match args.fs {
        FsKind::Hfs => {
            let catalog_bytes = args
                .catalog_size
                .as_deref()
                .map(|s| parse_size(s).context("parsing --catalog-size"))
                .transpose()?
                .map(|v| v.min(u32::MAX as u64) as u32);
            let extents_bytes = args
                .extents_size
                .as_deref()
                .map(|s| parse_size(s).context("parsing --extents-size"))
                .transpose()?
                .map(|v| v.min(u32::MAX as u64) as u32);
            crate::cli::api::hfs::cmd_new_sized(
                args.image,
                &args.size,
                &args.name,
                args.block_size,
                catalog_bytes,
                extents_bytes,
            )
        }
        FsKind::Hfv => {
            // A flat HFV is just a blank classic-HFS volume with the BasiliskII
            // limits enforced: <= 2047 MB and a block size that keeps
            // total_blocks <= 65535. build_blank_hfv validates the cap.
            let explicit_bs = args.block_size;
            format_and_write(&args.image, &args.size, &args.name, move |size, name| {
                let bs = explicit_bs.unwrap_or_else(|| crate::fs::hfv::suggest_block_size(size));
                Ok(crate::fs::hfv::build_blank_hfv(size, bs, name)?)
            })
        }
        FsKind::Fat => format_and_write(&args.image, &args.size, &args.name, |size, name| {
            crate::fs::fat::create_blank_fat(size, Some(name))
        }),
        FsKind::Efs => write_blank_efs_image(
            &args.image,
            &args.size,
            &args.name,
            crate::fs::efs::resolve_bytes_per_inode(
                parse_size(&args.size).context("parsing --size")?,
                args.inodes,
                args.bytes_per_inode,
            ),
        ),
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

/// Format a bare EFS superfloppy by streaming only its non-zero regions to the
/// output file (the rest stays sparse), so large volumes never materialize the
/// whole image in memory. `bytes_per_inode` sets the inode density.
fn write_blank_efs_image(
    image: &std::path::Path,
    size_str: &str,
    name: &str,
    bytes_per_inode: u64,
) -> Result<()> {
    let size = parse_size(size_str).context("parsing --size")?;
    if size < 32 * 1024 {
        anyhow::bail!("EFS volume must be at least 32 KiB, got {size}");
    }
    let total_blocks_u64 = size / 512;
    if total_blocks_u64 > u32::MAX as u64 {
        anyhow::bail!("EFS volume size {size} exceeds u32 block range");
    }
    let total_blocks = total_blocks_u64 as u32;
    let disk_bytes = total_blocks as u64 * 512;

    let mut file =
        std::fs::File::create(image).with_context(|| format!("creating {}", image.display()))?;
    crate::fs::efs::write_blank_efs(&mut file, 0, total_blocks, name, bytes_per_inode)
        .with_context(|| format!("writing {}", image.display()))?;
    // Ensure the file is exactly the requested size even though the trailing
    // blocks past the replica superblock are sparse.
    file.set_len(disk_bytes)
        .with_context(|| format!("sizing {}", image.display()))?;
    log_stderr(format!(
        "wrote {} ({disk_bytes} bytes, volume {:?})",
        image.display(),
        name
    ));
    Ok(())
}
