//! `rb-cli reformat IMG[@N] --fs hfs --name NAME` — wipe a partition's
//! contents in place, leaving the surrounding partition table (APM/MBR/
//! GPT) untouched.
//!
//! Equivalent to `hformat -l NAME extracted_partition.dsk` but operating
//! on a partition inside a wrapped image: a fresh HFS volume sized to the
//! partition is built and overlaid at the partition offset; any trailing
//! slack (the volume rounds down to whole allocation blocks) is zeroed so
//! no stale catalog/template state survives.

use anyhow::{anyhow, bail, Context, Result};
use clap::Args;
use std::io::{Seek, SeekFrom, Write};

use crate::cli::img_at::ImageRef;
use crate::cli::io::open_image_rw;
use crate::cli::logging::log_stderr;
use crate::cli::parse::parse_size;
use crate::cli::verbs::new::FsKind;

#[derive(Debug, Args)]
pub struct ReformatArgs {
    /// Image reference (`path` or `path@N`).
    pub image: ImageRef,

    /// Filesystem to format the partition with. Only `hfs` is supported
    /// today.
    #[arg(long, value_enum)]
    pub fs: FsKind,

    /// New volume name. HFS: up to 27 Mac Roman bytes.
    #[arg(long, default_value = "rusty-backup")]
    pub name: String,

    /// HFS allocation block size in bytes (non-zero multiple of 512).
    /// Defaults to the smallest size that keeps total_blocks <= 65535.
    #[arg(long = "block-size")]
    pub block_size: Option<u32>,

    /// HFS Catalog B-tree initial size in bytes. Defaults to hformat-style
    /// scaling (~0.5% of the partition).
    #[arg(long = "catalog-size")]
    pub catalog_size: Option<String>,

    /// HFS Extents-overflow B-tree initial size in bytes. Defaults to
    /// ~half the catalog size.
    #[arg(long = "extents-size")]
    pub extents_size: Option<String>,
}

pub fn run(args: ReformatArgs) -> Result<()> {
    if args.fs != FsKind::Hfs {
        bail!("reformat: only --fs hfs is supported today");
    }

    // Resolve which partition we're targeting (offset + size) without
    // opening a filesystem — the existing contents are about to be wiped.
    let (_probe, ctx) =
        crate::cli::resolve::resolve_partition_ro(&args.image.path, args.image.partition)?;
    log_stderr(&ctx.label);

    if ctx.size < 1024 {
        bail!(
            "reformat: partition is only {} bytes — too small for an HFS volume",
            ctx.size
        );
    }

    let block_size = match args.block_size {
        Some(bs) => {
            if bs == 0 || bs % 512 != 0 {
                bail!("block-size must be a non-zero multiple of 512 (got {bs})");
            }
            bs
        }
        None => crate::cli::parse::pick_block_size(ctx.size),
    };

    let (default_cat, default_ext) = crate::fs::hfs::default_btree_sizes(ctx.size, block_size);
    let catalog_bytes = args
        .catalog_size
        .as_deref()
        .map(|s| parse_size(s).context("parsing --catalog-size"))
        .transpose()?
        .map(|v| v.min(u32::MAX as u64) as u32)
        .unwrap_or(default_cat);
    let extents_bytes = args
        .extents_size
        .as_deref()
        .map(|s| parse_size(s).context("parsing --extents-size"))
        .transpose()?
        .map(|v| v.min(u32::MAX as u64) as u32)
        .unwrap_or(default_ext);

    let volume = crate::fs::hfs::create_blank_hfs_sized(
        ctx.size,
        block_size,
        &args.name,
        extents_bytes,
        catalog_bytes,
    )
    .map_err(|e| anyhow!("failed to format HFS volume: {e}"))?;

    if volume.len() as u64 > ctx.size {
        // create_blank_hfs_sized must never exceed the requested size; this
        // is a defensive guard so we never clobber neighbouring partitions.
        bail!(
            "internal error: formatted volume ({} bytes) exceeds partition ({} bytes)",
            volume.len(),
            ctx.size
        );
    }

    let mut file = open_image_rw(&args.image.path)?;
    file.seek(SeekFrom::Start(ctx.offset))
        .context("seeking to partition offset")?;
    file.write_all(&volume)
        .context("writing fresh HFS volume")?;

    // Zero any trailing slack between the volume's end and the partition's
    // end so no stale template/catalog bytes survive in the partition.
    let slack = ctx.size - volume.len() as u64;
    if slack > 0 {
        let zeros = vec![0u8; 64 * 1024];
        let mut remaining = slack;
        while remaining > 0 {
            let n = remaining.min(zeros.len() as u64) as usize;
            file.write_all(&zeros[..n])
                .context("zero-filling partition slack")?;
            remaining -= n as u64;
        }
    }
    file.flush().ok();

    log_stderr(format!(
        "reformatted partition @ offset {} ({} bytes) as HFS volume {:?} (block_size={})",
        ctx.offset, ctx.size, args.name, block_size
    ));
    Ok(())
}
