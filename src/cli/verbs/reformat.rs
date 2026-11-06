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
use crate::cli::logging::log_stderr;
use crate::cli::parse::parse_size;

/// The filesystems `reformat` can write in place.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum ReformatFs {
    /// Classic HFS (Mac OS Standard).
    Hfs,
}

#[derive(Debug, Args)]
pub struct ReformatArgs {
    /// Image reference (`path` or `path@N`).
    pub image: ImageRef,

    /// Filesystem to format the partition with. Only `hfs` is supported
    /// today.
    #[arg(long, value_enum)]
    pub fs: ReformatFs,

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
    let ReformatFs::Hfs = args.fs;

    // Open the image read-write (decoding a CHD / container as needed) and
    // resolve which partition we're targeting (offset + size). We don't open a
    // filesystem — the existing contents are about to be wiped.
    let (mut file, ctx, commit) =
        crate::cli::resolve::resolve_partition_rw(&args.image.path, args.image.partition)?;
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

    let (default_cat, default_ext) = fs_mac::hfs::default_btree_sizes(ctx.size, block_size);
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

    // A dry run first, so a bad name fails before anything on the partition is zeroed.
    let len = fs_mac::hfs::write_blank_hfs_sized_into(
        &mut NullTarget(0),
        0,
        ctx.size,
        block_size,
        args.name.as_str(),
        extents_bytes,
        catalog_bytes,
    )
    .map_err(|e| anyhow!("failed to format HFS volume: {e}"))?;
    if len > ctx.size {
        bail!(
            "internal error: formatted volume ({len} bytes) exceeds partition ({} bytes)",
            ctx.size
        );
    }

    // Zero the whole partition so no stale bytes survive, then stream the metadata over it.
    file.seek(SeekFrom::Start(ctx.offset))
        .context("seeking to partition offset")?;
    let zeros = vec![0u8; 1 << 20];
    let mut remaining = ctx.size;
    while remaining > 0 {
        let n = remaining.min(zeros.len() as u64) as usize;
        file.write_all(&zeros[..n])
            .context("zeroing the partition")?;
        remaining -= n as u64;
    }
    fs_mac::hfs::write_blank_hfs_sized_into(
        &mut file,
        ctx.offset,
        ctx.size,
        block_size,
        args.name.as_str(),
        extents_bytes,
        catalog_bytes,
    )
    .map_err(|e| anyhow!("failed to format HFS volume: {e}"))?;
    file.flush().ok();

    // Persist: flatten/re-encode if the image was a CHD / container; no-op for
    // a raw image (writes already landed).
    drop(file);
    commit.commit()?;

    log_stderr(format!(
        "reformatted partition @ offset {} ({} bytes) as HFS volume {:?} (block_size={})",
        ctx.offset, ctx.size, args.name, block_size
    ));
    Ok(())
}

/// A target that discards what is written, for a dry run of a streaming formatter.
struct NullTarget(u64);

impl Write for NullTarget {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Seek for NullTarget {
    fn seek(&mut self, to: SeekFrom) -> std::io::Result<u64> {
        if let SeekFrom::Start(n) = to {
            self.0 = n;
        }
        Ok(self.0)
    }
}
