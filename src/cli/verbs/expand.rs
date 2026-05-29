//! `rb-cli expand IMG@N --size BYTES --block-size BS --output OUT` —
//! HFS expand-block-size flow: clone a classic-HFS volume into a fresh
//! blank volume of a chosen size and allocation block size, then either
//! wrap the result in a new APM disk image (default) or, with `--to-hfv`,
//! write it as a flat BasiliskII HFV (bare volume, no partition table,
//! capped at 2047 MB).
//!
//! Thin CLI over [`crate::model::hfs_expand_runner`], the same worker
//! the GUI's "Expand HFS Volume…" dialog uses. The source disk is
//! opened read-only; the output is always a brand-new file at
//! `--output`.
//!
//! **Scope.** Classic HFS only (the only filesystem with the
//! 65535-block-per-volume limit that benefits from re-flooring). HFS+,
//! HFSX, and non-Mac filesystems are out of scope — for those, use
//! `rb-cli restore` to lay out a fresh disk with different sizes.

use anyhow::{bail, Context, Result};
use clap::Args;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::cli::img_at::ImageRef;
use crate::cli::logging::log_stderr;
use crate::cli::parse::parse_size;
use crate::cli::resolve::resolve_partition_ro;
use crate::model::hfs_expand_runner::{
    max_volume_for_block_size, start_hfs_expand, suggest_block_size, summarize_source,
    ExpandOutput, BLOCK_SIZE_CHOICES,
};
use crate::model::status::ExpandStatus;
use crate::partition::format_size;

#[derive(Debug, Args)]
pub struct ExpandArgs {
    /// Source image reference (`path` or `path@N` for the classic HFS partition).
    pub image: ImageRef,

    /// Target volume size in bytes. Accepts suffixes (`K`, `M`, `G`).
    #[arg(long)]
    pub size: String,

    /// Allocation block size in bytes. One of: 4096, 8192, 16384, 32768,
    /// 65536. If omitted, picks the smallest block size whose 65535-block
    /// ceiling can hold `--size`.
    #[arg(long = "block-size")]
    pub block_size: Option<u32>,

    /// Destination path for the new image. Created (or truncated).
    #[arg(long)]
    pub output: PathBuf,

    /// Write a flat BasiliskII HFV (bare classic-HFS volume, no partition
    /// table) instead of an APM disk image. Capped at 2047 MB. Use this to
    /// produce a `.hfv` for BasiliskII / SheepShaver.
    #[arg(long = "to-hfv")]
    pub to_hfv: bool,
}

pub fn run(args: ExpandArgs) -> Result<()> {
    let target_size = parse_size(&args.size).context("parsing --size")?;

    let (_file, ctx) = resolve_partition_ro(&args.image.path, args.image.partition)?;
    // Drop the resolve handle — the runner reopens the source itself
    // (it needs to traverse the disk multiple times and own its own readers).
    drop(_file);

    let source = summarize_source(&args.image.path, ctx.offset, ctx.size)
        .context("inspecting source HFS volume")?;
    log_stderr(format!(
        "source: {:?} ({}, {} blocks of {} bytes, {} files / {} dirs)",
        source.volume_name,
        format_size(source.used_bytes),
        source.partition_size / 512,
        source.source_block_size,
        source.file_count,
        source.dir_count,
    ));

    let block_size = match args.block_size {
        Some(bs) => {
            if !BLOCK_SIZE_CHOICES.contains(&bs) {
                bail!(
                    "--block-size {bs} not allowed; choose one of {:?}",
                    BLOCK_SIZE_CHOICES
                );
            }
            bs
        }
        None => suggest_block_size(target_size),
    };
    let ceiling = max_volume_for_block_size(block_size);
    if target_size > ceiling {
        bail!(
            "--size {} exceeds the HFS 65535-block ceiling of {} for --block-size {}",
            format_size(target_size),
            format_size(ceiling),
            block_size
        );
    }
    if args.to_hfv && target_size > crate::fs::hfv::HFV_MAX_BYTES {
        bail!(
            "--size {} exceeds the BasiliskII HFV limit of {} (2047 MB); HFV volumes must be classic-HFS-mountable",
            format_size(target_size),
            format_size(crate::fs::hfv::HFV_MAX_BYTES)
        );
    }
    if target_size < source.used_bytes {
        bail!(
            "--size {} is smaller than the source's used bytes ({}); expand requires a target that holds the data",
            format_size(target_size),
            format_size(source.used_bytes)
        );
    }

    let output_mode = if args.to_hfv {
        ExpandOutput::FlatHfv
    } else {
        ExpandOutput::ApmDisk
    };
    log_stderr(format!(
        "rb-cli expand: target {} at {} byte blocks -> {} ({})",
        format_size(target_size),
        block_size,
        args.output.display(),
        if args.to_hfv { "flat HFV" } else { "APM disk" }
    ));

    let status = start_hfs_expand(source, target_size, block_size, args.output, output_mode);
    drain(status)
}

fn drain(status: Arc<Mutex<ExpandStatus>>) -> Result<()> {
    let mut last_step = String::new();
    loop {
        std::thread::sleep(Duration::from_millis(250));
        let (logs, step, finished, error) = match status.lock() {
            Ok(mut s) => (
                s.log_messages.drain(..).collect::<Vec<String>>(),
                s.current_step.clone(),
                s.finished,
                s.error.clone(),
            ),
            Err(_) => bail!("expand worker poisoned its status mutex"),
        };
        if step != last_step {
            log_stderr(format!("step: {step}"));
            last_step = step;
        }
        for line in logs {
            log_stderr(format!("  {line}"));
        }
        if finished {
            if let Some(e) = error {
                bail!("expand failed: {e}");
            }
            return Ok(());
        }
    }
}
