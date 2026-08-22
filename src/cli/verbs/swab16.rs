//! `rb-cli swab16 IN [OUT]` — swap the two bytes of every 16-bit word in a
//! disk image.
//!
//! Some vintage disk controllers move 16-bit words with the two bytes reversed,
//! so an image captured through one is byte-swapped throughout. This verb is
//! the format-agnostic fix-up: it rewrites the whole file and nothing else.
//!
//! The transform is an involution, so the same invocation converts in either
//! direction. To make that verifiable rather than blind, the verb probes the
//! partition table before and after and reports what it saw; probing is
//! best-effort and never fails the conversion.
//!
//! See `crate::rbformats::swab` for the transform and
//! `crate::partition::sgi_dklabel` for the SGI case that motivated it.

use anyhow::{bail, Context, Result};
use clap::Args;
use std::fs::File;
use std::path::{Path, PathBuf};

use crate::cli::logging::{log_stderr, out_stdout};
use crate::partition::{format_size, PartitionTable};
use crate::rbformats::swab;

#[derive(Debug, Args)]
pub struct Swab16Args {
    /// Image to convert.
    pub input: PathBuf,
    /// Destination image. Omit when passing `--in-place`.
    pub output: Option<PathBuf>,
    /// Rewrite INPUT itself instead of writing a separate OUTPUT.
    #[arg(long)]
    pub in_place: bool,
    /// Overwrite OUTPUT when it already exists.
    #[arg(long)]
    pub force: bool,
}

pub fn run(args: Swab16Args) -> Result<()> {
    if !args.input.is_file() {
        bail!("{} is not a file", args.input.display());
    }
    let out = match (&args.output, args.in_place) {
        (Some(_), true) => bail!("pass either OUTPUT or --in-place, not both"),
        (None, false) => bail!("give an OUTPUT path, or pass --in-place to rewrite INPUT"),
        (Some(o), false) => Some(o.clone()),
        (None, true) => None,
    };

    if let Some(o) = &out {
        if o.exists() && !args.force {
            bail!("{} already exists; pass --force to overwrite", o.display());
        }
        if same_file(&args.input, o) {
            bail!("OUTPUT is the same file as INPUT; use --in-place instead");
        }
    }

    let before = describe(&args.input);
    let len = args.input.metadata().map(|m| m.len()).unwrap_or(0);
    if len % 2 == 1 {
        log_stderr(format!(
            "warning: {} has an odd length ({len} bytes); the trailing byte has no \
             word to pair with and is copied through unchanged",
            args.input.display()
        ));
    }

    let target = out.clone().unwrap_or_else(|| args.input.clone());
    log_stderr(format!(
        "rb-cli swab16: {} ({}, detected: {before}) -> {}",
        args.input.display(),
        format_size(len),
        target.display()
    ));

    let mut last_pct: i32 = -1;
    let mut progress = |done: u64, total: u64| {
        if total == 0 {
            return;
        }
        let pct = ((done as f64 / total as f64) * 100.0) as i32;
        if pct / 10 != last_pct / 10 {
            log_stderr(format!(
                "  progress: {pct:>3}% ({} / {})",
                format_size(done),
                format_size(total)
            ));
            last_pct = pct;
        }
    };

    let written = match &out {
        Some(o) => swab::swab16_file(&args.input, o, &mut progress)
            .with_context(|| format!("swapping {} into {}", args.input.display(), o.display()))?,
        None => swab::swab16_file_in_place(&args.input, &mut progress)
            .with_context(|| format!("swapping {} in place", args.input.display()))?,
    };

    let after = describe(&target);
    out_stdout(format!(
        "swab16: wrote {} to {} (detected before: {before}, after: {after})",
        format_size(written),
        target.display()
    ));
    if before == after && before != UNRECOGNIZED {
        log_stderr(
            "note: the same table type was detected both before and after — this image may \
             read correctly in either orientation",
        );
    }
    Ok(())
}

const UNRECOGNIZED: &str = "unrecognized";

/// Best-effort probe so the caller can see the orientation flip take effect.
/// Never fails: an unreadable or unknown image just reports as such.
fn describe(path: &Path) -> String {
    let mut f = match File::open(path) {
        Ok(f) => f,
        Err(_) => return "unreadable".to_string(),
    };
    match PartitionTable::detect(&mut f) {
        Ok(PartitionTable::None { fs_hint, .. }) => {
            if fs_hint == "Unknown" {
                UNRECOGNIZED.to_string()
            } else {
                format!("no partition table, {fs_hint} filesystem")
            }
        }
        Ok(t) => match t.byte_order_name() {
            Some(order) => format!("{} ({order})", t.type_name()),
            None => t.type_name().to_string(),
        },
        Err(_) => UNRECOGNIZED.to_string(),
    }
}

/// Whether two paths name the same file, so `--in-place` isn't spelled as a
/// self-referential OUTPUT. Falls back to a path compare when either is absent.
fn same_file(a: &Path, b: &Path) -> bool {
    match (a.canonicalize(), b.canonicalize()) {
        (Ok(x), Ok(y)) => x == y,
        _ => a == b,
    }
}
