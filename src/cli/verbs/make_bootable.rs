//! `rb-cli make-bootable IMG [--boot-from DONOR] [--driver-from DONOR]
//! [--bless PATH] [--dry-run]` — auto-detect what a Mac disk needs to become
//! bootable and apply only the missing pieces.
//!
//! Classifies the disk (flat HFV vs full APM disk), then ensures — only where
//! absent — a SCSI driver + DDR (APM disks), boot blocks (copied from
//! `--boot-from`), and a blessed System Folder. Idempotent. Wraps
//! [`crate::fs::make_bootable`]. A flat HFV is kept flat (no APM wrapper is
//! added); converting flat -> full APM is the separate Expand/Export path.

use anyhow::{bail, Result};
use clap::Args;
use std::path::PathBuf;

use crate::cli::logging::out_stdout;
use crate::fs::make_bootable::{
    assess_bootability, make_bootable, BootDiskKind, DriverSource, MakeBootableOptions,
};

#[derive(Debug, Args)]
pub struct MakeBootableArgs {
    /// Disk image to make bootable, in place.
    pub image: PathBuf,

    /// Bootable donor disk to copy boot blocks from, if the target lacks them
    /// (its classic-HFS volume is auto-located and `'LK'`-validated). Without
    /// it, missing boot blocks are reported but not synthesized.
    #[arg(long = "boot-from", value_name = "DONOR")]
    pub boot_from: Option<PathBuf>,

    /// For a full (APM) disk missing a SCSI driver: extract it from a donor
    /// Apple-formatted disk instead of using the bundled driver.
    #[arg(long = "driver-from", value_name = "DONOR")]
    pub driver_from: Option<PathBuf>,

    /// Absolute Mac path of the folder to bless (e.g. `/System Folder`).
    /// Defaults to auto-blessing a root folder named "System Folder".
    #[arg(long = "bless", value_name = "PATH")]
    pub bless: Option<String>,

    /// Report what would change without writing anything.
    #[arg(long = "dry-run")]
    pub dry_run: bool,
}

pub fn run(args: MakeBootableArgs) -> Result<()> {
    // Show the starting state so the user sees what was detected.
    let before = assess_bootability(&args.image)?;
    let kind = match before.kind {
        BootDiskKind::FlatHfs => "flat HFS image (flat HFV — no APM wrapper)",
        BootDiskKind::ApmHfs => "full APM disk (Apple_HFS partition)",
    };
    out_stdout(format!(
        "Disk: {} [{}]",
        args.image.display(),
        before.volume_name.as_deref().unwrap_or("?")
    ));
    out_stdout(format!("Kind: {kind}"));

    let opts = MakeBootableOptions {
        donor: args.boot_from,
        driver: match args.driver_from {
            Some(p) => DriverSource::Donor(p),
            None => DriverSource::Builtin,
        },
        bless_path: args.bless,
        dry_run: args.dry_run,
    };

    let report = make_bootable(&args.image, &opts)?;

    let verb = if args.dry_run { "Would" } else { "Done" };
    let tag = if args.dry_run { "plan" } else { "ok" };
    for s in &report.applied {
        out_stdout(format!("  [{tag}] {s}"));
    }
    for s in &report.skipped {
        out_stdout(format!("  [skip] {s}"));
    }
    for s in &report.still_missing {
        out_stdout(format!("  [MISSING] {s}"));
    }

    if report.now_bootable {
        out_stdout(format!("{verb}: disk is bootable."));
        Ok(())
    } else if report.still_missing.is_empty() {
        out_stdout(format!("{verb}: no changes needed."));
        Ok(())
    } else {
        bail!("disk is not yet bootable; see the [MISSING] item(s) above");
    }
}
