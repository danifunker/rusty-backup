//! `rb-cli new-sgi-hdd IMG --size 50M [--name LABEL] [--heads N --sectors N]`
//! — Build a dvh-wrapped IRIX hard-disk image: an SGI volume header +
//! partition table at sector 0 wrapping a formatted EFS root partition,
//! mountable by IRIX 5.3–6.5 as a SCSI HDD.
//!
//! Wraps [`crate::partition::sgi_hdd_builder::build_sgi_efs_hdd`]. Unlike
//! `new --fs efs` — which makes a *bare* EFS superfloppy (an EFS CD-ROM that
//! IRIX reads with `mount -t efs`) — this produces a real hard disk that `fx`
//! / `prtvtoc` and the IRIX disk driver recognize, populatable through the
//! normal verbs (`put IMG@1 host/file /file`, `ls`, `get`, `fsck`).

use anyhow::{Context, Result};
use clap::{Args, ValueEnum};
use std::path::PathBuf;

use crate::cli::logging::log_stderr;
use crate::cli::parse::parse_size;
use crate::partition::sgi_hdd_builder::{
    build_sgi_efs_hdd, SgiHddOptions, DEFAULT_HEADS, DEFAULT_SECTORS_PER_TRACK,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum SgiFs {
    /// IRIX EFS root partition (IRIX 5.3–6.5). The only root filesystem
    /// supported today; XFS is a future addition.
    Efs,
}

#[derive(Debug, Args)]
pub struct NewSgiHddArgs {
    /// Image file to create. Overwritten if it already exists.
    pub image: PathBuf,

    /// Disk size (plain bytes or `K`/`KiB`/`M`/`MiB`/`G`/`GiB` suffixes, e.g.
    /// `50M`). Rounded up to a whole cylinder. Defaults to 50M.
    #[arg(long, default_value = "50M")]
    pub size: String,

    /// EFS volume label (up to 6 bytes; longer is truncated). Defaults to
    /// `rusty`.
    #[arg(long, default_value = "rusty")]
    pub name: String,

    /// Root filesystem to format. Only `efs` is supported today.
    #[arg(long, value_enum, default_value = "efs")]
    pub fs: SgiFs,

    /// Heads (tracks per cylinder). Geometry only governs cylinder alignment
    /// and the values `fx`/`prtvtoc` report; EFS uses its own layout.
    #[arg(long, default_value_t = DEFAULT_HEADS)]
    pub heads: u16,

    /// Sectors per track (512-byte sectors). Default 128, which with 16 heads
    /// gives clean 1 MiB cylinders.
    #[arg(long, default_value_t = DEFAULT_SECTORS_PER_TRACK)]
    pub sectors: u16,
}

pub fn run(args: NewSgiHddArgs) -> Result<()> {
    match args.fs {
        SgiFs::Efs => { /* the only supported root filesystem today */ }
    }

    let size_bytes = parse_size(&args.size).context("parsing --size")?;
    let opts = SgiHddOptions {
        size_bytes,
        name: args.name.clone(),
        heads: args.heads,
        sectors_per_track: args.sectors,
    };
    let (image, layout) = build_sgi_efs_hdd(&opts)?;
    std::fs::write(&args.image, &image)
        .with_context(|| format!("writing {}", args.image.display()))?;

    log_stderr(format!(
        "wrote {} ({} bytes, {} MiB) - SGI volume header + EFS root",
        args.image.display(),
        layout.disk_bytes,
        layout.disk_bytes / (1024 * 1024),
    ));
    log_stderr(format!(
        "  geometry: {} cyls x {} heads x {} secs/trk x 512 ({} sectors, {}-sector cylinders)",
        layout.cylinders,
        layout.heads,
        layout.sectors_per_track,
        layout.disk_sectors,
        layout.cylinder_sectors,
    ));
    log_stderr(format!(
        "  slot 8 VOLHDR: sectors 0..{}   slot 10 VOLUME: sectors 0..{} (whole disk)",
        layout.volhdr_sectors, layout.disk_sectors,
    ));
    log_stderr(format!(
        "  slot 0 EFS root: sectors {}..{} ({} sectors, {} MiB)",
        layout.efs_first_sector,
        layout.efs_first_sector + layout.efs_sectors,
        layout.efs_sectors,
        (layout.efs_sectors * 512) / (1024 * 1024),
    ));
    log_stderr(format!(
        "  populate it with: rb-cli put {}@1 host/file /file   (then ls / get / fsck)",
        args.image.display(),
    ));
    log_stderr(
        "  note: the header round-trips through rb-cli's SGI parser; real IRIX \
         fx/prtvtoc validation is unverified without hardware/emulator.",
    );
    Ok(())
}
