//! `rb-cli new-sgi-cdrom IMG.iso --size 600M [--name LABEL]` — Build an IRIX
//! EFS CD-ROM image: an SGI volume header + partition table at sector 0 with the
//! EFS filesystem in **slot 7 typed SYSV** (the IRIX EFS-CD convention; the
//! kernel's `IS_EFS()` accepts SYSV as well as EFS), using CD geometry
//! (1 head × 32 sectors). Mounted on IRIX with `mount -t efs <dev>s7 /CDROM`.
//!
//! Verified against real IRIX 5.3 / 6.5 distribution CDs, which use exactly this
//! shape. Unlike `new-sgi-hdd` (slot 0, hard-disk geometry) or `new --fs efs` (a
//! bare headerless EFS), this matches what IRIX expects on optical media. The
//! image is streamed to the file, so even a full CD never materializes in RAM.

use anyhow::{Context, Result};
use clap::Args;
use std::path::PathBuf;

use crate::cli::logging::log_stderr;
use crate::cli::parse::parse_size;
use crate::fs::efs::resolve_bytes_per_inode;
use crate::partition::sgi_hdd_builder::{write_sgi_efs_hdd, SgiHddOptions};

#[derive(Debug, Args)]
pub struct NewSgiCdromArgs {
    /// Image file to create (conventionally `.iso`). Overwritten if it exists.
    pub image: PathBuf,

    /// Disc size (plain bytes or `K`/`M`/`G` suffixes, e.g. `600M`). Rounded up
    /// to a whole 32-sector CD cylinder. Defaults to 600M (a CD-R). Keep it at
    /// or below your target media (~650-700 MiB for a CD).
    #[arg(long, default_value = "600M")]
    pub size: String,

    /// EFS volume label (up to 6 bytes; longer is truncated). Defaults to
    /// `rusty`.
    #[arg(long, default_value = "rusty")]
    pub name: String,

    /// Approximate total inode count for the EFS filesystem. Mutually exclusive
    /// with `--bytes-per-inode`. Default density is ~1 inode/4 KiB; real IRIX
    /// CDs are sparser (~32 KiB/inode), so pass a larger `--bytes-per-inode` (or
    /// fewer `--inodes`) if you only have a handful of large files.
    #[arg(long, conflicts_with = "bytes_per_inode")]
    pub inodes: Option<u64>,

    /// EFS inode density, in bytes per inode (smaller = more inodes). Floored at
    /// one inode per 512-byte block. Mutually exclusive with `--inodes`.
    #[arg(long)]
    pub bytes_per_inode: Option<u64>,
}

pub fn run(args: NewSgiCdromArgs) -> Result<()> {
    let size_bytes = parse_size(&args.size).context("parsing --size")?;
    let mut opts = SgiHddOptions::new_cdrom(size_bytes, args.name.clone());
    opts.bytes_per_inode = resolve_bytes_per_inode(size_bytes, args.inodes, args.bytes_per_inode);

    // Stream straight to the file: only the volume header + EFS metadata are
    // written; the rest stays sparse, so a full CD never materializes in RAM.
    let mut file = std::fs::File::create(&args.image)
        .with_context(|| format!("creating {}", args.image.display()))?;
    let layout = write_sgi_efs_hdd(&mut file, &opts)
        .with_context(|| format!("writing {}", args.image.display()))?;
    file.set_len(layout.disk_bytes)
        .with_context(|| format!("sizing {}", args.image.display()))?;

    log_stderr(format!(
        "wrote {} ({} bytes, {} MiB) - SGI volume header + EFS CD-ROM",
        args.image.display(),
        layout.disk_bytes,
        layout.disk_bytes / (1024 * 1024),
    ));
    log_stderr(format!(
        "  geometry: {} cyls x {} heads x {} secs/trk x 512 (CD geometry)",
        layout.cylinders, layout.heads, layout.sectors_per_track,
    ));
    log_stderr(format!(
        "  slot 8 VOLHDR: sectors 0..{}   slot 10 VOLUME: sectors 0..{} (whole disc)",
        layout.volhdr_sectors, layout.disk_sectors,
    ));
    log_stderr(format!(
        "  slot 7 EFS (type SYSV): sectors {}..{} ({} MiB)",
        layout.efs_first_sector,
        layout.efs_first_sector + layout.efs_sectors,
        (layout.efs_sectors * 512) / (1024 * 1024),
    ));
    log_stderr(format!(
        "  populate it with: rb-cli put {}@1 host/file /file   (then ls / get / fsck)",
        args.image.display(),
    ));
    log_stderr("  on IRIX: mount -t efs -o ro /dev/dsk/dks0d<N>s7 /CDROM");
    Ok(())
}
