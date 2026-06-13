//! `rb-cli mac-scsi-bless IMG [--driver-from DONOR | --driver RAW |
//! --builtin-driver] [--force-cksum-zero]` — install an Apple SCSI driver +
//! Driver Descriptor Record so a classic-Mac ROM (e.g. Quadra 800) registers
//! the disk over SCSI.
//!
//! Wraps [`crate::fs::mac_scsi_bless::bless_apm_disk`]. Analogous to
//! `new-x68k-hdd`, but for classic-Mac APM disks. Operates in place and never
//! moves partition data.
//!
//! Note: this makes the disk *readable* by the ROM (driver registration). It
//! does not change HFS boot-block behavior — whether a System (or a custom
//! `bbEntry`) actually boots is independent of driver installation.

use anyhow::{Context, Result};
use clap::Args;
use std::io::{Seek, SeekFrom};
use std::path::PathBuf;

use crate::cli::io::{open_image_ro, open_image_rw};
use crate::cli::logging::log_stderr;
use crate::fs::mac_scsi_bless::{bless_apm_disk, BlessOptions, MacScsiDriver};

#[derive(Debug, Args)]
pub struct MacScsiBlessArgs {
    /// APM disk image to make SCSI-bootable, in place.
    pub image: PathBuf,

    /// Extract the driver from a donor Apple-formatted disk's `Apple_Driver*`
    /// partition (most faithful — carries that disk's exact boot metadata).
    #[arg(long = "driver-from", value_name = "DONOR", conflicts_with_all = ["driver", "builtin_driver"])]
    pub driver_from: Option<PathBuf>,

    /// Use a raw driver image file (advanced; `pmBootCksum` is unknown for an
    /// arbitrary driver, so it is written as 0 — see `--force-cksum-zero`).
    #[arg(long = "driver", value_name = "FILE", conflicts_with_all = ["driver_from", "builtin_driver"])]
    pub driver: Option<PathBuf>,

    /// Use the bundled known-good Apple SCSI driver (this is the default when
    /// no driver source is given).
    #[arg(long = "builtin-driver")]
    pub builtin_driver: bool,

    /// Force `pmBootCksum = 0`. Some ROMs skip checksum verification then.
    #[arg(long = "force-cksum-zero")]
    pub force_cksum_zero: bool,
}

pub fn run(args: MacScsiBlessArgs) -> Result<()> {
    let driver = if let Some(donor) = &args.driver_from {
        let mut f = open_image_ro(donor)?;
        MacScsiDriver::from_donor(&mut f)
            .map_err(|e| anyhow::anyhow!("{e}"))
            .with_context(|| format!("extracting driver from {}", donor.display()))?
    } else if let Some(raw) = &args.driver {
        let bytes = std::fs::read(raw).with_context(|| format!("reading {}", raw.display()))?;
        MacScsiDriver::from_raw(&bytes)
    } else {
        // Default (and explicit `--builtin-driver`): the bundled driver.
        MacScsiDriver::builtin()
    };

    let mut f = open_image_rw(&args.image)?;
    let disk_len = f.seek(SeekFrom::End(0))?;
    f.rewind()?;

    let opts = BlessOptions {
        force_cksum_zero: args.force_cksum_zero,
    };
    let report =
        bless_apm_disk(&mut f, disk_len, &driver, &opts).map_err(|e| anyhow::anyhow!("{e}"))?;

    let verb = if report.was_already_present {
        "refreshed"
    } else if report.created_partition {
        "installed new"
    } else {
        "installed"
    };
    let created_note = if report.created_partition {
        format!(" (created {} partition)", driver.partition_type)
    } else {
        String::new()
    };
    log_stderr(format!(
        "mac-scsi-bless: {verb} SCSI driver ({} blocks) at block {}{created_note} in {}",
        report.driver_blocks,
        report.driver_block,
        args.image.display(),
    ));
    log_stderr(
        "Note: this registers the driver so the ROM can read the disk; it does not change \
         HFS boot-block behavior.",
    );
    Ok(())
}
