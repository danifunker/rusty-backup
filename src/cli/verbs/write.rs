//! `rb-cli write IMAGE DEVICE --yes` — pour an image file onto a block
//! device. Phase D scope: full safety machinery via
//! [`crate::cli::device_safety`] — system-disk refusal (overridable
//! with `--write-to-system-disk`), mounted-target refusal, mount-point
//! vs device-path disambiguation. `--yes` skips the confirmation
//! prompt but never the safety summary.

use anyhow::{bail, Context, Result};
use clap::Args;
use std::io::{Read, Write};
use std::path::PathBuf;

use crate::cli::device_safety::{preflight, print_safety_summary};
use crate::cli::logging::log_stderr;
use crate::partition::format_size;

#[derive(Debug, Args)]
pub struct WriteArgs {
    /// Source image file.
    pub image: PathBuf,

    /// Destination block-device path:
    ///   - Linux: `/dev/sdX` or `/dev/nvmeXnY`
    ///   - macOS: `/dev/diskN` / `/dev/rdiskN`
    ///   - Windows: `"\\.\PhysicalDriveN"` (quote for PowerShell)
    pub device: PathBuf,

    /// Required confirmation. Skips the prompt but never the safety
    /// summary printed on stderr.
    #[arg(long)]
    pub yes: bool,

    /// Allow writing to the system boot disk (refused by default).
    #[arg(long = "write-to-system-disk")]
    pub write_to_system_disk: bool,
}

pub fn run(args: WriteArgs) -> Result<()> {
    if !args.yes {
        bail!(
            "writing to {} is destructive; pass --yes to confirm",
            args.device.display()
        );
    }

    let pre = preflight(&args.device, args.write_to_system_disk)?;

    let src_size = std::fs::metadata(&args.image)
        .with_context(|| format!("stat {}", args.image.display()))?
        .len();

    print_safety_summary(
        "write",
        &args.image.display().to_string(),
        &args.device,
        src_size,
        pre.device.as_ref(),
    );

    let mut src = std::fs::File::open(&args.image)
        .with_context(|| format!("opening {}", args.image.display()))?;
    let dst_file = std::fs::OpenOptions::new()
        .write(true)
        .open(&args.device)
        .with_context(|| format!("opening {} for write", args.device.display()))?;
    let mut dst = crate::os::SectorAlignedWriter::new(dst_file);

    let mut buf = vec![0u8; 1024 * 1024];
    let mut total: u64 = 0;
    let mut last_log: u64 = 0;
    loop {
        let n = src.read(&mut buf)?;
        if n == 0 {
            break;
        }
        dst.write_all(&buf[..n])?;
        total += n as u64;
        if total - last_log >= 64 * 1024 * 1024 {
            log_stderr(format!(
                "  wrote {} of {}",
                format_size(total),
                format_size(src_size)
            ));
            last_log = total;
        }
    }
    dst.flush()?;
    log_stderr(format!(
        "done: wrote {} to {}",
        format_size(total),
        args.device.display()
    ));
    Ok(())
}
