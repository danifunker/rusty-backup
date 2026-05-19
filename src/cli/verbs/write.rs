//! `rb-cli write IMAGE DEVICE --yes` — pour an image file onto a block
//! device. Phase C scope: streaming copy with the device-write safety
//! summary surfaced on stderr and the `--yes` gate.
//!
//! Full safety machinery (system-disk refusal, mounted-target refusal,
//! `--write-to-system-disk` opt-in, mount-point vs device-path
//! disambiguation) lands in Phase D alongside the batch preflight
//! lift. Phase C requires `--yes` for every write and prints the
//! summary — that's the minimum useful surface.

use anyhow::{bail, Context, Result};
use clap::Args;
use std::io::{Read, Write};
use std::path::PathBuf;

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
}

pub fn run(args: WriteArgs) -> Result<()> {
    if !args.yes {
        bail!(
            "writing to {} is destructive; pass --yes to confirm",
            args.device.display()
        );
    }

    let src_size = std::fs::metadata(&args.image)
        .with_context(|| format!("stat {}", args.image.display()))?
        .len();

    print_safety_summary(&args.image, &args.device, src_size);

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

fn print_safety_summary(image: &std::path::Path, device: &std::path::Path, image_size: u64) {
    log_stderr("=== rb-cli write — safety summary ===");
    log_stderr(format!("  source:   {}", image.display()));
    log_stderr(format!(
        "  size:     {} ({} bytes)",
        format_size(image_size),
        image_size
    ));
    log_stderr(format!("  target:   {}", device.display()));
    // Try to find the device in src/os/'s enumeration to enrich the summary.
    if let Some(d) = crate::device::enumerate_devices()
        .into_iter()
        .find(|d| d.path == device)
    {
        log_stderr(format!(
            "  device:   {} ({}) [{}]",
            d.media_name,
            format_size(d.size_bytes),
            d.bus_protocol
        ));
        if d.is_system {
            log_stderr("  WARN:     target appears to be the system disk");
        }
        for p in &d.partitions {
            if !p.mount_point.as_os_str().is_empty() {
                log_stderr(format!(
                    "  mounted:  partition {:?} at {}",
                    p.name,
                    p.mount_point.display()
                ));
            }
        }
    } else {
        log_stderr("  (device not in os enumeration — caller must verify)");
    }
    log_stderr("======================================");
    let _ = anyhow::Ok::<()>(());
}
