//! `api sgi` — SGI/IRIX disk operations.

use anyhow::Result;
use clap::Subcommand;
use std::io::Write;
use std::path::PathBuf;

#[derive(Subcommand, Debug)]
pub enum SgiCommand {
    /// Re-encode an IRIX disk image into a CHD whose logical size matches
    /// the SGI volume header's used floor. Drops trailing zero padding
    /// past `max(first + blocks)` over all non-empty partition entries.
    /// Accepts a raw `.img` or an existing `.chd` as input; always writes
    /// a CHD. Refuses to overwrite the source or an existing output file.
    Shrink {
        /// Source image (raw `.img` or `.chd`). Must contain an SGI
        /// volume header at sector 0.
        input: PathBuf,
        /// Destination CHD path. Must end in `.chd`, must not already
        /// exist, and must not resolve to the same file as `input`.
        output: PathBuf,
    },
}

pub fn run(verb: SgiCommand) -> Result<()> {
    match verb {
        SgiCommand::Shrink { input, output } => cmd_shrink(input, output),
    }
}

fn cmd_shrink(input: PathBuf, output: PathBuf) -> Result<()> {
    use std::sync::atomic::AtomicBool;

    let cancel = AtomicBool::new(false);
    let mut last_mib: u64 = u64::MAX;
    let mut progress_cb = |bytes: u64| {
        let mib = bytes / (1024 * 1024);
        if mib != last_mib {
            eprint!("\rprogress: {mib} MiB written  ");
            let _ = std::io::stderr().flush();
            last_mib = mib;
        }
    };
    let cancel_check = || cancel.load(std::sync::atomic::Ordering::Relaxed);
    let mut log_cb = |msg: &str| {
        eprintln!("\n  {msg}");
    };

    let report = crate::rbformats::chd::shrink_sgi_disk_to_chd(
        &input,
        &output,
        &mut progress_cb,
        &cancel_check,
        &mut log_cb,
    )?;

    eprintln!();
    println!("Shrunk {} -> {}", input.display(), output.display());
    println!(
        "  source logical: {} bytes ({:.3} GiB)",
        report.source_logical_size,
        report.source_logical_size as f64 / (1024.0 * 1024.0 * 1024.0)
    );
    println!(
        "  new logical:    {} bytes ({:.3} GiB)",
        report.new_logical_size,
        report.new_logical_size as f64 / (1024.0 * 1024.0 * 1024.0)
    );
    println!(
        "  floor:          sector {} (max first+blocks across SGI partitions)",
        report.partition_floor_sectors
    );
    println!("  dropped:        {} bytes", report.bytes_dropped());
    Ok(())
}
