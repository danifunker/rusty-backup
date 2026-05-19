//! `api apm` — Apple Partition Map operations.

use anyhow::{anyhow, Result};
use clap::Subcommand;
use std::path::PathBuf;

use crate::cli::io::open_image_ro;
use crate::partition::apm::Apm;

#[derive(Subcommand, Debug)]
pub enum ApmCommand {
    /// Print the partition map of an APM disk image.
    Info { image: PathBuf },
}

pub fn run(verb: ApmCommand) -> Result<()> {
    match verb {
        ApmCommand::Info { image } => cmd_info(image),
    }
}

fn cmd_info(image: PathBuf) -> Result<()> {
    let mut file = open_image_ro(&image)?;
    let apm = Apm::parse(&mut file).map_err(|e| anyhow!("parsing APM: {e}"))?;
    let bs = apm.ddr.block_size as u64;
    println!(
        "DDR: block_size={} block_count={} drivers={}",
        apm.ddr.block_size, apm.ddr.block_count, apm.ddr.driver_count
    );
    println!("Map entries: {}", apm.map_entry_count);
    println!(
        "{:>3}  {:<24}  {:<24}  {:>10}  {:>10}  {:>10}  status",
        "idx", "type", "name", "start", "blocks", "bytes"
    );
    for (i, e) in apm.entries.iter().enumerate() {
        println!(
            "{:>3}  {:<24}  {:<24}  {:>10}  {:>10}  {:>10}  0x{:08x}",
            i + 1,
            e.partition_type,
            e.name,
            e.start_block,
            e.block_count,
            e.size_bytes(bs as u16),
            e.status
        );
    }
    Ok(())
}
