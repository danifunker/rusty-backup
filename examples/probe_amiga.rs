//! Probe an Amiga disk image and report its partition table.
//!
//! Usage:
//!   cargo run --example probe_amiga -- <path>

use std::env;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use rusty_backup::partition::PartitionTable;
use rusty_backup::rbformats::chd::ChdReader;

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("usage: {} <path>", args[0]);
        std::process::exit(2);
    }
    let path = &args[1];

    let table = if path.to_lowercase().ends_with(".chd") {
        let mut reader = ChdReader::open(Path::new(path))?;
        PartitionTable::detect(&mut reader)?
    } else {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        PartitionTable::detect(&mut reader)?
    };

    println!("Table: {}", table.type_name());
    if let PartitionTable::Rdb(rdb) = &table {
        println!(
            "  RDSK @ block {}: hw_block_size={} cyls={} heads={} secs={}",
            rdb.header.block_num,
            rdb.header.block_size,
            rdb.header.cylinders,
            rdb.header.heads,
            rdb.header.sectors,
        );
        println!(
            "  drive: {:?} {:?} rev {:?}",
            rdb.header.disk_vendor, rdb.header.disk_product, rdb.header.disk_revision,
        );
    }
    let parts = table.partitions();
    println!("Partitions: {}", parts.len());
    for part in &parts {
        println!(
            "  [{}] type=\"{}\" str={:?} start_lba={} size={} bootable={}",
            part.index,
            part.type_name,
            part.partition_type_string,
            part.start_lba,
            part.size_bytes,
            part.bootable,
        );
    }

    Ok(())
}
