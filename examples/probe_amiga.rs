//! Probe an Amiga disk image and report its partition table.
//!
//! Usage:
//!   cargo run --example probe_amiga -- <path>

use std::env;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use rusty_backup::fs;
use rusty_backup::fs::pfs3::CompactPfs3Reader;
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

        // Browse AFFS partitions inline.
        let type_str = part.partition_type_string.as_deref().unwrap_or("");
        if !fs::is_amiga_dos_type(type_str) && !fs::is_amiga_pfs3_type(type_str) {
            continue;
        }
        let offset = part.start_lba * 512;
        let opened: Result<Box<dyn fs::filesystem::Filesystem>, _> =
            if path.to_lowercase().ends_with(".chd") {
                let reader = ChdReader::open(Path::new(path))?;
                fs::open_filesystem(reader, offset, 0, Some(type_str))
            } else {
                let file = File::open(path)?;
                fs::open_filesystem(file, offset, 0, Some(type_str))
            };
        match opened {
            Ok(mut fs) => {
                let label = fs.volume_label().map(|s| s.to_string());
                let typ = fs.fs_type().to_string();
                let total = fs.total_size();
                let used = fs.used_size();
                println!("      label={label:?} type={typ} used={used}/{total}");
                if let Ok(root) = fs.root() {
                    if let Ok(children) = fs.list_directory(&root) {
                        for child in children.iter().take(5) {
                            println!(
                                "        - {} ({} bytes, {})",
                                child.name,
                                child.size,
                                if child.is_directory() { "dir" } else { "file" }
                            );
                        }
                        if children.len() > 5 {
                            println!("        ... and {} more", children.len() - 5);
                        }
                        // Pick the first plain file under 4 KiB and read it
                        // so we exercise the file-data path end-to-end.
                        if let Some(small) = children
                            .iter()
                            .find(|c| !c.is_directory() && c.size > 0 && c.size < 4096)
                        {
                            match fs.read_file(small, small.size as usize) {
                                Ok(data) => {
                                    let preview = data.iter().take(16).copied().collect::<Vec<_>>();
                                    println!(
                                        "        read {} bytes from {}: {:02x?}{}",
                                        data.len(),
                                        small.name,
                                        preview,
                                        if data.len() > 16 { " ..." } else { "" }
                                    );
                                }
                                Err(e) => println!("        read_file({}) failed: {e}", small.name),
                            }
                        }
                    }
                }
            }
            Err(e) => println!("      open failed: {e}"),
        }

        // For PFS3, also exercise the compact-reader bitmap walk so we
        // can compare data_size against used_size from the rootblock.
        if fs::is_amiga_pfs3_type(type_str) {
            let compact: Result<(_, _), _> = if path.to_lowercase().ends_with(".chd") {
                let reader = ChdReader::open(Path::new(path))?;
                CompactPfs3Reader::new(reader, offset).map(|(_r, res)| ((), res))
            } else {
                let file = File::open(path)?;
                CompactPfs3Reader::new(file, offset).map(|(_r, res)| ((), res))
            };
            match compact {
                Ok(((), result)) => {
                    println!(
                        "      compact: data={} orig={} (ratio {:.2}%)",
                        result.data_size,
                        result.original_size,
                        100.0 * result.data_size as f64 / result.original_size.max(1) as f64,
                    );
                }
                Err(e) => println!("      compact reader: {e}"),
            }
        }
    }

    Ok(())
}
