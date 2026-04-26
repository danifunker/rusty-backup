use rusty_backup::fs::filesystem::Filesystem;
use rusty_backup::fs::prodos::ProDosFilesystem;
use rusty_backup::partition::PartitionTable;
use std::fs::File;

fn probe(path: &str) {
    println!("=== {path} ===");
    let mut f = File::open(path).unwrap();
    match PartitionTable::detect(&mut f) {
        Ok(table) => {
            let partitions = table.partitions();
            println!("  partition table: {} partitions", partitions.len());
            match &table {
                PartitionTable::None {
                    fs_hint,
                    size_bytes,
                } => {
                    println!("  superfloppy: fs_hint={fs_hint} size={size_bytes}");
                }
                PartitionTable::Mbr(_) => println!("  MBR"),
                PartitionTable::Apm(_) => println!("  APM"),
                PartitionTable::Gpt { .. } => println!("  GPT"),
            }
            for p in &partitions {
                println!(
                    "    #{} start_lba={} type='{}'",
                    p.index, p.start_lba, p.type_name
                );
            }
        }
        Err(e) => {
            println!("  partition parse error: {e}");
        }
    }

    // Also try opening directly as ProDOS
    let f = File::open(path).unwrap();
    match ProDosFilesystem::open(f, 0) {
        Ok(mut fs) => {
            println!("  opened as ProDOS: label={:?}", fs.volume_label());
            let root = fs.root().unwrap();
            let entries = fs.list_directory(&root).unwrap();
            println!("  {} entries in root", entries.len());
            for e in entries.iter() {
                println!(
                    "    {} ({}, {} bytes)",
                    e.name,
                    if e.is_directory() { "dir" } else { "file" },
                    e.size
                );
                if e.is_directory() {
                    if let Ok(kids) = fs.list_directory(e) {
                        for k in kids.iter() {
                            println!(
                                "      {} ({}, {} bytes)",
                                k.name,
                                if k.is_directory() { "dir" } else { "file" },
                                k.size
                            );
                        }
                    }
                }
            }
        }
        Err(e) => println!("  ProDosFilesystem::open error: {e}"),
    }
}

fn main() {
    probe("/Users/dani/Downloads/BlankProDOSDisk.hdv");
    probe("/Users/dani/Downloads/gsos.hdv");
    probe("/Users/dani/Documents/IIgsHDWithFiles.hdv");
}
