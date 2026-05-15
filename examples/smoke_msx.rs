use rusty_backup::fs::open_filesystem;
use rusty_backup::partition::PartitionTable;
use std::fs::File;

fn main() {
    let path = std::env::args().nth(1).expect("usage: smoke_msx <path>");
    let mut f = File::open(&path).expect("open");
    let table = PartitionTable::detect(&mut f).expect("detect");
    let parts = table.partitions();
    println!("found {} partition(s)", parts.len());
    for (i, p) in parts.iter().enumerate() {
        let start_offset = p.start_lba * 512;
        println!(
            "  part[{i}] type=0x{:02X} start_lba={} offset={} size={}",
            p.partition_type_byte, p.start_lba, start_offset, p.size_bytes
        );
        let f2 = File::open(&path).expect("reopen");
        match open_filesystem(
            f2,
            start_offset,
            p.partition_type_byte,
            p.partition_type_string.as_deref(),
        ) {
            Ok(mut fs) => {
                println!(
                    "    fs_type={} label={:?} total={} used={}",
                    fs.fs_type(),
                    fs.volume_label(),
                    fs.total_size(),
                    fs.used_size()
                );
                let root = fs.root().expect("root");
                let entries = fs.list_directory(&root).expect("list root");
                println!("    root has {} entries:", entries.len());
                for (j, e) in entries.iter().take(15).enumerate() {
                    println!(
                        "      [{j}] {} ({} bytes, dir={})",
                        e.name,
                        e.size,
                        e.is_directory()
                    );
                }
            }
            Err(e) => println!("    open_filesystem error: {e}"),
        }
    }
}
