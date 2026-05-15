use rusty_backup::fs;
use rusty_backup::partition::PartitionTable;
use std::env;
use std::fs::File;
use std::io::BufReader;

fn main() -> anyhow::Result<()> {
    let p = env::args().nth(1).expect("path");
    let f = File::open(&p)?;
    let mut br = BufReader::new(f);
    let table = PartitionTable::detect(&mut br)?;
    for part in table.partitions() {
        let ts = part.partition_type_string.as_deref().unwrap_or("");
        if !fs::is_amiga_dos_type(ts) {
            continue;
        }
        let f2 = File::open(&p)?;
        let mut fs = fs::open_filesystem(f2, part.start_lba * 512, 0, Some(ts))?;
        match fs.fsck() {
            Some(Ok(r)) => {
                println!(
                    "partition {}: {} dirs, {} files",
                    part.index, r.stats.directories_checked, r.stats.files_checked
                );
                println!("  clean: {}", r.is_clean());
                for e in &r.errors {
                    println!("  ERR  {}: {}", e.code, e.message);
                }
                for w in &r.warnings {
                    println!("  WARN {}: {}", w.code, w.message);
                }
                for (k, v) in &r.stats.extra {
                    println!("  {}: {}", k, v);
                }
            }
            Some(Err(e)) => println!("partition {}: fsck failed: {}", part.index, e),
            None => println!("partition {}: fsck not supported", part.index),
        }
    }
    Ok(())
}
