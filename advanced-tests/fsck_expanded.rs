use rusty_backup::fs::hfs::HfsFilesystem;
use std::fs::File;
fn main() {
    let path = std::env::args().nth(1).unwrap();
    let f = File::open(&path).unwrap();
    let mut fs = HfsFilesystem::open(f, 96 * 512).unwrap();
    let s = fs.volume_summary();
    println!(
        "MDB: name={:?} block_size={} total_blocks={} free_blocks={} used_bytes={}",
        s.volume_name, s.block_size, s.total_blocks, s.free_blocks, s.used_bytes
    );
    match fs.fsck() {
        Ok(res) => {
            println!(
                "Files: {}  Dirs: {}",
                res.stats.files_checked, res.stats.directories_checked
            );
            for (k, v) in &res.stats.extra {
                println!("  {k}: {v}");
            }
            println!("Errors: {}", res.errors.len());
            for i in &res.errors {
                println!("  [E {}] {} repairable={}", i.code, i.message, i.repairable);
            }
            println!("Warnings: {}", res.warnings.len());
            for i in &res.warnings {
                println!("  [W {}] {}", i.code, i.message);
            }
            println!("Orphans: {}", res.orphaned_entries.len());
            for o in &res.orphaned_entries {
                println!(
                    "  CNID {} {:?} parent={} dir={}",
                    o.id, o.name, o.missing_parent_id, o.is_directory
                );
            }
        }
        Err(e) => println!("fsck err: {e:?}"),
    }
}
