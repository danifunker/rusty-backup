use rusty_backup::fs::hfs::HfsFilesystem;
use std::fs::File;
fn main() {
    let path = std::env::args().nth(1).unwrap();
    let off: u64 = std::env::args().nth(2).unwrap().parse().unwrap();
    let f = File::open(path).unwrap();
    let fs = HfsFilesystem::open(f, off).unwrap();
    let s = fs.volume_summary();
    println!(
        "name={:?} block_size={} total={} catalog_size={} extents_size={}",
        s.volume_name, s.block_size, s.total_blocks, s.catalog_file_size, s.extents_file_size
    );
}
