//! Emit a blank NTFS image for oracle validation of the formatter.
//! Usage: cargo run --example make_blank_ntfs -- <out_path> <size_mib> [label]
use rusty_backup::fs::ntfs_format::create_blank_ntfs;
use std::fs::File;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let out = &args[1];
    let size_mib: u64 = args[2].parse().unwrap();
    let label = args.get(3).map(|s| s.as_str());
    let mut f = File::create(out).unwrap();
    create_blank_ntfs(&mut f, size_mib * 1024 * 1024, 128, label).unwrap();
    println!("wrote {} ({} MiB) blank NTFS", out, size_mib);
}
