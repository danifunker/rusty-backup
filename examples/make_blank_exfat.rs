//! Emit a blank exFAT image for mount-testing the formatter.
//! Usage: cargo run --example make_blank_exfat -- <out_path> <size_mib> [label]
use rusty_backup::fs::exfat::{create_blank_exfat, ExfatFormatTemplate};
use std::fs::File;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let out = &args[1];
    let size_mib: u64 = args[2].parse().unwrap();
    let label = args.get(3).cloned();
    let template = ExfatFormatTemplate {
        bytes_per_sector: 512,
        sectors_per_cluster: 8, // 4 KiB clusters
        label,
    };
    let mut f = File::create(out).unwrap();
    create_blank_exfat(&mut f, &template, size_mib * 1024 * 1024).unwrap();
    println!("wrote {} ({} MiB) blank exFAT", out, size_mib);
}
