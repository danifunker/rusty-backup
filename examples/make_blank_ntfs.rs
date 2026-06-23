//! Emit a blank NTFS image for oracle validation of the formatter.
//! Usage: cargo run --example make_blank_ntfs -- <out_path> <size_mib> [label] [cluster] [sector]
use rusty_backup::fs::ntfs_format::{create_ntfs, NtfsFormatParams, NtfsGeometry};
use std::fs::File;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let out = &args[1];
    let size_mib: u64 = args[2].parse().unwrap();
    let label = args.get(3).map(|s| s.to_string());
    let cluster: u32 = args.get(4).map(|s| s.parse().unwrap()).unwrap_or(4096);
    let sector: u32 = args.get(5).map(|s| s.parse().unwrap()).unwrap_or(512);
    let geometry = NtfsGeometry::with_cluster_size(cluster, sector).expect("valid geometry");
    let mut f = File::create(out).unwrap();
    create_ntfs(
        &mut f,
        &NtfsFormatParams {
            total_size: size_mib * 1024 * 1024,
            geometry,
            mft_records_hint: 128,
            label,
        },
    )
    .unwrap();
    println!("wrote {out} ({size_mib} MiB) blank NTFS, cluster={cluster} sector={sector}");
}
