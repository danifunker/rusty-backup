//! End-to-end test: clone an APM image's HFS volume into a fresh blank
//! target file. Useful for verifying changes that affect catalog node usage.

use rusty_backup::fs::filesystem::EditableFilesystem;
use rusty_backup::fs::hfs::{create_blank_hfs_sized, HfsFilesystem, HFS_MAX_BTREE_FILE_SIZE};
use rusty_backup::fs::hfs_clone::clone_hfs_volume;
use rusty_backup::partition::apm::Apm;
use std::fs::{File, OpenOptions};
use std::io::Write;

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: test_clone_fix <src> [out] [bs] [mib]");
    let out_path = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "/tmp/test_clone_fix.hfs".into());
    let target_block_size: u32 = std::env::args()
        .nth(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(32 * 1024);
    let target_size_mib: u64 = std::env::args()
        .nth(4)
        .and_then(|s| s.parse().ok())
        .unwrap_or(2048);
    let target_size: u64 = target_size_mib * 1024 * 1024;

    let mut f = File::open(&path).unwrap();
    let apm = Apm::parse(&mut f).expect("parse APM");
    let hfs_entry = apm
        .entries
        .iter()
        .find(|e| e.partition_type == "Apple_HFS")
        .expect("no Apple_HFS");
    let off = hfs_entry.start_block as u64 * 512;

    let mut source = HfsFilesystem::open(File::open(&path).unwrap(), off).unwrap();
    let s = source.volume_summary();
    println!(
        "Source: {:?}  bs={}  files={}  folders={}  cat={} KiB",
        s.volume_name,
        s.block_size,
        s.file_count,
        s.folder_count,
        s.catalog_file_size / 1024
    );

    let catalog_min = (s.catalog_file_size as u32)
        .saturating_mul(3)
        .saturating_div(2)
        .min(HFS_MAX_BTREE_FILE_SIZE);
    let extents_min = (s.extents_file_size as u32).min(HFS_MAX_BTREE_FILE_SIZE);

    println!(
        "Building blank target file: {} MiB, bs={}, catalog>={} KiB, extents>={} KiB",
        target_size_mib,
        target_block_size,
        catalog_min / 1024,
        extents_min / 1024
    );
    let buf = create_blank_hfs_sized(
        target_size,
        target_block_size,
        &s.volume_name,
        extents_min,
        catalog_min,
    )
    .expect("create_blank_hfs_sized");
    {
        let mut out = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&out_path)
            .expect("create out");
        out.write_all(&buf).expect("write blank");
        out.flush().unwrap();
    }

    let out = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&out_path)
        .expect("reopen");
    let mut target = HfsFilesystem::open(out, 0).expect("open target file");

    println!("Cloning…");
    match clone_hfs_volume(&mut source, &mut target) {
        Ok(report) => {
            target.sync_metadata().expect("sync");
            println!(
                "Clone OK: {} dirs, {} files, {} bytes -> {}",
                report.dirs_copied, report.files_copied, report.data_bytes_copied, out_path
            );
        }
        Err(e) => {
            // Try to flush whatever we have so the partial output is probable.
            let _ = target.sync_metadata();
            eprintln!("Clone FAILED at runtime: {e:?}");
            eprintln!("Partial output written to: {out_path}");
            std::process::exit(1);
        }
    }
}
