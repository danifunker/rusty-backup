//! Throwaway helper: build /tmp/testdisk.hda — an APM-wrapped HFS image
//! with Apple_HFS at LBA 96 (image byte 0xC000), mirroring the layout
//! used in the user's `put --boot` regression reproducer.

use rusty_backup::fs::hfs::create_blank_hfs;
use rusty_backup::fs::hfs_clone::emit_apm_disk_with_hfs;
use rusty_backup::partition::apm::build_minimal_apm;
use std::io::Cursor;

fn main() {
    let total_blocks: u32 = 4096;
    let hfs_start: u32 = 96;
    let mut apm = build_minimal_apm(
        &[
            ("Apple_Driver43".to_string(), 32, 8),
            ("Apple_HFS".to_string(), hfs_start, total_blocks - hfs_start),
        ],
        512,
        total_blocks,
    );
    apm.entries[2].name = "MacOS".into();
    let blocks = apm.build_apm_blocks(Some(total_blocks));
    let mut source = vec![0u8; total_blocks as usize * 512];
    source[..blocks.len()].copy_from_slice(&blocks);
    let hfs = create_blank_hfs((total_blocks - hfs_start) as u64 * 512, 4096, "TestDisk").unwrap();
    let mut out: Vec<u8> = Vec::new();
    let src_len = source.len() as u64;
    emit_apm_disk_with_hfs(
        &mut Cursor::new(source),
        src_len,
        &hfs,
        &mut Cursor::new(&mut out),
    )
    .unwrap();
    std::fs::write("/tmp/testdisk.hda", &out).unwrap();
    println!(
        "wrote /tmp/testdisk.hda ({} bytes, Apple_HFS @ 0xC000)",
        out.len()
    );
}
