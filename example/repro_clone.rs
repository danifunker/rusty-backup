use rusty_backup::fs::hfs::{create_blank_hfs_sized, HfsFilesystem, HFS_MAX_BTREE_FILE_SIZE};
use rusty_backup::fs::hfs_clone::{clone_hfs_volume, emit_apm_disk_with_hfs};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Cursor, Seek};

fn main() {
    let src_path = std::env::args().nth(1).unwrap();
    let out_path = std::env::args().nth(2).expect("output path");
    let part_off: u64 = 96 * 512;
    let block_size = 32 * 1024u32;
    let target_mib: u64 = 2046;

    let src_file = File::open(&src_path).unwrap();
    let mut src_for_apm = src_file.try_clone().unwrap();
    let src_size = src_for_apm.seek(std::io::SeekFrom::End(0)).unwrap();
    let src_reader = BufReader::new(src_file);
    let mut src_fs = HfsFilesystem::open(src_reader, part_off).unwrap();
    let s = src_fs.volume_summary();
    println!(
        "source: name={:?} catalog={} extents={}",
        s.volume_name, s.catalog_file_size, s.extents_file_size
    );

    let cap = HFS_MAX_BTREE_FILE_SIZE;
    let catalog_min = (s.catalog_file_size.saturating_mul(3) / 2).min(cap);
    let extents_min = s.extents_file_size.min(cap);

    let mut tgt = create_blank_hfs_sized(
        target_mib * 1024 * 1024,
        block_size,
        &s.volume_name,
        extents_min,
        catalog_min,
    )
    .unwrap();
    let mut tgt_fs = HfsFilesystem::open(Cursor::new(&mut tgt), 0).unwrap();
    let r = clone_hfs_volume(&mut src_fs, &mut tgt_fs).unwrap();
    println!("cloned: {} files, {} dirs", r.files_copied, r.dirs_copied);

    let result = tgt_fs.fsck().unwrap();
    println!("fsck errors: {}", result.errors.len());
    for e in &result.errors {
        println!("  [{}] {}", e.code, e.message);
    }

    let mut out = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&out_path)
        .unwrap();
    let mut src2 = File::open(&src_path).unwrap();
    let er = emit_apm_disk_with_hfs(&mut src2, src_size, &tgt, &mut out).unwrap();
    println!(
        "emitted: hfs_start={} hfs_blocks={}",
        er.hfs_start_block, er.hfs_block_count
    );
}
