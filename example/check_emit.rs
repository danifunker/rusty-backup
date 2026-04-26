use byteorder::{BigEndian, ByteOrder};
use rusty_backup::fs::hfs::{create_blank_hfs_sized, HfsFilesystem, HFS_MAX_BTREE_FILE_SIZE};
use rusty_backup::fs::hfs_clone::{clone_hfs_volume, emit_apm_disk_with_hfs};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Cursor, Read, Seek, SeekFrom};

fn main() {
    let src_path = std::env::args().nth(1).unwrap();
    let out_path = std::env::args().nth(2).unwrap();
    let part_off: u64 = 96 * 512;

    let src_file = File::open(&src_path).unwrap();
    let mut src_for_apm = src_file.try_clone().unwrap();
    let src_size = src_for_apm.seek(SeekFrom::End(0)).unwrap();
    let mut src_fs = HfsFilesystem::open(BufReader::new(src_file), part_off).unwrap();
    let s = src_fs.volume_summary();

    let cap = HFS_MAX_BTREE_FILE_SIZE;
    let catalog_min = (s.catalog_file_size.saturating_mul(3) / 2).min(cap);
    let extents_min = s.extents_file_size.min(cap);

    let mut tgt = create_blank_hfs_sized(
        2046 * 1024 * 1024,
        32 * 1024,
        &s.volume_name,
        extents_min,
        catalog_min,
    )
    .unwrap();
    {
        let mut tgt_fs = HfsFilesystem::open(Cursor::new(&mut tgt), 0).unwrap();
        clone_hfs_volume(&mut src_fs, &mut tgt_fs).unwrap();
    }
    println!(
        "tgt buffer drAtrb (offset 1034..36): {:04x}",
        BigEndian::read_u16(&tgt[1024 + 10..1024 + 12])
    );

    let mut out = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&out_path)
        .unwrap();
    let mut src2 = File::open(&src_path).unwrap();
    emit_apm_disk_with_hfs(&mut src2, src_size, &tgt, &mut out).unwrap();
    drop(out);

    let mut f = File::open(&out_path).unwrap();
    let mut buf = [0u8; 2];
    f.seek(SeekFrom::Start(96 * 512 + 1024 + 10)).unwrap();
    f.read_exact(&mut buf).unwrap();
    println!(
        "output disk primary drAtrb: {:04x}",
        BigEndian::read_u16(&buf)
    );
}
