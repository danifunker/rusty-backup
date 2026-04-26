use byteorder::{BigEndian, ByteOrder};
use rusty_backup::fs::hfs::{create_blank_hfs_sized, HfsFilesystem, HFS_MAX_BTREE_FILE_SIZE};
use rusty_backup::fs::hfs_clone::clone_hfs_volume;
use std::fs::File;
use std::io::{BufReader, Cursor};

fn main() {
    let src_path = std::env::args().nth(1).unwrap();
    let part_off: u64 = 96 * 512;
    let block_size = 32 * 1024u32;

    let src_file = File::open(&src_path).unwrap();
    let mut src_fs = HfsFilesystem::open(BufReader::new(src_file), part_off).unwrap();
    let s = src_fs.volume_summary();

    let cap = HFS_MAX_BTREE_FILE_SIZE;
    let catalog_min = (s.catalog_file_size.saturating_mul(3) / 2).min(cap);
    let extents_min = s.extents_file_size.min(cap);

    let mut tgt = create_blank_hfs_sized(
        2046 * 1024 * 1024,
        block_size,
        &s.volume_name,
        extents_min,
        catalog_min,
    )
    .unwrap();
    println!(
        "[blank] drAtrb at +1034: {:04x}",
        BigEndian::read_u16(&tgt[1024 + 10..1024 + 12])
    );
    let alt_off = tgt.len() - 1024;
    println!(
        "[blank] alt drAtrb:    {:04x}",
        BigEndian::read_u16(&tgt[alt_off + 10..alt_off + 12])
    );

    {
        let mut tgt_fs = HfsFilesystem::open(Cursor::new(&mut tgt), 0).unwrap();
        clone_hfs_volume(&mut src_fs, &mut tgt_fs).unwrap();
    }
    println!(
        "[after clone] drAtrb: {:04x}",
        BigEndian::read_u16(&tgt[1024 + 10..1024 + 12])
    );
    println!(
        "[after clone] alt:    {:04x}",
        BigEndian::read_u16(&tgt[alt_off + 10..alt_off + 12])
    );
}
