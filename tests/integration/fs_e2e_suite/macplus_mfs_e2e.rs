//! End-to-end tests for MacPlus MFS dispatch.
//!
//! MFS has no Linux apt tool that produces real volumes (mkfs.hfs / hformat
//! only make HFS; BasiliskII would need a Mac Plus ROM the user would have
//! to supply). So these tests build a tiny synthetic MFS volume in-memory
//! and assert the partition-table + filesystem-dispatch wiring routes it
//! correctly through:
//!   `PartitionTable::detect` -> `PartitionTable::None { fs_hint: "MFS" }`
//!                            -> `open_filesystem(type_byte=0)` auto-detect
//!                            -> `MfsFilesystem::open` + read a known file
//!
//! When a real MFS .dsk surfaces (a vintage System 1.0 / 2.0 boot disk
//! pulled from a public archive), it can drop straight into
//! `tests/fixtures/` and re-use these same dispatch paths.

use std::io::Cursor;

use byteorder::{BigEndian, ByteOrder};
use rusty_backup::partition::PartitionTable;
// Filesystem trait methods (fs_type / root / list_directory / read_file) are
// invoked via dynamic dispatch on a Box<dyn Filesystem>, which does NOT
// require the trait to be in scope here.

const SECTOR: usize = 512;

/// Build a 16 KiB synthetic MFS volume mirroring the unit-test builder.
/// One file: "Hello" -> b"hi from mfs".
fn build_synthetic_mfs() -> Vec<u8> {
    const NUM_SECTORS: usize = 32;
    const ALLOC_BLOCK_SIZE: u32 = 1024;
    const FIRST_ALLOC_BLOCK_SECTOR: u16 = 5;
    const DIR_START: u16 = 3;
    const DIR_LEN: u16 = 2;
    const NUM_ALLOC_BLOCKS: u16 = 8;

    let mut disk = vec![0u8; NUM_SECTORS * SECTOR];

    // MDB
    let mdb = &mut disk[1024..1024 + 512];
    BigEndian::write_u16(&mut mdb[0..2], 0xD2D7); // drSigWord
    BigEndian::write_u16(&mut mdb[12..14], 1); // num files
    BigEndian::write_u16(&mut mdb[14..16], DIR_START);
    BigEndian::write_u16(&mut mdb[16..18], DIR_LEN);
    BigEndian::write_u16(&mut mdb[18..20], NUM_ALLOC_BLOCKS);
    BigEndian::write_u32(&mut mdb[20..24], ALLOC_BLOCK_SIZE);
    BigEndian::write_u32(&mut mdb[24..28], ALLOC_BLOCK_SIZE);
    BigEndian::write_u16(&mut mdb[28..30], FIRST_ALLOC_BLOCK_SECTOR);
    BigEndian::write_u32(&mut mdb[30..34], 2); // next file number
    BigEndian::write_u16(&mut mdb[34..36], NUM_ALLOC_BLOCKS - 1);
    let vname = b"E2ETest";
    mdb[36] = vname.len() as u8;
    mdb[37..37 + vname.len()].copy_from_slice(vname);

    // Volume map — block 2 -> 1 (end of chain), rest free. The map is at the
    // fixed offset 1024 + 64, and entry index 0 is block 2, so block 2's entry
    // is the first map entry. Block 2 is even -> hi byte = value>>4, next byte's
    // high nibble = value & 0x0F.
    let map_start = 1024 + 64;
    let value: u16 = 1;
    disk[map_start] = (value >> 4) as u8;
    disk[map_start + 1] = ((value & 0x0F) as u8) << 4;

    // File directory at sector 3.
    let dir_off = 3 * SECTOR;
    disk[dir_off] = 0x80; // flags: used
                          // Finder info: type "TEXT" / creator "ttxt"
    disk[dir_off + 2..dir_off + 6].copy_from_slice(b"TEXT");
    disk[dir_off + 6..dir_off + 10].copy_from_slice(b"ttxt");
    BigEndian::write_u32(&mut disk[dir_off + 18..dir_off + 22], 1); // file number
    BigEndian::write_u16(&mut disk[dir_off + 22..dir_off + 24], 2); // data first block
    BigEndian::write_u32(&mut disk[dir_off + 24..dir_off + 28], 11); // data length
    BigEndian::write_u32(&mut disk[dir_off + 28..dir_off + 32], ALLOC_BLOCK_SIZE);
    let name = b"Hello";
    disk[dir_off + 50] = name.len() as u8;
    disk[dir_off + 51..dir_off + 51 + name.len()].copy_from_slice(name);

    // File data at allocation block 2 (sector 5).
    let data_off = FIRST_ALLOC_BLOCK_SECTOR as usize * SECTOR;
    disk[data_off..data_off + 11].copy_from_slice(b"hi from mfs");

    disk
}

#[test]
fn mfs_superfloppy_is_detected_with_mfs_fs_hint() {
    let disk = build_synthetic_mfs();
    let mut cur = Cursor::new(disk);
    let table = PartitionTable::detect(&mut cur).unwrap();
    assert_eq!(
        table.type_name(),
        "None",
        "MFS volumes are superfloppies — no partition table"
    );
    let parts = table.partitions();
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].type_name, "MFS");
    assert_eq!(parts[0].start_lba, 0);
}

#[test]
fn open_filesystem_autodetect_dispatches_to_mfs() {
    let disk = build_synthetic_mfs();
    let mut cur = Cursor::new(disk.clone());
    let table = PartitionTable::detect(&mut cur).unwrap();
    let p0 = &table.partitions()[0];

    let cur2 = Cursor::new(disk);
    let mut fs = rusty_backup::fs::open_filesystem(
        cur2,
        p0.start_lba * 512,
        p0.partition_type_byte,
        p0.partition_type_string.as_deref(),
    )
    .unwrap();
    assert_eq!(fs.fs_type(), "MFS");
    assert_eq!(fs.volume_label(), Some("E2ETest"));

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, "Hello");
    assert_eq!(entries[0].type_code, Some(*b"TEXT"));
    assert_eq!(entries[0].creator_code, Some(*b"ttxt"));

    let data = fs.read_file(&entries[0], 1024).unwrap();
    assert_eq!(&data, b"hi from mfs");
}
