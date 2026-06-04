//! Wave-2 dispatch + write-path e2e tests.
//!
//! Each test feeds a synthetic volume through `fs::open_filesystem`
//! (string-route or detect-route depending on the FS) and confirms the
//! dispatch chain ends at the right driver. Write-path round-trips
//! land here for Human68k (which has full Add/Delete) and rely on the
//! engine-level tests for the others (ADFS + QDOS + ANDOS write paths
//! ship in follow-up commits per the plan tracker).

use std::io::{Cursor, Read};

use byteorder::{BigEndian, ByteOrder, LittleEndian};

use rusty_backup::fs::filesystem::CreateFileOptions;
#[allow(unused_imports)]
use rusty_backup::fs::filesystem::Filesystem;

// ----------------------------------------------------------------------------
// Human68k — explicit "human68k" string-route + put/get round-trip
// ----------------------------------------------------------------------------

fn build_human68k_disk() -> Vec<u8> {
    const TOTAL_SECTORS: u32 = 1440;
    const BYTES_PER_SECTOR: u16 = 512;
    const SECTORS_PER_CLUSTER: u8 = 1;
    const RESERVED_SECTORS: u16 = 1;
    const NUM_FATS: u8 = 2;
    const ROOT_ENTRIES: u16 = 112;
    const FAT_SECTORS: u16 = 3;

    let mut disk = vec![0u8; TOTAL_SECTORS as usize * BYTES_PER_SECTOR as usize];
    disk[0] = 0xEB;
    disk[1] = 0x3C;
    disk[2] = 0x90;
    disk[3..11].copy_from_slice(b"X68KFS  ");
    LittleEndian::write_u16(&mut disk[11..13], BYTES_PER_SECTOR);
    disk[13] = SECTORS_PER_CLUSTER;
    LittleEndian::write_u16(&mut disk[14..16], RESERVED_SECTORS);
    disk[16] = NUM_FATS;
    LittleEndian::write_u16(&mut disk[17..19], ROOT_ENTRIES);
    LittleEndian::write_u16(&mut disk[19..21], TOTAL_SECTORS as u16);
    disk[21] = 0xF9;
    LittleEndian::write_u16(&mut disk[22..24], FAT_SECTORS);
    let fat0 = RESERVED_SECTORS as usize * BYTES_PER_SECTOR as usize;
    disk[fat0] = 0xF9;
    disk[fat0 + 1] = 0xFF;
    disk[fat0 + 2] = 0xFF;
    disk
}

#[test]
fn dispatch_via_human68k_string_works_end_to_end_and_write_round_trips() {
    let disk = build_human68k_disk();
    let cur = Cursor::new(disk);
    let mut fs = rusty_backup::fs::open_editable_filesystem(cur, 0, 0, Some("human68k")).unwrap();
    assert!(fs.fs_type().contains("Human68k"));
    let root = fs.root().unwrap();
    let payload = b"hello human68k dispatch test".to_vec();
    let mut src = Cursor::new(payload.clone());
    let _ = fs
        .create_file(
            &root,
            "HI.TXT",
            &mut src,
            payload.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let hi = entries.iter().find(|e| e.name == "HI.TXT").unwrap();
    let mut got = Vec::new();
    Read::read_to_end(&mut Cursor::new(fs.read_file(hi, 4096).unwrap()), &mut got).unwrap();
    assert_eq!(got, payload);
}

// ----------------------------------------------------------------------------
// ADFS — auto-detect via Disc Record probe
// ----------------------------------------------------------------------------

fn build_adfs_eformat_disk() -> Vec<u8> {
    const TOTAL_BYTES: usize = 800 * 1024;
    const SECTOR_SIZE: u32 = 1024;
    const FILE_SECTOR: u32 = 0x10;
    const ROOT_SECTOR: u32 = 0x20;
    let dr_off = 0xC00usize + 0x1C0;
    let mut disk = vec![0u8; TOTAL_BYTES];
    disk[dr_off] = 10;
    disk[dr_off + 0x01] = 5;
    disk[dr_off + 0x02] = 2;
    disk[dr_off + 0x03] = 2;
    disk[dr_off + 0x04] = 15;
    disk[dr_off + 0x05] = 7;
    disk[dr_off + 0x09] = 2;
    LittleEndian::write_u16(&mut disk[dr_off + 0x0A..dr_off + 0x0C], 32);
    LittleEndian::write_u32(
        &mut disk[dr_off + 0x0C..dr_off + 0x10],
        ROOT_SECTOR * SECTOR_SIZE,
    );
    LittleEndian::write_u32(&mut disk[dr_off + 0x10..dr_off + 0x14], 800);
    LittleEndian::write_u16(&mut disk[dr_off + 0x14..dr_off + 0x16], 0xABCD);
    disk[dr_off + 0x16..dr_off + 0x20].copy_from_slice(b"AutoDisc  ");
    let root_off = (ROOT_SECTOR * SECTOR_SIZE) as usize;
    disk[root_off + 1] = b'H';
    disk[root_off + 2] = b'u';
    disk[root_off + 3] = b'g';
    disk[root_off + 4] = b'o';
    let e_off = root_off + 5;
    disk[e_off..e_off + 5].copy_from_slice(b"HELLO");
    LittleEndian::write_u32(&mut disk[e_off + 18..e_off + 22], 16);
    let f = FILE_SECTOR * SECTOR_SIZE;
    disk[e_off + 22] = (f & 0xFF) as u8;
    disk[e_off + 23] = ((f >> 8) & 0xFF) as u8;
    disk[e_off + 24] = ((f >> 16) & 0xFF) as u8;
    disk[e_off + 25] = 0x03;
    disk[f as usize..f as usize + 16].copy_from_slice(b"adfs auto detect");
    disk
}

#[test]
fn dispatch_via_auto_detect_routes_to_adfs() {
    let disk = build_adfs_eformat_disk();
    let cur = Cursor::new(disk);
    let mut fs = rusty_backup::fs::open_filesystem(cur, 0, 0, None).unwrap();
    assert!(fs.fs_type().starts_with("ADFS"));
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, "HELLO");
}

#[test]
fn partition_detect_recognises_x68k_human68k_table() {
    use rusty_backup::partition::x68k::{
        X68kPartitionTable, X68K_DEFAULT_SECTOR_SIZE, X68K_ENTRY_SIZE, X68K_MAGIC,
        X68K_TABLE_HEADER_SIZE, X68K_TABLE_OFFSET,
    };
    use rusty_backup::partition::PartitionTable;
    // Synthesize a 1 MB image with a single Human68k partition at
    // sector 64 (= the conventional first-partition offset).
    let total_bytes = 1024 * 1024usize;
    let mut img = vec![0u8; total_bytes];
    let off = X68K_TABLE_OFFSET as usize;
    BigEndian::write_u32(&mut img[off..off + 4], X68K_MAGIC);
    let e_off = off + X68K_TABLE_HEADER_SIZE;
    img[e_off..e_off + 8].copy_from_slice(b"Human   ");
    BigEndian::write_u32(&mut img[e_off + 8..e_off + 12], 64);
    BigEndian::write_u32(&mut img[e_off + 12..e_off + 16], 64);
    let _ = X68K_ENTRY_SIZE;
    let _ = X68K_DEFAULT_SECTOR_SIZE;
    let _: &X68kPartitionTable;
    let mut cur = Cursor::new(img);
    let table = PartitionTable::detect(&mut cur).unwrap();
    assert!(
        matches!(table, PartitionTable::X68k { .. }),
        "expected X68k partition table; got {:?}",
        table.type_name()
    );
    let parts = table.partitions();
    assert_eq!(parts.len(), 1);
    assert!(parts[0].type_name.contains("Human68k"));
    assert_eq!(parts[0].start_lba, 64);
    assert_eq!(parts[0].size_bytes, 64 * 512);
    assert_eq!(
        parts[0].partition_type_string.as_deref(),
        Some("human68k"),
        "Human68k partitions must dispatch to the Human68k engine"
    );
}

#[test]
fn dispatch_via_auto_detect_routes_to_qdos_mdv() {
    use rusty_backup::fs::qdos_mdv::{CART_NAME_OFFSET, MDV_CART_BYTES, MDV_SECTOR_BYTES};
    let mut cart = vec![0u8; MDV_CART_BYTES];
    // Sector-0 preamble + sync + cartridge name "TstCart".
    cart[0x0A] = 0xFF;
    cart[0x0B] = 0xFF;
    cart[0x0C] = 0xFF;
    cart[CART_NAME_OFFSET..CART_NAME_OFFSET + 7].copy_from_slice(b"TstCart");
    cart[CART_NAME_OFFSET + 7..CART_NAME_OFFSET + 10].copy_from_slice(b"   ");
    let _ = MDV_SECTOR_BYTES;
    let cur = Cursor::new(cart);
    let fs = rusty_backup::fs::open_filesystem(cur, 0, 0, None).unwrap();
    assert_eq!(fs.fs_type(), "QDOS Microdrive");
    assert_eq!(fs.volume_label(), Some("TstCart"));
}

#[test]
fn partition_superfloppy_detects_bare_adfs_hdf() {
    use rusty_backup::partition::PartitionTable;
    let mut disk = std::io::Cursor::new(build_adfs_eformat_disk());
    let table = PartitionTable::detect(&mut disk).unwrap();
    match table {
        PartitionTable::None { fs_hint, .. } => {
            assert_eq!(fs_hint, "ADFS", "bare ADFS should surface as fs_hint");
        }
        _ => panic!("expected PartitionTable::None for bare ADFS .hdf"),
    }
}

#[test]
fn source_reader_strips_arculator_hdf_header_and_routes_to_adfs() {
    use rusty_backup::model::source_reader;
    // Build an Arculator-wrapped synthetic: 512-byte header + bare ADFS.
    let bare = build_adfs_eformat_disk();
    let mut wrapped = vec![0u8; 0x200];
    wrapped[..16].copy_from_slice(b"ARCHEADER 0x0001");
    wrapped.extend_from_slice(&bare);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.hdf");
    std::fs::write(&path, &wrapped).unwrap();
    // is_arculator_hdf_path must accept the wrapped file but not the
    // bare ADFS.
    assert!(source_reader::is_arculator_hdf_path(&path));
    let bare_path = dir.path().join("bare.hdf");
    std::fs::write(&bare_path, &bare).unwrap();
    assert!(
        !source_reader::is_arculator_hdf_path(&bare_path),
        "bare .hdf must NOT trigger the Arculator strip path"
    );
    // After open_read, the stream's first 0xDC0+32 bytes must match
    // the bare ADFS image — i.e. the 512-byte header is gone.
    let mut reader = source_reader::open_read(&path).unwrap();
    use std::io::Read;
    let mut got = vec![0u8; 0xDC0 + 32];
    reader.read_exact(&mut got).unwrap();
    assert_eq!(&got, &bare[..0xDC0 + 32]);
}

// ----------------------------------------------------------------------------
// QDOS — auto-detect via QLWA signature
// ----------------------------------------------------------------------------

fn build_qdos_qxlwin_disk() -> Vec<u8> {
    const BLOCKS: u16 = 16;
    const SECTOR_BYTES: usize = 512;
    let header_bytes = 64usize;
    let dir_bytes = SECTOR_BYTES;
    let mut disk =
        vec![0u8; header_bytes + BLOCKS as usize * 2 + dir_bytes + BLOCKS as usize * SECTOR_BYTES];
    disk[0..4].copy_from_slice(b"QLWA");
    BigEndian::write_u16(&mut disk[6..8], 1);
    BigEndian::write_u16(&mut disk[8..10], 1);
    BigEndian::write_u16(&mut disk[10..12], 7);
    BigEndian::write_u16(&mut disk[12..14], BLOCKS - 1);
    BigEndian::write_u16(&mut disk[14..16], BLOCKS);
    BigEndian::write_u16(&mut disk[16..18], 1);
    BigEndian::write_u16(&mut disk[18..20], 16);
    BigEndian::write_u16(&mut disk[20..22], 4);
    BigEndian::write_u16(&mut disk[22..24], 1);
    disk[24] = 8;
    disk[25..33].copy_from_slice(b"AutoQL  ");
    let fat_off = header_bytes;
    BigEndian::write_u16(&mut disk[fat_off + 10..fat_off + 12], 0xFFFF);
    let dir_off = header_bytes + BLOCKS as usize * 2;
    BigEndian::write_u32(&mut disk[dir_off..dir_off + 4], 16);
    BigEndian::write_u16(&mut disk[dir_off + 14..dir_off + 16], 5);
    BigEndian::write_u16(&mut disk[dir_off + 22..dir_off + 24], 7);
    disk[dir_off + 24..dir_off + 31].copy_from_slice(b"AUTODET");
    let data_off = header_bytes + BLOCKS as usize * 2 + dir_bytes;
    let block_5 = data_off + 5 * SECTOR_BYTES;
    disk[block_5..block_5 + 16].copy_from_slice(b"qdos auto detect");
    disk
}

#[test]
fn dispatch_via_auto_detect_routes_to_qdos() {
    let disk = build_qdos_qxlwin_disk();
    let cur = Cursor::new(disk);
    let mut fs = rusty_backup::fs::open_filesystem(cur, 0, 0, None).unwrap();
    assert_eq!(fs.fs_type(), "QDOS (QXL.WIN)");
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, "AUTODET");
}

// ----------------------------------------------------------------------------
// ANDOS — auto-detect via signature probe
// ----------------------------------------------------------------------------

#[test]
fn dispatch_via_auto_detect_routes_to_andos() {
    let mut disk = vec![0u8; 1024 * 1024];
    disk[0x1F8..0x1F8 + 5].copy_from_slice(b"ANDOS");
    let cur = Cursor::new(disk);
    let mut fs = rusty_backup::fs::open_filesystem(cur, 0, 0, None).unwrap();
    assert_eq!(fs.fs_type(), "ANDOS (BK0011M)");
    let root = fs.root().unwrap();
    // Scaffold surface — list returns Unsupported rather than empty.
    let err = fs.list_directory(&root).unwrap_err();
    assert!(matches!(
        err,
        rusty_backup::fs::filesystem::FilesystemError::Unsupported(_)
    ));
}
