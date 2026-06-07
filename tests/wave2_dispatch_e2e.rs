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

/// FSM-valid synthetic E-format ADFS floppy. Same layout as
/// `tests/cli_archie.rs::build_adfs_eformat_disk` (and the in-tree
/// `src/fs/adfs.rs::tests::build_eformat_with_one_file` unit-test
/// fixture): single-zone disc, `dr.root = 0x200`, HELLO at indaddr
/// `0x500`. The walker resolves these the kernel way.
fn build_adfs_eformat_disk() -> Vec<u8> {
    const SECTOR_SIZE: usize = 1024;
    const TOTAL_BYTES: usize = 12 * SECTOR_SIZE;
    const ROOT_SECTOR: u32 = 2;
    const FILE_SECTOR: u32 = 4;
    const IDLEN: u32 = 15;
    let mut disk = vec![0u8; TOTAL_BYTES];

    // DR at byte 4 (kernel adfs_validate_dr0) — populate the embedded
    // copy AND the boot-block copy at byte 0xDC0 so both detect sites
    // in `detect_superfloppy` / `detect_filesystem_type` succeed.
    let dr_buf = build_dr_bytes(IDLEN, TOTAL_BYTES as u32);
    disk[0x04..0x04 + 60].copy_from_slice(&dr_buf);
    disk[0xDC0..0xDC0 + 60].copy_from_slice(&dr_buf);

    // FSM bitstream in zone 0.
    set_bit_le(&mut disk[..SECTOR_SIZE], 512 + 16 - 1); // frag 0  sectors 0..1
    write_frag_bits(&mut disk[..SECTOR_SIZE], 528, IDLEN, 16, 2); // root  sectors 2..3
    write_frag_bits(&mut disk[..SECTOR_SIZE], 544, IDLEN, 16, 5); // HELLO sectors 4..5
    write_frag_bits(&mut disk[..SECTOR_SIZE], 560, IDLEN, 6352, 0); // tail free

    // Root dir at byte 2048 with one entry "HELLO" of 16 bytes at
    // frag 5 (indaddr 0x500).
    let root_off = ROOT_SECTOR as usize * SECTOR_SIZE;
    disk[root_off + 1] = b'H';
    disk[root_off + 2] = b'u';
    disk[root_off + 3] = b'g';
    disk[root_off + 4] = b'o';
    let e_off = root_off + 5;
    disk[e_off..e_off + 5].copy_from_slice(b"HELLO");
    LittleEndian::write_u32(&mut disk[e_off + 18..e_off + 22], 16);
    disk[e_off + 22] = 0x00; // indaddr lo
    disk[e_off + 23] = 0x05; // frag_id (>> 8)
    disk[e_off + 24] = 0x00;
    disk[e_off + 25] = 0x03;

    // File payload at byte 4096 (sector 4).
    let file_off = FILE_SECTOR as usize * SECTOR_SIZE;
    disk[file_off..file_off + 16].copy_from_slice(b"adfs auto detect");
    disk
}

fn build_dr_bytes(idlen: u32, total_bytes: u32) -> [u8; 60] {
    let mut dr = [0u8; 60];
    dr[0x00] = 10; // log2_secsize
    dr[0x01] = 5;
    dr[0x02] = 2;
    dr[0x03] = 2;
    dr[0x04] = idlen as u8;
    dr[0x05] = 7; // log2bpmb
    dr[0x09] = 1; // nzones
    LittleEndian::write_u16(&mut dr[0x0A..0x0C], 1312); // zone_spare
    LittleEndian::write_u32(&mut dr[0x0C..0x10], 0x200); // dr.root
    LittleEndian::write_u32(&mut dr[0x10..0x14], total_bytes);
    LittleEndian::write_u16(&mut dr[0x14..0x16], 0xABCD);
    dr[0x16..0x20].copy_from_slice(b"AutoDisc  ");
    dr
}

fn set_bit_le(buf: &mut [u8], pos: u32) {
    buf[(pos >> 3) as usize] |= 1 << (pos & 7);
}

fn write_frag_bits(buf: &mut [u8], start: u32, idlen: u32, length_bits: u32, frag_id: u32) {
    for k in 0..idlen {
        if (frag_id >> k) & 1 == 1 {
            set_bit_le(buf, start + k);
        }
    }
    set_bit_le(buf, start + length_bits - 1);
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
    // Canonical QXL.WIN layout (verified against kilgus QXL.WIN sample +
    // sQLux QDisk.c source): cluster 0 holds header + FAT, root dir is
    // a regular file chain starting at header.root_cluster.
    const CC: u16 = 16;
    const SPC: u16 = 1;
    const CLUSTER_SIZE: usize = 512;
    const ROOT_CLUSTER: u16 = 1;
    const ROOT_LEN: u32 = 128;
    const FILE_CLUSTER: u16 = 5;
    let mut disk = vec![0u8; CC as usize * CLUSTER_SIZE];
    disk[0..4].copy_from_slice(b"QLWA");
    BigEndian::write_u16(&mut disk[0x04..0x06], 0x0005);
    let mut name = [b' '; 20];
    name[..6].copy_from_slice(b"AutoQL");
    disk[0x06..0x1A].copy_from_slice(&name);
    BigEndian::write_u16(&mut disk[0x22..0x24], SPC);
    BigEndian::write_u16(&mut disk[0x2A..0x2C], CC);
    BigEndian::write_u16(&mut disk[0x2C..0x2E], 8);
    BigEndian::write_u16(&mut disk[0x32..0x34], 6);
    BigEndian::write_u16(&mut disk[0x34..0x36], ROOT_CLUSTER);
    BigEndian::write_u32(&mut disk[0x36..0x3A], ROOT_LEN);
    // Root cluster's data lives at byte 512. Slot 0 left empty (volume
    // self-reference). Slot 1 = AUTODET file pointing at cluster 5.
    let dir_off = ROOT_CLUSTER as usize * CLUSTER_SIZE;
    let slot1 = dir_off + 64;
    BigEndian::write_u32(&mut disk[slot1..slot1 + 4], 16);
    BigEndian::write_u16(&mut disk[slot1 + 0x0E..slot1 + 0x10], 7); // name length
    disk[slot1 + 0x10..slot1 + 0x17].copy_from_slice(b"AUTODET");
    BigEndian::write_u16(&mut disk[slot1 + 0x3A..slot1 + 0x3C], FILE_CLUSTER);
    let file_off = FILE_CLUSTER as usize * CLUSTER_SIZE;
    disk[file_off..file_off + 16].copy_from_slice(b"qdos auto detect");
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

/// Same shape as `build_qdos_qxlwin_disk` but with a proper free-cluster
/// linked list (ffc=2, fc=12, chain 2→3→4→6→7→…→15→0), enough state for
/// the editable-dispatch round-trip test to allocate and free clusters.
fn build_qdos_qxlwin_disk_with_freelist() -> Vec<u8> {
    const CC: u16 = 16;
    const SPC: u16 = 1;
    const CLUSTER_SIZE: usize = 512;
    const ROOT_CLUSTER: u16 = 1;
    const ROOT_LEN: u32 = 128;
    const FILE_CLUSTER: u16 = 5;
    let mut disk = vec![0u8; CC as usize * CLUSTER_SIZE];
    disk[0..4].copy_from_slice(b"QLWA");
    BigEndian::write_u16(&mut disk[0x04..0x06], 0x0005);
    let mut name = [b' '; 20];
    name[..6].copy_from_slice(b"EditQL");
    disk[0x06..0x1A].copy_from_slice(&name);
    BigEndian::write_u16(&mut disk[0x22..0x24], SPC);
    BigEndian::write_u16(&mut disk[0x2A..0x2C], CC);
    BigEndian::write_u16(&mut disk[0x2C..0x2E], 12);
    BigEndian::write_u16(&mut disk[0x32..0x34], 2);
    BigEndian::write_u16(&mut disk[0x34..0x36], ROOT_CLUSTER);
    BigEndian::write_u32(&mut disk[0x36..0x3A], ROOT_LEN);
    let fat_off = 64;
    let set = |d: &mut [u8], cl: u16, val: u16| {
        let o = fat_off + cl as usize * 2;
        BigEndian::write_u16(&mut d[o..o + 2], val);
    };
    set(&mut disk, ROOT_CLUSTER, 0);
    set(&mut disk, FILE_CLUSTER, 0);
    let free_order: &[u16] = &[2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
    for w in free_order.windows(2) {
        set(&mut disk, w[0], w[1]);
    }
    set(&mut disk, *free_order.last().unwrap(), 0);
    let dir_off = ROOT_CLUSTER as usize * CLUSTER_SIZE;
    let slot1 = dir_off + 64;
    BigEndian::write_u32(&mut disk[slot1..slot1 + 4], 16);
    BigEndian::write_u16(&mut disk[slot1 + 0x0E..slot1 + 0x10], 7);
    disk[slot1 + 0x10..slot1 + 0x17].copy_from_slice(b"PREEXST");
    BigEndian::write_u16(&mut disk[slot1 + 0x3A..slot1 + 0x3C], FILE_CLUSTER);
    let file_off = FILE_CLUSTER as usize * CLUSTER_SIZE;
    disk[file_off..file_off + 16].copy_from_slice(b"qdos preexisting");
    disk
}

#[test]
fn editable_dispatch_via_qxlwin_string_round_trips_through_create_and_delete() {
    use rusty_backup::fs::filesystem::CreateFileOptions;
    let disk = build_qdos_qxlwin_disk_with_freelist();
    let cur = Cursor::new(disk);
    // String-keyed dispatch ("qxlwin") — the route the CLI / restore
    // pipeline takes when a QDOS volume is declared explicitly.
    let mut fs = rusty_backup::fs::open_editable_filesystem(cur, 0, 0, Some("qxlwin")).unwrap();
    assert_eq!(fs.fs_type(), "QDOS (QXL.WIN)");
    let root = fs.root().unwrap();
    let before = fs.list_directory(&root).unwrap();
    assert_eq!(before.len(), 1);
    assert_eq!(before[0].name, "PREEXST");

    let payload = b"editable dispatch round-trip ok";
    let new_entry = fs
        .create_file(
            &root,
            "NEWVIA_DISP",
            &mut payload.as_slice(),
            payload.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();
    assert_eq!(new_entry.size, payload.len() as u64);

    let after = fs.list_directory(&root).unwrap();
    assert_eq!(after.len(), 2);

    let new_in_listing = after.iter().find(|e| e.name == "NEWVIA_DISP").unwrap();
    let got = fs.read_file(new_in_listing, 1024).unwrap();
    assert_eq!(&got, payload);

    fs.delete_entry(&root, new_in_listing).unwrap();
    let post_delete = fs.list_directory(&root).unwrap();
    assert_eq!(post_delete.len(), 1);
    assert_eq!(post_delete[0].name, "PREEXST");

    fs.sync_metadata().unwrap();
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
