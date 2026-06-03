//! End-to-end tests for Apple-II DOS 3.3 dispatch.
//!
//! No Linux apt tool produces fresh DOS 3.3 disk images; CiderPress2 and
//! a2kit (our port reference) are the canonical authoring path on
//! Windows / via Rust. These tests build a minimal synthetic DOS 3.3
//! disk in-memory and assert that the partition + filesystem dispatch
//! chain routes both DOS-order (`.do`) and ProDOS-order (`.po`)
//! versions correctly:
//!
//!   `PartitionTable::detect` -> `PartitionTable::None { fs_hint: "DOS 3.3" }`
//!                            -> `open_filesystem(type_byte=0)` auto-detect
//!                            -> `AppleDosFilesystem::open` + read a known file
//!
//! Plus a file-level test: writing a `.po` file to a tempdir and
//! opening it through `source_reader::open_read` produces a DOS-order
//! byte stream that opens cleanly via the same dispatch.

use std::io::{Cursor, Write};

use rusty_backup::fs::apple_dos::{TYPE_B, TYPE_T};
use rusty_backup::model::source_reader;
use rusty_backup::partition::PartitionTable;
use rusty_backup::rbformats::containers::sector_order::{
    convert_do_to_po_bytes, APPLE_II_DISK_BYTES, APPLE_II_SECTORS_PER_TRACK, APPLE_II_SECTOR_BYTES,
    APPLE_II_TRACK_BYTES,
};

/// Build a 140 KB DOS-order DOS 3.3 disk with:
/// - VTOC at T17S0 (the standard fields + bitmap with the catalog +
///   T/S list + data sectors marked used)
/// - Full 15-sector catalog chain on T17 (S15 -> S14 -> ... -> S1)
/// - One file "HI.TXT" of type B (binary) whose payload is b"hi"
///
/// Both `apple_dos.rs`'s catalog walker and `sector_order::
/// detect_sector_order`'s chain hop-counter rely on the full catalog
/// chain being populated, so we lay down the standard 15-sector layout.
fn build_dos_disk() -> Vec<u8> {
    let mut disk = vec![0u8; APPLE_II_DISK_BYTES];

    // VTOC at T17S0.
    let vtoc_off = 17 * APPLE_II_TRACK_BYTES;
    let v = &mut disk[vtoc_off..vtoc_off + APPLE_II_SECTOR_BYTES];
    v[0x01] = 17;
    v[0x02] = 15;
    v[0x03] = 3;
    v[0x06] = 254;
    v[0x27] = 122;
    v[0x34] = 35;
    v[0x35] = APPLE_II_SECTORS_PER_TRACK as u8;
    v[0x36] = 0x00;
    v[0x37] = 0x01;
    // Bitmap: mark every sector free, then mark the 4 sectors we use.
    for t in 0..35 {
        let row = 0x38 + t * 4;
        v[row] = 0xFF; // sectors 15..8 free
        v[row + 1] = 0xFF; // sectors 7..0 free
    }
    let mut mark_used = |t: u8, s: u8| {
        let row = 0x38 + (t as usize) * 4;
        if s >= 8 {
            v[row] &= !(1 << (s - 8));
        } else {
            v[row + 1] &= !(1 << s);
        }
    };
    mark_used(17, 0); // VTOC
    for s in 1..=15 {
        mark_used(17, s); // every catalog sector
    }
    mark_used(1, 0); // T/S list for HI.TXT
    mark_used(2, 0); // data sector for HI.TXT

    // Catalog chain T17 S15 -> S14 -> ... -> S1 -> end.
    for cur_sec in 1..=15u8 {
        let off = 17 * APPLE_II_TRACK_BYTES + (cur_sec as usize) * APPLE_II_SECTOR_BYTES;
        let cat = &mut disk[off..off + APPLE_II_SECTOR_BYTES];
        if cur_sec == 1 {
            cat[0x01] = 0;
            cat[0x02] = 0;
        } else {
            cat[0x01] = 17;
            cat[0x02] = cur_sec - 1;
        }
    }

    // One file entry at the head of the first catalog sector (S15).
    let cat15_off = 17 * APPLE_II_TRACK_BYTES + 15 * APPLE_II_SECTOR_BYTES;
    let e0 = cat15_off + 0x0B;
    disk[e0] = 1; // first T/S list track
    disk[e0 + 1] = 0; // first T/S list sector
    disk[e0 + 2] = TYPE_B;
    // Filename "HI.TXT" in Apple-ASCII high-bit-set, space-padded.
    let name = b"HI.TXT";
    let name_off = e0 + 3;
    for i in 0..30 {
        disk[name_off + i] = if i < name.len() { name[i] | 0x80 } else { 0xA0 };
    }
    disk[e0 + 33] = 2; // length sectors LE
    disk[e0 + 34] = 0;

    // T/S list at T1S0.
    let tsl_off = APPLE_II_TRACK_BYTES; // T1S0
    let l = &mut disk[tsl_off..tsl_off + APPLE_II_SECTOR_BYTES];
    l[0x01] = 0;
    l[0x02] = 0;
    l[0x0C] = 2; // first data sector at T2S0
    l[0x0D] = 0;

    // Data sector at T2S0 with 4-byte binary header + b"hi" payload.
    let data_off = 2 * APPLE_II_TRACK_BYTES;
    let d = &mut disk[data_off..data_off + APPLE_II_SECTOR_BYTES];
    d[0] = 0x00; // load address LE 0x0800
    d[1] = 0x08;
    d[2] = 2; // payload length LE
    d[3] = 0;
    d[4] = b'h';
    d[5] = b'i';

    disk
}

#[test]
fn do_disk_is_detected_as_dos_3_3() {
    let disk = build_dos_disk();
    let mut cur = Cursor::new(disk);
    let table = PartitionTable::detect(&mut cur).unwrap();
    assert_eq!(table.type_name(), "None", "DOS 3.3 disks are superfloppies");
    let parts = table.partitions();
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].type_name, "DOS 3.3");
    assert_eq!(parts[0].start_lba, 0);
    assert_eq!(parts[0].size_bytes, APPLE_II_DISK_BYTES as u64);
}

#[test]
fn open_filesystem_autodetect_dispatches_to_apple_dos() {
    let disk = build_dos_disk();
    let cur = Cursor::new(disk.clone());
    let mut fs = rusty_backup::fs::open_filesystem(cur, 0, 0, None).unwrap();
    assert_eq!(fs.fs_type(), "DOS 3.3");

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    assert_eq!(entries.len(), 1);
    let hi = &entries[0];
    assert_eq!(hi.name, "HI.TXT");
    assert_eq!(hi.special_type.as_deref(), Some("B"));

    let payload = fs.read_file(hi, 1024).unwrap();
    assert_eq!(&payload, b"hi");
}

#[test]
fn po_file_is_converted_at_open_time() {
    // Write a .po file to a tempdir and open it via source_reader.
    // The .po byte stream is the same DOS 3.3 disk re-interleaved into
    // ProDOS-order; open_read should detect that and convert back to DO.
    let do_disk = build_dos_disk();
    let po_disk = convert_do_to_po_bytes(&do_disk).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("dosdisk.po");
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(&po_disk).unwrap();
    drop(f);

    // is_apple_ii_dsk_path should accept the file.
    assert!(source_reader::is_apple_ii_dsk_path(&path));

    let mut reader = source_reader::open_read(&path).unwrap();
    let table = PartitionTable::detect(&mut reader).unwrap();
    let parts = table.partitions();
    assert_eq!(parts.len(), 1);
    assert_eq!(
        parts[0].type_name, "DOS 3.3",
        ".po file should convert to DO before partition detect runs"
    );

    let mut fs = rusty_backup::fs::open_filesystem(reader, 0, 0, None).unwrap();
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    assert_eq!(entries.len(), 1);
    let hi = &entries[0];
    assert_eq!(hi.name, "HI.TXT");
    let payload = fs.read_file(hi, 1024).unwrap();
    assert_eq!(&payload, b"hi");
}

#[test]
fn do_file_passes_through_source_reader() {
    // Same disk as a .do file — open_read should pass through unchanged.
    let do_disk = build_dos_disk();
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("dosdisk.do");
    std::fs::write(&path, &do_disk).unwrap();

    assert!(source_reader::is_apple_ii_dsk_path(&path));
    let mut reader = source_reader::open_read(&path).unwrap();
    let table = PartitionTable::detect(&mut reader).unwrap();
    assert_eq!(table.partitions()[0].type_name, "DOS 3.3");
}

#[test]
fn text_file_round_trips_without_header_strip() {
    // Verify the binary-header strip only applies to type B. Build a
    // disk where the file is type T (text); the bytes should come back
    // verbatim, not have the first 4 bytes treated as a header.
    let mut disk = build_dos_disk();
    // Swap entry 0 to type T.
    let entry0 = 17 * APPLE_II_TRACK_BYTES + 15 * APPLE_II_SECTOR_BYTES + 0x0B;
    disk[entry0 + 2] = TYPE_T;

    let cur = Cursor::new(disk);
    let mut fs = rusty_backup::fs::open_filesystem(cur, 0, 0, None).unwrap();
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let raw = fs.read_file(&entries[0], 1024).unwrap();
    // For type T, the byte stream is the raw sector bytes — first 4 bytes
    // are the binary header we stamped at build time, NOT a payload.
    assert_eq!(raw.len(), 256, "one full sector returned for type T");
    assert_eq!(raw[0], 0x00); // load_address LE byte 0
    assert_eq!(raw[1], 0x08); // load_address LE byte 1
    assert_eq!(raw[2], 2); // length LE byte 0
    assert_eq!(raw[4], b'h');
    assert_eq!(raw[5], b'i');
}
