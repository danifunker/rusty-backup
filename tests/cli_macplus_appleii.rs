//! `rb-cli` parity tests for MacPlus MFS + Apple-II DOS 3.3.
//!
//! Both filesystems lack a Linux apt tool that produces real volumes,
//! so these tests build synthetic disks programmatically and prove
//! `rb-cli inspect / ls / get / put` work end-to-end through the
//! source_reader -> open_filesystem dispatch chain.
//!
//! Covers stages 6 (write-verified via rb-cli put -> get round-trip)
//! and 9 (CLI parity) of the per-format spine.

use std::path::{Path, PathBuf};
use std::process::Command;

use byteorder::{BigEndian, ByteOrder};

fn cli_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_rb-cli"))
}

fn run(args: &[&str]) -> std::process::Output {
    let out = Command::new(cli_bin())
        .args(args)
        .output()
        .expect("spawn rb-cli");
    if !out.status.success() {
        panic!(
            "command {args:?} failed: status={:?}\nstdout:\n{}\nstderr:\n{}",
            out.status,
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        );
    }
    out
}

// ----------------------------------------------------------------------------
// MFS synthetic-fixture writer
// ----------------------------------------------------------------------------

/// Build a 16 KiB synthetic MFS volume with one file "Hello" containing
/// b"hi from mfs cli". Mirrors src/fs/mfs.rs::tests::build_test_mfs.
fn write_mfs_volume(path: &Path) {
    const SECTOR: usize = 512;
    const NUM_SECTORS: usize = 32;
    const ALLOC_BLOCK_SIZE: u32 = 1024;
    const FIRST_ALLOC_BLOCK_SECTOR: u16 = 5;
    const DIR_START: u16 = 3;
    const DIR_LEN: u16 = 2;
    const NUM_ALLOC_BLOCKS: u16 = 8;

    let mut disk = vec![0u8; NUM_SECTORS * SECTOR];

    // MDB.
    let mdb = &mut disk[1024..1024 + 512];
    BigEndian::write_u16(&mut mdb[0..2], 0xD2D7);
    BigEndian::write_u16(&mut mdb[12..14], 1); // num files
    BigEndian::write_u16(&mut mdb[14..16], DIR_START);
    BigEndian::write_u16(&mut mdb[16..18], DIR_LEN);
    BigEndian::write_u16(&mut mdb[18..20], NUM_ALLOC_BLOCKS);
    BigEndian::write_u32(&mut mdb[20..24], ALLOC_BLOCK_SIZE);
    BigEndian::write_u32(&mut mdb[24..28], ALLOC_BLOCK_SIZE);
    BigEndian::write_u16(&mut mdb[28..30], FIRST_ALLOC_BLOCK_SECTOR);
    BigEndian::write_u32(&mut mdb[30..34], 2);
    BigEndian::write_u16(&mut mdb[34..36], NUM_ALLOC_BLOCKS - 1);
    let vname = b"CliTest";
    mdb[36] = vname.len() as u8;
    mdb[37..37 + vname.len()].copy_from_slice(vname);

    // Volume map: block 2 -> 1 (end of chain).
    // 12-bit packing per src/fs/mfs.rs::map_get: for even block 2,
    // bit_off = 24, byte_off = 3. map_bytes[3] = value >> 4 = 0;
    // map_bytes[4] high nibble = value & 0x0F.
    let map_start = 1024 + 36 + 1 + vname.len();
    disk[map_start + 3] = 0;
    disk[map_start + 4] = 0x10;

    // Directory entry at sector 3 for "Hello".
    let dir_off = 3 * SECTOR;
    disk[dir_off] = 0x80;
    disk[dir_off + 2..dir_off + 6].copy_from_slice(b"TEXT");
    disk[dir_off + 6..dir_off + 10].copy_from_slice(b"ttxt");
    BigEndian::write_u32(&mut disk[dir_off + 18..dir_off + 22], 1);
    BigEndian::write_u16(&mut disk[dir_off + 22..dir_off + 24], 2);
    BigEndian::write_u32(&mut disk[dir_off + 24..dir_off + 28], 15);
    BigEndian::write_u32(&mut disk[dir_off + 28..dir_off + 32], ALLOC_BLOCK_SIZE);
    let name = b"Hello";
    disk[dir_off + 50] = name.len() as u8;
    disk[dir_off + 51..dir_off + 51 + name.len()].copy_from_slice(name);

    // Data: block 2 at sector 5.
    let data_off = FIRST_ALLOC_BLOCK_SECTOR as usize * SECTOR;
    disk[data_off..data_off + 15].copy_from_slice(b"hi from mfs cli");

    std::fs::write(path, &disk).unwrap();
}

// ----------------------------------------------------------------------------
// DOS 3.3 synthetic-fixture writer
// ----------------------------------------------------------------------------

const APPLE_II_DISK_BYTES: usize = 143_360;
const APPLE_II_TRACK_BYTES: usize = 16 * 256;
const APPLE_II_SECTOR_BYTES: usize = 256;

/// Build a 140 KB DOS 3.3 disk with one type-T file "HI" containing
/// b"hi from dos33 cli". Mirrors the DOS-3.3 e2e test builder.
fn write_dos33_volume(path: &Path) {
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
    v[0x35] = 16;
    v[0x36] = 0x00;
    v[0x37] = 0x01;
    for t in 0..35 {
        let row = 0x38 + t * 4;
        v[row] = 0xFF;
        v[row + 1] = 0xFF;
    }
    let mut mark_used = |t: u8, s: u8| {
        let row = 0x38 + (t as usize) * 4;
        if s >= 8 {
            v[row] &= !(1 << (s - 8));
        } else {
            v[row + 1] &= !(1 << s);
        }
    };
    mark_used(17, 0);
    for s in 1..=15 {
        mark_used(17, s);
    }
    mark_used(1, 0);
    mark_used(2, 0);

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

    // Single file entry at the head of catalog S15.
    let cat15_off = 17 * APPLE_II_TRACK_BYTES + 15 * APPLE_II_SECTOR_BYTES;
    let e0 = cat15_off + 0x0B;
    disk[e0] = 1;
    disk[e0 + 1] = 0;
    disk[e0 + 2] = 0; // TYPE_T
    let name = b"HI";
    let name_off = e0 + 3;
    for i in 0..30 {
        disk[name_off + i] = if i < name.len() { name[i] | 0x80 } else { 0xA0 };
    }
    disk[e0 + 33] = 2;
    disk[e0 + 34] = 0;

    // T/S list at T1S0.
    let tsl_off = APPLE_II_TRACK_BYTES; // T1S0
    let l = &mut disk[tsl_off..tsl_off + APPLE_II_SECTOR_BYTES];
    l[0x01] = 0;
    l[0x02] = 0;
    l[0x0C] = 2;
    l[0x0D] = 0;

    // Data sector at T2S0.
    let data_off = 2 * APPLE_II_TRACK_BYTES;
    let payload = b"hi from dos33 cli";
    disk[data_off..data_off + payload.len()].copy_from_slice(payload);

    std::fs::write(path, &disk).unwrap();
}

// ----------------------------------------------------------------------------
// MFS CLI parity
// ----------------------------------------------------------------------------

#[test]
fn rb_cli_inspect_on_mfs_volume_reports_mfs() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("mfs.dsk");
    write_mfs_volume(&img);
    let out = run(&["inspect", img.to_str().unwrap()]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("Partition table: None"),
        "expected superfloppy in inspect, got:\n{stdout}"
    );
    assert!(
        stdout.contains("MFS"),
        "expected MFS partition in inspect, got:\n{stdout}"
    );
}

#[test]
fn rb_cli_ls_on_mfs_volume_lists_seeded_file() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("mfs.dsk");
    write_mfs_volume(&img);
    let out = run(&["ls", img.to_str().unwrap(), "/"]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("Hello"),
        "expected Hello in MFS ls, got:\n{stdout}"
    );
}

#[test]
fn rb_cli_get_extracts_mfs_file_byte_exactly() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("mfs.dsk");
    write_mfs_volume(&img);
    let dst = dir.path().join("hello.txt");
    run(&[
        "get",
        img.to_str().unwrap(),
        "/Hello",
        dst.to_str().unwrap(),
    ]);
    let bytes = std::fs::read(&dst).unwrap();
    assert_eq!(&bytes[..15], b"hi from mfs cli");
}

#[test]
fn rb_cli_put_then_get_round_trips_new_file_into_mfs() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("mfs.dsk");
    write_mfs_volume(&img);

    let host = dir.path().join("Round");
    let payload = b"round trip via rb-cli put on MFS";
    std::fs::write(&host, payload).unwrap();
    run(&[
        "put",
        img.to_str().unwrap(),
        host.to_str().unwrap(),
        "/Round",
    ]);
    let dst = dir.path().join("round_out");
    run(&[
        "get",
        img.to_str().unwrap(),
        "/Round",
        dst.to_str().unwrap(),
    ]);
    let got = std::fs::read(&dst).unwrap();
    assert_eq!(&got[..payload.len()], payload);

    // The original Hello must still be there.
    let dst2 = dir.path().join("hello_after");
    run(&[
        "get",
        img.to_str().unwrap(),
        "/Hello",
        dst2.to_str().unwrap(),
    ]);
    let still = std::fs::read(&dst2).unwrap();
    assert_eq!(&still[..15], b"hi from mfs cli");
}

// ----------------------------------------------------------------------------
// DOS 3.3 CLI parity
// ----------------------------------------------------------------------------

#[test]
fn rb_cli_inspect_on_dos33_volume_reports_dos_3_3() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("dos33.dsk");
    write_dos33_volume(&img);
    let out = run(&["inspect", img.to_str().unwrap()]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("Partition table: None"),
        "expected superfloppy in inspect, got:\n{stdout}"
    );
    assert!(
        stdout.contains("DOS 3.3"),
        "expected DOS 3.3 in inspect, got:\n{stdout}"
    );
}

#[test]
fn rb_cli_ls_on_dos33_volume_lists_seeded_file() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("dos33.dsk");
    write_dos33_volume(&img);
    let out = run(&["ls", img.to_str().unwrap(), "/"]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("HI"),
        "expected HI file in DOS 3.3 ls, got:\n{stdout}"
    );
}

#[test]
fn rb_cli_get_extracts_dos33_file_byte_exactly() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("dos33.dsk");
    write_dos33_volume(&img);
    let dst = dir.path().join("hi.txt");
    run(&["get", img.to_str().unwrap(), "/HI", dst.to_str().unwrap()]);
    let bytes = std::fs::read(&dst).unwrap();
    // Type T (text) means no binary-header strip — bytes come back
    // padded to the 256-byte sector boundary.
    assert_eq!(&bytes[..17], b"hi from dos33 cli");
}

#[test]
fn rb_cli_put_then_get_round_trips_new_file_into_dos33() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("dos33.dsk");
    write_dos33_volume(&img);

    let host = dir.path().join("ROUND");
    let payload = b"round trip via rb-cli put on DOS 3.3";
    std::fs::write(&host, payload).unwrap();
    run(&[
        "put",
        img.to_str().unwrap(),
        host.to_str().unwrap(),
        "/ROUND",
    ]);
    let dst = dir.path().join("round_out");
    run(&[
        "get",
        img.to_str().unwrap(),
        "/ROUND",
        dst.to_str().unwrap(),
    ]);
    let got = std::fs::read(&dst).unwrap();
    assert_eq!(&got[..payload.len()], payload);

    // The original HI must still be there.
    let dst2 = dir.path().join("hi_after");
    run(&["get", img.to_str().unwrap(), "/HI", dst2.to_str().unwrap()]);
    let still = std::fs::read(&dst2).unwrap();
    assert_eq!(&still[..17], b"hi from dos33 cli");
}
