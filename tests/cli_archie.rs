//! End-to-end CLI tests for the Acorn Archimedes / ADFS spine.
//!
//! Synthesizes a tiny E-format ADFS floppy at test time (the same
//! shape as the inline `build_eformat_with_one_file` fixture in
//! `src/fs/adfs.rs`) and drives it through `rb-cli`.
//!
//! The fixture has a structurally valid FSM with three named
//! fragments (`0 = free`, `2 = root dir`, `5 = HELLO`), so the walker
//! resolves `dr.root = 0x200` and the HELLO entry's `indaddr = 0x500`
//! the same way it does on real arc-04 / CROS42 / ICEBIRD discs.

use std::path::PathBuf;
use std::process::Command;

use byteorder::{ByteOrder, LittleEndian};

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

/// Write a single bit at LE bit position `pos` in `buf`.
fn set_bit_le(buf: &mut [u8], pos: u32) {
    buf[(pos >> 3) as usize] |= 1 << (pos & 7);
}

/// Write `nbits` LE bits of `value` starting at bit `start`.
fn write_bits_le(buf: &mut [u8], start: u32, nbits: u32, value: u64) {
    for k in 0..nbits {
        let pos = start + k;
        if (value >> k) & 1 == 1 {
            set_bit_le(buf, pos);
        }
    }
}

/// Write a Filecore fragment of `length_bits` map bits at bit `start`:
/// `idlen` bits of `frag_id`, zeros, then a 1-bit terminator at the
/// tail. The same primitive the in-tree unit-test fixture uses.
fn write_frag(buf: &mut [u8], start: u32, idlen: u32, length_bits: u32, frag_id: u32) {
    write_bits_le(buf, start, idlen, frag_id as u64);
    set_bit_le(buf, start + length_bits - 1);
}

/// Build a tiny FSM-valid E-format ADFS floppy with one root-level
/// file "HELLO" (30 bytes). Mirrors the kernel `adfs_validate_dr0`
/// single-zone layout: 4-byte zone header + 60-byte DR at byte 0,
/// bitstream starting at bit 512 of zone 0.
///
/// FSM params: log2_secsize=10, idlen=15, log2bpmb=7, nzones=1,
/// zone_spare=1312 (zone_size_bits = 6880, map2blk = -3 ⇒ 1 map bit
/// = 128 B). Fragments laid out in zone 0:
///
/// ```text
/// bit 512: frag 0  length 16  (FSM + 1 padding sector)
/// bit 528: frag 2  length 16  (root dir at sectors 2..3)
/// bit 544: frag 5  length 16  (HELLO payload at sectors 4..5)
/// bit 560: frag 0  length 6352 (tail free; closes the bitstream)
/// ```
fn build_adfs_eformat_disk() -> Vec<u8> {
    const SECTOR_SIZE: usize = 1024;
    const TOTAL_BYTES: usize = 12 * SECTOR_SIZE;
    const ROOT_SECTOR: u32 = 2;
    const FILE_SECTOR: u32 = 4;
    const IDLEN: u32 = 15;
    let mut disk = vec![0u8; TOTAL_BYTES];

    // Disc Record at byte 4 (zone-0-embedded — kernel
    // `adfs_validate_dr0` path).
    let dr_off = 0x04usize;
    disk[dr_off] = 10; // log2(1024)
    disk[dr_off + 0x01] = 5;
    disk[dr_off + 0x02] = 2;
    disk[dr_off + 0x03] = 2;
    disk[dr_off + 0x04] = IDLEN as u8;
    disk[dr_off + 0x05] = 7; // log2bpmb -> 1 map bit = 128 B
    disk[dr_off + 0x09] = 1; // nzones
    LittleEndian::write_u16(&mut disk[dr_off + 0x0A..dr_off + 0x0C], 1312);
    // dr.root = (frag 2, lo=0) = 0x200
    LittleEndian::write_u32(&mut disk[dr_off + 0x0C..dr_off + 0x10], 0x200);
    LittleEndian::write_u32(&mut disk[dr_off + 0x10..dr_off + 0x14], TOTAL_BYTES as u32);
    LittleEndian::write_u16(&mut disk[dr_off + 0x14..dr_off + 0x16], 0xABCD);
    disk[dr_off + 0x16..dr_off + 0x20].copy_from_slice(b"CliArchie ");

    // Populate the FSM zone-0 bitstream.
    write_frag(&mut disk[..SECTOR_SIZE], 512, IDLEN, 16, 0); // sectors 0..1
    write_frag(&mut disk[..SECTOR_SIZE], 528, IDLEN, 16, 2); // root sectors 2..3
    write_frag(&mut disk[..SECTOR_SIZE], 544, IDLEN, 16, 5); // HELLO sectors 4..5
    write_frag(&mut disk[..SECTOR_SIZE], 560, IDLEN, 6352, 0); // tail free

    // Root directory at byte 2048 (sector 2): Hugo magic + 1 entry.
    let root_off = ROOT_SECTOR as usize * SECTOR_SIZE;
    disk[root_off + 1] = b'H';
    disk[root_off + 2] = b'u';
    disk[root_off + 3] = b'g';
    disk[root_off + 4] = b'o';
    let e_off = root_off + 5;
    let name = b"HELLO\x00\x00\x00\x00\x00";
    disk[e_off..e_off + 10].copy_from_slice(name);
    LittleEndian::write_u32(&mut disk[e_off + 10..e_off + 14], 0xFFFFFFFF);
    LittleEndian::write_u32(&mut disk[e_off + 14..e_off + 18], 0);
    LittleEndian::write_u32(&mut disk[e_off + 18..e_off + 22], 30);
    // indaddr = (frag 5, lo=0) = 0x500
    disk[e_off + 22] = 0x00;
    disk[e_off + 23] = 0x05;
    disk[e_off + 24] = 0x00;
    disk[e_off + 25] = 0x03; // R + W

    // File payload at byte 4096 (sector 4).
    let file_byte_off = FILE_SECTOR as usize * SECTOR_SIZE;
    let payload = b"acorn archie cli parity test\x0d\x0a";
    disk[file_byte_off..file_byte_off + payload.len()].copy_from_slice(payload);
    disk
}

fn write_volume(dir: &std::path::Path) -> PathBuf {
    let img = dir.join("archie.adf");
    std::fs::write(&img, build_adfs_eformat_disk()).unwrap();
    img
}

#[test]
fn rb_cli_inspect_on_eformat_floppy_reports_adfs_superfloppy() {
    let dir = tempfile::tempdir().unwrap();
    let img = write_volume(dir.path());
    let out = run(&["inspect", img.to_str().unwrap()]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("Partition table: None"),
        "expected superfloppy in inspect, got:\n{stdout}"
    );
    assert!(
        stdout.contains("ADFS"),
        "expected ADFS in inspect, got:\n{stdout}"
    );
}

#[test]
fn rb_cli_ls_on_eformat_floppy_finds_seeded_file() {
    let dir = tempfile::tempdir().unwrap();
    let img = write_volume(dir.path());
    let out = run(&["ls", img.to_str().unwrap()]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("HELLO"),
        "expected HELLO in ls, got:\n{stdout}"
    );
}

#[test]
fn rb_cli_get_extracts_seeded_file_byte_exact() {
    let dir = tempfile::tempdir().unwrap();
    let img = write_volume(dir.path());
    let dst = dir.path().join("hello_out.txt");
    let _ = run(&["get", img.to_str().unwrap(), "HELLO", dst.to_str().unwrap()]);
    let extracted = std::fs::read(&dst).unwrap();
    assert_eq!(&extracted, b"acorn archie cli parity test\x0d\x0a");
}
