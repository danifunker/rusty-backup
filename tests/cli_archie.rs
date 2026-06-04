//! End-to-end CLI tests for the Acorn Archimedes / ADFS spine.
//!
//! Synthesizes a tiny 800 KB E-format ADFS floppy at test time (the
//! same shape as the inline `build_eformat_with_one_file` fixture in
//! `src/fs/adfs.rs`) and drives it through `rb-cli`.
//!
//! Scope is CURRENTLY READ-ONLY: ADFS write path is parked at
//! OPEN-WORK §7 behind the FSM walker (see the session log in
//! `docs/mister_filesystem_implementation_plan.md`). `inspect` /
//! `ls` / `get` go through the engine's contiguous-extent assumption,
//! which holds for the synthetic fixture and for freshly-written real
//! discs but not for fragmented production volumes — the latter need
//! the FSM walker. Stage 9 (CLI parity) for the Archie row is covered
//! here; stage 6 (write-verified) ships once the FSM walker lands.

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

/// Build an 800 KB E-format ADFS floppy with one root-level file
/// "HELLO" of 30 bytes at sector 0x10. DR at byte 0xDC0 (legacy floppy
/// boot-block + 0x1C0). Root directory ($) at byte 0x8000 with the
/// "Hugo" magic — small-format dir, 47-entry capacity.
fn build_adfs_eformat_disk() -> Vec<u8> {
    const TOTAL_BYTES: usize = 800 * 1024;
    const SECTOR_SIZE: u32 = 1024;
    const FILE_SECTOR: u32 = 0x10;
    const ROOT_SECTOR: u32 = 0x20;
    let mut disk = vec![0u8; TOTAL_BYTES];

    let dr_off = 0xDC0usize;
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
    disk[dr_off + 0x16..dr_off + 0x20].copy_from_slice(b"CliArchie ");

    let root_off = (ROOT_SECTOR * SECTOR_SIZE) as usize;
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
    let file_byte_off = FILE_SECTOR * SECTOR_SIZE;
    disk[e_off + 22] = (file_byte_off & 0xFF) as u8;
    disk[e_off + 23] = ((file_byte_off >> 8) & 0xFF) as u8;
    disk[e_off + 24] = ((file_byte_off >> 16) & 0xFF) as u8;
    disk[e_off + 25] = 0x03;

    let payload = b"acorn archie cli parity test\x0d\x0a";
    disk[file_byte_off as usize..file_byte_off as usize + payload.len()].copy_from_slice(payload);
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
