//! End-to-end CLI tests for the Sinclair QL / QDOS (QXL.WIN) spine.
//!
//! QDOS volumes don't ship as fixtures because rusty-backup has full
//! editable support — we synthesize a tiny QXL.WIN at test time using
//! the canonical layout (sQLux QDisk.c offsets + the per-file 64-byte
//! header convention) and drive it through `rb-cli`.
//!
//! Covers stages 8 (GUI/dispatch surface — exercised here via the
//! identical CLI open path) and 9 (CLI parity) of the per-format spine
//! for the QL core. Stage 6 (write-verified) round-trip is below.

use std::path::PathBuf;
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

/// Build a tiny 16-cluster (8 KB) QXL.WIN volume with one seeded file
/// `INIT` (user-visible content "rb-cli ql cli parity test") plus the
/// canonical free-cluster linked list (ffc=2, fc=11, chain 2→3→4→6→
/// 7→…→15→0; cluster 5 is allocated to INIT). Matches the layout the
/// engine emits for `create_file` — file_length on disk includes the
/// 64-byte per-file header.
fn build_qxlwin_volume() -> Vec<u8> {
    const CC: u16 = 16;
    const SPC: u16 = 1;
    const CLUSTER: usize = 512;
    const ROOT_CLUSTER: u16 = 1;
    const ROOT_LEN: u32 = 128;
    const FILE_CLUSTER: u16 = 5;
    const FILE_HEADER_BYTES: usize = 64;
    let payload = b"rb-cli ql cli parity test";
    let on_disk_len = FILE_HEADER_BYTES + payload.len(); // 64 + 25 = 89

    let mut disk = vec![0u8; CC as usize * CLUSTER];
    disk[0..4].copy_from_slice(b"QLWA");
    BigEndian::write_u16(&mut disk[0x04..0x06], 0x0005);
    let mut name = [b' '; 20];
    name[..6].copy_from_slice(b"CliQL ");
    disk[0x06..0x1A].copy_from_slice(&name);
    BigEndian::write_u16(&mut disk[0x22..0x24], SPC);
    BigEndian::write_u16(&mut disk[0x2A..0x2C], CC);
    BigEndian::write_u16(&mut disk[0x2C..0x2E], 11); // fc (one cluster allocated to INIT)
    BigEndian::write_u16(&mut disk[0x32..0x34], 2); // ffc — head of free list
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

    // Root dir slot 1 = INIT entry. file_length stored = total on disk.
    let slot1 = ROOT_CLUSTER as usize * CLUSTER + 64;
    BigEndian::write_u32(&mut disk[slot1..slot1 + 4], on_disk_len as u32);
    BigEndian::write_u16(&mut disk[slot1 + 0x0E..slot1 + 0x10], 4);
    disk[slot1 + 0x10..slot1 + 0x14].copy_from_slice(b"INIT");
    BigEndian::write_u16(&mut disk[slot1 + 0x3A..slot1 + 0x3C], FILE_CLUSTER);

    // Cluster 5: per-file header at byte 0..63 (mirror of dir entry),
    // payload starting at byte 64.
    let file_off = FILE_CLUSTER as usize * CLUSTER;
    BigEndian::write_u32(&mut disk[file_off..file_off + 4], on_disk_len as u32);
    BigEndian::write_u16(&mut disk[file_off + 0x0E..file_off + 0x10], 4);
    disk[file_off + 0x10..file_off + 0x14].copy_from_slice(b"INIT");
    BigEndian::write_u16(&mut disk[file_off + 0x3A..file_off + 0x3C], FILE_CLUSTER);
    let data_off = file_off + FILE_HEADER_BYTES;
    disk[data_off..data_off + payload.len()].copy_from_slice(payload);

    disk
}

fn write_volume(dir: &std::path::Path) -> PathBuf {
    let img = dir.join("ql_volume.win");
    std::fs::write(&img, build_qxlwin_volume()).unwrap();
    img
}

#[test]
fn rb_cli_inspect_on_qxlwin_volume_reports_qdos_superfloppy() {
    let dir = tempfile::tempdir().unwrap();
    let img = write_volume(dir.path());
    let out = run(&["inspect", img.to_str().unwrap()]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("Partition table: None"),
        "expected superfloppy in inspect, got:\n{stdout}"
    );
    assert!(
        stdout.contains("QDOS"),
        "expected QDOS in inspect, got:\n{stdout}"
    );
}

#[test]
fn rb_cli_ls_on_qxlwin_volume_finds_seeded_file_with_user_visible_size() {
    let dir = tempfile::tempdir().unwrap();
    let img = write_volume(dir.path());
    let out = run(&["ls", img.to_str().unwrap()]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("INIT"),
        "expected INIT file in ls, got:\n{stdout}"
    );
    // Reported size = on-disk - 64 (per-file header stripped) = 25 bytes.
    assert!(
        stdout.contains("25"),
        "expected user-visible size 25 in ls, got:\n{stdout}"
    );
}

#[test]
fn rb_cli_get_extracts_seeded_file_byte_exact() {
    let dir = tempfile::tempdir().unwrap();
    let img = write_volume(dir.path());
    let dst = dir.path().join("init_out.txt");
    let _ = run(&["get", img.to_str().unwrap(), "INIT", dst.to_str().unwrap()]);
    let extracted = std::fs::read(&dst).unwrap();
    assert_eq!(&extracted, b"rb-cli ql cli parity test");
}

#[test]
fn rb_cli_put_then_get_round_trips_a_new_file() {
    let dir = tempfile::tempdir().unwrap();
    let img = write_volume(dir.path());
    let src = dir.path().join("payload.txt");
    let payload = b"the new file via rb-cli put";
    std::fs::write(&src, payload).unwrap();
    let _ = run(&["put", img.to_str().unwrap(), src.to_str().unwrap(), "/NEW"]);
    let out = run(&["ls", img.to_str().unwrap()]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("NEW"),
        "expected NEW after put, got:\n{stdout}"
    );

    let dst = dir.path().join("new_back.txt");
    let _ = run(&["get", img.to_str().unwrap(), "NEW", dst.to_str().unwrap()]);
    let got = std::fs::read(&dst).unwrap();
    assert_eq!(&got, payload);
}

#[test]
fn rb_cli_rm_deletes_seeded_file() {
    let dir = tempfile::tempdir().unwrap();
    let img = write_volume(dir.path());
    let _ = run(&["rm", img.to_str().unwrap(), "INIT"]);
    let out = run(&["ls", img.to_str().unwrap()]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        !stdout.contains("INIT"),
        "expected INIT to be gone after rm, got:\n{stdout}"
    );
}
