//! `rb-cli` parity tests for **double-sided** Acorn DFS (`.dsd`).
//!
//! A `.dsd` stores two independent single-sided DFS volumes track-interleaved
//! (logical slot for track `t`, side `s` is `t*2 + s`). The engine
//! de-interleaves the two sides into a flat `side0 ‖ side1` buffer and presents
//! them as two `Acorn DFS` partitions (`PartitionTable::Dsd`), so `@1` is side 0
//! and `@2` is side 1.
//!
//! Fixture `test_bbc_dfs_ds.dsd.zst` is a 2×40-track (200 KB) image built by
//! interleaving the single-sided `test_bbc_dfs.ssd` fixture onto both sides
//! (side 1's disc title tweaked to `SIDE2` so the two are distinguishable).
//! Each side holds the same two known files:
//!   README (122 bytes)
//!   DATA   (2600 bytes, 11 sectors — multi-sector contiguous extent)
//!
//! These pin the read path (inspect / ls / get on both sides) and the write
//! path (put on one side re-interleaves back to `.dsd`, leaving the other side
//! byte-untouched).

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;

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

fn sha256_hex(b: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(b);
    h.finalize().iter().map(|x| format!("{x:02x}")).collect()
}

const DATA_SHA: &str = "f8263d6c822a7605d567923664d61e4691da7089b38f279ad4680c95ce527aa6";

fn fixture_to(tmp: &Path) -> PathBuf {
    let img = tmp.join("disk.dsd");
    let compressed = std::fs::read("tests/fixtures/test_bbc_dfs_ds.dsd.zst").expect("read fixture");
    let mut dec =
        zstd::stream::read::Decoder::new(std::io::Cursor::new(compressed)).expect("zstd decoder");
    let mut bytes = Vec::new();
    dec.read_to_end(&mut bytes).expect("decompress");
    assert_eq!(bytes.len(), 204_800, "2x40-track DSD is 200 KB");
    std::fs::write(&img, &bytes).expect("write fixture out");
    img
}

#[test]
fn inspect_reports_two_acorn_dfs_partitions() {
    let tmp = tempfile::tempdir().unwrap();
    let img = fixture_to(tmp.path());
    let stdout =
        String::from_utf8_lossy(&run(&["inspect", img.to_str().unwrap()]).stdout).into_owned();
    assert!(
        stdout.contains("DSD"),
        "inspect missing DSD table:\n{stdout}"
    );
    // Two "Acorn DFS" rows — one per side.
    assert_eq!(
        stdout.matches("Acorn DFS").count(),
        2,
        "expected two Acorn DFS partitions:\n{stdout}"
    );
}

#[test]
fn ls_lists_files_on_both_sides() {
    let tmp = tempfile::tempdir().unwrap();
    let img = fixture_to(tmp.path());
    for side in ["@1", "@2"] {
        let spec = format!("{}{}", img.to_str().unwrap(), side);
        let stdout = String::from_utf8_lossy(&run(&["ls", &spec]).stdout).into_owned();
        assert!(
            stdout.contains("README"),
            "side {side} missing README:\n{stdout}"
        );
        assert!(
            stdout.contains("DATA"),
            "side {side} missing DATA:\n{stdout}"
        );
    }
}

#[test]
fn get_extracts_multisector_file_from_side_two() {
    let tmp = tempfile::tempdir().unwrap();
    let img = fixture_to(tmp.path());
    let spec = format!("{}@2", img.to_str().unwrap());
    let dst = tmp.path().join("data.out");
    run(&["get", &spec, "DATA", dst.to_str().unwrap()]);
    let got = std::fs::read(&dst).unwrap();
    assert_eq!(got.len(), 2600);
    assert_eq!(sha256_hex(&got), DATA_SHA);
}

#[test]
fn put_on_side_one_reinterleaves_and_leaves_side_two_intact() {
    let tmp = tempfile::tempdir().unwrap();
    let img = fixture_to(tmp.path());
    let side1 = format!("{}@1", img.to_str().unwrap());
    let side2 = format!("{}@2", img.to_str().unwrap());

    // Write a multi-sector payload onto side 0 only.
    let payload: Vec<u8> = (0..900).map(|i| (i * 17 % 256) as u8).collect();
    let host = tmp.path().join("payload.bin");
    std::fs::write(&host, &payload).unwrap();
    run(&["put", &side1, host.to_str().unwrap(), "NEWPROG"]);

    // It shows up on side 0 and reads back byte-exact (proves the re-interleaved
    // `.dsd` still de-interleaves cleanly on the next open).
    let listing = String::from_utf8_lossy(&run(&["ls", &side1]).stdout).into_owned();
    assert!(listing.contains("NEWPROG"), "put file missing:\n{listing}");
    let back = tmp.path().join("back.bin");
    run(&["get", &side1, "NEWPROG", back.to_str().unwrap()]);
    assert_eq!(std::fs::read(&back).unwrap(), payload);

    // Side 1 is untouched: same files, and DATA still byte-exact.
    let s2 = String::from_utf8_lossy(&run(&["ls", &side2]).stdout).into_owned();
    assert!(!s2.contains("NEWPROG"), "edit leaked onto side 2:\n{s2}");
    assert!(
        s2.contains("README") && s2.contains("DATA"),
        "side 2 files lost:\n{s2}"
    );
    let data_back = tmp.path().join("data2.out");
    run(&["get", &side2, "DATA", data_back.to_str().unwrap()]);
    assert_eq!(sha256_hex(&std::fs::read(&data_back).unwrap()), DATA_SHA);
}
