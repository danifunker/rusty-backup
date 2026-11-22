//! `rb-cli` parity tests for Acorn DFS (BBC Micro / Acorn Electron MiSTer
//! cores, single-sided `.ssd`).
//!
//! Fixture `test_bbc_dfs.ssd.zst` is a 40-track (100 KB) single-sided DFS
//! image holding two known files (original test content):
//!   README (122 bytes)
//!   DATA   (2600 bytes, 11 sectors — multi-sector contiguous extent)
//!
//! The on-disk format and the read/write paths were cross-validated this
//! session bidirectionally against an independent clean-room Python DFS
//! reader/writer (transcribed from the BeebWiki Acorn DFS spec): the engine
//! reads the oracle's disks byte-exact and the oracle reads the engine's
//! `put`/`rm` output byte-exact, including locked files, non-`$` directories,
//! and real load/exec addresses. These tests pin the rb-cli surface
//! (inspect / ls / get / put / rm) end-to-end on a flat `.ssd`.

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

fn fixture_to(tmp: &Path) -> PathBuf {
    let img = tmp.join("disk.ssd");
    let compressed = std::fs::read("tests/fixtures/test_bbc_dfs.ssd.zst").expect("read fixture");
    let mut dec =
        zstd::stream::read::Decoder::new(std::io::Cursor::new(compressed)).expect("zstd decoder");
    let mut bytes = Vec::new();
    dec.read_to_end(&mut bytes).expect("decompress");
    std::fs::write(&img, &bytes).expect("write fixture out");
    img
}

#[test]
fn inspect_reports_acorn_dfs() {
    let tmp = tempfile::tempdir().unwrap();
    let img = fixture_to(tmp.path());
    let stdout =
        String::from_utf8_lossy(&run(&["inspect", img.to_str().unwrap()]).stdout).into_owned();
    assert!(
        stdout.contains("Acorn DFS"),
        "inspect missing Acorn DFS:\n{stdout}"
    );
}

#[test]
fn ls_finds_seeded_files() {
    let tmp = tempfile::tempdir().unwrap();
    let img = fixture_to(tmp.path());
    let stdout = String::from_utf8_lossy(&run(&["ls", img.to_str().unwrap()]).stdout).into_owned();
    assert!(stdout.contains("README"), "ls missing README:\n{stdout}");
    assert!(stdout.contains("DATA"), "ls missing DATA:\n{stdout}");
}

#[test]
fn get_extracts_multisector_file_byte_exact() {
    let tmp = tempfile::tempdir().unwrap();
    let img = fixture_to(tmp.path());
    let dst = tmp.path().join("data.out");
    run(&["get", img.to_str().unwrap(), "DATA", dst.to_str().unwrap()]);
    let got = std::fs::read(&dst).unwrap();
    assert_eq!(got.len(), 2600);
    assert_eq!(
        sha256_hex(&got),
        "f8263d6c822a7605d567923664d61e4691da7089b38f279ad4680c95ce527aa6"
    );
}

#[test]
fn put_get_rm_round_trip_on_flat_ssd() {
    let tmp = tempfile::tempdir().unwrap();
    let img = fixture_to(tmp.path());

    // A multi-sector payload (900 bytes -> 4 sectors) with a valid DFS name.
    let payload: Vec<u8> = (0..900).map(|i| (i * 17 % 256) as u8).collect();
    let host = tmp.path().join("payload.bin");
    std::fs::write(&host, &payload).unwrap();
    run(&[
        "put",
        img.to_str().unwrap(),
        host.to_str().unwrap(),
        "NEWPROG",
    ]);

    let listing = String::from_utf8_lossy(&run(&["ls", img.to_str().unwrap()]).stdout).into_owned();
    assert!(listing.contains("NEWPROG"), "put file missing:\n{listing}");

    let back = tmp.path().join("back.bin");
    run(&[
        "get",
        img.to_str().unwrap(),
        "NEWPROG",
        back.to_str().unwrap(),
    ]);
    assert_eq!(std::fs::read(&back).unwrap(), payload);

    run(&["rm", img.to_str().unwrap(), "NEWPROG"]);
    let after = String::from_utf8_lossy(&run(&["ls", img.to_str().unwrap()]).stdout).into_owned();
    assert!(!after.contains("NEWPROG"), "rm left file behind:\n{after}");
    assert!(after.contains("README"), "rm clobbered README:\n{after}");
    // The pre-existing multi-sector file must still extract byte-exact.
    let data_back = tmp.path().join("data2.out");
    run(&[
        "get",
        img.to_str().unwrap(),
        "DATA",
        data_back.to_str().unwrap(),
    ]);
    assert_eq!(
        sha256_hex(&std::fs::read(&data_back).unwrap()),
        "f8263d6c822a7605d567923664d61e4691da7089b38f279ad4680c95ce527aa6"
    );
}
