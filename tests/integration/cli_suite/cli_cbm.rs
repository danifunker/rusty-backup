//! `rb-cli` parity tests for Commodore CBM DOS (1541 `.d64`).
//!
//! Drives the `rb-cli` binary against a committed fixture
//! `test_cbm_1541.d64.zst` — a 1541 disk produced by the independent
//! Python `d64` reference library, holding two known files:
//! HELLO (PRG, 25 bytes) = `\x01\x08HELLO FROM CBM FIXTURE\r`, and
//! README (SEQ, 600 bytes) = `COMMODORE 64 BASIC V2  ` repeated.
//!
//! Because the fixture was written by an *independent* tool, the read
//! tests double as an external-oracle cross-check of our engine. The
//! write test (`put` -> `ls` -> `get` -> `rm`) exercises the full
//! source_reader -> open_editable_filesystem dispatch chain and proves
//! the on-disk format we emit round-trips through our own reader.
//!
//! Covers stages 6 (write-verified) and 9 (CLI parity) of the per-format
//! spine in `docs/mister_filesystem_implementation_plan.md`.

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

/// Decompress a tests/fixtures/*.zst file into `dst`.
fn decompress_fixture(name: &str, dst: &Path) {
    let path = format!("tests/fixtures/{name}");
    let compressed = std::fs::read(&path).expect("read fixture");
    let mut decoder =
        zstd::stream::read::Decoder::new(std::io::Cursor::new(compressed)).expect("zstd decoder");
    let mut bytes = Vec::new();
    decoder.read_to_end(&mut bytes).expect("decompress");
    std::fs::write(dst, &bytes).expect("write fixture out");
}

fn fixture_to(tmp: &Path) -> PathBuf {
    let img = tmp.join("cbm.d64");
    decompress_fixture("test_cbm_1541.d64.zst", &img);
    img
}

#[test]
fn rb_cli_inspect_reports_cbm_dos_superfloppy() {
    let tmp = tempfile::tempdir().unwrap();
    let img = fixture_to(tmp.path());
    let out = run(&["inspect", img.to_str().unwrap()]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("CBM DOS"),
        "inspect should report CBM DOS, got:\n{stdout}"
    );
}

#[test]
fn rb_cli_ls_finds_seeded_files() {
    let tmp = tempfile::tempdir().unwrap();
    let img = fixture_to(tmp.path());
    let out = run(&["ls", img.to_str().unwrap()]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("HELLO"), "ls missing HELLO:\n{stdout}");
    assert!(stdout.contains("README"), "ls missing README:\n{stdout}");
}

#[test]
fn rb_cli_get_extracts_oracle_written_file_byte_exact() {
    let tmp = tempfile::tempdir().unwrap();
    let img = fixture_to(tmp.path());
    let dst = tmp.path().join("hello.out");
    run(&["get", img.to_str().unwrap(), "HELLO", dst.to_str().unwrap()]);
    let got = std::fs::read(&dst).expect("read extracted");
    // The exact bytes the independent `d64` library wrote.
    assert_eq!(got, b"\x01\x08HELLO FROM CBM FIXTURE\r");
}

#[test]
fn rb_cli_put_get_rm_round_trip() {
    let tmp = tempfile::tempdir().unwrap();
    let img = fixture_to(tmp.path());

    // Write a host file in.
    let payload: Vec<u8> = (0..777).map(|i| (i * 11 % 256) as u8).collect();
    let host = tmp.path().join("payload.bin");
    std::fs::write(&host, &payload).unwrap();
    run(&[
        "put",
        img.to_str().unwrap(),
        host.to_str().unwrap(),
        "NEWPRG",
    ]);

    // It shows up alongside the seeded files.
    let listing = String::from_utf8_lossy(&run(&["ls", img.to_str().unwrap()]).stdout).into_owned();
    assert!(listing.contains("NEWPRG"), "put file missing:\n{listing}");

    // Read it back byte-exact.
    let back = tmp.path().join("back.bin");
    run(&[
        "get",
        img.to_str().unwrap(),
        "NEWPRG",
        back.to_str().unwrap(),
    ]);
    assert_eq!(std::fs::read(&back).unwrap(), payload);

    // Delete it; the seeded files survive.
    run(&["rm", img.to_str().unwrap(), "NEWPRG"]);
    let after = String::from_utf8_lossy(&run(&["ls", img.to_str().unwrap()]).stdout).into_owned();
    assert!(!after.contains("NEWPRG"), "rm left file behind:\n{after}");
    assert!(after.contains("HELLO"), "rm clobbered HELLO:\n{after}");
    assert!(after.contains("README"), "rm clobbered README:\n{after}");
}
