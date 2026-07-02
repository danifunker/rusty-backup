//! End-to-end tests for the `rb-cli` surface on AtariST images.
//!
//! Drives the `rb-cli` binary against the committed fixtures:
//!   - `test_atarist_floppy.msa.zst` — 720K FAT12 MSA floppy
//!   - `test_atarist_ahdi.img.zst`   — 32 MiB AHDI HDD (GEM FAT12 + BGM FAT16)
//!
//! Covers stages 8 (GUI / dispatch surface — exercised here via the CLI's
//! identical open path) and 9 (CLI parity) of the per-format spine for
//! AtariST. Stage 6 (write-verified) is below: writes a new FAT12 file
//! into the AHDI GEM partition via `rb-cli put`, re-reads it via
//! `rb-cli get` and asserts byte-identity.

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

fn try_run(args: &[&str]) -> std::process::Output {
    Command::new(cli_bin())
        .args(args)
        .output()
        .expect("spawn rb-cli")
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

// ----------------------------------------------------------------------------
// MSA floppy: rb-cli inspect / ls / get
// ----------------------------------------------------------------------------

#[test]
fn rb_cli_inspect_on_msa_fixture_reports_fat_superfloppy() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("atarist_floppy.msa");
    decompress_fixture("test_atarist_floppy.msa.zst", &img);

    let out = run(&["inspect", img.to_str().unwrap()]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    // Superfloppy => Partition table: None; one FAT partition.
    assert!(
        stdout.contains("Partition table: None"),
        "expected superfloppy in inspect, got:\n{stdout}"
    );
    assert!(
        stdout.contains("FAT"),
        "expected FAT partition in inspect, got:\n{stdout}"
    );
}

#[test]
fn rb_cli_ls_on_msa_fixture_finds_seeded_files() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("atarist_floppy.msa");
    decompress_fixture("test_atarist_floppy.msa.zst", &img);

    // `rb-cli ls` on the superfloppy lists the root directory.
    let out = run(&["ls", img.to_str().unwrap(), "/"]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.to_ascii_uppercase().contains("HELLO.TXT"),
        "expected HELLO.TXT in `rb-cli ls`, got:\n{stdout}"
    );
    assert!(
        stdout.to_ascii_uppercase().contains("PROG"),
        "expected PROG subdir in `rb-cli ls`, got:\n{stdout}"
    );
}

#[test]
fn rb_cli_get_extracts_seeded_file_from_msa_fixture() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("atarist_floppy.msa");
    decompress_fixture("test_atarist_floppy.msa.zst", &img);

    let dst = dir.path().join("extracted.txt");
    run(&[
        "get",
        img.to_str().unwrap(),
        "/HELLO.TXT",
        dst.to_str().unwrap(),
    ]);
    let bytes = std::fs::read(&dst).expect("extracted file present");
    assert_eq!(bytes, b"AtariST floppy hello via MSA round-trip.");
}

// ----------------------------------------------------------------------------
// AHDI disk: rb-cli inspect / ls (per partition)
// ----------------------------------------------------------------------------

#[test]
fn rb_cli_inspect_on_ahdi_fixture_lists_gem_and_bgm() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("ahdi.img");
    decompress_fixture("test_atarist_ahdi.img.zst", &img);

    let out = run(&["inspect", img.to_str().unwrap()]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("Partition table: AHDI"),
        "expected AHDI partition table in inspect, got:\n{stdout}"
    );
    assert!(
        stdout.contains("AHDI GEM"),
        "expected AHDI GEM in inspect, got:\n{stdout}"
    );
    assert!(
        stdout.contains("AHDI BGM"),
        "expected AHDI BGM in inspect, got:\n{stdout}"
    );
}

#[test]
fn rb_cli_ls_walks_both_ahdi_partitions() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("ahdi.img");
    decompress_fixture("test_atarist_ahdi.img.zst", &img);

    // Partition 1 (GEM, FAT12).
    let p1 = format!("{}@1", img.to_str().unwrap());
    let out = run(&["ls", &p1, "/"]);
    let stdout = String::from_utf8_lossy(&out.stdout).to_ascii_uppercase();
    assert!(
        stdout.contains("HELLO.TXT"),
        "expected HELLO.TXT in P1 (GEM), got:\n{stdout}"
    );

    // Partition 2 (BGM, FAT16).
    let p2 = format!("{}@2", img.to_str().unwrap());
    let out = run(&["ls", &p2, "/"]);
    let stdout = String::from_utf8_lossy(&out.stdout).to_ascii_uppercase();
    assert!(
        stdout.contains("HELLO.TXT"),
        "expected HELLO.TXT in P2 (BGM), got:\n{stdout}"
    );
}

#[test]
fn rb_cli_get_extracts_byte_identical_payload_from_ahdi_gem() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("ahdi.img");
    decompress_fixture("test_atarist_ahdi.img.zst", &img);

    let p1 = format!("{}@1", img.to_str().unwrap());
    let dst = dir.path().join("p0_hello.txt");
    run(&["get", &p1, "/HELLO.TXT", dst.to_str().unwrap()]);
    let bytes = std::fs::read(&dst).unwrap();
    assert_eq!(bytes, b"GEM partition 0 hello.");
}

// ----------------------------------------------------------------------------
// Write-verified: round-trip a new file through `rb-cli put` -> `get`
// ----------------------------------------------------------------------------

#[test]
fn rb_cli_put_then_get_round_trips_new_file_into_ahdi_gem() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("ahdi.img");
    decompress_fixture("test_atarist_ahdi.img.zst", &img);

    // Host-side source file with a stable payload.
    let host = dir.path().join("ROUND.TXT");
    let payload = b"round trip via rb-cli put";
    std::fs::write(&host, payload).unwrap();

    // Put into the GEM (FAT12) partition root. The CLI uses the existing
    // FAT EditableFilesystem under the hood, so a successful run proves the
    // AHDI -> FAT dispatch path holds end-to-end on the write side too.
    let p1 = format!("{}@1", img.to_str().unwrap());
    let out = try_run(&["put", &p1, host.to_str().unwrap(), "/ROUND.TXT"]);
    if !out.status.success() {
        // If the put verb has a different argument grammar we surface it for
        // diagnostics. Earlier versions accept the host file as a positional;
        // newer versions may expect `--dest`. Fall back to `--dest`-style.
        let out2 = run(&["put", &p1, host.to_str().unwrap(), "--dest", "/ROUND.TXT"]);
        eprintln!(
            "fallback put stdout:\n{}",
            String::from_utf8_lossy(&out2.stdout)
        );
    }

    // Read it back via `rb-cli get` and verify byte-identity.
    let dst = dir.path().join("round_out.txt");
    run(&["get", &p1, "/ROUND.TXT", dst.to_str().unwrap()]);
    let got = std::fs::read(&dst).unwrap();
    assert_eq!(
        got, payload,
        "round-tripped file does not match the source bytes"
    );

    // The original committed HELLO.TXT must still be there — adding ROUND
    // should not have touched it.
    let dst2 = dir.path().join("hello_after_put.txt");
    run(&["get", &p1, "/HELLO.TXT", dst2.to_str().unwrap()]);
    let still = std::fs::read(&dst2).unwrap();
    assert_eq!(still, b"GEM partition 0 hello.");
}
