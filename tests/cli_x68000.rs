//! End-to-end CLI tests for the X68000 / Sharp `.d88` + Human68k spine.
//!
//! Drives the `rb-cli` binary against the committed fixture:
//!   - `test_x68000_human68k_2dd.d88.zst` — 720 KB FAT12-compatible
//!     2DD floppy wrapped in the Sharp `.d88` container, seeded with
//!     `HELLO.TXT` (30 B) and `NOTE.TXT` (31 B) via mkfs.fat + mcopy.
//!
//! Covers stages 8 (GUI/dispatch surface) and 9 (CLI parity) of the
//! per-format spine. The .d88 container is decoded to a flat image in
//! the test before invoking rb-cli on the read/write verbs — `inspect`
//! / `ls` accept the wrapped path directly; `get` / `put` see the flat
//! image so we can verify Stage 6 round-trip via the CLI surface
//! without depending on the rb-cli .d88 write-back plumbing (which is
//! a separate maintenance item — out of scope for this CLI-parity
//! test).

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;

use rusty_backup::rbformats::containers::d88;

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

fn decompress_fixture(name: &str, dst: &Path) {
    let path = format!("tests/fixtures/{name}");
    let compressed = std::fs::read(&path).expect("read fixture");
    let mut decoder =
        zstd::stream::read::Decoder::new(std::io::Cursor::new(compressed)).expect("zstd decoder");
    let mut bytes = Vec::new();
    decoder.read_to_end(&mut bytes).expect("decompress");
    std::fs::write(dst, &bytes).expect("write fixture out");
}

fn wrapped_d88_path(dir: &Path) -> PathBuf {
    let img = dir.join("x68k_2dd.d88");
    decompress_fixture("test_x68000_human68k_2dd.d88.zst", &img);
    img
}

/// Decode the .d88 wrapper to a flat 720K FAT12 image so `rb-cli get`
/// / `put` can operate on it (the .d88 write-back surface in rb-cli is
/// out of scope for this test).
fn flat_fat12_path(dir: &Path) -> PathBuf {
    let wrapped = wrapped_d88_path(dir);
    let raw = std::fs::read(&wrapped).expect("read wrapped d88");
    let flat = d88::decode_d88_bytes(&raw).expect("decode d88");
    let out = dir.join("x68k_2dd_flat.img");
    std::fs::write(&out, &flat).expect("write flat image");
    out
}

#[test]
fn rb_cli_inspect_on_d88_fixture_reports_fat_superfloppy() {
    let dir = tempfile::tempdir().unwrap();
    let img = wrapped_d88_path(dir.path());
    let out = run(&["inspect", img.to_str().unwrap()]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("Partition table: None"),
        "expected superfloppy in inspect, got:\n{stdout}"
    );
    assert!(
        stdout.contains("FAT"),
        "expected FAT partition in inspect, got:\n{stdout}"
    );
    assert!(
        stdout.contains("720"),
        "expected 720 KiB size in inspect, got:\n{stdout}"
    );
}

#[test]
fn rb_cli_ls_on_d88_fixture_finds_seeded_files() {
    let dir = tempfile::tempdir().unwrap();
    let img = wrapped_d88_path(dir.path());
    let out = run(&["ls", img.to_str().unwrap()]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    let upper = stdout.to_ascii_uppercase();
    assert!(
        upper.contains("HELLO.TXT"),
        "expected HELLO.TXT in `rb-cli ls`, got:\n{stdout}"
    );
    assert!(
        upper.contains("NOTE.TXT"),
        "expected NOTE.TXT in `rb-cli ls`, got:\n{stdout}"
    );
}

#[test]
fn rb_cli_get_extracts_seeded_file_byte_exact_from_flat() {
    let dir = tempfile::tempdir().unwrap();
    let img = flat_fat12_path(dir.path());
    let dst = dir.path().join("hello_out.txt");
    let _ = run(&[
        "get",
        img.to_str().unwrap(),
        "HELLO.TXT",
        dst.to_str().unwrap(),
    ]);
    let extracted = std::fs::read(&dst).unwrap();
    assert_eq!(&extracted, b"Hello from D88 X68000 fixture.");
}

#[test]
fn rb_cli_put_then_get_round_trips_a_new_file_on_flat() {
    let dir = tempfile::tempdir().unwrap();
    let img = flat_fat12_path(dir.path());
    let src = dir.path().join("payload.txt");
    let payload = b"rb-cli x68000 round-trip test payload";
    std::fs::write(&src, payload).unwrap();

    let _ = run(&[
        "put",
        img.to_str().unwrap(),
        src.to_str().unwrap(),
        "/RT.TXT",
    ]);

    let out = run(&["ls", img.to_str().unwrap()]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.to_ascii_uppercase().contains("RT.TXT"),
        "expected RT.TXT after put, got:\n{stdout}"
    );

    let dst = dir.path().join("rt_back.txt");
    let _ = run(&[
        "get",
        img.to_str().unwrap(),
        "RT.TXT",
        dst.to_str().unwrap(),
    ]);
    let got = std::fs::read(&dst).unwrap();
    assert_eq!(&got, payload);
}
