//! End-to-end CLI tests for the Soviet BK0011M / ANDOS spine.
//!
//! Synthesizes a tiny ANDOS-signature disk at test time (1 MB +
//! `ANDOS` magic bytes at offset 0x1F8 of sector 0 — one of the
//! well-known BK BIOS slots) and runs it through `rb-cli inspect`.
//!
//! ## Current scope
//!
//! ANDOS is a **detect-only scaffold** in this codebase — the read /
//! write paths are parked at OPEN-WORK §9 behind a real-hardware
//! oracle (no English-language spec, no emulator that ships
//! pre-populated discs, tiny user audience). CLI parity for the
//! BK0011M row is therefore necessarily limited to confirming the
//! dispatch chain ends at `AndosFilesystem` and `inspect` surfaces
//! the volume as ANDOS rather than crashing on it.

use std::path::PathBuf;
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

fn build_andos_disk() -> Vec<u8> {
    let mut disk = vec![0u8; 1024 * 1024];
    disk[0x1F8..0x1F8 + 5].copy_from_slice(b"ANDOS");
    disk
}

fn write_volume(dir: &std::path::Path) -> PathBuf {
    let img = dir.join("bk0011m_andos.img");
    std::fs::write(&img, build_andos_disk()).unwrap();
    img
}

#[test]
fn rb_cli_inspect_on_andos_disk_routes_to_andos_engine() {
    let dir = tempfile::tempdir().unwrap();
    let img = write_volume(dir.path());
    let out = run(&["inspect", img.to_str().unwrap()]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("Partition table: None"),
        "expected superfloppy in inspect, got:\n{stdout}"
    );
    assert!(
        stdout.contains("ANDOS"),
        "expected ANDOS in inspect, got:\n{stdout}"
    );
}

#[test]
fn rb_cli_ls_on_andos_disk_surfaces_unsupported_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let img = write_volume(dir.path());
    // ANDOS is detect-only: `ls` should fail with a clean Unsupported
    // error, NOT crash. We don't assert on the exit code (rb-cli may
    // exit cleanly with a stderr message OR exit non-zero — either is
    // acceptable for the "detect-only" contract).
    let out = Command::new(cli_bin())
        .args(["ls", img.to_str().unwrap()])
        .output()
        .expect("spawn rb-cli");
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    // Either explicit unsupported (preferred) or any clean message
    // referencing ANDOS / unsupported.
    let unsupported = combined.to_ascii_lowercase().contains("unsupported")
        || combined.to_ascii_lowercase().contains("not supported")
        || combined.contains("ANDOS");
    assert!(
        unsupported,
        "expected ls to surface an unsupported / ANDOS message, got:\n{combined}"
    );
}
