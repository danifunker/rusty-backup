//! End-to-end CLI tests for the MITS Altair 8800 / CP/M spine.
//!
//! Synthesizes a tiny Altair 8" SSSD CP/M floppy at test time (DPB:
//! `altair_8in`, 77 trk × 26 spt × 128 B = 256.25 KB; 2 reserved
//! tracks for CCP/BDOS) and drives it through `rb-cli`.
//!
//! ## Current CLI parity scope
//!
//! CP/M volumes carry no on-disk signature — there is no way to
//! discriminate a CP/M disc from random bytes without an explicit DPB.
//! `rb-cli` therefore cannot auto-route a raw Altair image to the
//! CpmFilesystem engine, and currently has no `--fs-type cpm:NAME`
//! flag for declaring the DPB at the command line. Until that flag
//! lands, CLI parity for the Altair row is necessarily limited to
//! `inspect` (which surfaces the disc as an Unknown / superfloppy of
//! unknown FS shape — the correct behaviour given the absence of any
//! signature). End-to-end read + write coverage against an Altair
//! image lives at the engine layer via `tests/cpm_e2e.rs`, which
//! drives the same engine via direct API with the Amstrad-data DPB
//! (cpmtools byte-truth oracle).

use std::path::PathBuf;
use std::process::Command;

use rusty_backup::fs::cpm::CpmFilesystem;
use rusty_backup::fs::cpm_diskdefs::ALTAIR_8IN;
use rusty_backup::fs::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
use std::io::Cursor;

fn cli_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_rb-cli"))
}

fn run_status(args: &[&str]) -> std::process::Output {
    Command::new(cli_bin())
        .args(args)
        .output()
        .expect("spawn rb-cli")
}

/// Allocate a zero-filled Altair-shaped buffer (reserved tracks +
/// data area). Caller is responsible for stamping any directory /
/// payload bytes.
fn build_blank_altair_disk() -> Vec<u8> {
    let dpb = ALTAIR_8IN;
    let reserved = dpb.off as usize * dpb.spt as usize * 128;
    let total = reserved + dpb.data_bytes() as usize;
    vec![0xE5; total]
}

/// Build an Altair disc with one seeded file via the engine's
/// EditableFilesystem path, using a tempfile as the backing store.
/// Returns the on-disk bytes after sync.
fn build_altair_disk_with_one_file_at(path: &std::path::Path) {
    std::fs::write(path, build_blank_altair_disk()).unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let mut fs = CpmFilesystem::open_with_dpb(file, 0, ALTAIR_8IN).expect("open blank altair");
    let root = fs.root().unwrap();
    let payload = b"altair cp/m cli parity test\n";
    let _ = fs
        .create_file(
            &root,
            "HELLO.TXT",
            &mut payload.as_slice(),
            payload.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();
    fs.sync_metadata().unwrap();
}

fn build_altair_disk_with_one_file() -> Vec<u8> {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("altair_inmem.dsk");
    build_altair_disk_with_one_file_at(&p);
    std::fs::read(&p).unwrap()
}

#[test]
fn rb_cli_inspect_on_altair_disc_runs_and_reports_unknown_fs() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("altair_8in.dsk");
    build_altair_disk_with_one_file_at(&img);
    // `inspect` should not crash on an unrecognised superfloppy. CP/M
    // has no signature, so the FS column reads "Unknown" (or an
    // accidental short-detect match). The contract here is just
    // "rb-cli ran cleanly against the bytes" — full CP/M routing
    // requires a CLI DPB flag.
    let out = run_status(&["inspect", img.to_str().unwrap()]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "rb-cli inspect failed: status={:?}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        out.status
    );
    assert!(
        stdout.contains("Partition table: None"),
        "expected superfloppy in inspect, got:\n{stdout}"
    );
}

#[test]
fn engine_read_path_matches_payload_byte_exact() {
    // Engine-level coverage stand-in for the CLI `get` parity that
    // would normally exercise the same code path — until rb-cli grows
    // a `--fs-type cpm:altair_8in` flag, this is the closest CLI-shape
    // assertion we can make against the Altair preset specifically.
    let bytes = build_altair_disk_with_one_file();
    let cur = Cursor::new(bytes);
    let mut fs = CpmFilesystem::open_with_dpb(cur, 0, ALTAIR_8IN).unwrap();
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let hello = entries
        .iter()
        .find(|e| e.name == "HELLO.TXT")
        .unwrap()
        .clone();
    let data = fs.read_file(&hello, 4096).unwrap();
    assert_eq!(&data, b"altair cp/m cli parity test\n");
}
