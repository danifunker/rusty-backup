//! End-to-end CLI tests for the MITS Altair 8800 / CP/M spine.
//!
//! Synthesizes a tiny Altair 8" SSSD CP/M floppy at test time (DPB:
//! `altair_8in`, 77 trk × 26 spt × 128 B = 256.25 KB; 2 reserved
//! tracks for CCP/BDOS) and drives it through `rb-cli`.
//!
//! ## CLI parity scope
//!
//! CP/M volumes carry no on-disk signature — there is no way to
//! discriminate a CP/M disc from random bytes without an explicit DPB.
//! `rb-cli` therefore cannot auto-route a raw Altair image to the
//! CpmFilesystem engine. The `--fs-type cpm:<preset>` flag closes
//! that gap: every dispatch verb (inspect / ls / get / put / rm)
//! honours the override and forwards the preset name to the
//! `partition_type_string` dispatch branch in `src/fs/mod.rs`. The
//! same flag unblocks the CP/M-floppy Wave-3 cores (Amstrad / PCW /
//! Einstein / SVI328 / MultiComp / ZX+3) at zero per-core cost. Full
//! ls / get / put round-trips against the synthetic Altair fixture
//! live below; the byte-truth oracle (`cpmtools cpmls/cpmcp`) ships in
//! `tests/cpm_e2e.rs`.

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

fn run_ok(args: &[&str]) -> std::process::Output {
    let out = run_status(args);
    if !out.status.success() {
        panic!(
            "rb-cli {args:?} failed: status={:?}\nstdout:\n{}\nstderr:\n{}",
            out.status,
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        );
    }
    out
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
    // Engine-level coverage of the same code path the CLI now
    // dispatches through `--fs-type cpm:altair_8in`. Kept around as a
    // direct-API smoke check.
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

// ---- CLI parity tests using the new --fs-type cpm:NAME flag ----

#[test]
fn rb_cli_ls_with_fs_type_lists_seeded_file() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("altair_ls.dsk");
    build_altair_disk_with_one_file_at(&img);

    let out = run_ok(&[
        "ls",
        "--fs-type",
        "cpm:altair_8in",
        img.to_str().unwrap(),
        "/",
    ]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("HELLO.TXT"),
        "ls --fs-type should surface HELLO.TXT, got:\n{stdout}"
    );
}

#[test]
fn rb_cli_get_with_fs_type_extracts_byte_exact_payload() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("altair_get.dsk");
    build_altair_disk_with_one_file_at(&img);

    let out_path = dir.path().join("hello_out.txt");
    run_ok(&[
        "get",
        "--fs-type",
        "cpm:altair_8in",
        img.to_str().unwrap(),
        "/HELLO.TXT",
        out_path.to_str().unwrap(),
    ]);
    let extracted = std::fs::read(&out_path).unwrap();
    assert_eq!(&extracted, b"altair cp/m cli parity test\n");
}

#[test]
fn rb_cli_put_get_round_trips_a_new_file_via_fs_type() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("altair_putget.dsk");
    build_altair_disk_with_one_file_at(&img);

    let host_in = dir.path().join("seed.txt");
    let payload: &[u8] = b"Altair CLI put/get round-trip via --fs-type cpm:altair_8in";
    std::fs::write(&host_in, payload).unwrap();

    // put: write a new file into the CP/M volume.
    run_ok(&[
        "put",
        "--fs-type",
        "cpm:altair_8in",
        img.to_str().unwrap(),
        host_in.to_str().unwrap(),
        "/SEED.TXT",
    ]);

    // ls confirms it landed alongside HELLO.TXT.
    let ls_out = run_ok(&[
        "ls",
        "--fs-type",
        "cpm:altair_8in",
        img.to_str().unwrap(),
        "/",
    ]);
    let ls_stdout = String::from_utf8_lossy(&ls_out.stdout);
    assert!(
        ls_stdout.contains("HELLO.TXT") && ls_stdout.contains("SEED.TXT"),
        "ls after put should list both files, got:\n{ls_stdout}"
    );

    // get reads SEED.TXT back byte-exact.
    let host_out = dir.path().join("seed_out.txt");
    run_ok(&[
        "get",
        "--fs-type",
        "cpm:altair_8in",
        img.to_str().unwrap(),
        "/SEED.TXT",
        host_out.to_str().unwrap(),
    ]);
    assert_eq!(std::fs::read(&host_out).unwrap(), payload);
}

#[test]
fn rb_cli_rm_with_fs_type_deletes_a_file() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("altair_rm.dsk");
    build_altair_disk_with_one_file_at(&img);

    // Sanity: HELLO.TXT is there.
    let pre = run_ok(&[
        "ls",
        "--fs-type",
        "cpm:altair_8in",
        img.to_str().unwrap(),
        "/",
    ]);
    assert!(String::from_utf8_lossy(&pre.stdout).contains("HELLO.TXT"));

    run_ok(&[
        "rm",
        "--fs-type",
        "cpm:altair_8in",
        img.to_str().unwrap(),
        "/HELLO.TXT",
    ]);

    let post = run_ok(&[
        "ls",
        "--fs-type",
        "cpm:altair_8in",
        img.to_str().unwrap(),
        "/",
    ]);
    let post_stdout = String::from_utf8_lossy(&post.stdout);
    assert!(
        !post_stdout.contains("HELLO.TXT"),
        "ls after rm should NOT list HELLO.TXT, got:\n{post_stdout}"
    );
}

#[test]
fn rb_cli_rejects_unknown_cpm_preset_with_clear_error() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("altair_bad_preset.dsk");
    build_altair_disk_with_one_file_at(&img);

    let out = run_status(&[
        "ls",
        "--fs-type",
        "cpm:not_a_real_preset",
        img.to_str().unwrap(),
        "/",
    ]);
    assert!(!out.status.success(), "unknown preset should fail");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("unknown CP/M DPB preset"),
        "error should name the unknown-preset path, got:\n{stderr}"
    );
    assert!(
        stderr.contains("altair_8in"),
        "error should list valid presets including altair_8in, got:\n{stderr}"
    );
}
