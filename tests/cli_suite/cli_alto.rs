//! `rb-cli fsck` parity tests for Xerox Alto BFS packs (PARC Disk Image).
//!
//! Alto packs open through the container parser (`alto::open_pack`), not the
//! block-reader factory every other filesystem uses, so `rb-cli fsck` has its
//! own Alto branch (`src/cli/verbs/fsck.rs`). These tests pin that wiring
//! end-to-end: build a clean PDI + a bitmap-corrupted PDI in memory via the
//! library, then drive the real binary through detect -> repair -> re-check.
//!
//! The pack is a small Diablo-31-shaped BFS volume (192 pages) with one file.

use std::path::PathBuf;
use std::process::Command;

use rusty_backup::fs::alto::bfs::Bfs;
use rusty_backup::fs::alto::write::{add_file, create_blank};
use rusty_backup::fs::alto::{pdi, Disk, FsFamily, Geometry, LabelCodec};

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

/// Like [`run`] but tolerates a non-zero exit (fsck exits non-zero when it
/// finds errors). Returns the whole `Output`.
fn run_allow_fail(args: &[&str]) -> std::process::Output {
    Command::new(cli_bin())
        .args(args)
        .output()
        .expect("spawn rb-cli")
}

fn small_geom() -> Geometry {
    Geometry {
        family: FsFamily::Diablo,
        disk_model: 31,
        n_disks: 1,
        n_cylinders: 8,
        n_heads: 2,
        n_sectors: 12,
        label_bytes: 16,
        data_bytes: 512,
    }
}

fn clean_disk() -> Disk {
    let mut d = create_blank(small_geom()).expect("create_blank");
    d = add_file(&d, "HELLO.TXT", b"hi from the alto").expect("add_file");
    d
}

/// Corrupt the DiskDescriptor free-page count on a copy of `disk` and return
/// the PDI bytes. All-public API: find DiskDescriptor's leader through
/// `list_files`, follow its label chain to the data page, zero the count.
fn corrupt_free_pages_pdi(mut disk: Disk) -> Vec<u8> {
    let dd_leader = Bfs::new(&disk)
        .list_files()
        .expect("list_files")
        .into_iter()
        .find(|f| {
            f.name
                .trim_end_matches('.')
                .eq_ignore_ascii_case("DiskDescriptor")
        })
        .expect("DiskDescriptor entry")
        .leader_vda;
    let codec = LabelCodec::for_family(disk.geometry.family);
    let dd_data = codec
        .next(&disk.geometry, &disk.sector(dd_leader).unwrap().label)
        .expect("DiskDescriptor data page");
    // free-page count lives at byte 18-19 of the KDH (the DD's first data page).
    disk.sectors[dd_data].data[18] = 0;
    disk.sectors[dd_data].data[19] = 0;
    pdi::write(&disk)
}

fn write_pack(dir: &std::path::Path, bytes: &[u8]) -> PathBuf {
    let img = dir.join("pack.pdi");
    std::fs::write(&img, bytes).expect("write pdi");
    img
}

#[test]
fn fsck_reports_clean_on_a_good_pdi() {
    let tmp = tempfile::tempdir().unwrap();
    let img = write_pack(tmp.path(), &pdi::write(&clean_disk()));
    let out = run(&["fsck", img.to_str().unwrap()]);
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(s.contains("checked"), "no fsck report:\n{s}");
}

#[test]
fn fsck_json_envelope_marks_clean() {
    let tmp = tempfile::tempdir().unwrap();
    let img = write_pack(tmp.path(), &pdi::write(&clean_disk()));
    let out = run(&["fsck", img.to_str().unwrap(), "--format", "json"]);
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(s.contains("\"clean\""), "no clean field in JSON:\n{s}");
    assert!(s.contains("true"), "expected clean:true:\n{s}");
}

#[test]
fn fsck_detects_then_repairs_a_corrupt_disk_descriptor() {
    let tmp = tempfile::tempdir().unwrap();
    let img = write_pack(tmp.path(), &corrupt_free_pages_pdi(clean_disk()));

    // Detect: non-zero exit, and the specific finding is named.
    let out = run_allow_fail(&["fsck", img.to_str().unwrap()]);
    assert!(
        !out.status.success(),
        "fsck should exit non-zero on a corrupt pack"
    );
    let report = String::from_utf8_lossy(&out.stdout);
    assert!(
        report.contains("AltoFreePagesMismatch"),
        "expected AltoFreePagesMismatch:\n{report}"
    );

    // Repair in place (PDI input), then re-check clean.
    let rep = run(&["fsck", img.to_str().unwrap(), "--repair"]);
    assert!(
        String::from_utf8_lossy(&rep.stdout).contains("Repaired"),
        "repair did not report success:\n{}",
        String::from_utf8_lossy(&rep.stdout)
    );

    // A fresh check now passes (exit 0 -> run() would panic otherwise).
    run(&["fsck", img.to_str().unwrap()]);
}
