//! End-to-end CLI tests for the from-scratch provisioning verbs: `new hd
//! {rdb|sun|atari}` and `new volume xfs`.
//!
//! Each table writer already has unit tests that round-trip through our own
//! parser; these go through the real binary instead, so a break in the CLI
//! grammar, the geometry defaults or the type catalog shows up here rather
//! than in a user's hands. `docs/partition_table_writers_backlog.md` covers
//! how each writer is built and what it was validated against.

use std::path::PathBuf;
use std::process::Command;

fn cli_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_rb-cli"))
}

fn run(args: &[&str]) -> String {
    let out = Command::new(cli_bin())
        .args(args)
        .output()
        .expect("spawn rb-cli");
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    if !out.status.success() {
        panic!(
            "command {args:?} failed: status={:?}\nstdout:\n{stdout}\nstderr:\n{}",
            out.status,
            String::from_utf8_lossy(&out.stderr),
        );
    }
    stdout
}

#[test]
fn rdb_disk_carries_dos_types_and_amiga_drive_names() {
    let dir = tempfile::tempdir().expect("tempdir");
    let img = dir.path().join("amiga.img");
    let img_s = img.to_str().unwrap();

    run(&[
        "new",
        "hd",
        "rdb",
        img_s,
        "--size",
        "200M",
        "--partition",
        "60M:DOS\\3:WORK",
        "--partition",
        "rest:PFS\\3",
    ]);
    let info = run(&["inspect", img_s]);
    assert!(info.contains("Partition table: RDB"), "{info}");
    assert!(info.contains("AmigaDOS FFS-Intl (WORK)"), "{info}");
    // An unnamed entry gets the conventional AmigaDOS device name.
    assert!(info.contains("PFS (Amiga) (DH1)"), "{info}");
}

#[test]
fn sun_disk_reports_its_slices_and_skips_the_backup_alias() {
    let dir = tempfile::tempdir().expect("tempdir");
    let img = dir.path().join("sparc.img");
    let img_s = img.to_str().unwrap();

    run(&[
        "new",
        "hd",
        "sun",
        img_s,
        "--size",
        "200M",
        "--heads",
        "255",
        "--sectors",
        "63",
        "--partition",
        "20M:root",
        "--partition",
        "rest:usr",
    ]);
    let info = run(&["inspect", img_s]);
    assert!(info.contains("Partition table: Sun"), "{info}");
    // Slice 2 spans the whole disk and overlaps the real ones, so it must not
    // appear as a browsable partition.
    let listed = info.lines().filter(|l| l.contains("Sun ")).count();
    assert_eq!(listed, 2, "backup slice leaked into the list:\n{info}");
}

/// GEM cannot describe a partition over 16 MiB, so `place` promotes it; and a
/// filesystem poured into a partition has to be reachable through the table.
#[test]
fn atari_disk_promotes_oversized_gem_and_reaches_a_filled_partition() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fat = dir.path().join("gemdos.img");
    let img = dir.path().join("atari.img");
    let payload = dir.path().join("hello.txt");
    let (fat_s, img_s) = (fat.to_str().unwrap(), img.to_str().unwrap());
    std::fs::write(&payload, b"AHDI round trip.").unwrap();

    run(&[
        "new", "volume", "fat", fat_s, "--size", "8M", "--name", "GEMDOS",
    ]);
    let fill = format!("1={fat_s}");
    run(&[
        "new",
        "hd",
        "atari",
        img_s,
        "--size",
        "64M",
        "--partition",
        "8M",
        "--partition",
        "rest",
        "--fill",
        &fill,
    ]);

    let info = run(&["inspect", img_s]);
    assert!(info.contains("Partition table: AHDI"), "{info}");
    assert!(info.contains("AHDI GEM"), "8 MiB stays GEM:\n{info}");
    assert!(
        info.contains("AHDI BGM"),
        "the rest must become BGM:\n{info}"
    );

    let at1 = format!("{img_s}@1");
    run(&["put", &at1, payload.to_str().unwrap(), "/HELLO.TXT"]);
    let listing = run(&["ls", &at1]);
    assert!(listing.contains("HELLO.TXT"), "{listing}");
}

/// The XFS creator, through the CLI, checked with our own verifier — the real
/// `xfs_repair` cross-check lives in `scripts/xfs-oracle.sh`.
#[test]
fn xfs_volume_is_created_and_checks_clean() {
    let dir = tempfile::tempdir().expect("tempdir");
    let img = dir.path().join("root.xfs");
    let img_s = img.to_str().unwrap();

    run(&[
        "new", "volume", "xfs", img_s, "--size", "64M", "--name", "IRIXROOT",
    ]);
    let info = run(&["show", "fs-info", img_s]);
    assert!(info.contains("XFS v5"), "{info}");
    assert!(info.contains("IRIXROOT"), "{info}");

    let report = run(&["fsck", img_s]);
    assert!(
        !report.to_lowercase().contains("error"),
        "a freshly created volume must check clean:\n{report}"
    );
}
