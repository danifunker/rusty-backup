//! End-to-end CLI tests for the from-scratch provisioning verbs: `new hd
//! {rdb|sun|atari|sgi-dklabel}` and `new volume xfs`.
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

/// An IRIS 2000 / 3000 disk built from nothing, on the reference 3130's own
/// geometry, in both the native and the byte-swapped orientation.
#[test]
fn sgi_dklabel_disk_carries_efs_v1_slots_in_both_word_orders() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root_vol = dir.path().join("rootvol.img");
    let img = dir.path().join("iris.img");
    let swapped = dir.path().join("iris-swab.img");
    let payload = dir.path().join("hello.txt");
    let (root_s, img_s) = (root_vol.to_str().unwrap(), img.to_str().unwrap());
    std::fs::write(&payload, b"IRIS 3130 round trip.").unwrap();

    run(&[
        "new", "volume", "efs-v1", root_s, "--size", "9139200", // 17850 blocks
    ]);
    let fill = format!("1={root_s}");
    run(&[
        "new",
        "hd",
        "sgi-dklabel",
        img_s,
        "--size",
        "60135936", // 987c x 7h x 17s, the Priam V170's geometry
        "--heads",
        "7",
        "--sectors",
        "17",
        "--partition",
        "9139200:root",
        "--partition",
        "9078272:swap",
        "--partition",
        "rest:slice",
        "--fill",
        &fill,
    ]);

    let info = run(&["inspect", img_s]);
    assert!(
        info.contains("Partition table: SGI-DkLabel (native)"),
        "{info}"
    );
    for (role, lba) in [("root", "119"), ("swap", "17969"), ("slice", "35700")] {
        let want = format!("SGI {role}");
        let line = info
            .lines()
            .find(|l| l.contains(&want))
            .unwrap_or_else(|| panic!("no {role} slot listed:\n{info}"));
        assert!(line.contains(lba), "{role} is not at block {lba}: {line}");
    }
    // The whole-disk wrapper slot must not reach the browse list.
    assert_eq!(info.lines().filter(|l| l.contains("SGI ")).count(), 3);

    let at1 = format!("{img_s}@1");
    run(&["put", &at1, payload.to_str().unwrap(), "/hello.txt"]);
    run(&["fsck", &at1]);

    // Both orientations of the medium have to open, and writes made in the
    // swapped one have to survive being swapped back.
    let swapped_s = swapped.to_str().unwrap();
    run(&["swab16", img_s, swapped_s]);
    let swapped_info = run(&["inspect", swapped_s]);
    assert!(
        swapped_info.contains("SGI-DkLabel (byte-swapped)"),
        "{swapped_info}"
    );
    let sw1 = format!("{swapped_s}@1");
    run(&["mkdir", &sw1, "/etc"]);
    run(&["fsck", &sw1]);
    let back = dir.path().join("back.img");
    run(&["swab16", swapped_s, back.to_str().unwrap()]);
    let listing = run(&["ls", &format!("{}@1", back.to_str().unwrap())]);
    assert!(listing.contains("hello.txt"), "{listing}");
    assert!(listing.contains("etc"), "{listing}");
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
