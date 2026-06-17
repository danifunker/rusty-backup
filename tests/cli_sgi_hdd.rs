//! End-to-end CLI test for `new-sgi-hdd`: synthesize a dvh-wrapped EFS IRIX
//! hard disk, then drive the ordinary verbs against its EFS root partition —
//! the workflow that packages an IRIX toolbox binary for the IRIS emulator
//! (`put disk.img@1 ./bstoolbox /bstoolbox`).
//!
//! Deterministic (fixed payload, in-memory build), no emulator. The real-IRIX
//! `fx`/`prtvtoc`/`mount -t efs` check is a manual stretch goal documented in
//! `docs/SGI_Filesystems.md`; here we prove the rb-cli surface round-trips.

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

#[test]
fn sgi_efs_hdd_create_put_fsck_get_roundtrip() {
    let dir = tempfile::tempdir().expect("tempdir");
    let img = dir.path().join("irix.img");
    let img_s = img.to_str().unwrap();
    let at1 = format!("{img_s}@1");

    // 1. Synthesize a 50 MiB SGI/EFS hard disk. 50 MiB → a ~48 MiB EFS root,
    //    which exercises the scaled-firstcg path (> 32 MiB).
    run(&["new-sgi-hdd", img_s, "--size", "50M", "--name", "TOOLBOX"]);
    // The image is rounded up to a whole number of 1008-sector (504 KiB)
    // cylinders — at least the request, and an exact cylinder multiple.
    let disk_len = std::fs::metadata(&img).unwrap().len();
    let cylinder_bytes = 16 * 63 * 512; // heads × sectors × 512
    assert!(disk_len >= 50 * 1024 * 1024, "disk is at least the request");
    assert_eq!(
        disk_len % cylinder_bytes,
        0,
        "disk is a whole-cylinder size"
    );

    // 2. inspect shows an SGI table (not None) with the EFS partition AND the
    //    full dvh slot dump listing VOLHDR + VOLUME + EFS.
    let inspect = run(&["inspect", img_s]);
    let s = String::from_utf8_lossy(&inspect.stdout);
    assert!(s.contains("Partition table: SGI"), "inspect:\n{s}");
    assert!(s.contains("SGI EFS"), "EFS partition listed:\n{s}");
    assert!(s.contains("VOLHDR"), "VOLHDR slot shown:\n{s}");
    assert!(s.contains("VOLUME"), "VOLUME slot shown:\n{s}");

    // 3. put a deterministic payload into the EFS root (@1), then ls + fsck.
    let payload: Vec<u8> = (0..20_000u32)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
        .collect();
    let src = dir.path().join("bstoolbox");
    std::fs::write(&src, &payload).expect("write payload");
    run(&["put", &at1, src.to_str().unwrap(), "/bstoolbox"]);

    let ls = run(&["ls", &at1]);
    assert!(
        String::from_utf8_lossy(&ls.stdout).contains("bstoolbox"),
        "ls lists the file"
    );

    // fsck exits 0 on a clean volume (run() asserts success).
    run(&["fsck", &at1]);

    // 4. get it back; bytes must be identical.
    let out = dir.path().join("out.bin");
    run(&["get", &at1, "/bstoolbox", out.to_str().unwrap()]);
    let got = std::fs::read(&out).expect("read get output");
    assert_eq!(got, payload, "EFS round-trip is byte-identical");
}

#[test]
fn sgi_efs_cdrom_create_put_get_roundtrip() {
    let dir = tempfile::tempdir().expect("tempdir");
    let iso = dir.path().join("irix.iso");
    let iso_s = iso.to_str().unwrap();
    let at1 = format!("{iso_s}@1");

    // EFS CD-ROM: dvh + slot-7 SYSV EFS + 1x32 CD geometry (the IRIX EFS-CD
    // shape). 64 MiB keeps the test quick; the image is sparse on disk.
    run(&["new-sgi-cdrom", iso_s, "--size", "64M", "--name", "IRIXCD"]);
    let len = std::fs::metadata(&iso).unwrap().len();
    assert_eq!(
        len % 2048,
        0,
        "CD image is a whole number of 2048-byte sectors"
    );

    // inspect shows the EFS in slot 7 (surfaced as EFS even though it's SYSV).
    let inspect = run(&["inspect", iso_s]);
    let s = String::from_utf8_lossy(&inspect.stdout);
    assert!(s.contains("Partition table: SGI"), "inspect:\n{s}");
    assert!(s.contains("SGI EFS"), "slot-7 SYSV surfaces as EFS:\n{s}");

    // put / ls / fsck / get round-trip on the CD's EFS.
    let payload: Vec<u8> = (0..12_345u32)
        .map(|i| (i.wrapping_mul(40_503) >> 11) as u8)
        .collect();
    let src = dir.path().join("data");
    std::fs::write(&src, &payload).expect("write payload");
    run(&["put", &at1, src.to_str().unwrap(), "/DATA"]);
    assert!(String::from_utf8_lossy(&run(&["ls", &at1]).stdout).contains("DATA"));
    run(&["fsck", &at1]);

    let out = dir.path().join("out.bin");
    run(&["get", &at1, "/DATA", out.to_str().unwrap()]);
    assert_eq!(
        std::fs::read(&out).expect("read get output"),
        payload,
        "EFS CD round-trip is byte-identical"
    );
}
