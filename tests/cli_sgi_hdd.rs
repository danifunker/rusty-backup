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

    // 1. Synthesize a 50 MiB SGI/EFS hard disk. 50 MiB → a 48 MiB EFS root,
    //    which exercises the scaled-firstcg path (> 32 MiB).
    run(&["new-sgi-hdd", img_s, "--size", "50M", "--name", "TOOLBOX"]);
    // The image is exactly the rounded disk size (50 MiB, whole cylinders).
    assert_eq!(
        std::fs::metadata(&img).unwrap().len(),
        50 * 1024 * 1024,
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
