//! End-to-end CLI test for `optical new next-ufs`: a NeXT dlV3 label in 2048-byte
//! sectors around a 4.3BSD UFS, populated from a host folder with both archive
//! depths. The geometry itself is pinned by the unit tests in
//! `src/partition/next_cd_builder.rs`; this proves the verb surface round-trips.

use std::io::Write as _;
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

fn stdout(out: &std::process::Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// A folder shaped like a NeXT patch set: a pre-POSIX tar of a `.pkg`, a
/// `.tar.gz`, a gzip that is not a tar, and a plain file.
fn source_tree(root: &Path) {
    std::fs::create_dir_all(root.join("Patches")).unwrap();

    let mut b = tar::Builder::new(Vec::new());
    let mut d = tar::Header::new_old();
    d.set_size(0);
    d.set_mode(0o755);
    d.set_path("Patch.pkg/").unwrap();
    d.set_cksum();
    b.append(&d, std::io::empty()).unwrap();
    let mut h = tar::Header::new_old();
    h.set_size(9);
    h.set_mode(0o644);
    h.set_cksum();
    b.append_data(&mut h, "Patch.pkg/Patch.info", &b"patch v1\n"[..])
        .unwrap();
    std::fs::write(root.join("Patches/Patch.tar"), b.into_inner().unwrap()).unwrap();

    let f = std::fs::File::create(root.join("Patches/headers.tar.gz")).unwrap();
    let enc = flate2::write::GzEncoder::new(f, flate2::Compression::default());
    let mut t = tar::Builder::new(enc);
    let mut h = tar::Header::new_gnu();
    h.set_size(6);
    h.set_mode(0o644);
    h.set_cksum();
    t.append_data(&mut h, "include/a.h", &b"#pragma"[..6])
        .unwrap();
    t.into_inner().unwrap().finish().unwrap();

    let f = std::fs::File::create(root.join("notes.txt.gz")).unwrap();
    let mut enc = flate2::write::GzEncoder::new(f, flate2::Compression::default());
    enc.write_all(b"read me on NeXTSTEP\n").unwrap();
    enc.finish().unwrap();

    std::fs::write(root.join("README"), b"hello NeXT\n").unwrap();
}

#[test]
fn next_ufs_cdrom_builds_populates_and_round_trips() {
    let dir = tempfile::tempdir().expect("tempdir");
    let src = dir.path().join("src");
    source_tree(&src);
    let src_s = src.to_str().unwrap();

    // Full expansion: both tarballs unpack, the bare gzip decompresses.
    let iso = dir.path().join("next.iso");
    let iso_s = iso.to_str().unwrap();
    let at1 = format!("{iso_s}@1");
    run(&[
        "optical",
        "new",
        "next-ufs",
        iso_s,
        "--size",
        "auto",
        "--from-dir",
        src_s,
        "--expand-archives",
        "--expand-gunzip",
        "--name",
        "NEXT_TEST",
    ]);
    assert_eq!(std::fs::metadata(&iso).unwrap().len() % 2048, 0);

    let s = stdout(&run(&["inspect", iso_s]));
    assert!(s.contains("Partition table: NeXT"), "inspect:\n{s}");
    assert!(s.contains("4.3BSD"), "inspect:\n{s}");

    let root = stdout(&run(&["ls", &at1, "/"]));
    assert!(
        root.contains("notes.txt") && !root.contains("notes.txt.gz"),
        "{root}"
    );
    let patches = stdout(&run(&["ls", &at1, "/Patches"]));
    assert!(
        patches.contains("Patch") && patches.contains("headers"),
        "{patches}"
    );
    let pkg = stdout(&run(&["ls", &at1, "/Patches/Patch/Patch.pkg"]));
    assert!(
        pkg.contains("Patch.info"),
        "v7 dir entry became a directory: {pkg}"
    );
    run(&["fsck", &at1]);

    let out = dir.path().join("notes.out");
    run(&["get", &at1, "/notes.txt", out.to_str().unwrap()]);
    assert_eq!(std::fs::read(&out).unwrap(), b"read me on NeXTSTEP\n");

    // Gzip layer only: the tarball survives as a tar under its stripped name.
    let gz = dir.path().join("gz.iso");
    let gz_s = gz.to_str().unwrap();
    run(&[
        "optical",
        "new",
        "next-ufs",
        gz_s,
        "--size",
        "8M",
        "--from-dir",
        src_s,
        "--expand-gunzip",
    ]);
    let patches = stdout(&run(&["ls", &format!("{gz_s}@1"), "/Patches"]));
    assert!(
        patches.contains("headers.tar") && !patches.contains("headers.tar.gz"),
        "{patches}"
    );
    assert!(
        patches.contains("Patch.tar"),
        "a plain tar is left alone: {patches}"
    );
}

#[test]
fn next_ufs_cdrom_refuses_a_size_it_cannot_build_without_touching_the_output() {
    let dir = tempfile::tempdir().expect("tempdir");
    let iso = dir.path().join("tiny.iso");
    let out = Command::new(cli_bin())
        .args([
            "optical",
            "new",
            "next-ufs",
            iso.to_str().unwrap(),
            "--size",
            "1M",
        ])
        .output()
        .expect("spawn rb-cli");
    assert!(!out.status.success());
    assert!(
        !iso.exists(),
        "a refused build must not leave a file behind"
    );
}
