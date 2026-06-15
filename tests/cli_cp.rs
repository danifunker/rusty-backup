//! End-to-end tests for the `rb-cli cp` verb (image-to-image copy).
//!
//! Drives the real binary against scratch FAT images: single-file copy,
//! whole-volume recursive consolidation, the free-space preflight, and the
//! same-image guard. The engine-level fidelity matrix lives in
//! `tests/cp_engine.rs`; this is the grammar / wiring regression net.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn cli_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_rb-cli"))
}

fn run(args: &[&str]) -> Output {
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

fn run_expect_fail(args: &[&str]) -> Output {
    let out = Command::new(cli_bin())
        .args(args)
        .output()
        .expect("spawn rb-cli");
    assert!(
        !out.status.success(),
        "expected {args:?} to fail but it succeeded"
    );
    out
}

fn s(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

#[test]
fn cp_single_file_between_images() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.img");
    let dst = dir.path().join("dst.img");
    let host = dir.path().join("hello.txt");
    std::fs::write(&host, b"hello world\n").unwrap();

    run(&[
        "new",
        "--fs",
        "fat",
        "--size",
        "4M",
        "--name",
        "SRC",
        &s(&src),
    ]);
    run(&[
        "new",
        "--fs",
        "fat",
        "--size",
        "4M",
        "--name",
        "DST",
        &s(&dst),
    ]);
    run(&["put", &s(&src), &s(&host), "/HELLO.TXT"]);

    run(&["cp", &s(&src), "/HELLO.TXT", &s(&dst), "/HELLO.TXT"]);

    let ls = run(&["ls", &s(&dst), "/"]);
    let listing = String::from_utf8_lossy(&ls.stdout);
    assert!(
        listing.contains("HELLO.TXT"),
        "dst should contain HELLO.TXT:\n{listing}"
    );
}

#[test]
fn cp_recursive_whole_volume_into_subdir() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.img");
    let dst = dir.path().join("dst.img");
    let host = dir.path().join("a.bin");
    std::fs::write(&host, vec![7u8; 2048]).unwrap();

    run(&[
        "new",
        "--fs",
        "fat",
        "--size",
        "4M",
        "--name",
        "SRC",
        &s(&src),
    ]);
    run(&[
        "new",
        "--fs",
        "fat",
        "--size",
        "4M",
        "--name",
        "DST",
        &s(&dst),
    ]);
    run(&["mkdir", &s(&src), "/SUB"]);
    run(&["put", &s(&src), &s(&host), "/SUB/A.BIN"]);

    run(&[
        "cp",
        &s(&src),
        "/",
        &s(&dst),
        "/FROMSRC/",
        "-r",
        "--parents",
    ]);

    let ls = run(&["ls", &s(&dst), "/FROMSRC/SUB"]);
    let listing = String::from_utf8_lossy(&ls.stdout);
    assert!(
        listing.contains("A.BIN"),
        "tree should be rebuilt under /FROMSRC:\n{listing}"
    );
}

#[test]
fn cp_preflight_refuses_when_too_big() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.img");
    let dst = dir.path().join("dst.img");
    let host = dir.path().join("big.bin");
    std::fs::write(&host, vec![0u8; 3_000_000]).unwrap();

    run(&[
        "new",
        "--fs",
        "fat",
        "--size",
        "8M",
        "--name",
        "SRC",
        &s(&src),
    ]);
    run(&[
        "new",
        "--fs",
        "fat",
        "--size",
        "2M",
        "--name",
        "DST",
        &s(&dst),
    ]);
    run(&["put", &s(&src), &s(&host), "/BIG.BIN"]);

    let out = run_expect_fail(&["cp", &s(&src), "/BIG.BIN", &s(&dst), "/BIG.BIN"]);
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("copy needs"),
        "expected preflight message:\n{err}"
    );
    assert!(
        err.contains("resize"),
        "should suggest growing the volume:\n{err}"
    );
}

#[test]
fn cp_rejects_same_image() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.img");
    let host = dir.path().join("x.txt");
    std::fs::write(&host, b"x").unwrap();
    run(&[
        "new",
        "--fs",
        "fat",
        "--size",
        "4M",
        "--name",
        "SRC",
        &s(&src),
    ]);
    run(&["put", &s(&src), &s(&host), "/X.TXT"]);

    let out = run_expect_fail(&["cp", &s(&src), "/X.TXT", &s(&src), "/Y.TXT"]);
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("same image"),
        "expected same-image guard:\n{err}"
    );
}
