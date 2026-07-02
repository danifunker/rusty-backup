//! End-to-end test for the `api hfs` CLI surface. Drives the
//! `rb-cli` binary against scratch images in a tempdir.
//!
//! These are deliberately black-box tests: they exercise the same code
//! path scripted consumers (e.g. the lbmactwo build pipeline) hit, so
//! grammar or argument-shape regressions surface here before they
//! reach those callers.

use std::path::PathBuf;
use std::process::Command;

fn cli_bin() -> PathBuf {
    // Cargo sets CARGO_BIN_EXE_<name> for every [[bin]] target in scope.
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
fn hfs_round_trip_put_get_rm() {
    let dir = tempfile::tempdir().expect("tempdir");
    let img = dir.path().join("disk.dsk");
    let img_s = img.to_str().unwrap();

    run(&[
        "api", "hfs", "new", img_s, "--size", "800K", "--name", "Bench",
    ]);

    let expected_size = 800 * 1024;
    let got_size = std::fs::metadata(&img).unwrap().len();
    assert_eq!(got_size, expected_size, "800K image should be 819200 bytes");

    // Library should be able to re-open what we wrote.
    {
        use rusty_backup::fs::hfs::HfsFilesystem;
        let f = std::fs::File::open(&img).unwrap();
        let fs = HfsFilesystem::open(f, 0).expect("re-open HFS image");
        assert_eq!(fs.volume_summary().volume_name, "Bench");
    }

    let host = dir.path().join("hello.txt");
    std::fs::write(&host, b"hello from host\n").unwrap();
    run(&[
        "api",
        "hfs",
        "put",
        img_s,
        host.to_str().unwrap(),
        "/hello.txt",
    ]);

    let back = dir.path().join("back.txt");
    run(&[
        "api",
        "hfs",
        "get",
        img_s,
        "/hello.txt",
        back.to_str().unwrap(),
    ]);
    assert_eq!(std::fs::read(&host).unwrap(), std::fs::read(&back).unwrap());

    // put-zero pre-allocation.
    run(&[
        "api",
        "hfs",
        "put-zero",
        img_s,
        "/Results.jsonl",
        "4096",
        "--type",
        "TEXT",
        "--creator",
        "ttxt",
    ]);
    let ls = run(&["api", "hfs", "ls", img_s, "/"]);
    let ls_text = String::from_utf8(ls.stdout).unwrap();
    assert!(ls_text.contains("hello.txt"));
    assert!(ls_text.contains("Results.jsonl"));
    assert!(ls_text.contains("TEXT ttxt"));

    // Remove the file and confirm.
    run(&["api", "hfs", "rm", img_s, "/hello.txt"]);
    let ls2 = run(&["api", "hfs", "ls", img_s, "/"]);
    let ls2_text = String::from_utf8(ls2.stdout).unwrap();
    assert!(!ls2_text.contains("hello.txt"));
    assert!(ls2_text.contains("Results.jsonl"));

    run(&["api", "hfs", "validate", img_s]);
}

#[test]
fn hfs_put_boot_writes_exact_bytes_and_keeps_catalog() {
    let dir = tempfile::tempdir().expect("tempdir");
    let img = dir.path().join("disk.dsk");
    let img_s = img.to_str().unwrap();

    run(&[
        "api", "hfs", "new", img_s, "--size", "800K", "--name", "BootTest",
    ]);

    let bb = dir.path().join("bb.bin");
    let bb_data: Vec<u8> = (0..600u32).map(|i| (i % 256) as u8).collect();
    std::fs::write(&bb, &bb_data).unwrap();

    run(&["api", "hfs", "put-boot", img_s, bb.to_str().unwrap()]);

    // Exactly the source bytes at offset 0, nothing beyond.
    let img_bytes = std::fs::read(&img).unwrap();
    assert_eq!(&img_bytes[..bb_data.len()], &bb_data[..]);

    // Catalog still intact.
    let info = run(&["api", "hfs", "info", img_s]);
    let info_text = String::from_utf8(info.stdout).unwrap();
    assert!(info_text.contains("BootTest"));
}

#[test]
fn hfs_new_larger_volume_picks_appropriate_block_size() {
    let dir = tempfile::tempdir().expect("tempdir");
    let img = dir.path().join("scsi.dsk");
    let img_s = img.to_str().unwrap();

    run(&[
        "api", "hfs", "new", img_s, "--size", "5M", "--name", "SCSI5MB",
    ]);

    let info = run(&["api", "hfs", "info", img_s]);
    let info_text = String::from_utf8(info.stdout).unwrap();
    assert!(info_text.contains("SCSI5MB"));
}

#[test]
fn hfs_put_boot_rejects_oversize_source() {
    let dir = tempfile::tempdir().expect("tempdir");
    let img = dir.path().join("disk.dsk");
    let img_s = img.to_str().unwrap();
    run(&["api", "hfs", "new", img_s, "--size", "800K"]);

    let bb = dir.path().join("oversize.bin");
    std::fs::write(&bb, vec![0u8; 2048]).unwrap();

    let out = Command::new(cli_bin())
        .args(["api", "hfs", "put-boot", img_s, bb.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(!out.status.success(), "oversize put-boot should fail");
}
