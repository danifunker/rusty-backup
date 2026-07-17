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

/// `du` must count the resource fork, where `ls` reports data-fork bytes only.
/// This is the whole point of the verb: a classic-Mac app has its code in the
/// resource fork over a 0-byte data fork, so an `ls`-based size undercounts it.
#[test]
fn du_counts_both_forks_and_distinguishes_missing_from_empty() {
    let dir = tempfile::tempdir().expect("tempdir");
    let img = dir.path().join("disk.dsk");
    let img_s = img.to_str().unwrap();

    run(&[
        "new", img_s, "--fs", "hfs", "--size", "4M", "--name", "DuTest",
    ]);
    run(&["mkdir", img_s, "/App"]);

    // A resource-fork-only "application": 0-byte data fork, 20000-byte rsrc.
    let empty = dir.path().join("empty");
    std::fs::write(&empty, b"").unwrap();
    run(&[
        "put",
        img_s,
        empty.to_str().unwrap(),
        "/App/TheApp",
        "--type",
        "APPL",
        "--creator",
        "MYAP",
    ]);
    let rsrc = dir.path().join("rsrc.bin");
    std::fs::write(&rsrc, vec![0u8; 20000]).unwrap();
    run(&[
        "setrsrc",
        img_s,
        "/App/TheApp",
        "--from-file",
        rsrc.to_str().unwrap(),
    ]);

    // A plain data-fork document, and an empty subfolder.
    let doc = dir.path().join("doc.bin");
    std::fs::write(&doc, vec![0u8; 5000]).unwrap();
    run(&[
        "put",
        img_s,
        doc.to_str().unwrap(),
        "/App/Doc",
        "--type",
        "TEXT",
        "--creator",
        "ttxt",
    ]);
    run(&["mkdir", img_s, "/App/Empty"]);

    // ls sees a 0-byte TheApp (data fork only).
    let ls = String::from_utf8(run(&["ls", img_s, "/App"]).stdout).unwrap();
    assert!(ls.contains("TheApp"));

    // du --json must reflect the real resource-fork bytes.
    let out = run(&["du", img_s, "/App", "/App/Empty", "/Nope", "--json"]);
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let paths = v["result"]["paths"].as_array().unwrap();

    let app = &paths[0];
    assert_eq!(app["path"], "/App");
    assert_eq!(app["found"], true);
    assert_eq!(app["data_bytes"], 5000, "data fork = the 5000-byte doc");
    assert_eq!(app["rsrc_bytes"], 20000, "resource fork must be counted");
    assert_eq!(app["apparent_bytes"], 25000);
    assert_eq!(app["files"], 2);

    // Empty folder: found, but zero bytes — distinguishable from missing.
    let empty_dir = &paths[1];
    assert_eq!(empty_dir["found"], true);
    assert_eq!(empty_dir["apparent_bytes"], 0);

    // Missing path: found=false, no size dump.
    let missing = &paths[2];
    assert_eq!(missing["path"], "/Nope");
    assert_eq!(missing["found"], false);
    assert!(missing.get("data_bytes").is_none());

    // Allocation modeling is reported for HFS.
    assert_eq!(v["result"]["allocation_unit"], 512);

    // --depth 1 must emit the immediate children of /App.
    let out = run(&["du", img_s, "/App", "--depth", "1", "--json"]);
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let entries = v["result"]["paths"][0]["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 3, "Doc, TheApp, Empty");
}

/// The same both-fork accounting must work on HFS+, including the larger
/// allocation block (4 KiB by default), which `alloc_bytes` rounds to.
#[test]
fn du_on_hfsplus_counts_resource_fork_and_rounds_to_block() {
    let dir = tempfile::tempdir().expect("tempdir");
    let img = dir.path().join("plus.img");
    let img_s = img.to_str().unwrap();

    run(&[
        "new", img_s, "--fs", "hfsplus", "--size", "32M", "--name", "Plus",
    ]);
    run(&["mkdir", img_s, "/App"]);

    // Resource-fork-only app: 0-byte data fork, 30000-byte resource fork.
    let empty = dir.path().join("empty");
    std::fs::write(&empty, b"").unwrap();
    run(&[
        "put",
        img_s,
        empty.to_str().unwrap(),
        "/App/TheApp",
        "--type",
        "APPL",
        "--creator",
        "MYAP",
    ]);
    let rsrc = dir.path().join("rsrc.bin");
    std::fs::write(&rsrc, vec![0u8; 30000]).unwrap();
    run(&[
        "setrsrc",
        img_s,
        "/App/TheApp",
        "--from-file",
        rsrc.to_str().unwrap(),
    ]);

    let out = run(&["du", img_s, "/App", "--json"]);
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let unit = v["result"]["allocation_unit"].as_u64().unwrap();
    assert!(
        unit >= 4096,
        "HFS+ block size should be >= 4096, got {unit}"
    );

    let app = &v["result"]["paths"][0];
    assert_eq!(app["data_bytes"], 0);
    assert_eq!(app["rsrc_bytes"], 30000, "resource fork must be counted");
    assert_eq!(app["apparent_bytes"], 30000);
    // 30000 bytes rounds up to a whole number of allocation blocks.
    let alloc = app["alloc_bytes"].as_u64().unwrap();
    assert_eq!(alloc, 30000u64.div_ceil(unit) * unit);
    assert_eq!(app["files"], 1);
}
