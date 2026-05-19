//! End-to-end tests for the flat-verb CLI grammar (Phase A of
//! docs/cli-todo.md). Mirrors the existing `tests/cli_hfs.rs` but
//! drives `rb-cli new / put / get / mkdir / rm / ls / show / fsck` —
//! the stable scripted surface — instead of the deprecated `api hfs`
//! namespace.
//!
//! These run against scratch images in a tempdir. They are the
//! grammar-regression net for the documented verb set; if a flag
//! moves they fail fast.

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

fn run_expect_fail(args: &[&str]) -> std::process::Output {
    let out = Command::new(cli_bin())
        .args(args)
        .output()
        .expect("spawn rb-cli");
    assert!(
        !out.status.success(),
        "command {args:?} unexpectedly succeeded:\n{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    out
}

#[test]
fn new_hfs_put_get_mkdir_rm_round_trip() {
    let dir = tempfile::tempdir().expect("tempdir");
    let img = dir.path().join("disk.dsk");
    let img_s = img.to_str().unwrap();

    // 50M — the create_blank_hfs catalog/extents B-tree node count
    // scales with volume size; small floppies fill up after just a few
    // entries which is fine for the legacy tests in tests/cli_hfs.rs
    // but not for this mkdir + multiple-files scenario.
    run(&[
        "new", img_s, "--fs", "hfs", "--size", "50M", "--name", "FlatRT",
    ]);

    // Library can re-open it.
    {
        use rusty_backup::fs::hfs::HfsFilesystem;
        let f = std::fs::File::open(&img).unwrap();
        let fs = HfsFilesystem::open(f, 0).expect("re-open HFS image");
        assert_eq!(fs.volume_summary().volume_name, "FlatRT");
    }

    // Host file -> put.
    let host = dir.path().join("hello.txt");
    std::fs::write(&host, b"hello from host\n").unwrap();
    run(&["put", img_s, host.to_str().unwrap(), "/hello.txt"]);

    // get the same content back.
    let back = dir.path().join("back.txt");
    run(&["get", img_s, "/hello.txt", back.to_str().unwrap()]);
    assert_eq!(std::fs::read(&host).unwrap(), std::fs::read(&back).unwrap());

    // mkdir + put into the new dir.
    run(&["mkdir", img_s, "/Apps"]);
    let host2 = dir.path().join("foo.bin");
    std::fs::write(&host2, b"\x01\x02\x03").unwrap();
    run(&["put", img_s, host2.to_str().unwrap(), "/Apps/foo.bin"]);

    // ls shows both top-level entries.
    let ls = run(&["ls", img_s, "/"]);
    let ls_text = String::from_utf8(ls.stdout).unwrap();
    assert!(ls_text.contains("hello.txt"));
    assert!(ls_text.contains("Apps"));

    // ls deeper.
    let ls_apps = run(&["ls", img_s, "/Apps"]);
    let ls_apps_text = String::from_utf8(ls_apps.stdout).unwrap();
    assert!(ls_apps_text.contains("foo.bin"));

    // put --zero pre-allocates a typed file. `--dst` is required here
    // because the positional DST slot is reserved for the cp-like shape.
    run(&[
        "put",
        img_s,
        "--zero",
        "4096",
        "--dst",
        "/Results.jsonl",
        "--type",
        "TEXT",
        "--creator",
        "ttxt",
    ]);
    let ls3 = run(&["ls", img_s, "/"]);
    let ls3_text = String::from_utf8(ls3.stdout).unwrap();
    assert!(
        ls3_text.contains("Results.jsonl") && ls3_text.contains("TEXT ttxt"),
        "ls output missing Results.jsonl typed entry: {ls3_text}"
    );

    // rm the file we wrote first.
    run(&["rm", img_s, "/hello.txt"]);
    let ls4 = run(&["ls", img_s, "/"]);
    let ls4_text = String::from_utf8(ls4.stdout).unwrap();
    assert!(!ls4_text.contains("hello.txt"));

    // fsck --checkonly passes on the live image.
    run(&["fsck", img_s, "--checkonly"]);
}

#[test]
fn put_boot_writes_exact_bytes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let img = dir.path().join("disk.dsk");
    let img_s = img.to_str().unwrap();

    run(&[
        "new", img_s, "--fs", "hfs", "--size", "800K", "--name", "BootTest",
    ]);

    let bb = dir.path().join("bb.bin");
    let bb_data: Vec<u8> = (0..600u32).map(|i| (i % 256) as u8).collect();
    std::fs::write(&bb, &bb_data).unwrap();

    run(&["put", img_s, "--boot", bb.to_str().unwrap()]);

    let img_bytes = std::fs::read(&img).unwrap();
    assert_eq!(&img_bytes[..bb_data.len()], &bb_data[..]);
}

#[test]
fn show_fs_info_json_envelope_shape() {
    let dir = tempfile::tempdir().expect("tempdir");
    let img = dir.path().join("disk.dsk");
    let img_s = img.to_str().unwrap();

    run(&[
        "new", img_s, "--fs", "hfs", "--size", "800K", "--name", "JsonTest",
    ]);

    let out = run(&["show", "fs-info", img_s, "--format", "json"]);
    let text = String::from_utf8(out.stdout).unwrap();
    let v: serde_json::Value = serde_json::from_str(&text).expect("valid JSON");
    assert_eq!(v["schema_version"], 1);
    assert_eq!(v["status"]["error"], false);
    assert_eq!(v["result"]["filesystem"], "hfs");
    assert_eq!(v["result"]["volume_name"], "JsonTest");
}

#[test]
fn img_at_partition_selector_rejects_zero() {
    // Top-level help should still come up; specific error on bad selector.
    run_expect_fail(&["ls", "anything.hda@0"]);
}

#[test]
fn rm_nonexistent_returns_nonzero() {
    let dir = tempfile::tempdir().expect("tempdir");
    let img = dir.path().join("disk.dsk");
    let img_s = img.to_str().unwrap();
    run(&[
        "new", img_s, "--fs", "hfs", "--size", "800K", "--name", "Test",
    ]);
    run_expect_fail(&["rm", img_s, "/nope"]);
}

#[test]
fn completions_emits_nonempty_script_for_each_shell() {
    for shell in &["bash", "zsh", "fish", "power-shell", "elvish"] {
        let out = run(&["completions", shell]);
        assert!(
            !out.stdout.is_empty(),
            "completions for {shell} produced empty output"
        );
    }
}

#[test]
fn inspect_text_runs_on_raw_hfs_image() {
    let dir = tempfile::tempdir().expect("tempdir");
    let img = dir.path().join("disk.dsk");
    let img_s = img.to_str().unwrap();
    run(&[
        "new", img_s, "--fs", "hfs", "--size", "800K", "--name", "InsTest",
    ]);

    let out = run(&["inspect", img_s]);
    let text = String::from_utf8(out.stdout).unwrap();
    // Raw HFS at byte 0 has no partition table.
    assert!(text.contains("Partition table: None"));
}
