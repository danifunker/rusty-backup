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
fn generic_verbs_round_trip_against_hfsplus() {
    // Phase B: the flat verbs auto-detect filesystem via
    // open_filesystem / open_editable_filesystem dispatch. Building an
    // HFS+ image here (not the HFS image new --fs hfs makes) exercises
    // that path end-to-end against a non-HFS target.
    use rusty_backup::fs::hfsplus::create_blank_hfsplus;
    use std::io::Write;

    let dir = tempfile::tempdir().expect("tempdir");
    let img = dir.path().join("plus.img");

    let bytes = create_blank_hfsplus(8 * 1024 * 1024, 4096, "HfsPlusRT", false);
    let mut f = std::fs::File::create(&img).unwrap();
    f.write_all(&bytes).unwrap();
    drop(f);

    let img_s = img.to_str().unwrap();

    // ls / inspect work without --fs flag because the verb dispatch
    // uses partition-table + magic-byte detection.
    let inspect = run(&["inspect", img_s]);
    assert!(String::from_utf8_lossy(&inspect.stdout).contains("Partition table"));

    let ls0 = run(&["ls", img_s, "/"]);
    // Fresh HFS+ root may be empty or carry only the system-area
    // private-data dir — only assert we got a successful response.
    let _ = String::from_utf8(ls0.stdout).unwrap();

    // Put / get / rm roundtrip.
    let host = dir.path().join("host.bin");
    std::fs::write(&host, b"hfs+ rocks").unwrap();
    run(&["put", img_s, host.to_str().unwrap(), "/host.bin"]);

    let back = dir.path().join("back.bin");
    run(&["get", img_s, "/host.bin", back.to_str().unwrap()]);
    assert_eq!(
        std::fs::read(&host).unwrap(),
        std::fs::read(&back).unwrap(),
        "HFS+ put -> get should round-trip"
    );

    run(&["mkdir", img_s, "/SubDir"]);
    let ls_root = run(&["ls", img_s, "/"]);
    let ls_text = String::from_utf8(ls_root.stdout).unwrap();
    assert!(ls_text.contains("host.bin"));
    assert!(ls_text.contains("SubDir"));

    run(&["rm", img_s, "/host.bin"]);
}

#[test]
fn ls_and_rm_support_glob_patterns() {
    let dir = tempfile::tempdir().expect("tempdir");
    let img = dir.path().join("disk.dsk");
    let img_s = img.to_str().unwrap();
    run(&[
        "new", img_s, "--fs", "hfs", "--size", "50M", "--name", "GlobTest",
    ]);

    let a = dir.path().join("a.txt");
    std::fs::write(&a, b"a").unwrap();
    let b = dir.path().join("b.txt");
    std::fs::write(&b, b"b").unwrap();
    let c = dir.path().join("notes.md");
    std::fs::write(&c, b"c").unwrap();
    for (host, dst) in &[(&a, "/a.txt"), (&b, "/b.txt"), (&c, "/notes.md")] {
        run(&["put", img_s, host.to_str().unwrap(), dst]);
    }

    // ls /*.txt finds the two .txt files anywhere under root.
    let out = run(&["ls", img_s, "/*.txt"]);
    let text = String::from_utf8(out.stdout).unwrap();
    assert!(text.contains("/a.txt"), "expected /a.txt in {text}");
    assert!(text.contains("/b.txt"), "expected /b.txt in {text}");
    assert!(
        !text.contains("notes.md"),
        "notes.md should not match: {text}"
    );

    // exclude wins.
    let out2 = run(&["ls", img_s, "/*.txt", "--exclude", "/a.txt"]);
    let text2 = String::from_utf8(out2.stdout).unwrap();
    assert!(text2.contains("/b.txt"));
    assert!(!text2.contains("/a.txt"));

    // rm glob removes both .txt files.
    run(&["rm", img_s, "/*.txt"]);
    let ls_after = run(&["ls", img_s, "/"]);
    let text3 = String::from_utf8(ls_after.stdout).unwrap();
    assert!(!text3.contains("a.txt"));
    assert!(!text3.contains("b.txt"));
    assert!(text3.contains("notes.md"));
}

#[test]
fn backup_then_restore_round_trip_file_to_file() {
    // Phase C: rb-cli backup IMG DIR  ->  rb-cli restore DIR OUT
    // Smoke-tests that the two flat verbs hand off through the on-disk
    // backup folder. Uses an HFS image so we don't depend on having
    // root or an actual block device.

    let dir = tempfile::tempdir().expect("tempdir");
    let src_img = dir.path().join("src.dsk");
    let src_s = src_img.to_str().unwrap();

    // Build a small HFS image so the backup has something to chew.
    run(&[
        "new",
        src_s,
        "--fs",
        "hfs",
        "--size",
        "5M",
        "--name",
        "BackupSrc",
    ]);

    let backups = dir.path().join("backups");
    std::fs::create_dir_all(&backups).unwrap();

    run(&[
        "backup",
        src_s,
        backups.to_str().unwrap(),
        "--name",
        "round_trip",
        "--format",
        "zstd",
        "--checksum",
        "crc32",
    ]);

    let backup_dir = backups.join("round_trip");
    assert!(
        backup_dir.join("metadata.json").exists(),
        "backup didn't produce metadata.json"
    );

    let restored = dir.path().join("restored.img");
    run(&[
        "restore",
        backup_dir.to_str().unwrap(),
        restored.to_str().unwrap(),
    ]);

    // Restored file exists and is non-empty.
    let len = std::fs::metadata(&restored).unwrap().len();
    assert!(len > 0, "restored file is empty");
}

#[test]
fn write_requires_yes_flag() {
    // We can't actually flash a device in a test, but we can verify the
    // safety gate fires without --yes.
    let dir = tempfile::tempdir().expect("tempdir");
    let img = dir.path().join("src.dsk");
    let img_s = img.to_str().unwrap();
    run(&[
        "new",
        img_s,
        "--fs",
        "hfs",
        "--size",
        "800K",
        "--name",
        "WriteTest",
    ]);
    // Targeting a regular file would actually copy bytes if we passed
    // --yes; here we omit it to exercise the destructive-write refusal.
    let target = dir.path().join("target.bin");
    let target_s = target.to_str().unwrap();
    run_expect_fail(&["write", img_s, target_s]);
}

#[test]
fn batch_applies_mkdir_put_rm_in_one_pass() {
    let dir = tempfile::tempdir().expect("tempdir");
    let img = dir.path().join("disk.dsk");
    let img_s = img.to_str().unwrap();
    run(&[
        "new", img_s, "--fs", "hfs", "--size", "10M", "--name", "Batch",
    ]);

    let host_a = dir.path().join("a.bin");
    std::fs::write(&host_a, b"AAA").unwrap();
    let host_b = dir.path().join("b.bin");
    std::fs::write(&host_b, b"BBB").unwrap();

    let script = dir.path().join("script.json");
    let script_text = serde_json::json!({
        "schema": "rb-cli-batch/1",
        "target": img_s,
        "operations": [
            { "op": "mkdir", "path": "/Stuff" },
            { "op": "put", "src": host_a.to_str().unwrap(), "dst": "/Stuff/a.bin" },
            { "op": "put", "src": host_b.to_str().unwrap(), "dst": "/Stuff/b.bin" },
            { "op": "rm", "path": "/Stuff/a.bin" }
        ]
    });
    std::fs::write(&script, serde_json::to_string_pretty(&script_text).unwrap()).unwrap();

    // Dry-run prints a plan but doesn't write.
    let dry = run(&["batch", script.to_str().unwrap(), "--dry-run"]);
    let dry_text = String::from_utf8(dry.stdout).unwrap();
    assert!(dry_text.contains("mkdir /Stuff"));
    assert!(dry_text.contains("put"));

    // Real apply.
    run(&["batch", script.to_str().unwrap()]);

    let ls = run(&["ls", img_s, "/Stuff"]);
    let ls_text = String::from_utf8(ls.stdout).unwrap();
    assert!(!ls_text.contains("a.bin"), "a.bin should be rm'd");
    assert!(ls_text.contains("b.bin"), "b.bin should remain");
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
