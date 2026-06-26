//! End-to-end tests for the flat-verb CLI grammar (Phase A of
//! docs/cli-todo.md). Mirrors the existing `tests/cli_hfs.rs` but
//! drives `rb-cli new / put / get / mkdir / rm / ls / show / fsck` —
//! the stable scripted surface — instead of the deprecated `api hfs`
//! namespace.
//!
//! These run against scratch images in a tempdir. They are the
//! grammar-regression net for the documented verb set; if a flag
//! moves they fail fast.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;

fn cli_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_rb-cli"))
}

/// Decompress a `tests/fixtures/*.zst` image into `dst`.
fn decompress_fixture(name: &str, dst: &Path) {
    let path = format!("tests/fixtures/{name}");
    let compressed = std::fs::read(&path).expect("read fixture");
    let mut decoder =
        zstd::stream::read::Decoder::new(std::io::Cursor::new(compressed)).expect("zstd decoder");
    let mut bytes = Vec::new();
    decoder.read_to_end(&mut bytes).expect("decompress");
    std::fs::write(dst, &bytes).expect("write fixture out");
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

    // `locate` prints the file's absolute byte offset in the image.
    // Re-read at that offset and confirm it matches the source bytes —
    // this is the build-script use case (patch absolute offsets into a
    // boot block / supervisor payload without grepping for markers).
    let loc = run(&["locate", img_s, "/hello.txt"]);
    let loc_json: serde_json::Value =
        serde_json::from_slice(&loc.stdout).expect("locate emits JSON");
    let result = &loc_json["result"];
    assert_eq!(result["path"], "/hello.txt");
    assert_eq!(result["fragmented"], false);
    assert_eq!(result["partition_offset"], 0);
    let offset = result["offset"].as_u64().expect("offset is u64");
    let length = result["length"].as_u64().expect("length is u64");
    assert_eq!(length, std::fs::read(&host).unwrap().len() as u64);
    let img_bytes = std::fs::read(&img).unwrap();
    let slice = &img_bytes[offset as usize..(offset + length) as usize];
    assert_eq!(
        slice,
        std::fs::read(&host).unwrap().as_slice(),
        "bytes at locate offset must match source file"
    );

    // mkdir + put into the new dir. Use --print-offset here so we
    // exercise the put-then-emit shape build scripts will use; the
    // JSON it prints must agree with what a separate `locate` call
    // would return.
    run(&["mkdir", img_s, "/Apps"]);
    let host2 = dir.path().join("foo.bin");
    std::fs::write(&host2, b"\x01\x02\x03").unwrap();
    let put_out = run(&[
        "put",
        img_s,
        host2.to_str().unwrap(),
        "/Apps/foo.bin",
        "--print-offset",
    ]);
    let put_json: serde_json::Value =
        serde_json::from_slice(&put_out.stdout).expect("--print-offset emits JSON");
    let loc2 = run(&["locate", img_s, "/Apps/foo.bin"]);
    let loc2_json: serde_json::Value = serde_json::from_slice(&loc2.stdout).unwrap();
    assert_eq!(
        put_json["result"], loc2_json["result"],
        "put --print-offset must match a follow-up locate"
    );
    let off2 = put_json["result"]["offset"].as_u64().unwrap();
    let img_bytes2 = std::fs::read(&img).unwrap();
    assert_eq!(
        &img_bytes2[off2 as usize..off2 as usize + 3],
        b"\x01\x02\x03",
        "bytes at --print-offset location must match source"
    );

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
    // `filesystem` now reports the driver's own type string (was a
    // hardcoded "hfs" when fs-info was HFS-only).
    assert_eq!(v["result"]["filesystem"], "HFS");
    assert_eq!(v["result"]["volume_name"], "JsonTest");
}

#[test]
fn show_fs_info_fat_superfloppy_not_misdetected_as_hfs() {
    // Regression: a flat FAT12 superfloppy (FAT boot sector at byte 0, no MBR)
    // was misdetected as HFS because `show fs-info` hardcoded the HFS opener;
    // it died with "opening HFS: ... bad MDB signature: 0x1557". fs-info now
    // routes through the generic `open_filesystem` path (same as `ls` / the
    // GUI inspect path), so it must report FAT.
    //
    // The fixture is the real-world repro: a cb-dos FreeDOS 1.44 MB boot
    // floppy (label FD14-BOOT, ~1.3 MiB used / ~93 KiB free).
    let dir = tempfile::tempdir().expect("tempdir");
    let img = dir.path().join("freedos.img");
    decompress_fixture("test_fat12_freedos_superfloppy.img.zst", &img);
    let img_s = img.to_str().unwrap();

    // Text form: the filesystem line names FAT, never HFS, and the command
    // succeeds (run() panics on a non-zero exit, which is the bug's failure).
    let out = run(&["show", "fs-info", img_s]);
    let text = String::from_utf8(out.stdout).unwrap();
    assert!(
        text.contains("Filesystem:  FAT12"),
        "expected FAT12, got:\n{text}"
    );
    assert!(
        !text.contains("HFS"),
        "a FAT superfloppy must not be reported as HFS:\n{text}"
    );

    // JSON form: structured payload reports the FAT type, the known label, and
    // the used/free figures from the used-size fix (commit 96a446d).
    let out = run(&["show", "fs-info", img_s, "--format", "json"]);
    let text = String::from_utf8(out.stdout).unwrap();
    let v: serde_json::Value = serde_json::from_str(&text).expect("valid JSON");
    assert_eq!(v["status"]["error"], false);
    assert_eq!(v["result"]["filesystem"], "FAT12");
    assert_eq!(v["result"]["volume_name"], "FD14-BOOT");
    let total = v["result"]["total_bytes"].as_u64().unwrap();
    let used = v["result"]["used_bytes"].as_u64().unwrap();
    let free = v["result"]["free_bytes"].as_u64().unwrap();
    assert_eq!(total, 1474560, "1.44 MB floppy");
    assert_eq!(total, used + free, "total must equal used + free");
    // Real used/free of this disk image (not a total/2 placeholder).
    assert_eq!(used, 1379328);
    assert_eq!(free, 95232);
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
fn get_glob_extracts_each_match_under_destination() {
    let dir = tempfile::tempdir().expect("tempdir");
    let img = dir.path().join("disk.dsk");
    let img_s = img.to_str().unwrap();
    run(&[
        "new", img_s, "--fs", "hfs", "--size", "50M", "--name", "GetGlob",
    ]);

    // Populate three files. Distinct content per file so we can spot
    // any wrong-target write.
    for (name, content) in &[
        ("a.txt", b"alpha".as_slice()),
        ("b.txt", b"beta".as_slice()),
        ("c.md", b"gamma".as_slice()),
    ] {
        let host = dir.path().join(name);
        std::fs::write(&host, content).unwrap();
        run(&["put", img_s, host.to_str().unwrap(), &format!("/{name}")]);
    }

    // `get '/*.txt' OUT/` extracts a.txt + b.txt into OUT, leaves c.md.
    let out = dir.path().join("out");
    std::fs::create_dir(&out).unwrap();
    run(&[
        "get",
        img_s,
        "/*.txt",
        // Trailing separator handled by ensure_dir() in the verb.
        out.to_str().unwrap(),
    ]);
    assert_eq!(std::fs::read(out.join("a.txt")).unwrap(), b"alpha");
    assert_eq!(std::fs::read(out.join("b.txt")).unwrap(), b"beta");
    assert!(
        !out.join("c.md").exists(),
        "non-matching c.md should not have been extracted"
    );
}

#[test]
fn get_recursive_extracts_subdir_tree() {
    let dir = tempfile::tempdir().expect("tempdir");
    let img = dir.path().join("disk.dsk");
    let img_s = img.to_str().unwrap();
    run(&[
        "new", img_s, "--fs", "hfs", "--size", "50M", "--name", "GetRecur",
    ]);

    // Populate /Apps/ with one direct file + one nested dir + nested file.
    run(&["mkdir", img_s, "/Apps"]);
    run(&["mkdir", img_s, "/Apps/Sub"]);

    let a = dir.path().join("a.txt");
    std::fs::write(&a, b"top").unwrap();
    let b = dir.path().join("nested.txt");
    std::fs::write(&b, b"deep").unwrap();
    run(&["put", img_s, a.to_str().unwrap(), "/Apps/a.txt"]);
    run(&["put", img_s, b.to_str().unwrap(), "/Apps/Sub/nested.txt"]);

    let out = dir.path().join("out");
    run(&["get", img_s, "/Apps", out.to_str().unwrap(), "-r"]);

    // cp-like convention: the basename of the source rides under DST.
    let apps_dir = out.join("Apps");
    assert!(apps_dir.is_dir(), "Apps/ should exist under DST");
    assert_eq!(std::fs::read(apps_dir.join("a.txt")).unwrap(), b"top");
    assert!(apps_dir.join("Sub").is_dir(), "nested dir should exist");
    assert_eq!(
        std::fs::read(apps_dir.join("Sub").join("nested.txt")).unwrap(),
        b"deep"
    );
}

#[test]
fn get_literal_directory_without_recursive_is_an_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let img = dir.path().join("disk.dsk");
    let img_s = img.to_str().unwrap();
    run(&[
        "new", img_s, "--fs", "hfs", "--size", "5M", "--name", "GetNoR",
    ]);
    run(&["mkdir", img_s, "/Empty"]);

    let out = dir.path().join("out");
    let outs = out.to_str().unwrap();
    let err = run_expect_fail(&["get", img_s, "/Empty", outs]);
    let stderr = String::from_utf8_lossy(&err.stderr);
    assert!(
        stderr.contains("directory") && (stderr.contains("--recursive") || stderr.contains("-r")),
        "expected directory + recursive hint in stderr; got: {stderr}"
    );
}

#[test]
fn get_force_overwrites_existing_host_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let img = dir.path().join("disk.dsk");
    let img_s = img.to_str().unwrap();
    run(&[
        "new", img_s, "--fs", "hfs", "--size", "5M", "--name", "GetForce",
    ]);

    let host_src = dir.path().join("src.txt");
    std::fs::write(&host_src, b"new content").unwrap();
    run(&["put", img_s, host_src.to_str().unwrap(), "/src.txt"]);

    // Pre-populate DST with stale content.
    let back = dir.path().join("back.txt");
    std::fs::write(&back, b"old content").unwrap();

    // Without --force, the existing host file is a hard error.
    let err = run_expect_fail(&["get", img_s, "/src.txt", back.to_str().unwrap()]);
    let stderr = String::from_utf8_lossy(&err.stderr);
    assert!(
        stderr.contains("destination exists") || stderr.contains("already exists"),
        "expected existence error; got: {stderr}"
    );

    // --force replaces the host bytes.
    run(&["get", img_s, "/src.txt", back.to_str().unwrap(), "--force"]);
    assert_eq!(std::fs::read(&back).unwrap(), b"new content");
}

#[test]
fn get_skip_existing_leaves_old_content_alone() {
    let dir = tempfile::tempdir().expect("tempdir");
    let img = dir.path().join("disk.dsk");
    let img_s = img.to_str().unwrap();
    run(&[
        "new", img_s, "--fs", "hfs", "--size", "5M", "--name", "GetSkip",
    ]);

    let host_src = dir.path().join("src.txt");
    std::fs::write(&host_src, b"new content").unwrap();
    run(&["put", img_s, host_src.to_str().unwrap(), "/src.txt"]);

    let back = dir.path().join("back.txt");
    std::fs::write(&back, b"keep me").unwrap();
    run(&[
        "get",
        img_s,
        "/src.txt",
        back.to_str().unwrap(),
        "--skip-existing",
    ]);
    assert_eq!(std::fs::read(&back).unwrap(), b"keep me");
}

#[test]
fn get_glob_exclude_filters_matches() {
    let dir = tempfile::tempdir().expect("tempdir");
    let img = dir.path().join("disk.dsk");
    let img_s = img.to_str().unwrap();
    run(&[
        "new", img_s, "--fs", "hfs", "--size", "10M", "--name", "GetExcl",
    ]);

    for name in &["a.txt", "b.txt", "c.txt"] {
        let host = dir.path().join(name);
        std::fs::write(&host, name.as_bytes()).unwrap();
        run(&["put", img_s, host.to_str().unwrap(), &format!("/{name}")]);
    }

    let out = dir.path().join("out");
    std::fs::create_dir(&out).unwrap();
    run(&[
        "get",
        img_s,
        "/*.txt",
        out.to_str().unwrap(),
        "--exclude",
        "/b.txt",
    ]);
    assert!(out.join("a.txt").exists());
    assert!(!out.join("b.txt").exists(), "excluded file extracted");
    assert!(out.join("c.txt").exists());
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
fn put_boot_apm_writes_at_partition_offset_not_image_zero() {
    // Regression: `put --boot` against an APM-wrapped image must write
    // the 1024-byte boot region at the *partition's* LBA 0 (e.g. byte
    // 0xC000 for a typical Apple_HFS placement), not the image's byte
    // 0 — that would overwrite the APM Driver Descriptor Record and
    // brick the disk for a Mac ROM.
    use rusty_backup::fs::hfs::create_blank_hfs;
    use rusty_backup::fs::hfs_clone::emit_apm_disk_with_hfs;
    use rusty_backup::partition::apm::build_minimal_apm;
    use std::io::Cursor;

    let dir = tempfile::tempdir().expect("tempdir");
    let img = dir.path().join("apm.hda");

    let total_blocks: u32 = 4096;
    let driver_start: u32 = 32;
    let driver_blocks: u32 = 8;
    let hfs_start: u32 = 96; // 0xC000 — the canonical APM layout
    let mut apm = build_minimal_apm(
        &[
            ("Apple_Driver43".to_string(), driver_start, driver_blocks),
            ("Apple_HFS".to_string(), hfs_start, total_blocks - hfs_start),
        ],
        512,
        total_blocks,
    );
    apm.entries[2].name = "MacOS".to_string();
    let blocks = apm.build_apm_blocks(Some(total_blocks));
    let mut source = vec![0u8; total_blocks as usize * 512];
    source[..blocks.len()].copy_from_slice(&blocks);

    let hfs_bytes = (total_blocks - hfs_start) as u64 * 512;
    let hfs_image = create_blank_hfs(hfs_bytes, 4096, "BootApm").unwrap();
    let source_len = source.len() as u64;
    let mut out: Vec<u8> = Vec::new();
    let mut src_cursor = Cursor::new(source);
    let mut out_cursor = Cursor::new(&mut out);
    emit_apm_disk_with_hfs(&mut src_cursor, source_len, &hfs_image, &mut out_cursor)
        .expect("emit APM disk");
    std::fs::write(&img, &out).unwrap();

    // Snapshot byte 0 of the image (the DDR's "ER" signature) so we can
    // confirm `--boot` left it alone.
    let pre = std::fs::read(&img).unwrap();
    let ddr_sig_before = [pre[0], pre[1]];
    assert_eq!(
        &ddr_sig_before, b"ER",
        "expected APM DDR signature pre-write"
    );

    let bb = dir.path().join("bb.bin");
    let bb_data: Vec<u8> = (0..600u32).map(|i| (i % 256) as u8).collect();
    std::fs::write(&bb, &bb_data).unwrap();

    let img_at = format!("{}@1", img.to_str().unwrap());
    run(&["put", &img_at, "--boot", bb.to_str().unwrap()]);

    let post = std::fs::read(&img).unwrap();
    // DDR untouched.
    assert_eq!(
        &post[..2],
        b"ER",
        "image byte 0 must still be APM DDR signature; --boot smashed the wrong offset"
    );
    // Bytes landed at the partition's LBA 0.
    let part_off = hfs_start as usize * 512;
    assert_eq!(
        &post[part_off..part_off + bb_data.len()],
        &bb_data[..],
        "boot bytes must land at partition_offset, not image byte 0"
    );
}

#[test]
fn locate_returns_absolute_offset_inside_apm_wrapped_image() {
    // The load-bearing test for `locate`: an APM-wrapped disk where
    // the HFS partition starts well past byte 0. The printed offset
    // must include the partition base — that's the whole point of the
    // verb (the build-script consumer shouldn't have to know APM
    // layout).
    use rusty_backup::fs::hfs::create_blank_hfs;
    use rusty_backup::fs::hfs_clone::emit_apm_disk_with_hfs;
    use rusty_backup::partition::apm::build_minimal_apm;
    use std::io::Cursor;

    let dir = tempfile::tempdir().expect("tempdir");
    let img = dir.path().join("apm.hda");

    // Build a 256 KiB synthetic source APM with one driver + one HFS
    // partition starting at block 64 (= 0x8000 bytes). This mirrors
    // the in-crate `emit_apm_disk_round_trip` fixture, kept here so
    // the integration test is self-contained.
    let total_blocks: u32 = 4096; // 2 MiB total
    let driver_start: u32 = 32;
    let driver_blocks: u32 = 8;
    let hfs_start: u32 = 64;
    let mut apm = build_minimal_apm(
        &[
            ("Apple_Driver43".to_string(), driver_start, driver_blocks),
            ("Apple_HFS".to_string(), hfs_start, total_blocks - hfs_start),
        ],
        512,
        total_blocks,
    );
    apm.entries[2].name = "MacOS".to_string();
    let blocks = apm.build_apm_blocks(Some(total_blocks));
    let mut source = vec![0u8; total_blocks as usize * 512];
    source[..blocks.len()].copy_from_slice(&blocks);

    // Format a blank HFS volume sized to fill from hfs_start to end.
    let hfs_bytes = (total_blocks - hfs_start) as u64 * 512;
    let hfs_image = create_blank_hfs(hfs_bytes, 4096, "ApmLoc").unwrap();

    // emit_apm_disk_with_hfs writes the final disk.
    let mut out: Vec<u8> = Vec::new();
    let mut src_cursor = Cursor::new(source.clone());
    let mut out_cursor = Cursor::new(&mut out);
    emit_apm_disk_with_hfs(
        &mut src_cursor,
        source.len() as u64,
        &hfs_image,
        &mut out_cursor,
    )
    .expect("emit APM disk");
    std::fs::write(&img, &out).unwrap();

    let img_s = img.to_str().unwrap();

    // Put a payload through the verb, then locate it. Use the @N
    // selector because the auto-pick still has Apple_partition_map +
    // Apple_Driver43 alongside Apple_HFS — `resolve_partition_*`
    // filters those out for `pick_default_partition` but we pin to
    // partition 3 explicitly to match how scripts would target HFS.
    // The CLI's APM enumeration only surfaces FS-bearing partitions
    // (the self-referencing map entry and Apple_Driver43 are filtered
    // out), so the lone Apple_HFS shows up at index 1.
    let img_at = format!("{img_s}@1");
    let host = dir.path().join("payload.bin");
    let payload = b"abcdefghij-from-apm-payload";
    std::fs::write(&host, payload).unwrap();
    run(&["put", &img_at, host.to_str().unwrap(), "/Payload"]);

    let loc = run(&["locate", &img_at, "/Payload"]);
    let v: serde_json::Value = serde_json::from_slice(&loc.stdout).unwrap();
    let r = &v["result"];
    let offset = r["offset"].as_u64().expect("offset");
    let length = r["length"].as_u64().expect("length");
    let part_offset = r["partition_offset"].as_u64().expect("partition_offset");

    assert_eq!(length, payload.len() as u64);
    assert_eq!(
        part_offset,
        hfs_start as u64 * 512,
        "partition_offset must equal the APM-declared HFS start"
    );
    assert!(
        offset >= part_offset,
        "offset ({offset}) must be inside the HFS partition (starts at {part_offset})"
    );

    let disk = std::fs::read(&img).unwrap();
    assert_eq!(
        &disk[offset as usize..(offset + length) as usize],
        payload,
        "bytes at locate offset inside APM disk must match source payload"
    );
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
