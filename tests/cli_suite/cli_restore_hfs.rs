//! Restoring a classic HFS volume past what its block size reaches: every backup layout rebuilds
//! it, and past HFS's own limit the restore is refused before the target is touched.

use std::path::PathBuf;
use std::process::Command;

fn run_raw(args: &[&str]) -> (bool, String) {
    let out = Command::new(PathBuf::from(env!("CARGO_BIN_EXE_rb-cli")))
        .args(args)
        .output()
        .expect("spawn rb-cli");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.success(), text)
}

fn run(args: &[&str]) -> String {
    let (ok, text) = run_raw(args);
    assert!(ok, "command {args:?} failed:\n{text}");
    text
}

fn fsck_clean(target: &str) {
    let out = run(&["fsck", target]);
    assert!(!out.to_lowercase().contains("error:"), "{target}: {out}");
}

/// A 32 MiB APM disk with one HFS volume and a file, backed up as `format`.
fn hfs_backup(dir: &std::path::Path, format: &str) -> String {
    let p = |n: &str| dir.join(n).to_str().unwrap().to_string();
    run(&[
        "build",
        &p("a.img"),
        "--size",
        "32M",
        "--table",
        "apm",
        "--partition",
        "rest",
        "--format",
        "1=hfs:KEEP",
    ]);
    std::fs::write(p("kept.txt"), vec![0x4Bu8; 300_000]).unwrap();
    run(&[
        "put",
        &format!("{}@1", p("a.img")),
        &p("kept.txt"),
        "/kept.txt",
    ]);
    run(&[
        "backup",
        &p("a.img"),
        &p("bk"),
        "--name",
        "job",
        "--format",
        format,
    ]);
    p("bk/job")
}

fn restore_grows_past_the_block_limit(format: &str) {
    let dir = tempfile::tempdir().unwrap();
    let backup = hfs_backup(dir.path(), format);
    let target = dir.path().join("r.img").to_str().unwrap().to_string();
    let text = run(&[
        "restore",
        &backup,
        &target,
        "--size",
        "fill",
        "--target-size",
        "134217728",
    ]);
    assert!(text.contains("rebuilt with bigger blocks"), "{text}");
    assert!(text.contains("rebuilding it at"), "{text}");

    let part = format!("{target}@1");
    fsck_clean(&part);
    assert!(run(&["ls", &part]).contains("kept.txt"), "file kept");
    // The room is real, not just a bigger number in the MDB.
    let big = dir.path().join("big.bin");
    std::fs::write(&big, vec![7u8; 64 * 1024 * 1024]).unwrap();
    run(&["put", &part, big.to_str().unwrap(), "/big.bin"]);
    fsck_clean(&part);
}

#[test]
fn a_zstd_backup_grows_past_the_block_limit() {
    restore_grows_past_the_block_limit("zstd");
}

#[test]
fn a_raw_backup_grows_past_the_block_limit() {
    restore_grows_past_the_block_limit("raw");
}

#[cfg(feature = "chd")]
#[test]
fn a_chd_backup_grows_past_the_block_limit() {
    restore_grows_past_the_block_limit("chd");
}

#[test]
fn past_the_hfs_limit_nothing_is_written() {
    let dir = tempfile::tempdir().unwrap();
    let backup = hfs_backup(dir.path(), "zstd");
    let target = dir.path().join("r.img");
    let (ok, text) = run_raw(&[
        "restore",
        &backup,
        target.to_str().unwrap(),
        "--size",
        "fill",
        "--target-size",
        "5368709120",
    ]);
    assert!(!ok, "{text}");
    assert!(text.contains("Classic HFS stops at"), "{text}");
    assert!(!target.exists(), "the refusal comes before the target");
}
