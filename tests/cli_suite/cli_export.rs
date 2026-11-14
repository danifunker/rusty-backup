//! End-to-end tests for `rb-cli export` (a whole volume to one archive): the format from
//! `--format`, from OUT's extension, or from the filesystem; and the Mac-only formats refused
//! elsewhere.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn cli_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_rb-cli"))
}

fn rb(args: &[&str]) -> Output {
    Command::new(cli_bin())
        .args(args)
        .output()
        .expect("spawn rb-cli")
}

fn ok(args: &[&str]) {
    let out = rb(args);
    assert!(
        out.status.success(),
        "{args:?} failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

fn s(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

/// A FAT floppy holding HELLO.TXT.
fn fat_floppy(dir: &Path) -> PathBuf {
    let img = dir.join("f.img");
    let host = dir.join("hello.txt");
    std::fs::write(&host, b"hello export\n").unwrap();
    ok(&["new", "floppy", "fat", "--size", "1440K", &s(&img)]);
    ok(&["put", &s(&img), &s(&host), "/HELLO.TXT"]);
    img
}

#[test]
fn a_fat_volume_exports_to_a_zip_holding_its_file() {
    let dir = tempfile::tempdir().unwrap();
    let img = fat_floppy(dir.path());
    let out = dir.path().join("vol.zip");
    ok(&["export", &s(&img), &s(&out)]);
    let mut zip = zip::ZipArchive::new(std::fs::File::open(&out).unwrap()).unwrap();
    let mut body = String::new();
    std::io::Read::read_to_string(&mut zip.by_name("HELLO.TXT").unwrap(), &mut body).unwrap();
    assert_eq!(body, "hello export\n");
}

#[test]
fn the_format_comes_from_the_flag_then_the_extension_then_the_filesystem() {
    let dir = tempfile::tempdir().unwrap();
    let img = fat_floppy(dir.path());
    // --format wins over a misleading name.
    let forced = dir.path().join("forced.zip");
    ok(&["export", &s(&img), &s(&forced), "--format", "tgz"]);
    assert_eq!(&std::fs::read(&forced).unwrap()[..2], &[0x1f, 0x8b]);
    // No flag, no known extension: a FAT volume starts on Zip.
    let bare = dir.path().join("bare");
    ok(&["export", &s(&img), &s(&bare)]);
    assert_eq!(&std::fs::read(&bare).unwrap()[..2], b"PK");
}

#[test]
fn mac_archives_and_unknown_formats_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let img = fat_floppy(dir.path());
    let sit = rb(&["export", &s(&img), &s(&dir.path().join("x.sit"))]);
    assert!(!sit.status.success());
    assert!(String::from_utf8_lossy(&sit.stderr).contains("not offered"));
    let bad = rb(&[
        "export",
        &s(&img),
        &s(&dir.path().join("x.out")),
        "--format",
        "rar",
    ]);
    assert!(!bad.status.success());
    assert!(String::from_utf8_lossy(&bad.stderr).contains("unknown format"));
}
