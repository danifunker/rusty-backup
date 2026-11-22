//! `rb-cli tree`: every volume on a disk under a heading, one with `IMG@N`, filesystem IDs with
//! `--ids`, and a disc image's filesystems.

use std::path::{Path, PathBuf};
use std::process::Command;

fn cli() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_rb-cli"))
}

fn rb(args: &[&str]) -> (bool, String, String) {
    let out = Command::new(cli()).args(args).output().expect("run rb-cli");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// A 16 MiB MBR disk: partition 1 (FAT, "ONE") with DOCS/README.TXT, partition 2 ("TWO") with
/// TWO.TXT.
fn two_partition_disk(dir: &Path) -> String {
    let img = dir.join("d.img").to_string_lossy().into_owned();
    let txt = dir.join("a.txt");
    std::fs::write(&txt, b"hi\n").unwrap();
    let txt = txt.to_string_lossy().into_owned();
    let build = [
        "build",
        &img,
        "--size",
        "16M",
        "--table",
        "mbr",
        "--partition",
        "6M",
        "--partition",
        "rest",
        "--format",
        "1=fat:ONE",
        "--format",
        "2=fat:TWO",
    ];
    let p1 = format!("{img}@1");
    let p2 = format!("{img}@2");
    let steps: [&[&str]; 4] = [
        &build,
        &["mkdir", &p1, "/DOCS"],
        &["put", &p1, &txt, "/DOCS/README.TXT"],
        &["put", &p2, &txt, "/TWO.TXT"],
    ];
    for args in steps {
        let (ok, _, err) = rb(args);
        assert!(ok, "{args:?}: {err}");
    }
    img
}

#[test]
fn the_whole_disk_lists_every_partition_under_a_heading() {
    let dir = tempfile::tempdir().unwrap();
    let img = two_partition_disk(dir.path());
    let (ok, out, err) = rb(&["tree", &img]);
    assert!(ok, "{err}");
    let one = out.find("== Partition @1").expect("partition 1 heading");
    let two = out.find("== Partition @2").expect("partition 2 heading");
    assert!(one < two, "{out}");
    let (first, second) = out.split_at(two);
    assert!(
        first.contains("DOCS") && first.contains("README.TXT"),
        "{out}"
    );
    assert!(
        second.contains("TWO.TXT") && !second.contains("README.TXT"),
        "{out}"
    );
    assert!(out.contains("1 directories, 1 files") && out.contains("0 directories, 1 files"));
}

#[test]
fn one_partition_prints_its_tree_alone_with_the_label_on_stderr() {
    let dir = tempfile::tempdir().unwrap();
    let img = two_partition_disk(dir.path());
    let (ok, out, err) = rb(&["tree", &format!("{img}@2")]);
    assert!(ok, "{err}");
    assert!(out.starts_with("TWO\n"), "{out}");
    assert!(out.contains("TWO.TXT") && !out.contains("=="), "{out}");
    assert!(err.contains("Partition @2"), "{err}");
}

#[test]
fn ids_adds_each_entry_s_filesystem_id() {
    let dir = tempfile::tempdir().unwrap();
    let img = two_partition_disk(dir.path());
    let (ok, out, err) = rb(&["tree", &format!("{img}@1"), "--ids"]);
    assert!(ok, "{err}");
    assert!(out
        .lines()
        .all(|l| l.is_empty() || l.contains("(ID: ") || l.contains("files")));
}

#[test]
fn a_partition_out_of_range_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let img = two_partition_disk(dir.path());
    let (ok, _, err) = rb(&["tree", &format!("{img}@3")]);
    assert!(!ok && err.contains("out of range"), "{err}");
}

#[test]
fn a_disc_image_lists_its_filesystem() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    std::fs::create_dir_all(src.join("Folder")).unwrap();
    std::fs::write(src.join("Folder").join("Note"), b"note").unwrap();
    let iso = dir.path().join("cd.iso").to_string_lossy().into_owned();
    let (ok, _, err) = rb(&[
        "optical",
        "new",
        "mac-hfs",
        &iso,
        "--from-dir",
        &src.to_string_lossy(),
        "--size",
        "auto",
    ]);
    assert!(ok, "optical new: {err}");
    let (ok, out, err) = rb(&["tree", &iso]);
    assert!(ok, "{err}");
    assert!(
        out.contains("Folder") && out.contains("Note"),
        "{out}\n{err}"
    );
}
