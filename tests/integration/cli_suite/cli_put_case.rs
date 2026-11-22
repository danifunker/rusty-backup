//! `put --force` onto a case-insensitive volume: FAT matches `readme.txt`
//! to `README.TXT`, so the conflict check has to as well or the replace fails
//! with "already exists" (X3 in the 2026-09-01 audit).

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn cli_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_rb-cli"))
}

fn run(args: &[&str]) -> Output {
    Command::new(cli_bin())
        .args(args)
        .output()
        .expect("spawn rb-cli")
}

fn ok(args: &[&str]) -> String {
    let out = run(args);
    assert!(
        out.status.success(),
        "command {args:?} failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn s(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

#[test]
fn put_force_replaces_a_file_that_differs_only_in_case_on_fat() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("v.img");
    let first = dir.path().join("first.txt");
    let second = dir.path().join("second.txt");
    let back = dir.path().join("back.txt");
    std::fs::write(&first, b"first").unwrap();
    std::fs::write(&second, b"second, longer").unwrap();

    ok(&["new", "volume", "fat", "--size", "4M", &s(&img)]);
    ok(&["put", &s(&img), &s(&first), "/README.TXT"]);

    // Without --force the case-different name is still a conflict.
    let refused = run(&["put", &s(&img), &s(&second), "/readme.txt"]);
    assert!(!refused.status.success(), "a duplicate must be refused");

    ok(&["put", "--force", &s(&img), &s(&second), "/readme.txt"]);
    let ls = ok(&["ls", &s(&img), "/"]);
    assert_eq!(
        ls.lines()
            .filter(|l| l.to_ascii_uppercase().contains("README.TXT"))
            .count(),
        1,
        "one entry after the replace:\n{ls}"
    );
    ok(&["get", &s(&img), "/README.TXT", &s(&back)]);
    assert_eq!(std::fs::read(&back).unwrap(), b"second, longer");
}
