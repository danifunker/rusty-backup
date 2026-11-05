//! `rb-cli optical extract --to --on-error skip|stop` on the Rock Ridge fixture, with folders
//! in the destination where two of the disc's files would land so writing them fails.

use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

fn cli_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_rb-cli"))
}

fn rock_ridge(dir: &Path) -> PathBuf {
    let packed = std::fs::read("tests/fixtures/optical/rock_ridge.iso.zst").unwrap();
    let mut iso = Vec::new();
    zstd::stream::read::Decoder::new(Cursor::new(packed))
        .unwrap()
        .read_to_end(&mut iso)
        .unwrap();
    let path = dir.join("rr.iso");
    std::fs::write(&path, iso).unwrap();
    path
}

/// Run `optical extract` into `out`; `colon:name.txt` lands on `colon_name.txt` too, so 3 fail.
fn extract(disc: &Path, out: &Path, on_error: Option<&str>) -> (bool, String) {
    for blocker in ["empty.dat", "colon_name.txt"] {
        std::fs::create_dir_all(out.join(blocker)).unwrap();
    }
    let mut cmd = Command::new(cli_bin());
    cmd.args(["optical", "extract"])
        .arg(disc)
        .arg("--to")
        .arg(out)
        .stdin(Stdio::null());
    if let Some(mode) = on_error {
        cmd.args(["--on-error", mode]);
    }
    let o = cmd.output().expect("spawn rb-cli");
    (
        o.status.success(),
        String::from_utf8_lossy(&o.stderr).into_owned(),
    )
}

#[test]
fn on_error_skip_carries_on_and_lists_the_skipped_entries() {
    let dir = tempfile::tempdir().unwrap();
    let disc = rock_ridge(dir.path());
    let out = dir.path().join("out");
    let (ok, err) = extract(&disc, &out, Some("skip"));
    assert!(ok, "{err}");
    assert!(err.contains("warning: skipping /empty.dat:"), "{err}");
    assert!(err.contains("skipped 3 entry/entries:"), "{err}");
    assert!(err.contains("  /colon_name.txt: "), "{err}");
    assert!(out.join("docs").is_dir(), "the rest was extracted");
}

#[test]
fn on_error_stop_ends_at_the_first_failure_and_is_the_default_without_a_terminal() {
    let dir = tempfile::tempdir().unwrap();
    let disc = rock_ridge(dir.path());
    for mode in [Some("stop"), None] {
        let out = dir.path().join(format!("out-{mode:?}"));
        let (ok, err) = extract(&disc, &out, mode);
        assert!(!ok, "{mode:?}: {err}");
        assert!(err.contains("extraction stopped at /"), "{mode:?}: {err}");
        assert!(!err.contains("warning: skipping"), "{mode:?}: {err}");
    }
}

#[test]
fn on_error_ask_reads_the_answers_from_stdin() {
    use std::io::Write as _;
    let dir = tempfile::tempdir().unwrap();
    let disc = rock_ridge(dir.path());
    let out = dir.path().join("out");
    for blocker in ["empty.dat", "colon_name.txt"] {
        std::fs::create_dir_all(out.join(blocker)).unwrap();
    }
    let mut child = Command::new(cli_bin())
        .args(["optical", "extract"])
        .arg(&disc)
        .arg("--to")
        .arg(&out)
        .args(["--on-error", "ask"])
        .stdin(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    // Skip the first failure, a key that means nothing asks again, then stop at the second.
    child.stdin.take().unwrap().write_all(b"s\nx\nt\n").unwrap();
    let o = child.wait_with_output().unwrap();
    let err = String::from_utf8_lossy(&o.stderr);
    assert!(!o.status.success(), "{err}");
    assert_eq!(err.matches("Could not extract /").count(), 2, "{err}");
    assert_eq!(
        err.matches("[s]kip, [c]ontinue with all errors, s[t]op? ")
            .count(),
        3,
        "{err}"
    );
    assert!(err.contains("skipped 1 entry/entries:"), "{err}");
    assert!(err.contains("extraction stopped at /"), "{err}");
}
