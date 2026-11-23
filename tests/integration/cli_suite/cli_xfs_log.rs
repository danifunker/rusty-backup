//! `rb-cli fsck` on an XFS volume left with a dirty log by a crash: the check reports it, a
//! repair replays it as a mount would, and `--discard-log` is only accepted with `--repair`.

use std::path::{Path, PathBuf};
use std::process::Command;

fn cli(args: &[&str]) -> std::process::Output {
    Command::new(PathBuf::from(env!("CARGO_BIN_EXE_rb-cli")))
        .args(args)
        .output()
        .expect("spawn rb-cli")
}

/// The v4 dirty-log fixture (made by a forced shutdown; see `fs-xfs`'s log_replay tests).
fn dirty_image(dir: &Path) -> PathBuf {
    let src =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/sgi/xfs_v4_dirty_log.img.zst");
    let img = dir.join("dirty.img");
    let mut out = std::fs::File::create(&img).unwrap();
    crate::decode_sparse(std::fs::File::open(src).unwrap(), &mut out);
    img
}

#[test]
fn fsck_replays_a_dirty_xfs_log() {
    let dir = tempfile::tempdir().unwrap();
    let img = dirty_image(dir.path());
    let img_s = img.to_str().unwrap();

    let check = cli(&["fsck", img_s]);
    assert!(!check.status.success());
    assert!(String::from_utf8_lossy(&check.stdout).contains("[LogDirty]"));

    let lone = cli(&["fsck", "--discard-log", img_s]);
    assert!(!lone.status.success(), "--discard-log needs --repair");

    // A log that can be replayed is never thrown away.
    let discard = cli(&["fsck", "--repair", "--discard-log", img_s]);
    assert!(!discard.status.success());
    assert!(String::from_utf8_lossy(&discard.stderr).contains("can be replayed"));
    assert!(String::from_utf8_lossy(&cli(&["fsck", img_s]).stdout).contains("[LogDirty]"));

    let repair = cli(&["fsck", "--repair", img_s]);
    let out = String::from_utf8_lossy(&repair.stdout);
    assert!(
        repair.status.success(),
        "{out}\n{}",
        String::from_utf8_lossy(&repair.stderr)
    );
    assert!(
        out.contains("log: replayed 101 committed transactions"),
        "{out}"
    );
    assert!(cli(&["fsck", img_s]).status.success());

    let got = dir.path().join("hard.txt");
    let get = cli(&["get", img_s, "/keep/hard.txt", got.to_str().unwrap()]);
    assert!(get.status.success());
    assert_eq!(std::fs::read(got).unwrap(), b"data\n");
}
