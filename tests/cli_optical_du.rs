//! `rb-cli optical du` — recursive both-fork disk usage on an optical disc.
//!
//! Drives the real `rb-cli` binary against a tiny committed hybrid Mac/PC disc
//! image (ISO 9660 + HFS+ sides). The point is the caveat that `du` couldn't
//! reach optical media: `optical du --filesystem hfs` must count the resource
//! fork of a classic-Mac app, matching what macOS wrote.
//!
//! Fixture: tests/fixtures/optical/hybrid_rsrc.iso.zst — built on macOS with
//!   mkdir -p src/Apps
//!   printf 'data fork bytes' > src/Apps/GameApp            # 15-byte data fork
//!   head -c 5000 /dev/zero | tr '\0' R > src/Apps/GameApp/..namedfork/rsrc
//!   SetFile -t APPL -c PLAY src/Apps/GameApp
//!   hdiutil makehybrid -hfs -iso -joliet -default-volume-name RETRO -o disc.iso src
#![cfg(feature = "optical")]

use std::io::{Cursor, Read};
use std::path::PathBuf;
use std::process::Command;

fn cli_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_rb-cli"))
}

/// Decompress the committed fixture to a tempfile and return (path, guard).
fn fixture_disc() -> (PathBuf, tempfile::TempDir) {
    let compressed =
        std::fs::read("tests/fixtures/optical/hybrid_rsrc.iso.zst").expect("read fixture");
    let mut dec = zstd::stream::read::Decoder::new(Cursor::new(compressed)).unwrap();
    let mut iso = Vec::new();
    dec.read_to_end(&mut iso).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("disc.iso");
    std::fs::write(&path, &iso).unwrap();
    (path, dir)
}

fn run(args: &[&str]) -> std::process::Output {
    let out = Command::new(cli_bin()).args(args).output().expect("spawn");
    assert!(
        out.status.success(),
        "command {args:?} failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    out
}

#[test]
fn optical_du_hfs_side_counts_resource_fork() {
    let (disc, _guard) = fixture_disc();
    let disc = disc.to_str().unwrap();

    // The Apple HFS side of the hybrid carries the resource fork.
    let out = run(&[
        "optical",
        "du",
        disc,
        "/Apps",
        "--filesystem",
        "hfs",
        "--json",
    ]);
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let apps = &v["result"]["paths"][0];
    assert_eq!(apps["path"], "/Apps");
    assert_eq!(apps["found"], true);
    // /Apps holds one file, GameApp: 15-byte data fork + 5000-byte resource
    // fork — exactly what macOS wrote.
    assert_eq!(apps["data_bytes"], 15);
    assert_eq!(apps["rsrc_bytes"], 5000);
    assert_eq!(apps["apparent_bytes"], 5015);
    assert_eq!(apps["files"], 1);
    // Optical discs report a real allocation/logical block, so alloc modeling
    // is present (each fork rounded up to it).
    assert!(v["result"]["allocation_unit"].as_u64().unwrap() > 0);
    assert!(apps["alloc_bytes"].as_u64().unwrap() >= 5015);
}

#[test]
fn optical_du_missing_path_is_clean() {
    let (disc, _guard) = fixture_disc();
    let disc = disc.to_str().unwrap();
    let out = run(&[
        "optical",
        "du",
        disc,
        "/Nope",
        "--filesystem",
        "hfs",
        "--json",
    ]);
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let miss = &v["result"]["paths"][0];
    assert_eq!(miss["path"], "/Nope");
    assert_eq!(miss["found"], false);
    assert!(miss.get("rsrc_bytes").is_none());
}
