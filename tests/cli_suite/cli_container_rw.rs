//! Write verbs on sparse containers: a dynamic VHD, a sparse VMDK and a QCOW2
//! must accept `put` and hand the bytes back through `get`, because the read
//! side already decodes all three (R-043: every edit verb refused a dynamic
//! VHD with "Invalid MBR"). A fixed VHD is the control -- raw data with a
//! trailing footer, so the plain-file path was always right for it.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn cli_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_rb-cli"))
}

fn run(args: &[&str]) -> Output {
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

fn s(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

/// Convert a fresh FAT volume into `format`, then put / ls / get through it.
fn roundtrip_through(format: &str, out_ext: &str) {
    let dir = tempfile::tempdir().unwrap();
    let raw = dir.path().join("v.img");
    let conv_dir = dir.path().join(format);
    let payload = dir.path().join("payload.bin");
    let back = dir.path().join("back.bin");
    // Past one 2 MiB VHD block, so a write allocates a block the converter
    // never emitted (the FAT root sits in the first one).
    let bytes: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
    std::fs::write(&payload, &bytes).unwrap();

    run(&["new", "volume", "fat", "--size", "16M", &s(&raw)]);
    run(&["convert", &s(&raw), &s(&conv_dir), "--format", format]);
    let image = conv_dir.join(format!("v.{out_ext}"));
    assert!(image.exists(), "convert produced no {}", image.display());

    run(&["put", &s(&image), &s(&payload), "/P.BIN"]);
    let ls = run(&["ls", &s(&image), "/"]);
    assert!(
        String::from_utf8_lossy(&ls.stdout).contains("P.BIN"),
        "{format}: put did not land in the listing"
    );
    run(&["get", &s(&image), "/P.BIN", &s(&back)]);
    assert_eq!(
        std::fs::read(&back).unwrap(),
        bytes,
        "{format}: payload changed"
    );
    run(&["fsck", &s(&image)]);
}

#[test]
fn dynamic_vhd_accepts_writes() {
    roundtrip_through("vhd-dynamic", "vhd");
}

#[test]
fn fixed_vhd_still_accepts_writes() {
    roundtrip_through("vhd", "vhd");
}

#[test]
fn sparse_vmdk_accepts_writes() {
    roundtrip_through("vmdk-sparse", "vmdk");
}

#[test]
fn qcow2_accepts_writes() {
    roundtrip_through("qcow2", "qcow2");
}
