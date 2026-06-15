//! `rb-cli` parity tests for OS-9 / NitrOS-9 RBF (CoCo2 / CoCo3 cores, raw
//! `.dsk`).
//!
//! Fixture `test_coco_os9l2.dsk.zst` is the canonical 35-track (161280-byte)
//! "Color Computer 3  OS-9 LII" system disk from the NitrOS-9 toolshed
//! (`disks/os9l2_d1`). It is a real, hierarchical OS-9 volume: 57 files
//! across the root plus the `CMDS/` and `SYS/` subdirectories.
//!
//! The read path was cross-validated this session against an independent
//! clean-room Python RBF reader — rb-cli extracts every one of the 57 files
//! byte-exact. The write path was validated the same way: after `put`/`rm`,
//! all pre-existing files remain byte-exact and deletes reclaim space
//! exactly. These tests pin the rb-cli surface (inspect / ls / get / put /
//! rm) end-to-end, including subdirectory traversal.

use std::io::Read;
use std::path::{Path, PathBuf};
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

fn sha256_hex(b: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(b);
    h.finalize().iter().map(|x| format!("{x:02x}")).collect()
}

fn fixture_to(tmp: &Path) -> PathBuf {
    let img = tmp.join("disk.dsk");
    let compressed = std::fs::read("tests/fixtures/test_coco_os9l2.dsk.zst").expect("read fixture");
    let mut dec =
        zstd::stream::read::Decoder::new(std::io::Cursor::new(compressed)).expect("zstd decoder");
    let mut bytes = Vec::new();
    dec.read_to_end(&mut bytes).expect("decompress");
    std::fs::write(&img, &bytes).expect("write fixture out");
    img
}

#[test]
fn inspect_reports_os9() {
    let tmp = tempfile::tempdir().unwrap();
    let img = fixture_to(tmp.path());
    let stdout =
        String::from_utf8_lossy(&run(&["inspect", img.to_str().unwrap()]).stdout).into_owned();
    assert!(stdout.contains("OS-9"), "inspect missing OS-9:\n{stdout}");
}

#[test]
fn ls_root_and_subdir() {
    let tmp = tempfile::tempdir().unwrap();
    let img = fixture_to(tmp.path());
    let root = String::from_utf8_lossy(&run(&["ls", img.to_str().unwrap()]).stdout).into_owned();
    for must in ["OS9Boot", "CMDS", "SYS", "startup"] {
        assert!(root.contains(must), "ls root missing {must}:\n{root}");
    }
    // Descend into the CMDS directory.
    let cmds =
        String::from_utf8_lossy(&run(&["ls", img.to_str().unwrap(), "CMDS"]).stdout).into_owned();
    for must in ["copy", "dir", "format", "shell"] {
        assert!(cmds.contains(must), "ls CMDS missing {must}:\n{cmds}");
    }
}

#[test]
fn get_nested_file_byte_exact() {
    let tmp = tempfile::tempdir().unwrap();
    let img = fixture_to(tmp.path());

    // A file two levels deep.
    let dst = tmp.path().join("copy.out");
    run(&[
        "get",
        img.to_str().unwrap(),
        "CMDS/copy",
        dst.to_str().unwrap(),
    ]);
    let got = std::fs::read(&dst).unwrap();
    assert_eq!(got.len(), 743);
    assert_eq!(
        sha256_hex(&got),
        "d7fb17013f6efe4cb18f244e89b21c66705ec72e9bc955faa77f415d74207d2a"
    );

    // The root boot file.
    let boot = tmp.path().join("boot.out");
    run(&[
        "get",
        img.to_str().unwrap(),
        "OS9Boot",
        boot.to_str().unwrap(),
    ]);
    let got = std::fs::read(&boot).unwrap();
    assert_eq!(got.len(), 27107);
    assert_eq!(
        sha256_hex(&got),
        "1adfec2b68a492a582d648bf1ce568e1b9d9a46fb078a94f2bc7d26c8c7b6258"
    );
}

#[test]
fn put_get_rm_round_trip_preserves_originals() {
    let tmp = tempfile::tempdir().unwrap();
    let img = fixture_to(tmp.path());

    let payload: Vec<u8> = (0..3000).map(|i| (i * 31 % 256) as u8).collect();
    let host = tmp.path().join("payload.bin");
    std::fs::write(&host, &payload).unwrap();
    run(&[
        "put",
        img.to_str().unwrap(),
        host.to_str().unwrap(),
        "NEWFILE",
    ]);

    let listing = String::from_utf8_lossy(&run(&["ls", img.to_str().unwrap()]).stdout).into_owned();
    assert!(listing.contains("NEWFILE"), "put file missing:\n{listing}");

    let back = tmp.path().join("back.bin");
    run(&[
        "get",
        img.to_str().unwrap(),
        "NEWFILE",
        back.to_str().unwrap(),
    ]);
    assert_eq!(std::fs::read(&back).unwrap(), payload);

    run(&["rm", img.to_str().unwrap(), "NEWFILE"]);
    let after = String::from_utf8_lossy(&run(&["ls", img.to_str().unwrap()]).stdout).into_owned();
    assert!(!after.contains("NEWFILE"), "rm left file behind:\n{after}");

    // A pre-existing file is still byte-exact after the mutation cycle.
    let boot = tmp.path().join("boot2.out");
    run(&[
        "get",
        img.to_str().unwrap(),
        "OS9Boot",
        boot.to_str().unwrap(),
    ]);
    assert_eq!(
        sha256_hex(&std::fs::read(&boot).unwrap()),
        "1adfec2b68a492a582d648bf1ce568e1b9d9a46fb078a94f2bc7d26c8c7b6258",
        "OS9Boot corrupted by put/rm"
    );
}
