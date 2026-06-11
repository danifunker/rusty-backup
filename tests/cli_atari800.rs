//! `rb-cli` parity tests for Atari DOS 2 (8-bit Atari800 core, `.atr`).
//!
//! Fixture `test_atari_dos2.atr.zst` is a single-density DOS 2.0S image
//! holding two known files:
//!   README.TXT (40 bytes)
//!   DATA.BIN   (2600 bytes, 21 sectors — multi-sector chain)
//!
//! The on-disk format and the read/write paths were cross-validated this
//! session against the atrcopy-bundled real DOS 2.0S system disk (DOS.SYS /
//! DUP.SYS / AUTORUN.SYS read byte-exact) and an independent clean-room
//! Python reader; these tests pin the rb-cli surface
//! (inspect / ls / get / put / rm) end-to-end through the ATR container
//! edit round-trip (decode -> edit -> re-wrap header).

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
    let img = tmp.join("disk.atr");
    let compressed = std::fs::read("tests/fixtures/test_atari_dos2.atr.zst").expect("read fixture");
    let mut dec =
        zstd::stream::read::Decoder::new(std::io::Cursor::new(compressed)).expect("zstd decoder");
    let mut bytes = Vec::new();
    dec.read_to_end(&mut bytes).expect("decompress");
    std::fs::write(&img, &bytes).expect("write fixture out");
    img
}

#[test]
fn inspect_reports_atari_dos() {
    let tmp = tempfile::tempdir().unwrap();
    let img = fixture_to(tmp.path());
    let stdout =
        String::from_utf8_lossy(&run(&["inspect", img.to_str().unwrap()]).stdout).into_owned();
    assert!(
        stdout.contains("Atari DOS"),
        "inspect missing Atari DOS:\n{stdout}"
    );
}

#[test]
fn ls_finds_seeded_files() {
    let tmp = tempfile::tempdir().unwrap();
    let img = fixture_to(tmp.path());
    let stdout = String::from_utf8_lossy(&run(&["ls", img.to_str().unwrap()]).stdout).into_owned();
    assert!(
        stdout.contains("README.TXT"),
        "ls missing README.TXT:\n{stdout}"
    );
    assert!(
        stdout.contains("DATA.BIN"),
        "ls missing DATA.BIN:\n{stdout}"
    );
}

#[test]
fn get_extracts_multisector_file_byte_exact() {
    let tmp = tempfile::tempdir().unwrap();
    let img = fixture_to(tmp.path());
    let dst = tmp.path().join("data.out");
    run(&[
        "get",
        img.to_str().unwrap(),
        "DATA.BIN",
        dst.to_str().unwrap(),
    ]);
    let got = std::fs::read(&dst).unwrap();
    assert_eq!(got.len(), 2600);
    assert_eq!(
        sha256_hex(&got),
        "a27585bf1490cd28857e228b496343284877358d245fa6df222ac41ff5362e17"
    );
}

#[test]
fn put_get_rm_round_trip_through_atr_container() {
    let tmp = tempfile::tempdir().unwrap();
    let img = fixture_to(tmp.path());

    let payload: Vec<u8> = (0..900).map(|i| (i * 17 % 256) as u8).collect();
    let host = tmp.path().join("payload.bin");
    std::fs::write(&host, &payload).unwrap();
    run(&[
        "put",
        img.to_str().unwrap(),
        host.to_str().unwrap(),
        "NEW.BIN",
    ]);

    let listing = String::from_utf8_lossy(&run(&["ls", img.to_str().unwrap()]).stdout).into_owned();
    assert!(listing.contains("NEW.BIN"), "put file missing:\n{listing}");

    let back = tmp.path().join("back.bin");
    run(&[
        "get",
        img.to_str().unwrap(),
        "NEW.BIN",
        back.to_str().unwrap(),
    ]);
    assert_eq!(std::fs::read(&back).unwrap(), payload);

    run(&["rm", img.to_str().unwrap(), "NEW.BIN"]);
    let after = String::from_utf8_lossy(&run(&["ls", img.to_str().unwrap()]).stdout).into_owned();
    assert!(!after.contains("NEW.BIN"), "rm left file behind:\n{after}");
    assert!(
        after.contains("README.TXT"),
        "rm clobbered README.TXT:\n{after}"
    );
}
