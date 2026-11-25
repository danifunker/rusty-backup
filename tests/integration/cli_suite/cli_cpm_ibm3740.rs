//! `--fs-type cpm:ibm_3740` on a real 8" IBM 3740 disk, CP/M Users Group volume 54 (public
//! domain; agn453/CPM-UG-Disks, The Unlicense), converted from ImageDisk to a raw image. Its
//! records are skewed by 6, so the directory and files only read in order through the skew.

use std::path::{Path, PathBuf};
use std::process::Command;

fn cli(args: &[&str]) -> std::process::Output {
    Command::new(PathBuf::from(env!("CARGO_BIN_EXE_rb-cli")))
        .args(args)
        .output()
        .expect("spawn rb-cli")
}

#[test]
fn a_skewed_ibm_3740_disk_reads_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("cpmug054.img");
    let src =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/cpm_ibm3740_cpmug054.img.zst");
    let mut out = std::fs::File::create(&img).unwrap();
    zstd::stream::copy_decode(std::fs::File::open(src).unwrap(), &mut out).unwrap();
    let img_s = img.to_str().unwrap();

    let ls = cli(&["ls", "--fs-type", "cpm:ibm_3740", img_s, "/"]);
    assert!(
        ls.status.success(),
        "{}",
        String::from_utf8_lossy(&ls.stderr)
    );
    let listing = String::from_utf8_lossy(&ls.stdout);
    assert_eq!(
        listing.lines().filter(|l| l.starts_with("FILE")).count(),
        61,
        "{listing}"
    );
    assert!(listing.contains("-CP/M-UG.054"));

    let got = dir.path().join("BUG.BAS");
    let get = cli(&[
        "get",
        "--fs-type",
        "cpm:ibm_3740",
        img_s,
        "/BUG.BAS",
        got.to_str().unwrap(),
    ]);
    assert!(get.status.success());
    let text = std::fs::read(&got).unwrap();
    assert!(text.starts_with(b"5   REM *** CONVERTED TO RSTS/E BY DAVID AHL"));
    assert!(String::from_utf8_lossy(&text).contains("20  REM HARRISON JR. SR. HIGH SCHOOL"));
}
