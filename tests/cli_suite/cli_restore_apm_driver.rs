//! A Mac needs the driver partition to start up, and it sits outside the data partitions: every
//! backup layout must bring it back, and a backup that lost it gets the bundled one.

use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::Command;

fn run_raw(args: &[&str]) -> (bool, String) {
    let out = Command::new(PathBuf::from(env!("CARGO_BIN_EXE_rb-cli")))
        .args(args)
        .output()
        .expect("spawn rb-cli");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.success(), text)
}

fn run(args: &[&str]) -> String {
    let (ok, text) = run_raw(args);
    assert!(ok, "command {args:?} failed:\n{text}");
    text
}

/// The bytes of the disk's Apple_Driver43 partition.
fn driver_bytes(path: &Path) -> Vec<u8> {
    let mut f = std::fs::File::open(path).unwrap();
    let apm = part_tables::apm::Apm::parse(&mut f).unwrap();
    let e = apm
        .entries
        .iter()
        .find(|e| e.partition_type == "Apple_Driver43")
        .expect("a driver entry");
    let mut buf = vec![0u8; e.block_count as usize * 512];
    f.seek(SeekFrom::Start(e.start_block as u64 * 512)).unwrap();
    f.read_exact(&mut buf).unwrap();
    buf
}

/// A 32 MiB APM disk with a driver, backed up as `format`; returns the disk and the backup.
fn mac_disk(dir: &Path, format: &str) -> (PathBuf, String) {
    let img = dir.join("a.img");
    let s = img.to_str().unwrap();
    run(&[
        "build",
        s,
        "--size",
        "32M",
        "--table",
        "apm",
        "--partition",
        "rest",
        "--format",
        "1=hfs:KEEP",
    ]);
    let bk = dir.join("bk");
    run(&[
        "backup",
        s,
        bk.to_str().unwrap(),
        "--name",
        "job",
        "--format",
        format,
    ]);
    (img, bk.join("job").to_str().unwrap().to_string())
}

fn assert_driver_survives(format: &str, extra: &[&str]) {
    let dir = tempfile::tempdir().unwrap();
    let (img, backup) = mac_disk(dir.path(), format);
    let want = driver_bytes(&img);
    assert!(want.iter().any(|&b| b != 0), "the fixture has a driver");
    let target = dir.path().join("r.img");
    let mut args = vec!["restore", backup.as_str(), target.to_str().unwrap()];
    args.extend_from_slice(extra);
    let text = run(&args);
    assert!(!text.contains("did not keep"), "{text}");
    assert_eq!(driver_bytes(&target), want, "{format}: {text}");
}

#[test]
fn a_zstd_backup_keeps_the_driver() {
    assert_driver_survives("zstd", &[]);
}

#[test]
fn a_raw_backup_keeps_the_driver() {
    assert_driver_survives("raw", &[]);
}

#[test]
fn a_grown_restore_keeps_the_driver() {
    assert_driver_survives("zstd", &["--size", "fill", "--target-size", "134217728"]);
}

#[cfg(feature = "chd")]
#[test]
fn a_chd_backup_keeps_the_driver_at_any_size() {
    assert_driver_survives("chd", &[]);
    assert_driver_survives("chd", &["--size", "fill", "--target-size", "134217728"]);
}

#[test]
fn a_backup_that_lost_its_driver_gets_the_bundled_one_unless_told_not_to() {
    let dir = tempfile::tempdir().unwrap();
    let (_, backup) = mac_disk(dir.path(), "zstd");
    std::fs::remove_file(Path::new(&backup).join("apm-extras.bin")).unwrap();

    let with = dir.path().join("with.img");
    let text = run(&["restore", &backup, with.to_str().unwrap()]);
    assert!(text.contains("bundled Apple SCSI driver"), "{text}");
    assert!(driver_bytes(&with).iter().any(|&b| b != 0), "{text}");

    let without = dir.path().join("without.img");
    let text = run(&[
        "restore",
        &backup,
        without.to_str().unwrap(),
        "--no-mac-driver",
    ]);
    assert!(text.contains("cannot start up"), "{text}");
    assert!(driver_bytes(&without).iter().all(|&b| b == 0), "{text}");
}
