//! Two Ghost backups of the same disk (FAT32, then FAT16 and FAT32 in an extended partition):
//! one sector-by-sector, one file-aware. Both must list and read the same files.

use std::path::{Path, PathBuf};
use std::process::Command;

fn cli(args: &[&str]) -> std::process::Output {
    Command::new(PathBuf::from(env!("CARGO_BIN_EXE_rb-cli")))
        .args(args)
        .output()
        .expect("spawn rb-cli")
}

fn unpack(dir: &Path, fixture: &str) -> String {
    let img = dir.join(fixture.trim_end_matches(".zst"));
    let src = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(fixture);
    let mut out = std::fs::File::create(&img).unwrap();
    zstd::stream::copy_decode(std::fs::File::open(src).unwrap(), &mut out).unwrap();
    img.to_str().unwrap().to_string()
}

fn ls(img: &str, part: u32) -> String {
    let out = cli(&["ls", &format!("{img}@{part}"), "/"]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn read(dir: &Path, img: &str, part: u32, path: &str) -> Vec<u8> {
    let dst = dir.join(format!("p{part}.out"));
    let out = cli(&["get", &format!("{img}@{part}"), path, dst.to_str().unwrap()]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    std::fs::read(dst).unwrap()
}

fn check_disk(fixture: &str) {
    let dir = tempfile::tempdir().unwrap();
    let img = unpack(dir.path(), fixture);
    let root = ls(&img, 1);
    for name in ["COMMAND.COM", "EDIT.COM", "IO.SYS", "MSDOS.SYS", "TEST.TXT"] {
        assert!(root.contains(name), "{fixture} @1 lacks {name}:\n{root}");
    }
    // Each volume's label is a label, not an empty file.
    assert!(
        !root.contains("BOOT"),
        "{fixture} @1 lists its label:\n{root}"
    );
    assert!(
        !ls(&img, 3).contains("FAT16"),
        "{fixture} @3 lists its label"
    );
    assert!(
        !ls(&img, 4).contains("FAT32EX"),
        "{fixture} @4 lists its label"
    );
    assert_eq!(read(dir.path(), &img, 1, "/COMMAND.COM").len(), 93890);
    assert_eq!(
        read(dir.path(), &img, 3, "/TEST.TXT"),
        b"THIS IS ANOTHER TEST ON a FAT16 DRIVE\r\n"
    );
    assert!(read(dir.path(), &img, 4, "/TEST.TXT")
        .starts_with(b"This is yet another test on a FAT32 drive in extended partition"));
}

#[test]
fn a_sector_mode_multi_partition_ghost_reads_every_partition() {
    check_disk("test_gho_11_sector.gho.zst");
}

#[test]
fn a_file_aware_multi_partition_ghost_reads_every_partition() {
    check_disk("test_gho_75_none.gho.zst");
}
