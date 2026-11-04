//! End-to-end tests for the `rb-cli cp` verb (image-to-image copy).
//!
//! Drives the real binary against scratch FAT images: single-file copy,
//! whole-volume recursive consolidation, the free-space preflight, and the
//! same-image guard. The engine-level fidelity matrix lives in
//! `tests/cp_engine.rs`; this is the grammar / wiring regression net.

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

fn run_expect_fail(args: &[&str]) -> Output {
    let out = Command::new(cli_bin())
        .args(args)
        .output()
        .expect("spawn rb-cli");
    assert!(
        !out.status.success(),
        "expected {args:?} to fail but it succeeded"
    );
    out
}

fn s(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

#[test]
fn cp_single_file_between_images() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.img");
    let dst = dir.path().join("dst.img");
    let host = dir.path().join("hello.txt");
    std::fs::write(&host, b"hello world\n").unwrap();

    run(&[
        "new",
        "volume",
        "fat",
        "--size",
        "4M",
        "--name",
        "SRC",
        &s(&src),
    ]);
    run(&[
        "new",
        "volume",
        "fat",
        "--size",
        "4M",
        "--name",
        "DST",
        &s(&dst),
    ]);
    run(&["put", &s(&src), &s(&host), "/HELLO.TXT"]);

    run(&["cp", &s(&src), "/HELLO.TXT", &s(&dst), "/HELLO.TXT"]);

    let ls = run(&["ls", &s(&dst), "/"]);
    let listing = String::from_utf8_lossy(&ls.stdout);
    assert!(
        listing.contains("HELLO.TXT"),
        "dst should contain HELLO.TXT:\n{listing}"
    );
}

#[test]
fn cp_recursive_whole_volume_into_subdir() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.img");
    let dst = dir.path().join("dst.img");
    let host = dir.path().join("a.bin");
    std::fs::write(&host, vec![7u8; 2048]).unwrap();

    run(&[
        "new",
        "volume",
        "fat",
        "--size",
        "4M",
        "--name",
        "SRC",
        &s(&src),
    ]);
    run(&[
        "new",
        "volume",
        "fat",
        "--size",
        "4M",
        "--name",
        "DST",
        &s(&dst),
    ]);
    run(&["mkdir", &s(&src), "/SUB"]);
    run(&["put", &s(&src), &s(&host), "/SUB/A.BIN"]);

    run(&[
        "cp",
        &s(&src),
        "/",
        &s(&dst),
        "/FROMSRC/",
        "-r",
        "--parents",
    ]);

    let ls = run(&["ls", &s(&dst), "/FROMSRC/SUB"]);
    let listing = String::from_utf8_lossy(&ls.stdout);
    assert!(
        listing.contains("A.BIN"),
        "tree should be rebuilt under /FROMSRC:\n{listing}"
    );
}

#[test]
fn cp_preflight_refuses_when_too_big() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.img");
    let dst = dir.path().join("dst.img");
    let host = dir.path().join("big.bin");
    std::fs::write(&host, vec![0u8; 3_000_000]).unwrap();

    run(&[
        "new",
        "volume",
        "fat",
        "--size",
        "8M",
        "--name",
        "SRC",
        &s(&src),
    ]);
    run(&[
        "new",
        "volume",
        "fat",
        "--size",
        "2M",
        "--name",
        "DST",
        &s(&dst),
    ]);
    run(&["put", &s(&src), &s(&host), "/BIG.BIN"]);

    let out = run_expect_fail(&["cp", &s(&src), "/BIG.BIN", &s(&dst), "/BIG.BIN"]);
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("copy needs"),
        "expected preflight message:\n{err}"
    );
    assert!(
        err.contains("resize"),
        "should suggest growing the volume:\n{err}"
    );
}

#[test]
fn cp_rejects_same_image() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.img");
    let host = dir.path().join("x.txt");
    std::fs::write(&host, b"x").unwrap();
    run(&[
        "new",
        "volume",
        "fat",
        "--size",
        "4M",
        "--name",
        "SRC",
        &s(&src),
    ]);
    run(&["put", &s(&src), &s(&host), "/X.TXT"]);

    let out = run_expect_fail(&["cp", &s(&src), "/X.TXT", &s(&src), "/Y.TXT"]);
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("same image"),
        "expected same-image guard:\n{err}"
    );
}

// -- cp -p / --preserve-times (PR #97) ----------------------------------------

mod preserve_times {
    use super::{run, s};
    use disk_core::entry::FileEntry;
    use disk_core::filesystem::{
        CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem,
    };
    use fs_mac::hfs::{create_blank_hfs, HfsFilesystem};
    use std::fs::{File, OpenOptions};
    use std::path::Path;

    /// Distinct early-1990s catalog words (seconds since 1904) that can't be mistaken for "now".
    const FILE_DATES: (u32, u32, u32) = (2_750_000_000, 2_760_000_000, 2_770_000_000);
    const DIR_DATES: (u32, u32, u32) = (2_740_000_000, 2_745_000_000, 0);
    const MERGED_DIR_DATES: (u32, u32, u32) = (2_700_000_000, 2_710_000_000, 2_720_000_000);

    fn open_rw(path: &Path) -> HfsFilesystem<File> {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        HfsFilesystem::open(f, 0).unwrap()
    }

    fn blank_hfs(path: &Path, name: &str) {
        std::fs::write(path, create_blank_hfs(4 * 1024 * 1024, 512, name).unwrap()).unwrap();
    }

    fn child(fs: &mut HfsFilesystem<File>, dir: &FileEntry, name: &str) -> FileEntry {
        let kids = fs.list_directory(dir).unwrap();
        kids.into_iter()
            .find(|e| e.name == name)
            .unwrap_or_else(|| panic!("{name} missing under {}", dir.path))
    }

    /// `/Games/Game` on a fresh HFS image, both stamped with known catalog dates.
    fn source_image(path: &Path) {
        blank_hfs(path, "Src");
        let mut fs = open_rw(path);
        let root = fs.root().unwrap();
        let dir = fs
            .create_directory(&root, "Games", &CreateDirectoryOptions::default())
            .unwrap();
        let body = b"app";
        let file = fs
            .create_file(
                &dir,
                "Game",
                &mut &body[..],
                body.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();
        fs.set_dates(&file, FILE_DATES.0, FILE_DATES.1, FILE_DATES.2)
            .unwrap();
        fs.set_dates(&dir, DIR_DATES.0, DIR_DATES.1, DIR_DATES.2)
            .unwrap();
        fs.sync_metadata().unwrap();
    }

    /// The catalog dates of `/Games` and `/Games/Game` on `path`.
    fn copied_dates(path: &Path) -> (Option<(u32, u32, u32)>, Option<(u32, u32, u32)>) {
        let mut fs = open_rw(path);
        let root = fs.root().unwrap();
        let dir = child(&mut fs, &root, "Games");
        let file = child(&mut fs, &dir, "Game");
        (dir.mac_dates, file.mac_dates)
    }

    #[test]
    fn cp_p_keeps_raw_hfs_dates_and_plain_cp_does_not() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src.hfv");
        let kept = tmp.path().join("kept.hfv");
        let plain = tmp.path().join("plain.hfv");
        source_image(&src);
        blank_hfs(&kept, "Kept");
        blank_hfs(&plain, "Plain");

        run(&["cp", "-p", "-r", &s(&src), "/Games", &s(&kept), "/"]);
        run(&["cp", "-r", &s(&src), "/Games", &s(&plain), "/"]);

        let (dir, file) = copied_dates(&kept);
        assert_eq!(
            file,
            Some(FILE_DATES),
            "cp -p keeps the file's create/modify/backup words"
        );
        assert_eq!(
            dir,
            Some(DIR_DATES),
            "cp -p keeps the folder's dates, stamped after its contents"
        );

        let (dir, file) = copied_dates(&plain);
        assert_ne!(
            file,
            Some(FILE_DATES),
            "plain cp stamps the time of the copy"
        );
        assert_ne!(dir, Some(DIR_DATES), "plain cp stamps the time of the copy");
    }

    #[test]
    fn cp_p_leaves_a_folder_it_merges_into_alone() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src.hfv");
        let dst = tmp.path().join("dst.hfv");
        source_image(&src);
        blank_hfs(&dst, "Dst");
        {
            let mut fs = open_rw(&dst);
            let root = fs.root().unwrap();
            let games = fs
                .create_directory(&root, "Games", &CreateDirectoryOptions::default())
                .unwrap();
            let (c, m, b) = MERGED_DIR_DATES;
            fs.set_dates(&games, c, m, b).unwrap();
            fs.sync_metadata().unwrap();
        }

        run(&["cp", "-p", "-r", &s(&src), "/Games", &s(&dst), "/"]);

        let (dir, file) = copied_dates(&dst);
        assert_eq!(
            dir,
            Some(MERGED_DIR_DATES),
            "a folder cp did not create keeps its own dates"
        );
        assert_eq!(
            file,
            Some(FILE_DATES),
            "files copied into it still keep theirs"
        );
    }

    #[test]
    #[ignore = "R-077: HFS reads as local time, FAT as UTC, so the copy shifts by the UTC offset"]
    fn cp_p_from_hfs_to_fat_keeps_the_modification_date() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src.hfv");
        let dst = tmp.path().join("dst.img");
        source_image(&src);
        run(&[
            "new",
            "volume",
            "fat",
            "--size",
            "4M",
            "--name",
            "DST",
            &s(&dst),
        ]);

        run(&["cp", "-p", &s(&src), "/Games/Game", &s(&dst), "/GAME"]);

        // Both sides store local wall-clock time, so `ls -t` prints the same minute for each.
        let modified = |img: &Path, dir: &str, name: &str| -> String {
            let out = run(&["ls", &s(img), dir, "--format", "json"]);
            let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
            let rows = v["result"].as_array().unwrap();
            let row = rows
                .iter()
                .find(|r| {
                    r["name"]
                        .as_str()
                        .is_some_and(|n| n.eq_ignore_ascii_case(name))
                })
                .unwrap_or_else(|| panic!("{name} missing in {}", img.display()));
            row["modified"].as_str().unwrap().chars().take(16).collect()
        };
        assert_eq!(
            modified(&dst, "/", "GAME"),
            modified(&src, "/Games", "Game"),
            "cp -p onto FAT keeps the source's modification date"
        );
    }
}
