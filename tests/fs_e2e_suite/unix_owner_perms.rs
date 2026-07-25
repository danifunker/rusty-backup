//! `set_permissions` / `set_owner` across every Unix filesystem we can edit.
//!
//! Both hooks are what the GUI's file-metadata editor stages (`SetPermissions`
//! / `SetOwner` in the edit queue) and what a tar import falls back to for an
//! entry it can't stamp at creation time. They used to be implemented on
//! SquashFS alone — plus `set_permissions` on ext — so on EFS, UFS, Minix,
//! JFS and XFS the editor's Owner/Mode fields rendered (they key off the
//! entry carrying a `mode`, which those all do) and then failed with
//! `Unsupported` at apply time.
//!
//! One table so a newly-editable Unix filesystem is a row, not a new test.

use std::io::Cursor;

use rusty_backup::fs::entry::FileEntry;
use rusty_backup::fs::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};

fn load_fixture(rel: &str) -> Vec<u8> {
    let path = format!("tests/fixtures/{rel}");
    let compressed = std::fs::read(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    let mut dec = zstd::stream::read::Decoder::new(Cursor::new(compressed))
        .unwrap_or_else(|e| panic!("zstd {path}: {e}"));
    let mut out = Vec::new();
    std::io::Read::read_to_end(&mut dec, &mut out)
        .unwrap_or_else(|e| panic!("inflate {path}: {e}"));
    out
}

/// How to get a writable image plus the partition offset to open it at.
enum Volume {
    /// Format a blank volume of this size in memory.
    Format(fn(u64) -> Vec<u8>, u64),
    /// Decompress a fixture image.
    Fixture(&'static str),
}

struct Case {
    fs: &'static str,
    volume: Volume,
    /// A file already in the image to re-stamp, or `None` to create one.
    /// Fixtures get an existing path because not every driver here can
    /// create files (XFS is v4-short-form only, JFS has no create at all).
    existing: Option<&'static str>,
    /// uid/gid to set — narrow-field filesystems get values that fit.
    ids: (u32, u32),
}

fn blank_efs(size: u64) -> Vec<u8> {
    rusty_backup::fs::efs::create_blank_efs(size, "OWN").expect("format EFS")
}
fn blank_ext2(size: u64) -> Vec<u8> {
    rusty_backup::fs::ext_format::create_blank_ext2(size, "OWN").expect("format ext2")
}
fn blank_minix3(size: u64) -> Vec<u8> {
    rusty_backup::fs::minix::create_blank_minix(size, rusty_backup::fs::minix::MinixVersion::V3)
        .expect("format minix3")
}
fn blank_hfsplus(size: u64) -> Vec<u8> {
    rusty_backup::fs::hfsplus::create_blank_hfsplus(size, 4096, "OWN", false)
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            fs: "efs",
            volume: Volume::Format(blank_efs, 8 * 1024 * 1024),
            existing: None,
            // EFS inodes hold 16-bit ids.
            ids: (1000, 100),
        },
        Case {
            fs: "ext2",
            volume: Volume::Format(blank_ext2, 16 * 1024 * 1024),
            existing: None,
            // Past 16 bits, to exercise ext's split lo/hi id halves.
            ids: (100_000, 100_001),
        },
        Case {
            fs: "minix3",
            volume: Volume::Format(blank_minix3, 4 * 1024 * 1024),
            existing: None,
            ids: (1000, 100),
        },
        Case {
            fs: "ufs1",
            volume: Volume::Fixture("test_ufs1.img.zst"),
            existing: Some("hello.txt"),
            ids: (100_000, 100_001),
        },
        Case {
            fs: "ufs2",
            volume: Volume::Fixture("test_ufs2.img.zst"),
            existing: Some("hello.txt"),
            ids: (100_000, 100_001),
        },
        Case {
            fs: "jfs",
            volume: Volume::Fixture("test_jfs.img.zst"),
            existing: Some("hello.txt"),
            ids: (100_000, 100_001),
        },
        Case {
            fs: "xfs-v5",
            volume: Volume::Fixture("sgi/xfs_v5_modern_small.img.zst"),
            existing: Some("hello.txt"),
            ids: (100_000, 100_001),
        },
        // HFS+ is where OS X keeps POSIX permissions (HFSPlusBSDInfo in
        // the catalog record), so it belongs in this table even though it
        // isn't a Unix filesystem by lineage.
        Case {
            fs: "hfsplus",
            volume: Volume::Format(blank_hfsplus, 16 * 1024 * 1024),
            existing: None,
            ids: (501, 20),
        },
    ]
}

/// The first regular file in the root, or the named one.
fn pick_target(fs: &mut dyn EditableFilesystem, want: Option<&str>) -> FileEntry {
    let root = Filesystem::root(fs.as_filesystem_mut()).expect("root");
    let entries = fs.list_directory(&root).expect("list root");
    match want {
        Some(name) => entries
            .iter()
            .find(|e| e.name.eq_ignore_ascii_case(name))
            .cloned()
            // Fixtures differ in what they contain; any regular file does.
            .or_else(|| entries.iter().find(|e| !e.is_directory()).cloned())
            .expect("a regular file in the fixture root"),
        None => {
            let payload = b"owner test".to_vec();
            fs.create_file(
                &root,
                "own.bin",
                &mut Cursor::new(payload.clone()),
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .expect("create the target file")
        }
    }
}

/// Open an image file read/write, the way every caller does.
fn open_rw(path: &std::path::Path) -> Box<dyn EditableFilesystem> {
    let f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
    rusty_backup::fs::open_editable_filesystem(f, 0, 0, None)
        .unwrap_or_else(|e| panic!("open fs {}: {e}", path.display()))
}

#[test]
fn set_permissions_and_owner_work_on_every_unix_filesystem() {
    let dir = tempfile::tempdir().expect("tempdir");
    for case in cases() {
        let img = match case.volume {
            Volume::Format(f, size) => f(size),
            Volume::Fixture(rel) => load_fixture(rel),
        };
        let path = dir.path().join(format!("{}.img", case.fs));
        std::fs::write(&path, &img).expect("write image");

        let mut fs = open_rw(&path);
        // Fixtures aren't guaranteed pristine, so the bar is "the edit
        // introduced no new complaints" rather than "fsck is silent".
        let errors_before = fsck_errors(&mut *fs);
        let target = pick_target(&mut *fs, case.existing);
        let (uid, gid) = case.ids;

        fs.set_permissions(&target, 0o741)
            .unwrap_or_else(|e| panic!("{}: set_permissions: {e}", case.fs));
        fs.set_owner(&target, uid, gid)
            .unwrap_or_else(|e| panic!("{}: set_owner: {e}", case.fs));
        fs.sync_metadata()
            .unwrap_or_else(|e| panic!("{}: sync: {e}", case.fs));
        drop(fs);

        // Re-open from disk: an in-memory struct agreeing with itself
        // proves nothing about what landed in the image.
        let mut fs = open_rw(&path);
        let root = Filesystem::root(fs.as_filesystem_mut()).expect("root");
        let after = fs
            .list_directory(&root)
            .expect("list root")
            .into_iter()
            .find(|e| e.name == target.name)
            .unwrap_or_else(|| panic!("{}: target vanished", case.fs));

        assert_eq!(
            after.mode.map(|m| m & 0o7777),
            Some(0o741),
            "{}: permission bits did not survive a reopen",
            case.fs
        );
        assert_eq!(
            (after.uid, after.gid),
            (Some(uid), Some(gid)),
            "{}: ownership did not survive a reopen",
            case.fs
        );
        // The file type must be intact — writing bare permission bits over
        // the mode word would leave a typeless inode.
        assert!(
            !after.is_directory(),
            "{}: entry lost its regular-file type",
            case.fs
        );
        // XFS v5 checksums its inode cores; a metadata edit that forgot to
        // re-stamp the CRC would still read back fine here and be rejected
        // by Linux, so lean on the verifier that does check.
        let errors_after = fsck_errors(&mut *fs);
        assert!(
            errors_after <= errors_before,
            "{}: fsck went from {errors_before} to {errors_after} errors after the edit",
            case.fs
        );
    }
}

/// fsck error count, or 0 for a filesystem with no verifier.
fn fsck_errors(fs: &mut dyn EditableFilesystem) -> usize {
    match fs.as_filesystem_mut().fsck() {
        Some(Ok(report)) => report.errors.len(),
        _ => 0,
    }
}

/// A narrow on-disk field must refuse an id it can't hold rather than
/// truncate it. uid 65536 wrapped into a 16-bit field is 0 — root.
#[test]
fn narrow_id_fields_refuse_rather_than_truncate() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("efs.img");
    std::fs::write(&path, blank_efs(8 * 1024 * 1024)).expect("write image");

    let mut fs = open_rw(&path);
    let target = pick_target(&mut *fs, None);

    let err = fs
        .set_owner(&target, 65_536, 0)
        .expect_err("EFS stores 16-bit ids; uid 65536 must be refused");
    assert!(
        err.to_string().contains("does not fit"),
        "unexpected error: {err}"
    );

    // And the inode is untouched — not wrapped to 0, which is root.
    fs.sync_metadata().expect("sync");
    drop(fs);
    let mut fs = open_rw(&path);
    let root = Filesystem::root(fs.as_filesystem_mut()).expect("root");
    let after = fs
        .list_directory(&root)
        .expect("list")
        .into_iter()
        .find(|e| e.name == target.name)
        .expect("target");
    assert_eq!(
        after.uid,
        Some(0),
        "a refused set_owner must change nothing"
    );
}
