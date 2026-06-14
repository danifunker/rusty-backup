//! Cross-image copy engine (`rusty_backup::fs::copy`) integration tests.
//!
//! Exercises the real copy path between freshly-formatted volumes on disk:
//! DOS-attribute preservation (FAT->FAT), resource-fork + type/creator
//! round-trip (HFS->HFS), fidelity loss with warnings (HFS->FAT), the
//! long-name truncation policy, the free-space preflight projection, and
//! recursive directory copy.

use std::fs::{File, OpenOptions};
use std::io::Cursor;
use std::path::Path;

use rusty_backup::fs::copy::{
    self, AttrPolicy, Capabilities, ConflictMode, CopyOptions, CopyStats, NamePolicy,
};
use rusty_backup::fs::entry::FileEntry;
use rusty_backup::fs::filesystem::{
    CreateFileOptions, EditableFilesystem, Filesystem, ResourceForkSource,
};
use rusty_backup::fs::{fat, hfs, open_editable_filesystem, open_filesystem};

const MB: u64 = 1024 * 1024;

fn write_blank_fat(path: &Path, size: u64, label: &str) {
    std::fs::write(path, fat::create_blank_fat(size, Some(label)).unwrap()).unwrap();
}

fn write_blank_hfs(path: &Path, size: u64, label: &str) {
    std::fs::write(
        path,
        hfs::create_blank_hfs_sized(size, 512, label, 0, 0).unwrap(),
    )
    .unwrap();
}

fn open_rw(path: &Path) -> Box<dyn EditableFilesystem> {
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    open_editable_filesystem(f, 0, 0, None).unwrap()
}

fn open_ro(path: &Path) -> Box<dyn Filesystem> {
    open_filesystem(File::open(path).unwrap(), 0, 0, None).unwrap()
}

fn find(fs: &mut dyn Filesystem, parent: &FileEntry, name: &str) -> Option<FileEntry> {
    fs.list_directory(parent)
        .unwrap()
        .into_iter()
        .find(|e| e.name == name)
}

fn opts(recursive: bool) -> CopyOptions {
    CopyOptions {
        recursive,
        conflict: ConflictMode::Error,
        names: NamePolicy::Truncate,
        attrs: AttrPolicy::Preserve,
        flatten: false,
    }
}

fn noop(_: &str) {}

/// Copy a single named entry from `src` root to `dst` root, returning stats.
fn copy_one(src_path: &Path, dst_path: &Path, name: &str, dst_name: &str) -> CopyStats {
    let mut s = open_ro(src_path);
    let mut d = open_rw(dst_path);
    let sroot = s.root().unwrap();
    let droot = d.root().unwrap();
    let entry = find(&mut *s, &sroot, name).expect("source entry");
    let mut stats = CopyStats::default();
    copy::copy_into(
        &mut *s,
        &entry,
        &mut *d,
        &droot,
        dst_name,
        &opts(false),
        &mut stats,
        &noop,
    )
    .unwrap();
    d.sync_metadata().unwrap();
    stats
}

#[test]
fn fat_to_fat_preserves_dos_attributes() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.img");
    let dst = dir.path().join("dst.img");
    write_blank_fat(&src, 4 * MB, "SRC");
    write_blank_fat(&dst, 4 * MB, "DST");

    {
        let mut fs = open_rw(&src);
        let root = fs.root().unwrap();
        let data = b"system file";
        // read-only (0x01) + system (0x04)
        let o = CreateFileOptions {
            dos_attributes: Some(0x01 | 0x04),
            ..Default::default()
        };
        let mut c = Cursor::new(data.to_vec());
        fs.create_file(&root, "IO.SYS", &mut c, data.len() as u64, &o)
            .unwrap();
        fs.sync_metadata().unwrap();
    }

    let stats = copy_one(&src, &dst, "IO.SYS", "IO.SYS");
    assert_eq!(stats.files, 1);
    assert_eq!(stats.dropped_attrs, 0);

    let mut d = open_ro(&dst);
    let droot = d.root().unwrap();
    let e = find(&mut *d, &droot, "IO.SYS").expect("copied file");
    assert_eq!(e.dos_attributes, Some(0x05), "RO+SYS should round-trip");
}

#[test]
fn hfs_to_hfs_preserves_fork_and_type_creator() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.img");
    let dst = dir.path().join("dst.img");
    write_blank_hfs(&src, 4 * MB, "SRC");
    write_blank_hfs(&dst, 4 * MB, "DST");

    let data = b"data fork contents";
    let rsrc = vec![0xABu8; 64];
    {
        let mut fs = open_rw(&src);
        let root = fs.root().unwrap();
        let o = CreateFileOptions {
            type_code: Some("TEXT".into()),
            creator_code: Some("ttxt".into()),
            resource_fork: Some(ResourceForkSource::Data(rsrc.clone())),
            ..Default::default()
        };
        let mut c = Cursor::new(data.to_vec());
        fs.create_file(&root, "Doc", &mut c, data.len() as u64, &o)
            .unwrap();
        fs.sync_metadata().unwrap();
    }

    let stats = copy_one(&src, &dst, "Doc", "Doc");
    assert_eq!(stats.files, 1);
    assert_eq!(stats.dropped_forks, 0);
    assert_eq!(stats.dropped_type_creator, 0);

    let mut d = open_ro(&dst);
    let droot = d.root().unwrap();
    let e = find(&mut *d, &droot, "Doc").expect("copied file");
    assert_eq!(e.type_code.as_deref(), Some("TEXT"));
    assert_eq!(e.creator_code.as_deref(), Some("ttxt"));
    assert_eq!(e.resource_fork_size, Some(64));
    assert_eq!(d.read_file(&e, usize::MAX).unwrap(), data);
    let mut rbuf = Vec::new();
    d.write_resource_fork_to(&e, &mut rbuf).unwrap();
    assert_eq!(rbuf, rsrc);
}

#[test]
fn hfs_to_fat_drops_fork_and_type_creator_but_keeps_data() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.img");
    let dst = dir.path().join("dst.img");
    write_blank_hfs(&src, 4 * MB, "SRC");
    write_blank_fat(&dst, 4 * MB, "DST");

    let data = b"portable bytes";
    {
        let mut fs = open_rw(&src);
        let root = fs.root().unwrap();
        let o = CreateFileOptions {
            type_code: Some("TEXT".into()),
            creator_code: Some("ttxt".into()),
            resource_fork: Some(ResourceForkSource::Data(vec![1, 2, 3, 4])),
            ..Default::default()
        };
        let mut c = Cursor::new(data.to_vec());
        fs.create_file(&root, "Doc", &mut c, data.len() as u64, &o)
            .unwrap();
        fs.sync_metadata().unwrap();
    }

    let stats = copy_one(&src, &dst, "Doc", "Doc");
    assert_eq!(stats.files, 1);
    assert_eq!(stats.dropped_forks, 1, "FAT has no resource fork");
    assert_eq!(stats.dropped_type_creator, 1, "FAT has no type/creator");

    // Data fork still copied intact.
    let mut d = open_ro(&dst);
    let droot = d.root().unwrap();
    let e = find(&mut *d, &droot, "Doc").expect("copied file");
    assert_eq!(d.read_file(&e, usize::MAX).unwrap(), data);
    assert!(e.type_code.is_none());
}

#[test]
fn long_name_truncated_for_hfs_destination() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.img"); // FAT allows long (LFN) names
    let dst = dir.path().join("dst.img"); // HFS caps names at 31 bytes
    write_blank_fat(&src, 4 * MB, "SRC");
    write_blank_hfs(&dst, 4 * MB, "DST");

    let long = "AReallyExcessivelyLongFileNameThatHFSCannotStore.txt";
    assert!(long.chars().count() > 31);
    let data = b"x";
    {
        let mut fs = open_rw(&src);
        let root = fs.root().unwrap();
        let mut c = Cursor::new(data.to_vec());
        fs.create_file(
            &root,
            long,
            &mut c,
            data.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();
        fs.sync_metadata().unwrap();
    }

    let stats = copy_one(&src, &dst, long, long);
    assert_eq!(stats.files, 1);
    assert_eq!(stats.renamed, 1, "should report the rename");

    let mut d = open_ro(&dst);
    let droot = d.root().unwrap();
    let children = d.list_directory(&droot).unwrap();
    assert_eq!(children.len(), 1);
    assert!(
        children[0].name.chars().count() <= 31,
        "mangled name {:?} must fit HFS",
        children[0].name
    );
}

#[test]
fn preflight_projection_exceeds_free_space() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.img");
    let dst = dir.path().join("dst.img");
    write_blank_fat(&src, 8 * MB, "SRC");
    write_blank_fat(&dst, 2 * MB, "DST");

    {
        let mut fs = open_rw(&src);
        let root = fs.root().unwrap();
        let big = vec![0u8; 3_000_000];
        let mut c = Cursor::new(big);
        fs.create_file(
            &root,
            "BIG.BIN",
            &mut c,
            3_000_000,
            &CreateFileOptions::default(),
        )
        .unwrap();
        fs.sync_metadata().unwrap();
    }

    let mut s = open_ro(&src);
    let mut d = open_rw(&dst);
    let sroot = s.root().unwrap();
    let big = find(&mut *s, &sroot, "BIG.BIN").unwrap();
    let need = copy::project(
        &mut *s,
        &big,
        &Capabilities::infer(d.fs_type()),
        d.allocation_unit(),
        false,
    )
    .unwrap();
    let free = d.free_space().unwrap();
    assert!(
        need > free,
        "3 MB file must not fit a 2 MB volume (need={need}, free={free})"
    );
}

#[test]
fn recursive_directory_copy_rebuilds_tree() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.img");
    let dst = dir.path().join("dst.img");
    write_blank_fat(&src, 4 * MB, "SRC");
    write_blank_fat(&dst, 4 * MB, "DST");

    let payload = b"nested payload";
    {
        let mut fs = open_rw(&src);
        let root = fs.root().unwrap();
        let sub = fs
            .create_directory(&root, "SUB", &Default::default())
            .unwrap();
        let mut c = Cursor::new(payload.to_vec());
        fs.create_file(
            &sub,
            "B.BIN",
            &mut c,
            payload.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();
        fs.sync_metadata().unwrap();
    }

    // Copy the /SUB directory (named, not the root) recursively into dst.
    {
        let mut s = open_ro(&src);
        let mut d = open_rw(&dst);
        let sroot = s.root().unwrap();
        let droot = d.root().unwrap();
        let sub = find(&mut *s, &sroot, "SUB").unwrap();
        let mut stats = CopyStats::default();
        copy::copy_into(
            &mut *s,
            &sub,
            &mut *d,
            &droot,
            "SUB",
            &opts(true),
            &mut stats,
            &noop,
        )
        .unwrap();
        d.sync_metadata().unwrap();
        assert_eq!(stats.dirs, 1);
        assert_eq!(stats.files, 1);
    }

    let mut d = open_ro(&dst);
    let droot = d.root().unwrap();
    let sub = find(&mut *d, &droot, "SUB").expect("dir copied");
    assert!(sub.is_directory());
    let b = find(&mut *d, &sub, "B.BIN").expect("nested file copied");
    assert_eq!(d.read_file(&b, usize::MAX).unwrap(), payload);
}
