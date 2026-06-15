//! Library-level tests for the OS-9 / NitrOS-9 RBF engine (`src/fs/os9.rs`)
//! against the real `test_coco_os9l2.dsk` fixture. Covers the hierarchical
//! read tree and the `create_directory` write path (not reachable through the
//! flat `rb-cli put`/`rm` surface).

use rusty_backup::fs::entry::FileEntry;
use rusty_backup::fs::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem,
};
use rusty_backup::fs::os9::Os9Filesystem;
use std::io::{Cursor, Read};

fn fixture_bytes() -> Vec<u8> {
    let compressed = std::fs::read("tests/fixtures/test_coco_os9l2.dsk.zst").expect("read fixture");
    let mut dec = zstd::stream::read::Decoder::new(Cursor::new(compressed)).expect("zstd decoder");
    let mut bytes = Vec::new();
    dec.read_to_end(&mut bytes).expect("decompress");
    bytes
}

fn count_files<R: Read + std::io::Seek + Send>(
    fs: &mut Os9Filesystem<R>,
    dir: &FileEntry,
) -> usize {
    let mut n = 0;
    for child in fs.list_directory(dir).unwrap() {
        if child.is_directory() {
            n += count_files(fs, &child);
        } else {
            n += 1;
        }
    }
    n
}

#[test]
fn reads_full_hierarchical_tree() {
    let mut fs = Os9Filesystem::open(Cursor::new(fixture_bytes()), 0).unwrap();
    assert_eq!(fs.fs_type(), "OS-9");
    assert_eq!(fs.volume_label(), Some("Color Computer 3  OS-9 LII"));
    assert_eq!(fs.total_size(), 161280);
    let root = fs.root().unwrap();
    // 57 files across root + CMDS/ + SYS/.
    assert_eq!(count_files(&mut fs, &root), 57);

    // CMDS is a directory holding `copy`.
    let cmds = fs
        .list_directory(&root)
        .unwrap()
        .into_iter()
        .find(|e| e.name == "CMDS")
        .expect("CMDS dir");
    assert!(cmds.is_directory());
    let copy = fs
        .list_directory(&cmds)
        .unwrap()
        .into_iter()
        .find(|e| e.name == "copy")
        .expect("CMDS/copy");
    assert_eq!(copy.size, 743);
}

#[test]
fn create_directory_and_nested_file_round_trip() {
    let mut fs = Os9Filesystem::open(Cursor::new(fixture_bytes()), 0).unwrap();
    let root = fs.root().unwrap();
    let free0 = fs.free_space().unwrap();

    let dir = fs
        .create_directory(&root, "PROJ", &CreateDirectoryOptions::default())
        .unwrap();
    let payload: Vec<u8> = (0..1500u32).map(|i| (i % 256) as u8).collect();
    fs.create_file(
        &dir,
        "MAIN.B09",
        &mut Cursor::new(payload.clone()),
        payload.len() as u64,
        &CreateFileOptions::default(),
    )
    .unwrap();
    fs.sync_metadata().unwrap();

    // Re-open from the mutated bytes and read the nested file back.
    let bytes = fs.into_inner().into_inner();
    let mut fs2 = Os9Filesystem::open(Cursor::new(bytes), 0).unwrap();
    let root2 = fs2.root().unwrap();
    let proj = fs2
        .list_directory(&root2)
        .unwrap()
        .into_iter()
        .find(|e| e.name == "PROJ")
        .expect("PROJ dir");
    assert!(proj.is_directory());
    let main = fs2
        .list_directory(&proj)
        .unwrap()
        .into_iter()
        .find(|e| e.name == "MAIN.B09")
        .expect("PROJ/MAIN.B09");
    assert_eq!(fs2.read_file(&main, usize::MAX).unwrap(), payload);

    // Deleting the file then the (now-empty) directory reclaims all space.
    fs2.delete_entry(&proj, &main).unwrap();
    fs2.delete_entry(&root2, &proj).unwrap();
    fs2.sync_metadata().unwrap();
    assert_eq!(
        fs2.free_space().unwrap(),
        free0,
        "space not fully reclaimed"
    );
}

#[test]
fn refuses_to_delete_nonempty_directory() {
    let mut fs = Os9Filesystem::open(Cursor::new(fixture_bytes()), 0).unwrap();
    let root = fs.root().unwrap();
    let cmds = fs
        .list_directory(&root)
        .unwrap()
        .into_iter()
        .find(|e| e.name == "CMDS")
        .unwrap();
    let err = fs.delete_entry(&root, &cmds).unwrap_err();
    assert!(
        matches!(
            err,
            rusty_backup::fs::filesystem::FilesystemError::Unsupported(_)
        ),
        "expected refusal to delete non-empty dir, got {err:?}"
    );
}
