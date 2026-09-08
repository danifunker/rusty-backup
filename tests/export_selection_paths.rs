//! Multi-select export across folders must not collapse two same-named files
//! onto one another.
//!
//! Every export writer named a selected root by its bare filename, so picking
//! `/docs/notes.txt` and `/backup/notes.txt` together produced a single
//! `notes.txt` — in an archive, or as a real overwritten file on disk for the
//! folder outputs. These cases exercise the writers end to end rather than the
//! naming helpers alone, because the failure was silent: the export reported
//! success and simply lost one of the two files.

use rusty_backup::cli::resolve::{resolve_partition_ro, resolve_partition_rw};
use rusty_backup::fs::export_selection::{export_to_file, export_to_folder, ExportFormat};
use rusty_backup::fs::filesystem::{CreateDirectoryOptions, CreateFileOptions};
use rusty_backup::fs::resource_fork::ResourceForkMode;
use rusty_backup::fs::{entry::FileEntry, fat, open_editable_filesystem, open_filesystem};

const DOCS: &[u8] = b"FROM DOCS";
const BACKUP: &[u8] = b"FROM BACKUP";

/// A FAT volume with the same filename in two different folders.
fn make_colliding_image(path: &std::path::Path) {
    std::fs::write(
        path,
        fat::create_blank_fat(8 * 1024 * 1024, Some("COLLIDE")).unwrap(),
    )
    .unwrap();
    let (file, ctx, commit) = resolve_partition_rw(path, None).unwrap();
    let mut efs =
        open_editable_filesystem(file, ctx.offset, ctx.type_byte, ctx.type_string.as_deref())
            .unwrap();
    let root = efs.root().unwrap();
    for (dir, payload) in [("docs", DOCS), ("backup", BACKUP)] {
        efs.create_directory(&root, dir, &CreateDirectoryOptions::default())
            .unwrap();
        let parent = efs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name.eq_ignore_ascii_case(dir))
            .expect("directory created");
        let mut d = payload;
        efs.create_file(
            &parent,
            "notes.txt",
            &mut d,
            payload.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();
    }
    efs.sync_metadata().unwrap();
    drop(efs);
    commit.commit().unwrap();
}

/// The two `notes.txt` entries, one from each folder.
fn colliding_entries(fs: &mut dyn rusty_backup::fs::filesystem::Filesystem) -> Vec<FileEntry> {
    let root = fs.root().unwrap();
    let mut picked = Vec::new();
    for dir in ["docs", "backup"] {
        let d = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name.eq_ignore_ascii_case(dir))
            .expect("folder present");
        let f = fs
            .list_directory(&d)
            .unwrap()
            .into_iter()
            .find(|e| e.name.eq_ignore_ascii_case("notes.txt"))
            .expect("file present");
        picked.push(f);
    }
    picked
}

#[test]
fn folder_export_keeps_same_named_files_from_different_folders() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("collide.img");
    make_colliding_image(&img);

    let (file, ctx) = resolve_partition_ro(&img, None).unwrap();
    let mut fs =
        open_filesystem(file, ctx.offset, ctx.type_byte, ctx.type_string.as_deref()).unwrap();
    let picked = colliding_entries(&mut *fs);
    assert_eq!(picked.len(), 2);

    let out = tempfile::tempdir().unwrap();
    let summary = export_to_folder(
        &mut *fs,
        &picked,
        out.path(),
        ExportFormat::LooseFiles,
        ResourceForkMode::DataForkOnly,
        &|_, _, _| {},
        &|| false,
    )
    .unwrap();
    assert_eq!(summary.files, 2, "both files must be written");

    // Before the fix both landed at `out/notes.txt` and the second overwrote
    // the first, so this is the assertion that actually catches the defect.
    let from_docs = out.path().join("docs").join("notes.txt");
    let from_backup = out.path().join("backup").join("notes.txt");
    assert!(from_docs.is_file(), "docs/notes.txt missing");
    assert!(from_backup.is_file(), "backup/notes.txt missing");
    assert_eq!(std::fs::read(&from_docs).unwrap(), DOCS);
    assert_eq!(std::fs::read(&from_backup).unwrap(), BACKUP);
}

#[test]
fn tar_export_keeps_same_named_files_from_different_folders() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("collide.img");
    make_colliding_image(&img);

    let (file, ctx) = resolve_partition_ro(&img, None).unwrap();
    let mut fs =
        open_filesystem(file, ctx.offset, ctx.type_byte, ctx.type_string.as_deref()).unwrap();
    let picked = colliding_entries(&mut *fs);

    let out = tempfile::tempdir().unwrap();
    let tgz = out.path().join("sel.tar.gz");
    export_to_file(
        &mut *fs,
        &picked,
        &tgz,
        ExportFormat::TarGz,
        &|_, _, _| {},
        &|| false,
    )
    .unwrap();

    let raw = std::fs::File::open(&tgz).unwrap();
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(raw));
    let mut seen: Vec<(String, Vec<u8>)> = Vec::new();
    for e in archive.entries().unwrap() {
        let mut e = e.unwrap();
        let name = e.path().unwrap().to_string_lossy().into_owned();
        let mut buf = Vec::new();
        std::io::Read::read_to_end(&mut e, &mut buf).unwrap();
        if !buf.is_empty() {
            seen.push((name, buf));
        }
    }
    seen.sort();
    assert_eq!(seen.len(), 2, "both members must be present: {seen:?}");
    assert_eq!(seen[0].0, "backup/notes.txt");
    assert_eq!(seen[0].1, BACKUP);
    assert_eq!(seen[1].0, "docs/notes.txt");
    assert_eq!(seen[1].1, DOCS);
}

/// The behaviour that must NOT change: everything picked from one folder still
/// exports under bare names, with no folder wrapper.
#[test]
fn single_folder_selection_still_exports_bare_names() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("collide.img");
    make_colliding_image(&img);

    let (file, ctx) = resolve_partition_ro(&img, None).unwrap();
    let mut fs =
        open_filesystem(file, ctx.offset, ctx.type_byte, ctx.type_string.as_deref()).unwrap();
    let root = fs.root().unwrap();
    let docs = fs
        .list_directory(&root)
        .unwrap()
        .into_iter()
        .find(|e| e.name.eq_ignore_ascii_case("docs"))
        .unwrap();
    let only = fs.list_directory(&docs).unwrap();

    let out = tempfile::tempdir().unwrap();
    export_to_folder(
        &mut *fs,
        &only,
        out.path(),
        ExportFormat::LooseFiles,
        ResourceForkMode::DataForkOnly,
        &|_, _, _| {},
        &|| false,
    )
    .unwrap();
    assert!(
        out.path().join("notes.txt").is_file(),
        "a one-folder selection must stay flat, not gain a docs/ wrapper"
    );
}
