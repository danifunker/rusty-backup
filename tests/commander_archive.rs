//! Commander opens a Mac archive in-pane: `commander_descend::open_archive`
//! (what the pane's `load_archive` calls) decodes an archive into a browsable,
//! read-only `Filesystem` carrying both forks + type/creator.

use rusty_backup::fs::binhex::{build_binhex, BinHexFile};
use rusty_backup::model::commander_descend::open_archive;

#[test]
fn open_archive_browses_a_binhex_file_with_both_forks() {
    // Build a BinHex (.hqx) archive in memory holding one file with both forks.
    let data = b"hello data fork from the archive";
    let rsrc = b"RSRC fork bytes inside the archive";
    let hqx = build_binhex(&BinHexFile {
        name: "Note".into(),
        type_code: *b"TEXT",
        creator_code: *b"ttxt",
        flags: 0,
        data_fork: data.to_vec(),
        resource_fork: rsrc.to_vec(),
    });
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("note.hqx");
    std::fs::write(&path, hqx.as_bytes()).unwrap();

    // The exact call the Commander pane uses to open an archive as a source.
    let mut fs = open_archive(&path, Some("note.hqx".into())).unwrap();
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let note = entries
        .iter()
        .find(|e| e.is_file())
        .expect("archive lists its file");
    assert_eq!(note.type_code, Some(*b"TEXT"));
    assert_eq!(note.creator_code, Some(*b"ttxt"));
    assert_eq!(
        note.resource_fork_size,
        Some(rsrc.len() as u64),
        "the RSRC column has a size for archive entries"
    );

    // Both forks read back byte-exact (this is what copy-out / export use).
    let mut got_data = Vec::new();
    fs.write_file_to(note, &mut got_data).unwrap();
    assert_eq!(got_data, data, "data fork byte-exact");
    let mut got_rsrc = Vec::new();
    fs.write_resource_fork_to(note, &mut got_rsrc).unwrap();
    assert_eq!(got_rsrc, rsrc, "resource fork byte-exact");
}
