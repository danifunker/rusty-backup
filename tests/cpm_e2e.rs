//! End-to-end CP/M tests against a cpmtools-built fixture.
//!
//! Strongest oracle of any Wave-2 core: the fixture is produced by
//! Ubuntu's `cpmtools` (`mkfs.cpm -f cpcdata` + `cpmcp`), then read
//! back by our pure-Rust engine. Byte-identity of the seeded content
//! proves the two agree.
//!
//! Fixture: `tests/fixtures/test_cpm_amstrad_data.dsk.zst`
//!   format: cpmtools `cpcdata` (Amstrad CPC 6128 data-format, 180 KB)
//!   files:
//!     - `hello.txt` (33 B) — "Hello from CP/M cpmtools fixture."
//!     - `note.txt`  (31 B) — "Second file, different content."

use std::io::{Cursor, Read};

use rusty_backup::fs::cpm::CpmFilesystem;
use rusty_backup::fs::cpm_diskdefs::AMSTRAD_DATA;
use rusty_backup::fs::filesystem::Filesystem;

fn load_fixture(name: &str) -> Vec<u8> {
    let path = format!("tests/fixtures/{name}");
    let compressed = std::fs::read(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    let mut decoder = zstd::stream::read::Decoder::new(Cursor::new(compressed))
        .unwrap_or_else(|e| panic!("zstd decoder for {path}: {e}"));
    let mut out = Vec::new();
    decoder
        .read_to_end(&mut out)
        .unwrap_or_else(|e| panic!("decompress {path}: {e}"));
    out
}

#[test]
fn opens_cpmtools_built_fixture_with_amstrad_data_dpb() {
    let disk = load_fixture("test_cpm_amstrad_data.dsk.zst");
    assert_eq!(disk.len(), 180 * 1024, "Amstrad data is 180 KB");
    let cur = Cursor::new(disk);
    let mut fs = CpmFilesystem::open_with_dpb(cur, 0, AMSTRAD_DATA).unwrap();
    assert_eq!(fs.fs_type(), "CP/M");
    assert_eq!(fs.volume_label(), Some("amstrad_data"));
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
    assert!(names.contains(&"HELLO.TXT"), "got: {names:?}");
    assert!(names.contains(&"NOTE.TXT"), "got: {names:?}");
}

#[test]
fn reads_files_byte_exact_against_cpmcp_input() {
    let disk = load_fixture("test_cpm_amstrad_data.dsk.zst");
    let cur = Cursor::new(disk);
    let mut fs = CpmFilesystem::open_with_dpb(cur, 0, AMSTRAD_DATA).unwrap();
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let hello = entries.iter().find(|e| e.name == "HELLO.TXT").unwrap();
    let data = fs.read_file(hello, 4096).unwrap();
    assert_eq!(
        &data, b"Hello from CP/M cpmtools fixture.",
        "our decoder must agree with cpmcp's input bytes"
    );
    let note = entries.iter().find(|e| e.name == "NOTE.TXT").unwrap();
    let data = fs.read_file(note, 4096).unwrap();
    assert_eq!(&data, b"Second file, different content.");
}

#[test]
fn dispatch_via_open_filesystem_with_cpm_partition_type_string() {
    // Same disk, routed through fs::open_filesystem so the
    // partition_type_string "cpm:amstrad_data" dispatch path holds.
    let disk = load_fixture("test_cpm_amstrad_data.dsk.zst");
    let cur = Cursor::new(disk);
    let mut fs = rusty_backup::fs::open_filesystem(cur, 0, 0, Some("cpm:amstrad_data")).unwrap();
    assert_eq!(fs.fs_type(), "CP/M");
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    assert!(entries.iter().any(|e| e.name == "HELLO.TXT"));
}

#[test]
fn unknown_cpm_dpb_name_fails_with_helpful_error() {
    let disk = load_fixture("test_cpm_amstrad_data.dsk.zst");
    let cur = Cursor::new(disk);
    let result = rusty_backup::fs::open_filesystem(cur, 0, 0, Some("cpm:totally_bogus_preset"));
    let err = match result {
        Ok(_) => panic!("expected error for bogus preset"),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("totally_bogus_preset") && msg.contains("amstrad_data"),
        "error must name the bad preset and list valid ones; got: {msg}"
    );
}
