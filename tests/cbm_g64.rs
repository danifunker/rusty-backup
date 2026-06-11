//! End-to-end test for the `.g64` raw-GCR pipeline:
//!   cbm engine builds a .d64 with real files
//!     -> g64::encode_g64_from_d64 (GCR-encode every track)
//!       -> written to a .g64 file
//!         -> source_reader decodes the GCR back to flat sectors
//!           -> the cbm engine reads the files out, byte-exact.
//!
//! This exercises the production decode path (`source_reader` ->
//! `decode_g64_bytes`) through the real `rb-cli` binary, plus a
//! library-level round-trip.

use std::io::Cursor;
use std::path::PathBuf;
use std::process::Command;

use rusty_backup::fs::cbm::{create_blank, CbmFilesystem, CbmVariant};
use rusty_backup::fs::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
use rusty_backup::rbformats::containers::g64::{decode_g64_bytes, encode_g64_from_d64};

/// Build a 1541 .d64 with two known files and return the flat bytes.
fn build_d64_with_files() -> Vec<u8> {
    let img = create_blank(CbmVariant::D64, "GCRTEST", "GC").unwrap();
    let mut fs = CbmFilesystem::open(Cursor::new(img), 0).unwrap();
    let root = fs.root().unwrap();
    let hello = b"\x01\x08HELLO FROM GCR\r".to_vec();
    fs.create_file(
        &root,
        "HELLO",
        &mut Cursor::new(hello.clone()),
        hello.len() as u64,
        &CreateFileOptions::default(),
    )
    .unwrap();
    let big: Vec<u8> = (0..1500).map(|i| (i * 5 % 256) as u8).collect();
    fs.create_file(
        &root,
        "BIGFILE",
        &mut Cursor::new(big.clone()),
        big.len() as u64,
        &CreateFileOptions::default(),
    )
    .unwrap();
    fs.into_inner().into_inner()
}

#[test]
fn d64_to_g64_to_cbm_round_trip_library() {
    let d64 = build_d64_with_files();
    let g64 = encode_g64_from_d64(&d64, 35).expect("encode g64");

    // Decode the GCR back to flat sectors and read with the cbm engine.
    let flat = decode_g64_bytes(&g64).expect("decode g64");
    let mut fs = CbmFilesystem::open(Cursor::new(flat), 0).expect("open decoded d64");
    assert_eq!(fs.volume_label(), Some("GCRTEST"));
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
    assert!(names.contains(&"HELLO"), "missing HELLO: {names:?}");
    assert!(names.contains(&"BIGFILE"), "missing BIGFILE: {names:?}");

    let hello = entries.iter().find(|e| e.name == "HELLO").unwrap();
    assert_eq!(
        fs.read_file(hello, usize::MAX).unwrap(),
        b"\x01\x08HELLO FROM GCR\r"
    );
    let big = entries.iter().find(|e| e.name == "BIGFILE").unwrap();
    let expected: Vec<u8> = (0..1500).map(|i| (i * 5 % 256) as u8).collect();
    assert_eq!(fs.read_file(big, usize::MAX).unwrap(), expected);
}

fn cli_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_rb-cli"))
}

#[test]
fn rb_cli_reads_g64_through_source_reader() {
    let d64 = build_d64_with_files();
    let g64 = encode_g64_from_d64(&d64, 35).expect("encode g64");

    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("disk.g64");
    std::fs::write(&path, &g64).unwrap();

    // `ls` routes .g64 through source_reader -> GCR decode -> cbm.
    let out = Command::new(cli_bin())
        .args(["ls", path.to_str().unwrap()])
        .output()
        .expect("spawn rb-cli");
    assert!(out.status.success(), "ls failed: {:?}", out);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("HELLO"), "ls missing HELLO:\n{stdout}");
    assert!(stdout.contains("BIGFILE"), "ls missing BIGFILE:\n{stdout}");

    // `get` extracts a file byte-exact from the .g64 directly.
    let dst = tmp.path().join("hello.out");
    let out = Command::new(cli_bin())
        .args([
            "get",
            path.to_str().unwrap(),
            "HELLO",
            dst.to_str().unwrap(),
        ])
        .output()
        .expect("spawn rb-cli");
    assert!(out.status.success(), "get failed: {:?}", out);
    assert_eq!(std::fs::read(&dst).unwrap(), b"\x01\x08HELLO FROM GCR\r");
}
