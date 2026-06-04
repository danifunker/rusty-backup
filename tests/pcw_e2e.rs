//! End-to-end Amstrad PCW Format A tests against a cpmtools-built fixture.
//!
//! Fixture: `tests/fixtures/test_pcw_format_a.dsk.zst`
//!   format: cpmtools `pcw` (Amstrad PCW Format A, 40 trk × 9 × 512 B,
//!           off=1 reserved boot track, 1024 B allocation blocks)
//!   files:
//!     - `hello.txt` (32 B) — "Hello from PCW Format A fixture."
//!     - `note.txt`  (31 B) — "Second file, different content."
//!
//! Built by `scripts/generate-pcw-fixture.sh` via `mkfs.cpm -f pcw` +
//! `cpmcp -f pcw`. Real TOSEC PCW disks were used for SCOUTING (to
//! discover Format A geometry + identify the reserved-bytes formula
//! bug); they are explicitly NOT committed as test fixtures. cpmtools
//! is the byte-identity oracle.
//!
//! Regression coverage for the 2026-06-04 reserved-bytes formula fix:
//! `read_directory` used to compute `off × spt × 128 × (sector_size/128)`
//! which double-counted for sector_size > 128. With sector_size=512 and
//! off=1, the directory landed at byte 18432 instead of 4608, producing
//! garbage filenames. Since AMSTRAD_DATA (off=0) escaped the bug and
//! was the only DPB the existing tests touched, the bug went undetected
//! until this fixture was synthesized.

use std::io::{Cursor, Read};

use rusty_backup::fs::cpm::CpmFilesystem;
use rusty_backup::fs::cpm_diskdefs::AMSTRAD_PCW;
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

fn open_synth() -> CpmFilesystem<Cursor<Vec<u8>>> {
    let flat = load_fixture("test_pcw_format_a.dsk.zst");
    // Format A: 40 trk × 9 sec × 512 B = 184320 B flat.
    assert_eq!(flat.len(), 184_320, "PCW Format A is 180 KiB");
    CpmFilesystem::open_with_dpb(Cursor::new(flat), 0, AMSTRAD_PCW).unwrap()
}

#[test]
fn opens_synthesized_fixture_with_amstrad_pcw_dpb() {
    let mut fs = open_synth();
    assert_eq!(fs.fs_type(), "CP/M");
    assert_eq!(fs.volume_label(), Some("amstrad_pcw"));
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
    assert!(names.contains(&"HELLO.TXT"), "got: {names:?}");
    assert!(names.contains(&"NOTE.TXT"), "got: {names:?}");
}

#[test]
fn reads_files_byte_exact_against_cpmcp_input() {
    let mut fs = open_synth();
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let hello = entries.iter().find(|e| e.name == "HELLO.TXT").unwrap();
    let data = fs.read_file(hello, 4096).unwrap();
    assert_eq!(
        &data, b"Hello from PCW Format A fixture.",
        "our decoder must agree with cpmcp's input bytes"
    );
    let note = entries.iter().find(|e| e.name == "NOTE.TXT").unwrap();
    let data = fs.read_file(note, 4096).unwrap();
    assert_eq!(&data, b"Second file, different content.");
}

#[test]
fn dispatch_via_open_filesystem_with_amstrad_pcw_partition_type_string() {
    let flat = load_fixture("test_pcw_format_a.dsk.zst");
    let cur = Cursor::new(flat);
    let mut fs = rusty_backup::fs::open_filesystem(cur, 0, 0, Some("cpm:amstrad_pcw")).unwrap();
    assert_eq!(fs.fs_type(), "CP/M");
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    assert!(entries.iter().any(|e| e.name == "HELLO.TXT"));
}

#[test]
fn reserved_track_bytes_for_pcw_match_off_times_spt_times_128() {
    // Regression: with off=1, spt=36 (PCW Format A), the directory must
    // start at byte 4608 (= 1 × 9 × 512 = one reserved 512-B track).
    // The pre-fix formula computed 18432 (4× too deep), putting the
    // directory reader well inside the data area and producing garbage
    // listings. We pin both the right offset and the wrong one:
    //   - byte 4608 holds the CP/M-Plus volume-label entry that
    //     `mkfs.cpm -f pcw` stamps automatically: user=0x20 (label
    //     flag), name="UNLABELED " (cpmtools' default when no label
    //     is passed).
    //   - byte 4640 (slot 1) holds the first user file, HELLO.TXT.
    //   - byte 18432 sits inside the (still-zero) unallocated data
    //     area. `mkfs.cpm` doesn't lay 0xE5 there because we dd-zero
    //     the image first; what matters is that no recognizable
    //     directory entry shape lives at that offset. If the bug
    //     regresses, the directory reader would land there and the
    //     listing test above would fail to find HELLO.TXT.
    let flat = load_fixture("test_pcw_format_a.dsk.zst");
    assert_eq!(flat[4608], 0x20, "byte 4608 should be the label entry");
    assert_eq!(
        &flat[4609..4620],
        b"UNLABELED  ",
        "label entry's 11-char name+ext field"
    );
    assert_eq!(flat[4640], 0x00, "byte 4640 (slot 1) should be active");
    assert_eq!(&flat[4641..4649], b"HELLO   ", "slot 1's 8-char name");
    assert_eq!(&flat[4649..4652], b"TXT", "slot 1's 3-char ext");
    // Byte 18432 (off×spt×128×(ss/128) = the buggy offset) must not
    // contain a CP/M label or active entry head; it sits inside the
    // unallocated data area.
    assert_eq!(
        &flat[18_432..18_464],
        &[0u8; 32],
        "byte 18432 (where the buggy formula misread the directory) \
         must sit in the zero-filled unallocated data area"
    );
}
