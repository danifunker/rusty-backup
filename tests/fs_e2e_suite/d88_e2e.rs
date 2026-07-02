//! End-to-end Sharp `.d88` + Human68k tests against a synthesized fixture.
//!
//! Fixture: `tests/fixtures/test_x68000_human68k_2dd.d88.zst`
//!   wrapper:    Sharp .d88 (2DD, 80 cyl × 2 heads × 9 sec × 512 B)
//!   filesystem: FAT12 with Human68k-compatible BPB (mkfs.fat output;
//!                Human68k = FAT12 + 18.3 directory extension, the 8.3
//!                bytes are a subset so any FAT12 disk reads cleanly)
//!   files:
//!     - `HELLO.TXT` (30 B) — "Hello from D88 X68000 fixture."
//!     - `NOTE.TXT`  (31 B) — "Second file, different content."
//!
//! Built by `scripts/generate-d88-fixture.sh` via `mkfs.fat -F 12` +
//! `mcopy` + the in-tree `examples/d88_encode` wrapper helper. Real
//! TOSEC `.d88` dumps were used for SCOUTING — `anchor_mister_BLANK_
//! disk_X68000.D88.zst` validated that our decoder produces the
//! expected 1,261,568-byte flat for a 2HD disk and that our Human68k
//! engine then lists the standard system-disk contents (AUTOEXEC.BAT,
//! COMMAND.X, HUMAN.SYS, …) — but only the synthesized fixture is
//! committed as a test asset.

use std::io::{Cursor, Read};

use rusty_backup::fs::filesystem::Filesystem;
use rusty_backup::fs::human68k::Human68kFilesystem;
use rusty_backup::rbformats::containers::d88;

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
fn d88_decodes_to_expected_2dd_flat_size() {
    let raw = load_fixture("test_x68000_human68k_2dd.d88.zst");
    let flat = d88::decode_d88_bytes(&raw).unwrap();
    // 80 cyl × 2 sides × 9 sec × 512 B = 737280 B (= 720 KiB).
    assert_eq!(flat.len(), 737_280, "decoded 2DD flat is 720 KiB");
}

#[test]
fn looks_like_d88_accepts_wrapped_fixture() {
    let raw = load_fixture("test_x68000_human68k_2dd.d88.zst");
    assert!(d88::looks_like_d88_header(&raw[..0x24]));
}

#[test]
fn d88_then_human68k_lists_seeded_files() {
    let raw = load_fixture("test_x68000_human68k_2dd.d88.zst");
    let flat = d88::decode_d88_bytes(&raw).unwrap();
    let cur = Cursor::new(flat);
    let mut fs = Human68kFilesystem::open(cur, 0).unwrap();
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
    assert!(names.contains(&"HELLO.TXT"), "got: {names:?}");
    assert!(names.contains(&"NOTE.TXT"), "got: {names:?}");
}

#[test]
fn d88_then_human68k_reads_files_byte_exact_against_mcopy_input() {
    let raw = load_fixture("test_x68000_human68k_2dd.d88.zst");
    let flat = d88::decode_d88_bytes(&raw).unwrap();
    let cur = Cursor::new(flat);
    let mut fs = Human68kFilesystem::open(cur, 0).unwrap();
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let hello = entries.iter().find(|e| e.name == "HELLO.TXT").unwrap();
    let data = fs.read_file(hello, 4096).unwrap();
    assert_eq!(&data, b"Hello from D88 X68000 fixture.");
    let note = entries.iter().find(|e| e.name == "NOTE.TXT").unwrap();
    let data = fs.read_file(note, 4096).unwrap();
    assert_eq!(&data, b"Second file, different content.");
}

#[test]
fn encode_round_trip_holds_for_fixture_bytes() {
    // Decode → re-encode → decode again should be byte-identical: a
    // regression on the wrapper layout would surface here.
    let raw = load_fixture("test_x68000_human68k_2dd.d88.zst");
    let flat_1 = d88::decode_d88_bytes(&raw).unwrap();
    let re_encoded = d88::encode_d88_bytes(&flat_1, 80, 2, 9, 512, d88::D88Media::Dd2dd).unwrap();
    let flat_2 = d88::decode_d88_bytes(&re_encoded).unwrap();
    assert_eq!(flat_1, flat_2);
}
