//! Validates the `.g71` (1571 GCR) decoder against a **real c1541-produced
//! image** — the gold-standard reference the gap was waiting on.
//!
//! Fixture `test_cbm_1571.g71.zst` was written by VICE `c1541`:
//!   `c1541 -format "g71fixture,gf" g71 fix.g71 -write README -write SIDEONE`
//! holding two known files:
//!   README  (PRG, 19 bytes)
//!   SIDEONE (PRG, 180000 bytes) — large enough to spill past side 0's
//!           ~664-block capacity onto side-1 half-tracks (indices 84-152),
//!           so reading it back byte-exact proves the side-1 mapping.
//!
//! The decode path: source_reader sees the `.g71` extension + `GCR-1571`
//! signature, runs `decode_g64_bytes` (GCR -> flat .d71), and the cbm
//! engine reads the files out.

use std::io::{Cursor, Read};

use rusty_backup::fs::cbm::CbmFilesystem;
use rusty_backup::fs::filesystem::Filesystem;
use rusty_backup::rbformats::containers::g64::decode_g64_bytes;

fn sha256_hex(b: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(b);
    h.finalize().iter().map(|x| format!("{x:02x}")).collect()
}

fn fixture_bytes() -> Vec<u8> {
    let compressed = std::fs::read("tests/fixtures/test_cbm_1571.g71.zst").expect("read fixture");
    let mut dec = zstd::stream::read::Decoder::new(Cursor::new(compressed)).expect("zstd");
    let mut out = Vec::new();
    dec.read_to_end(&mut out).expect("decompress");
    out
}

#[test]
fn decodes_real_c1541_g71_including_side_one() {
    let g71 = fixture_bytes();
    assert_eq!(&g71[0..8], b"GCR-1571", "fixture is a G71");

    // Decode the GCR bitstream down to a flat D71 and read it.
    let d71 = decode_g64_bytes(&g71).expect("decode g71");
    assert_eq!(d71.len(), 349_696, "1571 D71 is 349696 bytes");

    let mut fs = CbmFilesystem::open(Cursor::new(d71), 0).expect("open d71");
    assert_eq!(fs.volume_label(), Some("G71FIXTURE"));
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
    assert!(names.contains(&"README"), "names: {names:?}");
    assert!(names.contains(&"SIDEONE"), "names: {names:?}");

    let readme = entries.iter().find(|e| e.name == "README").unwrap();
    let readme_data = fs.read_file(readme, usize::MAX).unwrap();
    assert_eq!(
        sha256_hex(&readme_data),
        "3d39597eabc1ae92e244ccf929281d9a86603319579263db0eaa7115a4212b12",
        "README content mismatch (len {})",
        readme_data.len()
    );

    // The side-spanning file: byte-exact match proves the side-1 half-track
    // mapping (84 + (track-36)*2) is correct against a real VICE image.
    let sideone = entries.iter().find(|e| e.name == "SIDEONE").unwrap();
    let sideone_data = fs.read_file(sideone, usize::MAX).unwrap();
    assert_eq!(sideone_data.len(), 180_000, "SIDEONE length");
    assert_eq!(
        sha256_hex(&sideone_data),
        "ea2c9b2cc856970acc033becdfba537a440a02fe08ba851f64ee017a2d731037",
        "SIDEONE content mismatch — side-1 decode is wrong"
    );
}
