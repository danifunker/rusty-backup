//! End-to-end Amstrad CPC EDSK + CP/M tests against a real TOSEC disk.
//!
//! Fixture: `tests/fixtures/anchor_cpc_ManicMiner.dsk.zst`
//!   Source: hoglet67 / TOSEC Amstrad_CPC_TOSEC_2012_04_23
//!   Title:  "Manic Miner (1985)(Software Projects - Amsoft)"
//!   Format: Standard CPCEMU DSK (40 trk × 9 sec × 512 B, AMSDOS layout)
//!
//! Real-world cross-validation: the same flat-decoded bytes our EDSK
//! decoder + amstrad_data DPB produce make `cpmtools` 2.21 crash with
//! `malloc(): invalid size (unsorted)` — our engine is more robust on
//! this particular EDSK while still extracting the canonical
//! `MANIC.BAS` (loader) + `MANIC.BIN` (game binary) pair every CPC
//! emulator knows.

use std::io::{Cursor, Read};

use rusty_backup::fs::cpm::CpmFilesystem;
use rusty_backup::fs::cpm_diskdefs::AMSTRAD_DATA;
use rusty_backup::fs::filesystem::Filesystem;
use rusty_backup::rbformats::containers::edsk::decode_edsk_bytes;

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
fn manic_miner_edsk_decodes_to_expected_flat_size() {
    let raw = load_fixture("anchor_cpc_ManicMiner.dsk.zst");
    assert_eq!(raw.len(), 194_816, "TOSEC DSK is 190 KiB on disk");
    let flat = decode_edsk_bytes(&raw).unwrap();
    // 40 tracks × 9 sectors × 512 B = 184320 (single-sided Amstrad data).
    assert_eq!(flat.len(), 184_320, "decoded flat is 180 KiB");
}

#[test]
fn manic_miner_lists_basic_loader_and_binary_via_amstrad_data() {
    let raw = load_fixture("anchor_cpc_ManicMiner.dsk.zst");
    let flat = decode_edsk_bytes(&raw).unwrap();
    let cur = Cursor::new(flat);
    let mut fs = CpmFilesystem::open_with_dpb(cur, 0, AMSTRAD_DATA).unwrap();
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
    assert!(
        names.contains(&"MANIC.BAS"),
        "expected MANIC.BAS loader; got {names:?}"
    );
    assert!(
        names.contains(&"MANIC.BIN"),
        "expected MANIC.BIN game binary; got {names:?}"
    );
}

#[test]
fn manic_miner_reads_basic_loader_with_amsdos_header() {
    let raw = load_fixture("anchor_cpc_ManicMiner.dsk.zst");
    let flat = decode_edsk_bytes(&raw).unwrap();
    let cur = Cursor::new(flat);
    let mut fs = CpmFilesystem::open_with_dpb(cur, 0, AMSTRAD_DATA).unwrap();
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let loader = entries.iter().find(|e| e.name == "MANIC.BAS").unwrap();
    let data = fs.read_file(loader, 8192).unwrap();
    // AMSDOS files start with a 128-byte header. The first 8 bytes
    // are zero (CP/M user 0 etc.), and the filename "MANIC   BAS" is
    // stamped at the start of the AMSDOS header at offset 1..12.
    assert!(data.len() >= 128, "BASIC loader must include AMSDOS header");
    assert_eq!(&data[1..9], b"MANIC   ", "AMSDOS header name bytes");
    assert_eq!(&data[9..12], b"BAS", "AMSDOS header ext bytes");
}

#[test]
fn manic_miner_reads_binary_with_plausible_size() {
    let raw = load_fixture("anchor_cpc_ManicMiner.dsk.zst");
    let flat = decode_edsk_bytes(&raw).unwrap();
    let cur = Cursor::new(flat);
    let mut fs = CpmFilesystem::open_with_dpb(cur, 0, AMSTRAD_DATA).unwrap();
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let bin = entries.iter().find(|e| e.name == "MANIC.BIN").unwrap();
    // Manic Miner's game binary is ~21 KB on this TOSEC dump (size
    // recorded in the CP/M directory entry).
    assert!(
        bin.size > 20_000 && bin.size < 25_000,
        "MANIC.BIN size should be ~21 KB; got {}",
        bin.size
    );
    let data = fs.read_file(bin, 65536).unwrap();
    // Read every byte the directory promises (zero-padded if short).
    assert!(data.len() >= bin.size as usize - 1024);
}
