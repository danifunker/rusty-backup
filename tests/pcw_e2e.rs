//! End-to-end Amstrad PCW EDSK + CP/M tests against a real TOSEC disk.
//!
//! Fixture: `tests/fixtures/anchor_pcw_PAW.dsk.zst`
//!   Source: TOSEC PCW Applications archive (`Amstrad PCW - Applications.7z`)
//!   Title:  "PAW (19xx)(Gilberts, Tim - Yeandle, Graeme)"
//!   Format: Standard CPCEMU DSK (40 trk × 9 sec × 512 B, PCW Format A
//!           system disk — `off=1` reserved boot track)
//!
//! PAW (Professional Adventure Writer) is the canonical PCW Format A
//! boot disk: track 0 holds the PCW BIOS, tracks 1..39 hold the CP/M
//! directory + files. cpmtools' `pcw` diskdef (libdsk `pcw180`) reads
//! the same disk cleanly, giving us a byte-for-byte oracle.
//!
//! Regression coverage for the 2026-06-04 reserved-bytes formula fix:
//! `read_directory` used to compute `off × spt × 128 × (sector_size/128)`
//! which double-counted for sector_size > 128. With sector_size=512 and
//! off=1, the directory landed at byte 18432 instead of 4608, producing
//! garbage filenames. Since AMSTRAD_DATA has off=0 and the only off>0
//! presets (PCW / EINSTEIN / etc.) had no real-world fixtures, the bug
//! went undetected until this disk landed.

use std::io::{Cursor, Read};

use rusty_backup::fs::cpm::CpmFilesystem;
use rusty_backup::fs::cpm_diskdefs::AMSTRAD_PCW;
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

fn open_paw() -> CpmFilesystem<Cursor<Vec<u8>>> {
    let raw = load_fixture("anchor_pcw_PAW.dsk.zst");
    assert_eq!(raw.len(), 194_816, "TOSEC DSK is 190 KiB on disk");
    let flat = decode_edsk_bytes(&raw).unwrap();
    assert_eq!(flat.len(), 184_320, "decoded flat is 180 KiB");
    CpmFilesystem::open_with_dpb(Cursor::new(flat), 0, AMSTRAD_PCW).unwrap()
}

#[test]
fn paw_edsk_decodes_to_format_a_flat_size() {
    let raw = load_fixture("anchor_pcw_PAW.dsk.zst");
    assert_eq!(raw.len(), 194_816);
    let flat = decode_edsk_bytes(&raw).unwrap();
    // PCW Format A: 40 tracks × 9 sectors × 512 B = 184320 (single-sided).
    assert_eq!(flat.len(), 184_320);
}

#[test]
fn paw_lists_canonical_files_via_amstrad_pcw() {
    let mut fs = open_paw();
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
    // Byte-identical with cpmtools `cpmls -f pcw` output for the same disk:
    //   addartp.com, diff.txt, edit.com, edit.ins, edit.key, editinst.com,
    //   epcw.z80, mppic.com, pawcomp.com, ...
    for expected in [
        "ADDARTP.COM",
        "DIFF.TXT",
        "EDIT.COM",
        "EDIT.INS",
        "EDIT.KEY",
        "EDITINST.COM",
        "PAWCOMP.COM",
    ] {
        assert!(
            names.contains(&expected),
            "expected {expected} on PAW disk; got {names:?}"
        );
    }
}

#[test]
fn paw_reads_edit_com_with_plausible_pcw_binary_shape() {
    let mut fs = open_paw();
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let edit = entries.iter().find(|e| e.name == "EDIT.COM").unwrap();
    // EDIT.COM on the PAW disk is the editor binary — non-empty and
    // CP/M-Plus-sized (a few KiB, well under the 64 KiB TPA cap).
    assert!(
        edit.size > 1024 && edit.size < 65_536,
        "EDIT.COM size out of plausible range: {}",
        edit.size
    );
    let data = fs.read_file(edit, 65_536).unwrap();
    assert!(data.len() >= edit.size as usize - 128);
}

#[test]
fn reserved_track_bytes_for_pcw_match_off_times_spt_times_128() {
    // Regression: with off=1, spt=36 (PCW Format A), the directory must
    // start at byte 4608 (= 1 × 9 × 512 = one reserved 512-B track).
    // The pre-fix formula put it at 18432, producing garbage entries.
    let raw = load_fixture("anchor_pcw_PAW.dsk.zst");
    let flat = decode_edsk_bytes(&raw).unwrap();
    // Byte 4608 starts the first directory entry: user=0, name=PAWCOMP,
    // ext=COM (8.3 padded with spaces).
    assert_eq!(flat[4608], 0x00, "user byte for first entry should be 0");
    assert_eq!(&flat[4609..4617], b"PAWCOMP ", "first entry's 8-char name");
    assert_eq!(&flat[4617..4620], b"COM", "first entry's 3-char ext");
}
