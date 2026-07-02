//! End-to-end tests for AtariST MiSTer-core fixtures.
//!
//! Two fixtures, both produced by `scripts/generate-atarist-fixtures.sh` and
//! `scripts/generate-ahdi-fixture.sh`:
//!
//! - `test_atarist_floppy.msa.zst` — 720K FAT12 floppy encoded with Hatari's
//!   reference `hmsa`. Decompresses to a real on-disk MSA file with header
//!   `$0E0F` + per-track RLE.
//! - `test_atarist_floppy.st.zst` — the same floppy as a flat raw stream
//!   (737280 bytes). Used to byte-compare against our decoder output.
//! - `test_atarist_ahdi.img.zst` — 32 MiB AHDI HDD image, GEM @ LBA 2 (8 MiB
//!   FAT12) + BGM @ LBA 16386 (16 MiB FAT16). Built by `mkfs.fat` +
//!   `mtools` with a Python-stamped AHDI root sector. mtools-verified at
//!   each partition's declared sector offset before commit.
//!
//! Content seeded into every FAT partition:
//!
//! - Floppy:   HELLO.TXT (40 B) + PROG/NESTED.TXT (18 B)
//! - AHDI P0 (GEM FAT12):  HELLO.TXT (22 B) + SUB/NESTED.TXT (21 B)
//! - AHDI P1 (BGM FAT16):  HELLO.TXT (22 B) + SUB/NESTED.TXT (21 B)
//!
//! Run with: cargo test --test atarist_e2e

use std::io::{Cursor, Read, Seek, SeekFrom};

use rusty_backup::fs::filesystem::Filesystem;
use rusty_backup::partition::PartitionTable;
use rusty_backup::rbformats::containers::msa::decode_msa_bytes;

/// Decompress a zstd-compressed fixture file into memory.
fn load_fixture(name: &str) -> Vec<u8> {
    let path = format!("tests/fixtures/{name}");
    let compressed =
        std::fs::read(&path).unwrap_or_else(|e| panic!("Failed to read fixture {path}: {e}"));
    let mut decoder = zstd::stream::read::Decoder::new(Cursor::new(compressed))
        .unwrap_or_else(|e| panic!("Failed to create zstd decoder for {path}: {e}"));
    let mut output = Vec::new();
    decoder
        .read_to_end(&mut output)
        .unwrap_or_else(|e| panic!("Failed to decompress {path}: {e}"));
    output
}

// ============================================================================
// MSA fixture — our decoder vs. Hatari's reference encoder
// ============================================================================

#[test]
fn msa_decoder_matches_hmsa_reference_byte_for_byte() {
    // The committed .msa was produced by Hatari's `hmsa` (Ubuntu hatari pkg).
    // Decoding it with our pure-Rust decoder must yield the exact same flat
    // sector stream that was the input to hmsa — that's the round-trip oracle.
    let msa = load_fixture("test_atarist_floppy.msa.zst");
    let expected_flat = load_fixture("test_atarist_floppy.st.zst");

    let decoded = decode_msa_bytes(&msa).expect("MSA decode failed");
    assert_eq!(
        decoded.len(),
        expected_flat.len(),
        "decoded length mismatch (expected {} == 720 KiB)",
        expected_flat.len()
    );
    assert_eq!(
        decoded, expected_flat,
        "decoded bytes != reference flat .st"
    );
}

#[test]
fn msa_decodes_to_a_720kb_floppy() {
    let msa = load_fixture("test_atarist_floppy.msa.zst");
    let decoded = decode_msa_bytes(&msa).unwrap();
    assert_eq!(decoded.len(), 720 * 1024);
    // Boot signature 0xAA55 at bytes 510-511 (a FAT VBR's hallmark).
    assert_eq!(decoded[510], 0x55);
    assert_eq!(decoded[511], 0xAA);
}

#[test]
fn msa_decoded_floppy_lists_and_reads_files_via_fat12() {
    let msa = load_fixture("test_atarist_floppy.msa.zst");
    let decoded = decode_msa_bytes(&msa).unwrap();

    let mut cur = Cursor::new(decoded);
    let table = PartitionTable::detect(&mut cur).unwrap();
    assert_eq!(
        table.type_name(),
        "None",
        "MSA-decoded floppy is superfloppy"
    );
    let parts = table.partitions();
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].type_name, "FAT");
    assert_eq!(parts[0].size_bytes, 720 * 1024);

    // Reopen with the FAT driver at offset 0 (superfloppy).
    cur.seek(SeekFrom::Start(0)).unwrap();
    let mut fs = rusty_backup::fs::fat::FatFilesystem::open(cur, 0).unwrap();
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
    assert!(names.iter().any(|n| n.eq_ignore_ascii_case("HELLO.TXT")));
    assert!(names.iter().any(|n| n.eq_ignore_ascii_case("PROG")));

    let hello = entries
        .iter()
        .find(|e| e.name.eq_ignore_ascii_case("HELLO.TXT"))
        .unwrap();
    let data = fs.read_file(hello, 4096).unwrap();
    assert_eq!(&data, b"AtariST floppy hello via MSA round-trip.");
    assert_eq!(data.len(), 40);

    let prog = entries
        .iter()
        .find(|e| e.name.eq_ignore_ascii_case("PROG"))
        .unwrap();
    let prog_entries = fs.list_directory(prog).unwrap();
    let nested = prog_entries
        .iter()
        .find(|e| e.name.eq_ignore_ascii_case("NESTED.TXT"))
        .unwrap();
    let data = fs.read_file(nested, 4096).unwrap();
    assert_eq!(&data, b"Nested under PROG.");
}

// ============================================================================
// AHDI fixture — partition-table parse + per-partition FAT round-trip
// ============================================================================

#[test]
fn ahdi_disk_detects_two_partitions_with_correct_geometry() {
    let disk = load_fixture("test_atarist_ahdi.img.zst");
    assert_eq!(disk.len(), 32 * 1024 * 1024, "AHDI fixture is 32 MiB");

    let mut cur = Cursor::new(disk);
    let table = PartitionTable::detect(&mut cur).unwrap();
    assert_eq!(table.type_name(), "AHDI");

    let parts = table.partitions();
    assert_eq!(parts.len(), 2);
    assert_eq!(parts[0].type_name, "AHDI GEM");
    assert_eq!(parts[0].start_lba, 2);
    assert_eq!(parts[0].size_bytes, 8 * 1024 * 1024);
    assert_eq!(parts[0].partition_type_byte, 0x01); // FAT12 dispatch byte
    assert!(!parts[0].is_logical);
    assert!(!parts[0].is_extended_container);

    assert_eq!(parts[1].type_name, "AHDI BGM");
    assert_eq!(parts[1].start_lba, 16386);
    assert_eq!(parts[1].size_bytes, 16 * 1024 * 1024);
    assert_eq!(parts[1].partition_type_byte, 0x06); // FAT16 dispatch byte
}

#[test]
fn ahdi_root_sector_checksum_validates() {
    use rusty_backup::partition::atari::AhdiTable;
    let disk = load_fixture("test_atarist_ahdi.img.zst");
    let table = AhdiTable::parse_root(&disk[..512]).unwrap();
    assert!(
        table.checksum_valid,
        "AHDI root checksum must validate (fixture stamped to 0x1234 word-sum)"
    );
}

#[test]
fn ahdi_gem_partition_reads_through_fat12() {
    let disk = load_fixture("test_atarist_ahdi.img.zst");
    let mut cur = Cursor::new(disk.clone());
    let table = PartitionTable::detect(&mut cur).unwrap();
    let p0 = &table.partitions()[0];

    let p0_cur = Cursor::new(disk);
    let mut fs = rusty_backup::fs::open_filesystem(
        p0_cur,
        p0.start_lba * 512,
        p0.partition_type_byte,
        p0.partition_type_string.as_deref(),
    )
    .unwrap();
    assert_eq!(fs.fs_type(), "FAT12");

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
    assert!(names.iter().any(|n| n.eq_ignore_ascii_case("HELLO.TXT")));
    assert!(names.iter().any(|n| n.eq_ignore_ascii_case("SUB")));

    let hello = entries
        .iter()
        .find(|e| e.name.eq_ignore_ascii_case("HELLO.TXT"))
        .unwrap();
    let data = fs.read_file(hello, 4096).unwrap();
    assert_eq!(&data, b"GEM partition 0 hello.");

    let sub = entries
        .iter()
        .find(|e| e.name.eq_ignore_ascii_case("SUB"))
        .unwrap();
    let sub_entries = fs.list_directory(sub).unwrap();
    let nested = sub_entries
        .iter()
        .find(|e| e.name.eq_ignore_ascii_case("NESTED.TXT"))
        .unwrap();
    let data = fs.read_file(nested, 4096).unwrap();
    assert_eq!(&data, b"P0 nested via mtools.");
}

#[test]
fn ahdi_bgm_partition_reads_through_fat16() {
    let disk = load_fixture("test_atarist_ahdi.img.zst");
    let mut cur = Cursor::new(disk.clone());
    let table = PartitionTable::detect(&mut cur).unwrap();
    let p1 = &table.partitions()[1];

    let p1_cur = Cursor::new(disk);
    let mut fs = rusty_backup::fs::open_filesystem(
        p1_cur,
        p1.start_lba * 512,
        p1.partition_type_byte,
        p1.partition_type_string.as_deref(),
    )
    .unwrap();
    assert_eq!(fs.fs_type(), "FAT16");

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let hello = entries
        .iter()
        .find(|e| e.name.eq_ignore_ascii_case("HELLO.TXT"))
        .unwrap();
    let data = fs.read_file(hello, 4096).unwrap();
    assert_eq!(&data, b"BGM partition 1 hello.");

    let sub = entries
        .iter()
        .find(|e| e.name.eq_ignore_ascii_case("SUB"))
        .unwrap();
    let sub_entries = fs.list_directory(sub).unwrap();
    let nested = sub_entries
        .iter()
        .find(|e| e.name.eq_ignore_ascii_case("NESTED.TXT"))
        .unwrap();
    let data = fs.read_file(nested, 4096).unwrap();
    assert_eq!(&data, b"P1 nested via mtools.");
}
