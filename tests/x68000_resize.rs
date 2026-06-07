//! Spine-stage-7 end-to-end test for the X68000 / Human68k HDD
//! resize path.
//!
//! Exercises the full pipeline:
//!
//!   1. Build a synthetic SASI HDD image: X68k partition table at
//!      byte 2048 + one Human68k (FAT12 BPB-compatible) partition at
//!      sector 64, seeded with a known file.
//!   2. Hand-assemble a backup folder for it (`partition-0.raw`,
//!      `x68k.json`, `metadata.json`) — the same shape the engine
//!      emits for the "PerPartition + Raw" output, but constructed
//!      directly so the test stays focused on the restore-side
//!      reconstruction path rather than the full backup pipeline.
//!   3. Run `reconstruct_disk_from_backup` with a size override that
//!      grows the partition by 2 MiB.
//!   4. Run `resize_filesystem_for` to extend the Human68k FAT to
//!      the new partition size (the per-FS resize step the
//!      `restore::mod` orchestrator runs after disk reconstruction).
//!   5. Verify:
//!      - The X68k magic + parsed table reflect the new size.
//!      - `Human68kFilesystem::open` succeeds at the new offset.
//!      - The seed file is still byte-identical after the resize.
//!
//! This covers spine stage 7 for X68000 (Wave-2). The companion
//! `tests/cli_x68000.rs` covers stages 8+9 (GUI dispatch / CLI parity)
//! against the committed .d88 fixture.

use std::io::{Cursor, Write};

use byteorder::{BigEndian, ByteOrder};

use rusty_backup::backup::metadata::{
    AlignmentMetadata, BackupLayout, BackupMetadata, PartitionMetadata,
};
use rusty_backup::fs::fat::create_blank_fat;
use rusty_backup::fs::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
use rusty_backup::fs::human68k::Human68kFilesystem;
use rusty_backup::fs::resize_filesystem_for;
use rusty_backup::partition::x68k::{
    X68kEntry, X68kPartitionTable, X68K_DEFAULT_SECTOR_SIZE, X68K_MAGIC, X68K_TABLE_OFFSET,
};
use rusty_backup::partition::PartitionSizeOverride;
use rusty_backup::rbformats::reconstruct_disk_from_backup;

/// First sector of the partition body. 64 × 512 = byte 32768 — the
/// canonical X68000 layout.
const PART_START_SECTOR: u64 = 64;
const SECTOR_SIZE: u64 = 512;

/// Build a fresh Human68k partition body of `size_bytes` and write a
/// single seed file inside it. Returns the raw partition bytes.
fn build_human68k_partition(size_bytes: u64, seed_name: &str, seed_data: &[u8]) -> Vec<u8> {
    let mut bytes = create_blank_fat(size_bytes, Some("X68KSEED")).expect("blank Human68k FAT");
    {
        let mut fs =
            Human68kFilesystem::open(Cursor::new(&mut bytes), 0).expect("open blank as Human68k");
        let root = fs.root().expect("root");
        let mut reader: &[u8] = seed_data;
        fs.create_file(
            &root,
            seed_name,
            &mut reader,
            seed_data.len() as u64,
            &CreateFileOptions::default(),
        )
        .expect("create seed file");
        fs.sync_metadata().expect("sync");
    }
    bytes
}

/// Wrap a Human68k partition body in a full SASI HDD image: X68k
/// partition table at byte 2048 + partition body at sector 64.
fn build_x68k_disk(part_bytes: &[u8]) -> Vec<u8> {
    let part_offset = PART_START_SECTOR * SECTOR_SIZE;
    let total = part_offset as usize + part_bytes.len();
    let mut disk = vec![0u8; total];

    let part_sectors = part_bytes.len() as u64 / SECTOR_SIZE;
    let disk_sectors = total as u64 / SECTOR_SIZE;

    // X68k table header: magic + disk size (sectors) + size copy + 0.
    let off = X68K_TABLE_OFFSET as usize;
    BigEndian::write_u32(&mut disk[off..off + 4], X68K_MAGIC);
    BigEndian::write_u32(&mut disk[off + 4..off + 8], disk_sectors as u32);
    BigEndian::write_u32(&mut disk[off + 8..off + 12], disk_sectors as u32);

    // Slot 0: Human partition at sector 64.
    let e_off = off + 16;
    disk[e_off..e_off + 8].copy_from_slice(b"Human   ");
    BigEndian::write_u32(&mut disk[e_off + 8..e_off + 12], PART_START_SECTOR as u32);
    BigEndian::write_u32(&mut disk[e_off + 12..e_off + 16], part_sectors as u32);

    // Drop the partition body.
    disk[part_offset as usize..].copy_from_slice(part_bytes);
    disk
}

/// Layout-preserving backup folder for an X68k disk: writes the
/// partition body as `partition-0.raw`, the parsed table as
/// `x68k.json`, and a hand-crafted `metadata.json` that routes
/// restore through the X68k branch.
fn write_backup_folder(
    dir: &std::path::Path,
    disk_total_bytes: u64,
    part_body: &[u8],
    table: &X68kPartitionTable,
) {
    let raw_path = dir.join("partition-0.raw");
    std::fs::write(&raw_path, part_body).expect("write partition-0.raw");

    let table_json = serde_json::to_string_pretty(table).expect("table json");
    std::fs::write(dir.join("x68k.json"), &table_json).expect("write x68k.json");

    let metadata = BackupMetadata {
        version: 1,
        created: "2026-06-04T00:00:00Z".to_string(),
        source_device: "synthetic-x68k-disk".to_string(),
        source_size_bytes: disk_total_bytes,
        partition_table_type: "X68k".to_string(),
        checksum_type: "sha256".to_string(),
        compression_type: "none".to_string(),
        split_size_mib: None,
        sector_by_sector: false,
        layout: BackupLayout::PerPartition,
        container: None,
        container_logical_size: None,
        container_sha1: None,
        size_policy: None,
        alignment: AlignmentMetadata {
            detected_type: "Custom".to_string(),
            first_partition_lba: PART_START_SECTOR,
            alignment_sectors: PART_START_SECTOR,
            heads: 0,
            sectors_per_track: 0,
        },
        partitions: vec![PartitionMetadata {
            index: 0,
            type_name: "X68k Human68k (Human)".to_string(),
            partition_type_byte: 0x01,
            start_lba: PART_START_SECTOR,
            start_byte: None,
            original_size_bytes: part_body.len() as u64,
            imaged_size_bytes: part_body.len() as u64,
            compressed_files: vec!["partition-0.raw".to_string()],
            checksum: String::new(),
            resized: false,
            compacted: false,
            is_logical: false,
            partition_type_string: Some("human68k".to_string()),
            minimum_size_bytes: None,
            defragmented_min_size_bytes: None,
            hfsplus_signature: None,
            defragmented_clone: false,
        }],
        bad_sectors: vec![],
        extended_container: None,
    };
    std::fs::write(
        dir.join("metadata.json"),
        serde_json::to_string_pretty(&metadata).unwrap(),
    )
    .unwrap();
}

/// End-to-end: build a 1 MiB Human68k image inside an X68k disk →
/// backup folder → restore-with-resize to a 3 MiB partition →
/// per-FS FAT resize → reopen + verify seed file survives.
#[test]
fn x68k_disk_round_trips_through_restore_with_resize() {
    const PART_BYTES_ORIG: u64 = 1024 * 1024;
    const PART_BYTES_NEW: u64 = 3 * 1024 * 1024;
    let part_offset = PART_START_SECTOR * SECTOR_SIZE;
    let disk_total_orig = part_offset + PART_BYTES_ORIG;
    let disk_total_new = part_offset + PART_BYTES_NEW;

    let seed_payload: &[u8] = b"X68000 HDD resize spine stage 7 - seed file (2026-06-04).";
    let part_body_orig = build_human68k_partition(PART_BYTES_ORIG, "HELLO.TXT", seed_payload);

    // Sanity: the body opens cleanly as Human68k before any resize.
    {
        let mut fs =
            Human68kFilesystem::open(Cursor::new(part_body_orig.clone()), 0).expect("source open");
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        assert_eq!(entries.len(), 1, "source disk has exactly HELLO.TXT");
        assert_eq!(entries[0].name, "HELLO.TXT");
        assert_eq!(fs.read_file(&entries[0], 1024).unwrap(), seed_payload);
    }

    let source_disk = build_x68k_disk(&part_body_orig);
    assert_eq!(source_disk.len() as u64, disk_total_orig);

    // Parse the table off the synthetic disk so we feed the sidecar
    // exactly what backup would have written.
    let table = X68kPartitionTable::detect(&mut Cursor::new(&source_disk))
        .expect("detect ok")
        .expect("X68k table present");
    assert_eq!(table.entries.len(), 1);
    assert_eq!(table.entries[0].start_sector, PART_START_SECTOR as u32);
    assert_eq!(
        table.entries[0].length_sectors as u64,
        PART_BYTES_ORIG / SECTOR_SIZE
    );

    let tmp = tempfile::tempdir().unwrap();
    write_backup_folder(tmp.path(), disk_total_orig, &part_body_orig, &table);

    // Reconstruct: grow the only partition from 1 MiB to 3 MiB.
    let overrides = vec![PartitionSizeOverride::size_only(
        0,
        PART_START_SECTOR,
        PART_BYTES_ORIG,
        PART_BYTES_NEW,
    )];

    let restored_path = tmp.path().join("restored.img");
    {
        let mut out = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&restored_path)
            .unwrap();
        out.set_len(disk_total_new).unwrap();
        let meta_text = std::fs::read_to_string(tmp.path().join("metadata.json")).unwrap();
        let meta: BackupMetadata = serde_json::from_str(&meta_text).unwrap();

        let mut log_buf: Vec<String> = Vec::new();
        let mut log_cb = |s: &str| log_buf.push(s.to_string());
        let mut progress_cb = |_: u64| {};
        let cancel_check = || false;
        let written = reconstruct_disk_from_backup(
            tmp.path(),
            &meta,
            None,
            &overrides,
            disk_total_new,
            &mut out,
            false,
            true,
            None,
            None,
            &mut progress_cb,
            &cancel_check,
            &mut log_cb,
        )
        .expect("reconstruct");
        assert_eq!(written, disk_total_new, "should fill the new disk size");
        out.flush().unwrap();
    }

    // After reconstruct: per-FS resize step (the orchestrator in
    // restore::mod runs this after partition data lands).
    {
        let mut out = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&restored_path)
            .unwrap();
        let mut log_buf: Vec<String> = Vec::new();
        let mut log_cb = |s: &str| log_buf.push(s.to_string());
        resize_filesystem_for(&mut out, part_offset, PART_BYTES_NEW, &mut log_cb)
            .expect("resize_filesystem_for");
    }

    // ── Verify ──
    let restored = std::fs::read(&restored_path).unwrap();
    assert_eq!(restored.len() as u64, disk_total_new);

    // X68k magic survives at byte 2048.
    assert_eq!(
        BigEndian::read_u32(&restored[X68K_TABLE_OFFSET as usize..]),
        X68K_MAGIC
    );

    // The X68k table reports the grown partition length.
    let restored_table = X68kPartitionTable::detect(&mut Cursor::new(&restored))
        .expect("detect ok")
        .expect("table present");
    assert_eq!(restored_table.entries.len(), 1);
    let e: &X68kEntry = &restored_table.entries[0];
    assert!(e.is_human68k(), "first entry is still Human68k");
    assert_eq!(e.start_sector as u64, PART_START_SECTOR);
    assert_eq!(
        e.length_sectors as u64,
        PART_BYTES_NEW / SECTOR_SIZE,
        "X68k entry length must reflect the override size"
    );

    // disk_size_field on the table refreshes to the new disk size.
    let new_disk_sectors = (disk_total_new / X68K_DEFAULT_SECTOR_SIZE) as u32;
    assert_eq!(
        restored_table.disk_size_field, new_disk_sectors,
        "disk_size_field on the X68k header refreshes to the new size"
    );

    // BPB total_sectors at the partition's first sector now reflects
    // the FAT resize. FAT12/16 stores small total at bytes 19..21 and
    // big total at bytes 32..36 — pick whichever is non-zero (same
    // convention `Human68kBpb::parse` uses).
    let part_bpb = &restored[part_offset as usize..part_offset as usize + 512];
    let small_total = u16::from_le_bytes([part_bpb[19], part_bpb[20]]) as u32;
    let big_total = u32::from_le_bytes([part_bpb[32], part_bpb[33], part_bpb[34], part_bpb[35]]);
    let observed_total = if small_total != 0 {
        small_total
    } else {
        big_total
    };
    assert_eq!(
        observed_total as u64,
        PART_BYTES_NEW / SECTOR_SIZE,
        "BPB total_sectors reflects the FAT resize",
    );

    // Human68k FS opens at the new offset; HELLO.TXT round-trips byte-exact.
    let mut fs = Human68kFilesystem::open(Cursor::new(&restored), part_offset).expect("reopen");
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, "HELLO.TXT");
    assert_eq!(fs.read_file(&entries[0], 1024).unwrap(), seed_payload);
}

/// 256-byte SASI disks place their Human68k partition on a
/// **non-512-aligned** byte (`start_sector * 256`). Restore must read,
/// place, and reopen the partition at that true byte offset — carried in
/// metadata as `start_byte` — not at the floored `start_lba * 512`.
///
/// This builds a synthetic SASI disk whose partition starts at sector 33
/// (byte 8448, which `8448 / 512 = 16` floors to byte 8192), backs it up
/// with `start_byte = 8448`, reconstructs, and asserts:
///   - the X68k table lands at the SASI offset 0x400 (derived sector size),
///   - the reconstructed partition body is byte-identical to the source,
///   - Human68k opens at byte 8448 and the seed file survives.
#[test]
fn x68k_sasi_256byte_partition_round_trips_at_true_byte_offset() {
    use rusty_backup::partition::x68k::X68K_TABLE_OFFSET_SASI;

    const SASI_SECTOR: u64 = 256;
    const START_SECTOR: u64 = 33; // 33 * 256 = 8448, NOT 512-aligned
    const PART_BYTES: u64 = 1024 * 1024;
    let start_byte = START_SECTOR * SASI_SECTOR; // 8448
    assert_ne!(start_byte % 512, 0, "test premise: non-512-aligned start");
    let floored_lba = start_byte / 512; // 16 -> *512 = 8192 (wrong place)
    let disk_total = start_byte + PART_BYTES;
    let length_sectors = PART_BYTES / SASI_SECTOR; // 4096

    let seed: &[u8] = b"X68000 SASI 256-byte-sector non-512-aligned partition seed.";
    let part_body = build_human68k_partition(PART_BYTES, "SASI.TXT", seed);

    // Sidecar table: one Human68k entry at sector 33, length in 256-B sectors.
    let table = X68kPartitionTable {
        disk_size_field: (disk_total / SASI_SECTOR) as u32,
        entries: vec![X68kEntry {
            name_raw: *b"Human68k",
            name_display: "Human68k".to_string(),
            start_sector: START_SECTOR as u32,
            length_sectors: length_sectors as u32,
        }],
    };

    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("partition-0.raw"), &part_body).unwrap();
    std::fs::write(
        tmp.path().join("x68k.json"),
        serde_json::to_string_pretty(&table).unwrap(),
    )
    .unwrap();
    let metadata = BackupMetadata {
        version: 1,
        created: "2026-06-07T00:00:00Z".to_string(),
        source_device: "synthetic-sasi-x68k".to_string(),
        source_size_bytes: disk_total,
        partition_table_type: "X68k".to_string(),
        checksum_type: "sha256".to_string(),
        compression_type: "none".to_string(),
        split_size_mib: None,
        sector_by_sector: false,
        layout: BackupLayout::PerPartition,
        container: None,
        container_logical_size: None,
        container_sha1: None,
        size_policy: None,
        alignment: AlignmentMetadata {
            detected_type: "Custom".to_string(),
            first_partition_lba: floored_lba,
            alignment_sectors: 1,
            heads: 0,
            sectors_per_track: 0,
        },
        partitions: vec![PartitionMetadata {
            index: 0,
            type_name: "X68k Human68k (Human68k)".to_string(),
            partition_type_byte: 0x01,
            start_lba: floored_lba,
            start_byte: Some(start_byte),
            original_size_bytes: PART_BYTES,
            imaged_size_bytes: PART_BYTES,
            compressed_files: vec!["partition-0.raw".to_string()],
            checksum: String::new(),
            resized: false,
            compacted: false,
            is_logical: false,
            partition_type_string: Some("human68k".to_string()),
            minimum_size_bytes: None,
            defragmented_min_size_bytes: None,
            hfsplus_signature: None,
            defragmented_clone: false,
        }],
        bad_sectors: vec![],
        extended_container: None,
    };
    std::fs::write(
        tmp.path().join("metadata.json"),
        serde_json::to_string_pretty(&metadata).unwrap(),
    )
    .unwrap();

    let restored_path = tmp.path().join("restored.hdf");
    {
        let mut out = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&restored_path)
            .unwrap();
        out.set_len(disk_total).unwrap();
        reconstruct_disk_from_backup(
            tmp.path(),
            &metadata,
            None,
            &[],
            disk_total,
            &mut out,
            false,
            true,
            None,
            None,
            &mut |_| {},
            &|| false,
            &mut |_| {},
        )
        .expect("reconstruct SASI");
        out.flush().unwrap();
    }

    let restored = std::fs::read(&restored_path).unwrap();
    assert_eq!(restored.len() as u64, disk_total);

    // The X68k table must land at the SASI offset 0x400 (sector size 256
    // derived from original_size_bytes / length_sectors), NOT 0x800.
    assert_eq!(
        BigEndian::read_u32(&restored[X68K_TABLE_OFFSET_SASI as usize..]),
        X68K_MAGIC,
        "SASI table written at 0x400"
    );

    // The partition body landed at the TRUE byte offset 8448. It's
    // byte-identical to the source except for the BPB "hidden sectors" field
    // (offset 28..32), which reconstruct legitimately patches to the
    // partition's LBA — and finding a FAT BPB there to patch is itself proof
    // the body landed at 8448.
    let placed = &restored[start_byte as usize..(start_byte + PART_BYTES) as usize];
    assert_eq!(
        &placed[..28],
        &part_body[..28],
        "BPB head byte-identical at 8448"
    );
    assert_eq!(
        &placed[32..],
        &part_body[32..],
        "body byte-identical past the patched field"
    );
    assert_eq!(
        placed[0], 0xEB,
        "FAT boot jump sits at the true offset 8448"
    );
    // The floored offset (8192) is in the gap between table and partition —
    // it must be zero-fill, not the misplaced BPB.
    assert_eq!(
        restored[floored_lba as usize * 512],
        0,
        "nothing is written at the floored 8192 offset"
    );

    // Human68k opens at the true offset and the seed file survives byte-exact.
    let mut fs = Human68kFilesystem::open(Cursor::new(&restored), start_byte).expect("reopen SASI");
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, "SASI.TXT");
    assert_eq!(fs.read_file(&entries[0], 1024).unwrap(), seed);
}

/// Round-trip without resize — useful as a smoke check that the X68k
/// reconstruct branch is byte-exact when no overrides are applied.
#[test]
fn x68k_disk_round_trips_through_restore_without_resize() {
    const PART_BYTES: u64 = 1024 * 1024;
    let part_offset = PART_START_SECTOR * SECTOR_SIZE;
    let disk_total = part_offset + PART_BYTES;

    let seed_payload: &[u8] = b"X68000 HDD: no-resize round-trip seed.";
    let part_body = build_human68k_partition(PART_BYTES, "NOTE.TXT", seed_payload);
    let _source_disk = build_x68k_disk(&part_body);

    let table_for_sidecar = {
        let synth = build_x68k_disk(&part_body);
        X68kPartitionTable::detect(&mut Cursor::new(synth))
            .unwrap()
            .unwrap()
    };

    let tmp = tempfile::tempdir().unwrap();
    write_backup_folder(tmp.path(), disk_total, &part_body, &table_for_sidecar);

    let restored_path = tmp.path().join("restored.img");
    {
        let mut out = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&restored_path)
            .unwrap();
        out.set_len(disk_total).unwrap();
        let meta_text = std::fs::read_to_string(tmp.path().join("metadata.json")).unwrap();
        let meta: BackupMetadata = serde_json::from_str(&meta_text).unwrap();
        reconstruct_disk_from_backup(
            tmp.path(),
            &meta,
            None,
            &[],
            disk_total,
            &mut out,
            false,
            true,
            None,
            None,
            &mut |_| {},
            &|| false,
            &mut |_| {},
        )
        .unwrap();
        out.flush().unwrap();
    }

    let restored = std::fs::read(&restored_path).unwrap();
    assert_eq!(restored.len() as u64, disk_total);

    // Table still parses, slot 0 length unchanged.
    let t = X68kPartitionTable::detect(&mut Cursor::new(&restored))
        .unwrap()
        .unwrap();
    assert_eq!(t.entries.len(), 1);
    assert_eq!(t.entries[0].length_sectors as u64, PART_BYTES / SECTOR_SIZE);

    // FS opens; NOTE.TXT survives.
    let mut fs = Human68kFilesystem::open(Cursor::new(&restored), part_offset).unwrap();
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, "NOTE.TXT");
    assert_eq!(fs.read_file(&entries[0], 1024).unwrap(), seed_payload);
}
