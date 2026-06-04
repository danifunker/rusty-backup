//! Spine-stage-7 end-to-end test for the QL / QDOS QXL.WIN HDD
//! resize path.
//!
//! The QL core reads QXL.WIN as a superfloppy (no partition table —
//! the whole image is one volume), so the test exercises the
//! superfloppy branch of `reconstruct_disk_from_backup` plus the new
//! `resize_qdos_in_place` per-FS resize.
//!
//! Flow:
//!   1. Build a fresh 64-cluster QXL.WIN volume (32 KiB at 512 B
//!      clusters) with one seed file.
//!   2. Hand-assemble a "PerPartition + Raw" backup folder for it.
//!   3. Run `reconstruct_disk_from_backup` with an override that
//!      grows the volume to 96 clusters (48 KiB).
//!   4. Run `resize_filesystem_for` to extend the QXL.WIN FAT.
//!   5. Verify the seed file round-trips byte-exact through the
//!      enlarged volume.
//!
//! Covers spine stage 7 for QL (Wave-2). Real-hardware boot test
//! remains in OPEN-WORK §7, alongside the existing sQLux byte-truth
//! reference cross-check (which already shipped for stage 4).

use std::io::Cursor;

use byteorder::{BigEndian, ByteOrder};

use rusty_backup::backup::metadata::{
    AlignmentMetadata, BackupLayout, BackupMetadata, PartitionMetadata,
};
use rusty_backup::fs::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
use rusty_backup::fs::qdos::{QdosFilesystem, QXLWIN_SIGNATURE};
use rusty_backup::fs::resize_filesystem_for;
use rusty_backup::partition::PartitionSizeOverride;
use rusty_backup::rbformats::reconstruct_disk_from_backup;

const SECTOR_BYTES: usize = 512;
const HEADER_BYTES: usize = 64;
const SPC: u16 = 1;
const CLUSTER_BYTES: usize = SPC as usize * SECTOR_BYTES;

/// Build a fresh QXL.WIN volume with `cc` clusters of 512 B each.
/// Cluster 0 holds the header + FAT, cluster 1 is the root directory
/// (single 64-byte self-reference slot, `rlen = 64`), clusters
/// [2, cc) are on the free chain in ascending order.
fn build_qxlwin(cc: u16, label: &str) -> Vec<u8> {
    assert!(
        cc >= 4,
        "minimum volume needs cluster 0/1 + at least 2 free"
    );

    let mut disk = vec![0u8; cc as usize * CLUSTER_BYTES];

    // Header.
    disk[0..4].copy_from_slice(QXLWIN_SIGNATURE);
    BigEndian::write_u16(&mut disk[0x04..0x06], 0x0005);
    let mut name = [b' '; 20];
    let bytes = label.as_bytes();
    let n = bytes.len().min(20);
    name[..n].copy_from_slice(&bytes[..n]);
    disk[0x06..0x1A].copy_from_slice(&name);
    BigEndian::write_u16(&mut disk[0x22..0x24], SPC);
    BigEndian::write_u16(&mut disk[0x2A..0x2C], cc);
    BigEndian::write_u16(&mut disk[0x2C..0x2E], cc - 2); // fc = cc - 2 (cluster 0+1 reserved)
    BigEndian::write_u16(&mut disk[0x32..0x34], 2); // ffc head = first free
    BigEndian::write_u16(&mut disk[0x34..0x36], 1); // root_cluster
    BigEndian::write_u32(&mut disk[0x36..0x3A], 64); // rlen = 1 slot (the self-ref)

    // FAT at byte 0x40. Slot conventions:
    //   - cluster 0: self-chain anchor; FAT region only spans cluster 0
    //     for this size, so slot[0] = 0.
    //   - cluster 1 (root dir): chain-tail (0).
    //   - clusters 2..cc-1: free chain links (k → k+1).
    //   - cluster cc-1: chain-tail (0).
    let fat_off = HEADER_BYTES;
    let set = |d: &mut [u8], cl: u16, val: u16| {
        let o = fat_off + cl as usize * 2;
        BigEndian::write_u16(&mut d[o..o + 2], val);
    };
    set(&mut disk, 0, 0);
    set(&mut disk, 1, 0);
    for k in 2..(cc - 1) {
        set(&mut disk, k, k + 1);
    }
    set(&mut disk, cc - 1, 0);

    // Root directory slot 0: self-reference. file_length=0, name_len=0
    // (parse_dir_entry rejects this slot per existing convention; QDOS
    // dir enumeration skips slot 0). Leaving all zeros is the
    // canonical encoding.

    disk
}

/// Layout-preserving backup folder for a QXL.WIN superfloppy:
/// `partition-0.raw` + minimal `metadata.json`.
fn write_backup_folder(dir: &std::path::Path, body: &[u8]) {
    std::fs::write(dir.join("partition-0.raw"), body).expect("partition-0.raw");
    let metadata = BackupMetadata {
        version: 1,
        created: "2026-06-04T00:00:00Z".to_string(),
        source_device: "synthetic-qxlwin-disk".to_string(),
        source_size_bytes: body.len() as u64,
        partition_table_type: "None".to_string(),
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
            detected_type: "None detected".to_string(),
            first_partition_lba: 0,
            alignment_sectors: 1,
            heads: 0,
            sectors_per_track: 0,
        },
        partitions: vec![PartitionMetadata {
            index: 0,
            type_name: "QDOS (QXL.WIN)".to_string(),
            partition_type_byte: 0,
            start_lba: 0,
            original_size_bytes: body.len() as u64,
            imaged_size_bytes: body.len() as u64,
            compressed_files: vec!["partition-0.raw".to_string()],
            checksum: String::new(),
            resized: false,
            compacted: false,
            is_logical: false,
            partition_type_string: Some("qdos".to_string()),
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

/// End-to-end: 64-cluster QXL.WIN -> backup folder -> restore-with-
/// resize to 96 clusters -> per-FS resize -> reopen + verify the seed
/// file's bytes survive.
#[test]
fn qxlwin_round_trips_through_restore_with_resize() {
    const OLD_CC: u16 = 64;
    const NEW_CC: u16 = 96;

    // 1. Build the blank, seed a known file via the real
    //    EditableFilesystem path so the FAT + directory + per-file
    //    header layout matches the production write path.
    let seed: &[u8] = b"QL QXL.WIN spine stage 7 round-trip test seed (2026-06-04).";
    let mut body = build_qxlwin(OLD_CC, "SEEDVOL");
    {
        let mut fs =
            QdosFilesystem::open(Cursor::new(&mut body), 0).expect("open synthetic QXL.WIN");
        let root = fs.root().unwrap();
        let mut reader: &[u8] = seed;
        fs.create_file(
            &root,
            "HELLO_QL",
            &mut reader,
            seed.len() as u64,
            &CreateFileOptions::default(),
        )
        .expect("create seed file");
        fs.sync_metadata().expect("sync");
    }
    // Sanity: the seed reads back from the synthetic before any
    // resize plumbing.
    {
        let mut fs = QdosFilesystem::open(Cursor::new(&body), 0).unwrap();
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        assert!(entries.iter().any(|e| e.name == "HELLO_QL"));
        let got = fs
            .read_file(entries.iter().find(|e| e.name == "HELLO_QL").unwrap(), 4096)
            .unwrap();
        assert_eq!(got, seed);
    }

    let tmp = tempfile::tempdir().unwrap();
    write_backup_folder(tmp.path(), &body);

    // 3. Restore with size override: grow to NEW_CC clusters.
    let new_size_bytes = NEW_CC as u64 * CLUSTER_BYTES as u64;
    let overrides = vec![PartitionSizeOverride::size_only(
        0,
        0,
        body.len() as u64,
        new_size_bytes,
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
        out.set_len(new_size_bytes).unwrap();
        let meta_text = std::fs::read_to_string(tmp.path().join("metadata.json")).unwrap();
        let meta: BackupMetadata = serde_json::from_str(&meta_text).unwrap();
        reconstruct_disk_from_backup(
            tmp.path(),
            &meta,
            None,
            &overrides,
            new_size_bytes,
            &mut out,
            false,
            true,
            None,
            None,
            &mut |_: u64| {},
            &|| false,
            &mut |_| {},
        )
        .expect("reconstruct");
    }

    // 4. Per-FS resize.
    {
        let mut out = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&restored_path)
            .unwrap();
        let mut log = Vec::<String>::new();
        resize_filesystem_for(&mut out, 0, new_size_bytes, &mut |s| {
            log.push(s.to_string())
        })
        .expect("resize_filesystem_for");
        assert!(
            log.iter().any(|m| m.contains("QDOS: resized")),
            "QDOS resize line should appear in the dispatcher log, got {:?}",
            log
        );
    }

    // 5. Verify.
    let restored = std::fs::read(&restored_path).unwrap();
    assert_eq!(restored.len() as u64, new_size_bytes);

    let mut fs = QdosFilesystem::open(Cursor::new(&restored), 0).expect("reopen");
    assert_eq!(fs.header.cc, NEW_CC, "cc reflects the resize");
    // The seed file is still in the catalogue.
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let hello = entries
        .iter()
        .find(|e| e.name == "HELLO_QL")
        .expect("seed file in dir post-resize");
    let got = fs.read_file(hello, 4096).unwrap();
    assert_eq!(got, seed, "seed bytes round-trip after resize");

    // Free count must have grown by the number of clusters we added
    // (every new tail cluster goes onto the free chain). The seed
    // file still occupies its one cluster, the FAT footprint hasn't
    // changed, so the only diff is the new tail.
    let added = (NEW_CC - OLD_CC) as u64;
    // 64-cluster blank: 2 reserved + (cc-2) free; seed file consumes
    // 1 cluster. Pre-resize free = OLD_CC - 3.
    let expected_free = (OLD_CC as u64 - 3) + added;
    assert_eq!(
        fs.header.fc as u64, expected_free,
        "post-resize free count = pre-resize free + new tail",
    );
    // ffc must point to a real cluster within bounds (or 0 if empty).
    assert!(
        fs.header.ffc < NEW_CC,
        "ffc must point inside the new cluster range"
    );
}
