//! End-to-end cover for the path the GUI's **Resize Partitions...**
//! button takes on an IRIX disk: `compute_resize_plan` → `apply_resize`
//! → `patch_filesystem_size` → `efs_resize::resize_efs_in_place`.
//!
//! The per-FS resize has unit tests of its own; what this adds is proof
//! that the *partition* half works for the SGI volume header too, so
//! shrinking and growing an EFS root partition is a real GUI flow and
//! not just a `rb-cli resize` one. Both halves have to agree — the
//! partition entry in the volume header and the EFS superblock inside
//! it — or IRIX sees a filesystem that disagrees with its slice.
//!
//! Flow:
//!   1. Build a 200 MiB dvh + EFS-root HDD image.
//!   2. Seed a file so the shrink has live data to preserve.
//!   3. Shrink the EFS partition to 100 MiB through the GUI's planner
//!      and applier; check the volume header, the superblock, fsck, and
//!      the seed file.
//!   4. Grow it back to the full disk and check all four again.

use std::io::{Cursor, Read, Seek, SeekFrom};

use rusty_backup::fs::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
use rusty_backup::partition::resize::{apply_resize, compute_resize_plan};
use rusty_backup::partition::sgi_hdd_builder::{build_sgi_efs_hdd, SgiHddOptions};
use rusty_backup::partition::PartitionTable;

const SEED_NAME: &str = "seed.bin";
/// Big enough to need indirect extents, so the resize paths are
/// exercised against a file whose extent list does not fit in the
/// inode — the shape a real IRIX root partition has.
const SEED_LEN: usize = 2 * 1024 * 1024;

fn seed_payload() -> Vec<u8> {
    (0..SEED_LEN).map(|i| (i % 251) as u8).collect()
}

/// The EFS partition's offset + size, straight from the SGI volume header.
fn efs_slice(img: &[u8]) -> (u64, u64) {
    let table = PartitionTable::detect(&mut Cursor::new(img)).expect("parse SGI volume header");
    assert!(
        matches!(table, PartitionTable::Sgi(_)),
        "expected an SGI volume header"
    );
    let part = table
        .partitions()
        .into_iter()
        .find(|p| p.partition_type_byte == 0xA1)
        .expect("EFS partition");
    (part.start_lba * 512, part.size_bytes)
}

/// Read the EFS superblock's `fs_size` (blocks) at `offset`.
fn efs_fs_size_blocks(img: &[u8], offset: u64) -> u32 {
    let sb = (offset + 512) as usize;
    u32::from_be_bytes(img[sb..sb + 4].try_into().unwrap())
}

/// Open the EFS volume, fsck it, and confirm the seed file is intact.
fn assert_healthy(img: &[u8], offset: u64) {
    let mut fs = rusty_backup::fs::efs::EfsFilesystem::open(Cursor::new(img), offset)
        .expect("open EFS after resize");
    let report = fs
        .fsck()
        .expect("EFS exposes an fsck")
        .expect("fsck runs clean");
    assert!(
        report.errors.is_empty(),
        "fsck errors after resize: {:?}",
        report.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
    );

    let root = Filesystem::root(&mut fs).expect("root");
    let entries = fs.list_directory(&root).expect("list root");
    let seed = entries
        .iter()
        .find(|e| e.name == SEED_NAME)
        .unwrap_or_else(|| panic!("{SEED_NAME} vanished across the resize"));
    assert_eq!(seed.size, SEED_LEN as u64);
    assert_eq!(
        fs.read_file(seed, usize::MAX).expect("read seed"),
        seed_payload(),
        "seed file content changed across the resize"
    );
}

/// Run the GUI's planner + applier for "set the EFS partition to
/// `new_size` bytes" and return the rewritten image.
fn gui_resize(img: Vec<u8>, new_size: u64) -> Vec<u8> {
    let disk_size = img.len() as u64;
    let table = PartitionTable::detect(&mut Cursor::new(&img)).expect("parse table");
    let partitions = table.partitions();
    let efs = partitions
        .iter()
        .find(|p| p.partition_type_byte == 0xA1)
        .expect("EFS partition");

    let plans = compute_resize_plan(&partitions, &[(efs.index, new_size)], 0, disk_size)
        .expect("compute resize plan");

    let mut cur = Cursor::new(img);
    let mut log = |_: &str| {};
    let mut progress = |_: u64, _: u64| {};
    apply_resize(
        &mut cur,
        &plans,
        &table,
        false, // not a device
        false, // not a VHD
        disk_size,
        &mut progress,
        &mut log,
    )
    .expect("apply resize");
    cur.into_inner()
}

/// Build the 200 MiB IRIX disk and drop a seed file in its root.
fn build_seeded_disk() -> Vec<u8> {
    let opts = SgiHddOptions::new(200 * 1024 * 1024, "resize");
    let (img, _layout) = build_sgi_efs_hdd(&opts).expect("build dvh + EFS HDD");
    let (offset, _size) = efs_slice(&img);

    let mut fs =
        rusty_backup::fs::efs::EfsFilesystem::open(Cursor::new(img), offset).expect("open EFS");
    let root = Filesystem::root(&mut fs).expect("root");
    let payload = seed_payload();
    fs.create_file(
        &root,
        SEED_NAME,
        &mut Cursor::new(payload.clone()),
        payload.len() as u64,
        &CreateFileOptions::default(),
    )
    .expect("seed the volume");
    EditableFilesystem::sync_metadata(&mut fs).expect("sync");

    let mut inner = fs.reader_into_inner();
    inner.seek(SeekFrom::Start(0)).expect("rewind");
    let mut out = Vec::new();
    inner.read_to_end(&mut out).expect("read back image");
    out
}

#[test]
fn gui_resize_shrinks_then_grows_an_efs_root_partition() {
    let img = build_seeded_disk();
    let (offset, original_size) = efs_slice(&img);
    let original_fs_size = efs_fs_size_blocks(&img, offset);
    assert_healthy(&img, offset);

    // --- Shrink to 100 MiB -------------------------------------------------
    let shrunk = gui_resize(img, 100 * 1024 * 1024);
    let (new_offset, new_size) = efs_slice(&shrunk);
    assert_eq!(
        new_offset, offset,
        "the EFS partition should not have moved"
    );
    assert!(
        new_size < original_size,
        "volume-header entry did not shrink: {new_size} vs {original_size}"
    );

    // The filesystem must have followed the partition entry down, and
    // must still fit inside it. EFS gives space back a whole cylinder
    // group at a time, so `fs_size` lands at or below the request.
    let shrunk_fs_size = efs_fs_size_blocks(&shrunk, new_offset);
    assert!(
        shrunk_fs_size < original_fs_size,
        "EFS superblock still claims {shrunk_fs_size} blocks (was {original_fs_size}) — \
         the partition entry shrank without the filesystem"
    );
    assert!(
        shrunk_fs_size as u64 * 512 <= new_size,
        "EFS claims {shrunk_fs_size} blocks, past its {new_size}-byte partition"
    );
    assert_healthy(&shrunk, new_offset);

    // --- Grow back to the original size ------------------------------------
    let grown = gui_resize(shrunk, original_size);
    let (grown_offset, grown_size) = efs_slice(&grown);
    assert_eq!(grown_size, original_size, "partition did not grow back");
    let grown_fs_size = efs_fs_size_blocks(&grown, grown_offset);
    assert!(
        grown_fs_size > shrunk_fs_size,
        "EFS superblock did not follow the partition back up"
    );
    assert!(
        grown_fs_size as u64 * 512 <= grown_size,
        "EFS claims {grown_fs_size} blocks, past its {grown_size}-byte partition"
    );
    assert_healthy(&grown, grown_offset);
}
