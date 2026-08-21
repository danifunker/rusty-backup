//! End-to-end wiring for SGI EFS v1 volumes behind an SGI disk label.
//!
//! The unit tests in `src/partition/sgi_dklabel.rs` and `src/fs/efs_v1.rs`
//! cover each format on its own. What this file covers is the path between
//! them: block 0 detected as a disk label, its slots enumerated with the
//! synthetic type byte, and `open_filesystem` routing that byte to the EFS v1
//! driver — in both the native and the byte-swapped orientation.
//!
//! The fixture is synthesized rather than checked in: a real IRIS 3130 disk is
//! 60 MB, and the reader is already validated against one file-for-file (see
//! `docs/SGI_EFS_v1.md`).

use byteorder::{BigEndian, ByteOrder};
use rusty_backup::fs::open_filesystem;
use rusty_backup::partition::sgi_dklabel::{swab16_in_place, SGI_DKLABEL_MAGIC};
use rusty_backup::partition::PartitionTable;
use std::io::Cursor;

const BS: usize = 512;
/// Where the one filesystem slot starts, mirroring the sample disk's block 119.
const SLOT_BASE: u32 = 119;

/// The EFS v1 geometry used throughout: firstcg 8, one 40-block group with 2
/// inode blocks, so fs_size is 48 and data starts at block 10.
const FIRSTCG: u32 = 8;
const CGFSIZE: u32 = 40;
const CGISIZE: u16 = 2;
const NCG: u16 = 1;
const FS_SIZE: u32 = FIRSTCG + NCG as u32 * CGFSIZE;

fn write_label(img: &mut [u8], slot_size: u32, total_blocks: u32) {
    BigEndian::write_u32(&mut img[0x00..0x04], SGI_DKLABEL_MAGIC);
    BigEndian::write_u16(&mut img[0x04..0x06], 1); // DT_V170
    BigEndian::write_u16(&mut img[0x06..0x08], 0); // DC_DSD5217
    BigEndian::write_u16(&mut img[0x08..0x0A], 1); // cylinders
    BigEndian::write_u16(&mut img[0x0A..0x0C], 1); // heads
    BigEndian::write_u16(&mut img[0x0C..0x0E], total_blocks as u16);
    BigEndian::write_u32(&mut img[0x0E..0x12], total_blocks);
    BigEndian::write_u16(&mut img[0x12..0x14], 0);
    img[0x14] = 0; // d_bootfs
    img[0x15] = 1; // d_swapfs
                   // Slot 0: the filesystem. Slot 1: swap. Slot 2: a whole-disk wrapper that
                   // must be filtered out of the browse list.
    for (i, (base, size)) in [
        (SLOT_BASE, slot_size),
        (SLOT_BASE + slot_size, 4u32),
        (0u32, total_blocks),
    ]
    .iter()
    .enumerate()
    {
        let o = 0x16 + i * 8;
        BigEndian::write_u32(&mut img[o..o + 4], *base);
        BigEndian::write_u32(&mut img[o + 4..o + 8], *size);
    }
    img[0x5C..0x5C + 10].copy_from_slice(b"Priam V170");
}

fn write_superblock(fs: &mut [u8]) {
    let sb = &mut fs[BS..BS + 0xA2];
    BigEndian::write_u32(&mut sb[0x00..0x04], FS_SIZE);
    BigEndian::write_u32(&mut sb[0x04..0x08], FIRSTCG);
    BigEndian::write_u32(&mut sb[0x08..0x0C], CGFSIZE);
    BigEndian::write_u16(&mut sb[0x0C..0x0E], CGISIZE);
    BigEndian::write_u16(&mut sb[0x0E..0x10], 17);
    BigEndian::write_u16(&mut sb[0x10..0x12], 7);
    BigEndian::write_u16(&mut sb[0x12..0x14], NCG);
    BigEndian::write_u32(&mut sb[0x16..0x1A], 0x21CB_DE2B);
    sb[0x1A..0x1E].copy_from_slice(b"root");
    sb[0x20..0x23].copy_from_slice(b"sgi");
    BigEndian::write_u32(&mut sb[0x26..0x2A], 0x0004_1755);
    BigEndian::write_u32(&mut sb[0x2E..0x32], 6); // fs_bmsize
    BigEndian::write_u32(&mut sb[0x32..0x36], 20); // fs_tfree
    BigEndian::write_u32(&mut sb[0x36..0x3A], 4); // fs_tinode
}

fn inode_offset(inum: u32) -> usize {
    let block = FIRSTCG + (inum / 4) % CGISIZE as u32;
    block as usize * BS + (inum % 4) as usize * 128
}

fn write_inode(fs: &mut [u8], inum: u32, mode: u16, size: u32, exts: &[(u32, u8, u32)]) {
    let o = inode_offset(inum);
    let ino = &mut fs[o..o + 128];
    BigEndian::write_u16(&mut ino[0x00..0x02], mode);
    BigEndian::write_u16(&mut ino[0x02..0x04], 2);
    BigEndian::write_u32(&mut ino[0x08..0x0C], size);
    BigEndian::write_u32(&mut ino[0x10..0x14], 0x21CB_DCA3);
    BigEndian::write_u16(&mut ino[0x1C..0x1E], exts.len() as u16);
    for (i, (bn, len, off)) in exts.iter().enumerate() {
        let eo = 0x20 + i * 8;
        BigEndian::write_u32(&mut ino[eo..eo + 4], *bn);
        BigEndian::write_u32(&mut ino[eo + 4..eo + 8], ((*len as u32) << 24) | *off);
    }
}

fn write_dir(fs: &mut [u8], bn: u32, entries: &[(u16, &str)]) {
    let base = bn as usize * BS;
    for (i, (inum, name)) in entries.iter().enumerate() {
        let o = base + i * 16;
        BigEndian::write_u16(&mut fs[o..o + 2], *inum);
        fs[o + 2..o + 2 + name.len()].copy_from_slice(name.as_bytes());
    }
}

/// A whole disk: label at block 0, one EFS v1 volume at block `SLOT_BASE`
/// holding `/hello` and `/sub/deep`.
fn build_disk() -> Vec<u8> {
    let slot_size = FS_SIZE;
    let total = SLOT_BASE + slot_size + 4;
    let mut img = vec![0u8; total as usize * BS];
    write_label(&mut img, slot_size, total);

    let base = SLOT_BASE as usize * BS;
    let fs = &mut img[base..base + FS_SIZE as usize * BS];
    write_superblock(fs);
    write_inode(fs, 2, 0o040777, 4 * 16, &[(10, 1, 0)]);
    write_dir(fs, 10, &[(2, "."), (2, ".."), (3, "hello"), (4, "sub")]);
    write_inode(fs, 3, 0o100644, 12, &[(11, 1, 0)]);
    fs[11 * BS..11 * BS + 12].copy_from_slice(b"hello world\n");
    write_inode(fs, 4, 0o040755, 3 * 16, &[(12, 1, 0)]);
    write_dir(fs, 12, &[(4, "."), (2, ".."), (5, "deep")]);
    write_inode(fs, 5, 0o100600, 5, &[(13, 1, 0)]);
    fs[13 * BS..13 * BS + 5].copy_from_slice(b"deep\n");
    img
}

/// Walk a built disk through the public API and assert what it holds.
fn assert_disk_reads(img: Vec<u8>, expect_swapped: bool) {
    let mut cursor = Cursor::new(img.clone());
    let table = PartitionTable::detect(&mut cursor).expect("disk label should be detected");
    assert_eq!(table.type_name(), "SGI-DkLabel");
    let PartitionTable::SgiDkLabel(label) = &table else {
        panic!("expected an SGI disk label, got {}", table.type_name());
    };
    assert_eq!(label.name, "Priam V170");
    assert_eq!(
        label.byte_order.display_name(),
        if expect_swapped {
            "byte-swapped"
        } else {
            "native"
        }
    );

    // Slot 2 is a whole-disk wrapper and must not be offered.
    let parts = table.partitions();
    assert_eq!(parts.len(), 2, "wrapper slot should be filtered: {parts:?}");
    assert_eq!(parts[0].type_name, "SGI root (EFS v1)");
    assert_eq!(parts[0].partition_type_byte, 0xA2);
    assert_eq!(parts[0].start_lba, SLOT_BASE as u64);
    assert!(parts[0].bootable);
    // Swap carries no filesystem, so it gets no routing byte.
    assert_eq!(parts[1].type_name, "SGI swap");
    assert_eq!(parts[1].partition_type_byte, 0);

    let mut fs = open_filesystem(
        Cursor::new(img),
        parts[0].byte_offset(),
        parts[0].partition_type_byte,
        parts[0].partition_type_string.as_deref(),
    )
    .expect("EFS v1 should open through the type byte");
    assert_eq!(fs.fs_type(), "SGI EFS v1");
    assert_eq!(fs.volume_label(), Some("root:sgi"));
    assert_eq!(fs.total_size(), FS_SIZE as u64 * BS as u64);

    let root = fs.root().unwrap();
    let mut names: Vec<String> = fs
        .list_directory(&root)
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();
    names.sort();
    assert_eq!(names, vec!["hello", "sub"]);

    let hello = fs
        .list_directory(&root)
        .unwrap()
        .into_iter()
        .find(|e| e.name == "hello")
        .unwrap();
    assert_eq!(fs.read_file(&hello, usize::MAX).unwrap(), b"hello world\n");

    let sub = fs
        .list_directory(&root)
        .unwrap()
        .into_iter()
        .find(|e| e.name == "sub")
        .unwrap();
    let deep = fs
        .list_directory(&sub)
        .unwrap()
        .into_iter()
        .find(|e| e.name == "deep")
        .unwrap();
    assert_eq!(deep.path, "/sub/deep");
    assert_eq!(deep.mode, Some(0o100600));
    assert_eq!(fs.read_file(&deep, usize::MAX).unwrap(), b"deep\n");
}

#[test]
fn native_disk_label_routes_slots_to_the_efs_v1_driver() {
    assert_disk_reads(build_disk(), false);
}

#[test]
fn byte_swapped_disk_reads_identically() {
    // How a dump off a period SGI controller actually arrives.
    let mut img = build_disk();
    swab16_in_place(&mut img);
    assert_disk_reads(img, true);
}

#[test]
fn a_bare_partition_image_is_detected_without_a_label() {
    // No disk label at all — just the filesystem, as when a slot has been
    // carved out on its own. The type byte is 0, so this exercises the
    // content probe in `detect_filesystem_type`.
    let disk = build_disk();
    let base = SLOT_BASE as usize * BS;
    let volume = disk[base..base + FS_SIZE as usize * BS].to_vec();
    for (label, img) in [
        ("native", volume.clone()),
        ("swapped", {
            let mut v = volume.clone();
            swab16_in_place(&mut v);
            v
        }),
    ] {
        let mut fs = open_filesystem(Cursor::new(img), 0, 0, None)
            .unwrap_or_else(|e| panic!("{label} bare volume should open: {e}"));
        assert_eq!(fs.fs_type(), "SGI EFS v1", "{label}");
        let root = fs.root().unwrap();
        assert_eq!(fs.list_directory(&root).unwrap().len(), 2, "{label}");
    }
}

/// A blank volume built by `create_blank_efs_v1`, opened through the public
/// editable factory and written to — the path `rb-cli put` and the GUI browse
/// view take. Proves the dispatch wiring, not just the driver internals.
#[test]
fn a_blank_volume_is_writable_through_the_public_factory() {
    use rusty_backup::fs::filesystem::{CreateDirectoryOptions, CreateFileOptions};
    use rusty_backup::fs::{efs_v1::create_blank_efs_v1, open_editable_filesystem};

    let img = create_blank_efs_v1(4 * 1024 * 1024, "e2e").unwrap();

    // A bare volume has no partition table, so it arrives as a superfloppy:
    // type byte 0 and no type string, i.e. the auto-detect path.
    let table = PartitionTable::detect(&mut Cursor::new(img.clone())).unwrap();
    match &table {
        PartitionTable::None { fs_hint, .. } => assert_eq!(fs_hint, "SGI EFS v1"),
        other => panic!("expected a superfloppy, got {other:?}"),
    }

    let mut fs = open_editable_filesystem(Cursor::new(img), 0, 0, None).unwrap();
    let root = fs.as_filesystem_mut().root().unwrap();
    let payload = b"written through the public factory".to_vec();
    fs.create_file(
        &root,
        "note",
        &mut Cursor::new(payload.clone()),
        payload.len() as u64,
        &CreateFileOptions::default(),
    )
    .unwrap();
    fs.create_directory(&root, "sub", &CreateDirectoryOptions::default())
        .unwrap();
    fs.sync_metadata().unwrap();

    let listing = fs.as_filesystem_mut().list_directory(&root).unwrap();
    let mut names: Vec<&str> = listing.iter().map(|e| e.name.as_str()).collect();
    names.sort_unstable();
    assert_eq!(names, vec!["note", "sub"]);

    let note = listing.iter().find(|e| e.name == "note").unwrap();
    let got = fs.as_filesystem_mut().read_file(note, usize::MAX).unwrap();
    assert_eq!(got, payload);
}

/// The same volume, byte-swapped the way a capture off a period controller is:
/// the synthetic type byte must still route, and writes must stay symmetric.
#[test]
fn a_byte_swapped_blank_volume_is_writable_too() {
    use rusty_backup::fs::filesystem::CreateFileOptions;
    use rusty_backup::fs::{efs_v1::create_blank_efs_v1, open_editable_filesystem};

    let mut img = create_blank_efs_v1(4 * 1024 * 1024, "e2e").unwrap();
    swab16_in_place(&mut img);

    let mut fs = open_editable_filesystem(Cursor::new(img), 0, 0, None).unwrap();
    let root = fs.as_filesystem_mut().root().unwrap();
    let payload = b"swapped write".to_vec();
    fs.create_file(
        &root,
        "swapped",
        &mut Cursor::new(payload.clone()),
        payload.len() as u64,
        &CreateFileOptions::default(),
    )
    .unwrap();
    fs.sync_metadata().unwrap();

    let listing = fs.as_filesystem_mut().list_directory(&root).unwrap();
    assert_eq!(listing.len(), 1);
    let got = fs
        .as_filesystem_mut()
        .read_file(&listing[0], usize::MAX)
        .unwrap();
    assert_eq!(got, payload);
}
