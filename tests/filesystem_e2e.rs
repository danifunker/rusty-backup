//! End-to-end tests using real filesystem images.
//!
//! Test fixtures are zstd-compressed images checked into tests/fixtures/.
//! They were generated on Linux using mkfs tools and populated with test files.
//!
//! Unix filesystems (ext2/ext4/btrfs) contain:
//!   hello.txt         — "Hello, <fstype>!"
//!   subdir/           — directory (mode 755)
//!   subdir/nested.txt — "nested file"
//!   link.txt          — symlink -> hello.txt
//!
//! Non-Unix filesystems (FAT12/16/32, NTFS, exFAT, HFS, HFS+) contain:
//!   hello.txt         — "Hello, <fstype>!"
//!   subdir/           — directory
//!   subdir/nested.txt — "nested file"
//!
//! Run with: cargo test --test filesystem_e2e

use rusty_backup::fs::filesystem::Filesystem;
use std::io::{Cursor, Read};

/// Decompress a zstd-compressed fixture file into an in-memory Vec<u8>.
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
// Test Group A: ext2 browsing
// ============================================================================

#[test]
fn test_ext2_browse_root() {
    let img = load_fixture("test_ext2.img.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::ext::ExtFilesystem::open(cursor, 0).unwrap();

    assert_eq!(fs.fs_type(), "ext2");
    assert_eq!(fs.volume_label(), Some("test_ext2"));

    let root = fs.root().unwrap();
    assert!(root.is_directory());

    let entries = fs.list_directory(&root).unwrap();
    let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
    assert!(
        names.contains(&"hello.txt"),
        "missing hello.txt in {names:?}"
    );
    assert!(names.contains(&"subdir"), "missing subdir in {names:?}");
    assert!(names.contains(&"link.txt"), "missing link.txt in {names:?}");
}

#[test]
fn test_ext2_read_file() {
    let img = load_fixture("test_ext2.img.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::ext::ExtFilesystem::open(cursor, 0).unwrap();

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let hello = entries.iter().find(|e| e.name == "hello.txt").unwrap();

    let data = fs.read_file(hello, 1024).unwrap();
    assert_eq!(&data, b"Hello, ext2!");
}

#[test]
fn test_ext2_symlink() {
    let img = load_fixture("test_ext2.img.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::ext::ExtFilesystem::open(cursor, 0).unwrap();

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let link = entries.iter().find(|e| e.name == "link.txt").unwrap();

    assert!(link.is_symlink());
    assert_eq!(link.symlink_target.as_deref(), Some("hello.txt"));
}

#[test]
fn test_ext2_permissions() {
    let img = load_fixture("test_ext2.img.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::ext::ExtFilesystem::open(cursor, 0).unwrap();

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let hello = entries.iter().find(|e| e.name == "hello.txt").unwrap();

    assert!(hello.uid.is_some(), "uid should be set");
    assert!(hello.gid.is_some(), "gid should be set");
    // Mode should be 0o100644 (regular file, rw-r--r--)
    assert_eq!(hello.mode.map(|m| m & 0o777), Some(0o644));
}

#[test]
fn test_ext2_nested_directory() {
    let img = load_fixture("test_ext2.img.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::ext::ExtFilesystem::open(cursor, 0).unwrap();

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let subdir = entries.iter().find(|e| e.name == "subdir").unwrap();
    assert!(subdir.is_directory());

    let sub_entries = fs.list_directory(subdir).unwrap();
    let nested = sub_entries.iter().find(|e| e.name == "nested.txt").unwrap();
    let data = fs.read_file(nested, 1024).unwrap();
    assert_eq!(&data, b"nested file");
}

// ============================================================================
// Test Group B: ext4 browsing
// ============================================================================

#[test]
fn test_ext4_browse_root() {
    let img = load_fixture("test_ext4.img.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::ext::ExtFilesystem::open(cursor, 0).unwrap();

    assert_eq!(fs.fs_type(), "ext4");
    assert_eq!(fs.volume_label(), Some("test_ext4"));

    let root = fs.root().unwrap();
    assert!(root.is_directory());

    let entries = fs.list_directory(&root).unwrap();
    let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
    assert!(
        names.contains(&"hello.txt"),
        "missing hello.txt in {names:?}"
    );
    assert!(names.contains(&"subdir"), "missing subdir in {names:?}");
    assert!(names.contains(&"link.txt"), "missing link.txt in {names:?}");
}

#[test]
fn test_ext4_read_file() {
    let img = load_fixture("test_ext4.img.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::ext::ExtFilesystem::open(cursor, 0).unwrap();

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let hello = entries.iter().find(|e| e.name == "hello.txt").unwrap();

    let data = fs.read_file(hello, 1024).unwrap();
    assert_eq!(&data, b"Hello, ext4!");
}

#[test]
fn test_ext4_symlink() {
    let img = load_fixture("test_ext4.img.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::ext::ExtFilesystem::open(cursor, 0).unwrap();

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let link = entries.iter().find(|e| e.name == "link.txt").unwrap();

    assert!(link.is_symlink());
    assert_eq!(link.symlink_target.as_deref(), Some("hello.txt"));
}

#[test]
fn test_ext4_permissions() {
    let img = load_fixture("test_ext4.img.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::ext::ExtFilesystem::open(cursor, 0).unwrap();

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let hello = entries.iter().find(|e| e.name == "hello.txt").unwrap();

    assert!(hello.uid.is_some(), "uid should be set");
    assert!(hello.gid.is_some(), "gid should be set");
    assert_eq!(hello.mode.map(|m| m & 0o777), Some(0o644));
}

#[test]
fn test_ext4_nested_directory() {
    let img = load_fixture("test_ext4.img.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::ext::ExtFilesystem::open(cursor, 0).unwrap();

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let subdir = entries.iter().find(|e| e.name == "subdir").unwrap();
    assert!(subdir.is_directory());

    let sub_entries = fs.list_directory(subdir).unwrap();
    let nested = sub_entries.iter().find(|e| e.name == "nested.txt").unwrap();
    let data = fs.read_file(nested, 1024).unwrap();
    assert_eq!(&data, b"nested file");
}

// ============================================================================
// Test Group C: btrfs browsing
// ============================================================================

#[test]
fn test_btrfs_browse_root() {
    let img = load_fixture("test_btrfs.img.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::btrfs::BtrfsFilesystem::open(cursor, 0).unwrap();

    assert_eq!(fs.fs_type(), "btrfs");
    assert_eq!(fs.volume_label(), Some("test_btrfs"));

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
    assert!(
        names.contains(&"hello.txt"),
        "missing hello.txt in {names:?}"
    );
    assert!(names.contains(&"subdir"), "missing subdir in {names:?}");
    assert!(names.contains(&"link.txt"), "missing link.txt in {names:?}");
}

#[test]
fn test_btrfs_read_file() {
    let img = load_fixture("test_btrfs.img.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::btrfs::BtrfsFilesystem::open(cursor, 0).unwrap();

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let hello = entries.iter().find(|e| e.name == "hello.txt").unwrap();

    let data = fs.read_file(hello, 1024).unwrap();
    assert_eq!(&data, b"Hello, btrfs!");
}

#[test]
fn test_btrfs_symlink() {
    let img = load_fixture("test_btrfs.img.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::btrfs::BtrfsFilesystem::open(cursor, 0).unwrap();

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let link = entries.iter().find(|e| e.name == "link.txt").unwrap();

    assert!(link.is_symlink());
    assert_eq!(link.symlink_target.as_deref(), Some("hello.txt"));
}

#[test]
fn test_btrfs_nested_directory() {
    let img = load_fixture("test_btrfs.img.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::btrfs::BtrfsFilesystem::open(cursor, 0).unwrap();

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let subdir = entries.iter().find(|e| e.name == "subdir").unwrap();
    assert!(subdir.is_directory());

    let sub_entries = fs.list_directory(subdir).unwrap();
    let nested = sub_entries.iter().find(|e| e.name == "nested.txt").unwrap();
    let data = fs.read_file(nested, 1024).unwrap();
    assert_eq!(&data, b"nested file");
}

// ============================================================================
// Test Group D: ext compaction round-trip
// ============================================================================

#[test]
fn test_ext4_compaction_round_trip() {
    let img = load_fixture("test_ext4.img.zst");
    let original_size = img.len();
    let cursor = Cursor::new(img);

    // Compact
    let (mut compact, info) = rusty_backup::fs::CompactExtReader::new(cursor, 0).unwrap();
    assert!(info.compacted_size <= original_size as u64);

    // Read compacted output
    let mut output = Vec::new();
    compact.read_to_end(&mut output).unwrap();

    // Verify the compacted image is still browsable
    let cursor = Cursor::new(output);
    let mut fs = rusty_backup::fs::ext::ExtFilesystem::open(cursor, 0).unwrap();
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let hello = entries.iter().find(|e| e.name == "hello.txt").unwrap();
    let data = fs.read_file(hello, 1024).unwrap();
    assert_eq!(&data, b"Hello, ext4!");
}

// ============================================================================
// Test Group E: btrfs compaction round-trip
// ============================================================================

#[test]
fn test_btrfs_compaction_round_trip() {
    let img = load_fixture("test_btrfs.img.zst");
    let original_size = img.len();
    let cursor = Cursor::new(img);

    // Compact
    let (mut compact, info) = rusty_backup::fs::CompactBtrfsReader::new(cursor, 0).unwrap();
    assert!(info.compacted_size <= original_size as u64);

    // Read compacted output
    let mut output = Vec::new();
    compact.read_to_end(&mut output).unwrap();

    // Verify the compacted image is still browsable
    let cursor = Cursor::new(output);
    let mut fs = rusty_backup::fs::btrfs::BtrfsFilesystem::open(cursor, 0).unwrap();
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let hello = entries.iter().find(|e| e.name == "hello.txt").unwrap();
    let data = fs.read_file(hello, 1024).unwrap();
    assert_eq!(&data, b"Hello, btrfs!");
}

// ============================================================================
// Test Group F: ext resize
// ============================================================================

#[test]
fn test_ext4_resize_grows() {
    let mut img = load_fixture("test_ext4.img.zst");
    let original_size = img.len();
    let new_size = original_size * 2;
    img.resize(new_size, 0);

    let mut cursor = Cursor::new(img);
    rusty_backup::fs::resize_ext_in_place(&mut cursor, 0, new_size as u64, &mut |msg| {
        eprintln!("  resize: {msg}");
    })
    .unwrap();

    // Validate
    cursor.set_position(0);
    let warnings = rusty_backup::fs::validate_ext_integrity(&mut cursor, 0, &mut |msg| {
        eprintln!("  validate: {msg}");
    })
    .unwrap();
    assert!(warnings.is_empty(), "warnings: {warnings:?}");

    // Should still be browsable
    let img = cursor.into_inner();
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::ext::ExtFilesystem::open(cursor, 0).unwrap();
    assert_eq!(fs.total_size(), new_size as u64);
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    assert!(entries.iter().any(|e| e.name == "hello.txt"));
}

// ============================================================================
// Test Group G: btrfs resize
// ============================================================================

#[test]
fn test_btrfs_resize_grows() {
    let mut img = load_fixture("test_btrfs.img.zst");
    let original_size = img.len();
    let new_size = original_size + 64 * 1024 * 1024; // grow by 64 MiB
    img.resize(new_size, 0);

    let mut cursor = Cursor::new(img);
    rusty_backup::fs::resize_btrfs_in_place(&mut cursor, 0, new_size as u64, &mut |msg| {
        eprintln!("  resize: {msg}");
    })
    .unwrap();

    // Validate
    cursor.set_position(0);
    let warnings = rusty_backup::fs::validate_btrfs_integrity(&mut cursor, 0, &mut |msg| {
        eprintln!("  validate: {msg}");
    })
    .unwrap();
    assert!(warnings.is_empty(), "warnings: {warnings:?}");

    // Should still be browsable with new total_bytes
    let img = cursor.into_inner();
    let cursor = Cursor::new(img);
    let fs = rusty_backup::fs::btrfs::BtrfsFilesystem::open(cursor, 0).unwrap();
    assert_eq!(fs.total_size(), new_size as u64);
}

// ============================================================================
// Test Group H: Filesystem detection routing
// ============================================================================

#[test]
fn test_detect_ext2_via_0x83() {
    let img = load_fixture("test_ext2.img.zst");
    let cursor = Cursor::new(img);
    let fs = rusty_backup::fs::open_filesystem(cursor, 0, 0x83, None).unwrap();
    assert_eq!(fs.fs_type(), "ext2");
}

#[test]
fn test_detect_ext4_via_0x83() {
    let img = load_fixture("test_ext4.img.zst");
    let cursor = Cursor::new(img);
    let fs = rusty_backup::fs::open_filesystem(cursor, 0, 0x83, None).unwrap();
    assert_eq!(fs.fs_type(), "ext4");
}

#[test]
fn test_detect_btrfs_via_0x83() {
    let img = load_fixture("test_btrfs.img.zst");
    let cursor = Cursor::new(img);
    let fs = rusty_backup::fs::open_filesystem(cursor, 0, 0x83, None).unwrap();
    assert_eq!(fs.fs_type(), "btrfs");
}

#[test]
fn test_detect_ext4_auto() {
    let img = load_fixture("test_ext4.img.zst");
    let cursor = Cursor::new(img);
    let fs = rusty_backup::fs::open_filesystem(cursor, 0, 0x00, None).unwrap();
    assert_eq!(fs.fs_type(), "ext4");
}

#[test]
fn test_detect_btrfs_auto() {
    let img = load_fixture("test_btrfs.img.zst");
    let cursor = Cursor::new(img);
    let fs = rusty_backup::fs::open_filesystem(cursor, 0, 0x00, None).unwrap();
    assert_eq!(fs.fs_type(), "btrfs");
}

/// Helper: find a file entry by name, case-insensitive (needed for FAT 8.3 names).
fn find_entry_ci<'a>(
    entries: &'a [rusty_backup::fs::entry::FileEntry],
    name: &str,
) -> Option<&'a rusty_backup::fs::entry::FileEntry> {
    let lower = name.to_lowercase();
    entries.iter().find(|e| e.name.to_lowercase() == lower)
}

/// Helper: check if entries contain a name (case-insensitive).
fn has_entry_ci(entries: &[rusty_backup::fs::entry::FileEntry], name: &str) -> bool {
    find_entry_ci(entries, name).is_some()
}

// ============================================================================
// Test Group I: FAT12 browsing
// ============================================================================

#[test]
fn test_fat12_browse_root() {
    let img = load_fixture("test_fat12.img.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::fat::FatFilesystem::open(cursor, 0).unwrap();

    assert_eq!(fs.fs_type(), "FAT12");
    assert_eq!(fs.volume_label(), Some("TEST_FAT12"));

    let root = fs.root().unwrap();
    assert!(root.is_directory());

    let entries = fs.list_directory(&root).unwrap();
    assert!(has_entry_ci(&entries, "hello.txt"), "missing hello.txt");
    assert!(has_entry_ci(&entries, "subdir"), "missing subdir");
}

#[test]
fn test_fat12_read_file() {
    let img = load_fixture("test_fat12.img.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::fat::FatFilesystem::open(cursor, 0).unwrap();

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let hello = find_entry_ci(&entries, "hello.txt").unwrap();

    let data = fs.read_file(hello, 1024).unwrap();
    assert_eq!(&data, b"Hello, fat12!");
}

#[test]
fn test_fat12_nested_directory() {
    let img = load_fixture("test_fat12.img.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::fat::FatFilesystem::open(cursor, 0).unwrap();

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let subdir = find_entry_ci(&entries, "subdir").unwrap();
    assert!(subdir.is_directory());

    let sub_entries = fs.list_directory(subdir).unwrap();
    let nested = find_entry_ci(&sub_entries, "nested.txt").unwrap();
    let data = fs.read_file(nested, 1024).unwrap();
    assert_eq!(&data, b"nested file");
}

// ============================================================================
// Test Group J: FAT16 browsing
// ============================================================================

#[test]
fn test_fat16_browse_root() {
    let img = load_fixture("test_fat16.img.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::fat::FatFilesystem::open(cursor, 0).unwrap();

    assert_eq!(fs.fs_type(), "FAT16");
    assert_eq!(fs.volume_label(), Some("TEST_FAT16"));

    let root = fs.root().unwrap();
    assert!(root.is_directory());

    let entries = fs.list_directory(&root).unwrap();
    assert!(has_entry_ci(&entries, "hello.txt"), "missing hello.txt");
    assert!(has_entry_ci(&entries, "subdir"), "missing subdir");
}

#[test]
fn test_fat16_read_file() {
    let img = load_fixture("test_fat16.img.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::fat::FatFilesystem::open(cursor, 0).unwrap();

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let hello = find_entry_ci(&entries, "hello.txt").unwrap();

    let data = fs.read_file(hello, 1024).unwrap();
    assert_eq!(&data, b"Hello, fat16!");
}

#[test]
fn test_fat16_nested_directory() {
    let img = load_fixture("test_fat16.img.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::fat::FatFilesystem::open(cursor, 0).unwrap();

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let subdir = find_entry_ci(&entries, "subdir").unwrap();
    assert!(subdir.is_directory());

    let sub_entries = fs.list_directory(subdir).unwrap();
    let nested = find_entry_ci(&sub_entries, "nested.txt").unwrap();
    let data = fs.read_file(nested, 1024).unwrap();
    assert_eq!(&data, b"nested file");
}

// ============================================================================
// Test Group K: FAT32 browsing
// ============================================================================

#[test]
fn test_fat32_browse_root() {
    let img = load_fixture("test_fat32.img.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::fat::FatFilesystem::open(cursor, 0).unwrap();

    assert_eq!(fs.fs_type(), "FAT32");
    assert_eq!(fs.volume_label(), Some("TEST_FAT32"));

    let root = fs.root().unwrap();
    assert!(root.is_directory());

    let entries = fs.list_directory(&root).unwrap();
    assert!(has_entry_ci(&entries, "hello.txt"), "missing hello.txt");
    assert!(has_entry_ci(&entries, "subdir"), "missing subdir");
}

#[test]
fn test_fat32_read_file() {
    let img = load_fixture("test_fat32.img.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::fat::FatFilesystem::open(cursor, 0).unwrap();

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let hello = find_entry_ci(&entries, "hello.txt").unwrap();

    let data = fs.read_file(hello, 1024).unwrap();
    assert_eq!(&data, b"Hello, fat32!");
}

#[test]
fn test_fat32_nested_directory() {
    let img = load_fixture("test_fat32.img.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::fat::FatFilesystem::open(cursor, 0).unwrap();

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let subdir = find_entry_ci(&entries, "subdir").unwrap();
    assert!(subdir.is_directory());

    let sub_entries = fs.list_directory(subdir).unwrap();
    let nested = find_entry_ci(&sub_entries, "nested.txt").unwrap();
    let data = fs.read_file(nested, 1024).unwrap();
    assert_eq!(&data, b"nested file");
}

// ============================================================================
// Test Group L: NTFS browsing
// ============================================================================

#[test]
fn test_ntfs_browse_root() {
    let img = load_fixture("test_ntfs.img.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::ntfs::NtfsFilesystem::open(cursor, 0).unwrap();

    assert!(fs.fs_type().starts_with("NTFS"), "got: {}", fs.fs_type());
    assert_eq!(fs.volume_label(), Some("test_ntfs"));

    let root = fs.root().unwrap();
    assert!(root.is_directory());

    let entries = fs.list_directory(&root).unwrap();
    let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
    assert!(
        names.contains(&"hello.txt"),
        "missing hello.txt in {names:?}"
    );
    assert!(names.contains(&"subdir"), "missing subdir in {names:?}");
}

#[test]
fn test_ntfs_read_file() {
    let img = load_fixture("test_ntfs.img.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::ntfs::NtfsFilesystem::open(cursor, 0).unwrap();

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let hello = entries.iter().find(|e| e.name == "hello.txt").unwrap();

    let data = fs.read_file(hello, 1024).unwrap();
    assert_eq!(&data, b"Hello, ntfs!");
}

#[test]
fn test_ntfs_nested_directory() {
    let img = load_fixture("test_ntfs.img.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::ntfs::NtfsFilesystem::open(cursor, 0).unwrap();

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let subdir = entries.iter().find(|e| e.name == "subdir").unwrap();
    assert!(subdir.is_directory());

    let sub_entries = fs.list_directory(subdir).unwrap();
    let nested = sub_entries.iter().find(|e| e.name == "nested.txt").unwrap();
    let data = fs.read_file(nested, 1024).unwrap();
    assert_eq!(&data, b"nested file");
}

// ============================================================================
// Test Group M: exFAT browsing
// ============================================================================

#[test]
fn test_exfat_browse_root() {
    let img = load_fixture("test_exfat.img.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::exfat::ExfatFilesystem::open(cursor, 0).unwrap();

    assert_eq!(fs.fs_type(), "exFAT");
    assert_eq!(fs.volume_label(), Some("test_exfat"));

    let root = fs.root().unwrap();
    assert!(root.is_directory());

    let entries = fs.list_directory(&root).unwrap();
    let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
    assert!(
        names.contains(&"hello.txt"),
        "missing hello.txt in {names:?}"
    );
    assert!(names.contains(&"subdir"), "missing subdir in {names:?}");
}

#[test]
fn test_exfat_read_file() {
    let img = load_fixture("test_exfat.img.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::exfat::ExfatFilesystem::open(cursor, 0).unwrap();

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let hello = entries.iter().find(|e| e.name == "hello.txt").unwrap();

    let data = fs.read_file(hello, 1024).unwrap();
    // exFAT read_file may return cluster-padded data; truncate to file size
    let data = &data[..hello.size as usize];
    assert_eq!(data, b"Hello, exfat!");
}

#[test]
fn test_exfat_nested_directory() {
    let img = load_fixture("test_exfat.img.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::exfat::ExfatFilesystem::open(cursor, 0).unwrap();

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let subdir = entries.iter().find(|e| e.name == "subdir").unwrap();
    assert!(subdir.is_directory());

    let sub_entries = fs.list_directory(subdir).unwrap();
    let nested = sub_entries.iter().find(|e| e.name == "nested.txt").unwrap();
    let data = fs.read_file(nested, 1024).unwrap();
    let data = &data[..nested.size as usize];
    assert_eq!(data, b"Nested exfat content");
}

// ============================================================================
// Test Group N: HFS (classic) browsing
// ============================================================================

#[test]
fn test_hfs_browse_root() {
    let img = load_fixture("test_hfs.img.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::hfs::HfsFilesystem::open(cursor, 0).unwrap();

    assert_eq!(fs.fs_type(), "HFS");
    assert_eq!(fs.volume_label(), Some("test_hfs"));

    let root = fs.root().unwrap();
    assert!(root.is_directory());

    let entries = fs.list_directory(&root).unwrap();
    let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
    assert!(
        names.contains(&"hello.txt"),
        "missing hello.txt in {names:?}"
    );
    assert!(names.contains(&"subdir"), "missing subdir in {names:?}");
}

#[test]
fn test_hfs_read_file() {
    let img = load_fixture("test_hfs.img.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::hfs::HfsFilesystem::open(cursor, 0).unwrap();

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let hello = entries.iter().find(|e| e.name == "hello.txt").unwrap();

    let data = fs.read_file(hello, 1024).unwrap();
    assert_eq!(&data, b"Hello, hfs!");
}

#[test]
fn test_hfs_nested_directory() {
    let img = load_fixture("test_hfs.img.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::hfs::HfsFilesystem::open(cursor, 0).unwrap();

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let subdir = entries.iter().find(|e| e.name == "subdir").unwrap();
    assert!(subdir.is_directory());

    let sub_entries = fs.list_directory(subdir).unwrap();
    let nested = sub_entries.iter().find(|e| e.name == "nested.txt").unwrap();
    let data = fs.read_file(nested, 1024).unwrap();
    assert_eq!(&data, b"nested file");
}

// ============================================================================
// Test Group O: HFS+ browsing
// ============================================================================

#[test]
fn test_hfsplus_browse_root() {
    let img = load_fixture("test_hfsplus.img.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::hfsplus::HfsPlusFilesystem::open(cursor, 0).unwrap();

    assert_eq!(fs.fs_type(), "HFS+");
    assert_eq!(fs.volume_label(), Some("test_hfsplus"));

    let root = fs.root().unwrap();
    assert!(root.is_directory());

    let entries = fs.list_directory(&root).unwrap();
    let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
    assert!(
        names.contains(&"hello.txt"),
        "missing hello.txt in {names:?}"
    );
    assert!(names.contains(&"subdir"), "missing subdir in {names:?}");
}

#[test]
fn test_hfsplus_read_file() {
    let img = load_fixture("test_hfsplus.img.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::hfsplus::HfsPlusFilesystem::open(cursor, 0).unwrap();

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let hello = entries.iter().find(|e| e.name == "hello.txt").unwrap();

    let data = fs.read_file(hello, 1024).unwrap();
    let content = String::from_utf8_lossy(&data);
    assert!(
        content.starts_with("Hello, hfsplus"),
        "unexpected content: {content:?}"
    );
}

#[test]
fn test_hfsplus_nested_directory() {
    let img = load_fixture("test_hfsplus.img.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::hfsplus::HfsPlusFilesystem::open(cursor, 0).unwrap();

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let subdir = entries.iter().find(|e| e.name == "subdir").unwrap();
    assert!(subdir.is_directory());

    let sub_entries = fs.list_directory(subdir).unwrap();
    let nested = sub_entries.iter().find(|e| e.name == "nested.txt").unwrap();
    let data = fs.read_file(nested, 1024).unwrap();
    assert_eq!(&data, b"nested file");
}

// ============================================================================
// Test Group P: FAT compaction round-trips
// ============================================================================

#[test]
fn test_fat12_compaction_round_trip() {
    let img = load_fixture("test_fat12.img.zst");
    let original_size = img.len();
    let cursor = Cursor::new(img);

    let (mut compact, info) = rusty_backup::fs::CompactFatReader::new(cursor, 0).unwrap();
    assert!(info.compacted_size <= original_size as u64);

    let mut output = Vec::new();
    compact.read_to_end(&mut output).unwrap();

    let cursor = Cursor::new(output);
    let mut fs = rusty_backup::fs::fat::FatFilesystem::open(cursor, 0).unwrap();
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let hello = find_entry_ci(&entries, "hello.txt").unwrap();
    let data = fs.read_file(hello, 1024).unwrap();
    assert_eq!(&data, b"Hello, fat12!");
}

#[test]
fn test_fat16_compaction_round_trip() {
    let img = load_fixture("test_fat16.img.zst");
    let original_size = img.len();
    let cursor = Cursor::new(img);

    let (mut compact, info) = rusty_backup::fs::CompactFatReader::new(cursor, 0).unwrap();
    assert!(info.compacted_size <= original_size as u64);

    let mut output = Vec::new();
    compact.read_to_end(&mut output).unwrap();

    let cursor = Cursor::new(output);
    let mut fs = rusty_backup::fs::fat::FatFilesystem::open(cursor, 0).unwrap();
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let hello = find_entry_ci(&entries, "hello.txt").unwrap();
    let data = fs.read_file(hello, 1024).unwrap();
    assert_eq!(&data, b"Hello, fat16!");
}

#[test]
fn test_fat32_compaction_round_trip() {
    let img = load_fixture("test_fat32.img.zst");
    let original_size = img.len();
    let cursor = Cursor::new(img);

    let (mut compact, info) = rusty_backup::fs::CompactFatReader::new(cursor, 0).unwrap();
    assert!(info.compacted_size <= original_size as u64);

    let mut output = Vec::new();
    compact.read_to_end(&mut output).unwrap();

    let cursor = Cursor::new(output);
    let mut fs = rusty_backup::fs::fat::FatFilesystem::open(cursor, 0).unwrap();
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let hello = find_entry_ci(&entries, "hello.txt").unwrap();
    let data = fs.read_file(hello, 1024).unwrap();
    assert_eq!(&data, b"Hello, fat32!");
}

// ============================================================================
// Test Group Q: NTFS compaction round-trip
// ============================================================================

#[test]
fn test_ntfs_compaction_round_trip() {
    let img = load_fixture("test_ntfs.img.zst");
    let original_size = img.len();
    let cursor = Cursor::new(img);

    let (mut compact, info) = rusty_backup::fs::CompactNtfsReader::new(cursor, 0).unwrap();
    assert!(info.compacted_size <= original_size as u64);

    let mut output = Vec::new();
    compact.read_to_end(&mut output).unwrap();

    // Verify the compacted image retains a valid NTFS superblock
    let cursor = Cursor::new(output);
    let fs = rusty_backup::fs::ntfs::NtfsFilesystem::open(cursor, 0).unwrap();
    assert!(fs.fs_type().starts_with("NTFS"));
}

// ============================================================================
// Test Group R: exFAT compaction round-trip
// ============================================================================

#[test]
fn test_exfat_compaction_round_trip() {
    let img = load_fixture("test_exfat.img.zst");
    let original_size = img.len();
    let cursor = Cursor::new(img);

    let (_compact, info) = rusty_backup::fs::CompactExfatReader::new(cursor, 0).unwrap();
    assert!(info.compacted_size <= original_size as u64);
    assert!(info.compacted_size > 0);
}

// ============================================================================
// Test Group S: HFS compaction round-trip
// ============================================================================

#[test]
fn test_hfs_compaction_round_trip() {
    let img = load_fixture("test_hfs.img.zst");
    let original_size = img.len();
    let cursor = Cursor::new(img);

    let (mut compact, info) = rusty_backup::fs::CompactHfsReader::new(cursor, 0).unwrap();
    assert!(info.compacted_size <= original_size as u64);

    let mut output = Vec::new();
    compact.read_to_end(&mut output).unwrap();

    // Verify the compacted image retains a valid HFS volume
    let cursor = Cursor::new(output);
    let fs = rusty_backup::fs::hfs::HfsFilesystem::open(cursor, 0).unwrap();
    assert_eq!(fs.fs_type(), "HFS");
}

// ============================================================================
// Test Group T: HFS+ compaction round-trip
// ============================================================================

#[test]
fn test_hfsplus_compaction_round_trip() {
    let img = load_fixture("test_hfsplus.img.zst");
    let original_size = img.len();
    let cursor = Cursor::new(img);

    // Verify compaction initializes and reports valid sizes
    let (_compact, info) = rusty_backup::fs::CompactHfsPlusReader::new(cursor, 0).unwrap();
    assert!(info.compacted_size <= original_size as u64);
    assert!(info.compacted_size > 0);
}

// ============================================================================
// Test Group U: FAT resize
// ============================================================================

#[test]
fn test_fat16_resize_grows() {
    let mut img = load_fixture("test_fat16.img.zst");
    let original_size = img.len();
    let new_size = original_size * 2;
    img.resize(new_size, 0);
    let new_total_sectors = (new_size / 512) as u32;

    let mut cursor = Cursor::new(img);
    rusty_backup::fs::resize_fat_in_place(&mut cursor, 0, new_total_sectors, &mut |msg| {
        eprintln!("  resize: {msg}");
    })
    .unwrap();

    // Validate
    cursor.set_position(0);
    let warnings = rusty_backup::fs::validate_fat_integrity(&mut cursor, 0, &mut |msg| {
        eprintln!("  validate: {msg}");
    })
    .unwrap();
    assert!(warnings.is_empty(), "warnings: {warnings:?}");

    // Should still be browsable
    let img = cursor.into_inner();
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::fat::FatFilesystem::open(cursor, 0).unwrap();
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    assert!(has_entry_ci(&entries, "hello.txt"));
}

#[test]
fn test_fat32_resize_grows() {
    let mut img = load_fixture("test_fat32.img.zst");
    let original_size = img.len();
    let new_size = original_size * 2;
    img.resize(new_size, 0);
    let new_total_sectors = (new_size / 512) as u32;

    let mut cursor = Cursor::new(img);
    rusty_backup::fs::resize_fat_in_place(&mut cursor, 0, new_total_sectors, &mut |msg| {
        eprintln!("  resize: {msg}");
    })
    .unwrap();

    // Validate
    cursor.set_position(0);
    let warnings = rusty_backup::fs::validate_fat_integrity(&mut cursor, 0, &mut |msg| {
        eprintln!("  validate: {msg}");
    })
    .unwrap();
    assert!(warnings.is_empty(), "warnings: {warnings:?}");

    // Should still be browsable
    let img = cursor.into_inner();
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::fat::FatFilesystem::open(cursor, 0).unwrap();
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    assert!(has_entry_ci(&entries, "hello.txt"));
}

// ============================================================================
// Test Group V: NTFS resize
// ============================================================================

#[test]
fn test_ntfs_resize_grows() {
    let mut img = load_fixture("test_ntfs.img.zst");
    let original_size = img.len();
    let new_size = original_size * 2;
    img.resize(new_size, 0);
    let new_total_sectors = (new_size / 512) as u64;

    let mut cursor = Cursor::new(img);
    rusty_backup::fs::resize_ntfs_in_place(&mut cursor, 0, new_total_sectors, &mut |msg| {
        eprintln!("  resize: {msg}");
    })
    .unwrap();

    // Validate
    cursor.set_position(0);
    rusty_backup::fs::validate_ntfs_integrity(&mut cursor, 0, &mut |msg| {
        eprintln!("  validate: {msg}");
    })
    .unwrap();

    // Should still be browsable
    let img = cursor.into_inner();
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::ntfs::NtfsFilesystem::open(cursor, 0).unwrap();
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    assert!(entries.iter().any(|e| e.name == "hello.txt"));
}

// ============================================================================
// Test Group W: exFAT resize
// ============================================================================

#[test]
fn test_exfat_resize_grows() {
    let mut img = load_fixture("test_exfat.img.zst");
    let original_size = img.len();
    let new_size = original_size * 2;
    img.resize(new_size, 0);
    let new_volume_length_sectors = (new_size / 512) as u64;

    let mut cursor = Cursor::new(img);
    rusty_backup::fs::resize_exfat_in_place(
        &mut cursor,
        0,
        new_volume_length_sectors,
        &mut |msg| {
            eprintln!("  resize: {msg}");
        },
    )
    .unwrap();

    // Validate
    cursor.set_position(0);
    rusty_backup::fs::validate_exfat_integrity(&mut cursor, 0, &mut |msg| {
        eprintln!("  validate: {msg}");
    })
    .unwrap();

    // Should still be browsable
    let img = cursor.into_inner();
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::exfat::ExfatFilesystem::open(cursor, 0).unwrap();
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    assert!(entries.iter().any(|e| e.name == "hello.txt"));
}

// ============================================================================
// Test Group X: HFS resize
// ============================================================================

#[test]
fn test_hfs_resize_grows() {
    let mut img = load_fixture("test_hfs.img.zst");
    let original_size = img.len();
    let new_size = original_size * 2;
    img.resize(new_size, 0);

    let mut cursor = Cursor::new(img);
    rusty_backup::fs::resize_hfs_in_place(&mut cursor, 0, new_size as u64, &mut |msg| {
        eprintln!("  resize: {msg}");
    })
    .unwrap();

    // Validate
    cursor.set_position(0);
    rusty_backup::fs::validate_hfs_integrity(&mut cursor, 0, &mut |msg| {
        eprintln!("  validate: {msg}");
    })
    .unwrap();

    // Should still be browsable
    let img = cursor.into_inner();
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::hfs::HfsFilesystem::open(cursor, 0).unwrap();
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    assert!(entries.iter().any(|e| e.name == "hello.txt"));
}

// ============================================================================
// Test Group Y: HFS+ resize
// ============================================================================

#[test]
fn test_hfsplus_resize_grows() {
    let mut img = load_fixture("test_hfsplus.img.zst");
    let original_size = img.len();
    let new_size = original_size * 2;
    img.resize(new_size, 0);

    let mut cursor = Cursor::new(img);
    rusty_backup::fs::resize_hfsplus_in_place(&mut cursor, 0, new_size as u64, &mut |msg| {
        eprintln!("  resize: {msg}");
    })
    .unwrap();

    // Validate
    cursor.set_position(0);
    rusty_backup::fs::validate_hfsplus_integrity(&mut cursor, 0, &mut |msg| {
        eprintln!("  validate: {msg}");
    })
    .unwrap();

    // Should still be browsable
    let img = cursor.into_inner();
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::hfsplus::HfsPlusFilesystem::open(cursor, 0).unwrap();
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    assert!(entries.iter().any(|e| e.name == "hello.txt"));
}

// ============================================================================
// Test Group Z: Filesystem detection routing (new filesystems)
// ============================================================================

#[test]
fn test_detect_fat12_via_0x01() {
    let img = load_fixture("test_fat12.img.zst");
    let cursor = Cursor::new(img);
    let fs = rusty_backup::fs::open_filesystem(cursor, 0, 0x01, None).unwrap();
    assert_eq!(fs.fs_type(), "FAT12");
}

#[test]
fn test_detect_fat16_via_0x06() {
    let img = load_fixture("test_fat16.img.zst");
    let cursor = Cursor::new(img);
    let fs = rusty_backup::fs::open_filesystem(cursor, 0, 0x06, None).unwrap();
    assert_eq!(fs.fs_type(), "FAT16");
}

#[test]
fn test_detect_fat32_via_0x0b() {
    let img = load_fixture("test_fat32.img.zst");
    let cursor = Cursor::new(img);
    let fs = rusty_backup::fs::open_filesystem(cursor, 0, 0x0B, None).unwrap();
    assert_eq!(fs.fs_type(), "FAT32");
}

#[test]
fn test_detect_ntfs_via_0x07() {
    let img = load_fixture("test_ntfs.img.zst");
    let cursor = Cursor::new(img);
    let fs = rusty_backup::fs::open_filesystem(cursor, 0, 0x07, None).unwrap();
    assert!(fs.fs_type().starts_with("NTFS"), "got: {}", fs.fs_type());
}

#[test]
fn test_detect_exfat_via_0x07() {
    let img = load_fixture("test_exfat.img.zst");
    let cursor = Cursor::new(img);
    let fs = rusty_backup::fs::open_filesystem(cursor, 0, 0x07, None).unwrap();
    assert_eq!(fs.fs_type(), "exFAT");
}

#[test]
fn test_detect_fat12_auto() {
    let img = load_fixture("test_fat12.img.zst");
    let cursor = Cursor::new(img);
    let fs = rusty_backup::fs::open_filesystem(cursor, 0, 0x00, None).unwrap();
    assert_eq!(fs.fs_type(), "FAT12");
}

#[test]
fn test_detect_fat32_auto() {
    let img = load_fixture("test_fat32.img.zst");
    let cursor = Cursor::new(img);
    let fs = rusty_backup::fs::open_filesystem(cursor, 0, 0x00, None).unwrap();
    assert_eq!(fs.fs_type(), "FAT32");
}

#[test]
fn test_detect_ntfs_auto() {
    let img = load_fixture("test_ntfs.img.zst");
    let cursor = Cursor::new(img);
    let fs = rusty_backup::fs::open_filesystem(cursor, 0, 0x00, None).unwrap();
    assert!(fs.fs_type().starts_with("NTFS"), "got: {}", fs.fs_type());
}

#[test]
fn test_detect_exfat_auto() {
    let img = load_fixture("test_exfat.img.zst");
    let cursor = Cursor::new(img);
    let fs = rusty_backup::fs::open_filesystem(cursor, 0, 0x00, None).unwrap();
    assert_eq!(fs.fs_type(), "exFAT");
}

#[test]
fn test_detect_hfs_via_apple_hfs() {
    let img = load_fixture("test_hfs.img.zst");
    let cursor = Cursor::new(img);
    let fs = rusty_backup::fs::open_filesystem(cursor, 0, 0x00, Some("Apple_HFS")).unwrap();
    assert_eq!(fs.fs_type(), "HFS");
}

#[test]
fn test_detect_hfsplus_auto() {
    let img = load_fixture("test_hfsplus.img.zst");
    let cursor = Cursor::new(img);
    let fs = rusty_backup::fs::open_filesystem(cursor, 0, 0x00, None).unwrap();
    assert_eq!(fs.fs_type(), "HFS+");
}

#[test]
fn test_detect_prodos_auto() {
    // Fixture: 800 KB Apple IIgs ProDOS volume "PRODOS3TST" with two
    // subdirectories (TSTFLDR, ANTETRIS) and a top-level FINDER.DATA file.
    // ANTETRIS holds a shareware game as sapling files.
    let img = load_fixture("test_prodos.hdv.zst");
    let cursor = Cursor::new(img);
    let fs = rusty_backup::fs::open_filesystem(cursor, 0, 0x00, None).unwrap();
    assert_eq!(fs.fs_type(), "ProDOS");
    assert_eq!(fs.volume_label(), Some("PRODOS3TST"));
}

#[test]
fn test_prodos_superfloppy_detection() {
    let img = load_fixture("test_prodos.hdv.zst");
    let mut cursor = Cursor::new(img);
    let table = rusty_backup::partition::PartitionTable::detect(&mut cursor).unwrap();
    match table {
        rusty_backup::partition::PartitionTable::None { fs_hint, .. } => {
            assert_eq!(fs_hint, "ProDOS");
        }
        other => panic!("expected superfloppy, got {other:?}"),
    }
}

#[test]
fn test_prodos_list_and_recurse() {
    use rusty_backup::fs::filesystem::Filesystem;

    let img = load_fixture("test_prodos.hdv.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::prodos::ProDosFilesystem::open(cursor, 0).unwrap();

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
    assert!(names.contains(&"TSTFLDR"), "expected TSTFLDR in {names:?}");
    assert!(
        names.contains(&"ANTETRIS"),
        "expected ANTETRIS in {names:?}"
    );
    assert!(
        names.contains(&"FINDER.DATA"),
        "expected FINDER.DATA in {names:?}"
    );

    // Directory storage_type ($D nibble) must map to directory entries, not
    // be skipped as a header.
    let tstfldr = entries.iter().find(|e| e.name == "TSTFLDR").unwrap();
    assert!(tstfldr.is_directory());

    // Recurse into TSTFLDR and verify the test text file is visible.
    let kids = fs.list_directory(tstfldr).unwrap();
    let kid_names: Vec<&str> = kids.iter().map(|e| e.name.as_str()).collect();
    assert!(
        kid_names.contains(&"TEST.TXT"),
        "expected TEST.TXT in {kid_names:?}"
    );

    // Recurse into ANTETRIS (shareware game folder) and confirm sapling files.
    let antetris = entries.iter().find(|e| e.name == "ANTETRIS").unwrap();
    let game_files = fs.list_directory(antetris).unwrap();
    let game_names: Vec<&str> = game_files.iter().map(|e| e.name.as_str()).collect();
    assert!(
        game_names.contains(&"ANTETRIS"),
        "expected ANTETRIS binary in {game_names:?}"
    );
    assert!(
        game_names.contains(&"ANTETRIS.DATA"),
        "expected ANTETRIS.DATA in {game_names:?}"
    );
}

#[test]
fn test_prodos_type_codes_populated() {
    // Verify list_directory populates FileEntry::type_code from the new
    // prodos_types table rather than returning raw hex codes.
    use rusty_backup::fs::filesystem::Filesystem;

    let img = load_fixture("test_prodos.hdv.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::prodos::ProDosFilesystem::open(cursor, 0).unwrap();

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let finder = entries
        .iter()
        .find(|e| e.name == "FINDER.DATA")
        .expect("FINDER.DATA present");
    let tc = finder
        .type_code
        .as_ref()
        .expect("type_code should be set for ProDOS files");
    // FINDER.DATA is stored with ProDOS type $C9 (FND) on IIgs volumes.
    assert_eq!(tc, "$C9 FND", "type_code for FINDER.DATA was {tc}");

    // Sanity-check a second entry: ANTETRIS/ANTETRIS is an S16 application
    // ($B3) on this fixture.
    let antetris_dir = entries.iter().find(|e| e.name == "ANTETRIS").unwrap();
    let game_files = fs.list_directory(antetris_dir).unwrap();
    let antetris_bin = game_files.iter().find(|e| e.name == "ANTETRIS").unwrap();
    let tc = antetris_bin.type_code.as_ref().unwrap();
    assert!(
        tc.starts_with('$') && tc.contains(' '),
        "type_code should be formatted as '$XX ABC', got {tc}"
    );
}

#[test]
fn test_prodos_create_file_auto_types_from_extension() {
    // With no explicit type_code in CreateFileOptions, the ProDOS writer
    // should fall back to the prodos_types extension table so that
    // "HELLO.TXT" → $04 TXT and "GAME.BAS" → $FC BAS with aux $0801.
    use rusty_backup::fs::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};

    let mut img = load_fixture("test_prodos.hdv.zst");

    {
        let cursor = Cursor::new(&mut img);
        let mut fs = rusty_backup::fs::prodos::ProDosFilesystem::open(cursor, 0).unwrap();

        let root = fs.root().unwrap();
        let txt = b"hello world";
        let txt_entry = fs
            .create_file(
                &root,
                "NOTE.TXT",
                &mut &txt[..],
                txt.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();
        assert_eq!(
            txt_entry.type_code.as_deref(),
            Some("$04 TXT"),
            "auto-detected type for .txt should be $04 TXT"
        );

        // Applesoft BASIC: name drives both type AND aux_type ($0801).
        let bas = b"10 PRINT \"HI\"";
        let bas_entry = fs
            .create_file(
                &root,
                "HELLO.BAS",
                &mut &bas[..],
                bas.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();
        assert_eq!(bas_entry.type_code.as_deref(), Some("$FC BAS"));

        fs.sync_metadata().unwrap();
    }

    // Reopen and verify the aux_type bytes actually landed on disk for HELLO.BAS.
    // Directory entry layout: entry offset 31-32 = aux_type (LE u16).
    let cursor = Cursor::new(img.clone());
    let mut fs = rusty_backup::fs::prodos::ProDosFilesystem::open(cursor, 0).unwrap();
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    assert!(entries.iter().any(|e| e.name == "NOTE.TXT"));
    assert!(entries.iter().any(|e| e.name == "HELLO.BAS"));

    // Find HELLO.BAS slot by scanning the volume directory key block (block 2)
    // for the name, then check its aux_type bytes at slot-offset 31..33.
    let block2 = &img[2 * 512..3 * 512];
    let mut aux = None;
    for slot in 0..13 {
        let eo = 4 + slot * 39;
        let name_len = (block2[eo] & 0xF) as usize;
        if name_len == 0 || name_len > 15 {
            continue;
        }
        let name: String = block2[eo + 1..eo + 1 + name_len]
            .iter()
            .map(|&b| b as char)
            .collect();
        if name == "HELLO.BAS" {
            aux = Some(u16::from_le_bytes([block2[eo + 31], block2[eo + 32]]));
            break;
        }
    }
    assert_eq!(
        aux,
        Some(0x0801),
        "HELLO.BAS should have aux_type $0801 from extension table, got {aux:?}"
    );
}

#[test]
fn test_prodos_edit_round_trip() {
    // End-to-end write test: open the fixture, create a file and a folder at
    // the root, sync metadata, drop the filesystem, re-open from the same
    // buffer, and verify everything is still visible and readable. This is
    // the test that would have caught the inverted $D/$E nibbles on real
    // directories — it exercises find_free_dir_slot, write_dir_entry,
    // update_dir_file_count, and the bitmap flush path.
    use rusty_backup::fs::filesystem::{
        CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem,
    };

    let mut img = load_fixture("test_prodos.hdv.zst");
    let payload = b"Hello from rusty-backup";

    {
        let cursor = Cursor::new(&mut img);
        let mut fs = rusty_backup::fs::prodos::ProDosFilesystem::open(cursor, 0).unwrap();

        let root = fs.root().unwrap();
        let create_opts = CreateFileOptions {
            type_code: Some("$04".into()), // TXT
            ..Default::default()
        };
        let created = fs
            .create_file(
                &root,
                "RBTEST",
                &mut &payload[..],
                payload.len() as u64,
                &create_opts,
            )
            .unwrap();
        assert_eq!(created.name, "RBTEST");

        let root = fs.root().unwrap();
        let _newdir = fs
            .create_directory(&root, "RBDIR", &CreateDirectoryOptions::default())
            .unwrap();

        fs.sync_metadata().unwrap();
    }

    // Reopen from the mutated bytes and verify state persisted.
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::prodos::ProDosFilesystem::open(cursor, 0).unwrap();
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
    assert!(
        names.contains(&"RBTEST"),
        "RBTEST missing after reopen; got {names:?}"
    );
    assert!(
        names.contains(&"RBDIR"),
        "RBDIR missing after reopen; got {names:?}"
    );
    assert!(
        names.contains(&"TSTFLDR"),
        "pre-existing TSTFLDR disappeared; got {names:?}"
    );

    // New file reads back correctly.
    let rbtest = entries.iter().find(|e| e.name == "RBTEST").unwrap();
    assert!(rbtest.is_file());
    let read_back = fs.read_file(rbtest, usize::MAX).unwrap();
    assert_eq!(read_back, payload, "RBTEST round-trip mismatch");

    // New directory is empty and listable.
    let rbdir = entries.iter().find(|e| e.name == "RBDIR").unwrap();
    assert!(rbdir.is_directory());
    let kids = fs.list_directory(rbdir).unwrap();
    assert!(kids.is_empty(), "new dir should be empty, got {kids:?}");
}

#[test]
fn test_prodos_read_file_contents() {
    use rusty_backup::fs::filesystem::Filesystem;

    let img = load_fixture("test_prodos.hdv.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::prodos::ProDosFilesystem::open(cursor, 0).unwrap();

    let root = fs.root().unwrap();
    let root_entries = fs.list_directory(&root).unwrap();
    let tstfldr = root_entries.iter().find(|e| e.name == "TSTFLDR").unwrap();
    let tst_kids = fs.list_directory(tstfldr).unwrap();
    let test_txt = tst_kids.iter().find(|e| e.name == "TEST.TXT").unwrap();
    assert!(test_txt.is_file());

    let data = fs.read_file(test_txt, usize::MAX).unwrap();
    assert_eq!(data.len(), test_txt.size as usize);
    // Sanity-check: not all zeros (which would indicate we followed the wrong
    // block pointer and read into uninitialized disk area).
    assert!(
        data.iter().any(|&b| b != 0),
        "TEST.TXT content was all zeros"
    );

    // Also read a sapling file from ANTETRIS to exercise the sapling codepath.
    let antetris = root_entries.iter().find(|e| e.name == "ANTETRIS").unwrap();
    let game_files = fs.list_directory(antetris).unwrap();
    let antetris_bin = game_files.iter().find(|e| e.name == "ANTETRIS").unwrap();
    assert_eq!(antetris_bin.mode, Some(2), "ANTETRIS should be a sapling");
    let bin_data = fs.read_file(antetris_bin, usize::MAX).unwrap();
    assert_eq!(bin_data.len(), antetris_bin.size as usize);
    assert!(bin_data.iter().any(|&b| b != 0));
}
