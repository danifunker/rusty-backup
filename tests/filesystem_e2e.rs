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
fn test_prodos_aux_type_populated_from_catalog() {
    // `list_prodos_directory` must surface the aux_type from the 39-byte
    // catalog entry. ANTETRIS is an S16 ($B3) binary whose aux type should be
    // non-zero on the fixture.
    use rusty_backup::fs::filesystem::Filesystem;

    let img = load_fixture("test_prodos.hdv.zst");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::prodos::ProDosFilesystem::open(cursor, 0).unwrap();
    let root = fs.root().unwrap();
    let root_entries = fs.list_directory(&root).unwrap();
    let antetris_dir = root_entries.iter().find(|e| e.name == "ANTETRIS").unwrap();
    let game_files = fs.list_directory(antetris_dir).unwrap();
    let antetris_bin = game_files.iter().find(|e| e.name == "ANTETRIS").unwrap();
    assert!(
        antetris_bin.aux_type.is_some(),
        "aux_type should be populated for ProDOS files"
    );
}

#[test]
fn test_prodos_set_prodos_type_round_trip() {
    // Create a stub file as BIN, then change it to TXT with a specific aux
    // via set_prodos_type, sync, reopen, and verify both fields landed.
    use rusty_backup::fs::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};

    let mut img = load_fixture("test_prodos.hdv.zst");
    let payload = b"not-really-text";

    {
        let cursor = Cursor::new(&mut img);
        let mut fs = rusty_backup::fs::prodos::ProDosFilesystem::open(cursor, 0).unwrap();
        let root = fs.root().unwrap();
        let opts = CreateFileOptions {
            type_code: Some("$06".into()),
            aux_type: Some(0x0000),
            ..Default::default()
        };
        let created = fs
            .create_file(
                &root,
                "RBTYPE",
                &mut &payload[..],
                payload.len() as u64,
                &opts,
            )
            .unwrap();
        assert_eq!(created.type_code.as_deref(), Some("$06 BIN"));
        assert_eq!(created.aux_type, Some(0x0000));

        // Change to $04 TXT with aux $1234.
        fs.set_prodos_type(&created, 0x04, 0x1234).unwrap();
        fs.sync_metadata().unwrap();
    }

    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::prodos::ProDosFilesystem::open(cursor, 0).unwrap();
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let rbtype = entries.iter().find(|e| e.name == "RBTYPE").unwrap();
    assert_eq!(
        rbtype.type_code.as_deref(),
        Some("$04 TXT"),
        "set_prodos_type should have flipped type byte"
    );
    assert_eq!(
        rbtype.aux_type,
        Some(0x1234),
        "set_prodos_type should have written aux"
    );
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

/// Exercise the GUI staged-edits machinery end-to-end against PFS3: stage
/// AddFile + CreateDirectory + DeleteEntry against a fresh blank volume,
/// dispatch through `edit_queue::apply_edit`, then sync and reopen.
/// Mirrors what `BrowseView::apply_staged_edits` does at runtime.
#[test]
fn test_pfs3_staged_edits_round_trip() {
    use rusty_backup::fs::filesystem::EditableFilesystem;
    use rusty_backup::fs::pfs3::{create_blank_pfs3, Pfs3Filesystem};
    use rusty_backup::model::edit_queue::{apply_edit, StagedEdit};

    let tmp = tempfile::tempdir().expect("tempdir");
    let img_path = tmp.path().join("vol.hdf");
    std::fs::write(&img_path, create_blank_pfs3(8192, "StagedEd").unwrap())
        .expect("write blank image");

    let host_payload: &[u8] = b"hello via staged edits";
    let host_file = tmp.path().join("hello.txt");
    std::fs::write(&host_file, host_payload).expect("write host payload");

    // Open editable, capture the live root, build the staged-edit batch.
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&img_path)
        .expect("open editable");
    let mut efs: Box<dyn EditableFilesystem> =
        Box::new(Pfs3Filesystem::open(file, 0).expect("open Pfs3Filesystem"));
    let root = efs.root().expect("root");

    let edits = vec![
        StagedEdit::CreateDirectory {
            parent: root.clone(),
            name: "Sub".to_string(),
        },
        StagedEdit::AddFile {
            parent: root.clone(),
            name: "hello.txt".to_string(),
            host_path: host_file.clone(),
            size: host_payload.len() as u64,
            prodos_type: None,
            prodos_aux: None,
            resource_fork: None,
            hfs_type_override: None,
            hfs_creator_override: None,
        },
    ];
    for edit in &edits {
        apply_edit(&mut *efs, edit).expect("apply staged edit");
    }
    efs.sync_metadata().expect("sync after batch 1");
    drop(efs);

    // Re-open read-only and verify both entries landed.
    let f2 = std::fs::File::open(&img_path).expect("reopen");
    let mut fs = Pfs3Filesystem::open(f2, 0).expect("reopen Pfs3Filesystem");
    let r = fs.root().expect("root after");
    let kids = fs.list_directory(&r).expect("list");
    assert_eq!(kids.len(), 2, "got {:?}", kids);
    let names: Vec<&str> = kids.iter().map(|c| c.name.as_str()).collect();
    assert!(names.contains(&"Sub"));
    assert!(names.contains(&"hello.txt"));
    let hello = kids.iter().find(|c| c.name == "hello.txt").unwrap();
    let data = fs.read_file(hello, host_payload.len()).expect("read");
    assert_eq!(data, host_payload);
    let sub = kids.iter().find(|c| c.name == "Sub").unwrap();
    let sub_kids = fs.list_directory(sub).expect("list sub");
    assert!(sub_kids.is_empty(), "new dir should start empty");

    let hello_fe = hello.clone();
    let r_fe = r.clone();
    drop(fs);

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&img_path)
        .expect("reopen editable");
    let mut efs: Box<dyn EditableFilesystem> =
        Box::new(Pfs3Filesystem::open(file, 0).expect("open editable"));
    let del_batch = vec![StagedEdit::DeleteEntry {
        parent: r_fe,
        entry: hello_fe,
    }];
    for edit in &del_batch {
        apply_edit(&mut *efs, edit).expect("apply delete");
    }
    efs.sync_metadata().expect("sync after delete");
    drop(efs);

    let f3 = std::fs::File::open(&img_path).expect("reopen 3");
    let mut fs = Pfs3Filesystem::open(f3, 0).expect("reopen Pfs3Filesystem 3");
    let r = fs.root().expect("root after delete");
    let kids = fs.list_directory(&r).expect("list after delete");
    let names: Vec<&str> = kids.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        names,
        vec!["Sub"],
        "hello.txt should be gone after staged delete"
    );
}

/// Stage `CreateDirectory` + `AddFile` + `DeleteEntry` against a fresh
/// blank AFFS floppy via the GUI-style dispatcher; round-trip through
/// reopen to verify writes landed. Mirrors the PFS3 / SFS staged-edits
/// tests so the BrowseView::apply_staged_edits codepath has explicit
/// AFFS coverage.
#[test]
fn test_affs_staged_edits_round_trip() {
    use byteorder::{BigEndian, ByteOrder};
    use rusty_backup::fs::affs::AffsFilesystem;
    use rusty_backup::fs::filesystem::EditableFilesystem;
    use rusty_backup::model::edit_queue::{apply_edit, StagedEdit};

    // Build a minimal empty FFS floppy: 1760 blocks, FFS variant, root at
    // block 880, bitmap at 881, bootblocks @ 0–1.
    fn make_blank_ffs(name: &str) -> Vec<u8> {
        const BSIZE: usize = 512;
        let total_blocks = 1760usize;
        let mut img = vec![0u8; total_blocks * BSIZE];
        // Boot block 0: 'DOS\1' (FFS).
        img[0..4].copy_from_slice(b"DOS\x01");
        // Root block at 880.
        let root = 880usize;
        let bm = 881usize;
        let off = root * BSIZE;
        BigEndian::write_u32(&mut img[off..off + 4], 2); // T_HEADER
        BigEndian::write_u32(&mut img[off + 12..off + 16], 0x48); // hash table size
        BigEndian::write_u32(&mut img[off + 0x138..off + 0x13C], 0xFFFFFFFF); // bm_flag = valid
        BigEndian::write_u32(&mut img[off + 0x13C..off + 0x140], bm as u32);
        // Volume name BSTR at 0x1B0.
        let nb = name.as_bytes();
        let n = nb.len().min(30);
        img[off + 0x1B0] = n as u8;
        img[off + 0x1B1..off + 0x1B1 + n].copy_from_slice(&nb[..n]);
        BigEndian::write_u32(&mut img[off + 0x1FC..off + 0x200], 1u32); // ST_ROOT
                                                                        // Normal checksum over root: word 5 = -(sum of others).
        let mut sum: u32 = 0;
        for w in 0..128 {
            if w == 5 {
                continue;
            }
            let v = BigEndian::read_u32(&img[off + w * 4..off + w * 4 + 4]);
            sum = sum.wrapping_add(v);
        }
        BigEndian::write_u32(&mut img[off + 20..off + 24], 0u32.wrapping_sub(sum));
        // Bitmap at 881: word 0 reserved for checksum. Set all 1758 data-area
        // bits as free; clear bits for root + bitmap themselves.
        let bm_off = bm * BSIZE;
        // 55 u32 words after the checksum cover 1760 bits.
        for w in 1..56 {
            BigEndian::write_u32(&mut img[bm_off + w * 4..bm_off + w * 4 + 4], 0xFFFF_FFFF);
        }
        // Bitmap covers blocks 2..1760 (skip boot blocks). Bit b of word w
        // is block_index = 2 + (w-1)*32 + b. Mark root (880) + bm (881) used.
        for &block in &[root as u32, bm as u32] {
            let block_in_bm = block - 2;
            let word = 1 + (block_in_bm / 32) as usize;
            let bit = block_in_bm % 32;
            let mut v = BigEndian::read_u32(&img[bm_off + word * 4..bm_off + word * 4 + 4]);
            v &= !(1u32 << bit);
            BigEndian::write_u32(&mut img[bm_off + word * 4..bm_off + word * 4 + 4], v);
        }
        // Bitmap-block checksum: -(sum of all other words). (Word 0 is the
        // checksum slot.)
        let mut bsum: u32 = 0;
        for w in 1..128 {
            let v = BigEndian::read_u32(&img[bm_off + w * 4..bm_off + w * 4 + 4]);
            bsum = bsum.wrapping_add(v);
        }
        BigEndian::write_u32(&mut img[bm_off..bm_off + 4], 0u32.wrapping_sub(bsum));
        img
    }

    let tmp = tempfile::tempdir().expect("tempdir");
    let img_path = tmp.path().join("vol.adf");
    std::fs::write(&img_path, make_blank_ffs("AffsEdit")).expect("write blank image");

    let host_payload: &[u8] = b"hello via AFFS staged edits";
    let host_file = tmp.path().join("hello.txt");
    std::fs::write(&host_file, host_payload).expect("write host payload");

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&img_path)
        .expect("open editable");
    let mut efs: Box<dyn EditableFilesystem> =
        Box::new(AffsFilesystem::open(file, 0).expect("open AffsFilesystem"));
    let root = efs.root().expect("root");
    let edits = vec![
        StagedEdit::CreateDirectory {
            parent: root.clone(),
            name: "Sub".to_string(),
        },
        StagedEdit::AddFile {
            parent: root.clone(),
            name: "hello.txt".to_string(),
            host_path: host_file.clone(),
            size: host_payload.len() as u64,
            prodos_type: None,
            prodos_aux: None,
            resource_fork: None,
            hfs_type_override: None,
            hfs_creator_override: None,
        },
    ];
    for edit in &edits {
        apply_edit(&mut *efs, edit).expect("apply staged edit");
    }
    efs.sync_metadata().expect("sync after batch 1");
    drop(efs);

    let f2 = std::fs::File::open(&img_path).expect("reopen");
    let mut fs = AffsFilesystem::open(f2, 0).expect("reopen AffsFilesystem");
    let r = fs.root().expect("root after");
    let kids = fs.list_directory(&r).expect("list");
    assert_eq!(kids.len(), 2, "got {:?}", kids);
    let names: Vec<&str> = kids.iter().map(|c| c.name.as_str()).collect();
    assert!(names.contains(&"Sub"));
    assert!(names.contains(&"hello.txt"));
    let hello = kids.iter().find(|c| c.name == "hello.txt").unwrap();
    let data = fs.read_file(hello, host_payload.len()).expect("read");
    assert_eq!(data, host_payload);
    // amiga_protection is now plumbed via FileEntry — default access=0.
    assert_eq!(hello.amiga_protection, Some(0));

    let hello_fe = hello.clone();
    let r_fe = r.clone();
    drop(fs);

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&img_path)
        .expect("reopen editable");
    let mut efs: Box<dyn EditableFilesystem> =
        Box::new(AffsFilesystem::open(file, 0).expect("open editable"));
    apply_edit(
        &mut *efs,
        &StagedEdit::DeleteEntry {
            parent: r_fe,
            entry: hello_fe,
        },
    )
    .expect("apply delete");
    efs.sync_metadata().expect("sync after delete");
    drop(efs);

    let f3 = std::fs::File::open(&img_path).expect("reopen 3");
    let mut fs = AffsFilesystem::open(f3, 0).expect("reopen AffsFilesystem 3");
    let r = fs.root().expect("root after delete");
    let kids = fs.list_directory(&r).expect("list after delete");
    let names: Vec<&str> = kids.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        names,
        vec!["Sub"],
        "hello.txt should be gone after staged delete"
    );
}

/// Same end-to-end shape as the PFS3 test but for SFS — staged
/// CreateDirectory + AddFile + DeleteEntry against a fresh blank
/// SFS volume, dispatched through `edit_queue::apply_edit`.
#[test]
fn test_sfs_staged_edits_round_trip() {
    use rusty_backup::fs::filesystem::EditableFilesystem;
    use rusty_backup::fs::sfs::{create_blank_sfs, SfsFilesystem};
    use rusty_backup::model::edit_queue::{apply_edit, StagedEdit};

    let tmp = tempfile::tempdir().expect("tempdir");
    let img_path = tmp.path().join("vol.hdf");
    std::fs::write(&img_path, create_blank_sfs(8192, "StagedSFS").unwrap())
        .expect("write blank image");

    let host_payload: &[u8] = b"hello via SFS staged edits";
    let host_file = tmp.path().join("hello.txt");
    std::fs::write(&host_file, host_payload).expect("write host payload");

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&img_path)
        .expect("open editable");
    let mut efs: Box<dyn EditableFilesystem> =
        Box::new(SfsFilesystem::open(file, 0).expect("open SfsFilesystem"));
    let root = efs.root().expect("root");
    let edits = vec![
        StagedEdit::CreateDirectory {
            parent: root.clone(),
            name: "Sub".to_string(),
        },
        StagedEdit::AddFile {
            parent: root.clone(),
            name: "hello.txt".to_string(),
            host_path: host_file.clone(),
            size: host_payload.len() as u64,
            prodos_type: None,
            prodos_aux: None,
            resource_fork: None,
            hfs_type_override: None,
            hfs_creator_override: None,
        },
    ];
    for edit in &edits {
        apply_edit(&mut *efs, edit).expect("apply staged edit");
    }
    efs.sync_metadata().expect("sync batch 1");
    drop(efs);

    let f2 = std::fs::File::open(&img_path).expect("reopen");
    let mut fs = SfsFilesystem::open(f2, 0).expect("reopen SfsFilesystem");
    let r = fs.root().expect("root after");
    let kids = fs.list_directory(&r).expect("list");
    assert_eq!(kids.len(), 2, "got {:?}", kids);
    let names: Vec<&str> = kids.iter().map(|c| c.name.as_str()).collect();
    assert!(names.contains(&"Sub"));
    assert!(names.contains(&"hello.txt"));
    let hello = kids.iter().find(|c| c.name == "hello.txt").unwrap();
    let data = fs.read_file(hello, host_payload.len()).expect("read");
    assert_eq!(data, host_payload);

    let hello_fe = hello.clone();
    let r_fe = r.clone();
    drop(fs);

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&img_path)
        .expect("reopen editable");
    let mut efs: Box<dyn EditableFilesystem> =
        Box::new(SfsFilesystem::open(file, 0).expect("open editable"));
    apply_edit(
        &mut *efs,
        &StagedEdit::DeleteEntry {
            parent: r_fe,
            entry: hello_fe,
        },
    )
    .expect("apply delete");
    efs.sync_metadata().expect("sync after delete");
    drop(efs);

    let f3 = std::fs::File::open(&img_path).expect("reopen 3");
    let mut fs = SfsFilesystem::open(f3, 0).expect("reopen 3");
    let r = fs.root().expect("root after delete");
    let kids = fs.list_directory(&r).expect("list after delete");
    let names: Vec<&str> = kids.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        names,
        vec!["Sub"],
        "hello.txt should be gone after staged delete"
    );
}

/// End-to-end synthetic test: build a tiny RDB+FFS disk in memory (well
/// under 64 KiB), run it through the full `PartitionTable::detect` →
/// `open_filesystem` dispatch pipeline, and verify the AFFS volume
/// inside the RDB partition opens cleanly. Substitutes for a
/// checked-in binary fixture — exercises the same code paths without
/// committing any opaque bytes.
#[test]
fn test_synthetic_rdb_ffs_pipeline() {
    use byteorder::{BigEndian, ByteOrder};
    use rusty_backup::fs;
    use rusty_backup::partition::PartitionTable;

    const BSIZE: usize = 512;
    let surfaces = 2u32;
    let blk_per_trk = 16u32;
    let low_cyl = 1u32;
    let high_cyl = 2u32;
    let part_blocks = surfaces * blk_per_trk * (high_cyl - low_cyl + 1);
    let start_lba = surfaces * blk_per_trk * low_cyl;
    let total_blocks = start_lba + part_blocks;
    let mut disk = vec![0u8; total_blocks as usize * BSIZE];

    // RDSK at block 0.
    let mut rdsk = [0u8; BSIZE];
    rdsk[0..4].copy_from_slice(b"RDSK");
    BigEndian::write_u32(&mut rdsk[4..8], 64);
    BigEndian::write_u32(&mut rdsk[16..20], 512);
    BigEndian::write_u32(&mut rdsk[28..32], 1); // part_list -> block 1
    BigEndian::write_u32(&mut rdsk[32..36], 0xFFFFFFFF);
    BigEndian::write_u32(&mut rdsk[24..28], 0xFFFFFFFF);
    BigEndian::write_u32(&mut rdsk[36..40], 0xFFFFFFFF);
    BigEndian::write_i32(&mut rdsk[8..12], 0);
    let mut sum: i32 = 0;
    for w in 0..128 {
        sum = sum.wrapping_add(BigEndian::read_i32(&rdsk[w * 4..w * 4 + 4]));
    }
    BigEndian::write_i32(&mut rdsk[8..12], sum.wrapping_neg());
    disk[0..BSIZE].copy_from_slice(&rdsk);

    // PART block at block 1.
    let mut part = [0u8; BSIZE];
    part[0..4].copy_from_slice(b"PART");
    BigEndian::write_u32(&mut part[4..8], 64);
    BigEndian::write_u32(&mut part[16..20], 0xFFFFFFFF);
    BigEndian::write_u32(&mut part[20..24], 1); // flags = bootable
    part[36] = 3;
    part[37..40].copy_from_slice(b"DH0");
    BigEndian::write_u32(&mut part[32 * 4..32 * 4 + 4], 16);
    BigEndian::write_u32(&mut part[33 * 4..33 * 4 + 4], 128);
    BigEndian::write_u32(&mut part[35 * 4..35 * 4 + 4], surfaces);
    BigEndian::write_u32(&mut part[36 * 4..36 * 4 + 4], 1);
    BigEndian::write_u32(&mut part[37 * 4..37 * 4 + 4], blk_per_trk);
    BigEndian::write_u32(&mut part[41 * 4..41 * 4 + 4], low_cyl);
    BigEndian::write_u32(&mut part[42 * 4..42 * 4 + 4], high_cyl);
    BigEndian::write_u32(&mut part[48 * 4..48 * 4 + 4], 0x444F5301); // DOS\1 (FFS)
    BigEndian::write_i32(&mut part[8..12], 0);
    let mut sum: i32 = 0;
    for w in 0..128 {
        sum = sum.wrapping_add(BigEndian::read_i32(&part[w * 4..w * 4 + 4]));
    }
    BigEndian::write_i32(&mut part[8..12], sum.wrapping_neg());
    disk[BSIZE..2 * BSIZE].copy_from_slice(&part);

    // Place a blank FFS volume at the partition's start_lba.
    let part_off = start_lba as usize * BSIZE;
    disk[part_off..part_off + 4].copy_from_slice(b"DOS\x01");
    let root_blk = (part_blocks / 2) as usize;
    let root_off = part_off + root_blk * BSIZE;
    BigEndian::write_u32(&mut disk[root_off..root_off + 4], 2); // T_HEADER
    BigEndian::write_u32(&mut disk[root_off + 12..root_off + 16], 0x48);
    BigEndian::write_u32(&mut disk[root_off + 0x138..root_off + 0x13C], 0xFFFFFFFF);
    let bm_blk = root_blk + 1;
    BigEndian::write_u32(&mut disk[root_off + 0x13C..root_off + 0x140], bm_blk as u32);
    disk[root_off + 0x1B0] = 7;
    disk[root_off + 0x1B1..root_off + 0x1B8].copy_from_slice(b"RdbTest");
    BigEndian::write_u32(&mut disk[root_off + 0x1FC..root_off + 0x200], 1u32);
    let mut s: u32 = 0;
    for w in 0..128 {
        if w == 5 {
            continue;
        }
        let v = BigEndian::read_u32(&disk[root_off + w * 4..root_off + w * 4 + 4]);
        s = s.wrapping_add(v);
    }
    BigEndian::write_u32(
        &mut disk[root_off + 20..root_off + 24],
        0u32.wrapping_sub(s),
    );
    let bm_off = part_off + bm_blk * BSIZE;
    for w in 1..56 {
        BigEndian::write_u32(&mut disk[bm_off + w * 4..bm_off + w * 4 + 4], 0xFFFF_FFFF);
    }
    for &block in &[root_blk as u32, bm_blk as u32] {
        let block_in_bm = block - 2;
        let word = 1 + (block_in_bm / 32) as usize;
        let bit = block_in_bm % 32;
        let mut v = BigEndian::read_u32(&disk[bm_off + word * 4..bm_off + word * 4 + 4]);
        v &= !(1u32 << bit);
        BigEndian::write_u32(&mut disk[bm_off + word * 4..bm_off + word * 4 + 4], v);
    }
    let mut bs: u32 = 0;
    for w in 1..128 {
        bs = bs.wrapping_add(BigEndian::read_u32(
            &disk[bm_off + w * 4..bm_off + w * 4 + 4],
        ));
    }
    BigEndian::write_u32(&mut disk[bm_off..bm_off + 4], 0u32.wrapping_sub(bs));

    // Run it through the full detect → open dispatch.
    assert!(
        disk.len() <= 64 * 1024,
        "synthetic RDB+FFS disk should be <= 64 KiB: got {} bytes",
        disk.len()
    );
    let mut cur = Cursor::new(disk);
    let table = PartitionTable::detect(&mut cur).expect("detect");
    let parts = table.partitions();
    assert_eq!(parts.len(), 1, "expected single RDB partition");
    let p = &parts[0];
    assert_eq!(p.partition_type_string.as_deref(), Some("DOS\\1"));
    assert_eq!(p.start_lba * 512, part_off as u64);
    let mut fs = fs::open_filesystem(
        cur,
        p.start_lba * 512,
        p.partition_type_byte,
        p.partition_type_string.as_deref(),
    )
    .expect("open AFFS via dispatch");
    assert_eq!(fs.volume_label(), Some("RdbTest"));
    let root = fs.root().expect("root");
    let kids = fs.list_directory(&root).expect("list");
    assert!(kids.is_empty(), "blank RDB+FFS volume should be empty");
}

/// Synthetic RDB+SFS disk, shrink an SFS partition via the export
/// reconstruct pipeline (`reconstruct_raw_rdb_disk`), then verify:
///   - the output disk is smaller than the source (RDSK + PART blocks
///     reflect new geometry, total bytes reduced),
///   - the patched RDB still parses and points at the shrunk partition,
///   - the SFS partition at its new size reopens and still lists its
///     contents.
///
/// This is the end-to-end "user shrinks an SFS partition and re-exports
/// the disk" path that the original bug report was about.
#[test]
fn test_rdb_sfs_export_shrink_round_trip() {
    use byteorder::{BigEndian, ByteOrder};
    use rusty_backup::fs;
    use rusty_backup::partition::{PartitionSizeOverride, PartitionTable};
    use rusty_backup::rbformats::reconstruct_raw_rdb_disk;

    const BSIZE: usize = 512;
    let surfaces = 2u32;
    let blk_per_trk = 16u32;
    let cyl_lbas = surfaces * blk_per_trk; // 32 LBAs/cyl = 16 KiB/cyl

    // SFS partition: low_cyl=1, high_cyl=200 = 200 cyls = 6400 LBAs = 3.2 MiB.
    let low_cyl = 1u32;
    let high_cyl = 200u32;
    let part_lbas = cyl_lbas * (high_cyl - low_cyl + 1);
    let start_lba = cyl_lbas * low_cyl;
    let total_lbas = start_lba + part_lbas;
    let mut disk = vec![0u8; total_lbas as usize * BSIZE];

    // RDSK at block 0.
    let mut rdsk = [0u8; BSIZE];
    rdsk[0..4].copy_from_slice(b"RDSK");
    BigEndian::write_u32(&mut rdsk[4..8], 64);
    BigEndian::write_u32(&mut rdsk[16..20], 512); // block_size
    BigEndian::write_u32(&mut rdsk[28..32], 1); // part_list -> block 1
    BigEndian::write_u32(&mut rdsk[32..36], 0xFFFFFFFF); // fs_list
    BigEndian::write_u32(&mut rdsk[24..28], 0xFFFFFFFF); // badblk_list
                                                         // Geometry: cylinders @ long 16, hi_cyl @ long 35, cyl_blks @ long 36.
    BigEndian::write_u32(&mut rdsk[16 * 4..16 * 4 + 4], high_cyl + 1);
    BigEndian::write_u32(&mut rdsk[35 * 4..35 * 4 + 4], high_cyl);
    BigEndian::write_u32(&mut rdsk[36 * 4..36 * 4 + 4], cyl_lbas);
    BigEndian::write_i32(&mut rdsk[8..12], 0);
    let mut sum: i32 = 0;
    for w in 0..128 {
        sum = sum.wrapping_add(BigEndian::read_i32(&rdsk[w * 4..w * 4 + 4]));
    }
    BigEndian::write_i32(&mut rdsk[8..12], sum.wrapping_neg());
    disk[0..BSIZE].copy_from_slice(&rdsk);

    // PART block at block 1.
    let mut part = [0u8; BSIZE];
    part[0..4].copy_from_slice(b"PART");
    BigEndian::write_u32(&mut part[4..8], 64);
    BigEndian::write_u32(&mut part[16..20], 0xFFFFFFFF); // next = NO_BLOCK
    BigEndian::write_u32(&mut part[20..24], 0); // flags = not bootable
    part[36] = 3;
    part[37..40].copy_from_slice(b"DH0");
    BigEndian::write_u32(&mut part[32 * 4..32 * 4 + 4], 16); // env_size
    BigEndian::write_u32(&mut part[33 * 4..33 * 4 + 4], 128); // env_block_size_longs = 128 -> 512 bytes
    BigEndian::write_u32(&mut part[35 * 4..35 * 4 + 4], surfaces);
    BigEndian::write_u32(&mut part[36 * 4..36 * 4 + 4], 1); // sec_per_blk
    BigEndian::write_u32(&mut part[37 * 4..37 * 4 + 4], blk_per_trk);
    BigEndian::write_u32(&mut part[41 * 4..41 * 4 + 4], low_cyl);
    BigEndian::write_u32(&mut part[42 * 4..42 * 4 + 4], high_cyl);
    BigEndian::write_u32(&mut part[48 * 4..48 * 4 + 4], 0x53465300); // SFS\0
    BigEndian::write_i32(&mut part[8..12], 0);
    let mut sum: i32 = 0;
    for w in 0..128 {
        sum = sum.wrapping_add(BigEndian::read_i32(&part[w * 4..w * 4 + 4]));
    }
    BigEndian::write_i32(&mut part[8..12], sum.wrapping_neg());
    disk[BSIZE..2 * BSIZE].copy_from_slice(&part);

    // SFS volume at partition offset. 6400 blocks @ 512 = 3.2 MiB.
    let sfs_img =
        rusty_backup::fs::sfs::create_blank_sfs(part_lbas, "RdbSfsRT").expect("create_blank_sfs");
    assert_eq!(sfs_img.len(), part_lbas as usize * BSIZE);
    let part_off = start_lba as usize * BSIZE;
    disk[part_off..part_off + sfs_img.len()].copy_from_slice(&sfs_img);

    let source_size_bytes = disk.len() as u64;

    // Source disk is parseable as RDB+SFS.
    let mut src_cur = Cursor::new(disk.clone());
    let table = PartitionTable::detect(&mut src_cur).expect("detect");
    let parts = table.partitions();
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].partition_type_string.as_deref(), Some("SFS\\0"));

    // Pick a shrink target: 100 cylinders = 1.6 MiB = 3200 LBAs. Must be
    // cylinder-aligned to satisfy Rdb::patch_for_restore.
    let new_part_lbas = cyl_lbas * 100;
    let new_part_bytes = new_part_lbas as u64 * 512;
    let overrides = vec![PartitionSizeOverride::size_only(
        parts[0].index,
        parts[0].start_lba,
        parts[0].size_bytes,
        new_part_bytes,
    )];

    // Run the export reconstruct.
    let mut out_disk: Vec<u8> = Vec::new();
    out_disk.resize(source_size_bytes as usize, 0);
    let mut dest = Cursor::new(out_disk);
    let mut src2 = Cursor::new(disk.clone());
    let mut log_lines: Vec<String> = Vec::new();
    let total_written = reconstruct_raw_rdb_disk(
        &mut src2,
        source_size_bytes,
        &mut dest,
        &overrides,
        None,
        &mut |_| {},
        &|| false,
        &mut |s| log_lines.push(s.to_string()),
    )
    .expect("rdb reconstruct");
    assert!(
        log_lines.iter().any(|l| l.contains("RDB export")),
        "expected RDB export log line; got {log_lines:?}"
    );

    // Output disk should be smaller than source: new partition end =
    // start_lba (32) + new_part_lbas (3200) = 3232 LBAs ≈ 1.62 MiB.
    let expected_total_lbas = start_lba + new_part_lbas;
    let expected_disk_bytes = expected_total_lbas as u64 * 512;
    assert_eq!(
        total_written, expected_disk_bytes,
        "total_written should match expected new disk size"
    );
    assert!(
        total_written < source_size_bytes,
        "output ({total_written}) should be smaller than source ({source_size_bytes})"
    );

    // Reopen the output disk and verify the RDB + SFS still parse.
    let out_bytes = dest.into_inner();
    let truncated = out_bytes[..total_written as usize].to_vec();
    let mut out_cur = Cursor::new(truncated);
    let out_table = PartitionTable::detect(&mut out_cur).expect("detect output");
    let out_parts = out_table.partitions();
    assert_eq!(out_parts.len(), 1, "should still have 1 partition");
    let p = &out_parts[0];
    assert_eq!(p.partition_type_string.as_deref(), Some("SFS\\0"));
    assert_eq!(
        p.size_bytes, new_part_bytes,
        "partition size should reflect shrink"
    );
    let mut fs = fs::open_filesystem(
        out_cur,
        p.start_lba * 512,
        p.partition_type_byte,
        p.partition_type_string.as_deref(),
    )
    .expect("open SFS after shrink");
    let root = fs.root().expect("root");
    let kids = fs.list_directory(&root).expect("list");
    assert!(kids.is_empty(), "blank SFS volume should still be empty");
}

/// Multi-partition auto-pack: shrinking partition 1 should slide
/// partition 2 down to close the gap, NOT leave a zero-filled hole at
/// partition 2's original LBA. This is the regression that produced a
/// 71 GiB output when the user expected ~38 GiB on a real
/// 3-partition Amiga disk.
#[test]
fn test_rdb_multi_partition_shrink_auto_packs() {
    use byteorder::{BigEndian, ByteOrder};
    use rusty_backup::partition::{PartitionSizeOverride, PartitionTable};
    use rusty_backup::rbformats::reconstruct_raw_rdb_disk;

    const BSIZE: usize = 512;
    let surfaces = 2u32;
    let blk_per_trk = 16u32;
    let cyl_lbas = surfaces * blk_per_trk; // 32 LBAs/cyl = 16 KiB/cyl

    // Two partitions back-to-back: P0 at cyls 1..50, P1 at cyls 50..150.
    // After shrinking P0 down to 10 cyls, P1 should slide from cyl 50 to
    // cyl 11 (auto-pack) — NOT remain at cyl 50.
    let p0_low = 1u32;
    let p0_high = 49u32; // 49 cyls = 1568 LBAs = 784 KiB
    let p1_low = 50u32;
    let p1_high = 149u32; // 100 cyls = 3200 LBAs = 1.6 MiB
    let p1_lbas = cyl_lbas * (p1_high - p1_low + 1);
    let p0_start = cyl_lbas * p0_low;
    let p1_start = cyl_lbas * p1_low;
    let total_lbas = p1_start + p1_lbas;
    let mut disk = vec![0u8; total_lbas as usize * BSIZE];

    // Stamp a recognizable byte pattern at the START of each partition's
    // data so we can verify P1's data was copied to its new (lower) dest.
    let p0_marker = [0xAA, 0xAA, 0xAA, 0xAA];
    let p1_marker = [0xBB, 0xBB, 0xBB, 0xBB];
    disk[p0_start as usize * BSIZE..p0_start as usize * BSIZE + 4].copy_from_slice(&p0_marker);
    disk[p1_start as usize * BSIZE..p1_start as usize * BSIZE + 4].copy_from_slice(&p1_marker);

    // RDSK.
    let mut rdsk = [0u8; BSIZE];
    rdsk[0..4].copy_from_slice(b"RDSK");
    BigEndian::write_u32(&mut rdsk[4..8], 64);
    BigEndian::write_u32(&mut rdsk[16..20], 512);
    BigEndian::write_u32(&mut rdsk[28..32], 1); // part_list -> block 1
    BigEndian::write_u32(&mut rdsk[32..36], 0xFFFFFFFF);
    BigEndian::write_u32(&mut rdsk[24..28], 0xFFFFFFFF);
    BigEndian::write_u32(&mut rdsk[16 * 4..16 * 4 + 4], p1_high + 1);
    BigEndian::write_u32(&mut rdsk[35 * 4..35 * 4 + 4], p1_high);
    BigEndian::write_u32(&mut rdsk[36 * 4..36 * 4 + 4], cyl_lbas);
    BigEndian::write_i32(&mut rdsk[8..12], 0);
    let mut sum: i32 = 0;
    for w in 0..128 {
        sum = sum.wrapping_add(BigEndian::read_i32(&rdsk[w * 4..w * 4 + 4]));
    }
    BigEndian::write_i32(&mut rdsk[8..12], sum.wrapping_neg());
    disk[0..BSIZE].copy_from_slice(&rdsk);

    // PART block helper.
    let mk_part = |next: u32, drv: &[u8], low: u32, high: u32, dos: u32| -> [u8; BSIZE] {
        let mut p = [0u8; BSIZE];
        p[0..4].copy_from_slice(b"PART");
        BigEndian::write_u32(&mut p[4..8], 64);
        BigEndian::write_u32(&mut p[16..20], next);
        BigEndian::write_u32(&mut p[20..24], 0);
        p[36] = drv.len() as u8;
        p[37..37 + drv.len()].copy_from_slice(drv);
        BigEndian::write_u32(&mut p[32 * 4..32 * 4 + 4], 16);
        BigEndian::write_u32(&mut p[33 * 4..33 * 4 + 4], 128);
        BigEndian::write_u32(&mut p[35 * 4..35 * 4 + 4], surfaces);
        BigEndian::write_u32(&mut p[36 * 4..36 * 4 + 4], 1);
        BigEndian::write_u32(&mut p[37 * 4..37 * 4 + 4], blk_per_trk);
        BigEndian::write_u32(&mut p[41 * 4..41 * 4 + 4], low);
        BigEndian::write_u32(&mut p[42 * 4..42 * 4 + 4], high);
        BigEndian::write_u32(&mut p[48 * 4..48 * 4 + 4], dos);
        BigEndian::write_i32(&mut p[8..12], 0);
        let mut sum: i32 = 0;
        for w in 0..128 {
            sum = sum.wrapping_add(BigEndian::read_i32(&p[w * 4..w * 4 + 4]));
        }
        BigEndian::write_i32(&mut p[8..12], sum.wrapping_neg());
        p
    };
    // P0 PART at block 1, points to P1 PART at block 2.
    disk[1 * BSIZE..2 * BSIZE].copy_from_slice(&mk_part(2, b"DH0", p0_low, p0_high, 0x444F5301));
    disk[2 * BSIZE..3 * BSIZE]
        .copy_from_slice(&mk_part(0xFFFFFFFF, b"DH1", p1_low, p1_high, 0x444F5301));

    // Source parses with two partitions.
    let mut src = std::io::Cursor::new(disk.clone());
    let table = PartitionTable::detect(&mut src).expect("detect");
    let parts = table.partitions();
    assert_eq!(parts.len(), 2);

    // Shrink P0 from 49 cyls (1568 LBAs) to 10 cyls (320 LBAs = 160 KiB).
    let p0_new_bytes = (cyl_lbas * 10) as u64 * 512;
    let overrides = vec![
        PartitionSizeOverride::size_only(
            parts[0].index,
            parts[0].start_lba,
            parts[0].size_bytes,
            p0_new_bytes,
        ),
        PartitionSizeOverride::size_only(
            parts[1].index,
            parts[1].start_lba,
            parts[1].size_bytes,
            parts[1].size_bytes, // unchanged
        ),
    ];

    let source_size_bytes = disk.len() as u64;
    let mut out_disk = vec![0u8; source_size_bytes as usize];
    let mut dest = std::io::Cursor::new(&mut out_disk);
    let mut src2 = std::io::Cursor::new(disk.clone());
    let total_written = reconstruct_raw_rdb_disk(
        &mut src2,
        source_size_bytes,
        &mut dest,
        &overrides,
        None,
        &mut |_| {},
        &|| false,
        &mut |_| {},
    )
    .expect("reconstruct");

    // Auto-pack expectation:
    //   - P0 occupies cyls 1..=10  (10 cyls, 320 LBAs)
    //   - P1 occupies cyls 11..=110 (100 cyls, 3200 LBAs)
    // Disk end at cyl 111 = 3552 LBAs = 1,776 KiB.
    let expected_lbas = cyl_lbas * 111;
    let expected_bytes = expected_lbas as u64 * 512;
    assert_eq!(
        total_written, expected_bytes,
        "auto-pack should produce a tightly-packed output"
    );
    assert!(
        total_written < source_size_bytes,
        "output should be smaller than source after shrink"
    );

    // P1's data (marked 0xBB×4) should be at its NEW dest LBA, not at the
    // original p1_start.
    let new_p1_start_lba = cyl_lbas * 11;
    let new_p1_offset = new_p1_start_lba as usize * BSIZE;
    let final_bytes = &out_disk[..total_written as usize];
    assert_eq!(
        &final_bytes[new_p1_offset..new_p1_offset + 4],
        &p1_marker,
        "P1 data should land at auto-packed dest LBA {new_p1_start_lba}"
    );

    // Reopen and verify the patched RDB.
    let mut out_cur = std::io::Cursor::new(final_bytes.to_vec());
    let out_table = PartitionTable::detect(&mut out_cur).expect("detect output");
    let out_parts = out_table.partitions();
    assert_eq!(out_parts.len(), 2);
    assert_eq!(out_parts[0].size_bytes, p0_new_bytes, "P0 patched size");
    assert_eq!(
        out_parts[1].start_lba * 512,
        new_p1_offset as u64,
        "P1 should report its NEW packed start LBA"
    );
}

/// End-to-end: invoke the universal `resize_filesystem_for` dispatcher
/// against an in-memory blank AFFS floppy, then verify the volume
/// reopens at its new geometry. Confirms AFFS is wired into the
/// dispatch alongside FAT / NTFS / HFS / SFS / PFS3.
#[test]
fn test_resize_filesystem_for_affs_shrink_round_trip() {
    use byteorder::{BigEndian, ByteOrder};
    use rusty_backup::fs::{self, filesystem::Filesystem, resize_filesystem_for};

    const BSIZE: usize = 512;

    // Build a 1760-sector blank FFS floppy inline (same recipe as
    // make_blank_ffs above, kept local so this test stands alone).
    fn make_blank(name: &str) -> Vec<u8> {
        let total = 1760usize;
        let mut img = vec![0u8; total * BSIZE];
        img[0..4].copy_from_slice(b"DOS\x01");
        let root = 880usize;
        let bm = 881usize;
        let off = root * BSIZE;
        BigEndian::write_u32(&mut img[off..off + 4], 2); // T_HEADER
        BigEndian::write_u32(&mut img[off + 4..off + 8], 880);
        BigEndian::write_u32(&mut img[off + 12..off + 16], 0x48);
        BigEndian::write_u32(&mut img[off + 0x138..off + 0x13C], 0xFFFFFFFF);
        BigEndian::write_u32(&mut img[off + 0x13C..off + 0x140], bm as u32);
        let nb = name.as_bytes();
        let n = nb.len().min(30);
        img[off + 0x1B0] = n as u8;
        img[off + 0x1B1..off + 0x1B1 + n].copy_from_slice(&nb[..n]);
        BigEndian::write_u32(&mut img[off + 0x1FC..off + 0x200], 1);
        let mut s: u32 = 0;
        for w in 0..128 {
            if w == 5 {
                continue;
            }
            let v = BigEndian::read_u32(&img[off + w * 4..off + w * 4 + 4]);
            s = s.wrapping_add(v);
        }
        BigEndian::write_u32(&mut img[off + 20..off + 24], 0u32.wrapping_sub(s));
        let bm_off = bm * BSIZE;
        for w in 1..56 {
            BigEndian::write_u32(&mut img[bm_off + w * 4..bm_off + w * 4 + 4], 0xFFFF_FFFF);
        }
        for &block in &[root as u32, bm as u32] {
            let block_in_bm = block - 2;
            let word = 1 + (block_in_bm / 32) as usize;
            let bit = block_in_bm % 32;
            let mut v = BigEndian::read_u32(&img[bm_off + word * 4..bm_off + word * 4 + 4]);
            v &= !(1u32 << bit);
            BigEndian::write_u32(&mut img[bm_off + word * 4..bm_off + word * 4 + 4], v);
        }
        let mut bs: u32 = 0;
        for w in 1..128 {
            bs = bs.wrapping_add(BigEndian::read_u32(
                &img[bm_off + w * 4..bm_off + w * 4 + 4],
            ));
        }
        BigEndian::write_u32(&mut img[bm_off..bm_off + 4], 0u32.wrapping_sub(bs));
        img
    }

    let img = make_blank("DispRT");
    let mut cur = Cursor::new(img);
    let mut log: Vec<String> = Vec::new();
    let new_size = 800u64 * BSIZE as u64;
    resize_filesystem_for(&mut cur, 0, new_size, &mut |s| log.push(s.to_string()))
        .expect("dispatcher resize");
    assert!(
        log.iter().any(|l| l.contains("AFFS resize")),
        "expected AFFS resize log, got {log:?}"
    );

    // Truncate to new size and verify reopen.
    let mut bytes = cur.into_inner();
    bytes.truncate(new_size as usize);
    let mut cur2 = Cursor::new(bytes);
    let fs = fs::affs::AffsFilesystem::open(&mut cur2, 0).expect("reopen");
    assert_eq!(fs.total_blocks(), 800);
    assert_eq!(fs.root_block_num(), 400);
    assert_eq!(fs.volume_label(), Some("DispRT"));
}
