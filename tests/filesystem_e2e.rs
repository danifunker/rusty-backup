//! End-to-end tests using real filesystem images.
//!
//! Test fixtures are zstd-compressed images checked into tests/fixtures/.
//! They were generated on Linux using mkfs.ext2/ext4/btrfs with -d/--rootdir
//! to populate files without needing mount/sudo.
//!
//! Each image contains:
//!   hello.txt       — "Hello, <fstype>!"
//!   subdir/         — directory (mode 755)
//!   subdir/nested.txt — "nested file"
//!   link.txt        — symlink -> hello.txt
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
