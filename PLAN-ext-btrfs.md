# Implementation Plan: ext2/3/4, btrfs, and Unix Filesystem Infrastructure

## How to Use This Plan

This plan is designed for implementation across multiple sessions. Each task is **self-contained** — it lists exactly which files to read, what to create/modify, how to verify, and what the expected outcome is. Start at Task 1 and work through in order. Mark tasks as DONE when complete.

When starting a new session, read this file first, find the first uncompleted task, and begin there.

---

## Progress Tracker

- [x] Task 1: FileEntry type extensions (entry.rs)
- [x] Task 2: Unix common — inode helpers (unix_common/inode.rs)
- [x] Task 3: Unix common — bitmap utilities (unix_common/bitmap.rs)
- [x] Task 4: Unix common — compact reader framework (unix_common/compact.rs)
- [x] Task 5: Unix common — validation helpers (unix_common/validate.rs)
- [x] Task 6: GUI updates for symlinks, specials, permissions (browse_view.rs)
- [x] Task 7: ext2/3/4 browsing — superblock + inode + directory parsing (ext.rs)
- [x] Task 8: Wire ext into mod.rs routing + detection
- [x] Task 9: ext2/3/4 compaction — CompactExtReader
- [x] Task 10: ext2/3/4 resize + validate + restore wiring
- [x] Task 11: btrfs browsing — superblock + chunk tree + FS tree (btrfs.rs)
- [x] Task 12: Wire btrfs into mod.rs routing + detection
- [x] Task 13: btrfs compaction — CompactBtrfsReader
- [x] Task 14: btrfs resize + validate + restore wiring
- [x] Task 15: Clonezilla block_cache ext/btrfs metadata scanning
- [ ] Task 16: End-to-end testing

---

## Task 1: FileEntry Type Extensions

**Status:** DONE
**Depends on:** Nothing
**Files to read first:** `src/fs/entry.rs`
**Files to modify:** `src/fs/entry.rs`
**Verify:** `cargo build` succeeds (will have unused warnings until GUI/FS modules use new fields — that's fine)

### What to do

Add new entry types and metadata fields to `FileEntry` so Unix filesystems can represent symlinks, special files, permissions, and ownership.

### Changes to `src/fs/entry.rs`

1. Add variants to `EntryType` enum:
   ```rust
   pub enum EntryType {
       File,
       Directory,
       Symlink,     // NEW
       Special,     // NEW — block dev, char dev, socket, fifo
   }
   ```

2. Add fields to `FileEntry` struct:
   ```rust
   pub struct FileEntry {
       // ... existing fields ...
       pub symlink_target: Option<String>,  // NEW — target path for symlinks
       pub special_type: Option<String>,    // NEW — "block device", "char device", "socket", "fifo"
       pub mode: Option<u32>,               // NEW — raw Unix mode bits (e.g., 0o100755)
       pub uid: Option<u32>,                // NEW — Unix user ID
       pub gid: Option<u32>,                // NEW — Unix group ID
   }
   ```

3. Add constructor `FileEntry::new_symlink(name, path, size, location, target)` — sets `entry_type: EntryType::Symlink`, stores target in `symlink_target`

4. Add constructor `FileEntry::new_special(name, path, location, special_type_str)` — sets `entry_type: EntryType::Special`, stores type in `special_type`

5. Update ALL existing constructors (`root()`, `new_directory()`, `new_file()`) to initialize new fields as `None`

6. Add helper methods:
   - `is_symlink(&self) -> bool`
   - `is_special(&self) -> bool`
   - `mode_string(&self) -> Option<String>` — returns e.g. `"drwxr-xr-x"`. Can be a placeholder that returns `None` for now (real implementation comes in Task 2 via `unix_common`)

7. Update `size_string()` — return empty string for specials (like directories)

### Why

Every subsequent task depends on these fields existing. Symlinks and specials are common on ext/btrfs. Permissions (mode/uid/gid) are stored in every Unix inode.

---

## Task 2: Unix Common — Inode Helpers

**Status:** DONE
**Depends on:** Task 1
**Files to read first:** `src/fs/entry.rs` (to see FileEntry struct), `src/fs/mod.rs` (to see module structure)
**Files to create:** `src/fs/unix_common/mod.rs`, `src/fs/unix_common/inode.rs`
**Files to modify:** `src/fs/mod.rs` (add `pub mod unix_common;`), `src/fs/entry.rs` (wire up `mode_string()`)
**Verify:** `cargo test --lib` passes, write unit tests for mode string and timestamp formatting

### What to do

Create `src/fs/unix_common/` module with shared helpers that every Unix filesystem (ext, btrfs, xfs, UFS, etc.) will use to build `FileEntry` objects from inode data.

### Create `src/fs/unix_common/mod.rs`
```rust
pub mod inode;
// pub mod bitmap;   // Task 3
// pub mod compact;  // Task 4
// pub mod validate; // Task 5
```

### Create `src/fs/unix_common/inode.rs`

Functions to implement:

1. **`unix_mode_string(mode: u32) -> String`**
   - Input: raw mode bits from inode (e.g., `0o040755` for a directory with rwxr-xr-x)
   - Output: 10-char string like `"drwxr-xr-x"` or `"-rw-r--r--"` or `"lrwxrwxrwx"`
   - First char: `d` (dir), `-` (file), `l` (symlink), `b` (block dev), `c` (char dev), `p` (fifo), `s` (socket)
   - Handle setuid (`s`/`S` in owner-x), setgid (`s`/`S` in group-x), sticky (`t`/`T` in other-x)
   - Mode bit masks: `S_IFMT=0o170000`, `S_IFDIR=0o040000`, `S_IFREG=0o100000`, `S_IFLNK=0o120000`, `S_IFBLK=0o060000`, `S_IFCHR=0o020000`, `S_IFIFO=0o010000`, `S_IFSOCK=0o140000`

2. **`unix_file_type(mode: u32) -> UnixFileType`**
   - Enum: `Regular, Directory, Symlink, BlockDevice, CharDevice, Fifo, Socket, Unknown`
   - Extracts type from `mode & 0o170000`

3. **`unix_entry_from_inode(name: &str, path: &str, mode: u32, size: u64, inode_num: u64, mtime: i64, uid: u32, gid: u32) -> FileEntry`**
   - Dispatches on `unix_file_type(mode)` to call appropriate `FileEntry` constructor
   - Sets `mode`, `uid`, `gid` fields
   - Sets `modified` via `format_unix_timestamp(mtime)`
   - For symlinks: creates with `EntryType::Symlink` (symlink_target filled later by caller after reading link data)
   - For block/char devices: creates with `EntryType::Special`, sets `special_type` to "block device" / "char device"
   - For fifos/sockets: creates with `EntryType::Special`, sets `special_type` to "fifo" / "socket"

4. **`format_unix_timestamp(seconds: i64) -> String`**
   - Converts Unix epoch seconds to `"YYYY-MM-DD HH:MM:SS"` format
   - No external crate needed — manual conversion from epoch (handle leap years, etc.)
   - Or use the same approach as existing timestamp formatting in the codebase

5. **`device_major_minor(dev: u32) -> (u32, u32)`**
   - Linux: `major = (dev >> 8) & 0xFF`, `minor = dev & 0xFF` (simplified)
   - Returns tuple for display in metadata panel

### Wire up in `src/fs/entry.rs`

Update `FileEntry::mode_string()` to call `crate::fs::unix_common::inode::unix_mode_string(mode)` when `self.mode` is `Some`.

### Unit tests

```rust
#[test]
fn test_mode_string_regular_file() {
    assert_eq!(unix_mode_string(0o100644), "-rw-r--r--");
}
#[test]
fn test_mode_string_directory() {
    assert_eq!(unix_mode_string(0o040755), "drwxr-xr-x");
}
#[test]
fn test_mode_string_symlink() {
    assert_eq!(unix_mode_string(0o120777), "lrwxrwxrwx");
}
#[test]
fn test_mode_string_setuid() {
    assert_eq!(unix_mode_string(0o104755), "-rwsr-xr-x");
}
#[test]
fn test_mode_string_sticky() {
    assert_eq!(unix_mode_string(0o041755), "drwxr-xr-t");
}
#[test]
fn test_format_timestamp() {
    assert_eq!(format_unix_timestamp(0), "1970-01-01 00:00:00");
    assert_eq!(format_unix_timestamp(1700000000), "2023-11-14 22:13:20");
}
```

---

## Task 3: Unix Common — Bitmap Utilities

**Status:** DONE
**Depends on:** Task 2 (unix_common module must exist)
**Files to read first:** `src/fs/unix_common/mod.rs`
**Partclone reference:** `../partclone/src/bitmap.h` (C bitmap macros), `../partclone/src/extfsclone.c` lines 100-257 (how ext reads block bitmaps)
**Files to create:** `src/fs/unix_common/bitmap.rs`
**Files to modify:** `src/fs/unix_common/mod.rs` (uncomment `pub mod bitmap;`)
**Verify:** `cargo test --lib` — write unit tests for bit operations

### What to do

Create a `BitmapReader` struct that wraps a byte slice and provides efficient bit-level operations. Used by ext (block/inode bitmaps), xfs (AG bitmaps), UFS (cylinder group bitmaps), Minix (zone maps).

### Create `src/fs/unix_common/bitmap.rs`

```rust
pub struct BitmapReader<'a> {
    data: &'a [u8],
    bit_count: u64,  // total valid bits (may be less than data.len() * 8)
}
```

Methods:
- `new(data: &[u8], bit_count: u64) -> Self`
- `is_bit_set(index: u64) -> bool` — `data[index / 8] & (1 << (index % 8)) != 0` (little-endian bit order, matching ext/Linux convention)
- `count_set_bits() -> u64` — use popcount per byte
- `count_clear_bits() -> u64` — `bit_count - count_set_bits()`
- `highest_set_bit() -> Option<u64>` — scan bytes from end backwards, find highest set bit. Used for `last_data_byte()` in ext.
- `lowest_clear_bit() -> Option<u64>` — scan forward for first free block
- `iter_set_bits() -> impl Iterator<Item = u64>` — yields indices of all set bits in order
- `iter_clear_bits() -> impl Iterator<Item = u64>` — yields indices of all clear bits

**Important**: ext uses little-endian bit order within each byte (bit 0 = LSB = block N*8+0). This matches standard Linux bitmap convention.

### Unit tests

```rust
#[test]
fn test_single_byte() {
    let data = [0b10100101u8]; // bits 0,2,5,7 set
    let bm = BitmapReader::new(&data, 8);
    assert!(bm.is_bit_set(0));
    assert!(!bm.is_bit_set(1));
    assert!(bm.is_bit_set(2));
    assert!(bm.is_bit_set(5));
    assert!(bm.is_bit_set(7));
    assert_eq!(bm.count_set_bits(), 4);
    assert_eq!(bm.highest_set_bit(), Some(7));
}

#[test]
fn test_partial_byte() {
    let data = [0xFF];
    let bm = BitmapReader::new(&data, 5); // only 5 bits valid
    assert_eq!(bm.count_set_bits(), 5); // not 8
}

#[test]
fn test_iter_set_bits() {
    let data = [0b00000101u8]; // bits 0, 2
    let bm = BitmapReader::new(&data, 8);
    let bits: Vec<u64> = bm.iter_set_bits().collect();
    assert_eq!(bits, vec![0, 2]);
}
```

---

## Task 4: Unix Common — Compact Reader Framework

**Status:** DONE
**Depends on:** Task 2 (unix_common module must exist)
**Files to read first:** `src/fs/fat.rs` (search for `CompactFatReader` — study the `Read` impl and virtual layout logic), `src/fs/ntfs.rs` (search for `CompactNtfsReader` — compare structure)
**Files to create:** `src/fs/unix_common/compact.rs`
**Files to modify:** `src/fs/unix_common/mod.rs` (uncomment `pub mod compact;`)
**Verify:** `cargo test --lib` — write unit tests with mock data

### What to do

Extract the common streaming `Read` logic that's duplicated across all five existing CompactReaders into a generic framework. Each filesystem's CompactReader will become a thin wrapper that builds a layout description and delegates streaming to this shared implementation.

### Design

The key insight: every CompactReader has the same virtual image structure:
1. **Pre-built header sections** (boot sector, rebuilt FAT/MFT/superblock, etc.) — served from `Vec<u8>` buffers
2. **Mapped data blocks** — old block N in source maps to new position M in output, read on-demand from source
3. **Zero-fill regions** — padding between sections

### Create `src/fs/unix_common/compact.rs`

```rust
/// A section in the virtual compacted image.
pub enum CompactSection {
    /// Pre-built data served from memory (boot sectors, rebuilt metadata, etc.)
    PreBuilt(Vec<u8>),
    /// A contiguous run of mapped blocks from the source.
    /// Maps new sequential blocks starting at virtual offset to old source blocks.
    MappedBlocks {
        /// For each new block index in this section, the old block number in the source.
        /// new_block[i] reads from source at old_blocks[i] * block_size + source_data_offset.
        old_blocks: Vec<u64>,
    },
    /// Zero-filled region of given byte length.
    Zeros(u64),
}

pub struct CompactLayout {
    pub sections: Vec<CompactSection>,
    pub block_size: usize,
    /// Absolute byte offset in source where data blocks start
    /// (e.g., after reserved sectors, FAT tables, etc.)
    pub source_data_start: u64,
    /// Absolute byte offset of the partition in the source device
    pub source_partition_offset: u64,
}

impl CompactLayout {
    pub fn total_size(&self) -> u64 { /* sum of all section sizes */ }
}

/// Generic streaming reader over a CompactLayout.
pub struct CompactStreamReader<R: Read + Seek> {
    source: R,
    layout: CompactLayout,
    position: u64,        // current virtual read position
    total_size: u64,
    block_buf: Vec<u8>,   // reusable buffer for reading one block
}

impl<R: Read + Seek> CompactStreamReader<R> {
    pub fn new(source: R, layout: CompactLayout) -> Self { ... }
}

impl<R: Read + Seek + Send> Read for CompactStreamReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // 1. Find which section contains self.position
        // 2. If PreBuilt: copy bytes from the Vec<u8> buffer
        // 3. If MappedBlocks: calculate which block, seek source, read block, copy bytes
        // 4. If Zeros: fill with 0x00
        // 5. Handle reads that span section boundaries (fill buf partially, advance to next section)
        // 6. Handle reads that span block boundaries within MappedBlocks
    }
}
```

### Interaction with existing code

- Do NOT modify existing CompactFatReader/CompactNtfsReader/etc. yet — those continue working as-is
- New filesystems (ext, btrfs) will use this framework
- Existing readers can be migrated incrementally in future refactoring sessions

### Unit tests

Create a test with mock sections:
```rust
#[test]
fn test_prebuilt_section() {
    let layout = CompactLayout {
        sections: vec![CompactSection::PreBuilt(vec![0xAA; 512])],
        block_size: 512,
        source_data_start: 0,
        source_partition_offset: 0,
    };
    let source = Cursor::new(vec![0u8; 0]); // unused
    let mut reader = CompactStreamReader::new(source, layout);
    let mut buf = vec![0u8; 512];
    reader.read_exact(&mut buf).unwrap();
    assert!(buf.iter().all(|&b| b == 0xAA));
}

#[test]
fn test_mapped_blocks() {
    // Source has 4 blocks of 512 bytes each. Map blocks [3, 1] (reverse order).
    let mut source_data = vec![0u8; 2048];
    source_data[512..1024].fill(0xBB);  // block 1
    source_data[1536..2048].fill(0xDD); // block 3
    let layout = CompactLayout {
        sections: vec![CompactSection::MappedBlocks {
            old_blocks: vec![3, 1],
        }],
        block_size: 512,
        source_data_start: 0,
        source_partition_offset: 0,
    };
    let mut reader = CompactStreamReader::new(Cursor::new(source_data), layout);
    let mut buf = vec![0u8; 1024];
    reader.read_exact(&mut buf).unwrap();
    assert!(buf[..512].iter().all(|&b| b == 0xDD));  // block 3 first
    assert!(buf[512..].iter().all(|&b| b == 0xBB));   // block 1 second
}

#[test]
fn test_cross_section_read() {
    // PreBuilt(4 bytes) + Zeros(4 bytes), read 8 bytes at once
    let layout = CompactLayout {
        sections: vec![
            CompactSection::PreBuilt(vec![0xFF; 4]),
            CompactSection::Zeros(4),
        ],
        block_size: 512,
        source_data_start: 0,
        source_partition_offset: 0,
    };
    let mut reader = CompactStreamReader::new(Cursor::new(vec![]), layout);
    let mut buf = vec![0u8; 8];
    reader.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, &[0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00]);
}
```

---

## Task 5: Unix Common — Validation Helpers

**Status:** DONE
**Depends on:** Task 2 (unix_common module must exist)
**Files to read first:** `src/fs/fat.rs` (search for `validate_fat_integrity`), `src/fs/ntfs.rs` (search for `validate_ntfs_integrity`) — understand the pattern
**Files to create:** `src/fs/unix_common/validate.rs`
**Files to modify:** `src/fs/unix_common/mod.rs` (uncomment `pub mod validate;`)
**Verify:** `cargo build`

### What to do

Create small shared validation helpers. These are simple utility functions — no complex logic.

### Create `src/fs/unix_common/validate.rs`

```rust
use anyhow::{bail, Result};

/// Check that magic bytes at a given offset match expected value.
pub fn validate_magic(data: &[u8], offset: usize, expected: &[u8], fs_name: &str) -> Result<()> {
    if data.len() < offset + expected.len() {
        bail!("{fs_name}: data too short for magic check at offset {offset}");
    }
    if &data[offset..offset + expected.len()] != expected {
        bail!("{fs_name}: invalid magic at offset {offset}");
    }
    Ok(())
}

/// Check that a value is a power of two.
pub fn validate_power_of_two(value: u64, name: &str) -> Result<()> {
    if value == 0 || (value & (value - 1)) != 0 {
        bail!("{name}: {value} is not a power of two");
    }
    Ok(())
}

/// Check that a value falls within an expected range.
pub fn validate_range(value: u64, min: u64, max: u64, name: &str) -> Result<()> {
    if value < min || value > max {
        bail!("{name}: {value} not in range [{min}, {max}]");
    }
    Ok(())
}

/// Accumulator for validation results (non-fatal warnings + fatal errors).
#[derive(Default)]
pub struct ValidationResult {
    pub warnings: Vec<String>,
    pub errors: Vec<String>,
}

impl ValidationResult {
    pub fn warn(&mut self, msg: impl Into<String>) { self.warnings.push(msg.into()); }
    pub fn error(&mut self, msg: impl Into<String>) { self.errors.push(msg.into()); }
    pub fn has_errors(&self) -> bool { !self.errors.is_empty() }
}
```

---

## Task 6: GUI Updates for Symlinks, Specials, Permissions

**Status:** DONE
**Depends on:** Task 1, Task 2
**Files to read first:** `src/gui/browse_view.rs` — understand how the tree and metadata panel are rendered. Look for how `FileEntry` fields are displayed, how `FileContent` is used, and how HFS type/creator codes are shown in the metadata panel.
**Files to modify:** `src/gui/browse_view.rs`
**Verify:** `cargo build` succeeds. Visual verification requires running with an ext/btrfs partition (comes later).

### What to do

Update the browse view to handle the new entry types and metadata fields.

### Changes to `src/gui/browse_view.rs`

1. **Tree rendering** — where directory entries are rendered in the tree:
   - Symlinks: append ` -> {target}` to the display name (or show in lighter color)
   - Special files: append type label in parentheses, e.g., `null (char device)`
   - Both should be leaf nodes (not expandable like directories)

2. **File selection** — when a symlink is clicked:
   - Set `FileContent::Text(target_path)` as the content — show the symlink target as text in the preview pane
   - No need to try to read the file contents

3. **File selection** — when a special is clicked:
   - Show "Special file — no preview available" or similar
   - Don't attempt to read file contents

4. **Metadata panel** — where file info is shown (name, size, modified date, HFS type/creator):
   - Add permissions line: show `mode_string()` result when `mode` is `Some` (e.g., `Permissions: drwxr-xr-x`)
   - Add ownership line: show `uid:gid` when `uid`/`gid` are `Some` (e.g., `Owner: 1000:1000`)
   - Add symlink target line when `symlink_target` is `Some`
   - Add special file type line when `special_type` is `Some`

5. **Entry type icon/prefix** — if the tree uses any icon or prefix characters:
   - Symlinks: consider `@` prefix or other visual cue
   - Specials: consider showing type indicator

### Pattern to follow

Look at how HFS `type_code` and `creator_code` are conditionally displayed in the metadata panel. The permissions/ownership display follows the same pattern — only show when the field is `Some`.

---

## Task 7: ext2/3/4 Filesystem Browsing

**Status:** DONE
**Depends on:** Tasks 1-3 (entry types, unix_common/inode, unix_common/bitmap)
**Files to read first:** `src/fs/fat.rs` (the `FatFilesystem` struct and `Filesystem` trait impl — as structural reference), `src/fs/filesystem.rs` (trait definition)
**Partclone reference:** `../partclone/src/extfsclone.c` (bitmap reading algorithm), `../partclone/src/btrfs/ctree.h` or Linux kernel `fs/ext4/ext4.h` for on-disk struct layouts
**Files to create:** `src/fs/ext.rs`
**Verify:** `cargo build` succeeds. Create small unit tests with hand-crafted ext2 superblock bytes.

### What to do

Implement `ExtFilesystem` struct that implements the `Filesystem` trait for ext2/3/4 partitions. This task covers browsing only (compaction, resize, validate come in later tasks).

### On-disk format quick reference

**Superblock** (at partition_offset + 1024, 1024 bytes):
| Offset | Size | Field | Notes |
|--------|------|-------|-------|
| 0x00 | u32 LE | s_inodes_count | Total inodes |
| 0x04 | u32 LE | s_blocks_count_lo | Total blocks (low 32 bits) |
| 0x18 | u32 LE | s_first_data_block | 0 for 4K blocks, 1 for 1K blocks |
| 0x1C | u32 LE | s_log_block_size | Block size = 1024 << this value |
| 0x20 | u32 LE | s_log_cluster_size | Cluster size (bigalloc) |
| 0x24 | u32 LE | s_blocks_per_group | Blocks per block group |
| 0x28 | u32 LE | s_clusters_per_group | Clusters per group (bigalloc) |
| 0x2C | u32 LE | s_inodes_per_group | Inodes per block group |
| 0x38 | u16 LE | s_magic | Must be 0xEF53 |
| 0x3E | u16 LE | s_inode_size | Inode struct size (128 or 256) |
| 0x5C | u32 LE | s_feature_compat | Compatible features |
| 0x60 | u32 LE | s_feature_incompat | Incompatible features |
| 0x64 | u32 LE | s_feature_ro_compat | Read-only compatible features |
| 0x78 | char[16] | s_volume_name | Volume label (null-terminated) |
| 0x150 | u32 LE | s_blocks_count_hi | Total blocks high 32 bits (64-bit mode) |
| 0x158 | u16 LE | s_desc_size | Group descriptor size (64-bit mode: 64, else 32) |

**Feature flags:**
- `INCOMPAT_EXTENTS = 0x40` — ext4 extent trees
- `INCOMPAT_64BIT = 0x80` — 64-bit block numbers, 64-byte group descriptors
- `COMPAT_HAS_JOURNAL = 0x04` — ext3/4 journal (inode 8)
- `RO_COMPAT_GDT_CSUM = 0x10` — group descriptor checksums, `BLOCK_UNINIT` flag

**Block Group Descriptor** (at superblock_block + 1, array of 32 or 64 bytes each):
| Offset | Size | Field | Notes |
|--------|------|-------|-------|
| 0x00 | u32 LE | bg_block_bitmap_lo | Block bitmap block (low) |
| 0x04 | u32 LE | bg_inode_bitmap_lo | Inode bitmap block (low) |
| 0x08 | u32 LE | bg_inode_table_lo | Inode table start block (low) |
| 0x0C | u16 LE | bg_free_blocks_count_lo | Free blocks in group |
| 0x0E | u16 LE | bg_free_inodes_count_lo | Free inodes in group |
| 0x12 | u16 LE | bg_flags | `0x01 = INODE_UNINIT`, `0x02 = BLOCK_UNINIT` |
| 0x20-0x3F | | 64-bit extensions | Only present if `INCOMPAT_64BIT` and `desc_size >= 64` |

**Inode** (128 or 256 bytes, in inode table):
| Offset | Size | Field | Notes |
|--------|------|-------|-------|
| 0x00 | u16 LE | i_mode | File type + permissions |
| 0x02 | u16 LE | i_uid | Owner UID (low 16 bits) |
| 0x04 | u32 LE | i_size_lo | File size low 32 bits |
| 0x08 | u32 LE | i_atime | Access time (Unix epoch) |
| 0x0C | u32 LE | i_ctime | Change time |
| 0x10 | u32 LE | i_mtime | Modification time |
| 0x1A | u16 LE | i_links_count | Hard link count |
| 0x1C | u32 LE | i_blocks_lo | 512-byte blocks allocated |
| 0x20 | u32 LE | i_flags | Inode flags (0x80000 = EXTENTS) |
| 0x28 | [u32; 15] | i_block | 12 direct + 3 indirect block pointers, OR ext4 extent tree root |
| 0x6C | u32 LE | i_size_hi | File size high 32 bits (regular files) |
| 0x78 | u16 LE | i_uid_hi | Owner UID high 16 bits |
| 0x7A | u16 LE | i_gid_hi | Owner GID high 16 bits |

**Inode flags:** `EXT4_EXTENTS_FL = 0x80000` — uses extent tree instead of indirect blocks

**ext4 Extent Header** (at i_block[0..2]):
| Offset | Size | Field |
|--------|------|-------|
| 0x00 | u16 LE | eh_magic | Must be 0xF30A |
| 0x02 | u16 LE | eh_entries | Number of valid entries |
| 0x04 | u16 LE | eh_max | Max entries capacity |
| 0x06 | u16 LE | eh_depth | 0 = leaf (extents), >0 = internal (index entries) |

**ext4 Extent (leaf, depth=0):**
| Offset | Size | Field |
|--------|------|-------|
| 0x00 | u32 LE | ee_block | Logical file block |
| 0x04 | u16 LE | ee_len | Number of blocks (>32768 = uninitialized) |
| 0x06 | u16 LE | ee_start_hi | Physical block high 16 bits |
| 0x08 | u32 LE | ee_start_lo | Physical block low 32 bits |

**ext4 Extent Index (internal, depth>0):**
| Offset | Size | Field |
|--------|------|-------|
| 0x00 | u32 LE | ei_block | Logical block covered |
| 0x04 | u32 LE | ei_leaf_lo | Child node block low |
| 0x08 | u16 LE | ei_leaf_hi | Child node block high |

**Directory Entry** (variable length, packed in data blocks):
| Offset | Size | Field |
|--------|------|-------|
| 0x00 | u32 LE | inode | Inode number (0 = deleted) |
| 0x04 | u16 LE | rec_len | Total entry length (for alignment/skipping) |
| 0x06 | u8 | name_len | Filename length |
| 0x07 | u8 | file_type | 1=regular, 2=directory, 7=symlink, etc. |
| 0x08 | [u8] | name | Filename (NOT null-terminated) |

**Special inodes:** 1 = bad blocks, 2 = root directory, 8 = journal

**Symlinks:**
- If `i_size < 60` and no blocks allocated: target stored directly in `i_block[0..14]` as bytes (fast symlink)
- Otherwise: target stored in data block(s) pointed to by `i_block`

### Implementation structure

```rust
pub struct ExtFilesystem<R> {
    reader: R,
    partition_offset: u64,
    block_size: u64,
    total_blocks: u64,
    inodes_per_group: u32,
    blocks_per_group: u32,
    inode_size: u16,
    group_count: u32,
    group_descriptors: Vec<GroupDescriptor>,  // parsed GDTs
    label: Option<String>,
    ext_version: ExtVersion,  // Ext2, Ext3, Ext4
    has_extents: bool,
    is_64bit: bool,
    desc_size: u16,
}

enum ExtVersion { Ext2, Ext3, Ext4 }

struct GroupDescriptor {
    block_bitmap: u64,
    inode_bitmap: u64,
    inode_table: u64,
    free_blocks: u32,
    free_inodes: u32,
    flags: u16,
}
```

Key methods:
- `open(reader, partition_offset) -> Result<Self>` — parse superblock + GDTs
- `read_inode(inode_num) -> Result<InodeData>` — locate in inode table, parse fields
- `read_inode_data(inode) -> Result<Vec<u8>>` — follow block pointers or extent tree
- `read_inode_data_blocks(inode) -> Result<Vec<u64>>` — list all block numbers (for compaction later)
- `parse_directory(data: &[u8]) -> Result<Vec<DirEntry>>` — parse directory block
- `read_symlink_target(inode) -> Result<String>` — fast or slow symlink

### htree directory handling

For ext3/4 htree-indexed directories: ignore the hash index structure entirely. Instead, read ALL data blocks of the directory inode and parse them linearly. The first block may have an htree root node (dx_root) — detect this by checking if the first directory entry is "." with a fake rec_len covering the dx_root — but we can still parse the actual entries from the subsequent blocks. This is simpler and correct, just not as fast as hash lookups.

### Filesystem trait implementation

- `root()` — read inode 2, return as directory FileEntry
- `list_directory(entry)` — read inode data, parse directory entries, for each entry read its inode to get mode/size/mtime, build FileEntry via `unix_entry_from_inode()`. Skip "." and ".." entries. Sort: directories first, then alphabetically.
- `read_file(entry, max_bytes)` — read inode data, return up to max_bytes
- `volume_label()` — from superblock `s_volume_name`
- `fs_type()` — "ext2", "ext3", or "ext4"
- `total_size()` — `total_blocks * block_size`
- `used_size()` — `(total_blocks - free_blocks) * block_size`
- `last_data_byte()` — scan block bitmaps from all groups using `BitmapReader::highest_set_bit()`, return `(highest_block + 1) * block_size`

---

## Task 8: Wire ext into mod.rs Routing

**Status:** DONE
**Depends on:** Task 7
**Files to read first:** `src/fs/mod.rs` (current routing logic)
**Files to modify:** `src/fs/mod.rs`
**Verify:** `cargo build`

### What to do

1. Add `pub mod ext;` to module declarations

2. Add ext exports:
   ```rust
   pub use ext::ExtFilesystem;
   // CompactExtReader, resize_ext_in_place, validate_ext_integrity — added later
   ```

3. Update `detect_filesystem_type()` — add ext detection:
   ```rust
   // After HFS/HFS+ check, check for ext at partition_offset + 1024 + 0x38
   if reader.seek(SeekFrom::Start(partition_offset + 1024 + 0x38)).is_ok() {
       let mut magic = [0u8; 2];
       if reader.read_exact(&mut magic).is_ok() && magic == [0x53, 0xEF] { // 0xEF53 LE
           return "ext";
       }
   }
   ```

4. Update `open_filesystem()`:
   - For type byte `0x83`: open as ext (try ext magic, if not found try btrfs — added later)
   - For type byte `0x00` (auto-detect): add "ext" to the match
   ```rust
   "ext" => Ok(Box::new(ext::ExtFilesystem::open(reader, partition_offset)?)),
   ```

5. Update `compact_partition_reader()`:
   - For type byte `0x83`: return `None` for now (compaction added in Task 9)
   - For type byte `0x00`: same pattern

6. Remove the existing `0x83 => Err(FilesystemError::Unsupported("ext2/3/4 browsing not yet supported"))` error

---

## Task 9: ext2/3/4 Compaction — CompactExtReader

**Status:** DONE
**Depends on:** Tasks 3, 4, 7 (bitmap utils, compact framework, ext browsing)
**Files to read first:** `src/fs/ext.rs` (the ExtFilesystem you built in Task 7), `src/fs/unix_common/compact.rs` (the framework from Task 4), `src/fs/unix_common/bitmap.rs`
**Partclone reference:** `../partclone/src/extfsclone.c` lines 100-257 (how it identifies used blocks, handles `BLOCK_UNINIT`)
**Files to modify:** `src/fs/ext.rs`, `src/fs/mod.rs`
**Verify:** `cargo build`. Unit test: construct CompactExtReader from a test ext2 image, verify it produces a valid smaller image.

### What to do

Add `CompactExtReader` to `src/fs/ext.rs` that produces a defragmented, compacted ext image using the `CompactStreamReader` framework.

### Algorithm

1. Parse superblock and group descriptors (reuse from ExtFilesystem)
2. For each block group:
   - If `BLOCK_UNINIT` flag set: all blocks in group are free, skip
   - Otherwise: read block bitmap, use `BitmapReader` to identify allocated blocks
3. Build `old_blocks: Vec<u64>` — list of all allocated data block numbers in order
4. Build `new_to_old` and `old_to_new` mappings
5. Pre-build metadata sections:
   - **Boot block**: first 1024 bytes (before superblock) — copy from source
   - **Superblock**: patched with new `s_blocks_count` = metadata blocks + allocated data blocks
   - **Group descriptors**: rebuilt for compacted layout (fewer groups, updated bitmap/inode table locations)
   - **Block bitmaps**: rebuilt (all set for full groups, partial for last group)
   - **Inode bitmaps**: copied from source (inodes don't move)
   - **Inode tables**: copied from source, BUT block pointers in each inode must be remapped using `old_to_new`
6. Build `CompactLayout`:
   - `PreBuilt(boot_block + superblock + group_descriptors + bitmaps + inode_tables)`
   - `MappedBlocks { old_blocks }` for data blocks
7. Wrap in `CompactStreamReader`

### Key detail: Inode block pointer remapping

When copying inode tables, each inode's block pointers must be updated:
- **Direct blocks (i_block[0..11])**: remap each non-zero value via `old_to_new`
- **Indirect blocks (i_block[12..14])**: the indirect block itself is a data block that contains an array of block pointers — it needs remapping too. Read the indirect block, remap all pointers, include it as a pre-built section.
- **Extent trees (ext4)**: leaf extents have `ee_start_lo/hi` physical block numbers that need remapping. Index entries have `ei_leaf_lo/hi` that also need remapping.

This is the most complex part — take care with ext4 extent trees.

### Wire up in mod.rs

Update `compact_partition_reader()` for type `0x83` and auto-detect `0x00`:
```rust
"ext" => {
    let (reader, info) = ext::CompactExtReader::new(reader, partition_offset).ok()?;
    Some((Box::new(reader), CompactResult { ... }))
}
```

---

## Task 10: ext2/3/4 Resize + Validate + Restore Wiring

**Status:** DONE
**Depends on:** Task 9
**Files to read first:** `src/restore/mod.rs` (search for `PartitionFsType` enum and the resize/validate match arms), `src/rbformats/mod.rs` (search for `patch_bpb_hidden_sectors` — see the patching chain), `src/rbformats/vhd.rs` (search for `resize_fat_in_place`)
**Files to modify:** `src/fs/ext.rs`, `src/restore/mod.rs`, `src/rbformats/vhd.rs`
**Verify:** `cargo build`

### What to do — three sub-parts

#### Part A: `resize_ext_in_place()` in `src/fs/ext.rs`

Grow-only resize. Signature matches other resize functions:
```rust
pub fn resize_ext_in_place(
    file: &mut (impl Read + Write + Seek),
    partition_offset: u64,
    new_total_sectors: u64,  // or new_total_bytes depending on pattern
    log_cb: &mut impl FnMut(&str),
) -> Result<()>
```

Algorithm:
1. Parse superblock at `partition_offset + 1024`
2. Calculate new total blocks from new size
3. If new blocks <= current blocks: log warning, return Ok (nothing to grow)
4. Calculate new block group count
5. For each new block group beyond current count:
   - Calculate block bitmap, inode bitmap, inode table positions
   - Write zeroed block bitmap (all free)
   - Write zeroed inode bitmap (all free)
   - Write zeroed inode table
6. Update/append group descriptors
7. Patch superblock: `s_blocks_count`, `s_free_blocks_count`
8. Write patched superblock at offset 1024
9. Write backup superblocks at sparse_super positions (groups 0, 1, powers of 3, 5, 7)

#### Part B: `validate_ext_integrity()` in `src/fs/ext.rs`

```rust
pub fn validate_ext_integrity(
    file: &mut (impl Read + Seek),
    partition_offset: u64,
    log_cb: &mut impl FnMut(&str),
) -> Result<Vec<String>>  // returns warnings
```

Checks:
1. Superblock magic 0xEF53
2. Block size is power of two (1024, 2048, or 4096)
3. Block group count matches `ceil(total_blocks / blocks_per_group)`
4. Sum of free blocks across all group descriptors matches superblock free count
5. Inode count consistency

#### Part C: Wire into restore

In `src/restore/mod.rs`:
1. Add `Ext` to `PartitionFsType` enum
2. In `detect_partition_fs_type()`: check for 0xEF53 at offset+1024+56, return `PartitionFsType::Ext`
3. In resize match arms: add `PartitionFsType::Ext => resize_ext_in_place(...)`
4. In validate match arms: add `PartitionFsType::Ext => validate_ext_integrity(...)`

In `src/rbformats/vhd.rs`:
1. Import `resize_ext_in_place`
2. Add ext resize call where other filesystem resizes are called

**Note:** ext doesn't store partition offset in superblock, so NO hidden sectors patching is needed. Don't add a `patch_ext_hidden_sectors` call.

---

## Task 11: btrfs Filesystem Browsing

**Status:** DONE
**Depends on:** Tasks 1-2 (entry types, unix_common/inode)
**Files to read first:** `src/fs/ext.rs` (your ext implementation as structural reference)
**Partclone reference:** `../partclone/src/btrfsclone.c` (tree walking), `../partclone/src/btrfs/ctree.h` (on-disk structs)
**Files to create:** `src/fs/btrfs.rs`
**New dependencies:** `crc32c` crate (add to Cargo.toml) — btrfs uses crc32c checksums on all metadata
**Verify:** `cargo build`. Unit tests with hand-crafted superblock bytes.

### What to do

This is the most complex filesystem to implement. Break it into sub-parts within this task.

### Part A: Superblock parsing

**Superblock** at partition_offset + 0x10000 (64KB), 4096 bytes:
| Offset | Size | Field | Notes |
|--------|------|-------|-------|
| 0x00 | [u8;32] | csum | Checksum (crc32c of rest) |
| 0x20 | [u8;16] | fsid | Filesystem UUID |
| 0x30 | u64 LE | bytenr | Physical address of this block |
| 0x38 | u64 LE | flags | |
| 0x40 | [u8;8] | magic | `_BHRfS_M` |
| 0x48 | u64 LE | generation | Transaction generation |
| 0x50 | u64 LE | root | Root tree root logical address |
| 0x58 | u64 LE | chunk_root | Chunk tree root logical address |
| 0x60 | u64 LE | log_root | Log tree root |
| 0x70 | u64 LE | total_bytes | Total device bytes |
| 0x78 | u64 LE | bytes_used | Used bytes |
| 0x80 | u64 LE | root_dir_objectid | Usually 6 (root tree dir) |
| 0x90 | u64 LE | num_devices | Number of devices |
| 0x98 | u32 LE | sector_size | Usually 4096 |
| 0x9C | u32 LE | node_size | Usually 16384 |
| 0xA0 | u32 LE | leaf_size | Same as node_size |
| 0xC0 | u64 LE | compat_flags | |
| 0xC8 | u64 LE | compat_ro_flags | |
| 0xD0 | u64 LE | incompat_flags | |
| 0x12B | [u8;256] | label | Volume label |
| 0x32B | u32 LE | sys_chunk_array_size | |
| 0x32F | [u8;2048] | sys_chunk_array | Bootstrap chunk map entries |

### Part B: Chunk tree and logical-to-physical mapping

The `sys_chunk_array` in the superblock contains enough chunk mappings to read the chunk tree itself. Parse it as:
- Series of: `btrfs_disk_key` (17 bytes: objectid u64 + type u8 + offset u64) + `btrfs_chunk` (variable size)
- Each chunk maps a logical address range to physical device offset(s)

`btrfs_chunk`:
| Offset | Size | Field |
|--------|------|-------|
| 0x00 | u64 LE | length | Chunk length in bytes |
| 0x08 | u64 LE | owner | Owner (dev extent tree objectid) |
| 0x10 | u64 LE | stripe_len | |
| 0x18 | u64 LE | type | SINGLE=0, DUP, RAID0, etc. |
| 0x30 | u16 LE | num_stripes | |
| 0x32 | u16 LE | sub_stripes | |
| 0x34 | ... | stripes[] | Array of (devid u64, offset u64, dev_uuid [16]) |

Build a `Vec<ChunkMapping>` sorted by logical address. To translate logical→physical:
```
find chunk where chunk.logical_start <= addr < chunk.logical_start + chunk.length
physical = chunk.stripes[0].offset + (addr - chunk.logical_start)
```
For DUP: two stripes point to same data — use stripe[0].

After bootstrapping from `sys_chunk_array`, read the full chunk tree (at `chunk_root` logical address) and add any additional mappings.

### Part C: B-tree node parsing and traversal

All btrfs metadata is in B-tree nodes of `node_size` bytes.

**Node header** (first 101 bytes):
| Offset | Size | Field |
|--------|------|-------|
| 0x00 | [u8;32] | csum | |
| 0x20 | [u8;16] | fsid | |
| 0x30 | u64 LE | bytenr | Logical address of this node |
| 0x38 | u64 LE | flags | |
| 0x48 | [u8;16] | chunk_tree_uuid | |
| 0x58 | u64 LE | generation | |
| 0x60 | u64 LE | owner | Tree that owns this node |
| 0x68 | u32 LE | nritems | Number of items |
| 0x6C | u8 | level | 0=leaf, >0=internal |

**Internal node** (level > 0): after header, array of key_ptr entries:
| Size | Field |
|------|-------|
| 17 bytes | key (objectid u64 + type u8 + offset u64) |
| 8 bytes | blockptr (u64 LE logical address of child) |
| 8 bytes | generation |

**Leaf node** (level == 0): after header, array of item entries:
| Size | Field |
|------|-------|
| 17 bytes | key |
| 4 bytes | offset (u32 LE — byte offset from start of leaf data area) |
| 4 bytes | size (u32 LE — item data size) |

Item data is stored at the END of the leaf, growing backwards. Item at index i has data at `node_start + sizeof(header) + nritems * sizeof(item_entry) + ... actually: node_start + item.offset`.

Wait — correction: `item.offset` is relative to the end of the item array. Actually in btrfs, `item.data_offset` is byte offset from the start of the leaf where the item data begins. Let me be precise: the data_offset field in each item entry is the byte offset from the beginning of the leaf node to where that item's data starts.

### Part D: Key types for browsing

| Type byte | Name | Description |
|-----------|------|-------------|
| 1 | INODE_ITEM | Inode metadata (mode, size, nlink, uid, gid, times) |
| 12 | ROOT_ITEM | Subvolume root info |
| 54 | DIR_ITEM | Directory entry (hashed) |
| 60 | DIR_INDEX | Directory entry (sequential index) — use this for listing |
| 84 | ROOT_REF | Subvolume parent reference |
| 108 | EXTENT_DATA | File data extents |

**INODE_ITEM** (160 bytes):
| Offset | Size | Field |
|--------|------|-------|
| 0x00 | u64 LE | generation |
| 0x08 | u64 LE | transid |
| 0x10 | u64 LE | size | File size |
| 0x18 | u64 LE | nbytes | Bytes allocated |
| 0x20 | u64 LE | block_group |
| 0x28 | u32 LE | nlink |
| 0x2C | u32 LE | uid |
| 0x30 | u32 LE | gid |
| 0x34 | u32 LE | mode | Unix mode bits |
| 0x38 | u64 LE | rdev | Device number (for block/char devs) |
| 0x40 | u64 LE | flags |
| 0x48 | u64 LE | sequence |
| 0x60 | timespec | atime | (sec u64 + nsec u32) |
| 0x6C | timespec | ctime |
| 0x78 | timespec | mtime |
| 0x84 | timespec | otime |

**DIR_INDEX** item data:
| Offset | Size | Field |
|--------|------|-------|
| 0x00 | u64 LE | child_objectid | Child inode/subvol objectid |
| 0x08 | u8 | child_type | `BTRFS_FT_REG_FILE=1, DIR=2, SYMLINK=7, etc.` |

Key for DIR_INDEX: `(parent_objectid, DIR_INDEX, sequence_number)`. The item data is followed by the filename bytes.

**EXTENT_DATA** item data:
| Offset | Size | Field |
|--------|------|-------|
| 0x00 | u64 LE | generation |
| 0x08 | u64 LE | ram_bytes | Uncompressed size |
| 0x10 | u8 | compression | 0=none, 1=zlib, 2=lzo, 3=zstd |
| 0x11 | u8 | encryption |
| 0x12 | u16 LE | other_encoding |
| 0x15 | u8 | type | 0=inline, 1=regular, 2=prealloc |
| For inline (type==0): |
| 0x15 | [u8] | data | Inline file data (or symlink target) |
| For regular (type==1): |
| 0x15 | u64 LE | disk_bytenr | Logical address of extent on disk |
| 0x1D | u64 LE | disk_num_bytes | Size of extent on disk |
| 0x25 | u64 LE | offset | Offset within extent |
| 0x2D | u64 LE | num_bytes | Bytes used from extent |

### Part E: Implementation structure

```rust
pub struct BtrfsFilesystem<R> {
    reader: R,
    partition_offset: u64,
    node_size: u32,
    sector_size: u32,
    total_bytes: u64,
    bytes_used: u64,
    label: Option<String>,
    chunk_map: Vec<ChunkMapping>,  // sorted by logical address
    root_tree_root: u64,           // logical address
    fs_tree_root: u64,             // logical address (found via root tree)
}

struct ChunkMapping {
    logical: u64,
    physical: u64,  // stripe[0] offset
    length: u64,
}
```

Key methods:
- `open(reader, partition_offset) -> Result<Self>` — parse superblock, build chunk map from sys_chunk_array, read chunk tree for full map, find fs tree root from root tree
- `logical_to_physical(logical: u64) -> Result<u64>` — binary search chunk_map
- `read_node(logical: u64) -> Result<Vec<u8>>` — translate to physical, read node_size bytes
- `find_item(tree_root: u64, key: &BtrfsKey) -> Result<Option<Vec<u8>>>` — B-tree lookup
- `list_items(tree_root: u64, min_key: &BtrfsKey, max_key: &BtrfsKey) -> Result<Vec<(BtrfsKey, Vec<u8>)>>` — range scan
- `read_inode(objectid: u64) -> Result<InodeData>` — lookup INODE_ITEM in fs tree
- `read_file_data(objectid: u64, max_bytes: usize) -> Result<Vec<u8>>` — follow EXTENT_DATA items
- `read_symlink(objectid: u64) -> Result<String>` — inline extent data as UTF-8

### Compression handling

For file preview, if an extent has compression != 0:
- **Phase 1**: Show "compressed data — preview unavailable" for zlib/lzo
- **Phase 2** (can be done later): Add `flate2` for zlib, `lzo1x-1` crate for lzo. Zstd already available.

### Subvolume handling

When listing a directory and encountering a DIR_INDEX with `child_type == BTRFS_FT_DIR` where the child objectid refers to a subvolume (check ROOT_ITEM in root tree), switch to that subvolume's fs tree for further traversal. Show subvolumes as regular directories in the browse tree.

---

## Task 12: Wire btrfs into mod.rs Routing

**Status:** DONE
**Depends on:** Task 11
**Files to read first:** `src/fs/mod.rs`
**Files to modify:** `src/fs/mod.rs`
**Verify:** `cargo build`

### What to do

1. Add `pub mod btrfs;`

2. Update `detect_filesystem_type()`:
   ```rust
   // After ext check, check for btrfs at partition_offset + 0x10000 + 0x40
   if reader.seek(SeekFrom::Start(partition_offset + 0x10040)).is_ok() {
       let mut magic = [0u8; 8];
       if reader.read_exact(&mut magic).is_ok() && &magic == b"_BHRfS_M" {
           return "btrfs";
       }
   }
   ```

3. Update `open_filesystem()` for type byte `0x83`:
   ```rust
   0x83 => {
       // Try ext first (superblock at +1024), then btrfs (+64KB)
       let fs_type = detect_0x83_type(&mut reader, partition_offset);
       match fs_type {
           "ext" => Ok(Box::new(ext::ExtFilesystem::open(reader, partition_offset)?)),
           "btrfs" => Ok(Box::new(btrfs::BtrfsFilesystem::open(reader, partition_offset)?)),
           _ => Err(FilesystemError::Unsupported("unknown Linux filesystem".into())),
       }
   }
   ```

4. Create `detect_0x83_type()` helper — check ext magic first (more common), then btrfs magic.

5. Also add both to the auto-detect (`0x00`) path.

---

## Task 13: btrfs Compaction — CompactBtrfsReader

**Status:** DONE
**Depends on:** Tasks 4, 11 (compact framework, btrfs browsing)
**Files to modify:** `src/fs/btrfs.rs`, `src/fs/mod.rs`
**Partclone reference:** `../partclone/src/btrfsclone.c` lines 249-480 (extent tree walking for used blocks)

### What to do

Add `CompactBtrfsReader` to `src/fs/btrfs.rs`. This is the hardest compaction to implement.

### Algorithm

1. Parse superblock and build chunk map (reuse from BtrfsFilesystem)
2. Walk all trees recursively to find every allocated extent:
   - For each tree node visited: mark the node's physical blocks as used
   - For each EXTENT_DATA item with type=regular: mark the extent's physical blocks as used
   - Also mark superblock mirror locations as used (offsets 0x10000, 0x4000000, 0x4000000000)
3. Sort all used physical block ranges
4. Build `CompactLayout`:
   - `PreBuilt` for first 64KB + superblock (with patched total_bytes)
   - `MappedBlocks` for each used physical extent
5. Rebuild chunk tree to map new logical addresses to new physical locations

### Fallback strategy

If full chunk tree remapping proves too complex:
- **Simple fallback**: Use `last_data_byte()` to find the highest used physical offset, copy everything from 0 to that point. This doesn't defragment but does trim trailing free space. Much simpler and still useful.
- Mark this in the code with a TODO for full compaction later.

### Wire up in mod.rs

Same pattern as ext — add to `compact_partition_reader()` for types `0x83` and `0x00`.

---

## Task 14: btrfs Resize + Validate + Restore Wiring

**Status:** DONE
**Depends on:** Task 13
**Files to modify:** `src/fs/btrfs.rs`, `src/restore/mod.rs`, `src/rbformats/vhd.rs`

### What to do

#### `resize_btrfs_in_place()` in `src/fs/btrfs.rs`

Simpler than ext resize. btrfs dynamically allocates chunks, so growing just requires:
1. Parse superblock
2. Update `total_bytes` field in superblock
3. Update `dev_item.total_bytes` (device item in chunk tree — find and patch)
4. Rewrite superblock with updated crc32c checksum
5. btrfs will see the new space and allocate chunks into it on next mount

#### `validate_btrfs_integrity()` in `src/fs/btrfs.rs`

1. Read superblock at offset +0x10000
2. Verify magic `_BHRfS_M`
3. Verify crc32c checksum (hash bytes 0x20 to end, compare with bytes 0x00-0x1F)
4. Check `total_bytes > 0`, `node_size` is reasonable (4096-65536), `num_devices >= 1`
5. Verify root tree root and chunk tree root are within device bounds

#### Restore wiring

Same pattern as Task 10 Part C:
1. Add `PartitionFsType::Btrfs` to enum in `src/restore/mod.rs`
2. Add btrfs to `detect_partition_fs_type()` — check magic at offset+0x10040
3. Add resize/validate calls in match arms
4. No hidden sectors patching needed for btrfs

---

## Task 15: Clonezilla block_cache Extensions

**Status:** DONE
**Depends on:** Tasks 7, 11 (need to understand ext and btrfs on-disk formats)
**Files to read first:** `src/clonezilla/block_cache.rs` — search for `identify_fat_metadata_blocks` and `scan_metadata` to understand the pattern
**Files to modify:** `src/clonezilla/block_cache.rs`
**Verify:** `cargo build`

### What to do

Add metadata block identification for ext and btrfs partitions within Clonezilla images, enabling the browse view to work with partclone-compressed ext/btrfs partitions.

### Add `identify_ext_metadata_blocks()`

Follow the same pattern as `identify_fat_metadata_blocks()`. For ext, the critical metadata blocks are:
- Block containing superblock (block at byte offset 1024)
- Block group descriptor table blocks (immediately after superblock block)
- Block bitmaps and inode bitmaps for first N block groups
- Inode table blocks for first N block groups (at minimum, enough for root directory inode #2)
- Root directory data blocks (read from inode 2's block pointers)

### Add `identify_btrfs_metadata_blocks()`

- Superblock block (at 64KB offset)
- Root tree root node
- Chunk tree root node
- First few levels of the FS tree

### Wire into `scan_metadata()`

Add filesystem type detection (ext magic / btrfs magic) and call the appropriate identification function.

---

## Task 16: End-to-End Testing (Linux only)

**Status:** NOT STARTED
**Depends on:** All previous tasks (1-15)
**Platform:** Linux (requires `mkfs.ext2/3/4`, `mkfs.btrfs` — NO sudo/mount needed)
**Verify:** `cargo test --test filesystem_e2e -- --ignored` passes all integration tests

### Overview

This task creates real filesystem images using Linux tools and writes integration tests that exercise the full stack: browsing, compaction, resize, validation, and filesystem detection routing. These tests complement the existing unit tests that use hand-crafted synthetic images.

### Key discovery: No sudo needed

Both `mkfs.ext*` and `mkfs.btrfs` support populating the filesystem from a source directory without needing mount:
- `mkfs.ext2/ext4 -d <rootdir>` — copies directory contents into the image
- `mkfs.btrfs --rootdir <rootdir>` — copies directory contents into the image

This means the script works in unprivileged environments (WSL2, containers, CI).

**Caveat:** Files will have the current user's uid/gid (not configurable without root). Tests should check `uid.is_some()` / `gid.is_some()` rather than specific values, or read the actual uid at test time.

### Prerequisites

Install required tools on the Linux machine (already present on this system):
```bash
sudo apt install e2fsprogs btrfs-progs  # Debian/Ubuntu
# Verified: e2fsprogs 1.47.0, btrfs-progs 6.6.3
```

### Step 1: Create test image generation script

Create `scripts/generate-test-images.sh` (no sudo required):

```bash
#!/bin/bash
set -euo pipefail

OUTDIR="tests/fixtures"
mkdir -p "$OUTDIR"

# Prepare a common source directory structure
SRCDIR=$(mktemp -d)
trap 'rm -rf "$SRCDIR"' EXIT

echo -n "Hello, ext2!" > "$SRCDIR/hello_ext2.txt"
echo -n "Hello, ext4!" > "$SRCDIR/hello_ext4.txt"
echo -n "Hello, btrfs!" > "$SRCDIR/hello_btrfs.txt"
echo -n "nested file" > "$SRCDIR/nested.txt"

# --- Helper to build a rootdir for a given fs ---
build_rootdir() {
    local ROOTDIR="$1"
    local HELLO_CONTENT="$2"
    mkdir -p "$ROOTDIR/subdir"
    cp "$SRCDIR/$HELLO_CONTENT" "$ROOTDIR/hello.txt"
    cp "$SRCDIR/nested.txt" "$ROOTDIR/subdir/nested.txt"
    ln -s hello.txt "$ROOTDIR/link.txt"
    chmod 644 "$ROOTDIR/hello.txt"
    chmod 755 "$ROOTDIR/subdir"
}

# --- ext2 image (4 MiB, no journal) ---
echo "Creating ext2 test image..."
EXT2_ROOT=$(mktemp -d)
build_rootdir "$EXT2_ROOT" "hello_ext2.txt"
dd if=/dev/zero of="$OUTDIR/test_ext2.img" bs=1M count=4 2>/dev/null
mkfs.ext2 -L "test_ext2" -F -d "$EXT2_ROOT" "$OUTDIR/test_ext2.img" 2>&1 | tail -1
rm -rf "$EXT2_ROOT"

# --- ext4 image (16 MiB — needs >=5 MiB for journal) ---
echo "Creating ext4 test image..."
EXT4_ROOT=$(mktemp -d)
build_rootdir "$EXT4_ROOT" "hello_ext4.txt"
dd if=/dev/zero of="$OUTDIR/test_ext4.img" bs=1M count=16 2>/dev/null
mkfs.ext4 -L "test_ext4" -F -d "$EXT4_ROOT" "$OUTDIR/test_ext4.img" 2>&1 | tail -1
rm -rf "$EXT4_ROOT"

# --- btrfs image (256 MiB — btrfs minimum is ~109 MiB) ---
echo "Creating btrfs test image..."
BTRFS_ROOT=$(mktemp -d)
build_rootdir "$BTRFS_ROOT" "hello_btrfs.txt"
dd if=/dev/zero of="$OUTDIR/test_btrfs.img" bs=1M count=256 2>/dev/null
mkfs.btrfs -L "test_btrfs" -f --rootdir "$BTRFS_ROOT" "$OUTDIR/test_btrfs.img" 2>&1 | tail -1
rm -rf "$BTRFS_ROOT"

echo ""
echo "Test images created in $OUTDIR/"
ls -lh "$OUTDIR"/*.img
```

Make executable: `chmod +x scripts/generate-test-images.sh`

Add `tests/fixtures/` to `.gitignore` (images are large and generated, not checked in).

### Step 2: Create integration test file

Create `tests/filesystem_e2e.rs`. Each test is gated with `#[ignore]` so they only run when explicitly requested (since they need the fixture images).

**Important API notes** (verified against actual codebase):
- `Filesystem` trait is at `rusty_backup::fs::filesystem::Filesystem`
- `volume_label()` returns `Option<&str>`
- `open_filesystem(reader, offset, type_byte, type_string)` takes 4 params — 4th is `Option<&str>`
- `CompactExtReader::new(reader, offset)` returns `Result<(Self, CompactResult), FilesystemError>`
- `CompactResult` has fields: `original_size: u64`, `compacted_size: u64`, `clusters_used: u32`
- `resize_ext_in_place(file, offset, new_total_bytes, log_cb)` — takes `new_total_bytes: u64`
- `resize_btrfs_in_place(file, offset, new_total_bytes, log_cb)` — same pattern
- ext2 at 4 MiB has no journal → detected as "ext2". ext4 at 16 MiB has journal → detected as "ext4"

```rust
//! End-to-end tests using real filesystem images.
//!
//! These tests require pre-generated fixture images in tests/fixtures/.
//! Run `scripts/generate-test-images.sh` first (Linux only, no root needed).
//!
//! Run with: cargo test --test filesystem_e2e -- --ignored

use std::io::Cursor;
use rusty_backup::fs::filesystem::Filesystem;
```

#### Test Group A: ext2 browsing

```rust
#[test]
#[ignore]
fn test_ext2_browse_root() {
    let img = std::fs::read("tests/fixtures/test_ext2.img")
        .expect("run scripts/generate-test-images.sh first");
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::ext::ExtFilesystem::open(cursor, 0).unwrap();

    assert_eq!(fs.fs_type(), "ext2");
    assert_eq!(fs.volume_label(), Some("test_ext2"));

    let root = fs.root().unwrap();
    assert!(root.is_directory());

    let entries = fs.list_directory(&root).unwrap();
    let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
    assert!(names.contains(&"hello.txt"), "missing hello.txt in {names:?}");
    assert!(names.contains(&"subdir"), "missing subdir in {names:?}");
    assert!(names.contains(&"link.txt"), "missing link.txt in {names:?}");
}

#[test]
#[ignore]
fn test_ext2_read_file() {
    let img = std::fs::read("tests/fixtures/test_ext2.img").unwrap();
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::ext::ExtFilesystem::open(cursor, 0).unwrap();

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let hello = entries.iter().find(|e| e.name == "hello.txt").unwrap();

    let data = fs.read_file(hello, 1024).unwrap();
    assert_eq!(&data, b"Hello, ext2!");
}

#[test]
#[ignore]
fn test_ext2_symlink() {
    let img = std::fs::read("tests/fixtures/test_ext2.img").unwrap();
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::ext::ExtFilesystem::open(cursor, 0).unwrap();

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let link = entries.iter().find(|e| e.name == "link.txt").unwrap();

    assert!(link.is_symlink());
    assert_eq!(link.symlink_target.as_deref(), Some("hello.txt"));
}

#[test]
#[ignore]
fn test_ext2_permissions() {
    let img = std::fs::read("tests/fixtures/test_ext2.img").unwrap();
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::ext::ExtFilesystem::open(cursor, 0).unwrap();

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let hello = entries.iter().find(|e| e.name == "hello.txt").unwrap();

    // uid/gid will be whatever the user who ran mkfs was (no sudo chown available)
    assert!(hello.uid.is_some(), "uid should be set");
    assert!(hello.gid.is_some(), "gid should be set");
    // Mode should be 0o100644 (regular file, rw-r--r--)
    assert_eq!(hello.mode.map(|m| m & 0o777), Some(0o644));
}

#[test]
#[ignore]
fn test_ext2_nested_directory() {
    let img = std::fs::read("tests/fixtures/test_ext2.img").unwrap();
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
```

#### Test Group B: ext4 browsing

Same structure as Group A but using `test_ext4.img`:
- `fs_type() == "ext4"` (has journal, so detected as ext4)
- `volume_label() == Some("test_ext4")`
- File content: `b"Hello, ext4!"`

Tests: `test_ext4_browse_root`, `test_ext4_read_file`, `test_ext4_symlink`,
`test_ext4_permissions`, `test_ext4_nested_directory`.

#### Test Group C: btrfs browsing

```rust
#[test]
#[ignore]
fn test_btrfs_browse_root() {
    let img = std::fs::read("tests/fixtures/test_btrfs.img").unwrap();
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::btrfs::BtrfsFilesystem::open(cursor, 0).unwrap();

    assert_eq!(fs.fs_type(), "btrfs");
    assert_eq!(fs.volume_label(), Some("test_btrfs"));

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
    assert!(names.contains(&"hello.txt"));
    assert!(names.contains(&"subdir"));
    assert!(names.contains(&"link.txt"));
}

#[test]
#[ignore]
fn test_btrfs_read_file() {
    let img = std::fs::read("tests/fixtures/test_btrfs.img").unwrap();
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::btrfs::BtrfsFilesystem::open(cursor, 0).unwrap();

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let hello = entries.iter().find(|e| e.name == "hello.txt").unwrap();

    let data = fs.read_file(hello, 1024).unwrap();
    assert_eq!(&data, b"Hello, btrfs!");
}

#[test]
#[ignore]
fn test_btrfs_symlink() {
    let img = std::fs::read("tests/fixtures/test_btrfs.img").unwrap();
    let cursor = Cursor::new(img);
    let mut fs = rusty_backup::fs::btrfs::BtrfsFilesystem::open(cursor, 0).unwrap();

    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let link = entries.iter().find(|e| e.name == "link.txt").unwrap();

    assert!(link.is_symlink());
    assert_eq!(link.symlink_target.as_deref(), Some("hello.txt"));
}

#[test]
#[ignore]
fn test_btrfs_nested_directory() {
    let img = std::fs::read("tests/fixtures/test_btrfs.img").unwrap();
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
```

#### Test Group D: ext compaction round-trip

```rust
#[test]
#[ignore]
fn test_ext4_compaction_round_trip() {
    let img = std::fs::read("tests/fixtures/test_ext4.img").unwrap();
    let original_size = img.len();
    let cursor = Cursor::new(img);

    // Compact
    let (mut compact, info) = rusty_backup::fs::CompactExtReader::new(cursor, 0).unwrap();
    assert!(info.compacted_size <= original_size as u64);

    // Read compacted output
    let mut output = Vec::new();
    std::io::Read::read_to_end(&mut compact, &mut output).unwrap();

    // Verify the compacted image is still browsable
    let cursor = Cursor::new(output);
    let mut fs = rusty_backup::fs::ext::ExtFilesystem::open(cursor, 0).unwrap();
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let hello = entries.iter().find(|e| e.name == "hello.txt").unwrap();
    let data = fs.read_file(hello, 1024).unwrap();
    assert_eq!(&data, b"Hello, ext4!");
}
```

#### Test Group E: btrfs compaction round-trip

```rust
#[test]
#[ignore]
fn test_btrfs_compaction_round_trip() {
    let img = std::fs::read("tests/fixtures/test_btrfs.img").unwrap();
    let original_size = img.len();
    let cursor = Cursor::new(img);

    // Compact
    let (mut compact, info) = rusty_backup::fs::CompactBtrfsReader::new(cursor, 0).unwrap();
    assert!(info.compacted_size <= original_size as u64);

    // Read compacted output
    let mut output = Vec::new();
    std::io::Read::read_to_end(&mut compact, &mut output).unwrap();

    // Verify the compacted image is still browsable
    let cursor = Cursor::new(output);
    let mut fs = rusty_backup::fs::btrfs::BtrfsFilesystem::open(cursor, 0).unwrap();
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let hello = entries.iter().find(|e| e.name == "hello.txt").unwrap();
    let data = fs.read_file(hello, 1024).unwrap();
    assert_eq!(&data, b"Hello, btrfs!");
}
```

#### Test Group F: ext resize

```rust
#[test]
#[ignore]
fn test_ext4_resize_grows() {
    let mut img = std::fs::read("tests/fixtures/test_ext4.img").unwrap();
    let original_size = img.len();
    let new_size = original_size * 2;
    img.resize(new_size, 0); // extend with zeros

    let mut cursor = Cursor::new(img);
    rusty_backup::fs::resize_ext_in_place(&mut cursor, 0, new_size as u64, &mut |msg| {
        eprintln!("  resize: {msg}");
    }).unwrap();

    // Validate
    cursor.set_position(0);
    let warnings = rusty_backup::fs::validate_ext_integrity(&mut cursor, 0, &mut |msg| {
        eprintln!("  validate: {msg}");
    }).unwrap();
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
```

#### Test Group G: btrfs resize

```rust
#[test]
#[ignore]
fn test_btrfs_resize_grows() {
    let mut img = std::fs::read("tests/fixtures/test_btrfs.img").unwrap();
    let original_size = img.len();
    let new_size = original_size + 64 * 1024 * 1024; // grow by 64 MiB
    img.resize(new_size, 0);

    let mut cursor = Cursor::new(img);
    rusty_backup::fs::resize_btrfs_in_place(&mut cursor, 0, new_size as u64, &mut |msg| {
        eprintln!("  resize: {msg}");
    }).unwrap();

    // Validate
    cursor.set_position(0);
    let warnings = rusty_backup::fs::validate_btrfs_integrity(&mut cursor, 0, &mut |msg| {
        eprintln!("  validate: {msg}");
    }).unwrap();
    assert!(warnings.is_empty(), "warnings: {warnings:?}");

    // Should still be browsable with new total_bytes
    let img = cursor.into_inner();
    let cursor = Cursor::new(img);
    let fs = rusty_backup::fs::btrfs::BtrfsFilesystem::open(cursor, 0).unwrap();
    assert_eq!(fs.total_size(), new_size as u64);
}
```

#### Test Group H: Filesystem detection routing

```rust
#[test]
#[ignore]
fn test_detect_ext4_via_0x83() {
    let img = std::fs::read("tests/fixtures/test_ext4.img").unwrap();
    let cursor = Cursor::new(img);
    // open_filesystem with type 0x83 should detect ext4
    let fs = rusty_backup::fs::open_filesystem(cursor, 0, 0x83, None).unwrap();
    assert_eq!(fs.fs_type(), "ext4");
}

#[test]
#[ignore]
fn test_detect_btrfs_via_0x83() {
    let img = std::fs::read("tests/fixtures/test_btrfs.img").unwrap();
    let cursor = Cursor::new(img);
    // open_filesystem with type 0x83 should detect btrfs
    let fs = rusty_backup::fs::open_filesystem(cursor, 0, 0x83, None).unwrap();
    assert_eq!(fs.fs_type(), "btrfs");
}

#[test]
#[ignore]
fn test_detect_ext4_auto() {
    let img = std::fs::read("tests/fixtures/test_ext4.img").unwrap();
    let cursor = Cursor::new(img);
    // open_filesystem with type 0x00 (auto-detect) should find ext4
    let fs = rusty_backup::fs::open_filesystem(cursor, 0, 0x00, None).unwrap();
    assert_eq!(fs.fs_type(), "ext4");
}

#[test]
#[ignore]
fn test_detect_btrfs_auto() {
    let img = std::fs::read("tests/fixtures/test_btrfs.img").unwrap();
    let cursor = Cursor::new(img);
    let fs = rusty_backup::fs::open_filesystem(cursor, 0, 0x00, None).unwrap();
    assert_eq!(fs.fs_type(), "btrfs");
}

#[test]
#[ignore]
fn test_detect_ext2_via_0x83() {
    let img = std::fs::read("tests/fixtures/test_ext2.img").unwrap();
    let cursor = Cursor::new(img);
    let fs = rusty_backup::fs::open_filesystem(cursor, 0, 0x83, None).unwrap();
    assert_eq!(fs.fs_type(), "ext2");
}
```

### Step 3: Running the tests

```bash
# 1. Generate test images (NO sudo needed)
scripts/generate-test-images.sh

# 2. Run the end-to-end tests
cargo test --test filesystem_e2e -- --ignored

# 3. Run everything (unit + integration)
cargo test --lib && cargo test --test filesystem_e2e -- --ignored
```

### Verification checklist

- [ ] `scripts/generate-test-images.sh` runs successfully without sudo, creates 3 images
- [ ] All ext2 browsing tests pass (root listing, file read, symlink, permissions, nested dir)
- [ ] All ext4 browsing tests pass (same checks)
- [ ] All btrfs browsing tests pass (same checks)
- [ ] ext4 compaction round-trip: compacted image still browsable with correct file data
- [ ] btrfs compaction round-trip: compacted image still browsable with correct file data
- [ ] ext4 resize: grown image passes validation and is browsable
- [ ] btrfs resize: grown image passes validation, total_size reflects new size
- [ ] Filesystem detection: types 0x83 and 0x00 correctly identify ext2, ext4, and btrfs
- [ ] `cargo test --lib` still passes all existing unit tests
- [ ] `cargo clippy` has no new warnings

---

## Design Decisions Reference

Keep this section for quick reference when starting any task.

### Architecture pattern (how each filesystem fits in)

```
src/fs/<fsname>.rs
  ├── <FsName>Filesystem struct (implements Filesystem trait)
  │   ├── open() — parse superblock/volume header
  │   ├── root() — return root directory entry
  │   ├── list_directory() — parse directory, build FileEntry list
  │   ├── read_file() — follow block pointers, return bytes
  │   ├── volume_label(), fs_type(), total_size(), used_size()
  │   └── last_data_byte() — find highest allocated block
  ├── Compact<FsName>Reader (implements Read + Send)
  │   └── Builds CompactLayout → CompactStreamReader
  ├── resize_<fsname>_in_place() — grow filesystem metadata
  └── validate_<fsname>_integrity() — check structural consistency

src/fs/mod.rs
  ├── detect_filesystem_type() — magic byte probing
  ├── open_filesystem() — route type byte → constructor
  └── compact_partition_reader() — route type byte → CompactReader

src/restore/mod.rs
  ├── PartitionFsType enum — add variant
  ├── detect_partition_fs_type() — add magic check
  ├── resize match arms — add resize call
  └── validate match arms — add validate call
```

### Partclone reference (../partclone/src/)

| Our task | Partclone file | What to reference |
|----------|---------------|-------------------|
| ext bitmap scanning | `extfsclone.c:100-257` | Block group iteration, BLOCK_UNINIT, bigalloc |
| btrfs tree walking | `btrfsclone.c:249-480` | Recursive descent, extent items, chunk mapping |
| btrfs on-disk structs | `btrfs/ctree.h` | Superblock, keys, items, extent data |
| xfs (future) | `xfsclone.c:102-242` | AG scanning, BNO B-tree, inverted bitmap |
| Bitmap ops | `bitmap.h` | set/clear/test bit macros |

### Key files in rusty-backup

| File | Role |
|------|------|
| `src/fs/filesystem.rs` | `Filesystem` trait definition |
| `src/fs/entry.rs` | `FileEntry` struct, `EntryType` enum |
| `src/fs/mod.rs` | Filesystem detection + routing |
| `src/fs/fat.rs` | Reference implementation (most complete) |
| `src/fs/unix_common/` | Shared Unix utilities (created in this plan) |
| `src/gui/browse_view.rs` | File browser UI |
| `src/restore/mod.rs` | Restore orchestration + resize/validate dispatch |
| `src/rbformats/mod.rs` | Disk reconstruction |
| `src/rbformats/vhd.rs` | VHD/VMDK export |
| `src/clonezilla/block_cache.rs` | Clonezilla image browsing cache |
| `src/backup/mod.rs` | Backup orchestration |

### No hidden sectors for Unix filesystems

Unlike FAT/NTFS/exFAT which store their partition LBA offset in the boot sector, ext and btrfs do NOT store partition offset in their superblocks. No `patch_hidden_sectors` function is needed. The restore code should simply skip the patching step for these filesystem types.

### Future filesystem checklist

When adding xfs, ZFS, UFS, ReiserFS, JFS, Minix, or any other Unix filesystem:
1. Create `src/fs/<fsname>.rs` implementing `Filesystem` trait
2. Use `unix_common::unix_entry_from_inode()` for building entries
3. Use `unix_common::BitmapReader` if the FS has block bitmaps
4. Use `unix_common::CompactStreamReader` with `CompactLayout` for compaction
5. Use `unix_common::validate_magic()` etc. for validation
6. Add magic detection to `detect_filesystem_type()` in `src/fs/mod.rs`
7. Add routing to `open_filesystem()` and `compact_partition_reader()`
8. Add `PartitionFsType` variant and wire resize/validate in `src/restore/mod.rs`
