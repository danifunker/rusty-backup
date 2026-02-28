# Ext2/3/4 Packed Compact Reader with Block Relocation

## Overview

Make ext work like FAT/NTFS: pack blocks during backup with resize2fs-style block relocation, producing a smaller valid filesystem image. Restore writes the packed image and updates size metadata.

Currently ext uses a layout-preserving compact reader (`compacted_size == original_size`, free blocks zero-filled). This prevents restoring to smaller partitions.

---

## Session 1: Compute Minimum Size and Build Relocation Map ✅

**Goal:** Given an ext partition, calculate the minimum number of block groups needed and build a mapping of which blocks need to move where.

### Tasks
- [x] Add helper: `metadata_blocks_in_group()` — returns count of metadata blocks per group (SB backup, GDT, bitmaps, inode table)
- [x] Add helper: `has_superblock_backup(group, sparse_super)` — true for groups 0, 1, and powers of 3, 5, 7
- [x] Add `RelocationPlan` struct with `min_groups`, `new_total_blocks`, `relocations: HashMap<u64,u64>`, `needs_relocation`
- [x] Add `build_relocation_map(reader, partition_offset)` — scans bitmaps, identifies metadata vs data blocks, computes minimum groups, pairs out-of-bounds data blocks with free slots
- [x] Unit tests: `test_has_superblock_backup`, `test_metadata_blocks_in_group`, `test_relocation_map_no_trailing_data`, `test_relocation_map_with_trailing_data`
- [x] Multi-group test image helper: `make_two_group_image()` (2 groups, 64 blocks each, data in both groups)

### Key code to reuse
- `CompactExtReader::new()` (ext.rs:1049-1210) — superblock + GDT parsing, bitmap scanning
- `BitmapReader::iter_set_bits()` / `iter_clear_bits()` (bitmap.rs)

### Files to modify
- `src/fs/ext.rs`

---

## Session 2: Inode Scanning and Block Pointer Patching ✅

**Goal:** Read every allocated inode, identify block references that point to relocated blocks, produce patched inode table data.

### Tasks
- [x] `scan_and_patch_inodes(reader, partition_offset, plan) -> PatchedInodeTables` — reads all inode bitmaps/tables, patches block pointers
- [x] `PatchedInodeTables` struct: `tables: Vec<Vec<u8>>` (per-group patched inode table bytes) + `indirect_block_patches: HashMap<u64, Vec<u8>>` (patched indirect/extent-index blocks)
- [x] Extent-based patching: `patch_extent_tree_in_inode()`, `patch_extent_block()`, `patch_extent_leaf()` — recursive extent tree walking with leaf start_lo/hi patching and index pointer patching
- [x] Indirect-block patching: `patch_indirect_in_inode()`, `patch_indirect_pointer()`, `patch_indirect_block_recursive()` — handles direct[0-11], single/double/triple indirect with recursive child patching
- [x] Edge cases: fast symlinks skipped, socket/fifo/device inodes skipped (no block pointers)
- [x] Tests: `test_scan_and_patch_direct_block_pointer`, `test_scan_and_patch_extent_based_inode`, `test_scan_and_patch_no_relocation_needed`

### Key code to reuse
- `read_inode()` (ext.rs:311-383) — read any inode by number
- `inode_data_blocks()` (ext.rs:385) — get physical blocks for inode
- `read_extent_tree()` (ext.rs:443) — walk extent B-tree
- `read_indirect_blocks()` (ext.rs:481) — walk indirect chains

### Files to modify
- `src/fs/ext.rs`

---

## Session 3: Rebuild Bitmaps and Group Descriptors ✅

**Goal:** Produce updated block bitmaps and group descriptors reflecting the new block layout after relocation.

### Tasks
- [x] `ShrinkMetadata` struct: `superblock: Vec<u8>`, `gdt: Vec<u8>`, `block_bitmaps: Vec<Vec<u8>>`
- [x] `rebuild_metadata_for_shrink(reader, partition_offset, plan) -> ShrinkMetadata`
  - Reads original bitmaps, applies relocation (clear OUT bits, set IN bits)
  - Recalculates `bg_free_blocks_count` per group from rebuilt bitmaps
  - Truncates GDT to `min_groups`
  - Patches superblock: `s_blocks_count`, `s_free_blocks_count`, `s_r_blocks_count` (proportional)
  - Handles 64-bit high words for all fields
- [x] Tests: `test_rebuild_metadata_superblock`, `test_rebuild_metadata_gdt_truncated`, `test_rebuild_metadata_bitmap`, `test_rebuild_metadata_no_shrink`

### Files to modify
- `src/fs/ext.rs`

---

## Session 4: Packed Stream Assembly (New CompactExtReader)

**Goal:** Assemble the packed output stream as a valid, smaller ext filesystem image using the CompactSection framework.

### Tasks
- [ ] Modify `CompactExtReader::new()` to call `build_relocation_map()` when data extends beyond minimum boundary
- [ ] Build stream sections in filesystem order:
  1. `PreBuilt` — boot block + patched superblock
  2. `PreBuilt` — patched GDT (fewer groups)
  3. Per group (0..min_groups):
     - `PreBuilt` — rebuilt block bitmap
     - `PreBuilt` — original inode bitmap (copied)
     - `PreBuilt` — patched inode table (from Session 2)
     - `MappedBlocks` — data blocks, with relocated blocks reading from their ORIGINAL source positions
     - `Zeros` — free blocks within group
- [ ] Set `compacted_size = new_total_blocks * block_size` (smaller than original)
- [ ] Set `data_size = allocated_blocks * block_size` (actual disk reads)
- [ ] When no relocation needed (data already fits), fall back to current layout-preserving behavior
- [ ] Integration test: compact an ext image, decompress, verify with `e2fsck`

### Key code to reuse
- `CompactSection` / `CompactStreamReader` (unix_common/compact.rs)
- Existing `CompactExtReader` pattern

### Files to modify
- `src/fs/ext.rs`
- `src/fs/mod.rs` (if CompactResult semantics change)

---

## Session 5: resize_ext_in_place Shrink Support + End-to-End Testing

**Goal:** Update the resize function to handle shrinking and verify the full backup→restore round-trip.

### Tasks
- [ ] Update `resize_ext_in_place()` to support shrinking:
  - Remove the `new_blocks <= old_blocks → skip` guard
  - Reduce `s_blocks_count` and recalculate `s_free_blocks_count`
  - Update `s_r_blocks_count` (reserved blocks) proportionally
  - Handle group count reduction in GDT
- [ ] End-to-end test: backup ext partition → restore to smaller partition → verify all files
- [ ] Test with ext2 (indirect blocks only), ext3, ext4 (extent trees)
- [ ] Test with 64-bit ext4
- [ ] Test edge case: filesystem with 1 block group (no shrink possible)
- [ ] Test: restore packed backup to LARGER partition (should grow correctly)
- [ ] Test: existing layout-preserving backups still restore correctly (backward compat)

### Files to modify
- `src/fs/ext.rs`
- `src/restore/mod.rs` (if any restore-path changes needed)

---

## Reference: Key Functions in Codebase

| Function | File | Lines | Purpose |
|----------|------|-------|---------|
| `CompactExtReader::new()` | ext.rs | 1049-1210 | Current layout-preserving compaction |
| `resize_ext_in_place()` | ext.rs | 859-934 | Current grow-only resize |
| `read_inode()` | ext.rs | 311-383 | Read any inode by number |
| `inode_data_blocks()` | ext.rs | 385-393 | Map inode → physical blocks |
| `read_extent_tree()` | ext.rs | 443-477 | Walk extent B-tree |
| `read_indirect_blocks()` | ext.rs | 481-541 | Walk indirect chains |
| `last_data_byte()` | ext.rs | 712-740 | Find highest allocated block |
| `BitmapReader` | unix_common/bitmap.rs | all | Bitmap iteration utilities |
| `CompactStreamReader` | unix_common/compact.rs | all | Section-based stream framework |
| `CompactFatReader` | fat.rs | 738-1300 | Reference: FAT packed reader |
