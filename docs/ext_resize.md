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

## Session 2: Inode Scanning and Block Pointer Patching

**Goal:** Read every allocated inode, identify block references that point to relocated blocks, produce patched inode table data.

### Tasks
- [ ] Add function: `scan_and_patch_inodes(reader, partition_offset, relocation_map) -> Vec<PatchedInodeTable>`
- [ ] Iterate inode bitmaps across all groups to find allocated inodes
- [ ] For each allocated inode, read it and determine block mapping type:
  - **Extent-based (ext4):** Walk extent tree, patch `ee_start_lo/hi` in leaf nodes. Also handle extent index nodes (`ei_leaf_lo/hi`) that may themselves be relocated.
  - **Indirect-block (ext2/ext3):** Patch direct block pointers (inode bytes 40-87), then walk single/double/triple indirect chains patching block numbers. Handle indirect blocks themselves being relocated.
- [ ] Produce patched inode table bytes (Vec<u8>) per block group, with all block pointers updated
- [ ] Handle edge cases: symlinks (inline if < 60 bytes), xattr blocks, special files
- [ ] Unit test: create inodes with known extent/indirect layouts, verify patching

### Key code to reuse
- `read_inode()` (ext.rs:311-383) — read any inode by number
- `inode_data_blocks()` (ext.rs:385) — get physical blocks for inode
- `read_extent_tree()` (ext.rs:443) — walk extent B-tree
- `read_indirect_blocks()` (ext.rs:481) — walk indirect chains

### Files to modify
- `src/fs/ext.rs`

---

## Session 3: Rebuild Bitmaps and Group Descriptors

**Goal:** Produce updated block bitmaps and group descriptors reflecting the new block layout after relocation.

### Tasks
- [ ] Add function: `rebuild_bitmaps(original_bitmaps, relocation_map, min_groups) -> Vec<Vec<u8>>`
  - For each group within new boundary:
    - Start with original bitmap
    - Clear bits for blocks relocated OUT
    - Set bits for blocks relocated IN
- [ ] Update group descriptors:
  - Recalculate `bg_free_blocks_count` per group from rebuilt bitmaps
  - Zero/remove descriptors for truncated trailing groups
- [ ] Update superblock fields for new image:
  - `s_blocks_count` (total blocks)
  - `s_free_blocks_count` (total free)
  - `s_block_group_nr` if needed
- [ ] Unit test: verify bitmap consistency — set bits == allocated blocks count

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
