# DONE: ext4 metadata_csum resize/shrink — and the resize2fs-grade repack

Branch: `add-missing-filesystems`. Oracle: local `e2fsck` / `mke2fs` /
`debugfs` (`/opt/homebrew/opt/e2fsprogs/sbin/`, `brew install e2fsprogs`).
e2fsprogs is **test-only** — rusty-backup produces valid images on its own; it
never shells out to e2fsck/resize2fs at runtime. See the [[ext4-metadata-csum]]
memory for the checksum design.

## Outcome

The "one remaining ext4 gap" (the shrink path was checksum-blind) turned out to
be much bigger than a checksum stamp. The shrink path in question is the
**backup compactor** `CompactExtReader::new_packed` (not the restore-side
`resize_ext_in_place`, which was already fine after the grow-superblock-restamp).
Oracle-checking the packed output — something the synthetic unit tests never did —
showed it corrupted *every* multi-group image. Fixing it correctly for real
mke2fs ext4 meant a `resize2fs`-grade repack. All shipped and `e2fsck`-verified.

## What shipped (3 commits on top of the earlier ext4 work)

1. `6de048a` — checksum-clean + structurally-correct **no-relocation** shrink:
   `s_inodes_count`/`s_free_inodes_count` fix; metadata_csum SB+GDT re-stamp;
   inode csum + separate extent-block (`et_checksum`) re-stamp in
   `scan_and_patch_inodes`; inode-drop safety guard.
2. `cea3e84` — flex_bg metadata-freeing on shrink + block-bitmap csum covers
   `blocks_per_group/8` bytes (not the whole block).
3. `fb19dd9` — the full block-relocation repack:
   - **flex_bg**: `metadata_blocks_set` from the actual GDT (not a fixed offset);
     linear per-block emit classifier; free dropped groups' clustered metadata.
   - **resize_inode**: dropped (`tune2fs -O ^resize_inode` style) — zero inode 7,
     clear the feature + `s_reserved_gdt_blocks`, free its blocks.
   - **contiguous extents**: relocate whole runs into equal/longer contiguous free
     regions; retry a larger group count rather than split a run.
   - **sizing/bitmap**: shrink the GDT (free excess GDT blocks), clear
     `BG_BLOCK_UNINIT`, set bitmap padding.

## Verification

`src/fs/ext_format.rs` tests (e2fsck/mke2fs/debugfs-gated):
`packed_shrink_real_ext4_is_e2fsck_clean` (feature combos × sizes, small-`-g` +
default geometry, 1 KiB + 4 KiB blocks), `packed_shrink_preserves_file_data`
(debugfs-written file round-trips through the shrunk image via our reader),
`packed_flex_bg_shrink_no_reloc_e2fsck_clean`, `packed_no_reloc_shrink_is_e2fsck_clean`.
`cargo test --lib` green (2211). e2fsck-clean incl. a 64 MiB→12 MiB pack with
journal relocation + resize-inode drop + metadata_csum.

## Known follow-ups (not blocking)

- `build_relocation_map`'s min-group **retry loop** is O(group_count) worst case
  (each attempt scans blocks) — fine for typical backups, could be smarter for
  very large volumes.
- A boundary-straddling extent or a run with no contiguous free region makes the
  packer fall back to a larger group count (less shrink), never a split — safe,
  occasionally leaves shrink on the table.
