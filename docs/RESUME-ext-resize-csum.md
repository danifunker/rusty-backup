# Resume: ext4 metadata_csum — finish the resize/shrink checksum path

Self-contained pickup for the one remaining ext4 gap. Branch:
`add-missing-filesystems`. Oracle: local `e2fsck` / `mke2fs`
(`/opt/homebrew/opt/e2fsprogs/sbin/`, `brew install e2fsprogs`). See the
[[ext4-metadata-csum]] memory for the full checksum design.

## Is ext2/3/4 fully supported? — the honest answer

- **Create ("new") — COMPLETE.** `rb-cli new --fs ext|ext3|ext4` (and the
  `batch` verb) all work and are `e2fsck -fn` clean. ext4 = extents +
  `metadata_csum` (crc32c) + jbd2 journal, 256-byte inodes. There is **no GUI
  "new" path for any filesystem** (create-blank is CLI-only by design across
  FAT/HFS/NTFS/EFS/AFFS/ext/… — not a gap).
- **Browse / read, edit, fsck — COMPLETE for ext4.** Editor
  (create_file/mkdir/delete/rename) and fsck (check+repair) recompute crc32c and
  stay `e2fsck`-clean, incl. on real Linux/mke2fs images (uninit_bg handled).
- **Resize / shrink / grow — the remaining gap (this doc).** The resize path
  writes metadata WITHOUT recomputing crc32c, so it would leave a
  `metadata_csum` ext4 with stale checksums. **Note:** ext resize is *explicitly*
  a "minimal, metadata-only" op that already recommends running `resize2fs`/`e2fsck`
  after (see the `resize_ext_in_place` doc comment) — so this is a
  don't-make-it-worse fix, not a full resize2fs.

## Committed already (5 commits)

`bbfd6e3` ext2+ext3 create · `aa08394` ext4 fsck check+repair · `fab9133` ext4
create · `e4119b0` ext4 checksum-aware editor · `c4cfd7f` docs. All
`e2fsck`-verified; 2206 lib tests green.

## Done: the grow/restore path

`resize_ext_in_place` (~line 3654) now re-stamps the superblock crc32c after
patching counts (guarded by `le32(&sb,0x64) & 0x0400`). Tested
(`ext4_grow_keeps_superblock_checksum_valid`) + committed. It patches only the SB
counts, so the SB stamp is sufficient for what it writes.

## The remaining task: the shrink path

**`rebuild_metadata_for_shrink`** (~line 3443) is still checksum-blind — the fuller shrink: rebuilds
   per-group **block bitmaps** + patches **GDT** free counts + patches the **SB**,
   returning `ShrinkMetadata { superblock, gdt, block_bitmaps }` (construction ends
   ~line 3623). For metadata_csum, before the `Ok(ShrinkMetadata{…})`, stamp:
   - each surviving group's **block-bitmap csum** into its descriptor
     (`ext_csum::stamp_block_bitmap_csum(seed, desc, &block_bitmaps[g])` — the
     bitmaps are full `block_size` bytes, see ~line 3540/3544), then
     `stamp_group_desc` (bg_checksum covers the bitmap-csum field; stamp it last).
     Inode-bitmap csums are unchanged (shrink doesn't touch inode bitmaps) so leave
     them.
   - the **superblock** (`stamp_superblock`).
   - `seed = ext_csum::csum_seed(&sb[0x68..0x78])`; `desc_size` is already computed
     in-scope (32 or 64).

   **Caveat / decide:** shrink also *relocates blocks* and patches inode block
   pointers via `scan_and_patch_inodes` (separate fn / `PatchedInodeTables`,
   applied by the caller in `src/partition/resize.rs`). Those relocated inodes'
   crc32c would ALSO go stale on ext4. Options:
   - (a) also make `scan_and_patch_inodes` stamp inode csums (needs seed + inum +
     generation), for a fully csum-clean shrink; or
   - (b) accept the "run resize2fs after" contract for shrink-with-relocation and
     only harden what `rebuild_metadata_for_shrink` directly writes (SB + GDT +
     block bitmaps); or
   - (c) if ext4 extent-mapped inodes aren't even handled by the relocation/inode
     patcher, **guard**: `rebuild_metadata_for_shrink` returns
     `FilesystemError::Unsupported` for metadata_csum (or has_extents) volumes so a
     shrink refuses rather than corrupts. **Check first** whether
     `build_relocation_map` / `scan_and_patch_inodes` handle extents at all — if
     not, (c) is the honest answer for shrink.

## Verify (e2fsck oracle — the pattern used throughout)

Add tests near `src/fs/ext_format.rs` tests (they have `create_blank_ext4`,
`e2fsprogs_bin`, `e2fsck_clean`):

- **Grow:** `create_blank_ext4(16 MiB)` → extend the Vec → `resize_ext_in_place(cur,
  0, new_size, log)` → assert the SB checksum is valid. (Full `e2fsck -fn` may
  still flag GDT free-count drift — that's the documented minimal-resize
  limitation, present on ext2 too; assert the *checksum* specifically, e.g. via
  `ext_csum::superblock_csum`, or accept known non-csum diffs.)
- **Shrink:** build a `RelocationPlan`, `rebuild_metadata_for_shrink`, apply the
  returned SB/GDT/bitmaps, `e2fsck -fn`. If going with guard (c), assert it errors
  on a metadata_csum volume instead.

Then: `cargo test --lib fs::ext`, `cargo clippy --lib -D warnings`, `cargo fmt`.
Commit as `feat(ext4): checksum-aware resize` (or `fix(ext4): guard resize on
metadata_csum` if you choose the guard). **Commit between phases** (user pref).

## Key facts (so you don't re-derive them)

- crc32c convention (verified vs mke2fs): `ext_csum::ext_crc(seed, d) =
  crc32c_append(seed ^ !0, d) ^ !0`; `seed = ext_crc(!0, uuid)`; **superblock** csum
  is seeded from `!0` not the uuid. All stampers in `src/fs/ext_csum.rs`
  (`stamp_superblock`, `stamp_group_desc`, `stamp_block_bitmap_csum`,
  `stamp_inode_bitmap_csum`, `stamp_inode`, `stamp_dir_block`), verified against the
  `test_ext4.img.zst` fixture.
- ext4 = `metadata_csum` when `ro_compat (sb@0x64) & 0x400`. `s_uuid` @ 0x68.
- `ExtFilesystem` already carries `metadata_csum` + `csum_seed` fields (open()
  parses them); `ExtGeom` too. But `resize_ext_in_place` /
  `rebuild_metadata_for_shrink` are **free functions** with only the raw SB bytes —
  derive the seed locally from `sb[0x68..0x78]`.
- Doc-sync when done: the completion-plan / coverage-audit already say ext is
  "complete"; if you land a shrink *guard*, footnote that shrink-with-relocation on
  metadata_csum ext4 is deferred to resize2fs.
