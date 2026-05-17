# Amiga Filesystem Resize — AFFS / PFS3 / SFS

In-place grow and shrink for the three Amiga filesystems we already read and
edit. This document goes deeper than the bullet list in
`docs/amiga_support.md` Phase 10: it analyzes the on-disk landmines per
filesystem, defines the shrink floor, and lays out the implementation phases.
Read + edit support already exists (shipped on `add-amiga-support`); resize
is the open work.

Editing already works for all three filesystems, so the scope here is
strictly the resize half. Unlike EFS, we do **not** need to build an
`EditableFilesystem` implementation as part of this — it's done.

---

## Why in-place (not clone-and-replace)

Same reasoning as the EFS plan, with one extra factor: all three Amiga
filesystems are already `is_layout_preserving_fs = true` in
`src/fs/mod.rs`, meaning the backup pipeline never relocates blocks between
backup and restore. The framework assumes the on-disk layout survives a
round-trip verbatim, and the existing restore path expects the resize step
to be a metadata-only patch (relocate a handful of structural blocks,
rewrite size-derived fields, fix checksums) rather than a full data shuffle.

Clone-and-replace would require building three new blank-volume creators
(one per filesystem) plus three replay engines, which buys us no real
robustness for layouts this simple. In-place wins.

The one place clone would be tempting is PFS3, whose rootblock-extension /
anode-bitmap-tree / reserved-area interaction is the gnarliest of the
three. We still recommend in-place because the PFS3 reference code
(`~/repos/amigasources/pfs3aio`) provides every formula we need; the
complexity is in the math, not the data motion.

---

## Cross-cutting facts (all three filesystems)

- **Big-endian everywhere**, multi-byte fields, full stop. Easy bug source.
- **Bitmap convention: set bit = FREE**, the opposite of FAT/NTFS/ext/etc.
  Every loop that reads a bitmap to compute "used blocks" must invert.
- **No partition-offset field on disk.** Unlike FAT's `BPB_HiddSec` or
  NTFS's `BPB_HiddenSectors`, none of the Amiga filesystems record their
  partition LBA in the volume header. RDB carries that. No
  hidden-sectors patching step is needed during restore.
- **Block size varies.** AFFS is effectively fixed at 512; PFS3 and SFS
  carry their own block-size in the rootblock and the RDB PART entry
  must match. Resize must NOT change block size — only block count.
- **RDB PART-entry fixup is shared.** When a partition's block count
  changes, the RDB PART's `de_LowCyl` / `de_HighCyl` (cylinder-based
  geometry derived from the partition's start LBA and block count) must
  be rewritten, and any subsequent partition entries may need to slide.
  This is the existing MBR-logicals problem with different field names.

---

## AFFS (OFS / FFS / DOS\\0..DOS\\7)

### On-disk landmines

The big one: **the root block sits at the midpoint of the partition.**
`src/fs/affs.rs:259` computes it as `(2 + total_blocks - 1) / 2` —
following ADFlib convention. Change `total_blocks` and the root block
**moves to a new physical location**. Every resize must:

1. Allocate the new root block location in the new bitmap.
2. Move the root block's contents (hashtable, bitmap-page pointers,
   metadata) from the old midpoint to the new midpoint.
3. Free the old midpoint location.
4. Rewrite the root checksum (sum of all longwords == 0 convention).

The old midpoint may currently hold a data block — that's a normal
allocation. It must be relocated into free space first; then the now-free
midpoint can be promoted to root.

Other AFFS structures that depend on volume size:

- **Bitmap pages**: each is 127 longs covering 127×32 = 4064 data blocks.
  The root block holds 25 inline `bm_pages[0..25]` pointers; beyond that a
  bitmap-extension chain links additional pages. Grow adds pages; shrink
  drops or partially clears them.
- **Hashtable**: in the root block, fixed at 72 entries; not size-derived.
- **Directory cache blocks** (DirCache variants DOS\\4..DOS\\7): cached
  copies of directory metadata. Not size-derived — survive resize.

### Shrink floor

The smallest target size where:

- No file data lives at `block >= new_total_blocks`.
- The new midpoint either is currently free, or holds a block that *can*
  be relocated within `[0..new_total_blocks)`.
- Enough free blocks exist below the new tail to hold every relocation.

In practice the floor is **`max(highest_allocated_block + 1, 2 *
allocated_blocks)`** — the 2× term accounts for the root sitting at the
midpoint, which forces at least half the volume to be "below the root."
Real-world Amiga volumes are rarely close to full, so this floor is
usually generous.

### Grow algorithm (`resize_affs_in_place` grow path)

1. Compute `new_total_blocks` and `new_root_block = (2 + new_total - 1)
   / 2`.
2. Read the current bitmap. Extend it: if extra blocks fit in the
   existing trailing bitmap-page pages (i.e. they were over-allocated),
   just clear bits; otherwise allocate new bitmap pages, link them via
   the bitmap-extension chain.
3. Mark the new tail blocks free in the extended bitmap.
4. If `new_root_block != old_root_block`:
   - If `new_root_block` is currently allocated to some other block,
     relocate that block into free space.
   - Copy the old root block's contents to `new_root_block`.
   - Mark the old root block free.
5. Rewrite root checksum.
6. Run `validate_affs_integrity` (Phase A.5 below).

### Shrink algorithm

1. Compute `new_total_blocks`, `new_root_block`.
2. Walk the bitmap, collecting every allocated block at index
   `>= new_total_blocks`. For each, allocate a free destination in
   `[0..new_total_blocks)`, copy the contents, update every reference.
   **References:** AFFS uses block numbers in hashtables, hash chains
   (`hashChain` field), file-header `firstData` and `dataBlocks[]`,
   extension-block `nextBlock`, parent links. We already enumerate these
   on the read path; the write path needs a `rewrite_block_pointer`
   helper that walks the same structures with mutation semantics.
3. Relocate the root block to `new_root_block` (same as grow step 4).
4. Trim bitmap pages whose covered range now lies entirely past the
   new tail; partially-trim the last one.
5. Update root's `numBlocks` (the field is implicit on AFFS — block
   count is derived from `bitmap_pages[]` extent, so no scalar field to
   rewrite; verify against ADFlib `adf_format.c`).
6. Rewrite root checksum.
7. Run `validate_affs_integrity`.

### Phase A breakdown

- **Phase A.0** — Shrink-floor function (~0.5 day). `pub fn
  affs_shrink_floor_bytes(reader, partition_offset, partition_size) ->
  io::Result<u64>` returns
  `max(highest_allocated_block + 1, 2 * allocated_blocks) * block_size`.
  Wire into `fs::partition_minimum_size` so the inspect-tab partition
  list shows the floor without any resize code in place. Ships
  independently — users can see potential savings on real images
  before the rest of Phase A lands.
- **Phase A.1** — Block relocator (~1 day). `relocate_block(old, new)`
  walks every reference type and rewrites the pointer. Stand-alone unit
  tested with synthetic fixtures (no resize wrapper yet).
- **Phase A.2** — Bitmap extender / trimmer (~1 day). Pure bitmap-page
  manipulation; no semantics. Unit tested by inspection.
- **Phase A.3** — Root relocator (~0.5 day). Composes the two above
  plus the checksum rewrite.
- **Phase A.4** — `resize_affs_in_place` wrapper (~0.5 day). Routes to
  grow vs. shrink based on size delta; orchestrates the steps.
- **Phase A.5** — `validate_affs_integrity` (~0.5 day). Re-open the
  resized volume via `open_filesystem`, walk root + bitmap, confirm no
  dangling references. Reuses the existing AFFS fsck path where
  practical.

Total: **~3-4 days**, matching the Phase 10a estimate in
`docs/amiga_support.md`.

Reference: `~/repos/amigasources/ADFlib` (`adf_vol.c`, `adf_bitm.c`,
`adf_format.c`).

---

## SFS (SFS\\0 / SFS\\2)

### On-disk landmines

- **Journal region (TRST/TRFA blocks)** typically lives near the end of
  the partition. The rootblock records its start block. On shrink the
  journal **must move** to sit below the new tail; on grow it can either
  stay in place or move out to the new tail (depends on whether
  SFS-savvy tools expect it adjacent to the volume end).
- **Single-leaf B-tree (BNDC) for extents**: leaf records reference
  block numbers. If we relocate a data extent, the corresponding BNDC
  leaf record must be rewritten. The btree itself sits at fixed-ish
  positions tied to the rootblock's `extent_bnode_root` field; the
  rootblock holds the pointer, so the btree node itself can be moved
  by rewriting that pointer.
- **Object-node admin tree** (the SFS analog of an inode table): a
  separate btree, also reachable from the rootblock. Same relocation
  story — pointer in rootblock, leaves on disk, move leaves and rewrite
  pointer.
- **Bitmap**: a chain of bitmap blocks, each with a self-identifying
  header and a `next_block` pointer. Length is size-derived.
- **Block size**: in the rootblock; not changed by resize.

We don't maintain the journal at runtime (`src/fs/sfs.rs` uses
sync-boundary atomicity instead — see the CLAUDE.md "Edit Mode
Stabilization" notes). That means the journal blocks on disk are
"placeholders" carrying TRST/TRFA magic but no actual log entries we
need to replay. Moving them is a mechanical block-copy + rootblock
field rewrite, with no semantics to preserve inside the journal.

### Shrink floor

- No file extent past `new_total_blocks`.
- Journal can fit somewhere in `[0..new_total_blocks)` — at minimum its
  current size (typically a few hundred blocks).
- Bitmap chain can cover `new_total_blocks` bits.

Floor ≈ `highest_allocated_block + journal_size + bitmap_growth_overhead`.

### Grow algorithm

1. Compute `new_total_blocks`.
2. Extend the bitmap chain: if the tail bitmap block has free bits
   covering the new range, just clear them; otherwise allocate a new
   bitmap block, mark it in itself, link via `next_block`.
3. Decision: move journal to new tail, or leave it where it is. SFS
   tools (smartfilesystem userspace) typically place the journal at
   the volume end; recommend matching by relocating. Allocate a new
   journal region at `new_total - journal_size`, copy TRST/TRFA blocks
   over (treat them as opaque bytes), update rootblock pointer, free
   old journal blocks.
4. Update rootblock `total_blocks` and recompute the rootblock
   checksum.
5. Run `validate_sfs_integrity`.

### Shrink algorithm

1. Walk bitmap → list of allocated blocks `>= new_total_blocks`.
2. For each allocated tail block: find its owning structure (extent in
   BNDC leaf, object-node admin record, or journal block). Allocate a
   destination in `[0..new_total_blocks)`. Copy. Rewrite the reference
   in the owning structure. Free the source.
3. Move the journal region into `[0..new_total_blocks)` (it almost
   certainly is in the way).
4. Truncate the bitmap chain — drop trailing bitmap blocks whose covered
   range falls entirely past the new tail.
5. Update rootblock `total_blocks` + checksum.
6. Run `validate_sfs_integrity`.

### B-tree complication

If a BNDC leaf record's extent crosses the new tail (start below, end
above), it must be **split**: keep the portion below, relocate the
portion above into a different free run, insert a new leaf record. The
existing extent-btree code only handles single-leaf BNDC and does not
implement splits (per `src/fs/sfs.rs:1365`). Resize would either:

- **(Recommended)** refuse to shrink if any extent crosses the new tail
  *and* the leaf is already at capacity. Surface this as
  `DiskFull`-style error with a clear message. Most retro Amiga volumes
  have small extent counts; this is rarely hit.
- Bite the bullet and implement BNDC leaf splits. The on-disk format
  supports it (the btree has a node-size field and growth pointers);
  our writer just doesn't. ~2 extra days.

Recommendation: ship without splits, document the limitation, add
splits later if a real workload hits the wall.

### Phase S breakdown

Restructured after Phase 0 learnings: shrink-to-floor is dramatically
simpler than the original "any size" shrink because, by definition, no
user-data block sits above the floor — so no BNDC/OBJC reference
rewrite is required. The full block-relocator is deferred as a design
choice (Phase S.3) and not implemented.

- **Phase S.0** — Shrink-floor function (~0.5 day). `pub fn
  sfs_shrink_floor_bytes(reader, partition_offset, partition_size) ->
  io::Result<u64>` returns `(highest_allocated_block +
  journal_size + bitmap_growth_overhead) * block_size`. Reads the
  rootblock for `journal_size`, walks bitmap chain once for the
  high-water. Wires into `fs::partition_minimum_size`. Ships
  independently of the rest of Phase S.
- **Phase S.1** — Shrink-to-floor (✓ DONE). Constrained to
  `new_size >= last_data_byte`. Implemented as `resize_sfs_in_place`
  in `src/fs/sfs.rs` and wired into `fs::resize_filesystem_for`.
  Verifies the floor invariant by walking the bitmap raw,
  read-modify-writes the bitmap block covering the new tail to mark
  the new backup-root position allocated, stamps fresh primary +
  backup rootblocks with bumped `sequencenumber`, writes backup first
  and primary last (commit point). Tests:
  `sfs_resize_shrink_round_trip`, `sfs_resize_refuses_shrink_below_floor`,
  `sfs_resize_skips_non_sfs`.
- **Phase S.2** — Grow (✓ DONE). Symmetric path in the same
  `resize_sfs_in_place` function. Refuses to grow past the existing
  bitmap chain's capacity — extending the chain would require
  relocating user data that currently sits at
  `bitmapbase + old_blocks_bitmap` and falls into S.3 territory. The
  in-bitmap-capacity grow case (e.g. 4096 → 6000 blocks at 512-byte
  block size, where 2 existing bitmap blocks cover up to 8000 blocks)
  works freely: marks the old backup-root and the newly-visible tail
  range free, marks the new backup-root allocated, stamps new
  rootblocks. Tests: `sfs_resize_grow_round_trip`,
  `sfs_resize_refuses_grow_past_bitmap_capacity`.
- **Phase S.3** — Shrink-past-floor with full block relocation
  (**design choice — not implemented**). Would require building a
  reverse map from each above-cut block to its owner (BNDC leaf record
  for file extents, OBJC `data_or_hashtable` for directory hashtables
  and soft-link blocks), allocating destinations in `[0..new_total)`,
  copying data, and rewriting every owner reference. The complexity
  multiplies with the existing single-leaf BNDC limitation (no leaf
  splits, see `src/fs/sfs.rs:1365`): a single split-spanning extent
  would need either a leaf split or a hard error. Substantial enough
  work that we deliberately stop at "shrink to the bitmap floor,"
  which is the most useful case in practice (matching what the
  inspect-tab minimum-size column already shows). Revisit only if a
  concrete workload needs to go smaller than the bitmap floor.
- **Phase S.4** — `validate_sfs_integrity` (~0.5 day). Reopen the
  resized volume via `open_filesystem`, sanity-check rootblock fields,
  walk the bitmap chain. Reuses the existing SFS check path where
  practical.

Total: **~3 days** for S.1 + S.2 + S.4. Phase S.3 deferred indefinitely.

Reference: `~/repos/amigasources/smartfilesystem`.

---

## PFS3 (PFS\\3 / PDS\\3 / muFS)

### On-disk landmines (the gnarliest of the three)

The PFS3 rootblock at block 2 holds many size-derived fields:

- **`rovingPointer`** — allocation cursor; clamped to `[0..total_blocks)`.
  Easy to fix (clamp to new size).
- **`alwaysFree`** — reserved free-block count used by the allocator to
  avoid full-volume deadlocks. Usually a fixed fraction; recompute from
  new size.
- **Reserved-area extent** — a region near the end of the volume holding
  rootblock extensions, anode bitmaps, bitmap-index tree nodes, and the
  deldir (deleted-files area). The extent's start block moves on resize
  (it's anchored to the volume tail).
- **Rootblock extension chain** — extension blocks pointing at the
  anode bitmap, bitmap-index root, and deldir. Pointers in these blocks
  reference the reserved-area positions; when the reserved area moves,
  every pointer in the extension chain must be rewritten.
- **Bitmap-index tree** — a multi-level tree over anode bitmaps. The
  tree itself doesn't change shape on resize unless the number of anode
  bitmaps changes (which it does when total blocks crosses a power-of-2
  threshold).
- **Anode bitmaps** — each covers a fixed number of blocks. Grow may
  require adding new anode bitmaps to the bitmap-index tree; shrink may
  drop trailing ones.
- **Block size**: in the rootblock; not changed by resize.

### Reserved-area motion is the key complication

On every resize, the reserved area moves. That means every pointer in
the rootblock extension chain has to be patched, the deldir's data may
need to copy (it's small, ~31 entries × a few blocks), and the
bitmap-index tree nodes themselves may move within the reserved area.

The reference implementation (`pfs3aio` mount-time format code) computes
all of this deterministically from `total_blocks`. We mirror its
sizing function (or call into the existing PFS3 format code we wrote
for the editable-FS test fixtures — see `create_blank_pfs3`) to compute
the new reserved-area layout, then do a guided copy.

### Shrink floor

- No file data past the new reserved-area start.
- Enough free blocks below to hold the new reserved area.
- `total_blocks - alwaysFree >= used_blocks` after the cut.

In practice the floor is **`reserved_area_size + highest_user_data_block
+ 1`**, where `reserved_area_size` is computed for the target volume
size (it grows roughly logarithmically with total blocks).

### Grow algorithm

1. Compute new reserved-area layout from `new_total_blocks`.
2. Allocate new reserved-area region near the new tail. Move every
   reserved-area block (rootblock extensions, anode bitmaps,
   bitmap-index tree nodes, deldir) into its new home. This is a
   bulk-copy followed by a deterministic pointer-rewrite pass.
3. If `new_total_blocks` adds enough capacity to need new anode
   bitmaps, allocate and initialize them inside the new reserved area;
   insert into the bitmap-index tree.
4. Extend the main bitmap coverage in the new anode bitmaps (mark new
   blocks free).
5. Recompute `rovingPointer`, `alwaysFree`, reserved-area extent
   pointer in the rootblock + every extension block.
6. Rewrite rootblock and extension checksums.
7. Run `validate_pfs3_integrity`.

### Shrink algorithm

1. Compute new reserved-area layout from `new_total_blocks`.
2. Walk anode bitmaps to confirm no user data past the new reserved-area
   start. If any: relocate down into free space (full block-relocator
   pass, harder than AFFS because file extents are tracked via anodes,
   not direct block pointers).
3. Move the reserved area into its new (lower) home — bulk copy +
   pointer rewrite.
4. Drop anode bitmaps whose covered range is entirely past the new
   tail; partially clear the last one.
5. Update rootblock fields, recompute checksums.
6. Run `validate_pfs3_integrity`.

### Anode-aware block relocation

PFS3 files reference data via **anodes** (allocation nodes), each
holding a chain of (block, count) tuples. To relocate a data block,
walk the anode owning it, rewrite the relevant tuple. Anodes themselves
live in anode-bitmap-tracked blocks. Anode IDs are stable across the
relocation — we rewrite the contents of the anode, not its identity.

This is *substantially* more code than AFFS's flat hashtable rewrite or
SFS's BNDC leaf update. Plan accordingly.

### Phase P breakdown

- **Phase P.0** — Shrink-floor function (~1 day). Depends on Phase P.1
  (reserved-area sizing) because the floor is
  `reserved_area_size(new_total) + highest_user_data_block + 1`, and
  `reserved_area_size` is itself a function of the target size. Tiny
  fixed-point loop: guess a floor, compute reserved-area at that size,
  see if it fits, iterate (typically converges in 2-3 iterations).
  Wires into `fs::partition_minimum_size`. Ships once Phase P.1 lands;
  the rest of Phase P can follow later.
- **Phase P.1** — Reserved-area sizing function (~0.5 day). Mirror
  pfs3aio; matches what `create_blank_pfs3` already does for blank
  formatting.
- **Phase P.2** — Reserved-area mover (~1.5 days). Bulk-copy +
  deterministic pointer rewrite for every extension-block field.
- **Phase P.3** — Anode-aware data-block relocator (~1.5 days). Walk
  anode, rewrite extent tuple, update bitmap.
- **Phase P.4** — Anode-bitmap and bitmap-index tree extend/trim
  (~1 day). Add/remove anode bitmaps from the bitmap-index tree.
- **Phase P.5** — `resize_pfs3_in_place` wrapper (~0.5 day).
- **Phase P.6** — `validate_pfs3_integrity` (~1 day). PFS3 has more
  invariants than AFFS — bitmap-index tree consistency, anode-bitmap
  consistency, deldir validity, reserved-area pointer chain.

Total: **~6 days**, matching Phase 10c's 4-6 estimate.

Reference: `~/repos/amigasources/pfs3aio` (non-trivial read; the
allocator + format code is in `disk.c` and `format.c`).

### Phase PD — Defragmenting clone (✓ SHIPPED, alternative to P.2/P.3/P.4)

Because PFS3's `rovingPointer` allocator scatters allocations
throughout the volume, the in-place shrink floor reported by
`last_data_byte` almost always equals the partition size — there's
nearly always at least one allocated block sitting near the tail. To
actually reclaim free space on a PFS3 volume, we needed a defragmenting
path that rebuilds the on-disk layout packed at the smaller size.

Rather than implement Phase P.2/P.3/P.4 (anode-aware in-place block
relocation + reserved-area motion), we built `clone_pfs3_volume` in
`src/fs/pfs3_clone.rs` modelled on the HFS clone pipeline:

1. Format a blank target of exactly `target_size` via
   `create_blank_pfs3`. Now sets `MODE_DATESTAMP` so dates we write
   into direntries are honored without needing a real-Amiga mount.
2. Walk the source's directory tree via `Filesystem::list_directory`,
   replaying onto the target via `EditableFilesystem::create_directory`
   / `create_file` / `create_symlink`. Each entry's
   `amiga_protection`, `amiga_comment`, and `amiga_date` (new raw
   `(i32,i32,i32)` triple on `FileEntry`) round-trip via
   `CreateFileOptions` / `CreateDirectoryOptions`.
3. Hardlinks are deferred to a second pass so any target order in the
   source walk is OK; targets that never appear during the walk are
   skipped with a warning.
4. `stream_defragmented_pfs3` wraps the clone: build a blank target,
   open it as `Pfs3Filesystem` over a tempfile (bounded RAM), clone,
   then drain the tempfile into the backup pipeline's writer.

Backup-pipeline integration: `DefragCloneShape::Pfs3` added next to
`Flat` / `Wrapped`; `detect_defrag_clone_shape` dispatches by
partition type string; both `src/backup/mod.rs` and
`src/backup/single_file_chd.rs` producer threads now call the PFS3
streamer for shape `Pfs3`. `has_defragmenting_writer` returns `true`
for PFS3 type strings, so `pick_shrink_target` returns the
defragmented value and the GUI's "Compact Space" checkbox lights up.

Floor: `Pfs3Filesystem::defragmented_minimum_size` overrides the
default `last_data_byte` and returns `used_size + 256 KiB` — a fast,
conservative estimate that gives the GUI a useful pre-clone preview.
The actual clone sizes precisely via `create_blank_pfs3`.

**v1 limitations** (documented in `pfs3_clone.rs`):
- PFS3 deldir (trashcan) is not enumerated by `list_directory`, so its
  contents are dropped during clone. This is expected behavior;
  surface as a warning in the GUI log.
- Hardlinks: bidirectional — both the link's `extrafields.link`
  (forward, resolver path) and the target's `extrafields.link` +
  `linknode.next` chain (backward, "list all hardlinks to this file")
  are wired. Mirrors pfs3aio `CreateLink` (`directory.c:2703-2727`).
  The clone tracks target_anode → target_parent_anode in a
  parent_map; pass 2 calls `register_hardlink_in_target_chain` after
  each `create_hardlink`. In-place direntry patcher grows the target's
  extrafields tail by 4-6 bytes; if the dirblock is full the patch
  errors out (chain extension for that case is a future enhancement).
- File body is buffered in a `Vec<u8>` per file during clone. Peak RAM
  is bounded by the largest single source file. Future: streaming
  pipe between source `write_file_to` and target `create_file`.

Phase P.6 (`validate_pfs3_integrity`) and the rest of Phase P remain
open for the pure in-place trim path; the defragmenting clone is the
production answer for "shrink my PFS3 partition."

---

## Shared infrastructure (Phase X — do once)

### X.1 — RDB PART-entry resize fixup (✓ DONE)

`Rdb::patch_for_restore(overrides, source)` in `src/partition/rdb.rs`
takes a `PartitionSizeOverride` list, recomputes `de_LowCyl` /
`de_HighCyl` per partition from the new (LBA, size) pair while
preserving each partition's geometry (`surfaces`, `blk_per_trk`,
`fs_block_size`), updates the RDSK `cylinders` / `hi_cyl` to span the
highest partition end, and re-stamps every 32-bit checksum. Returns
an `RdbRestorePlan` with `(block_num, 512-byte buffer)` pairs for
each modified block plus a `RdbPartitionPlan` per partition describing
the source → dest copy. Cylinder-alignment is enforced — unaligned
overrides fail with a clear error rather than silently producing a
malformed RDB.

### X.2 — `resize_filesystem_for` Amiga dispatch (✓ DONE)

`src/fs/mod.rs::resize_filesystem_for` calls
`sfs::resize_sfs_in_place` unconditionally; the SFS function probes
the rootblock id at the partition offset and silently no-ops for
non-SFS volumes. AFFS / PFS3 will plug in the same way once their
resize lands.

### X.3 — Export-pipeline RDB dispatch (✓ DONE)

The Raw/VHD export paths in `src/rbformats/{export,vhd}.rs` now do
APM-then-RDB detection via `detect_raw_apm` / `detect_raw_rdb` and
route RDB sources through `reconstruct_raw_rdb_disk` in
`src/rbformats/mod.rs`. That function:
1. Copies the first 16 sectors (RDB region) verbatim so FSHD/LSEG
   driver chains and BADB lists survive.
2. Overlays the patched RDSK + PART blocks from
   `Rdb::patch_for_restore` at their original block numbers.
3. Copies each partition's bytes from source LBA to dest LBA, zero-
   filling any growth tail.
4. Invokes `crate::fs::resize_filesystem_for` per resized partition
   so the SFS rootblock (and future AFFS / PFS3 metadata) lands at
   the new size on the dest.
5. Pads to the new total disk size (derived from the highest patched
   partition end).

Without these pieces, the SFS resize code was correct but never got
called end-to-end — the original bug report exactly matched that
gap. End-to-end coverage is `test_rdb_sfs_export_shrink_round_trip`
in `tests/filesystem_e2e.rs`.

### X.4 — Restore-flow / backup-folder dispatch (partial)

`reconstruct_disk_from_backup` (`src/rbformats/mod.rs`) bails with a
clear message when the backup folder's `partition_table_type` is
"RDB". A full backup-folder restore for Amiga disks requires raw
RDB-block preservation in the backup format plus a complete RDB
encoder (we currently only emit a parsed `rdb.json` sidecar). That
work is deferred — the direct-from-source export path covers the
"resize my Amiga disk" workflow today.

### X.4 — GUI hooks

The inspect-tab resize dialog (`src/gui/inspect_tab.rs`) already
supports per-partition resizing; the work is making `is_resizable_type`
return true for the Amiga partition strings and surfacing the per-FS
shrink floor through `partition_minimum_size`.

All three Amiga FSes are already marked `is_expensive_minimum = false`
(bitmap walk is cheap); no GUI calc-min button needed.

---

## Implementation order

0. **Phase 0 — Shrink-floor functions only** (✓ DONE). The inspect-tab
   partition list now shows the minimum size for every AFFS / PFS3 /
   SFS partition, letting you survey existing images for potential
   savings before committing to the rest of the implementation.
   - **AFFS**: existing `AffsFilesystem::last_data_byte` already returns
     `(highest_alloc_block + 1) * block_size`, which is a correct floor
     (the root-at-midpoint relocation buffer is always covered by the
     free space implied by `highest_alloc + 1 > allocated_blocks`). No
     code change needed.
   - **PFS3**: existing `Pfs3Filesystem::last_data_byte` walks the user
     bitmap (which sits past `last_reserved` at the front of the volume)
     and returns `(highest_user_alloc + 1) * 512`. The reserved area is
     anchored at the FRONT of the volume in our reader/format (contrary
     to what an earlier draft of this doc claimed), so it does not move
     on resize and no reserved-area sizing function is required for
     the floor. No code change needed.
   - **SFS**: required a fix. The trailing backup-rootblock at
     `totalblocks - 1` is always marked allocated, so the old
     `last_data_byte` returned the full partition size for every SFS
     volume. Now excludes the last `SFS_BLOCKS_RESERVED_END` block(s)
     from the bitmap walk and adds 1 block of headroom for the
     relocated backup root at the new tail. Covered by
     `sfs_last_data_byte_excludes_backup_root` and
     `sfs_last_data_byte_grows_with_file_data` in `src/fs/sfs.rs`.
1. **Phase X** — shared RDB PART fixup + `resize_filesystem_for` wiring
   (1 day). Lands after Phase 0, before any per-FS resize.
2. **Phase A** — AFFS in-place resize (3-4 days, minus the 0.5 day
   already spent in Phase 0). Most common Amiga
   restore case (DOS\\3 FFS on RDB). Unblocks the standard "restore to a
   larger CF card" workflow without touching PFS3/SFS.
3. **Phase S** — SFS in-place resize (4 days, minus 0.5 from Phase 0).
   Defer BNDC leaf splits.
4. **Phase P** — PFS3 in-place resize (6 days, minus 1.5 from Phase 0
   for the floor + reserved-area sizing). Most complex; benefits from
   the lessons of the simpler two.
5. **Phase T** — End-to-end tests (1 day). One synthetic shrink + one
   grow per filesystem in `tests/filesystem_e2e.rs`; round-trip tests
   against `~/amiga-filesystems/{DiskDoctor.adf, AmigaVision.hdf,
   amiga128gb.chd}` gated behind the existing env-var.

Total: **~15-16 days for all three filesystems**, or **~5 days for
AFFS-only** as a meaningful first slice.

---

## Non-goals (this document)

- **Block-size expand** (akin to `docs/hfs_expand_block_size.md`).
  Amiga floppies have a fixed 512-byte block size; HDF block size is
  set at format time. Changing it would require a reformat-and-copy
  pipeline equivalent to the HFS one — defer until someone asks.
- **BNDC leaf splits** in SFS. See SFS Phase S note above.
- **Aggressive shrink** in any filesystem. Unlike EFS's inode-renumber
  problem, the Amiga FSes don't have a "conservative vs. aggressive"
  split — the shrink floor falls out naturally from "no user data
  above the cut." There's no analog of EFS Phase 6 here.
- **Repair-during-resize.** Each `validate_*_integrity` pass runs
  read-only after the resize commits. If it fails, the operation is
  reported as failed; the user re-runs the existing Disk Validator
  / fsck flow separately. Resize + repair fused into one operation is
  out of scope.

---

## Reference material

- **`docs/amiga_support.md`** — full Amiga implementation plan including
  the Phase 10 bullet-list version of this resize work.
- **`docs/efs_resize_and_edit.md`** — companion document for EFS;
  similar shape, applies to a much simpler filesystem.
- **`~/repos/amigasources/ADFlib`** — AFFS reference (BSD-ish licensed,
  read-only).
- **`~/repos/amigasources/pfs3aio`** — PFS3 reference (AROS PFS3 port).
- **`~/repos/amigasources/smartfilesystem`** — SFS reference.
- **`src/fs/{affs,pfs3,sfs}.rs`** — our read + edit implementations.
  Resize code can live in the same files or in `*_resize.rs` siblings
  as the files grow.
