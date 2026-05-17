# EFS — Editable Filesystem + In-Place Grow / Shrink

Add full read/write support for SGI's Extent File System: create and delete
files and directories from the browse view, grow the volume to fill a larger
partition, and shrink the volume to its used footprint. All operations happen
in-place — no clone-and-replace pipeline. The XFS counterpart is tracked
separately and is **out of scope** for this document; see
`docs/SGI_Filesystems.md` for the XFS read-side plan and the eventual
rebuild-via-clone approach.

EFS has never had a resize tool on any platform. IRIX shipped `mkfs_efs(1M)`
and `fsck_efs(1M)`; no `growfs_efs` or `shrinkfs_efs` ever existed. We are
writing this from scratch against the on-disk format, using the existing
read-side implementation in `src/fs/efs.rs` as the structural reference.

---

## Why in-place (not clone-and-replace)

For XFS the clone pipeline is mandatory — packed inode numbers, AG-relative
btree pointers, and v5 metadata CRCs make in-place shrink prohibitively
complex. EFS has none of those constraints:

- **No btrees anywhere.** Free space is a flat bitmap; the inode table is a
  fixed array per cylinder group; directories are flat `(inum, name)` slot
  arrays; files have 12 inline extents and no indirect blocks.
- **No checksums.** No CRC or UUID stamping required when rewriting metadata.
- **No journal.** Sync-boundary atomicity (same pattern as SFS) is sufficient.
- **Big-endian, fixed layout.** A 92-byte superblock at sector 1 + a replica
  at `sb_replsb` is the entire volume header story.

Building a clone pipeline would mean writing `create_blank_efs` plus a
snapshot-and-replay engine just to avoid touching the source — a lot of code
for a filesystem where the in-place write primitives are themselves small.
In-place is the right call here.

---

## Background — on-disk layout (cribbed from `src/fs/efs.rs` + Linux kernel `fs/efs/`)

```
sector 0      boot block (unused by EFS; left for SGI volume header)
sector 1      primary superblock (92 useful bytes, parsed at byte 0)
sector ...    cylinder groups, each containing:
                cgisize blocks of inode table (4 inodes per 512-byte block)
                data blocks
sb_replsb     replica superblock (offset stored in primary sb)
```

Per-CG geometry comes from the superblock:

| Field | Meaning |
|---|---|
| `fs_size` | total volume size in 512-byte blocks |
| `firstcg` | block number where the first CG starts |
| `cgfsize` | blocks per CG (inode region + data region combined) |
| `cgisize` | inode-table blocks at the start of each CG |
| `ncg` | number of CGs |
| `bmsize` | bitmap size in **bytes** |
| `bmblock` | block number of the free-block bitmap |
| `replsb` | block number of the replica superblock |

Derived facts:

- **Inode number → physical position** is fully determined by CG geometry:
  `cg = (inum - 1) / inodes_per_cg`,
  `slot_in_cg = (inum - 1) % inodes_per_cg`,
  `inode_block = firstcg + cg * cgfsize + slot_in_cg / 4`,
  `byte_offset = (slot_in_cg % 4) * 128`.
  This is *the* constraint that makes shrink hard if we want to drop CGs
  that hold inodes — see Phase 6 (backlogged).
- **Inode "free" marker** is `di_mode == 0`. There is no separate inode bitmap.
- **Extent magic** byte must be `0`; non-zero indicates a corrupt extent.
- **Bitmap convention**: 1 bit per **data block**; the bitmap covers only the
  data region, not the inode-table region. Bit set = block in use (verify
  against the kernel — easy bug source).
- **Root inode** is fixed at inum 2 and must remain so. Bumping the root is
  not a supported operation by the IRIX kernel or any historical tool.

---

## Goals

1. Edit Mode in the browse view works on EFS partitions: add files via
   drag-and-drop, create directories, delete entries, with the same staged-
   edits flow used by HFS / FAT / NTFS / exFAT.
2. The inspect-tab resize dialog offers an in-place "Resize to N MB" action
   for EFS partitions, with both minimum-size and custom-size paths.
3. Grow works up to whatever space is available in the containing partition.
4. Shrink works down to the **conservative floor** (defined below). The
   aggressive floor (requires inode renumber) is backlogged.
5. Every resize is followed by an automatic `fsck_efs`-equivalent verifier
   pass before the new volume header is sealed.

---

## Non-goals (this document)

- XFS write support of any kind.
- EFS clone / rebuild pipeline.
- EFS aggressive shrink with inode renumbering — see Phase 6.
- Resource forks, xattrs, ACLs (EFS has none).
- Symlink editing beyond what the existing `src/fs/efs/symlink.rs` exposes
  (read-only). Add-side symlink creation is a stretch goal inside Phase 1.

---

## Phase 1 — Editable EFS

Implement `EditableFilesystem` for `EfsFilesystem`. The trait is already
defined in `src/fs/filesystem.rs` and exercised by the staged-edits UI in
`src/gui/browse_view.rs`; the work here is providing the EFS-side primitives.

### 1.1 Allocator primitives

In `src/fs/efs.rs` (or a new `src/fs/efs_alloc.rs` if the file gets large):

- `fn read_bitmap(&mut self) -> io::Result<Vec<u8>>` — load the full bitmap
  from `bmblock` × 512 covering `bmsize` bytes.
- `fn write_bitmap(&mut self, bm: &[u8]) -> io::Result<()>` — write back.
- `fn alloc_data_extent(&mut self, want_blocks: u32) -> io::Result<Vec<EfsExtent>>`
  — first-fit run search over the bitmap; returns one or more extents
  totaling `want_blocks`. Caller chooses how many to use (an EFS inode has
  12 slots, so if the allocator returns more than 12 fragments the write
  must fail with `DiskFull` — EFS cannot represent fragmented files past
  12 extents).
- `fn free_extent(&mut self, ext: EfsExtent)` — clear bits.
- Inode allocator: scan inode-table region of each CG for `mode == 0`,
  return the first free inum. Track high-water and free-counts in memory to
  avoid rescanning per allocation.

### 1.2 Inode mutation

- `fn read_inode(&mut self, inum: u32) -> io::Result<EfsInode>` (already exists
  on the read path; promote to write-friendly signature taking `&mut self`).
- `fn write_inode(&mut self, ino: &EfsInode) -> io::Result<()>` — serialize
  128 bytes, write into the correct slot, no surrounding-block read-modify-
  write hazards because inodes are naturally aligned.
- Helpers: `inode_position(&self, inum: u32) -> (block, byte_off)`.

### 1.3 Directory mutation

EFS directories are simple slot arrays; format details live in
`src/fs/efs.rs` around line 380. Add:

- `fn dir_insert(&mut self, dir_inum: u32, name: &[u8], child_inum: u32)`
- `fn dir_remove(&mut self, dir_inum: u32, name: &[u8])`
- Directory blocks must stay 512-byte sized; growing past a block boundary
  extends the dir's extent list. Compaction on remove collapses adjacent
  free slots so subsequent inserts succeed.
- Reject names containing `/` or NUL; truncate to the EFS name-length
  ceiling (255 bytes per slot).

### 1.4 `EditableFilesystem` impl

Methods required by the trait (see `src/fs/hfs.rs` for the reference shape):

- `create_file(parent, name, opts)` — alloc inode, init `mode=0o100644`,
  zero extents, link into parent dir.
- `create_directory(parent, name, opts)` — alloc inode, init `mode=0o040755`,
  alloc one data block for the dir, write `.` and `..` entries, link into
  parent.
- `write_file_data(inum, offset, bytes)` — extent allocation + write. Honor
  the 12-extent ceiling. Sparse-file holes are *not* supported by EFS;
  writing past EOF zero-fills.
- `delete_entry(parent, name)` — unlink dir entry, decrement nlink, free all
  extents + inode if nlink == 0.
- `delete_recursive(parent, name)` — depth-first.
- `sync_metadata()` — flush bitmap, dirty inodes, superblock + replica
  superblock. Per the
  [edit-mode stabilization rule](../CLAUDE.md#edit-mode-stabilization-docshfs_write_tweaksmd),
  individual mutation methods must NOT call sync; only the caller does.

### 1.5 Browse-view integration

Wire EFS into the existing flow:

- `open_editable_filesystem_by_string` in `src/fs/mod.rs` dispatches `"EFS"`
  to `EfsFilesystem::open` over a `Read + Write + Seek` reader.
- `is_layout_preserving_fs` returns `true` for EFS (it has free space; only
  the inode-table regions and bitmap have fixed positions).
- Staged-edits, projected-free-space, virtual-overlay all already work
  generically once the trait is implemented.

### 1.6 Tests

- Round-trip: create file → reopen → read back.
- Create + delete leaves bitmap and inode region pristine (compare bytes).
- Fragmented-write hits the 12-extent ceiling and errors cleanly.
- Directory grows past 1 block, then compacts back to 1 block on delete.
- Replica superblock stays in sync with primary.

---

## Phase 2 — EFS fsck

Independent of editing but required as the verifier for every resize. Lives
in `src/fs/efs_fsck.rs`, returning the shared `FsckResult` / `FsckIssue`
types from `src/fs/fsck.rs`.

Checks:

1. **Superblock integrity** — magic, sane geometry, `firstcg + ncg * cgfsize
   <= fs_size`, `bmblock` and `replsb` inside the volume, replica matches
   primary.
2. **Bitmap consistency** — count set bits, compare against the sum of all
   in-use inodes' extents. Flag double-allocations and orphan blocks.
3. **Inode table** — every `mode != 0` inode parses, has a valid extent
   chain (magic byte == 0, length > 0, extents don't overlap each other or
   cross CG boundaries pathologically), `nlink > 0`.
4. **Directory chain** — every dirent's `inum` resolves to an in-use inode;
   every directory has a `.` and `..` and the `..` matches its parent.
5. **Connectivity** — BFS from root (inum 2); orphan inodes go to
   `lost+found` if `EditableFilesystem::repair()` is invoked. (Repair is
   optional and can come later — verifier-only is enough for resize.)

Surface in the GUI alongside HFS via `is_checkable_type()` in
`inspect_tab.rs`.

---

## Phase 3 — Grow

Implement `pub fn grow_efs(reader: &mut (impl Read + Write + Seek),
partition_offset: u64, new_size_blocks: u32) -> io::Result<()>`.

Two sub-cases inside one entry point, chosen by size delta:

### 3a Extend-last-CG (small grow)

When `new_size - old_size < cgfsize`:

1. Move the replica superblock to its new position at `new_size - 1`
   (or wherever IRIX convention dictates — confirm against kernel
   `fs/efs/super.c`).
2. Zero the newly-added data blocks (safety, not strictly required).
3. Extend the bitmap: if the new bits fit inside the existing `bmsize`
   bytes (i.e. trailing zero-bits were available), just clear them; if
   not, allocate a new bitmap region of the required size, copy the old
   bitmap forward, free the old bitmap blocks. Update `bmblock` and
   `bmsize` in the superblock.
4. Bump `fs_size` in primary and replica superblock.
5. Run Phase-2 fsck.

### 3b Append-new-CGs (large grow)

When `new_size - old_size >= cgfsize`:

1. Compute `added_cgs = (new_size - old_size) / cgfsize`.
2. For each new CG: zero its inode-table region (every slot `mode == 0`
   means "free"). The data region need not be zeroed (it's free per the
   bitmap).
3. Move/extend bitmap as in 3a; mark all bits in the new data regions as
   "free."
4. Bump `fs_size`, `ncg`, and bitmap fields in both superblock copies.
5. Run Phase-2 fsck.

### 3c Heuristic

Choose 3b when at least one full CG fits in the growth space; otherwise 3a.
If the user's requested new size leaves a fractional CG at the end, treat
the fractional block count as a 3a-style extension on top of the new last
CG.

---

## Phase 4 — Conservative Shrink

Implement `pub fn shrink_efs_conservative(reader: &mut (impl Read + Write +
Seek), partition_offset: u64, new_size_blocks: u32) -> io::Result<()>`.

Pre-check:

- Compute the **conservative floor** = the smallest `new_size` such that
  every CG containing any in-use inode is fully preserved. Concretely:
  find the highest-numbered CG `H` with any non-zero-mode inode; the floor
  is `firstcg + (H + 1) * cgfsize`, possibly minus the trailing data-area
  blocks of CG `H` that are unallocated.
- Reject calls with `new_size < floor` and surface the floor to the caller
  for the dialog.

Algorithm:

1. **Plan the data relocation.** Build a list of every allocated data
   block currently above `new_size`. For each, find a free block strictly
   below `new_size` in the same partition; record `(old_block, new_block)`
   pairs.
2. **Update inodes first** (in memory). For every inode whose extents
   reference any above-cut blocks: replace the relevant extent entries to
   point at the new locations. Extents are 8 bytes each and can be split
   if the relocation crosses extent boundaries (the destination free run
   may be discontiguous). Respect the 12-extent ceiling — if relocation
   would push an inode past 12 extents, fail the shrink and surface the
   error. (Possible mitigation: prefer contiguous destination runs first
   to minimize fragmentation.)
3. **Copy the data.** Stream each `(old_block, new_block)` copy, 512 bytes
   at a time. Use a temporary read buffer; we are not source-destructive
   until step 5.
4. **Update the bitmap.** Mark relocated old blocks free, new blocks used.
5. **Truncate logically.** Move the replica superblock down. Update
   `fs_size`, `ncg` (drop any wholly-empty trailing CGs), `bmsize`,
   `bmblock` in both superblock copies.
6. **Write everything** in this order: data copies → inodes → bitmap →
   replica superblock → primary superblock. The primary-superblock write
   is the commit point; a crash before it leaves the volume readable at
   its old size (the relocated data is duplicated but harmless).
7. **Run Phase-2 fsck.** If it errors, the operation halts before primary
   superblock commit and the user keeps the original volume intact.

### Floor reporting

Wire the floor into `fs::partition_minimum_size` (see CLAUDE.md memory
"Partition Minimum-Size Unified Handler"). EFS is an expensive minimum
because computing the floor requires walking all inodes — mark it via
`is_expensive_minimum`.

---

## Phase 5 — GUI + Restore Integration

### 5.1 Inspect-tab resize dialog

The dialog already exists for FAT / NTFS / exFAT / HFS. Add EFS:

- `is_resizable_type()` returns true for the EFS partition-type strings.
- Resize dialog shows: current size, used size, conservative floor (with
  a tooltip explaining why it's higher than "raw used bytes"), and a
  slider clamped to `[floor .. partition_max]`.
- Worker thread dispatches to `grow_efs` or `shrink_efs_conservative`
  based on direction.

### 5.2 Restore flow

The restore pipeline (`src/restore/mod.rs`) detects filesystem type via
boot-sector magic to call the correct resize function after writing the
partition image. Add an EFS branch:

- Detect EFS by magic at partition_offset + 512 (`EFS_MAGIC_OLD` /
  `EFS_MAGIC_NEW`).
- Call `grow_efs` if the target partition is larger than the imaged
  partition, `shrink_efs_conservative` if smaller. (Shrink during restore
  is the same code path; this is why the conservative floor must be
  computed correctly.)

### 5.3 Hidden-sectors patching

EFS has no equivalent of `BPB_HiddSec` — partition offset is not stored in
the superblock. No patching needed; do **not** add an `efs::patch_hidden_
sectors` stub.

---

## Phase 6 — Aggressive Shrink (BACKLOGGED)

Not in scope for the initial work. Recorded here so the design is on paper
when we come back to it.

Goal: shrink past the conservative floor by re-packing inodes into the
lowest CGs.

Approach:

1. Build forward map `old_inum → new_inum` by walking the in-use inode
   list and assigning them to the lowest free slots, preserving root = 2.
2. Two-pass directory rewrite: walk every directory, replace each
   `(old_inum, name)` slot with `(new_inum, name)`. EFS dirs being flat
   slot arrays makes this tractable; XFS dir2 would not be.
3. Move inode bodies to their new positions. Inode contents survive the
   move untouched because extents are block-relative (not inode-relative).
4. Then run the Phase-4 data relocation + sb update.

Risk surface: any inode reference we miss = catalog corruption. Symlinks
store path strings, regular files have no parent backrefs, so dirents
are the only reference type — bounded but every dirent must be visited.

Mitigation: run Phase-2 fsck before *and* after the renumber pass;
abort to a snapshot if the post-fsck reports new orphans.

Estimated effort: 3-4 days on top of Phases 1-5.

---

## Implementation order

1. **Phase 1** — Editable EFS (1 week)
2. **Phase 2** — EFS fsck (3 days)
3. **Phase 3** — Grow (2 days; uses Phase 1 allocator)
4. **Phase 4** — Conservative shrink (3 days)
5. **Phase 5** — GUI + restore integration (2 days)
6. **Phase 6** — Aggressive shrink (deferred)

Each phase ships as its own PR. Phase 2 is independent of Phase 1 and can
proceed in parallel if useful, but Phase 1 lands first because the resize
phases depend on its allocator and inode-mutation primitives.

---

## Reference material

- Linux kernel `fs/efs/` — definitive on-disk format reference
  (read-only driver, but the layout details are complete).
- IRIX `mkfs_efs(1M)` and `fsck_efs(1M)` man pages — CG sizing
  conventions and integrity-check definitions.
- `src/fs/efs.rs` — our read implementation. The resize and edit code
  can mostly live alongside it, with `efs_alloc.rs` / `efs_fsck.rs` /
  `efs_resize.rs` siblings as the file grows.
- `docs/SGI_Filesystems.md` — original read-side plan; the XFS portions
  of that document are still in scope as a separate workstream.
- `docs/hfs_expand_block_size.md` — pattern reference for the
  blank-volume + clone approach, included here only to contrast with
  the in-place approach we are taking for EFS.
