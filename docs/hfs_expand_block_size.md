# Expand HFS Block Size — Reformat-and-Copy Plan

Add a feature that grows a classic HFS volume past its current `drNmAlBlks`
ceiling by creating a fresh, larger HFS volume with a bigger allocation block
size and copying every file, directory, and piece of Finder metadata across.
The source image is never mutated; output is always a new file.

This is the only safe way to grow classic HFS beyond the u16 block-count
ceiling (e.g. an 8K-block / 512 MB volume up to a 32K-block / 2 GB volume).
In-place block-size doubling is not viable because HFS allocations aren't
strictly aligned to multiples of any new block size.

---

## Background

Classic HFS records every file's location as `(start_block, block_count)` in
units of `drAlBlkSiz`. Catalog records, extents B-tree, and the Volume Bitmap
all index by allocation block. Changing `drAlBlkSiz` in-place corrupts every
recorded position. The only option is **reformat-and-copy**: read every file
out, write a fresh volume with the new geometry, write each file back.

### HFS allocation block size vs maximum volume size

| Block size | Max volume |
|---:|---:|
| 8 KB (current image) | 512 MB |
| 16 KB | 1024 MB |
| 32 KB | 2047.97 MB |
| 64 KB | 4095.94 MB (HFS hard ceiling) |

Apple's tools chose block size at format time as a function of partition size
so the 65535-block u16 wouldn't overflow.

---

## Goals

1. Show allocation block size in the inspect-tab partition table for HFS
   (and HFS+) partitions so users can see what they're working with.
2. Add an **"Expand HFS Volume…"** action that produces a brand-new disk
   image file with:
   - The user's chosen target partition size and allocation block size
   - All files, folders, data forks, resource forks
   - All Finder metadata: type/creator codes, finder flags, fdLocation,
     fdFldr, FXInfo, dates (create/modify/backup), DInfo for folders
   - Volume name
   - Blessed System Folder (`drFndrInfo[0]`)
   - Boot blocks (verbatim)
   - Custom volume icon and Desktop database file (they're regular files
     in the catalog and copy through the normal path)
3. Verify the new volume with `fsck` before reporting success.
4. Source image is read-only throughout; failure leaves the destination as
   a partially-written file we can delete (or leave for inspection).

---

## Non-Goals

- Conversion to HFS+ — separate feature, separate plan.
- Shrinking. (The ceiling here is "grow + reblock", not "shrink".)
- Preserving fragmentation layout. Files will be written contiguously on
  the target, which is generally an improvement.
- Multi-fork data preservation beyond data-fork + resource-fork (HFS only
  has those two).

---

## Implementation Steps

Each step is independently testable and shippable. Mark each as ✅ DONE when
complete and update [MEMORY.md](../../.claude/projects/-Users-dani-repos-rusty-backup/memory/MEMORY.md)
if any non-obvious patterns or edge cases come up during the work.

---

### Step 1: Show HFS Allocation Block Size in Inspect Partition Table

**Goal:** Add a "Block Size" column (or tooltip / detail) for HFS and HFS+
partitions in the Inspect-tab partition list. Users currently have no way
to see the block size, which is the critical input for understanding the
512 MB ceiling.

**Files:**
- `src/partition/mod.rs` — extend `PartitionInfo` with
  `hfs_block_size: Option<u32>` (populated for both classic HFS and HFS+;
  `None` otherwise).
- `src/partition/mod.rs` (or wherever inspect probes filesystems) — when an
  Apple_HFS / Apple_HFSPlus / type 0xAF partition is detected, read the MDB
  / VH and populate the field. We already have `hfs_max_growable_size` in
  `src/fs/hfs.rs`; extract a smaller helper that just returns block size,
  reused by both the cap and the display.
- `src/gui/inspect_tab.rs` — render the block size in the partition table
  (e.g. as a column "Block Size" showing "8 KiB", "32 KiB", or blank).

**Acceptance:**
- Partition table row for an Apple_HFS partition shows its allocation
  block size.
- Non-HFS rows leave the column blank.
- Test: open a known HFS image and visually verify; existing inspect tests
  still pass.

**Status:** ✅ DONE

---

### Step 2: Catalogue Source HFS Metadata to Preserve

**Goal:** Document — and lock down with reading helpers — every byte of
metadata that needs to survive the copy. This is the spec the rest of the
plan implements against. Output: a small Rust module `src/fs/hfs_clone.rs`
holding two structs (`SourceFileSnapshot`, `SourceDirSnapshot`) and the
read functions that fill them.

**What to preserve (per HFS spec, Inside Macintosh Vol VI):**

For **files** (catalog file record, type `0x02`):
- Name (Mac Roman, ≤31 bytes, already lossy-safe in code)
- CNID (file ID) — *not* preserved; target gets fresh CNIDs
- Parent directory CNID — preserved relative to dir tree, not numeric
- `FInfo` (16 bytes at offset 4): `fdType`, `fdCreator`, `fdFlags`,
  `fdLocation` (Point), `fdFldr` (window CNID)
- `FXInfo` (16 bytes at offset 36): `fdIconID`, `fdScript`, `fdXFlags`,
  `fdComment`, `fdPutAway`
- Create date, modify date, backup date (3 × u32 at offsets 12, 16, 20)
- Data fork: logical length + extents
- Resource fork: logical length + extents
- File flags (offset 2): locked bit (0x01)

For **directories** (catalog dir record, type `0x01`):
- Name
- Parent CNID — relative to dir tree
- `DInfo` (16 bytes at offset 22): `frRect`, `frFlags`, `frLocation`,
  `frView`
- `DXInfo` (16 bytes at offset 38): `frScroll`, `frOpenChain`, `frScript`,
  `frXFlags`, `frComment`, `frPutAway`
- Create date, modify date, backup date
- Valence is *not* preserved (recomputed by target writer)

For the **volume**:
- Name (`drVN`)
- Create date (`drCrDate`), modify date (`drLsMod`), backup date (`drVolBkUp`)
- `drFndrInfo[0..7]` (32 bytes total) — most importantly `drFndrInfo[0]`
  (blessed System Folder CNID)
- Boot blocks (sectors 0..1, 1024 bytes verbatim) — these contain the
  legacy boot loader and are copied byte-for-byte
- `drAtrb` flags? — set fresh on target (consistency-checked, hwlock, etc.)

**Files:**
- New: `src/fs/hfs_clone.rs` with `SourceVolumeSnapshot`,
  `SourceFileSnapshot`, `SourceDirSnapshot`.
- `src/fs/hfs.rs` — expose any private MDB fields needed (most are already
  pub(crate)) and add `read_full_finder_info()` style helpers as needed.

**Acceptance:**
- A test that opens the existing `make_editable_hfs_image()` test image,
  calls `SourceVolumeSnapshot::capture()`, and round-trips every documented
  field by parsing the snapshot.
- Test using a real-world HFS image with non-default Finder flags +
  type/creator + dates verifies they show up correctly in the snapshot.

**Status:** ✅ DONE — `src/fs/hfs_clone.rs` holds `SourceVolumeSnapshot`,
`SourceFileSnapshot`, `SourceDirSnapshot`, `SourceCatalogSnapshot`. Capture
round-trips volume name/dates/finder_info/boot blocks, directory DInfo/DXInfo
/dates, and file FInfo/FXInfo/dates/flags/extents. Real-world-image check is
deferred to Step 5's round-trip test (the plan calls for it there anyway).

---

### Step 3: Build a Blank HFS Volume at Arbitrary Size + Block Size

**Goal:** A function that produces an in-memory HFS partition image of a
caller-specified byte size and `drAlBlkSiz`, with empty catalog/extents
B-trees, an empty Volume Bitmap (only B-tree blocks marked allocated), and
a root directory (CNID 2). Everything else (volume name, dates, blessed
folder, boot blocks) is left for Step 6 to write.

The existing `initialize_empty_btrees` in `src/fs/hfs.rs:1208` already does
most of this for an *opened* filesystem. We'll factor a standalone
`create_blank_hfs(target_size_bytes, block_size, volume_name) -> Vec<u8>`
that builds the layout from scratch.

**Layout produced:**
```
sector 0-1   : boot blocks (zeroed for now)
sector 2     : MDB (primary)
sector 3..N  : Volume Bitmap (size = ceil(total_blocks / 8 / 512) sectors)
sector N..M  : reserved up to drAlBlSt
block 0..A   : extents B-tree (sized by block_size; ~4 alloc blocks min)
block A..B   : catalog B-tree (~4 alloc blocks min, room to grow)
block B..    : free
last sector  : MDB alternate (mirror of primary)
```

**Files:**
- `src/fs/hfs.rs` — add `pub fn create_blank_hfs(...)`.
- New test verifying that:
  1. The blank image opens via `HfsFilesystem::open`.
  2. `fsck` reports zero errors.
  3. `list_directory(root)` returns an empty list.
  4. `create_file` + `sync_metadata` works on the blank volume.
  5. The chosen `block_size` and total size are honoured exactly.

**Acceptance:** Tests above pass for `block_size ∈ {512, 4096, 16384, 32768}`
and `total_size ∈ {1 MB, 100 MB, 2 GB - 1}`.

**Status:** ✅ DONE — `pub fn create_blank_hfs(target_size_bytes, block_size,
volume_name) -> Result<Vec<u8>>` in `src/fs/hfs.rs`. Iteratively sizes the
volume bitmap, reuses `build_empty_hfs_catalog` + `build_empty_hfs_extents_btree`,
reserves 4+4 allocation blocks for the B-trees, writes primary + alternate
MDB, and rejects non-512-multiple block sizes. Tests cover the four block
sizes, a near-2 GB volume at 32K blocks, and the validation failure paths.

---

### Step 4: Extend EditableFilesystem (HFS) with Finder-Metadata Setters

**Goal:** Allow writing every metadata field the snapshot carries. Some
already exist (`set_type_creator`, `write_resource_fork`,
`set_blessed_folder`); others (full FInfo+FXInfo, dates, DInfo+DXInfo)
need to be added.

**New / updated APIs on `HfsFilesystem`:**
- `set_finder_info(entry, finfo: [u8;16], fxinfo: [u8;16])` — full
  FInfo+FXInfo write, replacing the current `set_type_creator` (which is
  still useful as a thin wrapper).
- `set_directory_finder_info(entry, dinfo: [u8;16], dxinfo: [u8;16])` —
  same for directories.
- `set_dates(entry, create: u32, modify: u32, backup: u32)` — for both
  files and directories.
- `set_file_locked(entry, locked: bool)` — flips the catalog file-record
  flags byte at offset 2.
- `set_volume_dates(create, modify, backup)` — MDB fields.
- `set_boot_blocks(&[u8;1024])` — write sectors 0-1 verbatim.
- `set_volume_finder_info(&[u8;32])` — `drFndrInfo`, useful for cases
  where blessed folder is fingerprinted by more than just `drFndrInfo[0]`.

**Files:**
- `src/fs/hfs.rs` — add the methods.
- Tests for each setter using `make_editable_hfs_image()` + a round-trip
  read.

**Acceptance:** All new setters covered by round-trip tests. No existing
tests regress.

**Status:** ✅ DONE — added 7 new methods on `HfsFilesystem`: `set_finder_info`,
`set_directory_finder_info`, `set_dates`, `set_file_locked`, `set_volume_dates`,
`set_boot_blocks`, `set_volume_finder_info`. Catalog-record setters use a new
`locate_record_data` helper that traces CNID → thread → name → record and are
guarded with snapshot/rollback. Volume-level setters stage into new
`pending_boot_blocks` / `pending_volume_dates` fields applied in
`do_sync_metadata`; pending dates override the automatic `hfs_now()` modify
stamp. 12 new round-trip tests cover all setters plus an fsck-clean check.

---

### Step 5: Walk-and-Copy Engine

**Goal:** A `clone_hfs_volume(source: &mut HfsFilesystem, target: &mut HfsFilesystem)`
that, given an opened source and an opened (blank) target, walks the source
catalog and replicates every directory and file with all metadata.

**Algorithm:**
1. Capture `SourceVolumeSnapshot` from source (Step 2).
2. Apply volume-level metadata to target: name, dates, boot blocks,
   `drFndrInfo`. (Blessed folder CNID is patched in step 7 below, after
   the System Folder has been recreated and gets its new CNID.)
3. BFS the source root. Maintain `cnid_map: HashMap<u32, u32>`
   mapping source CNID → target CNID. Root is `2 → 2`.
4. For each directory entry encountered (in BFS order so parents always
   exist before children):
   - If it's a directory: `target.create_directory(target_parent, name)`,
     then `set_directory_finder_info`, `set_dates`. Record source→target
     CNID mapping.
   - If it's a file: stream data fork into
     `target.create_file(target_parent, name, &mut data_reader, size, opts)`.
     If resource fork length > 0, `target.write_resource_fork(...)`. Then
     `set_finder_info`, `set_dates`, `set_file_locked` as needed.
5. After all entries are copied, look up the source's `drFndrInfo[0]`
   (blessed folder) in `cnid_map` and call
   `target.set_blessed_folder(target_blessed_entry)`.
6. `target.sync_metadata()`.

**Edge cases handled:**
- Files with multiple extents (HFS supports up to 3 inline + extents
  overflow B-tree) — read via existing `read_file` API which already
  handles extent walking.
- Resource forks of size 0 — skip writing.
- Empty directories — directory still created; no children to copy.
- Locked files — write content first, then set lock flag.
- Aliases (fdFlags & 0x8000) — they're regular files with FInfo flags +
  resource-fork content; copy through the normal path.
- The Desktop database (`Desktop DB`, `Desktop DF`) and `.VolumeIcon` —
  also regular files in the catalog; no special handling needed.
- Files with names that aren't representable in target Mac Roman — should
  fail loudly with a per-file error, surfaced in the report.

**Files:**
- `src/fs/hfs_clone.rs` — `clone_hfs_volume` plus the BFS, the CNID map,
  and a `CloneReport` summary (files copied, dirs copied, bytes, skipped,
  errors).

**Acceptance:**
- Test: build a small source HFS in-memory with a few files, dirs, a
  resource fork, a blessed folder. Clone to a blank target with a
  different block size. Verify:
  - Every entry exists at the right path
  - `read_file` on target returns identical bytes
  - Resource forks match
  - Type/creator codes match
  - Blessed folder is set on target and matches the source's System
    Folder by name
  - `fsck` reports no errors on target
- Test: source contains a file with a 4-extent data fork (forces
  extents-overflow walk); cloned target reads back identical bytes.

**Status:** ⬜ Pending

---

### Step 6: Emit New APM Disk With Cloned Partition

**Goal:** Wrap the cloned HFS partition in a fresh APM disk image so the
output is a usable .hda / .img file, not a bare partition blob.

**Pipeline:**
1. Build APM with the standard layout: DDR (block 0), partition map entry
   for `Apple_partition_map`, optional `Apple_Driver43` partition (we copy
   the source's driver partition verbatim if present), and the new
   `Apple_HFS` partition with its expanded size.
2. Use existing `Apm::build_apm_blocks` and the `reconstruct_raw_apm_disk`
   helper as templates; this case is simpler because we control all the
   inputs.
3. Driver partition: read source disk's `Apple_Driver43` partition bytes
   and copy as-is into the new disk image at the same relative position.

**Files:**
- `src/fs/hfs_clone.rs` — `emit_apm_disk_with_hfs(source_disk, hfs_image,
  output_path, apm_layout)` orchestrator.
- Possibly extract a `partition_layout::ApmLayoutBuilder` if needed, or
  inline the layout math (it's small).

**Acceptance:**
- Test: clone an APM+HFS test disk; open the resulting file; `Apm::parse`
  succeeds; the HFS partition opens; `fsck` clean.
- Manual: round-trip a real .hda through the pipeline; resulting file
  mounts in BasiliskII / Mini vMac.

**Status:** ⬜ Pending

---

### Step 7: GUI — "Expand HFS Volume…" Action

**Goal:** Wire Steps 1–6 into the inspect tab. For HFS partitions, expose
a button that opens a dialog with target size + target block size
selectors and a "Save As…" file picker for the output.

**UI:**
- Inspect tab, partition row context (or per-partition action button) for
  Apple_HFS rows: "Expand HFS Volume…".
- Dialog:
  - Source summary: current size, current block size, used space, file
    count.
  - Target partition size (MiB), with a min derived from used space.
  - Target allocation block size: dropdown of valid choices (4K, 8K, 16K,
    32K), each labeled with its max possible volume size.
    - Auto-suggested: smallest block size whose ceiling ≥ target size.
  - Output filename ("Save As…").
- Progress reporting via the existing log + progress bar pattern (status
  messages: "Reading source catalog… (532 entries)", "Copying files…
  (212 / 532, 84 MB / 412 MB)", "Verifying target volume…", "Done").
- On success: success dialog with a CloneReport summary and a button to
  open the output file in the inspect tab.
- On failure: error dialog with the failure message; the partial output
  file is left on disk and its path shown in the dialog.

**Files:**
- `src/gui/inspect_tab.rs` — the action button and dialog state machine.
- `src/gui/expand_hfs_dialog.rs` (or inline depending on scope) — dialog UI.

**Acceptance:** Manual end-to-end: load an HFS APM image, click Expand,
choose a larger size + larger block size, pick output path, watch progress,
verify resulting file mounts and contains every original file.

**Status:** ⬜ Pending

---

### Step 8: Post-Clone Verification + Final Polish

**Goal:** Belt-and-suspenders integrity checks before declaring success,
plus polish.

**Verification (run automatically after Step 6 emits the disk):**
1. Open the newly written HFS partition.
2. Run `fsck`. If errors, fail loudly and keep the output file for
   inspection — do not delete the partial output behind the user's back.
3. File-count and total-byte checks against the `CloneReport`: they must
   match the source enumeration.
4. (Optional) Per-file SHA256 spot-check: pick N random files, compare
   source vs target hashes. Configurable; default off because it doubles
   the read cost.

**Polish:**
- Cancellation: long copies must respect the cancel button.
- "Compare source and target" diagnostic action that re-runs an
  enumeration of both and reports any divergence (useful when debugging
  weird cases).
- Documentation update: `README.md` mention the feature and its 2 GB ceiling.

**Files:**
- `src/fs/hfs_clone.rs` — verification helpers.
- `src/gui/inspect_tab.rs` — cancellation plumbing.
- `README.md` — short feature blurb.

**Acceptance:** End-to-end test runs against a real-world HFS image and
reports clean. Cancellation mid-copy leaves a half-written file on disk
without crashing. The partial file is clearly labelled in the failure
dialog.

**Status:** ⬜ Pending

---

## Test Strategy

- **Unit tests** for each new function in `src/fs/hfs_clone.rs` and the
  blank-volume builder.
- **Round-trip tests** for the full pipeline using small synthetic HFS
  volumes (already easy to build in tests) — including all of: no
  blessed folder, blessed folder set, resource forks, deep directory
  trees, files with >3 extents, locked files, invisible files.
- **Real-disk smoke test** documented as a manual check: round-trip the
  user's `HD1_Q800d1.hda` through 8K → 32K and confirm Quadra-on-emulator
  boots from the result.

---

## Risk Notes

- HFS extents-overflow B-tree handling on the read side is the most
  delicate part. Reuse `HfsFilesystem::read_file` rather than re-implementing.
- Mac Roman name encoding: source might contain bytes that round-trip
  through our converter unchanged; if any file fails to encode on the
  target, surface a clear error rather than silently renaming.
- Date fields are HFS-epoch (seconds since 1904-01-01 UTC), not Unix.
  Already handled by `hfs_common::hfs_now`; reuse for parsing too.
- Blessed-folder fingerprinting beyond `drFndrInfo[0]`: some Mac OS
  versions also stash CNIDs in `drFndrInfo[3]` (the OS folder) and
  `[5]` (Mac OS X System file). Copy all 8 ints verbatim with the
  CNID-remap applied, not just the first one.
