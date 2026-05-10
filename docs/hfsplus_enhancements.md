# HFS+ Enhancements — Defrag-on-Backup, Accurate Minimum Size, and Edit-Mode Write Stabilization

Single combined plan covering three related bodies of work for HFS+:

1. **Accurate minimum size** — surface a *true* (defragmented) minimum next to the in-place trim point.
2. **Edit-mode write stabilization** — close the parity gap with classic HFS so inspect-tab edits don't corrupt the catalog or extents B-tree, and add the missing pieces (extended attributes, hardlinks, HFSX) needed for modern macOS volumes.
3. **Defrag-on-backup** — a clone-into-blank pipeline integrated as an opt-in backup mode that pipes source → packed HFS+ → compressor.

The three bodies share machinery (snapshot/rollback, fragmented allocation, extents-overflow write, blank-volume builder, fsck) so they're planned in one linear sequence. Each step is sized for **one session** — ~200–600 lines of code + tests — and leaves the tree green (`cargo build --all-targets`, `cargo test --lib`, `cargo clippy`).

## Why this order

- **Min-size first** delivers the visible user-reported fix (inspect-tab numbers, backup metadata) before any risky write-side work, and depends on nothing else.
- **Defensive guard before write foundations** — edit mode already works (poorly) on real HFS+ today; refusing journaled volumes up front prevents user-data corruption while we build the proper write paths.
- **Write foundations before clone, blank-builder before fsck before clone** — every later step needs at least one of these primitives.
- **Xattrs and hardlinks land before user-visible defrag-on-backup** — modern macOS HFS+ volumes (including the user's reference disks) carry both. Shipping defrag without them would silently drop xattrs and collapse hardlink topology, corrupting restores. The clone pipeline ships as engine code first, gets enhanced as xattr/hardlink support arrives, and only then surfaces in the GUI.
- **HFSX folds into the blank builder** — `create_blank_hfsplus` takes a `case_sensitive: bool` parameter from step 1 of phase 4 instead of being added later (which would break the signature).

## Conventions

Follow the patterns in [`CONTRIBUTING.md`](../CONTRIBUTING.md):
- Engine layer (`src/fs/`) before GUI; unit tests inline under `#[cfg(test)]`.
- Background work goes through a `model/*_runner.rs` with `Arc<Mutex<XxxStatus>>`. Never call `effective_partition_size` directly from the GUI; route through `partition_minimum_size`.
- ASCII only in user-visible strings.
- Preserve every parsed on-disk field, even unread ones.
- Use `hfs_common::walk_leaf_records` for catalog walks and `btree_split_leaf_with_insert` for any new leaf splits.

After each step: tick the box, run `cargo build --all-targets` (zero warnings), `cargo test --lib`, `cargo fmt`, and update [`MEMORY.md`](../../.claude/projects/-Users-dani-repos-rusty-backup/memory/MEMORY.md) if a non-obvious pattern surfaces.

---

## Phase 1 — Accurate minimum size

Read-only computation, lowest risk, ships the user-reported fix first.

### Step 1 — Defragmented-minimum computation

- [x] **Goal:** `partition_minimum_size` for HFS+ returns both the in-place trim point (today) and the post-defrag minimum (used data + B-tree overhead + VH region).

**Files:** `src/fs/mod.rs`, `src/fs/hfsplus.rs`

**Changes:**
- Extend `MinimumResult::Computed(Option<u64>)` to `MinimumResult::Computed { in_place: Option<u64>, defragmented: Option<u64> }`. Update every caller — `min_size_runner.rs`, `backup/sizes.rs`, `inspect_tab.rs`, `backup_tab.rs`. Non-HFS+ filesystems return `defragmented = in_place`.
- Add `HfsPlusFilesystem::defragmented_minimum_size() -> Result<u64>`:
  - `(total_blocks - free_blocks) * block_size` for user data
  - Plus catalog-file, extents-file, attributes-file, allocation-file logical sizes (already on `vh`)
  - Plus boot/VH/alt-VH overhead (`first_alloc_block * block_size` + 1024 for alt-VH region)
  - Round up to a multiple of `block_size`
- Make `last_data_byte()` callers explicit: it is the in-place trim, not the minimum.

**Tests:** Synthesize a 100 MiB volume with a 60 MiB free run in the middle: `in_place ≈ 100 MiB`, `defragmented ≈ 40 MiB + overhead`.

### Step 2 — Surface both numbers in the UI

- [x] **Goal:** Inspect-tab Min Size column and the backup-summary log show both values for HFS+; the existing "Calc min" deferred button computes both in one walk.

**Files:** `src/model/min_size_runner.rs`, `src/model/status.rs`, `src/gui/inspect_tab.rs`, `src/gui/backup_tab.rs`

**Changes:**
- Add `defragmented_min: Option<u64>` next to the existing `result` on `MinSizeStatus`.
- Inspect-tab Min Size column: render `"57.4 GiB / 38.2 GiB defrag"` for HFS+, plain single value otherwise. Tooltip: `"Left = trim only; right = if cloned into a blank volume during backup."`
- The "Trim only / Defragment" sub-radio in `size_mode_row.rs` was originally listed here but moves to **Step 22** — until the clone path exists, exposing a "Defragment" choice would promise behavior the engine can't deliver.

**Tests:** GUI is not unit-tested; verify by running `cargo run` on a fragmented HFS+ image.

### Step 3 — Honest min size in `metadata.json`

- [x] **Goal:** Backup metadata records both numbers so restore-time UI can pick the right one.

**Files:** `src/backup/metadata.rs`, `src/backup/sizes.rs`, `src/backup/mod.rs`, `src/fs/mod.rs` (new `defragmented_partition_size` helper), `src/restore/mod.rs` (struct fill-in only)

**Changes:**
- Extend `PartitionMetadata`: add `defragmented_min_size_bytes: Option<u64>` next to `minimum_size_bytes`. Optional + `#[serde(default)]` so old metadata loads.
- `PartitionSizing` gains `defragmented_min_sizes: Vec<Option<u64>>`; `analyze_partitions` populates it via the new `fs::defragmented_partition_size` helper, only retaining values that are strictly smaller than the in-place minimum.
- The "Defragmented min available" restore-side log message moves to **Step 22** — until the defrag-restore path exists, surfacing it now would be noise the user can't act on.

**Tests:** `test_defragmented_min_size_round_trip` covers serialize+deserialize with both `Some` and `None`, plus a stripped-JSON path that confirms `#[serde(default)]` lets pre-Phase-1 metadata load.

---

## Phase 2 — Edit-mode safety net

One step, lands before any other write-side work so existing edit mode stops corrupting journaled volumes.

### Step 4 — Refuse edits on journaled / xattr-bearing volumes (defensive)

- [x] **Goal:** Until the proper xattr B-tree write side (Phase 5) and journal replay (deferred) ship, return a clear error rather than corrupting metadata. The xattr half of this guard is removed in Step 13; the journaled half stays until journal replay ships separately.

**Files:** `src/fs/hfsplus.rs`, `src/gui/browse_view.rs` (error surface)

**Changes:**
- In `open_editable_filesystem` for HFS+: if `vh.attributes & kHFSVolumeJournaledBit (0x2000)` set, return `Err(FilesystemError::Unsupported("journaled HFS+ volume — clear the journal in macOS or open read-only"))`.
- If `vh.attributes_file.logical_size > 0`: warn but allow; refuse `delete_entry` on any file with matching records in the attributes B-tree (scan once at open, cache CNID set).
- GUI: surface the error in the existing toast; don't attempt to enter edit mode.

**Tests:** Unit test `open_editable_filesystem` against a synthetic VH with the journaled bit set.

---

## Phase 3 — Edit-mode write foundations

Each step builds on the one before. Snapshot/rollback first because every other write depends on it for atomicity.

### Step 5 — Snapshot / rollback for HFS+ mutations

- [x] **Goal:** Any failure inside `create_file` / `create_directory` / `delete_entry` / `set_type_creator` / `write_resource_fork` / `set_blessed_folder` rewinds in-memory state instead of leaving a half-applied catalog.

**Files:** `src/fs/hfsplus.rs`

**Changes:**
- Derive `Clone` on `HfsPlusVolumeHeader` and `BTreeHeaderRecord`.
- Add `HfsPlusFilesystem::snapshot()` returning a struct with `vh`, `catalog_data`, `catalog_header`, `bitmap`, `extents_overflow_data`, and (anticipating Phase 5) `attributes_data`. Add `restore_snapshot(s)`.
- Wrap each `EditableFilesystem` mutation in:
  ```
  let snap = self.snapshot();
  let r = (|| { ... existing body ... })();
  if r.is_err() { self.restore_snapshot(snap); }
  r
  ```
- `update_parent_valence` returns `Err(FilesystemError::NotFound(...))` instead of silently no-op'ing — this is what triggers the rollback.

**Tests:** `create_file` with a forced `DiskFull` error leaves `vh.free_blocks`, `vh.file_count`, and the catalog bytes byte-identical to the pre-call state.

### Step 6 — Fragmented block allocation

- [x] **Goal:** `allocate_blocks` no longer fails with `DiskFull` when free space exists but isn't contiguous.

**Files:** `src/fs/hfsplus.rs`

**Changes:**
- Replace `allocate_blocks(count) -> Result<u32>` with `allocate_extents(blocks_needed: u32) -> Result<Vec<ExtentDescriptor>>`. Greedy: largest free run first, accumulate until satisfied.
- Update `write_data_to_blocks` to walk the returned `Vec<ExtentDescriptor>`.
- Verify `free_fork_blocks` covers all 8 inline slots and stops at the first empty one.
- Update `next_allocation` hint after each allocation.

**Tests:** Pre-fragment a synthetic 1 MiB volume bitmap (alternating 1-block free / 1-block used), allocate a 4-block fork, verify it lands as 4 single-block extents and round-trips.

### Step 7 — Extents overflow B-tree write side

- [x] **Goal:** Files with more than 8 extents persist correctly via the extents-overflow B-tree (read side already exists at `read_fork_with_overflow`).

**Files:** `src/fs/hfsplus.rs`

**Changes:**
- After `allocate_extents` returns more than 8 extents, store the first 8 in `ForkData.extents`; for the remainder, build extents-overflow records keyed by `(forkType, fileID, startBlock)` and insert into `vh.extents_file`.
- `delete_entry`: when freeing a fork with overflow extents, remove its overflow records and free the blocks.
- Use `btree_split_leaf_with_insert` for splits.
- `do_sync_metadata` writes `extents_overflow_data` back via `write_fork_data`.

**Tests:** Synthesize a 12-extent file via the fragmented allocator, sync, reopen, verify `read_fork_with_overflow` reproduces the data byte-exactly.

### Step 8 — Touch dates + counters on every mutation

- [x] **Goal:** Catalog records show correct `contentModDate`/`attributeModDate`/`accessDate`; volume header `file_count`/`folder_count`/`modify_date` stay in sync.

**Files:** `src/fs/hfsplus.rs`

**Changes:**
- After every catalog-record edit, stamp `attributeModDate` (and `contentModDate` for fork writes) with `hfs_common::hfs_now()`.
- `set_type_creator` and `write_resource_fork` currently mutate the record bytes but don't touch dates; fix both.
- Verify `delete_entry` decrements `vh.file_count` / `vh.folder_count` and `create_*` increments.

**Tests:** Round-trip a `create_file` and assert the catalog record's three date fields equal sync-time `hfs_now()` to ±1 second.

---

## Phase 4 — Blank volume, snapshot capture, and fsck harness

Engine primitives the clone path needs. fsck lands before the clone so it's a verification harness for every later step. HFSX folds into the blank builder so its signature is right from the start.

### Step 9 — `create_blank_hfsplus` (HFS+ and HFSX)

- [x] **Goal:** Library function `pub fn create_blank_hfsplus(size_bytes: u64, block_size: u32, name: &str, case_sensitive: bool) -> Vec<u8>` that emits a freshly formatted, mountable HFS+ or HFSX image.

**Files:** `src/fs/hfsplus.rs`

**Changes:**
- Mirror `create_blank_hfs_sized` from classic HFS:
  - Boot blocks (1024 zero bytes is fine for non-bootable HFS+).
  - VH at offset 1024 with `signature = HFSX_SIGNATURE` if `case_sensitive` else `HFS_PLUS_SIGNATURE`, `version = 4`, `attributes = kHFSVolumeUnmountedBit (0x100)`. Never set the journaled bit.
  - Allocation file (size = `total_blocks / 8` rounded up to a block multiple).
  - Catalog file with `keyCompareType = kHFSBinaryCompare (0xBC)` if `case_sensitive` else `kHFSCaseFolding (0xCF)` in the BTHeaderRec.
  - Extents file (start = 4 blocks, initial size = 1 MiB).
  - Attributes file (logical_size = 0 for blank).
  - Empty catalog with two records: root folder thread (parent=1, key="", `recordType=3`, `parentID=2`, name=volume name) and root folder record (parent=1, key=name, `recordType=1`, `folderID=2`, valence=0, FolderInfo zeroed).
  - Empty extents B-tree (header node only).
  - Bitmap with reserved region marked allocated; everything else clear.
  - Alt-VH at `total_blocks * block_size - 1024`.

**Tests:** Build a 32 MiB HFS+ blank and a 32 MiB HFSX blank; both open via `HfsPlusFilesystem::open`, root listing empty, free-block count matches, `fs_type()` reports the correct string.

### Step 10 — Source snapshot capture

- [x] **Goal:** New `src/fs/hfsplus_clone.rs` (parallel to `hfs_clone.rs`) defining `SourceVolumeSnapshot`, `SourceFileSnapshot`, `SourceDirSnapshot`, `SourceCatalogSnapshot::capture(fs: &mut HfsPlusFilesystem)`.

**Files:** `src/fs/hfsplus_clone.rs` (new), `src/fs/mod.rs` (module wire-up)

**Changes:**
- `SourceFileSnapshot` carries: name, parent CNID, source CNID, type/creator, finder flags, dates (4), data fork bytes, resource fork bytes, BSD info if present.
- `SourceDirSnapshot` carries: name, parent CNID, source CNID, valence, FolderInfo, dates (4).
- `capture()` walks via `hfs_common::walk_leaf_records`, skipping thread records.
- Expose `pub(crate)` accessors on `HfsPlusFilesystem`: `catalog_data()`, `catalog_header()`, `vh()`. `read_fork_with_overflow` already exists.
- Until later phases land, `capture()` returns `Err(Unsupported(...))` if it sees:
  - xattrs (`vh.attributes_file.logical_size > 0`) — relaxed in Step 13
  - hardlinks (`(creator='hfs+', type='hlnk' | 'fdrp')` magic) — relaxed in Steps 16–17

**Tests:** Capture from a small synthesized HFS+ image with no xattrs/hardlinks; assert file count, dir count, total fork bytes match VH counters.

### Step 11 — `hfsplus_fsck` skeleton

- [x] **Goal:** Lightweight fsck that becomes the verification harness for every clone-related test from now on.

**Files:** `src/fs/hfsplus_fsck.rs` (new), `src/fs/fsck.rs` (no changes — shared types already abstract)

**Changes:**
- 3 phases: VH parse + sanity, catalog walk (count files/dirs/threads, detect orphans), bitmap consistency (`sum(allocated_bits) == total_blocks - free_blocks`).
- Returns the existing shared `FsckResult` so the GUI Check button works without changes.
- Wire up `Filesystem::fsck` for `HfsPlusFilesystem` to delegate.

**Tests:** Run against the blanks from Step 9 (clean), against a corrupted blank (force `vh.file_count = 999`), assert appropriate `FsckIssue`s.

---

## Phase 5 — Extended attributes B-tree write side

Lands before the clone is enabled in user-facing code so xattrs survive the round-trip.

### Step 12 — Attribute B-tree read parity

- [x] **Goal:** Parsed in-memory representation of the attributes B-tree alongside the existing extents-overflow buffer, plus a `list_xattrs(cnid)` accessor.

**Files:** `src/fs/hfsplus.rs`

**Changes:**
- Lazy-load `attributes_data: Option<Vec<u8>>` the same way `extents_overflow_data` is loaded, gated on `vh.attributes_file.logical_size > 0`.
- Add `pub(crate) fn list_xattrs(&mut self, cnid: u32) -> Result<Vec<XattrRecord>>` using `walk_leaf_records`.
- `XattrRecord` enum: `Inline(Vec<u8>)`, `Fork(ForkData)`, `Extents(Vec<ExtentDescriptor>)`.
- Read inline xattr payloads (the common case for Finder Info / `com.apple.*` keys); decode fork-style on demand only when explicitly read.

**Tests:** Synthesize a volume with a known `com.apple.FinderInfo` 32-byte inline xattr, assert `list_xattrs` round-trips it.

### Step 13 — Insert / delete xattr records

- [x] **Goal:** `set_xattr(cnid, name, value)` and `remove_xattr(cnid, name)` mutate the attribute B-tree under the snapshot/rollback guard from Step 5.

**Files:** `src/fs/hfsplus.rs`

**Changes:**
- `insert_xattr_record` / `remove_xattr_record` mirror the catalog-side insert/remove, using `btree_split_leaf_with_insert` for splits.
- `delete_entry` now removes every attribute record for the deleted CNID (loop on `list_xattrs`); the Step 4 refusal of xattr-bearing deletes can be relaxed to "delete works through xattrs".
- `do_sync_metadata` writes `attributes_data` back via `write_fork_data`.
- New error: `FilesystemError::XattrTooLarge` if a fork-style payload would exceed the attributes file capacity.
- `snapshot()` / `restore_snapshot` already capture `attributes_data` (added in Step 5).

**Tests:** Create file → set 3 xattrs → sync → reopen → all 3 round-trip → delete file → all 3 records gone from the attributes tree.

### Step 14 — Capture xattrs through the snapshot

- [x] **Goal:** `SourceFileSnapshot` and `SourceDirSnapshot` carry xattrs; capture no longer refuses xattr-bearing volumes.

**Files:** `src/fs/hfsplus_clone.rs`

**Changes:**
- Add `xattrs: Vec<(String, XattrRecord)>` to `SourceFileSnapshot` and `SourceDirSnapshot`, populated in `capture()`.
- Remove the `Unsupported(xattrs)` early-out from Step 10.

**Tests:** Capture from a synthesized HFS+ image with decmpfs xattrs, assert all xattrs land in the snapshot byte-equal.

---

## Phase 6 — Hardlinks

### Step 15 — Detect + read hardlinks

- [x] **Goal:** `list_directory` returns hardlinks as regular files/dirs whose `target_cnid` points at the inode; `read_file` follows the indirection.

**Files:** `src/fs/hfsplus.rs`

**Changes:**
- During catalog parsing, identify hardlinks by `(type, creator)` magic. Resolve `iNodeNum` (BSD `bsdInfo.special` u32) → real CNID lookup against the cached `HFS+ Private Data` / `.HFS+ Private Directory Data` listings (loaded lazily once per session).
- `FileEntry` gains `link_target_cnid: Option<u64>`; existing `location` continues to address the catalog row so deletion targets the link itself, not the inode.
- `read_file` / `write_file_to`: if `link_target_cnid.is_some()`, read forks from the inode CNID instead.

**Tests:** Synthesize an HFS+ image with one hardlinked file (two catalog entries → one inode), assert both list entries report identical bytes.

### Step 16 — Hardlink-aware delete + ref-counting

- [x] **Goal:** Deleting a hardlink decrements the inode's link count; deleting the last reference frees the inode.

**Files:** `src/fs/hfsplus.rs`

**Changes:**
- `delete_entry` on a hardlink: remove the catalog row, decrement BSD `linkCount`. If new count == 0, free the inode's forks and remove the `iNodeNNNN` entry from the private parent.
- Snapshot/rollback (Step 5) covers partial-failure cases; `attributes_data` already snapshotted (Step 5/13).

**Tests:** Two-link test: delete one, assert other still readable; delete second, assert inode entry gone and blocks freed.

### Step 17 — File hardlinks through capture + clone replay path

- [x] **Goal:** Capture preserves hardlink topology — N catalog rows pointing at one inode stay one inode in the snapshot. Clone replay (used in Step 21) emits each inode once.

**Files:** `src/fs/hfsplus_clone.rs`, `src/fs/hfsplus.rs`

**Changes:**
- `SourceCatalogSnapshot::capture` builds an `inode_map: HashMap<source_inode_cnid, Vec<source_link_cnid>>`. First pass: capture all inodes as `SourceFileSnapshot { is_inode: true }` plus their fork bytes. Second pass: hardlink rows reference the captured inode by source CNID.
- Add `target.create_hardlink_inode(...)` (creates `iNodeNNNN` under target's private parent) and `target.create_hardlink(parent, name, inode_cnid)`.
- Remove the `Unsupported(hardlinks)` early-out from Step 10.

**Tests:** Source with one inode + 3 hardlinks → snapshot+replay onto a fresh blank → assert target has one inode + 3 catalog rows (not 4 independent files).

### Step 18 — Directory hardlink support (10.5+)

- [x] **Goal:** Same machinery as 15–17 but for `(type='fdrp', creator='MACS')` directory hardlinks under `.HFS+ Private Directory Data\r`. (Plan typo: actual creator is `MACS`, per TN1150 / Darwin xnu.)

**Files:** `src/fs/hfsplus.rs`, `src/fs/hfsplus_clone.rs`

**Changes:**
- Parallel resolution path for directory hardlinks. Most Time Machine source images use these; ignoring them silently corrupts the catalog graph.
- Capture path: same dedup-on-capture pattern; the inode is a *directory* whose valence and child catalog entries snapshot normally.

**Tests:** Synthesized Time-Machine-shaped image with a directory hardlink; capture + replay; verify both link rows resolve to the same target directory contents.

---

## Phase 7 — HFSX comparison + UI

The signature/keyCompareType piece landed in Step 9. These two steps cover the runtime comparison and surfacing.

### Step 19 — Case-sensitive duplicate detection

- [x] **Goal:** `find_catalog_record(parent, name)` and the `AlreadyExists` check in `create_file` / `create_directory` honor the volume's case sensitivity.

**Files:** `src/fs/hfsplus.rs`

**Changes:**
- Add `HfsPlusFilesystem::case_sensitive() -> bool` (`vh.is_hfsx()` AND the case-sensitive attribute bit).
- `find_catalog_record` already does NFD comparison; extend it to also case-fold when `!case_sensitive()`. On HFSX, the comparison is binary NFD.
- `validate_hfsplus_create_name` is unchanged.

**Tests:** Create `Foo` and `foo` in the same parent on an HFSX volume — both succeed. On HFS+, the second fails with `AlreadyExists`.

### Step 20 — HFSX in min-size / clone reporting

- [x] **Goal:** UI strings and `metadata.json` distinguish HFS+ from HFSX where it matters.

**Files:** `src/fs/mod.rs` (`fs_name_for`), `src/gui/inspect_tab.rs`, `src/backup/metadata.rs`

**Changes:**
- Verify the inspect column shows "HFSX" not "HFS+" for HFSX partitions.
- `metadata.json` records the source signature so restore can warn if the user picks a target volume of the wrong kind.

**Tests:** Inspect-tab manual check on a known HFSX image.

---

## Phase 8 — Defrag clone + backup integration

By this point: snapshot capture, blank builder, fsck, xattrs, and hardlinks all work. Now we wire them together into a full clone and surface the user-visible feature.

### Step 21 — `clone_hfsplus_volume`

- [x] **Goal:** `pub fn clone_hfsplus_volume<R, W>(source: &mut HfsPlusFilesystem<R>, target: &mut HfsPlusFilesystem<W>) -> Result<CloneReport>` BFS-replays the source onto a freshly built target — including xattrs, file hardlinks, dir hardlinks, dates, and HFSX case sensitivity.

**Files:** `src/fs/hfsplus_clone.rs`

**Changes:**
- BFS from root (CNID 2). For each dir snapshot: `target.create_directory(parent_remapped, name, options)`, record CNID remap.
- For each file: `target.create_file(parent_remapped, name, &mut data, data_len, options)`. Resource fork via `target.write_resource_fork`. Then `target.set_type_creator` and date setters (Step 8 must be in for dates to land).
- Replay xattrs via `target.set_xattr` (Step 13).
- Replay hardlinks via `target.create_hardlink_inode` + `target.create_hardlink` (Step 17/18).
- Source's HFSX case-sensitivity flag flows into the blank builder (Step 9).
- Remap any CNID-valued `vh.finder_info` slots (volume blessed folder, etc.).
- `target.sync_metadata()` once at the end.
- Verify with `hfsplus_fsck` (Step 11) before returning success.

**Tests:** Clone a 32 MiB synthetic source containing ~30 files including a resource-fork-bearing file, a 12-extent fragmented file, an xattr-bearing file, and a hardlink pair. Assert byte-equal data forks, matching catalog metadata, surviving xattrs, preserved hardlink topology, and `fsck` clean on the target.

### Step 22 — Streamed defrag-clone backup integration

Step 21's `clone_hfsplus_volume` requires random-access target writes (B-tree splits, fork placement). Routing that through a multi-GB temp file and then re-streaming into the compressor would double the disk I/O on real-world disks. Step 22 instead builds a peer **streamed two-pass** engine that emits a forward-only image directly into the compressor — no temp file, ~½ the I/O. The user-facing toggle is opt-in (default off), framed as "Shrink partitions to minimum size" so the value proposition (smaller restore size) is named.

`clone_hfsplus_volume` stays in tree because it remains the cleanest engine for in-place editing tests and ad-hoc tooling. The streamed path is a separate function, not a refactor of Step 21.

**Pre-flight rejections** (per partition, before any bytes flow):
- **Embedded HFS+ inside an HFS wrapper** — `resolve_apple_hfs` reports `("hfsplus", embedded_offset != partition_offset)`. Cloning the inner volume to a flat HFS+ would silently drop the wrapper.
- **Dirty journaled volume** — `kHFSVolumeJournaledBit (0x2000)` set with `kHFSVolumeUnmountedBit (0x100)` clear. The journal carries pending metadata changes the catalog hasn't absorbed; cloning the catalog as-is would lose them.

A cleanly-unmounted journaled volume is *accepted* — the journal is empty/replayed, and the cloned target is built without the journaled bit, effectively nullifying the journal. Pre-flight failure aborts the whole backup with an explanatory error.

#### Step 22a — Pre-flight detection

- [x] **Goal:** `fs::can_defrag_clone_hfsplus(reader, partition_offset) -> Result<(), String>` returns the human-readable reason a partition can't be defrag-cloned, or `Ok(())` when it can.

**Files:** `src/fs/mod.rs`, `src/fs/hfsplus.rs`

**Changes:**
- Promote `HFSPLUS_VOLUME_JOURNALED_BIT` and `HFSPLUS_VOLUME_UNMOUNTED_BIT` to `pub(crate)`.
- Add `can_defrag_clone_hfsplus`. Branches on `resolve_apple_hfs` for the wrapper check, then reads `vh.attributes` at offset 1024+4 for the journal-state check.

**Tests:** Four unit tests in `hfsplus.rs::tests` (accept clean, accept clean-journaled, refuse dirty-journaled, refuse non-HFS+). Embedded-HFS+ refusal exists in code but isn't unit-tested — synthesizing an HFS-wrapped image is involved; integration coverage lands with 22f.

#### Step 22b — Defrag layout planner

- [x] **Goal:** Pure-planning function that, given a `SourceCatalogSnapshot` plus a target size, returns a `TargetLayoutPlan` describing every block placement on the target.

**Files:** `src/fs/hfsplus_defrag.rs` (new)

**Changes:**
- `TargetLayoutPlan` records: per-file contiguous data/resource fork extents in the user-data area, source→target CNID remap (sequential from CNID 16), reserved region sizes (boot/VH, bitmap, catalog file, extents-overflow file, attributes file).
- Catalog/extents/attributes file sizes derive from source values with a small safety margin (1.5× for catalog).
- Hardlinks: each inode is laid out once under HFS+ Private Data; stubs reference the inode CNID.
- Pure function, no I/O — testable by snapshot fixtures.

**Tests:** Plan a small fragmented synthetic source, assert per-file extents are contiguous and packed; CNID remap is sequential; reserved region sizes match expected.

#### Step 22c — Defrag B-tree builder

- [x] **Goal:** From a `TargetLayoutPlan` + `SourceCatalogSnapshot`, build the in-memory metadata image: catalog B-tree bytes, extents-overflow bytes (likely empty since we pack contiguously), attributes B-tree bytes, allocation bitmap, and the `HfsPlusVolumeHeader` for the target.

**Status: basic shell landed.** Directories and ordinary files emit. Hardlinks (file + dir) and xattrs trip an explicit `Unsupported` early-out for now — landing them is a follow-up sub-step. Reusable `hfs_common::btree_insert_full` was added so xattr inserts share the catalog insert/split machinery.

#### Step 22c-i — Hardlink emit (file + directory)

- [x] **Goal:** `build_target_metadata` emits hardlink topology: each file inode under `\0\0\0\0HFS+ Private Data` once, with N catalog stub rows pointing at it via `bsdInfo.special = inode_num`; same for directory inodes under `.HFS+ Private Directory Data\r`.

**Files:** `src/fs/hfsplus_defrag.rs`

**Changes:**
- Drop the `snapshot.hardlinks.is_empty() && snapshot.dir_hardlinks.is_empty()` early-out in `build_target_metadata`.
- For each file in `snapshot.files` with `is_inode = true`: emit as a regular file under the target's HFS+ Private Data dir (the planner already issues it a target CNID; the inode lives at `(target_private_dir_cnid, "iNode<inode_num>")`).
- For each directory in `snapshot.dirs` with `is_dir_inode = true`: emit as a regular folder under the target's `.HFS+ Private Directory Data\r` dir.
- For each `SourceHardlinkSnapshot`: emit a file record with `FInfo = ('hlnk', 'hfs+')`, `bsdInfo.special = inode_num`, empty forks, the source's per-record metadata.
- For each `SourceDirHardlinkSnapshot`: emit a file record (yes, file — directory hardlinks are encoded as FILE records on disk per TN1150) with `FInfo = ('fdrp', 'MACS')`, `bsdInfo.special = inode_num`, empty forks.
- Bump the inode's `bsdInfo.special` (linkCount) to the number of stubs that reference it.
- Ensure the target's two private dirs exist in the snapshot before stub emission — they're regular `SourceDirSnapshot` rows on the source so they flow through the existing dir replay path; just need to make sure the BFS order places them before stub emission.

**Tests:**
- Two-stubs-one-inode round-trip: source with `iNode17` + 2 hardlink rows pointing at it. Build metadata, stitch into target image (with the inode's data fork present), open as `HfsPlusFilesystem`, assert both stub paths read identical bytes via the existing hardlink resolution path.
- Same for a directory hardlink (one inode under `.HFS+ Private Directory Data\r` plus 2 stub rows).

#### Step 22c-ii — Xattr emit

- [x] **Goal:** `build_target_metadata` emits inline extended attributes onto every captured CNID's remapped target CNID, populating the target's attributes B-tree.

**Files:** `src/fs/hfsplus_defrag.rs`, possibly `src/fs/hfsplus.rs` (if the existing inline-xattr record builder isn't already exposed)

**Changes:**
- Drop the `xattrs.is_empty()` early-out and the `plan.attributes_blocks > 0` refusal in `build_target_metadata`.
- Initialize the target's attributes B-tree buffer when `plan.attributes_blocks > 0` (mirrors the catalog/extents init via `init_btree_buffer` with the attribute-record `max_key_len`).
- For each `(cnid, xattr)` from snapshot files / dirs / hardlinks / dir-hardlinks (whichever land in 22c-i): build an inline attr record (`XattrKind::Inline` only — fork-style xattrs trip a controlled error per Step 13's existing limitation) keyed by `(target_cnid, startBlock=0, name)`, insert via `hfs_common::btree_insert_full`.
- `TargetMetadata.attributes` becomes `Some(buf)` whenever any record was inserted; populate VH `attributes_file` ForkData accordingly.
- The planner already sizes `attributes_blocks` from the source's `vh.attributes_file.total_blocks`, so capacity is on hand.

**Tests:**
- Source with one `com.apple.FinderInfo` 32-byte inline xattr on a regular file; build + stitch + open; `list_xattrs(target_cnid)` returns the same bytes.
- Source with a fork-style xattr: builder surfaces a clear error referencing Step 13's inline-only limitation rather than silently dropping.

**Files:** `src/fs/hfsplus_defrag.rs`

**Changes:**
- Reuses `hfs_common::btree_split_leaf_with_insert`, `btree_alloc_node`, and the existing xattr-record builders against in-memory `Vec<u8>` backing buffers.
- Catalog records carry new CNIDs and new fork extents; xattrs replayed onto remapped CNIDs.
- Bitmap bits set for all reserved + allocated user blocks.
- VH gets the source's volume metadata (label, dates, finder info — with CNID remap on slots that match a known source CNID, slots 6/7 zeroed for fresh Finder volume ID).
- Memory bounded by source metadata size — typically hundreds of MB max on multi-GB volumes.

**Tests:** Build metadata for the planner's fixture, open the resulting bytes (with a separate stub for fork content) as `HfsPlusFilesystem`, walk the catalog, verify byte-equal records.

#### Step 22d — Streaming emit engine

- [x] **Goal:** `pub fn stream_defragmented_hfsplus<R, W>(source: &mut HfsPlusFilesystem<R>, target_size: u64, dst: &mut W) -> Result<DefragReport>` drives the planner + builder, then forward-only emits into `dst`.

**Files:** `src/fs/hfsplus_defrag.rs`

**Changes:**
- Order: boot+VH region → bitmap → catalog file → extents-overflow file → attributes file → user files (in target-extent block order, streamed via `fork_stream_reader`) → zero padding → alt VH (512 B) + 512 B zero pad.
- Single forward sweep; the only seek operations on `dst` happen if `W: Seek` is needed for alt-VH placement, but block-order emission obviates that.
- `DefragReport` mirrors `HfsPlusCloneReport` (files copied, dirs copied, fork bytes, xattrs, hardlinks).

**Tests:** Round-trip in 22e.

#### Step 22e — Streamed defrag round-trip test

- [x] **Goal:** End-to-end engine test.

**Files:** `src/fs/hfsplus_defrag.rs`

**Changes:** Build a synthetic source HFS+ with a nested directory tree, a file carrying both data + resource forks, and a hardlink pair. Stream into a `Vec<u8>`. Open that Vec as `HfsPlusFilesystem`, walk catalog, verify byte-equal fork content, preserved hardlink topology (both link rows resolve to the same bytes via `link_target_cnid`), `fsck` clean. Xattr round-trip is covered by `build_metadata_emits_inline_xattr` in 22c-ii — `create_blank_hfsplus` doesn't seed an attributes B-tree, so a live-source xattr in this fixture would require a separate seed builder.

#### Step 22f — Backup pipeline integration

Sub-split: engine plumbing + streaming pipe in 22f-i; restore-side handling + end-to-end backup→restore round-trip in 22f-ii.

##### Step 22f-i — Engine plumbing + streaming pipe

- [x] **Goal:** Backup engine gains a `shrink_to_minimum: bool` toggle; when set, HFS+/HFSX partitions route through `stream_defragmented_hfsplus` into the compressor instead of `CompactHfsPlusReader`.

**Files:** `src/backup/mod.rs`, `src/backup/metadata.rs`, `src/gui/backup_tab.rs` (default `false`)

**Changes:**
- `BackupConfig.shrink_to_minimum: bool`. Logged-and-ignored when `sector_by_sector` is on or single-file-CHD is in play.
- After `analyze_partitions`, for each HFS+/HFSX partition with a `defragmented_min_sizes[i]`, run `can_defrag_clone_hfsplus` pre-flight; on failure abort the whole backup with the human-readable reason.
- Override sizing for those partitions: `effective_sizes[i] = stream_sizes[i] = defragmented_min_sizes[i]`, `is_layout_preserving_flags[i] = false`. `compact_sizes[i]` is left as-is — the per-partition loop dispatches on `clone_target_sizes` first, so neither the compacted nor the trim branch runs for cloned partitions.
- New `channel_pipe()` helper (`ChannelPipeReader` / `ChannelPipeWriter`) — a bounded `sync_channel(4)` adapter so the producer thread (`stream_defragmented_hfsplus`) feeds the `compress_partition` consumer with backpressure but no temp file.
- Per-partition loop: when `clone_target_sizes[i].is_some()`, spawn a producer thread that opens a fresh `HfsPlusFilesystem` over a cloned source `File` handle and streams into the pipe; the main thread runs `compress_partition` against the reader. Errors from either side propagate via `JoinHandle.join()`.
- `PartitionMetadata.defragmented_clone: bool` (`#[serde(default, skip_serializing_if = std::ops::Not::not)]`); set to `true` in metadata for clone partitions.

**Tests:** Inline `pipe_tests` cover the `ChannelPipe` round-trip + EOF-on-writer-drop behaviors and `Send` bounds. The end-to-end backup-pipeline round-trip lands with 22f-ii — fabricating an MBR-wrapped HFS+ disk image and exercising `run_backup` belongs with the restore-side work since both halves want to share the same fixture.

##### Step 22f-ii — Restore handling + end-to-end round-trip

- [ ] **Goal:** A clone-backup partition restores at the chosen size with the streamed bytes verbatim; "Original size" restore zero-pads the tail (the cloned HFS+ inside is smaller than the partition window — Mac OS still mounts it). Volume-grow on restore is a separate feature; not in scope here.

**Files:** `src/restore/mod.rs`

**Changes:**
- Branch on `pm.defragmented_clone`: skip filesystem-aware resize; write the decompressed image bytes verbatim into the partition window and zero-pad the tail when the chosen partition size exceeds `imaged_size_bytes`.
- "Minimum" size picker prefers `defragmented_min_size_bytes` when present (already wired by Step 3 / Step 20).

**Tests:** End-to-end: build a synthesized MBR + HFS+ disk image with xattrs and hardlinks, run `run_backup` with `shrink_to_minimum=true`, then `run_restore` at "minimum" size, reopen via `HfsPlusFilesystem::open`, assert data + xattrs + hardlinks intact, fsck clean. The fixture is also reusable for the GUI smoke test in 22g.

#### Step 22g — Backup-tab toggle

- [ ] **Goal:** Surface the option in the GUI.

**Files:** `src/gui/backup_tab.rs`

**Changes:**
- Checkbox: "Shrink partitions to minimum size". Tooltip: `"Repacks fragmented HFS+ volumes during backup so the restore can target a smaller partition. Other filesystems are unaffected. Requires a clean (non-journaled or cleanly-unmounted) HFS+ volume — embedded HFS+ wrappers and dirty journaled volumes will abort the backup."`
- Disabled when `sector_by_sector` is on.

### Step 23 — Log polish

- [ ] **Goal:** The user understands what happened.

**Files:** `src/backup/mod.rs`

**Changes:**
- Log lines (ASCII only):
  - `"Streaming defragmented HFS+ for partition N: source 57.9 GiB -> 38.2 GiB"`
  - `"Defrag emit: 12,345 files / 678 folders / 38.1 GiB user data / 4 hardlinks / 89 xattrs"`
  - `"Aborting backup: partition N: <reason from can_defrag_clone_hfsplus>"`

**Tests:** Manual `cargo run` on the user's reference disks; verify both partitions stream-defrag cleanly and the resulting backup restores to a smaller target.

---

## Phase 9 — HFS+ journal: transactional writes + history viewer

By this point edit mode and defrag-clone work on cleanly-unmounted journaled volumes (the journal is already empty so we ignore it and rebuild the target without the journaled bit). What's still missing: writing **in place** to a journaled volume without breaking it, and recovering a *dirty* journaled volume that we'd otherwise have to refuse.

The journal is a physical-block redo log stored inside the volume (or, rarely, in an external journal partition). The on-disk layout is documented in **Apple TN1150 §"Journaled HFS Plus"** and in the Apple Open Source `xnu` tree (`bsd/hfs/hfs_journal.{c,h}`, also surfaced in `apple-oss-distributions/hfs`). Apple's `diskdev_cmds` / `fsck_hfs` is the cleanest reference for replay logic; the Linux kernel `fs/hfsplus/` driver does **not** implement the journal and is intentionally not a model.

The phase splits into three concerns: read-only parse + replay (steps 24–25), transactional writes (steps 26–27), and tooling (steps 28–29). Each step is sized for one session.

### Step 24 — Journal on-disk parse

- [ ] **Goal:** Library-only ability to locate, parse, and walk the journal without applying any state. No behavior change in edit mode or backup.

**Files:** `src/fs/hfsplus_journal.rs` (new), `src/fs/hfsplus.rs` (accessor wiring), `src/fs/mod.rs` (module wire-up)

**Changes:**
- Parse `JournalInfoBlock` from the disk block at `vh.journal_info_block` (512 B; fields: `flags`, `device_signature[8]`, `offset`, `size`, `ext_jnl_uuid[36]`, `machine_serial_num[48]`, padding). Bits we care about in `flags`: `kJIJournalInFSMask (0x1)` (in-volume journal — common case), `kJIJournalNeedInitMask (0x2)`, `kJIJournalOnOtherDeviceMask (0x4)` (treat as "unsupported, refuse").
- Parse `journal_header` at `journal_info.offset` (variable size, normally 4096 B; fields: `magic`, `endian`, `start`, `end`, `size`, `blhdr_size`, `checksum`, `jhdr_size`, `sequence_num`). Magic `0x4a4e4c78` (`'JNLx'`) native vs. byte-swapped `'xLNJ'` distinguishes endianness; honor it across every field for the rest of the parse.
- `JournalWalker::new(reader, jib, jh)` and `walker.next() -> Option<Result<TransactionView<'_>>>` walks `start → end` (with wraparound at `size`) returning `block_list_header { max_blocks, num_blocks, bytes_used, checksum, flags, sequence_num }` plus a slice of `block_info { bnum, bsize, next }` and an iterator of `(target_lba, &[u8])` block payloads. Verify each transaction's Adler-style checksum; surface mismatch as `JournalError::CorruptTransaction { sequence_num }`.
- `pub(crate) fn read_journal(&mut self) -> Result<Option<JournalState>>` on `HfsPlusFilesystem` returns `None` when the journaled bit is clear, `Some(JournalState { info, header, transactions: Vec<TransactionSummary> })` otherwise. `TransactionSummary` carries sequence number, block count, total bytes, and the target LBA range — enough for the history viewer.
- All structs `Clone + Debug`. No mutation.

**Tests:**
- Synthetic JIB + 4 KiB journal header + two hand-built transactions, parse via walker, assert sequence numbers + checksum verification on both ends of the byte order.
- Truncated header / bad magic / out-of-range `end` each produce a clear `JournalError`.

### Step 25 — Journal replay + dirty-volume recovery

- [ ] **Goal:** A dirty journaled volume can be opened by first replaying the journal in place. After replay the on-disk catalog matches what macOS would see post-mount, and `start == end` so subsequent writes start from a known good state.

**Files:** `src/fs/hfsplus_journal.rs`, `src/fs/hfsplus.rs`, `src/fs/mod.rs`

**Changes:**
- `pub fn replay_journal<RW: Read + Write + Seek>(disk: &mut RW, jib: &JournalInfoBlock, jh: &mut JournalHeader, partition_offset: u64) -> Result<ReplayReport>`. For each transaction in `start → end`:
  1. Verify the per-transaction checksum; on failure stop replay at this transaction (Apple's behavior — partial replay up to the last good record).
  2. For each `block_info`, write the payload bytes to `partition_offset + bnum * 512`. Honor the `next == ~0` end-of-list sentinel and skip `bsize == 0` "phantom" entries.
  3. After all blocks land, fsync, then advance `jh.start` past this transaction and rewrite the header. This makes replay idempotent — a crash mid-replay leaves the volume recoverable on the next attempt.
- `ReplayReport` carries transactions applied, blocks written, bytes written, last sequence number, plus the per-transaction summaries (re-used by the history viewer in Step 29).
- Replay does **not** clear the journaled bit — the volume remains journaled. The catalog is just current.
- `open_editable_filesystem` for HFS+ gains a `replay_dirty_journal: bool` flag (plumbed from a new `BackupConfig` / GUI option in 29). When `true` and the journal is dirty, replay before proceeding. When `false`, refuse with the existing Step 4 message.
- Refuse external-journal volumes (`kJIJournalOnOtherDeviceMask`) with a distinct error — out of scope for this phase.

**Tests:**
- Build a synthetic dirty journaled HFS+ image (one transaction containing a catalog node update); replay; reopen; assert the catalog reflects the journaled change and `jh.start == jh.end`.
- Crash-resume test: replay first transaction only (truncate `end` mid-list), reopen, replay again, assert no double-apply.
- Corrupt-checksum test: second transaction has bad checksum; replay applies the first, stops, returns `ReplayReport.partial = true` with the count.

### Step 26 — Transaction recorder

- [ ] **Goal:** A `TransactionBuilder` that buffers dirty 512-byte blocks during a high-level mutation (`create_file`, `delete_entry`, etc.) and emits one journal transaction at sync time.

**Files:** `src/fs/hfsplus_journal.rs`, `src/fs/hfsplus.rs`

**Changes:**
- `TransactionBuilder { dirty: BTreeMap<u64 /* lba */, Vec<u8>>, sequence_num: u64 }`. `record_block(lba, bytes)` overwrites; `record_range(lba, bytes)` splits into 512-byte chunks. Idempotent — last write wins, same semantics as Apple's `journal_modify_block_start/end`.
- `commit<RW: Read + Write + Seek>(self, disk, jib, jh) -> Result<()>`:
  1. Build a `block_list_header` referencing every `(lba, 512)` pair plus the trailing `next = ~0` sentinel and the dirty payloads.
  2. Compute the Adler-style transaction checksum exactly the way `journal_open` / `end_transaction` do in xnu (the same routine, applied to header + block_info array with the checksum field zeroed).
  3. Append to journal at `jh.end`, wrapping at `jh.size` (split write if it straddles the wrap point); fsync.
  4. Advance `jh.end` and `jh.sequence_num`; rewrite the header; fsync.
  5. Apply each `(lba, payload)` to its real disk location; fsync.
  6. Advance `jh.start` to `jh.end`; rewrite the header. (Crash between 4 and 6 is safe — replay re-applies; crash before 4 is safe — transaction never committed.)
- `HfsPlusFilesystem` gains `pending_tx: Option<TransactionBuilder>`. New helper `with_transaction(|builder| ...)` opens one for the duration of a mutation.
- Snapshot/rollback (Step 5) interaction: rollback discards the builder without committing; no journal entry is written for failed mutations.

**Tests:**
- Unit-test `TransactionBuilder` against a `Cursor<Vec<u8>>` standing in for the journal area: build 3 dirty blocks, commit, walk via `JournalWalker` from Step 24, assert byte-equal payloads + correct checksum.
- Wraparound test: journal `size = 8 KiB`, `end = 7 KiB`, transaction is 2 KiB — assert it splits across the wrap and replays correctly.
- Forced-failure between phases (4 / 5 / 6) leaves the volume in a state Step 25 can recover from.

### Step 27 — Wire mutations through the recorder; relax Step 4

- [ ] **Goal:** Every `EditableFilesystem` mutation path on `HfsPlusFilesystem` records its dirty blocks into a transaction. The Step 4 journaled-volume refusal becomes a soft warning (or disappears entirely) when `replay_dirty_journal=true` and the recorder is active.

**Files:** `src/fs/hfsplus.rs` (every site that currently writes to `disk` via `do_sync_metadata` and friends)

**Changes:**
- `do_sync_metadata` becomes the natural transaction boundary. Every block-level write inside it (`write_fork_data`, B-tree node writes, bitmap writes, VH + alt-VH) goes through `pending_tx.record_*` instead of writing to disk directly.
- After in-memory mutations succeed, `pending_tx.commit(...)` runs once at the end of `do_sync_metadata`; a failure rolls back via the Step 5 snapshot AND skips the commit, so the journal stays untouched.
- Touch-date stamping (Step 8) and counter updates ride inside the transaction.
- `open_editable_filesystem` for HFS+: when `vh.attributes & kHFSVolumeJournaledBit != 0`:
  - If clean (`kHFSVolumeUnmountedBit` set) — proceed; transactional writes preserve journaling.
  - If dirty and `replay_dirty_journal` is on — call `replay_journal` first, then proceed.
  - If dirty and not authorized — keep refusing with the Step 4 message.
- Multi-megabyte mutations (e.g. defrag-clone in place) are still routed through `clone_hfsplus_volume` writing to a **fresh** target with no journal; that's unchanged. Only edit-mode mutations get journaled.

**Tests:**
- End-to-end edit on a clean journaled HFS+ image: create a file, `do_sync_metadata`, reopen with macOS-style replay, assert the file is present and the journal is empty.
- End-to-end edit on a dirty journaled image with `replay_dirty_journal=true`: replay applies pending transaction, then our own transaction lands on top, both visible after a second replay-and-reopen.
- Snapshot rollback during a journaled mutation: forced `DiskFull` — assert the journal is byte-identical to before the mutation (no partial transaction written).

### Step 28 — fsck.hfsplus journal phase

- [ ] **Goal:** Existing `hfsplus_fsck` (Step 11) gains a journal-consistency phase so the GUI Check button reports journal corruption instead of silently passing it over.

**Files:** `src/fs/hfsplus_fsck.rs`, `src/fs/fsck.rs` (only if a new `FsckIssue` code is needed)

**Changes:**
- New phase 4 (journal): if journaled bit set, read JIB + header; assert `start ≤ end ≤ size`, `magic` matches `endian`, `jhdr_size` is sane; walk transactions via `JournalWalker`, recording `JournalChecksumMismatch { sequence_num }` and `JournalSequenceJump { from, to }` issues. Non-fatal (does not flip `clean = false` for the volume itself, but surfaces in the issue list).
- `FsckStats.extra` gains `journal_transactions_pending`, `journal_bytes_pending`.

**Tests:** Synthetic dirty journal with one good + one corrupt transaction; assert exactly one `JournalChecksumMismatch` issue.

### Step 29 — Journal history viewer (GUI)

- [ ] **Goal:** A read-only "Journal" view in `BrowseView` shows the parsed transaction log so the user can see what edit-mode (or macOS) wrote, in order, with enough detail to troubleshoot.

**Files:** `src/gui/browse_view.rs`, new `src/gui/journal_view.rs`, `src/gui/inspect_tab.rs`

**Changes:**
- New "Journal" button in the browse-view toolbar, enabled when `vh.attributes & kHFSVolumeJournaledBit` is set. Opens `JournalView` over a fresh read-only `HfsPlusFilesystem`.
- Top section: volume header summary — journal offset/size, current `start` / `end` (with wraparound indicator), `sequence_num`, in-FS vs. external, dirty-state interpretation.
- Transaction list (newest first): each row shows sequence number, block count, total bytes, target-LBA range (`min..max`), and a one-line classification of which subsystem(s) the writes touched (catalog / extents / attributes / bitmap / VH / fork data — derived by mapping `bnum * 512` against the source's `vh.*_file` extents in memory).
- Selecting a row reveals the per-block detail: `(lba, bsize, classification, hex preview of first 64 B)`. Helpful when diagnosing "why did macOS replay rewrite my catalog node 47?" — you can read the bytes that landed there.
- Pre-flight warnings rendered inline: "Transaction #N has bad checksum — macOS will stop replay here", etc. (uses Step 28's issue types).
- ASCII only (CLAUDE.md rule): use `OK`, `Warning`, `->`, no emoji or arrows beyond `->`.
- Same view also exposes "what we're about to write" once Step 27 is in: when the user is in edit mode with staged edits, render the *pending* transaction the same way before they hit Apply Edits. This is the single most useful troubleshooting payoff.

**Tests:** GUI is not unit-tested; verify by running `cargo run` against a synthetic dirty journaled image and against one of the user's reference macOS HFS+ partitions in read-only mode.

---

## Out of scope (file separately if needed)

- **Compression (decmpfs).** Cloned files are stored uncompressed; macOS will retain them as-is. Re-applying decmpfs is a separate feature.
- **External journal partitions.** Step 25 refuses `kJIJournalOnOtherDeviceMask` volumes. Vanishingly rare in retro-Mac contexts; lift only if a real-world disk surfaces.
- **Live-mount safety.** Everything in Phase 9 assumes the volume is offline (we own the file). Concurrent macOS access is out of scope.

---

## Verification matrix per step

| # | Phase | `cargo build --all-targets` | `cargo test --lib` | New unit tests added | GUI tested manually |
|---|---|---|---|---|---|
| 1 | min size | required | required | yes (synthetic 100 MiB) | no |
| 2 | min size | required | required | n/a | yes (column + tooltip) |
| 3 | min size | required | required | yes (metadata round-trip) | no |
| 4 | safety | required | required | yes (open refuses journaled) | yes (toast) |
| 5 | writes | required | required | yes (snapshot rollback) | no |
| 6 | writes | required | required | yes (fragmented alloc) | no |
| 7 | writes | required | required | yes (12-extent round-trip) | no |
| 8 | writes | required | required | yes (date stamping) | no |
| 9 | blank | required | required | yes (HFS+ and HFSX blanks) | no |
| 10 | capture | required | required | yes (capture counts) | no |
| 11 | fsck | required | required | yes (clean + corrupted) | yes (Check button) |
| 12 | xattr | required | required | yes (xattr round-trip) | no |
| 13 | xattr | required | required | yes (insert/delete) | no |
| 14 | xattr | required | required | yes (capture preserves xattrs) | no |
| 15 | hardlink | required | required | yes (read both link rows) | no |
| 16 | hardlink | required | required | yes (linkcount decrement) | no |
| 17 | hardlink | required | required | yes (capture dedupes inode) | no |
| 18 | hardlink | required | required | yes (TM-shaped image) | no |
| 19 | HFSX | required | required | yes (Foo/foo coexist) | no |
| 20 | HFSX | required | required | n/a | yes (inspect column) |
| 21 | clone | required | required | yes (xattrs+hardlinks survive) | no |
| 22a | preflight | required | required | yes (4 accept/refuse cases) | no |
| 22b | planner | required | required | yes (fixture-driven) | no |
| 22c | builder | required | required | yes (in-memory parse round-trip) | no |
| 22c-i | hardlinks | required | required | yes (file + dir hardlink replay) | no |
| 22c-ii | xattrs | required | required | yes (inline xattr replay) | no |
| 22d | emit | required | required | yes (covered by 22e) | no |
| 22e | round-trip | required | required | yes (e2e engine) | no |
| 22f-i | backup engine | required | required | yes (channel pipe) | no |
| 22f-ii | backup restore | required | required | yes (e2e backup+restore) | yes (full flow) |
| 22g | gui | required | required | n/a | yes (toggle) |
| 23 | polish | required | required | n/a | yes (reference disks) |
| 24 | journal | required | required | yes (parse + endian) | no |
| 25 | journal | required | required | yes (replay + idempotent + corrupt) | no |
| 26 | journal | required | required | yes (recorder + wraparound) | no |
| 27 | journal | required | required | yes (e2e edit on journaled vol) | yes (edit-mode toast) |
| 28 | journal | required | required | yes (fsck journal phase) | yes (Check button) |
| 29 | journal | required | required | n/a | yes (history view) |

---

## Reference disks for manual validation

- The user's two reference HFS+ partitions on `/dev/disk4` (`Apple_HFS (HFS+)` at LBA 263968 and 121929464) — fragmented, ~58 GiB each, 4 KiB block size. Defrag should drop to ~49 GiB and ~38 GiB respectively. Modern macOS volumes with xattrs and likely hardlinks — the steps that gate behind those features (Phases 5 + 6) must be in before the user-visible defrag toggle (Step 23).
- `~/Documents/HD10_512 Quadra_System7OldNew.hda` — classic HFS, used as a regression baseline that nothing in this plan should disturb.
