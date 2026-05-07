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

- [ ] **Goal:** Lightweight fsck that becomes the verification harness for every clone-related test from now on.

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

- [ ] **Goal:** Parsed in-memory representation of the attributes B-tree alongside the existing extents-overflow buffer, plus a `list_xattrs(cnid)` accessor.

**Files:** `src/fs/hfsplus.rs`

**Changes:**
- Lazy-load `attributes_data: Option<Vec<u8>>` the same way `extents_overflow_data` is loaded, gated on `vh.attributes_file.logical_size > 0`.
- Add `pub(crate) fn list_xattrs(&mut self, cnid: u32) -> Result<Vec<XattrRecord>>` using `walk_leaf_records`.
- `XattrRecord` enum: `Inline(Vec<u8>)`, `Fork(ForkData)`, `Extents(Vec<ExtentDescriptor>)`.
- Read inline xattr payloads (the common case for Finder Info / `com.apple.*` keys); decode fork-style on demand only when explicitly read.

**Tests:** Synthesize a volume with a known `com.apple.FinderInfo` 32-byte inline xattr, assert `list_xattrs` round-trips it.

### Step 13 — Insert / delete xattr records

- [ ] **Goal:** `set_xattr(cnid, name, value)` and `remove_xattr(cnid, name)` mutate the attribute B-tree under the snapshot/rollback guard from Step 5.

**Files:** `src/fs/hfsplus.rs`

**Changes:**
- `insert_xattr_record` / `remove_xattr_record` mirror the catalog-side insert/remove, using `btree_split_leaf_with_insert` for splits.
- `delete_entry` now removes every attribute record for the deleted CNID (loop on `list_xattrs`); the Step 4 refusal of xattr-bearing deletes can be relaxed to "delete works through xattrs".
- `do_sync_metadata` writes `attributes_data` back via `write_fork_data`.
- New error: `FilesystemError::XattrTooLarge` if a fork-style payload would exceed the attributes file capacity.
- `snapshot()` / `restore_snapshot` already capture `attributes_data` (added in Step 5).

**Tests:** Create file → set 3 xattrs → sync → reopen → all 3 round-trip → delete file → all 3 records gone from the attributes tree.

### Step 14 — Capture xattrs through the snapshot

- [ ] **Goal:** `SourceFileSnapshot` and `SourceDirSnapshot` carry xattrs; capture no longer refuses xattr-bearing volumes.

**Files:** `src/fs/hfsplus_clone.rs`

**Changes:**
- Add `xattrs: Vec<(String, XattrRecord)>` to `SourceFileSnapshot` and `SourceDirSnapshot`, populated in `capture()`.
- Remove the `Unsupported(xattrs)` early-out from Step 10.

**Tests:** Capture from a synthesized HFS+ image with decmpfs xattrs, assert all xattrs land in the snapshot byte-equal.

---

## Phase 6 — Hardlinks

### Step 15 — Detect + read hardlinks

- [ ] **Goal:** `list_directory` returns hardlinks as regular files/dirs whose `target_cnid` points at the inode; `read_file` follows the indirection.

**Files:** `src/fs/hfsplus.rs`

**Changes:**
- During catalog parsing, identify hardlinks by `(type, creator)` magic. Resolve `iNodeNum` (BSD `bsdInfo.special` u32) → real CNID lookup against the cached `HFS+ Private Data` / `.HFS+ Private Directory Data` listings (loaded lazily once per session).
- `FileEntry` gains `link_target_cnid: Option<u64>`; existing `location` continues to address the catalog row so deletion targets the link itself, not the inode.
- `read_file` / `write_file_to`: if `link_target_cnid.is_some()`, read forks from the inode CNID instead.

**Tests:** Synthesize an HFS+ image with one hardlinked file (two catalog entries → one inode), assert both list entries report identical bytes.

### Step 16 — Hardlink-aware delete + ref-counting

- [ ] **Goal:** Deleting a hardlink decrements the inode's link count; deleting the last reference frees the inode.

**Files:** `src/fs/hfsplus.rs`

**Changes:**
- `delete_entry` on a hardlink: remove the catalog row, decrement BSD `linkCount`. If new count == 0, free the inode's forks and remove the `iNodeNNNN` entry from the private parent.
- Snapshot/rollback (Step 5) covers partial-failure cases; `attributes_data` already snapshotted (Step 5/13).

**Tests:** Two-link test: delete one, assert other still readable; delete second, assert inode entry gone and blocks freed.

### Step 17 — File hardlinks through capture + clone replay path

- [ ] **Goal:** Capture preserves hardlink topology — N catalog rows pointing at one inode stay one inode in the snapshot. Clone replay (used in Step 21) emits each inode once.

**Files:** `src/fs/hfsplus_clone.rs`, `src/fs/hfsplus.rs`

**Changes:**
- `SourceCatalogSnapshot::capture` builds an `inode_map: HashMap<source_inode_cnid, Vec<source_link_cnid>>`. First pass: capture all inodes as `SourceFileSnapshot { is_inode: true }` plus their fork bytes. Second pass: hardlink rows reference the captured inode by source CNID.
- Add `target.create_hardlink_inode(...)` (creates `iNodeNNNN` under target's private parent) and `target.create_hardlink(parent, name, inode_cnid)`.
- Remove the `Unsupported(hardlinks)` early-out from Step 10.

**Tests:** Source with one inode + 3 hardlinks → snapshot+replay onto a fresh blank → assert target has one inode + 3 catalog rows (not 4 independent files).

### Step 18 — Directory hardlink support (10.5+)

- [ ] **Goal:** Same machinery as 15–17 but for `(type='fdrp', creator='hfs+')` directory hardlinks under `.HFS+ Private Directory Data`.

**Files:** `src/fs/hfsplus.rs`, `src/fs/hfsplus_clone.rs`

**Changes:**
- Parallel resolution path for directory hardlinks. Most Time Machine source images use these; ignoring them silently corrupts the catalog graph.
- Capture path: same dedup-on-capture pattern; the inode is a *directory* whose valence and child catalog entries snapshot normally.

**Tests:** Synthesized Time-Machine-shaped image with a directory hardlink; capture + replay; verify both link rows resolve to the same target directory contents.

---

## Phase 7 — HFSX comparison + UI

The signature/keyCompareType piece landed in Step 9. These two steps cover the runtime comparison and surfacing.

### Step 19 — Case-sensitive duplicate detection

- [ ] **Goal:** `find_catalog_record(parent, name)` and the `AlreadyExists` check in `create_file` / `create_directory` honor the volume's case sensitivity.

**Files:** `src/fs/hfsplus.rs`

**Changes:**
- Add `HfsPlusFilesystem::case_sensitive() -> bool` (`vh.is_hfsx()` AND the case-sensitive attribute bit).
- `find_catalog_record` already does NFD comparison; extend it to also case-fold when `!case_sensitive()`. On HFSX, the comparison is binary NFD.
- `validate_hfsplus_create_name` is unchanged.

**Tests:** Create `Foo` and `foo` in the same parent on an HFSX volume — both succeed. On HFS+, the second fails with `AlreadyExists`.

### Step 20 — HFSX in min-size / clone reporting

- [ ] **Goal:** UI strings and `metadata.json` distinguish HFS+ from HFSX where it matters.

**Files:** `src/fs/mod.rs` (`fs_name_for`), `src/gui/inspect_tab.rs`, `src/backup/metadata.rs`

**Changes:**
- Verify the inspect column shows "HFSX" not "HFS+" for HFSX partitions.
- `metadata.json` records the source signature so restore can warn if the user picks a target volume of the wrong kind.

**Tests:** Inspect-tab manual check on a known HFSX image.

---

## Phase 8 — Defrag clone + backup integration

By this point: snapshot capture, blank builder, fsck, xattrs, and hardlinks all work. Now we wire them together into a full clone and surface the user-visible feature.

### Step 21 — `clone_hfsplus_volume`

- [ ] **Goal:** `pub fn clone_hfsplus_volume<R, W>(source: &mut HfsPlusFilesystem<R>, target: &mut HfsPlusFilesystem<W>) -> Result<CloneReport>` BFS-replays the source onto a freshly built target — including xattrs, file hardlinks, dir hardlinks, dates, and HFSX case sensitivity.

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

### Step 22 — Backup-mode integration

- [ ] **Goal:** Backup pipeline gains a per-partition "Defragment HFS+" option; when set, the backup writes a packed clone instead of the layout-preserving stream.

**Files:** `src/model/backup_runner.rs` (or wherever the orchestrator lives — confirm via `grep -n run_backup src/backup/mod.rs`), `src/backup/sizes.rs`, `src/gui/backup_tab.rs`, `src/backup/metadata.rs`

**Changes:**
- New backup option struct field: `defragment_hfsplus_partitions: HashSet<usize>` (partition indices opted in).
- For opted-in partitions:
  1. Compute `defragmented_min_size_bytes` (Step 1).
  2. Open source HFS+ read-only; build target via `create_blank_hfsplus(defrag_size, source_block_size, source_name, source.is_hfsx())`.
  3. `clone_hfsplus_volume(source, target)`.
  4. `hfsplus_fsck(target)` — bail with `Err` if not clean.
  5. Hand the *target image bytes* (in a `Cursor<Vec<u8>>` or temp file) to the existing compactor + compressor.
  6. `metadata.json` records the original partition geometry plus a `cloned: true` flag and the original-volume checksum (so restore knows the source-byte image is *not* recoverable, only the data is).
- Restore-side: a clone-backup partition restores at the chosen size with the cloned bytes — no re-clone needed. If the user picks "original size" the restore zero-pads the tail.

**Tests:** End-to-end: backup a synthesized fragmented HFS+ with xattrs and hardlinks, restore at "minimum" size, mount via `HfsPlusFilesystem::open`, assert data + xattrs + hardlinks all intact.

### Step 23 — UI polish + log messages

- [ ] **Goal:** The user understands what happened.

**Files:** `src/gui/backup_tab.rs`, `src/gui/inspect_tab.rs`

**Changes:**
- Backup tab partition list: small "Defrag" toggle next to HFS+ rows only. Tooltip: `"Cloning packs data toward the start; restore size reflects only used data. The original byte layout is not preserved."`
- Log lines (ASCII only):
  - `"Defragmenting HFS+ partition N: source 57.9 GiB -> clone target 38.2 GiB"`
  - `"Cloned: 12,345 files / 678 folders / 38.1 GiB user data / 4 hardlinks / 89 xattrs"`
  - `"fsck OK on clone"` or `"ERROR: fsck on clone failed: ..."` followed by abort.
- Inspect-tab Min Size column tooltip references the backup-tab toggle.

**Tests:** Manual `cargo run` on the user's reference disks; verify both partitions report sensible numbers and the toggle path produces a working backup.

---

## Out of scope (file separately if needed)

- **Compression (decmpfs).** Cloned files are stored uncompressed; macOS will retain them as-is. Re-applying decmpfs is a separate feature.
- **Journal replay.** Currently we refuse journaled volumes (Step 4). Replay is non-trivial and deferred — once implemented, the Step 4 journaled-volume guard relaxes the same way the xattr guard does after Step 13.

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
| 22 | backup | required | required | yes (e2e backup+restore) | yes (full flow) |
| 23 | polish | required | required | n/a | yes (reference disks) |

---

## Reference disks for manual validation

- The user's two reference HFS+ partitions on `/dev/disk4` (`Apple_HFS (HFS+)` at LBA 263968 and 121929464) — fragmented, ~58 GiB each, 4 KiB block size. Defrag should drop to ~49 GiB and ~38 GiB respectively. Modern macOS volumes with xattrs and likely hardlinks — the steps that gate behind those features (Phases 5 + 6) must be in before the user-visible defrag toggle (Step 23).
- `~/Documents/HD10_512 Quadra_System7OldNew.hda` — classic HFS, used as a regression baseline that nothing in this plan should disturb.
