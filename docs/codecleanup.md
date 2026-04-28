# Code Cleanup Checklist

A working checklist of consolidation, dead-code, and module-placement cleanup opportunities found during a read-only survey of the codebase (~74k lines). Items are grouped by area; an overall priority list is at the bottom.

Conventions:
- `- [ ]` = open work item; check off as completed
- File:line references are starting points, not exact spans
- Each item carries an **Evidence** note (what was observed) and a **Suggested action**

> Note (GUI redesign): a major GUI overhaul is planned. Items in §5 and §10 are deliberately framed to make that redesign easier — pull model logic out of `src/gui/` first so the new GUI can be built against a stable, testable model layer.

> **Revisit-after-§5 list.** Several §2 items are intentionally parked — they need concrete model-layer consumers before the right trait shape is visible:
> - `MacFilesystem` sub-trait for `blessed_system_folder` / `set_blessed_folder`
> - `ProDosEditableFilesystem` sub-trait for `set_prodos_type`
> - Full `Filesystem` trait split into `FilesystemReader` / `FilesystemInspector` / `FilesystemRepair`
> - `fn capabilities() -> Capabilities` discovery method
>
> Suggested order: §3 → §4 → §7 (pure refactors) → §5 (model extraction) → return to §2 deferred items → §6 resize/validation strategies.

---

## 1. B-tree implementation duplication (HFS / HFS+)

- [x] **HFS+ still uses the older `btree_split_leaf`; HFS upgraded to `btree_split_leaf_with_insert`** (`src/fs/hfsplus.rs:777` vs `src/fs/hfs.rs:865`, helpers in `src/fs/hfs_common.rs:842` and `:978`)
  - **Evidence:** `btree_split_leaf` splits first then inserts in a separate step, which can leave undersized leaves; `btree_split_leaf_with_insert` merges both into an atomic byte-based split. HFS migrated after the "B-tree node full" bug noted in `MEMORY.md`; HFS+ did not.
  - **Done:** HFS+ `insert_catalog_record` now calls `btree_split_leaf_with_insert`. Old `btree_split_leaf` deleted from `hfs_common.rs`; its unit test ported to the new function. (HFS+'s incremental `btree_insert_into_index` / `btree_grow_root` parent-chain handling kept; only the leaf split changed.)

- [ ] **Catalog walking / key comparison patterns duplicated between HFS and HFS+** (`src/fs/hfs.rs`, `src/fs/hfsplus.rs`)
  - **Evidence:** Both walk a B-tree leaf chain doing key comparisons against `(parent_id, name)` tuples; only the key encoding differs.
  - **Suggested action:** Extract a generic `CatalogWalker<K: CatalogKey>` (or just shared helpers) into `hfs_common.rs`.

- [x] **`clump_size` field in HFS+ `ForkData` is parsed and serialized but never read** (`src/fs/hfsplus.rs:50`)
  - **Done:** Kept for VH round-trip symmetry; added a comment explaining why the field is unread but preserved. Removing it would force a magic zero-write at offset `[8..12]`, which is less clear than a named field.

- [x] ~~**`CATALOG_FOLDER_THREAD` constant unused in HFS+**~~ — **false positive.** Used at `hfsplus.rs:1666` (`build_thread_record(CATALOG_FOLDER_THREAD, parent_cnid, name)`).

- [x] ~~**`lookup_folder_name` in HFS+ has no callers**~~ — **false positive.** Called from the parent-chain resolver at `hfsplus.rs:1307` and `:1309`.

---

## 2. Filesystem trait surface

- [~] **Default `write_file_to` reads the entire file into RAM via `read_file(.., usize::MAX)`** (`src/fs/filesystem.rs:44`)
  - **Evidence:** No FS overrides this (audit's claim that HFS/HFS+/ext override was wrong — every implementation inherits the default). Risk is bounded in practice (vintage disk-image files are typically <100 MiB).
  - **Done (partial):** Added a doc comment + TODO at the trait method explaining the RAM behavior and what a future streaming override needs (a chunked `read_file_at(entry, offset, len)` API). Full streaming refactor deferred — would touch all 8 FS implementations and isn't blocking real workloads today.

- [x] **`set_permissions` was a silent no-op for most filesystems** (`src/fs/filesystem.rs:234`)
  - **Done:** Default now returns `Err(Unsupported)`. ext continues to override with the real implementation; other filesystems surface the unsupported state instead of silently dropping mode bits. Audit's note that ProDOS overrides was wrong — only ext does.
  - **Related gap:** ProDOS *does* have an access byte (`prodos.rs:602, 637, 664`) but no setter is exposed. Logged in [`TODO_missing_features.md`](TODO_missing_features.md) — it needs a ProDOS-specific method, not a Unix-mode shim.

- [x] **`set_type_creator`, `write_resource_fork`, `set_blessed_folder` were also silent no-ops**
  - **Done:** All three defaults now return `Err(Unsupported)`. HFS/HFS+ continue to override. UI gating in `gui/browse_view.rs` already restricts these StagedEdits to compatible filesystems, so no caller paths regress.

- [ ] **`blessed_system_folder` is Mac-only but lives on the base `Filesystem` trait** (`src/fs/filesystem.rs:71`)
  - **Suggested action:** Move to a `MacFilesystem` sub-trait (HFS, HFS+). **Deferred** until §5 model extraction lands and we can see how the GUI consumes capability info — a sub-trait split is invasive and trait-object boundaries make it awkward without that picture.

- [ ] **`set_prodos_type` is on `EditableFilesystem` but only ProDOS implements it** (`src/fs/filesystem.rs:250`, impl at `src/fs/prodos.rs:938`)
  - **Suggested action:** Move to a `ProDosEditableFilesystem` sub-trait, or expose via a `set_metadata(key, value)` shape. **Deferred** alongside the sub-trait split above.

- [ ] **`Filesystem` trait mixes read, inspection, repair, and Mac-specific concerns** (`src/fs/filesystem.rs:8`)
  - **Suggested action:** Consider splitting into `FilesystemReader` (browse), `FilesystemInspector` (sizes, last byte), `FilesystemRepair` (fsck), and feature-specific sub-traits. **Deferred until §5** — trait shape will be clearer once GUI consumers are simpler.

- [ ] **No capability discovery — callers must catch `Unsupported` at runtime**
  - **Suggested action:** Add `fn capabilities() -> Capabilities` returning a bitset (resource forks, type/creator, prodos types, unix perms, blessing, fsck, repair). GUI uses this to gate buttons instead of try/catch. **Deferred** — wait until §5 reveals concrete GUI gating sites.

---

## 3. Compact readers / compaction result types

- [x] **Each FS defines its own `Compact*Reader` with the same shape** (FAT, NTFS, exFAT, ext, btrfs, HFS, HFS+, ProDOS) — **evaluated, not pursued.**
  - **Evidence on review:** Truly shared state is small (`source`, `partition_offset`, `position`, `total_size`, `cluster_buf`) — ~40 bytes per struct. The interesting code is each filesystem's region/cluster layout in its `Read` impl, which is genuinely different (FAT scans boot→FAT→root_dir→data; NTFS is just boot+clusters; HFS/HFS+ are layout-preserving). A trait abstraction would obscure that without meaningful savings.
  - **Decision:** Keep separate per-FS readers. The audit's claim of high duplication overstated the case.

- [x] **Multiple equivalent result types: `CompactInfo` (FAT), `CompactExfatInfo`, `CompactNtfsInfo`** plus `CompactResult`
  - **Done:** All three legacy types deleted. FAT/NTFS/exFAT compact readers now return `CompactResult` directly. The dispatch in `fs/mod.rs::compact_partition_reader` collapsed from four 11-line reshuffle blocks to one-line passthroughs (~40 fewer lines). Re-export `CompactInfo` removed from `fs/mod.rs:31`.

- [x] **Three distinct sizes (`original_size`, `compacted_size`, `data_size`) are computed differently per FS**
  - **Done:** Added a "Compact reader sizing model" section to `src/fs/README.md` documenting the two reader styles (packed vs. layout-preserving) and the invariants each guarantees. Doc-comment on `CompactResult` now points at the README. New compact readers should add a unit test asserting their invariant.

---

## 4. Hidden-sector patching

- [x] **FAT, NTFS, exFAT hidden-sector patches share their preamble** (`src/fs/fat.rs`, `src/fs/ntfs.rs`, `src/fs/exfat.rs`)
  - **Done:** New `src/fs/patch.rs` exposes `read_boot_sector`, `patch_u32_le_in_buf` (returns `Some(old)` only when the field actually changes), and `write_sector_at`. FAT and NTFS patchers now share the read+detect+write-primary path; exFAT reuses `read_boot_sector` only (its u64@0x40 + 12-sector boot region + checksum recompute is genuinely unique). The audit's "one-line call" framing was overstated — backup-region writes (FAT32 sector-6 mirror, NTFS last-sector mirror, exFAT 12-sector backup region with checksum) differ enough that a single helper would obscure rather than simplify.

- [x] **HFS / HFS+ no-op `patch_*_hidden_sectors` stubs deleted**
  - **Done:** Removed the no-op functions and their re-exports in `src/fs/mod.rs`. HFS / HFS+ have no LBA-dependent VBR field; the dispatcher (next item) now skips them by construction.

- [x] **Dispatcher moved into `src/fs/mod.rs`**
  - **Done:** New `pub fn patch_hidden_sectors_for(file, partition_offset, start_lba, log_cb)` in `src/fs/mod.rs` calls FAT / NTFS / exFAT in sequence (each is a no-op on magic mismatch). All five call sites collapsed: `rbformats/mod.rs`, `rbformats/vhd.rs` (×2), `restore/mod.rs`, `restore/single.rs`, `partition/resize.rs`. Each previously inlined a 5-line ladder of per-FS calls.

---

## 5. Model logic embedded in GUI (high impact for redesign)

### `src/gui/browse_view.rs` — 4265 lines

- [~] **Staged-edit queue extracted (partial)**
  - **Done:** New `src/model/edit_queue.rs` (195 lines) owns the `StagedEdit` enum, the `resolve_dir_by_path` walk helper, and a pure `apply_edit(efs, &StagedEdit) -> Result<(), FilesystemError>` dispatch. `browse_view.rs::apply_staged_edits` shrank from ~165 lines to a 25-line view-side wrapper that opens the editable FS, replays the queue via `apply_edit`, syncs metadata, and refreshes its caches. Lib gains a new `pub mod model;` root.
  - **Still on `BrowseView`:** the `staged_edits: Vec<StagedEdit>` field itself plus the many query/inspection helpers (`pending_adds_for`, `pending_delete_for`, `find_pending_add_idx`, `resolved_hfs_type_creator`, etc.) that currently read directly from the vec. Folding those into an `EditQueue` model object is a separate (larger) iteration — the enum + apply alone are the cleanly-extractable kernel, and the view's UI logic is still tangled with the queue.
  - **`ArchiveEditContext`/`ArchiveEditProgress` not yet moved** — tracked under the next bullet.

- [x] **Archive extract/recompress orchestration extracted**
  - **Done:** New `src/model/archive_edit.rs` (243 lines) owns `ArchiveEditContext`, `ArchiveEditProgress`, `start_extract`, `start_compress`, and the shared `compute_file_checksum`. Each launcher spawns the worker thread and returns an `Arc<Mutex<ArchiveEditProgress>>` for the GUI to poll. The view's `start_archive_extract` (~60 lines) and `start_archive_compress` (~110 lines) collapsed to ~20 lines each — they now do the view-side state mutations (toggle edit mode, clear caches) and call the model. `poll_archive_edit` stays in the view since it heavily mutates view state on completion. Browse view: 4058 → 3851 lines.

- [ ] **`BrowseView` re-opens the filesystem on every operation** (per `MEMORY.md`)
  - **Evidence:** This is intentional today — but it suggests the view is doing model work it shouldn't.
  - **Suggested action:** A `BrowseSession` model object (in `src/model/`) can centralize "open editable FS, run op, sync, invalidate".

### `src/gui/inspect_tab.rs` — 3887 lines

- [ ] **Partition editor state → model** (inspect_tab.rs:117–250, 692–1164)
- [ ] **Export configuration & job orchestration → model** (inspect_tab.rs:1243–1929)
- [x] **Backup / Clonezilla loading + partition merging extracted**
  - **Done:** `src/model/backup_loader.rs` (360 lines) exposes `load_backup(folder)` which dispatches on whether the folder is a rusty-backup or Clonezilla image, plus the underlying `load_backup_metadata` and `load_clonezilla` parsers. Each returns a typed outcome (metadata + partition table + alignment + partitions + min sizes + warnings/info messages). The view's three load helpers (`load_backup_metadata`, `load_partitions_from_metadata`, `merge_logical_partitions_from_metadata`, `load_clonezilla_image`) collapsed into a thin `load_backup_metadata` wrapper plus two small `apply_*_outcome` helpers. `infer_fat_type_byte` moved to the model and is `pub`-imported by the one remaining view-side caller. inspect_tab.rs: 3858 → 3611 lines.
- [x] **Fsck/repair runner extracted**
  - **Done:** `src/model/fsck_runner.rs` (56 lines) exposes `run_fsck(path, offset, ptype, type_string) -> Result<Option<FsckResult>>` and `run_repair(...) -> Result<RepairReport>` — pure path-in / typed-result-out. inspect_tab.rs::run_fsck and run_repair_inspect collapsed from ~135 lines combined to ~75 lines (just path resolution + logging + state storage). The popup/confirm renderers stay on the view since they're pure UI.
  - **Suggested action (all four):** Establish `src/model/{partition_editor,export,backup_loader,fsck_runner}.rs`. Each owns its background thread + status struct; `inspect_tab.rs` becomes view + dispatch only. Goal: drop inspect_tab below ~2000 lines.

### Cross-cutting GUI

- [ ] **Per-tab `Status` structs (`ExportStatus`, `InspectStatus`, `CacheStatus`, `BlockCacheScan`) live inside GUI files**
  - **Suggested action:** Move to `src/model/` so the future GUI redesign can swap views without touching threading code.

- [ ] **State plumbed through long argument lists** (`devices`, `log`, `&mut self`, etc.)
  - **Suggested action:** Introduce per-tab context structs (`InspectContext`, `BrowseContext`) once model is extracted.

---

## 6. Resize / validation across filesystems

- [~] **Resize dispatcher consolidated; full strategy trait deferred**
  - **Done:** New `fs::resize_filesystem_for(file, offset, new_size_bytes, log_cb)` in `src/fs/mod.rs` blind-dispatches across FAT/NTFS/exFAT/HFS/HFS+/ext/btrfs (each is a no-op on magic mismatch). Two ladders in `rbformats/vhd.rs` (~45 lines + ~10 lines) collapsed to single calls — mirrors the `patch_hidden_sectors_for` pattern landed in §4.
  - **Not done:** A full `ResizeStrategy` trait with a shared read/patch/write helper. The shared boilerplate is genuinely thin (the read+magic-check is one helper away — see `fs/patch.rs`), and each FS's resize is structurally distinct: FAT walks BPB+FAT-tables; NTFS rewrites the boot-and-mirror VBR; HFS rewrites the MDB and alternate MDB; ext rewrites the superblock + group-descriptor table; btrfs the superblock; etc. A trait wrapping that wouldn't reduce code meaningfully — kept the standalone fns.

- [~] **Validate dispatcher consolidated; full ValidationContext deferred**
  - **Done:** New `restore::validate_filesystem_for(file, offset, fs_type, log_cb)` collapses the two near-identical match ladders in `src/restore/mod.rs` (~45 lines each → 3 lines each). It harmonizes the per-FS validators' heterogeneous return types (`Vec<String>` / `bool` / `()`) to `Result<()>`, since `restore` only cares about side-effect logging.
  - **Not done:** A full `ValidationContext` shared sink — each per-FS validator still returns its own shape, and a fully unified context would require migrating every call site (including any future diagnostic UI that *does* want the warnings list). Worth revisiting once the GUI gets a "validate filesystem" button that needs the structured results.

---

## 7. Clonezilla

- [x] **`block_cache.rs` (1461 lines) split into cache + scan**
  - **Done:** `scan_metadata` and the per-FS `identify_*_metadata_blocks` / `discover_*_directory_blocks` helpers moved into a new `src/clonezilla/metadata_scan.rs` (1063 lines). `block_cache.rs` is now 415 lines and contains only `PartcloneBlockCache`, `PartcloneBlockReader`, and `CacheState` (plus persistence). The audit's three-way split (`PartcloneDecompressor` / `PartcloneBlockStore` / `PartcloneBlockCache`) was rejected on read — the decompressor state is too tightly coupled to the cache's bitmap + on-demand reads to extract usefully; the natural fault line is "block storage" vs. "filesystem-aware metadata identification." Visibility on cache fields touched by scan was bumped to `pub(super)`.

- [x] ~~**`scan_metadata` parses partition metadata in parallel with `backup/metadata.rs`**~~ — **false positive on review.**
  - **Evidence:** `clonezilla/metadata_scan.rs::scan_metadata` walks the partclone *block stream* identifying filesystem metadata blocks (FAT table sectors, MFT records, ext GDT) for browsing. `backup/metadata.rs` defines the JSON schema for our own backup folders. They parse different inputs for different consumers — no real overlap.

- [x] **Clonezilla GPT routes through `partition::gpt::Gpt::parse`** — **already true.**
  - **Evidence:** `src/restore/mod.rs:1428` wraps `cz_image.gpt_primary_raw` in a `Cursor` and calls `Gpt::parse()` directly. CRC validation and serde round-trips share the same code path as native GPT restores. No additional work needed.

---

## 8. Per-filesystem audit (compact)

### FAT (`src/fs/fat.rs`, 3780 lines)
- [ ] Solid module organization (BPB → reader → editable → compact reader). No reorganization needed.
- [ ] Move `write_bpb` (line ~3259) into a shared helper if NTFS/exFAT can reuse it.

### NTFS (`src/fs/ntfs.rs`, 3276)
- [ ] Hidden-sector patcher consolidates with FAT/exFAT (§4).
- [ ] Audit MFT-record and index helpers for overlap with `unix_common/`.

### exFAT (`src/fs/exfat.rs`, 2411)
- [ ] Drop `CompactExfatInfo` in favor of unified type (§3).
- [ ] Hidden-sector patcher consolidates (§4).

### ext (`src/fs/ext.rs`, 5289 — largest single FS file)
- [ ] Inode handling overlaps with `unix_common/inode.rs`. Promote shared code there.
- [ ] Consider splitting per-revision code (ext2 vs ext3 vs ext4) into submodules under `src/fs/ext/`.
- [ ] Walk for any `eprintln!` debug calls; route through `log` crate.

### HFS (`src/fs/hfs.rs`, 4855) + `hfs_fsck.rs` (6588) + `hfs_common.rs` (1849) + `hfs_clone.rs` (1338)
- [ ] Largest area of recent change — confirm `hfs_common.rs` exposes everything `hfs_clone.rs` needs (`pub(crate)` items have crept in over time).
- [ ] Catalog walking / record-range helpers shared with HFS+ (§1).
- [ ] Audit debug `eprintln!` instances flagged in `hfs.rs:3068–3124` and replace with `log::debug!`.
- [ ] `hfs_fsck.rs` at 6588 lines is unwieldy — consider splitting per phase (mdb / btree / catalog / bitmap / repair) into submodules under `src/fs/hfs_fsck/`.

### HFS+ (`src/fs/hfsplus.rs`, 2607)
- [ ] Migrate to `btree_split_leaf_with_insert` (§1).
- [x] Dead-code sweep done: `clump_size` documented and kept; the other two were false positives.
- [ ] Currently lacks the editable surface HFS gained — confirm it's intentionally read-only or queue an edit-mode track.

### btrfs (`src/fs/btrfs.rs`, 1864)
- [ ] Mostly skeleton; when extending, lean on `unix_common/` for inode/bitmap.
- [ ] See `docs/TODO-BTRFS-TRIMMING.md` — fold those TODOs into this checklist or link them.

### ProDOS (`src/fs/prodos.rs`, 2087) + `prodos_types.rs` (421)
- [ ] Directory walking pattern resembles FAT/ext — candidate for shared walker.
- [ ] `set_prodos_type` trait method belongs on a sub-trait (§2).

---

## 9. rbformats / partition / backup miscellany

- [ ] **`rbformats/mod.rs` is 1712 lines and hosts cross-format orchestration**
  - **Suggested action:** Split: keep format dispatch + `reconstruct_disk_from_backup` here, move shared compress/decompress helpers to `src/rbformats/compress.rs`.

- [ ] **VHD export and `reconstruct_disk_from_backup` share scaffolding** (`src/rbformats/vhd.rs`, `src/rbformats/mod.rs`)
  - **Suggested action:** Confirm there's no duplicated layout/zero-fill code; if there is, factor out.

- [ ] **`partition/mod.rs` (935) vs `partition/mbr.rs` (880) vs `partition/gpt.rs` (943) vs `partition/apm.rs` (554)**
  - **Suggested action:** Audit `partition/mod.rs` for orchestration that should live elsewhere (alignment helpers belong in their own module; size override structs may belong in `restore`).

- [ ] **`backup/mod.rs` is 1064 lines — orchestration + size accounting + checksum logic**
  - **Suggested action:** Pull the size-accounting helpers (`effective_sizes`, `stream_sizes`, etc., per `MEMORY.md`) into `backup/sizes.rs`.

---

## 10. GUI redesign foundations

- [ ] **Establish `src/model/` as the single home for non-GUI state** (currently nothing under that name)
  - **Subdirs to seed:** `edit_queue.rs`, `archive_edit.rs`, `partition_editor.rs`, `export.rs`, `backup_loader.rs`, `fsck_runner.rs`, `extraction.rs`.
  - **Goal:** when the new GUI is built, every tab is a renderer over a model object.

- [ ] **Background-thread state structs (`*Status`, `*Progress`) move out of `gui/` into `model/`**

- [ ] **Decide on a progress / log channel pattern and apply consistently**
  - **Evidence:** Today some flows use `Arc<Mutex<Status>>`, others use callbacks, others use `LogPanel` borrows.
  - **Suggested action:** Pick one (most likely a typed `mpsc` channel + a status snapshot), document it in `docs/`, and migrate.

- [ ] **`src/gui/inspect_tab.rs` and `src/gui/restore_tab.rs` share partition-row rendering**
  - **Suggested action:** Extract a shared `partition_row_widget` so the redesign doesn't carry forward two copies.

---

## 11. Dead-code / housekeeping

- [ ] Remove `eprintln!` debug calls from production code paths (HFS compact reader at `src/fs/hfs.rs:3068–3124`; spot-check ext, btrfs).
- [ ] Audit `#[allow(dead_code)]` attributes — at minimum add comments saying *why* the field is preserved (e.g. round-trip fidelity).
- [ ] Confirm whether `examples/` binaries (`dump_root_dir_raw`, `make_blank_apm_hfs`, etc.) should stay in-tree or move to `tests/fixtures/`.
- [ ] Run `cargo +nightly udeps` (or `cargo machete`) to flag unused crate dependencies in `Cargo.toml`.
- [ ] Run `cargo clippy --all-targets -- -W clippy::dead_code -W clippy::unused_self` and triage.

---

## Priority overview

### High-value (do first — biggest leverage for the upcoming GUI redesign)
1. **§5 model extraction** — `browse_view.rs` and `inspect_tab.rs` lose ~30–40% of their length; the new GUI gets a stable model surface to build against.
2. **§3 compact-reader unification** — eight FSes collapse to one trait + helper.
3. **§4 hidden-sector patching consolidation** — three near-identical implementations → one helper, with stubs deleted.
4. **§7 split `clonezilla/block_cache.rs`** — 1461 → 3 × <500 line files.

### Medium-value (good engineering hygiene)
5. **§1 HFS+ B-tree migration** to `btree_split_leaf_with_insert`; delete the old path once unused.
6. **§2 trait surface refactor** — sub-traits for Mac / ProDOS / Unix-perms; capability discovery.
7. **§6 resize / validation helpers** — `ResizeStrategy` + `ValidationContext`.
8. **§8 ext + hfs_fsck submodule splits** — both files are large enough to benefit.

### Low-value (cosmetic / safe to defer)
9. **§1, §11 dead code removal** — `clump_size`, `CATALOG_FOLDER_THREAD`, `lookup_folder_name`, `eprintln!`s.
10. **§3 unify `CompactInfo` types** across FAT/exFAT/NTFS.
11. **§9 split `rbformats/mod.rs` and `backup/mod.rs`** if they grow further.

Estimated impact (rough): high-value items ≈ 2000 lines of refactor with ~15–20% net code reduction; medium-value adds another ~1000 lines of churn; low-value is small but tightens the codebase.
