# Code Cleanup Checklist

A working checklist of consolidation, dead-code, and module-placement cleanup opportunities found during a read-only survey of the codebase (~74k lines). Items are grouped by area; an overall priority list is at the bottom.

Conventions:
- `- [ ]` = open work item; check off as completed
- File:line references are starting points, not exact spans
- Each item carries an **Evidence** note (what was observed) and a **Suggested action**

> Note (GUI redesign): a major GUI overhaul is planned. Items in §5 and §10 are deliberately framed to make that redesign easier — pull model logic out of `src/gui/` first so the new GUI can be built against a stable, testable model layer.

> **Post-§5 verdict.** All four §2 items that were parked pending §5 (Mac/ProDOS sub-traits, full trait split, `capabilities()` bitset) were revisited and closed as "decided not to pursue" — the model-layer extraction in §5 didn't surface consumers that wanted any of those shapes. See §2 entries for rationale.

---

## 1. B-tree implementation duplication (HFS / HFS+)

- [x] **HFS+ still uses the older `btree_split_leaf`; HFS upgraded to `btree_split_leaf_with_insert`** (`src/fs/hfsplus.rs:777` vs `src/fs/hfs.rs:865`, helpers in `src/fs/hfs_common.rs:842` and `:978`)
  - **Evidence:** `btree_split_leaf` splits first then inserts in a separate step, which can leave undersized leaves; `btree_split_leaf_with_insert` merges both into an atomic byte-based split. HFS migrated after the "B-tree node full" bug noted in `MEMORY.md`; HFS+ did not.
  - **Done:** HFS+ `insert_catalog_record` now calls `btree_split_leaf_with_insert`. Old `btree_split_leaf` deleted from `hfs_common.rs`; its unit test ported to the new function. (HFS+'s incremental `btree_insert_into_index` / `btree_grow_root` parent-chain handling kept; only the leaf split changed.)

- [x] **Catalog walking / key comparison patterns duplicated between HFS and HFS+** (`src/fs/hfs.rs`, `src/fs/hfsplus.rs`)
  - **Done:** New `hfs_common::walk_leaf_records(catalog_data, first_leaf, node_size, visit) -> Option<T>` visits every record in a catalog leaf chain (skipping non-leaf nodes, with cycle detection and bounds checks), letting the visitor short-circuit by returning `Some(T)`. Replaced five hand-rolled leaf walks: HFS `find_catalog_record_by_name`, `update_parent_valence`, `find_file_record_offset_by_cnid`; HFS+ `find_catalog_record`, `update_parent_valence`, `find_file_by_id`. Each shrunk roughly 2× — the boilerplate (offset bounds, kind=-1 check, fLink chase, `btree_record_range` extraction) is now in one place. Key encoding differences (1-byte vs 2-byte `key_len`, Mac Roman vs UTF-16BE NFD) stay in the per-FS visitor closures, where they belong. Per-FS `catalog_compare` and `build_catalog_key` kept inline — they're tiny and the encoding-specific code reads more naturally there than behind a `CatalogKey` trait.

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

- [x] **`blessed_system_folder` Mac sub-trait considered and rejected**
  - **Decision:** §5 is now done; the model layer + GUI use `fs.blessed_system_folder()` directly (6 call sites in browse_view.rs) and the default `Err(Unsupported)` from §2's earlier work handles non-Mac filesystems cleanly. A `MacFilesystem` sub-trait would force downcasting at every call site without removing any code. Status quo is fine.

- [x] **`set_prodos_type` ProDOS sub-trait considered and rejected**
  - **Decision:** Same posture as the Mac sub-trait — the model layer (`edit_queue.rs:183`) calls `efs.set_prodos_type(...)` directly, with `Err(Unsupported)` from non-ProDOS filesystems. A `ProDosEditableFilesystem` sub-trait would force downcasting; a `set_metadata(key, value)` shape would be a less type-safe variant of the same call. Status quo is fine.

- [x] **`Filesystem` trait split (Reader / Inspector / Repair) considered and rejected**
  - **Decision:** §5 is now done and the model layer doesn't actually want a split surface — every consumer uses the full trait object. A split would require duplicating dispatch logic in `fs::open_filesystem` and giving every model module its own preferred slice. No payoff visible; trait stays unified.

- [x] **`fn capabilities() -> Capabilities` discovery considered and rejected**
  - **Decision:** GUI gates buttons by partition type byte (46 sites between `browse_view.rs` and `inspect_tab.rs` use helpers like `is_browsable_type`, `is_checkable_type`, `is_classic_hfs`). That's the existing pattern and it works without opening a filesystem first. A capability bitset would duplicate that information without simplifying any call site. Try/catch on `Err(Unsupported)` is fine for the few mutation paths where the FS is already open.

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

- [x] **Staged-edit queue extracted into `EditQueue`**
  - **Done (initial):** New `src/model/edit_queue.rs` owns the `StagedEdit` enum, the `resolve_dir_by_path` walk helper, and a pure `apply_edit(efs, &StagedEdit) -> Result<(), FilesystemError>` dispatch. `apply_staged_edits` shrank from ~165 lines to a 25-line wrapper.
  - **Done (follow-up):** Added an `EditQueue` model object (in the same file) wrapping the `Vec<StagedEdit>` plus all query/mutation helpers the view needs: `is_pending_delete`, `is_pending_add`, `pending_adds_for`, `remove_pending_add`, `pending_resource_fork_for`, `space_delta`, `resolved_hfs_type_creator`, `set_pending_hfs_override`, `replace_set_type_creator`, plus `set_pending_prodos_override` / `replace_set_prodos_type` for the parallel ProDOS path. `BrowseView::staged_edits` is now `EditQueue`, six private helper methods on the view (`is_pending_delete`, `is_pending_add`, `pending_adds_for`, `remove_pending_add`, `resolved_hfs_type_creator`, `pending_add_index`) and the inline `bytes_added/bytes_freed` summary + inline rsrc-badge `find_map` were deleted in favor of `EditQueue` calls. browse_view.rs: 3854 → 3645 lines (−209).

- [x] **Archive extract/recompress orchestration extracted**
  - **Done:** New `src/model/archive_edit.rs` (243 lines) owns `ArchiveEditContext`, `ArchiveEditProgress`, `start_extract`, `start_compress`, and the shared `compute_file_checksum`. Each launcher spawns the worker thread and returns an `Arc<Mutex<ArchiveEditProgress>>` for the GUI to poll. The view's `start_archive_extract` (~60 lines) and `start_archive_compress` (~110 lines) collapsed to ~20 lines each — they now do the view-side state mutations (toggle edit mode, clear caches) and call the model. `poll_archive_edit` stays in the view since it heavily mutates view state on completion. Browse view: 4058 → 3851 lines.

- [x] **`BrowseSession` extracted**
  - **Done:** New `src/model/browse_session.rs` (202 lines) owns the seven "where to open from" parameters (`source_path`, `partition_offset`, `partition_type`, `partition_type_string`, `preopen_file`, `partclone_cache`, `zstd_cache`) and exposes `open()` / `open_editable()`. Derives `Clone + Default` so background workers can be handed a session value. Replaces both the in-view `open_fs` / `open_editable_fs` methods and the free `create_filesystem` function. `BrowseView::launch_extraction` now clones a single `session` instead of seven fields, and `run_extraction` takes `&BrowseSession` instead of seven parameters (was `#[allow(clippy::too_many_arguments)]`). browse_view.rs: 3645 → 3432 lines (−213). Re-opening the filesystem per operation is still the pattern (intentional, see `MEMORY.md`); the session just makes the parameter bundle stop traveling separately.

### `src/gui/inspect_tab.rs` — 3887 lines

- [x] **Partition editor state → model**
  - **Done:** New `src/model/partition_editor.rs` (258 lines) owns `EditorEntry` + `PartitionEditor` with `entries`, `edits`, `errors`, `disk_size`, `status`, and the four `add_*` input fields. Methods `seed_from(&[PartitionInfo])`, `add_entry_from_inputs(is_mbr)`, `build_and_validate(&PartitionTable)`. `InspectTab`'s nine `editor_*` fields collapsed to `editor: PartitionEditor`; the local `EditorEntry` (47 lines) and the 142-line `build_and_validate_edits` body were deleted; `init_editor` shrank to two lines; the inline 40-line "Add" button block became one method call. inspect_tab.rs: 3611 → 3347 lines (−264). `apply_editor_changes` stays on the view since it's threaded through `LogPanel`.
- [x] **Export configuration & job orchestration → model**
  - **Done:** New `src/model/export_runner.rs` (~560 lines after fmt) owns `ExportStatus`, `ExportSizeChoice`, `PartitionExportConfig` (+ `effective_size`), and four entry points: `build_partition_configs`, `build_size_overrides`, `build_size_map`, plus three thread-spawning fns `start_clonezilla_whole_disk`, `start_native_whole_disk`, `start_per_partition` (the latter takes a `PerPartitionInputs` bundle to keep the call site readable). The 60-line `init_export_configs` body collapsed to a 6-line delegate; the 450-line `start_export` body collapsed to a ~95-line dialog + dispatch. inspect_tab.rs: 3347 → 2907 lines (−440). Three rusty_backup imports (`fs::fat::resize_fat_in_place`, `fs::hfs_max_growable_size`, `partition::PartitionSizeOverride`, `rbformats::vhd::build_vhd_footer`) plus `std::io::Write` are no longer needed in the view.
- [x] **Backup / Clonezilla loading + partition merging extracted**
  - **Done:** `src/model/backup_loader.rs` (360 lines) exposes `load_backup(folder)` which dispatches on whether the folder is a rusty-backup or Clonezilla image, plus the underlying `load_backup_metadata` and `load_clonezilla` parsers. Each returns a typed outcome (metadata + partition table + alignment + partitions + min sizes + warnings/info messages). The view's three load helpers (`load_backup_metadata`, `load_partitions_from_metadata`, `merge_logical_partitions_from_metadata`, `load_clonezilla_image`) collapsed into a thin `load_backup_metadata` wrapper plus two small `apply_*_outcome` helpers. `infer_fat_type_byte` moved to the model and is `pub`-imported by the one remaining view-side caller. inspect_tab.rs: 3858 → 3611 lines.
- [x] **Fsck/repair runner extracted**
  - **Done:** `src/model/fsck_runner.rs` (56 lines) exposes `run_fsck(path, offset, ptype, type_string) -> Result<Option<FsckResult>>` and `run_repair(...) -> Result<RepairReport>` — pure path-in / typed-result-out. inspect_tab.rs::run_fsck and run_repair_inspect collapsed from ~135 lines combined to ~75 lines (just path resolution + logging + state storage). The popup/confirm renderers stay on the view since they're pure UI.
  - **Suggested action (all four):** Establish `src/model/{partition_editor,export,backup_loader,fsck_runner}.rs`. Each owns its background thread + status struct; `inspect_tab.rs` becomes view + dispatch only. Goal: drop inspect_tab below ~2000 lines.

### Cross-cutting GUI

- [x] **Per-tab `Status` structs moved to `src/model/`**
  - **Done:** `ExportStatus` moved with the export-runner extraction above. The remaining four — `InspectStatus`, `CacheStatus`, `BlockCacheScan` (inspect_tab) and `VhdExportStatus` (backup_tab) — moved into `src/model/status.rs` (69 lines). All fields made `pub` so the views can construct them; no methods to move (these are pure shared-state structs polled from the GUI thread). inspect_tab.rs: 2907 → 2867 (−40); backup_tab.rs: 989 → 979 (−10).

- [x] **`TabContext` bundle replaces `(devices, log)` argument pairs**
  - **Done:** New `src/gui/context.rs` (28 lines) defines `TabContext<'a> { pub devices: &'a [DiskDevice], pub log: &'a mut LogPanel }` with a `new` constructor. Migrated all three tabs that follow the pattern: `InspectTab`, `BackupTab`, `RestoreTab`. ~33 method signatures simplified — methods that took both `devices: &[DiskDevice]` and `log: &mut LogPanel` (or just one) now take `ctx: &mut TabContext` (or `&TabContext` for read-only methods like `source_path`/`get_target_size`/`has_target`). `progress: &mut ProgressState` stays a separate parameter on the few methods that need it (Backup/Restore `show`/`poll_progress`), avoiding `Option<&mut _>` ergonomic pain. App's call sites in `gui/mod.rs` construct one `TabContext` per tab dispatch. inspect_tab.rs: 2867 → 2853 (−14); backup_tab.rs: 979 → 976 (−3); restore_tab.rs ≈ unchanged (−2 imports, sigs net out). Net code is roughly the same — the win is in argument-list ergonomics and giving the App a single bundle to construct, not raw line count. `BrowseView` was deliberately not converted: it doesn't take `devices` / `log`, so the bundle wouldn't help. The doc's "BrowseContext" suggestion turned out to not apply.

---

## 6. Resize / validation across filesystems

- [x] **Resize dispatcher consolidated; full strategy trait considered and rejected**
  - **Done:** `fs::resize_filesystem_for(file, offset, new_size_bytes, log_cb)` in `src/fs/mod.rs` blind-dispatches across FAT/NTFS/exFAT/HFS/HFS+/ext/btrfs (each is a no-op on magic mismatch). Two ladders in `rbformats/vhd.rs` (~45 lines + ~10 lines) collapsed to single calls — mirrors the `patch_hidden_sectors_for` pattern landed in §4.
  - **Decision:** A full `ResizeStrategy` trait with a shared read/patch/write helper is **not pursued.** The shared boilerplate is genuinely thin (the read+magic-check is already one helper away — see `fs/patch.rs`), and each FS's resize is structurally distinct: FAT walks BPB+FAT-tables; NTFS rewrites the boot-and-mirror VBR; HFS rewrites the MDB and alternate MDB; ext rewrites the superblock + group-descriptor table; btrfs the superblock; etc. A trait wrapping that wouldn't reduce code meaningfully. Standalone fns retained.

- [x] **Validate dispatcher consolidated; full ValidationContext considered and rejected**
  - **Done:** `restore::validate_filesystem_for(file, offset, fs_type, log_cb)` collapses the two near-identical match ladders in `src/restore/mod.rs` (~45 lines each → 3 lines each). It harmonizes the per-FS validators' heterogeneous return types (`Vec<String>` / `bool` / `()`) to `Result<()>`, since `restore` only cares about side-effect logging.
  - **Decision:** A full `ValidationContext` shared sink is **not pursued now.** Each per-FS validator still returns its own shape, and a fully unified context would require migrating every call site. There is no consumer today that needs the structured warnings list — `restore` only logs them. Revisit if/when a GUI "validate filesystem" surface lands that wants the typed results.

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
- [x] ~~Move `write_bpb` (line ~3259) into a shared helper if NTFS/exFAT can reuse it.~~ — not applicable.
  - **Evidence:** NTFS and exFAT VBRs have completely different layouts (NTFS = $Boot at LCN 0 with backup at last sector + FILE record signatures; exFAT = 12-sector boot region with checksum). Their write paths share no structure with FAT's BPB. The audit's "if NTFS/exFAT can reuse it" was speculative; the answer is no.

### NTFS (`src/fs/ntfs.rs`, 3276)
- [x] ~~Hidden-sector patcher consolidates with FAT/exFAT (§4).~~ — done as part of §4.
- [x] ~~Audit MFT-record and index helpers for overlap with `unix_common/`.~~ — no overlap.
  - **Evidence:** NTFS doesn't import `unix_common` at all. The shape is genuinely different — NTFS uses MFT records with security descriptors and `$STANDARD_INFORMATION` attributes; `unix_common` is POSIX-mode-bit + inode-shaped. There's no shared helper to extract.

### exFAT (`src/fs/exfat.rs`, 2411)
- [x] ~~Drop `CompactExfatInfo` in favor of unified type (§3).~~ — already gone.
  - **Evidence:** zero references to `CompactExfatInfo` in the codebase; the type was removed during the §3 compact-reader unification work.
- [x] ~~Hidden-sector patcher consolidates (§4).~~ — done as part of §4.

### ext (`src/fs/ext.rs`, 5289 — largest single FS file)
- [x] ~~Inode handling overlaps with `unix_common/inode.rs`. Promote shared code there.~~ — already done.
  - **Evidence:** `src/fs/ext.rs:17–21` imports from `unix_common::bitmap`, `unix_common::compact`, and `unix_common::inode` (`unix_entry_from_inode`, `unix_file_type`, `UnixFileType`). The shared inode helpers live in `unix_common/inode.rs` (412 lines) and ext is the active consumer.
- [x] ~~Consider splitting per-revision code (ext2 vs ext3 vs ext4) into submodules under `src/fs/ext/`.~~ — misframed; closed.
  - **Evidence:** ext2/3/4 are not separate codebases — they're feature bits on the same superblock (`has_extents`, `has_journal`, `is_64bit`). Per-version branches in `ext.rs` total 9 small `if self.has_extents`/`if self.is_64bit` checks; there's no per-revision code worth isolating. `ExtVersion` is just a display label.
  - **The real split available** is functional (read / edit / resize / validate / extent helpers) — viable but not a priority right now. Logged for future consideration if `ext.rs` grows further.
- [x] ~~Walk for any `eprintln!` debug calls; route through `log` crate.~~ — already clean.
  - **Evidence:** zero `eprintln!` instances in `src/fs/ext.rs` (also zero in `ntfs.rs` and `btrfs.rs`).

### HFS (`src/fs/hfs.rs`, 4855) + `hfs_fsck.rs` (6588) + `hfs_common.rs` (1849) + `hfs_clone.rs` (1338)
- [x] ~~Largest area of recent change — confirm `hfs_common.rs` exposes everything `hfs_clone.rs` needs (`pub(crate)` items have crept in over time).~~ — verified clean.
  - **Evidence:** `hfs_common.rs` has exactly one `pub(crate)` symbol; the rest are `pub` (intended public API) or `pub(super)`. The "items have crept in" worry hasn't materialized.
- [x] Catalog walking / record-range helpers shared with HFS+ (§1) — done via `hfs_common::walk_leaf_records`.
- [x] ~~Audit debug `eprintln!` instances flagged in `hfs.rs:3068–3124` and replace with `log::debug!`.~~ — false positive.
  - **Evidence:** all 17 `eprintln!`s in `src/fs/hfs.rs` (lines 3973–4074) are inside `#[cfg(test)]` blocks — manual `#[ignore]`-tagged real-image fsck tests that intentionally print to stderr. Production code is clean. The line range cited in the audit predated subsequent edits and no longer maps to anything.
- [x] `hfs_fsck.rs` fully split by phase.
  - `src/fs/hfs_fsck.rs` (was 6588 lines) is now `src/fs/hfs_fsck/mod.rs` (1916 lines, just orchestrators + tests + the `HfsFsckCode` enum) plus five phase submodules:
    - `mdb.rs` (260 lines) — Phase 1 MDB sanity.
    - `btree.rs` (1338 lines) — Phase 2 catalog B-tree structure checks + repairs, plus the shared btree primitives `record_key`, `extract_child_pointers`, `HFS_MAX_NRECS`.
    - `catalog.rs` (1147 lines) — Phase 3 catalog consistency checks + repairs (`CatalogKey`, `CatalogEntry`, `collect_catalog_entries`, `parse_catalog_record`, `check_directory_structure`, `check_catalog_consistency`, `repair_catalog_consistency`, `insert_thread_record`, `fix_thread_record`, `repair_thread_record_lengths`, `remove_thread_record`, `remove_record_by_key`, `fix_valence_mismatches`).
    - `bitmap.rs` (699 lines) — Phase 4 extent / allocation-bitmap cross-check + repairs.
    - `extents.rs` (1335 lines) — extents-overflow B-tree checks + repairs.
  - Cross-module wiring: `extents.rs` and `catalog.rs` import shared btree primitives via `super::btree::...`; `bitmap.rs` imports `CatalogEntry` / `collect_catalog_entries` from `super::catalog::...`; `catalog.rs` imports `validate_hfs_name` from `super::mdb::...`. `rebuild_index_nodes` is re-exported from mod.rs as `pub(crate)` so `hfs.rs` callers (`super::hfs_fsck::rebuild_index_nodes`) keep working unchanged. `hfs_issue`, `HfsFsckCode`, `CATALOG_DIR_THREAD`, `CATALOG_FILE_THREAD` stay in mod.rs as `pub(super)` shared helpers. All 61 fsck tests still pass.

### HFS+ (`src/fs/hfsplus.rs`, 2607)
- [x] ~~Migrate to `btree_split_leaf_with_insert` (§1).~~ — done.
  - **Evidence:** `src/fs/hfsplus.rs:13` imports `btree_split_leaf_with_insert` and line 782 calls it during catalog inserts.
- [x] Dead-code sweep done: `clump_size` documented and kept; the other two were false positives.
- [x] ~~Currently lacks the editable surface HFS gained — confirm it's intentionally read-only or queue an edit-mode track.~~ — not a refactor; logged as a missing feature.
  - **Decision:** HFS+ editable surface is a real product gap, but it's a feature add (1500–2500 lines mirroring the HFS edit-mode work in `docs/hfs_write_tweaks.md`), not cleanup. Tracked in [`TODO_missing_features.md`](TODO_missing_features.md) under "HFS+ — editable surface".

### btrfs (`src/fs/btrfs.rs`, 1864)
- [x] ~~Mostly skeleton; when extending, lean on `unix_common/` for inode/bitmap.~~ — already does.
  - **Evidence:** `src/fs/btrfs.rs:14–15` imports `unix_common::compact::{CompactLayout, CompactSection, CompactStreamReader}` and `unix_common::inode::unix_entry_from_inode`. Generic future-extension advice — no current action.
- [ ] See [`docs/TODO-BTRFS-TRIMMING.md`](TODO-BTRFS-TRIMMING.md) for the open btrfs compact-reader trimming work; that's its own track, not part of this cleanup pass.

### ProDOS (`src/fs/prodos.rs`, 2087) + `prodos_types.rs` (421)
- [x] ~~Directory walking pattern resembles FAT/ext — candidate for shared walker.~~ — rejected on inspection.
  - **Evidence:** FAT walks cluster chains + 32-byte MS-DOS directory entries; ext walks inode→block→linked dir entries; ProDOS walks block storage types (seedling/sapling/tree) with ProDOS-specific 4-byte entry headers. The shared abstraction is already the `Filesystem::list_directory` trait method; pushing it lower would require modeling three incompatible on-disk formats behind one walker. No payoff.
- [x] `set_prodos_type` trait method belongs on a sub-trait (§2) — closed in §2: sub-trait rejected.

---

## 9. rbformats / partition / backup miscellany

- [x] **`rbformats/mod.rs` split — compress/decompress dispatch extracted.**
  - **Done:** New `src/rbformats/compress.rs` (658 lines) owns `compress_partition`, `decompress_to_writer`, `decompress_partition_to_file`, `compress_file_to_archive`, plus the small shared helpers (`write_zeros`, `write_zeros_with_progress`, `is_all_zeros`, `output_path`, `file_name`, `SplitWriter`, `CHUNK_SIZE`). All eight tests covering these moved with them. mod.rs re-exports the public/`pub(crate)` items at the top of the file so existing call sites (`crate::rbformats::write_zeros`, `super::CHUNK_SIZE` from sibling format modules, etc.) continue to compile unchanged. mod.rs: 1703 → 1067 (−636); now hosts only `reconstruct_disk_from_backup`, `reconstruct_raw_apm_disk`, `detect_raw_apm`, image format detection (`ImageFormat`/`detect_image_format*`/`wrap_image_reader`/`SectionReader`), and the APM round-trip test.

- [x] ~~VHD export and `reconstruct_disk_from_backup` share scaffolding~~ — already shared.
  - **Evidence:** `src/rbformats/vhd.rs:8` imports `reconstruct_disk_from_backup, write_zeros, CHUNK_SIZE` from the parent module. All zero-fill and gap-handling in vhd.rs goes through `super::write_zeros`. No duplication to factor out.

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

- [x] **`eprintln!` debug calls migrated to the `log` crate.** HFS / HFS+ compact reader debug spew (`src/fs/hfs.rs:3068–3124`, `src/fs/hfsplus.rs:1907–1961`) now uses `log::debug!`. macOS DiskArbitration / authopen / sudo lifecycle messages (`src/os/macos.rs`, `src/os/macos/sudo.rs`) and the Linux startup elevation banners in `src/main.rs` use `log::info!` / `log::warn!`. WOZ partial-decode notice uses `log::warn!`. Remaining `eprintln!`s are inside `#[test]` blocks, which is appropriate.
- [x] **`#[allow(dead_code)]` audit — pattern-categorized rather than per-site annotated.** The 67 occurrences fall into three patterns: (a) on-disk format struct fields parsed for round-trip fidelity but not currently read (NTFS/btrfs/ext/exfat/woz/dmg headers), (b) on-disk constants kept for documentation (btrfs key types, file-type bytes), (c) GUI dialog state fields awaiting a feature consumer (`view_mode`, `EditorEntry`). Per-site comments would add 67 lines of noise without changing maintenance behavior. The per-file convention is implicit: structs in `src/fs/<fs>.rs` and `src/rbformats/<fmt>.rs` mirror their on-disk format and that fidelity has historically saved bug-hunt time.
- [x] **`example/` binaries decision: keep in-tree.** `example/README.md` already classifies them as "advanced debugging tests... mostly for testing internally how the application works." Only 3 of the 23 are registered in Cargo.toml as `[[example]]` entries (`probe_hfs_btree`, `test_clone_fix`, `fsck_bare`); the rest are unregistered scratch scripts that don't build automatically and can rot. They're not tests, so `tests/fixtures/` is wrong. Status quo is fine.
- [x] **`cargo machete` clean.** No unused crate dependencies in `Cargo.toml`.
- [~] **Clippy `dead_code` / `unused_self` triage — read, not actioned.** The lib-only run surfaces ~12 `unused_self` cases in FS modules; in each, `&self` is held for trait-shape consistency and removing it would clutter call sites. The bulk of clippy output is unrelated stylistic lints (`manual_div_ceil`, `manual_is_multiple_of`, `io::Error::other`) that belong to a separate housekeeping pass — fixable via `cargo clippy --fix` when desired.

---

## 12. Final pass — eliminate compilation warnings

- [ ] **Once all other refactor work is done, drive `cargo build --all-targets` to zero warnings.**
  - **Current state (refactor in progress):** `cargo build` (lib + bin) is already clean. `cargo build --all-targets` reports 14 warnings, mostly in `#[cfg(test)]` blocks and the `examples/` scratch binaries (unused imports, unused `mut`, unused variables, dead helpers like `build_empty_hfs_catalog`).
  - **Suggested action:** After landing the remaining real-work items in §8/§9/§10, do a single sweep: `cargo fix --lib --tests` for the auto-fixable ones, hand-edit the rest, and consider adding `#![deny(warnings)]` to the lib root or a CI check so regressions can't slip back in.
  - **Why last:** warnings churn during refactoring; cleaning them up before the refactor settles wastes work. Keep this as the final step before considering the cleanup pass complete.

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
