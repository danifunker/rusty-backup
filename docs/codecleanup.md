# Code Cleanup Checklist

A working checklist of consolidation, dead-code, and module-placement cleanup opportunities found during a read-only survey of the codebase (~74k lines). Items are grouped by area; an overall priority list is at the bottom.

Conventions:
- `- [ ]` = open work item; check off as completed
- File:line references are starting points, not exact spans
- Each item carries an **Evidence** note (what was observed) and a **Suggested action**

> Note (GUI redesign): a major GUI overhaul is planned. Items in §5 and §10 are deliberately framed to make that redesign easier — pull model logic out of `src/gui/` first so the new GUI can be built against a stable, testable model layer.

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

- [ ] **Default `write_file_to` reads the entire file into RAM via `read_file(.., usize::MAX)`** (`src/fs/filesystem.rs:44`)
  - **Evidence:** Only HFS, HFS+, and ext override. FAT/NTFS/exFAT/btrfs/ProDOS inherit the loading-everything default — a real risk for large extracted files. CLAUDE.md explicitly mandates streaming I/O.
  - **Suggested action:** Change the default to chunked streaming, or make `write_file_to` required and stream per-FS.

- [ ] **`set_permissions` is a silent no-op for most filesystems** (`src/fs/filesystem.rs:234`)
  - **Evidence:** Default returns `Ok(())`. Only ext and ProDOS do something meaningful — others silently drop the mode.
  - **Suggested action:** Either change the default to `Err(Unsupported)` (loud failure) or split into a `UnixEditableFilesystem` sub-trait.

- [ ] **`blessed_system_folder` is Mac-only but lives on the base `Filesystem` trait** (`src/fs/filesystem.rs:71`)
  - **Suggested action:** Move to a `MacFilesystem` sub-trait (HFS, HFS+).

- [ ] **`set_prodos_type` is on `EditableFilesystem` but only ProDOS implements it** (`src/fs/filesystem.rs:250`, impl at `src/fs/prodos.rs:938`)
  - **Suggested action:** Move to a `ProDosEditableFilesystem` sub-trait, or expose via a `set_metadata(key, value)` shape.

- [ ] **`Filesystem` trait mixes read, inspection, repair, and Mac-specific concerns** (`src/fs/filesystem.rs:8`)
  - **Suggested action:** Consider splitting into `FilesystemReader` (browse), `FilesystemInspector` (sizes, last byte), `FilesystemRepair` (fsck), and feature-specific sub-traits. Defer until §5 model extraction settles, since trait shape will be clearer once GUI consumers are simpler.

- [ ] **No capability discovery — callers must catch `Unsupported` at runtime**
  - **Suggested action:** Add `fn capabilities() -> Capabilities` returning a bitset (resource forks, type/creator, prodos types, unix perms, blessing, fsck, repair). GUI uses this to gate buttons instead of try/catch.

---

## 3. Compact readers / compaction result types

- [ ] **Each FS defines its own `Compact*Reader` with the same shape** (FAT, NTFS, exFAT, ext, btrfs, HFS, HFS+, ProDOS)
  - **Evidence:** All track phase + position + buffer and `impl Read`. No shared trait or scaffolding.
  - **Suggested action:** Introduce a `CompactReader` trait + `StreamingCompactState` helper for phase/position bookkeeping. Each FS supplies only the per-phase byte source.

- [ ] **Multiple equivalent result types: `CompactInfo` (FAT), `CompactExfatInfo`, `CompactNtfsInfo`** plus `CompactResult` in `rbformats`
  - **Suggested action:** Standardize on one (`CompactResult` from `rbformats/mod.rs` or hoisted to `fs/`). Delete the per-FS duplicates.

- [ ] **Three distinct sizes (`original_size`, `compacted_size`, `data_size`) are computed differently per FS**
  - **Evidence:** See `MEMORY.md` "CompactResult data_size vs compacted_size".
  - **Suggested action:** Document this distinction in `src/fs/README.md` and assert invariants in a shared helper rather than relying on each implementor to remember which is which.

---

## 4. Hidden-sector patching

- [ ] **FAT, NTFS, exFAT hidden-sector patches are nearly identical** (`src/fs/fat.rs:3208`, `src/fs/ntfs.rs:2823`, `src/fs/exfat.rs:1901`)
  - **Evidence:** All seek to BPB/VBR offset `0x1C..0x20`, write a u32 LE.
  - **Suggested action:** Single `patch_le_u32_at(file, sector_offset, field_offset, value)` helper in a new `fs/patch.rs`; the per-FS patcher becomes a one-line call.

- [ ] **HFS / HFS+ have no-op `patch_*_hidden_sectors` stubs** (`src/fs/hfs.rs:3401`, `src/fs/hfsplus.rs:2116`)
  - **Suggested action:** Either delete (and let `rbformats/mod.rs` skip them) or document why they're stubs.

- [ ] **`rbformats/mod.rs` orchestrates per-FS patching but the call list lives outside `fs/`**
  - **Suggested action:** Move the dispatcher into `src/fs/mod.rs` next to `compact_partition_reader()` and `effective_partition_size()`; `rbformats` should just call one `patch_hidden_sectors_for(parts, file)`.

---

## 5. Model logic embedded in GUI (high impact for redesign)

### `src/gui/browse_view.rs` — 4265 lines

- [ ] **Extract the staged-edit queue into a model module** (browse_view.rs:23–67, ~2337)
  - **Evidence:** `StagedEdit`, `ArchiveEditContext`, `ArchiveEditProgress`, `apply_staged_edits()` are pure business logic.
  - **Suggested action:** Create `src/model/edit_queue.rs`. Browse view becomes a renderer over `&EditQueue`.

- [ ] **Extract archive extract/recompress orchestration** (browse_view.rs:1419–1803)
  - **Evidence:** `start_extraction`, `launch_extraction`, `poll_archive_edit` manage temp files, decompression streams, and progress.
  - **Suggested action:** Move to `src/model/archive_edit.rs` exposing a simple progress handle.

- [ ] **`BrowseView` re-opens the filesystem on every operation** (per `MEMORY.md`)
  - **Evidence:** This is intentional today — but it suggests the view is doing model work it shouldn't.
  - **Suggested action:** A `BrowseSession` model object (in `src/model/`) can centralize "open editable FS, run op, sync, invalidate".

### `src/gui/inspect_tab.rs` — 3887 lines

- [ ] **Partition editor state → model** (inspect_tab.rs:117–250, 692–1164)
- [ ] **Export configuration & job orchestration → model** (inspect_tab.rs:1243–1929)
- [ ] **Backup / Clonezilla loading and partition merging → model** (inspect_tab.rs:1983–2173)
- [ ] **Fsck runner state → model** (inspect_tab.rs:2897–3212)
  - **Suggested action (all four):** Establish `src/model/{partition_editor,export,backup_loader,fsck_runner}.rs`. Each owns its background thread + status struct; `inspect_tab.rs` becomes view + dispatch only. Goal: drop inspect_tab below ~2000 lines.

### Cross-cutting GUI

- [ ] **Per-tab `Status` structs (`ExportStatus`, `InspectStatus`, `CacheStatus`, `BlockCacheScan`) live inside GUI files**
  - **Suggested action:** Move to `src/model/` so the future GUI redesign can swap views without touching threading code.

- [ ] **State plumbed through long argument lists** (`devices`, `log`, `&mut self`, etc.)
  - **Suggested action:** Introduce per-tab context structs (`InspectContext`, `BrowseContext`) once model is extracted.

---

## 6. Resize / validation across filesystems

- [ ] **Each FS implements `resize_*_in_place` with the same signature and shape** (FAT, NTFS, exFAT, ext, HFS, HFS+, btrfs, ProDOS)
  - **Suggested action:** A `ResizeStrategy` trait + a `resize_in_place_helper` for the read/patch/write boilerplate.

- [ ] **`validate_*_integrity` per-FS — all funnel into `FsckResult`** (`src/fs/fat.rs:2959`, `ext.rs:3152`, `hfsplus.rs:2084`, …)
  - **Suggested action:** A `ValidationContext` that records issues; per-FS code just emits findings.

---

## 7. Clonezilla

- [ ] **`block_cache.rs` is 1461 lines and conflates streaming, in-memory cache, and persistence** (`src/clonezilla/block_cache.rs`)
  - **Suggested action:** Split into `PartcloneDecompressor` (stream), `PartcloneBlockStore` (BTreeMap cache), and `PartcloneBlockCache` (state machine + persistence). Each <500 lines.

- [ ] **`scan_metadata` (block_cache.rs:429) parses partition metadata in parallel with `backup/metadata.rs`**
  - **Suggested action:** Unify under `src/backup/metadata.rs::parse_metadata_from_reader` (or a shared `PartitionMetadata` type).

- [ ] **Clonezilla GPT handling vs `partition/gpt.rs`** (`src/clonezilla/metadata.rs` reads `<disk>-gpt-1st`/`-gpt-2nd`)
  - **Evidence:** GPT parsing for restore happens via the primary raw file rather than via `partition/gpt.rs::Gpt`.
  - **Suggested action:** Confirm both code paths agree on CRC/serde, or route Clonezilla GPT through the same parser.

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
