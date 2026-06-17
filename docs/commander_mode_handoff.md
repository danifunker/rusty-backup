# Commander Mode — Handoff / Resume

Pick-up notes for the Commander Mode feature. Read this first, then the full
plan in [`commander_mode.md`](commander_mode.md).

Last updated: 2026-06-16

---

## TL;DR

- **Branch:** `commander-mode` (off `upcoming-mister`). `git checkout commander-mode`.
- **What it is:** a full-page, Midnight-Commander-style two-pane file explorer
  overlay. Full design in [`commander_mode.md`](commander_mode.md); tracked in
  [`OPEN-WORK.md`](OPEN-WORK.md) §6.1.
- **Where it's at:** a **working two-pane file manager** (M2-lite + M3 + host
  panes + the **M6 batch** + **M1 widget extraction** + **M4 File Info window** +
  **Phase 3 .adz/.hdz gzip containers** + two rounds of hands-on-test fixes).
  Each pane opens a **disk image / container** (pick a partition) or a **host
  folder**, with a sortable multi-select listing and `..` (double-click) / folder
  (double-click) nav. Image panes stage delete + copy-in + rename + **New Folder**
  (Apply/Discard, virtual overlay, unsaved guards + a per-pane **Close**); host
  panes write immediately. The middle column has **icon buttons** (painted floppy
  glyphs) for copy in **all four** combos, with a **"Keep original dates"** toggle
  (Amiga + HFS). Right-click also offers **Export to hard drive**, **Calculate
  checksums**, **Rename** (all 26 editable FS), and **File Info / Details**.
  Double-clicking a file (or right-click → File Info) opens a floating window
  with metadata + text/hex preview + an editable subset (HFS type/creator +
  dates, ProDOS type, ext permissions) staged onto the pane queue, with staged
  changes reflected and changed rows marked (blue + `* `). `.adz`/`.hdz` open as
  gzip containers (peeled to read, re-gzipped on edit). The per-pane tree was
  removed (R4 will reuse the browse view's tree).
- **Branch state:** `commander-mode`, tree clean, **~10 unpushed commits** past
  the CI-green nav fix `45a108c` (M1/M4/R1 → icon buttons). Only `45a108c` is on
  the remote / has run CI.
- **Verify on hardware (untested in the dev sandbox):** the macOS device
  elevation fix (`os/macos.rs open_device_for_inspect`, commit `ff36fa3`) — open
  a real `/dev/diskN` in Inspect, confirm the `authopen` admin prompt + read.
- **Round-3 punch-list (next, in order):**
  1. **Functional center Delete button** — needs an "active pane" concept (add
     `focused` to `PaneResponse`/`RowActions`, track `active: Side` on
     `CommanderMode`); the middle Delete acts on it (row right-click delete
     already works).
  2. **`.sit` → Archives redirect** — opening a Mac archive via Inspect's source
     picker routes to the Archives tab instead of failing as a disk.
  3. **Rolling "applied operations" log** for the Commander session (mirror
     `EditQueue::describe`, but for completed applies via `commander_ops::apply_edits`).
  4. **Right pane slightly cut off** — tweak `pane_w`/`mid_w` in `CommanderMode::show`.
  5. **Refine icon proportions** (`commander/mod.rs draw_copy_icon`/`draw_floppy`).
  6. **Phase 2 — "Open Backup…" in a pane:** lift Inspect's per-compression
     backup-open (`inspect_tab.rs open_browse` case 2 + `open_browse_zstd` /
     `open_browse_clonezilla`, ~line 4670+) into a shared `model` fn returning a
     configured `BrowseSession`; set `show_backup_folder: true` + handle
     `SourceEvent::BackupFolder`.
  7. **Phase 2 — device parity:** thread `&[DiskDevice]` (`gui/mod.rs self.devices`)
     into `CommanderMode::show` → `pane.show` → `source_bar`; `show_devices: true`;
     handle `SourceEvent::Device` via an elevated open + probe + preopen
     `BrowseSession`. **Untestable in sandbox; gated on the hardware verify above.**
  8. **R4** — replace the removed Commander tree with the browse view's tree (§3.3).
  9. **Per-filesystem editable-metadata matrix** — which `EditableFilesystem`
     setters each FS supports vs. returns `Unsupported`.
  10. **HDZ "no files found"** — needs a real `.hdz` fixture; likely RDB parsing
      of the inner `.hdf` after the gzip peel (`commander_source::probe_partitions`).
  11. M7 Find/Search; M5 Compare; broad metadata-setter backlog (§10.2).

## Commits on this branch

```
522af44  commander: pictured icon buttons in the middle column
6eccbb8  commander: New Folder in both panes
2fba0bd  commander: round-2 UI fixes (collision, drop tree, dbl-click .., meta marker)
ff36fa3  os/macos: prompt for elevation on any device-open failure (read-only)
c1fb387  commander: "Keep original dates" on image-to-image copy
b55bc46  fs/affs: read files that span extension (T_LIST) blocks
07bd014  commander: Phase 3 — .adz/.hdz as editable gzip containers
7353247  commander/inspect: make the Mac-archives picker filter opt-in
2f8dfb1  commander: Phase 5 — metadata-edit UX (staged values, blue tint, edits list, dates)
2e41a11  commander: M1 widget extraction + M4 File Info window + R1 source picker + round-1
45a108c  commander: fix navigate_to path split on Windows host panes (CI-green; pushed)
--- earlier ---
<H2>     commander: host copy combos + host delete (H2)
8d4773e  commander: host-folder panes (browse) — PaneSource::Host (H1)
05664a4  commander: unsaved-changes guards on close / source switch (M3c)
fea01aa  commander: cross-pane copy via the middle column (M3b)
f72c69a  commander: per-pane staged delete + Apply/Discard (M3a)
627e989  commander: read-only two-pane browser over DirListing (M2-lite)
e7c3711  commander: add handoff/resume doc
c055273  commander: wire entry-point shell (overlay + tab-bar button)
57eb650  commander-mode: plan doc + runnable layout mock (no engine yet)
```

(Run `git log --oneline upcoming-mister..commander-mode` to see just this work;
the H2 commit lands with this doc update.)

## What's landed (done)

- **Plan doc** — `docs/commander_mode.md` (14 sections: locked decisions,
  architecture, reuse/refactor strategy, copy/staging semantics, drag-to-load,
  editable-metadata inventory §10, export/archiving crate inventory §9b,
  interaction model §9a, milestones M0–M5).
- **Runnable layout mock** — `examples/commander_mock.rs` (throwaway, fake data).
  Demonstrates: two panes + middle column, multi-select (Ctrl/Cmd + Shift),
  sortable columns, editable `@N` path, copy icon + animated arrow, right-click
  context menu (copy/delete/undelete/info/export), delete-as-toggle + undelete,
  New Folder, per-pane Apply/Discard/Close, drag-to-load, File Info window + hex.
- **Entry-point shell (view layer)** — `src/gui/commander/{mod,pane}.rs`, launched
  from a "Commander Mode" button in the tab bar (`src/gui/mod.rs`). Opens a
  full-page overlay (`Option<CommanderMode>` on `RustyBackupApp`) that takes over
  the frame.
- **`DirListing` model** — `src/model/dir_listing.rs`. Pure, unit-tested model:
  owns the open `Box<dyn Filesystem>`, a navigation stack of `(dir, sorted
  children)` frames, the active `SortColumn`/direction, and the name-keyed
  multi-selection + anchor. Methods: `load_root`, `enter`/`up`/`reload`,
  `resort`, `current_rows`, `click`/`ctrl_click`/`shift_click`/`clear_selection`.
  Folders-first stable sort; shared `type_tag(&FileEntry)` helper (also the sort
  key for the Type column). 10 tests (sort, parent-row, ctrl/shift selection,
  real-filesystem `enter`/`up`).
- **`commander_source` model** — `src/model/commander_source.rs`.
  `probe_partitions(path)` peels any container (same way `BrowseSession::open`
  does) and returns the `Vec<PartitionInfo>`; `session_for(path, &PartitionInfo)`
  builds the pane's `BrowseSession` (offset via `byte_offset()`, FAT type-byte
  inference for superfloppies). Round-trip test on a real superfloppy.
- **`CommanderPane` binding** — `src/gui/commander/pane.rs`. Source bar
  (Open + partition dropdown + Apply/Discard + volume/free readout), path line,
  async open via `BrowseSession::spawn_open` (spinner, polled each frame), and the
  listing grid (sortable headers, multi-select with Ctrl/Cmd + Shift, `..` /
  double-click navigation) lifted from the mock.
- **`commander_ops` model** — `src/model/commander_ops.rs`. `apply_edits()` opens
  the pane's source read-write, replays the `EditQueue` via `apply_edit`, syncs,
  and commits; `spawn_apply()` threads it behind an `ApplyStatus`. `stage_copy()`
  extracts selected entries (data + resource fork + HFS type/creator) to a temp
  dir and returns the `AddFile`/`CreateDirectory` edits to recreate them on the
  destination (directories recurse). 3 unit tests (apply delete+add, copy subtree).
- **M3a staged delete + Apply/Discard** — right-click a row for Delete / Undelete
  / Remove-from-staging on the selection; Apply (threaded) writes and re-opens the
  source; Discard clears. Virtual overlay merges pending deletes (dimmed +
  strikethrough, `- `) and pending adds (green, `+ `) in `build_display_rows`.
  `EditQueue::remove_pending_delete` added (with test).
- **M3b cross-pane copy** — middle `Copy L->R` / `R->L` (and a per-row "Copy to
  other pane") stage an image→image copy onto the other pane's queue via
  `stage_copy`; `CommanderMode` owns a lazy `temp` `TempDir` and the copy
  direction; `CommanderPane::show` returns a `PaneResponse { status,
  copy_to_other }`.
- **M3c unsaved guard** — Close with staged edits opens a Discard&Close / Cancel
  modal; switching a pane's source/partition with a non-empty queue opens a
  per-pane Discard&switch / Cancel confirm.
- **H1 host panes (browse)** — `DirListing` gains a `ListingSource`
  (None / Image(fs) / Host); a host pane lists via `std::fs` (size +
  chrono-formatted mtime), `load_host_root` opens a folder, `up()` re-roots a
  host pane above its loaded root, `is_host()` exposes the kind. Pane gets an
  "Open Folder..." button; partition dropdown / Apply / free-space are
  image-only. Host-listing + nav unit-tested.
- **H2 host copy combos + delete** — `commander_ops` adds `stage_host_to_image`
  (host→image, real paths, no temp) and `spawn_host_copy` for the immediate
  `image→host` / `host→host` writes (threaded, `HostCopyStatus`); `CommanderMode::
  copy` dispatches on `(src_host, dest_host)` across all four combos and polls
  `pending_host_copy`, re-listing the destination on completion. Host panes get a
  right-click immediate Delete behind a confirm. 3 new copy unit tests.
- **M6.1 Export to hard drive** — a right-click "Export to hard drive..." action
  on any data row (image or host pane) `pick_folder`s a destination and runs the
  existing `spawn_host_copy` (`ImageToHost` for an image source, `HostToHost` for a
  host source). Pure reuse: `PaneResponse` gains `export_to_host`, `CommanderMode::
  export` builds the job, and `pending_host_copy`'s destination side is now
  `Option<Side>` (`None` = export, so `poll_host_copy` skips the re-list). Excluded
  on not-yet-applied staged-add rows (no real data yet).
- **M6.3 Rename** — new engine method `EditableFilesystem::rename(parent, entry,
  new_name)` (default `Unsupported`), now implemented **in place for every editable
  filesystem (26)**: FAT, exFAT, NTFS, ext, UFS, EFS, HFS, HFS+, MFS, ProDOS,
  Apple DOS 3.3, CBM, Atari DOS, RS-DOS, DragonDOS, Acorn DFS/ADFS, OS-9, QDOS,
  Human68k, CP/M, AFFS, PFS3, SFS, XFS, Alto BFS. Each keeps the entry's identity
  (start cluster / inode / CNID / objectnode) and data; only the parent listing's
  name changes. Strategies vary by on-disk shape: fixed-field in-place overwrite
  (CBM/Atari/ProDOS/OS-9/RS-DOS/DragonDOS/DFS/ADFS/Human68k/MFS…), remove+add for
  variable-length dirents (exFAT/ext/UFS/EFS/PFS3/SFS), B-tree re-key + thread fix
  (HFS in-place Str31, HFS+ rebuilt variable thread), NTFS dual-write (index $I30
  re-key + child `$FILE_NAME` grown/shrunk in place, sequence number preserved),
  AFFS hash-chain relink, XFS add-then-remove with matched `is_dir` so the parent
  nlink ±1 cancels, BFS full-volume rebuild (DV entry + leader). Case-only renames
  allowed, real collisions rejected; 36 unit tests (incl. XFS fsck-clean,
  HFS+/NTFS persistence + grow/shrink). Model: `StagedEdit::Rename` + `apply_edit`
  arm + `EditQueue::{pending_rename_for, remove_pending_rename}`. View: right-click
  "Rename..." (single entry) opens a name dialog validating via
  `Filesystem::validate_name`; image panes stage it (overlay shows `old -> new`,
  right-click "Cancel rename"), host panes rename immediately via `std::fs::rename`
  (existence-checked). Staging a delete drops any staged rename on the same entry.
  Rename is offered on any loaded pane (like Delete): browse-only filesystems
  (e.g. btrfs) surface `Unsupported` at Apply rather than being pre-gated, so there
  is no per-type `supports_rename` table to drift. **Follow-up:** a possible
  `rb-cli` rename verb reusing the trait method.
- **M6.2 Calculate Checksums** — new `model::checksum` (added the `sha1` crate):
  `ChecksumHasher` (a `std::io::Write` sink feeding crc32fast + md-5 + sha1 + sha2),
  `hash_reader(reader, progress) -> ChecksumSet { crc32, md5, sha1, sha256 }` with
  hex accessors, and `spawn(ChecksumJob)` threading a whole selection behind a
  `ChecksumStatus` (re-opens an image source on the worker, same as host copy;
  directories skipped). 4 unit tests (empty + "abc" known vectors, monotonic
  progress, writer==reader). View: right-click "Calculate checksums..." opens
  `CommanderMode::checksums` — an `egui::Window` with a per-file 4-row grid
  (monospace value + Copy button) and a live spinner/progress bar while hashing.
- **M6.4 Per-pane Tree view** — a "Tree" toggle on each pane's source bar splits
  the pane into a lazy folder tree (left, navigation) + the flat grid (right, the
  working set). "Tree navigates, grid is the working set" — clicking a tree folder
  calls `DirListing::navigate_to(path)` to set the grid's cwd, so the existing
  selection / copy / delete / rename / checksum model is untouched. New on
  `DirListing`: `root_entry()`, `list_dir(dir)` (public lister, off-stack),
  `navigate_to(path)` (rebuilds the stack from root). The tree state
  (`tree_cache: HashMap<path, child-dirs>` + `tree_expanded`) lives on the pane and
  is dropped on every source/partition switch + host re-list; nodes lazily list
  child directories on first expansion (the browse_view `CollapsingState` pattern),
  side-keyed `id_salt` per node. 1 model test (`navigate_to` jump + up + back).
  NOTE: this is the *contained* version — it does **not** do the §3.3 R4 refactor
  (lifting browse_view onto the shared tree); browse_view still has its own
  `directory_cache`/`expanded_paths`. R4 dedup is a follow-up.

## How to run it

```bash
# The runnable layout mock (fake data, no disk I/O):
cargo run --example commander_mock --features mock_gui

# The real app — click "Commander Mode" in the tab bar:
cargo run        # default features include `gui`

# The checks the pre-commit hook enforces (must be clean):
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test --lib
```

## Next step

The live, ordered punch-list is the **round-3** list in the TL;DR above. M6
(Export / Checksums / Rename-all-26-FS / the now-removed Tree), M1, M4, and
Phase 3 are all done. Build each remaining item model-first (engine → model →
thin view), unit-testing the model piece before the view.

**Still-open backlog beyond the round-3 list** (lower priority): the
browsable-partition gate (lift `is_browsable_type` out of `inspect_tab.rs`);
drag-to-load (§6, OS drop signals already wired in `gui/mod.rs`); archive Export
(§9b — zip/tgz/sit); resource-fork sidecars on host copies; Apply-and-close in
the unsaved guard (needs `CommanderMode::temp` alive until both applies finish);
a possible `rb-cli` rename verb; **M7 find/search** (§15.5 —
`model::find::search` recursive wildcard walk + a results grid mode).

## Reuse map (do NOT reinvent these)

| Need | Reuse |
|------|-------|
| open / list / `open_editable` / commit a source | `model::browse_session::BrowseSession` |
| probe partitions + build a pane session | `model::commander_source::{probe_partitions, session_for}` (new) |
| cwd / sorted rows / sort / multi-selection / nav | `model::dir_listing::DirListing` (new) |
| apply a queue / stage a copy / immediate host copy | `model::commander_ops::{apply_edits, spawn_apply, stage_copy, stage_host_to_image, spawn_host_copy}` (new) |
| staged copy/delete + apply | `model::edit_queue::{EditQueue, StagedEdit, apply_edit}` |
| picker extensions | `model::file_types::DISK_IMAGE_EXTS` |
| size / date formatting | `partition::format_size`, `FileEntry::size_string()` / `.modified` |
| metadata + hex detail, type/creator editor rows | extract from `gui::browse_view` (M1) |
| `(devices, log)` arg bundle | `gui::context::TabContext` |

## Gotchas (learned during this work)

- **Patched eframe.** This crate's `eframe::App` requires `fn ui(&mut self,
  &mut egui::Ui, ..)` (not stock `update()`), and panels are
  `egui::Panel::top("id").show_inside(ui, ..)` / `CentralPanel::default()
  .show_inside(ui, ..)`. Follow `src/gui/mod.rs` and `src/gui/commander/mod.rs`.
- **id_salt per pane.** Two near-identical panes render in one `Ui` tree, so every
  stateful widget (`ScrollArea`, `ComboBox`, `CollapsingState`, `Grid`) MUST take
  a side-keyed `id_salt` or egui raises an "ID clash" and one pane ghosts the
  other.
- **No Unicode glyphs** in UI text (CONTRIBUTING hard rule) — ASCII only: `..`,
  `<DIR>`, `->`, `^`/`v`, `A H S R`. The animated copy arrow is *painted* (not a
  glyph). egui-time animation needs `ui.ctx().request_repaint()`.
- **Layering (CONTRIBUTING).** `gui/` is a thin view — no orchestration, no
  background spawns (use a model runner / `BrowseSession::spawn_open`), no heavy
  state. State + queues live in `model/`.
- **`mock_gui` feature.** The mock is gated behind `mock_gui = ["dep:eframe",
  "dep:egui"]` (no rfd) so the default `cargo build --examples` doesn't pull the
  gtk3 rfd tree. Run it with `--features mock_gui`.
- **macOS trackpad right-click / multi-select** use egui's cross-platform
  `Modifiers::command` (= Cmd on macOS, Ctrl elsewhere) — already in the mock.

## How to resume a session

Point a fresh session at this file:

> "Resume Commander Mode. Read `docs/commander_mode_handoff.md`, then
> `docs/commander_mode.md` §15. We're on branch `commander-mode`. Browsing (image
> + host panes), all four copy combos, delete, and the unsaved guards are done.
> Build the M6 batch model-first per CONTRIBUTING.md (reusing `DirListing` /
> `commander_ops` / `EditQueue`): Export to hard drive (§15.3), Calculate
> Checksums (§15.2, add the `sha1` crate), Rename (§15.1, needs an
> `EditableFilesystem::rename` trait method), then the per-pane Tree view (§15.4).
> Build with `cargo run` and click 'Commander Mode'."

Open questions still to settle are in `commander_mode.md` §14 (checksum set,
rename scope, tree-view model, resource-fork sidecars on host copies, archive
formats, function-key bar, persisting pane state).
