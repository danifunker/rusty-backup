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
- **Where it's at:** plan + mock + wired shell + a **working two-pane file
  manager** (M2-lite + M3 + host panes): each pane opens a **disk image** (pick a
  partition) or a **host folder**, with a sortable multi-select listing and `..` /
  double-click nav. Image panes stage delete + copy-in (Apply/Discard writes
  through, virtual overlay, unsaved guards); host panes write immediately
  (delete behind a confirm). The middle column copies a selection between the
  panes in **all four** combos (image↔image, host→image, image→host, host→host).
- **Next concrete step (M6 — requested batch):** add right-click **Rename**,
  **Calculate Checksums** (CRC32/MD5/SHA1/SHA256 window), and **Export to hard
  drive**, plus a per-pane **Tree** toggle. A wildcard **Find/Search** is
  deferred (M7). Full design in [`commander_mode.md`](commander_mode.md) §15; the
  task breakdown is in "Next step" below.

## Commits on this branch

```
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

## Next step — M6 right-click / view-mode batch (do this first when resuming)

M2-lite + M3 + host panes are done. The **requested next batch** (full design in
[`commander_mode.md`](commander_mode.md) §15) adds these. Build each model-first
(engine → model → thin view), unit-testing the model piece before the menu item;
suggested order is smallest-win-first:

1. **Export to hard drive** (§15.3) — *smallest; pure reuse.* A right-click
   "Export to hard drive…" that `pick_folder`s a destination and runs the existing
   `commander_ops::spawn_host_copy` (`ImageToHost` / `HostToHost`) into it. No new
   engine — just a menu item + folder picker + the poll Commander already has. This
   is the §9b "Export → To host" item made first-class (loose files, not an archive).
2. **Calculate Checksums** (§15.2) — new screen showing CRC32 / MD5 / SHA1 / SHA256.
   - **Engine/model:** new `model::checksum` — `hash_reader(reader, progress) ->
     ChecksumSet { crc32, md5, sha1, sha256 }`, streaming once into all four hashers.
     `crc32fast` + `md-5` + `sha2` are vendored; **add the `sha1` crate** (tiny,
     RustCrypto). Do NOT reuse `backup::verify::RunningHasher` (CRC32/SHA256 only,
     backup-specific). Unit-test against known vectors. Thread it (Status pattern)
     for big files; source bytes via `Filesystem::write_file_to` / `std::fs`.
   - **View:** an `egui::Window` (4-row grid, monospace, per-row Copy button).
3. **Rename** (§15.1) — needs an engine gap filled.
   - **Engine:** `EditableFilesystem::rename(parent, entry, new_name)` (per-FS;
     start FAT/exFAT/HFS/HFS+/ext, others `Err(Unsupported)` so the item grays out).
   - **Model:** `StagedEdit::Rename { parent, entry, new_name }` + `apply_edit` arm.
   - **View:** a name dialog (reuse the New-Folder shape) validating via
     `Filesystem::validate_name`. Image pane stages it; host pane does immediate
     `std::fs::rename`. Single-selection only.
4. **Per-pane Tree view** (§15.4) — *largest; a shared-model refactor.* A "Tree"
   toggle on the source bar switches the pane to a lazy folder tree. Lift
   `browse_view`'s `directory_cache` + `expanded_paths` + `render_tree_entry`
   (egui `CollapsingState`) into the model (the §3.3 **R4** share) keyed by the same
   `ListingSource`; both browse view and Commander render over it. Side-keyed
   `id_salt` per `CollapsingState` (two-pane id gotcha). Can land last.

**Deferred — M7 find/search** (§15.5): a per-pane wildcard (`*`/`?`) name search —
`model::find::search(source, root, pattern, progress)` recursive walk + a results
grid mode. Not needed for the core loop; none of M6 depends on it.

Other backlog (unchanged): M1 widget extraction + the M4 File Info window (plan §9);
the browsable-partition gate (lift `is_browsable_type` out of `inspect_tab.rs`);
drag-to-load (§6, OS drop signals already wired in `gui/mod.rs`); New Folder
(`CreateDirectory`); archive Export (§9b); resource-fork sidecars on host copies;
Apply-and-close in the unsaved guard (needs `CommanderMode::temp` kept alive until
both applies finish).

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
