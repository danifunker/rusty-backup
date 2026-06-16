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
- **Next concrete step:** **M1 widget extraction + the M4 File Info window**
  (double-click → detail + editable HFS/ProDOS/ext metadata), plus the
  browsable-partition gate. See "Next step" below.

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

## Next step (do this first when resuming)

M2-lite + M3 + host panes are done — both panes browse images **and** host
folders, with all four copy combos, delete, and the unsaved guards. Pick the
next milestone:

1. **M1 widget extraction + M4 detail window** — extract `file_detail`
   (hex/metadata) and `metadata_editor` (type/creator rows) out of `browse_view`
   into shared modules, then add the double-click File Info window (plan §9) with
   the editable HFS/ProDOS/ext subset (the two new `StagedEdit` variants in §10.2).
2. **Browsable-partition gate** — lift `is_browsable_type` & friends out of
   `inspect_tab.rs` (private today) into a shared module so the partition dropdown
   can gray out non-FS partitions instead of listing all and erroring on open.
3. **Drag-to-load** (plan §6) — the OS drop signals are already wired in
   `gui/mod.rs`; route a dropped image/folder onto the pane under the pointer
   (load as source) and dropped host files onto an image pane (stage host→image).

Smaller follow-ups: New Folder (`CreateDirectory`) + Export (§9b); host→image
resource forks (AppleDouble import); image→host fork sidecars; Apply-and-close in
the unsaved guard (currently Discard&Close / Cancel only — a combined apply needs
to keep `CommanderMode::temp` alive until both applies finish, since copied blobs
live there); a copy-progress readout for large host writes.

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
> `docs/commander_mode.md`. We're on branch `commander-mode`. Browsing (image +
> host panes), all four copy combos, delete, and the unsaved guards are done;
> continue with the M1 widget extraction + the M4 File Info window (model-first
> per CONTRIBUTING.md, reusing `DirListing` / `commander_ops` / `EditQueue`).
> Build with `cargo run` and click 'Commander Mode'."

Open questions still to settle are in `commander_mode.md` §14 (resource-fork
handling on host↔image copies, function-key bar, persisting pane state, which
archive formats ship in v1).
