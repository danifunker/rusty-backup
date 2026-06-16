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
- **Where it's at:** plan + runnable mock + wired shell + a **working read-only
  browser** (M2-lite): each pane opens a disk image, picks a partition, and
  shows a sortable, multi-selectable listing with `..` / double-click
  navigation. Copy / delete / **staging is not built yet** (middle column is
  still disabled).
- **Next concrete step:** M3 — the staged copy/delete engine in the middle
  column (per-pane `EditQueue`, the four copy combos, Apply/Discard, virtual
  overlay). See "Next step" below.

## Commits on this branch

```
e7c3711  commander: add handoff/resume doc
c055273  commander: wire entry-point shell (overlay + tab-bar button)
57eb650  commander-mode: plan doc + runnable layout mock (no engine yet)
```

(Run `git log --oneline upcoming-mister..commander-mode` to see just this work.)

**Uncommitted in the working tree** (M2-lite read-only browser — green, ready to
commit): `src/model/dir_listing.rs`, `src/model/commander_source.rs`, the
`CommanderPane` binding in `src/gui/commander/pane.rs`, and the doc/status
refresh in `src/gui/commander/mod.rs`.

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
- **Read-only `CommanderPane` binding** — `src/gui/commander/pane.rs`. Source bar
  (Open + partition dropdown + volume/free readout), path line, async open via
  `BrowseSession::spawn_open` (spinner, polled each frame), and the listing grid
  (sortable headers, multi-select with Ctrl/Cmd + Shift, `..` / double-click
  navigation) lifted from the mock. Writes/copy/staging are still disabled.

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

The read-only browser (M2-lite) is done. The next milestone is **M3 — staged
writes** (plan §5, §7, §8). Build it model-first:

1. **Per-pane `EditQueue`** — give `CommanderPane` a `queue: EditQueue` (reuse
   `src/model/edit_queue.rs` as-is). The pane already owns a `BrowseSession`-
   derived open via `commander_source::session_for`; keep that session around so
   Apply can call `open_editable()`.
2. **Copy (the four backend combos, plan §5)** — middle-column `Copy L->R` /
   `R->L` act on the source pane's `listing.selected_entries()`:
   - host->image / image->image stage `StagedEdit::AddFile` onto the **dest**
     queue (image->image extracts data + resource fork + type/creator to
     `CommanderMode::temp` first);
   - image->host / host->host are immediate writes.
3. **Delete (§8)** — middle-column toggle stages `DeleteEntry` /
   `DeleteRecursive` (or un-stages a pending add); host deletes are immediate
   behind a confirm.
4. **Apply / Discard + virtual overlay (§7)** — lift `browse_view::
   apply_staged_edits` into a shared helper (open_editable -> drain -> `apply_edit`
   loop -> `sync_metadata` -> `commit`), then `listing.reload()`. Render pending
   adds green `+` / pending deletes dimmed+strikethrough (the `DirListing` rows
   need to merge in `queue.pending_adds_for(cwd)` and flag pending deletes — add
   a thin overlay step in the pane's `build_display_rows`).
5. **Unsaved-changes guard** — the Discard/Apply/Cancel modal on Close and on
   switching a pane's source while its queue is non-empty.

After M3, **M1 refactors** are still worth doing for the detail window (M4):
extract `file_detail` (hex/metadata) + `metadata_editor` (type/creator) out of
`browse_view`, and a shared `source_picker`. The browsable-partition gate
(`is_browsable_type` & friends, currently private in `inspect_tab.rs`) should be
lifted to a shared module too — the pane currently lists *all* partitions and
surfaces an open error for non-FS ones instead of graying them out.

## Reuse map (do NOT reinvent these)

| Need | Reuse |
|------|-------|
| open / list / `open_editable` / commit a source | `model::browse_session::BrowseSession` |
| probe partitions + build a pane session | `model::commander_source::{probe_partitions, session_for}` (new) |
| cwd / sorted rows / sort / multi-selection / nav | `model::dir_listing::DirListing` (new) |
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
> `docs/commander_mode.md`. We're on branch `commander-mode`. The read-only
> browser (M2-lite) is done; continue with M3 staged writes (model-first per
> CONTRIBUTING.md), reusing `EditQueue` and the `DirListing` /
> `commander_source` models. Build with `cargo run` and click 'Commander Mode'."

Open questions still to settle are in `commander_mode.md` §14 (resource-fork
handling on host↔image copies, function-key bar, persisting pane state, which
archive formats ship in v1).
