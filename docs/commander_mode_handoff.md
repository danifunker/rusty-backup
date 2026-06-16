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
- **Where it's at:** plan + runnable mock + the wired entry-point shell are
  committed and green. The real listing/copy/staging **engine is not built yet**.
- **Next concrete step:** the `DirListing` model (engine/model-first, unit-tested),
  then bind the view to it. See "Next step" below.

## Commits on this branch

```
c055273  commander: wire entry-point shell (overlay + tab-bar button)
57eb650  commander-mode: plan doc + runnable layout mock (no engine yet)
```

(Run `git log --oneline upcoming-mister..commander-mode` to see just this work.)

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
  the frame; each pane can pick a source file via the shared rfd picker. The
  listing body is a placeholder.

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

Per CONTRIBUTING.md the order is **engine → model → view**, model-first and
testable. The one genuinely new model object is `DirListing`:

1. **`src/model/dir_listing.rs`** — a pure, unit-tested model:
   - holds a `BrowseSession` (reuse — don't reinvent source opening), the current
     directory path, the cached sorted `Vec<FileEntry>`, sort state, and the
     multi-selection (`Vec<id>` + an anchor).
   - methods: `enter(dir)`, `up()`, `resort(col)`, selection ops
     (`click`/`ctrl_click`/`shift_click`/`clear`), `current_rows()`.
   - no egui, no threading in the pure path; add `#[cfg(test)]` tests for sort
     order (folders-first), `..` navigation, and the Ctrl/Shift selection logic
     (mirror the mock's `select_click`).
2. **Bind the view** — `CommanderPane` (in `src/gui/commander/pane.rs`) holds a
   `DirListing` + an `EditQueue` (reuse) and renders the column grid (lift the
   row/column painting from the mock). Keep orchestration out of the view.

Then follow milestones M1–M5 in the plan doc (M1 also extracts shared widgets —
`file_detail` hex/metadata + the type/creator editor + a source picker — out of
`browse_view`/`inspect_tab` so Commander and the classic browser share them).

## Reuse map (do NOT reinvent these)

| Need | Reuse |
|------|-------|
| open / list / `open_editable` / commit a source | `model::browse_session::BrowseSession` |
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
> `docs/commander_mode.md`. We're on branch `commander-mode`. Continue with the
> `DirListing` model (model-first per CONTRIBUTING.md), reusing `BrowseSession`
> and `EditQueue`. Run the mock with
> `cargo run --example commander_mock --features mock_gui`."

Open questions still to settle are in `commander_mode.md` §14 (resource-fork
handling on host↔image copies, function-key bar, persisting pane state, which
archive formats ship in v1).
