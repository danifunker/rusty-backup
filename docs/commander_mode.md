# Commander Mode — Design & Implementation Plan

Status: **Shipped: M2-lite + M3 staged writes + host panes + the M6 right-click
batch + M1 widget extraction (R0+R1) + the M4 File Info window + Phase 3
(.adz/.hdz editable gzip containers) + two rounds of hands-on-test fixes.** Both
panes browse disk images / containers and host folders; staged delete + copy-in
+ rename + New Folder on image panes, immediate writes on host panes, all four
copy combos, Apply/Discard, virtual overlay, unsaved guards. A floating File
Info window (double-click a file or right-click → "File Info / Details…") shows
metadata + a text/hex preview and stages the editable subset (HFS type/creator +
dates, ProDOS type, ext permissions) onto the owning pane's queue, reflecting the
staged value back and tinting changed rows (blue + a `* ` marker). The source bar
(Inspect tab + both Commander panes) is the shared `source_picker` widget; the
middle column uses procedurally-painted icon buttons. Image-to-image copy
preserves data + resource fork + type/creator + (optionally) original dates.

`.adz`/`.hdz` open as gzip containers (peeled for reading, re-gzipped on edit) —
never materialized to a separate `.adf`/`.hdf`. The old per-pane tree toggle was
removed (a future R4 pass reuses the browse view's tree instead).

**Round-3 / next (see `commander_mode_handoff.md` for the live punch-list):**
functional center Delete (active-pane), `.sit`→Archives redirect, a rolling
applied-ops log, **Phase 2** "Open Backup…" + physical-device parity in panes,
the R4 tree dedup (§3.3), a per-FS editable-metadata matrix, and the deferred
wildcard Find/Search (M7, §15.5).
Last updated: 2026-06-16
Owner: TBD

A Midnight Commander–style, full-page, two-pane file explorer for Rusty Backup.
Each pane points at a disk-image partition (or a real host folder), shows a flat
single-directory listing with sortable Name / Size / Modified / Type columns, and
the middle column carries the copy-direction and delete actions. All writes that
land in a disk image go through the existing **staged-edit** pipeline so they stay
atomic and prompt-on-exit; host-folder writes happen immediately.

A runnable visual mock ships alongside this plan:

```bash
cargo run --example commander_mock --features mock_gui
```

It is throwaway code backed by hardcoded sample volumes — no real disk I/O — and
exists only to agree on layout and interaction before we build the real thing.

---

## 1. Locked decisions (from kickoff Q&A)

| # | Decision | Choice |
|---|----------|--------|
| 1 | **Placement** | **Full-page modal overlay.** A toolbar button takes over the whole frame; the tab strip is hidden while open; a "Close" button returns to the previous tab. |
| 2 | **Pane sources** | A pane may be a **disk-image partition** *or* a **real host-OS folder**. Image writes are staged; host writes are immediate. |
| 3 | **Editable metadata (v1)** | Edit only what already has an `EditableFilesystem` setter: HFS type/creator/dates, ProDOS type/aux/access, ext permissions. Everything else is **read-only display** in v1. The full gap list is inventoried in §10 to check off later. |
| 4 | **Mock** | A runnable `examples/commander_mock.rs` egui mock (this is done). |
| 5 | **Drag-to-load** | Dropping a disk image onto a pane opens it as that pane's source (left and right loaded independently). Dropping host files onto an already-open image pane stages an import-copy. |
| 6 | **Reuse over rewrite** | Maximize reuse of existing GUI/model components; refactoring `browse_view` / `inspect_tab` to extract shared pieces is explicitly in scope (§3). |

**Deferred (not v1):**

- **Compare** (Beyond Compare–style content diff of two files / two trees). Designed
  for later — see §11.
- **Broad in-place metadata setters** (Amiga protection/comment/date, FAT/exFAT
  attribute bits, uid/gid on non-ext, generic mtime). Inventoried in §10; each is a
  new `EditableFilesystem` method. v1 displays these read-only.
- **Rename in place** — there is no `rename` on `EditableFilesystem` today; v1 has no
  rename (copy+delete is the workaround). Tracked in §10.

---

## 2. Architecture overview

### 2.1 Where it hooks into the app

`RustyBackupApp` (`src/gui/mod.rs`) gains one field:

```rust
commander: Option<CommanderMode>,   // Some(..) == overlay is open
```

A "Commander Mode" button on the top toolbar sets `commander = Some(CommanderMode::new())`.
In the render path (`RustyBackupApp::ui` / the panel dispatch around `mod.rs:1016`),
**short-circuit before the tab strip**:

```rust
if let Some(cmd) = &mut self.commander {
    if cmd.show(ctx, &mut shared_ctx) == CommanderExit::Close {
        self.commander = None;          // guarded by the unsaved-changes check inside show()
    }
    return;                              // tab strip + active tab not drawn this frame
}
// ...normal tab UI...
```

`CommanderMode::show` owns the full frame: a top bar (title + Close), the two panes
with the middle action column, and a bottom status/apply bar. It returns a small
`CommanderExit` enum so the app knows when to tear it down. The Close path runs the
same unsaved-changes guard the browse view uses (§7).

> **Build note flagged during research:** `mod.rs` currently uses a non-standard
> `fn ui(&mut self, ui: &mut egui::Ui, ...)` + `egui::Panel` (singular) idiom that the
> rest of the codebase does not. Commander Mode should follow the *ordinary* eframe
> idiom used by every tab and by `partition_bar` / `journal_view`
> (`fn show(&mut self, ui: &mut egui::Ui, ...)` with `TopBottomPanel` / `CentralPanel`
> / `egui::Window` / `ScrollArea` / `Grid`). Confirm the resolved egui version at build
> time before copying the `mod.rs` panel idiom.

### 2.2 Module layout

```
src/gui/commander/
    mod.rs          CommanderMode: top bar, two panes, middle actions, bottom bar, Close guard
    pane.rs         CommanderPane: one pane (source + listing + sort + selection + queue)
    source.rs       PaneSource enum (Empty | Host | Image) + loading / drag-drop routing
    actions.rs      copy (4 backend combos), delete, recursion, collisions, temp extraction
    detail.rs       floating "File Info" window (display + editable-subset staging)  -> wraps src/gui/file_detail (§3)
```

New shared modules extracted from existing code (see §3) live outside `commander/`
so both Commander and the classic browse view use them.

### 2.3 Data model

```rust
pub struct CommanderMode {
    left:  CommanderPane,
    right: CommanderPane,
    detail: Option<DetailWindow>,        // floating Info pane, see §9
    unsaved: Option<UnsavedPrompt>,      // Discard / Apply / Cancel modal
    temp: tempfile::TempDir,             // holds files extracted for staged image->image / import copies
    drop_target: Option<Side>,           // pane currently under a hovered OS drag (highlight)
}

pub struct CommanderPane {
    side: Side,                          // Left | Right
    source: PaneSource,
    listing: DirListing,                 // shared model (§3) — cwd, cached rows, sort, selection
    queue: EditQueue,                    // src/model/edit_queue.rs — reused as-is (image panes only)
    cached_fs: Option<Box<dyn Filesystem>>,   // read-only fs reused across listings
    pending_open: Option<Arc<Mutex<BrowseOpenStatus>>>,   // async source open + spinner
    pending_op: Option<Arc<Mutex<OpStatus>>>,             // async copy/apply/extract progress
}

pub enum PaneSource {
    Empty,                                            // nothing loaded; shows "Open... / drop here"
    Host  { root: PathBuf },                          // a real host directory subtree
    Image { session: BrowseSession, part: PartitionRef, editable: bool },
}
```

`BrowseSession` (`src/model/browse_session.rs`) is the existing engine that already
handles raw images, devices (with preopen fd), CHD/zstd/Clonezilla containers, the
read-only `open()` and the editable `open_editable()` → `(efs, commit)` pair. Each
image pane owns one. This is the single biggest reuse: Commander does **not** learn
container formats — it asks `BrowseSession`.

---

## 3. Reuse & refactor strategy

Goal: build Commander out of existing parts, extracting shared modules where the
current code is welded into `browse_view` / `inspect_tab`. Refactors are staged so
each lands with **no behavior change** to the existing browse view before Commander
depends on it.

### 3.1 Reuse as-is (no refactor)

| Component | Location | Used by Commander for |
|-----------|----------|-----------------------|
| `BrowseSession` (open/open_editable/spawn_open/commit) | `src/model/browse_session.rs` | opening any image-pane source, async open, container commit |
| `EditQueue` + `StagedEdit` + `edit_queue::apply_edit` | `src/model/edit_queue.rs` | the per-pane staging queue; Apply replays edits then `sync_metadata()` |
| `FileEntry` | `src/fs/entry.rs` | row model + detail metadata |
| `partition::format_size`, `FileEntry::size_string()` / `.modified` / `.mode_string()` | `src/partition/mod.rs`, `src/fs/entry.rs` | column formatting |
| `Filesystem::list_directory` / `read_file` / `write_file_to` / `write_resource_fork_to` | `src/fs/filesystem.rs` | listing + extraction for cross-volume copy |
| `gui::fonts::install_cjk_font` | `src/gui/fonts.rs` | CJK filename rendering (already global) |
| App drag-drop plumbing (`with_drag_and_drop(true)`, `raw.dropped_files`/`hovered_files`) | `src/main.rs`, `src/gui/mod.rs:735` | drop-to-load (§6) |

### 3.2 Extract into shared modules (refactor — user OK'd)

Each extraction is a pure move with the existing caller updated to use it; verifiable
because the classic browse view must look/behave identically afterward.

| New shared module | Extracted from | Consumers after |
|-------------------|----------------|-----------------|
| `src/gui/source_picker.rs` — device/image/host picker + partition dropdown widget | `inspect_tab.rs` source ComboBox + `open_browse` resolution (`inspect_tab.rs:420`, `:4586`) | Inspect tab **and** each Commander pane source bar |
| `src/model/dir_listing.rs` — `DirListing`: cwd path, cached `Vec<FileEntry>`, sort state, selection, `enter()/up()/resort()` | new (today's `browse_view` tree has no flat single-dir model) | both Commander panes; optional later migration of `browse_view` |
| `src/gui/file_detail.rs` — render metadata rows + hex preview + content-type classify | `browse_view.rs` `render_content_panel` (`:1544`), `render_hex_view` (`:5560`), `detect_content_type` (`:5494`) | browse view detail panel **and** Commander Info window |
| `src/gui/metadata_editor.rs` — HFS type/creator row, ProDOS type row, dates, ext perms widgets that emit `StagedEdit`s | `browse_view.rs` `render_hfs_type_row` (`:3278`), `render_prodos_type_row` (`:3471`) | browse view **and** Commander Info window |

### 3.3 Refactor sequencing

- **R0** (**done**) — Extracted `file_detail` (metadata rows, content-type classify,
  hex preview) + `metadata_editor` (HFS type/creator, ProDOS type, plus new HFS-dates
  and ext-permissions rows). Browse view behaves identically.
- **R1** (**done**) — Extracted `source_picker` from `inspect_tab`; the Inspect tab and
  both Commander pane source bars now render the same ComboBox widget and share the
  `pick_image_file` open/filter/materialize logic.
- **R2** (**done**) — `DirListing` model + the async listing worker.
- **R3** (**done**) — `CommanderPane` / `CommanderMode` on R0–R2.
- **R4** — (optional, later) migrate `browse_view`'s tree to share `DirListing` for
  its lazy children, retiring duplicate `directory_cache` logic. Commander's own tree
  (§15.4) shipped self-contained; this is the cross-view dedup.

---

## 4. The pane: source, listing, navigation

### 4.1 Source bar (top of each pane)

`[ Open... ] [ Open Folder... ] [ partition v ] [ Tree ]   <volume label>   free: <n>`

- **Open...** → image / device picker (rfd `pick_file`); **Open Folder...** → host
  folder (rfd `pick_folder`). (M1 will fold both into a shared `source_picker`.)
- **partition dropdown** — populated after an image is opened (reuses Inspect's
  partition parse). Selecting one (re)opens the pane's `BrowseSession` at that
  partition's offset/type. Host panes hide the dropdown.
- **Tree** toggle — switch this pane between the flat grid and a lazy folder tree
  (planned; full design in §15.4).
- Right side shows volume label + free space (`free_space()` for editable images);
  host panes show "host folder".

### 4.2 Listing

A flat, single-directory grid. Columns:

| Column | Source | Notes |
|--------|--------|-------|
| Name | `entry.name` | `..` pinned first; dirs grouped before files ("folders first") |
| Size | `entry.size_string()` (→ `partition::format_size`, base-1024 KiB/MiB/GiB) | blank for dirs |
| Modified | `entry.modified` (pre-formatted string) | blank if `None` |
| Type | short FS-appropriate tag | HFS `type_code`, ProDOS `$xx`, FAT attr letters (`A H S R`), `<DIR>`, `-> ` for symlink |

- **`..` row** — navigates to the parent. At an image volume root, `..` is disabled.
  For host panes, `..` walks up the real tree (may go above the initially-opened root —
  allowed).
- **Double-click a directory** → enter it (replaces the listing). Double-click a file →
  open the Info/detail window (§9).
- **Single-click** → select (multi-select with Ctrl/Shift for batch copy/delete; the
  mock shows single-select only).
- **Right-click (`secondary_clicked`)** → open the Info window for that entry.
- **Sortable headers** — click a column header to sort by it; click again to reverse.
  Caret suffix (`^` / `v`, ASCII) marks the active column. Folders-first is preserved
  within the sort.
- **Path line** — a breadcrumb / path label under the source bar shows the cwd.

### 4.3 Threading

Listing a directory, extracting files for copy, and applying a queue can be slow on
compressed containers or large directories. Follow the existing async pattern
(`BrowseSession::spawn_open` + `Arc<Mutex<Status>>` polled each frame + a spinner):
`pending_open` for source opens, `pending_op` for copy/apply/extract. The UI stays
responsive; controls on a pane are disabled while that pane has an op in flight.

---

## 5. Copy semantics (the core)

The middle column has **Copy L->R**, **Copy R->L**, and **Delete**. Copy moves the
source pane's selection into the destination pane's current directory. There are four
backend combinations; each preserves as much metadata as the destination supports.

| Source → Dest | Mechanism | Staged? |
|---------------|-----------|---------|
| **host → image** | stage `StagedEdit::AddFile { host_path = real file, ... }` (the existing add-file flow) | yes (dest queue) |
| **image → image** | extract source entry (data fork **+** resource fork + type/creator/aux) to `CommanderMode::temp`, then stage `AddFile { host_path = temp, resource_fork, hfs_type_override, .. }` on dest | yes (dest queue) |
| **image → host** | extract source data fork straight to the host destination path | no (immediate) |
| **host → host** | `std::fs::copy` | no (immediate) |

### 5.1 Metadata-preserving extraction

`StagedEdit::AddFile` already carries `resource_fork`, `hfs_type_override`,
`hfs_creator_override`, `prodos_type`, `prodos_aux`. So an image→image copy captures
those from the source `FileEntry` (via `write_resource_fork_to` + `type_code`/
`creator_code`/`aux_type`) and replays them onto the destination — type/creator,
resource forks, and ProDOS types survive a cross-volume copy.

### 5.2 Directory recursion

Copying a directory stages a `CreateDirectory` for the dir, then walks its subtree
(`list_directory`) staging `CreateDirectory` / `AddFile` for each descendant in order.
For image→host this recurses with real `create_dir_all` + extraction.

### 5.3 Cross-filesystem mapping & warnings

The destination silently drops metadata it can't hold (e.g. HFS resource fork / type
→ FAT). Surface a one-line, non-blocking warning in the bottom bar when a copy will
lose forks/metadata, and validate names against the destination
(`Filesystem::validate_name`) — on rejection, prompt to rename or skip.

### 5.4 Name collisions

If the destination already has an entry with that name, show a small modal:
**Overwrite / Skip / Cancel** (with "apply to all" for batch). Overwrite stages a
delete-then-add on the dest queue.

### 5.5 Temp-file lifetime

Files extracted for image→image / import copies live in `CommanderMode::temp`
(a `tempfile::TempDir`) until the destination queue is **applied**, because the staged
`AddFile` references them by path. Applying the queue (or discarding it) is when the
temp entries can be reclaimed.

---

## 6. Drag-to-load and drag-to-import

The app already enables OS drag-drop. While Commander is open, intercept
`ctx.input(|i| i.raw.hovered_files / i.raw.dropped_files)` and route by **pointer x**
(left half → left pane, right half → right pane). The hovered pane gets a highlighted
border + an overlay hint.

Routing rule for a drop on a pane:

1. **Pane is Empty**, or the dropped path has a **disk-image extension**
   (`model::file_types::DISK_IMAGE_EXTS`), or is a **folder** →
   **load it as the pane's source** (image → open + partition dropdown; folder → host
   pane). This is the "load one on the left, one on the right by dragging" flow.
2. **Pane already shows an image volume** and the drop is ordinary host files →
   **stage an import-copy** into the current directory (the host→image path from §5).

The mock wires the real `hovered_files`/`dropped_files` signals so dragging a file
onto a pane highlights it and "loads" a placeholder volume named after the file.

---

## 7. Staging, atomicity, and the unsaved guard

Each **image** pane owns its own `EditQueue`. Copy-into and delete-on an image pane
push to that pane's queue; nothing touches the disk until **Apply**.

- **Per-pane Apply / Discard** live in the bottom bar: `L: N staged  [Apply] [Discard]`
  and the mirror for R, each with a live **projected free-space** readout
  (`EditQueue::space_delta()`, green/yellow/red) exactly like the browse toolbar.
- **Apply** opens `session.open_editable()` → `(efs, commit)`, `drain()`s the queue,
  loops `edit_queue::apply_edit`, calls `efs.sync_metadata()`, drops `efs`,
  `commit.commit()` (re-encodes the container), then invalidates caches and re-lists.
  This is exactly `browse_view::apply_staged_edits` and should be lifted to a shared
  helper both call.
- **Virtual overlay** while staged: pending adds render green with a `+` prefix,
  pending deletes render dimmed + strikethrough — same convention as the browse view.
- **Unsaved-changes guard** — the same Discard / Apply / Cancel modal
  (`browse_view::render_unsaved_dialog`) fires when:
  - the user clicks **Close** with either queue non-empty, or
  - the user **switches a pane's source** (Open... / drop / new partition) with that
    pane's queue non-empty.
  Host panes never stage (immediate writes), so they never trigger this guard.

### 7.1 Both panes on the same volume (edge case)

If both panes resolve to the *same* `(image file, partition offset)`, two independent
queues could conflict. v1 rule: detect identical sources and **share a single queue**
between the panes (copy becomes an in-volume copy/duplicate), and after any Apply on a
shared image, **re-list both panes**. Document the rule; keep it simple.

---

## 8. Delete semantics

- **Image pane** → stage `DeleteEntry` / `DeleteRecursive` (recursive for non-empty
  dirs), shown via the strikethrough overlay; committed on Apply. Deleting a
  not-yet-applied pending add just unstages it.
- **Host pane** → immediate `std::fs::remove_file` / `remove_dir_all` behind a
  confirm modal (host deletes can't be meaningfully staged/undone).

---

## 9. File-detail / Inspect floating pane

**Double-clicking a file** (left button — reliable on every platform) opens a floating
`egui::Window` ("File Info: NAME"), built on the shared `file_detail` module (§3).
It is also reachable from the **right-click context menu** (§9a, "Info / Details") and
a **Details** button in the middle column. The window shows every metadata field the
entry's filesystem populates, plus a hex/text preview (reuses `render_hex_view` +
`detect_content_type`, capped at the existing 1 MB preview limit).

Displayed (read or read/write per §10):

- Always: name, full path, size, entry type, modified.
- HFS/HFS+/MFS: type code, creator code, resource-fork size. **Editable** (type/creator
  via `set_type_creator`; HFS also dates via `set_dates`, Finder flags via
  `set_finder_info`).
- ProDOS: type (`$xx`), aux type, access bits. **Editable** (`set_prodos_type` /
  `set_prodos_access`).
- Unix (ext/btrfs/xfs/ufs/jfs/reiserfs): mode (rwx), uid, gid. **Editable on ext only**
  (`set_permissions`); read-only elsewhere in v1.
- Amiga (AFFS/PFS3/SFS): protection word, comment, datestamp. **Read-only in v1.**
- FAT/exFAT: archive/hidden/system/read-only attribute bits. **Read-only in v1.**

Edits made here are **staged** onto the owning pane's queue (not written immediately),
consistent with §7. Two new `StagedEdit` variants + `apply_edit` arms are needed to
carry these (see §10.2): `SetPermissions` and `SetDates` (the trait setters already
exist; only the queue variants are missing). `SetTypeCreator`, `SetProdosType`,
`SetProdosAccess` already exist in the queue.

---

## 9a. Selection & interaction model (resolved in the mock)

These are settled from iterating on `examples/commander_mock.rs`:

- **Multi-select** — plain click selects one row; **Ctrl/Cmd-click** toggles a row in/out
  of the selection; **Shift-click** selects a contiguous range from the anchor row.
  Use egui's cross-platform `Modifiers::command` (= Cmd on macOS, Ctrl on Windows/Linux),
  read at click time, plus `Modifiers::shift`. Copy / Delete / Export all act on the whole
  selection; the selection lives per pane (`Pane::selected: Vec<id>` + an `anchor`).
- **De-select** — click empty space in a pane clears its selection (clicking a selected
  row again is *not* a de-select, because a fast second click registers as a double-click).
- **Double-click** — a directory enters it; a file opens File Info (§9).
- **Right-click → context menu** (not auto-open). The menu acts on the selection when the
  clicked row is part of it, otherwise on just that row. Items (those marked **[§15]**
  are the planned batch, not yet built):
  - **Copy to {other} pane** (→ stage onto the other pane's queue, §5) — *built*
  - **Delete** / **Undelete** / **Remove from staging** (the one toggle, below) — *built*
    (image panes stage; host panes delete immediately behind a confirm)
  - **Rename…** **[§15.1]** — opens a name dialog; staged on image panes, immediate on host
  - **Calculate Checksums…** **[§15.2]** — opens a window showing CRC32 / MD5 / SHA1 (+ SHA256)
  - **Export to hard drive…** **[§15.3]** — pick a host folder; immediate extraction
  - **Info / Details** (§9)
  - **Export…** submenu (§9b — archive formats: zip / tgz / sit)
- **Delete is a single toggle** (`toggle_delete`): on a normal entry it stages a delete;
  on an entry already staged for delete it **undeletes** (un-stages); on a staged copy /
  new folder it **drops it from the list**. This is the "undelete" behaviour — no separate
  button needed. A `Delete / Undelete` button in the middle column does the same on the
  active pane's selection.
- **New Folder** — middle-column button (and a future menu item) opens a name dialog and
  stages a `CreateDirectory` (shown as a green pending add). Delete on it un-stages.
- **Active pane** — the middle column (single Delete, New Folder, Details) acts on the
  last-interacted pane; the copy controls are explicitly directional (L→R / R→L).
- **egui id gotcha** — Commander renders two near-identical panes in one `Ui` tree, so
  every per-pane stateful widget (`ScrollArea`, `ComboBox`, `CollapsingState`, `Grid`)
  **must** take a side-keyed `id_salt`, or egui raises an "ID clash" and one pane's widget
  ghosts the other. (Hit during mock bring-up.)

## 9b. Export & archiving (out to host)

"Archive files out" = take a pane selection and write a host-side archive (an
image→host export, §5). Surfaced in two places already mocked: the **Export…** submenu
in the right-click menu and the **Export** section of the File Info window.

Crate inventory — **most of this is already vendored**:

| Target | Crate / source | Status |
|--------|----------------|--------|
| `.zip` (deflate, AES) | **`zip` 8** (`deflate`, `aes-crypto`) | **present** |
| `.gz` / raw zlib | **`flate2`** (zlib-ng backend) | **present** |
| `.bz2` | **`bzip2` 0.6** | **present** |
| `.zst` / seekable zstd | **`zstd` 0.13**, **`zeekstd` 0.6** | **present** |
| `.tar` and therefore `.tar.gz` / `.tar.bz2` / `.tar.zst` | **`tar`** crate (tar container) + the codecs above | **add `tar`** (small, MIT/Apache; gz/bz2/zst already here) |
| `.sit` (StuffIt) | **internal** `src/macarchive/` (arsenic/huffman/lzw) + `sit` CLI verb | **present** |
| BinHex `.hqx` | **internal** `src/cli/verbs/binhex.rs` | **present** |
| `.7z` (LZMA/LZMA2) | **`sevenz-rust2`** (pure-Rust, Apache-2.0) | **add only if wanted** |
| `.tar.xz` | **`xz2`** / liblzma | not present; add only if wanted |

Recommendation for v1 archiving:

- **`.zip`** — universal, already have the crate. Default for non-Mac selections.
- **`.tar.gz`** — add the tiny `tar` crate (gzip already via `flate2`). The Unix-friendly
  option; preserves mode/uid/gid/symlinks, which `tar` carries natively.
- **`.sit` (StuffIt)** — the right choice for **HFS/HFS+/MFS** selections because it
  preserves the **resource fork + type/creator** that zip/tar drop. Reuse the existing
  `src/macarchive/` writer (the `sit` CLI verb already builds archives) — no new crate.
- **`.7z`** — defer; add `sevenz-rust2` only if a user asks. zip + tgz + sit cover the ask.

Mechanics: archiving streams each selected entry's data fork (and, for the StuffIt path,
its resource fork + Finder info) through the chosen encoder to a host file chosen via the
picker. It is an **immediate** host write (no staging), like other image→host exports
(§5). For large selections, run on the existing worker-thread pattern (§4.3) with progress.

---

## 10. Editable-metadata inventory (the check-off list)

This is the master list of "what can we edit, and what's missing." v1 ships the
**Settable today** column; the **Gap** rows are future work, each an
`EditableFilesystem` trait method (and usually a `StagedEdit` variant + `apply_edit`
arm + a `metadata_editor` widget).

### 10.1 Per-field status

Legend: **R** = read/displayed today · **W** = in-place setter exists today · **gap** =
needs a new setter.

| Metadata field | Filesystems that read it | In-place setter today | v1 editable? | Gap to add |
|----------------|--------------------------|------------------------|--------------|------------|
| name (rename) | all | — none | no | `rename(parent, entry, new_name)` trait method + `StagedEdit::Rename` |
| modified (mtime) | most | — (HFS via `set_dates` only) | HFS only | generic `set_modified_time(entry, t)` for FAT/exFAT/NTFS/ext/Amiga/... |
| created date | (on disk for HFS, NTFS, exFAT) | HFS `set_dates` | HFS only | surface on `FileEntry` (not present today) + setters for NTFS/exFAT |
| backup date | HFS | HFS `set_dates` | HFS only | surface on `FileEntry` (not present today) |
| HFS/HFS+ type code | HFS, HFS+, MFS | `set_type_creator` ✓ | **yes** | — (queue variant `SetTypeCreator` exists) |
| HFS/HFS+ creator code | HFS, HFS+, MFS | `set_type_creator` ✓ | **yes** | — |
| HFS Finder flags (FInfo/FXInfo) | HFS | `set_finder_info` ✓ (classic HFS only) | yes (HFS) | `set_finder_info` for HFS+ ; `StagedEdit::SetFinderInfo` |
| resource fork | HFS, HFS+ | `write_resource_fork` ✓ | yes | — (carried by `AddFile`) |
| ProDOS type / aux | ProDOS | `set_prodos_type` ✓ | **yes** | — (`SetProdosType` exists) |
| ProDOS access bits | ProDOS | `set_prodos_access` ✓ | **yes** | — (`SetProdosAccess` exists) |
| Unix mode (rwx) | ext, btrfs, xfs, ufs, jfs, reiserfs | `set_permissions` ✓ **ext only** | ext only | `set_permissions` for btrfs/xfs/ufs/jfs/reiserfs ; `StagedEdit::SetPermissions` |
| Unix uid / gid | ext, btrfs, xfs, ufs, jfs, reiserfs | — none | no | `set_owner(entry, uid, gid)` across the Unix family |
| Amiga protection word | AFFS, PFS3, SFS | — none (create-time only) | no | `set_amiga_protection(entry, w)` |
| Amiga comment (filenote) | AFFS, PFS3, SFS | — none (create-time only) | no | `set_amiga_comment(entry, s)` |
| Amiga datestamp | AFFS, PFS3, SFS | — none (create-time only) | no | `set_amiga_dates(entry, (d,m,t))` |
| FAT/exFAT attribute bits | FAT, exFAT | — none (create-time only) | no | `set_dos_attributes(entry, bits)` |
| volume name | HFS (+others) | `set_volume_name` ✓ (classic HFS) | (volume-level) | `set_volume_name` for HFS+/FAT/exFAT/... |

### 10.2 Concrete to-do checklist

v1 (needed for the detail-pane editing we promised) — **done**:

- [x] Added `StagedEdit::SetPermissions { entry, mode }` + `apply_edit` arm (calls
      `set_permissions`). Wired ext via the `metadata_editor` permissions row.
- [x] Added `StagedEdit::SetDates { entry, create, modify, backup }` + `apply_edit`
      arm (calls `set_dates`). Wired HFS via the `metadata_editor` dates row.
- [x] Surfaced HFS create/modify/backup dates on `FileEntry::mac_dates` (raw Mac-epoch
      triple), decoded from the catalog in `hfs.rs`; `hfs_common::{format_mac_date,
      parse_mac_date}` convert to/from the `YYYY-MM-DD HH:MM:SS` display/edit string.
      (Chose the `FileEntry` field over a `file_detail`-only fetch — it serves both
      display and round-trip editing.)

Future phases (each unchecked = one driver-spanning task):

- [ ] `rename` trait method + `StagedEdit::Rename` (full design in §15.1).
- [ ] generic `set_modified_time`.
- [ ] `set_dos_attributes` (FAT, exFAT) + queue variant + editor widget.
- [ ] `set_amiga_protection` / `set_amiga_comment` / `set_amiga_dates` (AFFS, PFS3, SFS).
- [ ] `set_owner(uid, gid)` (ext, btrfs, xfs, ufs, jfs, reiserfs).
- [ ] `set_permissions` on the non-ext Unix filesystems.
- [ ] `set_finder_info` for HFS+.
- [ ] `set_volume_name` for HFS+/FAT/exFAT.

---

## 11. Compare (deferred)

A future "Compare" action over two selected files (or the two panes' current
directories): byte/structure diff for files, present-on-left / present-on-right /
differ for trees. Out of v1 scope; called out here so the layout reserves a (disabled)
"Compare" affordance in the middle column and so the listing model can later expose
per-row diff status. Re-spec when we get there.

---

## 12. Constraints & edge cases

- **No Unicode glyphs** (CLAUDE.md): ASCII only in all Commander chrome — `..`,
  `<DIR>`, `->`, `+`, `^`/`v` for sort, `A H S R` for FAT attrs. Filenames read from a
  filesystem render verbatim (the one allowed exception; CJK covered by the bundled
  font).
- **Read-only sources**: physical devices and containers we can open but not write
  (or opened read-only) disable the staging affordances; copy *out* still works,
  copy *in* / delete are greyed.
- **Large directories**: list on a worker thread; consider windowed rendering
  (`ScrollArea::show_rows`) if a directory has thousands of entries.
- **Symlinks / special files**: show with `-> target` / `(special)`; copying them
  cross-volume is best-effort (data only) with a warning; not v1-critical.
- **Host traversal**: a host pane may navigate above its initially-opened folder via
  `..`; that's intended.

---

## 13. Milestones

1. **M0 — mock** (done): `examples/commander_mock.rs`, layout & interaction agreed.
2. **M1 — refactor R0/R1** (**done**): extracted `file_detail` + `metadata_editor`
   (R0) and `source_picker` (R1, §3.2); the browse view, the Inspect tab, and both
   Commander panes adopt them. R4 (browse-view tree dedup) is still deferred.
3. **M2 — read-only Commander** (**done**, M2-lite): overlay, two panes, source open
   (picker), `DirListing`, columns, sort, `..` nav.
4. **M3 — staged writes** (**done**): per-pane `EditQueue`, copy (image↔image staged),
   delete, Apply/Discard, unsaved guard, virtual overlay.
5. **Host panes** (**done**): host-folder source, all four copy combos (host→image staged;
   image→host / host→host immediate threaded), immediate host delete.
6. **M4 — editable subset** (**done**): the floating File Info window (§9) — metadata
   rows + text/hex preview + the editable subset (HFS type/creator via the extracted
   widget; HFS dates and ext permissions via the new `SetDates` / `SetPermissions`
   queue variants; ProDOS type), staged onto the owning pane's queue. HFS catalog
   dates are surfaced on `FileEntry::mac_dates`.
7. **M6 — right-click action batch** (§15, **done**): **Rename** (§15.1), **Calculate
   Checksums** (§15.2), **Export to hard drive** (§15.3). The per-pane tree (§15.4)
   shipped then was **removed** as low-value; R4 will reuse the browse view's tree.
8. **Phase 3 — `.adz`/`.hdz` editable gzip containers** (**done**): `.adz`/`.hdz`
   open as gzip containers (peeled for reading via `source_reader::is_gzip_image_path`
   + `open_read`; re-gzipped on edit via `container_edit`'s `EditFormat::Gzip`), no
   longer materialized to a throwaway `.adf`/`.hdf`.
9. **Round-1 / round-2 hands-on-test fixes** (**done**): New Folder (both panes);
   "Keep original dates" on copy (`PreservedDates` — Amiga + HFS); staged-metadata
   shown blue + `* ` and reflected in the editor; specific date-validation errors;
   pending-edits list; macOS device elevation (`open_device_for_inspect`); AFFS
   large-file read (`T_LIST`); icon middle-buttons; `.sit` dropped from the Commander
   picker; deferred-switch unsaved guard + consolidated Close in Inspect.
10. **M7 — find / search** (§15.5, deferrable): wildcard name search per pane.
11. **M5+ / round-3** — functional center Delete (active-pane); `.sit`→Archives
    redirect; rolling applied-ops log; **Phase 2** "Open Backup…" + physical-device
    parity in Commander panes; the R4 tree dedup (§3.3); a per-FS editable-metadata
    matrix; Compare; the broad metadata-setter backlog (§10.2).

---

## 14. Open questions

- **Multi-select semantics** — *resolved* (§9a): Ctrl/Cmd-toggle + Shift-range via
  `Modifiers::command`. **Shipped.**
- **Archive formats beyond zip / tgz / sit** — ship `.7z` (`sevenz-rust2`) and/or
  `.tar.xz` (`xz2`), or defer until asked? (Inventory in §9b; archive export itself
  is not built yet.)
- **Host→image resource forks** — when a host file has an AppleDouble (`._name`) or a
  macOS named fork, do we import the resource fork? (Probably yes on macOS, via the
  existing `ResourceForkSource`.) **Shipped data-fork-only; fork import still open.**
- **Image→host resource forks** — write an AppleDouble sidecar, a macOS named fork, or
  drop the fork with a warning? **Shipped drop-with-warning; sidecars still open**
  (affects §15.3 Export to hard drive too).
- **Rename scope** (§15.1) — single-selection only, or batch/templated rename later?
- **Checksum set** (§15.2) — is CRC32 / MD5 / SHA1 / SHA256 the right default set, or
  add BLAKE3? Do we offer per-file rows for a multi-selection, or a combined manifest?
- **Tree view model** (§15.4) — fold the lazy tree into `DirListing`, or a sibling
  `DirTree`? (Ties into the R4 browse-view share.)
- **Function-key bar** — do we also want a classic MC F5/F6/F8 keyboard map, or are
  the middle buttons + right-click enough?
- **Persisting pane state** — remember last-opened sources / column widths across
  sessions, or always start empty?

---

## 15. Right-click actions, tree view, and search (planned batch)

This is the next batch of user-facing features (milestone M6, plus the deferrable
M7 search). All build on the engine/model that already shipped — `DirListing`
(image + host listing sources), `commander_ops` (apply / stage_copy /
stage_host_to_image / spawn_host_copy), and the per-pane `EditQueue`. Build
model-first per CONTRIBUTING (engine → model → thin view), unit-testing each new
model piece before wiring the menu item.

### 15.1 Rename

Rename the selected entry. There is **no `rename` on `EditableFilesystem` today**
(confirmed gap, §10), so this needs an engine-layer addition:

- **Engine** — `EditableFilesystem::rename(parent, entry, new_name) -> Result<()>`,
  with a per-FS implementation (in-place directory-entry rewrite; FAT rewrites the
  SFN/LFN set, HFS/HFS+ re-keys the catalog record, ext rewrites the dirent, etc.).
  Start with the filesystems users will hit first (FAT/exFAT/HFS/HFS+/ext); other
  drivers return `Err(Unsupported)` so the menu item can gray out.
- **Model** — `StagedEdit::Rename { parent, entry, new_name }` + an `apply_edit` arm
  calling `efs.rename(...)`. The virtual overlay shows the row under its new name
  (and the old name struck through, optional).
- **View** — a "Rename…" name dialog (reuse the New-Folder dialog shape), validating
  against `Filesystem::validate_name` and the destination's duplicate-name rule.
  - **Image pane** → stage `Rename` (applied on Apply).
  - **Host pane** → immediate `std::fs::rename` (no staging), then re-list.
- **Interim fallback** (if the trait method is deferred): rename via copy-to-new-name
  + delete-old on the *same* pane. Loses in-place semantics and is slower, so prefer
  the real trait method; document the fallback only as a stopgap.

Single-selection only (multi-rename / templated rename is out of scope).

### 15.2 Calculate Checksums

Open a small window ("Checksums: NAME") showing, for the selected file(s),
**CRC32**, **MD5**, **SHA1**, and **SHA256** as hex, with a copy-to-clipboard
button per row. For a multi-selection, show one row per file (and skip
directories, or recurse with a per-file breakdown — pick per-file rows).

- **Crates** — `crc32fast`, `md-5`, and `sha2` (SHA256) are **already vendored**;
  **add the `sha1` crate** (tiny, RustCrypto, same family as `md-5`/`sha2`). The
  existing `backup::verify::RunningHasher` only does CRC32/SHA256 and is
  backup-specific — do **not** overload it.
- **Model** — `model::checksum::hash_reader(reader, &dyn Fn(progress)) -> ChecksumSet`
  that streams the data **once** (64 KiB–1 MiB chunks) feeding all four hashers in
  parallel; `ChecksumSet { crc32, md5, sha1, sha256 }` with hex accessors. Source
  data comes from `Filesystem::write_file_to` (image) or `std::fs::File` (host).
  Unit-test against known vectors (e.g. the empty input and `"abc"`).
- **Threading** — large files: run on a worker thread behind a `ChecksumStatus`
  (the established Status pattern), polled each frame with a spinner / progress
  bar. The window shows "computing…" until done.
- **View** — `egui::Window` with a 4-row grid (algorithm → value), monospace values,
  a Copy button each. Reachable from the right-click menu and (later) the File Info
  window's detail rows.

### 15.3 Export to hard drive

A first-class "Export to hard drive…" menu action that writes the selection to a
host folder the user picks (rfd `pick_folder`), regardless of what the *other*
pane is pointing at. This is the §5 image→host (and host→host) export surfaced
directly:

- **Reuse** — exactly the H2 host-write engine: `commander_ops::spawn_host_copy`
  with `HostCopyJob::ImageToHost { session, entries, dest_dir = picked }` for an
  image source, or `HostToHost { entries, dest_dir }` for a host source. Threaded,
  with the same `HostCopyStatus` poll + completion toast Commander already uses.
- **Distinction from §9b** — §9b *archives* the selection into a single container
  file (zip / tgz / sit). §15.3 writes the selection out as **plain files/folders**
  into a chosen directory. Both are immediate host writes; the menu offers both
  ("Export to hard drive…" = loose files; "Export…" submenu = archive).
- **Forks** — data fork only for v1 (note metadata loss in the status line, as the
  copy engine already does); AppleDouble/MacBinary sidecars are a later option
  (open question, §14).

### 15.4 Per-pane tree view

A **tree toggle** button on each pane's source bar switches that pane between the
flat single-directory grid (today) and a hierarchical, lazily-expanding folder
tree — the classic file-manager left-tree navigation.

- **Reuse** — the browse view already implements exactly this: `directory_cache:
  HashMap<String, Vec<FileEntry>>` + `expanded_paths: HashSet<String>` +
  `render_tree_entry` (egui `CollapsingState`) in `gui/browse_view.rs`. The tree
  state belongs in the model, so this is the **R4** refactor from §3.3: lift the
  lazy-tree cache into `DirListing` (or a sibling `DirTree` model) keyed by the same
  `ListingSource`, and have both the browse view and Commander render over it. The
  per-`CollapsingState` `id_salt` must be side-keyed (the two-pane id gotcha, §9a).
- **Behaviour** — clicking a folder in the tree sets the flat grid's cwd (tree for
  navigation, grid for the working set), or the tree fully replaces the grid while
  toggled on — pick the "tree navigates, grid is the working set" split (matches MC
  and keeps copy/delete acting on a single directory's selection).
- **Scope note** — this is the largest item in the batch because of the shared-model
  refactor; it can land after Rename/Checksums/Export if we want quicker wins first.

### 15.5 Find / search (deferred — M7)

A per-pane search box that finds files by name with shell wildcards (`*`, `?`,
character classes). **Deferrable** — not needed for the core file-manager loop.

- **Model** — `model::find::search(source, root, pattern, &dyn Fn(progress)) ->
  Vec<FileEntry>`: a recursive walk over the listing source (image fs or host
  `std::fs`) matching each name against a compiled glob. Use a small glob matcher
  (the `globset` crate, or a hand-rolled `*`/`?` matcher — no new dep needed for the
  simple two-wildcard case). Threaded for large trees (Status pattern), cancelable.
- **View** — a search field in the source bar (or a Ctrl/Cmd-F popup); results show
  in the grid as a flat list with their full paths, selectable for copy/delete/export
  like any other selection. Double-click jumps to the containing directory.
- **Why deferred** — it needs a results-list view mode distinct from the cwd grid,
  and a recursive-walk worker; none of the M6 items depend on it.
