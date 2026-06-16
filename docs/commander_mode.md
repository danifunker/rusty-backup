# Commander Mode — Design & Implementation Plan

Status: **Planning** (no code yet beyond the runnable mock)
Last updated: 2026-06-15
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

- **R0** — Extract `file_detail` + `metadata_editor` + size/date helpers (pure moves).
  Browse view keeps working identically. *Ship, verify, then continue.*
- **R1** — Extract `source_picker` from `inspect_tab`; Inspect tab adopts it.
- **R2** — Add `DirListing` (new model) + the async listing worker.
- **R3** — Build `CommanderPane` / `CommanderMode` on R0–R2.
- **R4** — (optional, later) migrate `browse_view`'s tree to share `DirListing` for
  its lazy children, retiring duplicate `directory_cache` logic.

---

## 4. The pane: source, listing, navigation

### 4.1 Source bar (top of each pane)

`[ Open... ]  [ partition v ]   <volume label>   free: <n>`

- **Open...** → shared `source_picker`: pick a physical device, a disk image, or a
  host folder (rfd `pick_file` / `pick_folder`).
- **partition dropdown** — populated after an image is opened (reuses Inspect's
  partition parse). Selecting one (re)opens the pane's `BrowseSession` at that
  partition's offset/type. Host panes hide the dropdown.
- Right side shows volume label + free space (`free_space()` for editable images).

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
  clicked row is part of it, otherwise on just that row. Items:
  - **Copy to {other} pane** (→ stage onto the other pane's queue, §5)
  - **Delete** / **Undelete** / **Remove from staging** (the one toggle, below)
  - **Info / Details** (§9)
  - **Export…** submenu (§9b)
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

v1 (needed for the detail-pane editing we promised):

- [ ] Add `StagedEdit::SetPermissions { path, mode }` + `apply_edit` arm (calls
      `set_permissions`). Wire ext.
- [ ] Add `StagedEdit::SetDates { path, create, modify, backup }` + `apply_edit` arm
      (calls `set_dates`). Wire HFS.
- [ ] Surface HFS create/modify/backup dates so the detail pane can show + edit them
      (either extend `FileEntry` with optional `created`/`backup` strings, or a
      `file_detail`-only HFS date fetch). Pick one in R0.

Future phases (each unchecked = one driver-spanning task):

- [ ] `rename` trait method + `StagedEdit::Rename`.
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
2. **M1 — refactor R0/R1**: extract `file_detail`, `metadata_editor`, `source_picker`;
   browse view + Inspect adopt them unchanged.
3. **M2 — read-only Commander**: overlay, two panes, source open (picker + drag-load),
   `DirListing`, columns, sort, `..` nav, detail window (display only). No writes.
4. **M3 — staged writes**: per-pane `EditQueue`, copy (all four backend combos),
   delete, Apply/Discard, projected free space, unsaved guard, virtual overlay.
5. **M4 — editable subset**: detail-pane editing for HFS/ProDOS/ext (+ the two new
   `StagedEdit` variants from §10.2).
6. **M5+** — Compare; the broad metadata-setter backlog from §10.2.

---

## 14. Open questions

- **Multi-select semantics** — *resolved* (§9a): Ctrl/Cmd-toggle + Shift-range via
  `Modifiers::command`, lands in M2.
- **Archive formats beyond zip / tgz / sit** — ship `.7z` (`sevenz-rust2`) and/or
  `.tar.xz` (`xz2`) in v1, or defer until asked? (Inventory in §9b.)
- **Host→image resource forks** — when a host file has an AppleDouble (`._name`) or a
  macOS named fork, do we import the resource fork? (Probably yes on macOS, via the
  existing `ResourceForkSource`.) Decide in M3.
- **Image→host resource forks** — write an AppleDouble sidecar, a macOS named fork, or
  drop the fork with a warning? Decide in M3.
- **Function-key bar** — do we also want a classic MC F5/F6/F8 keyboard map, or are
  the middle buttons enough for v1? (Mock uses buttons only.)
- **Persisting pane state** — remember last-opened sources / column widths across
  sessions, or always start empty?
