# Source-Resolution Unification (Inspect ⇄ Commander ⇄ CLI ⇄ TUI)

**Goal.** One source-resolution model that every front end shares: "a user picks
a source (device / image / backup folder) → list its partitions (with
capabilities) → open a chosen partition." The GUI surfaces (Inspect tab,
Commander Mode panes) must end up **fully unified** over that one model + the
shared `BrowseSession`, and the model must be reusable by a future **TUI**
(`src/tui/`) with no new resolution code. The **CLI** shares the *engine* and may
opt into the resolver for features it gains, but legitimately keeps its own
thin, synchronous one-shot resolver (`cli/resolve.rs`) — it returns a plain
reader, not an async `BrowseSession`, and that shape difference is accepted.

This plan was triggered by two bugs (Commander couldn't open a Clonezilla
backup; it auto-opened an APM `Apple_Driver_IOKit` driver partition and errored)
that were both symptoms of source-resolution logic trapped in the Inspect *view*
rather than a shared model. The governing rule — the litmus test ("could the
CLI / a headless test / a TUI call this without egui/crossterm?") — lives in
[`CONTRIBUTING.md`](../CONTRIBUTING.md) § "Presentation layers: one model, many
UIs".

## Current state: one engine, (still) three orchestrations

Everything bottoms out on the same **engine** (`fs::open_filesystem`,
`PartitionTable::detect`, `source_reader` container peeling,
`backup_loader::load_backup`). The *orchestration* on top is what's only
partially unified:

| Concern | CLI | Inspect | Commander |
|---|---|---|---|
| FS dispatch / partition parse / container peel | shared | shared | shared |
| Capability gates (`fs::partition_is_browsable`) | — (doesn't gate) | ✅ | ✅ |
| `BrowseSession` (async open primitive) | — (sync reader) | ✅ | ✅ |
| Backup resolver (`commander_source::resolve_backup`) | — | ❌ **Phase 4** | ✅ |
| Cache-scan runner (`model::cache_runner`) | — | ✅ | ✅ |
| Resolution orchestration lives in | `cli/resolve.rs` | `inspect_tab::open_browse` (inline) | `model::commander_source` |

The end-state collapses the two GUI columns into the Commander column (the
shared model) and leaves the CLI column as a deliberately leaner sibling.

## Done

- **Phase 1 — capability gates → engine** (`3a4c208`). Moved `is_browsable_type`
  / `is_browsable_type_string` / `is_browsable_superfloppy` / `is_checkable_type`
  / `is_classic_hfs` / `is_superfloppy_hfs` out of `gui/inspect_tab.rs` into
  `fs/`, plus a combined `fs::partition_is_browsable`. Inspect + Commander call
  it; Commander skips non-filesystem partitions. **Fixes the APM-driver bug.**
- **Layering rule** (`e188812`). CONTRIBUTING "one model, many UIs" section.
- **Phase 2 — shared backup resolver** (`60548a2`). `commander_source::
  resolve_backup` → `ResolvedBackup` (Native | Clonezilla) via
  `backup_loader::load_backup`. **Fixes the Clonezilla "no metadata.json" bug.**
- **Phase 3 — cache-scan runner** (`3e5fc5a`). `model::cache_runner::
  spawn_partclone_scan` extracted from the Inspect view; Inspect *and* Commander
  share it. Commander browses Clonezilla end-to-end.
- **CHD backups** (`c1a7c2a`). `ResolvedBackup::open_partition` handles the
  single-file-chd layout (open the `.chd` container at the partition offset).
  **Fixes "partition 0 has no data files listed".**

## Remaining work

### Phase 4 — Inspect adopts the resolver (the GUI unification)

Inspect already shares the loader (`load_backup`), the gates (Phase 1), and the
cache runner (Phase 3). The remaining duplication is the **per-partition open
routing** in `inspect_tab::open_browse` (+ `open_browse_zstd` /
`open_browse_clonezilla`). To unify without regressing Inspect, do all of:

1. **`BrowseView::open_with_session(session: BrowseSession)`** — `BrowseView`
   already owns a `BrowseSession` and `open()` just fills its fields + calls
   `spawn_open()`; add the seam that takes a prebuilt session.
2. **Extend the resolver to every compression** Inspect supports, not just the
   read-only none/zstd subset: `chd` / `chd-dvd` (per-partition, if any still
   exist) + `woz`, and the **zstd seekable-cache upgrade** (Inspect opens
   streaming, then swaps to a background-built seekable cache — see
   `open_browse_zstd`). Today `session_for_backup_partition` covers none/zstd
   streaming only.
3. **Preserve Inspect's edit contexts** layered on top of the session:
   `set_archive_edit_context` (zstd/woz), the `chd_edit` flow, and
   `single_file_chd_backup_folder` (metadata refresh).
4. Route `open_browse` through `resolve_backup` + `ResolvedBackup::open_partition`
   → `browse_view.open_with_session(...)`, then delete the inline per-compression
   ladder.

**Blocked on:** needs the running GUI to validate every backup compression type
(none / zstd / chd / woz / single-file-chd) + Clonezilla still browse and edit
correctly. Do it with that validation in hand. This is the step that makes
Commander and Inspect *the same path*.

### Straggler A — unify image partition probing (3 copies → 1)

"Peel container → detect table → list partitions" is implemented three times:
`commander_source::probe_partitions`, `inspect_tab` (inline `PartitionTable::
detect` with image-wrapper handling, ~`inspect_tab.rs:2860`), and `cli/resolve.rs`
(inline `PartitionTable::detect`, ~`:201`). Promote one shared model fn (extend
`commander_source::probe_partitions`, or a `model::source` probe) that all three
call. This is what makes **every image container — CHD, GHO, IMZ, VHD, … —
probe identically everywhere** (see the `.GHO` note below).

### Straggler B — unify image per-partition session building

Inspect builds the open via `browse_view.open(path, offset, type, …)`; Commander
via `commander_source::session_for(path, part)`. Both produce a `BrowseSession`.
Fold Inspect onto `session_for` + `BrowseView::open_with_session` (rides on
Phase 4 / Straggler A).

### Straggler C — unify the Clonezilla cache-lookup decision tree

Inspect's `open_browse_clonezilla` holds the full decision tree: an in-memory
`block_caches: HashMap` hit, a persisted `_<device>.metadata.cache` load, then a
scan. Commander's resolver has the simpler `load_partclone_cache` (disk-or-scan).
Lift the whole "ready-in-memory? on-disk? else scan" decision into the resolver /
`cache_runner` so both share it (and Commander gains the in-memory reuse).

### Straggler D — single-file-chd: converge the two approaches

Inspect redirects the whole source to `<folder>/<container>` and inspects the
`.chd` as an image (`single_file_chd_backup_folder`); Commander opens the
container per-partition in `ResolvedBackup::open_partition`. Pick one (the
resolver path) once Phase 4 lands.

## Not a straggler / out of scope

- **`.GHO` files.** `.gho` / `.ghs` are disk-image **containers** (sector-mode
  Ghost), already in the picker (`DISK_IMAGE_EXTS`) and already peeled by the
  shared `source_reader` / `BrowseSession` path in **both** Inspect and
  Commander — so a `.gho` image already opens in a Commander pane today. **Phase
  4 does not touch it** (Phase 4 is backup-folder routing). What would make GHO
  (and every image container) provably-identical across all front ends is
  **Straggler A**, not Phase 4. Caveat: *file-aware* GHOs (virtual-FAT, not a raw
  sector stream) — `GhoReader::open` may refuse those; a format limitation, not a
  path issue.
- **CLI source resolution** (`cli/resolve.rs`). Stays a separate, synchronous,
  one-shot resolver by design (returns a reader, not a `BrowseSession`). It
  should *call into* the shared model when it gains a feature the model already
  has (browsable gating, a `rb-cli browse <clonezilla-folder>` verb → reuse
  `resolve_backup` + `cache_runner`), per the CONTRIBUTING litmus test — but it
  is not required to adopt `BrowseSession`.
- **Commander device parity** — deferred, **hardware-gated** behind the macOS
  device-elevation verify (`ff36fa3`). Tracked in
  [`commander_mode_handoff.md`](commander_mode_handoff.md).

## Acceptance criteria ("done")

- [ ] Inspect and Commander open every source (device / image / native backup /
      Clonezilla / single-file-chd) through **one** resolver + `BrowseSession`;
      `inspect_tab::open_browse`'s per-compression ladder is gone (Phase 4 + B +
      D).
- [ ] Image partition probing is one shared fn (Straggler A); CLI calls it too.
- [ ] The Clonezilla cache decision tree is in the model, shared (Straggler C).
- [ ] A new `src/tui/` could list + open any source using only `model::` —
      no resolution code of its own.
- [ ] CLI keeps `cli/resolve.rs` but re-uses the gates / `resolve_backup` /
      `cache_runner` for any browse-shaped feature it adds.
