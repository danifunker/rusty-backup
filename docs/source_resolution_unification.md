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
| Backup resolver (`commander_source::resolve_backup`) | — | ✅ (Phase 4) | ✅ |
| Cache-scan runner (`model::cache_runner`) | — | ✅ | ✅ |
| Clonezilla cache decision tree (`PartcloneCacheStore`) | — | ✅ (Straggler C) | ✅ (Straggler C) |
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
- **Phase 4 — Inspect adopts the resolver** (`8942090`, `43e89a5`, `fa17583`,
  `473ac50`, `1eeb48c`, `24abc2c`). *Implemented; pending GUI validation.* The
  steps below all landed:
  1. `BrowseView::open_with_session(session)` — the seam `open` /
     `open_streaming` / `open_partclone` now funnel through.
  2. `session_for_backup_partition` builds a session for **every** per-partition
     compression Inspect browses (none / chd / chd-dvd / woz on `source_path`;
     zstd streaming). `single_data_file` shares the data-file lookup + split /
     exists checks.
  3. The compression→edit-flow mapping lives in `commander_source::backup_edit_for`
     → `BackupEdit::{InPlace, Archive(ArchiveEditPlan), ReadOnly}`; Inspect's
     `apply_backup_edit` applies it (raw/CHD `mark_edit_supported`; zstd/woz
     `set_archive_edit_context`). The zstd seekable-cache upgrade stays in the
     view (`open_browse_zstd`); single-file-chd still reaches `open_browse` via
     the Case 1 image redirect (Straggler D converges it later).
  4. `inspect_tab::open_browse`'s native-folder ladder + the hand-rolled
     Clonezilla cache tree are gone — both go through the shared model.

  Inspect routes through the shared **free functions** (`single_data_file`,
  `session_for_backup_partition`, `backup_edit_for`) rather than storing a
  `ResolvedBackup` like Commander, because it eagerly resolves the backup at
  load time (and redirects single-file-chd to an image). Those free functions
  are exactly what `ResolvedBackup::open_partition` calls, so the duplication is
  gone; full `ResolvedBackup` adoption in Inspect is left to Stragglers B/D.
- **Straggler C — shared Clonezilla cache decision tree** (`473ac50`, `1eeb48c`).
  `commander_source::PartcloneCacheStore::resolve` owns the in-memory-hit →
  on-disk-load → scan decision (and stale-cache removal); Inspect and Commander
  both hold a store. Commander gained the in-memory reuse it lacked.

## Validation checklist (Phase 4 — run in the GUI)

Phase 4 is a faithful refactor of the working Inspect tab and **was not
validatable in a headless sandbox**. Before considering it done, confirm in a
running GUI that each source still **browses *and* edits**:

- [ ] Native backup: **raw / none** (browse + add/delete a file).
- [ ] Native backup: **zstd** (browse via streaming; re-open after the seekable
      cache builds to exercise the upgrade + reuse; edit via decompress →
      recompress).
- [ ] Native backup: **woz** (browse + archive edit).
- [ ] Native backup: **chd / chd-dvd** per-partition if available (browse +
      `chd_edit`).
- [ ] **single-file-chd** backup (Case 1 image redirect; browse + `chd_edit` +
      metadata refresh on save).
- [ ] **Clonezilla** backup (fresh scan, then re-open to exercise in-memory
      reuse).
- [ ] A plain **image** (unchanged Case 1).

## Remaining work

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

### Straggler C — unify the Clonezilla cache-lookup decision tree ✅ DONE

~~Inspect's `open_browse_clonezilla` holds the full decision tree~~ — done in
`473ac50` + `1eeb48c`. `commander_source::PartcloneCacheStore::resolve` returns
`PartcloneLookup::{Ready, NeedsScan}`, owning the in-memory-hit →
on-disk-load (`_<device>.metadata.cache`) → scan decision plus stale-cache
removal. Inspect and Commander each hold a store; both call `resolve` then hand
a `NeedsScan` cache to `cache_runner::spawn_partclone_scan` and `insert` the
result on completion. Commander gained the in-memory reuse it lacked.

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

- [x] Inspect and Commander open every source (device / image / native backup /
      Clonezilla / single-file-chd) through the **shared model** resolver
      functions + `BrowseSession`; `inspect_tab::open_browse`'s per-compression
      ladder is gone (Phase 4). *Inspect routes via the free functions, not a
      stored `ResolvedBackup` — full adoption + single-file-chd convergence is
      Stragglers B/D.* **Pending GUI validation** (see checklist above).
- [ ] Image partition probing is one shared fn (Straggler A); CLI calls it too.
- [x] The Clonezilla cache decision tree is in the model, shared (Straggler C —
      `PartcloneCacheStore`).
- [ ] A new `src/tui/` could list + open any source using only `model::` —
      no resolution code of its own.
- [ ] CLI keeps `cli/resolve.rs` but re-uses the gates / `resolve_backup` /
      `cache_runner` for any browse-shaped feature it adds.

## Resume prompt — Phase 4 (EXECUTED — kept for history)

> **Status:** Phase 4 was implemented across `8942090`..`24abc2c`; see the
> **Done** + **Validation checklist** sections above. This prompt is retained as
> the record of what was planned. The remaining open work is Stragglers A / B /
> D and the GUI validation pass.

Point a fresh session at this:

> **Resume: source-resolution unification, Phase 4 (Inspect adopts the resolver).**
> Branch `commander-mode` in `/Users/dani/repos/rusty-backup` (tree clean). Read
> `CONTRIBUTING.md` § "Presentation layers: one model, many UIs", then this whole
> file (`docs/source_resolution_unification.md`) — it's the authoritative plan.
>
> **State:** Phases 1–3 are done + committed (capability gates → `fs/`; shared
> backup resolver `commander_source::{resolve_backup, ResolvedBackup,
> session_for_backup_partition}`; `model::cache_runner`), plus the CHD-backup fix
> and a superfloppy-gate fix. Both originally-reported bugs are fixed and
> user-verified in the GUI. Commander Mode opens images, native backups
> (raw/zstd), CHD backups (single-file-chd), and Clonezilla — all through the
> shared model. **The Inspect tab still has its own per-compression open routing**
> in `gui/inspect_tab.rs::open_browse` (+ `open_browse_zstd`,
> `open_browse_clonezilla`); Phase 4 unifies it onto the resolver + the shared
> `BrowseSession`, then deletes the inline ladder.
>
> **Do, in order (each a separate commit):**
> 1. Add `BrowseView::open_with_session(session: BrowseSession)` to
>    `gui/browse_view.rs` — it already owns `session: BrowseSession`; `open()`
>    just fills its fields + `spawn_open()`. This is the seam.
> 2. Extend the resolver (`model/commander_source.rs`) to cover **every**
>    compression Inspect handles, not just the read-only none/zstd subset:
>    per-partition `woz`; the **zstd seekable-cache upgrade** (Inspect opens
>    streaming via `open_browse_zstd`, then builds a background seekable cache and
>    swaps — preserve it; the resolver only does streaming `ZstdStreamCache`
>    today); per-partition `chd`/`chd-dvd` if any real backups use it (CHD backups
>    are normally single-file-chd, already handled).
> 3. Preserve Inspect's edit contexts layered on the session:
>    `set_archive_edit_context` (zstd/woz), the `chd_edit` flow, and
>    `single_file_chd_backup_folder` metadata refresh (Straggler D — converge on
>    the resolver path).
> 4. Lift the Clonezilla cache decision tree into the resolver/`cache_runner`
>    (Straggler C): Inspect's `open_browse_clonezilla` holds the full "in-memory
>    `block_caches` hit → persisted `_<device>.metadata.cache` load → else scan"
>    logic; share it so Commander gains the in-memory reuse.
> 5. Route `open_browse` through `resolve_backup` + `ResolvedBackup::open_partition`
>    → `browse_view.open_with_session(...)`; delete the inline per-compression
>    ladder. (Optional follow-on: Stragglers A/B — collapse the 3 image-probe
>    copies + image session-building.)
>
> **Key files:** `gui/inspect_tab.rs` (`open_browse` + `open_browse_*`),
> `gui/browse_view.rs` (`open` ~L435 / `open_partclone` ~L490, owns the session),
> `model/commander_source.rs` (resolver), `model/browse_session.rs` (session
> fields), `model/cache_runner.rs`.
>
> **CRITICAL:** this touches the working Inspect tab and is **not validatable in a
> headless sandbox**. It's a faithful refactor — preserve Inspect behavior
> exactly, keep changes incremental + reversible, commit per sub-step, and after
> each step have the user GUI-validate that **every** backup compression still
> **browses *and* edits**: raw/none, zstd (incl. the seekable upgrade), woz,
> single-file-chd, Clonezilla, and a plain image — plus edit mode (archive-edit
> decompress→recompress, `chd_edit`). Do not merge blind.
>
> **Per-commit gates:** `cargo build --all-targets` (zero warnings),
> `cargo clippy --all-targets -- -D warnings`, `cargo test --lib` (1981+ green);
> no Unicode glyphs in UI strings; the pre-commit hook runs fmt+check+clippy.
> Done = the acceptance-criteria checklist above is satisfied.
