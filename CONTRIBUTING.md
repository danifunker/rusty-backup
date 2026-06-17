# Contributing to Rusty Backup

This document describes how to contribute code to Rusty Backup. The first half is the usual setup / build / test / commit info. The second half — **Architecture & code-placement rules** — is the important part: it captures the lessons from the large late-april refactor (the in-tree audit checklist that drove it has since been retired) so that future code lands in the right place the first time and we don't have to do that kind of cleanup pass again.

If you only read one section, read **"Where code lives"** and **"Adding a feature: the playbook"**.

---

## Development setup

### Prerequisites
- Rust toolchain (latest stable)
- Git
- Platform-specific dependencies (see `README.md`)

### First-time setup

```bash
git clone <your-fork-url>
cd rusty-backup
cargo build      # also installs the cargo-husky pre-commit hook
```

The first `cargo build` installs a git pre-commit hook that runs `cargo fmt --all` automatically. The hook lives in `.cargo-husky/hooks/pre-commit`.

### Build / run / test

```bash
cargo build                    # Debug
cargo build --release          # Release
cargo run                      # Run (debug)
cargo run --release            # Run (release)

cargo test                     # All tests
cargo test --lib               # Unit tests only
cargo test test_name           # Single test
cargo test --test '*'          # Integration tests only

cargo fmt --check              # Check formatting (CI-style)
cargo fmt                      # Auto-format
cargo clippy                   # Lint
cargo build --all-targets      # Must produce zero warnings
```

### Hard rules for every PR

1. `cargo build --all-targets` produces **zero warnings**. We just cleared them; don't bring them back. Treat new warnings as a build failure even if cargo doesn't.
2. `cargo test --lib` is green (604+ tests as of this writing).
3. `cargo fmt` has been run (the pre-commit hook handles this; don't bypass it with `--no-verify`).
4. **No Unicode glyphs in user-visible strings.** The default egui font has no emoji/symbol coverage — `✓ ✗ ⚠ ← → • ✖ ℹ` render as blank boxes. Use plain ASCII (`OK`, `Skipped`, `Warning`, `->`, `-`, `X`, `Info:`) in log lines, button labels, dialog text. The only exception is content read from a filesystem we're displaying verbatim.

---

## Where code lives

The codebase has four layers. **Adding code in the wrong layer is the single biggest source of entanglement.** Know which layer your change belongs in before you start.

```
┌──────────────────────────────────────────────────────────┐
│  src/gui/        — view layer (egui widgets, dialogs)    │  pure rendering + dispatch
│                    NEVER owns long-running work          │
├──────────────────────────────────────────────────────────┤
│  src/model/      — orchestration (runners, queues,       │  spawns workers, owns Status,
│                    status structs, edit queues)          │  exposes pure functions
├──────────────────────────────────────────────────────────┤
│  src/fs/         — filesystem trait + implementations    │  per-FS readers/editors
│  src/rbformats/  — VHD/CHD/zstd/raw compress/decompress  │  format codecs
│  src/partition/  — MBR/GPT/APM parsing + alignment       │  partition tables
│  src/backup/     — backup orchestration + sizing         │  cross-FS workflows
│  src/restore/    — restore orchestration + validation    │  cross-FS workflows
│  src/clonezilla/ — Clonezilla image format reader        │  external format
├──────────────────────────────────────────────────────────┤
│  src/error.rs, src/os/, src/device/, src/update/, etc.   │  utilities, OS shims
└──────────────────────────────────────────────────────────┘
```

### Dependency direction

Always upward — never sideways within a layer or downward.

- `gui/` may depend on `model/` and the engine layer (`fs/`, `rbformats/`, `partition/`, `backup/`, `restore/`).
- `model/` may depend on the engine layer.
- Engine modules may depend on each other within the same layer **only** when the dependency direction is obvious (e.g. `restore/` calls into `fs/` and `partition/`, never the other way around).
- The classic violation we just had to fix: `partition/` must **not** depend on `restore/`. `PartitionSizeOverride` lives in `partition/` because `partition/mbr.rs`, `partition/apm.rs`, and `partition/gpt.rs` consume it. Don't move things to where the *use case* lives if the *types* are shared more broadly.

### What `gui/` may NOT do

- Spawn background work directly. Always go through a `model/*_runner.rs`.
- Hold heavy state. The view holds `Arc<Mutex<XxxStatus>>` snapshots and small UI scratch (text fields, scroll positions, popup open/closed flags). The actual data lives in a model object.
- Define on-the-wire types (Status structs, edit enums). Those go in `model/`.
- Inline 50+ line orchestration bodies. If a button handler grew past ~30 lines, the work belongs behind a model function.

### What `model/` may NOT do

- Touch egui or `eframe::App`. Model code must be testable without a GUI.
- Render anything. Model functions return data; the view renders it.

---

## Presentation layers: one model, many UIs (CLI, GUI, and a future TUI)

This project has **more than one front end over the same core**, and is adding
more. Today: `src/gui/` (egui) and `src/cli/` (40+ verbs over `model/` with no
egui). A **TUI** (`src/tui/`, crossterm) is planned. They are all *thin adapters*
over the same `model/` + engine. The CLI is the proof the pattern works — it
drives backup, inspect, browse-view editing, fsck, resize, etc. without a single
GUI type.

**The rule that keeps it that way — the litmus test:**

> **Could the CLI (or a headless test, or a TUI) call this without egui /
> crossterm / rfd?** If yes, it belongs in `model/` or the engine — *not* in
> `gui/` (or `tui/`). A pure predicate, a partition probe, a "resolve this picked
> source into openable partitions" routine, a thread-spawn that reports progress
> — all pass the test, so none of them may live in a view.

When a view needs logic that fails the test only because it's *currently written*
inside a view (a private `fn` in `gui/inspect_tab.rs`, say), the fix is to **move
it down**, not to copy it into the second UI. Copying is how you get two
divergent implementations that drift — exactly the trap that left Commander Mode
unable to open Clonezilla backups or to gate out non-filesystem partitions,
because the source-resolution + `is_browsable_type*` gates were trapped in the
Inspect view. (The gates now live in `fs/`; source resolution is being lifted
into `model/`.)

**Where each kind of shared logic goes:**

| Kind of logic | Home | Example |
|---------------|------|---------|
| Pure capability predicate (`(type byte/string) -> bool`) | engine (`fs/`, `partition/`) | `fs::partition_is_browsable`, `fs::is_checkable_type` |
| "Resolve a picked source → openable partitions" | `model/` | `model::commander_source` / the source resolver |
| Background work with progress | `model/*_runner.rs` (callbacks at the leaf, `Status` at the runner) | `model::export_runner`, the cache-scan runner |
| Pure query/transform the view calls inline | `model/` pure fn or engine | `EditQueue` helpers, `partition::format_size` |
| Rendering, widget state, dialog open/closed flags | the view (`gui/`, later `tui/`) | egui panels, `ScrollArea` state |

**Checklist when adding a user-facing feature (extends the playbook below):**

- [ ] Is any of this logic something a *second* UI would need? If yes, it goes in
      `model/`/engine now — even if only one UI consumes it today.
- [ ] Does the GUI button handler contain a predicate, a probe, a resolve, or a
      spawn? Move it down; the handler should be ~10–20 lines of dispatch.
- [ ] Could I write a unit test for this logic without a GUI? If not, it's in the
      wrong layer.
- [ ] Would the CLI (and the future TUI) call the *same function*, or would each
      re-derive it? It must be the same function.

When in doubt, look at how an existing `cli/verbs/*.rs` calls into `model/` for
the same capability and mirror it — the CLI is the reference adapter.

---

## Adding a feature: the playbook

Follow this order. Every step makes the next one easier.

### 1. Decide where it lives

Walk down the layer diagram. "Where does the *data* this feature operates on live? Who consumes it?" If the answer is "engines" (a filesystem, a format, a partition table), it goes there. If the answer is "the GUI needs to coordinate work and show progress", you're adding a model module + a thin view binding.

### 2. Engine layer first (if applicable)

If the feature requires new filesystem behavior, new format support, or a new orchestration step, land that **first**, with unit tests, before touching the GUI. The engine layer is far easier to test in isolation. Most engine modules have inline `#[cfg(test)]` blocks; follow the pattern in the same file.

### 3. Model layer

Create or extend a `src/model/<name>.rs`:

- **Status struct** for any background work — goes in `src/model/status.rs` (joining `InspectStatus`, `CacheStatus`, `BlockCacheScan`, `VhdExportStatus`, `ExtractionProgress`, `ResizeStatus`, `ExpandStatus`, `BulkConvertStatus`). Fields are `pub`. Document units (bytes, items, percent).
- **Runner function** that spawns the worker thread and returns `Arc<Mutex<XxxStatus>>`. Pattern: leaf I/O takes callbacks (`progress_cb`, `cancel_check`, `log_cb`); the runner owns the Status mutex and translates callback fires into Status field writes. See [`docs/progress_pattern.md`](docs/progress_pattern.md) for the canonical shape.
- **Pure helpers** (no threading) for query/mutation logic the view calls inline. `EditQueue` is the reference implementation: a wrapper struct around `Vec<StagedEdit>` exposing every is-pending / pending-for / space-delta / replace-set-X helper the view used to inline.

### 4. View layer

In the appropriate `src/gui/<tab>.rs`:

- Hold `Arc<Mutex<XxxStatus>>` (or the `EditQueue` model object) on the view struct.
- In `update`, poll the status (`try_lock`, snapshot scalars, drain `log_messages: Vec<String>` into the shared `LogPanel`).
- Button handlers call the model runner and store the returned Arc. They should fit in ~10–20 lines.
- Use `TabContext<'a> { devices, log }` (in `src/gui/context.rs`) for tabs that need both — don't add new `(devices: &[DiskDevice], log: &mut LogPanel)` argument pairs.

### 5. Tests

- Engine logic: unit tests in the same file under `#[cfg(test)]`.
- Model orchestration: unit tests where the worker is called inline (skip the thread spawn) — every existing runner is testable this way.
- GUI: not unit-tested. If the change is GUI-only, run it locally and verify behavior in `cargo run`.

---

## Patterns to follow (the established conventions)

These are the patterns the refactor settled on. Use them as templates.

### Background work: callbacks at the leaf, Status at the runner

[`docs/progress_pattern.md`](docs/progress_pattern.md) is the source of truth. Summary:

- **Leaf I/O** in `rbformats/`, `backup/`, `restore/`, `fs/*` takes `progress_cb: &dyn Fn(...)`, `cancel_check: &dyn Fn() -> bool`, and `log_cb: &dyn Fn(&str)`. This keeps the leaf reusable from CLI/tests/GUI.
- **Runners** in `src/model/*_runner.rs` own `Arc<Mutex<XxxStatus>>`, spawn the thread, and pass closures that write into the Status. The runner is the only place that knows about threading.
- **GUI** polls each frame: `try_lock`, snapshot scalars, drain `log_messages`.

Don't switch this to `mpsc::channel` even though you'll be tempted. The Status pattern is what 12+ background workflows already use, and a typed-channel migration would either rewrite every leaf I/O function (losing CLI/test reuse) or wrap every callback in a `Sender → fn` adapter (same shape, more boilerplate).

### Filesystem dispatch: go through the routing helpers

`src/fs/mod.rs` is the routing layer. When you need "do X to a partition of unknown FS", call the dispatcher, not a per-FS function:

- `open_filesystem(...)` / `open_editable_filesystem(...)` — type-byte dispatch
- `compact_partition_reader(...)` — returns `Box<dyn Read + Send>`, returns `CompactResult`
- `partition_minimum_size(..., allow_expensive, progress)` — returns `MinimumResult::Computed | Deferred`. Always set `allow_expensive=false` from the GUI; let the user click "Calc min" to opt in to a volume walk. **Don't add new direct callers of `effective_partition_size` from the GUI.**
- `patch_hidden_sectors_for(file, partition_offset, start_lba, log_cb)` — runs FAT/NTFS/exFAT in sequence, no-ops on magic mismatch
- `resize_filesystem_for(file, offset, new_size_bytes, log_cb)` — same pattern across all 7 resizable filesystems

If you find yourself writing a `match fs_type { 0x0b => fat::..., 0x07 => ntfs::..., ... }` ladder, stop and put it in `fs/mod.rs` instead. We removed five of those during the refactor.

### Compact readers (per-FS, not unified)

Each FS defines its own `Compact*Reader` implementing `Read`. They return a unified `CompactResult` with three sizes:

- `original_size` — partition size as reported by the partition table
- `compacted_size` — bytes the reader will emit
- `data_size` — bytes the reader will actually read from disk

Two reader styles, both valid:
- **Packed** (FAT/NTFS/exFAT): `data_size == compacted_size < original_size`
- **Layout-preserving** (HFS/HFS+/ext/btrfs/ProDOS): `data_size < compacted_size == original_size`

When adding a new FS reader, add a unit test asserting your invariant and document it in the doc-comment. See [`src/fs/README.md`](src/fs/README.md).

### Hidden sectors patching: shared preamble, per-FS write

`src/fs/patch.rs` exposes `read_boot_sector`, `patch_u32_le_in_buf`, and `write_sector_at`. FAT/NTFS share the read+detect+write-primary path; exFAT only reuses the read helper because its 12-sector boot region with checksum is genuinely unique. **Don't try to unify the write path** — three filesystems' backup-region semantics diverge enough that a single helper would obscure rather than simplify. Stub no-ops for filesystems without LBA-dependent VBR fields are deleted; the dispatcher in `fs::mod` skips them by construction.

### B-tree splits in HFS / HFS+

When inserting into a leaf that might split, use `hfs_common::btree_split_leaf_with_insert` (atomic byte-based merge+split). The older `btree_split_leaf` (count-based, separate-insert) is gone for HFS and slated for the same migration on HFS+ if it ever needs new edit-mode work. **Do not reintroduce the old pattern** — it caused a "B-tree node full" bug on real images during the HFS expand-block-size work.

### Catalog walking in HFS / HFS+

Use `hfs_common::walk_leaf_records(catalog_data, first_leaf, node_size, visit) -> Option<T>`. It handles fLink chasing, kind=-1 skipping, cycle detection, and bounds checks. The visitor closure carries the per-FS key encoding (Mac Roman vs UTF-16BE NFD, 1-byte vs 2-byte key_len). Five hand-rolled walkers were collapsed into this helper during the refactor — don't add a sixth.

### Shared widgets

- `src/gui/size_mode_row.rs` — the radio set for Original / Minimum / Custom / FillRemaining is shared across **three** popups (restore, inspect-export, backup-VHD). If you're adding a fourth, use this widget. The deferred Calc-min button is built in via `SizeModeRowOptions::deferred`.
- `src/gui/context.rs` — `TabContext<'a>` for `(devices, log)` argument bundles.

### On-disk format struct fidelity

When parsing an on-disk structure (BPB, MFT record, MDB, ext superblock, GPT entry, ...), preserve every field — even fields you don't read. The cost is one struct field; the benefit is correct round-trip writes when you later need to mutate one field and write back. `clump_size` in HFS+ `ForkData` is the canonical example: parsed, serialized, never read, kept on purpose.

`#[allow(dead_code)]` on these fields is fine — the per-file convention is implicit and well-understood.

---

## Patterns to avoid (the things we considered and rejected)

The refactor explicitly considered and rejected several "obvious cleanups" that would have made things worse. **Don't propose these in PRs without strong new evidence.** The short version:

- **Trait splits (Reader / Inspector / Repair).** Every consumer uses the full `Filesystem` trait. Splitting forces dispatch duplication and per-module preferred slices for no payoff.
- **Capability bitset (`fn capabilities() -> Capabilities`).** GUI gates buttons by partition type byte using `is_browsable_type`, `is_checkable_type`, `is_classic_hfs`, etc. — the existing pattern works without opening the filesystem first. A bitset would duplicate type-byte logic.
- **Mac/ProDOS sub-traits (`MacFilesystem`, `ProDosFilesystem`).** The model layer calls `fs.blessed_system_folder()` / `efs.set_prodos_type(...)` directly; non-supporting filesystems return `Err(Unsupported)` from the trait default. Sub-traits would force downcasting at every call site without removing code.
- **Full `ResizeStrategy` / `ValidationContext` traits.** Each FS's resize/validate is structurally distinct (FAT walks BPB+FAT-tables, NTFS rewrites VBR, HFS rewrites MDB, ext rewrites superblock+GDT, ...). The shared boilerplate is one read+magic-check helper, which already exists. Standalone fns + a thin dispatcher in `fs/mod.rs` is the right shape.
- **Unified compact-reader trait.** Truly shared state per reader is ~40 bytes; the interesting code is each FS's region/cluster layout, which is genuinely different. Per-FS readers + unified `CompactResult` is the right boundary.
- **mpsc-channel migration of progress reporting.** See "Background work" above.
- **Rolling per-revision ext modules (ext2/ext3/ext4).** They're feature bits on one superblock, not separate codebases.

If you think one of these *now* makes sense for new work, that's fine — but the bar is real new evidence (e.g. a third consumer with a genuinely different shape), not "this would be cleaner." Spell out the new evidence in the PR description.

---

## Specific rules

### Logging

- Use the `log` crate (`log::info!`, `log::warn!`, `log::debug!`) for production code.
- `eprintln!` is allowed inside `#[cfg(test)]` blocks; nowhere else.
- For user-facing log lines (the in-app `LogPanel`), use the `log_cb: &dyn Fn(&str)` callback the leaf I/O takes. Plain ASCII only (see hard rules above).

### Error handling

- Library code returns `Result<T, FilesystemError>` (or the relevant typed error).
- Application code uses `anyhow::Result<T>` and `?` for propagation.
- New "this filesystem doesn't support X" cases: return `Err(FilesystemError::Unsupported)` from the trait method — don't silently no-op.

### I/O

- Use streaming I/O with 64 KiB – 1 MiB chunks. Never load an entire partition into RAM.
- Disk I/O must be sector-aligned (512 B or 4 KiB).
- Background work must accept a `cancel_check: &dyn Fn() -> bool` and check it at chunk boundaries.

### GUI

- Disable controls during active operations (the `ProgressState` `controls_enabled` flag handles this for top-bar work).
- Long-running operations always run on a worker thread spawned by a model runner. **Never block the egui frame.**
- No emoji / Unicode glyphs (see hard rules).

### Files / line counts

- We don't enforce a hard limit, but if a single file passes ~3000 lines you should ask whether it has multiple responsibilities. The refactor split 4 files past that threshold; the model+view separation is what unblocks it most often.

---

## Commit & PR guidelines

- Write commit messages that explain the **why**, not the **what** — the diff shows what changed.
- Reference an issue or doc section if applicable (e.g. `docs/OPEN-WORK.md §2.1`).
- One logical change per commit. The pre-commit hook runs `cargo fmt`, so don't bundle formatting noise.
- Before opening a PR: `cargo build --all-targets` (zero warnings), `cargo test --lib` (green), `cargo clippy` (no new warnings on touched files).
- PR description: what changed, why, and any architectural decisions worth flagging (especially if you went near a "considered and rejected" pattern).

---

## Reference docs

- [`CLAUDE.md`](CLAUDE.md) — top-level architecture overview and per-area pointers
- [`docs/OPEN-WORK.md`](docs/OPEN-WORK.md) — every open feature gap, with design detail per item. Start here when picking up new work.
- [`docs/mister_filesystem_implementation_plan.md`](docs/mister_filesystem_implementation_plan.md) — the multi-wave MiSTer parity plan (its own track)
- [`docs/progress_pattern.md`](docs/progress_pattern.md) — background-work pattern (callbacks + Status + GUI poll)
- [`src/fs/README.md`](src/fs/README.md) — filesystem trait + compact-reader sizing model
- [`src/rbformats/README.md`](src/rbformats/README.md) — backup format codecs
- [`docs/fsck.md`](docs/fsck.md) — fsck architecture (shared types, per-FS phases)
- [`docs/editing.md`](docs/editing.md) — staged-edit / EditableFilesystem pattern
