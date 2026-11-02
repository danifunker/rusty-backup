# Contributing to Rusty Backup

This document describes how to contribute code to Rusty Backup. The first half is the usual setup / build / test / commit info. The second half — **Architecture & code-placement rules** — is the important part: it captures the lessons from the large late-april refactor (the in-tree audit checklist that drove it has since been retired) so that future code lands in the right place the first time and we don't have to do that kind of cleanup pass again.

If you only read one section, read **"Architecture policy (MVC)"**: it is binding for every change.

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

The first `cargo build` installs a git pre-commit hook that runs `cargo fmt --all` automatically, then `cargo clippy --all-targets -- -D warnings` when Rust sources are staged. The hook lives in `.cargo-husky/hooks/pre-commit`.

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

Debug builds are memory-heavy: the crate is ~400k lines in one library and
`cargo test` links ~100 binaries against it. `Cargo.toml` therefore builds
debug targets with line tables only and `.cargo/config.toml` caps cargo at
four jobs. Do not raise either on a 16 GB machine; on Linux the userspace OOM
killer takes the whole terminal session, not just rustc. Background and the
numbers: [docs/build-memory-crashes.md](docs/build-memory-crashes.md).

### Hard rules for every PR

1. `cargo build --all-targets` produces **zero warnings**. We just cleared them; don't bring them back. Treat new warnings as a build failure even if cargo doesn't.
2. `cargo test --lib` is green (604+ tests as of this writing).
3. `cargo fmt` has been run (the pre-commit hook handles this; don't bypass it with `--no-verify`).
4. **No Unicode glyphs in user-visible strings.** The default egui font has no emoji/symbol coverage — `✓ ✗ ⚠ ← → • ✖ ℹ` render as blank boxes. Use plain ASCII (`OK`, `Skipped`, `Warning`, `->`, `-`, `X`, `Info:`) in log lines, button labels, dialog text. The only exception is content read from a filesystem we're displaying verbatim.
5. **The shared engine must compile on Rust 1.73.** Everything under `src/` is also built by the vintage macOS-10.7 / Windows-7 target (`rb-cli-vintage/`), which pins **Rust 1.73**. Do not use a std API newer than 1.73 in engine code — route the common offenders through the `compat` / `rust173_compat` shims instead. See **[Rust 1.73 floor for engine code](#rust-173-floor-for-engine-code)** below; this is the single easiest way to redden a build that `cargo build` on your machine says is fine.

---

## Architecture policy (MVC) — read before adding any code

This section is **binding**. Every change, in every front end, follows it. If a
change seems to need an exception, stop and raise it instead of working around
it. `CLAUDE.md` points every agent here before it writes code.

### The three layers

```
┌───────────────────────────────────────────────────────────────────────────┐
│ VIEW        src/gui/ (egui)   src/cli/ (rb-cli verbs)   the TUI            │
│             future native shells (AppKit / GTK4 / Win32, written in Rust)  │
│             renders state, maps input to Actions, fulfils Effects          │
├───────────────────────────────────────────────────────────────────────────┤
│ CONTROLLER  src/controller/  (one controller per task, screen or dialog)           │
│             state machines, Actions in / Effects out, validation rules,    │
│             key bindings, widget ids, job orchestration                    │
├───────────────────────────────────────────────────────────────────────────┤
│ MODEL       crates/*   (formats, filesystems, partition tables, containers)│
│             src/{fs,partition,rbformats,backup,restore,os,optical,...}     │
│             src/model/ (domain operations, runners, edit queue, jobs)      │
└───────────────────────────────────────────────────────────────────────────┘
```

**One controller per task, shared by every view.** The GUI, the TUI and the CLI
drive the *same* controller for the same task. Backup is one controller whether
it is started from the Backup tab, a TUI screen or `rb-cli backup`. A view never
re-implements what a controller does, and a controller never knows which view
is driving it.

Dependencies point **down only**: view -> controller -> model. A model module
never imports a controller or a view; a controller never imports a view or a
toolkit.

### Separation of duties

| Layer | Owns | Must not |
|---|---|---|
| **Model** | The truth: on-disk formats, filesystems, partition tables, containers, persistence (config, recent files), and domain operations (open, list, stage, apply, back up, restore). Pure functions and runners that report progress through callbacks or `model::job`. | Touch any UI toolkit, hold UI state (selection, cursor, which dialog is open), format text for display beyond plain data, or know a controller exists. |
| **Controller** (`src/controller/`) | Everything the user is *doing*: what is open, selected, staged, running and failing; which dialog or prompt is up; what an input means (the key map); validation rules; the `Action` enum it accepts and the `Effect` enum it emits; stable widget ids; starting, polling and cancelling jobs. | Import a toolkit (egui, ratatui/crossterm, rfd, objc2, gtk, Win32 UI), spawn raw threads, draw, block on I/O on the caller's thread, or assume a particular view. |
| **View** | Presentation only: layout, colours, fonts, scrolling, focus, animation. Turns raw input into controller `Action`s (through the controller's key map), draws from the controller's read accessors, and fulfils `Effect`s (file pickers, prompts, confirmations, clipboard). | Contain domain logic or business rules, import `model`/engine/crate types directly (the controller re-exports the plain data it hands out), spawn threads, decide what a key does, or keep state the controller should own. |

**The litmus test** for any piece of logic: *could a second view need it?* If
yes, it is controller or model code, even if only one view uses it today.
Copying logic into a second view is how two implementations drift (Commander
Mode once could not open Clonezilla backups because source resolution was
trapped in the Inspect view). When logic is found in a view, **move it down**;
never copy it sideways.

| Kind of logic | Home |
|---|---|
| A format, filesystem, partition-table or container rule | a `crates/*` crate (or `src/fs`, `src/partition`, ... until it moves) |
| A domain operation with progress (open, list, stage, apply, back up) | `src/model/` runner or function, run as a `model::job` |
| "What happens when the user does X" (state, sequencing, enablement, validation) | the task's controller in `src/controller/` |
| A key binding or shortcut | the controller key map (`src/controller/keys.rs`), never a view |
| Layout, widget state, scroll position, colours | the view |

### Rust and the front end

- **All logic is Rust.** The model and controllers are Rust in this repository,
  compiled by every target, including the vintage ones (Rust 1.73 and mrustc;
  see [Rust 1.73 floor for engine code](#rust-173-floor-for-engine-code)).
- **Views are Rust too.** egui, the TUI and the CLI already are. Native shells
  (AppKit, GTK4, Win32) are written in Rust over the platform's bindings
  (`objc2`, `gtk4-rs`, `windows-rs`): Rust calls into the native toolkit, so the
  boundary between our code and the platform is the toolkit's API, inside Rust.
- A non-Rust file in a shell (an `Info.plist`, a manifest, an icon, a XIB or
  resource script) carries **no logic**. If a platform genuinely needs native
  code (Swift, Objective-C, C#), it is a thin widget wrapper that calls back
  into Rust and holds no rules or state.

### Controllers: the contract

- A controller implements `controller::Controller`: `apply(Action) -> Vec<Effect>` for
  user intents and `tick() -> Vec<Effect>` to drain finished jobs. Views read
  state through plain accessors.
- **`Action` and `Effect` enums are exhaustive.** Views `match` them with no `_`
  arm, so a new action or effect fails to compile in every view that has not
  handled it. That compile error is how a feature reaches every front end.
- **Every actionable element has a stable widget id** in the controller's `ids`
  module; every view sets it as the accessibility identifier, so one UI
  scenario test drives every view.
- **One controller, many uses.** When a second use case needs a variation, add
  a *profile* (a configuration struct of capabilities: read-only, staging on or
  off, which columns, which sources) to the existing controller. Do not fork
  it. The file browser is the reference: one browser-pane controller, configured
  per use (each Commander pane, the Inspect browse view, the TUI explorer).
- **The CLI is a view.** An `rb-cli` verb parses arguments into the
  controller's input, runs it (interactive prompts become flags or `--yes`), and
  prints its results. It does not reach past the controller into the model for
  anything the controller covers.

### Prompts, confirmations and validation

- A view asks the user something only because a controller emitted an `Effect`
  (`PickFile`, `PickFolder`, `Confirm`, `AskPassword`, `AskText`), and it
  answers with the matching `Action`.
- **Validation rules belong to the controller; the view runs them.** A text
  prompt carries a validator the controller defines (a plain Rust function from
  the input to "ok" or an error message). The view calls it as the user types
  to show the error inline and to enable the OK button. The controller runs the
  same validator again on the answer: the view's check is for feedback, the
  controller's is authoritative.

### Key bindings

- Every shortcut is **data**: one table in `src/controller/keys.rs` of
  (key, modifiers) -> action, per controller. Views translate their native key
  events into the table's key type and look the action up; no view hard-codes
  what a key does.
- The table is the single place to change a shortcut, and a user override file
  (loaded through the config store) can replace any entry.
- `Primary` means Ctrl on Windows and Linux and Cmd on macOS, so one entry
  serves every platform. Platform-specific entries are allowed where a platform
  convention differs (for example, the Mac Delete key).
- Shortcuts act only when no text field has focus.
- Default file-manager bindings (Commander and every browser pane):

| Key | Action |
|---|---|
| F1 | Help |
| F2 | Rename |
| F3 or Primary+I | Info |
| F5 | Refresh |
| F6 | Copy to the other pane |
| F8 or Delete (Mac: Delete / Backspace) | Delete |
| Primary+N | New folder |
| F9 | View menu (listing style, quick view, log) |
| Primary+K | Quick view: the active pane's File Info in the other pane |
| Primary+Shift+K | Connect to an `rb-cli serve` daemon (a terminal also takes Alt+K, since most send Ctrl+Shift+K as Ctrl+K) |
| Tab | Switch pane (a folder tree, then its list, then the other pane) |
| Enter | Open |
| Right / Left | Expand / collapse in a tree (a flat list leaves them to the view) |
| Space or Insert | Mark |

### Async work

- **Every slow operation is a `model::job::Job`**: anything that touches a
  device or image, the network, or walks a filesystem. Nothing slow runs on a
  view's thread or inside `apply` / `tick`.
- Jobs declare the resources they touch (`Resource` keys) and register with the
  `JobRegistry`, which is what disables conflicting controls and says why.
- A job reports phase, progress and log lines through `JobCtx`, honours
  cancellation, and is panic-guarded. Views show a job only through its
  snapshot (`gui::progress::show_job`, the TUI "Working" box, `rb-cli --status`).
- Views wake through `Notify` (egui repaint, the TUI wake channel, the native
  main-thread dispatch) instead of polling every frame.
- **No `thread::spawn` outside `model::job` and `worker::spawn_guarded`.**
  `scripts/check-boundaries.sh` fails the build on a raw spawn in a view or a
  controller.

### Crates

- **What goes in a crate:** format, filesystem, partition-table and container
  logic that is useful on its own: no UI, no app configuration, no controller
  types. Each crate has its own tests and builds on Rust 1.73 (and for `qnx2fs`,
  `wasm32`).
- **A fix in a crate ships.** Bump the crate's version in the same change
  (semver: adding a public field to a struct with public fields, or removing
  public API, is a minor bump while the major is 0), bump dependents that need
  the new version, update every lockfile (`Cargo.lock`, `rb-cli-vintage/`,
  `rb-cli-ppc/`, `rb-cli-sol9/`; each must pass `cargo metadata --locked`), and
  publish through `publish-crates.yml` (dry run first). A crate with a
  changelog gets an entry.
- The app depends on crates by path, so it sees a fix immediately; other
  consumers (icon-emu and others) only see it once it is published.

### Regression

- **Every change** runs the tests for what it touched: unit tests in the same
  file, controller tests (`#[cfg(test)]` in `src/controller`, and the scenarios in
  `tests/ui_scenarios/`), the matching `rb-regress run --filter` cases, and, when
  it changes how a filesystem is written, the emulator scenario where the real
  operating system judges it (`regression-tests/emulator/`).
- **A milestone** (the end of a phase, before merging to `main`) runs the full
  suite: `regression-tests/scripts/regress-local.sh` on Linux, plus the Windows
  and macOS legs and `rb-regress parity` / `verify` across all three.
- A new checker (oracle or emulator scenario) is not trusted until its control
  — the same check on deliberately damaged input — has failed.
- A defect found by any of this gets an `R-NNN` entry in
  `docs/Regression_Bugs.md`; if it is not fixed in the same change it goes in
  `regression-tests/data/known-failures.toml` (or the emulator `cases.txt`).

### Enforcement

- `scripts/check-boundaries.sh` (CI job `boundaries`) fails on toolkit code or
  raw threads in `src/controller` and on raw threads in views, and counts views'
  direct references to the model and engine. That count only goes down; a path
  that reaches zero is switched to enforcing.
- Enforced so far (`ENFORCED` in the script): `src/gui/commander`,
  `src/gui/browse_view.rs`, `src/gui/inspect_tab.rs`, `src/gui/backup_tab.rs`,
  `src/gui/restore_tab.rs`, `src/gui/physical_disk_export.rs`,
  `src/gui/partition_editor_modal.rs`, `src/gui/progress.rs`,
  `src/gui/alto_resize_dialog.rs`, `src/gui/squashfs_budget_dialog.rs`,
  `src/gui/expand_hfs_dialog.rs`, `src/gui/floppy_convert_dialog.rs`,
  `src/gui/remote_browser.rs`, `src/gui/resize_popup.rs`, `src/gui/optical_tab.rs`,
  `src/gui/optical_browse_view.rs`, `src/gui/optical_audio.rs`,
  `src/cli/verbs/tui_app/commander.rs` and `src/cli/verbs/tui_app/explorer.rs`.
- Exhaustive `Action` / `Effect` enums make an unhandled feature a compile
  error.
- Controller logic is tested without any view. Code that cannot be tested
  without a view is in the wrong layer.

### Adding a feature: the playbook

1. **Model first.** New format, filesystem or domain behaviour lands in its
   crate or model module, with unit tests.
2. **Controller.** Add or extend the task's controller: its state, its
   `Action`s and `Effect`s, any validators, key-map entries and widget ids. Test
   it with no view.
3. **Every view.** Each view handles the new actions and effects (the compiler
   lists the ones that do not): the GUI, the TUI, and the CLI where the feature
   is scriptable. Interactive-only dialogs (Settings, elevation prompts) are
   GUI- and TUI-only. Record any view that cannot support it, with the reason,
   in `docs/platform_matrix.md`.
4. **Tests and regression** as above, then the documentation sync in
   `CLAUDE.md` (README tables, MiSTer matrix, picker extensions).

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

### Comments

- **One line. Two if it truly needs it. Never three or more — ever.** This is a hard rule for every comment you write inside a `.rs` file, inline `//` and doc `///` alike.
- If the explanation doesn't fit in two lines, it isn't a comment — it's documentation. Put it in the module's `//!` header, `src/fs/README.md`, `src/rbformats/README.md`, or a `docs/` page, and reference that from the one-line comment.
- Say **why**, not what. `// bump the cursor past the BPB` restates the code; `// FAT32's BPB is 90 bytes, not 62 — 0x24 onward differs` earns its line.
- Existing multi-line blocks are grandfathered; don't retrofit files you aren't otherwise touching. But don't add new ones.

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

### Rust 1.73 floor for engine code

Everything under `src/` is compiled twice: by the normal desktop toolchain, **and** by the vintage build (`rb-cli-vintage/`, a second top-level manifest that reuses the same `../src/` sources for the macOS 10.7 / Windows 7 targets). That build pins **Rust 1.73** — the last toolchain whose prebuilt std targets those OSes. So an engine file may only use std APIs available in 1.73.

This is a trap because **your local `cargo build` cannot see it.** You're on a modern toolchain where the newer API exists, so it compiles clean and even passes `cargo clippy -D warnings`. It fails only on the 1.73 build, which usually means only in CI (or when someone builds the vintage target). Worse, **clippy actively pushes you into it**: its auto-fixes suggest `x.is_multiple_of(n)`, `io::Error::other(e)`, `iter::repeat_n(..)` — all of which postdate 1.73. Accepting a clippy suggestion in engine code is the most common way this breaks.

The two shim modules exist to resolve the tension — they let one call site satisfy both toolchains:

- **`src/compat.rs`** — *always compiled.* Each helper is `#[cfg]`-split so the modern build uses the current clippy-clean std API and only the vintage build falls back. Use `crate::compat::io_other(e)` instead of `io::Error::other(e)` (1.74); `crate::compat::repeat_n(..)` instead of `iter::repeat_n(..)`. This is where a discouraged-but-1.73-safe form is quarantined so the desktop tree stays lint-clean.
- **`src/rust173_compat.rs`** — *compiled only under the `rust173-polyfill` feature* (the vintage manifest enables it; the desktop never does). Extension traits that add a method 1.73 lacks entirely — `is_multiple_of` (1.87), `Option::is_none_or` (1.82) — so the identical `x.is_multiple_of(n)` call syntax resolves on both. Bring the trait into scope with `#[cfg(feature = "rust173-polyfill")] use crate::rust173_compat::IntIsMultipleOf as _;` at the top of the file. If the only use is in `#[cfg(test)]` code, scope the import `#[cfg(all(test, feature = "rust173-polyfill"))]` so it isn't an unused import on the vintage *library* build.

Known-safe within the floor (no shim needed): `div_ceil` / `next_multiple_of` (1.73.0), `is_some_and` (1.70). When unsure whether an API predates 1.73, check its "Stable since" in the std docs before using it in `src/`.

**Verify before you push.** Your normal build won't catch a violation; do one of:

```bash
# Compile the shared source under the vintage feature/dep set. Runs on the
# modern toolchain but exercises the compat/polyfill wiring. Expect a pile of
# unused-import warnings for the polyfill trait — those are the modern compiler
# seeing the inherent method and are benign; a hard ERROR is the real signal.
cargo build --manifest-path rb-cli-vintage/Cargo.toml \
  --no-default-features --features native-zstd,remote,tui,rust173-polyfill,windows-legacy,yaml \
  --ignore-rust-version
```

That feature list is exactly what CI's Windows vintage leg builds (`.github/workflows/release.yml`), so keep the two together. **`windows-legacy` is not optional on Windows** — the vintage manifest pins the `windows` crate to 0.58 (0.62 needs rustc 1.82), and 0.61 wrapped some nullable pointer params in `Option`, so `src/os/windows.rs` carries `#[cfg]`-split call sites for `CreateWellKnownSid` and `ShellExecuteW`. Omit the feature and the build dies with four `Option<PSID>` / `Option<HWND>` type errors that look like an engine bug and are not one. It gates only `#[cfg(windows)]` code, so it is inert on the macOS 10.7 leg and this one command is correct on every platform.

The definitive check is an actual `rustup toolchain install 1.73.0` build of that manifest; the command above is the cheap proxy that catches the wiring mistakes (wrong shim path, raw `io::Error::other` left in, unused imports).

### The pipeline builds more than your machine does

A green `cargo test` and a green `rb-regress run` are not a green pipeline.
Run this before pushing:

```bash
scripts/preflight.sh
```

It runs what CI runs: `cargo test --release` (CI's test step is release, not
debug), the MiSTer feature set (`optical` on, `native-zstd` off — anything
touching zstd must go through `crate::rbformats::zstd_compat`, never the `zstd`
crate directly), the Rust 1.73 vintage floor, and the doc-parity tests.

**After pushing, read the job list, not the run conclusion.** Several jobs are
`continue-on-error: true` — the MiSTer `rb-cli-mini` build among them — so they
can fail while `gh run list` still reports the run as `success`. That hid a
compile error for a day.

```bash
gh run view <run-id>
```

### mrustc mis-lowers `leading_zeros` / `leading_ones` on narrow integers

The PowerPC build (`rb-cli-ppc`, via mrustc's C backend — see `scripts/build-ppc.sh`)
gets the **wrong answer**, not a compile error, for these:

| on `u8` / `u16` | mrustc | rustc |
| --- | --- | --- |
| `0x01u8.leading_zeros()` | **31** | 7 |
| `0x80u8.leading_zeros()` | **24** | 0 |
| `0x01u8.leading_ones()` | **24** | 0 |

mrustc counts as if the value were 32 bits wide. `u32` and `u64` are correct, and
`trailing_zeros`, `trailing_ones`, and `count_ones` are correct at every width.

**Widen to `u32` before the call** in engine code:

```rust
// WRONG on PowerPC - wraps to ~4.29e9 for any byte
let top_bit = 7 - byte.leading_zeros();
// Correct on both
let top_bit = 31 - (byte as u32).leading_zeros();
```

Why this deserves its own section: `cargo build`, `cargo clippy`, and the whole
test suite pass, because native rustc is right. It cost a day. `BitmapReader`
used the wrong form, so **every** PowerPC backup of ext / UFS / JFS / AFFS
imaged the volume at full size instead of its used extent — a silent ~7x size
regression with no error anywhere. It hid especially well because `count_ones`
*is* correct, so bit *counts* agreed across platforms and only *positions* were
wrong: `inspect`, `ls`, `fsck`, and `show fs-info` all matched.

To check a suspect intrinsic in about a minute, without a PowerPC build at all —
this is a codegen bug, so it reproduces on the host:

```bash
cat > /tmp/t.rs <<'EOF'
fn main() { println!("{}", 0x01u8.leading_zeros()); }
EOF
cd ~/repos/mrustc && MRUSTC_TARGET_VER=1.74 \
  bin/mrustc /tmp/t.rs -o /tmp/t_m -L output-1.74.0 && /tmp/t_m   # mrustc
rustc -O -o /tmp/t_n /tmp/t.rs && /tmp/t_n                        # truth
```

No vendored dependency is affected — every dependency use is on `u32`/`u64`/
`usize` (checked, including `hashbrown`, whose generic `GroupWord` is `u32` or
`u64`). Re-check if that ever changes.

---

## Commit & PR guidelines

- Write commit messages that explain the **why**, not the **what** — the diff shows what changed.
- Reference an issue or doc section if applicable (e.g. `docs/OPEN-WORK.md §2.1`).
- One logical change per commit. The pre-commit hook runs `cargo fmt`, so don't bundle formatting noise.
- Before opening a PR: `cargo build --all-targets` (zero warnings), `cargo test --lib` (green), `cargo clippy` (no new warnings on touched files). If you touched engine code under `src/`, also run the vintage-feature build from **[Rust 1.73 floor for engine code](#rust-173-floor-for-engine-code)** — a modern-toolchain green does not imply a 1.73 green.
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
