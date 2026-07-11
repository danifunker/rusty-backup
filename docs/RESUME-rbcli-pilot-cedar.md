# Agent task: give `rb-cli` first-class Pilot/Cedar disk authoring

## Mission

`rb-cli` cannot **create or populate** a Xerox Pilot/Cedar disk image (`.pdi`), even
though all the core logic already exists in `src/fs/alto/pilot.rs`. Today the only
thing that drives that logic is a dev-only example (`examples/pilot_probe.rs`), plus a
one-off builder (`examples/cedar_kitchen_sink.rs`) that was used to hand-build three
Cedar 6.1 "kitchen sink" disks. Your job is to close the GUI/CLI parity gap so those
disks (and any Pilot/Cedar volume) are reproducible from committed `rb-cli` verbs.

This is a real parity gap under the rule in `CLAUDE.md` ("if it's a one-shot operation
a user might want to script, it should land in both GUI and CLI"). Read `CONTRIBUTING.md`
and `CLAUDE.md` in full before writing code.

## What already exists (ground truth — verify each before relying on it)

Core Pilot/Cedar API, all in `src/fs/alto/pilot.rs`, already tested and working:

| Function | Signature (abbreviated) | Purpose |
|---|---|---|
| `create_blank` | `(Geometry, Generation, name: &str) -> Result<Disk>` | blank Pilot/Cedar volume |
| `pilot_geometry` | `(total_pages: u16) -> Geometry` | geometry helper (note: `u16` page cap) |
| `install_boot_file` | `(&Disk, Generation, PvBootFile, bytes: &[u8]) -> Result<Disk>` | germ / bootfile / microcode slot |
| `add_file` | `(&Disk, Generation, data: &[u8]) -> Result<(Disk, u32 /*FileID*/)>` | add one file; returns its FileID |
| `delete_file` | `(&Disk, fid: u32) -> Result<Disk>` | remove by FileID |
| `set_client_directory` | `(&Disk, Generation, &[(String, u16 /*version*/, u32 /*fid*/)]) -> Result<Disk>` | write the Cedar `client` name directory (name -> FileID) |
| `write_pdi` | `(&Disk, Generation) -> Vec<u8>` | serialize to `.pdi` bytes |
| `read_volume` | `(&Disk, Generation) -> Result<PilotVolume>` | parse geometry/VAM |

`Generation` = `CedarNucleus | OriginalPilot`. `PvBootFile` = `Germ | BootFile | Microcode | Checkpoint`.
`PilotFilesystem` implements the **read** `Filesystem` trait (pilot.rs ~line 1006) — but
**not** `EditableFilesystem`.

Reference implementations that encode the exact working call sequence:
- `examples/pilot_probe.rs` — subcommands `new` / `add` / `set-dir` / `install-boot` /
  `extract-boot` / `boot-info` / `probe` / `verify` / `roundtrip`. This is the canonical
  recipe. **The new rb-cli verbs should behave identically**, so `pilot_probe verify`
  keeps passing on their output.
- `examples/cedar_kitchen_sink.rs` — batch builder: `create_blank` -> `install_boot_file`
  (germ+boot) -> loop `add_file` -> `set_client_directory`. Note it adds files against an
  **in-memory `Disk`** in one process; the `pilot_probe add` CLI re-reads the whole PDI
  per file, which is O(n^2). Your `put`/bulk path must avoid that (see `begin_bulk`).

## CLI code map (where to make changes)

- `src/cli/mod.rs` — clap `Command` enum (top-level verbs + their doc comments).
- `src/cli/verbs/new.rs` — the `new` verb. Uses a `FsKind` enum and `match args.fs { ... }`
  with a `format_and_write` / `create_blank_*` call per FS (see `FsKind::Efs`, `::Affs`).
- `src/cli/verbs/*.rs` — one file per verb (`partmap.rs`, `put`… follow the pattern).
- `src/cli/api/*.rs` — shared helpers (`hfs.rs::cmd_new` etc.).
- `src/cli/resolve.rs` — resolves `IMG[@N]` targets to a filesystem handle; how `put`/`ls`
  reach an `EditableFilesystem`.
- `src/fs/filesystem.rs:331` — the `EditableFilesystem` trait (methods: `begin_bulk`,
  `end_bulk`, `create_file`, `create_directory`, `delete_entry`, `rename`, …).
- `src/fs/alto/bfs.rs:352` — **`impl EditableFilesystem for BfsFilesystem`**: the closest
  precedent (another Alto-family editable FS). Use it as your template.
- `docs/cli-reference.md` is **generated** by `example/generate_cli_reference.rs`; the man
  page by `example/generate_manpage.rs`. Regenerate both, don't hand-edit.
- `src/model/file_types.rs` — `DISK_IMAGE_EXTS`. `pdi` is already present (leave as-is).

## Deliverables

Implement enough for `rb-cli` to fully create and populate a Pilot/Cedar `.pdi`, matching
the `pilot_probe` recipe. Recommended surface (adjust naming to fit the existing grammar —
check `docs/cli-reference.md` first so verbs stay consistent):

1. **`new --fs pilot`** — add `FsKind::Pilot` to `src/cli/verbs/new.rs`, calling
   `pilot::create_blank(pilot::pilot_geometry(pages), gen, name)`. Needs:
   - `--generation cedar|pilot` (default `cedar`),
   - a way to set the page/byte size (reuse `--size`; convert bytes->pages, cap at 65535),
   - optional `--germ <path>` / `--boot <path>` to install the boot chain in one shot.

2. **`install-boot`** verb (mirror `pilot_probe install-boot`): 
   `rb-cli install-boot IMG <germ|bootfile|microcode> <hostfile>` -> `install_boot_file`.
   Add an `extract-boot` and `boot-info` counterpart if cheap (nice-to-have).

3. **`impl EditableFilesystem for PilotFilesystem`** so `put` / `rm` / `mkdir` / `cp` work:
   - `create_file(parent, name, data, len, opts)` -> `add_file`, then add a
     `(name, 1, fid)` entry to the client directory. Pilot has **no directories** — it's a
     flat FileID space with one client name directory; map "parent" to the volume root and
     reject `create_directory` with a clear error (or make it a no-op that returns root).
   - `delete_entry` -> `delete_file` + drop the client-dir entry.
   - **Use `begin_bulk`/`end_bulk`**: rewriting the whole client directory and re-reading
     the volume on every `create_file` is O(n^2). In bulk mode, accumulate added FileIDs
     and write the client directory **once** at `end_bulk`. `put`/`untar`/`cp` of many
     files MUST go through bulk mode. (This is the single most important correctness/perf
     requirement — a naive per-file client-dir rewrite will be unusably slow and will hit
     the single-leaf limit spuriously.)

4. **Fix `rb-cli ls` on Pilot/Cedar.** Today `inspect` correctly reports the `Pilot/Cedar`
   partition, but `ls` falls back to a generic block **carver** (`whole-disk.img`,
   `carved-blkNNNNNN.txt`) instead of routing to `PilotFilesystem::list_directory`. Wire
   `ls`/`get` to the real Pilot browse (the GUI's Alto/Pilot BrowseSession already does
   this — reuse that dispatch, don't reinvent).

5. **A bulk/name path.** `set_client_directory` names files. Decide how a user names files
   from the CLI — either `put IMG host/File /Name` sets the client-dir name (preferred,
   via the bulk path), and/or a small `pilot set-dir IMG Name=FileID …` escape hatch that
   mirrors the example. Whatever you choose, one command must be able to name a batch.

## Known constraints (do NOT try to lift these here)

- **Client name directory is single-leaf, ~50 names max.** `build_client_directory` in
  pilot.rs errors past one B-tree page ("multi-page trees not yet written"). Surface this
  as a clean CLI error, not a panic. The multi-page B-tree writer is explicitly OUT OF
  SCOPE for this task.
- **Volume size is capped at 65535 pages (~33.5 MB)** because `pilot_geometry` takes a
  `u16`. If `--size` exceeds that, error clearly. OUT OF SCOPE to widen.
- Files beyond the ~50 named ones legitimately live on the disk addressed by FileID (they
  surface with synthetic `LVx_…` names). That's expected, not a bug.

## Repo conventions (enforced)

- Read `CONTRIBUTING.md` and follow its style. Reuse existing layers (resolve.rs,
  EditableFilesystem, the BrowseSession Alto branch) — do not duplicate logic.
- Edit files with the Edit/Write tools, never `sed` (the maintainer wants per-file
  reviewable diffs).
- No Unicode glyphs in any user-visible string (log lines, errors, help) — ASCII only.
- **Pre-commit doc sync (CLAUDE.md):** update the README image-formats / filesystems
  tables if Pilot/Cedar write support changes what a user can do; regenerate
  `docs/cli-reference.md` + the man page; extend the `DISK_IMAGE_EXTS` regression test if
  you touch it. Land docs in the same commit as code.
- Add unit/integration tests: round-trip a `new --fs pilot` + `put` + name via the public
  API, and assert `read_volume` / `list_directory` see the files. Follow the test style in
  pilot.rs's existing `#[cfg(test)] mod tests`.

## Acceptance criteria

1. `cargo fmt --check`, `cargo clippy` (no new warnings), `cargo test` all green.
2. `cargo build --release` succeeds; `rb-cli new --help` lists `pilot`; `rb-cli --help`
   lists the new verbs.
3. **Reproduce a Cedar disk end-to-end from `rb-cli` alone** and prove parity with the
   example-built ones. Concretely, a shell recipe like:
   ```
   rb-cli new out.pdi --fs pilot --generation cedar --size 32M \
       --germ '<chm>/Dorado.germ!4' --boot '<chm>/BasicCedarDorado.boot!22'
   rb-cli put out.pdi 'MazeWar.BCD' /MazeWar.BCD      # (bulk for many files)
   ...
   ```
   must produce a `.pdi` that **passes** `pilot_probe verify out.pdi germ=… bootfile=…`
   (germ+boot byte-identical, VAM agrees, round-trips byte-for-byte) and whose named files
   list via `rb-cli ls`.
4. The three kitchen-sink disks (`~/repos/xerox-dorado-dani/CedarDisk/CedarDorado-kitchensink*.pdi`)
   are reproducible via the new CLI (a committed script/example replacing the scratchpad
   manifests is a bonus, not required).

## Out of scope (do not touch)

- Multi-page client-directory B-tree writer (the ~50-name lift).
- Widening the volume past 65535 pages.
- Anything in the Dorado **emulator** repo (`~/repos/xerox-dorado-dani`) — including the
  Pilot timer/RTC boot blocker. This task is rusty-backup CLI only.

## Context / provenance

The Pilot/Cedar filesystem, the `.pdi` format, and the three kitchen-sink disks are
described in `docs/cedar_boot_fixture.md`, `docs/RESUME-cedar-disk-consistency.md`, and the
`examples/pilot_probe.rs` / `examples/cedar_kitchen_sink.rs` sources. The source corpus for
test files (Cedar 6.1 `.bcd`) lives at `~/PARC-Stuff/cyan/cedarchest6.1/`. Boot payloads:
`~/repos/xerox-dorado-dani/chm/cedar/germ/Dorado.germ!4` and
`.../chm/cedar/cedar6.1/BasicCedarDorado.boot!22`.
