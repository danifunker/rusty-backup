# RESUME: GUI disk provisioning + open items

Branch `fix-permission-issues`. Working tree clean as of the Build Disk commit.

**This thread is done.** For what is left, see
[`RESUME-disk-provisioning-followups.md`](RESUME-disk-provisioning-followups.md),
which orders the remaining items smallest-first. Keep reading here for how the
shipped pieces fit together.

## Read first

1. `CONTRIBUTING.md` — especially the zero-warning rule, the Rust 1.73 engine
   floor, and **comments are one line, two at the absolute most**.
2. `CLAUDE.md` — GUI/CLI/TUI parity expectations and the pre-commit doc sync.
3. `docs/partition_table_writers_backlog.md` — Sun / RDB / AHDI / XFS backlog.

## CI is green

The cross-platform break is **confirmed fixed**. Run `30900569821` (the
`ElevatedSource::file` / `SourceHandle` fix) and `30955935101` both passed all
24 jobs; the branch had been failing 19 of 24 on every non-macOS target since
its first commit.

**Lesson worth keeping:** `cargo check --all-targets` compiles ONE `target_os`
arm. "All targets" means test/bench/example targets, not platforms. When
changing a type used behind `#[cfg(target_os)]`, sweep every construction site:

    grep -rn "ElevatedSource {" src/     # finds all 4 cfg arms in one second

`rustup` is not installed on this machine (Rust is via Homebrew), so cross-target
`cargo check` isn't available without changing the toolchain setup.

## Shared partition-editor modal — DONE (all 3 steps)

The goal was building a disk in the GUI: pick a table type, define partitions,
assign a source image to each, apply in one action. The user chose "extend the
shared modal" over a new Partition tab, to avoid two competing partition UIs.

- **Step 1 — DONE.** Modal extracted to `src/gui/partition_editor_modal.rs`.
- **Step 2 — DONE.** `Mode::{EditExisting,BuildNew}`. BuildNew adds the
  table-type picker over the five writable tables and an alignment field
  (plus heads / sectors-per-track on SGI).
- **Step 3 — DONE.** Per-row source-image assignment, and an apply pipeline
  that writes the table then pours each assigned image into its partition on
  one opened handle.

Where it all landed:

| Piece | Where |
|---|---|
| Layout maths + all 5 table writers (lifted out of the CLI verb) | `partition/provision.rs` |
| Build-a-disk working state (rows, sizes, sources, validation) | `model/disk_builder.rs` |
| Threaded apply: table + per-partition fill on one handle | `model/provision_runner.rs` |
| Modal, both modes | `gui/partition_editor_modal.rs` |
| Restore tab's `Build Disk` mode | `gui/restore_tab.rs` |
| CLI parity | `rb-cli new hd ... --fill N=PATH` |

Notes for whoever touches this next:

- `provision::WRITABLE_TABLES` is the single list the GUI picker and the
  `every_writable_table_writes_and_reparses` round-trip test both read. Adding
  a writer to it gets both for free.
- Rows carry a size *string* (`"20M"`, `"rest"`), never a start LBA —
  `provision::place` derives the layout every frame, so the preview bar is
  always exactly what gets written.
- `partition::parse_size` now lives beside `format_size`; `cli::parse::parse_size`
  delegates to it so the GUI never reaches into `cli::`.
- `type_catalog::kind_of` was silently returning `Other` for an X68000 table.
  Fixed — the editor modal had been showing "Table type: Other" for those.
- `gui/partition_editor_modal.rs` has headless `egui::__run_test_ui` smoke
  tests covering both modes and every table kind. That is what replaced the
  never-performed visual check of the Step 1 extraction; still worth an
  eyeball once, but a Grid cell-count mismatch or id clash now fails a test.

Verified end to end on macOS: `rb-cli new hd mbr` with `--fill` from a **UDZO
compressed** DMG produced a disk macOS mounts (`/Volumes/FILLME`), and the same
via GPT extracted byte-for-byte identical to the source volume.

## Open items

### TEMP-DIAG must be removed
`grep -rn TEMP-DIAG src/`. Temporary device-permission logging added to chase a
macOS restore that failed with `Permission denied (os error 13)` at the very end
of an otherwise successful HFS+ restore. **That bug is still undiagnosed.** When
the user retests, the tell is whether
`reusing the elevated descriptor for /dev/rdiskN (no new prompt)` appears before
the failure. If it never appears, the failing step opens the device raw and
bypasses the elevation layer entirely — a different fix from the descriptor
cache. Remove the logging once resolved.

### TUI has no multi-partition build
The TUI's New wizard still offers only a single whole-disk partition per table
type. The GUI and CLI now both do multi-partition + fill. `DiskBuilder` is
UI-agnostic, so a TUI screen over it is the obvious next parity step.

### `write` cannot select a partition out of the SOURCE
Writing a DMG into a partition only works if the DMG holds a **bare volume**.
`hdiutil create -fs HFS+` (and Disk Utility) produce a **whole GPT disk** with
the volume at LBA 40 — verified: the bytes land perfectly and macOS still won't
mount it, because the partition then starts with a GPT header.
Wanted: `rb-cli write Source.dmg@1 /dev/diskN --partition 2`. The engine reads
`IMG@N` everywhere else; `write` just doesn't accept it. The same gap applies to
Build Disk's per-row source picker.

### Backlog (documented, not started)
`docs/partition_table_writers_backlog.md` — Sun VTOC, RDB, AHDI writers and an
XFS creator, each with the format details and a named oracle. None should ship
without validation; all four are formats where a wrong write looks fine and
fails later.

## Verification commands that matter here

    cargo fmt && cargo clippy --all-targets        # zero warnings is a hard rule
    cargo test --lib                                # 2748 passing
    cargo test --bin rusty-backup                   # incl. the headless modal tests
    cargo check --lib --no-default-features --features os-stub,native-zstd
    cargo check --lib --no-default-features --features native-zstd,optical
    cargo build --manifest-path rb-cli-vintage/Cargo.toml \
      --no-default-features --features native-zstd,remote,tui,rust173-polyfill \
      --ignore-rust-version

The pre-commit hook runs fmt + clippy `-D warnings` + CLI-doc regeneration. Do
**not** pass `--no-verify` (CONTRIBUTING rule 3); earlier commits on this branch
did, to keep an unrelated staged deletion out, and that file is now gone.

## Testing tricks used this session

Real device nodes without touching real disks:

    hdiutil create -size 128m -layout MBRSPUD -fs MS-DOS -volname T target
    hdiutil attach -nomount target.dmg          # -> /dev/diskN
    hdiutil detach /dev/diskN

Compressed source that genuinely exercises the decoder (an uncompressed UDIF
does not): `hdiutil convert in.dmg -format UDZO -o out` — 16 MiB volume becomes
an 11 KB file. Verify writes byte-for-byte with
`hdiutil convert -format UDTO` plus `cmp`.

A built image can be checked without mounting it: `gpt -r show file.img` reads
the primary and backup GPT and reports the exact LBA of each region.

The TUI is TTY-guarded and cannot be driven from a pipe; factor logic into pure
functions and unit-test those instead. The **GUI** now can be driven headlessly
via `egui::__run_test_ui` — see the tests at the bottom of
`gui/partition_editor_modal.rs` for the pattern.
