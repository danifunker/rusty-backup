# RESUME: GUI disk provisioning + open items

Branch `fix-permission-issues`, 16 commits past `main` (`f7db48f`), **3 unpushed**.
Working tree clean as of `a6cc555`.

## Read first

1. `CONTRIBUTING.md` — especially the zero-warning rule, the Rust 1.73 engine
   floor, and **comments are one line, two at the absolute most**.
2. `CLAUDE.md` — GUI/CLI/TUI parity expectations and the pre-commit doc sync.
3. `docs/partition_table_writers_backlog.md` — Sun / RDB / AHDI / XFS backlog.

## THE ONE THING TO DO FIRST

**Push and read CI.** The branch has never been verified on anything but macOS.

Commit `36e3a9e` fixes a break that made **19 of 24 CI jobs fail** — every
non-macOS target — from the branch's very first commit. `ElevatedSource::file`
changed from `File` to `SourceHandle`, and only the macOS arm was updated; the
Windows and generic-Unix arms were missed. That fix compiles here but *cannot*
be exercised on a Mac, so CI is the only confirmation.

    gh run list --branch fix-permission-issues --limit 5
    gh run view <id> --log-failed --job <job-id>

**Lesson worth keeping:** `cargo check --all-targets` compiles ONE `target_os`
arm. "All targets" means test/bench/example targets, not platforms. When
changing a type used behind `#[cfg(target_os)]`, sweep every construction site:

    grep -rn "ElevatedSource {" src/     # finds all 4 cfg arms in one second

`rustup` is not installed on this machine (Rust is via Homebrew), so cross-target
`cargo check` isn't available without changing the toolchain setup.

## In flight: shared partition-editor modal (3 steps, 1 done)

The goal is building a disk in the GUI — pick a table type, define partitions,
assign a source image to each, apply in one action. The user explicitly likes the
existing disk-layout graph modal and chose "extend the shared modal" over a new
Partition tab, to avoid two competing partition UIs.

- **Step 1 — DONE (`a6cc555`).** Modal extracted to
  `src/gui/partition_editor_modal.rs`. Pure refactor, no behaviour change.
  Returns `Action::{Stay,Close,Apply}`; the caller applies. Inspect calls it via
  a 14-line wrapper.
  *Unverified:* nobody has eyeballed the Inspect > Edit Partition Table window
  since the extraction. Widget-call multiset was diffed and is identical, which
  would catch a dropped control but not a reorder. **Look at it once.**

- **Step 2 — TODO.** Add `Mode::BuildNew` to the modal: a table-type picker
  offering the five writable tables (MBR, GPT, APM, SGI, X68000), launched from
  the Restore tab. In `EditExisting` mode the picker is absent and nothing
  changes.

- **Step 3 — TODO.** Per-row source-image assignment, and an apply pipeline that
  writes the table then pours each assigned image into its partition.

Everything step 2/3 needs already exists:

| Need | Where |
|---|---|
| Layout maths (alignment, `rest`, overrun refusal) | `cli/verbs/new_partitioned_hd.rs::place()` |
| Table writers (5) | same file: `write_{mbr,gpt,apm,sgi,x68k}` |
| Bounded partition write | `model/physical_write_runner.rs` `WriteExtent::partition` |
| Disk map widget | `gui/partition_bar.rs` + `show_disk_layout_bars` |
| Type dropdown values | `partition/type_catalog.rs` |

Note `place()` and the writers currently live in a CLI verb module. Step 2/3
should probably lift them into a core module (e.g. `src/partition/provision.rs`)
so the GUI isn't calling into `cli::verbs` — CLAUDE.md wants shared logic in core.

## Open items

### TEMP-DIAG must be removed
8 sites, `grep -rn TEMP-DIAG src/`. Temporary device-permission logging added to
chase a macOS restore that failed with `Permission denied (os error 13)` at the
very end of an otherwise successful HFS+ restore. **That bug is still
undiagnosed.** When the user retests, the tell is whether
`reusing the elevated descriptor for /dev/rdiskN (no new prompt)` appears before
the failure. If it never appears, the failing step opens the device raw and
bypasses the elevation layer entirely — a different fix from the descriptor
cache. Remove the logging once resolved.

### GUI has no image-creation surface
`new hd` is in the CLI (multi-partition) and TUI (single partition) but the GUI
has **no equivalent at all** — not even for the pre-existing x68k/sgi-efs. This
predates the branch. Step 2/3 above is effectively the fix.

### `write` cannot select a partition out of the SOURCE
Writing a DMG into a partition only works if the DMG holds a **bare volume**.
`hdiutil create -fs HFS+` (and Disk Utility) produce a **whole GPT disk** with
the volume at LBA 40 — verified: the bytes land perfectly and macOS still won't
mount it, because the partition then starts with a GPT header.
Wanted: `rb-cli write Source.dmg@1 /dev/diskN --partition 2`. The engine reads
`IMG@N` everywhere else; `write` just doesn't accept it.

### Backlog (documented, not started)
`docs/partition_table_writers_backlog.md` — Sun VTOC, RDB, AHDI writers and an
XFS creator, each with the format details and a named oracle. None should ship
without validation; all four are formats where a wrong write looks fine and
fails later.

## Verification commands that matter here

    cargo fmt && cargo clippy --all-targets        # zero warnings is a hard rule
    cargo test --lib                                # 2729 passing
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

The TUI is TTY-guarded and cannot be driven from a pipe; factor logic into pure
functions and unit-test those instead.
