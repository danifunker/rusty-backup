# RESUME: disk-provisioning follow-ups

Successor to `docs/RESUME-gui-disk-provisioning.md`, which is **done** — the GUI
Build Disk mode, `partition/provision.rs`, `model/disk_builder.rs`,
`model/provision_runner.rs` and `rb-cli new hd --fill` all shipped and CI is
green. Read that doc first for how the pieces fit; this one is only what is
left.

Items are ordered **smallest first**. Each is independent — do them in any
order, but the ordering below is the cheapest path to a shorter list.

## Read first

1. `CONTRIBUTING.md` — zero-warning rule, the Rust 1.73 engine floor,
   **comments are one line, two at the absolute most**.
2. `CLAUDE.md` — GUI/CLI/TUI parity and the pre-commit doc sync.

---

## 1. Remove the TEMP-DIAG logging — mechanical, but GATED

**Size:** ~30 minutes. **Blocked on:** a hardware retest by the user.

    grep -rn TEMP-DIAG src/        # 8 sites

Sites: `gui/{inspect,backup,restore}_tab.rs`, `os/mod.rs`, `os/macos.rs`,
`os/macos_stub.rs`.

This is temporary device-permission logging added to chase a macOS restore that
failed with `Permission denied (os error 13)` at the very end of an otherwise
successful HFS+ restore. **That bug is still undiagnosed** — do not delete the
logging until it is.

The tell when the user retests: does
`reusing the elevated descriptor for /dev/rdiskN (no new prompt)` appear before
the failure?

- **It appears** -> the descriptor cache is being used and the failure is
  downstream; look at what runs after the last partition write.
- **It never appears** -> the failing step opens the device raw and bypasses the
  elevation layer entirely. That is a different fix from the descriptor cache.
  See `docs/`-adjacent notes and `os/macos.rs`'s `ELEVATED_DEVICES`.

Only after that is understood should the 8 sites come out.

---

## 2. `write` should accept `SOURCE@N` — small, and it unblocks a real trap

**Size:** small. Mostly a type change plus reusing an existing resolver.

`rb-cli write Source.dmg@1 /dev/diskN --partition 2` is rejected today. The
engine understands `IMG@N` everywhere else — `src/cli/img_at.rs::ImageRef` is
already used by ~10 verbs (`ls`, `cp`, `rm`, `du`, `partmap`, `tar`, `batch`, …).
`write` just never adopted it.

**Why it matters beyond the CLI:** `hdiutil create -fs HFS+` and Disk Utility
produce a **whole GPT disk**, not a bare volume. Assigning one of those as a
Build Disk row source (or to `--fill`) writes bytes that land perfectly and
still won't mount, because the partition then starts with a GPT header. This is
the single most likely "it said it worked but the disk is broken" report from
the new feature.

**Where to change it**

| What | Where |
|---|---|
| `WriteArgs.image: PathBuf` -> `ImageRef` | `src/cli/verbs/write.rs:23` |
| Source open (currently whole-file) | `write.rs::run`, and `model/physical_write_runner.rs::open_source_image` |
| Existing partition slicer to reuse | `cli/resolve.rs::resolve_partition_streaming` |
| GUI per-row source picker | `gui/partition_editor_modal.rs::source_cell` |
| `--fill N=PATH` parser | `cli/verbs/new_partitioned_hd.rs::parse_fills` |

Note `--partition` already exists on `write` as the *destination* selector, so
the help text must make source-vs-destination unmistakable. `img_at.rs`'s doc
comment explains why `@` is the safe separator.

Suggested shape: give `physical_write_runner` an optional source-partition
selector alongside the path, so all three callers (CLI `write`, `--fill`, GUI
Build Disk) get it from one change rather than three.

---

## 3. TUI multi-partition build — medium-small

**Size:** medium-small. A screen over logic that already exists.

The TUI's New wizard still offers exactly one whole-disk partition per table
type (`cli/verbs/tui_app.rs`, the `HdPlatform::{Mbr,Gpt,Apm,SgiTable,X68kTable}`
arm — it hardcodes `partitions: vec!["rest".to_string()]`).

`model/disk_builder.rs::DiskBuilder` is deliberately UI-agnostic: `plan()`,
`validate()`, `add_row()`, `remove_row()`, `move_row()`, `sources()` do all the
work and are already unit-tested. A TUI screen is a renderer over it, and
`model/provision_runner.rs::run_worker` is the same apply path the GUI uses.

Remember the TUI conventions: ASCII-only, TTY-guarded, RAII terminal restore,
and the testable-core pattern (the TUI can't be driven from a pipe, so put
logic in pure functions and test those).

---

## 4. "Format as…" per Build Disk row — medium, and the biggest UX win

**Size:** medium. This is the item that makes Build Disk feel finished.

**The state of play — read this before planning:**

- `rb-cli reformat` (format **in place** into an existing partition) supports
  **HFS only**. It hard-bails on anything else — `verbs/reformat.rs:52`.
- `rb-cli new volume <fs>` / `new floppy <fs>` create ~19 filesystems (pfs3 and sfs added by F-003, 2026-08-15), but as
  **standalone bare volume images**, not into a partition.
- The GUI has **no** blank-volume creation surface at all.

So a GUI user who builds a disk today gets correct, **empty** partitions and no
way to format them without leaving the app.

**The cheap fix is not `reformat`.** Every creator is already a core function
returning bytes — `fs::fat::create_blank_fat`, `fs::hfs::create_blank_hfs_sized`,
`fs::affs::create_blank_affs`, `fs::hfv::build_blank_hfv`, and so on. So:

1. Lift the `FsKind` -> bytes dispatch out of `cli/verbs/new.rs` (the match
   running to ~line 790, plus the 7 `write_blank_*` helpers at 793-954) into a
   core `fs::blank::create(kind, size, opts) -> Result<Vec<u8>>`. `new.rs` becomes
   grammar over it, exactly as `new_partitioned_hd.rs` is now grammar over
   `partition::provision`.
2. Add a **Format as…** dropdown to each `BuilderRow`, beside the existing
   source-image picker. The two are mutually exclusive: a row is empty, filled
   from an image, or freshly formatted.
3. `provision_runner` generates the volume in memory and pours it through the
   fill path that already exists. No `reformat` involvement at all.

This also hands the GUI its first blank-volume creation surface for free, which
was an open gap long before this branch.

One wrinkle for step 1's `-> Result<Vec<u8>>` signature: the newer creators are
deliberately **streaming**, because only a fraction of the volume is populated.
`fs::xfs::format::write_blank_xfs`, `ext_format::write_blank_ext*`,
`efs::write_blank_efs` and `ntfs_format::create_ntfs` all take a `Write + Seek`
and leave the rest sparse; the `create_blank_*` wrappers that return bytes are
for `batch` and tests. A 1 GiB XFS volume materialized as a `Vec` is 1 GiB of
heap and fails outright on the 32-bit Windows target. Prefer
`create(kind, size, opts, sink: &mut impl Write + Seek)`.

Watch out for the per-filesystem options the CLI exposes (`--cluster-size`,
`--sector-size`, `--case-sensitive`, `--min-catalog`, `--affs-variant`,
`--cpm-preset`, `--fat32`, `--inodes` / `--bytes-per-inode`). Do **not** try to
surface all of them per row — take the auto-selected defaults, and leave the
knobs to the CLI. Several filesystems also have fixed geometry (`atari`,
`apple-dos`, `cpm`, `os9`) and must be refused for an arbitrary partition size
rather than silently mis-sized.

---

## 5. `reformat` beyond HFS — medium-large, and largely optional

**Size:** medium-large. Consider skipping.

Formatting **in place** into an existing partition, for the other `FsKind`s.
Item 4 obviates this for the Build Disk case (build the volume in memory, pour
it in), so this is only worth doing for the standalone
`rb-cli reformat IMG@N --fs X` workflow on a disk that already exists.

If it is done, the guard at `reformat.rs:104` is the pattern to keep: refuse
when the formatted volume exceeds the partition, so a neighbour is never
clobbered — plus the trailing-slack zero-fill so no stale bytes survive.

---

## 6. Backlog table writers — DONE

All four shipped on `add-xfs-rdb`; `docs/partition_table_writers_backlog.md` is
now a record of how each writer works rather than a to-do list.

- **XFS creation** — `src/fs/xfs/format.rs`, `rb-cli new volume xfs`. v5/CRC,
  32 MiB minimum, `xfs_repair`-clean to 16 GiB via `scripts/xfs-oracle.sh sweep`.
- **RDB (Amiga)** — `provision::write_rdb`, validated with `amitools` `rdbtool`.
- **Sun (SMI VTOC)** — `provision::write_sun`, validated with `fdisk` / `sfdisk`.
- **AHDI (Atari ST)** — `provision::write_ahdi`.

Two bugs came out of it, both worth knowing about:

- `v5_crc::stamp_sblock_hdr_for_ag` derived a btree block's `bb_blkno` from the
  `agno << sb_agblklog` fsblock encoding, which only equals the physical address
  when `sb_agblocks` is a power of two. Every non-power-of-two-AG XFS had corrupt
  headers above AG 0. Shared with the repair path, so it predated the creator.
- `AG_MAX_BLOCKS` must be `(1 << 28) - 1` — XFS caps an AG at `XFS_MAX_AG_BYTES`
  (1 TiB), not the 4 TiB a `1 << 30` implies.

**What is still open on these formats**, none of it blocking:

- AHDI writes only the four primary slots — no XGM extended chain — and leaves
  the 454-byte bootstrap zeroed, so the disk is not TOS-bootable.
- RDB and Sun can be *written* from scratch but not *edited*; the RDB editor is
  still `SetBootable`-only.

## Verification commands

    cargo fmt && cargo clippy --all-targets        # zero warnings is a hard rule
    cargo test --lib                                # 2763 passing
    cargo test --bin rusty-backup                   # incl. the headless modal tests
    cargo check --lib --no-default-features --features os-stub,native-zstd
    cargo check --lib --no-default-features --features native-zstd,optical
    cargo build --manifest-path rb-cli-vintage/Cargo.toml \
      --no-default-features --features native-zstd,remote,tui,rust173-polyfill \
      --ignore-rust-version

The pre-commit hook runs fmt + clippy `-D warnings` + CLI-doc regeneration. Do
**not** pass `--no-verify` (CONTRIBUTING rule 3).

`cargo check --all-targets` compiles **one** `target_os` arm — "all targets"
means test/bench/example targets, not platforms. When touching anything behind
`#[cfg(target_os)]`, sweep the construction sites by grep and let CI confirm;
`rustup` is not installed here, so cross-target checks are not available.

## Testing tricks that apply to all of these

Real device nodes without touching real disks:

    hdiutil create -size 128m -layout MBRSPUD -fs MS-DOS -volname T target
    hdiutil attach -nomount target.dmg          # -> /dev/diskN
    hdiutil detach /dev/diskN

A compressed source that genuinely exercises the decoder (an uncompressed UDIF
does not): `hdiutil convert in.dmg -format UDZO -o out`.

Check a built image without mounting it: `gpt -r show file.img` reports the exact
LBA of the primary and backup GPT regions. Verify a fill byte-for-byte with
`hdiutil convert -format UDTO` plus `cmp`.

The GUI can be driven headlessly via `egui::__run_test_ui` — see the tests at
the bottom of `gui/partition_editor_modal.rs`. The TUI cannot (it is
TTY-guarded); factor logic into pure functions and unit-test those.
