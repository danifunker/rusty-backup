# Remote access — RESUME PROMPT (remaining items)

**Paste this to start a fresh session on the remaining remote-access work.**
Read `docs/remote_access_handoff.md` (full design + history) and the memory note
`rb-daemon-remote-transfer` first; this file is the short "what's done / what's
left" pointer.

---

## Where we are (2026-06-21, branch `add-crusty-backup-dos-poc`)

Everything is over the **block tier**: the daemon (`rb-cli serve`, protocol v2)
keeps a file/device open and serves raw byte ranges (`OpenBlock`/`OpenDevice` →
`ReadBlock` → `CloseBlock`); the desktop does ALL parsing via `RemoteBlockReader`
(`Read + Seek`, `src/remote/block_reader.rs`). The `remote` cargo feature is on
by default in every build.

**DONE and headless-tested (10/10 loopback tests in `tests/remote_filesystem.rs`,
`cargo test --test remote_filesystem --features remote,pure-zstd,chd`):**

- **Remote INSPECT (read-only):** Inspect tab → Source ▾ → "Connect to Remote…"
  → host picker → pick an image → partition table, per-partition **Browse**,
  **Check (fsck)**, **Calc min**, **Re-inspect**, **switch images w/o reconnect**,
  **Close Remote**. `run_inspect` is the worked reader-seam (local path | `remote:
  Option<(conn,path)>`).
- **Remote-IMAGE backup** (`d092bc7`): `run_backup` → thin wrapper over
  `run_backup_from(BackupSource, config, progress)`; source funnels through a
  `SourceFactory` enum (`Local{File}` / `Remote{conn,path,size,is_device}`,
  `src/backup/mod.rs`). Inspect tab "Back Up Image…" button.
- **Remote PHYSICAL-DRIVE backup** (engine `ce8fbfc`, Backup-tab GUI `29a4245`)
  — *the headline goal*. Daemon `ListDevices` + `OpenDevice{path}` (validated
  against `enumerate_devices()`, read-only, ioctl size). Backup tab → Source ▾ →
  **"Remote Drive…"** → connect → device list → pick → loads the partition table
  over the wire → Start Backup. Testable core `src/model/backup_remote.rs`.
- **Remote CHD + shrink-to-minimum** (`f406a4a`): when a remote backup needs
  random-access local `File` reads (single-file CHD, or HFS+/PFS3 defrag-clone),
  `run_backup_from` **materializes the remote disk to a local temp once**
  (`materialize_remote_to_temp`, 4 MiB chunks, progress+cancel,
  `TempFileGuard::deleting`) then runs the normal local pipeline — full feature
  parity. Zstd/Raw/VHD still stream directly (no temp).
- **FAT compaction data-loss fix** (`940013b`): `CompactFatReader` (zstd smart +
  CHD) was packing FAT volumes below the FAT-type cluster floor → 16-bit FAT
  re-read as FAT12 → multi-cluster files truncated to one cluster (silent data
  loss on DOS/Win9x disks; pre-existing). Fixed by padding the packed cluster
  count to the type minimum. See `[[chd-fat-packing-truncation-bug]]`.

**Daemon must be the current build** for `ListDevices`/`OpenDevice` + ranged
reads; run it as **root** on the MiSTer (`rb-cli serve --root /media/fat`) — raw
device reads need it.

---

## Remaining items (prioritized)

### 0. INTERACTIVE VERIFICATION (the gate before anything else)
Every GUI surface above is **COMPILE-VERIFIED ONLY** — never run with a display.
On real hardware (the MiSTer), confirm end-to-end and report any breakage:
- Backup tab → "Remote Drive…" → connect → does the device list populate? (needs
  root daemon) → pick a drive → does the partition grid fill in? → Start Backup
  (Zstd) → byte-exact pull. Then **CHD** + **resize-to-minimum** (exercise the
  materialize path).
- Inspect tab "Back Up Image…" (image-file path).
- Re-confirm remote inspect (browse/fsck/calc-min/switch) still works.
Hand the produced backup folder back for desktop verification (metadata
`source_device` = `rb://…/dev/sdX`, partition files round-trip in Inspect).

### 1. Remote EDITING (write back) — `open_editable_filesystem` refuses remote
The next big engine feature. Two options (decide first):
- **(a) Block-level write:** add `WriteBlock{handle,offset,bytes}` verb + a
  read-WRITE `RemoteBlockReader` so `open_editable_filesystem` edits in place over
  the wire. General (any fs edit + repair + resize), writes raw blocks. The
  `block_handles` table already stores `(File,size)` — would need a RW open.
- **(b) Operation-level:** route browse-view edits (add file / mkdir / delete)
  through the daemon's existing stage→apply path (`OpenSession`/`StageUpload`/
  `StageMkdir`/`Apply`, already in Family F). Simpler for file ops; doesn't cover
  arbitrary fs-structure edits. Caveat: a block-opened image (inspect) and a write
  session are different daemon handles — reconcile.
- Recommendation: **(a)** is the general answer and unlocks #2 cleanly; (b) is a
  shortcut for the common add/delete case.

### 2. fsck REPAIR + RESIZE over the wire — gated on #1 (both are read-write).

### 3. Restore-tab remote TARGET
We did Backup (remote source); Restore to a remote drive/image is the mirror.
`restore/mod.rs` writes to a path/device — needs the same write seam as #1
(`WriteBlock` / a remote `Write+Seek`), plus a Restore-tab "Remote…" target
picker (reuse `model::backup_remote` connect + `RemoteSourceState` UI shape).

### 4. TUI remote browser
A crossterm remote browser reusing `RemoteConnection` (ASCII-only, TTY-guard;
follow the `bless pick` conventions, `[[cli-tui-crossterm]]`).

### 5. Family B + MiSTer packaging (handoff §4, plan §)
Family B = the chunked cb-dos backup stream (Phase 4, blocked on cb-dos local
round-trip). MiSTer install packaging/service (Scripts `.sh` + downloader DB).

### Smaller cleanups / risks
- **Superfloppy remote device** (no partition table) — exercise + polish the
  `PartitionTable::None` path for a bare-FS device.
- **macOS `/dev/rdiskN` read-alignment** — `OpenDevice` reads are unaligned;
  fine on Linux/MiSTer (the target), would need aligned reads for a macOS daemon.
- **NTFS/exFAT packer floor** — the FAT fix's sibling: verify the NTFS/exFAT
  `compact_partition_reader` packers don't have an analogous below-minimum bug.
- **Per-partition sizing UI for remote** — the Backup tab's min-size / frag /
  Compact columns stay empty for a remote source (the engine computes shrink on
  the materialized temp, but the user can't pick per-partition custom sizes).
- **Daemon elevation** — we *require* the daemon already runs as root for
  `OpenDevice`; we don't escalate. Fine for the MiSTer; a general desktop daemon
  just errors if unprivileged.

## How to work (constraints)
- **GUI is COMPILE-VERIFY ONLY here** (no display) — put confidence in headless
  loopback tests (`serve_on` + `RemoteConnection`/`RemoteBlockReader` in
  `tests/remote_filesystem.rs`); lean on the user for interactive checks.
- `remote` is **feature-gated**; gate model-layer remote code with
  `#[cfg(feature = "remote")]` (see `backup_remote.rs`, `browse_session.rs`).
  `gui` always pulls `remote`.
- Pre-commit hook enforces `cargo fmt` + `clippy --all-targets -D warnings`.
- Lib must still build **without** the feature:
  `cargo build --lib --no-default-features --features pure-zstd`.
- Keep each slice green, commit, validate over loopback; push the branch to get
  CI builds (a published Release is `main`-only; the armv7 MiSTer `rb-cli-mini`
  artifact is what you reload on the device).

## Commit trail (this push, newest first)
```
940013b fix: CHD/zstd FAT compaction truncated multi-cluster files (data loss)
f406a4a remote: CHD + shrink backup of a remote source (materialize-to-temp)
29a4245 remote: back up a remote physical drive from the Backup tab
ce8fbfc remote: daemon serves physical devices for backup (ListDevices + OpenDevice)
d092bc7 remote: back up a remote image over the block tier (per-partition)
af87ea1 remote: Inspect wired (browse/fsck/calc-min/switch over the block tier)
afaeb91 remote: block tier — RemoteBlockReader (Read+Seek over the wire)
```
