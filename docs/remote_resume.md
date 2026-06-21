# Remote access — RESUME PROMPT (remaining items)

**Paste this to start a fresh session on the remaining remote-access work.**
Read `docs/remote_access_handoff.md` (full state + design) and the memory note
`rb-daemon-remote-transfer` first; this file is the short "what's left" pointer.

---

## Where we are (2026-06-21, branch `add-crusty-backup-dos-poc`)

Remote **image inspection is fully wired, READ-ONLY, over the block tier**. The
daemon keeps an image open (`OpenBlock`/`ReadBlock`/`CloseBlock`, protocol **v2**)
and serves raw byte ranges; the desktop does all parsing via
`RemoteBlockReader` (`Read + Seek`, `src/remote/block_reader.rs`). Working over
the wire in the **Inspect** tab: Source ▾ → "Connect to Remote…" → host picker →
double-click an image → **partition table**, per-partition **Browse**, **Check
(fsck)**, **Calc min**, **Re-inspect**, **switch images without reconnect** ("Pick
Another Image"), **Close Remote**. Write/path actions (Export, Edit/Add/Resize
Partition, HFS Expand/Export) are disabled for remote.

`run_inspect` (`src/gui/inspect_tab.rs`) is the **worked reader-seam example**:
source = local path | `remote: Option<(conn, path)>`; the remote branch builds a
`RemoteBlockReader`, skips device-claim / container-decode / format-detect, and
every per-partition probe funnels through `make_probe_reader` (+ the min-size and
fsck readers), which build fresh block readers for remote. Local path untouched.

6 loopback tests green: `tests/remote_filesystem.rs` (block-reader partition-table
parse, `BrowseSession.remote`, `run_fsck_reader`, connection sharing, browser
core). Run: `cargo test --test remote_filesystem --features remote,pure-zstd,chd`.

**v2 daemon required** for ranged reads — restart `rb-cli serve` from current
source. The v1 operation-level browse (Commander) still works on an old daemon.

## Remaining items (prioritized)

### 1. Remote-image BACKUP (pull to the desktop) — DONE (per-partition)
**DONE — reader seam shipped.** `run_backup` is now a thin wrapper over
`run_backup_from(BackupSource, config, progress)` (`src/backup/mod.rs`). The
source funnels through a `SourceFactory` enum (`Local{File,guard,path}` /
`Remote{conn,path,size}`) that mints fresh seekable readers: `open() ->
Box<dyn ReadSeek>` (cloned `File` locally, a fresh `RemoteBlockReader` over the
wire), `total_size()`, `local_file()`. Every generic engine path (partition-table
parse, FS probes, `analyze_partitions` in `sizes.rs`, compaction, trim read,
`gpt.bin` export, per-partition metadata) goes through `factory.open()`. The two
`File`-bound paths — **single-file CHD** and **HFS+/PFS3 defrag-clone** — stay
**local-only** via `factory.local_file()` and are gated off for remote (remote +
CHD → clear bail; remote + shrink-to-minimum → warn + ignore). So remote backup =
**Zstd / Raw / VHD per-partition**, which round-trips byte-exact.
- Headless test `run_backup_pulls_remote_image_byte_exact`
  (`tests/remote_filesystem.rs`): partitioned FAT image → serve → `run_backup_from`
  over the block tier → asserts `partition-0.raw` is byte-exact vs the source
  partition, `mbr.bin` == sector 0, and metadata records the `rb://…` source +
  full size. 7/7 remote loopback tests green.
- GUI (compile-verified only): Inspect tab → inspect a remote image → **"Back Up
  Image..."** button picks a destination folder and spawns
  `run_backup_from(BackupSource::Remote)`.
- **NOTE (user feedback):** image-file backup is "cool but not the target" — the
  real goal is the physical-drive backup below. See memory
  `[[remote-backup-priority]]`.

### 1b. Remote PHYSICAL-DRIVE backup — DONE (the actual goal)
**DONE — engine `ce8fbfc` + Backup-tab GUI `29a4245`.** The daemon enumerates its
own disks and serves a raw device over the same block tier; the desktop pulls a
backup of a remote drive without moving the media.
- **Daemon (`ce8fbfc`):** protocol v2 adds `ListDevices -> Devices{Vec<WireDevice>}`
  and `OpenDevice{path}` (device-backed sibling of `OpenBlock`). `OpenDevice` is
  **not** sandbox-joined under `--root`; the path must match a live
  `enumerate_devices()` entry exactly (so it can't read arbitrary files),
  read-only, size via ioctl (a device's `metadata().len()` is 0). `block_handles`
  now stores `(File, size)`. **Needs root** on the daemon for raw-disk read (the
  MiSTer runs as root, so fine).
- **Desktop:** `RemoteBlockReader::open_device`; `BackupSource::Remote` /
  `SourceFactory::Remote` gain `is_device` → `run_backup_from` opens the right
  kind; the rest of the pipeline is identical (a drive backs up exactly like a
  remote image). Testable core `src/model/backup_remote.rs`
  (`connect_and_list_devices` / `load_remote_source`).
- **Backup tab GUI (compile-verified only):** source dropdown → **"Remote
  Drive..."** → connect dialog (host:port) → device list → pick a drive → loads
  its partition table over the wire → Start Backup runs
  `run_backup_from(BackupSource::Remote{is_device:true})`. Zstd/Raw/VHD
  per-partition (CHD + shrink steered off). `RemoteSourceState` + `poll_remote` +
  `show_remote_picker` + `start_remote_backup`.
- Tests: `list_devices_round_trips_and_open_device_reads` (ListDevices round-trips;
  `/etc/hostname` refused as "not an enumerated device"; real-device read is
  best-effort, skipped without root) + `backup_remote_core_lists_and_loads_partitions`.
  9/9 remote loopback tests green.
- **TO TEST:** reload `rb-cli` on the MiSTer (new daemon verbs) + interactive
  Backup-tab check: connect → pick a drive → Start Backup → byte-exact pull.
- **Still open:** Restore-tab remote **target**; whole-disk (no partition table)
  device handling polish; macOS `/dev/rdiskN` read-alignment (the daemon target is
  Linux/MiSTer, so unaddressed).

### 2. Remote EDITING (write back) — `open_editable` refuses remote today
Two options (decide first):
- **(a) Block-level write:** add a `WriteBlock{handle,offset,bytes}` verb +
  read-WRITE `RemoteBlockReader` so `open_editable_filesystem` edits in place over
  the wire. General (any fs edit + repair + resize) but writes raw blocks.
- **(b) Operation-level:** route the browse-view edit ops (add file / mkdir /
  delete) through the daemon's **existing** stage→apply write path
  (`OpenSession`/`StageUpload`/`StageMkdir`/`Apply` — already implemented + tested
  in Family F). Simpler for file add/delete; doesn't cover arbitrary fs-structure
  edits. Caveat: a block-opened image (inspect) and a write session are different
  daemon handles — reconcile.
- Unlocks fsck **repair** (vs check) and **resize**.

### 3. fsck REPAIR + RESIZE over the wire — gated on #2 (both read-write).

### Other plan surfaces (handoff §4)
Backup/Restore tab remote source/target; TUI remote browser; Family B (cb-dos
backup stream); MiSTer packaging.

## How to work (constraints)
- **GUI is COMPILE-VERIFY ONLY here** (no display) — put confidence in headless
  loopback tests (`serve_on` + `RemoteConnection`/`RemoteBlockReader` in
  `tests/remote_filesystem.rs`); lean on the user for interactive checks.
- `remote` is **feature-gated**; gate model-layer remote code with
  `#[cfg(feature = "remote")]` (see `browse_session.rs`, `min_size_runner.rs`).
  `gui` always pulls `remote`.
- Pre-commit hook enforces `cargo fmt` + `clippy --all-targets -D warnings`.
- Lib must still build **without** the feature:
  `cargo build --lib --no-default-features --features pure-zstd`.
- Keep each slice green, commit, validate over loopback; push the branch to get
  CI desktop builds (a published Release is `main`-only).
