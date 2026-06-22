# Resume prompt — remote per-partition sizing UI + superfloppy remote device

**Paste this into a fresh session to pick up these two Backup-tab items.** They
are the last "Smaller cleanups / risks" left on the otherwise feature-complete
remote/Family-F work. First read `docs/remote_resume.md` (the umbrella state) and
the memory note `rb-daemon-remote-transfer`; this file is the focused, actionable
scope for just these two.

## Where things stand
Family F (desktop ⟷ daemon) is feature-complete: remote inspect / backup / edit /
fsck-repair / resize / restore all ship (engine + CLI + GUI wiring), each with a
headless loopback test. Two recent data-loss bugs were fixed (FAT32 over-pad
`993d2ac`; NTFS/exFAT packer `462f371`). These two items are **GUI enhancements**,
not gaps — whole-disk and shrink-to-minimum remote backup already work; you just
can't currently pick *per-partition* sizes for a remote source, and the
bare-FS-device (superfloppy) remote path is only lightly exercised.

## Constraints (unchanged)
- **GUI is COMPILE-VERIFY ONLY in the agent env** (no display). Put confidence in
  headless loopback tests (`serve_on` + `RemoteConnection`/`RemoteBlockReader` in
  `tests/remote_filesystem.rs`); lean on the user for interactive checks.
- `remote` is feature-gated: gate model/GUI remote code with
  `#[cfg(feature = "remote")]`.
- Pre-commit runs `cargo fmt` + `cargo check` + `clippy --all-targets -D warnings`
  — **NOT tests**. Run the full `cargo test` before committing (the FAT32 bug
  slipped past lib+remote tests but broke `tests/filesystem_e2e.rs`).
- Lib must build without the feature
  (`cargo build --lib --no-default-features --features pure-zstd`) and the slim
  CLI too (`cargo build --bin rb-cli --no-default-features --features pure-zstd`).
- Keep each slice green, commit, push the branch (`add-crusty-backup-dos-poc`) for
  CI.

---

## Item 1 — Per-partition sizing UI for a remote backup source

**Goal:** for a remote backup source (Backup tab → "Remote Drive…"), populate the
per-partition **Min Size**, **Frag.**, and **Compact** columns and let the user
pick per-partition sizes — the same UX local sources already have. Today those
columns stay blank for remote.

### Why they're blank (root cause)
The per-partition sizing maps (`partition_min_sizes`, `partition_fragmentation`,
`partition_defrag_min_sizes`, `deferred_min_sizes`, `partition_defrag_enabled`)
are never populated for a remote source. Specifically (`src/gui/backup_tab.rs`):
- `poll_remote` (~lines 1823–1867) loads `RemoteSourceInfo.partitions` into
  `source_partitions` and `select_all_partitions()`, but never kicks a min-size
  calc nor sets `deferred_min_sizes`. The "stays empty" comment is at ~1858–1861.
- `source_path(ctx)` (~1696–1704) returns `None` for a remote source, and
  `start_min_size_calc` (~1078–1140) has **no `MinSizeSource::Remote` arm** — it
  falls into the `_ =>` File arm, finds `source_file == None`, logs "no open
  source handle", and returns.
- The partition grid (`show`, grid block ~454–567) does NOT gate on
  `remote_active()` — the cells just fall through to `ui.label("")` because the
  backing maps are empty. So **no grid changes are needed** — just fill the maps.

### The runner already does the hard part
`src/model/min_size_runner.rs` already has a working `MinSizeSource::Remote`
(~lines 76–80 enum, ~160–181 `spawn` arm) that builds a `RemoteBlockReader` and
runs `fs::partition_minimum_size` (which computes in_place min, defragmented min,
AND fragmentation % — all reader-generic, no temp needed). It's proven by the
Inspect tab (`inspect_tab.rs` ~4732–4736).

### THE GOTCHA — image vs device opener
The runner's `Remote` arm opens via `RemoteBlockReader::open(conn, &path)` — the
**image-file** opener. A Backup-tab remote source is a **physical device**
(`remote.selected` holds a device path; backup uses `open_device` /
`BackupSource::Remote { is_device: true }`). Opening a device path through the
file-open RPC will fail or misbehave. **Fix the runner first:** add an
`is_device: bool` to `MinSizeSource::Remote` (or add a `RemoteDevice` variant) and
call `RemoteBlockReader::open_device` when true. Update the existing Inspect
caller to pass `is_device: false` (it inspects an image).

### Implementation steps
1. **`min_size_runner.rs`:** add `is_device` to `MinSizeSource::Remote`; in
   `spawn`, pick `open_device` vs `open`. Update the Inspect-tab caller
   (`is_device: false`).
2. **`backup_tab.rs` `start_min_size_calc`:** add a Remote branch *before* the
   `source_path` match — when `self.remote_active()`, build
   `MinSizeSource::Remote { conn: self.remote.conn.clone()?, path: <device path
   from remote.selected>, is_device: true }`. (Reuse the request fields:
   `partition_offset = part.start_lba * 512`, type byte/string, size, index.)
3. **`backup_tab.rs` `poll_remote`:** after loading `source_partitions`, mark each
   FS-bearing partition as **deferred** (`deferred_min_sizes.insert(idx, fs_name)`)
   so a **"Calc min"** button appears per partition — do NOT auto-walk every
   partition over the wire (HFS+/ext/btrfs catalog walks are many ranged
   round-trips and can be very slow on a remote link; deferring matches the
   local "expensive" convention). Use `fs::is_expensive_minimum(type, type_str)`
   to decide deferred vs eager; FAT/NTFS/exFAT are cheap and can be kicked
   eagerly via `start_min_size_calc`. Replace the "stays empty" comment.
4. Once a calc finishes, the existing `poll_min_size_calcs` →
   `record_min_size_result` path fills `partition_fragmentation` +
   `partition_defrag_min_sizes` and auto-enables the **Compact** toggle — no extra
   work; the grid renders it.
5. **Honor the picked sizes in the backup.** Verify `run_backup_from` for a remote
   source honors `BackupConfig.partition_target_sizes` and
   `defrag_partition_indices`. For remote+CHD/shrink it materializes to a local
   temp then runs the local pipeline (`backup/mod.rs` ~445–457), so per-partition
   sizes should flow through; confirm the non-CHD remote path
   (`SourceFactory::Remote`, ~474) also respects them. If `start_backup` for
   remote bypasses the size-collection plumbing (it routes straight to
   `start_backup` at ~742–747, skipping the VHD/CHD min-size kickoff), make sure
   the chosen `partition_target_sizes` are still assembled and passed.

### Verification
- **Headless:** extend `tests/remote_filesystem.rs` — connect over loopback, then
  drive `min_size_runner::spawn` with `MinSizeSource::Remote { is_device: false }`
  against a served FAT/HFS image and assert it returns a min size (and frag for
  HFS). This proves the runner path end to end over the wire. (The *device*
  `is_device: true` path needs root + a real device → interactive.)
- **Interactive (user, on the MiSTer):** Backup tab → Remote Drive → connect →
  pick a drive → per-partition "Calc min" populates Min Size/Frag, Compact toggle
  appears → pick a custom/min size → Start Backup → byte-exact, correctly-sized
  backup folder.

---

## Item 2 — Superfloppy remote device (bare FS, no partition table)

**Goal:** confirm + polish backing up a remote **bare-filesystem device** (no
MBR/GPT — a raw FAT/HFS/etc. device) end to end. Mostly verification with light
polish.

### What already happens
`model::backup_remote::load_remote_source` runs `PartitionTable::detect`; a
bare-FS device yields `PartitionTable::None { fs_hint }` → `is_superfloppy = true`
and **one** `PartitionInfo` at offset 0 (`PartitionTable::None.partitions()`
returns a single synthetic partition — see the test at the bottom of
`src/partition/mod.rs` asserting `table.partitions()[0].type_name == "FAT"`).
`poll_remote` loads that one partition and `select_all_partitions()`. The backup
engine has a superfloppy path (`backup/mod.rs` ~655, 720, 930, 1370, 1690, 1809;
`backup/sizes.rs:76`). So the plumbing should already work.

### What to exercise / polish
1. **Headless:** add a loopback test in `tests/remote_filesystem.rs` that serves a
   **bare FAT superfloppy image** (e.g. `fat::create_blank_fat` + a file, served
   as an image with `is_device=false`) and runs `run_backup_from(BackupSource::
   Remote { is_device: false, .. })`, asserting the captured partition is
   byte-exact and `metadata.json` records `layout`/`partition_table_type ==
   "None"` (mirror `run_backup_pulls_remote_image_byte_exact`, but superfloppy).
   This proves the no-table path over the wire without hardware.
2. **Polish:** confirm the Backup tab shows the single partition + table desc
   ("No partition table (superfloppy)") and that Item 1's per-partition sizing
   works for that single offset-0 partition.
3. **Interactive (user):** a real bare-FS USB stick on the MiSTer (device path,
   `is_device=true`) → backs up correctly.

### Watch for
- `seek(End(0))` returning 0 for device files (macOS) — the engine already has a
  superfloppy size fix-up (`backup/mod.rs` ~635, `backup_tab.rs` ~1379); make sure
  the remote device size comes from the daemon's `OpenDevice` ioctl size
  (`RemoteBlockReader::len()`), not a `seek(End)`.

---

## Suggested order
Do **Item 1** first (the runner `is_device` fix + Backup-tab wiring + headless
image-remote min-size test), commit, then **Item 2** (superfloppy loopback backup
test + verify Item 1 covers the single partition), commit. Push for CI. Hand the
interactive (device) checks to the user with a short test script.

## Acceptance criteria
- Remote backup source shows per-partition Min Size / Frag / Compact, populated
  via the "Calc min" button (expensive FS) or eagerly (FAT/NTFS/exFAT).
- A per-partition custom/minimum size chosen for a remote source is honored by the
  produced backup.
- Headless loopback tests: remote min-size calc (image), and superfloppy remote
  backup byte-exact. Full `cargo test` green; clippy `-D warnings` clean; slim
  lib + CLI build clean.
