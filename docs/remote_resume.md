# Remote access — RESUME PROMPT (remaining items)

**Paste this to start a fresh session on the remaining remote-access work.**
Read `docs/remote_access_handoff.md` (full design + history) and the memory note
`rb-daemon-remote-transfer` first; this file is the short "what's done / what's
left" pointer.

---

## Where we are (2026-06-21, branch `add-crusty-backup-dos-poc`)

> **Update (later 2026-06-21):** Family F is **feature-complete** on the
> engine/CLI/GUI side. Remote **editing**, **fsck repair**, **resize**, and
> **restore-to-a-remote-target** all shipped over the block tier (engine +
> headless tests), and the GUI surfaces are wired (browse-view Edit Mode,
> fsck Repair, Restore-tab "Remote…" target picker) — **compile-verified
> only**. See §1–§3. What remains: (a) the **interactive hardware-verification
> pass** (§0 — only the user can run it), (b) two flagged items — per-partition
> sizing UI for a remote backup source (enhancement) + the NTFS/exFAT packer
> audit (`[[ntfs-exfat-packer-audit]]`, pre-existing, needs a fragmented
> fixture), and (c) the **TUI** (§4, deprioritized — the MiSTer just needs the
> daemon installer/packaging, §5). Also fixed a CI-red FAT32 over-pad
> regression (`993d2ac`).

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

### 1. Remote EDITING (write back) — ENGINE DONE (`22f1812`, option a)
Chose **(a) block-level write**. Added `OpenBlockRw`/`WriteBlock`/`FlushBlock`
(protocol v2, additive), a read-WRITE `RemoteBlockReader` (`open_rw` + `impl
Write`: write-through `WriteBlock` + read-cache patch, `flush` -> `FlushBlock`;
daemon syncs writable handles on `CloseBlock` too). `BrowseSession::open_editable`
now builds a RW reader for a remote source. Devices stay read-only. Headless test
`edit_remote_image_over_block_tier`. **GUI wired (`8da635c`, compile-only):**
`open_browse`'s remote branch now calls `mark_edit_supported()`, so the
browse-view "Edit Mode" button appears for a remote-inspected image and the
staged-edit Apply routes through `session.open_editable()` (the RW reader).
Needs an interactive check on hardware.

### 2. fsck REPAIR + RESIZE over the wire — DONE.
- **Repair** (`65c2919`): `run_repair_reader` (RW reader) + inspect-tab repair
  dispatch builds a RW `RemoteBlockReader` for remote. Headless test
  `repair_remote_image_over_block_tier` (AFFS bitmap mismatch, fixed over the
  wire). GUI repair is the fsck-popup "Repair" button (only gated on
  `result.repairable`, so it lights up for remote) — interactive-verify.
- **Resize** (`d0ba6f3`): FS-only in-place resize (`resize_filesystem_for`) over
  a RW reader — `model::resize_remote::resize_remote_partition` +
  `cli::resolve::resolve_partition_in_reader` (shared `@N` semantics) +
  `rb-cli resize rb://host/img@N --size`. Headless test
  `resize_remote_image_over_block_tier`. Partition-table resize
  (`partition::apply_resize`, moves/truncates) is NOT offered over the wire — it
  would grow the image, which the block tier can't. CLI-only (the GUI resize
  popup is the partition-table kind); no GUI remote-resize surface.

### 3. Restore-tab remote TARGET — ENGINE + CLI DONE (`d79eb60`)
Restore a backup folder to a remote drive/image. `restore::run_restore` is
`File`-bound (SectorAlignedWriter, set_len, mid-restore re-opens, FS
finalization via `&mut File`), so instead of rewriting it target-generic this
**materializes to a local staging image then raw-pushes it** to the remote via
`WriteBlock` (mirror of remote-CHD backup) — every layout works unchanged.
- Daemon `OpenWriteTarget{path,is_device,size}`: device opens RW (validated vs
  `enumerate_devices`, `open_target_for_writing`, elevated); image file is
  created/truncated to `size` (new `sandbox_join_create`) + opened RW.
- `model::restore_remote::restore_to_remote` (run_restore -> staging -> push).
- `rb-cli restore ./backup rb://host/img` (or device path + `--device --yes`).
- Headless test `restore_to_remote_image_round_trips`.
- **GUI wired (`8da635c`, compile-only):** Restore-tab "Restore to a remote
  machine" checkbox -> connect (`connect_and_list_devices`) -> pick a device or
  name an image file -> Start Restore pushes via `restore_to_remote` on a worker
  (shared `resolve_alignment` / `build_partition_sizes` with the local path).
  Device-write target needs an elevated daemon + is interactive-verify only
  (image-file target is the headlessly-tested path). Future optimization: a
  streaming target seam (make `run_restore` target-generic) to skip staging.

### 4. TUI remote browser — DEPRIORITIZED (user, 2026-06-21)
A crossterm remote browser was planned (reuse `RemoteConnection`, ASCII-only,
TTY-guard, `[[cli-tui-crossterm]]`), but the user decided the MiSTer doesn't
need an on-device browser TUI — it just needs the **daemon installer/packaging**
(§5). So skip the TUI; the desktop is the smart client. Revisit only if an
on-device interactive browser is actually wanted later.

### 5. Family B + MiSTer packaging (handoff §4, plan §)
Family B = the chunked cb-dos backup stream (Phase 4, blocked on cb-dos local
round-trip). **MiSTer install packaging/service** (Scripts `.sh` + downloader
DB) is what the MiSTer actually needs to run the daemon — the near-term §4
replacement per the user. Not started.

### Smaller cleanups / risks
> **Per-partition remote sizing + superfloppy remote device — DONE.** Both items
> from the scoped prompt `docs/remote_backup_sizing_resume.md` shipped: the
> `min_size_runner::MinSizeSource::Remote` arm gained an `is_device` flag (picks
> `open_device` vs `open`); the Backup tab now populates the per-partition Min
> Size / Frag / Compact columns for a remote source (cheap FAT/NTFS/exFAT
> eagerly, expensive HFS/ext/btrfs behind the "Calc min" button) and honors the
> chosen sizes by routing remote backups through the same `build_partition_sizing`
> helper as local. Headless tests `remote_min_size_calc_over_block_tier` +
> `run_backup_pulls_remote_superfloppy_byte_exact` in `tests/remote_filesystem.rs`.
> Still **interactive-verify only** for the real device path (root + a real disk).
- **macOS `/dev/rdiskN` read-alignment** — `OpenDevice` reads are unaligned;
  fine on Linux/MiSTer (the target), would need aligned reads for a macOS daemon.
- **NTFS/exFAT packer** — FIXED (`462f371`). Was a confirmed data-loss bug: the
  dense `CompactNtfsReader`/`CompactExfatReader` emitted the boot cluster (LCN 0)
  twice (prepended boot region + `used_cluster_list[0]==0`), misaligning the
  stream; with the MFT's absolute-LCN runs unchanged and restore writing verbatim
  + zero-padding, non-resident files were silently corrupted in smart mode. The
  old test passed only by a hole/dup cancellation coincidence + read a resident
  file. Fix routes NTFS/exFAT smart compaction through `into_layout_preserving()`
  (position-preserving). Regression tests in `tests/filesystem_e2e.rs`
  (`*_smart_compaction_is_layout_preserving`, `ntfs_compaction_preserves_nonresident_file`).
  See `[[ntfs-exfat-packer-audit]]`.
- **Per-partition sizing UI for remote** — DONE (see the merged item above):
  `poll_remote` now fills the maps, `start_min_size_calc` has a Remote arm, and
  the `min_size_runner` Remote arm takes an `is_device` flag so a Backup-tab
  remote source opens via `open_device`.
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
- Pre-commit hook enforces `cargo fmt` + `cargo check` + `clippy --all-targets
  -D warnings` — but **NOT tests**. Run the full `cargo test` (not just `--lib`)
  before committing FS/format changes: the FAT32 over-pad regression (`993d2ac`)
  passed lib+remote tests but broke `tests/filesystem_e2e.rs` on every CI target.
- Lib must still build **without** the feature:
  `cargo build --lib --no-default-features --features pure-zstd`. (Also confirm
  the slim CLI: `cargo build --bin rb-cli --no-default-features --features
  pure-zstd` — remote CLI branches must be `#[cfg(feature = "remote")]` gated.)
- Keep each slice green, commit, validate over loopback; push the branch to get
  CI builds (a published Release is `main`-only; the armv7 MiSTer `rb-cli-mini`
  artifact is what you reload on the device).

## Commit trail (this push, newest first)
```
462f371 fix: NTFS/exFAT smart compaction corrupted non-resident files (data loss)
8da635c remote: wire GUI edit-mode + Restore-tab remote target
d79eb60 remote: restore a backup to a remote target (device or image)
d0ba6f3 remote: resize a remote filesystem in place over the block tier
993d2ac fix: FAT compaction over-padded FAT32, breaking small volumes (CI red)
65c2919 remote: fsck repair over the wire (run_repair_reader)
22f1812 remote: edit a remote image in place over the block tier (WriteBlock)
940013b fix: CHD/zstd FAT compaction truncated multi-cluster files (data loss)
f406a4a remote: CHD + shrink backup of a remote source (materialize-to-temp)
29a4245 remote: back up a remote physical drive from the Backup tab
ce8fbfc remote: daemon serves physical devices for backup (ListDevices + OpenDevice)
d092bc7 remote: back up a remote image over the block tier (per-partition)
af87ea1 remote: Inspect wired (browse/fsck/calc-min/switch over the block tier)
afaeb91 remote: block tier — RemoteBlockReader (Read+Seek over the wire)
```
