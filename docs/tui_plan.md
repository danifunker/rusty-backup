# Terminal UI (`rb-cli tui`) — implementation plan

A full-screen, keyboard-driven terminal UI for rusty-backup, inside the `rb-cli`
binary behind the optional `tui` cargo feature. Third first-class surface next to
the egui GUI and the flat CLI, and the native interactive surface for targets the
GUI can't reach (serial console, MiSTer, classic-Mac terminal, SSH onto an old box).

**One rule:** the TUI is a *view* over the shared `src/model/` + `src/fs/` +
`src/cli/verbs/` logic the GUI and CLI already use. No operation logic is
reimplemented in the TUI. Each screen is a plain state struct; the ratatui shell
draws it and feeds it keys. Main file: `src/cli/verbs/tui_app.rs`.

---

## How to work from this plan

- Do the steps **in order**; each is independently shippable and leaves the app
  working.
- When a step lands, tick its box here in the **same commit** and add a one-line
  note of what shipped.
- **Every step ends with the same gates:** `cargo fmt`, `cargo clippy --bin
  rb-cli -- -D warnings`, slim build (`--no-default-features --features
  pure-zstd`), full build (`cargo build`), and a live PTY test of the new screen
  (the `pyte`-driven harness in the session scratchpad).
- **Shared-code output:** anything that calls a CLI verb / shared runner that
  prints to stdout/stderr must wrap it in `with_stderr_suppressed(..)` so it
  doesn't scribble on the alternate screen. Do **not** use `terminal.clear()` to
  paper over bleed — it hits ratatui's clear-then-diff bug and blanks live cells.
- **Long operations** (backup/restore/convert/rip/bulk) already run on worker
  threads in their `src/model/` runner and report through `Arc<Mutex<Status>>`.
  The screen starts the runner, stashes the handle, and each `tick()` locks it to
  render the existing bottom progress bar (`draw_progress`, live rate + ETA).

---

## Done so far

- [x] **Shell** — event loop, 9-tab bar, footer key bar, `?` help overlay,
  adaptive borders (`RB_TUI_ASCII=1` forces ASCII), `NO_COLOR`, min-size guard,
  privilege badge + device-op gating, no-args launch prompt (`[tui] launch =
  ask|always|never`).
- [x] **Navigation** — Left/Right switch tabs, Up/Down move selection, Enter
  drills in, Esc backs out, `1`–`9` jump; vim `hjkl`, `g/G`, Home/End.
- [x] **Inspect** — disk list → device detail; `o` opens a file/backup (path +
  MRU move-to-front via `update::{load_recent,push_recent}` + Tab-to-browse);
  backup-aware (`model::backup_loader::load_backup`). Opened image shows its
  **partition table**; each partition opens a two-pane **Explorer** (tree + list)
  over the shared `resolve` + `open_filesystem` + `list_directory`:
  browse/PageUp-Down/Home-End, **View** (data + resource fork, `r` toggles, hex
  for binary), **Export** (`e`: 5 fork modes + `.tar`/`.tar.gz` + `.mar`),
  **Import** (`i`), **metadata edit** (`m`: HFS/HFS+ Type/Creator + modified
  date), **bless** (`b`, with a confirmation naming the folder it replaces).
- [x] **New Disk** — 3-step wizard (class → filesystem → path/size/name) driving
  `cli::verbs::new::run` (Floppy + Volume; HD/CD-ROM noted as CLI-driven).
- [x] **Backup** — source (image file or physical disk) → config (output/name/
  format/checksum) → run on a worker thread via `backup::run_backup` with the
  live progress bar; output identical to the `rb-cli backup` verb.
- [x] **Restore** — load a backup folder / `.cbk` → config (target/size/
  alignment) → run via `restore::run_restore`; round-trip byte-identical to the
  source. (Device targets deferred to the elevation pass.)
- [x] **Bulk** — source folder → format + review list (un-check to skip) → run
  via `bulk_convert_runner::start_bulk_convert` with per-file progress.
- [x] **Optical (rip)** — local drive list → config (output/format/eject) → run
  via `optical::rip::run_rip`. Wired; needs a physical drive to verify a full rip.
- [x] **Archives** — open a Mac archive, list entries, extract to host (fork
  format cycled with `f`) via `macarchive::extract`.
- [x] **Commander** — dual-pane (host folder / image partition) browse + copy
  between panes (host↔image, host↔host, **image↔image**) via `dir_listing` +
  `commander_ops`; **multi-select** (Space), **sort** (`s`/`S`), **new folder**
  (`n`), **delete** (`x`, confirmed) — all batch-aware.
- [x] **Explorer edits** — `n` new folder, `x` delete, import / metadata / bless,
  **fsck check (`f`) + repair (`F`)**, and a **transform launcher (`t`)** that
  prefills the palette (convert / resize / expand / partition table).
- [x] **Command palette** — `:` runs any `rb-cli` verb (alt-screen suspended);
  also the execution path for the Explorer/Optical/Archives launchers.
- [x] **Optical / Archives image ops** — `i` on Optical opens a browse/extract/
  info/convert/new launcher; `c` on Archives prefills `archive create`.
- [x] **Device targets** — Backup source *and* **Restore target** can be a
  physical disk (target-type chooser + destructive-write confirmation).
- [x] **Settings** — `update::UpdateConfig` editor (two persisted toggles) plus
  **`c` check-for-updates**; `rb-cli update --apply` self-updates (Windows
  self_replace / Unix temp-file+rename).
- [x] **`menu` verb** absorbed — opens the full TUI on the Backup tab (slim
  builds keep the standalone appliance menu).
- [x] **crossterm** deduped to 0.29 (single version in the tree).

Shared modal pieces already built and reusable: `FilePicker` (path + MRU +
Tab-to-browse + make-folder), the bottom progress bar (`draw_progress`), scoped
confirmation overlays, `with_stderr_suppressed`.

---

## Steps to implement (in order)

Each step names the screen, the shared code to wire, and the acceptance check.

### Step 1 — Backup tab  [DONE]
- [x] Source picker: image file via `FilePicker` (Backup MRU) OR physical disk
  from the `enumerate_devices` list (`d`); both become `BackupConfig.source_path`.
- [x] Config form: output folder (Tab-to-browse), name, format
  (Zstd/CHD/VHD/gzip/LZ4/Raw via Left/Right), checksum (sha256/crc32).
- [x] Run on a worker thread via `backup::run_backup(config, Arc<Mutex<
  BackupProgress>>)` (caught-unwind, always sets `finished`); progress mirrored
  into the shared bottom bar (live rate + ETA); Esc cancels via `cancel_requested`.
- [x] Physical-disk source shows an elevation caution when unelevated (the runner
  itself handles the admin prompt on macOS).
- **Verified:** backing up a FAT image through the TUI produces a backup folder
  byte-identical to the `rb-cli backup` verb (metadata.json + partition-N + sha256).

### Step 2 — Restore tab  [DONE]
- [x] Load a backup folder (via `model::backup_loader::load_backup`) or a `.cbk`
  (materialized with `rbformats::cbk::materialize_cbk_to_folder`, temp-dir guard
  moved into the worker so it outlives the restore). Reads `source_size_bytes` +
  partition count from the metadata for defaults.
- [x] Config form: target image path (Tab-to-browse), size policy
  (Original / Minimum, applied to every partition), alignment (Original /
  Modern 1MB). Runs via `restore::run_restore(config, Arc<Mutex<RestoreProgress>>)`
  on a worker thread; progress mirrored into the shared bar; Esc cancels.
- [x] Device targets: Tab on the Target field opens a "Restore target" chooser
  (Image file / Physical device); a device is picked from the shared disk list
  and shown as `<path> [device]`, setting `RestoreConfig::target_is_device`.
  Starting a device restore first raises a destructive-write confirmation naming
  the device + size (with an elevation caution when unelevated); only `y`
  proceeds. The write itself is code-only (feeds the proven `run_restore`; not
  exercised against a real disk here).
- **Verified:** a backup→restore round-trip through the TUI produced an image
  **byte-identical** (`cmp`) to the original source.

### Step 3 — Bulk tab  [DONE]
- [x] Source-folder picker → `bulk_convert_runner::scan_source_folder(source,
  format)` (re-scans when the format changes, since the filter is format-aware).
- [x] Review list with un-check-to-skip (Space toggles `selected`) over a flat
  cursor (Format / Output / files / Start); format cycled with Left/Right; output
  folder Tab-to-browse.
- [x] Run `bulk_convert_runner::start_bulk_convert(...)` (spawns its own thread);
  each tick reads `current_index/total_files/current_bytes` into the shared bar
  and shows the running file; Esc sets `cancel_requested`. (General whole-disk
  formats — Raw/VHD/VHD-Dynamic/QCOW2/VMDK×2/CHD/DVD-CHD; CD/floppy formats with
  input constraints stay CLI-only.)
- **Verified:** converting a 3-image folder to VHD with one file un-checked
  produced exactly the two selected `.vhd` outputs ("Converted 2 ok, 0 failed").

### Step 4 — Optical tab  [rip wired; needs a drive to fully verify]
- [x] Drive list via `model::optical_devices::list_local_rip_devices()`
  (`r` rescans; shows "no drives" when none present).
- [x] Rip: drive -> config (output path / format ISO|BIN-CUE / eject) -> run
  `optical::rip::run_rip(config, Arc<Mutex<RipProgress>>)` on a worker thread
  (same threading + progress-bar pattern as Backup/Restore); Esc cancels.
- [x] Optical *image* operations launcher (`i` on the Drives step): a menu
  (Browse / Extract / Info / Convert / New SGI-EFS) prefills the `:` command
  palette with the matching `optical ...` verb, run through shared CLI dispatch.
  Optical browse/extract use the `opticaldiscs` crate's own filesystem trait (a
  distinct browser from the partition Explorer), so — like the Explorer's
  transforms — they're palette-served rather than a second inline browser.
- **Verified here:** drive enumeration + screen render (no drive in CI sandbox);
  the `i` launcher's "New SGI EFS" ran through the palette and produced a valid
  629 MB SGI-EFS `.iso` (`rb-cli inspect` shows the SYSV slot-7 + VOLHDR layout).
  **Needs a physical drive** to verify an end-to-end rip (wired per the CLI's
  `run_rip` path, so it mirrors a proven flow).

### Step 5 — Archives tab  [DONE]
- [x] Open `.sit/.sea/.cpt/.mar` (+ `.hqx`) via `macarchive::extract::open`; list
  entries (path / type / creator / fork sizes), scrollable.
- [x] Extract to a host folder preserving forks via `macarchive::extract::
  extract_all` in a chosen container format (`f` cycles BinHex / MacBinary /
  AppleDouble / Raw). Synchronous — archives are small.
- [x] Create an archive from host files: `c` on the empty Archives screen
  prefills the `:` palette with `archive create` (the supported create form),
  run through shared CLI dispatch. Creating from *image contents* stays a
  stage-to-host-then-archive job and is deferred.
- **Verified:** opened a `.mar` (entry list matched `rb-cli archive list`),
  extracted to `.hqx` host files; `c` prefills the archive-create command line.

### Step 6 — Commander tab  [core DONE]
- [x] Dual-pane, each pane a host folder (`DirListing::load_host_root`) OR an
  image partition (`commander_source::{probe_partitions, session_for}` +
  `BrowseSession::open` → `DirListing::load_root`; superfloppy = whole-disk
  session). Tab switches focus; Enter/Backspace navigate; partition chooser for
  multi-partition images.
- [x] Copy the selected entry to the other pane's cwd (`c` / F5): host→image
  stages + `commander_ops::apply_edits`; image→host via `fork_export`; host→host
  via `std::fs`. Destination refreshes after.
- [x] image->image copy (`c`/F5): `commander_ops::stage_copy` extracts the entry
  to a temp dir, then `apply_edits` on the destination pane's `BrowseSession`.
- [x] delete (`x`/Del/F8, confirmed) and new folder (`n`, name prompt) on the
  active pane: host = `std::fs`; image = a `DeleteRecursive` / `CreateDirectory`
  staged edit applied through the pane's session. Both refresh after.
- [x] multi-select (`Space` marks/advances, `*` prefix; copy/delete act on the
  marks when any are set, else the cursor entry) and sort (`s` cycles the
  `DirListing::SortColumn`, `S` flips direction) — both over the model's existing
  `ctrl_click`/`selected_entries`/`resort` that the GUI browse view already uses.
- [ ] checksum still deferred (next Commander pass).
- **Verified:** host->image copy put a file into a FAT image (content-identical
  `cmp`); image->image copy left->right through the TUI produced a `SEED.TXT`
  byte-identical to the source; `n`/`x` created then removed a folder on an image
  pane; both images `fsck`-clean afterwards.

### Step 7 — Edit + transform actions (into Inspect/Explorer)  [mkdir/rm/put DONE]
- [x] In the Explorer: `i` import (put), `n` new folder
  (`EditableFilesystem::create_directory`), `x` / Delete remove a file or folder
  (`delete_recursive`, with confirmation), plus the earlier `m` metadata edit and
  `b` bless. All go through `resolve_partition_rw_forced` → `open_editable_
  filesystem` → op → `sync_metadata` → commit, then reopen the view.
- [x] `fsck` check (`f`) + repair (`F`): `f` runs `fs.fsck()` on the open
  partition and shows a scrollable report; `F` reopens read-write through the
  shared repair core (`resolve_partition_rw_forced` -> `open_editable_filesystem`
  -> `repair` -> commit) and shows a repair + re-check report.
- [x] whole-image transforms (`t`): a launcher menu (Convert / Resize / Expand /
  Partition table) prefills the `:` command palette with the right verb + the
  current image path, then runs through the shared CLI `dispatch` — the palette
  is the proven execution path, so the Explorer doesn't reimplement four wizards.
- [ ] `chmeta`/`setrsrc`/`reformat` and `partmap` edits remain palette-served
  (all are CLI verbs the `:` palette already runs); CHD metadata deferred.
- **Verified:** created + deleted through the Explorer (reflected by `rb-cli ls`);
  `f` shows CLEAN FAT12 stats and `F` runs + re-checks (image stays fsck-clean,
  file byte-identical); `t` -> Convert produced a readable `.vhd` via the palette.

### Step 8 — Command palette / REPL  [DONE]
- [x] `:` opens an input line; the text is tokenized with `shell_words::split`,
  parsed into `crate::cli::Command` via a `no_binary_name` clap wrapper, and run
  through `crate::cli::dispatch`.
- [x] The run loop suspends the alt-screen (disable raw mode + LeaveAlternate
  Screen), runs the verb to the real terminal, prints `[Press Enter to return]`,
  waits for a keypress, then re-enters (`terminal.clear()`).
- **Verified:** `ls <img>` from the palette printed the partition table + file
  listing on the suspended screen, then returned cleanly to the TUI.

### Step 9 — `rb-cli update` verb + updater (gated, `tui-update` feature)  [verb DONE]
- [x] `Command::Update` always exists. Without `tui-update` it prints "not built
  into this build" + the releases URL and exits non-zero (exit 2). Also fixed the
  `update` module gate to `any(gui, tui-update)` so a gui-less appliance build
  with `tui-update` still has the checker.
- [x] With `tui-update`: `update::check_for_updates(&cfg.update_check, ver)`
  reports current / latest / up-to-date-or-newer + the platform download URL.
- [x] Auto in-place replace via `rb-cli update --apply` (tui-update): Windows uses
  the existing `self_replace` path; macOS/Linux use the new
  `update::download_and_apply_cli_update_unix` — download the CLI asset, extract
  the `rb-cli` executable (`.zip` / `.tar.gz` / raw ELF/Mach-O), write it beside
  the running binary, `chmod +x`, and atomically `rename` over it. Conservative:
  it refuses to replace unless it positively extracted a valid executable, else
  it errors with the download link (never clobbers the binary).
- [x] Settings "check now" (`c`): runs `check_for_updates` and reports on the
  status line (tui-update); explains how to enable it otherwise.
- **Verified:** default build `rb-cli update` prints the not-built message + URL,
  exit 2. tui-update build: `update --apply` against a real (Windows-only) release
  correctly refused to install a non-matching asset and left the binary intact;
  5 unit tests exercise the zip/tar.gz/raw extraction + reject paths; Settings `c`
  hit GitHub live and reported "Update available: ...". The successful in-place
  swap itself is code-only (needs a matching macOS/Linux release asset to verify).

### Step 10 — Polish leftovers
- [x] Absorb the `menu` appliance verb as an alias into Backup/Restore — when
  the `tui` feature is compiled in, `rb-cli menu` opens the full TUI on the
  Backup tab (`tui_app::run_on(BACKUP_TAB, ..)`) instead of the standalone
  crossterm appliance screen. The old screen stays as the fallback for slim
  builds without `tui` (the i486 appliance), and its `MenuState` unit tests still
  run in the default build.
- [x] Elevation-prompt UX in the TUI — the device-restore destructive-write
  confirmation (names the device + size, elevation caution when unelevated) is
  the interactive gate; the OS auth itself is handled by `os::*` when
  `run_restore` opens the raw device. Physical-disk ops stay badge-gated + noted
  when unelevated across Backup/Restore/Optical.
- [x] crossterm dedupe — bumped our direct dep 0.28 -> 0.29 to match ratatui
  0.30's transitive `crossterm 0.29`; no API changes needed in `menu`/`cli::tui`/
  `bless_pick`/`dir_picker`/`setup`/`tui_app`. Tree now has a single crossterm.

---

## Adopted conventions (already in the shell — keep matching them)

- Top tab bar; `1`–`9` / Tab / Left-Right to switch.
- Persistent footer key bar (3–5 context bindings).
- `?` help overlay; focus = accent border + reversed title.
- Selection = reversed video (survives monochrome); color reinforces, never carries.
- Adaptive borders (rounded Unicode / ASCII fallback); `$NO_COLOR`; min-size guard.
- Confirmation overlays for destructive actions (delete/overwrite/device write).
- Privilege badge; disable physical-disk ops when unelevated.

## Verified runner signatures (so steps don't re-explore)

- **New Disk:** build `cli::verbs::new::NewCommand::Floppy(FloppyArgs{..})` /
  `Volume(VolumeArgs{..})` (all fields pub) → `cli::verbs::new::run(cmd)`.
- **Bulk:** `model::bulk_convert_runner::{scan_source_folder, start_bulk_convert}`;
  status `model::status::BulkConvertStatus`.
- **Optical:** `model::optical_devices::list_rip_devices`; `optical::rip::{RipConfig,
  RipProgress, run_rip}`; `cli::verbs::optical::{OpticalCommand, run}`.
- **Palette:** `shell_words::split` → `rusty_backup::cli::Command` (clap
  `no_binary_name`) → `rusty_backup::cli::dispatch(command)`.
- **Settings / updater:** `update::UpdateConfig::{load, save, user_config_path}`,
  `update::UpdateCheckConfig{enabled, repository_url}`, `update::check_for_updates`,
  `model::update_runner` (Windows self-update today).
