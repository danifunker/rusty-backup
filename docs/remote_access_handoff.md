# Remote access — refactor handoff & continuation prompt

**Read this first, then `docs/remote_transfer_plan.md` (the umbrella design) and the
memory note `rb-daemon-remote-transfer`.** This file is a self-contained prompt to
pick up the remote-access work in a fresh session. It captures (1) what's built,
(2) the new requirement — making remote access reusable across the whole app —
and (3) a concrete model to get there.

---

## 0. One-paragraph mission

Rusty Backup has a working **network daemon** (`rb-cli serve`) and an `rb://`
client that let the desktop browse a remote machine's filesystem *and the disk
images on it*, read/copy files, and (planned) back a remote disk up. Today this
is wired into **Commander Mode only**. The new goal: lift remote access into a
**shared model + reusable file-browser UI** so it's available from the **Backup
tab, Restore tab, Inspect tab, and the CLI/TUI** — and so a user can **switch
between images on one remote without reconnecting**.

---

## 1. What is already built (Family F — implemented + validated)

All under `src/remote/` behind a `remote` cargo feature that is **on by default
and compiled into every shipped build** (desktop, MiSTer armv7, i486/i586
appliance — every CI/Docker build passes `--features …,remote`). It's pure
`std::net` + `serde_json` + `tempfile`, no new dependency. Branch:
`add-crusty-backup-dos-poc`.

### Daemon — `rb-cli serve [--bind 0.0.0.0:7341] [--root DIR] [--staging-dir DIR]`
- `src/remote/server.rs`. Blocking, **thread-per-connection** (`std::net`, no async
  runtime). Each connection owns its own handle + session tables → no shared
  locking. Paths are **sandboxed to `--root`** (run with `--root /` for the whole
  machine; on the MiSTer `--root /media/fat`).
- Runs the *same engine the local CLI runs*: reads via
  `resolve_partition_streaming_forced_inside` → `open_filesystem` →
  `list_directory`/`write_file_to`; writes via `resolve_partition_rw_forced` →
  `open_editable_filesystem` → `create_file`/`create_directory` → `sync_metadata`
  → commit.
- Testable entry: `serve_on(listener, root, staging_dir)` takes a pre-bound
  listener (used by the integration test).

### Wire protocol — `src/remote/protocol.rs`
Length-framed: control frames are `serde_json`; bulk file bytes are a raw chunk
stream (`[u32 len][bytes]…[u32 0]`). `Hello` carries `magic`+`version` (a binary
`Hello` for the JSON-free cb-dos client is a future Family-B addition). Verbs:
- **Read/browse:** `Hello`, `OpenImage{path,partition}` → `Opened{handle, label,
  fs_type, volume_label, total_size, used_size}`, `ListDir{handle,path}`,
  `ReadFile{handle,path}` (→ `FileBegin` + chunk stream), `Close{handle}`.
- **Host-FS browse:** `ListHostDir{path}`, `HostStat{path}` → `HostKind{exists,
  is_dir}`, `ReadHostFile{path}`.
- **Write (stage→apply):** `OpenSession{image_path,partition}` →
  `SessionOpened{session}`, `StageUpload{…}` + chunk stream, `StageMkdir{…}`,
  `StageCopyLocal{…}` (on-device remote→remote), `Apply{session}` →
  `Applied{count}`, `CloseSession{session}`.

**Key fact for the refactor:** the daemon's **handle table is per-connection**.
One connection can `OpenImage` many times → many `handle`s open at once. So **one
TCP connection can hold several open images simultaneously** — exactly what "switch
images without reconnecting" needs.

### Client — `src/remote/client.rs`
- `RemoteSession` (`connect`, `open_image`, `list_dir`, `read_file`,
  `open_session`/`stage_upload`/`stage_mkdir`/`stage_copy_local`/`apply`/
  `close_session`, `list_host_dir`/`host_stat`/`read_host_file`). **Owns one
  `TcpStream` (BufReader+BufWriter). It is blocking, `!Sync`, not `Clone`, and
  does one request/reply at a time.** This constraint drives the refactor design
  (see §3).

### `Filesystem` adapters — `src/remote/fs.rs`
- `RemoteFilesystem` — a `Box<dyn Filesystem>` over an **opened image** (browse
  inside it). Owns its own `RemoteSession`.
- `RemoteHostFilesystem` — a `Box<dyn Filesystem>` over the daemon's **host FS**
  (the file browser; `ListHostDir`/`ReadHostFile`). Owns its own `RemoteSession`.
- **Both open their own connection today** (one `RemoteSession` each). That's the
  thing the new model changes — see §3.

### CLI — `rb://host:port/path@N` refs
Wired into `ls` / `get` / `put` / `mkdir` / `cp` (`src/cli/verbs/`). `ls
rb://host/` browses the host FS; `ls rb://host/img@N /p` browses inside an image;
on-device `cp` when both refs are on the same daemon. So the **CLI already has a
usable remote surface** — a TUI browser can reuse the client directly.

### GUI — Commander remote pane (`src/gui/commander/pane.rs`)
The remote pane is a **file browser**: "Remote…" → connect dialog (host:port
only) → browse the daemon's host FS → **double-click a file / right-click → "Open
Image"** → browse inside it. Plus: a **"Close Image"** button (back to host
browse at the folder you opened from), **teal tinting** of files whose extension
is in `DISK_IMAGE_EXTS`, a shortened source-bar label, and remote→local copy
(image→image stages via the existing arm; image→host extracts over the wire).
`RemoteConn { addr, mode: Host | Image{path,partition} }` marks the pane.
**Limitation it shares with the adapters: opening an image makes a NEW
connection.**

### Tests
`tests/remote_filesystem.rs` — loopback (port-0 listener + `serve_on`): browses a
FAT image (`RemoteFilesystem`) and the host FS (`RemoteHostFilesystem`), reads
files back **byte-exact**. Run with `cargo test --test remote_filesystem
--features remote,pure-zstd,chd` (the `chd` dodges a pre-existing slim-test break
in `rbformats/export.rs`).

### Loopback to try it
```
rb-cli serve --root /some/dir/with/images        # terminal 1 (or --root / for whole machine)
rb-cli ls  rb://127.0.0.1:7341/                   # browse host FS
rb-cli ls  rb://127.0.0.1:7341/disk.img@1 /       # browse inside an image
# GUI: Commander tab -> "Remote..." -> host:7341 -> browse -> double-click an image
```

---

## 2. Locked decisions (don't re-litigate)

- **One daemon, two families** over one TCP transport: **F** = file transfer
  (operation-level, implemented), **B** = backup stream (chunked, planned).
- **Family B is Phase 4**, gated on the cb-dos *local* removable-media round-trip
  proving the native format first (cb-dos Phases 1–4, unstarted). MiSTer
  packaging is Phase 5.
- `remote` is a **cargo feature, on by default, enabled in every build pipeline**;
  build *without* it via `--no-default-features` (omit `remote`).
- GUI remote = a **file browser** (connect → browse host → open image), not
  connect-with-image-path-upfront.
- **Remote-disk BACKUP** = back up a remote **physical drive/partition**, pulled
  to **this desktop**, via the **block tier** (desktop reads remote device blocks)
  feeding the existing `run_backup`. The daemon **enumerates devices and runs
  elevated (admin/root)**; whole-machine browse. **Backup of an image file = a
  transfer** (FTP-like copy), already covered by `ReadHostFile`/`get`.
- **Gotcha:** `run_backup(BackupConfig{ source_path: PathBuf })` is **path-based**
  (`backup/mod.rs:244` opens via `os::open_source_for_reading` then a
  `BufReader`). Remote-disk backup needs a **reader seam** so the engine can take
  a `Read+Seek` (a remote block reader) instead of only a path. Device
  enumeration exists: `os::enumerate_devices() -> Vec<DiskDevice>`
  (`src/os/{mod,linux,macos,windows}.rs`).

---

## 3. THE NEW REQUIREMENT (this is the task)

> "We've scoped this feature entirely to the Commander view, but we'll need to
> access it from many places — the Backup and Restore tabs, the Inspect tab for a
> single image, and via CLI/TUI. For Inspect: maybe an **"Open Remote"** item on
> the file-options list that pulls up a **file-browser window**; the user selects
> the image they want (like Commander), and can **switch images without having to
> reconnect** to the remote."

So: extract remote access out of Commander into a **shared model + reusable UI**,
and wire it into Backup, Restore, Inspect, and the CLI/TUI.

### 3a. The core problem to solve: one connection, many images
Today each `RemoteFilesystem`/`RemoteHostFilesystem` owns its own `RemoteSession`
(one TCP connection), and opening an image reconnects. But the **daemon's handle
table is per-connection** — one connection can hold many open-image handles. The
refactor: introduce a **shared connection** that opens images as *handles* on one
session, so the user browses once and opens/switches images without reconnecting.

### 3b. Proposed model — `RemoteConnection` (new, shared)
A connection object that owns the single `RemoteSession` and brokers all
operations on it:
- `RemoteConnection::connect(addr) -> RemoteConnection` (one TCP session).
- `browse_host(path) -> Vec<FileEntry>` / `host_stat`.
- `open_image(path, partition) -> ImageHandle` (a `handle: u64` on the *same*
  session — no reconnect).
- `list_dir(handle, path)`, `read_file(handle, path, sink)`, `close(handle)`.
- Produces lightweight **`Filesystem` views** bound to `(shared connection,
  handle)` — i.e. `RemoteFilesystem`/`RemoteHostFilesystem` become thin views over
  a shared `RemoteConnection` instead of owning a `RemoteSession` each.

**Threading constraint (critical):** `RemoteSession` is blocking, `!Sync`, not
`Clone`, one-request-at-a-time. A `Filesystem` trait object is `Send` and used
`&mut`, so a *single owner* is fine — but the GUI wants the connection live across
frames *and* to feed it into worker threads (open/copy/backup). Two viable
shapes:
1. **`Arc<Mutex<RemoteConnection>>`** — every op locks the session. Simple;
   serializes all ops on that connection (acceptable — the wire is sequential
   anyway). The `Filesystem` views hold the `Arc<Mutex<…>>` + a handle and lock
   per call. Watch for: a long `read_file` holds the lock (blocks browsing); a
   `Filesystem` method takes `&mut self` and returns owned data, so locking
   inside each method works.
2. **Connection-actor thread** — the session lives on its own thread; ops are
   messages over a channel; the UI/threads get futures/blocking-recv. More code,
   but no lock-holding-across-a-big-read.
   *Recommendation:* start with `Arc<Mutex<RemoteConnection>>` (matches the repo's
   `Arc<Mutex<Status>>` idiom); revisit the actor only if lock contention during
   large reads hurts. Keep image opens (cheap) and file reads (potentially big)
   in mind.

### 3c. Reusable browser UI (egui)
A **`RemoteBrowser` window/component** (own module, e.g.
`src/gui/remote_browser.rs`) usable from any tab:
- Connect (host:port, saved connections), browse host FS, navigate, select an
  image, "Open" → returns a chosen `(RemoteConnection-shared, image handle/path,
  partition)` to the caller.
- **Switch images without reconnecting:** it keeps the `RemoteConnection` and
  opens each picked image as a new handle on it.
- Commander's pane should be refactored to *use* this component (don't fork the
  browse UI again).

### 3d. Per-surface wiring
- **Inspect tab** (`src/gui/inspect_tab.rs`): add **"Open Remote…"** to the file/
  source options → pops the `RemoteBrowser` window → user picks an image → Inspect
  opens it (it already consumes a `Box<dyn Filesystem>` / image source; feed it a
  remote view). Let the window stay open so the user re-picks/ switches images on
  the same connection.
- **Backup tab / Restore tab**: a remote **source** (Backup) / **target**
  (Restore). Backup of a remote *image* = read it remotely; backup of a remote
  *drive* needs §2's block-tier + `run_backup` reader seam (bigger).
- **CLI**: `rb://` refs already work across verbs. `rb-cli backup
  rb://host/<disk> ./out` and `rb-cli restore ./backup rb://host/<target>` are the
  planned forms (plan §11) — the block-tier read for whole-disk is the gap.
- **TUI**: a crossterm remote browser (reuse `RemoteConnection`; follow the
  ASCII-only/TTY-guard MiSTer-TUI conventions, see `bless pick`).

---

## 4. Suggested implementation order

1. **`RemoteConnection`** (shared, one session, multi-handle) in `src/remote/`;
   refactor `RemoteFilesystem`/`RemoteHostFilesystem` into views over it. Keep the
   loopback test green; add a test that **opens two images on one connection**
   (proves no-reconnect switching).
   **DONE — commit `92829b6`.** `src/remote/connection.rs`: `RemoteConnection`
   owns the one `RemoteSession`, tracks open-image handles, brokers
   `open_image`/`close_image`/`list_dir`/`read_file` + host browse;
   `connect_shared(addr) -> Arc<Mutex<RemoteConnection>>`. The two FS adapters are
   now thin views holding `Arc<Mutex<RemoteConnection>>` + a handle, locking per
   `Filesystem` call (`lock_conn` maps a poisoned mutex to an I/O error). New
   constructors: `RemoteFilesystem::on_connection(conn, path, partition)` /
   `RemoteHostFilesystem::on_connection(conn, root_path)` (the no-reconnect path);
   the old `open(addr, …)` stays as a fresh-connection convenience (creates its
   own `connect_shared`), so Commander + the CLI compile unchanged. A
   `RemoteFilesystem` releases its handle on **drop via `try_lock`** (never
   blocks — skips if an op holds the lock, daemon reaps on disconnect). Added
   `RemoteSession::close` for the read-side `Close{handle}`. Both views expose
   `connection()` to clone the shared `Arc<Mutex<…>>` (open another image on it).
   Test: `two_images_open_on_one_connection_without_reconnect` in
   `tests/remote_filesystem.rs` — interleaved byte-exact reads of two images on
   one connection + handle bookkeeping.
2. **`RemoteBrowser`** egui component; refactor Commander's pane to use it
   (behavior parity: connect → host browse → open image → Close Image → copy).
   **NEXT.** The plumbing the pane needs now exists: keep an
   `Option<Arc<Mutex<RemoteConnection>>>` on the browser; **initial connect** (and
   "Remote…" to a new host) calls `RemoteConnection::connect_shared` then
   `RemoteHostFilesystem::on_connection`; **open image** reuses that same
   connection via `RemoteFilesystem::on_connection(conn.clone(), …)`; **Close
   Image** re-browses host via `RemoteHostFilesystem::on_connection(conn.clone(),
   return_dir)` — *no reconnect*. Today's `pane.rs` `spawn_connect_host` /
   `spawn_open_image` each call the old `open(addr, …)` (fresh connection every
   time); the refactor is to thread the shared `conn` through both and only
   create a new one on the initial/explicit connect. Drop ordering is safe: an
   open runs on a worker thread holding an `Arc` clone; when `poll_remote` swaps
   the new fs into the listing, the old view drops on the UI thread and its
   `try_lock` close runs uncontended (the worker has finished).
   **DONE — commits `f11c103` (core) + `09c45c3` (Commander).** Chose the
   testable-core route: `src/model/remote_browser.rs` `RemoteBrowser` =
   `{conn, addr, mode, image_return_dir}` with blocking transitions
   `connect`/`browse_host`/`open_image(path,part,opened_from)`/`close_image`,
   each returning a `BrowseTarget { fs: Box<dyn Filesystem>, root, entries, mode,
   fs_type, volume_label, total/used }` ready for `DirListing::load_root`.
   `#[cfg(feature="remote")]`. Headless test
   `browser_core_opens_switches_and_closes_on_one_connection`. Commander's
   `pane.rs` now holds `Option<RemoteBrowser>` (moved in/out of the worker per
   transition) + a `RemoteConn{addr, BrowseMode}` display cache; `spawn_connect`
   (fresh conn) / `spawn_open_image` (same conn) / `spawn_close_image` (same
   conn); `poll_remote` re-installs the browser + loads the target. A failed
   open/close keeps the connection + listing (status only); a failed fresh
   connect surfaces the error. Removed the duplicate `RemoteMode`, the
   `RemoteOpened` enum, and `remote_host_return`. **GUI wiring is COMPILE-VERIFIED
   ONLY — needs an interactive check** (connect → open image → Close Image → open
   a *different* image, confirming no reconnect; copy still works).
3. **Inspect "Connect to Remote…"** → browse + (eventually) full inspect.
   **PARTIALLY DONE — see the block-tier pivot below.** Shipped: `src/gui/
   remote_browser.rs` `RemoteBrowsePanel` (inline, not a window) wired into the
   Inspect source dropdown (`source_picker` gained `show_remote` + `SourceEvent::
   Remote`); connect → host picker → operation-level browse of an image's files,
   persistent connection, Close-Remote-Image / Disconnect buttons (commit
   `d8696ae`). This is **operation-level** (one daemon-mounted FS = a file
   browser) — it does NOT yet show the partition table or run backup/export/resize
   on a remote image.

### Block-tier pivot (user direction, 2026-06-21): remote images must run Inspect's FULL pipeline

The operation-level model can't give Inspect the partition table / backup /
export / resize (those need seekable raw access to the whole disk). The correct
model is the **block tier**: the daemon serves raw byte *ranges*; the desktop
engine does ALL parsing. **DONE & PROVEN (commit `afaeb91`):**
- **Protocol v2** (additive): `HostFileSize` + `ReadHostRange` verbs, `FileSize`
  response. Server reads a sandboxed file's range (4 MiB/read cap). Client
  `RemoteSession::{host_file_size,read_host_range}` + `RemoteConnection` brokers.
- **`RemoteBlockReader`** (`src/remote/block_reader.rs`): `Read + Seek` over the
  wire, one 256 KiB read-ahead window cached. `Send + 'static` → feeds straight
  into `open_filesystem` / any `Read+Seek` engine code.
- **Handle-based — the image STAYS OPEN on the daemon (commit `dacda9c`).** Per
  user direction, the v2 verbs are now `OpenBlock{path}->BlockOpened{handle,size}`
  / `ReadBlock{handle,offset,len}` / `CloseBlock{handle}`: the daemon keeps the
  file open in a per-connection block-handle table for the session (the user
  works against one open image), and `RemoteBlockReader` closes the handle on
  drop. (Replaced the earlier stateless `HostFileSize`/`ReadHostRange`.)
- **Scope trim (user, 2026-06-21):** **skip export-to-new-files over the wire
  initially** — the remote interaction is "simple read requests, daemon reflects
  the open image." So the near-term integration is inspect + per-partition browse
  + read; backup/export/resize over the wire come later.
- **Headless test** `block_reader_parses_remote_partition_table_and_filesystem`:
  builds a real MBR-partitioned FAT disk, serves it, and over the block reader
  reads byte-exact across seeks, parses the MBR, opens the partition's FS, reads a
  file byte-exact. **The whole engine stack works over ranged reads.**
- **v2 means the MiSTer daemon must be refreshed** to serve ranged reads (image
  *inspection*); the v1 operation-level browse still works on an old daemon.

**Reader seam DONE — remote Inspect is wired (read-only) over the block tier:**
- `run_inspect` (`928810c`) takes a source = local path | `remote:
  Option<(conn,path)>`; the remote branch builds a `RemoteBlockReader`, skips
  device-claim / container-decode / format-detect, and parses the table. Every
  per-partition probe funnels through `make_probe_reader` + the min-size reader,
  both of which build fresh block readers for remote → volume labels / APM / 0x83
  / HFS probes / min-size all work over the wire. Local path untouched.
- Panel is now a **picker**: connect → host browse → pick image → emits
  `inspect_request(conn,path)` and collapses; Inspect runs the full partition
  view. **Browse** (`52e352a`, `BrowseSession.remote` source), **Calc min**
  (`4c40b70`, `MinSizeSource::Remote`), **fsck/Check** (`4b0183d`,
  `run_fsck_reader`), and **Re-inspect** all work over the wire. **Switch images
  without reconnect** (`442184f`, `RemoteBrowser::from_connection` +
  panel.`browse_on` + "Pick Another Image"). Write/path-based actions (Export,
  Edit Table, Add/Resize Partition, HFS Expand/Export) are disabled for remote.
- Headless tests: `browse_session_opens_remote_image_over_block_tier`,
  `fsck_runs_over_block_reader` (+ the block-reader + partition-table tests).
- **NOT yet over the wire (deferred):** **editing** a remote image
  (`open_editable` refuses it), **resize**. fsck **repair** (vs check) is still
  path-based. **Backup of a remote image now works — see step 4.**
4. **Remote-image BACKUP — DONE (per-partition).** `run_backup` is now a thin
   wrapper over `run_backup_from(BackupSource, config, progress)`; the source
   funnels through a `SourceFactory` enum (`Local{File,guard,path}` /
   `Remote{conn,path,size}`) with `open()->Box<dyn ReadSeek>` (cloned `File` /
   fresh `RemoteBlockReader`), `total_size()`, `local_file()`. Generic engine
   paths (table parse, FS probes, `sizes::analyze_partitions`, compaction, trim
   read, gpt.bin, per-partition metadata) go through `factory.open()`; the two
   `File`-bound paths (single-file CHD, HFS+/PFS3 defrag-clone) stay local-only
   via `factory.local_file()` and are gated off for remote (remote+CHD bails;
   remote+shrink warns+ignores). Remote backup = **Zstd / Raw / VHD
   per-partition**, byte-exact. Test `run_backup_pulls_remote_image_byte_exact`.
   GUI: Inspect "Back Up Image..." button (compile-verified, needs interactive
   check). **Still open:** Restore-tab remote **target**; and the bigger
   **remote-disk backup** (block-tier raw-device reads §8 + device enumeration +
   elevated daemon + `ListDevices` verb) — the `SourceFactory` seam is its
   foundation (just feed the device-backed `RemoteBlockReader`).
5. **TUI** remote browser.

GUI work can only be **compile-verified here** (no display in the agent env) — the
engine pieces (`RemoteConnection`, block reader, reader-seam) are **headlessly
testable** over a loopback `serve_on`, so put the confidence there and lean on the
user for interactive GUI checks. Each shippable slice: keep it green
(`cargo fmt` + `clippy --all-targets -D warnings` are enforced by the pre-commit
hook), commit, and validate over loopback.

---

## 5. Gotchas / constraints checklist

- `RemoteSession`: blocking, `!Sync`, not `Clone`, one req/reply. Share via
  `Arc<Mutex>` or an actor thread; never assume two ops can run concurrently on
  one connection.
- Daemon handle table is **per-connection** → one connection = many open images
  (the basis for no-reconnect switching). Close handles you stop using.
- Sandbox: the daemon restricts to `--root`; whole-machine = `--root /` (Linux/
  macOS). Windows drive-letter enumeration (`A:`/`C:`) isn't implemented — a
  `ListDrives`/`ListDevices` verb is needed for that and for device backup.
- `run_backup` is **path-based** — needs a reader seam for remote-disk backup.
- Remote-disk backup needs the daemon to **raw-read devices** (elevated) and a
  `ListDevices` verb; the device enum (`os::enumerate_devices`) and privileged
  raw access (`os::open_source_for_reading`, `src/privileged/`) already exist to
  reuse.
- Reuse existing layers (CLAUDE.md rule): `BrowseSession`/`EditQueue`/
  `DISK_IMAGE_EXTS`/`source_picker`/the copy engine — don't duplicate. Read
  `CONTRIBUTING.md`.
- No Unicode glyphs in UI/log strings (egui default font lacks them) — ASCII only.
- GUI binary is a separate crate: from `src/gui/**` reference the library as
  `rusty_backup::…`, not `crate::…`.

---

## 6. Current commits (newest first, on `add-crusty-backup-dos-poc`)

```
afaeb91 remote: block tier — RemoteBlockReader (Read+Seek over the wire)     <- block-tier core (PROVEN)
d8696ae remote: inline remote browse panel in the Inspect tab (op-level)     <- Step 3 (browse)
09c45c3 remote: Commander pane uses RemoteBrowser — no reconnect on switch  <- Step 2 (GUI)
f11c103 remote: testable RemoteBrowser core (connect / open / switch / close) <- Step 2 (core)
b027e92 docs: mark remote-access Step 1 done; refine Step 2 plan
92829b6 remote: shared RemoteConnection (one session, many open images)   <- Step 1
942e3db remote: UI polish + fix remote-image -> host copy
a161b4e remote: rework GUI pane into a file browser (connect -> browse -> open image)
bc6e672 remote: RemoteHostFilesystem + ReadHostFile — the file-browser foundation
80053d7 remote: keep `remote` a feature (on by default), enabled in all build pipelines
5701b75 remote: Phase 3 (minimal) — GUI Commander remote pane
542cf82 remote: RemoteFilesystem adapter (Phase 3 foundation) + Opened metadata
125060b remote: Phase 2 — host-FS browse + on-device remote->remote copy
1a33d6a remote: Phase 1 — Family F write path (stage->apply) over rb://
7b349d4 remote: Phase 0 — read-only network daemon (rb-cli serve) + rb:// client
85a0546 docs: unify remote networking into one rb-cli serve daemon (two families)
```

Design source of truth: **`docs/remote_transfer_plan.md`** (umbrella) +
**`docs/cb_dos_network_and_state.md`** (Family B deep-dive). Memory note:
`rb-daemon-remote-transfer`.
