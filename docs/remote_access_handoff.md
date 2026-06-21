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
2. **`RemoteBrowser`** egui component; refactor Commander's pane to use it
   (behavior parity: connect → host browse → open image → Close Image → copy).
3. **Inspect "Open Remote…"** → `RemoteBrowser` → open image; switch without
   reconnect.
4. **Backup/Restore tabs** — remote image source/target first (reuses the read
   path); then the **remote-disk backup** (block tier `ReadAt`/`RemoteBlockReader`
   §8 of the plan + the `run_backup` reader-seam refactor + device enumeration +
   elevated daemon).
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
