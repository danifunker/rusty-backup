# Remote Transfer, Networked Backup & `rb-cli serve` — Unified Design Plan

**Status:** design / idea-capture (no code yet). Iterate against this doc rather than re-deriving in chat.

**One-line:** one network daemon (`rb-cli serve`, flagship-deployed on the MiSTer FPGA, runnable on any
host) speaking **one TCP transport** that exposes **two operation families** — a rich *file-transfer*
family (browse a remote machine and the disk images on it, copy files between images in any direction) and
a chunked, resumable *backup-stream* family (push/pull a whole-disk backup over the wire, restore it back).
The constrained DOS client (cb-dos / `CRUSTYBK.EXE`) speaks only the backup-stream family, JSON-free, over
mTCP.

This doc is the **umbrella**. The backup-stream family's deep internals (chunk container, resume map,
fingerprint, manifest, swap exclusion) live in [`cb_dos_network_and_state.md`](cb_dos_network_and_state.md)
and are cross-referenced here, not duplicated. The DOS-side mTCP transport user guide is
[`cb_dos_networking.md`](cb_dos_networking.md).

---

## 0. The two operation families at a glance

Everything below hangs off this split. One daemon, one port, one handshake; two families negotiated by a
capability flag.

| | **Family F — File transfer** | **Family B — Backup stream** |
|---|---|---|
| What it moves | individual files, into/out of/between disk images | a whole disk/partition backup (the native PerPartition format), resumably |
| Granularity | operation-level (`OpenImage`/`ListDir`/`ReadFile`/`Stage*`/`Apply`) | chunk-level (source-keyed gzip members in a `.cbk` container) |
| Control encoding | `serde_json` control frames + raw payloads | **binary** chunk frames (C-implementable on a 486) |
| Canonical client | desktop GUI (Commander Mode) / `rb-cli` | cb-dos / `CRUSTYBK.EXE`; also the desktop for whole-disk capture |
| Resumable? | per-`Apply` atomic batch (staging) | yes — ddrescue-style transfer map, the headline feature |
| Reaches *inside* images? | **yes** (whole point) | no — it streams the block image + a file manifest sidecar |
| Deep-dive doc | this doc (§5.1, §6) | [`cb_dos_network_and_state.md`](cb_dos_network_and_state.md) (§2–§6) |

**Family B is symmetric** — it has a **Producer** role (smart-reads a disk → emits source-keyed gzip
members) and a **Consumer** role (lands members into a `.cbk` / native folder behind a resume journal, or
writes them onto a live disk with resize). *Backup vs restore* and *push vs pull* are just which endpoint
plays producer vs consumer and whether the target is a backup artifact or a live disk. One protocol covers
all of:

- **cb-dos pushes its own backup** — DOS box = producer (its C imaging code), host = consumer (lands the
  `.cbk`). *The original cb-dos use case.*
- **Desktop pulls a remote disk's backup** — remote daemon = producer (runs the engine's smart-imaging read
  on the disk it holds), desktop = consumer (writes a local backup folder). *Heavy FS-aware read stays on
  the host that has the disk — no block round-trip, same rationale as §2.2.*
- **Restore over the wire** — whoever holds the backup is producer, whoever holds the target disk is
  consumer; members flow the other way; consumer resizes as it writes. Covers cb-dos pulling a restore onto
  its card **and** the desktop pushing a restore onto a remote disk.

(All three backup directions are in scope per the design decision of 2026-06-20.)

---

## 1. Goals & use cases

### Family F — file transfer (reach into a remote machine)
- Browse a remote machine's **host filesystem** (e.g. the MiSTer SD card under `/media/fat`).
- **Open a disk image that lives on the remote** and browse *inside* it (FAT/HFS/Amiga/etc.).
- **Copy files between images**, in every direction:
  - desktop image → remote image (the headline case: drop files into the MiSTer's image),
  - remote image → desktop image,
  - **remote image → remote image, entirely on-device** (both files already on the MiSTer — must not
    round-trip the data through the desktop),
  - desktop ↔ desktop already works today (local Commander Mode).

### Family B — networked backup (move a whole disk's backup over the wire)
- **Vintage box pushes its own backup** — cb-dos images its local disk via `int 13h` and streams the
  native backup to a host running `rb-cli serve`, **resumably**, without shuffling floppies.
- **Desktop pulls a remote disk's backup** — back up a remote MiSTer/host's whole disk over the network
  into a local backup folder; the daemon does the FS-aware read, the desktop just journals members.
- **Restore over the wire** — push a backup back onto a remote/vintage disk (with resize), either
  direction.

### Common
- Works with **two MiSTers**, a MiSTer + your hard drive, an image on your local disk, or a vintage DOS box
  — the daemon is platform-generic; the MiSTer is just the flagship packaged target.
- **Manual IP:port to connect** for v1. Auto-discovery is deferred (§14).
- **Live transfer telemetry**: bytes / KiB transferred and transfer speed, plus a list of active
  connections and in-flight operations (both families).

### Non-goals (for v1)
- mDNS / zeroconf discovery, TLS, multi-user accounts, pairing — all deferred to later phases.
- Re-implementing rusty-backup's filesystem engine anywhere else (notably *not* in Go / mrext).
- **cb-dos as a server.** The DOS box is a Family-B *client* only (producer or consumer); it never runs a
  daemon or serves operation-level browse — a 486 has no business hosting Family F.

---

## 2. Key architectural decisions (with rationale)

### 2.1 The daemon is `rb-cli` with a `serve` verb (single binary, in-process engine)
The daemon links the **same library functions `rb-cli` already calls** — `fs::open_editable_filesystem`,
`create_file`, `sync_metadata`, `write_file_to`, and the backup orchestration in `src/backup/` —
directly, in-process. It does **not** shell out to `rb-cli`. rb-cli, the GUI, and the daemon are three
front-ends over one engine crate.

- Ship as a **subcommand of `rb-cli`** (`rb-cli serve …`). One build, one release artifact, one thing to
  keep updated. The MiSTer `rb-daemon.sh` in the Scripts folder is that same binary.
- Optionally add a 30-line `[[bin]] rb-daemon` shim that calls `remote::serve` purely for a clean
  process / service / log identity (`ps` shows `rb-daemon`, not `rb-cli serve`). Cosmetic.
- Build for the MiSTer with the existing slim profile: `--no-default-features` (GUI/optical off),
  `--features chd` if CHD images are kept on the device (rides the existing
  `build-rb-cli-mini-armv7` CI job, `.github/workflows/release.yml:727`).

### 2.2 Operation-level protocol for Family F (not block-level), with an optional block tier as fallback
Three options were weighed for how the Family-F wire boundary works:

| Option | Verdict |
|---|---|
| **Shell out to `rb-cli`** per file | Rejected: process spawn per op, reopens the image every time (no batching), can't stream fine-grained progress out of a subprocess. |
| **Dumb block server** (`ReadAt`/`WriteAt`); all FS brains on the desktop | Rejected as primary: a remote→remote copy would round-trip *all the file data* desktop→MiSTer→back for files that already live on the device. Absurd for a big HDF. Kept only as an optional **fallback** (§8) for formats the daemon build can't decode. |
| **In-process engine, operation-level protocol** | **Chosen.** Batching, byte-accurate progress, and on-device remote→remote with zero data round-trip all fall out for free. |

The same "keep the FS-aware work next to the disk" logic is why **Family B's pull direction** runs the
smart-imaging *producer* on the daemon, not the desktop (§0).

### 2.3 One unified daemon, two families, one transport (chosen 2026-06-20)
Earlier these were two designs: `rb-cli serve` (Family F, this doc) and `rb-cli net-serve` (Family B, the
cb-dos doc). They are now **one daemon verb (`rb-cli serve`)** sharing:

- **one TCP transport + framing** (§5.0),
- **one handshake** — a **binary `Hello`** + capability flags, so the cb-dos client never has to parse
  JSON,
- **one version contract** (`DAEMON_VERSION` / `MIN_DAEMON_VERSION`).

After the handshake the client declares which family it's using; the daemon routes. A client implements
only the families it needs: the desktop does both; **cb-dos does only Family B**. `net-serve` is dropped
(folded into `serve`); the cb-dos doc is annotated to match.

**Why binary `Hello` (not JSON):** the only thing cb-dos *must* speak to reach Family B is the handshake.
Keep that handshake and Family B's chunk frames **binary** and the 486 client never touches a JSON parser.
Family F can layer `serde_json` control frames on top for the desktop's benefit; cb-dos is blind to them.

### 2.4 Relocate Commander's stage→apply pipeline onto the daemon (Family F)
The requirement "stage data onto the MiSTer first, then the apply step does the diff; batch multiple
files; cache" **is Commander Mode's existing `StagedEdit` / `EditQueue` / Apply pipeline**
(`src/model/commander_ops.rs`, `src/model/edit_queue.rs`). Today a cross-pane copy:

1. extracts the source file to a **host temp blob** (`commander_ops.rs` `stage_copy_into` →
   `src_fs.write_file_to(entry, blob)`),
2. stages `StagedEdit::AddFile { parent, name, host_path: blob, size }` into the destination `EditQueue`,
3. **Apply** opens the destination `EditableFilesystem` once, replays all staged edits via `create_file`,
   `sync_metadata`, commits.

The remote design is exactly this with **the queue and the staging blobs living on the daemon**. The
desktop Commander code barely changes — it gets a `RemoteEditQueue` backend instead of a local one.

### 2.5 Family B reuses the chunked-container / resume machinery already designed
Family B is **not** new protocol invention — it is the chunked `.cbk` container, ddrescue-style transfer
map, source-keyed gzip members, fingerprint gate, and file manifest already specified in
[`cb_dos_network_and_state.md`](cb_dos_network_and_state.md). This doc only adds: (a) it rides the unified
`serve` daemon + handshake, and (b) the producer/consumer symmetry that lets the desktop pull/restore, not
just cb-dos push. See §6 + §7.

### 2.6 Don't fork mrext; borrow its deployment rails
mrext (`github.com/wizzomafizzo/mrext`, **GPL-3.0**, inbound-compatible with our AGPL-3.0) is in
maintenance mode (maintainer focus on Zaparoo). Its `cmd/remote` Go daemon can list the SD card and ZIPs
but **cannot browse inside disk images** — that's our whole product, and re-implementing it in Go is a
non-starter. So:

- **Reuse the install/run convention** (Scripts-menu `.sh`, `user-startup.sh` service line, `downloader`
  DB-JSON update channel) — §12.
- **Optionally** contribute a small, standalone-useful PR to mrext (Range-capable file download) as an
  *accelerator*, never a dependency — Appendix A.

### 2.7 Template: the existing privileged-helper IPC
`src/privileged/protocol.rs` already proves the pattern: a `serde` `DaemonRequest`/`DaemonResponse` enum,
handle-based (`OpenDiskRead → handle`, `ReadSectors { handle, lba, count }`), with `DAEMON_VERSION` /
`MIN_DAEMON_VERSION` for compatibility and a `/tmp/...progress.json` for crash recovery. The network
protocol is this, over TCP instead of a Unix socket, at the operation level (Family F) and the
chunk-stream level (Family B).

---

## 3. Architecture diagram

```
┌──── DESKTOP (rusty-backup GUI / rb-cli) ── Family F + B client ────────────┐
│  Commander pane (browse/copy) ·  Backup/Restore tab (whole-disk over wire)  │
│  trait RemoteBackend ─ RemoteSession (F: stage/apply/browse · B: producer/  │
│                                            consumer of chunk members)        │
└───────────────────┬─────────────────────────────────────┬──────────────────┘
                     │ TCP, length-framed                  │
                     │ binary Hello + capability flags     │
   ┌─────────────────▼──────────────────┐    ┌─────────────▼──────────────────┐
   │  rb-cli serve  (MiSTer / any host) │    │  cb-dos / CRUSTYBK.EXE (DOS box)│
   │  remote::server — accept loop      │    │  Family B client ONLY (no JSON) │
   │  Family F: in-proc engine, session │    │  producer: int13h smart-image → │
   │     table = cache, SD staging      │    │     gzip members → push          │
   │  Family B: producer (engine read)  │    │  consumer: pull restore → card   │
   │     / consumer (.cbk + resume map) │    │  mTCP raw TCP over packet driver │
   │  remote::service — PID/start/stop  │    │  Network screen: DHCP/static/IP/ │
   │  remote TUI (crossterm): status/IP │    │     host:port  (writes MTCP.CFG) │
   │     ↕ /tmp/rb-daemon.sock          │    │  packet driver pre-loaded (TSR)  │
   └────────────────────────────────────┘    └─────────────────────────────────┘
```

The DOS box is a **client** in both of its roles (producer when backing itself up, consumer when restoring
onto its own card). The daemon on the MiSTer/desktop is the **server** for both families; for Family B it
plays producer (desktop pulls a backup of it) or consumer (cb-dos / desktop pushes a backup to it) as the
session dictates.

---

## 4. Modules / layout

```
src/remote/
  mod.rs        # pub use; feature gate
  protocol.rs   # binary Hello + capability flags + framing + version constants
  server.rs     # pub fn serve(cfg) -> blocking TcpListener accept loop; family routing; session table
  session.rs    # Session { dest image handle, EditQueue, staging blobs (F) | .cbk + resume map (B) }
  service.rs    # PID-file lifecycle (start/stop/restart/status), user-startup.sh install
  tui.rs        # crossterm status/control console (Scripts-menu front-end)
  backend.rs    # CLIENT side: trait RemoteBackend + RemoteSession used by GUI/CLI (F + B)
  stream.rs     # Family B producer/consumer: chunk frames, .cbk container, resume map, fingerprint
  config.rs     # rb-daemon.ini parse (bind, root, writable, allow_push_update, staging_dir, token)

[[bin]] rb-daemon   # optional thin shim → remote::serve / remote::service
src/cli/...         # `serve` subcommand; rb:// ref support in resolve.rs (§11)
src/gui/commander/  # remote pane source kind (§11)

crusty-backup/      # the DOS client (separate C/DJGPP tree) — Family B only, mTCP transport (§10)
```

Feature-gate the networking behind a `remote` cargo feature (mirroring how `reqwest` is gated behind
`gui`) so a no-network slim build stays possible; include it in the MiSTer build. The on-disk
`.cbk`/resume/fingerprint code in `stream.rs` is shared engine logic and lands whether or not `remote` is
on (it is also the local-container format) — gate only the socket parts.

---

## 5. Wire protocol

### 5.0 Shared transport + handshake (both families)
- **Transport:** blocking thread-per-connection `std::net::TcpListener` — **no async runtime** (matches
  the repo's `std::thread` + `Arc<Mutex<Status>>` idiom; keeps the slim build lean). On DOS, mTCP's
  blocking socket calls map onto the same model.
- **Framing:** `u32 length + u8 kind`. Family F control frames are `serde_json`; Family B chunk frames and
  the `Hello` handshake are **fixed binary** (little-endian, packed) so the C client needs no JSON. Bulk
  file/chunk data is **raw appended bytes** (never base64-in-JSON).
- **Handshake (binary):**
  ```
  Hello   { magic u32, version u16, platform u8, role u8 }
  HelloAck{ version u16, platform u8, capabilities u16, flags u8 }
            capabilities: bit0 FAMILY_F  bit1 FAMILY_B  bit2 PUSH_UPDATE
            flags:        bit0 writable   bit1 push_update_allowed
  ```
  Reuse `DAEMON_VERSION` / `MIN_DAEMON_VERSION`. Keep changes additive so a newer client talks to an older
  daemon and fails *clearly* on major mismatch. A client that only set `FAMILY_B` (cb-dos) is served
  without ever touching JSON.

### 5.1 Family F verb table (operation-level; serde_json control + raw payloads)
```
Session
  OpenSession                          -> {session_id}
  CloseSession{session}                Abort{session}            # discard staging

Browse (host FS + inside images)
  Probe{path}                          -> [PartitionInfo]        # parse partition table
  OpenImage{path,part}                 -> {fs_handle}
  ListDir{fs_handle|host, path}        -> [FileEntry]
  Stat{...}                            -> FileEntry
  ReadFile{fs_handle, path, off, len}  -> bytes (stream)         # preview / image->desktop
  ListHostDir / HostStat / HostMkdir / HostDelete                # raw SD browse, sandboxed to root

Stage edits into a destination session queue (deferred; diff happens only at Apply)
  StageUpload{session, dest_parent, name} + bytes   # desktop -> remote staging blob; queue AddFile
  StageCopyLocal{session, src_img, src_path, dest_parent, name}  # ON-DEVICE, no round-trip
  StageMkdir{session, parent, name}
  StageDelete{session, parent, entry}

Commit
  Apply{session}                       -> emits Progress*, then Done    # open-once, replay, sync, commit
```

### 5.2 Family B verb table (chunk-stream; binary frames)
Direction-agnostic producer/consumer protocol. See [`cb_dos_network_and_state.md`](cb_dos_network_and_state.md)
§2 (chunk/container), §3 (resume), §4 (fingerprint), §5 (manifest) for the on-the-wire chunk header,
`.cbk` index, transfer map, and fingerprint/manifest payloads — *not duplicated here*.
```
Backup (producer -> consumer)
  BeginBackup { session, source_id, fingerprint, layout }   # client is producer (push: cb-dos / desktop)
  RequestBackup { source_path, params }                     # daemon is producer (pull: desktop captures remote)
  Chunk { logical_id, src_offset, len, crc32 } + bytes      # the unit; source-keyed gzip member
  Manifest { ... }  Fingerprint { ... }                     # sidecars (cb-dos doc §4/§5)
  Commit { session }  /  Abort { session }

Resume (the headline feature — cb-dos doc §3)
  Resume { session, fingerprint } -> per-logical { complete | resume_at K }   # K = source-offset cursor

Restore (producer = backup holder -> consumer = live disk, with resize)
  BeginRestore { session, backup_id, sizing }               # consumer writes the target disk
  ( Chunk frames flow producer -> consumer; consumer resizes as it lands them )

Shared
  Progress { op_id, bytes_done, total, phase }              # server -> client events; speed is client-derived
```

---

## 6. Session / staging / cache model

### 6.1 Family F — session table = the cache
Modeled on the privileged daemon's `handle: u64` table. Each open session holds: the kept-open destination
image handle, the pending `EditQueue`, and its staging blobs. Multiple `Stage*` calls accumulate, then
**one** `Apply`. A read-cache of recently-opened *source* images rides the same table. Idle-timeout +
explicit `CloseSession` + cleanup-on-`Abort`.

- **Where in-flight data lives:** a session-scoped staging dir **on the SD card**, e.g.
  `/media/fat/Scripts/.rb-daemon/staging/<session>/<uuid>.blob`.
  - **SD, not `/tmp`** — `/tmp` is tmpfs (RAM); staging a multi-hundred-MB image there would OOM the
    device. Configurable via `rb-daemon.ini` `staging_dir=`, defaulting to the largest writable mount.
  - **The diff is deferred:** staging only lands bytes; the destination image is opened editable **only at
    Apply**, which replays the queue → `create_file` from each blob → `sync_metadata` → commit, then
    deletes the blobs. This is `apply_staged_edits` from Commander, run on the daemon.
- **Double-write optimizations:** single-file desktop→remote can **stream-through** `create_file` with no
  blob; **remote→remote skips staging entirely** (daemon streams `write_file_to(src) → create_file(dst)` on
  apply; the desktop sends only the *command*). Staging-by-default exists for the multi-file **atomic
  batch**.
- **MiSTer gotcha:** if the staging mount is FAT32, blobs > 4 GiB fail — let `staging_dir` point at an
  exFAT/ext mount, and chunk large blobs.

### 6.2 Family B — the `.cbk` container *is* the session journal
Family B does **not** use the staging-blob model. Its durable state is the chunked `.cbk` container on the
consumer side, which triple-duties as wire framing, **ddrescue-style resume log**, and deliverable
(cb-dos doc §2–§3). The fsync-before-record ordering + truncate-to-last-committed resume rule lives there;
the daemon's Family-B consumer is the host listener that owns that durable state. The session table holds a
Family-B session's `{ .cbk handle, resume map, fingerprint }` instead of `{ EditQueue, blobs }`.

---

## 7. Family B directions in detail (the producer/consumer symmetry)

| Direction | Producer (smart-reads disk → members) | Consumer (journals members / writes disk) | Notes |
|---|---|---|---|
| cb-dos **push** | cb-dos C imaging (`int 13h` + gzip member emit) | `rb-cli serve` consumer → `.cbk` → materialize native folder | The original cb-dos case. cb-dos doc §2c, §9. |
| desktop **pull** | `rb-cli serve` on the remote (engine `compact_partition_reader` + gzip) | desktop `RemoteSession` consumer → local backup folder | Heavy read stays on the host with the disk (§2.2). |
| **restore** (either way) | whoever holds the backup `.cbk`/folder | whoever writes the live disk, **resizing as it lands** | cb-dos doc §9 (7e). Resize uses the existing restore engine. |

The **fingerprint gate** (geometry + MBR/ptable CRC + volume serials + allocation-bitmap CRC) and the
**file manifest** (size + mtime + archive bit; round-tripped for idempotency; boot-section protection) are
direction-agnostic and apply to every Family-B transfer — see cb-dos doc §4–§5. Swap-file exclusion
(§6 there) belongs in the **shared** compaction path per the GUI/CLI-parity rule, so the desktop benefits
identically when it is the producer.

---

## 8. Optional block tier (fallback only)

A minimal `ReadAt`/`WriteAt`/`Len` tier lets the **desktop** parse a remote image locally over a
`RemoteRwSeek` (`Read + Write + Seek` proxy + a client read-ahead cache modeled on
`PartcloneBlockCache`). Used only when the daemon build can't decode a format (e.g. CHD off on a
constrained daemon) — the desktop pulls byte ranges and decodes. Not the primary path; document but don't
lead with it. Irrelevant to cb-dos (Family B only).

---

## 9. Progress & speed

Operations are async with an `op_id`; the daemon emits `Progress { op_id, bytes_done, total, phase }`
frames (both families). The engine already exposes progress callbacks and a `CountingRead` wrapper to hang
these on. **Speed is a client-side derivative** — the wire carries cumulative bytes; the client (and the
TUI) compute KiB/s from the byte delta over wall-time. The daemon is ordinary Rust, so
`std::time::Instant` is fine there; the DOS client computes its own KiB/s the same way for its progress UI.

---

## 10. cb-dos as a Family-B client + its TCP/IP config (layered)

cb-dos implements **only** the binary `Hello` handshake + Family B (producer for push-backup, consumer for
pull-restore), over **mTCP** raw TCP sockets. It is JSON-free and never a server. FTP (today's
`cb_dos_networking.md` transport) stays as a zero-integration fallback for moving a `.cbk`/folder off the
box before native Family B lands.

### 10.1 The layered config model (chosen 2026-06-20)
DOS networking is two layers and they split cleanly across the launch boundary:

| Layer | Where it's configured | Why there |
|---|---|---|
| **Packet driver** (per-NIC TSR on `int 60h`) | **pre-launch** — `NET.BAT` / `AUTOEXEC.BAT` | A TSR can't be reliably loaded from *inside* a DJGPP protected-mode program. This one piece must precede CRUSTYBK. |
| **IP config** (DHCP vs static, mask/gateway/DNS) | **inside CRUSTYBK** — Network screen | It's just `MTCP.CFG` text + a DHCP call; CRUSTYBK can read & rewrite it. |
| **Destination** (`host:port`, optional token) | **inside CRUSTYBK** — Network screen | cb-dos-specific setting; persisted to a small cb-dos settings file. |

### 10.2 CRUSTYBK "Network" screen
A TUI screen (same crossterm-on-DOS conventions as the rest of `CRUSTYBK.EXE`: ASCII-only, RAII restore)
that:
- **Views** current link/IP status — parsed from `MTCP.CFG` and/or queried from the initialized mTCP
  library (IP, mask, gateway, DNS, whether a lease is held).
- **Runs DHCP** on demand (invoke the DHCP path; mTCP writes the lease back into `MTCP.CFG`).
- **Sets a static IP** — edit IP / mask / gateway / DNS fields → write `MTCP.CFG`.
- **Sets the destination** `host:port` (+ optional token) for push/pull/restore.
- **Detects "no packet driver loaded"** and tells the user to run `NET.BAT` first, rather than hanging.

On entering any network action, CRUSTYBK initializes the mTCP library from `MTCPCFG`/`MTCP.CFG`. The
borrow-vs-port-mTCP-to-DJGPP question (cb-dos doc §1b, §8) gates how much of this is library calls vs
shelling out to mTCP's `DHCP.EXE` — resolve it in the Phase-7 spike; the *UX surface above is the same
either way*.

### 10.3 Honest constraints
- The packet-driver TSR genuinely can't move into the app — that boundary is fixed (§10.1).
- mTCP needs ~96–256 KB free RAM; fine on anything that runs DOS networking. CRUSTYBK + the mTCP stack
  co-resident needs a memory check at the Phase-7 spike.
- Static-IP edits write `MTCP.CFG`; on a read-only boot floppy/CD the user must copy `\NET` to writable
  media first (already documented in `cb_dos_networking.md`).

---

## 11. Desktop integration

### GUI
- **Commander Mode (Family F):** add a remote source kind alongside the existing `ListingSource`
  (`src/model/dir_listing.rs`) — either a `Remote` variant or feed a `Box<dyn Filesystem>` produced over
  the remote session into `DirListing::load_root`. "Connect to remote…" dialog: host:port (+ optional
  token), **saved connections**. Cross-pane copy arms in `CommanderMode::copy`'s `(src_host, dest_host)`
  match mostly **just work**: a remote source presents a `Box<dyn Filesystem>`, `stage_copy` already
  extracts via `write_file_to`; the new arms route staging to the remote session, and remote→remote uses
  `StageCopyLocal`.
- **Backup / Restore tabs (Family B):** the Backup tab gains a **remote source** (capture a connected
  daemon's disk/image over the wire → local backup folder); the Restore tab gains a **remote target**
  (push a restore onto a connected daemon's disk). Both surface the §9 progress + speed.

### CLI parity (required by the repo's GUI/CLI parity rule)
- New `rb://host:port/path@N` ref form, routed through `src/cli/resolve.rs` / `src/model/source_reader.rs`
  so existing verbs work transparently:
  - Family F: `rb-cli cp rb://mister:7341/games/dos.img@1 ./local.img@1`, `rb-cli ls rb://mister:7341/`,
    `get`, `put`.
  - Family B: `rb-cli backup rb://mister:7341/<disk-or-image> ./out-folder` (pull),
    `rb-cli restore ./backup-folder rb://mister:7341/<target>` (push restore) — mirror the local
    `backup`/`restore` verbs with an `rb://` endpoint.
- `rb-cli serve [--bind 0.0.0.0:7341] [--root /media/fat] [--writable] [--token …]` runs the daemon
  (offers both families; `--family-f-off` / `--family-b-off` to restrict a constrained deployment).

---

## 12. Packaging, install & maintenance (MiSTer)

### Auto-start at boot
One line appended to `/media/fat/linux/user-startup.sh` (mrext's `config.StartupFile`):
```sh
# rb-daemon
[[ -e /media/fat/Scripts/rb-daemon.sh ]] && /media/fat/Scripts/rb-daemon.sh service start
```
`rb-daemon service install` adds the line idempotently and starts; `service uninstall` removes + stops.
mrext's `Startup.AddService` (`pkg/mister/startup.go:147`) is the reference. **Honest caveat:** the
downloader intentionally won't run arbitrary post-install hooks, so it can't silently register boot. The
ecosystem norm (mrext Remote, BGM, TapTo) is **run-once from the Scripts menu → self-register →
automatic forever after.** We collapse that to a single Scripts-menu press that runs `service install`.

### Service lifecycle
Port mrext's `pkg/service/service.go`: PID file (`/tmp/rb-daemon.pid`), `service {start|stop|restart|
status}`, SIGTERM/SIGINT cleanup, `Running()` liveness via the PID. Log to `/tmp/rb-daemon.log`. Config
from `/media/fat/Scripts/rb-daemon.ini` (hand-rolled INI reader, no new dep).

### Update channel (downloader DB JSON)
Publish a per-app DB (mirrors mrext's `releases/remote/remote.json`):
```json
{
  "db_id": "rbackup/rb-daemon",
  "timestamp": 0,
  "files": {
    "Scripts/rb-daemon.sh": {
      "hash": "<md5>", "size": 0,
      "url": "https://github.com/danifunker/rusty-backup/releases/download/<tag>/rb-daemon.sh",
      "reboot": true,
      "tags": ["rb-daemon"]
    }
  },
  "folders": { "Scripts/": {} }
}
```
- Release asset `rb-daemon.sh` = the armv7 ELF named `.sh` (so it shows in the Scripts menu), **UPX-`-9`**
  compressed like mrext.
- Tier 1 (immediate, no coordination): user adds `db_url` to `downloader.ini`; the stock `update` pulls it.
- Tier 2 (broader reach): get the DB listed in **Update All**'s *Unofficial Scripts* submenu — coordinate
  with theypsilon (`Update_All_MiSTer`); an existing ecosystem relationship makes this realistic once
  tier 1 is proven.
- `"reboot": true` is the standard "adopt the new binary" mechanism (§13.2).
- **CI**: extend the `build-rb-cli-mini-armv7` job to compute md5+size, sign the binary (§13.3), write +
  commit `mister/rb-daemon.json`. mrext automates exactly this.

---

## 13. Security & updates

### 13.1 Write-by-default (chosen) and its mitigations
Write access is **on by default** (user's call). It's a bigger exposure than a manually-launched tool, so:
- The TUI makes every connection and every active write/transfer **visible and killable** (disable drains,
  not kills) — this is itself the primary mitigation.
- The daemon **logs every apply / every completed backup** (peer, image, files) to `/tmp/rb-daemon.log`
  for an audit trail.
- **Refuse to open-editable an image currently mounted by a running core** (writing under a live core
  corrupts it). Warn loudly otherwise. Same guard applies to a Family-B restore onto a live disk.
- No auth on the LAN for v1 (consistent with mrext's trusted-LAN model, and with the cb-dos doc's
  "isolated host↔vintage-box LAN, no crypto on a 486" posture). An optional `token=` in `rb-daemon.ini`
  is a cheap future hardening, carried in `Hello`.

### 13.2 Updating the daemon while it runs
- **File swap is safe by construction.** Linux won't overwrite a running executable in place
  (`ETXTBSY`); the downloader installs via temp + atomic `rename()`. The running daemon keeps its **old
  inode** and old code; the new binary takes effect only on the **next launch**. `"reboot": true` (or a
  daemon self-restart-when-idle) adopts it.
- **The real risk is the restart/reboot *moment* coinciding with an in-flight Apply or backup** → a
  half-written image. Mitigate with **drain-before-restart** (stop accepting sessions, let active
  Applies/transfers finish, then exit) and a **persisted/recoverable queue** — Family F's staging blobs +
  `EditQueue`, and Family B's `.cbk` + resume map, both already persist on SD, so an interrupted batch
  rolls back (F never committed) or resumes (B from the transfer map).
- **Version skew is certain** (daemon updates via Update-All; desktop and cb-dos update on their own
  cadences). The `Hello` handshake + `MIN_DAEMON_VERSION` + additive protocol changes handle it.
- **No double-start:** PID file + `Running()` check; `service restart` stops-before-start.

### 13.3 Push-update from the desktop (opt-in, signed) — later phase
Lets the desktop push a version-matched daemon to the MiSTer and restart it. Great for the dev loop and it
**eliminates version skew** (push makes daemon == client). But it graduates the daemon into a **remote
software-replacement primitive**, so it ships gated:

- **Consent is anchored at the device.** The opt-in is a question in the **MiSTer-side first-run setup
  TUI** (not the desktop installer — a desktop-side toggle would be security theater since the attacker
  controls their own desktop). Persisted to `rb-daemon.ini` `allow_push_update=false` (default off).
  Re-toggleable in the TUI settings. **TTY-guard:** launched non-interactively (no human at the console),
  it falls back to the saved/safe value and never silently enables push.
- **Signed binaries.** CI signs each released `rb-daemon` with an **Ed25519** release key; the daemon
  embeds the matching public key and verifies the signature over pushed bytes before accepting. Only
  genuine signed builds are accepted (`ed25519-compact` is slim-build-friendly; we already pull
  `sha2`/`aes`). With signing the residual risk is "a LAN peer forces a restart / installs a different
  *genuine* version (incl. older)" — not arbitrary code execution. **Without signing it is literally
  arbitrary RCE, so we do not offer unsigned push.**
- **Setup-TUI prompt copy** (signed case, default highlighted on *No*):
  ```
   Remote daemon updates
   ------------------------------------------------------
   Allow the Rusty Backup desktop app to install daemon
   updates to this MiSTer over the network?

   Only genuine, signed Rusty Backup builds are accepted,
   but this still lets any desktop on your network replace
   the daemon program (including with older versions).
   Enable only on a network you trust.

     > No, keep updates manual        (recommended)
       Yes, allow remote updates
  ```
- **Mechanics:** `PushBinary{version,size,sha256,signature}` + bytes → daemon writes to an SD temp,
  verifies signature + hash, keeps `rb-daemon.sh.bak`, atomic-`rename()`s over the path, **drains**, then
  adopts via `SelfUpdate{mode}`:
  - **Prefer daemon self-restart** — `exec` the *path* (not `/proc/self/exe`, which is the old inode) so
    it relaunches into the new binary without a full reboot (doesn't interrupt a running core).
  - **Force reboot** only when explicitly requested and confirmed; refuse/warn if a core is running.
- **Brick-safety:** `user-startup.sh` backgrounds the daemon (`&`), so even a broken pushed binary
  degrades to *"no daemon,"* not a failed boot. Keep `.bak`; ideally promote-on-successful-handshake
  (write `.new`, swap only once the new one comes up clean) for A/B rollback.
- **Capability flag:** `Hello` reports `push_update_allowed`; the desktop's "Update remote daemon" button
  is enabled only when the device allows it, else it points the user to the device-side toggle.
- **Update-All conflict gotcha:** a pushed binary whose hash differs from the downloader DB will be
  *reverted* (re-downloaded) on the next Update-All run — silently downgrading a dev/pushed build. So
  push-update is best for matching the released version on the spot, or for dev (disable the DB then).
- **Future hardening:** trust-on-first-use **pairing code** shown on the TUI when a new desktop first
  tries to push — binds push capability to consoles the operator approved once.

---

## 14. MiSTer-side TUI (the daemon's console)

Runs when you launch `rb-daemon.sh` from the Scripts menu. Built on the existing crossterm MiSTer-TUI
conventions (ASCII-only per the no-Unicode rule, TTY-guard, RAII terminal restore, testable-core —
established by `bless pick`). Shows **both families' connections**:

```
 rb-daemon            [ RUNNING ]   boot: enabled   push-update: disabled
 connect to:  192.168.1.42:7341     (eth0)
 ---------------------------------------------------------------
 connections:
   192.168.1.10   F: apply session#3   game.hdf@1   18.4 MiB / 64 MiB   6.1 MiB/s
   192.168.1.55   F: browsing          dos.img@1    idle
   192.168.1.77   B: pull-backup       hd0          812 MiB / 2 GiB     9.0 MiB/s
 ---------------------------------------------------------------
 [E] disable daemon   [U] remote updates: off   [Q] quit (daemon keeps running)
```

- The boot-launched daemon and the Scripts-menu TUI are **separate processes**, so the TUI is itself a
  thin local client — it connects to the running daemon over `/tmp/rb-daemon.sock` and issues `Status`
  for the live connection/transfer list; `Enable/Disable` toggles service + boot persistence.
- **"Default to maintain existing configuration":** on launch the TUI *reflects* current state and changes
  nothing until a key is pressed.
- The TUI also hosts the **first-run setup**, including the push-update consent question (§13.3).
- IP display: enumerate LAN interfaces (read `/proc/net` or shell `ip -4 addr`; minor).

---

## 15. Phased implementation plan

Each phase ends at a validation gate. **Family F and Family B interleave** — Family B's deep machinery
(chunk container, resume, fingerprint, manifest) is the cb-dos doc's Phase 7 and is gated on the local
removable-media round-trip proving the native format first (cb-dos Phases 2–4). The unified daemon's
*shared transport* is the join point.

- **Phase 0 — read-only spike (de-risk latency/caching, Family F). — DONE (loopback-validated).**
  Landed in `src/remote/` behind the `remote` cargo feature (pure `std::net` + `serde_json`, no new dep):
  `rb-cli serve --bind --root` daemon (thread-per-connection, handle table), the `Hello` / `OpenImage` /
  `ListDir` / `ReadFile` verbs, and a client `RemoteSession` wired into `rb-cli ls` / `get` via
  `rb://host:port/img@N` refs (`RemoteRef`). The daemon runs the *same* local pipeline
  (`resolve_partition_streaming_forced_inside` → `open_filesystem` → `list_directory` / `write_file_to`),
  so remote == local. *Gate met:* over loopback, `ls` matched the local listing and `get` returned a
  20 KB file byte-exact; path-escape / missing-file / file-as-dir all error cleanly. **Simplification vs
  the spec:** the `Hello` is JSON here (Family F only); the **binary** `Hello` for the JSON-free cb-dos
  client is introduced additively in Phase 5a (the magic+version fields are already in place). Not yet
  done: client read-ahead caching and a real two-machine latency measurement (loopback hides RTT).
- **Phase 1 — Family F write path (stage→apply).** Sessions, SD staging, `StageUpload`/`Apply`, the
  relocated `EditQueue`. *Gate:* copy a file into a remote image, pull the SD, confirm a core mounts it.
- **Phase 2 — host-FS browse + remote→remote (Family F).** `ListHostDir`/sandbox-to-root; `StageCopyLocal`
  on-device copy. *Gate:* on-device image→image copy with no desktop data round-trip; `ls rb://host/`.
- **Phase 3 — GUI Commander remote pane (Family F).** Connect dialog, saved connections, remote source
  kind, copy arms, progress UI. *Gate:* drag a file from a remote image into a local image in the GUI.
- **Phase 4 — MiSTer packaging & service.** `service install` + `user-startup.sh`, downloader DB,
  CI armv7 + UPX, TUI status/enable-disable. *Gate:* enable from Scripts menu, survives reboot.
- **Phase 5 — Family B over the unified daemon.** Bring the cb-dos doc's chunk/resume/fingerprint/manifest
  machinery (its Phase 7a–7g) onto `rb-cli serve` as Family B, with the producer/consumer symmetry:
  - **5a** binary `Hello` + Family-B negotiation; cb-dos client opens a socket, handshakes (cb-dos doc 7a;
    resolve mTCP borrow-vs-port + the CRUSTYBK Network screen here, §10).
  - **5b** cb-dos **push**: chunk PUT into a daemon-side `.cbk`, materialize the native folder, desktop
    restores it unchanged (cb-dos doc 7b–7c).
  - **5c** **resume** (fsync-before-record, truncate-to-committed, `Resume` handshake, fingerprint verify)
    (cb-dos doc 7d).
  - **5d** desktop **pull** + **restore over the wire** (daemon as producer; restore as consumer with
    resize) (cb-dos doc 7e). *Gate:* desktop captures a remote disk's backup, and a wire-restore boots.
  - **5e** manifest + idempotency + boot-section + swap exclusion in the **shared** compaction path
    (cb-dos doc 7f–7g).
- **Phase 6 — later/optional.** Signed push-update (§13.3) + Ed25519 release signing; mDNS discovery;
  TLS/token; pairing; mrext PR-A (Appendix A); incremental backup (cb-dos doc 7h); desktop reads `.cbk`
  directly (cb-dos doc 7i); "image in use by a core" guard hardening; optional block tier (§8).

---

## 16. Risks & mitigations

| Risk | Mitigation |
|---|---|
| Round-trip amplification during FS parse / capture | Operation-level Family F + producer-on-the-disk-host for Family B pull (daemon does the read). Validate in Phase 0 / 5d. |
| In-place / Apply write durability over the wire | Family F stage→apply (commit only at end); Family B fsync-before-record + transfer map; drain-before-restart. |
| Writing under a running core corrupts the image | Refuse open-editable / wire-restore on mounted images; warn. |
| Write-by-default exposure on the LAN | Visible/killable TUI, audit log, optional token; explicit "trusted LAN" framing (both families). |
| cb-dos can't parse JSON on a 486 | Binary `Hello` + binary Family-B frames; cb-dos never touches the JSON Family-F path. |
| DOS IP-config UX | Layered model (§10): packet driver pre-launch, IP + host:port inside CRUSTYBK; FTP fallback retained. |
| Push-update = remote code replacement | Device-anchored opt-in (default off), Ed25519 signing, TTY-guard, capability flag; no unsigned push. |
| Daemon/desktop/cb-dos version skew | `Hello` + `MIN_DAEMON_VERSION` + additive protocol. |
| FAT32 4 GiB staging limit | Configurable `staging_dir`; chunk large blobs; Family B's `.cbk` chunking sidesteps it. |
| mTCP toolchain mixing (Watcom vs DJGPP) | Open question; resolve borrow-vs-port in the Phase-5a spike (cb-dos doc §1b, §8). |
| Slim-build weight | Gate networking behind a `remote` feature; std::net + serde_json (Family F) + binary frames (Family B), no async runtime. |

---

## Appendix A — mrext interop (optional accelerator, not a dependency)

mrext is GPL-3.0 (inbound-compatible) but maintenance-mode. **PR-A — generic sandboxed file download**
(small, standalone-useful): a single handler in `cmd/remote/main.go`'s `setupApi` (`:195`) —
`GET /api/files/raw?path=…` that path-cleans via the existing `cleanPath` sandbox (`menu/files.go:17`)
and calls `http.ServeFile`, which **already honors `Range:` requests**. ~30 lines → byte-range reads of
arbitrary sandboxed files against a stock-ish mrext, giving the desktop an `mrext://` *read* backend with
zero extra install for the many users already running Remote. **PR-B — ranged/resumable upload** (the
write half) is bigger and more contentious (mrext's 15 s timeouts at `main.go:160`, and unauth'd
arbitrary-path writes raise the risk profile for *all* mrext users), so it waits — native `rb://` covers
writes meanwhile. Sequencing: ship `rb://` first; open PR-A early as a goodwill probe of maintainer
responsiveness; follow with PR-B only if PR-A merges cleanly. **Never** put image-aware logic in mrext.

The lightest "ride the mrext rails" option needs **no mrext code at all**: distribute `rb-daemon.sh`
through the downloader DB / Update-All Unofficial Scripts (§12), exactly how mrext ships third-party
scripts.

## Appendix B — open decisions (defaults chosen here; override freely)

1. **Default port `7341`** (mrext owns 8182) — configurable in `rb-daemon.ini`.
2. **One daemon, two families** (`rb-cli serve`); `net-serve` folded in — *decided 2026-06-20*.
3. **Family F control = `serde_json`; Family B + `Hello` = binary** (C-friendly) — `postcard` for Family F
   only if profiling demands.
4. **All three backup directions in scope** (cb-dos push · desktop pull · restore either way) — *decided
   2026-06-20*.
5. **cb-dos is a Family-B client only**, never a server; FTP retained as a fallback transport.
6. **DOS net config is layered** (driver pre-launch · IP + host:port inside CRUSTYBK) — *decided
   2026-06-20*.
7. **Push-update + Ed25519 signing land in Phase 6** (after the core paths work), not v1.
8. **CLI parity (`rb://` refs) is in scope** per the repo's GUI/CLI parity rule — Family F (`ls`/`cp`/
   `get`/`put`) and Family B (`backup`/`restore` to an `rb://` endpoint).
9. **Daemon is platform-generic** (any host can serve / be backed up); MiSTer is the packaged flagship.
10. **Optional block tier (§8) is kept as a fallback**, not built in Phases 0–5.
