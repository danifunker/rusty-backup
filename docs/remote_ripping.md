# Remote optical ripping — desktop-driven, device-streamed

**Status:** design / not yet implemented — tick the [Progress tracker](#progress-tracker)
as work lands. **Author context:** follow-on to the MiSTer optical work
(`optical` + `remote` are both compiled into `rb-cli-mini` and the desktop app
today).

Legend: `[ ]` todo · `[~]` in progress · `[x]` done & verified · `[!]` blocked /
needs decision · `[-]` dropped.

## Motivation

The SuperStation One (and other MiSTer-family armv7 devices) can have a CD/DVD
drive attached, and `rb-cli optical rip` already runs on-device. But the
Cyclone V's ~800 MHz dual Cortex-A9 is a poor fit for the CPU-heavy part of
ripping: **CHD compression** (LZMA / zlib / FLAC over ~700 MB of sector data)
takes many minutes and pins the device.

The fix: let the **MiSTer do only what must be local to the drive** — issue
SCSI `READ TOC` / `READ CD` commands, run the read-retry loop, eject — and have
the **desktop do all the encoding**. Only raw sector bytes cross the LAN; no
compression happens on the device.

```
MiSTer (rb-cli serve)                 Desktop (rb app / rb-cli)
──────────────────────                ─────────────────────────
cd-da-reader: READ TOC / READ CD      RemoteCdReader (read_toc, read_data_sectors)
RetryConfig backoff loop (near drive) write local .bin + .cue / .iso
ship raw 2352-byte sectors  ───────►  CHD compress (libchdman)   ← CPU-heavy, stays here
eject                       ◄───────  
```

## Goals / non-goals

**Goals**
- Drive a *remote* optical drive from the desktop GUI and the CLI.
- All encoding (ISO assembly, BIN/CUE assembly, CHD compression) on the desktop.
- A **single unified device picker**: local drives + every connected daemon's
  drives in one list/pulldown, each tagged with where it lives.
- CLI/GUI parity (`rb-cli optical rip --device rb://host:port/dev/sr0`).

**Non-goals (for the first cut)**
- Multi-session / multiple simultaneous rips against one daemon (the drive and
  `cd-da-reader`'s handle are singular — see "One-session constraint").
- Subchannel/C2-pointer faithful "secure" ripping, GD-ROM, multisession.
- Audio-CD metadata lookup (MusicBrainz, etc.).

## The clean seam (why this is small)

`src/optical/rip.rs` touches the physical drive through **exactly three**
`cd-da-reader` calls — `CdReader::open`, `read_toc`, `read_data_sectors` — plus
`eject_disc`. Everything else (`rip_iso`/`rip_bin_cue` write loops,
`generate_cue_sheet`, `convert::to_chd`) is already plain local code running in
the desktop worker thread. So we abstract those calls behind a trait and swap
the implementation; the encode half never moves.

```rust
// new: src/optical/source.rs
pub trait OpticalSource: Send {
    fn read_toc(&self) -> Result<cd_da_reader::Toc>;
    fn read_data_sectors(&self, lba: u32, count: u32,
                         mode: cd_da_reader::SectorReadMode) -> Result<Vec<u8>>;
    fn eject(&self) -> Result<()>;
}

pub struct LocalCdReader { inner: cd_da_reader::CdReader, device_path: String, retry: RetryConfig }
pub struct RemoteCdReader { conn: Arc<Mutex<RemoteConnection>>, handle: u64 }
```

- `LocalCdReader` wraps today's `cd_da_reader::CdReader` (retry applied locally,
  `eject` = the existing `eject_disc` shell-out).
- `RemoteCdReader` proxies each method to the daemon (retry applied
  **daemon-side**, `eject` proxied).

`rip_iso` / `rip_bin_cue` change only in that they take `&dyn OpticalSource`
instead of constructing `CdReader` directly. `run_rip` builds the right impl
from the (new) target. **Note:** `read_data_sectors` drops the `&RetryConfig`
arg from the trait — retry belongs next to the drive, so it is supplied at
`open` time (passed verbatim to a `LocalCdReader`, serialized to the daemon for
a `RemoteCdReader`).

### RipConfig target

```rust
pub enum OpticalTarget {
    Local(String),                                              // "/dev/sr0"
    Remote { conn: Arc<Mutex<RemoteConnection>>, device_path: String },
}
// RipConfig.device_path: PathBuf  ->  RipConfig.device: OpticalTarget
```

`run_rip`:
```rust
let src: Box<dyn OpticalSource> = match &config.device {
    OpticalTarget::Local(p)            => Box::new(LocalCdReader::open(p, &config.retry)?),
    OpticalTarget::Remote{conn, device_path} => Box::new(RemoteCdReader::open(conn.clone(), device_path, &config.retry)?),
};
```

## Daemon protocol additions (the "optical tier")

The wire protocol (`src/remote/protocol.rs`) is JSON control frames
(`[u32 LE len][JSON]` via `write_control`/`read_control`) plus a bulk path
(`Response::FileBegin{size}` followed by a `ChunkWriter` stream of
`[u32 n][n bytes]…[u32 0]`). The block tier's `ReadBlock` already returns bytes
this way — the optical sector read reuses it verbatim.

**New capability bit** (advertised in `Hello.capabilities` only when the daemon
was built with the `optical` feature — which `rb-cli-mini` is):
```rust
pub const CAP_FAMILY_O: u16 = 1 << 2;   // optical drive proxy
```

**New `Request` variants** (`protocol.rs`), dispatched in `server.rs`'s
`handle_conn` match:

| Request | Response | Server action |
|---|---|---|
| `ListOpticalDrives` | `OpticalDrives { drives: Vec<WireOpticalDrive> }` | `cd_da_reader::CdReader::list_drives()` |
| `OpenOptical { path, retry: WireRetryConfig }` | `OpticalOpened { handle }` | `CdReader::open(path)`; store in `OpticalHandle` |
| `ReadToc { handle }` | `Toc { toc: WireToc }` | `reader.read_toc()` |
| `ReadOpticalSectors { handle, lba, count, mode }` | `FileBegin { size }` + chunk stream | `reader.read_data_sectors(lba,count,mode,&retry)` |
| `EjectOptical { handle }` | `Ok` | proxy of `eject_disc` against the device |
| `CloseOptical { handle }` | `Ok` | drop the `CdReader`, free the global handle |

`ReadOpticalSectors` size = `count * mode.sector_size()` (2048 cooked /
2352 raw|audio), capped to the existing `MAX_RANGE_READ` (4 MiB) — the desktop
already requests `SECTORS_PER_CHUNK = 128` sectors (~301 KB raw) per call.

**Server handle store:** add `struct OpticalHandle { reader: cd_da_reader::CdReader, retry: RetryConfig, device_path: String }` alongside the existing `BlockHandle`. Errors map to `Response::Error{message}` (the desktop re-wraps as an `anyhow`/rip error).

### One-session constraint (important)

`cd_da_reader::CdReader` stores the drive fd in a **process-global
`static mut DRIVE_HANDLE`**, so a process can hold **one** open optical drive at
a time. The daemon therefore serializes optical sessions: a second `OpenOptical`
while one is live returns `Error{"optical drive busy"}`. For a one-drive MiSTer
this is the natural limit; document it rather than engineer around it.

### Elevation

`cd_da_reader::open` uses `O_RDWR` (SCSI pass-through needs write access to the
device node). The daemon must run with permission to open `/dev/sr0` — i.e. as
root on the MiSTer, the same elevated `rb-daemon` posture already used for
remote physical-disk backup.

## Wire DTOs

`cd-da-reader`'s public types have all-`pub` fields but **no `serde`/`Clone`**,
so mirror them in `src/remote/protocol.rs` with `From` conversions (no change to
`cd-da-reader` needed — we both encode from and reconstruct the real types):

```rust
#[derive(Serialize, Deserialize)] pub struct WireTrack { pub number:u8, pub start_lba:u32, pub start_msf:(u8,u8,u8), pub is_audio:bool }
#[derive(Serialize, Deserialize)] pub struct WireToc   { pub first_track:u8, pub last_track:u8, pub tracks:Vec<WireTrack>, pub leadout_lba:u32 }
#[derive(Serialize, Deserialize)] pub enum   WireSectorMode { Audio, DataCooked, DataRaw }
#[derive(Serialize, Deserialize)] pub struct WireRetryConfig { /* mirrors RetryConfig's 5 fields */ }
#[derive(Serialize, Deserialize)] pub struct WireOpticalDrive { pub device_path:String, pub display_name:String, pub has_audio_cd:bool }
// From<cd_da_reader::Toc> for WireToc, From<&WireToc> for cd_da_reader::Toc, etc.
```

## Unified device picker (local + remote in one list)

A small testable core in `src/model/optical_devices.rs`:

```rust
pub enum DeviceLocation { Local, Remote { conn: Arc<Mutex<RemoteConnection>>, label: String } } // label = "host:port"
pub struct RipDevice { pub display_name: String, pub device_path: String, pub location: DeviceLocation }

pub fn list_rip_devices(remotes: &[Arc<Mutex<RemoteConnection>>]) -> Vec<RipDevice>;
// local:  cd_da_reader::CdReader::list_drives()  -> RipDevice{ Local }
// remote: for each conn whose Hello advertised CAP_FAMILY_O, send ListOpticalDrives
//         -> RipDevice{ Remote{ conn, label } }  (skip daemons without the optical capability)
```

`RipDevice::into_target()` yields the right `OpticalTarget` (`Local(path)` or
`Remote{conn, device_path}`) for `RipConfig`.

## GUI changes (`src/gui/optical_tab.rs`)

- Replace the local-only drive combo (today `refresh_drives` →
  `opticaldiscs::drives::list_drives`) with the **unified** list from
  `list_rip_devices`. Each entry labeled by location:
  - local: `TSSTcorp CD/DVDW SH-224 (/dev/sr0)`
  - remote: `[mister.local:7341] HL-DT-ST GP65NB60 (/dev/sr0)`
- Drop the `SourceMode::PhysicalDrive` vs separate-remote split — a device is a
  device; the picker carries its location. (`SourceMode::ImageFile` stays for
  the convert/browse-an-image path.)
- Add a small **"Add remote daemon..."** affordance that pops the existing
  host:port connect dialog (reuse `RemoteConnection::connect_shared` and the
  dialog already used by `RemoteBrowsePanel` / the Backup tab's
  `RemoteSourceState`). Connected daemons are remembered for the session (and
  optionally persisted in `config.json`); `Refresh` re-queries local + all
  remotes.
- `start_rip` / `start_rip_to_chd` build `RipConfig.device` =
  `selected_device.into_target()`. **Nothing in the encode path changes** —
  `rip_to_chd_worker` still rips to a local temp `.bin`/`.cue` and runs
  `convert::to_chd` on the desktop.

The connect → list → pick → run worker-thread/`Arc<Mutex<…>>` pattern is copied
straight from `BackupTab::RemoteSourceState`.

## CLI changes (`src/cli/verbs/optical.rs`)

- `rb-cli optical drives [--remote host:port]...` — lists local drives, plus
  each `--remote` daemon's drives (one `ListOpticalDrives` per connection).
  Output stays `<device-path>  <display-name>`, with remote rows prefixed
  `[host:port] `.
- `rb-cli optical rip --device <PATH-or-URL> ...` — `--device` accepts a local
  path (`/dev/sr0`) **or** an `rb://host:port/dev/sr0` URL. The single
  `CdReader::open` seam parses the scheme: `rb://` → `RemoteRef::parse` +
  `connect_shared` → `OpticalTarget::Remote`; else `OpticalTarget::Local`.
- All output formats (`--format iso|bincue`, and CHD via `optical convert` or a
  future `--format chd`) work identically — encoding is always local.

## The CHD constraint (already handled)

CD-CHD creation is **strictly path-based**: `libchdman_rs::cd::create_from_cue` /
`create_from_iso` need a real on-disk `.cue`/`.bin`/`.iso` (MAME's `parse_toc`).
So the desktop materializes a local temp BIN+CUE, then compresses — which is
exactly what `rip_to_chd_worker` already does (rip → temp `.cue`/`.bin` →
`to_chd` → delete temps). The remote reader just feeds that same two-step flow;
no new wrinkle.

## Data volumes / network

- Raw sector = **2352 B** (BIN/CUE, all tracks); cooked ISO = 2048 B.
- Full ~74-min disc ≈ 333,000 sectors ≈ **~780 MB** raw streamed MiSTer→desktop.
- Desktop requests `SECTORS_PER_CHUNK = 128` sectors (~301 KB) per
  `ReadOpticalSectors`; the daemon re-splits into ≤27-sector `READ CD` commands
  internally (stays device-side).
- Tradeoff: you ship ~780 MB raw over the LAN instead of a smaller compressed
  CHD — the right call when the alternative is pinning the armv7 CPU for minutes.
  On wired gigabit (~110 MB/s) the transfer is a few seconds of wall-clock per
  minute of audio; on 100 Mbit it is bounded by the link, not the device CPU.

## Progress tracker

Update the marker as each item lands; keep the one-line note current (date +
commit when done, or the blocker for `[!]`). Phases are roughly sequential but
P1.1 (the local refactor) is independent and can land first on its own.

### Phase 0 — prerequisites
- [x] `optical` + `remote` both compiled into `rb-cli-mini` and the desktop
  build, so the MiSTer can run `rb-cli serve` *and* drive the local optical
  stack. *(done — the MiSTer optical PR.)*

### Phase 1 — streaming ISO / BIN-CUE over the wire (no encode change)
- [x] **P1.1 — `OpticalSource` seam (local-only refactor, no behavior change).**
  *(done 2026-06-27)* New `src/optical/source.rs` with the trait + `LocalCdReader`;
  `rip_iso` / `rip_bin_cue` take `&dyn OpticalSource`; `run_rip` builds the source
  via `open_optical_source`; `eject` moved behind the trait. `RipConfig` keeps
  `device_path` for now — the `OpticalTarget` switch is deferred to P1.7 where the
  remote dispatch needs it (avoids a `remote`-feature-gated enum variant with no
  consumer yet). Verified: optical unit tests green, no behavior change.
- [x] **P1.2 — wire DTOs.** *(done 2026-06-27)* `WireToc` / `WireTrack` /
  `WireSectorMode` / `WireRetryConfig` / `WireOpticalDrive` in `protocol.rs`
  (always under `remote`); `From` conversions to/from the `cd-da-reader` types
  gated behind `optical` (in a `optical_conv` submodule). Serde round-trip +
  conversion unit tests green.
- [x] **P1.3 — protocol surface.** *(done 2026-06-27)* `CAP_FAMILY_O = 1 << 2`;
  the six optical `Request` variants (`ListOpticalDrives` / `OpenOptical` /
  `ReadToc` / `ReadOpticalSectors` / `EjectOptical` / `CloseOptical`) + the
  `OpticalOpened` / `Toc` / `OpticalDrives` responses (sector data reuses
  `FileBegin` + chunk stream).
- [x] **P1.4 — daemon handlers.** *(done 2026-06-27)* `server.rs`
  `optical_server` module: a per-connection `OpticalState` that wraps a
  `LocalCdReader` (reusing the P1.1 read/eject ops) + a **process-global**
  `AtomicBool` busy guard (released on session drop / connection teardown, since
  `cd-da-reader` holds a global handle); dispatch arms for all six verbs
  (`not(optical)` builds reply "built without the optical feature"); `Hello`
  advertises `CAP_FAMILY_O` under `optical`. Builds clean in optical /
  remote-only configs. *Requires the elevated daemon (root) to open `/dev/sr0`
  `O_RDWR`.*
- [x] **P1.5 — client methods.** *(done 2026-06-27)* `RemoteSession::{list_optical_drives,
  open_optical, read_toc, read_optical_sectors, eject_optical, close_optical}`
  in `client.rs` (return the Wire DTOs, so no `optical` gate), delegated through
  `RemoteConnection` in `connection.rs`.
- [x] **P1.6 — `RemoteCdReader`.** *(done 2026-06-27)* `source.rs` (gated
  `remote`) implements `OpticalSource` over an `Arc<Mutex<RemoteConnection>>`;
  retry sent at open, `Drop` calls `close_optical` to free the daemon's slot.
- [x] **P1.7 — wire it up.** *(done 2026-06-27)* `OpticalTarget` (`Local` |
  `Remote{conn, device_path}`, `Remote` arm `#[cfg(feature = "remote")]`) with a
  manual `Debug` (hides the conn) + `resolve()` that parses an
  `rb://host:port/dev/sr0` device arg into a remote connection; `RipConfig.device`
  replaces `device_path`; `open_optical_source` branches Local/Remote. CLI
  `optical rip --device rb://…` works (pulled the CLI URL-parse forward from
  P3.3); GUI/CLI local call sites build `OpticalTarget::Local`. Builds clean in
  optical+remote / optical-only / default(GUI); 14 optical unit tests green.
- [~] **P1.8 — validate.** *(plumbing verified 2026-06-27)* `tests/remote_optical.rs`:
  loopback client↔daemon test green — the `optical`-built daemon handles
  `ListOpticalDrives` (round-trips, doesn't disclaim the feature), and
  `OpenOptical` of a bogus device errors cleanly **and** releases the
  process-global busy guard (a second open fails at open, not with "busy").
  Remaining: the byte-identical rip-a-real-disc validation (desktop ↔ a daemon on
  a Linux box with an actual drive) needs hardware — user's SuperStation / a
  networked box.

### Phase 2 — CHD compression on the desktop
- [ ] **P2.1 — remote → CHD.** Feed `RemoteCdReader` through the existing
  `rip_to_chd_worker` (temp `.bin`/`.cue` materialized locally, `convert::to_chd`
  runs on the desktop). No protocol change.
- [ ] **P2.2 — validate.** Remote-ripped CD-CHD opens in `chdman info` and loads
  in MAME; the MiSTer CPU stays idle during compression (spot-check `top`).

### Phase 3 — unified device picker + CLI parity
- [x] **P3.1 — picker core.** *(done 2026-06-27)* `src/model/optical_devices.rs`:
  `RipDevice` / `DeviceLocation` (`Local` | `Remote{conn,label}`, `Remote` gated
  `remote`); `list_local_rip_devices` + `append_remote_rip_devices` (errors
  swallowed → offline/non-optical daemons add nothing, which also capability-gates
  the picker) + `list_rip_devices`; `picker_label` / `cli_device_arg` /
  `into_target` helpers. Unit test (local label/arg/target) + loopback test
  (`remote_rip_device_enumeration_over_loopback`) green.
- [x] **P3.2 — GUI.** *(done 2026-06-27, compile-validated; runtime UI pending a
  user check)* `optical_tab` now holds a unified `rip_devices: Vec<RipDevice>` +
  `remote_daemons`; the drive combo lists local + remote drives by `picker_label`;
  an "Add remote daemon…" modal connects on a worker thread
  (`ConnectStatus`/`poll_add_remote`, non-freezing) and unlocks the Physical-drive
  mode. Rip dispatches via `RipDevice::to_target()`; `start_rip_to_chd` /
  `rip_to_chd_worker` take an `OpticalTarget` (encode still local). Remote drives
  are rip-only — `get_browsable_path` returns `None` for them (disc-info/browse
  open the device locally).
- [x] **P3.3 — CLI.** *(done 2026-06-27)* `optical drives --remote host:port`
  (repeatable) lists local + each daemon's drives via the picker core, printing a
  feedable `<device-arg>` (`rb://host:port/dev/sr0` for remote rows).
  `optical rip --device rb://…` landed in P1.7.
- [x] **P3.4 — polish.** *(done 2026-06-27)* Capability gating falls out of
  `append_remote_rip_devices` (a daemon without `optical` errors on
  `list_optical_drives` → contributes no drives). Location-aware eject is
  automatic via `OpticalSource::eject` (local shells out locally; remote sends
  `EjectOptical` to the daemon); the GUI eject checkbox gained a hover note.
- [ ] **P3.5 — optional (deferred).** Persist known remotes in `config.json` so
  they reappear across sessions. Not blocking; pick up if desired.

### Done criteria (cross-cutting)
- [ ] README "Inspect/optical" + `docs/full_MiSTer_support_status.md` note remote
  ripping (per the CLAUDE.md doc-sync rule).
- [ ] CLI/GUI parity confirmed (same drives, same formats, both surfaces).
- [ ] `DISK_IMAGE_EXTS` / picker filters unaffected (no new container types) —
  confirm nothing to update.

> When actively working a phase, mirror its open items into the session task
> list (`TaskCreate`) for in-flight tracking; this doc stays the durable record.

## File-by-file touch points

| File | Change |
|---|---|
| `src/optical/source.rs` (new) | `OpticalSource` trait, `LocalCdReader`, `RemoteCdReader` |
| `src/optical/rip.rs` | `rip_iso`/`rip_bin_cue` take `&dyn OpticalSource`; `RipConfig.device: OpticalTarget`; `eject` via the trait |
| `src/remote/protocol.rs` | `CAP_FAMILY_O`; optical `Request`/`Response` variants; `WireToc`/`WireTrack`/`WireSectorMode`/`WireRetryConfig`/`WireOpticalDrive` + `From` impls |
| `src/remote/server.rs` | `OpticalHandle`; dispatch arms for the optical tier; one-session guard; advertise `CAP_FAMILY_O` |
| `src/remote/client.rs` | `RemoteSession`: `list_optical_drives`, `open_optical`, `read_toc`, `read_optical_sectors`, `eject_optical`, `close_optical` |
| `src/model/optical_devices.rs` (new) | `RipDevice`, `DeviceLocation`, `list_rip_devices` |
| `src/gui/optical_tab.rs` | unified picker; "Add remote daemon" dialog; `OpticalTarget` wiring (encode path unchanged) |
| `src/cli/verbs/optical.rs` | `optical drives --remote`; `optical rip --device rb://…` scheme parse |
| `docs/full_MiSTer_support_status.md`, `README.md` | note remote ripping once shipped |

## Open questions / risks

- **One-session serialization** — acceptable for one drive; revisit only if a
  daemon ever fronts multiple drives (would need per-drive handles, which
  `cd-da-reader`'s global handle blocks today).
- **Error fidelity** — `CdReaderError`/`ScsiError` collapse to a string over the
  wire; the desktop loses the structured SCSI sense. Acceptable; log the message.
- **Connection reuse** — the GUI should reuse the connection it listed drives
  with for the rip (don't reconnect). The CLI's `rb://` path connects fresh.
- **Eject semantics** — `EjectOptical` ejects the *remote* tray; make the GUI
  button label/location-aware so it's obvious which machine ejects.
- **Capability gating** — only offer remote drives from daemons whose `Hello`
  advertised `CAP_FAMILY_O`; older/non-optical daemons simply don't appear.

## Reuse (what already exists)

- `RemoteConnection::connect_shared` (`src/remote/connection.rs`) — shared,
  brokered session; the model for `RemoteCdReader`'s transport.
- `RemoteBlockReader` (`src/remote/block_reader.rs`) — the structural template
  (handle lifecycle, `Drop` closes the daemon handle).
- `write_control`/`read_control` + `FileBegin`/`ChunkWriter`/`read_chunks`
  (`src/remote/protocol.rs`) — framing for the TOC (JSON) and sectors (bulk).
- `BackupTab::RemoteSourceState` (`src/gui/backup_tab.rs`) — the connect → list →
  pick → worker-thread/poll GUI pattern.
