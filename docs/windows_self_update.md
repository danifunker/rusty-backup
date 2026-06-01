# Windows Install + Self-Update (self_update / self_replace + Inno Setup)

Status: **IN PROGRESS.** Self-update + installer (Phases 1-7) implemented and
green on CI; Windows-VM runtime testing (Phase 8) + docs (Phase 9) remain. The
on-demand **elevation rework** (Phases A-H below) is implemented (A-G) and
pending its own CI/VM verification (H). Supersedes the earlier Velopack plan
(removed): Velopack required SemVer and a per-arch packaging toolchain; this
approach needs neither.

## Goal

Give Windows the same "installed into an updatable area, self-updates in place"
experience macOS (DMG → `/Applications`) and Linux (AppImage + zsync) already
have — **without SemVer, without a bespoke updater binary, and without a
heavyweight framework.**

Today Windows ships a **portable ZIP** the user extracts anywhere; the in-app
"update" banner only opens the GitHub releases page in a browser
(`src/gui/mod.rs:558`). `src/update.rs` already does the "is a newer release
available?" half via a plain string compare of the GitHub `releases/latest`
tag (update.rs:161) — **no SemVer**. We reuse that and add the download +
in-place replace half.

## Approach

Two off-the-shelf crates do the hard parts; Inno Setup handles first install.

- **`self_replace`** — replaces the **currently running executable**, correctly
  handling the Windows locked-running-exe problem (moves the running image
  aside, drops the new one in). Battle-tested; this is the exact problem that
  motivated the abandoned "separate `rb-updater` app" idea — the ecosystem
  already packaged that trick, so we don't build it.
- **`self_update`** — lists GitHub Releases, picks the asset for this OS/arch,
  downloads, extracts, and replaces the binary via `self_replace`. Version
  comparison is configurable, so we drive it off the existing date-tag check
  instead of SemVer.
- **Inno Setup** — a free, CLI-compilable installer that gives a GUI install
  wizard, user-selectable install dir, Start-Menu shortcut, Add/Remove Programs
  (ARP) entry, and an uninstaller — all for free.

Precedent in the Rust ecosystem: rustup self-updates via the same
`self_replace` mechanism and installs to a user-writable dir; WezTerm (Rust GUI)
ships an Inno Setup installer. `cargo-dist`/`axoupdater` is the heavier
"framework" alternative we are intentionally not taking.

## Design decisions / answers to the open questions

### Install location — user-selectable, default per-user
Inno's "Select Destination Location" page defaults to
`%LocalAppData%\Programs\Rusty Backup` (per-user, no elevation → `self_replace`
can overwrite freely) but lets the user pick another folder. Caveat for
protected dirs (`Program Files`): the GUI now runs **`asInvoker`** (the
`requireAdministrator` manifest was removed — see the elevation rework below),
so an un-elevated `self_replace` **cannot** write to `Program Files`. We default
to and **recommend `%LocalAppData%`**; a self-update into a protected dir will
fail the in-place swap. Follow-up (not yet done): gate self-update on a
writability probe and fall back to "download the installer" when the install
dir isn't user-writable.

### Start Menu + Add/Remove Programs — yes, via Inno
Inno auto-creates the Start-Menu group/shortcut (optional desktop icon), the
uninstaller, and the ARP entry under `HKCU\…\Uninstall\{AppId}` (per-user
install).

### rb-cli on PATH — installer task, default yes
An Inno `[Tasks]` checkbox ("Add rb-cli to PATH", checked by default) adds a
`bin\` subdir to the **per-user PATH** (`HKCU\Environment`) with
`ChangesEnvironment=yes` so the change broadcasts without a reboot. Install
`rb-cli.exe` into `bin\` (not the root) so only the CLI lands on PATH, not the
GUI exe. Uninstaller removes the PATH entry.

### File associations — owned by the app, not the installer
The supported-extension list **grows between installer releases** (the app
self-updates in place), so a static Inno association list goes stale the moment
a new format lands. Therefore the **app** owns registration:

- **Single source of truth.** Centralize the extension list into one module
  (e.g. `src/model/file_types.rs`) that both the `add_filter` file pickers and
  the association registrar consume. Today the list is duplicated and already
  drifted (inspect_tab.rs:351 lists ~30 exts incl. `hfv`/`gho`/`woz`;
  restore_tab.rs:395 only `img`/`raw`/`bin`) — this refactor is a standalone win.
- **App registers per-user, idempotently.** Writes `HKCU\Software\Classes\
  RustyBackup.DiskImage` ProgId (icon + open command) and
  `HKCU\Software\Classes\.<ext>` → ProgId (+ `OpenWithProgids`) for every
  extension in the central list. Re-runs on launch **when the build version
  changed**, so a self-update that adds `.newext` registers it automatically —
  no reinstall.
- **One code path, three callers.** A hidden `--register-file-associations` /
  `--unregister-file-associations` flag drives it. The Inno installer offers an
  opt-in `[Tasks]` checkbox ("Associate disk image files with Rusty Backup") and,
  when checked, runs the app once with that flag; an in-app **Settings toggle**
  calls the same routine; the uninstaller calls the unregister flag.
- **Windows default-handler caveat.** Registering makes RB a *handler* (appears
  in "Open with", eligible as default). Windows 8+ does **not** let an app
  silently seize the default — the user confirms via the Windows prompt /
  Settings. "Associate" therefore means "register as handler," not "force
  default." Document this so expectations are right.

### Cross-platform associations — one list, three plumbings
File associations are **not Windows-only**, but the mechanism differs and only
Windows registers at runtime. The Phase-3 `src/model/file_types.rs` list is the
single source of truth; each platform's metadata is **generated from it**, never
hand-duplicated (mirror the existing `generate_manpage` /
`generate_cli_reference` example binaries):

- **Windows** — runtime `HKCU\Software\Classes` registrar (above). New
  extensions register live on next launch after a self-update; no reinstall.
- **macOS** — declarative `CFBundleDocumentTypes` (+ UTI declarations) in the
  app bundle's `Info.plist`, generated into the CI plist step (release.yml
  ~212-234). Launch Services auto-registers on install; the **app does not
  register at runtime**. New extensions take effect when a new build is
  installed (i.e. on the next DMG update).
- **Linux** — declarative `.desktop` `MimeType=` + a shared-mime-info XML
  (glob → MIME). The `.deb`/`.rpm` packages install these reliably; the
  AppImage embeds a `.desktop` (release.yml AppImage job) but associations only
  apply **if the user integrates the AppImage**. New extensions take effect on
  package/AppImage update.

Implication: on macOS/Linux a newly added extension only associates after the
user updates to a build whose bundle/package declares it — there is no live
re-register like Windows, because those platforms key off static bundle
metadata. Since updates replace the whole bundle/package, this is handled on
update; it just isn't instantaneous. Mac/Linux association generation is
tracked as follow-up work to Phase 3, separate from this doc's Windows focus.

### Keeping ARP accurate across in-app updates
`self_replace` swaps the exe **in place** within the install dir, bypassing the
installer — so Inno's ARP `DisplayVersion` would go **stale**. The uninstaller
still works (the install dir / `UninstallString` path is unchanged). Fix: after
a successful self-update the app **rewrites `HKCU\…\Uninstall\{AppId}\
DisplayVersion`** (and DisplayName) to the new version — HKCU, no admin needed.
This is the standard pattern for "installer creates ARP, app keeps it current."

### Multi-binary update
We ship `rusty-backup.exe` (GUI) + `rb-cli.exe`. `self_replace` targets the
running GUI exe; `rb-cli.exe` is **not running**, so the update flow overwrites
it with a plain file copy from the downloaded archive. The download asset is the
existing Windows ZIP (already built in CI), so no new artifact is required for
updates.

### Portable ZIP coexistence
Keep shipping the portable ZIP. A portable (non-installed) copy should **not**
write ARP/registry entries — detect "installed vs portable" (e.g. presence of an
install marker or running from `%LocalAppData%\RustyBackup`) and skip
registry-touching steps when portable. Portable copies can still self-update in
place (the dir is user-writable).

## What we deliberately are NOT doing
- No SemVer (reuse the date-tag string compare).
- No bespoke updater process (`self_replace` is the updater).
- No delta updates (irrelevant at this app's size — full ZIP re-download is
  fine).
- No per-machine MSI / Program Files default (fights self-update + needs admin).

## Phased plan & effort estimates

Effort in developer-days. Core in-app update (P1–P2) is ~2–2.5 d; the full
polished install experience is **~5–6 d**.

### Phase 1 — In-app self-update core — ~1–1.5 days
- Add `self_update` + `self_replace` to `Cargo.toml` (GUI binary; `#[cfg(windows)]`).
- Reuse `update::check_for_updates` to decide "newer?" (date-tag compare).
- On confirm: download the Windows ZIP asset for this arch, `self_replace` the
  GUI exe, plain-copy `rb-cli.exe`, then relaunch + exit.
- Non-Windows keeps the existing browser-banner path untouched.

### Phase 2 — GUI update UX — ~1 day
- Replace the Windows "View Release" button (`gui/mod.rs:558`) with
  **"Download & Install Update"** → check → download (progress) → apply →
  prompt restart. Run on a worker thread (existing `Arc<Mutex<…>>` status
  pattern); never block the UI thread.
- Show download progress in the banner.

### Phase 3 — Central extension registry + file-association registrar — ~1 day
- Add `src/model/file_types.rs`: the canonical supported-extension list +
  ProgId definition. Repoint the scattered `add_filter` calls (inspect_tab,
  restore_tab, backup_tab, optical_tab, expand_hfs_dialog) at it to kill the
  drift.
- Add `--register-file-associations` / `--unregister-file-associations` flags
  that write/remove the `HKCU\Software\Classes` ProgId + per-`.ext` keys from
  that list. Run registration automatically on launch when the build version
  changed (store a "registered-for-version" marker). Add an in-app Settings
  toggle that calls the same routine. Windows-only.

### Phase 4 — Inno Setup installer — ~1–1.5 days
- Author `installer/rusty-backup.iss`: per-user mode (`PrivilegesRequired=
  lowest`), default `%LocalAppData%\RustyBackup`, user-selectable dir,
  Start-Menu shortcut (+ optional desktop icon), auto uninstaller + ARP entry,
  stable `{AppId}` GUID. Bundles `rusty-backup.exe` (root) + `rb-cli.exe`
  (`bin\` subdir) + icon.
- `[Tasks]`: "Add rb-cli to PATH" (default checked → add `bin\` to HKCU PATH,
  `ChangesEnvironment=yes`); "Associate disk image files with Rusty Backup"
  (opt-in → run app once with `--register-file-associations`).
- Uninstaller removes the PATH entry and runs `--unregister-file-associations`.

### Phase 5 — Keep ARP DisplayVersion current — ~0.5 day
- After a successful self-update, write the new version to
  `HKCU\Software\Microsoft\Windows\CurrentVersion\Uninstall\{AppId}\
  DisplayVersion` (+ DisplayName). Skip when running portable.

### Phase 6 — Location / elevation / portable reconciliation — ~0.5 day
- Confirm self-update works from `%LocalAppData%` (unprivileged) and from a
  protected dir (via the app's existing elevation).
- Detect portable vs installed; skip registry writes (associations, ARP) when
  portable.
- Optional: writability probe + "download installer" fallback for the
  no-admin/protected-dir case.

### Phase 7 — CI — ~0.5 day
- In `build-windows`: bundle `rb-cli.exe` (in `bin\`) alongside the GUI in the
  ZIP (if not already), install Inno (`iscc`), compile `rusty-backup.iss` per
  arch, upload `Setup.exe`. Extend the `release` job's artifact filter
  (`release.yml:686`) to include `*.exe` installers. Keep the ZIP.

### Phase 8 — Testing — ~1 day
- Clean Windows VM: run `Setup.exe`, verify install dir choice, "Add rb-cli to
  PATH" (open a new shell → `rb-cli --version`), associations checkbox,
  Start-Menu shortcut, ARP entry, launch.
- Publish a higher version; verify in-app "Download & Install Update" replaces
  the exe, relaunches, refreshes ARP `DisplayVersion`, and re-registers
  associations for any newly added extension.
- Verify uninstaller removes the app, PATH entry, and associations after
  several self-updates.
- Verify a portable ZIP copy self-updates but writes no registry entries.

### Phase 9 — Docs & migration — ~0.5 day
- README/release notes: "Windows: `Setup.exe` (installs + auto-updates) or the
  portable ZIP." Note existing ZIP users run Setup once to get Start-Menu/ARP.
- Update `docs/WINDOWS-WRITE.md` / config docs if update settings change.

## Resolved decisions (2026-06-01)
- **Scope of first pass:** Phases 1-6 (in-app update + file_types + Inno
  installer + ARP + portable/elevation). CI (7), testing (8), docs (9) deferred.
- **Branch:** `new-file-formats` (commit directly, per-slice).
- **Installer association default:** **default-on (checked)**. "Add rb-cli to
  PATH" also default-checked.
- **Update apply mode:** **prompt** — "Download & Install Update" button →
  download w/ progress → prompt restart. Never silent.

## Open questions (deferred)
- Single ZIP asset for both arch updates, or per-arch? (`self_update` selects by
  asset name — keep the existing per-arch ZIP naming.)
- Oldest supported Windows for the i686 build — confirm `self_replace` /
  `self_update` + TLS (reqwest, already a dep) work there.

## Progress tracker
- [x] Phase 1 — in-app self-update core (`update.rs`: download/extract +
      self_replace swap; `model::update_runner`)
- [x] Phase 2 — GUI update UX (`gui::mod::show_update_actions`)
- [x] Phase 3 — central extension registry + association registrar
      (`model::file_types`, `os::file_assoc`, config fields, main.rs hooks,
      Settings toggle)
- [x] Phase 4 — Inno Setup installer (`installer/rusty-backup.iss`)
- [x] Phase 5 — ARP DisplayVersion refresh (`os::win_install`)
- [x] Phase 6 — portable detection (`os::win_install::is_installed`); elevation
      is inherent (GUI runs elevated); writability-probe fallback not needed
- [x] Phase 7 — CI: `build-windows` compiles `installer/rusty-backup.iss` via
      Inno Setup (choco) per arch and uploads `Setup.exe`; the release job's
      `*.exe` filter attaches it. Pending first green CI run to verify the
      Windows-only code compiles.
- [ ] Phase 8 — testing on Windows VM (deferred — requires Windows)
- [ ] Phase 9 — docs & migration (deferred)

## Implementation notes (for the deferred Phases 7-9)
- Host is Homebrew Rust (no rustup / no Windows std), so all `#[cfg(windows)]`
  paths (`self_replace`, `os::file_assoc`, `os::win_install`, the Windows
  branches of `gui::mod`/`settings_dialog`/`main`) were written but **not
  compiled here** — they need CI / a Windows VM to verify. Cross-platform parts
  (download/extract, `model::file_types` + tests, picker repoints, config) do
  compile and pass on host.
- New deps (Windows-only): `self-replace`, `winreg`.
- Phase 7 CI must: add `rb-cli.exe` into the Windows ZIP under `bin\` so the
  self-updater's sidecar refresh + the installer's PATH layout line up; install
  Inno (`iscc`) and compile `installer/rusty-backup.iss` per arch passing
  `/DMyAppVersion=<version>`; attach `Setup.exe` to the release (extend the
  `release` job artifact filter).
- The `AppId` GUID is duplicated in `installer/rusty-backup.iss` and
  `os::win_install::UNINSTALL_SUBKEY` — keep them in sync.

---

## Elevation rework (on-demand admin) — fixes installer error 740

The GUI shipped a `requireAdministrator` manifest (always-elevated). That broke
the per-user installer: an un-elevated `Setup.exe` cannot `CreateProcess` an
always-elevated exe at post-install, giving **"CreateProcess failed; code 740,
the requested operation requires elevation."** It also forced a UAC prompt on
every launch. This rework moves the GUI to `asInvoker` with a single, explicit,
global elevation gate. Continues the trajectory of commits `e50f4fb` (gate
admin to physical-disk ops, manifest → GUI binary only) and `e3b0173`.

### Model
- GUI launches **`asInvoker`** (no manifest). File-only flows (inspect/backup/
  restore against image files, self-update, association registration) never
  elevate, never prompt.
- **One global gate**: a top-bar **"Show Physical Devices"** button (stylized
  shield) is the *only* elevation trigger. Click → `os::windows::
  request_elevation()` relaunches the whole process via UAC. The elevated
  instance auto-enumerates devices on startup (the existing startup
  `enumerate_devices()` returns devices when elevated), so all tabs' device
  lists populate at once. Once elevated the button becomes "Refresh Devices".
- Elevation is **whole-process** (Windows can't elevate one call); a global
  top-bar gate honestly reflects that. Low-level read/write paths never
  relaunch (that would `process::exit` mid-operation) — they only `bail!`
  clearly if somehow not elevated.

### Phases
- [x] **A** — drop `requireAdministrator` manifest (`build.rs`); remove orphaned
      `.manifest`/`.rc` + `embed-resource` build-dep. GUI now `asInvoker`.
- [x] **B** — `os/windows.rs`: `enumerate_devices()` returns empty (no relaunch)
      when not elevated; read/write paths drop auto-relaunch, keep clear
      `bail!`; `check_status()` → `NeedsElevation` instead of erroring.
- [x] **C** — global state: startup `enumerate_devices()` already auto-populates
      when elevated, so post-relaunch is seamless (no new state needed).
- [x] **D** — top-bar `ElevationGate` button (`gui/mod.rs`): shielded "Show
      Physical Devices" → `request_elevation()`; else "Refresh Devices".
- [x] **E** — `assets/icons/shield.png`: tintable B/W quartered-shield
      silhouette; lazy `shield_texture` loader (egui API verified on host).
- [x] **F** — `no_devices_hint()` + in-dropdown hints in backup/restore tabs
      when the device list is empty.
- [x] **G** — `RB_NO_AUTO_ELEVATE` removed: it only gated the auto-relaunch that
      no longer exists, so it's obsolete (no escape hatch needed when the GUI
      starts un-elevated by default).
- [ ] **H** — CI compile (Windows-only code) + Windows-VM runtime test: Setup.exe
      completes without 740; button prompts UAC once; devices appear across
      tabs; device backup/restore works; self-update relaunch keeps elevation.

### Notes / caveats
- Optical tab: physical-disc ripping opens the drive with `GENERIC_READ |
  GENERIC_WRITE` for `IOCTL_SCSI_PASS_THROUGH_DIRECT` (via `cd-da-reader`), which
  needs admin on Windows — it does NOT go through `open_source_for_reading`. The
  "Physical drive" source radio is now grayed out (kept visible, with a
  disabled-hover hint) until the user elevates via the top-bar button;
  image-file convert stays available unelevated. Gating helper:
  `gui::physical_devices_available()`.
- Removing the manifest changes **release** behavior: a non-elevated GUI that
  reaches a write path fails fast with a clear message instead of being admin.
  Intended.
- Relaunch discards GUI state (tab/selection). The button lives in the top bar
  and is the first thing a device user clicks, so the loss is trivial — but it
  is a real relaunch, not in-place.
