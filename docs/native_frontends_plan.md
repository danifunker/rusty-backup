# Native front-ends plan

Status: **draft, 2026-10-29.** Nothing here has been built yet.

This plan moves rusty-backup from one egui GUI to a native front-end on every platform. Every feature is written once in shared Rust, and each front-end only renders it.

Companion documents:
- [long_operations_inventory.md](long_operations_inventory.md): every slow operation and its wait-state bug (the Phase 1 work list).
- CONTRIBUTING.md § "Presentation layers": the existing layering rules this plan builds on.

## 1. Decisions so far

| # | Decision |
|---|---|
| D1 | egui stays during the transition. The goal is to retire it once each native shell reaches parity. |
| D2 | Linux: **GTK4 + libadwaita (≥ 1.5)** for modern, **GTK3** for vintage where GTK3 is available, and the **TUI** everywhere else (§9). Decided 2026-10-29. |
| D3 | PPC Tiger/Leopard and Solaris 9 should eventually get a GUI. Deferred (§8). |
| D4 | **Two shells per OS.** A *modern* shell may use current crates. A *vintage* shell covers anything built with Rust 1.73 or older (or mrustc), and keeps its dependencies frozen. Both shells sit on the same view-models. |
| D5 | Future sessions survey every build target for each new feature and wire it in by default (§7). |
| D6 | Tests run in three tiers: view-model tests and per-shell smoke tests on every CI run, plus full UI automation monthly, quarterly, or around major UI changes (§6). |
| D7 | macOS is the first native shell. Linux is discussed in detail first (§9). |

## 2. Where the code stands

**Survey of 2026-10-29:**

| Area | Finding |
|---|---|
| `src/model/` (about 20k LOC) | Has no egui dependency, and its stated goal is that each tab renders a model object. Progress is shared through `Arc<Mutex<Status>>` and is poll-only. |
| `src/gui/` (about 35.9k LOC) | About 60% is view. About 11.5k LOC is business or orchestration logic, and about 3k is poll glue that changes state inside `show()`. |
| Logic in the GUI | Concentrated in inspect_tab, browse_view, commander/pane, backup_tab and restore_tab. |
| Engine calls | The GUI calls the engine (fs, backup, partition, and so on) about 290 times, against about 160 calls into `model`. |
| Job orchestration | Each front-end builds `BackupConfig` and `RestoreConfig` and starts the threads itself. The TUI's backup is missing split, CHD options, size policy, defrag and remote. |
| Commander | The operations are shared (about 7.2k LOC in model). The pane state machine is not: the GUI's `CommanderPane` and the TUI's `CmdPane` diverged, and the TUI lacks unlock, preflight, async apply, partclone and wrappers. |
| Wait states | About 45 slow operations run on the UI thread, and most worker threads have no panic guard. See the inventory. |
| C ABI | None, so no C or ObjC shell can link the engine. |
| UI tests | Two egui no-panic tests and about 37 TUI state tests. No accessibility-driven tests. AccessKit is compiled into egui but unused. |

## 3. Target matrix: what "native" means per platform

| OS family | World | Builds today | Native shell | Shell language | Status |
|---|---|---|---|---|---|
| macOS 10.13+ (arm64, x64) | modern | egui .app + rb-cli | **AppKit** via `objc2` (already a dependency) | Rust | proposed (first) |
| macOS 10.7 (x64) | vintage | rb-cli (Rust 1.73) | AppKit through 1.73-compatible bindings (old `objc`/`cocoa` crates) or an ObjC shell | Rust or ObjC | to verify |
| Mac OS X 10.4/10.5 PPC | vintage | rb-cli (mrustc) | Carbon or Cocoa-10.4 in C/ObjC over a C ABI | C / ObjC | deferred (D3) |
| Windows 10/11 (x64, i686, ARM64) | modern | egui + rb-cli | **Win32 + Common Controls v6** via `windows` | Rust | proposed |
| Windows 7/8 (x64, i686) | vintage | rb-cli (Rust 1.73, `windows-legacy` 0.58) | Win32 via `windows` 0.58 | Rust | proposed |
| Linux 24.04-era+ (x64, arm64, riscv64) | modern | egui AppImage, deb, rpm, Arch | **GTK4 + libadwaita ≥ 1.5** via `gtk4-rs` + `libadwaita` (MSRV 1.92), using the system libraries | Rust | decided (D2) |
| Linux, any age with X11/Wayland | vintage | egui AppImage | **GTK3** via `gtk` 0.18 (gtk3-rs, archived); Anylinux AppImage bundles GTK3 | Rust | decided (D2); 1.73 build to verify (§9) |
| Linux without GTK3 (no display server, or an arch or libc with no GTK3 build: ppc, ppc64, i586/i486, static musl, the appliance) | n/a | rb-cli + TUI or menu | **TUI** | Rust | decided (D2) |
| Solaris 9 SPARC | vintage | rb-cli (mrustc) | Motif/CDE in C over a C ABI | C | deferred (D3) |
| MiSTer (armv7) | n/a | rb-cli + ratatui TUI | **TUI** (framebuffer console, no GL) | Rust | the TUI is the shell |
| Buildroot appliance, i486 | n/a | rb-cli + crossterm menu | TUI or menu | Rust | keep |
| CLI-only musl / extra-arch | n/a | rb-cli + TUI | none | - | keep |
| DOS (`crusty-backup`) | n/a | separate C program | conio | C | out of scope |

The TUI counts as a front-end throughout this plan. It uses the same view-models and gets the same parity treatment.

## 4. Target architecture

```
engine        src/{fs,backup,restore,partition,rbformats,os,optical,remote,...}
   ^
model         src/model/   runners, sessions, edit queue, status structs  (exists)
   ^
jobs          src/model/job.rs        one "wait" abstraction           (new, Phase 0)
   ^
view-models   src/vm/                 one per screen/dialog + AppVm     (new, Phase 2)
   ^
shells        egui (src/gui)  TUI (src/cli/verbs/tui*)  appkit/  gtk4/  win32/  vintage/*
```

**Rules:**
- **The shell boundary is an import rule.** Shells may import only `rusty_backup::vm`, plus plain data types re-exported from it. A CI script fails the build if a shell crate references `rusty_backup::{fs,backup,restore,partition,rbformats,os,optical,remote,model}` directly. This is how "view-only" gets enforced.
- **View-models live in `src/` and obey the engine floor.** They must compile on Rust 1.73 (through the `compat` / `rust173_compat` shims) and under mrustc, because the vintage shells use them. Nothing toolkit-specific goes in `src/vm`.
- **Each shell is its own package.** For example `frontends/appkit/`, `frontends/gtk4/`, `frontends/win32/` and `frontends-vintage/{appkit-10.7,win32-win7}/`. Each has its own lockfile, following the `rb-cli-vintage/` pattern (`[lib] path = "../../src/lib.rs"` or a path dependency). Modern shells can then use current crates without moving the engine's floor.

### 4.1 The job ("wait") abstraction

Every slow operation goes through one type. A shell can only show a wait state through it, so wait states are correct by construction.

```rust
// src/model/job.rs (sketch, 1.73-safe)
pub enum Progress { Indeterminate, Determinate { done: u64, total: u64, unit: Unit } }
pub enum JobState<R> { Running { phase: String, progress: Progress, can_cancel: bool },
                       Done(R), Failed(String), Cancelled }
pub trait Notify: Send + Sync { fn wake(&self); }          // shell-supplied, coalesced
pub struct Job<R> { /* Arc<Mutex<..>>, cancel flag, generation counter, resource keys */ }
impl<R> Job<R> {
    pub fn spawn(keys: Vec<Resource>, notify: Arc<dyn Notify>,
                 body: impl FnOnce(&JobCtx) -> anyhow::Result<R> + Send + 'static) -> Self;
    pub fn snapshot(&self) -> JobState<&R>;   // cheap; UI thread only ever calls this
    pub fn cancel(&self);
}
// JobCtx: set_phase, set_progress (throttled ~10 Hz), is_cancelled, log line
```

- **Panic-safe by construction.** `spawn` uses `run_guarded`. No raw `thread::spawn` remains in shells or view-models (enforced by a grep check).
- **Push and poll.** `Notify::wake` is the push half. Each shell implements it once:
  - egui: `ctx.request_repaint()`
  - AppKit: `dispatch_async` to the main queue
  - GTK: `glib::MainContext::invoke`
  - Win32: `PostMessage(WM_APP_JOB)`
  - TUI: a wake channel into its event loop

  Shells redraw from `snapshot()`, so no shell needs a per-frame timer. This replaces the repaint-every-frame loop (`gui/mod.rs:898`) and the global egui context in `ui_logger.rs:29`.
- **Resource keys and a `JobRegistry`.** Each job declares what it touches (a device path, an image path, a backup folder). The registry replaces `busy_elsewhere`, covers every job (today Inspect and browse writers are missed), and lets a view-model say *why* a control is disabled.
- **Elevation and dialogs never block the UI thread.** An elevation prompt is a job step. A file dialog, a confirmation or a password request is an *effect* (§4.2) that the shell fulfills before the job continues. Privileged handles travel inside the job, so a device is never reopened by path.
- **Log lines** flow through the job context into an `AppVm` log store, replacing the GUI-owned `LogPanel`.
- **The TUI** gets a real "working" frame. A wake from a job triggers a redraw, which ends the "frozen until next key" behavior.

### 4.2 View-models

There is one view-model per screen or dialog:
- **Screens:** `InspectVm`, `BackupVm`, `RestoreVm`, `BrowseVm`, `CommanderPaneVm` + `CommanderVm`, `OpticalVm`, `ArchivesVm`, `BulkVm`, `SettingsVm`.
- **Dialogs:** `ResizeVm`, `HfsExpandVm`, `PartitionEditorVm`, `MetadataEditorVm`, and so on.
- **`AppVm`** holds what is currently GUI-owned: devices, recent files, settings persistence, the log, the job registry, and cross-tab handoffs (open in Inspect, nested open, redirect to Optical).

Each view-model has the same shape:

```rust
pub trait ViewModel {
    type Action;                       // user intents: OpenSource(path), StartBackup, Cancel, ...
    fn apply(&mut self, a: Self::Action) -> Vec<Effect>;
    fn tick(&mut self) -> Vec<Effect>; // drains finished jobs, advances state machines
    // plus plain read accessors the shell renders from
}
pub enum Effect { PickFile(PickSpec), PickFolder, Confirm(ConfirmSpec), AskPassword,
                  Notify(String), OpenUrl(String), CopyToClipboard(String), ShowDialog(DialogId), ... }
```

- **`Action` and `Effect` are exhaustive enums.** Shells must `match` them without a `_` arm. Adding a feature then **fails to compile in every shell that has not handled it**. This compile-time check is what makes fan-out (D5) enforceable rather than a memory.
- **Widget IDs live in the view-model.** Each actionable element has a stable string ID (`"backup.start"`, `"commander.left.list"`). Every shell sets that ID as its accessibility identifier, so one test scenario can drive every shell (§6).
- **Key bindings are data.** Commander's Midnight Commander keys (F5 copy, F6 move, Tab to switch panes) become a table of `Action` × default binding per platform. macOS can then use Cmd-based bindings without forking logic.

## 5. Phases

Each phase lands as small per-slice commits and pull requests. Exit criteria are measurable.

### Phase 0: Groundwork (toolkit-free)

1. Add `src/model/job.rs`, the `Notify` trait and `JobRegistry`. Re-express `worker.rs` on top of them.
2. Add `src/vm/` with the `ViewModel` / `Action` / `Effect` skeleton and `AppVm`, including the log, recent files and settings moved out of `gui/`.
3. Write `docs/platform_matrix.md` (a machine-checkable version of §3) and the fan-out rule in CLAUDE.md (§7).
4. Add the boundary checks (§4) as a script run in CI, starting in warn-only mode.
5. Add the headless view-model test harness: a scenario runner that replays `Action`s and asserts on state (§6, Tier A).

**Exit:** the egui GUI still works unchanged, the new modules compile on Rust 1.73 (vintage CI job), and the harness runs in `cargo test`.

### Phase 1: The wait audit, fixed in egui and the TUI first

Work through [long_operations_inventory.md](long_operations_inventory.md), worst first. Every operation moves onto `Job`. This phase pays off right away for today's users, and shakes out the job API before any native shell depends on it.

- All about 45 synchronous slow operations move off the UI thread:
  - the backup source scan and preview, apply staged edits, CHD edit entry
  - `prepare_disk_image_path`, fsck and repair, the render-time archive sniff
  - remote I/O, partition apply, Make Bootable, TUI busy-waits
- Every spawn gets a panic guard. Every job registers its resources.
- Cancel is added where the inventory marks "no cancel" and the operation allows it. Determinate progress is added where a total is knowable.

**Exit:**
- No `thread::spawn` in `src/gui` or `src/cli/verbs/tui*`.
- The inventory's SYNC column is empty, apart from a list of agreed exceptions such as reading a 256-byte floppy header.
- A test exists per job proving that a panic ends in `Failed`.

### Phase 2: Extract the view-models

The pilot is **Commander**. Lift `CommanderPane` (`gui/commander/pane.rs:164`) and the orchestration in `commander/mod.rs` into `CommanderPaneVm` / `CommanderVm`. Rebuild both the egui and TUI Commander on top of them. **The proof is TUI parity:** unlock, preflight, async apply, partclone and wrappers arrive in the TUI with little or no new logic.

Then go screen by screen, ordered by logic weight:
1. Inspect (about 2.5k logic LOC)
2. Browse (about 2.2k)
3. Backup (about 1.3k)
4. Restore (about 0.9k; this also unifies `BackupConfig` / `RestoreConfig` building into one builder that the CLI uses too)
5. Optical
6. Archives
7. Bulk
8. Dialogs
9. Move `src/optical/browse_view.rs` and `src/theme.rs` into the egui shell.

**Exit:**
- The boundary check is switched to enforcing for egui and the TUI.
- `src/gui` is pure view.
- The TUI reaches feature parity with the GUI, or every gap is documented in the platform matrix as N/A with a reason.

### Phase 3: macOS modern (AppKit)

- Shell package `frontends/appkit/`, built on `objc2` and `objc2-app-kit`, with a minimum of macOS 10.13 (same as today).
- Build order:
  1. App skeleton and `Notify`
  2. Inspect and the partition bar (custom `NSView`)
  3. Commander (`NSOutlineView`, Cmd key bindings, file-promise drag-out)
  4. Backup and Restore
  5. Browse and the editors
  6. Optical, Archives, Bulk, dialogs
- Ships as a second .app beside the egui one (for example "Rusty Backup (Native)") until parity. Packaging reuses the DMG and notarization job.
- Privilege flow: keep authopen and the descriptor cache (see `macos_authopen_descriptor_cache`). Jobs carry the descriptor.

**Exit:** Tier B smoke passes in CI, and Tier C full automation passes once.

### Phase 4: Linux modern (GTK4)

- Shell package `frontends/gtk4/`.
- Widgets: `ColumnView` / `ListView` for listings, `TreeListModel` for trees, a custom `DrawingArea` for the partition bar, `FileDialog` (portal-aware) for pickers.
- Packaging and the pre-22.04 question: see §9.

### Phase 5: Windows modern (Win32)

- Shell package `frontends/win32/`.
- Uses Common Controls v6 through `windows`: `ListView` in virtual mode, `TreeView`, a custom-drawn partition bar, `IFileDialog`.
- Per-monitor DPI v2 manifest.
- UAC flow unchanged.

### Phase 6: Vintage shells (D4)

- **Win7/8:** `frontends-vintage/win32-win7/` on Rust 1.73 with `windows` 0.58.
  - Start by forking the modern Win32 shell, since Win32 has changed little.
  - Keep them two packages per D4, even if they stay near-identical at first.
- **macOS 10.7:** `frontends-vintage/appkit-10.7/`.
  - First verify which ObjC bindings build on 1.73.
  - If none do, write a thin ObjC shell over the C ABI from §8.
- **PPC and Solaris 9:** deferred (§8).

### Phase 7: Retire egui, per platform

When a platform's native shell passes Tier B and Tier C at parity, drop egui from that platform's release job. After the last platform, remove the `gui` feature and `src/gui/`.

## 6. Testing (D6)

| Tier | What it covers | How it runs | When |
|---|---|---|---|
| **A: view-model** | Every `Action`, state transition, job lifecycle (progress, cancel, failure, panic) and effect sequence | Headless `cargo test`. Scenarios are TOML files under `tests/ui_scenarios/`, replayed against the view-models with fixture images from `tests/fixtures/`. | Every CI test job. Add a `cargo test --lib vm` step to the vintage 1.73 jobs, which are build-only today. |
| **B: shell smoke** | Each shell launches, shows main screens, opens a fixture image, browses one directory, runs and cancels one job, and shows the in-progress state | The *same* scenario format, driven through the OS accessibility API by stable widget ID:<br>- macOS: AX / `AXUIElement`<br>- Linux: AT-SPI under Xvfb + D-Bus<br>- Windows: UI Automation<br>- TUI: ratatui `TestBackend` (none exist today)<br>- egui: the same AX/AT-SPI/UIA drivers via AccessKit, which is already compiled in | Every CI run, on each platform's runner |
| **C: full UI automation** | Every screen and dialog in every scenario, plus screenshot capture for human review (not pixel-diff gated) | Same drivers as Tier B, full scenario set | `workflow_dispatch` plus a monthly/quarterly `schedule`, and before merging major UI changes. Vintage shells: scripted where a VM or host exists (Win7 VM, 10.7 VM, the G5 over ssh); otherwise a written checklist. |

- **Sequencing.** Build the Tier B drivers against **egui first** (Phase 1–2) through AccessKit. The harness then exists and is proven before the first native shell, and it later catches parity regressions *between* egui and the native shell.
- **One scenario, many shells.** Scenarios reference widget IDs and `Action`s only, never toolkit details. Adding a shell means adding a driver, not rewriting tests.

## 7. Fan-out by default (D5)

The goal is that a future session, given "add feature X", reaches every front-end on its own.

1. **`docs/platform_matrix.md`** is the single source of truth: target, world (modern/vintage), shells, toolchain (stable / 1.73 / mrustc / nightly build-std), feature flags (`chd`, `pure-zstd`, `crypto`, `optical`, `audio`), and known limits. CI parses it so a release job and its matrix row cannot drift.
2. **A new CLAUDE.md section, "Feature fan-out" (mandatory):**
   1. Put the logic in engine, model or view-model, never in a shell.
   2. Read the platform matrix.
   3. Handle the new `Action`/`Effect` in every shell, which the exhaustive matches force anyway.
   4. For each target, check the constraints (1.73 API floor, mrustc gaps, no `chd` on slim builds).
   5. Mark the feature N/A with a reason where it cannot work.
   6. Add a Tier A scenario.
   7. Extend the CLI if the feature is scriptable (existing parity rule).

   It joins the existing pre-commit documentation sync checklist.
3. **Optionally, a project skill (`/fanout`)** that runs the survey and prints a per-target checklist before coding starts.
4. **Mechanical backstops:**
   - The exhaustive `match` turns a missed shell into a compile error.
   - The boundary-check script turns logic in a shell into a CI error.
   - The `doc_parity`-style test extends to the platform matrix.

## 8. Vintage GUIs over a C ABI (deferred, D3)

PPC (mrustc, built on the G5) and Solaris 9 (mrustc + gcc 4.9) cannot run Rust GUI bindings. When this is picked up:

- Add a `staticlib` C ABI over `src/vm`: opaque view-model handles, `apply(action_json)`, `snapshot_json()`, a wake callback, and effect callbacks.
  - JSON keeps the ABI tiny and mrustc-friendly.
  - The exhaustive-match guarantee is lost at the C boundary, so a generated action/effect list plus a CI check stands in for it.
- Shells:
  - Carbon (or Cocoa-10.4 ObjC) for PPC, using the old `ppc-tiger/rusty_backup_gui.c` as reference only, not its engine.
  - Motif for Solaris, with the tree views flattened.
- Alternative to evaluate then: extend the `rb-cli serve` protocol with job and event messages, and have the vintage shell drive a local engine process. No FFI is needed, but it adds a protocol to version.

## 9. Linux

### Today's Linux floors

What CI builds today (checked against `release.yml`, 2026-10-29):

| Artifact | Built on | Runs on |
|---|---|---|
| deb / rpm / rb-cli | `ubuntu-24.04` runners (`release.yml:48-51`) | **glibc ≥ 2.39 only**: Ubuntu 24.04+, Debian 13+, Fedora 40+. They **already do not run on 22.04** (glibc 2.35). |
| AppImage | Arch container, bundled by Anylinux **quick-sharun** (`release.yml:471-621`) | Any distro, because sharun ships its own libraries **including glibc and the loader**. The egui AppImage already bundles GTK3 (for `rfd`) and Mesa this way. |
| musl rb-cli | static musl | Any Linux |

A GTK4 dependency therefore does **not** raise any floor:
- The packaged path is already 24.04+, and 24.04 ships GTK 4.14.
- The "older Linux" path already exists: the Anylinux AppImage, which can bundle GTK4 the same way it bundles GTK3 today.

### Decision (D2, 2026-10-29)

- **Modern: GTK4 + libadwaita ≥ 1.5.** On GNOME (the default on Ubuntu, Fedora, Debian and RHEL), libadwaita *is* the native look. Its dialogs, toasts, settings rows, status pages and adaptive split view replace view code we would otherwise hand-build. Custom widgets (partition bar, Commander lists, hex view) stay plain GTK4. Reference apps: Pika Backup, Impression, Mission Center (Rust + GTK4 + libadwaita), and Nautilus for the two-pane file manager.
- **Vintage: GTK3, where GTK3 is available.** It ships in the Anylinux AppImage and follows the user's GTK theme, which suits older Xfce and MATE desktops.
- **Everywhere else: the TUI.** That means no display server, or an arch or libc with no GTK3 build.

### GTK4 vs GTK3 shells

| Shell | Package | Toolchain | Ships as | Runs on |
|---|---|---|---|---|
| GTK4 (modern) | `frontends/gtk4/` | stable (gtk4-rs 0.11 needs rustc ≥ 1.92) | deb, rpm, Arch package against the system `libgtk-4-1` ≥ 4.14 | 24.04-era+ distros |
| GTK3 (vintage) | `frontends-vintage/gtk3/` | ideally Rust 1.73 with the vintage engine floor (gtk3-rs 0.18 MSRV believed ≈1.70; **verify**) | Anylinux AppImage with GTK3 bundled | any distro, any age |

Why this works well:
- **Same model as every other OS (D4).** Linux gets its own modern/vintage pair. If gtk3-rs builds on 1.73, the GTK3 shell lives in the same vintage world as Win7 and macOS 10.7, with the same frozen dependencies and the same CI job style.
- **Less risky than bundling GTK4 on old systems.** The AppImage pipeline already bundles GTK3 today, because `rfd` uses it. GTK3 draws with cairo and needs no GL, which suits old hardware and VMs.
- **Native look on the old desktops.** Xfce and MATE are still GTK3, and LTS distros from 2018–2022 ship GTK3 themes.

Costs:
- **gtk3-rs is archived (since 2024).** It builds, but gets no fixes. Pin it like the other vintage dependencies. If it rots, the fallback is a thin C GTK3 shell over the C ABI from §8.
- **A sixth GUI shell.** GTK3 and GTK4 APIs differ a lot: GTK4 removed `Container::add`, replaced the event model, and moved to list models. So this is a fork, as with the Win32 pair, sharing only the view-models. The exhaustive `Action`/`Effect` matches keep it from drifting.
- **No `FileDialog` / `ColumnView`.** GTK3 uses `FileChooserNative` and `TreeView`. Commander's outline view maps onto `TreeView` + `TreeStore` well.

### Earlier option, superseded: one GTK4 shell, two delivery paths

| Path | Who it is for | GTK4 comes from |
|---|---|---|
| deb / rpm / Arch package | 24.04-era and newer distros | the system (`libgtk-4-1` ≥ 4.14) |
| Anylinux AppImage | everything else, including old distros | bundled, built in the Arch container |

No separate old-Linux shell (GTK3, FLTK, or egui kept for old Linux). This rests on a **Phase 4 prototype gate:**
- Build a GTK4 hello-world through quick-sharun.
- Run it on the oldest distro we care about, for example a 20.04 or CentOS 7–era VM.
- Run it under `GSK_RENDERER=cairo` (no GL) for old hardware and VMs.

If the prototype fails, fall back to keeping the egui AppImage only for old distros, as a time-boxed exception.

**Compile-time GTK floor:** gtk4-rs `v4_14`. That gives `gtk::FileDialog` (4.10+), `ColumnView` with sorting and sections, and `TreeListModel`, and matches Ubuntu 24.04.

**Notes:**
- `gtk4-rs` needs a recent rustc. That is fine, since Linux has no 1.73 build.
- Wayland and X11 both work. The riscv64 cross build needs `libgtk-4-dev:riscv64` from ubuntu-ports `noble`, next to today's GTK3 sysroot.
- The privilege flow ("Unlock Physical Devices" -> pkexec) and the launch smoke test (`release.yml:625`) carry over unchanged.
- Tier B smoke runs under Xvfb (already installed in the AppImage container) with a D-Bus session and AT-SPI. GTK4 exposes its accessibility tree over AT-SPI natively.

### Flatpak: not for the main build

- Raw block devices need `--device=all`, plus a privilege path. pkexec does not work from inside the sandbox; `flatpak-spawn --host` defeats the point.
- Possible later: an **images-only Flathub edition** built with the existing `no-physical-devices` feature, where file pickers go through portals. That is a distribution decision, not a shell decision.

### Open Linux questions

0. Does gtk3-rs 0.18 build on Rust 1.73? This decides whether the GTK3 shell joins the vintage CI world or needs its own toolchain pin.
1. ~~Plain GTK4 or libadwaita~~: decided, libadwaita ≥ 1.5.
2. Accept "AppImage covers old distros" pending the prototype gate, and which distro or year is the oldest to test against?
3. Flatpak images-only edition: later, or never?

## 10. Risks and mitigations

| Risk | Mitigation |
|---|---|
| Lifting about 14.5k LOC out of the GUI regresses behavior | One screen per slice, starting with Commander. Tier A scenarios are written *before* each extraction; Tier B on egui catches view regressions. |
| The job API is wrong for some operation (streaming, nested jobs, multi-step wizards) | Phase 1 migrates all about 66 operations onto it in egui before any native shell depends on it |
| View-models accidentally use post-1.73 APIs | The vintage CI job compiles and tests `src/vm` (and `cargo +1.73 check` locally, per CONTRIBUTING) |
| mrustc cannot compile some view-model construct | Keep `src/vm` plain: no proc-macros, no const generics. Check with the PPC/Sol9 builds each phase. |
| N shells drift apart | Exhaustive matches, widget-ID scenarios shared across shells, and the platform matrix enforced by CI |
| Accessibility-driven tests are flaky in CI | Tier B stays tiny and deterministic, using fixture images and no real devices. Tier C is periodic and non-blocking. |
| Elevation regresses when moved off the UI thread | Jobs carry the privileged handle. The existing macOS hardware audit checklist is re-run after Phase 1. |
| Shipping native and egui side by side confuses users | Mark the native app as "preview" until parity; one release-note line per platform |

## 11. Open decisions

1. Linux (§9): does gtk3-rs build on 1.73; the oldest distro to test the GTK3 AppImage on; the Flatpak images-only edition.
2. macOS 10.7 bindings: which ObjC crate builds on 1.73, or ObjC shell + C ABI.
3. C ABI over JSON versus an engine process over the extended daemon protocol, for PPC and Sol9 (§8).
4. Whether the Buildroot appliance gains the `tui` feature or keeps `menu.rs`.
5. Naming and distribution of the side-by-side native preview apps.
6. Whether `/fanout` becomes a project skill, or the CLAUDE.md section alone is enough.
