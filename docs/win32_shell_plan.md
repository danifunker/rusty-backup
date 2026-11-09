# Win32 shell plan (Windows)

Phase 5 of [native_frontends_plan.md](native_frontends_plan.md). Drafted 2026-11-09 from what the
GTK4 shell ([gtk4_shell_plan.md](gtk4_shell_plan.md)) and the tier 8 flows
([regression-tests/FLOWS.md](../regression-tests/FLOWS.md)) taught. Not started.

The shell is `frontends/win32/`: Win32 and Common Controls v6 through the `windows` crate (0.62,
which the engine already links), a pure view over `rusty_backup::controller`, built on stable Rust
with its own lockfile. It targets Windows 10 and 11 (x64, i686, ARM64). It ships beside the egui
build, marked preview, until it reaches parity (exit criteria below); then Phase 7 drops egui
from the Windows release.

The Windows 7/8 shell is Phase 6 (`frontends-vintage/win32-win7/`, `windows` 0.58, Rust 1.73), a
fork of this one; see "Toward Windows 7" at the end.

## Before slice 0

These land first, in the shared code. They are listed once for both new shells in
[native_frontends_plan.md](native_frontends_plan.md) § Phase 2b.

- Labels and result messages come from the controllers, so this shell's `drivers/labels.toml`
  section starts empty and stays empty.
- The scripted-pick hook (`RUSTY_BACKUP_TEST_PICKS`) is a shared helper the shell calls.
- The behaviour drift the GTK flows found is settled (staged vs immediate host copy, convert on
  the Optical screen).
- `docs/capabilities.md` exists, with an empty Win32 column to fill.

## Rules for every slice

The GTK plan's rules are the starting point; most are about retained-mode toolkits, not GTK.

- CONTRIBUTING.md § Architecture policy is binding; `scripts/check-boundaries.sh` adds
  `frontends/win32` at 0 engine references. Logic a second view would need moves **down** into the
  controller and still passes the 1.73 floor.
- `src/gui/<screen>.rs` and `frontends/gtk4/src/<screen>/` are the references; the controller is
  the contract. Match every `Action` and `Effect` with no `_` arm.
- **Every actionable control carries its controller id as its UI Automation `AutomationId`**, so
  the flow driver finds controls by id and never by label. A plain Win32 control's AutomationId is
  its numeric control ID, so the shell sets the string through Dynamic Annotation
  (`IAccPropServices::SetHwndPropStr` with `AutomationId_Property`). **Gate 0** checks that this
  shows up in UIA on Windows 10 and 7; if it does not, the fallback is one generated table from
  controller id to control ID that the shell and the driver both read.
- Every field has a label control before it in the tab order (UIA names a field from it), so no
  field is unnamed.
- No Unicode glyphs in strings: the UI font has them, but the TUI and logs do not, and strings are
  shared. Comments are one line, two at most.

### The retained-mode pattern, in Win32 terms

- Each screen is a struct of `HWND`s with `build()`, `apply(action)` + `tick()`, and `refresh()`,
  driven from one window procedure per screen.
- `refresh()` reads everything from the controller first and releases the borrow, then sets
  controls inside the screen's sync guard. **Win32 notifies on programmatic changes**
  (`EN_CHANGE` on `SetWindowText`, `LVN_ITEMCHANGED` on a selection set, `CBN_SELCHANGE` on some
  sets), which is exactly the re-entrancy the GTK shell's `SyncGuard` exists for; every
  `WM_COMMAND` / `WM_NOTIFY` handler returns early while it is active.
- Only set a control when its value changed: `SetWindowText` resets the caret, and refilling a list
  view loses selection and scroll. Lists are `LVS_OWNERDATA` (virtual) list views and tree views
  reading the controller's snapshot; update item counts and invalidate, never refill.
- No polling timers. `Notify::wake` is `PostMessage(WM_APP_JOB)` to the main window, coalesced; a
  readout that moves with no job (a playing CD-DA track) re-arms a one-shot `SetTimer` only while
  it moves.
- `wake` may come from the UI thread itself (a `Job::cancel`), so it always posts and never runs the
  tick inline (GTK had to learn this).
- Names read off a disk can hold NUL; everything from a volume goes through one `plain()` helper
  before it becomes a wide string.
- Start asks twice: enabled from `may_start`, and the click asks again. A destructive start writes
  only on the answer to the controller's own `Confirm`, shown with `write_target()`'s name.
- A dialog with its own controller is a modeless window the host holds and ticks; `WM_CLOSE` is
  refused until the controller agrees, so a running write keeps it up. Never a modal loop the
  controller waits in: a confirmation or a picker is an `Effect` the effect host answers.
- Off-screen tabs skip their refresh and redraw when shown; the tick still runs.

### Windows specifics

- **Privilege.** UAC as today: the app runs unelevated, and "Unlock Physical Devices" relaunches
  elevated. Fix audit finding R14 first (the elevated relaunch drops argv, so a double-clicked file
  needing elevation is lost) and R15 (only lettered volumes are locked, only `PhysicalDrive0..15`
  enumerated) before slice 1 needs them.
- **The Win7 trap.** `DeviceIoControl` with a NULL `lpBytesReturned` and a NULL `lpOverlapped`
  access-violates on Windows 7 only; the shell never adds such a call, and its review checks.
- **DPI.** Per-monitor v2 in the manifest; every size comes from `GetDpiForWindow`, and the custom
  partition bar redraws on `WM_DPICHANGED`.
- **Visual styles.** Common Controls v6 through the manifest, the system font from
  `SystemParametersInfo`, and dark mode left to the system (Win32 has no supported dark API for
  common controls; follow the system's light theme until there is one).
- **Drag and drop.** `IDropTarget` routes through `AppController::route_drop`; a drag out to
  Explorer (`IDataObject` with `CF_HDROP` after extracting to a temp folder) is optional.
- **File associations and update.** The egui build's associations and in-app updater move to the
  native app when it replaces egui; until then the native preview registers nothing.
- **Packaging.** A second exe in the Inno installer and the ZIP, beside egui.
- **Build memory.** The OOM fix (`debug = "line-tables-only"`, `jobs = 4`) applies; keep the repo's
  `target` out of Defender's real-time scan on the dev machine.

## Slices

The GTK slices, in the same order; each is done when its tier 8 flows pass on `win32`.

| # | Slice | Proved by |
|---|---|---|
| 0 | Skeleton: window, tab control, log, effect host, `Notify`, UAC relaunch, manifest, CI target | the app starts under the driver |
| 1 | Inspect: source picker, partition bar (custom-drawn), partition table, tools | `flow.inspect.hfs-*` (with slice 7a) |
| 2 | Browse view on `BrowserPaneController` (virtual list and tree view), file detail, journal | `flow.optical.*` (with slice 6) |
| 3 | Commander: two panes, key map, drag and drop, staged edits | `flow.commander.*`, `flow.files.*` |
| 4 | Backup and Restore | `flow.backup.*`, `flow.restore.*`, `flow.network.*` |
| 5 | Build Disk wizard | `flow.build.*` |
| 6 | Optical (with the CD-DA player) and Archives | `flow.optical.*`, `flow.audio.*` |
| 7a | Volume dialogs: Resize, Expand HFS, Alto, partition editor, disk writer, Expand Image | `flow.inspect.hfs-*` |
| 7b | Metadata editor, Export, remote, settings, update banner | `flow.network.*` |
| 7c | Convert tab | a convert flow (to write; there is none yet) |
| 8 | Drop routing, file open (command line and associations), hand-offs, About, icon | manual: drops cannot be driven yet |
| 9 | The UIA driver in rb-regress; the installer and ZIP beside egui | every tier 8 flow with `shells` gaining `win32` |

## The flow drivers

### Win32: `drivers/uia_driver.py`

Same protocol as the GTK driver (one JSON op per line on stdin, `{"ok": ...}` back), run by a
`runner/src/uia.rs` client, so every flow file only gains `"win32"` in its `shells`.

- **Language.** Python with `comtypes` over `UIAutomationCore` (or the `uiautomation` package that
  wraps it), mirroring `gtk_driver.py`. GitHub's Windows runners have Python and an interactive
  desktop session, so no Xvfb equivalent is needed.
- **Finding controls.** By `AutomationId`, which is the controller id. No `labels.toml` section;
  `[all]` messages still apply.
- **Acting.** UIA patterns: `Invoke` for buttons and menu items, `Value` for edits,
  `SelectionItem` / `ExpandCollapse` for combo boxes, lists and trees, `RangeValue` for spinners.
  No synthetic mouse clicks, so none of the GTK driver's coordinate and popover workarounds should
  be needed. If one is, write it down the way FLOWS.md does for GTK.
- **Pickers.** The shell's effect host calls the shared scripted-pick helper, so `IFileDialog` is
  never driven.
- **Audio.** Flows must not play out of the runner's or a desk machine's speakers: find the WASAPI
  equivalent of the GTK driver's ALSA null sink, or a test switch in the audio player.

### The TUI on Windows: ConPTY in `runner/src/tui.rs`

The TUI driver is a Unix pty today, so `--shell tui` resolves to skip-platform on Windows. Add a
Windows `Session` over `CreatePseudoConsole` (Windows 10 1809+), declared by hand in the runner
like `openpty` is, so the harness keeps no new crates and its 1.73 floor. The screen parser is
unchanged: ConPTY emits the same VT sequences. Windows 7 has no ConPTY, so the TUI flows stay
skip-platform there.

**Gate 0, before slice 9 depends on it:** on Windows 10 and on the Win7 VM, a test window whose
button carries an AutomationId set by Dynamic Annotation is found by that id from Python.

## Exit criteria

- Every tier 8 flow passes on `win32` and on the TUI over ConPTY, and the expected failures match
  the other shells'.
- The Win32 column of `docs/capabilities.md` has no unexplained gap.
- The old Phase 2 open item, a GUI smoke on Windows, is covered by the flows rather than by hand.

## Toward Windows 7 (Phase 6)

`frontends-vintage/win32-win7/` starts as a fork of this shell on `windows` 0.58 and Rust 1.73.
Keep the fork cheap: no Windows 10-only API in the modern shell where an older one does the same
(per-monitor v2 is the one deliberate exception; Win7 gets system DPI). The UIA driver runs
against it on the Win7 VM if Gate 0 passed there. Tier 8 flows then gain a `win32-win7` shell.

## Open questions

1. Dynamic Annotation for AutomationId (Gate 0), or the numeric id table.
2. Python `comtypes` / `uiautomation` for the driver, or a Rust helper outside the 1.73 harness.
3. Whether the Win32 preview shares the egui installer or gets its own until parity.
