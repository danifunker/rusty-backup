# RESUME: the Win32 shell, on the Windows box (handoff 2026-11-09)

The next native shell is Windows (main plan D7). This session starts it on the Windows
development machine, on branch **`win32-shell`**: all of this initiative lands there. Don't open a
PR into `main` unless asked.

## Paste this into the new session

> Read `docs/RESUME-win32-shell.md`, then `docs/win32_shell_plan.md`,
> `docs/native_frontends_plan.md` § 1 (decisions D7, D8) and § Phase 2b, CONTRIBUTING.md
> § Architecture policy (MVC), `docs/gtk4_shell_plan.md` § Rules and the retained-mode pattern,
> and `regression-tests/FLOWS.md`. Check out and pull the `win32-shell` branch, and check
> `git status`. Then follow the plan's "Order of work", starting with Gate 0, and keep going until
> something actually blocks you.

## Where things stand

- **Shipped:** the TUI and the GTK4 shell (Linux) run on the shared controllers in
  `src/controller/`; egui still ships everywhere. No Win32 code exists yet.
- **Tier 8 front-end flows** (`regression-tests/cases/tier8/`, driven by `rb-regress`): one flow
  file per area runs on every shell in its `shells` list and checks what the shell wrote with
  `rb-cli`. Last full run on Linux, 2026-11-09: 89 pass, 4 expected failures (R-089, R-090), on
  the TUI and GTK.
- **Decided 2026-11-09:**
  - D7: Windows before macOS.
  - D8: every front-end presents the same UI; a difference between shells is a controller bug.
  - Copying from an image into a host folder is staged and written on Apply (GTK changes to
    match). Disc conversion happens in the Convert tab (the TUI's in-place Optical convert goes).
- **Plans:** `docs/win32_shell_plan.md` (this work), `docs/appkit_shell_plan.md` (after it).

## The order of work (from the plan)

1. **Gate 0.** A small test window: a button whose UIA `AutomationId` is set to `"backup.start"`
   through Dynamic Annotation (`IAccPropServices::SetHwndPropStr`, `AutomationId_Property`), found
   by that id from Python (`comtypes` over `UIAutomationCore`, or the `uiautomation` package). Try
   it on this machine and, if it can be reached, the Win7 VM. If it fails, the fallback is a
   generated controller-id-to-control-ID table the shell and the driver both read; record which
   way it went in the plan.
2. **The TUI over ConPTY.** `regression-tests/runner/src/tui.rs` is a Unix pty today, so on Windows
   `--shell tui` is skip-platform. Add a Windows `Session` over `CreatePseudoConsole` (Windows 10
   1809+), declaring the kernel32 calls by hand as `openpty` is, so the harness keeps no new
   crates and its Rust 1.73 floor. The screen parser stays as it is. Then run the TUI flows:
   `rb-regress run --tiers 8 --shell tui`. That is the Windows baseline; failures there are in
   shared code, not Win32.
3. **Phase 2b's shared work** the shell builds on: labels and result messages as controller
   accessors, the shared scripted-pick hook (GTK's `RUSTY_BACKUP_TEST_PICKS` in
   `frontends/gtk4/src/effects.rs` moves into `src/controller`), the staged host copy and the
   Convert-tab hand-off. Switch the TUI over here (its flows prove it). Leave GTK's switch-over
   for a Linux session, and note that in the plan.
4. **Audit R14 and R15** in `src/os/windows.rs` (`docs/RESUME-audit-2-windows.md` § 1).
5. **Slices 0a (the layout helper), 0 (the skeleton), then the UIA driver**
   (`regression-tests/drivers/uia_driver.py` + `runner/src/uia.rs`, the same JSON-lines protocol
   as `gtk_driver.py` + `gtk.rs`). Every later slice is proved by flows as it lands.
6. Slices 1 to 8, in the plan's order.

## Facts the flows taught (read FLOWS.md for the rest)

- `regression-tests/drivers/labels.toml` maps controller ids to what each shell calls them. Every
  entry where shells differ is drift; Phase 2b's goal is a `[win32]` section that never needs to
  exist, because Win32 finds controls by `AutomationId`.
- `rb-regress tui-shot` prints the TUI's screen after each key (`tab:NAME`, `fill:TEXT`,
  `wait:TEXT`). It is how a flow gets written. On Windows it needs step 2 first.
- Check the bytes, not the screen: end flows with `rb-cli` steps (`get` + `files_identical`,
  `fsck`, `inspect --format json`). Prefer `wait_file` for a job's end.
- A bug a flow finds is recorded in `docs/Regression_Bugs.md` and listed in
  `regression-tests/data/known-failures.toml`, not worked around.
- Side runs use `--scratch-root ../scratch-x --report-root ../runs-x` (git-ignored). Never
  `git add regression-tests` as a whole: run output got committed that way once.
- Flows that need the fixture corpus (optical, audio) resolve to skip-fixture without it. Set
  `fixture_root` in `regression-tests/local.toml` if the corpus is reachable from this machine.
- `rb-regress` has only been built on Linux since the flow work. Building it on Windows is the
  first check; `tui.rs` is gated to Unix today, so step 2 is where it gets a Windows side.

## This machine

- **Build memory:** `debug = "line-tables-only"` and `jobs = 4` are in the tree; never run two
  cargo builds at once. Keep the repo's `target` folder out of Defender's real-time scan.
- **Python** for the UIA driver: `comtypes` (or `uiautomation`). The GTK driver is the model.
- **UAC:** run the flows unelevated, against image files only.
- **The Win7 trap:** `DeviceIoControl` with a NULL `lpBytesReturned` and a NULL `lpOverlapped`
  access-violates on Windows 7 only. Never add such a call.

## Rules that bite

- Engine, model and controller code compiles on Rust 1.73 (`crate::compat`,
  `rust173_compat`); the shell itself is stable Rust with its own lockfile. clippy's autofixes
  (`is_multiple_of`, `io::Error::other`, `repeat_n`) postdate 1.73.
- Comments are one line, two at most. No Unicode glyphs in UI strings or logs.
- Every `Action` and `Effect` is matched with no `_` arm; `scripts/check-boundaries.sh` must
  report `frontends/win32` at 0 engine references.
- The pre-commit hook runs `git add -u` and clippy with `-D warnings`: commit with explicit paths.
- Commit often, one coherent step per commit, on `win32-shell`. Pushing it runs CI on every target
  but the appliance, with no release; batch pushes rather than pushing every commit.
- A crate fix ships with a version bump and a publish.
