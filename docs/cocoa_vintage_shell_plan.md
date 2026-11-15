# The vintage Cocoa shell: Mac OS X 10.4 to 10.12

**Status: Phases 1 to 4 done for x86_64 (2026-11-14).** § 7 has what the gates showed, § 8 where it stands. This is Phase 6's macOS
item in [native_frontends_plan.md](native_frontends_plan.md), with §8's C ABI, for the Macs the
AppKit shell ([appkit_shell_plan.md](appkit_shell_plan.md), 10.13 and later) cannot reach.

## 1. Decisions so far

| | Decision |
|---|---|
| V1 | Three Mac builds: the AppKit shell (10.13+), this shell on x86_64 (10.7 to 10.12), and this shell on PowerPC (10.4 and 10.5, G3 floor). |
| V2 | Intel Macs that stop at 10.4 to 10.6 run the PowerPC build under Rosetta. No i386 slice, no mrustc Intel target: only a handful of Intel models top out below 10.7. |
| V3 | 10.7 first. It builds on Rust 1.73 here in minutes and has a real test Mac. PowerPC follows once the shell works. (macOS 27 is the last release with full Rosetta 2; after it, an arm64 build of the same code stands in for local runs, § 7.) |
| V4 | The floor is 10.4, not 10.3. 10.3 means Xcode 1.5, no Objective-C 2.0 runtime and a smaller AppKit, for little gain. |
| V5 | The shell is Objective-C 1.0 with manual retain/release. objc2 creates classes through the Objective-C 2.0 runtime (10.5+) and mrustc cannot transpile it, so a Rust shell cannot reach PowerPC. One Objective-C code base serves both builds. |

## 2. The shape: Rust screens over an Objective-C widget host

The AppKit shell's screens are already written on two toolkit-free layers:

- `layout.rs`: a tree of rows and columns turned into rectangles, with no AppKit in it;
- `Form<K>`: a screen's controls keyed by its own enum, with about 25 operations (button, check,
  radio, label, pop-up, field, progress; show, enable, set text, picked, select, set items).

A screen is its controller's reading (`read() -> Shown`), its layout tree and its `Form` calls.
Only `Form`, the tables and a few custom views (partition bar, row icons) touch AppKit.

So the plan is **not** to rewrite 20,000 lines of screens in Objective-C. It splits them:

```
src/controller  (the view-models, unchanged)
      |
frontends/mac-screens/   Rust 1.73, mrustc-clean: every screen's Shown, layout and event
      |                   handling, written against a `Host` trait instead of AppKit
      |
      +-- frontends/appkit/          implements Host with objc2 (10.13+)
      |
      +-- frontends-vintage/cocoa/   implements Host in Objective-C over a C ABI (10.4 to 10.12)
```

- **`Host` trait** (Rust): the `Form` operations, plus windows, tables and outlines, menus, the
  toolbar, timers and the effect panels. Keys cross as small integers; text as UTF-8.
- **The C ABI** has two directions:
  - Rust calls the host: `rb_host_button(win, key, title, ax_id)`, `rb_host_set_text(...)`,
    `rb_host_table_rows(...)` and so on, declared `extern "C"` in Rust and defined in Objective-C.
    Both sides link into one executable, so no callback registration is needed.
  - The host calls Rust: `rb_start()`, `rb_event(win, key, kind)`, `rb_text_changed(...)`,
    `rb_tick()` from an `NSTimer`, `rb_key(win, key, mods)`, `rb_answer(...)` for a panel's
    reply, `rb_free(...)` for strings Rust returned.
- **The Objective-C host** is a generic widget factory: an `NSWindow` per screen, a flipped content
  view with frames set from the layout, controls tagged with the controller's id as their
  accessibility identifier, and `NSOpenPanel` / `NSSavePanel` / `NSAlert` / a prompt window for
  effects. Estimate: 3,000 to 4,000 lines.

What this buys:

- The two Mac shells cannot drift: a screen fix lands once, in `mac-screens`.
- The Objective-C side has no screen logic, so the 10.4 compatibility surface is small.
- Motif on Solaris 9 (§8 of the frontends plan) could later implement the same `Host`.

What it costs:

- Moving the AppKit shell's screens into `mac-screens` touches a finished, tested shell. The tier
  8 flows (65 on appkit) are the safety net, and the move goes one screen at a time.
- `mac-screens` must stay on Rust 1.73 and mrustc-clean: no let-else, no newer std, no proc
  macros. It is checked by the MSRV script and the Linux box's `hostc` stage.
- The JSON snapshot ABI in §8 of the frontends plan is dropped. Its weak point was that each
  vintage shell would still re-implement every screen.

**The alternative, for comparison:** keep §8 as written (Rust exposes view-model snapshots, the
Objective-C shell builds each screen itself). Less change to the AppKit shell, but roughly
15,000 lines of Objective-C that duplicate it and drift from it. Not recommended.

## 3. Builds

### x86_64, 10.7 to 10.12

- Engine and `mac-screens`: a staticlib from the `rb-cli-vintage/` manifest (Rust 1.73,
  `x86_64-apple-darwin`, deployment target 10.7, its frozen lockfile and features: no CHD, no
  optical, no crypto).
- Objective-C host: clang with `-mmacosx-version-min=10.7 -fno-objc-arc -Werror` and
  `-Werror=unguarded-availability`, so an API newer than 10.7 fails the build. Deprecation
  warnings are off (10.4 has only the old names) and `-fno-blocks` is out (the modern SDK's
  headers need blocks); our code uses no blocks, which gcc 4.0 on the G5 enforces.
- CI: a new `macos-vintage-cocoa` target beside `macos-vintage`, producing a `.app` in a DMG.
- The Lion Mac compiles the Objective-C side with its own Xcode 4.6 at each milestone, as a check
  that nothing post-10.7 slipped past the availability warnings.

### PowerPC, 10.4 and 10.5

`scripts/build-ppc-cocoa.sh` (with `PPC_HOST` set) builds it in three steps:

- `host`: Apple gcc 4.0 on the G5 compiles `host.m` and `main.m` against the 10.4u SDK with
  `-DRB_RUST_MAIN` into `librbcocoahost.a`, kept on the G5 (one per CPU family).
- `app`: `scripts/build-ppc.sh ppc` builds rb-cli-ppc's second bin, `rusty-backup-cocoa`
  (`rust/src/ppc_main.rs`). mrustc links binaries, not staticlibs, so Rust owns `main` and calls
  the host's `rb_objc_main`; `#[link]` attributes name the archive, Cocoa and libobjc, and the
  archive's folder on the G5 is added to `PPC_LDFLAGS`, which reach its gcc unchanged. gcc 10's
  driver does the link.
- `bundle`: the .app (10.4 floor in Info.plist) and its HFS+ DMG are made on the G5, into
  `dist/Rusty-Backup-Cocoa-macos-ppc-<cpu>-<version>.dmg`. `scripts/ppc-package.sh` (`RB_APP`)
  copies the MacPorts gcc10 runtime and the 10.4-built legacy-support into `Contents/Frameworks`,
  so the app needs no MacPorts, and checks the CPU floor and the Tiger imports as for rb-cli.

Tiger needs `panic=abort` (no `_Unwind_GetIPInfo` in `libgcc_s.10.4`). No CI: built and packaged
on the G5 like `rb-cli-ppc`.

## 4. Testing

- **Every day:** the x86_64 10.7 build on this Mac under Rosetta 2, driven by the tier 8 flows.
  Once Rosetta goes (macOS 28), the arm64 build of the same code (`build.sh release arm64`).
- **Driver:** the Swift AX driver needs 10.9+, so it is ported to Objective-C. One driver then
  runs everywhere from 10.7 up, and could replace the Swift one. To check in Gate 0: whether 10.7
  reports `AXIdentifier` for an id set from code.
- **The Lion Mac:** each milestone runs there; the flows run there too once the Objective-C driver
  works and Accessibility is enabled.
- **PowerPC:** by hand on the G5 at first. 10.4's AX has no identifiers, so the flows would need
  titles there; decide later.
- **Screenshots:** the tier 9 tour gains a `cocoa` shell, so both Mac shells can be compared
  side by side.

## 5. Phases

0. **Gates**, each a day or less:
   - a Rust 1.73 staticlib linked into a 10.7 Objective-C app that opens a window and shows
     `rb-cli`'s version, launched on the Lion Mac;
   - `hostc` on the Linux box transpiles a stub `mac-screens` crate;
   - the Objective-C AX driver reads an identifier on 10.7 and on this Mac.
1. **`mac-screens` and the `Host` trait**, with `Form` and `layout` moved in and the AppKit shell
   switched to them. The appkit flows must stay green.
2. **The Objective-C host**: windows, the `Form` controls, tables, effects, the timer. Inspect
   moves into `mac-screens` and runs on both shells.
3. **The rest of the screens**, in the AppKit shell's slice order, each moved and then run on
   the vintage shell, with its flows.
4. **Packaging and CI** for x86_64: the `.app`, its DMG, the CI target, a release asset.
5. **PowerPC**: the staticlib through mrustc, the host built on the G5, Tiger fixes, a DMG made
   on the G5.

## 6. Open questions

1. Whether to move the AppKit shell's screens (recommended) or fork them for the vintage shell.
2. Whether 10.4's AppKit covers every control the screens use (outline views and toolbars do;
   a few details such as the dark appearance are skipped there). To be listed in phase 2.
3. Physical disks: `os-stub` has none on PowerPC. 10.7 has the full device layer.
4. What the PowerPC release looks like: a DMG on GitHub built by hand, or a separate channel.

## 7. Gate 0 results

Built by `frontends-vintage/cocoa/build.sh [debug|release] [x86_64|arm64]`. The Rust half is the
`rusty-backup-cocoa` staticlib (`frontends-vintage/cocoa/rust/`), a member of `rb-cli-vintage`'s
workspace so it shares that frozen 1.73 lockfile. The host is `frontends-vintage/cocoa/host/main.m`.
The AX probe is `regression-tests/drivers/ax_probe.m`, Objective-C 1.0 and built for 10.7.

| Gate | Result |
|---|---|
| Rust 1.73 staticlib in a 10.7 Objective-C app | **Pass.** x86_64, `LC_VERSION_MIN_MACOSX` 10.7, no known post-10.7 symbols, 436 KB. It launches on the Lion Mac (10.7.5) through `open` with no dyld error. Its window still has to be seen there (below). |
| Same app on this Mac | **Pass**, both the x86_64 build under Rosetta 2 and the arm64 build: the window shows the version and summary from Rust. |
| AX identifiers set from code | **Pass on this Mac** (x86_64 and arm64): `setIdentifier:` (10.7 API) is read back as `AXIdentifier` for all three labels. **10.7 not yet checked:** Accessibility is off on the Lion Mac. |
| `hostc` transpiles a stub `mac-screens` | Not started. |

Lion test-rig findings:

- A GUI app started from an ssh shell gets no window-server connection, and
  `CGWindowListCopyWindowInfo` blocks. Start apps with `open`. `launchctl bsexec` needs root.
- `/usr/bin/python -c "import Quartz"` hangs over ssh; don't use it for window ids.
- `NSLog` from an `open`-launched app did not show up in `syslog` or `system.log` for the
  `admin` user, so the window id for `screencapture -l` has no route back yet.
- `scp` fails against Lion's sshd; pipe through `ssh ... 'cat > file'` instead.
- The Mac drops off the network for minutes at a time ("Host is down").

## 8. Where it stands (2026-11-14)

Phases 1 to 3 are done for x86_64: every AppKit screen, dialog and the main window live in
`frontends/mac-screens/src/`, and both shells run them.

- **Layout.** `frontends/mac-screens/src/` is shared source, not a crate: the two shells link
  different engine packages under the one crate name `rusty_backup`, so each mounts it with
  `#[path = ".../mac-screens/src/mod.rs"] mod screens;`. The vintage build compiling it on 1.73 is
  its version check. Let-else is fine (mrustc already takes the engine's 684).
- **The `Host` trait** (`host.rs`): windows, the `Form` controls, lists, pane lists (rows with
  indent, icon and tick box), a canvas with a `Pen` (the partition bar draws through it), text
  views and areas, menus at a point, the main window's pages, panes, toolbar, menu bar, drops,
  the panels and alerts the shared `EffectHost` asks for, and the main queue (`later`, `after`).
- **Feature gates.** The mounting crate's features decide: `chd` (a stand-in `chd` module with
  the same calls and no controls keeps the screens free of cfg lines), `optical` (Optical, its
  remote window, the disc browser) and `update` (the banner). AppKit turns all three on; the
  vintage build has none of them (its engine has no CHD, optical or network stack).
- **AppKit** (`frontends/appkit/src/`) is now its host only: `form.rs` (the `Host`), `rows.rs`,
  `canvas.rs`, `table.rs`, `effects.rs` (panels), `popup.rs`, `toolbar.rs`, `menu.rs`, `ui.rs`,
  and `app.rs` (the application object and its delegate). All 68 tier 8 appkit flows pass.
- **Vintage** (`frontends-vintage/cocoa/`): `rust/src/host.rs` implements the `Host` over the C
  ABI that `host/host.m` (Objective-C 1.0, 10.4 APIs, `NSInteger` shimmed) fulfils; `menu.rs` is
  its menu bar; `lib.rs` opens the shared main window. Cell-based lists (10.4 has no other), an
  indented, iconned name cell, the Tabler toolbar and row icons in the bundle, NSMenu pop-ups with
  a pre-10.6 fallback, `performSelectorOnMainThread:` for wakes and `later`.
- **The flow driver** (`regression-tests/drivers/ax_driver.m`) is `ax_driver.swift` in
  Objective-C, built for 10.7. The runner's `cocoa` shell runs each AppKit flow on the vintage app
  (`--cocoa`), save those that need CHD, optical media, CD audio or Convert.
- **CI.** `build-macos-vintage` builds the app with `build.sh`, checks its 10.7 floor and symbols,
  and ships `Rusty-Backup-Cocoa-macos-vintage-x64-<ver>.dmg` (HFS+, readable on 10.7).
- **Flows.** All cocoa flows pass here under Rosetta, the floppy conversions on the Convert tab
  included, and all 65 pass on a real 10.7.5 Mac through `regression-tests/drivers/remote_cocoa.py`
  (FLOWS.md, "Cocoa flows on a real 10.7 Mac").
- **Small screens.** Windows are kept inside the screen's usable area (`fit_on_screen` in
  `host.m`), and wrapped text is sized by the lines it wraps to (`Form::wrapped_lines`), so every
  tab fits an 800x600 display. `RUSTY_BACKUP_SCREEN=800x600` lays the app out as on that screen,
  for a screenshot run; on PowerPC, the bundle's icon is `icon-vintage.icns`
  (`scripts/make-legacy-icns.py`), since Tiger reads none of `icon.icns`' formats.
- **PowerPC** (Phase 5): built per § 3 for G3 and G4 and run on a G5 under 10.5.8. Two fixes
  came out of it: mrustc wrote float constants in the host's byte order (every column width
  read as 0; fixed in the pinned fork, `docs/build-ppc-mrustc.md`), and push buttons now get
  their bezel's full height (Leopard's pill lost its top edge in a 23 pt row).
