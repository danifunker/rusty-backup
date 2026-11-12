# AppKit shell plan (macOS)

Phase 3 of [native_frontends_plan.md](native_frontends_plan.md). Drafted 2026-11-09 from what the
GTK4 shell ([gtk4_shell_plan.md](gtk4_shell_plan.md)) and the tier 8 flows
([regression-tests/FLOWS.md](../regression-tests/FLOWS.md)) taught. Second, after the Win32 shell
(main plan D7). **Slice 0 under way (2026-11-11)**; see "Progress" below.

The shell is `frontends/appkit/`: AppKit through `objc2` / `objc2-app-kit` 0.3 (the versions the
engine already links for IOKit and DiskArbitration), a pure view over `rusty_backup::controller`,
built on stable Rust with its own lockfile. Minimum macOS 10.13, the same as the egui `.app`. It
ships as a second app beside egui, marked preview, until it reaches parity (exit criteria below);
then Phase 7 drops egui from the macOS release.

The 10.7 vintage shell is Phase 6 and is not this plan; whatever is learned here about the AX
driver and the slices carries over to it.

## Before slice 0

These land first, in the shared code, because they decide how much this shell has to invent.
They are listed once for both new shells in
[native_frontends_plan.md](native_frontends_plan.md) § Phase 2b.

- Labels and result messages come from the controllers, so this shell's `drivers/labels.toml`
  section starts empty and stays empty.
- The scripted-pick hook (`RUSTY_BACKUP_TEST_PICKS`) is a shared helper the shell calls.
- The behaviour drift the GTK flows found is settled (staged vs immediate host copy, convert on
  the Optical screen), so this shell copies one answer, not two.
- `docs/capabilities.md` exists, with an empty AppKit column to fill.

## Rules for every slice

The GTK plan's rules are the starting point; most are about retained-mode toolkits, not GTK.

- CONTRIBUTING.md § Architecture policy is binding; `scripts/check-boundaries.sh` adds
  `frontends/appkit` at 0 engine references. Logic a second view would need moves **down** into
  the controller and still passes the 1.73 floor.
- `src/gui/<screen>.rs` and `frontends/gtk4/src/<screen>/` are the references for what a screen
  does; the controller is the contract. Match every `Action` and `Effect` with no `_` arm.
- **Every actionable view gets its controller id as its accessibility identifier**
  (`setAccessibilityIdentifier(ids::X)`). AX exposes it as `AXIdentifier`, so the flow driver
  finds controls by id and never by label. This is the one big improvement over GTK 4.14, which
  keeps widget names off AT-SPI.
- Every caption-and-field pair is linked (`setAccessibilityTitleUIElement` or a labelled control),
  so no field is unnamed. GTK's Expand HFS size spin was not, and a screen reader could not name it.
- No Unicode glyphs in strings. SF Symbols are fine: they are images. Comments are one line, two at
  most.

### The retained-mode pattern, in AppKit terms

- Each screen is a struct of views with `build()`, `apply(action)` + `tick()`, and `refresh()`.
- `refresh()` reads everything from the controller first and releases the borrow, then sets views
  inside the screen's sync guard. AppKit's target-action does not fire on a programmatic set, but
  delegates, bindings and `NSTableView` selection notifications do; every handler returns early
  while the guard is active.
- Only set a view when its value changed: a blind `setStringValue` moves the caret mid-typing, and
  `reloadData` loses selection and scroll. Reload rows in place (`reloadDataForRowIndexes`).
- Lists are view-based `NSTableView` / `NSOutlineView` over the controller's rows, with a data
  source that reads the controller's snapshot; never one view per row in a stack view.
- No polling timers. `Notify::wake` is `dispatch_async` to the main queue, coalesced; a readout
  that moves with no job (a playing CD-DA track) re-arms a one-shot timer only while it moves.
- `wake` may arrive from the main thread (a `Job::cancel`), so it always queues and never runs the
  tick inline: the GTK shell had to move from `MainContext::invoke` to `idle_add_once` for this.
- Names read off a disk can hold NUL. Everything from a volume goes through one `plain()` helper
  before it reaches an `NSString`.
- Start asks twice: enabled from `may_start`, and the click asks `may_start` again. A destructive
  start writes only on the answer to the controller's own `Confirm`, shown with the controller's
  `write_target()` name.
- A dialog with a controller of its own (Resize, Expand HFS, Alto, the disk writer) is a struct the
  host holds and ticks. Its window refuses a close the controller has not agreed to
  (`windowShouldClose:`), so a running write keeps it up.
- A sheet or alert is an `Effect` fulfilled by the effect host, never a modal loop the controller
  waits in.
- Off-screen tabs skip their refresh and redraw when selected; the tick still runs.

### macOS specifics

- **Privilege.** Keep authopen and the descriptor cache (`macos_authopen_descriptor_cache`); jobs
  carry the descriptor. The open audit findings R6 (a locked card's EACCES read as "needs
  privilege") and R11 (an authopen cancel reported as a failure) are fixed in the engine before
  slice 1 needs them.
- **Keys.** `Primary` in `controller::keys` is Cmd; Commander's F-keys need fn on a laptop, so every
  command also has its menu item (the menu bar is where Mac users look).
- **Menus (agreed 2026-11-12, draft: [mockups/appkit-draft.html](mockups/appkit-draft.html)).**
  The window keeps Win32's controls (D8) and the menu bar carries the same commands, each running
  its button's action and greyed out with it: Rusty Backup (About, Check for Updates..., Settings...),
  File (Open File for Inspect..., Open CD-ROM Image..., Open Backup Folder..., Open Recent, Open
  Device, Connect to Remote..., Refresh Devices, Close Backup, Close), Edit's standard items, View
  (the screens on Cmd 1 to 8, the log), Disk and Partition (Inspect's disk and row tools), Window
  and Help. Shortcuts go into the controller's key map, not the shell.
- **In-app update (built 2026-11-12, `update::mac`).** As egui does on Windows:
  the banner's Install downloads the release's signed, notarized DMG for this app and arch,
  attaches it read-only (`hdiutil attach -nobrowse -readonly`), checks the bundle inside with
  `codesign --verify --deep --strict` and that its Team ID matches the running app's, copies it
  beside the running bundle with `ditto`, swaps the two by rename and removes the old one (the
  running process keeps its open files), detaches, and on Restart Now relaunches with `open -n`.
  The menu's Check for Updates... (`AppAction::CheckForUpdates`) checks at once and says when the
  build is up to date. A folder the user cannot write
  (a standard account's /Applications) or an unsigned build falls back to opening the DMG. The
  logic is the engine's (`src/update.rs`) and the app controller's `InstallUpdate`, so egui on
  macOS gets it too; the AppKit preview takes its own DMG, as Win32 takes its own ZIP.
- **Drag and drop.** Drop routing goes through `AppController::route_drop` as in GTK. A drag *out*
  to Finder is a file promise (`NSFilePromiseProvider`); GTK never built its drag out.
- **Packaging.** A second `.app` in the same DMG job, signed and notarized with the egui one; the
  bundle id differs so both can be installed.

## Slices

The GTK slices, in the same order; each is done when its tier 8 flows pass on `appkit`.

| # | Slice | Proved by |
|---|---|---|
| 0 | Skeleton: window, tab view, log, effect host, `Notify`, authopen, menu bar, CI target | the app starts under the driver |
| 1 | Inspect: source picker, partition bar (custom `NSView`), partition table, tools | `flow.inspect.hfs-*` (with slice 7a) |
| 2 | Browse view on `BrowserPaneController` (`NSOutlineView`), file detail, journal | `flow.optical.*` (with slice 6) |
| 3 | Commander: two panes, key map, drag and drop, staged edits | `flow.commander.*`, `flow.files.*` |
| 4 | Backup and Restore | `flow.backup.*`, `flow.restore.*`, `flow.network.*` |
| 5 | Build Disk wizard | `flow.build.*` |
| 6 | Optical (with the CD-DA player) and Archives | `flow.optical.*`, `flow.audio.*` |
| 7a | Volume dialogs: Resize, Expand HFS, Alto, partition editor, disk writer, Expand Image | `flow.inspect.hfs-*` |
| 7b | Metadata editor, Export, remote, settings, update banner | `flow.network.*` |
| 7c | Convert tab | a convert flow (to write; there is none yet) |
| 8 | Drop routing, file open (`application:openURLs:`), hand-offs, About, icon | manual: drops cannot be driven yet |
| 9 | The AX driver in rb-regress, the DMG beside egui | every tier 8 flow with `shells` gaining `appkit` |

## The flow driver: `drivers/ax_driver.swift`

Same protocol as the GTK driver (one JSON op per line on stdin, `{"ok": ...}` back), run by a
`runner/src/ax.rs` client, so every flow file only gains `"appkit"` in its `shells`.

- **Language.** Swift (decided 2026-11-11): one file over `ApplicationServices` (`AXUIElement`),
  compiled by `swiftc` from the Xcode command-line tools, which every macOS runner has. No Python
  package to install, and the same protocol as `gtk_driver.py`.
- **Finding controls.** By `AXIdentifier`, which is the controller id. No `labels.toml` section;
  `[all]` messages still apply.
- **Acting.** `AXPress` for buttons, menu items and pop-up items; `AXValue` set for text fields,
  steppers and sliders; `AXSelectedRows` for table rows. No synthetic mouse clicks, so none of the
  GTK driver's coordinate and popover workarounds should be needed. If one is, write it down the
  way FLOWS.md does for GTK.
- **Pickers.** The shell's effect host calls the shared scripted-pick helper, so `NSOpenPanel` is
  never driven.
- **Audio.** The app's output goes to a null device in flows; find the macOS equivalent of the GTK
  driver's ALSA null sink (an aggregate device, or a test switch in the audio player) before
  `flow.audio.*` runs on a Mac with speakers.
- **The TUI on macOS** already works: `runner/src/tui.rs` is a Unix pty. Add a macOS runner leg to
  run `--shell tui` too.

**Gate 0, before slice 9 depends on it:** the process driving AX needs the Accessibility (TCC)
permission. Locally that is one grant to the terminal or Python. On GitHub's macOS runners, check
whether it is already granted (UI-testing images often are); if not, the AX flows run on a
self-hosted Mac (named in the gitignored `local.toml`, never in the repo) and CI keeps the build
plus `--shell tui`.

## Progress

- **Slice 0 (2026-11-11), built, not yet seen under the driver.** `frontends/appkit/`: its own
  workspace and lockfile, `MACOSX_DEPLOYMENT_TARGET` 10.13 in its `.cargo/config.toml`. The window
  with the D9 screen toolbar (`toolbar.rs`, an `NSToolbar` of selectable items, the shared PNGs
  as template images, Commander at the right end and off until slice 3), the top bar, update
  banner, a page per screen (a scroll view over a flipped body, holding a placeholder until its
  slice), the status line and the log (`app.rs`); the menu bar (`menu.rs`); the effect host
  (`effects.rs`: open and save panels after the scripted pick, alerts for Confirm and Choose, a
  prompt with the controller's validator run as the user types, the pasteboard, URLs), with every
  question opened from a main-queue block of its own; `MacWake` queues one tick on the main queue.
  `layout.rs` is the Win32 shell's tree (prune, then measure), so hidden views take no room;
  every control's action goes to one target that reads the sender's controller id.
  `scripts/check-boundaries.sh` enforces `frontends/appkit/src` from the start, and CI target
  `appkit` runs clippy, the layout tests, the build and the Gate 0 probe.
- **The driver**: `drivers/ax_driver.swift` (the UIA driver's protocol and ops) and
  `runner/src/ax.rs`, which builds it with `swiftc` into `runner/target/` and passes it
  `--appkit <binary>`.
- **Gate 0 on a desk Mac: passed (2026-11-11, macOS 27).** The driver found every control by
  `AXIdentifier`, switched screens through the toolbar items, pressed a button by id and answered
  an alert. The grant goes to the process macOS holds responsible: run from the Claude desktop
  app, that is the Claude Code binary (`claude.app` under `~/Library/Application Support/Claude`),
  not Claude.app, and it took a new process to see it.
- **Gate 0 on GitHub: passed (2026-11-12).** The `macos-26` runner reports `AXIsProcessTrusted`
  true, and `flow.shell.appkit-start` passes in the `appkit` job, so the AX flows run in CI.

- **Slice 1 (2026-11-12), Inspect, first pass.** `inspect.rs` over `InspectController`, as the
  Win32 screen: the source pull-down (recent files and devices), Open File, Open Backup Folder,
  the facts, the job line with its bar, the two rows of tools, the partition bar
  (`partition_bar.rs`, a custom `NSView` with Win32's palette and legend), the partition list
  (`table.rs`, a view-based `NSTableView`), the row tools, and the Check and CHD Info windows
  (`report.rs`). Tools whose windows later slices build (Export, the table editor, Resize, Expand
  Image, Expand HFS, Alto, Browse, Convert, remote) show, stay off and say which slice brings
  them. The menu bar has the agreed File, View, Disk and Partition menus; each item follows the
  control with its id (`ui::usable`), and the shortcuts come from
  `controller::app::MENU_SHORTCUTS` and `AppTab::shortcut`. Checked under the AX driver: an
  AHDI disk and an HFS floppy open, Check runs and its window closes. Learned:
  `NSPopUpButton::itemAtIndex` raises on an empty menu, and a raised Objective-C exception
  inside a delegate callback silently ends it, so the window never appeared; ask the count first.

- **Slice 2 (2026-11-12), the browse view.** `browse.rs` over the single-pane
  `CommanderController`, as the Win32 view: the volume line and header tools, the edit bar, the
  job line with Cancel, notices, the folder tree and listing (`pane_view.rs`: an `NSTableView`
  subclass that hands unused keys to the controller's key map and reports right-clicks, with
  tick boxes, the row icons from `assets/icons/rows` and the indent of a row inside a folder),
  the side panel over its text or hex preview, the status line and mark bar, the View and row
  menus (`popup.rs`), and every window the controller describes (`model_window.rs`, Win32's
  `window.rs` in AppKit). A window's actions reach the view from a main-queue block of their
  own (`app::later`), since the owner may rebuild the window that sent them. Inspect's Browse
  opens it in the page's place. `flow.browse.inspect-extract-all`, `-new-folder` and
  `-edit-text` pass on `appkit`.

- **Slice 3 (2026-11-12), Commander.** `commander.rs` on one `CommanderController`, as the
  Win32 screen: two sides (source pull-down with recent sources, File / Folder / Backup / Remote,
  Close, partitions, New Folder, Apply / Discard / Edits, title and free space, the path as link
  crumbs, the unlock row, the job line) over the shared `PaneView`, the copy column between them,
  quick view, the log and every window the controller describes. It covers the window (the
  toolbar and the screens hide) until the controller closes it; closing the window with staged
  edits asks the controller first. A click-marking pane shows its marks as the selection and
  reports the list taking focus, as Win32 does, so an AX row selection marks and activates its
  side. `drivers/labels.toml` gains an `[appkit]` section with Win32's two behaviour entries and
  the Commander toolbar item's label (an `NSToolbarItem` carries no AX identifier). Not done:
  dragging rows between panes, and drops from Finder (slice 8).

- **Slice 4, Backup (2026-11-12).** `backup.rs` and its VHD options window
  (`backup_vhd.rs`) over `BackupController`, with the shared CHD control (`chd.rs`), all on
  `form.rs`: a screen's controls keyed by its own enum, their actions and text changes handed
  to it by key. A value or pick set through accessibility sends AppKit no change notification
  (it goes through the field's cell, and a pop-up's action waits for its menu to close), so the
  form keeps each field's and pop-up's last reported value: a field's cell reports an outside
  set at once, any other unreported change goes before the next action, and a refresh never
  undoes an edit the screen has not heard. `flow.backup.image-zstd` and `-image-vhd` pass. The
  Remote Drive window comes with slice 7b.

- **Slice 4, Restore (2026-11-12).** `restore.rs` over `RestoreController` on `form.rs`: the
  backup folder and its Recent list, the target (a device, an image file or a new disk), the
  expand and size rows with the before and after partition bars, alignment, notes and the
  caveats. Restore and Inspect share one backup folder through `AppController::share_backup`,
  and the top bar's Close Backup closes it on both. Writing to a target image as its own action
  waits for the disk writer (slice 7a). `flow.restore.folder-to-image` and
  `-share-with-inspect` pass; all 46 appkit flows pass.

- **Slice 5, Build Disk (2026-11-12).** `build_disk.rs` over `BuildDiskController`, the wizard's
  steps on one form with Back / Next / Create, and `build_rows.rs` for the Partitions and
  Options steps, whose rows and option groups are built again only when the plan's shape
  changes. A row's Pick... is a pull-down of the table's type catalog, so accessibility can
  reach every entry. Open in Inspect switches to Inspect with the built image. The three
  `flow.build` flows pass; all 49 appkit flows pass.

- **Slice 6, Optical and Archives (2026-11-12).** `optical.rs` over `OpticalController`: the
  source (a drive, or an image with a pull-down of recent ones), the disc and its tools, the
  rip form with the shared CHD control, and the CD-DA player in its own window, with a 250 ms
  timer while a track plays because nothing else wakes the shell then. The disc browser is
  `BrowseView` in disc mode, standing in the Optical page's place while it is open. CHD Info
  reuses the report window. `archives.rs` over `ArchivesController`, its entries in a `Table`
  that gained a tick-box column. Inspect now hands discs and archives to these screens. Add
  remote daemon waits for slice 7b and Convert... for 7c. The optical, audio and archives
  flows pass; all 53 appkit flows pass.

- **Slice 7a, volume dialogs (2026-11-12).** The disk writer (`disk_write.rs`, from Restore's
  Write Image File), Expand HFS (`expand_hfs.rs`), Resize Partitions (`resize.rs`), Resize Alto
  Disk (`alto.rs`), Expand Image (`expand_image.rs`) and the partition table editor
  (`editor.rs`), each a window on `form.rs` over its controller. A field read while it is being
  edited commits the edit, and its cell reports back: the form now reads without holding its
  borrow and ignores that report. A new flow, `flow.inspect.table.expand-add`, covers Expand
  Image, Resize's preview and the table editor; the disk-write and Expand HFS flows pass too, and
  all 57 appkit flows pass. Inspect's Export window (which also opens the disk writer) comes with
  slice 7b. The Alto dialog has no flow yet (no Alto fixture in tier 8).

## Exit criteria

- Every tier 8 flow passes on `appkit` (the audio flow too), and the expected failures match the
  other shells' (the same bugs, not new ones).
- The AppKit column of `docs/capabilities.md` has no unexplained gap.
- The old Phase 2 open item, a GUI smoke on macOS, is covered by the flows rather than by hand.

## Open questions

1. ~~PyObjC or a Swift helper for the driver~~: Swift (2026-11-11). ~~Whether GitHub's macOS
   runners grant AX~~: they do (2026-11-12).
2. ~~App name and bundle id~~: bundle id `io.github.danifunker.rusty-backup` (2026-11-11), distinct
   from egui's `io.github.dani.rusty-backup`, so both install side by side.
3. How the 10.7 vintage shell (Phase 6) shares slices with this one: forked like the Win32 pair,
   or this package built against older bindings.
