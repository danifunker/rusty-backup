# GTK4 shell plan

Phase 4 of [native_frontends_plan.md](native_frontends_plan.md), detail in its §9. Branch: `linux-FE`.
The shell is `frontends/gtk4/`: GTK4 + libadwaita, a pure view over `rusty_backup::controller`,
built on stable Rust with its own lockfile. It ships beside the egui build until it reaches parity
(Tier B and C of the plan's §6), and only then replaces egui on Linux (Phase 7).

## Rules for every slice

- CONTRIBUTING.md § Architecture policy is binding. `scripts/check-boundaries.sh` enforces the
  shell at 0 engine references. Logic a second view would need moves **down** into the controller,
  never sideways out of `src/gui`; that code still has to pass the 1.73 floor.
- `src/gui/<screen>.rs` is the reference for what a screen does; its controller is the contract.
  Match every `Action` and `Effect` with no `_` arm.
- Every actionable widget gets its controller id: `widget.set_widget_name(ids::X)`.
- No Unicode glyphs in strings. Symbolic icons are fine: they are not text.
- Comments are one line, two at most.

## The retained-mode pattern

egui redraws from the controller every frame. GTK keeps widgets, so each screen is a struct of
widgets with:

- `build()`: creates the widgets once and connects signals to `apply(Action)`.
- `apply(action)` and `tick()`: run the controller, pass effects to `EffectHost::run`, then `refresh()`.
- `refresh()`: **read everything from the controller first and release the borrow**, then set
  widgets inside `SyncGuard::run`. Setting a widget fires its change signal, which re-enters the
  controller and panics on the `RefCell`. Every handler returns early when `sync.active()`.
- Only set a widget when its value changed: a blind `set_text` moves the cursor mid-typing, and a
  rebuilt list loses selection and scroll.
- Lists: `gio::ListStore` + `ColumnView` / `ListView`, with `TreeListModel` for trees. Update rows
  in place. Never put one widget per row in a `Box` for a list that can be long.
- No polling timers: jobs wake the shell through `GtkWake`. A running job's progress arrives by wake.

## Slices

| # | Slice | Status |
|---|---|---|
| 0 | Skeleton: window, tabs, log, effect host, `Notify`, elevation, CI target | done |
| 1 | Inspect: source picker, partition bar (`DrawingArea`), partition table, tools | |
| 2 | Browse view on `BrowserPaneController` (`ColumnView`, `TreeListModel`), file detail, journal | |
| 3 | Commander: two panes, key map, drag and drop, staged edits | |
| 4 | Backup and Restore | |
| 5 | Build Disk wizard | |
| 6 | Optical and Archives | |
| 7 | Dialogs: resize, expand HFS, partition editor, metadata editor, bulk and floppy convert, remote, settings, update banner | |
| 8 | Drop routing, file open, cross-tab handoffs, about, `.desktop` and icon, elevated relaunch | |
| 9 | Tier B smoke over AT-SPI; deb, rpm and Arch packaging beside egui | |

## Running it without a display

`gtk4-broadwayd :5`, then `GDK_BACKEND=broadway BROADWAY_DISPLAY=:5 rusty-backup-gtk`, and open
`http://127.0.0.1:8085/` in a browser. This needs no X server and opens nothing on the desktop.
