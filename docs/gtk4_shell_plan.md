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
- Keep a screen's `Rc` out of its builders with `Rc::new_cyclic`: they take an action closure that
  upgrades a `Weak` and drops the action while `sync.active()`, so no handler checks the guard itself.
- A `RefCell` a change signal's handler reads must not be borrowed while the store or widget
  changes: `ListStore::splice` fires `selected-notify` synchronously. Swap the cached rows first.
- `ListStore` rows are `glib::BoxedAnyObject` around the controller's plain row struct. A button in
  a cell captures a weak `ListItem` and reads the bound row when clicked, because cells are recycled.
- A button whose dialog is a later slice stays on screen through `widgets::coming`.
- `log::` calls on the UI thread do not wake the shell. A view's own message goes through
  `app::append_log` with the screen's drained log lines.
- The GTK colours live in `theme.rs` and are the only thing copied from `src/gui/theme.rs`.

## Slices

| # | Slice | Status |
|---|---|---|
| 0 | Skeleton: window, tabs, log, effect host, `Notify`, elevation, CI target | done |
| 1 | Inspect: source picker, partition bar (`DrawingArea`), partition table, tools | done; Browse, Check, CHD Info, Export, Expand Image and remote wait on slices 2 and 7, greyed out |
| 2 | Browse view on `BrowserPaneController` (`ColumnView`, `TreeListModel`), file detail, journal | |
| 3 | Commander: two panes, key map, drag and drop, staged edits | |
| 4 | Backup and Restore | |
| 5 | Build Disk wizard | |
| 6 | Optical and Archives | |
| 7 | Dialogs: resize, expand HFS, Alto resize, partition editor, metadata editor, export, Expand Image, Check results, CHD Info, bulk and floppy convert, remote (with Inspect's Pick Another Image, Back Up Image, Close Remote), settings, update banner | |
| 8 | Drop routing, file open, cross-tab handoffs, about, `.desktop` and icon, elevated relaunch | |
| 9 | Tier B smoke over AT-SPI; deb, rpm and Arch packaging beside egui | |

## Running it without a display

`gtk4-broadwayd :5`, then `GDK_BACKEND=broadway BROADWAY_DISPLAY=:5 rusty-backup-gtk`, and open
`http://127.0.0.1:8085/` in a browser. This needs no X server.

Set `GDK_DEBUG=no-portals` too. Without it `gtk::FileDialog` asks the desktop's portal, so the
picker opens on the real desktop and not in Broadway. Move the pointer before a click: a press with
no motion before it can reach the row under a cell's button rather than the button. A browser that
connects late shows the frame from when it connected until the next redraw.
