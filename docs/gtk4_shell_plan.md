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
- Names read off a disk can hold NUL (HFS+'s private folder does), and turning one into a C string
  panics inside GTK and aborts. Everything from a volume goes through `widgets::plain`; `set_text`
  already does. Reach for it in a `StringList`, a tooltip, a text buffer or a dialog title.
- `GtkWake` queues with `idle_add_once`, never `MainContext::invoke`: `Job::cancel` wakes from the
  main thread, and `invoke` would run the tick there, inside `apply`'s borrow.
- A controller tree arrives flat, so `browser::tree::FlatTree` mirrors it into a `TreeListModel`: one
  `ListStore` per open folder, rows updated in place, spliced only when their siblings changed. With
  every expansion applied a row's list position is its index. Run `sync` inside the `SyncGuard`.
- A `BoxedAnyObject` changed in place fires nothing, so cells register a draw closure with their
  screen and `redraw` runs them after a sync. Only on-screen cells exist, so this stays cheap, and
  labels that cost a lookup are asked for here, not for every row.
- A row's click goes through a `GestureClick` on the cell, claimed so the list's own selection never
  runs; `.rb-list` drops the cell padding so cells cover the row. Leave a press on the `expander`
  node unclaimed. The `MultiSelection` only mirrors the controller, set under the guard.
- Keys: an `EventControllerKey` in the capture phase on the pane root, through `keys::key_of` and
  `mods_of`, then `CommanderController::binds`. Nothing in the shell says what a key does.
- `widgets::keep_focus` stops toolbar buttons taking the keyboard on a click, or the pane's keys die
  after any button press. Run it over a screen's root once it is built.
- A window the controller holds open is a `dialogs::Slot`: a key rebuilds it, no key closes it, a
  user close sends its dismiss action. Keep live parts (a count, a readout) out of the key.
- Nothing wakes the shell between a cursor move and `FOLLOW_DELAY`, so the view sets a one-shot
  timeout while `follow_pending()` says one is due. It is a deadline, not a polling timer.
- Nothing that appears on selection may sit above the listing: the row moves under the pointer and
  the second click of a double click misses. The marked-rows bar lives in the fixed footer.
- Shortcuts are read by one capture-phase key controller on the window, which hands them to the screen
  on top while no dialog is up (`EffectHost::dialog_open`). A closing `adw::Dialog` leaves nothing
  focused, and a controller lower down then never sees a key. `widgets::typing` keeps text entries
  and open menus out of it.
- A window a pane opens lives in `dialogs::PaneDialogs`, one set per side. A two-pane screen tags the
  titles, `(L)` and `(R)`; everything else in `Dialogs` is the screen's. Windows only Commander opens
  are in `commander/windows.rs`, on the same `Slot`.
- A drag between panes is the view's own: `PaneView::watch_drags` reads raw events through an
  `EventControllerLegacy` on the screen root, in capture. The row's claimed click denies any gesture,
  and the pointer's grab does not survive the row being selected, so a controller lower down stops
  seeing motion. Broadway delivers no GDK drag, and this works on every backend. `gtk::DragSource`
  belongs to a drag out to the file manager, which nothing does yet.
- A press on one of several selected rows selects only on release, so a drag carries them all.
- Keep live parts out of a window's key, and let the window update them: the checksum window holds a
  `JobList`, File Info a `FileInfo` the refresh feeds.
- A label that names a running file must not move what follows it: the job row's phase has a fixed
  width and ellipsizes, or Cancel walks away from the pointer as the copy goes.
- `CenterBox` sides take their natural width, so a pane with one more button pushes the column. The
  sides are clipped by a `ScrolledWindow` with an `External` policy to stay equal.

## Slices

| # | Slice | Status |
|---|---|---|
| 0 | Skeleton: window, tabs, log, effect host, `Notify`, elevation, CI target | done |
| 1 | Inspect: source picker, partition bar (`DrawingArea`), partition table, tools | done; Browse, Check, CHD Info, Export, Expand Image and remote wait on slices 2 and 7, greyed out |
| 2 | Browse view on `BrowserPaneController` (`ColumnView`, `TreeListModel`), file detail, journal | done; `browser/` is the reusable pane, detail panel and windows; the metadata editors wait on slice 7, greyed out; the Optical disc browser header and host drag and drop wait on slices 6 and 8 |
| 3 | Commander: two panes, key map, drag and drop, staged edits | done; `commander/` takes the window over, every source opens (remote included: `Connect` is the controller's own prompt), File Info is `browser/info.rs` with the controller's field prompts, and Browse opens it from the row menu; the graphical metadata editors stay slice 7; a drop from the file manager is slice 8, a drag out to it is not built |
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
connects late shows the frame from when it connected until the next redraw. A closing `adw::Dialog`
swallows clicks until its animation ends, which a browser tab paints slowly: wait before the next
click. Typed text arrives only as key events, so drive a text field one key at a time. The window
follows the browser's size, so a wider browser means a wider window, not a layout bug.

To drive it from a script, read the page's own globals. `sendInput(BROADWAY_EVENT_KEY_PRESS, [keysym,
state])` reaches GTK where a synthesized `KeyboardEvent` does not, with state 4 for Ctrl. A
`MouseEvent` dispatched on `document.elementFromPoint` works for clicks and drags once one real click
has landed after the page loaded. Motion is compressed, so a slow drag may arrive as a single event.
Broadway has no GDK drag: a `gtk::DragSource` starts and nothing ever receives it.
