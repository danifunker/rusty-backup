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
- A form is `adw::PreferencesGroup`s in a `form::page`, with a footer that never scrolls: the outcome, Start, Cancel and the bar stay on screen. Everything but the footer goes insensitive while a job runs.
- A partition table inside a form is a `gtk::Grid`: it is bounded by the table, and a `ColumnView` would recycle a `SpinButton` mid-edit. Rows are kept while their indices match.
- Start asks twice: the button's sensitivity comes from `may_start`, and the click asks `may_start` again, since a button only knows what the last refresh knew. Anything the controller cannot see (another tab's job, a device list being read) goes in `locked_by`.
- A destructive start is only ever `Start`, and the write begins on the answer to the controller's own `Confirm`. The name above the button is `write_target()`, the name that question uses, never the view's copy of a device row. A yes counts only for the question it answered: the controller compares it, so a refresh that moved the row voids it.
- Off screen a form skips its refresh (`is_mapped`): a job wakes the app many times a second and the tab redraws when it is selected. The tick still runs, so a job's end and its log are seen.
- In `app.rs` write `super::backup`, not `crate::backup`: `scripts/check-boundaries.sh` reads `crate::backup` and `crate::restore` as the engine's modules, in comments too.
- A wizard reads only the page on screen: `Panel` is one enum over the steps, matched with no `_` arm, so a job's wakes never lay out a table nobody sees.
- A controller edit that takes a whole struct (`FormatOptions`, `PresetView`) keeps the struct last shown in a `held` cell, and the handler clones it before it sends: the send refreshes the form, which replaces the cell.
- A size entry keeps the user's words: while its text parses to the controller's value it is left alone, so `4K` does not turn into `4096` mid-typing, and an unparsable text stays while it has the focus.
- A partition row is two lines of the grid, fields over content and moves, so a GPT GUID fits at the form's width. The cells are rebuilt only when `Shape` changes: the table, its columns, or which rows can copy.
- A "Pick..." that only stamps a value is a `MenuButton` with a list, not a `DropDown`: there is nothing to show as selected, and a `DropDown` takes the width of its longest item.
- A readout that moves with time while no job wakes the shell (a playing CD-DA track) re-arms a one-shot `timeout_add_local_once` from its refresh only while it moves, as `FOLLOW_DELAY` does; it is not a polling timer.
- A screen whose refresh is skipped off screen connects its root's `map` to `refresh`: a tab selected from code is mapped after the tick that would have drawn it.
- A list whose rows carry a tick box binds the box's state inside the screen's `SyncGuard`, so a recycled cell taking its new row's value sends nothing back.
- `TextView::scroll_mark_onscreen` is queued until the lines are measured, so a mark deleted straight after never scrolls. Keep the mark.
- A dialog with a controller of its own (`volume/`: resize, Expand HFS, Alto, the disk writer) is a struct built when it opens and held by its host, which ticks it and drops it once the controller says it closed. Its `adw::Dialog` has `can_close(false)`: a user close sends the controller's Close, and the window goes by `force_close` only when the controller agrees, so a running write keeps it up. Restore keeps its disk writer for good, so `is_running` sees a write after the window is hidden.
- A button inside a dialog checks `EffectHost::dialog_over(dialog)`, not `dialog_open`: the dialog itself is the visible one, and only the question it asked covers it.
- A dialog's height is a wish: `content_height` shrinks to the app window, so give the body a `vexpand` scroller (`volume::scroller`). A scroller with `propagate_natural_height` asks for more than the window and the bottom is clipped.
- A window only the view holds open (Expand Image) still sits on a `Slot`; when the screen's `Action` is not `Clone`, the slot sends a small `Dismiss` enum and the screen maps it.
- A `ColumnView` row changed in place with its own object keeps its cell, even after `items_changed`: replace the row with a new `BoxedAnyObject` (`splice(i, 1, ..)`) so its tick box rebinds.
- A window the app opens from the header menu (`settings.rs`, `bulk.rs`) is a struct with its own controller; Bulk Convert's controller outlives its window, so the app keeps it and ticks it.
- A destructive dialog shows "Writes to:" with the controller's own name (`ResizeController::write_target`, `table_write_target`, `write_target_name`), the name its question uses.

## Slices

| # | Slice | Status |
|---|---|---|
| 0 | Skeleton: window, tabs, log, effect host, `Notify`, elevation, CI target | done |
| 1 | Inspect: source picker, partition bar (`DrawingArea`), partition table, tools | done; Browse, Check, CHD Info, Export, Expand Image and remote wait on slices 2 and 7, greyed out |
| 2 | Browse view on `BrowserPaneController` (`ColumnView`, `TreeListModel`), file detail, journal | done; `browser/` is the reusable pane, detail panel and windows; the metadata editors wait on slice 7, greyed out; the Optical disc browser header and host drag and drop wait on slices 6 and 8 |
| 3 | Commander: two panes, key map, drag and drop, staged edits | done; `commander/` takes the window over, every source opens (remote included: `Connect` is the controller's own prompt), File Info is `browser/info.rs` with the controller's field prompts, and Browse opens it from the row menu; the graphical metadata editors stay slice 7; a drop from the file manager is slice 8, a drag out to it is not built |
| 4 | Backup and Restore | done; `backup/` and `restore/` are the tabs, `form.rs` the adw rows and `chd.rs` / `size_row.rs` the controls later slices reuse; Write Image File picks its source but its Physical Disk Export window is slice 7, greyed out; the top bar's Close Backup and the Inspect share live in `app.rs` |
| 5 | Build Disk wizard | done; `build_disk/` is the tab, every step egui has: Kind, Output (image and container, or a device), Disk (table, donor, volume, preset, size), Partitions (`rows.rs`), Options, Review, Build; the option rules, hints and `DiskKind::hint` moved into the controller; "Open in Inspect" is `app::open_in_inspect` |
| 6 | Optical and Archives | done; `optical/` is the tab (source, disc line, output form with the CD CHD control, the CHD Info, Add remote daemon and CD-DA player windows) and `archives/` the other (entry list with tick boxes, fork format, Extract All / Selected, Mount in Inspect); the disc browser is `BrowseView::disc`, with its own header and only Extract beside the listing; Inspect hands an optical image or a Mac archive over through `app::open_in_optical` / `open_in_archives`; Start asks before it replaces an output file, and the output follows the format's extension; a drive is unit-tested only |
| 7a | Volume dialogs: Resize Partitions, Expand HFS, Resize Alto Disk, the partition editor (Edit Partition Table, Add Partition), Physical Disk Export / Write Image File, Expand Image, Check results, CHD Info, the SquashFS budget | done; `volume/` holds the dialogs with their own controllers (`resize.rs`, `expand_hfs.rs`, `alto.rs`, `disk_write.rs`), `inspect/windows.rs` and `inspect/editor.rs` the Inspect ones; Restore's Write Image File opens the disk writer, which can also pick an image-file target (`PickTargetImage`); the SquashFS budget and the browse view's CHD Info and Check were already slice 2. Resize and Apply Changes now ask a `Confirm` a yes must still match, a resize after its write plans nothing more, and the writer voids a yes whose target changed. A device target is unit-tested only |
| 7b | Dialogs: metadata editor, Export Disk Image (Inspect's door to Physical Disk Export), bulk and floppy convert, remote (with Inspect's Pick Another Image, Back Up Image, Close Remote), settings, update banner | done; the header menu (`app.rs`: Settings, Bulk Convert, version), `settings.rs`, the update banner (text and View Release from the app controller), `bulk.rs` with its strip on every tab, `inspect/export.rs` (its settings edited through `SetExportWholeDisk`, `SetExportFormat`, `SetExportSize`; Physical Disk Export hands the image to Inspect's own disk writer), `volume/floppy.rs`, `inspect/remote.rs` (the daemon listing takes Inspect's main area; Pick Another Image, Back Up Image, Close Remote and Disconnect sit under the picker), and Edit Metadata over the pane's own `MetadataForm` (type and creator with a known-type pick, Mac dates, permissions, owner; the TUI uses the same form). File Info keeps the controller's field prompts. Nothing is greyed out any more, so `widgets::coming` is gone |
| 7c | A Convert tab in every shell (egui, TUI, GTK; `rb-cli convert` to consider): one file or a folder, routed by a new controller to the bulk, floppy, optical and export engines. Inspect stays browse, resize and export (per partition, resized); Bulk Convert and Convert Floppy Container move into the tab. Details to agree with the user first | planned (agreed 2026-11-07) |
| 8 | Drop routing, file open, cross-tab handoffs, about, `.desktop` and icon, elevated relaunch | done; a `DropTarget` for file lists on the window: Commander opens the path in its active pane, Inspect's browse view in edit mode stages the files (`AddPicked`), anything else goes through `AppController::route_drop`. File open is `HANDLES_OPEN` (`main.rs`), the hand-offs are `app::open_in_*`. About is `adw::AboutDialog` over `controller::app::about`. `data/` holds the `.desktop` file (validated); the icon is installed by packaging, so an uninstalled build shows a placeholder. Elevation relaunches the running binary through pkexec, now passing `GDK_BACKEND`. Not verified: drops (Broadway has no drag and drop) and the pkexec relaunch (needs the user's password) |
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

A CD-DA track played under the harness goes to the speakers unless the scratch HOME holds an `.asoundrc` with `pcm.!default { type null }`.

To drive it from a script, read the page's own globals. `sendInput(BROADWAY_EVENT_KEY_PRESS, [keysym,
state])` reaches GTK where a synthesized `KeyboardEvent` does not, with state 4 for Ctrl. A
`MouseEvent` dispatched on `document.elementFromPoint` works for clicks and drags once one real click
has landed after the page loaded. Motion is compressed, so a slow drag may arrive as a single event.
Broadway has no GDK drag: a `gtk::DragSource` starts and nothing ever receives it.

A `ComboRow`, picker or `DropDown` opens reliably only if the pointer moved onto it first: hover, wait,
click, and take a screenshot before clicking anything in the list. A popup or a closing dialog can leave the page drawing at another scale, so the next click lands elsewhere: reload the page (navigate to it again) before reading coordinates. The pane's `type` and `key` actions drop punctuation; type a path as one `sendInput` press and release per character, the keysym being the Latin-1 code (`0xff0d` is Return, `0xff08` BackSpace, Ctrl+L is `[0x6c, 4]`), and wait, since the frame lags typed text by several seconds. A blind click into a device list is
how a physical disk gets picked. Text goes to whatever has focus, so check a `SpinRow`'s text took the
click (a double click does) before typing. A synthesized `MouseEvent` does not reach a popup's list, and leaves a text field without the keys after a dialog has closed: use the pane's real click for both. The frame lags the state by a second or more, so wait before reading a screenshot. In a file dialog, Ctrl+L then the path then Return works; a
save dialog takes the path straight into its name field. When the page draws smaller than the frame (the window sits in the top-left 78% of the screenshot), clicks still land at full-scale coordinates: divide the screenshot's position by the scale, or reload until it draws full size.
