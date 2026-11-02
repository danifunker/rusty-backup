# Phase 2: the Commander pilot, slice by slice

Working plan for Phase 2 of [native_frontends_plan.md](native_frontends_plan.md).
Read [CONTRIBUTING.md § Architecture policy (MVC)](../CONTRIBUTING.md#architecture-policy-mvc--read-before-adding-any-code)
first; it is binding. Each slice is one or more commits on `native-frontends`,
each green (tests, `scripts/check-boundaries.sh`, the GUI build).

## Where it starts

- `src/gui/commander/pane.rs` (~4.1k lines): `CommanderPane`, one pane. Almost all
  of it is controller state and orchestration interleaved with egui drawing
  (`show()`, `source_bar`, `render_rows`).
- `src/gui/commander/mod.rs` (~1.7k): `CommanderMode`, the two-pane screen: copy,
  export, checksums, detail window, host conflicts, rename notice, session log.
- The TUI has its own thinner Commander in `src/cli/verbs/tui_app.rs`: `CmdPane`
  (~1971), `CommanderState` (~2121), `handle_commander_key` (~7377),
  `draw_commander*` (~9947-10225). It writes immediately (no staging queue) and has
  no unlock, preflight, backup / partclone sources or wrapper tree.
- The egui Commander had no keyboard shortcuts at all (slice 2 added them).

## Decisions

- The TUI Commander stages edits and applies them, like the GUI.
- Shortcuts come from `src/controller/keys.rs` (done): F1 help, F2 rename, F3 /
  Primary+I info, F5 refresh, F6 copy, F7 edit, F8 / Delete (Mac Delete/Backspace)
  delete, Primary+N new folder, F10 apply, Tab, Enter, Backspace (Mac Cmd+Up)
  parent, Space / Insert mark, Esc close; user overrides in `keybindings.toml`.
- No cross-volume move.
- Text prompts (rename, new folder, connect) are `Effect::AskText` carrying a
  controller-defined validator; the view runs it for inline feedback, the
  controller re-checks on submit. The unlock password is `AskPassword` (or the
  controller's `needs_password()` state, drawn inline by the GUI).
- After Commander, generalise `CommanderPaneController` into one browser-pane
  controller configured by a profile, used by the Commander panes, the Inspect
  browse view and the TUI explorer.

## Field classification (from the survey)

`CommanderPane` (pane.rs ~165-289), controller state: side, source, partitions,
resolved_backup, selected_part, listing, session, browser, remote, archive_source,
optical_source, optical_choices, cache_store, volume_label, fs_type, total_size,
used_size, queue, pending_open / apply / preflight / scan / wrapper / remote,
open_phase, apply_review, apply_report, reopen_restore, error, needs_password,
password_input, pending_switch, pending_host_delete, rename_dialog,
new_folder_dialog, connect_dialog, show_edits_popup, log_events, wrapper_tree,
tree_selected, tree_anchor, last_cwd, recent. View-only: nothing but egui's own
memory; `tree_index` is a cache the controller rebuilds; `open_rate` becomes a job
snapshot.

`CommanderMode` (mod.rs ~138-193), controller state: left, right, status, temp,
unsaved_close, pending_host_copy, pending_host_conflict, rename_notice,
pending_stage_copy, checksums, detail (minus the egui editors), keep_dates,
export_fork_mode, export_format, active, session_log. View: progress_window
widget, show_log, the metadata editor widgets.

## Slices

| # | Slice | Status |
|---|---|---|
| 1 | `controller::commander` skeleton: `CommanderPaneController` owns listing, queue, wrapper tree; display rows move there | done (b940c5f8) |
| - | Rename `vm` -> `controller`; key map (`controller::keys`) | done (d2ab1e1e, 6aecd207) |
| 2 | Navigation, selection and cursor as `CommanderPaneAction`s (`Enter`, `Up`, `NavigateTo`, `Click{ctrl,shift}`, `TreeClick`, `ToggleMark`, `SortBy`, cursor moves, `ActivateCursor`); collapse wrappers on cwd change inside the controller; the egui pane dispatches actions; egui keyboard through `KeyMap` | done (dc13fc05) |
| 3 | Sources as jobs: open image / host / backup / archive / optical, partition select, partclone scan, unlock; `Confirm` / `Pick*` answered by a per-view `gui::effects::EffectHost` | done (ad0950b4, 49fdb34b) |
| 4 | Queue, preflight, apply, report, reopen, wrappers; host delete / rename / mkdir as jobs; `AskText` with validators | done (52b691de, eddb9dee) |
| 5 | Remote (cfg `remote`): connect, open / close image, remote apply | done (1718ee44) |
| 6 | `CommanderController`: both panes, active side, copy, export, checksums, detail data, conflicts, rename notice, session log, unsaved close (`Effect::CloseScreen`) | done (4b807866, 665f7573) |
| 7 | Full key map wired in egui (help overlay listing the bindings) | covered by slices 2 + 6 |
| 8 | TUI Commander rebuilt on the controllers (8a open / navigate / copy, 8b queue / apply / unlock / wrappers); delete `CmdPane` / `CommanderState` | done (4e7e75c6) |
| 9 | Enforce the boundary for `src/gui/commander` and the TUI Commander in `scripts/check-boundaries.sh` | done (7f677b11, c3e749c5, 283d6f92) |
| 10 | One browser-pane controller with profiles; Inspect browse view and TUI explorer move onto it | steps 1-2 done (4a1918a1, 639c37e8); listing styles + quick view done (00835cc5, 55a57f48, b5eb93ac); step 3 (Inspect browse view) done (871b1680, 219d596c, and the leftovers commit; see "Step 3 status"); `src/gui/browse_view.rs` is enforced |
| 11 | The Inspect tab on `controller::inspect` (`InspectController`); the TUI's image / backup open drives it too; `src/gui/inspect_tab.rs` enforced | done (d135d11d, 74d65c28, and the enforcement commit; see "Slice 11: the Inspect tab") |

Slice 3 notes, for the slices that follow:

- Effects that answer back go through the asking controller's view: each egui
  Commander pane owns an `EffectHost` and turns its `Reply`s into that pane's
  `Picked` / `Confirmed`. `gui::run_effects` stays for `AppController`, which
  asks nothing. Pickers are still blocking rfd dialogs on the UI thread.
- An open job holds its path as a `Resource` only while it runs. A new open
  abandons the running one (cancel) and, if they share a path, waits for it to
  let go; another job holding the path fails the open with "in use by".
- Transitional: `install_remote` plus `PaneRequest::SourceReset` (slice 5 moves
  remote in).

Slice 4 notes:

- `TextSpec.validator` is `fn(&TextSpec, &str) -> Result<(), String>`; its context
  (taken names, refused characters, case folding, the "unchanged" value) is data
  in the spec. An empty error disables OK silently. The controller re-runs it on
  `Answered`, then the filesystem's `validate_name`, and re-asks with the error
  (the rejected text pre-filled) on a refusal.
- Edit state lives in `controller/commander/pane/edits.rs`. A source change drops
  in-flight edit work (`EditState::reset`); a partition reopen keeps the report.
- Messages from navigation helpers (wrapper expansion) go through the
  controller's `out` buffer, returned by the next `apply` / `tick`.
- Transitional for slice 5: `set_remote_source` (wrapper extraction and copy
  from a remote image) and `report_apply_failure` (the GUI's remote apply);
  the GUI keeps `spawn_remote_apply` and its `ApplyStatus` progress.
- For slice 6: `stage_edits` / `unstage_since` are still methods the two-pane
  copy calls, and the Close guard clears `queue` directly.
- Behaviour changes: Discard asks first; the edits list can remove one edit;
  New Folder and Rename are refused on an archive pane; a taken name is
  refused in the prompt (it used to fail at Apply).

Slice 5 notes:

- Remote state lives in `controller/commander/pane/remote.rs`. The `RemoteBrowser`
  moves into each connect / open / close job and comes back with the listing, so the
  pane holds it only between jobs; `opening()` / `is_opening()` cover those jobs.
  Job keys are `rb://host:port/` (connect, close) and `rb://host:port/path` (open,
  apply).
- Actions are unconditional; without the `remote` feature `Connect`,
  `OpenRemoteImage` and `CloseRemoteImage` report "remote browsing is not built in."
- Remote apply is the local flow with a different target (`ApplyTarget` in
  `edits.rs`): `preflight_remote_apply`, the same review, `remote_apply_in_job`.
  The daemon cannot skip, so a review's Skip drops the conflicting copies from the
  queue; Replace uploads with force. Only copies and new folders travel; delete and
  rename are refused when asked for. A failed remote apply changed nothing, keeps
  its queue and report, and is not reopened.
- `PaneRequest` is down to `ShowInfo`; `SourceReset` and `OpenImage` are gone.
  `display_rows()` takes no argument (the controller knows when it is remote).
- `gui/commander/progress.rs` stays: the host copy and stage copy still use it (gone in slice 6).
- Behaviour changes: a remote host pane no longer shows Apply / Discard / New
  Folder (staging there never worked); Delete / F8 / rename on a remote pane are
  refused up front; the connect prompt validates the port; a remote copy that hits
  an existing name now gets the review instead of failing the whole upload.
- Tests: `tests/remote_filesystem.rs` `commander_remote::*` drive the controller
  against a loopback daemon (connect, open, new folder, apply, reopen, close,
  disconnect; Skip and Replace; a refused connect).

Slice 6 notes:

- `CommanderController` (`controller/commander/screen.rs`, `screen/copy.rs`) owns both
  panes. `Pane(side, ..)` forwards; a pane's `Message`s become the status line
  (`[L] ...`), its `take_log` lines the session log, its File Info requests
  (`take_info_request`, which replaced `PaneRequest`) open the detail data. A pane's
  picker / confirm / prompt leaves as an effect and the answer is routed back to the
  pane that asked by `purpose` (`asker`); the screen's own purposes are `EXPORT` and
  `CLOSE_UNSAVED`. The egui Commander has one `EffectHost`.
- Copy, stage copy, export and checksums are jobs on the app registry
  (`commander_ops::{host_copy_in_job, stage_copy_in_job}`, `checksum::checksum_in_job`,
  all over the old runners through `worker::run_mirrored`). They hold the destination
  folder and the source image they read, so an apply on that source waits. One copy or
  export runs at a time.
- Archive wrapper mounts now carry a reopen recipe (`ReopenRecipe::ArchiveBytes` or
  by path), so the synchronous wrapper copy / export fallbacks and `export_now` are gone.
- `Key(Key, Mods)` resolves through the `KeyMap` in the controller (`binds` tells the
  view which events to hand over); the F1 help rows come from `key_help()`. That
  covers slice 7.
- File Info: the controller holds side, entry, editable, fs type, preview bytes,
  xattrs and the last result; the egui editors read the pane queue through a
  `MetadataSink` and send `StageMetadata(MetadataEdit)`; `EditQueue::stage_metadata`
  does the staging (the browse view uses the same sink on its own queue).
- Transitional: the File Info preview (up to 1 MiB) and xattrs are still read
  synchronously from the pane's open filesystem in `OpenDetail`; host-copy conflict
  detection stats the destination on the UI thread, as before.
- Behaviour changes: the checksum window shows a progress bar, then all results (not
  each file as it finishes) and gains "Copy all"; a second copy cannot start while a
  stage copy runs (before only host copies blocked it); metadata edits are refused
  while the pane applies; a copy/export from a `.sit` expanded inside an image now
  runs as a job with progress; the unsaved-close dialog is the shared confirmation.

Slice 8 notes:

- One code commit: the old TUI Commander could not be removed in halves. `CmdPane`,
  `CommanderState`, `InfoView`, the `cmd_*` helpers, the connect / checksum /
  host-copy polling in `tick` and the old `handle_commander_key` / `draw_commander*`
  are gone (about 2,300 lines of `tui_app.rs`); the view is
  `src/cli/verbs/tui_app/commander.rs`, which `scripts/check-boundaries.sh` now
  counts as a shell (it reaches the engine 0 times).
- Key map additions (`controller::keys`): `open-image` Primary+O, `open-folder`
  Primary+D, `open-backup` Primary+B, `connect` Primary+K, `partition` Primary+P,
  `sort` Primary+S, `checksums` Primary+G, `export` Primary+E, `edits` Primary+U,
  `log` Primary+L. `KeyMap::for_terminal` / `load_terminal` use the PC chords on
  every platform.
- Controller additions: `ShowPartitions` + `partitions_shown()` (the egui view draws
  a chooser window for it too), `EditDetail(DetailField)` (mode / owner / xattr
  through `AskText` with validators, staged with `MetadataEdit`), `is_working()`,
  `key_hint()`, `CommanderController::polled` (shared registry, no wake), and
  Parent at a remote image's root closes the image. Both panes' rows are rebuilt
  after every `apply` / `tick`, so a copy's staged add shows at once.
- The TUI has no wake: the event loop polls every 50 ms while `is_working()` (200
  ms otherwise) and calls `tick`. Stderr is muted while a job runs so engine
  advisories cannot scribble on the screen.
- Behaviour changes (TUI): edits are staged and written by F10 after the preflight
  (no more immediate writes into images); keys follow the shared map (F5 refresh,
  F6 copy, F8 delete, F2 rename, F3 info, Ctrl+N new folder, Ctrl+O / Ctrl+D open,
  Ctrl+K connect instead of `R`, Ctrl+S sort, Ctrl+G checksums, F1 or `?` help);
  the vim `h/j/k/l` and letter keys (`o i c x n v r s S p # R`) are gone; Left /
  Right switch tabs as on every other tab; Esc closes the topmost window, then
  Commander (asking first while edits are staged) and returns to the previous tab.
  The TUI also gains backup / partclone / archive sources, wrapper expansion,
  unlock, rename, export, the conflict review, the rename notice and the session
  log. Behaviour change (both views): F6 / F8 / Ctrl+G / Ctrl+E act on the cursor
  row when nothing is selected.
- Not carried over: the old File Info's SquashFS size budget and uid/gid names
  (FileDetail does not carry them yet). Restored in slice 9.

Slice 9 notes:

- Quit guard (7f677b11): `CommanderAction::RequestQuit` asks with the shared
  unsaved-edits confirmation (purpose `QUIT_UNSAVED`) and emits the new `Effect::Quit`.
  The TUI's `q` and top-level Esc go through it from any tab (switching to Commander to
  ask); the GUI cancels a window close (`ViewportCommand::CancelClose`) while edits are
  staged and closes on yes. Ctrl+C in the TUI still quits at once (the emergency exit).
  The GUI Commander is a full-window overlay, so there is no tab switch that could drop it.
- Leftovers (c3e749c5): File Info reads its preview, xattrs, uid/gid names and (in a
  SquashFS volume's root folder) the size budget in a job (`model::entry_detail`) over the
  pane's `StageSource`; `FileDetail.loading` is true until it lands, and closing File Info
  cancels it. Closing the checksum window cancels the hashing job. The TUI shows
  `keybindings.toml` problems in the status line and the session log. The TUI's
  unreachable Commander placeholder is gone.
- Enforcement (283d6f92): `ENFORCED` in `scripts/check-boundaries.sh` lists
  `src/gui/commander` and `src/cli/verbs/tui_app/commander.rs`; both are at 0. The global
  report went 863 -> 858. `controller::commander` re-exports `EditQueue`, `JobRegistry`,
  `FileEntry`, `format_size`, `IdNameMap`, `Xattr`, `SizePlan`.
- Behaviour changes: quitting with staged edits asks (TUI and GUI); File Info shows owner
  names and the SquashFS budget in both views, and its preview appears a moment after the
  window opens ("Reading..." until then).
- Left:
  - `host_copy_conflicts` still stats the destination on the UI thread
    (`controller/commander/screen/copy.rs` `copy` and `export_picked`); it is one stat per
    selected top-level entry, not a walk, but a slow network folder would stall a frame.
    Making it a job needs a "checking" step before the conflict review.
  - The TUI has no wake: crossterm's `event::poll` cannot be woken from another thread,
    so a real wake needs an input-reader thread feeding a channel (and stopping it around
    the `:` palette, which reads stdin itself). The 50 ms poll while working stays.
  - The GUI Commander still reaches the model through shared egui helpers outside the
    enforced path (`gui::metadata_editor`, `gui::file_detail`, `gui::source_picker`);
    slice 10 and later move those.

## Slice 10: one browser pane

User decision (2026-11-01): one browser-pane controller, configured per use, instead of
three file browsers. CONTRIBUTING.md § "One controller, many uses" names it the reference
case.

### What each browser does today

| Feature | Commander pane | TUI Explorer (`tui_app.rs` `Explorer`) | Inspect browse view (`gui/browse_view.rs`) |
|---|---|---|---|
| Source | picked by the user: image, folder, backup, archive, disc, remote, drop, recent | fixed: one partition chosen on the Inspect tab, opened through `cli::resolve` | fixed: the Inspect tab's partition (session, partclone cache, streaming, archive edit context, single-file CHD) |
| Listing | flat list with `..`, sortable columns, wrappers expanded in place | folder tree + the tree folder's list, dirs first | one expandable tree, lazy reads through the `FsRead` queue |
| Selection | click / ctrl / shift, Space marks per folder | Space marks, kept across folders (tree and list) | checkbox marks across the tree |
| Writes | staged queue, Apply (preflight, review, report, reopen) | immediate: mkdir, delete, import, metadata, bless, text save, each reopening `rw` | staged queue behind an edit mode, its own review, unsaved-close dialog, staging errors, CHD edit mode (flatten), direct edit, SquashFS budget |
| Bring files in | copy from the other pane | import one host file (Replace / Skip / Cancel) | add file / folder, drag and drop, archive import (extract / convert / as is), tar import, boot blocks |
| Take files out | screen export (folder or archive format), host copy | export menu: fork modes, tar, tar.gz, mar | extract (with overwrite check), export marked as archive, tar export, ProDOS export modes |
| Look inside | File Info window (preview, xattrs, owner names) | preview overlay: text / hex, resource fork toggle | content panel (metadata rows, hex / text), Mac archive window, nested image open |
| Metadata | File Info: mode / owner / xattr (egui: type / creator, ProDOS, dates) | form: type, creator, modified, created, mode, owner | HFS type row, ProDOS type row |
| Text editor | - | yes (line endings, encodings) | yes (load / save jobs) |
| Volume tools | - | fsck, repair, bless, transforms (palette) | fsck, repair, tree listing |

### The profile

`PaneProfile` (`src/controller/browser/profile.rs`) is plain data the pane checks before
it acts; a capability that is off refuses its action with "<what> is not available in this
view." (`BrowserPaneController::refused`, one table in `pane.rs`). Views read it only to
decide what to draw.

- `sources: Sources { image, host, backup, remote }`: what the user may open from the pane.
  `Sources::NONE` when the host screen fixes the source.
- `fixed_source`: the host opened the source with `open_fixed(path, partition)`; Close and
  `SelectPartition` are refused.
- `staging`: edits wait for Apply (true), or apply at once through the same preflight /
  review / write / reopen jobs (false; the Explorer's "writes immediately").
- `read_only`, `rename`, `new_folder`, `delete`: what may be written.
- `wrappers`: archives and images expand in place (off: they are plain files).
- `listing: ListingStyle` (`CombinedTree`, `FlatList`, `TreeAndList`): the style a pane
  starts in; `styles` lists the ones the user may switch to (`SetListingStyle` is refused
  otherwise). `detail: DetailPlacement` (`SidePanel` follows the cursor, `OnRequest` is
  the File Info window). See "Listing styles and quick view" below.
- `tools: Tools { fsck, repair, bless, text_edit, import, metadata, transforms }`.

Constructors: `PaneProfile::commander()` (today's Commander pane exactly: every source,
staging, wrappers, no tools, flat list, File Info on request), `explorer()` (fixed source,
no staging, folder tree + list, every tool), `inspect()` (fixed source, staging, bless /
text / import / metadata, combined tree, side-panel details), and `.read_only()` on any
of them.

### Who owns what

- `controller::browser::BrowserPaneController` (moved from `controller::commander`, which
  re-exports it as `pane` / `rows` so existing paths keep working): source, listing,
  selection, cursor, queue, apply, wrappers, remote; behind `tools`, the volume tools that
  act on the pane's own filesystem (fsck / repair jobs and their report, bless and metadata
  and imports as staged edits, the text editor's load / save jobs, saved text staged as a
  replacing `AddFile`); behind `folder_tree`, the tree.
- Screen controllers own what spans a pane: `CommanderController` (two panes: copy, export,
  checksums, File Info, log, keys); `ExplorerController` (one pane: preview, export with a
  format menu, transforms, close, keys); the Inspect browse view's host (partition choice
  stays with the Inspect tab).
- View-only: widget state. The text editor's buffer and cursor, scroll offsets, a metadata
  form's fields while typing (the controller validates on submit), colours and layout.

### Migration order

1. Move the pane to `controller::browser`, rename it `BrowserPaneController` /
   `BrowserPaneAction`, add `PaneProfile`, `open_fixed` and staging off. Commander keeps
   `PaneProfile::commander()`; its tests pass unchanged apart from the rename.
2. TUI Explorer onto `ExplorerController` + the `explorer` profile; delete `Explorer` and its
   `cli::resolve` write helpers; `tui_app/explorer.rs` joins `ENFORCED`.
3. Inspect browse view onto the pane with the `inspect` profile: listing / navigation /
   selection / staging / apply first, then export / extract, the text editor, archive import,
   tar, CHD edit mode and the SquashFS budget.

### Slice 10 notes

Step 1 (4a1918a1): the move, `PaneProfile`, `open_fixed`, staging off. Behaviour unchanged.

Step 2 (639c37e8, TUI Explorer):

- The Explorer is the Commander screen with one pane: `CommanderController::single(services,
  keymap, PaneProfile::explorer())`, opened with `open_fixed(image, partition)` (the Inspect
  tab's partition-table row is the pane's partition index; the two read the same table).
  `is_single()` screens draw one pane, Tab is `SwitchFocus` (tree / list), Copy has no target,
  and Esc asks "Close <volume>?" even with nothing staged.
- New pane parts, behind the profile: `pane/tree.rs` (the folder tree; its cursor is always the
  listing's folder) and `pane/tools.rs` (check / repair jobs and their `ToolReport`; bless,
  import, the metadata form (`MetadataFields` -> `MetadataEdit`s) and saved text (a temp file
  staged as a replacing `AddFile`) all through the queue; `transforms()` for the command line).
  A pane without staging applies each staged edit at once and asks before a delete
  (`purpose::DELETE_NOW`). A copy that already says Replace / Skip is not a review question
  (`drop_decided_conflicts`).
- Keys: `KeyMap::explorer_defaults` / `explorer_terminal` (`Command::EXPLORER`): the
  file-manager keys without sources, copy and partition, plus Ctrl+F check, Ctrl+R repair,
  Ctrl+B bless, Ctrl+O import, Ctrl+A metadata, Ctrl+T transforms, F7 edit text, Left / Right
  for the tree. No Ctrl+I / M / H / J (a terminal reads them as Tab / Enter / Backspace / LF).
  `key_help()` lists only bound commands.
- `src/cli/verbs/tui_app/explorer.rs` (ENFORCED, 0 references) draws the tree beside the
  Commander pane drawing and the Explorer's own windows; the text editor's buffer stays view
  state. `Explorer`, `build_explorer`, `explorer_write_task` and the `cli::resolve` write
  helpers are gone from `tui_app.rs` (10,978 -> 7,616 lines); shells' engine references 858 -> 808.
- Both TUI views gained an "Export as" menu (`export_choices()`, chosen before the controller's
  `Export`) and a text / hex preview in File Info with `r` for the resource fork
  (`FileDetail.rsrc_preview`).
- Behaviour changes (Explorer): the shared keys replace the letter keys (`d e i m n x f r b t`)
  and vim `hjkl`; Enter on a file opens File Info (with the preview) instead of a preview
  window; an import onto a taken name gets the apply review (Skip / Overwrite) instead of the
  r/s/c prompt; repair asks first; marks are per folder (they no longer span folders, and the
  tree takes no marks); the "Blessed:" crumb is gone; the volume opens through the browser
  pane's path (`BrowseSession`) instead of `cli::resolve`. Gained: rename, checksums, the
  session log, sort, wrapper expansion, the staged-edits list and the help window.

Listing styles and quick view (00835cc5 controller, 55a57f48 egui, b5eb93ac TUI; user
decisions of 2026-11-02):

- The pane's style is runtime state (`listing_style()`), seeded from the profile and from
  the view's remembered choice (`RecentStore::listing_style`, stored in config.json
  `listing_styles` by `CommanderRecent`; `remember_style_as("commander-left")` etc.). A
  fixed-source pane gets `PrefsOnly(store)`, so it keeps its style but records no recent
  files. Switching keeps the folder and the listing's selection; leaving the combined tree
  moves a deep cursor to its top row and drops the deep selection.
- Combined tree (`pane/expand.rs`, `rows::splice_folders`): base folder rows are
  expandable; `ToggleExpand(node_id)` (also wrappers), `ExpandCursor` / `CollapseCursor`
  (Right / Left; Collapse on a closed row steps to its parent row). Children are listed by a
  job (`model::dir_listing::list_folder`) on a reader opened once from the pane's
  `StageSource` and handed back by each job, holding the source's `Resource` (Busy: retried
  next tick); a host pane lists through `std::fs`. A reload, host write, apply reopen or new
  source bumps a generation (in-flight listings are dropped) and re-lists the expanded set,
  which survives a reopen of the same source. Deep rows (`DisplayRow.in_folder`) select by
  path in their own selection, exclusive with the listing's and the wrapper tree's (as those
  two already were); `TreeClick` / `TreeContextSelect` / `ToggleMark` route to it, a Shift
  range stays in one folder. Delete stages against each row's own folder (`selected_pairs`),
  rename goes through `RenameAtCursor` (`PromptTarget::Rename` carries the folder),
  `target_folder()` (the cursor's open folder, else the cursor row's folder) is where new
  folders and copies land (`cwd_entry()`), and name checks read that folder (`names_in`).
  Wrappers inside an expanded folder expand too (`entry_by_path`).
- Tree and list: the existing `pane/tree.rs`, now behind the style; `FolderTreeClick(i)` /
  `FolderTreeToggle(i)` for mouse views; a click in the list takes the keyboard back. In
  two-pane Commander, Tab visits a pane's tree, its list, then the other pane. egui and the
  two-pane TUI put the tree above the list (a half-width pane has no room beside it); the
  TUI Explorer keeps it beside.
- Following detail (`screen/follow.rs`): one slot per side, used by the quick view (active
  side, `ToggleQuickView`, Primary+K) and by a `SidePanel` profile. The cursor row's
  `FileDetail` metadata comes at once from `pane.cursor_detail()` (no selection change);
  the slice-9 `entry_detail` job starts once the cursor rested `FOLLOW_DELAY` (150 ms) and
  is cancelled when it moves. Accessors: `quick_view()`, `quick_view_detail()`,
  `side_detail(side)`, `follow_pending()` (views keep ticking / repainting). Quick-view
  details are never editable; a side panel's are, and `StageMetadata` falls back to the
  active pane's side panel when no File Info window is open.
- Keys: `QuickView` Primary+K, `Connect` moved to Primary+Shift+K (terminal table: Alt+K
  first, since most terminals send Ctrl+Shift+K as Ctrl+K), `ViewMenu` F9, `Expand` Right,
  `Collapse` Left. `binds()` reports Expand / Collapse unbound while the active pane is a
  flat list, so the TUI keeps Left / Right for tab switching there. The Explorer's Left /
  Right are now Expand / Collapse (same behaviour in the tree; in the list Right opens a
  folder or wrapper, no longer a file's File Info, and Left goes up).
- View menu: `view_menu()` returns `ViewMenuItem`s (styles, Quick view, Session log,
  Keyboard shortcuts) that egui draws as its "View" menu / F9 window and the TUI as its F9
  popup; `ShowViewMenu(bool)` and Esc close it.
- The Inspect browse view is the first to draw `side_detail` (step 3c); a folder still being
  listed is only a row flag (`loading`), so it draws its own "Loading..." child row.

Step 3 (Inspect browse view): done, 3a-3g and the leftovers (see "Step 3 status" below). It is the size of slices 1-8 together.
`src/gui/browse_view.rs` (7,218 lines, about 100 fields of state on `BrowseView`, lines 55-335)
needs one decision first and then lands in parts, each green:

- Decision (made 2026-11-02, done above): the pane grew listing styles; `inspect()` is the
  combined tree with a side panel, so the browse view keeps its look (`render_tree_entry`,
  :1570, and its lazy `FsRead` queue, :2034-2160, become the pane's combined tree).
- 3a, source and listing: `open_with_session` / `open` / `open_partclone` / `open_streaming`
  (:647-745) and the password prompt (:225-230) become `open_fixed_session(BrowseSession)` on
  the pane (partclone and streaming are session constructors in the model); selection and marks
  (`selected_entry`, `marked`, :63-68, `marked_export_entries` :1776) become pane selection.
- 3b, staging and apply: `staged_edits`, edit mode, `add_host_paths` / `stage_host_*`
  (:3954-4106), `perform_delete` (:4512), `create_new_folder` (:6396), staging errors (:5747),
  `start_apply` / `poll_apply_job` / follow-ups (:4534-4700), the conflict review (:4932) and the
  unsaved-close dialog (:5013) map onto the pane's `stage_edits`, Apply, review, report and the
  screen's close guard.
- 3c, look inside: `select_file` / `render_content_panel` (:2163-2634) and the xattr cache become
  the screen's File Info (`FileDetail` + its job); the ProDOS / HFS / perms / owner / xattr
  editors (:175-189) already stage through `MetadataSink`, so they send `StageMetadata`.
- 3d, take files out: extraction with its overwrite check (:2676-2830), `export_marked`
  (:1835), archive export (:4337) and tar export (:3225) become the screen's export (ProDOS
  export modes need a field on the export job).
- 3e, tools: the text editor (:4758-4930, `load_editor_text` / `save_editor_text` :7153-7230)
  becomes `EditText` / `SaveText`; fsck / repair (:5834-6170) becomes `Fsck` / `Repair`
  (`ToolReport` needs the structured `FsckResult` for the debug toggle and the journal view);
  the tree listing (:6184-6390) and boot blocks (:3920) become new tools.
- 3f, archives: the Mac archive window (:5307), archive import (:4107-4320, :5106) and tar
  import (:3476-3670) become wrapper expansion plus an `ImportArchive` tool.
- 3g, source-level modes stay with the Inspect tab: CHD edit mode (:3001-3224), direct edit
  (:3330), the SquashFS budget (:3352-3433) and the archive edit context (:763) decide how the
  session opens for writing; the pane receives the resulting session. `take_nested_open`
  (:2635) becomes the wrapper tree's open, handed to the host.
- Then `src/gui/browse_view.rs` joins ENFORCED.

### Step 3 status

The browse view hosts `CommanderController::single(.., PaneProfile::inspect())` (keys from
`keybindings.toml`, style remembered as `inspect`). The Inspect tab still builds the
`BrowseSession`; the view hands it over with `open_fixed_session` and hands it again whenever
it changes (Edit Mode on a CHD or a compressed backup, Show deleted, Full scan, the seekable
cache upgrade): the pane reopens the same path keeping the queue, folder and expanded set.
`src/gui/browse_view.rs` 7,218 -> 6,661 lines; shells' engine references 808 -> 775.

- 3a (c70214dd): listing, expansion, cursor, marks (the pane's selection, checkbox = ctrl-click),
  the queue (the pane's), the open and the password prompt through the pane. The tree is drawn
  from `rows()` with the old look; a fixed `/` row shows the volume. View menu = `view_menu()`
  (other styles drawn by Commander's `PaneView::show_listing`). Controller additions:
  `open_fixed_session`, `CursorTo`, `row_entry`, `entry_at`, `folder_children`,
  `set_read_only` (Edit Mode, set every frame), `VolumeInfo.blessed`,
  `BrowseSession.show_deleted`, staged folders' staged contents in the combined tree.
- 3c (16370cc6): the right panel is `side_detail()` drawn with Commander's `detail_meta` /
  `detail_preview` (editors stage through `StageMetadata`); a staged add has details; a side
  panel is editable only while the pane is writable. Preview reads, xattr cache and id-name map
  are gone from the view.
- 3b, part (3379b8bd): Apply (button and unsaved dialog) is the pane's Apply with PaneView's
  review / report; the view watches it land for its follow-ups (recompress, leave Edit Mode,
  close, CHD diff dirty). New Folder is the pane's prompt.
- 3e, editor (63749b6b): EditText / SaveText; `TextDoc.folder` + `SaveText.folder` so a deep file
  saves into its own folder.
- 3d, part (81b52913, 8ca88cc1): "Export selected" is the screen's Export; the export drops
  entries inside another selected folder.
- Marks across levels (e36d6438, user decision of 2026-11-02): in the combined tree a mark or a
  ctrl-click no longer clears the other level, so the listing's selection and the deep rows'
  together are the selection (`selected_entries` / `selected_pairs` return both; the wrapper tree's
  stays exclusive, a Shift range stays in one folder, flat lists are unchanged). `Mark(node_id)`
  toggles a row without moving the cursor; the Inspect checkboxes send it. Copy, delete, export and
  checksums act on the mixed selection.
- Text editing (user decision of 2026-11-02, reversed the same day): unchanged since 63749b6b. The
  editor needs Edit Mode, Save stages, Apply writes (`the_text_editor_saves_a_file_deep_in_the_tree_back_to_its_folder_on_apply`).
- 3b (e551ed46): `AddHostPaths { paths, folder }` (Add File, drops, the archive import's staging)
  stages through `model::commander_ops::stage_host_for` (forks only into HFS, ProDOS `#TTAAAA`
  names); names are legalised as a Commander copy's, and what cannot be staged is listed by
  `staging_errors()` until `DismissStagingErrors`. Delete with nothing marked takes the cursor row;
  `PaneProfile.confirm_folder_delete` (Inspect only) asks before staging the delete of a folder that
  holds anything or was never listed (`purpose::DELETE_FOLDERS`). `staged_summary()` is the unsaved
  dialog's summary; its Discard answers the pane's `DISCARD_STAGED`.
- 3e (76962633): `inspect()` turns on fsck and repair. `ToolReport` carries `CheckSummary` /
  `RepairSummary` (stats, errors, warnings with the debug flag, orphans by parent, repairable count)
  beside its lines. `ListTree { ids }` (`take_tree_text`), `ShowJournal` (`take_journal`) and
  `BootBlocks` (donor picked, read in a job, staged as `WriteBootBlocks`; `boot_blocks_present()`
  probed in a job) are pane tools (`pane/volume.rs`). Check, tree, journal and the archive reads
  borrow the reader the combined tree keeps for its folder listings (`take_reader` / `give_reader`).
- 3d (84015e3b, 13a8f448): `model::extract` + `HostCopyJob::Extract`; `CommanderAction::Extract
  { from, path, options, dest }` (a picker unless `dest`, which Open as Disk Image uses) runs through
  the copy job and the shared Overwrite / Skip / Cancel review. `CommanderAction::ExportEntry { from,
  path, format }` exports one entry or the whole volume (`/`) through the screen's export; a tar
  export takes its compression from the picked name. `ExportFormat::StuffItHqx` (.sit.hqx) is new.
- 3f, part (881da65b): `model::mac_archive_read`; `SniffArchive(path)` (own job slot, verdict in
  `archive_sniff`) and `ReadArchive { path, purpose }` (`take_archive`). The view's second reader
  (`cached_fs`, `take_or_open_fs`) is gone, so a large HFS+ catalog is read by the pane only.
- `src/gui/browse_view.rs` 6,661 -> 4,752 lines; shells' engine references 775 -> 724.

- 3f (871b1680): `pane/archives.rs` + `model::archive_import`. `ReadArchive` lands in
  `archive_viewer()` (Browse: rows, marks, fork container; `ArchiveMark`, `ArchiveFormat`,
  `ArchiveExtract { all }` picks a folder, `CloseArchive`) or `archive_save()` (Extract:
  `SaveArchive { raw }` picks a file or folder, `CancelSaveArchive`); `take_archive` is gone.
  `AddPicked(paths)` (Add File, drops) classifies picks in a job: plain items are staged, a tar waits
  in `pending_tar()` for `ChooseTar(Import | AsFile | Cancel)`, a Mac archive on HFS waits in
  `pending_import()` for `ChooseImport(Expand | ConvertOnly | AsIs | Cancel)` (decoded into a temp
  folder in a job, kept while anything is staged). `ImportTar` picks a tar; the import is a job
  (preflight, then `tar_preview()` + `ConfirmTarImport(bool)` when lossy) that writes at once and
  reopens. Results wait in `take_archive_notice()`; `archive_working()` is the job.
- Export default (1ef20c65, user request): `default_export_format(fs_type)` (PC -> Zip, MFS / HFS /
  HFS+ -> MAR, Unix -> tar.gz, else loose files). The screen keeps a pick per pane with the volume it
  was made on (a refresh keeps it, another volume drops it); `export_format()` / `export_format_for`
  / `export_choice_index()` serve the egui Commander, the Inspect view and the TUI Export-as menu.
- 3g (219d596c): `screen/modes.rs`, `CommanderAction::Mode(ModeAction)`. The host calls
  `open_inspect(session, label)` and `inspect_edit_in_place` / `inspect_edit_archive(ctx)` /
  `inspect_backup_folder`; `close_inspect` drops the panes, then a CHD diff or archive temp.
  `ToggleEditMode` backs up and opens a CHD over its diff, decompresses a zstd / WOZ partition,
  measures a SquashFS (`take_size_plan()` for the view's budget dialog, answered by `SetBudget`), or
  probes an editable open, each a job (`mode_working()`); leaving flattens or recompresses (jobs via
  `chd_edit_open::flatten_in_job`, `archive_edit::{extract_for_edit, compress_after_edit}`, no more
  `spawn_guarded` progress structs). `Apply` (recompresses an archive after), `Unsaved(Discard |
  Apply | Cancel)`, `RequestClose` (`take_close_request()`), `space()` (free-space probe job),
  `ChdInfo` (a job), `ShowDeleted`, `CarveFullScan`, `UpgradeSeekable`, `Password`, `OpenNested(path)`
  (room checked, extract into a temp folder, `take_nested_open()`). `volume_generation()` tells the
  view to drop its panel after a reopen or write (the pane's `write_count()` counts apply, repair and
  tar import). `BrowseSession::{partclone, streaming}` are the session constructors.
- Leftovers (this commit): Bless Folder is the pane's `Bless` (asks first); Lock / Unlock are
  `SetProdosLock(bool)` on the cursor entry; the tree window's Save is `SaveTree` (a save picker, then
  a host-write job of `last_tree()`). ProDOS type helpers, `preview_bytes` (QNX 2 RS -> LF),
  `TextShape` / `LineEnding`, `EntryType`, `FileEntryTypeDisplay`, `ArchiveEditContext` and
  `PartcloneCache` are re-exported from `controller::commander`.
- `src/gui/browse_view.rs` 4,752 -> 2,920 lines, 0 engine references, in ENFORCED; shells' engine
  references 724 -> 654. `src/gui/inspect_tab.rs` still had 181 (slice 11 below).

What is left in the browse view, none of it engine code: Add File's multi-file picker is still an
`rfd` call in the view (`add_file_dialog`, `src/gui/browse_view.rs:1975`; `Effect::PickFile` picks one
file), and the SquashFS budget dialog (`src/gui/squashfs_budget_dialog.rs`) still names
`fs_squashfs` types (2 references).

Behaviour changes since 881da65b: Bless Folder asks first; Lock / Unlock and Bless act on the
cursor row; entering Edit Mode on an image, CHD Info, the CHD flatten, the archive decompress and
recompress, and the archive viewer's and save dialog's writes run as jobs with a progress window
(the flatten and recompress no longer show an inline bar and cannot be cancelled); picks and drops
are classified off the UI thread and go to the cursor's folder; the decoded-archive temp folder is
kept until nothing is staged; the unsaved question's Apply with only a CHD diff waiting now leaves
Edit Mode (it did nothing); "Export selected" starts from the filesystem's default format (Zip /
MAR / tar.gz / loose files) instead of always MAR, and remembers a pick per volume.

Behaviour changes before that: the root is a fixed `/` row (no collapse); archives and images inside
the volume expand in place (wrappers), their rows show details only; the shared keys (arrows, Right
/ Left, Space, F2 rename, F8 delete, Ctrl+N, F9, Esc close) work once the tree is clicked; editing
text needs Edit Mode and Save stages (Apply writes it); after Apply the selection clears and the
tree reloads in place instead of freezing the window; HFS dates can be edited in the side panel.
Since 881da65b: host names a volume cannot hold are legalised (renamed, with a status line) rather
than refused, and a tar or Mac archive inside a dropped folder is added as a plain file (only
top-level picks and drops ask); Delete acts on the marks (else the cursor row) and the full-folder
question is the shared confirmation; Repair needs Edit Mode; the extraction's overwrite question
gains Skip and its progress is the copy job's window; the single-file BinHex export asks for a
folder; Check reuses the pane's reader.

## Slice 11: the Inspect tab

`src/gui/inspect_tab.rs` (6,212 lines, 181 engine references) was the GUI's largest screen: the source
picker, the open (a 800-line worker body), the partition grid, and a dozen disk tools, each with its
own `Arc<Mutex<Status>>` thread or job, all in the view.

### Steps

1. (d135d11d) Model and controller. `model::inspect_runner::inspect_source` is the old worker body
   (elevated / streamed / remote open, format and table detection, APM HFS and Linux relabels, HFS
   block sizes, volume labels, cheap minimum sizes, the optical redirect); `is_floppy_container_file`
   and `apply_volume_label` moved with it. `cache_runner::build_seekable_zstd` is the seekable-cache
   build. `worker::wait_spawned` follows a runner that spawns its own worker from inside a job.
   `controller::inspect::InspectController` (`mod.rs`, `open.rs`, `tools.rs`, `export.rs`,
   `browse.rs`) owns the source and what was found in it, every job, and the rules:
   - Sources: `InspectSource::{Device { slot, identity }, Image { path, temp }, Backup { folder },
     Remote { conn }}`. `Open` / `Close` ask `purpose::DISCARD_EDITS` first while the browse view
     holds edits (the shell reports that with `set_browse_dirty` each frame); `Load` / `Unload` are
     the unasked hand-offs (another tab, Open as Disk Image, a remote pick, Close Remote).
     `reconcile_devices` is the old device-slot check. A Mac archive becomes
     `HostRequest::OpenArchive`; a single-file-CHD backup reopens as its CHD with the folder kept
     for the export warning and the browse view's metadata refresh.
   - Results as plain data: `table_summary`, `alignment`, `backup_summary` (with the minimum restore
     size), `clonezilla_summary`, `min_cell` / `defrag_min` (backup record, else computed, else
     Calc min), `row_actions` (Browse, Check, Expand HFS, Defragment, Alto resize), `layout` (the
     disk bar's segments and trailing free space), `disk_size`, `flux_faults`, the enablement
     accessors (`can_edit_table`, `can_expand_image`, `can_make_bootable`, `can_export`, ...).
   - Jobs: the inspect and the backup load, Calc min and the backup label probes, Check / Repair
     (`purpose::REPAIR`), Make Bootable (`purpose::BOOT_DONOR` picker), the table write, Expand
     Image (raw and CHD), Defragment (`purpose::DEFRAGMENT`), the word swap (`purpose::SWAB_DEST`),
     Expand HFS's summary (`HostRequest::ExpandHfs`), CHD Info, Export (`EXPORT_DISK` /
     `EXPORT_FOLDER` / `EXPORT_HFV` pickers, the CHD free-space check, the sector-0 read in the
     job), the remote Back Up Image (`REMOTE_BACKUP`), the partclone scan and the seekable build.
     Log lines go through a sink jobs share; the shell drains `take_log`.
   - Browse: `HostRequest::OpenBrowse(BrowseOpen { session, edit: BrowseEdit, label_prefix,
     backup_folder })`, `UpgradeSeekable(path)`, `CloseBrowse`. The browse view's
     `open_request` replaced `open` / `open_partclone` / `open_streaming` and the edit setters.
   - `wait_opened` blocks until an open settles, for a headless shell's worker.
2. (74d65c28) The egui tab renders and maps: it keeps only widget state (the CHD options control,
   the Expand Image MiB field, the dialogs it hosts) and the recent-files mirror.
3. (this commit) `src/gui/inspect_tab.rs` joins `ENFORCED`; the TUI's Inspect tab opens an image file
   and a backup folder through the controller (`inspected_rows`, `open_backup_headless` in
   `tui_app.rs`); a ui scenario (`tests/ui_scenarios/inspect_open_check_close.toml`, vm `inspect`)
   opens, checks and closes behind the discard question.

Shells' engine references 654 -> 470 (`inspect_tab.rs` 181 -> 0, an unused re-export in
`expand_hfs_dialog.rs`, two `backup_loader` calls in the TUI).

### Behaviour changes

- The discard, Repair and Defragment questions are the shared confirmation dialog (the
  Defragment warning is part of its message, no longer a coloured line).
- Expand Image on a raw or VHD image, CHD Info, and the CHD sector-0 read for an export run in jobs
  (they ran on the UI thread). Expand Image (raw and CHD), Defragment and the check / table / Make
  Bootable steps show a small progress window; the export, the partclone scan and the seekable
  build use the shared job bar (rate and time left from the job).
- Closing or switching the source cancels running Calc min walks (they ran on to the end).
- Every source change closes the browse view, including a hand-off from another tab, Open as Disk
  Image and Close Remote (they left the old volume's browse view open).
- The top bar's Close Backup closes Inspect's source only when it is that backup (it cleared the
  partition list under an open image or device).
- Cancelling the discard question no longer ends a remote session that was open.
- A streamed zstd partition that cannot be opened logs the error and closes the browse view (the
  browse view showed it).
- The sector-by-sector warning reads the metadata loaded with the backup instead of re-reading
  `metadata.json` every frame.
- TUI: an image file's rows come from the controller, so a CHD, VHD or other container shows its
  partitions and Linux / APM rows name the filesystem found; a single-file-CHD backup folder opens
  as its disk image; a backup's info lines include the loader's progress lines. Devices keep the
  plain, unelevated read (the controller's open elevates, which prompts on macOS); when the
  controller finds no table the old raw read is used, so a table-less image still shows one row.
  The open also measures the cheap minimum sizes (FAT, NTFS, exFAT), so a large image opens a
  little slower.

### Left

- The TUI Inspect tab has none of the disk tools, Calc min or Export; it would drive the same
  `InspectController` actions (its effects are the TUI's existing pickers and confirmations).
- The dialogs the tab hosts still reach the engine: `partition_editor_modal.rs` (25),
  `resize_popup.rs` (22), `physical_disk_export.rs` (19), `remote_browser.rs` (12),
  `floppy_convert_dialog.rs` (8), `progress.rs` (7), `expand_hfs_dialog.rs` (4),
  `alto_resize_dialog.rs` (2). Each is a controller of its own (resize, Expand HFS, physical
  export) or plain data the inspect controller could re-export (`PartitionInfo`, the editor).
- Pickers are still blocking rfd calls on the UI thread (`EffectHost`), as in every egui view.

## Slice 12: the Backup tab

`src/gui/backup_tab.rs` (2,604 lines, 62 engine references) held the whole Backup flow in the
view: the source dropdown and its change detection, the image decode, the scan and image preview,
a raw `min_size_runner` thread per Calc min, the Compact Space rules, the single-file-CHD sizing,
the VHD options window and its whole-disk export thread, the Remote Drive connect / load threads,
and the backup worker. The TUI's Backup screen built its own `BackupConfig` and ran `run_backup`
on its own thread.

### Steps

1. (20b74a23) Model and controller. `model::backup_plan` holds the rules: `MinSizes` (the walks'
   in-place and defragmented minimums, fragmentation, the Compact toggle, its 5% auto-check and the
   toggle-aware effective minimum), `BackupOptions` (the split rule: VHD and CHD are one file;
   sector-by-sector turns resize off; a resize-to-minimum CHD needs minimums first), `sizing`
   (MinPlus20 targets, the Compact opt-in set, the precomputed minimums), `partition_filter`,
   `build_config`, `deferred_selected`, `remote_min_plan` and `is_device_path`.
   `controller::backup::BackupController` (`mod.rs`, `source.rs`, `remote.rs`, `run.rs`) owns:
   - Sources: `SelectDevice { slot, device }`, `OpenImage { path, rename }` (a wrapped image is
     decoded in a job first), `PickImage` (an `Effect::PickFile` with the disk-image and Mac-archive
     filters), the remote drive (`RemoteOpen`, `RemoteSetAddress`, `RemoteConnect`, `RemotePick`,
     `RemoteClose`, `RemoteClear`), `CloseSource` (releases the escalated device) and
     `reconcile_devices` (the old per-frame device-slot check). `HostRequest::RememberRecent` asks
     the shell to put an opened image on the recent list.
   - What was found as plain data: `rows()` (`PartitionRow` with `MinCell` and `CompactCell`),
     `table_desc`, `load_error`, `vhd_rows()` (`VhdRow`), `remote_drives()`.
   - Jobs: the decode, the scan / preview (`CancelScan`), Calc min (logging each phase), the remote
     connect and load, the backup (`run_mirrored` over `run_backup` / `run_backup_from`, its log and
     progress mirrored; `progress()` is the shared bar's `BackupBar`) and the whole-disk VHD export.
   - Start's rules: `validate()` (source, an image that exists, destination, name, a selection),
     the VHD window (scanning first when nothing was read), Calc min on the selected deferred
     partitions before a resize-to-minimum CHD (`start_pending`, `CancelPendingStart`), and a Start
     during the decode or scan that waits for it. The Custom CHD choice that travels with `Start` is
     saved to config.json (`saved_chd_custom` restores it).
   - `wait_idle` / `wait_finished` block for a headless shell or a test.
2. (08f563dd) The egui tab renders and maps: it keeps only the CHD options control, the recent-list
   mirror and whether it shows the shared progress bar.
3. (this commit) `src/gui/backup_tab.rs` joins `ENFORCED`; the TUI's Backup screen drives a
   headless `BackupController` (source, destination, name, format, checksum, Start, Cancel, the
   result and the last log lines; `the_backup_screen_backs_up_a_disk_through_the_controller`); a ui
   scenario (`tests/ui_scenarios/backup_open_validate_run.toml`, vm `backup`) opens an image, walks
   Start's refusals, opens and closes the VHD window, runs a zstd backup and closes. The dead
   `model::status::VhdExportStatus` is gone.

Shells' engine references 470 -> 393 (`backup_tab.rs` 62 -> 0, `tui_app.rs` 96 -> 81).

`rb-cli backup` keeps its own `BackupConfig`: its rules differ on purpose (`--split-size` passes
through so the engine refuses a split CHD, the name is the source stem and a time stamp, `--defrag`
is the FAT defragment, nothing is resized), so `build_config` would change what it writes.

### Behaviour changes

- GUI: picking a remote drive replaces the local source (the old tab dropped the remote pick on
  the next frame when a device or image was still selected).
- GUI: switching or closing the source cancels running Calc min walks (they ran on).
- GUI: a remote backup gets the same split rule as a local one (a VHD is never split) and remembers
  a Custom CHD choice too; the partition filter is sorted.
- GUI: a failed image decode is logged in the log panel (it went to stderr only); a recent pick of a
  wrapped image renames the backup once decoded (only an image ready at once was renamed).
- GUI: a panic in the backup reads "Backup failed: <message>" (it read "backup worker panicked").
- TUI: the backup name comes from the controller (size, label or stem, and the date, as the GUI
  names it) and is replaced on every source pick (it was the bare stem, kept once typed); an empty
  name is refused ("Enter a backup name.") instead of becoming "backup"; a missing output folder
  reads "Choose a destination folder.".
- TUI: an image source is previewed (partitions and minimums are read, and their count shows on the
  form), wrapped images (`.hdz`, `.adz`, `.msa`, ...) are decoded first, and a Start during either
  waits for it. The engine gets the minimums the preview found (the GUI always passed them).
- TUI: the run shows the controller's last log lines, and after Esc cancels it shows the final
  result instead of keeping "Cancel requested...".
- CLI: none.

### Left

- The TUI Backup screen has no partition selection, Compact, resize, split, CHD codec choice, remote
  drive or VHD window; each is an existing `BackupAction`.
- `rb-cli backup` could take `validate()` and `BackupOptions` once its rules are reconciled with the
  GUI's (the three differences above).
- The pickers are still blocking rfd calls on the UI thread (`EffectHost`).

Each slice: `cargo test` for what it touched, `tests/ui_scenarios.rs`,
`scripts/check-boundaries.sh`, the GUI build (`cargo check` / `clippy --bin rusty-backup`),
and the vintage proxy build from CONTRIBUTING.md § Rust 1.73 floor.
